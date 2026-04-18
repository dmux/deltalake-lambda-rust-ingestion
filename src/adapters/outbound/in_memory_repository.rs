use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::domain::commands::{
    CreateTableCommand, DeleteTableCommand, FieldDef, GetSchemaCommand, InsertCommand,
    OptimizeCommand, Record, TableHistoryCommand, TimeTravelCommand, UpsertCommand, VacuumCommand,
};
use crate::domain::ports::TableRepository;
use crate::domain::results::{
    CommitEntry, CreateTableResult, DeleteTableResult, HistoryResult, InsertResult, OptimizeResult,
    SchemaField, SchemaResult, TimeTravelResult, UpsertResult, VacuumResult,
};
use crate::error::AppError;

// ── Internal state ────────────────────────────────────────────────────────────

struct VersionSnapshot {
    version: i64,
    timestamp_ms: i64,
    records: Vec<Record>,
}

struct TableState {
    schema: Vec<FieldDef>,
    #[allow(dead_code)]
    partition_columns: Vec<String>,
    records: Vec<Record>,
    version: i64,
    num_files: usize,
    history: Vec<CommitEntry>,
    /// One snapshot per commit — enables time travel by version or timestamp.
    snapshots: Vec<VersionSnapshot>,
    /// Simulated orphaned file names for vacuum tests.
    orphaned_files: Vec<String>,
}

impl TableState {
    fn new(schema: Vec<FieldDef>, partition_columns: Vec<String>) -> Self {
        let now = now_ms();
        Self {
            schema,
            partition_columns,
            records: vec![],
            version: 0,
            num_files: 0,
            orphaned_files: vec![
                "part-00000-orphan1.snappy.parquet".into(),
                "part-00000-orphan2.snappy.parquet".into(),
            ],
            history: vec![CommitEntry {
                version: 0,
                timestamp_ms: now,
                operation: "CREATE TABLE".into(),
                operation_parameters: HashMap::new(),
            }],
            snapshots: vec![VersionSnapshot {
                version: 0,
                timestamp_ms: now,
                records: vec![],
            }],
        }
    }

    fn commit(&mut self, operation: &str, params: HashMap<String, String>) -> i64 {
        self.version += 1;
        let now = now_ms();
        self.history.push(CommitEntry {
            version: self.version,
            timestamp_ms: now,
            operation: operation.into(),
            operation_parameters: params,
        });
        self.snapshots.push(VersionSnapshot {
            version: self.version,
            timestamp_ms: now,
            records: self.records.clone(),
        });
        self.version
    }

    fn snapshot_at_version(&self, version: i64) -> Option<&VersionSnapshot> {
        self.snapshots.iter().find(|s| s.version == version)
    }

    fn snapshot_at_timestamp(&self, timestamp_ms: i64) -> Option<&VersionSnapshot> {
        // Return the latest snapshot whose commit timestamp is <= requested
        self.snapshots
            .iter()
            .filter(|s| s.timestamp_ms <= timestamp_ms)
            .last()
    }

    fn schema_fields(&self) -> Vec<SchemaField> {
        self.schema
            .iter()
            .map(|f| SchemaField {
                name: f.name.clone(),
                data_type: f.data_type.clone(),
                nullable: f.nullable,
            })
            .collect()
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Parse `"target.col = source.col"` pairs from a merge predicate.
/// Supports AND-chained clauses: `"target.a = source.a AND target.b = source.b"`.
fn extract_key_pairs(predicate: &str) -> Vec<(String, String)> {
    predicate
        .split("AND")
        .filter_map(|clause| {
            let parts: Vec<&str> = clause.split('=').collect();
            if parts.len() != 2 {
                return None;
            }
            let left = parts[0].trim().strip_prefix("target.")?.trim().to_string();
            let right = parts[1].trim().strip_prefix("source.")?.trim().to_string();
            Some((left, right))
        })
        .collect()
}

// ── Adapter ───────────────────────────────────────────────────────────────────

/// Outbound adapter — implements `TableRepository` entirely in memory.
///
/// No S3, no Parquet, no delta-rs. Used in unit and integration tests so the
/// full `IngestionService` can be exercised without any AWS infrastructure.
pub struct InMemoryTableRepository {
    tables: Mutex<HashMap<String, TableState>>,
}

impl InMemoryTableRepository {
    pub fn new() -> Self {
        Self {
            tables: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for InMemoryTableRepository {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableRepository for InMemoryTableRepository {
    async fn create_table(&self, cmd: CreateTableCommand) -> Result<CreateTableResult, AppError> {
        let mut tables = self.tables.lock().unwrap();

        if tables.contains_key(&cmd.table_uri) {
            let version = tables[&cmd.table_uri].version;
            return Ok(CreateTableResult {
                version,
                message: "Table already existed".into(),
            });
        }

        let state = TableState::new(cmd.schema, cmd.partition_columns);
        let version = state.version;
        tables.insert(cmd.table_uri, state);

        Ok(CreateTableResult {
            version,
            message: "Table created".into(),
        })
    }

    async fn insert(&self, cmd: InsertCommand) -> Result<InsertResult, AppError> {
        let mut tables = self.tables.lock().unwrap();
        let state = tables
            .get_mut(&cmd.table_uri)
            .ok_or_else(|| AppError::Delta(deltalake::DeltaTableError::NotATable(cmd.table_uri.clone())))?;

        let num_rows = cmd.records.len();
        state.records.extend(cmd.records);
        state.num_files += 1;

        let mut params = HashMap::new();
        params.insert("mode".into(), "Append".into());
        let version = state.commit("WRITE", params);

        Ok(InsertResult {
            version,
            rows_written: num_rows,
        })
    }

    async fn upsert(&self, cmd: UpsertCommand) -> Result<UpsertResult, AppError> {
        let mut tables = self.tables.lock().unwrap();
        let state = tables
            .get_mut(&cmd.table_uri)
            .ok_or_else(|| AppError::Delta(deltalake::DeltaTableError::NotATable(cmd.table_uri.clone())))?;

        let key_pairs = extract_key_pairs(&cmd.merge_predicate);
        if key_pairs.is_empty() {
            return Err(AppError::Schema(format!(
                "Could not parse merge predicate: '{}'",
                cmd.merge_predicate
            )));
        }

        let mut inserted = 0usize;
        let mut updated = 0usize;
        let num_source = cmd.records.len();

        for source_row in &cmd.records {
            // Check if there is a matching target row
            let match_idx = state.records.iter().position(|target| {
                key_pairs.iter().all(|(target_col, source_col)| {
                    target.get(target_col) == source_row.get(source_col)
                })
            });

            match match_idx {
                Some(idx) => {
                    // Update non-key columns in-place
                    for (k, v) in source_row {
                        state.records[idx].insert(k.clone(), v.clone());
                    }
                    updated += 1;
                }
                None => {
                    state.records.push(source_row.clone());
                    inserted += 1;
                }
            }
        }

        let num_output = state.records.len();
        let mut params = HashMap::new();
        params.insert("predicate".into(), cmd.merge_predicate.clone());
        let version = state.commit("MERGE", params);

        Ok(UpsertResult {
            version,
            num_source_rows: num_source,
            num_target_rows_inserted: inserted,
            num_target_rows_updated: updated,
            num_target_rows_deleted: 0,
            num_output_rows: num_output,
        })
    }

    async fn vacuum(&self, cmd: VacuumCommand) -> Result<VacuumResult, AppError> {
        let mut tables = self.tables.lock().unwrap();
        let state = tables
            .get_mut(&cmd.table_uri)
            .ok_or_else(|| AppError::Delta(deltalake::DeltaTableError::NotATable(cmd.table_uri.clone())))?;

        let files_to_delete = state.orphaned_files.clone();

        if !cmd.dry_run {
            state.orphaned_files.clear();
        }

        Ok(VacuumResult {
            version: state.version,
            dry_run: cmd.dry_run,
            files_deleted: files_to_delete,
        })
    }

    async fn optimize(&self, cmd: OptimizeCommand) -> Result<OptimizeResult, AppError> {
        let mut tables = self.tables.lock().unwrap();
        let state = tables
            .get_mut(&cmd.table_uri)
            .ok_or_else(|| AppError::Delta(deltalake::DeltaTableError::NotATable(cmd.table_uri.clone())))?;

        // Simulate compacting N small files into 1
        let files_before = state.num_files;
        let files_removed = if files_before > 1 { files_before - 1 } else { 0 };
        state.num_files = if files_before > 0 { 1 } else { 0 };

        let mut params = HashMap::new();
        if !cmd.zorder_columns.is_empty() {
            params.insert("zOrderBy".into(), cmd.zorder_columns.join(","));
        }
        let version = state.commit("OPTIMIZE", params);

        Ok(OptimizeResult {
            version,
            files_added: if files_before > 1 { 1 } else { 0 },
            files_removed,
            total_files_skipped: 0,
        })
    }

    async fn time_travel(&self, cmd: TimeTravelCommand) -> Result<TimeTravelResult, AppError> {
        let tables = self.tables.lock().unwrap();
        let state = tables
            .get(&cmd.table_uri)
            .ok_or_else(|| AppError::Delta(deltalake::DeltaTableError::NotATable(cmd.table_uri.clone())))?;

        let snapshot = if let Some(version) = cmd.version {
            state
                .snapshot_at_version(version)
                .ok_or_else(|| AppError::Schema(format!("Version {version} not found")))?
        } else if let Some(ts_str) = cmd.timestamp {
            let dt: chrono::DateTime<chrono::Utc> = ts_str
                .parse()
                .map_err(|_| AppError::Timestamp(ts_str.clone()))?;
            let ts_ms = dt.timestamp_millis();
            state
                .snapshot_at_timestamp(ts_ms)
                .ok_or_else(|| AppError::Schema("No version found before timestamp".into()))?
        } else {
            return Err(AppError::MissingField(
                "version or timestamp required".into(),
            ));
        };

        let fields = state.schema_fields();
        let num_files = snapshot.records.len(); // simplified: 1 file per record batch

        Ok(TimeTravelResult {
            version: snapshot.version,
            num_files,
            schema: SchemaResult {
                version: snapshot.version,
                num_files,
                fields,
            },
        })
    }

    async fn table_history(&self, cmd: TableHistoryCommand) -> Result<HistoryResult, AppError> {
        let tables = self.tables.lock().unwrap();
        let state = tables
            .get(&cmd.table_uri)
            .ok_or_else(|| AppError::Delta(deltalake::DeltaTableError::NotATable(cmd.table_uri.clone())))?;

        let mut entries = state.history.clone();
        entries.reverse(); // newest first, like real Delta history
        if let Some(limit) = cmd.limit {
            entries.truncate(limit);
        }

        let total = entries.len();
        Ok(HistoryResult {
            total_commits: total,
            history: entries,
        })
    }

    async fn get_schema(&self, cmd: GetSchemaCommand) -> Result<SchemaResult, AppError> {
        let tables = self.tables.lock().unwrap();
        let state = tables
            .get(&cmd.table_uri)
            .ok_or_else(|| AppError::Delta(deltalake::DeltaTableError::NotATable(cmd.table_uri.clone())))?;

        Ok(SchemaResult {
            version: state.version,
            num_files: state.num_files,
            fields: state.schema_fields(),
        })
    }

    async fn delete_table(&self, cmd: DeleteTableCommand) -> Result<DeleteTableResult, AppError> {
        let mut tables = self.tables.lock().unwrap();
        if tables.remove(&cmd.table_uri).is_none() {
            return Err(AppError::Delta(deltalake::DeltaTableError::NotATable(
                cmd.table_uri.clone(),
            )));
        }
        Ok(DeleteTableResult {
            table_uri: cmd.table_uri,
            objects_deleted: 0, // in-memory: no actual objects
        })
    }
}

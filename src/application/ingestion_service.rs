use std::sync::Arc;

use tracing::info;

use crate::domain::commands::{
    CreateTableCommand, DeleteTableCommand, GetSchemaCommand, InsertCommand, OptimizeCommand,
    TableHistoryCommand, TimeTravelCommand, UpsertCommand, VacuumCommand,
};
use crate::domain::ports::TableRepository;
use crate::domain::results::{
    CreateTableResult, DeleteTableResult, HistoryResult, InsertResult, OptimizeResult, SchemaResult,
    TimeTravelResult, UpsertResult, VacuumResult,
};
use crate::error::AppError;

/// Application service — the core hexagon.
///
/// Orchestrates operations by validating commands and delegating to the
/// `TableRepository` port. It has no knowledge of Lambda, S3, or delta-rs:
/// those details live in the adapter implementations.
pub struct IngestionService {
    repo: Arc<dyn TableRepository>,
}

impl IngestionService {
    pub fn new(repo: Arc<dyn TableRepository>) -> Self {
        Self { repo }
    }

    pub async fn create_table(
        &self,
        cmd: CreateTableCommand,
    ) -> Result<CreateTableResult, AppError> {
        if cmd.schema.is_empty() {
            return Err(AppError::MissingField(
                "schema must have at least one field".into(),
            ));
        }
        info!(table_uri = %cmd.table_uri, fields = cmd.schema.len(), "create_table");
        self.repo.create_table(cmd).await
    }

    pub async fn insert(&self, cmd: InsertCommand) -> Result<InsertResult, AppError> {
        if cmd.records.is_empty() {
            return Ok(InsertResult {
                version: -1,
                rows_written: 0,
            });
        }
        info!(table_uri = %cmd.table_uri, rows = cmd.records.len(), "insert");
        self.repo.insert(cmd).await
    }

    pub async fn upsert(&self, cmd: UpsertCommand) -> Result<UpsertResult, AppError> {
        if cmd.records.is_empty() {
            return Ok(UpsertResult {
                version: -1,
                num_source_rows: 0,
                num_target_rows_inserted: 0,
                num_target_rows_updated: 0,
                num_target_rows_deleted: 0,
                num_output_rows: 0,
            });
        }
        if cmd.merge_predicate.is_empty() {
            return Err(AppError::MissingField("merge_predicate".into()));
        }
        info!(
            table_uri = %cmd.table_uri,
            rows = cmd.records.len(),
            predicate = %cmd.merge_predicate,
            "upsert"
        );
        self.repo.upsert(cmd).await
    }

    pub async fn vacuum(&self, cmd: VacuumCommand) -> Result<VacuumResult, AppError> {
        info!(
            table_uri = %cmd.table_uri,
            retention_hours = cmd.retention_hours,
            dry_run = cmd.dry_run,
            "vacuum"
        );
        self.repo.vacuum(cmd).await
    }

    pub async fn optimize(&self, cmd: OptimizeCommand) -> Result<OptimizeResult, AppError> {
        info!(
            table_uri = %cmd.table_uri,
            zorder_columns = ?cmd.zorder_columns,
            "optimize"
        );
        self.repo.optimize(cmd).await
    }

    pub async fn time_travel(
        &self,
        cmd: TimeTravelCommand,
    ) -> Result<TimeTravelResult, AppError> {
        if cmd.version.is_none() && cmd.timestamp.is_none() {
            return Err(AppError::MissingField(
                "either 'version' or 'timestamp' is required".into(),
            ));
        }
        info!(table_uri = %cmd.table_uri, version = ?cmd.version, timestamp = ?cmd.timestamp, "time_travel");
        self.repo.time_travel(cmd).await
    }

    pub async fn table_history(
        &self,
        cmd: TableHistoryCommand,
    ) -> Result<HistoryResult, AppError> {
        info!(table_uri = %cmd.table_uri, limit = ?cmd.limit, "table_history");
        self.repo.table_history(cmd).await
    }

    pub async fn get_schema(&self, cmd: GetSchemaCommand) -> Result<SchemaResult, AppError> {
        info!(table_uri = %cmd.table_uri, "get_schema");
        self.repo.get_schema(cmd).await
    }

    pub async fn delete_table(
        &self,
        cmd: DeleteTableCommand,
    ) -> Result<DeleteTableResult, AppError> {
        info!(table_uri = %cmd.table_uri, "delete_table");
        self.repo.delete_table(cmd).await
    }
}

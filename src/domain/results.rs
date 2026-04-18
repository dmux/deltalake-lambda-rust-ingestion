#[derive(Debug, Clone)]
pub struct DeleteTableResult {
    pub table_uri: String,
    pub objects_deleted: usize,
}

/// A single column in a Delta table schema.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaField {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct CreateTableResult {
    pub version: i64,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct InsertResult {
    pub version: i64,
    pub rows_written: usize,
}

#[derive(Debug, Clone)]
pub struct UpsertResult {
    pub version: i64,
    pub num_source_rows: usize,
    pub num_target_rows_inserted: usize,
    pub num_target_rows_updated: usize,
    pub num_target_rows_deleted: usize,
    pub num_output_rows: usize,
}

#[derive(Debug, Clone)]
pub struct VacuumResult {
    pub version: i64,
    pub dry_run: bool,
    pub files_deleted: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct OptimizeResult {
    pub version: i64,
    pub files_added: usize,
    pub files_removed: usize,
    pub total_files_skipped: usize,
}

#[derive(Debug, Clone)]
pub struct TimeTravelResult {
    pub version: i64,
    pub num_files: usize,
    pub schema: SchemaResult,
}

#[derive(Debug, Clone)]
pub struct CommitEntry {
    pub version: i64,
    pub timestamp_ms: i64,
    pub operation: String,
    pub operation_parameters: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct HistoryResult {
    pub total_commits: usize,
    pub history: Vec<CommitEntry>,
}

#[derive(Debug, Clone)]
pub struct SchemaResult {
    pub version: i64,
    pub num_files: usize,
    pub fields: Vec<SchemaField>,
}

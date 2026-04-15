use std::collections::HashMap;

pub type Record = HashMap<String, serde_json::Value>;

/// Describes a single column when creating a table schema.
#[derive(Debug, Clone)]
pub struct FieldDef {
    pub name: String,
    /// Type name as a string: "string", "long", "double", "boolean", "timestamp", etc.
    pub data_type: String,
    pub nullable: bool,
}

// ── Commands ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CreateTableCommand {
    pub table_uri: String,
    pub schema: Vec<FieldDef>,
    pub partition_columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct InsertCommand {
    pub table_uri: String,
    pub records: Vec<Record>,
    pub partition_columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct UpsertCommand {
    pub table_uri: String,
    pub records: Vec<Record>,
    /// SQL predicate, e.g. "target.id = source.id"
    pub merge_predicate: String,
    /// Key columns used to identify matching rows
    pub match_columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct VacuumCommand {
    pub table_uri: String,
    pub retention_hours: i64,
    pub dry_run: bool,
}

#[derive(Debug, Clone)]
pub struct OptimizeCommand {
    pub table_uri: String,
    pub zorder_columns: Vec<String>,
    pub target_file_size: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct TimeTravelCommand {
    pub table_uri: String,
    /// Read at this exact version number.
    pub version: Option<i64>,
    /// Read as of this RFC3339 timestamp.
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TableHistoryCommand {
    pub table_uri: String,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct GetSchemaCommand {
    pub table_uri: String,
}

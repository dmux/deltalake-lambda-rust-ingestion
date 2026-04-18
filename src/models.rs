use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ── Top-level request / response ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct LambdaRequest {
    pub operation: String,
    pub table_uri: String,
    #[serde(default)]
    pub payload: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct LambdaResponse {
    pub success: bool,
    pub operation: String,
    pub table_uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ── Per-operation payload models ──────────────────────────────────────────────

/// Field definition used when creating a new Delta table schema.
#[derive(Debug, Deserialize)]
pub struct SchemaFieldDef {
    pub name: String,
    /// Accepted values: "string", "integer", "long", "float", "double",
    /// "boolean", "date", "timestamp", "timestamp_ntz", "binary"
    pub data_type: String,
    #[serde(default = "default_true")]
    pub nullable: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub struct CreateTablePayload {
    pub schema: Vec<SchemaFieldDef>,
    #[serde(default)]
    pub partition_columns: Vec<String>,
    /// When true, all existing S3 objects under the table prefix are deleted
    /// before creating the table — equivalent to DROP + CREATE.
    #[serde(default)]
    pub overwrite: bool,
}

#[derive(Debug, Deserialize)]
pub struct InsertPayload {
    pub records: Vec<HashMap<String, serde_json::Value>>,
    #[serde(default)]
    pub partition_columns: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpsertPayload {
    pub records: Vec<HashMap<String, serde_json::Value>>,
    /// SQL predicate matching source and target rows, e.g. "target.id = source.id"
    pub merge_predicate: String,
    /// Columns used as match keys (used to infer which columns to update)
    pub match_columns: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct VacuumPayload {
    /// Retention period in hours. Default: 168 (7 days).
    #[serde(default = "default_retention")]
    pub retention_hours: i64,
    /// If true, list files to be removed without deleting them.
    #[serde(default)]
    pub dry_run: bool,
}

fn default_retention() -> i64 {
    168
}

#[derive(Debug, Deserialize)]
pub struct OptimizePayload {
    /// Columns for Z-order clustering. Empty = plain compaction.
    #[serde(default)]
    pub zorder_columns: Vec<String>,
    /// Target Parquet file size in bytes. Default: 256 MB.
    pub target_file_size: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct TimeTravelPayload {
    /// Read the table at this exact version number.
    pub version: Option<i64>,
    /// Read the table as of this RFC3339 timestamp, e.g. "2024-01-01T00:00:00Z".
    pub timestamp: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TableHistoryPayload {
    /// Maximum number of commit log entries to return. Default: all.
    pub limit: Option<usize>,
}

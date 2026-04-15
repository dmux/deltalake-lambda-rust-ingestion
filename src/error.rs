use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Delta table error: {0}")]
    Delta(#[from] deltalake::DeltaTableError),

    #[error("Unknown operation: {0}")]
    UnknownOperation(String),

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Arrow error: {0}")]
    Arrow(String),

    #[error("DataFusion error: {0}")]
    DataFusion(String),

    #[error("Invalid timestamp: {0}")]
    Timestamp(String),
}

impl From<deltalake::arrow::error::ArrowError> for AppError {
    fn from(e: deltalake::arrow::error::ArrowError) -> Self {
        AppError::Arrow(e.to_string())
    }
}

impl From<deltalake::datafusion::error::DataFusionError> for AppError {
    fn from(e: deltalake::datafusion::error::DataFusionError) -> Self {
        AppError::DataFusion(e.to_string())
    }
}

// AppError implements std::error::Error + Send + Sync (via thiserror),
// so the blanket impl already covers Box<dyn Error> — no manual impl needed.

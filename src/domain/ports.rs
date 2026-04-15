use async_trait::async_trait;

use crate::domain::commands::{
    CreateTableCommand, GetSchemaCommand, InsertCommand, OptimizeCommand, TableHistoryCommand,
    TimeTravelCommand, UpsertCommand, VacuumCommand,
};
use crate::domain::results::{
    CreateTableResult, HistoryResult, InsertResult, OptimizeResult, SchemaResult,
    TimeTravelResult, UpsertResult, VacuumResult,
};
use crate::error::AppError;

/// Secondary port (driven side) — the interface the application core uses to
/// persist and query Delta Lake tables. Any adapter that implements this trait
/// can be injected: the real S3-backed delta-rs adapter in production, or an
/// in-memory adapter in tests.
#[async_trait]
pub trait TableRepository: Send + Sync {
    async fn create_table(&self, cmd: CreateTableCommand) -> Result<CreateTableResult, AppError>;
    async fn insert(&self, cmd: InsertCommand) -> Result<InsertResult, AppError>;
    async fn upsert(&self, cmd: UpsertCommand) -> Result<UpsertResult, AppError>;
    async fn vacuum(&self, cmd: VacuumCommand) -> Result<VacuumResult, AppError>;
    async fn optimize(&self, cmd: OptimizeCommand) -> Result<OptimizeResult, AppError>;
    async fn time_travel(&self, cmd: TimeTravelCommand) -> Result<TimeTravelResult, AppError>;
    async fn table_history(&self, cmd: TableHistoryCommand) -> Result<HistoryResult, AppError>;
    async fn get_schema(&self, cmd: GetSchemaCommand) -> Result<SchemaResult, AppError>;
}

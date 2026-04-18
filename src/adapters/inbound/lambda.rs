use std::sync::Arc;

use lambda_runtime::{Error, LambdaEvent};
use serde_json::json;
use tracing::{error, info, instrument};

use crate::application::ingestion_service::IngestionService;
use crate::domain::commands::{
    CreateTableCommand, DeleteTableCommand, FieldDef, GetSchemaCommand, InsertCommand,
    OptimizeCommand, TableHistoryCommand, TimeTravelCommand, UpsertCommand, VacuumCommand,
};
use crate::error::AppError;
use crate::models::{
    CreateTablePayload, InsertPayload, LambdaRequest, LambdaResponse, OptimizePayload,
    TableHistoryPayload, TimeTravelPayload, UpsertPayload, VacuumPayload,
};

/// Primary (inbound) adapter — translates Lambda JSON events into typed domain
/// commands, delegates to `IngestionService`, and serializes the result back
/// to the Lambda response envelope.
#[instrument(skip(event, service), fields(operation, table_uri))]
pub async fn handle(
    event: LambdaEvent<LambdaRequest>,
    service: Arc<IngestionService>,
) -> Result<LambdaResponse, Error> {
    let req = event.payload;
    let op = req.operation.clone();
    let uri = req.table_uri.clone();

    tracing::Span::current().record("operation", &op.as_str());
    tracing::Span::current().record("table_uri", &uri.as_str());
    info!("Dispatching operation");

    let result = dispatch(&op, &uri, &req.payload, &service).await;

    match result {
        Ok(value) => Ok(LambdaResponse {
            success: true,
            operation: op,
            table_uri: uri,
            result: Some(value),
            error: None,
        }),
        Err(e) => {
            error!(error = %e, "Operation failed");
            Ok(LambdaResponse {
                success: false,
                operation: op,
                table_uri: uri,
                result: None,
                error: Some(e.to_string()),
            })
        }
    }
}

async fn dispatch(
    operation: &str,
    uri: &str,
    payload: &serde_json::Value,
    service: &IngestionService,
) -> Result<serde_json::Value, AppError> {
    match operation {
        "create_table" => {
            let p: CreateTablePayload = serde_json::from_value(payload.clone())?;
            let cmd = CreateTableCommand {
                table_uri: uri.to_string(),
                schema: p
                    .schema
                    .into_iter()
                    .map(|f| FieldDef {
                        name: f.name,
                        data_type: f.data_type,
                        nullable: f.nullable,
                    })
                    .collect(),
                partition_columns: p.partition_columns,
                overwrite: p.overwrite,
            };
            let r = service.create_table(cmd).await?;
            Ok(json!({ "version": r.version, "message": r.message }))
        }

        "insert" => {
            let p: InsertPayload = serde_json::from_value(payload.clone())?;
            let cmd = InsertCommand {
                table_uri: uri.to_string(),
                records: p.records,
                partition_columns: p.partition_columns,
            };
            let r = service.insert(cmd).await?;
            Ok(json!({ "version": r.version, "rows_written": r.rows_written }))
        }

        "upsert" => {
            let p: UpsertPayload = serde_json::from_value(payload.clone())?;
            let cmd = UpsertCommand {
                table_uri: uri.to_string(),
                records: p.records,
                merge_predicate: p.merge_predicate,
                match_columns: p.match_columns,
            };
            let r = service.upsert(cmd).await?;
            Ok(json!({
                "version": r.version,
                "num_source_rows": r.num_source_rows,
                "num_target_rows_inserted": r.num_target_rows_inserted,
                "num_target_rows_updated": r.num_target_rows_updated,
                "num_output_rows": r.num_output_rows,
            }))
        }

        "vacuum" => {
            let p: VacuumPayload = serde_json::from_value(payload.clone())?;
            let cmd = VacuumCommand {
                table_uri: uri.to_string(),
                retention_hours: p.retention_hours,
                dry_run: p.dry_run,
            };
            let r = service.vacuum(cmd).await?;
            Ok(json!({
                "version": r.version,
                "dry_run": r.dry_run,
                "files_deleted": r.files_deleted,
            }))
        }

        "optimize" => {
            let p: OptimizePayload = serde_json::from_value(payload.clone())?;
            let cmd = OptimizeCommand {
                table_uri: uri.to_string(),
                zorder_columns: p.zorder_columns,
                target_file_size: p.target_file_size,
            };
            let r = service.optimize(cmd).await?;
            Ok(json!({
                "version": r.version,
                "files_added": r.files_added,
                "files_removed": r.files_removed,
                "total_files_skipped": r.total_files_skipped,
            }))
        }

        "time_travel" => {
            let p: TimeTravelPayload = serde_json::from_value(payload.clone())?;
            let cmd = TimeTravelCommand {
                table_uri: uri.to_string(),
                version: p.version,
                timestamp: p.timestamp,
            };
            let r = service.time_travel(cmd).await?;
            Ok(json!({
                "version": r.version,
                "num_files": r.num_files,
                "schema": {
                    "fields": r.schema.fields.iter().map(|f| json!({
                        "name": f.name,
                        "data_type": f.data_type,
                        "nullable": f.nullable,
                    })).collect::<Vec<_>>()
                }
            }))
        }

        "table_history" => {
            let p: TableHistoryPayload = serde_json::from_value(payload.clone())?;
            let cmd = TableHistoryCommand {
                table_uri: uri.to_string(),
                limit: p.limit,
            };
            let r = service.table_history(cmd).await?;
            Ok(json!({
                "total_commits": r.total_commits,
                "history": r.history.iter().map(|c| json!({
                    "version": c.version,
                    "timestamp": c.timestamp_ms,
                    "operation": c.operation,
                    "operation_parameters": c.operation_parameters,
                })).collect::<Vec<_>>()
            }))
        }

        "get_schema" => {
            let cmd = GetSchemaCommand {
                table_uri: uri.to_string(),
            };
            let r = service.get_schema(cmd).await?;
            Ok(json!({
                "version": r.version,
                "num_files": r.num_files,
                "schema": {
                    "fields": r.fields.iter().map(|f| json!({
                        "name": f.name,
                        "data_type": f.data_type,
                        "nullable": f.nullable,
                    })).collect::<Vec<_>>()
                }
            }))
        }

        "delete_table" => {
            let cmd = DeleteTableCommand {
                table_uri: uri.to_string(),
            };
            let r = service.delete_table(cmd).await?;
            Ok(json!({
                "table_uri": r.table_uri,
                "objects_deleted": r.objects_deleted,
            }))
        }

        other => Err(AppError::UnknownOperation(other.to_string())),
    }
}

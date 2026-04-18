use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use deltalake::arrow::array::{
    Array, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use deltalake::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::kernel::{DataType as DeltaDataType, PrimitiveType, StructType};
use deltalake::operations::optimize::OptimizeType;
use deltalake::protocol::SaveMode;
use deltalake::{open_table_with_storage_options, DeltaOps, DeltaTableBuilder};

use crate::domain::commands::{
    CreateTableCommand, DeleteTableCommand, GetSchemaCommand, InsertCommand, OptimizeCommand,
    Record, TableHistoryCommand, TimeTravelCommand, UpsertCommand, VacuumCommand,
};
use crate::domain::ports::TableRepository;
use crate::domain::results::{
    CommitEntry, CreateTableResult, DeleteTableResult, HistoryResult, InsertResult, OptimizeResult,
    SchemaField, SchemaResult, TimeTravelResult, UpsertResult, VacuumResult,
};
use crate::error::AppError;

fn s3_storage_options() -> HashMap<String, String> {
    let mut opts = HashMap::new();
    opts.insert(
        "AWS_S3_ALLOW_UNSAFE_RENAME".to_string(),
        "true".to_string(),
    );
    opts
}

fn parse_delta_type(s: &str) -> Result<DeltaDataType, AppError> {
    match s.to_lowercase().as_str() {
        "string" | "str" => Ok(DeltaDataType::Primitive(PrimitiveType::String)),
        "integer" | "int" => Ok(DeltaDataType::Primitive(PrimitiveType::Integer)),
        "long" => Ok(DeltaDataType::Primitive(PrimitiveType::Long)),
        "float" => Ok(DeltaDataType::Primitive(PrimitiveType::Float)),
        "double" => Ok(DeltaDataType::Primitive(PrimitiveType::Double)),
        "boolean" | "bool" => Ok(DeltaDataType::Primitive(PrimitiveType::Boolean)),
        "date" => Ok(DeltaDataType::Primitive(PrimitiveType::Date)),
        "timestamp" => Ok(DeltaDataType::Primitive(PrimitiveType::Timestamp)),
        "timestamp_ntz" => Ok(DeltaDataType::Primitive(PrimitiveType::TimestampNtz)),
        "binary" => Ok(DeltaDataType::Primitive(PrimitiveType::Binary)),
        other => Err(AppError::Schema(format!("Unknown data type: '{other}'"))),
    }
}

fn delta_primitive_to_arrow(p: &PrimitiveType) -> Result<ArrowDataType, AppError> {
    Ok(match p {
        PrimitiveType::String => ArrowDataType::Utf8,
        PrimitiveType::Integer => ArrowDataType::Int32,
        PrimitiveType::Long => ArrowDataType::Int64,
        PrimitiveType::Float => ArrowDataType::Float32,
        PrimitiveType::Double => ArrowDataType::Float64,
        PrimitiveType::Boolean => ArrowDataType::Boolean,
        PrimitiveType::Date => ArrowDataType::Date32,
        PrimitiveType::Timestamp => {
            ArrowDataType::Timestamp(
                deltalake::arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into()),
            )
        }
        PrimitiveType::TimestampNtz => {
            ArrowDataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Microsecond, None)
        }
        PrimitiveType::Binary => ArrowDataType::Binary,
        PrimitiveType::Byte => ArrowDataType::Int8,
        PrimitiveType::Short => ArrowDataType::Int16,
        PrimitiveType::Decimal(p, s) => ArrowDataType::Decimal128(*p, *s as i8),
    })
}

fn delta_schema_to_results(schema: &StructType) -> Vec<SchemaField> {
    schema
        .fields()
        .map(|f| SchemaField {
            name: f.name().to_string(),
            data_type: format!("{}", f.data_type()),
            nullable: f.is_nullable(),
        })
        .collect()
}

fn records_to_record_batch(
    records: &[Record],
    schema: &StructType,
) -> Result<RecordBatch, AppError> {
    let arrow_fields: Vec<Field> = schema
        .fields()
        .map(|f| {
            let arrow_type = match f.data_type() {
                DeltaDataType::Primitive(p) => delta_primitive_to_arrow(p)?,
                other => {
                    return Err(AppError::Schema(format!(
                        "Complex type not supported for column '{}': {:?}",
                        f.name(),
                        other
                    )))
                }
            };
            Ok(Field::new(f.name(), arrow_type, f.is_nullable()))
        })
        .collect::<Result<_, AppError>>()?;

    let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));

    let arrays: Vec<Arc<dyn Array>> = schema
        .fields()
        .map(|field| build_array(field.name(), field.data_type(), records))
        .collect::<Result<_, AppError>>()?;

    Ok(RecordBatch::try_new(arrow_schema, arrays)?)
}

fn build_array(
    col: &str,
    dt: &DeltaDataType,
    records: &[Record],
) -> Result<Arc<dyn Array>, AppError> {
    match dt {
        DeltaDataType::Primitive(p) => match p {
            PrimitiveType::String => {
                let vals: Vec<Option<&str>> = records
                    .iter()
                    .map(|r| r.get(col).and_then(|v| v.as_str()))
                    .collect();
                Ok(Arc::new(StringArray::from(vals)))
            }
            PrimitiveType::Integer | PrimitiveType::Byte | PrimitiveType::Short => {
                let vals: Vec<Option<i32>> = records
                    .iter()
                    .map(|r| r.get(col).and_then(|v| v.as_i64()).map(|n| n as i32))
                    .collect();
                Ok(Arc::new(Int32Array::from(vals)))
            }
            PrimitiveType::Long => {
                let vals: Vec<Option<i64>> = records
                    .iter()
                    .map(|r| r.get(col).and_then(|v| v.as_i64()))
                    .collect();
                Ok(Arc::new(Int64Array::from(vals)))
            }
            PrimitiveType::Float | PrimitiveType::Double => {
                let vals: Vec<Option<f64>> = records
                    .iter()
                    .map(|r| r.get(col).and_then(|v| v.as_f64()))
                    .collect();
                Ok(Arc::new(Float64Array::from(vals)))
            }
            PrimitiveType::Boolean => {
                let vals: Vec<Option<bool>> = records
                    .iter()
                    .map(|r| r.get(col).and_then(|v| v.as_bool()))
                    .collect();
                Ok(Arc::new(BooleanArray::from(vals)))
            }
            PrimitiveType::Timestamp => {
                let vals: Vec<Option<i64>> = records
                    .iter()
                    .map(|r| r.get(col).and_then(|v| v.as_i64()))
                    .collect();
                Ok(Arc::new(
                    TimestampMicrosecondArray::from(vals).with_timezone("UTC"),
                ))
            }
            PrimitiveType::TimestampNtz => {
                let vals: Vec<Option<i64>> = records
                    .iter()
                    .map(|r| r.get(col).and_then(|v| v.as_i64()))
                    .collect();
                Ok(Arc::new(TimestampMicrosecondArray::from(vals)))
            }
            PrimitiveType::Date => {
                let vals: Vec<Option<i32>> = records
                    .iter()
                    .map(|r| r.get(col).and_then(|v| v.as_i64()).map(|n| n as i32))
                    .collect();
                Ok(Arc::new(Date32Array::from(vals)))
            }
            other => Err(AppError::Schema(format!(
                "Unsupported primitive type '{other:?}' for column '{col}'"
            ))),
        },
        other => Err(AppError::Schema(format!(
            "Complex type '{other:?}' not supported for column '{col}'"
        ))),
    }
}

// ── Adapter ───────────────────────────────────────────────────────────────────

/// Outbound adapter — implements `TableRepository` using delta-rs + Amazon S3.
pub struct DeltaTableRepository;

/// Delete every S3 object under the given `s3://bucket/prefix` URI.
/// Returns the number of objects deleted.
async fn delete_s3_prefix(table_uri: &str) -> Result<usize, AppError> {
    let stripped = table_uri
        .strip_prefix("s3://")
        .ok_or_else(|| AppError::Schema(format!("Invalid S3 URI: {table_uri}")))?;
    let (bucket, prefix_raw) = stripped
        .split_once('/')
        .ok_or_else(|| AppError::Schema(format!("Invalid S3 URI: {table_uri}")))?;
    let prefix = if prefix_raw.ends_with('/') {
        prefix_raw.to_string()
    } else {
        format!("{prefix_raw}/")
    };

    let config = aws_config::load_from_env().await;
    let s3_conf = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .build();
    let client = aws_sdk_s3::Client::from_conf(s3_conf);

    let mut objects_deleted = 0usize;
    let mut continuation_token: Option<String> = None;

    loop {
        let mut list_req = client.list_objects_v2().bucket(bucket).prefix(&prefix);
        if let Some(token) = continuation_token {
            list_req = list_req.continuation_token(token);
        }
        let resp = list_req
            .send()
            .await
            .map_err(|e| AppError::Schema(e.to_string()))?;

        let identifiers: Vec<aws_sdk_s3::types::ObjectIdentifier> = resp
            .contents()
            .iter()
            .filter_map(|obj| {
                let key = obj.key()?;
                aws_sdk_s3::types::ObjectIdentifier::builder()
                    .key(key)
                    .build()
                    .ok()
            })
            .collect();

        if !identifiers.is_empty() {
            let count = identifiers.len();
            let delete = aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(identifiers))
                .build()
                .map_err(|e| AppError::Schema(e.to_string()))?;
            client
                .delete_objects()
                .bucket(bucket)
                .delete(delete)
                .send()
                .await
                .map_err(|e| AppError::Schema(e.to_string()))?;
            objects_deleted += count;
        }

        if resp.is_truncated().unwrap_or(false) {
            continuation_token = resp.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }

    Ok(objects_deleted)
}

#[async_trait]
impl TableRepository for DeltaTableRepository {
    async fn create_table(&self, cmd: CreateTableCommand) -> Result<CreateTableResult, AppError> {
        let columns: Vec<deltalake::kernel::StructField> = cmd
            .schema
            .iter()
            .map(|f| {
                let dt = parse_delta_type(&f.data_type)?;
                Ok(deltalake::kernel::StructField::new(
                    f.name.clone(),
                    dt,
                    f.nullable,
                ))
            })
            .collect::<Result<_, AppError>>()?;

        if cmd.overwrite {
            delete_s3_prefix(&cmd.table_uri).await?;
        }

        let table = DeltaOps::try_from_uri_with_storage_options(
            &cmd.table_uri,
            s3_storage_options(),
        )
        .await?
        .create()
        .with_columns(columns)
        .with_partition_columns(cmd.partition_columns)
        .with_save_mode(if cmd.overwrite {
            SaveMode::ErrorIfExists
        } else {
            SaveMode::Ignore
        })
        .await?;

        Ok(CreateTableResult {
            version: table.version(),
            message: if cmd.overwrite {
                "Table recreated"
            } else {
                "Table created (or already existed)"
            }
            .into(),
        })
    }

    async fn insert(&self, cmd: InsertCommand) -> Result<InsertResult, AppError> {
        let table =
            open_table_with_storage_options(&cmd.table_uri, s3_storage_options()).await?;
        let schema = table.get_schema()?;
        let batch = records_to_record_batch(&cmd.records, &schema)?;
        let num_rows = batch.num_rows();

        let table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .with_partition_columns(cmd.partition_columns)
            .await?;

        Ok(InsertResult {
            version: table.version(),
            rows_written: num_rows,
        })
    }

    async fn upsert(&self, cmd: UpsertCommand) -> Result<UpsertResult, AppError> {
        let table =
            open_table_with_storage_options(&cmd.table_uri, s3_storage_options()).await?;
        let schema = table.get_schema()?;
        let source_batch = records_to_record_batch(&cmd.records, &schema)?;

        let all_columns: Vec<String> =
            schema.fields().map(|f| f.name().to_string()).collect();
        let update_columns: Vec<String> = all_columns
            .iter()
            .filter(|c| !cmd.match_columns.contains(c))
            .cloned()
            .collect();

        let ctx = SessionContext::new();
        let source_df = ctx.read_batch(source_batch)?;

        let (table, metrics) = DeltaOps(table)
            .merge(source_df, cmd.merge_predicate)
            .with_source_alias("source")
            .with_target_alias("target")
            .when_matched_update(|mut update| {
                for col in &update_columns {
                    update = update.update(col, format!("source.{col}"));
                }
                update
            })?
            .when_not_matched_insert(|mut insert| {
                for col in &all_columns {
                    insert = insert.set(col, format!("source.{col}"));
                }
                insert
            })?
            .await?;

        Ok(UpsertResult {
            version: table.version(),
            num_source_rows: metrics.num_source_rows,
            num_target_rows_inserted: metrics.num_target_rows_inserted,
            num_target_rows_updated: metrics.num_target_rows_updated,
            num_target_rows_deleted: metrics.num_target_rows_deleted,
            num_output_rows: metrics.num_output_rows,
        })
    }

    async fn vacuum(&self, cmd: VacuumCommand) -> Result<VacuumResult, AppError> {
        let table =
            open_table_with_storage_options(&cmd.table_uri, s3_storage_options()).await?;

        let (table, metrics) = DeltaOps(table)
            .vacuum()
            .with_retention_period(Duration::hours(cmd.retention_hours))
            .with_dry_run(cmd.dry_run)
            .await?;

        Ok(VacuumResult {
            version: table.version(),
            dry_run: metrics.dry_run,
            files_deleted: metrics.files_deleted,
        })
    }

    async fn optimize(&self, cmd: OptimizeCommand) -> Result<OptimizeResult, AppError> {
        let optimize_type = if cmd.zorder_columns.is_empty() {
            OptimizeType::Compact
        } else {
            OptimizeType::ZOrder(cmd.zorder_columns)
        };

        let table =
            open_table_with_storage_options(&cmd.table_uri, s3_storage_options()).await?;

        let mut builder = DeltaOps(table).optimize().with_type(optimize_type);

        if let Some(size) = cmd.target_file_size {
            builder = builder.with_target_size(size as i64);
        }

        let (table, metrics) = builder.await?;

        Ok(OptimizeResult {
            version: table.version(),
            files_added: metrics.num_files_added as usize,
            files_removed: metrics.num_files_removed as usize,
            total_files_skipped: metrics.total_files_skipped,
        })
    }

    async fn time_travel(&self, cmd: TimeTravelCommand) -> Result<TimeTravelResult, AppError> {
        let mut table = DeltaTableBuilder::from_uri(&cmd.table_uri)
            .with_storage_options(s3_storage_options())
            .build()
            .map_err(AppError::Delta)?;

        if let Some(version) = cmd.version {
            table.load_version(version).await?;
        } else if let Some(ts_str) = cmd.timestamp {
            let dt: DateTime<Utc> = ts_str
                .parse()
                .map_err(|_| AppError::Timestamp(ts_str.clone()))?;
            table.load_with_datetime(dt).await?;
        }

        let schema = table.get_schema()?;

        Ok(TimeTravelResult {
            version: table.version(),
            num_files: table.get_files_count(),
            schema: SchemaResult {
                version: table.version(),
                num_files: table.get_files_count(),
                fields: delta_schema_to_results(&schema),
            },
        })
    }

    async fn table_history(&self, cmd: TableHistoryCommand) -> Result<HistoryResult, AppError> {
        let table =
            open_table_with_storage_options(&cmd.table_uri, s3_storage_options()).await?;
        let current_version = table.version();
        let history = table.history(cmd.limit).await?;

        // history() returns commits newest-first; derive version by offset from current.
        // CommitInfo in kernel has no `version` field — it is implied by log filename.
        let entries: Vec<CommitEntry> = history
            .iter()
            .enumerate()
            .map(|(i, c)| CommitEntry {
                version: current_version - i as i64,
                timestamp_ms: c.timestamp.unwrap_or(0),
                operation: c.operation.clone().unwrap_or_default(),
                operation_parameters: c
                    .operation_parameters
                    .as_ref()
                    .map(|m| {
                        m.iter()
                            .map(|(k, v)| (k.clone(), v.to_string()))
                            .collect()
                    })
                    .unwrap_or_default(),
            })
            .collect();

        Ok(HistoryResult {
            total_commits: entries.len(),
            history: entries,
        })
    }

    async fn get_schema(&self, cmd: GetSchemaCommand) -> Result<SchemaResult, AppError> {
        let table =
            open_table_with_storage_options(&cmd.table_uri, s3_storage_options()).await?;
        let schema = table.get_schema()?;

        Ok(SchemaResult {
            version: table.version(),
            num_files: table.get_files_count(),
            fields: delta_schema_to_results(&schema),
        })
    }

    async fn delete_table(&self, cmd: DeleteTableCommand) -> Result<DeleteTableResult, AppError> {
        let objects_deleted = delete_s3_prefix(&cmd.table_uri).await?;
        Ok(DeleteTableResult {
            table_uri: cmd.table_uri,
            objects_deleted,
        })
    }
}

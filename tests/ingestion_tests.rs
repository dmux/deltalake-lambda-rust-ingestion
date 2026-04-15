/// Integration tests for `IngestionService` using `InMemoryTableRepository`.
///
/// No S3, no AWS credentials, no delta-rs — the full application core is
/// exercised through the same port that production uses, just with a
/// different adapter on the driven side.
use std::collections::HashMap;
use std::sync::Arc;

use deltalake_lambda_rust_ingestion::adapters::outbound::in_memory_repository::InMemoryTableRepository;
use deltalake_lambda_rust_ingestion::application::ingestion_service::IngestionService;
use deltalake_lambda_rust_ingestion::domain::commands::{
    CreateTableCommand, FieldDef, GetSchemaCommand, InsertCommand, OptimizeCommand,
    TableHistoryCommand, TimeTravelCommand, UpsertCommand, VacuumCommand,
};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn make_service() -> IngestionService {
    let repo = Arc::new(InMemoryTableRepository::new());
    IngestionService::new(repo)
}

fn basic_schema() -> Vec<FieldDef> {
    vec![
        FieldDef { name: "id".into(),         data_type: "long".into(),   nullable: false },
        FieldDef { name: "event_type".into(),  data_type: "string".into(), nullable: true },
        FieldDef { name: "value".into(),       data_type: "double".into(), nullable: true },
    ]
}

fn make_record(id: i64, event_type: &str, value: f64) -> HashMap<String, serde_json::Value> {
    [
        ("id".into(),         serde_json::json!(id)),
        ("event_type".into(), serde_json::json!(event_type)),
        ("value".into(),      serde_json::json!(value)),
    ]
    .into_iter()
    .collect()
}

const URI: &str = "mem://test/events";

// ── create_table ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_table_success() {
    let svc = make_service();
    let result = svc
        .create_table(CreateTableCommand {
            table_uri: URI.into(),
            schema: basic_schema(),
            partition_columns: vec!["event_type".into()],
        })
        .await
        .unwrap();

    assert_eq!(result.version, 0);
    assert!(result.message.contains("created"));
}

#[tokio::test]
async fn test_create_table_idempotent() {
    let svc = make_service();
    let cmd = || CreateTableCommand {
        table_uri: URI.into(),
        schema: basic_schema(),
        partition_columns: vec![],
    };

    svc.create_table(cmd()).await.unwrap();
    let second = svc.create_table(cmd()).await.unwrap();
    // Second call must succeed and report the existing version
    assert_eq!(second.version, 0);
    assert!(second.message.contains("existed") || second.message.contains("created"));
}

#[tokio::test]
async fn test_create_table_empty_schema_rejected() {
    let svc = make_service();
    let err = svc
        .create_table(CreateTableCommand {
            table_uri: URI.into(),
            schema: vec![],
            partition_columns: vec![],
        })
        .await
        .unwrap_err();

    assert!(err.to_string().contains("schema"));
}

// ── insert ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_insert_appends_records_and_bumps_version() {
    let svc = make_service();
    svc.create_table(CreateTableCommand {
        table_uri: URI.into(),
        schema: basic_schema(),
        partition_columns: vec![],
    })
    .await
    .unwrap();

    let r1 = svc
        .insert(InsertCommand {
            table_uri: URI.into(),
            records: vec![make_record(1, "click", 1.5)],
            partition_columns: vec![],
        })
        .await
        .unwrap();

    assert_eq!(r1.rows_written, 1);
    assert_eq!(r1.version, 1);

    let r2 = svc
        .insert(InsertCommand {
            table_uri: URI.into(),
            records: vec![make_record(2, "view", 2.0), make_record(3, "click", 0.5)],
            partition_columns: vec![],
        })
        .await
        .unwrap();

    assert_eq!(r2.rows_written, 2);
    assert_eq!(r2.version, 2);
}

#[tokio::test]
async fn test_insert_empty_records_is_noop() {
    let svc = make_service();
    svc.create_table(CreateTableCommand {
        table_uri: URI.into(),
        schema: basic_schema(),
        partition_columns: vec![],
    })
    .await
    .unwrap();

    let r = svc
        .insert(InsertCommand {
            table_uri: URI.into(),
            records: vec![],
            partition_columns: vec![],
        })
        .await
        .unwrap();

    assert_eq!(r.rows_written, 0);
    // Version must not have incremented
    let schema = svc
        .get_schema(GetSchemaCommand { table_uri: URI.into() })
        .await
        .unwrap();
    assert_eq!(schema.version, 0);
}

// ── upsert ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_upsert_updates_existing_and_inserts_new() {
    let svc = make_service();
    svc.create_table(CreateTableCommand {
        table_uri: URI.into(),
        schema: basic_schema(),
        partition_columns: vec![],
    })
    .await
    .unwrap();

    // Seed two records
    svc.insert(InsertCommand {
        table_uri: URI.into(),
        records: vec![make_record(1, "click", 1.5), make_record(2, "view", 2.0)],
        partition_columns: vec![],
    })
    .await
    .unwrap();

    // Upsert: update id=1, insert id=3
    let r = svc
        .upsert(UpsertCommand {
            table_uri: URI.into(),
            records: vec![
                make_record(1, "click", 99.9), // should update
                make_record(3, "purchase", 49.0), // should insert
            ],
            merge_predicate: "target.id = source.id".into(),
            match_columns: vec!["id".into()],
        })
        .await
        .unwrap();

    assert_eq!(r.num_source_rows, 2);
    assert_eq!(r.num_target_rows_updated, 1);
    assert_eq!(r.num_target_rows_inserted, 1);
    assert_eq!(r.num_output_rows, 3); // 2 original + 1 new
}

#[tokio::test]
async fn test_upsert_empty_predicate_rejected() {
    let svc = make_service();
    svc.create_table(CreateTableCommand {
        table_uri: URI.into(),
        schema: basic_schema(),
        partition_columns: vec![],
    })
    .await
    .unwrap();

    let err = svc
        .upsert(UpsertCommand {
            table_uri: URI.into(),
            records: vec![make_record(1, "click", 1.0)],
            merge_predicate: "".into(),
            match_columns: vec!["id".into()],
        })
        .await
        .unwrap_err();

    assert!(err.to_string().contains("merge_predicate"));
}

#[tokio::test]
async fn test_upsert_empty_records_is_noop() {
    let svc = make_service();
    svc.create_table(CreateTableCommand {
        table_uri: URI.into(),
        schema: basic_schema(),
        partition_columns: vec![],
    })
    .await
    .unwrap();

    let r = svc
        .upsert(UpsertCommand {
            table_uri: URI.into(),
            records: vec![],
            merge_predicate: "target.id = source.id".into(),
            match_columns: vec!["id".into()],
        })
        .await
        .unwrap();

    assert_eq!(r.num_source_rows, 0);
    assert_eq!(r.num_output_rows, 0);
}

// ── get_schema ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_schema_returns_declared_fields() {
    let svc = make_service();
    svc.create_table(CreateTableCommand {
        table_uri: URI.into(),
        schema: basic_schema(),
        partition_columns: vec![],
    })
    .await
    .unwrap();

    let s = svc
        .get_schema(GetSchemaCommand { table_uri: URI.into() })
        .await
        .unwrap();

    assert_eq!(s.fields.len(), 3);
    assert_eq!(s.fields[0].name, "id");
    assert_eq!(s.fields[0].data_type, "long");
    assert!(!s.fields[0].nullable);
    assert_eq!(s.fields[1].name, "event_type");
    assert_eq!(s.fields[2].name, "value");
}

#[tokio::test]
async fn test_get_schema_on_missing_table_returns_error() {
    let svc = make_service();
    let err = svc
        .get_schema(GetSchemaCommand {
            table_uri: "mem://does-not-exist".into(),
        })
        .await
        .unwrap_err();

    assert!(err.to_string().to_lowercase().contains("delta") || err.to_string().contains("does-not-exist"));
}

// ── table_history ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_history_grows_with_each_operation() {
    let svc = make_service();
    svc.create_table(CreateTableCommand {
        table_uri: URI.into(),
        schema: basic_schema(),
        partition_columns: vec![],
    })
    .await
    .unwrap();

    svc.insert(InsertCommand {
        table_uri: URI.into(),
        records: vec![make_record(1, "click", 1.0)],
        partition_columns: vec![],
    })
    .await
    .unwrap();

    svc.insert(InsertCommand {
        table_uri: URI.into(),
        records: vec![make_record(2, "view", 2.0)],
        partition_columns: vec![],
    })
    .await
    .unwrap();

    let h = svc
        .table_history(TableHistoryCommand {
            table_uri: URI.into(),
            limit: None,
        })
        .await
        .unwrap();

    // CREATE TABLE + 2 inserts = 3 commits
    assert_eq!(h.total_commits, 3);
    // Newest first
    assert_eq!(h.history[0].version, 2);
    assert_eq!(h.history[0].operation, "WRITE");
    assert_eq!(h.history[2].version, 0);
    assert_eq!(h.history[2].operation, "CREATE TABLE");
}

#[tokio::test]
async fn test_history_limit_is_respected() {
    let svc = make_service();
    svc.create_table(CreateTableCommand {
        table_uri: URI.into(),
        schema: basic_schema(),
        partition_columns: vec![],
    })
    .await
    .unwrap();

    for i in 0..5 {
        svc.insert(InsertCommand {
            table_uri: URI.into(),
            records: vec![make_record(i, "click", 1.0)],
            partition_columns: vec![],
        })
        .await
        .unwrap();
    }

    let h = svc
        .table_history(TableHistoryCommand {
            table_uri: URI.into(),
            limit: Some(3),
        })
        .await
        .unwrap();

    assert_eq!(h.total_commits, 3);
}

// ── time_travel ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_time_travel_by_version() {
    let svc = make_service();
    svc.create_table(CreateTableCommand {
        table_uri: URI.into(),
        schema: basic_schema(),
        partition_columns: vec![],
    })
    .await
    .unwrap();

    svc.insert(InsertCommand {
        table_uri: URI.into(),
        records: vec![make_record(1, "click", 1.0)],
        partition_columns: vec![],
    })
    .await
    .unwrap();

    // Travel back to version 0 (empty table, just created)
    let r = svc
        .time_travel(TimeTravelCommand {
            table_uri: URI.into(),
            version: Some(0),
            timestamp: None,
        })
        .await
        .unwrap();

    assert_eq!(r.version, 0);
    assert_eq!(r.num_files, 0); // empty at creation
}

#[tokio::test]
async fn test_time_travel_by_timestamp() {
    let svc = make_service();
    svc.create_table(CreateTableCommand {
        table_uri: URI.into(),
        schema: basic_schema(),
        partition_columns: vec![],
    })
    .await
    .unwrap();

    // Travel to a far-future timestamp — should resolve to the latest snapshot
    let r = svc
        .time_travel(TimeTravelCommand {
            table_uri: URI.into(),
            version: None,
            timestamp: Some("2099-01-01T00:00:00Z".into()),
        })
        .await
        .unwrap();

    assert_eq!(r.version, 0); // only version 0 exists
}

#[tokio::test]
async fn test_time_travel_requires_version_or_timestamp() {
    let svc = make_service();
    svc.create_table(CreateTableCommand {
        table_uri: URI.into(),
        schema: basic_schema(),
        partition_columns: vec![],
    })
    .await
    .unwrap();

    let err = svc
        .time_travel(TimeTravelCommand {
            table_uri: URI.into(),
            version: None,
            timestamp: None,
        })
        .await
        .unwrap_err();

    assert!(err.to_string().contains("version") || err.to_string().contains("timestamp"));
}

#[tokio::test]
async fn test_time_travel_to_unknown_version_returns_error() {
    let svc = make_service();
    svc.create_table(CreateTableCommand {
        table_uri: URI.into(),
        schema: basic_schema(),
        partition_columns: vec![],
    })
    .await
    .unwrap();

    let err = svc
        .time_travel(TimeTravelCommand {
            table_uri: URI.into(),
            version: Some(999),
            timestamp: None,
        })
        .await
        .unwrap_err();

    assert!(err.to_string().contains("999") || err.to_string().contains("not found"));
}

// ── vacuum ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_vacuum_dry_run_lists_files_without_deleting() {
    let svc = make_service();
    svc.create_table(CreateTableCommand {
        table_uri: URI.into(),
        schema: basic_schema(),
        partition_columns: vec![],
    })
    .await
    .unwrap();

    let r1 = svc
        .vacuum(VacuumCommand {
            table_uri: URI.into(),
            retention_hours: 168,
            dry_run: true,
        })
        .await
        .unwrap();

    assert!(r1.dry_run);
    assert_eq!(r1.files_deleted.len(), 2); // the two simulated orphans

    // Second dry-run should still see the files (nothing was deleted)
    let r2 = svc
        .vacuum(VacuumCommand {
            table_uri: URI.into(),
            retention_hours: 168,
            dry_run: true,
        })
        .await
        .unwrap();

    assert_eq!(r2.files_deleted.len(), 2);
}

#[tokio::test]
async fn test_vacuum_real_run_cleans_files() {
    let svc = make_service();
    svc.create_table(CreateTableCommand {
        table_uri: URI.into(),
        schema: basic_schema(),
        partition_columns: vec![],
    })
    .await
    .unwrap();

    let r1 = svc
        .vacuum(VacuumCommand {
            table_uri: URI.into(),
            retention_hours: 168,
            dry_run: false,
        })
        .await
        .unwrap();

    assert!(!r1.dry_run);
    assert_eq!(r1.files_deleted.len(), 2);

    // After real vacuum, orphaned files are gone
    let r2 = svc
        .vacuum(VacuumCommand {
            table_uri: URI.into(),
            retention_hours: 168,
            dry_run: false,
        })
        .await
        .unwrap();

    assert_eq!(r2.files_deleted.len(), 0);
}

// ── optimize ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_optimize_compacts_files() {
    let svc = make_service();
    svc.create_table(CreateTableCommand {
        table_uri: URI.into(),
        schema: basic_schema(),
        partition_columns: vec![],
    })
    .await
    .unwrap();

    // Insert 3 times to produce 3 simulated files
    for i in 0..3 {
        svc.insert(InsertCommand {
            table_uri: URI.into(),
            records: vec![make_record(i, "click", 1.0)],
            partition_columns: vec![],
        })
        .await
        .unwrap();
    }

    let r = svc
        .optimize(OptimizeCommand {
            table_uri: URI.into(),
            zorder_columns: vec![],
            target_file_size: None,
        })
        .await
        .unwrap();

    // 3 files → 1: removed=2, added=1
    assert_eq!(r.files_removed, 2);
    assert_eq!(r.files_added, 1);

    // Subsequent optimize on already-compact table is a no-op
    let r2 = svc
        .optimize(OptimizeCommand {
            table_uri: URI.into(),
            zorder_columns: vec!["event_type".into()],
            target_file_size: None,
        })
        .await
        .unwrap();

    assert_eq!(r2.files_removed, 0);
    assert_eq!(r2.files_added, 0);
}

// ── end-to-end scenario ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_full_lifecycle() {
    let svc = make_service();

    // 1. Create
    svc.create_table(CreateTableCommand {
        table_uri: URI.into(),
        schema: basic_schema(),
        partition_columns: vec!["event_type".into()],
    })
    .await
    .unwrap();

    // 2. Insert batch
    svc.insert(InsertCommand {
        table_uri: URI.into(),
        records: vec![make_record(1, "click", 1.5), make_record(2, "view", 2.0)],
        partition_columns: vec!["event_type".into()],
    })
    .await
    .unwrap();

    // 3. Upsert (update id=1, insert id=3)
    let upsert_r = svc
        .upsert(UpsertCommand {
            table_uri: URI.into(),
            records: vec![make_record(1, "click", 99.9), make_record(3, "purchase", 49.0)],
            merge_predicate: "target.id = source.id".into(),
            match_columns: vec!["id".into()],
        })
        .await
        .unwrap();

    assert_eq!(upsert_r.num_target_rows_updated, 1);
    assert_eq!(upsert_r.num_target_rows_inserted, 1);

    // 4. Time travel — read state before upsert (version 1)
    let tt = svc
        .time_travel(TimeTravelCommand {
            table_uri: URI.into(),
            version: Some(1),
            timestamp: None,
        })
        .await
        .unwrap();
    assert_eq!(tt.version, 1);

    // 5. History — should have 3 commits (CREATE + WRITE + MERGE)
    let history = svc
        .table_history(TableHistoryCommand {
            table_uri: URI.into(),
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(history.total_commits, 3);

    // 6. Optimize
    svc.optimize(OptimizeCommand {
        table_uri: URI.into(),
        zorder_columns: vec!["event_type".into()],
        target_file_size: None,
    })
    .await
    .unwrap();

    // 7. Vacuum (dry run)
    let vac = svc
        .vacuum(VacuumCommand {
            table_uri: URI.into(),
            retention_hours: 168,
            dry_run: true,
        })
        .await
        .unwrap();
    assert!(vac.dry_run);

    // 8. Schema still correct
    let schema = svc
        .get_schema(GetSchemaCommand { table_uri: URI.into() })
        .await
        .unwrap();
    assert_eq!(schema.fields.len(), 3);
}

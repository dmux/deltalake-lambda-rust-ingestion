use std::sync::Arc;

use lambda_runtime::{run, service_fn, Error};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use deltalake_lambda_rust_ingestion::adapters::inbound::lambda::handle;
use deltalake_lambda_rust_ingestion::adapters::outbound::delta_repository::DeltaTableRepository;
use deltalake_lambda_rust_ingestion::application::ingestion_service::IngestionService;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Structured JSON logs for CloudWatch Logs Insights.
    tracing_subscriber::registry()
        .with(
            EnvFilter::from_default_env()
                .add_directive("deltalake_lambda_rust_ingestion=info".parse().unwrap()),
        )
        .with(tracing_subscriber::fmt::layer().json())
        .init();

    // Register S3 object store handlers — must happen before any s3:// URI
    // is opened. Called once at cold start; Lambda reuses the process across
    // warm invocations.
    deltalake::aws::register_handlers(None);

    // Wire up dependency injection:
    //   InMemoryTableRepository  ← swapped in during tests
    //   DeltaTableRepository     ← used in production
    let repo = Arc::new(DeltaTableRepository);
    let service = Arc::new(IngestionService::new(repo));

    run(service_fn(move |event| {
        let svc = service.clone();
        async move { handle(event, svc).await }
    }))
    .await
}

use chronos_feat::api::{create_router, AppState, SchemaRegistry};
use chronos_feat::ingest::{IngestionPipeline, IngestionPipelineConfig, WindowFeatureSpec, AggregationOp};
use chronos_feat::store::{InMemoryOnlineStore, LakehouseParquetBridge, ParquetBridgeConfig, PointInTimeAsOfJoinEngine};
use parking_lot::RwLock;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "chronos_feat=debug,tower_http=debug,info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting Chronos Distributed Real-Time ML Feature Store...");

    // 2. Load configurations
    let port: u16 = std::env::var("CHRONOS_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);

    let lakehouse_dir = std::env::var("CHRONOS_DATA_DIR")
        .unwrap_or_else(|_| "./data/lakehouse".to_string());

    let ttl_secs: i64 = std::env::var("CHRONOS_TTL_SECS")
        .ok()
        .and_then(|t| t.parse().ok())
        .unwrap_or(86400 * 7); // 7 days

    // 3. Initialize dual-tier stores
    let online_store = Arc::new(InMemoryOnlineStore::new(Some(ttl_secs * 1_000_000)));

    let bridge_config = ParquetBridgeConfig {
        base_path: PathBuf::from(lakehouse_dir),
        batch_flush_size: 1000,
        compression: parquet::basic::Compression::SNAPPY,
    };
    let parquet_bridge = Arc::new(LakehouseParquetBridge::new(bridge_config)?);

    let schema_registry = Arc::new(SchemaRegistry::new());
    let asof_engine = Arc::new(RwLock::new(PointInTimeAsOfJoinEngine::new(Some(ttl_secs * 1_000_000))));

    // 4. Initialize streaming ingestion pipeline
    let pipeline_config = IngestionPipelineConfig::default();
    let (pipeline, _pipeline_worker) = IngestionPipeline::new(
        pipeline_config,
        online_store.clone(),
        parquet_bridge.clone(),
    );
    let pipeline = Arc::new(pipeline);

    // Register default sliding window aggregations
    pipeline.register_window_spec(WindowFeatureSpec {
        name: "tx_amount_sum_5m".into(),
        source_field: "amount".into(),
        op: AggregationOp::Sum,
        window_duration_micros: 300_000_000, // 5 min
        bucket_resolution_micros: 1_000_000,  // 1 sec
        half_life_micros: None,
    });
    pipeline.register_window_spec(WindowFeatureSpec {
        name: "tx_count_1h".into(),
        source_field: "amount".into(),
        op: AggregationOp::Count,
        window_duration_micros: 3_600_000_000, // 1 hour
        bucket_resolution_micros: 10_000_000,  // 10 sec
        half_life_micros: None,
    });

    // 5. Periodic Lakehouse flush task
    let bridge_clone = parquet_bridge.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            match bridge_clone.flush_all() {
                Ok(count) => {
                    if count > 0 {
                        info!("Periodic flush: flushed {} events to Parquet lakehouse", count);
                    }
                }
                Err(e) => error!("Error in periodic Parquet lakehouse flush: {:?}", e),
            }
        }
    });

    // 6. Periodic TTL eviction task
    let store_clone = online_store.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(300));
        loop {
            interval.tick().await;
            let now = chrono::Utc::now().timestamp_micros();
            let evicted = store_clone.evict_expired(now);
            if evicted > 0 {
                info!("TTL cleanup: evicted {} expired entities", evicted);
            }
        }
    });

    // 7. Assemble application state and router
    let app_state = Arc::new(AppState {
        online_store: online_store.clone(),
        schema_registry: schema_registry.clone(),
        ingestion_pipeline: pipeline.clone(),
        asof_engine: asof_engine.clone(),
    });

    let app = create_router(app_state);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("Chronos feature store running on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

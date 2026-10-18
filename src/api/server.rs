use crate::api::handlers::{
    asof_join_handler, get_online_features_handler, get_schema_handler, health_check_handler,
    ingest_event_handler, metrics_handler, register_schema_handler, AppState,
};
use axum::routing::{get, post};
use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::info;

pub fn create_router(state: Arc<AppState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .route("/health", get(health_check_handler))
        .route("/metrics", get(metrics_handler))
        .route("/v1/events", post(ingest_event_handler))
        .route("/v1/features/online", post(get_online_features_handler))
        .route("/v1/features/historical", post(asof_join_handler))
        .route("/v1/features/schema", post(register_schema_handler))
        .route("/v1/features/schema/:name", get(get_schema_handler))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}

pub async fn run_server(state: Arc<AppState>, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let app = create_router(state);
    info!("Chronos ML Feature Store HTTP/gRPC server listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

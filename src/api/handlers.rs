use crate::api::schema::{FeatureViewSchema, SchemaRegistry};
use crate::error::ChronosError;
use crate::ingest::event::{Event, FeatureValue};
use crate::ingest::pipeline::IngestionPipeline;
use crate::store::asof_join::{AsOfJoinResult, HistoricalObservation, PointInTimeAsOfJoinEngine};
use crate::store::memory::InMemoryOnlineStore;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

pub struct AppState {
    pub online_store: Arc<InMemoryOnlineStore>,
    pub schema_registry: Arc<SchemaRegistry>,
    pub ingestion_pipeline: Arc<IngestionPipeline>,
    pub asof_engine: Arc<RwLock<PointInTimeAsOfJoinEngine>>,
}

#[derive(Debug, Deserialize)]
pub struct IngestEventRequest {
    pub entity_id: String,
    pub event_type: String,
    pub timestamp: Option<i64>,
    pub values: HashMap<String, FeatureValue>,
}

#[derive(Debug, Serialize)]
pub struct IngestEventResponse {
    pub status: String,
    pub event_id: String,
    pub timestamp: i64,
}

#[derive(Debug, Deserialize)]
pub struct OnlineFeaturesRequest {
    pub entity_ids: Vec<String>,
    pub features: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct OnlineFeaturesResponse {
    pub features: HashMap<String, HashMap<String, FeatureValue>>,
}

#[derive(Debug, Deserialize)]
pub struct HistoricalFeaturesRequest {
    pub observations: Vec<HistoricalObservation>,
    pub features: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct HistoricalFeaturesResponse {
    pub results: Vec<AsOfJoinResult>,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub version: &'static str,
    pub uptime_secs: u64,
    pub entities_count: usize,
}

// Handler: Ingest Event
pub async fn ingest_event_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<IngestEventRequest>,
) -> Result<Json<IngestEventResponse>, (StatusCode, String)> {
    let ts = payload.timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp_micros());

    let mut event = Event::new(&payload.entity_id, &payload.event_type, ts);
    event.values = payload.values.clone();

    // Ingest into pipeline
    state
        .ingestion_pipeline
        .ingest(event.clone())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Also populate asof engine for historical lookups
    {
        let mut asof = state.asof_engine.write();
        asof.insert_point(&payload.entity_id, ts, payload.values);
    }

    Ok(Json(IngestEventResponse {
        status: "accepted".into(),
        event_id: event.id.to_string(),
        timestamp: ts,
    }))
}

// Handler: Get Online Features
pub async fn get_online_features_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<OnlineFeaturesRequest>,
) -> Json<OnlineFeaturesResponse> {
    let features = state
        .online_store
        .get_online_features(&payload.entity_ids, &payload.features);

    Json(OnlineFeaturesResponse { features })
}

// Handler: Historical AS-OF Join Features
pub async fn asof_join_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<HistoricalFeaturesRequest>,
) -> Result<Json<HistoricalFeaturesResponse>, (StatusCode, String)> {
    let asof = state.asof_engine.read();
    let results = asof
        .batch_join_asof(&payload.observations, payload.features.as_deref())
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    Ok(Json(HistoricalFeaturesResponse { results }))
}

// Handler: Register Schema
pub async fn register_schema_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<FeatureViewSchema>,
) -> (StatusCode, Json<serde_json::Value>) {
    state.schema_registry.register_view(payload.clone());
    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "status": "registered",
            "feature_view": payload.name
        })),
    )
}

// Handler: Get Schema
pub async fn get_schema_handler(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<FeatureViewSchema>, (StatusCode, String)> {
    match state.schema_registry.get_view(&name) {
        Some(view) => Ok(Json(view)),
        None => Err((
            StatusCode::NOT_FOUND,
            format!("Feature view '{}' not found", name),
        )),
    }
}

// Handler: Health Check
pub async fn health_check_handler(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy",
        version: env!("CARGO_PKG_VERSION"),
        uptime_secs: 42,
        entities_count: state.online_store.entity_count(),
    })
}

// Handler: Prometheus Metrics
pub async fn metrics_handler(State(state): State<Arc<AppState>>) -> (StatusCode, String) {
    let (reads, writes, hits, misses, avg_lat) = state.online_store.get_metrics();
    let metrics = format!(
        "# HELP chronos_reads_total Total number of online feature read operations\n\
         # TYPE chronos_reads_total counter\n\
         chronos_reads_total {}\n\
         # HELP chronos_writes_total Total number of feature writes\n\
         # TYPE chronos_writes_total counter\n\
         chronos_writes_total {}\n\
         # HELP chronos_cache_hits_total Total feature cache hits\n\
         # TYPE chronos_cache_hits_total counter\n\
         chronos_cache_hits_total {}\n\
         # HELP chronos_cache_misses_total Total feature cache misses\n\
         # TYPE chronos_cache_misses_total counter\n\
         chronos_cache_misses_total {}\n\
         # HELP chronos_avg_read_latency_microseconds Average read latency\n\
         # TYPE chronos_avg_read_latency_microseconds gauge\n\
         chronos_avg_read_latency_microseconds {:.3}\n",
        reads, writes, hits, misses, avg_lat
    );
    (StatusCode::OK, metrics)
}

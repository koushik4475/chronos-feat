use crate::error::{ChronosError, Result};
use crate::ingest::aggregator::{EntitySlidingWindows, WindowFeatureSpec};
use crate::ingest::event::Event;
use crate::store::memory::InMemoryOnlineStore;
use crate::store::parquet_bridge::LakehouseParquetBridge;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

pub struct IngestionPipelineConfig {
    pub channel_capacity: usize,
    pub allowed_lateness_micros: i64,
}

impl Default for IngestionPipelineConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 100_000,
            allowed_lateness_micros: 60_000_000, // 60 seconds
        }
    }
}

pub struct IngestionPipeline {
    sender: mpsc::Sender<Event>,
    config: IngestionPipelineConfig,
    specs: Arc<RwLock<Vec<WindowFeatureSpec>>>,
    entity_windows: Arc<DashMap<String, EntitySlidingWindows>>,
    online_store: Arc<InMemoryOnlineStore>,
    parquet_bridge: Arc<LakehouseParquetBridge>,
}

impl IngestionPipeline {
    pub fn new(
        config: IngestionPipelineConfig,
        online_store: Arc<InMemoryOnlineStore>,
        parquet_bridge: Arc<LakehouseParquetBridge>,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<Event>(config.channel_capacity);
        let specs = Arc::new(RwLock::new(Vec::<WindowFeatureSpec>::new()));
        let entity_windows = Arc::new(DashMap::<String, EntitySlidingWindows>::new());

        let specs_clone = specs.clone();
        let entity_windows_clone = entity_windows.clone();
        let online_store_clone = online_store.clone();
        let parquet_bridge_clone = parquet_bridge.clone();
        let allowed_lateness = config.allowed_lateness_micros;

        let worker_handle = tokio::spawn(async move {
            info!("Ingestion pipeline consumer loop started");
            while let Some(event) = rx.recv().await {
                let current_specs = { specs_clone.read().clone() };

                // 1. Ingest raw values into online store
                for (field, val) in &event.values {
                    online_store_clone.put_feature(&event.entity_id, field, val.clone(), event.timestamp);
                }

                // 2. Process sliding window features
                let mut entity_entry = entity_windows_clone
                    .entry(event.entity_id.clone())
                    .or_insert_with(|| EntitySlidingWindows::new(event.entity_id.clone()));

                for spec in &current_specs {
                    if let Some(val) = event.values.get(&spec.source_field) {
                        if let Err(e) = entity_entry.ingest_field(spec, event.timestamp, val) {
                            warn!("Failed to ingest field '{}': {:?}", spec.source_field, e);
                        } else if let Some(computed) = entity_entry.compute_value(spec, event.timestamp) {
                            online_store_clone.put_feature(&event.entity_id, &spec.name, computed, event.timestamp);
                        }
                    }
                }

                // 3. Buffer into Lakehouse bridge for offline Parquet persistence
                if let Err(e) = parquet_bridge_clone.buffer_event(&event) {
                    error!("Lakehouse buffer error for event {}: {:?}", event.id, e);
                }
            }
            info!("Ingestion pipeline consumer loop ended");
        });

        let pipeline = Self {
            sender: tx,
            config,
            specs,
            entity_windows,
            online_store,
            parquet_bridge,
        };

        (pipeline, worker_handle)
    }

    pub fn register_window_spec(&self, spec: WindowFeatureSpec) {
        self.specs.write().push(spec);
    }

    pub async fn ingest(&self, event: Event) -> Result<()> {
        self.sender
            .send(event)
            .await
            .map_err(|e| ChronosError::IngestionError(format!("Channel full or closed: {}", e)))
    }

    pub fn try_ingest(&self, event: Event) -> Result<()> {
        self.sender
            .try_send(event)
            .map_err(|e| ChronosError::IngestionError(format!("Channel capacity exceeded: {}", e)))
    }
}

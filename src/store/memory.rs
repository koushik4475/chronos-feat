use crate::error::{ChronosError, Result};
use crate::ingest::event::FeatureValue;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampedValue {
    pub value: FeatureValue,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Default)]
pub struct EntityRecord {
    pub features: HashMap<String, TimestampedValue>,
}

#[derive(Debug, Default)]
pub struct StoreMetrics {
    pub read_count: AtomicU64,
    pub write_count: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub total_read_latency_nanos: AtomicU64,
}

/// Ultra-Fast Low-Latency In-Memory Feature Store
pub struct InMemoryOnlineStore {
    entities: DashMap<String, RwLock<EntityRecord>>,
    ttl_micros: Option<i64>,
    metrics: Arc<StoreMetrics>,
}

impl InMemoryOnlineStore {
    pub fn new(ttl_micros: Option<i64>) -> Self {
        Self {
            entities: DashMap::new(),
            ttl_micros,
            metrics: Arc::new(StoreMetrics::default()),
        }
    }

    /// Single feature insert/update (O(1) amortized, sub-microsecond)
    pub fn put_feature(&self, entity_id: &str, feature_name: &str, value: FeatureValue, timestamp: i64) {
        let entry = self.entities.entry(entity_id.to_string()).or_insert_with(|| {
            RwLock::new(EntityRecord::default())
        });

        {
            let mut record = entry.write();
            record.features.insert(
                feature_name.to_string(),
                TimestampedValue { value, timestamp },
            );
        }

        self.metrics.write_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Batch feature insert for an entity
    pub fn put_features(&self, entity_id: &str, features: HashMap<String, FeatureValue>, timestamp: i64) {
        let entry = self.entities.entry(entity_id.to_string()).or_insert_with(|| {
            RwLock::new(EntityRecord::default())
        });

        {
            let mut record = entry.write();
            for (k, v) in features {
                record.features.insert(k, TimestampedValue { value: v, timestamp });
            }
        }

        self.metrics.write_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Ultra-low latency online feature vector retrieval for real-time model inference
    pub fn get_online_features(
        &self,
        entity_ids: &[String],
        feature_names: &[String],
    ) -> HashMap<String, HashMap<String, FeatureValue>> {
        let start = Instant::now();
        let mut results = HashMap::with_capacity(entity_ids.len());

        for entity_id in entity_ids {
            let mut entity_map = HashMap::with_capacity(feature_names.len());

            if let Some(entry) = self.entities.get(entity_id) {
                let record = entry.read();
                for fname in feature_names {
                    if let Some(tv) = record.features.get(fname) {
                        entity_map.insert(fname.clone(), tv.value.clone());
                        self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                    } else {
                        self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
                    }
                }
            } else {
                self.metrics.cache_misses.fetch_add(feature_names.len() as u64, Ordering::Relaxed);
            }

            results.insert(entity_id.clone(), entity_map);
        }

        let elapsed_nanos = start.elapsed().as_nanos() as u64;
        self.metrics.read_count.fetch_add(1, Ordering::Relaxed);
        self.metrics.total_read_latency_nanos.fetch_add(elapsed_nanos, Ordering::Relaxed);

        results
    }

    /// Get single feature value
    pub fn get_feature(&self, entity_id: &str, feature_name: &str) -> Option<FeatureValue> {
        if let Some(entry) = self.entities.get(entity_id) {
            let record = entry.read();
            record.features.get(feature_name).map(|tv| tv.value.clone())
        } else {
            None
        }
    }

    /// Run TTL eviction of expired records
    pub fn evict_expired(&self, current_time: i64) -> usize {
        let ttl = match self.ttl_micros {
            Some(t) => t,
            None => return 0,
        };

        let cutoff = current_time - ttl;
        let mut evicted = 0;

        self.entities.retain(|_, entry| {
            let mut record = entry.write();
            record.features.retain(|_, tv| tv.timestamp >= cutoff);
            let keep = !record.features.is_empty();
            if !keep {
                evicted += 1;
            }
            keep
        });

        evicted
    }

    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }

    pub fn get_metrics(&self) -> (u64, u64, u64, u64, f64) {
        let reads = self.metrics.read_count.load(Ordering::Relaxed);
        let writes = self.metrics.write_count.load(Ordering::Relaxed);
        let hits = self.metrics.cache_hits.load(Ordering::Relaxed);
        let misses = self.metrics.cache_misses.load(Ordering::Relaxed);
        let total_nanos = self.metrics.total_read_latency_nanos.load(Ordering::Relaxed);
        let avg_latency_micros = if reads > 0 {
            (total_nanos as f64) / (reads as f64 * 1000.0)
        } else {
            0.0
        };
        (reads, writes, hits, misses, avg_latency_micros)
    }
}

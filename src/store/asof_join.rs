use crate::error::{ChronosError, Result};
use crate::ingest::event::FeatureValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalObservation {
    pub entity_id: String,
    pub observation_timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalFeaturePoint {
    pub timestamp: i64,
    pub features: HashMap<String, FeatureValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsOfJoinResult {
    pub entity_id: String,
    pub observation_timestamp: i64,
    pub feature_timestamp: Option<i64>,
    pub features: HashMap<String, FeatureValue>,
}

pub struct PointInTimeAsOfJoinEngine {
    // entity_id -> chronologically sorted Vec<TemporalFeaturePoint>
    history: HashMap<String, Vec<TemporalFeaturePoint>>,
    tolerance_micros: Option<i64>,
}

impl PointInTimeAsOfJoinEngine {
    pub fn new(tolerance_micros: Option<i64>) -> Self {
        Self {
            history: HashMap::new(),
            tolerance_micros,
        }
    }

    /// Insert or append a temporal feature snapshot for an entity
    pub fn insert_point(&mut self, entity_id: &str, timestamp: i64, features: HashMap<String, FeatureValue>) {
        let points = self.history.entry(entity_id.to_string()).or_insert_with(Vec::new);
        let point = TemporalFeaturePoint { timestamp, features };

        // Maintain sorted order by timestamp
        match points.binary_search_by_key(&timestamp, |p| p.timestamp) {
            Ok(idx) => {
                // Merge or replace features at exact same timestamp
                points[idx].features.extend(point.features);
            }
            Err(idx) => {
                points.insert(idx, point);
            }
        }
    }

    /// Performs Point-in-Time AS-OF Join for a single observation
    /// Strictly guarantees: feature_timestamp <= observation_timestamp
    pub fn join_asof(
        &self,
        entity_id: &str,
        observation_timestamp: i64,
        requested_features: Option<&[String]>,
    ) -> Result<AsOfJoinResult> {
        let points = match self.history.get(entity_id) {
            Some(p) => p,
            None => {
                return Ok(AsOfJoinResult {
                    entity_id: entity_id.to_string(),
                    observation_timestamp,
                    feature_timestamp: None,
                    features: HashMap::new(),
                })
            }
        };

        if points.is_empty() {
            return Ok(AsOfJoinResult {
                entity_id: entity_id.to_string(),
                observation_timestamp,
                feature_timestamp: None,
                features: HashMap::new(),
            });
        }

        // Binary search to find the largest timestamp <= observation_timestamp
        // partition_point finds the first element where predicate is false
        let idx = points.partition_point(|p| p.timestamp <= observation_timestamp);

        if idx == 0 {
            // All points are in the future relative to observation_timestamp!
            // No valid feature without lookahead bias.
            return Ok(AsOfJoinResult {
                entity_id: entity_id.to_string(),
                observation_timestamp,
                feature_timestamp: None,
                features: HashMap::new(),
            });
        }

        let best_point = &points[idx - 1];

        // Sanity check against future lookahead
        if best_point.timestamp > observation_timestamp {
            return Err(ChronosError::LookaheadBiasViolation {
                obs_ts: observation_timestamp,
                evt_ts: best_point.timestamp,
            });
        }

        // Check tolerance window if configured
        if let Some(tolerance) = self.tolerance_micros {
            if observation_timestamp - best_point.timestamp > tolerance {
                // Expired beyond tolerance window
                return Ok(AsOfJoinResult {
                    entity_id: entity_id.to_string(),
                    observation_timestamp,
                    feature_timestamp: None,
                    features: HashMap::new(),
                });
            }
        }

        // Filter requested features if specified
        let features = match requested_features {
            Some(names) => {
                let mut map = HashMap::new();
                for name in names {
                    if let Some(val) = best_point.features.get(name) {
                        map.insert(name.clone(), val.clone());
                    }
                }
                map
            }
            None => best_point.features.clone(),
        };

        Ok(AsOfJoinResult {
            entity_id: entity_id.to_string(),
            observation_timestamp,
            feature_timestamp: Some(best_point.timestamp),
            features,
        })
    }

    /// Batch AS-OF join for generating offline training datasets
    pub fn batch_join_asof(
        &self,
        observations: &[HistoricalObservation],
        requested_features: Option<&[String]>,
    ) -> Result<Vec<AsOfJoinResult>> {
        let mut results = Vec::with_capacity(observations.len());
        for obs in observations {
            let res = self.join_asof(&obs.entity_id, obs.observation_timestamp, requested_features)?;
            results.push(res);
        }
        Ok(results)
    }
}

use crate::error::{ChronosError, Result};
use crate::ingest::event::FeatureValue;
use parking_lot::RwLock;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaDataType {
    Float64,
    Int64,
    String,
    Bool,
    Vector32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationRule {
    Range { min: Option<f64>, max: Option<f64> },
    RegexPattern(String),
    CategoricalAllowlist(Vec<String>),
    VectorDimension(usize),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureDef {
    pub name: String,
    pub data_type: SchemaDataType,
    pub description: String,
    pub nullable: bool,
    pub validation_rules: Vec<ValidationRule>,
}

impl FeatureDef {
    pub fn validate(&self, value: &FeatureValue) -> Result<()> {
        match (self.data_type, value) {
            (SchemaDataType::Float64, FeatureValue::Float(f)) => {
                for rule in &self.validation_rules {
                    if let ValidationRule::Range { min, max } = rule {
                        if let Some(low) = min {
                            if *f < *low {
                                return Err(ChronosError::ValidationError(format!(
                                    "Feature '{}' value {} below minimum {}",
                                    self.name, f, low
                                )));
                            }
                        }
                        if let Some(high) = max {
                            if *f > *high {
                                return Err(ChronosError::ValidationError(format!(
                                    "Feature '{}' value {} above maximum {}",
                                    self.name, f, high
                                )));
                            }
                        }
                    }
                }
            }
            (SchemaDataType::Int64, FeatureValue::Int(i)) => {
                for rule in &self.validation_rules {
                    if let ValidationRule::Range { min, max } = rule {
                        let f = *i as f64;
                        if let Some(low) = min {
                            if f < *low {
                                return Err(ChronosError::ValidationError(format!(
                                    "Feature '{}' value {} below min {}",
                                    self.name, i, low
                                )));
                            }
                        }
                        if let Some(high) = max {
                            if f > *high {
                                return Err(ChronosError::ValidationError(format!(
                                    "Feature '{}' value {} above max {}",
                                    self.name, i, high
                                )));
                            }
                        }
                    }
                }
            }
            (SchemaDataType::String, FeatureValue::String(s)) => {
                for rule in &self.validation_rules {
                    match rule {
                        ValidationRule::RegexPattern(pat) => {
                            let re = Regex::new(pat).map_err(|e| {
                                ChronosError::ValidationError(format!("Invalid regex {}: {}", pat, e))
                            })?;
                            if !re.is_match(s) {
                                return Err(ChronosError::ValidationError(format!(
                                    "Feature '{}' value '{}' does not match regex '{}'",
                                    self.name, s, pat
                                )));
                            }
                        }
                        ValidationRule::CategoricalAllowlist(allowed) => {
                            if !allowed.contains(s) {
                                return Err(ChronosError::ValidationError(format!(
                                    "Feature '{}' value '{}' not in allowlist {:?}",
                                    self.name, s, allowed
                                )));
                            }
                        }
                        _ => {}
                    }
                }
            }
            (SchemaDataType::Bool, FeatureValue::Bool(_)) => {}
            (SchemaDataType::Vector32, FeatureValue::Vector(v)) => {
                for rule in &self.validation_rules {
                    if let ValidationRule::VectorDimension(expected_dim) = rule {
                        if v.len() != *expected_dim {
                            return Err(ChronosError::ValidationError(format!(
                                "Feature '{}' vector dim {} != expected {}",
                                self.name,
                                v.len(),
                                expected_dim
                            )));
                        }
                    }
                }
            }
            _ => {
                return Err(ChronosError::ValidationError(format!(
                    "Feature '{}' type mismatch: expected {:?}, got {:?}",
                    self.name, self.data_type, value
                )));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureViewSchema {
    pub name: String,
    pub entity_key: String,
    pub features: HashMap<String, FeatureDef>,
    pub ttl_micros: Option<i64>,
}

#[derive(Clone, Default)]
pub struct SchemaRegistry {
    views: Arc<RwLock<HashMap<String, FeatureViewSchema>>>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            views: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn register_view(&self, view: FeatureViewSchema) {
        self.views.write().insert(view.name.clone(), view);
    }

    pub fn get_view(&self, view_name: &str) -> Option<FeatureViewSchema> {
        self.views.read().get(view_name).cloned()
    }

    pub fn validate_features(&self, view_name: &str, values: &HashMap<String, FeatureValue>) -> Result<()> {
        let views = self.views.read();
        let view = views
            .get(view_name)
            .ok_or_else(|| ChronosError::FeatureViewNotFound(view_name.to_string()))?;

        for (fname, val) in values {
            if let Some(fdef) = view.features.get(fname) {
                fdef.validate(val)?;
            }
        }

        Ok(())
    }
}

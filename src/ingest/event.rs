use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FeatureValue {
    Float(f64),
    Int(i64),
    String(String),
    Bool(bool),
    Vector(Vec<f32>),
}

impl FeatureValue {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            FeatureValue::Float(f) => Some(*f),
            FeatureValue::Int(i) => Some(*i as f64),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            FeatureValue::Int(i) => Some(*i),
            FeatureValue::Float(f) => Some(*f as i64),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            FeatureValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            FeatureValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_vector(&self) -> Option<&[f32]> {
        match self {
            FeatureValue::Vector(v) => Some(v.as_slice()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    #[serde(default = "Uuid::new_v4")]
    pub id: Uuid,
    pub entity_id: String,
    pub event_type: String,
    pub timestamp: i64, // Unix timestamp in microseconds
    pub values: HashMap<String, FeatureValue>,
}

impl Event {
    pub fn new(entity_id: impl Into<String>, event_type: impl Into<String>, timestamp: i64) -> Self {
        Self {
            id: Uuid::new_v4(),
            entity_id: entity_id.into(),
            event_type: event_type.into(),
            timestamp,
            values: HashMap::new(),
        }
    }

    pub fn with_value(mut self, key: impl Into<String>, value: FeatureValue) -> Self {
        self.values.insert(key.into(), value);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureRecord {
    pub entity_id: String,
    pub feature_name: String,
    pub value: FeatureValue,
    pub timestamp: i64,
}

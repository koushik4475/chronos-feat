use crate::error::{ChronosError, Result};
use crate::ingest::event::FeatureValue;
use crate::ingest::window::{SlidingWindow, WindowAggregateResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggregationOp {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    StdDev,
    Variance,
    ExponentialDecay,
    HyperLogLog,
}

/// Continuous exponential decay aggregator
/// Weight decays as e^(-lambda * dt) where lambda = ln(2) / half_life
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExponentialDecayState {
    pub half_life_micros: f64,
    pub lambda: f64,
    pub accumulated_value: f64,
    pub last_timestamp: i64,
}

impl ExponentialDecayState {
    pub fn new(half_life_micros: f64) -> Self {
        let lambda = 2.0f64.ln() / half_life_micros;
        Self {
            half_life_micros,
            lambda,
            accumulated_value: 0.0,
            last_timestamp: 0,
        }
    }

    pub fn update(&mut self, timestamp: i64, value: f64) {
        if self.last_timestamp == 0 {
            self.accumulated_value = value;
            self.last_timestamp = timestamp;
            return;
        }

        if timestamp >= self.last_timestamp {
            let dt = (timestamp - self.last_timestamp) as f64;
            let decay = (-self.lambda * dt).exp();
            self.accumulated_value = self.accumulated_value * decay + value;
            self.last_timestamp = timestamp;
        } else {
            // Out of order past event: decay backwards
            let dt = (self.last_timestamp - timestamp) as f64;
            let decay = (-self.lambda * dt).exp();
            self.accumulated_value += value * decay;
        }
    }

    pub fn current_value(&self, current_time: i64) -> f64 {
        if self.last_timestamp == 0 {
            return 0.0;
        }
        if current_time <= self.last_timestamp {
            return self.accumulated_value;
        }
        let dt = (current_time - self.last_timestamp) as f64;
        let decay = (-self.lambda * dt).exp();
        self.accumulated_value * decay
    }
}

/// HyperLogLog cardinality estimator sketch (p = 10, m = 1024 registers)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperLogLogSketch {
    pub registers: Vec<u8>,
}

impl HyperLogLogSketch {
    pub const P: usize = 10;
    pub const M: usize = 1 << Self::P; // 1024 registers
    pub const ALPHA: f64 = 0.7213 / (1.0 + 1.079 / (Self::M as f64));

    pub fn new() -> Self {
        Self {
            registers: vec![0; Self::M],
        }
    }

    #[inline]
    fn fnv1a_hash(data: &str) -> u64 {
        let mut hash = 0xcbf29ce484222325u64;
        for byte in data.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3u64);
        }
        hash
    }

    pub fn insert(&mut self, item: &str) {
        let hash = Self::fnv1a_hash(item);
        let index = (hash >> (64 - Self::P)) as usize;
        let w = (hash << Self::P) | (1 << (Self::P - 1)); // remainder with sentinel
        let leading_zeros = (w.leading_zeros() as u8) + 1;
        if leading_zeros > self.registers[index] {
            self.registers[index] = leading_zeros;
        }
    }

    pub fn estimate(&self) -> u64 {
        let mut sum = 0.0f64;
        let mut zero_registers = 0u64;

        for &val in &self.registers {
            sum += 2.0f64.powi(-(val as i32));
            if val == 0 {
                zero_registers += 1;
            }
        }

        let m = Self::M as f64;
        let mut estimate = Self::ALPHA * m * m / sum;

        // Linear counting for small range
        if estimate <= 2.5 * m && zero_registers > 0 {
            estimate = m * ((m / (zero_registers as f64)).ln());
        }

        estimate.round() as u64
    }
}

/// Spec for a window-aggregated feature (e.g. tx_amount_sum_5m)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowFeatureSpec {
    pub name: String,
    pub source_field: String,
    pub op: AggregationOp,
    pub window_duration_micros: i64,
    pub bucket_resolution_micros: i64,
    pub half_life_micros: Option<f64>,
}

/// Entity-level sliding window container
#[derive(Debug, Clone)]
pub struct EntitySlidingWindows {
    pub entity_id: String,
    pub sliding_windows: HashMap<String, SlidingWindow>,
    pub exp_decays: HashMap<String, ExponentialDecayState>,
    pub hll_sketches: HashMap<String, HyperLogLogSketch>,
}

impl EntitySlidingWindows {
    pub fn new(entity_id: impl Into<String>) -> Self {
        Self {
            entity_id: entity_id.into(),
            sliding_windows: HashMap::new(),
            exp_decays: HashMap::new(),
            hll_sketches: HashMap::new(),
        }
    }

    pub fn register_spec(&mut self, spec: &WindowFeatureSpec, allowed_lateness_micros: i64) {
        match spec.op {
            AggregationOp::ExponentialDecay => {
                let half_life = spec.half_life_micros.unwrap_or(spec.window_duration_micros as f64);
                self.exp_decays
                    .entry(spec.name.clone())
                    .or_insert_with(|| ExponentialDecayState::new(half_life));
            }
            AggregationOp::HyperLogLog => {
                self.hll_sketches
                    .entry(spec.name.clone())
                    .or_insert_with(HyperLogLogSketch::new);
            }
            _ => {
                self.sliding_windows
                    .entry(spec.name.clone())
                    .or_insert_with(|| {
                        SlidingWindow::new(
                            spec.window_duration_micros,
                            spec.bucket_resolution_micros,
                            allowed_lateness_micros,
                        )
                    });
            }
        }
    }

    pub fn ingest_field(&mut self, spec: &WindowFeatureSpec, timestamp: i64, value: &FeatureValue) -> Result<()> {
        match spec.op {
            AggregationOp::ExponentialDecay => {
                let num = value.as_f64().ok_or_else(|| {
                    ChronosError::ValidationError(format!("Field {} must be numeric for exp decay", spec.source_field))
                })?;
                let decay = self.exp_decays.entry(spec.name.clone()).or_insert_with(|| {
                    let half_life = spec.half_life_micros.unwrap_or(spec.window_duration_micros as f64);
                    ExponentialDecayState::new(half_life)
                });
                decay.update(timestamp, num);
            }
            AggregationOp::HyperLogLog => {
                let s = match value {
                    FeatureValue::String(s) => s.clone(),
                    FeatureValue::Int(i) => i.to_string(),
                    FeatureValue::Float(f) => f.to_string(),
                    FeatureValue::Bool(b) => b.to_string(),
                    FeatureValue::Vector(_) => return Err(ChronosError::ValidationError("Vector not supported in HLL".into())),
                };
                let hll = self.hll_sketches.entry(spec.name.clone()).or_insert_with(HyperLogLogSketch::new);
                hll.insert(&s);
            }
            _ => {
                let num = value.as_f64().ok_or_else(|| {
                    ChronosError::ValidationError(format!("Field {} must be numeric for window aggregation", spec.source_field))
                })?;
                let window = self.sliding_windows.entry(spec.name.clone()).or_insert_with(|| {
                    SlidingWindow::new(
                        spec.window_duration_micros,
                        spec.bucket_resolution_micros,
                        spec.window_duration_micros / 10,
                    )
                });
                window.add(timestamp, num);
            }
        }
        Ok(())
    }

    pub fn compute_value(&self, spec: &WindowFeatureSpec, current_time: i64) -> Option<FeatureValue> {
        match spec.op {
            AggregationOp::ExponentialDecay => {
                let decay = self.exp_decays.get(&spec.name)?;
                Some(FeatureValue::Float(decay.current_value(current_time)))
            }
            AggregationOp::HyperLogLog => {
                let hll = self.hll_sketches.get(&spec.name)?;
                Some(FeatureValue::Int(hll.estimate() as i64))
            }
            op => {
                let window = self.sliding_windows.get(&spec.name)?;
                let agg = window.aggregate(current_time);
                match op {
                    AggregationOp::Count => Some(FeatureValue::Int(agg.count as i64)),
                    AggregationOp::Sum => Some(FeatureValue::Float(agg.sum)),
                    AggregationOp::Avg => Some(FeatureValue::Float(agg.avg)),
                    AggregationOp::Min => Some(FeatureValue::Float(agg.min)),
                    AggregationOp::Max => Some(FeatureValue::Float(agg.max)),
                    AggregationOp::Variance => Some(FeatureValue::Float(agg.variance)),
                    AggregationOp::StdDev => Some(FeatureValue::Float(agg.stddev)),
                    _ => None,
                }
            }
        }
    }
}

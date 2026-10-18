use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowType {
    Sliding,
    Tumbling,
}

#[derive(Debug, Clone)]
pub struct TimeBucket {
    pub bucket_start: i64,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    // Welford intermediate state for stddev
    pub mean: f64,
    pub m2: f64,
}

impl TimeBucket {
    pub fn new(bucket_start: i64) -> Self {
        Self {
            bucket_start,
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            mean: 0.0,
            m2: 0.0,
        }
    }

    pub fn update(&mut self, val: f64) {
        self.count += 1;
        self.sum += val;
        if val < self.min {
            self.min = val;
        }
        if val > self.max {
            self.max = val;
        }
        // Online Welford update
        let delta = val - self.mean;
        self.mean += delta / (self.count as f64);
        let delta2 = val - self.mean;
        self.m2 += delta * delta2;
    }

    pub fn variance(&self) -> f64 {
        if self.count < 2 {
            0.0
        } else {
            self.m2 / ((self.count - 1) as f64)
        }
    }

    pub fn stddev(&self) -> f64 {
        self.variance().sqrt()
    }
}

/// High-performance sliding window feature aggregator
#[derive(Debug, Clone)]
pub struct SlidingWindow {
    pub window_duration_micros: i64,
    pub bucket_resolution_micros: i64,
    pub allowed_lateness_micros: i64,
    pub watermark: i64,
    // BTreeMap of bucket_start -> TimeBucket for sorted chronological ordering
    buckets: BTreeMap<i64, TimeBucket>,
}

impl SlidingWindow {
    pub fn new(window_duration_micros: i64, bucket_resolution_micros: i64, allowed_lateness_micros: i64) -> Self {
        assert!(bucket_resolution_micros > 0, "Bucket resolution must be positive");
        assert!(window_duration_micros >= bucket_resolution_micros, "Window duration must be >= resolution");
        Self {
            window_duration_micros,
            bucket_resolution_micros,
            allowed_lateness_micros,
            watermark: 0,
            buckets: BTreeMap::new(),
        }
    }

    #[inline]
    fn align_bucket(&self, timestamp: i64) -> i64 {
        timestamp - (timestamp % self.bucket_resolution_micros)
    }

    /// Ingests an event value into the appropriate sliding window bucket
    pub fn add(&mut self, timestamp: i64, value: f64) -> bool {
        // Watermark check
        if timestamp > self.watermark {
            self.watermark = timestamp;
        }

        let lateness_cutoff = self.watermark - self.allowed_lateness_micros;
        if timestamp < lateness_cutoff {
            // Late event dropped
            return false;
        }

        let bucket_key = self.align_bucket(timestamp);
        self.buckets
            .entry(bucket_key)
            .or_insert_with(|| TimeBucket::new(bucket_key))
            .update(value);

        // Prune expired buckets
        self.prune(timestamp);
        true
    }

    /// Prunes buckets that fall completely outside the window [current_time - window_duration, current_time]
    pub fn prune(&mut self, current_time: i64) {
        let cutoff = current_time - self.window_duration_micros;
        let expired_keys: Vec<i64> = self.buckets
            .range(..cutoff)
            .map(|(&k, _)| k)
            .collect();

        for k in expired_keys {
            self.buckets.remove(&k);
        }
    }

    /// Computes aggregated metrics over the active sliding window ending at current_time
    pub fn aggregate(&self, current_time: i64) -> WindowAggregateResult {
        let window_start = current_time - self.window_duration_micros;
        let mut total_count = 0u64;
        let mut total_sum = 0.0f64;
        let mut min_val = f64::INFINITY;
        let mut max_val = f64::NEG_INFINITY;

        // Combined Welford's algorithm across active buckets
        let mut combined_mean = 0.0f64;
        let mut combined_m2 = 0.0f64;

        for (&bucket_start, bucket) in self.buckets.range(window_start..) {
            if bucket_start > current_time {
                continue;
            }
            if bucket.count == 0 {
                continue;
            }

            let new_count = total_count + bucket.count;
            let delta = bucket.mean - combined_mean;
            let n1 = total_count as f64;
            let n2 = bucket.count as f64;
            let n = new_count as f64;

            combined_mean += delta * (n2 / n);
            combined_m2 += bucket.m2 + delta * delta * (n1 * n2 / n);

            total_count = new_count;
            total_sum += bucket.sum;
            if bucket.min < min_val {
                min_val = bucket.min;
            }
            if bucket.max > max_val {
                max_val = bucket.max;
            }
        }

        let avg = if total_count > 0 {
            total_sum / (total_count as f64)
        } else {
            0.0
        };

        let variance = if total_count > 1 {
            combined_m2 / ((total_count - 1) as f64)
        } else {
            0.0
        };

        WindowAggregateResult {
            count: total_count,
            sum: total_sum,
            avg,
            min: if total_count > 0 { min_val } else { 0.0 },
            max: if total_count > 0 { max_val } else { 0.0 },
            variance,
            stddev: variance.sqrt(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowAggregateResult {
    pub count: u64,
    pub sum: f64,
    pub avg: f64,
    pub min: f64,
    pub max: f64,
    pub variance: f64,
    pub stddev: f64,
}

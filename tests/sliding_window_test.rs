use chronos_feat::ingest::{
    AggregationOp, EntitySlidingWindows, ExponentialDecayState, FeatureValue, HyperLogLogSketch,
    SlidingWindow, WindowFeatureSpec,
};
use chronos_feat::store::PointInTimeAsOfJoinEngine;
use std::collections::HashMap;

#[test]
fn test_sliding_window_aggregation_and_pruning() {
    // 5-second window, 1-second bucket resolution, 1-second allowed lateness
    let window_duration = 5_000_000; // 5s in micros
    let bucket_resolution = 1_000_000; // 1s in micros
    let allowed_lateness = 1_000_000;

    let mut window = SlidingWindow::new(window_duration, bucket_resolution, allowed_lateness);

    let base_time = 1_000_000_000;

    // Insert 5 events at 1-second increments: values 10.0, 20.0, 30.0, 40.0, 50.0
    for i in 0..5 {
        let ts = base_time + (i * 1_000_000);
        let val = ((i + 1) * 10) as f64;
        assert!(window.add(ts, val));
    }

    let agg = window.aggregate(base_time + 4_000_000);
    assert_eq!(agg.count, 5);
    assert_eq!(agg.sum, 150.0);
    assert_eq!(agg.avg, 30.0);
    assert_eq!(agg.min, 10.0);
    assert_eq!(agg.max, 50.0);

    // Advance time by 3 seconds (to base_time + 7s).
    // The events at base_time and base_time + 1s should expire from the 5s window!
    let current_time = base_time + 7_000_000;
    window.prune(current_time);

    let agg_after = window.aggregate(current_time);
    // Active timestamps: base_time + 2s (30.0), +3s (40.0), +4s (50.0)
    assert_eq!(agg_after.count, 3);
    assert_eq!(agg_after.sum, 120.0);
    assert_eq!(agg_after.avg, 40.0);
    assert_eq!(agg_after.min, 30.0);
    assert_eq!(agg_after.max, 50.0);
}

#[test]
fn test_welford_variance_and_stddev() {
    let mut window = SlidingWindow::new(10_000_000, 1_000_000, 1_000_000);
    let base_ts = 1_000_000_000;

    // Dataset: [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
    // Mean = 40 / 8 = 5.0
    // Sample Variance = sum((x - 5)^2) / (8 - 1) = (9 + 1 + 1 + 1 + 0 + 0 + 4 + 16) / 7 = 32 / 7 ≈ 4.5714
    // Sample StdDev = sqrt(32/7) ≈ 2.138
    let values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
    for (i, &v) in values.iter().enumerate() {
        window.add(base_ts + (i as i64 * 100_000), v);
    }

    let agg = window.aggregate(base_ts + 1_000_000);
    assert_eq!(agg.count, 8);
    assert_eq!(agg.avg, 5.0);

    let expected_variance = 32.0 / 7.0;
    let expected_stddev = expected_variance.sqrt();

    assert!((agg.variance - expected_variance).abs() < 1e-4);
    assert!((agg.stddev - expected_stddev).abs() < 1e-4);
}

#[test]
fn test_exponential_decay_recency() {
    // Half life: 10 seconds (10_000_000 micros)
    let half_life = 10_000_000.0;
    let mut exp_decay = ExponentialDecayState::new(half_life);

    let t0 = 1_000_000_000;
    exp_decay.update(t0, 100.0);

    // Immediate value should be 100.0
    let v0 = exp_decay.current_value(t0);
    assert_eq!(v0, 100.0);

    // Exactly one half-life later (t0 + 10s), value should have decayed by half to ~50.0
    let t1 = t0 + 10_000_000;
    let v1 = exp_decay.current_value(t1);
    assert!((v1 - 50.0).abs() < 1e-3);

    // Two half-lives later (t0 + 20s), value should have decayed to ~25.0
    let t2 = t0 + 20_000_000;
    let v2 = exp_decay.current_value(t2);
    assert!((v2 - 25.0).abs() < 1e-3);
}

#[test]
fn test_hyperloglog_cardinality_sketch() {
    let mut hll = HyperLogLogSketch::new();

    // Insert 5,000 distinct items
    let n = 5000;
    for i in 0..n {
        hll.insert(&format!("user_session_uuid_{}", i));
    }

    let estimate = hll.estimate();
    // Theoretical standard error for m=1024 registers is ~ 1.04 / sqrt(1024) = 3.25%
    let error = ((estimate as f64 - n as f64).abs()) / (n as f64);
    assert!(
        error < 0.08,
        "HLL estimate {} error {:.2}% exceeded tolerance for true count {}",
        estimate,
        error * 100.0,
        n
    );
}

#[test]
fn test_point_in_time_asof_join_strict_temporal_leak_prevention() {
    let mut asof = PointInTimeAsOfJoinEngine::new(Some(3600_000_000)); // 1 hour tolerance
    let entity = "user_42";

    // Feature updates at t = 100, 200, 300
    let mut f100 = HashMap::new();
    f100.insert("risk_score".into(), FeatureValue::Float(0.12));
    f100.insert("login_count".into(), FeatureValue::Int(1));
    asof.insert_point(entity, 100_000_000, f100);

    let mut f200 = HashMap::new();
    f200.insert("risk_score".into(), FeatureValue::Float(0.45));
    f200.insert("login_count".into(), FeatureValue::Int(5));
    asof.insert_point(entity, 200_000_000, f200);

    let mut f300 = HashMap::new();
    f300.insert("risk_score".into(), FeatureValue::Float(0.98));
    f300.insert("login_count".into(), FeatureValue::Int(12));
    asof.insert_point(entity, 300_000_000, f300);

    // Case 1: Observation at t = 250 (between 200 and 300)
    // Must return values from t = 200 (risk_score = 0.45). If it returns 0.98, it's a lookahead data leak!
    let join_res = asof.join_asof(entity, 250_000_000, None).unwrap();
    assert_eq!(join_res.feature_timestamp, Some(200_000_000));
    assert_eq!(join_res.features.get("risk_score"), Some(&FeatureValue::Float(0.45)));
    assert_eq!(join_res.features.get("login_count"), Some(&FeatureValue::Int(5)));

    // Case 2: Observation prior to all feature points at t = 50
    // Must return None / empty features, NO FUTURE VALUES!
    let join_early = asof.join_asof(entity, 50_000_000, None).unwrap();
    assert_eq!(join_early.feature_timestamp, None);
    assert!(join_early.features.is_empty());

    // Case 3: Exact match at t = 300
    let join_exact = asof.join_asof(entity, 300_000_000, None).unwrap();
    assert_eq!(join_exact.feature_timestamp, Some(300_000_000));
    assert_eq!(join_exact.features.get("risk_score"), Some(&FeatureValue::Float(0.98)));
}

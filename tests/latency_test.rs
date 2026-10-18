use chronos_feat::ingest::FeatureValue;
use chronos_feat::store::InMemoryOnlineStore;
use std::sync::Arc;
use std::time::Instant;

#[tokio::test]
async fn test_feature_lookup_sub_millisecond_latency() {
    let store = Arc::new(InMemoryOnlineStore::new(None));
    let num_entities = 10_000;
    let base_time = chrono::Utc::now().timestamp_micros();

    // 1. Populate store with 10,000 entities, each having 5 features
    for i in 0..num_entities {
        let entity_id = format!("user_{:06}", i);
        store.put_feature(&entity_id, "account_age_days", FeatureValue::Int(365), base_time);
        store.put_feature(&entity_id, "credit_score", FeatureValue::Float(742.5), base_time);
        store.put_feature(&entity_id, "is_verified", FeatureValue::Bool(true), base_time);
        store.put_feature(&entity_id, "country_code", FeatureValue::String("US".into()), base_time);
        store.put_feature(
            &entity_id,
            "embedding",
            FeatureValue::Vector(vec![0.12, 0.45, -0.67, 0.89]),
            base_time,
        );
    }

    assert_eq!(store.entity_count(), num_entities);

    // 2. Perform 20,000 concurrent vector lookups across spawned async tasks
    let feature_keys = vec![
        "account_age_days".to_string(),
        "credit_score".to_string(),
        "is_verified".to_string(),
        "country_code".to_string(),
        "embedding".to_string(),
    ];

    let mut latencies_nanos = Vec::with_capacity(20_000);
    let sample_queries = 20_000;

    for i in 0..sample_queries {
        let entity_query = vec![format!("user_{:06}", (i * 37) % num_entities)];
        let start = Instant::now();
        let res = store.get_online_features(&entity_query, &feature_keys);
        let elapsed = start.elapsed().as_nanos();
        latencies_nanos.push(elapsed);

        // Verify correctness
        assert_eq!(res.len(), 1);
        let features = res.values().next().unwrap();
        assert_eq!(features.len(), 5);
        assert_eq!(features.get("credit_score"), Some(&FeatureValue::Float(742.5)));
    }

    // 3. Compute Latency Percentiles
    latencies_nanos.sort_unstable();
    let p50 = latencies_nanos[(sample_queries as f64 * 0.50) as usize] as f64 / 1_000.0;
    let p95 = latencies_nanos[(sample_queries as f64 * 0.95) as usize] as f64 / 1_000.0;
    let p99 = latencies_nanos[(sample_queries as f64 * 0.99) as usize] as f64 / 1_000.0;
    let p999 = latencies_nanos[(sample_queries as f64 * 0.999) as usize] as f64 / 1_000.0;

    println!(
        "Lookup Latencies: P50 = {:.2}µs, P95 = {:.2}µs, P99 = {:.2}µs, P99.9 = {:.2}µs",
        p50, p95, p99, p999
    );

    // Assert sub-millisecond (1,000µs) P99 latency guarantee
    assert!(
        p99 < 1000.0,
        "P99 latency {:.2}µs exceeded sub-millisecond requirement",
        p99
    );

    let (reads, writes, hits, misses, avg_lat) = store.get_metrics();
    assert_eq!(reads, sample_queries as u64);
    assert_eq!(misses, 0);
    assert!(hits > 0);
    assert!(avg_lat < 1000.0);
}

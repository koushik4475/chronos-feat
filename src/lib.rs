//! Chronos-Feat: Ultra-Low-Latency Distributed Real-Time ML Feature Store
//!
//! Features:
//! - Sub-millisecond P99 online feature vector serving
//! - Streaming event ingestion with fine-grained sliding-window feature aggregation
//! - Continuous online operators: Count, Sum, Avg, Min, Max, Welford's StdDev, Exponential Decay, HyperLogLog
//! - Dual-tier storage: Sharded in-memory cache + Columnar Apache Parquet lakehouse bridge
//! - Point-in-Time AS-OF Join engine eliminating ML train/serve skew and lookahead data leakage
//! - Strict schema validation rules (ranges, regex, categorical sets, embedding dimensions)

pub mod api;
pub mod error;
pub mod ingest;
pub mod store;

pub use api::{AppState, FeatureDef, FeatureViewSchema, SchemaDataType, SchemaRegistry, ValidationRule};
pub use error::{ChronosError, Result};
pub use ingest::{
    AggregationOp, EntitySlidingWindows, Event, ExponentialDecayState, FeatureRecord, FeatureValue,
    HyperLogLogSketch, IngestionPipeline, IngestionPipelineConfig, SlidingWindow, TimeBucket,
    WindowAggregateResult, WindowFeatureSpec, WindowType,
};
pub use store::{
    AsOfJoinResult, EntityRecord, HistoricalObservation, InMemoryOnlineStore, LakehouseParquetBridge,
    ParquetBridgeConfig, PointInTimeAsOfJoinEngine, StoreMetrics, TemporalFeaturePoint, TimestampedValue,
};

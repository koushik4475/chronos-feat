use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum ChronosError {
    #[error("Schema validation failed: {0}")]
    ValidationError(String),

    #[error("Feature not found: entity={entity_id}, feature={feature_name}")]
    FeatureNotFound {
        entity_id: String,
        feature_name: String,
    },

    #[error("Feature view '{0}' not registered")]
    FeatureViewNotFound(String),

    #[error("Entity '{0}' has no recorded features")]
    EntityNotFound(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Parquet lakehouse bridge error: {0}")]
    ParquetError(String),

    #[error("Ingestion pipeline error: {0}")]
    IngestionError(String),

    #[error("Lookahead bias violation: observation time {obs_ts} is prior to event time {evt_ts}")]
    LookaheadBiasViolation { obs_ts: i64, evt_ts: i64 },

    #[error("Out-of-order event beyond watermarked lateness boundary: timestamp {0}")]
    LateEventDropped(i64),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, ChronosError>;

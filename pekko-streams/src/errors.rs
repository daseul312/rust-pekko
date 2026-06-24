//! Error types for Pekko Streams

use thiserror::Error;

/// Main stream error type
#[derive(Debug, Error)]
pub enum StreamError {
    #[error("Stream cancelled")]
    Cancelled,

    #[error("Stream failed: {0}")]
    Failed(String),

    #[error("Backpressure timeout")]
    BackpressureTimeout,

    #[error("Buffer overflow (capacity: {0})")]
    BufferOverflow(usize),

    #[error("Stage failed: {0}")]
    StageFailed(String),

    #[error("Materialization failed: {0}")]
    MaterializationFailed(String),

    #[error("Channel closed")]
    ChannelClosed,

    #[error("Timeout")]
    Timeout,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type for stream operations
pub type StreamResult<T> = Result<T, StreamError>;

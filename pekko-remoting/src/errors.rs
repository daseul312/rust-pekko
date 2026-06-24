//! Error types for Pekko Remoting

use thiserror::Error;

/// Main remoting error type
#[derive(Debug, Error)]
pub enum RemotingError {
    #[error("Transport error: {0}")]
    Transport(#[from] TransportError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] SerializationError),

    #[error("Addressing error: {0}")]
    Addressing(#[from] AddressingError),

    #[error("Cluster error: {0}")]
    Cluster(#[from] ClusterError),

    #[error("Connection error: {0}")]
    Connection(#[from] ConnectionError),
}

/// Transport layer errors
#[derive(Debug, Error)]
pub enum TransportError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Bind failed on {address}: {source}")]
    BindFailed {
        address: String,
        source: std::io::Error,
    },

    #[error("Connection failed to {address}: {source}")]
    ConnectionFailed {
        address: String,
        source: std::io::Error,
    },

    #[error("Send failed: {0}")]
    SendFailed(String),

    #[error("Receive failed: {0}")]
    ReceiveFailed(String),

    #[error("Transport shutdown")]
    Shutdown,

    #[error("Invalid frame: {reason}")]
    InvalidFrame { reason: String },

    #[error("Timeout")]
    Timeout,

    #[error("Not connected")]
    NotConnected,

    #[error("Not implemented")]
    NotImplemented,

    #[error("Protocol error: {0}")]
    ProtocolError(String),
}

/// Serialization errors
#[derive(Debug, Error)]
pub enum SerializationError {
    #[error("Serialization failed: {0}")]
    SerializationFailed(String),

    #[error("Deserialization failed: {0}")]
    DeserializationFailed(String),

    #[error("Unsupported format")]
    UnsupportedFormat,

    #[error("Invalid data: {0}")]
    InvalidData(String),
}

/// Addressing errors
#[derive(Debug, Error)]
pub enum AddressingError {
    #[error("Invalid address: {0}")]
    InvalidAddress(String),

    #[error("Address resolution failed: {0}")]
    ResolutionFailed(String),

    #[error("Unknown address: {0}")]
    UnknownAddress(String),

    #[error("Invalid format: {address}")]
    InvalidFormat { address: String },

    #[error("Invalid port: {port}")]
    InvalidPort { port: String },

    #[error("Invalid path: {path}")]
    InvalidPath { path: String },
}

/// Cluster errors
#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("Cluster join failed: {0}")]
    JoinFailed(String),

    #[error("Cluster leave failed: {0}")]
    LeaveFailed(String),

    #[error("Cluster member not found: {0}")]
    MemberNotFound(String),

    #[error("Cluster state error: {0}")]
    StateError(String),
}

/// Connection errors
#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("Connection lost: {0}")]
    Lost(String),

    #[error("Connection refused: {0}")]
    Refused(String),

    #[error("Connection timeout: {0}")]
    Timeout(String),
}

/// Result type for remoting operations
pub type RemotingResult<T> = Result<T, RemotingError>;

/// Result type for transport operations
pub type TransportResult<T> = Result<T, TransportError>;

/// Result type for cluster operations
pub type ClusterResult<T> = Result<T, ClusterError>;

/// Result type for addressing operations
pub type AddressingResult<T> = Result<T, AddressingError>;

/// Result type for connection operations
pub type ConnectionResult<T> = Result<T, ConnectionError>;
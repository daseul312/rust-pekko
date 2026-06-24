//! Serialization for remote messages

pub use crate::routing::MessageSerializer;

// Re-export serialization error
pub use crate::errors::SerializationError;

/// Serialization result type
pub type SerializationResult<T> = Result<T, SerializationError>;
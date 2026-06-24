//! # Serialization Registry - Type-safe Serialization Infrastructure
//!
//! Provides a registry-based serialization system inspired by Akka/Pekko's
//! serialization binding mechanism. Supports multiple codecs, compression,
//! and type-safe serializer lookup by type ID.
//!
//! ## Architecture
//! - **SerializerId**: Unique numeric identifier for each serializer
//! - **Codec**: Encoding format (Json, Bincode, MessagePack, Protobuf, Custom)
//! - **Compression**: Optional payload compression (None, Lz4, Zstd, Snappy)
//! - **SerializerEntry**: Registered serializer with codec + compression config
//! - **SerializationRegistry**: Central registry mapping type IDs → serializers

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

// ============================================================================
// Serializer Identity
// ============================================================================

/// Unique identifier for a registered serializer
pub type SerializerId = u32;

/// Type identifier for message types (used as registry key)
pub type TypeId = u64;

/// Auto-incrementing type ID generator
static NEXT_TYPE_ID: AtomicU64 = AtomicU64::new(1000);

/// Generate a new unique TypeId
pub fn next_type_id() -> TypeId {
    NEXT_TYPE_ID.fetch_add(1, Ordering::Relaxed)
}

// ============================================================================
// Codec
// ============================================================================

/// Serialization codec / wire format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Codec {
    /// JSON (text-based, human-readable)
    Json,
    /// Bincode (compact binary, Rust-native)
    Bincode,
    /// MessagePack (compact binary, cross-language)
    MessagePack,
    /// Protocol Buffers
    Protobuf,
    /// Raw bytes (no encoding)
    Raw,
    /// Custom codec with numeric identifier
    Custom(u16),
}

impl fmt::Display for Codec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Codec::Json => write!(f, "json"),
            Codec::Bincode => write!(f, "bincode"),
            Codec::MessagePack => write!(f, "msgpack"),
            Codec::Protobuf => write!(f, "protobuf"),
            Codec::Raw => write!(f, "raw"),
            Codec::Custom(id) => write!(f, "custom-{}", id),
        }
    }
}

// ============================================================================
// Compression
// ============================================================================

/// Compression algorithm for serialized payloads
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Compression {
    /// No compression
    None,
    /// LZ4 (fast compression/decompression)
    Lz4,
    /// Zstandard (high compression ratio)
    Zstd,
    /// Snappy (balanced speed/ratio)
    Snappy,
}

impl Compression {
    /// Compress data using this algorithm
    pub fn compress(&self, data: &[u8]) -> Vec<u8> {
        match self {
            Compression::None => data.to_vec(),
            Compression::Lz4 => {
                // Simulated LZ4: prepend marker + store original
                let mut out = vec![0x04]; // LZ4 marker
                out.extend_from_slice(&(data.len() as u32).to_le_bytes());
                out.extend_from_slice(data);
                out
            }
            Compression::Zstd => {
                let mut out = vec![0x28]; // ZSTD marker
                out.extend_from_slice(&(data.len() as u32).to_le_bytes());
                out.extend_from_slice(data);
                out
            }
            Compression::Snappy => {
                let mut out = vec![0x73]; // Snappy marker
                out.extend_from_slice(&(data.len() as u32).to_le_bytes());
                out.extend_from_slice(data);
                out
            }
        }
    }

    /// Decompress data using this algorithm
    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, SerializationRegistryError> {
        match self {
            Compression::None => Ok(data.to_vec()),
            Compression::Lz4 => {
                if data.len() < 5 || data[0] != 0x04 {
                    return Err(SerializationRegistryError::DecompressionFailed(
                        "Invalid LZ4 header".into(),
                    ));
                }
                let _orig_len =
                    u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
                Ok(data[5..].to_vec())
            }
            Compression::Zstd => {
                if data.len() < 5 || data[0] != 0x28 {
                    return Err(SerializationRegistryError::DecompressionFailed(
                        "Invalid ZSTD header".into(),
                    ));
                }
                Ok(data[5..].to_vec())
            }
            Compression::Snappy => {
                if data.len() < 5 || data[0] != 0x73 {
                    return Err(SerializationRegistryError::DecompressionFailed(
                        "Invalid Snappy header".into(),
                    ));
                }
                Ok(data[5..].to_vec())
            }
        }
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Compression::None => write!(f, "none"),
            Compression::Lz4 => write!(f, "lz4"),
            Compression::Zstd => write!(f, "zstd"),
            Compression::Snappy => write!(f, "snappy"),
        }
    }
}

// ============================================================================
// Errors
// ============================================================================

/// Serialization registry errors
#[derive(Debug, Clone)]
pub enum SerializationRegistryError {
    /// No serializer found for the given type
    SerializerNotFound(TypeId),
    /// Serializer ID already registered
    DuplicateSerializer(SerializerId),
    /// Type already bound to a different serializer
    TypeAlreadyBound { type_id: TypeId, existing: SerializerId },
    /// Serialization failed
    SerializationFailed(String),
    /// Deserialization failed
    DeserializationFailed(String),
    /// Decompression failed
    DecompressionFailed(String),
    /// Codec mismatch between header and expected
    CodecMismatch { expected: Codec, actual: Codec },
    /// Payload too large
    PayloadTooLarge { size: usize, max: usize },
}

impl fmt::Display for SerializationRegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SerializerNotFound(id) => write!(f, "No serializer for type {}", id),
            Self::DuplicateSerializer(id) => write!(f, "Serializer {} already registered", id),
            Self::TypeAlreadyBound { type_id, existing } => {
                write!(f, "Type {} already bound to serializer {}", type_id, existing)
            }
            Self::SerializationFailed(msg) => write!(f, "Serialization failed: {}", msg),
            Self::DeserializationFailed(msg) => write!(f, "Deserialization failed: {}", msg),
            Self::DecompressionFailed(msg) => write!(f, "Decompression failed: {}", msg),
            Self::CodecMismatch { expected, actual } => {
                write!(f, "Codec mismatch: expected {}, got {}", expected, actual)
            }
            Self::PayloadTooLarge { size, max } => {
                write!(f, "Payload too large: {} bytes (max {})", size, max)
            }
        }
    }
}

// ============================================================================
// Serializer Entry
// ============================================================================

/// A registered serializer configuration
#[derive(Debug, Clone)]
pub struct SerializerEntry {
    /// Unique serializer ID
    pub id: SerializerId,
    /// Human-readable name
    pub name: String,
    /// Codec used for encoding/decoding
    pub codec: Codec,
    /// Compression applied after serialization
    pub compression: Compression,
    /// Maximum payload size (0 = unlimited)
    pub max_payload_size: usize,
    /// Whether this serializer is enabled
    pub enabled: bool,
}

impl SerializerEntry {
    pub fn new(id: SerializerId, name: impl Into<String>, codec: Codec) -> Self {
        Self {
            id,
            name: name.into(),
            codec,
            compression: Compression::None,
            max_payload_size: 0,
            enabled: true,
        }
    }

    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_max_payload(mut self, max_bytes: usize) -> Self {
        self.max_payload_size = max_bytes;
        self
    }

    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }

    /// Simulate serialization (codec-specific encoding)
    pub fn serialize(&self, data: &[u8]) -> Result<Vec<u8>, SerializationRegistryError> {
        if !self.enabled {
            return Err(SerializationRegistryError::SerializationFailed(
                format!("Serializer '{}' is disabled", self.name),
            ));
        }

        if self.max_payload_size > 0 && data.len() > self.max_payload_size {
            return Err(SerializationRegistryError::PayloadTooLarge {
                size: data.len(),
                max: self.max_payload_size,
            });
        }

        // Build wire format: [serializer_id(4)] [codec(1)] [compression(1)] [payload...]
        let mut wire = Vec::with_capacity(6 + data.len());
        wire.extend_from_slice(&self.id.to_le_bytes());
        wire.push(codec_to_byte(self.codec));
        wire.push(compression_to_byte(self.compression));

        let compressed = self.compression.compress(data);
        wire.extend_from_slice(&compressed);
        Ok(wire)
    }

    /// Deserialize from wire format
    pub fn deserialize(&self, wire: &[u8]) -> Result<Vec<u8>, SerializationRegistryError> {
        if wire.len() < 6 {
            return Err(SerializationRegistryError::DeserializationFailed(
                "Wire data too short".into(),
            ));
        }

        let ser_id = u32::from_le_bytes([wire[0], wire[1], wire[2], wire[3]]);
        if ser_id != self.id {
            return Err(SerializationRegistryError::DeserializationFailed(
                format!("Serializer ID mismatch: expected {}, got {}", self.id, ser_id),
            ));
        }

        let wire_codec = byte_to_codec(wire[4]);
        if wire_codec != self.codec {
            return Err(SerializationRegistryError::CodecMismatch {
                expected: self.codec,
                actual: wire_codec,
            });
        }

        let wire_compression = byte_to_compression(wire[5]);
        wire_compression.decompress(&wire[6..])
    }
}

// ============================================================================
// Wire format helpers
// ============================================================================

fn codec_to_byte(codec: Codec) -> u8 {
    match codec {
        Codec::Json => 1,
        Codec::Bincode => 2,
        Codec::MessagePack => 3,
        Codec::Protobuf => 4,
        Codec::Raw => 5,
        Codec::Custom(id) => 128 + (id as u8),
    }
}

fn byte_to_codec(b: u8) -> Codec {
    match b {
        1 => Codec::Json,
        2 => Codec::Bincode,
        3 => Codec::MessagePack,
        4 => Codec::Protobuf,
        5 => Codec::Raw,
        n if n >= 128 => Codec::Custom((n - 128) as u16),
        _ => Codec::Raw,
    }
}

fn compression_to_byte(c: Compression) -> u8 {
    match c {
        Compression::None => 0,
        Compression::Lz4 => 1,
        Compression::Zstd => 2,
        Compression::Snappy => 3,
    }
}

fn byte_to_compression(b: u8) -> Compression {
    match b {
        0 => Compression::None,
        1 => Compression::Lz4,
        2 => Compression::Zstd,
        3 => Compression::Snappy,
        _ => Compression::None,
    }
}

// ============================================================================
// Type Binding
// ============================================================================

/// Binding from a type to its serializer
#[derive(Debug, Clone)]
pub struct TypeBinding {
    pub type_id: TypeId,
    pub type_name: String,
    pub serializer_id: SerializerId,
    pub registered_at: Instant,
}

// ============================================================================
// Serialization Registry
// ============================================================================

/// Central registry for serializers and type bindings.
///
/// Maps type IDs to serializer entries, allowing type-safe serialization
/// lookup at runtime (similar to Akka's serialization bindings in application.conf).
pub struct SerializationRegistry {
    /// Registered serializers by ID
    serializers: HashMap<SerializerId, SerializerEntry>,
    /// Type → serializer bindings
    type_bindings: HashMap<TypeId, TypeBinding>,
    /// Name → serializer ID index
    name_index: HashMap<String, SerializerId>,
    /// Fallback serializer ID (used when no binding exists)
    fallback_serializer: Option<SerializerId>,
    /// Stats
    stats: RegistryStats,
}

/// Registry usage statistics
#[derive(Debug, Clone, Default)]
pub struct RegistryStats {
    pub total_serializations: u64,
    pub total_deserializations: u64,
    pub total_bytes_serialized: u64,
    pub total_bytes_deserialized: u64,
    pub serialization_errors: u64,
    pub deserialization_errors: u64,
    pub fallback_used: u64,
}

impl SerializationRegistry {
    /// Create an empty registry
    pub fn new() -> Self {
        Self {
            serializers: HashMap::new(),
            type_bindings: HashMap::new(),
            name_index: HashMap::new(),
            fallback_serializer: None,
            stats: RegistryStats::default(),
        }
    }

    /// Create a registry with default serializers pre-registered
    pub fn with_defaults() -> Self {
        let mut reg = Self::new();
        reg.register_serializer(
            SerializerEntry::new(1, "json", Codec::Json),
        ).ok();
        reg.register_serializer(
            SerializerEntry::new(2, "bincode", Codec::Bincode),
        ).ok();
        reg.register_serializer(
            SerializerEntry::new(3, "msgpack", Codec::MessagePack),
        ).ok();
        reg.register_serializer(
            SerializerEntry::new(4, "protobuf", Codec::Protobuf),
        ).ok();
        reg.register_serializer(
            SerializerEntry::new(5, "raw", Codec::Raw),
        ).ok();
        reg.fallback_serializer = Some(2); // bincode as default
        reg
    }

    /// Register a new serializer
    pub fn register_serializer(
        &mut self,
        entry: SerializerEntry,
    ) -> Result<(), SerializationRegistryError> {
        if self.serializers.contains_key(&entry.id) {
            return Err(SerializationRegistryError::DuplicateSerializer(entry.id));
        }
        self.name_index.insert(entry.name.clone(), entry.id);
        self.serializers.insert(entry.id, entry);
        Ok(())
    }

    /// Unregister a serializer by ID
    pub fn unregister_serializer(&mut self, id: SerializerId) -> bool {
        if let Some(entry) = self.serializers.remove(&id) {
            self.name_index.remove(&entry.name);
            // Remove all type bindings pointing to this serializer
            self.type_bindings.retain(|_, binding| binding.serializer_id != id);
            if self.fallback_serializer == Some(id) {
                self.fallback_serializer = None;
            }
            true
        } else {
            false
        }
    }

    /// Bind a type to a serializer
    pub fn bind_type(
        &mut self,
        type_id: TypeId,
        type_name: impl Into<String>,
        serializer_id: SerializerId,
    ) -> Result<(), SerializationRegistryError> {
        // Check serializer exists
        if !self.serializers.contains_key(&serializer_id) {
            return Err(SerializationRegistryError::SerializerNotFound(type_id));
        }

        // Check for duplicate binding (allow rebind to same serializer)
        if let Some(existing) = self.type_bindings.get(&type_id) {
            if existing.serializer_id != serializer_id {
                return Err(SerializationRegistryError::TypeAlreadyBound {
                    type_id,
                    existing: existing.serializer_id,
                });
            }
        }

        self.type_bindings.insert(type_id, TypeBinding {
            type_id,
            type_name: type_name.into(),
            serializer_id,
            registered_at: Instant::now(),
        });
        Ok(())
    }

    /// Unbind a type
    pub fn unbind_type(&mut self, type_id: TypeId) -> bool {
        self.type_bindings.remove(&type_id).is_some()
    }

    /// Set the fallback serializer
    pub fn set_fallback(&mut self, serializer_id: SerializerId) -> Result<(), SerializationRegistryError> {
        if !self.serializers.contains_key(&serializer_id) {
            return Err(SerializationRegistryError::SerializerNotFound(0));
        }
        self.fallback_serializer = Some(serializer_id);
        Ok(())
    }

    /// Look up a serializer for a type
    pub fn serializer_for(&self, type_id: TypeId) -> Option<&SerializerEntry> {
        self.type_bindings
            .get(&type_id)
            .and_then(|binding| self.serializers.get(&binding.serializer_id))
            .or_else(|| {
                self.fallback_serializer
                    .and_then(|id| self.serializers.get(&id))
            })
    }

    /// Look up a serializer by name
    pub fn serializer_by_name(&self, name: &str) -> Option<&SerializerEntry> {
        self.name_index
            .get(name)
            .and_then(|id| self.serializers.get(id))
    }

    /// Look up a serializer by ID
    pub fn serializer_by_id(&self, id: SerializerId) -> Option<&SerializerEntry> {
        self.serializers.get(&id)
    }

    /// Serialize data for a given type
    pub fn serialize(
        &mut self,
        type_id: TypeId,
        data: &[u8],
    ) -> Result<Vec<u8>, SerializationRegistryError> {
        let is_fallback;
        let serializer_id;

        if let Some(binding) = self.type_bindings.get(&type_id) {
            serializer_id = binding.serializer_id;
            is_fallback = false;
        } else if let Some(fb) = self.fallback_serializer {
            serializer_id = fb;
            is_fallback = true;
        } else {
            self.stats.serialization_errors += 1;
            return Err(SerializationRegistryError::SerializerNotFound(type_id));
        }

        let serializer = self.serializers.get(&serializer_id)
            .ok_or(SerializationRegistryError::SerializerNotFound(type_id))?
            .clone();

        match serializer.serialize(data) {
            Ok(wire) => {
                self.stats.total_serializations += 1;
                self.stats.total_bytes_serialized += wire.len() as u64;
                if is_fallback {
                    self.stats.fallback_used += 1;
                }
                Ok(wire)
            }
            Err(e) => {
                self.stats.serialization_errors += 1;
                Err(e)
            }
        }
    }

    /// Deserialize data using the serializer ID embedded in the wire format
    pub fn deserialize(
        &mut self,
        wire: &[u8],
    ) -> Result<Vec<u8>, SerializationRegistryError> {
        if wire.len() < 6 {
            self.stats.deserialization_errors += 1;
            return Err(SerializationRegistryError::DeserializationFailed(
                "Wire data too short".into(),
            ));
        }

        let ser_id = u32::from_le_bytes([wire[0], wire[1], wire[2], wire[3]]);
        let serializer = self.serializers.get(&ser_id)
            .ok_or_else(|| {
                self.stats.deserialization_errors += 1;
                SerializationRegistryError::DeserializationFailed(
                    format!("Unknown serializer ID: {}", ser_id),
                )
            })?
            .clone();

        match serializer.deserialize(wire) {
            Ok(data) => {
                self.stats.total_deserializations += 1;
                self.stats.total_bytes_deserialized += data.len() as u64;
                Ok(data)
            }
            Err(e) => {
                self.stats.deserialization_errors += 1;
                Err(e)
            }
        }
    }

    /// Get number of registered serializers
    pub fn serializer_count(&self) -> usize {
        self.serializers.len()
    }

    /// Get number of type bindings
    pub fn binding_count(&self) -> usize {
        self.type_bindings.len()
    }

    /// Get all type bindings for a given serializer
    pub fn bindings_for_serializer(&self, serializer_id: SerializerId) -> Vec<&TypeBinding> {
        self.type_bindings
            .values()
            .filter(|b| b.serializer_id == serializer_id)
            .collect()
    }

    /// List all registered serializer names
    pub fn serializer_names(&self) -> Vec<&str> {
        self.serializers.values().map(|e| e.name.as_str()).collect()
    }

    /// Get registry statistics
    pub fn stats(&self) -> &RegistryStats {
        &self.stats
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = RegistryStats::default();
    }
}

impl Default for SerializationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // --- Codec & Compression Display ---

    #[test]
    fn test_codec_display() {
        assert_eq!(Codec::Json.to_string(), "json");
        assert_eq!(Codec::Bincode.to_string(), "bincode");
        assert_eq!(Codec::MessagePack.to_string(), "msgpack");
        assert_eq!(Codec::Protobuf.to_string(), "protobuf");
        assert_eq!(Codec::Raw.to_string(), "raw");
        assert_eq!(Codec::Custom(7).to_string(), "custom-7");
    }

    #[test]
    fn test_compression_display() {
        assert_eq!(Compression::None.to_string(), "none");
        assert_eq!(Compression::Lz4.to_string(), "lz4");
        assert_eq!(Compression::Zstd.to_string(), "zstd");
        assert_eq!(Compression::Snappy.to_string(), "snappy");
    }

    // --- Compression round-trip ---

    #[test]
    fn test_compression_none_roundtrip() {
        let data = b"hello world";
        let compressed = Compression::None.compress(data);
        let decompressed = Compression::None.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compression_lz4_roundtrip() {
        let data = b"repeated data repeated data repeated data";
        let compressed = Compression::Lz4.compress(data);
        assert_ne!(compressed, data); // has header
        let decompressed = Compression::Lz4.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compression_zstd_roundtrip() {
        let data = b"zstd compression test data";
        let compressed = Compression::Zstd.compress(data);
        let decompressed = Compression::Zstd.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compression_snappy_roundtrip() {
        let data = b"snappy is fast";
        let compressed = Compression::Snappy.compress(data);
        let decompressed = Compression::Snappy.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_decompression_invalid_header() {
        let bad_data = vec![0xFF, 0, 0, 0, 0]; // wrong marker
        assert!(Compression::Lz4.decompress(&bad_data).is_err());
        assert!(Compression::Zstd.decompress(&bad_data).is_err());
        assert!(Compression::Snappy.decompress(&bad_data).is_err());
    }

    #[test]
    fn test_decompression_too_short() {
        let short = vec![0x04, 0, 0];
        assert!(Compression::Lz4.decompress(&short).is_err());
    }

    // --- Codec wire encoding ---

    #[test]
    fn test_codec_byte_roundtrip() {
        let codecs = [Codec::Json, Codec::Bincode, Codec::MessagePack, Codec::Protobuf, Codec::Raw, Codec::Custom(3)];
        for codec in &codecs {
            let b = codec_to_byte(*codec);
            let decoded = byte_to_codec(b);
            assert_eq!(*codec, decoded, "Failed roundtrip for {:?}", codec);
        }
    }

    #[test]
    fn test_compression_byte_roundtrip() {
        let compressions = [Compression::None, Compression::Lz4, Compression::Zstd, Compression::Snappy];
        for c in &compressions {
            let b = compression_to_byte(*c);
            let decoded = byte_to_compression(b);
            assert_eq!(*c, decoded);
        }
    }

    // --- SerializerEntry ---

    #[test]
    fn test_serializer_entry_serialize_deserialize() {
        let entry = SerializerEntry::new(42, "test-json", Codec::Json);
        let data = b"hello world";
        let wire = entry.serialize(data).unwrap();
        let decoded = entry.deserialize(&wire).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_serializer_entry_with_compression() {
        let entry = SerializerEntry::new(10, "compressed", Codec::Bincode)
            .with_compression(Compression::Lz4);
        let data = b"compressed payload data";
        let wire = entry.serialize(data).unwrap();
        let decoded = entry.deserialize(&wire).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_serializer_entry_max_payload() {
        let entry = SerializerEntry::new(10, "limited", Codec::Raw)
            .with_max_payload(10);
        let small = b"short";
        assert!(entry.serialize(small).is_ok());
        let big = b"this is way too long for the limit";
        match entry.serialize(big) {
            Err(SerializationRegistryError::PayloadTooLarge { size, max }) => {
                assert_eq!(max, 10);
                assert_eq!(size, big.len());
            }
            other => panic!("Expected PayloadTooLarge, got {:?}", other),
        }
    }

    #[test]
    fn test_serializer_entry_disabled() {
        let entry = SerializerEntry::new(10, "disabled", Codec::Raw).disabled();
        assert!(!entry.enabled);
        assert!(entry.serialize(b"data").is_err());
    }

    #[test]
    fn test_serializer_entry_codec_mismatch() {
        let json_entry = SerializerEntry::new(10, "json", Codec::Json);
        let bincode_entry = SerializerEntry::new(10, "bincode", Codec::Bincode);
        let wire = json_entry.serialize(b"data").unwrap();
        match bincode_entry.deserialize(&wire) {
            Err(SerializationRegistryError::CodecMismatch { expected, actual }) => {
                assert_eq!(expected, Codec::Bincode);
                assert_eq!(actual, Codec::Json);
            }
            other => panic!("Expected CodecMismatch, got {:?}", other),
        }
    }

    #[test]
    fn test_serializer_entry_id_mismatch() {
        let entry1 = SerializerEntry::new(1, "a", Codec::Raw);
        let entry2 = SerializerEntry::new(2, "b", Codec::Raw);
        let wire = entry1.serialize(b"data").unwrap();
        assert!(entry2.deserialize(&wire).is_err());
    }

    #[test]
    fn test_deserialize_too_short() {
        let entry = SerializerEntry::new(1, "a", Codec::Raw);
        assert!(entry.deserialize(&[0, 1, 2]).is_err());
    }

    // --- Registry ---

    #[test]
    fn test_registry_new_empty() {
        let reg = SerializationRegistry::new();
        assert_eq!(reg.serializer_count(), 0);
        assert_eq!(reg.binding_count(), 0);
    }

    #[test]
    fn test_registry_with_defaults() {
        let reg = SerializationRegistry::with_defaults();
        assert_eq!(reg.serializer_count(), 5);
        assert!(reg.serializer_by_name("json").is_some());
        assert!(reg.serializer_by_name("bincode").is_some());
        assert!(reg.serializer_by_name("msgpack").is_some());
        assert!(reg.serializer_by_name("protobuf").is_some());
        assert!(reg.serializer_by_name("raw").is_some());
    }

    #[test]
    fn test_register_serializer() {
        let mut reg = SerializationRegistry::new();
        let entry = SerializerEntry::new(100, "custom", Codec::Json);
        reg.register_serializer(entry).unwrap();
        assert_eq!(reg.serializer_count(), 1);
        assert!(reg.serializer_by_id(100).is_some());
        assert!(reg.serializer_by_name("custom").is_some());
    }

    #[test]
    fn test_register_duplicate_serializer() {
        let mut reg = SerializationRegistry::new();
        reg.register_serializer(SerializerEntry::new(1, "a", Codec::Raw)).unwrap();
        match reg.register_serializer(SerializerEntry::new(1, "b", Codec::Json)) {
            Err(SerializationRegistryError::DuplicateSerializer(1)) => {}
            other => panic!("Expected DuplicateSerializer, got {:?}", other),
        }
    }

    #[test]
    fn test_unregister_serializer() {
        let mut reg = SerializationRegistry::with_defaults();
        assert!(reg.serializer_by_name("json").is_some());
        reg.unregister_serializer(1);
        assert!(reg.serializer_by_name("json").is_none());
        assert_eq!(reg.serializer_count(), 4);
    }

    #[test]
    fn test_unregister_cleans_bindings() {
        let mut reg = SerializationRegistry::new();
        reg.register_serializer(SerializerEntry::new(1, "a", Codec::Raw)).unwrap();
        reg.bind_type(100, "MyType", 1).unwrap();
        assert_eq!(reg.binding_count(), 1);
        reg.unregister_serializer(1);
        assert_eq!(reg.binding_count(), 0);
    }

    #[test]
    fn test_unregister_clears_fallback() {
        let mut reg = SerializationRegistry::new();
        reg.register_serializer(SerializerEntry::new(1, "a", Codec::Raw)).unwrap();
        reg.set_fallback(1).unwrap();
        reg.unregister_serializer(1);
        assert!(reg.fallback_serializer.is_none());
    }

    #[test]
    fn test_bind_type() {
        let mut reg = SerializationRegistry::with_defaults();
        reg.bind_type(1001, "OrderEvent", 1).unwrap(); // bind to json
        assert_eq!(reg.binding_count(), 1);
        let ser = reg.serializer_for(1001).unwrap();
        assert_eq!(ser.codec, Codec::Json);
    }

    #[test]
    fn test_bind_type_nonexistent_serializer() {
        let mut reg = SerializationRegistry::new();
        assert!(reg.bind_type(1, "Foo", 999).is_err());
    }

    #[test]
    fn test_bind_type_duplicate_different() {
        let mut reg = SerializationRegistry::with_defaults();
        reg.bind_type(100, "T", 1).unwrap();
        match reg.bind_type(100, "T", 2) {
            Err(SerializationRegistryError::TypeAlreadyBound { type_id: 100, existing: 1 }) => {}
            other => panic!("Expected TypeAlreadyBound, got {:?}", other),
        }
    }

    #[test]
    fn test_bind_type_same_serializer_ok() {
        let mut reg = SerializationRegistry::with_defaults();
        reg.bind_type(100, "T", 1).unwrap();
        // Re-binding to same serializer is OK
        reg.bind_type(100, "T", 1).unwrap();
    }

    #[test]
    fn test_unbind_type() {
        let mut reg = SerializationRegistry::with_defaults();
        reg.bind_type(100, "T", 1).unwrap();
        assert!(reg.unbind_type(100));
        assert!(!reg.unbind_type(100)); // already removed
        assert_eq!(reg.binding_count(), 0);
    }

    #[test]
    fn test_serializer_for_with_fallback() {
        let mut reg = SerializationRegistry::with_defaults();
        // No binding for type 9999, but fallback is set to bincode (ID=2)
        let ser = reg.serializer_for(9999).unwrap();
        assert_eq!(ser.codec, Codec::Bincode);
    }

    #[test]
    fn test_serializer_for_no_fallback() {
        let reg = SerializationRegistry::new();
        assert!(reg.serializer_for(9999).is_none());
    }

    #[test]
    fn test_serialize_deserialize_via_registry() {
        let mut reg = SerializationRegistry::with_defaults();
        reg.bind_type(500, "TestMsg", 1).unwrap(); // json serializer

        let data = b"test payload";
        let wire = reg.serialize(500, data).unwrap();
        let decoded = reg.deserialize(&wire).unwrap();
        assert_eq!(decoded, data);
        assert_eq!(reg.stats().total_serializations, 1);
        assert_eq!(reg.stats().total_deserializations, 1);
    }

    #[test]
    fn test_serialize_with_fallback() {
        let mut reg = SerializationRegistry::with_defaults();
        // No binding, uses fallback (bincode)
        let data = b"fallback data";
        let wire = reg.serialize(9999, data).unwrap();
        let decoded = reg.deserialize(&wire).unwrap();
        assert_eq!(decoded, data);
        assert_eq!(reg.stats().fallback_used, 1);
    }

    #[test]
    fn test_serialize_no_serializer() {
        let mut reg = SerializationRegistry::new(); // no serializers, no fallback
        match reg.serialize(1, b"data") {
            Err(SerializationRegistryError::SerializerNotFound(_)) => {}
            other => panic!("Expected SerializerNotFound, got {:?}", other),
        }
        assert_eq!(reg.stats().serialization_errors, 1);
    }

    #[test]
    fn test_deserialize_unknown_serializer() {
        let mut reg = SerializationRegistry::new();
        // Fabricate wire with serializer ID 99
        let mut wire = vec![99, 0, 0, 0, 1, 0]; // id=99, codec=json, compression=none
        wire.extend_from_slice(b"fake");
        assert!(reg.deserialize(&wire).is_err());
        assert_eq!(reg.stats().deserialization_errors, 1);
    }

    #[test]
    fn test_deserialize_too_short_wire() {
        let mut reg = SerializationRegistry::new();
        assert!(reg.deserialize(&[0, 1]).is_err());
    }

    #[test]
    fn test_serialize_with_compression() {
        let mut reg = SerializationRegistry::new();
        let entry = SerializerEntry::new(50, "compressed-json", Codec::Json)
            .with_compression(Compression::Zstd);
        reg.register_serializer(entry).unwrap();
        reg.bind_type(700, "CompressedMsg", 50).unwrap();

        let data = b"compressed message payload";
        let wire = reg.serialize(700, data).unwrap();
        let decoded = reg.deserialize(&wire).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_bindings_for_serializer() {
        let mut reg = SerializationRegistry::with_defaults();
        reg.bind_type(100, "A", 1).unwrap();
        reg.bind_type(200, "B", 1).unwrap();
        reg.bind_type(300, "C", 2).unwrap();
        let json_bindings = reg.bindings_for_serializer(1);
        assert_eq!(json_bindings.len(), 2);
        let bincode_bindings = reg.bindings_for_serializer(2);
        assert_eq!(bincode_bindings.len(), 1);
    }

    #[test]
    fn test_serializer_names() {
        let reg = SerializationRegistry::with_defaults();
        let mut names = reg.serializer_names();
        names.sort();
        assert_eq!(names, vec!["bincode", "json", "msgpack", "protobuf", "raw"]);
    }

    #[test]
    fn test_reset_stats() {
        let mut reg = SerializationRegistry::with_defaults();
        reg.serialize(1, b"data").unwrap();
        assert!(reg.stats().total_serializations > 0);
        reg.reset_stats();
        assert_eq!(reg.stats().total_serializations, 0);
    }

    #[test]
    fn test_next_type_id_unique() {
        let id1 = next_type_id();
        let id2 = next_type_id();
        let id3 = next_type_id();
        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert!(id2 > id1);
    }

    #[test]
    fn test_error_display() {
        let err = SerializationRegistryError::SerializerNotFound(42);
        assert!(err.to_string().contains("42"));

        let err = SerializationRegistryError::PayloadTooLarge { size: 1000, max: 100 };
        assert!(err.to_string().contains("1000"));
        assert!(err.to_string().contains("100"));

        let err = SerializationRegistryError::CodecMismatch {
            expected: Codec::Json,
            actual: Codec::Bincode,
        };
        assert!(err.to_string().contains("json"));
        assert!(err.to_string().contains("bincode"));
    }

    #[test]
    fn test_set_fallback_nonexistent() {
        let mut reg = SerializationRegistry::new();
        assert!(reg.set_fallback(999).is_err());
    }

    #[test]
    fn test_empty_data_roundtrip() {
        let entry = SerializerEntry::new(1, "raw", Codec::Raw);
        let wire = entry.serialize(b"").unwrap();
        let decoded = entry.deserialize(&wire).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_large_payload_roundtrip() {
        let entry = SerializerEntry::new(1, "raw", Codec::Raw)
            .with_compression(Compression::Snappy);
        let data: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();
        let wire = entry.serialize(&data).unwrap();
        let decoded = entry.deserialize(&wire).unwrap();
        assert_eq!(decoded, data);
    }
}

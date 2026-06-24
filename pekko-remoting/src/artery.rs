//! Artery Transport - High-performance transport for Pekko Remoting
//!
//! Artery is modeled after Scala Pekko's Artery transport, designed for
//! high-throughput, low-latency inter-node communication.
//!
//! Features:
//! - Multiplexed streams over a single connection (control, ordinary, large messages)
//! - Built-in compression for actor refs and manifests
//! - Flight recorder for debugging
//! - Handshake protocol with version negotiation
//! - Idle detection and connection lifecycle management

use crate::addressing::RemoteAddress;
use crate::errors::{TransportError, TransportResult};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut, Buf, BufMut};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tracing::{debug, error, warn};

// ============= Stream Priorities =============

/// Stream ID for multiplexed channels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StreamId {
    /// Control messages (highest priority): handshake, heartbeat, system messages
    Control = 0,
    /// Ordinary messages: regular actor messages
    Ordinary = 1,
    /// Large messages: messages exceeding the large-message threshold
    Large = 2,
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamId::Control => write!(f, "Control"),
            StreamId::Ordinary => write!(f, "Ordinary"),
            StreamId::Large => write!(f, "Large"),
        }
    }
}

// ============= Artery Configuration =============

/// Configuration for the Artery transport
#[derive(Debug, Clone)]
pub struct ArteryConfig {
    /// Transport protocol variant
    pub transport: ArteryTransportMode,
    /// Bind hostname
    pub bind_hostname: String,
    /// Bind port (0 for auto-assign)
    pub bind_port: u16,
    /// Large message threshold in bytes
    pub large_message_threshold: usize,
    /// Buffer size for ordinary messages
    pub ordinary_buffer_size: usize,
    /// Buffer size for large messages
    pub large_buffer_size: usize,
    /// Buffer size for control messages
    pub control_buffer_size: usize,
    /// Handshake timeout
    pub handshake_timeout: Duration,
    /// Idle timeout before closing connections
    pub idle_timeout: Duration,
    /// Enable flight recorder for debugging
    pub flight_recorder_enabled: bool,
    /// Maximum flight recorder entries
    pub flight_recorder_size: usize,
    /// Enable compression of actor refs
    pub compression_enabled: bool,
    /// Maximum frame size
    pub max_frame_size: usize,
}

impl Default for ArteryConfig {
    fn default() -> Self {
        Self {
            transport: ArteryTransportMode::Tcp,
            bind_hostname: "127.0.0.1".to_string(),
            bind_port: 25520,
            large_message_threshold: 256 * 1024, // 256 KB
            ordinary_buffer_size: 1024,
            large_buffer_size: 64,
            control_buffer_size: 256,
            handshake_timeout: Duration::from_secs(20),
            idle_timeout: Duration::from_secs(300),
            flight_recorder_enabled: false,
            flight_recorder_size: 10000,
            compression_enabled: true,
            max_frame_size: 4 * 1024 * 1024, // 4 MB
        }
    }
}

/// Transport mode variants
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArteryTransportMode {
    /// TCP with Pekko Streams framing
    Tcp,
    /// TCP with TLS encryption
    TlsTcp,
    /// UDP (Aeron) - placeholder for future implementation
    Aeron,
}

// ============= Handshake Protocol =============

/// Protocol version for compatibility negotiation
pub const ARTERY_PROTOCOL_VERSION: u32 = 1;

/// Handshake request sent by the initiating node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeRequest {
    pub origin_address: RemoteAddress,
    pub protocol_version: u32,
    pub cookie: Option<String>,
    pub capabilities: Vec<String>,
}

/// Handshake response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeResponse {
    pub status: HandshakeStatus,
    pub remote_address: RemoteAddress,
    pub protocol_version: u32,
    pub capabilities: Vec<String>,
}

/// Handshake status codes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HandshakeStatus {
    /// Handshake accepted
    Accepted,
    /// Version mismatch
    IncompatibleVersion,
    /// Authentication failed
    AuthenticationFailed,
    /// Rejected for other reasons
    Rejected,
}

// ============= Artery Envelope =============

/// An envelope carrying a message through Artery transport
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArteryEnvelope {
    /// Which stream this message belongs to
    pub stream_id: StreamId,
    /// Sequence number for ordering
    pub seq_no: u64,
    /// Sender address
    pub sender: Option<String>,
    /// Recipient address (actor path)
    pub recipient: String,
    /// Serializer ID
    pub serializer_id: u32,
    /// Message manifest (type info)
    pub manifest: String,
    /// Serialized message payload
    pub payload: Vec<u8>,
}

impl ArteryEnvelope {
    /// Create an ordinary message envelope
    pub fn ordinary(recipient: String, serializer_id: u32, manifest: String, payload: Vec<u8>) -> Self {
        let stream = if payload.len() > 256 * 1024 {
            StreamId::Large
        } else {
            StreamId::Ordinary
        };
        Self {
            stream_id: stream,
            seq_no: 0, // Set by the transport
            sender: None,
            recipient,
            serializer_id,
            manifest,
            payload,
        }
    }

    /// Create a control message envelope
    pub fn control(payload: Vec<u8>) -> Self {
        Self {
            stream_id: StreamId::Control,
            seq_no: 0,
            sender: None,
            recipient: String::new(),
            serializer_id: 0,
            manifest: "control".to_string(),
            payload,
        }
    }

    /// Total size of this envelope
    pub fn size(&self) -> usize {
        self.payload.len() + self.recipient.len() + self.manifest.len() + 32 // approximate overhead
    }
}

// ============= Flight Recorder =============

/// Flight recorder for debugging transport issues
#[derive(Debug)]
pub struct FlightRecorder {
    entries: Vec<FlightRecorderEntry>,
    max_size: usize,
    enabled: bool,
}

/// A single flight recorder entry
#[derive(Debug, Clone)]
pub struct FlightRecorderEntry {
    pub timestamp: Instant,
    pub event: FlightEvent,
    pub remote: Option<RemoteAddress>,
    pub detail: String,
}

/// Flight recorder event types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlightEvent {
    ConnectionEstablished,
    ConnectionClosed,
    HandshakeStarted,
    HandshakeCompleted,
    HandshakeFailed,
    MessageSent,
    MessageReceived,
    HeartbeatSent,
    HeartbeatReceived,
    HeartbeatTimeout,
    CompressionTableUpdate,
    Error,
}

impl FlightRecorder {
    pub fn new(max_size: usize, enabled: bool) -> Self {
        Self {
            entries: Vec::with_capacity(max_size.min(1000)),
            max_size,
            enabled,
        }
    }

    /// Record an event
    pub fn record(&mut self, event: FlightEvent, remote: Option<RemoteAddress>, detail: String) {
        if !self.enabled {
            return;
        }

        if self.entries.len() >= self.max_size {
            self.entries.remove(0);
        }

        self.entries.push(FlightRecorderEntry {
            timestamp: Instant::now(),
            event,
            remote,
            detail,
        });
    }

    /// Get recent entries
    pub fn recent(&self, count: usize) -> &[FlightRecorderEntry] {
        let start = self.entries.len().saturating_sub(count);
        &self.entries[start..]
    }

    /// Get entries by event type
    pub fn by_event(&self, event: &FlightEvent) -> Vec<&FlightRecorderEntry> {
        self.entries.iter().filter(|e| &e.event == event).collect()
    }

    /// Total number of recorded entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if recorder is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Clear all entries
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

// ============= Compression Tables =============

/// Compression table for reducing repeated actor ref strings
#[derive(Debug, Clone)]
pub struct CompressionTable {
    /// Map from full string to compressed ID
    forward: HashMap<String, u32>,
    /// Map from compressed ID to full string
    reverse: HashMap<u32, String>,
    /// Next available ID
    next_id: u32,
    /// Maximum table size
    max_entries: usize,
}

impl CompressionTable {
    pub fn new(max_entries: usize) -> Self {
        Self {
            forward: HashMap::new(),
            reverse: HashMap::new(),
            next_id: 1, // 0 = uncompressed
            max_entries,
        }
    }

    /// Compress a string, returning its ID. Returns 0 if not in table.
    pub fn compress(&mut self, value: &str) -> u32 {
        if let Some(&id) = self.forward.get(value) {
            return id;
        }

        if self.forward.len() >= self.max_entries {
            return 0; // Table full, send uncompressed
        }

        let id = self.next_id;
        self.next_id += 1;
        self.forward.insert(value.to_string(), id);
        self.reverse.insert(id, value.to_string());
        id
    }

    /// Decompress an ID back to the original string
    pub fn decompress(&self, id: u32) -> Option<&str> {
        self.reverse.get(&id).map(|s| s.as_str())
    }

    /// Register a value without returning an ID (for incoming table updates)
    pub fn register(&mut self, id: u32, value: String) {
        self.forward.insert(value.clone(), id);
        self.reverse.insert(id, value);
        if id >= self.next_id {
            self.next_id = id + 1;
        }
    }

    /// Number of entries in the table
    pub fn len(&self) -> usize {
        self.forward.len()
    }

    /// Check if table is empty
    pub fn is_empty(&self) -> bool {
        self.forward.is_empty()
    }
}

// ============= Association (Connection) =============

/// State of an association (connection) between two nodes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssociationState {
    /// Initial state, handshake not started
    New,
    /// Handshake in progress
    Handshaking,
    /// Fully connected
    Active,
    /// Idle (no recent activity)
    Idle,
    /// Connection is closing
    Closing,
    /// Connection closed
    Closed,
}

/// An association represents a connection to a remote node
pub struct Association {
    pub remote_address: RemoteAddress,
    pub state: AssociationState,
    /// Separate channels for each stream priority
    pub control_tx: mpsc::Sender<ArteryEnvelope>,
    pub ordinary_tx: mpsc::Sender<ArteryEnvelope>,
    pub large_tx: mpsc::Sender<ArteryEnvelope>,
    /// Sequence counter
    seq_counter: AtomicU64,
    /// Compression tables (one per direction)
    pub actor_ref_compression: CompressionTable,
    pub manifest_compression: CompressionTable,
    /// Statistics
    pub stats: AssociationStats,
    /// Last activity timestamp
    pub last_activity: Instant,
}

/// Per-association statistics
#[derive(Debug, Clone, Default)]
pub struct AssociationStats {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub control_messages: u64,
    pub large_messages: u64,
    pub compression_hits: u64,
    pub compression_misses: u64,
}

impl Association {
    pub fn new(
        remote_address: RemoteAddress,
        control_tx: mpsc::Sender<ArteryEnvelope>,
        ordinary_tx: mpsc::Sender<ArteryEnvelope>,
        large_tx: mpsc::Sender<ArteryEnvelope>,
    ) -> Self {
        Self {
            remote_address,
            state: AssociationState::New,
            control_tx,
            ordinary_tx,
            large_tx,
            seq_counter: AtomicU64::new(1),
            actor_ref_compression: CompressionTable::new(256),
            manifest_compression: CompressionTable::new(256),
            stats: AssociationStats::default(),
            last_activity: Instant::now(),
        }
    }

    /// Send an envelope through the appropriate stream
    pub async fn send(&mut self, mut envelope: ArteryEnvelope) -> TransportResult<()> {
        envelope.seq_no = self.seq_counter.fetch_add(1, Ordering::Relaxed);
        self.last_activity = Instant::now();

        let result = match envelope.stream_id {
            StreamId::Control => {
                self.stats.control_messages += 1;
                self.control_tx.send(envelope).await
            }
            StreamId::Ordinary => {
                self.ordinary_tx.send(envelope).await
            }
            StreamId::Large => {
                self.stats.large_messages += 1;
                self.large_tx.send(envelope).await
            }
        };

        match result {
            Ok(()) => {
                self.stats.messages_sent += 1;
                Ok(())
            }
            Err(_) => Err(TransportError::SendFailed("Association channel closed".to_string())),
        }
    }

    /// Get next sequence number
    pub fn next_seq(&self) -> u64 {
        self.seq_counter.load(Ordering::Relaxed)
    }

    /// Check if the association is idle
    pub fn is_idle(&self, timeout: Duration) -> bool {
        self.last_activity.elapsed() > timeout
    }

    /// Touch the association (update last activity)
    pub fn touch(&mut self) {
        self.last_activity = Instant::now();
    }
}

// ============= Artery Transport =============

/// Artery transport system managing all associations
pub struct ArteryTransport {
    config: ArteryConfig,
    local_address: Option<RemoteAddress>,
    associations: Arc<RwLock<HashMap<RemoteAddress, AssociationHandle>>>,
    flight_recorder: Arc<Mutex<FlightRecorder>>,
    running: Arc<AtomicBool>,
    stats: Arc<RwLock<ArteryTransportStats>>,
}

/// Handle to an association for thread-safe access
pub struct AssociationHandle {
    pub remote_address: RemoteAddress,
    pub state: AssociationState,
    pub control_tx: mpsc::Sender<ArteryEnvelope>,
    pub ordinary_tx: mpsc::Sender<ArteryEnvelope>,
    pub large_tx: mpsc::Sender<ArteryEnvelope>,
    // Keep receivers alive so senders don't fail
    _control_rx: Arc<Mutex<mpsc::Receiver<ArteryEnvelope>>>,
    _ordinary_rx: Arc<Mutex<mpsc::Receiver<ArteryEnvelope>>>,
    _large_rx: Arc<Mutex<mpsc::Receiver<ArteryEnvelope>>>,
    pub stats: AssociationStats,
    seq_counter: Arc<AtomicU64>,
    last_activity: Arc<RwLock<Instant>>,
}

impl AssociationHandle {
    /// Send an envelope through the appropriate channel
    pub async fn send(&self, mut envelope: ArteryEnvelope) -> TransportResult<()> {
        envelope.seq_no = self.seq_counter.fetch_add(1, Ordering::Relaxed);

        let result = match envelope.stream_id {
            StreamId::Control => self.control_tx.send(envelope).await,
            StreamId::Ordinary => self.ordinary_tx.send(envelope).await,
            StreamId::Large => self.large_tx.send(envelope).await,
        };

        result.map_err(|_| TransportError::SendFailed("Channel closed".to_string()))?;

        // Update last activity
        *self.last_activity.write().await = Instant::now();
        Ok(())
    }
}

/// Global transport statistics
#[derive(Debug, Clone, Default)]
pub struct ArteryTransportStats {
    pub total_associations: u64,
    pub active_associations: u64,
    pub total_messages_sent: u64,
    pub total_messages_received: u64,
    pub total_bytes_sent: u64,
    pub total_bytes_received: u64,
    pub handshake_successes: u64,
    pub handshake_failures: u64,
}

impl ArteryTransport {
    pub fn new(config: ArteryConfig) -> Self {
        let recorder = FlightRecorder::new(
            config.flight_recorder_size,
            config.flight_recorder_enabled,
        );

        Self {
            config,
            local_address: None,
            associations: Arc::new(RwLock::new(HashMap::new())),
            flight_recorder: Arc::new(Mutex::new(recorder)),
            running: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(RwLock::new(ArteryTransportStats::default())),
        }
    }

    /// Start the Artery transport
    pub async fn start(&mut self) -> TransportResult<()> {
        if self.running.load(Ordering::Relaxed) {
            return Err(TransportError::ProtocolError("Already running".to_string()));
        }

        self.local_address = Some(RemoteAddress::new(
            "pekko.artery",
            "system",
            &self.config.bind_hostname,
            self.config.bind_port,
        ));

        self.running.store(true, Ordering::Relaxed);
        // Record in flight recorder
        let mut recorder = self.flight_recorder.lock().await;
        recorder.record(
            FlightEvent::ConnectionEstablished,
            self.local_address.clone(),
            "Transport started".to_string(),
        );

        Ok(())
    }

    /// Stop the Artery transport
    pub async fn stop(&mut self) -> TransportResult<()> {
        self.running.store(false, Ordering::Relaxed);

        // Close all associations
        let mut assocs = self.associations.write().await;
        assocs.clear();
        let mut recorder = self.flight_recorder.lock().await;
        recorder.record(
            FlightEvent::ConnectionClosed,
            self.local_address.clone(),
            "Transport stopped".to_string(),
        );

        Ok(())
    }

    /// Create a new association to a remote node
    pub async fn associate(&self, remote: RemoteAddress) -> TransportResult<()> {
        if !self.running.load(Ordering::Relaxed) {
            return Err(TransportError::NotConnected);
        }

        let (control_tx, control_rx) = mpsc::channel(self.config.control_buffer_size);
        let (ordinary_tx, ordinary_rx) = mpsc::channel(self.config.ordinary_buffer_size);
        let (large_tx, large_rx) = mpsc::channel(self.config.large_buffer_size);

        let handle = AssociationHandle {
            remote_address: remote.clone(),
            state: AssociationState::Active,
            control_tx,
            ordinary_tx,
            large_tx,
            _control_rx: Arc::new(Mutex::new(control_rx)),
            _ordinary_rx: Arc::new(Mutex::new(ordinary_rx)),
            _large_rx: Arc::new(Mutex::new(large_rx)),
            stats: AssociationStats::default(),
            seq_counter: Arc::new(AtomicU64::new(1)),
            last_activity: Arc::new(RwLock::new(Instant::now())),
        };

        let mut assocs = self.associations.write().await;
        assocs.insert(remote.clone(), handle);

        let mut stats = self.stats.write().await;
        stats.total_associations += 1;
        stats.active_associations += 1;

        let mut recorder = self.flight_recorder.lock().await;
        recorder.record(
            FlightEvent::ConnectionEstablished,
            Some(remote),
            "Association created".to_string(),
        );

        Ok(())
    }

    /// Send a message to a remote node
    pub async fn send(&self, remote: &RemoteAddress, envelope: ArteryEnvelope) -> TransportResult<()> {
        let assocs = self.associations.read().await;
        if let Some(handle) = assocs.get(remote) {
            handle.send(envelope).await?;

            let mut stats = self.stats.write().await;
            stats.total_messages_sent += 1;
            Ok(())
        } else {
            Err(TransportError::NotConnected)
        }
    }

    /// Perform a handshake with a remote node
    pub async fn handshake(&self, remote: &RemoteAddress, cookie: Option<String>) -> TransportResult<HandshakeResponse> {
        let mut recorder = self.flight_recorder.lock().await;
        recorder.record(
            FlightEvent::HandshakeStarted,
            Some(remote.clone()),
            "Initiating handshake".to_string(),
        );
        drop(recorder);

        let _request = HandshakeRequest {
            origin_address: self.local_address.clone().ok_or(TransportError::NotConnected)?,
            protocol_version: ARTERY_PROTOCOL_VERSION,
            cookie,
            capabilities: vec!["compression".to_string(), "large-messages".to_string()],
        };

        // In a real implementation, this would send the request over the network
        // and await the response. For now, simulate a successful handshake.
        let response = HandshakeResponse {
            status: HandshakeStatus::Accepted,
            remote_address: remote.clone(),
            protocol_version: ARTERY_PROTOCOL_VERSION,
            capabilities: vec!["compression".to_string()],
        };

        let mut recorder = self.flight_recorder.lock().await;
        match response.status {
            HandshakeStatus::Accepted => {
                recorder.record(
                    FlightEvent::HandshakeCompleted,
                    Some(remote.clone()),
                    "Handshake accepted".to_string(),
                );
                let mut stats = self.stats.write().await;
                stats.handshake_successes += 1;
            }
            _ => {
                recorder.record(
                    FlightEvent::HandshakeFailed,
                    Some(remote.clone()),
                    format!("Handshake failed: {:?}", response.status),
                );
                let mut stats = self.stats.write().await;
                stats.handshake_failures += 1;
            }
        }

        Ok(response)
    }

    /// Close an association
    pub async fn disassociate(&self, remote: &RemoteAddress) -> TransportResult<()> {
        let mut assocs = self.associations.write().await;
        if assocs.remove(remote).is_some() {
            let mut stats = self.stats.write().await;
            stats.active_associations = stats.active_associations.saturating_sub(1);

            let mut recorder = self.flight_recorder.lock().await;
            recorder.record(
                FlightEvent::ConnectionClosed,
                Some(remote.clone()),
                "Association closed".to_string(),
            );
            Ok(())
        } else {
            Err(TransportError::NotConnected)
        }
    }

    /// Get transport statistics
    pub async fn stats(&self) -> ArteryTransportStats {
        self.stats.read().await.clone()
    }

    /// Get flight recorder snapshot
    pub async fn flight_recorder_entries(&self, count: usize) -> Vec<FlightRecorderEntry> {
        let recorder = self.flight_recorder.lock().await;
        recorder.recent(count).to_vec()
    }

    /// Number of active associations
    pub async fn association_count(&self) -> usize {
        self.associations.read().await.len()
    }

    /// Check if running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Get local address
    pub fn local_address(&self) -> Option<&RemoteAddress> {
        self.local_address.as_ref()
    }
}

// ============= Idle Detection =============

/// Manages idle detection for associations
pub struct IdleDetector {
    timeout: Duration,
}

impl IdleDetector {
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    /// Check associations and return addresses of idle ones
    pub async fn detect_idle(&self, associations: &HashMap<RemoteAddress, AssociationHandle>) -> Vec<RemoteAddress> {
        let mut idle = Vec::new();
        for (addr, handle) in associations {
            let last = *handle.last_activity.read().await;
            if last.elapsed() > self.timeout {
                idle.push(addr.clone());
            }
        }
        idle
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== ArteryConfig Tests =====

    #[test]
    fn test_artery_config_defaults() {
        let config = ArteryConfig::default();
        assert_eq!(config.bind_port, 25520);
        assert_eq!(config.transport, ArteryTransportMode::Tcp);
        assert_eq!(config.large_message_threshold, 256 * 1024);
        assert!(config.compression_enabled);
    }

    // ===== StreamId Tests =====

    #[test]
    fn test_stream_id_display() {
        assert_eq!(format!("{}", StreamId::Control), "Control");
        assert_eq!(format!("{}", StreamId::Ordinary), "Ordinary");
        assert_eq!(format!("{}", StreamId::Large), "Large");
    }

    // ===== ArteryEnvelope Tests =====

    #[test]
    fn test_ordinary_envelope_small() {
        let env = ArteryEnvelope::ordinary(
            "/user/actor-1".to_string(),
            1,
            "test.Message".to_string(),
            vec![1, 2, 3],
        );
        assert_eq!(env.stream_id, StreamId::Ordinary);
        assert_eq!(env.recipient, "/user/actor-1");
    }

    #[test]
    fn test_ordinary_envelope_large() {
        let payload = vec![0u8; 300 * 1024]; // 300KB > threshold
        let env = ArteryEnvelope::ordinary(
            "/user/actor-1".to_string(),
            1,
            "test.LargeMessage".to_string(),
            payload,
        );
        assert_eq!(env.stream_id, StreamId::Large);
    }

    #[test]
    fn test_control_envelope() {
        let env = ArteryEnvelope::control(vec![1, 2, 3]);
        assert_eq!(env.stream_id, StreamId::Control);
        assert_eq!(env.manifest, "control");
    }

    // ===== CompressionTable Tests =====

    #[test]
    fn test_compression_table_basic() {
        let mut table = CompressionTable::new(100);

        let id1 = table.compress("/user/actor-1");
        assert!(id1 > 0);

        let id2 = table.compress("/user/actor-1");
        assert_eq!(id1, id2); // Same string → same ID

        let id3 = table.compress("/user/actor-2");
        assert_ne!(id1, id3); // Different string → different ID

        assert_eq!(table.decompress(id1), Some("/user/actor-1"));
        assert_eq!(table.decompress(id3), Some("/user/actor-2"));
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn test_compression_table_full() {
        let mut table = CompressionTable::new(2);

        let id1 = table.compress("a");
        let id2 = table.compress("b");
        let id3 = table.compress("c"); // Table full

        assert!(id1 > 0);
        assert!(id2 > 0);
        assert_eq!(id3, 0); // Uncompressed sentinel
    }

    #[test]
    fn test_compression_table_register() {
        let mut table = CompressionTable::new(100);
        table.register(42, "test-value".to_string());

        assert_eq!(table.decompress(42), Some("test-value"));
        assert_eq!(table.compress("test-value"), 42);
    }

    // ===== FlightRecorder Tests =====

    #[test]
    fn test_flight_recorder_basic() {
        let mut recorder = FlightRecorder::new(100, true);

        recorder.record(
            FlightEvent::ConnectionEstablished,
            Some(RemoteAddress::tcp("test", "10.0.0.1", 2552)),
            "Test connection".to_string(),
        );

        assert_eq!(recorder.len(), 1);
        assert!(!recorder.is_empty());

        let recent = recorder.recent(10);
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].event, FlightEvent::ConnectionEstablished);
    }

    #[test]
    fn test_flight_recorder_disabled() {
        let mut recorder = FlightRecorder::new(100, false);

        recorder.record(FlightEvent::MessageSent, None, "test".to_string());
        assert!(recorder.is_empty());
    }

    #[test]
    fn test_flight_recorder_eviction() {
        let mut recorder = FlightRecorder::new(3, true);

        recorder.record(FlightEvent::MessageSent, None, "1".to_string());
        recorder.record(FlightEvent::MessageReceived, None, "2".to_string());
        recorder.record(FlightEvent::HeartbeatSent, None, "3".to_string());
        recorder.record(FlightEvent::HeartbeatReceived, None, "4".to_string());

        assert_eq!(recorder.len(), 3);
        let recent = recorder.recent(10);
        assert_eq!(recent[0].detail, "2"); // Oldest was evicted
    }

    #[test]
    fn test_flight_recorder_by_event() {
        let mut recorder = FlightRecorder::new(100, true);

        recorder.record(FlightEvent::MessageSent, None, "sent-1".to_string());
        recorder.record(FlightEvent::MessageReceived, None, "recv-1".to_string());
        recorder.record(FlightEvent::MessageSent, None, "sent-2".to_string());

        let sent = recorder.by_event(&FlightEvent::MessageSent);
        assert_eq!(sent.len(), 2);
    }

    // ===== Handshake Tests =====

    #[test]
    fn test_handshake_request_creation() {
        let req = HandshakeRequest {
            origin_address: RemoteAddress::tcp("system", "10.0.0.1", 25520),
            protocol_version: ARTERY_PROTOCOL_VERSION,
            cookie: Some("secret".to_string()),
            capabilities: vec!["compression".to_string()],
        };
        assert_eq!(req.protocol_version, 1);
        assert_eq!(req.cookie, Some("secret".to_string()));
    }

    // ===== ArteryTransport Tests =====

    #[tokio::test]
    async fn test_artery_transport_lifecycle() {
        let config = ArteryConfig::default();
        let mut transport = ArteryTransport::new(config);

        assert!(!transport.is_running());

        transport.start().await.unwrap();
        assert!(transport.is_running());
        assert!(transport.local_address().is_some());

        transport.stop().await.unwrap();
        assert!(!transport.is_running());
    }

    #[tokio::test]
    async fn test_artery_transport_association() {
        let config = ArteryConfig::default();
        let mut transport = ArteryTransport::new(config);
        transport.start().await.unwrap();

        let remote = RemoteAddress::tcp("remote", "10.0.0.2", 25520);
        transport.associate(remote.clone()).await.unwrap();

        assert_eq!(transport.association_count().await, 1);

        // Send a message
        let envelope = ArteryEnvelope::ordinary(
            "/user/actor-1".to_string(),
            1,
            "test.Message".to_string(),
            vec![1, 2, 3],
        );
        transport.send(&remote, envelope).await.unwrap();

        let stats = transport.stats().await;
        assert_eq!(stats.total_messages_sent, 1);
        assert_eq!(stats.active_associations, 1);

        // Disassociate
        transport.disassociate(&remote).await.unwrap();
        assert_eq!(transport.association_count().await, 0);

        transport.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_artery_transport_handshake() {
        let config = ArteryConfig::default();
        let mut transport = ArteryTransport::new(config);
        transport.start().await.unwrap();

        let remote = RemoteAddress::tcp("remote", "10.0.0.2", 25520);
        let response = transport.handshake(&remote, Some("cookie".to_string())).await.unwrap();

        assert_eq!(response.status, HandshakeStatus::Accepted);
        assert_eq!(response.protocol_version, ARTERY_PROTOCOL_VERSION);

        let stats = transport.stats().await;
        assert_eq!(stats.handshake_successes, 1);

        transport.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_artery_transport_send_not_connected() {
        let config = ArteryConfig::default();
        let mut transport = ArteryTransport::new(config);
        transport.start().await.unwrap();

        let remote = RemoteAddress::tcp("remote", "10.0.0.2", 25520);
        let envelope = ArteryEnvelope::ordinary(
            "/user/actor".to_string(), 1, "test".to_string(), vec![],
        );

        let result = transport.send(&remote, envelope).await;
        assert!(result.is_err());

        transport.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_artery_transport_flight_recorder() {
        let mut config = ArteryConfig::default();
        config.flight_recorder_enabled = true;
        let mut transport = ArteryTransport::new(config);

        transport.start().await.unwrap();

        let remote = RemoteAddress::tcp("remote", "10.0.0.2", 25520);
        transport.associate(remote).await.unwrap();

        let entries = transport.flight_recorder_entries(100).await;
        assert!(entries.len() >= 2); // start + associate

        transport.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_artery_transport_multiple_associations() {
        let config = ArteryConfig::default();
        let mut transport = ArteryTransport::new(config);
        transport.start().await.unwrap();

        for i in 1..=5 {
            let remote = RemoteAddress::tcp("remote", &format!("10.0.0.{}", i), 25520);
            transport.associate(remote).await.unwrap();
        }

        assert_eq!(transport.association_count().await, 5);

        let stats = transport.stats().await;
        assert_eq!(stats.total_associations, 5);
        assert_eq!(stats.active_associations, 5);

        transport.stop().await.unwrap();
    }

    // ===== IdleDetector Tests =====

    #[tokio::test]
    async fn test_idle_detector() {
        let detector = IdleDetector::new(Duration::from_millis(50));

        let (tx, rx) = mpsc::channel(1);
        let (tx2, rx2) = mpsc::channel(1);
        let (tx3, rx3) = mpsc::channel(1);

        let mut assocs = HashMap::new();
        let addr = RemoteAddress::tcp("test", "10.0.0.1", 25520);
        assocs.insert(addr.clone(), AssociationHandle {
            remote_address: addr.clone(),
            state: AssociationState::Active,
            control_tx: tx,
            ordinary_tx: tx2,
            large_tx: tx3,
            _control_rx: Arc::new(Mutex::new(rx)),
            _ordinary_rx: Arc::new(Mutex::new(rx2)),
            _large_rx: Arc::new(Mutex::new(rx3)),
            stats: AssociationStats::default(),
            seq_counter: Arc::new(AtomicU64::new(1)),
            last_activity: Arc::new(RwLock::new(Instant::now() - Duration::from_secs(1))),
        });

        let idle = detector.detect_idle(&assocs).await;
        assert_eq!(idle.len(), 1);
        assert_eq!(idle[0], addr);
    }
}

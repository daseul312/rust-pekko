//! Connection management and pooling for remote actor communication
//! 
//! This module provides:
//! - ConnectionPool: Manages multiple connections with auto-reconnect
//! - ConnectionHandle: Active connection with send/receive capabilities
//! - Handshake protocol for connection establishment

use crate::{
    addressing::RemoteAddress,
    errors::{ConnectionError, ConnectionResult, TransportError, TransportResult},
    proto::remoting::RemoteEnvelope,
    transport::{Frame, PekkoCodec, TransportConfig, TransportStats},
};
use bytes::BytesMut;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use tokio::sync::RwLock;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, oneshot, Mutex},
    time::timeout,
};
use tokio_util::codec::Framed;
use tracing::{debug, error, warn};
use uuid::Uuid;

/// Connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Initial state, not yet connected
    Disconnected,
    /// Attempting to connect
    Connecting,
    /// Handshake in progress
    Handshaking,
    /// Fully connected and operational
    Connected,
    /// Connection is closing
    Closing,
    /// Connection has failed
    Failed,
}

/// Connection metadata
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub remote_address: RemoteAddress,
    pub local_addr: Option<SocketAddr>,
    pub peer_addr: Option<SocketAddr>,
    pub state: ConnectionState,
    pub connected_at: Option<Instant>,
    pub last_activity: Instant,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub reconnect_count: u32,
}

impl ConnectionInfo {
    fn new(remote_address: RemoteAddress) -> Self {
        Self {
            remote_address,
            local_addr: None,
            peer_addr: None,
            state: ConnectionState::Disconnected,
            connected_at: None,
            last_activity: Instant::now(),
            messages_sent: 0,
            messages_received: 0,
            bytes_sent: 0,
            bytes_received: 0,
            reconnect_count: 0,
        }
    }
}

/// Outgoing message with optional response channel
#[derive(Debug)]
pub struct OutgoingMessage {
    pub envelope: RemoteEnvelope,
    pub response_tx: Option<oneshot::Sender<TransportResult<RemoteEnvelope>>>,
}

/// Incoming message from remote
#[derive(Debug)]
pub struct IncomingMessage {
    pub envelope: RemoteEnvelope,
    pub from: RemoteAddress,
}

/// Connection handle for active connections
pub struct ConnectionHandle {
    /// Remote address
    remote_address: RemoteAddress,
    /// Connection info
    info: Arc<RwLock<ConnectionInfo>>,
    /// Sender for outgoing messages
    outgoing_tx: mpsc::UnboundedSender<OutgoingMessage>,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Sequence number for messages
    seq_no: Arc<AtomicU64>,
}

impl Debug for ConnectionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionHandle")
            .field("remote_address", &self.remote_address)
            .field("running", &self.running.load(Ordering::SeqCst))
            .finish()
    }
}

impl Clone for ConnectionHandle {
    fn clone(&self) -> Self {
        Self {
            remote_address: self.remote_address.clone(),
            info: self.info.clone(),
            outgoing_tx: self.outgoing_tx.clone(),
            running: self.running.clone(),
            seq_no: self.seq_no.clone(),
        }
    }
}

impl ConnectionHandle {
    /// Create a new connection handle
    pub async fn connect(
        remote_address: RemoteAddress,
        config: &TransportConfig,
        incoming_tx: mpsc::UnboundedSender<IncomingMessage>,
        local_system: String,
    ) -> TransportResult<Self> {
        let socket_addr = remote_address.socket_addr()
            .map_err(|e| TransportError::ConnectionFailed {
                address: remote_address.to_string(),
                source: std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()),
            })?;

        // Connect with timeout
        let stream = timeout(
            Duration::from_millis(config.connection_timeout_ms),
            TcpStream::connect(socket_addr),
        )
        .await
        .map_err(|_| TransportError::Timeout)?
        .map_err(|e| TransportError::ConnectionFailed {
            address: remote_address.to_string(),
            source: e,
        })?;

        // Set TCP options
        stream.set_nodelay(true).ok();

        let local_addr = stream.local_addr().ok();
        let peer_addr = stream.peer_addr().ok();

        // Create framed connection
        let codec = PekkoCodec::new(config.maximum_frame_size);
        let framed = Framed::new(stream, codec);

        // Initialize connection info
        let info = Arc::new(RwLock::new(ConnectionInfo {
            remote_address: remote_address.clone(),
            local_addr,
            peer_addr,
            state: ConnectionState::Connecting,
            connected_at: None,
            last_activity: Instant::now(),
            messages_sent: 0,
            messages_received: 0,
            bytes_sent: 0,
            bytes_received: 0,
            reconnect_count: 0,
        }));

        let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel();
        let running = Arc::new(AtomicBool::new(true));
        let seq_no = Arc::new(AtomicU64::new(0));

        // Spawn connection handler task
        let handle = Self {
            remote_address: remote_address.clone(),
            info: info.clone(),
            outgoing_tx,
            running: running.clone(),
            seq_no: seq_no.clone(),
        };

        // Start the I/O loop
        tokio::spawn(Self::io_loop(
            framed,
            outgoing_rx,
            incoming_tx,
            remote_address.clone(),
            info.clone(),
            running.clone(),
            config.heartbeat_interval_ms,
            local_system,
        ));

        Ok(handle)
    }

    /// I/O loop for handling send/receive
    async fn io_loop(
        mut framed: Framed<TcpStream, PekkoCodec>,
        mut outgoing_rx: mpsc::UnboundedReceiver<OutgoingMessage>,
        incoming_tx: mpsc::UnboundedSender<IncomingMessage>,
        remote_address: RemoteAddress,
        info: Arc<RwLock<ConnectionInfo>>,
        running: Arc<AtomicBool>,
        heartbeat_interval_ms: u64,
        local_system: String,
    ) {
        // Perform handshake
        {
            info.write().await.state = ConnectionState::Handshaking;
        }

        // Send handshake
        let handshake = Handshake {
            system_name: local_system.clone(),
            actor_system_version: "0.1.0".to_string(),
            protocol_version: 1,
            capabilities: vec!["compression".to_string(), "streaming".to_string()],
        };

        let handshake_frame = Frame::Control(bincode::serialize(&ControlFrame::Handshake(handshake.clone())).unwrap());
        if let Err(e) = framed.send(handshake_frame).await {
            error!("Handshake send failed: {}", e);
            info.write().await.state = ConnectionState::Failed;
            return;
        }

        // Wait for handshake ack (with timeout)
        match timeout(Duration::from_secs(5), framed.next()).await {
            Ok(Some(Ok(Frame::Control(data)))) => {
                match bincode::deserialize::<ControlFrame>(&data) {
                    Ok(ControlFrame::HandshakeAck(ack)) => {
                        let mut info_guard = info.write().await;
                        info_guard.state = ConnectionState::Connected;
                        info_guard.connected_at = Some(Instant::now());
                    }
                    _ => {
                        error!("Invalid handshake response");
                        info.write().await.state = ConnectionState::Failed;
                        return;
                    }
                }
            }
            _ => {
                error!("Handshake timeout or error");
                info.write().await.state = ConnectionState::Failed;
                return;
            }
        }

        let mut heartbeat_interval = tokio::time::interval(Duration::from_millis(heartbeat_interval_ms));
        let mut heartbeat_seq: u64 = 0;

        // Main I/O loop
        loop {
            if !running.load(Ordering::SeqCst) {
                break;
            }

            tokio::select! {
                // Handle outgoing messages
                Some(msg) = outgoing_rx.recv() => {
                    let data = msg.envelope.encode_to_vec();
                    let frame = Frame::Message(data.clone());

                    match framed.send(frame).await {
                        Ok(_) => {
                            {
                                let mut info_guard = info.write().await;
                                info_guard.messages_sent += 1;
                                info_guard.bytes_sent += data.len() as u64;
                                info_guard.last_activity = Instant::now();
                            }
                            debug!("Sent message to {}", remote_address);
                        }
                        Err(e) => {
                            error!("Send failed: {}", e);
                            if let Some(tx) = msg.response_tx {
                                let _ = tx.send(Err(e));
                            }
                        }
                    }
                }

                // Handle incoming frames
                Some(result) = framed.next() => {
                    match result {
                        Ok(frame) => {
                            // Update last activity
                            info.write().await.last_activity = Instant::now();

                            match frame {
                                Frame::Message(data) => {
                                    {
                                        let mut info_guard = info.write().await;
                                        info_guard.messages_received += 1;
                                        info_guard.bytes_received += data.len() as u64;
                                    }

                                    match RemoteEnvelope::decode(data.as_slice()) {
                                        Ok(envelope) => {
                                            let _ = incoming_tx.send(IncomingMessage {
                                                envelope,
                                                from: remote_address.clone(),
                                            });
                                        }
                                        Err(e) => {
                                            warn!("Failed to decode envelope: {}", e);
                                        }
                                    }
                                }
                                Frame::Control(data) => {
                                    if let Ok(ctrl) = bincode::deserialize::<ControlFrame>(&data) {
                                        match ctrl {
                                            ControlFrame::Handshake(_h) => {
                                                // Respond to incoming handshake
                                                let ack = ControlFrame::HandshakeAck(HandshakeAck {
                                                    system_name: local_system.clone(),
                                                    accepted: true,
                                                    reject_reason: String::new(),
                                                });
                                                let _ = framed.send(Frame::Control(bincode::serialize(&ack).unwrap())).await;
                                            }
                                            ControlFrame::Disconnect { reason } => {
                                                running.store(false, Ordering::SeqCst);
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                Frame::Heartbeat { seq_no } => {
                                    // Respond with ack
                                    let _ = framed.send(Frame::HeartbeatAck { seq_no }).await;
                                }
                                Frame::HeartbeatAck { seq_no: _ } => {
                                    // Heartbeat acknowledged
                                    debug!("Heartbeat ack from {}", remote_address);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Receive error: {}", e);
                            break;
                        }
                    }
                }

                // Send heartbeats
                _ = heartbeat_interval.tick() => {
                    heartbeat_seq += 1;
                    let _ = framed.send(Frame::Heartbeat { seq_no: heartbeat_seq }).await;
                }
            }
        }

        // Cleanup
        info.write().await.state = ConnectionState::Disconnected;
    }

    /// Send a message through this connection
    pub fn send(&self, envelope: RemoteEnvelope) -> TransportResult<()> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(TransportError::NotConnected);
        }

        self.outgoing_tx
            .send(OutgoingMessage {
                envelope,
                response_tx: None,
            })
            .map_err(|_| TransportError::SendFailed("Channel closed".to_string()))
    }

    /// Send a message and wait for response (ask pattern)
    pub async fn ask(&self, mut envelope: RemoteEnvelope, timeout_ms: u64) -> TransportResult<RemoteEnvelope> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(TransportError::NotConnected);
        }

        // Assign sequence number
        envelope.seq_no = self.seq_no.fetch_add(1, Ordering::SeqCst);

        let (response_tx, response_rx) = oneshot::channel();

        self.outgoing_tx
            .send(OutgoingMessage {
                envelope,
                response_tx: Some(response_tx),
            })
            .map_err(|_| TransportError::SendFailed("Channel closed".to_string()))?;

        match timeout(Duration::from_millis(timeout_ms), response_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(TransportError::ReceiveFailed("Response channel closed".to_string())),
            Err(_) => Err(TransportError::Timeout),
        }
    }

    /// Get connection info
    pub async fn info(&self) -> ConnectionInfo {
        self.info.read().await.clone()
    }

    /// Check if connected (sync version using running flag only)
    pub fn is_connected(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Check if connected (async version with full state check)
    pub async fn is_connected_async(&self) -> bool {
        self.running.load(Ordering::SeqCst) && 
        self.info.read().await.state == ConnectionState::Connected
    }

    /// Close the connection
    pub fn close(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

/// Handshake message for connection establishment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Handshake {
    pub system_name: String,
    pub actor_system_version: String,
    pub protocol_version: u32,
    pub capabilities: Vec<String>,
}

/// Handshake acknowledgment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeAck {
    pub system_name: String,
    pub accepted: bool,
    pub reject_reason: String,
}

/// Control frame types for connection management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlFrame {
    Handshake(Handshake),
    HandshakeAck(HandshakeAck),
    Disconnect { reason: String },
    Ping { seq_no: u64 },
    Pong { seq_no: u64 },
}

/// Connection pool for managing multiple connections
pub struct ConnectionPool {
    /// Active connections by remote address
    connections: DashMap<RemoteAddress, Arc<ConnectionHandle>>,
    /// Transport configuration
    config: TransportConfig,
    /// Incoming message receiver
    incoming_tx: mpsc::UnboundedSender<IncomingMessage>,
    /// Local system name
    local_system: String,
    /// Pool statistics
    stats: Arc<RwLock<PoolStats>>,
}

impl Debug for ConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionPool")
            .field("connections", &self.connections.len())
            .field("local_system", &self.local_system)
            .finish()
    }
}

/// Pool statistics
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    pub total_connections: u64,
    pub active_connections: u32,
    pub failed_connections: u64,
    pub reconnections: u64,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new(
        config: TransportConfig,
        local_system: String,
    ) -> (Self, mpsc::UnboundedReceiver<IncomingMessage>) {
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();

        let pool = Self {
            connections: DashMap::new(),
            config,
            incoming_tx,
            local_system,
            stats: Arc::new(RwLock::new(PoolStats::default())),
        };

        (pool, incoming_rx)
    }

    /// Get or create a connection to the remote address
    pub async fn get_or_connect(&self, remote_address: &RemoteAddress) -> TransportResult<Arc<ConnectionHandle>> {
        // Check for existing connection
        if let Some(conn) = self.connections.get(remote_address) {
            if conn.is_connected() {
                return Ok(conn.clone());
            }
            // Remove stale connection
            drop(conn);
            self.connections.remove(remote_address);
        }

        // Create new connection
        let handle = ConnectionHandle::connect(
            remote_address.clone(),
            &self.config,
            self.incoming_tx.clone(),
            self.local_system.clone(),
        )
        .await?;

        let handle = Arc::new(handle);
        self.connections.insert(remote_address.clone(), handle.clone());

        {
            let mut stats = self.stats.write().await;
            stats.total_connections += 1;
            stats.active_connections = self.connections.len() as u32;
        }

        Ok(handle)
    }

    /// Send a message to a remote address
    pub async fn send(&self, remote_address: &RemoteAddress, envelope: RemoteEnvelope) -> TransportResult<()> {
        let conn = self.get_or_connect(remote_address).await?;
        conn.send(envelope)
    }

    /// Send and wait for response
    pub async fn ask(
        &self,
        remote_address: &RemoteAddress,
        envelope: RemoteEnvelope,
        timeout_ms: u64,
    ) -> TransportResult<RemoteEnvelope> {
        let conn = self.get_or_connect(remote_address).await?;
        conn.ask(envelope, timeout_ms).await
    }

    /// Close a specific connection
    pub fn close(&self, remote_address: &RemoteAddress) {
        if let Some((_, conn)) = self.connections.remove(remote_address) {
            conn.close();
        }
    }

    /// Close all connections
    pub fn close_all(&self) {
        for entry in self.connections.iter() {
            entry.value().close();
        }
        self.connections.clear();
    }

    /// Get pool statistics
    pub async fn stats(&self) -> PoolStats {
        let mut stats = self.stats.read().await.clone();
        stats.active_connections = self.connections.len() as u32;
        stats
    }

    /// List all connected addresses
    pub fn connected_addresses(&self) -> Vec<RemoteAddress> {
        self.connections
            .iter()
            .filter(|e| e.value().is_connected())
            .map(|e| e.key().clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_info() {
        let addr = RemoteAddress::tcp("test", "127.0.0.1", 2552);
        let info = ConnectionInfo::new(addr.clone());
        
        assert_eq!(info.state, ConnectionState::Disconnected);
        assert_eq!(info.remote_address, addr);
        assert_eq!(info.messages_sent, 0);
    }

    #[test]
    fn test_control_frame_serialization() {
        let handshake = super::Handshake {
            system_name: "test".to_string(),
            actor_system_version: "0.1.0".to_string(),
            protocol_version: 1,
            capabilities: vec!["test".to_string()],
        };
        
        let frame = ControlFrame::Handshake(handshake);
        let serialized = bincode::serialize(&frame).unwrap();
        let deserialized: ControlFrame = bincode::deserialize(&serialized).unwrap();
        
        match deserialized {
            ControlFrame::Handshake(h) => {
                assert_eq!(h.system_name, "test");
            }
            _ => panic!("Expected Handshake"),
        }
    }

    #[tokio::test]
    async fn test_connection_pool_creation() {
        let config = TransportConfig::default();
        let (pool, _rx) = ConnectionPool::new(config, "test-system".to_string());
        
        assert_eq!(pool.stats().await.active_connections, 0);
        assert!(pool.connected_addresses().is_empty());
    }
}

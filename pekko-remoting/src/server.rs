//! TCP Server for accepting incoming remote connections
//!
//! This module provides:
//! - RemoteServer: Listens for and accepts incoming TCP connections
//! - Inbound connection handling with handshake
//! - Integration with ConnectionPool for bidirectional communication

use crate::{
    addressing::RemoteAddress,
    connection::{
        ConnectionState, ConnectionInfo, ControlFrame, Handshake, HandshakeAck,
        IncomingMessage,
    },
    errors::{TransportError, TransportResult},
    proto::remoting::RemoteEnvelope,
    transport::{Frame, PekkoCodec, TransportConfig},
};
use futures::{SinkExt, StreamExt};
use prost::Message;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, RwLock},
    time::timeout,
};
use tokio_util::codec::Framed;
use tracing::{debug, error, warn};

/// Inbound connection from a remote system
pub struct InboundConnection {
    /// Remote address (resolved after handshake)
    pub remote_address: Option<RemoteAddress>,
    /// Peer socket address
    pub peer_addr: SocketAddr,
    /// Connection state
    pub state: ConnectionState,
    /// Framed TCP stream
    framed: Framed<TcpStream, PekkoCodec>,
    /// Incoming message sender
    incoming_tx: mpsc::UnboundedSender<IncomingMessage>,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Local system name
    local_system: String,
    /// Messages received
    messages_received: u64,
    /// Bytes received
    bytes_received: u64,
    /// Connected timestamp
    connected_at: Option<Instant>,
}

impl InboundConnection {
    /// Create from accepted TCP stream
    pub fn new(
        stream: TcpStream,
        peer_addr: SocketAddr,
        config: &TransportConfig,
        incoming_tx: mpsc::UnboundedSender<IncomingMessage>,
        local_system: String,
    ) -> Self {
        let codec = PekkoCodec::new(config.maximum_frame_size);
        let framed = Framed::new(stream, codec);

        Self {
            remote_address: None,
            peer_addr,
            state: ConnectionState::Connecting,
            framed,
            incoming_tx,
            running: Arc::new(AtomicBool::new(true)),
            local_system,
            messages_received: 0,
            bytes_received: 0,
            connected_at: None,
        }
    }

    /// Run the inbound connection handler
    pub async fn run(mut self) {
        // Wait for handshake from client
        self.state = ConnectionState::Handshaking;

        match timeout(Duration::from_secs(10), self.framed.next()).await {
            Ok(Some(Ok(Frame::Control(data)))) => {
                match bincode::deserialize::<ControlFrame>(&data) {
                    Ok(ControlFrame::Handshake(handshake)) => {
                        // Store remote system info
                        self.remote_address = Some(RemoteAddress::tcp(
                            &handshake.system_name,
                            &self.peer_addr.ip().to_string(),
                            self.peer_addr.port(),
                        ));

                        // Send handshake ack
                        let ack = ControlFrame::HandshakeAck(HandshakeAck {
                            system_name: self.local_system.clone(),
                            accepted: true,
                            reject_reason: String::new(),
                        });

                        if let Err(e) = self.framed.send(Frame::Control(
                            bincode::serialize(&ack).unwrap()
                        )).await {
                            error!("Failed to send handshake ack: {}", e);
                            return;
                        }

                        self.state = ConnectionState::Connected;
                        self.connected_at = Some(Instant::now());
                    }
                    _ => {
                        error!("Invalid handshake from {}", self.peer_addr);
                        return;
                    }
                }
            }
            Ok(Some(Ok(_))) => {
                // Received a non-control frame during handshake (Message, Heartbeat, etc.)
                error!("Expected handshake from {}, got data frame", self.peer_addr);
                return;
            }
            Ok(Some(Err(e))) => {
                error!("Handshake error from {}: {}", self.peer_addr, e);
                return;
            }
            Ok(None) => {
                error!("Connection closed during handshake from {}", self.peer_addr);
                return;
            }
            Err(_) => {
                error!("Handshake timeout from {}", self.peer_addr);
                return;
            }
        }

        // Main message loop
        while self.running.load(Ordering::SeqCst) {
            tokio::select! {
                result = self.framed.next() => {
                    match result {
                        Some(Ok(frame)) => {
                            if !self.handle_frame(frame).await {
                                break;
                            }
                        }
                        Some(Err(e)) => {
                            error!("Receive error from {:?}: {}", self.remote_address, e);
                            break;
                        }
                        None => {
                            break;
                        }
                    }
                }
            }
        }

        self.state = ConnectionState::Disconnected;
    }

    /// Handle an incoming frame
    async fn handle_frame(&mut self, frame: Frame) -> bool {
        match frame {
            Frame::Message(data) => {
                self.messages_received += 1;
                self.bytes_received += data.len() as u64;

                match RemoteEnvelope::decode(data.as_slice()) {
                    Ok(envelope) => {
                        if let Some(remote_addr) = &self.remote_address {
                            let _ = self.incoming_tx.send(IncomingMessage {
                                envelope,
                                from: remote_addr.clone(),
                            });
                        }
                    }
                    Err(e) => {
                        warn!("Failed to decode envelope: {}", e);
                    }
                }
                true
            }
            Frame::Control(data) => {
                if let Ok(ctrl) = bincode::deserialize::<ControlFrame>(&data) {
                    match ctrl {
                        ControlFrame::Disconnect { reason } => {
                            return false;
                        }
                        ControlFrame::Ping { seq_no } => {
                            let pong = ControlFrame::Pong { seq_no };
                            let _ = self.framed.send(Frame::Control(
                                bincode::serialize(&pong).unwrap()
                            )).await;
                        }
                        _ => {}
                    }
                }
                true
            }
            Frame::Heartbeat { seq_no } => {
                let _ = self.framed.send(Frame::HeartbeatAck { seq_no }).await;
                true
            }
            Frame::HeartbeatAck { .. } => true,
        }
    }

    /// Close the connection
    pub fn close(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

/// TCP Server for accepting remote connections
pub struct RemoteServer {
    /// Bind address
    bind_addr: String,
    /// Transport config
    config: TransportConfig,
    /// Local system name
    local_system: String,
    /// Incoming message sender (shared with all connections)
    incoming_tx: mpsc::UnboundedSender<IncomingMessage>,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Listener task handle
    listener_handle: RwLock<Option<tokio::task::JoinHandle<()>>>,
    /// Active connections count
    active_connections: Arc<AtomicU64>,
    /// Total connections accepted
    total_connections: Arc<AtomicU64>,
}

impl RemoteServer {
    /// Create a new remote server
    pub fn new(
        bind_host: &str,
        bind_port: u16,
        config: TransportConfig,
        local_system: String,
        incoming_tx: mpsc::UnboundedSender<IncomingMessage>,
    ) -> Self {
        Self {
            bind_addr: format!("{}:{}", bind_host, bind_port),
            config,
            local_system,
            incoming_tx,
            running: Arc::new(AtomicBool::new(false)),
            listener_handle: RwLock::new(None),
            active_connections: Arc::new(AtomicU64::new(0)),
            total_connections: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Start listening for connections
    pub async fn start(&self) -> TransportResult<()> {
        if self.running.load(Ordering::SeqCst) {
            return Ok(()); // Already running
        }

        let listener = TcpListener::bind(&self.bind_addr)
            .await
            .map_err(|e| TransportError::BindFailed {
                address: self.bind_addr.clone(),
                source: e,
            })?;

        let local_addr = listener.local_addr()
            .map_err(|e| TransportError::BindFailed {
                address: self.bind_addr.clone(),
                source: e,
            })?;
        self.running.store(true, Ordering::SeqCst);

        // Spawn the listener loop
        let running = self.running.clone();
        let config = self.config.clone();
        let local_system = self.local_system.clone();
        let incoming_tx = self.incoming_tx.clone();
        let active_connections = self.active_connections.clone();
        let total_connections = self.total_connections.clone();

        let handle = tokio::spawn(async move {
            Self::accept_loop(
                listener,
                running,
                config,
                local_system,
                incoming_tx,
                active_connections,
                total_connections,
            ).await;
        });

        *self.listener_handle.write().await = Some(handle);

        Ok(())
    }

    /// Accept loop
    async fn accept_loop(
        listener: TcpListener,
        running: Arc<AtomicBool>,
        config: TransportConfig,
        local_system: String,
        incoming_tx: mpsc::UnboundedSender<IncomingMessage>,
        active_connections: Arc<AtomicU64>,
        total_connections: Arc<AtomicU64>,
    ) {
        while running.load(Ordering::SeqCst) {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    // Set TCP options
                    stream.set_nodelay(true).ok();

                    total_connections.fetch_add(1, Ordering::SeqCst);
                    active_connections.fetch_add(1, Ordering::SeqCst);

                    // Create and spawn inbound connection handler
                    let conn = InboundConnection::new(
                        stream,
                        peer_addr,
                        &config,
                        incoming_tx.clone(),
                        local_system.clone(),
                    );

                    let active = active_connections.clone();
                    tokio::spawn(async move {
                        conn.run().await;
                        active.fetch_sub(1, Ordering::SeqCst);
                    });
                }
                Err(e) => {
                    if running.load(Ordering::SeqCst) {
                        error!("Accept error: {}", e);
                    }
                }
            }
        }
    }

    /// Stop the server
    pub async fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);

        if let Some(handle) = self.listener_handle.write().await.take() {
            handle.abort();
        }
    }

    /// Check if running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Get active connection count
    pub fn active_connections(&self) -> u64 {
        self.active_connections.load(Ordering::SeqCst)
    }

    /// Get total connections accepted
    pub fn total_connections(&self) -> u64 {
        self.total_connections.load(Ordering::SeqCst)
    }
}

/// Server statistics
#[derive(Debug, Clone, Default)]
pub struct ServerStats {
    pub active_connections: u64,
    pub total_connections: u64,
    pub is_running: bool,
}

impl RemoteServer {
    /// Get server statistics
    pub fn stats(&self) -> ServerStats {
        ServerStats {
            active_connections: self.active_connections(),
            total_connections: self.total_connections(),
            is_running: self.is_running(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_creation() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let config = TransportConfig::default();
        let server = RemoteServer::new(
            "127.0.0.1",
            0, // Random port
            config,
            "test-system".to_string(),
            tx,
        );

        assert!(!server.is_running());
        assert_eq!(server.active_connections(), 0);
    }

    #[tokio::test]
    async fn test_server_stats() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let config = TransportConfig::default();
        let server = RemoteServer::new(
            "127.0.0.1",
            0,
            config,
            "test-system".to_string(),
            tx,
        );

        let stats = server.stats();
        assert!(!stats.is_running);
        assert_eq!(stats.active_connections, 0);
        assert_eq!(stats.total_connections, 0);
    }
}

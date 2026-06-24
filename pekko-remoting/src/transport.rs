//! Transport layer for remote communication

use crate::{
    addressing::RemoteAddress,
    errors::{TransportError, TransportResult},
    proto::remoting::RemoteEnvelope,
};
use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use futures::StreamExt;
use parking_lot::RwLock;
use prost::Message; // Message 트레이트 import 추가
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::timeout,
};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{debug, error, warn};

/// Transport configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransportConfig {
    pub bind_address: String,
    pub bind_port: u16,
    pub maximum_frame_size: usize,
    pub connection_timeout_ms: u64,
    pub send_buffer_size: usize,
    pub receive_buffer_size: usize,
    pub enable_compression: bool,
    pub compression_threshold: usize,
    pub enable_tls: bool,
    pub heartbeat_interval_ms: u64,
    pub heartbeat_timeout_ms: u64,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1".to_string(),
            bind_port: 2552,
            maximum_frame_size: 1024 * 1024, // 1MB
            connection_timeout_ms: 5000,
            send_buffer_size: 64 * 1024, // 64KB
            receive_buffer_size: 64 * 1024, // 64KB
            enable_compression: false,
            compression_threshold: 1024,
            enable_tls: false,
            heartbeat_interval_ms: 1000,
            heartbeat_timeout_ms: 5000,
        }
    }
}

/// Transport statistics
#[derive(Debug, Clone, Default)]
pub struct TransportStats {
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub connections_active: u32,
    pub connections_total: u32,
    pub errors: u32,
}

/// Transport layer trait
#[async_trait]
pub trait Transport: Send + Sync + Debug {
    /// Start the transport layer
    async fn start(&mut self, config: &TransportConfig) -> TransportResult<()>;

    /// Stop the transport layer
    async fn stop(&mut self) -> TransportResult<()>;

    /// Send a message to a remote address
    async fn send(&mut self, target: RemoteAddress, message: RemoteEnvelope) -> TransportResult<()>;

    /// Get transport statistics
    fn stats(&self) -> TransportStats;

    /// Get local address
    fn local_address(&self) -> Option<RemoteAddress>;
}

/// Outgoing message wrapper
#[derive(Debug, Clone)]
pub struct OutgoingMessage {
    pub target: RemoteAddress,
    pub envelope: RemoteEnvelope,
}

/// TCP transport implementation
#[derive(Debug)]
pub struct TcpTransport {
    config: TransportConfig,
    local_address: Option<RemoteAddress>,
    connections: Arc<RwLock<HashMap<RemoteAddress, Connection>>>,
    listener_handle: Option<tokio::task::JoinHandle<()>>,
    message_sender: Option<mpsc::UnboundedSender<OutgoingMessage>>,
    stats: Arc<RwLock<TransportStats>>,
}

impl TcpTransport {
    pub fn new(config: TransportConfig) -> Self {
        Self {
            config,
            local_address: None,
            connections: Arc::new(RwLock::new(HashMap::new())),
            listener_handle: None,
            message_sender: None,
            stats: Arc::new(RwLock::new(TransportStats::default())),
        }
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn start(&mut self, config: &TransportConfig) -> TransportResult<()> {
        self.config = config.clone();
        
        let bind_addr = format!("{}:{}", config.bind_address, config.bind_port);
        let listener = TcpListener::bind(&bind_addr)
            .await
            .map_err(|e| TransportError::BindFailed {
                address: bind_addr.clone(),
                source: e,
            })?;

        let local_addr = listener.local_addr()
            .map_err(|e| TransportError::BindFailed {
                address: bind_addr.clone(),
                source: e,
            })?;

        self.local_address = Some(RemoteAddress::tcp(
            "system",
            &config.bind_address,
            local_addr.port(),
        ));

        let (message_tx, message_rx) = mpsc::unbounded_channel();
        self.message_sender = Some(message_tx);

        let connections = self.connections.clone();
        let stats = self.stats.clone();
        
        // Start message sender task
        let _sender_task = tokio::spawn(Self::message_sender_loop(message_rx, connections, stats));

        // Start listener task
        let listener_task = tokio::spawn(Self::listener_loop(
            listener, 
            self.connections.clone(), 
            self.stats.clone(), 
            config.clone()
        ));

        self.listener_handle = Some(listener_task);
        Ok(())
    }

    async fn stop(&mut self) -> TransportResult<()> {
        if let Some(handle) = self.listener_handle.take() {
            handle.abort();
        }

        self.message_sender = None;
        Ok(())
    }

    async fn send(&mut self, target: RemoteAddress, envelope: RemoteEnvelope) -> TransportResult<()> {
        if let Some(sender) = &self.message_sender {
            let message = OutgoingMessage {
                target: target.clone(),
                envelope,
            };
            sender.send(message)
                .map_err(|_| TransportError::SendFailed("Channel closed".to_string()))?;
            Ok(())
        } else {
            Err(TransportError::SendFailed("Transport not started".to_string()))
        }
    }

    fn stats(&self) -> TransportStats {
        self.stats.read().clone()  // parking_lot은 동기적
    }

    fn local_address(&self) -> Option<RemoteAddress> {
        self.local_address.clone()
    }
}

impl TcpTransport {
    async fn message_sender_loop(
        mut message_rx: tokio::sync::mpsc::UnboundedReceiver<OutgoingMessage>,
        connections: Arc<RwLock<HashMap<RemoteAddress, Connection>>>,
        stats: Arc<RwLock<TransportStats>>,
    ) {
        while let Some(message) = message_rx.recv().await {
            if let Err(e) = Self::send_message(&message, connections.clone(), stats.clone()).await {
                error!("Failed to send message to {}: {}", message.target, e);
            }
        }
    }

    async fn listener_loop(
        listener: TcpListener,
        connections: Arc<RwLock<HashMap<RemoteAddress, Connection>>>,
        stats: Arc<RwLock<TransportStats>>,
        config: TransportConfig,
    ) {
        while let Ok((stream, addr)) = listener.accept().await {
            debug!("New connection from {}", addr);
            
            let codec = PekkoCodec::new(config.maximum_frame_size);
            let framed = Framed::new(stream, codec);
            
            let connection = Connection::new(framed, addr, stats.clone());
            let remote_addr = RemoteAddress::tcp("remote", &addr.ip().to_string(), addr.port());
            
            connections.write().insert(remote_addr, connection);  // .await 제거
        }
    }

    async fn send_message(
        message: &OutgoingMessage,
        connections: Arc<RwLock<HashMap<RemoteAddress, Connection>>>,
        stats: Arc<RwLock<TransportStats>>,
    ) -> TransportResult<()> {
        // 연결을 찾고 보내기
        let connection_exists = {
            let connections = connections.read();
            connections.contains_key(&message.target)
        };
        
        if connection_exists {
            // 실제로는 연결을 통해 메시지를 보내야 함
            // 현재는 placeholder로 구현
            warn!("Connection.send not fully implemented - needs mutable access to framed");
            
            let mut stats = stats.write();
            stats.bytes_sent += message.envelope.encoded_len() as u64;
            stats.messages_sent += 1;
        } else {
            return Err(TransportError::ConnectionFailed {
                address: format!("{:?}", message.target),
                source: std::io::Error::new(std::io::ErrorKind::NotConnected, "No connection"),
            });
        }
        Ok(())
    }
}

/// Connection wrapper
#[derive(Debug)]
pub struct Connection {
    framed: Framed<TcpStream, PekkoCodec>,
    peer_addr: SocketAddr,
    stats: Arc<RwLock<TransportStats>>,
}

impl Connection {
    fn new(
        framed: Framed<TcpStream, PekkoCodec>,
        peer_addr: SocketAddr,
        stats: Arc<RwLock<TransportStats>>,
    ) -> Self {
        Self {
            framed,
            peer_addr,
            stats,
        }
    }

    async fn send(&self, envelope: &RemoteEnvelope) -> TransportResult<()> {
        // Convert to frame and send
        let serialized = Vec::new(); // placeholder - 실제로는 prost로 직렬화
        let frame = Frame::Message(serialized);
        
        // 실제 전송 로직 (placeholder)
        warn!("Connection.send not fully implemented - needs mutable access to framed");
        Ok(())
    }

    async fn close(&self) -> TransportResult<()> {
        debug!("Closed connection to {}", self.peer_addr);
        Ok(())
    }
}

/// Message frame types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Frame {
    Message(Vec<u8>), // 직렬화된 메시지 데이터
    Control(Vec<u8>), // 직렬화된 제어 메시지
    Heartbeat { seq_no: u64 },
    HeartbeatAck { seq_no: u64 },
}

/// Pekko message codec for framing
#[derive(Debug)]
pub struct PekkoCodec {
    max_frame_size: usize,
}

impl PekkoCodec {
    pub fn new(max_frame_size: usize) -> Self {
        Self { max_frame_size }
    }
}

impl Decoder for PekkoCodec {
    type Item = Frame;
    type Error = TransportError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        
        if len > self.max_frame_size {
            return Err(TransportError::InvalidFrame {
                reason: format!("Frame too large: {} bytes", len),
            });
        }

        if src.len() < 4 + len {
            return Ok(None);
        }

        src.advance(4);
        let frame_data = src.split_to(len);
        
        match bincode::deserialize(&frame_data) {
            Ok(frame) => Ok(Some(frame)),
            Err(e) => Err(TransportError::InvalidFrame {
                reason: format!("Deserialization failed: {}", e),
            }),
        }
    }
}

impl Encoder<Frame> for PekkoCodec {
    type Error = TransportError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let frame_data = bincode::serialize(&item)
            .map_err(|e| TransportError::InvalidFrame {
                reason: format!("Serialization failed: {}", e),
            })?;

        if frame_data.len() > self.max_frame_size {
            return Err(TransportError::InvalidFrame {
                reason: format!("Frame too large: {} bytes", frame_data.len()),
            });
        }

        let len = frame_data.len() as u32;
        dst.put_u32(len);
        dst.put_slice(&frame_data);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_frame_serialization() {
        let frame = Frame::Message(Vec::new()); // 직렬화된 데이터 placeholder
        
        // Test that we can serialize/deserialize
        let serialized = bincode::serialize(&frame).unwrap();
        let deserialized: Frame = bincode::deserialize(&serialized).unwrap();
        
        match deserialized {
            Frame::Message(data) => {
                assert_eq!(data.len(), 0); // placeholder 데이터
            }
            _ => panic!("Expected Message frame"),
        }
    }
}
//! Remote Actor System - Integration layer between remoting and local actor system
//!
//! This module provides:
//! - RemoteActorSystem: Wrapper that adds remoting capabilities to ActorSystem
//! - RemoteActorRefProvider: Provides remote actor references
//! - Message routing between local and remote actors

use crate::{
    addressing::{RemoteAddress, RemoteActorPath},
    connection::{ConnectionPool, IncomingMessage, ConnectionState},
    errors::{RemotingError, RemotingResult, TransportError},
    proto::remoting::RemoteEnvelope,
    routing::RemoteActorRef,
    server::RemoteServer,
    transport::TransportConfig,
};
use pekko_actor::{ActorPath, ActorRef, ActorSystem};
use prost::Message;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, warn};

/// Remote actor system configuration
#[derive(Debug, Clone)]
pub struct RemoteSystemConfig {
    /// System name (used in addressing)
    pub system_name: String,
    /// Bind address for accepting connections
    pub bind_host: String,
    /// Bind port
    pub bind_port: u16,
    /// Transport configuration
    pub transport: TransportConfig,
}

impl Default for RemoteSystemConfig {
    fn default() -> Self {
        Self {
            system_name: "default".to_string(),
            bind_host: "127.0.0.1".to_string(),
            bind_port: 2552,
            transport: TransportConfig::default(),
        }
    }
}

impl RemoteSystemConfig {
    pub fn new(system_name: &str) -> Self {
        Self {
            system_name: system_name.to_string(),
            ..Default::default()
        }
    }

    pub fn with_bind(mut self, host: &str, port: u16) -> Self {
        self.bind_host = host.to_string();
        self.bind_port = port;
        self
    }

    /// Get the local address for this system
    pub fn local_address(&self) -> RemoteAddress {
        RemoteAddress::tcp(&self.system_name, &self.bind_host, self.bind_port)
    }
}

/// Remote Actor System - adds remoting to a local ActorSystem
pub struct RemoteActorSystem {
    /// Configuration
    config: RemoteSystemConfig,
    /// Connection pool for outgoing connections
    connection_pool: Arc<ConnectionPool>,
    /// TCP server for accepting connections
    server: Arc<RemoteServer>,
    /// Incoming message receiver
    incoming_rx: RwLock<Option<mpsc::UnboundedReceiver<IncomingMessage>>>,
    /// Local address
    local_address: RemoteAddress,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Remote actor registry (path -> local handler)
    registry: Arc<RwLock<HashMap<String, ActorRefRegistration>>>,
    /// Listener task handle
    listener_handle: RwLock<Option<tokio::task::JoinHandle<()>>>,
}

/// Registration entry for remote-accessible actors
#[derive(Clone)]
pub struct ActorRefRegistration {
    pub path: ActorPath,
    pub handler: Arc<dyn RemoteMessageHandler>,
}

impl std::fmt::Debug for ActorRefRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorRefRegistration")
            .field("path", &self.path)
            .finish()
    }
}

/// Trait for handling incoming remote messages
#[async_trait::async_trait]
pub trait RemoteMessageHandler: Send + Sync {
    /// Handle an incoming remote message
    async fn handle(&self, envelope: RemoteEnvelope) -> RemotingResult<Option<RemoteEnvelope>>;
}

impl RemoteActorSystem {
    /// Create a new remote actor system
    pub fn new(config: RemoteSystemConfig) -> Self {
        let local_address = config.local_address();
        
        // Create channel for incoming messages (shared between pool and server)
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        
        // Create connection pool for outgoing connections
        let (pool, pool_rx) = ConnectionPool::new(
            config.transport.clone(),
            config.system_name.clone(),
        );
        
        // Create server for incoming connections
        let server = RemoteServer::new(
            &config.bind_host,
            config.bind_port,
            config.transport.clone(),
            config.system_name.clone(),
            incoming_tx.clone(),
        );

        Self {
            config: config.clone(),
            connection_pool: Arc::new(pool),
            server: Arc::new(server),
            incoming_rx: RwLock::new(Some(incoming_rx)),
            local_address,
            running: Arc::new(AtomicBool::new(false)),
            registry: Arc::new(RwLock::new(HashMap::new())),
            listener_handle: RwLock::new(None),
        }
    }

    /// Start the remote system (begin accepting connections)
    pub async fn start(&self) -> RemotingResult<()> {
        if self.running.load(Ordering::SeqCst) {
            return Ok(()); // Already running
        }

        self.running.store(true, Ordering::SeqCst);
        
        // Start the TCP server for accepting inbound connections
        self.server.start().await
            .map_err(RemotingError::Transport)?;
        
        // Take the receiver (can only start once)
        let incoming_rx = self.incoming_rx.write().await.take()
            .ok_or_else(|| RemotingError::Transport(TransportError::SendFailed(
                "Remote system already started".to_string()
            )))?;

        // Start incoming message handler
        let registry = self.registry.clone();
        let running = self.running.clone();
        let pool = self.connection_pool.clone();

        let handle = tokio::spawn(async move {
            Self::incoming_message_loop(incoming_rx, registry, running, pool).await;
        });

        *self.listener_handle.write().await = Some(handle);
        Ok(())
    }

    /// Stop the remote system
    pub async fn stop(&self) -> RemotingResult<()> {
        self.running.store(false, Ordering::SeqCst);
        
        // Stop the TCP server
        self.server.stop().await;
        
        // Close all outbound connections
        self.connection_pool.close_all();

        if let Some(handle) = self.listener_handle.write().await.take() {
            handle.abort();
        }
        Ok(())
    }

    /// Process incoming messages
    async fn incoming_message_loop(
        mut incoming_rx: mpsc::UnboundedReceiver<IncomingMessage>,
        registry: Arc<RwLock<HashMap<String, ActorRefRegistration>>>,
        running: Arc<AtomicBool>,
        _pool: Arc<ConnectionPool>,
    ) {
        while running.load(Ordering::SeqCst) {
            match incoming_rx.recv().await {
                Some(msg) => {
                    debug!("Received message from {}", msg.from);

                    // Find the target actor
                    if let Some(recipient) = &msg.envelope.recipient {
                        let path = &recipient.path;
                        let registry_guard = registry.read().await;

                        if let Some(registration) = registry_guard.get(path) {
                            let handler = registration.handler.clone();
                            drop(registry_guard);

                            // Handle the message
                            match handler.handle(msg.envelope).await {
                                Ok(Some(response)) => {
                                    // Send response back (if needed)
                                    debug!("Response generated for message");
                                    // TODO: Send response through connection pool
                                }
                                Ok(None) => {
                                    // No response needed
                                }
                                Err(e) => {
                                    error!("Error handling remote message: {}", e);
                                }
                            }
                        } else {
                            warn!("No handler registered for path: {}", path);
                        }
                    }
                }
                None => {
                    // Channel closed
                    break;
                }
            }
        }
    }

    /// Register a local actor to be accessible remotely
    pub async fn register<H: RemoteMessageHandler + 'static>(
        &self,
        path: ActorPath,
        handler: H,
    ) -> RemotingResult<()> {
        let path_str = path.to_string();
        
        self.registry.write().await.insert(
            path_str.clone(),
            ActorRefRegistration {
                path,
                handler: Arc::new(handler),
            },
        );

        debug!("Registered remote handler for {}", path_str);
        Ok(())
    }

    /// Unregister a local actor
    pub async fn unregister(&self, path: &ActorPath) -> RemotingResult<()> {
        self.registry.write().await.remove(&path.to_string());
        Ok(())
    }

    /// Get a remote actor reference
    pub fn remote_actor(&self, address: RemoteAddress, path: ActorPath) -> RemoteActorRef {
        let remote_path = RemoteActorPath::new(address.clone(), path);
        RemoteActorRef::new(remote_path, address)
    }

    /// Send a message to a remote actor (fire-and-forget)
    pub async fn tell(&self, target: &RemoteActorRef, message: Vec<u8>, manifest: &str) -> RemotingResult<()> {
        let proto_address = crate::proto::remoting::RemoteAddress {
            hostname: target.address.host.clone(),
            port: target.address.port as u32,
            protocol: target.address.protocol.clone(),
            system: target.address.system.clone(),
        };

        let proto_actor_ref = crate::proto::remoting::RemoteActorRef {
            path: target.path.to_string(),
            address: Some(proto_address.clone()),
            uid: "0".to_string(),
        };

        let sender_ref = crate::proto::remoting::RemoteActorRef {
            path: "".to_string(),
            address: Some(crate::proto::remoting::RemoteAddress {
                hostname: self.local_address.host.clone(),
                port: self.local_address.port as u32,
                protocol: self.local_address.protocol.clone(),
                system: self.local_address.system.clone(),
            }),
            uid: "0".to_string(),
        };

        let envelope = RemoteEnvelope {
            message: Some(prost_types::Any {
                type_url: format!("type.pekko/{}", manifest),
                value: message,
            }),
            recipient: Some(proto_actor_ref),
            sender: Some(sender_ref),
            message_manifest: manifest.to_string(),
            metadata: HashMap::new(),
            seq_no: 0,
            timestamp: Some(prost_types::Timestamp {
                seconds: chrono::Utc::now().timestamp(),
                nanos: 0,
            }),
        };

        self.connection_pool.send(&target.address, envelope).await
            .map_err(RemotingError::Transport)
    }

    /// Send a message and wait for response (ask pattern)
    pub async fn ask(
        &self,
        target: &RemoteActorRef,
        message: Vec<u8>,
        manifest: &str,
        timeout_ms: u64,
    ) -> RemotingResult<RemoteEnvelope> {
        let proto_address = crate::proto::remoting::RemoteAddress {
            hostname: target.address.host.clone(),
            port: target.address.port as u32,
            protocol: target.address.protocol.clone(),
            system: target.address.system.clone(),
        };

        let proto_actor_ref = crate::proto::remoting::RemoteActorRef {
            path: target.path.to_string(),
            address: Some(proto_address),
            uid: "0".to_string(),
        };

        let sender_ref = crate::proto::remoting::RemoteActorRef {
            path: "".to_string(),
            address: Some(crate::proto::remoting::RemoteAddress {
                hostname: self.local_address.host.clone(),
                port: self.local_address.port as u32,
                protocol: self.local_address.protocol.clone(),
                system: self.local_address.system.clone(),
            }),
            uid: "0".to_string(),
        };

        let envelope = RemoteEnvelope {
            message: Some(prost_types::Any {
                type_url: format!("type.pekko/{}", manifest),
                value: message,
            }),
            recipient: Some(proto_actor_ref),
            sender: Some(sender_ref),
            message_manifest: manifest.to_string(),
            metadata: HashMap::new(),
            seq_no: 0,
            timestamp: Some(prost_types::Timestamp {
                seconds: chrono::Utc::now().timestamp(),
                nanos: 0,
            }),
        };

        self.connection_pool.ask(&target.address, envelope, timeout_ms).await
            .map_err(RemotingError::Transport)
    }

    /// Get the local address
    pub fn local_address(&self) -> &RemoteAddress {
        &self.local_address
    }

    /// Check if the system is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Get connection pool statistics
    pub async fn stats(&self) -> crate::connection::PoolStats {
        self.connection_pool.stats().await
    }

    /// Get server statistics
    pub fn server_stats(&self) -> crate::server::ServerStats {
        self.server.stats()
    }

    /// Get combined statistics for the remote system
    pub async fn full_stats(&self) -> RemoteSystemStats {
        let pool_stats = self.connection_pool.stats().await;
        let server_stats = self.server.stats();

        RemoteSystemStats {
            // Outbound (client) connections
            outbound_connections: pool_stats.active_connections,
            outbound_total: pool_stats.total_connections,
            // Inbound (server) connections
            inbound_connections: server_stats.active_connections,
            inbound_total: server_stats.total_connections,
            // Server state
            server_running: server_stats.is_running,
            system_running: self.is_running(),
        }
    }
}

/// Combined statistics for the remote system
#[derive(Debug, Clone, Default)]
pub struct RemoteSystemStats {
    /// Active outbound connections (to other systems)
    pub outbound_connections: u32,
    /// Total outbound connections made
    pub outbound_total: u64,
    /// Active inbound connections (from other systems)
    pub inbound_connections: u64,
    /// Total inbound connections received
    pub inbound_total: u64,
    /// Whether the server is accepting connections
    pub server_running: bool,
    /// Whether the system is running
    pub system_running: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_remote_system_creation() {
        let config = RemoteSystemConfig::new("test-system")
            .with_bind("127.0.0.1", 2552);
        
        let system = RemoteActorSystem::new(config);
        
        assert_eq!(system.local_address().system, "test-system");
        assert_eq!(system.local_address().port, 2552);
        assert!(!system.is_running());
    }

    #[tokio::test]
    async fn test_remote_system_config() {
        let config = RemoteSystemConfig::default();
        assert_eq!(config.system_name, "default");
        assert_eq!(config.bind_port, 2552);

        let addr = config.local_address();
        assert_eq!(addr.protocol, "pekko.tcp");
    }

    #[tokio::test]
    async fn test_get_remote_actor() {
        let config = RemoteSystemConfig::new("local-system");
        let system = RemoteActorSystem::new(config);

        let remote_addr = RemoteAddress::tcp("remote-system", "192.168.1.100", 2552);
        let path = ActorPath::parse("pekko://remote-system/user/test-actor").unwrap();

        let remote_ref = system.remote_actor(remote_addr.clone(), path);
        
        assert_eq!(remote_ref.address().host, "192.168.1.100");
        assert_eq!(remote_ref.address().port, 2552);
    }
}

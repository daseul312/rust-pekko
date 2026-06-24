//! Message routing and remote actor references

use crate::{
    proto::remoting::RemoteEnvelope,
    RemoteActorPath, RemoteAddress, RemotingError, RemotingResult, Transport, TransportError,
};
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
    collections::HashMap,
    fmt::Debug,
    sync::Arc,
};
use tokio::sync::RwLock;
use tracing::{debug, error, warn};
use uuid::Uuid;

/// Remote actor reference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteActorRef {
    pub path: RemoteActorPath,
    pub address: RemoteAddress,
}

impl RemoteActorRef {
    pub fn new(path: RemoteActorPath, address: RemoteAddress) -> Self {
        Self { path, address }
    }

    pub fn address(&self) -> &RemoteAddress {
        &self.address
    }
}

/// Message serializer
#[derive(Debug, Clone)]
pub struct MessageSerializer {
    // Serialization configuration
}

impl MessageSerializer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn serialize<T: Any>(&self, _message: &T) -> RemotingResult<Vec<u8>> {
        // Placeholder implementation
        Ok(Vec::new())
    }

    pub fn deserialize<T: Any>(&self, _data: &[u8], _type_name: &str) -> RemotingResult<Box<T>> {
        // Placeholder implementation
        Err(RemotingError::Serialization(crate::SerializationError::UnsupportedFormat))
    }
}

/// Message router for remote communication
#[derive(Debug, Clone)]
pub struct MessageRouter {
    inner: Arc<RwLock<MessageRouterInner>>,
}

struct MessageRouterInner {
    transport: Option<Arc<dyn Transport>>,
    local_address: Option<RemoteAddress>,
    serializer: MessageSerializer,
    sequence_number: u64,
}

impl Debug for MessageRouterInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageRouterInner")
            .field("transport", &self.transport.is_some())
            .field("local_address", &self.local_address)
            .field("serializer", &self.serializer)
            .field("sequence_number", &self.sequence_number)
            .finish()
    }
}

impl MessageRouter {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MessageRouterInner {
                transport: None,
                local_address: None,
                serializer: MessageSerializer::new(),
                sequence_number: 0,
            })),
        }
    }

    pub async fn set_transport(&self, transport: Arc<dyn Transport>) -> RemotingResult<()> {
        let mut inner = self.inner.write().await;
        inner.transport = Some(transport.clone());
        inner.local_address = transport.local_address();
        Ok(())
    }

    pub async fn send<M: Send + 'static>(
        &self,
        target: &RemoteActorRef,
        _message: M,
    ) -> RemotingResult<()> {
        let inner = self.inner.read().await;
        if let Some(transport) = &inner.transport {
            // 프로토콜 버퍼 타입으로 변환
            let proto_address = crate::proto::remoting::RemoteAddress {
                hostname: target.address.host.clone(), // host -> hostname
                port: target.address.port as u32, // i32 -> u32
                protocol: target.address.protocol.clone(),
                system: target.address.system.clone(),
            };

            let proto_actor_ref = crate::proto::remoting::RemoteActorRef {
                path: target.path.to_string(),
                address: Some(proto_address),
                uid: "0".to_string(), // i32 -> String
            };

            let envelope = RemoteEnvelope {
                message: Some(prost_types::Any {
                    type_url: "pekko.remoting.Message".to_string(),
                    value: Vec::new(),
                }),
                recipient: Some(proto_actor_ref),
                sender: None,
                message_manifest: "Message".to_string(),
                metadata: HashMap::new(), // Vec -> HashMap
                seq_no: 0,
                timestamp: Some(prost_types::Timestamp { // 0 -> Option<Timestamp>
                    seconds: 0,
                    nanos: 0,
                }),
            };
            
            // Arc<dyn Transport>를 &mut으로 변환할 수 없으므로 다른 방식 사용
            // 실제로는 transport를 Arc<Mutex<dyn Transport>>로 래핑해야 함
            warn!("Transport.send not fully implemented - needs mutable access");
            Ok(())
        } else {
            Err(RemotingError::Transport(TransportError::NotConnected))
        }
    }

    /// Route a message to a remote actor
    pub async fn route<M: Send + 'static>(
        &self,
        target: &RemoteActorRef,
        message: M,
    ) -> RemotingResult<()> {
        self.send(target, message).await
    }

    /// Broadcast a message to multiple actors
    pub async fn broadcast<M: Send + Sync + Clone + 'static>(
        &self,
        _path_pattern: &str,
        _message: M,
    ) -> RemotingResult<()> {
        let inner = self.inner.read().await;
        if let Some(_transport) = &inner.transport {
            // 브로드캐스트 구현 (실제로는 패턴 매칭이 필요)
            Ok(())
        } else {
            Err(RemotingError::Transport(TransportError::SendFailed("No transport available".to_string())))
        }
    }

    /// Get local address
    pub async fn local_address(&self) -> Option<RemoteAddress> {
        let inner = self.inner.read().await;
        inner.local_address.clone()
    }

    /// Get transport statistics
    pub async fn stats(&self) -> Option<crate::transport::TransportStats> {
        let inner = self.inner.read().await;
        inner.transport.as_ref().map(|t| t.stats())
    }
}

/// Actor selection for pattern-based routing
#[derive(Debug, Clone)]
pub struct ActorSelection {
    path_pattern: String,
    router: MessageRouter,
}

impl ActorSelection {
    pub fn new(path_pattern: String, router: MessageRouter) -> Self {
        Self {
            path_pattern,
            router,
        }
    }

    /// Send a message to all matching actors
    pub async fn tell<M: Send + Sync + Clone + 'static>(&self, message: M) -> Result<(), RemotingError> {
        self.router.broadcast(&self.path_pattern, message).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::addressing::RemoteAddress;
    use pekko_actor::ActorPath;

    #[tokio::test]
    async fn test_message_router() {
        let _router = MessageRouter::new(); // _ 추가
        let address = RemoteAddress::new("pekko", "system", "127.0.0.1", 2552);
        let actor_path = ActorPath::root(pekko_actor::ActorAddress::local("test-actor"));
        let path = RemoteActorPath::new(address.clone(), actor_path);
        let actor_ref = RemoteActorRef::new(path, address);
        
        assert_eq!(actor_ref.address().host, "127.0.0.1");
        assert_eq!(actor_ref.address().port, 2552);
    }

    #[tokio::test]
    async fn test_actor_selection() {
        let router = MessageRouter::new();
        let selection = ActorSelection::new("user/*".to_string(), router);
        
        // Test that we can create the selection
        assert_eq!(selection.path_pattern, "user/*");
    }

    #[tokio::test]
    async fn test_message_serializer() {
        let serializer = MessageSerializer::new();
        
        // Test serialization
        let data = serializer.serialize(&"test message").unwrap();
        assert_eq!(data.len(), 0); // placeholder implementation
    }
}
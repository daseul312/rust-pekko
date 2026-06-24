//! Pekko Remoting - Distributed actor communication for Rust

pub mod addressing;
pub mod artery;
pub mod cluster;
pub mod cluster_sharding;
pub mod cluster_singleton;
pub mod config;
pub mod connection;
pub mod errors;
pub mod failure_detection;
pub mod remote_system;
pub mod routing;
pub mod serialization;
pub mod server;
pub mod split_brain_resolver;
pub mod transport;
pub mod crdt;
pub mod gossip;
pub mod cluster_events;
pub mod distributed_pub_sub;
pub mod adaptive_load_balancing;
pub mod leader_election;
pub mod remote_watcher;
pub mod handshake_protocol;
pub mod serialization_registry;
pub mod cluster_aware_router;
pub mod cluster_client;
pub mod reliable_delivery;
pub mod cluster_metrics;
pub mod remote_deployment;
pub mod multi_dc_cluster;

// Include generated proto modules
pub mod proto {
    pub mod remoting {
        include!(concat!(env!("OUT_DIR"), "/pekko.remoting.rs"));
    }
    pub mod cluster {
        include!(concat!(env!("OUT_DIR"), "/pekko.cluster.rs"));
    }
}

// Re-export commonly used types
pub use bytes::Bytes;
pub use pekko_actor::{Actor, ActorRef, ActorSystem, Props};
pub use tokio;
pub use tracing;
pub use uuid::Uuid;

// Re-export core types
pub use addressing::{RemoteAddress, RemoteActorPath, UniqueAddress};
pub use config::{ClusterConfig, RemotingConfig, TransportConfig};
pub use connection::{ConnectionHandle, ConnectionPool, ConnectionState, ConnectionInfo, IncomingMessage};
pub use remote_system::{RemoteActorSystem, RemoteSystemConfig, RemoteMessageHandler};
pub use server::{RemoteServer, InboundConnection, ServerStats};
pub use cluster_singleton::{ClusterSingletonManager, ClusterSingletonProxy, SingletonSettings, SingletonActor, SingletonFactory, SingletonState};
pub use cluster_sharding::{ShardRegion, ShardingSettings, EntityRef, ShardedEntity, EntityFactory, AllocationStrategy};
pub use errors::*;
pub use routing::{MessageRouter, RemoteActorRef};
pub use split_brain_resolver::{SplitBrainResolver, Partition, Resolution, PartitionDecision, evaluate_partitions};
pub use artery::{ArteryTransport, ArteryConfig, ArteryTransportMode, ArteryEnvelope, StreamId, AssociationState, FlightRecorder, CompressionTable};
pub use transport::{Transport, TcpTransport};
pub use crdt::{CrdtValue, GCounter, PNCounter, GSet, ORSet, LWWRegister, ORMap, Flag, Replicator, DataKey, ReadConsistency, WriteConsistency};
pub use gossip::{GossipManager, GossipConfig, GossipState, GossipEnvelope, MemberState, VersionVector};
pub use cluster_events::{ClusterEventBus, ClusterEventSubscription, EventFilter, EventType, ClusterStateSnapshot};
pub use distributed_pub_sub::{DistributedPubSubMediator, Topic, SubscriberId, SubscriptionGroup, PubSubMessage, Subscription, PublishStatus, DeadLetter, DeadLetterReason, MediatorStats, TopicRouter};
pub use adaptive_load_balancing::{LoadBalancer, LoadBalancerBuilder, NodeId, NodeMetrics, RoutingStrategy, RoutingContext, RoundRobinStrategy, RandomStrategy, ConsistentHashStrategy, LeastLoadStrategy, WeightedStrategy};
pub use leader_election::{LeaderElectionManager, ElectionConfig, ElectionResult, ElectionStats, CandidateId, CandidateInfo, CandidateStatus, LeaderEvent, Lease};
pub use remote_watcher::{WatchTarget, WatchEvent, QuarantineReason, NodeStatus, HeartbeatState, RemoteWatcherConfig, RemoteWatcher, WatcherStats};
pub use handshake_protocol::{ProtocolVersion, Capability, AuthMethod, AuthChallenge, AuthResponse, HelloMessage, ChallengeMessage, ResponseMessage, HandshakeResult, RejectReason, HandshakeState, HandshakeSession, HandshakeCoordinator};
pub use serialization_registry::{SerializerId, TypeId, Codec, Compression, SerializationRegistryError, SerializerEntry, TypeBinding, SerializationRegistry, RegistryStats};
pub use cluster_aware_router::{ClusterAwareStrategy, RoutableMember, MemberReachability, ClusterAwareRouter, ClusterRouterStats};
pub use cluster_client::{ContactPoint, ContactProtocol, ClusterClientSettings, ContactStatus, BufferedMessage, MessagePriority, ClusterClient, ClusterClientStats, SendResult};
pub use reliable_delivery::{DeliverySemantics, ReliableDeliveryConfig, SequenceNr, DeliveryId, Ack, ReliableDelivery, ReliableDeliveryStats, SendOutcome, AckResult, RetryAction};

// Phase 5: Cluster Metrics, Remote Deployment, Multi-DC
pub use cluster_metrics::{MetricsError, MetricType, MetricValue, Ewma, ClusterNodeMetrics, MetricsSelectorType, MetricsSelector, MetricsCollectorConfig, MetricsGossipEnvelope, MetricsCollector, CollectorStats, MetricsRoutingDecision, MetricsAwareRouter, RouterStats};
pub use remote_deployment::{DeploymentError, ResourceRequirements, NodeCapabilities, RemoteScope, DeploymentStrategyType, DeploymentStrategy, RemoteProps, DeployedActor, RemoteDeployer, DeploymentEvent, DeploymentEventType, DeployerStats};
pub use multi_dc_cluster::{MultiDcError, DataCenter, DcMember, DcMemberStatus, CrossDcFailureDetectorConfig, CrossDcFailureDetector, ReplicationMode, ReplicationRequest, ReplicationAck, ReplicationResult, ReplicationManager, ReplicationStats, MultiDcConfig, MultiDcCluster, MultiDcStats, DcStats};

use async_trait::async_trait; // async_trait import 추가

/// Remote actor system trait
#[async_trait]
pub trait RemoteActorSystemProvider: Send + Sync {
    async fn start(&mut self) -> Result<(), RemotingError>;
    async fn stop(&mut self) -> Result<(), RemotingError>;
    async fn deploy_remote<A: Actor + 'static>(&self, props: pekko_actor::Props<A>) -> Result<RemoteActorRef, RemotingError>;
}

/// Default remote actor system implementation
pub struct DefaultRemoteActorSystemProvider {
    config: RemotingConfig,
}

impl DefaultRemoteActorSystemProvider {
    pub fn new(config: RemotingConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl RemoteActorSystemProvider for DefaultRemoteActorSystemProvider {
    async fn start(&mut self) -> Result<(), RemotingError> {
        // Implementation would start the remote system
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), RemotingError> {
        // Implementation would stop the remote system
        Ok(())
    }

    async fn deploy_remote<A: Actor + 'static>(&self, _props: pekko_actor::Props<A>) -> Result<RemoteActorRef, RemotingError> {
        // Implementation would deploy a remote actor
        Err(RemotingError::Transport(TransportError::NotImplemented))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_remote_system_provider() {
        let config = RemotingConfig::default();
        let mut system = DefaultRemoteActorSystemProvider::new(config); // mut 추가
        assert!(system.start().await.is_ok());
    }
}
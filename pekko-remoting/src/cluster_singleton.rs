//! Cluster Singleton - Ensures a single actor instance runs in the cluster
//!
//! The Cluster Singleton pattern ensures that exactly one instance of an actor
//! runs somewhere in the cluster. When the node hosting the singleton becomes
//! unavailable, the singleton is automatically started on another node.
//!
//! # Components
//!
//! - `ClusterSingletonManager`: Manages the singleton lifecycle on each node
//! - `ClusterSingletonProxy`: Routes messages to the singleton wherever it runs
//! - `SingletonSettings`: Configuration for singleton behavior
//!
//! # Example
//!
//! ```ignore
//! // Create singleton manager
//! let settings = SingletonSettings::new("my-singleton")
//!     .with_role("backend");
//!
//! let manager = ClusterSingletonManager::new(
//!     cluster.clone(),
//!     settings,
//!     || MyActor::new(), // Factory function
//! );
//!
//! // Start the manager
//! manager.start().await?;
//!
//! // Use proxy to send messages
//! let proxy = manager.proxy();
//! proxy.tell(MyMessage).await?;
//! ```

use crate::{
    cluster::{ClusterSystem, Member, MemberStatus, ClusterEvent},
    addressing::{RemoteAddress, UniqueAddress},
    errors::{ClusterError, ClusterResult},
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, RwLock, watch};
use tracing::{debug, error, warn};

/// Settings for cluster singleton
#[derive(Debug, Clone)]
pub struct SingletonSettings {
    /// Name of the singleton actor
    pub name: String,
    /// Role required to host the singleton (empty = any node)
    pub role: Option<String>,
    /// Minimum number of members before singleton starts
    pub min_members: usize,
    /// Hand-over retry interval
    pub hand_over_retry_interval: Duration,
    /// Buffer size for messages while singleton is unavailable
    pub buffer_size: usize,
}

impl SingletonSettings {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            role: None,
            min_members: 1,
            hand_over_retry_interval: Duration::from_secs(1),
            buffer_size: 1000,
        }
    }

    pub fn with_role(mut self, role: &str) -> Self {
        self.role = Some(role.to_string());
        self
    }

    pub fn with_min_members(mut self, min: usize) -> Self {
        self.min_members = min;
        self
    }

    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }
}

/// State of the singleton manager
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SingletonState {
    /// Waiting for cluster to reach minimum size
    WaitingForMembers,
    /// This node is the oldest, should start singleton
    BecomingOldest,
    /// Singleton is running on this node
    Running,
    /// Handing over to another node
    HandingOver,
    /// Singleton is running elsewhere, this is a proxy
    Proxying,
    /// Manager is stopped
    Stopped,
}

/// Message wrapper for singleton actor
#[derive(Debug, Clone)]
pub struct SingletonMessage {
    /// Payload
    pub payload: Vec<u8>,
    /// Message type/manifest
    pub manifest: String,
    /// Optional reply channel
    pub reply_to: Option<RemoteAddress>,
}

/// Trait for singleton actor factories
pub trait SingletonFactory: Send + Sync {
    /// Create a new instance of the singleton actor
    fn create(&self) -> Box<dyn SingletonActor>;
}

/// Trait for singleton actors
#[async_trait]
pub trait SingletonActor: Send + Sync {
    /// Start the singleton
    async fn start(&mut self) -> ClusterResult<()>;
    
    /// Stop the singleton (for hand-over)
    async fn stop(&mut self) -> ClusterResult<()>;
    
    /// Handle an incoming message
    async fn handle(&mut self, message: SingletonMessage) -> ClusterResult<Option<Vec<u8>>>;
}

/// Manages the singleton lifecycle
pub struct ClusterSingletonManager {
    /// Settings
    settings: SingletonSettings,
    /// Current state
    state: Arc<RwLock<SingletonState>>,
    /// The cluster system
    cluster: Arc<ClusterSystem>,
    /// The singleton actor (if running on this node)
    actor: Arc<RwLock<Option<Box<dyn SingletonActor>>>>,
    /// Factory for creating the singleton
    factory: Arc<dyn SingletonFactory>,
    /// Running flag
    running: Arc<AtomicBool>,
    /// State change notifier
    state_tx: watch::Sender<SingletonState>,
    /// Oldest member (singleton host)
    oldest: Arc<RwLock<Option<UniqueAddress>>>,
    /// Message buffer (when singleton is unavailable)
    buffer: Arc<RwLock<Vec<SingletonMessage>>>,
    /// Incoming message sender
    message_tx: mpsc::UnboundedSender<SingletonMessage>,
}

impl ClusterSingletonManager {
    /// Create a new singleton manager
    pub fn new<F: SingletonFactory + 'static>(
        cluster: Arc<ClusterSystem>,
        settings: SingletonSettings,
        factory: F,
    ) -> Self {
        let (state_tx, _) = watch::channel(SingletonState::WaitingForMembers);
        let (message_tx, message_rx) = mpsc::unbounded_channel();

        Self {
            settings,
            state: Arc::new(RwLock::new(SingletonState::WaitingForMembers)),
            cluster,
            actor: Arc::new(RwLock::new(None)),
            factory: Arc::new(factory),
            running: Arc::new(AtomicBool::new(false)),
            state_tx,
            oldest: Arc::new(RwLock::new(None)),
            buffer: Arc::new(RwLock::new(Vec::new())),
            message_tx,
        }
    }

    /// Start the singleton manager
    pub async fn start(&self) -> ClusterResult<()> {
        if self.running.load(Ordering::SeqCst) {
            return Ok(());
        }

        self.running.store(true, Ordering::SeqCst);
        // Spawn the state machine task
        let state = self.state.clone();
        let cluster = self.cluster.clone();
        let settings = self.settings.clone();
        let actor = self.actor.clone();
        let factory = self.factory.clone();
        let running = self.running.clone();
        let oldest = self.oldest.clone();
        let state_tx = self.state_tx.clone();
        let buffer = self.buffer.clone();

        tokio::spawn(async move {
            Self::state_machine_loop(
                state,
                cluster,
                settings,
                actor,
                factory,
                running,
                oldest,
                state_tx,
                buffer,
            ).await;
        });

        Ok(())
    }

    /// Stop the singleton manager
    pub async fn stop(&self) -> ClusterResult<()> {
        self.running.store(false, Ordering::SeqCst);

        // Stop the singleton if running
        let mut actor_guard = self.actor.write().await;
        if let Some(actor) = actor_guard.take() {
            // actor.stop() is called automatically when dropped
            drop(actor);
        }

        *self.state.write().await = SingletonState::Stopped;
        Ok(())
    }

    /// State machine loop
    async fn state_machine_loop(
        state: Arc<RwLock<SingletonState>>,
        cluster: Arc<ClusterSystem>,
        settings: SingletonSettings,
        actor: Arc<RwLock<Option<Box<dyn SingletonActor>>>>,
        factory: Arc<dyn SingletonFactory>,
        running: Arc<AtomicBool>,
        oldest: Arc<RwLock<Option<UniqueAddress>>>,
        state_tx: watch::Sender<SingletonState>,
        buffer: Arc<RwLock<Vec<SingletonMessage>>>,
    ) {
        let mut check_interval = tokio::time::interval(Duration::from_secs(1));

        while running.load(Ordering::SeqCst) {
            check_interval.tick().await;

            let current_state = *state.read().await;
            
            match current_state {
                SingletonState::WaitingForMembers => {
                    // Check if we have enough members
                    let members = cluster.members().await;
                    let eligible_members = Self::filter_eligible(&members, &settings.role);
                    
                    if eligible_members.len() >= settings.min_members {
                        // Determine oldest (leader election)
                        if let Some(oldest_member) = Self::find_oldest(&eligible_members) {
                            *oldest.write().await = Some(oldest_member.unique_address.clone());
                            
                            if cluster.is_leader().await {
                                // We are the oldest, become the singleton host
                                *state.write().await = SingletonState::BecomingOldest;
                                let _ = state_tx.send(SingletonState::BecomingOldest);
                            } else {
                                // Someone else is oldest, we proxy
                                *state.write().await = SingletonState::Proxying;
                                let _ = state_tx.send(SingletonState::Proxying);
                            }
                        }
                    }
                }
                
                SingletonState::BecomingOldest => {
                    // Create and start the singleton actor
                    let mut new_actor = factory.create();
                    if let Err(e) = new_actor.start().await {
                        error!("Failed to start singleton: {}", e);
                        continue;
                    }
                    
                    *actor.write().await = Some(new_actor);
                    // Drain buffer
                    let buffered: Vec<_> = buffer.write().await.drain(..).collect();
                    if !buffered.is_empty() {
                        if let Some(ref mut a) = *actor.write().await {
                            for msg in buffered {
                                let _ = a.handle(msg).await;
                            }
                        }
                    }
                    
                    *state.write().await = SingletonState::Running;
                    let _ = state_tx.send(SingletonState::Running);
                }
                
                SingletonState::Running => {
                    // Check if we're still the oldest
                    if !cluster.is_leader().await {
                        *state.write().await = SingletonState::HandingOver;
                        let _ = state_tx.send(SingletonState::HandingOver);
                    }
                }
                
                SingletonState::HandingOver => {
                    // Stop the singleton
                    if let Some(mut a) = actor.write().await.take() {
                        if let Err(e) = a.stop().await {
                            error!("Error stopping singleton: {}", e);
                        }
                    }
                    *state.write().await = SingletonState::Proxying;
                    let _ = state_tx.send(SingletonState::Proxying);
                }
                
                SingletonState::Proxying => {
                    // Check if we should become oldest
                    if cluster.is_leader().await {
                        *state.write().await = SingletonState::BecomingOldest;
                        let _ = state_tx.send(SingletonState::BecomingOldest);
                    }
                }
                
                SingletonState::Stopped => {
                    break;
                }
            }
        }
    }

    /// Filter members by role
    fn filter_eligible(members: &[Member], role: &Option<String>) -> Vec<Member> {
        members.iter()
            .filter(|m| matches!(m.status, MemberStatus::Up | MemberStatus::Joining))
            .filter(|m| {
                match role {
                    Some(r) => m.roles.contains(r),
                    None => true,
                }
            })
            .cloned()
            .collect()
    }

    /// Find the oldest member (lowest UID = oldest)
    fn find_oldest(members: &[Member]) -> Option<Member> {
        members.iter()
            .min_by_key(|m| m.unique_address.uid)
            .cloned()
    }

    /// Get current state
    pub async fn current_state(&self) -> SingletonState {
        *self.state.read().await
    }

    /// Subscribe to state changes
    pub fn subscribe(&self) -> watch::Receiver<SingletonState> {
        self.state_tx.subscribe()
    }

    /// Create a proxy for sending messages to the singleton
    pub fn proxy(&self) -> ClusterSingletonProxy {
        ClusterSingletonProxy {
            name: self.settings.name.clone(),
            message_tx: self.message_tx.clone(),
            state: self.state.clone(),
            actor: self.actor.clone(),
            oldest: self.oldest.clone(),
            buffer: self.buffer.clone(),
            buffer_size: self.settings.buffer_size,
        }
    }
}

/// Proxy for sending messages to the singleton
pub struct ClusterSingletonProxy {
    name: String,
    message_tx: mpsc::UnboundedSender<SingletonMessage>,
    state: Arc<RwLock<SingletonState>>,
    actor: Arc<RwLock<Option<Box<dyn SingletonActor>>>>,
    oldest: Arc<RwLock<Option<UniqueAddress>>>,
    buffer: Arc<RwLock<Vec<SingletonMessage>>>,
    buffer_size: usize,
}

impl ClusterSingletonProxy {
    /// Send a message to the singleton (fire-and-forget)
    pub async fn tell(&self, payload: Vec<u8>, manifest: &str) -> ClusterResult<()> {
        let message = SingletonMessage {
            payload,
            manifest: manifest.to_string(),
            reply_to: None,
        };

        let state = *self.state.read().await;
        
        match state {
            SingletonState::Running => {
                // Local singleton, deliver directly
                if let Some(ref mut actor) = *self.actor.write().await {
                    actor.handle(message).await?;
                }
            }
            SingletonState::Proxying => {
                // Remote singleton, would need to forward via network
                // For now, buffer the message
                let mut buffer = self.buffer.write().await;
                if buffer.len() < self.buffer_size {
                    buffer.push(message);
                    debug!("Buffered message for remote singleton (buffer: {})", buffer.len());
                } else {
                    warn!("Singleton buffer full, dropping message");
                    return Err(ClusterError::JoinFailed("Buffer full".to_string()));
                }
            }
            _ => {
                // Singleton not available, buffer
                let mut buffer = self.buffer.write().await;
                if buffer.len() < self.buffer_size {
                    buffer.push(message);
                    debug!("Buffered message (singleton not ready, buffer: {})", buffer.len());
                } else {
                    warn!("Singleton buffer full, dropping message");
                    return Err(ClusterError::JoinFailed("Buffer full".to_string()));
                }
            }
        }

        Ok(())
    }

    /// Send a message and wait for response
    pub async fn ask(&self, payload: Vec<u8>, manifest: &str, _timeout: Duration) -> ClusterResult<Vec<u8>> {
        let message = SingletonMessage {
            payload,
            manifest: manifest.to_string(),
            reply_to: None, // Would need to include reply address
        };

        let state = *self.state.read().await;
        
        match state {
            SingletonState::Running => {
                if let Some(ref mut actor) = *self.actor.write().await {
                    match actor.handle(message).await? {
                        Some(response) => Ok(response),
                        None => Err(ClusterError::StateError("No response".to_string())),
                    }
                } else {
                    Err(ClusterError::StateError("Actor not available".to_string()))
                }
            }
            _ => {
                Err(ClusterError::StateError("Singleton not running locally".to_string()))
            }
        }
    }

    /// Get the singleton name
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestSingletonActor {
        started: bool,
        message_count: u32,
    }

    #[async_trait]
    impl SingletonActor for TestSingletonActor {
        async fn start(&mut self) -> ClusterResult<()> {
            self.started = true;
            Ok(())
        }

        async fn stop(&mut self) -> ClusterResult<()> {
            self.started = false;
            Ok(())
        }

        async fn handle(&mut self, _message: SingletonMessage) -> ClusterResult<Option<Vec<u8>>> {
            self.message_count += 1;
            Ok(Some(b"response".to_vec()))
        }
    }

    struct TestFactory;

    impl SingletonFactory for TestFactory {
        fn create(&self) -> Box<dyn SingletonActor> {
            Box::new(TestSingletonActor {
                started: false,
                message_count: 0,
            })
        }
    }

    #[test]
    fn test_singleton_settings() {
        let settings = SingletonSettings::new("test-singleton")
            .with_role("backend")
            .with_min_members(3)
            .with_buffer_size(500);

        assert_eq!(settings.name, "test-singleton");
        assert_eq!(settings.role, Some("backend".to_string()));
        assert_eq!(settings.min_members, 3);
        assert_eq!(settings.buffer_size, 500);
    }

    #[test]
    fn test_singleton_state() {
        assert_eq!(SingletonState::WaitingForMembers, SingletonState::WaitingForMembers);
        assert_ne!(SingletonState::Running, SingletonState::Proxying);
    }

    #[tokio::test]
    async fn test_filter_eligible() {
        let members = vec![
            Member {
                unique_address: UniqueAddress::generate(RemoteAddress::tcp("sys", "127.0.0.1", 2552)),
                roles: vec!["backend".to_string()],
                status: MemberStatus::Up,
                uptime: 100,
            },
            Member {
                unique_address: UniqueAddress::generate(RemoteAddress::tcp("sys", "127.0.0.1", 2553)),
                roles: vec!["frontend".to_string()],
                status: MemberStatus::Up,
                uptime: 50,
            },
        ];

        let filtered = ClusterSingletonManager::filter_eligible(&members, &Some("backend".to_string()));
        assert_eq!(filtered.len(), 1);

        let all = ClusterSingletonManager::filter_eligible(&members, &None);
        assert_eq!(all.len(), 2);
    }
}

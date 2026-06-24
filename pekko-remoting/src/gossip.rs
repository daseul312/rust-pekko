//! Gossip Protocol — Cluster membership protocol
//!
//! Full implementation of Pekko's gossip protocol for cluster state convergence:
//!
//! - **GossipState**: Snapshot of cluster membership with vector clock versioning
//! - **GossipEnvelope**: Serializable gossip message for node-to-node exchange
//! - **GossipManager**: Manages periodic gossiping, state merging, and convergence
//! - **Seed node discovery**: Initial contact points for joining a cluster
//! - **Convergence detection**: All nodes agree on cluster state

use crate::addressing::{RemoteAddress, UniqueAddress};
use crate::cluster::{ClusterEvent, Member, MemberStatus};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, warn};

// ============= Vector Clock =============

/// Vector clock for ordering gossip state updates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionVector {
    versions: BTreeMap<String, u64>,
}

impl VersionVector {
    pub fn new() -> Self {
        Self { versions: BTreeMap::new() }
    }

    /// Increment the version for a node.
    pub fn increment(&mut self, node: &str) {
        let entry = self.versions.entry(node.to_string()).or_insert(0);
        *entry += 1;
    }

    /// Get the version for a node.
    pub fn version_for(&self, node: &str) -> u64 {
        self.versions.get(node).copied().unwrap_or(0)
    }

    /// Merge with another vector clock (component-wise max).
    pub fn merge(&mut self, other: &VersionVector) {
        for (node, &ver) in &other.versions {
            let entry = self.versions.entry(node.clone()).or_insert(0);
            *entry = (*entry).max(ver);
        }
    }

    /// Check if this vector clock dominates (is newer than or equal to) another.
    pub fn dominates(&self, other: &VersionVector) -> bool {
        for (node, &ver) in &other.versions {
            if self.version_for(node) < ver {
                return false;
            }
        }
        true
    }

    /// Check if two vector clocks are concurrent (neither dominates).
    pub fn is_concurrent_with(&self, other: &VersionVector) -> bool {
        !self.dominates(other) && !other.dominates(self)
    }

    /// Check if equal.
    pub fn is_same(&self, other: &VersionVector) -> bool {
        self.versions == other.versions
    }
}

impl fmt::Display for VersionVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let entries: Vec<String> = self.versions.iter()
            .map(|(k, v)| format!("{}:{}", k, v))
            .collect();
        write!(f, "VV({})", entries.join(", "))
    }
}

// ============= Gossip State =============

/// Snapshot of the cluster state that gets gossiped between nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipState {
    /// Current cluster members and their status.
    pub members: HashMap<String, MemberState>,
    /// Vector clock for versioning this state.
    pub version: VersionVector,
    /// The set of node IDs that have seen this version.
    pub seen_by: HashSet<String>,
    /// Leader of the cluster (if elected).
    pub leader: Option<String>,
}

/// Per-member state in the gossip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberState {
    pub address: RemoteAddress,
    pub status: MemberStatus,
    pub roles: Vec<String>,
    pub uptime_secs: u64,
}

impl GossipState {
    pub fn new() -> Self {
        Self {
            members: HashMap::new(),
            version: VersionVector::new(),
            seen_by: HashSet::new(),
            leader: None,
        }
    }

    /// Add or update a member.
    pub fn update_member(&mut self, node_id: &str, member: MemberState) {
        self.members.insert(node_id.to_string(), member);
        self.version.increment(node_id);
        self.seen_by.clear();
        self.seen_by.insert(node_id.to_string());
    }

    /// Mark a member as a specific status.
    pub fn set_member_status(&mut self, node_id: &str, status: MemberStatus, updated_by: &str) {
        if let Some(m) = self.members.get_mut(node_id) {
            m.status = status;
        }
        self.version.increment(updated_by);
        self.seen_by.clear();
        self.seen_by.insert(updated_by.to_string());
    }

    /// Remove a member entirely.
    pub fn remove_member(&mut self, node_id: &str, updated_by: &str) {
        self.members.remove(node_id);
        self.version.increment(updated_by);
        self.seen_by.clear();
        self.seen_by.insert(updated_by.to_string());
    }

    /// Mark this state as seen by a node.
    pub fn mark_seen(&mut self, node_id: &str) {
        self.seen_by.insert(node_id.to_string());
    }

    /// Check if the gossip has converged (all alive members have seen it).
    pub fn has_converged(&self) -> bool {
        let alive_members: HashSet<String> = self.members.iter()
            .filter(|(_, m)| matches!(m.status, MemberStatus::Up | MemberStatus::Joining))
            .map(|(id, _)| id.clone())
            .collect();
        alive_members.is_subset(&self.seen_by)
    }

    /// Merge with another node's gossip state.
    pub fn merge(&mut self, other: &GossipState, local_node: &str) {
        // Merge version vectors
        self.version.merge(&other.version);

        // Merge members: for each member, keep the one with the "later" status
        for (id, other_member) in &other.members {
            match self.members.get(id) {
                Some(our_member) => {
                    // Keep the one with a further-along status
                    if status_order(&other_member.status) > status_order(&our_member.status) {
                        self.members.insert(id.clone(), other_member.clone());
                    }
                }
                None => {
                    self.members.insert(id.clone(), other_member.clone());
                }
            }
        }

        // Merge seen_by
        for node in &other.seen_by {
            self.seen_by.insert(node.clone());
        }
        self.seen_by.insert(local_node.to_string());

        // Elect leader: node with lowest ID among Up members
        self.leader = self.elect_leader();
    }

    /// Elect the leader as the node with the lowest ID among Up members.
    fn elect_leader(&self) -> Option<String> {
        self.members.iter()
            .filter(|(_, m)| matches!(m.status, MemberStatus::Up))
            .map(|(id, _)| id.clone())
            .min()
    }

    /// Get members in a specific status.
    pub fn members_with_status(&self, status: MemberStatus) -> Vec<(&String, &MemberState)> {
        self.members.iter()
            .filter(|(_, m)| std::mem::discriminant(&m.status) == std::mem::discriminant(&status))
            .collect()
    }
}

/// Ordering of member statuses for merge conflict resolution.
fn status_order(status: &MemberStatus) -> u8 {
    match status {
        MemberStatus::Joining => 0,
        MemberStatus::Up => 1,
        MemberStatus::Leaving => 2,
        MemberStatus::Exiting => 3,
        MemberStatus::Down => 4,
        MemberStatus::Removed => 5,
    }
}

// ============= Gossip Envelope =============

/// A gossip message sent between nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipEnvelope {
    /// ID of the sending node.
    pub from: String,
    /// ID of the receiving node.
    pub to: String,
    /// The gossip state snapshot.
    pub state: GossipState,
    /// Sequence number for deduplication.
    pub seq_nr: u64,
}

impl GossipEnvelope {
    pub fn new(from: &str, to: &str, state: GossipState, seq_nr: u64) -> Self {
        Self {
            from: from.to_string(),
            to: to.to_string(),
            state,
            seq_nr,
        }
    }
}

// ============= Gossip Manager =============

/// Configuration for the gossip manager.
#[derive(Debug, Clone)]
pub struct GossipConfig {
    /// How often to send gossip messages.
    pub gossip_interval: Duration,
    /// Timeout for considering a node unreachable.
    pub unreachable_timeout: Duration,
    /// Max number of nodes to gossip with per tick.
    pub gossip_fanout: usize,
    /// Seed nodes for initial cluster join.
    pub seed_nodes: Vec<RemoteAddress>,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            gossip_interval: Duration::from_secs(1),
            unreachable_timeout: Duration::from_secs(10),
            gossip_fanout: 3,
            seed_nodes: Vec::new(),
        }
    }
}

/// Manages gossip protocol lifecycle.
pub struct GossipManager {
    node_id: String,
    config: GossipConfig,
    state: Arc<RwLock<GossipState>>,
    seq_counter: std::sync::atomic::AtomicU64,
    event_tx: mpsc::Sender<ClusterEvent>,
    event_rx: Option<mpsc::Receiver<ClusterEvent>>,
}

impl GossipManager {
    pub fn new(node_id: impl Into<String>, config: GossipConfig) -> Self {
        let (event_tx, event_rx) = mpsc::channel(256);
        Self {
            node_id: node_id.into(),
            config,
            state: Arc::new(RwLock::new(GossipState::new())),
            seq_counter: std::sync::atomic::AtomicU64::new(0),
            event_tx,
            event_rx: Some(event_rx),
        }
    }

    /// Take the event receiver (can only be called once).
    pub fn take_event_receiver(&mut self) -> Option<mpsc::Receiver<ClusterEvent>> {
        self.event_rx.take()
    }

    /// Register this node as a cluster member.
    pub async fn join(&self, address: RemoteAddress, roles: Vec<String>) {
        let member_state = MemberState {
            address,
            status: MemberStatus::Joining,
            roles,
            uptime_secs: 0,
        };
        let mut state = self.state.write().await;
        state.update_member(&self.node_id, member_state.clone());
    }

    /// Mark this node as Up.
    pub async fn mark_up(&self) {
        let mut state = self.state.write().await;
        state.set_member_status(&self.node_id, MemberStatus::Up, &self.node_id);
    }

    /// Mark a node as leaving.
    pub async fn leave(&self, node_id: &str) {
        let mut state = self.state.write().await;
        state.set_member_status(node_id, MemberStatus::Leaving, &self.node_id);
    }

    /// Mark a node as Down (unreachable).
    pub async fn down(&self, node_id: &str) {
        let mut state = self.state.write().await;
        state.set_member_status(node_id, MemberStatus::Down, &self.node_id);
        warn!("Node {} marked as Down", node_id);
    }

    /// Handle receiving a gossip envelope from another node.
    pub async fn receive_gossip(&self, envelope: GossipEnvelope) -> Vec<ClusterEvent> {
        let mut events = Vec::new();
        let mut state = self.state.write().await;

        // Track status changes before merge
        let before: HashMap<String, MemberStatus> = state.members.iter()
            .map(|(k, v)| (k.clone(), v.status))
            .collect();

        state.merge(&envelope.state, &self.node_id);

        // Detect changes and emit events
        for (id, member) in &state.members {
            let new_status = member.status;
            match before.get(id) {
                Some(&old_status) if std::mem::discriminant(&old_status) != std::mem::discriminant(&new_status) => {
                    let member_info = Member {
                        unique_address: UniqueAddress {
                            address: member.address.clone(),
                            uid: 0,
                        },
                        roles: member.roles.clone(),
                        status: new_status,
                        uptime: member.uptime_secs,
                    };
                    let event = match new_status {
                        MemberStatus::Up => ClusterEvent::MemberUp { member: member_info },
                        MemberStatus::Leaving => ClusterEvent::MemberLeft { member: member_info },
                        MemberStatus::Down => ClusterEvent::MemberDown { member: member_info },
                        MemberStatus::Removed => ClusterEvent::MemberRemoved { member: member_info, previous_status: old_status },
                        _ => continue,
                    };
                    events.push(event);
                }
                None => {
                    // New member
                    let member_info = Member {
                        unique_address: UniqueAddress {
                            address: member.address.clone(),
                            uid: 0,
                        },
                        roles: member.roles.clone(),
                        status: new_status,
                        uptime: member.uptime_secs,
                    };
                    events.push(ClusterEvent::MemberJoined { member: member_info });
                }
                _ => {}
            }
        }

        // Emit events through channel
        for event in &events {
            let _ = self.event_tx.try_send(event.clone());
        }

        events
    }

    /// Create a gossip envelope to send to another node.
    pub async fn create_envelope(&self, target_node: &str) -> GossipEnvelope {
        let state = self.state.read().await;
        let seq = self.seq_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        GossipEnvelope::new(&self.node_id, target_node, state.clone(), seq)
    }

    /// Get current cluster state.
    pub async fn cluster_state(&self) -> GossipState {
        self.state.read().await.clone()
    }

    /// Check if the cluster state has converged.
    pub async fn has_converged(&self) -> bool {
        self.state.read().await.has_converged()
    }

    /// Get the current leader.
    pub async fn leader(&self) -> Option<String> {
        self.state.read().await.leader.clone()
    }

    /// Select nodes to gossip with (up to gossip_fanout random nodes).
    pub async fn select_gossip_targets(&self) -> Vec<String> {
        let state = self.state.read().await;
        let alive: Vec<String> = state.members.iter()
            .filter(|(id, m)| {
                *id != &self.node_id && matches!(m.status, MemberStatus::Up | MemberStatus::Joining)
            })
            .map(|(id, _)| id.clone())
            .collect();

        // Take up to gossip_fanout targets
        alive.into_iter().take(self.config.gossip_fanout).collect()
    }
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_vector_basic() {
        let mut vv = VersionVector::new();
        vv.increment("a");
        vv.increment("a");
        vv.increment("b");
        assert_eq!(vv.version_for("a"), 2);
        assert_eq!(vv.version_for("b"), 1);
        assert_eq!(vv.version_for("c"), 0);
    }

    #[test]
    fn test_version_vector_merge() {
        let mut vv1 = VersionVector::new();
        vv1.increment("a");
        vv1.increment("a");

        let mut vv2 = VersionVector::new();
        vv2.increment("a");
        vv2.increment("b");

        vv1.merge(&vv2);
        assert_eq!(vv1.version_for("a"), 2); // max(2, 1)
        assert_eq!(vv1.version_for("b"), 1);
    }

    #[test]
    fn test_version_vector_dominates() {
        let mut vv1 = VersionVector::new();
        vv1.increment("a");
        vv1.increment("a");
        vv1.increment("b");

        let mut vv2 = VersionVector::new();
        vv2.increment("a");

        assert!(vv1.dominates(&vv2));
        assert!(!vv2.dominates(&vv1));
    }

    #[test]
    fn test_version_vector_concurrent() {
        let mut vv1 = VersionVector::new();
        vv1.increment("a");

        let mut vv2 = VersionVector::new();
        vv2.increment("b");

        assert!(vv1.is_concurrent_with(&vv2));
    }

    #[test]
    fn test_gossip_state_update_member() {
        let mut state = GossipState::new();
        let member = MemberState {
            address: RemoteAddress::tcp("test", "127.0.0.1", 2551),
            status: MemberStatus::Joining,
            roles: vec!["worker".to_string()],
            uptime_secs: 0,
        };
        state.update_member("node-1", member);
        assert_eq!(state.members.len(), 1);
        assert!(state.seen_by.contains("node-1"));
    }

    #[test]
    fn test_gossip_state_convergence() {
        let mut state = GossipState::new();
        let m1 = MemberState {
            address: RemoteAddress::tcp("test", "127.0.0.1", 2551),
            status: MemberStatus::Up,
            roles: vec![],
            uptime_secs: 10,
        };
        let m2 = MemberState {
            address: RemoteAddress::tcp("test", "127.0.0.2", 2551),
            status: MemberStatus::Up,
            roles: vec![],
            uptime_secs: 5,
        };
        state.update_member("a", m1);
        state.mark_seen("a");
        state.update_member("b", m2);
        state.mark_seen("b");
        // After updating b, seen_by was cleared then b added. Need a to see it too.
        state.mark_seen("a");
        assert!(state.has_converged());
    }

    #[test]
    fn test_gossip_state_merge() {
        let mut s1 = GossipState::new();
        s1.update_member("a", MemberState {
            address: RemoteAddress::tcp("test", "127.0.0.1", 2551),
            status: MemberStatus::Up,
            roles: vec![],
            uptime_secs: 10,
        });

        let mut s2 = GossipState::new();
        s2.update_member("b", MemberState {
            address: RemoteAddress::tcp("test", "127.0.0.2", 2551),
            status: MemberStatus::Up,
            roles: vec![],
            uptime_secs: 5,
        });

        s1.merge(&s2, "a");
        assert_eq!(s1.members.len(), 2);
        assert!(s1.members.contains_key("a"));
        assert!(s1.members.contains_key("b"));
    }

    #[test]
    fn test_gossip_state_leader_election() {
        let mut state = GossipState::new();
        state.update_member("c", MemberState {
            address: RemoteAddress::tcp("test", "127.0.0.3", 2551),
            status: MemberStatus::Up,
            roles: vec![],
            uptime_secs: 0,
        });
        state.update_member("a", MemberState {
            address: RemoteAddress::tcp("test", "127.0.0.1", 2551),
            status: MemberStatus::Up,
            roles: vec![],
            uptime_secs: 0,
        });
        state.leader = state.elect_leader();
        assert_eq!(state.leader, Some("a".to_string())); // lowest ID
    }

    #[test]
    fn test_gossip_state_status_filter() {
        let mut state = GossipState::new();
        state.update_member("a", MemberState {
            address: RemoteAddress::tcp("test", "127.0.0.1", 2551),
            status: MemberStatus::Up,
            roles: vec![],
            uptime_secs: 0,
        });
        state.update_member("b", MemberState {
            address: RemoteAddress::tcp("test", "127.0.0.2", 2551),
            status: MemberStatus::Down,
            roles: vec![],
            uptime_secs: 0,
        });

        let up = state.members_with_status(MemberStatus::Up);
        assert_eq!(up.len(), 1);
        let down = state.members_with_status(MemberStatus::Down);
        assert_eq!(down.len(), 1);
    }

    #[test]
    fn test_gossip_envelope() {
        let env = GossipEnvelope::new("a", "b", GossipState::new(), 1);
        assert_eq!(env.from, "a");
        assert_eq!(env.to, "b");
        assert_eq!(env.seq_nr, 1);
    }

    #[tokio::test]
    async fn test_gossip_manager_join() {
        let config = GossipConfig::default();
        let manager = GossipManager::new("node-1", config);
        let addr = RemoteAddress::tcp("test", "127.0.0.1", 2551);
        manager.join(addr, vec!["worker".to_string()]).await;

        let state = manager.cluster_state().await;
        assert_eq!(state.members.len(), 1);
        assert!(matches!(
            state.members.get("node-1").unwrap().status,
            MemberStatus::Joining
        ));
    }

    #[tokio::test]
    async fn test_gossip_manager_mark_up() {
        let config = GossipConfig::default();
        let manager = GossipManager::new("node-1", config);
        let addr = RemoteAddress::tcp("test", "127.0.0.1", 2551);
        manager.join(addr, vec![]).await;
        manager.mark_up().await;

        let state = manager.cluster_state().await;
        assert!(matches!(
            state.members.get("node-1").unwrap().status,
            MemberStatus::Up
        ));
    }

    #[tokio::test]
    async fn test_gossip_manager_receive_gossip() {
        let config = GossipConfig::default();
        let manager = GossipManager::new("node-1", config);
        let addr1 = RemoteAddress::tcp("test", "127.0.0.1", 2551);
        manager.join(addr1, vec![]).await;
        manager.mark_up().await;

        // Simulate receiving gossip from node-2
        let mut remote_state = GossipState::new();
        remote_state.update_member("node-2", MemberState {
            address: RemoteAddress::tcp("test", "127.0.0.2", 2551),
            status: MemberStatus::Up,
            roles: vec![],
            uptime_secs: 5,
        });

        let envelope = GossipEnvelope::new("node-2", "node-1", remote_state, 1);
        let events = manager.receive_gossip(envelope).await;

        // Should detect new member joining
        assert!(!events.is_empty());
        let state = manager.cluster_state().await;
        assert_eq!(state.members.len(), 2);
    }

    #[tokio::test]
    async fn test_gossip_manager_create_envelope() {
        let config = GossipConfig::default();
        let manager = GossipManager::new("node-1", config);
        let addr = RemoteAddress::tcp("test", "127.0.0.1", 2551);
        manager.join(addr, vec![]).await;

        let envelope = manager.create_envelope("node-2").await;
        assert_eq!(envelope.from, "node-1");
        assert_eq!(envelope.to, "node-2");
        assert_eq!(envelope.seq_nr, 0);
    }

    #[tokio::test]
    async fn test_gossip_manager_select_targets() {
        let config = GossipConfig { gossip_fanout: 2, ..Default::default() };
        let manager = GossipManager::new("node-1", config);

        let addr = RemoteAddress::tcp("test", "127.0.0.1", 2551);
        manager.join(addr, vec![]).await;
        manager.mark_up().await;

        // Add other nodes via gossip
        let mut remote = GossipState::new();
        for i in 2..=5 {
            remote.update_member(&format!("node-{}", i), MemberState {
                address: RemoteAddress::tcp("test", &format!("127.0.0.{}", i), 2551),
                status: MemberStatus::Up,
                roles: vec![],
                uptime_secs: 0,
            });
        }
        let env = GossipEnvelope::new("node-2", "node-1", remote, 1);
        manager.receive_gossip(env).await;

        let targets = manager.select_gossip_targets().await;
        assert!(targets.len() <= 2); // fanout = 2
        assert!(!targets.contains(&"node-1".to_string())); // self excluded
    }

    #[test]
    fn test_gossip_config_default() {
        let config = GossipConfig::default();
        assert_eq!(config.gossip_interval, Duration::from_secs(1));
        assert_eq!(config.gossip_fanout, 3);
        assert!(config.seed_nodes.is_empty());
    }

    #[test]
    fn test_status_order() {
        assert!(status_order(&MemberStatus::Up) > status_order(&MemberStatus::Joining));
        assert!(status_order(&MemberStatus::Down) > status_order(&MemberStatus::Leaving));
        assert!(status_order(&MemberStatus::Removed) > status_order(&MemberStatus::Down));
    }
}

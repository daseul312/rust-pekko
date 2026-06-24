//! Cluster Events — Event subscription and distribution system
//!
//! Provides a typed event bus for cluster membership changes:
//!
//! - **ClusterEventBus**: Subscribe to and receive cluster events
//! - **ClusterEventSubscription**: Individual subscription handle
//! - **EventFilter**: Filter events by type
//! - **ClusterStateSnapshot**: Point-in-time snapshot of full cluster state

use crate::addressing::RemoteAddress;
use crate::cluster::{ClusterEvent, Member, MemberStatus};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug};

// ============= Event Filter =============

/// Filter for selecting which cluster events to receive.
#[derive(Debug, Clone)]
pub enum EventFilter {
    /// Receive all events
    All,
    /// Only member status changes
    MemberEvents,
    /// Only leadership changes
    LeaderEvents,
    /// Only unreachability changes
    ReachabilityEvents,
    /// Custom filter: match specific event types
    Custom(Vec<EventType>),
}

/// Types of cluster events for filtering.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EventType {
    MemberJoined,
    MemberUp,
    MemberLeft,
    MemberExited,
    MemberDown,
    MemberRemoved,
    LeaderChanged,
    RoleLeaderChanged,
    UnreachableMember,
    ClusterShuttingDown,
}

impl EventFilter {
    /// Check if a cluster event matches this filter.
    pub fn matches(&self, event: &ClusterEvent) -> bool {
        match self {
            EventFilter::All => true,
            EventFilter::MemberEvents => matches!(
                event,
                ClusterEvent::MemberJoined { .. }
                    | ClusterEvent::MemberUp { .. }
                    | ClusterEvent::MemberLeft { .. }
                    | ClusterEvent::MemberExited { .. }
                    | ClusterEvent::MemberDown { .. }
                    | ClusterEvent::MemberRemoved { .. }
            ),
            EventFilter::LeaderEvents => matches!(
                event,
                ClusterEvent::LeaderChanged { .. } | ClusterEvent::RoleLeaderChanged { .. }
            ),
            EventFilter::ReachabilityEvents => matches!(
                event,
                ClusterEvent::UnreachableMember { .. }
            ),
            EventFilter::Custom(types) => {
                let event_type = event_to_type(event);
                types.contains(&event_type)
            }
        }
    }
}

fn event_to_type(event: &ClusterEvent) -> EventType {
    match event {
        ClusterEvent::MemberJoined { .. } => EventType::MemberJoined,
        ClusterEvent::MemberUp { .. } => EventType::MemberUp,
        ClusterEvent::MemberLeft { .. } => EventType::MemberLeft,
        ClusterEvent::MemberExited { .. } => EventType::MemberExited,
        ClusterEvent::MemberDown { .. } => EventType::MemberDown,
        ClusterEvent::MemberRemoved { .. } => EventType::MemberRemoved,
        ClusterEvent::LeaderChanged { .. } => EventType::LeaderChanged,
        ClusterEvent::RoleLeaderChanged { .. } => EventType::RoleLeaderChanged,
        ClusterEvent::UnreachableMember { .. } => EventType::UnreachableMember,
        ClusterEvent::ClusterShuttingDown => EventType::ClusterShuttingDown,
    }
}

// ============= Cluster Event Bus =============

/// Central event bus for distributing cluster events to subscribers.
pub struct ClusterEventBus {
    /// Broadcast channel for event distribution
    sender: broadcast::Sender<ClusterEvent>,
    /// Subscription counter
    subscription_counter: AtomicU64,
    /// Track active subscriptions
    subscriptions: RwLock<HashMap<u64, SubscriptionInfo>>,
    /// Event history (bounded)
    history: RwLock<Vec<ClusterEvent>>,
    /// Max history size
    max_history: usize,
    /// Total events published
    events_published: AtomicU64,
}

struct SubscriptionInfo {
    filter: EventFilter,
    label: String,
}

impl ClusterEventBus {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender,
            subscription_counter: AtomicU64::new(0),
            subscriptions: RwLock::new(HashMap::new()),
            history: RwLock::new(Vec::new()),
            max_history: 1000,
            events_published: AtomicU64::new(0),
        }
    }

    /// Subscribe to cluster events with a filter.
    pub async fn subscribe(
        &self,
        filter: EventFilter,
        label: impl Into<String>,
    ) -> ClusterEventSubscription {
        let id = self.subscription_counter.fetch_add(1, Ordering::Relaxed);
        let receiver = self.sender.subscribe();
        let label_str = label.into();

        let mut subs = self.subscriptions.write().await;
        subs.insert(id, SubscriptionInfo {
            filter: filter.clone(),
            label: label_str.clone(),
        });

        debug!("New subscription #{} ({}) with filter {:?}", id, label_str, filter);

        ClusterEventSubscription {
            id,
            receiver,
            filter,
        }
    }

    /// Unsubscribe by ID.
    pub async fn unsubscribe(&self, id: u64) {
        let mut subs = self.subscriptions.write().await;
        if subs.remove(&id).is_some() {
            debug!("Removed subscription #{}", id);
        }
    }

    /// Publish an event to all subscribers.
    pub async fn publish(&self, event: ClusterEvent) {
        self.events_published.fetch_add(1, Ordering::Relaxed);

        // Add to history
        {
            let mut history = self.history.write().await;
            history.push(event.clone());
            if history.len() > self.max_history {
                history.remove(0);
            }
        }

        // Broadcast (ignore send errors — no subscribers is okay)
        let _ = self.sender.send(event);
    }

    /// Get the count of active subscriptions.
    pub async fn subscription_count(&self) -> usize {
        self.subscriptions.read().await.len()
    }

    /// Get total events published.
    pub fn events_published(&self) -> u64 {
        self.events_published.load(Ordering::Relaxed)
    }

    /// Get event history.
    pub async fn history(&self) -> Vec<ClusterEvent> {
        self.history.read().await.clone()
    }

    /// Get the current state based on event history.
    pub async fn current_state(&self) -> ClusterStateSnapshot {
        let history = self.history.read().await;
        let mut members: HashMap<String, MemberInfo> = HashMap::new();
        let mut leader: Option<String> = None;

        for event in history.iter() {
            match event {
                ClusterEvent::MemberJoined { member } | ClusterEvent::MemberUp { member } => {
                    let key = format!("{}:{}", member.unique_address.address.host, member.unique_address.address.port);
                    members.insert(key, MemberInfo {
                        address: member.unique_address.address.clone(),
                        status: member.status,
                        roles: member.roles.clone(),
                    });
                }
                ClusterEvent::MemberDown { member } | ClusterEvent::MemberRemoved { member, .. } => {
                    let key = format!("{}:{}", member.unique_address.address.host, member.unique_address.address.port);
                    members.insert(key, MemberInfo {
                        address: member.unique_address.address.clone(),
                        status: member.status,
                        roles: member.roles.clone(),
                    });
                }
                ClusterEvent::LeaderChanged { leader: l } => {
                    leader = l.as_ref().map(|u| format!("{}:{}", u.address.host, u.address.port));
                }
                _ => {}
            }
        }

        ClusterStateSnapshot { members, leader }
    }
}

/// A subscription to cluster events.
pub struct ClusterEventSubscription {
    pub id: u64,
    receiver: broadcast::Receiver<ClusterEvent>,
    filter: EventFilter,
}

impl ClusterEventSubscription {
    /// Wait for the next event that matches the filter.
    pub async fn recv(&mut self) -> Option<ClusterEvent> {
        loop {
            match self.receiver.recv().await {
                Ok(event) => {
                    if self.filter.matches(&event) {
                        return Some(event);
                    }
                    // Skip non-matching events
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    debug!("Subscription #{} lagged by {} events", self.id, n);
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return None;
                }
            }
        }
    }

    /// Non-blocking try_recv.
    pub fn try_recv(&mut self) -> Option<ClusterEvent> {
        loop {
            match self.receiver.try_recv() {
                Ok(event) => {
                    if self.filter.matches(&event) {
                        return Some(event);
                    }
                }
                Err(_) => return None,
            }
        }
    }
}

// ============= Cluster State Snapshot =============

/// Member info for snapshot.
#[derive(Debug, Clone)]
pub struct MemberInfo {
    pub address: RemoteAddress,
    pub status: MemberStatus,
    pub roles: Vec<String>,
}

/// Point-in-time cluster state derived from event history.
#[derive(Debug, Clone)]
pub struct ClusterStateSnapshot {
    pub members: HashMap<String, MemberInfo>,
    pub leader: Option<String>,
}

impl ClusterStateSnapshot {
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    pub fn up_members(&self) -> Vec<&MemberInfo> {
        self.members.values()
            .filter(|m| matches!(m.status, MemberStatus::Up))
            .collect()
    }

    pub fn down_members(&self) -> Vec<&MemberInfo> {
        self.members.values()
            .filter(|m| matches!(m.status, MemberStatus::Down))
            .collect()
    }

    pub fn members_with_role(&self, role: &str) -> Vec<&MemberInfo> {
        self.members.values()
            .filter(|m| m.roles.contains(&role.to_string()))
            .collect()
    }
}

impl fmt::Display for ClusterStateSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Cluster State: {} members", self.members.len())?;
        if let Some(leader) = &self.leader {
            writeln!(f, "  Leader: {}", leader)?;
        }
        for (key, info) in &self.members {
            writeln!(f, "  {} [{:?}] roles={:?}", key, info.status, info.roles)?;
        }
        Ok(())
    }
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;
    use crate::addressing::UniqueAddress;

    fn make_member(host: &str, port: u16, status: MemberStatus) -> Member {
        Member {
            unique_address: UniqueAddress {
                address: RemoteAddress::tcp("test", host, port),
                uid: 0,
            },
            roles: vec!["worker".to_string()],
            status,
            uptime: 0,
        }
    }

    #[test]
    fn test_event_filter_all() {
        let filter = EventFilter::All;
        let event = ClusterEvent::MemberUp { member: make_member("127.0.0.1", 2551, MemberStatus::Up) };
        assert!(filter.matches(&event));
    }

    #[test]
    fn test_event_filter_member_events() {
        let filter = EventFilter::MemberEvents;
        let member_event = ClusterEvent::MemberUp { member: make_member("127.0.0.1", 2551, MemberStatus::Up) };
        let leader_event = ClusterEvent::LeaderChanged { leader: None };
        assert!(filter.matches(&member_event));
        assert!(!filter.matches(&leader_event));
    }

    #[test]
    fn test_event_filter_leader_events() {
        let filter = EventFilter::LeaderEvents;
        let leader_event = ClusterEvent::LeaderChanged { leader: None };
        let member_event = ClusterEvent::MemberUp { member: make_member("127.0.0.1", 2551, MemberStatus::Up) };
        assert!(filter.matches(&leader_event));
        assert!(!filter.matches(&member_event));
    }

    #[test]
    fn test_event_filter_custom() {
        let filter = EventFilter::Custom(vec![EventType::MemberUp, EventType::MemberDown]);
        let up = ClusterEvent::MemberUp { member: make_member("127.0.0.1", 2551, MemberStatus::Up) };
        let down = ClusterEvent::MemberDown { member: make_member("127.0.0.1", 2551, MemberStatus::Down) };
        let joined = ClusterEvent::MemberJoined { member: make_member("127.0.0.1", 2551, MemberStatus::Joining) };
        assert!(filter.matches(&up));
        assert!(filter.matches(&down));
        assert!(!filter.matches(&joined));
    }

    #[tokio::test]
    async fn test_event_bus_publish_subscribe() {
        let bus = ClusterEventBus::new(64);
        let mut sub = bus.subscribe(EventFilter::All, "test").await;

        let event = ClusterEvent::MemberUp { member: make_member("127.0.0.1", 2551, MemberStatus::Up) };
        bus.publish(event).await;

        let received = sub.try_recv();
        assert!(received.is_some());
    }

    #[tokio::test]
    async fn test_event_bus_filtered_subscribe() {
        let bus = ClusterEventBus::new(64);
        let mut sub = bus.subscribe(EventFilter::LeaderEvents, "leader-watcher").await;

        // Publish member event (should be filtered out)
        bus.publish(ClusterEvent::MemberUp {
            member: make_member("127.0.0.1", 2551, MemberStatus::Up)
        }).await;

        // Publish leader event (should pass filter)
        bus.publish(ClusterEvent::LeaderChanged { leader: None }).await;

        let received = sub.try_recv();
        assert!(received.is_some());
        assert!(matches!(received.unwrap(), ClusterEvent::LeaderChanged { .. }));
    }

    #[tokio::test]
    async fn test_event_bus_subscription_count() {
        let bus = ClusterEventBus::new(64);
        let sub1 = bus.subscribe(EventFilter::All, "s1").await;
        let _sub2 = bus.subscribe(EventFilter::All, "s2").await;
        assert_eq!(bus.subscription_count().await, 2);

        bus.unsubscribe(sub1.id).await;
        assert_eq!(bus.subscription_count().await, 1);
    }

    #[tokio::test]
    async fn test_event_bus_history() {
        let bus = ClusterEventBus::new(64);
        bus.publish(ClusterEvent::MemberUp {
            member: make_member("127.0.0.1", 2551, MemberStatus::Up)
        }).await;
        bus.publish(ClusterEvent::MemberDown {
            member: make_member("127.0.0.2", 2551, MemberStatus::Down)
        }).await;

        let history = bus.history().await;
        assert_eq!(history.len(), 2);
        assert_eq!(bus.events_published(), 2);
    }

    #[tokio::test]
    async fn test_event_bus_current_state() {
        let bus = ClusterEventBus::new(64);
        bus.publish(ClusterEvent::MemberUp {
            member: make_member("127.0.0.1", 2551, MemberStatus::Up)
        }).await;
        bus.publish(ClusterEvent::MemberUp {
            member: make_member("127.0.0.2", 2551, MemberStatus::Up)
        }).await;
        bus.publish(ClusterEvent::MemberDown {
            member: make_member("127.0.0.2", 2551, MemberStatus::Down)
        }).await;

        let snapshot = bus.current_state().await;
        assert_eq!(snapshot.member_count(), 2);
        assert_eq!(snapshot.up_members().len(), 1);
        assert_eq!(snapshot.down_members().len(), 1);
    }

    #[tokio::test]
    async fn test_cluster_state_snapshot_roles() {
        let bus = ClusterEventBus::new(64);
        bus.publish(ClusterEvent::MemberUp {
            member: make_member("127.0.0.1", 2551, MemberStatus::Up)
        }).await;

        let snapshot = bus.current_state().await;
        let with_role = snapshot.members_with_role("worker");
        assert_eq!(with_role.len(), 1);
    }

    #[tokio::test]
    async fn test_cluster_state_snapshot_display() {
        let bus = ClusterEventBus::new(64);
        bus.publish(ClusterEvent::MemberUp {
            member: make_member("127.0.0.1", 2551, MemberStatus::Up)
        }).await;

        let snapshot = bus.current_state().await;
        let display = format!("{}", snapshot);
        assert!(display.contains("1 members"));
    }
}

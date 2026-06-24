// cluster_aware_router.rs - Cluster-aware routing with membership-driven updates
// Routes messages to actors across cluster nodes with automatic member tracking

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::Instant;

// ============================================================================
// ClusterAwareRoutingStrategy
// ============================================================================

/// Routing strategy that considers cluster membership
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterAwareStrategy {
    /// Route to all members with a specific role
    RoleBased(String),
    /// Route round-robin across members
    RoundRobin,
    /// Route to member with least load (requires metrics)
    LeastLoad,
    /// Route based on consistent hashing of the message key
    ConsistentHash { virtual_nodes: u32 },
    /// Route to first available member
    FirstAvailable,
    /// Broadcast to all members
    Broadcast,
    /// Random member selection
    Random,
}

// ============================================================================
// ClusterMember (local representation for routing)
// ============================================================================

/// A cluster member for routing purposes
#[derive(Debug, Clone)]
pub struct RoutableMember {
    pub address: String,
    pub roles: HashSet<String>,
    pub status: MemberReachability,
    pub load: f64,
    pub last_seen: Instant,
}

/// Reachability status of a member
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberReachability {
    Reachable,
    Unreachable,
    Unknown,
}

impl RoutableMember {
    pub fn new(address: &str) -> Self {
        Self {
            address: address.to_string(),
            roles: HashSet::new(),
            status: MemberReachability::Reachable,
            load: 0.0,
            last_seen: Instant::now(),
        }
    }

    pub fn with_role(mut self, role: &str) -> Self {
        self.roles.insert(role.to_string());
        self
    }

    pub fn with_roles(mut self, roles: &[&str]) -> Self {
        for r in roles {
            self.roles.insert(r.to_string());
        }
        self
    }

    pub fn with_load(mut self, load: f64) -> Self {
        self.load = load;
        self
    }

    pub fn is_reachable(&self) -> bool {
        self.status == MemberReachability::Reachable
    }

    pub fn has_role(&self, role: &str) -> bool {
        self.roles.contains(role)
    }
}

// ============================================================================
// ClusterAwareRouter
// ============================================================================

/// A router that automatically tracks cluster membership and routes accordingly
pub struct ClusterAwareRouter {
    members: HashMap<String, RoutableMember>,
    strategy: ClusterAwareStrategy,
    round_robin_idx: usize,
    /// Only route to reachable members
    reachable_only: bool,
    /// Filter by role
    role_filter: Option<String>,
    stats: ClusterRouterStats,
}

/// Statistics for the cluster-aware router
#[derive(Debug, Clone, Default)]
pub struct ClusterRouterStats {
    pub total_routed: u64,
    pub total_broadcast: u64,
    pub no_target_count: u64,
    pub member_added: u64,
    pub member_removed: u64,
    pub member_unreachable: u64,
}

impl ClusterAwareRouter {
    pub fn new(strategy: ClusterAwareStrategy) -> Self {
        let role_filter = if let ClusterAwareStrategy::RoleBased(ref role) = strategy {
            Some(role.clone())
        } else {
            None
        };
        Self {
            members: HashMap::new(),
            strategy,
            round_robin_idx: 0,
            reachable_only: true,
            role_filter,
            stats: ClusterRouterStats::default(),
        }
    }

    pub fn with_role_filter(mut self, role: &str) -> Self {
        self.role_filter = Some(role.to_string());
        self
    }

    pub fn allow_unreachable(mut self) -> Self {
        self.reachable_only = false;
        self
    }

    /// Add or update a cluster member
    pub fn add_member(&mut self, member: RoutableMember) {
        self.stats.member_added += 1;
        self.members.insert(member.address.clone(), member);
    }

    /// Remove a cluster member
    pub fn remove_member(&mut self, address: &str) -> bool {
        let removed = self.members.remove(address).is_some();
        if removed {
            self.stats.member_removed += 1;
        }
        removed
    }

    /// Mark a member as unreachable
    pub fn mark_unreachable(&mut self, address: &str) {
        if let Some(m) = self.members.get_mut(address) {
            m.status = MemberReachability::Unreachable;
            self.stats.member_unreachable += 1;
        }
    }

    /// Mark a member as reachable again
    pub fn mark_reachable(&mut self, address: &str) {
        if let Some(m) = self.members.get_mut(address) {
            m.status = MemberReachability::Reachable;
            m.last_seen = Instant::now();
        }
    }

    /// Update member load metric
    pub fn update_load(&mut self, address: &str, load: f64) {
        if let Some(m) = self.members.get_mut(address) {
            m.load = load;
        }
    }

    /// Get eligible members based on filters
    fn eligible_members(&self) -> Vec<&RoutableMember> {
        self.members
            .values()
            .filter(|m| {
                if self.reachable_only && !m.is_reachable() {
                    return false;
                }
                if let Some(ref role) = self.role_filter {
                    if !m.has_role(role) {
                        return false;
                    }
                }
                true
            })
            .collect()
    }

    /// Route to a single target member address
    pub fn route(&mut self, _message_key: Option<&str>) -> Option<String> {
        self.stats.total_routed += 1;
        let eligible: Vec<RoutableMember> = self.eligible_members().into_iter().cloned().collect();

        if eligible.is_empty() {
            self.stats.no_target_count += 1;
            return None;
        }

        let strategy = self.strategy.clone();
        match &strategy {
            ClusterAwareStrategy::RoundRobin | ClusterAwareStrategy::RoleBased(_) => {
                let idx = self.round_robin_idx % eligible.len();
                self.round_robin_idx = self.round_robin_idx.wrapping_add(1);
                Some(eligible[idx].address.clone())
            }
            ClusterAwareStrategy::LeastLoad => {
                eligible
                    .iter()
                    .min_by(|a, b| a.load.partial_cmp(&b.load).unwrap_or(std::cmp::Ordering::Equal))
                    .map(|m| m.address.clone())
            }
            ClusterAwareStrategy::ConsistentHash { virtual_nodes } => {
                let key = _message_key.unwrap_or("default");
                let hash = Self::simple_hash(key);
                let idx = (hash % (*virtual_nodes).max(1)) as usize % eligible.len();
                Some(eligible[idx].address.clone())
            }
            ClusterAwareStrategy::FirstAvailable => {
                Some(eligible[0].address.clone())
            }
            ClusterAwareStrategy::Random => {
                // Deterministic "random" based on count
                let idx = (self.stats.total_routed as usize) % eligible.len();
                Some(eligible[idx].address.clone())
            }
            ClusterAwareStrategy::Broadcast => {
                // For broadcast, return first; use route_all for all targets
                Some(eligible[0].address.clone())
            }
        }
    }

    /// Route to all eligible targets (for broadcast)
    pub fn route_all(&mut self) -> Vec<String> {
        self.stats.total_broadcast += 1;
        self.eligible_members()
            .iter()
            .map(|m| m.address.clone())
            .collect()
    }

    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    pub fn reachable_count(&self) -> usize {
        self.members.values().filter(|m| m.is_reachable()).count()
    }

    pub fn get_member(&self, address: &str) -> Option<&RoutableMember> {
        self.members.get(address)
    }

    pub fn stats(&self) -> &ClusterRouterStats {
        &self.stats
    }

    fn simple_hash(s: &str) -> u32 {
        s.bytes().fold(0u32, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u32))
    }
}

impl fmt::Debug for ClusterAwareRouter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterAwareRouter")
            .field("strategy", &self.strategy)
            .field("members", &self.members.len())
            .finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_router(strategy: ClusterAwareStrategy) -> ClusterAwareRouter {
        let mut router = ClusterAwareRouter::new(strategy);
        router.add_member(RoutableMember::new("node-1").with_role("worker").with_load(0.3));
        router.add_member(RoutableMember::new("node-2").with_role("worker").with_load(0.7));
        router.add_member(RoutableMember::new("node-3").with_role("manager").with_load(0.1));
        router
    }

    #[test]
    fn test_round_robin() {
        let mut router = setup_router(ClusterAwareStrategy::RoundRobin);
        let r1 = router.route(None);
        let r2 = router.route(None);
        let r3 = router.route(None);
        assert!(r1.is_some());
        assert!(r2.is_some());
        assert!(r3.is_some());
    }

    #[test]
    fn test_round_robin_cycles() {
        let mut router = setup_router(ClusterAwareStrategy::RoundRobin);
        let r1 = router.route(None).unwrap();
        let _r2 = router.route(None);
        let _r3 = router.route(None);
        let r4 = router.route(None).unwrap();
        assert_eq!(r1, r4); // cycles back
    }

    #[test]
    fn test_least_load() {
        let mut router = setup_router(ClusterAwareStrategy::LeastLoad);
        let target = router.route(None).unwrap();
        assert_eq!(target, "node-3"); // load 0.1
    }

    #[test]
    fn test_first_available() {
        let mut router = setup_router(ClusterAwareStrategy::FirstAvailable);
        let target = router.route(None);
        assert!(target.is_some());
    }

    #[test]
    fn test_consistent_hash() {
        let mut router = setup_router(ClusterAwareStrategy::ConsistentHash { virtual_nodes: 100 });
        let r1 = router.route(Some("order-123"));
        let r2 = router.route(Some("order-123"));
        assert_eq!(r1, r2); // same key → same target
    }

    #[test]
    fn test_broadcast() {
        let mut router = setup_router(ClusterAwareStrategy::Broadcast);
        let all = router.route_all();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_role_based() {
        let mut router = setup_router(ClusterAwareStrategy::RoleBased("worker".to_string()));
        let target = router.route(None).unwrap();
        assert!(target == "node-1" || target == "node-2");
    }

    #[test]
    fn test_role_filter() {
        let mut router = setup_router(ClusterAwareStrategy::RoundRobin)
            .with_role_filter("manager");
        let target = router.route(None).unwrap();
        assert_eq!(target, "node-3");
    }

    #[test]
    fn test_unreachable_filtered() {
        let mut router = setup_router(ClusterAwareStrategy::RoundRobin);
        router.mark_unreachable("node-1");
        let eligible: Vec<_> = (0..10).filter_map(|_| router.route(None)).collect();
        assert!(eligible.iter().all(|t| t != "node-1"));
    }

    #[test]
    fn test_mark_reachable() {
        let mut router = setup_router(ClusterAwareStrategy::RoundRobin);
        router.mark_unreachable("node-1");
        router.mark_reachable("node-1");
        assert!(router.get_member("node-1").unwrap().is_reachable());
    }

    #[test]
    fn test_remove_member() {
        let mut router = setup_router(ClusterAwareStrategy::RoundRobin);
        assert!(router.remove_member("node-1"));
        assert_eq!(router.member_count(), 2);
    }

    #[test]
    fn test_no_eligible_members() {
        let mut router = ClusterAwareRouter::new(ClusterAwareStrategy::RoundRobin);
        assert!(router.route(None).is_none());
        assert_eq!(router.stats().no_target_count, 1);
    }

    #[test]
    fn test_update_load() {
        let mut router = setup_router(ClusterAwareStrategy::LeastLoad);
        router.update_load("node-2", 0.01);
        let target = router.route(None).unwrap();
        assert_eq!(target, "node-2"); // now lowest load
    }

    #[test]
    fn test_member_count() {
        let router = setup_router(ClusterAwareStrategy::RoundRobin);
        assert_eq!(router.member_count(), 3);
        assert_eq!(router.reachable_count(), 3);
    }

    #[test]
    fn test_stats() {
        let mut router = setup_router(ClusterAwareStrategy::RoundRobin);
        router.route(None);
        router.route(None);
        assert_eq!(router.stats().total_routed, 2);
        assert_eq!(router.stats().member_added, 3);
    }

    #[test]
    fn test_routable_member_roles() {
        let m = RoutableMember::new("n1").with_roles(&["worker", "manager"]);
        assert!(m.has_role("worker"));
        assert!(m.has_role("manager"));
        assert!(!m.has_role("admin"));
    }

    #[test]
    fn test_allow_unreachable() {
        let mut router = setup_router(ClusterAwareStrategy::RoundRobin).allow_unreachable();
        router.mark_unreachable("node-1");
        // Should still include unreachable
        let all = router.route_all();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_debug() {
        let router = setup_router(ClusterAwareStrategy::RoundRobin);
        let debug = format!("{:?}", router);
        assert!(debug.contains("ClusterAwareRouter"));
    }
}

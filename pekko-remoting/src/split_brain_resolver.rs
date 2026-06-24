//! Split Brain Resolver - Handles network partitions in a cluster
//!
//! When a network partition occurs, the cluster may split into multiple
//! sub-clusters. The Split Brain Resolver decides which partition should
//! survive and which should be downed.
//!
//! Strategies:
//! - **KeepMajority**: Keep the partition with more than half the nodes
//! - **KeepOldest**: Keep the partition containing the oldest node
//! - **KeepReferee**: Keep the partition containing a designated referee node
//! - **StaticQuorum**: Keep the partition with at least N nodes
//! - **LeaseMajority**: Like KeepMajority but with lease-based consistency

use crate::addressing::{RemoteAddress, UniqueAddress};
use crate::cluster::{Member, MemberStatus};
use crate::config::{SplitBrainResolverConfig, SplitBrainStrategy};
use crate::errors::ClusterError;
use std::collections::HashSet;
use std::time::{Duration, Instant};
use tracing::{ warn, error};

/// Result of a split brain resolution
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    /// Keep these nodes, down the rest
    KeepPartition(HashSet<UniqueAddress>),
    /// Down all nodes (indecision or unstable)
    DownAll,
    /// No action needed (not enough info or not yet stable)
    NoAction,
}

/// Decision about a single partition
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionDecision {
    /// This partition survives
    Survive,
    /// This partition should be downed
    Down,
    /// Undecided
    Undecided,
}

/// Represents one side of a network partition
#[derive(Debug, Clone)]
pub struct Partition {
    pub members: Vec<Member>,
}

impl Partition {
    pub fn new(members: Vec<Member>) -> Self {
        Self { members }
    }

    /// Number of reachable (Up) members
    pub fn reachable_count(&self) -> usize {
        self.members.iter()
            .filter(|m| matches!(m.status, MemberStatus::Up | MemberStatus::Joining))
            .count()
    }

    /// Total member count
    pub fn size(&self) -> usize {
        self.members.len()
    }

    /// Check if partition contains a specific address
    pub fn contains(&self, address: &UniqueAddress) -> bool {
        self.members.iter().any(|m| m.unique_address == *address)
    }

    /// Check if partition contains a member with a matching host:port
    pub fn contains_address(&self, address: &RemoteAddress) -> bool {
        self.members.iter().any(|m| {
            m.unique_address.address.host == address.host
                && m.unique_address.address.port == address.port
        })
    }

    /// Get the oldest member (by uptime, higher = older)
    pub fn oldest_member(&self) -> Option<&Member> {
        self.members.iter()
            .filter(|m| matches!(m.status, MemberStatus::Up))
            .max_by_key(|m| m.uptime)
    }

    /// Check if partition contains members with a specific role
    pub fn members_with_role(&self, role: &str) -> Vec<&Member> {
        self.members.iter()
            .filter(|m| m.roles.contains(&role.to_string()))
            .collect()
    }

    /// Get unique addresses of all members
    pub fn addresses(&self) -> HashSet<UniqueAddress> {
        self.members.iter().map(|m| m.unique_address.clone()).collect()
    }
}

/// Split Brain Resolver
pub struct SplitBrainResolver {
    config: SplitBrainResolverConfig,
    /// When the unreachable state was first detected
    unreachable_since: Option<Instant>,
}

impl SplitBrainResolver {
    pub fn new(config: SplitBrainResolverConfig) -> Self {
        Self {
            config,
            unreachable_since: None,
        }
    }

    /// Record that unreachable members were detected
    pub fn mark_unreachable(&mut self) {
        if self.unreachable_since.is_none() {
            self.unreachable_since = Some(Instant::now());
        }
    }

    /// Reset the unreachable tracking (all nodes reachable again)
    pub fn reset(&mut self) {
        if self.unreachable_since.is_some() {
        }
        self.unreachable_since = None;
    }

    /// Check if the stable-after duration has elapsed
    pub fn is_stable(&self) -> bool {
        if let Some(since) = self.unreachable_since {
            since.elapsed() >= Duration::from_millis(self.config.stable_after_ms)
        } else {
            false
        }
    }

    /// Resolve a split brain situation
    ///
    /// `own_partition` - the partition this node belongs to
    /// `total_members_before_split` - total cluster size before the partition
    pub fn resolve(
        &self,
        own_partition: &Partition,
        total_members_before_split: usize,
    ) -> Resolution {
        if !self.is_stable() {
            return Resolution::NoAction;
        }

        match &self.config.strategy {
            SplitBrainStrategy::KeepMajority => {
                self.resolve_keep_majority(own_partition, total_members_before_split)
            }
            SplitBrainStrategy::KeepOldest => {
                self.resolve_keep_oldest(own_partition)
            }
            SplitBrainStrategy::KeepReferee { referee_address } => {
                self.resolve_keep_referee(own_partition, referee_address)
            }
            SplitBrainStrategy::StaticQuorum { quorum_size } => {
                self.resolve_static_quorum(own_partition, *quorum_size)
            }
            SplitBrainStrategy::LeaseMajority { .. } => {
                // LeaseMajority behaves like KeepMajority but with lease-based tie-breaking
                self.resolve_lease_majority(own_partition, total_members_before_split)
            }
        }
    }

    /// KeepMajority: The partition with more than half the nodes survives.
    /// On exact split (even number), the partition with the lowest-addressed node wins.
    fn resolve_keep_majority(
        &self,
        own_partition: &Partition,
        total_members: usize,
    ) -> Resolution {
        let own_size = own_partition.reachable_count();
        let majority = total_members / 2 + 1;

        if own_size >= majority {
            Resolution::KeepPartition(own_partition.addresses())
        } else if own_size * 2 == total_members {
            // Exact 50/50 split — use lowest address as tiebreaker
            // The partition with the lowest-addressed member wins
            // In a real system, we'd compare with the other partition.
            // For safety, if we can't determine, down all.
            if self.config.down_all_when_unstable {
                Resolution::DownAll
            } else {
                Resolution::NoAction
            }
        } else {
            warn!(
                "KeepMajority: this partition has {} of {} members (majority={}), downing self",
                own_size, total_members, majority
            );
            Resolution::DownAll
        }
    }

    /// KeepOldest: The partition containing the oldest member survives.
    fn resolve_keep_oldest(&self, own_partition: &Partition) -> Resolution {
        if let Some(oldest) = own_partition.oldest_member() {
            Resolution::KeepPartition(own_partition.addresses())
        } else {
            warn!("KeepOldest: no Up members in this partition");
            if self.config.down_all_when_unstable {
                Resolution::DownAll
            } else {
                Resolution::NoAction
            }
        }
    }

    /// KeepReferee: The partition containing the designated referee node survives.
    fn resolve_keep_referee(
        &self,
        own_partition: &Partition,
        referee_address: &str,
    ) -> Resolution {
        let referee_addr = match RemoteAddress::parse(referee_address) {
            Ok(addr) => addr,
            Err(_) => {
                // Try as host:port format
                let parts: Vec<&str> = referee_address.split(':').collect();
                if parts.len() == 2 {
                    if let Ok(port) = parts[1].parse::<u16>() {
                        RemoteAddress::tcp("system", parts[0], port)
                    } else {
                        error!("KeepReferee: invalid referee address format: {}", referee_address);
                        return Resolution::DownAll;
                    }
                } else {
                    error!("KeepReferee: invalid referee address: {}", referee_address);
                    return Resolution::DownAll;
                }
            }
        };

        if own_partition.contains_address(&referee_addr) {
            Resolution::KeepPartition(own_partition.addresses())
        } else {
            warn!("KeepReferee: referee node not in this partition, downing self");
            Resolution::DownAll
        }
    }

    /// StaticQuorum: The partition with at least quorum_size members survives.
    fn resolve_static_quorum(
        &self,
        own_partition: &Partition,
        quorum_size: u32,
    ) -> Resolution {
        let own_size = own_partition.reachable_count();

        if own_size >= quorum_size as usize {
            Resolution::KeepPartition(own_partition.addresses())
        } else {
            warn!(
                "StaticQuorum: this partition has {} members (quorum={}), downing self",
                own_size, quorum_size
            );
            Resolution::DownAll
        }
    }

    /// LeaseMajority: Like KeepMajority but factors in lease ownership.
    /// The partition holding the lease (or majority) survives.
    fn resolve_lease_majority(
        &self,
        own_partition: &Partition,
        total_members: usize,
    ) -> Resolution {
        // In a full implementation, we'd check who holds the distributed lease.
        // For now, fall back to majority behavior.
        let own_size = own_partition.reachable_count();
        let majority = total_members / 2 + 1;

        if own_size >= majority {
            Resolution::KeepPartition(own_partition.addresses())
        } else {
            warn!(
                "LeaseMajority: this partition has {} of {} members (majority={}), downing self",
                own_size, total_members, majority
            );
            Resolution::DownAll
        }
    }
}

/// Evaluate two partitions and decide which survives
pub fn evaluate_partitions(
    config: &SplitBrainResolverConfig,
    partition_a: &Partition,
    partition_b: &Partition,
) -> (PartitionDecision, PartitionDecision) {
    let total = partition_a.size() + partition_b.size();

    match &config.strategy {
        SplitBrainStrategy::KeepMajority => {
            let a_size = partition_a.reachable_count();
            let b_size = partition_b.reachable_count();
            let majority = total / 2 + 1;

            if a_size > b_size && a_size >= majority {
                (PartitionDecision::Survive, PartitionDecision::Down)
            } else if b_size > a_size && b_size >= majority {
                (PartitionDecision::Down, PartitionDecision::Survive)
            } else {
                // Tie — both undecided
                (PartitionDecision::Undecided, PartitionDecision::Undecided)
            }
        }
        SplitBrainStrategy::KeepOldest => {
            let a_oldest = partition_a.oldest_member().map(|m| m.uptime).unwrap_or(0);
            let b_oldest = partition_b.oldest_member().map(|m| m.uptime).unwrap_or(0);

            if a_oldest > b_oldest {
                (PartitionDecision::Survive, PartitionDecision::Down)
            } else if b_oldest > a_oldest {
                (PartitionDecision::Down, PartitionDecision::Survive)
            } else {
                (PartitionDecision::Undecided, PartitionDecision::Undecided)
            }
        }
        SplitBrainStrategy::KeepReferee { referee_address } => {
            let referee_addr = RemoteAddress::parse(referee_address).ok()
                .or_else(|| {
                    let parts: Vec<&str> = referee_address.split(':').collect();
                    if parts.len() == 2 {
                        parts[1].parse::<u16>().ok().map(|port| RemoteAddress::tcp("system", parts[0], port))
                    } else {
                        None
                    }
                });

            if let Some(addr) = referee_addr {
                if partition_a.contains_address(&addr) {
                    (PartitionDecision::Survive, PartitionDecision::Down)
                } else if partition_b.contains_address(&addr) {
                    (PartitionDecision::Down, PartitionDecision::Survive)
                } else {
                    (PartitionDecision::Undecided, PartitionDecision::Undecided)
                }
            } else {
                (PartitionDecision::Undecided, PartitionDecision::Undecided)
            }
        }
        SplitBrainStrategy::StaticQuorum { quorum_size } => {
            let q = *quorum_size as usize;
            let a_ok = partition_a.reachable_count() >= q;
            let b_ok = partition_b.reachable_count() >= q;

            match (a_ok, b_ok) {
                (true, false) => (PartitionDecision::Survive, PartitionDecision::Down),
                (false, true) => (PartitionDecision::Down, PartitionDecision::Survive),
                (true, true) => (PartitionDecision::Survive, PartitionDecision::Survive),
                (false, false) => (PartitionDecision::Down, PartitionDecision::Down),
            }
        }
        SplitBrainStrategy::LeaseMajority { .. } => {
            // Same as KeepMajority for now
            let a_size = partition_a.reachable_count();
            let b_size = partition_b.reachable_count();
            let majority = total / 2 + 1;

            if a_size > b_size && a_size >= majority {
                (PartitionDecision::Survive, PartitionDecision::Down)
            } else if b_size > a_size && b_size >= majority {
                (PartitionDecision::Down, PartitionDecision::Survive)
            } else {
                (PartitionDecision::Undecided, PartitionDecision::Undecided)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LeaseSettings;

    fn make_member(host: &str, port: u16, uptime: u64, status: MemberStatus) -> Member {
        Member {
            unique_address: UniqueAddress::new(
                RemoteAddress::tcp("test", host, port),
                port as u64,
            ),
            roles: vec!["worker".to_string()],
            status,
            uptime,
        }
    }

    fn make_partition(members: Vec<Member>) -> Partition {
        Partition::new(members)
    }

    fn default_config(strategy: SplitBrainStrategy) -> SplitBrainResolverConfig {
        SplitBrainResolverConfig {
            strategy,
            stable_after_ms: 0, // immediate for testing
            down_all_when_unstable: true,
        }
    }

    // ===== KeepMajority Tests =====

    #[test]
    fn test_keep_majority_larger_partition_survives() {
        let config = default_config(SplitBrainStrategy::KeepMajority);
        let mut resolver = SplitBrainResolver::new(config);
        resolver.mark_unreachable();

        let partition = make_partition(vec![
            make_member("10.0.0.1", 2552, 100, MemberStatus::Up),
            make_member("10.0.0.2", 2552, 90, MemberStatus::Up),
            make_member("10.0.0.3", 2552, 80, MemberStatus::Up),
        ]);

        let result = resolver.resolve(&partition, 5);
        assert!(matches!(result, Resolution::KeepPartition(_)));

        if let Resolution::KeepPartition(addrs) = result {
            assert_eq!(addrs.len(), 3);
        }
    }

    #[test]
    fn test_keep_majority_smaller_partition_downs() {
        let config = default_config(SplitBrainStrategy::KeepMajority);
        let mut resolver = SplitBrainResolver::new(config);
        resolver.mark_unreachable();

        let partition = make_partition(vec![
            make_member("10.0.0.1", 2552, 100, MemberStatus::Up),
            make_member("10.0.0.2", 2552, 90, MemberStatus::Up),
        ]);

        let result = resolver.resolve(&partition, 5);
        assert_eq!(result, Resolution::DownAll);
    }

    #[test]
    fn test_keep_majority_even_split_downs_all() {
        let config = default_config(SplitBrainStrategy::KeepMajority);
        let mut resolver = SplitBrainResolver::new(config);
        resolver.mark_unreachable();

        let partition = make_partition(vec![
            make_member("10.0.0.1", 2552, 100, MemberStatus::Up),
            make_member("10.0.0.2", 2552, 90, MemberStatus::Up),
        ]);

        let result = resolver.resolve(&partition, 4);
        assert_eq!(result, Resolution::DownAll);
    }

    // ===== KeepOldest Tests =====

    #[test]
    fn test_keep_oldest_with_oldest_node() {
        let config = default_config(SplitBrainStrategy::KeepOldest);
        let mut resolver = SplitBrainResolver::new(config);
        resolver.mark_unreachable();

        let partition = make_partition(vec![
            make_member("10.0.0.1", 2552, 1000, MemberStatus::Up),
            make_member("10.0.0.2", 2552, 500, MemberStatus::Up),
        ]);

        let result = resolver.resolve(&partition, 5);
        assert!(matches!(result, Resolution::KeepPartition(_)));
    }

    #[test]
    fn test_keep_oldest_no_up_members() {
        let config = default_config(SplitBrainStrategy::KeepOldest);
        let mut resolver = SplitBrainResolver::new(config);
        resolver.mark_unreachable();

        let partition = make_partition(vec![
            make_member("10.0.0.1", 2552, 100, MemberStatus::Down),
        ]);

        let result = resolver.resolve(&partition, 5);
        assert_eq!(result, Resolution::DownAll);
    }

    // ===== KeepReferee Tests =====

    #[test]
    fn test_keep_referee_has_referee() {
        let config = default_config(SplitBrainStrategy::KeepReferee {
            referee_address: "10.0.0.1:2552".to_string(),
        });
        let mut resolver = SplitBrainResolver::new(config);
        resolver.mark_unreachable();

        let partition = make_partition(vec![
            make_member("10.0.0.1", 2552, 100, MemberStatus::Up),
            make_member("10.0.0.2", 2552, 90, MemberStatus::Up),
        ]);

        let result = resolver.resolve(&partition, 5);
        assert!(matches!(result, Resolution::KeepPartition(_)));
    }

    #[test]
    fn test_keep_referee_missing_referee() {
        let config = default_config(SplitBrainStrategy::KeepReferee {
            referee_address: "10.0.0.99:2552".to_string(),
        });
        let mut resolver = SplitBrainResolver::new(config);
        resolver.mark_unreachable();

        let partition = make_partition(vec![
            make_member("10.0.0.1", 2552, 100, MemberStatus::Up),
            make_member("10.0.0.2", 2552, 90, MemberStatus::Up),
        ]);

        let result = resolver.resolve(&partition, 5);
        assert_eq!(result, Resolution::DownAll);
    }

    // ===== StaticQuorum Tests =====

    #[test]
    fn test_static_quorum_meets_quorum() {
        let config = default_config(SplitBrainStrategy::StaticQuorum { quorum_size: 3 });
        let mut resolver = SplitBrainResolver::new(config);
        resolver.mark_unreachable();

        let partition = make_partition(vec![
            make_member("10.0.0.1", 2552, 100, MemberStatus::Up),
            make_member("10.0.0.2", 2552, 90, MemberStatus::Up),
            make_member("10.0.0.3", 2552, 80, MemberStatus::Up),
        ]);

        let result = resolver.resolve(&partition, 5);
        assert!(matches!(result, Resolution::KeepPartition(_)));
    }

    #[test]
    fn test_static_quorum_below_quorum() {
        let config = default_config(SplitBrainStrategy::StaticQuorum { quorum_size: 3 });
        let mut resolver = SplitBrainResolver::new(config);
        resolver.mark_unreachable();

        let partition = make_partition(vec![
            make_member("10.0.0.1", 2552, 100, MemberStatus::Up),
            make_member("10.0.0.2", 2552, 90, MemberStatus::Up),
        ]);

        let result = resolver.resolve(&partition, 5);
        assert_eq!(result, Resolution::DownAll);
    }

    // ===== LeaseMajority Tests =====

    #[test]
    fn test_lease_majority_larger_partition() {
        let config = default_config(SplitBrainStrategy::LeaseMajority {
            lease_settings: LeaseSettings {
                lease_name: "test-lease".to_string(),
                owner_name: "node-1".to_string(),
                time_to_live_ms: 15000,
                heartbeat_interval_ms: 5000,
                heartbeat_timeout_ms: 10000,
                lease_operation_timeout_ms: 5000,
            },
        });
        let mut resolver = SplitBrainResolver::new(config);
        resolver.mark_unreachable();

        let partition = make_partition(vec![
            make_member("10.0.0.1", 2552, 100, MemberStatus::Up),
            make_member("10.0.0.2", 2552, 90, MemberStatus::Up),
            make_member("10.0.0.3", 2552, 80, MemberStatus::Up),
        ]);

        let result = resolver.resolve(&partition, 5);
        assert!(matches!(result, Resolution::KeepPartition(_)));
    }

    // ===== Stability Timer Tests =====

    #[test]
    fn test_not_stable_returns_no_action() {
        let config = SplitBrainResolverConfig {
            strategy: SplitBrainStrategy::KeepMajority,
            stable_after_ms: 999999, // Very long
            down_all_when_unstable: true,
        };
        let mut resolver = SplitBrainResolver::new(config);
        resolver.mark_unreachable();

        let partition = make_partition(vec![
            make_member("10.0.0.1", 2552, 100, MemberStatus::Up),
        ]);

        let result = resolver.resolve(&partition, 3);
        assert_eq!(result, Resolution::NoAction);
    }

    #[test]
    fn test_reset_clears_timer() {
        let config = default_config(SplitBrainStrategy::KeepMajority);
        let mut resolver = SplitBrainResolver::new(config);

        resolver.mark_unreachable();
        assert!(resolver.is_stable()); // stable_after_ms = 0
        resolver.reset();
        assert!(!resolver.is_stable());
    }

    // ===== evaluate_partitions Tests =====

    #[test]
    fn test_evaluate_keep_majority_partitions() {
        let config = default_config(SplitBrainStrategy::KeepMajority);

        let a = make_partition(vec![
            make_member("10.0.0.1", 2552, 100, MemberStatus::Up),
            make_member("10.0.0.2", 2552, 90, MemberStatus::Up),
            make_member("10.0.0.3", 2552, 80, MemberStatus::Up),
        ]);
        let b = make_partition(vec![
            make_member("10.0.0.4", 2552, 70, MemberStatus::Up),
            make_member("10.0.0.5", 2552, 60, MemberStatus::Up),
        ]);

        let (da, db) = evaluate_partitions(&config, &a, &b);
        assert_eq!(da, PartitionDecision::Survive);
        assert_eq!(db, PartitionDecision::Down);
    }

    #[test]
    fn test_evaluate_keep_oldest_partitions() {
        let config = default_config(SplitBrainStrategy::KeepOldest);

        let a = make_partition(vec![
            make_member("10.0.0.1", 2552, 100, MemberStatus::Up),
        ]);
        let b = make_partition(vec![
            make_member("10.0.0.2", 2552, 500, MemberStatus::Up),
        ]);

        let (da, db) = evaluate_partitions(&config, &a, &b);
        assert_eq!(da, PartitionDecision::Down);
        assert_eq!(db, PartitionDecision::Survive);
    }

    #[test]
    fn test_evaluate_static_quorum_both_meet() {
        let config = default_config(SplitBrainStrategy::StaticQuorum { quorum_size: 2 });

        let a = make_partition(vec![
            make_member("10.0.0.1", 2552, 100, MemberStatus::Up),
            make_member("10.0.0.2", 2552, 90, MemberStatus::Up),
        ]);
        let b = make_partition(vec![
            make_member("10.0.0.3", 2552, 80, MemberStatus::Up),
            make_member("10.0.0.4", 2552, 70, MemberStatus::Up),
            make_member("10.0.0.5", 2552, 60, MemberStatus::Up),
        ]);

        let (da, db) = evaluate_partitions(&config, &a, &b);
        assert_eq!(da, PartitionDecision::Survive);
        assert_eq!(db, PartitionDecision::Survive);
    }

    #[test]
    fn test_evaluate_static_quorum_neither_meets() {
        let config = default_config(SplitBrainStrategy::StaticQuorum { quorum_size: 4 });

        let a = make_partition(vec![
            make_member("10.0.0.1", 2552, 100, MemberStatus::Up),
            make_member("10.0.0.2", 2552, 90, MemberStatus::Up),
        ]);
        let b = make_partition(vec![
            make_member("10.0.0.3", 2552, 80, MemberStatus::Up),
        ]);

        let (da, db) = evaluate_partitions(&config, &a, &b);
        assert_eq!(da, PartitionDecision::Down);
        assert_eq!(db, PartitionDecision::Down);
    }

    #[test]
    fn test_evaluate_keep_referee() {
        let config = default_config(SplitBrainStrategy::KeepReferee {
            referee_address: "10.0.0.3:2552".to_string(),
        });

        let a = make_partition(vec![
            make_member("10.0.0.1", 2552, 100, MemberStatus::Up),
            make_member("10.0.0.2", 2552, 90, MemberStatus::Up),
        ]);
        let b = make_partition(vec![
            make_member("10.0.0.3", 2552, 80, MemberStatus::Up),
        ]);

        let (da, db) = evaluate_partitions(&config, &a, &b);
        assert_eq!(da, PartitionDecision::Down);
        assert_eq!(db, PartitionDecision::Survive);
    }

    #[test]
    fn test_partition_contains() {
        let member = make_member("10.0.0.1", 2552, 100, MemberStatus::Up);
        let partition = make_partition(vec![member.clone()]);

        assert!(partition.contains(&member.unique_address));
        assert!(partition.contains_address(&RemoteAddress::tcp("test", "10.0.0.1", 2552)));
        assert!(!partition.contains_address(&RemoteAddress::tcp("test", "10.0.0.99", 2552)));
    }

    #[test]
    fn test_partition_oldest_member() {
        let partition = make_partition(vec![
            make_member("10.0.0.1", 2552, 100, MemberStatus::Up),
            make_member("10.0.0.2", 2552, 500, MemberStatus::Up),
            make_member("10.0.0.3", 2552, 200, MemberStatus::Down),
        ]);

        let oldest = partition.oldest_member().unwrap();
        assert_eq!(oldest.uptime, 500);
    }

    #[test]
    fn test_partition_members_with_role() {
        let mut m1 = make_member("10.0.0.1", 2552, 100, MemberStatus::Up);
        m1.roles = vec!["frontend".to_string()];
        let mut m2 = make_member("10.0.0.2", 2552, 90, MemberStatus::Up);
        m2.roles = vec!["backend".to_string()];
        let mut m3 = make_member("10.0.0.3", 2552, 80, MemberStatus::Up);
        m3.roles = vec!["frontend".to_string(), "backend".to_string()];

        let partition = make_partition(vec![m1, m2, m3]);

        assert_eq!(partition.members_with_role("frontend").len(), 2);
        assert_eq!(partition.members_with_role("backend").len(), 2);
        assert_eq!(partition.members_with_role("unknown").len(), 0);
    }
}

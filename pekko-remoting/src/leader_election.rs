//! # Leader Election
//!
//! Explicit leader election coordination for cluster operations.
//!
//! Provides:
//! - Lease-based leadership with configurable timeout
//! - Multiple candidate tracking and priority
//! - Leadership change notifications
//! - Re-election on leader failure

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Candidate identifier
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct CandidateId(String);

impl CandidateId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CandidateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Election term — monotonically increasing
pub type Term = u64;

/// Priority for tiebreaking (lower number = higher priority)
pub type Priority = u32;

/// Candidate status
#[derive(Debug, Clone, PartialEq)]
pub enum CandidateStatus {
    Follower,
    Candidate,
    Leader,
}

/// Leader election event
#[derive(Debug, Clone)]
pub enum LeaderEvent {
    /// A new leader was elected
    LeaderElected {
        leader: CandidateId,
        term: Term,
    },
    /// The leader stepped down or lease expired
    LeaderLost {
        former_leader: CandidateId,
        term: Term,
    },
    /// A candidate joined the election
    CandidateJoined(CandidateId),
    /// A candidate left the election
    CandidateLeft(CandidateId),
    /// Election started
    ElectionStarted { term: Term },
}

/// Lease for leadership
#[derive(Debug, Clone)]
pub struct Lease {
    pub holder: CandidateId,
    pub term: Term,
    pub granted_at: Instant,
    pub duration: Duration,
}

impl Lease {
    pub fn is_expired(&self) -> bool {
        self.granted_at.elapsed() > self.duration
    }

    pub fn remaining(&self) -> Duration {
        self.duration.saturating_sub(self.granted_at.elapsed())
    }

    pub fn is_held_by(&self, candidate: &CandidateId) -> bool {
        &self.holder == candidate && !self.is_expired()
    }
}

/// Candidate registration
#[derive(Debug, Clone)]
pub struct CandidateInfo {
    pub id: CandidateId,
    pub priority: Priority,
    pub status: CandidateStatus,
    pub registered_at: Instant,
    pub last_heartbeat: Instant,
}

/// Election result
#[derive(Debug, Clone, PartialEq)]
pub enum ElectionResult {
    /// A leader was elected
    Elected(CandidateId),
    /// No candidates available
    NoCandidates,
    /// Election already in progress
    AlreadyInProgress,
    /// Tie between candidates
    Tie(Vec<CandidateId>),
}

/// Election configuration
#[derive(Debug, Clone)]
pub struct ElectionConfig {
    /// Default lease duration
    pub lease_duration: Duration,
    /// Time after which a candidate is considered dead
    pub heartbeat_timeout: Duration,
    /// Whether to auto re-elect when lease expires
    pub auto_reelect: bool,
}

impl Default for ElectionConfig {
    fn default() -> Self {
        Self {
            lease_duration: Duration::from_secs(15),
            heartbeat_timeout: Duration::from_secs(10),
            auto_reelect: true,
        }
    }
}

/// Election stats
#[derive(Debug, Clone)]
pub struct ElectionStats {
    pub current_term: Term,
    pub total_elections: u64,
    pub total_candidates: usize,
    pub active_candidates: usize,
    pub current_leader: Option<CandidateId>,
    pub leader_changes: u64,
}

// ---------------------------------------------------------------------------
// LeaderElectionManager
// ---------------------------------------------------------------------------

/// Manages leader election across candidates
pub struct LeaderElectionManager {
    config: ElectionConfig,
    candidates: BTreeMap<CandidateId, CandidateInfo>,
    current_lease: Option<Lease>,
    current_term: Term,
    total_elections: u64,
    leader_changes: u64,
    event_senders: Vec<mpsc::UnboundedSender<LeaderEvent>>,
}

impl LeaderElectionManager {
    pub fn new(config: ElectionConfig) -> Self {
        Self {
            config,
            candidates: BTreeMap::new(),
            current_lease: None,
            current_term: 0,
            total_elections: 0,
            leader_changes: 0,
            event_senders: Vec::new(),
        }
    }

    pub fn with_default_config() -> Self {
        Self::new(ElectionConfig::default())
    }

    /// Register a candidate for election
    pub fn register_candidate(&mut self, id: CandidateId, priority: Priority) -> bool {
        if self.candidates.contains_key(&id) {
            return false;
        }

        let info = CandidateInfo {
            id: id.clone(),
            priority,
            status: CandidateStatus::Follower,
            registered_at: Instant::now(),
            last_heartbeat: Instant::now(),
        };

        self.candidates.insert(id.clone(), info);
        self.emit_event(LeaderEvent::CandidateJoined(id));
        true
    }

    /// Unregister a candidate
    pub fn unregister_candidate(&mut self, id: &CandidateId) -> bool {
        if self.candidates.remove(id).is_some() {
            // If the leader left, clear the lease
            if let Some(ref lease) = self.current_lease {
                if &lease.holder == id {
                    let term = lease.term;
                    let former = id.clone();
                    self.current_lease = None;
                    self.emit_event(LeaderEvent::LeaderLost { former_leader: former, term });
                }
            }
            self.emit_event(LeaderEvent::CandidateLeft(id.clone()));
            true
        } else {
            false
        }
    }

    /// Record a heartbeat from a candidate
    pub fn heartbeat(&mut self, id: &CandidateId) -> bool {
        if let Some(c) = self.candidates.get_mut(id) {
            c.last_heartbeat = Instant::now();
            true
        } else {
            false
        }
    }

    /// Run an election
    pub fn elect(&mut self) -> ElectionResult {
        // Remove dead candidates
        let timeout = self.config.heartbeat_timeout;
        self.candidates.retain(|_, c| c.last_heartbeat.elapsed() < timeout);

        if self.candidates.is_empty() {
            return ElectionResult::NoCandidates;
        }

        self.current_term += 1;
        self.total_elections += 1;
        let term = self.current_term;

        self.emit_event(LeaderEvent::ElectionStarted { term });

        // Find the candidate with highest priority (lowest number)
        // Clone IDs to avoid borrowing self.candidates while calling grant_lease
        let mut sorted: Vec<(CandidateId, Priority)> = self.candidates.values()
            .map(|c| (c.id.clone(), c.priority))
            .collect();
        sorted.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

        let winner_id = sorted[0].0.clone();
        self.grant_lease(winner_id.clone(), term);
        ElectionResult::Elected(winner_id)
    }

    /// Check if current leader's lease is still valid
    pub fn is_leader_active(&self) -> bool {
        self.current_lease.as_ref().map_or(false, |l| !l.is_expired())
    }

    /// Get current leader
    pub fn current_leader(&self) -> Option<&CandidateId> {
        self.current_lease.as_ref()
            .filter(|l| !l.is_expired())
            .map(|l| &l.holder)
    }

    /// Get current lease
    pub fn current_lease(&self) -> Option<&Lease> {
        self.current_lease.as_ref()
    }

    /// Check if a specific candidate is the leader
    pub fn is_leader(&self, id: &CandidateId) -> bool {
        self.current_lease.as_ref().map_or(false, |l| l.is_held_by(id))
    }

    /// Step down as leader
    pub fn step_down(&mut self, id: &CandidateId) -> bool {
        if let Some(ref lease) = self.current_lease {
            if &lease.holder == id {
                let term = lease.term;
                let former = id.clone();
                self.current_lease = None;
                if let Some(c) = self.candidates.get_mut(id) {
                    c.status = CandidateStatus::Follower;
                }
                self.emit_event(LeaderEvent::LeaderLost { former_leader: former, term });
                return true;
            }
        }
        false
    }

    /// Renew the leader's lease
    pub fn renew_lease(&mut self, id: &CandidateId) -> bool {
        if let Some(ref mut lease) = self.current_lease {
            if &lease.holder == id && !lease.is_expired() {
                lease.granted_at = Instant::now();
                return true;
            }
        }
        false
    }

    /// Get all candidates
    pub fn candidates(&self) -> Vec<&CandidateInfo> {
        self.candidates.values().collect()
    }

    /// Get stats
    pub fn stats(&self) -> ElectionStats {
        let active = self.candidates.values()
            .filter(|c| c.last_heartbeat.elapsed() < self.config.heartbeat_timeout)
            .count();

        ElectionStats {
            current_term: self.current_term,
            total_elections: self.total_elections,
            total_candidates: self.candidates.len(),
            active_candidates: active,
            current_leader: self.current_leader().cloned(),
            leader_changes: self.leader_changes,
        }
    }

    /// Subscribe to leader events
    pub fn subscribe(&mut self) -> mpsc::UnboundedReceiver<LeaderEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.event_senders.push(tx);
        rx
    }

    fn grant_lease(&mut self, candidate: CandidateId, term: Term) {
        // Update old leader status
        if let Some(ref lease) = self.current_lease {
            if let Some(c) = self.candidates.get_mut(&lease.holder) {
                c.status = CandidateStatus::Follower;
            }
        }

        // Grant new lease
        let lease = Lease {
            holder: candidate.clone(),
            term,
            granted_at: Instant::now(),
            duration: self.config.lease_duration,
        };

        self.current_lease = Some(lease);
        self.leader_changes += 1;

        if let Some(c) = self.candidates.get_mut(&candidate) {
            c.status = CandidateStatus::Leader;
        }

        self.emit_event(LeaderEvent::LeaderElected { leader: candidate, term });
    }

    fn emit_event(&mut self, event: LeaderEvent) {
        self.event_senders.retain(|tx| tx.send(event.clone()).is_ok());
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_manager() -> LeaderElectionManager {
        LeaderElectionManager::with_default_config()
    }

    #[test]
    fn test_register_candidate() {
        let mut mgr = make_manager();
        assert!(mgr.register_candidate(CandidateId::new("c1"), 1));
        assert_eq!(mgr.candidates().len(), 1);
    }

    #[test]
    fn test_register_duplicate() {
        let mut mgr = make_manager();
        assert!(mgr.register_candidate(CandidateId::new("c1"), 1));
        assert!(!mgr.register_candidate(CandidateId::new("c1"), 1));
    }

    #[test]
    fn test_elect_single_candidate() {
        let mut mgr = make_manager();
        mgr.register_candidate(CandidateId::new("c1"), 1);

        let result = mgr.elect();
        assert_eq!(result, ElectionResult::Elected(CandidateId::new("c1")));
        assert!(mgr.is_leader(&CandidateId::new("c1")));
    }

    #[test]
    fn test_elect_by_priority() {
        let mut mgr = make_manager();
        mgr.register_candidate(CandidateId::new("low"), 10);
        mgr.register_candidate(CandidateId::new("high"), 1);
        mgr.register_candidate(CandidateId::new("mid"), 5);

        let result = mgr.elect();
        assert_eq!(result, ElectionResult::Elected(CandidateId::new("high")));
    }

    #[test]
    fn test_elect_no_candidates() {
        let mut mgr = make_manager();
        assert_eq!(mgr.elect(), ElectionResult::NoCandidates);
    }

    #[test]
    fn test_step_down() {
        let mut mgr = make_manager();
        mgr.register_candidate(CandidateId::new("c1"), 1);
        mgr.elect();

        assert!(mgr.step_down(&CandidateId::new("c1")));
        assert!(!mgr.is_leader(&CandidateId::new("c1")));
        assert!(mgr.current_leader().is_none());
    }

    #[test]
    fn test_step_down_non_leader() {
        let mut mgr = make_manager();
        mgr.register_candidate(CandidateId::new("c1"), 1);
        mgr.register_candidate(CandidateId::new("c2"), 2);
        mgr.elect();

        assert!(!mgr.step_down(&CandidateId::new("c2")));
    }

    #[test]
    fn test_unregister_leader() {
        let mut mgr = make_manager();
        mgr.register_candidate(CandidateId::new("c1"), 1);
        mgr.elect();

        mgr.unregister_candidate(&CandidateId::new("c1"));
        assert!(mgr.current_leader().is_none());
    }

    #[test]
    fn test_renew_lease() {
        let mut mgr = make_manager();
        mgr.register_candidate(CandidateId::new("c1"), 1);
        mgr.elect();

        assert!(mgr.renew_lease(&CandidateId::new("c1")));
        assert!(mgr.is_leader_active());
    }

    #[test]
    fn test_renew_non_leader() {
        let mut mgr = make_manager();
        mgr.register_candidate(CandidateId::new("c1"), 1);
        mgr.register_candidate(CandidateId::new("c2"), 2);
        mgr.elect();

        assert!(!mgr.renew_lease(&CandidateId::new("c2")));
    }

    #[test]
    fn test_lease_expiry() {
        let config = ElectionConfig {
            lease_duration: Duration::from_millis(1),
            ..Default::default()
        };
        let mut mgr = LeaderElectionManager::new(config);
        mgr.register_candidate(CandidateId::new("c1"), 1);
        mgr.elect();

        std::thread::sleep(Duration::from_millis(5));

        assert!(!mgr.is_leader_active());
        assert!(mgr.current_leader().is_none());
    }

    #[test]
    fn test_re_election_after_step_down() {
        let mut mgr = make_manager();
        mgr.register_candidate(CandidateId::new("c1"), 1);
        mgr.register_candidate(CandidateId::new("c2"), 2);
        mgr.elect();
        assert!(mgr.is_leader(&CandidateId::new("c1")));

        mgr.step_down(&CandidateId::new("c1"));
        let result = mgr.elect();
        // c1 still has higher priority
        assert_eq!(result, ElectionResult::Elected(CandidateId::new("c1")));
    }

    #[test]
    fn test_election_increments_term() {
        let mut mgr = make_manager();
        mgr.register_candidate(CandidateId::new("c1"), 1);

        mgr.elect();
        assert_eq!(mgr.stats().current_term, 1);

        mgr.elect();
        assert_eq!(mgr.stats().current_term, 2);
    }

    #[test]
    fn test_heartbeat() {
        let mut mgr = make_manager();
        mgr.register_candidate(CandidateId::new("c1"), 1);
        assert!(mgr.heartbeat(&CandidateId::new("c1")));
        assert!(!mgr.heartbeat(&CandidateId::new("unknown")));
    }

    #[tokio::test]
    async fn test_event_subscription() {
        let mut mgr = make_manager();
        let mut rx = mgr.subscribe();

        mgr.register_candidate(CandidateId::new("c1"), 1);
        mgr.elect();

        // Should receive CandidateJoined, ElectionStarted, LeaderElected
        let e1 = rx.recv().await.unwrap();
        assert!(matches!(e1, LeaderEvent::CandidateJoined(_)));

        let e2 = rx.recv().await.unwrap();
        assert!(matches!(e2, LeaderEvent::ElectionStarted { .. }));

        let e3 = rx.recv().await.unwrap();
        assert!(matches!(e3, LeaderEvent::LeaderElected { .. }));
    }

    #[test]
    fn test_stats() {
        let mut mgr = make_manager();
        mgr.register_candidate(CandidateId::new("c1"), 1);
        mgr.register_candidate(CandidateId::new("c2"), 2);
        mgr.elect();

        let stats = mgr.stats();
        assert_eq!(stats.total_candidates, 2);
        assert_eq!(stats.total_elections, 1);
        assert_eq!(stats.leader_changes, 1);
        assert_eq!(stats.current_leader, Some(CandidateId::new("c1")));
    }

    #[test]
    fn test_tiebreak_by_id() {
        let mut mgr = make_manager();
        // Same priority, different IDs
        mgr.register_candidate(CandidateId::new("b"), 1);
        mgr.register_candidate(CandidateId::new("a"), 1);

        let result = mgr.elect();
        // "a" wins tiebreak (lexicographic order)
        assert_eq!(result, ElectionResult::Elected(CandidateId::new("a")));
    }

    #[test]
    fn test_candidate_status_updates() {
        let mut mgr = make_manager();
        mgr.register_candidate(CandidateId::new("c1"), 1);
        mgr.register_candidate(CandidateId::new("c2"), 2);

        mgr.elect();

        let candidates = mgr.candidates();
        let c1 = candidates.iter().find(|c| c.id == CandidateId::new("c1")).unwrap();
        let c2 = candidates.iter().find(|c| c.id == CandidateId::new("c2")).unwrap();

        assert_eq!(c1.status, CandidateStatus::Leader);
        assert_eq!(c2.status, CandidateStatus::Follower);
    }

    #[test]
    fn test_lease_remaining() {
        let mut mgr = LeaderElectionManager::new(ElectionConfig {
            lease_duration: Duration::from_secs(60),
            ..Default::default()
        });
        mgr.register_candidate(CandidateId::new("c1"), 1);
        mgr.elect();

        let lease = mgr.current_lease().unwrap();
        assert!(lease.remaining() > Duration::from_secs(50));
        assert!(lease.remaining() <= Duration::from_secs(60));
    }
}

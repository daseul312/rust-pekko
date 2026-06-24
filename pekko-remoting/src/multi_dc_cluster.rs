//! Multi-DC Cluster - Multi-datacenter cluster support
//!
//! Provides datacenter-aware clustering with cross-DC replication,
//! DC-local operations, and configurable cross-DC failure detection.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::{Duration, Instant};

// =============================================================================
// Core Types
// =============================================================================

/// Errors for multi-DC operations
#[derive(Debug, Clone, PartialEq)]
pub enum MultiDcError {
    /// Datacenter not found
    DcNotFound(String),
    /// Node not in any datacenter
    NodeNotInDc(String),
    /// Cross-DC communication failure
    CrossDcFailure { from: String, to: String, reason: String },
    /// Insufficient replicas
    InsufficientReplicas { required: usize, available: usize },
    /// Configuration error
    InvalidConfig(String),
    /// DC already exists
    DcAlreadyExists(String),
    /// Quorum not achievable
    QuorumNotAchievable { dc: String, needed: usize, available: usize },
}

impl fmt::Display for MultiDcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MultiDcError::DcNotFound(dc) => write!(f, "Datacenter not found: {}", dc),
            MultiDcError::NodeNotInDc(n) => write!(f, "Node not in any DC: {}", n),
            MultiDcError::CrossDcFailure { from, to, reason } => {
                write!(f, "Cross-DC failure {}→{}: {}", from, to, reason)
            }
            MultiDcError::InsufficientReplicas { required, available } => {
                write!(f, "Insufficient replicas: need {}, have {}", required, available)
            }
            MultiDcError::InvalidConfig(e) => write!(f, "Invalid config: {}", e),
            MultiDcError::DcAlreadyExists(dc) => write!(f, "DC already exists: {}", dc),
            MultiDcError::QuorumNotAchievable { dc, needed, available } => {
                write!(f, "Quorum not achievable in {}: need {}, have {}", dc, needed, available)
            }
        }
    }
}

/// Datacenter identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataCenter {
    /// DC name
    pub name: String,
    /// DC region
    pub region: Option<String>,
    /// Priority (higher = preferred for leader election)
    pub priority: u32,
    /// Whether this DC is the primary
    pub is_primary: bool,
}

impl DataCenter {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            region: None,
            priority: 0,
            is_primary: false,
        }
    }

    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    pub fn as_primary(mut self) -> Self {
        self.is_primary = true;
        self
    }
}

impl fmt::Display for DataCenter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if let Some(region) = &self.region {
            write!(f, " ({})", region)?;
        }
        Ok(())
    }
}

/// Node membership in a datacenter
#[derive(Debug, Clone)]
pub struct DcMember {
    /// Node identifier
    pub node_id: String,
    /// Datacenter name
    pub dc_name: String,
    /// Member status
    pub status: DcMemberStatus,
    /// When joined
    pub joined_at: Instant,
    /// Roles
    pub roles: Vec<String>,
}

/// Status of a DC member
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DcMemberStatus {
    /// Node is joining
    Joining,
    /// Node is up and running
    Up,
    /// Node is leaving gracefully
    Leaving,
    /// Node has been detected as down
    Down,
    /// Node has been removed
    Removed,
    /// Node is unreachable (cross-DC detection)
    Unreachable,
}

impl fmt::Display for DcMemberStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DcMemberStatus::Joining => write!(f, "Joining"),
            DcMemberStatus::Up => write!(f, "Up"),
            DcMemberStatus::Leaving => write!(f, "Leaving"),
            DcMemberStatus::Down => write!(f, "Down"),
            DcMemberStatus::Removed => write!(f, "Removed"),
            DcMemberStatus::Unreachable => write!(f, "Unreachable"),
        }
    }
}

// =============================================================================
// Cross-DC Failure Detection
// =============================================================================

/// Configuration for cross-DC failure detection
#[derive(Debug, Clone)]
pub struct CrossDcFailureDetectorConfig {
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Acceptable heartbeat pause (before marking unreachable)
    pub acceptable_pause: Duration,
    /// Threshold for phi accrual detector
    pub threshold: f64,
    /// Number of heartbeats to keep for history
    pub max_sample_size: usize,
    /// Minimum standard deviation
    pub min_std_deviation: Duration,
}

impl Default for CrossDcFailureDetectorConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(5),
            acceptable_pause: Duration::from_secs(20),
            threshold: 10.0,
            max_sample_size: 200,
            min_std_deviation: Duration::from_millis(500),
        }
    }
}

/// Cross-DC failure detector tracking heartbeats between DCs
#[derive(Debug)]
pub struct CrossDcFailureDetector {
    /// Configuration
    config: CrossDcFailureDetectorConfig,
    /// Last heartbeat received from each DC
    last_heartbeat: HashMap<String, Instant>,
    /// Heartbeat intervals for phi calculation
    heartbeat_intervals: HashMap<String, Vec<Duration>>,
    /// Current phi values
    phi_values: HashMap<String, f64>,
    /// DCs marked as unreachable
    unreachable: HashSet<String>,
}

impl CrossDcFailureDetector {
    pub fn new(config: CrossDcFailureDetectorConfig) -> Self {
        Self {
            config,
            last_heartbeat: HashMap::new(),
            heartbeat_intervals: HashMap::new(),
            phi_values: HashMap::new(),
            unreachable: HashSet::new(),
        }
    }

    /// Record a heartbeat from a DC
    pub fn heartbeat(&mut self, dc_name: &str) {
        let now = Instant::now();

        if let Some(last) = self.last_heartbeat.get(dc_name) {
            let interval = now.duration_since(*last);
            let intervals = self.heartbeat_intervals.entry(dc_name.to_string())
                .or_insert_with(Vec::new);
            intervals.push(interval);
            if intervals.len() > self.config.max_sample_size {
                intervals.remove(0);
            }
        }

        self.last_heartbeat.insert(dc_name.to_string(), now);
        self.unreachable.remove(dc_name);
        self.phi_values.insert(dc_name.to_string(), 0.0);
    }

    /// Calculate phi value for a DC
    pub fn phi(&self, dc_name: &str) -> Option<f64> {
        let last = self.last_heartbeat.get(dc_name)?;
        let intervals = self.heartbeat_intervals.get(dc_name)?;

        if intervals.is_empty() {
            return Some(0.0);
        }

        let elapsed = last.elapsed();
        let mean = intervals.iter()
            .map(|d| d.as_millis() as f64)
            .sum::<f64>() / intervals.len() as f64;

        let variance = intervals.iter()
            .map(|d| {
                let diff = d.as_millis() as f64 - mean;
                diff * diff
            })
            .sum::<f64>() / intervals.len() as f64;

        let std_dev = variance.sqrt().max(self.config.min_std_deviation.as_millis() as f64);

        let elapsed_ms = elapsed.as_millis() as f64;
        let y = (elapsed_ms - mean) / std_dev;
        let phi = -(-y * (std::f64::consts::LN_2 / std::f64::consts::SQRT_2)).exp().ln()
            / std::f64::consts::LN_10;

        Some(phi.max(0.0))
    }

    /// Check if a DC is considered unreachable
    pub fn is_unreachable(&self, dc_name: &str) -> bool {
        if self.unreachable.contains(dc_name) {
            return true;
        }

        if let Some(phi) = self.phi(dc_name) {
            phi > self.config.threshold
        } else {
            // No heartbeat ever received — check if we've been waiting too long
            false
        }
    }

    /// Update status and return newly unreachable DCs
    pub fn check_all(&mut self) -> Vec<String> {
        let mut newly_unreachable = Vec::new();

        let dc_names: Vec<String> = self.last_heartbeat.keys().cloned().collect();
        for dc_name in dc_names {
            let phi = self.phi(&dc_name).unwrap_or(0.0);
            self.phi_values.insert(dc_name.clone(), phi);

            if phi > self.config.threshold && !self.unreachable.contains(&dc_name) {
                self.unreachable.insert(dc_name.clone());
                newly_unreachable.push(dc_name);
            }
        }

        newly_unreachable
    }

    /// Get all unreachable DCs
    pub fn unreachable_dcs(&self) -> Vec<&str> {
        self.unreachable.iter().map(|s| s.as_str()).collect()
    }

    /// Get current phi values
    pub fn all_phi_values(&self) -> &HashMap<String, f64> {
        &self.phi_values
    }

    /// Reset failure detector for a DC
    pub fn reset(&mut self, dc_name: &str) {
        self.last_heartbeat.remove(dc_name);
        self.heartbeat_intervals.remove(dc_name);
        self.phi_values.remove(dc_name);
        self.unreachable.remove(dc_name);
    }
}

// =============================================================================
// Replication
// =============================================================================

/// Cross-DC replication mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationMode {
    /// Synchronous replication (wait for all DCs)
    Sync,
    /// Async replication (fire and forget)
    Async,
    /// Semi-sync (wait for at least N DCs)
    SemiSync { min_acks: usize },
    /// Primary-secondary replication
    PrimarySecondary,
}

impl fmt::Display for ReplicationMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplicationMode::Sync => write!(f, "Sync"),
            ReplicationMode::Async => write!(f, "Async"),
            ReplicationMode::SemiSync { min_acks } => write!(f, "SemiSync({})", min_acks),
            ReplicationMode::PrimarySecondary => write!(f, "PrimarySecondary"),
        }
    }
}

/// Replication request
#[derive(Debug, Clone)]
pub struct ReplicationRequest {
    /// Request ID
    pub id: u64,
    /// Source DC
    pub source_dc: String,
    /// Target DCs (empty = all)
    pub target_dcs: Vec<String>,
    /// Data key
    pub key: String,
    /// Data payload (simplified as bytes length)
    pub payload_size: usize,
    /// Replication mode
    pub mode: ReplicationMode,
    /// Created at
    pub created_at: Instant,
    /// Timeout
    pub timeout: Duration,
}

/// Replication acknowledgment
#[derive(Debug, Clone)]
pub struct ReplicationAck {
    /// Request ID
    pub request_id: u64,
    /// Acknowledging DC
    pub dc_name: String,
    /// Success or failure
    pub success: bool,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Time taken
    pub duration: Duration,
}

/// Result of a replication operation
#[derive(Debug, Clone)]
pub struct ReplicationResult {
    /// Request ID
    pub request_id: u64,
    /// Overall success
    pub success: bool,
    /// Acks received
    pub acks: Vec<ReplicationAck>,
    /// DCs that didn't respond
    pub missing_acks: Vec<String>,
    /// Total duration
    pub duration: Duration,
}

/// Manages cross-DC replication
#[derive(Debug)]
pub struct ReplicationManager {
    /// Local DC name
    local_dc: String,
    /// Default replication mode
    default_mode: ReplicationMode,
    /// Pending replication requests
    pending: HashMap<u64, ReplicationRequest>,
    /// Received acks
    acks: HashMap<u64, Vec<ReplicationAck>>,
    /// Request counter
    request_counter: u64,
    /// Completed replications
    completed: Vec<ReplicationResult>,
    /// Max completed history
    max_history: usize,
}

impl ReplicationManager {
    pub fn new(local_dc: impl Into<String>, default_mode: ReplicationMode) -> Self {
        Self {
            local_dc: local_dc.into(),
            default_mode,
            pending: HashMap::new(),
            acks: HashMap::new(),
            request_counter: 0,
            completed: Vec::new(),
            max_history: 10000,
        }
    }

    /// Create a replication request
    pub fn replicate(
        &mut self,
        key: impl Into<String>,
        payload_size: usize,
        target_dcs: Vec<String>,
    ) -> ReplicationRequest {
        self.request_counter += 1;
        let request = ReplicationRequest {
            id: self.request_counter,
            source_dc: self.local_dc.clone(),
            target_dcs: target_dcs.clone(),
            key: key.into(),
            payload_size,
            mode: self.default_mode,
            created_at: Instant::now(),
            timeout: Duration::from_secs(10),
        };

        self.pending.insert(request.id, request.clone());
        self.acks.insert(request.id, Vec::new());
        request
    }

    /// Record an acknowledgment
    pub fn acknowledge(&mut self, ack: ReplicationAck) -> Option<ReplicationResult> {
        if let Some(ack_list) = self.acks.get_mut(&ack.request_id) {
            ack_list.push(ack.clone());
        }

        // Check if replication is complete
        if let Some(request) = self.pending.get(&ack.request_id) {
            let ack_list = self.acks.get(&ack.request_id)?;

            let complete = match request.mode {
                ReplicationMode::Sync => {
                    ack_list.len() >= request.target_dcs.len()
                }
                ReplicationMode::Async => true, // Always complete immediately
                ReplicationMode::SemiSync { min_acks } => {
                    let success_count = ack_list.iter().filter(|a| a.success).count();
                    success_count >= min_acks || ack_list.len() >= request.target_dcs.len()
                }
                ReplicationMode::PrimarySecondary => {
                    ack_list.len() >= 1 // At least one secondary ack
                }
            };

            if complete {
                return self.complete_request(ack.request_id);
            }
        }

        None
    }

    /// Check for timed-out requests
    pub fn check_timeouts(&mut self) -> Vec<ReplicationResult> {
        let timed_out: Vec<u64> = self.pending.iter()
            .filter(|(_, req)| req.created_at.elapsed() > req.timeout)
            .map(|(id, _)| *id)
            .collect();

        timed_out.into_iter()
            .filter_map(|id| self.complete_request(id))
            .collect()
    }

    fn complete_request(&mut self, request_id: u64) -> Option<ReplicationResult> {
        let request = self.pending.remove(&request_id)?;
        let acks = self.acks.remove(&request_id).unwrap_or_default();

        let acked_dcs: HashSet<String> = acks.iter().map(|a| a.dc_name.clone()).collect();
        let missing: Vec<String> = request.target_dcs.iter()
            .filter(|dc| !acked_dcs.contains(*dc))
            .cloned()
            .collect();

        let success = match request.mode {
            ReplicationMode::Sync => missing.is_empty() && acks.iter().all(|a| a.success),
            ReplicationMode::Async => true,
            ReplicationMode::SemiSync { min_acks } => {
                acks.iter().filter(|a| a.success).count() >= min_acks
            }
            ReplicationMode::PrimarySecondary => acks.iter().any(|a| a.success),
        };

        let result = ReplicationResult {
            request_id,
            success,
            acks,
            missing_acks: missing,
            duration: request.created_at.elapsed(),
        };

        if self.completed.len() >= self.max_history {
            self.completed.drain(..self.max_history / 2);
        }
        self.completed.push(result.clone());

        Some(result)
    }

    /// Number of pending requests
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Local DC name
    pub fn local_dc(&self) -> &str {
        &self.local_dc
    }

    /// Replication stats
    pub fn stats(&self) -> ReplicationStats {
        let success_count = self.completed.iter().filter(|r| r.success).count();
        ReplicationStats {
            total_requests: self.request_counter,
            pending: self.pending.len(),
            completed: self.completed.len(),
            success_count,
            failure_count: self.completed.len() - success_count,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicationStats {
    pub total_requests: u64,
    pub pending: usize,
    pub completed: usize,
    pub success_count: usize,
    pub failure_count: usize,
}

// =============================================================================
// Multi-DC Cluster Manager
// =============================================================================

/// Configuration for multi-DC cluster
#[derive(Debug, Clone)]
pub struct MultiDcConfig {
    /// Local datacenter name
    pub local_dc: String,
    /// Cross-DC failure detection config
    pub failure_detection: CrossDcFailureDetectorConfig,
    /// Default replication mode
    pub replication_mode: ReplicationMode,
    /// Whether to prefer local DC for reads
    pub prefer_local_reads: bool,
    /// Maximum cross-DC latency tolerance
    pub max_cross_dc_latency: Duration,
}

impl MultiDcConfig {
    pub fn new(local_dc: impl Into<String>) -> Self {
        Self {
            local_dc: local_dc.into(),
            failure_detection: CrossDcFailureDetectorConfig::default(),
            replication_mode: ReplicationMode::Async,
            prefer_local_reads: true,
            max_cross_dc_latency: Duration::from_secs(30),
        }
    }

    pub fn with_replication_mode(mut self, mode: ReplicationMode) -> Self {
        self.replication_mode = mode;
        self
    }
}

/// Manages the multi-DC cluster topology
#[derive(Debug)]
pub struct MultiDcCluster {
    /// Configuration
    config: MultiDcConfig,
    /// Known datacenters
    datacenters: HashMap<String, DataCenter>,
    /// DC members (dc_name → members)
    members: HashMap<String, Vec<DcMember>>,
    /// Cross-DC failure detector
    failure_detector: CrossDcFailureDetector,
    /// Replication manager
    replication: ReplicationManager,
    /// Cross-DC latency tracking (from→to → latencies)
    latencies: HashMap<(String, String), Vec<Duration>>,
}

impl MultiDcCluster {
    pub fn new(config: MultiDcConfig) -> Self {
        let fd_config = config.failure_detection.clone();
        let repl_mode = config.replication_mode;
        let local_dc = config.local_dc.clone();

        Self {
            config: config.clone(),
            datacenters: HashMap::new(),
            members: HashMap::new(),
            failure_detector: CrossDcFailureDetector::new(fd_config),
            replication: ReplicationManager::new(&local_dc, repl_mode),
            latencies: HashMap::new(),
        }
    }

    /// Register a datacenter
    pub fn register_dc(&mut self, dc: DataCenter) -> Result<(), MultiDcError> {
        if self.datacenters.contains_key(&dc.name) {
            return Err(MultiDcError::DcAlreadyExists(dc.name.clone()));
        }
        self.members.insert(dc.name.clone(), Vec::new());
        self.datacenters.insert(dc.name.clone(), dc);
        Ok(())
    }

    /// Add a member to a datacenter
    pub fn add_member(&mut self, node_id: impl Into<String>, dc_name: &str) -> Result<(), MultiDcError> {
        if !self.datacenters.contains_key(dc_name) {
            return Err(MultiDcError::DcNotFound(dc_name.to_string()));
        }

        let member = DcMember {
            node_id: node_id.into(),
            dc_name: dc_name.to_string(),
            status: DcMemberStatus::Joining,
            joined_at: Instant::now(),
            roles: Vec::new(),
        };

        self.members.entry(dc_name.to_string())
            .or_insert_with(Vec::new)
            .push(member);

        Ok(())
    }

    /// Update member status
    pub fn update_member_status(&mut self, node_id: &str, status: DcMemberStatus) -> Result<(), MultiDcError> {
        for members in self.members.values_mut() {
            if let Some(member) = members.iter_mut().find(|m| m.node_id == node_id) {
                member.status = status;
                return Ok(());
            }
        }
        Err(MultiDcError::NodeNotInDc(node_id.to_string()))
    }

    /// Remove a member
    pub fn remove_member(&mut self, node_id: &str) -> Option<DcMember> {
        for members in self.members.values_mut() {
            if let Some(pos) = members.iter().position(|m| m.node_id == node_id) {
                return Some(members.remove(pos));
            }
        }
        None
    }

    /// Get members of a datacenter
    pub fn dc_members(&self, dc_name: &str) -> Option<&Vec<DcMember>> {
        self.members.get(dc_name)
    }

    /// Get members with a specific status
    pub fn members_with_status(&self, dc_name: &str, status: DcMemberStatus) -> Vec<&DcMember> {
        self.members.get(dc_name)
            .map(|members| members.iter().filter(|m| m.status == status).collect())
            .unwrap_or_default()
    }

    /// Get up members of a DC
    pub fn up_members(&self, dc_name: &str) -> Vec<&DcMember> {
        self.members_with_status(dc_name, DcMemberStatus::Up)
    }

    /// Get local DC members
    pub fn local_members(&self) -> Vec<&DcMember> {
        self.members.get(&self.config.local_dc)
            .map(|m| m.iter().collect())
            .unwrap_or_default()
    }

    /// Find which DC a node belongs to
    pub fn node_dc(&self, node_id: &str) -> Option<&str> {
        for (dc_name, members) in &self.members {
            if members.iter().any(|m| m.node_id == node_id) {
                return Some(dc_name.as_str());
            }
        }
        None
    }

    /// Record cross-DC heartbeat
    pub fn record_heartbeat(&mut self, dc_name: &str) {
        self.failure_detector.heartbeat(dc_name);
    }

    /// Record cross-DC latency
    pub fn record_latency(&mut self, from_dc: &str, to_dc: &str, latency: Duration) {
        let key = (from_dc.to_string(), to_dc.to_string());
        let latencies = self.latencies.entry(key).or_insert_with(Vec::new);
        latencies.push(latency);
        if latencies.len() > 100 {
            latencies.drain(..50);
        }
    }

    /// Get average latency between DCs
    pub fn avg_latency(&self, from_dc: &str, to_dc: &str) -> Option<Duration> {
        let key = (from_dc.to_string(), to_dc.to_string());
        let latencies = self.latencies.get(&key)?;
        if latencies.is_empty() { return None; }
        let sum: Duration = latencies.iter().sum();
        Some(sum / latencies.len() as u32)
    }

    /// Check DC reachability
    pub fn is_dc_reachable(&self, dc_name: &str) -> bool {
        !self.failure_detector.is_unreachable(dc_name)
    }

    /// Get reachable DCs
    pub fn reachable_dcs(&self) -> Vec<&str> {
        self.datacenters.keys()
            .filter(|dc| !self.failure_detector.is_unreachable(dc))
            .map(|s| s.as_str())
            .collect()
    }

    /// Get all known datacenters
    pub fn all_dcs(&self) -> Vec<&DataCenter> {
        self.datacenters.values().collect()
    }

    /// Get a specific datacenter
    pub fn get_dc(&self, name: &str) -> Option<&DataCenter> {
        self.datacenters.get(name)
    }

    /// Get the primary DC
    pub fn primary_dc(&self) -> Option<&DataCenter> {
        self.datacenters.values().find(|dc| dc.is_primary)
    }

    /// Replicate data across DCs
    pub fn replicate(&mut self, key: impl Into<String>, payload_size: usize) -> ReplicationRequest {
        let target_dcs: Vec<String> = self.datacenters.keys()
            .filter(|dc| **dc != self.config.local_dc)
            .cloned()
            .collect();
        self.replication.replicate(key, payload_size, target_dcs)
    }

    /// Acknowledge replication
    pub fn ack_replication(&mut self, ack: ReplicationAck) -> Option<ReplicationResult> {
        self.replication.acknowledge(ack)
    }

    /// Local DC name
    pub fn local_dc(&self) -> &str {
        &self.config.local_dc
    }

    /// Total member count across all DCs
    pub fn total_members(&self) -> usize {
        self.members.values().map(|m| m.len()).sum()
    }

    /// DC count
    pub fn dc_count(&self) -> usize {
        self.datacenters.len()
    }

    /// Get cluster stats
    pub fn stats(&self) -> MultiDcStats {
        let mut per_dc: HashMap<String, DcStats> = HashMap::new();
        for (dc_name, members) in &self.members {
            let up = members.iter().filter(|m| m.status == DcMemberStatus::Up).count();
            let total = members.len();
            per_dc.insert(dc_name.clone(), DcStats {
                total_members: total,
                up_members: up,
                is_reachable: !self.failure_detector.is_unreachable(dc_name),
            });
        }

        MultiDcStats {
            total_dcs: self.datacenters.len(),
            total_members: self.total_members(),
            local_dc: self.config.local_dc.clone(),
            per_dc,
            replication: self.replication.stats(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MultiDcStats {
    pub total_dcs: usize,
    pub total_members: usize,
    pub local_dc: String,
    pub per_dc: HashMap<String, DcStats>,
    pub replication: ReplicationStats,
}

#[derive(Debug, Clone)]
pub struct DcStats {
    pub total_members: usize,
    pub up_members: usize,
    pub is_reachable: bool,
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // DataCenter Tests
    #[test]
    fn test_dc_new() {
        let dc = DataCenter::new("us-east-1");
        assert_eq!(dc.name, "us-east-1");
        assert!(!dc.is_primary);
        assert_eq!(dc.priority, 0);
    }

    #[test]
    fn test_dc_builder() {
        let dc = DataCenter::new("eu-west-1")
            .with_region("Europe")
            .with_priority(10)
            .as_primary();
        assert_eq!(dc.region, Some("Europe".into()));
        assert_eq!(dc.priority, 10);
        assert!(dc.is_primary);
    }

    #[test]
    fn test_dc_display() {
        let dc = DataCenter::new("us-east-1").with_region("US East");
        assert_eq!(dc.to_string(), "us-east-1 (US East)");

        let dc2 = DataCenter::new("local");
        assert_eq!(dc2.to_string(), "local");
    }

    // DcMemberStatus Tests
    #[test]
    fn test_member_status_display() {
        assert_eq!(DcMemberStatus::Up.to_string(), "Up");
        assert_eq!(DcMemberStatus::Unreachable.to_string(), "Unreachable");
    }

    // CrossDcFailureDetector Tests
    #[test]
    fn test_fd_new() {
        let fd = CrossDcFailureDetector::new(CrossDcFailureDetectorConfig::default());
        assert!(fd.unreachable_dcs().is_empty());
    }

    #[test]
    fn test_fd_heartbeat_reachable() {
        let mut fd = CrossDcFailureDetector::new(CrossDcFailureDetectorConfig::default());
        fd.heartbeat("dc2");
        assert!(!fd.is_unreachable("dc2"));
    }

    #[test]
    fn test_fd_no_heartbeat_not_unreachable() {
        let fd = CrossDcFailureDetector::new(CrossDcFailureDetectorConfig::default());
        // No heartbeat received → not unreachable (we haven't started tracking)
        assert!(!fd.is_unreachable("unknown-dc"));
    }

    #[test]
    fn test_fd_phi_after_heartbeat() {
        let mut fd = CrossDcFailureDetector::new(CrossDcFailureDetectorConfig::default());
        fd.heartbeat("dc2");
        // After first heartbeat, no intervals recorded yet → phi returns None
        let phi = fd.phi("dc2");
        assert!(phi.is_none());
        // After second heartbeat, intervals exist → phi is Some
        std::thread::sleep(Duration::from_millis(5));
        fd.heartbeat("dc2");
        let phi = fd.phi("dc2");
        assert!(phi.is_some());
        assert!(phi.unwrap() < 1.0);
    }

    #[test]
    fn test_fd_phi_with_intervals() {
        let mut fd = CrossDcFailureDetector::new(CrossDcFailureDetectorConfig::default());
        fd.heartbeat("dc2");
        std::thread::sleep(Duration::from_millis(10));
        fd.heartbeat("dc2");
        std::thread::sleep(Duration::from_millis(10));
        fd.heartbeat("dc2");

        let phi = fd.phi("dc2").unwrap();
        assert!(phi >= 0.0); // Should be low (recent heartbeat)
    }

    #[test]
    fn test_fd_reset() {
        let mut fd = CrossDcFailureDetector::new(CrossDcFailureDetectorConfig::default());
        fd.heartbeat("dc2");
        fd.reset("dc2");
        assert!(fd.phi("dc2").is_none());
    }

    #[test]
    fn test_fd_check_all_no_unreachable() {
        let mut fd = CrossDcFailureDetector::new(CrossDcFailureDetectorConfig::default());
        fd.heartbeat("dc2");
        let newly = fd.check_all();
        assert!(newly.is_empty());
    }

    // ReplicationMode Tests
    #[test]
    fn test_replication_mode_display() {
        assert_eq!(ReplicationMode::Sync.to_string(), "Sync");
        assert_eq!(ReplicationMode::Async.to_string(), "Async");
        assert_eq!(ReplicationMode::SemiSync { min_acks: 2 }.to_string(), "SemiSync(2)");
    }

    // ReplicationManager Tests
    #[test]
    fn test_repl_manager_new() {
        let rm = ReplicationManager::new("dc1", ReplicationMode::Async);
        assert_eq!(rm.local_dc(), "dc1");
        assert_eq!(rm.pending_count(), 0);
    }

    #[test]
    fn test_repl_replicate() {
        let mut rm = ReplicationManager::new("dc1", ReplicationMode::Async);
        let req = rm.replicate("key1", 100, vec!["dc2".into(), "dc3".into()]);
        assert_eq!(req.source_dc, "dc1");
        assert_eq!(req.target_dcs.len(), 2);
        assert_eq!(rm.pending_count(), 1);
    }

    #[test]
    fn test_repl_async_immediate_complete() {
        let mut rm = ReplicationManager::new("dc1", ReplicationMode::Async);
        let req = rm.replicate("key1", 100, vec!["dc2".into()]);

        let ack = ReplicationAck {
            request_id: req.id,
            dc_name: "dc2".into(),
            success: true,
            error: None,
            duration: Duration::from_millis(50),
        };

        let result = rm.acknowledge(ack);
        assert!(result.is_some());
        assert!(result.unwrap().success);
    }

    #[test]
    fn test_repl_sync_needs_all_acks() {
        let mut rm = ReplicationManager::new("dc1", ReplicationMode::Sync);
        let req = rm.replicate("key1", 100, vec!["dc2".into(), "dc3".into()]);

        // First ack
        let ack1 = ReplicationAck {
            request_id: req.id,
            dc_name: "dc2".into(),
            success: true,
            error: None,
            duration: Duration::from_millis(50),
        };
        assert!(rm.acknowledge(ack1).is_none()); // Not complete yet

        // Second ack → complete
        let ack2 = ReplicationAck {
            request_id: req.id,
            dc_name: "dc3".into(),
            success: true,
            error: None,
            duration: Duration::from_millis(100),
        };
        let result = rm.acknowledge(ack2);
        assert!(result.is_some());
        assert!(result.unwrap().success);
    }

    #[test]
    fn test_repl_semi_sync() {
        let mut rm = ReplicationManager::new("dc1", ReplicationMode::SemiSync { min_acks: 1 });
        let req = rm.replicate("key1", 100, vec!["dc2".into(), "dc3".into()]);

        // One successful ack is enough
        let ack = ReplicationAck {
            request_id: req.id,
            dc_name: "dc2".into(),
            success: true,
            error: None,
            duration: Duration::from_millis(50),
        };
        let result = rm.acknowledge(ack);
        assert!(result.is_some());
        assert!(result.unwrap().success);
    }

    #[test]
    fn test_repl_stats() {
        let mut rm = ReplicationManager::new("dc1", ReplicationMode::Async);
        let req = rm.replicate("key1", 100, vec!["dc2".into()]);
        let ack = ReplicationAck {
            request_id: req.id,
            dc_name: "dc2".into(),
            success: true,
            error: None,
            duration: Duration::from_millis(10),
        };
        rm.acknowledge(ack);

        let stats = rm.stats();
        assert_eq!(stats.total_requests, 1);
        assert_eq!(stats.completed, 1);
    }

    // MultiDcCluster Tests
    #[test]
    fn test_cluster_new() {
        let config = MultiDcConfig::new("dc1");
        let cluster = MultiDcCluster::new(config);
        assert_eq!(cluster.local_dc(), "dc1");
        assert_eq!(cluster.dc_count(), 0);
    }

    #[test]
    fn test_cluster_register_dc() {
        let config = MultiDcConfig::new("dc1");
        let mut cluster = MultiDcCluster::new(config);

        cluster.register_dc(DataCenter::new("dc1")).unwrap();
        cluster.register_dc(DataCenter::new("dc2")).unwrap();
        assert_eq!(cluster.dc_count(), 2);
    }

    #[test]
    fn test_cluster_register_duplicate_dc() {
        let config = MultiDcConfig::new("dc1");
        let mut cluster = MultiDcCluster::new(config);

        cluster.register_dc(DataCenter::new("dc1")).unwrap();
        assert!(matches!(cluster.register_dc(DataCenter::new("dc1")),
            Err(MultiDcError::DcAlreadyExists(_))));
    }

    #[test]
    fn test_cluster_add_member() {
        let config = MultiDcConfig::new("dc1");
        let mut cluster = MultiDcCluster::new(config);
        cluster.register_dc(DataCenter::new("dc1")).unwrap();

        cluster.add_member("node1", "dc1").unwrap();
        cluster.add_member("node2", "dc1").unwrap();
        assert_eq!(cluster.total_members(), 2);
    }

    #[test]
    fn test_cluster_add_member_bad_dc() {
        let config = MultiDcConfig::new("dc1");
        let mut cluster = MultiDcCluster::new(config);
        assert!(matches!(cluster.add_member("node1", "bad-dc"),
            Err(MultiDcError::DcNotFound(_))));
    }

    #[test]
    fn test_cluster_update_member_status() {
        let config = MultiDcConfig::new("dc1");
        let mut cluster = MultiDcCluster::new(config);
        cluster.register_dc(DataCenter::new("dc1")).unwrap();
        cluster.add_member("node1", "dc1").unwrap();

        cluster.update_member_status("node1", DcMemberStatus::Up).unwrap();
        let members = cluster.up_members("dc1");
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].node_id, "node1");
    }

    #[test]
    fn test_cluster_update_unknown_member() {
        let config = MultiDcConfig::new("dc1");
        let mut cluster = MultiDcCluster::new(config);
        assert!(cluster.update_member_status("unknown", DcMemberStatus::Up).is_err());
    }

    #[test]
    fn test_cluster_remove_member() {
        let config = MultiDcConfig::new("dc1");
        let mut cluster = MultiDcCluster::new(config);
        cluster.register_dc(DataCenter::new("dc1")).unwrap();
        cluster.add_member("node1", "dc1").unwrap();

        let removed = cluster.remove_member("node1");
        assert!(removed.is_some());
        assert_eq!(cluster.total_members(), 0);
    }

    #[test]
    fn test_cluster_node_dc() {
        let config = MultiDcConfig::new("dc1");
        let mut cluster = MultiDcCluster::new(config);
        cluster.register_dc(DataCenter::new("dc1")).unwrap();
        cluster.register_dc(DataCenter::new("dc2")).unwrap();
        cluster.add_member("node1", "dc1").unwrap();
        cluster.add_member("node2", "dc2").unwrap();

        assert_eq!(cluster.node_dc("node1"), Some("dc1"));
        assert_eq!(cluster.node_dc("node2"), Some("dc2"));
        assert_eq!(cluster.node_dc("node3"), None);
    }

    #[test]
    fn test_cluster_local_members() {
        let config = MultiDcConfig::new("dc1");
        let mut cluster = MultiDcCluster::new(config);
        cluster.register_dc(DataCenter::new("dc1")).unwrap();
        cluster.register_dc(DataCenter::new("dc2")).unwrap();
        cluster.add_member("node1", "dc1").unwrap();
        cluster.add_member("node2", "dc2").unwrap();

        let local = cluster.local_members();
        assert_eq!(local.len(), 1);
        assert_eq!(local[0].node_id, "node1");
    }

    #[test]
    fn test_cluster_dc_reachable() {
        let config = MultiDcConfig::new("dc1");
        let mut cluster = MultiDcCluster::new(config);
        cluster.register_dc(DataCenter::new("dc1")).unwrap();
        cluster.register_dc(DataCenter::new("dc2")).unwrap();
        cluster.record_heartbeat("dc2");
        assert!(cluster.is_dc_reachable("dc2"));
    }

    #[test]
    fn test_cluster_latency_tracking() {
        let config = MultiDcConfig::new("dc1");
        let mut cluster = MultiDcCluster::new(config);
        cluster.record_latency("dc1", "dc2", Duration::from_millis(50));
        cluster.record_latency("dc1", "dc2", Duration::from_millis(100));

        let avg = cluster.avg_latency("dc1", "dc2").unwrap();
        assert_eq!(avg.as_millis(), 75);
    }

    #[test]
    fn test_cluster_primary_dc() {
        let config = MultiDcConfig::new("dc1");
        let mut cluster = MultiDcCluster::new(config);
        cluster.register_dc(DataCenter::new("dc1").as_primary()).unwrap();
        cluster.register_dc(DataCenter::new("dc2")).unwrap();

        let primary = cluster.primary_dc().unwrap();
        assert_eq!(primary.name, "dc1");
        assert!(primary.is_primary);
    }

    #[test]
    fn test_cluster_replicate() {
        let config = MultiDcConfig::new("dc1")
            .with_replication_mode(ReplicationMode::Async);
        let mut cluster = MultiDcCluster::new(config);
        cluster.register_dc(DataCenter::new("dc1")).unwrap();
        cluster.register_dc(DataCenter::new("dc2")).unwrap();

        let req = cluster.replicate("key1", 256);
        assert_eq!(req.source_dc, "dc1");
        assert_eq!(req.target_dcs.len(), 1); // Only dc2 (excluding local)
    }

    #[test]
    fn test_cluster_replicate_and_ack() {
        let config = MultiDcConfig::new("dc1")
            .with_replication_mode(ReplicationMode::Async);
        let mut cluster = MultiDcCluster::new(config);
        cluster.register_dc(DataCenter::new("dc1")).unwrap();
        cluster.register_dc(DataCenter::new("dc2")).unwrap();

        let req = cluster.replicate("key1", 256);
        let ack = ReplicationAck {
            request_id: req.id,
            dc_name: "dc2".into(),
            success: true,
            error: None,
            duration: Duration::from_millis(50),
        };

        let result = cluster.ack_replication(ack);
        assert!(result.is_some());
        assert!(result.unwrap().success);
    }

    #[test]
    fn test_cluster_stats() {
        let config = MultiDcConfig::new("dc1");
        let mut cluster = MultiDcCluster::new(config);
        cluster.register_dc(DataCenter::new("dc1")).unwrap();
        cluster.register_dc(DataCenter::new("dc2")).unwrap();
        cluster.add_member("n1", "dc1").unwrap();
        cluster.add_member("n2", "dc1").unwrap();
        cluster.add_member("n3", "dc2").unwrap();

        let stats = cluster.stats();
        assert_eq!(stats.total_dcs, 2);
        assert_eq!(stats.total_members, 3);
    }

    // MultiDcError Tests
    #[test]
    fn test_multi_dc_error_display() {
        let e = MultiDcError::DcNotFound("dc1".into());
        assert!(e.to_string().contains("dc1"));

        let e = MultiDcError::CrossDcFailure {
            from: "dc1".into(), to: "dc2".into(), reason: "timeout".into()
        };
        assert!(e.to_string().contains("dc1") && e.to_string().contains("dc2"));
    }

    // Config Tests
    #[test]
    fn test_multi_dc_config() {
        let config = MultiDcConfig::new("dc1")
            .with_replication_mode(ReplicationMode::Sync);
        assert_eq!(config.local_dc, "dc1");
        assert_eq!(config.replication_mode, ReplicationMode::Sync);
        assert!(config.prefer_local_reads);
    }

    // Reachable DCs
    #[test]
    fn test_cluster_reachable_dcs() {
        let config = MultiDcConfig::new("dc1");
        let mut cluster = MultiDcCluster::new(config);
        cluster.register_dc(DataCenter::new("dc1")).unwrap();
        cluster.register_dc(DataCenter::new("dc2")).unwrap();

        let reachable = cluster.reachable_dcs();
        assert_eq!(reachable.len(), 2);
    }

    // Get DC
    #[test]
    fn test_cluster_get_dc() {
        let config = MultiDcConfig::new("dc1");
        let mut cluster = MultiDcCluster::new(config);
        cluster.register_dc(DataCenter::new("dc1").with_region("US")).unwrap();

        let dc = cluster.get_dc("dc1").unwrap();
        assert_eq!(dc.region, Some("US".into()));
        assert!(cluster.get_dc("nonexistent").is_none());
    }
}

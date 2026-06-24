//! Cluster Metrics - Node metrics collection and distribution via gossip
//!
//! Provides cluster-wide metrics collection and distribution using gossip protocol.
//! Supports CPU, memory, heap usage tracking with EWMA smoothing for stability.
//! MetricsSelector enables routing decisions based on real-time node metrics.

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

// =============================================================================
// Core Types
// =============================================================================

/// Error types for cluster metrics
#[derive(Debug, Clone, PartialEq)]
pub enum MetricsError {
    /// Node not found in metrics registry
    NodeNotFound(String),
    /// Metric type not available
    MetricNotAvailable(String),
    /// Collector error
    CollectionFailed(String),
    /// Configuration error
    InvalidConfig(String),
    /// Stale metrics (older than max age)
    StaleMetrics { node: String, age_ms: u64 },
}

impl fmt::Display for MetricsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricsError::NodeNotFound(n) => write!(f, "Node not found: {}", n),
            MetricsError::MetricNotAvailable(m) => write!(f, "Metric not available: {}", m),
            MetricsError::CollectionFailed(e) => write!(f, "Collection failed: {}", e),
            MetricsError::InvalidConfig(e) => write!(f, "Invalid config: {}", e),
            MetricsError::StaleMetrics { node, age_ms } => {
                write!(f, "Stale metrics for {}: {}ms old", node, age_ms)
            }
        }
    }
}

/// Metric type identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricType {
    /// CPU usage (0.0 - 1.0)
    CpuUsage,
    /// System load average (1-minute)
    SystemLoadAverage,
    /// Heap memory used (bytes)
    HeapMemoryUsed,
    /// Heap memory committed (bytes)
    HeapMemoryCommitted,
    /// Heap memory max (bytes)
    HeapMemoryMax,
    /// Total physical memory (bytes)
    TotalMemory,
    /// Available memory (bytes)
    AvailableMemory,
    /// Number of processors
    Processors,
    /// Custom metric
    Custom(u32),
}

impl fmt::Display for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricType::CpuUsage => write!(f, "cpu_usage"),
            MetricType::SystemLoadAverage => write!(f, "system_load_average"),
            MetricType::HeapMemoryUsed => write!(f, "heap_memory_used"),
            MetricType::HeapMemoryCommitted => write!(f, "heap_memory_committed"),
            MetricType::HeapMemoryMax => write!(f, "heap_memory_max"),
            MetricType::TotalMemory => write!(f, "total_memory"),
            MetricType::AvailableMemory => write!(f, "available_memory"),
            MetricType::Processors => write!(f, "processors"),
            MetricType::Custom(id) => write!(f, "custom_{}", id),
        }
    }
}

/// A single metric value with EWMA smoothing
#[derive(Debug, Clone)]
pub struct MetricValue {
    /// Metric type
    pub metric_type: MetricType,
    /// Raw value
    pub value: f64,
    /// EWMA-smoothed value
    pub smoothed_value: f64,
    /// Timestamp of measurement
    pub timestamp: Instant,
    /// Whether this is a rate-based metric
    pub is_rate: bool,
}

impl MetricValue {
    pub fn new(metric_type: MetricType, value: f64) -> Self {
        Self {
            metric_type,
            value,
            smoothed_value: value,
            timestamp: Instant::now(),
            is_rate: matches!(metric_type, MetricType::CpuUsage | MetricType::SystemLoadAverage),
        }
    }

    /// Update with new value and apply EWMA smoothing
    pub fn update(&mut self, new_value: f64, alpha: f64) {
        self.value = new_value;
        self.smoothed_value = alpha * new_value + (1.0 - alpha) * self.smoothed_value;
        self.timestamp = Instant::now();
    }

    /// Age of this metric
    pub fn age(&self) -> Duration {
        self.timestamp.elapsed()
    }
}

// =============================================================================
// EWMA (Exponentially Weighted Moving Average)
// =============================================================================

/// EWMA smoothing calculator
#[derive(Debug, Clone)]
pub struct Ewma {
    /// Smoothing factor (0.0 - 1.0). Higher = more responsive, lower = more stable.
    alpha: f64,
    /// Current smoothed value
    value: f64,
    /// Whether initialized
    initialized: bool,
    /// Number of samples seen
    samples: u64,
}

impl Ewma {
    /// Create with decay factor alpha
    pub fn new(alpha: f64) -> Self {
        let alpha = alpha.clamp(0.01, 1.0);
        Self {
            alpha,
            value: 0.0,
            initialized: false,
            samples: 0,
        }
    }

    /// Create with half-life duration and collection interval
    pub fn from_half_life(half_life: Duration, interval: Duration) -> Self {
        let interval_ms = interval.as_millis() as f64;
        let half_life_ms = half_life.as_millis() as f64;
        let alpha = if half_life_ms > 0.0 {
            1.0 - (-interval_ms * (2.0_f64.ln()) / half_life_ms).exp()
        } else {
            1.0
        };
        Self::new(alpha)
    }

    /// Feed a new sample and get the smoothed value
    pub fn update(&mut self, sample: f64) -> f64 {
        self.samples += 1;
        if !self.initialized {
            self.value = sample;
            self.initialized = true;
        } else {
            self.value = self.alpha * sample + (1.0 - self.alpha) * self.value;
        }
        self.value
    }

    /// Current smoothed value
    pub fn value(&self) -> f64 {
        self.value
    }

    /// Alpha (smoothing factor)
    pub fn alpha(&self) -> f64 {
        self.alpha
    }

    /// Number of samples processed
    pub fn samples(&self) -> u64 {
        self.samples
    }

    /// Whether initialized
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Reset to uninitialized state
    pub fn reset(&mut self) {
        self.value = 0.0;
        self.initialized = false;
        self.samples = 0;
    }
}

// =============================================================================
// Node Metrics
// =============================================================================

/// Collected metrics for a single node
#[derive(Debug, Clone)]
pub struct ClusterNodeMetrics {
    /// Node identifier (address string)
    pub node_id: String,
    /// Individual metric values
    pub metrics: HashMap<MetricType, MetricValue>,
    /// When these metrics were collected
    pub collected_at: Instant,
    /// Sequence number for ordering
    pub sequence_nr: u64,
}

impl ClusterNodeMetrics {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            metrics: HashMap::new(),
            collected_at: Instant::now(),
            sequence_nr: 0,
        }
    }

    /// Set a metric value
    pub fn set_metric(&mut self, metric_type: MetricType, value: f64) {
        self.metrics.entry(metric_type)
            .and_modify(|m| m.update(value, 0.3))
            .or_insert_with(|| MetricValue::new(metric_type, value));
        self.collected_at = Instant::now();
        self.sequence_nr += 1;
    }

    /// Get a metric value
    pub fn get_metric(&self, metric_type: MetricType) -> Option<&MetricValue> {
        self.metrics.get(&metric_type)
    }

    /// Get smoothed value for a metric
    pub fn smoothed(&self, metric_type: MetricType) -> Option<f64> {
        self.metrics.get(&metric_type).map(|m| m.smoothed_value)
    }

    /// Get raw value for a metric
    pub fn raw(&self, metric_type: MetricType) -> Option<f64> {
        self.metrics.get(&metric_type).map(|m| m.value)
    }

    /// CPU usage (0.0 - 1.0)
    pub fn cpu_usage(&self) -> Option<f64> {
        self.smoothed(MetricType::CpuUsage)
    }

    /// Heap memory usage ratio (0.0 - 1.0)
    pub fn heap_usage_ratio(&self) -> Option<f64> {
        let used = self.smoothed(MetricType::HeapMemoryUsed)?;
        let max = self.smoothed(MetricType::HeapMemoryMax)?;
        if max > 0.0 { Some(used / max) } else { None }
    }

    /// Memory usage ratio (0.0 - 1.0)
    pub fn memory_usage_ratio(&self) -> Option<f64> {
        let total = self.smoothed(MetricType::TotalMemory)?;
        let available = self.smoothed(MetricType::AvailableMemory)?;
        if total > 0.0 { Some(1.0 - available / total) } else { None }
    }

    /// Age of these metrics
    pub fn age(&self) -> Duration {
        self.collected_at.elapsed()
    }

    /// Merge with newer metrics (gossip convergence)
    pub fn merge(&mut self, other: &ClusterNodeMetrics) {
        if other.sequence_nr > self.sequence_nr {
            for (metric_type, metric_value) in &other.metrics {
                self.metrics.entry(*metric_type)
                    .and_modify(|m| m.update(metric_value.value, 0.3))
                    .or_insert_with(|| metric_value.clone());
            }
            self.collected_at = other.collected_at;
            self.sequence_nr = other.sequence_nr;
        }
    }
}

// =============================================================================
// Metrics Selector
// =============================================================================

/// Strategy for selecting/scoring nodes based on metrics
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MetricsSelectorType {
    /// Select based on CPU usage (lower is better)
    Cpu,
    /// Select based on heap memory usage (lower is better)
    HeapMemory,
    /// Select based on system load average (lower is better)
    SystemLoad,
    /// Select based on mixed metrics (weighted combination)
    Mixed { cpu_weight: f64, memory_weight: f64, load_weight: f64 },
    /// Select based on available memory (higher is better)
    AvailableMemory,
}

/// Selector that computes node scores from metrics
#[derive(Debug, Clone)]
pub struct MetricsSelector {
    /// Selector type
    selector_type: MetricsSelectorType,
    /// Maximum acceptable metric age
    max_age: Duration,
}

impl MetricsSelector {
    pub fn cpu() -> Self {
        Self {
            selector_type: MetricsSelectorType::Cpu,
            max_age: Duration::from_secs(30),
        }
    }

    pub fn heap_memory() -> Self {
        Self {
            selector_type: MetricsSelectorType::HeapMemory,
            max_age: Duration::from_secs(30),
        }
    }

    pub fn system_load() -> Self {
        Self {
            selector_type: MetricsSelectorType::SystemLoad,
            max_age: Duration::from_secs(30),
        }
    }

    pub fn mixed(cpu_weight: f64, memory_weight: f64, load_weight: f64) -> Self {
        Self {
            selector_type: MetricsSelectorType::Mixed {
                cpu_weight,
                memory_weight,
                load_weight,
            },
            max_age: Duration::from_secs(30),
        }
    }

    pub fn available_memory() -> Self {
        Self {
            selector_type: MetricsSelectorType::AvailableMemory,
            max_age: Duration::from_secs(30),
        }
    }

    pub fn with_max_age(mut self, max_age: Duration) -> Self {
        self.max_age = max_age;
        self
    }

    /// Compute a score for a node (higher = better candidate for routing)
    pub fn score(&self, metrics: &ClusterNodeMetrics) -> Result<f64, MetricsError> {
        if metrics.age() > self.max_age {
            return Err(MetricsError::StaleMetrics {
                node: metrics.node_id.clone(),
                age_ms: metrics.age().as_millis() as u64,
            });
        }

        match self.selector_type {
            MetricsSelectorType::Cpu => {
                let cpu = metrics.cpu_usage()
                    .ok_or_else(|| MetricsError::MetricNotAvailable("cpu_usage".into()))?;
                Ok(1.0 - cpu) // Lower CPU = higher score
            }
            MetricsSelectorType::HeapMemory => {
                let ratio = metrics.heap_usage_ratio()
                    .ok_or_else(|| MetricsError::MetricNotAvailable("heap_memory".into()))?;
                Ok(1.0 - ratio) // Lower heap usage = higher score
            }
            MetricsSelectorType::SystemLoad => {
                let load = metrics.smoothed(MetricType::SystemLoadAverage)
                    .ok_or_else(|| MetricsError::MetricNotAvailable("system_load".into()))?;
                let processors = metrics.smoothed(MetricType::Processors).unwrap_or(1.0);
                let normalized = if processors > 0.0 { load / processors } else { load };
                Ok(1.0 / (1.0 + normalized)) // Lower load = higher score
            }
            MetricsSelectorType::Mixed { cpu_weight, memory_weight, load_weight } => {
                let total_weight = cpu_weight + memory_weight + load_weight;
                if total_weight == 0.0 {
                    return Err(MetricsError::InvalidConfig("All weights are zero".into()));
                }

                let mut score = 0.0;
                let mut used_weight = 0.0;

                if let Some(cpu) = metrics.cpu_usage() {
                    score += (1.0 - cpu) * cpu_weight;
                    used_weight += cpu_weight;
                }
                if let Some(ratio) = metrics.heap_usage_ratio() {
                    score += (1.0 - ratio) * memory_weight;
                    used_weight += memory_weight;
                }
                if let Some(load) = metrics.smoothed(MetricType::SystemLoadAverage) {
                    let processors = metrics.smoothed(MetricType::Processors).unwrap_or(1.0);
                    let norm = if processors > 0.0 { load / processors } else { load };
                    score += (1.0 / (1.0 + norm)) * load_weight;
                    used_weight += load_weight;
                }

                if used_weight > 0.0 {
                    Ok(score / used_weight)
                } else {
                    Err(MetricsError::MetricNotAvailable("no metrics available".into()))
                }
            }
            MetricsSelectorType::AvailableMemory => {
                let available = metrics.smoothed(MetricType::AvailableMemory)
                    .ok_or_else(|| MetricsError::MetricNotAvailable("available_memory".into()))?;
                let total = metrics.smoothed(MetricType::TotalMemory).unwrap_or(available);
                if total > 0.0 {
                    Ok(available / total) // Higher available = higher score
                } else {
                    Ok(0.0)
                }
            }
        }
    }

    /// Select the best node from a set of metrics
    pub fn select_best<'a>(
        &self,
        nodes: &'a [ClusterNodeMetrics],
    ) -> Option<(&'a ClusterNodeMetrics, f64)> {
        nodes.iter()
            .filter_map(|n| self.score(n).ok().map(|s| (n, s)))
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
    }

    /// Rank nodes by score (best first)
    pub fn rank<'a>(
        &self,
        nodes: &'a [ClusterNodeMetrics],
    ) -> Vec<(&'a ClusterNodeMetrics, f64)> {
        let mut ranked: Vec<_> = nodes.iter()
            .filter_map(|n| self.score(n).ok().map(|s| (n, s)))
            .collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        ranked
    }

    /// Compute weights for weighted routing (probability distribution)
    pub fn weights(&self, nodes: &[ClusterNodeMetrics]) -> Vec<(String, f64)> {
        let scored: Vec<_> = nodes.iter()
            .filter_map(|n| self.score(n).ok().map(|s| (n.node_id.clone(), s)))
            .collect();

        let total: f64 = scored.iter().map(|(_, s)| s).sum();
        if total == 0.0 {
            return scored.into_iter().map(|(id, _)| (id, 1.0 / nodes.len() as f64)).collect();
        }

        scored.into_iter().map(|(id, s)| (id, s / total)).collect()
    }

    pub fn selector_type(&self) -> MetricsSelectorType {
        self.selector_type
    }
}

// =============================================================================
// Metrics Collector
// =============================================================================

/// Configuration for metrics collection
#[derive(Debug, Clone)]
pub struct MetricsCollectorConfig {
    /// How often to collect metrics locally
    pub collection_interval: Duration,
    /// How often to gossip metrics to peers
    pub gossip_interval: Duration,
    /// Maximum age before metrics are considered stale
    pub max_metrics_age: Duration,
    /// EWMA alpha (smoothing factor)
    pub ewma_alpha: f64,
    /// Which metrics to collect
    pub enabled_metrics: Vec<MetricType>,
    /// Maximum number of nodes to track
    pub max_nodes: usize,
}

impl Default for MetricsCollectorConfig {
    fn default() -> Self {
        Self {
            collection_interval: Duration::from_secs(3),
            gossip_interval: Duration::from_secs(5),
            max_metrics_age: Duration::from_secs(30),
            ewma_alpha: 0.3,
            enabled_metrics: vec![
                MetricType::CpuUsage,
                MetricType::SystemLoadAverage,
                MetricType::HeapMemoryUsed,
                MetricType::HeapMemoryCommitted,
                MetricType::HeapMemoryMax,
                MetricType::TotalMemory,
                MetricType::AvailableMemory,
                MetricType::Processors,
            ],
            max_nodes: 1000,
        }
    }
}

/// Gossip envelope for distributing metrics
#[derive(Debug, Clone)]
pub struct MetricsGossipEnvelope {
    /// Source node
    pub from: String,
    /// Metrics for all known nodes
    pub node_metrics: Vec<ClusterNodeMetrics>,
    /// Gossip sequence number
    pub sequence_nr: u64,
    /// Timestamp
    pub timestamp: Instant,
}

impl MetricsGossipEnvelope {
    pub fn new(from: impl Into<String>, node_metrics: Vec<ClusterNodeMetrics>, sequence_nr: u64) -> Self {
        Self {
            from: from.into(),
            node_metrics,
            sequence_nr,
            timestamp: Instant::now(),
        }
    }
}

/// Collects and distributes node metrics across the cluster
#[derive(Debug)]
pub struct MetricsCollector {
    /// Local node identifier
    local_node: String,
    /// All known node metrics
    node_metrics: HashMap<String, ClusterNodeMetrics>,
    /// Configuration
    config: MetricsCollectorConfig,
    /// EWMA calculators per metric type
    ewma_calculators: HashMap<MetricType, Ewma>,
    /// Gossip sequence number
    gossip_seq: u64,
    /// Collection count
    collection_count: u64,
    /// Last collection time
    last_collection: Option<Instant>,
    /// Last gossip time
    last_gossip: Option<Instant>,
    /// Removed nodes (recently left)
    removed_nodes: Vec<(String, Instant)>,
}

impl MetricsCollector {
    pub fn new(local_node: impl Into<String>, config: MetricsCollectorConfig) -> Self {
        let local = local_node.into();
        let mut node_metrics = HashMap::new();
        node_metrics.insert(local.clone(), ClusterNodeMetrics::new(&local));

        let ewma_calculators = config.enabled_metrics.iter()
            .map(|mt| (*mt, Ewma::new(config.ewma_alpha)))
            .collect();

        Self {
            local_node: local,
            node_metrics,
            config,
            ewma_calculators,
            gossip_seq: 0,
            collection_count: 0,
            last_collection: None,
            last_gossip: None,
            removed_nodes: Vec::new(),
        }
    }

    /// Record a local metric measurement
    pub fn record_metric(&mut self, metric_type: MetricType, value: f64) {
        // Apply EWMA smoothing
        let smoothed = if let Some(ewma) = self.ewma_calculators.get_mut(&metric_type) {
            ewma.update(value)
        } else {
            value
        };

        if let Some(local_metrics) = self.node_metrics.get_mut(&self.local_node) {
            local_metrics.set_metric(metric_type, smoothed);
        }

        self.collection_count += 1;
        self.last_collection = Some(Instant::now());
    }

    /// Record a batch of local metrics
    pub fn record_batch(&mut self, metrics: &[(MetricType, f64)]) {
        for (metric_type, value) in metrics {
            self.record_metric(*metric_type, *value);
        }
    }

    /// Receive metrics gossip from another node
    pub fn receive_gossip(&mut self, envelope: &MetricsGossipEnvelope) {
        for remote_metrics in &envelope.node_metrics {
            if remote_metrics.node_id == self.local_node {
                continue; // Don't overwrite our own metrics
            }

            if self.node_metrics.len() >= self.config.max_nodes
                && !self.node_metrics.contains_key(&remote_metrics.node_id)
            {
                continue; // At capacity, don't add new nodes
            }

            self.node_metrics.entry(remote_metrics.node_id.clone())
                .and_modify(|existing| existing.merge(remote_metrics))
                .or_insert_with(|| remote_metrics.clone());
        }
    }

    /// Create a gossip envelope for distribution
    pub fn create_gossip(&mut self) -> MetricsGossipEnvelope {
        self.gossip_seq += 1;
        self.last_gossip = Some(Instant::now());

        let metrics: Vec<ClusterNodeMetrics> = self.node_metrics.values()
            .filter(|m| m.age() < self.config.max_metrics_age)
            .cloned()
            .collect();

        MetricsGossipEnvelope::new(&self.local_node, metrics, self.gossip_seq)
    }

    /// Get metrics for a specific node
    pub fn get_node_metrics(&self, node_id: &str) -> Option<&ClusterNodeMetrics> {
        self.node_metrics.get(node_id)
    }

    /// Get local node metrics
    pub fn local_metrics(&self) -> Option<&ClusterNodeMetrics> {
        self.node_metrics.get(&self.local_node)
    }

    /// Get all node metrics
    pub fn all_metrics(&self) -> Vec<&ClusterNodeMetrics> {
        self.node_metrics.values().collect()
    }

    /// Get non-stale metrics
    pub fn active_metrics(&self) -> Vec<&ClusterNodeMetrics> {
        self.node_metrics.values()
            .filter(|m| m.age() < self.config.max_metrics_age)
            .collect()
    }

    /// Remove a node from tracking
    pub fn remove_node(&mut self, node_id: &str) {
        if let Some(_removed) = self.node_metrics.remove(node_id) {
            self.removed_nodes.push((node_id.to_string(), Instant::now()));
        }
    }

    /// Prune stale metrics
    pub fn prune_stale(&mut self) -> Vec<String> {
        let max_age = self.config.max_metrics_age * 3; // 3x max age before removal
        let stale: Vec<String> = self.node_metrics.iter()
            .filter(|(id, m)| **id != self.local_node && m.age() > max_age)
            .map(|(id, _)| id.clone())
            .collect();

        for id in &stale {
            self.node_metrics.remove(id);
            self.removed_nodes.push((id.clone(), Instant::now()));
        }

        // Clean old removed entries
        let cleanup_age = Duration::from_secs(300);
        self.removed_nodes.retain(|(_, t)| t.elapsed() < cleanup_age);

        stale
    }

    /// Number of tracked nodes
    pub fn node_count(&self) -> usize {
        self.node_metrics.len()
    }

    /// Local node id
    pub fn local_node(&self) -> &str {
        &self.local_node
    }

    /// Configuration
    pub fn config(&self) -> &MetricsCollectorConfig {
        &self.config
    }

    /// Collector stats
    pub fn stats(&self) -> CollectorStats {
        CollectorStats {
            local_node: self.local_node.clone(),
            tracked_nodes: self.node_metrics.len(),
            collection_count: self.collection_count,
            gossip_seq: self.gossip_seq,
            stale_count: self.node_metrics.values()
                .filter(|m| m.age() > self.config.max_metrics_age)
                .count(),
        }
    }
}

/// Statistics for the metrics collector
#[derive(Debug, Clone)]
pub struct CollectorStats {
    pub local_node: String,
    pub tracked_nodes: usize,
    pub collection_count: u64,
    pub gossip_seq: u64,
    pub stale_count: usize,
}

// =============================================================================
// Metrics-Aware Routing
// =============================================================================

/// Routing decision based on metrics
#[derive(Debug, Clone)]
pub struct MetricsRoutingDecision {
    /// Selected node
    pub node_id: String,
    /// Score of the selected node
    pub score: f64,
    /// All considered nodes with scores
    pub candidates: Vec<(String, f64)>,
    /// Selector used
    pub selector_type: MetricsSelectorType,
}

/// Router that uses cluster metrics for routing decisions
#[derive(Debug)]
pub struct MetricsAwareRouter {
    /// Selector strategy
    selector: MetricsSelector,
    /// Minimum number of candidates for routing
    min_candidates: usize,
    /// Whether to use weighted random selection
    weighted_random: bool,
    /// Decision history
    decisions: Vec<(String, Instant)>,
    /// Max history size
    max_history: usize,
}

impl MetricsAwareRouter {
    pub fn new(selector: MetricsSelector) -> Self {
        Self {
            selector,
            min_candidates: 1,
            weighted_random: false,
            decisions: Vec::new(),
            max_history: 1000,
        }
    }

    pub fn with_min_candidates(mut self, min: usize) -> Self {
        self.min_candidates = min;
        self
    }

    pub fn with_weighted_random(mut self, enabled: bool) -> Self {
        self.weighted_random = enabled;
        self
    }

    /// Route to the best node based on metrics
    pub fn route(&mut self, nodes: &[ClusterNodeMetrics]) -> Result<MetricsRoutingDecision, MetricsError> {
        let ranked = self.selector.rank(nodes);

        if ranked.len() < self.min_candidates {
            return Err(MetricsError::InvalidConfig(format!(
                "Need at least {} candidates, only {} available",
                self.min_candidates,
                ranked.len()
            )));
        }

        let candidates: Vec<(String, f64)> = ranked.iter()
            .map(|(n, s)| (n.node_id.clone(), *s))
            .collect();

        let selected = if self.weighted_random && candidates.len() > 1 {
            // Simple deterministic weighted selection based on scores
            let total: f64 = candidates.iter().map(|(_, s)| s).sum();
            let threshold = total * 0.5; // Pick based on cumulative distribution
            let mut cumulative = 0.0;
            let mut chosen = &candidates[0];
            for c in &candidates {
                cumulative += c.1;
                if cumulative >= threshold {
                    chosen = c;
                    break;
                }
            }
            chosen.clone()
        } else {
            candidates[0].clone()
        };

        // Track decision
        if self.decisions.len() >= self.max_history {
            self.decisions.drain(..self.max_history / 2);
        }
        self.decisions.push((selected.0.clone(), Instant::now()));

        Ok(MetricsRoutingDecision {
            node_id: selected.0,
            score: selected.1,
            candidates,
            selector_type: self.selector.selector_type(),
        })
    }

    /// Get routing statistics
    pub fn routing_stats(&self) -> RouterStats {
        let mut distribution: HashMap<String, usize> = HashMap::new();
        for (node, _) in &self.decisions {
            *distribution.entry(node.clone()).or_insert(0) += 1;
        }

        RouterStats {
            total_decisions: self.decisions.len(),
            distribution,
            selector_type: self.selector.selector_type(),
        }
    }
}

/// Statistics for the metrics-aware router
#[derive(Debug, Clone)]
pub struct RouterStats {
    pub total_decisions: usize,
    pub distribution: HashMap<String, usize>,
    pub selector_type: MetricsSelectorType,
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // EWMA Tests
    #[test]
    fn test_ewma_new() {
        let ewma = Ewma::new(0.5);
        assert_eq!(ewma.alpha(), 0.5);
        assert!(!ewma.is_initialized());
        assert_eq!(ewma.samples(), 0);
    }

    #[test]
    fn test_ewma_clamp() {
        let low = Ewma::new(-1.0);
        assert_eq!(low.alpha(), 0.01);

        let high = Ewma::new(5.0);
        assert_eq!(high.alpha(), 1.0);
    }

    #[test]
    fn test_ewma_first_sample() {
        let mut ewma = Ewma::new(0.5);
        let val = ewma.update(100.0);
        assert_eq!(val, 100.0);
        assert!(ewma.is_initialized());
        assert_eq!(ewma.samples(), 1);
    }

    #[test]
    fn test_ewma_smoothing() {
        let mut ewma = Ewma::new(0.5);
        ewma.update(100.0);
        let val = ewma.update(200.0);
        // 0.5 * 200 + 0.5 * 100 = 150
        assert!((val - 150.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_ewma_convergence() {
        let mut ewma = Ewma::new(0.3);
        ewma.update(0.0);
        for _ in 0..100 {
            ewma.update(1.0);
        }
        // Should converge close to 1.0
        assert!((ewma.value() - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_ewma_reset() {
        let mut ewma = Ewma::new(0.5);
        ewma.update(100.0);
        ewma.update(200.0);
        ewma.reset();
        assert!(!ewma.is_initialized());
        assert_eq!(ewma.samples(), 0);
    }

    #[test]
    fn test_ewma_from_half_life() {
        let ewma = Ewma::from_half_life(Duration::from_secs(10), Duration::from_secs(1));
        assert!(ewma.alpha() > 0.0 && ewma.alpha() < 1.0);
    }

    // MetricValue Tests
    #[test]
    fn test_metric_value_new() {
        let mv = MetricValue::new(MetricType::CpuUsage, 0.75);
        assert_eq!(mv.value, 0.75);
        assert_eq!(mv.smoothed_value, 0.75);
        assert!(mv.is_rate);
        assert_eq!(mv.metric_type, MetricType::CpuUsage);
    }

    #[test]
    fn test_metric_value_update() {
        let mut mv = MetricValue::new(MetricType::HeapMemoryUsed, 1000.0);
        mv.update(2000.0, 0.5);
        assert_eq!(mv.value, 2000.0);
        assert!((mv.smoothed_value - 1500.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_metric_value_is_rate() {
        assert!(MetricValue::new(MetricType::CpuUsage, 0.5).is_rate);
        assert!(MetricValue::new(MetricType::SystemLoadAverage, 1.0).is_rate);
        assert!(!MetricValue::new(MetricType::HeapMemoryUsed, 1000.0).is_rate);
    }

    // ClusterNodeMetrics Tests
    #[test]
    fn test_node_metrics_new() {
        let nm = ClusterNodeMetrics::new("node1");
        assert_eq!(nm.node_id, "node1");
        assert!(nm.metrics.is_empty());
    }

    #[test]
    fn test_node_metrics_set_get() {
        let mut nm = ClusterNodeMetrics::new("node1");
        nm.set_metric(MetricType::CpuUsage, 0.5);
        assert!(nm.get_metric(MetricType::CpuUsage).is_some());
        assert!(nm.get_metric(MetricType::HeapMemoryUsed).is_none());
    }

    #[test]
    fn test_node_metrics_cpu_usage() {
        let mut nm = ClusterNodeMetrics::new("node1");
        nm.set_metric(MetricType::CpuUsage, 0.75);
        let cpu = nm.cpu_usage().unwrap();
        assert!((cpu - 0.75).abs() < 0.01);
    }

    #[test]
    fn test_node_metrics_heap_ratio() {
        let mut nm = ClusterNodeMetrics::new("node1");
        nm.set_metric(MetricType::HeapMemoryUsed, 500.0);
        nm.set_metric(MetricType::HeapMemoryMax, 1000.0);
        let ratio = nm.heap_usage_ratio().unwrap();
        assert!((ratio - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_node_metrics_memory_ratio() {
        let mut nm = ClusterNodeMetrics::new("node1");
        nm.set_metric(MetricType::TotalMemory, 16000.0);
        nm.set_metric(MetricType::AvailableMemory, 8000.0);
        let ratio = nm.memory_usage_ratio().unwrap();
        assert!((ratio - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_node_metrics_merge_newer() {
        let mut nm1 = ClusterNodeMetrics::new("node1");
        nm1.set_metric(MetricType::CpuUsage, 0.5);

        let mut nm2 = ClusterNodeMetrics::new("node1");
        nm2.sequence_nr = nm1.sequence_nr + 1;
        nm2.set_metric(MetricType::CpuUsage, 0.8);

        nm1.merge(&nm2);
        assert!(nm1.sequence_nr >= nm2.sequence_nr);
    }

    #[test]
    fn test_node_metrics_merge_older_ignored() {
        let mut nm1 = ClusterNodeMetrics::new("node1");
        nm1.set_metric(MetricType::CpuUsage, 0.5);
        nm1.set_metric(MetricType::CpuUsage, 0.6); // seq_nr increments
        let seq = nm1.sequence_nr;

        let mut nm_old = ClusterNodeMetrics::new("node1");
        nm_old.sequence_nr = 0;
        nm_old.set_metric(MetricType::CpuUsage, 0.1);

        nm1.merge(&nm_old);
        assert_eq!(nm1.sequence_nr, seq); // Unchanged
    }

    // MetricsSelector Tests
    #[test]
    fn test_selector_cpu_score() {
        let selector = MetricsSelector::cpu();
        let mut nm = ClusterNodeMetrics::new("node1");
        nm.set_metric(MetricType::CpuUsage, 0.3);

        let score = selector.score(&nm).unwrap();
        assert!((score - 0.7).abs() < 0.01);
    }

    #[test]
    fn test_selector_heap_memory_score() {
        let selector = MetricsSelector::heap_memory();
        let mut nm = ClusterNodeMetrics::new("node1");
        nm.set_metric(MetricType::HeapMemoryUsed, 400.0);
        nm.set_metric(MetricType::HeapMemoryMax, 1000.0);

        let score = selector.score(&nm).unwrap();
        assert!((score - 0.6).abs() < 0.01);
    }

    #[test]
    fn test_selector_system_load_score() {
        let selector = MetricsSelector::system_load();
        let mut nm = ClusterNodeMetrics::new("node1");
        nm.set_metric(MetricType::SystemLoadAverage, 2.0);
        nm.set_metric(MetricType::Processors, 4.0);

        let score = selector.score(&nm).unwrap();
        // load/proc = 0.5, score = 1/(1+0.5) = 0.667
        assert!((score - 0.667).abs() < 0.01);
    }

    #[test]
    fn test_selector_mixed_score() {
        let selector = MetricsSelector::mixed(1.0, 1.0, 0.0);
        let mut nm = ClusterNodeMetrics::new("node1");
        nm.set_metric(MetricType::CpuUsage, 0.2);
        nm.set_metric(MetricType::HeapMemoryUsed, 300.0);
        nm.set_metric(MetricType::HeapMemoryMax, 1000.0);

        let score = selector.score(&nm).unwrap();
        // cpu_score=0.8, heap_score=0.7, avg=0.75
        assert!((score - 0.75).abs() < 0.01);
    }

    #[test]
    fn test_selector_available_memory() {
        let selector = MetricsSelector::available_memory();
        let mut nm = ClusterNodeMetrics::new("node1");
        nm.set_metric(MetricType::AvailableMemory, 12000.0);
        nm.set_metric(MetricType::TotalMemory, 16000.0);

        let score = selector.score(&nm).unwrap();
        assert!((score - 0.75).abs() < 0.01);
    }

    #[test]
    fn test_selector_missing_metric() {
        let selector = MetricsSelector::cpu();
        let nm = ClusterNodeMetrics::new("node1");
        assert!(selector.score(&nm).is_err());
    }

    #[test]
    fn test_selector_select_best() {
        let selector = MetricsSelector::cpu();
        let mut n1 = ClusterNodeMetrics::new("node1");
        n1.set_metric(MetricType::CpuUsage, 0.8); // High CPU
        let mut n2 = ClusterNodeMetrics::new("node2");
        n2.set_metric(MetricType::CpuUsage, 0.2); // Low CPU (better)
        let mut n3 = ClusterNodeMetrics::new("node3");
        n3.set_metric(MetricType::CpuUsage, 0.5);

        let nodes = vec![n1, n2, n3];
        let (best, _score) = selector.select_best(&nodes).unwrap();
        assert_eq!(best.node_id, "node2");
    }

    #[test]
    fn test_selector_rank() {
        let selector = MetricsSelector::cpu();
        let mut n1 = ClusterNodeMetrics::new("node1");
        n1.set_metric(MetricType::CpuUsage, 0.8);
        let mut n2 = ClusterNodeMetrics::new("node2");
        n2.set_metric(MetricType::CpuUsage, 0.2);
        let mut n3 = ClusterNodeMetrics::new("node3");
        n3.set_metric(MetricType::CpuUsage, 0.5);

        let nodes = vec![n1, n2, n3];
        let ranked = selector.rank(&nodes);
        assert_eq!(ranked[0].0.node_id, "node2"); // Best
        assert_eq!(ranked[1].0.node_id, "node3");
        assert_eq!(ranked[2].0.node_id, "node1"); // Worst
    }

    #[test]
    fn test_selector_weights() {
        let selector = MetricsSelector::cpu();
        let mut n1 = ClusterNodeMetrics::new("node1");
        n1.set_metric(MetricType::CpuUsage, 0.5);
        let mut n2 = ClusterNodeMetrics::new("node2");
        n2.set_metric(MetricType::CpuUsage, 0.5);

        let nodes = vec![n1, n2];
        let weights = selector.weights(&nodes);
        assert_eq!(weights.len(), 2);
        // Equal CPU → equal weights
        let total: f64 = weights.iter().map(|(_, w)| w).sum();
        assert!((total - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_selector_with_max_age() {
        let selector = MetricsSelector::cpu().with_max_age(Duration::from_millis(1));
        let mut nm = ClusterNodeMetrics::new("node1");
        nm.set_metric(MetricType::CpuUsage, 0.5);
        // Force stale by waiting
        std::thread::sleep(Duration::from_millis(2));
        assert!(matches!(selector.score(&nm), Err(MetricsError::StaleMetrics { .. })));
    }

    // MetricsCollector Tests
    #[test]
    fn test_collector_new() {
        let collector = MetricsCollector::new("node1", MetricsCollectorConfig::default());
        assert_eq!(collector.local_node(), "node1");
        assert_eq!(collector.node_count(), 1); // Self
    }

    #[test]
    fn test_collector_record_metric() {
        let mut collector = MetricsCollector::new("node1", MetricsCollectorConfig::default());
        collector.record_metric(MetricType::CpuUsage, 0.5);

        let local = collector.local_metrics().unwrap();
        assert!(local.cpu_usage().is_some());
    }

    #[test]
    fn test_collector_record_batch() {
        let mut collector = MetricsCollector::new("node1", MetricsCollectorConfig::default());
        collector.record_batch(&[
            (MetricType::CpuUsage, 0.5),
            (MetricType::HeapMemoryUsed, 1000.0),
            (MetricType::HeapMemoryMax, 2000.0),
        ]);

        let local = collector.local_metrics().unwrap();
        assert!(local.cpu_usage().is_some());
        assert!(local.heap_usage_ratio().is_some());
    }

    #[test]
    fn test_collector_gossip_roundtrip() {
        let mut c1 = MetricsCollector::new("node1", MetricsCollectorConfig::default());
        c1.record_metric(MetricType::CpuUsage, 0.3);

        let envelope = c1.create_gossip();

        let mut c2 = MetricsCollector::new("node2", MetricsCollectorConfig::default());
        c2.record_metric(MetricType::CpuUsage, 0.7);
        c2.receive_gossip(&envelope);

        assert_eq!(c2.node_count(), 2);
        assert!(c2.get_node_metrics("node1").is_some());
    }

    #[test]
    fn test_collector_gossip_no_overwrite_local() {
        let mut c1 = MetricsCollector::new("node1", MetricsCollectorConfig::default());
        c1.record_metric(MetricType::CpuUsage, 0.9);

        // Create gossip that includes node1 metrics with different value
        let mut fake_metrics = ClusterNodeMetrics::new("node1");
        fake_metrics.set_metric(MetricType::CpuUsage, 0.1);
        fake_metrics.sequence_nr = 999;
        let envelope = MetricsGossipEnvelope::new("node2", vec![fake_metrics], 1);

        c1.receive_gossip(&envelope);
        // Should NOT overwrite local metrics
        let local = c1.local_metrics().unwrap();
        let cpu = local.cpu_usage().unwrap();
        assert!(cpu > 0.5); // Still close to 0.9, not 0.1
    }

    #[test]
    fn test_collector_remove_node() {
        let mut c = MetricsCollector::new("node1", MetricsCollectorConfig::default());
        // Add node2 via gossip
        let mut nm2 = ClusterNodeMetrics::new("node2");
        nm2.set_metric(MetricType::CpuUsage, 0.5);
        nm2.sequence_nr = 1;
        let env = MetricsGossipEnvelope::new("node2", vec![nm2], 1);
        c.receive_gossip(&env);
        assert_eq!(c.node_count(), 2);

        c.remove_node("node2");
        assert_eq!(c.node_count(), 1);
    }

    #[test]
    fn test_collector_stats() {
        let mut c = MetricsCollector::new("node1", MetricsCollectorConfig::default());
        c.record_metric(MetricType::CpuUsage, 0.5);
        c.record_metric(MetricType::CpuUsage, 0.6);
        let stats = c.stats();
        assert_eq!(stats.local_node, "node1");
        assert_eq!(stats.tracked_nodes, 1);
        assert_eq!(stats.collection_count, 2);
    }

    #[test]
    fn test_collector_active_metrics() {
        let mut c = MetricsCollector::new("node1", MetricsCollectorConfig::default());
        c.record_metric(MetricType::CpuUsage, 0.5);
        let active = c.active_metrics();
        assert_eq!(active.len(), 1);
    }

    #[test]
    fn test_collector_max_nodes_limit() {
        let mut config = MetricsCollectorConfig::default();
        config.max_nodes = 2;
        let mut c = MetricsCollector::new("node1", config);

        // Add node2
        let mut nm2 = ClusterNodeMetrics::new("node2");
        nm2.set_metric(MetricType::CpuUsage, 0.5);
        nm2.sequence_nr = 1;
        let env = MetricsGossipEnvelope::new("node2", vec![nm2], 1);
        c.receive_gossip(&env);

        // Try to add node3 (should be rejected — at max)
        let mut nm3 = ClusterNodeMetrics::new("node3");
        nm3.set_metric(MetricType::CpuUsage, 0.3);
        nm3.sequence_nr = 1;
        let env = MetricsGossipEnvelope::new("node3", vec![nm3], 2);
        c.receive_gossip(&env);

        assert_eq!(c.node_count(), 2);
    }

    // MetricsGossipEnvelope Tests
    #[test]
    fn test_gossip_envelope_new() {
        let nm = ClusterNodeMetrics::new("node1");
        let env = MetricsGossipEnvelope::new("node1", vec![nm], 1);
        assert_eq!(env.from, "node1");
        assert_eq!(env.sequence_nr, 1);
        assert_eq!(env.node_metrics.len(), 1);
    }

    // MetricsAwareRouter Tests
    #[test]
    fn test_router_route_best() {
        let selector = MetricsSelector::cpu();
        let mut router = MetricsAwareRouter::new(selector);

        let mut n1 = ClusterNodeMetrics::new("node1");
        n1.set_metric(MetricType::CpuUsage, 0.8);
        let mut n2 = ClusterNodeMetrics::new("node2");
        n2.set_metric(MetricType::CpuUsage, 0.2);

        let nodes = vec![n1, n2];
        let decision = router.route(&nodes).unwrap();
        assert_eq!(decision.node_id, "node2"); // Lower CPU
    }

    #[test]
    fn test_router_weighted_random() {
        let selector = MetricsSelector::cpu();
        let mut router = MetricsAwareRouter::new(selector).with_weighted_random(true);

        let mut n1 = ClusterNodeMetrics::new("node1");
        n1.set_metric(MetricType::CpuUsage, 0.2);
        let mut n2 = ClusterNodeMetrics::new("node2");
        n2.set_metric(MetricType::CpuUsage, 0.8);

        let nodes = vec![n1, n2];
        let decision = router.route(&nodes).unwrap();
        // With weighted random, node1 should be selected more often
        assert_eq!(decision.candidates.len(), 2);
    }

    #[test]
    fn test_router_min_candidates() {
        let selector = MetricsSelector::cpu();
        let mut router = MetricsAwareRouter::new(selector).with_min_candidates(3);

        let mut n1 = ClusterNodeMetrics::new("node1");
        n1.set_metric(MetricType::CpuUsage, 0.5);

        let nodes = vec![n1];
        assert!(router.route(&nodes).is_err());
    }

    #[test]
    fn test_router_stats() {
        let selector = MetricsSelector::cpu();
        let mut router = MetricsAwareRouter::new(selector);

        let mut n1 = ClusterNodeMetrics::new("node1");
        n1.set_metric(MetricType::CpuUsage, 0.5);
        let nodes = vec![n1];

        router.route(&nodes).unwrap();
        router.route(&nodes).unwrap();

        let stats = router.routing_stats();
        assert_eq!(stats.total_decisions, 2);
        assert_eq!(*stats.distribution.get("node1").unwrap(), 2);
    }

    // MetricType Tests
    #[test]
    fn test_metric_type_display() {
        assert_eq!(MetricType::CpuUsage.to_string(), "cpu_usage");
        assert_eq!(MetricType::HeapMemoryUsed.to_string(), "heap_memory_used");
        assert_eq!(MetricType::Custom(42).to_string(), "custom_42");
    }

    // MetricsError Tests
    #[test]
    fn test_metrics_error_display() {
        let e = MetricsError::NodeNotFound("node1".into());
        assert!(e.to_string().contains("node1"));

        let e = MetricsError::StaleMetrics { node: "n".into(), age_ms: 5000 };
        assert!(e.to_string().contains("5000"));
    }

    // Mixed selector zero weights
    #[test]
    fn test_selector_mixed_zero_weights() {
        let selector = MetricsSelector::mixed(0.0, 0.0, 0.0);
        let mut nm = ClusterNodeMetrics::new("node1");
        nm.set_metric(MetricType::CpuUsage, 0.5);
        assert!(matches!(selector.score(&nm), Err(MetricsError::InvalidConfig(_))));
    }

    // Edge case: heap max zero
    #[test]
    fn test_heap_ratio_zero_max() {
        let mut nm = ClusterNodeMetrics::new("node1");
        nm.set_metric(MetricType::HeapMemoryUsed, 100.0);
        nm.set_metric(MetricType::HeapMemoryMax, 0.0);
        assert!(nm.heap_usage_ratio().is_none());
    }

    // Multi-gossip convergence
    #[test]
    fn test_three_node_gossip() {
        let config = MetricsCollectorConfig::default();
        let mut c1 = MetricsCollector::new("node1", config.clone());
        let mut c2 = MetricsCollector::new("node2", config.clone());
        let mut c3 = MetricsCollector::new("node3", config.clone());

        c1.record_metric(MetricType::CpuUsage, 0.2);
        c2.record_metric(MetricType::CpuUsage, 0.5);
        c3.record_metric(MetricType::CpuUsage, 0.8);

        // c1 → c2
        let env1 = c1.create_gossip();
        c2.receive_gossip(&env1);
        assert_eq!(c2.node_count(), 2);

        // c2 → c3 (now c2 has c1's metrics too)
        let env2 = c2.create_gossip();
        c3.receive_gossip(&env2);
        assert_eq!(c3.node_count(), 3); // c3 knows about c1, c2, c3
    }
}

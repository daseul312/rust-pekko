//! # Adaptive Load Balancing
//!
//! Smart message distribution strategies for routing messages across cluster nodes.
//!
//! Provides:
//! - Pluggable routing strategies (RoundRobin, Random, ConsistentHash, LeastLoad, Weighted)
//! - Node metrics tracking (latency, throughput, error rate)
//! - Adaptive strategy selection based on metrics
//! - Consistent hashing for entity affinity

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// NodeId & NodeMetrics
// ---------------------------------------------------------------------------

/// Unique node identifier
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Real-time metrics for a node
#[derive(Debug, Clone)]
pub struct NodeMetrics {
    pub node_id: NodeId,
    pub active_connections: u64,
    pub total_requests: u64,
    pub total_errors: u64,
    pub avg_latency_ms: f64,
    pub last_updated: Instant,
    pub weight: f64,
}

impl NodeMetrics {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            active_connections: 0,
            total_requests: 0,
            total_errors: 0,
            avg_latency_ms: 0.0,
            last_updated: Instant::now(),
            weight: 1.0,
        }
    }

    pub fn error_rate(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            self.total_errors as f64 / self.total_requests as f64
        }
    }

    /// Load score: lower is better
    pub fn load_score(&self) -> f64 {
        let latency_factor = self.avg_latency_ms / 100.0; // normalize
        let error_factor = self.error_rate() * 10.0;
        let conn_factor = self.active_connections as f64 / 100.0;
        latency_factor + error_factor + conn_factor
    }

    pub fn is_healthy(&self, max_error_rate: f64) -> bool {
        self.error_rate() < max_error_rate
    }
}

// ---------------------------------------------------------------------------
// RoutingStrategy trait + implementations
// ---------------------------------------------------------------------------

/// Routing strategy trait
pub trait RoutingStrategy: Send + Sync {
    fn name(&self) -> &str;
    fn select<'a>(&self, nodes: &'a [NodeId], key: Option<&str>, ctx: &RoutingContext) -> Option<&'a NodeId>;
}

/// Context for routing decisions
pub struct RoutingContext {
    pub metrics: HashMap<NodeId, NodeMetrics>,
    pub round_robin_counter: u64,
}

impl RoutingContext {
    pub fn new() -> Self {
        Self {
            metrics: HashMap::new(),
            round_robin_counter: 0,
        }
    }
}

impl Default for RoutingContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Round-robin strategy
pub struct RoundRobinStrategy;

impl RoutingStrategy for RoundRobinStrategy {
    fn name(&self) -> &str {
        "round-robin"
    }

    fn select<'a>(&self, nodes: &'a [NodeId], _key: Option<&str>, ctx: &RoutingContext) -> Option<&'a NodeId> {
        if nodes.is_empty() {
            return None;
        }
        let idx = ctx.round_robin_counter as usize % nodes.len();
        Some(&nodes[idx])
    }
}

/// Random strategy
pub struct RandomStrategy {
    seed: AtomicU64,
}

impl RandomStrategy {
    pub fn new() -> Self {
        Self {
            seed: AtomicU64::new(Instant::now().elapsed().as_nanos() as u64),
        }
    }

    fn next_rand(&self) -> u64 {
        // Simple xorshift64
        let mut s = self.seed.load(Ordering::Relaxed);
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        self.seed.store(s, Ordering::Relaxed);
        s
    }
}

impl Default for RandomStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl RoutingStrategy for RandomStrategy {
    fn name(&self) -> &str {
        "random"
    }

    fn select<'a>(&self, nodes: &'a [NodeId], _key: Option<&str>, _ctx: &RoutingContext) -> Option<&'a NodeId> {
        if nodes.is_empty() {
            return None;
        }
        let idx = self.next_rand() as usize % nodes.len();
        Some(&nodes[idx])
    }
}

/// Consistent hashing strategy
pub struct ConsistentHashStrategy {
    virtual_nodes: usize,
}

impl ConsistentHashStrategy {
    pub fn new(virtual_nodes: usize) -> Self {
        Self { virtual_nodes }
    }

    fn hash_key(key: &str) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

impl Default for ConsistentHashStrategy {
    fn default() -> Self {
        Self::new(150)
    }
}

impl RoutingStrategy for ConsistentHashStrategy {
    fn name(&self) -> &str {
        "consistent-hash"
    }

    fn select<'a>(&self, nodes: &'a [NodeId], key: Option<&str>, _ctx: &RoutingContext) -> Option<&'a NodeId> {
        if nodes.is_empty() {
            return None;
        }

        let key = key.unwrap_or("default");
        let key_hash = Self::hash_key(key);

        // Build virtual node ring
        let mut ring: Vec<(u64, usize)> = Vec::new();
        for (i, node) in nodes.iter().enumerate() {
            for v in 0..self.virtual_nodes {
                let vnode_key = format!("{}:{}", node.as_str(), v);
                ring.push((Self::hash_key(&vnode_key), i));
            }
        }
        ring.sort_by_key(|&(h, _)| h);

        // Find first node with hash >= key_hash
        let idx = ring.iter()
            .position(|&(h, _)| h >= key_hash)
            .unwrap_or(0);

        Some(&nodes[ring[idx].1])
    }
}

/// Least-load strategy — picks node with lowest load score
pub struct LeastLoadStrategy;

impl RoutingStrategy for LeastLoadStrategy {
    fn name(&self) -> &str {
        "least-load"
    }

    fn select<'a>(&self, nodes: &'a [NodeId], _key: Option<&str>, ctx: &RoutingContext) -> Option<&'a NodeId> {
        if nodes.is_empty() {
            return None;
        }

        nodes.iter()
            .min_by(|a, b| {
                let sa = ctx.metrics.get(a).map_or(0.0, |m| m.load_score());
                let sb = ctx.metrics.get(b).map_or(0.0, |m| m.load_score());
                sa.partial_cmp(&sb).unwrap_or(std::cmp::Ordering::Equal)
            })
    }
}

/// Weighted strategy — higher weight = more traffic
pub struct WeightedStrategy;

impl RoutingStrategy for WeightedStrategy {
    fn name(&self) -> &str {
        "weighted"
    }

    fn select<'a>(&self, nodes: &'a [NodeId], _key: Option<&str>, ctx: &RoutingContext) -> Option<&'a NodeId> {
        if nodes.is_empty() {
            return None;
        }

        let total_weight: f64 = nodes.iter()
            .map(|n| ctx.metrics.get(n).map_or(1.0, |m| m.weight))
            .sum();

        if total_weight <= 0.0 {
            return nodes.first();
        }

        // Use round-robin counter as pseudo-random
        let target = (ctx.round_robin_counter as f64 % total_weight).abs();
        let mut cumulative = 0.0;

        for node in nodes {
            cumulative += ctx.metrics.get(node).map_or(1.0, |m| m.weight);
            if cumulative > target {
                return Some(node);
            }
        }

        nodes.last()
    }
}

// ---------------------------------------------------------------------------
// LoadBalancer — main coordinator
// ---------------------------------------------------------------------------

/// Adaptive load balancer
pub struct LoadBalancer {
    strategy: Box<dyn RoutingStrategy>,
    nodes: Vec<NodeId>,
    metrics: HashMap<NodeId, NodeMetrics>,
    counter: AtomicU64,
    max_error_rate: f64,
    /// Total selections made
    total_selections: AtomicU64,
}

impl LoadBalancer {
    pub fn new(strategy: Box<dyn RoutingStrategy>) -> Self {
        Self {
            strategy,
            nodes: Vec::new(),
            metrics: HashMap::new(),
            counter: AtomicU64::new(0),
            max_error_rate: 0.5,
            total_selections: AtomicU64::new(0),
        }
    }

    pub fn with_max_error_rate(mut self, rate: f64) -> Self {
        self.max_error_rate = rate;
        self
    }

    /// Add a node
    pub fn add_node(&mut self, node_id: NodeId) {
        if !self.nodes.contains(&node_id) {
            self.metrics.insert(node_id.clone(), NodeMetrics::new(node_id.clone()));
            self.nodes.push(node_id);
        }
    }

    /// Remove a node
    pub fn remove_node(&mut self, node_id: &NodeId) -> bool {
        if let Some(pos) = self.nodes.iter().position(|n| n == node_id) {
            self.nodes.remove(pos);
            self.metrics.remove(node_id);
            true
        } else {
            false
        }
    }

    /// Select a node for routing
    pub fn select(&self, key: Option<&str>) -> Option<&NodeId> {
        let healthy: Vec<NodeId> = self.nodes.iter()
            .filter(|n| {
                self.metrics.get(*n).map_or(true, |m| m.is_healthy(self.max_error_rate))
            })
            .cloned()
            .collect();

        if healthy.is_empty() {
            return None;
        }

        let ctx = RoutingContext {
            metrics: self.metrics.clone(),
            round_robin_counter: self.counter.fetch_add(1, Ordering::Relaxed),
        };

        // Find in healthy nodes, then map back to self.nodes for lifetime
        let result = self.strategy.select(&healthy, key, &ctx)?;
        self.total_selections.fetch_add(1, Ordering::Relaxed);
        self.nodes.iter().find(|n| *n == result)
    }

    /// Record a successful request
    pub fn record_success(&mut self, node_id: &NodeId, latency: Duration) {
        if let Some(m) = self.metrics.get_mut(node_id) {
            m.total_requests += 1;
            let lat_ms = latency.as_secs_f64() * 1000.0;
            // Exponential moving average
            m.avg_latency_ms = m.avg_latency_ms * 0.9 + lat_ms * 0.1;
            m.last_updated = Instant::now();
        }
    }

    /// Record a failed request
    pub fn record_failure(&mut self, node_id: &NodeId) {
        if let Some(m) = self.metrics.get_mut(node_id) {
            m.total_requests += 1;
            m.total_errors += 1;
            m.last_updated = Instant::now();
        }
    }

    /// Update active connections for a node
    pub fn set_active_connections(&mut self, node_id: &NodeId, count: u64) {
        if let Some(m) = self.metrics.get_mut(node_id) {
            m.active_connections = count;
        }
    }

    /// Set weight for a node
    pub fn set_weight(&mut self, node_id: &NodeId, weight: f64) {
        if let Some(m) = self.metrics.get_mut(node_id) {
            m.weight = weight;
        }
    }

    /// Get metrics for a node
    pub fn node_metrics(&self, node_id: &NodeId) -> Option<&NodeMetrics> {
        self.metrics.get(node_id)
    }

    /// Get all node IDs
    pub fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }

    /// Get healthy nodes
    pub fn healthy_nodes(&self) -> Vec<&NodeId> {
        self.nodes.iter()
            .filter(|n| self.metrics.get(*n).map_or(true, |m| m.is_healthy(self.max_error_rate)))
            .collect()
    }

    /// Get strategy name
    pub fn strategy_name(&self) -> &str {
        self.strategy.name()
    }

    /// Total selections made
    pub fn total_selections(&self) -> u64 {
        self.total_selections.load(Ordering::Relaxed)
    }

    /// Replace the routing strategy
    pub fn set_strategy(&mut self, strategy: Box<dyn RoutingStrategy>) {
        self.strategy = strategy;
    }
}

// ---------------------------------------------------------------------------
// LoadBalancerBuilder
// ---------------------------------------------------------------------------

pub struct LoadBalancerBuilder {
    strategy: Box<dyn RoutingStrategy>,
    nodes: Vec<NodeId>,
    max_error_rate: f64,
    weights: HashMap<NodeId, f64>,
}

impl LoadBalancerBuilder {
    pub fn new() -> Self {
        Self {
            strategy: Box::new(RoundRobinStrategy),
            nodes: Vec::new(),
            max_error_rate: 0.5,
            weights: HashMap::new(),
        }
    }

    pub fn strategy(mut self, strategy: Box<dyn RoutingStrategy>) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn round_robin(self) -> Self {
        self.strategy(Box::new(RoundRobinStrategy))
    }

    pub fn random(self) -> Self {
        self.strategy(Box::new(RandomStrategy::new()))
    }

    pub fn consistent_hash(self, virtual_nodes: usize) -> Self {
        self.strategy(Box::new(ConsistentHashStrategy::new(virtual_nodes)))
    }

    pub fn least_load(self) -> Self {
        self.strategy(Box::new(LeastLoadStrategy))
    }

    pub fn weighted(self) -> Self {
        self.strategy(Box::new(WeightedStrategy))
    }

    pub fn node(mut self, id: impl Into<String>) -> Self {
        self.nodes.push(NodeId::new(id));
        self
    }

    pub fn node_with_weight(mut self, id: impl Into<String>, weight: f64) -> Self {
        let node = NodeId::new(id);
        self.weights.insert(node.clone(), weight);
        self.nodes.push(node);
        self
    }

    pub fn max_error_rate(mut self, rate: f64) -> Self {
        self.max_error_rate = rate;
        self
    }

    pub fn build(self) -> LoadBalancer {
        let mut lb = LoadBalancer::new(self.strategy)
            .with_max_error_rate(self.max_error_rate);
        for node in self.nodes {
            lb.add_node(node.clone());
            if let Some(&w) = self.weights.get(&node) {
                lb.set_weight(&node, w);
            }
        }
        lb
    }
}

impl Default for LoadBalancerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_robin() {
        let mut lb = LoadBalancerBuilder::new()
            .round_robin()
            .node("n1").node("n2").node("n3")
            .build();

        let s1 = lb.select(None).unwrap().clone();
        let s2 = lb.select(None).unwrap().clone();
        let s3 = lb.select(None).unwrap().clone();
        let s4 = lb.select(None).unwrap().clone();

        // Should cycle
        assert_eq!(s1, s4);
        assert_ne!(s1, s2);
    }

    #[test]
    fn test_random_strategy() {
        let lb = LoadBalancerBuilder::new()
            .random()
            .node("n1").node("n2")
            .build();

        // Should always return a valid node
        for _ in 0..20 {
            assert!(lb.select(None).is_some());
        }
    }

    #[test]
    fn test_consistent_hash() {
        let lb = LoadBalancerBuilder::new()
            .consistent_hash(100)
            .node("n1").node("n2").node("n3")
            .build();

        // Same key → same node
        let s1 = lb.select(Some("user:123")).unwrap().clone();
        let s2 = lb.select(Some("user:123")).unwrap().clone();
        assert_eq!(s1, s2);

        // Different key may pick different node (not guaranteed)
        let _s3 = lb.select(Some("user:456")).unwrap();
    }

    #[test]
    fn test_least_load() {
        let mut lb = LoadBalancerBuilder::new()
            .least_load()
            .node("n1").node("n2")
            .build();

        // Make n1 loaded
        lb.set_active_connections(&NodeId::new("n1"), 100);

        let selected = lb.select(None).unwrap().clone();
        assert_eq!(selected, NodeId::new("n2"));
    }

    #[test]
    fn test_weighted_strategy() {
        let lb = LoadBalancerBuilder::new()
            .weighted()
            .node_with_weight("n1", 10.0)
            .node_with_weight("n2", 1.0)
            .build();

        let mut counts: HashMap<String, usize> = HashMap::new();
        for _ in 0..100 {
            let n = lb.select(None).unwrap();
            *counts.entry(n.as_str().to_string()).or_default() += 1;
        }

        // n1 should get significantly more selections
        assert!(counts.get("n1").copied().unwrap_or(0) > counts.get("n2").copied().unwrap_or(0));
    }

    #[test]
    fn test_empty_nodes() {
        let lb = LoadBalancer::new(Box::new(RoundRobinStrategy));
        assert!(lb.select(None).is_none());
    }

    #[test]
    fn test_add_remove_nodes() {
        let mut lb = LoadBalancer::new(Box::new(RoundRobinStrategy));
        lb.add_node(NodeId::new("n1"));
        lb.add_node(NodeId::new("n2"));
        assert_eq!(lb.nodes().len(), 2);

        lb.remove_node(&NodeId::new("n1"));
        assert_eq!(lb.nodes().len(), 1);
        assert_eq!(lb.nodes()[0], NodeId::new("n2"));
    }

    #[test]
    fn test_duplicate_add() {
        let mut lb = LoadBalancer::new(Box::new(RoundRobinStrategy));
        lb.add_node(NodeId::new("n1"));
        lb.add_node(NodeId::new("n1")); // Duplicate
        assert_eq!(lb.nodes().len(), 1);
    }

    #[test]
    fn test_record_success() {
        let mut lb = LoadBalancerBuilder::new()
            .node("n1")
            .build();

        lb.record_success(&NodeId::new("n1"), Duration::from_millis(50));
        lb.record_success(&NodeId::new("n1"), Duration::from_millis(100));

        let m = lb.node_metrics(&NodeId::new("n1")).unwrap();
        assert_eq!(m.total_requests, 2);
        assert_eq!(m.total_errors, 0);
        assert!(m.avg_latency_ms > 0.0);
    }

    #[test]
    fn test_record_failure() {
        let mut lb = LoadBalancerBuilder::new()
            .node("n1")
            .build();

        lb.record_success(&NodeId::new("n1"), Duration::from_millis(10));
        lb.record_failure(&NodeId::new("n1"));

        let m = lb.node_metrics(&NodeId::new("n1")).unwrap();
        assert_eq!(m.total_requests, 2);
        assert_eq!(m.total_errors, 1);
        assert!((m.error_rate() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_unhealthy_node_excluded() {
        let mut lb = LoadBalancerBuilder::new()
            .round_robin()
            .max_error_rate(0.3)
            .node("n1").node("n2")
            .build();

        // Make n1 unhealthy
        for _ in 0..10 {
            lb.record_failure(&NodeId::new("n1"));
        }

        // Should only route to n2
        for _ in 0..10 {
            let selected = lb.select(None).unwrap().clone();
            assert_eq!(selected, NodeId::new("n2"));
        }
    }

    #[test]
    fn test_all_nodes_unhealthy() {
        let mut lb = LoadBalancerBuilder::new()
            .max_error_rate(0.1)
            .node("n1")
            .build();

        for _ in 0..10 {
            lb.record_failure(&NodeId::new("n1"));
        }

        // All unhealthy → no selection
        assert!(lb.select(None).is_none());
    }

    #[test]
    fn test_healthy_nodes() {
        let mut lb = LoadBalancerBuilder::new()
            .max_error_rate(0.3)
            .node("n1").node("n2").node("n3")
            .build();

        for _ in 0..10 {
            lb.record_failure(&NodeId::new("n2"));
        }

        let healthy = lb.healthy_nodes();
        assert_eq!(healthy.len(), 2);
        assert!(!healthy.contains(&&NodeId::new("n2")));
    }

    #[test]
    fn test_strategy_name() {
        let lb = LoadBalancerBuilder::new().round_robin().build();
        assert_eq!(lb.strategy_name(), "round-robin");
    }

    #[test]
    fn test_set_strategy() {
        let mut lb = LoadBalancerBuilder::new()
            .round_robin()
            .node("n1")
            .build();

        assert_eq!(lb.strategy_name(), "round-robin");
        lb.set_strategy(Box::new(LeastLoadStrategy));
        assert_eq!(lb.strategy_name(), "least-load");
    }

    #[test]
    fn test_total_selections() {
        let lb = LoadBalancerBuilder::new()
            .node("n1")
            .build();

        lb.select(None);
        lb.select(None);
        lb.select(None);
        assert_eq!(lb.total_selections(), 3);
    }

    #[test]
    fn test_node_metrics_load_score() {
        let mut m = NodeMetrics::new(NodeId::new("n"));
        assert_eq!(m.load_score(), 0.0);

        m.avg_latency_ms = 200.0;
        m.active_connections = 50;
        assert!(m.load_score() > 0.0);
    }
}

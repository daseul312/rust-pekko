//! Remote Deployment - Deploy actors on remote nodes
//!
//! Provides patterns for deploying actors to remote cluster nodes.
//! Supports scope-based deployment, deployment strategies (round-robin, load-based, affinity),
//! and remote props configuration with resource requirements.

use std::collections::HashMap;
use std::fmt;
use std::time::Instant;

// =============================================================================
// Core Types
// =============================================================================

/// Errors for remote deployment
#[derive(Debug, Clone, PartialEq)]
pub enum DeploymentError {
    /// No suitable node available
    NoSuitableNode(String),
    /// Node unreachable
    NodeUnreachable(String),
    /// Deployment failed on remote node
    DeploymentFailed { node: String, reason: String },
    /// Configuration error
    InvalidConfig(String),
    /// Resource requirements not met
    InsufficientResources { node: String, required: String },
    /// Deployment limit reached
    LimitReached { node: String, current: usize, max: usize },
    /// Scope not found
    ScopeNotFound(String),
}

impl fmt::Display for DeploymentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeploymentError::NoSuitableNode(r) => write!(f, "No suitable node: {}", r),
            DeploymentError::NodeUnreachable(n) => write!(f, "Node unreachable: {}", n),
            DeploymentError::DeploymentFailed { node, reason } => {
                write!(f, "Deployment failed on {}: {}", node, reason)
            }
            DeploymentError::InvalidConfig(e) => write!(f, "Invalid config: {}", e),
            DeploymentError::InsufficientResources { node, required } => {
                write!(f, "Insufficient resources on {}: needs {}", node, required)
            }
            DeploymentError::LimitReached { node, current, max } => {
                write!(f, "Limit reached on {}: {}/{}", node, current, max)
            }
            DeploymentError::ScopeNotFound(s) => write!(f, "Scope not found: {}", s),
        }
    }
}

/// Resource requirements for deployment
#[derive(Debug, Clone, PartialEq)]
pub struct ResourceRequirements {
    /// Minimum available memory (bytes)
    pub min_memory: Option<u64>,
    /// Maximum CPU usage (0.0-1.0)
    pub max_cpu_usage: Option<f64>,
    /// Minimum processors
    pub min_processors: Option<u32>,
    /// Custom requirements (key-value)
    pub custom: HashMap<String, String>,
}

impl Default for ResourceRequirements {
    fn default() -> Self {
        Self {
            min_memory: None,
            max_cpu_usage: None,
            min_processors: None,
            custom: HashMap::new(),
        }
    }
}

impl ResourceRequirements {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_min_memory(mut self, bytes: u64) -> Self {
        self.min_memory = Some(bytes);
        self
    }

    pub fn with_max_cpu(mut self, usage: f64) -> Self {
        self.max_cpu_usage = Some(usage.clamp(0.0, 1.0));
        self
    }

    pub fn with_min_processors(mut self, count: u32) -> Self {
        self.min_processors = Some(count);
        self
    }

    pub fn with_custom(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.custom.insert(key.into(), value.into());
        self
    }

    /// Check if requirements are satisfied by node capabilities
    pub fn satisfied_by(&self, capabilities: &NodeCapabilities) -> bool {
        if let Some(min_mem) = self.min_memory {
            if capabilities.available_memory < min_mem {
                return false;
            }
        }
        if let Some(max_cpu) = self.max_cpu_usage {
            if capabilities.cpu_usage > max_cpu {
                return false;
            }
        }
        if let Some(min_proc) = self.min_processors {
            if capabilities.processors < min_proc {
                return false;
            }
        }
        for (key, required_val) in &self.custom {
            match capabilities.custom.get(key) {
                Some(val) if val == required_val => {}
                _ => return false,
            }
        }
        true
    }
}

/// Capabilities of a remote node
#[derive(Debug, Clone)]
pub struct NodeCapabilities {
    /// Node identifier
    pub node_id: String,
    /// Available memory (bytes)
    pub available_memory: u64,
    /// Current CPU usage (0.0-1.0)
    pub cpu_usage: f64,
    /// Number of processors
    pub processors: u32,
    /// Currently deployed actor count
    pub deployed_count: usize,
    /// Maximum deployments allowed
    pub max_deployments: usize,
    /// Node roles/tags
    pub roles: Vec<String>,
    /// Custom capabilities
    pub custom: HashMap<String, String>,
    /// Last updated
    pub updated_at: Instant,
}

impl NodeCapabilities {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            available_memory: 0,
            cpu_usage: 0.0,
            processors: 1,
            deployed_count: 0,
            max_deployments: 100,
            roles: Vec::new(),
            custom: HashMap::new(),
            updated_at: Instant::now(),
        }
    }

    pub fn with_memory(mut self, available: u64) -> Self {
        self.available_memory = available;
        self
    }

    pub fn with_cpu(mut self, usage: f64) -> Self {
        self.cpu_usage = usage.clamp(0.0, 1.0);
        self
    }

    pub fn with_processors(mut self, count: u32) -> Self {
        self.processors = count;
        self
    }

    pub fn with_max_deployments(mut self, max: usize) -> Self {
        self.max_deployments = max;
        self
    }

    pub fn with_roles(mut self, roles: Vec<String>) -> Self {
        self.roles = roles;
        self
    }

    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }

    pub fn has_capacity(&self) -> bool {
        self.deployed_count < self.max_deployments
    }

    pub fn remaining_capacity(&self) -> usize {
        self.max_deployments.saturating_sub(self.deployed_count)
    }
}

// =============================================================================
// Deployment Scope
// =============================================================================

/// Scope that defines where actors can be deployed
#[derive(Debug, Clone)]
pub struct RemoteScope {
    /// Scope name
    pub name: String,
    /// Allowed node roles (empty = any)
    pub allowed_roles: Vec<String>,
    /// Excluded nodes
    pub excluded_nodes: Vec<String>,
    /// Preferred nodes (tried first)
    pub preferred_nodes: Vec<String>,
    /// Resource requirements
    pub requirements: ResourceRequirements,
    /// Maximum deployments per node in this scope
    pub max_per_node: Option<usize>,
    /// Whether to allow co-location with other scopes
    pub allow_colocation: bool,
}

impl RemoteScope {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            allowed_roles: Vec::new(),
            excluded_nodes: Vec::new(),
            preferred_nodes: Vec::new(),
            requirements: ResourceRequirements::default(),
            max_per_node: None,
            allow_colocation: true,
        }
    }

    pub fn with_roles(mut self, roles: Vec<String>) -> Self {
        self.allowed_roles = roles;
        self
    }

    pub fn with_excluded(mut self, nodes: Vec<String>) -> Self {
        self.excluded_nodes = nodes;
        self
    }

    pub fn with_preferred(mut self, nodes: Vec<String>) -> Self {
        self.preferred_nodes = nodes;
        self
    }

    pub fn with_requirements(mut self, reqs: ResourceRequirements) -> Self {
        self.requirements = reqs;
        self
    }

    pub fn with_max_per_node(mut self, max: usize) -> Self {
        self.max_per_node = Some(max);
        self
    }

    /// Check if a node is eligible for this scope
    pub fn is_eligible(&self, node: &NodeCapabilities) -> bool {
        // Check exclusions
        if self.excluded_nodes.contains(&node.node_id) {
            return false;
        }

        // Check roles
        if !self.allowed_roles.is_empty() {
            if !self.allowed_roles.iter().any(|r| node.has_role(r)) {
                return false;
            }
        }

        // Check capacity
        if !node.has_capacity() {
            return false;
        }

        // Check resource requirements
        self.requirements.satisfied_by(node)
    }

    /// Check if a node is preferred
    pub fn is_preferred(&self, node_id: &str) -> bool {
        self.preferred_nodes.contains(&node_id.to_string())
    }
}

// =============================================================================
// Deployment Strategy
// =============================================================================

/// Strategy for selecting which node to deploy on
#[derive(Debug, Clone, PartialEq)]
pub enum DeploymentStrategyType {
    /// Deploy to the node with the least actors
    LeastLoaded,
    /// Round-robin across eligible nodes
    RoundRobin,
    /// Deploy to a specific node
    Specific(String),
    /// Deploy based on affinity (same node as related actors)
    Affinity { group: String },
    /// Deploy to the node with the most available resources
    ResourceBased,
    /// Random selection among eligible nodes
    Random,
}

impl fmt::Display for DeploymentStrategyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeploymentStrategyType::LeastLoaded => write!(f, "LeastLoaded"),
            DeploymentStrategyType::RoundRobin => write!(f, "RoundRobin"),
            DeploymentStrategyType::Specific(n) => write!(f, "Specific({})", n),
            DeploymentStrategyType::Affinity { group } => write!(f, "Affinity({})", group),
            DeploymentStrategyType::ResourceBased => write!(f, "ResourceBased"),
            DeploymentStrategyType::Random => write!(f, "Random"),
        }
    }
}

/// Deployment strategy with state
#[derive(Debug)]
pub struct DeploymentStrategy {
    /// Strategy type
    strategy_type: DeploymentStrategyType,
    /// Round-robin counter
    rr_counter: usize,
    /// Affinity mapping (group → node)
    affinity_map: HashMap<String, String>,
}

impl DeploymentStrategy {
    pub fn least_loaded() -> Self {
        Self {
            strategy_type: DeploymentStrategyType::LeastLoaded,
            rr_counter: 0,
            affinity_map: HashMap::new(),
        }
    }

    pub fn round_robin() -> Self {
        Self {
            strategy_type: DeploymentStrategyType::RoundRobin,
            rr_counter: 0,
            affinity_map: HashMap::new(),
        }
    }

    pub fn specific(node: impl Into<String>) -> Self {
        Self {
            strategy_type: DeploymentStrategyType::Specific(node.into()),
            rr_counter: 0,
            affinity_map: HashMap::new(),
        }
    }

    pub fn affinity(group: impl Into<String>) -> Self {
        Self {
            strategy_type: DeploymentStrategyType::Affinity { group: group.into() },
            rr_counter: 0,
            affinity_map: HashMap::new(),
        }
    }

    pub fn resource_based() -> Self {
        Self {
            strategy_type: DeploymentStrategyType::ResourceBased,
            rr_counter: 0,
            affinity_map: HashMap::new(),
        }
    }

    pub fn random() -> Self {
        Self {
            strategy_type: DeploymentStrategyType::Random,
            rr_counter: 0,
            affinity_map: HashMap::new(),
        }
    }

    /// Select a node for deployment
    pub fn select<'a>(
        &mut self,
        eligible: &'a [NodeCapabilities],
    ) -> Result<&'a NodeCapabilities, DeploymentError> {
        if eligible.is_empty() {
            return Err(DeploymentError::NoSuitableNode("No eligible nodes".into()));
        }

        match &self.strategy_type {
            DeploymentStrategyType::LeastLoaded => {
                eligible.iter()
                    .min_by_key(|n| n.deployed_count)
                    .ok_or_else(|| DeploymentError::NoSuitableNode("No nodes".into()))
            }

            DeploymentStrategyType::RoundRobin => {
                let idx = self.rr_counter % eligible.len();
                self.rr_counter += 1;
                Ok(&eligible[idx])
            }

            DeploymentStrategyType::Specific(target) => {
                eligible.iter()
                    .find(|n| n.node_id == *target)
                    .ok_or_else(|| DeploymentError::NodeUnreachable(target.clone()))
            }

            DeploymentStrategyType::Affinity { group } => {
                // Check if we have a node assigned for this group
                if let Some(node_id) = self.affinity_map.get(group) {
                    if let Some(node) = eligible.iter().find(|n| n.node_id == *node_id) {
                        return Ok(node);
                    }
                }
                // Assign to least loaded
                let selected = eligible.iter()
                    .min_by_key(|n| n.deployed_count)
                    .ok_or_else(|| DeploymentError::NoSuitableNode("No nodes".into()))?;
                self.affinity_map.insert(group.clone(), selected.node_id.clone());
                Ok(selected)
            }

            DeploymentStrategyType::ResourceBased => {
                eligible.iter()
                    .max_by(|a, b| {
                        let score_a = a.available_memory as f64 * (1.0 - a.cpu_usage);
                        let score_b = b.available_memory as f64 * (1.0 - b.cpu_usage);
                        score_a.partial_cmp(&score_b).unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .ok_or_else(|| DeploymentError::NoSuitableNode("No nodes".into()))
            }

            DeploymentStrategyType::Random => {
                // Deterministic "random" based on counter
                let idx = self.rr_counter % eligible.len();
                self.rr_counter = self.rr_counter.wrapping_add(7); // Step by prime
                Ok(&eligible[idx])
            }
        }
    }

    pub fn strategy_type(&self) -> &DeploymentStrategyType {
        &self.strategy_type
    }
}

// =============================================================================
// Remote Props
// =============================================================================

/// Configuration for deploying an actor remotely
#[derive(Debug, Clone)]
pub struct RemoteProps {
    /// Actor type identifier
    pub actor_type: String,
    /// Deployment scope
    pub scope: Option<String>,
    /// Resource requirements
    pub requirements: ResourceRequirements,
    /// Deployment strategy override
    pub strategy: Option<DeploymentStrategyType>,
    /// Dispatcher hint
    pub dispatcher: Option<String>,
    /// Mailbox capacity
    pub mailbox_capacity: Option<usize>,
    /// Supervision strategy
    pub supervision: Option<String>,
    /// Custom configuration
    pub config: HashMap<String, String>,
}

impl RemoteProps {
    pub fn new(actor_type: impl Into<String>) -> Self {
        Self {
            actor_type: actor_type.into(),
            scope: None,
            requirements: ResourceRequirements::default(),
            strategy: None,
            dispatcher: None,
            mailbox_capacity: None,
            supervision: None,
            config: HashMap::new(),
        }
    }

    pub fn with_scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }

    pub fn with_requirements(mut self, reqs: ResourceRequirements) -> Self {
        self.requirements = reqs;
        self
    }

    pub fn with_strategy(mut self, strategy: DeploymentStrategyType) -> Self {
        self.strategy = Some(strategy);
        self
    }

    pub fn with_dispatcher(mut self, dispatcher: impl Into<String>) -> Self {
        self.dispatcher = Some(dispatcher.into());
        self
    }

    pub fn with_mailbox_capacity(mut self, capacity: usize) -> Self {
        self.mailbox_capacity = Some(capacity);
        self
    }

    pub fn with_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.insert(key.into(), value.into());
        self
    }
}

// =============================================================================
// Remote Deployer
// =============================================================================

/// Tracks a deployed actor
#[derive(Debug, Clone)]
pub struct DeployedActor {
    /// Actor identifier
    pub actor_id: String,
    /// Actor type
    pub actor_type: String,
    /// Node it's deployed on
    pub node_id: String,
    /// Scope name
    pub scope: Option<String>,
    /// When deployed
    pub deployed_at: Instant,
    /// Props used
    pub props: RemoteProps,
}

/// Manages remote actor deployments across the cluster
#[derive(Debug)]
pub struct RemoteDeployer {
    /// Available scopes
    scopes: HashMap<String, RemoteScope>,
    /// Default strategy
    default_strategy: DeploymentStrategy,
    /// Known node capabilities
    node_capabilities: HashMap<String, NodeCapabilities>,
    /// Deployed actors
    deployed: Vec<DeployedActor>,
    /// Deployment counter for IDs
    deploy_counter: u64,
    /// Deployment history
    history: Vec<DeploymentEvent>,
    /// Max history size
    max_history: usize,
}

/// Record of a deployment event
#[derive(Debug, Clone)]
pub struct DeploymentEvent {
    pub event_type: DeploymentEventType,
    pub actor_id: String,
    pub node_id: String,
    pub timestamp: Instant,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DeploymentEventType {
    Deployed,
    Undeployed,
    Migrated { from: String },
    Failed(String),
}

impl RemoteDeployer {
    pub fn new() -> Self {
        Self {
            scopes: HashMap::new(),
            default_strategy: DeploymentStrategy::least_loaded(),
            node_capabilities: HashMap::new(),
            deployed: Vec::new(),
            deploy_counter: 0,
            history: Vec::new(),
            max_history: 10000,
        }
    }

    pub fn with_default_strategy(mut self, strategy: DeploymentStrategy) -> Self {
        self.default_strategy = strategy;
        self
    }

    /// Register a deployment scope
    pub fn register_scope(&mut self, scope: RemoteScope) {
        self.scopes.insert(scope.name.clone(), scope);
    }

    /// Update node capabilities
    pub fn update_node(&mut self, capabilities: NodeCapabilities) {
        self.node_capabilities.insert(capabilities.node_id.clone(), capabilities);
    }

    /// Remove a node
    pub fn remove_node(&mut self, node_id: &str) {
        self.node_capabilities.remove(node_id);
    }

    /// Deploy an actor
    pub fn deploy(&mut self, props: RemoteProps) -> Result<DeployedActor, DeploymentError> {
        // Determine eligible nodes
        let eligible = self.find_eligible_nodes(&props)?;

        if eligible.is_empty() {
            return Err(DeploymentError::NoSuitableNode(
                format!("No nodes satisfy requirements for {}", props.actor_type)
            ));
        }

        // Select node using strategy
        let selected_id = {
            let selected = self.default_strategy.select(&eligible)?;
            selected.node_id.clone()
        };

        // Create deployment record
        self.deploy_counter += 1;
        let actor_id = format!("actor-{}-{}", props.actor_type, self.deploy_counter);

        let deployed = DeployedActor {
            actor_id: actor_id.clone(),
            actor_type: props.actor_type.clone(),
            node_id: selected_id.clone(),
            scope: props.scope.clone(),
            deployed_at: Instant::now(),
            props,
        };

        // Update node's deployed count
        if let Some(cap) = self.node_capabilities.get_mut(&selected_id) {
            cap.deployed_count += 1;
        }

        self.deployed.push(deployed.clone());
        self.record_event(DeploymentEvent {
            event_type: DeploymentEventType::Deployed,
            actor_id,
            node_id: selected_id,
            timestamp: Instant::now(),
        });

        Ok(deployed)
    }

    /// Undeploy an actor
    pub fn undeploy(&mut self, actor_id: &str) -> Option<DeployedActor> {
        if let Some(pos) = self.deployed.iter().position(|d| d.actor_id == actor_id) {
            let removed = self.deployed.remove(pos);

            // Update node's deployed count
            if let Some(cap) = self.node_capabilities.get_mut(&removed.node_id) {
                cap.deployed_count = cap.deployed_count.saturating_sub(1);
            }

            self.record_event(DeploymentEvent {
                event_type: DeploymentEventType::Undeployed,
                actor_id: actor_id.to_string(),
                node_id: removed.node_id.clone(),
                timestamp: Instant::now(),
            });

            Some(removed)
        } else {
            None
        }
    }

    /// Migrate an actor to a different node
    pub fn migrate(&mut self, actor_id: &str, target_node: &str) -> Result<DeployedActor, DeploymentError> {
        let target_cap = self.node_capabilities.get(target_node)
            .ok_or_else(|| DeploymentError::NodeUnreachable(target_node.into()))?
            .clone();

        if !target_cap.has_capacity() {
            return Err(DeploymentError::LimitReached {
                node: target_node.into(),
                current: target_cap.deployed_count,
                max: target_cap.max_deployments,
            });
        }

        // Find and update the deployment
        let deployed = self.deployed.iter_mut()
            .find(|d| d.actor_id == actor_id)
            .ok_or_else(|| DeploymentError::DeploymentFailed {
                node: target_node.into(),
                reason: format!("Actor {} not found", actor_id),
            })?;

        let old_node = deployed.node_id.clone();

        // Update node counts
        if let Some(cap) = self.node_capabilities.get_mut(&old_node) {
            cap.deployed_count = cap.deployed_count.saturating_sub(1);
        }
        if let Some(cap) = self.node_capabilities.get_mut(target_node) {
            cap.deployed_count += 1;
        }

        deployed.node_id = target_node.to_string();
        deployed.deployed_at = Instant::now();
        let result = deployed.clone();

        self.record_event(DeploymentEvent {
            event_type: DeploymentEventType::Migrated { from: old_node },
            actor_id: actor_id.to_string(),
            node_id: target_node.to_string(),
            timestamp: Instant::now(),
        });

        Ok(result)
    }

    /// Find eligible nodes for deployment
    fn find_eligible_nodes(&self, props: &RemoteProps) -> Result<Vec<NodeCapabilities>, DeploymentError> {
        let mut eligible: Vec<NodeCapabilities> = if let Some(scope_name) = &props.scope {
            let scope = self.scopes.get(scope_name)
                .ok_or_else(|| DeploymentError::ScopeNotFound(scope_name.clone()))?;

            self.node_capabilities.values()
                .filter(|n| scope.is_eligible(n) && props.requirements.satisfied_by(n))
                .cloned()
                .collect()
        } else {
            self.node_capabilities.values()
                .filter(|n| n.has_capacity() && props.requirements.satisfied_by(n))
                .cloned()
                .collect()
        };

        // Sort preferred nodes first (if scope defines them)
        if let Some(scope_name) = &props.scope {
            if let Some(scope) = self.scopes.get(scope_name) {
                eligible.sort_by(|a, b| {
                    let a_pref = scope.is_preferred(&a.node_id);
                    let b_pref = scope.is_preferred(&b.node_id);
                    b_pref.cmp(&a_pref)
                });
            }
        }

        Ok(eligible)
    }

    /// Get all deployed actors
    pub fn deployed_actors(&self) -> &[DeployedActor] {
        &self.deployed
    }

    /// Get actors deployed on a specific node
    pub fn actors_on_node(&self, node_id: &str) -> Vec<&DeployedActor> {
        self.deployed.iter().filter(|d| d.node_id == node_id).collect()
    }

    /// Get actors of a specific type
    pub fn actors_of_type(&self, actor_type: &str) -> Vec<&DeployedActor> {
        self.deployed.iter().filter(|d| d.actor_type == actor_type).collect()
    }

    /// Number of deployed actors
    pub fn deployed_count(&self) -> usize {
        self.deployed.len()
    }

    /// Number of known nodes
    pub fn node_count(&self) -> usize {
        self.node_capabilities.len()
    }

    /// Get deployment statistics
    pub fn stats(&self) -> DeployerStats {
        let mut per_node: HashMap<String, usize> = HashMap::new();
        let mut per_type: HashMap<String, usize> = HashMap::new();
        for d in &self.deployed {
            *per_node.entry(d.node_id.clone()).or_insert(0) += 1;
            *per_type.entry(d.actor_type.clone()).or_insert(0) += 1;
        }

        DeployerStats {
            total_deployed: self.deployed.len(),
            total_nodes: self.node_capabilities.len(),
            total_scopes: self.scopes.len(),
            per_node,
            per_type,
            total_events: self.history.len(),
        }
    }

    fn record_event(&mut self, event: DeploymentEvent) {
        if self.history.len() >= self.max_history {
            self.history.drain(..self.max_history / 2);
        }
        self.history.push(event);
    }
}

impl Default for RemoteDeployer {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for the remote deployer
#[derive(Debug, Clone)]
pub struct DeployerStats {
    pub total_deployed: usize,
    pub total_nodes: usize,
    pub total_scopes: usize,
    pub per_node: HashMap<String, usize>,
    pub per_type: HashMap<String, usize>,
    pub total_events: usize,
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: &str, cpu: f64, mem: u64, _deployed: usize) -> NodeCapabilities {
        NodeCapabilities::new(id)
            .with_cpu(cpu)
            .with_memory(mem)
            .with_processors(4)
            .with_max_deployments(10)
    }

    fn make_node_with_deployed(id: &str, cpu: f64, mem: u64, deployed: usize) -> NodeCapabilities {
        let mut n = NodeCapabilities::new(id)
            .with_cpu(cpu)
            .with_memory(mem)
            .with_processors(4)
            .with_max_deployments(10);
        n.deployed_count = deployed;
        n
    }

    // ResourceRequirements Tests
    #[test]
    fn test_resource_requirements_default() {
        let req = ResourceRequirements::default();
        assert!(req.min_memory.is_none());
        assert!(req.max_cpu_usage.is_none());
    }

    #[test]
    fn test_resource_requirements_builder() {
        let req = ResourceRequirements::new()
            .with_min_memory(1024)
            .with_max_cpu(0.8)
            .with_min_processors(2)
            .with_custom("gpu", "true");

        assert_eq!(req.min_memory, Some(1024));
        assert_eq!(req.max_cpu_usage, Some(0.8));
        assert_eq!(req.min_processors, Some(2));
        assert_eq!(req.custom.get("gpu"), Some(&"true".to_string()));
    }

    #[test]
    fn test_resource_satisfied() {
        let req = ResourceRequirements::new()
            .with_min_memory(1000)
            .with_max_cpu(0.8);

        let cap = NodeCapabilities::new("n1")
            .with_memory(2000)
            .with_cpu(0.5);
        assert!(req.satisfied_by(&cap));
    }

    #[test]
    fn test_resource_not_satisfied_memory() {
        let req = ResourceRequirements::new().with_min_memory(5000);
        let cap = NodeCapabilities::new("n1").with_memory(1000);
        assert!(!req.satisfied_by(&cap));
    }

    #[test]
    fn test_resource_not_satisfied_cpu() {
        let req = ResourceRequirements::new().with_max_cpu(0.5);
        let cap = NodeCapabilities::new("n1").with_cpu(0.9);
        assert!(!req.satisfied_by(&cap));
    }

    #[test]
    fn test_resource_custom_requirement() {
        let req = ResourceRequirements::new().with_custom("gpu", "true");
        let mut cap = NodeCapabilities::new("n1");
        assert!(!req.satisfied_by(&cap));

        cap.custom.insert("gpu".to_string(), "true".to_string());
        assert!(req.satisfied_by(&cap));
    }

    // NodeCapabilities Tests
    #[test]
    fn test_node_capabilities_new() {
        let nc = NodeCapabilities::new("node1");
        assert_eq!(nc.node_id, "node1");
        assert!(nc.has_capacity());
    }

    #[test]
    fn test_node_capabilities_roles() {
        let nc = NodeCapabilities::new("n1")
            .with_roles(vec!["worker".into(), "compute".into()]);
        assert!(nc.has_role("worker"));
        assert!(!nc.has_role("storage"));
    }

    #[test]
    fn test_node_capabilities_capacity() {
        let mut nc = NodeCapabilities::new("n1").with_max_deployments(2);
        nc.deployed_count = 1;
        assert!(nc.has_capacity());
        assert_eq!(nc.remaining_capacity(), 1);

        nc.deployed_count = 2;
        assert!(!nc.has_capacity());
        assert_eq!(nc.remaining_capacity(), 0);
    }

    // RemoteScope Tests
    #[test]
    fn test_scope_new() {
        let scope = RemoteScope::new("compute");
        assert_eq!(scope.name, "compute");
        assert!(scope.allow_colocation);
    }

    #[test]
    fn test_scope_eligible_basic() {
        let scope = RemoteScope::new("compute");
        let cap = make_node("n1", 0.5, 2000, 0);
        assert!(scope.is_eligible(&cap));
    }

    #[test]
    fn test_scope_eligible_with_roles() {
        let scope = RemoteScope::new("compute")
            .with_roles(vec!["worker".into()]);

        let cap1 = NodeCapabilities::new("n1")
            .with_roles(vec!["worker".into()])
            .with_max_deployments(10);
        assert!(scope.is_eligible(&cap1));

        let cap2 = NodeCapabilities::new("n2")
            .with_roles(vec!["frontend".into()])
            .with_max_deployments(10);
        assert!(!scope.is_eligible(&cap2));
    }

    #[test]
    fn test_scope_excluded_node() {
        let scope = RemoteScope::new("compute")
            .with_excluded(vec!["bad-node".into()]);

        let cap = make_node("bad-node", 0.1, 10000, 0);
        assert!(!scope.is_eligible(&cap));

        let cap2 = make_node("good-node", 0.1, 10000, 0);
        assert!(scope.is_eligible(&cap2));
    }

    #[test]
    fn test_scope_with_requirements() {
        let scope = RemoteScope::new("compute")
            .with_requirements(ResourceRequirements::new().with_min_memory(5000));

        let cap1 = make_node("n1", 0.5, 10000, 0);
        assert!(scope.is_eligible(&cap1));

        let cap2 = make_node("n2", 0.5, 1000, 0);
        assert!(!scope.is_eligible(&cap2));
    }

    #[test]
    fn test_scope_preferred() {
        let scope = RemoteScope::new("compute")
            .with_preferred(vec!["preferred-node".into()]);
        assert!(scope.is_preferred("preferred-node"));
        assert!(!scope.is_preferred("other-node"));
    }

    // DeploymentStrategy Tests
    #[test]
    fn test_strategy_least_loaded() {
        let mut strategy = DeploymentStrategy::least_loaded();
        let nodes = vec![
            make_node_with_deployed("n1", 0.5, 2000, 5),
            make_node_with_deployed("n2", 0.5, 2000, 2),
            make_node_with_deployed("n3", 0.5, 2000, 8),
        ];

        let selected = strategy.select(&nodes).unwrap();
        assert_eq!(selected.node_id, "n2"); // Least loaded
    }

    #[test]
    fn test_strategy_round_robin() {
        let mut strategy = DeploymentStrategy::round_robin();
        let nodes = vec![
            make_node("n1", 0.5, 2000, 0),
            make_node("n2", 0.5, 2000, 0),
            make_node("n3", 0.5, 2000, 0),
        ];

        assert_eq!(strategy.select(&nodes).unwrap().node_id, "n1");
        assert_eq!(strategy.select(&nodes).unwrap().node_id, "n2");
        assert_eq!(strategy.select(&nodes).unwrap().node_id, "n3");
        assert_eq!(strategy.select(&nodes).unwrap().node_id, "n1"); // Wraps
    }

    #[test]
    fn test_strategy_specific() {
        let mut strategy = DeploymentStrategy::specific("n2");
        let nodes = vec![
            make_node("n1", 0.5, 2000, 0),
            make_node("n2", 0.5, 2000, 0),
        ];

        let selected = strategy.select(&nodes).unwrap();
        assert_eq!(selected.node_id, "n2");
    }

    #[test]
    fn test_strategy_specific_not_found() {
        let mut strategy = DeploymentStrategy::specific("n99");
        let nodes = vec![make_node("n1", 0.5, 2000, 0)];
        assert!(strategy.select(&nodes).is_err());
    }

    #[test]
    fn test_strategy_affinity() {
        let mut strategy = DeploymentStrategy::affinity("group1");
        let nodes = vec![
            make_node_with_deployed("n1", 0.5, 2000, 5),
            make_node_with_deployed("n2", 0.5, 2000, 2),
        ];

        // First select assigns to least loaded
        let s1 = strategy.select(&nodes).unwrap();
        let assigned = s1.node_id.clone();

        // Second select returns same node (affinity)
        let s2 = strategy.select(&nodes).unwrap();
        assert_eq!(s2.node_id, assigned);
    }

    #[test]
    fn test_strategy_resource_based() {
        let mut strategy = DeploymentStrategy::resource_based();
        let nodes = vec![
            make_node("n1", 0.9, 1000, 0), // High CPU, low mem
            make_node("n2", 0.1, 10000, 0), // Low CPU, high mem (best)
        ];

        let selected = strategy.select(&nodes).unwrap();
        assert_eq!(selected.node_id, "n2");
    }

    #[test]
    fn test_strategy_empty_nodes() {
        let mut strategy = DeploymentStrategy::least_loaded();
        let nodes: Vec<NodeCapabilities> = vec![];
        assert!(strategy.select(&nodes).is_err());
    }

    #[test]
    fn test_strategy_type_display() {
        assert_eq!(DeploymentStrategyType::LeastLoaded.to_string(), "LeastLoaded");
        assert_eq!(DeploymentStrategyType::RoundRobin.to_string(), "RoundRobin");
    }

    // RemoteProps Tests
    #[test]
    fn test_remote_props_new() {
        let props = RemoteProps::new("worker-actor");
        assert_eq!(props.actor_type, "worker-actor");
        assert!(props.scope.is_none());
    }

    #[test]
    fn test_remote_props_builder() {
        let props = RemoteProps::new("worker")
            .with_scope("compute")
            .with_dispatcher("fork-join")
            .with_mailbox_capacity(1000)
            .with_config("key", "value");

        assert_eq!(props.scope, Some("compute".into()));
        assert_eq!(props.dispatcher, Some("fork-join".into()));
        assert_eq!(props.mailbox_capacity, Some(1000));
    }

    // RemoteDeployer Tests
    #[test]
    fn test_deployer_new() {
        let deployer = RemoteDeployer::new();
        assert_eq!(deployer.deployed_count(), 0);
        assert_eq!(deployer.node_count(), 0);
    }

    #[test]
    fn test_deployer_deploy_simple() {
        let mut deployer = RemoteDeployer::new();
        deployer.update_node(make_node("n1", 0.5, 2000, 0));
        deployer.update_node(make_node("n2", 0.5, 2000, 0));

        let props = RemoteProps::new("worker");
        let result = deployer.deploy(props).unwrap();
        assert_eq!(result.actor_type, "worker");
        assert_eq!(deployer.deployed_count(), 1);
    }

    #[test]
    fn test_deployer_deploy_with_scope() {
        let mut deployer = RemoteDeployer::new();
        deployer.update_node(
            NodeCapabilities::new("n1")
                .with_roles(vec!["compute".into()])
                .with_cpu(0.5)
                .with_memory(2000)
                .with_max_deployments(10)
        );
        deployer.update_node(
            NodeCapabilities::new("n2")
                .with_roles(vec!["storage".into()])
                .with_cpu(0.5)
                .with_memory(2000)
                .with_max_deployments(10)
        );

        let scope = RemoteScope::new("compute-scope")
            .with_roles(vec!["compute".into()]);
        deployer.register_scope(scope);

        let props = RemoteProps::new("worker").with_scope("compute-scope");
        let result = deployer.deploy(props).unwrap();
        assert_eq!(result.node_id, "n1"); // Only compute node eligible
    }

    #[test]
    fn test_deployer_deploy_no_nodes() {
        let mut deployer = RemoteDeployer::new();
        let props = RemoteProps::new("worker");
        assert!(deployer.deploy(props).is_err());
    }

    #[test]
    fn test_deployer_deploy_scope_not_found() {
        let mut deployer = RemoteDeployer::new();
        deployer.update_node(make_node("n1", 0.5, 2000, 0));

        let props = RemoteProps::new("worker").with_scope("nonexistent");
        assert!(matches!(deployer.deploy(props), Err(DeploymentError::ScopeNotFound(_))));
    }

    #[test]
    fn test_deployer_undeploy() {
        let mut deployer = RemoteDeployer::new();
        deployer.update_node(make_node("n1", 0.5, 2000, 0));

        let deployed = deployer.deploy(RemoteProps::new("worker")).unwrap();
        assert_eq!(deployer.deployed_count(), 1);

        let removed = deployer.undeploy(&deployed.actor_id);
        assert!(removed.is_some());
        assert_eq!(deployer.deployed_count(), 0);
    }

    #[test]
    fn test_deployer_undeploy_nonexistent() {
        let mut deployer = RemoteDeployer::new();
        assert!(deployer.undeploy("nonexistent").is_none());
    }

    #[test]
    fn test_deployer_migrate() {
        let mut deployer = RemoteDeployer::new();
        deployer.update_node(make_node("n1", 0.5, 2000, 0));
        deployer.update_node(make_node("n2", 0.3, 3000, 0));

        let deployed = deployer.deploy(RemoteProps::new("worker")).unwrap();
        let original_node = deployed.node_id.clone();

        let target = if original_node == "n1" { "n2" } else { "n1" };
        let migrated = deployer.migrate(&deployed.actor_id, target).unwrap();
        assert_eq!(migrated.node_id, target);
    }

    #[test]
    fn test_deployer_migrate_target_full() {
        let mut deployer = RemoteDeployer::new();
        deployer.update_node(make_node("n1", 0.5, 2000, 0));
        let mut full_node = make_node("n2", 0.3, 3000, 0);
        full_node.deployed_count = 10; // At max
        deployer.update_node(full_node);

        let deployed = deployer.deploy(RemoteProps::new("worker")).unwrap();
        assert!(matches!(deployer.migrate(&deployed.actor_id, "n2"),
            Err(DeploymentError::LimitReached { .. })));
    }

    #[test]
    fn test_deployer_actors_on_node() {
        let mut deployer = RemoteDeployer::new();
        deployer.update_node(make_node("n1", 0.5, 2000, 0));

        deployer.deploy(RemoteProps::new("worker")).unwrap();
        deployer.deploy(RemoteProps::new("processor")).unwrap();

        let on_n1 = deployer.actors_on_node("n1");
        assert_eq!(on_n1.len(), 2);
    }

    #[test]
    fn test_deployer_actors_of_type() {
        let mut deployer = RemoteDeployer::new();
        deployer.update_node(make_node("n1", 0.5, 2000, 0));

        deployer.deploy(RemoteProps::new("worker")).unwrap();
        deployer.deploy(RemoteProps::new("worker")).unwrap();
        deployer.deploy(RemoteProps::new("processor")).unwrap();

        assert_eq!(deployer.actors_of_type("worker").len(), 2);
        assert_eq!(deployer.actors_of_type("processor").len(), 1);
    }

    #[test]
    fn test_deployer_stats() {
        let mut deployer = RemoteDeployer::new();
        deployer.update_node(make_node("n1", 0.5, 2000, 0));
        deployer.update_node(make_node("n2", 0.5, 2000, 0));

        deployer.deploy(RemoteProps::new("worker")).unwrap();
        deployer.deploy(RemoteProps::new("worker")).unwrap();

        let stats = deployer.stats();
        assert_eq!(stats.total_deployed, 2);
        assert_eq!(stats.total_nodes, 2);
    }

    #[test]
    fn test_deployer_remove_node() {
        let mut deployer = RemoteDeployer::new();
        deployer.update_node(make_node("n1", 0.5, 2000, 0));
        assert_eq!(deployer.node_count(), 1);

        deployer.remove_node("n1");
        assert_eq!(deployer.node_count(), 0);
    }

    // DeploymentError Tests
    #[test]
    fn test_deployment_error_display() {
        let e = DeploymentError::NoSuitableNode("test".into());
        assert!(e.to_string().contains("test"));

        let e = DeploymentError::LimitReached { node: "n1".into(), current: 10, max: 10 };
        assert!(e.to_string().contains("10/10"));
    }

    // DeploymentEvent Tests
    #[test]
    fn test_deployment_event_types() {
        assert_eq!(DeploymentEventType::Deployed, DeploymentEventType::Deployed);
        assert_ne!(DeploymentEventType::Deployed, DeploymentEventType::Undeployed);

        let migrated = DeploymentEventType::Migrated { from: "n1".into() };
        assert!(matches!(migrated, DeploymentEventType::Migrated { .. }));
    }
}

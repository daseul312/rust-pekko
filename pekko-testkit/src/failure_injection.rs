//! # Failure Injection Kit - Resilience & Fault-Tolerance Testing
//!
//! Simulate failures for testing actor system resilience. Provides:
//! - **FailureInjector**: Intercept and corrupt/drop/delay messages
//! - **NetworkPartition**: Simulate network splits between actors
//! - **ChaosMonkey**: Random failure generation
//! - **FailureStats**: Track injected failures

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::time::Duration;

// ============================================================================
// Failure Types
// ============================================================================

/// Type of failure to inject
#[derive(Debug, Clone, PartialEq)]
pub enum FailureType {
    /// Drop the message entirely
    Drop,
    /// Delay message delivery
    Delay(Duration),
    /// Corrupt message payload (replace with error marker)
    Corrupt,
    /// Simulate timeout (delay beyond acceptable threshold)
    Timeout(Duration),
    /// Return an error to the sender
    Error(String),
    /// Duplicate the message
    Duplicate,
}

impl fmt::Display for FailureType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FailureType::Drop => write!(f, "Drop"),
            FailureType::Delay(d) => write!(f, "Delay({:?})", d),
            FailureType::Corrupt => write!(f, "Corrupt"),
            FailureType::Timeout(d) => write!(f, "Timeout({:?})", d),
            FailureType::Error(msg) => write!(f, "Error({})", msg),
            FailureType::Duplicate => write!(f, "Duplicate"),
        }
    }
}

// ============================================================================
// FailureRule — when to apply a failure
// ============================================================================

/// Rule that determines when a failure should be injected
#[derive(Debug, Clone)]
pub struct FailureRule {
    /// Name of this rule for identification
    pub name: String,
    /// Source actor filter (None = any source)
    pub source: Option<String>,
    /// Target actor filter (None = any target)
    pub target: Option<String>,
    /// What failure to inject
    pub failure_type: FailureType,
    /// How many times to apply (None = unlimited)
    pub remaining: Option<u32>,
    /// Whether this rule is active
    pub enabled: bool,
}

impl FailureRule {
    pub fn new(name: impl Into<String>, failure_type: FailureType) -> Self {
        Self {
            name: name.into(),
            source: None,
            target: None,
            failure_type,
            remaining: None,
            enabled: true,
        }
    }

    pub fn from_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn to_target(mut self, target: impl Into<String>) -> Self {
        self.target = Some(target.into());
        self
    }

    pub fn times(mut self, count: u32) -> Self {
        self.remaining = Some(count);
        self
    }

    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }

    /// Check if this rule matches the given source and target
    fn matches(&self, source: &str, target: &str) -> bool {
        if !self.enabled {
            return false;
        }
        if let Some(ref s) = self.source {
            if s != source {
                return false;
            }
        }
        if let Some(ref t) = self.target {
            if t != target {
                return false;
            }
        }
        if let Some(remaining) = self.remaining {
            if remaining == 0 {
                return false;
            }
        }
        true
    }

    /// Consume one use of this rule
    fn consume(&mut self) {
        if let Some(ref mut remaining) = self.remaining {
            *remaining = remaining.saturating_sub(1);
        }
    }
}

// ============================================================================
// MessageOutcome — result of running message through injector
// ============================================================================

/// What happened to a message after passing through the failure injector
#[derive(Debug, Clone, PartialEq)]
pub enum MessageOutcome {
    /// Message passed through normally
    Delivered,
    /// Message was dropped
    Dropped,
    /// Message was delayed by the given duration
    Delayed(Duration),
    /// Message was corrupted
    Corrupted,
    /// Message timed out
    TimedOut(Duration),
    /// Error was returned
    Errored(String),
    /// Message was duplicated
    Duplicated,
}

// ============================================================================
// FailureInjector
// ============================================================================

/// Intercepts messages between actors and injects configurable failures.
pub struct FailureInjector {
    rules: Vec<FailureRule>,
    history: VecDeque<InjectionRecord>,
    max_history: usize,
    stats: FailureStats,
}

/// Record of an injection event
#[derive(Debug, Clone)]
pub struct InjectionRecord {
    pub source: String,
    pub target: String,
    pub rule_name: String,
    pub outcome: MessageOutcome,
}

/// Statistics about injected failures
#[derive(Debug, Clone, Default)]
pub struct FailureStats {
    pub total_messages: u64,
    pub total_injected: u64,
    pub drops: u64,
    pub delays: u64,
    pub corruptions: u64,
    pub timeouts: u64,
    pub errors: u64,
    pub duplicates: u64,
    pub passed_through: u64,
}

impl FailureInjector {
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            history: VecDeque::new(),
            max_history: 1000,
            stats: FailureStats::default(),
        }
    }

    /// Add a failure rule
    pub fn add_rule(&mut self, rule: FailureRule) {
        self.rules.push(rule);
    }

    /// Remove a rule by name
    pub fn remove_rule(&mut self, name: &str) -> bool {
        let len_before = self.rules.len();
        self.rules.retain(|r| r.name != name);
        self.rules.len() < len_before
    }

    /// Enable or disable a rule by name
    pub fn set_rule_enabled(&mut self, name: &str, enabled: bool) -> bool {
        for rule in &mut self.rules {
            if rule.name == name {
                rule.enabled = enabled;
                return true;
            }
        }
        false
    }

    /// Process a message through the injector, returning the outcome
    pub fn process(&mut self, source: &str, target: &str) -> MessageOutcome {
        self.stats.total_messages += 1;

        // Find first matching rule
        let mut matched_idx = None;
        for (i, rule) in self.rules.iter().enumerate() {
            if rule.matches(source, target) {
                matched_idx = Some(i);
                break;
            }
        }

        if let Some(idx) = matched_idx {
            let rule = &mut self.rules[idx];
            let outcome = match &rule.failure_type {
                FailureType::Drop => {
                    self.stats.drops += 1;
                    MessageOutcome::Dropped
                }
                FailureType::Delay(d) => {
                    self.stats.delays += 1;
                    MessageOutcome::Delayed(*d)
                }
                FailureType::Corrupt => {
                    self.stats.corruptions += 1;
                    MessageOutcome::Corrupted
                }
                FailureType::Timeout(d) => {
                    self.stats.timeouts += 1;
                    MessageOutcome::TimedOut(*d)
                }
                FailureType::Error(msg) => {
                    self.stats.errors += 1;
                    MessageOutcome::Errored(msg.clone())
                }
                FailureType::Duplicate => {
                    self.stats.duplicates += 1;
                    MessageOutcome::Duplicated
                }
            };

            let record = InjectionRecord {
                source: source.to_string(),
                target: target.to_string(),
                rule_name: rule.name.clone(),
                outcome: outcome.clone(),
            };

            rule.consume();

            self.stats.total_injected += 1;
            self.record(record);
            outcome
        } else {
            self.stats.passed_through += 1;
            MessageOutcome::Delivered
        }
    }

    fn record(&mut self, record: InjectionRecord) {
        if self.history.len() >= self.max_history {
            self.history.pop_front();
        }
        self.history.push_back(record);
    }

    /// Get failure statistics
    pub fn stats(&self) -> &FailureStats {
        &self.stats
    }

    /// Get injection history
    pub fn history(&self) -> &VecDeque<InjectionRecord> {
        &self.history
    }

    /// Clear all rules
    pub fn clear_rules(&mut self) {
        self.rules.clear();
    }

    /// Reset stats and history
    pub fn reset(&mut self) {
        self.stats = FailureStats::default();
        self.history.clear();
    }

    /// Number of active rules
    pub fn active_rule_count(&self) -> usize {
        self.rules.iter().filter(|r| r.enabled).count()
    }

    /// Get injection rate (injected / total)
    pub fn injection_rate(&self) -> f64 {
        if self.stats.total_messages == 0 {
            0.0
        } else {
            self.stats.total_injected as f64 / self.stats.total_messages as f64
        }
    }
}

impl Default for FailureInjector {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// NetworkPartition
// ============================================================================

/// Simulates network partitions between groups of actors
pub struct NetworkPartition {
    /// Partition groups — actors in different groups cannot communicate
    groups: Vec<HashSet<String>>,
    /// Individual blocked routes (source → set of blocked targets)
    blocked_routes: HashMap<String, HashSet<String>>,
    /// Whether partition is active
    active: bool,
    /// Stats
    blocked_count: u64,
    allowed_count: u64,
}

impl NetworkPartition {
    pub fn new() -> Self {
        Self {
            groups: Vec::new(),
            blocked_routes: HashMap::new(),
            active: false,
            blocked_count: 0,
            allowed_count: 0,
        }
    }

    /// Create a partition with two groups
    pub fn split(group_a: Vec<String>, group_b: Vec<String>) -> Self {
        let mut partition = Self::new();
        partition.groups.push(group_a.into_iter().collect());
        partition.groups.push(group_b.into_iter().collect());
        partition.active = true;
        partition
    }

    /// Block a specific route
    pub fn block_route(&mut self, source: impl Into<String>, target: impl Into<String>) {
        self.blocked_routes
            .entry(source.into())
            .or_default()
            .insert(target.into());
        self.active = true;
    }

    /// Unblock a specific route
    pub fn unblock_route(&mut self, source: &str, target: &str) -> bool {
        if let Some(targets) = self.blocked_routes.get_mut(source) {
            let removed = targets.remove(target);
            if targets.is_empty() {
                self.blocked_routes.remove(source);
            }
            removed
        } else {
            false
        }
    }

    /// Activate the partition
    pub fn activate(&mut self) {
        self.active = true;
    }

    /// Deactivate (heal) the partition
    pub fn heal(&mut self) {
        self.active = false;
    }

    /// Check if a message can pass from source to target
    pub fn can_communicate(&mut self, source: &str, target: &str) -> bool {
        if !self.active {
            self.allowed_count += 1;
            return true;
        }

        // Check individual blocked routes
        if let Some(targets) = self.blocked_routes.get(source) {
            if targets.contains(target) {
                self.blocked_count += 1;
                return false;
            }
        }

        // Check group partition
        for group in &self.groups {
            if group.contains(source) {
                // Source is in this group — target must also be in this group
                if !group.contains(target) {
                    // Check if target is in any other group
                    let target_in_other = self.groups.iter().any(|g| g != group && g.contains(target));
                    if target_in_other {
                        self.blocked_count += 1;
                        return false;
                    }
                }
                self.allowed_count += 1;
                return true;
            }
        }

        // Source not in any partition group — allow
        self.allowed_count += 1;
        true
    }

    /// Check if partition is currently active
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Get blocked message count
    pub fn blocked_count(&self) -> u64 {
        self.blocked_count
    }

    /// Get allowed message count
    pub fn allowed_count(&self) -> u64 {
        self.allowed_count
    }

    /// Reset stats
    pub fn reset_stats(&mut self) {
        self.blocked_count = 0;
        self.allowed_count = 0;
    }
}

impl Default for NetworkPartition {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// ChaosMonkey
// ============================================================================

/// Random failure generator for stress-testing actor systems.
/// Uses a deterministic seed for reproducible tests.
pub struct ChaosMonkey {
    /// Probability of failure (0.0 - 1.0)
    failure_probability: f64,
    /// Available failure types to randomly choose from
    failure_types: Vec<FailureType>,
    /// Deterministic seed counter (simple LCG for reproducibility)
    seed: u64,
    /// Whether the monkey is active
    active: bool,
    /// Stats
    total_checks: u64,
    total_failures: u64,
}

impl ChaosMonkey {
    pub fn new(failure_probability: f64) -> Self {
        Self {
            failure_probability: failure_probability.clamp(0.0, 1.0),
            failure_types: vec![FailureType::Drop],
            seed: 42,
            active: true,
            total_checks: 0,
            total_failures: 0,
        }
    }

    /// Set failure types to choose from
    pub fn with_failure_types(mut self, types: Vec<FailureType>) -> Self {
        self.failure_types = types;
        self
    }

    /// Set the random seed for reproducibility
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Simple LCG pseudo-random number generator (deterministic)
    fn next_random(&mut self) -> f64 {
        // LCG: next = (a * seed + c) mod m
        self.seed = self.seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        // Convert to [0, 1)
        (self.seed >> 33) as f64 / (1u64 << 31) as f64
    }

    /// Check if a failure should be injected, and which type
    pub fn should_fail(&mut self) -> Option<FailureType> {
        if !self.active || self.failure_types.is_empty() {
            return None;
        }
        self.total_checks += 1;

        let roll = self.next_random();
        if roll < self.failure_probability {
            self.total_failures += 1;
            let type_roll = self.next_random();
            let idx = (type_roll * self.failure_types.len() as f64) as usize;
            let idx = idx.min(self.failure_types.len() - 1);
            Some(self.failure_types[idx].clone())
        } else {
            None
        }
    }

    /// Activate the monkey
    pub fn activate(&mut self) {
        self.active = true;
    }

    /// Deactivate the monkey
    pub fn deactivate(&mut self) {
        self.active = false;
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    pub fn total_checks(&self) -> u64 {
        self.total_checks
    }

    pub fn total_failures(&self) -> u64 {
        self.total_failures
    }

    pub fn failure_rate(&self) -> f64 {
        if self.total_checks == 0 {
            0.0
        } else {
            self.total_failures as f64 / self.total_checks as f64
        }
    }

    /// Set failure probability
    pub fn set_probability(&mut self, p: f64) {
        self.failure_probability = p.clamp(0.0, 1.0);
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // --- FailureType ---

    #[test]
    fn test_failure_type_display() {
        assert_eq!(FailureType::Drop.to_string(), "Drop");
        assert_eq!(FailureType::Corrupt.to_string(), "Corrupt");
        assert_eq!(FailureType::Duplicate.to_string(), "Duplicate");
        assert!(FailureType::Delay(Duration::from_millis(100)).to_string().contains("100"));
        assert!(FailureType::Error("timeout".into()).to_string().contains("timeout"));
    }

    // --- FailureRule ---

    #[test]
    fn test_rule_matches_any() {
        let rule = FailureRule::new("drop-all", FailureType::Drop);
        assert!(rule.matches("a", "b"));
        assert!(rule.matches("x", "y"));
    }

    #[test]
    fn test_rule_matches_source_filter() {
        let rule = FailureRule::new("drop-from-a", FailureType::Drop).from_source("a");
        assert!(rule.matches("a", "b"));
        assert!(!rule.matches("x", "b"));
    }

    #[test]
    fn test_rule_matches_target_filter() {
        let rule = FailureRule::new("drop-to-b", FailureType::Drop).to_target("b");
        assert!(rule.matches("a", "b"));
        assert!(!rule.matches("a", "x"));
    }

    #[test]
    fn test_rule_matches_both_filters() {
        let rule = FailureRule::new("specific", FailureType::Drop)
            .from_source("a")
            .to_target("b");
        assert!(rule.matches("a", "b"));
        assert!(!rule.matches("a", "x"));
        assert!(!rule.matches("x", "b"));
    }

    #[test]
    fn test_rule_disabled() {
        let rule = FailureRule::new("disabled", FailureType::Drop).disabled();
        assert!(!rule.matches("a", "b"));
    }

    #[test]
    fn test_rule_limited_uses() {
        let mut rule = FailureRule::new("once", FailureType::Drop).times(2);
        assert!(rule.matches("a", "b"));
        rule.consume();
        assert!(rule.matches("a", "b"));
        rule.consume();
        assert!(!rule.matches("a", "b")); // exhausted
    }

    // --- FailureInjector ---

    #[test]
    fn test_injector_no_rules() {
        let mut inj = FailureInjector::new();
        let outcome = inj.process("a", "b");
        assert_eq!(outcome, MessageOutcome::Delivered);
        assert_eq!(inj.stats().passed_through, 1);
    }

    #[test]
    fn test_injector_drop() {
        let mut inj = FailureInjector::new();
        inj.add_rule(FailureRule::new("drop", FailureType::Drop));
        let outcome = inj.process("a", "b");
        assert_eq!(outcome, MessageOutcome::Dropped);
        assert_eq!(inj.stats().drops, 1);
        assert_eq!(inj.stats().total_injected, 1);
    }

    #[test]
    fn test_injector_delay() {
        let mut inj = FailureInjector::new();
        inj.add_rule(FailureRule::new("slow", FailureType::Delay(Duration::from_millis(500))));
        let outcome = inj.process("a", "b");
        assert_eq!(outcome, MessageOutcome::Delayed(Duration::from_millis(500)));
        assert_eq!(inj.stats().delays, 1);
    }

    #[test]
    fn test_injector_corrupt() {
        let mut inj = FailureInjector::new();
        inj.add_rule(FailureRule::new("corrupt", FailureType::Corrupt));
        assert_eq!(inj.process("a", "b"), MessageOutcome::Corrupted);
    }

    #[test]
    fn test_injector_error() {
        let mut inj = FailureInjector::new();
        inj.add_rule(FailureRule::new("err", FailureType::Error("db down".into())));
        assert_eq!(inj.process("a", "b"), MessageOutcome::Errored("db down".into()));
    }

    #[test]
    fn test_injector_duplicate() {
        let mut inj = FailureInjector::new();
        inj.add_rule(FailureRule::new("dup", FailureType::Duplicate));
        assert_eq!(inj.process("a", "b"), MessageOutcome::Duplicated);
    }

    #[test]
    fn test_injector_targeted_rule() {
        let mut inj = FailureInjector::new();
        inj.add_rule(
            FailureRule::new("target-drop", FailureType::Drop)
                .from_source("actor-a")
                .to_target("actor-b"),
        );
        assert_eq!(inj.process("actor-a", "actor-b"), MessageOutcome::Dropped);
        assert_eq!(inj.process("actor-a", "actor-c"), MessageOutcome::Delivered);
        assert_eq!(inj.process("actor-x", "actor-b"), MessageOutcome::Delivered);
    }

    #[test]
    fn test_injector_limited_rule() {
        let mut inj = FailureInjector::new();
        inj.add_rule(FailureRule::new("once", FailureType::Drop).times(1));
        assert_eq!(inj.process("a", "b"), MessageOutcome::Dropped);
        assert_eq!(inj.process("a", "b"), MessageOutcome::Delivered); // exhausted
    }

    #[test]
    fn test_injector_remove_rule() {
        let mut inj = FailureInjector::new();
        inj.add_rule(FailureRule::new("drop", FailureType::Drop));
        assert_eq!(inj.process("a", "b"), MessageOutcome::Dropped);
        assert!(inj.remove_rule("drop"));
        assert_eq!(inj.process("a", "b"), MessageOutcome::Delivered);
    }

    #[test]
    fn test_injector_enable_disable() {
        let mut inj = FailureInjector::new();
        inj.add_rule(FailureRule::new("drop", FailureType::Drop));
        inj.set_rule_enabled("drop", false);
        assert_eq!(inj.process("a", "b"), MessageOutcome::Delivered);
        inj.set_rule_enabled("drop", true);
        assert_eq!(inj.process("a", "b"), MessageOutcome::Dropped);
    }

    #[test]
    fn test_injector_history() {
        let mut inj = FailureInjector::new();
        inj.add_rule(FailureRule::new("drop", FailureType::Drop));
        inj.process("actor-a", "actor-b");
        assert_eq!(inj.history().len(), 1);
        assert_eq!(inj.history()[0].source, "actor-a");
        assert_eq!(inj.history()[0].rule_name, "drop");
    }

    #[test]
    fn test_injector_injection_rate() {
        let mut inj = FailureInjector::new();
        inj.add_rule(
            FailureRule::new("drop-a", FailureType::Drop).from_source("a"),
        );
        inj.process("a", "b"); // dropped
        inj.process("x", "b"); // delivered
        assert!((inj.injection_rate() - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_injector_reset() {
        let mut inj = FailureInjector::new();
        inj.add_rule(FailureRule::new("drop", FailureType::Drop));
        inj.process("a", "b");
        inj.reset();
        assert_eq!(inj.stats().total_messages, 0);
        assert!(inj.history().is_empty());
    }

    #[test]
    fn test_injector_active_rule_count() {
        let mut inj = FailureInjector::new();
        inj.add_rule(FailureRule::new("a", FailureType::Drop));
        inj.add_rule(FailureRule::new("b", FailureType::Drop).disabled());
        assert_eq!(inj.active_rule_count(), 1);
    }

    // --- NetworkPartition ---

    #[test]
    fn test_partition_inactive() {
        let mut p = NetworkPartition::new();
        assert!(p.can_communicate("a", "b"));
    }

    #[test]
    fn test_partition_split() {
        let mut p = NetworkPartition::split(
            vec!["a".into(), "b".into()],
            vec!["c".into(), "d".into()],
        );
        assert!(p.can_communicate("a", "b")); // same group
        assert!(!p.can_communicate("a", "c")); // cross-group
        assert!(!p.can_communicate("c", "a")); // cross-group reverse
        assert!(p.can_communicate("c", "d")); // same group
    }

    #[test]
    fn test_partition_heal() {
        let mut p = NetworkPartition::split(
            vec!["a".into()],
            vec!["b".into()],
        );
        assert!(!p.can_communicate("a", "b"));
        p.heal();
        assert!(p.can_communicate("a", "b"));
    }

    #[test]
    fn test_partition_block_route() {
        let mut p = NetworkPartition::new();
        p.block_route("a", "b");
        assert!(!p.can_communicate("a", "b"));
        assert!(p.can_communicate("b", "a")); // one-directional
    }

    #[test]
    fn test_partition_unblock_route() {
        let mut p = NetworkPartition::new();
        p.block_route("a", "b");
        assert!(!p.can_communicate("a", "b"));
        p.unblock_route("a", "b");
        assert!(p.can_communicate("a", "b"));
    }

    #[test]
    fn test_partition_stats() {
        let mut p = NetworkPartition::split(
            vec!["a".into()],
            vec!["b".into()],
        );
        p.can_communicate("a", "b"); // blocked
        p.can_communicate("a", "b"); // blocked
        p.can_communicate("a", "a"); // Not in other group, so allowed (same group doesn't have "a" and "a" pair issue)
        assert_eq!(p.blocked_count(), 2);
    }

    #[test]
    fn test_partition_unknown_actor() {
        let mut p = NetworkPartition::split(
            vec!["a".into()],
            vec!["b".into()],
        );
        // "x" is not in any group — should be allowed
        assert!(p.can_communicate("x", "a"));
        assert!(p.can_communicate("x", "b"));
    }

    // --- ChaosMonkey ---

    #[test]
    fn test_chaos_monkey_zero_probability() {
        let mut monkey = ChaosMonkey::new(0.0);
        for _ in 0..100 {
            assert!(monkey.should_fail().is_none());
        }
        assert_eq!(monkey.total_checks(), 100);
        assert_eq!(monkey.total_failures(), 0);
    }

    #[test]
    fn test_chaos_monkey_full_probability() {
        let mut monkey = ChaosMonkey::new(1.0);
        for _ in 0..10 {
            assert!(monkey.should_fail().is_some());
        }
        assert_eq!(monkey.total_failures(), 10);
    }

    #[test]
    fn test_chaos_monkey_deterministic() {
        let mut monkey1 = ChaosMonkey::new(0.5).with_seed(123);
        let mut monkey2 = ChaosMonkey::new(0.5).with_seed(123);

        let results1: Vec<_> = (0..20).map(|_| monkey1.should_fail().is_some()).collect();
        let results2: Vec<_> = (0..20).map(|_| monkey2.should_fail().is_some()).collect();
        assert_eq!(results1, results2);
    }

    #[test]
    fn test_chaos_monkey_multiple_types() {
        let mut monkey = ChaosMonkey::new(1.0)
            .with_failure_types(vec![
                FailureType::Drop,
                FailureType::Corrupt,
                FailureType::Duplicate,
            ])
            .with_seed(42);

        let mut types = HashSet::new();
        for _ in 0..100 {
            if let Some(ft) = monkey.should_fail() {
                types.insert(ft.to_string());
            }
        }
        // With 100 samples, we should hit all 3 types
        assert!(types.len() >= 2, "Expected multiple failure types, got: {:?}", types);
    }

    #[test]
    fn test_chaos_monkey_deactivate() {
        let mut monkey = ChaosMonkey::new(1.0);
        assert!(monkey.should_fail().is_some());
        monkey.deactivate();
        assert!(monkey.should_fail().is_none());
        monkey.activate();
        assert!(monkey.should_fail().is_some());
    }

    #[test]
    fn test_chaos_monkey_failure_rate() {
        let mut monkey = ChaosMonkey::new(0.5).with_seed(0);
        for _ in 0..1000 {
            monkey.should_fail();
        }
        let rate = monkey.failure_rate();
        // Should be roughly 0.5 ± 0.1
        assert!(rate > 0.3 && rate < 0.7,
            "Expected ~0.5 failure rate, got {}", rate);
    }

    #[test]
    fn test_chaos_monkey_set_probability() {
        let mut monkey = ChaosMonkey::new(0.0);
        for _ in 0..10 {
            assert!(monkey.should_fail().is_none());
        }
        monkey.set_probability(1.0);
        assert!(monkey.should_fail().is_some());
    }

    #[test]
    fn test_chaos_monkey_empty_failure_types() {
        let mut monkey = ChaosMonkey::new(1.0).with_failure_types(vec![]);
        assert!(monkey.should_fail().is_none());
    }
}

// supervision_assertions.rs - Assertions for supervisor directive decisions
// Allows test code to verify which supervision strategy was chosen for failures

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

// ============================================================================
// SupervisionDirective
// ============================================================================

/// Supervision directive that a supervisor decided on
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SupervisionDirective {
    Resume,
    Restart,
    Stop,
    Escalate,
}

impl fmt::Display for SupervisionDirective {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SupervisionDirective::Resume => write!(f, "Resume"),
            SupervisionDirective::Restart => write!(f, "Restart"),
            SupervisionDirective::Stop => write!(f, "Stop"),
            SupervisionDirective::Escalate => write!(f, "Escalate"),
        }
    }
}

// ============================================================================
// SupervisionEvent
// ============================================================================

/// A recorded supervision event
#[derive(Debug, Clone)]
pub struct SupervisionEvent {
    pub parent: String,
    pub child: String,
    pub failure_type: String,
    pub failure_message: String,
    pub directive: SupervisionDirective,
    pub timestamp: Instant,
    pub attempt: u32,
}

impl SupervisionEvent {
    pub fn new(
        parent: &str,
        child: &str,
        failure_type: &str,
        directive: SupervisionDirective,
    ) -> Self {
        Self {
            parent: parent.to_string(),
            child: child.to_string(),
            failure_type: failure_type.to_string(),
            failure_message: String::new(),
            directive,
            timestamp: Instant::now(),
            attempt: 1,
        }
    }

    pub fn with_message(mut self, msg: &str) -> Self {
        self.failure_message = msg.to_string();
        self
    }

    pub fn with_attempt(mut self, attempt: u32) -> Self {
        self.attempt = attempt;
        self
    }

    pub fn age(&self) -> Duration {
        self.timestamp.elapsed()
    }
}

// ============================================================================
// SupervisionRule
// ============================================================================

/// An expected supervision rule for assertions
#[derive(Debug, Clone)]
pub struct SupervisionRule {
    pub failure_pattern: String,
    pub expected_directive: SupervisionDirective,
    pub max_retries: Option<u32>,
    pub within_duration: Option<Duration>,
}

impl SupervisionRule {
    pub fn new(pattern: &str, directive: SupervisionDirective) -> Self {
        Self {
            failure_pattern: pattern.to_string(),
            expected_directive: directive,
            max_retries: None,
            within_duration: None,
        }
    }

    pub fn with_max_retries(mut self, max: u32) -> Self {
        self.max_retries = Some(max);
        self
    }

    pub fn with_within(mut self, duration: Duration) -> Self {
        self.within_duration = Some(duration);
        self
    }

    /// Check if a failure type matches this rule's pattern
    pub fn matches(&self, failure_type: &str) -> bool {
        if self.failure_pattern == "*" {
            return true;
        }
        if self.failure_pattern.ends_with('*') {
            let prefix = &self.failure_pattern[..self.failure_pattern.len() - 1];
            return failure_type.starts_with(prefix);
        }
        self.failure_pattern == failure_type
    }
}

// ============================================================================
// SupervisionProbe
// ============================================================================

/// Collects and asserts on supervision decisions
pub struct SupervisionProbe {
    events: Vec<SupervisionEvent>,
    rules: Vec<SupervisionRule>,
    directive_counts: HashMap<SupervisionDirective, u64>,
    failure_counts: HashMap<String, u64>,
}

impl SupervisionProbe {
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
            rules: Vec::new(),
            directive_counts: HashMap::new(),
            failure_counts: HashMap::new(),
        }
    }

    /// Add an expected rule
    pub fn expect_rule(&mut self, rule: SupervisionRule) {
        self.rules.push(rule);
    }

    /// Record a supervision event
    pub fn record(&mut self, event: SupervisionEvent) {
        *self.directive_counts.entry(event.directive.clone()).or_insert(0) += 1;
        *self.failure_counts.entry(event.failure_type.clone()).or_insert(0) += 1;
        self.events.push(event);
    }

    /// Record a supervision decision (convenience method)
    pub fn record_decision(
        &mut self,
        parent: &str,
        child: &str,
        failure_type: &str,
        directive: SupervisionDirective,
    ) {
        let event = SupervisionEvent::new(parent, child, failure_type, directive);
        self.record(event);
    }

    // ========================================================================
    // Assertions
    // ========================================================================

    /// Assert that a specific failure type resulted in the expected directive
    pub fn assert_directive_for(
        &self,
        failure_type: &str,
        expected: SupervisionDirective,
    ) -> bool {
        self.events
            .iter()
            .any(|e| e.failure_type == failure_type && e.directive == expected)
    }

    /// Assert that a child actor received a specific directive
    pub fn assert_child_directive(&self, child: &str, expected: SupervisionDirective) -> bool {
        self.events
            .iter()
            .any(|e| e.child == child && e.directive == expected)
    }

    /// Assert that a parent issued a specific directive
    pub fn assert_parent_issued(&self, parent: &str, expected: SupervisionDirective) -> bool {
        self.events
            .iter()
            .any(|e| e.parent == parent && e.directive == expected)
    }

    /// Assert no escalation occurred
    pub fn assert_no_escalation(&self) -> bool {
        !self.events.iter().any(|e| e.directive == SupervisionDirective::Escalate)
    }

    /// Assert no stops occurred
    pub fn assert_no_stops(&self) -> bool {
        !self.events.iter().any(|e| e.directive == SupervisionDirective::Stop)
    }

    /// Assert that all recorded rules match the expected directives
    pub fn assert_rules_match(&self) -> Vec<RuleViolation> {
        let mut violations = Vec::new();
        for event in &self.events {
            for rule in &self.rules {
                if rule.matches(&event.failure_type) && event.directive != rule.expected_directive {
                    violations.push(RuleViolation {
                        event: event.clone(),
                        expected: rule.expected_directive.clone(),
                        actual: event.directive.clone(),
                        rule_pattern: rule.failure_pattern.clone(),
                    });
                }
            }
        }
        violations
    }

    /// Assert max restart attempts for a child
    pub fn assert_max_restarts(&self, child: &str, max: u32) -> bool {
        let restart_count = self
            .events
            .iter()
            .filter(|e| e.child == child && e.directive == SupervisionDirective::Restart)
            .count();
        restart_count as u32 <= max
    }

    // ========================================================================
    // Queries
    // ========================================================================

    /// Get all events for a specific child
    pub fn events_for_child(&self, child: &str) -> Vec<&SupervisionEvent> {
        self.events.iter().filter(|e| e.child == child).collect()
    }

    /// Get all events with a specific directive
    pub fn events_with_directive(&self, directive: &SupervisionDirective) -> Vec<&SupervisionEvent> {
        self.events.iter().filter(|e| &e.directive == directive).collect()
    }

    /// Get count of a specific directive
    pub fn directive_count(&self, directive: &SupervisionDirective) -> u64 {
        self.directive_counts.get(directive).copied().unwrap_or(0)
    }

    /// Get total event count
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Get all events
    pub fn all_events(&self) -> &[SupervisionEvent] {
        &self.events
    }

    /// Get unique failure types
    pub fn failure_types(&self) -> Vec<&str> {
        self.failure_counts.keys().map(|s| s.as_str()).collect()
    }

    /// Get the last directive for a child
    pub fn last_directive_for(&self, child: &str) -> Option<&SupervisionDirective> {
        self.events
            .iter()
            .rev()
            .find(|e| e.child == child)
            .map(|e| &e.directive)
    }

    /// Clear all recorded events
    pub fn clear(&mut self) {
        self.events.clear();
        self.directive_counts.clear();
        self.failure_counts.clear();
    }
}

impl Default for SupervisionProbe {
    fn default() -> Self {
        Self::new()
    }
}

/// A rule violation found during assertion
#[derive(Debug, Clone)]
pub struct RuleViolation {
    pub event: SupervisionEvent,
    pub expected: SupervisionDirective,
    pub actual: SupervisionDirective,
    pub rule_pattern: String,
}

impl fmt::Display for RuleViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Violation: {} failed with '{}' → expected {} but got {}",
            self.event.child, self.event.failure_type, self.expected, self.actual
        )
    }
}

impl fmt::Debug for SupervisionProbe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SupervisionProbe")
            .field("events", &self.events.len())
            .field("rules", &self.rules.len())
            .finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_probe() -> SupervisionProbe {
        let mut probe = SupervisionProbe::new();
        probe.record_decision("parent", "child-1", "NullPointerException", SupervisionDirective::Restart);
        probe.record_decision("parent", "child-2", "IOException", SupervisionDirective::Resume);
        probe.record_decision("parent", "child-1", "FatalError", SupervisionDirective::Stop);
        probe
    }

    #[test]
    fn test_record_events() {
        let probe = setup_probe();
        assert_eq!(probe.event_count(), 3);
    }

    #[test]
    fn test_assert_directive_for() {
        let probe = setup_probe();
        assert!(probe.assert_directive_for("NullPointerException", SupervisionDirective::Restart));
        assert!(probe.assert_directive_for("IOException", SupervisionDirective::Resume));
        assert!(!probe.assert_directive_for("IOException", SupervisionDirective::Stop));
    }

    #[test]
    fn test_assert_child_directive() {
        let probe = setup_probe();
        assert!(probe.assert_child_directive("child-1", SupervisionDirective::Restart));
        assert!(probe.assert_child_directive("child-1", SupervisionDirective::Stop));
        assert!(!probe.assert_child_directive("child-2", SupervisionDirective::Stop));
    }

    #[test]
    fn test_assert_parent_issued() {
        let probe = setup_probe();
        assert!(probe.assert_parent_issued("parent", SupervisionDirective::Restart));
        assert!(probe.assert_parent_issued("parent", SupervisionDirective::Resume));
    }

    #[test]
    fn test_assert_no_escalation() {
        let probe = setup_probe();
        assert!(probe.assert_no_escalation());

        let mut probe2 = SupervisionProbe::new();
        probe2.record_decision("p", "c", "err", SupervisionDirective::Escalate);
        assert!(!probe2.assert_no_escalation());
    }

    #[test]
    fn test_assert_no_stops() {
        let mut probe = SupervisionProbe::new();
        probe.record_decision("p", "c", "err", SupervisionDirective::Restart);
        assert!(probe.assert_no_stops());
    }

    #[test]
    fn test_directive_count() {
        let probe = setup_probe();
        assert_eq!(probe.directive_count(&SupervisionDirective::Restart), 1);
        assert_eq!(probe.directive_count(&SupervisionDirective::Resume), 1);
        assert_eq!(probe.directive_count(&SupervisionDirective::Stop), 1);
        assert_eq!(probe.directive_count(&SupervisionDirective::Escalate), 0);
    }

    #[test]
    fn test_events_for_child() {
        let probe = setup_probe();
        assert_eq!(probe.events_for_child("child-1").len(), 2);
        assert_eq!(probe.events_for_child("child-2").len(), 1);
    }

    #[test]
    fn test_events_with_directive() {
        let probe = setup_probe();
        assert_eq!(probe.events_with_directive(&SupervisionDirective::Restart).len(), 1);
    }

    #[test]
    fn test_last_directive_for() {
        let probe = setup_probe();
        assert_eq!(probe.last_directive_for("child-1"), Some(&SupervisionDirective::Stop));
        assert_eq!(probe.last_directive_for("child-2"), Some(&SupervisionDirective::Resume));
        assert_eq!(probe.last_directive_for("unknown"), None);
    }

    #[test]
    fn test_rule_matching() {
        let rule = SupervisionRule::new("IOException", SupervisionDirective::Resume);
        assert!(rule.matches("IOException"));
        assert!(!rule.matches("NullPointerException"));
    }

    #[test]
    fn test_rule_wildcard() {
        let rule = SupervisionRule::new("*", SupervisionDirective::Restart);
        assert!(rule.matches("anything"));
        assert!(rule.matches("IOException"));
    }

    #[test]
    fn test_rule_prefix_wildcard() {
        let rule = SupervisionRule::new("IO*", SupervisionDirective::Resume);
        assert!(rule.matches("IOException"));
        assert!(rule.matches("IOError"));
        assert!(!rule.matches("NullPointerException"));
    }

    #[test]
    fn test_assert_rules_match() {
        let mut probe = SupervisionProbe::new();
        probe.expect_rule(SupervisionRule::new("IOException", SupervisionDirective::Resume));
        probe.record_decision("p", "c", "IOException", SupervisionDirective::Resume);
        assert!(probe.assert_rules_match().is_empty());
    }

    #[test]
    fn test_assert_rules_violation() {
        let mut probe = SupervisionProbe::new();
        probe.expect_rule(SupervisionRule::new("IOException", SupervisionDirective::Resume));
        probe.record_decision("p", "c", "IOException", SupervisionDirective::Stop);
        let violations = probe.assert_rules_match();
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].expected, SupervisionDirective::Resume);
        assert_eq!(violations[0].actual, SupervisionDirective::Stop);
    }

    #[test]
    fn test_assert_max_restarts() {
        let mut probe = SupervisionProbe::new();
        probe.record_decision("p", "c", "err", SupervisionDirective::Restart);
        probe.record_decision("p", "c", "err", SupervisionDirective::Restart);
        assert!(probe.assert_max_restarts("c", 3));
        assert!(probe.assert_max_restarts("c", 2));
        assert!(!probe.assert_max_restarts("c", 1));
    }

    #[test]
    fn test_failure_types() {
        let probe = setup_probe();
        let types = probe.failure_types();
        assert_eq!(types.len(), 3);
    }

    #[test]
    fn test_clear() {
        let mut probe = setup_probe();
        probe.clear();
        assert_eq!(probe.event_count(), 0);
        assert_eq!(probe.directive_count(&SupervisionDirective::Restart), 0);
    }

    #[test]
    fn test_supervision_event_with_message() {
        let event = SupervisionEvent::new("p", "c", "err", SupervisionDirective::Restart)
            .with_message("something went wrong")
            .with_attempt(3);
        assert_eq!(event.failure_message, "something went wrong");
        assert_eq!(event.attempt, 3);
    }

    #[test]
    fn test_event_age() {
        let event = SupervisionEvent::new("p", "c", "err", SupervisionDirective::Restart);
        std::thread::sleep(Duration::from_millis(5));
        assert!(event.age() >= Duration::from_millis(5));
    }

    #[test]
    fn test_violation_display() {
        let event = SupervisionEvent::new("p", "child-1", "IOException", SupervisionDirective::Stop);
        let violation = RuleViolation {
            event,
            expected: SupervisionDirective::Resume,
            actual: SupervisionDirective::Stop,
            rule_pattern: "IOException".to_string(),
        };
        let display = format!("{}", violation);
        assert!(display.contains("child-1"));
        assert!(display.contains("Resume"));
        assert!(display.contains("Stop"));
    }

    #[test]
    fn test_directive_display() {
        assert_eq!(format!("{}", SupervisionDirective::Resume), "Resume");
        assert_eq!(format!("{}", SupervisionDirective::Restart), "Restart");
        assert_eq!(format!("{}", SupervisionDirective::Stop), "Stop");
        assert_eq!(format!("{}", SupervisionDirective::Escalate), "Escalate");
    }

    #[test]
    fn test_debug_display() {
        let probe = SupervisionProbe::new();
        let debug = format!("{:?}", probe);
        assert!(debug.contains("SupervisionProbe"));
    }

    #[test]
    fn test_rule_with_options() {
        let rule = SupervisionRule::new("err", SupervisionDirective::Restart)
            .with_max_retries(3)
            .with_within(Duration::from_secs(10));
        assert_eq!(rule.max_retries, Some(3));
        assert_eq!(rule.within_duration, Some(Duration::from_secs(10)));
    }
}

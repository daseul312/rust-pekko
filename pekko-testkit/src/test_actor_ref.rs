// test_actor_ref.rs - TestActorRef for direct actor state inspection and interaction
// Inspired by Scala Pekko's TestActorRef which provides access to underlying actor state

use std::collections::VecDeque;
use std::fmt;
use std::time::{Duration, Instant};

// ============================================================================
// ActorState snapshot
// ============================================================================

/// A snapshot of an actor's observable state at a point in time
#[derive(Debug, Clone)]
pub struct ActorStateSnapshot {
    pub state_name: String,
    pub properties: Vec<(String, StateValue)>,
    pub captured_at: Instant,
}

/// A value that can be stored in state snapshots
#[derive(Debug, Clone, PartialEq)]
pub enum StateValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    List(Vec<StateValue>),
    None,
}

impl ActorStateSnapshot {
    pub fn new(name: &str) -> Self {
        Self {
            state_name: name.to_string(),
            properties: Vec::new(),
            captured_at: Instant::now(),
        }
    }

    pub fn with_property(mut self, key: &str, value: StateValue) -> Self {
        self.properties.push((key.to_string(), value));
        self
    }

    pub fn get(&self, key: &str) -> Option<&StateValue> {
        self.properties.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub fn get_string(&self, key: &str) -> Option<&str> {
        match self.get(key) {
            Some(StateValue::String(s)) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn get_int(&self, key: &str) -> Option<i64> {
        match self.get(key) {
            Some(StateValue::Int(i)) => Some(*i),
            _ => None,
        }
    }

    pub fn get_bool(&self, key: &str) -> Option<bool> {
        match self.get(key) {
            Some(StateValue::Bool(b)) => Some(*b),
            _ => None,
        }
    }

    pub fn property_count(&self) -> usize {
        self.properties.len()
    }

    pub fn has_property(&self, key: &str) -> bool {
        self.properties.iter().any(|(k, _)| k == key)
    }
}

impl fmt::Display for StateValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StateValue::String(s) => write!(f, "\"{}\"", s),
            StateValue::Int(i) => write!(f, "{}", i),
            StateValue::Float(v) => write!(f, "{:.2}", v),
            StateValue::Bool(b) => write!(f, "{}", b),
            StateValue::List(l) => write!(f, "[{}]", l.iter().map(|v| format!("{}", v)).collect::<Vec<_>>().join(", ")),
            StateValue::None => write!(f, "None"),
        }
    }
}

// ============================================================================
// TestMessage
// ============================================================================

/// A test message with tracking metadata
#[derive(Debug, Clone)]
pub struct TestMessage {
    pub payload: Vec<u8>,
    pub sender: String,
    pub sequence_nr: u64,
    pub sent_at: Instant,
    pub label: Option<String>,
}

impl TestMessage {
    pub fn new(payload: Vec<u8>) -> Self {
        Self {
            payload,
            sender: "test".to_string(),
            sequence_nr: 0,
            sent_at: Instant::now(),
            label: None,
        }
    }

    pub fn with_sender(mut self, sender: &str) -> Self {
        self.sender = sender.to_string();
        self
    }

    pub fn with_label(mut self, label: &str) -> Self {
        self.label = Some(label.to_string());
        self
    }

    pub fn with_sequence(mut self, seq: u64) -> Self {
        self.sequence_nr = seq;
        self
    }

    pub fn age(&self) -> Duration {
        self.sent_at.elapsed()
    }
}

// ============================================================================
// TestActorRef
// ============================================================================

/// A test wrapper for actors that allows direct state inspection and message injection
pub struct TestActorRef {
    name: String,
    /// Captured state snapshots
    snapshots: Vec<ActorStateSnapshot>,
    /// Messages sent to this actor
    received: VecDeque<TestMessage>,
    /// Messages sent by this actor
    sent: VecDeque<TestMessage>,
    /// Current lifecycle state
    lifecycle: LifecycleState,
    /// Message processing history
    processing_history: Vec<ProcessingRecord>,
    /// Auto-increment for sequence numbers
    next_seq: u64,
    stats: TestActorRefStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleState {
    Created,
    Started,
    Running,
    Suspended,
    Stopped,
    Restarted,
}

/// Record of a processed message
#[derive(Debug, Clone)]
pub struct ProcessingRecord {
    pub message_label: String,
    pub started_at: Instant,
    pub duration: Duration,
    pub result: ProcessingResult,
    pub state_before: String,
    pub state_after: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessingResult {
    Handled,
    Unhandled,
    Failed,
    Stashed,
}

#[derive(Debug, Clone, Default)]
pub struct TestActorRefStats {
    pub messages_received: u64,
    pub messages_sent: u64,
    pub messages_handled: u64,
    pub messages_unhandled: u64,
    pub messages_failed: u64,
    pub restarts: u64,
    pub state_transitions: u64,
}

impl TestActorRef {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            snapshots: Vec::new(),
            received: VecDeque::new(),
            sent: VecDeque::new(),
            lifecycle: LifecycleState::Created,
            processing_history: Vec::new(),
            next_seq: 1,
            stats: TestActorRefStats::default(),
        }
    }

    /// Get actor name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get current lifecycle state
    pub fn lifecycle(&self) -> LifecycleState {
        self.lifecycle
    }

    /// Transition lifecycle state
    pub fn set_lifecycle(&mut self, state: LifecycleState) {
        if state == LifecycleState::Restarted {
            self.stats.restarts += 1;
        }
        self.lifecycle = state;
    }

    // ========================================================================
    // State Inspection
    // ========================================================================

    /// Capture a state snapshot
    pub fn capture_state(&mut self, snapshot: ActorStateSnapshot) {
        self.stats.state_transitions += 1;
        self.snapshots.push(snapshot);
    }

    /// Get the latest state snapshot
    pub fn current_state(&self) -> Option<&ActorStateSnapshot> {
        self.snapshots.last()
    }

    /// Get all state snapshots (history)
    pub fn state_history(&self) -> &[ActorStateSnapshot] {
        &self.snapshots
    }

    /// Get state snapshot at index
    pub fn state_at(&self, index: usize) -> Option<&ActorStateSnapshot> {
        self.snapshots.get(index)
    }

    /// Assert current state name
    pub fn assert_state_name(&self, expected: &str) -> bool {
        self.current_state()
            .map_or(false, |s| s.state_name == expected)
    }

    /// Assert a property value in current state
    pub fn assert_state_property(&self, key: &str, expected: &StateValue) -> bool {
        self.current_state()
            .and_then(|s| s.get(key))
            .map_or(false, |v| v == expected)
    }

    // ========================================================================
    // Message Injection & Tracking
    // ========================================================================

    /// Inject a message into this actor
    pub fn tell(&mut self, payload: Vec<u8>) {
        let seq = self.next_seq;
        self.next_seq += 1;
        let msg = TestMessage::new(payload).with_sequence(seq);
        self.received.push_back(msg);
        self.stats.messages_received += 1;
    }

    /// Inject a message with sender info
    pub fn tell_from(&mut self, payload: Vec<u8>, sender: &str) {
        let seq = self.next_seq;
        self.next_seq += 1;
        let msg = TestMessage::new(payload)
            .with_sender(sender)
            .with_sequence(seq);
        self.received.push_back(msg);
        self.stats.messages_received += 1;
    }

    /// Inject a labeled message
    pub fn tell_labeled(&mut self, payload: Vec<u8>, label: &str) {
        let seq = self.next_seq;
        self.next_seq += 1;
        let msg = TestMessage::new(payload)
            .with_label(label)
            .with_sequence(seq);
        self.received.push_back(msg);
        self.stats.messages_received += 1;
    }

    /// Record that the actor sent a message
    pub fn record_sent(&mut self, payload: Vec<u8>, target: &str) {
        let msg = TestMessage::new(payload).with_sender(target);
        self.sent.push_back(msg);
        self.stats.messages_sent += 1;
    }

    /// Get next received message
    pub fn next_received(&mut self) -> Option<TestMessage> {
        self.received.pop_front()
    }

    /// Peek at next received message without removing
    pub fn peek_received(&self) -> Option<&TestMessage> {
        self.received.front()
    }

    /// Get number of pending received messages
    pub fn pending_count(&self) -> usize {
        self.received.len()
    }

    /// Get sent messages count
    pub fn sent_count(&self) -> usize {
        self.sent.len()
    }

    /// Drain all sent messages
    pub fn drain_sent(&mut self) -> Vec<TestMessage> {
        self.sent.drain(..).collect()
    }

    // ========================================================================
    // Processing Records
    // ========================================================================

    /// Record a message processing event
    pub fn record_processing(
        &mut self,
        label: &str,
        duration: Duration,
        result: ProcessingResult,
        state_before: &str,
        state_after: &str,
    ) {
        match result {
            ProcessingResult::Handled => self.stats.messages_handled += 1,
            ProcessingResult::Unhandled => self.stats.messages_unhandled += 1,
            ProcessingResult::Failed => self.stats.messages_failed += 1,
            ProcessingResult::Stashed => {}
        }
        self.processing_history.push(ProcessingRecord {
            message_label: label.to_string(),
            started_at: Instant::now(),
            duration,
            result,
            state_before: state_before.to_string(),
            state_after: state_after.to_string(),
        });
    }

    /// Get processing history
    pub fn processing_history(&self) -> &[ProcessingRecord] {
        &self.processing_history
    }

    /// Get last N processing records
    pub fn last_processed(&self, n: usize) -> &[ProcessingRecord] {
        let start = self.processing_history.len().saturating_sub(n);
        &self.processing_history[start..]
    }

    /// Check if any message caused a state transition
    pub fn had_transition(&self, from: &str, to: &str) -> bool {
        self.processing_history
            .iter()
            .any(|r| r.state_before == from && r.state_after == to)
    }

    /// Count messages with a specific result
    pub fn count_result(&self, result: ProcessingResult) -> usize {
        self.processing_history
            .iter()
            .filter(|r| r.result == result)
            .count()
    }

    /// Average processing time
    pub fn avg_processing_time(&self) -> Option<Duration> {
        if self.processing_history.is_empty() {
            return None;
        }
        let total: Duration = self.processing_history.iter().map(|r| r.duration).sum();
        Some(total / self.processing_history.len() as u32)
    }

    // ========================================================================
    // Stats
    // ========================================================================

    pub fn stats(&self) -> &TestActorRefStats {
        &self.stats
    }

    /// Reset all state (for reuse between test cases)
    pub fn reset(&mut self) {
        self.snapshots.clear();
        self.received.clear();
        self.sent.clear();
        self.processing_history.clear();
        self.lifecycle = LifecycleState::Created;
        self.next_seq = 1;
        self.stats = TestActorRefStats::default();
    }
}

impl fmt::Debug for TestActorRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestActorRef")
            .field("name", &self.name)
            .field("lifecycle", &self.lifecycle)
            .field("pending", &self.received.len())
            .field("snapshots", &self.snapshots.len())
            .finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_actor_ref() {
        let actor = TestActorRef::new("test-actor");
        assert_eq!(actor.name(), "test-actor");
        assert_eq!(actor.lifecycle(), LifecycleState::Created);
        assert_eq!(actor.pending_count(), 0);
    }

    #[test]
    fn test_lifecycle_transitions() {
        let mut actor = TestActorRef::new("actor");
        actor.set_lifecycle(LifecycleState::Started);
        assert_eq!(actor.lifecycle(), LifecycleState::Started);
        actor.set_lifecycle(LifecycleState::Running);
        assert_eq!(actor.lifecycle(), LifecycleState::Running);
    }

    #[test]
    fn test_restart_counted() {
        let mut actor = TestActorRef::new("actor");
        actor.set_lifecycle(LifecycleState::Restarted);
        actor.set_lifecycle(LifecycleState::Restarted);
        assert_eq!(actor.stats().restarts, 2);
    }

    #[test]
    fn test_state_snapshot() {
        let mut actor = TestActorRef::new("actor");
        let snapshot = ActorStateSnapshot::new("idle")
            .with_property("count", StateValue::Int(0))
            .with_property("name", StateValue::String("test".to_string()));
        actor.capture_state(snapshot);
        assert!(actor.assert_state_name("idle"));
        assert!(actor.assert_state_property("count", &StateValue::Int(0)));
    }

    #[test]
    fn test_state_history() {
        let mut actor = TestActorRef::new("actor");
        actor.capture_state(ActorStateSnapshot::new("state1"));
        actor.capture_state(ActorStateSnapshot::new("state2"));
        actor.capture_state(ActorStateSnapshot::new("state3"));
        assert_eq!(actor.state_history().len(), 3);
        assert_eq!(actor.state_at(0).unwrap().state_name, "state1");
        assert!(actor.assert_state_name("state3"));
    }

    #[test]
    fn test_state_getters() {
        let snapshot = ActorStateSnapshot::new("test")
            .with_property("name", StateValue::String("hello".to_string()))
            .with_property("count", StateValue::Int(42))
            .with_property("active", StateValue::Bool(true));
        assert_eq!(snapshot.get_string("name"), Some("hello"));
        assert_eq!(snapshot.get_int("count"), Some(42));
        assert_eq!(snapshot.get_bool("active"), Some(true));
        assert!(snapshot.has_property("name"));
        assert!(!snapshot.has_property("missing"));
        assert_eq!(snapshot.property_count(), 3);
    }

    #[test]
    fn test_tell_messages() {
        let mut actor = TestActorRef::new("actor");
        actor.tell(vec![1, 2, 3]);
        actor.tell(vec![4, 5, 6]);
        assert_eq!(actor.pending_count(), 2);
        assert_eq!(actor.stats().messages_received, 2);
    }

    #[test]
    fn test_tell_from() {
        let mut actor = TestActorRef::new("actor");
        actor.tell_from(vec![1], "sender-1");
        let msg = actor.next_received().unwrap();
        assert_eq!(msg.sender, "sender-1");
    }

    #[test]
    fn test_tell_labeled() {
        let mut actor = TestActorRef::new("actor");
        actor.tell_labeled(vec![1], "greet");
        let msg = actor.peek_received().unwrap();
        assert_eq!(msg.label.as_deref(), Some("greet"));
    }

    #[test]
    fn test_sequence_numbers() {
        let mut actor = TestActorRef::new("actor");
        actor.tell(vec![1]);
        actor.tell(vec![2]);
        let m1 = actor.next_received().unwrap();
        let m2 = actor.next_received().unwrap();
        assert_eq!(m1.sequence_nr, 1);
        assert_eq!(m2.sequence_nr, 2);
    }

    #[test]
    fn test_record_sent() {
        let mut actor = TestActorRef::new("actor");
        actor.record_sent(vec![1, 2], "target");
        assert_eq!(actor.sent_count(), 1);
        let sent = actor.drain_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(actor.sent_count(), 0);
    }

    #[test]
    fn test_processing_record() {
        let mut actor = TestActorRef::new("actor");
        actor.record_processing(
            "msg1",
            Duration::from_millis(5),
            ProcessingResult::Handled,
            "idle",
            "active",
        );
        assert_eq!(actor.processing_history().len(), 1);
        assert_eq!(actor.stats().messages_handled, 1);
    }

    #[test]
    fn test_had_transition() {
        let mut actor = TestActorRef::new("actor");
        actor.record_processing("m", Duration::from_millis(1), ProcessingResult::Handled, "idle", "active");
        assert!(actor.had_transition("idle", "active"));
        assert!(!actor.had_transition("active", "idle"));
    }

    #[test]
    fn test_count_result() {
        let mut actor = TestActorRef::new("actor");
        actor.record_processing("m1", Duration::from_millis(1), ProcessingResult::Handled, "a", "a");
        actor.record_processing("m2", Duration::from_millis(1), ProcessingResult::Unhandled, "a", "a");
        actor.record_processing("m3", Duration::from_millis(1), ProcessingResult::Handled, "a", "a");
        assert_eq!(actor.count_result(ProcessingResult::Handled), 2);
        assert_eq!(actor.count_result(ProcessingResult::Unhandled), 1);
    }

    #[test]
    fn test_avg_processing_time() {
        let mut actor = TestActorRef::new("actor");
        assert!(actor.avg_processing_time().is_none());
        actor.record_processing("m1", Duration::from_millis(10), ProcessingResult::Handled, "a", "a");
        actor.record_processing("m2", Duration::from_millis(20), ProcessingResult::Handled, "a", "a");
        let avg = actor.avg_processing_time().unwrap();
        assert_eq!(avg, Duration::from_millis(15));
    }

    #[test]
    fn test_last_processed() {
        let mut actor = TestActorRef::new("actor");
        for i in 0..5 {
            actor.record_processing(&format!("m{}", i), Duration::from_millis(1), ProcessingResult::Handled, "a", "a");
        }
        let last2 = actor.last_processed(2);
        assert_eq!(last2.len(), 2);
        assert_eq!(last2[0].message_label, "m3");
    }

    #[test]
    fn test_reset() {
        let mut actor = TestActorRef::new("actor");
        actor.tell(vec![1]);
        actor.capture_state(ActorStateSnapshot::new("s1"));
        actor.record_sent(vec![2], "target");
        actor.reset();
        assert_eq!(actor.pending_count(), 0);
        assert_eq!(actor.sent_count(), 0);
        assert!(actor.current_state().is_none());
        assert_eq!(actor.lifecycle(), LifecycleState::Created);
    }

    #[test]
    fn test_state_value_display() {
        assert_eq!(format!("{}", StateValue::Int(42)), "42");
        assert_eq!(format!("{}", StateValue::Bool(true)), "true");
        assert_eq!(format!("{}", StateValue::None), "None");
        assert_eq!(format!("{}", StateValue::String("hi".into())), "\"hi\"");
    }

    #[test]
    fn test_debug_display() {
        let actor = TestActorRef::new("test");
        let debug = format!("{:?}", actor);
        assert!(debug.contains("TestActorRef"));
        assert!(debug.contains("test"));
    }

    #[test]
    fn test_message_age() {
        let msg = TestMessage::new(vec![1]);
        std::thread::sleep(Duration::from_millis(5));
        assert!(msg.age() >= Duration::from_millis(5));
    }

    #[test]
    fn test_stats_comprehensive() {
        let mut actor = TestActorRef::new("actor");
        actor.tell(vec![1]);
        actor.tell(vec![2]);
        actor.record_sent(vec![3], "t");
        actor.record_processing("m", Duration::from_millis(1), ProcessingResult::Failed, "a", "a");
        actor.capture_state(ActorStateSnapshot::new("s"));
        assert_eq!(actor.stats().messages_received, 2);
        assert_eq!(actor.stats().messages_sent, 1);
        assert_eq!(actor.stats().messages_failed, 1);
        assert_eq!(actor.stats().state_transitions, 1);
    }
}

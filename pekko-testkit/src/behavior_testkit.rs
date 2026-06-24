//! # Behavior TestKit - Isolated Actor Behavior Testing
//!
//! Test actor behaviors without a full ActorSystem. Inspired by
//! Akka/Pekko's `BehaviorTestKit` for lightweight, synchronous testing.
//!
//! ## Key Components
//! - **BehaviorTestKit**: Run a behavior function and collect effects
//! - **TestInbox**: Simulated message inbox for feeding messages
//! - **Effect**: Outcomes of processing (messages sent, children spawned, timers)
//! - **BehaviorResult**: Return value from behavior processing

use std::collections::VecDeque;
use std::fmt;
use std::time::{Duration, Instant};

// ============================================================================
// Effect — what happened when a behavior processed a message
// ============================================================================

/// Represents a side-effect produced by a behavior during message processing
#[derive(Debug, Clone)]
pub enum Effect {
    /// A message was sent to another actor
    MessageSent {
        target: String,
        message: String,
    },
    /// A child actor was spawned
    Spawned {
        child_name: String,
        behavior_type: String,
    },
    /// A timer was scheduled
    TimerScheduled {
        key: String,
        delay: Duration,
        is_repeat: bool,
    },
    /// A timer was cancelled
    TimerCancelled {
        key: String,
    },
    /// The behavior stopped itself
    Stopped,
    /// The behavior was restarted
    Restarted,
    /// A log message was emitted
    Logged {
        level: LogLevel,
        message: String,
    },
    /// Custom user-defined effect
    Custom(String),
    /// No effect (message was consumed silently)
    NoEffect,
}

impl fmt::Display for Effect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Effect::MessageSent { target, message } => {
                write!(f, "MessageSent({} -> {})", message, target)
            }
            Effect::Spawned { child_name, behavior_type } => {
                write!(f, "Spawned({}: {})", child_name, behavior_type)
            }
            Effect::TimerScheduled { key, delay, is_repeat } => {
                write!(f, "Timer({}, {:?}, repeat={})", key, delay, is_repeat)
            }
            Effect::TimerCancelled { key } => write!(f, "TimerCancelled({})", key),
            Effect::Stopped => write!(f, "Stopped"),
            Effect::Restarted => write!(f, "Restarted"),
            Effect::Logged { level, message } => write!(f, "Log({}: {})", level, message),
            Effect::Custom(s) => write!(f, "Custom({})", s),
            Effect::NoEffect => write!(f, "NoEffect"),
        }
    }
}

/// Log levels for behavior logging effects
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogLevel::Trace => write!(f, "TRACE"),
            LogLevel::Debug => write!(f, "DEBUG"),
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Warn => write!(f, "WARN"),
            LogLevel::Error => write!(f, "ERROR"),
        }
    }
}

// ============================================================================
// BehaviorResult — what the behavior wants to do next
// ============================================================================

/// Result of processing a message — determines next behavior
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BehaviorResult {
    /// Continue with current behavior
    Same,
    /// Stop the behavior
    Stop,
    /// Restart the behavior
    Restart,
    /// Switch to a named behavior variant
    Become(String),
    /// Unhandled message
    Unhandled,
}

// ============================================================================
// TestInbox — simulated message source
// ============================================================================

/// A simulated inbox for feeding messages into a behavior under test
pub struct TestInbox<T> {
    messages: VecDeque<T>,
}

impl<T: fmt::Debug> TestInbox<T> {
    pub fn new() -> Self {
        Self {
            messages: VecDeque::new(),
        }
    }

    /// Add a message to the inbox
    pub fn send(&mut self, msg: T) {
        self.messages.push_back(msg);
    }

    /// Add multiple messages
    pub fn send_all(&mut self, msgs: impl IntoIterator<Item = T>) {
        self.messages.extend(msgs);
    }

    /// Check if inbox is empty
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Number of pending messages
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// Take the next message from the inbox
    pub fn take(&mut self) -> Option<T> {
        self.messages.pop_front()
    }

    /// Peek at the next message without consuming
    pub fn peek(&self) -> Option<&T> {
        self.messages.front()
    }

    /// Clear all pending messages
    pub fn clear(&mut self) {
        self.messages.clear();
    }
}

impl<T: fmt::Debug> Default for TestInbox<T> {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// EffectCollector — context passed to behavior for recording effects
// ============================================================================

/// Context that behaviors use to record their side-effects
pub struct EffectCollector {
    effects: Vec<Effect>,
    current_behavior: String,
    children: Vec<String>,
    timers: Vec<(String, Duration, bool)>,
}

impl EffectCollector {
    pub fn new() -> Self {
        Self {
            effects: Vec::new(),
            current_behavior: "initial".to_string(),
            children: Vec::new(),
            timers: Vec::new(),
        }
    }

    /// Record sending a message to a target
    pub fn send_message(&mut self, target: impl Into<String>, message: impl Into<String>) {
        self.effects.push(Effect::MessageSent {
            target: target.into(),
            message: message.into(),
        });
    }

    /// Record spawning a child actor
    pub fn spawn_child(&mut self, name: impl Into<String>, behavior_type: impl Into<String>) {
        let name = name.into();
        self.children.push(name.clone());
        self.effects.push(Effect::Spawned {
            child_name: name,
            behavior_type: behavior_type.into(),
        });
    }

    /// Record scheduling a timer
    pub fn schedule_timer(&mut self, key: impl Into<String>, delay: Duration, repeat: bool) {
        let key = key.into();
        self.timers.push((key.clone(), delay, repeat));
        self.effects.push(Effect::TimerScheduled {
            key,
            delay,
            is_repeat: repeat,
        });
    }

    /// Record cancelling a timer
    pub fn cancel_timer(&mut self, key: impl Into<String>) {
        let key = key.into();
        self.timers.retain(|(k, _, _)| k != &key);
        self.effects.push(Effect::TimerCancelled { key });
    }

    /// Record a log event
    pub fn log(&mut self, level: LogLevel, message: impl Into<String>) {
        self.effects.push(Effect::Logged {
            level,
            message: message.into(),
        });
    }

    /// Record stopping
    pub fn stop(&mut self) {
        self.effects.push(Effect::Stopped);
    }

    /// Record restart
    pub fn restart(&mut self) {
        self.effects.push(Effect::Restarted);
    }

    /// Record a custom effect
    pub fn custom(&mut self, description: impl Into<String>) {
        self.effects.push(Effect::Custom(description.into()));
    }

    /// Get spawned children names
    pub fn spawned_children(&self) -> &[String] {
        &self.children
    }

    /// Get active timer keys
    pub fn active_timers(&self) -> Vec<&str> {
        self.timers.iter().map(|(k, _, _)| k.as_str()).collect()
    }

    /// Drain all collected effects
    fn drain(&mut self) -> Vec<Effect> {
        self.effects.drain(..).collect()
    }
}

impl Default for EffectCollector {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// BehaviorTestKit — the main testing harness
// ============================================================================

/// Lightweight behavior testing without a full ActorSystem.
///
/// Feed messages into a behavior function and assert on the produced effects.
///
/// ```ignore
/// let mut kit = BehaviorTestKit::new("greeter");
/// let effects = kit.run_with(|ctx| {
///     ctx.send_message("caller", "Hello!");
///     BehaviorResult::Same
/// });
/// assert!(effects.iter().any(|e| matches!(e, Effect::MessageSent { .. })));
/// ```
pub struct BehaviorTestKit {
    name: String,
    collector: EffectCollector,
    behavior_state: BehaviorResult,
    total_messages_processed: u64,
    total_effects: u64,
    started_at: Instant,
    run_log: Vec<RunRecord>,
}

/// Record of a single behavior run
#[derive(Debug, Clone)]
pub struct RunRecord {
    pub message_index: u64,
    pub effects: Vec<Effect>,
    pub result: BehaviorResult,
    pub duration: Duration,
}

impl BehaviorTestKit {
    /// Create a new BehaviorTestKit with a given actor name
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            collector: EffectCollector::new(),
            behavior_state: BehaviorResult::Same,
            total_messages_processed: 0,
            total_effects: 0,
            started_at: Instant::now(),
            run_log: Vec::new(),
        }
    }

    /// Get the actor name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Run a behavior function and collect its effects
    pub fn run_with<F>(&mut self, behavior_fn: F) -> Vec<Effect>
    where
        F: FnOnce(&mut EffectCollector) -> BehaviorResult,
    {
        let start = Instant::now();
        let result = behavior_fn(&mut self.collector);
        let effects = self.collector.drain();
        let duration = start.elapsed();

        self.total_messages_processed += 1;
        self.total_effects += effects.len() as u64;
        self.behavior_state = result.clone();

        self.run_log.push(RunRecord {
            message_index: self.total_messages_processed,
            effects: effects.clone(),
            result,
            duration,
        });

        effects
    }

    /// Run multiple behavior invocations
    pub fn run_many<F>(&mut self, count: usize, mut behavior_fn: F) -> Vec<Vec<Effect>>
    where
        F: FnMut(usize, &mut EffectCollector) -> BehaviorResult,
    {
        let mut all_effects = Vec::with_capacity(count);
        for i in 0..count {
            let start = Instant::now();
            let result = behavior_fn(i, &mut self.collector);
            let effects = self.collector.drain();
            let duration = start.elapsed();

            self.total_messages_processed += 1;
            self.total_effects += effects.len() as u64;
            self.behavior_state = result.clone();

            self.run_log.push(RunRecord {
                message_index: self.total_messages_processed,
                effects: effects.clone(),
                result,
                duration,
            });

            all_effects.push(effects);
        }
        all_effects
    }

    /// Get current behavior state
    pub fn current_state(&self) -> &BehaviorResult {
        &self.behavior_state
    }

    /// Check if behavior has stopped
    pub fn is_stopped(&self) -> bool {
        self.behavior_state == BehaviorResult::Stop
    }

    /// Check if behavior became a named variant
    pub fn became(&self, name: &str) -> bool {
        self.behavior_state == BehaviorResult::Become(name.to_string())
    }

    /// Get spawned children
    pub fn spawned_children(&self) -> &[String] {
        self.collector.spawned_children()
    }

    /// Get active timers
    pub fn active_timers(&self) -> Vec<&str> {
        self.collector.active_timers()
    }

    /// Total messages processed
    pub fn messages_processed(&self) -> u64 {
        self.total_messages_processed
    }

    /// Total effects produced
    pub fn total_effects(&self) -> u64 {
        self.total_effects
    }

    /// Get the run log
    pub fn run_log(&self) -> &[RunRecord] {
        &self.run_log
    }

    /// Get the last run record
    pub fn last_run(&self) -> Option<&RunRecord> {
        self.run_log.last()
    }

    /// Check if a specific effect type was ever produced
    pub fn has_effect<F>(&self, predicate: F) -> bool
    where
        F: Fn(&Effect) -> bool,
    {
        self.run_log.iter().any(|r| r.effects.iter().any(&predicate))
    }

    /// Count effects matching a predicate across all runs
    pub fn count_effects<F>(&self, predicate: F) -> usize
    where
        F: Fn(&Effect) -> bool,
    {
        self.run_log
            .iter()
            .flat_map(|r| r.effects.iter())
            .filter(|e| predicate(e))
            .count()
    }

    /// Get uptime since creation
    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Reset state (clear run log, effects, restart behavior)
    pub fn reset(&mut self) {
        self.collector = EffectCollector::new();
        self.behavior_state = BehaviorResult::Same;
        self.total_messages_processed = 0;
        self.total_effects = 0;
        self.run_log.clear();
    }
}

// ============================================================================
// Assertion helpers
// ============================================================================

/// Assert that effects contain a MessageSent to a specific target
pub fn assert_message_sent(effects: &[Effect], target: &str) {
    assert!(
        effects.iter().any(|e| matches!(e, Effect::MessageSent { target: t, .. } if t == target)),
        "Expected MessageSent to '{}', got: {:?}",
        target,
        effects
    );
}

/// Assert that effects contain a Spawned child with a given name
pub fn assert_spawned(effects: &[Effect], child_name: &str) {
    assert!(
        effects.iter().any(|e| matches!(e, Effect::Spawned { child_name: n, .. } if n == child_name)),
        "Expected Spawned '{}', got: {:?}",
        child_name,
        effects
    );
}

/// Assert that effects contain a TimerScheduled with a given key
pub fn assert_timer_scheduled(effects: &[Effect], key: &str) {
    assert!(
        effects.iter().any(|e| matches!(e, Effect::TimerScheduled { key: k, .. } if k == key)),
        "Expected TimerScheduled '{}', got: {:?}",
        key,
        effects
    );
}

/// Assert no effects were produced
pub fn assert_no_effects(effects: &[Effect]) {
    let meaningful: Vec<_> = effects.iter().filter(|e| !matches!(e, Effect::NoEffect)).collect();
    assert!(
        meaningful.is_empty(),
        "Expected no effects, got: {:?}",
        meaningful
    );
}

/// Assert that effects contain a Stopped effect
pub fn assert_stopped(effects: &[Effect]) {
    assert!(
        effects.iter().any(|e| matches!(e, Effect::Stopped)),
        "Expected Stopped effect, got: {:?}",
        effects
    );
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // --- Effect Display ---

    #[test]
    fn test_effect_display() {
        let e = Effect::MessageSent {
            target: "actor-1".into(),
            message: "hello".into(),
        };
        assert!(e.to_string().contains("actor-1"));

        let e = Effect::Spawned {
            child_name: "worker".into(),
            behavior_type: "Worker".into(),
        };
        assert!(e.to_string().contains("worker"));

        let e = Effect::Stopped;
        assert_eq!(e.to_string(), "Stopped");

        let e = Effect::NoEffect;
        assert_eq!(e.to_string(), "NoEffect");
    }

    #[test]
    fn test_log_level_display() {
        assert_eq!(LogLevel::Info.to_string(), "INFO");
        assert_eq!(LogLevel::Error.to_string(), "ERROR");
        assert_eq!(LogLevel::Trace.to_string(), "TRACE");
    }

    // --- TestInbox ---

    #[test]
    fn test_inbox_basic() {
        let mut inbox = TestInbox::<String>::new();
        assert!(inbox.is_empty());
        assert_eq!(inbox.len(), 0);

        inbox.send("hello".into());
        assert!(!inbox.is_empty());
        assert_eq!(inbox.len(), 1);

        let msg = inbox.take().unwrap();
        assert_eq!(msg, "hello");
        assert!(inbox.is_empty());
    }

    #[test]
    fn test_inbox_send_all() {
        let mut inbox = TestInbox::new();
        inbox.send_all(vec![1, 2, 3]);
        assert_eq!(inbox.len(), 3);
        assert_eq!(inbox.take(), Some(1));
        assert_eq!(inbox.take(), Some(2));
        assert_eq!(inbox.take(), Some(3));
        assert_eq!(inbox.take(), None);
    }

    #[test]
    fn test_inbox_peek() {
        let mut inbox = TestInbox::new();
        inbox.send(42);
        assert_eq!(inbox.peek(), Some(&42));
        assert_eq!(inbox.len(), 1); // not consumed
        inbox.take();
        assert!(inbox.peek().is_none());
    }

    #[test]
    fn test_inbox_clear() {
        let mut inbox = TestInbox::new();
        inbox.send_all(vec![1, 2, 3]);
        inbox.clear();
        assert!(inbox.is_empty());
    }

    // --- EffectCollector ---

    #[test]
    fn test_collector_send_message() {
        let mut c = EffectCollector::new();
        c.send_message("target-1", "hello");
        let effects = c.drain();
        assert_eq!(effects.len(), 1);
        assert!(matches!(&effects[0], Effect::MessageSent { target, message }
            if target == "target-1" && message == "hello"));
    }

    #[test]
    fn test_collector_spawn_child() {
        let mut c = EffectCollector::new();
        c.spawn_child("worker-1", "WorkerBehavior");
        assert_eq!(c.spawned_children(), &["worker-1"]);
        let effects = c.drain();
        assert!(matches!(&effects[0], Effect::Spawned { child_name, .. } if child_name == "worker-1"));
    }

    #[test]
    fn test_collector_timers() {
        let mut c = EffectCollector::new();
        c.schedule_timer("tick", Duration::from_secs(1), true);
        c.schedule_timer("timeout", Duration::from_secs(5), false);
        assert_eq!(c.active_timers().len(), 2);

        c.cancel_timer("tick");
        assert_eq!(c.active_timers().len(), 1);
        assert_eq!(c.active_timers()[0], "timeout");
    }

    #[test]
    fn test_collector_log() {
        let mut c = EffectCollector::new();
        c.log(LogLevel::Info, "started processing");
        let effects = c.drain();
        assert!(matches!(&effects[0], Effect::Logged { level: LogLevel::Info, message } if message == "started processing"));
    }

    #[test]
    fn test_collector_stop_restart() {
        let mut c = EffectCollector::new();
        c.stop();
        c.restart();
        let effects = c.drain();
        assert!(matches!(&effects[0], Effect::Stopped));
        assert!(matches!(&effects[1], Effect::Restarted));
    }

    #[test]
    fn test_collector_custom_effect() {
        let mut c = EffectCollector::new();
        c.custom("persisted event #42");
        let effects = c.drain();
        assert!(matches!(&effects[0], Effect::Custom(s) if s == "persisted event #42"));
    }

    // --- BehaviorTestKit ---

    #[test]
    fn test_kit_creation() {
        let kit = BehaviorTestKit::new("test-actor");
        assert_eq!(kit.name(), "test-actor");
        assert_eq!(kit.messages_processed(), 0);
        assert_eq!(kit.total_effects(), 0);
        assert!(!kit.is_stopped());
    }

    #[test]
    fn test_kit_run_with_no_effects() {
        let mut kit = BehaviorTestKit::new("actor");
        let effects = kit.run_with(|_ctx| BehaviorResult::Same);
        assert!(effects.is_empty());
        assert_eq!(kit.messages_processed(), 1);
        assert_eq!(*kit.current_state(), BehaviorResult::Same);
    }

    #[test]
    fn test_kit_run_with_message_send() {
        let mut kit = BehaviorTestKit::new("greeter");
        let effects = kit.run_with(|ctx| {
            ctx.send_message("caller", "Hello!");
            BehaviorResult::Same
        });
        assert_eq!(effects.len(), 1);
        assert_message_sent(&effects, "caller");
    }

    #[test]
    fn test_kit_run_with_spawn() {
        let mut kit = BehaviorTestKit::new("supervisor");
        let effects = kit.run_with(|ctx| {
            ctx.spawn_child("worker-1", "Worker");
            ctx.spawn_child("worker-2", "Worker");
            BehaviorResult::Same
        });
        assert_eq!(effects.len(), 2);
        assert_spawned(&effects, "worker-1");
        assert_spawned(&effects, "worker-2");
        assert_eq!(kit.spawned_children().len(), 2);
    }

    #[test]
    fn test_kit_behavior_stop() {
        let mut kit = BehaviorTestKit::new("actor");
        let effects = kit.run_with(|ctx| {
            ctx.stop();
            BehaviorResult::Stop
        });
        assert!(kit.is_stopped());
        assert_stopped(&effects);
    }

    #[test]
    fn test_kit_behavior_become() {
        let mut kit = BehaviorTestKit::new("actor");
        kit.run_with(|_ctx| BehaviorResult::Become("active".into()));
        assert!(kit.became("active"));
        assert!(!kit.became("idle"));
    }

    #[test]
    fn test_kit_run_many() {
        let mut kit = BehaviorTestKit::new("counter");
        let all_effects = kit.run_many(3, |i, ctx| {
            ctx.send_message("logger", format!("count={}", i));
            BehaviorResult::Same
        });
        assert_eq!(all_effects.len(), 3);
        assert_eq!(kit.messages_processed(), 3);
        assert_eq!(kit.total_effects(), 3);
    }

    #[test]
    fn test_kit_run_log() {
        let mut kit = BehaviorTestKit::new("actor");
        kit.run_with(|ctx| {
            ctx.send_message("a", "msg1");
            BehaviorResult::Same
        });
        kit.run_with(|ctx| {
            ctx.send_message("b", "msg2");
            BehaviorResult::Stop
        });
        assert_eq!(kit.run_log().len(), 2);
        assert_eq!(kit.run_log()[0].result, BehaviorResult::Same);
        assert_eq!(kit.run_log()[1].result, BehaviorResult::Stop);
    }

    #[test]
    fn test_kit_last_run() {
        let mut kit = BehaviorTestKit::new("actor");
        assert!(kit.last_run().is_none());
        kit.run_with(|_ctx| BehaviorResult::Same);
        assert!(kit.last_run().is_some());
        assert_eq!(kit.last_run().unwrap().message_index, 1);
    }

    #[test]
    fn test_kit_has_effect() {
        let mut kit = BehaviorTestKit::new("actor");
        kit.run_with(|ctx| {
            ctx.spawn_child("w1", "Worker");
            BehaviorResult::Same
        });
        assert!(kit.has_effect(|e| matches!(e, Effect::Spawned { .. })));
        assert!(!kit.has_effect(|e| matches!(e, Effect::Stopped)));
    }

    #[test]
    fn test_kit_count_effects() {
        let mut kit = BehaviorTestKit::new("actor");
        kit.run_many(5, |i, ctx| {
            ctx.send_message("target", format!("msg-{}", i));
            if i % 2 == 0 {
                ctx.log(LogLevel::Debug, "even");
            }
            BehaviorResult::Same
        });
        assert_eq!(kit.count_effects(|e| matches!(e, Effect::MessageSent { .. })), 5);
        assert_eq!(kit.count_effects(|e| matches!(e, Effect::Logged { .. })), 3); // i=0,2,4
    }

    #[test]
    fn test_kit_reset() {
        let mut kit = BehaviorTestKit::new("actor");
        kit.run_with(|ctx| {
            ctx.spawn_child("w", "W");
            BehaviorResult::Stop
        });
        assert!(kit.is_stopped());
        assert_eq!(kit.messages_processed(), 1);

        kit.reset();
        assert!(!kit.is_stopped());
        assert_eq!(kit.messages_processed(), 0);
        assert!(kit.run_log().is_empty());
    }

    #[test]
    fn test_kit_timer_scheduling() {
        let mut kit = BehaviorTestKit::new("actor");
        kit.run_with(|ctx| {
            ctx.schedule_timer("heartbeat", Duration::from_secs(1), true);
            ctx.schedule_timer("timeout", Duration::from_secs(30), false);
            BehaviorResult::Same
        });
        let timers = kit.active_timers();
        assert_eq!(timers.len(), 2);
        assert!(timers.contains(&"heartbeat"));
        assert!(timers.contains(&"timeout"));
    }

    #[test]
    fn test_kit_timer_cancellation() {
        let mut kit = BehaviorTestKit::new("actor");
        kit.run_with(|ctx| {
            ctx.schedule_timer("tick", Duration::from_millis(100), true);
            BehaviorResult::Same
        });
        kit.run_with(|ctx| {
            ctx.cancel_timer("tick");
            BehaviorResult::Same
        });
        assert!(kit.active_timers().is_empty());
    }

    // --- Assertion helpers ---

    #[test]
    fn test_assert_no_effects_pass() {
        assert_no_effects(&[]);
        assert_no_effects(&[Effect::NoEffect]);
    }

    #[test]
    #[should_panic(expected = "Expected no effects")]
    fn test_assert_no_effects_fail() {
        assert_no_effects(&[Effect::Stopped]);
    }

    #[test]
    #[should_panic(expected = "Expected MessageSent")]
    fn test_assert_message_sent_fail() {
        assert_message_sent(&[], "target");
    }

    #[test]
    #[should_panic(expected = "Expected Spawned")]
    fn test_assert_spawned_fail() {
        assert_spawned(&[], "child");
    }

    #[test]
    fn test_behavior_result_unhandled() {
        let mut kit = BehaviorTestKit::new("actor");
        kit.run_with(|_ctx| BehaviorResult::Unhandled);
        assert_eq!(*kit.current_state(), BehaviorResult::Unhandled);
    }

    #[test]
    fn test_complex_behavior_scenario() {
        let mut kit = BehaviorTestKit::new("order-processor");

        // Phase 1: receive order
        let effects = kit.run_with(|ctx| {
            ctx.log(LogLevel::Info, "Order received");
            ctx.spawn_child("validator", "OrderValidator");
            ctx.schedule_timer("order-timeout", Duration::from_secs(30), false);
            BehaviorResult::Become("validating".into())
        });
        assert_eq!(effects.len(), 3);
        assert!(kit.became("validating"));

        // Phase 2: validation complete
        let effects = kit.run_with(|ctx| {
            ctx.cancel_timer("order-timeout");
            ctx.send_message("payment-service", "charge $50");
            BehaviorResult::Become("charging".into())
        });
        assert_eq!(effects.len(), 2);
        assert!(kit.became("charging"));

        // Phase 3: payment confirmed
        let effects = kit.run_with(|ctx| {
            ctx.send_message("customer", "Order confirmed!");
            ctx.log(LogLevel::Info, "Order completed");
            ctx.stop();
            BehaviorResult::Stop
        });
        assert!(kit.is_stopped());
        assert_eq!(kit.messages_processed(), 3);
        assert_eq!(effects.len(), 3);
    }
}

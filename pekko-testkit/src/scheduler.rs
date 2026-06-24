//! Test Scheduler — Virtual time for deterministic testing
//!
//! Provides a controllable time source that replaces real time in tests:
//!
//! - **TestScheduler**: Advance time manually, schedule callbacks
//! - **VirtualClock**: Monotonic virtual clock that only advances on command
//! - **ScheduledTask**: Tasks scheduled to fire at a virtual time
//! - **TimedAssertion**: Assert things happen at specific virtual times

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, Notify};
use tracing::debug;

// ============= Virtual Clock =============

/// A virtual clock that only advances when explicitly told to.
/// Thread-safe and shareable.
#[derive(Debug, Clone)]
pub struct VirtualClock {
    /// Current virtual time in microseconds.
    now_us: Arc<AtomicU64>,
    /// Notify waiters when time advances.
    notify: Arc<Notify>,
}

impl VirtualClock {
    pub fn new() -> Self {
        Self {
            now_us: Arc::new(AtomicU64::new(0)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Get current virtual time.
    pub fn now(&self) -> Duration {
        Duration::from_micros(self.now_us.load(AtomicOrdering::SeqCst))
    }

    /// Get current virtual time in milliseconds.
    pub fn now_millis(&self) -> u64 {
        self.now_us.load(AtomicOrdering::SeqCst) / 1000
    }

    /// Advance time by a duration.
    pub fn advance(&self, duration: Duration) {
        let us = duration.as_micros() as u64;
        self.now_us.fetch_add(us, AtomicOrdering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Set time to an absolute value.
    pub fn set(&self, time: Duration) {
        self.now_us.store(time.as_micros() as u64, AtomicOrdering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Wait until time is advanced (for cooperative testing).
    pub async fn wait_for_advance(&self) {
        self.notify.notified().await;
    }

    /// Reset clock to zero.
    pub fn reset(&self) {
        self.now_us.store(0, AtomicOrdering::SeqCst);
    }
}

// ============= Scheduled Task =============

/// A task scheduled to fire at a specific virtual time.
struct ScheduledTask {
    fire_at_us: u64,
    seq: u64,
    callback: Box<dyn FnOnce() + Send>,
}

impl Eq for ScheduledTask {}

impl PartialEq for ScheduledTask {
    fn eq(&self, other: &Self) -> bool {
        self.fire_at_us == other.fire_at_us && self.seq == other.seq
    }
}

impl Ord for ScheduledTask {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap: earlier fire time = higher priority
        other.fire_at_us.cmp(&self.fire_at_us)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl PartialOrd for ScheduledTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// ============= Test Scheduler =============

/// A scheduler with virtual time control.
/// Schedule tasks, advance time, and they fire deterministically.
pub struct TestScheduler {
    clock: VirtualClock,
    tasks: Arc<Mutex<BinaryHeap<ScheduledTask>>>,
    seq_counter: AtomicU64,
    executed_count: AtomicU64,
}

impl TestScheduler {
    pub fn new() -> Self {
        Self {
            clock: VirtualClock::new(),
            tasks: Arc::new(Mutex::new(BinaryHeap::new())),
            seq_counter: AtomicU64::new(0),
            executed_count: AtomicU64::new(0),
        }
    }

    /// Get the virtual clock.
    pub fn clock(&self) -> &VirtualClock {
        &self.clock
    }

    /// Schedule a one-shot task to fire after a delay from now.
    pub async fn schedule_once(&self, delay: Duration, callback: impl FnOnce() + Send + 'static) {
        let fire_at = self.clock.now() + delay;
        let seq = self.seq_counter.fetch_add(1, AtomicOrdering::Relaxed);
        let mut tasks = self.tasks.lock().await;
        tasks.push(ScheduledTask {
            fire_at_us: fire_at.as_micros() as u64,
            seq,
            callback: Box::new(callback),
        });
    }

    /// Schedule a task at an absolute virtual time.
    pub async fn schedule_at(&self, time: Duration, callback: impl FnOnce() + Send + 'static) {
        let seq = self.seq_counter.fetch_add(1, AtomicOrdering::Relaxed);
        let mut tasks = self.tasks.lock().await;
        tasks.push(ScheduledTask {
            fire_at_us: time.as_micros() as u64,
            seq,
            callback: Box::new(callback),
        });
    }

    /// Advance time and execute all tasks that should have fired.
    pub async fn advance(&self, duration: Duration) -> usize {
        self.clock.advance(duration);
        self.execute_pending().await
    }

    /// Advance to a specific time and execute tasks.
    pub async fn advance_to(&self, time: Duration) -> usize {
        self.clock.set(time);
        self.execute_pending().await
    }

    /// Execute all tasks whose fire time is <= current virtual time.
    async fn execute_pending(&self) -> usize {
        let now_us = self.clock.now().as_micros() as u64;
        let mut count = 0;

        loop {
            let task = {
                let mut tasks = self.tasks.lock().await;
                match tasks.peek() {
                    Some(t) if t.fire_at_us <= now_us => tasks.pop(),
                    _ => None,
                }
            };

            match task {
                Some(t) => {
                    (t.callback)();
                    count += 1;
                    self.executed_count.fetch_add(1, AtomicOrdering::Relaxed);
                }
                None => break,
            }
        }

        count
    }

    /// Get the number of pending (not yet executed) tasks.
    pub async fn pending_count(&self) -> usize {
        self.tasks.lock().await.len()
    }

    /// Get the total number of executed tasks.
    pub fn executed_count(&self) -> u64 {
        self.executed_count.load(AtomicOrdering::Relaxed)
    }

    /// Current virtual time.
    pub fn now(&self) -> Duration {
        self.clock.now()
    }

    /// Reset the scheduler (clear tasks, reset time).
    pub async fn reset(&self) {
        self.clock.reset();
        self.tasks.lock().await.clear();
        self.executed_count.store(0, AtomicOrdering::Relaxed);
    }
}

impl fmt::Debug for TestScheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestScheduler")
            .field("now", &self.clock.now())
            .field("executed", &self.executed_count())
            .finish()
    }
}

// ============= Timed Assertion =============

/// Assert that something happens at a specific virtual time.
pub struct TimedAssertion {
    clock: VirtualClock,
    events: Vec<(Duration, String)>,
}

impl TimedAssertion {
    pub fn new(clock: VirtualClock) -> Self {
        Self {
            clock,
            events: Vec::new(),
        }
    }

    /// Record an event at the current virtual time.
    pub fn record(&mut self, label: impl Into<String>) {
        self.events.push((self.clock.now(), label.into()));
    }

    /// Assert an event happened at a specific time.
    pub fn assert_event_at(&self, label: &str, expected_time: Duration) {
        let found = self.events.iter().find(|(_, l)| l == label);
        match found {
            Some((actual_time, _)) => {
                assert_eq!(
                    *actual_time, expected_time,
                    "Event '{}' happened at {:?}, expected {:?}",
                    label, actual_time, expected_time
                );
            }
            None => panic!("Event '{}' not found", label),
        }
    }

    /// Assert an event happened within a tolerance of a specific time.
    pub fn assert_event_near(&self, label: &str, expected: Duration, tolerance: Duration) {
        let found = self.events.iter().find(|(_, l)| l == label);
        match found {
            Some((actual, _)) => {
                let diff = if *actual > expected {
                    *actual - expected
                } else {
                    expected - *actual
                };
                assert!(
                    diff <= tolerance,
                    "Event '{}' at {:?}, expected {:?} ± {:?}",
                    label, actual, expected, tolerance
                );
            }
            None => panic!("Event '{}' not found", label),
        }
    }

    /// Assert events happened in order.
    pub fn assert_order(&self, labels: &[&str]) {
        let mut last_time = Duration::ZERO;
        for label in labels {
            let found = self.events.iter().find(|(_, l)| l == label);
            match found {
                Some((time, _)) => {
                    assert!(
                        *time >= last_time,
                        "Event '{}' at {:?} happened before previous event at {:?}",
                        label, time, last_time
                    );
                    last_time = *time;
                }
                None => panic!("Event '{}' not found", label),
            }
        }
    }

    /// Get all recorded events.
    pub fn events(&self) -> &[(Duration, String)] {
        &self.events
    }

    /// Get event count.
    pub fn count(&self) -> usize {
        self.events.len()
    }
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicI32;

    // ---- VirtualClock Tests ----

    #[test]
    fn test_virtual_clock_initial() {
        let clock = VirtualClock::new();
        assert_eq!(clock.now(), Duration::ZERO);
    }

    #[test]
    fn test_virtual_clock_advance() {
        let clock = VirtualClock::new();
        clock.advance(Duration::from_millis(100));
        assert_eq!(clock.now_millis(), 100);
        clock.advance(Duration::from_millis(50));
        assert_eq!(clock.now_millis(), 150);
    }

    #[test]
    fn test_virtual_clock_set() {
        let clock = VirtualClock::new();
        clock.set(Duration::from_secs(5));
        assert_eq!(clock.now(), Duration::from_secs(5));
    }

    #[test]
    fn test_virtual_clock_reset() {
        let clock = VirtualClock::new();
        clock.advance(Duration::from_secs(10));
        clock.reset();
        assert_eq!(clock.now(), Duration::ZERO);
    }

    #[test]
    fn test_virtual_clock_clone_shares_state() {
        let clock1 = VirtualClock::new();
        let clock2 = clock1.clone();
        clock1.advance(Duration::from_millis(200));
        assert_eq!(clock2.now_millis(), 200);
    }

    // ---- TestScheduler Tests ----

    #[tokio::test]
    async fn test_scheduler_schedule_and_advance() {
        let scheduler = TestScheduler::new();
        let counter = Arc::new(AtomicI32::new(0));
        let c = counter.clone();

        scheduler.schedule_once(Duration::from_millis(100), move || {
            c.fetch_add(1, AtomicOrdering::Relaxed);
        }).await;

        assert_eq!(counter.load(AtomicOrdering::Relaxed), 0);
        scheduler.advance(Duration::from_millis(50)).await;
        assert_eq!(counter.load(AtomicOrdering::Relaxed), 0); // not yet
        scheduler.advance(Duration::from_millis(60)).await;
        assert_eq!(counter.load(AtomicOrdering::Relaxed), 1); // fired!
    }

    #[tokio::test]
    async fn test_scheduler_multiple_tasks_order() {
        let scheduler = TestScheduler::new();
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));

        let l1 = log.clone();
        scheduler.schedule_once(Duration::from_millis(300), move || {
            l1.lock().unwrap().push("third");
        }).await;

        let l2 = log.clone();
        scheduler.schedule_once(Duration::from_millis(100), move || {
            l2.lock().unwrap().push("first");
        }).await;

        let l3 = log.clone();
        scheduler.schedule_once(Duration::from_millis(200), move || {
            l3.lock().unwrap().push("second");
        }).await;

        scheduler.advance(Duration::from_millis(500)).await;

        let result = log.lock().unwrap();
        assert_eq!(*result, vec!["first", "second", "third"]);
    }

    #[tokio::test]
    async fn test_scheduler_advance_to() {
        let scheduler = TestScheduler::new();
        let counter = Arc::new(AtomicI32::new(0));
        let c = counter.clone();

        scheduler.schedule_at(Duration::from_secs(5), move || {
            c.fetch_add(1, AtomicOrdering::Relaxed);
        }).await;

        scheduler.advance_to(Duration::from_secs(5)).await;
        assert_eq!(counter.load(AtomicOrdering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_scheduler_pending_count() {
        let scheduler = TestScheduler::new();
        scheduler.schedule_once(Duration::from_millis(100), || {}).await;
        scheduler.schedule_once(Duration::from_millis(200), || {}).await;
        assert_eq!(scheduler.pending_count().await, 2);

        scheduler.advance(Duration::from_millis(150)).await;
        assert_eq!(scheduler.pending_count().await, 1);
        assert_eq!(scheduler.executed_count(), 1);
    }

    #[tokio::test]
    async fn test_scheduler_reset() {
        let scheduler = TestScheduler::new();
        scheduler.schedule_once(Duration::from_millis(100), || {}).await;
        scheduler.advance(Duration::from_millis(50)).await;
        scheduler.reset().await;

        assert_eq!(scheduler.now(), Duration::ZERO);
        assert_eq!(scheduler.pending_count().await, 0);
        assert_eq!(scheduler.executed_count(), 0);
    }

    // ---- TimedAssertion Tests ----

    #[test]
    fn test_timed_assertion_record_and_assert() {
        let clock = VirtualClock::new();
        let mut ta = TimedAssertion::new(clock.clone());

        clock.advance(Duration::from_millis(100));
        ta.record("event-a");

        clock.advance(Duration::from_millis(200));
        ta.record("event-b");

        ta.assert_event_at("event-a", Duration::from_millis(100));
        ta.assert_event_at("event-b", Duration::from_millis(300));
        assert_eq!(ta.count(), 2);
    }

    #[test]
    fn test_timed_assertion_near() {
        let clock = VirtualClock::new();
        let mut ta = TimedAssertion::new(clock.clone());

        clock.advance(Duration::from_millis(105));
        ta.record("fuzzy");

        ta.assert_event_near("fuzzy", Duration::from_millis(100), Duration::from_millis(10));
    }

    #[test]
    fn test_timed_assertion_order() {
        let clock = VirtualClock::new();
        let mut ta = TimedAssertion::new(clock.clone());

        clock.advance(Duration::from_millis(10));
        ta.record("first");
        clock.advance(Duration::from_millis(20));
        ta.record("second");
        clock.advance(Duration::from_millis(30));
        ta.record("third");

        ta.assert_order(&["first", "second", "third"]);
    }

    #[test]
    #[should_panic(expected = "Event 'missing' not found")]
    fn test_timed_assertion_missing_event() {
        let clock = VirtualClock::new();
        let ta = TimedAssertion::new(clock);
        ta.assert_event_at("missing", Duration::ZERO);
    }
}

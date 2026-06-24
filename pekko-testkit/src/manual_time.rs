// manual_time.rs - System-level manual time control for deterministic testing
// Provides a controllable clock that can be used across the entire test system

use std::collections::BinaryHeap;
use std::cmp::Ordering;
use std::fmt;
use std::time::Duration;

// ============================================================================
// ManualClock
// ============================================================================

/// A manually controllable clock for deterministic time-based testing
/// Unlike VirtualClock in scheduler.rs, this focuses on system-wide time control
#[derive(Debug)]
pub struct ManualClock {
    /// Current time in nanoseconds from epoch
    current_nanos: u64,
    /// Scheduled timers
    timers: BinaryHeap<ScheduledTimer>,
    /// Timer ID counter
    next_timer_id: u64,
    /// Fired timers history
    fired_history: Vec<FiredTimer>,
    /// Whether to auto-fire timers when advancing
    auto_fire: bool,
    stats: ManualClockStats,
}

#[derive(Debug, Clone, Default)]
pub struct ManualClockStats {
    pub advances: u64,
    pub timers_scheduled: u64,
    pub timers_fired: u64,
    pub timers_cancelled: u64,
    pub total_advanced_nanos: u64,
}

impl ManualClock {
    pub fn new() -> Self {
        Self {
            current_nanos: 0,
            timers: BinaryHeap::new(),
            next_timer_id: 1,
            fired_history: Vec::new(),
            auto_fire: true,
            stats: ManualClockStats::default(),
        }
    }

    /// Create with a specific start time
    pub fn with_start_time(nanos: u64) -> Self {
        Self {
            current_nanos: nanos,
            ..Self::new()
        }
    }

    /// Get current time as Duration
    pub fn now(&self) -> Duration {
        Duration::from_nanos(self.current_nanos)
    }

    /// Get current time in nanoseconds
    pub fn now_nanos(&self) -> u64 {
        self.current_nanos
    }

    /// Get current time in milliseconds
    pub fn now_millis(&self) -> u64 {
        self.current_nanos / 1_000_000
    }

    /// Advance time by a duration, firing any due timers
    pub fn advance(&mut self, duration: Duration) -> Vec<FiredTimer> {
        let target = self.current_nanos + duration.as_nanos() as u64;
        self.advance_to_nanos(target)
    }

    /// Advance time to a specific nanosecond timestamp
    pub fn advance_to_nanos(&mut self, target_nanos: u64) -> Vec<FiredTimer> {
        if target_nanos <= self.current_nanos {
            return Vec::new();
        }

        self.stats.advances += 1;
        self.stats.total_advanced_nanos += target_nanos - self.current_nanos;

        let mut fired = Vec::new();

        if self.auto_fire {
            // Fire all timers between current and target
            while let Some(timer) = self.timers.peek() {
                if timer.fire_at_nanos > target_nanos {
                    break;
                }
                let timer = self.timers.pop().unwrap();
                self.current_nanos = timer.fire_at_nanos;
                let fired_timer = FiredTimer {
                    timer_id: timer.id,
                    label: timer.label.clone(),
                    scheduled_at_nanos: timer.scheduled_at_nanos,
                    fired_at_nanos: timer.fire_at_nanos,
                };
                self.stats.timers_fired += 1;
                self.fired_history.push(fired_timer.clone());
                fired.push(fired_timer);

                // Handle repeating timers
                if let Some(interval) = timer.repeat_interval {
                    let next_fire = timer.fire_at_nanos + interval.as_nanos() as u64;
                    if next_fire <= target_nanos || !self.auto_fire {
                        let next_id = self.next_timer_id;
                        self.next_timer_id += 1;
                        self.timers.push(ScheduledTimer {
                            id: next_id,
                            label: timer.label,
                            fire_at_nanos: next_fire,
                            scheduled_at_nanos: self.current_nanos,
                            repeat_interval: Some(interval),
                            cancelled: false,
                        });
                    }
                }
            }
        }

        self.current_nanos = target_nanos;
        fired
    }

    /// Advance to the next scheduled timer and fire it
    pub fn advance_to_next(&mut self) -> Option<FiredTimer> {
        let next_time = self.timers.peek().map(|t| t.fire_at_nanos)?;
        let mut fired = self.advance_to_nanos(next_time);
        fired.pop()
    }

    // ========================================================================
    // Timer Management
    // ========================================================================

    /// Schedule a one-shot timer
    pub fn schedule_once(&mut self, delay: Duration, label: &str) -> u64 {
        let id = self.next_timer_id;
        self.next_timer_id += 1;
        let fire_at = self.current_nanos + delay.as_nanos() as u64;
        self.timers.push(ScheduledTimer {
            id,
            label: label.to_string(),
            fire_at_nanos: fire_at,
            scheduled_at_nanos: self.current_nanos,
            repeat_interval: None,
            cancelled: false,
        });
        self.stats.timers_scheduled += 1;
        id
    }

    /// Schedule a repeating timer
    pub fn schedule_repeated(&mut self, initial_delay: Duration, interval: Duration, label: &str) -> u64 {
        let id = self.next_timer_id;
        self.next_timer_id += 1;
        let fire_at = self.current_nanos + initial_delay.as_nanos() as u64;
        self.timers.push(ScheduledTimer {
            id,
            label: label.to_string(),
            fire_at_nanos: fire_at,
            scheduled_at_nanos: self.current_nanos,
            repeat_interval: Some(interval),
            cancelled: false,
        });
        self.stats.timers_scheduled += 1;
        id
    }

    /// Cancel a timer by ID
    pub fn cancel(&mut self, timer_id: u64) -> bool {
        let mut found = false;
        let timers: Vec<_> = self.timers.drain().collect();
        for timer in timers {
            if timer.id == timer_id {
                found = true;
                self.stats.timers_cancelled += 1;
                // Don't push back - effectively removing it
            } else {
                self.timers.push(timer);
            }
        }
        found
    }

    /// Get number of pending timers
    pub fn pending_timers(&self) -> usize {
        self.timers.len()
    }

    /// Check if a timer with the given label is pending
    pub fn has_pending(&self, label: &str) -> bool {
        self.timers.iter().any(|t| t.label == label)
    }

    /// Get next fire time
    pub fn next_fire_time(&self) -> Option<Duration> {
        self.timers.peek().map(|t| Duration::from_nanos(t.fire_at_nanos))
    }

    /// Get time until next timer fires
    pub fn time_to_next(&self) -> Option<Duration> {
        self.timers.peek().map(|t| {
            let diff = t.fire_at_nanos.saturating_sub(self.current_nanos);
            Duration::from_nanos(diff)
        })
    }

    // ========================================================================
    // History & Assertions
    // ========================================================================

    /// Get fired timer history
    pub fn fired_history(&self) -> &[FiredTimer] {
        &self.fired_history
    }

    /// Assert that a timer with the given label has fired
    pub fn assert_fired(&self, label: &str) -> bool {
        self.fired_history.iter().any(|f| f.label == label)
    }

    /// Assert that a timer fired at a specific time
    pub fn assert_fired_at(&self, label: &str, expected_nanos: u64) -> bool {
        self.fired_history
            .iter()
            .any(|f| f.label == label && f.fired_at_nanos == expected_nanos)
    }

    /// Count how many times a label has fired
    pub fn fire_count(&self, label: &str) -> usize {
        self.fired_history.iter().filter(|f| f.label == label).count()
    }

    /// Get all labels of pending timers
    pub fn pending_labels(&self) -> Vec<String> {
        self.timers.iter().map(|t| t.label.clone()).collect()
    }

    /// Set auto-fire behavior
    pub fn set_auto_fire(&mut self, auto: bool) {
        self.auto_fire = auto;
    }

    /// Get stats
    pub fn stats(&self) -> &ManualClockStats {
        &self.stats
    }

    /// Clear history
    pub fn clear_history(&mut self) {
        self.fired_history.clear();
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// ScheduledTimer (internal)
// ============================================================================

#[derive(Debug, Clone)]
struct ScheduledTimer {
    id: u64,
    label: String,
    fire_at_nanos: u64,
    scheduled_at_nanos: u64,
    repeat_interval: Option<Duration>,
    cancelled: bool,
}

// Min-heap ordering: earliest fire time first
impl PartialEq for ScheduledTimer {
    fn eq(&self, other: &Self) -> bool {
        self.fire_at_nanos == other.fire_at_nanos
    }
}

impl Eq for ScheduledTimer {}

impl PartialOrd for ScheduledTimer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledTimer {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse for min-heap (BinaryHeap is max-heap)
        other.fire_at_nanos.cmp(&self.fire_at_nanos)
    }
}

// ============================================================================
// FiredTimer
// ============================================================================

/// Record of a timer that has fired
#[derive(Debug, Clone)]
pub struct FiredTimer {
    pub timer_id: u64,
    pub label: String,
    pub scheduled_at_nanos: u64,
    pub fired_at_nanos: u64,
}

impl FiredTimer {
    pub fn latency(&self) -> Duration {
        Duration::from_nanos(self.fired_at_nanos - self.scheduled_at_nanos)
    }
}

impl fmt::Display for FiredTimer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Timer '{}' fired at {}ns (scheduled at {}ns)",
            self.label, self.fired_at_nanos, self.scheduled_at_nanos
        )
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_time() {
        let clock = ManualClock::new();
        assert_eq!(clock.now(), Duration::ZERO);
        assert_eq!(clock.now_nanos(), 0);
        assert_eq!(clock.now_millis(), 0);
    }

    #[test]
    fn test_with_start_time() {
        let clock = ManualClock::with_start_time(1_000_000_000);
        assert_eq!(clock.now(), Duration::from_secs(1));
    }

    #[test]
    fn test_advance() {
        let mut clock = ManualClock::new();
        clock.advance(Duration::from_secs(5));
        assert_eq!(clock.now(), Duration::from_secs(5));
    }

    #[test]
    fn test_advance_multiple() {
        let mut clock = ManualClock::new();
        clock.advance(Duration::from_secs(3));
        clock.advance(Duration::from_secs(2));
        assert_eq!(clock.now(), Duration::from_secs(5));
    }

    #[test]
    fn test_advance_backward_noop() {
        let mut clock = ManualClock::new();
        clock.advance(Duration::from_secs(5));
        let fired = clock.advance_to_nanos(3_000_000_000);
        assert!(fired.is_empty());
        assert_eq!(clock.now(), Duration::from_secs(5));
    }

    #[test]
    fn test_schedule_and_fire() {
        let mut clock = ManualClock::new();
        clock.schedule_once(Duration::from_secs(10), "timeout");
        assert_eq!(clock.pending_timers(), 1);
        let fired = clock.advance(Duration::from_secs(10));
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].label, "timeout");
        assert_eq!(clock.pending_timers(), 0);
    }

    #[test]
    fn test_schedule_not_yet_fired() {
        let mut clock = ManualClock::new();
        clock.schedule_once(Duration::from_secs(10), "timeout");
        let fired = clock.advance(Duration::from_secs(5));
        assert!(fired.is_empty());
        assert_eq!(clock.pending_timers(), 1);
    }

    #[test]
    fn test_multiple_timers_ordering() {
        let mut clock = ManualClock::new();
        clock.schedule_once(Duration::from_secs(5), "first");
        clock.schedule_once(Duration::from_secs(10), "second");
        clock.schedule_once(Duration::from_secs(3), "zero");

        let fired = clock.advance(Duration::from_secs(10));
        assert_eq!(fired.len(), 3);
        assert_eq!(fired[0].label, "zero");
        assert_eq!(fired[1].label, "first");
        assert_eq!(fired[2].label, "second");
    }

    #[test]
    fn test_advance_to_next() {
        let mut clock = ManualClock::new();
        clock.schedule_once(Duration::from_secs(5), "timer1");
        clock.schedule_once(Duration::from_secs(10), "timer2");

        let fired = clock.advance_to_next().unwrap();
        assert_eq!(fired.label, "timer1");
        assert_eq!(clock.now(), Duration::from_secs(5));
    }

    #[test]
    fn test_advance_to_next_empty() {
        let mut clock = ManualClock::new();
        assert!(clock.advance_to_next().is_none());
    }

    #[test]
    fn test_cancel_timer() {
        let mut clock = ManualClock::new();
        let id = clock.schedule_once(Duration::from_secs(10), "cancel-me");
        assert!(clock.cancel(id));
        assert_eq!(clock.pending_timers(), 0);
        let fired = clock.advance(Duration::from_secs(15));
        assert!(fired.is_empty());
    }

    #[test]
    fn test_cancel_nonexistent() {
        let mut clock = ManualClock::new();
        assert!(!clock.cancel(999));
    }

    #[test]
    fn test_has_pending() {
        let mut clock = ManualClock::new();
        clock.schedule_once(Duration::from_secs(10), "check");
        assert!(clock.has_pending("check"));
        assert!(!clock.has_pending("other"));
    }

    #[test]
    fn test_next_fire_time() {
        let mut clock = ManualClock::new();
        assert!(clock.next_fire_time().is_none());
        clock.schedule_once(Duration::from_secs(5), "t");
        assert_eq!(clock.next_fire_time(), Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_time_to_next() {
        let mut clock = ManualClock::new();
        clock.schedule_once(Duration::from_secs(10), "t");
        clock.advance(Duration::from_secs(3));
        assert_eq!(clock.time_to_next(), Some(Duration::from_secs(7)));
    }

    #[test]
    fn test_assert_fired() {
        let mut clock = ManualClock::new();
        clock.schedule_once(Duration::from_secs(5), "done");
        clock.advance(Duration::from_secs(5));
        assert!(clock.assert_fired("done"));
        assert!(!clock.assert_fired("not-done"));
    }

    #[test]
    fn test_assert_fired_at() {
        let mut clock = ManualClock::new();
        clock.schedule_once(Duration::from_secs(5), "exact");
        clock.advance(Duration::from_secs(10));
        assert!(clock.assert_fired_at("exact", 5_000_000_000));
    }

    #[test]
    fn test_fire_count() {
        let mut clock = ManualClock::new();
        clock.schedule_once(Duration::from_millis(100), "a");
        clock.schedule_once(Duration::from_millis(200), "a");
        clock.schedule_once(Duration::from_millis(300), "b");
        clock.advance(Duration::from_secs(1));
        assert_eq!(clock.fire_count("a"), 2);
        assert_eq!(clock.fire_count("b"), 1);
    }

    #[test]
    fn test_pending_labels() {
        let mut clock = ManualClock::new();
        clock.schedule_once(Duration::from_secs(1), "a");
        clock.schedule_once(Duration::from_secs(2), "b");
        let labels = clock.pending_labels();
        assert_eq!(labels.len(), 2);
    }

    #[test]
    fn test_fired_timer_latency() {
        let mut clock = ManualClock::new();
        clock.schedule_once(Duration::from_secs(5), "t");
        clock.advance(Duration::from_secs(5));
        let history = clock.fired_history();
        assert_eq!(history[0].latency(), Duration::from_secs(5));
    }

    #[test]
    fn test_fired_timer_display() {
        let fired = FiredTimer {
            timer_id: 1,
            label: "test".to_string(),
            scheduled_at_nanos: 0,
            fired_at_nanos: 5_000_000_000,
        };
        let display = format!("{}", fired);
        assert!(display.contains("test"));
        assert!(display.contains("5000000000"));
    }

    #[test]
    fn test_stats() {
        let mut clock = ManualClock::new();
        clock.schedule_once(Duration::from_secs(5), "t1");
        clock.schedule_once(Duration::from_secs(10), "t2");
        clock.advance(Duration::from_secs(5));
        let id = clock.schedule_once(Duration::from_secs(1), "t3");
        clock.cancel(id);
        assert_eq!(clock.stats().timers_scheduled, 3);
        assert_eq!(clock.stats().timers_fired, 1);
        assert_eq!(clock.stats().timers_cancelled, 1);
        assert_eq!(clock.stats().advances, 1);
    }

    #[test]
    fn test_clear_history() {
        let mut clock = ManualClock::new();
        clock.schedule_once(Duration::from_secs(1), "t");
        clock.advance(Duration::from_secs(1));
        assert!(!clock.fired_history().is_empty());
        clock.clear_history();
        assert!(clock.fired_history().is_empty());
    }

    #[test]
    fn test_debug() {
        let clock = ManualClock::new();
        let debug = format!("{:?}", clock);
        assert!(debug.contains("ManualClock"));
    }

    #[test]
    fn test_default() {
        let clock = ManualClock::default();
        assert_eq!(clock.now_nanos(), 0);
    }
}

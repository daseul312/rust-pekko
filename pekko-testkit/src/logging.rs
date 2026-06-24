//! LoggingTestKit - Assert on log output during tests
//!
//! Similar to Scala Pekko's `LoggingTestKit`, this captures log events
//! and provides assertions on expected log messages, levels, and patterns.
//!
//! Uses `tracing` subscriber to capture structured log events.

use std::fmt;
use std::sync::{Arc, Mutex};

/// Log level for matching
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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

/// A captured log event
#[derive(Debug, Clone)]
pub struct LogEvent {
    pub level: LogLevel,
    pub message: String,
    pub target: String,
}

/// Shared log collector
#[derive(Debug, Clone, Default)]
pub struct LogCollector {
    events: Arc<Mutex<Vec<LogEvent>>>,
}

impl LogCollector {
    /// Create a new empty log collector
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Record a log event
    pub fn record(&self, event: LogEvent) {
        self.events.lock().unwrap().push(event);
    }

    /// Get all collected events
    pub fn events(&self) -> Vec<LogEvent> {
        self.events.lock().unwrap().clone()
    }

    /// Get count of collected events
    pub fn count(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    /// Clear all collected events
    pub fn clear(&self) {
        self.events.lock().unwrap().clear();
    }

    /// Get events matching a level
    pub fn events_at_level(&self, level: LogLevel) -> Vec<LogEvent> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.level == level)
            .cloned()
            .collect()
    }

    /// Get events containing a substring in message
    pub fn events_containing(&self, substring: &str) -> Vec<LogEvent> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.message.contains(substring))
            .cloned()
            .collect()
    }
}

/// Builder for log assertions.
///
/// Use `LoggingTestKit` to set up expectations about log output,
/// then run code and verify the expectations.
///
/// ## Example
/// ```ignore
/// let collector = LogCollector::new();
///
/// // ... run code that produces log events ...
/// collector.record(LogEvent {
///     level: LogLevel::Info,
///     message: "actor started".to_string(),
///     target: "my_actor".to_string(),
/// });
///
/// LoggingTestKit::new()
///     .expect_log_containing("actor started")
///     .expect_log_at_level(LogLevel::Info)
///     .assert(&collector);
/// ```
pub struct LoggingTestKit {
    expectations: Vec<LogExpectation>,
}

enum LogExpectation {
    /// Expect at least one log containing substring
    MessageContaining(String),
    /// Expect at least one log at given level
    AtLevel(LogLevel),
    /// Expect at least one log at level with message substring
    AtLevelContaining(LogLevel, String),
    /// Expect at least N total log events
    MinCount(usize),
    /// Expect exactly N log events matching a level
    ExactCountAtLevel(usize, LogLevel),
    /// Expect no logs at given level
    NoneAtLevel(LogLevel),
    /// Expect at least one log from target
    FromTarget(String),
    /// Custom predicate
    Custom(Box<dyn Fn(&[LogEvent]) -> Result<(), String> + Send>),
}

impl LoggingTestKit {
    /// Create a new LoggingTestKit with no expectations
    pub fn new() -> Self {
        Self {
            expectations: Vec::new(),
        }
    }

    /// Expect at least one log message containing a substring
    pub fn expect_log_containing(mut self, substring: &str) -> Self {
        self.expectations
            .push(LogExpectation::MessageContaining(substring.to_string()));
        self
    }

    /// Expect at least one log at a specific level
    pub fn expect_log_at_level(mut self, level: LogLevel) -> Self {
        self.expectations.push(LogExpectation::AtLevel(level));
        self
    }

    /// Expect at least one log at level with message containing substring
    pub fn expect_log_at_level_containing(mut self, level: LogLevel, substring: &str) -> Self {
        self.expectations.push(LogExpectation::AtLevelContaining(
            level,
            substring.to_string(),
        ));
        self
    }

    /// Expect at least N total log events
    pub fn expect_min_count(mut self, n: usize) -> Self {
        self.expectations.push(LogExpectation::MinCount(n));
        self
    }

    /// Expect exactly N log events at a specific level
    pub fn expect_exact_count_at_level(mut self, count: usize, level: LogLevel) -> Self {
        self.expectations
            .push(LogExpectation::ExactCountAtLevel(count, level));
        self
    }

    /// Expect no logs at a specific level
    pub fn expect_none_at_level(mut self, level: LogLevel) -> Self {
        self.expectations.push(LogExpectation::NoneAtLevel(level));
        self
    }

    /// Expect at least one log from a specific target
    pub fn expect_from_target(mut self, target: &str) -> Self {
        self.expectations
            .push(LogExpectation::FromTarget(target.to_string()));
        self
    }

    /// Add a custom assertion on collected events
    pub fn expect_custom<F>(mut self, f: F) -> Self
    where
        F: Fn(&[LogEvent]) -> Result<(), String> + Send + 'static,
    {
        self.expectations.push(LogExpectation::Custom(Box::new(f)));
        self
    }

    /// Assert all expectations against collected logs (panics on failure)
    pub fn assert(&self, collector: &LogCollector) {
        let events = collector.events();

        for expectation in &self.expectations {
            match expectation {
                LogExpectation::MessageContaining(substring) => {
                    assert!(
                        events.iter().any(|e| e.message.contains(substring.as_str())),
                        "LoggingTestKit: expected log containing '{}', got: {:?}",
                        substring,
                        events.iter().map(|e| &e.message).collect::<Vec<_>>()
                    );
                }
                LogExpectation::AtLevel(level) => {
                    assert!(
                        events.iter().any(|e| e.level == *level),
                        "LoggingTestKit: expected log at level {}, got levels: {:?}",
                        level,
                        events.iter().map(|e| e.level).collect::<Vec<_>>()
                    );
                }
                LogExpectation::AtLevelContaining(level, substring) => {
                    assert!(
                        events
                            .iter()
                            .any(|e| e.level == *level && e.message.contains(substring.as_str())),
                        "LoggingTestKit: expected {} log containing '{}', got: {:?}",
                        level,
                        substring,
                        events
                            .iter()
                            .filter(|e| e.level == *level)
                            .map(|e| &e.message)
                            .collect::<Vec<_>>()
                    );
                }
                LogExpectation::MinCount(n) => {
                    assert!(
                        events.len() >= *n,
                        "LoggingTestKit: expected at least {} events, got {}",
                        n,
                        events.len()
                    );
                }
                LogExpectation::ExactCountAtLevel(count, level) => {
                    let actual = events.iter().filter(|e| e.level == *level).count();
                    assert_eq!(
                        actual, *count,
                        "LoggingTestKit: expected {} events at level {}, got {}",
                        count, level, actual
                    );
                }
                LogExpectation::NoneAtLevel(level) => {
                    let found: Vec<_> = events
                        .iter()
                        .filter(|e| e.level == *level)
                        .map(|e| &e.message)
                        .collect();
                    assert!(
                        found.is_empty(),
                        "LoggingTestKit: expected no logs at level {}, but found: {:?}",
                        level,
                        found
                    );
                }
                LogExpectation::FromTarget(target) => {
                    assert!(
                        events.iter().any(|e| e.target.contains(target.as_str())),
                        "LoggingTestKit: expected log from target '{}', got targets: {:?}",
                        target,
                        events.iter().map(|e| &e.target).collect::<Vec<_>>()
                    );
                }
                LogExpectation::Custom(f) => {
                    if let Err(msg) = f(&events) {
                        panic!("LoggingTestKit: custom assertion failed: {}", msg);
                    }
                }
            }
        }
    }

    /// Try to assert, returning Result instead of panicking
    pub fn try_assert(&self, collector: &LogCollector) -> Result<(), String> {
        let events = collector.events();

        for expectation in &self.expectations {
            match expectation {
                LogExpectation::MessageContaining(substring) => {
                    if !events.iter().any(|e| e.message.contains(substring.as_str())) {
                        return Err(format!(
                            "expected log containing '{}'",
                            substring
                        ));
                    }
                }
                LogExpectation::AtLevel(level) => {
                    if !events.iter().any(|e| e.level == *level) {
                        return Err(format!("expected log at level {}", level));
                    }
                }
                LogExpectation::AtLevelContaining(level, substring) => {
                    if !events
                        .iter()
                        .any(|e| e.level == *level && e.message.contains(substring.as_str()))
                    {
                        return Err(format!(
                            "expected {} log containing '{}'",
                            level, substring
                        ));
                    }
                }
                LogExpectation::MinCount(n) => {
                    if events.len() < *n {
                        return Err(format!(
                            "expected at least {} events, got {}",
                            n,
                            events.len()
                        ));
                    }
                }
                LogExpectation::ExactCountAtLevel(count, level) => {
                    let actual = events.iter().filter(|e| e.level == *level).count();
                    if actual != *count {
                        return Err(format!(
                            "expected {} events at level {}, got {}",
                            count, level, actual
                        ));
                    }
                }
                LogExpectation::NoneAtLevel(level) => {
                    if events.iter().any(|e| e.level == *level) {
                        return Err(format!("expected no logs at level {}", level));
                    }
                }
                LogExpectation::FromTarget(target) => {
                    if !events.iter().any(|e| e.target.contains(target.as_str())) {
                        return Err(format!("expected log from target '{}'", target));
                    }
                }
                LogExpectation::Custom(f) => {
                    f(&events)?;
                }
            }
        }
        Ok(())
    }
}

impl Default for LoggingTestKit {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_events() -> LogCollector {
        let collector = LogCollector::new();
        collector.record(LogEvent {
            level: LogLevel::Info,
            message: "actor started successfully".to_string(),
            target: "pekko::actor".to_string(),
        });
        collector.record(LogEvent {
            level: LogLevel::Debug,
            message: "processing message #42".to_string(),
            target: "pekko::actor".to_string(),
        });
        collector.record(LogEvent {
            level: LogLevel::Warn,
            message: "mailbox near capacity".to_string(),
            target: "pekko::mailbox".to_string(),
        });
        collector.record(LogEvent {
            level: LogLevel::Error,
            message: "actor crashed: division by zero".to_string(),
            target: "pekko::supervision".to_string(),
        });
        collector.record(LogEvent {
            level: LogLevel::Info,
            message: "actor restarted".to_string(),
            target: "pekko::supervision".to_string(),
        });
        collector
    }

    #[test]
    fn test_expect_log_containing() {
        let collector = sample_events();
        LoggingTestKit::new()
            .expect_log_containing("actor started")
            .assert(&collector);
    }

    #[test]
    #[should_panic(expected = "expected log containing")]
    fn test_expect_log_containing_fails() {
        let collector = sample_events();
        LoggingTestKit::new()
            .expect_log_containing("nonexistent message")
            .assert(&collector);
    }

    #[test]
    fn test_expect_log_at_level() {
        let collector = sample_events();
        LoggingTestKit::new()
            .expect_log_at_level(LogLevel::Warn)
            .assert(&collector);
    }

    #[test]
    fn test_expect_at_level_containing() {
        let collector = sample_events();
        LoggingTestKit::new()
            .expect_log_at_level_containing(LogLevel::Error, "division by zero")
            .assert(&collector);
    }

    #[test]
    #[should_panic(expected = "expected ERROR log")]
    fn test_expect_at_level_containing_wrong_level() {
        let collector = sample_events();
        LoggingTestKit::new()
            .expect_log_at_level_containing(LogLevel::Error, "actor started")
            .assert(&collector);
    }

    #[test]
    fn test_expect_min_count() {
        let collector = sample_events();
        LoggingTestKit::new()
            .expect_min_count(3)
            .assert(&collector);
    }

    #[test]
    #[should_panic(expected = "expected at least")]
    fn test_expect_min_count_fails() {
        let collector = sample_events();
        LoggingTestKit::new()
            .expect_min_count(100)
            .assert(&collector);
    }

    #[test]
    fn test_expect_exact_count_at_level() {
        let collector = sample_events();
        LoggingTestKit::new()
            .expect_exact_count_at_level(2, LogLevel::Info)
            .assert(&collector);
    }

    #[test]
    fn test_expect_none_at_level() {
        let collector = sample_events();
        LoggingTestKit::new()
            .expect_none_at_level(LogLevel::Trace)
            .assert(&collector);
    }

    #[test]
    #[should_panic(expected = "expected no logs at level")]
    fn test_expect_none_at_level_fails() {
        let collector = sample_events();
        LoggingTestKit::new()
            .expect_none_at_level(LogLevel::Error)
            .assert(&collector);
    }

    #[test]
    fn test_expect_from_target() {
        let collector = sample_events();
        LoggingTestKit::new()
            .expect_from_target("pekko::supervision")
            .assert(&collector);
    }

    #[test]
    fn test_expect_custom() {
        let collector = sample_events();
        LoggingTestKit::new()
            .expect_custom(|events| {
                let error_count = events.iter().filter(|e| e.level == LogLevel::Error).count();
                if error_count == 1 {
                    Ok(())
                } else {
                    Err(format!("expected 1 error, got {}", error_count))
                }
            })
            .assert(&collector);
    }

    #[test]
    fn test_chained_expectations() {
        let collector = sample_events();
        LoggingTestKit::new()
            .expect_log_at_level(LogLevel::Info)
            .expect_log_at_level(LogLevel::Error)
            .expect_log_containing("restarted")
            .expect_none_at_level(LogLevel::Trace)
            .expect_min_count(4)
            .assert(&collector);
    }

    #[test]
    fn test_try_assert_ok() {
        let collector = sample_events();
        let result = LoggingTestKit::new()
            .expect_log_containing("actor started")
            .try_assert(&collector);
        assert!(result.is_ok());
    }

    #[test]
    fn test_try_assert_err() {
        let collector = sample_events();
        let result = LoggingTestKit::new()
            .expect_log_containing("nonexistent")
            .try_assert(&collector);
        assert!(result.is_err());
    }

    #[test]
    fn test_collector_methods() {
        let collector = sample_events();

        assert_eq!(collector.count(), 5);

        let warns = collector.events_at_level(LogLevel::Warn);
        assert_eq!(warns.len(), 1);
        assert!(warns[0].message.contains("mailbox"));

        let actor_logs = collector.events_containing("actor");
        assert_eq!(actor_logs.len(), 3); // started, crashed, restarted

        collector.clear();
        assert_eq!(collector.count(), 0);
    }
}

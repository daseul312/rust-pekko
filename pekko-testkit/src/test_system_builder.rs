//! # TestActorSystem Builder - Fluent Configuration for Test Systems
//!
//! Extends the basic TestActorSystem with a builder pattern for comprehensive
//! test system configuration, including config overrides, logging, supervision,
//! and RAII cleanup.
//!
//! Inspired by Akka/Pekko's ActorTestKit.create() pattern.

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration key-value store for test systems
#[derive(Debug, Clone)]
pub struct TestConfig {
    values: HashMap<String, ConfigValue>,
}

/// Configuration value type
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Duration(Duration),
    List(Vec<ConfigValue>),
}

impl ConfigValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            ConfigValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ConfigValue::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ConfigValue::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ConfigValue::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_duration(&self) -> Option<Duration> {
        match self {
            ConfigValue::Duration(d) => Some(*d),
            _ => None,
        }
    }
}

impl fmt::Display for ConfigValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigValue::String(s) => write!(f, "\"{}\"", s),
            ConfigValue::Int(i) => write!(f, "{}", i),
            ConfigValue::Float(v) => write!(f, "{}", v),
            ConfigValue::Bool(b) => write!(f, "{}", b),
            ConfigValue::Duration(d) => write!(f, "{:?}", d),
            ConfigValue::List(l) => write!(f, "{:?}", l),
        }
    }
}

impl TestConfig {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    /// Set a string config value
    pub fn set_string(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.values.insert(key.into(), ConfigValue::String(value.into()));
    }

    /// Set an integer config value
    pub fn set_int(&mut self, key: impl Into<String>, value: i64) {
        self.values.insert(key.into(), ConfigValue::Int(value));
    }

    /// Set a float config value
    pub fn set_float(&mut self, key: impl Into<String>, value: f64) {
        self.values.insert(key.into(), ConfigValue::Float(value));
    }

    /// Set a boolean config value
    pub fn set_bool(&mut self, key: impl Into<String>, value: bool) {
        self.values.insert(key.into(), ConfigValue::Bool(value));
    }

    /// Set a duration config value
    pub fn set_duration(&mut self, key: impl Into<String>, value: Duration) {
        self.values.insert(key.into(), ConfigValue::Duration(value));
    }

    /// Get a config value by key
    pub fn get(&self, key: &str) -> Option<&ConfigValue> {
        self.values.get(key)
    }

    /// Get a string value
    pub fn get_string(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(ConfigValue::as_str)
    }

    /// Get an integer value
    pub fn get_int(&self, key: &str) -> Option<i64> {
        self.get(key).and_then(ConfigValue::as_i64)
    }

    /// Get a boolean value
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.get(key).and_then(ConfigValue::as_bool)
    }

    /// Get a duration value
    pub fn get_duration(&self, key: &str) -> Option<Duration> {
        self.get(key).and_then(ConfigValue::as_duration)
    }

    /// Check if a key exists
    pub fn contains(&self, key: &str) -> bool {
        self.values.contains_key(key)
    }

    /// Get number of config entries
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Merge another config into this one (other takes precedence)
    pub fn merge(&mut self, other: &TestConfig) {
        for (k, v) in &other.values {
            self.values.insert(k.clone(), v.clone());
        }
    }

    /// Get all keys
    pub fn keys(&self) -> Vec<&str> {
        self.values.keys().map(|k| k.as_str()).collect()
    }
}

impl Default for TestConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Supervision Strategy
// ============================================================================

/// Supervision strategy for test system actors
#[derive(Debug, Clone, PartialEq)]
pub enum TestSupervisionStrategy {
    /// Resume processing (ignore the failure)
    Resume,
    /// Restart the actor
    Restart,
    /// Stop the actor
    Stop,
    /// Escalate to parent
    Escalate,
}

impl fmt::Display for TestSupervisionStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resume => write!(f, "Resume"),
            Self::Restart => write!(f, "Restart"),
            Self::Stop => write!(f, "Stop"),
            Self::Escalate => write!(f, "Escalate"),
        }
    }
}

// ============================================================================
// Log Level
// ============================================================================

/// Logging level for test system
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TestLogLevel {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl fmt::Display for TestLogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Off => write!(f, "OFF"),
            Self::Error => write!(f, "ERROR"),
            Self::Warn => write!(f, "WARN"),
            Self::Info => write!(f, "INFO"),
            Self::Debug => write!(f, "DEBUG"),
            Self::Trace => write!(f, "TRACE"),
        }
    }
}

// ============================================================================
// TestActorSystemBuilder
// ============================================================================

/// Fluent builder for creating configured test actor systems
pub struct TestActorSystemBuilder {
    name: String,
    config: TestConfig,
    log_level: TestLogLevel,
    supervision_strategy: TestSupervisionStrategy,
    shutdown_timeout: Duration,
    default_message_timeout: Duration,
    tags: Vec<String>,
}

impl TestActorSystemBuilder {
    /// Create a new builder with a system name
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            config: TestConfig::new(),
            log_level: TestLogLevel::Warn,
            supervision_strategy: TestSupervisionStrategy::Stop,
            shutdown_timeout: Duration::from_secs(5),
            default_message_timeout: Duration::from_secs(3),
            tags: Vec::new(),
        }
    }

    /// Set a string config value
    pub fn with_config_string(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.set_string(key, value);
        self
    }

    /// Set an integer config value
    pub fn with_config_int(mut self, key: impl Into<String>, value: i64) -> Self {
        self.config.set_int(key, value);
        self
    }

    /// Set a boolean config value
    pub fn with_config_bool(mut self, key: impl Into<String>, value: bool) -> Self {
        self.config.set_bool(key, value);
        self
    }

    /// Set a duration config value
    pub fn with_config_duration(mut self, key: impl Into<String>, value: Duration) -> Self {
        self.config.set_duration(key, value);
        self
    }

    /// Merge an existing config
    pub fn with_config(mut self, config: TestConfig) -> Self {
        self.config.merge(&config);
        self
    }

    /// Set the log level
    pub fn with_log_level(mut self, level: TestLogLevel) -> Self {
        self.log_level = level;
        self
    }

    /// Enable debug logging
    pub fn with_debug_logging(self) -> Self {
        self.with_log_level(TestLogLevel::Debug)
    }

    /// Enable trace logging
    pub fn with_trace_logging(self) -> Self {
        self.with_log_level(TestLogLevel::Trace)
    }

    /// Disable logging
    pub fn with_no_logging(self) -> Self {
        self.with_log_level(TestLogLevel::Off)
    }

    /// Set supervision strategy
    pub fn with_supervision(mut self, strategy: TestSupervisionStrategy) -> Self {
        self.supervision_strategy = strategy;
        self
    }

    /// Set shutdown timeout
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Set default message timeout
    pub fn with_message_timeout(mut self, timeout: Duration) -> Self {
        self.default_message_timeout = timeout;
        self
    }

    /// Add a tag for categorization
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Build the test system
    pub fn build(self) -> ManagedTestSystem {
        ManagedTestSystem {
            name: self.name,
            config: self.config,
            log_level: self.log_level,
            supervision_strategy: self.supervision_strategy,
            shutdown_timeout: self.shutdown_timeout,
            default_message_timeout: self.default_message_timeout,
            tags: self.tags,
            started_at: Instant::now(),
            is_shutdown: false,
            actors_created: 0,
            messages_processed: 0,
        }
    }
}

// ============================================================================
// ManagedTestSystem — the built test system with lifecycle
// ============================================================================

/// A fully configured test system with RAII cleanup.
///
/// Drops automatically when going out of scope, performing shutdown.
pub struct ManagedTestSystem {
    name: String,
    config: TestConfig,
    log_level: TestLogLevel,
    supervision_strategy: TestSupervisionStrategy,
    shutdown_timeout: Duration,
    default_message_timeout: Duration,
    tags: Vec<String>,
    started_at: Instant,
    is_shutdown: bool,
    actors_created: u64,
    messages_processed: u64,
}

impl ManagedTestSystem {
    /// Get system name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get config reference
    pub fn config(&self) -> &TestConfig {
        &self.config
    }

    /// Get log level
    pub fn log_level(&self) -> TestLogLevel {
        self.log_level
    }

    /// Get supervision strategy
    pub fn supervision_strategy(&self) -> &TestSupervisionStrategy {
        &self.supervision_strategy
    }

    /// Get shutdown timeout
    pub fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }

    /// Get default message timeout
    pub fn default_message_timeout(&self) -> Duration {
        self.default_message_timeout
    }

    /// Get tags
    pub fn tags(&self) -> &[String] {
        &self.tags
    }

    /// Get uptime
    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Check if system has been shut down
    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    /// Record a simulated actor creation
    pub fn record_actor_created(&mut self) {
        self.actors_created += 1;
    }

    /// Record a simulated message processing
    pub fn record_message(&mut self) {
        self.messages_processed += 1;
    }

    /// Get actors created count
    pub fn actors_created(&self) -> u64 {
        self.actors_created
    }

    /// Get messages processed count
    pub fn messages_processed(&self) -> u64 {
        self.messages_processed
    }

    /// Manually shutdown the system
    pub fn shutdown(&mut self) {
        if !self.is_shutdown {
            self.is_shutdown = true;
        }
    }

    /// Get system stats summary
    pub fn stats_summary(&self) -> SystemStats {
        SystemStats {
            name: self.name.clone(),
            uptime: self.uptime(),
            actors_created: self.actors_created,
            messages_processed: self.messages_processed,
            is_shutdown: self.is_shutdown,
            log_level: self.log_level,
            config_entries: self.config.len(),
        }
    }
}

/// System statistics snapshot
#[derive(Debug, Clone)]
pub struct SystemStats {
    pub name: String,
    pub uptime: Duration,
    pub actors_created: u64,
    pub messages_processed: u64,
    pub is_shutdown: bool,
    pub log_level: TestLogLevel,
    pub config_entries: usize,
}

impl fmt::Display for SystemStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TestSystem[{}] uptime={:?} actors={} msgs={} shutdown={}",
            self.name, self.uptime, self.actors_created, self.messages_processed, self.is_shutdown
        )
    }
}

/// RAII: Automatically shutdown when dropped
impl Drop for ManagedTestSystem {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // --- TestConfig ---

    #[test]
    fn test_config_empty() {
        let config = TestConfig::new();
        assert!(config.is_empty());
        assert_eq!(config.len(), 0);
    }

    #[test]
    fn test_config_set_get_string() {
        let mut config = TestConfig::new();
        config.set_string("name", "test-system");
        assert_eq!(config.get_string("name"), Some("test-system"));
        assert!(config.contains("name"));
    }

    #[test]
    fn test_config_set_get_int() {
        let mut config = TestConfig::new();
        config.set_int("max-actors", 100);
        assert_eq!(config.get_int("max-actors"), Some(100));
    }

    #[test]
    fn test_config_set_get_bool() {
        let mut config = TestConfig::new();
        config.set_bool("debug", true);
        assert_eq!(config.get_bool("debug"), Some(true));
    }

    #[test]
    fn test_config_set_get_duration() {
        let mut config = TestConfig::new();
        config.set_duration("timeout", Duration::from_secs(30));
        assert_eq!(config.get_duration("timeout"), Some(Duration::from_secs(30)));
    }

    #[test]
    fn test_config_missing_key() {
        let config = TestConfig::new();
        assert!(config.get("missing").is_none());
        assert!(config.get_string("missing").is_none());
        assert!(config.get_int("missing").is_none());
    }

    #[test]
    fn test_config_type_mismatch() {
        let mut config = TestConfig::new();
        config.set_string("name", "hello");
        assert!(config.get_int("name").is_none());
        assert!(config.get_bool("name").is_none());
    }

    #[test]
    fn test_config_merge() {
        let mut base = TestConfig::new();
        base.set_string("a", "1");
        base.set_string("b", "2");

        let mut overlay = TestConfig::new();
        overlay.set_string("b", "override");
        overlay.set_string("c", "3");

        base.merge(&overlay);
        assert_eq!(base.get_string("a"), Some("1"));
        assert_eq!(base.get_string("b"), Some("override")); // overridden
        assert_eq!(base.get_string("c"), Some("3")); // new
        assert_eq!(base.len(), 3);
    }

    #[test]
    fn test_config_keys() {
        let mut config = TestConfig::new();
        config.set_string("alpha", "a");
        config.set_int("beta", 2);
        let mut keys = config.keys();
        keys.sort();
        assert_eq!(keys, vec!["alpha", "beta"]);
    }

    #[test]
    fn test_config_value_display() {
        assert!(ConfigValue::String("hello".into()).to_string().contains("hello"));
        assert_eq!(ConfigValue::Int(42).to_string(), "42");
        assert_eq!(ConfigValue::Bool(true).to_string(), "true");
    }

    // --- TestSupervisionStrategy ---

    #[test]
    fn test_supervision_display() {
        assert_eq!(TestSupervisionStrategy::Resume.to_string(), "Resume");
        assert_eq!(TestSupervisionStrategy::Restart.to_string(), "Restart");
        assert_eq!(TestSupervisionStrategy::Stop.to_string(), "Stop");
        assert_eq!(TestSupervisionStrategy::Escalate.to_string(), "Escalate");
    }

    // --- TestLogLevel ---

    #[test]
    fn test_log_level_display() {
        assert_eq!(TestLogLevel::Off.to_string(), "OFF");
        assert_eq!(TestLogLevel::Debug.to_string(), "DEBUG");
        assert_eq!(TestLogLevel::Trace.to_string(), "TRACE");
    }

    #[test]
    fn test_log_level_ordering() {
        assert!(TestLogLevel::Error < TestLogLevel::Warn);
        assert!(TestLogLevel::Warn < TestLogLevel::Info);
        assert!(TestLogLevel::Info < TestLogLevel::Debug);
        assert!(TestLogLevel::Debug < TestLogLevel::Trace);
    }

    // --- Builder ---

    #[test]
    fn test_builder_defaults() {
        let system = TestActorSystemBuilder::new("test").build();
        assert_eq!(system.name(), "test");
        assert_eq!(system.log_level(), TestLogLevel::Warn);
        assert_eq!(*system.supervision_strategy(), TestSupervisionStrategy::Stop);
        assert_eq!(system.shutdown_timeout(), Duration::from_secs(5));
        assert_eq!(system.default_message_timeout(), Duration::from_secs(3));
        assert!(system.tags().is_empty());
        assert!(!system.is_shutdown());
    }

    #[test]
    fn test_builder_with_config() {
        let system = TestActorSystemBuilder::new("test")
            .with_config_string("remote.host", "localhost")
            .with_config_int("remote.port", 2551)
            .with_config_bool("remote.ssl", true)
            .build();
        assert_eq!(system.config().get_string("remote.host"), Some("localhost"));
        assert_eq!(system.config().get_int("remote.port"), Some(2551));
        assert_eq!(system.config().get_bool("remote.ssl"), Some(true));
    }

    #[test]
    fn test_builder_with_log_level() {
        let system = TestActorSystemBuilder::new("test")
            .with_debug_logging()
            .build();
        assert_eq!(system.log_level(), TestLogLevel::Debug);

        let system2 = TestActorSystemBuilder::new("test2")
            .with_trace_logging()
            .build();
        assert_eq!(system2.log_level(), TestLogLevel::Trace);

        let system3 = TestActorSystemBuilder::new("test3")
            .with_no_logging()
            .build();
        assert_eq!(system3.log_level(), TestLogLevel::Off);
    }

    #[test]
    fn test_builder_with_supervision() {
        let system = TestActorSystemBuilder::new("test")
            .with_supervision(TestSupervisionStrategy::Restart)
            .build();
        assert_eq!(*system.supervision_strategy(), TestSupervisionStrategy::Restart);
    }

    #[test]
    fn test_builder_with_timeouts() {
        let system = TestActorSystemBuilder::new("test")
            .with_shutdown_timeout(Duration::from_secs(10))
            .with_message_timeout(Duration::from_secs(5))
            .build();
        assert_eq!(system.shutdown_timeout(), Duration::from_secs(10));
        assert_eq!(system.default_message_timeout(), Duration::from_secs(5));
    }

    #[test]
    fn test_builder_with_tags() {
        let system = TestActorSystemBuilder::new("test")
            .with_tag("integration")
            .with_tag("slow")
            .build();
        assert_eq!(system.tags(), &["integration", "slow"]);
    }

    #[test]
    fn test_builder_with_merged_config() {
        let mut base = TestConfig::new();
        base.set_string("env", "test");
        base.set_int("workers", 4);

        let system = TestActorSystemBuilder::new("test")
            .with_config(base)
            .with_config_int("workers", 8) // override
            .build();
        assert_eq!(system.config().get_string("env"), Some("test"));
        assert_eq!(system.config().get_int("workers"), Some(8));
    }

    // --- ManagedTestSystem ---

    #[test]
    fn test_system_actor_tracking() {
        let mut system = TestActorSystemBuilder::new("test").build();
        assert_eq!(system.actors_created(), 0);
        system.record_actor_created();
        system.record_actor_created();
        assert_eq!(system.actors_created(), 2);
    }

    #[test]
    fn test_system_message_tracking() {
        let mut system = TestActorSystemBuilder::new("test").build();
        system.record_message();
        system.record_message();
        system.record_message();
        assert_eq!(system.messages_processed(), 3);
    }

    #[test]
    fn test_system_shutdown() {
        let mut system = TestActorSystemBuilder::new("test").build();
        assert!(!system.is_shutdown());
        system.shutdown();
        assert!(system.is_shutdown());
        // Idempotent
        system.shutdown();
        assert!(system.is_shutdown());
    }

    #[test]
    fn test_system_drop_shutdown() {
        let system = TestActorSystemBuilder::new("test").build();
        assert!(!system.is_shutdown());
        // Drop will call shutdown
        drop(system);
        // Can't check after drop, but at least it doesn't panic
    }

    #[test]
    fn test_system_stats_summary() {
        let mut system = TestActorSystemBuilder::new("my-test").build();
        system.record_actor_created();
        system.record_message();
        system.record_message();

        let stats = system.stats_summary();
        assert_eq!(stats.name, "my-test");
        assert_eq!(stats.actors_created, 1);
        assert_eq!(stats.messages_processed, 2);
        assert!(!stats.is_shutdown);
        assert!(stats.to_string().contains("my-test"));
    }

    #[test]
    fn test_system_uptime() {
        let system = TestActorSystemBuilder::new("test").build();
        std::thread::sleep(Duration::from_millis(10));
        assert!(system.uptime() >= Duration::from_millis(10));
    }

    #[test]
    fn test_complex_builder_scenario() {
        let mut config = TestConfig::new();
        config.set_string("akka.remote.transport", "tcp");
        config.set_int("akka.remote.port", 2551);
        config.set_duration("akka.remote.timeout", Duration::from_secs(10));

        let system = TestActorSystemBuilder::new("cluster-test")
            .with_config(config)
            .with_config_bool("akka.cluster.auto-join", false)
            .with_debug_logging()
            .with_supervision(TestSupervisionStrategy::Restart)
            .with_shutdown_timeout(Duration::from_secs(30))
            .with_tag("cluster")
            .with_tag("integration")
            .build();

        assert_eq!(system.name(), "cluster-test");
        assert_eq!(system.config().get_string("akka.remote.transport"), Some("tcp"));
        assert_eq!(system.config().get_int("akka.remote.port"), Some(2551));
        assert_eq!(system.config().get_bool("akka.cluster.auto-join"), Some(false));
        assert_eq!(system.log_level(), TestLogLevel::Debug);
        assert_eq!(*system.supervision_strategy(), TestSupervisionStrategy::Restart);
        assert_eq!(system.shutdown_timeout(), Duration::from_secs(30));
        assert_eq!(system.tags().len(), 2);
    }
}

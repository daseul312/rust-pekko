//! # Throttle — Stream Rate Limiting
//!
//! Pekko/Akka-style throttle operator for controlling stream element emission rate.
//!
//! ## Features
//!
//! - Token bucket rate limiting with burst support
//! - Shaping mode (delays elements) vs Enforcing mode (fails on overload)
//! - Cost-based throttling (variable cost per element)
//! - Weighted throttle with dynamic cost functions
//! - Throttle statistics and monitoring

use std::collections::VecDeque;
use std::fmt;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// ThrottleMode
// ---------------------------------------------------------------------------

/// Controls how throttle handles elements that exceed the rate limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThrottleMode {
    /// Shaping: delays elements to maintain the rate (backpressure)
    Shaping,
    /// Enforcing: fails the stream if rate is exceeded
    Enforcing,
}

impl Default for ThrottleMode {
    fn default() -> Self {
        ThrottleMode::Shaping
    }
}

// ---------------------------------------------------------------------------
// ThrottleError
// ---------------------------------------------------------------------------

/// Errors that can occur during throttling.
#[derive(Debug, Clone, PartialEq)]
pub enum ThrottleError {
    /// Rate limit exceeded in Enforcing mode
    RateLimitExceeded {
        /// Current available tokens
        available: f64,
        /// Cost of the element that was rejected
        required: f64,
    },
    /// Throttle has been shut down
    Shutdown,
    /// Invalid configuration
    InvalidConfig(String),
}

impl fmt::Display for ThrottleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ThrottleError::RateLimitExceeded { available, required } => {
                write!(f, "Rate limit exceeded: available={available:.2}, required={required:.2}")
            }
            ThrottleError::Shutdown => write!(f, "Throttle has been shut down"),
            ThrottleError::InvalidConfig(msg) => write!(f, "Invalid config: {msg}"),
        }
    }
}

// ---------------------------------------------------------------------------
// ThrottleResult
// ---------------------------------------------------------------------------

/// Result of attempting to pass an element through the throttle.
#[derive(Debug, Clone, PartialEq)]
pub enum ThrottleResult<T> {
    /// Element passed immediately (tokens were available)
    Emit(T),
    /// Element will be delayed by this duration (Shaping mode)
    Delayed {
        element: T,
        delay: Duration,
    },
    /// Element was rejected (Enforcing mode)
    Rejected(ThrottleError),
}

impl<T> ThrottleResult<T> {
    pub fn is_emit(&self) -> bool {
        matches!(self, ThrottleResult::Emit(_))
    }

    pub fn is_delayed(&self) -> bool {
        matches!(self, ThrottleResult::Delayed { .. })
    }

    pub fn is_rejected(&self) -> bool {
        matches!(self, ThrottleResult::Rejected(_))
    }
}

// ---------------------------------------------------------------------------
// TokenBucket (internal rate limiter)
// ---------------------------------------------------------------------------

/// Token bucket algorithm for rate limiting.
#[derive(Debug, Clone)]
pub struct TokenBucket {
    /// Maximum tokens (burst size)
    capacity: f64,
    /// Current available tokens
    tokens: f64,
    /// Tokens added per second
    rate: f64,
    /// Last time tokens were refilled
    last_refill: Instant,
}

impl TokenBucket {
    pub fn new(rate: f64, burst: f64) -> Self {
        Self {
            capacity: burst,
            tokens: burst,
            rate,
            last_refill: Instant::now(),
        }
    }

    pub fn new_with_time(rate: f64, burst: f64, now: Instant) -> Self {
        Self {
            capacity: burst,
            tokens: burst,
            rate,
            last_refill: now,
        }
    }

    /// Refill tokens based on elapsed time.
    pub fn refill(&mut self, now: Instant) {
        let elapsed = now.duration_since(self.last_refill);
        let new_tokens = elapsed.as_secs_f64() * self.rate;
        self.tokens = (self.tokens + new_tokens).min(self.capacity);
        self.last_refill = now;
    }

    /// Try to consume `cost` tokens. Returns true if successful.
    pub fn try_consume(&mut self, cost: f64, now: Instant) -> bool {
        self.refill(now);
        if self.tokens >= cost {
            self.tokens -= cost;
            true
        } else {
            false
        }
    }

    /// Calculate how long to wait until `cost` tokens are available.
    pub fn wait_time(&self, cost: f64) -> Duration {
        if self.tokens >= cost {
            return Duration::ZERO;
        }
        let deficit = cost - self.tokens;
        if self.rate <= 0.0 {
            return Duration::from_secs(u64::MAX);
        }
        Duration::from_secs_f64(deficit / self.rate)
    }

    pub fn available(&self) -> f64 {
        self.tokens
    }

    pub fn capacity(&self) -> f64 {
        self.capacity
    }

    pub fn rate(&self) -> f64 {
        self.rate
    }
}

// ---------------------------------------------------------------------------
// ThrottleConfig
// ---------------------------------------------------------------------------

/// Configuration for a Throttle stage.
#[derive(Debug, Clone)]
pub struct ThrottleConfig {
    /// Number of elements per time period
    pub elements: u32,
    /// Time period for the element count
    pub per: Duration,
    /// Maximum burst size (defaults to `elements`)
    pub burst: u32,
    /// Throttle mode: Shaping or Enforcing
    pub mode: ThrottleMode,
}

impl ThrottleConfig {
    pub fn new(elements: u32, per: Duration) -> Self {
        Self {
            elements,
            per,
            burst: elements,
            mode: ThrottleMode::Shaping,
        }
    }

    pub fn with_burst(mut self, burst: u32) -> Self {
        self.burst = burst;
        self
    }

    pub fn with_mode(mut self, mode: ThrottleMode) -> Self {
        self.mode = mode;
        self
    }

    /// Calculate the rate in elements per second.
    pub fn rate_per_second(&self) -> f64 {
        if self.per.is_zero() {
            return 0.0;
        }
        self.elements as f64 / self.per.as_secs_f64()
    }

    pub fn validate(&self) -> Result<(), ThrottleError> {
        if self.elements == 0 {
            return Err(ThrottleError::InvalidConfig("elements must be > 0".into()));
        }
        if self.per.is_zero() {
            return Err(ThrottleError::InvalidConfig("period must be > 0".into()));
        }
        if self.burst == 0 {
            return Err(ThrottleError::InvalidConfig("burst must be > 0".into()));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Throttle
// ---------------------------------------------------------------------------

/// Throttle stage that limits the rate of element emission.
///
/// Supports both fixed-cost (1 per element) and variable-cost throttling.
#[derive(Debug)]
pub struct Throttle {
    bucket: TokenBucket,
    config: ThrottleConfig,
    stats: ThrottleStats,
    pending: VecDeque<PendingElement>,
    shutdown: bool,
}

#[derive(Debug, Clone)]
struct PendingElement {
    cost: f64,
    queued_at: Instant,
}

impl Throttle {
    pub fn new(config: ThrottleConfig) -> Result<Self, ThrottleError> {
        config.validate()?;
        let rate = config.rate_per_second();
        let burst = config.burst as f64;
        Ok(Self {
            bucket: TokenBucket::new(rate, burst),
            config,
            stats: ThrottleStats::default(),
            pending: VecDeque::new(),
            shutdown: false,
        })
    }

    /// Create a throttle that allows `elements` per `duration`.
    pub fn elements_per(elements: u32, duration: Duration) -> Result<Self, ThrottleError> {
        Self::new(ThrottleConfig::new(elements, duration))
    }

    /// Try to pass an element with cost 1.
    pub fn try_pass<T>(&mut self, element: T) -> ThrottleResult<T> {
        self.try_pass_with_cost(element, 1.0)
    }

    /// Try to pass an element with a custom cost.
    pub fn try_pass_with_cost<T>(&mut self, element: T, cost: f64) -> ThrottleResult<T> {
        if self.shutdown {
            return ThrottleResult::Rejected(ThrottleError::Shutdown);
        }

        let now = Instant::now();

        if self.bucket.try_consume(cost, now) {
            self.stats.emitted += 1;
            self.stats.total_cost += cost;
            ThrottleResult::Emit(element)
        } else {
            match self.config.mode {
                ThrottleMode::Shaping => {
                    let delay = self.bucket.wait_time(cost);
                    self.stats.delayed += 1;
                    self.stats.total_delay += delay;
                    self.pending.push_back(PendingElement {
                        cost,
                        queued_at: now,
                    });
                    ThrottleResult::Delayed { element, delay }
                }
                ThrottleMode::Enforcing => {
                    self.stats.rejected += 1;
                    ThrottleResult::Rejected(ThrottleError::RateLimitExceeded {
                        available: self.bucket.available(),
                        required: cost,
                    })
                }
            }
        }
    }

    /// Process pending elements after their delay has elapsed.
    pub fn process_pending(&mut self) -> usize {
        let now = Instant::now();
        self.bucket.refill(now);
        let mut processed = 0;

        while let Some(pending) = self.pending.front() {
            if self.bucket.try_consume(pending.cost, now) {
                self.pending.pop_front();
                self.stats.emitted += 1;
                processed += 1;
            } else {
                break;
            }
        }

        processed
    }

    /// Get the number of pending elements.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Shut down the throttle, rejecting all future elements.
    pub fn shutdown(&mut self) {
        self.shutdown = true;
        self.pending.clear();
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    pub fn stats(&self) -> &ThrottleStats {
        &self.stats
    }

    pub fn config(&self) -> &ThrottleConfig {
        &self.config
    }

    /// Get the current rate (elements/sec) available.
    pub fn current_rate(&self) -> f64 {
        self.config.rate_per_second()
    }

    pub fn available_tokens(&self) -> f64 {
        self.bucket.available()
    }

    /// Reset stats and refill bucket.
    pub fn reset(&mut self) {
        self.stats = ThrottleStats::default();
        self.bucket = TokenBucket::new(self.config.rate_per_second(), self.config.burst as f64);
        self.pending.clear();
        self.shutdown = false;
    }
}

// ---------------------------------------------------------------------------
// CostFunction
// ---------------------------------------------------------------------------

/// Defines how to calculate the cost of an element for throttling.
pub trait CostFunction<T> {
    fn cost(&self, element: &T) -> f64;
}

/// Fixed cost of 1 per element.
#[derive(Debug, Clone)]
pub struct UnitCost;

impl<T> CostFunction<T> for UnitCost {
    fn cost(&self, _: &T) -> f64 {
        1.0
    }
}

/// Cost based on element size (e.g., byte length).
#[derive(Debug, Clone)]
pub struct SizeCost;

impl CostFunction<Vec<u8>> for SizeCost {
    fn cost(&self, element: &Vec<u8>) -> f64 {
        element.len() as f64
    }
}

impl CostFunction<String> for SizeCost {
    fn cost(&self, element: &String) -> f64 {
        element.len() as f64
    }
}

/// Custom cost function using a closure.
pub struct CustomCost<F> {
    func: F,
}

impl<F> CustomCost<F> {
    pub fn new(func: F) -> Self {
        Self { func }
    }
}

impl<T, F: Fn(&T) -> f64> CostFunction<T> for CustomCost<F> {
    fn cost(&self, element: &T) -> f64 {
        (self.func)(element)
    }
}

// ---------------------------------------------------------------------------
// CostThrottle
// ---------------------------------------------------------------------------

/// Throttle with automatic cost calculation per element.
pub struct CostThrottle<T, C: CostFunction<T>> {
    throttle: Throttle,
    cost_fn: C,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, C: CostFunction<T>> CostThrottle<T, C> {
    pub fn new(config: ThrottleConfig, cost_fn: C) -> Result<Self, ThrottleError> {
        Ok(Self {
            throttle: Throttle::new(config)?,
            cost_fn,
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn try_pass(&mut self, element: T) -> ThrottleResult<T> {
        let cost = self.cost_fn.cost(&element);
        self.throttle.try_pass_with_cost(element, cost)
    }

    pub fn stats(&self) -> &ThrottleStats {
        self.throttle.stats()
    }

    pub fn reset(&mut self) {
        self.throttle.reset();
    }
}

// ---------------------------------------------------------------------------
// ThrottleStats
// ---------------------------------------------------------------------------

/// Statistics about throttle operation.
#[derive(Debug, Clone, Default)]
pub struct ThrottleStats {
    /// Number of elements emitted immediately
    pub emitted: u64,
    /// Number of elements delayed (Shaping mode)
    pub delayed: u64,
    /// Number of elements rejected (Enforcing mode)
    pub rejected: u64,
    /// Total cost consumed
    pub total_cost: f64,
    /// Total delay accumulated
    pub total_delay: Duration,
}

impl ThrottleStats {
    pub fn total_processed(&self) -> u64 {
        self.emitted + self.delayed + self.rejected
    }

    pub fn delay_ratio(&self) -> f64 {
        let total = self.total_processed();
        if total == 0 {
            return 0.0;
        }
        self.delayed as f64 / total as f64
    }

    pub fn reject_ratio(&self) -> f64 {
        let total = self.total_processed();
        if total == 0 {
            return 0.0;
        }
        self.rejected as f64 / total as f64
    }

    pub fn average_delay(&self) -> Duration {
        if self.delayed == 0 {
            return Duration::ZERO;
        }
        self.total_delay / self.delayed as u32
    }
}

// ---------------------------------------------------------------------------
// ThrottleBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for Throttle configuration.
pub struct ThrottleBuilder {
    elements: u32,
    per: Duration,
    burst: Option<u32>,
    mode: ThrottleMode,
}

impl ThrottleBuilder {
    pub fn new(elements: u32, per: Duration) -> Self {
        Self {
            elements,
            per,
            burst: None,
            mode: ThrottleMode::Shaping,
        }
    }

    /// Set maximum burst size.
    pub fn burst(mut self, burst: u32) -> Self {
        self.burst = Some(burst);
        self
    }

    /// Set to Shaping mode (delay elements).
    pub fn shaping(mut self) -> Self {
        self.mode = ThrottleMode::Shaping;
        self
    }

    /// Set to Enforcing mode (reject on overload).
    pub fn enforcing(mut self) -> Self {
        self.mode = ThrottleMode::Enforcing;
        self
    }

    pub fn build(self) -> Result<Throttle, ThrottleError> {
        let config = ThrottleConfig {
            elements: self.elements,
            per: self.per,
            burst: self.burst.unwrap_or(self.elements),
            mode: self.mode,
        };
        Throttle::new(config)
    }
}

// ---------------------------------------------------------------------------
// AdaptiveThrottle
// ---------------------------------------------------------------------------

/// Adaptive throttle that adjusts rate based on downstream feedback.
#[derive(Debug)]
pub struct AdaptiveThrottle {
    throttle: Throttle,
    min_rate: f64,
    max_rate: f64,
    /// Current rate factor (1.0 = normal)
    rate_factor: f64,
    /// Adjustment step
    step: f64,
    /// Window for tracking success/failure
    window: SlidingWindow,
}

#[derive(Debug)]
struct SlidingWindow {
    successes: VecDeque<Instant>,
    failures: VecDeque<Instant>,
    window_size: Duration,
}

impl SlidingWindow {
    fn new(window_size: Duration) -> Self {
        Self {
            successes: VecDeque::new(),
            failures: VecDeque::new(),
            window_size,
        }
    }

    fn record_success(&mut self, now: Instant) {
        self.successes.push_back(now);
        self.cleanup(now);
    }

    fn record_failure(&mut self, now: Instant) {
        self.failures.push_back(now);
        self.cleanup(now);
    }

    fn cleanup(&mut self, now: Instant) {
        let cutoff = now.checked_sub(self.window_size).unwrap_or(now);
        while self.successes.front().map_or(false, |&t| t < cutoff) {
            self.successes.pop_front();
        }
        while self.failures.front().map_or(false, |&t| t < cutoff) {
            self.failures.pop_front();
        }
    }

    fn failure_rate(&self) -> f64 {
        let total = self.successes.len() + self.failures.len();
        if total == 0 {
            return 0.0;
        }
        self.failures.len() as f64 / total as f64
    }

    fn total_count(&self) -> usize {
        self.successes.len() + self.failures.len()
    }
}

impl AdaptiveThrottle {
    pub fn new(
        base_config: ThrottleConfig,
        min_rate: f64,
        max_rate: f64,
    ) -> Result<Self, ThrottleError> {
        let _base_rate = base_config.rate_per_second();
        Ok(Self {
            throttle: Throttle::new(base_config)?,
            min_rate: min_rate.max(0.1),
            max_rate: max_rate.max(min_rate),
            rate_factor: 1.0,
            step: 0.1,
            window: SlidingWindow::new(Duration::from_secs(10)),
        })
    }

    /// Record downstream success (speed up).
    pub fn record_success(&mut self) {
        let now = Instant::now();
        self.window.record_success(now);
        self.maybe_adjust();
    }

    /// Record downstream failure/backpressure (slow down).
    pub fn record_failure(&mut self) {
        let now = Instant::now();
        self.window.record_failure(now);
        self.maybe_adjust();
    }

    fn maybe_adjust(&mut self) {
        if self.window.total_count() < 5 {
            return;
        }

        let failure_rate = self.window.failure_rate();
        let base_rate = self.throttle.config.rate_per_second();

        if failure_rate > 0.5 {
            // High failure: decrease rate significantly
            self.rate_factor = (self.rate_factor - self.step * 2.0).max(self.min_rate / base_rate);
        } else if failure_rate > 0.2 {
            // Moderate failure: decrease rate slightly
            self.rate_factor = (self.rate_factor - self.step).max(self.min_rate / base_rate);
        } else if failure_rate < 0.05 {
            // Very low failure: increase rate
            self.rate_factor = (self.rate_factor + self.step).min(self.max_rate / base_rate);
        }
    }

    pub fn try_pass<T>(&mut self, element: T) -> ThrottleResult<T> {
        self.throttle.try_pass_with_cost(element, 1.0 / self.rate_factor)
    }

    pub fn current_effective_rate(&self) -> f64 {
        self.throttle.config.rate_per_second() * self.rate_factor
    }

    pub fn rate_factor(&self) -> f64 {
        self.rate_factor
    }

    pub fn stats(&self) -> &ThrottleStats {
        self.throttle.stats()
    }

    pub fn failure_rate(&self) -> f64 {
        self.window.failure_rate()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // === ThrottleConfig tests ===

    #[test]
    fn test_config_rate_per_second() {
        let config = ThrottleConfig::new(100, Duration::from_secs(1));
        assert!((config.rate_per_second() - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_config_rate_per_second_fractional() {
        let config = ThrottleConfig::new(10, Duration::from_secs(2));
        assert!((config.rate_per_second() - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_config_validation_ok() {
        let config = ThrottleConfig::new(10, Duration::from_secs(1));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_zero_elements() {
        let config = ThrottleConfig::new(0, Duration::from_secs(1));
        assert!(matches!(config.validate(), Err(ThrottleError::InvalidConfig(_))));
    }

    #[test]
    fn test_config_validation_zero_period() {
        let config = ThrottleConfig::new(10, Duration::ZERO);
        assert!(matches!(config.validate(), Err(ThrottleError::InvalidConfig(_))));
    }

    #[test]
    fn test_config_with_burst() {
        let config = ThrottleConfig::new(10, Duration::from_secs(1)).with_burst(50);
        assert_eq!(config.burst, 50);
    }

    #[test]
    fn test_config_with_mode() {
        let config = ThrottleConfig::new(10, Duration::from_secs(1)).with_mode(ThrottleMode::Enforcing);
        assert_eq!(config.mode, ThrottleMode::Enforcing);
    }

    // === TokenBucket tests ===

    #[test]
    fn test_token_bucket_initial_tokens() {
        let now = Instant::now();
        let bucket = TokenBucket::new_with_time(10.0, 10.0, now);
        assert!((bucket.available() - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_token_bucket_consume() {
        let now = Instant::now();
        let mut bucket = TokenBucket::new_with_time(10.0, 10.0, now);
        assert!(bucket.try_consume(5.0, now));
        assert!((bucket.available() - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_token_bucket_consume_exact() {
        let now = Instant::now();
        let mut bucket = TokenBucket::new_with_time(10.0, 10.0, now);
        assert!(bucket.try_consume(10.0, now));
        assert!(bucket.available() < f64::EPSILON);
    }

    #[test]
    fn test_token_bucket_consume_exceeds() {
        let now = Instant::now();
        let mut bucket = TokenBucket::new_with_time(10.0, 10.0, now);
        assert!(!bucket.try_consume(11.0, now));
        assert!((bucket.available() - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_token_bucket_wait_time() {
        let now = Instant::now();
        let mut bucket = TokenBucket::new_with_time(10.0, 10.0, now);
        bucket.try_consume(10.0, now);
        let wait = bucket.wait_time(5.0);
        // Need 5 tokens at 10/sec = 0.5 seconds
        assert!((wait.as_secs_f64() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_token_bucket_wait_time_zero_when_available() {
        let now = Instant::now();
        let bucket = TokenBucket::new_with_time(10.0, 10.0, now);
        assert_eq!(bucket.wait_time(5.0), Duration::ZERO);
    }

    #[test]
    fn test_token_bucket_capacity() {
        let bucket = TokenBucket::new(10.0, 20.0);
        assert!((bucket.capacity() - 20.0).abs() < f64::EPSILON);
        assert!((bucket.rate() - 10.0).abs() < f64::EPSILON);
    }

    // === Throttle tests ===

    #[test]
    fn test_throttle_basic_emit() {
        let mut throttle = Throttle::elements_per(10, Duration::from_secs(1)).unwrap();
        let result = throttle.try_pass("hello");
        assert!(result.is_emit());
        assert_eq!(throttle.stats().emitted, 1);
    }

    #[test]
    fn test_throttle_burst_then_delay() {
        let config = ThrottleConfig::new(2, Duration::from_secs(1));
        let mut throttle = Throttle::new(config).unwrap();

        // First 2 should emit (burst = 2)
        assert!(throttle.try_pass(1).is_emit());
        assert!(throttle.try_pass(2).is_emit());

        // 3rd should be delayed (Shaping mode)
        let result = throttle.try_pass(3);
        assert!(result.is_delayed());
    }

    #[test]
    fn test_throttle_enforcing_reject() {
        let config = ThrottleConfig::new(2, Duration::from_secs(1))
            .with_mode(ThrottleMode::Enforcing);
        let mut throttle = Throttle::new(config).unwrap();

        assert!(throttle.try_pass(1).is_emit());
        assert!(throttle.try_pass(2).is_emit());

        // 3rd should be rejected (Enforcing mode)
        let result = throttle.try_pass(3);
        assert!(result.is_rejected());
        assert_eq!(throttle.stats().rejected, 1);
    }

    #[test]
    fn test_throttle_with_cost() {
        let config = ThrottleConfig::new(10, Duration::from_secs(1));
        let mut throttle = Throttle::new(config).unwrap();

        // Cost 5 should work
        assert!(throttle.try_pass_with_cost("a", 5.0).is_emit());
        // Cost 5 again should work (total 10 = burst)
        assert!(throttle.try_pass_with_cost("b", 5.0).is_emit());
        // Cost 1 should be delayed (exceeds burst)
        assert!(throttle.try_pass_with_cost("c", 1.0).is_delayed());
    }

    #[test]
    fn test_throttle_shutdown() {
        let mut throttle = Throttle::elements_per(10, Duration::from_secs(1)).unwrap();
        throttle.shutdown();
        assert!(throttle.is_shutdown());
        assert!(throttle.try_pass("x").is_rejected());
    }

    #[test]
    fn test_throttle_reset() {
        let mut throttle = Throttle::elements_per(2, Duration::from_secs(1)).unwrap();
        throttle.try_pass(1);
        throttle.try_pass(2);
        assert_eq!(throttle.stats().emitted, 2);

        throttle.reset();
        assert_eq!(throttle.stats().emitted, 0);
        assert!(throttle.try_pass(1).is_emit());
    }

    #[test]
    fn test_throttle_pending_count() {
        let config = ThrottleConfig::new(1, Duration::from_secs(1));
        let mut throttle = Throttle::new(config).unwrap();

        throttle.try_pass(1); // emitted
        throttle.try_pass(2); // delayed
        throttle.try_pass(3); // delayed

        assert_eq!(throttle.pending_count(), 2);
    }

    #[test]
    fn test_throttle_current_rate() {
        let throttle = Throttle::elements_per(100, Duration::from_secs(1)).unwrap();
        assert!((throttle.current_rate() - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_throttle_available_tokens() {
        let mut throttle = Throttle::elements_per(10, Duration::from_secs(1)).unwrap();
        throttle.try_pass(1);
        assert!(throttle.available_tokens() < 10.0);
    }

    // === ThrottleBuilder tests ===

    #[test]
    fn test_builder_basic() {
        let throttle = ThrottleBuilder::new(100, Duration::from_secs(1))
            .build()
            .unwrap();
        assert!((throttle.current_rate() - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_builder_with_burst() {
        let throttle = ThrottleBuilder::new(10, Duration::from_secs(1))
            .burst(50)
            .build()
            .unwrap();
        assert_eq!(throttle.config().burst, 50);
    }

    #[test]
    fn test_builder_shaping() {
        let throttle = ThrottleBuilder::new(10, Duration::from_secs(1))
            .shaping()
            .build()
            .unwrap();
        assert_eq!(throttle.config().mode, ThrottleMode::Shaping);
    }

    #[test]
    fn test_builder_enforcing() {
        let throttle = ThrottleBuilder::new(10, Duration::from_secs(1))
            .enforcing()
            .build()
            .unwrap();
        assert_eq!(throttle.config().mode, ThrottleMode::Enforcing);
    }

    // === CostFunction tests ===

    #[test]
    fn test_unit_cost() {
        let cost_fn = UnitCost;
        assert!((cost_fn.cost(&42) - 1.0).abs() < f64::EPSILON);
        assert!((cost_fn.cost(&"hello") - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_size_cost_vec() {
        let cost_fn = SizeCost;
        let data = vec![1u8, 2, 3, 4, 5];
        assert!((cost_fn.cost(&data) - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_size_cost_string() {
        let cost_fn = SizeCost;
        let data = String::from("hello");
        assert!((cost_fn.cost(&data) - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_custom_cost() {
        let cost_fn = CustomCost::new(|x: &i32| *x as f64 * 2.0);
        assert!((cost_fn.cost(&5) - 10.0).abs() < f64::EPSILON);
    }

    // === CostThrottle tests ===

    #[test]
    fn test_cost_throttle_with_size() {
        let config = ThrottleConfig::new(100, Duration::from_secs(1)).with_burst(100);
        let mut throttle = CostThrottle::new(config, SizeCost).unwrap();

        // 50-byte message, cost=50
        let result = throttle.try_pass(vec![0u8; 50]);
        assert!(result.is_emit());

        // 50 more bytes, still within burst of 100
        let result = throttle.try_pass(vec![0u8; 50]);
        assert!(result.is_emit());

        // 1 more byte should be delayed
        let result = throttle.try_pass(vec![0u8; 1]);
        assert!(result.is_delayed());
    }

    #[test]
    fn test_cost_throttle_stats() {
        let config = ThrottleConfig::new(10, Duration::from_secs(1));
        let mut throttle = CostThrottle::new(config, UnitCost).unwrap();

        for i in 0..10 {
            throttle.try_pass(i);
        }

        assert_eq!(throttle.stats().emitted, 10);
    }

    #[test]
    fn test_cost_throttle_reset() {
        let config = ThrottleConfig::new(5, Duration::from_secs(1));
        let mut throttle = CostThrottle::new(config, UnitCost).unwrap();

        for i in 0..5 {
            throttle.try_pass(i);
        }
        assert_eq!(throttle.stats().emitted, 5);

        throttle.reset();
        assert_eq!(throttle.stats().emitted, 0);
    }

    // === ThrottleStats tests ===

    #[test]
    fn test_stats_total_processed() {
        let stats = ThrottleStats {
            emitted: 10,
            delayed: 5,
            rejected: 2,
            ..Default::default()
        };
        assert_eq!(stats.total_processed(), 17);
    }

    #[test]
    fn test_stats_delay_ratio() {
        let stats = ThrottleStats {
            emitted: 8,
            delayed: 2,
            rejected: 0,
            ..Default::default()
        };
        assert!((stats.delay_ratio() - 0.2).abs() < f64::EPSILON);
    }

    #[test]
    fn test_stats_reject_ratio() {
        let stats = ThrottleStats {
            emitted: 7,
            delayed: 0,
            rejected: 3,
            ..Default::default()
        };
        assert!((stats.reject_ratio() - 0.3).abs() < f64::EPSILON);
    }

    #[test]
    fn test_stats_average_delay() {
        let stats = ThrottleStats {
            delayed: 4,
            total_delay: Duration::from_secs(8),
            ..Default::default()
        };
        assert_eq!(stats.average_delay(), Duration::from_secs(2));
    }

    #[test]
    fn test_stats_zero_processed() {
        let stats = ThrottleStats::default();
        assert_eq!(stats.total_processed(), 0);
        assert!((stats.delay_ratio()).abs() < f64::EPSILON);
        assert!((stats.reject_ratio()).abs() < f64::EPSILON);
        assert_eq!(stats.average_delay(), Duration::ZERO);
    }

    // === ThrottleMode tests ===

    #[test]
    fn test_throttle_mode_default() {
        assert_eq!(ThrottleMode::default(), ThrottleMode::Shaping);
    }

    // === ThrottleError tests ===

    #[test]
    fn test_error_display_rate_exceeded() {
        let err = ThrottleError::RateLimitExceeded { available: 1.5, required: 3.0 };
        assert!(err.to_string().contains("1.50"));
        assert!(err.to_string().contains("3.00"));
    }

    #[test]
    fn test_error_display_shutdown() {
        let err = ThrottleError::Shutdown;
        assert!(err.to_string().contains("shut down"));
    }

    #[test]
    fn test_error_display_invalid_config() {
        let err = ThrottleError::InvalidConfig("bad".into());
        assert!(err.to_string().contains("bad"));
    }

    // === AdaptiveThrottle tests ===

    #[test]
    fn test_adaptive_throttle_basic() {
        let config = ThrottleConfig::new(100, Duration::from_secs(1));
        let mut throttle = AdaptiveThrottle::new(config, 10.0, 200.0).unwrap();
        assert!(throttle.try_pass("x").is_emit());
    }

    #[test]
    fn test_adaptive_throttle_initial_rate_factor() {
        let config = ThrottleConfig::new(100, Duration::from_secs(1));
        let throttle = AdaptiveThrottle::new(config, 10.0, 200.0).unwrap();
        assert!((throttle.rate_factor() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_adaptive_throttle_effective_rate() {
        let config = ThrottleConfig::new(100, Duration::from_secs(1));
        let throttle = AdaptiveThrottle::new(config, 10.0, 200.0).unwrap();
        assert!((throttle.current_effective_rate() - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_adaptive_throttle_failure_slows_down() {
        let config = ThrottleConfig::new(100, Duration::from_secs(1));
        let mut throttle = AdaptiveThrottle::new(config, 10.0, 200.0).unwrap();

        // Record many failures
        for _ in 0..10 {
            throttle.record_failure();
        }

        assert!(throttle.rate_factor() < 1.0);
        assert!(throttle.failure_rate() > 0.5);
    }

    #[test]
    fn test_adaptive_throttle_success_speeds_up() {
        let config = ThrottleConfig::new(100, Duration::from_secs(1));
        let mut throttle = AdaptiveThrottle::new(config, 10.0, 200.0).unwrap();

        // First slow down with failures (need >= 5 to trigger adjustment)
        for _ in 0..10 {
            throttle.record_failure();
        }
        let slow_factor = throttle.rate_factor();
        assert!(slow_factor < 1.0, "Should have slowed down");

        // Now record many successes to dilute failure rate below 5%
        // With 10 failures, need at least 200 successes to get < 5% failure rate
        for _ in 0..250 {
            throttle.record_success();
        }

        // failure_rate should now be very low, triggering speed-up
        assert!(throttle.rate_factor() > slow_factor,
            "rate_factor {} should be > slow_factor {}, failure_rate={}",
            throttle.rate_factor(), slow_factor, throttle.failure_rate());
    }

    #[test]
    fn test_adaptive_throttle_stats() {
        let config = ThrottleConfig::new(100, Duration::from_secs(1));
        let mut throttle = AdaptiveThrottle::new(config, 10.0, 200.0).unwrap();
        throttle.try_pass(1);
        assert!(throttle.stats().emitted > 0 || throttle.stats().delayed > 0);
    }

    // === ThrottleResult tests ===

    #[test]
    fn test_throttle_result_emit() {
        let result: ThrottleResult<i32> = ThrottleResult::Emit(42);
        assert!(result.is_emit());
        assert!(!result.is_delayed());
        assert!(!result.is_rejected());
    }

    #[test]
    fn test_throttle_result_delayed() {
        let result: ThrottleResult<i32> = ThrottleResult::Delayed {
            element: 42,
            delay: Duration::from_millis(100),
        };
        assert!(!result.is_emit());
        assert!(result.is_delayed());
        assert!(!result.is_rejected());
    }

    #[test]
    fn test_throttle_result_rejected() {
        let result: ThrottleResult<i32> = ThrottleResult::Rejected(ThrottleError::Shutdown);
        assert!(!result.is_emit());
        assert!(!result.is_delayed());
        assert!(result.is_rejected());
    }
}

//! # RetryFlow — Stream Retry and Recovery Patterns
//!
//! Pekko/Akka-style retry and recovery operators for stream processing.
//!
//! ## Features
//!
//! - RetryFlow: retry failed operations with configurable backoff
//! - RecoverWith: replace failed elements with fallback values
//! - RestartFlow: restart entire flow stages on failure
//! - RetryBudget: limit total retries to prevent resource exhaustion
//! - Circuit breaker integration for retry decisions

use std::collections::VecDeque;
use std::fmt;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// RetryError
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum RetryError {
    /// All retry attempts exhausted
    RetriesExhausted {
        attempts: u32,
        last_error: String,
    },
    /// Retry budget exhausted
    BudgetExhausted {
        remaining_budget: f64,
    },
    /// Operation timed out
    Timeout {
        elapsed: Duration,
    },
    /// Circuit breaker is open
    CircuitOpen,
    /// Flow was shut down
    Shutdown,
}

impl fmt::Display for RetryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RetryError::RetriesExhausted { attempts, last_error } => {
                write!(f, "Retries exhausted after {attempts} attempts: {last_error}")
            }
            RetryError::BudgetExhausted { remaining_budget } => {
                write!(f, "Retry budget exhausted (remaining: {remaining_budget:.2})")
            }
            RetryError::Timeout { elapsed } => {
                write!(f, "Retry timeout after {elapsed:?}")
            }
            RetryError::CircuitOpen => write!(f, "Circuit breaker is open"),
            RetryError::Shutdown => write!(f, "Flow has been shut down"),
        }
    }
}

// ---------------------------------------------------------------------------
// BackoffStrategy
// ---------------------------------------------------------------------------

/// Strategy for calculating delay between retries.
#[derive(Debug, Clone)]
pub enum BackoffStrategy {
    /// Fixed delay between retries
    Fixed(Duration),
    /// Exponential backoff with base delay and multiplier
    Exponential {
        initial: Duration,
        multiplier: f64,
        max_delay: Duration,
    },
    /// Linear increase in delay
    Linear {
        initial: Duration,
        increment: Duration,
        max_delay: Duration,
    },
    /// Fibonacci-based delay sequence
    Fibonacci {
        initial: Duration,
        max_delay: Duration,
    },
    /// No delay between retries
    Immediate,
}

impl BackoffStrategy {
    /// Calculate delay for a given attempt number (0-based).
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        match self {
            BackoffStrategy::Fixed(d) => *d,
            BackoffStrategy::Exponential { initial, multiplier, max_delay } => {
                let delay_ms = initial.as_millis() as f64 * multiplier.powi(attempt as i32);
                let delay = Duration::from_millis(delay_ms as u64);
                delay.min(*max_delay)
            }
            BackoffStrategy::Linear { initial, increment, max_delay } => {
                let delay = *initial + *increment * attempt;
                delay.min(*max_delay)
            }
            BackoffStrategy::Fibonacci { initial, max_delay } => {
                let fib = fibonacci(attempt);
                let delay = Duration::from_millis(initial.as_millis() as u64 * fib);
                delay.min(*max_delay)
            }
            BackoffStrategy::Immediate => Duration::ZERO,
        }
    }

    /// Create exponential backoff with sensible defaults.
    pub fn exponential(initial: Duration) -> Self {
        BackoffStrategy::Exponential {
            initial,
            multiplier: 2.0,
            max_delay: Duration::from_secs(60),
        }
    }

    /// Create exponential backoff with custom multiplier.
    pub fn exponential_with(initial: Duration, multiplier: f64, max_delay: Duration) -> Self {
        BackoffStrategy::Exponential {
            initial,
            multiplier,
            max_delay,
        }
    }
}

fn fibonacci(n: u32) -> u64 {
    if n == 0 { return 1; }
    if n == 1 { return 1; }
    let mut a: u64 = 1;
    let mut b: u64 = 1;
    for _ in 2..=n {
        let c = a.saturating_add(b);
        a = b;
        b = c;
    }
    b
}

// ---------------------------------------------------------------------------
// RetryPolicy
// ---------------------------------------------------------------------------

/// Policy that determines whether an error should be retried.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Backoff strategy
    pub backoff: BackoffStrategy,
    /// Maximum total time for all attempts
    pub timeout: Option<Duration>,
    /// Whether to retry on timeout errors
    pub retry_on_timeout: bool,
}

impl RetryPolicy {
    pub fn new(max_retries: u32, backoff: BackoffStrategy) -> Self {
        Self {
            max_retries,
            backoff,
            timeout: None,
            retry_on_timeout: false,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn with_retry_on_timeout(mut self, retry: bool) -> Self {
        self.retry_on_timeout = retry;
        self
    }

    /// Simple policy: retry N times with fixed delay.
    pub fn fixed(max_retries: u32, delay: Duration) -> Self {
        Self::new(max_retries, BackoffStrategy::Fixed(delay))
    }

    /// Exponential backoff policy.
    pub fn exponential(max_retries: u32, initial_delay: Duration) -> Self {
        Self::new(max_retries, BackoffStrategy::exponential(initial_delay))
    }

    /// No retries.
    pub fn no_retry() -> Self {
        Self::new(0, BackoffStrategy::Immediate)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::exponential(3, Duration::from_millis(100))
    }
}

// ---------------------------------------------------------------------------
// RetryState
// ---------------------------------------------------------------------------

/// Tracks the state of a retry operation.
#[derive(Debug, Clone)]
pub struct RetryState {
    /// Current attempt number (0 = first attempt)
    pub attempt: u32,
    /// Errors from previous attempts
    pub errors: Vec<String>,
    /// When the first attempt started
    pub started_at: Instant,
    /// Total delay accumulated
    pub total_delay: Duration,
}

impl RetryState {
    pub fn new() -> Self {
        Self {
            attempt: 0,
            errors: Vec::new(),
            started_at: Instant::now(),
            total_delay: Duration::ZERO,
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    pub fn record_error(&mut self, error: String) {
        self.errors.push(error);
        self.attempt += 1;
    }

    pub fn last_error(&self) -> Option<&str> {
        self.errors.last().map(|s| s.as_str())
    }
}

// ---------------------------------------------------------------------------
// RetryDecision
// ---------------------------------------------------------------------------

/// Decision made by retry logic after an error.
#[derive(Debug, Clone, PartialEq)]
pub enum RetryDecision {
    /// Retry after the specified delay
    Retry { delay: Duration },
    /// Give up, no more retries
    GiveUp { reason: String },
}

// ---------------------------------------------------------------------------
// RetryFlow
// ---------------------------------------------------------------------------

/// RetryFlow applies retry logic to a stream processing function.
///
/// When an element fails processing, it retries according to the configured
/// policy with backoff delays.
pub struct RetryFlow<T: Clone> {
    policy: RetryPolicy,
    states: Vec<RetryState>,
    pending: VecDeque<(T, RetryState)>,
    stats: RetryFlowStats,
    shutdown: bool,
}

impl<T: Clone> RetryFlow<T> {
    pub fn new(policy: RetryPolicy) -> Self {
        Self {
            policy,
            states: Vec::new(),
            pending: VecDeque::new(),
            stats: RetryFlowStats::default(),
            shutdown: false,
        }
    }

    /// Submit an element for processing. Returns the retry decision.
    pub fn on_success(&mut self) {
        self.stats.successes += 1;
    }

    /// Handle a first failure for an element (creates new retry state).
    pub fn on_failure(&mut self, element: T, error: &str) -> RetryDecision {
        self.on_failure_with_state(element, error, None)
    }

    /// Handle a repeated failure for an element with existing retry state.
    pub fn on_failure_retry(&mut self, element: T, error: &str, state: RetryState) -> RetryDecision {
        self.on_failure_with_state(element, error, Some(state))
    }

    fn on_failure_with_state(&mut self, element: T, error: &str, existing_state: Option<RetryState>) -> RetryDecision {
        if self.shutdown {
            return RetryDecision::GiveUp {
                reason: "Flow shut down".into(),
            };
        }

        let mut state = existing_state.unwrap_or_else(RetryState::new);
        state.record_error(error.to_string());

        // Check timeout
        if let Some(timeout) = self.policy.timeout {
            if state.elapsed() >= timeout {
                self.stats.exhausted += 1;
                return RetryDecision::GiveUp {
                    reason: format!("Timeout after {:?}", state.elapsed()),
                };
            }
        }

        // Check max retries
        if state.attempt > self.policy.max_retries {
            self.stats.exhausted += 1;
            return RetryDecision::GiveUp {
                reason: format!("Max retries ({}) exhausted", self.policy.max_retries),
            };
        }

        // Calculate delay
        let delay = self.policy.backoff.delay_for_attempt(state.attempt - 1);
        state.total_delay += delay;

        self.stats.retries += 1;
        self.pending.push_back((element, state));

        RetryDecision::Retry { delay }
    }

    /// Get next pending element ready for retry (with its state for re-submission).
    pub fn next_pending(&mut self) -> Option<(T, RetryState)> {
        self.pending.pop_front()
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn shutdown(&mut self) {
        self.shutdown = true;
        self.pending.clear();
    }

    pub fn stats(&self) -> &RetryFlowStats {
        &self.stats
    }
}

// ---------------------------------------------------------------------------
// RecoverWith
// ---------------------------------------------------------------------------

/// Recovery strategy that replaces failed elements with fallback values.
#[derive(Debug, Clone)]
pub enum RecoveryStrategy<T> {
    /// Use a default value
    DefaultValue(T),
    /// Skip the failed element
    Skip,
    /// Use the last successful value (or default if none)
    LastSuccess(Option<T>),
    /// Stop the stream on error
    Stop,
}

/// RecoverWith stage that handles errors in a stream by applying a recovery strategy.
pub struct RecoverWith<T: Clone> {
    strategy: RecoveryStrategy<T>,
    last_success: Option<T>,
    stats: RecoverStats,
}

impl<T: Clone> RecoverWith<T> {
    pub fn new(strategy: RecoveryStrategy<T>) -> Self {
        Self {
            strategy,
            last_success: None,
            stats: RecoverStats::default(),
        }
    }

    pub fn with_default(value: T) -> Self {
        Self::new(RecoveryStrategy::DefaultValue(value))
    }

    pub fn skip() -> Self {
        Self::new(RecoveryStrategy::Skip)
    }

    pub fn last_success(default: T) -> Self {
        Self::new(RecoveryStrategy::LastSuccess(Some(default)))
    }

    pub fn stop() -> Self {
        Self::new(RecoveryStrategy::Stop)
    }

    /// Process a successful element.
    pub fn on_success(&mut self, element: T) -> RecoverResult<T> {
        self.last_success = Some(element.clone());
        self.stats.passed += 1;
        RecoverResult::Emit(element)
    }

    /// Process a failed element.
    pub fn on_error(&mut self, _error: &str) -> RecoverResult<T> {
        self.stats.recovered += 1;
        match &self.strategy {
            RecoveryStrategy::DefaultValue(v) => RecoverResult::Emit(v.clone()),
            RecoveryStrategy::Skip => RecoverResult::Skip,
            RecoveryStrategy::LastSuccess(default) => {
                if let Some(ref last) = self.last_success {
                    RecoverResult::Emit(last.clone())
                } else if let Some(ref d) = default {
                    RecoverResult::Emit(d.clone())
                } else {
                    RecoverResult::Skip
                }
            }
            RecoveryStrategy::Stop => {
                self.stats.stopped = true;
                RecoverResult::Stop
            }
        }
    }

    pub fn stats(&self) -> &RecoverStats {
        &self.stats
    }
}

/// Result of recovery processing.
#[derive(Debug, Clone, PartialEq)]
pub enum RecoverResult<T> {
    /// Emit this value downstream
    Emit(T),
    /// Skip (filter out) the failed element
    Skip,
    /// Stop the stream
    Stop,
}

impl<T> RecoverResult<T> {
    pub fn is_emit(&self) -> bool {
        matches!(self, RecoverResult::Emit(_))
    }
    pub fn is_skip(&self) -> bool {
        matches!(self, RecoverResult::Skip)
    }
    pub fn is_stop(&self) -> bool {
        matches!(self, RecoverResult::Stop)
    }
}

// ---------------------------------------------------------------------------
// RestartSettings
// ---------------------------------------------------------------------------

/// Configuration for restarting a flow stage on failure.
#[derive(Debug, Clone)]
pub struct RestartSettings {
    /// Minimum backoff delay
    pub min_backoff: Duration,
    /// Maximum backoff delay
    pub max_backoff: Duration,
    /// Random factor for jitter (0.0 to 1.0)
    pub random_factor: f64,
    /// Maximum number of restarts (-1 for unlimited)
    pub max_restarts: i32,
    /// Time window for counting restarts
    pub max_restarts_within: Duration,
}

impl RestartSettings {
    pub fn new(min_backoff: Duration, max_backoff: Duration, random_factor: f64) -> Self {
        Self {
            min_backoff,
            max_backoff,
            random_factor: random_factor.clamp(0.0, 1.0),
            max_restarts: -1,
            max_restarts_within: Duration::from_secs(60),
        }
    }

    pub fn with_max_restarts(mut self, max: i32, within: Duration) -> Self {
        self.max_restarts = max;
        self.max_restarts_within = within;
        self
    }

    /// Calculate backoff for a given restart attempt.
    pub fn backoff_for_restart(&self, restart_count: u32) -> Duration {
        let base = self.min_backoff.as_secs_f64() * 2.0_f64.powi(restart_count as i32);
        let capped = base.min(self.max_backoff.as_secs_f64());
        // Apply jitter: delay * (1 + random_factor * [-0.5, 0.5])
        // Without real randomness, use deterministic jitter based on count
        let jitter_offset = ((restart_count as f64 * 0.618033988749895) % 1.0) - 0.5;
        let jittered = capped * (1.0 + self.random_factor * jitter_offset);
        let final_secs = jittered.max(self.min_backoff.as_secs_f64());
        Duration::from_secs_f64(final_secs)
    }
}

impl Default for RestartSettings {
    fn default() -> Self {
        Self::new(
            Duration::from_millis(100),
            Duration::from_secs(30),
            0.2,
        )
    }
}

// ---------------------------------------------------------------------------
// RestartFlow
// ---------------------------------------------------------------------------

/// RestartFlow restarts the entire flow stage on failure.
///
/// Uses exponential backoff with jitter between restarts.
#[derive(Debug)]
pub struct RestartFlow {
    settings: RestartSettings,
    state: RestartState,
    restart_times: VecDeque<Instant>,
    stats: RestartFlowStats,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestartState {
    Running,
    Restarting,
    BackingOff { restart_count: u32 },
    Stopped,
    Failed,
}

impl RestartFlow {
    pub fn new(settings: RestartSettings) -> Self {
        Self {
            settings,
            state: RestartState::Running,
            restart_times: VecDeque::new(),
            stats: RestartFlowStats::default(),
        }
    }

    /// Handle a failure. Returns the backoff duration if restart is allowed.
    pub fn on_failure(&mut self, error: &str) -> Result<Duration, RetryError> {
        if self.state == RestartState::Stopped || self.state == RestartState::Failed {
            return Err(RetryError::Shutdown);
        }

        // Check restart limit
        let now = Instant::now();
        self.cleanup_old_restarts(now);

        if self.settings.max_restarts >= 0 {
            if self.restart_times.len() >= self.settings.max_restarts as usize {
                self.state = RestartState::Failed;
                self.stats.failures += 1;
                return Err(RetryError::RetriesExhausted {
                    attempts: self.restart_times.len() as u32,
                    last_error: error.to_string(),
                });
            }
        }

        let restart_count = self.restart_times.len() as u32;
        let backoff = self.settings.backoff_for_restart(restart_count);

        self.restart_times.push_back(now);
        self.state = RestartState::BackingOff { restart_count };
        self.stats.restarts += 1;
        self.stats.total_backoff += backoff;

        Ok(backoff)
    }

    /// Mark the flow as successfully restarted.
    pub fn on_restart_complete(&mut self) {
        self.state = RestartState::Running;
    }

    /// Stop the restart flow permanently.
    pub fn stop(&mut self) {
        self.state = RestartState::Stopped;
    }

    fn cleanup_old_restarts(&mut self, now: Instant) {
        let window = self.settings.max_restarts_within;
        while let Some(&front) = self.restart_times.front() {
            if now.duration_since(front) > window {
                self.restart_times.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn state(&self) -> &RestartState {
        &self.state
    }

    pub fn restart_count(&self) -> usize {
        self.restart_times.len()
    }

    pub fn stats(&self) -> &RestartFlowStats {
        &self.stats
    }
}

// ---------------------------------------------------------------------------
// RetryBudget
// ---------------------------------------------------------------------------

/// Budget-based retry limiter to prevent retry storms.
///
/// Uses a token-bucket approach: each retry costs tokens, and tokens
/// refill over time. Prevents cascading failures from excessive retries.
#[derive(Debug)]
pub struct RetryBudget {
    /// Maximum budget (tokens)
    max_budget: f64,
    /// Current available budget
    available: f64,
    /// Budget refill rate (tokens per second)
    refill_rate: f64,
    /// Last refill time
    last_refill: Instant,
    /// Cost per retry
    retry_cost: f64,
    /// Deposit per success (refills budget)
    success_deposit: f64,
    stats: BudgetStats,
}

impl RetryBudget {
    pub fn new(max_budget: f64, refill_rate: f64) -> Self {
        Self {
            max_budget,
            available: max_budget,
            refill_rate,
            last_refill: Instant::now(),
            retry_cost: 1.0,
            success_deposit: 0.1,
            stats: BudgetStats::default(),
        }
    }

    pub fn with_costs(mut self, retry_cost: f64, success_deposit: f64) -> Self {
        self.retry_cost = retry_cost;
        self.success_deposit = success_deposit;
        self
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.available = (self.available + elapsed * self.refill_rate).min(self.max_budget);
        self.last_refill = now;
    }

    /// Try to spend retry budget. Returns true if budget available.
    pub fn try_retry(&mut self) -> bool {
        self.refill();
        if self.available >= self.retry_cost {
            self.available -= self.retry_cost;
            self.stats.retries_allowed += 1;
            true
        } else {
            self.stats.retries_denied += 1;
            false
        }
    }

    /// Deposit budget after a successful operation.
    pub fn deposit_success(&mut self) {
        self.refill();
        self.available = (self.available + self.success_deposit).min(self.max_budget);
        self.stats.deposits += 1;
    }

    pub fn available_budget(&self) -> f64 {
        self.available
    }

    pub fn max_budget(&self) -> f64 {
        self.max_budget
    }

    pub fn stats(&self) -> &BudgetStats {
        &self.stats
    }
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct RetryFlowStats {
    pub successes: u64,
    pub retries: u64,
    pub exhausted: u64,
}

impl RetryFlowStats {
    pub fn total_attempts(&self) -> u64 {
        self.successes + self.retries + self.exhausted
    }

    pub fn retry_rate(&self) -> f64 {
        let total = self.total_attempts();
        if total == 0 { return 0.0; }
        self.retries as f64 / total as f64
    }
}

#[derive(Debug, Clone, Default)]
pub struct RecoverStats {
    pub passed: u64,
    pub recovered: u64,
    pub stopped: bool,
}

impl RecoverStats {
    pub fn total(&self) -> u64 {
        self.passed + self.recovered
    }

    pub fn recovery_rate(&self) -> f64 {
        let total = self.total();
        if total == 0 { return 0.0; }
        self.recovered as f64 / total as f64
    }
}

#[derive(Debug, Clone, Default)]
pub struct RestartFlowStats {
    pub restarts: u64,
    pub failures: u64,
    pub total_backoff: Duration,
}

impl RestartFlowStats {
    pub fn average_backoff(&self) -> Duration {
        if self.restarts == 0 {
            return Duration::ZERO;
        }
        self.total_backoff / self.restarts as u32
    }
}

#[derive(Debug, Clone, Default)]
pub struct BudgetStats {
    pub retries_allowed: u64,
    pub retries_denied: u64,
    pub deposits: u64,
}

impl BudgetStats {
    pub fn denial_rate(&self) -> f64 {
        let total = self.retries_allowed + self.retries_denied;
        if total == 0 { return 0.0; }
        self.retries_denied as f64 / total as f64
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // === BackoffStrategy tests ===

    #[test]
    fn test_fixed_backoff() {
        let strategy = BackoffStrategy::Fixed(Duration::from_millis(100));
        assert_eq!(strategy.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(strategy.delay_for_attempt(5), Duration::from_millis(100));
    }

    #[test]
    fn test_exponential_backoff() {
        let strategy = BackoffStrategy::Exponential {
            initial: Duration::from_millis(100),
            multiplier: 2.0,
            max_delay: Duration::from_secs(10),
        };
        assert_eq!(strategy.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(strategy.delay_for_attempt(1), Duration::from_millis(200));
        assert_eq!(strategy.delay_for_attempt(2), Duration::from_millis(400));
        assert_eq!(strategy.delay_for_attempt(3), Duration::from_millis(800));
    }

    #[test]
    fn test_exponential_backoff_max_cap() {
        let strategy = BackoffStrategy::Exponential {
            initial: Duration::from_secs(1),
            multiplier: 2.0,
            max_delay: Duration::from_secs(5),
        };
        // 2^10 = 1024 seconds, should be capped
        assert_eq!(strategy.delay_for_attempt(10), Duration::from_secs(5));
    }

    #[test]
    fn test_linear_backoff() {
        let strategy = BackoffStrategy::Linear {
            initial: Duration::from_millis(100),
            increment: Duration::from_millis(50),
            max_delay: Duration::from_secs(1),
        };
        assert_eq!(strategy.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(strategy.delay_for_attempt(1), Duration::from_millis(150));
        assert_eq!(strategy.delay_for_attempt(2), Duration::from_millis(200));
    }

    #[test]
    fn test_fibonacci_backoff() {
        let strategy = BackoffStrategy::Fibonacci {
            initial: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
        };
        // fib: 1, 1, 2, 3, 5, 8, 13...
        assert_eq!(strategy.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(strategy.delay_for_attempt(1), Duration::from_millis(100));
        assert_eq!(strategy.delay_for_attempt(2), Duration::from_millis(200));
        assert_eq!(strategy.delay_for_attempt(3), Duration::from_millis(300));
        assert_eq!(strategy.delay_for_attempt(4), Duration::from_millis(500));
    }

    #[test]
    fn test_immediate_backoff() {
        let strategy = BackoffStrategy::Immediate;
        assert_eq!(strategy.delay_for_attempt(0), Duration::ZERO);
        assert_eq!(strategy.delay_for_attempt(10), Duration::ZERO);
    }

    #[test]
    fn test_exponential_constructor() {
        let strategy = BackoffStrategy::exponential(Duration::from_millis(200));
        assert_eq!(strategy.delay_for_attempt(0), Duration::from_millis(200));
    }

    // === fibonacci helper ===

    #[test]
    fn test_fibonacci_sequence() {
        assert_eq!(fibonacci(0), 1);
        assert_eq!(fibonacci(1), 1);
        assert_eq!(fibonacci(2), 2);
        assert_eq!(fibonacci(3), 3);
        assert_eq!(fibonacci(4), 5);
        assert_eq!(fibonacci(5), 8);
        assert_eq!(fibonacci(6), 13);
    }

    // === RetryPolicy tests ===

    #[test]
    fn test_retry_policy_fixed() {
        let policy = RetryPolicy::fixed(3, Duration::from_millis(100));
        assert_eq!(policy.max_retries, 3);
    }

    #[test]
    fn test_retry_policy_exponential() {
        let policy = RetryPolicy::exponential(5, Duration::from_millis(50));
        assert_eq!(policy.max_retries, 5);
    }

    #[test]
    fn test_retry_policy_no_retry() {
        let policy = RetryPolicy::no_retry();
        assert_eq!(policy.max_retries, 0);
    }

    #[test]
    fn test_retry_policy_with_timeout() {
        let policy = RetryPolicy::default().with_timeout(Duration::from_secs(30));
        assert_eq!(policy.timeout, Some(Duration::from_secs(30)));
    }

    #[test]
    fn test_retry_policy_default() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_retries, 3);
    }

    // === RetryState tests ===

    #[test]
    fn test_retry_state_new() {
        let state = RetryState::new();
        assert_eq!(state.attempt, 0);
        assert!(state.errors.is_empty());
    }

    #[test]
    fn test_retry_state_record_error() {
        let mut state = RetryState::new();
        state.record_error("error1".into());
        assert_eq!(state.attempt, 1);
        assert_eq!(state.errors.len(), 1);
        assert_eq!(state.last_error(), Some("error1"));
    }

    #[test]
    fn test_retry_state_multiple_errors() {
        let mut state = RetryState::new();
        state.record_error("e1".into());
        state.record_error("e2".into());
        state.record_error("e3".into());
        assert_eq!(state.attempt, 3);
        assert_eq!(state.last_error(), Some("e3"));
    }

    // === RetryFlow tests ===

    #[test]
    fn test_retry_flow_success() {
        let mut flow: RetryFlow<String> = RetryFlow::new(RetryPolicy::default());
        flow.on_success();
        assert_eq!(flow.stats().successes, 1);
    }

    #[test]
    fn test_retry_flow_failure_triggers_retry() {
        let policy = RetryPolicy::fixed(3, Duration::from_millis(100));
        let mut flow = RetryFlow::new(policy);

        let decision = flow.on_failure("element".to_string(), "test error");
        assert!(matches!(decision, RetryDecision::Retry { .. }));
        assert_eq!(flow.stats().retries, 1);
    }

    #[test]
    fn test_retry_flow_max_retries_exhausted() {
        let policy = RetryPolicy::fixed(2, Duration::from_millis(10));
        let mut flow = RetryFlow::new(policy);

        // First retry (new element)
        let d1 = flow.on_failure("elem".to_string(), "err1");
        assert!(matches!(d1, RetryDecision::Retry { .. }));

        // Get pending with state
        let (elem, state) = flow.next_pending().unwrap();

        // Second retry (same element, carry state forward)
        let d2 = flow.on_failure_retry(elem.clone(), "err2", state);
        assert!(matches!(d2, RetryDecision::Retry { .. }));

        // Get pending again
        let (elem, state) = flow.next_pending().unwrap();

        // Third attempt = give up (max_retries=2, attempt becomes 3 > 2)
        let d3 = flow.on_failure_retry(elem, "err3", state);
        assert!(matches!(d3, RetryDecision::GiveUp { .. }));
    }

    #[test]
    fn test_retry_flow_shutdown() {
        let mut flow: RetryFlow<String> = RetryFlow::new(RetryPolicy::default());
        flow.shutdown();
        let decision = flow.on_failure("x".to_string(), "err");
        assert!(matches!(decision, RetryDecision::GiveUp { .. }));
    }

    #[test]
    fn test_retry_flow_pending() {
        let policy = RetryPolicy::fixed(3, Duration::from_millis(10));
        let mut flow = RetryFlow::new(policy);
        flow.on_failure("elem1".to_string(), "err");
        assert_eq!(flow.pending_count(), 1);

        let (elem, _state) = flow.next_pending().unwrap();
        assert_eq!(elem, "elem1".to_string());
        assert_eq!(flow.pending_count(), 0);
    }

    // === RecoverWith tests ===

    #[test]
    fn test_recover_with_default() {
        let mut recover = RecoverWith::with_default(0);
        let result = recover.on_error("some error");
        assert_eq!(result, RecoverResult::Emit(0));
    }

    #[test]
    fn test_recover_with_skip() {
        let mut recover: RecoverWith<i32> = RecoverWith::skip();
        let result = recover.on_error("err");
        assert!(result.is_skip());
    }

    #[test]
    fn test_recover_with_stop() {
        let mut recover: RecoverWith<i32> = RecoverWith::stop();
        let result = recover.on_error("err");
        assert!(result.is_stop());
        assert!(recover.stats().stopped);
    }

    #[test]
    fn test_recover_with_last_success() {
        let mut recover = RecoverWith::last_success(0);

        // No prior success: use default
        let result = recover.on_error("err");
        assert_eq!(result, RecoverResult::Emit(0));

        // After a success
        recover.on_success(42);
        let result = recover.on_error("err");
        assert_eq!(result, RecoverResult::Emit(42));
    }

    #[test]
    fn test_recover_on_success() {
        let mut recover = RecoverWith::with_default(0);
        let result = recover.on_success(42);
        assert_eq!(result, RecoverResult::Emit(42));
        assert_eq!(recover.stats().passed, 1);
    }

    #[test]
    fn test_recover_stats() {
        let mut recover = RecoverWith::with_default(0);
        recover.on_success(1);
        recover.on_success(2);
        recover.on_error("err");
        assert_eq!(recover.stats().passed, 2);
        assert_eq!(recover.stats().recovered, 1);
        assert!((recover.stats().recovery_rate() - 1.0 / 3.0).abs() < 0.01);
    }

    // === RestartSettings tests ===

    #[test]
    fn test_restart_settings_default() {
        let settings = RestartSettings::default();
        assert_eq!(settings.min_backoff, Duration::from_millis(100));
        assert_eq!(settings.max_backoff, Duration::from_secs(30));
    }

    #[test]
    fn test_restart_settings_backoff_increases() {
        let settings = RestartSettings::new(
            Duration::from_millis(100),
            Duration::from_secs(10),
            0.0, // no jitter for deterministic test
        );
        let d0 = settings.backoff_for_restart(0);
        let d1 = settings.backoff_for_restart(1);
        let d2 = settings.backoff_for_restart(2);
        assert!(d0 <= d1);
        assert!(d1 <= d2);
    }

    #[test]
    fn test_restart_settings_with_max_restarts() {
        let settings = RestartSettings::default()
            .with_max_restarts(5, Duration::from_secs(60));
        assert_eq!(settings.max_restarts, 5);
    }

    // === RestartFlow tests ===

    #[test]
    fn test_restart_flow_first_failure() {
        let settings = RestartSettings::default();
        let mut flow = RestartFlow::new(settings);
        assert_eq!(*flow.state(), RestartState::Running);

        let result = flow.on_failure("test error");
        assert!(result.is_ok());
        assert!(matches!(flow.state(), RestartState::BackingOff { .. }));
    }

    #[test]
    fn test_restart_flow_restart_complete() {
        let mut flow = RestartFlow::new(RestartSettings::default());
        flow.on_failure("err").unwrap();
        flow.on_restart_complete();
        assert_eq!(*flow.state(), RestartState::Running);
    }

    #[test]
    fn test_restart_flow_max_restarts_exceeded() {
        let settings = RestartSettings::default()
            .with_max_restarts(2, Duration::from_secs(60));
        let mut flow = RestartFlow::new(settings);

        assert!(flow.on_failure("e1").is_ok());
        flow.on_restart_complete();
        assert!(flow.on_failure("e2").is_ok());
        flow.on_restart_complete();

        // 3rd failure should fail
        let result = flow.on_failure("e3");
        assert!(result.is_err());
        assert_eq!(*flow.state(), RestartState::Failed);
    }

    #[test]
    fn test_restart_flow_stop() {
        let mut flow = RestartFlow::new(RestartSettings::default());
        flow.stop();
        assert_eq!(*flow.state(), RestartState::Stopped);
        assert!(flow.on_failure("err").is_err());
    }

    #[test]
    fn test_restart_flow_stats() {
        let mut flow = RestartFlow::new(RestartSettings::default());
        flow.on_failure("e1").unwrap();
        flow.on_restart_complete();
        flow.on_failure("e2").unwrap();

        assert_eq!(flow.stats().restarts, 2);
        assert!(flow.stats().total_backoff > Duration::ZERO);
    }

    #[test]
    fn test_restart_flow_count() {
        let mut flow = RestartFlow::new(RestartSettings::default());
        flow.on_failure("e1").unwrap();
        assert_eq!(flow.restart_count(), 1);
        flow.on_failure("e2").unwrap();
        assert_eq!(flow.restart_count(), 2);
    }

    // === RetryBudget tests ===

    #[test]
    fn test_budget_initial_full() {
        let budget = RetryBudget::new(10.0, 1.0);
        assert!((budget.available_budget() - 10.0).abs() < f64::EPSILON);
        assert!((budget.max_budget() - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_budget_try_retry_success() {
        let mut budget = RetryBudget::new(10.0, 1.0);
        assert!(budget.try_retry());
        assert!(budget.available_budget() < 10.0);
    }

    #[test]
    fn test_budget_exhaustion() {
        let mut budget = RetryBudget::new(3.0, 0.0); // No refill
        assert!(budget.try_retry()); // 2.0 left
        assert!(budget.try_retry()); // 1.0 left
        assert!(budget.try_retry()); // 0.0 left
        assert!(!budget.try_retry()); // denied
    }

    #[test]
    fn test_budget_deposit_success() {
        let mut budget = RetryBudget::new(10.0, 0.0);
        budget.try_retry(); // 9.0
        budget.deposit_success(); // 9.1
        assert!(budget.available_budget() > 9.0);
    }

    #[test]
    fn test_budget_with_costs() {
        let mut budget = RetryBudget::new(10.0, 0.0).with_costs(5.0, 1.0);
        assert!(budget.try_retry()); // 5.0 left
        assert!(budget.try_retry()); // 0.0 left
        assert!(!budget.try_retry()); // denied
    }

    #[test]
    fn test_budget_stats() {
        let mut budget = RetryBudget::new(2.0, 0.0);
        budget.try_retry();
        budget.try_retry();
        budget.try_retry(); // denied

        assert_eq!(budget.stats().retries_allowed, 2);
        assert_eq!(budget.stats().retries_denied, 1);
        assert!((budget.stats().denial_rate() - 1.0 / 3.0).abs() < 0.01);
    }

    #[test]
    fn test_budget_deposit_stats() {
        let mut budget = RetryBudget::new(10.0, 0.0);
        budget.deposit_success();
        budget.deposit_success();
        assert_eq!(budget.stats().deposits, 2);
    }

    // === RetryFlowStats tests ===

    #[test]
    fn test_retry_flow_stats_total() {
        let stats = RetryFlowStats {
            successes: 10,
            retries: 5,
            exhausted: 2,
        };
        assert_eq!(stats.total_attempts(), 17);
    }

    #[test]
    fn test_retry_flow_stats_retry_rate() {
        let stats = RetryFlowStats {
            successes: 8,
            retries: 2,
            exhausted: 0,
        };
        assert!((stats.retry_rate() - 0.2).abs() < f64::EPSILON);
    }

    // === RecoverResult tests ===

    #[test]
    fn test_recover_result_methods() {
        let emit: RecoverResult<i32> = RecoverResult::Emit(42);
        assert!(emit.is_emit());
        assert!(!emit.is_skip());
        assert!(!emit.is_stop());

        let skip: RecoverResult<i32> = RecoverResult::Skip;
        assert!(skip.is_skip());

        let stop: RecoverResult<i32> = RecoverResult::Stop;
        assert!(stop.is_stop());
    }

    // === RestartFlowStats tests ===

    #[test]
    fn test_restart_stats_average_backoff() {
        let stats = RestartFlowStats {
            restarts: 4,
            failures: 0,
            total_backoff: Duration::from_secs(8),
        };
        assert_eq!(stats.average_backoff(), Duration::from_secs(2));
    }

    #[test]
    fn test_restart_stats_zero_restarts() {
        let stats = RestartFlowStats::default();
        assert_eq!(stats.average_backoff(), Duration::ZERO);
    }

    // === RetryError tests ===

    #[test]
    fn test_retry_error_display() {
        let err = RetryError::RetriesExhausted { attempts: 5, last_error: "timeout".into() };
        assert!(err.to_string().contains("5"));
        assert!(err.to_string().contains("timeout"));
    }

    #[test]
    fn test_retry_error_budget() {
        let err = RetryError::BudgetExhausted { remaining_budget: 0.5 };
        assert!(err.to_string().contains("0.50"));
    }

    #[test]
    fn test_retry_error_timeout() {
        let err = RetryError::Timeout { elapsed: Duration::from_secs(30) };
        assert!(err.to_string().contains("30"));
    }

    #[test]
    fn test_retry_error_circuit() {
        let err = RetryError::CircuitOpen;
        assert!(err.to_string().contains("Circuit"));
    }

    #[test]
    fn test_retry_error_shutdown() {
        let err = RetryError::Shutdown;
        assert!(err.to_string().contains("shut down"));
    }
}

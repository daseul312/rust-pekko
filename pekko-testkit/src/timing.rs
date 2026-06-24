//! Timing and synchronization helpers for actor tests
//!
//! Provides utilities for time-bounded assertions and eventual consistency checks.

use std::future::Future;
use std::time::{Duration, Instant};
use thiserror::Error;

/// Timing-related test errors
#[derive(Debug, Error)]
pub enum TimingError {
    #[error("Operation exceeded deadline of {0:?} (elapsed: {1:?})")]
    Deadline(Duration, Duration),

    #[error("Condition not met within {0:?}")]
    ConditionNotMet(Duration),
}

/// Execute an async operation within a time bound.
///
/// Panics if the operation takes longer than `max_duration`.
///
/// ## Example
/// ```ignore
/// let result = Within::new(Duration::from_secs(1))
///     .run(async { some_operation().await })
///     .await;
/// ```
pub struct Within {
    max_duration: Duration,
}

impl Within {
    /// Create a new Within assertion with a max duration
    pub fn new(max_duration: Duration) -> Self {
        Self { max_duration }
    }

    /// Run an async operation within the time bound
    pub async fn run<F, R>(&self, future: F) -> R
    where
        F: Future<Output = R>,
    {
        let start = Instant::now();
        match tokio::time::timeout(self.max_duration, future).await {
            Ok(result) => result,
            Err(_) => panic!(
                "Within: operation exceeded deadline of {:?} (elapsed: {:?})",
                self.max_duration,
                start.elapsed()
            ),
        }
    }

    /// Run an async operation and return Result instead of panicking
    pub async fn try_run<F, R>(&self, future: F) -> Result<R, TimingError>
    where
        F: Future<Output = R>,
    {
        let start = Instant::now();
        match tokio::time::timeout(self.max_duration, future).await {
            Ok(result) => Ok(result),
            Err(_) => Err(TimingError::Deadline(self.max_duration, start.elapsed())),
        }
    }
}

/// Assert that a condition becomes true within a timeout.
///
/// Polls the condition at regular intervals until it returns true
/// or the timeout is reached.
///
/// ## Example
/// ```ignore
/// EventuallyAssert::new(Duration::from_secs(5))
///     .with_interval(Duration::from_millis(100))
///     .assert(|| counter.load(Ordering::SeqCst) >= 10)
///     .await;
/// ```
pub struct EventuallyAssert {
    max_wait: Duration,
    poll_interval: Duration,
}

impl EventuallyAssert {
    /// Create with max wait time (default poll interval: 50ms)
    pub fn new(max_wait: Duration) -> Self {
        Self {
            max_wait,
            poll_interval: Duration::from_millis(50),
        }
    }

    /// Set custom poll interval
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Assert that a sync condition becomes true within the timeout
    pub async fn assert<F>(&self, condition: F)
    where
        F: Fn() -> bool,
    {
        let start = Instant::now();
        loop {
            if condition() {
                return;
            }
            if start.elapsed() > self.max_wait {
                panic!(
                    "EventuallyAssert: condition not met within {:?}",
                    self.max_wait
                );
            }
            tokio::time::sleep(self.poll_interval).await;
        }
    }

    /// Assert with a message for better error output
    pub async fn assert_with_msg<F>(&self, msg: &str, condition: F)
    where
        F: Fn() -> bool,
    {
        let start = Instant::now();
        loop {
            if condition() {
                return;
            }
            if start.elapsed() > self.max_wait {
                panic!(
                    "EventuallyAssert: '{}' - condition not met within {:?}",
                    msg, self.max_wait
                );
            }
            tokio::time::sleep(self.poll_interval).await;
        }
    }

    /// Try to assert (returns Result instead of panicking)
    pub async fn try_assert<F>(&self, condition: F) -> Result<(), TimingError>
    where
        F: Fn() -> bool,
    {
        let start = Instant::now();
        loop {
            if condition() {
                return Ok(());
            }
            if start.elapsed() > self.max_wait {
                return Err(TimingError::ConditionNotMet(self.max_wait));
            }
            tokio::time::sleep(self.poll_interval).await;
        }
    }
}

/// Convenience: sleep for N milliseconds
pub async fn sleep_millis(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

/// Convenience: yield to the async scheduler
pub async fn yield_now() {
    tokio::task::yield_now().await;
}

/// Measure the duration of an async operation
pub async fn measure<F, R>(future: F) -> (R, Duration)
where
    F: Future<Output = R>,
{
    let start = Instant::now();
    let result = future.await;
    (result, start.elapsed())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_within_success() {
        let result = Within::new(Duration::from_secs(1))
            .run(async { 42 })
            .await;
        assert_eq!(result, 42);
    }

    #[tokio::test]
    #[should_panic(expected = "exceeded deadline")]
    async fn test_within_timeout() {
        Within::new(Duration::from_millis(50))
            .run(async {
                tokio::time::sleep(Duration::from_secs(1)).await;
            })
            .await;
    }

    #[tokio::test]
    async fn test_within_try_run() {
        let result = Within::new(Duration::from_millis(50))
            .try_run(async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                42
            })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_eventually_assert() {
        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();

        // Spawn a task that increments counter
        tokio::spawn(async move {
            for _ in 0..10 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                c.fetch_add(1, Ordering::SeqCst);
            }
        });

        EventuallyAssert::new(Duration::from_secs(1))
            .with_interval(Duration::from_millis(20))
            .assert(move || counter.load(Ordering::SeqCst) >= 5)
            .await;
    }

    #[tokio::test]
    #[should_panic(expected = "condition not met")]
    async fn test_eventually_assert_fails() {
        EventuallyAssert::new(Duration::from_millis(100))
            .with_interval(Duration::from_millis(20))
            .assert(|| false)
            .await;
    }

    #[tokio::test]
    async fn test_eventually_try_assert() {
        let result = EventuallyAssert::new(Duration::from_millis(100))
            .try_assert(|| false)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sleep_millis() {
        let start = Instant::now();
        sleep_millis(50).await;
        assert!(start.elapsed() >= Duration::from_millis(40));
    }

    #[tokio::test]
    async fn test_measure() {
        let (result, duration) = measure(async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            "done"
        })
        .await;
        assert_eq!(result, "done");
        assert!(duration >= Duration::from_millis(40));
    }
}

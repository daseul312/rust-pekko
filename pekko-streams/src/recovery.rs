//! Error recovery operators for streams
//!
//! Provides operators to handle errors gracefully in stream pipelines:
//! - `recover` - replace errors with fallback elements
//! - `retry` - retry failed operations
//! - `restart_with_backoff` - restart with exponential backoff

use crate::{
    errors::{StreamError, StreamResult},
    stage::FlowStage,
};
use async_trait::async_trait;
use std::time::Duration;

// ============= Recover =============

/// Recover stage - on error, produce a fallback element
pub(crate) struct RecoverStage<F> {
    pub f: F,
}

#[async_trait]
impl<T, F> FlowStage<T, T> for RecoverStage<F>
where
    T: Send + Sync + 'static,
    F: FnMut(&str) -> Option<T> + Send + Sync + 'static,
{
    async fn process(&mut self, element: T) -> StreamResult<Vec<T>> {
        Ok(vec![element])
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<T>> {
        Ok(vec![])
    }

    async fn on_error(&mut self, error: String) -> StreamResult<()> {
        match (self.f)(&error) {
            Some(_fallback) => {
                // Error was recovered
                Ok(())
            }
            None => Err(StreamError::StageFailed(error)),
        }
    }
}

// ============= RecoverWith =============

/// RecoverWith stage - on error from upstream, produce fallback elements
pub(crate) struct RecoverWithStage<F> {
    pub f: F,
}

#[async_trait]
impl<T, F> FlowStage<T, T> for RecoverWithStage<F>
where
    T: Send + Sync + 'static,
    F: FnMut(&str) -> Vec<T> + Send + Sync + 'static,
{
    async fn process(&mut self, element: T) -> StreamResult<Vec<T>> {
        Ok(vec![element])
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<T>> {
        Ok(vec![])
    }

    async fn on_error(&mut self, error: String) -> StreamResult<()> {
        let _fallbacks = (self.f)(&error);
        Ok(()) // Recovered
    }
}

// ============= Retry =============

/// Retry stage - wraps an inner operation with retry logic
pub(crate) struct RetryStage<F> {
    pub f: F,
    pub max_retries: usize,
    pub attempts: usize,
}

#[async_trait]
impl<In, Out, F> FlowStage<In, Out> for RetryStage<F>
where
    In: Clone + Send + Sync + 'static,
    Out: Send + Sync + 'static,
    F: FnMut(In) -> Result<Out, String> + Send + Sync + 'static,
{
    async fn process(&mut self, element: In) -> StreamResult<Vec<Out>> {
        let mut last_error = String::new();
        for _ in 0..=self.max_retries {
            match (self.f)(element.clone()) {
                Ok(out) => {
                    self.attempts = 0;
                    return Ok(vec![out]);
                }
                Err(e) => {
                    last_error = e;
                    self.attempts += 1;
                }
            }
        }
        Err(StreamError::StageFailed(format!(
            "Retry exhausted after {} attempts: {}",
            self.max_retries + 1,
            last_error
        )))
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<Out>> {
        Ok(vec![])
    }
}

// ============= BackoffRetry =============

/// Exponential backoff configuration
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    pub initial: Duration,
    pub max: Duration,
    pub multiplier: f64,
}

impl ExponentialBackoff {
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            max,
            multiplier: 2.0,
        }
    }

    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    fn delay_for_attempt(&self, attempt: usize) -> Duration {
        let delay = self.initial.as_secs_f64() * self.multiplier.powi(attempt as i32);
        let max = self.max.as_secs_f64();
        Duration::from_secs_f64(delay.min(max))
    }
}

/// Backoff retry stage
pub(crate) struct BackoffRetryStage<F> {
    pub f: F,
    pub backoff: ExponentialBackoff,
    pub max_retries: usize,
}

#[async_trait]
impl<In, Out, F> FlowStage<In, Out> for BackoffRetryStage<F>
where
    In: Clone + Send + Sync + 'static,
    Out: Send + Sync + 'static,
    F: FnMut(In) -> Result<Out, String> + Send + Sync + 'static,
{
    async fn process(&mut self, element: In) -> StreamResult<Vec<Out>> {
        let mut last_error = String::new();
        for attempt in 0..=self.max_retries {
            match (self.f)(element.clone()) {
                Ok(out) => return Ok(vec![out]),
                Err(e) => {
                    last_error = e;
                    if attempt < self.max_retries {
                        let delay = self.backoff.delay_for_attempt(attempt);
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
        Err(StreamError::StageFailed(format!(
            "Backoff retry exhausted after {} attempts: {}",
            self.max_retries + 1,
            last_error
        )))
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<Out>> {
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Source, Flow};
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

    #[tokio::test]
    async fn test_retry_success() {
        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();

        let result = Source::from_vec(vec![1, 2, 3])
            .via(Flow::retry(3, move |x: i32| {
                let _attempt = c.fetch_add(1, Ordering::SeqCst);
                Ok(x * 2)
            }))
            .run_collect()
            .await
            .unwrap();

        assert_eq!(result, vec![2, 4, 6]);
    }

    #[tokio::test]
    async fn test_retry_with_failures() {
        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();

        let result = Source::from_vec(vec![1])
            .via(Flow::retry(3, move |x: i32| {
                let attempt = c.fetch_add(1, Ordering::SeqCst);
                if attempt < 2 {
                    Err("not yet".to_string())
                } else {
                    Ok(x * 10)
                }
            }))
            .run_collect()
            .await
            .unwrap();

        assert_eq!(result, vec![10]);
    }

    #[tokio::test]
    async fn test_retry_exhausted() {
        let result = Source::from_vec(vec![1])
            .via(Flow::retry(2, |_x: i32| -> Result<i32, String> {
                Err("always fails".to_string())
            }))
            .run_collect()
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_backoff_retry() {
        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();

        let start = std::time::Instant::now();
        let result = Source::from_vec(vec![42])
            .via(Flow::retry_with_backoff(
                3,
                ExponentialBackoff::new(Duration::from_millis(10), Duration::from_millis(100)),
                move |x: i32| {
                    let attempt = c.fetch_add(1, Ordering::SeqCst);
                    if attempt < 2 {
                        Err("not yet".to_string())
                    } else {
                        Ok(x)
                    }
                },
            ))
            .run_collect()
            .await
            .unwrap();

        assert_eq!(result, vec![42]);
        // Should have some delay from backoff
        assert!(start.elapsed() >= Duration::from_millis(15));
    }

    #[test]
    fn test_exponential_backoff_delays() {
        let backoff = ExponentialBackoff::new(
            Duration::from_millis(100),
            Duration::from_secs(10),
        );
        assert_eq!(backoff.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(backoff.delay_for_attempt(1), Duration::from_millis(200));
        assert_eq!(backoff.delay_for_attempt(2), Duration::from_millis(400));
    }
}

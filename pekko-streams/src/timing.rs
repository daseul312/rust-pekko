//! Time-based stream operators
//!
//! Provides operators for rate-limiting, buffering, delaying, and debouncing.

use crate::{
    errors::StreamResult,
    stage::FlowStage,
};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

// ============= Throttle =============

/// Throttle stage - rate-limits elements to at most one per duration
pub(crate) struct ThrottleStage {
    pub interval: Duration,
    pub max_burst: usize,
    pub tokens: usize,
    pub last_refill: Instant,
}

impl ThrottleStage {
    pub fn new(interval: Duration, max_burst: usize) -> Self {
        Self {
            interval,
            max_burst,
            tokens: max_burst,
            last_refill: Instant::now(),
        }
    }

    fn refill_tokens(&mut self) {
        let elapsed = self.last_refill.elapsed();
        let new_tokens = (elapsed.as_secs_f64() / self.interval.as_secs_f64()) as usize;
        if new_tokens > 0 {
            self.tokens = (self.tokens + new_tokens).min(self.max_burst);
            self.last_refill = Instant::now();
        }
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> FlowStage<T, T> for ThrottleStage {
    async fn process(&mut self, element: T) -> StreamResult<Vec<T>> {
        self.refill_tokens();
        if self.tokens > 0 {
            self.tokens -= 1;
            Ok(vec![element])
        } else {
            // Wait for next token
            tokio::time::sleep(self.interval).await;
            self.tokens = self.max_burst.saturating_sub(1);
            self.last_refill = Instant::now();
            Ok(vec![element])
        }
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<T>> {
        Ok(vec![])
    }
}

// ============= Buffer =============

/// Buffer stage - buffers up to N elements
pub(crate) struct BufferStage<T> {
    pub buffer: VecDeque<T>,
    pub capacity: usize,
}

#[async_trait]
impl<T: Send + Sync + 'static> FlowStage<T, T> for BufferStage<T> {
    async fn process(&mut self, element: T) -> StreamResult<Vec<T>> {
        self.buffer.push_back(element);

        // If buffer is full, flush one element
        if self.buffer.len() > self.capacity {
            if let Some(elem) = self.buffer.pop_front() {
                return Ok(vec![elem]);
            }
        }
        Ok(vec![])
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<T>> {
        Ok(self.buffer.drain(..).collect())
    }
}

// ============= Delay =============

/// Delay stage - delays each element by a duration
pub(crate) struct DelayStage {
    pub duration: Duration,
}

#[async_trait]
impl<T: Send + Sync + 'static> FlowStage<T, T> for DelayStage {
    async fn process(&mut self, element: T) -> StreamResult<Vec<T>> {
        tokio::time::sleep(self.duration).await;
        Ok(vec![element])
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<T>> {
        Ok(vec![])
    }
}

// ============= Debounce =============

/// Debounce stage - emits the latest element after a period of silence
pub(crate) struct DebounceStage<T> {
    pub duration: Duration,
    pub pending: Option<T>,
    pub last_seen: Option<Instant>,
}

#[async_trait]
impl<T: Send + Sync + 'static> FlowStage<T, T> for DebounceStage<T> {
    async fn process(&mut self, element: T) -> StreamResult<Vec<T>> {
        let now = Instant::now();
        let mut result = vec![];

        // If there's a pending element and enough time has passed, emit it
        if let (Some(pending), Some(last)) = (self.pending.take(), self.last_seen) {
            if now.duration_since(last) >= self.duration {
                result.push(pending);
            }
        }

        self.pending = Some(element);
        self.last_seen = Some(now);
        Ok(result)
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<T>> {
        // Emit the last pending element on completion
        match self.pending.take() {
            Some(elem) => Ok(vec![elem]),
            None => Ok(vec![]),
        }
    }
}

// ============= Sample =============

/// Sample stage - emits the latest element at regular intervals
pub(crate) struct SampleStage<T> {
    pub interval: Duration,
    pub last_emit: Instant,
    pub latest: Option<T>,
}

#[async_trait]
impl<T: Send + Sync + 'static> FlowStage<T, T> for SampleStage<T> {
    async fn process(&mut self, element: T) -> StreamResult<Vec<T>> {
        self.latest = Some(element);
        let now = Instant::now();

        if now.duration_since(self.last_emit) >= self.interval {
            self.last_emit = now;
            match self.latest.take() {
                Some(elem) => Ok(vec![elem]),
                None => Ok(vec![]),
            }
        } else {
            Ok(vec![])
        }
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<T>> {
        match self.latest.take() {
            Some(elem) => Ok(vec![elem]),
            None => Ok(vec![]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Source, Flow};

    #[tokio::test]
    async fn test_throttle() {
        let start = Instant::now();
        let result = Source::from_vec(vec![1, 2, 3])
            .via(Flow::throttle(Duration::from_millis(10), 1))
            .run_collect()
            .await
            .unwrap();

        assert_eq!(result, vec![1, 2, 3]);
        // At least some time should have passed
        assert!(start.elapsed() >= Duration::from_millis(15));
    }

    #[tokio::test]
    async fn test_throttle_burst() {
        let _start = Instant::now();
        let result = Source::from_vec(vec![1, 2, 3, 4])
            .via(Flow::throttle(Duration::from_millis(50), 3))
            .run_collect()
            .await
            .unwrap();

        assert_eq!(result, vec![1, 2, 3, 4]);
        // First 3 should be fast (burst), 4th should wait
    }

    #[tokio::test]
    async fn test_buffer() {
        let result = Source::from_vec(vec![1, 2, 3, 4, 5])
            .via(Flow::buffer(3))
            .run_collect()
            .await
            .unwrap();

        // Buffer collects up to 3 elements then starts flushing, rest flushed on complete
        assert_eq!(result.len(), 5);
        // All elements should be present
        let mut sorted = result.clone();
        sorted.sort();
        assert_eq!(sorted, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_delay() {
        let start = Instant::now();
        let result = Source::from_vec(vec![1, 2])
            .via(Flow::delay(Duration::from_millis(20)))
            .run_collect()
            .await
            .unwrap();

        assert_eq!(result, vec![1, 2]);
        assert!(start.elapsed() >= Duration::from_millis(35));
    }

    #[tokio::test]
    async fn test_debounce_on_complete() {
        // With debounce, the last pending element is emitted on_complete
        let result = Source::from_vec(vec![1, 2, 3])
            .via(Flow::debounce(Duration::from_secs(10))) // long debounce
            .run_collect()
            .await
            .unwrap();

        // Only the last element should be emitted (on_complete)
        assert!(result.contains(&3));
    }
}

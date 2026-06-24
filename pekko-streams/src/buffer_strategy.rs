//! Advanced Buffer Strategies — Dynamic buffering and priority queues
//!
//! Extends the basic overflow strategies with:
//!
//! - **DynamicBuffer**: Buffer that grows/shrinks based on demand
//! - **PriorityBuffer**: Priority queue-based buffer
//! - **ConflateBuffer**: Merge/aggregate incoming elements when back-pressured
//! - **ExtrapolateBuffer**: Generate elements when upstream is slow
//! - **BufferMetrics**: Monitoring for buffer utilization

use crate::errors::{StreamError, StreamResult};
use crate::stage::{FlowStage, StreamElement};
use async_trait::async_trait;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use tracing::debug;

// ============= Dynamic Buffer =============

/// A buffer that dynamically adjusts its capacity within bounds.
pub struct DynamicBuffer<T: Send + Sync> {
    buffer: VecDeque<T>,
    min_capacity: usize,
    max_capacity: usize,
    current_capacity: usize,
    /// Scale up when buffer utilization exceeds this ratio (0.0–1.0).
    high_watermark: f64,
    /// Scale down when utilization drops below this ratio.
    low_watermark: f64,
    metrics: BufferMetrics,
}

impl<T: Send + Sync> DynamicBuffer<T> {
    pub fn new(min_capacity: usize, max_capacity: usize) -> Self {
        assert!(min_capacity > 0);
        assert!(max_capacity >= min_capacity);
        Self {
            buffer: VecDeque::with_capacity(min_capacity),
            min_capacity,
            max_capacity,
            current_capacity: min_capacity,
            high_watermark: 0.8,
            low_watermark: 0.2,
            metrics: BufferMetrics::new(),
        }
    }

    pub fn with_watermarks(mut self, low: f64, high: f64) -> Self {
        self.low_watermark = low.clamp(0.0, 1.0);
        self.high_watermark = high.clamp(0.0, 1.0);
        self
    }

    pub fn enqueue(&mut self, item: T) -> Result<(), T> {
        if self.buffer.len() >= self.current_capacity {
            self.try_expand();
        }
        if self.buffer.len() < self.current_capacity {
            self.buffer.push_back(item);
            self.metrics.record_enqueue();
            self.adjust_capacity();
            Ok(())
        } else {
            self.metrics.record_overflow();
            Err(item)
        }
    }

    pub fn dequeue(&mut self) -> Option<T> {
        let item = self.buffer.pop_front();
        if item.is_some() {
            self.metrics.record_dequeue();
            self.adjust_capacity();
        }
        item
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.current_capacity
    }

    pub fn utilization(&self) -> f64 {
        if self.current_capacity == 0 {
            0.0
        } else {
            self.buffer.len() as f64 / self.current_capacity as f64
        }
    }

    pub fn metrics(&self) -> &BufferMetrics {
        &self.metrics
    }

    fn try_expand(&mut self) {
        if self.current_capacity < self.max_capacity {
            self.current_capacity = (self.current_capacity * 2).min(self.max_capacity);
            debug!("DynamicBuffer: expanded to {}", self.current_capacity);
        }
    }

    fn adjust_capacity(&mut self) {
        let util = self.utilization();
        if util < self.low_watermark && self.current_capacity > self.min_capacity {
            let new_cap = (self.current_capacity / 2).max(self.min_capacity);
            if new_cap >= self.buffer.len() {
                self.current_capacity = new_cap;
                debug!("DynamicBuffer: shrunk to {}", self.current_capacity);
            }
        }
    }
}

// ============= Priority Buffer =============

/// Wrapper for priority queue items.
struct PriorityItem<T> {
    priority: i64,
    seq: u64,
    item: T,
}

impl<T> Eq for PriorityItem<T> {}

impl<T> PartialEq for PriorityItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.seq == other.seq
    }
}

impl<T> Ord for PriorityItem<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority first, then earlier sequence (FIFO for same priority)
        self.priority.cmp(&other.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl<T> PartialOrd for PriorityItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A buffer that dequeues elements by priority.
pub struct PriorityBuffer<T: Send + Sync> {
    heap: BinaryHeap<PriorityItem<T>>,
    capacity: usize,
    seq_counter: u64,
    metrics: BufferMetrics,
}

impl<T: Send + Sync> PriorityBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(capacity),
            capacity,
            seq_counter: 0,
            metrics: BufferMetrics::new(),
        }
    }

    pub fn enqueue(&mut self, item: T, priority: i64) -> Result<(), T> {
        if self.heap.len() >= self.capacity {
            self.metrics.record_overflow();
            return Err(item);
        }
        let seq = self.seq_counter;
        self.seq_counter += 1;
        self.heap.push(PriorityItem { priority, seq, item });
        self.metrics.record_enqueue();
        Ok(())
    }

    pub fn dequeue(&mut self) -> Option<T> {
        let result = self.heap.pop().map(|pi| pi.item);
        if result.is_some() {
            self.metrics.record_dequeue();
        }
        result
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn metrics(&self) -> &BufferMetrics {
        &self.metrics
    }
}

// ============= Conflate Buffer =============

/// Conflate: when back-pressured, merge new elements into a seed value.
/// This reduces multiple pending elements into one by aggregating them.
pub struct ConflateStage<T, S, SeedFn, AggregateFn>
where
    T: Send + Sync,
    S: Send + Sync + Clone,
    SeedFn: Fn(&T) -> S + Send + Sync,
    AggregateFn: Fn(S, T) -> S + Send + Sync,
{
    seed_fn: SeedFn,
    aggregate_fn: AggregateFn,
    state: Option<S>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, S, SeedFn, AggregateFn> ConflateStage<T, S, SeedFn, AggregateFn>
where
    T: Send + Sync,
    S: Send + Sync + Clone,
    SeedFn: Fn(&T) -> S + Send + Sync,
    AggregateFn: Fn(S, T) -> S + Send + Sync,
{
    pub fn new(seed_fn: SeedFn, aggregate_fn: AggregateFn) -> Self {
        Self {
            seed_fn,
            aggregate_fn,
            state: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T, S, SeedFn, AggregateFn> FlowStage<T, S> for ConflateStage<T, S, SeedFn, AggregateFn>
where
    T: Send + Sync + 'static,
    S: Send + Sync + Clone + 'static,
    SeedFn: Fn(&T) -> S + Send + Sync,
    AggregateFn: Fn(S, T) -> S + Send + Sync,
{
    async fn process(&mut self, element: T) -> StreamResult<Vec<S>> {
        match self.state.take() {
            None => {
                // First element: create seed
                self.state = Some((self.seed_fn)(&element));
                // In Pekko, conflate emits when downstream demands.
                // Here we simulate by always emitting the latest aggregate.
                Ok(vec![self.state.clone().unwrap()])
            }
            Some(current) => {
                // Aggregate into existing state
                let new_state = (self.aggregate_fn)(current, element);
                let output = new_state.clone();
                self.state = Some(new_state);
                Ok(vec![output])
            }
        }
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<S>> {
        match self.state.take() {
            Some(final_state) => Ok(vec![final_state]),
            None => Ok(vec![]),
        }
    }
}

// ============= Extrapolate =============

/// Extrapolate: when downstream demands but upstream hasn't produced,
/// generates elements from the last received element.
pub struct ExtrapolateStage<T, F>
where
    T: Send + Sync + Clone,
    F: Fn(&T) -> T + Send + Sync,
{
    extrapolate_fn: F,
    last: Option<T>,
}

impl<T, F> ExtrapolateStage<T, F>
where
    T: Send + Sync + Clone,
    F: Fn(&T) -> T + Send + Sync,
{
    pub fn new(extrapolate_fn: F) -> Self {
        Self {
            extrapolate_fn,
            last: None,
        }
    }

    /// Generate an extrapolated element if no new input.
    pub fn extrapolate(&mut self) -> Option<T> {
        if let Some(l) = self.last.take() {
            let next = (self.extrapolate_fn)(&l);
            self.last = Some(next.clone());
            Some(next)
        } else {
            None
        }
    }
}

#[async_trait]
impl<T, F> FlowStage<T, T> for ExtrapolateStage<T, F>
where
    T: Send + Sync + Clone + 'static,
    F: Fn(&T) -> T + Send + Sync,
{
    async fn process(&mut self, element: T) -> StreamResult<Vec<T>> {
        self.last = Some(element.clone());
        Ok(vec![element])
    }
}

// ============= Buffer Metrics =============

/// Tracks buffer utilization metrics.
#[derive(Debug, Clone)]
pub struct BufferMetrics {
    enqueued: Arc<AtomicU64>,
    dequeued: Arc<AtomicU64>,
    overflows: Arc<AtomicU64>,
}

impl BufferMetrics {
    pub fn new() -> Self {
        Self {
            enqueued: Arc::new(AtomicU64::new(0)),
            dequeued: Arc::new(AtomicU64::new(0)),
            overflows: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn record_enqueue(&self) {
        self.enqueued.fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub fn record_dequeue(&self) {
        self.dequeued.fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub fn record_overflow(&self) {
        self.overflows.fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub fn enqueued(&self) -> u64 {
        self.enqueued.load(AtomicOrdering::Relaxed)
    }

    pub fn dequeued(&self) -> u64 {
        self.dequeued.load(AtomicOrdering::Relaxed)
    }

    pub fn overflows(&self) -> u64 {
        self.overflows.load(AtomicOrdering::Relaxed)
    }

    pub fn throughput(&self) -> u64 {
        self.dequeued()
    }
}

impl fmt::Display for BufferMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BufferMetrics(enqueued={}, dequeued={}, overflows={})",
            self.enqueued(),
            self.dequeued(),
            self.overflows()
        )
    }
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;

    // ---- DynamicBuffer Tests ----

    #[test]
    fn test_dynamic_buffer_basic() {
        let mut buf = DynamicBuffer::new(4, 16);
        assert_eq!(buf.capacity(), 4);

        for i in 0..4 {
            assert!(buf.enqueue(i).is_ok());
        }
        assert_eq!(buf.len(), 4);

        // Capacity should expand on next enqueue
        assert!(buf.enqueue(4).is_ok());
        assert!(buf.capacity() > 4);
    }

    #[test]
    fn test_dynamic_buffer_dequeue() {
        let mut buf = DynamicBuffer::new(4, 16);
        buf.enqueue(10).unwrap();
        buf.enqueue(20).unwrap();
        assert_eq!(buf.dequeue(), Some(10));
        assert_eq!(buf.dequeue(), Some(20));
        assert_eq!(buf.dequeue(), None);
    }

    #[test]
    fn test_dynamic_buffer_utilization() {
        let mut buf = DynamicBuffer::new(4, 16);
        assert_eq!(buf.utilization(), 0.0);
        buf.enqueue(1).unwrap();
        buf.enqueue(2).unwrap();
        assert!((buf.utilization() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_dynamic_buffer_max_cap() {
        let mut buf = DynamicBuffer::new(2, 4);
        for i in 0..4 {
            assert!(buf.enqueue(i).is_ok());
        }
        // At max capacity, next should fail
        assert!(buf.enqueue(99).is_err());
    }

    #[test]
    fn test_dynamic_buffer_metrics() {
        let mut buf = DynamicBuffer::new(4, 16);
        buf.enqueue(1).unwrap();
        buf.enqueue(2).unwrap();
        buf.dequeue();

        assert_eq!(buf.metrics().enqueued(), 2);
        assert_eq!(buf.metrics().dequeued(), 1);
    }

    // ---- PriorityBuffer Tests ----

    #[test]
    fn test_priority_buffer_ordering() {
        let mut buf = PriorityBuffer::new(10);
        buf.enqueue("low", 1).unwrap();
        buf.enqueue("high", 10).unwrap();
        buf.enqueue("medium", 5).unwrap();

        assert_eq!(buf.dequeue(), Some("high"));
        assert_eq!(buf.dequeue(), Some("medium"));
        assert_eq!(buf.dequeue(), Some("low"));
    }

    #[test]
    fn test_priority_buffer_fifo_same_priority() {
        let mut buf = PriorityBuffer::new(10);
        buf.enqueue("first", 5).unwrap();
        buf.enqueue("second", 5).unwrap();
        buf.enqueue("third", 5).unwrap();

        assert_eq!(buf.dequeue(), Some("first"));
        assert_eq!(buf.dequeue(), Some("second"));
        assert_eq!(buf.dequeue(), Some("third"));
    }

    #[test]
    fn test_priority_buffer_overflow() {
        let mut buf = PriorityBuffer::new(2);
        assert!(buf.enqueue(1, 1).is_ok());
        assert!(buf.enqueue(2, 2).is_ok());
        assert!(buf.enqueue(3, 3).is_err());
        assert_eq!(buf.metrics().overflows(), 1);
    }

    // ---- Conflate Tests ----

    #[tokio::test]
    async fn test_conflate_aggregation() {
        let mut stage = ConflateStage::new(
            |x: &i32| *x,           // seed: identity
            |acc, x| acc + x,        // aggregate: sum
        );

        let r1 = stage.process(10).await.unwrap();
        assert_eq!(r1, vec![10]);

        let r2 = stage.process(20).await.unwrap();
        assert_eq!(r2, vec![30]); // 10 + 20

        let r3 = stage.process(5).await.unwrap();
        assert_eq!(r3, vec![35]); // 30 + 5
    }

    #[tokio::test]
    async fn test_conflate_on_complete() {
        let mut stage = ConflateStage::new(
            |x: &i32| *x,
            |acc, x| acc + x,
        );
        stage.process(1).await.unwrap();
        stage.process(2).await.unwrap();
        stage.process(3).await.unwrap();

        let final_val = stage.on_complete().await.unwrap();
        assert_eq!(final_val, vec![6]);
    }

    // ---- Extrapolate Tests ----

    #[tokio::test]
    async fn test_extrapolate_passthrough() {
        let mut stage = ExtrapolateStage::new(|x: &i32| x + 1);
        let r = stage.process(10).await.unwrap();
        assert_eq!(r, vec![10]);
    }

    #[tokio::test]
    async fn test_extrapolate_generate() {
        let mut stage = ExtrapolateStage::new(|x: &i32| x + 1);
        stage.process(10).await.unwrap();

        // Extrapolate when no new input
        assert_eq!(stage.extrapolate(), Some(11));
        assert_eq!(stage.extrapolate(), Some(12));
        assert_eq!(stage.extrapolate(), Some(13));
    }

    // ---- BufferMetrics Tests ----

    #[test]
    fn test_buffer_metrics() {
        let metrics = BufferMetrics::new();
        metrics.record_enqueue();
        metrics.record_enqueue();
        metrics.record_dequeue();
        metrics.record_overflow();

        assert_eq!(metrics.enqueued(), 2);
        assert_eq!(metrics.dequeued(), 1);
        assert_eq!(metrics.overflows(), 1);
        assert_eq!(metrics.throughput(), 1);
    }

    #[test]
    fn test_buffer_metrics_display() {
        let metrics = BufferMetrics::new();
        metrics.record_enqueue();
        let display = format!("{}", metrics);
        assert!(display.contains("enqueued=1"));
    }
}

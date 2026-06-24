//! Flow - Transforms stream elements
//!
//! A Flow has exactly one input and one output. It represents a transformation
//! step in a stream pipeline.

use crate::{
    errors::StreamResult,
    stage::FlowStage,
    operators::*,
    timing::*,
    recovery::*,
};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// A flow that transforms elements
pub struct Flow<In, Out> {
    pub(crate) inner: Box<dyn FlowStageBoxed<In, Out>>,
}

/// Boxed flow stage trait (for dynamic dispatch)
#[async_trait]
pub trait FlowStageBoxed<In: Send + Sync, Out: Send + Sync>: Send + Sync {
    async fn process(&mut self, element: In) -> StreamResult<Vec<Out>>;
    async fn on_complete(&mut self) -> StreamResult<Vec<Out>>;
}

impl<In: Send + Sync + 'static, Out: Send + Sync + 'static> Flow<In, Out> {
    /// Create a flow from a FlowStage
    pub fn from_stage<S: FlowStage<In, Out> + 'static>(stage: S) -> Self {
        Self {
            inner: Box::new(FlowStageWrapper { stage }),
        }
    }

    /// Map each element
    pub fn map<F>(f: F) -> Self
    where
        F: FnMut(In) -> Out + Send + Sync + 'static,
    {
        Self::from_stage(MapStage { f })
    }

    /// Filter elements
    pub fn filter<F>(f: F) -> Flow<In, In>
    where
        F: FnMut(&In) -> bool + Send + Sync + 'static,
    {
        Flow::from_stage(FilterStage { f })
    }

    /// Take only the first N elements
    pub fn take(n: usize) -> Flow<In, In> {
        Flow::from_stage(TakeStage { remaining: n })
    }

    /// Drop the first N elements
    pub fn drop(n: usize) -> Flow<In, In> {
        Flow::from_stage(DropStage { remaining: n })
    }

    /// Batch elements into groups
    pub fn grouped(size: usize) -> Flow<In, Vec<In>> {
        Flow::from_stage(GroupedStage {
            buffer: Vec::with_capacity(size),
            size,
        })
    }

    /// Chain another flow
    pub fn via<Out2: Send + Sync + 'static>(self, next: Flow<Out, Out2>) -> Flow<In, Out2> {
        Flow {
            inner: Box::new(ChainedFlowStage {
                first: self.inner,
                second: next.inner,
            }),
        }
    }
}

// ============= Advanced Operators =============

impl<In: Send + Sync + 'static> Flow<In, In> {
    /// Deduplicate consecutive equal elements
    pub fn deduplicate() -> Self
    where
        In: Clone + PartialEq,
    {
        Self::from_stage(DeduplicateStage { last: None })
    }

    /// Deduplicate by key function
    pub fn deduplicate_by<K, F>(f: F) -> Self
    where
        K: PartialEq + Send + Sync + 'static,
        F: FnMut(&In) -> K + Send + Sync + 'static,
    {
        Self::from_stage(DeduplicateByStage {
            last_key: None,
            f,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Intersperse a separator between elements
    pub fn intersperse(separator: In) -> Self
    where
        In: Clone,
    {
        Self::from_stage(IntersperseStage {
            separator,
            first: true,
        })
    }

    /// Throttle: rate-limit elements (token bucket)
    pub fn throttle(interval: Duration, max_burst: usize) -> Self {
        Self::from_stage(ThrottleStage::new(interval, max_burst))
    }

    /// Buffer up to N elements before emitting
    pub fn buffer(capacity: usize) -> Self {
        Self::from_stage(BufferStage {
            buffer: VecDeque::with_capacity(capacity),
            capacity,
        })
    }

    /// Delay each element by a duration
    pub fn delay(duration: Duration) -> Self {
        Self::from_stage(DelayStage { duration })
    }

    /// Debounce: emit latest element after silence
    pub fn debounce(duration: Duration) -> Self {
        Self::from_stage(DebounceStage {
            duration,
            pending: None,
            last_seen: None,
        })
    }

    /// Sample: emit the latest element at intervals
    pub fn sample(interval: Duration) -> Self {
        Self::from_stage(SampleStage {
            interval,
            last_emit: Instant::now(),
            latest: None,
        })
    }
}

// ============= FlatMap / Scan / Window =============

impl<In: Send + Sync + 'static, Out: Send + Sync + 'static> Flow<In, Out> {
    /// FlatMap - map each element to a Vec and flatten
    pub fn flat_map<F>(f: F) -> Self
    where
        F: FnMut(In) -> Vec<Out> + Send + Sync + 'static,
    {
        Self::from_stage(FlatMapStage { f })
    }

    /// Stateful map - transform with persistent state
    pub fn stateful_map<State, F>(initial: State, f: F) -> Self
    where
        State: Send + Sync + 'static,
        F: FnMut(&mut State, In) -> Out + Send + Sync + 'static,
    {
        Self::from_stage(StatefulMapStage { state: initial, f })
    }

    /// Retry an operation up to max_retries times
    pub fn retry<F>(max_retries: usize, f: F) -> Self
    where
        In: Clone,
        F: FnMut(In) -> Result<Out, String> + Send + Sync + 'static,
    {
        Self::from_stage(RetryStage {
            f,
            max_retries,
            attempts: 0,
        })
    }

    /// Retry with exponential backoff
    pub fn retry_with_backoff<F>(
        max_retries: usize,
        backoff: ExponentialBackoff,
        f: F,
    ) -> Self
    where
        In: Clone,
        F: FnMut(In) -> Result<Out, String> + Send + Sync + 'static,
    {
        Self::from_stage(BackoffRetryStage {
            f,
            backoff,
            max_retries,
        })
    }
}

// Scan returns the State type
impl<In: Send + Sync + 'static, State: Clone + Send + Sync + 'static> Flow<In, State> {
    /// Scan - like fold but emits intermediate values
    pub fn scan<F>(initial: State, f: F) -> Self
    where
        F: FnMut(&mut State, In) + Send + Sync + 'static,
    {
        Self::from_stage(ScanStage { state: initial, f })
    }
}

// Sliding window returns Vec<In>
impl<In: Clone + Send + Sync + 'static> Flow<In, Vec<In>> {
    /// Sliding window of last N elements
    pub fn sliding_window(size: usize) -> Self {
        Self::from_stage(SlidingWindowStage {
            window: VecDeque::with_capacity(size),
            size,
        })
    }
}

// Enumerate returns (usize, In)
impl<In: Send + Sync + 'static> Flow<In, (usize, In)> {
    /// Add index to each element
    pub fn enumerate() -> Self {
        Self::from_stage(EnumerateStage { index: 0 })
    }
}

// ============= Partition / GroupBy / Window =============

impl<In: Send + Sync + 'static> Flow<In, (bool, In)> {
    /// Partition stream by predicate — emits (true/false, element) pairs
    pub fn partition<F>(f: F) -> Self
    where
        F: FnMut(&In) -> bool + Send + Sync + 'static,
    {
        Self::from_stage(PartitionStage { f })
    }
}

impl<In: Send + Sync + 'static, K: Clone + Send + Sync + 'static> Flow<In, (K, In)> {
    /// Tag each element with a key — emits (key, element) pairs
    pub fn group_by<F>(f: F) -> Self
    where
        F: FnMut(&In) -> K + Send + Sync + 'static,
    {
        Self::from_stage(GroupByStage {
            f,
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<In: Send + Sync + 'static, K: Clone + Eq + std::hash::Hash + Send + Sync + 'static> Flow<In, (K, Vec<In>)> {
    /// Collect all elements by key, emit grouped results on complete
    pub fn collect_by_key<F>(f: F) -> Self
    where
        F: FnMut(&In) -> K + Send + Sync + 'static,
    {
        Self::from_stage(CollectByKeyStage {
            f,
            groups: std::collections::HashMap::new(),
        })
    }
}

impl<In: Clone + Send + Sync + 'static> Flow<In, Vec<In>> {
    /// Time-based window: collect elements for a duration, then emit as batch
    pub fn time_window(duration: Duration) -> Self {
        Self::from_stage(TimeWindowStage {
            duration,
            buffer: Vec::new(),
            window_start: None,
        })
    }

    /// Count-based tumbling window: emit every N elements
    pub fn count_window(size: usize) -> Self {
        Self::from_stage(CountWindowStage {
            size,
            stride: size,
            buffer: VecDeque::new(),
        })
    }

    /// Count-based sliding window with custom stride
    pub fn count_window_with_stride(size: usize, stride: usize) -> Self {
        Self::from_stage(CountWindowStage {
            size,
            stride,
            buffer: VecDeque::new(),
        })
    }
}

impl<In: Send + Sync + 'static> Flow<Vec<In>, In> {
    /// Flatten nested collections
    pub fn flatten() -> Self {
        Self::from_stage(FlattenStage)
    }
}

// ============= Flow Stage Wrapper =============

struct FlowStageWrapper<S> {
    stage: S,
}

#[async_trait]
impl<In: Send + Sync + 'static, Out: Send + Sync + 'static, S: FlowStage<In, Out> + 'static> FlowStageBoxed<In, Out>
    for FlowStageWrapper<S>
{
    async fn process(&mut self, element: In) -> StreamResult<Vec<Out>> {
        self.stage.process(element).await
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<Out>> {
        self.stage.on_complete().await
    }
}

// ============= Flow Stage Implementations =============

/// Map stage
struct MapStage<F> {
    f: F,
}

#[async_trait]
impl<In: Send + Sync + 'static, Out: Send + Sync + 'static, F: FnMut(In) -> Out + Send + Sync> FlowStage<In, Out> for MapStage<F> {
    async fn process(&mut self, element: In) -> StreamResult<Vec<Out>> {
        Ok(vec![(self.f)(element)])
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<Out>> {
        Ok(vec![])
    }
}

/// Filter stage
struct FilterStage<F> {
    f: F,
}

#[async_trait]
impl<T: Send + Sync + 'static, F: FnMut(&T) -> bool + Send + Sync + 'static> FlowStage<T, T> for FilterStage<F> {
    async fn process(&mut self, element: T) -> StreamResult<Vec<T>> {
        if (self.f)(&element) {
            Ok(vec![element])
        } else {
            Ok(vec![])
        }
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<T>> {
        Ok(vec![])
    }
}

/// Take stage
struct TakeStage {
    remaining: usize,
}

#[async_trait]
impl<T: Send + Sync + 'static> FlowStage<T, T> for TakeStage {
    async fn process(&mut self, element: T) -> StreamResult<Vec<T>> {
        if self.remaining > 0 {
            self.remaining -= 1;
            Ok(vec![element])
        } else {
            Ok(vec![])
        }
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<T>> {
        Ok(vec![])
    }
}

/// Drop stage
struct DropStage {
    remaining: usize,
}

#[async_trait]
impl<T: Send + Sync + 'static> FlowStage<T, T> for DropStage {
    async fn process(&mut self, element: T) -> StreamResult<Vec<T>> {
        if self.remaining > 0 {
            self.remaining -= 1;
            Ok(vec![])
        } else {
            Ok(vec![element])
        }
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<T>> {
        Ok(vec![])
    }
}

/// Grouped stage
struct GroupedStage<T> {
    buffer: Vec<T>,
    size: usize,
}

#[async_trait]
impl<T: Send + Sync + 'static> FlowStage<T, Vec<T>> for GroupedStage<T> {
    async fn process(&mut self, element: T) -> StreamResult<Vec<Vec<T>>> {
        self.buffer.push(element);

        if self.buffer.len() >= self.size {
            let group = std::mem::replace(&mut self.buffer, Vec::with_capacity(self.size));
            Ok(vec![group])
        } else {
            Ok(vec![])
        }
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<Vec<T>>> {
        if self.buffer.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![std::mem::take(&mut self.buffer)])
        }
    }
}

/// Chained flow stage
struct ChainedFlowStage<In: Send + Sync, Mid: Send + Sync, Out: Send + Sync> {
    first: Box<dyn FlowStageBoxed<In, Mid>>,
    second: Box<dyn FlowStageBoxed<Mid, Out>>,
}

#[async_trait]
impl<In: Send + Sync + 'static, Mid: Send + Sync + 'static, Out: Send + Sync + 'static> FlowStageBoxed<In, Out>
    for ChainedFlowStage<In, Mid, Out>
{
    async fn process(&mut self, element: In) -> StreamResult<Vec<Out>> {
        let mid_results = self.first.process(element).await?;
        let mut out_results = Vec::new();

        for mid in mid_results {
            out_results.extend(self.second.process(mid).await?);
        }

        Ok(out_results)
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<Out>> {
        let mid_results = self.first.on_complete().await?;
        let mut out_results = Vec::new();

        for mid in mid_results {
            out_results.extend(self.second.process(mid).await?);
        }

        out_results.extend(self.second.on_complete().await?);
        Ok(out_results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Source;

    #[tokio::test]
    async fn test_flow_map() {
        let result = Source::from_vec(vec![1, 2, 3])
            .via(Flow::map(|x| x * 10))
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![10, 20, 30]);
    }

    #[tokio::test]
    async fn test_flow_filter() {
        let result = Source::from_vec(vec![1, 2, 3, 4, 5])
            .via(Flow::<i32, i32>::filter(|x| x % 2 == 1))
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 3, 5]);
    }

    #[tokio::test]
    async fn test_flow_grouped() {
        let result = Source::from_vec(vec![1, 2, 3, 4, 5])
            .via(Flow::<i32, Vec<i32>>::grouped(2))
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![vec![1, 2], vec![3, 4], vec![5]]);
    }

    #[tokio::test]
    async fn test_flow_chain() {
        let flow = Flow::map(|x: i32| x * 2).via(Flow::<i32, i32>::filter(|x| *x > 5));
        let result = Source::from_vec(vec![1, 2, 3, 4, 5])
            .via(flow)
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![6, 8, 10]);
    }

    #[tokio::test]
    async fn test_flow_flat_map() {
        let result = Source::from_vec(vec![1, 2, 3])
            .via(Flow::flat_map(|x| vec![x, x * 100]))
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 100, 2, 200, 3, 300]);
    }

    #[tokio::test]
    async fn test_flow_scan() {
        let result = Source::from_vec(vec![1, 2, 3])
            .via(Flow::scan(0i32, |acc, x: i32| { *acc += x; }))
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 3, 6]);
    }

    #[tokio::test]
    async fn test_flow_enumerate() {
        let result = Source::from_vec(vec!["x", "y", "z"])
            .via(Flow::enumerate())
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![(0, "x"), (1, "y"), (2, "z")]);
    }

    #[tokio::test]
    async fn test_flow_deduplicate() {
        let result = Source::from_vec(vec![1, 1, 2, 3, 3, 3, 4])
            .via(Flow::deduplicate())
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_flow_intersperse() {
        let result = Source::from_vec(vec![10, 20, 30])
            .via(Flow::intersperse(0))
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![10, 0, 20, 0, 30]);
    }
}

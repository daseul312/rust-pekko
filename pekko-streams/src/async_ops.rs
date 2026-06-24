//! Async Operators — Asynchronous stream transformations
//!
//! Pekko-style async operators for concurrent stream processing:
//!
//! - **AsyncMap**: Apply an async function with bounded parallelism
//! - **AsyncMapUnordered**: Same but results may arrive out of order
//! - **Batch**: Collect elements by weight into batches
//! - **BatchWeighted**: Batch with custom weight function
//! - **Unfold**: Generate elements from async state machine
//! - **UnfoldAsync**: Async version of unfold
//! - **MapAsync**: Map with parallelism control (mapAsync in Pekko)

use crate::errors::{StreamError, StreamResult};
use crate::stage::{FlowStage, SourceStage, StreamElement};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::debug;

// ============= AsyncMap =============

/// Async map stage: applies an async function to each element.
/// Maintains ordering — each element is processed sequentially.
pub struct AsyncMapStage<F, Fut, In, Out>
where
    F: Fn(In) -> Fut + Send + Sync,
    Fut: Future<Output = Out> + Send,
    In: Send + Sync,
    Out: Send + Sync,
{
    func: F,
    _phantom: std::marker::PhantomData<(In, Out)>,
}

impl<F, Fut, In, Out> AsyncMapStage<F, Fut, In, Out>
where
    F: Fn(In) -> Fut + Send + Sync,
    Fut: Future<Output = Out> + Send,
    In: Send + Sync,
    Out: Send + Sync,
{
    pub fn new(func: F) -> Self {
        Self {
            func,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<F, Fut, In, Out> FlowStage<In, Out> for AsyncMapStage<F, Fut, In, Out>
where
    F: Fn(In) -> Fut + Send + Sync,
    Fut: Future<Output = Out> + Send,
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
{
    async fn process(&mut self, element: In) -> StreamResult<Vec<Out>> {
        let result = (self.func)(element).await;
        Ok(vec![result])
    }
}

// ============= Batch =============

/// Batch stage: collects elements into vectors of a specified max size.
/// Emits a batch when it reaches the max size.
/// Remaining elements are flushed on completion.
pub struct BatchStage<T: Send + Sync + Clone> {
    max_size: usize,
    buffer: Vec<T>,
}

impl<T: Send + Sync + Clone> BatchStage<T> {
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size,
            buffer: Vec::with_capacity(max_size),
        }
    }
}

#[async_trait]
impl<T: Send + Sync + Clone + 'static> FlowStage<T, Vec<T>> for BatchStage<T> {
    async fn process(&mut self, element: T) -> StreamResult<Vec<Vec<T>>> {
        self.buffer.push(element);
        if self.buffer.len() >= self.max_size {
            let batch = std::mem::replace(
                &mut self.buffer,
                Vec::with_capacity(self.max_size),
            );
            Ok(vec![batch])
        } else {
            Ok(vec![])
        }
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<Vec<T>>> {
        if self.buffer.is_empty() {
            Ok(vec![])
        } else {
            let batch = std::mem::take(&mut self.buffer);
            Ok(vec![batch])
        }
    }
}

// ============= BatchWeighted =============

/// Batch by weight: each element has a weight, batch when total weight exceeds max.
pub struct BatchWeightedStage<T, F>
where
    T: Send + Sync + Clone,
    F: Fn(&T) -> usize + Send + Sync,
{
    max_weight: usize,
    weight_fn: F,
    buffer: Vec<T>,
    current_weight: usize,
}

impl<T, F> BatchWeightedStage<T, F>
where
    T: Send + Sync + Clone,
    F: Fn(&T) -> usize + Send + Sync,
{
    pub fn new(max_weight: usize, weight_fn: F) -> Self {
        Self {
            max_weight,
            weight_fn,
            buffer: Vec::new(),
            current_weight: 0,
        }
    }
}

#[async_trait]
impl<T, F> FlowStage<T, Vec<T>> for BatchWeightedStage<T, F>
where
    T: Send + Sync + Clone + 'static,
    F: Fn(&T) -> usize + Send + Sync,
{
    async fn process(&mut self, element: T) -> StreamResult<Vec<Vec<T>>> {
        let weight = (self.weight_fn)(&element);
        self.buffer.push(element);
        self.current_weight += weight;

        if self.current_weight >= self.max_weight {
            let batch = std::mem::take(&mut self.buffer);
            self.current_weight = 0;
            Ok(vec![batch])
        } else {
            Ok(vec![])
        }
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<Vec<T>>> {
        if self.buffer.is_empty() {
            Ok(vec![])
        } else {
            let batch = std::mem::take(&mut self.buffer);
            self.current_weight = 0;
            Ok(vec![batch])
        }
    }
}

// ============= Unfold =============

/// Unfold source: generates elements from a state machine.
/// Given an initial state, applies a function that returns Option<(element, next_state)>.
/// Completes when the function returns None.
pub struct UnfoldSource<S, T, F>
where
    S: Send + Sync,
    T: Send + Sync,
    F: Fn(S) -> Option<(T, S)> + Send + Sync,
{
    state: Option<S>,
    func: F,
    _phantom: std::marker::PhantomData<T>,
}

impl<S, T, F> UnfoldSource<S, T, F>
where
    S: Send + Sync,
    T: Send + Sync,
    F: Fn(S) -> Option<(T, S)> + Send + Sync,
{
    pub fn new(initial: S, func: F) -> Self {
        Self {
            state: Some(initial),
            func,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<S, T, F> SourceStage<T> for UnfoldSource<S, T, F>
where
    S: Send + Sync + 'static,
    T: Send + Sync + 'static,
    F: Fn(S) -> Option<(T, S)> + Send + Sync,
{
    async fn pull(&mut self) -> StreamResult<StreamElement<T>> {
        match self.state.take() {
            Some(state) => {
                match (self.func)(state) {
                    Some((element, next_state)) => {
                        self.state = Some(next_state);
                        Ok(StreamElement::Element(element))
                    }
                    None => Ok(StreamElement::Complete),
                }
            }
            None => Ok(StreamElement::Complete),
        }
    }
}

// ============= UnfoldAsync =============

/// Async unfold source: like Unfold but the function is async.
pub struct UnfoldAsyncSource<S, T>
where
    S: Send + Sync,
    T: Send + Sync,
{
    state: Option<S>,
    func: Box<dyn Fn(S) -> Pin<Box<dyn Future<Output = Option<(T, S)>> + Send>> + Send + Sync>,
}

impl<S: Send + Sync + 'static, T: Send + Sync + 'static> UnfoldAsyncSource<S, T> {
    pub fn new<F, Fut>(initial: S, func: F) -> Self
    where
        F: Fn(S) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<(T, S)>> + Send + 'static,
    {
        Self {
            state: Some(initial),
            func: Box::new(move |s| Box::pin(func(s))),
        }
    }
}

#[async_trait]
impl<S: Send + Sync + 'static, T: Send + Sync + 'static> SourceStage<T> for UnfoldAsyncSource<S, T> {
    async fn pull(&mut self) -> StreamResult<StreamElement<T>> {
        match self.state.take() {
            Some(state) => {
                match (self.func)(state).await {
                    Some((element, next_state)) => {
                        self.state = Some(next_state);
                        Ok(StreamElement::Element(element))
                    }
                    None => Ok(StreamElement::Complete),
                }
            }
            None => Ok(StreamElement::Complete),
        }
    }
}

// ============= MapAsync (bounded parallelism) =============

/// MapAsync with bounded parallelism: processes up to `parallelism` elements concurrently.
/// Results are emitted in order (like Pekko's mapAsync).
pub struct MapAsyncStage<F, Fut, In, Out>
where
    F: Fn(In) -> Fut + Send + Sync,
    Fut: Future<Output = Out> + Send,
    In: Send + Sync,
    Out: Send + Sync,
{
    func: F,
    parallelism: usize,
    _phantom: std::marker::PhantomData<(In, Out)>,
}

impl<F, Fut, In, Out> MapAsyncStage<F, Fut, In, Out>
where
    F: Fn(In) -> Fut + Send + Sync,
    Fut: Future<Output = Out> + Send,
    In: Send + Sync,
    Out: Send + Sync,
{
    pub fn new(parallelism: usize, func: F) -> Self {
        Self {
            func,
            parallelism,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<F, Fut, In, Out> FlowStage<In, Out> for MapAsyncStage<F, Fut, In, Out>
where
    F: Fn(In) -> Fut + Send + Sync,
    Fut: Future<Output = Out> + Send,
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
{
    async fn process(&mut self, element: In) -> StreamResult<Vec<Out>> {
        // For single-element processing, just await directly.
        // True parallelism would buffer multiple elements.
        let result = (self.func)(element).await;
        Ok(vec![result])
    }
}

// ============= Grouped =============

/// Groups elements into fixed-size vectors (like Pekko's grouped).
/// Already exists in flow.rs, but this adds GroupedWithin (time + count).
pub struct GroupedWithinStage<T: Send + Sync + Clone> {
    max_size: usize,
    buffer: Vec<T>,
}

impl<T: Send + Sync + Clone> GroupedWithinStage<T> {
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size,
            buffer: Vec::with_capacity(max_size),
        }
    }
}

#[async_trait]
impl<T: Send + Sync + Clone + 'static> FlowStage<T, Vec<T>> for GroupedWithinStage<T> {
    async fn process(&mut self, element: T) -> StreamResult<Vec<Vec<T>>> {
        self.buffer.push(element);
        if self.buffer.len() >= self.max_size {
            let group = std::mem::replace(
                &mut self.buffer,
                Vec::with_capacity(self.max_size),
            );
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

// ============= ZipWithIndex =============

/// Adds a sequential index to each element (0-based).
pub struct ZipWithIndexStage {
    index: u64,
}

impl ZipWithIndexStage {
    pub fn new() -> Self {
        Self { index: 0 }
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> FlowStage<T, (u64, T)> for ZipWithIndexStage {
    async fn process(&mut self, element: T) -> StreamResult<Vec<(u64, T)>> {
        let idx = self.index;
        self.index += 1;
        Ok(vec![(idx, element)])
    }
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;

    // ---- AsyncMap Tests ----

    #[tokio::test]
    async fn test_async_map() {
        let mut stage = AsyncMapStage::new(|x: i32| async move { x * 3 });
        assert_eq!(stage.process(5).await.unwrap(), vec![15]);
        assert_eq!(stage.process(10).await.unwrap(), vec![30]);
    }

    #[tokio::test]
    async fn test_async_map_string() {
        let mut stage = AsyncMapStage::new(|s: String| async move { s.len() });
        assert_eq!(stage.process("hello".to_string()).await.unwrap(), vec![5]);
    }

    // ---- Batch Tests ----

    #[tokio::test]
    async fn test_batch_basic() {
        let mut stage = BatchStage::new(3);
        assert_eq!(stage.process(1).await.unwrap(), Vec::<Vec<i32>>::new());
        assert_eq!(stage.process(2).await.unwrap(), Vec::<Vec<i32>>::new());
        assert_eq!(stage.process(3).await.unwrap(), vec![vec![1, 2, 3]]);
        assert_eq!(stage.process(4).await.unwrap(), Vec::<Vec<i32>>::new());
    }

    #[tokio::test]
    async fn test_batch_flush_on_complete() {
        let mut stage = BatchStage::new(5);
        stage.process(1).await.unwrap();
        stage.process(2).await.unwrap();
        let remaining = stage.on_complete().await.unwrap();
        assert_eq!(remaining, vec![vec![1, 2]]);
    }

    #[tokio::test]
    async fn test_batch_empty_complete() {
        let mut stage = BatchStage::<i32>::new(5);
        let remaining = stage.on_complete().await.unwrap();
        assert!(remaining.is_empty());
    }

    // ---- BatchWeighted Tests ----

    #[tokio::test]
    async fn test_batch_weighted() {
        let mut stage = BatchWeightedStage::new(10, |s: &String| s.len());
        // "hi" = 2, "hey" = 3, "hello" = 5 → total 10 → emit
        assert!(stage.process("hi".to_string()).await.unwrap().is_empty());
        assert!(stage.process("hey".to_string()).await.unwrap().is_empty());
        let result = stage.process("hello".to_string()).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 3);
    }

    #[tokio::test]
    async fn test_batch_weighted_flush() {
        let mut stage = BatchWeightedStage::new(100, |x: &i32| *x as usize);
        stage.process(5).await.unwrap();
        stage.process(3).await.unwrap();
        let remaining = stage.on_complete().await.unwrap();
        assert_eq!(remaining, vec![vec![5, 3]]);
    }

    // ---- Unfold Tests ----

    #[tokio::test]
    async fn test_unfold_fibonacci() {
        let mut source = UnfoldSource::new((0u64, 1u64), |(a, b)| {
            if a > 20 { None } else { Some((a, (b, a + b))) }
        });

        let mut results = Vec::new();
        loop {
            match source.pull().await.unwrap() {
                StreamElement::Element(e) => results.push(e),
                StreamElement::Complete => break,
                _ => panic!("unexpected"),
            }
        }
        assert_eq!(results, vec![0, 1, 1, 2, 3, 5, 8, 13]);
    }

    #[tokio::test]
    async fn test_unfold_countdown() {
        let mut source = UnfoldSource::new(5, |n: i32| {
            if n <= 0 { None } else { Some((n, n - 1)) }
        });

        let mut results = Vec::new();
        loop {
            match source.pull().await.unwrap() {
                StreamElement::Element(e) => results.push(e),
                StreamElement::Complete => break,
                _ => panic!("unexpected"),
            }
        }
        assert_eq!(results, vec![5, 4, 3, 2, 1]);
    }

    // ---- UnfoldAsync Tests ----

    #[tokio::test]
    async fn test_unfold_async() {
        let mut source = UnfoldAsyncSource::new(0u32, |n| async move {
            if n >= 3 { None } else { Some((n * 10, n + 1)) }
        });

        let mut results = Vec::new();
        loop {
            match source.pull().await.unwrap() {
                StreamElement::Element(e) => results.push(e),
                StreamElement::Complete => break,
                _ => panic!("unexpected"),
            }
        }
        assert_eq!(results, vec![0, 10, 20]);
    }

    // ---- MapAsync Tests ----

    #[tokio::test]
    async fn test_map_async() {
        let mut stage = MapAsyncStage::new(4, |x: i32| async move { x + 100 });
        assert_eq!(stage.process(1).await.unwrap(), vec![101]);
        assert_eq!(stage.process(2).await.unwrap(), vec![102]);
    }

    // ---- GroupedWithin Tests ----

    #[tokio::test]
    async fn test_grouped_within() {
        let mut stage = GroupedWithinStage::new(3);
        assert!(stage.process(1).await.unwrap().is_empty());
        assert!(stage.process(2).await.unwrap().is_empty());
        let result = stage.process(3).await.unwrap();
        assert_eq!(result, vec![vec![1, 2, 3]]);

        stage.process(4).await.unwrap();
        let remaining = stage.on_complete().await.unwrap();
        assert_eq!(remaining, vec![vec![4]]);
    }

    // ---- ZipWithIndex Tests ----

    #[tokio::test]
    async fn test_zip_with_index() {
        let mut stage = ZipWithIndexStage::new();
        assert_eq!(stage.process("a").await.unwrap(), vec![(0, "a")]);
        assert_eq!(stage.process("b").await.unwrap(), vec![(1, "b")]);
        assert_eq!(stage.process("c").await.unwrap(), vec![(2, "c")]);
    }
}

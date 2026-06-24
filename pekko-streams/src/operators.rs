//! Advanced stream operators
//!
//! Provides higher-level operators for stream transformation including
//! flat_map, scan, sliding window, deduplication, and stateful operations.

use crate::{
    errors::StreamResult,
    stage::FlowStage,
};
use async_trait::async_trait;
use std::collections::VecDeque;

// ============= FlatMap / MapConcat =============

/// FlatMap stage - maps each element to a collection and flattens
pub(crate) struct FlatMapStage<F> {
    pub f: F,
}

#[async_trait]
impl<In, Out, F> FlowStage<In, Out> for FlatMapStage<F>
where
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
    F: FnMut(In) -> Vec<Out> + Send + Sync + 'static,
{
    async fn process(&mut self, element: In) -> StreamResult<Vec<Out>> {
        Ok((self.f)(element))
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<Out>> {
        Ok(vec![])
    }
}

// ============= Scan =============

/// Scan stage - like fold but emits each intermediate accumulator value
pub(crate) struct ScanStage<State, F> {
    pub state: State,
    pub f: F,
}

#[async_trait]
impl<In, State, F> FlowStage<In, State> for ScanStage<State, F>
where
    In: Send + Sync + 'static,
    State: Clone + Send + Sync + 'static,
    F: FnMut(&mut State, In) + Send + Sync + 'static,
{
    async fn process(&mut self, element: In) -> StreamResult<Vec<State>> {
        (self.f)(&mut self.state, element);
        Ok(vec![self.state.clone()])
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<State>> {
        Ok(vec![])
    }
}

// ============= Sliding Window =============

/// Sliding window stage - emits a window of the last N elements
pub(crate) struct SlidingWindowStage<T> {
    pub window: VecDeque<T>,
    pub size: usize,
}

#[async_trait]
impl<T: Clone + Send + Sync + 'static> FlowStage<T, Vec<T>> for SlidingWindowStage<T> {
    async fn process(&mut self, element: T) -> StreamResult<Vec<Vec<T>>> {
        self.window.push_back(element);
        if self.window.len() > self.size {
            self.window.pop_front();
        }
        Ok(vec![self.window.iter().cloned().collect()])
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<Vec<T>>> {
        Ok(vec![])
    }
}

// ============= Deduplicate =============

/// Deduplicate stage - removes consecutive duplicates
pub(crate) struct DeduplicateStage<T> {
    pub last: Option<T>,
}

#[async_trait]
impl<T: Clone + PartialEq + Send + Sync + 'static> FlowStage<T, T> for DeduplicateStage<T> {
    async fn process(&mut self, element: T) -> StreamResult<Vec<T>> {
        let emit = match &self.last {
            Some(prev) if *prev == element => false,
            _ => true,
        };
        self.last = Some(element.clone());
        if emit {
            Ok(vec![element])
        } else {
            Ok(vec![])
        }
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<T>> {
        Ok(vec![])
    }
}

/// Deduplicate by key stage
pub(crate) struct DeduplicateByStage<T, K, F> {
    pub last_key: Option<K>,
    pub f: F,
    pub _phantom: std::marker::PhantomData<T>,
}

#[async_trait]
impl<T, K, F> FlowStage<T, T> for DeduplicateByStage<T, K, F>
where
    T: Send + Sync + 'static,
    K: PartialEq + Send + Sync + 'static,
    F: FnMut(&T) -> K + Send + Sync + 'static,
{
    async fn process(&mut self, element: T) -> StreamResult<Vec<T>> {
        let key = (self.f)(&element);
        let emit = match &self.last_key {
            Some(prev) if *prev == key => false,
            _ => true,
        };
        self.last_key = Some(key);
        if emit {
            Ok(vec![element])
        } else {
            Ok(vec![])
        }
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<T>> {
        Ok(vec![])
    }
}

// ============= Intersperse =============

/// Intersperse stage - inserts separator between elements
pub(crate) struct IntersperseStage<T> {
    pub separator: T,
    pub first: bool,
}

#[async_trait]
impl<T: Clone + Send + Sync + 'static> FlowStage<T, T> for IntersperseStage<T> {
    async fn process(&mut self, element: T) -> StreamResult<Vec<T>> {
        if self.first {
            self.first = false;
            Ok(vec![element])
        } else {
            Ok(vec![self.separator.clone(), element])
        }
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<T>> {
        Ok(vec![])
    }
}

// ============= StatefulMap =============

/// Stateful map stage - maintains state across transformations
pub(crate) struct StatefulMapStage<State, F> {
    pub state: State,
    pub f: F,
}

#[async_trait]
impl<In, Out, State, F> FlowStage<In, Out> for StatefulMapStage<State, F>
where
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
    State: Send + Sync + 'static,
    F: FnMut(&mut State, In) -> Out + Send + Sync + 'static,
{
    async fn process(&mut self, element: In) -> StreamResult<Vec<Out>> {
        let result = (self.f)(&mut self.state, element);
        Ok(vec![result])
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<Out>> {
        Ok(vec![])
    }
}

// ============= Enumerate =============

/// Enumerate stage - adds index to each element
pub(crate) struct EnumerateStage {
    pub index: usize,
}

#[async_trait]
impl<T: Send + Sync + 'static> FlowStage<T, (usize, T)> for EnumerateStage {
    async fn process(&mut self, element: T) -> StreamResult<Vec<(usize, T)>> {
        let i = self.index;
        self.index += 1;
        Ok(vec![(i, element)])
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<(usize, T)>> {
        Ok(vec![])
    }
}

// ============= Partition =============

/// Partition stage - split stream by predicate into (true, false) outputs
pub(crate) struct PartitionStage<F> {
    pub f: F,
}

#[async_trait]
impl<T, F> FlowStage<T, (bool, T)> for PartitionStage<F>
where
    T: Send + Sync + 'static,
    F: FnMut(&T) -> bool + Send + Sync + 'static,
{
    async fn process(&mut self, element: T) -> StreamResult<Vec<(bool, T)>> {
        let matched = (self.f)(&element);
        Ok(vec![(matched, element)])
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<(bool, T)>> {
        Ok(vec![])
    }
}

// ============= GroupBy =============

/// GroupBy stage - group elements by key into (K, T) pairs
pub(crate) struct GroupByStage<K, F> {
    pub f: F,
    pub _phantom: std::marker::PhantomData<K>,
}

#[async_trait]
impl<T, K, F> FlowStage<T, (K, T)> for GroupByStage<K, F>
where
    T: Send + Sync + 'static,
    K: Clone + Send + Sync + 'static,
    F: FnMut(&T) -> K + Send + Sync + 'static,
{
    async fn process(&mut self, element: T) -> StreamResult<Vec<(K, T)>> {
        let key = (self.f)(&element);
        Ok(vec![(key, element)])
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<(K, T)>> {
        Ok(vec![])
    }
}

// ============= CollectByKey =============

/// CollectByKey stage - accumulate elements by key, emit groups on complete
pub(crate) struct CollectByKeyStage<K, V, F> {
    pub f: F,
    pub groups: std::collections::HashMap<K, Vec<V>>,
}

#[async_trait]
impl<T, K, F> FlowStage<T, (K, Vec<T>)> for CollectByKeyStage<K, T, F>
where
    T: Send + Sync + 'static,
    K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
    F: FnMut(&T) -> K + Send + Sync + 'static,
{
    async fn process(&mut self, element: T) -> StreamResult<Vec<(K, Vec<T>)>> {
        let key = (self.f)(&element);
        self.groups.entry(key).or_default().push(element);
        Ok(vec![])
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<(K, Vec<T>)>> {
        let groups = std::mem::take(&mut self.groups);
        Ok(groups.into_iter().collect())
    }
}

// ============= TimeWindow =============

/// TimeWindow stage - collect elements within a duration window
pub(crate) struct TimeWindowStage<T> {
    pub duration: std::time::Duration,
    pub buffer: Vec<T>,
    pub window_start: Option<std::time::Instant>,
}

#[async_trait]
impl<T: Send + Sync + 'static> FlowStage<T, Vec<T>> for TimeWindowStage<T> {
    async fn process(&mut self, element: T) -> StreamResult<Vec<Vec<T>>> {
        let now = std::time::Instant::now();

        if self.window_start.is_none() {
            self.window_start = Some(now);
        }

        self.buffer.push(element);

        // Check if window duration has elapsed
        if now.duration_since(self.window_start.unwrap()) >= self.duration {
            let batch = std::mem::take(&mut self.buffer);
            self.window_start = Some(now);
            Ok(vec![batch])
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

// ============= CountWindow =============

/// CountWindow stage - collect exactly N elements per window
pub(crate) struct CountWindowStage<T> {
    pub size: usize,
    pub stride: usize,
    pub buffer: VecDeque<T>,
}

#[async_trait]
impl<T: Clone + Send + Sync + 'static> FlowStage<T, Vec<T>> for CountWindowStage<T> {
    async fn process(&mut self, element: T) -> StreamResult<Vec<Vec<T>>> {
        self.buffer.push_back(element);

        if self.buffer.len() >= self.size {
            let window: Vec<T> = self.buffer.iter().cloned().collect();
            // Slide by stride
            for _ in 0..self.stride.min(self.buffer.len()) {
                self.buffer.pop_front();
            }
            Ok(vec![window])
        } else {
            Ok(vec![])
        }
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<Vec<T>>> {
        // Emit remaining buffer if non-empty
        if self.buffer.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![self.buffer.drain(..).collect()])
        }
    }
}

// ============= Flatten =============

/// Flatten stage - flatten nested collections
pub(crate) struct FlattenStage;

#[async_trait]
impl<T: Send + Sync + 'static> FlowStage<Vec<T>, T> for FlattenStage {
    async fn process(&mut self, element: Vec<T>) -> StreamResult<Vec<T>> {
        Ok(element)
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<T>> {
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use crate::{Source, Flow};

    #[tokio::test]
    async fn test_flat_map() {
        let result = Source::from_vec(vec![1, 2, 3])
            .via(Flow::flat_map(|x| vec![x, x * 10]))
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 10, 2, 20, 3, 30]);
    }

    #[tokio::test]
    async fn test_scan() {
        let result = Source::from_vec(vec![1, 2, 3, 4])
            .via(Flow::scan(0i32, |acc, x: i32| { *acc += x; }))
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 3, 6, 10]);
    }

    #[tokio::test]
    async fn test_sliding_window() {
        let result = Source::from_vec(vec![1, 2, 3, 4, 5])
            .via(Flow::sliding_window(3))
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![
            vec![1],
            vec![1, 2],
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![3, 4, 5],
        ]);
    }

    #[tokio::test]
    async fn test_deduplicate() {
        let result = Source::from_vec(vec![1, 1, 2, 2, 2, 3, 1, 1])
            .via(Flow::deduplicate())
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3, 1]);
    }

    #[tokio::test]
    async fn test_intersperse() {
        let result = Source::from_vec(vec![1, 2, 3])
            .via(Flow::intersperse(0))
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 0, 2, 0, 3]);
    }

    #[tokio::test]
    async fn test_stateful_map() {
        // Running sum via stateful_map
        let result = Source::from_vec(vec![1, 2, 3, 4])
            .via(Flow::stateful_map(0i32, |sum, x: i32| {
                *sum += x;
                *sum
            }))
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 3, 6, 10]);
    }

    #[tokio::test]
    async fn test_enumerate() {
        let result = Source::from_vec(vec!["a", "b", "c"])
            .via(Flow::enumerate())
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![(0, "a"), (1, "b"), (2, "c")]);
    }

    #[tokio::test]
    async fn test_partition() {
        let result = Source::from_vec(vec![1, 2, 3, 4, 5])
            .partition(|x| x % 2 == 0)
            .run_collect()
            .await
            .unwrap();
        // (false, 1), (true, 2), (false, 3), (true, 4), (false, 5)
        let evens: Vec<i32> = result.iter().filter(|(b, _)| *b).map(|(_, v)| *v).collect();
        let odds: Vec<i32> = result.iter().filter(|(b, _)| !*b).map(|(_, v)| *v).collect();
        assert_eq!(evens, vec![2, 4]);
        assert_eq!(odds, vec![1, 3, 5]);
    }

    #[tokio::test]
    async fn test_collect_by_key() {
        let mut result = Source::from_vec(vec![1, 2, 3, 4, 5, 6])
            .collect_by_key(|x| if x % 2 == 0 { "even" } else { "odd" })
            .run_collect()
            .await
            .unwrap();
        result.sort_by_key(|(k, _)| k.to_string());
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, "even");
        assert_eq!(result[0].1, vec![2, 4, 6]);
        assert_eq!(result[1].0, "odd");
        assert_eq!(result[1].1, vec![1, 3, 5]);
    }

    #[tokio::test]
    async fn test_count_window() {
        let result = Source::from_vec(vec![1, 2, 3, 4, 5])
            .count_window(2)
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![vec![1, 2], vec![3, 4], vec![5]]);
    }

    #[tokio::test]
    async fn test_count_window_sliding() {
        let result = Source::from_vec(vec![1, 2, 3, 4, 5])
            .via(Flow::count_window_with_stride(3, 1))
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![3, 4, 5],
            vec![4, 5],
        ]);
    }

    #[tokio::test]
    async fn test_flatten() {
        let result = Source::from_vec(vec![vec![1, 2], vec![3], vec![4, 5, 6]])
            .via(Flow::flatten())
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_group_by() {
        let result = Source::from_vec(vec![1, 2, 3, 4])
            .group_by(|x| if x % 2 == 0 { "even" } else { "odd" })
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![
            ("odd", 1), ("even", 2), ("odd", 3), ("even", 4)
        ]);
    }

    #[tokio::test]
    async fn test_flat_map_empty() {
        let result = Source::from_vec(vec![1, 2, 3])
            .via(Flow::flat_map(|x: i32| {
                if x % 2 == 0 { vec![x] } else { vec![] }
            }))
            .run_collect()
            .await
            .unwrap();
        assert_eq!(result, vec![2]);
    }
}

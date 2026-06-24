//! SubFlow - Sub-stream processing after groupBy
//!
//! Implements Scala Pekko's SubFlow concept: after grouping a stream by key,
//! apply transformations to each sub-stream independently, then merge results.
//!
//! ## Example
//! ```ignore
//! let result = Source::from_vec(vec![1, 2, 3, 4, 5, 6])
//!     .sub_flow_by(|x| x % 2)       // group by even/odd
//!     .map(|x| x * 10)               // transform each sub-stream
//!     .merge_substreams()             // merge back
//!     .run_collect()
//!     .await?;
//! ```

use crate::errors::StreamResult;
use crate::source::Source;
use crate::stage::{SourceStage, StreamElement};
use async_trait::async_trait;
use std::collections::HashMap;
use std::hash::Hash;

/// A sub-flow that processes elements grouped by key
///
/// Each key gets its own independent stream of transformations.
pub struct SubFlow<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    source: Box<dyn SourceStage<V>>,
    key_fn: Box<dyn FnMut(&V) -> K + Send + Sync>,
    transforms: Vec<Box<dyn FnMut(Vec<V>) -> Vec<V> + Send + Sync>>,
}

impl<K, V> SubFlow<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Create a new sub-flow from a source and key function
    pub(crate) fn new<F>(source: Box<dyn SourceStage<V>>, key_fn: F) -> Self
    where
        F: FnMut(&V) -> K + Send + Sync + 'static,
    {
        Self {
            source,
            key_fn: Box::new(key_fn),
            transforms: Vec::new(),
        }
    }

    /// Map each element within sub-streams
    pub fn map<F>(mut self, f: F) -> Self
    where
        F: FnMut(V) -> V + Send + Sync + Clone + 'static,
    {
        let transform: Box<dyn FnMut(Vec<V>) -> Vec<V> + Send + Sync> = {
            let mut f = f;
            Box::new(move |items: Vec<V>| items.into_iter().map(|x| f(x)).collect())
        };
        self.transforms.push(transform);
        self
    }

    /// Filter elements within sub-streams
    pub fn filter<F>(mut self, f: F) -> Self
    where
        F: FnMut(&V) -> bool + Send + Sync + Clone + 'static,
    {
        let transform: Box<dyn FnMut(Vec<V>) -> Vec<V> + Send + Sync> = {
            let mut f = f;
            Box::new(move |items: Vec<V>| items.into_iter().filter(|x| f(x)).collect())
        };
        self.transforms.push(transform);
        self
    }

    /// Take first N elements from each sub-stream
    pub fn take(mut self, n: usize) -> Self {
        let transform: Box<dyn FnMut(Vec<V>) -> Vec<V> + Send + Sync> =
            Box::new(move |items: Vec<V>| items.into_iter().take(n).collect());
        self.transforms.push(transform);
        self
    }

    /// Merge all sub-streams back into a single stream
    ///
    /// Collects all elements, groups by key, applies transforms per group,
    /// then emits all results in key-group order.
    pub fn merge_substreams(self) -> Source<V> {
        Source::from_stage(SubFlowMergeStage {
            source: self.source,
            key_fn: self.key_fn,
            transforms: self.transforms,
            collected: false,
            output_buffer: std::collections::VecDeque::new(),
        })
    }

    /// Merge sub-streams back, emitting (key, elements) pairs
    pub fn merge_substreams_with_key(self) -> Source<(K, Vec<V>)> {
        Source::from_stage(SubFlowKeyedMergeStage {
            source: self.source,
            key_fn: self.key_fn,
            transforms: self.transforms,
            collected: false,
            output_buffer: std::collections::VecDeque::new(),
        })
    }

    /// Reduce each sub-stream to a single value
    pub fn reduce<F>(self, f: F) -> Source<(K, V)>
    where
        F: FnMut(V, V) -> V + Send + Sync + Clone + 'static,
    {
        Source::from_stage(SubFlowReduceStage {
            source: self.source,
            key_fn: self.key_fn,
            transforms: self.transforms,
            reduce_fn: Box::new(f),
            collected: false,
            output_buffer: std::collections::VecDeque::new(),
        })
    }

    /// Count elements in each sub-stream
    pub fn count_per_key(self) -> Source<(K, usize)> {
        Source::from_stage(SubFlowCountStage {
            source: self.source,
            key_fn: self.key_fn,
            collected: false,
            output_buffer: std::collections::VecDeque::new(),
        })
    }
}

// ============= Internal helper: collect & group =============

async fn collect_and_group<K, V>(
    source: &mut Box<dyn SourceStage<V>>,
    key_fn: &mut Box<dyn FnMut(&V) -> K + Send + Sync>,
) -> StreamResult<Vec<(K, Vec<V>)>>
where
    K: Clone + Eq + Hash,
    V: Send + Sync,
{
    let mut groups: HashMap<K, Vec<V>> = HashMap::new();
    let mut key_order: Vec<K> = Vec::new();

    loop {
        match source.pull().await? {
            StreamElement::Element(elem) => {
                let key = (key_fn)(&elem);
                if !groups.contains_key(&key) {
                    key_order.push(key.clone());
                }
                groups.entry(key).or_default().push(elem);
            }
            StreamElement::Complete => break,
            StreamElement::Error(e) => return Err(crate::errors::StreamError::StageFailed(e)),
        }
    }

    let result: Vec<(K, Vec<V>)> = key_order
        .into_iter()
        .filter_map(|k| {
            groups.remove(&k).map(|v| (k, v))
        })
        .collect();

    Ok(result)
}

// ============= SubFlowMergeStage =============

struct SubFlowMergeStage<K, V>
where
    K: Clone + Eq + Hash + Send + Sync,
    V: Send + Sync,
{
    source: Box<dyn SourceStage<V>>,
    key_fn: Box<dyn FnMut(&V) -> K + Send + Sync>,
    transforms: Vec<Box<dyn FnMut(Vec<V>) -> Vec<V> + Send + Sync>>,
    collected: bool,
    output_buffer: std::collections::VecDeque<V>,
}

#[async_trait]
impl<K, V> SourceStage<V> for SubFlowMergeStage<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    async fn pull(&mut self) -> StreamResult<StreamElement<V>> {
        if !self.collected {
            self.collected = true;
            let groups = collect_and_group(&mut self.source, &mut self.key_fn).await?;

            for (_key, mut items) in groups {
                for transform in &mut self.transforms {
                    items = transform(items);
                }
                for item in items {
                    self.output_buffer.push_back(item);
                }
            }
        }

        match self.output_buffer.pop_front() {
            Some(elem) => Ok(StreamElement::Element(elem)),
            None => Ok(StreamElement::Complete),
        }
    }
}

// ============= SubFlowKeyedMergeStage =============

struct SubFlowKeyedMergeStage<K, V>
where
    K: Clone + Eq + Hash + Send + Sync,
    V: Send + Sync,
{
    source: Box<dyn SourceStage<V>>,
    key_fn: Box<dyn FnMut(&V) -> K + Send + Sync>,
    transforms: Vec<Box<dyn FnMut(Vec<V>) -> Vec<V> + Send + Sync>>,
    collected: bool,
    output_buffer: std::collections::VecDeque<(K, Vec<V>)>,
}

#[async_trait]
impl<K, V> SourceStage<(K, Vec<V>)> for SubFlowKeyedMergeStage<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    async fn pull(&mut self) -> StreamResult<StreamElement<(K, Vec<V>)>> {
        if !self.collected {
            self.collected = true;
            let groups = collect_and_group(&mut self.source, &mut self.key_fn).await?;

            for (key, mut items) in groups {
                for transform in &mut self.transforms {
                    items = transform(items);
                }
                if !items.is_empty() {
                    self.output_buffer.push_back((key, items));
                }
            }
        }

        match self.output_buffer.pop_front() {
            Some(pair) => Ok(StreamElement::Element(pair)),
            None => Ok(StreamElement::Complete),
        }
    }
}

// ============= SubFlowReduceStage =============

struct SubFlowReduceStage<K, V>
where
    K: Clone + Eq + Hash + Send + Sync,
    V: Send + Sync,
{
    source: Box<dyn SourceStage<V>>,
    key_fn: Box<dyn FnMut(&V) -> K + Send + Sync>,
    transforms: Vec<Box<dyn FnMut(Vec<V>) -> Vec<V> + Send + Sync>>,
    reduce_fn: Box<dyn FnMut(V, V) -> V + Send + Sync>,
    collected: bool,
    output_buffer: std::collections::VecDeque<(K, V)>,
}

#[async_trait]
impl<K, V> SourceStage<(K, V)> for SubFlowReduceStage<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    async fn pull(&mut self) -> StreamResult<StreamElement<(K, V)>> {
        if !self.collected {
            self.collected = true;
            let groups = collect_and_group(&mut self.source, &mut self.key_fn).await?;

            for (key, mut items) in groups {
                for transform in &mut self.transforms {
                    items = transform(items);
                }
                if !items.is_empty() {
                    let mut iter = items.into_iter();
                    let first = iter.next().unwrap();
                    let reduced = iter.fold(first, |acc, x| (self.reduce_fn)(acc, x));
                    self.output_buffer.push_back((key, reduced));
                }
            }
        }

        match self.output_buffer.pop_front() {
            Some(pair) => Ok(StreamElement::Element(pair)),
            None => Ok(StreamElement::Complete),
        }
    }
}

// ============= SubFlowCountStage =============

struct SubFlowCountStage<K, V>
where
    K: Clone + Eq + Hash + Send + Sync,
    V: Send + Sync,
{
    source: Box<dyn SourceStage<V>>,
    key_fn: Box<dyn FnMut(&V) -> K + Send + Sync>,
    collected: bool,
    output_buffer: std::collections::VecDeque<(K, usize)>,
}

#[async_trait]
impl<K, V> SourceStage<(K, usize)> for SubFlowCountStage<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    async fn pull(&mut self) -> StreamResult<StreamElement<(K, usize)>> {
        if !self.collected {
            self.collected = true;
            let groups = collect_and_group(&mut self.source, &mut self.key_fn).await?;

            for (key, items) in groups {
                self.output_buffer.push_back((key, items.len()));
            }
        }

        match self.output_buffer.pop_front() {
            Some(pair) => Ok(StreamElement::Element(pair)),
            None => Ok(StreamElement::Complete),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_subflow_map_merge() {
        let result = Source::from_vec(vec![1, 2, 3, 4, 5, 6])
            .sub_flow_by(|x| x % 2)
            .map(|x| x * 10)
            .merge_substreams()
            .run_collect()
            .await
            .unwrap();

        // Odd group: [1,3,5] → [10,30,50], Even group: [2,4,6] → [20,40,60]
        // Order: odds first (key=1 appeared before key=0)
        assert_eq!(result, vec![10, 30, 50, 20, 40, 60]);
    }

    #[tokio::test]
    async fn test_subflow_filter_merge() {
        let result = Source::from_vec(vec![1, 2, 3, 4, 5, 6, 7, 8])
            .sub_flow_by(|x| x % 3)
            .filter(|x| *x > 3)
            .merge_substreams()
            .run_collect()
            .await
            .unwrap();

        // key=1: [1,4,7] → [4,7]
        // key=2: [2,5,8] → [5,8]
        // key=0: [3,6] → [6]
        assert_eq!(result, vec![4, 7, 5, 8, 6]);
    }

    #[tokio::test]
    async fn test_subflow_take_merge() {
        let result = Source::from_vec(vec![1, 2, 3, 4, 5, 6])
            .sub_flow_by(|x| x % 2)
            .take(2)
            .merge_substreams()
            .run_collect()
            .await
            .unwrap();

        // Odd: [1,3,5] → take(2) → [1,3]
        // Even: [2,4,6] → take(2) → [2,4]
        assert_eq!(result, vec![1, 3, 2, 4]);
    }

    #[tokio::test]
    async fn test_subflow_keyed_merge() {
        let result = Source::from_vec(vec!["apple", "banana", "avocado", "blueberry", "cherry"])
            .sub_flow_by(|s| s.chars().next().unwrap())
            .merge_substreams_with_key()
            .run_collect()
            .await
            .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], ('a', vec!["apple", "avocado"]));
        assert_eq!(result[1], ('b', vec!["banana", "blueberry"]));
        assert_eq!(result[2], ('c', vec!["cherry"]));
    }

    #[tokio::test]
    async fn test_subflow_reduce() {
        let result = Source::from_vec(vec![1, 2, 3, 4, 5, 6])
            .sub_flow_by(|x| x % 2)
            .reduce(|a, b| a + b)
            .run_collect()
            .await
            .unwrap();

        // Odd: 1+3+5=9, Even: 2+4+6=12
        assert_eq!(result, vec![(1, 9), (0, 12)]);
    }

    #[tokio::test]
    async fn test_subflow_count_per_key() {
        let result = Source::from_vec(vec![1, 2, 3, 4, 5, 6, 7])
            .sub_flow_by(|x| x % 3)
            .count_per_key()
            .run_collect()
            .await
            .unwrap();

        // key=1: [1,4,7] → 3
        // key=2: [2,5] → 2
        // key=0: [3,6] → 2
        assert_eq!(result, vec![(1, 3), (2, 2), (0, 2)]);
    }

    #[tokio::test]
    async fn test_subflow_chained_transforms() {
        let result = Source::from_vec(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
            .sub_flow_by(|x| if *x <= 5 { "low" } else { "high" })
            .map(|x| x * 2)
            .filter(|x| *x > 4)
            .merge_substreams()
            .run_collect()
            .await
            .unwrap();

        // low: [1,2,3,4,5] → *2 → [2,4,6,8,10] → >4 → [6,8,10]
        // high: [6,7,8,9,10] → *2 → [12,14,16,18,20] → >4 → [12,14,16,18,20]
        assert_eq!(result, vec![6, 8, 10, 12, 14, 16, 18, 20]);
    }
}

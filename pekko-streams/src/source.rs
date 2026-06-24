//! Source - Produces stream elements
//!
//! A Source has exactly one output. It represents the beginning of a stream
//! pipeline and is responsible for producing elements.

use crate::{
    errors::StreamResult,
    flow::Flow,
    sink::Sink,
    stage::{StreamElement, SourceStage},
};
use async_trait::async_trait;
use tokio::sync::mpsc;

/// A source of stream elements
pub struct Source<Out: Send + Sync + 'static> {
    pub(crate) inner: Box<dyn SourceStage<Out>>,
}

impl<Out: Send + Sync + Clone + 'static> Source<Out> {
    /// Create a source from a SourceStage implementation
    pub fn from_stage<S: SourceStage<Out> + 'static>(stage: S) -> Self {
        Self {
            inner: Box::new(stage),
        }
    }

    /// Create a source from an iterator
    pub fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Out> + Send + 'static,
        I::IntoIter: Send + Sync,
    {
        Self::from_stage(IterSource {
            iter: iter.into_iter(),
        })
    }

    /// Create a source from a vector
    pub fn from_vec(vec: Vec<Out>) -> Self {
        Self::from_stage(VecSource { vec, index: 0 })
    }

    /// Create a source that emits a single element
    pub fn single(element: Out) -> Self {
        Self::from_vec(vec![element])
    }

    /// Create an empty source
    pub fn empty() -> Self {
        Self::from_vec(vec![])
    }

    /// Create a source that repeats an element
    pub fn repeat(element: Out) -> Self {
        Self::from_stage(RepeatSource { element })
    }

    /// Create a source from a channel receiver
    pub fn from_channel(receiver: mpsc::Receiver<Out>) -> Self {
        Self::from_stage(ChannelSource { receiver })
    }

    /// Run the source with a sink
    pub async fn run_with<Mat: Send>(mut self, mut sink: Sink<Out, Mat>) -> StreamResult<Mat> {
        loop {
            match self.inner.pull().await? {
                StreamElement::Element(elem) => {
                    sink.inner.process(elem).await?;
                }
                StreamElement::Complete => {
                    return sink.inner.on_complete().await;
                }
                StreamElement::Error(e) => {
                    return sink.inner.on_error(e).await;
                }
            }
        }
    }

    /// Run and collect all elements into a Vec
    pub async fn run_collect(self) -> StreamResult<Vec<Out>> {
        self.run_with(Sink::collect()).await
    }

    /// Run and count elements
    pub async fn run_count(self) -> StreamResult<usize> {
        self.run_with(Sink::count()).await
    }

    /// Apply a flow transformation
    pub fn via<Out2: Send + Sync + Clone + 'static>(self, flow: Flow<Out, Out2>) -> Source<Out2> {
        Source::from_stage(ViaStage {
            source: self.inner,
            flow: flow.inner,
            buffer: std::collections::VecDeque::new(),
            source_complete: false,
        })
    }

    /// Take only the first N elements
    pub fn take(self, n: usize) -> Self {
        self.via(Flow::<Out, Out>::take(n))
    }

    /// Map each element
    pub fn map<Out2: Send + Sync + Clone + 'static, F>(self, f: F) -> Source<Out2>
    where
        F: FnMut(Out) -> Out2 + Send + Sync + 'static,
    {
        self.via(Flow::map(f))
    }

    /// Filter elements
    pub fn filter<F>(self, f: F) -> Source<Out>
    where
        F: FnMut(&Out) -> bool + Send + Sync + 'static,
    {
        self.via(Flow::<Out, Out>::filter(f))
    }

    /// Merge this source with another (interleaved)
    pub fn merge(self, other: Source<Out>) -> Source<Out> {
        use crate::graph::Merge;
        Merge::new(2).run(vec![self, other])
    }

    /// Concatenate this source with another (sequential)
    pub fn concat(self, other: Source<Out>) -> Source<Out> {
        use crate::graph::Concat;
        Concat::new().run(vec![self, other])
    }

    /// Zip this source with another
    pub fn zip<B: Send + Sync + Clone + 'static>(self, other: Source<B>) -> Source<(Out, B)> {
        use crate::graph::Zip;
        Zip::new().run(self, other)
    }

    /// FlatMap each element
    pub fn flat_map<Out2: Send + Sync + Clone + 'static, F>(self, f: F) -> Source<Out2>
    where
        F: FnMut(Out) -> Vec<Out2> + Send + Sync + 'static,
    {
        self.via(Flow::flat_map(f))
    }

    /// Scan - fold with intermediate values
    pub fn scan<State: Clone + Send + Sync + 'static, F>(self, initial: State, f: F) -> Source<State>
    where
        F: FnMut(&mut State, Out) + Send + Sync + 'static,
    {
        self.via(Flow::scan(initial, f))
    }

    /// Enumerate - add index to each element
    pub fn enumerate(self) -> Source<(usize, Out)> {
        self.via(Flow::enumerate())
    }

    /// Partition by predicate — emits (true/false, element) pairs
    pub fn partition<F>(self, f: F) -> Source<(bool, Out)>
    where
        F: FnMut(&Out) -> bool + Send + Sync + 'static,
    {
        self.via(Flow::partition(f))
    }

    /// Group by key — emits (key, element) pairs
    pub fn group_by<K, F>(self, f: F) -> Source<(K, Out)>
    where
        K: Clone + Send + Sync + 'static,
        F: FnMut(&Out) -> K + Send + Sync + 'static,
    {
        self.via(Flow::group_by(f))
    }

    /// Collect all by key, emit groups on complete
    pub fn collect_by_key<K, F>(self, f: F) -> Source<(K, Vec<Out>)>
    where
        K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
        F: FnMut(&Out) -> K + Send + Sync + 'static,
    {
        self.via(Flow::collect_by_key(f))
    }

    /// Count-based window
    pub fn count_window(self, size: usize) -> Source<Vec<Out>> {
        self.via(Flow::count_window(size))
    }

    /// Create a SubFlow by grouping elements by key
    ///
    /// Each group is processed independently, then merged back.
    pub fn sub_flow_by<K, F>(self, key_fn: F) -> crate::subflow::SubFlow<K, Out>
    where
        K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
        F: FnMut(&Out) -> K + Send + Sync + 'static,
    {
        crate::subflow::SubFlow::new(self.inner, key_fn)
    }

    /// Run this source on a separate async task with a bounded channel
    pub fn async_boundary(self, buffer_size: usize) -> Source<Out> {
        crate::async_boundary::async_source(
            self,
            crate::async_boundary::AsyncBoundaryConfig::new(buffer_size),
        )
    }
}

// ============= Source Stage Implementations =============

/// Source from an iterator
struct IterSource<I: Iterator> {
    iter: I,
}

#[async_trait]
impl<I, T> SourceStage<T> for IterSource<I>
where
    I: Iterator<Item = T> + Send + Sync,
    T: Send + Sync,
{
    async fn pull(&mut self) -> StreamResult<StreamElement<T>> {
        match self.iter.next() {
            Some(elem) => Ok(StreamElement::Element(elem)),
            None => Ok(StreamElement::Complete),
        }
    }
}

/// Source from a Vec
struct VecSource<T> {
    vec: Vec<T>,
    index: usize,
}

#[async_trait]
impl<T: Send + Sync + Clone> SourceStage<T> for VecSource<T> {
    async fn pull(&mut self) -> StreamResult<StreamElement<T>> {
        if self.index < self.vec.len() {
            let elem = self.vec[self.index].clone();
            self.index += 1;
            Ok(StreamElement::Element(elem))
        } else {
            Ok(StreamElement::Complete)
        }
    }
}

/// Source that repeats an element infinitely
struct RepeatSource<T> {
    element: T,
}

#[async_trait]
impl<T: Clone + Send + Sync> SourceStage<T> for RepeatSource<T> {
    async fn pull(&mut self) -> StreamResult<StreamElement<T>> {
        Ok(StreamElement::Element(self.element.clone()))
    }
}

/// Source from a channel
struct ChannelSource<T> {
    receiver: mpsc::Receiver<T>,
}

#[async_trait]
impl<T: Send + Sync> SourceStage<T> for ChannelSource<T> {
    async fn pull(&mut self) -> StreamResult<StreamElement<T>> {
        match self.receiver.recv().await {
            Some(elem) => Ok(StreamElement::Element(elem)),
            None => Ok(StreamElement::Complete),
        }
    }
}

/// Via stage - combines source with flow
struct ViaStage<In: Send + Sync, Out: Send + Sync> {
    source: Box<dyn SourceStage<In>>,
    flow: Box<dyn crate::flow::FlowStageBoxed<In, Out>>,
    buffer: std::collections::VecDeque<Out>,
    source_complete: bool,
}

#[async_trait]
impl<In: Send + Sync + 'static, Out: Send + Sync + 'static> SourceStage<Out> for ViaStage<In, Out> {
    async fn pull(&mut self) -> StreamResult<StreamElement<Out>> {
        // Drain buffered elements first
        if let Some(elem) = self.buffer.pop_front() {
            return Ok(StreamElement::Element(elem));
        }

        if self.source_complete {
            return Ok(StreamElement::Complete);
        }

        loop {
            match self.source.pull().await? {
                StreamElement::Element(elem) => {
                    let results = self.flow.process(elem).await?;
                    for item in results {
                        self.buffer.push_back(item);
                    }
                    if let Some(first) = self.buffer.pop_front() {
                        return Ok(StreamElement::Element(first));
                    }
                    // process returned empty, try next element
                }
                StreamElement::Complete => {
                    let final_results = self.flow.on_complete().await?;
                    for item in final_results {
                        self.buffer.push_back(item);
                    }
                    self.source_complete = true;
                    if let Some(first) = self.buffer.pop_front() {
                        return Ok(StreamElement::Element(first));
                    }
                    return Ok(StreamElement::Complete);
                }
                StreamElement::Error(e) => {
                    return Ok(StreamElement::Error(e));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_source_from_vec() {
        let source = Source::from_vec(vec![1, 2, 3, 4, 5]);
        let result = source.run_collect().await.unwrap();
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_source_single() {
        let source = Source::single(42);
        let result = source.run_collect().await.unwrap();
        assert_eq!(result, vec![42]);
    }

    #[tokio::test]
    async fn test_source_empty() {
        let source: Source<i32> = Source::empty();
        let result = source.run_collect().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_source_map() {
        let source = Source::from_vec(vec![1, 2, 3]).map(|x| x * 2);
        let result = source.run_collect().await.unwrap();
        assert_eq!(result, vec![2, 4, 6]);
    }

    #[tokio::test]
    async fn test_source_filter() {
        let source = Source::from_vec(vec![1, 2, 3, 4, 5]).filter(|x| x % 2 == 0);
        let result = source.run_collect().await.unwrap();
        assert_eq!(result, vec![2, 4]);
    }

    #[tokio::test]
    async fn test_source_take() {
        let source = Source::from_vec((1..=100).collect()).take(3);
        let result = source.run_collect().await.unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_source_count() {
        let source = Source::from_vec((1..=10).collect());
        let count = source.run_count().await.unwrap();
        assert_eq!(count, 10);
    }
}

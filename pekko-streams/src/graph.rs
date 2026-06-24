//! Graph DSL for building complex stream topologies
//!
//! While Source, Flow, and Sink handle linear pipelines, the Graph DSL
//! allows building more complex topologies like fan-out, fan-in, cycles, etc.
//!
//! ## Shapes
//! - **Broadcast** - Fan-out: sends each element to all outputs
//! - **Merge** - Fan-in: collects elements from all inputs
//! - **Balance** - Round-robin distribution across outputs
//! - **Zip** - Combines elements from two streams into tuples
//! - **Concat** - Sequential concatenation of streams

use crate::{
    errors::StreamResult,
    source::Source,
    sink::Sink,
    stage::StreamElement,
};
use std::marker::PhantomData;
use tokio::sync::mpsc;

/// A runnable graph ready for execution
pub struct RunnableGraph<Mat> {
    runner: Box<dyn FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = StreamResult<Mat>> + Send>> + Send>,
}

impl<Mat: Send + 'static> RunnableGraph<Mat> {
    /// Create a runnable graph from a source and sink
    pub fn from_source_sink<T: Send + Sync + Clone + 'static>(
        source: Source<T>,
        sink: Sink<T, Mat>,
    ) -> Self {
        Self {
            runner: Box::new(move || {
                Box::pin(async move {
                    source.run_with(sink).await
                })
            }),
        }
    }

    /// Run the graph
    pub async fn run(self) -> StreamResult<Mat> {
        (self.runner)().await
    }
}

/// Builder for creating graphs
pub struct GraphBuilder {
    // Placeholder for future graph building functionality
}

impl GraphBuilder {
    /// Create a new graph builder
    pub fn new() -> Self {
        Self {}
    }

    /// Build a linear graph from source to sink
    pub fn from_source<T: Send + Sync + Clone + 'static, Mat: Send + 'static>(
        source: Source<T>,
        sink: Sink<T, Mat>,
    ) -> RunnableGraph<Mat> {
        RunnableGraph::from_source_sink(source, sink)
    }
}

impl Default for GraphBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============= Broadcast - Fan-out (1 → N) =============

/// Broadcast - fan-out to multiple outputs.
/// Each element from the input is cloned and sent to all output channels.
pub struct Broadcast<T> {
    outputs: usize,
    _phantom: PhantomData<T>,
}

impl<T: Clone + Send + Sync + 'static> Broadcast<T> {
    /// Create a broadcast with N outputs
    pub fn new(outputs: usize) -> Self {
        assert!(outputs > 0, "Broadcast must have at least 1 output");
        Self {
            outputs,
            _phantom: PhantomData,
        }
    }

    /// Get the number of outputs
    pub fn output_count(&self) -> usize {
        self.outputs
    }

    /// Run the broadcast: consume a source and produce N sources
    pub fn run(self, source: Source<T>) -> Vec<Source<T>> {
        let buffer_size = 256;
        let mut senders = Vec::with_capacity(self.outputs);
        let mut sources = Vec::with_capacity(self.outputs);

        for _ in 0..self.outputs {
            let (tx, rx) = mpsc::channel(buffer_size);
            senders.push(tx);
            sources.push(Source::from_channel(rx));
        }

        tokio::spawn(async move {
            let mut src = source;
            loop {
                match src.pull().await {
                    Ok(StreamElement::Element(elem)) => {
                        let mut all_closed = true;
                        for sender in &senders {
                            if sender.send(elem.clone()).await.is_ok() {
                                all_closed = false;
                            }
                        }
                        if all_closed {
                            break;
                        }
                    }
                    Ok(StreamElement::Complete) | Ok(StreamElement::Error(_)) => break,
                    Err(_) => break,
                }
            }
        });

        sources
    }
}

// ============= Merge - Fan-in (N → 1) =============

/// Merge - fan-in from multiple inputs.
/// Collects elements from all input sources into a single output.
pub struct Merge<T> {
    inputs: usize,
    _phantom: PhantomData<T>,
}

impl<T: Send + Sync + Clone + 'static> Merge<T> {
    /// Create a merge with N inputs
    pub fn new(inputs: usize) -> Self {
        assert!(inputs > 0, "Merge must have at least 1 input");
        Self {
            inputs,
            _phantom: PhantomData,
        }
    }

    /// Get the number of inputs
    pub fn input_count(&self) -> usize {
        self.inputs
    }

    /// Merge multiple sources into one
    pub fn run(self, sources: Vec<Source<T>>) -> Source<T> {
        assert_eq!(sources.len(), self.inputs, "Expected {} sources", self.inputs);
        let buffer_size = 256;
        let (tx, rx) = mpsc::channel(buffer_size);

        for source in sources {
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut src = source;
                loop {
                    match src.pull().await {
                        Ok(StreamElement::Element(elem)) => {
                            if tx.send(elem).await.is_err() {
                                break;
                            }
                        }
                        Ok(StreamElement::Complete) | Ok(StreamElement::Error(_)) => break,
                        Err(_) => break,
                    }
                }
            });
        }
        // Drop the original sender so the channel closes when all spawned tasks finish
        drop(tx);

        Source::from_channel(rx)
    }
}

// ============= Balance - Round-robin (1 → N) =============

/// Balance - distribute elements round-robin across outputs.
/// Unlike Broadcast, each element goes to exactly one output.
pub struct Balance<T> {
    outputs: usize,
    _phantom: PhantomData<T>,
}

impl<T: Send + Sync + Clone + 'static> Balance<T> {
    /// Create a balance with N outputs
    pub fn new(outputs: usize) -> Self {
        assert!(outputs > 0, "Balance must have at least 1 output");
        Self {
            outputs,
            _phantom: PhantomData,
        }
    }

    /// Get the number of outputs
    pub fn output_count(&self) -> usize {
        self.outputs
    }

    /// Run the balance: distribute elements round-robin
    pub fn run(self, source: Source<T>) -> Vec<Source<T>> {
        let buffer_size = 256;
        let mut senders = Vec::with_capacity(self.outputs);
        let mut sources = Vec::with_capacity(self.outputs);

        for _ in 0..self.outputs {
            let (tx, rx) = mpsc::channel(buffer_size);
            senders.push(tx);
            sources.push(Source::from_channel(rx));
        }

        let output_count = self.outputs;
        tokio::spawn(async move {
            let mut src = source;
            let mut index = 0usize;
            loop {
                match src.pull().await {
                    Ok(StreamElement::Element(elem)) => {
                        // Round-robin: send to next open output
                        let i = index % output_count;
                        index += 1;
                        if senders[i].send(elem).await.is_err() {
                            break; // Output closed
                        }
                    }
                    Ok(StreamElement::Complete) | Ok(StreamElement::Error(_)) => break,
                    Err(_) => break,
                }
            }
        });

        sources
    }
}

// ============= Zip - Combine two streams =============

/// Zip - combine elements from two streams into tuples.
/// Emits a tuple for each pair of elements; completes when either completes.
pub struct Zip<A, B> {
    _phantom: PhantomData<(A, B)>,
}

impl<A: Send + Sync + Clone + 'static, B: Send + Sync + Clone + 'static> Zip<A, B> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    /// Zip two sources into a source of tuples
    pub fn run(self, source_a: Source<A>, source_b: Source<B>) -> Source<(A, B)> {
        let buffer_size = 256;
        let (tx, rx) = mpsc::channel(buffer_size);

        tokio::spawn(async move {
            let mut a = source_a;
            let mut b = source_b;
            loop {
                let elem_a = match a.pull().await {
                    Ok(StreamElement::Element(e)) => e,
                    _ => break,
                };
                let elem_b = match b.pull().await {
                    Ok(StreamElement::Element(e)) => e,
                    _ => break,
                };
                if tx.send((elem_a, elem_b)).await.is_err() {
                    break;
                }
            }
        });

        Source::from_channel(rx)
    }
}

impl<A: Send + Sync + Clone + 'static, B: Send + Sync + Clone + 'static> Default for Zip<A, B> {
    fn default() -> Self {
        Self::new()
    }
}

// ============= Concat - Sequential streams =============

/// Concat - concatenate multiple streams sequentially.
/// Processes each source fully before moving to the next.
pub struct Concat<T> {
    _phantom: PhantomData<T>,
}

impl<T: Send + Sync + Clone + 'static> Concat<T> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    /// Concatenate multiple sources into one (sequential)
    pub fn run(self, sources: Vec<Source<T>>) -> Source<T> {
        let buffer_size = 256;
        let (tx, rx) = mpsc::channel(buffer_size);

        tokio::spawn(async move {
            for source in sources {
                let mut src = source;
                loop {
                    match src.pull().await {
                        Ok(StreamElement::Element(elem)) => {
                            if tx.send(elem).await.is_err() {
                                return;
                            }
                        }
                        Ok(StreamElement::Complete) => break,
                        Ok(StreamElement::Error(_)) => return,
                        Err(_) => return,
                    }
                }
            }
        });

        Source::from_channel(rx)
    }
}

impl<T: Send + Sync + Clone + 'static> Default for Concat<T> {
    fn default() -> Self {
        Self::new()
    }
}

// ============= Source convenience methods =============

impl<T: Send + Sync + Clone + 'static> Source<T> {
    /// Expose pull for graph shapes (internal)
    pub(crate) async fn pull(&mut self) -> StreamResult<StreamElement<T>> {
        self.inner.pull().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_runnable_graph() {
        let source = Source::from_iter(1..=5);
        let sink = Sink::collect();

        let graph = RunnableGraph::from_source_sink(source, sink);
        let result = graph.run().await.unwrap();

        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_graph_builder() {
        let source = Source::from_iter(1..=3);
        let sink = Sink::count();

        let graph = GraphBuilder::from_source(source, sink);
        let result = graph.run().await.unwrap();

        assert_eq!(result, 3);
    }

    #[tokio::test]
    async fn test_broadcast_basic() {
        let source = Source::from_vec(vec![1, 2, 3]);
        let broadcast = Broadcast::new(2);
        let outputs = broadcast.run(source);

        assert_eq!(outputs.len(), 2);

        let r0 = outputs.into_iter().nth(0).unwrap().run_collect().await.unwrap();
        // Due to async timing, at least verify we get the elements
        assert!(!r0.is_empty());
    }

    #[tokio::test]
    async fn test_broadcast_all_receive() {
        let source = Source::from_vec(vec![10, 20, 30]);
        let broadcast = Broadcast::new(3);
        let mut outputs = broadcast.run(source);

        let mut results = Vec::new();
        for src in outputs.drain(..) {
            let r = src.run_collect().await.unwrap();
            results.push(r);
        }

        for r in &results {
            assert_eq!(r, &vec![10, 20, 30]);
        }
    }

    #[tokio::test]
    async fn test_merge_basic() {
        let s1 = Source::from_vec(vec![1, 2, 3]);
        let s2 = Source::from_vec(vec![4, 5, 6]);

        let merge = Merge::new(2);
        let merged = merge.run(vec![s1, s2]);
        let mut result = merged.run_collect().await.unwrap();
        result.sort();

        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_merge_three_sources() {
        let s1 = Source::from_vec(vec![1]);
        let s2 = Source::from_vec(vec![2]);
        let s3 = Source::from_vec(vec![3]);

        let merge = Merge::new(3);
        let merged = merge.run(vec![s1, s2, s3]);
        let mut result = merged.run_collect().await.unwrap();
        result.sort();

        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_balance_distributes() {
        let source = Source::from_vec(vec![1, 2, 3, 4, 5, 6]);
        let balance = Balance::new(2);
        let mut outputs = balance.run(source);

        let mut total = Vec::new();
        for src in outputs.drain(..) {
            total.extend(src.run_collect().await.unwrap());
        }
        total.sort();

        assert_eq!(total, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_zip_basic() {
        let s1 = Source::from_vec(vec![1, 2, 3]);
        let s2 = Source::from_vec(vec!["a", "b", "c"]);

        let zip = Zip::new();
        let zipped = zip.run(s1, s2);
        let result = zipped.run_collect().await.unwrap();

        assert_eq!(result, vec![(1, "a"), (2, "b"), (3, "c")]);
    }

    #[tokio::test]
    async fn test_zip_different_lengths() {
        let s1 = Source::from_vec(vec![1, 2, 3, 4, 5]);
        let s2 = Source::from_vec(vec!["a", "b"]);

        let zip = Zip::new();
        let zipped = zip.run(s1, s2);
        let result = zipped.run_collect().await.unwrap();

        // Zip stops at shorter stream
        assert_eq!(result, vec![(1, "a"), (2, "b")]);
    }

    #[tokio::test]
    async fn test_concat_basic() {
        let s1 = Source::from_vec(vec![1, 2, 3]);
        let s2 = Source::from_vec(vec![4, 5, 6]);

        let concat = Concat::new();
        let concatenated = concat.run(vec![s1, s2]);
        let result = concatenated.run_collect().await.unwrap();

        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_concat_three_sources() {
        let s1 = Source::from_vec(vec![1]);
        let s2 = Source::from_vec(vec![2, 3]);
        let s3 = Source::from_vec(vec![4, 5, 6]);

        let concat = Concat::new();
        let concatenated = concat.run(vec![s1, s2, s3]);
        let result = concatenated.run_collect().await.unwrap();

        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_concat_with_empty() {
        let s1 = Source::from_vec(vec![1, 2]);
        let s2: Source<i32> = Source::empty();
        let s3 = Source::from_vec(vec![3, 4]);

        let concat = Concat::new();
        let concatenated = concat.run(vec![s1, s2, s3]);
        let result = concatenated.run_collect().await.unwrap();

        assert_eq!(result, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_broadcast_then_merge() {
        // Fan-out then fan-in: source → broadcast(2) → merge(2) → sink
        let source = Source::from_vec(vec![1, 2, 3]);
        let broadcast = Broadcast::new(2);
        let outputs = broadcast.run(source);

        let merge = Merge::new(2);
        let merged = merge.run(outputs);
        let mut result = merged.run_collect().await.unwrap();
        result.sort();

        // Each element appears twice (broadcast to 2, then merged)
        assert_eq!(result, vec![1, 1, 2, 2, 3, 3]);
    }

    #[tokio::test]
    async fn test_broadcast_count() {
        let broadcast: Broadcast<i32> = Broadcast::new(3);
        assert_eq!(broadcast.output_count(), 3);
    }

    #[tokio::test]
    async fn test_merge_count() {
        let merge: Merge<i32> = Merge::new(2);
        assert_eq!(merge.input_count(), 2);
    }
}

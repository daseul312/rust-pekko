//! Async Boundary - Decouple stream stages across async task boundaries
//!
//! An async boundary inserts a channel between two parts of a pipeline,
//! allowing them to run on separate tokio tasks. This is useful for:
//!
//! - Isolating CPU-heavy stages from I/O-bound stages
//! - Enabling true parallelism between pipeline stages
//! - Controlling backpressure between independent processing units
//!
//! Modeled after Scala Pekko's `.async` boundary operator.

use crate::errors::{StreamError, StreamResult};
use crate::source::Source;
use crate::stage::{FlowStage, SourceStage, StreamElement};
use async_trait::async_trait;
use tokio::sync::mpsc;

/// Configuration for async boundaries
#[derive(Debug, Clone)]
pub struct AsyncBoundaryConfig {
    /// Buffer size for the channel between tasks
    pub buffer_size: usize,
}

impl AsyncBoundaryConfig {
    pub fn new(buffer_size: usize) -> Self {
        Self { buffer_size }
    }
}

impl Default for AsyncBoundaryConfig {
    fn default() -> Self {
        Self { buffer_size: 16 }
    }
}

/// Insert an async boundary after a source, running the source on a separate task
///
/// The source will be pulled on a spawned tokio task, and elements are
/// sent through a bounded channel to the downstream consumer.
pub fn async_source<T>(source: Source<T>, config: AsyncBoundaryConfig) -> Source<T>
where
    T: Send + Sync + Clone + 'static,
{
    let (tx, rx) = mpsc::channel(config.buffer_size);

    // Spawn the source on a separate task
    tokio::spawn(async move {
        let mut inner = source.inner;
        loop {
            match inner.pull().await {
                Ok(StreamElement::Element(elem)) => {
                    if tx.send(Ok(elem)).await.is_err() {
                        break; // Downstream dropped
                    }
                }
                Ok(StreamElement::Complete) => {
                    break;
                }
                Ok(StreamElement::Error(e)) => {
                    let _ = tx.send(Err(e)).await;
                    break;
                }
                Err(e) => {
                    let _ = tx.send(Err(e.to_string())).await;
                    break;
                }
            }
        }
    });

    Source::from_stage(AsyncBoundarySource { receiver: rx })
}

/// Source that receives from an async boundary channel
struct AsyncBoundarySource<T> {
    receiver: mpsc::Receiver<Result<T, String>>,
}

#[async_trait]
impl<T: Send + Sync + 'static> SourceStage<T> for AsyncBoundarySource<T> {
    async fn pull(&mut self) -> StreamResult<StreamElement<T>> {
        match self.receiver.recv().await {
            Some(Ok(elem)) => Ok(StreamElement::Element(elem)),
            Some(Err(e)) => Ok(StreamElement::Error(e)),
            None => Ok(StreamElement::Complete),
        }
    }
}

/// A flow stage that runs its inner processing on a separate task
///
/// Elements are sent to a worker task through a channel, processed,
/// and results are sent back through another channel.
pub struct AsyncFlow<In, Out>
where
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
{
    input_tx: mpsc::Sender<Option<In>>,
    output_rx: mpsc::Receiver<StreamResult<Vec<Out>>>,
}

impl<In, Out> AsyncFlow<In, Out>
where
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
{
    /// Wrap a FlowStage in an async boundary
    pub fn new<S>(mut stage: S, buffer_size: usize) -> Self
    where
        S: FlowStage<In, Out> + 'static,
    {
        let (input_tx, mut input_rx) = mpsc::channel::<Option<In>>(buffer_size);
        let (output_tx, output_rx) = mpsc::channel::<StreamResult<Vec<Out>>>(buffer_size);

        tokio::spawn(async move {
            while let Some(msg) = input_rx.recv().await {
                match msg {
                    Some(element) => {
                        let result = stage.process(element).await;
                        if output_tx.send(result).await.is_err() {
                            break;
                        }
                    }
                    None => {
                        // on_complete signal
                        let result = stage.on_complete().await;
                        let _ = output_tx.send(result).await;
                        break;
                    }
                }
            }
        });

        Self {
            input_tx,
            output_rx,
        }
    }
}

#[async_trait]
impl<In, Out> FlowStage<In, Out> for AsyncFlow<In, Out>
where
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
{
    async fn process(&mut self, element: In) -> StreamResult<Vec<Out>> {
        self.input_tx
            .send(Some(element))
            .await
            .map_err(|_| StreamError::ChannelClosed)?;

        self.output_rx
            .recv()
            .await
            .ok_or(StreamError::ChannelClosed)?
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<Out>> {
        self.input_tx
            .send(None)
            .await
            .map_err(|_| StreamError::ChannelClosed)?;

        self.output_rx
            .recv()
            .await
            .ok_or(StreamError::ChannelClosed)?
    }
}

/// Parallel map - process elements in parallel using multiple worker tasks
///
/// Spawns `parallelism` worker tasks, distributes elements round-robin,
/// and collects results preserving input order.
pub async fn parallel_map<In, Out, F>(
    source: Source<In>,
    parallelism: usize,
    buffer_per_worker: usize,
    mut f: F,
) -> StreamResult<Vec<Out>>
where
    In: Send + Sync + Clone + 'static,
    Out: Send + Sync + Clone + 'static,
    F: FnMut(In) -> Out + Send + Sync + Clone + 'static,
{
    let (results_tx, mut results_rx) =
        mpsc::channel::<(usize, Out)>(parallelism * buffer_per_worker);

    // Collect all source elements first
    let elements = source.run_collect().await?;
    let total = elements.len();

    // Spawn workers
    for (idx, element) in elements.into_iter().enumerate() {
        let tx = results_tx.clone();
        let mut f_clone = f.clone();
        tokio::spawn(async move {
            let result = f_clone(element);
            let _ = tx.send((idx, result)).await;
        });
    }

    // Drop the original sender so the channel closes when all workers complete
    drop(results_tx);

    // Collect results
    let mut indexed_results: Vec<(usize, Out)> = Vec::with_capacity(total);
    while let Some(pair) = results_rx.recv().await {
        indexed_results.push(pair);
    }

    // Sort by original index to preserve order
    indexed_results.sort_by_key(|(idx, _)| *idx);
    Ok(indexed_results.into_iter().map(|(_, v)| v).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flow::Flow;

    #[tokio::test]
    async fn test_async_source() {
        let source = Source::from_vec(vec![1, 2, 3, 4, 5]);
        let async_src = async_source(source, AsyncBoundaryConfig::default());

        let result = async_src.run_collect().await.unwrap();
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_async_source_with_transform() {
        let source = Source::from_vec(vec![1, 2, 3]);
        let result = async_source(source, AsyncBoundaryConfig::new(4))
            .map(|x| x * 10)
            .run_collect()
            .await
            .unwrap();

        assert_eq!(result, vec![10, 20, 30]);
    }

    #[tokio::test]
    async fn test_async_flow() {
        // Test AsyncFlow by wrapping a FlatMapStage in an async boundary
        let async_stage = AsyncFlow::new(
            crate::operators::FlatMapStage {
                f: |x: i32| vec![x, x * 10],
            },
            8,
        );

        let result = Source::from_vec(vec![1, 2, 3])
            .via(Flow::from_stage(async_stage))
            .run_collect()
            .await
            .unwrap();

        assert_eq!(result, vec![1, 10, 2, 20, 3, 30]);
    }

    #[tokio::test]
    async fn test_async_source_large_stream() {
        let data: Vec<i32> = (1..=1000).collect();
        let source = Source::from_vec(data.clone());
        let result = async_source(source, AsyncBoundaryConfig::new(32))
            .run_collect()
            .await
            .unwrap();

        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_parallel_map() {
        let source = Source::from_vec(vec![1, 2, 3, 4, 5]);
        let result = parallel_map(source, 4, 8, |x| x * x).await.unwrap();
        assert_eq!(result, vec![1, 4, 9, 16, 25]);
    }

    #[tokio::test]
    async fn test_parallel_map_preserves_order() {
        let source = Source::from_vec((1..=20).collect());
        let result = parallel_map(source, 8, 4, |x: i32| {
            // Simulate varying processing times
            x * 2
        })
        .await
        .unwrap();

        let expected: Vec<i32> = (1..=20).map(|x| x * 2).collect();
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_async_boundary_backpressure() {
        // Test with small buffer to ensure backpressure works
        let source = Source::from_vec((1..=100).collect::<Vec<i32>>());
        let result = async_source(source, AsyncBoundaryConfig::new(2))
            .run_collect()
            .await
            .unwrap();

        assert_eq!(result.len(), 100);
        assert_eq!(result[0], 1);
        assert_eq!(result[99], 100);
    }
}

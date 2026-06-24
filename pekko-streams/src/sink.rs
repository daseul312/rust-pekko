//! Sink - Consumes stream elements
//!
//! A Sink has exactly one input and no outputs. It represents the end of a
//! stream pipeline and produces a materialized value when complete.

use crate::{
    errors::{StreamError, StreamResult},
    stage::SinkStage,
};
use async_trait::async_trait;
use tokio::sync::mpsc;

/// A sink that consumes elements
pub struct Sink<In, Mat> {
    pub(crate) inner: Box<dyn SinkStage<In, Mat>>,
}

impl<In: Send + Sync + 'static, Mat: Send + 'static> Sink<In, Mat> {
    /// Create a sink from a SinkStage
    pub fn from_stage<S: SinkStage<In, Mat> + 'static>(stage: S) -> Self {
        Self {
            inner: Box::new(stage),
        }
    }
}

impl<In: Send + Sync + 'static> Sink<In, ()> {
    /// Create a sink that ignores all elements
    pub fn ignore() -> Self {
        Self::from_stage(IgnoreSink)
    }

    /// Create a sink that applies a function to each element
    pub fn foreach<F>(f: F) -> Self
    where
        F: FnMut(In) + Send + Sync + 'static,
    {
        Self::from_stage(ForeachSink { f })
    }

    /// Create a sink that sends elements to a channel
    pub fn to_channel(sender: mpsc::Sender<In>) -> Self {
        Self::from_stage(ChannelSink { sender })
    }
}

impl<In: Send + Sync + Clone + 'static> Sink<In, Vec<In>> {
    /// Create a sink that collects all elements into a Vec
    pub fn collect() -> Self {
        Self::from_stage(CollectSink { elements: Vec::new() })
    }
}

impl<In: Send + Sync + 'static> Sink<In, usize> {
    /// Create a sink that counts elements
    pub fn count() -> Self {
        Self::from_stage(CountSink { count: 0 })
    }
}

impl<In: Send + Sync + 'static> Sink<In, Option<In>> {
    /// Create a sink that returns the first element
    pub fn first() -> Self {
        Self::from_stage(FirstSink { first: None })
    }

    /// Create a sink that returns the last element
    pub fn last() -> Self {
        Self::from_stage(LastSink { last: None })
    }
}

impl<In: Send + Sync + Default + 'static> Sink<In, In> {
    /// Create a sink that folds all elements
    pub fn fold<F>(initial: In, f: F) -> Self
    where
        F: FnMut(In, In) -> In + Send + Sync + 'static,
    {
        Self::from_stage(FoldSink {
            accumulator: initial,
            f,
        })
    }
}

// ============= Sink Stage Implementations =============

/// Ignore sink
struct IgnoreSink;

#[async_trait]
impl<T: Send + Sync + 'static> SinkStage<T, ()> for IgnoreSink {
    async fn process(&mut self, _element: T) -> StreamResult<()> {
        Ok(())
    }

    async fn on_complete(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn on_error(&mut self, _error: String) -> StreamResult<()> {
        Ok(())
    }
}

/// Foreach sink
struct ForeachSink<F> {
    f: F,
}

#[async_trait]
impl<T: Send + Sync + 'static, F: FnMut(T) + Send + Sync> SinkStage<T, ()> for ForeachSink<F> {
    async fn process(&mut self, element: T) -> StreamResult<()> {
        (self.f)(element);
        Ok(())
    }

    async fn on_complete(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn on_error(&mut self, _error: String) -> StreamResult<()> {
        Ok(())
    }
}

/// Collect sink
struct CollectSink<T> {
    elements: Vec<T>,
}

#[async_trait]
impl<T: Send + Sync + Clone + 'static> SinkStage<T, Vec<T>> for CollectSink<T> {
    async fn process(&mut self, element: T) -> StreamResult<()> {
        self.elements.push(element);
        Ok(())
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<T>> {
        Ok(std::mem::take(&mut self.elements))
    }

    async fn on_error(&mut self, error: String) -> StreamResult<Vec<T>> {
        Err(StreamError::StageFailed(error))
    }
}

/// Count sink
struct CountSink {
    count: usize,
}

#[async_trait]
impl<T: Send + Sync + 'static> SinkStage<T, usize> for CountSink {
    async fn process(&mut self, _element: T) -> StreamResult<()> {
        self.count += 1;
        Ok(())
    }

    async fn on_complete(&mut self) -> StreamResult<usize> {
        Ok(self.count)
    }

    async fn on_error(&mut self, error: String) -> StreamResult<usize> {
        Err(StreamError::StageFailed(error))
    }
}

/// First sink
struct FirstSink<T> {
    first: Option<T>,
}

#[async_trait]
impl<T: Send + Sync + 'static> SinkStage<T, Option<T>> for FirstSink<T> {
    async fn process(&mut self, element: T) -> StreamResult<()> {
        if self.first.is_none() {
            self.first = Some(element);
        }
        Ok(())
    }

    async fn on_complete(&mut self) -> StreamResult<Option<T>> {
        Ok(self.first.take())
    }

    async fn on_error(&mut self, error: String) -> StreamResult<Option<T>> {
        Err(StreamError::StageFailed(error))
    }
}

/// Last sink
struct LastSink<T> {
    last: Option<T>,
}

#[async_trait]
impl<T: Send + Sync + 'static> SinkStage<T, Option<T>> for LastSink<T> {
    async fn process(&mut self, element: T) -> StreamResult<()> {
        self.last = Some(element);
        Ok(())
    }

    async fn on_complete(&mut self) -> StreamResult<Option<T>> {
        Ok(self.last.take())
    }

    async fn on_error(&mut self, error: String) -> StreamResult<Option<T>> {
        Err(StreamError::StageFailed(error))
    }
}

/// Fold sink
struct FoldSink<T, F> {
    accumulator: T,
    f: F,
}

#[async_trait]
impl<T: Send + Sync + Default + 'static, F: FnMut(T, T) -> T + Send + Sync> SinkStage<T, T> for FoldSink<T, F> {
    async fn process(&mut self, element: T) -> StreamResult<()> {
        let acc = std::mem::take(&mut self.accumulator);
        self.accumulator = (self.f)(acc, element);
        Ok(())
    }

    async fn on_complete(&mut self) -> StreamResult<T> {
        Ok(std::mem::take(&mut self.accumulator))
    }

    async fn on_error(&mut self, error: String) -> StreamResult<T> {
        Err(StreamError::StageFailed(error))
    }
}

/// Channel sink
struct ChannelSink<T> {
    sender: mpsc::Sender<T>,
}

#[async_trait]
impl<T: Send + Sync + 'static> SinkStage<T, ()> for ChannelSink<T> {
    async fn process(&mut self, element: T) -> StreamResult<()> {
        self.sender.send(element).await
            .map_err(|_| StreamError::ChannelClosed)
    }

    async fn on_complete(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn on_error(&mut self, _error: String) -> StreamResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Source;

    #[tokio::test]
    async fn test_sink_collect() {
        let result = Source::from_vec(vec![1, 2, 3])
            .run_with(Sink::collect())
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_sink_count() {
        let result = Source::from_vec(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
            .run_with(Sink::count())
            .await
            .unwrap();
        assert_eq!(result, 10);
    }

    #[tokio::test]
    async fn test_sink_first() {
        let result = Source::from_vec(vec![1, 2, 3, 4, 5])
            .run_with(Sink::first())
            .await
            .unwrap();
        assert_eq!(result, Some(1));
    }

    #[tokio::test]
    async fn test_sink_last() {
        let result = Source::from_vec(vec![1, 2, 3, 4, 5])
            .run_with(Sink::last())
            .await
            .unwrap();
        assert_eq!(result, Some(5));
    }

    #[tokio::test]
    async fn test_sink_fold() {
        let result = Source::from_vec(vec![1, 2, 3, 4, 5])
            .run_with(Sink::fold(0, |acc, x| acc + x))
            .await
            .unwrap();
        assert_eq!(result, 15);
    }
}

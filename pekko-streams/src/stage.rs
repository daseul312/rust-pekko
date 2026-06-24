//! Stage definitions for stream processing
//!
//! Stages are the building blocks of stream pipelines. Each stage can:
//! - Pull from upstream (request elements)
//! - Push to downstream (emit elements)
//! - Handle completion and failure

use crate::errors::StreamResult;
use async_trait::async_trait;
use std::fmt::Debug;

/// Signal sent between stages
#[derive(Debug, Clone)]
pub enum Signal {
    /// Request N more elements
    Pull(usize),
    /// Cancel the stream
    Cancel,
}

/// Element or completion sent downstream
#[derive(Debug, Clone)]
pub enum StreamElement<T> {
    /// A data element
    Element(T),
    /// Stream completed successfully
    Complete,
    /// Stream failed
    Error(String),
}

impl<T> StreamElement<T> {
    pub fn is_terminal(&self) -> bool {
        matches!(self, StreamElement::Complete | StreamElement::Error(_))
    }

    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> StreamElement<U> {
        match self {
            StreamElement::Element(t) => StreamElement::Element(f(t)),
            StreamElement::Complete => StreamElement::Complete,
            StreamElement::Error(e) => StreamElement::Error(e),
        }
    }
}

/// Inlet for receiving elements from upstream
#[async_trait]
pub trait Inlet<T: Send>: Send + Sync {
    /// Pull the next element
    async fn pull(&mut self) -> StreamResult<StreamElement<T>>;

    /// Cancel the upstream
    async fn cancel(&mut self) -> StreamResult<()>;
}

/// Outlet for sending elements downstream
#[async_trait]
pub trait Outlet<T: Send>: Send + Sync {
    /// Push an element downstream
    async fn push(&mut self, element: StreamElement<T>) -> StreamResult<()>;

    /// Complete the stream
    async fn complete(&mut self) -> StreamResult<()> {
        self.push(StreamElement::Complete).await
    }

    /// Fail the stream
    async fn fail(&mut self, error: String) -> StreamResult<()> {
        self.push(StreamElement::Error(error)).await
    }
}

/// A stage that produces elements
#[async_trait]
pub trait SourceStage<Out: Send + Sync>: Send + Sync {
    /// Pull the next element
    async fn pull(&mut self) -> StreamResult<StreamElement<Out>>;

    /// Called when downstream cancels
    async fn on_cancel(&mut self) -> StreamResult<()> {
        Ok(())
    }
}

/// A stage that transforms elements
#[async_trait]
pub trait FlowStage<In: Send + Sync, Out: Send + Sync>: Send + Sync {
    /// Process an input element
    async fn process(&mut self, element: In) -> StreamResult<Vec<Out>>;

    /// Called when upstream completes
    async fn on_complete(&mut self) -> StreamResult<Vec<Out>> {
        Ok(vec![])
    }

    /// Called on error
    async fn on_error(&mut self, error: String) -> StreamResult<()> {
        Err(crate::errors::StreamError::StageFailed(error))
    }
}

/// A stage that consumes elements
#[async_trait]
pub trait SinkStage<In: Send + Sync, Mat>: Send + Sync {
    /// Process an input element
    async fn process(&mut self, element: In) -> StreamResult<()>;

    /// Called when upstream completes, returns materialized value
    async fn on_complete(&mut self) -> StreamResult<Mat>;

    /// Called on error
    async fn on_error(&mut self, error: String) -> StreamResult<Mat>;
}

/// Buffer overflow strategy
#[derive(Debug, Clone, Copy)]
pub enum OverflowStrategy {
    /// Drop the oldest elements
    DropOldest,
    /// Drop the newest elements (incoming)
    DropNewest,
    /// Fail the stream
    Fail,
    /// Block until space is available (backpressure)
    Backpressure,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_element_map() {
        let elem: StreamElement<i32> = StreamElement::Element(5);
        let mapped = elem.map(|x| x * 2);
        
        match mapped {
            StreamElement::Element(x) => assert_eq!(x, 10),
            _ => panic!("Expected Element"),
        }
    }

    #[test]
    fn test_stream_element_terminal() {
        assert!(!StreamElement::<i32>::Element(1).is_terminal());
        assert!(StreamElement::<i32>::Complete.is_terminal());
        assert!(StreamElement::<i32>::Error("err".to_string()).is_terminal());
    }
}

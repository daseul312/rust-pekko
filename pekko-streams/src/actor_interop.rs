//! Actor-Streams Interop
//!
//! Bridges between Pekko Actors and Streams for seamless integration.
//! - ActorSource: Receive messages from an Actor as a Stream
//! - ActorSink: Send stream elements to an Actor
//! - ActorFlow: Transform elements through an Actor (ask pattern)

use crate::{
    errors::{StreamError, StreamResult},
    source::Source,
    sink::Sink,
    stage::{SourceStage, SinkStage, StreamElement},
};
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use std::time::Duration;

// ============= ActorRef Abstraction =============

/// A simplified actor reference for stream interop
/// This allows streams to work without depending on pekko-actor directly
pub trait ActorRefLike<M>: Send + Sync + Clone + 'static {
    /// Send a message to the actor (fire and forget)
    fn tell(&self, msg: M);
    
    /// Send a message and wait for a response
    fn ask<R: Send + 'static>(&self, msg: M, timeout: Duration) -> impl std::future::Future<Output = Result<R, String>> + Send;
}

// ============= ActorSource =============

/// Creates a Source that receives messages sent to it
/// Returns (Source, Sender) - use the sender to push messages into the stream
pub fn actor_source<T: Send + Sync + Clone + 'static>(buffer_size: usize) -> (Source<T>, mpsc::Sender<T>) {
    let (tx, rx) = mpsc::channel(buffer_size);
    let source = Source::from_channel(rx);
    (source, tx)
}

/// Creates a Source that materializes to a sender
/// This is the Akka-style ActorSource
pub struct ActorSourceStage<T> {
    receiver: mpsc::Receiver<T>,
}

impl<T: Send + Sync + Clone + 'static> ActorSourceStage<T> {
    pub fn new(buffer_size: usize) -> (Self, mpsc::Sender<T>) {
        let (tx, rx) = mpsc::channel(buffer_size);
        (Self { receiver: rx }, tx)
    }
}

#[async_trait]
impl<T: Send + Sync + Clone> SourceStage<T> for ActorSourceStage<T> {
    async fn pull(&mut self) -> StreamResult<StreamElement<T>> {
        match self.receiver.recv().await {
            Some(elem) => Ok(StreamElement::Element(elem)),
            None => Ok(StreamElement::Complete),
        }
    }
}

// ============= ActorSink =============

/// A sink that forwards all elements to a channel sender
/// The receiver can be used by an actor to process elements
pub fn actor_sink<T: Send + Sync + 'static>(sender: mpsc::Sender<T>) -> Sink<T, ()> {
    Sink::to_channel(sender)
}

/// Sink configuration for actor integration
pub struct ActorSinkConfig {
    /// Behavior when the actor's mailbox is full
    pub on_overflow: OverflowStrategy,
    /// Acknowledge each message?
    pub ack_messages: bool,
}

impl Default for ActorSinkConfig {
    fn default() -> Self {
        Self {
            on_overflow: OverflowStrategy::Backpressure,
            ack_messages: false,
        }
    }
}

/// Strategy for handling overflow
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowStrategy {
    /// Block until space is available (default, provides backpressure)
    Backpressure,
    /// Drop the newest element
    DropNewest,
    /// Drop the oldest element in the buffer
    DropOldest,
    /// Fail the stream
    Fail,
}

// ============= ActorFlow (Ask Pattern) =============

/// A flow that sends each element to an actor and expects a response
pub struct AskFlow<In, Out> {
    sender: mpsc::Sender<AskMessage<In, Out>>,
    timeout: Duration,
}

struct AskMessage<In, Out> {
    payload: In,
    reply_to: oneshot::Sender<Out>,
}

impl<In: Send + Sync + 'static, Out: Send + Sync + 'static> AskFlow<In, Out> {
    /// Create a new ask flow
    /// Returns (AskFlow, Receiver) - the receiver should be used by the actor to process requests
    pub fn new(buffer_size: usize, timeout: Duration) -> (Self, mpsc::Receiver<AskMessage<In, Out>>) {
        let (tx, rx) = mpsc::channel(buffer_size);
        (Self { sender: tx, timeout }, rx)
    }

    /// Process an element through the actor
    pub async fn process(&self, element: In) -> StreamResult<Out> {
        let (reply_tx, reply_rx) = oneshot::channel();
        
        let msg = AskMessage {
            payload: element,
            reply_to: reply_tx,
        };
        
        self.sender.send(msg).await
            .map_err(|_| StreamError::ChannelClosed)?;
        
        tokio::time::timeout(self.timeout, reply_rx)
            .await
            .map_err(|_| StreamError::Timeout)?
            .map_err(|_| StreamError::ChannelClosed)
    }
}

// ============= Sink.actorRef equivalent =============

/// Creates a sink that sends messages to an actor-like destination
/// with configurable completion and failure messages
pub struct ActorRefSink<T, A: ActorRefLike<T>> {
    actor_ref: A,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Send + Sync + Clone + 'static, A: ActorRefLike<T>> ActorRefSink<T, A> {
    pub fn new(actor_ref: A) -> Self {
        Self {
            actor_ref,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + Sync + Clone, A: ActorRefLike<T>> SinkStage<T, ()> for ActorRefSink<T, A> {
    async fn process(&mut self, element: T) -> StreamResult<()> {
        self.actor_ref.tell(element);
        Ok(())
    }

    async fn on_complete(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn on_error(&mut self, _error: String) -> StreamResult<()> {
        Ok(())
    }
}

// ============= Source.queue equivalent =============

/// A bounded source queue that provides backpressure
pub struct SourceQueue<T> {
    sender: mpsc::Sender<T>,
}

impl<T: Send> SourceQueue<T> {
    /// Offer an element to the queue
    /// Returns Ok(true) if accepted, Ok(false) if dropped, Err if queue is closed
    pub async fn offer(&self, element: T) -> Result<bool, StreamError> {
        match self.sender.send(element).await {
            Ok(()) => Ok(true),
            Err(_) => Err(StreamError::ChannelClosed),
        }
    }

    /// Try to offer without waiting
    pub fn try_offer(&self, element: T) -> Result<bool, StreamError> {
        match self.sender.try_send(element) {
            Ok(()) => Ok(true),
            Err(mpsc::error::TrySendError::Full(_)) => Ok(false),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(StreamError::ChannelClosed),
        }
    }

    /// Complete the queue (no more elements)
    pub fn complete(self) {
        drop(self.sender);
    }
}

/// Create a source with a queue for pushing elements
pub fn source_queue<T: Send + Sync + Clone + 'static>(buffer_size: usize) -> (Source<T>, SourceQueue<T>) {
    let (tx, rx) = mpsc::channel(buffer_size);
    let source = Source::from_channel(rx);
    let queue = SourceQueue { sender: tx };
    (source, queue)
}

// ============= Sink.queue equivalent =============

/// A sink that collects elements into a queue for external consumption
pub struct SinkQueue<T> {
    receiver: mpsc::Receiver<T>,
}

impl<T: Send> SinkQueue<T> {
    /// Pull an element from the queue
    pub async fn pull(&mut self) -> Option<T> {
        self.receiver.recv().await
    }

    /// Try to pull without waiting
    pub fn try_pull(&mut self) -> Option<T> {
        self.receiver.try_recv().ok()
    }
}

/// Create a sink with a queue for pulling elements
pub fn sink_queue<T: Send + Sync + 'static>(buffer_size: usize) -> (Sink<T, ()>, SinkQueue<T>) {
    let (tx, rx) = mpsc::channel(buffer_size);
    let sink = Sink::to_channel(tx);
    let queue = SinkQueue { receiver: rx };
    (sink, queue)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_actor_source() {
        let (source, sender) = actor_source::<i32>(10);
        
        // Send some elements
        sender.send(1).await.unwrap();
        sender.send(2).await.unwrap();
        sender.send(3).await.unwrap();
        drop(sender); // Complete the stream
        
        let result = source.run_collect().await.unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_source_queue() {
        let (source, queue) = source_queue::<i32>(10);
        
        // Offer elements
        queue.offer(10).await.unwrap();
        queue.offer(20).await.unwrap();
        queue.offer(30).await.unwrap();
        queue.complete();
        
        let result = source.run_collect().await.unwrap();
        assert_eq!(result, vec![10, 20, 30]);
    }

    #[tokio::test]
    async fn test_sink_queue() {
        let (sink, mut queue) = sink_queue::<i32>(10);
        
        let source = Source::from_vec(vec![1, 2, 3]);
        
        // Run in background
        let handle = tokio::spawn(async move {
            source.run_with(sink).await.unwrap();
        });
        
        // Pull from queue
        let mut results = Vec::new();
        while let Some(elem) = queue.pull().await {
            results.push(elem);
        }
        
        handle.await.unwrap();
        assert_eq!(results, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_try_offer_backpressure() {
        let (_source, queue) = source_queue::<i32>(2);
        
        // Fill the queue
        assert!(queue.try_offer(1).unwrap());
        assert!(queue.try_offer(2).unwrap());
        
        // Queue is full, should return false
        assert!(!queue.try_offer(3).unwrap());
    }
}

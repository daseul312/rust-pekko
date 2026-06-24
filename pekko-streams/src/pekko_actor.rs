//! Integration with pekko-actor
//!
//! This module provides integration patterns for connecting Pekko Actors with Streams.
//! The integration is done through channels, allowing actors to send/receive stream elements.
//!
//! ## Patterns
//!
//! 1. **Actor as Source**: Actor sends messages into a stream
//! 2. **Actor as Sink**: Stream sends elements to an actor  
//! 3. **Actor as Flow**: Actor transforms elements (ask pattern)
//!
//! ## Example
//!
//! ```ignore
//! use pekko_streams::pekko_actor::{ActorSourceBuilder, ActorSinkBuilder};
//!
//! // Actor sends into stream
//! let (source, sender) = ActorSourceBuilder::new(100).build();
//! actor.tell(RegisterSender(sender));
//!
//! // Stream sends to actor
//! let (sink, receiver) = ActorSinkBuilder::new(100).build();
//! spawn_actor_with_receiver(receiver);
//! ```

use crate::{
    source::Source,
    sink::Sink,
    actor_interop::{source_queue, sink_queue, SourceQueue, SinkQueue},
};
use tokio::sync::mpsc;

/// Builder for creating a Source that receives from actors
pub struct ActorSourceBuilder {
    buffer_size: usize,
}

impl ActorSourceBuilder {
    pub fn new(buffer_size: usize) -> Self {
        Self { buffer_size }
    }

    /// Build the source
    /// Returns (Source, Sender) - give the Sender to your actor
    pub fn build<T: Send + Sync + Clone + 'static>(self) -> (Source<T>, mpsc::Sender<T>) {
        let (tx, rx) = mpsc::channel(self.buffer_size);
        let source = Source::from_channel(rx);
        (source, tx)
    }

    /// Build with a SourceQueue for more control
    pub fn build_queue<T: Send + Sync + Clone + 'static>(self) -> (Source<T>, SourceQueue<T>) {
        source_queue(self.buffer_size)
    }
}

/// Builder for creating a Sink that sends to actors
pub struct ActorSinkBuilder {
    buffer_size: usize,
}

impl ActorSinkBuilder {
    pub fn new(buffer_size: usize) -> Self {
        Self { buffer_size }
    }

    /// Build the sink
    /// Returns (Sink, Receiver) - use the Receiver in your actor
    pub fn build<T: Send + Sync + 'static>(self) -> (Sink<T, ()>, mpsc::Receiver<T>) {
        let (tx, rx) = mpsc::channel(self.buffer_size);
        let sink = Sink::to_channel(tx);
        (sink, rx)
    }

    /// Build with a SinkQueue for more control
    pub fn build_queue<T: Send + Sync + 'static>(self) -> (Sink<T, ()>, SinkQueue<T>) {
        sink_queue(self.buffer_size)
    }
}

/// Convenience functions
pub mod ext {
    use super::*;

    /// Create a source that actors can send to
    pub fn actor_source<T: Send + Sync + Clone + 'static>(
        buffer_size: usize,
    ) -> (Source<T>, mpsc::Sender<T>) {
        ActorSourceBuilder::new(buffer_size).build()
    }

    /// Create a sink that sends to actors
    pub fn actor_sink<T: Send + Sync + 'static>(
        buffer_size: usize,
    ) -> (Sink<T, ()>, mpsc::Receiver<T>) {
        ActorSinkBuilder::new(buffer_size).build()
    }
}

pub use ext::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_actor_source_builder() {
        let (source, sender) = ActorSourceBuilder::new(10).build::<i32>();
        
        sender.send(1).await.unwrap();
        sender.send(2).await.unwrap();
        sender.send(3).await.unwrap();
        drop(sender);
        
        let result = source.run_collect().await.unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_actor_sink_builder() {
        let (sink, mut receiver) = ActorSinkBuilder::new(10).build::<i32>();
        
        let source = Source::from_vec(vec![1, 2, 3]);
        
        let handle = tokio::spawn(async move {
            source.run_with(sink).await.unwrap();
        });
        
        let mut results = Vec::new();
        while let Some(elem) = receiver.recv().await {
            results.push(elem);
        }
        
        handle.await.unwrap();
        assert_eq!(results, vec![1, 2, 3]);
    }
}

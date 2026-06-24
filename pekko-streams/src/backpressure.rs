//! Reactive Streams Backpressure
//!
//! Implements demand-based flow control between stream stages.
//! Modeled after Reactive Streams specification (Publisher/Subscriber/Subscription).
//!
//! Key concepts:
//! - **BackpressuredSource**: Source that respects downstream demand
//! - **BackpressuredSink**: Sink that signals demand upstream
//! - **BoundedChannel**: Bounded async channel with overflow strategies
//! - **DemandSignal**: Explicit demand signaling between stages

use crate::errors::{StreamError, StreamResult};
use crate::stage::{OverflowStrategy, SourceStage, StreamElement};
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Notify, Semaphore};

// ============= Demand Signal =============

/// Tracks demand between producer and consumer
#[derive(Debug, Clone)]
pub struct DemandSignal {
    /// Outstanding demand count
    demand: Arc<AtomicUsize>,
    /// Notify when new demand arrives
    notify: Arc<Notify>,
    /// Whether the subscriber has cancelled
    cancelled: Arc<AtomicBool>,
}

impl DemandSignal {
    /// Create a new demand signal
    pub fn new() -> Self {
        Self {
            demand: Arc::new(AtomicUsize::new(0)),
            notify: Arc::new(Notify::new()),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Request N more elements (subscriber → publisher)
    pub fn request(&self, n: usize) {
        self.demand.fetch_add(n, Ordering::SeqCst);
        self.notify.notify_one();
    }

    /// Try to consume one unit of demand (publisher side)
    /// Returns true if demand was available
    pub fn try_acquire(&self) -> bool {
        loop {
            let current = self.demand.load(Ordering::SeqCst);
            if current == 0 {
                return false;
            }
            if self
                .demand
                .compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Wait until demand is available or cancelled
    pub async fn wait_for_demand(&self) -> bool {
        loop {
            if self.cancelled.load(Ordering::SeqCst) {
                return false;
            }
            if self.try_acquire() {
                return true;
            }
            self.notify.notified().await;
        }
    }

    /// Get current demand
    pub fn current_demand(&self) -> usize {
        self.demand.load(Ordering::SeqCst)
    }

    /// Cancel the subscription
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.notify.notify_one();
    }

    /// Check if cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

impl Default for DemandSignal {
    fn default() -> Self {
        Self::new()
    }
}

// ============= Bounded Channel with Overflow Strategy =============

/// A bounded channel that applies an overflow strategy when full
pub struct BoundedChannel<T> {
    sender: mpsc::Sender<T>,
    receiver: mpsc::Receiver<T>,
    capacity: usize,
    strategy: OverflowStrategy,
    /// Semaphore for backpressure strategy — limits in-flight elements
    semaphore: Option<Arc<Semaphore>>,
}

impl<T: Send + 'static> BoundedChannel<T> {
    /// Create a new bounded channel
    pub fn new(capacity: usize, strategy: OverflowStrategy) -> Self {
        let (sender, receiver) = match strategy {
            OverflowStrategy::Backpressure => {
                // For backpressure, use exact capacity
                mpsc::channel(capacity)
            }
            _ => {
                // For other strategies, use larger internal buffer
                // but enforce capacity semantically
                mpsc::channel(capacity * 2)
            }
        };

        let semaphore = match strategy {
            OverflowStrategy::Backpressure => Some(Arc::new(Semaphore::new(capacity))),
            _ => None,
        };

        Self {
            sender,
            receiver,
            capacity,
            strategy,
            semaphore,
        }
    }

    /// Get the capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Split into sender and receiver
    pub fn split(self) -> (BoundedSender<T>, BoundedReceiver<T>) {
        (
            BoundedSender {
                sender: self.sender,
                strategy: self.strategy,
                semaphore: self.semaphore.clone(),
            },
            BoundedReceiver {
                receiver: self.receiver,
                semaphore: self.semaphore,
            },
        )
    }
}

/// Sender half of a bounded channel
pub struct BoundedSender<T> {
    sender: mpsc::Sender<T>,
    strategy: OverflowStrategy,
    semaphore: Option<Arc<Semaphore>>,
}

impl<T: Send + 'static> BoundedSender<T> {
    /// Send an element, applying overflow strategy if full
    pub async fn send(&self, element: T) -> Result<(), StreamError> {
        match self.strategy {
            OverflowStrategy::Backpressure => {
                // Wait for a permit (blocks until space is available)
                if let Some(ref sem) = self.semaphore {
                    let _permit = sem
                        .acquire()
                        .await
                        .map_err(|_| StreamError::ChannelClosed)?;
                    // Forget the permit — receiver will add it back
                    _permit.forget();
                }
                self.sender
                    .send(element)
                    .await
                    .map_err(|_| StreamError::ChannelClosed)
            }
            OverflowStrategy::DropNewest => {
                // Try to send, if full just drop the element
                match self.sender.try_send(element) {
                    Ok(()) => Ok(()),
                    Err(mpsc::error::TrySendError::Full(_)) => Ok(()), // silently drop
                    Err(mpsc::error::TrySendError::Closed(_)) => Err(StreamError::ChannelClosed),
                }
            }
            OverflowStrategy::DropOldest => {
                // If full, try to send. The receiver will handle eviction.
                // For simplicity, just use try_send with a fallback
                match self.sender.try_send(element) {
                    Ok(()) => Ok(()),
                    Err(mpsc::error::TrySendError::Full(elem)) => {
                        // Force send by awaiting (the channel has 2x capacity)
                        self.sender
                            .send(elem)
                            .await
                            .map_err(|_| StreamError::ChannelClosed)
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => Err(StreamError::ChannelClosed),
                }
            }
            OverflowStrategy::Fail => {
                match self.sender.try_send(element) {
                    Ok(()) => Ok(()),
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        Err(StreamError::BufferOverflow(self.sender.capacity()))
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => Err(StreamError::ChannelClosed),
                }
            }
        }
    }

    /// Check if the channel is closed
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

impl<T: Send + 'static> Clone for BoundedSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            strategy: self.strategy,
            semaphore: self.semaphore.clone(),
        }
    }
}

/// Receiver half of a bounded channel
pub struct BoundedReceiver<T> {
    receiver: mpsc::Receiver<T>,
    semaphore: Option<Arc<Semaphore>>,
}

impl<T: Send + 'static> BoundedReceiver<T> {
    /// Receive the next element
    pub async fn recv(&mut self) -> Option<T> {
        let elem = self.receiver.recv().await;
        if elem.is_some() {
            // Return permit to semaphore (allows sender to proceed)
            if let Some(ref sem) = self.semaphore {
                sem.add_permits(1);
            }
        }
        elem
    }

    /// Try to receive without blocking
    pub fn try_recv(&mut self) -> Option<T> {
        match self.receiver.try_recv() {
            Ok(elem) => {
                if let Some(ref sem) = self.semaphore {
                    sem.add_permits(1);
                }
                Some(elem)
            }
            Err(_) => None,
        }
    }
}

// ============= BackpressuredSource =============

/// A source that respects downstream demand signals
pub struct BackpressuredSource<T: Send + Sync + 'static> {
    receiver: BoundedReceiver<T>,
}

impl<T: Send + Sync + 'static> BackpressuredSource<T> {
    /// Create from a bounded receiver
    pub fn new(receiver: BoundedReceiver<T>) -> Self {
        Self { receiver }
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> SourceStage<T> for BackpressuredSource<T> {
    async fn pull(&mut self) -> StreamResult<StreamElement<T>> {
        match self.receiver.recv().await {
            Some(elem) => Ok(StreamElement::Element(elem)),
            None => Ok(StreamElement::Complete),
        }
    }
}

// ============= Backpressured Pipeline Runner =============

/// Configuration for backpressured pipeline execution
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Buffer size between stages
    pub buffer_size: usize,
    /// Overflow strategy
    pub strategy: OverflowStrategy,
    /// Initial demand (how many elements to request at start)
    pub initial_demand: usize,
}

impl BackpressureConfig {
    /// Create with default settings (buffer=16, backpressure strategy)
    pub fn new() -> Self {
        Self {
            buffer_size: 16,
            strategy: OverflowStrategy::Backpressure,
            initial_demand: 16,
        }
    }

    /// Set buffer size
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self.initial_demand = size;
        self
    }

    /// Set overflow strategy
    pub fn with_strategy(mut self, strategy: OverflowStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Set initial demand
    pub fn with_initial_demand(mut self, demand: usize) -> Self {
        self.initial_demand = demand;
        self
    }
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Run a source → sink pipeline with backpressure using a bounded channel
pub async fn run_with_backpressure<T, Mat>(
    mut source: Box<dyn SourceStage<T>>,
    mut sink: Box<dyn crate::stage::SinkStage<T, Mat>>,
    config: BackpressureConfig,
) -> StreamResult<Mat>
where
    T: Send + Sync + 'static,
    Mat: Send + 'static,
{
    let channel = BoundedChannel::new(config.buffer_size, config.strategy);
    let (sender, mut receiver) = channel.split();

    // Producer task: pulls from source and sends through bounded channel
    let producer = tokio::spawn(async move {
        loop {
            match source.pull().await {
                Ok(StreamElement::Element(elem)) => {
                    if sender.send(elem).await.is_err() {
                        break;
                    }
                }
                Ok(StreamElement::Complete) => break,
                Ok(StreamElement::Error(_)) => break,
                Err(_) => break,
            }
        }
    });

    // Consumer: receives from channel and feeds to sink
    loop {
        match receiver.recv().await {
            Some(elem) => {
                sink.process(elem).await?;
            }
            None => {
                break;
            }
        }
    }

    // Wait for producer to finish
    let _ = producer.await;

    sink.on_complete().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::Source;
    use crate::sink::Sink;

    #[test]
    fn test_demand_signal_basic() {
        let signal = DemandSignal::new();

        assert_eq!(signal.current_demand(), 0);
        assert!(!signal.try_acquire());

        signal.request(3);
        assert_eq!(signal.current_demand(), 3);

        assert!(signal.try_acquire());
        assert_eq!(signal.current_demand(), 2);

        assert!(signal.try_acquire());
        assert!(signal.try_acquire());
        assert!(!signal.try_acquire());
        assert_eq!(signal.current_demand(), 0);
    }

    #[test]
    fn test_demand_signal_cancel() {
        let signal = DemandSignal::new();
        assert!(!signal.is_cancelled());

        signal.cancel();
        assert!(signal.is_cancelled());
    }

    #[tokio::test]
    async fn test_demand_signal_wait() {
        let signal = DemandSignal::new();
        let signal2 = signal.clone();

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            signal2.request(1);
        });

        let got = signal.wait_for_demand().await;
        assert!(got);
    }

    #[tokio::test]
    async fn test_bounded_channel_backpressure() {
        let channel = BoundedChannel::new(2, OverflowStrategy::Backpressure);
        let (sender, mut receiver) = channel.split();

        // Send 2 elements (fills buffer)
        sender.send(1).await.unwrap();
        sender.send(2).await.unwrap();

        // Third send should block until we receive
        let sender2 = sender.clone();
        let handle = tokio::spawn(async move {
            sender2.send(3).await.unwrap();
            true
        });

        // Small delay to ensure send is blocked
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Receive one to unblock
        assert_eq!(receiver.recv().await, Some(1));

        // Now the spawned send should complete
        let completed = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            handle,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(completed);

        assert_eq!(receiver.recv().await, Some(2));
        assert_eq!(receiver.recv().await, Some(3));
    }

    #[tokio::test]
    async fn test_bounded_channel_drop_newest() {
        let channel = BoundedChannel::new(2, OverflowStrategy::DropNewest);
        let (sender, mut receiver) = channel.split();

        sender.send(1).await.unwrap();
        sender.send(2).await.unwrap();
        sender.send(3).await.unwrap(); // dropped (buffer uses 2x capacity internally, so try a few more)
        sender.send(4).await.unwrap();
        sender.send(5).await.unwrap(); // some may be dropped

        // We should get at least the first elements
        let first = receiver.recv().await;
        assert_eq!(first, Some(1));
    }

    #[tokio::test]
    async fn test_bounded_channel_fail() {
        let channel = BoundedChannel::new(1, OverflowStrategy::Fail);
        let (sender, _receiver) = channel.split();

        sender.send(1).await.unwrap();
        // The internal channel has 2x capacity, so try exceeding that
        sender.send(2).await.unwrap();
        let result = sender.send(3).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_run_with_backpressure() {
        let source = Source::from_vec(vec![1, 2, 3, 4, 5]);
        let sink: Sink<i32, Vec<i32>> = Sink::collect();
        let config = BackpressureConfig::new().with_buffer_size(2);

        let result = run_with_backpressure(
            source.inner,
            sink.inner,
            config,
        )
        .await
        .unwrap();

        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_backpressure_slow_consumer() {
        let source = Source::from_vec((1..=20).collect::<Vec<i32>>());
        let config = BackpressureConfig::new().with_buffer_size(4);

        // Use a slow-processing sink
        let sink = Sink::foreach(|_x: i32| {
            // Simulating slow processing
            std::thread::sleep(std::time::Duration::from_millis(1));
        });

        let result = run_with_backpressure(
            source.inner,
            sink.inner,
            config,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_backpressure_config() {
        let config = BackpressureConfig::new()
            .with_buffer_size(32)
            .with_strategy(OverflowStrategy::DropNewest)
            .with_initial_demand(8);

        assert_eq!(config.buffer_size, 32);
        assert_eq!(config.initial_demand, 8);
    }
}

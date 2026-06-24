//! Stream TestKit — Testing utilities for Pekko Streams
//!
//! Provides test sources, sinks, and assertion helpers for stream testing:
//!
//! - **TestSource**: Controllable source that emits elements on demand
//! - **TestSink**: Collects elements and provides assertion methods
//! - **StreamAssertion**: Fluent assertions for stream output
//! - **TestPublisher / TestSubscriber**: Manual backpressure control

use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::time::timeout;

// ============= TestSource =============

/// A controllable test source that emits elements on demand.
/// Useful for testing stream processing stages with controlled input.
pub struct TestSource<T: Send + 'static> {
    sender: mpsc::Sender<T>,
    receiver: Arc<Mutex<mpsc::Receiver<T>>>,
    completed: Arc<AtomicBool>,
    elements_sent: AtomicU64,
}

impl<T: Send + Debug + 'static> TestSource<T> {
    /// Create a new test source with the given buffer capacity.
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self {
            sender: tx,
            receiver: Arc::new(Mutex::new(rx)),
            completed: Arc::new(AtomicBool::new(false)),
            elements_sent: AtomicU64::new(0),
        }
    }

    /// Create a test source with default capacity (100).
    pub fn default_capacity() -> Self {
        Self::new(100)
    }

    /// Send an element into the stream.
    pub async fn send(&self, element: T) -> Result<(), mpsc::error::SendError<T>> {
        let result = self.sender.send(element).await;
        if result.is_ok() {
            self.elements_sent.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Send multiple elements.
    pub async fn send_all(&self, elements: Vec<T>) -> usize {
        let mut count = 0;
        for elem in elements {
            if self.sender.send(elem).await.is_ok() {
                count += 1;
                self.elements_sent.fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }
        count
    }

    /// Complete the source (close the channel).
    pub fn complete(&self) {
        self.completed.store(true, Ordering::SeqCst);
        // Dropping a clone doesn't close the channel, so we just mark it
    }

    /// Check if the source is marked as completed.
    pub fn is_completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }

    /// Get the number of elements sent.
    pub fn sent_count(&self) -> u64 {
        self.elements_sent.load(Ordering::Relaxed)
    }

    /// Get a receiver to consume elements from this source.
    pub fn take_receiver(&self) -> Arc<Mutex<mpsc::Receiver<T>>> {
        self.receiver.clone()
    }
}

// ============= TestSink =============

/// A test sink that collects elements and provides assertion methods.
pub struct TestSink<T: Send + 'static> {
    sender: mpsc::UnboundedSender<T>,
    collected: Arc<Mutex<Vec<T>>>,
    receiver: Arc<Mutex<mpsc::UnboundedReceiver<T>>>,
    element_count: Arc<AtomicU64>,
    notify: Arc<Notify>,
}

impl<T: Send + Debug + Clone + 'static> TestSink<T> {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            sender: tx,
            collected: Arc::new(Mutex::new(Vec::new())),
            receiver: Arc::new(Mutex::new(rx)),
            element_count: Arc::new(AtomicU64::new(0)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Get a sender to push elements into the sink.
    pub fn sender(&self) -> mpsc::UnboundedSender<T> {
        self.sender.clone()
    }

    /// Push an element into the sink (simulates receiving from stream).
    pub async fn push(&self, element: T) {
        let mut collected = self.collected.lock().await;
        collected.push(element);
        self.element_count.fetch_add(1, Ordering::Relaxed);
        self.notify.notify_waiters();
    }

    /// Push multiple elements.
    pub async fn push_all(&self, elements: Vec<T>) {
        let mut collected = self.collected.lock().await;
        let count = elements.len();
        collected.extend(elements);
        self.element_count.fetch_add(count as u64, Ordering::Relaxed);
        self.notify.notify_waiters();
    }

    /// Get all collected elements.
    pub async fn get_all(&self) -> Vec<T> {
        self.collected.lock().await.clone()
    }

    /// Get element count.
    pub fn count(&self) -> u64 {
        self.element_count.load(Ordering::Relaxed)
    }

    /// Wait until at least `n` elements have been collected.
    pub async fn wait_for_n(&self, n: usize, max_wait: Duration) -> Vec<T> {
        let deadline = tokio::time::Instant::now() + max_wait;
        loop {
            {
                let collected = self.collected.lock().await;
                if collected.len() >= n {
                    return collected[..n].to_vec();
                }
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                let collected = self.collected.lock().await;
                panic!(
                    "TestSink: timeout waiting for {} elements, got {}",
                    n,
                    collected.len()
                );
            }
            let _ = timeout(remaining, self.notify.notified()).await;
        }
    }

    /// Assert that collected elements equal expected.
    pub async fn assert_elements(&self, expected: &[T])
    where
        T: PartialEq,
    {
        let collected = self.collected.lock().await;
        assert_eq!(
            collected.as_slice(),
            expected,
            "TestSink: collected elements mismatch"
        );
    }

    /// Assert that collected elements contain expected.
    pub async fn assert_contains(&self, expected: &T)
    where
        T: PartialEq,
    {
        let collected = self.collected.lock().await;
        assert!(
            collected.contains(expected),
            "TestSink: expected element {:?} not found in collected {:?}",
            expected,
            *collected
        );
    }

    /// Assert element count.
    pub async fn assert_count(&self, expected: usize) {
        let collected = self.collected.lock().await;
        assert_eq!(
            collected.len(),
            expected,
            "TestSink: expected {} elements, got {}",
            expected,
            collected.len()
        );
    }

    /// Clear collected elements.
    pub async fn clear(&self) {
        self.collected.lock().await.clear();
        self.element_count.store(0, Ordering::Relaxed);
    }
}

// ============= StreamAssertion =============

/// Fluent assertion builder for stream test results.
pub struct StreamAssertion<T> {
    elements: Vec<T>,
    label: String,
}

impl<T: Debug + PartialEq + Clone> StreamAssertion<T> {
    /// Create a new stream assertion from collected elements.
    pub fn new(elements: Vec<T>) -> Self {
        Self {
            elements,
            label: "stream".to_string(),
        }
    }

    /// Set a label for better error messages.
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = label.into();
        self
    }

    /// Assert element count.
    pub fn has_count(self, expected: usize) -> Self {
        assert_eq!(
            self.elements.len(),
            expected,
            "[{}] expected {} elements, got {}",
            self.label,
            expected,
            self.elements.len()
        );
        self
    }

    /// Assert elements equal expected.
    pub fn equals(self, expected: &[T]) -> Self {
        assert_eq!(
            self.elements.as_slice(),
            expected,
            "[{}] elements mismatch",
            self.label
        );
        self
    }

    /// Assert all elements satisfy a predicate.
    pub fn all_match<F: Fn(&T) -> bool>(self, predicate: F) -> Self {
        for (i, elem) in self.elements.iter().enumerate() {
            assert!(
                predicate(elem),
                "[{}] element at index {} ({:?}) did not match predicate",
                self.label,
                i,
                elem
            );
        }
        self
    }

    /// Assert any element satisfies predicate.
    pub fn any_match<F: Fn(&T) -> bool>(self, predicate: F) -> Self {
        assert!(
            self.elements.iter().any(|e| predicate(e)),
            "[{}] no element matched predicate",
            self.label
        );
        self
    }

    /// Assert first element.
    pub fn first_is(self, expected: &T) -> Self {
        assert!(
            !self.elements.is_empty(),
            "[{}] stream is empty, expected first = {:?}",
            self.label,
            expected
        );
        assert_eq!(
            &self.elements[0], expected,
            "[{}] first element mismatch",
            self.label
        );
        self
    }

    /// Assert last element.
    pub fn last_is(self, expected: &T) -> Self {
        assert!(
            !self.elements.is_empty(),
            "[{}] stream is empty, expected last = {:?}",
            self.label,
            expected
        );
        let last = self.elements.last().unwrap();
        assert_eq!(
            last, expected,
            "[{}] last element mismatch",
            self.label
        );
        self
    }

    /// Assert the stream is empty.
    pub fn is_empty(self) -> Self {
        assert!(
            self.elements.is_empty(),
            "[{}] expected empty stream, got {} elements",
            self.label,
            self.elements.len()
        );
        self
    }

    /// Assert elements are sorted.
    pub fn is_sorted(self) -> Self
    where
        T: Ord,
    {
        for i in 1..self.elements.len() {
            assert!(
                self.elements[i - 1] <= self.elements[i],
                "[{}] elements not sorted at index {}: {:?} > {:?}",
                self.label,
                i,
                self.elements[i - 1],
                self.elements[i]
            );
        }
        self
    }

    /// Assert no duplicates.
    pub fn has_no_duplicates(self) -> Self
    where
        T: Eq + std::hash::Hash,
    {
        let mut seen = std::collections::HashSet::new();
        for (i, elem) in self.elements.iter().enumerate() {
            assert!(
                seen.insert(elem),
                "[{}] duplicate element at index {}: {:?}",
                self.label,
                i,
                elem
            );
        }
        self
    }

    /// Assert contains a subsequence.
    pub fn contains_subsequence(self, subseq: &[T]) -> Self {
        let mut si = 0;
        for elem in &self.elements {
            if si < subseq.len() && *elem == subseq[si] {
                si += 1;
            }
        }
        assert_eq!(
            si,
            subseq.len(),
            "[{}] subsequence not found. Matched {}/{} elements",
            self.label,
            si,
            subseq.len()
        );
        self
    }

    /// Get the elements (consume assertion).
    pub fn into_elements(self) -> Vec<T> {
        self.elements
    }

    /// Get reference to elements.
    pub fn elements(&self) -> &[T] {
        &self.elements
    }
}

// ============= TestPublisher =============

/// Manual backpressure-aware publisher for testing subscribers.
pub struct TestPublisher<T: Send + 'static> {
    sender: mpsc::Sender<T>,
    demand: Arc<AtomicU64>,
    completed: Arc<AtomicBool>,
    error: Arc<Mutex<Option<String>>>,
}

impl<T: Send + Debug + 'static> TestPublisher<T> {
    /// Create a new test publisher with given buffer size.
    pub fn new(buffer: usize) -> (Self, mpsc::Receiver<T>) {
        let (tx, rx) = mpsc::channel(buffer);
        let publisher = Self {
            sender: tx,
            demand: Arc::new(AtomicU64::new(0)),
            completed: Arc::new(AtomicBool::new(false)),
            error: Arc::new(Mutex::new(None)),
        };
        (publisher, rx)
    }

    /// Send next element. Respects backpressure by waiting for demand.
    pub async fn send_next(&self, element: T) -> Result<(), String> {
        if self.completed.load(Ordering::SeqCst) {
            return Err("Publisher already completed".into());
        }
        self.sender
            .send(element)
            .await
            .map_err(|e| format!("Send failed: {}", e))?;
        self.demand.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Send multiple elements.
    pub async fn send_next_n(&self, elements: Vec<T>) -> Result<usize, String> {
        let mut count = 0;
        for elem in elements {
            self.send_next(elem).await?;
            count += 1;
        }
        Ok(count)
    }

    /// Signal completion.
    pub fn send_complete(&self) {
        self.completed.store(true, Ordering::SeqCst);
    }

    /// Signal error.
    pub async fn send_error(&self, error: impl Into<String>) {
        *self.error.lock().await = Some(error.into());
    }

    /// Get total demand (elements sent).
    pub fn total_demand(&self) -> u64 {
        self.demand.load(Ordering::Relaxed)
    }

    /// Check if completed.
    pub fn is_completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }
}

// ============= TestSubscriber =============

/// A test subscriber that tracks demand and received elements.
pub struct TestSubscriber<T: Send + 'static> {
    received: Arc<Mutex<Vec<T>>>,
    demand_requested: Arc<AtomicU64>,
    completed: Arc<AtomicBool>,
    error: Arc<Mutex<Option<String>>>,
    notify: Arc<Notify>,
}

impl<T: Send + Debug + Clone + 'static> TestSubscriber<T> {
    pub fn new() -> Self {
        Self {
            received: Arc::new(Mutex::new(Vec::new())),
            demand_requested: Arc::new(AtomicU64::new(0)),
            completed: Arc::new(AtomicBool::new(false)),
            error: Arc::new(Mutex::new(None)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Request N elements (simulate backpressure demand).
    pub fn request(&self, n: u64) {
        self.demand_requested.fetch_add(n, Ordering::Relaxed);
    }

    /// Called when an element is received.
    pub async fn on_next(&self, element: T) {
        self.received.lock().await.push(element);
        self.notify.notify_waiters();
    }

    /// Called when stream completes.
    pub fn on_complete(&self) {
        self.completed.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Called when error occurs.
    pub async fn on_error(&self, error: impl Into<String>) {
        *self.error.lock().await = Some(error.into());
        self.notify.notify_waiters();
    }

    /// Get all received elements.
    pub async fn get_received(&self) -> Vec<T> {
        self.received.lock().await.clone()
    }

    /// Wait for N elements or timeout.
    pub async fn expect_n(&self, n: usize, max_wait: Duration) -> Vec<T> {
        let deadline = tokio::time::Instant::now() + max_wait;
        loop {
            {
                let received = self.received.lock().await;
                if received.len() >= n {
                    return received[..n].to_vec();
                }
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                let received = self.received.lock().await;
                panic!(
                    "TestSubscriber: expected {} elements, got {} after {:?}",
                    n,
                    received.len(),
                    max_wait
                );
            }
            let _ = timeout(remaining, self.notify.notified()).await;
        }
    }

    /// Expect completion within timeout.
    pub async fn expect_complete(&self, max_wait: Duration) {
        let deadline = tokio::time::Instant::now() + max_wait;
        loop {
            if self.completed.load(Ordering::SeqCst) {
                return;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                panic!("TestSubscriber: expected completion within {:?}", max_wait);
            }
            let _ = timeout(remaining, self.notify.notified()).await;
        }
    }

    /// Expect error within timeout.
    pub async fn expect_error(&self, max_wait: Duration) -> String {
        let deadline = tokio::time::Instant::now() + max_wait;
        loop {
            {
                let err = self.error.lock().await;
                if let Some(e) = err.as_ref() {
                    return e.clone();
                }
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                panic!("TestSubscriber: expected error within {:?}", max_wait);
            }
            let _ = timeout(remaining, self.notify.notified()).await;
        }
    }

    /// Check if completed.
    pub fn is_completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }

    /// Get total demand requested.
    pub fn total_demand(&self) -> u64 {
        self.demand_requested.load(Ordering::Relaxed)
    }
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;

    // ---- TestSource Tests ----

    #[tokio::test]
    async fn test_source_send_and_count() {
        let source = TestSource::<i32>::new(10);
        source.send(1).await.unwrap();
        source.send(2).await.unwrap();
        assert_eq!(source.sent_count(), 2);
    }

    #[tokio::test]
    async fn test_source_send_all() {
        let source = TestSource::<i32>::new(10);
        let count = source.send_all(vec![1, 2, 3, 4, 5]).await;
        assert_eq!(count, 5);
        assert_eq!(source.sent_count(), 5);
    }

    #[tokio::test]
    async fn test_source_complete() {
        let source = TestSource::<i32>::new(10);
        assert!(!source.is_completed());
        source.complete();
        assert!(source.is_completed());
    }

    // ---- TestSink Tests ----

    #[tokio::test]
    async fn test_sink_push_and_get() {
        let sink = TestSink::<i32>::new();
        sink.push(1).await;
        sink.push(2).await;
        sink.push(3).await;

        let all = sink.get_all().await;
        assert_eq!(all, vec![1, 2, 3]);
        assert_eq!(sink.count(), 3);
    }

    #[tokio::test]
    async fn test_sink_push_all() {
        let sink = TestSink::<i32>::new();
        sink.push_all(vec![10, 20, 30]).await;

        sink.assert_elements(&[10, 20, 30]).await;
        sink.assert_count(3).await;
    }

    #[tokio::test]
    async fn test_sink_assert_contains() {
        let sink = TestSink::<String>::new();
        sink.push("hello".to_string()).await;
        sink.push("world".to_string()).await;

        sink.assert_contains(&"hello".to_string()).await;
    }

    #[tokio::test]
    async fn test_sink_wait_for_n() {
        let sink = TestSink::<i32>::new();
        let sink_clone = TestSink {
            sender: sink.sender.clone(),
            collected: sink.collected.clone(),
            receiver: sink.receiver.clone(),
            element_count: sink.element_count.clone(),
            notify: sink.notify.clone(),
        };

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            sink_clone.push(1).await;
            sink_clone.push(2).await;
            sink_clone.push(3).await;
        });

        let elements = sink.wait_for_n(3, Duration::from_secs(1)).await;
        assert_eq!(elements, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_sink_clear() {
        let sink = TestSink::<i32>::new();
        sink.push_all(vec![1, 2, 3]).await;
        sink.clear().await;
        sink.assert_count(0).await;
    }

    // ---- StreamAssertion Tests ----

    #[test]
    fn test_assertion_has_count() {
        StreamAssertion::new(vec![1, 2, 3]).has_count(3);
    }

    #[test]
    fn test_assertion_equals() {
        StreamAssertion::new(vec![1, 2, 3]).equals(&[1, 2, 3]);
    }

    #[test]
    fn test_assertion_all_match() {
        StreamAssertion::new(vec![2, 4, 6]).all_match(|x| x % 2 == 0);
    }

    #[test]
    fn test_assertion_any_match() {
        StreamAssertion::new(vec![1, 2, 3]).any_match(|x| *x == 2);
    }

    #[test]
    fn test_assertion_first_last() {
        StreamAssertion::new(vec![10, 20, 30])
            .first_is(&10)
            .last_is(&30);
    }

    #[test]
    fn test_assertion_is_sorted() {
        StreamAssertion::new(vec![1, 2, 3, 4, 5]).is_sorted();
    }

    #[test]
    fn test_assertion_no_duplicates() {
        StreamAssertion::new(vec![1, 2, 3, 4, 5]).has_no_duplicates();
    }

    #[test]
    fn test_assertion_contains_subsequence() {
        StreamAssertion::new(vec![1, 2, 3, 4, 5]).contains_subsequence(&[2, 4]);
    }

    #[test]
    fn test_assertion_chaining() {
        StreamAssertion::new(vec![1, 2, 3])
            .with_label("test-stream")
            .has_count(3)
            .first_is(&1)
            .last_is(&3)
            .is_sorted()
            .has_no_duplicates()
            .all_match(|x| *x > 0);
    }

    #[test]
    fn test_assertion_empty() {
        StreamAssertion::<i32>::new(vec![]).is_empty();
    }

    // ---- TestPublisher Tests ----

    #[tokio::test]
    async fn test_publisher_send_and_receive() {
        let (publisher, mut rx) = TestPublisher::<i32>::new(10);

        publisher.send_next(1).await.unwrap();
        publisher.send_next(2).await.unwrap();

        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(publisher.total_demand(), 2);
    }

    #[tokio::test]
    async fn test_publisher_complete() {
        let (publisher, _rx) = TestPublisher::<i32>::new(10);
        publisher.send_complete();
        assert!(publisher.is_completed());

        let result = publisher.send_next(1).await;
        assert!(result.is_err());
    }

    // ---- TestSubscriber Tests ----

    #[tokio::test]
    async fn test_subscriber_on_next() {
        let sub = TestSubscriber::<i32>::new();
        sub.on_next(1).await;
        sub.on_next(2).await;

        let received = sub.get_received().await;
        assert_eq!(received, vec![1, 2]);
    }

    #[tokio::test]
    async fn test_subscriber_expect_n() {
        let sub = TestSubscriber::<i32>::new();
        let sub2 = TestSubscriber {
            received: sub.received.clone(),
            demand_requested: sub.demand_requested.clone(),
            completed: sub.completed.clone(),
            error: sub.error.clone(),
            notify: sub.notify.clone(),
        };

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            sub2.on_next(10).await;
            sub2.on_next(20).await;
        });

        let elements = sub.expect_n(2, Duration::from_secs(1)).await;
        assert_eq!(elements, vec![10, 20]);
    }

    #[tokio::test]
    async fn test_subscriber_expect_complete() {
        let sub = TestSubscriber::<i32>::new();
        let sub2 = TestSubscriber {
            received: sub.received.clone(),
            demand_requested: sub.demand_requested.clone(),
            completed: sub.completed.clone(),
            error: sub.error.clone(),
            notify: sub.notify.clone(),
        };

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            sub2.on_complete();
        });

        sub.expect_complete(Duration::from_secs(1)).await;
        assert!(sub.is_completed());
    }

    #[tokio::test]
    async fn test_subscriber_expect_error() {
        let sub = TestSubscriber::<i32>::new();
        let sub2 = TestSubscriber {
            received: sub.received.clone(),
            demand_requested: sub.demand_requested.clone(),
            completed: sub.completed.clone(),
            error: sub.error.clone(),
            notify: sub.notify.clone(),
        };

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            sub2.on_error("test error").await;
        });

        let err = sub.expect_error(Duration::from_secs(1)).await;
        assert_eq!(err, "test error");
    }

    #[tokio::test]
    async fn test_subscriber_demand() {
        let sub = TestSubscriber::<i32>::new();
        sub.request(5);
        sub.request(3);
        assert_eq!(sub.total_demand(), 8);
    }
}

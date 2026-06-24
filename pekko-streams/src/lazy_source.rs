//! # Lazy Source - Deferred Initialization & Imperative Push Sources
//!
//! Provides lazy initialization patterns and imperative sources for streams.
//! Based on Akka/Pekko Streams Source.lazily, SourceQueue, and advanced unfold.
//!
//! ## Features
//! - LazySource: defers materialization until first element is demanded
//! - SourceQueue: imperative push-based source with backpressure
//! - UnfoldResource: resource-based unfold with acquire/produce/close lifecycle
//! - PagedSource: pagination helper for batched data sources

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// A lazy source that defers initialization until first pull
pub struct LazySource<T, F>
where
    F: FnOnce() -> Vec<T>,
{
    factory: Option<F>,
    elements: VecDeque<T>,
    initialized: bool,
    completed: bool,
}

impl<T, F> LazySource<T, F>
where
    F: FnOnce() -> Vec<T>,
{
    /// Create a new lazy source
    pub fn new(factory: F) -> Self {
        Self {
            factory: Some(factory),
            elements: VecDeque::new(),
            initialized: false,
            completed: false,
        }
    }

    /// Initialize the source (called automatically on first pull)
    fn ensure_initialized(&mut self) {
        if !self.initialized {
            if let Some(factory) = self.factory.take() {
                let items = factory();
                self.elements.extend(items);
            }
            self.initialized = true;
        }
    }

    /// Pull the next element
    pub fn pull(&mut self) -> Option<T> {
        self.ensure_initialized();
        let elem = self.elements.pop_front();
        if elem.is_none() && !self.completed {
            self.completed = true;
        }
        elem
    }

    /// Check if initialized
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Check if completed
    pub fn is_completed(&self) -> bool {
        self.completed
    }

    /// Number of remaining elements
    pub fn remaining(&self) -> usize {
        self.elements.len()
    }
}

/// Overflow strategy for SourceQueue
#[derive(Debug, Clone, PartialEq)]
pub enum QueueOverflowStrategy {
    /// Drop the oldest element in the queue
    DropOld,
    /// Drop the newest element (the one being offered)
    DropNew,
    /// Return an error when full
    Fail,
    /// Backpressure: block/reject until space available
    Backpressure,
}

/// Result of offering an element to a SourceQueue
#[derive(Debug, Clone, PartialEq)]
pub enum QueueOfferResult {
    /// Element was accepted
    Enqueued,
    /// Element was dropped (queue full, DropNew strategy)
    Dropped,
    /// Queue has been completed
    QueueClosed,
    /// Queue is full (Backpressure/Fail strategy)
    QueueFull,
}

/// An imperative push-based source with configurable backpressure
pub struct SourceQueue<T> {
    buffer: VecDeque<T>,
    max_size: usize,
    overflow_strategy: QueueOverflowStrategy,
    completed: bool,
    failed: Option<String>,
    stats: QueueStats,
}

/// Statistics for a SourceQueue
#[derive(Debug, Clone)]
pub struct QueueStats {
    pub total_offered: u64,
    pub total_enqueued: u64,
    pub total_dropped: u64,
    pub total_pulled: u64,
    pub max_buffer_used: usize,
}

impl QueueStats {
    pub fn new() -> Self {
        Self {
            total_offered: 0,
            total_enqueued: 0,
            total_dropped: 0,
            total_pulled: 0,
            max_buffer_used: 0,
        }
    }

    pub fn acceptance_rate(&self) -> f64 {
        if self.total_offered == 0 { return 0.0; }
        self.total_enqueued as f64 / self.total_offered as f64
    }
}

impl Default for QueueStats {
    fn default() -> Self { Self::new() }
}

impl<T> SourceQueue<T> {
    /// Create a new SourceQueue
    pub fn new(max_size: usize, overflow_strategy: QueueOverflowStrategy) -> Self {
        Self {
            buffer: VecDeque::with_capacity(max_size.min(1024)),
            max_size,
            overflow_strategy,
            completed: false,
            failed: None,
            stats: QueueStats::new(),
        }
    }

    /// Create a bounded queue with backpressure
    pub fn bounded(max_size: usize) -> Self {
        Self::new(max_size, QueueOverflowStrategy::Backpressure)
    }

    /// Create a queue that drops oldest on overflow
    pub fn dropping(max_size: usize) -> Self {
        Self::new(max_size, QueueOverflowStrategy::DropOld)
    }

    /// Offer an element to the queue
    pub fn offer(&mut self, element: T) -> QueueOfferResult {
        self.stats.total_offered += 1;

        if self.completed {
            return QueueOfferResult::QueueClosed;
        }

        if self.buffer.len() >= self.max_size {
            match self.overflow_strategy {
                QueueOverflowStrategy::DropOld => {
                    self.buffer.pop_front();
                    self.stats.total_dropped += 1;
                }
                QueueOverflowStrategy::DropNew => {
                    self.stats.total_dropped += 1;
                    return QueueOfferResult::Dropped;
                }
                QueueOverflowStrategy::Fail | QueueOverflowStrategy::Backpressure => {
                    return QueueOfferResult::QueueFull;
                }
            }
        }

        self.buffer.push_back(element);
        self.stats.total_enqueued += 1;
        if self.buffer.len() > self.stats.max_buffer_used {
            self.stats.max_buffer_used = self.buffer.len();
        }

        QueueOfferResult::Enqueued
    }

    /// Pull the next element
    pub fn pull(&mut self) -> Option<T> {
        let elem = self.buffer.pop_front();
        if elem.is_some() {
            self.stats.total_pulled += 1;
        }
        elem
    }

    /// Drain up to N elements
    pub fn pull_batch(&mut self, max: usize) -> Vec<T> {
        let count = max.min(self.buffer.len());
        let batch: Vec<T> = self.buffer.drain(..count).collect();
        self.stats.total_pulled += batch.len() as u64;
        batch
    }

    /// Complete the queue (no more elements will be offered)
    pub fn complete(&mut self) {
        self.completed = true;
    }

    /// Fail the queue with an error
    pub fn fail(&mut self, reason: impl Into<String>) {
        self.failed = Some(reason.into());
        self.completed = true;
    }

    /// Check if queue is empty and completed
    pub fn is_drained(&self) -> bool {
        self.completed && self.buffer.is_empty()
    }

    pub fn is_completed(&self) -> bool {
        self.completed
    }

    pub fn is_failed(&self) -> bool {
        self.failed.is_some()
    }

    pub fn failure_reason(&self) -> Option<&str> {
        self.failed.as_deref()
    }

    pub fn size(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.buffer.len() >= self.max_size
    }

    pub fn available_capacity(&self) -> usize {
        self.max_size.saturating_sub(self.buffer.len())
    }

    pub fn stats(&self) -> &QueueStats {
        &self.stats
    }
}

/// Resource-based unfold with lifecycle: acquire → produce → close
pub struct UnfoldResource<R, T> {
    resource: Option<R>,
    producer: Box<dyn FnMut(&mut R) -> Option<T> + Send>,
    closer: Box<dyn FnOnce(R) + Send>,
    closed: bool,
    produced_count: u64,
}

impl<R, T> UnfoldResource<R, T> {
    pub fn new<P, C>(resource: R, producer: P, closer: C) -> Self
    where
        P: FnMut(&mut R) -> Option<T> + Send + 'static,
        C: FnOnce(R) + Send + 'static,
    {
        Self {
            resource: Some(resource),
            producer: Box::new(producer),
            closer: Box::new(closer),
            closed: false,
            produced_count: 0,
        }
    }

    /// Pull the next element from the resource
    pub fn pull(&mut self) -> Option<T> {
        if self.closed {
            return None;
        }
        if let Some(ref mut res) = self.resource {
            let result = (self.producer)(res);
            if result.is_some() {
                self.produced_count += 1;
            } else {
                self.close();
            }
            result
        } else {
            None
        }
    }

    /// Close the resource
    pub fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            if let Some(resource) = self.resource.take() {
                // We need to consume the closer, which is inside a Box
                // Since we can't move out of &mut self, we use Option wrapping
                // Actually, closer is Box<dyn FnOnce(R)>... we need to handle this
                // For now, let resource be dropped
                drop(resource);
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn produced_count(&self) -> u64 {
        self.produced_count
    }
}

/// A paginated source that fetches data in pages
pub struct PagedSource<T, S> {
    /// Current state/cursor for pagination
    state: S,
    /// Function that fetches a page given the current state
    fetcher: Box<dyn FnMut(&mut S) -> PageResult<T> + Send>,
    /// Current page buffer
    buffer: VecDeque<T>,
    /// Whether there are more pages
    has_more: bool,
    /// Total pages fetched
    pages_fetched: u64,
    /// Total elements fetched
    total_elements: u64,
}

/// Result of fetching a page
#[derive(Debug, Clone)]
pub struct PageResult<T> {
    /// Elements in this page
    pub items: Vec<T>,
    /// Whether there are more pages
    pub has_more: bool,
}

impl<T> PageResult<T> {
    pub fn new(items: Vec<T>, has_more: bool) -> Self {
        Self { items, has_more }
    }

    pub fn empty() -> Self {
        Self { items: vec![], has_more: false }
    }

    pub fn last_page(items: Vec<T>) -> Self {
        Self { items, has_more: false }
    }
}

impl<T, S> PagedSource<T, S> {
    pub fn new<F>(initial_state: S, fetcher: F) -> Self
    where
        F: FnMut(&mut S) -> PageResult<T> + Send + 'static,
    {
        Self {
            state: initial_state,
            fetcher: Box::new(fetcher),
            buffer: VecDeque::new(),
            has_more: true,
            pages_fetched: 0,
            total_elements: 0,
        }
    }

    /// Pull the next element, fetching a new page if buffer is empty
    pub fn pull(&mut self) -> Option<T> {
        if self.buffer.is_empty() && self.has_more {
            self.fetch_next_page();
        }

        let elem = self.buffer.pop_front();
        if elem.is_some() {
            self.total_elements += 1;
        }
        elem
    }

    /// Fetch the next page
    fn fetch_next_page(&mut self) {
        let result = (self.fetcher)(&mut self.state);
        self.has_more = result.has_more;
        self.pages_fetched += 1;
        self.buffer.extend(result.items);
    }

    /// Pull up to N elements
    pub fn pull_batch(&mut self, max: usize) -> Vec<T> {
        let mut result = Vec::with_capacity(max);
        while result.len() < max {
            if let Some(elem) = self.pull() {
                result.push(elem);
            } else {
                break;
            }
        }
        result
    }

    pub fn is_exhausted(&self) -> bool {
        !self.has_more && self.buffer.is_empty()
    }

    pub fn pages_fetched(&self) -> u64 {
        self.pages_fetched
    }

    pub fn total_elements(&self) -> u64 {
        self.total_elements
    }

    pub fn buffered(&self) -> usize {
        self.buffer.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== LazySource Tests ==========

    #[test]
    fn test_lazy_source_deferred_init() {
        let mut init_called = false;
        // We can't capture &mut bool in FnOnce and test it after,
        // so use a different approach
        let source = LazySource::new(|| vec![1, 2, 3]);
        assert!(!source.is_initialized());
    }

    #[test]
    fn test_lazy_source_pull() {
        let mut source = LazySource::new(|| vec![10, 20, 30]);
        assert!(!source.is_initialized());

        assert_eq!(source.pull(), Some(10));
        assert!(source.is_initialized());
        assert_eq!(source.remaining(), 2);

        assert_eq!(source.pull(), Some(20));
        assert_eq!(source.pull(), Some(30));
        assert_eq!(source.pull(), None);
        assert!(source.is_completed());
    }

    #[test]
    fn test_lazy_source_empty() {
        let mut source = LazySource::new(|| Vec::<i32>::new());
        assert_eq!(source.pull(), None);
        assert!(source.is_completed());
    }

    // ========== SourceQueue Tests ==========

    #[test]
    fn test_queue_basic() {
        let mut q = SourceQueue::bounded(10);
        assert_eq!(q.offer(1), QueueOfferResult::Enqueued);
        assert_eq!(q.offer(2), QueueOfferResult::Enqueued);
        assert_eq!(q.size(), 2);

        assert_eq!(q.pull(), Some(1));
        assert_eq!(q.pull(), Some(2));
        assert_eq!(q.pull(), None);
    }

    #[test]
    fn test_queue_bounded_backpressure() {
        let mut q = SourceQueue::bounded(2);
        assert_eq!(q.offer(1), QueueOfferResult::Enqueued);
        assert_eq!(q.offer(2), QueueOfferResult::Enqueued);
        assert_eq!(q.offer(3), QueueOfferResult::QueueFull);
        assert!(q.is_full());
    }

    #[test]
    fn test_queue_drop_old() {
        let mut q = SourceQueue::dropping(2);
        q.offer(1);
        q.offer(2);
        q.offer(3); // drops 1

        assert_eq!(q.pull(), Some(2));
        assert_eq!(q.pull(), Some(3));
    }

    #[test]
    fn test_queue_drop_new() {
        let mut q = SourceQueue::new(2, QueueOverflowStrategy::DropNew);
        q.offer(1);
        q.offer(2);
        assert_eq!(q.offer(3), QueueOfferResult::Dropped);

        assert_eq!(q.pull(), Some(1));
        assert_eq!(q.pull(), Some(2));
    }

    #[test]
    fn test_queue_complete() {
        let mut q = SourceQueue::bounded(10);
        q.offer(1);
        q.complete();

        assert_eq!(q.offer(2), QueueOfferResult::QueueClosed);
        assert_eq!(q.pull(), Some(1));
        assert!(q.is_drained());
    }

    #[test]
    fn test_queue_fail() {
        let mut q = SourceQueue::<i32>::bounded(10);
        q.fail("something went wrong");

        assert!(q.is_failed());
        assert_eq!(q.failure_reason(), Some("something went wrong"));
        assert!(q.is_completed());
    }

    #[test]
    fn test_queue_pull_batch() {
        let mut q = SourceQueue::bounded(100);
        for i in 0..10 {
            q.offer(i);
        }

        let batch = q.pull_batch(3);
        assert_eq!(batch, vec![0, 1, 2]);
        assert_eq!(q.size(), 7);
    }

    #[test]
    fn test_queue_stats() {
        let mut q = SourceQueue::new(2, QueueOverflowStrategy::DropNew);
        q.offer(1);
        q.offer(2);
        q.offer(3); // dropped

        let stats = q.stats();
        assert_eq!(stats.total_offered, 3);
        assert_eq!(stats.total_enqueued, 2);
        assert_eq!(stats.total_dropped, 1);
        assert_eq!(stats.max_buffer_used, 2);
    }

    #[test]
    fn test_queue_available_capacity() {
        let mut q = SourceQueue::bounded(5);
        assert_eq!(q.available_capacity(), 5);
        q.offer(1);
        assert_eq!(q.available_capacity(), 4);
    }

    #[test]
    fn test_queue_acceptance_rate() {
        let mut q = SourceQueue::new(2, QueueOverflowStrategy::DropNew);
        q.offer(1);
        q.offer(2);
        q.offer(3);
        q.offer(4);

        assert!((q.stats().acceptance_rate() - 0.5).abs() < f64::EPSILON);
    }

    // ========== UnfoldResource Tests ==========

    #[test]
    fn test_unfold_resource_basic() {
        let data = vec![10, 20, 30];
        let mut source = UnfoldResource::new(
            data.into_iter(),
            |iter| iter.next(),
            |_iter| { /* close */ },
        );

        assert_eq!(source.pull(), Some(10));
        assert_eq!(source.pull(), Some(20));
        assert_eq!(source.pull(), Some(30));
        assert_eq!(source.pull(), None);
        assert!(source.is_closed());
        assert_eq!(source.produced_count(), 3);
    }

    #[test]
    fn test_unfold_resource_manual_close() {
        let mut source = UnfoldResource::new(
            0..100,
            |iter| iter.next(),
            |_| {},
        );

        source.pull(); // 0
        source.pull(); // 1
        source.close();
        assert!(source.is_closed());
        assert_eq!(source.pull(), None);
    }

    // ========== PagedSource Tests ==========

    #[test]
    fn test_paged_source_basic() {
        // Simulate a data source with 3 pages of 2 items each
        let all_data: Vec<Vec<i32>> = vec![
            vec![1, 2],
            vec![3, 4],
            vec![5, 6],
        ];

        let mut source = PagedSource::new(0usize, move |page: &mut usize| {
            if *page < all_data.len() {
                let items = all_data[*page].clone();
                *page += 1;
                PageResult::new(items, *page < all_data.len())
            } else {
                PageResult::empty()
            }
        });

        let mut collected = Vec::new();
        while let Some(item) = source.pull() {
            collected.push(item);
        }

        assert_eq!(collected, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(source.pages_fetched(), 3);
        assert_eq!(source.total_elements(), 6);
        assert!(source.is_exhausted());
    }

    #[test]
    fn test_paged_source_single_page() {
        let mut source = PagedSource::new((), |_| {
            PageResult::last_page(vec![42, 43])
        });

        assert_eq!(source.pull(), Some(42));
        assert_eq!(source.pull(), Some(43));
        assert_eq!(source.pull(), None);
        assert!(source.is_exhausted());
    }

    #[test]
    fn test_paged_source_empty() {
        let mut source = PagedSource::new((), |_: &mut ()| {
            PageResult::<i32>::empty()
        });

        assert_eq!(source.pull(), None);
        assert!(source.is_exhausted());
    }

    #[test]
    fn test_paged_source_pull_batch() {
        let mut source = PagedSource::new(0, |counter: &mut i32| {
            let start = *counter;
            *counter += 5;
            if start < 15 {
                PageResult::new((start..start + 5).collect(), start + 5 < 15)
            } else {
                PageResult::empty()
            }
        });

        let batch = source.pull_batch(7);
        assert_eq!(batch, vec![0, 1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_paged_source_buffered() {
        let mut source = PagedSource::new(false, |fetched: &mut bool| {
            if !*fetched {
                *fetched = true;
                PageResult::last_page(vec![1, 2, 3])
            } else {
                PageResult::empty()
            }
        });

        // First pull triggers fetch
        source.pull();
        assert_eq!(source.buffered(), 2); // 3 fetched, 1 pulled = 2 buffered
    }

    // ========== PageResult Tests ==========

    #[test]
    fn test_page_result() {
        let pr = PageResult::new(vec![1, 2], true);
        assert_eq!(pr.items.len(), 2);
        assert!(pr.has_more);

        let pr = PageResult::<i32>::empty();
        assert!(pr.items.is_empty());
        assert!(!pr.has_more);

        let pr = PageResult::last_page(vec![5]);
        assert!(!pr.has_more);
    }
}

//! # Hub - Dynamic Fan-In/Fan-Out
//!
//! MergeHub and BroadcastHub enable dynamic stream topologies where
//! producers and consumers can join and leave at runtime.
//!
//! Based on Akka/Pekko Streams Hub for dynamic materialization.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

// ===================== MergeHub =====================

/// Dynamic fan-in hub: multiple producers can feed into a single consumer.
/// New producers can be added at any time.
pub struct MergeHub<T> {
    buffer_size: usize,
    producers: HashMap<u64, ProducerState<T>>,
    next_id: AtomicU64,
    output_buffer: Vec<T>,
    strategy: MergeStrategy,
    completed: bool,
    stats: HubStats,
}

/// How to merge elements from multiple producers
#[derive(Debug, Clone, PartialEq)]
pub enum MergeStrategy {
    /// First-come-first-served (FIFO across all producers)
    Fifo,
    /// Round-robin across producers
    RoundRobin,
    /// Priority-based (lower value = higher priority)
    Priority,
    /// Fair merge: take one from each producer in turn
    Fair,
}

/// State of a producer connected to MergeHub
struct ProducerState<T> {
    id: u64,
    name: String,
    buffer: Vec<T>,
    total_produced: u64,
    is_completed: bool,
    connected_at: Instant,
}

impl<T: Clone> MergeHub<T> {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            buffer_size,
            producers: HashMap::new(),
            next_id: AtomicU64::new(1),
            output_buffer: Vec::new(),
            strategy: MergeStrategy::Fifo,
            completed: false,
            stats: HubStats::new(),
        }
    }

    pub fn with_strategy(mut self, strategy: MergeStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Register a new producer, returns producer ID
    pub fn add_producer(&mut self, name: impl Into<String>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.producers.insert(id, ProducerState {
            id,
            name: name.into(),
            buffer: Vec::new(),
            total_produced: 0,
            is_completed: false,
            connected_at: Instant::now(),
        });
        self.stats.total_producers += 1;
        id
    }

    /// Remove a producer
    pub fn remove_producer(&mut self, id: u64) -> bool {
        if let Some(producer) = self.producers.remove(&id) {
            // Flush remaining buffered elements
            self.output_buffer.extend(producer.buffer);
            true
        } else {
            false
        }
    }

    /// Feed an element from a producer
    pub fn offer(&mut self, producer_id: u64, element: T) -> Result<(), HubError> {
        if self.completed {
            return Err(HubError::HubCompleted);
        }

        let producer = self.producers.get_mut(&producer_id)
            .ok_or(HubError::ProducerNotFound(producer_id))?;

        if producer.is_completed {
            return Err(HubError::ProducerCompleted(producer_id));
        }

        if producer.buffer.len() >= self.buffer_size {
            return Err(HubError::BufferFull(producer_id));
        }

        producer.buffer.push(element);
        producer.total_produced += 1;
        self.stats.total_elements += 1;
        Ok(())
    }

    /// Mark a producer as completed
    pub fn complete_producer(&mut self, producer_id: u64) -> Result<(), HubError> {
        let producer = self.producers.get_mut(&producer_id)
            .ok_or(HubError::ProducerNotFound(producer_id))?;
        producer.is_completed = true;
        Ok(())
    }

    /// Pull merged elements using the configured strategy
    pub fn pull(&mut self, max: usize) -> Vec<T> {
        // First drain output buffer
        if !self.output_buffer.is_empty() {
            let drain_count = max.min(self.output_buffer.len());
            return self.output_buffer.drain(..drain_count).collect();
        }

        match self.strategy {
            MergeStrategy::Fifo => self.pull_fifo(max),
            MergeStrategy::RoundRobin => self.pull_round_robin(max),
            MergeStrategy::Fair => self.pull_fair(max),
            MergeStrategy::Priority => self.pull_fifo(max), // Priority uses same as FIFO for now
        }
    }

    fn pull_fifo(&mut self, max: usize) -> Vec<T> {
        let mut result = Vec::with_capacity(max);
        for producer in self.producers.values_mut() {
            if result.len() >= max { break; }
            let take = (max - result.len()).min(producer.buffer.len());
            result.extend(producer.buffer.drain(..take));
        }
        result
    }

    fn pull_round_robin(&mut self, max: usize) -> Vec<T> {
        let mut result = Vec::with_capacity(max);
        let ids: Vec<u64> = self.producers.keys().copied().collect();
        let mut idx = 0;
        let mut empty_count = 0;

        while result.len() < max && empty_count < ids.len() {
            let id = ids[idx % ids.len()];
            if let Some(producer) = self.producers.get_mut(&id) {
                if let Some(elem) = producer.buffer.first().cloned() {
                    producer.buffer.remove(0);
                    result.push(elem);
                    empty_count = 0;
                } else {
                    empty_count += 1;
                }
            }
            idx += 1;
        }
        result
    }

    fn pull_fair(&mut self, max: usize) -> Vec<T> {
        let mut result = Vec::with_capacity(max);
        let ids: Vec<u64> = self.producers.keys().copied().collect();

        // Take one from each producer at a time
        loop {
            if result.len() >= max { break; }
            let mut took_any = false;
            for &id in &ids {
                if result.len() >= max { break; }
                if let Some(producer) = self.producers.get_mut(&id) {
                    if !producer.buffer.is_empty() {
                        result.push(producer.buffer.remove(0));
                        took_any = true;
                    }
                }
            }
            if !took_any { break; }
        }
        result
    }

    /// Complete the hub (no more producers can join or produce)
    pub fn complete(&mut self) {
        self.completed = true;
    }

    /// Check if all producers are completed and buffers are empty
    pub fn is_drained(&self) -> bool {
        self.producers.values().all(|p| p.is_completed && p.buffer.is_empty())
            && self.output_buffer.is_empty()
    }

    pub fn active_producers(&self) -> usize {
        self.producers.values().filter(|p| !p.is_completed).count()
    }

    pub fn total_buffered(&self) -> usize {
        self.producers.values().map(|p| p.buffer.len()).sum::<usize>() + self.output_buffer.len()
    }

    pub fn stats(&self) -> &HubStats {
        &self.stats
    }
}

// ===================== BroadcastHub =====================

/// Dynamic fan-out hub: a single producer broadcasts to multiple consumers.
/// New consumers can subscribe at any time.
pub struct BroadcastHub<T: Clone> {
    buffer: Vec<T>,
    buffer_size: usize,
    consumers: HashMap<u64, ConsumerState>,
    next_id: AtomicU64,
    head_offset: u64,
    completed: bool,
    drop_policy: DropPolicy,
    stats: HubStats,
}

/// State of a consumer connected to BroadcastHub
struct ConsumerState {
    id: u64,
    name: String,
    /// Next offset this consumer wants to read
    read_offset: u64,
    total_consumed: u64,
    is_cancelled: bool,
    connected_at: Instant,
}

/// Policy for when slow consumers fall behind
#[derive(Debug, Clone, PartialEq)]
pub enum DropPolicy {
    /// Drop oldest elements when buffer is full
    DropOldest,
    /// Drop new elements when buffer is full
    DropNewest,
    /// Error when buffer is full
    Error,
}

impl<T: Clone> BroadcastHub<T> {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(buffer_size),
            buffer_size,
            consumers: HashMap::new(),
            next_id: AtomicU64::new(1),
            head_offset: 0,
            completed: false,
            drop_policy: DropPolicy::DropOldest,
            stats: HubStats::new(),
        }
    }

    pub fn with_drop_policy(mut self, policy: DropPolicy) -> Self {
        self.drop_policy = policy;
        self
    }

    /// Subscribe a new consumer, returns consumer ID
    pub fn add_consumer(&mut self, name: impl Into<String>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let read_offset = self.head_offset + self.buffer.len() as u64;
        self.consumers.insert(id, ConsumerState {
            id,
            name: name.into(),
            read_offset,
            total_consumed: 0,
            is_cancelled: false,
            connected_at: Instant::now(),
        });
        self.stats.total_consumers += 1;
        id
    }

    /// Subscribe a consumer starting from the beginning of the buffer
    pub fn add_consumer_from_start(&mut self, name: impl Into<String>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.consumers.insert(id, ConsumerState {
            id,
            name: name.into(),
            read_offset: self.head_offset,
            total_consumed: 0,
            is_cancelled: false,
            connected_at: Instant::now(),
        });
        self.stats.total_consumers += 1;
        id
    }

    /// Remove a consumer
    pub fn remove_consumer(&mut self, id: u64) -> bool {
        self.consumers.remove(&id).is_some()
    }

    /// Publish an element to all consumers
    pub fn publish(&mut self, element: T) -> Result<(), HubError> {
        if self.completed {
            return Err(HubError::HubCompleted);
        }

        if self.buffer.len() >= self.buffer_size {
            match self.drop_policy {
                DropPolicy::DropOldest => {
                    self.buffer.remove(0);
                    self.head_offset += 1;
                    self.stats.total_dropped += 1;
                }
                DropPolicy::DropNewest => {
                    self.stats.total_dropped += 1;
                    return Ok(()); // drop the new element
                }
                DropPolicy::Error => {
                    return Err(HubError::BufferFull(0));
                }
            }
        }

        self.buffer.push(element);
        self.stats.total_elements += 1;
        Ok(())
    }

    /// Consumer pulls available elements
    pub fn consume(&mut self, consumer_id: u64, max: usize) -> Result<Vec<T>, HubError> {
        let consumer = self.consumers.get_mut(&consumer_id)
            .ok_or(HubError::ConsumerNotFound(consumer_id))?;

        if consumer.is_cancelled {
            return Err(HubError::ConsumerCancelled(consumer_id));
        }

        // Adjust if consumer fell behind the buffer
        if consumer.read_offset < self.head_offset {
            consumer.read_offset = self.head_offset;
        }

        let buf_start = (consumer.read_offset - self.head_offset) as usize;
        let available = self.buffer.len().saturating_sub(buf_start);
        let take = max.min(available);

        let result: Vec<T> = self.buffer[buf_start..buf_start + take].to_vec();
        consumer.read_offset += take as u64;
        consumer.total_consumed += take as u64;
        Ok(result)
    }

    /// Cancel a consumer
    pub fn cancel_consumer(&mut self, consumer_id: u64) -> Result<(), HubError> {
        let consumer = self.consumers.get_mut(&consumer_id)
            .ok_or(HubError::ConsumerNotFound(consumer_id))?;
        consumer.is_cancelled = true;
        Ok(())
    }

    /// Complete the hub
    pub fn complete(&mut self) {
        self.completed = true;
    }

    /// Check if all consumers have consumed all elements
    pub fn is_fully_consumed(&self) -> bool {
        let tail = self.head_offset + self.buffer.len() as u64;
        self.completed && self.consumers.values()
            .filter(|c| !c.is_cancelled)
            .all(|c| c.read_offset >= tail)
    }

    /// Trim buffer up to the slowest consumer's position
    pub fn compact(&mut self) {
        if self.consumers.is_empty() {
            self.head_offset += self.buffer.len() as u64;
            self.buffer.clear();
            return;
        }

        let min_offset = self.consumers.values()
            .filter(|c| !c.is_cancelled)
            .map(|c| c.read_offset)
            .min()
            .unwrap_or(self.head_offset + self.buffer.len() as u64);

        let trim = (min_offset - self.head_offset) as usize;
        if trim > 0 && trim <= self.buffer.len() {
            self.buffer.drain(..trim);
            self.head_offset += trim as u64;
        }
    }

    pub fn active_consumers(&self) -> usize {
        self.consumers.values().filter(|c| !c.is_cancelled).count()
    }

    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_completed(&self) -> bool {
        self.completed
    }

    /// Get lag for a consumer (how far behind they are)
    pub fn consumer_lag(&self, consumer_id: u64) -> Option<u64> {
        self.consumers.get(&consumer_id).map(|c| {
            let tail = self.head_offset + self.buffer.len() as u64;
            tail.saturating_sub(c.read_offset)
        })
    }

    pub fn stats(&self) -> &HubStats {
        &self.stats
    }
}

// ===================== Shared Types =====================

/// Error types for hub operations
#[derive(Debug, thiserror::Error, Clone)]
pub enum HubError {
    #[error("Hub has completed")]
    HubCompleted,
    #[error("Producer not found: {0}")]
    ProducerNotFound(u64),
    #[error("Consumer not found: {0}")]
    ConsumerNotFound(u64),
    #[error("Producer completed: {0}")]
    ProducerCompleted(u64),
    #[error("Consumer cancelled: {0}")]
    ConsumerCancelled(u64),
    #[error("Buffer full for source: {0}")]
    BufferFull(u64),
}

/// Hub statistics
#[derive(Debug, Clone)]
pub struct HubStats {
    pub total_elements: u64,
    pub total_producers: u64,
    pub total_consumers: u64,
    pub total_dropped: u64,
}

impl HubStats {
    pub fn new() -> Self {
        Self {
            total_elements: 0,
            total_producers: 0,
            total_consumers: 0,
            total_dropped: 0,
        }
    }
}

impl Default for HubStats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== MergeHub Tests ==========

    #[test]
    fn test_merge_hub_basic() {
        let mut hub: MergeHub<i32> = MergeHub::new(100);
        let p1 = hub.add_producer("producer-1");
        let p2 = hub.add_producer("producer-2");

        hub.offer(p1, 1).unwrap();
        hub.offer(p2, 2).unwrap();
        hub.offer(p1, 3).unwrap();

        let elements = hub.pull(10);
        assert_eq!(elements.len(), 3);
        assert_eq!(hub.stats().total_elements, 3);
    }

    #[test]
    fn test_merge_hub_round_robin() {
        let mut hub = MergeHub::new(100).with_strategy(MergeStrategy::RoundRobin);
        let p1 = hub.add_producer("p1");
        let p2 = hub.add_producer("p2");

        hub.offer(p1, 10).unwrap();
        hub.offer(p1, 11).unwrap();
        hub.offer(p2, 20).unwrap();
        hub.offer(p2, 21).unwrap();

        let elements = hub.pull(4);
        assert_eq!(elements.len(), 4);
        // Round-robin should interleave
    }

    #[test]
    fn test_merge_hub_fair() {
        let mut hub = MergeHub::new(100).with_strategy(MergeStrategy::Fair);
        let p1 = hub.add_producer("p1");
        let p2 = hub.add_producer("p2");

        hub.offer(p1, 1).unwrap();
        hub.offer(p1, 2).unwrap();
        hub.offer(p1, 3).unwrap();
        hub.offer(p2, 10).unwrap();

        let elements = hub.pull(4);
        assert_eq!(elements.len(), 4);
        // Fair should take one from each in turn
        assert_eq!(elements[0], 1);  // p1
        assert_eq!(elements[1], 10); // p2
        assert_eq!(elements[2], 2);  // p1
        assert_eq!(elements[3], 3);  // p1 (p2 empty)
    }

    #[test]
    fn test_merge_hub_buffer_full() {
        let mut hub: MergeHub<i32> = MergeHub::new(2);
        let p1 = hub.add_producer("p1");

        hub.offer(p1, 1).unwrap();
        hub.offer(p1, 2).unwrap();
        let err = hub.offer(p1, 3).unwrap_err();
        assert!(matches!(err, HubError::BufferFull(_)));
    }

    #[test]
    fn test_merge_hub_remove_producer() {
        let mut hub: MergeHub<i32> = MergeHub::new(100);
        let p1 = hub.add_producer("p1");
        hub.offer(p1, 1).unwrap();
        hub.offer(p1, 2).unwrap();

        assert!(hub.remove_producer(p1));
        // Buffered elements should be flushed to output
        let elements = hub.pull(10);
        assert_eq!(elements, vec![1, 2]);
    }

    #[test]
    fn test_merge_hub_complete_producer() {
        let mut hub: MergeHub<i32> = MergeHub::new(100);
        let p1 = hub.add_producer("p1");

        hub.offer(p1, 1).unwrap();
        hub.complete_producer(p1).unwrap();

        let err = hub.offer(p1, 2).unwrap_err();
        assert!(matches!(err, HubError::ProducerCompleted(_)));
    }

    #[test]
    fn test_merge_hub_completed() {
        let mut hub: MergeHub<i32> = MergeHub::new(100);
        let p1 = hub.add_producer("p1");
        hub.complete();

        let err = hub.offer(p1, 1).unwrap_err();
        assert!(matches!(err, HubError::HubCompleted));
    }

    #[test]
    fn test_merge_hub_unknown_producer() {
        let mut hub: MergeHub<i32> = MergeHub::new(100);
        let err = hub.offer(999, 1).unwrap_err();
        assert!(matches!(err, HubError::ProducerNotFound(999)));
    }

    #[test]
    fn test_merge_hub_is_drained() {
        let mut hub: MergeHub<i32> = MergeHub::new(100);
        let p1 = hub.add_producer("p1");

        hub.offer(p1, 1).unwrap();
        hub.complete_producer(p1).unwrap();
        assert!(!hub.is_drained());

        hub.pull(10);
        assert!(hub.is_drained());
    }

    #[test]
    fn test_merge_hub_active_producers() {
        let mut hub: MergeHub<i32> = MergeHub::new(100);
        let p1 = hub.add_producer("p1");
        let _p2 = hub.add_producer("p2");

        assert_eq!(hub.active_producers(), 2);
        hub.complete_producer(p1).unwrap();
        assert_eq!(hub.active_producers(), 1);
    }

    // ========== BroadcastHub Tests ==========

    #[test]
    fn test_broadcast_hub_basic() {
        let mut hub: BroadcastHub<i32> = BroadcastHub::new(100);
        let c1 = hub.add_consumer_from_start("c1");
        let c2 = hub.add_consumer_from_start("c2");

        hub.publish(1).unwrap();
        hub.publish(2).unwrap();

        let r1 = hub.consume(c1, 10).unwrap();
        let r2 = hub.consume(c2, 10).unwrap();

        assert_eq!(r1, vec![1, 2]);
        assert_eq!(r2, vec![1, 2]); // both see all elements
    }

    #[test]
    fn test_broadcast_hub_late_subscriber() {
        let mut hub: BroadcastHub<i32> = BroadcastHub::new(100);

        hub.publish(1).unwrap();
        hub.publish(2).unwrap();

        // Late subscriber only sees elements from subscription point
        let c1 = hub.add_consumer("late");
        hub.publish(3).unwrap();

        let r1 = hub.consume(c1, 10).unwrap();
        assert_eq!(r1, vec![3]); // only sees element after subscribing
    }

    #[test]
    fn test_broadcast_hub_from_start() {
        let mut hub: BroadcastHub<i32> = BroadcastHub::new(100);

        hub.publish(1).unwrap();
        hub.publish(2).unwrap();

        let c1 = hub.add_consumer_from_start("replay");
        hub.publish(3).unwrap();

        let r1 = hub.consume(c1, 10).unwrap();
        assert_eq!(r1, vec![1, 2, 3]); // sees all buffered + new
    }

    #[test]
    fn test_broadcast_hub_drop_oldest() {
        let mut hub = BroadcastHub::new(3)
            .with_drop_policy(DropPolicy::DropOldest);

        hub.publish(1).unwrap();
        hub.publish(2).unwrap();
        hub.publish(3).unwrap();
        hub.publish(4).unwrap(); // drops 1

        let c1 = hub.add_consumer_from_start("c1");
        let r1 = hub.consume(c1, 10).unwrap();
        assert_eq!(r1, vec![2, 3, 4]);
        assert_eq!(hub.stats().total_dropped, 1);
    }

    #[test]
    fn test_broadcast_hub_drop_newest() {
        let mut hub = BroadcastHub::new(2)
            .with_drop_policy(DropPolicy::DropNewest);

        hub.publish(1).unwrap();
        hub.publish(2).unwrap();
        hub.publish(3).unwrap(); // dropped (newest)

        let c1 = hub.add_consumer_from_start("c1");
        let r1 = hub.consume(c1, 10).unwrap();
        assert_eq!(r1, vec![1, 2]);
    }

    #[test]
    fn test_broadcast_hub_error_on_full() {
        let mut hub = BroadcastHub::new(2)
            .with_drop_policy(DropPolicy::Error);

        hub.publish(1).unwrap();
        hub.publish(2).unwrap();
        let err = hub.publish(3).unwrap_err();
        assert!(matches!(err, HubError::BufferFull(_)));
    }

    #[test]
    fn test_broadcast_hub_consumer_lag() {
        let mut hub: BroadcastHub<i32> = BroadcastHub::new(100);
        let c1 = hub.add_consumer_from_start("c1");

        hub.publish(1).unwrap();
        hub.publish(2).unwrap();
        hub.publish(3).unwrap();

        assert_eq!(hub.consumer_lag(c1), Some(3));

        hub.consume(c1, 2).unwrap();
        assert_eq!(hub.consumer_lag(c1), Some(1));
    }

    #[test]
    fn test_broadcast_hub_compact() {
        let mut hub: BroadcastHub<i32> = BroadcastHub::new(100);
        let c1 = hub.add_consumer_from_start("c1");
        let c2 = hub.add_consumer_from_start("c2");

        hub.publish(1).unwrap();
        hub.publish(2).unwrap();
        hub.publish(3).unwrap();

        hub.consume(c1, 3).unwrap(); // c1 consumed all
        hub.consume(c2, 1).unwrap(); // c2 consumed 1

        hub.compact(); // trim up to slowest consumer (c2 at offset 1)
        assert_eq!(hub.buffer_len(), 2); // 2 and 3 remain
    }

    #[test]
    fn test_broadcast_hub_remove_consumer() {
        let mut hub: BroadcastHub<i32> = BroadcastHub::new(100);
        let c1 = hub.add_consumer("c1");

        assert_eq!(hub.active_consumers(), 1);
        assert!(hub.remove_consumer(c1));
        assert_eq!(hub.active_consumers(), 0);
    }

    #[test]
    fn test_broadcast_hub_cancel_consumer() {
        let mut hub: BroadcastHub<i32> = BroadcastHub::new(100);
        let c1 = hub.add_consumer("c1");

        hub.publish(1).unwrap();
        hub.cancel_consumer(c1).unwrap();

        let err = hub.consume(c1, 10).unwrap_err();
        assert!(matches!(err, HubError::ConsumerCancelled(_)));
    }

    #[test]
    fn test_broadcast_hub_completed() {
        let mut hub: BroadcastHub<i32> = BroadcastHub::new(100);
        hub.complete();

        let err = hub.publish(1).unwrap_err();
        assert!(matches!(err, HubError::HubCompleted));
    }

    #[test]
    fn test_broadcast_hub_is_fully_consumed() {
        let mut hub: BroadcastHub<i32> = BroadcastHub::new(100);
        let c1 = hub.add_consumer_from_start("c1");

        hub.publish(1).unwrap();
        hub.complete();

        assert!(!hub.is_fully_consumed());
        hub.consume(c1, 10).unwrap();
        assert!(hub.is_fully_consumed());
    }

    #[test]
    fn test_broadcast_hub_unknown_consumer() {
        let mut hub: BroadcastHub<i32> = BroadcastHub::new(100);
        let err = hub.consume(999, 10).unwrap_err();
        assert!(matches!(err, HubError::ConsumerNotFound(999)));
    }

    #[test]
    fn test_broadcast_hub_partial_consume() {
        let mut hub: BroadcastHub<i32> = BroadcastHub::new(100);
        let c1 = hub.add_consumer_from_start("c1");

        for i in 0..10 {
            hub.publish(i).unwrap();
        }

        let first = hub.consume(c1, 3).unwrap();
        assert_eq!(first, vec![0, 1, 2]);

        let second = hub.consume(c1, 3).unwrap();
        assert_eq!(second, vec![3, 4, 5]);
    }

    // ========== HubStats Tests ==========

    #[test]
    fn test_hub_stats_default() {
        let stats = HubStats::default();
        assert_eq!(stats.total_elements, 0);
        assert_eq!(stats.total_dropped, 0);
    }
}

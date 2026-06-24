//! # Distributed Publish-Subscribe
//!
//! Topic-based messaging across a cluster, inspired by Pekko's DistributedPubSub.
//!
//! Provides:
//! - Topic registration and subscription management
//! - Publish messages to all subscribers of a topic
//! - Send messages to one subscriber (round-robin) of a topic
//! - Group subscriptions for load-balanced consumption
//! - Dead letter tracking for undeliverable messages

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Unique subscriber identifier
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct SubscriberId(String);

impl SubscriberId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SubscriberId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Topic name
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Topic(String);

impl Topic {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Subscription group for load-balanced consumption
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct SubscriptionGroup(String);

impl SubscriptionGroup {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A message envelope for pub/sub
#[derive(Debug, Clone)]
pub struct PubSubMessage {
    pub topic: Topic,
    pub payload: Vec<u8>,
    pub sender: Option<SubscriberId>,
    pub timestamp: std::time::Instant,
}

impl PubSubMessage {
    pub fn new(topic: Topic, payload: Vec<u8>) -> Self {
        Self {
            topic,
            payload,
            sender: None,
            timestamp: std::time::Instant::now(),
        }
    }

    pub fn with_sender(mut self, sender: SubscriberId) -> Self {
        self.sender = Some(sender);
        self
    }
}

/// Subscription handle returned on subscribe
#[derive(Debug)]
pub struct Subscription {
    pub id: SubscriberId,
    pub topic: Topic,
    pub group: Option<SubscriptionGroup>,
    pub receiver: mpsc::UnboundedReceiver<PubSubMessage>,
}

/// Status of a publish operation
#[derive(Debug, Clone, PartialEq)]
pub enum PublishStatus {
    /// Message delivered to N subscribers
    Delivered(usize),
    /// No subscribers for the topic
    NoSubscribers,
    /// Send to one subscriber (round-robin)
    SentToOne(SubscriberId),
}

/// Dead letter entry
#[derive(Debug, Clone)]
pub struct DeadLetter {
    pub topic: Topic,
    pub payload_size: usize,
    pub timestamp: std::time::Instant,
    pub reason: DeadLetterReason,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DeadLetterReason {
    NoSubscribers,
    SubscriberDropped,
    SendFailed,
}

/// Subscriber entry
struct SubscriberEntry {
    id: SubscriberId,
    sender: mpsc::UnboundedSender<PubSubMessage>,
    group: Option<SubscriptionGroup>,
}

/// Stats for the mediator
#[derive(Debug, Clone)]
pub struct MediatorStats {
    pub total_topics: usize,
    pub total_subscribers: usize,
    pub messages_published: u64,
    pub messages_delivered: u64,
    pub dead_letters: u64,
}

// ---------------------------------------------------------------------------
// DistributedPubSubMediator
// ---------------------------------------------------------------------------

/// Central mediator for distributed publish-subscribe
pub struct DistributedPubSubMediator {
    /// Topic → list of subscribers
    topics: Arc<RwLock<HashMap<Topic, Vec<SubscriberEntry>>>>,
    /// Round-robin counters per topic
    rr_counters: Arc<RwLock<HashMap<Topic, usize>>>,
    /// Dead letters
    dead_letters: Arc<RwLock<Vec<DeadLetter>>>,
    /// Stats
    messages_published: Arc<AtomicU64>,
    messages_delivered: Arc<AtomicU64>,
    dead_letter_count: Arc<AtomicU64>,
    /// Max dead letters to retain
    max_dead_letters: usize,
}

impl DistributedPubSubMediator {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
            rr_counters: Arc::new(RwLock::new(HashMap::new())),
            dead_letters: Arc::new(RwLock::new(Vec::new())),
            messages_published: Arc::new(AtomicU64::new(0)),
            messages_delivered: Arc::new(AtomicU64::new(0)),
            dead_letter_count: Arc::new(AtomicU64::new(0)),
            max_dead_letters: 1000,
        }
    }

    pub fn with_max_dead_letters(mut self, max: usize) -> Self {
        self.max_dead_letters = max;
        self
    }

    /// Subscribe to a topic
    pub async fn subscribe(&self, topic: Topic, subscriber_id: SubscriberId) -> Subscription {
        self.subscribe_with_group(topic, subscriber_id, None).await
    }

    /// Subscribe to a topic with a group
    pub async fn subscribe_with_group(
        &self,
        topic: Topic,
        subscriber_id: SubscriberId,
        group: Option<SubscriptionGroup>,
    ) -> Subscription {
        let (tx, rx) = mpsc::unbounded_channel();
        let entry = SubscriberEntry {
            id: subscriber_id.clone(),
            sender: tx,
            group: group.clone(),
        };

        let mut topics = self.topics.write().await;
        topics.entry(topic.clone()).or_default().push(entry);

        Subscription {
            id: subscriber_id,
            topic,
            group,
            receiver: rx,
        }
    }

    /// Unsubscribe from a topic
    pub async fn unsubscribe(&self, topic: &Topic, subscriber_id: &SubscriberId) -> bool {
        let mut topics = self.topics.write().await;
        if let Some(subscribers) = topics.get_mut(topic) {
            let before = subscribers.len();
            subscribers.retain(|s| &s.id != subscriber_id);
            let removed = subscribers.len() < before;
            if subscribers.is_empty() {
                topics.remove(topic);
            }
            removed
        } else {
            false
        }
    }

    /// Publish a message to all subscribers of a topic
    pub async fn publish(&self, message: PubSubMessage) -> PublishStatus {
        self.messages_published.fetch_add(1, Ordering::Relaxed);

        let mut topics = self.topics.write().await;
        let subscribers = match topics.get_mut(&message.topic) {
            Some(subs) if !subs.is_empty() => subs,
            _ => {
                self.record_dead_letter(&message.topic, message.payload.len(), DeadLetterReason::NoSubscribers).await;
                return PublishStatus::NoSubscribers;
            }
        };

        let mut delivered = 0usize;
        let mut to_remove = Vec::new();

        for (i, sub) in subscribers.iter().enumerate() {
            match sub.sender.send(message.clone()) {
                Ok(()) => delivered += 1,
                Err(_) => to_remove.push(i),
            }
        }

        // Remove dropped subscribers (reverse order to preserve indices)
        for i in to_remove.into_iter().rev() {
            subscribers.remove(i);
        }

        if subscribers.is_empty() {
            topics.remove(&message.topic);
        }

        self.messages_delivered.fetch_add(delivered as u64, Ordering::Relaxed);
        PublishStatus::Delivered(delivered)
    }

    /// Send a message to one subscriber (round-robin)
    pub async fn send_to_one(&self, message: PubSubMessage) -> PublishStatus {
        self.messages_published.fetch_add(1, Ordering::Relaxed);

        let topics = self.topics.read().await;
        let subscribers = match topics.get(&message.topic) {
            Some(subs) if !subs.is_empty() => subs,
            _ => {
                self.record_dead_letter(&message.topic, message.payload.len(), DeadLetterReason::NoSubscribers).await;
                return PublishStatus::NoSubscribers;
            }
        };

        let mut counters = self.rr_counters.write().await;
        let counter = counters.entry(message.topic.clone()).or_insert(0);
        let idx = *counter % subscribers.len();
        *counter = counter.wrapping_add(1);

        let sub = &subscribers[idx];
        match sub.sender.send(message) {
            Ok(()) => {
                self.messages_delivered.fetch_add(1, Ordering::Relaxed);
                PublishStatus::SentToOne(sub.id.clone())
            }
            Err(_) => {
                self.dead_letter_count.fetch_add(1, Ordering::Relaxed);
                PublishStatus::NoSubscribers
            }
        }
    }

    /// Send to one subscriber within a specific group (round-robin within group)
    pub async fn send_to_group(&self, message: PubSubMessage, group: &SubscriptionGroup) -> PublishStatus {
        self.messages_published.fetch_add(1, Ordering::Relaxed);

        let topics = self.topics.read().await;
        let subscribers = match topics.get(&message.topic) {
            Some(subs) => subs,
            None => {
                self.record_dead_letter(&message.topic, message.payload.len(), DeadLetterReason::NoSubscribers).await;
                return PublishStatus::NoSubscribers;
            }
        };

        let group_subs: Vec<&SubscriberEntry> = subscribers
            .iter()
            .filter(|s| s.group.as_ref() == Some(group))
            .collect();

        if group_subs.is_empty() {
            self.record_dead_letter(&message.topic, message.payload.len(), DeadLetterReason::NoSubscribers).await;
            return PublishStatus::NoSubscribers;
        }

        let group_key = Topic::new(format!("{}::{}", message.topic.as_str(), group.as_str()));
        let mut counters = self.rr_counters.write().await;
        let counter = counters.entry(group_key).or_insert(0);
        let idx = *counter % group_subs.len();
        *counter = counter.wrapping_add(1);

        let sub = group_subs[idx];
        match sub.sender.send(message) {
            Ok(()) => {
                self.messages_delivered.fetch_add(1, Ordering::Relaxed);
                PublishStatus::SentToOne(sub.id.clone())
            }
            Err(_) => {
                self.dead_letter_count.fetch_add(1, Ordering::Relaxed);
                PublishStatus::NoSubscribers
            }
        }
    }

    /// Get list of topics
    pub async fn topics(&self) -> Vec<Topic> {
        self.topics.read().await.keys().cloned().collect()
    }

    /// Get subscriber count for a topic
    pub async fn subscriber_count(&self, topic: &Topic) -> usize {
        self.topics.read().await.get(topic).map_or(0, |s| s.len())
    }

    /// Get all subscriber IDs for a topic
    pub async fn subscribers(&self, topic: &Topic) -> Vec<SubscriberId> {
        self.topics.read().await
            .get(topic)
            .map_or_else(Vec::new, |subs| subs.iter().map(|s| s.id.clone()).collect())
    }

    /// Check if a topic has subscribers
    pub async fn has_subscribers(&self, topic: &Topic) -> bool {
        self.subscriber_count(topic).await > 0
    }

    /// Get dead letters
    pub async fn dead_letters(&self) -> Vec<DeadLetter> {
        self.dead_letters.read().await.clone()
    }

    /// Clear dead letters
    pub async fn clear_dead_letters(&self) {
        self.dead_letters.write().await.clear();
    }

    /// Get mediator stats
    pub async fn stats(&self) -> MediatorStats {
        let topics = self.topics.read().await;
        let total_subscribers: usize = topics.values().map(|s| s.len()).sum();
        MediatorStats {
            total_topics: topics.len(),
            total_subscribers,
            messages_published: self.messages_published.load(Ordering::Relaxed),
            messages_delivered: self.messages_delivered.load(Ordering::Relaxed),
            dead_letters: self.dead_letter_count.load(Ordering::Relaxed),
        }
    }

    /// Remove all subscribers and topics
    pub async fn clear(&self) {
        self.topics.write().await.clear();
        self.rr_counters.write().await.clear();
    }

    async fn record_dead_letter(&self, topic: &Topic, payload_size: usize, reason: DeadLetterReason) {
        self.dead_letter_count.fetch_add(1, Ordering::Relaxed);
        let mut dead_letters = self.dead_letters.write().await;
        if dead_letters.len() >= self.max_dead_letters {
            dead_letters.remove(0);
        }
        dead_letters.push(DeadLetter {
            topic: topic.clone(),
            payload_size,
            timestamp: std::time::Instant::now(),
            reason,
        });
    }
}

impl Default for DistributedPubSubMediator {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for DistributedPubSubMediator {
    fn clone(&self) -> Self {
        Self {
            topics: Arc::clone(&self.topics),
            rr_counters: Arc::clone(&self.rr_counters),
            dead_letters: Arc::clone(&self.dead_letters),
            messages_published: Arc::clone(&self.messages_published),
            messages_delivered: Arc::clone(&self.messages_delivered),
            dead_letter_count: Arc::clone(&self.dead_letter_count),
            max_dead_letters: self.max_dead_letters,
        }
    }
}

// ---------------------------------------------------------------------------
// TopicRouter — multi-topic subscription helper
// ---------------------------------------------------------------------------

/// Manages subscriptions to multiple topics
pub struct TopicRouter {
    mediator: DistributedPubSubMediator,
    subscriptions: Vec<(Topic, SubscriberId)>,
}

impl TopicRouter {
    pub fn new(mediator: DistributedPubSubMediator) -> Self {
        Self {
            mediator,
            subscriptions: Vec::new(),
        }
    }

    /// Subscribe to multiple topics
    pub async fn subscribe_all(
        &mut self,
        subscriber_id: &SubscriberId,
        topics: &[Topic],
    ) -> Vec<Subscription> {
        let mut result = Vec::new();
        for topic in topics {
            let sub_id = SubscriberId::new(format!("{}:{}", subscriber_id.as_str(), topic.as_str()));
            let sub = self.mediator.subscribe(topic.clone(), sub_id.clone()).await;
            self.subscriptions.push((topic.clone(), sub_id));
            result.push(sub);
        }
        result
    }

    /// Unsubscribe from all topics
    pub async fn unsubscribe_all(&mut self) {
        for (topic, id) in self.subscriptions.drain(..) {
            self.mediator.unsubscribe(&topic, &id).await;
        }
    }

    /// Broadcast to multiple topics
    pub async fn broadcast(&self, topics: &[Topic], payload: Vec<u8>) -> Vec<PublishStatus> {
        let mut results = Vec::new();
        for topic in topics {
            let msg = PubSubMessage::new(topic.clone(), payload.clone());
            results.push(self.mediator.publish(msg).await);
        }
        results
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(topic: &str, data: &[u8]) -> PubSubMessage {
        PubSubMessage::new(Topic::new(topic), data.to_vec())
    }

    #[tokio::test]
    async fn test_subscribe_and_publish() {
        let mediator = DistributedPubSubMediator::new();
        let topic = Topic::new("events");
        let mut sub = mediator.subscribe(topic.clone(), SubscriberId::new("s1")).await;

        let status = mediator.publish(msg("events", b"hello")).await;
        assert_eq!(status, PublishStatus::Delivered(1));

        let received = sub.receiver.recv().await.unwrap();
        assert_eq!(received.payload, b"hello");
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let mediator = DistributedPubSubMediator::new();
        let topic = Topic::new("news");

        let mut sub1 = mediator.subscribe(topic.clone(), SubscriberId::new("s1")).await;
        let mut sub2 = mediator.subscribe(topic.clone(), SubscriberId::new("s2")).await;

        let status = mediator.publish(msg("news", b"update")).await;
        assert_eq!(status, PublishStatus::Delivered(2));

        assert_eq!(sub1.receiver.recv().await.unwrap().payload, b"update");
        assert_eq!(sub2.receiver.recv().await.unwrap().payload, b"update");
    }

    #[tokio::test]
    async fn test_publish_no_subscribers() {
        let mediator = DistributedPubSubMediator::new();
        let status = mediator.publish(msg("empty", b"data")).await;
        assert_eq!(status, PublishStatus::NoSubscribers);
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let mediator = DistributedPubSubMediator::new();
        let topic = Topic::new("t1");
        let _sub = mediator.subscribe(topic.clone(), SubscriberId::new("s1")).await;

        assert_eq!(mediator.subscriber_count(&topic).await, 1);
        assert!(mediator.unsubscribe(&topic, &SubscriberId::new("s1")).await);
        assert_eq!(mediator.subscriber_count(&topic).await, 0);
    }

    #[tokio::test]
    async fn test_unsubscribe_nonexistent() {
        let mediator = DistributedPubSubMediator::new();
        let topic = Topic::new("t1");
        assert!(!mediator.unsubscribe(&topic, &SubscriberId::new("nobody")).await);
    }

    #[tokio::test]
    async fn test_send_to_one_round_robin() {
        let mediator = DistributedPubSubMediator::new();
        let topic = Topic::new("work");

        let mut sub1 = mediator.subscribe(topic.clone(), SubscriberId::new("w1")).await;
        let mut sub2 = mediator.subscribe(topic.clone(), SubscriberId::new("w2")).await;

        // First message → w1
        let s1 = mediator.send_to_one(msg("work", b"job1")).await;
        assert!(matches!(s1, PublishStatus::SentToOne(_)));

        // Second message → w2
        let s2 = mediator.send_to_one(msg("work", b"job2")).await;
        assert!(matches!(s2, PublishStatus::SentToOne(_)));

        // Each should have received one message
        assert!(sub1.receiver.try_recv().is_ok());
        assert!(sub2.receiver.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_group_subscription() {
        let mediator = DistributedPubSubMediator::new();
        let topic = Topic::new("tasks");
        let group = SubscriptionGroup::new("workers");

        let mut sub1 = mediator.subscribe_with_group(
            topic.clone(), SubscriberId::new("g1"), Some(group.clone())
        ).await;
        let mut sub2 = mediator.subscribe_with_group(
            topic.clone(), SubscriberId::new("g2"), Some(group.clone())
        ).await;

        // Send to group — only one should receive
        let status = mediator.send_to_group(msg("tasks", b"task1"), &group).await;
        assert!(matches!(status, PublishStatus::SentToOne(_)));

        let r1 = sub1.receiver.try_recv().is_ok();
        let r2 = sub2.receiver.try_recv().is_ok();
        assert!(r1 ^ r2); // XOR: exactly one received
    }

    #[tokio::test]
    async fn test_dead_letters() {
        let mediator = DistributedPubSubMediator::new();
        mediator.publish(msg("ghost", b"lost")).await;

        let dead = mediator.dead_letters().await;
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].reason, DeadLetterReason::NoSubscribers);
    }

    #[tokio::test]
    async fn test_dead_letter_limit() {
        let mediator = DistributedPubSubMediator::new().with_max_dead_letters(3);

        for i in 0..5 {
            mediator.publish(msg("void", &[i])).await;
        }

        let dead = mediator.dead_letters().await;
        assert_eq!(dead.len(), 3); // Oldest ones evicted
    }

    #[tokio::test]
    async fn test_stats() {
        let mediator = DistributedPubSubMediator::new();
        let topic = Topic::new("metrics");
        let _sub = mediator.subscribe(topic.clone(), SubscriberId::new("s1")).await;

        mediator.publish(msg("metrics", b"a")).await;
        mediator.publish(msg("metrics", b"b")).await;
        mediator.publish(msg("nowhere", b"c")).await;

        let stats = mediator.stats().await;
        assert_eq!(stats.total_topics, 1);
        assert_eq!(stats.total_subscribers, 1);
        assert_eq!(stats.messages_published, 3);
        assert_eq!(stats.messages_delivered, 2);
        assert_eq!(stats.dead_letters, 1);
    }

    #[tokio::test]
    async fn test_topics_list() {
        let mediator = DistributedPubSubMediator::new();
        let _s1 = mediator.subscribe(Topic::new("a"), SubscriberId::new("s1")).await;
        let _s2 = mediator.subscribe(Topic::new("b"), SubscriberId::new("s2")).await;

        let mut topics = mediator.topics().await;
        topics.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        assert_eq!(topics.len(), 2);
        assert_eq!(topics[0].as_str(), "a");
        assert_eq!(topics[1].as_str(), "b");
    }

    #[tokio::test]
    async fn test_has_subscribers() {
        let mediator = DistributedPubSubMediator::new();
        let topic = Topic::new("check");
        assert!(!mediator.has_subscribers(&topic).await);

        let _sub = mediator.subscribe(topic.clone(), SubscriberId::new("s1")).await;
        assert!(mediator.has_subscribers(&topic).await);
    }

    #[tokio::test]
    async fn test_clear() {
        let mediator = DistributedPubSubMediator::new();
        let _s = mediator.subscribe(Topic::new("x"), SubscriberId::new("s")).await;
        mediator.clear().await;
        assert_eq!(mediator.topics().await.len(), 0);
    }

    #[tokio::test]
    async fn test_message_with_sender() {
        let mediator = DistributedPubSubMediator::new();
        let topic = Topic::new("chat");
        let mut sub = mediator.subscribe(topic.clone(), SubscriberId::new("listener")).await;

        let m = PubSubMessage::new(topic.clone(), b"hi".to_vec())
            .with_sender(SubscriberId::new("alice"));
        mediator.publish(m).await;

        let received = sub.receiver.recv().await.unwrap();
        assert_eq!(received.sender.unwrap().as_str(), "alice");
    }

    #[tokio::test]
    async fn test_subscriber_ids() {
        let mediator = DistributedPubSubMediator::new();
        let topic = Topic::new("t");
        let _s1 = mediator.subscribe(topic.clone(), SubscriberId::new("a")).await;
        let _s2 = mediator.subscribe(topic.clone(), SubscriberId::new("b")).await;

        let ids = mediator.subscribers(&topic).await;
        assert_eq!(ids.len(), 2);
    }

    #[tokio::test]
    async fn test_clone_shares_state() {
        let mediator = DistributedPubSubMediator::new();
        let clone = mediator.clone();
        let topic = Topic::new("shared");

        let _sub = mediator.subscribe(topic.clone(), SubscriberId::new("s1")).await;
        assert_eq!(clone.subscriber_count(&topic).await, 1);
    }

    #[tokio::test]
    async fn test_topic_router_broadcast() {
        let mediator = DistributedPubSubMediator::new();
        let topics = vec![Topic::new("a"), Topic::new("b")];

        let mut router = TopicRouter::new(mediator.clone());
        let mut subs = router.subscribe_all(&SubscriberId::new("r"), &topics).await;

        let results = router.broadcast(&topics, b"data".to_vec()).await;
        assert!(results.iter().all(|s| matches!(s, PublishStatus::Delivered(1))));

        assert!(subs[0].receiver.try_recv().is_ok());
        assert!(subs[1].receiver.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_topic_router_unsubscribe_all() {
        let mediator = DistributedPubSubMediator::new();
        let topics = vec![Topic::new("x"), Topic::new("y")];

        let mut router = TopicRouter::new(mediator.clone());
        let _subs = router.subscribe_all(&SubscriberId::new("r"), &topics).await;

        assert!(mediator.has_subscribers(&topics[0]).await);
        router.unsubscribe_all().await;
        assert!(!mediator.has_subscribers(&topics[0]).await);
    }

    #[tokio::test]
    async fn test_dropped_subscriber_cleanup() {
        let mediator = DistributedPubSubMediator::new();
        let topic = Topic::new("cleanup");

        // Subscribe and immediately drop the subscription (receiver)
        let sub = mediator.subscribe(topic.clone(), SubscriberId::new("dropper")).await;
        drop(sub.receiver);

        // Publishing should clean up the dropped subscriber
        let status = mediator.publish(msg("cleanup", b"test")).await;
        // After cleanup, no more subscribers
        assert_eq!(mediator.subscriber_count(&topic).await, 0);
    }
}

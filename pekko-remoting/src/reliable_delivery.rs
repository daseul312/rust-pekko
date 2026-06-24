// reliable_delivery.rs - Protocol-level reliable message delivery for remoting
// Ensures at-least-once or at-most-once delivery semantics across network boundaries

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::time::{Duration, Instant};

// ============================================================================
// DeliverySemantics
// ============================================================================

/// Delivery guarantee level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliverySemantics {
    /// Best effort, no guarantees (fire-and-forget)
    BestEffort,
    /// At most once delivery (no retries, dedup)
    AtMostOnce,
    /// At least once delivery (retries until ack)
    AtLeastOnce,
}

// ============================================================================
// DeliveryConfig
// ============================================================================

/// Configuration for reliable delivery
#[derive(Debug, Clone)]
pub struct ReliableDeliveryConfig {
    pub semantics: DeliverySemantics,
    /// How long to wait for an ack before retrying
    pub ack_timeout: Duration,
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Backoff multiplier for retries
    pub backoff_multiplier: f64,
    /// Maximum backoff duration
    pub max_backoff: Duration,
    /// Window size for flow control (max in-flight messages)
    pub window_size: usize,
    /// Deduplication window (how long to remember delivered IDs)
    pub dedup_window: Duration,
    /// Whether to preserve ordering within a destination
    pub ordered: bool,
}

impl Default for ReliableDeliveryConfig {
    fn default() -> Self {
        Self {
            semantics: DeliverySemantics::AtLeastOnce,
            ack_timeout: Duration::from_secs(5),
            max_retries: 5,
            backoff_multiplier: 2.0,
            max_backoff: Duration::from_secs(60),
            window_size: 100,
            dedup_window: Duration::from_secs(300),
            ordered: false,
        }
    }
}

impl ReliableDeliveryConfig {
    pub fn best_effort() -> Self {
        Self {
            semantics: DeliverySemantics::BestEffort,
            max_retries: 0,
            ..Default::default()
        }
    }

    pub fn at_most_once() -> Self {
        Self {
            semantics: DeliverySemantics::AtMostOnce,
            max_retries: 0,
            ..Default::default()
        }
    }

    pub fn at_least_once() -> Self {
        Self::default()
    }

    pub fn with_retries(mut self, max: u32) -> Self {
        self.max_retries = max;
        self
    }

    pub fn with_window(mut self, size: usize) -> Self {
        self.window_size = size;
        self
    }

    pub fn with_ordered(mut self, ordered: bool) -> Self {
        self.ordered = ordered;
        self
    }
}

// ============================================================================
// SequenceNr & DeliveryId
// ============================================================================

/// Monotonically increasing sequence number
pub type SequenceNr = u64;

/// Unique delivery identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeliveryId {
    pub source: String,
    pub destination: String,
    pub sequence_nr: SequenceNr,
}

impl DeliveryId {
    pub fn new(source: &str, destination: &str, seq: SequenceNr) -> Self {
        Self {
            source: source.to_string(),
            destination: destination.to_string(),
            sequence_nr: seq,
        }
    }
}

impl fmt::Display for DeliveryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}→{}#{}", self.source, self.destination, self.sequence_nr)
    }
}

// ============================================================================
// InFlightMessage
// ============================================================================

/// A message currently in-flight awaiting acknowledgment
#[derive(Debug, Clone)]
struct InFlightMessage {
    delivery_id: DeliveryId,
    payload: Vec<u8>,
    first_sent: Instant,
    last_sent: Instant,
    attempts: u32,
    next_retry_at: Instant,
}

impl InFlightMessage {
    fn new(delivery_id: DeliveryId, payload: Vec<u8>, ack_timeout: Duration) -> Self {
        let now = Instant::now();
        Self {
            delivery_id,
            payload,
            first_sent: now,
            last_sent: now,
            attempts: 1,
            next_retry_at: now + ack_timeout,
        }
    }

    fn should_retry(&self, now: Instant) -> bool {
        now >= self.next_retry_at
    }

    fn record_retry(&mut self, config: &ReliableDeliveryConfig) {
        self.attempts += 1;
        self.last_sent = Instant::now();
        let backoff = config.ack_timeout.mul_f64(
            config.backoff_multiplier.powi((self.attempts - 1) as i32),
        );
        let capped = backoff.min(config.max_backoff);
        self.next_retry_at = Instant::now() + capped;
    }

    fn total_duration(&self) -> Duration {
        self.first_sent.elapsed()
    }
}

// ============================================================================
// Acknowledgment
// ============================================================================

/// An acknowledgment for a delivered message
#[derive(Debug, Clone)]
pub struct Ack {
    pub delivery_id: DeliveryId,
    pub received_at: Instant,
    pub cumulative: bool,
}

impl Ack {
    pub fn single(delivery_id: DeliveryId) -> Self {
        Self {
            delivery_id,
            received_at: Instant::now(),
            cumulative: false,
        }
    }

    pub fn cumulative(delivery_id: DeliveryId) -> Self {
        Self {
            delivery_id,
            received_at: Instant::now(),
            cumulative: true,
        }
    }
}

// ============================================================================
// ReliableDelivery
// ============================================================================

/// Protocol-level reliable delivery manager
pub struct ReliableDelivery {
    config: ReliableDeliveryConfig,
    /// In-flight messages keyed by sequence number
    in_flight: HashMap<SequenceNr, InFlightMessage>,
    /// Next sequence number to assign
    next_seq: SequenceNr,
    /// Pending messages waiting for window space
    pending: VecDeque<(String, Vec<u8>)>,
    /// Deduplication set: recently delivered IDs
    delivered: HashMap<DeliveryId, Instant>,
    /// Source identity
    source_id: String,
    stats: ReliableDeliveryStats,
}

/// Statistics for reliable delivery
#[derive(Debug, Clone, Default)]
pub struct ReliableDeliveryStats {
    pub messages_sent: u64,
    pub messages_acked: u64,
    pub messages_retried: u64,
    pub messages_failed: u64,
    pub duplicates_detected: u64,
    pub window_full_count: u64,
    pub cumulative_acks: u64,
}

impl ReliableDelivery {
    pub fn new(source_id: &str, config: ReliableDeliveryConfig) -> Self {
        Self {
            config,
            in_flight: HashMap::new(),
            next_seq: 1,
            pending: VecDeque::new(),
            delivered: HashMap::new(),
            source_id: source_id.to_string(),
            stats: ReliableDeliveryStats::default(),
        }
    }

    /// Send a message with reliable delivery
    pub fn send(&mut self, destination: &str, payload: Vec<u8>) -> SendOutcome {
        if self.config.semantics == DeliverySemantics::BestEffort {
            self.stats.messages_sent += 1;
            let seq = self.next_seq;
            self.next_seq += 1;
            return SendOutcome::Sent(DeliveryId::new(&self.source_id, destination, seq));
        }

        // Check flow control window
        if self.in_flight.len() >= self.config.window_size {
            self.stats.window_full_count += 1;
            if self.config.ordered {
                self.pending.push_back((destination.to_string(), payload));
                return SendOutcome::Queued(self.pending.len());
            }
            return SendOutcome::WindowFull;
        }

        self.do_send(destination, payload)
    }

    fn do_send(&mut self, destination: &str, payload: Vec<u8>) -> SendOutcome {
        let seq = self.next_seq;
        self.next_seq += 1;
        let delivery_id = DeliveryId::new(&self.source_id, destination, seq);

        if self.config.semantics == DeliverySemantics::AtLeastOnce {
            let msg = InFlightMessage::new(delivery_id.clone(), payload, self.config.ack_timeout);
            self.in_flight.insert(seq, msg);
        }

        self.stats.messages_sent += 1;
        SendOutcome::Sent(delivery_id)
    }

    /// Process an acknowledgment
    pub fn acknowledge(&mut self, ack: &Ack) -> AckResult {
        let seq = ack.delivery_id.sequence_nr;

        if ack.cumulative {
            // Cumulative ack: acknowledge all messages up to this sequence number
            let to_remove: Vec<SequenceNr> = self
                .in_flight
                .keys()
                .filter(|&&s| s <= seq)
                .cloned()
                .collect();
            let count = to_remove.len();
            for s in to_remove {
                self.in_flight.remove(&s);
                self.stats.messages_acked += 1;
            }
            self.stats.cumulative_acks += 1;
            self.try_send_pending();
            AckResult::CumulativeAck(count)
        } else if self.in_flight.remove(&seq).is_some() {
            self.stats.messages_acked += 1;
            self.try_send_pending();
            AckResult::Acknowledged
        } else {
            AckResult::Unknown
        }
    }

    /// Try to send pending messages now that window has space
    fn try_send_pending(&mut self) {
        while self.in_flight.len() < self.config.window_size {
            if let Some((dest, payload)) = self.pending.pop_front() {
                self.do_send(&dest, payload);
            } else {
                break;
            }
        }
    }

    /// Check for messages that need retry
    pub fn check_retries(&mut self) -> Vec<RetryAction> {
        let now = Instant::now();
        let max_retries = self.config.max_retries;
        let mut actions = Vec::new();

        let seqs: Vec<SequenceNr> = self.in_flight.keys().cloned().collect();
        for seq in seqs {
            if let Some(msg) = self.in_flight.get(&seq) {
                if !msg.should_retry(now) {
                    continue;
                }
                if msg.attempts >= max_retries {
                    // Give up
                    let failed = self.in_flight.remove(&seq).unwrap();
                    let total_duration = failed.total_duration();
                    self.stats.messages_failed += 1;
                    actions.push(RetryAction::GiveUp {
                        delivery_id: failed.delivery_id,
                        attempts: failed.attempts,
                        total_duration,
                    });
                } else {
                    // Retry
                    let config = self.config.clone();
                    let msg = self.in_flight.get_mut(&seq).unwrap();
                    msg.record_retry(&config);
                    self.stats.messages_retried += 1;
                    actions.push(RetryAction::Retry {
                        delivery_id: msg.delivery_id.clone(),
                        payload: msg.payload.clone(),
                        attempt: msg.attempts,
                    });
                }
            }
        }
        actions
    }

    /// Check if a message is a duplicate (receiver-side dedup)
    pub fn is_duplicate(&mut self, delivery_id: &DeliveryId) -> bool {
        // Clean old entries
        let cutoff = Instant::now() - self.config.dedup_window;
        self.delivered.retain(|_, t| *t > cutoff);

        if self.delivered.contains_key(delivery_id) {
            self.stats.duplicates_detected += 1;
            true
        } else {
            self.delivered.insert(delivery_id.clone(), Instant::now());
            false
        }
    }

    /// Get number of in-flight messages
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    /// Get number of pending messages
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Get window utilization (0.0 to 1.0)
    pub fn window_utilization(&self) -> f64 {
        if self.config.window_size == 0 {
            return 1.0;
        }
        self.in_flight.len() as f64 / self.config.window_size as f64
    }

    /// Get statistics
    pub fn stats(&self) -> &ReliableDeliveryStats {
        &self.stats
    }

    /// Get current sequence number
    pub fn current_sequence(&self) -> SequenceNr {
        self.next_seq - 1
    }

    /// Get delivery semantics
    pub fn semantics(&self) -> DeliverySemantics {
        self.config.semantics
    }
}

/// Outcome of a send operation
#[derive(Debug, Clone)]
pub enum SendOutcome {
    /// Message sent (may be in-flight awaiting ack)
    Sent(DeliveryId),
    /// Message queued (waiting for window space)
    Queued(usize),
    /// Window is full and ordering not required
    WindowFull,
}

/// Result of acknowledging a message
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckResult {
    /// Single message acknowledged
    Acknowledged,
    /// Cumulative ack processed (N messages acknowledged)
    CumulativeAck(usize),
    /// Unknown sequence number (already acked or never sent)
    Unknown,
}

/// Action to take for retry
#[derive(Debug, Clone)]
pub enum RetryAction {
    /// Retry sending this message
    Retry {
        delivery_id: DeliveryId,
        payload: Vec<u8>,
        attempt: u32,
    },
    /// Give up on this message
    GiveUp {
        delivery_id: DeliveryId,
        attempts: u32,
        total_duration: Duration,
    },
}

impl fmt::Debug for ReliableDelivery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReliableDelivery")
            .field("source", &self.source_id)
            .field("semantics", &self.config.semantics)
            .field("in_flight", &self.in_flight.len())
            .field("pending", &self.pending.len())
            .finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_delivery() -> ReliableDelivery {
        ReliableDelivery::new("source-1", ReliableDeliveryConfig::at_least_once())
    }

    #[test]
    fn test_send_assigns_sequence() {
        let mut rd = setup_delivery();
        if let SendOutcome::Sent(id) = rd.send("dest-1", vec![1, 2, 3]) {
            assert_eq!(id.sequence_nr, 1);
            assert_eq!(id.source, "source-1");
            assert_eq!(id.destination, "dest-1");
        } else {
            panic!("Expected Sent");
        }
    }

    #[test]
    fn test_sequence_increments() {
        let mut rd = setup_delivery();
        rd.send("dest-1", vec![1]);
        rd.send("dest-1", vec![2]);
        assert_eq!(rd.current_sequence(), 2);
    }

    #[test]
    fn test_in_flight_tracking() {
        let mut rd = setup_delivery();
        rd.send("dest-1", vec![1]);
        rd.send("dest-1", vec![2]);
        assert_eq!(rd.in_flight_count(), 2);
    }

    #[test]
    fn test_acknowledge() {
        let mut rd = setup_delivery();
        let id = if let SendOutcome::Sent(id) = rd.send("dest-1", vec![1]) {
            id
        } else {
            panic!("Expected Sent");
        };
        let result = rd.acknowledge(&Ack::single(id));
        assert_eq!(result, AckResult::Acknowledged);
        assert_eq!(rd.in_flight_count(), 0);
    }

    #[test]
    fn test_cumulative_ack() {
        let mut rd = setup_delivery();
        rd.send("dest-1", vec![1]);
        rd.send("dest-1", vec![2]);
        rd.send("dest-1", vec![3]);
        assert_eq!(rd.in_flight_count(), 3);

        let ack_id = DeliveryId::new("source-1", "dest-1", 2);
        let result = rd.acknowledge(&Ack::cumulative(ack_id));
        assert_eq!(result, AckResult::CumulativeAck(2));
        assert_eq!(rd.in_flight_count(), 1);
    }

    #[test]
    fn test_unknown_ack() {
        let mut rd = setup_delivery();
        let fake_id = DeliveryId::new("source-1", "dest-1", 999);
        let result = rd.acknowledge(&Ack::single(fake_id));
        assert_eq!(result, AckResult::Unknown);
    }

    #[test]
    fn test_window_full() {
        let config = ReliableDeliveryConfig::at_least_once().with_window(2);
        let mut rd = ReliableDelivery::new("src", config);
        rd.send("d", vec![1]);
        rd.send("d", vec![2]);
        let result = rd.send("d", vec![3]);
        assert!(matches!(result, SendOutcome::WindowFull));
    }

    #[test]
    fn test_ordered_queuing() {
        let config = ReliableDeliveryConfig::at_least_once()
            .with_window(2)
            .with_ordered(true);
        let mut rd = ReliableDelivery::new("src", config);
        rd.send("d", vec![1]);
        rd.send("d", vec![2]);
        let result = rd.send("d", vec![3]);
        assert!(matches!(result, SendOutcome::Queued(1)));
        assert_eq!(rd.pending_count(), 1);
    }

    #[test]
    fn test_pending_sent_after_ack() {
        let config = ReliableDeliveryConfig::at_least_once()
            .with_window(1)
            .with_ordered(true);
        let mut rd = ReliableDelivery::new("src", config);
        rd.send("d", vec![1]);
        rd.send("d", vec![2]); // queued
        assert_eq!(rd.in_flight_count(), 1);
        assert_eq!(rd.pending_count(), 1);

        let ack = Ack::single(DeliveryId::new("src", "d", 1));
        rd.acknowledge(&ack);
        // Pending message should now be in-flight
        assert_eq!(rd.in_flight_count(), 1);
        assert_eq!(rd.pending_count(), 0);
    }

    #[test]
    fn test_best_effort_no_tracking() {
        let mut rd = ReliableDelivery::new("src", ReliableDeliveryConfig::best_effort());
        rd.send("d", vec![1]);
        assert_eq!(rd.in_flight_count(), 0);
        assert_eq!(rd.stats().messages_sent, 1);
    }

    #[test]
    fn test_at_most_once_no_tracking() {
        let mut rd = ReliableDelivery::new("src", ReliableDeliveryConfig::at_most_once());
        rd.send("d", vec![1]);
        // AtMostOnce without retries behaves like best effort
        assert_eq!(rd.in_flight_count(), 0);
    }

    #[test]
    fn test_deduplication() {
        let mut rd = setup_delivery();
        let id = DeliveryId::new("src", "dest", 1);
        assert!(!rd.is_duplicate(&id));
        assert!(rd.is_duplicate(&id));
        assert_eq!(rd.stats().duplicates_detected, 1);
    }

    #[test]
    fn test_check_retries_gives_up() {
        let config = ReliableDeliveryConfig {
            ack_timeout: Duration::from_millis(1),
            max_retries: 1,
            ..ReliableDeliveryConfig::at_least_once()
        };
        let mut rd = ReliableDelivery::new("src", config);
        rd.send("d", vec![1]);
        std::thread::sleep(Duration::from_millis(5));

        let actions = rd.check_retries();
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], RetryAction::GiveUp { .. }));
        assert_eq!(rd.stats().messages_failed, 1);
    }

    #[test]
    fn test_check_retries_retries() {
        let config = ReliableDeliveryConfig {
            ack_timeout: Duration::from_millis(1),
            max_retries: 3,
            ..ReliableDeliveryConfig::at_least_once()
        };
        let mut rd = ReliableDelivery::new("src", config);
        rd.send("d", vec![1]);
        std::thread::sleep(Duration::from_millis(5));

        let actions = rd.check_retries();
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], RetryAction::Retry { attempt: 2, .. }));
        assert_eq!(rd.stats().messages_retried, 1);
    }

    #[test]
    fn test_window_utilization() {
        let config = ReliableDeliveryConfig::at_least_once().with_window(10);
        let mut rd = ReliableDelivery::new("src", config);
        assert_eq!(rd.window_utilization(), 0.0);
        rd.send("d", vec![1]);
        assert!((rd.window_utilization() - 0.1).abs() < 0.001);
    }

    #[test]
    fn test_delivery_id_display() {
        let id = DeliveryId::new("src", "dest", 42);
        assert_eq!(id.to_string(), "src→dest#42");
    }

    #[test]
    fn test_config_presets() {
        let be = ReliableDeliveryConfig::best_effort();
        assert_eq!(be.semantics, DeliverySemantics::BestEffort);
        assert_eq!(be.max_retries, 0);

        let amo = ReliableDeliveryConfig::at_most_once();
        assert_eq!(amo.semantics, DeliverySemantics::AtMostOnce);

        let alo = ReliableDeliveryConfig::at_least_once();
        assert_eq!(alo.semantics, DeliverySemantics::AtLeastOnce);
    }

    #[test]
    fn test_stats_tracking() {
        let mut rd = setup_delivery();
        rd.send("d", vec![1]);
        rd.send("d", vec![2]);
        let id1 = DeliveryId::new("source-1", "d", 1);
        rd.acknowledge(&Ack::single(id1));
        assert_eq!(rd.stats().messages_sent, 2);
        assert_eq!(rd.stats().messages_acked, 1);
    }

    #[test]
    fn test_semantics_getter() {
        let rd = setup_delivery();
        assert_eq!(rd.semantics(), DeliverySemantics::AtLeastOnce);
    }

    #[test]
    fn test_debug_display() {
        let rd = setup_delivery();
        let debug = format!("{:?}", rd);
        assert!(debug.contains("ReliableDelivery"));
        assert!(debug.contains("source-1"));
    }
}

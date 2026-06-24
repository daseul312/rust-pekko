// cluster_client.rs - ClusterClient for external→cluster communication
// Enables non-cluster systems to send messages into a cluster via contact points

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::time::{Duration, Instant};

// ============================================================================
// ContactPoint
// ============================================================================

/// A cluster contact point that the client can connect through
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContactPoint {
    pub address: String,
    pub port: u16,
    pub protocol: ContactProtocol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ContactProtocol {
    Tcp,
    Udp,
    Http,
    Https,
}

impl ContactPoint {
    pub fn new(address: &str, port: u16) -> Self {
        Self {
            address: address.to_string(),
            port,
            protocol: ContactProtocol::Tcp,
        }
    }

    pub fn with_protocol(mut self, protocol: ContactProtocol) -> Self {
        self.protocol = protocol;
        self
    }

    pub fn endpoint(&self) -> String {
        format!("{}://{}:{}", self.protocol.scheme(), self.address, self.port)
    }
}

impl ContactProtocol {
    pub fn scheme(&self) -> &'static str {
        match self {
            ContactProtocol::Tcp => "tcp",
            ContactProtocol::Udp => "udp",
            ContactProtocol::Http => "http",
            ContactProtocol::Https => "https",
        }
    }
}

impl fmt::Display for ContactPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.endpoint())
    }
}

// ============================================================================
// ClusterClientSettings
// ============================================================================

/// Configuration for ClusterClient behavior
#[derive(Debug, Clone)]
pub struct ClusterClientSettings {
    /// How often to refresh contact points
    pub refresh_interval: Duration,
    /// Timeout for establishing connection to a contact point
    pub connect_timeout: Duration,
    /// Max retries before marking a contact point as failed
    pub max_retries: u32,
    /// Buffer size for outgoing messages when no contact is available
    pub buffer_size: usize,
    /// Reconnection backoff base duration
    pub reconnect_backoff: Duration,
    /// Max reconnection backoff duration
    pub max_reconnect_backoff: Duration,
    /// Whether to auto-discover new contact points from the cluster
    pub auto_discover: bool,
    /// Max number of contact points to maintain
    pub max_contacts: usize,
}

impl Default for ClusterClientSettings {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(3),
            connect_timeout: Duration::from_secs(5),
            max_retries: 3,
            buffer_size: 1000,
            reconnect_backoff: Duration::from_millis(500),
            max_reconnect_backoff: Duration::from_secs(30),
            auto_discover: true,
            max_contacts: 10,
        }
    }
}

// ============================================================================
// ContactPointState
// ============================================================================

/// Tracked state for a contact point
#[derive(Debug, Clone)]
struct ContactPointState {
    contact: ContactPoint,
    status: ContactStatus,
    consecutive_failures: u32,
    last_success: Option<Instant>,
    last_failure: Option<Instant>,
    latency_ms: Option<f64>,
    messages_sent: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContactStatus {
    Connected,
    Connecting,
    Disconnected,
    Failed,
    Blacklisted,
}

impl ContactPointState {
    fn new(contact: ContactPoint) -> Self {
        Self {
            contact,
            status: ContactStatus::Disconnected,
            consecutive_failures: 0,
            last_success: None,
            last_failure: None,
            latency_ms: None,
            messages_sent: 0,
        }
    }

    fn record_success(&mut self, latency_ms: f64) {
        self.status = ContactStatus::Connected;
        self.consecutive_failures = 0;
        self.last_success = Some(Instant::now());
        self.latency_ms = Some(latency_ms);
        self.messages_sent += 1;
    }

    fn record_failure(&mut self, max_retries: u32) {
        self.consecutive_failures += 1;
        self.last_failure = Some(Instant::now());
        if self.consecutive_failures >= max_retries {
            self.status = ContactStatus::Failed;
        } else {
            self.status = ContactStatus::Disconnected;
        }
    }

    fn is_available(&self) -> bool {
        matches!(self.status, ContactStatus::Connected | ContactStatus::Disconnected)
    }
}

// ============================================================================
// ClusterClientMessage (buffered outbound messages)
// ============================================================================

/// A message buffered for delivery to the cluster
#[derive(Debug, Clone)]
pub struct BufferedMessage {
    pub target_path: String,
    pub payload: Vec<u8>,
    pub created_at: Instant,
    pub attempts: u32,
    pub priority: MessagePriority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MessagePriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl BufferedMessage {
    pub fn new(target_path: &str, payload: Vec<u8>) -> Self {
        Self {
            target_path: target_path.to_string(),
            payload,
            created_at: Instant::now(),
            attempts: 0,
            priority: MessagePriority::Normal,
        }
    }

    pub fn with_priority(mut self, priority: MessagePriority) -> Self {
        self.priority = priority;
        self
    }

    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }
}

// ============================================================================
// ClusterClient
// ============================================================================

/// Client for communicating with a Pekko cluster from outside
pub struct ClusterClient {
    contacts: HashMap<String, ContactPointState>,
    settings: ClusterClientSettings,
    buffer: VecDeque<BufferedMessage>,
    active_contact: Option<String>,
    discovered_contacts: HashSet<String>,
    stats: ClusterClientStats,
}

/// Statistics for the cluster client
#[derive(Debug, Clone, Default)]
pub struct ClusterClientStats {
    pub messages_sent: u64,
    pub messages_buffered: u64,
    pub messages_dropped: u64,
    pub contact_switches: u64,
    pub discovery_events: u64,
    pub total_failures: u64,
}

impl ClusterClient {
    pub fn new(initial_contacts: Vec<ContactPoint>, settings: ClusterClientSettings) -> Self {
        let mut contacts = HashMap::new();
        for cp in initial_contacts {
            let key = cp.endpoint();
            contacts.insert(key, ContactPointState::new(cp));
        }
        Self {
            contacts,
            settings,
            buffer: VecDeque::new(),
            active_contact: None,
            discovered_contacts: HashSet::new(),
            stats: ClusterClientStats::default(),
        }
    }

    pub fn with_defaults(initial_contacts: Vec<ContactPoint>) -> Self {
        Self::new(initial_contacts, ClusterClientSettings::default())
    }

    /// Send a message to an actor path in the cluster
    pub fn send(&mut self, target_path: &str, payload: Vec<u8>) -> SendResult {
        let msg = BufferedMessage::new(target_path, payload);
        self.send_buffered(msg)
    }

    /// Send a message with priority
    pub fn send_with_priority(
        &mut self,
        target_path: &str,
        payload: Vec<u8>,
        priority: MessagePriority,
    ) -> SendResult {
        let msg = BufferedMessage::new(target_path, payload).with_priority(priority);
        self.send_buffered(msg)
    }

    fn send_buffered(&mut self, msg: BufferedMessage) -> SendResult {
        // Try to send immediately if we have an active contact
        if let Some(ref active) = self.active_contact {
            if let Some(state) = self.contacts.get(active) {
                if state.status == ContactStatus::Connected {
                    self.stats.messages_sent += 1;
                    return SendResult::Sent(active.clone());
                }
            }
        }

        // Buffer the message
        if self.buffer.len() >= self.settings.buffer_size {
            // Drop oldest low-priority message if buffer is full
            if let Some(pos) = self.buffer.iter().position(|m| m.priority == MessagePriority::Low) {
                self.buffer.remove(pos);
                self.stats.messages_dropped += 1;
            } else {
                self.stats.messages_dropped += 1;
                return SendResult::Dropped;
            }
        }

        self.stats.messages_buffered += 1;
        self.buffer.push_back(msg);
        SendResult::Buffered(self.buffer.len())
    }

    /// Attempt to connect to any available contact point
    pub fn connect(&mut self) -> Option<String> {
        // Find best available contact (prefer connected, then by latency)
        let best = self
            .contacts
            .iter()
            .filter(|(_, s)| s.is_available())
            .min_by(|(_, a), (_, b)| {
                // Prefer connected over disconnected
                let a_connected = a.status == ContactStatus::Connected;
                let b_connected = b.status == ContactStatus::Connected;
                if a_connected != b_connected {
                    return b_connected.cmp(&a_connected);
                }
                // Then prefer lower latency
                let a_lat = a.latency_ms.unwrap_or(f64::MAX);
                let b_lat = b.latency_ms.unwrap_or(f64::MAX);
                a_lat.partial_cmp(&b_lat).unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(k, _)| k.clone());

        if let Some(ref endpoint) = best {
            if self.active_contact.as_ref() != Some(endpoint) {
                self.stats.contact_switches += 1;
            }
            self.active_contact = Some(endpoint.clone());
        }
        best
    }

    /// Record a successful interaction with a contact point
    pub fn record_success(&mut self, endpoint: &str, latency_ms: f64) {
        if let Some(state) = self.contacts.get_mut(endpoint) {
            state.record_success(latency_ms);
        }
    }

    /// Record a failure for a contact point
    pub fn record_failure(&mut self, endpoint: &str) {
        let max_retries = self.settings.max_retries;
        if let Some(state) = self.contacts.get_mut(endpoint) {
            state.record_failure(max_retries);
            self.stats.total_failures += 1;
        }

        // If active contact failed, try to switch
        if self.active_contact.as_deref() == Some(endpoint) {
            self.active_contact = None;
            self.connect();
        }
    }

    /// Add a newly discovered contact point
    pub fn discover_contact(&mut self, contact: ContactPoint) -> bool {
        let key = contact.endpoint();
        if self.contacts.contains_key(&key) {
            return false;
        }
        if self.contacts.len() >= self.settings.max_contacts {
            return false;
        }
        self.discovered_contacts.insert(key.clone());
        self.contacts.insert(key, ContactPointState::new(contact));
        self.stats.discovery_events += 1;
        true
    }

    /// Blacklist a contact point (permanent until reset)
    pub fn blacklist(&mut self, endpoint: &str) {
        if let Some(state) = self.contacts.get_mut(endpoint) {
            state.status = ContactStatus::Blacklisted;
        }
        if self.active_contact.as_deref() == Some(endpoint) {
            self.active_contact = None;
        }
    }

    /// Reset a blacklisted contact point
    pub fn reset_contact(&mut self, endpoint: &str) {
        if let Some(state) = self.contacts.get_mut(endpoint) {
            state.status = ContactStatus::Disconnected;
            state.consecutive_failures = 0;
        }
    }

    /// Drain buffered messages (returns messages to send)
    pub fn drain_buffer(&mut self, max: usize) -> Vec<BufferedMessage> {
        let count = max.min(self.buffer.len());
        self.buffer.drain(..count).collect()
    }

    /// Get current buffer size
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    /// Get active contact endpoint
    pub fn active_contact(&self) -> Option<&str> {
        self.active_contact.as_deref()
    }

    /// Get number of known contacts
    pub fn contact_count(&self) -> usize {
        self.contacts.len()
    }

    /// Get number of available (connectable) contacts
    pub fn available_count(&self) -> usize {
        self.contacts.values().filter(|s| s.is_available()).count()
    }

    /// Get contact status
    pub fn contact_status(&self, endpoint: &str) -> Option<ContactStatus> {
        self.contacts.get(endpoint).map(|s| s.status)
    }

    /// Get all contact endpoints
    pub fn all_endpoints(&self) -> Vec<String> {
        self.contacts.keys().cloned().collect()
    }

    /// Get statistics
    pub fn stats(&self) -> &ClusterClientStats {
        &self.stats
    }

    /// Check if the client has any active connection
    pub fn is_connected(&self) -> bool {
        self.active_contact.as_ref().map_or(false, |ep| {
            self.contacts
                .get(ep)
                .map_or(false, |s| s.status == ContactStatus::Connected)
        })
    }
}

/// Result of a send operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendResult {
    /// Message sent immediately via the given endpoint
    Sent(String),
    /// Message buffered (current buffer size)
    Buffered(usize),
    /// Message dropped (buffer full)
    Dropped,
}

impl fmt::Debug for ClusterClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterClient")
            .field("contacts", &self.contacts.len())
            .field("active", &self.active_contact)
            .field("buffer", &self.buffer.len())
            .finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_contacts() -> Vec<ContactPoint> {
        vec![
            ContactPoint::new("10.0.0.1", 2551),
            ContactPoint::new("10.0.0.2", 2551),
            ContactPoint::new("10.0.0.3", 2551),
        ]
    }

    fn setup_client() -> ClusterClient {
        ClusterClient::with_defaults(test_contacts())
    }

    #[test]
    fn test_client_creation() {
        let client = setup_client();
        assert_eq!(client.contact_count(), 3);
        assert_eq!(client.available_count(), 3);
        assert!(!client.is_connected());
    }

    #[test]
    fn test_contact_point_display() {
        let cp = ContactPoint::new("10.0.0.1", 2551);
        assert_eq!(cp.endpoint(), "tcp://10.0.0.1:2551");
        assert_eq!(cp.to_string(), "tcp://10.0.0.1:2551");
    }

    #[test]
    fn test_contact_protocol() {
        let cp = ContactPoint::new("10.0.0.1", 443).with_protocol(ContactProtocol::Https);
        assert_eq!(cp.endpoint(), "https://10.0.0.1:443");
    }

    #[test]
    fn test_connect_picks_available() {
        let mut client = setup_client();
        let result = client.connect();
        assert!(result.is_some());
        assert!(client.active_contact().is_some());
    }

    #[test]
    fn test_record_success() {
        let mut client = setup_client();
        let ep = client.connect().unwrap();
        client.record_success(&ep, 5.0);
        assert_eq!(client.contact_status(&ep), Some(ContactStatus::Connected));
        assert!(client.is_connected());
    }

    #[test]
    fn test_record_failure_switches_contact() {
        let mut client = setup_client();
        let ep = client.connect().unwrap();
        // Fail max_retries times to trigger failed status
        for _ in 0..3 {
            client.record_failure(&ep);
        }
        assert_eq!(client.contact_status(&ep), Some(ContactStatus::Failed));
        // Active contact should have switched
        assert_ne!(client.active_contact(), Some(ep.as_str()));
    }

    #[test]
    fn test_send_buffered_when_no_connection() {
        let mut client = setup_client();
        let result = client.send("/user/actor1", vec![1, 2, 3]);
        assert!(matches!(result, SendResult::Buffered(_)));
        assert_eq!(client.buffer_len(), 1);
    }

    #[test]
    fn test_send_immediate_when_connected() {
        let mut client = setup_client();
        let ep = client.connect().unwrap();
        client.record_success(&ep, 1.0);
        let result = client.send("/user/actor1", vec![1, 2, 3]);
        assert!(matches!(result, SendResult::Sent(_)));
    }

    #[test]
    fn test_buffer_overflow_drops() {
        let settings = ClusterClientSettings {
            buffer_size: 3,
            ..Default::default()
        };
        let mut client = ClusterClient::new(test_contacts(), settings);
        client.send("/a", vec![1]);
        client.send("/b", vec![2]);
        client.send("/c", vec![3]);
        let result = client.send("/d", vec![4]);
        // Should drop since buffer is full (all normal priority)
        assert_eq!(result, SendResult::Dropped);
    }

    #[test]
    fn test_buffer_overflow_drops_low_priority() {
        let settings = ClusterClientSettings {
            buffer_size: 2,
            ..Default::default()
        };
        let mut client = ClusterClient::new(test_contacts(), settings);
        client.send_with_priority("/a", vec![1], MessagePriority::Low);
        client.send_with_priority("/b", vec![2], MessagePriority::Normal);
        // This should succeed by dropping the low-priority message
        let result = client.send_with_priority("/c", vec![3], MessagePriority::High);
        assert!(matches!(result, SendResult::Buffered(_)));
        assert_eq!(client.buffer_len(), 2);
    }

    #[test]
    fn test_drain_buffer() {
        let mut client = setup_client();
        client.send("/a", vec![1]);
        client.send("/b", vec![2]);
        client.send("/c", vec![3]);
        let drained = client.drain_buffer(2);
        assert_eq!(drained.len(), 2);
        assert_eq!(client.buffer_len(), 1);
    }

    #[test]
    fn test_discover_contact() {
        let mut client = setup_client();
        let new_cp = ContactPoint::new("10.0.0.4", 2551);
        assert!(client.discover_contact(new_cp));
        assert_eq!(client.contact_count(), 4);
    }

    #[test]
    fn test_discover_duplicate_rejected() {
        let mut client = setup_client();
        let dup = ContactPoint::new("10.0.0.1", 2551);
        assert!(!client.discover_contact(dup));
    }

    #[test]
    fn test_discover_max_contacts() {
        let settings = ClusterClientSettings {
            max_contacts: 3,
            ..Default::default()
        };
        let mut client = ClusterClient::new(test_contacts(), settings);
        let new_cp = ContactPoint::new("10.0.0.4", 2551);
        assert!(!client.discover_contact(new_cp));
    }

    #[test]
    fn test_blacklist() {
        let mut client = setup_client();
        let ep = client.connect().unwrap();
        client.blacklist(&ep);
        assert_eq!(client.contact_status(&ep), Some(ContactStatus::Blacklisted));
        assert_ne!(client.active_contact(), Some(ep.as_str()));
        assert_eq!(client.available_count(), 2);
    }

    #[test]
    fn test_reset_contact() {
        let mut client = setup_client();
        let ep = "tcp://10.0.0.1:2551".to_string();
        client.blacklist(&ep);
        client.reset_contact(&ep);
        assert_eq!(client.contact_status(&ep), Some(ContactStatus::Disconnected));
        assert_eq!(client.available_count(), 3);
    }

    #[test]
    fn test_message_priority_ordering() {
        assert!(MessagePriority::Low < MessagePriority::Normal);
        assert!(MessagePriority::Normal < MessagePriority::High);
        assert!(MessagePriority::High < MessagePriority::Critical);
    }

    #[test]
    fn test_buffered_message_age() {
        let msg = BufferedMessage::new("/test", vec![1]);
        std::thread::sleep(Duration::from_millis(10));
        assert!(msg.age() >= Duration::from_millis(10));
    }

    #[test]
    fn test_all_endpoints() {
        let client = setup_client();
        let eps = client.all_endpoints();
        assert_eq!(eps.len(), 3);
    }

    #[test]
    fn test_stats_tracking() {
        let mut client = setup_client();
        client.send("/a", vec![1]);
        client.send("/b", vec![2]);
        assert_eq!(client.stats().messages_buffered, 2);

        let ep = client.connect().unwrap();
        client.record_success(&ep, 1.0);
        client.send("/c", vec![3]);
        assert_eq!(client.stats().messages_sent, 1);
    }

    #[test]
    fn test_contact_switch_stat() {
        let mut client = setup_client();
        client.connect();
        let switches_before = client.stats().contact_switches;
        // Force switch by failing active
        if let Some(ep) = client.active_contact().map(|s| s.to_string()) {
            for _ in 0..3 {
                client.record_failure(&ep);
            }
        }
        assert!(client.stats().contact_switches > switches_before);
    }

    #[test]
    fn test_prefer_lower_latency() {
        let mut client = setup_client();
        let eps: Vec<_> = client.all_endpoints();
        // Record different latencies
        client.record_success(&eps[0], 100.0);
        client.record_success(&eps[1], 10.0);
        client.record_success(&eps[2], 50.0);
        // Reconnect should prefer lower latency
        client.active_contact = None;
        let best = client.connect().unwrap();
        assert_eq!(best, eps[1]); // 10ms latency
    }

    #[test]
    fn test_debug_display() {
        let client = setup_client();
        let debug = format!("{:?}", client);
        assert!(debug.contains("ClusterClient"));
    }

    #[test]
    fn test_settings_default() {
        let settings = ClusterClientSettings::default();
        assert_eq!(settings.max_retries, 3);
        assert_eq!(settings.buffer_size, 1000);
        assert!(settings.auto_discover);
    }

    #[test]
    fn test_send_result_variants() {
        let sent = SendResult::Sent("tcp://10.0.0.1:2551".to_string());
        let buffered = SendResult::Buffered(5);
        let dropped = SendResult::Dropped;
        assert_ne!(sent, buffered);
        assert_ne!(buffered, dropped);
    }
}

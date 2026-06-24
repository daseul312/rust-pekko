//! TestProbe - Message collection and assertion for actor testing
//!
//! TestProbe acts as a test actor that collects messages and provides
//! assertion methods for verifying actor behavior.

use std::collections::VecDeque;
use std::fmt::Debug;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

/// Default timeout for expect operations
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(3);

/// A test probe that collects messages and provides assertion methods
pub struct TestProbe<T: Send + 'static> {
    receiver: mpsc::UnboundedReceiver<T>,
    sender: mpsc::UnboundedSender<T>,
    received: VecDeque<T>,
    default_timeout: Duration,
}

impl<T: Send + Debug + 'static> TestProbe<T> {
    /// Create a new test probe with default timeout
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            receiver: rx,
            sender: tx,
            received: VecDeque::new(),
            default_timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Create a new test probe with custom timeout
    pub fn with_timeout(timeout: Duration) -> Self {
        let mut probe = Self::new();
        probe.default_timeout = timeout;
        probe
    }

    /// Get a sender to send messages to this probe
    pub fn sender(&self) -> mpsc::UnboundedSender<T> {
        self.sender.clone()
    }

    /// Expect a message within the default timeout
    pub async fn expect_message(&mut self) -> T {
        self.expect_message_within(self.default_timeout).await
    }

    /// Expect a message within a specified timeout
    pub async fn expect_message_within(&mut self, duration: Duration) -> T {
        match timeout(duration, self.receiver.recv()).await {
            Ok(Some(msg)) => {
                msg
            }
            Ok(None) => panic!("TestProbe: channel closed while expecting message"),
            Err(_) => panic!(
                "TestProbe: timeout ({:?}) waiting for message",
                duration
            ),
        }
    }

    /// Expect a message matching a predicate
    pub async fn expect_message_matching<F>(&mut self, predicate: F) -> T
    where
        F: Fn(&T) -> bool,
    {
        let msg = self.expect_message().await;
        assert!(
            predicate(&msg),
            "TestProbe: received message {:?} did not match predicate",
            msg
        );
        msg
    }

    /// Expect a specific message (equality check)
    pub async fn expect_msg(&mut self, expected: &T)
    where
        T: PartialEq,
    {
        let msg = self.expect_message().await;
        assert_eq!(
            msg, *expected,
            "TestProbe: expected {:?} but got {:?}",
            expected, msg
        );
    }

    /// Expect no message within a duration
    pub async fn expect_no_message(&mut self, duration: Duration) -> bool {
        match timeout(duration, self.receiver.recv()).await {
            Ok(Some(msg)) => {
                panic!(
                    "TestProbe: expected no message but received {:?}",
                    msg
                );
            }
            Ok(None) => true,  // Channel closed, no message
            Err(_) => true,    // Timeout = no message (expected)
        }
    }

    /// Expect no message within a short default duration (100ms)
    pub async fn expect_no_message_short(&mut self) -> bool {
        self.expect_no_message(Duration::from_millis(100)).await
    }

    /// Receive a message if available (non-blocking)
    pub fn try_receive(&mut self) -> Option<T> {
        self.receiver.try_recv().ok()
    }

    /// Receive all available messages (non-blocking)
    pub fn receive_all(&mut self) -> Vec<T> {
        let mut msgs = Vec::new();
        while let Ok(msg) = self.receiver.try_recv() {
            msgs.push(msg);
        }
        msgs
    }

    /// Receive exactly N messages
    pub async fn receive_n(&mut self, n: usize) -> Vec<T> {
        let mut msgs = Vec::with_capacity(n);
        for _ in 0..n {
            msgs.push(self.expect_message().await);
        }
        msgs
    }

    /// Fish for a message matching a predicate, discarding non-matching ones
    /// Similar to Scala Pekko's `fishForMessage`
    pub async fn fish_for_message<F>(&mut self, max: Duration, predicate: F) -> T
    where
        F: Fn(&T) -> bool,
    {
        let deadline = tokio::time::Instant::now() + max;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                panic!("TestProbe: fish_for_message timed out after {:?}", max);
            }
            match timeout(remaining, self.receiver.recv()).await {
                Ok(Some(msg)) => {
                    if predicate(&msg) {
                        return msg;
                    }
                    // Discard non-matching message
                }
                Ok(None) => panic!("TestProbe: channel closed during fish_for_message"),
                Err(_) => panic!("TestProbe: fish_for_message timed out after {:?}", max),
            }
        }
    }

    /// Fish for a specific number of messages matching a predicate
    pub async fn fish_for_n<F>(&mut self, n: usize, max: Duration, predicate: F) -> Vec<T>
    where
        F: Fn(&T) -> bool,
    {
        let deadline = tokio::time::Instant::now() + max;
        let mut collected = Vec::with_capacity(n);
        while collected.len() < n {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                panic!(
                    "TestProbe: fish_for_n timed out after {:?} (found {}/{})",
                    max,
                    collected.len(),
                    n
                );
            }
            match timeout(remaining, self.receiver.recv()).await {
                Ok(Some(msg)) => {
                    if predicate(&msg) {
                        collected.push(msg);
                    }
                }
                Ok(None) => panic!("TestProbe: channel closed during fish_for_n"),
                Err(_) => panic!(
                    "TestProbe: fish_for_n timed out (found {}/{})",
                    collected.len(),
                    n
                ),
            }
        }
        collected
    }

    /// Receive messages while predicate returns true
    pub async fn receive_while<F>(&mut self, max: Duration, predicate: F) -> Vec<T>
    where
        F: Fn(&T) -> bool,
    {
        let deadline = tokio::time::Instant::now() + max;
        let mut collected = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return collected;
            }
            match timeout(remaining, self.receiver.recv()).await {
                Ok(Some(msg)) => {
                    if predicate(&msg) {
                        collected.push(msg);
                    } else {
                        return collected;
                    }
                }
                Ok(None) | Err(_) => return collected,
            }
        }
    }

    /// Get the number of received messages in history
    pub fn message_count(&self) -> usize {
        self.received.len()
    }

    /// Clear the message history
    pub fn clear(&mut self) {
        self.received.clear();
        // Also drain any pending messages
        while self.receiver.try_recv().is_ok() {}
    }

    /// Get the default timeout
    pub fn timeout(&self) -> Duration {
        self.default_timeout
    }

    /// Set the default timeout
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.default_timeout = timeout;
    }
}

impl<T: Send + Debug + 'static> Default for TestProbe<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a pair of (probe, sender) for convenience
pub fn probe_pair<T: Send + Debug + 'static>() -> (TestProbe<T>, mpsc::UnboundedSender<T>) {
    let probe = TestProbe::new();
    let sender = probe.sender();
    (probe, sender)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_probe_basic_message() {
        let mut probe = TestProbe::<String>::new();
        let sender = probe.sender();

        sender.send("hello".to_string()).unwrap();

        let msg = probe.expect_message().await;
        assert_eq!(msg, "hello");
    }

    #[tokio::test]
    async fn test_probe_multiple_messages() {
        let mut probe = TestProbe::<i32>::new();
        let sender = probe.sender();

        sender.send(1).unwrap();
        sender.send(2).unwrap();
        sender.send(3).unwrap();

        let msgs = probe.receive_n(3).await;
        assert_eq!(msgs, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_probe_expect_no_message() {
        let mut probe = TestProbe::<i32>::new();
        assert!(probe.expect_no_message(Duration::from_millis(50)).await);
    }

    #[tokio::test]
    #[should_panic(expected = "timeout")]
    async fn test_probe_timeout() {
        let mut probe = TestProbe::<i32>::with_timeout(Duration::from_millis(50));
        let _msg = probe.expect_message().await; // Should panic
    }

    #[tokio::test]
    async fn test_probe_matching() {
        let mut probe = TestProbe::<i32>::new();
        let sender = probe.sender();

        sender.send(42).unwrap();

        let msg = probe.expect_message_matching(|x| *x > 40).await;
        assert_eq!(msg, 42);
    }

    #[tokio::test]
    async fn test_probe_expect_msg() {
        let mut probe = TestProbe::<String>::new();
        let sender = probe.sender();

        sender.send("expected".to_string()).unwrap();
        probe.expect_msg(&"expected".to_string()).await;
    }

    #[tokio::test]
    async fn test_probe_try_receive() {
        let mut probe = TestProbe::<i32>::new();
        let sender = probe.sender();

        assert!(probe.try_receive().is_none());

        sender.send(1).unwrap();
        assert_eq!(probe.try_receive(), Some(1));
        assert!(probe.try_receive().is_none());
    }

    #[tokio::test]
    async fn test_probe_receive_all() {
        let mut probe = TestProbe::<i32>::new();
        let sender = probe.sender();

        sender.send(1).unwrap();
        sender.send(2).unwrap();
        sender.send(3).unwrap();

        // Small delay for messages to be buffered
        tokio::time::sleep(Duration::from_millis(10)).await;

        let msgs = probe.receive_all();
        assert_eq!(msgs, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_probe_clear() {
        let mut probe = TestProbe::<i32>::new();
        let sender = probe.sender();

        sender.send(1).unwrap();
        sender.send(2).unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;
        probe.clear();

        assert!(probe.try_receive().is_none());
    }

    #[tokio::test]
    async fn test_probe_pair() {
        let (mut probe, sender) = probe_pair::<String>();

        sender.send("via pair".to_string()).unwrap();
        let msg = probe.expect_message().await;
        assert_eq!(msg, "via pair");
    }

    #[tokio::test]
    async fn test_fish_for_message() {
        let mut probe = TestProbe::<i32>::new();
        let sender = probe.sender();

        sender.send(1).unwrap();
        sender.send(2).unwrap();
        sender.send(42).unwrap();
        sender.send(3).unwrap();

        let msg = probe
            .fish_for_message(Duration::from_secs(1), |x| *x == 42)
            .await;
        assert_eq!(msg, 42);
    }

    #[tokio::test]
    #[should_panic(expected = "fish_for_message timed out")]
    async fn test_fish_for_message_timeout() {
        let mut probe = TestProbe::<i32>::new();
        let sender = probe.sender();

        sender.send(1).unwrap();
        sender.send(2).unwrap();

        // No matching message — should timeout
        let _msg = probe
            .fish_for_message(Duration::from_millis(50), |x| *x == 999)
            .await;
    }

    #[tokio::test]
    async fn test_fish_for_n() {
        let mut probe = TestProbe::<i32>::new();
        let sender = probe.sender();

        for i in 0..10 {
            sender.send(i).unwrap();
        }

        // Fish for 3 even numbers
        let evens = probe
            .fish_for_n(3, Duration::from_secs(1), |x| x % 2 == 0)
            .await;
        assert_eq!(evens, vec![0, 2, 4]);
    }

    #[tokio::test]
    async fn test_receive_while() {
        let mut probe = TestProbe::<i32>::new();
        let sender = probe.sender();

        sender.send(1).unwrap();
        sender.send(2).unwrap();
        sender.send(3).unwrap();
        sender.send(100).unwrap(); // breaks predicate
        sender.send(4).unwrap();

        let msgs = probe
            .receive_while(Duration::from_secs(1), |x| *x < 10)
            .await;
        assert_eq!(msgs, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_receive_while_timeout() {
        let mut probe = TestProbe::<i32>::new();
        // No messages — should return empty on timeout
        let msgs = probe
            .receive_while(Duration::from_millis(50), |_| true)
            .await;
        assert!(msgs.is_empty());
    }
}

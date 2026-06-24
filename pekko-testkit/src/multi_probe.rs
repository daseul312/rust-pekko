//! MultiProbe & FishingOutcome — Advanced probe utilities
//!
//! - **MultiProbe**: Monitor multiple message channels simultaneously
//! - **FishingOutcome**: Classify fish results (Continue, Complete, Fail)
//! - **WatchProbe**: Death watch style probe for lifecycle events
//! - **AutoPilot**: Automatic response generation for probes

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::time::timeout;

// ============= FishingOutcome =============

/// Result of examining a message during fishing operations.
/// Similar to Scala Pekko's FishingOutcome.
#[derive(Debug, Clone, PartialEq)]
pub enum FishingOutcome {
    /// Message matches — keep and continue fishing
    Continue,
    /// Message matches — keep and stop fishing (success)
    Complete,
    /// Message doesn't match — discard and continue fishing
    ContinueAndIgnore,
    /// Fishing failed — abort with error
    Fail(String),
}

/// Fish for messages with fine-grained control over each message.
pub async fn fish_with_outcome<T, F>(
    receiver: &mut mpsc::UnboundedReceiver<T>,
    max_duration: Duration,
    mut handler: F,
) -> Vec<T>
where
    T: Send + Debug + 'static,
    F: FnMut(&T) -> FishingOutcome,
{
    let deadline = tokio::time::Instant::now() + max_duration;
    let mut collected = Vec::new();

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "fish_with_outcome: timed out after {:?}, collected {} messages",
                max_duration,
                collected.len()
            );
        }

        match timeout(remaining, receiver.recv()).await {
            Ok(Some(msg)) => {
                let outcome = handler(&msg);
                match outcome {
                    FishingOutcome::Continue => {
                        collected.push(msg);
                    }
                    FishingOutcome::Complete => {
                        collected.push(msg);
                        return collected;
                    }
                    FishingOutcome::ContinueAndIgnore => {
                        // Discard
                    }
                    FishingOutcome::Fail(reason) => {
                        panic!(
                            "fish_with_outcome: failed at message {:?} — {}",
                            msg, reason
                        );
                    }
                }
            }
            Ok(None) => panic!("fish_with_outcome: channel closed"),
            Err(_) => panic!(
                "fish_with_outcome: timed out after {:?}",
                max_duration
            ),
        }
    }
}

// ============= MultiProbe =============

/// Monitor multiple named channels simultaneously.
/// Useful for testing scenarios where an actor sends to different targets.
pub struct MultiProbe<T: Send + 'static> {
    channels: Arc<Mutex<HashMap<String, Vec<T>>>>,
    senders: HashMap<String, mpsc::UnboundedSender<T>>,
    receivers: Arc<Mutex<HashMap<String, mpsc::UnboundedReceiver<T>>>>,
    notify: Arc<Notify>,
    total_received: Arc<AtomicU64>,
}

impl<T: Send + Debug + Clone + 'static> MultiProbe<T> {
    /// Create a new multi-probe with given channel names.
    pub fn new(names: &[&str]) -> Self {
        let mut senders = HashMap::new();
        let mut receivers = HashMap::new();

        for name in names {
            let (tx, rx) = mpsc::unbounded_channel();
            senders.insert(name.to_string(), tx);
            receivers.insert(name.to_string(), rx);
        }

        Self {
            channels: Arc::new(Mutex::new(HashMap::new())),
            senders,
            receivers: Arc::new(Mutex::new(receivers)),
            notify: Arc::new(Notify::new()),
            total_received: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get a sender for a named channel.
    pub fn sender(&self, name: &str) -> mpsc::UnboundedSender<T> {
        self.senders
            .get(name)
            .unwrap_or_else(|| panic!("MultiProbe: unknown channel '{}'", name))
            .clone()
    }

    /// Expect a message on a specific channel.
    pub async fn expect_on(&self, name: &str, max_wait: Duration) -> T {
        let mut receivers = self.receivers.lock().await;
        let rx = receivers
            .get_mut(name)
            .unwrap_or_else(|| panic!("MultiProbe: unknown channel '{}'", name));

        match timeout(max_wait, rx.recv()).await {
            Ok(Some(msg)) => {
                self.total_received.fetch_add(1, Ordering::Relaxed);
                let mut channels = self.channels.lock().await;
                channels
                    .entry(name.to_string())
                    .or_insert_with(Vec::new)
                    .push(msg.clone());
                self.notify.notify_waiters();
                msg
            }
            Ok(None) => panic!("MultiProbe: channel '{}' closed", name),
            Err(_) => panic!(
                "MultiProbe: timeout ({:?}) on channel '{}'",
                max_wait, name
            ),
        }
    }

    /// Expect no message on a specific channel.
    pub async fn expect_nothing_on(&self, name: &str, duration: Duration) -> bool {
        let mut receivers = self.receivers.lock().await;
        let rx = receivers
            .get_mut(name)
            .unwrap_or_else(|| panic!("MultiProbe: unknown channel '{}'", name));

        match timeout(duration, rx.recv()).await {
            Ok(Some(msg)) => panic!(
                "MultiProbe: expected no message on '{}' but got {:?}",
                name, msg
            ),
            Ok(None) | Err(_) => true,
        }
    }

    /// Get all received messages for a channel.
    pub async fn received_on(&self, name: &str) -> Vec<T> {
        let channels = self.channels.lock().await;
        channels.get(name).cloned().unwrap_or_default()
    }

    /// Get total received count across all channels.
    pub fn total_received(&self) -> u64 {
        self.total_received.load(Ordering::Relaxed)
    }

    /// Expect messages on any channel (returns channel name and message).
    pub async fn expect_on_any(&self, names: &[&str], max_wait: Duration) -> (String, T) {
        let deadline = tokio::time::Instant::now() + max_wait;

        loop {
            // Try each channel
            for name in names {
                let mut receivers = self.receivers.lock().await;
                if let Some(rx) = receivers.get_mut(*name) {
                    if let Ok(msg) = rx.try_recv() {
                        self.total_received.fetch_add(1, Ordering::Relaxed);
                        let mut channels = self.channels.lock().await;
                        channels
                            .entry(name.to_string())
                            .or_insert_with(Vec::new)
                            .push(msg.clone());
                        return (name.to_string(), msg);
                    }
                }
            }

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                panic!(
                    "MultiProbe: timeout ({:?}) waiting on any of {:?}",
                    max_wait, names
                );
            }

            // Brief sleep to avoid busy-waiting
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

// ============= WatchProbe =============

/// Lifecycle event types.
#[derive(Debug, Clone, PartialEq)]
pub enum LifecycleEvent {
    Started(String),
    Stopped(String),
    Restarted(String),
    Failed(String, String), // (name, reason)
}

/// A probe for monitoring actor lifecycle events (death watch).
pub struct WatchProbe {
    events: Arc<Mutex<Vec<LifecycleEvent>>>,
    notify: Arc<Notify>,
    watching: Arc<Mutex<Vec<String>>>,
}

impl WatchProbe {
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            notify: Arc::new(Notify::new()),
            watching: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Start watching an actor by name.
    pub async fn watch(&self, name: impl Into<String>) {
        self.watching.lock().await.push(name.into());
    }

    /// Report a lifecycle event.
    pub async fn report(&self, event: LifecycleEvent) {
        self.events.lock().await.push(event);
        self.notify.notify_waiters();
    }

    /// Expect a specific lifecycle event.
    pub async fn expect_event(&self, expected: &LifecycleEvent, max_wait: Duration) {
        let deadline = tokio::time::Instant::now() + max_wait;
        loop {
            {
                let events = self.events.lock().await;
                if events.contains(expected) {
                    return;
                }
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                let events = self.events.lock().await;
                panic!(
                    "WatchProbe: expected {:?} not found. Events: {:?}",
                    expected, *events
                );
            }
            let _ = timeout(remaining, self.notify.notified()).await;
        }
    }

    /// Expect a termination event for an actor.
    pub async fn expect_terminated(&self, name: &str, max_wait: Duration) {
        self.expect_event(
            &LifecycleEvent::Stopped(name.to_string()),
            max_wait,
        )
        .await;
    }

    /// Expect a restart event for an actor.
    pub async fn expect_restarted(&self, name: &str, max_wait: Duration) {
        self.expect_event(
            &LifecycleEvent::Restarted(name.to_string()),
            max_wait,
        )
        .await;
    }

    /// Get all events.
    pub async fn events(&self) -> Vec<LifecycleEvent> {
        self.events.lock().await.clone()
    }

    /// Get event count.
    pub async fn event_count(&self) -> usize {
        self.events.lock().await.len()
    }

    /// Get list of watched actors.
    pub async fn watching_list(&self) -> Vec<String> {
        self.watching.lock().await.clone()
    }
}

// ============= AutoPilot =============

/// Automatic message handler for probes.
/// Sends automatic responses based on received messages.
pub struct AutoPilot<T: Send + 'static> {
    handler: Arc<Mutex<Box<dyn Fn(&T) -> Option<T> + Send>>>,
    active: Arc<AtomicBool>,
    responses_sent: Arc<AtomicU64>,
}

impl<T: Send + Debug + 'static> AutoPilot<T> {
    /// Create an autopilot with a handler function.
    /// The handler receives a message and optionally returns a response.
    pub fn new<F>(handler: F) -> Self
    where
        F: Fn(&T) -> Option<T> + Send + 'static,
    {
        Self {
            handler: Arc::new(Mutex::new(Box::new(handler))),
            active: Arc::new(AtomicBool::new(true)),
            responses_sent: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Process a message and get a response (if any).
    pub async fn process(&self, message: &T) -> Option<T> {
        if !self.active.load(Ordering::SeqCst) {
            return None;
        }
        let handler = self.handler.lock().await;
        let response = handler(message);
        if response.is_some() {
            self.responses_sent.fetch_add(1, Ordering::Relaxed);
        }
        response
    }

    /// Deactivate the autopilot.
    pub fn deactivate(&self) {
        self.active.store(false, Ordering::SeqCst);
    }

    /// Activate the autopilot.
    pub fn activate(&self) {
        self.active.store(true, Ordering::SeqCst);
    }

    /// Check if active.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    /// Get number of responses sent.
    pub fn responses_sent(&self) -> u64 {
        self.responses_sent.load(Ordering::Relaxed)
    }
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;

    // ---- FishingOutcome Tests ----

    #[tokio::test]
    async fn test_fish_with_outcome_complete() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.send(3).unwrap();

        let result = fish_with_outcome(&mut rx, Duration::from_secs(1), |msg| {
            if *msg == 3 {
                FishingOutcome::Complete
            } else {
                FishingOutcome::Continue
            }
        })
        .await;

        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_fish_with_outcome_ignore() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.send(3).unwrap();
        tx.send(4).unwrap();

        let result = fish_with_outcome(&mut rx, Duration::from_secs(1), |msg| {
            if *msg == 4 {
                FishingOutcome::Complete
            } else if msg % 2 == 0 {
                FishingOutcome::ContinueAndIgnore
            } else {
                FishingOutcome::Continue
            }
        })
        .await;

        // 2 was ignored, 1, 3 collected, 4 completes
        assert_eq!(result, vec![1, 3, 4]);
    }

    #[tokio::test]
    #[should_panic(expected = "failed at message")]
    async fn test_fish_with_outcome_fail() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        tx.send(1).unwrap();
        tx.send(-1).unwrap();

        let _result = fish_with_outcome(&mut rx, Duration::from_secs(1), |msg| {
            if *msg < 0 {
                FishingOutcome::Fail("negative number".to_string())
            } else {
                FishingOutcome::Continue
            }
        })
        .await;
    }

    // ---- MultiProbe Tests ----

    #[tokio::test]
    async fn test_multi_probe_basic() {
        let mp = MultiProbe::<String>::new(&["inbox", "outbox"]);
        let inbox_tx = mp.sender("inbox");
        let outbox_tx = mp.sender("outbox");

        inbox_tx.send("request".to_string()).unwrap();
        outbox_tx.send("response".to_string()).unwrap();

        let msg1 = mp.expect_on("inbox", Duration::from_secs(1)).await;
        assert_eq!(msg1, "request");

        let msg2 = mp.expect_on("outbox", Duration::from_secs(1)).await;
        assert_eq!(msg2, "response");

        assert_eq!(mp.total_received(), 2);
    }

    #[tokio::test]
    async fn test_multi_probe_expect_nothing() {
        let mp = MultiProbe::<i32>::new(&["ch1"]);
        assert!(mp.expect_nothing_on("ch1", Duration::from_millis(50)).await);
    }

    #[tokio::test]
    async fn test_multi_probe_received_on() {
        let mp = MultiProbe::<i32>::new(&["data"]);
        let tx = mp.sender("data");

        tx.send(10).unwrap();
        tx.send(20).unwrap();

        mp.expect_on("data", Duration::from_secs(1)).await;
        mp.expect_on("data", Duration::from_secs(1)).await;

        let received = mp.received_on("data").await;
        assert_eq!(received, vec![10, 20]);
    }

    #[tokio::test]
    async fn test_multi_probe_expect_on_any() {
        let mp = MultiProbe::<String>::new(&["a", "b", "c"]);
        let tx_b = mp.sender("b");

        tx_b.send("hello from b".to_string()).unwrap();

        let (channel, msg) = mp
            .expect_on_any(&["a", "b", "c"], Duration::from_secs(1))
            .await;
        assert_eq!(channel, "b");
        assert_eq!(msg, "hello from b");
    }

    // ---- WatchProbe Tests ----

    #[tokio::test]
    async fn test_watch_probe_lifecycle() {
        let wp = WatchProbe::new();
        wp.watch("actor-1").await;

        wp.report(LifecycleEvent::Started("actor-1".to_string())).await;
        wp.report(LifecycleEvent::Stopped("actor-1".to_string())).await;

        wp.expect_terminated("actor-1", Duration::from_secs(1)).await;
        assert_eq!(wp.event_count().await, 2);
    }

    #[tokio::test]
    async fn test_watch_probe_restart() {
        let wp = WatchProbe::new();
        wp.watch("actor-2").await;

        wp.report(LifecycleEvent::Restarted("actor-2".to_string())).await;

        wp.expect_restarted("actor-2", Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn test_watch_probe_failed() {
        let wp = WatchProbe::new();
        wp.report(LifecycleEvent::Failed(
            "actor-3".to_string(),
            "out of memory".to_string(),
        ))
        .await;

        let events = wp.events().await;
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0],
            LifecycleEvent::Failed("actor-3".to_string(), "out of memory".to_string())
        );
    }

    #[tokio::test]
    async fn test_watch_probe_watching_list() {
        let wp = WatchProbe::new();
        wp.watch("a").await;
        wp.watch("b").await;

        let list = wp.watching_list().await;
        assert_eq!(list, vec!["a", "b"]);
    }

    // ---- AutoPilot Tests ----

    #[tokio::test]
    async fn test_autopilot_echo() {
        let pilot = AutoPilot::new(|msg: &i32| Some(*msg * 2));

        let response = pilot.process(&5).await;
        assert_eq!(response, Some(10));
        assert_eq!(pilot.responses_sent(), 1);
    }

    #[tokio::test]
    async fn test_autopilot_selective() {
        let pilot = AutoPilot::new(|msg: &i32| {
            if *msg > 0 {
                Some(*msg)
            } else {
                None
            }
        });

        assert_eq!(pilot.process(&5).await, Some(5));
        assert_eq!(pilot.process(&-1).await, None);
        assert_eq!(pilot.responses_sent(), 1);
    }

    #[tokio::test]
    async fn test_autopilot_deactivate() {
        let pilot = AutoPilot::new(|msg: &i32| Some(*msg));

        assert!(pilot.is_active());
        assert_eq!(pilot.process(&1).await, Some(1));

        pilot.deactivate();
        assert!(!pilot.is_active());
        assert_eq!(pilot.process(&2).await, None);

        pilot.activate();
        assert_eq!(pilot.process(&3).await, Some(3));
        assert_eq!(pilot.responses_sent(), 2);
    }
}

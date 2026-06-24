// kill_switch.rs - External Stream Cancellation Control
// UniqueKillSwitch (1:1) and SharedKillSwitch (1:N) patterns

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::fmt;

// ============================================================================
// KillSwitch Reason
// ============================================================================

/// Reason why a stream was shut down
#[derive(Debug, Clone, PartialEq)]
pub enum ShutdownReason {
    /// Normal shutdown requested
    Graceful,
    /// Shutdown due to an error
    Failure(String),
    /// Shutdown due to abort (immediate)
    Abort,
    /// Shutdown with a custom reason
    Custom(String),
}

impl fmt::Display for ShutdownReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShutdownReason::Graceful => write!(f, "graceful shutdown"),
            ShutdownReason::Failure(msg) => write!(f, "failure: {}", msg),
            ShutdownReason::Abort => write!(f, "abort"),
            ShutdownReason::Custom(msg) => write!(f, "{}", msg),
        }
    }
}

// ============================================================================
// Kill Switch State
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum KillSwitchState {
    Running,
    ShuttingDown,
    Completed,
}

// ============================================================================
// Shared Kill Switch Inner
// ============================================================================

struct KillSwitchInner {
    is_shutdown: AtomicBool,
    is_completed: AtomicBool,
    reason: Mutex<Option<ShutdownReason>>,
    shutdown_count: AtomicU64,
    listeners: Mutex<Vec<Box<dyn Fn(&ShutdownReason) + Send + Sync>>>,
    name: String,
}

impl KillSwitchInner {
    fn new(name: &str) -> Self {
        Self {
            is_shutdown: AtomicBool::new(false),
            is_completed: AtomicBool::new(false),
            reason: Mutex::new(None),
            shutdown_count: AtomicU64::new(0),
            listeners: Mutex::new(Vec::new()),
            name: name.to_string(),
        }
    }

    fn shutdown(&self, reason: ShutdownReason) -> bool {
        // Only first shutdown succeeds
        if self.is_shutdown.compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
            let mut r = self.reason.lock().unwrap();
            *r = Some(reason.clone());
            self.shutdown_count.fetch_add(1, Ordering::Relaxed);

            // Notify listeners
            let listeners = self.listeners.lock().unwrap();
            for listener in listeners.iter() {
                listener(&reason);
            }
            true
        } else {
            false
        }
    }

    fn is_shutdown(&self) -> bool {
        self.is_shutdown.load(Ordering::SeqCst)
    }

    fn complete(&self) {
        self.is_completed.store(true, Ordering::SeqCst);
    }

    fn state(&self) -> KillSwitchState {
        if self.is_completed.load(Ordering::SeqCst) {
            KillSwitchState::Completed
        } else if self.is_shutdown.load(Ordering::SeqCst) {
            KillSwitchState::ShuttingDown
        } else {
            KillSwitchState::Running
        }
    }

    fn reason(&self) -> Option<ShutdownReason> {
        self.reason.lock().unwrap().clone()
    }
}

// ============================================================================
// UniqueKillSwitch — controls exactly one stream
// ============================================================================

/// A kill switch bound to a single stream materialization.
/// When shutdown is called, the associated stream is cancelled.
#[derive(Clone)]
pub struct UniqueKillSwitch {
    inner: Arc<KillSwitchInner>,
}

impl UniqueKillSwitch {
    pub fn new() -> Self {
        Self::with_name("unique-kill-switch")
    }

    pub fn with_name(name: &str) -> Self {
        Self {
            inner: Arc::new(KillSwitchInner::new(name)),
        }
    }

    /// Request graceful shutdown of the stream
    pub fn shutdown(&self) -> bool {
        self.inner.shutdown(ShutdownReason::Graceful)
    }

    /// Abort the stream immediately with a reason
    pub fn abort(&self, reason: &str) -> bool {
        self.inner.shutdown(ShutdownReason::Abort)
            || self.inner.shutdown(ShutdownReason::Failure(reason.to_string()))
    }

    /// Shutdown with a specific reason
    pub fn shutdown_with_reason(&self, reason: ShutdownReason) -> bool {
        self.inner.shutdown(reason)
    }

    /// Check if shutdown has been requested
    pub fn is_shutdown(&self) -> bool {
        self.inner.is_shutdown()
    }

    /// Mark the stream as completed
    pub fn complete(&self) {
        self.inner.complete();
    }

    /// Get the current state
    pub fn state(&self) -> KillSwitchState {
        self.inner.state()
    }

    /// Get the shutdown reason (if shutdown)
    pub fn reason(&self) -> Option<ShutdownReason> {
        self.inner.reason()
    }

    /// Name of this kill switch
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Register a callback for shutdown events
    pub fn on_shutdown<F>(&self, callback: F)
    where
        F: Fn(&ShutdownReason) + Send + Sync + 'static,
    {
        let mut listeners = self.inner.listeners.lock().unwrap();
        listeners.push(Box::new(callback));
    }
}

impl Default for UniqueKillSwitch {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for UniqueKillSwitch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UniqueKillSwitch")
            .field("name", &self.inner.name)
            .field("state", &self.state())
            .finish()
    }
}

// ============================================================================
// SharedKillSwitch — controls multiple streams
// ============================================================================

/// A kill switch that can be shared across multiple streams.
/// When shutdown is called, ALL associated streams are cancelled.
pub struct SharedKillSwitch {
    inner: Arc<KillSwitchInner>,
    streams: Mutex<Vec<Arc<KillSwitchInner>>>,
    stream_count: AtomicU64,
}

impl SharedKillSwitch {
    pub fn new(name: &str) -> Self {
        Self {
            inner: Arc::new(KillSwitchInner::new(name)),
            streams: Mutex::new(Vec::new()),
            stream_count: AtomicU64::new(0),
        }
    }

    /// Create a flow-through kill switch for a stream.
    /// Returns a UniqueKillSwitch that is controlled by this SharedKillSwitch.
    pub fn flow(&self) -> UniqueKillSwitch {
        let stream_inner = Arc::new(KillSwitchInner::new(&format!(
            "{}-stream-{}",
            self.inner.name,
            self.stream_count.fetch_add(1, Ordering::Relaxed)
        )));

        self.streams.lock().unwrap().push(Arc::clone(&stream_inner));

        UniqueKillSwitch {
            inner: stream_inner,
        }
    }

    /// Shutdown all associated streams
    pub fn shutdown(&self) -> usize {
        let reason = ShutdownReason::Graceful;
        self.inner.shutdown(reason.clone());

        let streams = self.streams.lock().unwrap();
        let mut count = 0;
        for stream in streams.iter() {
            if stream.shutdown(reason.clone()) {
                count += 1;
            }
        }
        count
    }

    /// Abort all streams with a reason
    pub fn abort(&self, reason: &str) -> usize {
        let shutdown_reason = ShutdownReason::Failure(reason.to_string());
        self.inner.shutdown(shutdown_reason.clone());

        let streams = self.streams.lock().unwrap();
        let mut count = 0;
        for stream in streams.iter() {
            if stream.shutdown(shutdown_reason.clone()) {
                count += 1;
            }
        }
        count
    }

    /// Check if shutdown has been requested
    pub fn is_shutdown(&self) -> bool {
        self.inner.is_shutdown()
    }

    /// Get the current state
    pub fn state(&self) -> KillSwitchState {
        self.inner.state()
    }

    /// Number of streams controlled by this switch
    pub fn stream_count(&self) -> usize {
        self.streams.lock().unwrap().len()
    }

    /// Number of streams that are still running
    pub fn active_stream_count(&self) -> usize {
        self.streams.lock().unwrap()
            .iter()
            .filter(|s| !s.is_shutdown())
            .count()
    }

    /// Name of this kill switch
    pub fn name(&self) -> &str {
        &self.inner.name
    }
}

impl fmt::Debug for SharedKillSwitch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedKillSwitch")
            .field("name", &self.inner.name)
            .field("state", &self.state())
            .field("stream_count", &self.stream_count())
            .finish()
    }
}

// ============================================================================
// KillSwitch-aware Stream Processing
// ============================================================================

/// Process elements through a kill switch gate.
/// Returns elements until the kill switch is triggered.
pub fn take_until_shutdown<T>(
    elements: Vec<T>,
    kill_switch: &UniqueKillSwitch,
) -> Vec<T> {
    let mut result = Vec::new();
    for elem in elements {
        if kill_switch.is_shutdown() {
            break;
        }
        result.push(elem);
    }
    result
}

/// Applies a function to elements, respecting the kill switch.
pub fn map_until_shutdown<T, U, F>(
    elements: Vec<T>,
    kill_switch: &UniqueKillSwitch,
    f: F,
) -> Vec<U>
where
    F: Fn(T) -> U,
{
    let mut result = Vec::new();
    for elem in elements {
        if kill_switch.is_shutdown() {
            break;
        }
        result.push(f(elem));
    }
    result
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    // --- ShutdownReason tests ---

    #[test]
    fn test_shutdown_reason_display() {
        assert_eq!(ShutdownReason::Graceful.to_string(), "graceful shutdown");
        assert_eq!(ShutdownReason::Abort.to_string(), "abort");
        assert_eq!(
            ShutdownReason::Failure("oops".into()).to_string(),
            "failure: oops"
        );
        assert_eq!(
            ShutdownReason::Custom("custom reason".into()).to_string(),
            "custom reason"
        );
    }

    // --- UniqueKillSwitch tests ---

    #[test]
    fn test_unique_kill_switch_initial_state() {
        let ks = UniqueKillSwitch::new();
        assert!(!ks.is_shutdown());
        assert_eq!(ks.state(), KillSwitchState::Running);
        assert!(ks.reason().is_none());
    }

    #[test]
    fn test_unique_kill_switch_shutdown() {
        let ks = UniqueKillSwitch::new();
        assert!(ks.shutdown()); // first call succeeds
        assert!(ks.is_shutdown());
        assert_eq!(ks.state(), KillSwitchState::ShuttingDown);
        assert_eq!(ks.reason(), Some(ShutdownReason::Graceful));
    }

    #[test]
    fn test_unique_kill_switch_double_shutdown() {
        let ks = UniqueKillSwitch::new();
        assert!(ks.shutdown());
        assert!(!ks.shutdown()); // second call returns false
    }

    #[test]
    fn test_unique_kill_switch_complete() {
        let ks = UniqueKillSwitch::new();
        ks.shutdown();
        ks.complete();
        assert_eq!(ks.state(), KillSwitchState::Completed);
    }

    #[test]
    fn test_unique_kill_switch_with_name() {
        let ks = UniqueKillSwitch::with_name("my-stream");
        assert_eq!(ks.name(), "my-stream");
    }

    #[test]
    fn test_unique_kill_switch_custom_reason() {
        let ks = UniqueKillSwitch::new();
        ks.shutdown_with_reason(ShutdownReason::Custom("timeout".into()));
        assert_eq!(ks.reason(), Some(ShutdownReason::Custom("timeout".into())));
    }

    #[test]
    fn test_unique_kill_switch_on_shutdown_callback() {
        let ks = UniqueKillSwitch::new();
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = Arc::clone(&called);

        ks.on_shutdown(move |_reason| {
            called_clone.store(true, Ordering::Relaxed);
        });

        assert!(!called.load(Ordering::Relaxed));
        ks.shutdown();
        assert!(called.load(Ordering::Relaxed));
    }

    #[test]
    fn test_unique_kill_switch_clone_shares_state() {
        let ks1 = UniqueKillSwitch::new();
        let ks2 = ks1.clone();

        ks1.shutdown();
        assert!(ks2.is_shutdown());
    }

    #[test]
    fn test_unique_kill_switch_debug() {
        let ks = UniqueKillSwitch::with_name("test");
        let debug = format!("{:?}", ks);
        assert!(debug.contains("UniqueKillSwitch"));
        assert!(debug.contains("test"));
    }

    // --- SharedKillSwitch tests ---

    #[test]
    fn test_shared_kill_switch_creation() {
        let sks = SharedKillSwitch::new("shared-1");
        assert!(!sks.is_shutdown());
        assert_eq!(sks.stream_count(), 0);
        assert_eq!(sks.name(), "shared-1");
    }

    #[test]
    fn test_shared_kill_switch_flow() {
        let sks = SharedKillSwitch::new("shared");
        let ks1 = sks.flow();
        let ks2 = sks.flow();

        assert_eq!(sks.stream_count(), 2);
        assert!(!ks1.is_shutdown());
        assert!(!ks2.is_shutdown());
    }

    #[test]
    fn test_shared_kill_switch_shutdown_all() {
        let sks = SharedKillSwitch::new("shared");
        let ks1 = sks.flow();
        let ks2 = sks.flow();
        let ks3 = sks.flow();

        let count = sks.shutdown();
        assert_eq!(count, 3);
        assert!(ks1.is_shutdown());
        assert!(ks2.is_shutdown());
        assert!(ks3.is_shutdown());
        assert!(sks.is_shutdown());
    }

    #[test]
    fn test_shared_kill_switch_abort() {
        let sks = SharedKillSwitch::new("shared");
        let ks1 = sks.flow();
        let ks2 = sks.flow();

        let count = sks.abort("critical error");
        assert_eq!(count, 2);
        assert!(ks1.is_shutdown());
        assert_eq!(ks1.reason(), Some(ShutdownReason::Failure("critical error".into())));
    }

    #[test]
    fn test_shared_kill_switch_active_count() {
        let sks = SharedKillSwitch::new("shared");
        let ks1 = sks.flow();
        let _ks2 = sks.flow();

        assert_eq!(sks.active_stream_count(), 2);

        ks1.shutdown();
        assert_eq!(sks.active_stream_count(), 1);
    }

    #[test]
    fn test_shared_kill_switch_independent_streams() {
        let sks = SharedKillSwitch::new("shared");
        let ks1 = sks.flow();
        let ks2 = sks.flow();

        // Individual stream shutdown doesn't affect others
        ks1.shutdown();
        assert!(ks1.is_shutdown());
        assert!(!ks2.is_shutdown());
        assert!(!sks.is_shutdown()); // shared switch itself not triggered
    }

    #[test]
    fn test_shared_kill_switch_debug() {
        let sks = SharedKillSwitch::new("debug-test");
        let debug = format!("{:?}", sks);
        assert!(debug.contains("SharedKillSwitch"));
        assert!(debug.contains("debug-test"));
    }

    // --- Processing with KillSwitch ---

    #[test]
    fn test_take_until_shutdown_no_shutdown() {
        let ks = UniqueKillSwitch::new();
        let data = vec![1, 2, 3, 4, 5];
        let result = take_until_shutdown(data, &ks);
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_take_until_shutdown_immediate() {
        let ks = UniqueKillSwitch::new();
        ks.shutdown();
        let data = vec![1, 2, 3, 4, 5];
        let result = take_until_shutdown(data, &ks);
        assert!(result.is_empty());
    }

    #[test]
    fn test_map_until_shutdown() {
        let ks = UniqueKillSwitch::new();
        let data = vec![1, 2, 3];
        let result = map_until_shutdown(data, &ks, |x| x * 10);
        assert_eq!(result, vec![10, 20, 30]);
    }

    #[test]
    fn test_map_until_shutdown_aborted() {
        let ks = UniqueKillSwitch::new();
        ks.shutdown();
        let data = vec![1, 2, 3];
        let result = map_until_shutdown(data, &ks, |x| x * 10);
        assert!(result.is_empty());
    }

    // --- Thread safety tests ---

    #[test]
    fn test_unique_kill_switch_thread_safe() {
        let ks = UniqueKillSwitch::new();
        let ks_clone = ks.clone();

        let handle = std::thread::spawn(move || {
            ks_clone.shutdown();
        });
        handle.join().unwrap();

        assert!(ks.is_shutdown());
    }

    #[test]
    fn test_shared_kill_switch_concurrent_flows() {
        let sks = Arc::new(SharedKillSwitch::new("concurrent"));
        let mut handles = Vec::new();

        for _ in 0..10 {
            let sks_clone = Arc::clone(&sks);
            handles.push(std::thread::spawn(move || {
                sks_clone.flow()
            }));
        }

        let switches: Vec<UniqueKillSwitch> = handles.into_iter()
            .map(|h| h.join().unwrap())
            .collect();

        assert_eq!(sks.stream_count(), 10);

        sks.shutdown();
        for ks in &switches {
            assert!(ks.is_shutdown());
        }
    }

    #[test]
    fn test_on_shutdown_captures_reason() {
        let ks = UniqueKillSwitch::new();
        let captured = Arc::new(Mutex::new(None::<ShutdownReason>));
        let captured_clone = Arc::clone(&captured);

        ks.on_shutdown(move |reason| {
            *captured_clone.lock().unwrap() = Some(reason.clone());
        });

        ks.shutdown_with_reason(ShutdownReason::Failure("test error".into()));
        let captured_reason = captured.lock().unwrap().clone();
        assert_eq!(captured_reason, Some(ShutdownReason::Failure("test error".into())));
    }

    #[test]
    fn test_multiple_listeners() {
        let ks = UniqueKillSwitch::new();
        let count = Arc::new(AtomicUsize::new(0));

        for _ in 0..3 {
            let c = Arc::clone(&count);
            ks.on_shutdown(move |_| {
                c.fetch_add(1, Ordering::Relaxed);
            });
        }

        ks.shutdown();
        assert_eq!(count.load(Ordering::Relaxed), 3);
    }
}

//! TestActorSystem - Convenience test actor system factory
//!
//! Provides a wrapper around ActorSystem with test-friendly defaults
//! and helper methods for spawning test actors.

use pekko_actor::ActorSystem;
use std::sync::Arc;

/// A test-friendly wrapper around ActorSystem
pub struct TestActorSystem {
    system: Arc<ActorSystem>,
    name: String,
}

impl TestActorSystem {
    /// Create a new test actor system with a given name
    pub fn new(name: &str) -> Self {
        let system = ActorSystem::new(name);
        Self {
            system: Arc::new(system),
            name: name.to_string(),
        }
    }

    /// Create with a unique auto-generated name (for parallel tests)
    pub fn create() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        Self::new(&format!("test-system-{}", id))
    }

    /// Get the underlying ActorSystem
    pub fn system(&self) -> &ActorSystem {
        &self.system
    }

    /// Get the Arc<ActorSystem>
    pub fn system_arc(&self) -> Arc<ActorSystem> {
        self.system.clone()
    }

    /// Get the system name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Shutdown the actor system
    pub async fn shutdown(&self) {
        // ActorSystem doesn't have explicit shutdown yet,
        // but we provide the method for forward compatibility
    }
}

impl Default for TestActorSystem {
    fn default() -> Self {
        Self::create()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_system() {
        let system = TestActorSystem::new("my-test");
        assert_eq!(system.name(), "my-test");
    }

    #[tokio::test]
    async fn test_auto_named_system() {
        let s1 = TestActorSystem::create();
        let s2 = TestActorSystem::create();
        assert_ne!(s1.name(), s2.name());
    }

    #[tokio::test]
    async fn test_system_arc() {
        let system = TestActorSystem::new("arc-test");
        let arc = system.system_arc();
        assert!(Arc::strong_count(&arc) >= 1);
    }

    #[tokio::test]
    async fn test_shutdown() {
        let system = TestActorSystem::new("shutdown-test");
        system.shutdown().await;
        // Should not panic
    }
}

//! # Pekko TestKit - Testing utilities for Pekko actors
//!
//! Provides comprehensive testing tools modeled after Scala Pekko TestKit:
//!
//! - **TestProbe<T>** - Collects and asserts on messages
//! - **TestActorSystem** - Convenient test actor system factory
//! - **Message assertions** - Pattern matching and ordering verification
//! - **Timing helpers** - Duration-based assertions (within, eventually)
//! - **LoggingTestKit** - Assert on log events during tests
//!
//! ## Example
//!
//! ```ignore
//! use pekko_testkit::prelude::*;
//!
//! #[tokio::test]
//! async fn test_actor_sends_response() {
//!     let system = TestActorSystem::new("test");
//!     let probe = TestProbe::<String>::new();
//!     let sender = probe.sender();
//!
//!     sender.send("hello".to_string()).await.unwrap();
//!
//!     let msg = probe.expect_message().await;
//!     assert_eq!(msg, "hello");
//! }
//! ```

pub mod probe;
pub mod test_system;
pub mod matchers;
pub mod timing;
pub mod logging;
pub mod scheduler;
pub mod stream_testkit;
pub mod multi_probe;
pub mod behavior_testkit;
pub mod failure_injection;
pub mod test_system_builder;
pub mod test_actor_ref;
pub mod supervision_assertions;
pub mod manual_time;

pub mod prelude {
    pub use crate::probe::{TestProbe, probe_pair};
    pub use crate::test_system::TestActorSystem;
    pub use crate::matchers::{
        expect_in_order, expect_any_order, expect_subsequence,
        expect_contains, expect_not_contains, MessageSequence,
    };
    pub use crate::timing::{Within, EventuallyAssert, sleep_millis, yield_now, measure};
    pub use crate::logging::{LoggingTestKit, LogCollector, LogEvent, LogLevel};
    pub use crate::scheduler::{VirtualClock, TestScheduler, TimedAssertion};
    pub use crate::stream_testkit::{TestSource, TestSink, StreamAssertion, TestPublisher, TestSubscriber};
    pub use crate::multi_probe::{MultiProbe, FishingOutcome, WatchProbe, LifecycleEvent, AutoPilot, fish_with_outcome};
    pub use crate::behavior_testkit::{BehaviorTestKit, Effect, BehaviorResult, TestInbox, EffectCollector, LogLevel as BehaviorLogLevel, RunRecord};
    pub use crate::failure_injection::{FailureInjector, FailureRule, FailureType, MessageOutcome, NetworkPartition, ChaosMonkey, FailureStats, InjectionRecord};
    pub use crate::test_system_builder::{TestActorSystemBuilder, ManagedTestSystem, TestConfig, ConfigValue, TestSupervisionStrategy, TestLogLevel, SystemStats};
    pub use crate::test_actor_ref::{TestActorRef, ActorStateSnapshot, StateValue, TestMessage, LifecycleState, ProcessingRecord, ProcessingResult, TestActorRefStats};
    pub use crate::supervision_assertions::{SupervisionProbe, SupervisionEvent, SupervisionDirective, SupervisionRule, RuleViolation};
    pub use crate::manual_time::{ManualClock, ManualClockStats, FiredTimer};
}

pub use prelude::*;

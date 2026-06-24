//! # Pekko Streams - Reactive Streams for Rust
//!
//! Pekko Streams provides backpressure-aware stream processing with a DSL
//! for building data processing pipelines.
//!
//! ## Core Concepts
//!
//! - **Source**: Produces elements (like an iterator)
//! - **Flow**: Transforms elements (map, filter, etc.)
//! - **Sink**: Consumes elements (collect, foreach, etc.)
//! - **RunnableGraph**: A complete pipeline ready to execute
//!
//! ## Graph DSL
//!
//! - **Broadcast**: Fan-out - sends each element to all outputs
//! - **Merge**: Fan-in - collects elements from all inputs
//! - **Balance**: Round-robin distribution across outputs
//! - **Zip**: Combines elements from two streams into tuples
//! - **Concat**: Sequential concatenation of streams
//!
//! ## Advanced Operators
//!
//! - **flat_map**: Map and flatten
//! - **scan**: Fold with intermediate values
//! - **sliding_window**: Window of last N elements
//! - **throttle/buffer/delay**: Time-based flow control
//! - **retry/recover**: Error recovery
//! - **SubFlow**: Group by key, process sub-streams, merge back
//! - **AsyncBoundary**: Run stages on separate tasks
//! - **Backpressure**: Demand-based flow control
//!
//! ## Example
//!
//! ```ignore
//! use pekko_streams::prelude::*;
//!
//! // Create a simple pipeline
//! let result = Source::from_iter(1..=10)
//!     .via(Flow::map(|x| x * 2))
//!     .via(Flow::filter(|x| x > 10))
//!     .run_with(Sink::collect())
//!     .await?;
//!
//! assert_eq!(result, vec![12, 14, 16, 18, 20]);
//! ```

pub mod actor_interop;
pub mod async_boundary;
pub mod backpressure;
pub mod errors;
pub mod flow;
pub mod graph;
pub mod materializer;
pub mod operators;
pub mod recovery;
pub mod sink;
pub mod source;
pub mod stage;
pub mod subflow;
pub mod timing;
pub mod supervision;
pub mod async_ops;
pub mod buffer_strategy;
pub mod kill_switch;
pub mod stream_ops;
pub mod file_io;
pub mod hub;
pub mod framing;
pub mod lazy_source;
pub mod throttle;
pub mod retry_flow;
pub mod compression;

pub mod pekko_actor;

pub mod prelude {
    pub use crate::source::Source;
    pub use crate::flow::Flow;
    pub use crate::sink::Sink;
    pub use crate::graph::{RunnableGraph, GraphBuilder, Broadcast, Merge, Balance, Zip, Concat};
    pub use crate::materializer::Materializer;
    pub use crate::errors::{StreamError, StreamResult};
    pub use crate::stage::OverflowStrategy;
    pub use crate::recovery::ExponentialBackoff;
    pub use crate::backpressure::{BackpressureConfig, BoundedChannel, DemandSignal, run_with_backpressure};
    pub use crate::async_boundary::{async_source, AsyncBoundaryConfig, parallel_map};
    pub use crate::actor_interop::{actor_source, actor_sink, source_queue, sink_queue, SourceQueue, SinkQueue};
    pub use crate::pekko_actor::{ActorSourceBuilder, ActorSinkBuilder};
    pub use crate::supervision::{SupervisionStrategy, Directive, RestartSettings, RestartSource, SupervisionStats};
    pub use crate::async_ops::{AsyncMapStage, BatchStage, BatchWeightedStage, UnfoldSource, UnfoldAsyncSource, MapAsyncStage};
    pub use crate::buffer_strategy::{DynamicBuffer, PriorityBuffer, ConflateStage, ExtrapolateStage, BufferMetrics};
    pub use crate::kill_switch::{UniqueKillSwitch, SharedKillSwitch, KillSwitchState, ShutdownReason};
    pub use crate::stream_ops::{TakeWhile, DropWhile, WireTap, StreamLog, LogLevel};
    pub use crate::file_io::{FileSource, FileSink, FileWriteMode, FileReadMode};
    pub use crate::hub::{MergeHub, BroadcastHub, MergeStrategy, DropPolicy, HubError, HubStats};
    pub use crate::framing::{
        DelimiterFraming, DelimiterFramingConfig, LengthPrefixFraming, LengthPrefixConfig,
        ByteOrder, FramingError, encode_with_delimiter, encode_with_length_prefix,
    };
    pub use crate::lazy_source::{
        LazySource, QueueOverflowStrategy, QueueOfferResult, QueueStats,
        UnfoldResource, PagedSource, PageResult,
    };

    // Phase 5: Throttle, Retry, Compression (30차)
    pub use crate::throttle::{
        Throttle, ThrottleConfig, ThrottleMode, ThrottleResult, ThrottleError,
        ThrottleStats, ThrottleBuilder, TokenBucket, CostFunction, UnitCost, SizeCost,
        CostThrottle, AdaptiveThrottle,
    };
    pub use crate::retry_flow::{
        RetryFlow, RetryPolicy, RetryState, RetryDecision, RetryError,
        BackoffStrategy, RecoverWith, RecoveryStrategy, RecoverResult,
        RestartFlow, RestartState, RestartFlowStats,
        RetryBudget, RetryFlowStats, RecoverStats, BudgetStats,
    };
    pub use crate::compression::{
        CompressionCodec, CompressionLevel, CompressionConfig, CompressionError,
        CompressionStage, CompressionStats, StageState, CompressionPipeline,
        PipelineStats, CodecRegistry,
    };
}

pub use prelude::*;

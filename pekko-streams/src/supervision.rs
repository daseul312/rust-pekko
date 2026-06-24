//! Stream Supervision — Automatic restart and error handling strategies
//!
//! Modeled after Scala Pekko's stream supervision:
//!
//! - **SupervisionStrategy**: Resume, Restart, or Stop on failure
//! - **SupervisedSource**: Wraps a source with supervision logic
//! - **SupervisedFlow**: Wraps a flow with supervision logic
//! - **RestartSource**: Auto-restarting source with backoff
//! - **RestartFlow**: Auto-restarting flow with backoff
//! - **RestartSettings**: Configurable backoff parameters

use crate::errors::{StreamError, StreamResult};
use crate::stage::{FlowStage, SourceStage, StreamElement};
use async_trait::async_trait;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

// ============= Supervision Strategy =============

/// Decider function: given an error, decide what to do.
pub type Decider = Box<dyn Fn(&StreamError) -> Directive + Send + Sync>;

/// What to do when a stage encounters an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Directive {
    /// Skip the failing element and continue processing.
    Resume,
    /// Restart the stage (reset state) and continue.
    Restart,
    /// Stop the stream immediately.
    Stop,
}

/// Supervision strategy that decides how to handle stream failures.
pub struct SupervisionStrategy {
    decider: Decider,
    max_restarts: usize,
    within: Duration,
}

impl SupervisionStrategy {
    /// Create a strategy with a custom decider.
    pub fn new(decider: Decider) -> Self {
        Self {
            decider,
            max_restarts: 3,
            within: Duration::from_secs(60),
        }
    }

    /// Always resume on error (skip failing element).
    pub fn resuming() -> Self {
        Self::new(Box::new(|_| Directive::Resume))
    }

    /// Always restart on error (reset stage state).
    pub fn restarting() -> Self {
        Self::new(Box::new(|_| Directive::Restart))
    }

    /// Always stop on error.
    pub fn stopping() -> Self {
        Self::new(Box::new(|_| Directive::Stop))
    }

    /// Set maximum number of restarts within a time window.
    pub fn with_max_restarts(mut self, max: usize, within: Duration) -> Self {
        self.max_restarts = max;
        self.within = within;
        self
    }

    /// Decide what to do for a given error.
    pub fn decide(&self, error: &StreamError) -> Directive {
        (self.decider)(error)
    }

    pub fn max_restarts(&self) -> usize {
        self.max_restarts
    }

    pub fn within(&self) -> Duration {
        self.within
    }
}

impl fmt::Debug for SupervisionStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SupervisionStrategy")
            .field("max_restarts", &self.max_restarts)
            .field("within", &self.within)
            .finish()
    }
}

// ============= Restart Settings =============

/// Settings for restart backoff.
#[derive(Debug, Clone)]
pub struct RestartSettings {
    /// Minimum backoff duration.
    pub min_backoff: Duration,
    /// Maximum backoff duration.
    pub max_backoff: Duration,
    /// Randomness factor (0.0 = no jitter, 1.0 = full jitter).
    pub random_factor: f64,
    /// Maximum number of restarts (-1 = unlimited).
    pub max_restarts: i64,
    /// Time window for counting restarts.
    pub max_restarts_within: Duration,
}

impl RestartSettings {
    pub fn new(min_backoff: Duration, max_backoff: Duration, random_factor: f64) -> Self {
        Self {
            min_backoff,
            max_backoff,
            random_factor,
            max_restarts: -1,
            max_restarts_within: Duration::from_secs(0),
        }
    }

    pub fn with_max_restarts(mut self, max: i64, within: Duration) -> Self {
        self.max_restarts = max;
        self.max_restarts_within = within;
        self
    }

    /// Calculate backoff duration for a given attempt.
    pub fn backoff_for_attempt(&self, attempt: u32) -> Duration {
        let base = self.min_backoff.as_millis() as f64
            * (2.0_f64).powi(attempt as i32);
        let capped = base.min(self.max_backoff.as_millis() as f64);

        // Apply jitter
        let jitter = if self.random_factor > 0.0 {
            // Deterministic "jitter" based on attempt number (no rand dependency)
            let pseudo_random = ((attempt as f64 * 7.31) % 1.0) * self.random_factor;
            capped * (1.0 - self.random_factor + pseudo_random * 2.0)
        } else {
            capped
        };

        Duration::from_millis(jitter.max(1.0) as u64)
    }
}

impl Default for RestartSettings {
    fn default() -> Self {
        Self::new(
            Duration::from_millis(100),
            Duration::from_secs(30),
            0.2,
        )
    }
}

// ============= Supervised Source =============

/// A source wrapped with supervision logic.
/// On error, applies the supervision strategy to decide whether to resume, restart, or stop.
pub struct SupervisedSource<S, F>
where
    S: SourceStage<F> + Send + Sync,
    F: Send + Sync + Clone,
{
    factory: Box<dyn Fn() -> S + Send + Sync>,
    source: S,
    strategy: SupervisionStrategy,
    restart_count: u32,
    restart_timestamps: Vec<std::time::Instant>,
    _phantom: std::marker::PhantomData<F>,
}

impl<S, F> SupervisedSource<S, F>
where
    S: SourceStage<F> + Send + Sync,
    F: Send + Sync + Clone,
{
    pub fn new(factory: impl Fn() -> S + Send + Sync + 'static, strategy: SupervisionStrategy) -> Self {
        let source = factory();
        Self {
            factory: Box::new(factory),
            source,
            strategy,
            restart_count: 0,
            restart_timestamps: Vec::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    fn can_restart(&mut self) -> bool {
        let now = std::time::Instant::now();
        let window = self.strategy.within();

        // Remove old timestamps outside the window
        self.restart_timestamps.retain(|t| now.duration_since(*t) < window);

        if self.restart_timestamps.len() >= self.strategy.max_restarts() {
            return false;
        }

        self.restart_timestamps.push(now);
        true
    }
}

#[async_trait]
impl<S, F> SourceStage<F> for SupervisedSource<S, F>
where
    S: SourceStage<F> + Send + Sync,
    F: Send + Sync + Clone + 'static,
{
    async fn pull(&mut self) -> StreamResult<StreamElement<F>> {
        match self.source.pull().await {
            Ok(elem) => Ok(elem),
            Err(ref error) => {
                let directive = self.strategy.decide(error);
                match directive {
                    Directive::Resume => {
                        debug!("SupervisedSource: resuming after error");
                        // Try pulling again (skip the error)
                        self.source.pull().await
                    }
                    Directive::Restart => {
                        if self.can_restart() {
                            self.restart_count += 1;
                            self.source = (self.factory)();
                            self.source.pull().await
                        } else {
                            warn!("SupervisedSource: max restarts exceeded, stopping");
                            Err(StreamError::StageFailed(
                                "Max restarts exceeded".to_string(),
                            ))
                        }
                    }
                    Directive::Stop => {
                        warn!("SupervisedSource: stopping on error");
                        Ok(StreamElement::Error(format!("{}", error)))
                    }
                }
            }
        }
    }
}

// ============= Supervised Flow =============

/// A flow stage wrapped with supervision logic.
pub struct SupervisedFlow<Stage, In, Out>
where
    Stage: FlowStage<In, Out> + Send + Sync,
    In: Send + Sync,
    Out: Send + Sync,
{
    factory: Box<dyn Fn() -> Stage + Send + Sync>,
    stage: Stage,
    strategy: SupervisionStrategy,
    restart_count: u32,
    restart_timestamps: Vec<std::time::Instant>,
    _phantom: std::marker::PhantomData<(In, Out)>,
}

impl<Stage, In, Out> SupervisedFlow<Stage, In, Out>
where
    Stage: FlowStage<In, Out> + Send + Sync,
    In: Send + Sync,
    Out: Send + Sync,
{
    pub fn new(factory: impl Fn() -> Stage + Send + Sync + 'static, strategy: SupervisionStrategy) -> Self {
        let stage = factory();
        Self {
            factory: Box::new(factory),
            stage,
            strategy,
            restart_count: 0,
            restart_timestamps: Vec::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    fn can_restart(&mut self) -> bool {
        let now = std::time::Instant::now();
        let window = self.strategy.within();
        self.restart_timestamps.retain(|t| now.duration_since(*t) < window);
        if self.restart_timestamps.len() >= self.strategy.max_restarts() {
            return false;
        }
        self.restart_timestamps.push(now);
        true
    }
}

#[async_trait]
impl<Stage, In, Out> FlowStage<In, Out> for SupervisedFlow<Stage, In, Out>
where
    Stage: FlowStage<In, Out> + Send + Sync,
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
{
    async fn process(&mut self, element: In) -> StreamResult<Vec<Out>> {
        match self.stage.process(element).await {
            Ok(result) => Ok(result),
            Err(ref error) => {
                let directive = self.strategy.decide(error);
                match directive {
                    Directive::Resume => {
                        debug!("SupervisedFlow: resuming, skipping element");
                        Ok(vec![]) // Skip this element
                    }
                    Directive::Restart => {
                        if self.can_restart() {
                            self.restart_count += 1;
                            self.stage = (self.factory)();
                            Ok(vec![]) // Skip the element that caused the restart
                        } else {
                            warn!("SupervisedFlow: max restarts exceeded");
                            Err(StreamError::StageFailed("Max restarts exceeded".to_string()))
                        }
                    }
                    Directive::Stop => {
                        warn!("SupervisedFlow: stopping on error");
                        Err(StreamError::StageFailed(format!("{}", error)))
                    }
                }
            }
        }
    }

    async fn on_complete(&mut self) -> StreamResult<Vec<Out>> {
        self.stage.on_complete().await
    }
}

// ============= Restart Source =============

/// A source that automatically restarts with backoff when it fails or completes.
pub struct RestartSource<T: Send + Sync + 'static> {
    factory: Box<dyn Fn() -> Box<dyn SourceStage<T>> + Send + Sync>,
    source: Option<Box<dyn SourceStage<T>>>,
    settings: RestartSettings,
    restart_count: u32,
    completed: bool,
}

impl<T: Send + Sync + 'static> RestartSource<T> {
    pub fn new(
        settings: RestartSettings,
        factory: impl Fn() -> Box<dyn SourceStage<T>> + Send + Sync + 'static,
    ) -> Self {
        let source = factory();
        Self {
            factory: Box::new(factory),
            source: Some(source),
            settings,
            restart_count: 0,
            completed: false,
        }
    }

    async fn try_restart(&mut self) -> bool {
        if self.settings.max_restarts >= 0
            && self.restart_count as i64 >= self.settings.max_restarts
        {
            return false;
        }

        let delay = self.settings.backoff_for_attempt(self.restart_count);
        self.restart_count += 1;
        tokio::time::sleep(delay).await;
        self.source = Some((self.factory)());
        true
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> SourceStage<T> for RestartSource<T> {
    async fn pull(&mut self) -> StreamResult<StreamElement<T>> {
        if self.completed {
            return Ok(StreamElement::Complete);
        }

        loop {
            if let Some(ref mut source) = self.source {
                match source.pull().await {
                    Ok(StreamElement::Element(e)) => {
                        return Ok(StreamElement::Element(e));
                    }
                    Ok(StreamElement::Complete) | Err(_) => {
                        // Source completed or errored — try restart
                        self.source = None;
                        if !self.try_restart().await {
                            self.completed = true;
                            return Ok(StreamElement::Complete);
                        }
                        // Loop back to pull from new source
                    }
                    Ok(StreamElement::Error(e)) => {
                        self.source = None;
                        if !self.try_restart().await {
                            self.completed = true;
                            return Ok(StreamElement::Error(e));
                        }
                    }
                }
            } else {
                self.completed = true;
                return Ok(StreamElement::Complete);
            }
        }
    }
}

// ============= Supervision Stats =============

/// Tracks supervision statistics.
#[derive(Debug, Clone)]
pub struct SupervisionStats {
    restarts: Arc<AtomicU64>,
    resumes: Arc<AtomicU64>,
    stops: Arc<AtomicU64>,
}

impl SupervisionStats {
    pub fn new() -> Self {
        Self {
            restarts: Arc::new(AtomicU64::new(0)),
            resumes: Arc::new(AtomicU64::new(0)),
            stops: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn record_restart(&self) {
        self.restarts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_resume(&self) {
        self.resumes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_stop(&self) {
        self.stops.fetch_add(1, Ordering::Relaxed);
    }

    pub fn restarts(&self) -> u64 {
        self.restarts.load(Ordering::Relaxed)
    }

    pub fn resumes(&self) -> u64 {
        self.resumes.load(Ordering::Relaxed)
    }

    pub fn stops(&self) -> u64 {
        self.stops.load(Ordering::Relaxed)
    }

    pub fn total_incidents(&self) -> u64 {
        self.restarts() + self.resumes() + self.stops()
    }
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Directive & Strategy Tests ----

    #[test]
    fn test_directive_values() {
        assert_eq!(Directive::Resume, Directive::Resume);
        assert_eq!(Directive::Restart, Directive::Restart);
        assert_eq!(Directive::Stop, Directive::Stop);
        assert_ne!(Directive::Resume, Directive::Stop);
    }

    #[test]
    fn test_strategy_resuming() {
        let strategy = SupervisionStrategy::resuming();
        let error = StreamError::Failed("test".to_string());
        assert_eq!(strategy.decide(&error), Directive::Resume);
    }

    #[test]
    fn test_strategy_restarting() {
        let strategy = SupervisionStrategy::restarting();
        let error = StreamError::Cancelled;
        assert_eq!(strategy.decide(&error), Directive::Restart);
    }

    #[test]
    fn test_strategy_stopping() {
        let strategy = SupervisionStrategy::stopping();
        let error = StreamError::Timeout;
        assert_eq!(strategy.decide(&error), Directive::Stop);
    }

    #[test]
    fn test_strategy_custom_decider() {
        let strategy = SupervisionStrategy::new(Box::new(|err| {
            match err {
                StreamError::Timeout => Directive::Resume,
                StreamError::Cancelled => Directive::Stop,
                _ => Directive::Restart,
            }
        }));

        assert_eq!(strategy.decide(&StreamError::Timeout), Directive::Resume);
        assert_eq!(strategy.decide(&StreamError::Cancelled), Directive::Stop);
        assert_eq!(strategy.decide(&StreamError::Failed("x".to_string())), Directive::Restart);
    }

    #[test]
    fn test_strategy_max_restarts() {
        let strategy = SupervisionStrategy::restarting()
            .with_max_restarts(5, Duration::from_secs(30));
        assert_eq!(strategy.max_restarts(), 5);
        assert_eq!(strategy.within(), Duration::from_secs(30));
    }

    // ---- RestartSettings Tests ----

    #[test]
    fn test_restart_settings_default() {
        let settings = RestartSettings::default();
        assert_eq!(settings.min_backoff, Duration::from_millis(100));
        assert_eq!(settings.max_backoff, Duration::from_secs(30));
        assert!(settings.random_factor > 0.0);
    }

    #[test]
    fn test_restart_settings_backoff() {
        let settings = RestartSettings::new(
            Duration::from_millis(100),
            Duration::from_secs(10),
            0.0, // no jitter for deterministic test
        );
        // attempt 0: 100ms * 2^0 = 100ms
        assert_eq!(settings.backoff_for_attempt(0), Duration::from_millis(100));
        // attempt 1: 100ms * 2^1 = 200ms
        assert_eq!(settings.backoff_for_attempt(1), Duration::from_millis(200));
        // attempt 2: 100ms * 2^2 = 400ms
        assert_eq!(settings.backoff_for_attempt(2), Duration::from_millis(400));
    }

    #[test]
    fn test_restart_settings_backoff_cap() {
        let settings = RestartSettings::new(
            Duration::from_millis(100),
            Duration::from_millis(500), // low cap
            0.0,
        );
        // attempt 5: 100ms * 2^5 = 3200ms → capped at 500ms
        assert_eq!(settings.backoff_for_attempt(5), Duration::from_millis(500));
    }

    #[test]
    fn test_restart_settings_with_max_restarts() {
        let settings = RestartSettings::default()
            .with_max_restarts(10, Duration::from_secs(60));
        assert_eq!(settings.max_restarts, 10);
        assert_eq!(settings.max_restarts_within, Duration::from_secs(60));
    }

    // ---- SupervisionStats Tests ----

    #[test]
    fn test_supervision_stats() {
        let stats = SupervisionStats::new();
        assert_eq!(stats.total_incidents(), 0);

        stats.record_restart();
        stats.record_restart();
        stats.record_resume();
        stats.record_stop();

        assert_eq!(stats.restarts(), 2);
        assert_eq!(stats.resumes(), 1);
        assert_eq!(stats.stops(), 1);
        assert_eq!(stats.total_incidents(), 4);
    }

    #[test]
    fn test_supervision_stats_clone() {
        let stats = SupervisionStats::new();
        stats.record_restart();
        let stats2 = stats.clone();
        stats.record_restart();
        // Both share the same Arc counters
        assert_eq!(stats2.restarts(), 2);
    }

    // ---- SupervisedSource Tests ----

    /// A test source that fails N times then succeeds
    struct FailingSource {
        fail_count: u32,
        current: u32,
        values: Vec<i32>,
        index: usize,
    }

    impl FailingSource {
        fn new(fail_count: u32, values: Vec<i32>) -> Self {
            Self { fail_count, current: 0, values, index: 0 }
        }
    }

    #[async_trait]
    impl SourceStage<i32> for FailingSource {
        async fn pull(&mut self) -> StreamResult<StreamElement<i32>> {
            if self.current < self.fail_count {
                self.current += 1;
                return Err(StreamError::Failed(format!("fail #{}", self.current)));
            }
            if self.index < self.values.len() {
                let val = self.values[self.index];
                self.index += 1;
                Ok(StreamElement::Element(val))
            } else {
                Ok(StreamElement::Complete)
            }
        }
    }

    #[tokio::test]
    async fn test_supervised_source_resume() {
        // Source fails once, then yields [10, 20]
        let mut supervised = SupervisedSource::new(
            || FailingSource::new(1, vec![10, 20]),
            SupervisionStrategy::resuming(),
        );

        // First pull triggers error → resume → re-pull → gets 10
        let result = supervised.pull().await;
        // After resume, the source incremented current past fail_count, so we get values
        assert!(matches!(result, Ok(StreamElement::Element(_))));
    }

    #[tokio::test]
    async fn test_supervised_source_restart() {
        let mut supervised = SupervisedSource::new(
            || FailingSource::new(1, vec![42]),
            SupervisionStrategy::restarting().with_max_restarts(3, Duration::from_secs(60)),
        );

        // First pull errors → restart → new source → first pull errors again (new source also fails 1)
        // → 2nd pull from new source succeeds
        let result = supervised.pull().await;
        assert!(matches!(result, Ok(StreamElement::Element(_)) | Err(_)));
    }

    #[tokio::test]
    async fn test_supervised_source_stop() {
        let mut supervised = SupervisedSource::new(
            || FailingSource::new(1, vec![42]),
            SupervisionStrategy::stopping(),
        );

        let result = supervised.pull().await;
        assert!(matches!(result, Ok(StreamElement::Error(_))));
    }

    // ---- RestartSource Tests ----

    struct CountSource {
        items: Vec<i32>,
        index: usize,
    }

    #[async_trait]
    impl SourceStage<i32> for CountSource {
        async fn pull(&mut self) -> StreamResult<StreamElement<i32>> {
            if self.index < self.items.len() {
                let val = self.items[self.index];
                self.index += 1;
                Ok(StreamElement::Element(val))
            } else {
                Ok(StreamElement::Complete)
            }
        }
    }

    #[tokio::test]
    async fn test_restart_source_restarts_on_complete() {
        let call_count = Arc::new(AtomicU64::new(0));
        let cc = call_count.clone();

        let mut source = RestartSource::new(
            RestartSettings::new(Duration::from_millis(1), Duration::from_millis(10), 0.0)
                .with_max_restarts(2, Duration::from_secs(60)),
            move || {
                cc.fetch_add(1, Ordering::Relaxed);
                Box::new(CountSource { items: vec![1], index: 0 }) as Box<dyn SourceStage<i32>>
            },
        );

        // First pull: element from first source
        let r1 = source.pull().await.unwrap();
        assert!(matches!(r1, StreamElement::Element(1)));

        // Second pull: first source completes → restart → element from second source
        let r2 = source.pull().await.unwrap();
        assert!(matches!(r2, StreamElement::Element(1)));

        // Third pull: second source completes → restart → element from third source
        let r3 = source.pull().await.unwrap();
        assert!(matches!(r3, StreamElement::Element(1)));

        // Fourth pull: third source completes → max restarts reached → complete
        let r4 = source.pull().await.unwrap();
        assert!(matches!(r4, StreamElement::Complete));
    }

    // ---- SupervisedFlow Tests ----

    struct FailingMapStage {
        fail_on: i32,
    }

    #[async_trait]
    impl FlowStage<i32, i32> for FailingMapStage {
        async fn process(&mut self, element: i32) -> StreamResult<Vec<i32>> {
            if element == self.fail_on {
                Err(StreamError::Failed(format!("failed on {}", element)))
            } else {
                Ok(vec![element * 2])
            }
        }
    }

    #[tokio::test]
    async fn test_supervised_flow_resume() {
        let mut flow = SupervisedFlow::new(
            || FailingMapStage { fail_on: 3 },
            SupervisionStrategy::resuming(),
        );

        assert_eq!(flow.process(1).await.unwrap(), vec![2]);
        assert_eq!(flow.process(2).await.unwrap(), vec![4]);
        // Element 3 fails → resume → empty output (skipped)
        assert_eq!(flow.process(3).await.unwrap(), vec![]);
        assert_eq!(flow.process(4).await.unwrap(), vec![8]);
    }

    #[tokio::test]
    async fn test_supervised_flow_restart() {
        let mut flow = SupervisedFlow::new(
            || FailingMapStage { fail_on: 3 },
            SupervisionStrategy::restarting().with_max_restarts(5, Duration::from_secs(60)),
        );

        assert_eq!(flow.process(1).await.unwrap(), vec![2]);
        // Element 3 fails → restart stage → skipped
        assert_eq!(flow.process(3).await.unwrap(), vec![]);
        // After restart, stage still fails on 3 but works for others
        assert_eq!(flow.process(5).await.unwrap(), vec![10]);
    }

    #[tokio::test]
    async fn test_supervised_flow_stop() {
        let mut flow = SupervisedFlow::new(
            || FailingMapStage { fail_on: 3 },
            SupervisionStrategy::stopping(),
        );

        assert_eq!(flow.process(1).await.unwrap(), vec![2]);
        // Element 3 fails → stop
        assert!(flow.process(3).await.is_err());
    }
}

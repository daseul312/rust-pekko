//! Materializer - Executes stream graphs
//!
//! The materializer is responsible for allocating resources and running
//! the stream graph. It manages the execution context and handles
//! lifecycle events.

use crate::errors::StreamResult;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Materializer settings
#[derive(Debug, Clone)]
pub struct MaterializerSettings {
    /// Maximum number of concurrent streams
    pub max_concurrent_streams: usize,
    /// Default buffer size for async boundaries
    pub buffer_size: usize,
    /// Enable debug logging
    pub debug: bool,
}

impl Default for MaterializerSettings {
    fn default() -> Self {
        Self {
            max_concurrent_streams: 100,
            buffer_size: 16,
            debug: false,
        }
    }
}

impl MaterializerSettings {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_concurrent_streams(mut self, n: usize) -> Self {
        self.max_concurrent_streams = n;
        self
    }

    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    pub fn with_debug(mut self, debug: bool) -> Self {
        self.debug = debug;
        self
    }
}

/// The materializer that executes stream graphs
pub struct Materializer {
    settings: MaterializerSettings,
    semaphore: Arc<Semaphore>,
}

impl Materializer {
    /// Create a new materializer with default settings
    pub fn new() -> Self {
        Self::with_settings(MaterializerSettings::default())
    }

    /// Create a materializer with custom settings
    pub fn with_settings(settings: MaterializerSettings) -> Self {
        let semaphore = Arc::new(Semaphore::new(settings.max_concurrent_streams));
        Self { settings, semaphore }
    }

    /// Get the settings
    pub fn settings(&self) -> &MaterializerSettings {
        &self.settings
    }

    /// Run a graph with this materializer
    pub async fn run<Mat: Send + 'static>(
        &self,
        graph: crate::graph::RunnableGraph<Mat>,
    ) -> StreamResult<Mat> {
        // Acquire a permit to limit concurrency
        let _permit = self.semaphore.acquire().await
            .map_err(|_| crate::errors::StreamError::MaterializationFailed(
                "Semaphore closed".to_string()
            ))?;

        graph.run().await
    }

    /// Get the number of available permits
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }
}

impl Default for Materializer {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for Materializer {
    fn clone(&self) -> Self {
        Self {
            settings: self.settings.clone(),
            semaphore: self.semaphore.clone(),
        }
    }
}

/// Execution context for stream stages
pub struct ExecutionContext {
    /// Materializer reference
    materializer: Arc<Materializer>,
}

impl ExecutionContext {
    /// Create a new execution context
    pub fn new(materializer: Arc<Materializer>) -> Self {
        Self { materializer }
    }

    /// Get the materializer
    pub fn materializer(&self) -> &Materializer {
        &self.materializer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Source, Sink, graph::RunnableGraph};

    #[test]
    fn test_materializer_settings() {
        let settings = MaterializerSettings::new()
            .with_max_concurrent_streams(50)
            .with_buffer_size(32)
            .with_debug(true);

        assert_eq!(settings.max_concurrent_streams, 50);
        assert_eq!(settings.buffer_size, 32);
        assert!(settings.debug);
    }

    #[tokio::test]
    async fn test_materializer_run() {
        let materializer = Materializer::new();
        
        let source = Source::from_iter(1..=5);
        let sink = Sink::collect();
        let graph = RunnableGraph::from_source_sink(source, sink);
        
        let result = materializer.run(graph).await.unwrap();
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_materializer_permits() {
        let settings = MaterializerSettings::new()
            .with_max_concurrent_streams(10);
        let materializer = Materializer::with_settings(settings);
        
        assert_eq!(materializer.available_permits(), 10);
    }

    #[test]
    fn test_execution_context() {
        let materializer = Arc::new(Materializer::new());
        let ctx = ExecutionContext::new(materializer.clone());
        
        assert_eq!(
            ctx.materializer().settings().max_concurrent_streams,
            100
        );
    }
}

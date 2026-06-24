//! Failure detection mechanisms for distributed systems

use crate::{
    config::{FailureDetectorConfig, FailureDetectorType},
    RemoteAddress,
};
use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Failure detector trait
pub trait FailureDetector: Send + Sync {
    /// Check if a node is considered failed
    fn is_available(&self, address: &RemoteAddress) -> bool;
    
    /// Record a heartbeat from a node
    fn record_heartbeat(&mut self, address: &RemoteAddress);
    
    /// Get the failure detector configuration
    fn config(&self) -> &FailureDetectorConfig;
}

/// Phi accrual failure detector
#[derive(Debug)]
pub struct PhiAccrualFailureDetector {
    config: FailureDetectorConfig,
    heartbeat_history: std::collections::HashMap<RemoteAddress, Vec<Instant>>,
}

impl PhiAccrualFailureDetector {
    pub fn new(config: FailureDetectorConfig) -> Self {
        Self {
            config,
            heartbeat_history: std::collections::HashMap::new(),
        }
    }
}

impl FailureDetector for PhiAccrualFailureDetector {
    fn is_available(&self, address: &RemoteAddress) -> bool {
        // Simplified implementation
        if let Some(heartbeats) = self.heartbeat_history.get(address) {
            if let Some(last_heartbeat) = heartbeats.last() {
                let elapsed = last_heartbeat.elapsed();
                elapsed < Duration::from_millis(self.config.threshold as u64)
            } else {
                false
            }
        } else {
            false
        }
    }

    fn record_heartbeat(&mut self, address: &RemoteAddress) {
        let now = Instant::now();
        let heartbeats = self.heartbeat_history.entry(address.clone()).or_insert_with(Vec::new);
        heartbeats.push(now);
        
        // Keep only recent heartbeats
        let cutoff = now - Duration::from_millis(self.config.max_sample_size as u64);
        heartbeats.retain(|&time| time > cutoff);
    }

    fn config(&self) -> &FailureDetectorConfig {
        &self.config
    }
}

/// Deadline failure detector
#[derive(Debug)]
pub struct DeadlineFailureDetector {
    config: FailureDetectorConfig,
    last_heartbeat: std::collections::HashMap<RemoteAddress, Instant>,
}

impl DeadlineFailureDetector {
    pub fn new(config: FailureDetectorConfig) -> Self {
        Self {
            config,
            last_heartbeat: std::collections::HashMap::new(),
        }
    }
}

impl FailureDetector for DeadlineFailureDetector {
    fn is_available(&self, address: &RemoteAddress) -> bool {
        if let Some(last) = self.last_heartbeat.get(address) {
            last.elapsed() < Duration::from_millis(self.config.threshold as u64)
        } else {
            false
        }
    }

    fn record_heartbeat(&mut self, address: &RemoteAddress) {
        self.last_heartbeat.insert(address.clone(), Instant::now());
    }

    fn config(&self) -> &FailureDetectorConfig {
        &self.config
    }
}

/// No-op failure detector (always considers nodes available)
#[derive(Debug)]
pub struct NoOpFailureDetector {
    config: FailureDetectorConfig,
}

impl NoOpFailureDetector {
    pub fn new(config: FailureDetectorConfig) -> Self {
        Self { config }
    }
}

impl FailureDetector for NoOpFailureDetector {
    fn is_available(&self, _address: &RemoteAddress) -> bool {
        true
    }

    fn record_heartbeat(&mut self, _address: &RemoteAddress) {
        // No-op
    }

    fn config(&self) -> &FailureDetectorConfig {
        &self.config
    }
}

/// Create a failure detector based on configuration
pub fn create_failure_detector(config: FailureDetectorConfig) -> Box<dyn FailureDetector> {
    match config.implementation {
        FailureDetectorType::PhiAccrualFailureDetector => {
            Box::new(PhiAccrualFailureDetector::new(config))
        }
        FailureDetectorType::DeadlineFailureDetector => {
            Box::new(DeadlineFailureDetector::new(config))
        }
        FailureDetectorType::NoOpFailureDetector => {
            Box::new(NoOpFailureDetector::new(config))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FailureDetectorConfig, FailureDetectorType};

    fn create_phi_accrual_config() -> FailureDetectorConfig {
        FailureDetectorConfig {
            implementation: FailureDetectorType::PhiAccrualFailureDetector,
            threshold: 1000.0,
            min_std_deviation_ms: 100,
            acceptable_heartbeat_pause_ms: 3000,
            heartbeat_interval_ms: 1000,
            max_sample_size: 1000,
        }
    }

    fn create_deadline_config() -> FailureDetectorConfig {
        FailureDetectorConfig {
            implementation: FailureDetectorType::DeadlineFailureDetector,
            threshold: 1000.0,
            min_std_deviation_ms: 100,
            acceptable_heartbeat_pause_ms: 3000,
            heartbeat_interval_ms: 1000,
            max_sample_size: 1000,
        }
    }

    fn create_noop_config() -> FailureDetectorConfig {
        FailureDetectorConfig {
            implementation: FailureDetectorType::NoOpFailureDetector,
            threshold: 1000.0,
            min_std_deviation_ms: 100,
            acceptable_heartbeat_pause_ms: 3000,
            heartbeat_interval_ms: 1000,
            max_sample_size: 1000,
        }
    }

    fn create_test_config() -> FailureDetectorConfig {
        FailureDetectorConfig {
            implementation: FailureDetectorType::PhiAccrualFailureDetector,
            threshold: 1000.0,
            min_std_deviation_ms: 100,
            acceptable_heartbeat_pause_ms: 3000,
            heartbeat_interval_ms: 1000,
            max_sample_size: 1000,
        }
    }

    #[test]
    fn test_phi_accrual_failure_detector() {
        let config = create_phi_accrual_config();
        
        let mut detector = PhiAccrualFailureDetector::new(config);
        let address = RemoteAddress::tcp("test", "127.0.0.1", 2552);
        
        // Initially not available
        assert!(!detector.is_available(&address));
        
        // Record heartbeat
        detector.record_heartbeat(&address);
        assert!(detector.is_available(&address));
    }

    #[test]
    fn test_deadline_failure_detector() {
        let config = create_deadline_config();
        
        let mut detector = DeadlineFailureDetector::new(config);
        let address = RemoteAddress::tcp("test", "127.0.0.1", 2552);
        
        // Initially not available
        assert!(!detector.is_available(&address));
        
        // Record heartbeat
        detector.record_heartbeat(&address);
        assert!(detector.is_available(&address));
    }

    #[test]
    fn test_no_op_failure_detector() {
        let config = create_noop_config();
        
        let detector = NoOpFailureDetector::new(config);
        let address = RemoteAddress::tcp("test", "127.0.0.1", 2552);
        
        // Always available
        assert!(detector.is_available(&address));
    }

    #[test]
    fn test_create_failure_detector() {
        let config = create_test_config();
        
        let detector = create_failure_detector(config);
        let address = RemoteAddress::tcp("test", "127.0.0.1", 2552);
        
        // Test that we can create the detector
        assert!(!detector.is_available(&address));
    }
}
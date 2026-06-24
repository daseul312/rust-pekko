//! Configuration for remoting and clustering

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Main remoting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemotingConfig {
    pub transport: TransportConfig,
    pub cluster: ClusterConfig,
    pub security: SecurityConfig,
    pub serialization: SerializationConfig,
}

impl Default for RemotingConfig {
    fn default() -> Self {
        Self {
            transport: TransportConfig::default(),
            cluster: ClusterConfig::default(),
            security: SecurityConfig::default(),
            serialization: SerializationConfig::default(),
        }
    }
}

/// Transport layer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransportConfig {
    pub bind_address: String,
    pub bind_port: u16,
    pub external_hostname: Option<String>,
    pub external_port: Option<u16>,
    pub maximum_frame_size: usize,
    pub connection_timeout_ms: u64,
    pub send_buffer_size: usize,
    pub receive_buffer_size: usize,
    pub tcp_nodelay: bool,
    pub tcp_keepalive: bool,
    pub backlog: u32,
    pub enable_compression: bool,
    pub compression_threshold: usize,
    pub compression_algorithm: CompressionAlgorithm,
    pub enable_tls: bool,
    pub tls_config: Option<TlsConfig>,
    pub heartbeat_interval_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub connection_pool_size: usize,
    pub max_connections_per_node: usize,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1".to_string(),
            bind_port: 2552,
            external_hostname: None,
            external_port: None,
            maximum_frame_size: 128 * 1024, // 128 KB
            connection_timeout_ms: 15000,   // 15 seconds
            send_buffer_size: 256 * 1024,   // 256 KB
            receive_buffer_size: 256 * 1024, // 256 KB
            tcp_nodelay: true,
            tcp_keepalive: true,
            backlog: 128,
            enable_compression: false,
            compression_threshold: 1024,    // 1 KB
            compression_algorithm: CompressionAlgorithm::Lz4,
            enable_tls: false,
            tls_config: None,
            heartbeat_interval_ms: 30000,   // 30 seconds
            heartbeat_timeout_ms: 90000,    // 90 seconds
            connection_pool_size: 10,
            max_connections_per_node: 5,
        }
    }
}

/// Compression algorithms
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "lz4")]
    Lz4,
    #[serde(rename = "gzip")]
    Gzip,
    #[serde(rename = "snappy")]
    Snappy,
}

/// TLS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub certificate_path: String,
    pub private_key_path: String,
    pub ca_certificate_path: Option<String>,
    pub verify_peer: bool,
    pub cipher_suites: Vec<String>,
    pub protocols: Vec<String>,
}

/// Cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub seed_nodes: Vec<String>,
    pub node_roles: Vec<String>,
    pub min_members: u32,
    pub auto_join: bool,
    pub gossip_interval_ms: u64,
    pub gossip_time_to_live_ms: u64,
    pub failure_detector: FailureDetectorConfig,
    pub split_brain_resolver: SplitBrainResolverConfig,
    pub join_timeout_ms: u64,
    pub shutdown_timeout_ms: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            seed_nodes: vec![],
            node_roles: vec!["worker".to_string()],
            min_members: 1,
            auto_join: true,
            gossip_interval_ms: 1000,       // 1 second
            gossip_time_to_live_ms: 120000, // 2 minutes
            failure_detector: FailureDetectorConfig::default(),
            split_brain_resolver: SplitBrainResolverConfig::default(),
            join_timeout_ms: 60000,         // 1 minute
            shutdown_timeout_ms: 30000,     // 30 seconds
        }
    }
}

/// Failure detector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureDetectorConfig {
    pub implementation: FailureDetectorType,
    pub threshold: f64,
    pub min_std_deviation_ms: u64,
    pub acceptable_heartbeat_pause_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub max_sample_size: usize,
}

impl Default for FailureDetectorConfig {
    fn default() -> Self {
        Self {
            implementation: FailureDetectorType::PhiAccrualFailureDetector,
            threshold: 10.0,
            min_std_deviation_ms: 100,
            acceptable_heartbeat_pause_ms: 3000,
            heartbeat_interval_ms: 1000,
            max_sample_size: 200,
        }
    }
}

/// Failure detector implementations
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum FailureDetectorType {
    #[serde(rename = "phi-accrual")]
    PhiAccrualFailureDetector,
    #[serde(rename = "deadline")]
    DeadlineFailureDetector,
    #[serde(rename = "no-op")]
    NoOpFailureDetector,
}

/// Split brain resolver configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitBrainResolverConfig {
    pub strategy: SplitBrainStrategy,
    pub stable_after_ms: u64,
    pub down_all_when_unstable: bool,
}

impl Default for SplitBrainResolverConfig {
    fn default() -> Self {
        Self {
            strategy: SplitBrainStrategy::KeepMajority,
            stable_after_ms: 20000, // 20 seconds
            down_all_when_unstable: true,
        }
    }
}

/// Split brain resolution strategies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SplitBrainStrategy {
    #[serde(rename = "keep-majority")]
    KeepMajority,
    #[serde(rename = "keep-oldest")]
    KeepOldest,
    #[serde(rename = "keep-referee")]
    KeepReferee { referee_address: String },
    #[serde(rename = "static-quorum")]
    StaticQuorum { quorum_size: u32 },
    #[serde(rename = "lease-majority")]
    LeaseMajority { lease_settings: LeaseSettings },
}

/// Lease settings for split brain resolution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseSettings {
    pub lease_name: String,
    pub owner_name: String,
    pub time_to_live_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub lease_operation_timeout_ms: u64,
}

/// Security configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub enabled: bool,
    pub cookie: Option<String>,
    pub require_mutual_authentication: bool,
    pub trusted_selection_paths: Vec<String>,
    pub untrusted_mode: bool,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cookie: None,
            require_mutual_authentication: false,
            trusted_selection_paths: vec![],
            untrusted_mode: false,
        }
    }
}

/// Serialization configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializationConfig {
    pub default_serializer: String,
    pub serializers: HashMap<String, SerializerConfig>,
    pub serialization_bindings: HashMap<String, String>,
    pub allow_java_serialization: bool,
}

impl Default for SerializationConfig {
    fn default() -> Self {
        let mut serializers = HashMap::new();
        serializers.insert("json".to_string(), SerializerConfig {
            class: "JsonSerializer".to_string(),
            compress: false,
        });
        serializers.insert("protobuf".to_string(), SerializerConfig {
            class: "ProtobufSerializer".to_string(),
            compress: true,
        });

        Self {
            default_serializer: "json".to_string(),
            serializers,
            serialization_bindings: HashMap::new(),
            allow_java_serialization: false,
        }
    }
}

/// Individual serializer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializerConfig {
    pub class: String,
    pub compress: bool,
}

/// Configuration builder for fluent API
pub struct RemotingConfigBuilder {
    config: RemotingConfig,
}

impl RemotingConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: RemotingConfig::default(),
        }
    }

    pub fn with_bind_address(mut self, address: &str, port: u16) -> Self {
        self.config.transport.bind_address = address.to_string();
        self.config.transport.bind_port = port;
        self
    }

    pub fn with_external_address(mut self, hostname: &str, port: u16) -> Self {
        self.config.transport.external_hostname = Some(hostname.to_string());
        self.config.transport.external_port = Some(port);
        self
    }

    pub fn with_tls(mut self, cert_path: &str, key_path: &str) -> Self {
        self.config.transport.enable_tls = true;
        self.config.transport.tls_config = Some(TlsConfig {
            certificate_path: cert_path.to_string(),
            private_key_path: key_path.to_string(),
            ca_certificate_path: None,
            verify_peer: true,
            cipher_suites: vec![],
            protocols: vec!["TLSv1.3".to_string(), "TLSv1.2".to_string()],
        });
        self
    }

    pub fn with_compression(mut self, algorithm: CompressionAlgorithm, threshold: usize) -> Self {
        self.config.transport.enable_compression = true;
        self.config.transport.compression_algorithm = algorithm;
        self.config.transport.compression_threshold = threshold;
        self
    }

    pub fn with_cluster(mut self, seed_nodes: Vec<String>) -> Self {
        self.config.cluster.enabled = true;
        self.config.cluster.seed_nodes = seed_nodes;
        self
    }

    pub fn with_cluster_roles(mut self, roles: Vec<String>) -> Self {
        self.config.cluster.node_roles = roles;
        self
    }

    pub fn with_security_cookie(mut self, cookie: &str) -> Self {
        self.config.security.enabled = true;
        self.config.security.cookie = Some(cookie.to_string());
        self
    }

    pub fn with_split_brain_resolver(mut self, strategy: SplitBrainStrategy) -> Self {
        self.config.cluster.split_brain_resolver.strategy = strategy;
        self
    }

    pub fn with_failure_detector_threshold(mut self, threshold: f64) -> Self {
        self.config.cluster.failure_detector.threshold = threshold;
        self
    }

    pub fn build(self) -> RemotingConfig {
        self.config
    }
}

impl Default for RemotingConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Load configuration from file
pub fn load_config_from_file(path: &str) -> Result<RemotingConfig, ConfigError> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| ConfigError::FileRead(path.to_string(), e.to_string()))?;

    if path.ends_with(".toml") {
        toml::from_str(&content).map_err(|e| ConfigError::Parse(e.to_string()))
    } else if path.ends_with(".json") {
        serde_json::from_str(&content).map_err(|e| ConfigError::Parse(e.to_string()))
    } else {
        Err(ConfigError::UnsupportedFormat(path.to_string()))
    }
}

/// Load configuration from environment variables
pub fn load_config_from_env() -> Result<RemotingConfig, ConfigError> {
    let mut config = RemotingConfig::default();

    // Transport configuration
    if let Ok(bind_addr) = std::env::var("PEKKO_REMOTE_BIND_ADDRESS") {
        config.transport.bind_address = bind_addr;
    }

    if let Ok(bind_port) = std::env::var("PEKKO_REMOTE_BIND_PORT") {
        config.transport.bind_port = bind_port
            .parse()
            .map_err(|e| ConfigError::Parse(format!("Invalid bind port: {}", e)))?;
    }

    if let Ok(external_hostname) = std::env::var("PEKKO_REMOTE_EXTERNAL_HOSTNAME") {
        config.transport.external_hostname = Some(external_hostname);
    }

    if let Ok(external_port) = std::env::var("PEKKO_REMOTE_EXTERNAL_PORT") {
        config.transport.external_port = Some(
            external_port
                .parse()
                .map_err(|e| ConfigError::Parse(format!("Invalid external port: {}", e)))?
        );
    }

    // Cluster configuration
    if let Ok(seed_nodes) = std::env::var("PEKKO_CLUSTER_SEED_NODES") {
        config.cluster.seed_nodes = seed_nodes
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        config.cluster.enabled = true;
    }

    if let Ok(roles) = std::env::var("PEKKO_CLUSTER_ROLES") {
        config.cluster.node_roles = roles
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
    }

    // Security configuration
    if let Ok(cookie) = std::env::var("PEKKO_REMOTE_SECURITY_COOKIE") {
        config.security.enabled = true;
        config.security.cookie = Some(cookie);
    }

    Ok(config)
}

/// Configuration validation
pub fn validate_config(config: &RemotingConfig) -> Result<(), ConfigError> {
    // Validate transport settings
    if config.transport.bind_port == 0 {
        return Err(ConfigError::Validation("Bind port cannot be 0".to_string()));
    }

    if config.transport.maximum_frame_size == 0 {
        return Err(ConfigError::Validation("Maximum frame size cannot be 0".to_string()));
    }

    // Validate cluster settings
    if config.cluster.enabled && config.cluster.seed_nodes.is_empty() {
        return Err(ConfigError::Validation(
            "Cluster enabled but no seed nodes configured".to_string()
        ));
    }

    // Validate TLS settings
    if config.transport.enable_tls {
        if let Some(tls_config) = &config.transport.tls_config {
            if tls_config.certificate_path.is_empty() || tls_config.private_key_path.is_empty() {
                return Err(ConfigError::Validation(
                    "TLS enabled but certificate or private key path is empty".to_string()
                ));
            }
        } else {
            return Err(ConfigError::Validation(
                "TLS enabled but no TLS configuration provided".to_string()
            ));
        }
    }

    Ok(())
}

/// Configuration errors
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Failed to read config file {0}: {1}")]
    FileRead(String, String),

    #[error("Failed to parse config: {0}")]
    Parse(String),

    #[error("Unsupported config format: {0}")]
    UnsupportedFormat(String),

    #[error("Configuration validation failed: {0}")]
    Validation(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RemotingConfig::default();
        
        assert_eq!(config.transport.bind_address, "127.0.0.1");
        assert_eq!(config.transport.bind_port, 2552);
        assert!(!config.cluster.enabled);
        assert!(!config.security.enabled);
    }

    #[test]
    fn test_config_builder() {
        let config = RemotingConfigBuilder::new()
            .with_bind_address("0.0.0.0", 2553)
            .with_external_address("example.com", 2553)
            .with_cluster(vec!["seed1:2552".to_string(), "seed2:2552".to_string()])
            .with_cluster_roles(vec!["frontend".to_string(), "backend".to_string()])
            .with_security_cookie("my-secret-cookie")
            .build();

        assert_eq!(config.transport.bind_address, "0.0.0.0");
        assert_eq!(config.transport.bind_port, 2553);
        assert_eq!(config.transport.external_hostname, Some("example.com".to_string()));
        assert_eq!(config.transport.external_port, Some(2553));
        assert!(config.cluster.enabled);
        assert_eq!(config.cluster.seed_nodes.len(), 2);
        assert_eq!(config.cluster.node_roles, vec!["frontend", "backend"]);
        assert!(config.security.enabled);
        assert_eq!(config.security.cookie, Some("my-secret-cookie".to_string()));
    }

    #[test]
    fn test_config_validation() {
        let mut config = RemotingConfig::default();
        
        // Valid config should pass
        assert!(validate_config(&config).is_ok());
        
        // Invalid port should fail
        config.transport.bind_port = 0;
        assert!(validate_config(&config).is_err());
        
        // Reset port
        config.transport.bind_port = 2552;
        
        // Cluster enabled without seed nodes should fail
        config.cluster.enabled = true;
        config.cluster.seed_nodes.clear();
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_serialization() {
        let config = RemotingConfig::default();
        
        // Test TOML serialization
        let toml_str = toml::to_string(&config).unwrap();
        let deserialized: RemotingConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(config.transport.bind_port, deserialized.transport.bind_port);
        
        // Test JSON serialization  
        let json_str = serde_json::to_string(&config).unwrap();
        let deserialized: RemotingConfig = serde_json::from_str(&json_str).unwrap();
        assert_eq!(config.transport.bind_port, deserialized.transport.bind_port);
    }
}
//! # Compression — Stream Compression Codecs
//!
//! Provides compression and decompression stages for stream processing.
//!
//! ## Features
//!
//! - Multiple codec support: Gzip, Deflate, Snappy, LZ4, Zstd, Identity (passthrough)
//! - Chunked compression for streaming data
//! - Compression level configuration
//! - Compression statistics and metrics
//! - Auto-detection of compressed data format

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// CompressionCodec
// ---------------------------------------------------------------------------

/// Supported compression codecs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressionCodec {
    /// No compression (passthrough)
    Identity,
    /// Gzip (RFC 1952)
    Gzip,
    /// Deflate (RFC 1951)
    Deflate,
    /// Snappy (fast compression)
    Snappy,
    /// LZ4 (very fast compression)
    Lz4,
    /// Zstandard (high ratio compression)
    Zstd,
}

impl CompressionCodec {
    /// Get the typical file extension for this codec.
    pub fn extension(&self) -> &'static str {
        match self {
            CompressionCodec::Identity => "",
            CompressionCodec::Gzip => ".gz",
            CompressionCodec::Deflate => ".zz",
            CompressionCodec::Snappy => ".snappy",
            CompressionCodec::Lz4 => ".lz4",
            CompressionCodec::Zstd => ".zst",
        }
    }

    /// Get the MIME content-encoding value.
    pub fn content_encoding(&self) -> &'static str {
        match self {
            CompressionCodec::Identity => "identity",
            CompressionCodec::Gzip => "gzip",
            CompressionCodec::Deflate => "deflate",
            CompressionCodec::Snappy => "snappy",
            CompressionCodec::Lz4 => "lz4",
            CompressionCodec::Zstd => "zstd",
        }
    }

    /// Whether this codec actually compresses data.
    pub fn is_compressed(&self) -> bool {
        !matches!(self, CompressionCodec::Identity)
    }

    /// Try to detect codec from magic bytes.
    pub fn detect(data: &[u8]) -> Option<CompressionCodec> {
        if data.len() < 2 {
            return None;
        }
        match (data[0], data[1]) {
            (0x1f, 0x8b) => Some(CompressionCodec::Gzip),
            (0x78, 0x01) | (0x78, 0x5e) | (0x78, 0x9c) | (0x78, 0xda) => {
                Some(CompressionCodec::Deflate)
            }
            (0x28, 0xb5) if data.len() >= 4 && data[2] == 0x2f && data[3] == 0xfd => {
                Some(CompressionCodec::Zstd)
            }
            (0x04, 0x22) if data.len() >= 4 && data[2] == 0x4d && data[3] == 0x18 => {
                Some(CompressionCodec::Lz4)
            }
            _ => None,
        }
    }

    /// Parse from string.
    pub fn from_str(s: &str) -> Option<CompressionCodec> {
        match s.to_lowercase().as_str() {
            "identity" | "none" | "" => Some(CompressionCodec::Identity),
            "gzip" | "gz" => Some(CompressionCodec::Gzip),
            "deflate" | "zlib" => Some(CompressionCodec::Deflate),
            "snappy" => Some(CompressionCodec::Snappy),
            "lz4" => Some(CompressionCodec::Lz4),
            "zstd" | "zstandard" => Some(CompressionCodec::Zstd),
            _ => None,
        }
    }
}

impl fmt::Display for CompressionCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.content_encoding())
    }
}

// ---------------------------------------------------------------------------
// CompressionLevel
// ---------------------------------------------------------------------------

/// Compression level preset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionLevel {
    /// Fastest compression (least ratio)
    Fastest,
    /// Fast compression
    Fast,
    /// Default/balanced compression
    Default,
    /// Good compression ratio
    Good,
    /// Best compression ratio (slowest)
    Best,
    /// Custom level (codec-specific, typically 1-22)
    Custom(u32),
}

impl CompressionLevel {
    /// Convert to a numeric level for the given codec.
    pub fn to_numeric(&self, codec: CompressionCodec) -> u32 {
        match self {
            CompressionLevel::Fastest => match codec {
                CompressionCodec::Gzip | CompressionCodec::Deflate => 1,
                CompressionCodec::Zstd => 1,
                CompressionCodec::Lz4 => 1,
                _ => 1,
            },
            CompressionLevel::Fast => match codec {
                CompressionCodec::Gzip | CompressionCodec::Deflate => 3,
                CompressionCodec::Zstd => 3,
                _ => 2,
            },
            CompressionLevel::Default => match codec {
                CompressionCodec::Gzip | CompressionCodec::Deflate => 6,
                CompressionCodec::Zstd => 3,
                _ => 3,
            },
            CompressionLevel::Good => match codec {
                CompressionCodec::Gzip | CompressionCodec::Deflate => 7,
                CompressionCodec::Zstd => 9,
                _ => 5,
            },
            CompressionLevel::Best => match codec {
                CompressionCodec::Gzip | CompressionCodec::Deflate => 9,
                CompressionCodec::Zstd => 22,
                _ => 9,
            },
            CompressionLevel::Custom(n) => *n,
        }
    }
}

impl Default for CompressionLevel {
    fn default() -> Self {
        CompressionLevel::Default
    }
}

// ---------------------------------------------------------------------------
// CompressionConfig
// ---------------------------------------------------------------------------

/// Configuration for a compression/decompression stage.
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Codec to use
    pub codec: CompressionCodec,
    /// Compression level
    pub level: CompressionLevel,
    /// Chunk size for streaming compression
    pub chunk_size: usize,
    /// Whether to flush after each chunk
    pub flush_per_chunk: bool,
    /// Buffer size for internal operations
    pub buffer_size: usize,
}

impl CompressionConfig {
    pub fn new(codec: CompressionCodec) -> Self {
        Self {
            codec,
            level: CompressionLevel::Default,
            chunk_size: 8192,
            flush_per_chunk: false,
            buffer_size: 32768,
        }
    }

    pub fn with_level(mut self, level: CompressionLevel) -> Self {
        self.level = level;
        self
    }

    pub fn with_chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = size.max(64);
        self
    }

    pub fn with_flush(mut self, flush: bool) -> Self {
        self.flush_per_chunk = flush;
        self
    }

    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size.max(256);
        self
    }

    pub fn gzip() -> Self {
        Self::new(CompressionCodec::Gzip)
    }

    pub fn deflate() -> Self {
        Self::new(CompressionCodec::Deflate)
    }

    pub fn snappy() -> Self {
        Self::new(CompressionCodec::Snappy)
    }

    pub fn lz4() -> Self {
        Self::new(CompressionCodec::Lz4)
    }

    pub fn zstd() -> Self {
        Self::new(CompressionCodec::Zstd)
    }

    pub fn identity() -> Self {
        Self::new(CompressionCodec::Identity)
    }
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self::new(CompressionCodec::Gzip)
    }
}

// ---------------------------------------------------------------------------
// CompressionError
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum CompressionError {
    /// Invalid input data
    InvalidData(String),
    /// Codec not supported
    UnsupportedCodec(String),
    /// Buffer too small
    BufferTooSmall { needed: usize, available: usize },
    /// Decompression produced more data than expected
    DecompressionOverflow { limit: usize },
    /// Checksum mismatch
    ChecksumMismatch { expected: u32, actual: u32 },
    /// Compression stage error
    StageError(String),
}

impl fmt::Display for CompressionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionError::InvalidData(msg) => write!(f, "Invalid data: {msg}"),
            CompressionError::UnsupportedCodec(c) => write!(f, "Unsupported codec: {c}"),
            CompressionError::BufferTooSmall { needed, available } => {
                write!(f, "Buffer too small: need {needed}, have {available}")
            }
            CompressionError::DecompressionOverflow { limit } => {
                write!(f, "Decompression overflow (limit: {limit})")
            }
            CompressionError::ChecksumMismatch { expected, actual } => {
                write!(f, "Checksum mismatch: expected {expected:#x}, got {actual:#x}")
            }
            CompressionError::StageError(msg) => write!(f, "Stage error: {msg}"),
        }
    }
}

// ---------------------------------------------------------------------------
// CompressionStage — simulated compression/decompression
// ---------------------------------------------------------------------------

/// A simulated compression stage that tracks metrics.
///
/// In a real implementation, this would wrap actual codec implementations.
/// Here we simulate compression ratios for testing and API correctness.
#[derive(Debug)]
pub struct CompressionStage {
    config: CompressionConfig,
    stats: CompressionStats,
    state: StageState,
    /// Simulated output buffer
    output_buffer: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageState {
    Ready,
    Processing,
    Flushing,
    Finished,
    Error,
}

impl CompressionStage {
    pub fn new(config: CompressionConfig) -> Self {
        Self {
            config,
            stats: CompressionStats::default(),
            state: StageState::Ready,
            output_buffer: Vec::new(),
        }
    }

    pub fn compress(config: CompressionConfig) -> Self {
        Self::new(config)
    }

    pub fn decompress(config: CompressionConfig) -> Self {
        Self::new(config)
    }

    /// Compress a chunk of data.
    pub fn compress_chunk(&mut self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        if self.state == StageState::Error || self.state == StageState::Finished {
            return Err(CompressionError::StageError("Stage not active".into()));
        }

        self.state = StageState::Processing;
        let start = Instant::now();

        let input_size = data.len();
        self.stats.bytes_in += input_size as u64;
        self.stats.chunks_processed += 1;

        // Simulate compression based on codec and level
        let compressed = self.simulate_compress(data);
        let output_size = compressed.len();
        self.stats.bytes_out += output_size as u64;
        self.stats.processing_time += start.elapsed();

        self.state = StageState::Ready;
        Ok(compressed)
    }

    /// Decompress a chunk of data.
    pub fn decompress_chunk(&mut self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        if self.state == StageState::Error || self.state == StageState::Finished {
            return Err(CompressionError::StageError("Stage not active".into()));
        }

        self.state = StageState::Processing;
        let start = Instant::now();

        let input_size = data.len();
        self.stats.bytes_in += input_size as u64;
        self.stats.chunks_processed += 1;

        // Simulate decompression (inverse of compression)
        let decompressed = self.simulate_decompress(data);
        let output_size = decompressed.len();
        self.stats.bytes_out += output_size as u64;
        self.stats.processing_time += start.elapsed();

        self.state = StageState::Ready;
        Ok(decompressed)
    }

    /// Flush any buffered data.
    pub fn flush(&mut self) -> Result<Vec<u8>, CompressionError> {
        self.state = StageState::Flushing;
        let buffered = std::mem::take(&mut self.output_buffer);
        self.state = StageState::Ready;
        Ok(buffered)
    }

    /// Finish the stage, flushing remaining data.
    pub fn finish(&mut self) -> Result<Vec<u8>, CompressionError> {
        let flushed = self.flush()?;
        self.state = StageState::Finished;
        Ok(flushed)
    }

    pub fn state(&self) -> &StageState {
        &self.state
    }

    pub fn config(&self) -> &CompressionConfig {
        &self.config
    }

    pub fn stats(&self) -> &CompressionStats {
        &self.stats
    }

    /// Reset the stage for reuse.
    pub fn reset(&mut self) {
        self.stats = CompressionStats::default();
        self.state = StageState::Ready;
        self.output_buffer.clear();
    }

    // Simulate compression by applying a ratio based on codec
    fn simulate_compress(&self, data: &[u8]) -> Vec<u8> {
        if matches!(self.config.codec, CompressionCodec::Identity) {
            return data.to_vec();
        }

        let ratio = self.simulated_ratio();
        let target_size = ((data.len() as f64) * ratio) as usize;
        let target_size = target_size.max(1);

        // Create "compressed" data by taking every Nth byte + header
        let mut result = Vec::with_capacity(target_size + 4);
        // Add magic bytes for codec detection
        match self.config.codec {
            CompressionCodec::Gzip => {
                result.push(0x1f);
                result.push(0x8b);
            }
            CompressionCodec::Deflate => {
                result.push(0x78);
                result.push(0x9c);
            }
            CompressionCodec::Zstd => {
                result.push(0x28);
                result.push(0xb5);
                result.push(0x2f);
                result.push(0xfd);
            }
            CompressionCodec::Lz4 => {
                result.push(0x04);
                result.push(0x22);
                result.push(0x4d);
                result.push(0x18);
            }
            _ => {}
        }

        // Fill to target size
        let remaining = if target_size > result.len() {
            target_size - result.len()
        } else {
            0
        };
        let step = if remaining > 0 && data.len() > remaining {
            data.len() / remaining
        } else {
            1
        };

        let mut i = 0;
        while result.len() < target_size && i < data.len() {
            result.push(data[i]);
            i += step;
        }

        result
    }

    fn simulate_decompress(&self, data: &[u8]) -> Vec<u8> {
        if matches!(self.config.codec, CompressionCodec::Identity) {
            return data.to_vec();
        }

        let ratio = self.simulated_ratio();
        if ratio <= 0.0 {
            return data.to_vec();
        }
        let target_size = ((data.len() as f64) / ratio) as usize;
        let target_size = target_size.max(data.len());

        // Simulate decompression by expanding data
        let mut result = Vec::with_capacity(target_size);
        for i in 0..target_size {
            let skip_header = match self.config.codec {
                CompressionCodec::Gzip | CompressionCodec::Deflate => 2,
                CompressionCodec::Zstd | CompressionCodec::Lz4 => 4,
                _ => 0,
            };
            let data_start = skip_header.min(data.len());
            let data_slice = &data[data_start..];
            if data_slice.is_empty() {
                result.push(0);
            } else {
                result.push(data_slice[i % data_slice.len()]);
            }
        }
        result
    }

    fn simulated_ratio(&self) -> f64 {
        let base = match self.config.codec {
            CompressionCodec::Identity => 1.0,
            CompressionCodec::Snappy => 0.6,
            CompressionCodec::Lz4 => 0.55,
            CompressionCodec::Gzip => 0.4,
            CompressionCodec::Deflate => 0.42,
            CompressionCodec::Zstd => 0.35,
        };

        // Adjust based on level
        let level_factor = match self.config.level {
            CompressionLevel::Fastest => 1.2,
            CompressionLevel::Fast => 1.1,
            CompressionLevel::Default => 1.0,
            CompressionLevel::Good => 0.9,
            CompressionLevel::Best => 0.8,
            CompressionLevel::Custom(n) => 1.0 - (n as f64 * 0.01).min(0.3),
        };

        (base * level_factor).clamp(0.1, 1.0)
    }
}

// ---------------------------------------------------------------------------
// CompressionStats
// ---------------------------------------------------------------------------

/// Statistics for compression/decompression operations.
#[derive(Debug, Clone, Default)]
pub struct CompressionStats {
    /// Total bytes read (input)
    pub bytes_in: u64,
    /// Total bytes written (output)
    pub bytes_out: u64,
    /// Number of chunks processed
    pub chunks_processed: u64,
    /// Total processing time
    pub processing_time: Duration,
}

impl CompressionStats {
    /// Compression ratio (output / input).
    pub fn ratio(&self) -> f64 {
        if self.bytes_in == 0 {
            return 1.0;
        }
        self.bytes_out as f64 / self.bytes_in as f64
    }

    /// Space savings percentage.
    pub fn savings_percent(&self) -> f64 {
        (1.0 - self.ratio()) * 100.0
    }

    /// Average throughput in bytes per second.
    pub fn throughput_bytes_per_sec(&self) -> f64 {
        let secs = self.processing_time.as_secs_f64();
        if secs <= 0.0 {
            return 0.0;
        }
        self.bytes_in as f64 / secs
    }

    /// Average chunk size (input).
    pub fn avg_chunk_size(&self) -> f64 {
        if self.chunks_processed == 0 {
            return 0.0;
        }
        self.bytes_in as f64 / self.chunks_processed as f64
    }
}

// ---------------------------------------------------------------------------
// CompressionPipeline
// ---------------------------------------------------------------------------

/// Pipeline that chains multiple compression/decompression stages.
#[derive(Debug)]
pub struct CompressionPipeline {
    stages: Vec<PipelineStage>,
    stats: PipelineStats,
}

#[derive(Debug)]
struct PipelineStage {
    name: String,
    stage: CompressionStage,
}

impl CompressionPipeline {
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
            stats: PipelineStats::default(),
        }
    }

    /// Add a compression stage.
    pub fn add_compress(mut self, name: &str, config: CompressionConfig) -> Self {
        self.stages.push(PipelineStage {
            name: name.to_string(),
            stage: CompressionStage::compress(config),
        });
        self
    }

    /// Add a decompression stage.
    pub fn add_decompress(mut self, name: &str, config: CompressionConfig) -> Self {
        self.stages.push(PipelineStage {
            name: name.to_string(),
            stage: CompressionStage::decompress(config),
        });
        self
    }

    /// Process data through all stages.
    pub fn process(&mut self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        let mut current = data.to_vec();
        self.stats.total_in += data.len() as u64;

        for pipeline_stage in &mut self.stages {
            current = pipeline_stage.stage.compress_chunk(&current)?;
        }

        self.stats.total_out += current.len() as u64;
        self.stats.batches += 1;
        Ok(current)
    }

    /// Get stats for a specific stage by name.
    pub fn stage_stats(&self, name: &str) -> Option<&CompressionStats> {
        self.stages.iter()
            .find(|s| s.name == name)
            .map(|s| s.stage.stats())
    }

    pub fn stage_count(&self) -> usize {
        self.stages.len()
    }

    pub fn stats(&self) -> &PipelineStats {
        &self.stats
    }

    /// Reset all stages.
    pub fn reset(&mut self) {
        for s in &mut self.stages {
            s.stage.reset();
        }
        self.stats = PipelineStats::default();
    }
}

#[derive(Debug, Clone, Default)]
pub struct PipelineStats {
    pub total_in: u64,
    pub total_out: u64,
    pub batches: u64,
}

impl PipelineStats {
    pub fn overall_ratio(&self) -> f64 {
        if self.total_in == 0 { return 1.0; }
        self.total_out as f64 / self.total_in as f64
    }
}

// ---------------------------------------------------------------------------
// CodecRegistry
// ---------------------------------------------------------------------------

/// Registry for codec configurations with named presets.
#[derive(Debug)]
pub struct CodecRegistry {
    codecs: HashMap<String, CompressionConfig>,
}

impl CodecRegistry {
    pub fn new() -> Self {
        let mut codecs = HashMap::new();
        codecs.insert("gzip".into(), CompressionConfig::gzip());
        codecs.insert("gzip-fast".into(), CompressionConfig::gzip().with_level(CompressionLevel::Fast));
        codecs.insert("gzip-best".into(), CompressionConfig::gzip().with_level(CompressionLevel::Best));
        codecs.insert("deflate".into(), CompressionConfig::deflate());
        codecs.insert("snappy".into(), CompressionConfig::snappy());
        codecs.insert("lz4".into(), CompressionConfig::lz4());
        codecs.insert("zstd".into(), CompressionConfig::zstd());
        codecs.insert("zstd-fast".into(), CompressionConfig::zstd().with_level(CompressionLevel::Fast));
        codecs.insert("identity".into(), CompressionConfig::identity());
        Self { codecs }
    }

    pub fn get(&self, name: &str) -> Option<&CompressionConfig> {
        self.codecs.get(name)
    }

    pub fn register(&mut self, name: &str, config: CompressionConfig) {
        self.codecs.insert(name.to_string(), config);
    }

    pub fn available_codecs(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.codecs.keys().map(|s| s.as_str()).collect();
        names.sort();
        names
    }

    pub fn contains(&self, name: &str) -> bool {
        self.codecs.contains_key(name)
    }
}

impl Default for CodecRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // === CompressionCodec tests ===

    #[test]
    fn test_codec_extension() {
        assert_eq!(CompressionCodec::Gzip.extension(), ".gz");
        assert_eq!(CompressionCodec::Deflate.extension(), ".zz");
        assert_eq!(CompressionCodec::Snappy.extension(), ".snappy");
        assert_eq!(CompressionCodec::Lz4.extension(), ".lz4");
        assert_eq!(CompressionCodec::Zstd.extension(), ".zst");
        assert_eq!(CompressionCodec::Identity.extension(), "");
    }

    #[test]
    fn test_codec_content_encoding() {
        assert_eq!(CompressionCodec::Gzip.content_encoding(), "gzip");
        assert_eq!(CompressionCodec::Identity.content_encoding(), "identity");
    }

    #[test]
    fn test_codec_is_compressed() {
        assert!(!CompressionCodec::Identity.is_compressed());
        assert!(CompressionCodec::Gzip.is_compressed());
        assert!(CompressionCodec::Zstd.is_compressed());
    }

    #[test]
    fn test_codec_detect_gzip() {
        let data = vec![0x1f, 0x8b, 0x08, 0x00];
        assert_eq!(CompressionCodec::detect(&data), Some(CompressionCodec::Gzip));
    }

    #[test]
    fn test_codec_detect_deflate() {
        let data = vec![0x78, 0x9c, 0x00, 0x00];
        assert_eq!(CompressionCodec::detect(&data), Some(CompressionCodec::Deflate));
    }

    #[test]
    fn test_codec_detect_zstd() {
        let data = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00];
        assert_eq!(CompressionCodec::detect(&data), Some(CompressionCodec::Zstd));
    }

    #[test]
    fn test_codec_detect_lz4() {
        let data = vec![0x04, 0x22, 0x4d, 0x18, 0x00];
        assert_eq!(CompressionCodec::detect(&data), Some(CompressionCodec::Lz4));
    }

    #[test]
    fn test_codec_detect_unknown() {
        let data = vec![0x00, 0x00, 0x00, 0x00];
        assert_eq!(CompressionCodec::detect(&data), None);
    }

    #[test]
    fn test_codec_detect_too_short() {
        let data = vec![0x1f];
        assert_eq!(CompressionCodec::detect(&data), None);
    }

    #[test]
    fn test_codec_from_str() {
        assert_eq!(CompressionCodec::from_str("gzip"), Some(CompressionCodec::Gzip));
        assert_eq!(CompressionCodec::from_str("gz"), Some(CompressionCodec::Gzip));
        assert_eq!(CompressionCodec::from_str("snappy"), Some(CompressionCodec::Snappy));
        assert_eq!(CompressionCodec::from_str("lz4"), Some(CompressionCodec::Lz4));
        assert_eq!(CompressionCodec::from_str("zstd"), Some(CompressionCodec::Zstd));
        assert_eq!(CompressionCodec::from_str("none"), Some(CompressionCodec::Identity));
        assert_eq!(CompressionCodec::from_str("unknown"), None);
    }

    #[test]
    fn test_codec_display() {
        assert_eq!(format!("{}", CompressionCodec::Gzip), "gzip");
        assert_eq!(format!("{}", CompressionCodec::Identity), "identity");
    }

    // === CompressionLevel tests ===

    #[test]
    fn test_level_to_numeric_gzip() {
        assert_eq!(CompressionLevel::Fastest.to_numeric(CompressionCodec::Gzip), 1);
        assert_eq!(CompressionLevel::Default.to_numeric(CompressionCodec::Gzip), 6);
        assert_eq!(CompressionLevel::Best.to_numeric(CompressionCodec::Gzip), 9);
    }

    #[test]
    fn test_level_to_numeric_zstd() {
        assert_eq!(CompressionLevel::Fastest.to_numeric(CompressionCodec::Zstd), 1);
        assert_eq!(CompressionLevel::Best.to_numeric(CompressionCodec::Zstd), 22);
    }

    #[test]
    fn test_level_custom() {
        assert_eq!(CompressionLevel::Custom(15).to_numeric(CompressionCodec::Zstd), 15);
    }

    #[test]
    fn test_level_default() {
        assert_eq!(CompressionLevel::default(), CompressionLevel::Default);
    }

    // === CompressionConfig tests ===

    #[test]
    fn test_config_gzip() {
        let config = CompressionConfig::gzip();
        assert_eq!(config.codec, CompressionCodec::Gzip);
        assert_eq!(config.level, CompressionLevel::Default);
    }

    #[test]
    fn test_config_with_level() {
        let config = CompressionConfig::gzip().with_level(CompressionLevel::Best);
        assert_eq!(config.level, CompressionLevel::Best);
    }

    #[test]
    fn test_config_with_chunk_size() {
        let config = CompressionConfig::gzip().with_chunk_size(16384);
        assert_eq!(config.chunk_size, 16384);
    }

    #[test]
    fn test_config_with_chunk_size_minimum() {
        let config = CompressionConfig::gzip().with_chunk_size(10);
        assert_eq!(config.chunk_size, 64); // minimum 64
    }

    #[test]
    fn test_config_with_flush() {
        let config = CompressionConfig::gzip().with_flush(true);
        assert!(config.flush_per_chunk);
    }

    #[test]
    fn test_config_all_codecs() {
        assert_eq!(CompressionConfig::deflate().codec, CompressionCodec::Deflate);
        assert_eq!(CompressionConfig::snappy().codec, CompressionCodec::Snappy);
        assert_eq!(CompressionConfig::lz4().codec, CompressionCodec::Lz4);
        assert_eq!(CompressionConfig::zstd().codec, CompressionCodec::Zstd);
        assert_eq!(CompressionConfig::identity().codec, CompressionCodec::Identity);
    }

    // === CompressionStage tests ===

    #[test]
    fn test_stage_compress_identity() {
        let mut stage = CompressionStage::new(CompressionConfig::identity());
        let data = b"hello world";
        let compressed = stage.compress_chunk(data).unwrap();
        assert_eq!(compressed, data);
    }

    #[test]
    fn test_stage_compress_gzip() {
        let mut stage = CompressionStage::new(CompressionConfig::gzip());
        let data = vec![0u8; 1000];
        let compressed = stage.compress_chunk(&data).unwrap();
        // Gzip should produce smaller output
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_stage_compress_snappy() {
        let mut stage = CompressionStage::new(CompressionConfig::snappy());
        let data = vec![0u8; 1000];
        let compressed = stage.compress_chunk(&data).unwrap();
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_stage_stats_after_compress() {
        let mut stage = CompressionStage::new(CompressionConfig::gzip());
        stage.compress_chunk(&[0u8; 100]).unwrap();
        assert_eq!(stage.stats().bytes_in, 100);
        assert!(stage.stats().bytes_out > 0);
        assert_eq!(stage.stats().chunks_processed, 1);
    }

    #[test]
    fn test_stage_multiple_chunks() {
        let mut stage = CompressionStage::new(CompressionConfig::lz4());
        stage.compress_chunk(&[0u8; 100]).unwrap();
        stage.compress_chunk(&[1u8; 200]).unwrap();
        stage.compress_chunk(&[2u8; 300]).unwrap();
        assert_eq!(stage.stats().bytes_in, 600);
        assert_eq!(stage.stats().chunks_processed, 3);
    }

    #[test]
    fn test_stage_decompress_gzip() {
        let mut stage = CompressionStage::new(CompressionConfig::gzip());
        let data = vec![0x1f, 0x8b, 0x01, 0x02, 0x03, 0x04, 0x05];
        let decompressed = stage.decompress_chunk(&data).unwrap();
        assert!(decompressed.len() >= data.len());
    }

    #[test]
    fn test_stage_state_transitions() {
        let mut stage = CompressionStage::new(CompressionConfig::gzip());
        assert_eq!(*stage.state(), StageState::Ready);

        stage.compress_chunk(&[0u8; 10]).unwrap();
        assert_eq!(*stage.state(), StageState::Ready);

        stage.finish().unwrap();
        assert_eq!(*stage.state(), StageState::Finished);
    }

    #[test]
    fn test_stage_error_after_finish() {
        let mut stage = CompressionStage::new(CompressionConfig::gzip());
        stage.finish().unwrap();
        assert!(stage.compress_chunk(&[0]).is_err());
    }

    #[test]
    fn test_stage_reset() {
        let mut stage = CompressionStage::new(CompressionConfig::gzip());
        stage.compress_chunk(&[0u8; 100]).unwrap();
        stage.finish().unwrap();

        stage.reset();
        assert_eq!(*stage.state(), StageState::Ready);
        assert_eq!(stage.stats().bytes_in, 0);
    }

    #[test]
    fn test_stage_flush() {
        let mut stage = CompressionStage::new(CompressionConfig::gzip());
        let flushed = stage.flush().unwrap();
        assert!(flushed.is_empty());
    }

    #[test]
    fn test_stage_config_access() {
        let config = CompressionConfig::zstd().with_level(CompressionLevel::Best);
        let stage = CompressionStage::new(config);
        assert_eq!(stage.config().codec, CompressionCodec::Zstd);
        assert_eq!(stage.config().level, CompressionLevel::Best);
    }

    // === CompressionStats tests ===

    #[test]
    fn test_stats_ratio() {
        let stats = CompressionStats {
            bytes_in: 1000,
            bytes_out: 400,
            ..Default::default()
        };
        assert!((stats.ratio() - 0.4).abs() < f64::EPSILON);
    }

    #[test]
    fn test_stats_savings_percent() {
        let stats = CompressionStats {
            bytes_in: 1000,
            bytes_out: 300,
            ..Default::default()
        };
        assert!((stats.savings_percent() - 70.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_stats_zero_input() {
        let stats = CompressionStats::default();
        assert!((stats.ratio() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_stats_avg_chunk_size() {
        let stats = CompressionStats {
            bytes_in: 3000,
            chunks_processed: 3,
            ..Default::default()
        };
        assert!((stats.avg_chunk_size() - 1000.0).abs() < f64::EPSILON);
    }

    // === CompressionPipeline tests ===

    #[test]
    fn test_pipeline_single_stage() {
        let mut pipeline = CompressionPipeline::new()
            .add_compress("gzip", CompressionConfig::gzip());

        let data = vec![0u8; 1000];
        let result = pipeline.process(&data).unwrap();
        assert!(result.len() < data.len());
    }

    #[test]
    fn test_pipeline_multi_stage() {
        let mut pipeline = CompressionPipeline::new()
            .add_compress("snappy", CompressionConfig::snappy())
            .add_compress("gzip", CompressionConfig::gzip());

        let data = vec![0u8; 1000];
        let result = pipeline.process(&data).unwrap();
        assert!(result.len() < data.len());
        assert_eq!(pipeline.stage_count(), 2);
    }

    #[test]
    fn test_pipeline_stage_stats() {
        let mut pipeline = CompressionPipeline::new()
            .add_compress("gzip", CompressionConfig::gzip());

        pipeline.process(&[0u8; 100]).unwrap();

        let stats = pipeline.stage_stats("gzip").unwrap();
        assert!(stats.bytes_in > 0);
    }

    #[test]
    fn test_pipeline_stats() {
        let mut pipeline = CompressionPipeline::new()
            .add_compress("lz4", CompressionConfig::lz4());

        pipeline.process(&[0u8; 500]).unwrap();
        pipeline.process(&[1u8; 300]).unwrap();

        assert_eq!(pipeline.stats().total_in, 800);
        assert_eq!(pipeline.stats().batches, 2);
        assert!(pipeline.stats().overall_ratio() < 1.0);
    }

    #[test]
    fn test_pipeline_reset() {
        let mut pipeline = CompressionPipeline::new()
            .add_compress("gzip", CompressionConfig::gzip());

        pipeline.process(&[0u8; 100]).unwrap();
        pipeline.reset();

        assert_eq!(pipeline.stats().total_in, 0);
        assert_eq!(pipeline.stats().batches, 0);
    }

    // === CodecRegistry tests ===

    #[test]
    fn test_registry_default_codecs() {
        let registry = CodecRegistry::new();
        assert!(registry.contains("gzip"));
        assert!(registry.contains("snappy"));
        assert!(registry.contains("lz4"));
        assert!(registry.contains("zstd"));
        assert!(registry.contains("identity"));
    }

    #[test]
    fn test_registry_get() {
        let registry = CodecRegistry::new();
        let config = registry.get("gzip").unwrap();
        assert_eq!(config.codec, CompressionCodec::Gzip);
    }

    #[test]
    fn test_registry_get_fast() {
        let registry = CodecRegistry::new();
        let config = registry.get("gzip-fast").unwrap();
        assert_eq!(config.level, CompressionLevel::Fast);
    }

    #[test]
    fn test_registry_register_custom() {
        let mut registry = CodecRegistry::new();
        let custom = CompressionConfig::zstd()
            .with_level(CompressionLevel::Custom(15))
            .with_chunk_size(4096);
        registry.register("zstd-custom", custom);
        assert!(registry.contains("zstd-custom"));
    }

    #[test]
    fn test_registry_available_codecs() {
        let registry = CodecRegistry::new();
        let codecs = registry.available_codecs();
        assert!(codecs.len() >= 9);
        assert!(codecs.contains(&"gzip"));
    }

    #[test]
    fn test_registry_not_found() {
        let registry = CodecRegistry::new();
        assert!(registry.get("nonexistent").is_none());
    }

    // === CompressionError tests ===

    #[test]
    fn test_error_display_invalid_data() {
        let err = CompressionError::InvalidData("corrupt".into());
        assert!(err.to_string().contains("corrupt"));
    }

    #[test]
    fn test_error_display_buffer_too_small() {
        let err = CompressionError::BufferTooSmall { needed: 1000, available: 500 };
        assert!(err.to_string().contains("1000"));
        assert!(err.to_string().contains("500"));
    }

    #[test]
    fn test_error_display_checksum() {
        let err = CompressionError::ChecksumMismatch { expected: 0xDEAD, actual: 0xBEEF };
        assert!(err.to_string().contains("0xdead"));
        assert!(err.to_string().contains("0xbeef"));
    }

    #[test]
    fn test_error_display_overflow() {
        let err = CompressionError::DecompressionOverflow { limit: 10000 };
        assert!(err.to_string().contains("10000"));
    }

    #[test]
    fn test_error_display_unsupported() {
        let err = CompressionError::UnsupportedCodec("brotli".into());
        assert!(err.to_string().contains("brotli"));
    }
}

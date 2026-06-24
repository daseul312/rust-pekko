//! 데이터 수집 및 전처리 모듈
//! 
//! ML4T 시스템의 데이터 파이프라인을 담당합니다:
//! - 시장 데이터 수집 (OHLCV)
//! - 뉴스 및 센티먼트 데이터
//! - 대체 데이터 (위성 이미지, 소셜 미디어 등)
//! - 데이터 정제 및 특성 추출
//! - 데이터 품질 관리

pub mod collectors;
pub mod processors;
pub mod storage;
pub mod quality;
pub mod features;
pub mod pipeline;
pub mod example;

pub use collectors::*;
pub use processors::*;
pub use storage::*;
pub use quality::*;
pub use features::*;
pub use pipeline::*;
pub use example::*;

use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use anyhow::Result;

/// 시장 데이터 구조
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketData {
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,
    pub vwap: f64,
    pub adjusted_close: Option<f64>,
    pub dividend: Option<f64>,
    pub split_ratio: Option<f64>,
}

/// 뉴스 데이터 구조
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewsData {
    pub id: String,
    pub title: String,
    pub content: String,
    pub summary: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub source: String,
    pub url: Option<String>,
    pub sentiment_score: Option<f64>, // -1.0 ~ 1.0
    pub sentiment_magnitude: Option<f64>,
    pub entities: Vec<Entity>,
    pub symbols: Vec<String>,
    pub categories: Vec<String>,
}

/// 개체명 인식 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub text: String,
    pub entity_type: EntityType,
    pub confidence: f64,
    pub start_pos: usize,
    pub end_pos: usize,
}

/// 개체 타입
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum EntityType {
    Company,
    Person,
    Location,
    Currency,
    Date,
    Number,
    Other,
}

/// 대체 데이터 구조
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlternativeData {
    pub id: String,
    pub data_type: AlternativeDataType,
    pub timestamp: DateTime<Utc>,
    pub source: String,
    pub metadata: HashMap<String, String>,
    pub raw_data: Vec<u8>,
    pub processed_features: HashMap<String, f64>,
}

/// 대체 데이터 타입
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AlternativeDataType {
    SatelliteImage,
    SocialMediaSentiment,
    EconomicIndicator,
    WeatherData,
    SupplyChainData,
    PatentData,
    RegulatoryFiling,
    Other,
}

impl std::fmt::Display for AlternativeDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlternativeDataType::SatelliteImage => write!(f, "satellite"),
            AlternativeDataType::SocialMediaSentiment => write!(f, "social_sentiment"),
            AlternativeDataType::EconomicIndicator => write!(f, "economic_indicator"),
            AlternativeDataType::WeatherData => write!(f, "weather"),
            AlternativeDataType::SupplyChainData => write!(f, "supply_chain"),
            AlternativeDataType::PatentData => write!(f, "patent"),
            AlternativeDataType::RegulatoryFiling => write!(f, "regulatory"),
            AlternativeDataType::Other => write!(f, "other"),
        }
    }
}

/// 데이터 소스 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceConfig {
    pub source_name: String,
    pub data_type: DataType,
    pub api_config: ApiConfig,
    pub update_frequency: UpdateFrequency,
    pub retry_config: RetryConfig,
    pub quality_thresholds: QualityThresholds,
}

/// 데이터 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    MarketData,
    NewsData,
    AlternativeData,
    FundamentalData,
    MacroEconomicData,
}

/// API 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    pub base_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub rate_limit: RateLimitConfig,
    pub timeout_seconds: u64,
    pub headers: HashMap<String, String>,
}

/// 속도 제한 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    pub requests_per_minute: u32,
    pub requests_per_hour: u32,
    pub requests_per_day: u32,
    pub burst_limit: u32,
}

/// 업데이트 빈도
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateFrequency {
    RealTime,
    EverySecond,
    EveryMinute,
    Every5Minutes,
    Every15Minutes,
    EveryHour,
    Daily,
    Weekly,
}

/// 재시도 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_multiplier: f64,
    pub jitter: bool,
}

/// 데이터 품질 임계값
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityThresholds {
    pub min_data_completeness: f64, // 0.0 ~ 1.0
    pub max_latency_seconds: u64,
    pub max_price_deviation: f64, // 가격 이상치 감지
    pub max_volume_spike: f64, // 거래량 급증 감지
    pub min_confidence_score: f64, // 신뢰도 최소값
}

/// 데이터 파이프라인 상태
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PipelineStatus {
    Initializing,
    Running,
    Paused,
    Stopped,
    Error(String),
}

/// 데이터 수집 통계
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionStats {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub total_records: u64,
    pub data_quality_score: f64,
    pub average_latency_ms: f64,
    pub last_update: DateTime<Utc>,
    pub errors: Vec<String>,
}

/// 데이터 품질 메트릭
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityMetrics {
    pub completeness: f64, // 데이터 완성도
    pub accuracy: f64, // 정확도
    pub consistency: f64, // 일관성
    pub timeliness: f64, // 시의성
    pub validity: f64, // 유효성
    pub overall_score: f64, // 전체 품질 점수
}

impl MarketData {
    /// 수익률 계산
    pub fn calculate_return(&self, prev_close: f64) -> f64 {
        (self.close - prev_close) / prev_close
    }
    
    /// 변동률 계산
    pub fn calculate_volatility(&self, prev_close: f64) -> f64 {
        let return_val = self.calculate_return(prev_close);
        return_val.abs()
    }
    
    /// 가격 범위 비율
    pub fn price_range_ratio(&self) -> f64 {
        if self.close > 0.0 {
            (self.high - self.low) / self.close
        } else {
            0.0
        }
    }
    
    /// 거래량 가중 평균 가격 편차
    pub fn vwap_deviation(&self) -> f64 {
        if self.vwap > 0.0 {
            (self.close - self.vwap) / self.vwap
        } else {
            0.0
        }
    }
}

impl NewsData {
    /// 센티먼트 점수 정규화
    pub fn normalized_sentiment(&self) -> f64 {
        self.sentiment_score.unwrap_or(0.0)
    }
    
    /// 중요도 점수 계산
    pub fn importance_score(&self) -> f64 {
        let sentiment_weight = self.sentiment_magnitude.unwrap_or(0.0);
        let entity_weight = self.entities.len() as f64 * 0.1;
        let symbol_weight = self.symbols.len() as f64 * 0.2;
        
        (sentiment_weight + entity_weight + symbol_weight).min(1.0)
    }
    
    /// 관련 심볼 여부 확인
    pub fn is_related_to(&self, symbol: &str) -> bool {
        self.symbols.contains(&symbol.to_string())
    }
}

impl AlternativeData {
    /// 특성 추출
    pub fn extract_feature(&self, feature_name: &str) -> Option<f64> {
        self.processed_features.get(feature_name).copied()
    }
    
    /// 특성 업데이트
    pub fn update_feature(&mut self, feature_name: String, value: f64) {
        self.processed_features.insert(feature_name, value);
    }
    
    /// 메타데이터 조회
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }
}

impl QualityMetrics {
    /// 품질 점수 계산
    pub fn calculate_overall_score(&mut self) {
        self.overall_score = (self.completeness + self.accuracy + self.consistency + 
                             self.timeliness + self.validity) / 5.0;
    }
    
    /// 품질 등급 반환
    pub fn quality_grade(&self) -> &'static str {
        match self.overall_score {
            score if score >= 0.9 => "A",
            score if score >= 0.8 => "B", 
            score if score >= 0.7 => "C",
            score if score >= 0.6 => "D",
            _ => "F",
        }
    }
}

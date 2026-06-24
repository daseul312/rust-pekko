//! 데이터 수집기 모듈
//! 
//! 다양한 데이터 소스로부터 데이터를 수집하는 수집기들을 제공합니다:
//! - 시장 데이터 수집기 (KIS API, Alpha Vantage, Yahoo Finance 등)
//! - 뉴스 데이터 수집기 (NewsAPI, Reuters, Bloomberg 등)
//! - 대체 데이터 수집기 (위성 이미지, 소셜 미디어 등)

pub mod market_data_collector;
pub mod news_collector;
pub mod alternative_data_collector;
pub mod rate_limiter;
pub mod base_collector;
pub mod real_api_collector;

pub use market_data_collector::*;
pub use news_collector::*;
pub use alternative_data_collector::*;
pub use rate_limiter::*;
pub use base_collector::*;
pub use real_api_collector::*;

use crate::data::{MarketData, NewsData, AlternativeData, DataSourceConfig};
use anyhow::Result;
use std::collections::HashMap;
use tokio::time::Duration;

/// 데이터 수집기 트레이트
pub trait DataCollector {
    type DataType;
    
    /// 데이터 수집기 초기화
    fn initialize(&mut self, config: DataSourceConfig) -> Result<()>;
    
    /// 데이터 수집
    fn collect(&self, params: CollectParams) -> Result<Self::DataType>;
    
    /// 배치 데이터 수집
    fn collect_batch(&self, params: Vec<CollectParams>) -> Result<Vec<Self::DataType>>;
    
    /// 데이터 소스 상태 확인
    fn health_check(&self) -> Result<HealthStatus>;
    
    /// 수집 통계 조회
    fn get_stats(&self) -> CollectionStats;
}

/// 수집 파라미터
#[derive(Debug, Clone)]
pub struct CollectParams {
    pub symbol: Option<String>,
    pub start_date: Option<chrono::DateTime<chrono::Utc>>,
    pub end_date: Option<chrono::DateTime<chrono::Utc>>,
    pub interval: Option<String>,
    pub limit: Option<u32>,
    pub additional_params: HashMap<String, String>,
}

/// 수집 통계
#[derive(Debug, Clone)]
pub struct CollectionStats {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub total_records: u64,
    pub average_latency_ms: f64,
    pub last_success: Option<chrono::DateTime<chrono::Utc>>,
    pub last_error: Option<String>,
}

/// 헬스 상태
#[derive(Debug, Clone)]
pub enum HealthStatus {
    Healthy,
    Degraded(String),
    Unhealthy(String),
}

/// 수집기 팩토리
pub struct CollectorFactory;

impl CollectorFactory {
    /// 시장 데이터 수집기 생성
    pub fn create_market_data_collector(source: &str) -> Result<Box<dyn DataCollector<DataType = MarketData>>> {
        match source {
            "kis" => Ok(Box::new(MarketDataCollector::new_kis())),
            "alpha_vantage" => Ok(Box::new(MarketDataCollector::new_alpha_vantage())),
            "yahoo" => Ok(Box::new(MarketDataCollector::new_yahoo())),
            _ => Err(anyhow::anyhow!("지원하지 않는 데이터 소스: {}", source))
        }
    }
    
    /// 뉴스 수집기 생성
    pub fn create_news_collector(source: &str) -> Result<Box<dyn DataCollector<DataType = NewsData>>> {
        match source {
            "newsapi" => Ok(Box::new(NewsCollector::new_newsapi())),
            "reuters" => Ok(Box::new(NewsCollector::new_reuters())),
            "bloomberg" => Ok(Box::new(NewsCollector::new_bloomberg())),
            _ => Err(anyhow::anyhow!("지원하지 않는 뉴스 소스: {}", source))
        }
    }
    
    /// 대체 데이터 수집기 생성
    pub fn create_alternative_data_collector(data_type: &str) -> Result<Box<dyn DataCollector<DataType = AlternativeData>>> {
        match data_type {
            "satellite" => Ok(Box::new(AlternativeDataCollector::new_satellite())),
            "social_sentiment" => Ok(Box::new(AlternativeDataCollector::new_social_sentiment())),
            "economic_indicators" => Ok(Box::new(AlternativeDataCollector::new_economic_indicators())),
            _ => Err(anyhow::anyhow!("지원하지 않는 대체 데이터 타입: {}", data_type))
        }
    }
}

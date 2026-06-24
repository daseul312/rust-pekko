//! 실제 API 연동 모듈
//! 
//! 다양한 외부 API들을 통합하여 데이터를 수집하고 거래를 실행합니다.

pub mod kis_api;
pub mod alpha_vantage;
pub mod news_api;
pub mod websocket;
pub mod rate_limiter;
pub mod real_kis_api;
pub mod real_alpha_vantage_api;

use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use anyhow::Result;
use chrono::{DateTime, Utc};
use async_trait::async_trait;

/// API 클라이언트 트레이트
#[async_trait]
pub trait ApiClient: Send + Sync + Clone {
    type Config;
    type Response;
    
    async fn new(config: Self::Config) -> Result<Self> where Self: Sized;
    async fn authenticate(&mut self) -> Result<()>;
    async fn get_market_data(&self, symbol: &str) -> Result<MarketDataResponse>;
    async fn get_account_info(&self) -> Result<AccountInfo>;
    async fn place_order(&self, order: &OrderRequest) -> Result<OrderResponse>;
    async fn get_positions(&self) -> Result<Vec<Position>>;
    async fn get_order_status(&self, order_id: &str) -> Result<OrderStatus>;
}

/// 시장 데이터 응답
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketDataResponse {
    pub symbol: String,
    pub price: f64,
    pub volume: u64,
    pub timestamp: DateTime<Utc>,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
}

/// 계좌 정보
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountInfo {
    pub account_id: String,
    pub total_balance: f64,
    pub cash_balance: f64,
    pub positions: Vec<Position>,
}

/// 주문 요청
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: f64,
    pub price: Option<f64>,
}

/// 주문 응답
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResponse {
    pub order_id: String,
    pub status: OrderStatus,
    pub filled_quantity: f64,
    pub filled_price: f64,
    pub commission: f64,
    pub timestamp: DateTime<Utc>,
}

/// 포지션 정보
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub quantity: f64,
    pub average_price: f64,
    pub current_price: f64,
    pub market_value: f64,
    pub unrealized_pnl: f64,
}

/// 주문 사이드
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

/// 주문 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
    Stop,
    StopLimit,
}

/// 주문 상태
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderStatus {
    Submitted,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
    Completed,
}

/// 주문 유효 기간
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeInForce {
    Day,
    GTC, // Good Till Cancelled
    IOC, // Immediate or Cancel
    FOK, // Fill or Kill
}

/// API 에러
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("인증 실패: {0}")]
    AuthenticationError(String),
    
    #[error("API 호출 제한: {0}")]
    RateLimitError(String),
    
    #[error("네트워크 오류: {0}")]
    NetworkError(String),
    
    #[error("데이터 파싱 오류: {0}")]
    ParseError(String),
    
    #[error("주문 오류: {0}")]
    OrderError(String),
    
    #[error("계좌 오류: {0}")]
    AccountError(String),
}

/// API 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    pub api_key: String,
    pub api_secret: String,
    pub base_url: String,
    pub timeout_ms: u64,
    pub retry_attempts: u32,
    pub rate_limit: RateLimitConfig,
}

/// API 제한 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    pub requests_per_second: u32,
    pub requests_per_minute: u32,
    pub requests_per_hour: u32,
    pub requests_per_day: u32,
}

//! ML4T 시스템의 모든 Actor들
//!
//! 전통적 Actor와 함수형 Actor를 모두 지원합니다.

pub mod ml_agent;
pub mod data_agent;
pub mod trading_agent;
pub mod risk_agent;
pub mod monitoring_agent;

// Event Sourcing + Persistence
pub mod trading_events;
pub mod persistent_trading_agent;

// 뉴스 기반 투자 시스템
pub mod news_collector_actor;
pub mod news_analyzer_actor;

// 함수형 Actor 모듈 (선택적)
#[cfg(feature = "functional_actors")]
pub mod functional;

pub use ml_agent::*;
pub use data_agent::*;
pub use trading_agent::*;
pub use risk_agent::*;
pub use monitoring_agent::*;
pub use trading_events::*;
pub use persistent_trading_agent::*;
pub use news_collector_actor::*;
pub use news_analyzer_actor::*;

// 함수형 Actor 재내보내기
#[cfg(feature = "functional_actors")]
pub use functional::*;

use serde::{Serialize, Deserialize};

/// 데이터 수집 Actor 메시지
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataMessage {
    CollectMarketData,
    ProcessNewsData,
    UpdateAlternativeData,
    DataReady { symbol: String, data: MarketData },
}

/// ML 모델 Actor 메시지
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MLMessage {
    PredictReturns { symbols: Vec<String> },
    TrainModel { model_type: String, features: Vec<String> },
    UpdateModel { model_id: String },
    PredictionReady { symbol: String, prediction: f64, confidence: f64 },
}

/// 거래 실행 Actor 메시지
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TradingMessage {
    GenerateSignals,
    ExecuteBuy { symbol: String, quantity: u32, price: f64 },
    ExecuteSell { symbol: String, quantity: u32, price: f64 },
    UpdatePosition { symbol: String, position: Position },
    SignalGenerated { symbol: String, signal: TradingSignal },
    ExecutePositions,
}

/// 리스크 관리 Actor 메시지
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RiskMessage {
    AssessPortfolioRisk,
    CheckPositionLimits,
    CalculateVaR,
    UpdateRiskMetrics { symbol: String, risk: RiskMetrics },
    RiskAlert { level: RiskLevel, message: String },
}

/// 모니터링 Actor 메시지
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MonitoringMessage {
    UpdatePerformance,
    GenerateReport,
    CheckSystemHealth,
    PerformanceUpdate { metrics: PerformanceMetrics },
    Alert { severity: AlertLevel, message: String },
}

/// 시장 데이터 구조 (간단 버전)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketData {
    pub symbol: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,
    pub vwap: f64,
}

// 기존 타입들 (간단한 placeholder)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub quantity: i64,
    pub average_price: f64,
    pub current_price: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TradingSignal {
    Buy { strength: f64 },
    Sell { strength: f64 },
    Hold,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskMetrics {
    pub var_95: f64,
    pub var_1d: f64,
    pub max_drawdown: f64,
    pub sharpe_ratio: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

impl std::fmt::Display for RiskLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RiskLevel::Low => write!(f, "낮음"),
            RiskLevel::Medium => write!(f, "중간"),
            RiskLevel::High => write!(f, "높음"),
            RiskLevel::Critical => write!(f, "심각"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub total_return: f64,
    pub annualized_return: f64,
    pub volatility: f64,
    pub max_drawdown: f64,
    pub daily_return: f64,
    pub sharpe_ratio: f64,
    pub win_rate: f64,
    pub total_trades: u32,
    pub portfolio_value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertLevel {
    Info,
    Warning,
    Error,
    Critical,
}

impl std::fmt::Display for AlertLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertLevel::Info => write!(f, "정보"),
            AlertLevel::Warning => write!(f, "경고"),
            AlertLevel::Error => write!(f, "오류"),
            AlertLevel::Critical => write!(f, "심각"),
        }
    }
}

/// Actor 초기화 함수
pub fn init() {
    #[cfg(feature = "functional_actors")]
    functional::init();
}
//! 백테스팅 엔진
//! 
//! ML4T 워크플로우의 핵심인 백테스팅 시스템을 구현합니다.
//! - 전략 평가 및 성능 분석
//! - 위험도 측정 (VaR, Sharpe Ratio, Max Drawdown)
//! - Walk-forward Analysis
//! - Monte Carlo Simulation
//! - 거래 비용 및 슬리피지 모델링

pub mod engine;
pub mod strategy;
pub mod performance;
pub mod risk_metrics;
pub mod walk_forward;
pub mod monte_carlo;
pub mod transaction_costs;

use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

/// 백테스팅 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestConfig {
    pub start_date: DateTime<Utc>,
    pub end_date: DateTime<Utc>,
    pub initial_capital: f64,
    pub symbols: Vec<String>,
    pub rebalance_frequency: RebalanceFrequency,
    pub transaction_costs: TransactionCosts,
    pub slippage: SlippageModel,
    pub benchmark: Option<String>,
    pub risk_free_rate: f64,
    pub max_position_size: f64,
    pub max_portfolio_risk: f64,
}

/// 리밸런싱 빈도
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RebalanceFrequency {
    Daily,
    Weekly,
    Monthly,
    Quarterly,
    Custom(u32), // 일 단위
}

/// 거래 비용 모델
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionCosts {
    pub commission_per_trade: f64,
    pub commission_rate: f64, // 거래 금액 대비 비율
    pub minimum_commission: f64,
    pub maximum_commission: f64,
}

/// 슬리피지 모델
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SlippageModel {
    Fixed(f64), // 고정 슬리피지 (bps)
    Linear(f64), // 거래량 대비 선형 슬리피지
    SquareRoot(f64), // 제곱근 모델
    MarketImpact { // 시장 임팩트 모델
        a: f64, // 기본 임팩트
        b: f64, // 거래량 계수
        sigma: f64, // 변동성 계수
    },
}

/// 백테스팅 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResult {
    pub config: BacktestConfig,
    pub performance: PerformanceMetrics,
    pub risk_metrics: RiskMetrics,
    pub trades: Vec<Trade>,
    pub portfolio_history: Vec<PortfolioSnapshot>,
    pub benchmark_comparison: Option<BenchmarkComparison>,
    pub execution_summary: ExecutionSummary,
}

/// 성능 메트릭스
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub total_return: f64,
    pub annualized_return: f64,
    pub volatility: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,
    pub information_ratio: f64,
    pub max_drawdown: f64,
    pub max_drawdown_duration: u32, // 일 단위
    pub win_rate: f64,
    pub profit_factor: f64,
    pub average_trade_return: f64,
    pub average_win: f64,
    pub average_loss: f64,
}

/// 위험 메트릭스
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskMetrics {
    pub var_95: f64, // 95% VaR
    pub var_99: f64, // 99% VaR
    pub cvar_95: f64, // 95% CVaR (Expected Shortfall)
    pub cvar_99: f64, // 99% CVaR
    pub beta: f64,
    pub alpha: f64,
    pub tracking_error: f64,
    pub downside_deviation: f64,
    pub skewness: f64,
    pub kurtosis: f64,
}

/// 거래 기록
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub trade_id: String,
    pub symbol: String,
    pub side: TradeSide,
    pub quantity: u32,
    pub entry_price: f64,
    pub exit_price: Option<f64>,
    pub entry_time: DateTime<Utc>,
    pub exit_time: Option<DateTime<Utc>>,
    pub pnl: Option<f64>,
    pub commission: f64,
    pub slippage: f64,
    pub holding_period: Option<u32>, // 일 단위
    pub return_pct: Option<f64>,
}

/// 거래 사이드
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TradeSide {
    Buy,
    Sell,
}

/// 포트폴리오 스냅샷
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioSnapshot {
    pub timestamp: DateTime<Utc>,
    pub total_value: f64,
    pub cash: f64,
    pub positions: HashMap<String, PositionSnapshot>,
    pub daily_return: f64,
    pub cumulative_return: f64,
    pub drawdown: f64,
}

/// 포지션 스냅샷
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub symbol: String,
    pub quantity: i32,
    pub market_price: f64,
    pub market_value: f64,
    pub cost_basis: f64,
    pub unrealized_pnl: f64,
    pub weight: f64, // 포트폴리오 대비 비중
}

/// 벤치마크 비교
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkComparison {
    pub benchmark_symbol: String,
    pub benchmark_return: f64,
    pub benchmark_volatility: f64,
    pub benchmark_sharpe: f64,
    pub excess_return: f64,
    pub tracking_error: f64,
    pub information_ratio: f64,
    pub beta: f64,
    pub alpha: f64,
}

/// 실행 요약
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionSummary {
    pub total_trades: u32,
    pub winning_trades: u32,
    pub losing_trades: u32,
    pub total_commission: f64,
    pub total_slippage: f64,
    pub average_trade_size: f64,
    pub turnover_rate: f64,
    pub execution_quality: ExecutionQuality,
}

/// 실행 품질
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionQuality {
    pub fill_rate: f64, // 체결률
    pub price_improvement: f64, // 가격 개선
    pub market_impact: f64, // 시장 임팩트
    pub timing_efficiency: f64, // 타이밍 효율성
}

/// 백테스팅 에러
#[derive(Debug, thiserror::Error)]
pub enum BacktestError {
    #[error("데이터 오류: {0}")]
    DataError(String),
    
    #[error("전략 오류: {0}")]
    StrategyError(String),
    
    #[error("실행 오류: {0}")]
    ExecutionError(String),
    
    #[error("리스크 관리 오류: {0}")]
    RiskError(String),
    
    #[error("설정 오류: {0}")]
    ConfigError(String),
}

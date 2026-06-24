//! Alpha Factor 연구 모듈
//! 
//! WorldQuant 101 Formulaic Alphas 기반 Alpha Factor 연구 시스템:
//! - Alpha Factor 정의 및 구현
//! - Factor 연구 및 백테스팅
//! - Factor 성능 분석 및 평가
//! - Factor 조합 및 포트폴리오 구성
//! - Factor 모니터링 및 리밸런싱

pub mod factor_definition;
pub mod factor_research;
pub mod factor_evaluation;
pub mod factor_combination;
pub mod factor_monitoring;
pub mod worldquant_101;

pub use factor_definition::*;
pub use factor_research::*;
pub use factor_evaluation::*;
pub use factor_combination::*;
pub use factor_monitoring::*;
pub use worldquant_101::*;

use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use anyhow::Result;

/// Alpha Factor 정의
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlphaFactor {
    pub id: String,
    pub name: String,
    pub description: String,
    pub formula: String,
    pub category: FactorCategory,
    pub parameters: HashMap<String, f64>,
    pub lookback_period: u32,
    pub universe: UniverseDefinition,
    pub rebalance_frequency: RebalanceFrequency,
    pub implementation: FactorImplementation,
}

/// Factor 카테고리
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FactorCategory {
    Momentum,
    MeanReversion,
    Value,
    Quality,
    Growth,
    Volatility,
    Volume,
    CrossSectional,
    TimeSeries,
    Fundamental,
    Technical,
    Alternative,
}

/// 유니버스 정의
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniverseDefinition {
    pub market: String,
    pub min_market_cap: Option<f64>,
    pub max_market_cap: Option<f64>,
    pub min_price: Option<f64>,
    pub max_price: Option<f64>,
    pub min_volume: Option<f64>,
    pub sectors: Option<Vec<String>>,
    pub exclude_sectors: Option<Vec<String>>,
    pub min_liquidity_days: Option<u32>,
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

/// Factor 구현 방식
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FactorImplementation {
    Formulaic(String), // 수식 기반
    MachineLearning(String), // ML 모델 기반
    Hybrid(String, String), // 수식 + ML 조합
}

/// Factor 연구 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorResearchResult {
    pub factor_id: String,
    pub research_period: (DateTime<Utc>, DateTime<Utc>),
    pub performance_metrics: FactorPerformanceMetrics,
    pub risk_metrics: FactorRiskMetrics,
    pub factor_statistics: FactorStatistics,
    pub backtest_results: BacktestResults,
    pub ic_analysis: ICAnalysis,
    pub turnover_analysis: TurnoverAnalysis,
    pub sector_exposure: SectorExposure,
    pub style_exposure: StyleExposure,
}

/// Factor 성능 지표
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorPerformanceMetrics {
    pub total_return: f64,
    pub annualized_return: f64,
    pub annualized_volatility: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,
    pub information_ratio: f64,
    pub hit_rate: f64,
    pub average_win: f64,
    pub average_loss: f64,
    pub profit_factor: f64,
    pub max_drawdown: f64,
    pub max_drawdown_duration: u32,
}

/// Factor 리스크 지표
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorRiskMetrics {
    pub var_95: f64,
    pub var_99: f64,
    pub cvar_95: f64,
    pub cvar_99: f64,
    pub tail_ratio: f64,
    pub downside_deviation: f64,
    pub beta: f64,
    pub tracking_error: f64,
    pub correlation_with_market: f64,
    pub correlation_with_benchmark: f64,
}

/// Factor 통계
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorStatistics {
    pub mean_ic: f64,
    pub ic_std: f64,
    pub ic_ir: f64,
    pub ic_skewness: f64,
    pub ic_kurtosis: f64,
    pub mean_factor_return: f64,
    pub factor_return_std: f64,
    pub factor_return_skewness: f64,
    pub factor_return_kurtosis: f64,
    pub factor_quantile_spread: f64,
    pub factor_decile_spread: f64,
}

/// 백테스트 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResults {
    pub start_date: DateTime<Utc>,
    pub end_date: DateTime<Utc>,
    pub initial_capital: f64,
    pub final_capital: f64,
    pub total_return: f64,
    pub annualized_return: f64,
    pub annualized_volatility: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub total_trades: u32,
    pub winning_trades: u32,
    pub losing_trades: u32,
    pub win_rate: f64,
    pub average_win: f64,
    pub average_loss: f64,
    pub profit_factor: f64,
    pub daily_returns: Vec<f64>,
    pub monthly_returns: Vec<f64>,
    pub equity_curve: Vec<(DateTime<Utc>, f64)>,
    pub drawdown_curve: Vec<(DateTime<Utc>, f64)>,
}

/// IC (Information Coefficient) 분석
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ICAnalysis {
    pub mean_ic: f64,
    pub ic_std: f64,
    pub ic_ir: f64,
    pub ic_skewness: f64,
    pub ic_kurtosis: f64,
    pub ic_autocorrelation: Vec<f64>,
    pub ic_by_period: HashMap<String, f64>, // 일별, 주별, 월별 IC
    pub ic_by_sector: HashMap<String, f64>,
    pub ic_by_market_cap: HashMap<String, f64>,
    pub ic_rolling: Vec<(DateTime<Utc>, f64)>,
    pub ic_quantile_analysis: QuantileAnalysis,
}

/// 퀀타일 분석
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantileAnalysis {
    pub quintile_returns: Vec<f64>,
    pub decile_returns: Vec<f64>,
    pub top_minus_bottom: f64,
    pub monotonicity_score: f64,
    pub quantile_turnover: Vec<f64>,
}

/// 턴오버 분석
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnoverAnalysis {
    pub average_turnover: f64,
    pub turnover_volatility: f64,
    pub turnover_by_period: HashMap<String, f64>,
    pub turnover_cost_impact: f64,
    pub effective_turnover: f64,
    pub turnover_attribution: TurnoverAttribution,
}

/// 턴오버 속성 분석
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnoverAttribution {
    pub factor_induced: f64,
    pub universe_changes: f64,
    pub rebalancing: f64,
    pub drift: f64,
}

/// 섹터 노출
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SectorExposure {
    pub sector_weights: HashMap<String, f64>,
    pub sector_returns: HashMap<String, f64>,
    pub sector_contribution: HashMap<String, f64>,
    pub sector_concentration: f64,
    pub max_sector_weight: f64,
    pub sector_rotation_speed: f64,
}

/// 스타일 노출
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StyleExposure {
    pub value_exposure: f64,
    pub growth_exposure: f64,
    pub momentum_exposure: f64,
    pub quality_exposure: f64,
    pub volatility_exposure: f64,
    pub size_exposure: f64,
    pub style_consistency: f64,
    pub style_drift: f64,
}

/// Factor 조합
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorCombination {
    pub id: String,
    pub name: String,
    pub factors: Vec<FactorWeight>,
    pub combination_method: CombinationMethod,
    pub rebalance_frequency: RebalanceFrequency,
    pub risk_model: RiskModel,
    pub constraints: PortfolioConstraints,
}

/// Factor 가중치
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorWeight {
    pub factor_id: String,
    pub weight: f64,
    pub weight_method: WeightMethod,
}

/// 가중치 방법
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WeightMethod {
    Equal,           // 동일 가중
    ICWeighted,      // IC 기반 가중
    VolatilityWeighted, // 변동성 기반 가중
    Optimization,    // 최적화 기반
    Custom(Vec<f64>), // 사용자 정의
}

/// 조합 방법
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CombinationMethod {
    LinearCombination,
    RankCombination,
    ZScoreCombination,
    PercentileCombination,
    OptimizedCombination,
}

/// 리스크 모델
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskModel {
    pub model_type: RiskModelType,
    pub lookback_period: u32,
    pub rebalance_frequency: RebalanceFrequency,
    pub factor_exposures: Vec<String>,
    pub specific_risk_model: bool,
}

/// 리스크 모델 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RiskModelType {
    FactorModel,
    StatisticalModel,
    FundamentalModel,
    HybridModel,
}

/// 포트폴리오 제약조건
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioConstraints {
    pub max_position_weight: f64,
    pub max_sector_weight: f64,
    pub max_turnover: f64,
    pub long_only: bool,
    pub leverage_limit: f64,
    pub concentration_limit: f64,
    pub liquidity_constraints: bool,
    pub transaction_cost_model: TransactionCostModel,
}

/// 거래 비용 모델
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionCostModel {
    pub commission_rate: f64,
    pub market_impact_rate: f64,
    pub timing_cost_rate: f64,
    pub bid_ask_spread: f64,
    pub liquidity_adjustment: f64,
}

/// Factor 모니터링 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorMonitoringConfig {
    pub factor_id: String,
    pub monitoring_frequency: MonitoringFrequency,
    pub alert_thresholds: AlertThresholds,
    pub performance_tracking: PerformanceTracking,
    pub risk_monitoring: RiskMonitoring,
    pub attribution_analysis: AttributionAnalysis,
}

/// 모니터링 빈도
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MonitoringFrequency {
    RealTime,
    Intraday(u32), // 분 단위
    Daily,
    Weekly,
    Monthly,
}

/// 알림 임계값
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertThresholds {
    pub ic_threshold: f64,
    pub return_threshold: f64,
    pub volatility_threshold: f64,
    pub drawdown_threshold: f64,
    pub turnover_threshold: f64,
    pub correlation_threshold: f64,
}

/// 성능 추적
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceTracking {
    pub track_returns: bool,
    pub track_ic: bool,
    pub track_turnover: bool,
    pub track_attribution: bool,
    pub track_risk_metrics: bool,
    pub benchmark_comparison: bool,
}

/// 리스크 모니터링
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskMonitoring {
    pub var_monitoring: bool,
    pub stress_testing: bool,
    pub scenario_analysis: bool,
    pub factor_exposure_monitoring: bool,
    pub concentration_monitoring: bool,
}

/// 속성 분석
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributionAnalysis {
    pub factor_attribution: bool,
    pub sector_attribution: bool,
    pub style_attribution: bool,
    pub country_attribution: bool,
    pub currency_attribution: bool,
    pub interaction_effects: bool,
}

/// Factor 연구 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorResearchConfig {
    pub research_period: (DateTime<Utc>, DateTime<Utc>),
    pub universe: UniverseDefinition,
    pub benchmark: String,
    pub transaction_costs: TransactionCostModel,
    pub risk_free_rate: f64,
    pub rebalance_frequency: RebalanceFrequency,
    pub lookback_period: u32,
    pub holding_period: u32,
    pub quantile_count: u32,
    pub statistical_tests: Vec<StatisticalTest>,
}

/// 통계적 검정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StatisticalTest {
    TTest,
    WilcoxonTest,
    BootstrapTest,
    MonteCarloTest,
    PermutationTest,
}

impl AlphaFactor {
    /// 새로운 Alpha Factor 생성
    pub fn new(
        id: String,
        name: String,
        description: String,
        formula: String,
        category: FactorCategory,
    ) -> Self {
        Self {
            id,
            name,
            description,
            formula,
            category,
            parameters: HashMap::new(),
            lookback_period: 252, // 1년
            universe: UniverseDefinition::default(),
            rebalance_frequency: RebalanceFrequency::Daily,
            implementation: FactorImplementation::Formulaic("".to_string()),
        }
    }

    /// 파라미터 설정
    pub fn with_parameters(mut self, parameters: HashMap<String, f64>) -> Self {
        self.parameters = parameters;
        self
    }

    /// 룩백 기간 설정
    pub fn with_lookback_period(mut self, period: u32) -> Self {
        self.lookback_period = period;
        self
    }

    /// 유니버스 설정
    pub fn with_universe(mut self, universe: UniverseDefinition) -> Self {
        self.universe = universe;
        self
    }

    /// 리밸런싱 빈도 설정
    pub fn with_rebalance_frequency(mut self, frequency: RebalanceFrequency) -> Self {
        self.rebalance_frequency = frequency;
        self
    }

    /// 구현 방식 설정
    pub fn with_implementation(mut self, implementation: FactorImplementation) -> Self {
        self.implementation = implementation;
        self
    }
}

impl Default for UniverseDefinition {
    fn default() -> Self {
        Self {
            market: "KOSPI".to_string(),
            min_market_cap: Some(100_000_000_000.0), // 1000억원
            max_market_cap: None,
            min_price: Some(1000.0),
            max_price: None,
            min_volume: Some(1_000_000.0), // 100만주
            sectors: None,
            exclude_sectors: Some(vec!["금융".to_string()]),
            min_liquidity_days: Some(250),
        }
    }
}

impl FactorPerformanceMetrics {
    /// 새로운 성능 지표 생성
    pub fn new() -> Self {
        Self {
            total_return: 0.0,
            annualized_return: 0.0,
            annualized_volatility: 0.0,
            sharpe_ratio: 0.0,
            sortino_ratio: 0.0,
            calmar_ratio: 0.0,
            information_ratio: 0.0,
            hit_rate: 0.0,
            average_win: 0.0,
            average_loss: 0.0,
            profit_factor: 0.0,
            max_drawdown: 0.0,
            max_drawdown_duration: 0,
        }
    }

    /// 성능 지표 계산
    pub fn calculate_metrics(&mut self, returns: &[f64], benchmark_returns: &[f64]) {
        if returns.is_empty() {
            return;
        }

        // 기본 통계
        self.total_return = returns.iter().fold(1.0, |acc, &r| acc * (1.0 + r)) - 1.0;
        self.annualized_return = (1.0 + self.total_return).powf(252.0 / returns.len() as f64) - 1.0;
        
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|&r| (r - mean_return).powi(2))
            .sum::<f64>() / returns.len() as f64;
        self.annualized_volatility = variance.sqrt() * (252.0_f64).sqrt();

        // Sharpe Ratio (무위험수익률 3% 가정)
        let risk_free_rate = 0.03;
        self.sharpe_ratio = (self.annualized_return - risk_free_rate) / self.annualized_volatility;

        // Sortino Ratio
        let downside_returns: Vec<f64> = returns.iter()
            .filter(|&&r| r < 0.0)
            .copied()
            .collect();
        if !downside_returns.is_empty() {
            let downside_variance = downside_returns.iter()
                .map(|&r| r.powi(2))
                .sum::<f64>() / downside_returns.len() as f64;
            self.sortino_ratio = (self.annualized_return - risk_free_rate) / downside_variance.sqrt();
        }

        // Information Ratio
        if benchmark_returns.len() == returns.len() {
            let active_returns: Vec<f64> = returns.iter()
                .zip(benchmark_returns.iter())
                .map(|(&r, &br)| r - br)
                .collect();
            
            let mean_active_return = active_returns.iter().sum::<f64>() / active_returns.len() as f64;
            let active_variance = active_returns.iter()
                .map(|&ar| (ar - mean_active_return).powi(2))
                .sum::<f64>() / active_returns.len() as f64;
            
            self.information_ratio = mean_active_return / active_variance.sqrt() * (252.0_f64).sqrt();
        }

        // Hit Rate
        let positive_returns = returns.iter().filter(|&&r| r > 0.0).count();
        self.hit_rate = positive_returns as f64 / returns.len() as f64;

        // Win/Loss 분석
        let wins: Vec<f64> = returns.iter().filter(|&&r| r > 0.0).copied().collect();
        let losses: Vec<f64> = returns.iter().filter(|&&r| r < 0.0).copied().collect();
        
        self.average_win = if !wins.is_empty() {
            wins.iter().sum::<f64>() / wins.len() as f64
        } else {
            0.0
        };
        
        self.average_loss = if !losses.is_empty() {
            losses.iter().sum::<f64>() / losses.len() as f64
        } else {
            0.0
        };

        // Profit Factor
        let total_wins = wins.iter().sum::<f64>();
        let total_losses = losses.iter().sum::<f64>().abs();
        self.profit_factor = if total_losses > 0.0 {
            total_wins / total_losses
        } else if total_wins > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        // Maximum Drawdown
        let mut peak = 1.0;
        let mut max_dd = 0.0;
        let mut current_dd_duration = 0;
        let mut max_dd_duration = 0;

        for &ret in returns {
            peak = peak.max(1.0 + ret);
            let current_value = 1.0 + ret;
            let drawdown = (peak - current_value) / peak;
            
            if drawdown > max_dd {
                max_dd = drawdown;
            }
            
            if drawdown > 0.0 {
                current_dd_duration += 1;
                max_dd_duration = max_dd_duration.max(current_dd_duration);
            } else {
                current_dd_duration = 0;
            }
        }

        self.max_drawdown = max_dd;
        self.max_drawdown_duration = max_dd_duration;

        // Calmar Ratio
        self.calmar_ratio = if self.max_drawdown > 0.0 {
            self.annualized_return / self.max_drawdown
        } else {
            0.0
        };
    }
}

impl FactorRiskMetrics {
    /// 새로운 리스크 지표 생성
    pub fn new() -> Self {
        Self {
            var_95: 0.0,
            var_99: 0.0,
            cvar_95: 0.0,
            cvar_99: 0.0,
            tail_ratio: 0.0,
            downside_deviation: 0.0,
            beta: 0.0,
            tracking_error: 0.0,
            correlation_with_market: 0.0,
            correlation_with_benchmark: 0.0,
        }
    }

    /// 리스크 지표 계산
    pub fn calculate_risk_metrics(&mut self, returns: &[f64], market_returns: &[f64], benchmark_returns: &[f64]) {
        if returns.is_empty() {
            return;
        }

        // VaR 계산
        let mut sorted_returns = returns.to_vec();
        sorted_returns.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let var_95_index = (sorted_returns.len() as f64 * 0.05) as usize;
        let var_99_index = (sorted_returns.len() as f64 * 0.01) as usize;
        
        self.var_95 = if var_95_index < sorted_returns.len() {
            sorted_returns[var_95_index]
        } else {
            sorted_returns[0]
        };
        
        self.var_99 = if var_99_index < sorted_returns.len() {
            sorted_returns[var_99_index]
        } else {
            sorted_returns[0]
        };

        // CVaR 계산
        let cvar_95_returns: Vec<f64> = sorted_returns.iter()
            .take(var_95_index + 1)
            .copied()
            .collect();
        self.cvar_95 = if !cvar_95_returns.is_empty() {
            cvar_95_returns.iter().sum::<f64>() / cvar_95_returns.len() as f64
        } else {
            0.0
        };

        let cvar_99_returns: Vec<f64> = sorted_returns.iter()
            .take(var_99_index + 1)
            .copied()
            .collect();
        self.cvar_99 = if !cvar_99_returns.is_empty() {
            cvar_99_returns.iter().sum::<f64>() / cvar_99_returns.len() as f64
        } else {
            0.0
        };

        // Tail Ratio
        let positive_tail = sorted_returns.iter()
            .rev()
            .take(var_95_index + 1)
            .sum::<f64>() / (var_95_index + 1) as f64;
        let negative_tail = cvar_95_returns.iter().sum::<f64>() / cvar_95_returns.len() as f64;
        
        self.tail_ratio = if negative_tail != 0.0 {
            positive_tail / negative_tail.abs()
        } else {
            0.0
        };

        // Downside Deviation
        let downside_returns: Vec<f64> = returns.iter()
            .filter(|&&r| r < 0.0)
            .copied()
            .collect();
        
        if !downside_returns.is_empty() {
            let downside_variance = downside_returns.iter()
                .map(|&r| r.powi(2))
                .sum::<f64>() / downside_returns.len() as f64;
            self.downside_deviation = downside_variance.sqrt();
        }

        // Beta 및 상관관계
        if market_returns.len() == returns.len() {
            self.calculate_beta_and_correlation(returns, market_returns);
        }

        if benchmark_returns.len() == returns.len() {
            self.calculate_tracking_error(returns, benchmark_returns);
        }
    }

    /// Beta 및 상관관계 계산
    fn calculate_beta_and_correlation(&mut self, returns: &[f64], market_returns: &[f64]) {
        let n = returns.len() as f64;
        
        let mean_returns = returns.iter().sum::<f64>() / n;
        let mean_market = market_returns.iter().sum::<f64>() / n;
        
        let covariance = returns.iter()
            .zip(market_returns.iter())
            .map(|(&r, &mr)| (r - mean_returns) * (mr - mean_market))
            .sum::<f64>() / n;
        
        let market_variance = market_returns.iter()
            .map(|&mr| (mr - mean_market).powi(2))
            .sum::<f64>() / n;
        
        self.beta = if market_variance > 0.0 {
            covariance / market_variance
        } else {
            0.0
        };
        
        let returns_std = returns.iter()
            .map(|&r| (r - mean_returns).powi(2))
            .sum::<f64>() / n;
        
        let market_std = market_variance.sqrt();
        let returns_std = returns_std.sqrt();
        
        self.correlation_with_market = if returns_std > 0.0 && market_std > 0.0 {
            covariance / (returns_std * market_std)
        } else {
            0.0
        };
    }

    /// Tracking Error 계산
    fn calculate_tracking_error(&mut self, returns: &[f64], benchmark_returns: &[f64]) {
        let active_returns: Vec<f64> = returns.iter()
            .zip(benchmark_returns.iter())
            .map(|(&r, &br)| r - br)
            .collect();
        
        let mean_active = active_returns.iter().sum::<f64>() / active_returns.len() as f64;
        let variance = active_returns.iter()
            .map(|&ar| (ar - mean_active).powi(2))
            .sum::<f64>() / active_returns.len() as f64;
        
        self.tracking_error = variance.sqrt() * (252.0_f64).sqrt();
        
        // 벤치마크와의 상관관계
        let n = returns.len() as f64;
        let mean_returns = returns.iter().sum::<f64>() / n;
        let mean_benchmark = benchmark_returns.iter().sum::<f64>() / n;
        
        let covariance = returns.iter()
            .zip(benchmark_returns.iter())
            .map(|(&r, &br)| (r - mean_returns) * (br - mean_benchmark))
            .sum::<f64>() / n;
        
        let benchmark_variance = benchmark_returns.iter()
            .map(|&br| (br - mean_benchmark).powi(2))
            .sum::<f64>() / n;
        
        let returns_std = returns.iter()
            .map(|&r| (r - mean_returns).powi(2))
            .sum::<f64>() / n;
        
        let benchmark_std = benchmark_variance.sqrt();
        let returns_std = returns_std.sqrt();
        
        self.correlation_with_benchmark = if returns_std > 0.0 && benchmark_std > 0.0 {
            covariance / (returns_std * benchmark_std)
        } else {
            0.0
        };
    }
}

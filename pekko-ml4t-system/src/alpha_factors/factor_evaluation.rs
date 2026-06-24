//! Factor 평가 모듈
//! 
//! Alpha Factor의 성능 평가 및 검증을 위한 시스템:
//! - Factor 성능 지표 계산 및 분석
//! - Factor 안정성 및 지속성 평가
//! - Factor 리스크 분석 및 모니터링
//! - Factor 비교 및 순위 매기기
//! - Factor 선별 및 최적화

use super::*;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use anyhow::Result;
use tracing::{ warn, error};

/// Factor 평가 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorEvaluationConfig {
    pub evaluation_id: String,
    pub name: String,
    pub description: String,
    pub evaluation_period: (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>),
    pub factors_to_evaluate: Vec<String>, // Factor ID 리스트
    pub evaluation_metrics: Vec<EvaluationMetric>,
    pub comparison_benchmarks: Vec<String>,
    pub statistical_significance_level: f64,
    pub minimum_observation_period: u32, // 일 단위
    pub stability_test_periods: Vec<u32>, // 다양한 기간으로 안정성 테스트
    pub risk_adjustment_methods: Vec<RiskAdjustmentMethod>,
}

/// 평가 지표
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvaluationMetric {
    InformationCoefficient,
    InformationRatio,
    SharpeRatio,
    SortinoRatio,
    CalmarRatio,
    HitRate,
    ProfitFactor,
    MaximumDrawdown,
    TailRatio,
    Turnover,
    Stability,
    Persistence,
    Robustness,
    DecayRate,
    Capacity,
}

/// 리스크 조정 방법
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RiskAdjustmentMethod {
    MarketNeutral,
    SectorNeutral,
    StyleNeutral,
    VolatilityAdjusted,
    BetaAdjusted,
    VaRAdjusted,
    CVaRAdjusted,
}

/// Factor 평가자
pub struct FactorEvaluator {
    config: FactorEvaluationConfig,
    factor_results: HashMap<String, FactorResearchResult>,
    evaluation_results: HashMap<String, FactorEvaluationResult>,
    benchmark_data: HashMap<String, Vec<BenchmarkDataPoint>>,
    market_data: HashMap<String, Vec<MarketDataPoint>>,
}

/// Factor 평가 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorEvaluationResult {
    pub factor_id: String,
    pub evaluation_period: (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>),
    pub overall_score: f64,
    pub performance_score: f64,
    pub risk_score: f64,
    pub stability_score: f64,
    pub capacity_score: f64,
    pub ranking: u32,
    pub percentile: f64,
    pub evaluation_metrics: HashMap<EvaluationMetric, f64>,
    pub risk_adjusted_metrics: HashMap<RiskAdjustmentMethod, f64>,
    pub stability_analysis: StabilityAnalysis,
    pub persistence_analysis: PersistenceAnalysis,
    pub robustness_analysis: RobustnessAnalysis,
    pub capacity_analysis: CapacityAnalysis,
    pub recommendations: Vec<FactorRecommendation>,
    pub warnings: Vec<FactorWarning>,
}

/// 안정성 분석
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StabilityAnalysis {
    pub time_stability: TimeStabilityMetrics,
    pub cross_sectional_stability: CrossSectionalStabilityMetrics,
    pub market_regime_stability: MarketRegimeStabilityMetrics,
    pub stability_score: f64,
}

/// 시간 안정성 지표
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeStabilityMetrics {
    pub ic_stability: f64, // IC의 시간적 안정성
    pub return_stability: f64, // 수익률의 시간적 안정성
    pub volatility_stability: f64, // 변동성의 시간적 안정성
    pub consistency_score: f64, // 일관성 점수
}

/// 횡단면 안정성 지표
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossSectionalStabilityMetrics {
    pub sector_stability: f64, // 섹터 간 안정성
    pub size_stability: f64, // 시가총액 구간별 안정성
    pub style_stability: f64, // 스타일별 안정성
    pub concentration_stability: f64, // 집중도 안정성
}

/// 시장 체제별 안정성 지표
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketRegimeStabilityMetrics {
    pub bull_market_performance: f64, // 상승장 성능
    pub bear_market_performance: f64, // 하락장 성능
    pub high_volatility_performance: f64, // 고변동성 시기 성능
    pub low_volatility_performance: f64, // 저변동성 시기 성능
    pub regime_consistency: f64, // 체제별 일관성
}

/// 지속성 분석
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceAnalysis {
    pub ic_persistence: f64, // IC 지속성
    pub return_persistence: f64, // 수익률 지속성
    pub signal_persistence: f64, // 신호 지속성
    pub decay_rate: f64, // 쇠퇴율
    pub half_life: u32, // 반감기 (일 단위)
}

/// 견고성 분석
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RobustnessAnalysis {
    pub parameter_sensitivity: f64, // 파라미터 민감도
    pub outlier_sensitivity: f64, // 이상치 민감도
    pub universe_sensitivity: f64, // 유니버스 민감도
    pub time_period_sensitivity: f64, // 기간 민감도
    pub robustness_score: f64, // 전체 견고성 점수
}

/// 용량 분석
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityAnalysis {
    pub theoretical_capacity: f64, // 이론적 용량 (억원)
    pub practical_capacity: f64, // 실제 용량 (억원)
    pub capacity_utilization: f64, // 용량 활용도
    pub capacity_decay_rate: f64, // 용량 쇠퇴율
    pub liquidity_constraints: Vec<LiquidityConstraint>,
}

/// 유동성 제약
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidityConstraint {
    pub symbol: String,
    pub constraint_type: LiquidityConstraintType,
    pub severity: f64, // 0-1, 1이 가장 심각
    pub impact: f64, // 용량에 미치는 영향
}

/// 유동성 제약 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LiquidityConstraintType {
    LowVolume,
    HighBidAskSpread,
    HighMarketImpact,
    LimitedTradingHours,
    RegulatoryRestriction,
}

/// Factor 권고사항
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorRecommendation {
    pub recommendation_type: RecommendationType,
    pub priority: Priority,
    pub description: String,
    pub action: String,
    pub expected_impact: f64, // 예상 개선 효과
}

/// 권고사항 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecommendationType {
    ParameterOptimization,
    UniverseAdjustment,
    RebalancingFrequency,
    RiskManagement,
    CapacityManagement,
    SignalFiltering,
    ModelEnhancement,
}

/// 우선순위
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Priority {
    Low,
    Medium,
    High,
    Critical,
}

/// Factor 경고
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorWarning {
    pub warning_type: WarningType,
    pub severity: WarningSeverity,
    pub description: String,
    pub impact: String,
    pub mitigation: String,
}

/// 경고 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WarningType {
    PerformanceDegradation,
    HighRisk,
    LowStability,
    CapacityOverflow,
    ParameterDrift,
    ModelDecay,
    MarketRegimeChange,
    LiquidityIssue,
}

/// 경고 심각도
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WarningSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

impl FactorEvaluator {
    /// 새로운 Factor 평가자 생성
    pub fn new(config: FactorEvaluationConfig) -> Self {
        Self {
            config,
            factor_results: HashMap::new(),
            evaluation_results: HashMap::new(),
            benchmark_data: HashMap::new(),
            market_data: HashMap::new(),
        }
    }

    /// Factor 연구 결과 추가
    pub fn add_factor_result(&mut self, factor_id: String, result: FactorResearchResult) {
        self.factor_results.insert(factor_id, result);
    }

    /// 벤치마크 데이터 설정
    pub fn set_benchmark_data(&mut self, benchmark_id: String, data: Vec<BenchmarkDataPoint>) {
        self.benchmark_data.insert(benchmark_id, data);
    }

    /// 시장 데이터 설정
    pub fn set_market_data(&mut self, symbol: String, data: Vec<MarketDataPoint>) {
        self.market_data.insert(symbol, data);
    }

    /// Factor 평가 실행
    pub async fn evaluate_factors(&mut self) -> Result<HashMap<String, FactorEvaluationResult>> {
        let mut evaluation_results = HashMap::new();

        for factor_id in &self.config.factors_to_evaluate {
            if let Some(factor_result) = self.factor_results.get(factor_id) {
                let evaluation_result = self.evaluate_single_factor(factor_id, factor_result).await?;
                evaluation_results.insert(factor_id.clone(), evaluation_result);
            } else {
                warn!("Factor {}의 연구 결과를 찾을 수 없습니다", factor_id);
            }
        }

        // 전체 Factor 순위 매기기
        self.rank_factors(&mut evaluation_results).await?;

        self.evaluation_results = evaluation_results.clone();
        Ok(evaluation_results)
    }

    /// 단일 Factor 평가
    async fn evaluate_single_factor(&self, factor_id: &str, factor_result: &FactorResearchResult) -> Result<FactorEvaluationResult> {
        // 1. 성능 점수 계산
        let performance_score = self.calculate_performance_score(factor_result).await?;

        // 2. 리스크 점수 계산
        let risk_score = self.calculate_risk_score(factor_result).await?;

        // 3. 안정성 분석
        let stability_analysis = self.analyze_stability(factor_id, factor_result).await?;

        // 4. 지속성 분석
        let persistence_analysis = self.analyze_persistence(factor_id, factor_result).await?;

        // 5. 견고성 분석
        let robustness_analysis = self.analyze_robustness(factor_id, factor_result).await?;

        // 6. 용량 분석
        let capacity_analysis = self.analyze_capacity(factor_id, factor_result).await?;

        // 7. 평가 지표 계산
        let evaluation_metrics = self.calculate_evaluation_metrics(factor_result).await?;

        // 8. 리스크 조정 지표 계산
        let risk_adjusted_metrics = self.calculate_risk_adjusted_metrics(factor_result).await?;

        // 9. 권고사항 생성
        let recommendations = self.generate_recommendations(factor_id, factor_result, &stability_analysis, &robustness_analysis, &capacity_analysis).await?;

        // 10. 경고 생성
        let warnings = self.generate_warnings(factor_id, factor_result, &stability_analysis, &risk_score).await?;

        // 11. 전체 점수 계산
        let overall_score = self.calculate_overall_score(
            performance_score, 
            risk_score, 
            stability_analysis.stability_score, 
            capacity_analysis.capacity_utilization
        );

        let result = FactorEvaluationResult {
            factor_id: factor_id.to_string(),
            evaluation_period: self.config.evaluation_period,
            overall_score,
            performance_score,
            risk_score,
            stability_score: stability_analysis.stability_score,
            capacity_score: capacity_analysis.capacity_utilization,
            ranking: 0, // 나중에 설정
            percentile: 0.0, // 나중에 설정
            evaluation_metrics,
            risk_adjusted_metrics,
            stability_analysis,
            persistence_analysis,
            robustness_analysis,
            capacity_analysis,
            recommendations,
            warnings,
        };
        Ok(result)
    }

    /// 성능 점수 계산
    async fn calculate_performance_score(&self, factor_result: &FactorResearchResult) -> Result<f64> {
        let mut score = 0.0;
        let mut weight_sum = 0.0;

        // Information Coefficient 가중치
        let ic_weight = 0.3;
        score += factor_result.ic_analysis.mean_ic * ic_weight;
        weight_sum += ic_weight;

        // Information Ratio 가중치
        let ir_weight = 0.25;
        score += factor_result.performance_metrics.information_ratio * ir_weight * 0.1; // 정규화
        weight_sum += ir_weight;

        // Sharpe Ratio 가중치
        let sharpe_weight = 0.2;
        score += factor_result.performance_metrics.sharpe_ratio * sharpe_weight * 0.1; // 정규화
        weight_sum += sharpe_weight;

        // Hit Rate 가중치
        let hit_rate_weight = 0.15;
        score += (factor_result.performance_metrics.hit_rate - 0.5) * hit_rate_weight * 2.0; // 0.5를 기준으로 정규화
        weight_sum += hit_rate_weight;

        // Profit Factor 가중치
        let pf_weight = 0.1;
        score += (factor_result.performance_metrics.profit_factor - 1.0) * pf_weight * 0.1; // 1.0을 기준으로 정규화
        weight_sum += pf_weight;

        Ok(if weight_sum > 0.0 { score / weight_sum } else { 0.0 })
    }

    /// 리스크 점수 계산
    async fn calculate_risk_score(&self, factor_result: &FactorResearchResult) -> Result<f64> {
        let mut risk_score = 0.0;
        let mut risk_factors = 0.0;

        // Maximum Drawdown (낮을수록 좋음)
        let max_dd = factor_result.performance_metrics.max_drawdown.abs();
        risk_score += max_dd * 0.3;
        risk_factors += 0.3;

        // VaR (낮을수록 좋음)
        let var_95 = factor_result.risk_metrics.var_95.abs();
        risk_score += var_95 * 0.2;
        risk_factors += 0.2;

        // Tracking Error (낮을수록 좋음)
        let tracking_error = factor_result.risk_metrics.tracking_error.abs();
        risk_score += tracking_error * 0.2;
        risk_factors += 0.2;

        // Volatility (낮을수록 좋음)
        let volatility = factor_result.performance_metrics.annualized_volatility;
        risk_score += volatility * 0.15;
        risk_factors += 0.15;

        // Turnover (낮을수록 좋음)
        let turnover = factor_result.turnover_analysis.average_turnover;
        risk_score += turnover * 0.15;
        risk_factors += 0.15;

        // 리스크 점수는 낮을수록 좋으므로 반전
        Ok(if risk_factors > 0.0 { 1.0 - (risk_score / risk_factors) } else { 0.5 })
    }

    /// 안정성 분석
    async fn analyze_stability(&self, factor_id: &str, factor_result: &FactorResearchResult) -> Result<StabilityAnalysis> {
        // 시간 안정성
        let time_stability = self.analyze_time_stability(factor_result).await?;

        // 횡단면 안정성
        let cross_sectional_stability = self.analyze_cross_sectional_stability(factor_result).await?;

        // 시장 체제별 안정성
        let market_regime_stability = self.analyze_market_regime_stability(factor_result).await?;

        // 전체 안정성 점수
        let stability_score = (
            time_stability.consistency_score * 0.4 +
            cross_sectional_stability.sector_stability * 0.3 +
            market_regime_stability.regime_consistency * 0.3
        ).max(0.0).min(1.0);

        Ok(StabilityAnalysis {
            time_stability,
            cross_sectional_stability,
            market_regime_stability,
            stability_score,
        })
    }

    /// 시간 안정성 분석
    async fn analyze_time_stability(&self, factor_result: &FactorResearchResult) -> Result<TimeStabilityMetrics> {
        // IC 안정성 (IC 표준편차의 역수)
        let ic_stability = if factor_result.ic_analysis.ic_std > 0.0 {
            1.0 / (1.0 + factor_result.ic_analysis.ic_std)
        } else {
            1.0
        };

        // 수익률 안정성 (변동성의 역수)
        let return_stability = if factor_result.performance_metrics.annualized_volatility > 0.0 {
            1.0 / (1.0 + factor_result.performance_metrics.annualized_volatility)
        } else {
            1.0
        };

        // 변동성 안정성 (시뮬레이션)
        let volatility_stability = 0.8; // 실제로는 변동성의 변동성을 계산

        // 일관성 점수
        let consistency_score = (ic_stability * 0.4 + return_stability * 0.4 + volatility_stability * 0.2).max(0.0).min(1.0);

        Ok(TimeStabilityMetrics {
            ic_stability,
            return_stability,
            volatility_stability,
            consistency_score,
        })
    }

    /// 횡단면 안정성 분석
    async fn analyze_cross_sectional_stability(&self, factor_result: &FactorResearchResult) -> Result<CrossSectionalStabilityMetrics> {
        // 섹터 안정성 (섹터 집중도의 역수)
        let sector_stability = 1.0 - factor_result.sector_exposure.sector_concentration;

        // 시가총액 안정성 (시뮬레이션)
        let size_stability = 0.7;

        // 스타일 안정성 (스타일 일관성)
        let style_stability = factor_result.style_exposure.style_consistency;

        // 집중도 안정성 (최대 섹터 가중치의 역수)
        let concentration_stability = 1.0 - factor_result.sector_exposure.max_sector_weight;

        Ok(CrossSectionalStabilityMetrics {
            sector_stability,
            size_stability,
            style_stability,
            concentration_stability,
        })
    }

    /// 시장 체제별 안정성 분석
    async fn analyze_market_regime_stability(&self, factor_result: &FactorResearchResult) -> Result<MarketRegimeStabilityMetrics> {
        // 시뮬레이션된 체제별 성능 (실제로는 시장 체제를 구분하여 계산)
        let bull_market_performance = factor_result.performance_metrics.annualized_return * 1.2;
        let bear_market_performance = factor_result.performance_metrics.annualized_return * 0.8;
        let high_volatility_performance = factor_result.performance_metrics.annualized_return * 0.9;
        let low_volatility_performance = factor_result.performance_metrics.annualized_return * 1.1;

        // 체제별 일관성 (성능 차이의 역수)
        let regime_consistency = 1.0 / (1.0 + (bull_market_performance - bear_market_performance).abs());

        Ok(MarketRegimeStabilityMetrics {
            bull_market_performance,
            bear_market_performance,
            high_volatility_performance,
            low_volatility_performance,
            regime_consistency,
        })
    }

    /// 지속성 분석
    async fn analyze_persistence(&self, factor_id: &str, factor_result: &FactorResearchResult) -> Result<PersistenceAnalysis> {
        // IC 지속성 (IC IR)
        let ic_persistence = factor_result.ic_analysis.ic_ir / 10.0; // 정규화

        // 수익률 지속성 (시뮬레이션)
        let return_persistence = 0.6;

        // 신호 지속성 (시뮬레이션)
        let signal_persistence = 0.7;

        // 쇠퇴율 (시뮬레이션)
        let decay_rate = 0.05; // 월 5% 쇠퇴

        // 반감기 계산 (월 단위)
        let half_life = (0.693 / decay_rate) as u32;

        Ok(PersistenceAnalysis {
            ic_persistence,
            return_persistence,
            signal_persistence,
            decay_rate,
            half_life,
        })
    }

    /// 견고성 분석
    async fn analyze_robustness(&self, factor_id: &str, factor_result: &FactorResearchResult) -> Result<RobustnessAnalysis> {
        // 파라미터 민감도 (시뮬레이션)
        let parameter_sensitivity = 0.3; // 낮을수록 좋음

        // 이상치 민감도 (시뮬레이션)
        let outlier_sensitivity = 0.4;

        // 유니버스 민감도 (시뮬레이션)
        let universe_sensitivity = 0.2;

        // 기간 민감도 (시뮬레이션)
        let time_period_sensitivity = 0.5;

        // 전체 견고성 점수 (낮을수록 좋음)
        let robustness_score = 1.0 - (
            parameter_sensitivity * 0.3 +
            outlier_sensitivity * 0.25 +
            universe_sensitivity * 0.25 +
            time_period_sensitivity * 0.2
        );

        Ok(RobustnessAnalysis {
            parameter_sensitivity,
            outlier_sensitivity,
            universe_sensitivity,
            time_period_sensitivity,
            robustness_score,
        })
    }

    /// 용량 분석
    async fn analyze_capacity(&self, factor_id: &str, factor_result: &FactorResearchResult) -> Result<CapacityAnalysis> {
        // 이론적 용량 (시뮬레이션, 억원 단위)
        let theoretical_capacity = 1000.0;

        // 실제 용량 (거래량, 유동성 고려)
        let practical_capacity = theoretical_capacity * 0.6; // 60% 활용 가능

        // 용량 활용도
        let capacity_utilization = 0.3; // 현재 30% 활용

        // 용량 쇠퇴율
        let capacity_decay_rate = 0.02; // 월 2% 쇠퇴

        // 유동성 제약 (시뮬레이션)
        let liquidity_constraints = vec![
            LiquidityConstraint {
                symbol: "000270".to_string(),
                constraint_type: LiquidityConstraintType::LowVolume,
                severity: 0.3,
                impact: 0.1,
            },
            LiquidityConstraint {
                symbol: "005930".to_string(),
                constraint_type: LiquidityConstraintType::HighMarketImpact,
                severity: 0.2,
                impact: 0.05,
            },
        ];

        Ok(CapacityAnalysis {
            theoretical_capacity,
            practical_capacity,
            capacity_utilization,
            capacity_decay_rate,
            liquidity_constraints,
        })
    }

    /// 평가 지표 계산
    async fn calculate_evaluation_metrics(&self, factor_result: &FactorResearchResult) -> Result<HashMap<EvaluationMetric, f64>> {
        let mut metrics = HashMap::new();

        metrics.insert(EvaluationMetric::InformationCoefficient, factor_result.ic_analysis.mean_ic);
        metrics.insert(EvaluationMetric::InformationRatio, factor_result.performance_metrics.information_ratio);
        metrics.insert(EvaluationMetric::SharpeRatio, factor_result.performance_metrics.sharpe_ratio);
        metrics.insert(EvaluationMetric::SortinoRatio, factor_result.performance_metrics.sortino_ratio);
        metrics.insert(EvaluationMetric::CalmarRatio, factor_result.performance_metrics.calmar_ratio);
        metrics.insert(EvaluationMetric::HitRate, factor_result.performance_metrics.hit_rate);
        metrics.insert(EvaluationMetric::ProfitFactor, factor_result.performance_metrics.profit_factor);
        metrics.insert(EvaluationMetric::MaximumDrawdown, factor_result.performance_metrics.max_drawdown);
        metrics.insert(EvaluationMetric::TailRatio, factor_result.risk_metrics.tail_ratio);
        metrics.insert(EvaluationMetric::Turnover, factor_result.turnover_analysis.average_turnover);

        Ok(metrics)
    }

    /// 리스크 조정 지표 계산
    async fn calculate_risk_adjusted_metrics(&self, factor_result: &FactorResearchResult) -> Result<HashMap<RiskAdjustmentMethod, f64>> {
        let mut metrics = HashMap::new();

        // 시장 중립 수익률
        let market_neutral_return = factor_result.performance_metrics.annualized_return - 0.05; // 시장 수익률 5% 가정
        metrics.insert(RiskAdjustmentMethod::MarketNeutral, market_neutral_return);

        // 섹터 중립 수익률
        let sector_neutral_return = factor_result.performance_metrics.annualized_return * 0.9;
        metrics.insert(RiskAdjustmentMethod::SectorNeutral, sector_neutral_return);

        // 스타일 중립 수익률
        let style_neutral_return = factor_result.performance_metrics.annualized_return * 0.95;
        metrics.insert(RiskAdjustmentMethod::StyleNeutral, style_neutral_return);

        // 변동성 조정 수익률
        let volatility_adjusted_return = factor_result.performance_metrics.annualized_return / (1.0 + factor_result.performance_metrics.annualized_volatility);
        metrics.insert(RiskAdjustmentMethod::VolatilityAdjusted, volatility_adjusted_return);

        // 베타 조정 수익률
        let beta_adjusted_return = factor_result.performance_metrics.annualized_return - (factor_result.risk_metrics.beta * 0.05);
        metrics.insert(RiskAdjustmentMethod::BetaAdjusted, beta_adjusted_return);

        // VaR 조정 수익률
        let var_adjusted_return = factor_result.performance_metrics.annualized_return + factor_result.risk_metrics.var_95;
        metrics.insert(RiskAdjustmentMethod::VaRAdjusted, var_adjusted_return);

        // CVaR 조정 수익률
        let cvar_adjusted_return = factor_result.performance_metrics.annualized_return + factor_result.risk_metrics.cvar_95;
        metrics.insert(RiskAdjustmentMethod::CVaRAdjusted, cvar_adjusted_return);

        Ok(metrics)
    }

    /// 권고사항 생성
    async fn generate_recommendations(&self, factor_id: &str, factor_result: &FactorResearchResult,
                                    stability_analysis: &StabilityAnalysis, robustness_analysis: &RobustnessAnalysis,
                                    capacity_analysis: &CapacityAnalysis) -> Result<Vec<FactorRecommendation>> {
        let mut recommendations = Vec::new();

        // 성능 기반 권고사항
        if factor_result.performance_metrics.information_ratio < 1.0 {
            recommendations.push(FactorRecommendation {
                recommendation_type: RecommendationType::ModelEnhancement,
                priority: Priority::High,
                description: "Information Ratio가 낮습니다. 모델 개선이 필요합니다.".to_string(),
                action: "Feature engineering 및 모델 파라미터 최적화를 고려하세요.".to_string(),
                expected_impact: 0.2,
            });
        }

        // 안정성 기반 권고사항
        if stability_analysis.stability_score < 0.7 {
            recommendations.push(FactorRecommendation {
                recommendation_type: RecommendationType::UniverseAdjustment,
                priority: Priority::Medium,
                description: "Factor 안정성이 낮습니다. 유니버스 조정을 고려하세요.".to_string(),
                action: "더 안정적인 종목들로 유니버스를 재구성하세요.".to_string(),
                expected_impact: 0.15,
            });
        }

        // 견고성 기반 권고사항
        if robustness_analysis.robustness_score < 0.6 {
            recommendations.push(FactorRecommendation {
                recommendation_type: RecommendationType::ParameterOptimization,
                priority: Priority::Medium,
                description: "Factor 견고성이 낮습니다. 파라미터 최적화가 필요합니다.".to_string(),
                action: "다양한 시장 환경에서 테스트하여 파라미터를 조정하세요.".to_string(),
                expected_impact: 0.1,
            });
        }

        // 용량 기반 권고사항
        if capacity_analysis.capacity_utilization > 0.8 {
            recommendations.push(FactorRecommendation {
                recommendation_type: RecommendationType::CapacityManagement,
                priority: Priority::High,
                description: "Factor 용량 활용도가 높습니다. 용량 관리가 필요합니다.".to_string(),
                action: "포지션 크기를 줄이거나 유동성이 높은 종목으로 전환하세요.".to_string(),
                expected_impact: 0.1,
            });
        }

        // 턴오버 기반 권고사항
        if factor_result.turnover_analysis.average_turnover > 0.8 {
            recommendations.push(FactorRecommendation {
                recommendation_type: RecommendationType::RebalancingFrequency,
                priority: Priority::Medium,
                description: "턴오버가 높습니다. 리밸런싱 빈도를 조정하세요.".to_string(),
                action: "일일 리밸런싱에서 주간 또는 월간으로 변경을 고려하세요.".to_string(),
                expected_impact: 0.05,
            });
        }

        Ok(recommendations)
    }

    /// 경고 생성
    async fn generate_warnings(&self, factor_id: &str, factor_result: &FactorResearchResult,
                             stability_analysis: &StabilityAnalysis, risk_score: &f64) -> Result<Vec<FactorWarning>> {
        let mut warnings = Vec::new();

        // 성능 저하 경고
        if factor_result.performance_metrics.annualized_return < 0.0 {
            warnings.push(FactorWarning {
                warning_type: WarningType::PerformanceDegradation,
                severity: WarningSeverity::Error,
                description: "Factor가 음의 수익률을 보이고 있습니다.".to_string(),
                impact: "투자 성과에 부정적 영향을 미칠 수 있습니다.".to_string(),
                mitigation: "Factor 재검토 및 모델 수정을 고려하세요.".to_string(),
            });
        }

        // 높은 리스크 경고
        if *risk_score < 0.3 {
            warnings.push(FactorWarning {
                warning_type: WarningType::HighRisk,
                severity: WarningSeverity::Warning,
                description: "Factor의 리스크 수준이 높습니다.".to_string(),
                impact: "예상치 못한 손실이 발생할 수 있습니다.".to_string(),
                mitigation: "리스크 관리 규칙을 강화하세요.".to_string(),
            });
        }

        // 낮은 안정성 경고
        if stability_analysis.stability_score < 0.5 {
            warnings.push(FactorWarning {
                warning_type: WarningType::LowStability,
                severity: WarningSeverity::Warning,
                description: "Factor의 안정성이 낮습니다.".to_string(),
                impact: "성능이 예측하기 어려울 수 있습니다.".to_string(),
                mitigation: "안정성 개선을 위한 모델 수정을 고려하세요.".to_string(),
            });
        }

        // 높은 드로우다운 경고
        if factor_result.performance_metrics.max_drawdown > 0.15 {
            warnings.push(FactorWarning {
                warning_type: WarningType::HighRisk,
                severity: WarningSeverity::Error,
                description: "최대 드로우다운이 15%를 초과했습니다.".to_string(),
                impact: "상당한 손실이 발생할 수 있습니다.".to_string(),
                mitigation: "스탑로스 규칙을 강화하고 포지션 크기를 줄이세요.".to_string(),
            });
        }

        Ok(warnings)
    }

    /// 전체 점수 계산
    fn calculate_overall_score(&self, performance_score: f64, risk_score: f64, 
                              stability_score: f64, capacity_score: f64) -> f64 {
        // 가중 평균으로 전체 점수 계산
        let weights = (0.4, 0.3, 0.2, 0.1); // 성능, 리스크, 안정성, 용량
        
        performance_score * weights.0 +
        risk_score * weights.1 +
        stability_score * weights.2 +
        capacity_score * weights.3
    }

    /// Factor 순위 매기기
    async fn rank_factors(&self, evaluation_results: &mut HashMap<String, FactorEvaluationResult>) -> Result<()> {
        // 전체 점수로 정렬
        let mut sorted_factors: Vec<_> = evaluation_results.iter_mut().collect();
        sorted_factors.sort_by(|a, b| b.1.overall_score.partial_cmp(&a.1.overall_score).unwrap());

        // 순위 및 백분위 설정
        let total_factors = sorted_factors.len();
        for (rank, (_, result)) in sorted_factors.iter_mut().enumerate() {
            result.ranking = (rank + 1) as u32;
            result.percentile = 1.0 - (rank as f64 / total_factors as f64);
        }

        Ok(())
    }

    /// Factor 비교 리포트 생성
    pub async fn generate_comparison_report(&self) -> Result<FactorComparisonReport> {
        let mut comparison_report = FactorComparisonReport {
            evaluation_period: self.config.evaluation_period,
            total_factors: self.evaluation_results.len(),
            top_performers: Vec::new(),
            bottom_performers: Vec::new(),
            risk_adjusted_rankings: Vec::new(),
            stability_rankings: Vec::new(),
            capacity_rankings: Vec::new(),
            correlation_matrix: HashMap::new(),
            factor_attribution: FactorAttribution::new(),
            recommendations: Vec::new(),
        };

        // 상위/하위 성과자 식별
        let mut sorted_results: Vec<_> = self.evaluation_results.values().collect();
        sorted_results.sort_by(|a, b| b.overall_score.partial_cmp(&a.overall_score).unwrap());

        let top_count = (sorted_results.len() / 4).max(1); // 상위 25%
        let bottom_count = (sorted_results.len() / 4).max(1); // 하위 25%

        comparison_report.top_performers = sorted_results.iter().take(top_count)
            .map(|r| FactorSummary {
                factor_id: r.factor_id.clone(),
                overall_score: r.overall_score,
                performance_score: r.performance_score,
                risk_score: r.risk_score,
                stability_score: r.stability_score,
            })
            .collect();

        comparison_report.bottom_performers = sorted_results.iter().rev().take(bottom_count)
            .map(|r| FactorSummary {
                factor_id: r.factor_id.clone(),
                overall_score: r.overall_score,
                performance_score: r.performance_score,
                risk_score: r.risk_score,
                stability_score: r.stability_score,
            })
            .collect();

        // 리스크 조정 순위
        sorted_results.sort_by(|a, b| {
            let a_risk_adj = a.risk_adjusted_metrics.get(&RiskAdjustmentMethod::MarketNeutral).unwrap_or(&0.0);
            let b_risk_adj = b.risk_adjusted_metrics.get(&RiskAdjustmentMethod::MarketNeutral).unwrap_or(&0.0);
            b_risk_adj.partial_cmp(a_risk_adj).unwrap()
        });

        comparison_report.risk_adjusted_rankings = sorted_results.iter()
            .map(|r| FactorSummary {
                factor_id: r.factor_id.clone(),
                overall_score: *r.risk_adjusted_metrics.get(&RiskAdjustmentMethod::MarketNeutral).unwrap_or(&0.0),
                performance_score: r.performance_score,
                risk_score: r.risk_score,
                stability_score: r.stability_score,
            })
            .collect();

        Ok(comparison_report)
    }
}

/// Factor 비교 리포트
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorComparisonReport {
    pub evaluation_period: (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>),
    pub total_factors: usize,
    pub top_performers: Vec<FactorSummary>,
    pub bottom_performers: Vec<FactorSummary>,
    pub risk_adjusted_rankings: Vec<FactorSummary>,
    pub stability_rankings: Vec<FactorSummary>,
    pub capacity_rankings: Vec<FactorSummary>,
    pub correlation_matrix: HashMap<String, HashMap<String, f64>>,
    pub factor_attribution: FactorAttribution,
    pub recommendations: Vec<String>,
}

/// Factor 요약
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorSummary {
    pub factor_id: String,
    pub overall_score: f64,
    pub performance_score: f64,
    pub risk_score: f64,
    pub stability_score: f64,
}

/// Factor 속성 분석
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorAttribution {
    pub category_performance: HashMap<String, f64>,
    pub complexity_performance: HashMap<String, f64>,
    pub parameter_sensitivity: HashMap<String, f64>,
}

impl FactorAttribution {
    pub fn new() -> Self {
        Self {
            category_performance: HashMap::new(),
            complexity_performance: HashMap::new(),
            parameter_sensitivity: HashMap::new(),
        }
    }
}

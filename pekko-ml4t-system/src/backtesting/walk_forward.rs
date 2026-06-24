//! Walk-Forward Analysis
//! 
//! 백테스팅의 과적합을 방지하고 전략의 견고성을 검증하는 Walk-Forward Analysis

use super::{
    BacktestConfig, BacktestResult
};
use crate::backtesting::engine::{BacktestEngine, TradingStrategy};
use crate::actors::MarketData;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use chrono::{DateTime, Utc, Duration};
use anyhow::Result;
use tracing::{ warn, error, debug};

/// Walk-Forward 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardConfig {
    pub initial_training_period: u32, // 초기 훈련 기간 (일)
    pub retraining_frequency: u32, // 재훈련 빈도 (일)
    pub validation_period: u32, // 검증 기간 (일)
    pub min_training_samples: u32, // 최소 훈련 샘플 수
    pub strategy_optimization: bool, // 전략 파라미터 최적화 여부
    pub optimization_metric: OptimizationMetric, // 최적화 메트릭
}

/// 최적화 메트릭
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OptimizationMetric {
    SharpeRatio,
    SortinoRatio,
    CalmarRatio,
    TotalReturn,
    MaxDrawdown,
    ProfitFactor,
    WinRate,
}

/// Walk-Forward 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardResult {
    pub config: WalkForwardConfig,
    pub periods: Vec<WalkForwardPeriod>,
    pub overall_performance: WalkForwardPerformance,
    pub stability_metrics: StabilityMetrics,
    pub optimization_results: Option<OptimizationResults>,
}

/// Walk-Forward 기간
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardPeriod {
    pub period_id: u32,
    pub training_start: DateTime<Utc>,
    pub training_end: DateTime<Utc>,
    pub validation_start: DateTime<Utc>,
    pub validation_end: DateTime<Utc>,
    pub training_results: Option<BacktestResult>,
    pub validation_results: BacktestResult,
    pub strategy_parameters: HashMap<String, f64>,
    pub performance_metrics: PeriodPerformance,
}

/// 기간별 성능
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeriodPerformance {
    pub training_return: f64,
    pub validation_return: f64,
    pub training_sharpe: f64,
    pub validation_sharpe: f64,
    pub training_max_dd: f64,
    pub validation_max_dd: f64,
    pub consistency_score: f64, // 훈련과 검증 성능의 일관성
}

/// Walk-Forward 성능
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardPerformance {
    pub total_return: f64,
    pub annualized_return: f64,
    pub volatility: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub max_drawdown: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub total_trades: u32,
    pub average_period_return: f64,
    pub period_return_volatility: f64,
}

/// 안정성 메트릭스
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StabilityMetrics {
    pub return_consistency: f64, // 수익률 일관성
    pub parameter_stability: f64, // 파라미터 안정성
    pub performance_degradation: f64, // 성능 저하율
    pub overfitting_risk: f64, // 과적합 위험도
    pub robustness_score: f64, // 견고성 점수
}

/// 최적화 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationResults {
    pub parameter_ranges: HashMap<String, (f64, f64)>, // 파라미터 범위
    pub optimization_history: Vec<OptimizationStep>, // 최적화 과정
    pub best_parameters: HashMap<String, f64>, // 최적 파라미터
    pub parameter_sensitivity: HashMap<String, f64>, // 파라미터 민감도
}

/// 최적화 단계
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationStep {
    pub iteration: u32,
    pub parameters: HashMap<String, f64>,
    pub performance: f64,
    pub metric_used: OptimizationMetric,
}

/// Walk-Forward 분석기
pub struct WalkForwardAnalyzer<T>
where
    T: TradingStrategy,
{
    config: WalkForwardConfig,
    strategy_builder: Box<dyn Fn(HashMap<String, f64>) -> T>,
    data_cache: HashMap<String, Vec<MarketData>>,
}

impl<T> WalkForwardAnalyzer<T>
where
    T: TradingStrategy + Clone,
{
    /// 새로운 Walk-Forward 분석기 생성
    pub fn new(
        config: WalkForwardConfig,
        strategy_builder: Box<dyn Fn(HashMap<String, f64>) -> T>,
    ) -> Self {
        Self {
            config,
            strategy_builder,
            data_cache: HashMap::new(),
        }
    }

    /// 시장 데이터 로드
    pub fn load_market_data(&mut self, symbol: &str, data: Vec<MarketData>) {
        let data_len = data.len();
        self.data_cache.insert(symbol.to_string(), data);
    }

    /// Walk-Forward 분석 실행
    pub async fn run_analysis(&mut self, base_config: BacktestConfig) -> Result<WalkForwardResult> {
        let mut periods = Vec::new();
        let mut current_date = base_config.start_date;
        let mut period_id = 1;

        // Walk-Forward 기간 생성
        while current_date + Duration::days(self.config.initial_training_period as i64 + self.config.validation_period as i64) <= base_config.end_date {
            let training_start = current_date;
            let training_end = current_date + Duration::days(self.config.initial_training_period as i64);
            let validation_start = training_end + Duration::days(1);
            let validation_end = validation_start + Duration::days(self.config.validation_period as i64);
            // 1. 훈련 데이터 준비
            let training_data = self.prepare_training_data(training_start, training_end).await?;
            
            // 2. 전략 최적화 (선택적)
            let optimized_parameters = if self.config.strategy_optimization {
                self.optimize_strategy_parameters(&training_data, training_start, training_end).await?
            } else {
                HashMap::new() // 기본 파라미터 사용
            };

            // 3. 훈련 기간 백테스팅 (선택적)
            let training_results = if self.config.strategy_optimization {
                Some(self.run_period_backtest(&training_data, training_start, training_end, &optimized_parameters).await?)
            } else {
                None
            };

            // 4. 검증 기간 백테스팅
            let validation_data = self.prepare_validation_data(validation_start, validation_end).await?;
            let validation_results = self.run_period_backtest(&validation_data, validation_start, validation_end, &optimized_parameters).await?;

            // 5. 성능 메트릭스 계산
            let performance_metrics = self.calculate_period_performance(&training_results, &validation_results).await?;

            // Walk-Forward 기간 생성
            let period = WalkForwardPeriod {
                period_id,
                training_start,
                training_end,
                validation_start,
                validation_end,
                training_results,
                validation_results,
                strategy_parameters: optimized_parameters,
                performance_metrics,
            };

            periods.push(period);

            // 다음 기간으로 이동
            current_date += Duration::days(self.config.retraining_frequency as i64);
            period_id += 1;
        }
        // 전체 성능 계산
        let overall_performance = self.calculate_overall_performance(&periods).await?;
        
        // 안정성 메트릭스 계산
        let stability_metrics = self.calculate_stability_metrics(&periods).await?;
        
        // 최적화 결과 (선택적)
        let optimization_results = if self.config.strategy_optimization {
            Some(self.generate_optimization_results(&periods).await?)
        } else {
            None
        };

        Ok(WalkForwardResult {
            config: self.config.clone(),
            periods,
            overall_performance,
            stability_metrics,
            optimization_results,
        })
    }

    /// 훈련 데이터 준비
    async fn prepare_training_data(&self, start_date: DateTime<Utc>, end_date: DateTime<Utc>) -> Result<HashMap<String, Vec<MarketData>>> {
        let mut training_data = HashMap::new();
        
        for (symbol, all_data) in &self.data_cache {
            let filtered_data: Vec<MarketData> = all_data.iter()
                .filter(|data| data.timestamp >= start_date && data.timestamp <= end_date)
                .cloned()
                .collect();
            
            if filtered_data.len() >= self.config.min_training_samples as usize {
                training_data.insert(symbol.clone(), filtered_data);
            } else {
                warn!("{} 훈련 데이터 부족: {}개 (최소: {})", 
                      symbol, filtered_data.len(), self.config.min_training_samples);
            }
        }
        
        Ok(training_data)
    }

    /// 검증 데이터 준비
    async fn prepare_validation_data(&self, start_date: DateTime<Utc>, end_date: DateTime<Utc>) -> Result<HashMap<String, Vec<MarketData>>> {
        let mut validation_data = HashMap::new();
        
        for (symbol, all_data) in &self.data_cache {
            let filtered_data: Vec<MarketData> = all_data.iter()
                .filter(|data| data.timestamp >= start_date && data.timestamp <= end_date)
                .cloned()
                .collect();
            
            validation_data.insert(symbol.clone(), filtered_data);
        }
        
        Ok(validation_data)
    }

    /// 전략 파라미터 최적화
    async fn optimize_strategy_parameters(
        &self,
        training_data: &HashMap<String, Vec<MarketData>>,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
    ) -> Result<HashMap<String, f64>> {
        // TODO: 실제 최적화 알고리즘 구현 (Grid Search, Bayesian Optimization 등)
        // 현재는 더미 구현
        
        let mut best_parameters = HashMap::new();
        let mut best_performance = f64::NEG_INFINITY;
        
        // 간단한 그리드 서치 시뮬레이션
        let param_ranges = self.get_parameter_ranges();
        let total_combinations = param_ranges.values().map(|(_, max)| *max as usize).product::<usize>();
        // 샘플링된 파라미터 조합 테스트
        for i in 0..100.min(total_combinations) {
            let parameters = self.sample_parameters(&param_ranges);
            
            // 파라미터로 전략 생성 및 백테스팅
            let strategy = (self.strategy_builder)(parameters.clone());
            let performance = self.evaluate_parameters(&strategy, training_data, start_date, end_date).await?;
            
            if performance > best_performance {
                best_performance = performance;
                best_parameters = parameters;
            }
            
            debug!("최적화 진행: {}/100, 현재 최고 성능: {:.4}", i + 1, best_performance);
        }
        Ok(best_parameters)
    }

    /// 파라미터 범위 정의
    fn get_parameter_ranges(&self) -> HashMap<String, (f64, f64)> {
        let mut ranges = HashMap::new();
        ranges.insert("lookback_period".to_string(), (5.0, 50.0));
        ranges.insert("entry_threshold".to_string(), (0.5, 3.0));
        ranges.insert("exit_threshold".to_string(), (0.1, 1.0));
        ranges.insert("max_position_size".to_string(), (0.1, 0.5));
        ranges
    }

    /// 파라미터 샘플링
    fn sample_parameters(&self, ranges: &HashMap<String, (f64, f64)>) -> HashMap<String, f64> {
        let mut parameters = HashMap::new();
        
        for (param_name, (min, max)) in ranges {
            let value = min + (max - min) * (rand::random::<f64>());
            parameters.insert(param_name.clone(), value);
        }
        
        parameters
    }

    /// 파라미터 평가
    async fn evaluate_parameters(
        &self,
        strategy: &T,
        data: &HashMap<String, Vec<MarketData>>,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
    ) -> Result<f64> {
        // 간단한 백테스팅으로 파라미터 평가
        let mut config = BacktestConfig {
            start_date,
            end_date,
            initial_capital: 1000000.0,
            symbols: data.keys().cloned().collect(),
            rebalance_frequency: super::RebalanceFrequency::Daily,
            transaction_costs: super::TransactionCosts {
                commission_per_trade: 0.0,
                commission_rate: 0.001,
                minimum_commission: 0.0,
                maximum_commission: 1000.0,
            },
            slippage: super::SlippageModel::Fixed(5.0),
            benchmark: None,
            risk_free_rate: 0.02,
            max_position_size: 0.2,
            max_portfolio_risk: 0.1,
        };

        let mut engine = BacktestEngine::new(config);
        
        // 데이터 로드
        for (symbol, symbol_data) in data {
            engine.load_market_data(symbol, symbol_data.clone());
        }

        // 백테스팅 실행
        let result = engine.run_backtest(strategy).await?;
        
        // 최적화 메트릭 반환
        Ok(match self.config.optimization_metric {
            OptimizationMetric::SharpeRatio => result.performance.sharpe_ratio,
            OptimizationMetric::SortinoRatio => result.performance.sortino_ratio,
            OptimizationMetric::CalmarRatio => result.performance.calmar_ratio,
            OptimizationMetric::TotalReturn => result.performance.total_return,
            OptimizationMetric::MaxDrawdown => -result.performance.max_drawdown, // 최소화
            OptimizationMetric::ProfitFactor => result.performance.profit_factor,
            OptimizationMetric::WinRate => result.performance.win_rate,
        })
    }

    /// 기간별 백테스팅 실행
    async fn run_period_backtest(
        &self,
        data: &HashMap<String, Vec<MarketData>>,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
        parameters: &HashMap<String, f64>,
    ) -> Result<BacktestResult> {
        let config = BacktestConfig {
            start_date,
            end_date,
            initial_capital: 1000000.0,
            symbols: data.keys().cloned().collect(),
            rebalance_frequency: super::RebalanceFrequency::Daily,
            transaction_costs: super::TransactionCosts {
                commission_per_trade: 0.0,
                commission_rate: 0.001,
                minimum_commission: 0.0,
                maximum_commission: 1000.0,
            },
            slippage: super::SlippageModel::Fixed(5.0),
            benchmark: None,
            risk_free_rate: 0.02,
            max_position_size: 0.2,
            max_portfolio_risk: 0.1,
        };

        let mut engine = BacktestEngine::new(config);
        
        // 데이터 로드
        for (symbol, symbol_data) in data {
            engine.load_market_data(symbol, symbol_data.clone());
        }

        // 파라미터로 전략 생성
        let strategy = (self.strategy_builder)(parameters.clone());

        // 백테스팅 실행
        engine.run_backtest(&strategy).await
    }

    /// 기간별 성능 계산
    async fn calculate_period_performance(
        &self,
        training_results: &Option<BacktestResult>,
        validation_results: &BacktestResult,
    ) -> Result<PeriodPerformance> {
        let training_return = training_results.as_ref()
            .map(|r| r.performance.total_return)
            .unwrap_or(0.0);
        
        let validation_return = validation_results.performance.total_return;
        
        let training_sharpe = training_results.as_ref()
            .map(|r| r.performance.sharpe_ratio)
            .unwrap_or(0.0);
        
        let validation_sharpe = validation_results.performance.sharpe_ratio;
        
        let training_max_dd = training_results.as_ref()
            .map(|r| r.performance.max_drawdown)
            .unwrap_or(0.0);
        
        let validation_max_dd = validation_results.performance.max_drawdown;
        
        // 일관성 점수 계산 (훈련과 검증 성능의 상관관계)
        let consistency_score = if training_return != 0.0 {
            1.0 - (training_return - validation_return).abs() / training_return.abs()
        } else {
            0.0
        };

        Ok(PeriodPerformance {
            training_return,
            validation_return,
            training_sharpe,
            validation_sharpe,
            training_max_dd,
            validation_max_dd,
            consistency_score,
        })
    }

    /// 전체 성능 계산
    async fn calculate_overall_performance(&self, periods: &[WalkForwardPeriod]) -> Result<WalkForwardPerformance> {
        let total_return = periods.iter()
            .map(|p| p.validation_results.performance.total_return)
            .sum::<f64>() / periods.len() as f64;
        
        let annualized_return = (1.0 + total_return).powf(252.0 / self.config.validation_period as f64) - 1.0;
        
        let returns: Vec<f64> = periods.iter()
            .map(|p| p.validation_results.performance.total_return)
            .collect();
        
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let volatility = returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / returns.len() as f64;
        let volatility = volatility.sqrt() * (252.0 / self.config.validation_period as f64).sqrt();
        
        let sharpe_ratio = (annualized_return - 0.02) / volatility;
        
        let max_drawdown = periods.iter()
            .map(|p| p.validation_results.performance.max_drawdown)
            .fold(0.0, f64::max);
        
        let total_trades: u32 = periods.iter()
            .map(|p| p.validation_results.execution_summary.total_trades)
            .sum();
        
        let win_rate = periods.iter()
            .map(|p| p.validation_results.performance.win_rate)
            .sum::<f64>() / periods.len() as f64;
        
        let profit_factor = periods.iter()
            .map(|p| p.validation_results.performance.profit_factor)
            .sum::<f64>() / periods.len() as f64;

        Ok(WalkForwardPerformance {
            total_return,
            annualized_return,
            volatility,
            sharpe_ratio,
            sortino_ratio: 0.0, // TODO: Sortino ratio 계산
            max_drawdown,
            win_rate,
            profit_factor,
            total_trades,
            average_period_return: mean_return,
            period_return_volatility: volatility,
        })
    }

    /// 안정성 메트릭스 계산
    async fn calculate_stability_metrics(&self, periods: &[WalkForwardPeriod]) -> Result<StabilityMetrics> {
        let returns: Vec<f64> = periods.iter()
            .map(|p| p.validation_results.performance.total_return)
            .collect();
        
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let return_variance = returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        let return_consistency = 1.0 / (1.0 + return_variance);
        
        // 파라미터 안정성 (파라미터 변화량)
        let parameter_stability = self.calculate_parameter_stability(periods);
        
        // 성능 저하율 (훈련 vs 검증)
        let performance_degradation = self.calculate_performance_degradation(periods);
        
        // 과적합 위험도
        let overfitting_risk = self.calculate_overfitting_risk(periods);
        
        // 견고성 점수 (종합)
        let robustness_score = (return_consistency + parameter_stability + (1.0 - performance_degradation) + (1.0 - overfitting_risk)) / 4.0;

        Ok(StabilityMetrics {
            return_consistency,
            parameter_stability,
            performance_degradation,
            overfitting_risk,
            robustness_score,
        })
    }

    /// 파라미터 안정성 계산
    fn calculate_parameter_stability(&self, periods: &[WalkForwardPeriod]) -> f64 {
        if periods.len() < 2 {
            return 1.0;
        }

        let mut total_variance = 0.0;
        let mut param_count = 0;

        // 모든 파라미터에 대해 변화량 계산
        let all_params: Vec<&str> = periods[0].strategy_parameters.keys().map(|k| k.as_str()).collect();
        
        for param in all_params {
            let values: Vec<f64> = periods.iter()
                .filter_map(|p| p.strategy_parameters.get(param))
                .cloned()
                .collect();
            
            if values.len() > 1 {
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance = values.iter()
                    .map(|v| (v - mean).powi(2))
                    .sum::<f64>() / values.len() as f64;
                
                total_variance += variance;
                param_count += 1;
            }
        }

        if param_count > 0 {
            1.0 / (1.0 + total_variance / param_count as f64)
        } else {
            1.0
        }
    }

    /// 성능 저하율 계산
    fn calculate_performance_degradation(&self, periods: &[WalkForwardPeriod]) -> f64 {
        let training_returns: Vec<f64> = periods.iter()
            .filter_map(|p| p.training_results.as_ref())
            .map(|r| r.performance.total_return)
            .collect();
        
        let validation_returns: Vec<f64> = periods.iter()
            .map(|p| p.validation_results.performance.total_return)
            .collect();

        if training_returns.is_empty() || validation_returns.is_empty() {
            return 0.0;
        }

        let training_mean = training_returns.iter().sum::<f64>() / training_returns.len() as f64;
        let validation_mean = validation_returns.iter().sum::<f64>() / validation_returns.len() as f64;

        if training_mean > 0.0 {
            (training_mean - validation_mean) / training_mean.abs()
        } else {
            0.0
        }
    }

    /// 과적합 위험도 계산
    fn calculate_overfitting_risk(&self, periods: &[WalkForwardPeriod]) -> f64 {
        let mut risk_scores = Vec::new();

        for period in periods {
            if let Some(training_results) = &period.training_results {
                let training_sharpe = training_results.performance.sharpe_ratio;
                let validation_sharpe = period.validation_results.performance.sharpe_ratio;
                
                if training_sharpe > 0.0 {
                    let degradation = (training_sharpe - validation_sharpe) / training_sharpe;
                    risk_scores.push(degradation.max(0.0));
                }
            }
        }

        if risk_scores.is_empty() {
            0.0
        } else {
            risk_scores.iter().sum::<f64>() / risk_scores.len() as f64
        }
    }

    /// 최적화 결과 생성
    async fn generate_optimization_results(&self, periods: &[WalkForwardPeriod]) -> Result<OptimizationResults> {
        // TODO: 최적화 과정 및 파라미터 민감도 분석 구현
        Ok(OptimizationResults {
            parameter_ranges: HashMap::new(),
            optimization_history: Vec::new(),
            best_parameters: HashMap::new(),
            parameter_sensitivity: HashMap::new(),
        })
    }
}

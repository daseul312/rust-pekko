//! Monte Carlo Simulation
//! 
//! 백테스팅 결과의 통계적 유의성을 검증하고 다양한 시나리오를 시뮬레이션

use super::{BacktestResult, PerformanceMetrics};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use anyhow::Result;
use tracing::{ warn, error, debug};
use rand::Rng;

/// Monte Carlo 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonteCarloConfig {
    pub num_simulations: u32, // 시뮬레이션 횟수
    pub confidence_levels: Vec<f64>, // 신뢰 구간 (예: [0.05, 0.95])
    pub bootstrap_method: BootstrapMethod, // 부트스트랩 방법
    pub scenario_analysis: bool, // 시나리오 분석 여부
    pub stress_testing: bool, // 스트레스 테스트 여부
    pub random_seed: Option<u64>, // 랜덤 시드
}

/// 부트스트랩 방법
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BootstrapMethod {
    BlockBootstrap { block_size: usize }, // 블록 부트스트랩
    StationaryBootstrap { block_size: usize }, // 정상 부트스트랩
    CircularBootstrap { block_size: usize }, // 원형 부트스트랩
    ResidualBootstrap, // 잔차 부트스트랩
}

/// Monte Carlo 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonteCarloResult {
    pub config: MonteCarloConfig,
    pub original_performance: PerformanceMetrics,
    pub simulation_results: Vec<SimulationResult>,
    pub confidence_intervals: HashMap<String, ConfidenceInterval>,
    pub statistical_tests: StatisticalTests,
    pub scenario_analysis: Option<ScenarioAnalysis>,
    pub stress_test_results: Option<StressTestResults>,
}

/// 시뮬레이션 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationResult {
    pub simulation_id: u32,
    pub total_return: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub volatility: f64,
    pub return_series: Vec<f64>,
}

/// 신뢰 구간
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfidenceInterval {
    pub metric: String,
    pub lower_bound: f64,
    pub upper_bound: f64,
    pub confidence_level: f64,
    pub original_value: f64,
    pub p_value: f64, // 통계적 유의성
}

/// 통계적 검정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticalTests {
    pub sharpe_ratio_test: SharpeRatioTest,
    pub max_drawdown_test: MaxDrawdownTest,
    pub return_significance_test: ReturnSignificanceTest,
    pub stability_test: StabilityTest,
}

/// Sharpe Ratio 검정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharpeRatioTest {
    pub original_sharpe: f64,
    pub p_value: f64,
    pub is_significant: bool,
    pub confidence_interval: (f64, f64),
}

/// Max Drawdown 검정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaxDrawdownTest {
    pub original_max_dd: f64,
    pub p_value: f64,
    pub is_significant: bool,
    pub confidence_interval: (f64, f64),
}

/// 수익률 유의성 검정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReturnSignificanceTest {
    pub original_return: f64,
    pub p_value: f64,
    pub is_significant: bool,
    pub confidence_interval: (f64, f64),
}

/// 안정성 검정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StabilityTest {
    pub consistency_score: f64,
    pub p_value: f64,
    pub is_stable: bool,
}

/// 시나리오 분석
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioAnalysis {
    pub bull_market_scenario: ScenarioResult,
    pub bear_market_scenario: ScenarioResult,
    pub high_volatility_scenario: ScenarioResult,
    pub low_volatility_scenario: ScenarioResult,
    pub crisis_scenario: ScenarioResult,
}

/// 시나리오 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioResult {
    pub scenario_name: String,
    pub expected_return: f64,
    pub expected_volatility: f64,
    pub expected_sharpe: f64,
    pub expected_max_dd: f64,
    pub probability: f64,
}

/// 스트레스 테스트 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StressTestResults {
    pub historical_stress_tests: Vec<HistoricalStressTest>,
    pub synthetic_stress_tests: Vec<SyntheticStressTest>,
    pub worst_case_scenarios: Vec<WorstCaseScenario>,
}

/// 역사적 스트레스 테스트
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalStressTest {
    pub event_name: String,
    pub event_date: String,
    pub market_return: f64,
    pub strategy_return: f64,
    pub drawdown: f64,
    pub recovery_time: u32,
}

/// 합성 스트레스 테스트
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyntheticStressTest {
    pub test_name: String,
    pub shock_magnitude: f64,
    pub shock_duration: u32,
    pub strategy_return: f64,
    pub max_drawdown: f64,
}

/// 최악 시나리오
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorstCaseScenario {
    pub scenario_name: String,
    pub probability: f64,
    pub expected_return: f64,
    pub expected_drawdown: f64,
    pub var_99: f64,
}

/// Monte Carlo 분석기
pub struct MonteCarloAnalyzer {
    config: MonteCarloConfig,
    return_series: Vec<f64>,
    original_metrics: PerformanceMetrics,
}

impl MonteCarloAnalyzer {
    /// 새로운 Monte Carlo 분석기 생성
    pub fn new(config: MonteCarloConfig, backtest_result: &BacktestResult) -> Self {
        // 일일 수익률 시계열 추출
        let return_series: Vec<f64> = backtest_result.portfolio_history.windows(2)
            .map(|window| {
                let prev = window[0].total_value;
                let curr = window[1].total_value;
                (curr - prev) / prev
            })
            .collect();

        Self {
            config,
            return_series,
            original_metrics: backtest_result.performance.clone(),
        }
    }

    /// Monte Carlo 시뮬레이션 실행
    pub async fn run_simulation(&self) -> Result<MonteCarloResult> {
        // 랜덤 시드 설정
        if let Some(seed) = self.config.random_seed {
            // TODO: 시드 설정 구현
        }

        // 시뮬레이션 실행
        let simulation_results = self.run_bootstrap_simulations().await?;
        
        // 신뢰 구간 계산
        let confidence_intervals = self.calculate_confidence_intervals(&simulation_results).await?;
        
        // 통계적 검정
        let statistical_tests = self.perform_statistical_tests(&simulation_results).await?;
        
        // 시나리오 분석 (선택적)
        let scenario_analysis = if self.config.scenario_analysis {
            Some(self.perform_scenario_analysis().await?)
        } else {
            None
        };
        
        // 스트레스 테스트 (선택적)
        let stress_test_results = if self.config.stress_testing {
            Some(self.perform_stress_tests().await?)
        } else {
            None
        };
        Ok(MonteCarloResult {
            config: self.config.clone(),
            original_performance: self.original_metrics.clone(),
            simulation_results,
            confidence_intervals,
            statistical_tests,
            scenario_analysis,
            stress_test_results,
        })
    }

    /// 부트스트랩 시뮬레이션 실행
    async fn run_bootstrap_simulations(&self) -> Result<Vec<SimulationResult>> {
        let mut results = Vec::new();
        let mut rng = rand::thread_rng();

        for i in 0..self.config.num_simulations {
            let simulated_returns = match &self.config.bootstrap_method {
                BootstrapMethod::BlockBootstrap { block_size } => {
                    self.block_bootstrap(*block_size, &mut rng)
                }
                BootstrapMethod::StationaryBootstrap { block_size } => {
                    self.stationary_bootstrap(*block_size, &mut rng)
                }
                BootstrapMethod::CircularBootstrap { block_size } => {
                    self.circular_bootstrap(*block_size, &mut rng)
                }
                BootstrapMethod::ResidualBootstrap => {
                    self.residual_bootstrap(&mut rng)
                }
            };

            let metrics = self.calculate_metrics_from_returns(&simulated_returns);

            results.push(SimulationResult {
                simulation_id: i + 1,
                total_return: metrics.total_return,
                sharpe_ratio: metrics.sharpe_ratio,
                max_drawdown: metrics.max_drawdown,
                win_rate: metrics.win_rate,
                profit_factor: metrics.profit_factor,
                volatility: metrics.volatility,
                return_series: simulated_returns,
            });

            if (i + 1) % 1000 == 0 {
            }
        }

        Ok(results)
    }

    /// 블록 부트스트랩
    fn block_bootstrap(&self, block_size: usize, rng: &mut impl Rng) -> Vec<f64> {
        let mut bootstrapped_returns = Vec::new();
        let num_blocks = (self.return_series.len() + block_size - 1) / block_size;

        for _ in 0..num_blocks {
            let start_idx = rng.gen_range(0..=self.return_series.len().saturating_sub(block_size));
            let end_idx = (start_idx + block_size).min(self.return_series.len());
            
            for i in start_idx..end_idx {
                bootstrapped_returns.push(self.return_series[i]);
            }
        }

        bootstrapped_returns.truncate(self.return_series.len());
        bootstrapped_returns
    }

    /// 정상 부트스트랩
    fn stationary_bootstrap(&self, block_size: usize, rng: &mut impl Rng) -> Vec<f64> {
        let mut bootstrapped_returns = Vec::new();
        let mut current_idx = rng.gen_range(0..self.return_series.len());

        while bootstrapped_returns.len() < self.return_series.len() {
            let block_length = rng.gen_range(1..=block_size);
            
            for _ in 0..block_length {
                if bootstrapped_returns.len() >= self.return_series.len() {
                    break;
                }
                bootstrapped_returns.push(self.return_series[current_idx]);
                current_idx = (current_idx + 1) % self.return_series.len();
            }
        }

        bootstrapped_returns
    }

    /// 원형 부트스트랩
    fn circular_bootstrap(&self, block_size: usize, rng: &mut impl Rng) -> Vec<f64> {
        let mut bootstrapped_returns = Vec::new();
        let extended_series = self.return_series.iter().cycle().take(self.return_series.len() * 2).cloned().collect::<Vec<f64>>();

        while bootstrapped_returns.len() < self.return_series.len() {
            let start_idx = rng.gen_range(0..self.return_series.len());
            let end_idx = (start_idx + block_size).min(extended_series.len());
            
            for i in start_idx..end_idx {
                if bootstrapped_returns.len() >= self.return_series.len() {
                    break;
                }
                bootstrapped_returns.push(extended_series[i]);
            }
        }

        bootstrapped_returns
    }

    /// 잔차 부트스트랩
    fn residual_bootstrap(&self, rng: &mut impl Rng) -> Vec<f64> {
        // AR(1) 모델 피팅 후 잔차 부트스트랩
        let mean_return = self.return_series.iter().sum::<f64>() / self.return_series.len() as f64;
        let residuals: Vec<f64> = self.return_series.iter().map(|r| r - mean_return).collect();
        
        let mut bootstrapped_returns = Vec::new();
        let mut current_return = mean_return;
        
        for _ in 0..self.return_series.len() {
            let residual = residuals[rng.gen_range(0..residuals.len())];
            current_return = 0.1 * current_return + residual; // AR(1) 계수 0.1 가정
            bootstrapped_returns.push(current_return);
        }

        bootstrapped_returns
    }

    /// 수익률에서 성능 지표 계산
    fn calculate_metrics_from_returns(&self, returns: &[f64]) -> PerformanceMetrics {
        let total_return = returns.iter().fold(1.0, |acc, r| acc * (1.0 + r)) - 1.0;
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter().map(|r| (r - mean_return).powi(2)).sum::<f64>() / returns.len() as f64;
        let volatility = variance.sqrt() * (252.0_f64).sqrt();
        
        let annualized_return = (1.0 + total_return).powf(252.0 / returns.len() as f64) - 1.0;
        let sharpe_ratio = (annualized_return - 0.02) / volatility;
        
        // 최대 낙폭 계산
        let mut peak = 1.0;
        let mut max_drawdown = 0.0;
        let mut current_value = 1.0;
        
        for return_val in returns {
            current_value *= 1.0 + return_val;
            if current_value > peak {
                peak = current_value;
            }
            let drawdown = (peak - current_value) / peak;
            if drawdown > max_drawdown {
                max_drawdown = drawdown;
            }
        }
        
        // 승률 계산
        let positive_returns = returns.iter().filter(|&&r| r > 0.0).count();
        let win_rate = positive_returns as f64 / returns.len() as f64;
        
        // 수익 팩터 계산
        let positive_sum: f64 = returns.iter().filter(|&&r| r > 0.0).sum();
        let negative_sum: f64 = returns.iter().filter(|&&r| r < 0.0).map(|r| r.abs()).sum();
        let profit_factor = if negative_sum > 0.0 { positive_sum / negative_sum } else { 0.0 };
        
        PerformanceMetrics {
            total_return,
            annualized_return,
            volatility,
            sharpe_ratio,
            sortino_ratio: 0.0, // TODO: Sortino ratio 계산
            calmar_ratio: if max_drawdown > 0.0 { annualized_return / max_drawdown } else { 0.0 },
            information_ratio: 0.0,
            max_drawdown,
            max_drawdown_duration: 0, // TODO: 낙폭 지속 기간 계산
            win_rate,
            profit_factor,
            average_trade_return: mean_return,
            average_win: positive_sum / positive_returns.max(1) as f64,
            average_loss: negative_sum / (returns.len() - positive_returns).max(1) as f64,
        }
    }

    /// 신뢰 구간 계산
    async fn calculate_confidence_intervals(&self, results: &[SimulationResult]) -> Result<HashMap<String, ConfidenceInterval>> {
        let mut intervals = HashMap::new();
        
        for confidence_level in &self.config.confidence_levels {
            let alpha = 1.0 - confidence_level;
            let lower_percentile = alpha / 2.0;
            let upper_percentile = 1.0 - alpha / 2.0;
            
            // Sharpe Ratio 신뢰 구간
            let mut sharpe_ratios: Vec<f64> = results.iter().map(|r| r.sharpe_ratio).collect();
            sharpe_ratios.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let sharpe_lower = sharpe_ratios[(lower_percentile * results.len() as f64) as usize];
            let sharpe_upper = sharpe_ratios[(upper_percentile * results.len() as f64) as usize];
            
            intervals.insert(
                format!("sharpe_ratio_{}", (confidence_level * 100.0) as u32),
                ConfidenceInterval {
                    metric: "Sharpe Ratio".to_string(),
                    lower_bound: sharpe_lower,
                    upper_bound: sharpe_upper,
                    confidence_level: *confidence_level,
                    original_value: self.original_metrics.sharpe_ratio,
                    p_value: self.calculate_p_value(&sharpe_ratios, self.original_metrics.sharpe_ratio),
                }
            );
            
            // Total Return 신뢰 구간
            let mut total_returns: Vec<f64> = results.iter().map(|r| r.total_return).collect();
            total_returns.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let return_lower = total_returns[(lower_percentile * results.len() as f64) as usize];
            let return_upper = total_returns[(upper_percentile * results.len() as f64) as usize];
            
            intervals.insert(
                format!("total_return_{}", (confidence_level * 100.0) as u32),
                ConfidenceInterval {
                    metric: "Total Return".to_string(),
                    lower_bound: return_lower,
                    upper_bound: return_upper,
                    confidence_level: *confidence_level,
                    original_value: self.original_metrics.total_return,
                    p_value: self.calculate_p_value(&total_returns, self.original_metrics.total_return),
                }
            );
            
            // Max Drawdown 신뢰 구간
            let mut max_drawdowns: Vec<f64> = results.iter().map(|r| r.max_drawdown).collect();
            max_drawdowns.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let dd_lower = max_drawdowns[(lower_percentile * results.len() as f64) as usize];
            let dd_upper = max_drawdowns[(upper_percentile * results.len() as f64) as usize];
            
            intervals.insert(
                format!("max_drawdown_{}", (confidence_level * 100.0) as u32),
                ConfidenceInterval {
                    metric: "Max Drawdown".to_string(),
                    lower_bound: dd_lower,
                    upper_bound: dd_upper,
                    confidence_level: *confidence_level,
                    original_value: self.original_metrics.max_drawdown,
                    p_value: self.calculate_p_value(&max_drawdowns, self.original_metrics.max_drawdown),
                }
            );
        }
        
        Ok(intervals)
    }

    /// P-value 계산
    fn calculate_p_value(&self, distribution: &[f64], observed_value: f64) -> f64 {
        let count_above = distribution.iter().filter(|&&x| x >= observed_value).count();
        count_above as f64 / distribution.len() as f64
    }

    /// 통계적 검정 수행
    async fn perform_statistical_tests(&self, results: &[SimulationResult]) -> Result<StatisticalTests> {
        let sharpe_ratios: Vec<f64> = results.iter().map(|r| r.sharpe_ratio).collect();
        let max_drawdowns: Vec<f64> = results.iter().map(|r| r.max_drawdown).collect();
        let total_returns: Vec<f64> = results.iter().map(|r| r.total_return).collect();

        // Sharpe Ratio 검정
        let sharpe_p_value = self.calculate_p_value(&sharpe_ratios, self.original_metrics.sharpe_ratio);
        let sharpe_test = SharpeRatioTest {
            original_sharpe: self.original_metrics.sharpe_ratio,
            p_value: sharpe_p_value,
            is_significant: sharpe_p_value < 0.05,
            confidence_interval: (sharpe_ratios[25], sharpe_ratios[975]), // 95% CI
        };

        // Max Drawdown 검정
        let dd_p_value = self.calculate_p_value(&max_drawdowns, self.original_metrics.max_drawdown);
        let dd_test = MaxDrawdownTest {
            original_max_dd: self.original_metrics.max_drawdown,
            p_value: dd_p_value,
            is_significant: dd_p_value < 0.05,
            confidence_interval: (max_drawdowns[25], max_drawdowns[975]),
        };

        // Return 유의성 검정
        let return_p_value = self.calculate_p_value(&total_returns, self.original_metrics.total_return);
        let return_test = ReturnSignificanceTest {
            original_return: self.original_metrics.total_return,
            p_value: return_p_value,
            is_significant: return_p_value < 0.05,
            confidence_interval: (total_returns[25], total_returns[975]),
        };

        // 안정성 검정
        let stability_test = StabilityTest {
            consistency_score: self.calculate_consistency_score(results),
            p_value: 0.0, // TODO: 안정성 P-value 계산
            is_stable: true, // TODO: 안정성 판정 로직
        };

        Ok(StatisticalTests {
            sharpe_ratio_test: sharpe_test,
            max_drawdown_test: dd_test,
            return_significance_test: return_test,
            stability_test,
        })
    }

    /// 일관성 점수 계산
    fn calculate_consistency_score(&self, results: &[SimulationResult]) -> f64 {
        let returns: Vec<f64> = results.iter().map(|r| r.total_return).collect();
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter().map(|r| (r - mean_return).powi(2)).sum::<f64>() / returns.len() as f64;
        
        // 낮은 분산 = 높은 일관성
        1.0 / (1.0 + variance)
    }

    /// 시나리오 분석 수행
    async fn perform_scenario_analysis(&self) -> Result<ScenarioAnalysis> {
        // 다양한 시장 상황 시뮬레이션
        let bull_market = self.simulate_scenario(0.15, 0.20).await?; // 15% 수익률, 20% 변동성
        let bear_market = self.simulate_scenario(-0.10, 0.25).await?; // -10% 수익률, 25% 변동성
        let high_vol = self.simulate_scenario(0.08, 0.35).await?; // 8% 수익률, 35% 변동성
        let low_vol = self.simulate_scenario(0.12, 0.15).await?; // 12% 수익률, 15% 변동성
        let crisis = self.simulate_scenario(-0.20, 0.40).await?; // -20% 수익률, 40% 변동성

        Ok(ScenarioAnalysis {
            bull_market_scenario: bull_market,
            bear_market_scenario: bear_market,
            high_volatility_scenario: high_vol,
            low_volatility_scenario: low_vol,
            crisis_scenario: crisis,
        })
    }

    /// 시나리오 시뮬레이션
    async fn simulate_scenario(&self, expected_return: f64, volatility: f64) -> Result<ScenarioResult> {
        // 정규분포 기반 시뮬레이션
        let mut rng = rand::thread_rng();
        let num_days = self.return_series.len();
        
        let mut simulated_returns = Vec::new();
        for _ in 0..num_days {
            let daily_return = expected_return / 252.0 + volatility / (252.0_f64).sqrt() * rng.sample::<f64, _>(rand_distr::StandardNormal);
            simulated_returns.push(daily_return);
        }

        let metrics = self.calculate_metrics_from_returns(&simulated_returns);

        Ok(ScenarioResult {
            scenario_name: format!("Return: {:.1}%, Vol: {:.1}%", expected_return * 100.0, volatility * 100.0),
            expected_return: metrics.total_return,
            expected_volatility: metrics.volatility,
            expected_sharpe: metrics.sharpe_ratio,
            expected_max_dd: metrics.max_drawdown,
            probability: 0.2, // TODO: 실제 확률 계산
        })
    }

    /// 스트레스 테스트 수행
    async fn perform_stress_tests(&self) -> Result<StressTestResults> {
        let historical_tests = self.perform_historical_stress_tests().await?;
        let synthetic_tests = self.perform_synthetic_stress_tests().await?;
        let worst_case = self.identify_worst_case_scenarios().await?;

        Ok(StressTestResults {
            historical_stress_tests: historical_tests,
            synthetic_stress_tests: synthetic_tests,
            worst_case_scenarios: worst_case,
        })
    }

    /// 역사적 스트레스 테스트
    async fn perform_historical_stress_tests(&self) -> Result<Vec<HistoricalStressTest>> {
        // 주요 금융 위기 이벤트들
        let crisis_events = vec![
            ("2008 Financial Crisis", "2008-09-15", -0.50),
            ("COVID-19 Crash", "2020-03-16", -0.35),
            ("Dot-com Bubble", "2000-03-10", -0.30),
            ("Black Monday", "1987-10-19", -0.23),
        ];

        let mut results = Vec::new();
        for (name, date, market_return) in crisis_events {
            // 전략의 위기 대응 능력 시뮬레이션
            let strategy_return = self.simulate_crisis_response(market_return).await?;
            let drawdown = (-strategy_return).max(0.0);
            
            results.push(HistoricalStressTest {
                event_name: name.to_string(),
                event_date: date.to_string(),
                market_return,
                strategy_return,
                drawdown,
                recovery_time: self.estimate_recovery_time(drawdown),
            });
        }

        Ok(results)
    }

    /// 위기 대응 시뮬레이션
    async fn simulate_crisis_response(&self, market_shock: f64) -> Result<f64> {
        // 전략의 베타와 알파를 고려한 위기 대응 모델링
        let beta = 1.0; // TODO: 실제 베타 계산
        let alpha = 0.02; // TODO: 실제 알파 계산
        
        let strategy_return = alpha + beta * market_shock + rand::thread_rng().sample::<f64, _>(rand_distr::StandardNormal) * 0.05;
        Ok(strategy_return)
    }

    /// 회복 시간 추정
    fn estimate_recovery_time(&self, drawdown: f64) -> u32 {
        // 회복 시간은 낙폭에 비례
        (drawdown * 100.0) as u32
    }

    /// 합성 스트레스 테스트
    async fn perform_synthetic_stress_tests(&self) -> Result<Vec<SyntheticStressTest>> {
        let mut results = Vec::new();
        
        // 다양한 충격 크기와 지속 기간 테스트
        let shock_magnitudes = vec![0.05, 0.10, 0.15, 0.20, 0.30];
        let shock_durations = vec![1, 5, 10, 20, 50];

        for magnitude in shock_magnitudes {
            for duration in &shock_durations {
                let strategy_return = self.simulate_shock_response(magnitude, *duration).await?;
                let max_drawdown = (-strategy_return).max(0.0);

                results.push(SyntheticStressTest {
                    test_name: format!("{}% shock for {} days", magnitude * 100.0, duration),
                    shock_magnitude: magnitude,
                    shock_duration: *duration,
                    strategy_return,
                    max_drawdown,
                });
            }
        }

        Ok(results)
    }

    /// 충격 대응 시뮬레이션
    async fn simulate_shock_response(&self, magnitude: f64, duration: u32) -> Result<f64> {
        let mut cumulative_return = 0.0;
        
        for day in 0..duration {
            let daily_shock = magnitude / duration as f64;
            let daily_return = daily_shock + rand::thread_rng().sample::<f64, _>(rand_distr::StandardNormal) * 0.02;
            cumulative_return += daily_return;
        }

        Ok(cumulative_return)
    }

    /// 최악 시나리오 식별
    async fn identify_worst_case_scenarios(&self) -> Result<Vec<WorstCaseScenario>> {
        let mut scenarios = Vec::new();

        // 꼬리 위험 시나리오들
        scenarios.push(WorstCaseScenario {
            scenario_name: "1% Tail Risk".to_string(),
            probability: 0.01,
            expected_return: -0.25,
            expected_drawdown: 0.35,
            var_99: -0.30,
        });

        scenarios.push(WorstCaseScenario {
            scenario_name: "Black Swan Event".to_string(),
            probability: 0.001,
            expected_return: -0.50,
            expected_drawdown: 0.60,
            var_99: -0.70,
        });

        Ok(scenarios)
    }
}

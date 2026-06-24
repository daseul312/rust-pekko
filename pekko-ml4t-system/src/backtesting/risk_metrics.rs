//! 위험 메트릭스 계산
//! 
//! 다양한 위험 지표들을 계산합니다.

use super::{BacktestResult, RiskMetrics};
use std::collections::HashMap;
use anyhow::Result;
use tracing::{ debug};

/// 위험 메트릭스 계산기
pub struct RiskMetricsCalculator {
    return_series: Vec<f64>,
    benchmark_returns: Option<Vec<f64>>,
}

impl RiskMetricsCalculator {
    /// 새로운 위험 메트릭스 계산기 생성
    pub fn new(backtest_result: &BacktestResult) -> Self {
        // 일일 수익률 시계열 추출
        let return_series: Vec<f64> = backtest_result.portfolio_history.windows(2)
            .map(|window| {
                let prev = window[0].total_value;
                let curr = window[1].total_value;
                (curr - prev) / prev
            })
            .collect();

        Self {
            return_series,
            benchmark_returns: None, // TODO: 벤치마크 수익률 로드
        }
    }

    /// 벤치마크 수익률 설정
    pub fn set_benchmark_returns(&mut self, benchmark_returns: Vec<f64>) {
        self.benchmark_returns = Some(benchmark_returns);
    }

    /// 모든 위험 메트릭스 계산
    pub fn calculate_all_metrics(&self) -> Result<RiskMetrics> {
        let var_metrics = self.calculate_var_metrics()?;
        let volatility_metrics = self.calculate_volatility_metrics()?;
        let benchmark_metrics = self.calculate_benchmark_metrics()?;
        let distribution_metrics = self.calculate_distribution_metrics()?;

        Ok(RiskMetrics {
            var_95: var_metrics.var_95,
            var_99: var_metrics.var_99,
            cvar_95: var_metrics.cvar_95,
            cvar_99: var_metrics.cvar_99,
            beta: benchmark_metrics.beta,
            alpha: benchmark_metrics.alpha,
            tracking_error: benchmark_metrics.tracking_error,
            downside_deviation: volatility_metrics.downside_deviation,
            skewness: distribution_metrics.skewness,
            kurtosis: distribution_metrics.kurtosis,
        })
    }

    /// VaR 메트릭스 계산
    fn calculate_var_metrics(&self) -> Result<VaRMetrics> {
        let mut sorted_returns = self.return_series.clone();
        sorted_returns.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let var_95_index = (sorted_returns.len() as f64 * 0.05) as usize;
        let var_99_index = (sorted_returns.len() as f64 * 0.01) as usize;

        let var_95 = if var_95_index < sorted_returns.len() {
            sorted_returns[var_95_index]
        } else {
            0.0
        };

        let var_99 = if var_99_index < sorted_returns.len() {
            sorted_returns[var_99_index]
        } else {
            0.0
        };

        // CVaR (Expected Shortfall) 계산
        let cvar_95 = if var_95_index > 0 {
            sorted_returns[..var_95_index].iter().sum::<f64>() / var_95_index as f64
        } else {
            0.0
        };

        let cvar_99 = if var_99_index > 0 {
            sorted_returns[..var_99_index].iter().sum::<f64>() / var_99_index as f64
        } else {
            0.0
        };

        Ok(VaRMetrics {
            var_95,
            var_99,
            cvar_95,
            cvar_99,
        })
    }

    /// 변동성 메트릭스 계산
    fn calculate_volatility_metrics(&self) -> Result<VolatilityMetrics> {
        let mean_return = self.return_series.iter().sum::<f64>() / self.return_series.len() as f64;
        
        // 전체 변동성
        let variance = self.return_series.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / self.return_series.len() as f64;
        let volatility = variance.sqrt() * (252.0_f64).sqrt(); // 연환산

        // 하방 변동성 (Downside Deviation)
        let downside_returns: Vec<f64> = self.return_series.iter()
            .filter(|&&r| r < 0.0)
            .cloned()
            .collect();
        
        let downside_variance = if !downside_returns.is_empty() {
            downside_returns.iter().map(|r| r.powi(2)).sum::<f64>() / downside_returns.len() as f64
        } else {
            0.0
        };
        let downside_deviation = downside_variance.sqrt() * (252.0_f64).sqrt();

        Ok(VolatilityMetrics {
            volatility,
            downside_deviation,
        })
    }

    /// 벤치마크 메트릭스 계산
    fn calculate_benchmark_metrics(&self) -> Result<BenchmarkMetrics> {
        if let Some(benchmark_returns) = &self.benchmark_returns {
            if benchmark_returns.len() != self.return_series.len() {
                return Err(anyhow::anyhow!("벤치마크 수익률 길이가 일치하지 않습니다"));
            }

            // 베타 계산
            let portfolio_mean = self.return_series.iter().sum::<f64>() / self.return_series.len() as f64;
            let benchmark_mean = benchmark_returns.iter().sum::<f64>() / benchmark_returns.len() as f64;

            let covariance = self.return_series.iter()
                .zip(benchmark_returns.iter())
                .map(|(r, b)| (r - portfolio_mean) * (b - benchmark_mean))
                .sum::<f64>() / self.return_series.len() as f64;

            let benchmark_variance = benchmark_returns.iter()
                .map(|b| (b - benchmark_mean).powi(2))
                .sum::<f64>() / benchmark_returns.len() as f64;

            let beta = if benchmark_variance > 0.0 {
                covariance / benchmark_variance
            } else {
                1.0
            };

            // 알파 계산
            let portfolio_annual_return = (1.0 + portfolio_mean).powi(252) - 1.0;
            let benchmark_annual_return = (1.0 + benchmark_mean).powi(252) - 1.0;
            let alpha = portfolio_annual_return - (0.02 + beta * (benchmark_annual_return - 0.02)); // CAPM

            // 추적 오차 계산
            let tracking_error = self.return_series.iter()
                .zip(benchmark_returns.iter())
                .map(|(r, b)| r - b)
                .map(|diff| diff.powi(2))
                .sum::<f64>() / self.return_series.len() as f64;
            let tracking_error = tracking_error.sqrt() * (252.0_f64).sqrt();

            Ok(BenchmarkMetrics {
                beta,
                alpha,
                tracking_error,
            })
        } else {
            // 벤치마크가 없으면 기본값
            Ok(BenchmarkMetrics {
                beta: 1.0,
                alpha: 0.0,
                tracking_error: 0.0,
            })
        }
    }

    /// 분포 메트릭스 계산
    fn calculate_distribution_metrics(&self) -> Result<DistributionMetrics> {
        let mean_return = self.return_series.iter().sum::<f64>() / self.return_series.len() as f64;
        let variance = self.return_series.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / self.return_series.len() as f64;
        let std_dev = variance.sqrt();

        // 왜도 (Skewness) 계산
        let skewness = if std_dev > 0.0 {
            self.return_series.iter()
                .map(|r| ((r - mean_return) / std_dev).powi(3))
                .sum::<f64>() / self.return_series.len() as f64
        } else {
            0.0
        };

        // 첨도 (Kurtosis) 계산
        let kurtosis = if std_dev > 0.0 {
            self.return_series.iter()
                .map(|r| ((r - mean_return) / std_dev).powi(4))
                .sum::<f64>() / self.return_series.len() as f64 - 3.0 // Excess kurtosis
        } else {
            0.0
        };

        Ok(DistributionMetrics {
            skewness,
            kurtosis,
        })
    }

    /// 조건부 VaR 계산 (Monte Carlo 방법)
    pub fn calculate_conditional_var(&self, confidence_level: f64, num_simulations: u32) -> Result<f64> {
        use rand::Rng;
        use rand_distr::{Distribution, Normal};

        let mean_return = self.return_series.iter().sum::<f64>() / self.return_series.len() as f64;
        let variance = self.return_series.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / self.return_series.len() as f64;
        let std_dev = variance.sqrt();

        let mut rng = rand::thread_rng();
        let normal = Normal::new(mean_return, std_dev)?;
        
        let mut simulated_returns = Vec::new();
        for _ in 0..num_simulations {
            simulated_returns.push(normal.sample(&mut rng));
        }

        simulated_returns.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let var_index = ((1.0 - confidence_level) * num_simulations as f64) as usize;
        
        Ok(simulated_returns[var_index])
    }

    /// 꼬리 위험 (Tail Risk) 분석
    pub fn analyze_tail_risk(&self) -> Result<TailRiskAnalysis> {
        let var_95 = self.calculate_var_metrics()?.var_95;
        let var_99 = self.calculate_var_metrics()?.var_99;
        
        // 극값 이론 기반 꼬리 위험 측정
        let extreme_returns: Vec<f64> = self.return_series.iter()
            .filter(|&&r| r < var_95) // 5% 꼬리
            .cloned()
            .collect();

        let tail_mean = if !extreme_returns.is_empty() {
            extreme_returns.iter().sum::<f64>() / extreme_returns.len() as f64
        } else {
            0.0
        };

        let tail_volatility = if extreme_returns.len() > 1 {
            let tail_variance = extreme_returns.iter()
                .map(|r| (r - tail_mean).powi(2))
                .sum::<f64>() / extreme_returns.len() as f64;
            tail_variance.sqrt()
        } else {
            0.0
        };

        Ok(TailRiskAnalysis {
            var_95,
            var_99,
            tail_mean,
            tail_volatility,
            extreme_events: extreme_returns.len(),
            tail_ratio: extreme_returns.len() as f64 / self.return_series.len() as f64,
        })
    }

    /// 압축 위험 (Compression Risk) 분석
    pub fn analyze_compression_risk(&self) -> Result<CompressionRiskAnalysis> {
        // 변동성 압축 기간 식별
        let window_size = 20; // 20일 윈도우
        let mut volatility_windows = Vec::new();
        
        for i in window_size..self.return_series.len() {
            let window_returns = &self.return_series[i-window_size..i];
            let window_mean = window_returns.iter().sum::<f64>() / window_size as f64;
            let window_variance = window_returns.iter()
                .map(|r| (r - window_mean).powi(2))
                .sum::<f64>() / window_size as f64;
            let window_volatility = window_variance.sqrt();
            volatility_windows.push(window_volatility);
        }

        let mean_volatility = volatility_windows.iter().sum::<f64>() / volatility_windows.len() as f64;
        let low_vol_periods = volatility_windows.iter()
            .filter(|&&v| v < mean_volatility * 0.5)
            .count();

        Ok(CompressionRiskAnalysis {
            mean_volatility,
            low_volatility_periods: low_vol_periods,
            compression_ratio: low_vol_periods as f64 / volatility_windows.len() as f64,
            volatility_clustering: self.detect_volatility_clustering(&volatility_windows),
        })
    }

    /// 변동성 클러스터링 감지
    fn detect_volatility_clustering(&self, volatility_windows: &[f64]) -> bool {
        // ARCH 효과 검정 (간단한 버전)
        let mean_vol = volatility_windows.iter().sum::<f64>() / volatility_windows.len() as f64;
        let high_vol_periods = volatility_windows.iter()
            .filter(|&&v| v > mean_vol)
            .count();

        high_vol_periods as f64 / volatility_windows.len() as f64 > 0.3
    }
}

/// VaR 메트릭스
#[derive(Debug, Clone)]
struct VaRMetrics {
    var_95: f64,
    var_99: f64,
    cvar_95: f64,
    cvar_99: f64,
}

/// 변동성 메트릭스
#[derive(Debug, Clone)]
struct VolatilityMetrics {
    volatility: f64,
    downside_deviation: f64,
}

/// 벤치마크 메트릭스
#[derive(Debug, Clone)]
struct BenchmarkMetrics {
    beta: f64,
    alpha: f64,
    tracking_error: f64,
}

/// 분포 메트릭스
#[derive(Debug, Clone)]
struct DistributionMetrics {
    skewness: f64,
    kurtosis: f64,
}

/// 꼬리 위험 분석
#[derive(Debug, Clone)]
pub struct TailRiskAnalysis {
    pub var_95: f64,
    pub var_99: f64,
    pub tail_mean: f64,
    pub tail_volatility: f64,
    pub extreme_events: usize,
    pub tail_ratio: f64,
}

/// 압축 위험 분석
#[derive(Debug, Clone)]
pub struct CompressionRiskAnalysis {
    pub mean_volatility: f64,
    pub low_volatility_periods: usize,
    pub compression_ratio: f64,
    pub volatility_clustering: bool,
}

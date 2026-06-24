//! 성능 분석 및 시각화
//! 
//! 백테스팅 결과의 성능을 분석하고 시각화합니다.

use super::{BacktestResult, PerformanceMetrics, RiskMetrics, Trade, PortfolioSnapshot};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use anyhow::Result;
use tracing::{ debug};

/// 성능 분석기
pub struct PerformanceAnalyzer {
    result: BacktestResult,
}

/// 성능 리포트
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceReport {
    pub summary: PerformanceSummary,
    pub monthly_returns: HashMap<String, f64>, // YYYY-MM -> 수익률
    pub drawdown_analysis: DrawdownAnalysis,
    pub trade_analysis: TradeAnalysis,
    pub risk_analysis: RiskAnalysis,
    pub benchmark_analysis: Option<BenchmarkAnalysis>,
}

/// 성능 요약
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceSummary {
    pub total_return: f64,
    pub annualized_return: f64,
    pub volatility: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub total_trades: u32,
    pub best_trade: f64,
    pub worst_trade: f64,
    pub average_win: f64,
    pub average_loss: f64,
}

/// 낙폭 분석
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrawdownAnalysis {
    pub max_drawdown: f64,
    pub max_drawdown_duration: u32,
    pub current_drawdown: f64,
    pub drawdown_periods: Vec<DrawdownPeriod>,
    pub recovery_analysis: RecoveryAnalysis,
}

/// 낙폭 기간
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrawdownPeriod {
    pub start_date: String,
    pub end_date: String,
    pub peak_value: f64,
    pub trough_value: f64,
    pub drawdown_pct: f64,
    pub duration_days: u32,
}

/// 회복 분석
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryAnalysis {
    pub average_recovery_time: f64,
    pub max_recovery_time: u32,
    pub recovery_success_rate: f64,
}

/// 거래 분석
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeAnalysis {
    pub total_trades: u32,
    pub winning_trades: u32,
    pub losing_trades: u32,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub average_trade_return: f64,
    pub average_win: f64,
    pub average_loss: f64,
    pub best_trade: f64,
    pub worst_trade: f64,
    pub largest_win: f64,
    pub largest_loss: f64,
    pub consecutive_wins: u32,
    pub consecutive_losses: u32,
    pub trade_distribution: TradeDistribution,
}

/// 거래 분포
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeDistribution {
    pub return_buckets: HashMap<String, u32>, // 수익률 구간별 거래 수
    pub holding_period_distribution: HashMap<String, u32>, // 보유 기간별 분포
    pub monthly_trade_count: HashMap<String, u32>, // 월별 거래 수
}

/// 위험 분석
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskAnalysis {
    pub var_95: f64,
    pub var_99: f64,
    pub cvar_95: f64,
    pub cvar_99: f64,
    pub volatility: f64,
    pub downside_deviation: f64,
    pub skewness: f64,
    pub kurtosis: f64,
    pub beta: f64,
    pub alpha: f64,
    pub tracking_error: f64,
    pub information_ratio: f64,
}

/// 벤치마크 분석
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkAnalysis {
    pub benchmark_symbol: String,
    pub excess_return: f64,
    pub tracking_error: f64,
    pub information_ratio: f64,
    pub beta: f64,
    pub alpha: f64,
    pub correlation: f64,
    pub rolling_correlation: Vec<RollingCorrelation>,
}

/// 롤링 상관관계
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollingCorrelation {
    pub date: String,
    pub correlation: f64,
}

impl PerformanceAnalyzer {
    /// 새로운 성능 분석기 생성
    pub fn new(result: BacktestResult) -> Self {
        Self { result }
    }

    /// 종합 성능 리포트 생성
    pub fn generate_report(&self) -> Result<PerformanceReport> {
        let summary = self.calculate_summary()?;
        let monthly_returns = self.calculate_monthly_returns()?;
        let drawdown_analysis = self.analyze_drawdowns()?;
        let trade_analysis = self.analyze_trades()?;
        let risk_analysis = self.analyze_risks()?;
        let benchmark_analysis = if self.result.benchmark_comparison.is_some() {
            Some(self.analyze_benchmark()?)
        } else {
            None
        };

        Ok(PerformanceReport {
            summary,
            monthly_returns,
            drawdown_analysis,
            trade_analysis,
            risk_analysis,
            benchmark_analysis,
        })
    }

    /// 성능 요약 계산
    fn calculate_summary(&self) -> Result<PerformanceSummary> {
        let perf = &self.result.performance;
        let exec = &self.result.execution_summary;

        // 최고/최악 거래 찾기
        let completed_trades: Vec<&Trade> = self.result.trades.iter()
            .filter(|t| t.pnl.is_some())
            .collect();

        let best_trade = completed_trades.iter()
            .map(|t| t.pnl.unwrap())
            .fold(f64::NEG_INFINITY, f64::max);

        let worst_trade = completed_trades.iter()
            .map(|t| t.pnl.unwrap())
            .fold(f64::INFINITY, f64::min);

        Ok(PerformanceSummary {
            total_return: perf.total_return,
            annualized_return: perf.annualized_return,
            volatility: perf.volatility,
            sharpe_ratio: perf.sharpe_ratio,
            max_drawdown: perf.max_drawdown,
            win_rate: perf.win_rate,
            profit_factor: perf.profit_factor,
            total_trades: exec.total_trades,
            best_trade,
            worst_trade,
            average_win: perf.average_win,
            average_loss: perf.average_loss,
        })
    }

    /// 월별 수익률 계산
    fn calculate_monthly_returns(&self) -> Result<HashMap<String, f64>> {
        let mut monthly_returns = HashMap::new();
        
        // 포트폴리오 히스토리에서 월별 수익률 계산
        let mut current_month: Option<String> = None;
        let mut month_start_value = None;
        
        for snapshot in &self.result.portfolio_history {
            let month_key = snapshot.timestamp.format("%Y-%m").to_string();
            
            if current_month != Some(month_key.clone()) {
                // 새로운 월 시작
                if let Some(start_value) = month_start_value {
                    // 이전 월 수익률 계산
                    if let Some(prev_snapshot) = self.result.portfolio_history.iter()
                        .rev()
                        .find(|s| s.timestamp.format("%Y-%m").to_string() != month_key) {
                        let monthly_return = (prev_snapshot.total_value - start_value) / start_value;
                        monthly_returns.insert(current_month.as_ref().unwrap().clone(), monthly_return);
                    }
                }
                
                current_month = Some(month_key.clone());
                month_start_value = Some(snapshot.total_value);
            }
        }
        
        Ok(monthly_returns)
    }

    /// 낙폭 분석
    fn analyze_drawdowns(&self) -> Result<DrawdownAnalysis> {
        let mut drawdown_periods = Vec::new();
        let mut current_drawdown_start = None;
        let mut peak_value = 0.0;
        let mut max_drawdown = 0.0;
        let mut max_drawdown_duration = 0;
        let mut current_drawdown_duration = 0;

        for snapshot in &self.result.portfolio_history {
            if snapshot.total_value > peak_value {
                peak_value = snapshot.total_value;
                
                // 새로운 최고점 - 이전 낙폭 기간 종료
                if let Some(start_date) = current_drawdown_start {
                    if current_drawdown_duration > 0 {
                        drawdown_periods.push(DrawdownPeriod {
                            start_date: start_date,
                            end_date: snapshot.timestamp.format("%Y-%m-%d").to_string(),
                            peak_value,
                            trough_value: snapshot.total_value,
                            drawdown_pct: max_drawdown,
                            duration_days: current_drawdown_duration,
                        });
                    }
                }
                
                current_drawdown_start = None;
                current_drawdown_duration = 0;
                max_drawdown = 0.0;
            } else {
                // 낙폭 지속
                let drawdown = (peak_value - snapshot.total_value) / peak_value;
                if drawdown > max_drawdown {
                    max_drawdown = drawdown;
                }
                
                if current_drawdown_start.is_none() {
                    current_drawdown_start = Some(snapshot.timestamp.format("%Y-%m-%d").to_string());
                }
                
                current_drawdown_duration += 1;
                if current_drawdown_duration > max_drawdown_duration {
                    max_drawdown_duration = current_drawdown_duration;
                }
            }
        }

        // 현재 낙폭
        let current_drawdown = if !self.result.portfolio_history.is_empty() {
            let latest = &self.result.portfolio_history[self.result.portfolio_history.len() - 1];
            (peak_value - latest.total_value) / peak_value
        } else {
            0.0
        };

        // 회복 분석
        let recovery_analysis = self.analyze_recovery(&drawdown_periods)?;

        Ok(DrawdownAnalysis {
            max_drawdown: self.result.performance.max_drawdown,
            max_drawdown_duration,
            current_drawdown,
            drawdown_periods,
            recovery_analysis,
        })
    }

    /// 회복 분석
    fn analyze_recovery(&self, drawdown_periods: &[DrawdownPeriod]) -> Result<RecoveryAnalysis> {
        let mut recovery_times = Vec::new();
        let mut successful_recoveries = 0;

        for period in drawdown_periods {
            if period.duration_days > 0 {
                recovery_times.push(period.duration_days as f64);
                successful_recoveries += 1;
            }
        }

        let average_recovery_time = if !recovery_times.is_empty() {
            recovery_times.iter().sum::<f64>() / recovery_times.len() as f64
        } else {
            0.0
        };

        let max_recovery_time = recovery_times.iter()
            .fold(0.0_f64, |max, &time| max.max(time)) as u32;

        let recovery_success_rate = if !drawdown_periods.is_empty() {
            successful_recoveries as f64 / drawdown_periods.len() as f64
        } else {
            0.0
        };

        Ok(RecoveryAnalysis {
            average_recovery_time,
            max_recovery_time,
            recovery_success_rate,
        })
    }

    /// 거래 분석
    fn analyze_trades(&self) -> Result<TradeAnalysis> {
        let completed_trades: Vec<&Trade> = self.result.trades.iter()
            .filter(|t| t.pnl.is_some())
            .collect();

        let winning_trades = completed_trades.iter()
            .filter(|t| t.pnl.unwrap() > 0.0)
            .count();

        let losing_trades = completed_trades.iter()
            .filter(|t| t.pnl.unwrap() < 0.0)
            .count();

        let win_rate = if !completed_trades.is_empty() {
            winning_trades as f64 / completed_trades.len() as f64
        } else {
            0.0
        };

        let total_pnl: f64 = completed_trades.iter()
            .map(|t| t.pnl.unwrap())
            .sum();

        let winning_pnl: f64 = completed_trades.iter()
            .filter(|t| t.pnl.unwrap() > 0.0)
            .map(|t| t.pnl.unwrap())
            .sum();

        let losing_pnl: f64 = completed_trades.iter()
            .filter(|t| t.pnl.unwrap() < 0.0)
            .map(|t| t.pnl.unwrap().abs())
            .sum();

        let profit_factor = if losing_pnl > 0.0 {
            winning_pnl / losing_pnl
        } else {
            0.0
        };

        let average_trade_return = if !completed_trades.is_empty() {
            total_pnl / completed_trades.len() as f64
        } else {
            0.0
        };

        let average_win = if winning_trades > 0 {
            winning_pnl / winning_trades as f64
        } else {
            0.0
        };

        let average_loss = if losing_trades > 0 {
            losing_pnl / losing_trades as f64
        } else {
            0.0
        };

        let best_trade = completed_trades.iter()
            .map(|t| t.pnl.unwrap())
            .fold(f64::NEG_INFINITY, f64::max);

        let worst_trade = completed_trades.iter()
            .map(|t| t.pnl.unwrap())
            .fold(f64::INFINITY, f64::min);

        // 연속 승/패 계산
        let (consecutive_wins, consecutive_losses) = self.calculate_consecutive_trades(&completed_trades);

        // 거래 분포 분석
        let trade_distribution = self.analyze_trade_distribution(&completed_trades)?;

        Ok(TradeAnalysis {
            total_trades: completed_trades.len() as u32,
            winning_trades: winning_trades as u32,
            losing_trades: losing_trades as u32,
            win_rate,
            profit_factor,
            average_trade_return,
            average_win,
            average_loss,
            best_trade,
            worst_trade,
            largest_win: best_trade,
            largest_loss: worst_trade,
            consecutive_wins,
            consecutive_losses,
            trade_distribution,
        })
    }

    /// 연속 승/패 계산
    fn calculate_consecutive_trades(&self, trades: &[&Trade]) -> (u32, u32) {
        let mut max_consecutive_wins = 0;
        let mut max_consecutive_losses = 0;
        let mut current_wins = 0;
        let mut current_losses = 0;

        for trade in trades {
            if let Some(pnl) = trade.pnl {
                if pnl > 0.0 {
                    current_wins += 1;
                    current_losses = 0;
                    max_consecutive_wins = max_consecutive_wins.max(current_wins);
                } else {
                    current_losses += 1;
                    current_wins = 0;
                    max_consecutive_losses = max_consecutive_losses.max(current_losses);
                }
            }
        }

        (max_consecutive_wins, max_consecutive_losses)
    }

    /// 거래 분포 분석
    fn analyze_trade_distribution(&self, trades: &[&Trade]) -> Result<TradeDistribution> {
        let mut return_buckets = HashMap::new();
        let mut holding_period_distribution = HashMap::new();
        let mut monthly_trade_count = HashMap::new();

        // 수익률 구간별 분포
        for trade in trades {
            if let Some(pnl) = trade.pnl {
                let return_pct = pnl / trade.entry_price;

                let bucket = if return_pct < -0.1 {
                    "Less than -10%"
                } else if return_pct < -0.05 {
                    "-10% to -5%"
                } else if return_pct < 0.0 {
                    "-5% to 0%"
                } else if return_pct < 0.05 {
                    "0% to 5%"
                } else if return_pct < 0.1 {
                    "5% to 10%"
                } else {
                    "More than 10%"
                };

                *return_buckets.entry(bucket.to_string()).or_insert(0) += 1;
            }

            // 보유 기간 분포
            if let Some(holding_period) = trade.holding_period {
                let bucket = if holding_period <= 1 {
                    "1 day"
                } else if holding_period <= 5 {
                    "2-5 days"
                } else if holding_period <= 20 {
                    "6-20 days"
                } else if holding_period <= 60 {
                    "21-60 days"
                } else {
                    "More than 60 days"
                };

                *holding_period_distribution.entry(bucket.to_string()).or_insert(0) += 1;
            }

            // 월별 거래 수
            let month_key = trade.entry_time.format("%Y-%m").to_string();
            *monthly_trade_count.entry(month_key).or_insert(0) += 1;
        }

        Ok(TradeDistribution {
            return_buckets,
            holding_period_distribution,
            monthly_trade_count,
        })
    }

    /// 위험 분석
    fn analyze_risks(&self) -> Result<RiskAnalysis> {
        let risk = &self.result.risk_metrics;

        Ok(RiskAnalysis {
            var_95: risk.var_95,
            var_99: risk.var_99,
            cvar_95: risk.cvar_95,
            cvar_99: risk.cvar_99,
            volatility: risk.downside_deviation,
            downside_deviation: risk.downside_deviation,
            skewness: risk.skewness,
            kurtosis: risk.kurtosis,
            beta: risk.beta,
            alpha: risk.alpha,
            tracking_error: risk.tracking_error,
            information_ratio: risk.tracking_error,
        })
    }

    /// 벤치마크 분석
    fn analyze_benchmark(&self) -> Result<BenchmarkAnalysis> {
        if let Some(benchmark) = &self.result.benchmark_comparison {
            // 롤링 상관관계 계산 (30일 윈도우)
            let rolling_correlation = self.calculate_rolling_correlation(30)?;

            Ok(BenchmarkAnalysis {
                benchmark_symbol: benchmark.benchmark_symbol.clone(),
                excess_return: benchmark.excess_return,
                tracking_error: benchmark.tracking_error,
                information_ratio: benchmark.information_ratio,
                beta: benchmark.beta,
                alpha: benchmark.alpha,
                correlation: 0.0, // TODO: 전체 기간 상관관계 계산
                rolling_correlation,
            })
        } else {
            Err(anyhow::anyhow!("벤치마크 데이터가 없습니다"))
        }
    }

    /// 롤링 상관관계 계산
    fn calculate_rolling_correlation(&self, window: usize) -> Result<Vec<RollingCorrelation>> {
        // TODO: 벤치마크 데이터와의 롤링 상관관계 계산
        Ok(vec![])
    }

    /// 리포트를 마크다운으로 출력
    pub fn generate_markdown_report(&self) -> Result<String> {
        let report = self.generate_report()?;
        
        let mut markdown = String::new();
        
        // 헤더
        markdown.push_str("# 백테스팅 성능 리포트\n\n");
        
        // 요약
        markdown.push_str("## 📊 성능 요약\n\n");
        markdown.push_str(&format!("- **총 수익률**: {:.2}%\n", report.summary.total_return * 100.0));
        markdown.push_str(&format!("- **연환산 수익률**: {:.2}%\n", report.summary.annualized_return * 100.0));
        markdown.push_str(&format!("- **변동성**: {:.2}%\n", report.summary.volatility * 100.0));
        markdown.push_str(&format!("- **샤프 비율**: {:.2}\n", report.summary.sharpe_ratio));
        markdown.push_str(&format!("- **최대 낙폭**: {:.2}%\n", report.summary.max_drawdown * 100.0));
        markdown.push_str(&format!("- **승률**: {:.2}%\n", report.summary.win_rate * 100.0));
        markdown.push_str(&format!("- **수익 팩터**: {:.2}\n", report.summary.profit_factor));
        markdown.push_str(&format!("- **총 거래 수**: {}\n", report.summary.total_trades));
        
        // 거래 분석
        markdown.push_str("\n## 📈 거래 분석\n\n");
        markdown.push_str(&format!("- **승리 거래**: {} ({}%)\n", 
                                   report.trade_analysis.winning_trades, 
                                   report.trade_analysis.win_rate * 100.0));
        markdown.push_str(&format!("- **패배 거래**: {} ({}%)\n", 
                                   report.trade_analysis.losing_trades, 
                                   (1.0 - report.trade_analysis.win_rate) * 100.0));
        markdown.push_str(&format!("- **평균 거래 수익률**: {:.2}%\n", report.trade_analysis.average_trade_return * 100.0));
        markdown.push_str(&format!("- **평균 승리**: {:.2}%\n", report.trade_analysis.average_win * 100.0));
        markdown.push_str(&format!("- **평균 패배**: {:.2}%\n", report.trade_analysis.average_loss * 100.0));
        markdown.push_str(&format!("- **최고 거래**: {:.2}%\n", report.trade_analysis.best_trade * 100.0));
        markdown.push_str(&format!("- **최악 거래**: {:.2}%\n", report.trade_analysis.worst_trade * 100.0));
        
        // 위험 분석
        markdown.push_str("\n## ⚠️ 위험 분석\n\n");
        markdown.push_str(&format!("- **VaR (95%)**: {:.2}%\n", report.risk_analysis.var_95 * 100.0));
        markdown.push_str(&format!("- **VaR (99%)**: {:.2}%\n", report.risk_analysis.var_99 * 100.0));
        markdown.push_str(&format!("- **CVaR (95%)**: {:.2}%\n", report.risk_analysis.cvar_95 * 100.0));
        markdown.push_str(&format!("- **왜도**: {:.2}\n", report.risk_analysis.skewness));
        markdown.push_str(&format!("- **첨도**: {:.2}\n", report.risk_analysis.kurtosis));
        
        // 낙폭 분석
        markdown.push_str("\n## 📉 낙폭 분석\n\n");
        markdown.push_str(&format!("- **최대 낙폭**: {:.2}%\n", report.drawdown_analysis.max_drawdown * 100.0));
        markdown.push_str(&format!("- **최대 낙폭 지속 기간**: {}일\n", report.drawdown_analysis.max_drawdown_duration));
        markdown.push_str(&format!("- **현재 낙폭**: {:.2}%\n", report.drawdown_analysis.current_drawdown * 100.0));
        markdown.push_str(&format!("- **평균 회복 시간**: {:.1}일\n", report.drawdown_analysis.recovery_analysis.average_recovery_time));
        
        Ok(markdown)
    }
}

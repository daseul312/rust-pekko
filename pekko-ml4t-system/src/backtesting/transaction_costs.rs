//! 거래 비용 모델링
//! 
//! 실제 거래에서 발생하는 다양한 비용들을 모델링합니다.

use super::{TransactionCosts, SlippageModel};
use serde::{Serialize, Deserialize};
use anyhow::Result;
use tracing::{debug};

/// 거래 비용 계산기
pub struct TransactionCostCalculator {
    commission_model: CommissionModel,
    slippage_model: SlippageModel,
    market_impact_model: MarketImpactModel,
    timing_cost_model: TimingCostModel,
}

/// 수수료 모델
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommissionModel {
    Fixed { per_trade: f64 },
    Percentage { rate: f64, min: f64, max: f64 },
    Tiered { tiers: Vec<CommissionTier> },
    Korean { 
        domestic_rate: f64,
        foreign_rate: f64,
        min_commission: f64,
    },
    US {
        per_share: f64,
        min_commission: f64,
        max_commission: f64,
    },
}

/// 수수료 티어
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommissionTier {
    pub min_value: f64,
    pub max_value: f64,
    pub rate: f64,
}

/// 시장 임팩트 모델
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MarketImpactModel {
    Linear { coefficient: f64 },
    SquareRoot { coefficient: f64 },
    AlmgrenChriss { 
        permanent_impact: f64,
        temporary_impact: f64,
    },
    Kissell { 
        a: f64, // 기본 임팩트
        b: f64, // 거래량 계수
        sigma: f64, // 변동성 계수
    },
}

/// 타이밍 비용 모델
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimingCostModel {
    Fixed { cost: f64 },
    VolatilityBased { 
        base_cost: f64,
        volatility_multiplier: f64,
    },
    MarketConditionBased {
        bull_market_cost: f64,
        bear_market_cost: f64,
        sideways_market_cost: f64,
    },
}

/// 거래 비용 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionCostResult {
    pub total_cost: f64,
    pub commission: f64,
    pub slippage: f64,
    pub market_impact: f64,
    pub timing_cost: f64,
    pub cost_breakdown: CostBreakdown,
}

/// 비용 세부 내역
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostBreakdown {
    pub commission_percentage: f64,
    pub slippage_percentage: f64,
    pub market_impact_percentage: f64,
    pub timing_cost_percentage: f64,
    pub total_cost_percentage: f64,
}

impl TransactionCostCalculator {
    /// 새로운 거래 비용 계산기 생성
    pub fn new(
        commission_model: CommissionModel,
        slippage_model: SlippageModel,
        market_impact_model: MarketImpactModel,
        timing_cost_model: TimingCostModel,
    ) -> Self {
        Self {
            commission_model,
            slippage_model,
            market_impact_model,
            timing_cost_model,
        }
    }

    /// 거래 비용 계산
    pub fn calculate_costs(
        &self,
        trade_value: f64,
        quantity: u32,
        market_volatility: f64,
        market_condition: MarketCondition,
        execution_time: Option<f64>, // 초 단위
    ) -> Result<TransactionCostResult> {
        debug!("거래 비용 계산: 가치={}, 수량={}, 변동성={:.4}", 
               trade_value, quantity, market_volatility);

        // 1. 수수료 계산
        let commission = self.calculate_commission(trade_value, quantity)?;

        // 2. 슬리피지 계산
        let slippage = self.calculate_slippage(trade_value, quantity, market_volatility)?;

        // 3. 시장 임팩트 계산
        let market_impact = self.calculate_market_impact(trade_value, quantity, market_volatility)?;

        // 4. 타이밍 비용 계산
        let timing_cost = self.calculate_timing_cost(
            trade_value, 
            market_volatility, 
            market_condition, 
            execution_time
        )?;

        let total_cost = commission + slippage + market_impact + timing_cost;

        let cost_breakdown = CostBreakdown {
            commission_percentage: commission / trade_value * 100.0,
            slippage_percentage: slippage / trade_value * 100.0,
            market_impact_percentage: market_impact / trade_value * 100.0,
            timing_cost_percentage: timing_cost / trade_value * 100.0,
            total_cost_percentage: total_cost / trade_value * 100.0,
        };

        debug!("거래 비용 완료: 총 비용={:.2} ({:.2}%)", total_cost, cost_breakdown.total_cost_percentage);

        Ok(TransactionCostResult {
            total_cost,
            commission,
            slippage,
            market_impact,
            timing_cost,
            cost_breakdown,
        })
    }

    /// 수수료 계산
    fn calculate_commission(&self, trade_value: f64, quantity: u32) -> Result<f64> {
        let commission = match &self.commission_model {
            CommissionModel::Fixed { per_trade } => *per_trade,
            
            CommissionModel::Percentage { rate, min, max } => {
                let commission = trade_value * rate;
                commission.max(*min).min(*max)
            }
            
            CommissionModel::Tiered { tiers } => {
                self.calculate_tiered_commission(trade_value, tiers)?
            }
            
            CommissionModel::Korean { domestic_rate, min_commission, .. } => {
                let commission = trade_value * domestic_rate;
                commission.max(*min_commission)
            }
            
            CommissionModel::US { per_share, min_commission, max_commission } => {
                let commission = quantity as f64 * per_share;
                commission.max(*min_commission).min(*max_commission)
            }
        };

        Ok(commission)
    }

    /// 티어별 수수료 계산
    fn calculate_tiered_commission(&self, trade_value: f64, tiers: &[CommissionTier]) -> Result<f64> {
        for tier in tiers {
            if trade_value >= tier.min_value && trade_value <= tier.max_value {
                return Ok(trade_value * tier.rate);
            }
        }
        
        // 최상위 티어 적용
        if let Some(last_tier) = tiers.last() {
            Ok(trade_value * last_tier.rate)
        } else {
            Err(anyhow::anyhow!("수수료 티어가 정의되지 않았습니다"))
        }
    }

    /// 슬리피지 계산
    fn calculate_slippage(&self, trade_value: f64, quantity: u32, market_volatility: f64) -> Result<f64> {
        let slippage = match &self.slippage_model {
            SlippageModel::Fixed(bps) => {
                trade_value * bps / 10000.0
            }
            
            SlippageModel::Linear(coeff) => {
                trade_value * coeff * (quantity as f64).sqrt()
            }
            
            SlippageModel::SquareRoot(coeff) => {
                trade_value * coeff * (quantity as f64).sqrt()
            }
            
            SlippageModel::MarketImpact { a, b, sigma } => {
                let volume_impact = b * (quantity as f64).sqrt();
                let volatility_impact = sigma * market_volatility;
                trade_value * (a + volume_impact + volatility_impact)
            }
        };

        Ok(slippage)
    }

    /// 시장 임팩트 계산
    fn calculate_market_impact(&self, trade_value: f64, quantity: u32, market_volatility: f64) -> Result<f64> {
        let market_impact = match &self.market_impact_model {
            MarketImpactModel::Linear { coefficient } => {
                trade_value * coefficient * (quantity as f64 / 1000.0) // 천주 단위
            }
            
            MarketImpactModel::SquareRoot { coefficient } => {
                trade_value * coefficient * (quantity as f64).sqrt() / 100.0
            }
            
            MarketImpactModel::AlmgrenChriss { permanent_impact, temporary_impact } => {
                let volume_ratio = quantity as f64 / 10000.0; // 만주 기준
                let permanent = *permanent_impact * volume_ratio;
                let temporary = *temporary_impact * volume_ratio.sqrt();
                trade_value * (permanent + temporary)
            }
            
            MarketImpactModel::Kissell { a, b, sigma } => {
                let volume_factor = b * (quantity as f64).sqrt();
                let volatility_factor = sigma * market_volatility;
                trade_value * (a + volume_factor + volatility_factor)
            }
        };

        Ok(market_impact)
    }

    /// 타이밍 비용 계산
    fn calculate_timing_cost(
        &self, 
        trade_value: f64, 
        market_volatility: f64, 
        market_condition: MarketCondition,
        execution_time: Option<f64>
    ) -> Result<f64> {
        let timing_cost = match &self.timing_cost_model {
            TimingCostModel::Fixed { cost } => *cost,
            
            TimingCostModel::VolatilityBased { base_cost, volatility_multiplier } => {
                base_cost + (volatility_multiplier * market_volatility * trade_value)
            }
            
            TimingCostModel::MarketConditionBased { bull_market_cost, bear_market_cost, sideways_market_cost } => {
                let cost_rate = match market_condition {
                    MarketCondition::Bull => *bull_market_cost,
                    MarketCondition::Bear => *bear_market_cost,
                    MarketCondition::Sideways => *sideways_market_cost,
                };
                trade_value * cost_rate
            }
        };

        // 실행 시간에 따른 추가 비용
        let time_based_cost = if let Some(exec_time) = execution_time {
            // 실행 시간이 길수록 비용 증가 (시간 위험)
            trade_value * market_volatility * (exec_time / 3600.0) * 0.001 // 시간당 0.1%
        } else {
            0.0
        };

        Ok(timing_cost + time_based_cost)
    }

    /// 일일 거래 비용 추정
    pub fn estimate_daily_costs(
        &self,
        daily_turnover: f64,
        average_trade_size: f64,
        market_volatility: f64,
        num_trades: u32,
    ) -> Result<TransactionCostResult> {
        let total_trade_value = daily_turnover;
        let average_quantity = (total_trade_value / average_trade_size) as u32;

        self.calculate_costs(
            total_trade_value,
            average_quantity,
            market_volatility,
            MarketCondition::Sideways,
            None,
        )
    }

    /// 포트폴리오 리밸런싱 비용 추정
    pub fn estimate_rebalancing_costs(
        &self,
        portfolio_value: f64,
        rebalancing_ratio: f64, // 리밸런싱되는 비중
        market_volatility: f64,
        market_condition: MarketCondition,
    ) -> Result<TransactionCostResult> {
        let trade_value = portfolio_value * rebalancing_ratio;
        let estimated_quantity = (trade_value / 100.0) as u32; // 평균 주가 100원 가정

        self.calculate_costs(
            trade_value,
            estimated_quantity,
            market_volatility,
            market_condition,
            None,
        )
    }

    /// 비용 효율성 분석
    pub fn analyze_cost_efficiency(&self, results: &[TransactionCostResult]) -> CostEfficiencyAnalysis {
        let total_costs: Vec<f64> = results.iter().map(|r| r.total_cost).collect();
        let total_values: Vec<f64> = results.iter().map(|r| r.total_cost / r.cost_breakdown.total_cost_percentage * 100.0).collect();

        let mean_cost_rate = total_costs.iter().sum::<f64>() / total_values.iter().sum::<f64>() * 100.0;
        let median_cost_rate = self.calculate_median(
            &results.iter().map(|r| r.cost_breakdown.total_cost_percentage).collect::<Vec<f64>>()
        );

        let cost_volatility = self.calculate_volatility(
            &results.iter().map(|r| r.cost_breakdown.total_cost_percentage).collect::<Vec<f64>>()
        );

        CostEfficiencyAnalysis {
            mean_cost_rate,
            median_cost_rate,
            cost_volatility,
            total_transactions: results.len(),
            total_cost: total_costs.iter().sum::<f64>(),
            cost_trend: self.analyze_cost_trend(results),
        }
    }

    /// 중간값 계산
    fn calculate_median(&self, values: &[f64]) -> f64 {
        let mut sorted_values = values.to_vec();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let len = sorted_values.len();
        if len % 2 == 0 {
            (sorted_values[len / 2 - 1] + sorted_values[len / 2]) / 2.0
        } else {
            sorted_values[len / 2]
        }
    }

    /// 변동성 계산
    fn calculate_volatility(&self, values: &[f64]) -> f64 {
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = values.iter()
            .map(|v| (v - mean).powi(2))
            .sum::<f64>() / values.len() as f64;
        variance.sqrt()
    }

    /// 비용 추세 분석
    fn analyze_cost_trend(&self, results: &[TransactionCostResult]) -> CostTrend {
        if results.len() < 10 {
            return CostTrend::InsufficientData;
        }

        let recent_costs: Vec<f64> = results.iter().rev().take(10).map(|r| r.cost_breakdown.total_cost_percentage).collect();
        let earlier_costs: Vec<f64> = results.iter().rev().skip(10).take(10).map(|r| r.cost_breakdown.total_cost_percentage).collect();

        let recent_mean = recent_costs.iter().sum::<f64>() / recent_costs.len() as f64;
        let earlier_mean = earlier_costs.iter().sum::<f64>() / earlier_costs.len() as f64;

        let change = recent_mean - earlier_mean;
        let change_pct = change / earlier_mean * 100.0;

        if change_pct > 5.0 {
            CostTrend::Increasing
        } else if change_pct < -5.0 {
            CostTrend::Decreasing
        } else {
            CostTrend::Stable
        }
    }
}

/// 시장 상황
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MarketCondition {
    Bull,
    Bear,
    Sideways,
}

/// 비용 효율성 분석
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostEfficiencyAnalysis {
    pub mean_cost_rate: f64,
    pub median_cost_rate: f64,
    pub cost_volatility: f64,
    pub total_transactions: usize,
    pub total_cost: f64,
    pub cost_trend: CostTrend,
}

/// 비용 추세
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CostTrend {
    Increasing,
    Decreasing,
    Stable,
    InsufficientData,
}

/// 기본 비용 모델 팩토리
pub struct CostModelFactory;

impl CostModelFactory {
    /// 한국 주식 시장 기본 모델
    pub fn korean_stock_model() -> TransactionCostCalculator {
        TransactionCostCalculator::new(
            CommissionModel::Korean {
                domestic_rate: 0.00015, // 0.015%
                foreign_rate: 0.00025,  // 0.025%
                min_commission: 15.0,   // 15원
            },
            SlippageModel::Fixed(5.0), // 5bps
            MarketImpactModel::SquareRoot { coefficient: 0.1 },
            TimingCostModel::VolatilityBased {
                base_cost: 0.0001, // 0.01%
                volatility_multiplier: 0.5,
            },
        )
    }

    /// 미국 주식 시장 기본 모델
    pub fn us_stock_model() -> TransactionCostCalculator {
        TransactionCostCalculator::new(
            CommissionModel::US {
                per_share: 0.005,     // $0.005 per share
                min_commission: 1.0,   // $1.00
                max_commission: 50.0,  // $50.00
            },
            SlippageModel::MarketImpact {
                a: 0.0005,  // 5bps 기본
                b: 0.001,   // 거래량 계수
                sigma: 0.8, // 변동성 계수
            },
            MarketImpactModel::Kissell {
                a: 0.0003,
                b: 0.0008,
                sigma: 0.6,
            },
            TimingCostModel::MarketConditionBased {
                bull_market_cost: 0.0002,
                bear_market_cost: 0.0003,
                sideways_market_cost: 0.0001,
            },
        )
    }

    /// 암호화폐 기본 모델
    pub fn crypto_model() -> TransactionCostCalculator {
        TransactionCostCalculator::new(
            CommissionModel::Percentage {
                rate: 0.001,  // 0.1%
                min: 0.0,
                max: 1000.0,
            },
            SlippageModel::Linear(0.02),
            MarketImpactModel::Linear { coefficient: 0.01 },
            TimingCostModel::Fixed { cost: 0.0 },
        )
    }

    /// 저비용 모델 (대량 거래용)
    pub fn low_cost_model() -> TransactionCostCalculator {
        TransactionCostCalculator::new(
            CommissionModel::Percentage {
                rate: 0.0001, // 0.01%
                min: 0.0,
                max: 100.0,
            },
            SlippageModel::Fixed(2.0), // 2bps
            MarketImpactModel::SquareRoot { coefficient: 0.05 },
            TimingCostModel::Fixed { cost: 0.0 },
        )
    }
}

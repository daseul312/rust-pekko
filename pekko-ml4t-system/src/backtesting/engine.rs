//! 백테스팅 엔진 핵심 구현
//! 
//! 실제 백테스팅을 수행하는 메인 엔진

use super::{
    BacktestConfig, BacktestResult, PerformanceMetrics, RiskMetrics, 
    Trade, PortfolioSnapshot, PositionSnapshot, BenchmarkComparison,
    ExecutionSummary, BacktestError, RebalanceFrequency, TransactionCosts,
    SlippageModel, TradeSide, ExecutionQuality
};
use crate::api::MarketDataResponse;
use crate::actors::{MarketData, TradingSignal};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use chrono::{DateTime, Utc, Duration, Datelike};
use anyhow::Result;
use tracing::{ warn, error, debug};

/// 백테스팅 엔진
pub struct BacktestEngine {
    config: BacktestConfig,
    data_cache: HashMap<String, Vec<MarketData>>,
    portfolio: Portfolio,
    trades: Vec<Trade>,
    portfolio_history: Vec<PortfolioSnapshot>,
    current_trade_id: u32,
}

/// 포트폴리오 상태
#[derive(Debug, Clone)]
struct Portfolio {
    cash: f64,
    positions: HashMap<String, Position>,
    total_value: f64,
    daily_returns: Vec<f64>,
    peak_value: f64,
    max_drawdown: f64,
}

/// 포지션 상태
#[derive(Debug, Clone)]
struct Position {
    symbol: String,
    quantity: i32,
    average_price: f64,
    market_value: f64,
    unrealized_pnl: f64,
}

/// 전략 트레이트
pub trait TradingStrategy: Send + Sync {
    fn generate_signals(
        &self,
        market_data: &HashMap<String, MarketData>,
        portfolio: &PortfolioSnapshot,
    ) -> Result<HashMap<String, TradingSignal>>;
    
    fn get_name(&self) -> String;
    fn get_parameters(&self) -> HashMap<String, f64>;
}

impl BacktestEngine {
    /// 새로운 백테스팅 엔진 생성
    pub fn new(config: BacktestConfig) -> Self {
        Self {
            config,
            data_cache: HashMap::new(),
            portfolio: Portfolio {
                cash: 0.0,
                positions: HashMap::new(),
                total_value: 0.0,
                daily_returns: Vec::new(),
                peak_value: 0.0,
                max_drawdown: 0.0,
            },
            trades: Vec::new(),
            portfolio_history: Vec::new(),
            current_trade_id: 1,
        }
    }

    /// 시장 데이터 로드
    pub fn load_market_data(&mut self, symbol: &str, data: Vec<MarketData>) {
        let data_len = data.len();
        self.data_cache.insert(symbol.to_string(), data);
    }

    /// 백테스팅 실행
    pub async fn run_backtest<T>(&mut self, strategy: &T) -> Result<BacktestResult>
    where
        T: TradingStrategy,
    {
        // 초기 포트폴리오 설정
        self.portfolio.cash = self.config.initial_capital;
        self.portfolio.total_value = self.config.initial_capital;
        self.portfolio.peak_value = self.config.initial_capital;

        // 거래일 생성
        let trading_days = self.generate_trading_days()?;
        // 메인 백테스팅 루프
        for (i, &current_date) in trading_days.iter().enumerate() {
            debug!("백테스팅 진행: {}/{} ({})", 
                   i + 1, trading_days.len(), current_date.format("%Y-%m-%d"));

            // 1. 현재 시장 데이터 수집
            let market_data = self.get_market_data_for_date(current_date)?;
            
            // 2. 포트폴리오 가치 업데이트
            self.update_portfolio_value(&market_data, current_date)?;
            
            // 3. 리밸런싱 확인
            if self.should_rebalance(current_date, i)? {
                // 4. 전략 신호 생성
                let current_snapshot = self.get_current_portfolio_snapshot(current_date, &market_data);
                let signals = strategy.generate_signals(&market_data, &current_snapshot)?;
                
                // 5. 포트폴리오 리밸런싱
                self.rebalance_portfolio(&signals, &market_data, current_date)?;
            }
            
            // 6. 포트폴리오 스냅샷 저장
            self.save_portfolio_snapshot(current_date, &market_data)?;
        }

        // 결과 생성
        let result = self.generate_results()?;
        Ok(result)
    }

    /// 거래일 생성 (주말 제외)
    fn generate_trading_days(&self) -> Result<Vec<DateTime<Utc>>> {
        let mut trading_days = Vec::new();
        let mut current_date = self.config.start_date;
        
        while current_date <= self.config.end_date {
            // 주말 제외 (토요일=6, 일요일=0)
            let weekday = current_date.weekday().number_from_sunday();
            if weekday != 1 && weekday != 7 { // 월요일=2, 일요일=1
                trading_days.push(current_date);
            }
            current_date = current_date + Duration::days(1);
        }
        
        Ok(trading_days)
    }

    /// 특정 날짜의 시장 데이터 수집
    fn get_market_data_for_date(&self, date: DateTime<Utc>) -> Result<HashMap<String, MarketData>> {
        let mut market_data = HashMap::new();
        
        for symbol in &self.config.symbols {
            if let Some(data_series) = self.data_cache.get(symbol) {
                // 해당 날짜에 가장 가까운 데이터 찾기
                let mut closest_data = None;
                let mut min_diff = Duration::days(365);
                
                for data_point in data_series {
                    let diff = (data_point.timestamp - date).abs();
                    if diff < min_diff && data_point.timestamp <= date {
                        min_diff = diff;
                        closest_data = Some(data_point);
                    }
                }
                
                if let Some(data) = closest_data {
                    market_data.insert(symbol.clone(), data.clone());
                } else {
                    warn!("{} 데이터를 찾을 수 없습니다: {}", symbol, date.format("%Y-%m-%d"));
                }
            }
        }
        
        Ok(market_data)
    }

    /// 포트폴리오 가치 업데이트
    fn update_portfolio_value(&mut self, market_data: &HashMap<String, MarketData>, date: DateTime<Utc>) -> Result<()> {
        let mut total_market_value = self.portfolio.cash;
        
        // 포지션 가치 업데이트
        for (symbol, position) in &mut self.portfolio.positions {
            if let Some(data) = market_data.get(symbol) {
                position.market_value = (position.quantity as f64) * data.close;
                position.unrealized_pnl = position.market_value - (position.quantity as f64 * position.average_price);
                total_market_value += position.market_value;
            }
        }
        
        self.portfolio.total_value = total_market_value;
        
        // 최고점 및 최대 낙폭 업데이트
        if self.portfolio.total_value > self.portfolio.peak_value {
            self.portfolio.peak_value = self.portfolio.total_value;
        }
        
        let current_drawdown = (self.portfolio.peak_value - self.portfolio.total_value) / self.portfolio.peak_value;
        if current_drawdown > self.portfolio.max_drawdown {
            self.portfolio.max_drawdown = current_drawdown;
        }
        
        Ok(())
    }

    /// 리밸런싱 필요 여부 확인
    fn should_rebalance(&self, date: DateTime<Utc>, day_index: usize) -> Result<bool> {
        match self.config.rebalance_frequency {
            RebalanceFrequency::Daily => Ok(true),
            RebalanceFrequency::Weekly => Ok(day_index % 5 == 0), // 매주 금요일
            RebalanceFrequency::Monthly => {
                let day_of_month = date.day();
                Ok(day_of_month == 1) // 매월 1일
            }
            RebalanceFrequency::Quarterly => {
                let month = date.month();
                let day = date.day();
                Ok((month % 3 == 1) && day == 1) // 분기 첫날
            }
            RebalanceFrequency::Custom(days) => Ok(day_index % (days as usize) == 0),
        }
    }

    /// 현재 포트폴리오 스냅샷 생성
    fn get_current_portfolio_snapshot(&self, date: DateTime<Utc>, market_data: &HashMap<String, MarketData>) -> PortfolioSnapshot {
        let mut positions = HashMap::new();
        
        for (symbol, position) in &self.portfolio.positions {
            if let Some(data) = market_data.get(symbol) {
                let weight = position.market_value / self.portfolio.total_value;
                positions.insert(symbol.clone(), PositionSnapshot {
                    symbol: symbol.clone(),
                    quantity: position.quantity,
                    market_price: data.close,
                    market_value: position.market_value,
                    cost_basis: position.quantity as f64 * position.average_price,
                    unrealized_pnl: position.unrealized_pnl,
                    weight,
                });
            }
        }
        
        let daily_return = if let Some(last_snapshot) = self.portfolio_history.last() {
            (self.portfolio.total_value - last_snapshot.total_value) / last_snapshot.total_value
        } else {
            0.0
        };
        
        let cumulative_return = (self.portfolio.total_value - self.config.initial_capital) / self.config.initial_capital;
        let drawdown = (self.portfolio.peak_value - self.portfolio.total_value) / self.portfolio.peak_value;
        
        PortfolioSnapshot {
            timestamp: date,
            total_value: self.portfolio.total_value,
            cash: self.portfolio.cash,
            positions,
            daily_return,
            cumulative_return,
            drawdown,
        }
    }

    /// 포트폴리오 리밸런싱
    fn rebalance_portfolio(
        &mut self,
        signals: &HashMap<String, TradingSignal>,
        market_data: &HashMap<String, MarketData>,
        date: DateTime<Utc>,
    ) -> Result<()> {
        for (symbol, signal) in signals {
            if let Some(data) = market_data.get(symbol) {
                match signal {
                    TradingSignal::Buy { strength } => {
                        self.execute_buy(symbol, data, *strength, date)?;
                    }
                    TradingSignal::Sell { strength } => {
                        self.execute_sell(symbol, data, *strength, date)?;
                    }
                    TradingSignal::Hold => {
                        // 포지션 유지
                    }
                }
            }
        }
        
        Ok(())
    }

    /// 매수 실행
    fn execute_buy(&mut self, symbol: &str, data: &MarketData, strength: f64, date: DateTime<Utc>) -> Result<()> {
        // 포지션 사이징 계산 (Kelly Criterion 기반)
        let target_weight = strength * self.config.max_position_size;
        let target_value = self.portfolio.total_value * target_weight;
        
        // 현재 포지션 가치
        let current_value = self.portfolio.positions
            .get(symbol)
            .map(|p| p.market_value)
            .unwrap_or(0.0);
        
        let additional_value = target_value - current_value;
        
        if additional_value > 0.0 && self.portfolio.cash >= additional_value {
            let quantity = (additional_value / data.close) as u32;
            let trade_value = quantity as f64 * data.close;
            
            // 거래 비용 계산
            let (commission, slippage) = self.calculate_transaction_costs(trade_value, quantity)?;
            let total_cost = trade_value + commission + slippage;
            
            if self.portfolio.cash >= total_cost {
                // 포지션 업데이트
                let position = self.portfolio.positions.entry(symbol.to_string()).or_insert(Position {
                    symbol: symbol.to_string(),
                    quantity: 0,
                    average_price: 0.0,
                    market_value: 0.0,
                    unrealized_pnl: 0.0,
                });
                
                let new_quantity = position.quantity + quantity as i32;
                let new_average_price = ((position.quantity as f64 * position.average_price) + trade_value) / new_quantity as f64;
                
                position.quantity = new_quantity;
                position.average_price = new_average_price;
                position.market_value = new_quantity as f64 * data.close;
                position.unrealized_pnl = position.market_value - (new_quantity as f64 * new_average_price);
                
                self.portfolio.cash -= total_cost;
                
                // 거래 기록
                let trade = Trade {
                    trade_id: format!("{}", self.current_trade_id),
                    symbol: symbol.to_string(),
                    side: TradeSide::Buy,
                    quantity,
                    entry_price: data.close,
                    exit_price: None,
                    entry_time: date,
                    exit_time: None,
                    pnl: None,
                    commission,
                    slippage,
                    holding_period: None,
                    return_pct: None,
                };
                
                self.trades.push(trade);
                self.current_trade_id += 1;
                
                debug!("매수 실행: {} {}주 @ ₩{} (비용: ₩{})", 
                       symbol, quantity, data.close, total_cost);
            }
        }
        
        Ok(())
    }

    /// 매도 실행
    fn execute_sell(&mut self, symbol: &str, data: &MarketData, strength: f64, date: DateTime<Utc>) -> Result<()> {
        if let Some(position) = self.portfolio.positions.get(symbol) {
            if position.quantity > 0 {
                let sell_quantity = ((position.quantity as f64) * strength) as u32;
                let sell_quantity = sell_quantity.min(position.quantity as u32);
                let trade_value = sell_quantity as f64 * data.close;
                
                // 거래 비용 계산
                let (commission, slippage) = self.calculate_transaction_costs(trade_value, sell_quantity)?;
                let net_proceeds = trade_value - commission - slippage;
                
                // 포지션 업데이트
                let position = self.portfolio.positions.get_mut(symbol).unwrap();
                position.quantity -= sell_quantity as i32;
                position.market_value = position.quantity as f64 * data.close;
                position.unrealized_pnl = position.market_value - (position.quantity as f64 * position.average_price);
                
                self.portfolio.cash += net_proceeds;
                
                // 실현 손익 계산
                let cost_basis = sell_quantity as f64 * position.average_price;
                let realized_pnl = trade_value - cost_basis - commission - slippage;
                
                // 거래 기록
                let trade = Trade {
                    trade_id: format!("{}", self.current_trade_id),
                    symbol: symbol.to_string(),
                    side: TradeSide::Sell,
                    quantity: sell_quantity,
                    entry_price: position.average_price,
                    exit_price: Some(data.close),
                    entry_time: date, // 실제로는 포지션 진입 시간이어야 함
                    exit_time: Some(date),
                    pnl: Some(realized_pnl),
                    commission,
                    slippage,
                    holding_period: None, // TODO: 실제 보유 기간 계산
                    return_pct: Some(realized_pnl / cost_basis),
                };
                
                self.trades.push(trade);
                self.current_trade_id += 1;
                
                debug!("매도 실행: {} {}주 @ ₩{} (손익: ₩{})", 
                       symbol, sell_quantity, data.close, realized_pnl);
            }
        }
        
        Ok(())
    }

    /// 거래 비용 계산
    fn calculate_transaction_costs(&self, trade_value: f64, quantity: u32) -> Result<(f64, f64)> {
        // 수수료 계산
        let commission = (trade_value * self.config.transaction_costs.commission_rate)
            .max(self.config.transaction_costs.minimum_commission)
            .min(self.config.transaction_costs.maximum_commission)
            + self.config.transaction_costs.commission_per_trade;
        
        // 슬리피지 계산
        let slippage = match &self.config.slippage {
            SlippageModel::Fixed(bps) => trade_value * bps / 10000.0,
            SlippageModel::Linear(coeff) => trade_value * coeff * (quantity as f64).sqrt(),
            SlippageModel::SquareRoot(coeff) => trade_value * coeff * (quantity as f64).sqrt(),
            SlippageModel::MarketImpact { a, b, sigma } => {
                let volume_impact = b * (quantity as f64).sqrt();
                let volatility_impact = sigma * 0.02; // 가정된 일일 변동성
                trade_value * (a + volume_impact + volatility_impact)
            }
        };
        
        Ok((commission, slippage))
    }

    /// 포트폴리오 스냅샷 저장
    fn save_portfolio_snapshot(&mut self, date: DateTime<Utc>, market_data: &HashMap<String, MarketData>) -> Result<()> {
        let snapshot = self.get_current_portfolio_snapshot(date, market_data);
        
        // 일일 수익률 계산
        if let Some(last_snapshot) = self.portfolio_history.last() {
            let daily_return = (snapshot.total_value - last_snapshot.total_value) / last_snapshot.total_value;
            self.portfolio.daily_returns.push(daily_return);
        }
        
        self.portfolio_history.push(snapshot);
        Ok(())
    }

    /// 백테스팅 결과 생성
    fn generate_results(&mut self) -> Result<BacktestResult> {
        // 성능 메트릭스 계산
        let performance = self.calculate_performance_metrics()?;
        
        // 위험 메트릭스 계산
        let risk_metrics = self.calculate_risk_metrics()?;
        
        // 벤치마크 비교 (선택적)
        let benchmark_comparison = if let Some(benchmark_symbol) = &self.config.benchmark {
            Some(self.calculate_benchmark_comparison(benchmark_symbol)?)
        } else {
            None
        };
        
        // 실행 요약 생성
        let execution_summary = self.generate_execution_summary()?;
        
        Ok(BacktestResult {
            config: self.config.clone(),
            performance,
            risk_metrics,
            trades: self.trades.clone(),
            portfolio_history: self.portfolio_history.clone(),
            benchmark_comparison,
            execution_summary,
        })
    }

    /// 성능 메트릭스 계산
    fn calculate_performance_metrics(&self) -> Result<PerformanceMetrics> {
        let total_return = (self.portfolio.total_value - self.config.initial_capital) / self.config.initial_capital;
        
        // 연환산 수익률
        let days = self.portfolio_history.len() as f64;
        let years = days / 252.0; // 거래일 기준
        let annualized_return = (1.0 + total_return).powf(1.0 / years) - 1.0;
        
        // 변동성 (연환산)
        let mean_return = self.portfolio.daily_returns.iter().sum::<f64>() / days;
        let variance = self.portfolio.daily_returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / days;
        let volatility = variance.sqrt() * (252.0_f64).sqrt();
        
        // Sharpe Ratio
        let sharpe_ratio = (annualized_return - self.config.risk_free_rate) / volatility;
        
        // Sortino Ratio (하방 변동성)
        let downside_returns: Vec<f64> = self.portfolio.daily_returns.iter()
            .filter(|&&r| r < 0.0)
            .cloned()
            .collect();
        let downside_variance = if !downside_returns.is_empty() {
            downside_returns.iter().map(|r| r.powi(2)).sum::<f64>() / downside_returns.len() as f64
        } else {
            0.0
        };
        let downside_deviation = downside_variance.sqrt() * (252.0_f64).sqrt();
        let sortino_ratio = if downside_deviation > 0.0 {
            (annualized_return - self.config.risk_free_rate) / downside_deviation
        } else {
            0.0
        };
        
        // Calmar Ratio
        let calmar_ratio = if self.portfolio.max_drawdown > 0.0 {
            annualized_return / self.portfolio.max_drawdown
        } else {
            0.0
        };
        
        // 거래 통계
        let completed_trades: Vec<&Trade> = self.trades.iter()
            .filter(|t| t.exit_price.is_some() && t.pnl.is_some())
            .collect();
        
        let winning_trades = completed_trades.iter().filter(|t| t.pnl.unwrap() > 0.0).count();
        let win_rate = if !completed_trades.is_empty() {
            winning_trades as f64 / completed_trades.len() as f64
        } else {
            0.0
        };
        
        let total_pnl = completed_trades.iter().map(|t| t.pnl.unwrap()).sum::<f64>();
        let winning_pnl = completed_trades.iter()
            .filter(|t| t.pnl.unwrap() > 0.0)
            .map(|t| t.pnl.unwrap())
            .sum::<f64>();
        let losing_pnl = completed_trades.iter()
            .filter(|t| t.pnl.unwrap() < 0.0)
            .map(|t| t.pnl.unwrap().abs())
            .sum::<f64>();
        
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
        
        let average_loss = if completed_trades.len() - winning_trades > 0 {
            losing_pnl / (completed_trades.len() - winning_trades) as f64
        } else {
            0.0
        };
        
        Ok(PerformanceMetrics {
            total_return,
            annualized_return,
            volatility,
            sharpe_ratio,
            sortino_ratio,
            calmar_ratio,
            information_ratio: 0.0, // TODO: 벤치마크 대비 계산
            max_drawdown: self.portfolio.max_drawdown,
            max_drawdown_duration: 0, // TODO: 최대 낙폭 지속 기간 계산
            win_rate,
            profit_factor,
            average_trade_return,
            average_win,
            average_loss,
        })
    }

    /// 위험 메트릭스 계산
    fn calculate_risk_metrics(&self) -> Result<RiskMetrics> {
        // VaR 계산 (Historical Method)
        let mut returns = self.portfolio.daily_returns.clone();
        returns.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let var_95_index = (returns.len() as f64 * 0.05) as usize;
        let var_99_index = (returns.len() as f64 * 0.01) as usize;
        
        let var_95 = if var_95_index < returns.len() {
            returns[var_95_index]
        } else {
            0.0
        };
        
        let var_99 = if var_99_index < returns.len() {
            returns[var_99_index]
        } else {
            0.0
        };
        
        // CVaR (Expected Shortfall)
        let cvar_95 = if var_95_index > 0 {
            returns[..var_95_index].iter().sum::<f64>() / var_95_index as f64
        } else {
            0.0
        };
        
        let cvar_99 = if var_99_index > 0 {
            returns[..var_99_index].iter().sum::<f64>() / var_99_index as f64
        } else {
            0.0
        };
        
        // 통계적 지표
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter().map(|r| (r - mean_return).powi(2)).sum::<f64>() / returns.len() as f64;
        let std_dev = variance.sqrt();
        
        // Skewness
        let skewness = if std_dev > 0.0 {
            returns.iter().map(|r| ((r - mean_return) / std_dev).powi(3)).sum::<f64>() / returns.len() as f64
        } else {
            0.0
        };
        
        // Kurtosis
        let kurtosis = if std_dev > 0.0 {
            returns.iter().map(|r| ((r - mean_return) / std_dev).powi(4)).sum::<f64>() / returns.len() as f64 - 3.0
        } else {
            0.0
        };
        
        Ok(RiskMetrics {
            var_95,
            var_99,
            cvar_95,
            cvar_99,
            beta: 1.0, // TODO: 벤치마크 대비 베타 계산
            alpha: 0.0, // TODO: 알파 계산
            tracking_error: 0.0, // TODO: 추적 오차 계산
            downside_deviation: std_dev,
            skewness,
            kurtosis,
        })
    }

    /// 벤치마크 비교 계산
    fn calculate_benchmark_comparison(&self, benchmark_symbol: &str) -> Result<BenchmarkComparison> {
        // TODO: 벤치마크 데이터 로드 및 비교 계산
        Ok(BenchmarkComparison {
            benchmark_symbol: benchmark_symbol.to_string(),
            benchmark_return: 0.0,
            benchmark_volatility: 0.0,
            benchmark_sharpe: 0.0,
            excess_return: 0.0,
            tracking_error: 0.0,
            information_ratio: 0.0,
            beta: 1.0,
            alpha: 0.0,
        })
    }

    /// 실행 요약 생성
    fn generate_execution_summary(&self) -> Result<ExecutionSummary> {
        let total_trades = self.trades.len() as u32;
        let winning_trades = self.trades.iter().filter(|t| t.pnl.unwrap_or(0.0) > 0.0).count() as u32;
        let losing_trades = self.trades.iter().filter(|t| t.pnl.unwrap_or(0.0) < 0.0).count() as u32;
        
        let total_commission = self.trades.iter().map(|t| t.commission).sum::<f64>();
        let total_slippage = self.trades.iter().map(|t| t.slippage).sum::<f64>();
        
        let total_trade_value = self.trades.iter()
            .map(|t| (t.quantity as f64) * t.entry_price)
            .sum::<f64>();
        
        let average_trade_size = if total_trades > 0 {
            total_trade_value / total_trades as f64
        } else {
            0.0
        };
        
        let turnover_rate = if self.portfolio.total_value > 0.0 {
            total_trade_value / self.portfolio.total_value
        } else {
            0.0
        };
        
        Ok(ExecutionSummary {
            total_trades,
            winning_trades,
            losing_trades,
            total_commission,
            total_slippage,
            average_trade_size,
            turnover_rate,
            execution_quality: ExecutionQuality {
                fill_rate: 1.0, // TODO: 실제 체결률 계산
                price_improvement: 0.0,
                market_impact: 0.0,
                timing_efficiency: 1.0,
            },
        })
    }
}

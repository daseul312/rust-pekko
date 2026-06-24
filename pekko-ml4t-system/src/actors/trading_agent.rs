//! 거래 실행 Actor
//! 
//! ML4T 워크플로우의 거래 신호를 받아 실제 거래를 실행합니다.
//! - 신호 기반 포지션 관리
//! - 리스크 관리와 연동
//! - 거래 실행 및 포트폴리오 업데이트

use crate::actors::{TradingMessage, TradingSignal, Position};
use pekko_actor::{Actor, ActorContext};
use async_trait::async_trait;
use anyhow::Result;
use tracing::{ error};

/// 거래 실행 Agent
pub struct TradingAgent {
    positions: std::collections::HashMap<String, Position>,
    cash: f64,
    initial_capital: f64,
}

impl TradingAgent {
    pub fn new(
        broker_config: BrokerConfig,
        execution_config: ExecutionConfig,
    ) -> Self {
        Self {
            positions: std::collections::HashMap::new(),
            cash: broker_config.initial_capital,
            initial_capital: broker_config.initial_capital,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub initial_capital: f64,
    pub commission_rate: f64,
}

#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    pub max_position_size: f64,
    pub slippage_tolerance: f64,
}

#[async_trait]
impl Actor for TradingAgent {
    type Message = TradingMessage;
    
    async fn receive(&mut self, message: Self::Message, _context: &mut ActorContext<Self>) {
        match message {
            TradingMessage::GenerateSignals => {
                // TODO: 신호 생성 로직
            }
            TradingMessage::ExecuteBuy { symbol, quantity, price } => {
                // TODO: 매수 실행 로직
            }
            TradingMessage::ExecuteSell { symbol, quantity, price } => {
                // TODO: 매도 실행 로직
            }
            TradingMessage::UpdatePosition { symbol, position } => {
                // TODO: 포지션 업데이트 로직
            }
            TradingMessage::SignalGenerated { symbol, signal } => {
                // TODO: 신호 처리 로직
            }
            TradingMessage::ExecutePositions => {
                // TODO: 포지션 실행 로직
            }
        }
    }
}

impl TradingAgent {
    fn execute_buy(&mut self, symbol: String, quantity: u32, price: f64) -> Result<()> {
        let cost = quantity as f64 * price;
        if cost > self.cash {
            return Err(anyhow::anyhow!("잔고 부족"));
        }
        
        self.cash -= cost;
        
        if let Some(position) = self.positions.get_mut(&symbol) {
            let total_quantity = position.quantity + quantity as i64;
            let total_cost = position.average_price * position.quantity as f64 + cost;
            position.average_price = total_cost / total_quantity as f64;
            position.quantity = total_quantity;
        } else {
            self.positions.insert(symbol.clone(), Position {
                symbol: symbol.clone(),
                quantity: quantity as i64,
                average_price: price,
                current_price: price,
                unrealized_pnl: 0.0,
                realized_pnl: 0.0,
            });
        }
        Ok(())
    }
    
    fn execute_sell(&mut self, symbol: String, quantity: u32, price: f64) -> Result<()> {
        if let Some(position) = self.positions.get_mut(&symbol) {
            if position.quantity < quantity as i64 {
                return Err(anyhow::anyhow!("보유 수량 부족"));
            }

            let proceeds = quantity as f64 * price;
            let cost = quantity as f64 * position.average_price;
            let pnl = proceeds - cost;

            self.cash += proceeds;
            position.quantity -= quantity as i64;
            position.realized_pnl += pnl;
            
            if position.quantity == 0 {
                self.positions.remove(&symbol);
            }
            Ok(())
        } else {
            Err(anyhow::anyhow!("포지션 없음"))
        }
    }
    
    fn update_position(&mut self, symbol: String, position: Position) {
        self.positions.insert(symbol, position);
    }
    
    fn handle_signal(&mut self, symbol: String, signal: TradingSignal, context: &mut ActorContext<Self>) -> Result<()> {
        match signal {
            TradingSignal::Buy { strength } => {
                if strength > 0.7 {
                    let quantity = (self.cash * 0.1 / 100000.0) as u32; // 예시: 잔고의 10%
                    let price = 100000.0; // 예시 가격
                    self.execute_buy(symbol.clone(), quantity, price)?;
                }
            }
            TradingSignal::Sell { strength } => {
                if strength > 0.7 {
                    if let Some(position) = self.positions.get(&symbol) {
                        let quantity = (position.quantity as f64 * 0.5) as u32; // 예시: 포지션의 50%
                        let price = position.current_price;
                        self.execute_sell(symbol.clone(), quantity, price)?;
                    }
                }
            }
            TradingSignal::Hold => {
                // 아무것도 하지 않음
            }
        }
        Ok(())
    }
}

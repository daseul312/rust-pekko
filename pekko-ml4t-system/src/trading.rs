//! 거래 실행 모듈
//! 
//! 실제 거래 실행과 관련된 기능을 제공합니다.
//! - 주문 관리
//! - 포지션 관리
//! - 리스크 관리
//! - 거래 실행

use anyhow::Result;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use tracing::{ debug, warn, error};

/// 거래 시스템
pub struct TradingSystem {
    positions: HashMap<String, Position>,
    orders: Vec<Order>,
    cash: f64,
    config: TradingConfig,
}

/// 포지션
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub quantity: i32,
    pub average_price: f64,
    pub current_price: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
}

/// 주문
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub order_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: u32,
    pub price: f64,
    pub order_type: OrderType,
    pub status: OrderStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// 주문 방향
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

/// 주문 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
    Stop,
    StopLimit,
}

/// 주문 상태
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderStatus {
    Pending,
    Filled,
    PartiallyFilled,
    Cancelled,
    Rejected,
}

/// 거래 설정
#[derive(Debug, Clone)]
pub struct TradingConfig {
    pub max_position_size: f64,
    pub stop_loss_pct: f64,
    pub take_profit_pct: f64,
    pub commission_rate: f64,
    pub slippage_rate: f64,
}

impl TradingSystem {
    pub fn new(initial_cash: f64, config: TradingConfig) -> Self {
        Self {
            positions: HashMap::new(),
            orders: Vec::new(),
            cash: initial_cash,
            config,
        }
    }
    
    /// 매수 주문 생성
    pub async fn create_buy_order(
        &mut self,
        symbol: String,
        quantity: u32,
        price: f64,
        order_type: OrderType,
    ) -> Result<String> {
        let order_id = self.generate_order_id();
        
        let order = Order {
            order_id: order_id.clone(),
            symbol: symbol.clone(),
            side: OrderSide::Buy,
            quantity,
            price,
            order_type,
            status: OrderStatus::Pending,
            created_at: chrono::Utc::now(),
        };
        
        self.orders.push(order);
        Ok(order_id)
    }
    
    /// 매도 주문 생성
    pub async fn create_sell_order(
        &mut self,
        symbol: String,
        quantity: u32,
        price: f64,
        order_type: OrderType,
    ) -> Result<String> {
        let order_id = self.generate_order_id();
        
        let order = Order {
            order_id: order_id.clone(),
            symbol: symbol.clone(),
            side: OrderSide::Sell,
            quantity,
            price,
            order_type,
            status: OrderStatus::Pending,
            created_at: chrono::Utc::now(),
        };
        
        self.orders.push(order);
        Ok(order_id)
    }
    
    /// 주문 실행
    pub async fn execute_order(&mut self, order_id: &str, execution_price: f64) -> Result<()> {
        // 먼저 주문 정보를 가져옴
        let (symbol, quantity, side) = {
            let order = self.orders.iter()
                .find(|o| o.order_id == order_id)
                .ok_or_else(|| anyhow::anyhow!("주문을 찾을 수 없습니다: {}", order_id))?;
            (order.symbol.clone(), order.quantity, order.side.clone())
        };
        
        match side {
            OrderSide::Buy => {
                let total_cost = quantity as f64 * execution_price;
                let commission = total_cost * self.config.commission_rate;
                let total_amount = total_cost + commission;
                
                if total_amount > self.cash {
                    // 주문 상태 업데이트
                    if let Some(order) = self.orders.iter_mut().find(|o| o.order_id == order_id) {
                        order.status = OrderStatus::Rejected;
                    }
                    return Err(anyhow::anyhow!("잔고 부족"));
                }
                
                self.cash -= total_amount;
                self.update_position(&symbol, quantity as i32, execution_price);
                
                // 주문 상태 업데이트
                if let Some(order) = self.orders.iter_mut().find(|o| o.order_id == order_id) {
                    order.status = OrderStatus::Filled;
                }
            }
            OrderSide::Sell => {
                if let Some(position) = self.positions.get(&symbol) {
                    if position.quantity < quantity as i32 {
                        if let Some(order) = self.orders.iter_mut().find(|o| o.order_id == order_id) {
                            order.status = OrderStatus::Rejected;
                        }
                        return Err(anyhow::anyhow!("보유 수량 부족"));
                    }
                } else {
                    if let Some(order) = self.orders.iter_mut().find(|o| o.order_id == order_id) {
                        order.status = OrderStatus::Rejected;
                    }
                    return Err(anyhow::anyhow!("포지션 없음"));
                }
                
                let total_proceeds = quantity as f64 * execution_price;
                let commission = total_proceeds * self.config.commission_rate;
                let net_proceeds = total_proceeds - commission;
                
                self.cash += net_proceeds;
                self.update_position(&symbol, -(quantity as i32), execution_price);
                
                // 주문 상태 업데이트
                if let Some(order) = self.orders.iter_mut().find(|o| o.order_id == order_id) {
                    order.status = OrderStatus::Filled;
                }
            }
        }
        
        Ok(())
    }
    
    /// 포지션 업데이트
    fn update_position(&mut self, symbol: &str, quantity_change: i32, price: f64) {
        if let Some(position) = self.positions.get_mut(symbol) {
            let total_quantity = position.quantity + quantity_change;
            let total_cost = position.average_price * position.quantity as f64 + price * quantity_change as f64;
            
            if total_quantity == 0 {
                self.positions.remove(symbol);
            } else {
                position.average_price = total_cost / total_quantity as f64;
                position.quantity = total_quantity;
            }
        } else if quantity_change > 0 {
            self.positions.insert(symbol.to_string(), Position {
                symbol: symbol.to_string(),
                quantity: quantity_change,
                average_price: price,
                current_price: price,
                unrealized_pnl: 0.0,
                realized_pnl: 0.0,
            });
        }
    }
    
    /// 포지션 업데이트 (현재 가격)
    pub fn update_position_prices(&mut self, price_updates: &HashMap<String, f64>) {
        for (symbol, current_price) in price_updates {
            if let Some(position) = self.positions.get_mut(symbol) {
                position.current_price = *current_price;
                position.unrealized_pnl = (position.current_price - position.average_price) * position.quantity as f64;
            }
        }
    }
    
    /// 포트폴리오 가치 계산
    pub fn calculate_portfolio_value(&self) -> f64 {
        let positions_value: f64 = self.positions.values()
            .map(|p| p.current_price * p.quantity as f64)
            .sum();
        
        self.cash + positions_value
    }
    
    /// 총 수익률 계산
    pub fn calculate_total_return(&self, initial_cash: f64) -> f64 {
        let current_value = self.calculate_portfolio_value();
        (current_value - initial_cash) / initial_cash
    }
    
    /// 주문 ID 생성
    fn generate_order_id(&self) -> String {
        format!("ORD_{}", chrono::Utc::now().timestamp_millis())
    }
    
    /// 포지션 조회
    pub fn get_position(&self, symbol: &str) -> Option<&Position> {
        self.positions.get(symbol)
    }
    
    /// 모든 포지션 조회
    pub fn get_all_positions(&self) -> &HashMap<String, Position> {
        &self.positions
    }
    
    /// 주문 조회
    pub fn get_order(&self, order_id: &str) -> Option<&Order> {
        self.orders.iter().find(|o| o.order_id == order_id)
    }
    
    /// 모든 주문 조회
    pub fn get_all_orders(&self) -> &Vec<Order> {
        &self.orders
    }
    
    /// 현금 잔고 조회
    pub fn get_cash(&self) -> f64 {
        self.cash
    }
}

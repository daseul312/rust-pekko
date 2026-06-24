use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

/// 사용자 계정 정보
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserAccount {
    pub user_id: String,
    pub username: String,
    pub total_cash: f64,           // 총 현금
    pub invested_amount: f64,       // 투자된 금액
    pub total_value: f64,          // 총 자산 가치
    pub profit_loss: f64,          // 손익
    pub profit_loss_rate: f64,     // 손익률 (%)
    pub portfolio: HashMap<String, UserPosition>, // 보유 종목
    pub transaction_history: Vec<Transaction>,     // 거래 내역
    pub created_at: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
}

/// 사용자 포지션 (보유 주식)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserPosition {
    pub symbol: String,
    pub stock_name: String,
    pub quantity: i32,             // 보유 수량
    pub average_price: f64,        // 평균 매수가
    pub current_price: f64,        // 현재가
    pub total_cost: f64,           // 총 매수금액
    pub current_value: f64,        // 현재 평가금액
    pub profit_loss: f64,          // 평가손익
    pub profit_loss_rate: f64,     // 평가손익률 (%)
    pub purchase_date: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
}

/// 거래 내역
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub transaction_id: String,
    pub user_id: String,
    pub symbol: String,
    pub stock_name: String,
    pub transaction_type: TransactionType,
    pub quantity: i32,
    pub price: f64,
    pub total_amount: f64,
    pub commission: f64,           // 수수료
    pub net_amount: f64,           // 실수령/실지급 금액
    pub timestamp: DateTime<Utc>,
    pub reason: String,           // 거래 사유
}

/// 거래 유형
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TransactionType {
    Buy,     // 매수
    Sell,    // 매도
    Dividend, // 배당
}

/// 매매 전략 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingStrategy {
    pub user_id: String,
    pub max_position_ratio: f64,   // 최대 포지션 비율 (예: 0.2 = 20%)
    pub profit_target: f64,        // 목표 수익률 (%)
    pub stop_loss: f64,            // 손절매 수익률 (%)
    pub rebalance_threshold: f64,  // 리밸런싱 임계값 (%)
    pub max_stocks: usize,         // 최대 보유 종목 수
    pub min_cash_ratio: f64,       // 최소 현금 보유 비율
    pub auto_trading: bool,        // 자동 매매 활성화
    pub created_at: DateTime<Utc>,
}

/// 매매 신호
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TradingSignal {
    Buy {
        symbol: String,
        stock_name: String,
        quantity: i32,
        price: f64,
        reason: String,
        confidence: f64,           // 신뢰도 (0-1)
    },
    Sell {
        symbol: String,
        stock_name: String,
        quantity: i32,
        price: f64,
        reason: String,
        confidence: f64,
    },
    Hold {
        symbol: String,
        stock_name: String,
        reason: String,
    },
    Rebalance {
        target_allocation: HashMap<String, f64>, // 목표 배분 비율
        reason: String,
    },
}

impl UserAccount {
    pub fn new(user_id: String, username: String, initial_cash: f64) -> Self {
        Self {
            user_id,
            username,
            total_cash: initial_cash,
            invested_amount: 0.0,
            total_value: initial_cash,
            profit_loss: 0.0,
            profit_loss_rate: 0.0,
            portfolio: HashMap::new(),
            transaction_history: Vec::new(),
            created_at: Utc::now(),
            last_updated: Utc::now(),
        }
    }

    /// 계정 업데이트
    pub fn update_account(&mut self, stock_prices: &HashMap<String, f64>) {
        let mut total_invested = 0.0;
        let mut total_current_value = 0.0;

        // 포트폴리오 업데이트
        for (symbol, position) in self.portfolio.iter_mut() {
            if let Some(&current_price) = stock_prices.get(symbol) {
                position.current_price = current_price;
                position.current_value = position.quantity as f64 * current_price;
                position.profit_loss = position.current_value - position.total_cost;
                position.profit_loss_rate = if position.total_cost > 0.0 {
                    (position.profit_loss / position.total_cost) * 100.0
                } else {
                    0.0
                };
                position.last_updated = Utc::now();

                total_invested += position.total_cost;
                total_current_value += position.current_value;
            }
        }

        self.invested_amount = total_invested;
        self.total_value = self.total_cash + total_current_value;
        self.profit_loss = self.total_value - (self.total_cash + total_invested);
        self.profit_loss_rate = if total_invested > 0.0 {
            (self.profit_loss / total_invested) * 100.0
        } else {
            0.0
        };
        self.last_updated = Utc::now();
    }

    /// 매수 실행
    pub fn buy_stock(&mut self, symbol: String, stock_name: String, quantity: i32, price: f64) -> Result<(), String> {
        let total_cost = quantity as f64 * price;
        let commission = total_cost * 0.0015; // 0.15% 수수료
        let net_amount = total_cost + commission;

        if self.total_cash < net_amount {
            return Err(format!("잔고 부족: 필요 금액 {:.0}원, 보유 현금 {:.0}원", net_amount, self.total_cash));
        }

        // 현금 차감
        self.total_cash -= net_amount;

        // 포지션 업데이트 또는 생성
        if let Some(position) = self.portfolio.get_mut(&symbol) {
            // 기존 포지션에 추가
            let new_total_cost = position.total_cost + total_cost;
            let new_quantity = position.quantity + quantity;
            position.average_price = new_total_cost / new_quantity as f64;
            position.quantity = new_quantity;
            position.total_cost = new_total_cost;
            position.current_price = price;
            position.last_updated = Utc::now();
        } else {
            // 새 포지션 생성
            let position = UserPosition {
                symbol: symbol.clone(),
                stock_name: stock_name.clone(),
                quantity,
                average_price: price,
                current_price: price,
                total_cost: total_cost,
                current_value: total_cost,
                profit_loss: 0.0,
                profit_loss_rate: 0.0,
                purchase_date: Utc::now(),
                last_updated: Utc::now(),
            };
            self.portfolio.insert(symbol.clone(), position);
        }

        // 거래 내역 추가
        let transaction = Transaction {
            transaction_id: format!("TXN_{}_{}", self.user_id, Utc::now().timestamp()),
            user_id: self.user_id.clone(),
            symbol,
            stock_name,
            transaction_type: TransactionType::Buy,
            quantity,
            price,
            total_amount: total_cost,
            commission,
            net_amount,
            timestamp: Utc::now(),
            reason: "자동 매수".to_string(),
        };
        self.transaction_history.push(transaction);

        Ok(())
    }

    /// 매도 실행
    pub fn sell_stock(&mut self, symbol: String, quantity: i32, price: f64) -> Result<(), String> {
        let stock_name = if let Some(position) = self.portfolio.get(&symbol) {
            position.stock_name.clone()
        } else {
            return Err(format!("보유하지 않은 종목: {}", symbol));
        };

        if let Some(position) = self.portfolio.get_mut(&symbol) {
            if position.quantity < quantity {
                return Err(format!("보유 수량 부족: 보유 {}주, 매도 요청 {}주", position.quantity, quantity));
            }

            let sell_amount = quantity as f64 * price;
            let commission = sell_amount * 0.0015; // 0.15% 수수료
            let net_amount = sell_amount - commission;

            // 현금 증가
            self.total_cash += net_amount;

            // 포지션 업데이트
            position.quantity -= quantity;
            if position.quantity == 0 {
                // 모든 주식 매도 시 포지션 제거
                self.portfolio.remove(&symbol);
            } else {
                // 부분 매도 시 평균 단가 재계산
                position.total_cost = position.quantity as f64 * position.average_price;
                position.current_price = price;
                position.last_updated = Utc::now();
            }

            // 거래 내역 추가
            let transaction = Transaction {
                transaction_id: format!("TXN_{}_{}", self.user_id, Utc::now().timestamp()),
                user_id: self.user_id.clone(),
                symbol: symbol.clone(),
                stock_name,
                transaction_type: TransactionType::Sell,
                quantity,
                price,
                total_amount: sell_amount,
                commission,
                net_amount,
                timestamp: Utc::now(),
                reason: "자동 매도".to_string(),
            };
            self.transaction_history.push(transaction);

            Ok(())
        } else {
            Err(format!("보유하지 않은 종목: {}", symbol))
        }
    }

    /// 포트폴리오 비율 계산
    pub fn get_portfolio_allocation(&self) -> HashMap<String, f64> {
        let mut allocation = HashMap::new();
        let total_value = self.total_value;

        if total_value > 0.0 {
            for (symbol, position) in &self.portfolio {
                let ratio = position.current_value / total_value;
                allocation.insert(symbol.clone(), ratio);
            }
        }

        allocation
    }

    /// 현금 비율 계산
    pub fn get_cash_ratio(&self) -> f64 {
        if self.total_value > 0.0 {
            self.total_cash / self.total_value
        } else {
            1.0
        }
    }

    /// 포트폴리오 요약 정보
    pub fn get_portfolio_summary(&self) -> PortfolioSummary {
        let mut top_gainers = Vec::new();
        let mut top_losers = Vec::new();
        let mut total_invested = 0.0;
        let mut total_current_value = 0.0;

        for (symbol, position) in &self.portfolio {
            total_invested += position.total_cost;
            total_current_value += position.current_value;

            if position.profit_loss > 0.0 {
                top_gainers.push((symbol.clone(), position.profit_loss_rate));
            } else if position.profit_loss < 0.0 {
                top_losers.push((symbol.clone(), position.profit_loss_rate));
            }
        }

        top_gainers.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        top_losers.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        PortfolioSummary {
            total_stocks: self.portfolio.len(),
            total_invested,
            total_current_value,
            total_profit_loss: total_current_value - total_invested,
            total_profit_loss_rate: if total_invested > 0.0 {
                ((total_current_value - total_invested) / total_invested) * 100.0
            } else {
                0.0
            },
            cash_ratio: self.get_cash_ratio(),
            top_gainers: top_gainers.into_iter().take(3).collect(),
            top_losers: top_losers.into_iter().take(3).collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioSummary {
    pub total_stocks: usize,
    pub total_invested: f64,
    pub total_current_value: f64,
    pub total_profit_loss: f64,
    pub total_profit_loss_rate: f64,
    pub cash_ratio: f64,
    pub top_gainers: Vec<(String, f64)>,
    pub top_losers: Vec<(String, f64)>,
}

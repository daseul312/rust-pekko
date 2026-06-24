//! 트레이딩 이벤트 정의
//!
//! Event Sourcing을 위한 불변 이벤트 타입들

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use pekko_persistence::Uuid;

/// 트레이딩 이벤트 (불변, 저장됨)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TradeEvent {
    /// 계좌 생성
    AccountCreated {
        account_id: String,
        initial_capital: f64,
        timestamp: DateTime<Utc>,
    },

    /// 현금 입금
    CashDeposited {
        amount: f64,
        timestamp: DateTime<Utc>,
    },

    /// 현금 출금
    CashWithdrawn {
        amount: f64,
        timestamp: DateTime<Utc>,
    },

    /// 매수 체결
    BuyExecuted {
        order_id: Uuid,
        symbol: String,
        quantity: i64,
        price: f64,
        commission: f64,
        timestamp: DateTime<Utc>,
    },

    /// 매도 체결
    SellExecuted {
        order_id: Uuid,
        symbol: String,
        quantity: i64,
        price: f64,
        commission: f64,
        realized_pnl: f64,
        timestamp: DateTime<Utc>,
    },

    /// 포지션 가격 업데이트 (시가평가)
    PriceUpdated {
        symbol: String,
        price: f64,
        timestamp: DateTime<Utc>,
    },

    /// 배당금 수령
    DividendReceived {
        symbol: String,
        amount: f64,
        timestamp: DateTime<Utc>,
    },

    /// 포지션 청산 (전량 매도)
    PositionClosed {
        symbol: String,
        final_pnl: f64,
        timestamp: DateTime<Utc>,
    },
}

impl TradeEvent {
    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            TradeEvent::AccountCreated { timestamp, .. } => *timestamp,
            TradeEvent::CashDeposited { timestamp, .. } => *timestamp,
            TradeEvent::CashWithdrawn { timestamp, .. } => *timestamp,
            TradeEvent::BuyExecuted { timestamp, .. } => *timestamp,
            TradeEvent::SellExecuted { timestamp, .. } => *timestamp,
            TradeEvent::PriceUpdated { timestamp, .. } => *timestamp,
            TradeEvent::DividendReceived { timestamp, .. } => *timestamp,
            TradeEvent::PositionClosed { timestamp, .. } => *timestamp,
        }
    }

    pub fn event_type(&self) -> &'static str {
        match self {
            TradeEvent::AccountCreated { .. } => "AccountCreated",
            TradeEvent::CashDeposited { .. } => "CashDeposited",
            TradeEvent::CashWithdrawn { .. } => "CashWithdrawn",
            TradeEvent::BuyExecuted { .. } => "BuyExecuted",
            TradeEvent::SellExecuted { .. } => "SellExecuted",
            TradeEvent::PriceUpdated { .. } => "PriceUpdated",
            TradeEvent::DividendReceived { .. } => "DividendReceived",
            TradeEvent::PositionClosed { .. } => "PositionClosed",
        }
    }
}

/// 포트폴리오 스냅샷 (상태 저장용)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioSnapshot {
    pub account_id: String,
    pub cash: f64,
    pub positions: Vec<PositionSnapshot>,
    pub total_realized_pnl: f64,
    pub total_commission_paid: f64,
    pub snapshot_timestamp: DateTime<Utc>,
}

/// 개별 포지션 스냅샷
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub symbol: String,
    pub quantity: i64,
    pub average_price: f64,
    pub current_price: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
}

impl PortfolioSnapshot {
    pub fn total_value(&self) -> f64 {
        let positions_value: f64 = self
            .positions
            .iter()
            .map(|p| p.quantity as f64 * p.current_price)
            .sum();
        self.cash + positions_value
    }

    pub fn total_unrealized_pnl(&self) -> f64 {
        self.positions.iter().map(|p| p.unrealized_pnl).sum()
    }
}

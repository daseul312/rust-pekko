//! Persistent Trading Agent
//!
//! Event Sourcing + Snapshot을 사용하는 트레이딩 에이전트
//! PostgreSQL(Neon)에 모든 거래 이벤트를 저장하고 복구 가능
//!
//! Actor trait를 구현하여 메시지 기반 비동기 처리 지원

use crate::actors::trading_events::{PortfolioSnapshot, PositionSnapshot, TradeEvent};
use crate::actors::{Position, TradingSignal};
use async_trait::async_trait;
use chrono::Utc;
use pekko_actor::{Actor, ActorContext};
use pekko_persistence::{
    Journal, PersistentRepr, SnapshotMetadata, SnapshotStore, Uuid,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{ error};

// ============================================================
// Actor 메시지 타입
// ============================================================

/// Persistent Trading Agent 메시지
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PersistentTradingMessage {
    /// 계좌 초기화
    InitializeAccount {
        account_id: String,
        initial_capital: f64,
    },
    /// 매수 실행
    ExecuteBuy {
        symbol: String,
        quantity: i64,
        price: f64,
    },
    /// 매도 실행
    ExecuteSell {
        symbol: String,
        quantity: i64,
        price: f64,
    },
    /// 가격 업데이트 (시가평가)
    UpdatePrice {
        symbol: String,
        price: f64,
    },
    /// 신호 처리
    HandleSignal {
        symbol: String,
        signal: TradingSignal,
        current_price: f64,
    },
    /// 스냅샷 저장 요청
    SaveSnapshot,
    /// 상태 조회 (응답은 로그로 출력)
    GetStatus,
}

/// 스냅샷 정책 설정
const SNAPSHOT_EVERY_N_EVENTS: u64 = 100;

/// Persistent Trading Agent 상태
#[derive(Debug, Clone)]
pub struct TradingState {
    pub account_id: String,
    pub positions: HashMap<String, Position>,
    pub cash: f64,
    pub initial_capital: f64,
    pub total_realized_pnl: f64,
    pub total_commission_paid: f64,
    pub sequence_nr: u64,
}

impl Default for TradingState {
    fn default() -> Self {
        Self {
            account_id: String::new(),
            positions: HashMap::new(),
            cash: 0.0,
            initial_capital: 0.0,
            total_realized_pnl: 0.0,
            total_commission_paid: 0.0,
            sequence_nr: 0,
        }
    }
}

impl TradingState {
    /// 이벤트를 상태에 적용 (Event Sourcing의 핵심)
    pub fn apply_event(&mut self, event: &TradeEvent) {
        match event {
            TradeEvent::AccountCreated {
                account_id,
                initial_capital,
                ..
            } => {
                self.account_id = account_id.clone();
                self.initial_capital = *initial_capital;
                self.cash = *initial_capital;
            }

            TradeEvent::CashDeposited { amount, .. } => {
                self.cash += amount;
            }

            TradeEvent::CashWithdrawn { amount, .. } => {
                self.cash -= amount;
            }

            TradeEvent::BuyExecuted {
                symbol,
                quantity,
                price,
                commission,
                ..
            } => {
                let cost = *quantity as f64 * price + commission;
                self.cash -= cost;
                self.total_commission_paid += commission;

                if let Some(pos) = self.positions.get_mut(symbol) {
                    // 기존 포지션에 추가
                    let total_qty = pos.quantity + quantity;
                    let total_cost =
                        pos.average_price * pos.quantity as f64 + (*quantity as f64 * price);
                    pos.average_price = total_cost / total_qty as f64;
                    pos.quantity = total_qty;
                } else {
                    // 새 포지션 생성
                    self.positions.insert(
                        symbol.clone(),
                        Position {
                            symbol: symbol.clone(),
                            quantity: *quantity,
                            average_price: *price,
                            current_price: *price,
                            unrealized_pnl: 0.0,
                            realized_pnl: 0.0,
                        },
                    );
                }
            }

            TradeEvent::SellExecuted {
                symbol,
                quantity,
                price,
                commission,
                realized_pnl,
                ..
            } => {
                let proceeds = *quantity as f64 * price - commission;
                self.cash += proceeds;
                self.total_commission_paid += commission;
                self.total_realized_pnl += realized_pnl;

                if let Some(pos) = self.positions.get_mut(symbol) {
                    pos.quantity -= quantity;
                    pos.realized_pnl += realized_pnl;

                    if pos.quantity == 0 {
                        self.positions.remove(symbol);
                    }
                }
            }

            TradeEvent::PriceUpdated { symbol, price, .. } => {
                if let Some(pos) = self.positions.get_mut(symbol) {
                    pos.current_price = *price;
                    pos.unrealized_pnl =
                        (price - pos.average_price) * pos.quantity as f64;
                }
            }

            TradeEvent::DividendReceived { amount, .. } => {
                self.cash += amount;
            }

            TradeEvent::PositionClosed { symbol, .. } => {
                self.positions.remove(symbol);
            }
        }
        self.sequence_nr += 1;
    }

    /// 스냅샷으로 변환
    pub fn to_snapshot(&self) -> PortfolioSnapshot {
        PortfolioSnapshot {
            account_id: self.account_id.clone(),
            cash: self.cash,
            positions: self
                .positions
                .values()
                .map(|p| PositionSnapshot {
                    symbol: p.symbol.clone(),
                    quantity: p.quantity,
                    average_price: p.average_price,
                    current_price: p.current_price,
                    unrealized_pnl: p.unrealized_pnl,
                    realized_pnl: p.realized_pnl,
                })
                .collect(),
            total_realized_pnl: self.total_realized_pnl,
            total_commission_paid: self.total_commission_paid,
            snapshot_timestamp: Utc::now(),
        }
    }

    /// 스냅샷에서 복원
    pub fn from_snapshot(snapshot: &PortfolioSnapshot, sequence_nr: u64) -> Self {
        let mut positions = HashMap::new();
        for ps in &snapshot.positions {
            positions.insert(
                ps.symbol.clone(),
                Position {
                    symbol: ps.symbol.clone(),
                    quantity: ps.quantity,
                    average_price: ps.average_price,
                    current_price: ps.current_price,
                    unrealized_pnl: ps.unrealized_pnl,
                    realized_pnl: ps.realized_pnl,
                },
            );
        }

        Self {
            account_id: snapshot.account_id.clone(),
            positions,
            cash: snapshot.cash,
            initial_capital: snapshot.cash, // 스냅샷 시점 기준
            total_realized_pnl: snapshot.total_realized_pnl,
            total_commission_paid: snapshot.total_commission_paid,
            sequence_nr,
        }
    }

    /// 총 포트폴리오 가치
    pub fn total_value(&self) -> f64 {
        let positions_value: f64 = self
            .positions
            .values()
            .map(|p| p.quantity as f64 * p.current_price)
            .sum();
        self.cash + positions_value
    }
}

/// Persistent Trading Agent
pub struct PersistentTradingAgent<J: Journal, S: SnapshotStore> {
    persistence_id: String,
    state: TradingState,
    journal: Arc<J>,
    snapshot_store: Arc<S>,
    writer_uuid: Uuid,
    commission_rate: f64,
}

impl<J: Journal, S: SnapshotStore> PersistentTradingAgent<J, S> {
    /// 새 에이전트 생성 (복구 포함)
    pub async fn new(
        persistence_id: String,
        journal: Arc<J>,
        snapshot_store: Arc<S>,
        commission_rate: f64,
    ) -> Result<Self, String> {
        let writer_uuid = Uuid::new_v4();
        let mut agent = Self {
            persistence_id: persistence_id.clone(),
            state: TradingState::default(),
            journal,
            snapshot_store,
            writer_uuid,
            commission_rate,
        };

        // 복구 수행
        agent.recover().await?;

        Ok(agent)
    }

    /// 복구: 스냅샷 로드 → 이후 이벤트 재생
    async fn recover(&mut self) -> Result<(), String> {
        // 1. 최신 스냅샷 로드
        let snapshot_result = self
            .snapshot_store
            .load_snapshot(&self.persistence_id, None, None)
            .await
            .map_err(|e| format!("스냅샷 로드 실패: {}", e))?;

        let from_sequence_nr = if let Some((metadata, data)) = snapshot_result {
            let snapshot: PortfolioSnapshot =
                serde_json::from_slice(&data).map_err(|e| format!("스냅샷 역직렬화 실패: {}", e))?;
            self.state = TradingState::from_snapshot(&snapshot, metadata.sequence_nr);
            metadata.sequence_nr + 1
        } else {
            1
        };

        // 2. 스냅샷 이후 이벤트 재생
        let messages = self
            .journal
            .replay_messages(&self.persistence_id, from_sequence_nr, u64::MAX, None)
            .await
            .map_err(|e| format!("이벤트 재생 실패: {}", e))?;
        for msg in messages {
            let event: TradeEvent = serde_json::from_slice(&msg.payload)
                .map_err(|e| format!("이벤트 역직렬화 실패: {}", e))?;
            self.state.apply_event(&event);
        }
        Ok(())
    }

    /// 이벤트 저장 및 적용
    async fn persist_event(&mut self, event: TradeEvent) -> Result<(), String> {
        let payload = serde_json::to_vec(&event).map_err(|e| format!("직렬화 실패: {}", e))?;
        
        let next_seq = self.state.sequence_nr + 1;
        let repr = PersistentRepr::new(
            self.persistence_id.clone(),
            next_seq,
            event.event_type().to_string(),
            payload,
        );

        self.journal
            .write_messages(vec![repr])
            .await
            .map_err(|e| format!("이벤트 저장 실패: {}", e))?;

        // 상태에 적용
        self.state.apply_event(&event);
        // 스냅샷 체크
        if self.state.sequence_nr % SNAPSHOT_EVERY_N_EVENTS == 0 {
            self.save_snapshot().await?;
        }

        Ok(())
    }

    /// 스냅샷 저장
    async fn save_snapshot(&self) -> Result<(), String> {
        let snapshot = self.state.to_snapshot();
        let data = serde_json::to_vec(&snapshot).map_err(|e| format!("스냅샷 직렬화 실패: {}", e))?;

        let metadata = SnapshotMetadata {
            persistence_id: self.persistence_id.clone(),
            sequence_nr: self.state.sequence_nr,
            timestamp: Utc::now(),
        };

        self.snapshot_store
            .save_snapshot(&metadata, data)
            .await
            .map_err(|e| format!("스냅샷 저장 실패: {}", e))?;
        Ok(())
    }

    // ========== 트레이딩 명령 ==========

    /// 계좌 초기화
    pub async fn initialize_account(
        &mut self,
        account_id: String,
        initial_capital: f64,
    ) -> Result<(), String> {
        if !self.state.account_id.is_empty() {
            return Err("계좌가 이미 초기화되었습니다".to_string());
        }

        let event = TradeEvent::AccountCreated {
            account_id,
            initial_capital,
            timestamp: Utc::now(),
        };

        self.persist_event(event).await
    }

    /// 매수 실행
    pub async fn execute_buy(
        &mut self,
        symbol: String,
        quantity: i64,
        price: f64,
    ) -> Result<Uuid, String> {
        let cost = quantity as f64 * price;
        let commission = cost * self.commission_rate;
        let total_cost = cost + commission;

        if total_cost > self.state.cash {
            return Err(format!(
                "잔고 부족: 필요={:.2}, 보유={:.2}",
                total_cost, self.state.cash
            ));
        }

        let order_id = Uuid::new_v4();
        let event = TradeEvent::BuyExecuted {
            order_id,
            symbol: symbol.clone(),
            quantity,
            price,
            commission,
            timestamp: Utc::now(),
        };

        self.persist_event(event).await?;
        Ok(order_id)
    }

    /// 매도 실행
    pub async fn execute_sell(
        &mut self,
        symbol: String,
        quantity: i64,
        price: f64,
    ) -> Result<Uuid, String> {
        let position = self
            .state
            .positions
            .get(&symbol)
            .ok_or_else(|| format!("포지션 없음: {}", symbol))?;

        if position.quantity < quantity {
            return Err(format!(
                "보유 수량 부족: 필요={}, 보유={}",
                quantity, position.quantity
            ));
        }

        let proceeds = quantity as f64 * price;
        let commission = proceeds * self.commission_rate;
        let cost_basis = quantity as f64 * position.average_price;
        let realized_pnl = proceeds - cost_basis - commission;

        let order_id = Uuid::new_v4();
        let event = TradeEvent::SellExecuted {
            order_id,
            symbol: symbol.clone(),
            quantity,
            price,
            commission,
            realized_pnl,
            timestamp: Utc::now(),
        };

        self.persist_event(event).await?;
        Ok(order_id)
    }

    /// 가격 업데이트 (시가평가)
    pub async fn update_price(&mut self, symbol: String, price: f64) -> Result<(), String> {
        if !self.state.positions.contains_key(&symbol) {
            return Ok(()); // 포지션 없으면 무시
        }

        let event = TradeEvent::PriceUpdated {
            symbol,
            price,
            timestamp: Utc::now(),
        };

        self.persist_event(event).await
    }

    /// 신호 처리
    pub async fn handle_signal(
        &mut self,
        symbol: String,
        signal: TradingSignal,
        current_price: f64,
    ) -> Result<Option<Uuid>, String> {
        match signal {
            TradingSignal::Buy { strength } => {
                if strength > 0.7 {
                    // 잔고의 10%로 매수
                    let amount = self.state.cash * 0.1;
                    let quantity = (amount / current_price) as i64;
                    if quantity > 0 {
                        let order_id = self.execute_buy(symbol, quantity, current_price).await?;
                        return Ok(Some(order_id));
                    }
                }
            }
            TradingSignal::Sell { strength } => {
                if strength > 0.7 {
                    if let Some(pos) = self.state.positions.get(&symbol) {
                        let quantity = (pos.quantity as f64 * 0.5) as i64; // 50% 매도
                        if quantity > 0 {
                            let order_id =
                                self.execute_sell(symbol, quantity, current_price).await?;
                            return Ok(Some(order_id));
                        }
                    }
                }
            }
            TradingSignal::Hold => {}
        }
        Ok(None)
    }

    // ========== 조회 ==========

    pub fn state(&self) -> &TradingState {
        &self.state
    }

    pub fn cash(&self) -> f64 {
        self.state.cash
    }

    pub fn positions(&self) -> &HashMap<String, Position> {
        &self.state.positions
    }

    pub fn total_value(&self) -> f64 {
        self.state.total_value()
    }

    pub fn total_realized_pnl(&self) -> f64 {
        self.state.total_realized_pnl
    }
}

// ============================================================
// Actor trait 구현
// ============================================================

#[async_trait]
impl<J, S> Actor for PersistentTradingAgent<J, S>
where
    J: Journal + Send + Sync + 'static,
    S: SnapshotStore + Send + Sync + 'static,
{
    type Message = PersistentTradingMessage;

    async fn receive(&mut self, message: Self::Message, _ctx: &mut ActorContext<Self>) {
        match message {
            PersistentTradingMessage::InitializeAccount {
                account_id,
                initial_capital,
            } => {
                match self.initialize_account(account_id.clone(), initial_capital).await {
                    Ok(()) => (),
                    Err(e) => error!("❌ 계좌 초기화 실패: {}", e),
                }
            }

            PersistentTradingMessage::ExecuteBuy {
                symbol,
                quantity,
                price,
            } => {
                match self.execute_buy(symbol.clone(), quantity, price).await {
                    Ok(order_id) => (),
                    Err(e) => error!("❌ 매수 실패: {}", e),
                }
            }

            PersistentTradingMessage::ExecuteSell {
                symbol,
                quantity,
                price,
            } => {
                match self.execute_sell(symbol.clone(), quantity, price).await {
                    Ok(order_id) => (),
                    Err(e) => error!("❌ 매도 실패: {}", e),
                }
            }

            PersistentTradingMessage::UpdatePrice { symbol, price } => {
                match self.update_price(symbol.clone(), price).await {
                    Ok(()) => (),
                    Err(e) => error!("❌ 가격 업데이트 실패: {}", e),
                }
            }

            PersistentTradingMessage::HandleSignal {
                symbol,
                signal,
                current_price,
            } => {
                match self.handle_signal(symbol.clone(), signal.clone(), current_price).await {
                    Ok(Some(order_id)) => {
                        ()
                    }
                    Ok(None) => {
                        ()
                    }
                    Err(e) => error!("❌ 신호 처리 실패: {}", e),
                }
            }

            PersistentTradingMessage::SaveSnapshot => {
                match self.save_snapshot().await {
                    Ok(()) => (),
                    Err(e) => error!("❌ 스냅샷 저장 실패: {}", e),
                }
            }

            PersistentTradingMessage::GetStatus => {
                for (symbol, pos) in &self.state.positions {
                }
            }
        }
    }

    async fn pre_start(&mut self) {
    }

    async fn post_stop(&mut self) {
        // 종료 전 스냅샷 저장
        if let Err(e) = self.save_snapshot().await {
            error!("❌ 종료 시 스냅샷 저장 실패: {}", e);
        }
    }
}

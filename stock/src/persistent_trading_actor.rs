//! Persistent Trading Actor with CoreDB Event Sourcing
//!
//! 거래 이벤트를 CoreDB에 저장하고, 재시작 시 상태를 복구합니다.

use anyhow::Result;
use chrono::{DateTime, Utc};
use pekko_persistence::{
    CoreDBJournal, CoreDBSnapshotStore, Journal, JournalConfig, PersistentRepr,
    SnapshotConfig, SnapshotMetadata, SnapshotStore,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug};
use uuid::Uuid;

// ============================================================================
// Events (이벤트 소싱용)
// ============================================================================

/// 거래 이벤트
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TradingEvent {
    /// 시스템 시작
    SystemStarted {
        timestamp: DateTime<Utc>,
        dry_run: bool,
    },
    /// 잔고 업데이트
    BalanceUpdated {
        timestamp: DateTime<Utc>,
        total_cash: f64,
        total_evaluation: f64,
    },
    /// 포지션 업데이트
    PositionUpdated {
        timestamp: DateTime<Utc>,
        stock_code: String,
        stock_name: String,
        quantity: u32,
        avg_price: f64,
        current_price: f64,
    },
    /// 매수 주문
    BuyOrderPlaced {
        timestamp: DateTime<Utc>,
        order_id: String,
        stock_code: String,
        stock_name: String,
        quantity: u32,
        price: f64,
    },
    /// 매도 주문
    SellOrderPlaced {
        timestamp: DateTime<Utc>,
        order_id: String,
        stock_code: String,
        quantity: u32,
        price: f64,
        profit_loss_rate: f64,
    },
    /// 주문 체결
    OrderExecuted {
        timestamp: DateTime<Utc>,
        order_id: String,
        executed_quantity: u32,
        executed_price: f64,
    },
    /// 전략 변경
    StrategyChanged {
        timestamp: DateTime<Utc>,
        profit_target: f64,
        stop_loss: f64,
        max_position_ratio: f64,
    },
    /// 관심종목 추가
    WatchlistAdded {
        timestamp: DateTime<Utc>,
        stock_code: String,
    },
    /// 관심종목 제거
    WatchlistRemoved {
        timestamp: DateTime<Utc>,
        stock_code: String,
    },
}

// ============================================================================
// State (Actor 상태)
// ============================================================================

/// 자동매매 전략
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingStrategy {
    pub profit_target: f64,
    pub stop_loss: f64,
    pub max_position_ratio: f64,
    pub min_cash_ratio: f64,
}

impl Default for TradingStrategy {
    fn default() -> Self {
        Self {
            profit_target: 5.0,
            stop_loss: -3.0,
            max_position_ratio: 30.0,
            min_cash_ratio: 20.0,
        }
    }
}

/// 보유 종목 정보
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub stock_code: String,
    pub stock_name: String,
    pub quantity: u32,
    pub avg_price: f64,
    pub current_price: f64,
    pub profit_loss_rate: f64,
}

/// Actor 상태 (스냅샷 저장용)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingState {
    pub positions: HashMap<String, Position>,
    pub total_cash: f64,
    pub watchlist: Vec<String>,
    pub strategy: TradingStrategy,
    pub is_active: bool,
    pub dry_run: bool,
    pub last_sync_time: Option<DateTime<Utc>>,
}

impl Default for TradingState {
    fn default() -> Self {
        Self {
            positions: HashMap::new(),
            total_cash: 0.0,
            watchlist: vec![
                "005930".to_string(), // 삼성전자
                "000660".to_string(), // SK하이닉스
                "035720".to_string(), // 카카오
                "035420".to_string(), // NAVER
            ],
            strategy: TradingStrategy::default(),
            is_active: false,
            dry_run: true,
            last_sync_time: None,
        }
    }
}

// ============================================================================
// Persistent Trading Actor
// ============================================================================

/// CoreDB를 사용한 Persistent Trading Actor
pub struct PersistentTradingActor {
    persistence_id: String,
    state: TradingState,
    sequence_nr: u64,
    journal: Arc<RwLock<CoreDBJournal>>,
    snapshot_store: Arc<RwLock<CoreDBSnapshotStore>>,
    snapshot_interval: u64, // N개 이벤트마다 스냅샷
    writer_uuid: Uuid,
}

impl PersistentTradingActor {
    /// 새 Actor 생성 및 상태 복구
    pub async fn new(
        persistence_id: &str,
        db_path: &str,
    ) -> Result<Self> {
        // Journal 초기화
        let journal_config = JournalConfig {
            keyspace: "stock_trading".to_string(),
            table_name: "events".to_string(),
            ..Default::default()
        };
        let journal = CoreDBJournal::new(db_path, journal_config).await?;
        let journal = Arc::new(RwLock::new(journal));

        // Snapshot Store 초기화
        let snapshot_config = SnapshotConfig {
            keyspace: "stock_trading".to_string(),
            table_name: "snapshots".to_string(),
            ..Default::default()
        };
        let snapshot_store = CoreDBSnapshotStore::new(db_path, snapshot_config).await?;
        let snapshot_store = Arc::new(RwLock::new(snapshot_store));

        let mut actor = Self {
            persistence_id: persistence_id.to_string(),
            state: TradingState::default(),
            sequence_nr: 0,
            journal,
            snapshot_store,
            snapshot_interval: 100, // 100개 이벤트마다 스냅샷
            writer_uuid: Uuid::new_v4(),
        };

        // 상태 복구
        actor.recover().await?;
        Ok(actor)
    }

    /// 상태 복구 (스냅샷 + 이벤트 재생)
    async fn recover(&mut self) -> Result<()> {
        // 1. 최신 스냅샷 로드
        let snapshot_store = self.snapshot_store.read().await;
        if let Some((metadata, data)) = snapshot_store
            .load_snapshot(&self.persistence_id, None, None)
            .await?
        {
            let state: TradingState = serde_json::from_slice(&data)?;
            self.state = state;
            self.sequence_nr = metadata.sequence_nr;
        }
        drop(snapshot_store);

        // 2. 스냅샷 이후 이벤트 재생
        let events = {
            let journal = self.journal.read().await;
            journal
                .replay_messages(
                    &self.persistence_id,
                    self.sequence_nr + 1,
                    u64::MAX,
                    None,
                )
                .await?
        }; // journal lock 해제

        for repr in events {
            let event: TradingEvent = serde_json::from_slice(&repr.payload)?;
            self.apply_event(&event);
            self.sequence_nr = repr.sequence_nr;
        }
        Ok(())
    }

    /// 이벤트 적용 (상태 변경)
    fn apply_event(&mut self, event: &TradingEvent) {
        match event {
            TradingEvent::SystemStarted { dry_run, .. } => {
                self.state.is_active = true;
                self.state.dry_run = *dry_run;
            }
            TradingEvent::BalanceUpdated { total_cash, .. } => {
                self.state.total_cash = *total_cash;
            }
            TradingEvent::PositionUpdated {
                stock_code,
                stock_name,
                quantity,
                avg_price,
                current_price,
                ..
            } => {
                if *quantity == 0 {
                    self.state.positions.remove(stock_code);
                } else {
                    let profit_loss_rate = if *avg_price > 0.0 {
                        ((current_price - avg_price) / avg_price) * 100.0
                    } else {
                        0.0
                    };
                    self.state.positions.insert(
                        stock_code.clone(),
                        Position {
                            stock_code: stock_code.clone(),
                            stock_name: stock_name.clone(),
                            quantity: *quantity,
                            avg_price: *avg_price,
                            current_price: *current_price,
                            profit_loss_rate,
                        },
                    );
                }
            }
            TradingEvent::StrategyChanged {
                profit_target,
                stop_loss,
                max_position_ratio,
                ..
            } => {
                self.state.strategy.profit_target = *profit_target;
                self.state.strategy.stop_loss = *stop_loss;
                self.state.strategy.max_position_ratio = *max_position_ratio;
            }
            TradingEvent::WatchlistAdded { stock_code, .. } => {
                if !self.state.watchlist.contains(stock_code) {
                    self.state.watchlist.push(stock_code.clone());
                }
            }
            TradingEvent::WatchlistRemoved { stock_code, .. } => {
                self.state.watchlist.retain(|s| s != stock_code);
            }
            // 주문 이벤트는 별도 히스토리로 관리 (상태에 직접 영향 X)
            TradingEvent::BuyOrderPlaced { .. }
            | TradingEvent::SellOrderPlaced { .. }
            | TradingEvent::OrderExecuted { .. } => {}
        }
    }

    /// 이벤트 영속화
    pub async fn persist_event(&mut self, event: TradingEvent) -> Result<()> {
        self.sequence_nr += 1;
        let payload = serde_json::to_vec(&event)?;

        let repr = PersistentRepr {
            persistence_id: self.persistence_id.clone(),
            sequence_nr: self.sequence_nr,
            manifest: "TradingEvent".to_string(),
            payload,
            timestamp: Utc::now(),
            writer_uuid: self.writer_uuid,
            tags: vec![],
        };

        // 이벤트 저장
        {
            let journal = self.journal.read().await;
            journal.write_messages(vec![repr]).await?;
        } // journal lock 해제

        // 이벤트 적용
        self.apply_event(&event);

        // 스냅샷 체크
        if self.sequence_nr % self.snapshot_interval == 0 {
            self.save_snapshot().await?;
        }

        debug!(
            "[{}] 이벤트 저장 완료 (sequence_nr: {})",
            self.persistence_id, self.sequence_nr
        );

        Ok(())
    }

    /// 스냅샷 저장
    pub async fn save_snapshot(&mut self) -> Result<()> {
        let data = serde_json::to_vec(&self.state)?;
        let metadata = SnapshotMetadata {
            persistence_id: self.persistence_id.clone(),
            sequence_nr: self.sequence_nr,
            timestamp: Utc::now(),
        };

        let store = self.snapshot_store.read().await;
        store.save_snapshot(&metadata, data).await?;
        Ok(())
    }

    // ========================================================================
    // 비즈니스 로직
    // ========================================================================

    /// 시스템 시작
    pub async fn start_trading(&mut self, dry_run: bool) -> Result<()> {
        let event = TradingEvent::SystemStarted {
            timestamp: Utc::now(),
            dry_run,
        };
        self.persist_event(event).await?;
        Ok(())
    }

    /// 잔고 업데이트
    pub async fn update_balance(&mut self, total_cash: f64, total_evaluation: f64) -> Result<()> {
        let event = TradingEvent::BalanceUpdated {
            timestamp: Utc::now(),
            total_cash,
            total_evaluation,
        };
        self.persist_event(event).await?;
        Ok(())
    }

    /// 포지션 업데이트
    pub async fn update_position(
        &mut self,
        stock_code: &str,
        stock_name: &str,
        quantity: u32,
        avg_price: f64,
        current_price: f64,
    ) -> Result<()> {
        let event = TradingEvent::PositionUpdated {
            timestamp: Utc::now(),
            stock_code: stock_code.to_string(),
            stock_name: stock_name.to_string(),
            quantity,
            avg_price,
            current_price,
        };
        self.persist_event(event).await?;
        Ok(())
    }

    /// 매수 주문 기록
    pub async fn record_buy_order(
        &mut self,
        stock_code: &str,
        stock_name: &str,
        quantity: u32,
        price: f64,
    ) -> Result<String> {
        let order_id = Uuid::new_v4().to_string();
        let event = TradingEvent::BuyOrderPlaced {
            timestamp: Utc::now(),
            order_id: order_id.clone(),
            stock_code: stock_code.to_string(),
            stock_name: stock_name.to_string(),
            quantity,
            price,
        };
        self.persist_event(event).await?;
        Ok(order_id)
    }

    /// 매도 주문 기록
    pub async fn record_sell_order(
        &mut self,
        stock_code: &str,
        quantity: u32,
        price: f64,
        profit_loss_rate: f64,
    ) -> Result<String> {
        let order_id = Uuid::new_v4().to_string();
        let event = TradingEvent::SellOrderPlaced {
            timestamp: Utc::now(),
            order_id: order_id.clone(),
            stock_code: stock_code.to_string(),
            quantity,
            price,
            profit_loss_rate,
        };
        self.persist_event(event).await?;
        Ok(order_id)
    }

    /// 주문 체결 기록
    pub async fn record_order_execution(
        &mut self,
        order_id: &str,
        executed_quantity: u32,
        executed_price: f64,
    ) -> Result<()> {
        let event = TradingEvent::OrderExecuted {
            timestamp: Utc::now(),
            order_id: order_id.to_string(),
            executed_quantity,
            executed_price,
        };
        self.persist_event(event).await?;
        Ok(())
    }

    /// 전략 변경
    pub async fn update_strategy(
        &mut self,
        profit_target: f64,
        stop_loss: f64,
        max_position_ratio: f64,
    ) -> Result<()> {
        let event = TradingEvent::StrategyChanged {
            timestamp: Utc::now(),
            profit_target,
            stop_loss,
            max_position_ratio,
        };
        self.persist_event(event).await?;
        Ok(())
    }

    /// 관심종목 추가
    pub async fn add_to_watchlist(&mut self, stock_code: &str) -> Result<()> {
        let event = TradingEvent::WatchlistAdded {
            timestamp: Utc::now(),
            stock_code: stock_code.to_string(),
        };
        self.persist_event(event).await?;
        Ok(())
    }

    /// 관심종목 제거
    pub async fn remove_from_watchlist(&mut self, stock_code: &str) -> Result<()> {
        let event = TradingEvent::WatchlistRemoved {
            timestamp: Utc::now(),
            stock_code: stock_code.to_string(),
        };
        self.persist_event(event).await?;
        Ok(())
    }

    // ========================================================================
    // 상태 조회
    // ========================================================================

    pub fn get_state(&self) -> &TradingState {
        &self.state
    }

    pub fn get_positions(&self) -> &HashMap<String, Position> {
        &self.state.positions
    }

    pub fn get_total_cash(&self) -> f64 {
        self.state.total_cash
    }

    pub fn get_watchlist(&self) -> &Vec<String> {
        &self.state.watchlist
    }

    pub fn get_strategy(&self) -> &TradingStrategy {
        &self.state.strategy
    }

    pub fn is_active(&self) -> bool {
        self.state.is_active
    }

    pub fn sequence_nr(&self) -> u64 {
        self.sequence_nr
    }
}

// ============================================================================
// Actor Message 정의
// ============================================================================

/// Actor에게 보내는 메시지
#[derive(Debug, Clone)]
pub enum TradingMessage {
    StartTrading { dry_run: bool },
    UpdateBalance { total_cash: f64, total_evaluation: f64 },
    Buy { stock_code: String, stock_name: String, quantity: u32, price: f64 },
    Sell { stock_code: String, quantity: u32, price: f64, profit_loss_rate: f64 },
    GetState,
    SaveSnapshot,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_persistent_trading_actor() {
        let tmp = tempdir().unwrap();
        let db_path = tmp.path().to_str().unwrap();

        // Actor 생성
        let mut actor = PersistentTradingActor::new("test-trader", db_path)
            .await
            .unwrap();

        // 거래 시작
        actor.start_trading(true).await.unwrap();
        assert!(actor.is_active());

        // 잔고 업데이트
        actor.update_balance(1_000_000.0, 1_000_000.0).await.unwrap();
        assert_eq!(actor.get_total_cash(), 1_000_000.0);

        // 포지션 업데이트
        actor
            .update_position("005930", "삼성전자", 10, 70000.0, 72000.0)
            .await
            .unwrap();
        assert!(actor.get_positions().contains_key("005930"));

        // 매수 주문
        let order_id = actor
            .record_buy_order("035720", "카카오", 5, 50000.0)
            .await
            .unwrap();
        assert!(!order_id.is_empty());

        // 스냅샷 저장
        actor.save_snapshot().await.unwrap();

        // 새 Actor로 복구 테스트
        drop(actor);
        let recovered = PersistentTradingActor::new("test-trader", db_path)
            .await
            .unwrap();

        assert!(recovered.is_active());
        assert_eq!(recovered.get_total_cash(), 1_000_000.0);
        assert!(recovered.get_positions().contains_key("005930"));
    }
}

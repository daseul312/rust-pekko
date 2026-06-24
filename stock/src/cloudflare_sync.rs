//! Cloudflare KV 동기화 모듈
//! 자동매매 상태를 Cloudflare KV에 저장하여 웹 대시보드와 동기화

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::process::Command;
use chrono::{DateTime, Utc, Local};
use tracing::{ warn, error};

/// 자동매매 상태
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoTradingState {
    /// 활성화 여부
    pub is_active: bool,
    /// 현재 전략 설정
    pub strategy: TradingStrategyState,
    /// 보유 종목 현황
    pub positions: Vec<PositionState>,
    /// 오늘 거래 내역
    pub today_trades: Vec<TradeRecord>,
    /// 오늘 수익률
    pub today_profit_rate: f64,
    /// 시작 시간
    pub started_at: Option<String>,
    /// 마지막 업데이트
    pub updated_at: String,
    /// 상태 메시지
    pub status_message: String,
    /// 이벤트 로그 (최근 10개)
    pub recent_events: Vec<EventLog>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingStrategyState {
    pub profit_target: f64,
    pub stop_loss: f64,
    pub max_position_ratio: f64,
    pub min_cash_ratio: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionState {
    pub stock_code: String,
    pub stock_name: String,
    pub quantity: u32,
    pub avg_price: f64,
    pub current_price: f64,
    pub profit_loss_rate: f64,
    pub profit_loss_amount: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub timestamp: String,
    pub stock_code: String,
    pub stock_name: String,
    pub trade_type: String,  // "buy" or "sell"
    pub quantity: u32,
    pub price: f64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventLog {
    pub timestamp: String,
    pub level: String,  // "info", "warn", "error"
    pub message: String,
}

impl Default for AutoTradingState {
    fn default() -> Self {
        Self {
            is_active: false,
            strategy: TradingStrategyState {
                profit_target: 5.0,
                stop_loss: -3.0,
                max_position_ratio: 30.0,
                min_cash_ratio: 20.0,
            },
            positions: vec![],
            today_trades: vec![],
            today_profit_rate: 0.0,
            started_at: None,
            updated_at: Utc::now().to_rfc3339(),
            status_message: "비활성화됨".to_string(),
            recent_events: vec![],
        }
    }
}

/// Cloudflare KV 동기화 클라이언트
pub struct CloudflareKVSync {
    namespace_id: String,
    wrangler_path: String,
    project_dir: String,
}

impl CloudflareKVSync {
    pub fn new() -> Self {
        Self {
            namespace_id: "c5df91a936da490cb5e69bd4e2b5c875".to_string(),
            wrangler_path: "npx".to_string(),
            project_dir: "/Volumes/T7/Moltbot/astro-site".to_string(),
        }
    }

    /// 상태를 Cloudflare KV에 저장
    pub fn sync_state(&self, state: &AutoTradingState) -> Result<()> {
        let json = serde_json::to_string(state)?;
        let output = Command::new(&self.wrangler_path)
            .args([
                "wrangler", "kv", "key", "put",
                "auto_trading_state",
                &json,
                "--namespace-id", &self.namespace_id,
                "--remote",
            ])
            .current_dir(&self.project_dir)
            .output()?;

        if output.status.success() {
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!("❌ KV 동기화 실패: {}", stderr);
            Err(anyhow::anyhow!("KV 동기화 실패: {}", stderr))
        }
    }

    /// 상태 불러오기
    pub fn load_state(&self) -> Result<AutoTradingState> {
        let output = Command::new(&self.wrangler_path)
            .args([
                "wrangler", "kv", "key", "get",
                "auto_trading_state",
                "--namespace-id", &self.namespace_id,
                "--remote",
            ])
            .current_dir(&self.project_dir)
            .output()?;

        if output.status.success() {
            let json = String::from_utf8_lossy(&output.stdout);
            let state: AutoTradingState = serde_json::from_str(&json)?;
            Ok(state)
        } else {
            // 없으면 기본값 반환
            Ok(AutoTradingState::default())
        }
    }

    /// 잔고 정보도 함께 동기화
    pub fn sync_balance(&self, balance_json: &str) -> Result<()> {
        let output = Command::new(&self.wrangler_path)
            .args([
                "wrangler", "kv", "key", "put",
                "account_balance",
                balance_json,
                "--namespace-id", &self.namespace_id,
                "--remote",
            ])
            .current_dir(&self.project_dir)
            .output()?;

        if output.status.success() {
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!("⚠️ 잔고 동기화 실패: {}", stderr);
            Err(anyhow::anyhow!("잔고 동기화 실패: {}", stderr))
        }
    }
}

impl AutoTradingState {
    pub fn add_event(&mut self, level: &str, message: &str) {
        let event = EventLog {
            timestamp: Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            level: level.to_string(),
            message: message.to_string(),
        };
        
        self.recent_events.insert(0, event);
        
        // 최근 10개만 유지
        if self.recent_events.len() > 10 {
            self.recent_events.truncate(10);
        }
        
        self.updated_at = Utc::now().to_rfc3339();
    }

    pub fn add_trade(&mut self, trade: TradeRecord) {
        self.today_trades.insert(0, trade);
        self.updated_at = Utc::now().to_rfc3339();
    }

    pub fn set_active(&mut self, active: bool) {
        self.is_active = active;
        if active {
            self.started_at = Some(Utc::now().to_rfc3339());
            self.status_message = "자동매매 실행 중".to_string();
            self.add_event("info", "자동매매 시작됨");
        } else {
            self.started_at = None;
            self.status_message = "비활성화됨".to_string();
            self.add_event("info", "자동매매 중지됨");
        }
        self.updated_at = Utc::now().to_rfc3339();
    }

    pub fn update_positions(&mut self, positions: Vec<PositionState>) {
        self.positions = positions;
        self.updated_at = Utc::now().to_rfc3339();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_state() {
        let state = AutoTradingState::default();
        assert!(!state.is_active);
        assert_eq!(state.strategy.profit_target, 5.0);
    }

    #[test]
    fn test_add_event() {
        let mut state = AutoTradingState::default();
        state.add_event("info", "테스트 이벤트");
        assert_eq!(state.recent_events.len(), 1);
        assert_eq!(state.recent_events[0].message, "테스트 이벤트");
    }
}

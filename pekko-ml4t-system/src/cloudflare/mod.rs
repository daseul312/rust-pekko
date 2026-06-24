//! Cloudflare KV 연동 모듈 (StockPick AI와 통신)

pub mod polling_actor;
pub use polling_actor::*;

use serde::{Deserialize, Serialize};
use anyhow::Result;
use tracing::{ warn, error};

/// 자동매매 전략 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingStrategy {
    pub profit_target: f64,      // 목표 수익률 (%)
    pub stop_loss: f64,          // 손절 기준 (%)
    pub max_position_ratio: f64, // 최대 포지션 비율 (%)
    pub min_cash_ratio: f64,     // 최소 현금 비율 (%)
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

/// 포지션 정보
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub stock_code: String,
    pub stock_name: String,
    pub quantity: i32,
    pub avg_price: f64,
    pub current_price: f64,
    pub profit_rate: f64,
}

/// 거래 기록
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub timestamp: String,
    pub stock_code: String,
    pub stock_name: String,
    pub action: String,  // "BUY" or "SELL"
    pub quantity: i32,
    pub price: f64,
    pub reason: String,
}

/// 이벤트 로그
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingEvent {
    pub timestamp: String,
    pub level: String,   // "info", "warn", "error"
    pub message: String,
}

/// 자동매매 상태 (StockPick AI KV와 동기화)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoTradingState {
    pub is_active: bool,
    pub strategy: TradingStrategy,
    pub positions: Vec<Position>,
    pub today_trades: Vec<Trade>,
    pub today_profit_rate: f64,
    pub started_at: Option<String>,
    pub updated_at: String,
    pub status_message: String,
    pub recent_events: Vec<TradingEvent>,
}

impl Default for AutoTradingState {
    fn default() -> Self {
        Self {
            is_active: false,
            strategy: TradingStrategy::default(),
            positions: vec![],
            today_trades: vec![],
            today_profit_rate: 0.0,
            started_at: None,
            updated_at: chrono::Utc::now().to_rfc3339(),
            status_message: "대기 중".to_string(),
            recent_events: vec![],
        }
    }
}

/// API 응답 래퍼
#[derive(Debug, Deserialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

/// StockPick AI 클라이언트
#[derive(Clone)]
pub struct StockPickClient {
    base_url: String,
    client: reqwest::Client,
}

impl StockPickClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }
    
    /// 자동매매 상태 조회 (폴링)
    pub async fn get_status(&self) -> Result<AutoTradingState> {
        let url = format!("{}/api/auto-trading/status", self.base_url);
        
        let response = self.client
            .get(&url)
            .send()
            .await?;
        
        if !response.status().is_success() {
            anyhow::bail!("Failed to get status: {}", response.status());
        }
        
        let api_response: ApiResponse<AutoTradingState> = response.json().await?;
        
        if api_response.success {
            Ok(api_response.data.unwrap_or_default())
        } else {
            anyhow::bail!("API error: {}", api_response.error.unwrap_or_default())
        }
    }
    
    /// 상태 동기화 (pekko-rust → StockPick AI)
    pub async fn sync_state(&self, state: &SyncRequest) -> Result<()> {
        let url = format!("{}/api/auto-trading/sync", self.base_url);
        
        let response = self.client
            .post(&url)
            .json(state)
            .send()
            .await?;
        
        if !response.status().is_success() {
            anyhow::bail!("Failed to sync state: {}", response.status());
        }
        Ok(())
    }
    
    /// 이벤트만 전송
    pub async fn send_event(&self, level: &str, message: &str) -> Result<()> {
        let request = SyncRequest {
            positions: None,
            today_trades: None,
            today_profit_rate: None,
            status_message: None,
            event: Some(TradingEvent {
                timestamp: chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                level: level.to_string(),
                message: message.to_string(),
            }),
        };
        
        self.sync_state(&request).await
    }
}

/// 동기화 요청
#[derive(Debug, Serialize)]
pub struct SyncRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub positions: Option<Vec<Position>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub today_trades: Option<Vec<Trade>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub today_profit_rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<TradingEvent>,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_default_strategy() {
        let strategy = TradingStrategy::default();
        assert_eq!(strategy.profit_target, 5.0);
        assert_eq!(strategy.stop_loss, -3.0);
    }
}

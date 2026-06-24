//! StockPick AI 폴링 Actor
//! 
//! 주기적으로 웹 상태를 확인하고 자동매매 명령을 처리

use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{ warn, error};
use anyhow::Result;

use super::{StockPickClient, AutoTradingState};

/// 폴링 Actor 메시지
pub enum PollingMessage {
    /// 즉시 상태 체크
    CheckNow,
    /// 폴링 중지
    Stop,
    /// 상태 변경 콜백 등록
    OnStateChange(Box<dyn Fn(AutoTradingState) + Send + Sync>),
}

impl std::fmt::Debug for PollingMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PollingMessage::CheckNow => write!(f, "CheckNow"),
            PollingMessage::Stop => write!(f, "Stop"),
            PollingMessage::OnStateChange(_) => write!(f, "OnStateChange(...)"),
        }
    }
}

/// 폴링 Actor
pub struct PollingActor {
    client: StockPickClient,
    poll_interval: Duration,
    last_state: Option<AutoTradingState>,
    on_state_change: Option<Box<dyn Fn(AutoTradingState) + Send + Sync>>,
}

impl PollingActor {
    pub fn new(base_url: &str, poll_interval_secs: u64) -> Self {
        Self {
            client: StockPickClient::new(base_url),
            poll_interval: Duration::from_secs(poll_interval_secs),
            last_state: None,
            on_state_change: None,
        }
    }
    
    /// Actor 시작 (백그라운드 태스크)
    pub fn start(mut self) -> mpsc::Sender<PollingMessage> {
        let (tx, mut rx) = mpsc::channel::<PollingMessage>(32);
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.poll_interval);
            
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        self.poll().await;
                    }
                    Some(msg) = rx.recv() => {
                        match msg {
                            PollingMessage::CheckNow => {
                                self.poll().await;
                            }
                            PollingMessage::Stop => {
                                break;
                            }
                            PollingMessage::OnStateChange(callback) => {
                                self.on_state_change = Some(callback);
                            }
                        }
                    }
                }
            }
        });
        
        tx
    }
    
    async fn poll(&mut self) {
        match self.client.get_status().await {
            Ok(state) => {
                // is_active 변경 감지
                let state_changed = match &self.last_state {
                    Some(last) => last.is_active != state.is_active,
                    None => true,
                };
                
                if state_changed {
                    if state.is_active {
                        // 웹에 연결 확인 이벤트 전송
                        if let Err(e) = self.client.send_event("info", "pekko-rust 연결됨 - 자동매매 시작").await {
                            warn!("Failed to send event: {}", e);
                        }
                    } else {
                    }
                    
                    // 콜백 호출
                    if let Some(ref callback) = self.on_state_change {
                        callback(state.clone());
                    }
                }
                
                self.last_state = Some(state);
            }
            Err(e) => {
                warn!("Failed to poll StockPick AI: {}", e);
            }
        }
    }
    
    /// 현재 상태 조회
    pub fn get_last_state(&self) -> Option<&AutoTradingState> {
        self.last_state.as_ref()
    }
}

/// 간단한 폴링 시작 헬퍼
pub async fn start_polling(
    base_url: &str, 
    poll_interval_secs: u64,
    on_active: impl Fn(AutoTradingState) + Send + Sync + 'static,
) -> mpsc::Sender<PollingMessage> {
    let actor = PollingActor::new(base_url, poll_interval_secs);
    let tx = actor.start();
    
    // 상태 변경 콜백 등록
    let _ = tx.send(PollingMessage::OnStateChange(Box::new(on_active))).await;
    
    tx
}

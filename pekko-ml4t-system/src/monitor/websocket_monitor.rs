//! WebSocket 기반 Actor 모니터링 서버
//!
//! Actor 시스템의 메시지 흐름을 실시간으로 웹 클라이언트에 전송합니다.

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, RwLock};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{ warn, error};

/// Actor 이벤트 (브라우저로 전송)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorEvent {
    /// 발신 Actor ID
    pub from: String,
    /// 수신 Actor ID  
    pub to: String,
    /// 메시지 타입/내용
    pub msg: String,
    /// 타임스탬프 (Unix millis)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
    /// 추가 데이터
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl ActorEvent {
    pub fn new(from: impl Into<String>, to: impl Into<String>, msg: impl Into<String>) -> Self {
        Self {
            from: from.into(),
            to: to.into(),
            msg: msg.into(),
            timestamp: Some(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)),
            data: None,
        }
    }

    pub fn with_data(mut self, data: serde_json::Value) -> Self {
        self.data = Some(data);
        self
    }
}

/// 모니터링 서버 설정
#[derive(Debug, Clone)]
pub struct MonitorConfig {
    /// WebSocket 서버 주소
    pub address: String,
    /// 포트
    pub port: u16,
    /// 이벤트 버퍼 크기
    pub buffer_size: usize,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1".to_string(),
            port: 9090,
            buffer_size: 1000,
        }
    }
}

/// Actor 모니터링 서버
pub struct ActorMonitor {
    config: MonitorConfig,
    event_tx: broadcast::Sender<ActorEvent>,
    client_count: Arc<RwLock<usize>>,
}

impl ActorMonitor {
    /// 새 모니터 생성
    pub fn new(config: MonitorConfig) -> Self {
        let (event_tx, _) = broadcast::channel(config.buffer_size);
        Self {
            config,
            event_tx,
            client_count: Arc::new(RwLock::new(0)),
        }
    }

    /// 기본 설정으로 생성
    pub fn default_monitor() -> Self {
        Self::new(MonitorConfig::default())
    }

    /// 이벤트 발행자 가져오기
    pub fn get_sender(&self) -> ActorEventSender {
        ActorEventSender {
            tx: self.event_tx.clone(),
        }
    }

    /// 서버 시작
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr = format!("{}:{}", self.config.address, self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        let event_tx = self.event_tx.clone();
        let client_count = self.client_count.clone();

        tokio::spawn(async move {
            while let Ok((stream, peer_addr)) = listener.accept().await {
                let tx = event_tx.clone();
                let count = client_count.clone();
                
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, peer_addr, tx, count).await {
                        warn!("WebSocket 연결 오류: {}", e);
                    }
                });
            }
        });

        Ok(())
    }

    /// 현재 연결된 클라이언트 수
    pub async fn client_count(&self) -> usize {
        *self.client_count.read().await
    }
}

/// 이벤트 발행자 (Actor에서 사용)
#[derive(Clone)]
pub struct ActorEventSender {
    tx: broadcast::Sender<ActorEvent>,
}

impl ActorEventSender {
    /// 이벤트 발행
    pub fn send(&self, event: ActorEvent) {
        let _ = self.tx.send(event);
    }

    /// 간단한 메시지 이벤트 발행
    pub fn emit(&self, from: &str, to: &str, msg: &str) {
        self.send(ActorEvent::new(from, to, msg));
    }
}

/// WebSocket 연결 처리
async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    event_tx: broadcast::Sender<ActorEvent>,
    client_count: Arc<RwLock<usize>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ws_stream = accept_async(stream).await?;
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    
    // 클라이언트 수 증가
    {
        let mut count = client_count.write().await;
        *count += 1;
    }

    // 이벤트 구독
    let mut event_rx = event_tx.subscribe();

    // 연결 메시지 전송
    let welcome = serde_json::json!({
        "type": "connected",
        "message": "Actor Monitor에 연결되었습니다"
    });
    ws_sender.send(Message::Text(welcome.to_string())).await?;

    loop {
        tokio::select! {
            // Actor 이벤트 수신 -> 클라이언트로 전송
            event = event_rx.recv() => {
                match event {
                    Ok(evt) => {
                        let json = serde_json::to_string(&evt)?;
                        if ws_sender.send(Message::Text(json)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("클라이언트 {} 지연: {} 이벤트 스킵", peer_addr, n);
                    }
                    Err(_) => break,
                }
            }
            
            // 클라이언트 메시지 수신 (ping/pong 등)
            msg = ws_receiver.next() => {
                match msg {
                    Some(Ok(Message::Ping(data))) => {
                        let _ = ws_sender.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
        }
    }

    // 클라이언트 수 감소
    {
        let mut count = client_count.write().await;
        *count = count.saturating_sub(1);
    }

    Ok(())
}

// ============================================================
// 매크로: Actor에서 쉽게 이벤트 발행
// ============================================================

/// Actor 이벤트 발행 매크로
#[macro_export]
macro_rules! emit_actor_event {
    ($sender:expr, $from:expr, $to:expr, $msg:expr) => {
        if let Some(ref sender) = $sender {
            sender.emit($from, $to, $msg);
        }
    };
}

// ============================================================
// 테스트용 시뮬레이터
// ============================================================

/// 테스트용 이벤트 시뮬레이터
pub struct EventSimulator {
    sender: ActorEventSender,
}

impl EventSimulator {
    pub fn new(sender: ActorEventSender) -> Self {
        Self { sender }
    }

    /// 시뮬레이션 시작
    pub async fn run_simulation(&self) {
        let scenarios = vec![
            ("news_collector", "news_analyzer", "NewsArticle"),
            ("news_analyzer", "persistent_trading", "BuySignal"),
            ("data_agent", "ml_agent", "MarketData"),
            ("ml_agent", "persistent_trading", "Prediction"),
            ("persistent_trading", "postgres", "SaveEvent"),
            ("persistent_trading", "risk_agent", "CheckRisk"),
            ("risk_agent", "persistent_trading", "Approved"),
            ("sentiment_actor", "prediction_actor", "Score"),
            ("prediction_actor", "persistent_trading", "Signal"),
            ("persistent_trading", "monitoring", "Metrics"),
        ];

        loop {
            for (from, to, msg) in &scenarios {
                self.sender.emit(from, to, msg);
                tokio::time::sleep(tokio::time::Duration::from_millis(500 + rand::random::<u64>() % 500)).await;
            }
        }
    }
}

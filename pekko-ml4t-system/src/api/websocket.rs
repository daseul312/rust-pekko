//! WebSocket 실시간 데이터 스트림
//! 
//! 실시간 시장 데이터, 뉴스, 주문 상태 업데이트를 위한 WebSocket 클라이언트

use super::{ApiClient, ApiConfig, MarketDataResponse, ApiError};
use futures_util::{SinkExt, StreamExt};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use anyhow::Result;
use chrono::{DateTime, Utc};
use tracing::{ warn, error, debug};
use tokio::sync::mpsc;

/// WebSocket 클라이언트
#[derive(Clone)]
pub struct WebSocketClient {
    config: ApiConfig,
    provider: WebSocketProvider,
    subscriptions: HashMap<String, Vec<String>>,
    message_sender: Option<mpsc::UnboundedSender<WebSocketMessage>>,
}

/// WebSocket 제공업체
#[derive(Debug, Clone)]
pub enum WebSocketProvider {
    KIS,           // 한국투자증권
    AlphaVantage,  // Alpha Vantage
    Polygon,       // Polygon.io
    IEX,           // IEX Cloud
    Binance,       // Binance
    Custom(String), // 커스텀 WebSocket
}

/// WebSocket 메시지 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WebSocketMessage {
    MarketData {
        symbol: String,
        price: f64,
        volume: u64,
        timestamp: DateTime<Utc>,
    },
    Trade {
        symbol: String,
        price: f64,
        quantity: u64,
        side: String,
        timestamp: DateTime<Utc>,
    },
    OrderBook {
        symbol: String,
        bids: Vec<(f64, u64)>,
        asks: Vec<(f64, u64)>,
        timestamp: DateTime<Utc>,
    },
    News {
        symbol: String,
        title: String,
        sentiment: Option<f64>,
        timestamp: DateTime<Utc>,
    },
    OrderUpdate {
        order_id: String,
        status: String,
        filled_quantity: u64,
        remaining_quantity: u64,
        timestamp: DateTime<Utc>,
    },
    Heartbeat {
        timestamp: DateTime<Utc>,
    },
    Error {
        message: String,
        timestamp: DateTime<Utc>,
    },
}

/// WebSocket 구독 요청
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionRequest {
    pub channel: String,
    pub symbols: Vec<String>,
    pub action: SubscriptionAction,
}

/// 구독 액션
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubscriptionAction {
    Subscribe,
    Unsubscribe,
}

/// KIS WebSocket 메시지
#[derive(Debug, Deserialize)]
struct KisWebSocketMessage {
    #[serde(rename = "tr_id")]
    tr_id: String,
    #[serde(rename = "tr_key")]
    tr_key: String,
    #[serde(rename = "output")]
    output: Option<KisWebSocketOutput>,
    #[serde(rename = "msg")]
    msg: Option<String>,
}

#[derive(Debug, Deserialize)]
struct KisWebSocketOutput {
    #[serde(rename = "stck_prpr")]
    current_price: String,
    #[serde(rename = "stck_oprc")]
    opening_price: String,
    #[serde(rename = "stck_hgpr")]
    high_price: String,
    #[serde(rename = "stck_lwpr")]
    low_price: String,
    #[serde(rename = "acml_vol")]
    accumulated_volume: String,
    #[serde(rename = "hts_kor_isnm")]
    stock_name: String,
    #[serde(rename = "stck_prdt")]
    price_date: String,
    #[serde(rename = "stck_prtm")]
    price_time: String,
}

/// Polygon WebSocket 메시지
#[derive(Debug, Deserialize)]
struct PolygonWebSocketMessage {
    ev: String, // 이벤트 타입
    sym: String, // 심볼
    p: Option<f64>, // 가격
    s: Option<u64>, // 사이즈
    t: Option<u64>, // 타임스탬프
    x: Option<u32>, // 거래소
}

#[async_trait::async_trait]
impl ApiClient for WebSocketClient {
    type Config = ApiConfig;
    type Response = WebSocketMessage;

    async fn new(config: Self::Config) -> Result<Self> {
        let provider = if config.base_url.contains("koreainvestment") {
            WebSocketProvider::KIS
        } else if config.base_url.contains("polygon") {
            WebSocketProvider::Polygon
        } else if config.base_url.contains("iex") {
            WebSocketProvider::IEX
        } else if config.base_url.contains("binance") {
            WebSocketProvider::Binance
        } else {
            WebSocketProvider::Custom(config.base_url.clone())
        };

        Ok(Self {
            config,
            provider,
            subscriptions: HashMap::new(),
            message_sender: None,
        })
    }

    async fn authenticate(&mut self) -> Result<()> {
        match &self.provider {
            WebSocketProvider::KIS => {
                // KIS WebSocket 인증은 별도 처리 필요
                if self.config.api_key.is_empty() {
                    return Err(ApiError::AuthenticationError("KIS API 키가 필요합니다".to_string()).into());
                }
            }
            WebSocketProvider::Polygon => {
                if self.config.api_key.is_empty() {
                    return Err(ApiError::AuthenticationError("Polygon API 키가 필요합니다".to_string()).into());
                }
            }
            _ => {
                // 기타 제공업체 인증 로직
            }
        }
        Ok(())
    }

    async fn get_market_data(&self, symbol: &str) -> Result<MarketDataResponse> {
        // WebSocket은 실시간 스트림이므로 단일 데이터 조회는 지원하지 않음
        Err(ApiError::ParseError("WebSocket은 실시간 스트림만 지원합니다".to_string()).into())
    }

    async fn get_account_info(&self) -> Result<super::AccountInfo> {
        Err(ApiError::AccountError("WebSocket은 계좌 정보를 제공하지 않습니다".to_string()).into())
    }

    async fn place_order(&self, _order: &super::OrderRequest) -> Result<super::OrderResponse> {
        Err(ApiError::OrderError("WebSocket은 주문 실행을 제공하지 않습니다".to_string()).into())
    }

    async fn get_positions(&self) -> Result<Vec<super::Position>> {
        Err(ApiError::AccountError("WebSocket은 포지션 정보를 제공하지 않습니다".to_string()).into())
    }

    async fn get_order_status(&self, _order_id: &str) -> Result<super::OrderStatus> {
        Err(ApiError::OrderError("WebSocket은 주문 상태를 제공하지 않습니다".to_string()).into())
    }
}

impl WebSocketClient {
    /// WebSocket 연결 시작
    pub async fn start_stream(&mut self) -> Result<mpsc::UnboundedReceiver<WebSocketMessage>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.message_sender = Some(tx);

        let ws_url = self.get_websocket_url();
        let (ws_stream, _) = connect_async(&ws_url).await?;
        let (mut write, mut read) = ws_stream.split();

        // 인증 메시지 전송
        if let Err(e) = self.send_auth_message(&mut write).await {
            error!("WebSocket 인증 실패: {}", e);
            return Err(e);
        }

        // 구독 메시지 전송
        for (channel, symbols) in &self.subscriptions.clone() {
            if let Err(e) = self.send_subscription(&mut write, channel, symbols, SubscriptionAction::Subscribe).await {
                error!("구독 실패: {}", e);
            }
        }

        // 메시지 수신 루프
        let message_sender = self.message_sender.clone();
        tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        if let Some(sender) = &message_sender {
                            if let Ok(ws_message) = Self::parse_message(&text) {
                                if let Err(e) = sender.send(ws_message) {
                                    error!("메시지 전송 실패: {}", e);
                                }
                            }
                        }
                    }
                    Ok(Message::Binary(_)) => {
                        debug!("바이너리 메시지 수신");
                    }
                    Ok(Message::Frame(_)) => {
                        debug!("프레임 메시지 수신");
                    }
                    Ok(Message::Ping(_)) => {
                        debug!("Ping 메시지 수신");
                    }
                    Ok(Message::Pong(_)) => {
                        debug!("Pong 메시지 수신");
                    }
                    Ok(Message::Close(_)) => {
                        break;
                    }
                    Err(e) => {
                        error!("WebSocket 오류: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(rx)
    }

    /// 구독 추가
    pub async fn subscribe(&mut self, channel: &str, symbols: Vec<String>) -> Result<()> {
        self.subscriptions.insert(channel.to_string(), symbols.clone());
        
        if let Some(sender) = &self.message_sender {
            // TODO: 실제 WebSocket에 구독 메시지 전송
        }
        
        Ok(())
    }

    /// 구독 제거
    pub async fn unsubscribe(&mut self, channel: &str) -> Result<()> {
        if let Some(symbols) = self.subscriptions.remove(channel) {
            if let Some(sender) = &self.message_sender {
                // TODO: 실제 WebSocket에 구독 해제 메시지 전송
            }
        }
        
        Ok(())
    }

    /// WebSocket URL 생성
    fn get_websocket_url(&self) -> String {
        match &self.provider {
            WebSocketProvider::KIS => {
                format!("wss://{}", self.config.base_url.replace("https://", "").replace("http://", ""))
            }
            WebSocketProvider::Polygon => {
                format!("wss://socket.polygon.io/stocks?apiKey={}", self.config.api_key)
            }
            WebSocketProvider::IEX => {
                format!("wss://ws-api.iextrading.com/1.0/tops?token={}", self.config.api_key)
            }
            WebSocketProvider::Binance => {
                "wss://stream.binance.com:9443/ws/btcusdt@ticker".to_string()
            }
            WebSocketProvider::Custom(url) => {
                url.clone()
            }
            WebSocketProvider::AlphaVantage => {
                format!("wss://www.alphavantage.co/query?function=LISTING_STATUS&apikey={}", self.config.api_key)
            }
        }
    }

    /// 인증 메시지 전송
    async fn send_auth_message(&self, write: &mut futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, Message>) -> Result<()> {
        match &self.provider {
            WebSocketProvider::Polygon => {
                let auth_msg = serde_json::json!({
                    "action": "auth",
                    "params": self.config.api_key
                });
                write.send(Message::Text(auth_msg.to_string())).await?;
            }
            WebSocketProvider::KIS => {
                // KIS는 별도 인증 프로세스 필요
            }
            _ => {
                // 기타 제공업체 인증 로직
            }
        }
        Ok(())
    }

    /// 구독 메시지 전송
    async fn send_subscription(
        &self,
        write: &mut futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, Message>,
        channel: &str,
        symbols: &[String],
        action: SubscriptionAction,
    ) -> Result<()> {
        match &self.provider {
            WebSocketProvider::Polygon => {
                let action_str = match action {
                    SubscriptionAction::Subscribe => "subscribe",
                    SubscriptionAction::Unsubscribe => "unsubscribe",
                };
                
                let subscription_msg = serde_json::json!({
                    "action": action_str,
                    "params": format!("{}.{}", channel, symbols.join(","))
                });
                
                write.send(Message::Text(subscription_msg.to_string())).await?;
            }
            WebSocketProvider::KIS => {
                // KIS 구독 메시지 형식
            }
            _ => {
                // 기타 제공업체 구독 로직
            }
        }
        Ok(())
    }

    /// 메시지 파싱
    fn parse_message(text: &str) -> Result<WebSocketMessage> {
        // JSON 파싱 시도
        if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(text) {
            // Polygon 메시지 파싱
            if let Ok(polygon_msg) = serde_json::from_value::<PolygonWebSocketMessage>(json_value.clone()) {
                return Ok(WebSocketMessage::MarketData {
                    symbol: polygon_msg.sym,
                    price: polygon_msg.p.unwrap_or(0.0),
                    volume: polygon_msg.s.unwrap_or(0),
                    timestamp: Utc::now(),
                });
            }
            
            // KIS 메시지 파싱
            if let Ok(kis_msg) = serde_json::from_value::<KisWebSocketMessage>(json_value) {
                if let Some(output) = kis_msg.output {
                    return Ok(WebSocketMessage::MarketData {
                        symbol: output.stock_name,
                        price: output.current_price.parse::<f64>().unwrap_or(0.0),
                        volume: output.accumulated_volume.parse::<u64>().unwrap_or(0),
                        timestamp: Utc::now(),
                    });
                }
            }
        }
        
        // 파싱 실패 시 에러 메시지로 처리
        Ok(WebSocketMessage::Error {
            message: format!("파싱 실패: {}", text),
            timestamp: Utc::now(),
        })
    }

    /// 하트비트 메시지 전송
    pub async fn send_heartbeat(&self) -> Result<()> {
        if let Some(sender) = &self.message_sender {
            let heartbeat = WebSocketMessage::Heartbeat {
                timestamp: Utc::now(),
            };
            if let Err(e) = sender.send(heartbeat) {
                error!("하트비트 전송 실패: {}", e);
            }
        }
        Ok(())
    }

    /// 연결 상태 확인
    pub fn is_connected(&self) -> bool {
        self.message_sender.is_some()
    }

    /// 구독 목록 조회
    pub fn get_subscriptions(&self) -> &HashMap<String, Vec<String>> {
        &self.subscriptions
    }
}

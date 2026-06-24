//! StockPick AI 자동매매 데몬
//! 
//! 웹에서 자동매매 시작/중지 명령을 폴링하고
//! 실제 자동매매를 실행합니다.
//! 
//! 실행: cargo run --bin stockpick_sync

use std::env;
use std::sync::Arc;
use anyhow::Result;
use tokio::sync::RwLock;
use tokio::time::{Duration, interval};
use tracing::{ warn, error, Level};
use tracing_subscriber::FmtSubscriber;
use chrono::{Local, Timelike};

use stock::korea_investment_api::KoreaInvestmentAPI;
use stock::cloudflare_sync::{AutoTradingState, PositionState, TradeRecord, EventLog};
use pekko_ml4t_system::cloudflare::{StockPickClient, SyncRequest};

/// 간단한 자동매매 시스템
struct TradingSystem {
    api: KoreaInvestmentAPI,
    client: StockPickClient,
    state: Arc<RwLock<AutoTradingState>>,
    dry_run: bool,
}

impl TradingSystem {
    async fn new(client: StockPickClient, dry_run: bool) -> Result<Self> {
        let mut api = KoreaInvestmentAPI::from_env()?;
        api.get_access_token().await?;
        
        Ok(Self {
            api,
            client,
            state: Arc::new(RwLock::new(AutoTradingState::default())),
            dry_run,
        })
    }
    
    /// 웹에서 상태 폴링
    async fn poll_state(&self) -> Result<bool> {
        let web_state = self.client.get_status().await?;
        let mut state = self.state.write().await;
        
        let was_active = state.is_active;
        state.is_active = web_state.is_active;
        state.strategy = stock::cloudflare_sync::TradingStrategyState {
            profit_target: web_state.strategy.profit_target,
            stop_loss: web_state.strategy.stop_loss,
            max_position_ratio: web_state.strategy.max_position_ratio,
            min_cash_ratio: web_state.strategy.min_cash_ratio,
        };
        
        // 상태 변경 감지
        if !was_active && state.is_active {
            state.started_at = Some(Local::now().to_rfc3339());
            state.status_message = "자동매매 실행 중".to_string();
            self.client.send_event("info", "pekko-rust: 자동매매 시작").await.ok();
        } else if was_active && !state.is_active {
            state.started_at = None;
            state.status_message = "중지됨".to_string();
            self.client.send_event("info", "pekko-rust: 자동매매 중지").await.ok();
        }
        
        Ok(state.is_active)
    }
    
    /// 잔고 및 보유종목 조회
    async fn update_positions(&self) -> Result<()> {
        let (_cash, holdings) = self.api.get_account_info().await?;
        
        let mut positions: Vec<PositionState> = Vec::new();
        
        for (stock_code, quantity, avg_price) in holdings.iter() {
            // 현재가 조회
            let current_price = match self.api.get_stock_price(stock_code).await {
                Ok(p) => p.current_price.parse::<f64>().unwrap_or(*avg_price),
                Err(_) => *avg_price,
            };
            
            let profit_rate = if *avg_price > 0.0 {
                ((current_price - avg_price) / avg_price) * 100.0
            } else {
                0.0
            };
            let profit_amount = ((current_price - avg_price) * (*quantity as f64)) as i64;
            
            positions.push(PositionState {
                stock_code: stock_code.clone(),
                stock_name: stock_code.clone(), // 종목명 = 코드 (임시)
                quantity: *quantity,
                avg_price: *avg_price,
                current_price,
                profit_loss_rate: profit_rate,
                profit_loss_amount: profit_amount,
            });
        }
        
        // 웹에 동기화
        let sync_positions: Vec<pekko_ml4t_system::cloudflare::Position> = positions.iter().map(|p| {
            pekko_ml4t_system::cloudflare::Position {
                stock_code: p.stock_code.clone(),
                stock_name: p.stock_name.clone(),
                quantity: p.quantity as i32,
                avg_price: p.avg_price,
                current_price: p.current_price,
                profit_rate: p.profit_loss_rate,
            }
        }).collect();
        
        let request = SyncRequest {
            positions: Some(sync_positions),
            today_trades: None,
            today_profit_rate: None,
            status_message: Some("포지션 업데이트".to_string()),
            event: None,
        };
        
        self.client.sync_state(&request).await?;
        
        let mut state = self.state.write().await;
        state.positions = positions;
        
        Ok(())
    }
    
    /// 매도 신호 체크
    async fn check_sell_signals(&self) -> Result<Vec<(String, String, u32, String)>> {
        let state = self.state.read().await;
        let mut signals = Vec::new();
        
        for pos in &state.positions {
            let reason = if pos.profit_loss_rate >= state.strategy.profit_target {
                Some(format!("🎯 목표 수익률 달성 (+{:.1}%)", pos.profit_loss_rate))
            } else if pos.profit_loss_rate <= state.strategy.stop_loss {
                Some(format!("⚠️ 손절매 ({:.1}%)", pos.profit_loss_rate))
            } else {
                None
            };
            
            if let Some(r) = reason {
                signals.push((pos.stock_code.clone(), pos.stock_name.clone(), pos.quantity, r));
            }
        }
        
        Ok(signals)
    }
    
    /// 매도 실행
    async fn execute_sell(&self, stock_code: &str, stock_name: &str, quantity: u32, reason: &str) -> Result<()> {
        if self.dry_run {
            self.client.send_event("info", &format!("[DRY RUN] 매도: {} {}주", stock_name, quantity)).await.ok();
            return Ok(());
        }
        
        // 현재가 조회
        let price = self.api.get_stock_price(stock_code).await?;
        let current_price = price.current_price.parse::<f64>().unwrap_or(0.0);
        
        // 시장가 매도
        match self.api.sell_stock(stock_code, quantity, 0).await {
            Ok(order_result) => {
                // 웹에 거래 기록
                let trade = pekko_ml4t_system::cloudflare::Trade {
                    timestamp: Local::now().to_rfc3339(),
                    stock_code: stock_code.to_string(),
                    stock_name: stock_name.to_string(),
                    action: "SELL".to_string(),
                    quantity: quantity as i32,
                    price: current_price,
                    reason: reason.to_string(),
                };
                
                let request = SyncRequest {
                    positions: None,
                    today_trades: Some(vec![trade]),
                    today_profit_rate: None,
                    status_message: Some(format!("매도 완료: {}", stock_name)),
                    event: Some(pekko_ml4t_system::cloudflare::TradingEvent {
                        timestamp: Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                        level: "info".to_string(),
                        message: format!("매도: {} {}주 @{:.0}원", stock_name, quantity, current_price),
                    }),
                };
                self.client.sync_state(&request).await.ok();
            }
            Err(e) => {
                error!("❌ 매도 주문 실패: {}", e);
                self.client.send_event("error", &format!("매도 실패: {} - {}", stock_name, e)).await.ok();
            }
        }
        
        Ok(())
    }
    
    /// 트레이딩 루프 (1분 간격)
    async fn trading_loop(&self) {
        let mut interval = interval(Duration::from_secs(60));
        
        loop {
            interval.tick().await;
            
            let is_active = match self.poll_state().await {
                Ok(active) => active,
                Err(e) => {
                    warn!("상태 폴링 실패: {}", e);
                    continue;
                }
            };
            
            if !is_active {
                continue;
            }
            
            // 장 시간 체크 (09:00 ~ 15:30)
            let now = Local::now();
            let hour = now.hour();
            let minute = now.minute();
            let time_num = hour * 100 + minute;
            
            if time_num < 900 || time_num > 1530 {
                continue;
            }
            // 포지션 업데이트
            if let Err(e) = self.update_positions().await {
                warn!("포지션 업데이트 실패: {}", e);
                continue;
            }
            
            // 매도 신호 체크
            match self.check_sell_signals().await {
                Ok(signals) => {
                    for (code, name, qty, reason) in signals {
                        if let Err(e) = self.execute_sell(&code, &name, qty, &reason).await {
                            error!("매도 실행 실패: {}", e);
                        }
                    }
                }
                Err(e) => {
                    warn!("매도 신호 체크 실패: {}", e);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 로깅 설정
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .init();
    
    // 환경 변수에서 URL 로드
    dotenv::dotenv().ok();
    let base_url = env::var("STOCKPICK_URL")
        .unwrap_or_else(|_| "https://stock-monitor-8yp.pages.dev".to_string());
    
    let dry_run = env::var("DRY_RUN")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(true);  // 기본값: dry run
    let client = StockPickClient::new(&base_url);
    
    // 시작 이벤트 전송
    client.send_event("info", "pekko-rust 연결됨").await.ok();
    
    // 트레이딩 시스템 초기화
    let system = match TradingSystem::new(client.clone(), dry_run).await {
        Ok(s) => s,
        Err(e) => {
            error!("❌ 트레이딩 시스템 초기화 실패: {}", e);
            client.send_event("error", &format!("초기화 실패: {}", e)).await.ok();
            return Err(e);
        }
    };
    // 트레이딩 루프 시작
    let trading_handle = tokio::spawn(async move {
        system.trading_loop().await;
    });
    
    // Ctrl+C 대기
    tokio::signal::ctrl_c().await?;
    client.send_event("info", "pekko-rust 연결 종료").await.ok();
    
    trading_handle.abort();
    
    Ok(())
}

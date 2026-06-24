use crate::kiwoom_real_trading::{KiwoomRealTradingClient, RealTradingResult, AccountBalance, HoldingStock};
use crate::user_account::{UserAccount, TradingStrategy, TradingSignal};
use pekko_actor::{Actor, ActorContext};
use async_trait::async_trait;
use std::collections::HashMap;
use tracing::{ warn, error};
use anyhow::Result;

/// 실매매 연동 액터
#[derive(Debug, Clone)]
pub struct RealTradingActor {
    kiwoom_client: KiwoomRealTradingClient,
    user_accounts: HashMap<String, UserAccount>,
    trading_strategies: HashMap<String, TradingStrategy>,
    is_active: bool,
    last_sync_time: Option<chrono::DateTime<chrono::Utc>>,
}

impl RealTradingActor {
    pub fn new(
        app_key: String, 
        app_secret: String, 
        account_number: Option<String>, 
        is_simulation: bool
    ) -> Self {
        Self {
            kiwoom_client: KiwoomRealTradingClient::new(app_key, app_secret, account_number, is_simulation),
            user_accounts: HashMap::new(),
            trading_strategies: HashMap::new(),
            is_active: false,
            last_sync_time: None,
        }
    }

    /// 실매매 시스템 활성화
    pub async fn activate(&mut self) -> Result<()> {
        // 액세스 토큰 발급
        self.kiwoom_client.get_access_token().await?;
        
        // 계좌 정보 동기화
        self.sync_account_info().await?;
        
        self.is_active = true;
        Ok(())
    }

    /// 계좌 정보 동기화
    pub async fn sync_account_info(&mut self) -> Result<()> {
        // 계좌 잔고 조회
        let balance = self.kiwoom_client.get_account_balance().await?;
        
        // 보유 종목 조회
        let holdings = self.kiwoom_client.get_holding_stocks().await?;
        self.last_sync_time = Some(chrono::Utc::now());
        Ok(())
    }

    /// 실매매 주문 실행
    pub async fn execute_real_order(&self, signal: TradingSignal) -> Result<RealTradingResult> {
        if !self.is_active {
            return Err(anyhow::anyhow!("실매매 시스템이 비활성화 상태입니다"));
        }

        match signal {
            TradingSignal::Buy { symbol, quantity, price, .. } => {
                let price_i32 = (price * 100.0) as i32; // 원 단위를 전 단위로 변환
                self.kiwoom_client.buy_stock(&symbol, quantity, price_i32).await
            }
            TradingSignal::Sell { symbol, quantity, price, .. } => {
                let price_i32 = (price * 100.0) as i32;
                self.kiwoom_client.sell_stock(&symbol, quantity, price_i32).await
            }
            _ => Err(anyhow::anyhow!("지원하지 않는 매매 신호입니다"))
        }
    }

    /// 실매매 상태 확인
    pub fn get_trading_status(&self) -> TradingStatus {
        TradingStatus {
            is_active: self.is_active,
            is_simulation: self.kiwoom_client.is_simulation(),
            is_token_valid: self.kiwoom_client.is_token_valid(),
            last_sync_time: self.last_sync_time,
            account_count: self.user_accounts.len(),
        }
    }

    /// 안전 장치 확인
    fn check_safety_limits(&self, signal: &TradingSignal) -> Result<()> {
        match signal {
            TradingSignal::Buy { quantity, .. } => {
                if *quantity > 1000 {
                    return Err(anyhow::anyhow!("안전 장치: 한 번에 1000주 이상 매수 불가"));
                }
            }
            TradingSignal::Sell { quantity, .. } => {
                if *quantity > 1000 {
                    return Err(anyhow::anyhow!("안전 장치: 한 번에 1000주 이상 매도 불가"));
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// 실매매 로그 기록
    fn log_real_trading(&self, signal: &TradingSignal, result: &RealTradingResult) {
        let trading_type = match signal {
            TradingSignal::Buy { .. } => "매수",
            TradingSignal::Sell { .. } => "매도",
            _ => "기타",
        };

        if result.success {
        } else {
            error!("❌ 실매매 실패: {} - {}", trading_type, result.message);
        }
    }
}

#[derive(Debug, Clone)]
pub struct TradingStatus {
    pub is_active: bool,
    pub is_simulation: bool,
    pub is_token_valid: bool,
    pub last_sync_time: Option<chrono::DateTime<chrono::Utc>>,
    pub account_count: usize,
}

/// 실매매 메시지
#[derive(Debug, Clone)]
pub enum RealTradingMessage {
    Activate,
    SyncAccount,
    ExecuteOrder { signal: TradingSignal },
    GetStatus,
    Deactivate,
}

#[async_trait]
impl Actor for RealTradingActor {
    type Message = RealTradingMessage;

    async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
        match msg {
            RealTradingMessage::Activate => {
                match self.activate().await {
                    Ok(_) => (),
                    Err(e) => error!("실매매 시스템 활성화 실패: {}", e),
                }
            }
            
            RealTradingMessage::SyncAccount => {
                match self.sync_account_info().await {
                    Ok(_) => (),
                    Err(e) => error!("계좌 정보 동기화 실패: {}", e),
                }
            }
            
            RealTradingMessage::ExecuteOrder { signal } => {
                // 안전 장치 확인
                if let Err(e) = self.check_safety_limits(&signal) {
                    error!("안전 장치 위반: {}", e);
                    return;
                }

                match self.execute_real_order(signal.clone()).await {
                    Ok(result) => {
                        self.log_real_trading(&signal, &result);
                        
                        if result.success {
                        } else {
                            warn!("실매매 주문 실패: {}", result.message);
                        }
                    }
                    Err(e) => error!("실매매 주문 실행 오류: {}", e),
                }
            }
            
            RealTradingMessage::GetStatus => {
                let status = self.get_trading_status();
            }
            
            RealTradingMessage::Deactivate => {
                self.is_active = false;
            }
        }
    }

    async fn pre_start(&mut self) {
    }

    async fn post_stop(&mut self) {
    }
}

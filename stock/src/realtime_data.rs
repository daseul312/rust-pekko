use crate::kiwoom_api::{KiwoomApiClient, convert_korean_stock_to_international};
use crate::models::{Stock, StockRecommendationMessage, RiskLevel};
use pekko_actor::{Actor, ActorContext};
use async_trait::async_trait;
use std::collections::HashMap;
use tracing::{ warn, error};
use tokio::time::{interval, Duration};
use anyhow::Result;

/// 실시간 데이터 연동 액터
#[derive(Debug, Clone)]
pub struct RealTimeDataActor {
    kiwoom_client: KiwoomApiClient,
    stock_cache: HashMap<String, Stock>,
    last_update: Option<chrono::DateTime<chrono::Utc>>,
    update_interval: Duration,
}

impl RealTimeDataActor {
    pub fn new(app_key: String, app_secret: String) -> Self {
        Self {
            kiwoom_client: KiwoomApiClient::new(app_key, app_secret),
            stock_cache: HashMap::new(),
            last_update: None,
            update_interval: Duration::from_secs(60), // 1분마다 업데이트
        }
    }

    /// 실시간 데이터 업데이트
    async fn update_real_time_data(&mut self) -> Result<()> {
        // 토큰이 없으면 발급
        if !self.kiwoom_client.is_token_valid() {
            self.kiwoom_client.get_access_token().await?;
        }

        // 저위험 주식 목록 조회
        match self.kiwoom_client.get_low_risk_stocks().await {
            Ok(korean_stocks) => {
                let mut updated_count = 0;
                
                for korean_stock in korean_stocks {
                    let international_stock = convert_korean_stock_to_international(korean_stock);
                    let symbol = international_stock.symbol.clone();
                    
                    self.stock_cache.insert(symbol, international_stock);
                    updated_count += 1;
                }
                
                self.last_update = Some(chrono::Utc::now());
            }
            Err(e) => {
                error!("실시간 데이터 업데이트 실패: {}", e);
                return Err(e);
            }
        }

        Ok(())
    }

    /// 캐시된 주식 정보 조회
    fn get_cached_stock(&self, symbol: &str) -> Option<&Stock> {
        self.stock_cache.get(symbol)
    }

    /// 모든 캐시된 주식 정보 조회
    fn get_all_cached_stocks(&self) -> Vec<Stock> {
        self.stock_cache.values().cloned().collect()
    }

    /// 저위험 주식 필터링
    fn get_low_risk_cached_stocks(&self) -> Vec<Stock> {
        self.stock_cache
            .values()
            .filter(|stock| stock.is_low_risk())
            .cloned()
            .collect()
    }

    /// 섹터별 주식 조회
    fn get_stocks_by_sector(&self, sector: &str) -> Vec<Stock> {
        self.stock_cache
            .values()
            .filter(|stock| stock.sector.to_lowercase() == sector.to_lowercase())
            .cloned()
            .collect()
    }

    /// 시장 상황 분석
    async fn analyze_market_conditions(&self) -> Result<MarketAnalysis> {
        // KOSPI 지수 조회
        let kospi_index = match self.kiwoom_client.get_market_index("J").await {
            Ok(index) => Some(index),
            Err(e) => {
                warn!("KOSPI 지수 조회 실패: {}", e);
                None
            }
        };

        // KOSDAQ 지수 조회
        let kosdaq_index = match self.kiwoom_client.get_market_index("Q").await {
            Ok(index) => Some(index),
            Err(e) => {
                warn!("KOSDAQ 지수 조회 실패: {}", e);
                None
            }
        };

        Ok(MarketAnalysis {
            kospi_index,
            kosdaq_index,
            analysis_time: chrono::Utc::now(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct MarketAnalysis {
    pub kospi_index: Option<crate::kiwoom_api::KiwoomMarketIndex>,
    pub kosdaq_index: Option<crate::kiwoom_api::KiwoomMarketIndex>,
    pub analysis_time: chrono::DateTime<chrono::Utc>,
}

/// 실시간 데이터 요청 메시지
#[derive(Debug, Clone)]
pub enum RealTimeDataMessage {
    UpdateData,
    GetStockInfo(String),
    GetLowRiskStocks,
    GetStocksBySector(String),
    AnalyzeMarket,
    GetMarketAnalysis,
}

#[async_trait]
impl Actor for RealTimeDataActor {
    type Message = RealTimeDataMessage;

    async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
        match msg {
            RealTimeDataMessage::UpdateData => {
                match self.update_real_time_data().await {
                    Ok(_) => (),
                    Err(e) => error!("실시간 데이터 업데이트 실패: {}", e),
                }
            }
            
            RealTimeDataMessage::GetStockInfo(symbol) => {
                if let Some(stock) = self.get_cached_stock(&symbol) {
                } else {
                    warn!("주식 정보를 찾을 수 없음: {}", symbol);
                }
            }
            
            RealTimeDataMessage::GetLowRiskStocks => {
                let low_risk_stocks = self.get_low_risk_cached_stocks();
            }
            
            RealTimeDataMessage::GetStocksBySector(sector) => {
                let sector_stocks = self.get_stocks_by_sector(&sector);
            }
            
            RealTimeDataMessage::AnalyzeMarket => {
                match self.analyze_market_conditions().await {
                    Ok(analysis) => {
                        if let Some(kospi) = analysis.kospi_index {
                        }
                        if let Some(kosdaq) = analysis.kosdaq_index {
                        }
                    }
                    Err(e) => error!("시장 분석 실패: {}", e),
                }
            }
            
            RealTimeDataMessage::GetMarketAnalysis => {
                // 시장 분석 결과를 캐시에 저장하거나 반환
            }
        }
    }

    async fn pre_start(&mut self) {
        // 초기 데이터 로드
        match self.update_real_time_data().await {
            Ok(_) => (),
            Err(e) => error!("초기 데이터 로드 실패: {}", e),
        }
    }

    async fn post_stop(&mut self) {
    }
}

/// 실시간 데이터 모니터링 서비스
pub struct RealTimeDataService {
    data_actor_ref: Option<pekko_actor::ActorRef<RealTimeDataActor>>,
}

impl RealTimeDataService {
    pub fn new() -> Self {
        Self {
            data_actor_ref: None,
        }
    }

    pub fn set_data_actor_ref(&mut self, actor_ref: pekko_actor::ActorRef<RealTimeDataActor>) {
        self.data_actor_ref = Some(actor_ref);
    }

    /// 주기적 데이터 업데이트 시작
    pub async fn start_periodic_update(&self) {
        if let Some(actor_ref) = &self.data_actor_ref {
            let mut interval = interval(Duration::from_secs(60));
            
            loop {
                interval.tick().await;
                actor_ref.tell(RealTimeDataMessage::UpdateData);
            }
        }
    }

    /// 시장 분석 요청
    pub async fn request_market_analysis(&self) {
        if let Some(actor_ref) = &self.data_actor_ref {
            actor_ref.tell(RealTimeDataMessage::AnalyzeMarket);
        }
    }
}

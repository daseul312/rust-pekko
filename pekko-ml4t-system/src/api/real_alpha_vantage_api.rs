use super::*;
use crate::config::api_config::{ApiConfig, AlphaVantageConfig};
use anyhow::{anyhow, Result};
use chrono::{Utc, DateTime, Datelike, Timelike};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use tracing::{ warn, error, debug};

/// 실제 Alpha Vantage API 클라이언트
#[derive(Debug, Clone)]
pub struct RealAlphaVantageApi {
    config: AlphaVantageConfig,
    client: Client,
    last_request_time: Option<DateTime<Utc>>,
}

/// Alpha Vantage 주식 시세 응답
#[derive(Debug, Deserialize)]
struct AlphaVantageQuoteResponse {
    #[serde(rename = "Global Quote")]
    global_quote: Option<AlphaVantageQuote>,
    #[serde(rename = "Error Message")]
    error_message: Option<String>,
    #[serde(rename = "Note")]
    note: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AlphaVantageQuote {
    #[serde(rename = "01. symbol")]
    symbol: String,
    #[serde(rename = "02. open")]
    open: String,
    #[serde(rename = "03. high")]
    high: String,
    #[serde(rename = "04. low")]
    low: String,
    #[serde(rename = "05. price")]
    price: String,
    #[serde(rename = "06. volume")]
    volume: String,
    #[serde(rename = "07. latest trading day")]
    latest_trading_day: String,
    #[serde(rename = "08. previous close")]
    previous_close: String,
    #[serde(rename = "09. change")]
    change: String,
    #[serde(rename = "10. change percent")]
    change_percent: String,
}

/// Alpha Vantage 기술적 지표 응답
#[derive(Debug, Deserialize)]
struct AlphaVantageTechnicalResponse {
    #[serde(rename = "Meta Data")]
    meta_data: Option<AlphaVantageMetaData>,
    #[serde(rename = "Technical Analysis: RSI")]
    technical_analysis: Option<HashMap<String, AlphaVantageTechnicalData>>,
    #[serde(rename = "Error Message")]
    error_message: Option<String>,
    #[serde(rename = "Note")]
    note: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AlphaVantageMetaData {
    #[serde(rename = "1: Symbol")]
    symbol: String,
    #[serde(rename = "2: Indicator")]
    indicator: String,
    #[serde(rename = "3: Last Refreshed")]
    last_refreshed: String,
    #[serde(rename = "4: Interval")]
    interval: String,
    #[serde(rename = "5: Time Period")]
    time_period: String,
    #[serde(rename = "6: Series Type")]
    series_type: String,
    #[serde(rename = "7: Time Zone")]
    time_zone: String,
}

#[derive(Debug, Deserialize)]
struct AlphaVantageTechnicalData {
    #[serde(rename = "RSI")]
    rsi: String,
}

impl RealAlphaVantageApi {
    /// 새로운 Alpha Vantage API 클라이언트 생성
    pub fn new(config: AlphaVantageConfig) -> Self {
        let client = Client::new();
        Self {
            config,
            client,
            last_request_time: None,
        }
    }

    /// API 요청 제한을 확인하고 대기
    async fn check_rate_limit(&mut self) -> Result<()> {
        if let Some(last_request) = self.last_request_time {
            let time_since_last = Utc::now() - last_request;
            let min_interval = std::time::Duration::from_secs(60 / self.config.rate_limit_per_minute as u64);
            let min_interval_delta = chrono::Duration::from_std(min_interval).unwrap();
            
            if time_since_last < min_interval_delta {
                let wait_time = min_interval_delta - time_since_last;
                let wait_duration = wait_time.to_std().unwrap();
                debug!("API 요청 제한으로 대기: {:?}", wait_time);
                sleep(wait_duration).await;
            }
        }
        self.last_request_time = Some(Utc::now());
        Ok(())
    }

    /// 기본 API 파라미터 생성
    fn get_base_params(&self) -> HashMap<String, String> {
        let mut params = HashMap::new();
        params.insert("apikey".to_string(), self.config.api_key.clone());
        params.insert("datatype".to_string(), "json".to_string());
        params
    }
}

#[async_trait::async_trait]
impl ApiClient for RealAlphaVantageApi {
    type Config = AlphaVantageConfig;
    type Response = MarketDataResponse;

    async fn new(config: Self::Config) -> Result<Self> {
        Ok(Self::new(config))
    }

    async fn authenticate(&mut self) -> Result<()> {
        // Alpha Vantage는 API 키만으로 인증
        Ok(())
    }

    async fn get_market_data(&self, symbol: &str) -> Result<MarketDataResponse> {
        let mut client = self.clone();
        client.check_rate_limit().await?;
        
        let url = &self.config.base_url;
        let mut params = client.get_base_params();
        params.insert("function".to_string(), "GLOBAL_QUOTE".to_string());
        params.insert("symbol".to_string(), symbol.to_string());

        let response = self.client
            .get(url)
            .query(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(anyhow!("Alpha Vantage API 요청 실패: {}", error_text));
        }

        let quote_response: AlphaVantageQuoteResponse = response.json().await?;

        // 에러 메시지 확인
        if let Some(error_msg) = quote_response.error_message {
            return Err(anyhow!("Alpha Vantage API 에러: {}", error_msg));
        }

        // API 제한 메시지 확인
        if let Some(note) = quote_response.note {
            warn!("Alpha Vantage API 제한: {}", note);
            return Err(anyhow!("API 요청 제한: {}", note));
        }

        let quote = quote_response.global_quote
            .ok_or_else(|| anyhow!("시세 데이터가 없습니다"))?;

        // 가격 문자열을 f64로 변환
        let price = quote.price.parse::<f64>()
            .map_err(|_| anyhow!("가격 파싱 실패: {}", quote.price))?;
        let open = quote.open.parse::<f64>()
            .map_err(|_| anyhow!("시가 파싱 실패: {}", quote.open))?;
        let high = quote.high.parse::<f64>()
            .map_err(|_| anyhow!("고가 파싱 실패: {}", quote.high))?;
        let low = quote.low.parse::<f64>()
            .map_err(|_| anyhow!("저가 파싱 실패: {}", quote.low))?;
        let volume = quote.volume.parse::<u64>()
            .map_err(|_| anyhow!("거래량 파싱 실패: {}", quote.volume))?;

        Ok(MarketDataResponse {
            symbol: symbol.to_string(),
            price,
            volume,
            timestamp: Utc::now(),
            open: Some(open),
            high: Some(high),
            low: Some(low),
            close: Some(price),
        })
    }

    async fn get_account_info(&self) -> Result<AccountInfo> {
        // Alpha Vantage는 시장 데이터만 제공하므로 Mock 데이터 반환
        warn!("Alpha Vantage API는 계좌 정보를 제공하지 않습니다");
        Ok(AccountInfo {
            account_id: "alpha_vantage_mock".to_string(),
            total_balance: 1_000_000.0,
            cash_balance: 1_000_000.0,
            positions: Vec::new(),
        })
    }

    async fn place_order(&self, _order: &OrderRequest) -> Result<OrderResponse> {
        Err(anyhow!("Alpha Vantage API는 주문 기능을 제공하지 않습니다"))
    }

    async fn get_positions(&self) -> Result<Vec<Position>> {
        // Alpha Vantage는 포지션 정보를 제공하지 않음
        Ok(Vec::new())
    }

    async fn get_order_status(&self, _order_id: &str) -> Result<OrderStatus> {
        Err(anyhow!("Alpha Vantage API는 주문 상태 조회 기능을 제공하지 않습니다"))
    }
}

/// Alpha Vantage API 확장 기능
impl RealAlphaVantageApi {
    /// RSI 기술적 지표 조회
    pub async fn get_rsi(&mut self, symbol: &str, period: u32) -> Result<f64> {
        self.check_rate_limit().await?;
        
        let url = &self.config.base_url;
        let mut params = self.get_base_params();
        params.insert("function".to_string(), "RSI".to_string());
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("interval".to_string(), "daily".to_string());
        params.insert("time_period".to_string(), period.to_string());
        params.insert("series_type".to_string(), "close".to_string());

        let response = self.client
            .get(url)
            .query(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(anyhow!("Alpha Vantage RSI 요청 실패: {}", error_text));
        }

        let rsi_response: AlphaVantageTechnicalResponse = response.json().await?;

        if let Some(error_msg) = rsi_response.error_message {
            return Err(anyhow!("Alpha Vantage RSI API 에러: {}", error_msg));
        }

        if let Some(note) = rsi_response.note {
            warn!("Alpha Vantage RSI API 제한: {}", note);
            return Err(anyhow!("RSI API 요청 제한: {}", note));
        }

        let technical_analysis = rsi_response.technical_analysis
            .ok_or_else(|| anyhow!("RSI 데이터가 없습니다"))?;

        // 가장 최근 RSI 값 가져오기
        let latest_date = technical_analysis.keys()
            .max()
            .ok_or_else(|| anyhow!("RSI 날짜 데이터가 없습니다"))?;

        let latest_rsi = technical_analysis.get(latest_date)
            .ok_or_else(|| anyhow!("RSI 값이 없습니다"))?;

        latest_rsi.rsi.parse::<f64>()
            .map_err(|_| anyhow!("RSI 파싱 실패: {}", latest_rsi.rsi))
    }

    /// 여러 종목의 시세를 한 번에 조회
    pub async fn get_multiple_quotes(&mut self, symbols: &[String]) -> Result<Vec<MarketDataResponse>> {
        let mut results = Vec::new();
        
        for symbol in symbols {
            match self.get_market_data(symbol).await {
                Ok(data) => results.push(data),
                Err(e) => {
                    error!("{} 시세 조회 실패: {}", symbol, e);
                    // 실패한 종목은 건너뛰고 계속 진행
                }
            }
        }

        Ok(results)
    }

    /// 시장 상태 확인 (개장/폐장)
    pub async fn is_market_open(&self) -> Result<bool> {
        // 간단한 구현: 평일 09:30-16:00 EST (뉴욕 시간)
        let now = Utc::now();
        let weekday = now.weekday();
        let hour = now.hour();

        // 주말이 아닌 경우
        let is_weekday = weekday != chrono::Weekday::Sat && weekday != chrono::Weekday::Sun;
        
        // 장중 시간 (EST 기준, UTC로 변환 필요)
        // 실제로는 timezone 라이브러리를 사용해야 함
        let is_market_hours = hour >= 14 && hour < 21; // 간단한 UTC 기준

        Ok(is_weekday && is_market_hours)
    }
}

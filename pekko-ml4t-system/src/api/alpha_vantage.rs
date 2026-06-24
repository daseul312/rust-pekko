//! Alpha Vantage API 클라이언트
//! 
//! 미국 주식 시장 데이터 및 기술적 지표를 제공하는 API

use super::{
    ApiClient, ApiConfig, MarketDataResponse, AccountInfo, OrderRequest, 
    OrderResponse, Position, OrderStatus, ApiError
};
use reqwest::Client;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use anyhow::Result;
use chrono::{DateTime, Utc};
use tracing::{ warn, error};

/// Alpha Vantage API 클라이언트
#[derive(Clone)]
pub struct AlphaVantageClient {
    client: Client,
    config: ApiConfig,
}

/// Alpha Vantage 주식 데이터 응답
#[derive(Debug, Deserialize)]
struct AlphaVantageResponse {
    #[serde(rename = "Time Series (Daily)")]
    time_series: Option<HashMap<String, DailyData>>,
    
    #[serde(rename = "Global Quote")]
    global_quote: Option<QuoteData>,
    
    #[serde(rename = "Error Message")]
    error_message: Option<String>,
    
    #[serde(rename = "Note")]
    note: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DailyData {
    #[serde(rename = "1. open")]
    open: String,
    #[serde(rename = "2. high")]
    high: String,
    #[serde(rename = "3. low")]
    low: String,
    #[serde(rename = "4. close")]
    close: String,
    #[serde(rename = "5. volume")]
    volume: String,
}

#[derive(Debug, Deserialize)]
struct QuoteData {
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
struct TechnicalIndicatorResponse {
    #[serde(rename = "Technical Analysis: SMA")]
    sma: Option<HashMap<String, IndicatorData>>,
    
    #[serde(rename = "Technical Analysis: EMA")]
    ema: Option<HashMap<String, IndicatorData>>,
    
    #[serde(rename = "Technical Analysis: RSI")]
    rsi: Option<HashMap<String, IndicatorData>>,
    
    #[serde(rename = "Technical Analysis: MACD")]
    macd: Option<HashMap<String, MacdData>>,
    
    #[serde(rename = "Error Message")]
    error_message: Option<String>,
    
    #[serde(rename = "Note")]
    note: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IndicatorData {
    #[serde(rename = "SMA")]
    sma: Option<String>,
    #[serde(rename = "EMA")]
    ema: Option<String>,
    #[serde(rename = "RSI")]
    rsi: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MacdData {
    #[serde(rename = "MACD")]
    macd: String,
    #[serde(rename = "MACD_Signal")]
    macd_signal: String,
    #[serde(rename = "MACD_Hist")]
    macd_hist: String,
}

#[async_trait::async_trait]
impl ApiClient for AlphaVantageClient {
    type Config = ApiConfig;
    type Response = MarketDataResponse;

    async fn new(config: Self::Config) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_millis(config.timeout_ms))
            .build()?;

        Ok(Self {
            client,
            config,
        })
    }

    async fn authenticate(&mut self) -> Result<()> {
        // Alpha Vantage는 API 키만으로 인증
        if self.config.api_key.is_empty() {
            return Err(ApiError::AuthenticationError("API 키가 필요합니다".to_string()).into());
        }
        Ok(())
    }

    async fn get_market_data(&self, symbol: &str) -> Result<MarketDataResponse> {
        let url = format!("{}/query", self.config.base_url);
        
        let params = [
            ("function", "GLOBAL_QUOTE"),
            ("symbol", symbol),
            ("apikey", &self.config.api_key),
        ];

        let response = self.client
            .get(&url)
            .query(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(ApiError::NetworkError(error_text).into());
        }

        let quote_response: AlphaVantageResponse = response.json().await?;
        
        if let Some(error_msg) = quote_response.error_message {
            return Err(ApiError::ParseError(error_msg).into());
        }
        
        if let Some(note) = quote_response.note {
            warn!("Alpha Vantage API 제한: {}", note);
            return Err(ApiError::RateLimitError(note).into());
        }

        if let Some(quote) = quote_response.global_quote {
            let current_price = quote.price.parse::<f64>()?;
            let open_price = quote.open.parse::<f64>()?;
            let high_price = quote.high.parse::<f64>()?;
            let low_price = quote.low.parse::<f64>()?;
            let volume = quote.volume.parse::<u64>()?;

            Ok(MarketDataResponse {
                symbol: quote.symbol,
                timestamp: Utc::now(),
                price: current_price,
                volume,
                open: Some(open_price),
                high: Some(high_price),
                low: Some(low_price),
                close: Some(current_price),
            })
        } else {
            Err(ApiError::ParseError("주가 데이터가 없습니다".to_string()).into())
        }
    }

    async fn get_account_info(&self) -> Result<AccountInfo> {
        // Alpha Vantage는 데이터 제공 API이므로 계좌 정보 없음
        Err(ApiError::AccountError("Alpha Vantage는 계좌 정보를 제공하지 않습니다".to_string()).into())
    }

    async fn place_order(&self, _order: &OrderRequest) -> Result<OrderResponse> {
        // Alpha Vantage는 거래 API가 아님
        Err(ApiError::OrderError("Alpha Vantage는 거래 API가 아닙니다".to_string()).into())
    }

    async fn get_positions(&self) -> Result<Vec<Position>> {
        // Alpha Vantage는 포지션 정보 없음
        Err(ApiError::AccountError("Alpha Vantage는 포지션 정보를 제공하지 않습니다".to_string()).into())
    }

    async fn get_order_status(&self, _order_id: &str) -> Result<OrderStatus> {
        // Alpha Vantage는 주문 상태 정보 없음
        Err(ApiError::OrderError("Alpha Vantage는 주문 상태를 제공하지 않습니다".to_string()).into())
    }
}

impl AlphaVantageClient {
    /// 기술적 지표 조회
    pub async fn get_technical_indicator(
        &self,
        symbol: &str,
        indicator: &str,
        interval: &str,
        time_period: u32,
    ) -> Result<HashMap<String, f64>> {
        let url = format!("{}/query", self.config.base_url);
        
        let params = [
            ("function", indicator),
            ("symbol", symbol),
            ("interval", interval),
            ("time_period", &time_period.to_string()),
            ("series_type", "close"),
            ("apikey", &self.config.api_key),
        ];

        let response = self.client
            .get(&url)
            .query(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(ApiError::NetworkError(error_text).into());
        }

        let indicator_response: TechnicalIndicatorResponse = response.json().await?;
        
        if let Some(error_msg) = indicator_response.error_message {
            return Err(ApiError::ParseError(error_msg).into());
        }
        
        if let Some(note) = indicator_response.note {
            warn!("Alpha Vantage API 제한: {}", note);
            return Err(ApiError::RateLimitError(note).into());
        }

        let mut results = HashMap::new();
        
        match indicator {
            "SMA" => {
                if let Some(sma_data) = indicator_response.sma {
                    for (date, data) in sma_data {
                        if let Some(sma_value) = data.sma {
                            if let Ok(value) = sma_value.parse::<f64>() {
                                results.insert(date, value);
                            }
                        }
                    }
                }
            }
            "EMA" => {
                if let Some(ema_data) = indicator_response.ema {
                    for (date, data) in ema_data {
                        if let Some(ema_value) = data.ema {
                            if let Ok(value) = ema_value.parse::<f64>() {
                                results.insert(date, value);
                            }
                        }
                    }
                }
            }
            "RSI" => {
                if let Some(rsi_data) = indicator_response.rsi {
                    for (date, data) in rsi_data {
                        if let Some(rsi_value) = data.rsi {
                            if let Ok(value) = rsi_value.parse::<f64>() {
                                results.insert(date, value);
                            }
                        }
                    }
                }
            }
            _ => {
                return Err(ApiError::ParseError(format!("지원하지 않는 지표: {}", indicator)).into());
            }
        }
        
        Ok(results)
    }

    /// 여러 기술적 지표를 한번에 조회
    pub async fn get_multiple_indicators(
        &self,
        symbol: &str,
    ) -> Result<HashMap<String, f64>> {
        let mut all_indicators = HashMap::new();
        
        // SMA 20일
        if let Ok(sma_20) = self.get_technical_indicator(symbol, "SMA", "daily", 20).await {
            if let Some(latest_value) = sma_20.values().next() {
                all_indicators.insert("sma_20".to_string(), *latest_value);
            }
        }
        
        // EMA 12일
        if let Ok(ema_12) = self.get_technical_indicator(symbol, "EMA", "daily", 12).await {
            if let Some(latest_value) = ema_12.values().next() {
                all_indicators.insert("ema_12".to_string(), *latest_value);
            }
        }
        
        // RSI 14일
        if let Ok(rsi_14) = self.get_technical_indicator(symbol, "RSI", "daily", 14).await {
            if let Some(latest_value) = rsi_14.values().next() {
                all_indicators.insert("rsi_14".to_string(), *latest_value);
            }
        }
        
        Ok(all_indicators)
    }

    /// 주식 검색
    pub async fn search_symbol(&self, keywords: &str) -> Result<Vec<String>> {
        let url = format!("{}/query", self.config.base_url);
        
        let params = [
            ("function", "SYMBOL_SEARCH"),
            ("keywords", keywords),
            ("apikey", &self.config.api_key),
        ];

        let response = self.client
            .get(&url)
            .query(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(ApiError::NetworkError(error_text).into());
        }

        // TODO: 실제 검색 결과 파싱
        Ok(vec![keywords.to_string()])
    }
}

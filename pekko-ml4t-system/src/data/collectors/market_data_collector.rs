//! 시장 데이터 수집기
//! 
//! 다양한 데이터 소스로부터 시장 데이터를 수집합니다:
//! - KIS API (한국투자증권)
//! - Alpha Vantage API
//! - Yahoo Finance API
//! - 기타 금융 데이터 제공업체

use super::{DataCollector, CollectParams, CollectionStats, HealthStatus, BaseCollector};
use crate::data::{MarketData, DataSourceConfig};
use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{ warn, error, debug};

/// 시장 데이터 수집기
pub struct MarketDataCollector {
    base: BaseCollector,
    source_type: MarketDataSource,
    api_keys: HashMap<String, String>,
}

/// 시장 데이터 소스 타입
#[derive(Debug, Clone)]
pub enum MarketDataSource {
    KIS,
    AlphaVantage,
    YahooFinance,
    IEXCloud,
    Polygon,
    Quandl,
}

/// KIS API 응답 구조
#[derive(Debug, Deserialize)]
struct KISResponse {
    rt_cd: String,
    msg_cd: String,
    msg1: String,
    output: Option<Vec<KISMarketData>>,
}

#[derive(Debug, Deserialize)]
struct KISMarketData {
    stck_bsop_date: String,
    stck_clpr: String,
    stck_oprc: String,
    stck_hgpr: String,
    stck_lwpr: String,
    acml_vol: String,
    acml_tr_pbmn: String,
}

/// Alpha Vantage API 응답 구조
#[derive(Debug, Deserialize)]
struct AlphaVantageResponse {
    #[serde(rename = "Time Series (Daily)")]
    time_series: Option<HashMap<String, AlphaVantageData>>,
    #[serde(rename = "Error Message")]
    error_message: Option<String>,
    #[serde(rename = "Note")]
    note: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AlphaVantageData {
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

/// Yahoo Finance API 응답 구조
#[derive(Debug, Deserialize)]
struct YahooFinanceResponse {
    chart: YahooChart,
}

#[derive(Debug, Deserialize)]
struct YahooChart {
    result: Option<Vec<YahooResult>>,
    error: Option<YahooError>,
}

#[derive(Debug, Deserialize)]
struct YahooResult {
    meta: YahooMeta,
    timestamp: Vec<i64>,
    indicators: YahooIndicators,
}

#[derive(Debug, Deserialize)]
struct YahooMeta {
    currency: String,
    symbol: String,
    exchangeName: String,
    instrumentType: String,
    firstTradeDate: i64,
    regularMarketTime: i64,
    gmtoffset: i64,
    timezone: String,
    exchangeTimezoneName: String,
    regularMarketPrice: f64,
    chartPreviousClose: f64,
    priceHint: i64,
    currentTradingPeriod: YahooTradingPeriod,
}

#[derive(Debug, Deserialize)]
struct YahooTradingPeriod {
    pre: YahooPeriod,
    regular: YahooPeriod,
    post: YahooPeriod,
}

#[derive(Debug, Deserialize)]
struct YahooPeriod {
    timezone: String,
    start: i64,
    end: i64,
    gmtoffset: i64,
}

#[derive(Debug, Deserialize)]
struct YahooIndicators {
    quote: Vec<YahooQuote>,
}

#[derive(Debug, Deserialize)]
struct YahooQuote {
    open: Vec<Option<f64>>,
    high: Vec<Option<f64>>,
    low: Vec<Option<f64>>,
    close: Vec<Option<f64>>,
    volume: Vec<Option<u64>>,
}

#[derive(Debug, Deserialize)]
struct YahooError {
    code: String,
    description: String,
}

impl MarketDataCollector {
    /// KIS API 수집기 생성
    pub fn new_kis() -> Self {
        let config = DataSourceConfig {
            source_name: "KIS API".to_string(),
            data_type: crate::data::DataType::MarketData,
            api_config: crate::data::ApiConfig {
                base_url: "https://openapi.koreainvestment.com:9443".to_string(),
                api_key: None,
                api_secret: None,
                rate_limit: crate::data::RateLimitConfig {
                    requests_per_minute: 60,
                    requests_per_hour: 3600,
                    requests_per_day: 86400,
                    burst_limit: 10,
                },
                timeout_seconds: 30,
                headers: HashMap::new(),
            },
            update_frequency: crate::data::UpdateFrequency::RealTime,
            retry_config: crate::data::RetryConfig {
                max_retries: 3,
                initial_delay_ms: 1000,
                max_delay_ms: 10000,
                backoff_multiplier: 2.0,
                jitter: true,
            },
            quality_thresholds: crate::data::QualityThresholds {
                min_data_completeness: 0.95,
                max_latency_seconds: 5,
                max_price_deviation: 0.1,
                max_volume_spike: 5.0,
                min_confidence_score: 0.8,
            },
        };

        Self {
            base: BaseCollector::new(config),
            source_type: MarketDataSource::KIS,
            api_keys: HashMap::new(),
        }
    }

    /// Alpha Vantage API 수집기 생성
    pub fn new_alpha_vantage() -> Self {
        let config = DataSourceConfig {
            source_name: "Alpha Vantage".to_string(),
            data_type: crate::data::DataType::MarketData,
            api_config: crate::data::ApiConfig {
                base_url: "https://www.alphavantage.co".to_string(),
                api_key: None,
                api_secret: None,
                rate_limit: crate::data::RateLimitConfig {
                    requests_per_minute: 5,
                    requests_per_hour: 500,
                    requests_per_day: 5000,
                    burst_limit: 1,
                },
                timeout_seconds: 30,
                headers: HashMap::new(),
            },
            update_frequency: crate::data::UpdateFrequency::EveryMinute,
            retry_config: crate::data::RetryConfig {
                max_retries: 3,
                initial_delay_ms: 1000,
                max_delay_ms: 10000,
                backoff_multiplier: 2.0,
                jitter: true,
            },
            quality_thresholds: crate::data::QualityThresholds {
                min_data_completeness: 0.9,
                max_latency_seconds: 10,
                max_price_deviation: 0.2,
                max_volume_spike: 10.0,
                min_confidence_score: 0.7,
            },
        };

        Self {
            base: BaseCollector::new(config),
            source_type: MarketDataSource::AlphaVantage,
            api_keys: HashMap::new(),
        }
    }

    /// Yahoo Finance API 수집기 생성
    pub fn new_yahoo() -> Self {
        let config = DataSourceConfig {
            source_name: "Yahoo Finance".to_string(),
            data_type: crate::data::DataType::MarketData,
            api_config: crate::data::ApiConfig {
                base_url: "https://query1.finance.yahoo.com".to_string(),
                api_key: None,
                api_secret: None,
                rate_limit: crate::data::RateLimitConfig {
                    requests_per_minute: 60,
                    requests_per_hour: 3600,
                    requests_per_day: 86400,
                    burst_limit: 5,
                },
                timeout_seconds: 15,
                headers: HashMap::new(),
            },
            update_frequency: crate::data::UpdateFrequency::EveryMinute,
            retry_config: crate::data::RetryConfig {
                max_retries: 3,
                initial_delay_ms: 500,
                max_delay_ms: 5000,
                backoff_multiplier: 2.0,
                jitter: true,
            },
            quality_thresholds: crate::data::QualityThresholds {
                min_data_completeness: 0.95,
                max_latency_seconds: 5,
                max_price_deviation: 0.15,
                max_volume_spike: 8.0,
                min_confidence_score: 0.85,
            },
        };

        Self {
            base: BaseCollector::new(config),
            source_type: MarketDataSource::YahooFinance,
            api_keys: HashMap::new(),
        }
    }

    /// API 키 설정
    pub fn set_api_key(&mut self, key_name: &str, key_value: String) {
        self.api_keys.insert(key_name.to_string(), key_value);
    }

    /// KIS API로부터 데이터 수집
    fn collect_from_kis(&self, params: CollectParams) -> Result<Vec<MarketData>> {
        let symbol = params.symbol.ok_or_else(|| anyhow::anyhow!("심볼이 필요합니다"))?;
        
        // KIS API 엔드포인트 구성
        let url = format!("{}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", 
                         self.base.get_config().api_config.base_url);
        
        let mut query_params = HashMap::new();
        query_params.insert("fid_cond_mrkt_div_code", "J");
        query_params.insert("fid_input_iscd", &symbol);
        query_params.insert("fid_input_date_1", "20240101");
        query_params.insert("fid_input_date_2", "20241231");
        query_params.insert("fid_period_div_code", "D");
        query_params.insert("fid_org_adj_prc", "0");

        let mut request_builder = self.base.get_client()
            .get(&url)
            .query(&query_params);

        if let Some(access_token) = self.api_keys.get("access_token") {
            request_builder = request_builder.header("authorization", format!("Bearer {}", access_token));
        }
        if let Some(app_key) = self.api_keys.get("app_key") {
            request_builder = request_builder.header("appkey", app_key);
        }
        if let Some(app_secret) = self.api_keys.get("app_secret") {
            request_builder = request_builder.header("appsecret", app_secret);
        }
        request_builder = request_builder.header("tr_id", "FHKST03010100");

        let response: KISResponse = tokio::runtime::Handle::current().block_on(self.base.execute_request(request_builder))?;

        if response.rt_cd != "0" {
            return Err(anyhow::anyhow!("KIS API 오류: {}", response.msg1));
        }

        let market_data = response.output.unwrap_or_default();
        let mut result = Vec::new();

        for data in market_data {
            let timestamp = chrono::NaiveDate::parse_from_str(&data.stck_bsop_date, "%Y%m%d")?
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc();

            let open = data.stck_oprc.parse::<f64>()?;
            let high = data.stck_hgpr.parse::<f64>()?;
            let low = data.stck_lwpr.parse::<f64>()?;
            let close = data.stck_clpr.parse::<f64>()?;
            let volume = data.acml_vol.parse::<u64>()?;
            let vwap = data.acml_tr_pbmn.parse::<f64>()? / volume as f64;

            result.push(MarketData {
                symbol: symbol.clone(),
                timestamp,
                open,
                high,
                low,
                close,
                volume,
                vwap,
                adjusted_close: Some(close),
                dividend: None,
                split_ratio: None,
            });
        }

        self.base.update_record_count(result.len() as u64);
        Ok(result)
    }

    /// Alpha Vantage API로부터 데이터 수집
    fn collect_from_alpha_vantage(&self, params: CollectParams) -> Result<Vec<MarketData>> {
        let symbol = params.symbol.ok_or_else(|| anyhow::anyhow!("심볼이 필요합니다"))?;
        let api_key = self.api_keys.get("api_key")
            .ok_or_else(|| anyhow::anyhow!("Alpha Vantage API 키가 필요합니다"))?;

        let url = format!("{}/query", self.base.get_config().api_config.base_url);
        
        let mut query_params = HashMap::new();
        query_params.insert("function", "TIME_SERIES_DAILY");
        query_params.insert("symbol", &symbol);
        query_params.insert("apikey", api_key);
        query_params.insert("outputsize", "full");

        if let Some(interval) = &params.interval {
            query_params.insert("interval", interval);
        }

        let request_builder = self.base.get_client()
            .get(&url)
            .query(&query_params);

        let response: AlphaVantageResponse = tokio::runtime::Handle::current().block_on(self.base.execute_request(request_builder))?;

        if let Some(error) = response.error_message {
            return Err(anyhow::anyhow!("Alpha Vantage API 오류: {}", error));
        }

        if let Some(note) = response.note {
            warn!("Alpha Vantage API 노트: {}", note);
        }

        let time_series = response.time_series.ok_or_else(|| 
            anyhow::anyhow!("시간 시계열 데이터가 없습니다"))?;

        let mut result = Vec::new();
        let mut dates: Vec<_> = time_series.keys().collect();
        dates.sort_by(|a, b| b.cmp(a)); // 최신 날짜부터

        for date_str in dates {
            if let Some(data) = time_series.get(date_str) {
                let timestamp = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d")?
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc();

                let open = data.open.parse::<f64>()?;
                let high = data.high.parse::<f64>()?;
                let low = data.low.parse::<f64>()?;
                let close = data.close.parse::<f64>()?;
                let volume = data.volume.parse::<u64>()?;
                let vwap = (high + low + close) / 3.0;

                result.push(MarketData {
                    symbol: symbol.clone(),
                    timestamp,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    vwap,
                    adjusted_close: Some(close),
                    dividend: None,
                    split_ratio: None,
                });
            }
        }

        self.base.update_record_count(result.len() as u64);
        Ok(result)
    }

    /// Yahoo Finance API로부터 데이터 수집
    fn collect_from_yahoo(&self, params: CollectParams) -> Result<Vec<MarketData>> {
        let symbol = params.symbol.ok_or_else(|| anyhow::anyhow!("심볼이 필요합니다"))?;
        
        let start_timestamp = params.start_date
            .unwrap_or_else(|| Utc::now() - chrono::Duration::days(365))
            .timestamp();
        
        let end_timestamp = params.end_date
            .unwrap_or_else(|| Utc::now())
            .timestamp();

        let url = format!("{}/v8/finance/chart/{}", 
                         self.base.get_config().api_config.base_url, symbol);
        
        let mut query_params: HashMap<&str, String> = HashMap::new();
        query_params.insert("period1", start_timestamp.to_string());
        query_params.insert("period2", end_timestamp.to_string());
        query_params.insert("interval", "1d".to_string());
        query_params.insert("includePrePost", "true".to_string());
        query_params.insert("events", "div,split".to_string());

        let request_builder = self.base.get_client()
            .get(&url)
            .query(&query_params);

        let response: YahooFinanceResponse = tokio::runtime::Handle::current().block_on(self.base.execute_request(request_builder))?;

        let chart_result = response.chart.result
            .ok_or_else(|| anyhow::anyhow!("차트 데이터가 없습니다"))?;

        if chart_result.is_empty() {
            return Err(anyhow::anyhow!("차트 결과가 비어있습니다"));
        }

        let result_data = &chart_result[0];
        let timestamps = &result_data.timestamp;
        let quotes = &result_data.indicators.quote[0];

        let mut result = Vec::new();

        for (i, &timestamp) in timestamps.iter().enumerate() {
            if i < quotes.open.len() && 
               quotes.open[i].is_some() && 
               quotes.high[i].is_some() && 
               quotes.low[i].is_some() && 
               quotes.close[i].is_some() && 
               quotes.volume[i].is_some() {
                
                let timestamp = DateTime::from_timestamp(timestamp, 0)
                    .unwrap_or_else(|| Utc::now());
                
                let open = quotes.open[i].unwrap();
                let high = quotes.high[i].unwrap();
                let low = quotes.low[i].unwrap();
                let close = quotes.close[i].unwrap();
                let volume = quotes.volume[i].unwrap();
                let vwap = (high + low + close) / 3.0;

                result.push(MarketData {
                    symbol: symbol.clone(),
                    timestamp,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    vwap,
                    adjusted_close: Some(close),
                    dividend: None,
                    split_ratio: None,
                });
            }
        }

        self.base.update_record_count(result.len() as u64);
        Ok(result)
    }
}

impl DataCollector for MarketDataCollector {
    type DataType = MarketData;

    fn initialize(&mut self, config: DataSourceConfig) -> Result<()> {
        self.base.initialize(config)
    }

    fn collect(&self, params: CollectParams) -> Result<Self::DataType> {
        let results = self.collect_batch(vec![params])?;
        if results.is_empty() {
            return Err(anyhow::anyhow!("수집된 데이터가 없습니다"));
        }
        Ok(results[0].clone())
    }

    fn collect_batch(&self, params: Vec<CollectParams>) -> Result<Vec<Self::DataType>> {
        let mut all_results = Vec::new();

        for param in params {
            let results = match self.source_type {
                MarketDataSource::KIS => self.collect_from_kis(param)?,
                MarketDataSource::AlphaVantage => self.collect_from_alpha_vantage(param)?,
                MarketDataSource::YahooFinance => self.collect_from_yahoo(param)?,
                _ => return Err(anyhow::anyhow!("지원하지 않는 데이터 소스: {:?}", self.source_type)),
            };
            
            all_results.extend(results);
        }

        Ok(all_results)
    }

    fn health_check(&self) -> Result<HealthStatus> {
        self.base.health_check()
    }

    fn get_stats(&self) -> CollectionStats {
        self.base.get_stats()
    }
}

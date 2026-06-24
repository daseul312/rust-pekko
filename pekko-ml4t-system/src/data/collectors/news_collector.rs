//! 뉴스 데이터 수집기
//! 
//! 다양한 뉴스 소스로부터 뉴스 데이터를 수집합니다:
//! - NewsAPI
//! - Reuters
//! - Bloomberg
//! - 기타 뉴스 제공업체

use super::{DataCollector, CollectParams, CollectionStats, HealthStatus, BaseCollector};
use crate::data::{NewsData, DataSourceConfig, Entity, EntityType};
use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{ warn, error, debug};

/// 뉴스 데이터 수집기
pub struct NewsCollector {
    base: BaseCollector,
    source_type: NewsSource,
    api_keys: HashMap<String, String>,
}

/// 뉴스 소스 타입
#[derive(Debug, Clone)]
pub enum NewsSource {
    NewsAPI,
    Reuters,
    Bloomberg,
    GoogleNews,
    YahooNews,
}

/// NewsAPI 응답 구조
#[derive(Debug, Deserialize)]
struct NewsAPIResponse {
    status: String,
    totalResults: u32,
    articles: Vec<NewsAPIArticle>,
}

#[derive(Debug, Deserialize)]
struct NewsAPIArticle {
    title: String,
    description: Option<String>,
    content: Option<String>,
    url: String,
    urlToImage: Option<String>,
    publishedAt: String,
    source: NewsAPISource,
}

#[derive(Debug, Deserialize)]
struct NewsAPISource {
    id: Option<String>,
    name: String,
}

/// Reuters 응답 구조 (예시)
#[derive(Debug, Deserialize)]
struct ReutersResponse {
    articles: Vec<ReutersArticle>,
}

#[derive(Debug, Deserialize)]
struct ReutersArticle {
    headline: String,
    summary: Option<String>,
    body: String,
    published_date: String,
    url: String,
    category: String,
    symbols: Vec<String>,
}

/// Bloomberg 응답 구조 (예시)
#[derive(Debug, Deserialize)]
struct BloombergResponse {
    news: Vec<BloombergArticle>,
}

#[derive(Debug, Deserialize)]
struct BloombergArticle {
    title: String,
    summary: String,
    content: String,
    timestamp: String,
    url: String,
    tags: Vec<String>,
    tickers: Vec<String>,
}

impl NewsCollector {
    /// NewsAPI 수집기 생성
    pub fn new_newsapi() -> Self {
        let config = DataSourceConfig {
            source_name: "NewsAPI".to_string(),
            data_type: crate::data::DataType::NewsData,
            api_config: crate::data::ApiConfig {
                base_url: "https://newsapi.org/v2".to_string(),
                api_key: None,
                api_secret: None,
                rate_limit: crate::data::RateLimitConfig {
                    requests_per_minute: 60,
                    requests_per_hour: 1000,
                    requests_per_day: 10000,
                    burst_limit: 5,
                },
                timeout_seconds: 30,
                headers: HashMap::new(),
            },
            update_frequency: crate::data::UpdateFrequency::Every15Minutes,
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
                max_price_deviation: 0.0, // 뉴스는 가격 데이터 없음
                max_volume_spike: 0.0,
                min_confidence_score: 0.7,
            },
        };

        Self {
            base: BaseCollector::new(config),
            source_type: NewsSource::NewsAPI,
            api_keys: HashMap::new(),
        }
    }

    /// Reuters 수집기 생성
    pub fn new_reuters() -> Self {
        let config = DataSourceConfig {
            source_name: "Reuters".to_string(),
            data_type: crate::data::DataType::NewsData,
            api_config: crate::data::ApiConfig {
                base_url: "https://api.reuters.com".to_string(),
                api_key: None,
                api_secret: None,
                rate_limit: crate::data::RateLimitConfig {
                    requests_per_minute: 30,
                    requests_per_hour: 500,
                    requests_per_day: 5000,
                    burst_limit: 3,
                },
                timeout_seconds: 30,
                headers: HashMap::new(),
            },
            update_frequency: crate::data::UpdateFrequency::Every15Minutes,
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
                max_price_deviation: 0.0,
                max_volume_spike: 0.0,
                min_confidence_score: 0.8,
            },
        };

        Self {
            base: BaseCollector::new(config),
            source_type: NewsSource::Reuters,
            api_keys: HashMap::new(),
        }
    }

    /// Bloomberg 수집기 생성
    pub fn new_bloomberg() -> Self {
        let config = DataSourceConfig {
            source_name: "Bloomberg".to_string(),
            data_type: crate::data::DataType::NewsData,
            api_config: crate::data::ApiConfig {
                base_url: "https://api.bloomberg.com".to_string(),
                api_key: None,
                api_secret: None,
                rate_limit: crate::data::RateLimitConfig {
                    requests_per_minute: 20,
                    requests_per_hour: 300,
                    requests_per_day: 3000,
                    burst_limit: 2,
                },
                timeout_seconds: 30,
                headers: HashMap::new(),
            },
            update_frequency: crate::data::UpdateFrequency::Every15Minutes,
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
                max_price_deviation: 0.0,
                max_volume_spike: 0.0,
                min_confidence_score: 0.85,
            },
        };

        Self {
            base: BaseCollector::new(config),
            source_type: NewsSource::Bloomberg,
            api_keys: HashMap::new(),
        }
    }

    /// API 키 설정
    pub fn set_api_key(&mut self, key_name: &str, key_value: String) {
        self.api_keys.insert(key_name.to_string(), key_value);
    }

    /// NewsAPI로부터 데이터 수집
    fn collect_from_newsapi(&self, params: CollectParams) -> Result<Vec<NewsData>> {
        let api_key = self.api_keys.get("api_key")
            .ok_or_else(|| anyhow::anyhow!("NewsAPI API 키가 필요합니다"))?;

        let url = format!("{}/everything", self.base.get_config().api_config.base_url);
        
        let mut query_params: HashMap<&str, String> = HashMap::new();
        query_params.insert("apiKey", api_key.clone());
        query_params.insert("language", "en".to_string());
        query_params.insert("sortBy", "publishedAt".to_string());
        query_params.insert("pageSize", "100".to_string());

        // 심볼 기반 검색
        if let Some(symbol) = &params.symbol {
            query_params.insert("q", symbol.to_string());
        }

        // 날짜 범위 설정
        if let Some(start_date) = params.start_date {
            query_params.insert("from", start_date.format("%Y-%m-%d").to_string());
        }
        if let Some(end_date) = params.end_date {
            query_params.insert("to", end_date.format("%Y-%m-%d").to_string());
        }

        let request_builder = self.base.get_client()
            .get(&url)
            .query(&query_params);

        let response: NewsAPIResponse = tokio::runtime::Handle::current().block_on(self.base.execute_request(request_builder))?;

        if response.status != "ok" {
            return Err(anyhow::anyhow!("NewsAPI 응답 오류: {}", response.status));
        }

        let mut result = Vec::new();

        for (i, article) in response.articles.into_iter().enumerate() {
            let timestamp = chrono::DateTime::parse_from_rfc3339(&article.publishedAt)
                .map_err(|e| anyhow::anyhow!("날짜 파싱 오류: {}", e))?
                .with_timezone(&Utc);

            let content = article.content
                .or(article.description.clone())
                .unwrap_or_else(|| article.title.clone());

            let news_data = NewsData {
                id: format!("newsapi_{}", i),
                title: article.title,
                content: content.clone(),
                summary: article.description.clone(),
                timestamp,
                source: article.source.name,
                url: Some(article.url),
                sentiment_score: None, // TODO: 센티먼트 분석 추가
                sentiment_magnitude: None,
                entities: self.extract_entities_from_text(&content)?,
                symbols: self.extract_symbols_from_text(&content)?,
                categories: vec!["general".to_string()],
            };

            result.push(news_data);
        }

        self.base.update_record_count(result.len() as u64);
        Ok(result)
    }

    /// Reuters로부터 데이터 수집
    fn collect_from_reuters(&self, params: CollectParams) -> Result<Vec<NewsData>> {
        let url = format!("{}/news", self.base.get_config().api_config.base_url);
        
        let mut query_params = HashMap::new();
        query_params.insert("limit", "100");
        query_params.insert("category", "business");

        if let Some(symbol) = &params.symbol {
            query_params.insert("symbol", symbol);
        }

        let request_builder = self.base.get_client()
            .get(&url)
            .query(&query_params);

        let response: ReutersResponse = tokio::runtime::Handle::current().block_on(self.base.execute_request(request_builder))?;

        let mut result = Vec::new();

        for (i, article) in response.articles.into_iter().enumerate() {
            let timestamp = chrono::DateTime::parse_from_rfc3339(&article.published_date)
                .map_err(|e| anyhow::anyhow!("날짜 파싱 오류: {}", e))?
                .with_timezone(&Utc);

            let news_data = NewsData {
                id: format!("reuters_{}", i),
                title: article.headline,
                content: article.body.clone(),
                summary: article.summary,
                timestamp,
                source: "Reuters".to_string(),
                url: Some(article.url),
                sentiment_score: None,
                sentiment_magnitude: None,
                entities: self.extract_entities_from_text(&article.body)?,
                symbols: article.symbols,
                categories: vec![article.category],
            };

            result.push(news_data);
        }

        self.base.update_record_count(result.len() as u64);
        Ok(result)
    }

    /// Bloomberg로부터 데이터 수집
    fn collect_from_bloomberg(&self, params: CollectParams) -> Result<Vec<NewsData>> {
        let url = format!("{}/news", self.base.get_config().api_config.base_url);
        
        let mut query_params = HashMap::new();
        query_params.insert("limit", "100");

        if let Some(symbol) = &params.symbol {
            query_params.insert("ticker", symbol);
        }

        let request_builder = self.base.get_client()
            .get(&url)
            .query(&query_params);

        let response: BloombergResponse = tokio::runtime::Handle::current().block_on(self.base.execute_request(request_builder))?;

        let mut result = Vec::new();

        for (i, article) in response.news.into_iter().enumerate() {
            let timestamp = chrono::DateTime::parse_from_rfc3339(&article.timestamp)
                .map_err(|e| anyhow::anyhow!("날짜 파싱 오류: {}", e))?
                .with_timezone(&Utc);

            let news_data = NewsData {
                id: format!("bloomberg_{}", i),
                title: article.title,
                content: article.content.clone(),
                summary: Some(article.summary),
                timestamp,
                source: "Bloomberg".to_string(),
                url: Some(article.url),
                sentiment_score: None,
                sentiment_magnitude: None,
                entities: self.extract_entities_from_text(&article.content)?,
                symbols: article.tickers,
                categories: article.tags,
            };

            result.push(news_data);
        }

        self.base.update_record_count(result.len() as u64);
        Ok(result)
    }

    /// 텍스트에서 개체명 추출 (간단한 구현)
    fn extract_entities_from_text(&self, text: &str) -> Result<Vec<Entity>> {
        let mut entities = Vec::new();
        
        // 간단한 휴리스틱 기반 개체명 인식
        let words: Vec<&str> = text.split_whitespace().collect();
        
        for (i, word) in words.iter().enumerate() {
            let clean_word = word.trim_matches(|c: char| !c.is_alphanumeric());
            
            // 회사명 패턴 (대문자로 시작하는 2단어 이상)
            if clean_word.len() > 2 && clean_word.chars().next().unwrap().is_uppercase() {
                if i + 1 < words.len() {
                    let next_word = words[i + 1].trim_matches(|c: char| !c.is_alphanumeric());
                    if next_word.chars().next().unwrap().is_uppercase() {
                        entities.push(Entity {
                            text: format!("{} {}", clean_word, next_word),
                            entity_type: EntityType::Company,
                            confidence: 0.7,
                            start_pos: 0, // TODO: 실제 위치 계산
                            end_pos: 0,
                        });
                    }
                }
            }
            
            // 숫자 패턴
            if clean_word.parse::<f64>().is_ok() {
                entities.push(Entity {
                    text: clean_word.to_string(),
                    entity_type: EntityType::Number,
                    confidence: 0.9,
                    start_pos: 0,
                    end_pos: 0,
                });
            }
            
            // 날짜 패턴 (간단한)
            if clean_word.contains("2024") || clean_word.contains("2023") {
                entities.push(Entity {
                    text: clean_word.to_string(),
                    entity_type: EntityType::Date,
                    confidence: 0.8,
                    start_pos: 0,
                    end_pos: 0,
                });
            }
        }
        
        Ok(entities)
    }

    /// 텍스트에서 심볼 추출 (간단한 구현)
    fn extract_symbols_from_text(&self, text: &str) -> Result<Vec<String>> {
        let mut symbols = Vec::new();
        
        // 일반적인 주식 심볼 패턴들
        let common_symbols = [
            "AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "NFLX",
            "삼성전자", "기아", "현대차", "LG전자", "SK하이닉스", "NAVER",
            "000270", "005930", "000660", "035420", "005380",
        ];
        
        for symbol in &common_symbols {
            if text.to_uppercase().contains(symbol) {
                symbols.push(symbol.to_string());
            }
        }
        
        // 티커 심볼 패턴 (대문자 1-5글자)
        let re = regex::Regex::new(r"\b[A-Z]{1,5}\b")?;
        for mat in re.find_iter(text) {
            let symbol = mat.as_str();
            if !symbols.contains(&symbol.to_string()) {
                symbols.push(symbol.to_string());
            }
        }
        
        Ok(symbols)
    }

    /// 센티먼트 분석 (간단한 구현)
    async fn analyze_sentiment(&self, text: &str) -> Result<(f64, f64)> {
        let positive_words = ["good", "great", "excellent", "positive", "up", "rise", "gain", "profit", "strong"];
        let negative_words = ["bad", "terrible", "negative", "down", "fall", "loss", "weak", "poor", "decline"];
        
        let text_lower = text.to_lowercase();
        let words: Vec<&str> = text_lower.split_whitespace().collect();
        
        let mut positive_count = 0;
        let mut negative_count = 0;
        
        for word in &words {
            if positive_words.contains(word) {
                positive_count += 1;
            }
            if negative_words.contains(word) {
                negative_count += 1;
            }
        }
        
        let total_words = words.len() as f64;
        if total_words == 0.0 {
            return Ok((0.0, 0.0));
        }
        
        let positive_ratio = positive_count as f64 / total_words;
        let negative_ratio = negative_count as f64 / total_words;
        
        let sentiment_score = (positive_ratio - negative_ratio).clamp(-1.0, 1.0);
        let sentiment_magnitude = (positive_ratio + negative_ratio).clamp(0.0, 1.0);
        
        Ok((sentiment_score, sentiment_magnitude))
    }
}

impl DataCollector for NewsCollector {
    type DataType = NewsData;

    fn initialize(&mut self, config: DataSourceConfig) -> Result<()> {
        self.base.initialize(config)
    }

    fn collect(&self, params: CollectParams) -> Result<Self::DataType> {
        let results = self.collect_batch(vec![params])?;
        if results.is_empty() {
            return Err(anyhow::anyhow!("수집된 뉴스 데이터가 없습니다"));
        }
        Ok(results[0].clone())
    }

    fn collect_batch(&self, params: Vec<CollectParams>) -> Result<Vec<Self::DataType>> {
        let mut all_results = Vec::new();

        for param in params {
            let results = match self.source_type {
                NewsSource::NewsAPI => self.collect_from_newsapi(param)?,
                NewsSource::Reuters => self.collect_from_reuters(param)?,
                NewsSource::Bloomberg => self.collect_from_bloomberg(param)?,
                _ => return Err(anyhow::anyhow!("지원하지 않는 뉴스 소스: {:?}", self.source_type)),
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

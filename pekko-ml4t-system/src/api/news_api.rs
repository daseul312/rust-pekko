//! 뉴스 API 클라이언트
//! 
//! 뉴스 데이터 수집 및 센티먼트 분석을 위한 API 통합

use super::{ApiClient, ApiConfig, ApiError};
use reqwest::Client;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use anyhow::Result;
use chrono::{DateTime, Utc};
use tracing::{ warn, error};

/// 뉴스 API 클라이언트
#[derive(Clone)]
pub struct NewsApiClient {
    client: Client,
    config: ApiConfig,
    api_provider: NewsApiProvider,
}

/// 뉴스 API 제공업체
#[derive(Debug, Clone)]
pub enum NewsApiProvider {
    NewsApi,        // newsapi.org
    AlphaVantage,   // Alpha Vantage News
    YahooFinance,   // Yahoo Finance News
    Custom(String), // 커스텀 API
}

/// 뉴스 기사
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewsArticle {
    pub title: String,
    pub description: String,
    pub content: String,
    pub url: String,
    pub source: String,
    pub published_at: DateTime<Utc>,
    pub symbols: Vec<String>,
    pub sentiment_score: Option<f64>, // -1.0 ~ 1.0
    pub relevance_score: f64,         // 0.0 ~ 1.0
}

/// 뉴스 센티먼트 분석 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentimentAnalysis {
    pub article_id: String,
    pub overall_sentiment: SentimentType,
    pub sentiment_score: f64,
    pub confidence: f64,
    pub positive_words: Vec<String>,
    pub negative_words: Vec<String>,
    pub neutral_words: Vec<String>,
}

/// 센티먼트 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SentimentType {
    Positive,
    Negative,
    Neutral,
}

/// NewsAPI.org 응답
#[derive(Debug, Deserialize)]
struct NewsApiResponse {
    status: String,
    total_results: u32,
    articles: Vec<NewsApiArticle>,
}

#[derive(Debug, Deserialize)]
struct NewsApiArticle {
    source: NewsApiSource,
    author: Option<String>,
    title: String,
    description: Option<String>,
    url: String,
    url_to_image: Option<String>,
    published_at: String,
    content: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NewsApiSource {
    id: Option<String>,
    name: String,
}

/// Alpha Vantage 뉴스 응답
#[derive(Debug, Deserialize)]
struct AlphaVantageNewsResponse {
    #[serde(rename = "News")]
    news: Vec<AlphaVantageNews>,
    #[serde(rename = "Error Message")]
    error_message: Option<String>,
    #[serde(rename = "Note")]
    note: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AlphaVantageNews {
    #[serde(rename = "title")]
    title: String,
    #[serde(rename = "url")]
    url: String,
    #[serde(rename = "time_published")]
    time_published: String,
    #[serde(rename = "authors")]
    authors: Vec<String>,
    #[serde(rename = "summary")]
    summary: String,
    #[serde(rename = "banner_image")]
    banner_image: Option<String>,
    #[serde(rename = "source")]
    source: String,
    #[serde(rename = "category_within_source")]
    category_within_source: String,
    #[serde(rename = "source_domain")]
    source_domain: String,
    #[serde(rename = "topics")]
    topics: Vec<AlphaVantageTopic>,
    #[serde(rename = "overall_sentiment_score")]
    overall_sentiment_score: f64,
    #[serde(rename = "overall_sentiment_label")]
    overall_sentiment_label: String,
    #[serde(rename = "ticker_sentiment")]
    ticker_sentiment: Vec<AlphaVantageTickerSentiment>,
}

#[derive(Debug, Deserialize)]
struct AlphaVantageTopic {
    #[serde(rename = "topic")]
    topic: String,
    #[serde(rename = "relevance_score")]
    relevance_score: String,
}

#[derive(Debug, Deserialize)]
struct AlphaVantageTickerSentiment {
    #[serde(rename = "ticker")]
    ticker: String,
    #[serde(rename = "relevance_score")]
    relevance_score: String,
    #[serde(rename = "ticker_sentiment_score")]
    ticker_sentiment_score: String,
    #[serde(rename = "ticker_sentiment_label")]
    ticker_sentiment_label: String,
}

#[async_trait::async_trait]
impl ApiClient for NewsApiClient {
    type Config = ApiConfig;
    type Response = NewsArticle;

    async fn new(config: Self::Config) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_millis(config.timeout_ms))
            .build()?;

        // API 제공업체 자동 감지
        let api_provider = if config.base_url.contains("newsapi.org") {
            NewsApiProvider::NewsApi
        } else if config.base_url.contains("alpha-vantage") {
            NewsApiProvider::AlphaVantage
        } else {
            NewsApiProvider::Custom(config.base_url.clone())
        };

        Ok(Self {
            client,
            config,
            api_provider,
        })
    }

    async fn authenticate(&mut self) -> Result<()> {
        if self.config.api_key.is_empty() {
            return Err(ApiError::AuthenticationError("뉴스 API 키가 필요합니다".to_string()).into());
        }
        Ok(())
    }

    async fn get_market_data(&self, _symbol: &str) -> Result<super::MarketDataResponse> {
        Err(ApiError::ParseError("뉴스 API는 시장 데이터를 제공하지 않습니다".to_string()).into())
    }

    async fn get_account_info(&self) -> Result<super::AccountInfo> {
        Err(ApiError::AccountError("뉴스 API는 계좌 정보를 제공하지 않습니다".to_string()).into())
    }

    async fn place_order(&self, _order: &super::OrderRequest) -> Result<super::OrderResponse> {
        Err(ApiError::OrderError("뉴스 API는 거래 기능을 제공하지 않습니다".to_string()).into())
    }

    async fn get_positions(&self) -> Result<Vec<super::Position>> {
        Err(ApiError::AccountError("뉴스 API는 포지션 정보를 제공하지 않습니다".to_string()).into())
    }

    async fn get_order_status(&self, _order_id: &str) -> Result<super::OrderStatus> {
        Err(ApiError::OrderError("뉴스 API는 주문 상태를 제공하지 않습니다".to_string()).into())
    }
}

impl NewsApiClient {
    /// 특정 심볼에 대한 뉴스 조회
    pub async fn get_news_for_symbol(&self, symbol: &str, days: u32) -> Result<Vec<NewsArticle>> {
        match &self.api_provider {
            NewsApiProvider::NewsApi => self.get_news_from_newsapi(symbol, days).await,
            NewsApiProvider::AlphaVantage => self.get_news_from_alpha_vantage(symbol).await,
            NewsApiProvider::YahooFinance => self.get_news_from_yahoo_finance(symbol).await,
            NewsApiProvider::Custom(url) => self.get_news_from_custom_api(url, symbol).await,
        }
    }

    /// NewsAPI.org에서 뉴스 조회
    async fn get_news_from_newsapi(&self, symbol: &str, days: u32) -> Result<Vec<NewsArticle>> {
        let url = format!("{}/v2/everything", self.config.base_url);
        
        let from_date = Utc::now() - chrono::Duration::days(days as i64);
        
        let params = [
            ("q", symbol),
            ("from", &from_date.format("%Y-%m-%d").to_string()),
            ("sortBy", "publishedAt"),
            ("language", "en"),
            ("pageSize", "100"),
            ("apiKey", &self.config.api_key),
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

        let news_response: NewsApiResponse = response.json().await?;
        
        let articles = news_response.articles
            .into_iter()
            .map(|article| {
                let published_at = chrono::DateTime::parse_from_rfc3339(&article.published_at)
                    .unwrap_or_else(|_| Utc::now().into())
                    .with_timezone(&Utc);
                
                NewsArticle {
                    title: article.title,
                    description: article.description.unwrap_or_default(),
                    content: article.content.unwrap_or_default(),
                    url: article.url,
                    source: article.source.name,
                    published_at,
                    symbols: vec![symbol.to_string()],
                    sentiment_score: None,
                    relevance_score: 1.0,
                }
            })
            .collect();

        Ok(articles)
    }

    /// Alpha Vantage에서 뉴스 조회
    async fn get_news_from_alpha_vantage(&self, symbol: &str) -> Result<Vec<NewsArticle>> {
        let url = format!("{}/query", self.config.base_url);
        
        let params = [
            ("function", "NEWS_SENTIMENT"),
            ("tickers", symbol),
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

        let news_response: AlphaVantageNewsResponse = response.json().await?;
        
        if let Some(error_msg) = news_response.error_message {
            return Err(ApiError::ParseError(error_msg).into());
        }
        
        if let Some(note) = news_response.note {
            warn!("Alpha Vantage API 제한: {}", note);
            return Err(ApiError::RateLimitError(note).into());
        }

        let articles = news_response.news
            .into_iter()
            .map(|news| {
                let published_at = chrono::DateTime::parse_from_rfc3339(&news.time_published)
                    .unwrap_or_else(|_| Utc::now().into())
                    .with_timezone(&Utc);
                
                let sentiment_score = Some(news.overall_sentiment_score);
                
                NewsArticle {
                    title: news.title,
                    description: news.summary.clone(),
                    content: news.summary, // Alpha Vantage는 요약만 제공
                    url: news.url,
                    source: news.source,
                    published_at,
                    symbols: news.ticker_sentiment.iter().map(|t| t.ticker.clone()).collect(),
                    sentiment_score,
                    relevance_score: 1.0,
                }
            })
            .collect();

        Ok(articles)
    }

    /// Yahoo Finance에서 뉴스 조회 (시뮬레이션)
    async fn get_news_from_yahoo_finance(&self, symbol: &str) -> Result<Vec<NewsArticle>> {
        // TODO: Yahoo Finance API 연동 (실제로는 웹 스크래핑 필요)
        Ok(vec![
            NewsArticle {
                title: format!("{} 관련 뉴스", symbol),
                description: format!("{}에 대한 최신 뉴스입니다", symbol),
                content: "뉴스 내용...".to_string(),
                url: "https://finance.yahoo.com".to_string(),
                source: "Yahoo Finance".to_string(),
                published_at: Utc::now(),
                symbols: vec![symbol.to_string()],
                sentiment_score: None,
                relevance_score: 1.0,
            }
        ])
    }

    /// 커스텀 API에서 뉴스 조회
    async fn get_news_from_custom_api(&self, url: &str, symbol: &str) -> Result<Vec<NewsArticle>> {
        // TODO: 커스텀 API 연동
        Ok(vec![])
    }

    /// 뉴스 센티먼트 분석
    pub async fn analyze_sentiment(&self, article: &NewsArticle) -> Result<SentimentAnalysis> {
        // 간단한 키워드 기반 센티먼트 분석
        let positive_words = vec![
            "good", "great", "excellent", "positive", "strong", "growth", "profit", "gain",
            "rise", "increase", "up", "bullish", "optimistic", "success", "win"
        ];
        
        let negative_words = vec![
            "bad", "terrible", "negative", "weak", "decline", "loss", "drop", "fall",
            "down", "decrease", "bearish", "pessimistic", "failure", "lose", "crash"
        ];

        let content = format!("{} {}", article.title, article.description).to_lowercase();
        
        let positive_count = positive_words.iter()
            .map(|word| content.matches(word).count())
            .sum::<usize>();
            
        let negative_count = negative_words.iter()
            .map(|word| content.matches(word).count())
            .sum::<usize>();

        let total_words = positive_count + negative_count;
        
        let sentiment_score = if total_words == 0 {
            0.0
        } else {
            (positive_count as f64 - negative_count as f64) / total_words as f64
        };

        let sentiment_type = if sentiment_score > 0.1 {
            SentimentType::Positive
        } else if sentiment_score < -0.1 {
            SentimentType::Negative
        } else {
            SentimentType::Neutral
        };

        Ok(SentimentAnalysis {
            article_id: format!("news_{}", article.published_at.timestamp()),
            overall_sentiment: sentiment_type,
            sentiment_score,
            confidence: (total_words as f64 / 10.0).min(1.0),
            positive_words: positive_words.into_iter()
                .filter(|word| content.contains(word))
                .map(|s| s.to_string())
                .collect(),
            negative_words: negative_words.into_iter()
                .filter(|word| content.contains(word))
                .map(|s| s.to_string())
                .collect(),
            neutral_words: vec![],
        })
    }

    /// 여러 심볼에 대한 뉴스 집계
    pub async fn get_aggregated_news(&self, symbols: &[String], days: u32) -> Result<HashMap<String, Vec<NewsArticle>>> {
        let mut all_news = HashMap::new();
        
        for symbol in symbols {
            match self.get_news_for_symbol(symbol, days).await {
                Ok(articles) => {
                    all_news.insert(symbol.clone(), articles);
                }
                Err(e) => {
                    warn!("{} 뉴스 조회 실패: {}", symbol, e);
                }
            }
            
            // API 제한을 위한 대기
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        
        Ok(all_news)
    }

    /// 센티먼트 점수 집계
    pub fn aggregate_sentiment(&self, articles: &[NewsArticle]) -> f64 {
        let sentiment_scores: Vec<f64> = articles
            .iter()
            .filter_map(|article| article.sentiment_score)
            .collect();
            
        if sentiment_scores.is_empty() {
            0.0
        } else {
            sentiment_scores.iter().sum::<f64>() / sentiment_scores.len() as f64
        }
    }
}

//! 뉴스 수집 Actor
//!
//! 여러 소스에서 실시간 경제/금융 뉴스를 수집합니다.
//! - NewsAPI.org
//! - Alpha Vantage News
//! - RSS Feeds (Yahoo Finance, Reuters 등)

use async_trait::async_trait;
use chrono::{DateTime, Utc, Duration};
use pekko_actor::{Actor, ActorContext};
use pekko_persistence::Uuid;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{ warn, error};

// ============================================================
// 뉴스 데이터 타입
// ============================================================

/// 뉴스 기사
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewsArticle {
    pub id: String,
    pub title: String,
    pub summary: String,
    pub content: String,
    pub url: String,
    pub source: NewsSource,
    pub published_at: DateTime<Utc>,
    pub symbols: Vec<String>,        // 관련 종목 티커
    pub categories: Vec<String>,     // 카테고리 (earnings, market, economy 등)
}

/// 뉴스 소스
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NewsSource {
    NewsApi,
    AlphaVantage,
    YahooFinance,
    Reuters,
    Bloomberg,
    CNBC,
    Custom(String),
}

impl std::fmt::Display for NewsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NewsSource::NewsApi => write!(f, "NewsAPI"),
            NewsSource::AlphaVantage => write!(f, "AlphaVantage"),
            NewsSource::YahooFinance => write!(f, "Yahoo Finance"),
            NewsSource::Reuters => write!(f, "Reuters"),
            NewsSource::Bloomberg => write!(f, "Bloomberg"),
            NewsSource::CNBC => write!(f, "CNBC"),
            NewsSource::Custom(name) => write!(f, "{}", name),
        }
    }
}

// ============================================================
// Actor 메시지
// ============================================================

/// 뉴스 수집 Actor 메시지
#[derive(Debug, Clone)]
pub enum NewsCollectorMessage {
    /// 특정 종목 뉴스 수집
    FetchNewsForSymbol { symbol: String },
    /// 시장 전체 뉴스 수집
    FetchMarketNews,
    /// 특정 카테고리 뉴스 수집
    FetchCategoryNews { category: String },
    /// 주기적 수집 시작
    StartPeriodicFetch { interval_secs: u64 },
    /// 주기적 수집 중지
    StopPeriodicFetch,
    /// 수집된 뉴스 전달 (내부용)
    NewsCollected { articles: Vec<NewsArticle> },
}

// ============================================================
// 뉴스 수집기 설정
// ============================================================

/// 뉴스 수집기 설정
#[derive(Debug, Clone)]
pub struct NewsCollectorConfig {
    pub newsapi_key: Option<String>,
    pub alphavantage_key: Option<String>,
    pub fetch_interval_secs: u64,
    pub max_articles_per_fetch: usize,
    pub symbols_to_watch: Vec<String>,
}

impl Default for NewsCollectorConfig {
    fn default() -> Self {
        Self {
            newsapi_key: std::env::var("NEWSAPI_KEY").ok(),
            alphavantage_key: std::env::var("ALPHAVANTAGE_KEY").ok(),
            fetch_interval_secs: 300, // 5분
            max_articles_per_fetch: 50,
            symbols_to_watch: vec![
                "AAPL".to_string(),
                "GOOGL".to_string(),
                "MSFT".to_string(),
                "AMZN".to_string(),
                "NVDA".to_string(),
                "TSLA".to_string(),
            ],
        }
    }
}

// ============================================================
// 뉴스 수집 Actor
// ============================================================

/// 뉴스 수집 Actor
pub struct NewsCollectorActor {
    config: NewsCollectorConfig,
    client: Client,
    cache: HashMap<String, NewsArticle>,  // id -> article
    last_fetch: Option<DateTime<Utc>>,
    collected_articles: Vec<NewsArticle>, // 최근 수집된 뉴스
}

impl NewsCollectorActor {
    pub fn new(config: NewsCollectorConfig) -> Self {
        Self {
            config,
            client: Client::new(),
            cache: HashMap::new(),
            last_fetch: None,
            collected_articles: Vec::new(),
        }
    }
    
    /// 최근 수집된 뉴스 가져오기
    pub fn get_collected_articles(&self) -> &[NewsArticle] {
        &self.collected_articles
    }

    /// NewsAPI.org에서 뉴스 가져오기
    async fn fetch_from_newsapi(&self, query: &str) -> Vec<NewsArticle> {
        let api_key = match &self.config.newsapi_key {
            Some(key) => key,
            None => {
                warn!("NewsAPI key not configured");
                return Vec::new();
            }
        };

        let url = format!(
            "https://newsapi.org/v2/everything?q={}&language=en&sortBy=publishedAt&pageSize={}&apiKey={}",
            query,
            self.config.max_articles_per_fetch,
            api_key
        );

        match self.client.get(&url).send().await {
            Ok(response) => {
                if let Ok(data) = response.json::<NewsApiResponse>().await {
                    data.articles
                        .into_iter()
                        .map(|a| self.convert_newsapi_article(a))
                        .collect()
                } else {
                    warn!("Failed to parse NewsAPI response");
                    Vec::new()
                }
            }
            Err(e) => {
                error!("NewsAPI request failed: {}", e);
                Vec::new()
            }
        }
    }

    /// Alpha Vantage에서 뉴스 가져오기
    async fn fetch_from_alphavantage(&self, symbol: &str) -> Vec<NewsArticle> {
        let api_key = match &self.config.alphavantage_key {
            Some(key) => key,
            None => {
                warn!("Alpha Vantage key not configured");
                return Vec::new();
            }
        };

        let url = format!(
            "https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={}&apikey={}",
            symbol,
            api_key
        );

        match self.client.get(&url).send().await {
            Ok(response) => {
                if let Ok(data) = response.json::<AlphaVantageNewsResponse>().await {
                    data.feed
                        .unwrap_or_default()
                        .into_iter()
                        .map(|a| self.convert_alphavantage_article(a, symbol))
                        .collect()
                } else {
                    warn!("Failed to parse Alpha Vantage response");
                    Vec::new()
                }
            }
            Err(e) => {
                error!("Alpha Vantage request failed: {}", e);
                Vec::new()
            }
        }
    }

    /// 무료 RSS/JSON 피드에서 뉴스 가져오기 (API 키 불필요)
    async fn fetch_from_free_sources(&self) -> Vec<NewsArticle> {
        let mut articles = Vec::new();

        // Finnhub 무료 뉴스 (제한적)
        if let Ok(response) = self.client
            .get("https://finnhub.io/api/v1/news?category=general&token=demo")
            .send()
            .await
        {
            if let Ok(data) = response.json::<Vec<FinnhubNews>>().await {
                for news in data.into_iter().take(10) {
                    let summary = news.summary.clone();
                    articles.push(NewsArticle {
                        id: format!("finnhub-{}", news.id),
                        title: news.headline,
                        summary,
                        content: news.summary,
                        url: news.url,
                        source: NewsSource::Custom("Finnhub".to_string()),
                        published_at: DateTime::from_timestamp(news.datetime, 0)
                            .unwrap_or_else(Utc::now),
                        symbols: news.related.split(',').map(|s| s.trim().to_string()).collect(),
                        categories: vec![news.category],
                    });
                }
            }
        }

        articles
    }

    /// 시뮬레이션용 샘플 뉴스 생성
    fn generate_sample_news(&self, symbol: &str) -> Vec<NewsArticle> {
        let now = Utc::now();
        let samples = vec![
            ("positive", "breaks record", 0.8),
            ("negative", "faces challenges", -0.6),
            ("neutral", "announces update", 0.0),
        ];

        samples.into_iter().enumerate().map(|(i, (sentiment, action, _score))| {
            NewsArticle {
                id: format!("sample-{}-{}-{}", symbol, sentiment, now.timestamp()),
                title: format!("{} {} in latest market development", symbol, action),
                summary: format!("Sample {} news for {} - for testing purposes", sentiment, symbol),
                content: format!("This is a sample {} article about {}. In real deployment, this would contain actual news content from various sources.", sentiment, symbol),
                url: format!("https://example.com/news/{}/{}", symbol.to_lowercase(), i),
                source: NewsSource::Custom("Sample".to_string()),
                published_at: now - Duration::minutes(i as i64 * 30),
                symbols: vec![symbol.to_string()],
                categories: vec!["market".to_string(), "stocks".to_string()],
            }
        }).collect()
    }

    fn convert_newsapi_article(&self, article: NewsApiArticle) -> NewsArticle {
        NewsArticle {
            id: format!("newsapi-{}", Uuid::new_v4()),
            title: article.title,
            summary: article.description.unwrap_or_default(),
            content: article.content.unwrap_or_default(),
            url: article.url,
            source: NewsSource::NewsApi,
            published_at: DateTime::parse_from_rfc3339(&article.published_at)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            symbols: Vec::new(), // NewsAPI doesn't provide ticker symbols
            categories: vec!["general".to_string()],
        }
    }

    fn convert_alphavantage_article(&self, article: AlphaVantageNews, symbol: &str) -> NewsArticle {
        let summary = article.summary.clone();
        NewsArticle {
            id: format!("av-{}", Uuid::new_v4()),
            title: article.title,
            summary,
            content: article.summary,
            url: article.url,
            source: NewsSource::AlphaVantage,
            published_at: DateTime::parse_from_str(&article.time_published, "%Y%m%dT%H%M%S")
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            symbols: vec![symbol.to_string()],
            categories: vec!["finance".to_string()],
        }
    }

    /// 수집된 뉴스 저장
    fn store_articles(&mut self, articles: &[NewsArticle]) {
        self.collected_articles.extend(articles.iter().cloned());
    }

    /// 캐시에 뉴스 저장 (중복 방지)
    fn cache_articles(&mut self, articles: Vec<NewsArticle>) -> Vec<NewsArticle> {
        let mut new_articles = Vec::new();
        for article in articles {
            if !self.cache.contains_key(&article.id) {
                self.cache.insert(article.id.clone(), article.clone());
                new_articles.push(article);
            }
        }
        new_articles
    }
}

#[async_trait]
impl Actor for NewsCollectorActor {
    type Message = NewsCollectorMessage;

    async fn receive(&mut self, message: Self::Message, _ctx: &mut ActorContext<Self>) {
        match message {
            NewsCollectorMessage::FetchNewsForSymbol { symbol } => {
                let mut all_articles = Vec::new();

                // 1. Alpha Vantage (API 키가 있으면)
                let av_articles = self.fetch_from_alphavantage(&symbol).await;
                all_articles.extend(av_articles);

                // 2. NewsAPI (API 키가 있으면)
                let query = format!("{} stock", symbol);
                let newsapi_articles = self.fetch_from_newsapi(&query).await;
                all_articles.extend(newsapi_articles);

                // 3. 무료 소스
                let free_articles = self.fetch_from_free_sources().await;
                all_articles.extend(free_articles);

                // 4. API 키 없으면 샘플 데이터
                if all_articles.is_empty() {
                    all_articles = self.generate_sample_news(&symbol);
                }

                // 중복 제거 및 캐시
                let new_articles = self.cache_articles(all_articles);
                self.last_fetch = Some(Utc::now());
                self.store_articles(&new_articles);
            }

            NewsCollectorMessage::FetchMarketNews => {
                let mut all_articles = Vec::new();

                // 각 관심 종목별 뉴스 수집
                for symbol in &self.config.symbols_to_watch.clone() {
                    let articles = self.fetch_from_alphavantage(symbol).await;
                    all_articles.extend(articles);
                }

                // 일반 시장 뉴스
                let market_news = self.fetch_from_newsapi("stock market economy").await;
                all_articles.extend(market_news);

                // 무료 소스
                let free_articles = self.fetch_from_free_sources().await;
                all_articles.extend(free_articles);

                // API 키 없으면 샘플 데이터
                if all_articles.is_empty() {
                    for symbol in &self.config.symbols_to_watch.clone() {
                        all_articles.extend(self.generate_sample_news(symbol));
                    }
                }

                let new_articles = self.cache_articles(all_articles);
                self.last_fetch = Some(Utc::now());
                self.store_articles(&new_articles);
            }

            NewsCollectorMessage::FetchCategoryNews { category } => {
                let articles = self.fetch_from_newsapi(&category).await;
                let new_articles = self.cache_articles(articles);
                self.store_articles(&new_articles);
            }

            NewsCollectorMessage::StartPeriodicFetch { interval_secs } => {
                // TODO: 스케줄러 연동
            }

            NewsCollectorMessage::StopPeriodicFetch => {
            }

            NewsCollectorMessage::NewsCollected { articles } => {
                let new_articles = self.cache_articles(articles);
                self.store_articles(&new_articles);
            }
        }
    }

    async fn pre_start(&mut self) {
    }
}

// ============================================================
// API 응답 타입
// ============================================================

#[derive(Debug, Deserialize)]
struct NewsApiResponse {
    articles: Vec<NewsApiArticle>,
}

#[derive(Debug, Deserialize)]
struct NewsApiArticle {
    title: String,
    description: Option<String>,
    content: Option<String>,
    url: String,
    #[serde(rename = "publishedAt")]
    published_at: String,
}

#[derive(Debug, Deserialize)]
struct AlphaVantageNewsResponse {
    feed: Option<Vec<AlphaVantageNews>>,
}

#[derive(Debug, Deserialize)]
struct AlphaVantageNews {
    title: String,
    summary: String,
    url: String,
    time_published: String,
}

#[derive(Debug, Deserialize)]
struct FinnhubNews {
    id: i64,
    headline: String,
    summary: String,
    url: String,
    datetime: i64,
    related: String,
    category: String,
}

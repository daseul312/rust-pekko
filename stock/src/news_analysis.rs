//! News Analysis Module
//!
//! Integrates with Coreon News to provide stock news analysis.
//! Path: /Volumes/T7/Work/safety-news/data/articles.json

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use anyhow::Result;
use tracing::{ warn};

/// Default path to Coreon News articles
const DEFAULT_ARTICLES_PATH: &str = "/Volumes/T7/Work/safety-news/data/articles.json";

/// News article from Coreon News
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewsArticle {
    pub id: String,
    pub title: String,
    pub url: String,
    pub summary: Option<String>,
    pub content: Option<String>,
    pub author: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    pub collected_at: DateTime<Utc>,
    pub source_key: String,
    pub source_name: String,
    pub category: String,
    pub region: String,
    pub tags: Vec<String>,
    pub vertical: String,
}

impl NewsArticle {
    /// Check if this is a Korean market news
    pub fn is_korean(&self) -> bool {
        self.region == "KR"
    }

    /// Check if this is a US market news  
    pub fn is_us(&self) -> bool {
        self.region == "US" || self.region == "Global"
    }

    /// Get category for analysis
    pub fn analysis_category(&self) -> NewsCategory {
        match self.category.as_str() {
            "Market" => NewsCategory::Market,
            "Investment" => NewsCategory::Investment,
            "Macro" => NewsCategory::Macro,
            "Earnings" => NewsCategory::Earnings,
            "IPO" => NewsCategory::IPO,
            "Crypto" => NewsCategory::Crypto,
            _ => NewsCategory::General,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NewsCategory {
    Market,
    Investment,
    Macro,
    Earnings,
    IPO,
    Crypto,
    General,
}

/// Sentiment analysis result
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Sentiment {
    VeryPositive,  // 매우 긍정
    Positive,      // 긍정
    Neutral,       // 중립
    Negative,      // 부정
    VeryNegative,  // 매우 부정
}

impl Sentiment {
    /// Convert to trading signal weight (-1.0 to 1.0)
    pub fn to_weight(&self) -> f64 {
        match self {
            Sentiment::VeryPositive => 1.0,
            Sentiment::Positive => 0.5,
            Sentiment::Neutral => 0.0,
            Sentiment::Negative => -0.5,
            Sentiment::VeryNegative => -1.0,
        }
    }
}

/// News with sentiment analysis
#[derive(Debug, Clone)]
pub struct AnalyzedNews {
    pub article: NewsArticle,
    pub sentiment: Sentiment,
    pub confidence: f64,  // 0.0 ~ 1.0
    pub keywords: Vec<String>,
    pub mentioned_stocks: Vec<String>,
}

/// News reader - loads articles from Coreon News
pub struct NewsReader {
    articles_path: PathBuf,
}

impl NewsReader {
    pub fn new() -> Self {
        Self {
            articles_path: PathBuf::from(DEFAULT_ARTICLES_PATH),
        }
    }

    pub fn with_path(path: impl Into<PathBuf>) -> Self {
        Self {
            articles_path: path.into(),
        }
    }

    /// Load all stock news articles
    pub fn load_stock_news(&self) -> Result<Vec<NewsArticle>> {
        let content = std::fs::read_to_string(&self.articles_path)?;
        let all_articles: Vec<NewsArticle> = serde_json::from_str(&content)?;
        
        let stock_articles: Vec<NewsArticle> = all_articles
            .into_iter()
            .filter(|a| a.vertical == "stock")
            .collect();
        Ok(stock_articles)
    }

    /// Load recent stock news (within hours)
    pub fn load_recent_news(&self, hours: i64) -> Result<Vec<NewsArticle>> {
        let articles = self.load_stock_news()?;
        let cutoff = Utc::now() - chrono::Duration::hours(hours);
        
        let recent: Vec<NewsArticle> = articles
            .into_iter()
            .filter(|a| {
                a.published_at
                    .map(|t| t > cutoff)
                    .unwrap_or(a.collected_at > cutoff)
            })
            .collect();
        Ok(recent)
    }

    /// Load news by region
    pub fn load_by_region(&self, region: &str) -> Result<Vec<NewsArticle>> {
        let articles = self.load_stock_news()?;
        
        let filtered: Vec<NewsArticle> = articles
            .into_iter()
            .filter(|a| a.region == region)
            .collect();
        
        Ok(filtered)
    }

    /// Load Korean stock news
    pub fn load_korean_news(&self) -> Result<Vec<NewsArticle>> {
        self.load_by_region("KR")
    }
}

impl Default for NewsReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple keyword-based sentiment analyzer
pub struct KeywordSentimentAnalyzer {
    positive_keywords: Vec<&'static str>,
    negative_keywords: Vec<&'static str>,
    very_positive_keywords: Vec<&'static str>,
    very_negative_keywords: Vec<&'static str>,
}

impl KeywordSentimentAnalyzer {
    pub fn new() -> Self {
        Self {
            positive_keywords: vec![
                // English
                "rise", "gain", "surge", "rally", "record", "high", "growth", 
                "profit", "bullish", "upgrade", "beat", "strong", "positive",
                // Korean
                "상승", "급등", "신고점", "호재", "증가", "강세", "상향", 
                "호실적", "성장", "흑자", "돌파", "긍정적",
            ],
            negative_keywords: vec![
                // English
                "fall", "drop", "decline", "loss", "bearish", "downgrade",
                "miss", "weak", "concern", "warning", "risk", "fear",
                // Korean
                "하락", "급락", "폭락", "약세", "하향", "악재", "손실",
                "적자", "우려", "경고", "리스크", "부정적",
            ],
            very_positive_keywords: vec![
                "soar", "skyrocket", "breakthrough", "historic", "all-time high",
                "폭등", "대박", "역대급", "사상최고",
            ],
            very_negative_keywords: vec![
                "crash", "collapse", "plunge", "crisis", "catastrophe",
                "폭락", "붕괴", "위기", "공포", "투매",
            ],
        }
    }

    /// Analyze sentiment of a news article
    pub fn analyze(&self, article: &NewsArticle) -> AnalyzedNews {
        let text = format!(
            "{} {}",
            article.title,
            article.summary.as_deref().unwrap_or("")
        ).to_lowercase();

        let mut score = 0.0;
        let mut keyword_count = 0;
        let mut found_keywords = Vec::new();

        // Check very positive
        for kw in &self.very_positive_keywords {
            if text.contains(kw) {
                score += 2.0;
                keyword_count += 1;
                found_keywords.push(kw.to_string());
            }
        }

        // Check very negative
        for kw in &self.very_negative_keywords {
            if text.contains(kw) {
                score -= 2.0;
                keyword_count += 1;
                found_keywords.push(kw.to_string());
            }
        }

        // Check positive
        for kw in &self.positive_keywords {
            if text.contains(kw) {
                score += 1.0;
                keyword_count += 1;
                found_keywords.push(kw.to_string());
            }
        }

        // Check negative
        for kw in &self.negative_keywords {
            if text.contains(kw) {
                score -= 1.0;
                keyword_count += 1;
                found_keywords.push(kw.to_string());
            }
        }

        let sentiment = match score {
            s if s >= 3.0 => Sentiment::VeryPositive,
            s if s >= 1.0 => Sentiment::Positive,
            s if s <= -3.0 => Sentiment::VeryNegative,
            s if s <= -1.0 => Sentiment::Negative,
            _ => Sentiment::Neutral,
        };

        // Confidence based on keyword count
        let confidence = (keyword_count as f64 / 5.0).min(1.0);

        // Extract mentioned stocks (simplified - look for stock codes)
        let mentioned_stocks = extract_stock_codes(&text);

        AnalyzedNews {
            article: article.clone(),
            sentiment,
            confidence,
            keywords: found_keywords,
            mentioned_stocks,
        }
    }

    /// Analyze multiple articles and get aggregate sentiment
    pub fn analyze_batch(&self, articles: &[NewsArticle]) -> BatchSentiment {
        let analyzed: Vec<AnalyzedNews> = articles
            .iter()
            .map(|a| self.analyze(a))
            .collect();

        let total = analyzed.len() as f64;
        if total == 0.0 {
            return BatchSentiment {
                articles: vec![],
                average_score: 0.0,
                overall_sentiment: Sentiment::Neutral,
                positive_count: 0,
                negative_count: 0,
                neutral_count: 0,
            };
        }

        let mut positive_count = 0;
        let mut negative_count = 0;
        let mut neutral_count = 0;
        let mut total_score = 0.0;

        for a in &analyzed {
            total_score += a.sentiment.to_weight() * a.confidence;
            match a.sentiment {
                Sentiment::VeryPositive | Sentiment::Positive => positive_count += 1,
                Sentiment::VeryNegative | Sentiment::Negative => negative_count += 1,
                Sentiment::Neutral => neutral_count += 1,
            }
        }

        let average_score = total_score / total;
        let overall_sentiment = match average_score {
            s if s >= 0.5 => Sentiment::VeryPositive,
            s if s >= 0.2 => Sentiment::Positive,
            s if s <= -0.5 => Sentiment::VeryNegative,
            s if s <= -0.2 => Sentiment::Negative,
            _ => Sentiment::Neutral,
        };

        BatchSentiment {
            articles: analyzed,
            average_score,
            overall_sentiment,
            positive_count,
            negative_count,
            neutral_count,
        }
    }
}

impl Default for KeywordSentimentAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

/// Batch sentiment analysis result
#[derive(Debug, Clone)]
pub struct BatchSentiment {
    pub articles: Vec<AnalyzedNews>,
    pub average_score: f64,
    pub overall_sentiment: Sentiment,
    pub positive_count: usize,
    pub negative_count: usize,
    pub neutral_count: usize,
}

impl BatchSentiment {
    /// Get a trading recommendation based on news sentiment
    pub fn trading_recommendation(&self) -> NewsTradingSignal {
        match self.overall_sentiment {
            Sentiment::VeryPositive => NewsTradingSignal::StrongBuy,
            Sentiment::Positive => NewsTradingSignal::Buy,
            Sentiment::Neutral => NewsTradingSignal::Hold,
            Sentiment::Negative => NewsTradingSignal::Sell,
            Sentiment::VeryNegative => NewsTradingSignal::StrongSell,
        }
    }

    /// Get summary text
    pub fn summary(&self) -> String {
        format!(
            "📰 뉴스 분석: {}개 기사 | 긍정 {} / 중립 {} / 부정 {} | 점수: {:.2}",
            self.articles.len(),
            self.positive_count,
            self.neutral_count,
            self.negative_count,
            self.average_score
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NewsTradingSignal {
    StrongBuy,
    Buy,
    Hold,
    Sell,
    StrongSell,
}

impl NewsTradingSignal {
    pub fn emoji(&self) -> &'static str {
        match self {
            NewsTradingSignal::StrongBuy => "🚀",
            NewsTradingSignal::Buy => "📈",
            NewsTradingSignal::Hold => "➖",
            NewsTradingSignal::Sell => "📉",
            NewsTradingSignal::StrongSell => "🔻",
        }
    }
}

/// Extract Korean stock codes (6 digits) from text
fn extract_stock_codes(text: &str) -> Vec<String> {
    let mut codes = Vec::new();
    
    // Simple regex-like pattern matching for 6-digit codes
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    
    while i + 5 < chars.len() {
        if chars[i..i+6].iter().all(|c| c.is_ascii_digit()) {
            let code: String = chars[i..i+6].iter().collect();
            if !codes.contains(&code) {
                codes.push(code);
            }
            i += 6;
        } else {
            i += 1;
        }
    }
    
    codes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sentiment_analyzer() {
        let analyzer = KeywordSentimentAnalyzer::new();
        
        let article = NewsArticle {
            id: "test".to_string(),
            title: "코스피 신고점 경신, 삼성전자 급등".to_string(),
            url: "http://test.com".to_string(),
            summary: Some("시장이 강세를 보이며 상승".to_string()),
            content: None,
            author: None,
            published_at: None,
            collected_at: Utc::now(),
            source_key: "test".to_string(),
            source_name: "Test".to_string(),
            category: "Market".to_string(),
            region: "KR".to_string(),
            tags: vec![],
            vertical: "stock".to_string(),
        };

        let result = analyzer.analyze(&article);
        assert!(matches!(result.sentiment, Sentiment::Positive | Sentiment::VeryPositive));
    }

    #[test]
    fn test_negative_sentiment() {
        let analyzer = KeywordSentimentAnalyzer::new();
        
        let article = NewsArticle {
            id: "test".to_string(),
            title: "시장 폭락, 투자자 공포".to_string(),
            url: "http://test.com".to_string(),
            summary: Some("급락세 지속, 우려 확산".to_string()),
            content: None,
            author: None,
            published_at: None,
            collected_at: Utc::now(),
            source_key: "test".to_string(),
            source_name: "Test".to_string(),
            category: "Market".to_string(),
            region: "KR".to_string(),
            tags: vec![],
            vertical: "stock".to_string(),
        };

        let result = analyzer.analyze(&article);
        assert!(matches!(result.sentiment, Sentiment::Negative | Sentiment::VeryNegative));
    }

    #[test]
    fn test_extract_stock_codes() {
        let text = "삼성전자(005930)가 상승하고 SK하이닉스(000660)도 강세";
        let codes = extract_stock_codes(text);
        assert!(codes.contains(&"005930".to_string()));
        assert!(codes.contains(&"000660".to_string()));
    }
}

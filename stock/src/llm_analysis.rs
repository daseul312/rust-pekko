//! LLM-based News Analysis
//!
//! Uses Grok (xAI) for intelligent news sentiment analysis.
//! Includes caching to reduce API calls.

use anyhow::{Result, Context};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{ warn, error};
use crate::news_analysis::{NewsArticle, Sentiment, BatchSentiment};

const XAI_API_URL: &str = "https://api.x.ai/v1/chat/completions";
const DEFAULT_MODEL: &str = "grok-3-fast";
const CACHE_TTL_SECS: u64 = 1800;  // 30분 캐시

/// Cached analysis result
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedResult {
    analysis: BatchAnalysis,
    timestamp: u64,
    article_count: usize,
}

/// LLM-based sentiment analyzer with caching
pub struct LlmSentimentAnalyzer {
    client: Client,
    api_key: String,
    model: String,
    cache: Mutex<HashMap<String, CachedResult>>,
    cache_path: PathBuf,
}

/// LLM response for a single article
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArticleAnalysis {
    pub sentiment: String,
    pub score: f64,
    pub impact: String,
    pub summary: String,
    pub key_factors: Vec<String>,
}

/// Batch analysis result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchAnalysis {
    pub articles: Vec<ArticleAnalysis>,
    pub overall_sentiment: String,
    pub overall_score: f64,
    pub market_outlook: String,
    pub recommendation: String,
    pub confidence: f64,
    pub reasoning: String,
}

impl LlmSentimentAnalyzer {
    pub fn new() -> Result<Self> {
        let api_key = std::env::var("XAI_API_KEY")
            .context("XAI_API_KEY not set. Get it from https://console.x.ai")?;
        
        let cache_path = dirs::cache_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("stock-llm-cache.json");
        
        let cache = Self::load_cache(&cache_path);
        
        Ok(Self {
            client: Client::new(),
            api_key,
            model: DEFAULT_MODEL.to_string(),
            cache: Mutex::new(cache),
            cache_path,
        })
    }

    pub fn with_model(mut self, model: &str) -> Self {
        self.model = model.to_string();
        self
    }

    fn load_cache(path: &PathBuf) -> HashMap<String, CachedResult> {
        fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    fn save_cache(&self) {
        if let Ok(cache) = self.cache.lock() {
            if let Ok(json) = serde_json::to_string_pretty(&*cache) {
                let _ = fs::write(&self.cache_path, json);
            }
        }
    }

    fn cache_key(symbol: Option<&str>, article_count: usize) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // Cache key: symbol + hour (같은 시간대는 캐시 사용)
        let hour = now / 3600;
        format!("{}:{}:{}", symbol.unwrap_or("all"), article_count, hour)
    }

    fn get_cached(&self, key: &str) -> Option<BatchAnalysis> {
        let cache = self.cache.lock().ok()?;
        let cached = cache.get(key)?;
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        if now - cached.timestamp < CACHE_TTL_SECS {
            Some(cached.analysis.clone())
        } else {
            None
        }
    }

    fn set_cached(&self, key: &str, analysis: &BatchAnalysis, article_count: usize) {
        if let Ok(mut cache) = self.cache.lock() {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            
            cache.insert(key.to_string(), CachedResult {
                analysis: analysis.clone(),
                timestamp: now,
                article_count,
            });
            
            // 오래된 캐시 정리 (24시간 이상)
            cache.retain(|_, v| now - v.timestamp < 86400);
        }
        self.save_cache();
    }

    /// Analyze a batch of news articles using LLM (with caching)
    pub async fn analyze_batch(&self, articles: &[NewsArticle], symbol: Option<&str>) -> Result<BatchAnalysis> {
        if articles.is_empty() {
            return Ok(BatchAnalysis::empty());
        }

        // 캐시 확인
        let cache_key = Self::cache_key(symbol, articles.len());
        if let Some(cached) = self.get_cached(&cache_key) {
            return Ok(cached);
        }
        // Prepare news summaries (limit to avoid token overflow)
        let news_text = articles.iter()
            .take(20)
            .enumerate()
            .map(|(i, a)| {
                let desc = a.summary.as_deref()
                    .or(a.content.as_deref())
                    .unwrap_or("");
                format!("{}. [{}] {} - {}", 
                    i + 1, 
                    a.source_name,
                    a.title,
                    desc.chars().take(200).collect::<String>()
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let symbol_context = symbol.map(|s| format!("종목: {}\n", s)).unwrap_or_default();
        
        let prompt = format!(r#"당신은 주식 시장 전문 애널리스트입니다.
다음 뉴스 기사들을 분석하여 투자 관점에서 평가해주세요.

{symbol_context}
=== 뉴스 기사 ===
{news_text}
=================

다음 JSON 형식으로만 응답하세요 (다른 텍스트 없이):
{{
  "overall_sentiment": "very_positive|positive|neutral|negative|very_negative",
  "overall_score": -1.0부터 1.0 사이 숫자,
  "market_outlook": "시장 전망 한 문장",
  "recommendation": "strong_buy|buy|hold|sell|strong_sell",
  "confidence": 0.0부터 1.0 사이 (판단 확신도),
  "reasoning": "판단 근거 2-3문장",
  "key_positive": ["긍정적 요인 최대 3개"],
  "key_negative": ["부정적 요인 최대 3개"]
}}"#);

        let response = self.call_llm(&prompt).await?;
        let analysis = self.parse_response(&response)?;
        
        // 캐시 저장
        self.set_cached(&cache_key, &analysis, articles.len());
        
        Ok(analysis)
    }

    /// Analyze single article (for real-time)
    pub async fn analyze_single(&self, article: &NewsArticle) -> Result<ArticleAnalysis> {
        let content = article.summary.as_deref()
            .or(article.content.as_deref())
            .unwrap_or("");
        
        let prompt = format!(r#"주식 시장 관점에서 이 뉴스를 분석하세요:

제목: {}
내용: {}

JSON으로만 응답:
{{
  "sentiment": "very_positive|positive|neutral|negative|very_negative",
  "score": -1.0~1.0,
  "impact": "high|medium|low",
  "summary": "한 줄 요약",
  "key_factors": ["핵심 요인 1-3개"]
}}"#, 
            article.title,
            content
        );

        let response = self.call_llm(&prompt).await?;
        serde_json::from_str(&response).context("Failed to parse LLM response")
    }

    async fn call_llm(&self, prompt: &str) -> Result<String> {
        let request_body = serde_json::json!({
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a stock market analyst. Always respond in valid JSON only."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.3,
            "max_tokens": 1000
        });

        let response = self.client
            .post(XAI_API_URL)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .context("Failed to call xAI API")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            error!("xAI API error: {} - {}", status, error_text);
            anyhow::bail!("xAI API error: {}", status);
        }

        let json: serde_json::Value = response.json().await?;
        let content = json["choices"][0]["message"]["content"]
            .as_str()
            .context("No content in response")?
            .to_string();

        // Clean up response (remove markdown code blocks if present)
        let content = content
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim()
            .to_string();

        Ok(content)
    }

    fn parse_response(&self, response: &str) -> Result<BatchAnalysis> {
        let json: serde_json::Value = serde_json::from_str(response)
            .context("Failed to parse LLM JSON response")?;

        Ok(BatchAnalysis {
            articles: vec![],
            overall_sentiment: json["overall_sentiment"].as_str().unwrap_or("neutral").to_string(),
            overall_score: json["overall_score"].as_f64().unwrap_or(0.0),
            market_outlook: json["market_outlook"].as_str().unwrap_or("").to_string(),
            recommendation: json["recommendation"].as_str().unwrap_or("hold").to_string(),
            confidence: json["confidence"].as_f64().unwrap_or(0.5),
            reasoning: json["reasoning"].as_str().unwrap_or("").to_string(),
        })
    }
}

impl BatchAnalysis {
    pub fn empty() -> Self {
        Self {
            articles: vec![],
            overall_sentiment: "neutral".to_string(),
            overall_score: 0.0,
            market_outlook: "데이터 없음".to_string(),
            recommendation: "hold".to_string(),
            confidence: 0.0,
            reasoning: "분석할 뉴스가 없습니다.".to_string(),
        }
    }

    /// Convert to BatchSentiment for compatibility
    pub fn to_batch_sentiment(&self) -> BatchSentiment {
        let sentiment = match self.overall_sentiment.as_str() {
            "very_positive" => Sentiment::VeryPositive,
            "positive" => Sentiment::Positive,
            "negative" => Sentiment::Negative,
            "very_negative" => Sentiment::VeryNegative,
            _ => Sentiment::Neutral,
        };

        BatchSentiment {
            articles: vec![],
            average_score: self.overall_score,
            overall_sentiment: sentiment,
            positive_count: 0,
            negative_count: 0,
            neutral_count: 0,
        }
    }

    /// Get trading signal
    pub fn trading_signal(&self) -> &str {
        &self.recommendation
    }

    /// Summary for logging
    pub fn summary(&self) -> String {
        format!(
            "🤖 LLM 분석: {} (점수: {:.2}, 확신도: {:.0}%) | 추천: {} | {}",
            self.overall_sentiment,
            self.overall_score,
            self.confidence * 100.0,
            self.recommendation.to_uppercase(),
            self.market_outlook
        )
    }
}

//! xAI Grok API 클라이언트 구현

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::llm::client::*;

/// Grok API 응답 구조체
#[derive(Debug, Deserialize)]
pub struct GrokResponse {
    pub id: String,
    pub object: String,
    pub created: u64,
    pub model: String,
    pub choices: Vec<GrokChoice>,
    pub usage: GrokUsage,
}

#[derive(Debug, Deserialize)]
pub struct GrokChoice {
    pub index: u32,
    pub message: GrokMessage,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GrokMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Deserialize)]
pub struct GrokUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

/// Grok 클라이언트
pub struct GrokClient {
    config: LLMConfig,
    client: Client,
}

impl GrokClient {
    pub fn new(config: LLMConfig) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds))
            .build()
            .expect("Failed to create HTTP client");

        Self { config, client }
    }

    async fn send_request(&self, messages: Vec<GrokMessage>) -> Result<GrokResponse, LLMError> {
        #[derive(Serialize)]
        struct GrokRequest {
            model: String,
            messages: Vec<GrokMessage>,
            max_tokens: u32,
            temperature: f64,
        }

        let request = GrokRequest {
            model: self.config.model.clone(),
            messages,
            max_tokens: self.config.max_tokens,
            temperature: self.config.temperature,
        };

        let response = self.client
            .post(&self.config.base_url)
            .header("Authorization", format!("Bearer {}", self.config.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(LLMError::APIError {
                code: status.as_u16(),
                message: error_text,
            });
        }

        let grok_response: GrokResponse = response.json().await
            .map_err(|e| LLMError::ParsingError(format!("Grok 응답 파싱 실패: {}", e)))?;

        if grok_response.choices.is_empty() {
            return Err(LLMError::ParsingError("응답에 선택지가 없습니다".to_string()));
        }

        Ok(grok_response)
    }
}

#[async_trait]
impl LLMClient for GrokClient {
    async fn analyze_sentiment(
        &self,
        _request: SentimentAnalysisRequest,
    ) -> Result<SentimentAnalysisResult, LLMError> {
        // 간단한 모의 응답 (실제로는 API 호출)
        Ok(SentimentAnalysisResult {
            sentiment: SentimentScore {
                positive: 0.7,
                negative: 0.1,
                neutral: 0.2,
                overall: 0.6,
            },
            reasoning: "Grok 기반 감정 분석".to_string(),
            confidence: 0.85,
        })
    }

    async fn generate_market_report(
        &self,
        _request: MarketReportRequest,
    ) -> Result<LLMAnalysisResult, LLMError> {
        Ok(LLMAnalysisResult {
            content: "Grok 기반 시장 리포트".to_string(),
            confidence: 0.9,
            tokens_used: 160,
            model: self.config.model.clone(),
            timestamp: chrono::Utc::now(),
        })
    }

    async fn predict_price(
        &self,
        _request: PricePredictionRequest,
    ) -> Result<PricePredictionResult, LLMError> {
        Ok(PricePredictionResult {
            predicted_price: 155.0,
            confidence: 0.8,
            reasoning: "Grok 기반 가격 예측".to_string(),
            time_horizon: "1d".to_string(),
        })
    }

    async fn analyze_text(
        &self,
        prompt: String,
        _context: Option<serde_json::Value>,
    ) -> Result<LLMAnalysisResult, LLMError> {
        Ok(LLMAnalysisResult {
            content: format!("Grok 분석: {}", prompt),
            confidence: 0.85,
            tokens_used: (prompt.chars().count() / 4) as u32,
            model: self.config.model.clone(),
            timestamp: chrono::Utc::now(),
        })
    }

    fn available_models(&self) -> Vec<String> {
        vec!["grok-1".to_string(), "grok-1-beta".to_string()]
    }

    fn current_model(&self) -> &str {
        &self.config.model
    }

    fn estimate_tokens(&self, text: &str) -> u32 {
        (text.chars().count() / 4) as u32
    }
}
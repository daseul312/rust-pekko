//! LLM 클라이언트 공통 인터페이스 및 타입 정의

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;

/// LLM 설정
#[derive(Debug, Clone)]
pub struct LLMConfig {
    pub api_key: String,
    pub base_url: String,
    pub model: String,
    pub max_tokens: u32,
    pub temperature: f64,
    pub timeout_seconds: u64,
}

/// LLM 오류 타입
#[derive(Debug)]
pub enum LLMError {
    APIError { code: u16, message: String },
    ParsingError(String),
    NetworkError(String),
    TimeoutError,
    ConfigurationError(String),
}

impl fmt::Display for LLMError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LLMError::APIError { code, message } => write!(f, "API 오류 {}: {}", code, message),
            LLMError::ParsingError(msg) => write!(f, "파싱 오류: {}", msg),
            LLMError::NetworkError(msg) => write!(f, "네트워크 오류: {}", msg),
            LLMError::TimeoutError => write!(f, "타임아웃 오류"),
            LLMError::ConfigurationError(msg) => write!(f, "설정 오류: {}", msg),
        }
    }
}

impl std::error::Error for LLMError {}

impl From<reqwest::Error> for LLMError {
    fn from(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            LLMError::TimeoutError
        } else if error.is_connect() {
            LLMError::NetworkError(format!("연결 오류: {}", error))
        } else {
            LLMError::NetworkError(format!("HTTP 오류: {}", error))
        }
    }
}

impl From<std::env::VarError> for LLMError {
    fn from(error: std::env::VarError) -> Self {
        LLMError::ConfigurationError(format!("환경 변수 오류: {}", error))
    }
}

impl From<serde_json::Error> for LLMError {
    fn from(error: serde_json::Error) -> Self {
        LLMError::ParsingError(format!("JSON 파싱 오류: {}", error))
    }
}

/// 감정 점수
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentimentScore {
    pub positive: f64,
    pub negative: f64,
    pub neutral: f64,
    pub overall: f64,
}

/// 감정 분석 요청
#[derive(Debug, Clone)]
pub struct SentimentAnalysisRequest {
    pub text: String,
    pub context: Option<String>,
}

/// 감정 분석 결과
#[derive(Debug, Clone)]
pub struct SentimentAnalysisResult {
    pub sentiment: SentimentScore,
    pub reasoning: String,
    pub confidence: f64,
}

/// 시장 리포트 요청
#[derive(Debug, Clone)]
pub struct MarketReportRequest {
    pub symbol: String,
    pub time_range: String,
    pub include_technical: bool,
    pub include_fundamental: bool,
}

/// 가격 예측 요청
#[derive(Debug, Clone)]
pub struct PricePredictionRequest {
    pub symbol: String,
    pub time_horizon: String,
    pub current_price: Option<f64>,
    pub technical_data: Option<serde_json::Value>,
}

/// 가격 예측 결과
#[derive(Debug, Clone)]
pub struct PricePredictionResult {
    pub predicted_price: f64,
    pub confidence: f64,
    pub reasoning: String,
    pub time_horizon: String,
}

/// 투자 의견 요청
#[derive(Debug, Clone)]
pub struct InvestmentOpinionRequest {
    pub symbol: String,
    pub time_horizon: String,
    pub risk_tolerance: String,
}

/// 투자 의견 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvestmentOpinionResult {
    pub recommendation: String,
    pub confidence: f64,
    pub reasoning: String,
    pub entry_price_range: Option<String>,
    pub stop_loss: Option<f64>,
    pub target_price: Option<f64>,
    pub time_horizon: String,
    pub risk_level: String,
}

/// 시장 분석 요청
#[derive(Debug, Clone)]
pub struct MarketAnalysisRequest {
    pub symbol: String,
    pub analysis_type: String,
    pub time_horizon: String,
    pub context: Option<serde_json::Value>,
}

/// 투자 인사이트 응답
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvestmentInsightResponse {
    pub insights: Vec<String>,
    pub confidence: f64,
    pub recommendations: Vec<String>,
}

/// LLM 분석 결과
#[derive(Debug, Clone)]
pub struct LLMAnalysisResult {
    pub content: String,
    pub confidence: f64,
    pub tokens_used: u32,
    pub model: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// LLM 클라이언트 트레이트
#[async_trait]
pub trait LLMClient: Send + Sync {
    /// 감정 분석
    async fn analyze_sentiment(
        &self,
        request: SentimentAnalysisRequest,
    ) -> Result<SentimentAnalysisResult, LLMError>;

    /// 시장 리포트 생성
    async fn generate_market_report(
        &self,
        request: MarketReportRequest,
    ) -> Result<LLMAnalysisResult, LLMError>;

    /// 가격 예측
    async fn predict_price(
        &self,
        request: PricePredictionRequest,
    ) -> Result<PricePredictionResult, LLMError>;

    /// 일반 텍스트 분석
    async fn analyze_text(
        &self,
        prompt: String,
        context: Option<serde_json::Value>,
    ) -> Result<LLMAnalysisResult, LLMError>;

    /// 투자 의견 생성
    async fn generate_investment_opinion(
        &self,
        request: InvestmentOpinionRequest,
    ) -> Result<InvestmentOpinionResult, LLMError> {
        // 기본 구현 (모의)
        Ok(InvestmentOpinionResult {
            recommendation: "HOLD".to_string(),
            confidence: 0.5,
            reasoning: "기본 투자 의견".to_string(),
            entry_price_range: None,
            stop_loss: None,
            target_price: None,
            time_horizon: request.time_horizon,
            risk_level: "Medium".to_string(),
        })
    }

    /// 사용 가능한 모델 목록
    fn available_models(&self) -> Vec<String>;

    /// 현재 사용 중인 모델
    fn current_model(&self) -> &str;

    /// 토큰 수 추정
    fn estimate_tokens(&self, text: &str) -> u32;
}

/// 테스트용 모의 클라이언트
pub struct MockLLMClient;

#[async_trait]
impl LLMClient for MockLLMClient {
    async fn analyze_sentiment(
        &self,
        _request: SentimentAnalysisRequest,
    ) -> Result<SentimentAnalysisResult, LLMError> {
        Ok(SentimentAnalysisResult {
            sentiment: SentimentScore {
                positive: 0.5,
                negative: 0.3,
                neutral: 0.2,
                overall: 0.2,
            },
            reasoning: "모의 감정 분석".to_string(),
            confidence: 0.7,
        })
    }

    async fn generate_market_report(
        &self,
        _request: MarketReportRequest,
    ) -> Result<LLMAnalysisResult, LLMError> {
        Ok(LLMAnalysisResult {
            content: "모의 시장 리포트".to_string(),
            confidence: 0.75,
            tokens_used: 100,
            model: "mock".to_string(),
            timestamp: chrono::Utc::now(),
        })
    }

    async fn predict_price(
        &self,
        request: PricePredictionRequest,
    ) -> Result<PricePredictionResult, LLMError> {
        Ok(PricePredictionResult {
            predicted_price: 150.0,
            confidence: 0.6,
            reasoning: format!("{} 심볼에 대한 모의 예측", request.symbol),
            time_horizon: request.time_horizon,
        })
    }

    async fn analyze_text(
        &self,
        prompt: String,
        _context: Option<serde_json::Value>,
    ) -> Result<LLMAnalysisResult, LLMError> {
        Ok(LLMAnalysisResult {
            content: format!("모의 분석: {}", prompt),
            confidence: 0.7,
            tokens_used: (prompt.chars().count() / 4) as u32,
            model: "mock".to_string(),
            timestamp: chrono::Utc::now(),
        })
    }

    fn available_models(&self) -> Vec<String> {
        vec!["mock".to_string()]
    }

    fn current_model(&self) -> &str {
        "mock"
    }

    fn estimate_tokens(&self, text: &str) -> u32 {
        (text.chars().count() / 4) as u32
    }
}

/// LLM 클라이언트 생성 함수들
pub fn create_default_client() -> Result<std::sync::Arc<dyn LLMClient>, Box<dyn std::error::Error>> {
    // 환경 변수에서 기본 클라이언트 결정
    let provider = std::env::var("DEFAULT_LLM_PROVIDER").unwrap_or_else(|_| "GROK".to_string());

    match provider.to_uppercase().as_str() {
        "OPENAI" => {
            let config = LLMConfig {
                api_key: std::env::var("OPENAI_API_KEY")?,
                base_url: std::env::var("OPENAI_BASE_URL").unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
                model: std::env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4".to_string()),
                max_tokens: std::env::var("OPENAI_MAX_TOKENS").unwrap_or_else(|_| "4000".to_string()).parse()?,
                temperature: std::env::var("OPENAI_TEMPERATURE").unwrap_or_else(|_| "0.7".to_string()).parse()?,
                timeout_seconds: std::env::var("OPENAI_TIMEOUT").unwrap_or_else(|_| "30".to_string()).parse()?,
            };
            Ok(std::sync::Arc::from(crate::llm::openai::OpenAIClient::new(config)))
        }
        "GROK" => {
            let config = LLMConfig {
                api_key: std::env::var("GROK_API_KEY")?,
                base_url: std::env::var("GROK_BASE_URL").unwrap_or_else(|_| "https://api.x.ai/v1".to_string()),
                model: std::env::var("GROK_MODEL").unwrap_or_else(|_| "grok-1".to_string()),
                max_tokens: std::env::var("GROK_MAX_TOKENS").unwrap_or_else(|_| "4000".to_string()).parse()?,
                temperature: std::env::var("GROK_TEMPERATURE").unwrap_or_else(|_| "0.7".to_string()).parse()?,
                timeout_seconds: std::env::var("GROK_TIMEOUT").unwrap_or_else(|_| "30".to_string()).parse()?,
            };
            Ok(std::sync::Arc::from(crate::llm::grok::GrokClient::new(config)))
        }
        _ => Err("지원되지 않는 LLM 제공자입니다".into()),
    }
}

/// 테스트용 클라이언트 생성
pub fn create_test_client() -> std::sync::Arc<dyn LLMClient> {
    std::sync::Arc::from(MockLLMClient)
}
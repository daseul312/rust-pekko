use serde::{Deserialize, Serialize};
use std::env;

/// API 설정을 관리하는 구조체
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    pub kis: KisApiConfig,
    pub alpha_vantage: AlphaVantageConfig,
    pub news_api: NewsApiConfig,
}

/// KIS API 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KisApiConfig {
    pub app_key: String,
    pub app_secret: String,
    pub account_number: String,
    pub base_url: String,
    pub ws_url: String,
}

/// Alpha Vantage API 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlphaVantageConfig {
    pub api_key: String,
    pub base_url: String,
    pub rate_limit_per_minute: u32,
}

/// News API 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewsApiConfig {
    pub api_key: Option<String>,
    pub base_url: String,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self::from_env().unwrap_or_else(|_| Self::mock_config())
    }
}

impl ApiConfig {
    /// 환경변수에서 API 설정을 로드합니다
    pub fn from_env() -> Result<Self, env::VarError> {
        Ok(Self {
            kis: KisApiConfig {
                app_key: env::var("KIS_APP_KEY")?,
                app_secret: env::var("KIS_APP_SECRET")?,
                account_number: env::var("KIS_ACCOUNT_NUMBER")?,
                base_url: "https://openapi.koreainvestment.com:9443".to_string(),
                ws_url: "ws://ops.koreainvestment.com:31000".to_string(),
            },
            alpha_vantage: AlphaVantageConfig {
                api_key: env::var("ALPHA_VANTAGE_API_KEY")?,
                base_url: "https://www.alphavantage.co/query".to_string(),
                rate_limit_per_minute: 5, // 무료 계정 제한
            },
            news_api: NewsApiConfig {
                api_key: env::var("NEWS_API_KEY").ok(),
                base_url: "https://newsapi.org/v2".to_string(),
            },
        })
    }

    /// 테스트용 Mock 설정
    pub fn mock_config() -> Self {
        Self {
            kis: KisApiConfig {
                app_key: "mock_app_key".to_string(),
                app_secret: "mock_app_secret".to_string(),
                account_number: "mock_account".to_string(),
                base_url: "https://openapi.koreainvestment.com:9443".to_string(),
                ws_url: "ws://ops.koreainvestment.com:31000".to_string(),
            },
            alpha_vantage: AlphaVantageConfig {
                api_key: "mock_api_key".to_string(),
                base_url: "https://www.alphavantage.co/query".to_string(),
                rate_limit_per_minute: 5,
            },
            news_api: NewsApiConfig {
                api_key: Some("mock_news_api_key".to_string()),
                base_url: "https://newsapi.org/v2".to_string(),
            },
        }
    }

    /// API 키가 유효한지 확인
    pub fn is_valid(&self) -> bool {
        !self.kis.app_key.is_empty() && 
        !self.alpha_vantage.api_key.is_empty() &&
        self.kis.app_key != "mock_app_key"
    }

    /// 특정 API가 활성화되어 있는지 확인
    pub fn is_kis_enabled(&self) -> bool {
        self.kis.app_key != "mock_app_key" && !self.kis.app_key.is_empty()
    }

    pub fn is_alpha_vantage_enabled(&self) -> bool {
        self.alpha_vantage.api_key != "mock_api_key" && !self.alpha_vantage.api_key.is_empty()
    }

    pub fn is_news_api_enabled(&self) -> bool {
        self.news_api.api_key.is_some() && 
        !self.news_api.api_key.as_ref().unwrap().is_empty()
    }
}

/// API 타입 열거형
#[derive(Debug, Clone, PartialEq)]
pub enum ApiType {
    Kis,
    AlphaVantage,
    NewsApi,
    Mock, // 시뮬레이션용
}

impl ApiType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ApiType::Kis => "KIS",
            ApiType::AlphaVantage => "Alpha Vantage",
            ApiType::NewsApi => "News API",
            ApiType::Mock => "Mock/Simulation",
        }
    }
}

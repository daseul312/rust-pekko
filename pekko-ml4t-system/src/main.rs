//! ML4T Pekko-Rust Agentic AI 자동매매 시스템 메인 실행 파일

use tracing::{ warn, error};
use anyhow::Result;
use chrono::{Local, Timelike};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use serde::{Deserialize, Serialize};

// ML4T 시스템 모듈들
use pekko_ml4t_system::*;

/// ML4T 자동매매 시스템의 메인 엔트리 포인트
#[tokio::main]
async fn main() -> Result<()> {
    // 로깅 초기화
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    // 시스템 상태 확인
    check_system_status().await?;

    // ML4T 시스템 초기화
    let system = ML4TSystem::new().await?;
    // 시스템 실행
    system.run().await?;

    Ok(())
}

/// 시스템 상태 확인
async fn check_system_status() -> Result<()> {
    // 환경 변수 확인
    let env_status = check_environment_variables();
    // API 연결 확인
    let api_status = check_api_connectivity().await;
    // 데이터베이스 연결 확인 (선택사항)
    let db_status = check_database_connection().await;
    Ok(())
}

/// 환경 변수 확인
fn check_environment_variables() -> bool {
    let required_vars = vec![
        "GROK_API_KEY",
        "DEFAULT_LLM_PROVIDER",
        "ALPHA_VANTAGE_API_KEY",
        "KIS_APP_KEY",
        "KIS_APP_SECRET",
        "NOTION_TOKEN",
    ];

    for var in required_vars {
        if std::env::var(var).is_err() {
            warn!("환경 변수 {}가 설정되지 않음", var);
            return false;
        }
    }

    true
}

/// API 연결성 확인
async fn check_api_connectivity() -> bool {
    // LLM API 연결 확인
    match llm::create_default_client() {
        Ok(client) => {
            true
        }
        Err(e) => {
            warn!("    LLM API: ❌ 연결 실패 - {}", e);
            false
        }
    }
}

/// 데이터베이스 연결 확인
async fn check_database_connection() -> bool {
    match std::env::var("DATABASE_URL") {
        Ok(url) if !url.is_empty() => {
            true
        }
        _ => {
            false
        }
    }
}

/// 시장 데이터 구조체
#[derive(Debug, Clone)]
pub struct MarketData {
    pub price: f64,
    pub volume: u64,
    pub change_percent: f64,
    pub latest_trading_day: String,
}

/// ML4T 메인 시스템
pub struct ML4TSystem {
    llm_client: Arc<dyn llm::LLMClient>,
    market_data_actor: functional::market_data_actor::MarketDataActor,
    sentiment_actor: functional::sentiment_actor::SentimentActor,
    prediction_actor: functional::prediction_actor::PredictionActor,
    report_actor: functional::report_actor::ReportGenerationActor,
    llm_analysis_actor: functional::llm_analysis_actor::LLMAnalysisActor,
}

impl ML4TSystem {
    /// 시스템 초기화
    pub async fn new() -> Result<Self> {
        // LLM 클라이언트 생성
        let llm_client: Arc<dyn llm::LLMClient> = match llm::create_default_client() {
            Ok(client) => Arc::from(client),
            Err(e) => {
                warn!("LLM 클라이언트 생성 실패, 모의 클라이언트 사용: {}", e);
                llm::create_test_client()
            }
        };

        // 액터들 생성
        let market_data_actor = functional::market_data_actor::MarketDataActor::new();
        let sentiment_actor = functional::sentiment_actor::SentimentActor::new();

        let prediction_actor = functional::prediction_actor::PredictionActor::new_with_mock_client();
        let report_actor = functional::report_actor::ReportGenerationActor::new_with_mock_client();
        let llm_analysis_actor = functional::llm_analysis_actor::LLMAnalysisActor::new_with_mock_client();
        Ok(Self {
            llm_client,
            market_data_actor,
            sentiment_actor,
            prediction_actor,
            report_actor,
            llm_analysis_actor,
        })
    }

    /// 시스템 실행
    pub async fn run(&self) -> Result<()> {
        // 데모 시나리오 실행
        self.run_demo_scenario().await?;

        // 시스템 모니터링 시작
        self.start_monitoring().await?;

        // 대기 (실제 운영에서는 무한 루프)
        sleep(Duration::from_secs(1)).await;
        Ok(())
    }

    /// 데모 시나리오 실행
    async fn run_demo_scenario(&self) -> Result<()> {
        let test_symbols = vec!["AAPL", "GOOGL", "MSFT"];

        for symbol in &test_symbols {
            // 1. 시장 데이터 수집 (모의)
            self.simulate_market_data_collection(symbol).await?;

            // 2. 감정 분석 (모의)
            self.simulate_sentiment_analysis(symbol).await?;

            // 3. 가격 예측 (LLM 사용 가능시)
            if self.llm_client.current_model() != "mock" {
                self.simulate_price_prediction(symbol).await?;
            }
        }
        Ok(())
    }

    /// 시장 데이터 수집 (실제 API 사용)
    async fn simulate_market_data_collection(&self, symbol: &str) -> Result<()> {
        // 실제 Alpha Vantage API 호출 시도
        match self.collect_real_market_data(symbol).await {
            Ok(data) => {
            }
            Err(e) => {
                warn!("    ⚠️ 실제 API 호출 실패, 모의 데이터 사용: {}", e);
                // 폴백으로 모의 데이터 생성
                let mock_price = 150.0 + (symbol.len() as f64 * 10.0);
            }
        }

        Ok(())
    }

    /// 실제 Alpha Vantage API로 시장 데이터 수집 (향후 구현)
    async fn collect_real_market_data(&self, _symbol: &str) -> Result<MarketData> {
        // TODO: 실제 API 구현
        // 현재는 모의 데이터 반환
        Ok(MarketData {
            price: 150.0,
            volume: 1000000,
            change_percent: 2.5,
            latest_trading_day: "2024-01-13".to_string(),
        })
    }

    /// 감정 분석 시뮬레이션
    async fn simulate_sentiment_analysis(&self, symbol: &str) -> Result<()> {
        // 실제로는 뉴스 API와 X API 호출
        let sentiment_score = 0.7; // 긍정적
        Ok(())
    }

    /// 가격 예측 시뮬레이션
    async fn simulate_price_prediction(&self, symbol: &str) -> Result<()> {
        // 실제로는 LLM API 호출
        match self.llm_client.analyze_text(
            format!("{} 주식의 향후 1주일 가격 전망을 분석해주세요.", symbol),
            None
        ).await {
            Ok(result) => {
            }
            Err(e) => {
                warn!("    ⚠️ LLM 분석 실패: {}", e);
            }
        }

        Ok(())
    }

    /// 시스템 모니터링 시작
    async fn start_monitoring(&self) -> Result<()> {
        // 주기적 상태 체크 (실제 운영에서는 별도 태스크로 실행)
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_system_initialization() {
        let system = ML4TSystem::new().await;
        assert!(system.is_ok(), "시스템 초기화 실패");
    }

    #[tokio::test]
    async fn test_environment_check() {
        let result = check_environment_variables();
        // 환경 변수가 모두 설정되어 있어야 함
        assert!(result, "환경 변수 설정 불완전");
    }
}
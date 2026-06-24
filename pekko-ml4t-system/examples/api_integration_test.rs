use pekko_ml4t_system::api::ApiClient;
use pekko_ml4t_system::api::real_kis_api::RealKisApi;
use pekko_ml4t_system::api::real_alpha_vantage_api::RealAlphaVantageApi;
use pekko_ml4t_system::config::api_config::{KisApiConfig, AlphaVantageConfig};
use anyhow::Result;
use tracing::{ error};

#[tokio::main]
async fn main() -> Result<()> {
    // 로깅 초기화
    tracing_subscriber::fmt::init();
    // KIS API 테스트
    if let Ok(kis_config) = get_kis_config() {
        test_kis_api(kis_config).await?;
    } else {
    }
    
    // Alpha Vantage API 테스트
    if let Ok(alpha_config) = get_alpha_vantage_config() {
        test_alpha_vantage_api(alpha_config).await?;
    } else {
    }
    Ok(())
}

fn get_kis_config() -> Result<KisApiConfig> {
    Ok(KisApiConfig {
        app_key: std::env::var("KIS_APP_KEY")
            .unwrap_or_else(|_| "test_app_key".to_string()),
        app_secret: std::env::var("KIS_APP_SECRET")
            .unwrap_or_else(|_| "test_app_secret".to_string()),
        account_number: std::env::var("KIS_ACCOUNT_NUMBER")
            .unwrap_or_else(|_| "test_account".to_string()),
        base_url: "https://openapi.koreainvestment.com:9443".to_string(),
        ws_url: "wss://openapi.koreainvestment.com:9443".to_string(),
    })
}

fn get_alpha_vantage_config() -> Result<AlphaVantageConfig> {
    Ok(AlphaVantageConfig {
        api_key: std::env::var("ALPHA_VANTAGE_API_KEY")
            .unwrap_or_else(|_| "demo".to_string()), // Alpha Vantage는 demo 키 제공
        base_url: "https://www.alphavantage.co/query".to_string(),
        rate_limit_per_minute: 5, // 무료 계정 제한
    })
}

async fn test_kis_api(config: KisApiConfig) -> Result<()> {
    let mut kis_client = RealKisApi::new(config);
    if let Err(e) = kis_client.authenticate().await {
        error!("KIS API 인증 실패: {}", e);
        return Ok(()); // 인증 실패해도 계속 진행
    }
    let symbols = vec!["005930", "000660", "035420"]; // 삼성전자, SK하이닉스, NAVER
    
    for symbol in symbols {
        match kis_client.get_market_data(&symbol).await {
            Ok(data) => {
            }
            Err(e) => {
                error!("{} 데이터 조회 실패: {}", symbol, e);
            }
        }
        
        // API 제한을 위해 잠시 대기
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }
    match kis_client.get_account_info().await {
        Ok(account) => {
        }
        Err(e) => {
            error!("계좌 정보 조회 실패: {}", e);
        }
    }
    
    Ok(())
}

async fn test_alpha_vantage_api(config: AlphaVantageConfig) -> Result<()> {
    let mut alpha_client = RealAlphaVantageApi::new(config);
    if let Err(e) = alpha_client.authenticate().await {
        error!("Alpha Vantage API 인증 실패: {}", e);
        return Ok(());
    }
    let symbols = vec!["AAPL", "MSFT", "GOOGL"]; // Apple, Microsoft, Google
    
    for symbol in symbols {
        match alpha_client.get_market_data(&symbol).await {
            Ok(data) => {
            }
            Err(e) => {
                error!("{} 데이터 조회 실패: {}", symbol, e);
            }
        }
        
        // API 제한을 위해 잠시 대기 (무료 계정은 1분에 5회 제한)
        tokio::time::sleep(tokio::time::Duration::from_secs(12)).await;
    }
    
    Ok(())
}

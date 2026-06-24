//! 실제 API 연동 예제
//! 
//! 다양한 API들을 실제로 연동하여 데이터를 수집하고 거래를 실행하는 예제

use crate::api::{
    KisApiClient, AlphaVantageClient, NewsApiClient, WebSocketClient,
    ApiClient, ApiConfig, RateLimiter, RateLimiterBuilder,
};
use crate::actors::{MarketData, NewsArticle};
use anyhow::Result;
use tracing::{ error, warn};
use std::collections::HashMap;

/// API 통합 예제 실행
pub async fn run_api_integration_example() -> Result<()> {
    // Rate Limiter 설정
    let rate_limiter = RateLimiterBuilder::new()
        .add_kis_api()
        .add_alpha_vantage_free()
        .add_news_api()
        .build();

    // 1. KIS API 연동 예제
    if let Err(e) = kis_api_example().await {
        error!("KIS API 예제 실패: {}", e);
    }

    // 2. Alpha Vantage API 연동 예제
    if let Err(e) = alpha_vantage_example().await {
        error!("Alpha Vantage API 예제 실패: {}", e);
    }

    // 3. News API 연동 예제
    if let Err(e) = news_api_example().await {
        error!("News API 예제 실패: {}", e);
    }

    // 4. WebSocket 실시간 데이터 예제
    if let Err(e) = websocket_example().await {
        error!("WebSocket 예제 실패: {}", e);
    }

    // 5. Rate Limiter 예제
    if let Err(e) = rate_limiter_example(rate_limiter).await {
        error!("Rate Limiter 예제 실패: {}", e);
    }
    Ok(())
}

/// KIS API 연동 예제
async fn kis_api_example() -> Result<()> {
    // KIS API 설정
    let config = ApiConfig {
        api_key: std::env::var("KIS_APP_KEY").unwrap_or_default(),
        api_secret: std::env::var("KIS_APP_SECRET").unwrap_or_default(),
        base_url: "https://openapi.koreainvestment.com:9443".to_string(),
        timeout_ms: 5000,
        retry_attempts: 3,
        rate_limit: crate::api::RateLimitConfig {
            requests_per_second: 10,
            requests_per_minute: 60,
            requests_per_hour: 3600,
            requests_per_day: 86400,
        },
    };

    if config.api_key.is_empty() {
        warn!("KIS API 키가 설정되지 않았습니다. 환경 변수 KIS_APP_KEY와 KIS_APP_SECRET를 설정하세요.");
        return Ok(());
    }

    // KIS API 클라이언트 생성
    let mut kis_client = KisApiClient::new(config).await?;
    kis_client.authenticate().await?;

    // 1. 시장 데이터 조회
    let symbols = vec!["000270", "005930", "000660"]; // 기아, 삼성전자, SK하이닉스
    
    for symbol in symbols {
        match kis_client.get_market_data(symbol).await {
            Ok(market_data) => {
                // Actor용 MarketData로 변환
                let actor_data = kis_client.to_actor_market_data(&market_data);
            }
            Err(e) => {
                error!("{} 데이터 조회 실패: {}", symbol, e);
            }
        }
        
        // API 제한을 위한 대기
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    // 2. 계좌 정보 조회
    match kis_client.get_account_info().await {
        Ok(account) => {
        }
        Err(e) => {
            error!("계좌 정보 조회 실패: {}", e);
        }
    }

    // 3. 포지션 조회
    match kis_client.get_positions().await {
        Ok(positions) => {
            for position in positions {
            }
        }
        Err(e) => {
            error!("포지션 조회 실패: {}", e);
        }
    }

    Ok(())
}

/// Alpha Vantage API 연동 예제
async fn alpha_vantage_example() -> Result<()> {
    // Alpha Vantage API 설정
    let config = ApiConfig {
        api_key: std::env::var("ALPHA_VANTAGE_API_KEY").unwrap_or_default(),
        api_secret: "".to_string(), // Alpha Vantage는 시크릿 불필요
        base_url: "https://www.alphavantage.co".to_string(),
        timeout_ms: 10000,
        retry_attempts: 3,
        rate_limit: crate::api::RateLimitConfig {
            requests_per_second: 5,
            requests_per_minute: 5,
            requests_per_hour: 300,
            requests_per_day: 500,
        },
    };

    if config.api_key.is_empty() {
        warn!("Alpha Vantage API 키가 설정되지 않았습니다. 환경 변수 ALPHA_VANTAGE_API_KEY를 설정하세요.");
        return Ok(());
    }

    // Alpha Vantage 클라이언트 생성
    let mut alpha_client = AlphaVantageClient::new(config).await?;
    alpha_client.authenticate().await?;

    // 1. 미국 주식 데이터 조회
    let us_symbols = vec!["AAPL", "MSFT", "GOOGL", "TSLA"];
    
    for symbol in us_symbols {
        match alpha_client.get_market_data(symbol).await {
            Ok(market_data) => {
            }
            Err(e) => {
                error!("{} 데이터 조회 실패: {}", symbol, e);
            }
        }
        
        // API 제한을 위한 대기 (Alpha Vantage는 매우 제한적)
        tokio::time::sleep(tokio::time::Duration::from_millis(12000)).await;
    }

    // 2. 기술적 지표 조회
    match alpha_client.get_multiple_indicators("AAPL").await {
        Ok(indicators) => {
            for (indicator, value) in indicators {
            }
        }
        Err(e) => {
            error!("기술적 지표 조회 실패: {}", e);
        }
    }

    Ok(())
}

/// News API 연동 예제
async fn news_api_example() -> Result<()> {
    // News API 설정
    let config = ApiConfig {
        api_key: std::env::var("NEWS_API_KEY").unwrap_or_default(),
        api_secret: "".to_string(),
        base_url: "https://newsapi.org".to_string(),
        timeout_ms: 5000,
        retry_attempts: 3,
        rate_limit: crate::api::RateLimitConfig {
            requests_per_second: 1,
            requests_per_minute: 60,
            requests_per_hour: 3600,
            requests_per_day: 86400,
        },
    };

    if config.api_key.is_empty() {
        warn!("News API 키가 설정되지 않았습니다. 환경 변수 NEWS_API_KEY를 설정하세요.");
        return Ok(());
    }

    // News API 클라이언트 생성
    let mut news_client = NewsApiClient::new(config).await?;
    news_client.authenticate().await?;

    // 1. 특정 종목 뉴스 조회
    let symbols = vec!["AAPL", "TSLA"];
    
    for symbol in symbols {
        match news_client.get_news_for_symbol(symbol, 7).await {
            Ok(articles) => {
                for (i, article) in articles.iter().enumerate().take(3) {
                    // 센티먼트 분석
                    match news_client.analyze_sentiment(article).await {
                        Ok(sentiment) => {
                        }
                        Err(e) => {
                            warn!("센티먼트 분석 실패: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("{} 뉴스 조회 실패: {}", symbol, e);
            }
        }
        
        // API 제한을 위한 대기
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    }

    // 2. 여러 종목 뉴스 집계
    match news_client.get_aggregated_news(&symbols, 3).await {
        Ok(all_news) => {
            for (symbol, articles) in all_news {
                let avg_sentiment = news_client.aggregate_sentiment(&articles);
            }
        }
        Err(e) => {
            error!("뉴스 집계 실패: {}", e);
        }
    }

    Ok(())
}

/// WebSocket 실시간 데이터 예제
async fn websocket_example() -> Result<()> {
    // WebSocket 설정 (Polygon.io 예제)
    let config = ApiConfig {
        api_key: std::env::var("POLYGON_API_KEY").unwrap_or_default(),
        api_secret: "".to_string(),
        base_url: "wss://socket.polygon.io".to_string(),
        timeout_ms: 30000,
        retry_attempts: 3,
        rate_limit: crate::api::RateLimitConfig {
            requests_per_second: 5,
            requests_per_minute: 60,
            requests_per_hour: 3600,
            requests_per_day: 86400,
        },
    };

    if config.api_key.is_empty() {
        warn!("Polygon API 키가 설정되지 않았습니다. 환경 변수 POLYGON_API_KEY를 설정하세요.");
        return Ok(());
    }

    // WebSocket 클라이언트 생성
    let mut ws_client = WebSocketClient::new(config).await?;
    ws_client.authenticate().await?;

    // 구독 설정
    ws_client.subscribe("T", vec!["AAPL".to_string(), "MSFT".to_string()]).await?;

    // WebSocket 스트림 시작
    let mut receiver = ws_client.start_stream().await?;

    // 10초간 실시간 데이터 수신
    let start_time = std::time::Instant::now();
    while start_time.elapsed() < std::time::Duration::from_secs(10) {
        if let Some(message) = receiver.recv().await {
            match message {
                crate::api::WebSocketMessage::MarketData { symbol, price, volume, timestamp } => {
                }
                crate::api::WebSocketMessage::Trade { symbol, price, quantity, side, timestamp } => {
                }
                crate::api::WebSocketMessage::Error { message, .. } => {
                    error!("WebSocket 오류: {}", message);
                }
                _ => {
                    debug!("기타 메시지: {:?}", message);
                }
            }
        }
    }

    // 구독 해제
    ws_client.unsubscribe("T").await?;
    Ok(())
}

/// Rate Limiter 예제
async fn rate_limiter_example(rate_limiter: RateLimiter) -> Result<()> {
    // API 호출 시뮬레이션
    let api_name = "test_api";
    
    // 제한 설정 추가
    let mut limiter = rate_limiter;
    limiter.add_limit(api_name.to_string(), crate::api::RateLimit {
        requests_per_second: 2,
        requests_per_minute: 10,
        requests_per_hour: 100,
        requests_per_day: 1000,
        burst_limit: 5,
    });

    // 연속 호출 테스트
    for i in 1..=5 {
        let is_allowed = limiter.is_allowed(api_name).await;
        if is_allowed {
            limiter.record_call(api_name).await;
        }
        
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }

    // 통계 조회
    if let Some(stats) = limiter.get_stats(api_name).await {
    }

    // 제한 대기 테스트
    let wait_time = limiter.wait_for_permission(api_name).await;
    Ok(())
}

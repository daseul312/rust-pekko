//! Stream Trading 드라이런 테스트
//!
//! 실제 시세를 가져와서 파이프라인 테스트
//!
//! ```
//! cargo run --example stream_trading_test
//! ```

use stock::korea_investment_api::KoreaInvestmentAPI;
use stock::stream_trading::*;
use std::time::Duration;
use tracing::{ Level};
use tracing_subscriber::FmtSubscriber;
use dotenv::dotenv;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    
    // 로깅 설정
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .compact()
        .init();
    // API 초기화
    let mut api = KoreaInvestmentAPI::from_env()?;
    api.get_access_token().await?;
    // 테스트할 종목들
    let symbols = vec![
        ("005930", "삼성전자"),
        ("000660", "SK하이닉스"),
        ("035420", "NAVER"),
    ];

    // 파이프라인 설정
    let pipeline = TradingPipeline::new(100);
    let strategy = MomentumStrategy::default();

    // 드라이런 모드로 실행
    let (price_sender, mut result_receiver) = pipeline.live_trading(strategy, api.clone()).await;
    // 각 종목 시세 조회 후 파이프라인에 전송
    for (symbol, name) in &symbols {
        match api.get_stock_price(symbol).await {
            Ok(price) => {
                let current = price.current_price.parse::<f64>().unwrap_or(0.0);
                let change = price.change_rate.parse::<f64>().unwrap_or(0.0);
                let update = PriceUpdate {
                    symbol: symbol.to_string(),
                    price,
                    timestamp: chrono::Utc::now(),
                };

                if price_sender.send(update).await.is_err() {
                    break;
                }
            }
            Err(e) => {
            }
        }
        
        // API 호출 간격
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // 채널 닫기 (파이프라인 종료 신호)
    drop(price_sender);

    // 결과 수집
    let mut signal_count = 0;
    while let Some(result) = result_receiver.recv().await {
        signal_count += 1;
        let emoji = if result.executed { "✅" } else { "🧪" };
    }

    if signal_count == 0 {
    }
    Ok(())
}

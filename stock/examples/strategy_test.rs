//! 전략 테스트
//!
//! RSI, MACD, 볼린저밴드, 복합전략 테스트
//!
//! ```
//! cargo run --example strategy_test
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
    
    let _subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .compact()
        .init();
    // API 초기화
    let mut api = KoreaInvestmentAPI::from_env()?;
    api.get_access_token().await?;
    // 테스트 종목
    let symbols = vec![
        ("005930", "삼성전자"),
        ("000660", "SK하이닉스"),
        ("035420", "NAVER"),
        ("035720", "카카오"),
        ("051910", "LG화학"),
    ];

    // 전략들
    let momentum = MomentumStrategy::default();
    let rsi = RSIStrategy::default();
    let macd = MACDStrategy::default();
    let bollinger = BollingerBandsStrategy::default();
    let combined = CombinedStrategy::default();
    for (code, name) in &symbols {
        match api.get_stock_price(code).await {
            Ok(price) => {
                let current = price.current_price.parse::<f64>().unwrap_or(0.0);
                let change = price.change_rate.parse::<f64>().unwrap_or(0.0);
                
                let change_emoji = if change > 0.0 { "📈" } else if change < 0.0 { "📉" } else { "➖" };
                // 분석용 데이터 생성
                let update = PriceUpdate {
                    symbol: code.to_string(),
                    price: price.clone(),
                    timestamp: chrono::Utc::now(),
                };
                
                // 트렌드 판단
                let trend = if change > 2.0 { Trend::StrongUp }
                    else if change > 0.5 { Trend::Up }
                    else if change > -0.5 { Trend::Neutral }
                    else if change > -2.0 { Trend::Down }
                    else { Trend::StrongDown };
                
                let analyzed = AnalyzedPrice {
                    update,
                    volatility: change.abs() / 2.0,
                    trend,
                    signal_strength: (change.abs() / 5.0).min(1.0),
                };
                
                // 각 전략별 신호
                let signals = vec![
                    ("Momentum", momentum.generate_signal(&analyzed)),
                    ("RSI", rsi.generate_signal(&analyzed)),
                    ("MACD", macd.generate_signal(&analyzed)),
                    ("Bollinger", bollinger.generate_signal(&analyzed)),
                    ("Combined", combined.generate_signal(&analyzed)),
                ];
                
                for (name, signal) in signals {
                    let (emoji, action) = match &signal {
                        TradingSignal::Buy { reason, .. } => ("🟢", format!("BUY  - {}", reason)),
                        TradingSignal::Sell { reason, .. } => ("🔴", format!("SELL - {}", reason)),
                        TradingSignal::Hold { .. } => ("⚪", "HOLD".to_string()),
                    };
                }
            }
            Err(e) => {
            }
        }
        
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(())
}

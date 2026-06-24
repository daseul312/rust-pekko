//! 뉴스 감성 + 트레이딩 통합 테스트
//!
//! Coreon News 뉴스를 분석해서 매매 신호에 반영
//!
//! ```
//! cargo run --example news_trading_test
//! ```

use stock::korea_investment_api::KoreaInvestmentAPI;
use stock::stream_trading::*;
use stock::news_analysis::{NewsReader, KeywordSentimentAnalyzer};
use std::time::Duration;
use tracing::{ warn, Level};
use tracing_subscriber::FmtSubscriber;
use dotenv::dotenv;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    
    // 로깅 설정
    let _subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .compact()
        .init();
    // ===== 1단계: 뉴스 분석 =====
    let reader = NewsReader::new();
    // 뉴스 시간 범위 (시간 단위) - 뉴스 수집 주기에 맞춰 조정
    let news_hours = std::env::var("NEWS_HOURS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(24);
    let articles = reader.load_recent_news(news_hours)?;
    
    if articles.is_empty() {
        warn!("⚠️ 최근 {}시간 뉴스 없음 (NEWS_HOURS 환경변수로 조정 가능)", news_hours);
    } else {
    }
    
    let analyzer = KeywordSentimentAnalyzer::new();
    let sentiment = analyzer.analyze_batch(&articles);
    // 주요 뉴스 출력
    if !sentiment.articles.is_empty() {
        for analyzed in sentiment.articles.iter().take(5) {
            let emoji = match analyzed.sentiment {
                stock::news_analysis::Sentiment::VeryPositive => "🟢🟢",
                stock::news_analysis::Sentiment::Positive => "🟢",
                stock::news_analysis::Sentiment::Neutral => "⚪",
                stock::news_analysis::Sentiment::Negative => "🔴",
                stock::news_analysis::Sentiment::VeryNegative => "🔴🔴",
            };
        }
    }

    // ===== 2단계: API + 전략 설정 =====
    let mut api = KoreaInvestmentAPI::from_env()?;
    api.get_access_token().await?;
    // NewsAwareStrategy 생성
    let base_strategy = MomentumStrategy::default();
    let mut news_strategy = NewsAwareStrategy::new(base_strategy);
    news_strategy.update_news_sentiment(sentiment.clone());
    // ===== 3단계: 파이프라인 실행 =====
    let pipeline = TradingPipeline::new(100);
    let (price_sender, mut result_receiver) = pipeline.live_trading(
        news_strategy, 
        api.clone()
    ).await;

    // 테스트 종목들
    let symbols = vec![
        ("005930", "삼성전자"),
        ("000660", "SK하이닉스"),
        ("035420", "NAVER"),
        ("035720", "카카오"),
        ("051910", "LG화학"),
    ];
    for (symbol, name) in &symbols {
        match api.get_stock_price(symbol).await {
            Ok(price) => {
                let current = price.current_price.parse::<f64>().unwrap_or(0.0);
                let change = price.change_rate.parse::<f64>().unwrap_or(0.0);
                let change_emoji = if change > 0.0 { "📈" } else if change < 0.0 { "📉" } else { "➖" };
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
                warn!("❌ {} 시세 조회 실패: {}", symbol, e);
            }
        }
        
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // 채널 닫기
    drop(price_sender);

    // 결과 수집
    let mut buy_count = 0;
    let mut sell_count = 0;
    let mut hold_count = 0;
    
    while let Some(result) = result_receiver.recv().await {
        let (emoji, action) = match &result.signal {
            TradingSignal::Buy { .. } => { buy_count += 1; ("🟢", "BUY") }
            TradingSignal::Sell { .. } => { sell_count += 1; ("🔴", "SELL") }
            TradingSignal::Hold { .. } => { hold_count += 1; ("⚪", "HOLD") }
        };
        
        // Hold는 출력 안 함 (너무 많음)
        if !matches!(result.signal, TradingSignal::Hold { .. }) {
        }
    }
    // 최종 추천
    let final_recommendation = sentiment.trading_recommendation();
    Ok(())
}

/// 문자열 자르기 (한글 안전)
fn truncate(s: &str, max_chars: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max_chars {
        s.to_string()
    } else {
        chars[..max_chars].iter().collect::<String>() + "..."
    }
}

/// 숫자 천 단위 쉼표 포맷
fn format_number(n: i64) -> String {
    let s = n.abs().to_string();
    let chars: Vec<char> = s.chars().collect();
    let mut result = String::new();
    
    for (i, c) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i) % 3 == 0 {
            result.push(',');
        }
        result.push(*c);
    }
    
    if n < 0 {
        format!("-{}", result)
    } else {
        result
    }
}

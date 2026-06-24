//! 실거래 테스트 (LIVE 모드)
//!
//! ⚠️ 주의: 실제 매매가 실행됩니다!
//!
//! 환경변수:
//! - LIVE_TRADING=true  : 실거래 활성화 (없으면 드라이런)
//! - MAX_AMOUNT=100000  : 최대 주문 금액 (기본: 10만원)
//! - SYMBOL=005930      : 테스트 종목 (기본: 삼성전자)
//! - USE_LLM=true       : LLM 분석 사용 (기본: false, 키워드 분석)
//!
//! ```
//! # 드라이런 (기본)
//! cargo run --example live_trading_test
//!
//! # LLM 분석 사용
//! USE_LLM=true cargo run --example live_trading_test
//!
//! # 실거래 + LLM (주의!)
//! LIVE_TRADING=true USE_LLM=true MAX_AMOUNT=50000 cargo run --example live_trading_test
//! ```

use stock::korea_investment_api::KoreaInvestmentAPI;
use stock::stream_trading::*;
use stock::news_analysis::{NewsReader, KeywordSentimentAnalyzer, BatchSentiment};
use stock::llm_analysis::LlmSentimentAnalyzer;
use std::time::Duration;
use tracing::{ warn, error, Level};
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

    // 환경변수 읽기
    let is_live = std::env::var("LIVE_TRADING")
        .map(|v| v == "true")
        .unwrap_or(false);
    
    let max_amount: u64 = std::env::var("MAX_AMOUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);
    
    let symbol = std::env::var("SYMBOL")
        .unwrap_or_else(|_| "005930".to_string());

    let use_llm = std::env::var("USE_LLM")
        .map(|v| v == "true")
        .unwrap_or(false);

    // 안전 확인
    if is_live {
        warn!("⚠️ ═══════════════════════════════════════════════════════");
        warn!("⚠️  실거래 모드 (LIVE TRADING)");
        warn!("⚠️  최대 주문 금액: {}원", format_number(max_amount as i64));
        warn!("⚠️  종목: {}", symbol);
        warn!("⚠️  분석: {}", if use_llm { "LLM (Grok)" } else { "키워드" });
        warn!("⚠️ ═══════════════════════════════════════════════════════");
        warn!("⚠️  5초 후 시작됩니다. 취소하려면 Ctrl+C");
        
        for i in (1..=5).rev() {
            warn!("⚠️  {}...", i);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        warn!("⚠️  시작!");
    } else {
    }
    // ===== 1. 뉴스 분석 =====
    let sentiment: BatchSentiment;
    
    if use_llm {
        let reader = NewsReader::new();
        let articles = reader.load_recent_news(24)?;
        
        match LlmSentimentAnalyzer::new() {
            Ok(analyzer) => {
                let llm_result = analyzer.analyze_batch(&articles, Some(&symbol)).await?;
                sentiment = llm_result.to_batch_sentiment();
            }
            Err(e) => {
                warn!("   ⚠️ LLM 분석 실패, 키워드 분석으로 대체: {}", e);
                let analyzer = KeywordSentimentAnalyzer::new();
                sentiment = analyzer.analyze_batch(&articles);
            }
        }
    } else {
        let reader = NewsReader::new();
        let news_hours = 24;
        let articles = reader.load_recent_news(news_hours)?;
        
        let analyzer = KeywordSentimentAnalyzer::new();
        sentiment = analyzer.analyze_batch(&articles);
    }
    // ===== 2. API 초기화 =====
    let mut api = KoreaInvestmentAPI::from_env()?;
    api.get_access_token().await?;
    // ===== 3. 현재 시세 조회 =====
    let price = api.get_stock_price(&symbol).await?;
    let current_price: f64 = price.current_price.parse().unwrap_or(0.0);
    let change_rate: f64 = price.change_rate.parse().unwrap_or(0.0);
    
    let change_emoji = if change_rate > 0.0 { "📈" } else if change_rate < 0.0 { "📉" } else { "➖" };
    // ===== 4. 전략 설정 및 파이프라인 실행 =====
    let base_strategy = MomentumStrategy::default();
    let mut news_strategy = NewsAwareStrategy::new(base_strategy);
    news_strategy.update_news_sentiment(sentiment.clone());
    
    let pipeline = TradingPipeline::new(100);
    
    let config = OrderConfig {
        dry_run: !is_live,
        default_quantity: 1,
        max_order_amount: max_amount,
    };
    
    let mode_str = if is_live { "LIVE" } else { "DRY-RUN" };
    let analysis_str = if use_llm { "LLM" } else { "키워드" };
    let (price_sender, mut result_receiver) = pipeline
        .live_trading_with_config(news_strategy, api.clone(), config)
        .await;

    // 가격 데이터 전송
    let update = PriceUpdate {
        symbol: symbol.clone(),
        price: price.clone(),
        timestamp: chrono::Utc::now(),
    };
    
    if price_sender.send(update).await.is_err() {
        error!("   ❌ 파이프라인 전송 실패");
        return Ok(());
    }
    
    // 채널 닫기
    drop(price_sender);

    // 결과 수신
    while let Some(result) = result_receiver.recv().await {
        let (emoji, action) = match &result.signal {
            TradingSignal::Buy { .. } => ("🟢", "BUY"),
            TradingSignal::Sell { .. } => ("🔴", "SELL"),
            TradingSignal::Hold { .. } => ("⚪", "HOLD"),
        };
        
        let status = if result.executed { "✅ 체결" } else { "⏸️ 미체결" };
        if let Some(order_id) = &result.order_id {
        }
    }
    // 잔고 확인 안내 (실거래인 경우)
    if is_live {
    }
    
    Ok(())
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

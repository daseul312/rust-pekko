//! 뉴스 기반 자동 투자 시스템 예제
//!
//! 뉴스 수집 → 감성 분석 → 투자 신호 → 거래 실행
//! 
//! 전체 흐름:
//! 1. NewsCollectorActor: 뉴스 소스에서 기사 수집
//! 2. NewsAnalyzerActor: 감성 분석 및 투자 신호 생성
//! 3. PersistentTradingAgent: 신호에 따라 거래 실행

use pekko_actor::ActorSystem;
use pekko_ml4t_system::actors::{
    // 뉴스 수집
    NewsCollectorActor, NewsCollectorConfig, NewsCollectorMessage, NewsArticle,
    // 뉴스 분석
    NewsAnalyzerActor, NewsAnalyzerMessage, AnalyzerConfig,
    // 트레이딩
    PersistentTradingAgent, PersistentTradingMessage,
};
use pekko_persistence::create_postgres_persistence;
use tokio::time::{sleep, Duration};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 로깅 설정
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║         🗞️ 뉴스 기반 자동 투자 시스템                         ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  NewsCollector → NewsAnalyzer → PersistentTradingAgent       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    // 1. Actor System 생성
    let system = ActorSystem::new("news-trading-system");
    println!("✅ ActorSystem 생성 완료");

    // 2. PostgreSQL 연결
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| {
            println!("⚠️ DATABASE_URL 미설정 - 메모리 모드로 실행");
            "".to_string()
        });

    // 3. Trading Agent 생성 (선택적 - DB 연결 시)
    let trading_agent = if !database_url.is_empty() {
        println!("📊 PostgreSQL 연결 중...");
        let (journal, snapshot_store) = create_postgres_persistence(&database_url).await?;
        
        let agent = PersistentTradingAgent::new(
            "news-trader-001".to_string(),
            journal,
            snapshot_store,
            0.001,
        ).await?;
        
        let actor_ref = system.spawn(agent, "trading-agent").await?;
        println!("✅ PersistentTradingAgent 스폰 완료");
        
        // 계좌 초기화
        actor_ref.tell(PersistentTradingMessage::InitializeAccount {
            account_id: "NEWS-TRADER-001".to_string(),
            initial_capital: 100_000.0,
        })?;
        sleep(Duration::from_millis(500)).await;
        
        Some(actor_ref)
    } else {
        None
    };

    // 4. 뉴스 수집기 설정
    println!();
    println!("📰 뉴스 수집기 초기화...");
    let collector_config = NewsCollectorConfig {
        newsapi_key: std::env::var("NEWSAPI_KEY").ok(),
        alphavantage_key: std::env::var("ALPHAVANTAGE_KEY").ok(),
        fetch_interval_secs: 300,
        max_articles_per_fetch: 20,
        symbols_to_watch: vec![
            "AAPL".to_string(),
            "NVDA".to_string(),
            "GOOGL".to_string(),
            "MSFT".to_string(),
            "TSLA".to_string(),
        ],
    };

    let news_collector = NewsCollectorActor::new(collector_config.clone());
    let collector_ref = system.spawn(news_collector, "news-collector").await?;
    println!("✅ NewsCollectorActor 스폰 완료");

    // 5. 뉴스 분석기 설정
    println!();
    println!("🔍 뉴스 분석기 초기화...");
    let analyzer_config = AnalyzerConfig {
        min_confidence: 0.4,
        signal_threshold: 0.25,
        lookback_hours: 24,
        min_articles_for_signal: 2,
    };

    let news_analyzer = NewsAnalyzerActor::new(analyzer_config);
    let analyzer_ref = system.spawn(news_analyzer, "news-analyzer").await?;
    println!("✅ NewsAnalyzerActor 스폰 완료");

    // 6. 시스템 준비 완료
    println!();
    println!("═══════════════════════════════════════════════════════════════");
    println!("🚀 시스템 준비 완료! 뉴스 기반 투자 시뮬레이션 시작...");
    println!("═══════════════════════════════════════════════════════════════");
    println!();

    // 7. 뉴스 수집 시작
    println!("📰 Step 1: 뉴스 수집");
    println!("───────────────────────────────────────────────────────────────");
    
    for symbol in &collector_config.symbols_to_watch {
        collector_ref.tell(NewsCollectorMessage::FetchNewsForSymbol {
            symbol: symbol.clone(),
        })?;
        sleep(Duration::from_millis(1000)).await;
    }
    
    // 수집 대기
    sleep(Duration::from_secs(2)).await;

    // 8. 샘플 뉴스로 분석 테스트
    println!();
    println!("🔍 Step 2: 뉴스 감성 분석");
    println!("───────────────────────────────────────────────────────────────");
    
    // 샘플 뉴스 데이터 (실제 API 없이 테스트용)
    let sample_news = vec![
        NewsArticle {
            id: "sample-1".to_string(),
            title: "NVDA stock surges to record high amid AI boom".to_string(),
            summary: "Nvidia shares rally dramatically as AI demand exceeds expectations".to_string(),
            content: "Strong earnings beat and positive outlook drive bullish sentiment".to_string(),
            url: "https://example.com/1".to_string(),
            source: pekko_ml4t_system::actors::NewsSource::Custom("Sample".to_string()),
            published_at: chrono::Utc::now(),
            symbols: vec!["NVDA".to_string()],
            categories: vec!["technology".to_string()],
        },
        NewsArticle {
            id: "sample-2".to_string(),
            title: "Apple faces concerns over iPhone sales decline".to_string(),
            summary: "Weak demand in China leads to pessimistic outlook for AAPL".to_string(),
            content: "Analysts downgrade Apple stock amid falling market share".to_string(),
            url: "https://example.com/2".to_string(),
            source: pekko_ml4t_system::actors::NewsSource::Custom("Sample".to_string()),
            published_at: chrono::Utc::now(),
            symbols: vec!["AAPL".to_string()],
            categories: vec!["technology".to_string()],
        },
        NewsArticle {
            id: "sample-3".to_string(),
            title: "Tesla announces breakthrough in battery technology".to_string(),
            summary: "Innovation could significantly reduce EV costs".to_string(),
            content: "Positive development for Tesla's competitive position".to_string(),
            url: "https://example.com/3".to_string(),
            source: pekko_ml4t_system::actors::NewsSource::Custom("Sample".to_string()),
            published_at: chrono::Utc::now(),
            symbols: vec!["TSLA".to_string()],
            categories: vec!["automotive".to_string()],
        },
        NewsArticle {
            id: "sample-4".to_string(),
            title: "Microsoft cloud revenue growth beats expectations".to_string(),
            summary: "Strong Azure performance drives outperformance".to_string(),
            content: "Bullish outlook for Microsoft as enterprise demand remains high".to_string(),
            url: "https://example.com/4".to_string(),
            source: pekko_ml4t_system::actors::NewsSource::Custom("Sample".to_string()),
            published_at: chrono::Utc::now(),
            symbols: vec!["MSFT".to_string()],
            categories: vec!["technology".to_string()],
        },
        NewsArticle {
            id: "sample-5".to_string(),
            title: "Google faces massive lawsuit over antitrust violations".to_string(),
            summary: "Scandal threatens Google's market dominance".to_string(),
            content: "Regulatory risk creates negative sentiment for GOOGL".to_string(),
            url: "https://example.com/5".to_string(),
            source: pekko_ml4t_system::actors::NewsSource::Custom("Sample".to_string()),
            published_at: chrono::Utc::now(),
            symbols: vec!["GOOGL".to_string()],
            categories: vec!["technology".to_string()],
        },
    ];

    // 분석 요청
    analyzer_ref.tell(NewsAnalyzerMessage::AnalyzeArticles {
        articles: sample_news,
    })?;
    sleep(Duration::from_secs(1)).await;

    // 9. 요약 생성
    println!();
    println!("📈 Step 3: 종목별 요약 생성");
    println!("───────────────────────────────────────────────────────────────");
    
    analyzer_ref.tell(NewsAnalyzerMessage::GenerateAllSummaries)?;
    sleep(Duration::from_secs(1)).await;

    // 10. 투자 신호 생성
    println!();
    println!("🎯 Step 4: 투자 신호 생성");
    println!("───────────────────────────────────────────────────────────────");
    
    // Trading Actor가 있으면 연결
    // (실제로는 actor_ref의 sender를 전달해야 하지만, 여기서는 시뮬레이션)
    
    analyzer_ref.tell(NewsAnalyzerMessage::GenerateSignals)?;
    sleep(Duration::from_secs(1)).await;

    // 11. 개별 종목 감성 조회
    println!();
    println!("📊 Step 5: 개별 종목 감성 조회");
    println!("───────────────────────────────────────────────────────────────");
    
    for symbol in &["NVDA", "AAPL", "TSLA", "MSFT", "GOOGL"] {
        analyzer_ref.tell(NewsAnalyzerMessage::GetSentimentFor {
            symbol: symbol.to_string(),
        })?;
        sleep(Duration::from_millis(300)).await;
    }

    // 12. Trading 상태 확인 (DB 연결 시)
    if let Some(ref trading_actor) = trading_agent {
        println!();
        println!("💰 Step 6: 포트폴리오 상태");
        println!("───────────────────────────────────────────────────────────────");
        
        trading_actor.tell(PersistentTradingMessage::GetStatus)?;
        sleep(Duration::from_secs(1)).await;
    }

    // 13. 완료
    println!();
    println!("═══════════════════════════════════════════════════════════════");
    println!("✅ 뉴스 기반 투자 시뮬레이션 완료!");
    println!();
    println!("📋 시스템 구성:");
    println!("   📰 NewsCollectorActor - 뉴스 수집 (NewsAPI, Alpha Vantage, RSS)");
    println!("   🔍 NewsAnalyzerActor - 감성 분석 및 신호 생성");
    println!("   💹 PersistentTradingAgent - Event Sourcing 기반 거래 실행");
    println!();
    println!("🔑 API 키 설정 방법:");
    println!("   export NEWSAPI_KEY=your_key_here");
    println!("   export ALPHAVANTAGE_KEY=your_key_here");
    println!("   export DATABASE_URL=postgresql://...");
    println!("═══════════════════════════════════════════════════════════════");

    // 정리
    system.stop_actor("news-collector").await?;
    system.stop_actor("news-analyzer").await?;
    if trading_agent.is_some() {
        system.stop_actor("trading-agent").await?;
    }

    Ok(())
}

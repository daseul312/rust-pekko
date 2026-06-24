//! Actor 기반 Persistent Trading Agent 예제
//!
//! pekko-actor 시스템과 통합된 Event Sourcing 트레이딩

use pekko_actor::ActorSystem;
use pekko_ml4t_system::actors::{
    PersistentTradingAgent, PersistentTradingMessage, TradingSignal,
};
use pekko_persistence::create_postgres_persistence;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 로깅 설정
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    // DATABASE_URL 환경변수에서 연결 문자열 가져오기
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL 환경변수가 설정되어야 합니다");

    println!("🚀 Actor 기반 Persistent Trading System 시작");
    println!("═══════════════════════════════════════════════");

    // 1. Actor System 생성
    let system = ActorSystem::new("trading-system");
    println!("✅ ActorSystem 생성 완료");

    // 2. Journal + SnapshotStore 생성
    println!("📊 PostgreSQL 연결 중...");
    let (journal, snapshot_store) = create_postgres_persistence(&database_url).await?;
    println!("✅ PostgreSQL 연결 완료");

    // 3. PersistentTradingAgent 생성 (자동 복구)
    let agent = PersistentTradingAgent::new(
        "actor-trading-001".to_string(),
        journal,
        snapshot_store,
        0.001, // 수수료율 0.1%
    )
    .await?;

    // 4. Actor로 스폰
    let trading_actor = system.spawn(agent, "persistent-trader").await?;
    println!("✅ PersistentTradingAgent Actor 스폰 완료");
    println!();

    // 5. 메시지 기반 거래 실행
    println!("📨 메시지 기반 거래 시작...");
    println!("───────────────────────────────────────────────");

    // 상태 확인
    trading_actor.tell(PersistentTradingMessage::GetStatus)?;
    sleep(Duration::from_millis(100)).await;

    // 계좌 초기화 (이미 있으면 무시됨)
    println!();
    println!("💰 계좌 초기화 메시지 전송...");
    trading_actor.tell(PersistentTradingMessage::InitializeAccount {
        account_id: "ACTOR-ACC-001".to_string(),
        initial_capital: 100_000.0,
    })?;
    sleep(Duration::from_millis(500)).await;

    // 매수 메시지
    println!();
    println!("🟢 매수 메시지 전송: AAPL 10주 @ $175.00");
    trading_actor.tell(PersistentTradingMessage::ExecuteBuy {
        symbol: "AAPL".to_string(),
        quantity: 10,
        price: 175.0,
    })?;
    sleep(Duration::from_millis(500)).await;

    // 가격 업데이트 메시지
    println!();
    println!("📊 가격 업데이트 메시지 전송: AAPL $185.00");
    trading_actor.tell(PersistentTradingMessage::UpdatePrice {
        symbol: "AAPL".to_string(),
        price: 185.0,
    })?;
    sleep(Duration::from_millis(500)).await;

    // 매도 메시지
    println!();
    println!("🔴 매도 메시지 전송: AAPL 5주 @ $185.00");
    trading_actor.tell(PersistentTradingMessage::ExecuteSell {
        symbol: "AAPL".to_string(),
        quantity: 5,
        price: 185.0,
    })?;
    sleep(Duration::from_millis(500)).await;

    // 신호 기반 거래 메시지
    println!();
    println!("🤖 신호 처리 메시지 전송: NVDA Buy(0.85)");
    trading_actor.tell(PersistentTradingMessage::HandleSignal {
        symbol: "NVDA".to_string(),
        signal: TradingSignal::Buy { strength: 0.85 },
        current_price: 480.0,
    })?;
    sleep(Duration::from_millis(500)).await;

    // 최종 상태 확인
    println!();
    println!("═══════════════════════════════════════════════");
    println!("📈 최종 상태 조회...");
    trading_actor.tell(PersistentTradingMessage::GetStatus)?;
    sleep(Duration::from_millis(500)).await;

    // 스냅샷 저장
    println!();
    println!("📸 스냅샷 저장 메시지 전송...");
    trading_actor.tell(PersistentTradingMessage::SaveSnapshot)?;
    sleep(Duration::from_millis(500)).await;

    println!();
    println!("═══════════════════════════════════════════════");
    println!("✅ 모든 거래가 PostgreSQL에 저장됨!");
    println!("💡 프로그램 재시작 시 Actor가 자동으로 복구됩니다.");
    println!();
    println!("🎯 Actor 패턴의 장점:");
    println!("   - 메시지 기반 비동기 처리");
    println!("   - 다른 Actor와 쉽게 통신");
    println!("   - Supervision으로 실패 복구");
    println!("   - Event Sourcing으로 완전한 이력 추적");

    // Actor 정지
    system.stop_actor("persistent-trader").await?;
    println!();
    println!("🛑 Actor 정지 완료");

    Ok(())
}

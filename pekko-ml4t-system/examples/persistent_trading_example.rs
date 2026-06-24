//! Persistent Trading Agent 예제
//!
//! PostgreSQL(Neon)을 사용한 Event Sourcing 트레이딩 시스템

use pekko_ml4t_system::actors::{PersistentTradingAgent, TradingSignal};
use pekko_persistence::{
    create_postgres_persistence, PostgresJournalConfig, PostgresSnapshotConfig,
};
use std::sync::Arc;
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

    println!("🚀 Persistent Trading Agent 시작");
    println!("📊 PostgreSQL 연결 중...");

    // Journal + SnapshotStore 생성 (커넥션 풀 공유)
    let (journal, snapshot_store) = create_postgres_persistence(&database_url).await?;

    // Trading Agent 생성 (자동 복구)
    let mut agent = PersistentTradingAgent::new(
        "trading-agent-001".to_string(),
        journal,
        snapshot_store,
        0.001, // 수수료율 0.1%
    )
    .await?;

    println!("\n📈 현재 상태:");
    println!("  잔고: ${:.2}", agent.cash());
    println!("  포지션: {:?}", agent.positions().keys().collect::<Vec<_>>());
    println!("  총 가치: ${:.2}", agent.total_value());

    // 계좌가 초기화되지 않았으면 초기화
    if agent.cash() == 0.0 {
        println!("\n💰 계좌 초기화 (초기 자본: $100,000)");
        agent
            .initialize_account("ACC-001".to_string(), 100_000.0)
            .await?;
    }

    // 매수 테스트
    println!("\n🟢 AAPL 10주 매수 @ $175.00");
    let order_id = agent.execute_buy("AAPL".to_string(), 10, 175.0).await?;
    println!("  주문 ID: {}", order_id);

    // 상태 확인
    println!("\n📈 매수 후 상태:");
    println!("  잔고: ${:.2}", agent.cash());
    println!("  AAPL 포지션: {:?}", agent.positions().get("AAPL"));

    // 가격 업데이트 (시가평가)
    println!("\n📊 가격 업데이트: AAPL $180.00");
    agent.update_price("AAPL".to_string(), 180.0).await?;

    if let Some(pos) = agent.positions().get("AAPL") {
        println!("  미실현 손익: ${:.2}", pos.unrealized_pnl);
    }

    // 매도 테스트
    println!("\n🔴 AAPL 5주 매도 @ $180.00");
    let order_id = agent.execute_sell("AAPL".to_string(), 5, 180.0).await?;
    println!("  주문 ID: {}", order_id);

    // 최종 상태
    println!("\n📈 최종 상태:");
    println!("  잔고: ${:.2}", agent.cash());
    println!("  총 가치: ${:.2}", agent.total_value());
    println!("  실현 손익: ${:.2}", agent.total_realized_pnl());

    // 신호 기반 거래 테스트
    println!("\n🤖 신호 기반 거래 테스트:");
    let signal = TradingSignal::Buy { strength: 0.85 };
    if let Some(order_id) = agent
        .handle_signal("NVDA".to_string(), signal, 450.0)
        .await?
    {
        println!("  NVDA 매수 주문: {}", order_id);
    }

    println!("\n✅ 모든 거래가 PostgreSQL에 저장됨!");
    println!("💡 프로그램 재시작 시 자동으로 복구됩니다.");

    Ok(())
}

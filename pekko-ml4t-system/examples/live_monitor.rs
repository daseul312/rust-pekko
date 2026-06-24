//! Actor 시스템 실시간 모니터링 서버
//!
//! 실행 후 브라우저에서 actor-graph-live.html 열기
//! 
//! ```bash
//! cargo run --example live_monitor
//! ```

use pekko_ml4t_system::monitor::{ActorMonitor, MonitorConfig, EventSimulator};
use tokio::time::{sleep, Duration};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 로깅 설정
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║         🖥️ Pekko-Rust Actor Live Monitor                     ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  WebSocket 서버가 실행됩니다.                                 ║");
    println!("║  브라우저에서 actor-graph-live.html을 열어주세요.             ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    // 모니터 설정
    let config = MonitorConfig {
        address: "127.0.0.1".to_string(),
        port: 9090,
        buffer_size: 1000,
    };

    // 모니터 시작
    let monitor = ActorMonitor::new(config);
    monitor.start().await?;

    println!("✅ WebSocket 서버 실행 중: ws://127.0.0.1:9090/actor-events");
    println!();
    println!("📂 브라우저에서 열기:");
    println!("   file:///Volumes/T7/Work/Rust_Pekko/apache-pekko-1.1.3-src-20250104/pekko-rust/pekko-ml4t-system/docs/actor-graph-live.html");
    println!();
    println!("🎮 또는 HTML에서 'Start Simulation' 버튼 클릭");
    println!("🔌 또는 'Connect WS' 버튼으로 이 서버에 연결");
    println!();
    println!("─────────────────────────────────────────────────────────────────");
    println!("📡 시뮬레이션 이벤트 전송 시작...");
    println!();

    // 시뮬레이터 시작
    let sender = monitor.get_sender();
    let simulator = EventSimulator::new(sender.clone());

    // 별도 태스크에서 시뮬레이션 실행
    tokio::spawn(async move {
        simulator.run_simulation().await;
    });

    // 메인 루프 (상태 출력)
    loop {
        let client_count = monitor.client_count().await;
        if client_count > 0 {
            println!("👥 연결된 클라이언트: {}", client_count);
        }
        sleep(Duration::from_secs(10)).await;
    }
}

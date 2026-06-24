use dotenv::dotenv;
use anyhow::Result;
use chrono::Local;

mod daily_report;
mod notion_reporter;

use daily_report::{DailyTradingReport, TradeRecord, PositionSummary};
use notion_reporter::NotionReporter;

#[tokio::main]
async fn main() -> Result<()> {
    // 환경 변수 로드
    dotenv().ok();
    
    // 로깅 초기화
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("\n🧪 노션 리포트 테스트");
    println!("{}", "=".repeat(50));
    
    // 테스트 리포트 생성
    let today = Local::now().format("%Y-%m-%d").to_string();
    let mut report = DailyTradingReport::new(today.clone());
    
    // 가상의 거래 내역 추가
    report.trades.push(TradeRecord {
        time: "09:30:00".to_string(),
        action: "매수".to_string(),
        symbol: "005930".to_string(),
        name: "삼성전자".to_string(),
        quantity: 10,
        price: 70000.0,
        total_amount: 700000.0,
        profit_loss: None,
    });
    
    report.trades.push(TradeRecord {
        time: "14:30:00".to_string(),
        action: "매도".to_string(),
        symbol: "005930".to_string(),
        name: "삼성전자".to_string(),
        quantity: 10,
        price: 72000.0,
        total_amount: 720000.0,
        profit_loss: Some(20000.0),
    });
    
    // 현재 보유 종목
    report.positions.push(PositionSummary {
        symbol: "035420".to_string(),
        name: "NAVER".to_string(),
        quantity: 5,
        avg_price: 200000.0,
        current_price: 210000.0,
        profit_loss_rate: 5.0,
        total_value: 1050000.0,
    });
    
    // 요약 정보
    report.summary.total_trades = 2;
    report.summary.buy_count = 1;
    report.summary.sell_count = 1;
    report.summary.total_profit_loss = 20000.0;
    report.summary.total_profit_loss_rate = 2.86;
    report.summary.win_rate = 100.0;
    
    // 수익 현황
    report.performance.starting_balance = 1000000.0;
    report.performance.ending_balance = 1070000.0;
    report.performance.daily_return = 70000.0;
    report.performance.daily_return_rate = 7.0;
    report.performance.max_drawdown = -1.5;
    
    // 마크다운 출력
    println!("\n📄 생성된 리포트 내용:\n");
    println!("{}", report.to_markdown());
    
    // 노션에 업로드
    println!("\n🚀 노션에 업로드 중...\n");
    
    match NotionReporter::from_env() {
        Ok(reporter) => {
            match reporter.create_daily_report(&report).await {
                Ok(url) => {
                    println!("✅ 노션 리포트 생성 성공!");
                    println!("🔗 URL: {}", url);
                    println!("\n💡 노션 페이지를 열어서 확인해보세요!");
                }
                Err(e) => {
                    println!("❌ 노션 리포트 생성 실패: {}", e);
                    println!("\n💡 확인사항:");
                    println!("   1. NOTION_TOKEN이 올바른지 확인");
                    println!("   2. NOTION_DATABASE_ID가 올바른지 확인");
                    println!("   3. 노션 데이터베이스에 Integration이 연결되어 있는지 확인");
                    println!("   4. 데이터베이스 속성 이름이 정확한지 확인:");
                    println!("      - 제목 (Title)");
                    println!("      - 날짜 (Date)");
                    println!("      - 수익률 (Number)");
                    println!("      - 거래횟수 (Number)");
                    println!("      - 승률 (Number)");
                    println!("      - 총손익 (Number)");
                }
            }
        }
        Err(e) => {
            println!("❌ 환경 변수 로드 실패: {}", e);
            println!("\n💡 .env 파일에 다음 항목들이 설정되어 있는지 확인하세요:");
            println!("   NOTION_TOKEN=your_token_here");
            println!("   NOTION_DATABASE_ID=your_database_id_here");
        }
    }
    
    Ok(())
}


mod korea_investment_api;

use korea_investment_api::KoreaInvestmentAPI;
use tracing_subscriber;

#[tokio::main]
async fn main() {
    // 로깅 초기화
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("\n📊 한국투자증권 주식 시세 조회 테스트\n");

    // API 클라이언트 생성
    let mut api = match KoreaInvestmentAPI::from_env() {
        Ok(api) => api,
        Err(e) => {
            eprintln!("❌ API 클라이언트 생성 실패: {}", e);
            std::process::exit(1);
        }
    };

    // 토큰 발급
    println!("🔐 토큰 발급 중...");
    match api.get_access_token().await {
        Ok(_) => println!("✅ 토큰 발급 성공!\n"),
        Err(e) => {
            eprintln!("❌ 토큰 발급 실패: {}", e);
            std::process::exit(1);
        }
    }

    // 주요 종목 시세 조회
    let stocks = vec![
        ("005930", "삼성전자"),
        ("000660", "SK하이닉스"),
        ("035720", "카카오"),
        ("035420", "NAVER"),
        ("051910", "LG화학"),
        ("006400", "삼성SDI"),
    ];

    println!("========================================");
    println!("📈 주식 시세 조회");
    println!("========================================\n");

    for (code, name) in stocks {
        match api.get_stock_price(code).await {
            Ok(stock_price) => {
                let current_price: f64 = stock_price.current_price.parse().unwrap_or(0.0);
                let change_rate: f64 = stock_price.change_rate.parse().unwrap_or(0.0);
                let volume: u64 = stock_price.volume.parse().unwrap_or(0);
                
                println!("📊 {} ({})", name, code);
                println!("   현재가: {:.0}원", current_price);
                println!("   등락률: {:.2}%", change_rate);
                println!("   거래량: {}주", volume);
                println!("   시가: {}원", stock_price.open_price);
                println!("   고가: {}원", stock_price.high_price);
                println!("   저가: {}원", stock_price.low_price);
                println!();
                
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            }
            Err(e) => {
                eprintln!("❌ {} ({}) 시세 조회 실패: {}", name, code, e);
            }
        }
    }

    println!("========================================");
    println!("✅ 시세 조회 테스트 완료!");
    println!("========================================");
}


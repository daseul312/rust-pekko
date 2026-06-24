use stock::korea_investment_api::KoreaInvestmentAPI;
use tracing_subscriber;

#[tokio::main]
async fn main() {
    // .env 파일 로드
    dotenv::dotenv().ok();
    
    // 로깅 초기화
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("\n📊 한국투자증권 계좌 정보 상세 조회\n");

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

    println!("========================================");
    println!("💰 계좌 정보 조회");
    println!("========================================\n");

    // 계좌 정보 조회
    match api.get_account_info().await {
        Ok((cash, holdings)) => {
            println!("✅ 계좌 조회 성공!\n");
            
            println!("💵 예수금: {:.0}원\n", cash);
            
            if holdings.is_empty() {
                println!("📊 보유 종목: 없음\n");
            } else {
                println!("📊 보유 종목: {}개\n", holdings.len());
                
                let mut total_value = 0.0;
                
                for (idx, (code, qty, avg_price)) in holdings.iter().enumerate() {
                    let value = *avg_price * (*qty as f64);
                    total_value += value;
                    
                    println!("{}. 종목 코드: {}", idx + 1, code);
                    println!("   보유 수량: {}주", qty);
                    println!("   평균 단가: {:.0}원", avg_price);
                    println!("   평가 금액: {:.0}원", value);
                    println!();
                }
                
                println!("📊 보유 주식 총 가치: {:.0}원", total_value);
                println!("💎 총 자산: {:.0}원\n", cash + total_value);
            }
        }
        Err(e) => {
            eprintln!("❌ 계좌 조회 실패: {}", e);
            std::process::exit(1);
        }
    }

    println!("========================================");
    println!("✅ 계좌 정보 조회 완료!");
    println!("========================================");
}


mod korea_investment_api;

use korea_investment_api::KoreaInvestmentAPI;
use tracing_subscriber;

#[tokio::main]
async fn main() {
    // 로깅 초기화
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("\n🚀 한국투자증권 API 연결 테스트 프로그램\n");

    // 환경 변수 확인
    println!("📋 환경 변수 확인 중...");
    
    let required_vars = vec!["KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACCOUNT_NO"];
    let mut missing_vars = Vec::new();

    for var in &required_vars {
        match std::env::var(var) {
            Ok(value) => {
                if var.contains("KEY") || var.contains("SECRET") {
                    println!("  ✅ {}: {}...", var, &value[..std::cmp::min(10, value.len())]);
                } else {
                    println!("  ✅ {}: {}", var, value);
                }
            }
            Err(_) => {
                println!("  ❌ {}: 설정되지 않음", var);
                missing_vars.push(*var);
            }
        }
    }

    if !missing_vars.is_empty() {
        eprintln!("\n❌ 오류: 다음 환경 변수가 설정되지 않았습니다:");
        for var in missing_vars {
            eprintln!("  - {}", var);
        }
        eprintln!("\n💡 .env 파일을 생성하고 다음 정보를 입력하세요:");
        eprintln!("  KIS_APP_KEY=발급받은_APP_KEY");
        eprintln!("  KIS_APP_SECRET=발급받은_APP_SECRET");
        eprintln!("  KIS_ACCOUNT_NO=계좌번호-상품코드 (예: 12345678-01)");
        eprintln!("  KIS_BASE_URL=https://openapi.koreainvestment.com:9443");
        eprintln!("\n또는 환경 변수로 설정하세요:");
        eprintln!("  export KIS_APP_KEY=...");
        eprintln!("  export KIS_APP_SECRET=...");
        eprintln!("  export KIS_ACCOUNT_NO=...");
        std::process::exit(1);
    }

    println!("\n");

    // API 클라이언트 생성
    let mut api = match KoreaInvestmentAPI::from_env() {
        Ok(api) => api,
        Err(e) => {
            eprintln!("❌ API 클라이언트 생성 실패: {}", e);
            std::process::exit(1);
        }
    };

    // 연결 테스트
    match api.test_connection().await {
        Ok(_) => {
            println!("\n✅ 모든 테스트 통과!");
            println!("🎉 한국투자증권 API 연결이 정상적으로 작동합니다.");
            println!("\n💡 이제 실제 자동매매 프로그램을 실행할 수 있습니다.");
        }
        Err(e) => {
            eprintln!("\n❌ 테스트 실패: {}", e);
            eprintln!("\n💡 문제 해결 방법:");
            eprintln!("  1. APP_KEY와 APP_SECRET이 올바른지 확인하세요");
            eprintln!("  2. 계좌번호 형식이 올바른지 확인하세요 (예: 12345678-01)");
            eprintln!("  3. 모의투자 계좌인 경우:");
            eprintln!("     KIS_BASE_URL=https://openapivts.koreainvestment.com:29443");
            eprintln!("  4. 실전투자 계좌인 경우:");
            eprintln!("     KIS_BASE_URL=https://openapi.koreainvestment.com:9443");
            eprintln!("  5. 네트워크 연결을 확인하세요");
            std::process::exit(1);
        }
    }
}


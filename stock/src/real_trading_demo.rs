use stock_recommender::{
    RealTradingActor, TradingSignal
};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 한국투자증권 실매매 연동 데모 ===\n");

    // 환경변수에서 API 키 읽기
    let app_key = env::var("KIWOOM_APP_KEY")
        .unwrap_or_else(|_| "your_app_key_here".to_string());
    let app_secret = env::var("KIWOOM_APP_SECRET")
        .unwrap_or_else(|_| "your_app_secret_here".to_string());
    let account_number = env::var("KIWOOM_ACCOUNT_NUMBER")
        .ok();
    
    // 모의투자 여부 확인 (기본값: true)
    let is_simulation = env::var("KIWOOM_SIMULATION")
        .unwrap_or_else(|_| "true".to_string())
        .parse::<bool>()
        .unwrap_or(true);

    if app_key == "your_app_key_here" || app_secret == "your_app_secret_here" {
        println!("⚠️  한국투자증권 API 키가 설정되지 않았습니다.");
        println!("환경변수를 설정하거나 데모 모드로 실행합니다.\n");
        run_demo_mode().await;
        return Ok(());
    }

    println!("🔧 설정 정보:");
    println!("   모의투자: {}", if is_simulation { "예" } else { "아니오 (실거래)" });
    println!("   계좌번호: {}", account_number.as_deref().unwrap_or("미설정"));
    println!();

    // 실매매 액터 생성 (직접 사용)
    let mut real_trading_actor = RealTradingActor::new(
        app_key, 
        app_secret, 
        account_number, 
        is_simulation
    );

    // 1. 실매매 시스템 활성화
    println!("--- 실매매 시스템 활성화 ---");
    match real_trading_actor.activate().await {
        Ok(_) => println!("✅ 실매매 시스템 활성화 성공"),
        Err(e) => println!("❌ 실매매 시스템 활성화 실패: {}", e),
    }

    // 2. 계좌 정보 동기화
    println!("\n--- 계좌 정보 동기화 ---");
    match real_trading_actor.sync_account_info().await {
        Ok(_) => println!("✅ 계좌 정보 동기화 성공"),
        Err(e) => println!("❌ 계좌 정보 동기화 실패: {}", e),
    }

    // 3. 실매매 상태 확인
    println!("\n--- 실매매 상태 확인 ---");
    let status = real_trading_actor.get_trading_status();
    println!("   활성화: {}", status.is_active);
    println!("   모의투자: {}", status.is_simulation);
    println!("   토큰유효: {}", status.is_token_valid);
    println!("   계정수: {}", status.account_count);

    // 4. 모의 매매 신호 생성 및 실행
    println!("\n--- 모의 매매 신호 실행 ---");
    
    // 매수 신호
    let buy_signal = TradingSignal::Buy {
        symbol: "005930".to_string(),  // 삼성전자
        stock_name: "삼성전자".to_string(),
        quantity: 1,  // 안전을 위해 1주만
        price: 75000.0,
        reason: "실매매 테스트".to_string(),
        confidence: 0.8,
    };
    
    match real_trading_actor.execute_real_order(buy_signal).await {
        Ok(result) => {
            if result.success {
                println!("✅ 매수 주문 성공: {}", result.message);
                println!("   주문번호: {:?}", result.order_number);
            } else {
                println!("❌ 매수 주문 실패: {}", result.message);
            }
        }
        Err(e) => println!("❌ 매수 주문 오류: {}", e),
    }

    // 매도 신호
    let sell_signal = TradingSignal::Sell {
        symbol: "005930".to_string(),
        stock_name: "삼성전자".to_string(),
        quantity: 1,
        price: 76000.0,
        reason: "실매매 테스트".to_string(),
        confidence: 0.8,
    };
    
    match real_trading_actor.execute_real_order(sell_signal).await {
        Ok(result) => {
            if result.success {
                println!("✅ 매도 주문 성공: {}", result.message);
                println!("   주문번호: {:?}", result.order_number);
            } else {
                println!("❌ 매도 주문 실패: {}", result.message);
            }
        }
        Err(e) => println!("❌ 매도 주문 오류: {}", e),
    }

    // 5. 최종 상태 확인
    println!("\n--- 최종 상태 확인 ---");
    let final_status = real_trading_actor.get_trading_status();
    println!("   활성화: {}", final_status.is_active);
    println!("   모의투자: {}", final_status.is_simulation);
    println!("   토큰유효: {}", final_status.is_token_valid);

    println!("\n=== 실매매 연동 데모 완료 ===");
    println!("⚠️  주의사항:");
    println!("1. 실제 거래 시에는 충분한 테스트를 거쳐야 합니다");
    println!("2. 모의투자로 먼저 검증하세요");
    println!("3. 자동매매는 금융 규제를 준수해야 합니다");
    println!("4. 시스템 오류로 인한 손실에 주의하세요");
    println!("5. 정기적인 모니터링이 필요합니다");

    Ok(())
}

/// 데모 모드 실행 (API 키 없이 시뮬레이션)
async fn run_demo_mode() {
    println!("=== 실매매 연동 데모 모드 ===\n");

    println!("📋 실매매 연동을 위한 필요 사항:");
    println!();
    println!("1. 🔑 한국투자증권 API 키 발급:");
    println!("   - 한국투자증권 개발자센터 방문");
    println!("   - OpenAPI 신청 및 승인");
    println!("   - App Key, App Secret 발급");
    println!();
    println!("2. 🏦 계좌 정보:");
    println!("   - 한국투자증권 계좌 개설");
    println!("   - 계좌번호 확인");
    println!("   - API 권한 설정");
    println!();
    println!("3. 🔧 환경변수 설정:");
    println!("   export KIWOOM_APP_KEY='your_app_key'");
    println!("   export KIWOOM_APP_SECRET='your_app_secret'");
    println!("   export KIWOOM_ACCOUNT_NUMBER='your_account_number'");
    println!("   export KIWOOM_SIMULATION='true'  # 모의투자 여부");
    println!();
    println!("4. ⚖️  법적 고려사항:");
    println!("   - 자동매매 관련 금융 규제 준수");
    println!("   - 시장 조작 방지");
    println!("   - 투자자 보호 규정 준수");
    println!("   - 정기적인 시스템 검증");
    println!();
    println!("5. 🛡️  안전 장치:");
    println!("   - 일일 거래 한도 설정");
    println!("   - 손실 한도 설정");
    println!("   - 긴급 정지 기능");
    println!("   - 로그 및 모니터링");
    println!();
    println!("6. 📊 모의투자 권장:");
    println!("   - 실제 거래 전 충분한 테스트");
    println!("   - 전략 검증 및 최적화");
    println!("   - 시스템 안정성 확인");
    println!();
    println!("💡 실매매 연동 시 주의사항:");
    println!("- 시스템 오류로 인한 손실 가능성");
    println!("- 네트워크 장애 시 거래 중단");
    println!("- API 제한 및 오류 처리 필요");
    println!("- 정기적인 시스템 점검 필수");
    println!("- 백업 시스템 구축 권장");
    println!();
    println!("🔗 한국투자증권 개발자센터:");
    println!("   https://apiportal.koreainvestment.com/");
    println!();
    println!("📞 문의사항:");
    println!("   - 한국투자증권 고객센터: 1544-5000");
    println!("   - 개발자 지원: dev@koreainvestment.com");
}
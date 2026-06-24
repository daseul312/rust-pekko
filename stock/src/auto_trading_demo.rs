use stock_recommender::{
    AutoTradingEngine, TradingStrategy, UserAccount, TransactionType
};
use std::collections::HashMap;
use chrono::Utc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 자동 매매 시스템 데모 (단순화 버전) ===\n");

    // 자동 매매 엔진 생성
    let mut trading_engine = AutoTradingEngine::new();

    // 1. 사용자 계정 등록
    println!("--- 사용자 계정 등록 ---");
    let users = vec![
        ("user1", "김투자", 10_000_000.0),  // 1천만원
        ("user2", "이매매", 50_000_000.0),  // 5천만원
        ("user3", "박자동", 100_000_000.0), // 1억원
    ];

    for (user_id, username, initial_cash) in users {
        trading_engine.register_user(user_id.to_string(), username.to_string(), initial_cash);
        println!("사용자 등록: {} ({}) - 초기 자금 {:.0}원", username, user_id, initial_cash);
    }

    // 2. 매매 전략 설정
    println!("\n--- 매매 전략 설정 ---");
    let strategies = vec![
        ("user1", TradingStrategy {
            user_id: "user1".to_string(),
            max_position_ratio: 0.2,      // 최대 20% 포지션
            profit_target: 15.0,          // 15% 수익 목표
            stop_loss: -10.0,             // 10% 손절매
            rebalance_threshold: 0.05,    // 5% 리밸런싱 임계값
            max_stocks: 5,                // 최대 5개 종목
            min_cash_ratio: 0.2,          // 최소 20% 현금 보유
            auto_trading: true,
            created_at: Utc::now(),
        }),
        ("user2", TradingStrategy {
            user_id: "user2".to_string(),
            max_position_ratio: 0.3,      // 최대 30% 포지션
            profit_target: 20.0,          // 20% 수익 목표
            stop_loss: -15.0,             // 15% 손절매
            rebalance_threshold: 0.1,     // 10% 리밸런싱 임계값
            max_stocks: 8,                // 최대 8개 종목
            min_cash_ratio: 0.15,         // 최소 15% 현금 보유
            auto_trading: true,
            created_at: Utc::now(),
        }),
        ("user3", TradingStrategy {
            user_id: "user3".to_string(),
            max_position_ratio: 0.25,     // 최대 25% 포지션
            profit_target: 12.0,          // 12% 수익 목표
            stop_loss: -8.0,              // 8% 손절매
            rebalance_threshold: 0.08,    // 8% 리밸런싱 임계값
            max_stocks: 6,                // 최대 6개 종목
            min_cash_ratio: 0.25,         // 최소 25% 현금 보유
            auto_trading: true,
            created_at: Utc::now(),
        }),
    ];

    for (user_id, strategy) in strategies {
        trading_engine.set_trading_strategy(user_id.to_string(), strategy);
        println!("매매 전략 설정: {}", user_id);
    }

    // 3. 초기 주식 가격 설정
    println!("\n--- 초기 주식 가격 설정 ---");
    let initial_prices = HashMap::from([
        ("005930".to_string(), 75000.0),  // 삼성전자
        ("105560".to_string(), 55000.0),  // KB금융
        ("055550".to_string(), 42000.0),  // 신한지주
        ("017670".to_string(), 48000.0),  // SK텔레콤
        ("003550".to_string(), 95000.0),  // LG
    ]);

    trading_engine.update_stock_prices(initial_prices);
    println!("초기 주식 가격 업데이트 완료");

    // 4. 자동 매매 실행 (라운드 1)
    println!("\n--- 자동 매매 실행 (라운드 1) ---");
    for user_id in ["user1", "user2", "user3"] {
        let signals = trading_engine.generate_trading_signals(user_id);
        println!("사용자 {}: {}개의 매매 신호 생성됨", user_id, signals.len());
        
        for signal in signals {
            match trading_engine.execute_trading_signal(user_id, signal) {
                Ok(result) => println!("  ✓ {}", result),
                Err(e) => println!("  ✗ {}", e),
            }
        }
    }

    // 5. 계정 상태 확인
    println!("\n--- 계정 상태 확인 ---");
    for user_id in ["user1", "user2", "user3"] {
        if let Some(account) = trading_engine.get_user_account(user_id) {
            println!("📊 {} ({})", account.username, user_id);
            println!("   총 자산: {:.0}원", account.total_value);
            println!("   현금: {:.0}원 ({:.1}%)", account.total_cash, account.get_cash_ratio() * 100.0);
            println!("   투자금액: {:.0}원", account.invested_amount);
            println!("   평가손익: {:.0}원 ({:+.1}%)", account.profit_loss, account.profit_loss_rate);
            println!("   보유 종목: {}개", account.portfolio.len());
            
            for (symbol, position) in &account.portfolio {
                println!("     - {}: {}주 @ {:.0}원 (손익: {:+.1}%)", 
                    symbol, position.quantity, position.current_price, position.profit_loss_rate);
            }
            println!();
        }
    }

    // 6. 가격 변동 시뮬레이션 (상승)
    println!("\n--- 가격 상승 시뮬레이션 ---");
    let rising_prices = HashMap::from([
        ("005930".to_string(), 80000.0),  // 삼성전자 +6.7%
        ("105560".to_string(), 60000.0),  // KB금융 +9.1%
        ("055550".to_string(), 45000.0),  // 신한지주 +7.1%
        ("017670".to_string(), 52000.0),  // SK텔레콤 +8.3%
        ("003550".to_string(), 100000.0), // LG +5.3%
    ]);

    trading_engine.update_stock_prices(rising_prices);
    println!("가격 상승 업데이트 완료");

    // 7. 수익 실현 매매 실행
    println!("\n--- 수익 실현 매매 실행 ---");
    for user_id in ["user1", "user2", "user3"] {
        let signals = trading_engine.generate_trading_signals(user_id);
        println!("사용자 {}: {}개의 매매 신호 생성됨", user_id, signals.len());
        
        for signal in signals {
            match trading_engine.execute_trading_signal(user_id, signal) {
                Ok(result) => println!("  ✓ {}", result),
                Err(e) => println!("  ✗ {}", e),
            }
        }
    }

    // 8. 가격 하락 시뮬레이션
    println!("\n--- 가격 하락 시뮬레이션 ---");
    let falling_prices = HashMap::from([
        ("005930".to_string(), 70000.0),  // 삼성전자 -12.5%
        ("105560".to_string(), 50000.0),  // KB금융 -16.7%
        ("055550".to_string(), 38000.0),  // 신한지주 -15.6%
        ("017670".to_string(), 45000.0),  // SK텔레콤 -13.5%
        ("003550".to_string(), 85000.0), // LG -15.0%
    ]);

    trading_engine.update_stock_prices(falling_prices);
    println!("가격 하락 업데이트 완료");

    // 9. 손절매 실행
    println!("\n--- 손절매 실행 ---");
    for user_id in ["user1", "user2", "user3"] {
        let signals = trading_engine.generate_trading_signals(user_id);
        println!("사용자 {}: {}개의 매매 신호 생성됨", user_id, signals.len());
        
        for signal in signals {
            match trading_engine.execute_trading_signal(user_id, signal) {
                Ok(result) => println!("  ✓ {}", result),
                Err(e) => println!("  ✗ {}", e),
            }
        }
    }

    // 10. 최종 계정 상태 확인
    println!("\n--- 최종 계정 상태 확인 ---");
    for user_id in ["user1", "user2", "user3"] {
        if let Some(account) = trading_engine.get_user_account(user_id) {
            println!("📊 {} ({}) - 최종 결과", account.username, user_id);
            println!("   총 자산: {:.0}원", account.total_value);
            println!("   현금: {:.0}원 ({:.1}%)", account.total_cash, account.get_cash_ratio() * 100.0);
            println!("   투자금액: {:.0}원", account.invested_amount);
            println!("   평가손익: {:.0}원 ({:+.1}%)", account.profit_loss, account.profit_loss_rate);
            println!("   보유 종목: {}개", account.portfolio.len());
            println!("   거래 내역: {}건", account.transaction_history.len());
            
            // 거래 내역 요약
            let buy_count = account.transaction_history.iter()
                .filter(|t| t.transaction_type == TransactionType::Buy).count();
            let sell_count = account.transaction_history.iter()
                .filter(|t| t.transaction_type == TransactionType::Sell).count();
            
            println!("     매수: {}건, 매도: {}건", buy_count, sell_count);
            println!();
        }
    }

    println!("=== 자동 매매 시스템 데모 완료 ===");
    println!("이 시스템은 다음과 같은 기능을 제공합니다:");
    println!("1. 사용자 계정 관리 및 포트폴리오 추적");
    println!("2. 목표 수익률 달성 시 자동 매도");
    println!("3. 손절매 설정에 따른 자동 매도");
    println!("4. 현금 비율 기반 자동 매수");
    println!("5. 포트폴리오 리밸런싱");
    println!("6. 실시간 손익 계산 및 관리");
    println!("7. 거래 내역 추적 및 분석");

    Ok(())
}
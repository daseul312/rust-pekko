use stock_recommender::{Stock, RiskLevel};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    println!("=== 저위험 주식투자 종목 추천 시스템 (데모) ===\n");

    // 샘플 주식 데이터 생성
    let mut stocks = create_sample_stocks();
    
    // 저위험 주식 필터링
    let low_risk_stocks: Vec<&Stock> = stocks.values()
        .filter(|stock| stock.is_low_risk())
        .collect();
    
    println!("--- 저위험 주식 목록 ---");
    for stock in &low_risk_stocks {
        println!("📈 {} ({})", stock.name, stock.symbol);
        println!("   섹터: {}", stock.sector);
        println!("   현재가: ${:.2}", stock.current_price);
        println!("   PER: {:.1}", stock.pe_ratio.unwrap_or(0.0));
        println!("   배당수익률: {:.1}%", stock.dividend_yield.unwrap_or(0.0));
        println!("   베타: {:.2}", stock.beta.unwrap_or(0.0));
        println!("   위험도: {:?}", stock.get_risk_level());
        println!("   추천점수: {:.1}/100", stock.recommendation_score);
        println!("   시가총액: ${:.0}B", stock.market_cap / 1_000_000_000.0);
        println!();
    }

    // 섹터별 분석
    println!("--- 섹터별 분석 ---");
    let mut sector_analysis: HashMap<String, Vec<&Stock>> = HashMap::new();
    for stock in &low_risk_stocks {
        sector_analysis.entry(stock.sector.clone()).or_insert_with(Vec::new).push(stock);
    }
    
    for (sector, sector_stocks) in sector_analysis {
        println!("🏢 {} 섹터: {}개 종목", sector, sector_stocks.len());
        for stock in sector_stocks {
            println!("   - {} (추천점수: {:.1})", stock.name, stock.recommendation_score);
        }
        println!();
    }

    // 포트폴리오 추천
    println!("--- 포트폴리오 추천 ---");
    recommend_portfolio(&low_risk_stocks, RiskLevel::VeryLow, 100000.0);
    recommend_portfolio(&low_risk_stocks, RiskLevel::Low, 50000.0);

    println!("=== 추천 완료 ===");
    println!("💡 투자 팁:");
    println!("1. 저위험 주식은 베타가 0.8 이하인 종목입니다");
    println!("2. 배당수익률이 높은 종목은 안정적인 수익을 제공합니다");
    println!("3. 대형주는 시장 변동에 상대적으로 안정적입니다");
    println!("4. 섹터별 분산투자로 리스크를 줄일 수 있습니다");
}

fn create_sample_stocks() -> HashMap<String, Stock> {
    let mut stocks = HashMap::new();
    
    // 대형 안전 주식들
    let sample_stocks = vec![
        Stock::new(
            "JNJ".to_string(),
            "Johnson & Johnson".to_string(),
            "Healthcare".to_string(),
            450_000_000_000.0,
            160.0,
            Some(15.2),
            Some(2.8),
            Some(0.45),
            0.18,
        ),
        Stock::new(
            "PG".to_string(),
            "Procter & Gamble".to_string(),
            "Consumer Goods".to_string(),
            380_000_000_000.0,
            155.0,
            Some(24.8),
            Some(2.4),
            Some(0.38),
            0.15,
        ),
        Stock::new(
            "KO".to_string(),
            "The Coca-Cola Company".to_string(),
            "Consumer Goods".to_string(),
            260_000_000_000.0,
            60.0,
            Some(22.5),
            Some(3.2),
            Some(0.42),
            0.16,
        ),
        Stock::new(
            "PEP".to_string(),
            "PepsiCo Inc.".to_string(),
            "Consumer Goods".to_string(),
            240_000_000_000.0,
            170.0,
            Some(26.3),
            Some(2.9),
            Some(0.48),
            0.19,
        ),
        Stock::new(
            "WMT".to_string(),
            "Walmart Inc.".to_string(),
            "Retail".to_string(),
            480_000_000_000.0,
            160.0,
            Some(28.9),
            Some(1.4),
            Some(0.52),
            0.22,
        ),
        Stock::new(
            "VZ".to_string(),
            "Verizon Communications".to_string(),
            "Telecommunications".to_string(),
            180_000_000_000.0,
            40.0,
            Some(8.5),
            Some(6.8),
            Some(0.35),
            0.14,
        ),
        Stock::new(
            "T".to_string(),
            "AT&T Inc.".to_string(),
            "Telecommunications".to_string(),
            120_000_000_000.0,
            18.0,
            Some(7.2),
            Some(7.5),
            Some(0.58),
            0.24,
        ),
        Stock::new(
            "MCD".to_string(),
            "McDonald's Corporation".to_string(),
            "Food & Beverage".to_string(),
            200_000_000_000.0,
            280.0,
            Some(25.4),
            Some(2.1),
            Some(0.48),
            0.20,
        ),
        Stock::new(
            "CL".to_string(),
            "Colgate-Palmolive".to_string(),
            "Consumer Goods".to_string(),
            65_000_000_000.0,
            80.0,
            Some(28.7),
            Some(2.3),
            Some(0.42),
            0.17,
        ),
        Stock::new(
            "GIS".to_string(),
            "General Mills".to_string(),
            "Food & Beverage".to_string(),
            45_000_000_000.0,
            70.0,
            Some(16.8),
            Some(3.4),
            Some(0.38),
            0.15,
        ),
    ];
    
    for stock in sample_stocks {
        stocks.insert(stock.symbol.clone(), stock);
    }
    
    stocks
}

fn recommend_portfolio(stocks: &[&Stock], risk_level: RiskLevel, investment_amount: f64) {
    println!("💰 포트폴리오 추천 (위험도: {:?}, 투자금액: ${:.0})", risk_level, investment_amount);
    
    let mut sorted_stocks = stocks.to_vec();
    sorted_stocks.sort_by(|a, b| b.recommendation_score.partial_cmp(&a.recommendation_score).unwrap());
    
    let portfolio_size = match risk_level {
        RiskLevel::VeryLow => 5,
        RiskLevel::Low => 8,
        _ => 3,
    };
    
    let selected_stocks: Vec<&Stock> = sorted_stocks.into_iter().take(portfolio_size).collect();
    
    for (i, stock) in selected_stocks.iter().enumerate() {
        let allocation_percent = 100.0 / portfolio_size as f64;
        let allocation_amount = investment_amount * (allocation_percent / 100.0);
        let shares = (allocation_amount / stock.current_price) as i32;
        
        println!("   {}. {} ({})", i + 1, stock.name, stock.symbol);
        println!("      배분: {:.1}% (${:.0})", allocation_percent, allocation_amount);
        println!("      주식 수: {}주", shares);
        println!("      예상 배당: ${:.2}/년", shares as f64 * stock.current_price * stock.dividend_yield.unwrap_or(0.0) / 100.0);
        println!();
    }
}

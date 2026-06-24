use stock_recommender::{
    KoreanStock, KoreanMarket, KoreanRiskLevel
};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    println!("=== 한국투자증권 실시간 주식 추천 시스템 (데모) ===\n");

    // 샘플 한국 주식 데이터 생성
    let korean_stocks = create_sample_korean_stocks();
    
    // 저위험 주식 필터링
    let low_risk_stocks: Vec<&KoreanStock> = korean_stocks.iter()
        .filter(|stock| stock.is_korean_low_risk())
        .collect();
    
    println!("--- 저위험 한국 주식 목록 ---");
    for stock in &low_risk_stocks {
        println!("📈 {} ({})", stock.name, stock.symbol);
        println!("   시장: {:?}", stock.market);
        println!("   섹터: {}", stock.sector);
        println!("   현재가: {:.0}원", stock.current_price);
        println!("   등락률: {:+.2}%", stock.price_change);
        println!("   PER: {:.1}", stock.per.unwrap_or(0.0));
        println!("   PBR: {:.1}", stock.pbr.unwrap_or(0.0));
        println!("   배당수익률: {:.1}%", stock.dividend_yield.unwrap_or(0.0));
        println!("   베타: {:.2}", stock.beta.unwrap_or(0.0));
        println!("   위험도: {:?}", stock.get_korean_risk_level());
        println!("   추천점수: {:.1}/100", stock.recommendation_score);
        println!("   시가총액: {:.0}조원", stock.market_cap as f64 / 1_000_000_000_000.0);
        println!();
    }

    // 섹터별 분석
    println!("--- 섹터별 분석 ---");
    let mut sector_analysis: HashMap<String, Vec<&KoreanStock>> = HashMap::new();
    for stock in &low_risk_stocks {
        let sector = stock.classify_korean_sector();
        sector_analysis.entry(sector).or_insert_with(Vec::new).push(stock);
    }
    
    for (sector, sector_stocks) in sector_analysis {
        println!("🏢 {} 섹터: {}개 종목", sector, sector_stocks.len());
        for stock in sector_stocks {
            println!("   - {} (추천점수: {:.1})", stock.name, stock.recommendation_score);
        }
        println!();
    }

    // 포트폴리오 추천
    println!("--- 한국 주식 포트폴리오 추천 ---");
    recommend_korean_portfolio(&low_risk_stocks, KoreanRiskLevel::VeryLow, 100_000_000.0); // 1억원
    recommend_korean_portfolio(&low_risk_stocks, KoreanRiskLevel::Low, 50_000_000.0); // 5천만원

    // 시장 분석
    println!("--- 한국 시장 분석 ---");
    analyze_korean_market(&low_risk_stocks);

    println!("=== 데모 완료 ===");
    println!("💡 한국 주식 투자 팁:");
    println!("1. 코스피 대형주는 상대적으로 안전합니다");
    println!("2. 베타가 0.8 이하인 종목을 선택하세요");
    println!("3. 배당수익률 2% 이상인 종목을 고려하세요");
    println!("4. PER 15 이하, PBR 1.5 이하인 종목을 선호하세요");
    println!("5. 섹터별 분산투자로 리스크를 줄이세요");
    println!("6. 한국투자증권 API 연동으로 실시간 데이터 활용 가능");
}

fn create_sample_korean_stocks() -> Vec<KoreanStock> {
    vec![
        // 대형주 (코스피)
        KoreanStock::new(
            "005930".to_string(),
            "삼성전자".to_string(),
            KoreanMarket::KOSPI,
            "전자".to_string(),
            75000.0,
            74500.0,
            0.67,
            8_500_000,
            450_000_000_000_000,
            Some(12.5),
            Some(1.2),
            Some(2.1),
            Some(0.65),
            0.25,
        ),
        KoreanStock::new(
            "000660".to_string(),
            "SK하이닉스".to_string(),
            KoreanMarket::KOSPI,
            "반도체".to_string(),
            120000.0,
            118000.0,
            1.69,
            2_100_000,
            85_000_000_000_000,
            Some(8.2),
            Some(0.9),
            Some(1.8),
            Some(0.72),
            0.28,
        ),
        KoreanStock::new(
            "035420".to_string(),
            "NAVER".to_string(),
            KoreanMarket::KOSPI,
            "IT".to_string(),
            180000.0,
            182000.0,
            -1.10,
            450_000,
            28_000_000_000_000,
            Some(18.5),
            Some(2.1),
            Some(0.5),
            Some(0.85),
            0.32,
        ),
        KoreanStock::new(
            "207940".to_string(),
            "삼성바이오로직스".to_string(),
            KoreanMarket::KOSPI,
            "바이오".to_string(),
            850000.0,
            840000.0,
            1.19,
            120_000,
            60_000_000_000_000,
            Some(45.2),
            Some(3.8),
            Some(0.2),
            Some(0.95),
            0.35,
        ),
        KoreanStock::new(
            "105560".to_string(),
            "KB금융".to_string(),
            KoreanMarket::KOSPI,
            "금융".to_string(),
            55000.0,
            54500.0,
            0.92,
            1_200_000,
            20_000_000_000_000,
            Some(4.2),
            Some(0.3),
            Some(4.5),
            Some(0.45),
            0.18,
        ),
        KoreanStock::new(
            "055550".to_string(),
            "신한지주".to_string(),
            KoreanMarket::KOSPI,
            "금융".to_string(),
            42000.0,
            41800.0,
            0.48,
            800_000,
            15_000_000_000_000,
            Some(3.8),
            Some(0.25),
            Some(5.2),
            Some(0.42),
            0.16,
        ),
        KoreanStock::new(
            "068270".to_string(),
            "셀트리온".to_string(),
            KoreanMarket::KOSPI,
            "바이오".to_string(),
            180000.0,
            178000.0,
            1.12,
            300_000,
            12_000_000_000_000,
            Some(22.1),
            Some(1.8),
            Some(1.2),
            Some(0.78),
            0.28,
        ),
        KoreanStock::new(
            "003550".to_string(),
            "LG".to_string(),
            KoreanMarket::KOSPI,
            "화학".to_string(),
            95000.0,
            94000.0,
            1.06,
            400_000,
            6_500_000_000_000,
            Some(6.8),
            Some(0.4),
            Some(3.8),
            Some(0.38),
            0.15,
        ),
        KoreanStock::new(
            "000270".to_string(),
            "기아".to_string(),
            KoreanMarket::KOSPI,
            "자동차".to_string(),
            110000.0,
            108000.0,
            1.85,
            1_500_000,
            25_000_000_000_000,
            Some(8.5),
            Some(0.6),
            Some(2.8),
            Some(0.68),
            0.22,
        ),
        KoreanStock::new(
            "017670".to_string(),
            "SK텔레콤".to_string(),
            KoreanMarket::KOSPI,
            "통신".to_string(),
            48000.0,
            47500.0,
            1.05,
            2_000_000,
            11_000_000_000_000,
            Some(8.2),
            Some(0.7),
            Some(4.2),
            Some(0.35),
            0.14,
        ),
        // 중형주 (코스닥)
        KoreanStock::new(
            "035720".to_string(),
            "카카오".to_string(),
            KoreanMarket::KOSDAQ,
            "IT".to_string(),
            45000.0,
            46000.0,
            -2.17,
            1_800_000,
            20_000_000_000_000,
            Some(25.8),
            Some(1.5),
            Some(0.3),
            Some(1.2),
            0.45,
        ),
        KoreanStock::new(
            "323410".to_string(),
            "카카오뱅크".to_string(),
            KoreanMarket::KOSDAQ,
            "금융".to_string(),
            35000.0,
            34500.0,
            1.45,
            800_000,
            8_500_000_000_000,
            Some(15.2),
            Some(1.8),
            Some(2.1),
            Some(0.88),
            0.38,
        ),
    ]
}

fn recommend_korean_portfolio(stocks: &[&KoreanStock], risk_level: KoreanRiskLevel, investment_amount: f64) {
    println!("💰 한국 주식 포트폴리오 추천 (위험도: {:?}, 투자금액: {:.0}원)", 
        risk_level, investment_amount);
    
    let mut sorted_stocks = stocks.to_vec();
    sorted_stocks.sort_by(|a, b| b.recommendation_score.partial_cmp(&a.recommendation_score).unwrap());
    
    let portfolio_size = match risk_level {
        KoreanRiskLevel::VeryLow => 5,
        KoreanRiskLevel::Low => 8,
        _ => 3,
    };
    
    let selected_stocks: Vec<&KoreanStock> = sorted_stocks.into_iter().take(portfolio_size).collect();
    
    for (i, stock) in selected_stocks.iter().enumerate() {
        let allocation_percent = 100.0 / portfolio_size as f64;
        let allocation_amount = investment_amount * (allocation_percent / 100.0);
        let shares = (allocation_amount / stock.current_price) as i32;
        
        println!("   {}. {} ({})", i + 1, stock.name, stock.symbol);
        println!("      배분: {:.1}% ({:.0}원)", allocation_percent, allocation_amount);
        println!("      주식 수: {}주", shares);
        println!("      예상 배당: {:.0}원/년", 
            shares as f64 * stock.current_price * stock.dividend_yield.unwrap_or(0.0) / 100.0);
        println!();
    }
}

fn analyze_korean_market(stocks: &[&KoreanStock]) {
    println!("📊 한국 시장 분석 결과:");
    
    // 시장별 분석
    let mut market_analysis: HashMap<KoreanMarket, Vec<&KoreanStock>> = HashMap::new();
    for stock in stocks {
        market_analysis.entry(stock.market.clone()).or_insert_with(Vec::new).push(stock);
    }
    
    for (market, market_stocks) in market_analysis {
        let avg_score = market_stocks.iter()
            .map(|s| s.recommendation_score)
            .sum::<f64>() / market_stocks.len() as f64;
        
        println!("   {:?} 시장: {}개 종목, 평균 추천점수 {:.1}", 
            market, market_stocks.len(), avg_score);
    }
    
    // 위험도별 분석
    let mut risk_analysis: HashMap<KoreanRiskLevel, Vec<&KoreanStock>> = HashMap::new();
    for stock in stocks {
        risk_analysis.entry(stock.get_korean_risk_level()).or_insert_with(Vec::new).push(stock);
    }
    
    println!("\n   위험도별 분포:");
    for (risk_level, risk_stocks) in risk_analysis {
        println!("   {:?}: {}개 종목", risk_level, risk_stocks.len());
    }
    
    // 배당수익률 분석
    let high_dividend_stocks: Vec<&KoreanStock> = stocks.iter()
        .filter(|s| s.dividend_yield.unwrap_or(0.0) > 3.0)
        .cloned()
        .collect();
    
    println!("\n   고배당 주식 (3% 이상): {}개", high_dividend_stocks.len());
    for stock in high_dividend_stocks {
        println!("   - {}: {:.1}%", stock.name, stock.dividend_yield.unwrap_or(0.0));
    }
}
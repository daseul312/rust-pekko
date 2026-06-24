//! Grok API를 사용한 주식 시장 분석 예제
//!
//! xAI의 Grok을 활용하여 X(Twitter) 데이터를 통합한 분석을 수행합니다.

use pekko_ml4t_system::llm::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🤖 Grok 기반 주식 시장 분석 예제");
    println!("   xAI의 Grok + X(Twitter) 데이터 통합");
    println!("=".repeat(50));

    // Grok 클라이언트 생성
    let grok_client = LLMClientFactory::create_grok_client(
        LLMClientFactory::load_grok_config_from_env()?
    )?;

    println!("✅ Grok 클라이언트 초기화 완료");
    println!("   모델: {}", grok_client.current_model());

    // 예제 1: 감정 분석 (X 트윗 특화)
    println!("\n📊 예제 1: X 트윗 감정 분석");
    let sentiment_request = SentimentAnalysisRequest {
        text: "AAPL stock is mooning! 🚀🚀🚀 #Apple #StockMarket".to_string(),
        context: Some("실시간 트윗 데이터".to_string()),
        symbol: Some("AAPL".to_string()),
    };

    match grok_client.analyze_sentiment(sentiment_request).await {
        Ok(result) => {
            println!("   긍정: {:.1}%, 부정: {:.1}%, 중립: {:.1}%",
                    result.sentiment.positive * 100.0,
                    result.sentiment.negative * 100.0,
                    result.sentiment.neutral * 100.0);
            println!("   종합 점수: {:.2}", result.sentiment.overall);
            println!("   분석: {}", result.reasoning);
        }
        Err(e) => println!("   감정 분석 실패: {}", e),
    }

    // 예제 2: 시장 리포트 생성 (X 데이터 통합)
    println!("\n📋 예제 2: 시장 리포트 생성 (X 데이터 통합)");
    let market_data = serde_json::json!({
        "trending_on_x": ["AAPL", "TSLA", "NVDA"],
        "social_sentiment": "bullish",
        "elon_musk_tweets": 3
    });

    let report_request = MarketReportRequest {
        symbols: vec!["AAPL".to_string(), "TSLA".to_string()],
        market_data,
        time_period: "2024-01-01".to_string(),
        include_technical_analysis: true,
    };

    match grok_client.generate_market_report(report_request).await {
        Ok(result) => {
            println!("   신뢰도: {:.1}%", result.confidence * 100.0);
            println!("   리포트 내용:");
            println!("   {}", result.content.lines().take(3).collect::<Vec<_>>().join("\n   "));
        }
        Err(e) => println!("   리포트 생성 실패: {}", e),
    }

    // 예제 3: 가격 예측 (X 트렌드 반영)
    println!("\n🎯 예제 3: 가격 예측 (X 트렌드 반영)");
    let historical_data = vec![
        serde_json::json!({"price": 150.0, "volume": 1000000, "date": "2024-01-01"}),
        serde_json::json!({"price": 155.0, "volume": 1200000, "date": "2024-01-02"}),
    ];

    let prediction_request = PricePredictionRequest {
        symbol: "AAPL".to_string(),
        historical_data,
        time_horizon: "3d".to_string(),
        prediction_type: PredictionType::ShortTerm,
    };

    match grok_client.predict_price(prediction_request).await {
        Ok(result) => {
            println!("   예측 가격: ${:.2}", result.predicted_price);
            println!("   신뢰도: {:.1}%", result.confidence * 100.0);
            println!("   기간: {}", result.time_horizon);
            println!("   근거: {}", result.reasoning);
        }
        Err(e) => println!("   가격 예측 실패: {}", e),
    }

    // 예제 4: 일반 텍스트 분석
    println!("\n💬 예제 4: 일반 텍스트 분석");
    let analysis_prompt = "현재 주식 시장 상황과 앞으로의 전망을 분석해주세요.".to_string();

    match grok_client.analyze_text(analysis_prompt, None).await {
        Ok(result) => {
            println!("   분석 결과: {}", result.content.chars().take(100).collect::<String>());
            println!("   신뢰도: {:.1}%", result.confidence * 100.0);
            println!("   사용 토큰: {}", result.tokens_used);
        }
        Err(e) => println!("   텍스트 분석 실패: {}", e),
    }

    println!("\n" + "=".repeat(50));
    println!("🎉 Grok 분석 예제 완료!");
    println!("💡 Grok의 강점:");
    println!("   • 실시간 X 데이터 통합");
    println!("   • Elon Musk 영향도 고려");
    println!("   • 바이럴 트렌드 분석");
    println!("   • 유머러스하고 직관적인 응답");

    Ok(())
}

/// Grok vs OpenAI 비교 함수
pub async fn compare_llm_providers() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔄 Grok vs OpenAI 비교 분석");

    // 같은 프롬프트로 두 LLM 비교
    let test_prompt = "AAPL 주식의 향후 1주 전망을 분석해주세요.".to_string();

    // Grok 분석
    if let Ok(grok_client) = LLMClientFactory::create_grok_client(
        LLMClientFactory::load_grok_config_from_env().unwrap_or_default()
    ) {
        println!("🤖 Grok 분석:");
        if let Ok(result) = grok_client.analyze_text(test_prompt.clone(), None).await {
            println!("   {}", result.content.lines().next().unwrap_or("분석 결과 없음"));
        }
    }

    // OpenAI 분석
    if let Ok(openai_client) = LLMClientFactory::create_openai_client(
        LLMClientFactory::load_openai_config_from_env().unwrap_or_default()
    ) {
        println!("\n🔄 OpenAI 분석:");
        if let Ok(result) = openai_client.analyze_text(test_prompt, None).await {
            println!("   {}", result.content.lines().next().unwrap_or("분석 결과 없음"));
        }
    }

    println!("\n💡 차이점:");
    println!("   • Grok: X 데이터 + 유머러스한 스타일");
    println!("   • OpenAI: 균형 잡힌 전문적 분석");

    Ok(())
}
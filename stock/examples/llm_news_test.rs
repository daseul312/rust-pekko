//! LLM 뉴스 분석 테스트
//!
//! Grok을 사용한 지능형 뉴스 감성 분석 테스트
//!
//! ```
//! # 환경변수 설정 필요
//! XAI_API_KEY=your_key cargo run --example llm_news_test
//! ```

use stock::news_analysis::{NewsReader, KeywordSentimentAnalyzer};
use stock::llm_analysis::LlmSentimentAnalyzer;
use tracing::{ Level};
use tracing_subscriber::FmtSubscriber;
use dotenv::dotenv;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    
    let _subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .compact()
        .init();
    // ===== 1. 뉴스 로드 =====
    let reader = NewsReader::new();
    let articles = reader.load_recent_news(24)?;
    if articles.is_empty() {
        return Ok(());
    }

    // ===== 2. 키워드 분석 (기존 방식) =====
    let keyword_analyzer = KeywordSentimentAnalyzer::new();
    let keyword_result = keyword_analyzer.analyze_batch(&articles);
    // ===== 3. LLM 분석 (새 방식) =====
    let llm_analyzer = match LlmSentimentAnalyzer::new() {
        Ok(a) => a,
        Err(e) => {
            return Ok(());
        }
    };

    let llm_result = llm_analyzer.analyze_batch(&articles, Some("005930")).await?;
    // ===== 비교 =====
    Ok(())
}

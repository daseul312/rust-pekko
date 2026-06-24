//! News Analysis Report Example
//!
//! Generates a trading recommendation based on stock news.

use stock::news_analysis::{NewsReader, KeywordSentimentAnalyzer};

fn main() {
    let reader = NewsReader::new();
    
    // Load all stock news (not just recent)
    let articles = match reader.load_stock_news() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("Error loading news: {}", e);
            return;
        }
    };
    
    println!("📰 Loaded {} stock news articles\n", articles.len());
    
    // Analyze sentiment
    let analyzer = KeywordSentimentAnalyzer::new();
    let result = analyzer.analyze_batch(&articles);
    
    println!("📊 뉴스 분석 리포트");
    println!("━━━━━━━━━━━━━━━━━━━━");
    println!("{}", result.summary());
    println!();
    
    let recommendation = result.trading_recommendation();
    println!("📌 매매 추천: {} {:?}", recommendation.emoji(), recommendation);
    println!();
    
    println!("🔥 주요 뉴스 (긍정/부정):");
    for analyzed in result.articles.iter()
        .filter(|a| !matches!(a.sentiment, stock::news_analysis::Sentiment::Neutral))
        .take(10) 
    {
        let emoji = match analyzed.sentiment {
            stock::news_analysis::Sentiment::VeryPositive => "🚀",
            stock::news_analysis::Sentiment::Positive => "📈",
            stock::news_analysis::Sentiment::Negative => "📉",
            stock::news_analysis::Sentiment::VeryNegative => "🔻",
            _ => "➖",
        };
        // Safe truncation for UTF-8
        let title: String = analyzed.article.title.chars().take(40).collect();
        println!("  {} {} [{}]", emoji, title, analyzed.article.region);
    }
}

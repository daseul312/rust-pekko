//! 뉴스 수집 CLI
//! 
//! Usage:
//!   cargo run --bin news_cli -- --symbol AAPL
//!   cargo run --bin news_cli -- --symbol 삼성전자 --days 7

use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Deserialize)]
struct NewsApiResponse {
    status: String,
    #[serde(rename = "totalResults")]
    total_results: Option<u32>,
    articles: Option<Vec<Article>>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Article {
    title: String,
    description: Option<String>,
    url: String,
    source: Source,
    #[serde(rename = "publishedAt")]
    published_at: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Source {
    name: String,
}

#[derive(Debug, Deserialize)]
struct AlphaVantageResponse {
    feed: Option<Vec<AlphaVantageNews>>,
}

#[derive(Debug, Deserialize, Serialize)]
struct AlphaVantageNews {
    title: String,
    url: String,
    summary: String,
    source: String,
    time_published: String,
    overall_sentiment_score: f64,
    overall_sentiment_label: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    
    let args: Vec<String> = env::args().collect();
    
    let mut symbol = "AAPL".to_string();
    let mut days = 3;
    let mut use_alpha_vantage = false;
    let mut use_naver = false;
    let mut store_to_memory = false;
    
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--symbol" | "-s" => {
                if i + 1 < args.len() {
                    symbol = args[i + 1].clone();
                    i += 1;
                }
            }
            "--days" | "-d" => {
                if i + 1 < args.len() {
                    days = args[i + 1].parse().unwrap_or(3);
                    i += 1;
                }
            }
            "--alpha" | "-a" => {
                use_alpha_vantage = true;
            }
            "--naver" | "-n" => {
                use_naver = true;
            }
            "--store" | "-m" => {
                store_to_memory = true;
            }
            "--help" | "-h" => {
                println!("뉴스 수집 CLI 🗞️");
                println!();
                println!("Usage:");
                println!("  news_cli [OPTIONS]");
                println!();
                println!("Options:");
                println!("  -s, --symbol <SYMBOL>  검색할 심볼/키워드 (기본: AAPL)");
                println!("  -d, --days <DAYS>      검색 기간 (기본: 3일)");
                println!("  -a, --alpha            Alpha Vantage API (센티먼트 포함)");
                println!("  -n, --naver            네이버 뉴스 (한국어)");
                println!("  -m, --store            memory-brain에 저장");
                println!("  -h, --help             도움말");
                return Ok(());
            }
            _ => {}
        }
        i += 1;
    }
    
    println!("🗞️ 뉴스 수집 CLI");
    println!("   심볼: {}", symbol);
    println!("   기간: {}일", days);
    println!();
    
    let client = Client::new();
    
    if use_naver {
        fetch_naver_news(&client, &symbol, store_to_memory).await?;
    } else if use_alpha_vantage {
        fetch_alpha_vantage(&client, &symbol, store_to_memory).await?;
    } else {
        fetch_newsapi(&client, &symbol, days, store_to_memory).await?;
    }
    
    Ok(())
}

async fn fetch_newsapi(client: &Client, symbol: &str, days: i32, store: bool) -> Result<()> {
    let api_key = env::var("NEWS_API_KEY")
        .expect("NEWS_API_KEY 환경변수가 필요합니다");
    
    let from_date = chrono::Utc::now() - chrono::Duration::days(days as i64);
    let from_str = from_date.format("%Y-%m-%d").to_string();
    
    println!("📰 NewsAPI에서 검색 중...");
    
    let url = format!(
        "https://newsapi.org/v2/everything?q={}&from={}&sortBy=publishedAt&language=en&pageSize=10&apiKey={}",
        symbol, from_str, api_key
    );
    
    let response: NewsApiResponse = client.get(&url).send().await?.json().await?;
    
    if response.status != "ok" {
        println!("❌ API 오류: {}", response.status);
        return Ok(());
    }
    
    let articles = response.articles.unwrap_or_default();
    println!("✅ {}개 기사 발견\n", articles.len());
    
    for (i, article) in articles.iter().enumerate() {
        println!("{}. {}", i + 1, article.title);
        println!("   📍 {} | {}", article.source.name, &article.published_at[..10]);
        if let Some(desc) = &article.description {
            let preview: String = desc.chars().take(100).collect();
            println!("   {}", preview);
        }
        println!();
        
        if store {
            store_to_memory_brain(&article.title, &format!("news,{}", symbol)).await.ok();
        }
    }
    
    Ok(())
}

async fn fetch_alpha_vantage(client: &Client, symbol: &str, store: bool) -> Result<()> {
    let api_key = env::var("ALPHA_VANTAGE_API_KEY")
        .expect("ALPHA_VANTAGE_API_KEY 환경변수가 필요합니다");
    
    println!("📊 Alpha Vantage에서 검색 중 (센티먼트 포함)...");
    
    let url = format!(
        "https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={}&apikey={}",
        symbol, api_key
    );
    
    let response: AlphaVantageResponse = client.get(&url).send().await?.json().await?;
    
    let news = response.feed.unwrap_or_default();
    println!("✅ {}개 기사 발견\n", news.len());
    
    for (i, article) in news.iter().take(10).enumerate() {
        let sentiment_emoji = if article.overall_sentiment_score > 0.1 {
            "🟢"
        } else if article.overall_sentiment_score < -0.1 {
            "🔴"
        } else {
            "⚪"
        };
        
        println!("{}. {} {}", i + 1, sentiment_emoji, article.title);
        println!("   📍 {} | 센티먼트: {} ({:.2})", 
            article.source, 
            article.overall_sentiment_label,
            article.overall_sentiment_score
        );
        let preview: String = article.summary.chars().take(100).collect();
        println!("   {}", preview);
        println!();
        
        if store {
            let content = format!("{} [센티먼트: {}]", article.title, article.overall_sentiment_label);
            store_to_memory_brain(&content, &format!("news,{},sentiment", symbol)).await.ok();
        }
    }
    
    Ok(())
}

async fn fetch_naver_news(client: &Client, query: &str, store: bool) -> Result<()> {
    let client_id = env::var("NAVER_CLIENT_ID").ok();
    let client_secret = env::var("NAVER_CLIENT_SECRET").ok();
    
    if client_id.is_none() || client_secret.is_none() {
        println!("⚠️ 네이버 API 키 필요 (NAVER_CLIENT_ID, NAVER_CLIENT_SECRET)");
        println!("   https://developers.naver.com 에서 발급받으세요");
        return Ok(());
    }
    
    println!("📰 네이버 뉴스에서 검색 중...");
    
    let url = format!(
        "https://openapi.naver.com/v1/search/news.json?query={}&display=10&sort=date",
        urlencoding::encode(query)
    );
    
    let response = client
        .get(&url)
        .header("X-Naver-Client-Id", client_id.unwrap())
        .header("X-Naver-Client-Secret", client_secret.unwrap())
        .send()
        .await?;
    
    if !response.status().is_success() {
        println!("❌ 네이버 API 오류: {}", response.status());
        return Ok(());
    }
    
    let data: serde_json::Value = response.json().await?;
    let items = data["items"].as_array();
    
    if let Some(items) = items {
        println!("✅ {}개 기사 발견\n", items.len());
        
        for (i, item) in items.iter().enumerate() {
            let title = item["title"].as_str().unwrap_or("")
                .replace("<b>", "").replace("</b>", "")
                .replace("&quot;", "\"").replace("&amp;", "&");
            let desc = item["description"].as_str().unwrap_or("")
                .replace("<b>", "").replace("</b>", "")
                .replace("&quot;", "\"").replace("&amp;", "&");
            let date = item["pubDate"].as_str().unwrap_or("");
            
            println!("{}. {}", i + 1, title);
            println!("   📅 {}", date);
            let preview: String = desc.chars().take(80).collect();
            println!("   {}", preview);
            println!();
            
            if store {
                store_to_memory_brain(&title, &format!("news,korea,{}", query)).await.ok();
            }
        }
    }
    
    Ok(())
}

async fn store_to_memory_brain(content: &str, tags: &str) -> Result<()> {
    let client = Client::new();
    let tags_vec: Vec<&str> = tags.split(',').collect();
    
    let body = serde_json::json!({
        "content": content,
        "tags": tags_vec
    });
    
    let response = client
        .post("http://127.0.0.1:3030/api/store")
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;
    
    if response.status().is_success() {
        println!("   💾 memory-brain에 저장됨");
    }
    
    Ok(())
}

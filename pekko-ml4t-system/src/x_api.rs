//! X(Twitter) API 클라이언트
//!
//! 실시간 트윗 데이터를 수집하여 감정 분석에 활용합니다.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// X API 응답 구조체들
#[derive(Debug, Deserialize)]
pub struct XTweet {
    pub id: String,
    pub text: String,
    pub author_id: String,
    pub created_at: String,
    pub public_metrics: Option<XTweetMetrics>,
    pub context_annotations: Option<Vec<XContextAnnotation>>,
}

#[derive(Debug, Deserialize)]
pub struct XTweetMetrics {
    pub retweet_count: u32,
    pub like_count: u32,
    pub reply_count: u32,
    pub quote_count: u32,
}

#[derive(Debug, Deserialize)]
pub struct XContextAnnotation {
    pub domain: XDomain,
    pub entity: XEntity,
}

#[derive(Debug, Deserialize)]
pub struct XDomain {
    pub id: String,
    pub name: String,
    pub description: String,
}

#[derive(Debug, Deserialize)]
pub struct XEntity {
    pub id: String,
    pub name: String,
    pub description: String,
}

/// X API 클라이언트 (간단한 구조)
pub struct XApiClient {
    bearer_token: String,
    client: reqwest::Client,
}

impl XApiClient {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let bearer_token = std::env::var("X_BEARER_TOKEN")
            .map_err(|_| "X_BEARER_TOKEN 환경 변수가 설정되지 않았습니다")?;

        let client = reqwest::Client::new();

        Ok(Self {
            bearer_token,
            client,
        })
    }

    /// 종목 관련 트윗 검색
    pub async fn search_tweets(&self, query: &str, max_results: u32) -> Result<Vec<XTweet>, Box<dyn std::error::Error>> {
        let url = format!(
            "https://api.twitter.com/2/tweets/search/recent?query={}&max_results={}&tweet.fields=created_at,author_id,public_metrics,context_annotations",
            urlencoding::encode(query),
            max_results
        );

        let response = self.client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(format!("X API 요청 실패: {}", response.status()).into());
        }

        #[derive(Deserialize)]
        struct XSearchResponse {
            data: Option<Vec<XTweet>>,
        }

        let search_response: XSearchResponse = response.json().await?;
        Ok(search_response.data.unwrap_or_default())
    }

    /// 핫 트렌드 키워드 검색
    pub async fn get_trending_topics(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        // 실제로는 트렌드 API를 사용하지만, 여기서는 모의 데이터 반환
        Ok(vec![
            "#StockMarket".to_string(),
            "#Bitcoin".to_string(),
            "#AI".to_string(),
            "#Tesla".to_string(),
            "#Apple".to_string(),
        ])
    }

    /// 종목 심볼 기반 트윗 검색 쿼리 생성
    pub fn create_symbol_query(&self, symbol: &str) -> String {
        format!("({} OR ${}) -is:retweet lang:en", symbol, symbol)
    }

    /// 감정 분석용 트윗 필터링
    pub fn filter_tweets_for_sentiment(&self, tweets: Vec<XTweet>) -> Vec<XTweet> {
        tweets.into_iter()
            .filter(|tweet| {
                // 기본 필터링: 영어, 일정 길이 이상, 리트윗 제외
                tweet.text.len() > 10 &&
                tweet.text.len() < 280 &&
                !tweet.text.contains("RT @")
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_symbol_query() {
        let client = XApiClient::new().unwrap();
        let query = client.create_symbol_query("AAPL");
        assert!(query.contains("AAPL"));
        assert!(query.contains("$AAPL"));
    }

    #[test]
    fn test_filter_tweets_for_sentiment() {
        let client = XApiClient::new().unwrap();

        let tweets = vec![
            XTweet {
                id: "1".to_string(),
                text: "AAPL is doing great!".to_string(),
                author_id: "123".to_string(),
                created_at: "2024-01-01T00:00:00Z".to_string(),
                public_metrics: None,
                context_annotations: None,
            },
            XTweet {
                id: "2".to_string(),
                text: "RT @someone AAPL sucks".to_string(), // 리트윗 제외
                author_id: "124".to_string(),
                created_at: "2024-01-01T00:00:00Z".to_string(),
                public_metrics: None,
                context_annotations: None,
            },
            XTweet {
                id: "3".to_string(),
                text: "Hi".to_string(), // 너무 짧음
                author_id: "125".to_string(),
                created_at: "2024-01-01T00:00:00Z".to_string(),
                public_metrics: None,
                context_annotations: None,
            },
        ];

        let filtered = client.filter_tweets_for_sentiment(tweets);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, "1");
    }
}
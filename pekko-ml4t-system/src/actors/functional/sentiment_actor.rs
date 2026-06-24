//! 감정 분석 함수형 Actor
//!
//! 뉴스, 소셜 미디어, 트윗 등의 텍스트 데이터를 분석하여 시장 감정을 평가합니다.

use pekko_actor::{FunctionalActor, FunctionalActorContext, Effect};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 감정 분석 상태 (불변)
#[derive(Debug, Clone)]
pub struct SentimentState {
    /// 심볼별 감정 히스토리
    pub sentiment_history: HashMap<String, Vec<SentimentScore>>,
    /// 감정 분석 설정
    pub config: SentimentConfig,
    /// 마지막 업데이트 시간
    pub last_update: chrono::DateTime<chrono::Utc>,
}

/// 감정 점수
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentimentScore {
    pub positive: f64,    // 긍정 점수 (0.0-1.0)
    pub negative: f64,    // 부정 점수 (0.0-1.0)
    pub neutral: f64,     // 중립 점수 (0.0-1.0)
    pub overall: f64,     // 종합 점수 (-1.0 to 1.0)
    pub confidence: f64,  // 분석 신뢰도 (0.0-1.0)
    pub source_count: u32, // 분석된 소스 수
    pub analyzed_at: chrono::DateTime<chrono::Utc>,
}

/// 감정 포인트 (텍스트 분석의 기본 단위)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentimentPoint {
    pub symbol: String,
    pub text: String,
    pub source: SentimentSource,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// 감정 분석 소스
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SentimentSource {
    News,
    Twitter,
    Reddit,
    StockForum,
    AnalystReport,
    Other(String),
}

/// 감정 분석 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentimentConfig {
    pub min_confidence_threshold: f64,
    pub max_history_size: usize,
    pub enable_twitter_analysis: bool,
    pub enable_news_analysis: bool,
    pub sentiment_decay_factor: f64, // 시간에 따른 감정 점수 감쇠
    pub weight_news: f64,
    pub weight_social: f64,
    pub weight_analyst: f64,
}

impl Default for SentimentConfig {
    fn default() -> Self {
        Self {
            min_confidence_threshold: 0.6,
            max_history_size: 1000,
            enable_twitter_analysis: true,
            enable_news_analysis: true,
            sentiment_decay_factor: 0.95, // 매일 5% 감쇠
            weight_news: 0.4,
            weight_social: 0.4,
            weight_analyst: 0.2,
        }
    }
}

impl SentimentState {
    pub fn new() -> Self {
        Self {
            sentiment_history: HashMap::new(),
            config: SentimentConfig::default(),
            last_update: chrono::Utc::now(),
        }
    }

    /// 감정 포인트 추가 (불변 방식)
    pub fn add_sentiment_point(mut self, point: SentimentPoint) -> Self {
        let symbol = point.symbol.clone();

        // 감정 분석 (단순 키워드 기반)
        let score = self.analyze_sentiment(&point.text, &point.source);

        let history = self.sentiment_history.entry(symbol).or_insert_with(Vec::new);

        // 최대 히스토리 크기 제한
        if history.len() >= self.config.max_history_size {
            history.remove(0);
        }

        history.push(score);
        self.last_update = chrono::Utc::now();
        self
    }

    /// 감정 분석 (단순 구현)
    fn analyze_sentiment(&self, text: &str, source: &SentimentSource) -> SentimentScore {
        let text_lower = text.to_lowercase();

        // 간단한 키워드 기반 분석
        let positive_keywords = ["상승", "급등", "호재", "buy", "bullish", "good", "excellent", "great", "positive"];
        let negative_keywords = ["하락", "급락", "악재", "sell", "bearish", "bad", "terrible", "negative", "crash"];

        let positive_count = positive_keywords.iter()
            .map(|kw| text_lower.matches(kw).count())
            .sum::<usize>() as f64;

        let negative_count = negative_keywords.iter()
            .map(|kw| text_lower.matches(kw).count())
            .sum::<usize>() as f64;

        let total_keywords = positive_count + negative_count;

        let positive = if total_keywords > 0.0 { positive_count / total_keywords } else { 0.5 };
        let negative = if total_keywords > 0.0 { negative_count / total_keywords } else { 0.5 };
        let neutral = 1.0 - positive - negative;

        let overall = positive - negative;

        // 소스별 가중치 적용
        let weight = match source {
            SentimentSource::News => self.config.weight_news,
            SentimentSource::Twitter | SentimentSource::Reddit | SentimentSource::StockForum => self.config.weight_social,
            SentimentSource::AnalystReport => self.config.weight_analyst,
            SentimentSource::Other(_) => 0.1,
        };

        SentimentScore {
            positive: positive * weight,
            negative: negative * weight,
            neutral: neutral * weight,
            overall: overall * weight,
            confidence: (total_keywords / 10.0).min(1.0), // 키워드 수에 따른 신뢰도
            source_count: 1,
            analyzed_at: chrono::Utc::now(),
        }
    }

    /// 특정 심볼의 최근 감정 점수 조회
    pub fn get_recent_sentiment(&self, symbol: &str) -> Option<&SentimentScore> {
        self.sentiment_history.get(symbol)?.last()
    }

    /// 특정 심볼의 평균 감정 점수 계산 (최근 N개)
    pub fn get_average_sentiment(&self, symbol: &str, count: usize) -> Option<SentimentScore> {
        let history = self.sentiment_history.get(symbol)?;

        if history.is_empty() {
            return None;
        }

        let recent_scores = history.iter()
            .rev()
            .take(count)
            .collect::<Vec<_>>();

        let len = recent_scores.len() as f64;

        let positive = recent_scores.iter().map(|s| s.positive).sum::<f64>() / len;
        let negative = recent_scores.iter().map(|s| s.negative).sum::<f64>() / len;
        let neutral = recent_scores.iter().map(|s| s.neutral).sum::<f64>() / len;
        let overall = recent_scores.iter().map(|s| s.overall).sum::<f64>() / len;
        let confidence = recent_scores.iter().map(|s| s.confidence).sum::<f64>() / len;
        let source_count = recent_scores.iter().map(|s| s.source_count).sum::<u32>();

        Some(SentimentScore {
            positive,
            negative,
            neutral,
            overall,
            confidence,
            source_count,
            analyzed_at: chrono::Utc::now(),
        })
    }

    /// 시간에 따른 감정 점수 감쇠 적용
    pub fn apply_time_decay(mut self) -> Self {
        let now = chrono::Utc::now();

        for history in self.sentiment_history.values_mut() {
            for score in history.iter_mut() {
                let hours_elapsed = (now - score.analyzed_at).num_hours() as f64;
                let decay_factor = self.config.sentiment_decay_factor.powf(hours_elapsed / 24.0); // 일단위 감쇠

                score.positive *= decay_factor;
                score.negative *= decay_factor;
                score.neutral *= decay_factor;
                score.overall *= decay_factor;
                score.confidence *= decay_factor;
            }
        }

        self.last_update = now;
        self
    }
}

/// 감정 분석 메시지
#[derive(Debug, Clone)]
pub enum SentimentMessage {
    /// 텍스트 감정 분석 요청
    AnalyzeText {
        symbol: String,
        text: String,
        source: SentimentSource,
    },

    /// 감정 점수 조회
    GetSentiment {
        symbol: String,
        average_count: Option<usize>,
    },

    /// 감정 통계 조회
    GetSentimentStats {
        symbol: Option<String>,
        hours: Option<u64>,
    },

    /// 시간 감쇠 적용
    ApplyTimeDecay,

    /// 설정 업데이트
    UpdateConfig {
        config: SentimentConfig,
    },
}

/// 감정 분석 통계
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentimentStats {
    pub symbol: Option<String>,
    pub total_analyses: usize,
    pub avg_positive: f64,
    pub avg_negative: f64,
    pub avg_neutral: f64,
    pub avg_overall: f64,
    pub avg_confidence: f64,
    pub period_hours: u64,
    pub generated_at: chrono::DateTime<chrono::Utc>,
}

/// 감정 분석 함수형 Actor
pub struct SentimentActor;

impl SentimentActor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SentimentActor {
    fn default() -> Self {
        Self::new()
    }
}

impl FunctionalActor for SentimentActor {
    type State = SentimentState;
    type Message = SentimentMessage;

    fn init(_ctx: &FunctionalActorContext) -> Self::State {
        SentimentState::new()
    }

    fn receive(
        state: Self::State,
        msg: Self::Message,
        _ctx: &FunctionalActorContext,
    ) -> (Self::State, Effect) {
        match msg {
            SentimentMessage::AnalyzeText { symbol, text, source } => {
                let point = SentimentPoint {
                    symbol,
                    text,
                    source,
                    timestamp: chrono::Utc::now(),
                };
                let new_state = state.add_sentiment_point(point);
                (new_state, Effect::None)
            }

            SentimentMessage::GetSentiment { symbol, average_count } => {
                let count = average_count.unwrap_or(1);
                if count == 1 {
                    if let Some(sentiment) = state.get_recent_sentiment(&symbol) {
                    } else {
                        tracing::warn!("{} 심볼의 감정 분석 데이터가 없습니다", symbol);
                    }
                } else {
                    if let Some(avg_sentiment) = state.get_average_sentiment(&symbol, count) {
                    } else {
                        tracing::warn!("{} 심볼의 충분한 감정 분석 데이터가 없습니다", symbol);
                    }
                }
                (state, Effect::None)
            }

            SentimentMessage::GetSentimentStats { symbol, hours } => {
                let hours = hours.unwrap_or(24);
                // 간단한 통계 계산 (실제로는 더 복잡한 로직 필요)
                (state, Effect::None)
            }

            SentimentMessage::ApplyTimeDecay => {
                let new_state = state.apply_time_decay();
                (new_state, Effect::None)
            }

            SentimentMessage::UpdateConfig { config } => {
                let mut new_state = state;
                new_state.config = config;
                (new_state, Effect::None)
            }
        }
    }

    fn apply_effect(
        _effect: Effect,
        _ctx: &mut FunctionalActorContext
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            // LLM API 호출 등의 비동기 작업
            tracing::debug!("SentimentActor Effect 적용");
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sentiment_analysis() {
        let state = SentimentState::new();

        let point = SentimentPoint {
            symbol: "AAPL".to_string(),
            text: "AAPL stock is showing bullish patterns and great earnings".to_string(),
            source: SentimentSource::News,
            timestamp: chrono::Utc::now(),
        };

        let new_state = state.add_sentiment_point(point);

        assert!(new_state.sentiment_history.contains_key("AAPL"));
        assert_eq!(new_state.sentiment_history["AAPL"].len(), 1);
    }

    #[test]
    fn test_get_recent_sentiment() {
        let state = SentimentState::new();
        let point = SentimentPoint {
            symbol: "AAPL".to_string(),
            text: "positive news".to_string(),
            source: SentimentSource::News,
            timestamp: chrono::Utc::now(),
        };

        let state_with_sentiment = state.add_sentiment_point(point);
        let sentiment = state_with_sentiment.get_recent_sentiment("AAPL");

        assert!(sentiment.is_some());
        assert!(sentiment.unwrap().positive > 0.0);
    }

    #[test]
    fn test_time_decay() {
        let state = SentimentState::new();
        let point = SentimentPoint {
            symbol: "AAPL".to_string(),
            text: "test".to_string(),
            source: SentimentSource::News,
            timestamp: chrono::Utc::now() - chrono::Duration::hours(48), // 2일 전
        };

        let state_with_sentiment = state.add_sentiment_point(point);
        let decayed_state = state_with_sentiment.apply_time_decay();

        let recent_sentiment = decayed_state.get_recent_sentiment("AAPL").unwrap();
        // 감쇠가 적용되어 원래 값보다 작아야 함
        assert!(recent_sentiment.positive < 1.0 || recent_sentiment.negative < 1.0);
    }
}
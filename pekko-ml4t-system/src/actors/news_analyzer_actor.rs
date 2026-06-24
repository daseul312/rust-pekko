//! 뉴스 분석 Actor
//!
//! 수집된 뉴스의 감성 분석 및 투자 신호 생성
//! - 키워드 기반 감성 분석
//! - 종목별 뉴스 점수 계산
//! - 투자 신호 생성 및 전달

use crate::actors::news_collector_actor::{NewsArticle, NewsSource};
use crate::actors::{TradingSignal, PersistentTradingMessage};
use async_trait::async_trait;
use chrono::{DateTime, Utc, Duration};
use pekko_actor::{Actor, ActorContext};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{ warn, debug, error};

// ============================================================
// 분석 결과 타입
// ============================================================

/// 뉴스 감성 분석 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewsSentiment {
    pub article_id: String,
    pub symbol: String,
    pub sentiment_score: f64,      // -1.0 (매우 부정) ~ 1.0 (매우 긍정)
    pub confidence: f64,           // 0.0 ~ 1.0
    pub sentiment_type: SentimentType,
    pub key_phrases: Vec<String>,
    pub analyzed_at: DateTime<Utc>,
}

/// 감성 타입
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SentimentType {
    VeryPositive,  // > 0.5
    Positive,      // 0.2 ~ 0.5
    Neutral,       // -0.2 ~ 0.2
    Negative,      // -0.5 ~ -0.2
    VeryNegative,  // < -0.5
}

impl SentimentType {
    fn from_score(score: f64) -> Self {
        if score > 0.5 { SentimentType::VeryPositive }
        else if score > 0.2 { SentimentType::Positive }
        else if score > -0.2 { SentimentType::Neutral }
        else if score > -0.5 { SentimentType::Negative }
        else { SentimentType::VeryNegative }
    }
}

/// 종목별 뉴스 요약
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolNewsSummary {
    pub symbol: String,
    pub article_count: usize,
    pub avg_sentiment: f64,
    pub sentiment_trend: SentimentTrend,
    pub recommended_action: RecommendedAction,
    pub confidence: f64,
    pub latest_headlines: Vec<String>,
    pub updated_at: DateTime<Utc>,
}

/// 감성 추세
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SentimentTrend {
    Improving,   // 점점 좋아지는 중
    Stable,      // 변화 없음
    Declining,   // 점점 나빠지는 중
}

/// 추천 액션
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecommendedAction {
    StrongBuy,
    Buy,
    Hold,
    Sell,
    StrongSell,
}

// ============================================================
// Actor 메시지
// ============================================================

/// 뉴스 분석 Actor 메시지
#[derive(Debug, Clone)]
pub enum NewsAnalyzerMessage {
    /// 단일 뉴스 분석
    AnalyzeArticle { article: NewsArticle },
    /// 여러 뉴스 일괄 분석
    AnalyzeArticles { articles: Vec<NewsArticle> },
    /// 종목별 요약 생성
    GenerateSummary { symbol: String },
    /// 모든 종목 요약 생성
    GenerateAllSummaries,
    /// 투자 신호 생성 요청
    GenerateSignals,
    /// 분석 결과 조회
    GetSentimentFor { symbol: String },
    /// Trading Actor 연결
    ConnectTradingActor { actor_ref: TradingActorRef },
}

/// Trading Actor 참조 (타입 소거)
pub struct TradingActorRef {
    sender: tokio::sync::mpsc::UnboundedSender<PersistentTradingMessage>,
}

impl TradingActorRef {
    pub fn new(sender: tokio::sync::mpsc::UnboundedSender<PersistentTradingMessage>) -> Self {
        Self { sender }
    }

    pub fn send(&self, msg: PersistentTradingMessage) -> Result<(), String> {
        self.sender.send(msg).map_err(|e| e.to_string())
    }
}

impl std::fmt::Debug for TradingActorRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TradingActorRef")
    }
}

impl Clone for TradingActorRef {
    fn clone(&self) -> Self {
        Self { sender: self.sender.clone() }
    }
}

// ============================================================
// 키워드 사전
// ============================================================

/// 감성 분석용 키워드 사전
struct SentimentDictionary {
    positive_words: HashMap<&'static str, f64>,
    negative_words: HashMap<&'static str, f64>,
    intensifiers: HashMap<&'static str, f64>,
}

impl SentimentDictionary {
    fn new() -> Self {
        let mut positive = HashMap::new();
        let mut negative = HashMap::new();
        let mut intensifiers = HashMap::new();

        // 긍정 키워드 (금융/투자 관련)
        positive.insert("surge", 0.8);
        positive.insert("soar", 0.9);
        positive.insert("rally", 0.7);
        positive.insert("gain", 0.5);
        positive.insert("growth", 0.6);
        positive.insert("profit", 0.6);
        positive.insert("beat", 0.7);
        positive.insert("exceed", 0.6);
        positive.insert("record", 0.7);
        positive.insert("breakthrough", 0.8);
        positive.insert("innovation", 0.5);
        positive.insert("upgrade", 0.6);
        positive.insert("bullish", 0.8);
        positive.insert("outperform", 0.7);
        positive.insert("strong", 0.5);
        positive.insert("positive", 0.5);
        positive.insert("optimistic", 0.6);
        positive.insert("success", 0.7);
        positive.insert("expand", 0.5);
        positive.insert("increase", 0.4);
        positive.insert("rise", 0.5);
        positive.insert("up", 0.3);
        positive.insert("high", 0.3);
        positive.insert("buy", 0.4);
        positive.insert("recommend", 0.4);

        // 부정 키워드
        negative.insert("crash", -0.9);
        negative.insert("plunge", -0.8);
        negative.insert("tumble", -0.7);
        negative.insert("fall", -0.5);
        negative.insert("drop", -0.5);
        negative.insert("decline", -0.5);
        negative.insert("loss", -0.6);
        negative.insert("miss", -0.6);
        negative.insert("weak", -0.5);
        negative.insert("bearish", -0.8);
        negative.insert("underperform", -0.7);
        negative.insert("downgrade", -0.7);
        negative.insert("warning", -0.6);
        negative.insert("concern", -0.5);
        negative.insert("risk", -0.4);
        negative.insert("negative", -0.5);
        negative.insert("pessimistic", -0.6);
        negative.insert("fail", -0.7);
        negative.insert("bankruptcy", -0.9);
        negative.insert("lawsuit", -0.6);
        negative.insert("fraud", -0.9);
        negative.insert("scandal", -0.8);
        negative.insert("sell", -0.4);
        negative.insert("down", -0.3);
        negative.insert("low", -0.3);

        // 강조어
        intensifiers.insert("very", 1.5);
        intensifiers.insert("extremely", 2.0);
        intensifiers.insert("significantly", 1.5);
        intensifiers.insert("slightly", 0.5);
        intensifiers.insert("somewhat", 0.7);
        intensifiers.insert("dramatically", 1.8);
        intensifiers.insert("sharply", 1.5);
        intensifiers.insert("massive", 1.8);
        intensifiers.insert("huge", 1.6);

        Self {
            positive_words: positive,
            negative_words: negative,
            intensifiers,
        }
    }
}

// ============================================================
// 뉴스 분석 Actor
// ============================================================

/// 뉴스 분석 Actor
pub struct NewsAnalyzerActor {
    dictionary: SentimentDictionary,
    sentiments: HashMap<String, Vec<NewsSentiment>>,  // symbol -> sentiments
    summaries: HashMap<String, SymbolNewsSummary>,    // symbol -> summary
    trading_actor: Option<TradingActorRef>,
    config: AnalyzerConfig,
}

/// 분석기 설정
#[derive(Debug, Clone)]
pub struct AnalyzerConfig {
    pub min_confidence: f64,
    pub signal_threshold: f64,        // 신호 생성 임계값
    pub lookback_hours: i64,          // 분석 대상 기간
    pub min_articles_for_signal: usize,
}

impl Default for AnalyzerConfig {
    fn default() -> Self {
        Self {
            min_confidence: 0.5,
            signal_threshold: 0.3,
            lookback_hours: 24,
            min_articles_for_signal: 3,
        }
    }
}

impl NewsAnalyzerActor {
    pub fn new(config: AnalyzerConfig) -> Self {
        Self {
            dictionary: SentimentDictionary::new(),
            sentiments: HashMap::new(),
            summaries: HashMap::new(),
            trading_actor: None,
            config,
        }
    }

    /// 텍스트 감성 분석
    fn analyze_text(&self, text: &str) -> (f64, f64, Vec<String>) {
        let text_lower = text.to_lowercase();
        let words: Vec<&str> = text_lower.split_whitespace().collect();
        
        let mut score = 0.0;
        let mut word_count = 0;
        let mut key_phrases = Vec::new();
        let mut intensifier = 1.0;

        for (i, word) in words.iter().enumerate() {
            // 강조어 체크
            if let Some(&mult) = self.dictionary.intensifiers.get(word) {
                intensifier = mult;
                continue;
            }

            // 긍정어 체크
            if let Some(&word_score) = self.dictionary.positive_words.get(word) {
                let adjusted_score = word_score * intensifier;
                score += adjusted_score;
                word_count += 1;
                key_phrases.push(format!("+{}: {:.2}", word, adjusted_score));
                intensifier = 1.0;
            }

            // 부정어 체크
            if let Some(&word_score) = self.dictionary.negative_words.get(word) {
                let adjusted_score = word_score * intensifier;
                score += adjusted_score;
                word_count += 1;
                key_phrases.push(format!("-{}: {:.2}", word, adjusted_score));
                intensifier = 1.0;
            }
        }

        // 정규화
        let normalized_score = if word_count > 0 {
            (score / word_count as f64).clamp(-1.0, 1.0)
        } else {
            0.0
        };

        // 신뢰도 (분석된 단어 수 기반)
        let confidence = (word_count as f64 / 10.0).min(1.0);

        (normalized_score, confidence, key_phrases)
    }

    /// 단일 기사 분석
    fn analyze_article(&self, article: &NewsArticle) -> Vec<NewsSentiment> {
        let combined_text = format!("{} {} {}", article.title, article.summary, article.content);
        let (score, confidence, key_phrases) = self.analyze_text(&combined_text);

        // 제목에 더 높은 가중치 부여
        let (title_score, title_conf, _) = self.analyze_text(&article.title);
        let final_score = score * 0.6 + title_score * 0.4;
        let final_conf = confidence * 0.6 + title_conf * 0.4;

        // 각 관련 종목에 대해 감성 결과 생성
        let symbols = if article.symbols.is_empty() {
            vec!["MARKET".to_string()]
        } else {
            article.symbols.clone()
        };

        symbols.into_iter().map(|symbol| {
            NewsSentiment {
                article_id: article.id.clone(),
                symbol,
                sentiment_score: final_score,
                confidence: final_conf,
                sentiment_type: SentimentType::from_score(final_score),
                key_phrases: key_phrases.clone(),
                analyzed_at: Utc::now(),
            }
        }).collect()
    }

    /// 종목별 요약 생성
    fn generate_summary(&self, symbol: &str) -> Option<SymbolNewsSummary> {
        let sentiments = self.sentiments.get(symbol)?;
        if sentiments.is_empty() {
            return None;
        }

        let cutoff = Utc::now() - Duration::hours(self.config.lookback_hours);
        let recent: Vec<_> = sentiments.iter()
            .filter(|s| s.analyzed_at > cutoff)
            .collect();

        if recent.is_empty() {
            return None;
        }

        // 평균 감성 점수
        let avg_sentiment: f64 = recent.iter()
            .map(|s| s.sentiment_score)
            .sum::<f64>() / recent.len() as f64;

        // 추세 계산 (최근 vs 이전)
        let mid = recent.len() / 2;
        let (older, newer): (Vec<_>, Vec<_>) = recent.iter()
            .enumerate()
            .partition(|(i, _)| *i < mid);

        let older_avg = if older.is_empty() { 0.0 } else {
            older.iter().map(|(_, s)| s.sentiment_score).sum::<f64>() / older.len() as f64
        };
        let newer_avg = if newer.is_empty() { 0.0 } else {
            newer.iter().map(|(_, s)| s.sentiment_score).sum::<f64>() / newer.len() as f64
        };

        let trend = if newer_avg - older_avg > 0.1 {
            SentimentTrend::Improving
        } else if older_avg - newer_avg > 0.1 {
            SentimentTrend::Declining
        } else {
            SentimentTrend::Stable
        };

        // 추천 액션
        let action = self.recommend_action(avg_sentiment, &trend, recent.len());

        // 신뢰도 (기사 수 기반)
        let confidence = (recent.len() as f64 / 10.0).min(1.0) *
            recent.iter().map(|s| s.confidence).sum::<f64>() / recent.len() as f64;

        Some(SymbolNewsSummary {
            symbol: symbol.to_string(),
            article_count: recent.len(),
            avg_sentiment,
            sentiment_trend: trend,
            recommended_action: action,
            confidence,
            latest_headlines: Vec::new(), // TODO: 헤드라인 추가
            updated_at: Utc::now(),
        })
    }

    /// 추천 액션 결정
    fn recommend_action(&self, sentiment: f64, trend: &SentimentTrend, article_count: usize) -> RecommendedAction {
        // 충분한 데이터가 없으면 Hold
        if article_count < self.config.min_articles_for_signal {
            return RecommendedAction::Hold;
        }

        // 감성 + 추세 기반 결정
        match (sentiment, trend) {
            (s, SentimentTrend::Improving) if s > 0.3 => RecommendedAction::StrongBuy,
            (s, _) if s > 0.3 => RecommendedAction::Buy,
            (s, SentimentTrend::Declining) if s < -0.3 => RecommendedAction::StrongSell,
            (s, _) if s < -0.3 => RecommendedAction::Sell,
            _ => RecommendedAction::Hold,
        }
    }

    /// 투자 신호 생성 및 전달
    fn generate_and_send_signals(&self) {
        let trading_actor = match &self.trading_actor {
            Some(actor) => actor,
            None => {
                warn!("Trading Actor 미연결 - 신호 전달 불가");
                return;
            }
        };

        for (symbol, summary) in &self.summaries {
            if summary.confidence < self.config.min_confidence {
                continue;
            }

            let signal = match &summary.recommended_action {
                RecommendedAction::StrongBuy => Some(TradingSignal::Buy { strength: 0.9 }),
                RecommendedAction::Buy => Some(TradingSignal::Buy { strength: 0.7 }),
                RecommendedAction::StrongSell => Some(TradingSignal::Sell { strength: 0.9 }),
                RecommendedAction::Sell => Some(TradingSignal::Sell { strength: 0.7 }),
                RecommendedAction::Hold => None,
            };

            if let Some(signal) = signal {
                // 현재 가격을 가져오는 로직이 필요하지만, 여기서는 예시로 고정값 사용
                let estimated_price = 150.0; // TODO: 실시간 가격 조회
                
                let msg = PersistentTradingMessage::HandleSignal {
                    symbol: symbol.clone(),
                    signal,
                    current_price: estimated_price,
                };
                
                if let Err(e) = trading_actor.send(msg) {
                    error!("신호 전달 실패: {}", e);
                }
            }
        }
    }
}

#[async_trait]
impl Actor for NewsAnalyzerActor {
    type Message = NewsAnalyzerMessage;

    async fn receive(&mut self, message: Self::Message, _ctx: &mut ActorContext<Self>) {
        match message {
            NewsAnalyzerMessage::AnalyzeArticle { article } => {
                debug!("📊 기사 분석: {}", article.title);
                let sentiments = self.analyze_article(&article);
                
                for sentiment in sentiments {
                    self.sentiments
                        .entry(sentiment.symbol.clone())
                        .or_insert_with(Vec::new)
                        .push(sentiment);
                }
            }

            NewsAnalyzerMessage::AnalyzeArticles { articles } => {
                for article in articles {
                    let sentiments = self.analyze_article(&article);
                    for sentiment in sentiments {
                        self.sentiments
                            .entry(sentiment.symbol.clone())
                            .or_insert_with(Vec::new)
                            .push(sentiment);
                    }
                }
            }

            NewsAnalyzerMessage::GenerateSummary { symbol } => {
                if let Some(summary) = self.generate_summary(&symbol) {
                    self.summaries.insert(symbol, summary);
                } else {
                    warn!("📈 {} 요약 생성 실패 - 데이터 부족", symbol);
                }
            }

            NewsAnalyzerMessage::GenerateAllSummaries => {
                let symbols: Vec<_> = self.sentiments.keys().cloned().collect();
                
                for symbol in symbols {
                    if let Some(summary) = self.generate_summary(&symbol) {
                        self.summaries.insert(symbol, summary);
                    }
                }
            }

            NewsAnalyzerMessage::GenerateSignals => {
                // 먼저 모든 요약 업데이트
                let symbols: Vec<_> = self.sentiments.keys().cloned().collect();
                for symbol in symbols {
                    if let Some(summary) = self.generate_summary(&symbol) {
                        self.summaries.insert(symbol, summary);
                    }
                }
                
                // 신호 생성 및 전달
                self.generate_and_send_signals();
            }

            NewsAnalyzerMessage::GetSentimentFor { symbol } => {
                if let Some(summary) = self.summaries.get(&symbol) {
                } else {
                    warn!("📊 {} 감성 정보 없음", symbol);
                }
            }

            NewsAnalyzerMessage::ConnectTradingActor { actor_ref } => {
                self.trading_actor = Some(actor_ref);
            }
        }
    }

    async fn pre_start(&mut self) {
    }
}

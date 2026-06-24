//! 가격 예측 함수형 Actor
//!
//! LLM과 기술적 분석을 결합하여 주식 가격을 예측합니다.

use crate::llm::{LLMClient, PricePredictionRequest, InvestmentOpinionResult};
use crate::actors::functional::market_data_actor::{MarketDataState, PricePoint};
use crate::actors::functional::sentiment_actor::{SentimentState, SentimentScore};
use pekko_actor::{FunctionalActor, FunctionalActorContext, Effect};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// PredictionActor의 상태 (불변)
#[derive(Debug, Clone)]
pub struct PredictionState {
    /// 가격 예측 히스토리
    pub prediction_history: HashMap<String, Vec<PricePrediction>>,
    /// 투자 의견 히스토리
    pub investment_opinions: HashMap<String, Vec<InvestmentOpinion>>,
    /// 예측 모델 성능 지표
    pub model_performance: HashMap<String, ModelMetrics>,
    /// 예측 설정
    pub config: PredictionConfig,
    /// 마지막 업데이트 시간
    pub last_update: chrono::DateTime<chrono::Utc>,
}

/// 가격 예측 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PricePrediction {
    pub symbol: String,
    pub predicted_price: f64,
    pub confidence: f64,
    pub reasoning: String,
    pub time_horizon: String,
    pub prediction_type: PredictionType,
    pub technical_signals: Vec<String>,
    pub sentiment_impact: Option<f64>,
    pub actual_price: Option<f64>, // 예측 후 실제 가격 (백테스트용)
    pub accuracy_score: Option<f64>, // 예측 정확도 (백테스트용)
    pub predicted_at: chrono::DateTime<chrono::Utc>,
}

/// 투자 의견
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvestmentOpinion {
    pub symbol: String,
    pub recommendation: Recommendation,
    pub confidence: f64,
    pub reasoning: String,
    pub time_horizon: String,
    pub entry_price_range: Option<String>,
    pub stop_loss: Option<f64>,
    pub target_price: Option<f64>,
    pub risk_level: RiskLevel,
    pub generated_at: chrono::DateTime<chrono::Utc>,
}

/// 추천 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Recommendation {
    StrongBuy,
    Buy,
    Hold,
    Sell,
    StrongSell,
}

impl Recommendation {
    pub fn as_str(&self) -> &'static str {
        match self {
            Recommendation::StrongBuy => "강력매수",
            Recommendation::Buy => "매수",
            Recommendation::Hold => "보유",
            Recommendation::Sell => "매도",
            Recommendation::StrongSell => "강력매도",
        }
    }

    pub fn to_numeric(&self) -> i32 {
        match self {
            Recommendation::StrongBuy => 2,
            Recommendation::Buy => 1,
            Recommendation::Hold => 0,
            Recommendation::Sell => -1,
            Recommendation::StrongSell => -2,
        }
    }

    pub fn from_string(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "BUY" | "매수" => Some(Recommendation::Buy),
            "STRONG_BUY" | "강력매수" => Some(Recommendation::StrongBuy),
            "HOLD" | "보유" => Some(Recommendation::Hold),
            "SELL" | "매도" => Some(Recommendation::Sell),
            "STRONG_SELL" | "강력매도" => Some(Recommendation::StrongSell),
            _ => None,
        }
    }
}

/// 예측 타입
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PredictionType {
    ShortTerm,  // 1-3일
    MediumTerm, // 1주
    LongTerm,   // 1개월 이상
}

impl PredictionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            PredictionType::ShortTerm => "단기",
            PredictionType::MediumTerm => "중기",
            PredictionType::LongTerm => "장기",
        }
    }
}

/// 리스크 레벨
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    VeryHigh,
}

impl RiskLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            RiskLevel::Low => "낮음",
            RiskLevel::Medium => "중간",
            RiskLevel::High => "높음",
            RiskLevel::VeryHigh => "매우 높음",
        }
    }
}

/// 모델 성능 지표
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelMetrics {
    pub symbol: String,
    pub total_predictions: u32,
    pub accurate_predictions: u32,
    pub avg_accuracy: f64,
    pub avg_confidence: f64,
    pub last_updated: chrono::DateTime<chrono::Utc>,
}

/// 예측 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictionConfig {
    pub min_confidence_threshold: f64,
    pub max_prediction_history: usize,
    pub enable_sentiment_integration: bool,
    pub enable_technical_analysis: bool,
    pub risk_adjustment_factor: f64,
    pub backtest_enabled: bool,
    pub prediction_horizons: Vec<String>,
}

impl Default for PredictionConfig {
    fn default() -> Self {
        Self {
            min_confidence_threshold: 0.6,
            max_prediction_history: 100,
            enable_sentiment_integration: true,
            enable_technical_analysis: true,
            risk_adjustment_factor: 1.0,
            backtest_enabled: true,
            prediction_horizons: vec![
                "1d".to_string(),
                "3d".to_string(),
                "1w".to_string(),
                "1m".to_string(),
            ],
        }
    }
}

impl PredictionState {
    pub fn new() -> Self {
        Self {
            prediction_history: HashMap::new(),
            investment_opinions: HashMap::new(),
            model_performance: HashMap::new(),
            config: PredictionConfig::default(),
            last_update: chrono::Utc::now(),
        }
    }

    /// 가격 예측 추가 (불변 방식)
    pub fn add_prediction(mut self, prediction: PricePrediction) -> Self {
        let symbol = prediction.symbol.clone();
        let history = self.prediction_history.entry(symbol.clone()).or_insert_with(Vec::new);

        // 최대 히스토리 크기 제한
        if history.len() >= self.config.max_prediction_history {
            history.remove(0);
        }

        history.push(prediction);

        // 모델 성능 업데이트
        self = self.update_model_performance(symbol);

        self.last_update = chrono::Utc::now();
        self
    }

    /// 투자 의견 추가
    pub fn add_investment_opinion(mut self, opinion: InvestmentOpinion) -> Self {
        let symbol = opinion.symbol.clone();
        let opinions = self.investment_opinions.entry(symbol).or_insert_with(Vec::new);

        // 최대 10개 의견 유지
        if opinions.len() >= 10 {
            opinions.remove(0);
        }

        opinions.push(opinion);
        self.last_update = chrono::Utc::now();
        self
    }

    /// 모델 성능 업데이트
    pub fn update_model_performance(mut self, symbol: String) -> Self {
        if let Some(predictions) = self.prediction_history.get(&symbol) {
            let total_predictions = predictions.len() as u32;
            let accurate_predictions = predictions
                .iter()
                .filter(|p| p.accuracy_score.map(|acc| acc > 0.8).unwrap_or(false))
                .count() as u32;

            let avg_accuracy = predictions
                .iter()
                .filter_map(|p| p.accuracy_score)
                .sum::<f64>() / predictions.len() as f64;

            let avg_confidence = predictions
                .iter()
                .map(|p| p.confidence)
                .sum::<f64>() / predictions.len() as f64;

            let metrics = ModelMetrics {
                symbol: symbol.clone(),
                total_predictions,
                accurate_predictions,
                avg_accuracy: avg_accuracy.max(0.0),
                avg_confidence,
                last_updated: chrono::Utc::now(),
            };

            self.model_performance.insert(symbol, metrics);
        }

        self
    }

    /// 특정 심볼의 최근 예측 조회
    pub fn get_recent_prediction(&self, symbol: &str, prediction_type: &PredictionType) -> Option<&PricePrediction> {
        self.prediction_history.get(symbol)?
            .iter()
            .rev()
            .find(|p| &p.prediction_type == prediction_type)
    }

    /// 특정 심볼의 투자 의견 조회
    pub fn get_latest_opinion(&self, symbol: &str) -> Option<&InvestmentOpinion> {
        self.investment_opinions.get(symbol)?
            .last()
    }

    /// 모델 성능 조회
    pub fn get_model_performance(&self, symbol: &str) -> Option<&ModelMetrics> {
        self.model_performance.get(symbol)
    }

    /// 예측 정확도 통계
    pub fn get_prediction_stats(&self, symbol: Option<&str>, days: u64) -> PredictionStats {
        let cutoff = chrono::Utc::now() - chrono::Duration::days(days as i64);

        let predictions: Vec<&PricePrediction> = if let Some(sym) = symbol {
            self.prediction_history.get(sym)
                .map(|preds| preds.iter().filter(|p| p.predicted_at > cutoff).collect())
                .unwrap_or_default()
        } else {
            self.prediction_history.values()
                .flat_map(|preds| preds.iter().filter(|p| p.predicted_at > cutoff))
                .collect()
        };

        let total_predictions = predictions.len();
        let accurate_predictions = predictions
            .iter()
            .filter(|p| p.accuracy_score.map(|acc| acc > 0.8).unwrap_or(false))
            .count();

        let avg_accuracy = if total_predictions > 0 {
            predictions.iter()
                .filter_map(|p| p.accuracy_score)
                .sum::<f64>() / total_predictions as f64
        } else {
            0.0
        };

        let avg_confidence = if total_predictions > 0 {
            predictions.iter().map(|p| p.confidence).sum::<f64>() / total_predictions as f64
        } else {
            0.0
        };

        PredictionStats {
            symbol: symbol.map(|s| s.to_string()),
            total_predictions,
            accurate_predictions,
            accuracy_rate: if total_predictions > 0 {
                accurate_predictions as f64 / total_predictions as f64
            } else {
                0.0
            },
            avg_accuracy,
            avg_confidence,
            period_days: days,
            generated_at: chrono::Utc::now(),
        }
    }
}

/// 예측 통계
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictionStats {
    pub symbol: Option<String>,
    pub total_predictions: usize,
    pub accurate_predictions: usize,
    pub accuracy_rate: f64,
    pub avg_accuracy: f64,
    pub avg_confidence: f64,
    pub period_days: u64,
    pub generated_at: chrono::DateTime<chrono::Utc>,
}

/// PredictionActor 메시지
#[derive(Debug, Clone)]
pub enum PredictionMessage {
    /// 가격 예측 요청
    PredictPrice {
        symbol: String,
        prediction_type: PredictionType,
        time_horizon: String,
    },

    /// 투자 의견 생성 요청
    GenerateInvestmentOpinion {
        symbol: String,
        time_horizon: String,
    },

    /// 예측 결과 조회
    GetPrediction {
        symbol: String,
        prediction_type: PredictionType,
    },

    /// 투자 의견 조회
    GetInvestmentOpinion {
        symbol: String,
    },

    /// 모델 성능 조회
    GetModelPerformance {
        symbol: String,
    },

    /// 예측 통계 조회
    GetPredictionStats {
        symbol: Option<String>,
        days: Option<u64>,
    },

    /// 실제 가격으로 예측 검증 (백테스트)
    ValidatePrediction {
        symbol: String,
        prediction_id: String,
        actual_price: f64,
    },

    /// 예측 모델 재학습
    RetrainModel {
        symbol: String,
    },

    /// 설정 업데이트
    UpdateConfig {
        config: PredictionConfig,
    },
}

/// 가격 예측 함수형 Actor
pub struct PredictionActor {
    llm_client: std::sync::Arc<dyn LLMClient>,
}

impl PredictionActor {
    pub fn new(llm_client: std::sync::Arc<dyn LLMClient>) -> Self {
        Self { llm_client }
    }

    /// 환경 변수에서 LLM 클라이언트 생성
    pub fn new_with_default_client() -> Result<Self, Box<dyn std::error::Error>> {
        let client = crate::llm::create_default_client()?;
        Ok(Self::new(std::sync::Arc::from(client)))
    }

    /// 모의 클라이언트로 생성
    pub fn new_with_mock_client() -> Self {
        let client = crate::llm::create_test_client();
        Self::new(std::sync::Arc::from(client))
    }

    /// 기술적 분석 지표 계산 (간단 버전)
    fn calculate_technical_signals(&self, symbol: &str, price_history: &[PricePoint]) -> Vec<String> {
        let mut signals = Vec::new();

        if price_history.len() < 2 {
            return signals;
        }

        let latest = &price_history[price_history.len() - 1];
        let previous = &price_history[price_history.len() - 2];

        // 가격 추세
        if latest.price > previous.price {
            signals.push("📈 가격 상승 추세".to_string());
        } else if latest.price < previous.price {
            signals.push("📉 가격 하락 추세".to_string());
        } else {
            signals.push("➡️ 가격 횡보".to_string());
        }

        // 거래량 분석
        if latest.volume > (previous.volume as f64 * 1.5) as u64 {
            signals.push("📊 거래량 급증".to_string());
        } else if latest.volume < (previous.volume as f64 * 0.7) as u64 {
            signals.push("📊 거래량 감소".to_string());
        }

        // 변동성 분석
        let change_percent = ((latest.price - previous.price) / previous.price * 100.0).abs();
        if change_percent > 5.0 {
            signals.push(format!("⚡ 고변동성 (변동률: {:.1}%)", change_percent));
        } else if change_percent < 1.0 {
            signals.push("🔹 저변동성".to_string());
        }

        signals
    }

    /// 감정 점수를 예측에 반영
    fn adjust_prediction_with_sentiment(&self, base_prediction: f64, sentiment: Option<&SentimentScore>) -> f64 {
        if let Some(sentiment) = sentiment {
            // 긍정적 감정이 높으면 가격 예측 상향, 부정적이면 하향 조정
            let sentiment_adjustment = (sentiment.positive - sentiment.negative) * 0.02; // ±2% 조정
            base_prediction * (1.0 + sentiment_adjustment)
        } else {
            base_prediction
        }
    }

    /// 예측 신뢰도 계산
    fn calculate_prediction_confidence(&self, technical_signals: &[String], sentiment_score: Option<f64>) -> f64 {
        let mut confidence = 0.5; // 기본 신뢰도

        // 기술적 신호 기반 조정
        confidence += (technical_signals.len() as f64) * 0.05; // 신호당 +5%

        // 감정 분석 반영
        if let Some(sentiment) = sentiment_score {
            confidence += sentiment * 0.1; // 감정 점수에 따라 ±10%
        }

        confidence.clamp(0.0, 1.0)
    }

    /// 투자 의견 생성 (기술적 + 감정 분석 기반)
    fn generate_investment_recommendation(
        &self,
        _symbol: &str,
        predicted_price: f64,
        current_price: f64,
        confidence: f64,
        technical_signals: &[String],
        sentiment: Option<&SentimentScore>,
    ) -> Recommendation {
        let price_change_percent = ((predicted_price - current_price) / current_price) * 100.0;

        // 기본 추천 로직
        let mut recommendation_score = 0.0;

        // 가격 변화 기반
        if price_change_percent > 10.0 {
            recommendation_score += 2.0; // 강력 매수
        } else if price_change_percent > 5.0 {
            recommendation_score += 1.0; // 매수
        } else if price_change_percent > -5.0 {
            recommendation_score += 0.0; // 보유
        } else if price_change_percent > -10.0 {
            recommendation_score -= 1.0; // 매도
        } else {
            recommendation_score -= 2.0; // 강력 매도
        }

        // 신뢰도 기반 조정
        recommendation_score *= confidence;

        // 기술적 신호 기반 조정
        let bullish_signals = technical_signals.iter()
            .filter(|s| s.contains("상승") || s.contains("📈"))
            .count();
        let bearish_signals = technical_signals.iter()
            .filter(|s| s.contains("하락") || s.contains("📉"))
            .count();

        recommendation_score += (bullish_signals as f64 - bearish_signals as f64) * 0.3;

        // 감정 분석 반영
        if let Some(sentiment) = sentiment {
            recommendation_score += (sentiment.positive - sentiment.negative) * 0.5;
        }

        // 최종 추천 결정
        if recommendation_score > 1.5 {
            Recommendation::StrongBuy
        } else if recommendation_score > 0.5 {
            Recommendation::Buy
        } else if recommendation_score > -0.5 {
            Recommendation::Hold
        } else if recommendation_score > -1.5 {
            Recommendation::Sell
        } else {
            Recommendation::StrongSell
        }
    }

    /// 리스크 레벨 평가
    fn assess_risk_level(&self, _symbol: &str, volatility: f64, confidence: f64) -> RiskLevel {
        let volatility_score = if volatility > 0.05 { 2 } else if volatility > 0.02 { 1 } else { 0 };
        let confidence_score = if confidence < 0.5 { 2 } else if confidence < 0.7 { 1 } else { 0 };

        match volatility_score + confidence_score {
            0 => RiskLevel::Low,
            1 => RiskLevel::Medium,
            2 => RiskLevel::High,
            _ => RiskLevel::VeryHigh,
        }
    }
}

impl FunctionalActor for PredictionActor {
    type State = PredictionState;
    type Message = PredictionMessage;

    fn init(_ctx: &FunctionalActorContext) -> Self::State {
        PredictionState::new()
    }

    fn receive(
        state: Self::State,
        msg: Self::Message,
        _ctx: &FunctionalActorContext,
    ) -> (Self::State, Effect) {
        match msg {
            PredictionMessage::PredictPrice { symbol, prediction_type, time_horizon } => {
                (state, Effect::None) // 실제로는 LLM API 호출 Effect
            }

            PredictionMessage::GenerateInvestmentOpinion { symbol, time_horizon } => {
                (state, Effect::None) // 실제로는 LLM API 호출 Effect
            }

            PredictionMessage::GetPrediction { symbol, prediction_type } => {
                if let Some(prediction) = state.get_recent_prediction(&symbol, &prediction_type) {
                } else {
                    tracing::warn!("{} 심볼의 {} 예측이 없습니다", symbol, prediction_type.as_str());
                }
                (state, Effect::None)
            }

            PredictionMessage::GetInvestmentOpinion { symbol } => {
                if let Some(opinion) = state.get_latest_opinion(&symbol) {
                } else {
                    tracing::warn!("{} 심볼의 투자 의견이 없습니다", symbol);
                }
                (state, Effect::None)
            }

            PredictionMessage::GetModelPerformance { symbol } => {
                if let Some(metrics) = state.get_model_performance(&symbol) {
                } else {
                    tracing::warn!("{} 심볼의 모델 성능 데이터가 없습니다", symbol);
                }
                (state, Effect::None)
            }

            PredictionMessage::GetPredictionStats { symbol, days } => {
                let days = days.unwrap_or(7);
                let stats = state.get_prediction_stats(symbol.as_deref(), days);
                let symbol_str = symbol.as_ref().map(|s| s.as_str()).unwrap_or("전체");
                (state, Effect::None)
            }

            PredictionMessage::ValidatePrediction { symbol, prediction_id, actual_price } => {
                // 예측 검증 (실제로는 prediction_id로 특정 예측을 찾아서 검증)
                (state, Effect::None)
            }

            PredictionMessage::RetrainModel { symbol } => {
                (state, Effect::None) // 실제로는 모델 재학습 Effect
            }

            PredictionMessage::UpdateConfig { config } => {
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
            tracing::debug!("PredictionActor Effect 적용");
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recommendation_as_str() {
        assert_eq!(Recommendation::StrongBuy.as_str(), "강력매수");
        assert_eq!(Recommendation::Buy.as_str(), "매수");
        assert_eq!(Recommendation::Hold.as_str(), "보유");
        assert_eq!(Recommendation::Sell.as_str(), "매도");
        assert_eq!(Recommendation::StrongSell.as_str(), "강력매도");
    }

    #[test]
    fn test_recommendation_to_numeric() {
        assert_eq!(Recommendation::StrongBuy.to_numeric(), 2);
        assert_eq!(Recommendation::Buy.to_numeric(), 1);
        assert_eq!(Recommendation::Hold.to_numeric(), 0);
        assert_eq!(Recommendation::Sell.to_numeric(), -1);
        assert_eq!(Recommendation::StrongSell.to_numeric(), -2);
    }

    #[test]
    fn test_recommendation_from_string() {
        assert_eq!(Recommendation::from_string("BUY").unwrap(), Recommendation::Buy);
        assert_eq!(Recommendation::from_string("매수").unwrap(), Recommendation::Buy);
        assert_eq!(Recommendation::from_string("HOLD").unwrap(), Recommendation::Hold);
        assert_eq!(Recommendation::from_string("invalid"), None);
    }

    #[test]
    fn test_prediction_type_as_str() {
        assert_eq!(PredictionType::ShortTerm.as_str(), "단기");
        assert_eq!(PredictionType::MediumTerm.as_str(), "중기");
        assert_eq!(PredictionType::LongTerm.as_str(), "장기");
    }

    #[test]
    fn test_state_add_prediction() {
        let state = PredictionState::new();
        let prediction = PricePrediction {
            symbol: "AAPL".to_string(),
            predicted_price: 150.0,
            confidence: 0.8,
            reasoning: "기술적 분석 기반".to_string(),
            time_horizon: "1d".to_string(),
            prediction_type: PredictionType::ShortTerm,
            technical_signals: vec!["가격 상승 추세".to_string()],
            sentiment_impact: Some(0.1),
            actual_price: None,
            accuracy_score: None,
            predicted_at: chrono::Utc::now(),
        };

        let new_state = state.add_prediction(prediction);

        assert_eq!(new_state.prediction_history.get("AAPL").unwrap().len(), 1);
        assert!(state.prediction_history.get("AAPL").is_none()); // 불변성 확인
    }

    #[test]
    fn test_state_get_recent_prediction() {
        let state = PredictionState::new();
        let prediction = PricePrediction {
            symbol: "AAPL".to_string(),
            predicted_price: 150.0,
            confidence: 0.8,
            reasoning: "테스트".to_string(),
            time_horizon: "1d".to_string(),
            prediction_type: PredictionType::ShortTerm,
            technical_signals: vec![],
            sentiment_impact: None,
            actual_price: None,
            accuracy_score: None,
            predicted_at: chrono::Utc::now(),
        };

        let state_with_prediction = state.add_prediction(prediction);
        let retrieved = state_with_prediction.get_recent_prediction("AAPL", &PredictionType::ShortTerm);

        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().predicted_price, 150.0);
    }

    #[test]
    fn test_generate_investment_recommendation() {
        let actor = PredictionActor::new_with_mock_client();
        let technical_signals = vec!["📈 가격 상승 추세".to_string()];
        let sentiment = SentimentScore {
            positive: 0.7,
            negative: 0.1,
            neutral: 0.2,
            overall: 0.6,
        };

        let recommendation = actor.generate_investment_recommendation(
            "AAPL",
            160.0, // 예측 가격
            150.0, // 현재 가격 (+6.7% 상승 예상)
            0.8,   // 신뢰도
            &technical_signals,
            Some(&sentiment),
        );

        // 긍정적 요인들이 많으므로 매수 추천 예상
        assert!(matches!(recommendation, Recommendation::Buy | Recommendation::StrongBuy));
    }
}
//! LLM 분석 함수형 Actor
//!
//! LLM을 사용하여 시장 데이터와 뉴스를 분석하고 투자 인사이트를 제공합니다.

use crate::llm::{LLMClient, MarketAnalysisRequest, InvestmentInsightResponse};
use crate::actors::functional::market_data_actor::MarketDataState;
use crate::actors::functional::sentiment_actor::SentimentState;
use pekko_actor::{FunctionalActor, FunctionalActorContext, Effect};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// LLM 분석 상태 (불변)
#[derive(Debug, Clone)]
pub struct LLMAnalysisState {
    /// 심볼별 분석 히스토리
    pub analysis_history: HashMap<String, Vec<MarketAnalysis>>,
    /// 투자 인사이트 히스토리
    pub insight_history: HashMap<String, Vec<InvestmentInsight>>,
    /// 분석 모델 성능
    pub model_performance: HashMap<String, AnalysisMetrics>,
    /// LLM 분석 설정
    pub config: LLMAnalysisConfig,
    /// 마지막 업데이트 시간
    pub last_update: chrono::DateTime<chrono::Utc>,
}

/// 시장 분석 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketAnalysis {
    pub symbol: String,
    pub analysis_type: AnalysisType,
    pub content: String,
    pub confidence: f64,
    pub key_insights: Vec<String>,
    pub recommendations: Vec<String>,
    pub risk_assessment: RiskLevel,
    pub time_horizon: String,
    pub analyzed_at: chrono::DateTime<chrono::Utc>,
}

/// 투자 인사이트
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvestmentInsight {
    pub symbol: String,
    pub insight_type: InsightType,
    pub title: String,
    pub description: String,
    pub confidence: f64,
    pub supporting_data: Vec<String>,
    pub action_items: Vec<String>,
    pub generated_at: chrono::DateTime<chrono::Utc>,
}

/// 분석 타입
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AnalysisType {
    Technical,
    Fundamental,
    Sentiment,
    Combined,
    RiskAssessment,
}

/// 인사이트 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InsightType {
    Bullish,
    Bearish,
    Neutral,
    HighRisk,
    Opportunity,
    Warning,
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

/// 분석 메트릭
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisMetrics {
    pub symbol: String,
    pub total_analyses: u32,
    pub avg_confidence: f64,
    pub accuracy_score: Option<f64>,
    pub response_time_avg: f64,
    pub last_updated: chrono::DateTime<chrono::Utc>,
}

/// LLM 분석 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LLMAnalysisConfig {
    pub min_confidence_threshold: f64,
    pub max_history_size: usize,
    pub enable_technical_analysis: bool,
    pub enable_fundamental_analysis: bool,
    pub enable_sentiment_analysis: bool,
    pub max_tokens_per_request: u32,
    pub analysis_timeout_seconds: u64,
    pub cache_enabled: bool,
    pub cache_ttl_hours: u64,
}

impl Default for LLMAnalysisConfig {
    fn default() -> Self {
        Self {
            min_confidence_threshold: 0.7,
            max_history_size: 100,
            enable_technical_analysis: true,
            enable_fundamental_analysis: true,
            enable_sentiment_analysis: true,
            max_tokens_per_request: 2000,
            analysis_timeout_seconds: 30,
            cache_enabled: true,
            cache_ttl_hours: 24,
        }
    }
}

impl LLMAnalysisState {
    pub fn new() -> Self {
        Self {
            analysis_history: HashMap::new(),
            insight_history: HashMap::new(),
            model_performance: HashMap::new(),
            config: LLMAnalysisConfig::default(),
            last_update: chrono::Utc::now(),
        }
    }

    /// 시장 분석 추가 (불변 방식)
    pub fn add_analysis(mut self, analysis: MarketAnalysis) -> Self {
        let symbol = analysis.symbol.clone();
        let history = self.analysis_history.entry(symbol.clone()).or_insert_with(Vec::new);

        // 최대 히스토리 크기 제한
        if history.len() >= self.config.max_history_size {
            history.remove(0);
        }

        history.push(analysis);

        // 모델 성능 업데이트
        self = self.update_model_performance(symbol);

        self.last_update = chrono::Utc::now();
        self
    }

    /// 투자 인사이트 추가
    pub fn add_insight(mut self, insight: InvestmentInsight) -> Self {
        let symbol = insight.symbol.clone();
        let insights = self.insight_history.entry(symbol).or_insert_with(Vec::new);

        // 최대 20개 인사이트 유지
        if insights.len() >= 20 {
            insights.remove(0);
        }

        insights.push(insight);
        self.last_update = chrono::Utc::now();
        self
    }

    /// 모델 성능 업데이트
    pub fn update_model_performance(mut self, symbol: String) -> Self {
        if let Some(analyses) = self.analysis_history.get(&symbol) {
            let total_analyses = analyses.len() as u32;
            let avg_confidence = analyses.iter().map(|a| a.confidence).sum::<f64>() / total_analyses as f64;

            // 평균 응답 시간 계산 (단순 구현)
            let response_time_avg = 5.0; // 초 단위 (실제로는 측정 필요)

            let metrics = AnalysisMetrics {
                symbol: symbol.clone(),
                total_analyses,
                avg_confidence,
                accuracy_score: None, // 실제로는 백테스트로 계산
                response_time_avg,
                last_updated: chrono::Utc::now(),
            };

            self.model_performance.insert(symbol, metrics);
        }

        self
    }

    /// 특정 심볼의 최근 분석 조회
    pub fn get_recent_analysis(&self, symbol: &str, analysis_type: &AnalysisType) -> Option<&MarketAnalysis> {
        self.analysis_history.get(symbol)?
            .iter()
            .rev()
            .find(|a| &a.analysis_type == analysis_type)
    }

    /// 특정 심볼의 최근 인사이트 조회
    pub fn get_recent_insights(&self, symbol: &str, limit: usize) -> Vec<&InvestmentInsight> {
        if let Some(insights) = self.insight_history.get(symbol) {
            insights.iter().rev().take(limit).collect()
        } else {
            Vec::new()
        }
    }

    /// 모델 성능 조회
    pub fn get_model_performance(&self, symbol: &str) -> Option<&AnalysisMetrics> {
        self.model_performance.get(symbol)
    }

    /// 분석 품질 점수 계산
    pub fn calculate_analysis_quality(&self, symbol: &str) -> Option<f64> {
        let analyses = self.analysis_history.get(symbol)?;
        if analyses.is_empty() {
            return None;
        }

        let total_confidence: f64 = analyses.iter().map(|a| a.confidence).sum();
        let avg_confidence = total_confidence / analyses.len() as f64;

        let rec_count: f64 = analyses.iter()
            .map(|a| a.recommendations.len() as f64)
            .sum();

        let insight_count: f64 = analyses.iter()
            .map(|a| a.key_insights.len() as f64)
            .sum();

        // 품질 점수 = 신뢰도 * (추천 수 + 인사이트 수) / 분석 수
        Some(avg_confidence * (rec_count + insight_count) / analyses.len() as f64)
    }
}

/// LLM 분석 메시지
#[derive(Debug, Clone)]
pub enum LLMAnalysisMessage {
    /// 시장 분석 요청
    AnalyzeMarket {
        symbol: String,
        analysis_type: AnalysisType,
        time_horizon: String,
        include_technical: bool,
        include_fundamental: bool,
        include_sentiment: bool,
    },

    /// 투자 인사이트 생성 요청
    GenerateInsight {
        symbol: String,
        insight_type: InsightType,
        context_data: Vec<String>,
    },

    /// 분석 결과 조회
    GetAnalysis {
        symbol: String,
        analysis_type: AnalysisType,
    },

    /// 투자 인사이트 조회
    GetInsights {
        symbol: String,
        limit: Option<usize>,
    },

    /// 모델 성능 조회
    GetModelPerformance {
        symbol: String,
    },

    /// 분석 품질 평가
    EvaluateAnalysisQuality {
        symbol: String,
    },

    /// 캐시 정리
    ClearCache {
        older_than_hours: u64,
    },

    /// 설정 업데이트
    UpdateConfig {
        config: LLMAnalysisConfig,
    },
}

/// LLM 분석 함수형 Actor
pub struct LLMAnalysisActor {
    llm_client: std::sync::Arc<dyn LLMClient>,
}

impl LLMAnalysisActor {
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

    /// 분석 타입을 문자열로 변환 (정적 함수)
    fn analysis_type_to_string(analysis_type: &AnalysisType) -> &'static str {
        match analysis_type {
            AnalysisType::Technical => "기술적 분석",
            AnalysisType::Fundamental => "기본적 분석",
            AnalysisType::Sentiment => "감정 분석",
            AnalysisType::Combined => "종합 분석",
            AnalysisType::RiskAssessment => "리스크 평가",
        }
    }

    /// 인사이트 타입을 문자열로 변환 (정적 함수)
    fn insight_type_to_string(insight_type: &InsightType) -> &'static str {
        match insight_type {
            InsightType::Bullish => "강세",
            InsightType::Bearish => "약세",
            InsightType::Neutral => "중립",
            InsightType::HighRisk => "고위험",
            InsightType::Opportunity => "기회",
            InsightType::Warning => "경고",
        }
    }
}

impl FunctionalActor for LLMAnalysisActor {
    type State = LLMAnalysisState;
    type Message = LLMAnalysisMessage;

    fn init(_ctx: &FunctionalActorContext) -> Self::State {
        LLMAnalysisState::new()
    }

    fn receive(
        state: Self::State,
        msg: Self::Message,
        _ctx: &FunctionalActorContext,
    ) -> (Self::State, Effect) {
        match msg {
            LLMAnalysisMessage::AnalyzeMarket {
                symbol,
                analysis_type,
                time_horizon,
                include_technical,
                include_fundamental,
                include_sentiment,
            } => {
                // 실제로는 LLM API 호출 Effect
                (state, Effect::None)
            }

            LLMAnalysisMessage::GenerateInsight { symbol, insight_type, context_data } => {
                // 실제로는 LLM API 호출 Effect
                (state, Effect::None)
            }

            LLMAnalysisMessage::GetAnalysis { symbol, analysis_type } => {
                if let Some(analysis) = state.get_recent_analysis(&symbol, &analysis_type) {
                } else {
                    tracing::warn!("{} 심볼의 {} 분석 데이터가 없습니다", symbol, Self::analysis_type_to_string(&analysis_type));
                }
                (state, Effect::None)
            }

            LLMAnalysisMessage::GetInsights { symbol, limit } => {
                let limit = limit.unwrap_or(5);
                let insights = state.get_recent_insights(&symbol, limit);

                if insights.is_empty() {
                    tracing::warn!("{} 심볼의 투자 인사이트가 없습니다", symbol);
                } else {
                    for (i, insight) in insights.iter().enumerate() {
                    }
                }
                (state, Effect::None)
            }

            LLMAnalysisMessage::GetModelPerformance { symbol } => {
                if let Some(metrics) = state.get_model_performance(&symbol) {
                } else {
                    tracing::warn!("{} 심볼의 모델 성능 데이터가 없습니다", symbol);
                }
                (state, Effect::None)
            }

            LLMAnalysisMessage::EvaluateAnalysisQuality { symbol } => {
                if let Some(quality_score) = state.calculate_analysis_quality(&symbol) {
                } else {
                    tracing::warn!("{} 심볼의 분석 품질을 평가할 데이터가 부족합니다", symbol);
                }
                (state, Effect::None)
            }

            LLMAnalysisMessage::ClearCache { older_than_hours } => {
                // 실제로는 캐시 정리 로직 구현
                (state, Effect::None)
            }

            LLMAnalysisMessage::UpdateConfig { config } => {
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
            tracing::debug!("LLMAnalysisActor Effect 적용");
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_analysis() {
        let state = LLMAnalysisState::new();

        let analysis = MarketAnalysis {
            symbol: "AAPL".to_string(),
            analysis_type: AnalysisType::Technical,
            content: "기술적 분석 결과".to_string(),
            confidence: 0.85,
            key_insights: vec!["상승 추세".to_string()],
            recommendations: vec!["매수 유지".to_string()],
            risk_assessment: RiskLevel::Medium,
            time_horizon: "1w".to_string(),
            analyzed_at: chrono::Utc::now(),
        };

        let new_state = state.add_analysis(analysis);

        assert!(new_state.analysis_history.contains_key("AAPL"));
        assert_eq!(new_state.analysis_history["AAPL"].len(), 1);
    }

    #[test]
    fn test_get_recent_analysis() {
        let state = LLMAnalysisState::new();

        let analysis = MarketAnalysis {
            symbol: "AAPL".to_string(),
            analysis_type: AnalysisType::Technical,
            content: "테스트 분석".to_string(),
            confidence: 0.8,
            key_insights: vec![],
            recommendations: vec![],
            risk_assessment: RiskLevel::Low,
            time_horizon: "1d".to_string(),
            analyzed_at: chrono::Utc::now(),
        };

        let state_with_analysis = state.add_analysis(analysis);
        let retrieved = state_with_analysis.get_recent_analysis("AAPL", &AnalysisType::Technical);

        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().confidence, 0.8);
    }

    #[test]
    fn test_add_insight() {
        let state = LLMAnalysisState::new();

        let insight = InvestmentInsight {
            symbol: "AAPL".to_string(),
            insight_type: InsightType::Bullish,
            title: "강세 신호".to_string(),
            description: "기술적 지표가 호재".to_string(),
            confidence: 0.9,
            supporting_data: vec!["SMA 골든크로스".to_string()],
            action_items: vec!["포지션 확대 고려".to_string()],
            generated_at: chrono::Utc::now(),
        };

        let new_state = state.add_insight(insight);

        assert!(new_state.insight_history.contains_key("AAPL"));
        assert_eq!(new_state.insight_history["AAPL"].len(), 1);
    }

    #[test]
    fn test_risk_level_as_str() {
        assert_eq!(RiskLevel::Low.as_str(), "낮음");
        assert_eq!(RiskLevel::High.as_str(), "높음");
    }
}
//! 리포트 생성 함수형 Actor
//!
//! 시장 데이터와 LLM을 활용하여 자동으로 투자 리포트를 생성합니다.

use crate::llm::{LLMClient, MarketReportRequest, LLMAnalysisResult};
use crate::actors::functional::market_data_actor::{MarketDataState, PricePoint};
use crate::actors::functional::sentiment_actor::SentimentState;
use pekko_actor::{FunctionalActor, FunctionalActorContext, Effect};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// ReportGenerationActor의 상태 (불변)
#[derive(Debug, Clone)]
pub struct ReportState {
    /// 생성된 리포트 히스토리
    pub report_history: Vec<MarketReport>,
    /// 리포트 버전 관리
    pub report_versions: HashMap<String, Vec<ReportVersion>>,
    /// 최근 시장 데이터 스냅샷
    pub market_snapshot: Option<MarketSnapshot>,
    /// 리포트 생성 설정
    pub config: ReportConfig,
    /// 마지막 업데이트 시간
    pub last_update: chrono::DateTime<chrono::Utc>,
}

/// 시장 리포트
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketReport {
    pub id: String,
    pub title: String,
    pub report_type: ReportType,
    pub time_period: String,
    pub symbols: Vec<String>,
    pub content: String,
    pub summary: String,
    pub key_insights: Vec<String>,
    pub recommendations: Vec<String>,
    pub risk_warnings: Vec<String>,
    pub confidence_score: f64,
    pub generated_at: chrono::DateTime<chrono::Utc>,
    pub data_sources: Vec<String>,
}

/// 리포트 타입
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReportType {
    DailyMarketReport,
    WeeklyMarketReport,
    SectorAnalysis,
    StockSpecificReport,
    RiskAssessment,
    OpportunityAnalysis,
}

/// 리포트 버전 정보
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportVersion {
    pub version: u32,
    pub report: MarketReport,
    pub changes_summary: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// 시장 데이터 스냅샷
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub price_data: HashMap<String, PricePoint>,
    pub market_indices: HashMap<String, f64>,
    pub volume_data: HashMap<String, u64>,
    pub volatility_measures: HashMap<String, f64>,
}

/// 리포트 생성 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportConfig {
    pub auto_generate_daily: bool,
    pub auto_generate_weekly: bool,
    pub min_data_points: usize,
    pub max_report_age_hours: u64,
    pub include_sentiment_analysis: bool,
    pub include_technical_analysis: bool,
    pub include_fundamental_analysis: bool,
    pub risk_tolerance: RiskTolerance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RiskTolerance {
    Conservative,
    Moderate,
    Aggressive,
}

impl Default for ReportConfig {
    fn default() -> Self {
        Self {
            auto_generate_daily: true,
            auto_generate_weekly: true,
            min_data_points: 10,
            max_report_age_hours: 24,
            include_sentiment_analysis: true,
            include_technical_analysis: true,
            include_fundamental_analysis: false,
            risk_tolerance: RiskTolerance::Moderate,
        }
    }
}

impl ReportState {
    pub fn new() -> Self {
        Self {
            report_history: Vec::new(),
            report_versions: HashMap::new(),
            market_snapshot: None,
            config: ReportConfig::default(),
            last_update: chrono::Utc::now(),
        }
    }

    /// 리포트 추가 (불변 방식)
    pub fn add_report(mut self, report: MarketReport) -> Self {
        // 리포트 히스토리에 추가
        self.report_history.push(report.clone());

        // 버전 관리
        let version_key = format!("{}_{}", report.symbols.join("_"), report.report_type.as_str());
        let versions = self.report_versions.entry(version_key).or_insert_with(Vec::new);

        let version_number = versions.len() as u32 + 1;
        versions.push(ReportVersion {
            version: version_number,
            report: report.clone(),
            changes_summary: format!("새 리포트 생성 (v{})", version_number),
            created_at: chrono::Utc::now(),
        });

        // 최대 히스토리 개수 제한 (50개)
        if self.report_history.len() > 50 {
            self.report_history.remove(0);
        }

        self.last_update = chrono::Utc::now();
        self
    }

    /// 시장 데이터 스냅샷 업데이트
    pub fn update_market_snapshot(mut self, snapshot: MarketSnapshot) -> Self {
        self.market_snapshot = Some(snapshot);
        self.last_update = chrono::Utc::now();
        self
    }

    /// 특정 타입의 최근 리포트 조회
    pub fn get_recent_report(&self, report_type: &ReportType, symbols: &[String]) -> Option<&MarketReport> {
        self.report_history
            .iter()
            .rev()
            .find(|r| {
                &r.report_type == report_type &&
                symbols.iter().all(|s| r.symbols.contains(s))
            })
    }

    /// 심볼별 리포트 목록 조회
    pub fn get_reports_for_symbol(&self, symbol: &str, limit: Option<usize>) -> Vec<&MarketReport> {
        let limit = limit.unwrap_or(10);
        self.report_history
            .iter()
            .filter(|r| r.symbols.contains(&symbol.to_string()))
            .rev()
            .take(limit)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    /// 기간별 리포트 통계
    pub fn get_report_stats(&self, days: u64) -> ReportStats {
        let cutoff = chrono::Utc::now() - chrono::Duration::days(days as i64);

        let recent_reports: Vec<&MarketReport> = self.report_history
            .iter()
            .filter(|r| r.generated_at > cutoff)
            .collect();

        let total_reports = recent_reports.len();
        let avg_confidence = if total_reports > 0 {
            recent_reports.iter().map(|r| r.confidence_score).sum::<f64>() / total_reports as f64
        } else {
            0.0
        };

        let report_types: HashMap<String, usize> = recent_reports
            .iter()
            .fold(HashMap::new(), |mut acc, r| {
                let type_str = r.report_type.as_str();
                *acc.entry(type_str.to_string()).or_insert(0) += 1;
                acc
            });

        ReportStats {
            total_reports,
            avg_confidence,
            report_types,
            period_days: days,
            generated_at: chrono::Utc::now(),
        }
    }
}

/// 리포트 통계
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportStats {
    pub total_reports: usize,
    pub avg_confidence: f64,
    pub report_types: HashMap<String, usize>,
    pub period_days: u64,
    pub generated_at: chrono::DateTime<chrono::Utc>,
}

impl ReportType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReportType::DailyMarketReport => "daily",
            ReportType::WeeklyMarketReport => "weekly",
            ReportType::SectorAnalysis => "sector",
            ReportType::StockSpecificReport => "stock_specific",
            ReportType::RiskAssessment => "risk",
            ReportType::OpportunityAnalysis => "opportunity",
        }
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            ReportType::DailyMarketReport => "일일 시장 리포트",
            ReportType::WeeklyMarketReport => "주간 시장 리포트",
            ReportType::SectorAnalysis => "섹터 분석",
            ReportType::StockSpecificReport => "종목별 리포트",
            ReportType::RiskAssessment => "리스크 평가",
            ReportType::OpportunityAnalysis => "기회 분석",
        }
    }
}

/// ReportGenerationActor 메시지
#[derive(Debug, Clone)]
pub enum ReportMessage {
    /// 리포트 생성 요청
    GenerateReport {
        report_type: ReportType,
        symbols: Vec<String>,
        time_period: String,
    },

    /// 자동 리포트 생성 (일일/주간)
    GenerateAutoReport {
        report_type: ReportType,
    },

    /// 리포트 조회
    GetReport {
        report_type: ReportType,
        symbols: Vec<String>,
    },

    /// 심볼별 리포트 목록 조회
    GetReportsForSymbol {
        symbol: String,
        limit: Option<usize>,
    },

    /// 리포트 통계 조회
    GetReportStats {
        days: Option<u64>,
    },

    /// 시장 데이터 스냅샷 업데이트
    UpdateMarketSnapshot {
        snapshot: MarketSnapshot,
    },

    /// 리포트 내보내기 (JSON/CSV)
    ExportReport {
        report_id: String,
        format: ExportFormat,
    },

    /// 오래된 리포트 정리
    CleanupOldReports {
        max_age_hours: u64,
    },

    /// 설정 업데이트
    UpdateConfig {
        config: ReportConfig,
    },
}

/// 내보내기 형식
#[derive(Debug, Clone)]
pub enum ExportFormat {
    Json,
    Csv,
    Markdown,
}

/// 리포트 생성 함수형 Actor
pub struct ReportGenerationActor {
    llm_client: std::sync::Arc<dyn LLMClient>,
}

impl ReportGenerationActor {
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

    /// 리포트 ID 생성
    fn generate_report_id(report_type: &ReportType, symbols: &[String], timestamp: &chrono::DateTime<chrono::Utc>) -> String {
        format!(
            "{}_{}_{}",
            report_type.as_str(),
            symbols.join("_"),
            timestamp.format("%Y%m%d_%H%M%S")
        )
    }

    /// 리포트 제목 생성
    fn generate_report_title(report_type: &ReportType, symbols: &[String], time_period: &str) -> String {
        match report_type {
            ReportType::DailyMarketReport => {
                format!("일일 시장 리포트 - {} ({})", symbols.join(", "), time_period)
            }
            ReportType::WeeklyMarketReport => {
                format!("주간 시장 리포트 - {} ({})", symbols.join(", "), time_period)
            }
            ReportType::StockSpecificReport => {
                format!("{} 종목 분석 리포트 ({})", symbols.join(", "), time_period)
            }
            ReportType::SectorAnalysis => {
                format!("섹터 분석 리포트 - {} ({})", symbols.join(", "), time_period)
            }
            ReportType::RiskAssessment => {
                format!("리스크 평가 리포트 - {} ({})", symbols.join(", "), time_period)
            }
            ReportType::OpportunityAnalysis => {
                format!("투자 기회 분석 리포트 - {} ({})", symbols.join(", "), time_period)
            }
        }
    }

    /// 시장 데이터 요약 생성
    fn summarize_market_data(&self, symbols: &[String], market_snapshot: &MarketSnapshot) -> String {
        let mut summary = String::new();
        summary.push_str("## 시장 데이터 요약\n\n");

        for symbol in symbols {
            if let Some(price_point) = market_snapshot.price_data.get(symbol) {
                // 가격 변동률 계산 (현재 가격과 시가 비교)
                let change_percent = if price_point.open > 0.0 {
                    ((price_point.price - price_point.open) / price_point.open) * 100.0
                } else {
                    0.0
                };

                summary.push_str(&format!(
                    "**{}**: ${:.2} (전일대비 {:.2}%, 거래량: {})\n",
                    symbol,
                    price_point.price,
                    change_percent,
                    price_point.volume
                ));
            }
        }

        summary.push_str("\n");
        summary
    }

    /// 투자 추천사항 생성
    fn generate_recommendations(&self, config: &ReportConfig, symbols: &[String]) -> Vec<String> {
        let mut recommendations = Vec::new();

        match config.risk_tolerance {
            RiskTolerance::Conservative => {
                recommendations.push("안정적인 우량주 위주로 포트폴리오 구성 권장".to_string());
                recommendations.push("단기 변동성이 높은 종목은 피하고 장기적 관점 유지".to_string());
            }
            RiskTolerance::Moderate => {
                recommendations.push("시장 평균 수익률을 목표로 균형 잡힌 포트폴리오 구성".to_string());
                recommendations.push("기술적 분석과 기본적 분석을 함께 고려".to_string());
            }
            RiskTolerance::Aggressive => {
                recommendations.push("고위험 고수익 전략 고려 가능".to_string());
                recommendations.push("단기 트레이딩 기회를 적극적으로 활용".to_string());
            }
        }

        for symbol in symbols {
            recommendations.push(format!("{} 종목에 대한 추가 모니터링 권장", symbol));
        }

        recommendations
    }

    /// 리스크 경고 생성
    fn generate_risk_warnings(&self, symbols: &[String]) -> Vec<String> {
        let mut warnings = vec![
            "주식 투자는 원금 손실의 위험이 있습니다.".to_string(),
            "과거 성과가 미래 수익을 보장하지 않습니다.".to_string(),
            "시장 상황에 따라 예측이 변경될 수 있습니다.".to_string(),
        ];

        for symbol in symbols {
            warnings.push(format!("{} 종목의 변동성에 유의하세요.", symbol));
        }

        warnings
    }
}

impl FunctionalActor for ReportGenerationActor {
    type State = ReportState;
    type Message = ReportMessage;

    fn init(_ctx: &FunctionalActorContext) -> Self::State {
        ReportState::new()
    }

    fn receive(
        state: Self::State,
        msg: Self::Message,
        _ctx: &FunctionalActorContext,
    ) -> (Self::State, Effect) {
        match msg {
            ReportMessage::GenerateReport { report_type, symbols, time_period: _ } => {
                // 리포트 생성 Effect
                (state, Effect::None) // 실제로는 LLM API 호출 Effect
            }

            ReportMessage::GenerateAutoReport { report_type } => {
                // 자동 리포트 생성 (현재 날짜 기반)
                let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
                let _symbols = vec!["AAPL".to_string(), "GOOGL".to_string(), "MSFT".to_string()]; // 기본 심볼들
                (state, Effect::None)
            }

            ReportMessage::GetReport { report_type, symbols } => {
                if let Some(report) = state.get_recent_report(&report_type, &symbols) {
                } else {
                    tracing::warn!(
                        "{} 타입의 리포트가 없습니다: {}",
                        report_type.display_name(),
                        symbols.join(", ")
                    );
                }
                (state, Effect::None)
            }

            ReportMessage::GetReportsForSymbol { symbol, limit } => {
                let reports = state.get_reports_for_symbol(&symbol, limit);
                (state, Effect::None)
            }

            ReportMessage::GetReportStats { days } => {
                let days = days.unwrap_or(7);
                let stats = state.get_report_stats(days);
                (state, Effect::None)
            }

            ReportMessage::UpdateMarketSnapshot { snapshot } => {
                let new_state = state.update_market_snapshot(snapshot);
                (new_state, Effect::None)
            }

            ReportMessage::ExportReport { report_id, format } => {
                // 리포트 내보내기 (실제로는 파일 시스템 작업)
                (state, Effect::None)
            }

            ReportMessage::CleanupOldReports { max_age_hours } => {
                // 오래된 리포트 정리 (실제로는 타임스탬프 기반 필터링)
                (state, Effect::None)
            }

            ReportMessage::UpdateConfig { config } => {
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
            tracing::debug!("ReportGenerationActor Effect 적용");
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_report_type_as_str() {
        assert_eq!(ReportType::DailyMarketReport.as_str(), "daily");
        assert_eq!(ReportType::WeeklyMarketReport.as_str(), "weekly");
        assert_eq!(ReportType::StockSpecificReport.as_str(), "stock_specific");
    }

    #[test]
    fn test_report_type_display_name() {
        assert_eq!(ReportType::DailyMarketReport.display_name(), "일일 시장 리포트");
        assert_eq!(ReportType::WeeklyMarketReport.display_name(), "주간 시장 리포트");
    }

    #[test]
    fn test_state_add_report() {
        let state = ReportState::new();

        let report = MarketReport {
            id: "test_report_001".to_string(),
            title: "테스트 리포트".to_string(),
            report_type: ReportType::DailyMarketReport,
            time_period: "2024-01-01".to_string(),
            symbols: vec!["AAPL".to_string()],
            content: "테스트 내용".to_string(),
            summary: "테스트 요약".to_string(),
            key_insights: vec!["인사이트 1".to_string()],
            recommendations: vec!["추천사항 1".to_string()],
            risk_warnings: vec!["리스크 1".to_string()],
            confidence_score: 0.85,
            generated_at: chrono::Utc::now(),
            data_sources: vec!["test_source".to_string()],
        };

        let new_state = state.add_report(report);

        assert_eq!(new_state.report_history.len(), 1);
        assert!(state.report_history.is_empty()); // 불변성 확인
    }

    #[test]
    fn test_state_get_recent_report() {
        let state = ReportState::new();

        let report = MarketReport {
            id: "daily_aapl_001".to_string(),
            title: "AAPL 일일 리포트".to_string(),
            report_type: ReportType::DailyMarketReport,
            time_period: "2024-01-01".to_string(),
            symbols: vec!["AAPL".to_string()],
            content: "내용".to_string(),
            summary: "요약".to_string(),
            key_insights: vec![],
            recommendations: vec![],
            risk_warnings: vec![],
            confidence_score: 0.8,
            generated_at: chrono::Utc::now(),
            data_sources: vec![],
        };

        let state_with_report = state.add_report(report);
        let retrieved = state_with_report.get_recent_report(
            &ReportType::DailyMarketReport,
            &["AAPL".to_string()]
        );

        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, "daily_aapl_001");
    }

    #[test]
    fn test_generate_report_id() {
        let report_type = ReportType::DailyMarketReport;
        let symbols = vec!["AAPL".to_string(), "GOOGL".to_string()];
        let timestamp = chrono::Utc::now();

        let id = ReportGenerationActor::generate_report_id(&report_type, &symbols, &timestamp);

        assert!(id.starts_with("daily_AAPL_GOOGL_"));
        assert!(id.contains(&timestamp.format("%Y%m%d").to_string()));
    }

    #[test]
    fn test_generate_report_title() {
        let report_type = ReportType::DailyMarketReport;
        let symbols = vec!["AAPL".to_string()];
        let time_period = "2024-01-01";

        let title = ReportGenerationActor::generate_report_title(&report_type, &symbols, time_period);

        assert_eq!(title, "일일 시장 리포트 - AAPL (2024-01-01)");
    }
}
//! 데이터 전처리 모듈
//! 
//! 수집된 데이터를 정제하고 특성 추출을 수행합니다:
//! - 데이터 정제 및 이상치 제거
//! - 결측값 처리
//! - 기술적 지표 계산
//! - 특성 엔지니어링
//! - 데이터 정규화 및 스케일링

pub mod data_cleaner;
pub mod feature_extractor;
pub mod technical_indicators;
pub mod data_transformer;
pub mod outlier_detector;

pub use data_cleaner::*;
pub use feature_extractor::*;
pub use technical_indicators::*;
pub use data_transformer::*;
pub use outlier_detector::*;

use crate::data::{MarketData, NewsData, AlternativeData};
use anyhow::Result;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use chrono::{DateTime, Utc};

/// 전처리 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreprocessingConfig {
    pub cleaning: CleaningConfig,
    pub feature_extraction: FeatureExtractionConfig,
    pub normalization: NormalizationConfig,
    pub outlier_detection: OutlierDetectionConfig,
}

/// 데이터 정제 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleaningConfig {
    pub remove_duplicates: bool,
    pub handle_missing_values: MissingValueStrategy,
    pub price_correction: bool,
    pub volume_filtering: bool,
    pub time_alignment: bool,
}

/// 결측값 처리 전략
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MissingValueStrategy {
    Drop,           // 삭제
    ForwardFill,    // 이전 값으로 채우기
    BackwardFill,   // 다음 값으로 채우기
    Interpolate,    // 보간
    Mean,           // 평균으로 채우기
    Median,         // 중간값으로 채우기
    Custom(f64),    // 사용자 정의 값
}

/// 특성 추출 설정
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FeatureExtractionConfig {
    pub technical_indicators: TechnicalIndicatorConfig,
    pub price_features: PriceFeatureConfig,
    pub volume_features: VolumeFeatureConfig,
    pub time_features: TimeFeatureConfig,
    pub statistical_features: StatisticalFeatureConfig,
}

/// 기술적 지표 설정
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TechnicalIndicatorConfig {
    pub sma_periods: Vec<u32>,
    pub ema_periods: Vec<u32>,
    pub rsi_period: u32,
    pub macd_fast: u32,
    pub macd_slow: u32,
    pub macd_signal: u32,
    pub bollinger_period: u32,
    pub bollinger_std: f64,
    pub atr_period: u32,
    pub stochastic_k: u32,
    pub stochastic_d: u32,
    pub williams_r_period: u32,
}

/// 가격 특성 설정
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PriceFeatureConfig {
    pub returns: bool,
    pub log_returns: bool,
    pub price_changes: bool,
    pub price_ratios: bool,
    pub volatility: bool,
    pub price_position: bool,
}

/// 거래량 특성 설정
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VolumeFeatureConfig {
    pub volume_ma: bool,
    pub volume_ratio: bool,
    pub volume_price_trend: bool,
    pub on_balance_volume: bool,
    pub accumulation_distribution: bool,
}

/// 시간 특성 설정
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TimeFeatureConfig {
    pub day_of_week: bool,
    pub month: bool,
    pub quarter: bool,
    pub year: bool,
    pub is_month_end: bool,
    pub is_quarter_end: bool,
    pub is_year_end: bool,
    pub trading_session: bool,
}

/// 통계적 특성 설정
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StatisticalFeatureConfig {
    pub rolling_mean: bool,
    pub rolling_std: bool,
    pub rolling_skew: bool,
    pub rolling_kurtosis: bool,
    pub rolling_percentiles: Vec<f64>,
    pub rolling_correlation: bool,
}

/// 정규화 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizationConfig {
    pub method: NormalizationMethod,
    pub feature_range: (f64, f64),
    pub robust_scaling: bool,
    pub preserve_outliers: bool,
}

/// 정규화 방법
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NormalizationMethod {
    MinMax,
    StandardScaler,
    RobustScaler,
    QuantileTransformer,
    PowerTransformer,
    None,
}

/// 이상치 탐지 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutlierDetectionConfig {
    pub method: OutlierDetectionMethod,
    pub threshold: f64,
    pub window_size: u32,
    pub action: OutlierAction,
}

/// 이상치 탐지 방법
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutlierDetectionMethod {
    ZScore,
    ModifiedZScore,
    IQR,
    IsolationForest,
    LocalOutlierFactor,
    OneClassSVM,
    None,
}

/// 이상치 처리 방법
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutlierAction {
    Remove,
    Cap,
    Transform,
    Flag,
}

/// 전처리된 데이터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedData {
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub raw_data: RawData,
    pub features: HashMap<String, f64>,
    pub quality_score: f64,
    pub outlier_flags: HashMap<String, bool>,
    pub metadata: HashMap<String, String>,
}

/// 원본 데이터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RawData {
    MarketData(MarketData),
    NewsData(NewsData),
    AlternativeData(AlternativeData),
}

/// 전처리 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreprocessingResult {
    pub processed_data: Vec<ProcessedData>,
    pub statistics: PreprocessingStatistics,
    pub quality_report: QualityReport,
    pub feature_importance: HashMap<String, f64>,
}

/// 전처리 통계
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreprocessingStatistics {
    pub total_records: u32,
    pub cleaned_records: u32,
    pub removed_records: u32,
    pub outlier_count: u32,
    pub missing_values_count: u32,
    pub feature_count: u32,
    pub processing_time_ms: u64,
}

/// 품질 보고서
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityReport {
    pub overall_quality: f64,
    pub completeness: f64,
    pub accuracy: f64,
    pub consistency: f64,
    pub timeliness: f64,
    pub validity: f64,
    pub issues: Vec<QualityIssue>,
}

/// 품질 이슈
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityIssue {
    pub issue_type: QualityIssueType,
    pub severity: QualityIssueSeverity,
    pub description: String,
    pub affected_records: u32,
    pub suggestion: String,
}

/// 품질 이슈 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QualityIssueType {
    MissingValues,
    Outliers,
    Duplicates,
    InvalidTimestamps,
    PriceAnomalies,
    VolumeAnomalies,
    DataInconsistency,
    LowConfidence,
}

/// 품질 이슈 심각도
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QualityIssueSeverity {
    Low,
    Medium,
    High,
    Critical,
}

impl Default for PreprocessingConfig {
    fn default() -> Self {
        Self {
            cleaning: CleaningConfig {
                remove_duplicates: true,
                handle_missing_values: MissingValueStrategy::ForwardFill,
                price_correction: true,
                volume_filtering: true,
                time_alignment: true,
            },
            feature_extraction: FeatureExtractionConfig {
                technical_indicators: TechnicalIndicatorConfig {
                    sma_periods: vec![5, 10, 20, 50, 200],
                    ema_periods: vec![12, 26],
                    rsi_period: 14,
                    macd_fast: 12,
                    macd_slow: 26,
                    macd_signal: 9,
                    bollinger_period: 20,
                    bollinger_std: 2.0,
                    atr_period: 14,
                    stochastic_k: 14,
                    stochastic_d: 3,
                    williams_r_period: 14,
                },
                price_features: PriceFeatureConfig {
                    returns: true,
                    log_returns: true,
                    price_changes: true,
                    price_ratios: true,
                    volatility: true,
                    price_position: true,
                },
                volume_features: VolumeFeatureConfig {
                    volume_ma: true,
                    volume_ratio: true,
                    volume_price_trend: true,
                    on_balance_volume: true,
                    accumulation_distribution: true,
                },
                time_features: TimeFeatureConfig {
                    day_of_week: true,
                    month: true,
                    quarter: true,
                    year: false,
                    is_month_end: true,
                    is_quarter_end: true,
                    is_year_end: false,
                    trading_session: true,
                },
                statistical_features: StatisticalFeatureConfig {
                    rolling_mean: true,
                    rolling_std: true,
                    rolling_skew: false,
                    rolling_kurtosis: false,
                    rolling_percentiles: vec![0.25, 0.5, 0.75],
                    rolling_correlation: false,
                },
            },
            normalization: NormalizationConfig {
                method: NormalizationMethod::StandardScaler,
                feature_range: (-1.0, 1.0),
                robust_scaling: false,
                preserve_outliers: false,
            },
            outlier_detection: OutlierDetectionConfig {
                method: OutlierDetectionMethod::ModifiedZScore,
                threshold: 3.5,
                window_size: 20,
                action: OutlierAction::Flag,
            },
        }
    }
}

impl ProcessedData {
    /// 특성 조회
    pub fn get_feature(&self, name: &str) -> Option<f64> {
        self.features.get(name).copied()
    }
    
    /// 특성 설정
    pub fn set_feature(&mut self, name: String, value: f64) {
        self.features.insert(name, value);
    }
    
    /// 이상치 여부 확인
    pub fn is_outlier(&self, feature_name: &str) -> bool {
        self.outlier_flags.get(feature_name).copied().unwrap_or(false)
    }
    
    /// 메타데이터 조회
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }
    
    /// 품질 점수 업데이트
    pub fn update_quality_score(&mut self, score: f64) {
        self.quality_score = score.clamp(0.0, 1.0);
    }
}

impl PreprocessingResult {
    /// 평균 품질 점수 계산
    pub fn average_quality_score(&self) -> f64 {
        if self.processed_data.is_empty() {
            return 0.0;
        }
        
        let total_score: f64 = self.processed_data.iter()
            .map(|data| data.quality_score)
            .sum();
        
        total_score / self.processed_data.len() as f64
    }
    
    /// 특성별 통계 계산
    pub fn feature_statistics(&self) -> HashMap<String, FeatureStatistics> {
        let mut stats = HashMap::new();
        
        if self.processed_data.is_empty() {
            return stats;
        }
        
        // 모든 특성명 수집
        let feature_names: std::collections::HashSet<String> = self.processed_data.iter()
            .flat_map(|data| data.features.keys().cloned())
            .collect();
        
        for feature_name in feature_names {
            let values: Vec<f64> = self.processed_data.iter()
                .filter_map(|data| data.features.get(&feature_name).copied())
                .collect();
            
            if !values.is_empty() {
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance = values.iter()
                    .map(|x| (x - mean).powi(2))
                    .sum::<f64>() / values.len() as f64;
                let std_dev = variance.sqrt();
                
                let min_val = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                let max_val = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
                
                stats.insert(feature_name, FeatureStatistics {
                    count: values.len(),
                    mean,
                    std_dev,
                    min: min_val,
                    max: max_val,
                    median: Self::calculate_median(&mut values.clone()),
                });
            }
        }
        
        stats
    }
    
    /// 중간값 계산
    fn calculate_median(values: &mut [f64]) -> f64 {
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let len = values.len();
        if len % 2 == 0 {
            (values[len / 2 - 1] + values[len / 2]) / 2.0
        } else {
            values[len / 2]
        }
    }
}

/// 특성 통계
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureStatistics {
    pub count: usize,
    pub mean: f64,
    pub std_dev: f64,
    pub min: f64,
    pub max: f64,
    pub median: f64,
}

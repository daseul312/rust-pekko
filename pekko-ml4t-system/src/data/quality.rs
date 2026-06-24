//! 데이터 품질 관리 모듈
//! 
//! 데이터의 품질을 평가하고 개선하는 기능을 제공합니다.
//! - 데이터 완성도 (Completeness)
//! - 데이터 정확성 (Accuracy)
//! - 데이터 일관성 (Consistency)
//! - 데이터 적시성 (Timeliness)
//! - 데이터 유효성 (Validity)

use anyhow::Result;
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use tracing::{ debug, warn, error};

/// 데이터 품질 메트릭
#[derive(Debug, Clone)]
pub struct QualityMetrics {
    pub completeness: f64,    // 완성도 (0.0 ~ 1.0)
    pub accuracy: f64,        // 정확성 (0.0 ~ 1.0)
    pub consistency: f64,     // 일관성 (0.0 ~ 1.0)
    pub timeliness: f64,      // 적시성 (0.0 ~ 1.0)
    pub validity: f64,        // 유효성 (0.0 ~ 1.0)
    pub overall_score: f64,   // 전체 점수 (0.0 ~ 1.0)
    pub timestamp: DateTime<Utc>,
}

/// 데이터 품질 이슈
#[derive(Debug, Clone)]
pub struct QualityIssue {
    pub issue_type: QualityIssueType,
    pub severity: IssueSeverity,
    pub description: String,
    pub affected_records: usize,
    pub suggested_fix: Option<String>,
}

/// 품질 이슈 타입
#[derive(Debug, Clone)]
pub enum QualityIssueType {
    MissingValues,
    DuplicateRecords,
    Outliers,
    InvalidFormat,
    InconsistentData,
    StaleData,
    DataDrift,
}

/// 이슈 심각도
#[derive(Debug, Clone)]
pub enum IssueSeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// 데이터 품질 평가기
pub struct QualityAssessor {
    config: QualityConfig,
}

/// 품질 평가 설정
#[derive(Debug, Clone)]
pub struct QualityConfig {
    pub completeness_threshold: f64,
    pub accuracy_threshold: f64,
    pub consistency_threshold: f64,
    pub timeliness_threshold: f64,
    pub validity_threshold: f64,
    pub outlier_threshold: f64,
    pub staleness_threshold_hours: i64,
}

impl Default for QualityConfig {
    fn default() -> Self {
        Self {
            completeness_threshold: 0.95,
            accuracy_threshold: 0.90,
            consistency_threshold: 0.85,
            timeliness_threshold: 0.80,
            validity_threshold: 0.90,
            outlier_threshold: 3.0, // 3 sigma
            staleness_threshold_hours: 24,
        }
    }
}

impl QualityAssessor {
    pub fn new(config: QualityConfig) -> Self {
        Self { config }
    }
    
    /// 데이터 품질 평가
    pub async fn assess_quality(
        &self,
        data: &HashMap<String, Vec<f64>>,
        expected_schema: &HashMap<String, String>,
    ) -> Result<(QualityMetrics, Vec<QualityIssue>)> {
        let completeness = self.calculate_completeness(data).await?;
        let accuracy = self.calculate_accuracy(data).await?;
        let consistency = self.calculate_consistency(data).await?;
        let timeliness = self.calculate_timeliness(data).await?;
        let validity = self.calculate_validity(data, expected_schema).await?;
        
        let overall_score = (completeness + accuracy + consistency + timeliness + validity) / 5.0;
        
        let metrics = QualityMetrics {
            completeness,
            accuracy,
            consistency,
            timeliness,
            validity,
            overall_score,
            timestamp: Utc::now(),
        };
        
        let issues = self.detect_quality_issues(data, &metrics).await?;
        Ok((metrics, issues))
    }
    
    /// 완성도 계산
    async fn calculate_completeness(&self, data: &HashMap<String, Vec<f64>>) -> Result<f64> {
        let mut total_values = 0;
        let mut missing_values = 0;
        
        for (column, values) in data {
            total_values += values.len();
            missing_values += values.iter().filter(|&&v| v.is_nan()).count();
        }
        
        if total_values == 0 {
            return Ok(0.0);
        }
        
        let completeness = 1.0 - (missing_values as f64 / total_values as f64);
        Ok(completeness.max(0.0).min(1.0))
    }
    
    /// 정확성 계산 (도메인 지식 기반 검증)
    async fn calculate_accuracy(&self, data: &HashMap<String, Vec<f64>>) -> Result<f64> {
        let mut accurate_values = 0;
        let mut total_values = 0;
        
        for (column, values) in data {
            for &value in values {
                if !value.is_nan() {
                    total_values += 1;
                    
                    // 도메인별 정확성 검증
                    if self.is_value_accurate(column, value) {
                        accurate_values += 1;
                    }
                }
            }
        }
        
        if total_values == 0 {
            return Ok(0.0);
        }
        
        Ok(accurate_values as f64 / total_values as f64)
    }
    
    /// 일관성 계산
    async fn calculate_consistency(&self, data: &HashMap<String, Vec<f64>>) -> Result<f64> {
        // 예시: 가격 데이터의 일관성 검사
        if let (Some(open), Some(high), Some(low), Some(close)) = (
            data.get("open"),
            data.get("high"),
            data.get("low"),
            data.get("close"),
        ) {
            let mut consistent_records = 0;
            let total_records = open.len().min(high.len()).min(low.len()).min(close.len());
            
            for i in 0..total_records {
                let o = open[i];
                let h = high[i];
                let l = low[i];
                let c = close[i];
                
                // OHLC 일관성 검사: low <= open,close <= high
                if l <= o && l <= c && o <= h && c <= h {
                    consistent_records += 1;
                }
            }
            
            if total_records > 0 {
                return Ok(consistent_records as f64 / total_records as f64);
            }
        }
        
        Ok(1.0) // 기본값
    }
    
    /// 적시성 계산
    async fn calculate_timeliness(&self, data: &HashMap<String, Vec<f64>>) -> Result<f64> {
        // 현재 시간과 데이터의 최신 시간 비교
        let now = Utc::now();
        let staleness_threshold = chrono::Duration::hours(self.config.staleness_threshold_hours);
        
        // TODO: 실제 타임스탬프 데이터와 비교
        // 여기서는 시뮬레이션
        let data_age_hours = 12.0; // 12시간 전 데이터
        let timeliness = if data_age_hours <= self.config.staleness_threshold_hours as f64 {
            1.0
        } else {
            1.0 - (data_age_hours / (self.config.staleness_threshold_hours as f64 * 2.0))
        };
        
        Ok(timeliness.max(0.0).min(1.0))
    }
    
    /// 유효성 계산
    async fn calculate_validity(&self, data: &HashMap<String, Vec<f64>>, schema: &HashMap<String, String>) -> Result<f64> {
        let mut valid_values = 0;
        let mut total_values = 0;
        
        for (column, values) in data {
            if let Some(expected_type) = schema.get(column) {
                for &value in values {
                    if !value.is_nan() {
                        total_values += 1;
                        
                        if self.is_value_valid(column, value, expected_type) {
                            valid_values += 1;
                        }
                    }
                }
            }
        }
        
        if total_values == 0 {
            return Ok(0.0);
        }
        
        Ok(valid_values as f64 / total_values as f64)
    }
    
    /// 품질 이슈 감지
    async fn detect_quality_issues(&self, data: &HashMap<String, Vec<f64>>, metrics: &QualityMetrics) -> Result<Vec<QualityIssue>> {
        let mut issues = Vec::new();
        
        // 완성도 이슈
        if metrics.completeness < self.config.completeness_threshold {
            issues.push(QualityIssue {
                issue_type: QualityIssueType::MissingValues,
                severity: if metrics.completeness < 0.8 { IssueSeverity::Critical } else { IssueSeverity::High },
                description: format!("데이터 완성도 부족: {:.2}%", metrics.completeness * 100.0),
                affected_records: 0, // TODO: 실제 계산
                suggested_fix: Some("결측치 보간 또는 제거 필요".to_string()),
            });
        }
        
        // 정확성 이슈
        if metrics.accuracy < self.config.accuracy_threshold {
            issues.push(QualityIssue {
                issue_type: QualityIssueType::InvalidFormat,
                severity: if metrics.accuracy < 0.7 { IssueSeverity::Critical } else { IssueSeverity::High },
                description: format!("데이터 정확성 부족: {:.2}%", metrics.accuracy * 100.0),
                affected_records: 0, // TODO: 실제 계산
                suggested_fix: Some("데이터 검증 규칙 강화 필요".to_string()),
            });
        }
        
        // 일관성 이슈
        if metrics.consistency < self.config.consistency_threshold {
            issues.push(QualityIssue {
                issue_type: QualityIssueType::InconsistentData,
                severity: IssueSeverity::Medium,
                description: format!("데이터 일관성 부족: {:.2}%", metrics.consistency * 100.0),
                affected_records: 0, // TODO: 실제 계산
                suggested_fix: Some("데이터 정규화 및 검증 로직 개선 필요".to_string()),
            });
        }
        
        // 적시성 이슈
        if metrics.timeliness < self.config.timeliness_threshold {
            issues.push(QualityIssue {
                issue_type: QualityIssueType::StaleData,
                severity: IssueSeverity::Medium,
                description: format!("데이터 적시성 부족: {:.2}%", metrics.timeliness * 100.0),
                affected_records: 0, // TODO: 실제 계산
                suggested_fix: Some("데이터 업데이트 주기 단축 필요".to_string()),
            });
        }
        
        // 이상치 감지
        for (column, values) in data {
            if let Some(outlier_count) = self.detect_outliers(values) {
                if outlier_count > 0 {
                    issues.push(QualityIssue {
                        issue_type: QualityIssueType::Outliers,
                        severity: if outlier_count > values.len() / 10 { IssueSeverity::High } else { IssueSeverity::Low },
                        description: format!("{} 컬럼에서 {}개의 이상치 감지", column, outlier_count),
                        affected_records: outlier_count,
                        suggested_fix: Some("이상치 분석 및 처리 필요".to_string()),
                    });
                }
            }
        }
        
        Ok(issues)
    }
    
    /// 이상치 감지
    fn detect_outliers(&self, values: &[f64]) -> Option<usize> {
        if values.len() < 3 {
            return None;
        }
        
        let valid_values: Vec<f64> = values.iter().filter(|&&v| !v.is_nan()).cloned().collect();
        if valid_values.len() < 3 {
            return None;
        }
        
        let mean = valid_values.iter().sum::<f64>() / valid_values.len() as f64;
        let variance = valid_values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / valid_values.len() as f64;
        let std_dev = variance.sqrt();
        
        let outlier_count = valid_values.iter()
            .filter(|&&v| (v - mean).abs() > self.config.outlier_threshold * std_dev)
            .count();
        
        Some(outlier_count)
    }
    
    /// 값 정확성 검증
    fn is_value_accurate(&self, column: &str, value: f64) -> bool {
        match column {
            "price" | "open" | "high" | "low" | "close" => value > 0.0,
            "volume" => value >= 0.0,
            "rsi" => value >= 0.0 && value <= 100.0,
            "macd" => true, // MACD는 음수 가능
            _ => true, // 기본적으로 유효하다고 가정
        }
    }
    
    /// 값 유효성 검증
    fn is_value_valid(&self, column: &str, value: f64, expected_type: &str) -> bool {
        match expected_type {
            "numeric" => !value.is_nan() && !value.is_infinite(),
            "positive" => !value.is_nan() && value > 0.0,
            "percentage" => !value.is_nan() && value >= 0.0 && value <= 1.0,
            _ => !value.is_nan(),
        }
    }
}

/// 품질 리포트 생성
pub struct QualityReporter;

impl QualityReporter {
    /// 마크다운 형식의 품질 리포트 생성
    pub fn generate_markdown_report(metrics: &QualityMetrics, issues: &[QualityIssue]) -> String {
        let mut report = String::new();
        
        report.push_str("# 데이터 품질 리포트\n\n");
        report.push_str(&format!("**생성 시간**: {}\n\n", metrics.timestamp.format("%Y-%m-%d %H:%M:%S UTC")));
        
        // 전체 점수
        report.push_str("## 📊 전체 품질 점수\n\n");
        report.push_str(&format!("**전체 점수**: {:.1}%\n\n", metrics.overall_score * 100.0));
        
        // 개별 메트릭
        report.push_str("## 📈 세부 메트릭\n\n");
        report.push_str(&format!("- **완성도**: {:.1}%\n", metrics.completeness * 100.0));
        report.push_str(&format!("- **정확성**: {:.1}%\n", metrics.accuracy * 100.0));
        report.push_str(&format!("- **일관성**: {:.1}%\n", metrics.consistency * 100.0));
        report.push_str(&format!("- **적시성**: {:.1}%\n", metrics.timeliness * 100.0));
        report.push_str(&format!("- **유효성**: {:.1}%\n\n", metrics.validity * 100.0));
        
        // 품질 이슈
        if issues.is_empty() {
            report.push_str("## ✅ 품질 이슈\n\n");
            report.push_str("품질 이슈가 발견되지 않았습니다.\n\n");
        } else {
            report.push_str("## ⚠️ 품질 이슈\n\n");
            
            for (i, issue) in issues.iter().enumerate() {
                let severity_icon = match issue.severity {
                    IssueSeverity::Low => "🟡",
                    IssueSeverity::Medium => "🟠",
                    IssueSeverity::High => "🔴",
                    IssueSeverity::Critical => "🚨",
                };
                
                report.push_str(&format!("### {}. {} {}\n\n", i + 1, severity_icon, issue.description));
                report.push_str(&format!("- **심각도**: {:?}\n", issue.severity));
                report.push_str(&format!("- **영향받는 레코드**: {}\n", issue.affected_records));
                
                if let Some(fix) = &issue.suggested_fix {
                    report.push_str(&format!("- **권장 조치**: {}\n", fix));
                }
                
                report.push_str("\n");
            }
        }
        
        // 권장사항
        report.push_str("## 💡 권장사항\n\n");
        if metrics.overall_score < 0.8 {
            report.push_str("- 데이터 품질 개선이 필요합니다.\n");
            report.push_str("- 품질 이슈들을 우선순위에 따라 해결하세요.\n");
        } else if metrics.overall_score < 0.9 {
            report.push_str("- 데이터 품질이 양호하지만 개선 여지가 있습니다.\n");
        } else {
            report.push_str("- 데이터 품질이 우수합니다.\n");
        }
        
        report
    }
}

//! 데이터 정제 모듈
//! 
//! 수집된 데이터를 정제하고 품질을 향상시킵니다:
//! - 중복 데이터 제거
//! - 결측값 처리
//! - 이상치 탐지 및 처리
//! - 데이터 일관성 검증
//! - 시간 정렬 및 정렬

use super::{PreprocessingConfig, CleaningConfig, MissingValueStrategy, ProcessedData, RawData, QualityReport, QualityIssue, QualityIssueType, QualityIssueSeverity};
use crate::data::{MarketData, NewsData, AlternativeData};
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use tracing::{ warn, error, debug};

/// 데이터 정제기
pub struct DataCleaner {
    config: CleaningConfig,
}

impl DataCleaner {
    /// 새로운 데이터 정제기 생성
    pub fn new(config: CleaningConfig) -> Self {
        Self { config }
    }

    /// 시장 데이터 정제
    pub async fn clean_market_data(&self, data: Vec<MarketData>) -> Result<(Vec<MarketData>, QualityReport)> {
        let mut cleaned_data = data;
        let mut quality_issues = Vec::new();
        
        // 1. 중복 제거
        if self.config.remove_duplicates {
            let (deduplicated, duplicates_count) = self.remove_duplicates(&cleaned_data);
            cleaned_data = deduplicated;
            
            if duplicates_count > 0 {
                quality_issues.push(QualityIssue {
                    issue_type: QualityIssueType::Duplicates,
                    severity: QualityIssueSeverity::Low,
                    description: format!("{}개의 중복 레코드 제거됨", duplicates_count),
                    affected_records: duplicates_count,
                    suggestion: "데이터 수집 과정에서 중복 방지 로직 추가 권장".to_string(),
                });
            }
        }
        
        // 2. 시간 정렬
        if self.config.time_alignment {
            cleaned_data.sort_by_key(|d| d.timestamp);
        }
        
        // 3. 결측값 처리
        let (processed_data, missing_count) = self.handle_missing_values(&cleaned_data)?;
        
        if missing_count > 0 {
            quality_issues.push(QualityIssue {
                issue_type: QualityIssueType::MissingValues,
                severity: QualityIssueSeverity::Medium,
                description: format!("{}개의 결측값 처리됨", missing_count),
                affected_records: missing_count,
                suggestion: "데이터 소스의 안정성 확인 필요".to_string(),
            });
        }
        
        // 4. 가격 이상치 탐지 및 수정
        if self.config.price_correction {
            let (corrected_data, anomaly_count) = self.detect_and_correct_price_anomalies(&processed_data)?;
            cleaned_data = corrected_data;
            
            if anomaly_count > 0 {
                quality_issues.push(QualityIssue {
                    issue_type: QualityIssueType::PriceAnomalies,
                    severity: QualityIssueSeverity::High,
                    description: format!("{}개의 가격 이상치 수정됨", anomaly_count),
                    affected_records: anomaly_count,
                    suggestion: "데이터 소스의 가격 검증 로직 강화 필요".to_string(),
                });
            }
        }
        
        // 5. 거래량 필터링
        if self.config.volume_filtering {
            let (filtered_data, volume_issues_count) = self.filter_volume_anomalies(&cleaned_data)?;
            cleaned_data = filtered_data;
            
            if volume_issues_count > 0 {
                quality_issues.push(QualityIssue {
                    issue_type: QualityIssueType::VolumeAnomalies,
                    severity: QualityIssueSeverity::Medium,
                    description: format!("{}개의 거래량 이상치 처리됨", volume_issues_count),
                    affected_records: volume_issues_count,
                    suggestion: "거래량 데이터 검증 로직 추가 권장".to_string(),
                });
            }
        }
        
        // 품질 보고서 생성
        let quality_report = self.generate_quality_report(&cleaned_data, &quality_issues)?;
        Ok((cleaned_data, quality_report))
    }

    /// 뉴스 데이터 정제
    pub async fn clean_news_data(&self, data: Vec<NewsData>) -> Result<(Vec<NewsData>, QualityReport)> {
        let mut cleaned_data = data;
        let mut quality_issues = Vec::new();
        
        // 1. 중복 제거 (제목과 내용 기반)
        if self.config.remove_duplicates {
            let (deduplicated, duplicates_count) = self.remove_news_duplicates(&cleaned_data);
            cleaned_data = deduplicated;
            
            if duplicates_count > 0 {
                quality_issues.push(QualityIssue {
                    issue_type: QualityIssueType::Duplicates,
                    severity: QualityIssueSeverity::Low,
                    description: format!("{}개의 중복 뉴스 제거됨", duplicates_count),
                    affected_records: duplicates_count,
                    suggestion: "뉴스 수집 과정에서 중복 방지 로직 추가 권장".to_string(),
                });
            }
        }
        
        // 2. 시간 정렬
        if self.config.time_alignment {
            cleaned_data.sort_by_key(|d| d.timestamp);
        }
        
        // 3. 빈 내용 또는 제목 필터링
        cleaned_data.retain(|news| !news.title.trim().is_empty() && !news.content.trim().is_empty());
        
        // 4. 센티먼트 점수 검증
        let invalid_sentiment_count = cleaned_data.iter()
            .filter(|news| {
                if let Some(score) = news.sentiment_score {
                    score < -1.0 || score > 1.0
                } else {
                    false
                }
            })
            .count();
        
        if invalid_sentiment_count > 0 {
            quality_issues.push(QualityIssue {
                issue_type: QualityIssueType::DataInconsistency,
                severity: QualityIssueSeverity::Medium,
                description: format!("{}개의 잘못된 센티먼트 점수 발견", invalid_sentiment_count),
                affected_records: invalid_sentiment_count as u32,
                suggestion: "센티먼트 분석 모델 검증 필요".to_string(),
            });
        }
        
        // 품질 보고서 생성
        let quality_report = self.generate_quality_report(&cleaned_data, &quality_issues)?;
        Ok((cleaned_data, quality_report))
    }

    /// 대체 데이터 정제
    pub async fn clean_alternative_data(&self, data: Vec<AlternativeData>) -> Result<(Vec<AlternativeData>, QualityReport)> {
        let mut cleaned_data = data;
        let mut quality_issues = Vec::new();
        
        // 1. 중복 제거
        if self.config.remove_duplicates {
            let (deduplicated, duplicates_count) = self.remove_alternative_duplicates(&cleaned_data);
            cleaned_data = deduplicated;
            
            if duplicates_count > 0 {
                quality_issues.push(QualityIssue {
                    issue_type: QualityIssueType::Duplicates,
                    severity: QualityIssueSeverity::Low,
                    description: format!("{}개의 중복 대체 데이터 제거됨", duplicates_count),
                    affected_records: duplicates_count,
                    suggestion: "대체 데이터 수집 과정에서 중복 방지 로직 추가 권장".to_string(),
                });
            }
        }
        
        // 2. 시간 정렬
        if self.config.time_alignment {
            cleaned_data.sort_by_key(|d| d.timestamp);
        }
        
        // 3. 빈 메타데이터 필터링
        cleaned_data.retain(|data| !data.metadata.is_empty());
        
        // 품질 보고서 생성
        let quality_report = self.generate_quality_report(&cleaned_data, &quality_issues)?;
        Ok((cleaned_data, quality_report))
    }

    /// 중복 데이터 제거 (시장 데이터)
    fn remove_duplicates(&self, data: &[MarketData]) -> (Vec<MarketData>, u32) {
        let mut seen = HashSet::new();
        let mut unique_data = Vec::new();
        let mut duplicate_count = 0;
        
        for item in data {
            let key = (item.symbol.clone(), item.timestamp);
            if seen.insert(key) {
                unique_data.push(item.clone());
            } else {
                duplicate_count += 1;
            }
        }
        
        (unique_data, duplicate_count)
    }

    /// 중복 뉴스 제거
    fn remove_news_duplicates(&self, data: &[NewsData]) -> (Vec<NewsData>, u32) {
        let mut seen = HashSet::new();
        let mut unique_data = Vec::new();
        let mut duplicate_count = 0;
        
        for item in data {
            let key = (item.title.clone(), item.timestamp);
            if seen.insert(key) {
                unique_data.push(item.clone());
            } else {
                duplicate_count += 1;
            }
        }
        
        (unique_data, duplicate_count)
    }

    /// 중복 대체 데이터 제거
    fn remove_alternative_duplicates(&self, data: &[AlternativeData]) -> (Vec<AlternativeData>, u32) {
        let mut seen = HashSet::new();
        let mut unique_data = Vec::new();
        let mut duplicate_count = 0;
        
        for item in data {
            let key = (item.data_type.clone(), item.timestamp);
            if seen.insert(key) {
                unique_data.push(item.clone());
            } else {
                duplicate_count += 1;
            }
        }
        
        (unique_data, duplicate_count)
    }

    /// 결측값 처리
    fn handle_missing_values(&self, data: &[MarketData]) -> Result<(Vec<MarketData>, u32)> {
        let mut processed_data = data.to_vec();
        let mut missing_count = 0;
        
        match self.config.handle_missing_values {
            MissingValueStrategy::Drop => {
                let original_len = processed_data.len();
                processed_data.retain(|d| {
                    d.open > 0.0 && d.high > 0.0 && d.low > 0.0 && d.close > 0.0 && d.volume > 0
                });
                missing_count = original_len - processed_data.len();
            }
            
            MissingValueStrategy::ForwardFill => {
                for i in 1..processed_data.len() {
                    if processed_data[i].open <= 0.0 {
                        processed_data[i].open = processed_data[i-1].open;
                        missing_count += 1;
                    }
                    if processed_data[i].close <= 0.0 {
                        processed_data[i].close = processed_data[i-1].close;
                        missing_count += 1;
                    }
                    // 다른 필드들도 동일하게 처리
                }
            }
            
            MissingValueStrategy::Interpolate => {
                // 선형 보간 구현
                missing_count = self.interpolate_missing_values(&mut processed_data)?.try_into().unwrap();
            }
            
            MissingValueStrategy::Mean => {
                let avg_price = processed_data.iter()
                    .filter(|d| d.close > 0.0)
                    .map(|d| d.close)
                    .sum::<f64>() / processed_data.iter().filter(|d| d.close > 0.0).count() as f64;
                
                for item in &mut processed_data {
                    if item.open <= 0.0 {
                        item.open = avg_price;
                        missing_count += 1;
                    }
                    if item.close <= 0.0 {
                        item.close = avg_price;
                        missing_count += 1;
                    }
                    // 다른 필드들도 동일하게 처리
                }
            }
            
            _ => {
                warn!("지원하지 않는 결측값 처리 전략: {:?}", self.config.handle_missing_values);
            }
        }
        
        Ok((processed_data, missing_count.try_into().unwrap()))
    }

    /// 선형 보간으로 결측값 처리
    fn interpolate_missing_values(&self, data: &mut [MarketData]) -> Result<u32> {
        let mut missing_count = 0;
        
        // 간단한 선형 보간 구현
        for i in 1..data.len()-1 {
            if data[i].close <= 0.0 && data[i-1].close > 0.0 && data[i+1].close > 0.0 {
                data[i].close = (data[i-1].close + data[i+1].close) / 2.0;
                data[i].open = data[i].close; // 간단한 처리
                data[i].high = data[i].close;
                data[i].low = data[i].close;
                missing_count += 1;
            }
        }
        
        Ok(missing_count)
    }

    /// 가격 이상치 탐지 및 수정
    fn detect_and_correct_price_anomalies(&self, data: &[MarketData]) -> Result<(Vec<MarketData>, u32)> {
        let mut corrected_data = data.to_vec();
        let mut anomaly_count = 0;
        
        for i in 1..corrected_data.len()-1 {
            // split_at_mut를 사용하여 mutable borrow 문제 해결
            let (left, right) = corrected_data.split_at_mut(i);
            let prev = &left[i-1];
            let (current_slice, next_slice) = right.split_at_mut(1);
            let current = &mut current_slice[0];
            let next = &next_slice[0];
            
            // 가격 급등/급락 탐지 (20% 이상 변화)
            let price_change_threshold = 0.2;
            
            if prev.close > 0.0 {
                let price_change = (current.close - prev.close).abs() / prev.close;
                
                if price_change > price_change_threshold {
                    // 가격 급변 시 수정
                    current.close = (prev.close + next.close) / 2.0;
                    current.open = current.close;
                    current.high = current.close * 1.01; // 1% 변동 허용
                    current.low = current.close * 0.99;
                    anomaly_count += 1;
                    
                    warn!("가격 이상치 수정: {} at {}", current.symbol, current.timestamp);
                }
            }
            
            // High < Low 체크
            if current.high < current.low {
                std::mem::swap(&mut current.high, &mut current.low);
                anomaly_count += 1;
            }
            
            // Close가 High/Low 범위를 벗어나는 경우
            if current.close > current.high {
                current.high = current.close;
                anomaly_count += 1;
            }
            if current.close < current.low {
                current.low = current.close;
                anomaly_count += 1;
            }
        }
        
        Ok((corrected_data, anomaly_count))
    }

    /// 거래량 이상치 필터링
    fn filter_volume_anomalies(&self, data: &[MarketData]) -> Result<(Vec<MarketData>, u32)> {
        let mut filtered_data = data.to_vec();
        let mut volume_issues_count = 0;
        
        // 평균 거래량 계산 (이상치 제외)
        let volumes: Vec<u64> = filtered_data.iter().map(|d| d.volume).collect();
        let median_volume = Self::calculate_median(&volumes);
        let volume_threshold = median_volume * 10.0; // 중간값의 10배 이상을 이상치로 간주
        
        // 이상치 제거 또는 조정
        for item in &mut filtered_data {
            if item.volume > volume_threshold as u64 {
                item.volume = median_volume as u64;
                volume_issues_count += 1;
            }
        }
        
        Ok((filtered_data, volume_issues_count))
    }

    /// 중간값 계산
    fn calculate_median(values: &[u64]) -> f64 {
        let mut sorted_values = values.to_vec();
        sorted_values.sort_unstable();
        
        let len = sorted_values.len();
        if len % 2 == 0 {
            (sorted_values[len / 2 - 1] + sorted_values[len / 2]) as f64 / 2.0
        } else {
            sorted_values[len / 2] as f64
        }
    }

    /// 품질 보고서 생성
    fn generate_quality_report<T>(&self, data: &[T], issues: &[QualityIssue]) -> Result<QualityReport> {
        let total_records = data.len() as u32;
        
        // 기본 품질 점수 계산
        let mut overall_quality = 1.0;
        let mut completeness = 1.0;
        let mut accuracy = 1.0;
        let mut consistency = 1.0;
        let mut timeliness: f64 = 1.0;
        let mut validity: f64 = 1.0;
        
        // 이슈별 품질 점수 감소
        for issue in issues {
            let severity_penalty = match issue.severity {
                QualityIssueSeverity::Low => 0.01,
                QualityIssueSeverity::Medium => 0.05,
                QualityIssueSeverity::High => 0.15,
                QualityIssueSeverity::Critical => 0.30,
            };
            
            let affected_ratio = issue.affected_records as f64 / total_records as f64;
            let penalty = severity_penalty * affected_ratio;
            
            overall_quality -= penalty;
            
            // 이슈 타입별 점수 감소
            match issue.issue_type {
                QualityIssueType::MissingValues => completeness -= penalty * 2.0,
                QualityIssueType::Duplicates => consistency -= penalty,
                QualityIssueType::PriceAnomalies => accuracy -= penalty * 3.0,
                QualityIssueType::VolumeAnomalies => accuracy -= penalty * 2.0,
                QualityIssueType::DataInconsistency => consistency -= penalty * 2.0,
                _ => {}
            }
        }
        
        // 점수 범위 조정 (0.0 ~ 1.0)
        overall_quality = overall_quality.max(0.0).min(1.0);
        completeness = completeness.max(0.0).min(1.0);
        accuracy = accuracy.max(0.0).min(1.0);
        consistency = consistency.max(0.0).min(1.0);
        timeliness = timeliness.max(0.0).min(1.0);
        validity = validity.max(0.0).min(1.0);
        
        Ok(QualityReport {
            overall_quality,
            completeness,
            accuracy,
            consistency,
            timeliness,
            validity,
            issues: issues.to_vec(),
        })
    }
}

impl Default for DataCleaner {
    fn default() -> Self {
        Self::new(CleaningConfig {
            remove_duplicates: true,
            handle_missing_values: MissingValueStrategy::ForwardFill,
            price_correction: true,
            volume_filtering: true,
            time_alignment: true,
        })
    }
}

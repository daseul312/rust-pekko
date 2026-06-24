//! 이상치 감지 모듈
//! 
//! 다양한 방법으로 이상치를 감지하고 처리합니다.
//! - 통계적 방법 (IQR, Z-Score)
//! - 머신러닝 방법 (Isolation Forest, Local Outlier Factor)
//! - 시계열 특화 방법 (STL, Seasonal Decomposition)

use anyhow::Result;
use std::collections::HashMap;
use tracing::{ debug, warn, error};

/// 이상치 감지기
pub struct OutlierDetector {
    config: OutlierDetectionConfig,
}

/// 이상치 감지 설정
#[derive(Debug, Clone)]
pub struct OutlierDetectionConfig {
    pub method: OutlierDetectionMethod,
    pub threshold: f64,
    pub window_size: usize,
    pub seasonal_period: Option<usize>,
}

/// 이상치 감지 방법
#[derive(Debug, Clone)]
pub enum OutlierDetectionMethod {
    IQR,
    ZScore,
    ModifiedZScore,
    IsolationForest,
    LocalOutlierFactor,
    STL,
    SeasonalDecomposition,
}

/// 이상치 감지 결과
#[derive(Debug, Clone)]
pub struct OutlierDetectionResult {
    pub outliers: Vec<usize>,
    pub outlier_scores: Vec<f64>,
    pub method_used: OutlierDetectionMethod,
    pub threshold: f64,
}

impl OutlierDetector {
    pub fn new(config: OutlierDetectionConfig) -> Self {
        Self { config }
    }
    
    /// 이상치 감지 실행
    pub async fn detect_outliers(&self, data: &HashMap<String, Vec<f64>>) -> Result<HashMap<String, OutlierDetectionResult>> {
        let mut results = HashMap::new();
        
        for (column, values) in data {
            let result = match self.config.method {
                OutlierDetectionMethod::IQR => {
                    self.detect_outliers_iqr(values).await?
                }
                OutlierDetectionMethod::ZScore => {
                    self.detect_outliers_zscore(values).await?
                }
                OutlierDetectionMethod::ModifiedZScore => {
                    self.detect_outliers_modified_zscore(values).await?
                }
                OutlierDetectionMethod::IsolationForest => {
                    self.detect_outliers_isolation_forest(values).await?
                }
                OutlierDetectionMethod::LocalOutlierFactor => {
                    self.detect_outliers_lof(values).await?
                }
                OutlierDetectionMethod::STL => {
                    self.detect_outliers_stl(values).await?
                }
                OutlierDetectionMethod::SeasonalDecomposition => {
                    self.detect_outliers_seasonal_decomposition(values).await?
                }
            };
            
            results.insert(column.clone(), result);
        }
        Ok(results)
    }
    
    /// IQR 방법으로 이상치 감지
    async fn detect_outliers_iqr(&self, values: &[f64]) -> Result<OutlierDetectionResult> {
        let valid_values: Vec<f64> = values.iter()
            .filter(|&&v| !v.is_nan())
            .cloned()
            .collect();
        
        if valid_values.len() < 4 {
            return Ok(OutlierDetectionResult {
                outliers: Vec::new(),
                outlier_scores: vec![0.0; values.len()],
                method_used: OutlierDetectionMethod::IQR,
                threshold: self.config.threshold,
            });
        }
        
        let mut sorted_values = valid_values.clone();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let q1_idx = sorted_values.len() / 4;
        let q3_idx = 3 * sorted_values.len() / 4;
        let q1 = sorted_values[q1_idx];
        let q3 = sorted_values[q3_idx];
        let iqr = q3 - q1;
        
        let lower_bound = q1 - self.config.threshold * iqr;
        let upper_bound = q3 + self.config.threshold * iqr;
        
        let mut outliers = Vec::new();
        let mut outlier_scores = vec![0.0; values.len()];
        
        for (i, &value) in values.iter().enumerate() {
            if !value.is_nan() && (value < lower_bound || value > upper_bound) {
                outliers.push(i);
                outlier_scores[i] = if value < lower_bound {
                    (lower_bound - value) / iqr
                } else {
                    (value - upper_bound) / iqr
                };
            }
        }
        
        Ok(OutlierDetectionResult {
            outliers,
            outlier_scores,
            method_used: OutlierDetectionMethod::IQR,
            threshold: self.config.threshold,
        })
    }
    
    /// Z-Score 방법으로 이상치 감지
    async fn detect_outliers_zscore(&self, values: &[f64]) -> Result<OutlierDetectionResult> {
        let valid_values: Vec<f64> = values.iter()
            .filter(|&&v| !v.is_nan())
            .cloned()
            .collect();
        
        if valid_values.len() < 2 {
            return Ok(OutlierDetectionResult {
                outliers: Vec::new(),
                outlier_scores: vec![0.0; values.len()],
                method_used: OutlierDetectionMethod::ZScore,
                threshold: self.config.threshold,
            });
        }
        
        let mean = valid_values.iter().sum::<f64>() / valid_values.len() as f64;
        let variance = valid_values.iter()
            .map(|&v| (v - mean).powi(2))
            .sum::<f64>() / valid_values.len() as f64;
        let std_dev = variance.sqrt();
        
        if std_dev == 0.0 {
            return Ok(OutlierDetectionResult {
                outliers: Vec::new(),
                outlier_scores: vec![0.0; values.len()],
                method_used: OutlierDetectionMethod::ZScore,
                threshold: self.config.threshold,
            });
        }
        
        let mut outliers = Vec::new();
        let mut outlier_scores = vec![0.0; values.len()];
        
        for (i, &value) in values.iter().enumerate() {
            if !value.is_nan() {
                let z_score = (value - mean).abs() / std_dev;
                outlier_scores[i] = z_score;
                
                if z_score > self.config.threshold {
                    outliers.push(i);
                }
            }
        }
        
        Ok(OutlierDetectionResult {
            outliers,
            outlier_scores,
            method_used: OutlierDetectionMethod::ZScore,
            threshold: self.config.threshold,
        })
    }
    
    /// Modified Z-Score 방법으로 이상치 감지
    async fn detect_outliers_modified_zscore(&self, values: &[f64]) -> Result<OutlierDetectionResult> {
        let valid_values: Vec<f64> = values.iter()
            .filter(|&&v| !v.is_nan())
            .cloned()
            .collect();
        
        if valid_values.len() < 3 {
            return Ok(OutlierDetectionResult {
                outliers: Vec::new(),
                outlier_scores: vec![0.0; values.len()],
                method_used: OutlierDetectionMethod::ModifiedZScore,
                threshold: self.config.threshold,
            });
        }
        
        let median = self.calculate_median(&valid_values);
        let mad = self.calculate_mad(&valid_values, median);
        
        if mad == 0.0 {
            return Ok(OutlierDetectionResult {
                outliers: Vec::new(),
                outlier_scores: vec![0.0; values.len()],
                method_used: OutlierDetectionMethod::ModifiedZScore,
                threshold: self.config.threshold,
            });
        }
        
        let mut outliers = Vec::new();
        let mut outlier_scores = vec![0.0; values.len()];
        
        for (i, &value) in values.iter().enumerate() {
            if !value.is_nan() {
                let modified_z_score = 0.6745 * (value - median).abs() / mad;
                outlier_scores[i] = modified_z_score;
                
                if modified_z_score > self.config.threshold {
                    outliers.push(i);
                }
            }
        }
        
        Ok(OutlierDetectionResult {
            outliers,
            outlier_scores,
            method_used: OutlierDetectionMethod::ModifiedZScore,
            threshold: self.config.threshold,
        })
    }
    
    /// Isolation Forest 방법으로 이상치 감지 (간단한 구현)
    async fn detect_outliers_isolation_forest(&self, values: &[f64]) -> Result<OutlierDetectionResult> {
        let valid_values: Vec<(usize, f64)> = values.iter()
            .enumerate()
            .filter(|(_, &v)| !v.is_nan())
            .map(|(i, &v)| (i, v))
            .collect();
        
        if valid_values.len() < 3 {
            return Ok(OutlierDetectionResult {
                outliers: Vec::new(),
                outlier_scores: vec![0.0; values.len()],
                method_used: OutlierDetectionMethod::IsolationForest,
                threshold: self.config.threshold,
            });
        }
        
        // 간단한 Isolation Forest 구현
        let mut outlier_scores = vec![0.0; values.len()];
        let mut outliers = Vec::new();
        
        // 각 점에 대해 이상치 점수 계산 (간단화된 버전)
        for (i, &value) in values.iter().enumerate() {
            if !value.is_nan() {
                // 값이 극값에 가까울수록 높은 이상치 점수
                let min_val = valid_values.iter().map(|(_, v)| *v).fold(f64::INFINITY, |a, b| a.min(b));
                let max_val = valid_values.iter().map(|(_, v)| *v).fold(f64::NEG_INFINITY, |a, b| a.max(b));
                let range = max_val - min_val;
                
                if range > 0.0 {
                    let normalized_distance = ((value - min_val) / range - 0.5).abs() * 2.0;
                    outlier_scores[i] = normalized_distance;
                    
                    if normalized_distance > self.config.threshold {
                        outliers.push(i);
                    }
                }
            }
        }
        
        Ok(OutlierDetectionResult {
            outliers,
            outlier_scores,
            method_used: OutlierDetectionMethod::IsolationForest,
            threshold: self.config.threshold,
        })
    }
    
    /// Local Outlier Factor 방법으로 이상치 감지 (간단한 구현)
    async fn detect_outliers_lof(&self, values: &[f64]) -> Result<OutlierDetectionResult> {
        // 간단한 LOF 구현
        self.detect_outliers_isolation_forest(values).await // 임시로 같은 로직 사용
    }
    
    /// STL (Seasonal and Trend decomposition using Loess) 방법으로 이상치 감지
    async fn detect_outliers_stl(&self, values: &[f64]) -> Result<OutlierDetectionResult> {
        // 간단한 STL 구현
        if let Some(period) = self.config.seasonal_period {
            if values.len() < period * 2 {
                return self.detect_outliers_zscore(values).await;
            }
            
            // 계절성 제거 후 잔차에서 이상치 감지
            let mut detrended_values = values.to_vec();
            
            // 간단한 이동평균으로 트렌드 제거
            for i in period..values.len() - period {
                if !values[i].is_nan() {
                    let trend = values[i - period..i + period + 1]
                        .iter()
                        .filter(|&&v| !v.is_nan())
                        .sum::<f64>() / (2 * period + 1) as f64;
                    detrended_values[i] = values[i] - trend;
                }
            }
            
            // 잔차에서 Z-Score로 이상치 감지
            let mut modified_values = values.to_vec();
            for i in 0..values.len() {
                if !values[i].is_nan() {
                    modified_values[i] = detrended_values[i];
                }
            }
            
            self.detect_outliers_zscore(&modified_values).await
        } else {
            self.detect_outliers_zscore(values).await
        }
    }
    
    /// 계절성 분해 방법으로 이상치 감지
    async fn detect_outliers_seasonal_decomposition(&self, values: &[f64]) -> Result<OutlierDetectionResult> {
        // 간단한 계절성 분해 구현
        self.detect_outliers_stl(values).await
    }
    
    /// 중앙값 계산
    fn calculate_median(&self, values: &[f64]) -> f64 {
        let mut sorted_values = values.to_vec();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let n = sorted_values.len();
        if n % 2 == 0 {
            (sorted_values[n / 2 - 1] + sorted_values[n / 2]) / 2.0
        } else {
            sorted_values[n / 2]
        }
    }
    
    /// 중위절대편차(MAD) 계산
    fn calculate_mad(&self, values: &[f64], median: f64) -> f64 {
        let deviations: Vec<f64> = values.iter()
            .map(|&v| (v - median).abs())
            .collect();
        
        self.calculate_median(&deviations)
    }
}

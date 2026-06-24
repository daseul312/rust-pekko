//! 특성 엔지니어링 모듈
//! 
//! 원시 데이터로부터 의미있는 특성을 생성하고 선택합니다.
//! - 특성 생성 (Feature Generation)
//! - 특성 선택 (Feature Selection)
//! - 특성 변환 (Feature Transformation)
//! - 특성 스케일링 (Feature Scaling)

use anyhow::Result;
use std::collections::HashMap;
use tracing::{ debug, warn, error};

/// 특성 엔지니어
pub struct FeatureEngineer {
    config: FeatureEngineeringConfig,
}

/// 특성 엔지니어링 설정
#[derive(Debug, Clone)]
pub struct FeatureEngineeringConfig {
    pub feature_generation: FeatureGenerationConfig,
    pub feature_selection: FeatureSelectionConfig,
    pub feature_scaling: FeatureScalingConfig,
}

/// 특성 생성 설정
#[derive(Debug, Clone)]
pub struct FeatureGenerationConfig {
    pub polynomial_features: bool,
    pub interaction_features: bool,
    pub time_features: bool,
    pub lag_features: Vec<usize>,
    pub rolling_features: Vec<usize>,
}

/// 특성 선택 설정
#[derive(Debug, Clone)]
pub struct FeatureSelectionConfig {
    pub method: FeatureSelectionMethod,
    pub max_features: Option<usize>,
    pub threshold: f64,
}

/// 특성 선택 방법
#[derive(Debug, Clone)]
pub enum FeatureSelectionMethod {
    VarianceThreshold,
    CorrelationThreshold,
    MutualInformation,
    ChiSquare,
    RecursiveFeatureElimination,
    L1Regularization,
}

/// 특성 스케일링 설정
#[derive(Debug, Clone)]
pub struct FeatureScalingConfig {
    pub method: ScalingMethod,
    pub robust_scaling: bool,
}

/// 스케일링 방법
#[derive(Debug, Clone)]
pub enum ScalingMethod {
    StandardScaler,
    MinMaxScaler,
    RobustScaler,
    PowerTransformer,
    QuantileTransformer,
}

impl FeatureEngineer {
    pub fn new(config: FeatureEngineeringConfig) -> Self {
        Self { config }
    }
    
    /// 특성 엔지니어링 실행
    pub async fn engineer_features(
        &self,
        raw_features: &HashMap<String, Vec<f64>>,
    ) -> Result<HashMap<String, Vec<f64>>> {
        let mut features = raw_features.clone();
        
        // 1. 특성 생성
        if self.config.feature_generation.polynomial_features {
            features = self.generate_polynomial_features(&features).await?;
        }
        
        if self.config.feature_generation.interaction_features {
            features = self.generate_interaction_features(&features).await?;
        }
        
        if self.config.feature_generation.time_features {
            features = self.generate_time_features(&features).await?;
        }
        
        if !self.config.feature_generation.lag_features.is_empty() {
            features = self.generate_lag_features(&features).await?;
        }
        
        if !self.config.feature_generation.rolling_features.is_empty() {
            features = self.generate_rolling_features(&features).await?;
        }
        
        // 2. 특성 선택
        features = self.select_features(&features).await?;
        
        // 3. 특성 스케일링
        features = self.scale_features(&features).await?;
        Ok(features)
    }
    
    /// 다항식 특성 생성
    async fn generate_polynomial_features(&self, features: &HashMap<String, Vec<f64>>) -> Result<HashMap<String, Vec<f64>>> {
        let mut new_features = features.clone();
        
        for (name, values) in features {
            // 2차 다항식 특성
            let squared_values: Vec<f64> = values.iter().map(|v| v * v).collect();
            new_features.insert(format!("{}_squared", name), squared_values);
            
            // 3차 다항식 특성
            let cubed_values: Vec<f64> = values.iter().map(|v| v * v * v).collect();
            new_features.insert(format!("{}_cubed", name), cubed_values);
        }
        
        Ok(new_features)
    }
    
    /// 상호작용 특성 생성
    async fn generate_interaction_features(&self, features: &HashMap<String, Vec<f64>>) -> Result<HashMap<String, Vec<f64>>> {
        let mut new_features = features.clone();
        let feature_names: Vec<String> = features.keys().cloned().collect();
        
        for i in 0..feature_names.len() {
            for j in i + 1..feature_names.len() {
                let name1 = &feature_names[i];
                let name2 = &feature_names[j];
                
                if let (Some(values1), Some(values2)) = (features.get(name1), features.get(name2)) {
                    let interaction_values: Vec<f64> = values1.iter()
                        .zip(values2.iter())
                        .map(|(v1, v2)| v1 * v2)
                        .collect();
                    
                    new_features.insert(format!("{}_x_{}", name1, name2), interaction_values);
                }
            }
        }
        
        Ok(new_features)
    }
    
    /// 시간 특성 생성
    async fn generate_time_features(&self, features: &HashMap<String, Vec<f64>>) -> Result<HashMap<String, Vec<f64>>> {
        let mut new_features = features.clone();
        
        // 시계열 길이 가정
        if let Some(first_feature) = features.values().next() {
            let length = first_feature.len();
            
            // 시간 인덱스
            let time_index: Vec<f64> = (0..length).map(|i| i as f64).collect();
            new_features.insert("time_index".to_string(), time_index);
            
            // 주기적 특성 (예: 일별, 주별 패턴)
            let daily_pattern: Vec<f64> = (0..length).map(|i| (i % 24) as f64 / 24.0).collect();
            new_features.insert("daily_pattern".to_string(), daily_pattern);
            
            let weekly_pattern: Vec<f64> = (0..length).map(|i| (i % 168) as f64 / 168.0).collect();
            new_features.insert("weekly_pattern".to_string(), weekly_pattern);
        }
        
        Ok(new_features)
    }
    
    /// 지연 특성 생성
    async fn generate_lag_features(&self, features: &HashMap<String, Vec<f64>>) -> Result<HashMap<String, Vec<f64>>> {
        let mut new_features = features.clone();
        
        for lag in &self.config.feature_generation.lag_features {
            for (name, values) in features {
                if values.len() > *lag {
                    let lag_values: Vec<f64> = (0..values.len())
                        .map(|i| if i >= *lag { values[i - lag] } else { f64::NAN })
                        .collect();
                    
                    new_features.insert(format!("{}_lag_{}", name, lag), lag_values);
                }
            }
        }
        
        Ok(new_features)
    }
    
    /// 롤링 특성 생성
    async fn generate_rolling_features(&self, features: &HashMap<String, Vec<f64>>) -> Result<HashMap<String, Vec<f64>>> {
        let mut new_features = features.clone();
        
        for window in &self.config.feature_generation.rolling_features {
            for (name, values) in features {
                if values.len() >= *window {
                    let rolling_mean = self.calculate_rolling_mean(values, *window);
                    new_features.insert(format!("{}_rolling_mean_{}", name, window), rolling_mean);
                    
                    let rolling_std = self.calculate_rolling_std(values, *window);
                    new_features.insert(format!("{}_rolling_std_{}", name, window), rolling_std);
                    
                    let rolling_max = self.calculate_rolling_max(values, *window);
                    new_features.insert(format!("{}_rolling_max_{}", name, window), rolling_max);
                    
                    let rolling_min = self.calculate_rolling_min(values, *window);
                    new_features.insert(format!("{}_rolling_min_{}", name, window), rolling_min);
                }
            }
        }
        
        Ok(new_features)
    }
    
    /// 특성 선택
    async fn select_features(&self, features: &HashMap<String, Vec<f64>>) -> Result<HashMap<String, Vec<f64>>> {
        let mut selected_features = features.clone();
        
        match self.config.feature_selection.method {
            FeatureSelectionMethod::VarianceThreshold => {
                selected_features = self.variance_threshold_selection(&selected_features).await?;
            }
            FeatureSelectionMethod::CorrelationThreshold => {
                selected_features = self.correlation_threshold_selection(&selected_features).await?;
            }
            FeatureSelectionMethod::MutualInformation => {
                selected_features = self.mutual_information_selection(&selected_features).await?;
            }
            _ => {
                warn!("지원하지 않는 특성 선택 방법: {:?}", self.config.feature_selection.method);
            }
        }
        
        // 최대 특성 수 제한
        if let Some(max_features) = self.config.feature_selection.max_features {
            if selected_features.len() > max_features {
                selected_features = self.limit_features(&selected_features, max_features).await?;
            }
        }
        
        Ok(selected_features)
    }
    
    /// 분산 임계값 기반 특성 선택
    async fn variance_threshold_selection(&self, features: &HashMap<String, Vec<f64>>) -> Result<HashMap<String, Vec<f64>>> {
        let mut selected_features = HashMap::new();
        
        for (name, values) in features {
            let variance = self.calculate_variance(values);
            if variance >= self.config.feature_selection.threshold {
                selected_features.insert(name.clone(), values.clone());
            }
        }
        
        Ok(selected_features)
    }
    
    /// 상관관계 임계값 기반 특성 선택
    async fn correlation_threshold_selection(&self, features: &HashMap<String, Vec<f64>>) -> Result<HashMap<String, Vec<f64>>> {
        let mut selected_features = HashMap::new();
        let mut to_remove = Vec::new();
        
        let feature_names: Vec<String> = features.keys().cloned().collect();
        
        for i in 0..feature_names.len() {
            for j in i + 1..feature_names.len() {
                let name1 = &feature_names[i];
                let name2 = &feature_names[j];
                
                if let (Some(values1), Some(values2)) = (features.get(name1), features.get(name2)) {
                    let correlation = self.calculate_correlation(values1, values2);
                    
                    if correlation.abs() > self.config.feature_selection.threshold {
                        // 상관관계가 높은 특성 중 하나 제거
                        to_remove.push(name2.clone());
                    }
                }
            }
        }
        
        for (name, values) in features {
            if !to_remove.contains(name) {
                selected_features.insert(name.clone(), values.clone());
            }
        }
        
        Ok(selected_features)
    }
    
    /// 상호정보량 기반 특성 선택
    async fn mutual_information_selection(&self, features: &HashMap<String, Vec<f64>>) -> Result<HashMap<String, Vec<f64>>> {
        // 간단한 상호정보량 계산 (실제로는 더 정교한 구현 필요)
        let mut selected_features = HashMap::new();
        
        for (name, values) in features {
            let mi_score = self.calculate_mutual_information(values);
            if mi_score >= self.config.feature_selection.threshold {
                selected_features.insert(name.clone(), values.clone());
            }
        }
        
        Ok(selected_features)
    }
    
    /// 특성 수 제한
    async fn limit_features(&self, features: &HashMap<String, Vec<f64>>, max_features: usize) -> Result<HashMap<String, Vec<f64>>> {
        let mut feature_scores: Vec<(String, f64)> = features.iter()
            .map(|(name, values)| {
                let score = self.calculate_feature_score(values);
                (name.clone(), score)
            })
            .collect();
        
        feature_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        
        let selected_names: Vec<String> = feature_scores.iter()
            .take(max_features)
            .map(|(name, _)| name.clone())
            .collect();
        
        let mut limited_features = HashMap::new();
        for name in selected_names {
            if let Some(values) = features.get(&name) {
                limited_features.insert(name, values.clone());
            }
        }
        
        Ok(limited_features)
    }
    
    /// 특성 스케일링
    async fn scale_features(&self, features: &HashMap<String, Vec<f64>>) -> Result<HashMap<String, Vec<f64>>> {
        let mut scaled_features = features.clone();
        
        match self.config.feature_scaling.method {
            ScalingMethod::StandardScaler => {
                scaled_features = self.standard_scale(&scaled_features).await?;
            }
            ScalingMethod::MinMaxScaler => {
                scaled_features = self.min_max_scale(&scaled_features).await?;
            }
            ScalingMethod::RobustScaler => {
                scaled_features = self.robust_scale(&scaled_features).await?;
            }
            _ => {
                warn!("지원하지 않는 스케일링 방법: {:?}", self.config.feature_scaling.method);
            }
        }
        
        Ok(scaled_features)
    }
    
    /// 표준화 스케일링
    async fn standard_scale(&self, features: &HashMap<String, Vec<f64>>) -> Result<HashMap<String, Vec<f64>>> {
        let mut scaled_features = HashMap::new();
        
        for (name, values) in features {
            let mean = values.iter().filter(|&&v| !v.is_nan()).sum::<f64>() / values.len() as f64;
            let variance = values.iter()
                .filter(|&&v| !v.is_nan())
                .map(|v| (v - mean).powi(2))
                .sum::<f64>() / values.len() as f64;
            let std_dev = variance.sqrt();
            
            if std_dev == 0.0 {
                scaled_features.insert(name.clone(), values.clone());
            } else {
                let scaled_values: Vec<f64> = values.iter()
                    .map(|&v| if v.is_nan() { v } else { (v - mean) / std_dev })
                    .collect();
                scaled_features.insert(name.clone(), scaled_values);
            }
        }
        
        Ok(scaled_features)
    }
    
    /// Min-Max 스케일링
    async fn min_max_scale(&self, features: &HashMap<String, Vec<f64>>) -> Result<HashMap<String, Vec<f64>>> {
        let mut scaled_features = HashMap::new();
        
        for (name, values) in features {
            let valid_values: Vec<f64> = values.iter().filter(|&&v| !v.is_nan()).cloned().collect();
            
            if valid_values.is_empty() {
                scaled_features.insert(name.clone(), values.clone());
                continue;
            }
            
            let min_val = valid_values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
            let max_val = valid_values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
            
            if max_val == min_val {
                scaled_features.insert(name.clone(), values.clone());
            } else {
                let scaled_values: Vec<f64> = values.iter()
                    .map(|&v| if v.is_nan() { v } else { (v - min_val) / (max_val - min_val) })
                    .collect();
                scaled_features.insert(name.clone(), scaled_values);
            }
        }
        
        Ok(scaled_features)
    }
    
    /// Robust 스케일링
    async fn robust_scale(&self, features: &HashMap<String, Vec<f64>>) -> Result<HashMap<String, Vec<f64>>> {
        let mut scaled_features = HashMap::new();
        
        for (name, values) in features {
            let valid_values: Vec<f64> = values.iter().filter(|&&v| !v.is_nan()).cloned().collect();
            
            if valid_values.is_empty() {
                scaled_features.insert(name.clone(), values.clone());
                continue;
            }
            
            let median = self.calculate_median(&valid_values);
            let mad = self.calculate_mad(&valid_values, median);
            
            if mad == 0.0 {
                scaled_features.insert(name.clone(), values.clone());
            } else {
                let scaled_values: Vec<f64> = values.iter()
                    .map(|&v| if v.is_nan() { v } else { (v - median) / mad })
                    .collect();
                scaled_features.insert(name.clone(), scaled_values);
            }
        }
        
        Ok(scaled_features)
    }
    
    // 유틸리티 함수들
    fn calculate_rolling_mean(&self, values: &[f64], window: usize) -> Vec<f64> {
        let mut rolling_mean = vec![f64::NAN; values.len()];
        
        for i in window - 1..values.len() {
            let window_values: Vec<f64> = values[i - window + 1..=i].to_vec();
            rolling_mean[i] = window_values.iter().sum::<f64>() / window as f64;
        }
        
        rolling_mean
    }
    
    fn calculate_rolling_std(&self, values: &[f64], window: usize) -> Vec<f64> {
        let mut rolling_std = vec![f64::NAN; values.len()];
        
        for i in window - 1..values.len() {
            let window_values: Vec<f64> = values[i - window + 1..=i].to_vec();
            let mean = window_values.iter().sum::<f64>() / window as f64;
            let variance = window_values.iter()
                .map(|v| (v - mean).powi(2))
                .sum::<f64>() / window as f64;
            rolling_std[i] = variance.sqrt();
        }
        
        rolling_std
    }
    
    fn calculate_rolling_max(&self, values: &[f64], window: usize) -> Vec<f64> {
        let mut rolling_max = vec![f64::NAN; values.len()];
        
        for i in window - 1..values.len() {
            rolling_max[i] = values[i - window + 1..=i].iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        }
        
        rolling_max
    }
    
    fn calculate_rolling_min(&self, values: &[f64], window: usize) -> Vec<f64> {
        let mut rolling_min = vec![f64::NAN; values.len()];
        
        for i in window - 1..values.len() {
            rolling_min[i] = values[i - window + 1..=i].iter().fold(f64::INFINITY, |a, &b| a.min(b));
        }
        
        rolling_min
    }
    
    fn calculate_variance(&self, values: &[f64]) -> f64 {
        let valid_values: Vec<f64> = values.iter().filter(|&&v| !v.is_nan()).cloned().collect();
        
        if valid_values.is_empty() {
            return 0.0;
        }
        
        let mean = valid_values.iter().sum::<f64>() / valid_values.len() as f64;
        valid_values.iter()
            .map(|v| (v - mean).powi(2))
            .sum::<f64>() / valid_values.len() as f64
    }
    
    fn calculate_correlation(&self, x: &[f64], y: &[f64]) -> f64 {
        if x.len() != y.len() {
            return 0.0;
        }
        
        let valid_pairs: Vec<(f64, f64)> = x.iter()
            .zip(y.iter())
            .filter(|(&a, &b)| !a.is_nan() && !b.is_nan())
            .map(|(&a, &b)| (a, b))
            .collect();
        
        if valid_pairs.is_empty() {
            return 0.0;
        }
        
        let mean_x = valid_pairs.iter().map(|(x, _)| x).sum::<f64>() / valid_pairs.len() as f64;
        let mean_y = valid_pairs.iter().map(|(_, y)| y).sum::<f64>() / valid_pairs.len() as f64;
        
        let numerator = valid_pairs.iter()
            .map(|(x, y)| (x - mean_x) * (y - mean_y))
            .sum::<f64>();
        
        let sum_sq_x = valid_pairs.iter()
            .map(|(x, _)| (x - mean_x).powi(2))
            .sum::<f64>();
        
        let sum_sq_y = valid_pairs.iter()
            .map(|(_, y)| (y - mean_y).powi(2))
            .sum::<f64>();
        
        if sum_sq_x == 0.0 || sum_sq_y == 0.0 {
            0.0
        } else {
            numerator / (sum_sq_x * sum_sq_y).sqrt()
        }
    }
    
    fn calculate_mutual_information(&self, values: &[f64]) -> f64 {
        // 간단한 상호정보량 계산 (실제로는 더 정교한 구현 필요)
        let variance = self.calculate_variance(values);
        variance / (1.0 + variance) // 정규화된 분산을 상호정보량으로 근사
    }
    
    fn calculate_feature_score(&self, values: &[f64]) -> f64 {
        // 특성 점수 계산 (분산 기반)
        self.calculate_variance(values)
    }
    
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
    
    fn calculate_mad(&self, values: &[f64], median: f64) -> f64 {
        let deviations: Vec<f64> = values.iter()
            .map(|&v| (v - median).abs())
            .collect();
        
        self.calculate_median(&deviations)
    }
}

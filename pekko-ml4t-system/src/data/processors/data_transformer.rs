//! 데이터 변환 모듈
//! 
//! 데이터를 다양한 형태로 변환하는 기능을 제공합니다.
//! - 데이터 타입 변환
//! - 인코딩/디코딩
//! - 피벗 테이블 생성
//! - 데이터 집계

use anyhow::Result;
use std::collections::HashMap;
use tracing::{ debug, warn, error};

/// 데이터 변환기
pub struct DataTransformer {
    config: TransformationConfig,
}

/// 변환 설정
#[derive(Debug, Clone)]
pub struct TransformationConfig {
    pub target_columns: Vec<String>,
    pub transformation_type: TransformationType,
    pub encoding_method: EncodingMethod,
}

/// 변환 타입
#[derive(Debug, Clone)]
pub enum TransformationType {
    Normalize,
    Standardize,
    LogTransform,
    BoxCox,
    QuantileTransform,
    PowerTransform,
}

/// 인코딩 방법
#[derive(Debug, Clone)]
pub enum EncodingMethod {
    OneHot,
    Label,
    Ordinal,
    Target,
    Frequency,
}

impl DataTransformer {
    pub fn new(config: TransformationConfig) -> Self {
        Self { config }
    }
    
    /// 데이터 변환 실행
    pub async fn transform(&self, data: &mut HashMap<String, Vec<f64>>) -> Result<()> {
        for column in &self.config.target_columns {
            if let Some(values) = data.get_mut(column) {
                match self.config.transformation_type {
                    TransformationType::Normalize => {
                        self.normalize_column(values).await?;
                    }
                    TransformationType::Standardize => {
                        self.standardize_column(values).await?;
                    }
                    TransformationType::LogTransform => {
                        self.log_transform_column(values).await?;
                    }
                    TransformationType::BoxCox => {
                        self.box_cox_transform_column(values).await?;
                    }
                    TransformationType::QuantileTransform => {
                        self.quantile_transform_column(values).await?;
                    }
                    TransformationType::PowerTransform => {
                        self.power_transform_column(values).await?;
                    }
                }
            }
        }
        Ok(())
    }
    
    /// 컬럼 정규화 (0-1 범위)
    async fn normalize_column(&self, values: &mut [f64]) -> Result<()> {
        let valid_values: Vec<f64> = values.iter()
            .filter(|&&v| !v.is_nan())
            .cloned()
            .collect();
        
        if valid_values.is_empty() {
            return Ok(());
        }
        
        let min_val = valid_values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max_val = valid_values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        
        if max_val == min_val {
            return Ok(());
        }
        
        for value in values.iter_mut() {
            if !value.is_nan() {
                *value = (*value - min_val) / (max_val - min_val);
            }
        }
        
        Ok(())
    }
    
    /// 컬럼 표준화 (평균 0, 표준편차 1)
    async fn standardize_column(&self, values: &mut [f64]) -> Result<()> {
        let valid_values: Vec<f64> = values.iter()
            .filter(|&&v| !v.is_nan())
            .cloned()
            .collect();
        
        if valid_values.is_empty() {
            return Ok(());
        }
        
        let mean = valid_values.iter().sum::<f64>() / valid_values.len() as f64;
        let variance = valid_values.iter()
            .map(|&v| (v - mean).powi(2))
            .sum::<f64>() / valid_values.len() as f64;
        let std_dev = variance.sqrt();
        
        if std_dev == 0.0 {
            return Ok(());
        }
        
        for value in values.iter_mut() {
            if !value.is_nan() {
                *value = (*value - mean) / std_dev;
            }
        }
        
        Ok(())
    }
    
    /// 로그 변환
    async fn log_transform_column(&self, values: &mut [f64]) -> Result<()> {
        for value in values.iter_mut() {
            if !value.is_nan() && *value > 0.0 {
                *value = value.ln();
            }
        }
        Ok(())
    }
    
    /// Box-Cox 변환
    async fn box_cox_transform_column(&self, values: &mut [f64]) -> Result<()> {
        // 간단한 Box-Cox 변환 (λ = 0.5)
        for value in values.iter_mut() {
            if !value.is_nan() && *value > 0.0 {
                *value = (value.powf(0.5) - 1.0) / 0.5;
            }
        }
        Ok(())
    }
    
    /// 분위수 변환
    async fn quantile_transform_column(&self, values: &mut [f64]) -> Result<()> {
        let mut valid_values: Vec<(usize, f64)> = values.iter()
            .enumerate()
            .filter(|(_, &v)| !v.is_nan())
            .map(|(i, &v)| (i, v))
            .collect();
        
        if valid_values.is_empty() {
            return Ok(());
        }
        
        valid_values.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        
        let n = valid_values.len();
        for (rank, (idx, _)) in valid_values.iter().enumerate() {
            values[*idx] = rank as f64 / (n - 1) as f64;
        }
        
        Ok(())
    }
    
    /// 거듭제곱 변환
    async fn power_transform_column(&self, values: &mut [f64]) -> Result<()> {
        for value in values.iter_mut() {
            if !value.is_nan() {
                *value = value.powf(0.5); // 제곱근 변환
            }
        }
        Ok(())
    }
}

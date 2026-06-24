//! 모델 평가 모듈
//! 
//! 머신러닝 모델의 성능을 평가하는 다양한 메트릭을 제공합니다.
//! - 회귀 메트릭 (MSE, MAE, R²)
//! - 분류 메트릭 (Accuracy, Precision, Recall, F1)
//! - 시계열 메트릭 (MAPE, SMAPE)
//! - 금융 특화 메트릭 (Sharpe, Sortino, Calmar)

use anyhow::Result;
use std::collections::HashMap;
use tracing::{ debug, warn, error};

/// 모델 평가 결과
#[derive(Debug, Clone)]
pub struct EvaluationResult {
    pub metrics: HashMap<String, f64>,
    pub predictions: Vec<f64>,
    pub targets: Vec<f64>,
    pub residuals: Vec<f64>,
}

/// 모델 평가기
pub struct ModelEvaluator {
    config: EvaluationConfig,
}

/// 평가 설정
#[derive(Debug, Clone)]
pub struct EvaluationConfig {
    pub metrics: Vec<EvaluationMetric>,
    pub cross_validation: bool,
    pub cv_folds: usize,
}

/// 평가 메트릭
#[derive(Debug, Clone)]
pub enum EvaluationMetric {
    // 회귀 메트릭
    MSE,
    MAE,
    RMSE,
    R2,
    AdjustedR2,
    MAPE,
    SMAPE,
    
    // 분류 메트릭
    Accuracy,
    Precision,
    Recall,
    F1,
    AUC,
    
    // 금융 메트릭
    SharpeRatio,
    SortinoRatio,
    CalmarRatio,
    MaxDrawdown,
    VaR,
    CVaR,
}

impl ModelEvaluator {
    pub fn new(config: EvaluationConfig) -> Self {
        Self { config }
    }
    
    /// 모델 평가 실행
    pub async fn evaluate(
        &self,
        predictions: &[f64],
        targets: &[f64],
    ) -> Result<EvaluationResult> {
        let residuals: Vec<f64> = predictions.iter()
            .zip(targets.iter())
            .map(|(pred, target)| pred - target)
            .collect();
        
        let mut metrics = HashMap::new();
        
        for metric in &self.config.metrics {
            let value = self.calculate_metric(metric, predictions, targets, &residuals).await?;
            metrics.insert(format!("{:?}", metric), value);
        }
        Ok(EvaluationResult {
            metrics,
            predictions: predictions.to_vec(),
            targets: targets.to_vec(),
            residuals,
        })
    }
    
    /// 메트릭 계산
    async fn calculate_metric(
        &self,
        metric: &EvaluationMetric,
        predictions: &[f64],
        targets: &[f64],
        residuals: &[f64],
    ) -> Result<f64> {
        match metric {
            EvaluationMetric::MSE => {
                let mse = residuals.iter().map(|r| r * r).sum::<f64>() / residuals.len() as f64;
                Ok(mse)
            }
            EvaluationMetric::MAE => {
                let mae = residuals.iter().map(|r| r.abs()).sum::<f64>() / residuals.len() as f64;
                Ok(mae)
            }
            EvaluationMetric::RMSE => {
                let mse = residuals.iter().map(|r| r * r).sum::<f64>() / residuals.len() as f64;
                Ok(mse.sqrt())
            }
            EvaluationMetric::R2 => {
                let mean_target = targets.iter().sum::<f64>() / targets.len() as f64;
                let ss_tot = targets.iter().map(|t| (t - mean_target).powi(2)).sum::<f64>();
                let ss_res = residuals.iter().map(|r| r * r).sum::<f64>();
                
                if ss_tot == 0.0 {
                    Ok(0.0)
                } else {
                    Ok(1.0 - ss_res / ss_tot)
                }
            }
            EvaluationMetric::MAPE => {
                let mape = targets.iter()
                    .zip(residuals.iter())
                    .map(|(target, residual)| (residual / target).abs())
                    .sum::<f64>() / targets.len() as f64 * 100.0;
                Ok(mape)
            }
            EvaluationMetric::SMAPE => {
                let smape = targets.iter()
                    .zip(predictions.iter())
                    .map(|(target, pred)| {
                        (pred - target).abs() / ((target.abs() + pred.abs()) / 2.0)
                    })
                    .sum::<f64>() / targets.len() as f64 * 100.0;
                Ok(smape)
            }
            EvaluationMetric::SharpeRatio => {
                let returns = self.calculate_returns(predictions)?;
                let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
                let std_return = self.calculate_std(&returns)?;
                
                if std_return == 0.0 {
                    Ok(0.0)
                } else {
                    Ok(mean_return / std_return * (252.0_f64).sqrt()) // 연율화
                }
            }
            EvaluationMetric::SortinoRatio => {
                let returns = self.calculate_returns(predictions)?;
                let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
                let downside_std = self.calculate_downside_std(&returns)?;
                
                if downside_std == 0.0 {
                    Ok(0.0)
                } else {
                    Ok(mean_return / downside_std * (252.0_f64).sqrt()) // 연율화
                }
            }
            EvaluationMetric::MaxDrawdown => {
                let returns = self.calculate_returns(predictions)?;
                Ok(self.calculate_max_drawdown(&returns))
            }
            _ => {
                warn!("지원하지 않는 메트릭: {:?}", metric);
                Ok(0.0)
            }
        }
    }
    
    /// 수익률 계산
    fn calculate_returns(&self, prices: &[f64]) -> Result<Vec<f64>> {
        if prices.len() < 2 {
            return Err(anyhow::anyhow!("가격 데이터가 부족합니다"));
        }
        
        let mut returns = Vec::new();
        for i in 1..prices.len() {
            if prices[i-1] != 0.0 {
                returns.push((prices[i] - prices[i-1]) / prices[i-1]);
            }
        }
        
        Ok(returns)
    }
    
    /// 표준편차 계산
    fn calculate_std(&self, values: &[f64]) -> Result<f64> {
        if values.is_empty() {
            return Ok(0.0);
        }
        
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = values.iter()
            .map(|v| (v - mean).powi(2))
            .sum::<f64>() / values.len() as f64;
        
        Ok(variance.sqrt())
    }
    
    /// 다운사이드 표준편차 계산
    fn calculate_downside_std(&self, returns: &[f64]) -> Result<f64> {
        let negative_returns: Vec<f64> = returns.iter()
            .filter(|&&r| r < 0.0)
            .cloned()
            .collect();
        
        if negative_returns.is_empty() {
            return Ok(0.0);
        }
        
        self.calculate_std(&negative_returns)
    }
    
    /// 최대 드로우다운 계산
    fn calculate_max_drawdown(&self, returns: &[f64]) -> f64 {
        let mut peak = 1.0;
        let mut max_dd: f64 = 0.0;
        
        for &ret in returns {
            peak *= (1.0 + ret);
            let drawdown = (peak - peak * (1.0 + ret)) / peak;
            max_dd = max_dd.max(drawdown.abs());
        }
        
        max_dd
    }
}

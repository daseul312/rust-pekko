//! ML 파이프라인 모듈
//! 
//! 머신러닝 워크플로우를 자동화하는 파이프라인을 제공합니다.
//! - 데이터 전처리 파이프라인
//! - 모델 훈련 파이프라인
//! - 예측 파이프라인
//! - 파이프라인 최적화

use crate::ml::MLModel;
use anyhow::Result;
use std::collections::HashMap;
use tracing::{ debug, warn, error};

/// ML 파이프라인
pub struct MLPipeline {
    steps: Vec<PipelineStep>,
    config: PipelineConfig,
}

/// 파이프라인 단계
#[derive(Debug, Clone)]
pub enum PipelineStep {
    DataPreprocessing,
    FeatureEngineering,
    ModelTraining,
    ModelEvaluation,
    ModelPrediction,
}

/// 파이프라인 설정
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub auto_optimize: bool,
    pub cross_validation: bool,
    pub cv_folds: usize,
    pub early_stopping: bool,
    pub patience: usize,
}

/// 파이프라인 실행 결과
pub struct PipelineResult {
    pub model: Box<dyn MLModel>,
    pub metrics: HashMap<String, f64>,
    pub execution_time: f64,
    pub steps_completed: Vec<PipelineStep>,
}

impl MLPipeline {
    pub fn new(config: PipelineConfig) -> Self {
        Self {
            steps: Vec::new(),
            config,
        }
    }
    
    /// 파이프라인 단계 추가
    pub fn add_step(&mut self, step: PipelineStep) {
        self.steps.push(step);
    }
    
    /// 파이프라인 실행
    pub async fn execute(
        &self,
        features: &HashMap<String, Vec<f64>>,
        targets: &[f64],
    ) -> Result<PipelineResult> {
        let start_time = std::time::Instant::now();
        
        let mut processed_features = features.clone();
        let mut processed_targets = targets.to_vec();
        let mut model: Option<Box<dyn MLModel>> = None;
        let mut metrics = HashMap::new();
        let mut completed_steps = Vec::new();
        
        for step in &self.steps {
            match step {
                PipelineStep::DataPreprocessing => {
                    processed_features = self.preprocess_data(&processed_features)?;
                    completed_steps.push(step.clone());
                }
                PipelineStep::FeatureEngineering => {
                    processed_features = self.engineer_features(&processed_features)?;
                    completed_steps.push(step.clone());
                }
                PipelineStep::ModelTraining => {
                    model = Some(self.train_model(&processed_features, &processed_targets)?);
                    completed_steps.push(step.clone());
                }
                PipelineStep::ModelEvaluation => {
                    if let Some(ref trained_model) = model {
                        metrics = self.evaluate_model(trained_model, &processed_features, &processed_targets)?;
                    }
                    completed_steps.push(step.clone());
                }
                PipelineStep::ModelPrediction => {
                    // 예측 단계는 별도로 실행
                    completed_steps.push(step.clone());
                }
            }
        }
        
        let execution_time = start_time.elapsed().as_secs_f64();
        Ok(PipelineResult {
            model: model.ok_or_else(|| anyhow::anyhow!("모델 훈련이 완료되지 않았습니다"))?,
            metrics,
            execution_time,
            steps_completed: completed_steps,
        })
    }
    
    /// 데이터 전처리
    fn preprocess_data(&self, features: &HashMap<String, Vec<f64>>) -> Result<HashMap<String, Vec<f64>>> {
        let mut processed_features = features.clone();
        
        // 결측치 처리
        for (name, values) in &mut processed_features {
            self.fill_missing_values(values);
        }
        
        // 이상치 처리
        for (name, values) in &mut processed_features {
            self.handle_outliers(values);
        }
        
        Ok(processed_features)
    }
    
    /// 특성 엔지니어링
    fn engineer_features(&self, features: &HashMap<String, Vec<f64>>) -> Result<HashMap<String, Vec<f64>>> {
        let mut engineered_features = features.clone();
        
        // 다항식 특성 생성
        let mut new_features = HashMap::new();
        for (name, values) in &engineered_features {
            let squared_values: Vec<f64> = values.iter().map(|v| v * v).collect();
            new_features.insert(format!("{}_squared", name), squared_values);
        }
        
        engineered_features.extend(new_features);
        
        Ok(engineered_features)
    }
    
    /// 모델 훈련
    fn train_model(
        &self,
        features: &HashMap<String, Vec<f64>>,
        targets: &[f64],
    ) -> Result<Box<dyn MLModel>> {
        // 특성을 행렬로 변환
        let feature_matrix = self.features_to_matrix(features);
        
        // 더미 모델 생성 및 훈련
        let mut model = Box::new(DummyModel::new());
        model.train(&feature_matrix, targets)?;
        
        Ok(model)
    }
    
    /// 모델 평가
    fn evaluate_model(
        &self,
        model: &Box<dyn MLModel>,
        features: &HashMap<String, Vec<f64>>,
        targets: &[f64],
    ) -> Result<HashMap<String, f64>> {
        let feature_matrix = self.features_to_matrix(features);
        let predictions = model.predict(&feature_matrix)?;
        
        let mut metrics = HashMap::new();
        
        // MSE 계산
        let mse = predictions.iter()
            .zip(targets.iter())
            .map(|(pred, target)| (pred - target).powi(2))
            .sum::<f64>() / predictions.len() as f64;
        
        metrics.insert("mse".to_string(), mse);
        metrics.insert("rmse".to_string(), mse.sqrt());
        
        // MAE 계산
        let mae = predictions.iter()
            .zip(targets.iter())
            .map(|(pred, target)| (pred - target).abs())
            .sum::<f64>() / predictions.len() as f64;
        
        metrics.insert("mae".to_string(), mae);
        
        // R² 계산
        let mean_target = targets.iter().sum::<f64>() / targets.len() as f64;
        let ss_tot = targets.iter().map(|t| (t - mean_target).powi(2)).sum::<f64>();
        let ss_res = predictions.iter()
            .zip(targets.iter())
            .map(|(pred, target)| (pred - target).powi(2))
            .sum::<f64>();
        
        let r2 = if ss_tot == 0.0 { 0.0 } else { 1.0 - ss_res / ss_tot };
        metrics.insert("r2".to_string(), r2);
        
        Ok(metrics)
    }
    
    /// 결측치 처리
    fn fill_missing_values(&self, values: &mut [f64]) {
        let valid_values: Vec<f64> = values.iter()
            .filter(|&&v| !v.is_nan())
            .cloned()
            .collect();
        
        if !valid_values.is_empty() {
            let mean = valid_values.iter().sum::<f64>() / valid_values.len() as f64;
            for value in values.iter_mut() {
                if value.is_nan() {
                    *value = mean;
                }
            }
        }
    }
    
    /// 이상치 처리
    fn handle_outliers(&self, values: &mut [f64]) {
        let valid_values: Vec<f64> = values.iter()
            .filter(|&&v| !v.is_nan())
            .cloned()
            .collect();
        
        if valid_values.len() < 4 {
            return;
        }
        
        let mut sorted_values = valid_values.clone();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let q1_idx = sorted_values.len() / 4;
        let q3_idx = 3 * sorted_values.len() / 4;
        let q1 = sorted_values[q1_idx];
        let q3 = sorted_values[q3_idx];
        let iqr = q3 - q1;
        
        let lower_bound = q1 - 1.5 * iqr;
        let upper_bound = q3 + 1.5 * iqr;
        
        for value in values.iter_mut() {
            if !value.is_nan() && (*value < lower_bound || *value > upper_bound) {
                // 이상치를 중앙값으로 대체
                let median = sorted_values[sorted_values.len() / 2];
                *value = median;
            }
        }
    }
    
    /// 특성을 행렬로 변환
    fn features_to_matrix(&self, features: &HashMap<String, Vec<f64>>) -> Vec<Vec<f64>> {
        if features.is_empty() {
            return Vec::new();
        }
        
        let feature_names: Vec<String> = features.keys().cloned().collect();
        let n_samples = features.values().next().unwrap().len();
        
        let mut matrix = vec![vec![0.0; feature_names.len()]; n_samples];
        
        for (feature_idx, name) in feature_names.iter().enumerate() {
            if let Some(values) = features.get(name) {
                for (sample_idx, &value) in values.iter().enumerate() {
                    matrix[sample_idx][feature_idx] = value;
                }
            }
        }
        
        matrix
    }
}

/// 더미 모델 (테스트용)
struct DummyModel {
    trained: bool,
}

impl DummyModel {
    fn new() -> Self {
        Self { trained: false }
    }
}

impl MLModel for DummyModel {
    fn name(&self) -> &str {
        "DummyModel"
    }
    
    fn model_type(&self) -> crate::ml::ModelType {
        crate::ml::ModelType::LinearRegression
    }
    
    fn train(&mut self, _features: &[Vec<f64>], _targets: &[f64]) -> Result<crate::ml::TrainingResult> {
        self.trained = true;
        Ok(crate::ml::TrainingResult {
            model_id: "dummy_pipeline".to_string(),
            training_time: 500,
            epochs: 10,
            final_loss: 0.1,
            validation_loss: Some(0.15),
            hyperparameters: HashMap::new(),
            validation_curve: Some(vec![]),
            metrics: crate::ml::ModelMetrics {
                accuracy: 0.95,
                precision: 0.92,
                recall: 0.88,
                f1_score: 0.90,
                mse: 0.05,
                rmse: 0.22,
                mae: 0.03,
                r2: 0.85,
                adjusted_r2: 0.83,
                mape: 0.05,
                smape: 0.05,
                directional_accuracy: Some(0.88),
                sharpe_ratio: Some(1.5),
                information_ratio: Some(0.8),
                max_drawdown: Some(-0.15),
                calmar_ratio: Some(2.0),
            },
            learning_curve: vec![0.5, 0.3, 0.2, 0.15, 0.12, 0.11, 0.105, 0.102, 0.101, 0.1],
            best_epoch: Some(8),
            convergence: true,
        })
    }
    
    fn predict(&self, features: &[Vec<f64>]) -> Result<Vec<f64>> {
        if !self.trained {
            return Err(anyhow::anyhow!("모델이 훈련되지 않았습니다"));
        }
        
        // 더미 예측: 첫 번째 특성의 평균값
        let predictions: Vec<f64> = features.iter()
            .map(|sample| {
                if sample.is_empty() {
                    0.0
                } else {
                    sample.iter().sum::<f64>() / sample.len() as f64
                }
            })
            .collect();
        
        Ok(predictions)
    }
    
    fn save(&self, _path: &str) -> Result<()> {
        Ok(())
    }
    
    fn load(_path: &str) -> Result<Box<dyn MLModel>> {
        Ok(Box::new(Self::new()))
    }
    
    fn get_parameters(&self) -> HashMap<String, f64> {
        let mut params = HashMap::new();
        params.insert("trained".to_string(), if self.trained { 1.0 } else { 0.0 });
        params
    }
    
    fn set_parameters(&mut self, parameters: HashMap<String, f64>) -> Result<()> {
        if let Some(&trained) = parameters.get("trained") {
            self.trained = trained > 0.5;
        }
        Ok(())
    }
}

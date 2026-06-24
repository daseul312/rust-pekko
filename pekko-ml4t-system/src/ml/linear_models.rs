//! 선형 모델 모듈
//! 
//! 다양한 선형 머신러닝 모델들:
//! - 선형 회귀 (Linear Regression)
//! - 릿지 회귀 (Ridge Regression)
//! - 라쏘 회귀 (Lasso Regression)
//! - 엘라스틱넷 (Elastic Net)
//! - 로지스틱 회귀 (Logistic Regression)
//! - 선형 SVM (Linear SVM)

use super::*;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use anyhow::Result;
use tracing::{ warn, error};

/// 선형 회귀 모델
pub struct LinearRegression {
    parameters: HashMap<String, f64>,
    weights: Vec<f64>,
    bias: f64,
    feature_count: usize,
    trained: bool,
    training_metrics: Option<ModelMetrics>,
}

/// 릿지 회귀 모델
pub struct RidgeRegression {
    parameters: HashMap<String, f64>,
    weights: Vec<f64>,
    bias: f64,
    alpha: f64,
    feature_count: usize,
    trained: bool,
    training_metrics: Option<ModelMetrics>,
}

/// 라쏘 회귀 모델
pub struct LassoRegression {
    parameters: HashMap<String, f64>,
    weights: Vec<f64>,
    bias: f64,
    alpha: f64,
    max_iterations: u32,
    tolerance: f64,
    feature_count: usize,
    trained: bool,
    training_metrics: Option<ModelMetrics>,
}

/// 엘라스틱넷 모델
pub struct ElasticNet {
    parameters: HashMap<String, f64>,
    weights: Vec<f64>,
    bias: f64,
    alpha: f64,
    l1_ratio: f64,
    max_iterations: u32,
    tolerance: f64,
    feature_count: usize,
    trained: bool,
    training_metrics: Option<ModelMetrics>,
}

/// 로지스틱 회귀 모델
pub struct LogisticRegression {
    parameters: HashMap<String, f64>,
    weights: Vec<f64>,
    bias: f64,
    feature_count: usize,
    trained: bool,
    training_metrics: Option<ModelMetrics>,
}

/// 선형 SVM 모델
pub struct LinearSVM {
    parameters: HashMap<String, f64>,
    weights: Vec<f64>,
    bias: f64,
    c: f64,
    feature_count: usize,
    trained: bool,
    training_metrics: Option<ModelMetrics>,
}

// LinearRegression 구현

impl LinearRegression {
    /// 새로운 선형 회귀 모델 생성
    pub fn new() -> Self {
        Self {
            parameters: HashMap::new(),
            weights: Vec::new(),
            bias: 0.0,
            feature_count: 0,
            trained: false,
            training_metrics: None,
        }
    }

    /// 정규방정식을 사용한 훈련
    fn train_normal_equation(&mut self, features: &[Vec<f64>], targets: &[f64]) -> Result<()> {
        let n_samples = features.len();
        let n_features = features[0].len();

        // X 행렬 생성 (bias term 포함)
        let mut x_matrix = vec![vec![1.0; n_samples]; n_features + 1];
        for (i, feature_vec) in features.iter().enumerate() {
            for (j, &feature_value) in feature_vec.iter().enumerate() {
                x_matrix[j + 1][i] = feature_value;
            }
        }

        // X^T * X 계산
        let mut xtx = vec![vec![0.0; n_features + 1]; n_features + 1];
        for i in 0..(n_features + 1) {
            for j in 0..(n_features + 1) {
                for k in 0..n_samples {
                    xtx[i][j] += x_matrix[i][k] * x_matrix[j][k];
                }
            }
        }

        // X^T * y 계산
        let mut xty = vec![0.0; n_features + 1];
        for i in 0..(n_features + 1) {
            for k in 0..n_samples {
                xty[i] += x_matrix[i][k] * targets[k];
            }
        }

        // (X^T * X)^(-1) * X^T * y 계산 (단순화된 가우스 소거법)
        let theta = self.solve_linear_system(&xtx, &xty)?;

        // 가중치 설정
        self.bias = theta[0];
        self.weights = theta[1..].to_vec();
        self.feature_count = n_features;
        self.trained = true;

        Ok(())
    }

    /// 선형 시스템 해결 (단순화된 구현)
    fn solve_linear_system(&self, matrix: &[Vec<f64>], vector: &[f64]) -> Result<Vec<f64>> {
        let n = matrix.len();
        let mut augmented = vec![vec![0.0; n + 1]; n];
        
        // 확대 행렬 생성
        for i in 0..n {
            for j in 0..n {
                augmented[i][j] = matrix[i][j];
            }
            augmented[i][n] = vector[i];
        }

        // 가우스 소거법 (전진 소거)
        for i in 0..n {
            // 피벗 찾기
            let mut max_row = i;
            for k in (i + 1)..n {
                if augmented[k][i].abs() > augmented[max_row][i].abs() {
                    max_row = k;
                }
            }

            // 행 교환
            augmented.swap(i, max_row);

            // 특이 행렬 확인
            if augmented[i][i].abs() < 1e-10 {
                return Err(anyhow::anyhow!("특이 행렬: 해를 찾을 수 없습니다"));
            }

            // 소거
            for k in (i + 1)..n {
                let factor = augmented[k][i] / augmented[i][i];
                for j in i..(n + 1) {
                    augmented[k][j] -= factor * augmented[i][j];
                }
            }
        }

        // 후진 대입
        let mut solution = vec![0.0; n];
        for i in (0..n).rev() {
            solution[i] = augmented[i][n];
            for j in (i + 1)..n {
                solution[i] -= augmented[i][j] * solution[j];
            }
            solution[i] /= augmented[i][i];
        }

        Ok(solution)
    }
}

impl MLModel for LinearRegression {
    fn name(&self) -> &str {
        "LinearRegression"
    }

    fn model_type(&self) -> ModelType {
        ModelType::LinearRegression
    }

    fn train(&mut self, features: &[Vec<f64>], targets: &[f64]) -> Result<TrainingResult> {
        if features.is_empty() || features[0].is_empty() {
            return Err(anyhow::anyhow!("훈련 데이터가 비어있습니다"));
        }

        let start_time = std::time::Instant::now();
        
        // 정규방정식을 사용한 훈련
        self.train_normal_equation(features, targets)?;

        // 훈련 지표 계산
        let predictions = self.predict(features)?;
        let mut metrics = ModelMetrics::new();
        metrics.calculate(&predictions, targets);
        self.training_metrics = Some(metrics.clone());

        let training_time = start_time.elapsed().as_millis() as u64;
        Ok(TrainingResult {
            model_id: format!("linear_regression_{}", chrono::Utc::now().timestamp()),
            training_time,
            epochs: 1, // 정규방정식은 한 번에 해결
            final_loss: metrics.mse,
            validation_loss: None,
            metrics: metrics.clone(),
            convergence: true,
            best_epoch: Some(1),
            learning_curve: vec![metrics.clone().mse],
            validation_curve: None,
            hyperparameters: self.parameters.clone(),
        })
    }

    fn predict(&self, features: &[Vec<f64>]) -> Result<Vec<f64>> {
        if !self.trained {
            return Err(anyhow::anyhow!("모델이 훈련되지 않았습니다"));
        }

        if features.is_empty() {
            return Ok(Vec::new());
        }

        if features[0].len() != self.feature_count {
            return Err(anyhow::anyhow!("특성 수가 일치하지 않습니다"));
        }

        let mut predictions = Vec::new();
        
        for feature_vec in features {
            let mut prediction = self.bias;
            for (i, &feature_value) in feature_vec.iter().enumerate() {
                if i < self.weights.len() {
                    prediction += self.weights[i] * feature_value;
                }
            }
            predictions.push(prediction);
        }

        Ok(predictions)
    }

    fn save(&self, path: &str) -> Result<()> {
        let model_data = LinearRegressionData {
            weights: self.weights.clone(),
            bias: self.bias,
            feature_count: self.feature_count,
            parameters: self.parameters.clone(),
        };

        let serialized = serde_json::to_string(&model_data)?;
        std::fs::write(path, serialized)?;
        
        Ok(())
    }

    fn load(path: &str) -> Result<Box<dyn MLModel>> {
        let content = std::fs::read_to_string(path)?;
        let model_data: LinearRegressionData = serde_json::from_str(&content)?;
        
        let mut model = LinearRegression::new();
        model.weights = model_data.weights;
        model.bias = model_data.bias;
        model.feature_count = model_data.feature_count;
        model.parameters = model_data.parameters;
        model.trained = true;
        
        Ok(Box::new(model))
    }

    fn get_parameters(&self) -> HashMap<String, f64> {
        self.parameters.clone()
    }

    fn set_parameters(&mut self, parameters: HashMap<String, f64>) -> Result<()> {
        self.parameters = parameters.clone();
        Ok(())
    }

    fn explainability_score(&self) -> f64 {
        1.0 // 선형 모델은 완전히 설명 가능
    }

    fn complexity_score(&self) -> f64 {
        0.1 // 매우 단순
    }

    fn inference_speed(&self) -> f64 {
        0.1 // 매우 빠름
    }
}

// RidgeRegression 구현

impl RidgeRegression {
    /// 새로운 릿지 회귀 모델 생성
    pub fn new(alpha: f64) -> Self {
        let mut parameters = HashMap::new();
        parameters.insert("alpha".to_string(), alpha);
        
        Self {
            parameters,
            weights: Vec::new(),
            bias: 0.0,
            alpha,
            feature_count: 0,
            trained: false,
            training_metrics: None,
        }
    }

    /// 릿지 회귀 훈련 (정규화된 정규방정식)
    fn train_ridge(&mut self, features: &[Vec<f64>], targets: &[f64]) -> Result<()> {
        let n_samples = features.len();
        let n_features = features[0].len();

        // X 행렬 생성
        let mut x_matrix = vec![vec![0.0; n_features]; n_samples];
        for (i, feature_vec) in features.iter().enumerate() {
            for (j, &feature_value) in feature_vec.iter().enumerate() {
                x_matrix[i][j] = feature_value;
            }
        }

        // X^T * X + alpha * I 계산
        let mut xtx_regularized = vec![vec![0.0; n_features]; n_features];
        for i in 0..n_features {
            for j in 0..n_features {
                for k in 0..n_samples {
                    xtx_regularized[i][j] += x_matrix[k][i] * x_matrix[k][j];
                }
                // 정규화 항 추가 (bias는 제외)
                if i == j {
                    xtx_regularized[i][j] += self.alpha;
                }
            }
        }

        // X^T * y 계산
        let mut xty = vec![0.0; n_features];
        for i in 0..n_features {
            for k in 0..n_samples {
                xty[i] += x_matrix[k][i] * targets[k];
            }
        }

        // 정규화된 정규방정식 해결
        let weights = self.solve_linear_system(&xtx_regularized, &xty)?;

        // bias 계산 (평균)
        let mut bias_sum = 0.0;
        for (i, feature_vec) in features.iter().enumerate() {
            let mut prediction = 0.0;
            for (j, &feature_value) in feature_vec.iter().enumerate() {
                prediction += weights[j] * feature_value;
            }
            bias_sum += targets[i] - prediction;
        }
        self.bias = bias_sum / n_samples as f64;

        self.weights = weights;
        self.feature_count = n_features;
        self.trained = true;

        Ok(())
    }

    fn solve_linear_system(&self, matrix: &[Vec<f64>], vector: &[f64]) -> Result<Vec<f64>> {
        // LinearRegression과 동일한 구현 재사용
        let n = matrix.len();
        let mut augmented = vec![vec![0.0; n + 1]; n];
        
        for i in 0..n {
            for j in 0..n {
                augmented[i][j] = matrix[i][j];
            }
            augmented[i][n] = vector[i];
        }

        for i in 0..n {
            let mut max_row = i;
            for k in (i + 1)..n {
                if augmented[k][i].abs() > augmented[max_row][i].abs() {
                    max_row = k;
                }
            }

            augmented.swap(i, max_row);

            if augmented[i][i].abs() < 1e-10 {
                return Err(anyhow::anyhow!("특이 행렬: 해를 찾을 수 없습니다"));
            }

            for k in (i + 1)..n {
                let factor = augmented[k][i] / augmented[i][i];
                for j in i..(n + 1) {
                    augmented[k][j] -= factor * augmented[i][j];
                }
            }
        }

        let mut solution = vec![0.0; n];
        for i in (0..n).rev() {
            solution[i] = augmented[i][n];
            for j in (i + 1)..n {
                solution[i] -= augmented[i][j] * solution[j];
            }
            solution[i] /= augmented[i][i];
        }

        Ok(solution)
    }
}

impl MLModel for RidgeRegression {
    fn name(&self) -> &str {
        "RidgeRegression"
    }

    fn model_type(&self) -> ModelType {
        ModelType::RidgeRegression
    }

    fn train(&mut self, features: &[Vec<f64>], targets: &[f64]) -> Result<TrainingResult> {
        if features.is_empty() || features[0].is_empty() {
            return Err(anyhow::anyhow!("훈련 데이터가 비어있습니다"));
        }

        let start_time = std::time::Instant::now();
        
        self.train_ridge(features, targets)?;

        let predictions = self.predict(features)?;
        let mut metrics = ModelMetrics::new();
        metrics.calculate(&predictions, targets);
        self.training_metrics = Some(metrics.clone());

        let training_time = start_time.elapsed().as_millis() as u64;
        Ok(TrainingResult {
            model_id: format!("ridge_regression_{}", chrono::Utc::now().timestamp()),
            training_time,
            epochs: 1,
            final_loss: metrics.mse,
            validation_loss: None,
            metrics: metrics.clone(),
            convergence: true,
            best_epoch: Some(1),
            learning_curve: vec![metrics.clone().mse],
            validation_curve: None,
            hyperparameters: self.parameters.clone(),
        })
    }

    fn predict(&self, features: &[Vec<f64>]) -> Result<Vec<f64>> {
        if !self.trained {
            return Err(anyhow::anyhow!("모델이 훈련되지 않았습니다"));
        }

        if features.is_empty() {
            return Ok(Vec::new());
        }

        if features[0].len() != self.feature_count {
            return Err(anyhow::anyhow!("특성 수가 일치하지 않습니다"));
        }

        let mut predictions = Vec::new();
        
        for feature_vec in features {
            let mut prediction = self.bias;
            for (i, &feature_value) in feature_vec.iter().enumerate() {
                if i < self.weights.len() {
                    prediction += self.weights[i] * feature_value;
                }
            }
            predictions.push(prediction);
        }

        Ok(predictions)
    }

    fn save(&self, path: &str) -> Result<()> {
        let model_data = RidgeRegressionData {
            weights: self.weights.clone(),
            bias: self.bias,
            alpha: self.alpha,
            feature_count: self.feature_count,
            parameters: self.parameters.clone(),
        };

        let serialized = serde_json::to_string(&model_data)?;
        std::fs::write(path, serialized)?;
        
        Ok(())
    }

    fn load(path: &str) -> Result<Box<dyn MLModel>> {
        let content = std::fs::read_to_string(path)?;
        let model_data: RidgeRegressionData = serde_json::from_str(&content)?;
        
        let mut model = RidgeRegression::new(model_data.alpha);
        model.weights = model_data.weights;
        model.bias = model_data.bias;
        model.feature_count = model_data.feature_count;
        model.parameters = model_data.parameters;
        model.trained = true;
        
        Ok(Box::new(model))
    }

    fn get_parameters(&self) -> HashMap<String, f64> {
        self.parameters.clone()
    }

    fn set_parameters(&mut self, parameters: HashMap<String, f64>) -> Result<()> {
        self.parameters = parameters.clone();
        if let Some(&alpha) = parameters.get("alpha") {
            self.alpha = alpha;
        }
        Ok(())
    }

    fn explainability_score(&self) -> f64 {
        1.0 // 선형 모델은 완전히 설명 가능
    }

    fn complexity_score(&self) -> f64 {
        0.2 // 릿지 정규화로 약간 복잡
    }

    fn inference_speed(&self) -> f64 {
        0.1 // 매우 빠름
    }
}

// LassoRegression 구현 (좌표 하강법)

impl LassoRegression {
    /// 새로운 라쏘 회귀 모델 생성
    pub fn new(alpha: f64, max_iterations: u32, tolerance: f64) -> Self {
        let mut parameters = HashMap::new();
        parameters.insert("alpha".to_string(), alpha);
        parameters.insert("max_iterations".to_string(), max_iterations as f64);
        parameters.insert("tolerance".to_string(), tolerance);
        
        Self {
            parameters,
            weights: Vec::new(),
            bias: 0.0,
            alpha,
            max_iterations,
            tolerance,
            feature_count: 0,
            trained: false,
            training_metrics: None,
        }
    }

    /// 좌표 하강법을 사용한 라쏘 훈련
    fn train_lasso(&mut self, features: &[Vec<f64>], targets: &[f64]) -> Result<()> {
        let n_samples = features.len();
        let n_features = features[0].len();

        // 데이터 전처리 (정규화)
        let (normalized_features, feature_means, feature_stds) = self.normalize_features(features);
        let target_mean = targets.iter().sum::<f64>() / n_samples as f64;
        let normalized_targets: Vec<f64> = targets.iter().map(|&t| t - target_mean).collect();

        // 초기화
        let mut weights = vec![0.0; n_features];
        let mut prev_weights = vec![f64::INFINITY; n_features];

        // 좌표 하강법
        for iteration in 0..self.max_iterations {
            let mut converged = true;

            for j in 0..n_features {
                // j번째 가중치 업데이트
                let mut rho_j = 0.0;
                for i in 0..n_samples {
                    let mut prediction = 0.0;
                    for k in 0..n_features {
                        if k != j {
                            prediction += weights[k] * normalized_features[i][k];
                        }
                    }
                    rho_j += normalized_features[i][j] * (normalized_targets[i] - prediction);
                }

                rho_j /= n_samples as f64;

                // Soft thresholding
                let new_weight = if rho_j > self.alpha {
                    (rho_j - self.alpha) / (1.0 + 1e-8) // 수치적 안정성을 위한 작은 값 추가
                } else if rho_j < -self.alpha {
                    (rho_j + self.alpha) / (1.0 + 1e-8)
                } else {
                    0.0
                };

                if (new_weight - weights[j]).abs() > self.tolerance {
                    converged = false;
                }

                weights[j] = new_weight;
            }

            // 수렴 확인
            if converged {
                break;
            }

            prev_weights = weights.clone();
        }

        // bias 계산
        let mut bias_sum = 0.0;
        for i in 0..n_samples {
            let mut prediction = 0.0;
            for j in 0..n_features {
                prediction += weights[j] * normalized_features[i][j];
            }
            bias_sum += normalized_targets[i] - prediction;
        }
        self.bias = bias_sum / n_samples as f64 + target_mean;

        // 가중치를 원래 스케일로 변환
        for j in 0..n_features {
            if feature_stds[j] > 1e-10 {
                weights[j] /= feature_stds[j];
            }
        }

        self.weights = weights;
        self.feature_count = n_features;
        self.trained = true;

        Ok(())
    }

    /// 특성 정규화
    fn normalize_features(&self, features: &[Vec<f64>]) -> (Vec<Vec<f64>>, Vec<f64>, Vec<f64>) {
        let n_samples = features.len();
        let n_features = features[0].len();

        let mut normalized = vec![vec![0.0; n_features]; n_samples];
        let mut means = vec![0.0; n_features];
        let mut stds = vec![0.0; n_features];

        // 평균 계산
        for j in 0..n_features {
            for i in 0..n_samples {
                means[j] += features[i][j];
            }
            means[j] /= n_samples as f64;
        }

        // 표준편차 계산 및 정규화
        for j in 0..n_features {
            let mut variance = 0.0;
            for i in 0..n_samples {
                let diff = features[i][j] - means[j];
                variance += diff * diff;
                normalized[i][j] = diff;
            }
            stds[j] = (variance / n_samples as f64).sqrt();
            
            if stds[j] > 1e-10 {
                for i in 0..n_samples {
                    normalized[i][j] /= stds[j];
                }
            }
        }

        (normalized, means, stds)
    }
}

impl MLModel for LassoRegression {
    fn name(&self) -> &str {
        "LassoRegression"
    }

    fn model_type(&self) -> ModelType {
        ModelType::LassoRegression
    }

    fn train(&mut self, features: &[Vec<f64>], targets: &[f64]) -> Result<TrainingResult> {
        if features.is_empty() || features[0].is_empty() {
            return Err(anyhow::anyhow!("훈련 데이터가 비어있습니다"));
        }

        let start_time = std::time::Instant::now();
        
        self.train_lasso(features, targets)?;

        let predictions = self.predict(features)?;
        let mut metrics = ModelMetrics::new();
        metrics.calculate(&predictions, targets);
        self.training_metrics = Some(metrics.clone());

        let training_time = start_time.elapsed().as_millis() as u64;
        Ok(TrainingResult {
            model_id: format!("lasso_regression_{}", chrono::Utc::now().timestamp()),
            training_time,
            epochs: self.max_iterations,
            final_loss: metrics.mse,
            validation_loss: None,
            metrics: metrics.clone(),
            convergence: true,
            best_epoch: Some(self.max_iterations),
            learning_curve: vec![metrics.clone().mse],
            validation_curve: None,
            hyperparameters: self.parameters.clone(),
        })
    }

    fn predict(&self, features: &[Vec<f64>]) -> Result<Vec<f64>> {
        if !self.trained {
            return Err(anyhow::anyhow!("모델이 훈련되지 않았습니다"));
        }

        if features.is_empty() {
            return Ok(Vec::new());
        }

        if features[0].len() != self.feature_count {
            return Err(anyhow::anyhow!("특성 수가 일치하지 않습니다"));
        }

        let mut predictions = Vec::new();
        
        for feature_vec in features {
            let mut prediction = self.bias;
            for (i, &feature_value) in feature_vec.iter().enumerate() {
                if i < self.weights.len() {
                    prediction += self.weights[i] * feature_value;
                }
            }
            predictions.push(prediction);
        }

        Ok(predictions)
    }

    fn save(&self, path: &str) -> Result<()> {
        let model_data = LassoRegressionData {
            weights: self.weights.clone(),
            bias: self.bias,
            alpha: self.alpha,
            max_iterations: self.max_iterations,
            tolerance: self.tolerance,
            feature_count: self.feature_count,
            parameters: self.parameters.clone(),
        };

        let serialized = serde_json::to_string(&model_data)?;
        std::fs::write(path, serialized)?;
        
        Ok(())
    }

    fn load(path: &str) -> Result<Box<dyn MLModel>> {
        let content = std::fs::read_to_string(path)?;
        let model_data: LassoRegressionData = serde_json::from_str(&content)?;
        
        let mut model = LassoRegression::new(model_data.alpha, model_data.max_iterations, model_data.tolerance);
        model.weights = model_data.weights;
        model.bias = model_data.bias;
        model.feature_count = model_data.feature_count;
        model.parameters = model_data.parameters;
        model.trained = true;
        
        Ok(Box::new(model))
    }

    fn get_parameters(&self) -> HashMap<String, f64> {
        self.parameters.clone()
    }

    fn set_parameters(&mut self, parameters: HashMap<String, f64>) -> Result<()> {
        self.parameters = parameters.clone();
        if let Some(&alpha) = self.parameters.get("alpha") {
            self.alpha = alpha;
        }
        if let Some(&max_iter) = self.parameters.get("max_iterations") {
            self.max_iterations = max_iter as u32;
        }
        if let Some(&tol) = self.parameters.get("tolerance") {
            self.tolerance = tol;
        }
        Ok(())
    }

    fn get_feature_importance(&self) -> Option<Vec<f64>> {
        Some(self.weights.iter().map(|w| w.abs()).collect())
    }

    fn explainability_score(&self) -> f64 {
        0.9 // L1 정규화로 일부 특성이 0이 될 수 있음
    }

    fn complexity_score(&self) -> f64 {
        0.3 // 좌표 하강법으로 약간 복잡
    }

    fn inference_speed(&self) -> f64 {
        0.1 // 매우 빠름
    }
}

// ElasticNet 구현 (Lasso와 Ridge의 조합)

impl ElasticNet {
    /// 새로운 엘라스틱넷 모델 생성
    pub fn new(alpha: f64, l1_ratio: f64, max_iterations: u32, tolerance: f64) -> Self {
        let mut parameters = HashMap::new();
        parameters.insert("alpha".to_string(), alpha);
        parameters.insert("l1_ratio".to_string(), l1_ratio);
        parameters.insert("max_iterations".to_string(), max_iterations as f64);
        parameters.insert("tolerance".to_string(), tolerance);
        
        Self {
            parameters,
            weights: Vec::new(),
            bias: 0.0,
            alpha,
            l1_ratio,
            max_iterations,
            tolerance,
            feature_count: 0,
            trained: false,
            training_metrics: None,
        }
    }

    /// 엘라스틱넷 훈련 (좌표 하강법 + L1/L2 정규화)
    fn train_elastic_net(&mut self, features: &[Vec<f64>], targets: &[f64]) -> Result<()> {
        let n_samples = features.len();
        let n_features = features[0].len();

        // 데이터 전처리
        let (normalized_features, feature_means, feature_stds) = self.normalize_features(features);
        let target_mean = targets.iter().sum::<f64>() / n_samples as f64;
        let normalized_targets: Vec<f64> = targets.iter().map(|&t| t - target_mean).collect();

        // 정규화 파라미터 계산
        let l1_penalty = self.alpha * self.l1_ratio;
        let l2_penalty = self.alpha * (1.0 - self.l1_ratio);

        // 초기화
        let mut weights = vec![0.0; n_features];

        // 좌표 하강법
        for iteration in 0..self.max_iterations {
            let mut converged = true;

            for j in 0..n_features {
                // j번째 가중치 업데이트
                let mut rho_j = 0.0;
                for i in 0..n_samples {
                    let mut prediction = 0.0;
                    for k in 0..n_features {
                        if k != j {
                            prediction += weights[k] * normalized_features[i][k];
                        }
                    }
                    rho_j += normalized_features[i][j] * (normalized_targets[i] - prediction);
                }

                rho_j /= n_samples as f64;

                // Elastic Net 업데이트 (L1 + L2 정규화)
                let denominator = 1.0 + l2_penalty;
                let new_weight = if rho_j > l1_penalty {
                    (rho_j - l1_penalty) / denominator
                } else if rho_j < -l1_penalty {
                    (rho_j + l1_penalty) / denominator
                } else {
                    0.0
                };

                if (new_weight - weights[j]).abs() > self.tolerance {
                    converged = false;
                }

                weights[j] = new_weight;
            }

            if converged {
                break;
            }
        }

        // bias 계산
        let mut bias_sum = 0.0;
        for i in 0..n_samples {
            let mut prediction = 0.0;
            for j in 0..n_features {
                prediction += weights[j] * normalized_features[i][j];
            }
            bias_sum += normalized_targets[i] - prediction;
        }
        self.bias = bias_sum / n_samples as f64 + target_mean;

        // 가중치를 원래 스케일로 변환
        for j in 0..n_features {
            if feature_stds[j] > 1e-10 {
                weights[j] /= feature_stds[j];
            }
        }

        self.weights = weights;
        self.feature_count = n_features;
        self.trained = true;

        Ok(())
    }

    fn normalize_features(&self, features: &[Vec<f64>]) -> (Vec<Vec<f64>>, Vec<f64>, Vec<f64>) {
        let n_samples = features.len();
        let n_features = features[0].len();

        let mut normalized = vec![vec![0.0; n_features]; n_samples];
        let mut means = vec![0.0; n_features];
        let mut stds = vec![0.0; n_features];

        // 평균 계산
        for j in 0..n_features {
            for i in 0..n_samples {
                means[j] += features[i][j];
            }
            means[j] /= n_samples as f64;
        }

        // 표준편차 계산 및 정규화
        for j in 0..n_features {
            let mut variance = 0.0;
            for i in 0..n_samples {
                let diff = features[i][j] - means[j];
                variance += diff * diff;
                normalized[i][j] = diff;
            }
            stds[j] = (variance / n_samples as f64).sqrt();
            
            if stds[j] > 1e-10 {
                for i in 0..n_samples {
                    normalized[i][j] /= stds[j];
                }
            }
        }

        (normalized, means, stds)
    }
}

impl MLModel for ElasticNet {
    fn name(&self) -> &str {
        "ElasticNet"
    }

    fn model_type(&self) -> ModelType {
        ModelType::ElasticNet
    }

    fn train(&mut self, features: &[Vec<f64>], targets: &[f64]) -> Result<TrainingResult> {
        if features.is_empty() || features[0].is_empty() {
            return Err(anyhow::anyhow!("훈련 데이터가 비어있습니다"));
        }

        let start_time = std::time::Instant::now();
        
        self.train_elastic_net(features, targets)?;

        let predictions = self.predict(features)?;
        let mut metrics = ModelMetrics::new();
        metrics.calculate(&predictions, targets);
        self.training_metrics = Some(metrics.clone());

        let training_time = start_time.elapsed().as_millis() as u64;
        Ok(TrainingResult {
            model_id: format!("elastic_net_{}", chrono::Utc::now().timestamp()),
            training_time,
            epochs: self.max_iterations,
            final_loss: metrics.mse,
            validation_loss: None,
            metrics: metrics.clone(),
            convergence: true,
            best_epoch: Some(self.max_iterations),
            learning_curve: vec![metrics.clone().mse],
            validation_curve: None,
            hyperparameters: self.parameters.clone(),
        })
    }

    fn predict(&self, features: &[Vec<f64>]) -> Result<Vec<f64>> {
        if !self.trained {
            return Err(anyhow::anyhow!("모델이 훈련되지 않았습니다"));
        }

        if features.is_empty() {
            return Ok(Vec::new());
        }

        if features[0].len() != self.feature_count {
            return Err(anyhow::anyhow!("특성 수가 일치하지 않습니다"));
        }

        let mut predictions = Vec::new();
        
        for feature_vec in features {
            let mut prediction = self.bias;
            for (i, &feature_value) in feature_vec.iter().enumerate() {
                if i < self.weights.len() {
                    prediction += self.weights[i] * feature_value;
                }
            }
            predictions.push(prediction);
        }

        Ok(predictions)
    }

    fn save(&self, path: &str) -> Result<()> {
        let model_data = ElasticNetData {
            weights: self.weights.clone(),
            bias: self.bias,
            alpha: self.alpha,
            l1_ratio: self.l1_ratio,
            max_iterations: self.max_iterations,
            tolerance: self.tolerance,
            feature_count: self.feature_count,
            parameters: self.parameters.clone(),
        };

        let serialized = serde_json::to_string(&model_data)?;
        std::fs::write(path, serialized)?;
        
        Ok(())
    }

    fn load(path: &str) -> Result<Box<dyn MLModel>> {
        let content = std::fs::read_to_string(path)?;
        let model_data: ElasticNetData = serde_json::from_str(&content)?;
        
        let mut model = ElasticNet::new(model_data.alpha, model_data.l1_ratio, model_data.max_iterations, model_data.tolerance);
        model.weights = model_data.weights;
        model.bias = model_data.bias;
        model.feature_count = model_data.feature_count;
        model.parameters = model_data.parameters;
        model.trained = true;
        
        Ok(Box::new(model))
    }

    fn get_parameters(&self) -> HashMap<String, f64> {
        self.parameters.clone()
    }

    fn set_parameters(&mut self, parameters: HashMap<String, f64>) -> Result<()> {
        self.parameters = parameters.clone();
        if let Some(&alpha) = self.parameters.get("alpha") {
            self.alpha = alpha;
        }
        if let Some(&l1_ratio) = self.parameters.get("l1_ratio") {
            self.l1_ratio = l1_ratio;
        }
        if let Some(&max_iter) = self.parameters.get("max_iterations") {
            self.max_iterations = max_iter as u32;
        }
        if let Some(&tol) = self.parameters.get("tolerance") {
            self.tolerance = tol;
        }
        Ok(())
    }

    fn get_feature_importance(&self) -> Option<Vec<f64>> {
        Some(self.weights.iter().map(|w| w.abs()).collect())
    }

    fn explainability_score(&self) -> f64 {
        0.9 // L1 정규화로 일부 특성이 0이 될 수 있음
    }

    fn complexity_score(&self) -> f64 {
        0.4 // L1 + L2 정규화로 약간 복잡
    }

    fn inference_speed(&self) -> f64 {
        0.1 // 매우 빠름
    }
}

// 로지스틱 회귀와 선형 SVM은 간단한 구현

impl LogisticRegression {
    pub fn new() -> Self {
        Self {
            parameters: HashMap::new(),
            weights: Vec::new(),
            bias: 0.0,
            feature_count: 0,
            trained: false,
            training_metrics: None,
        }
    }
}

impl MLModel for LogisticRegression {
    fn name(&self) -> &str {
        "LogisticRegression"
    }

    fn model_type(&self) -> ModelType {
        ModelType::LinearRegression // 임시로 LinearRegression으로 설정
    }

    fn train(&mut self, features: &[Vec<f64>], targets: &[f64]) -> Result<TrainingResult> {
        // 간단한 구현: 선형 회귀를 사용
        let mut linear_model = LinearRegression::new();
        let result = linear_model.train(features, targets)?;
        
        self.weights = linear_model.weights;
        self.bias = linear_model.bias;
        self.feature_count = linear_model.feature_count;
        self.trained = true;
        
        Ok(result)
    }

    fn predict(&self, features: &[Vec<f64>]) -> Result<Vec<f64>> {
        if !self.trained {
            return Err(anyhow::anyhow!("모델이 훈련되지 않았습니다"));
        }

        let mut predictions = Vec::new();
        
        for feature_vec in features {
            let mut logit = self.bias;
            for (i, &feature_value) in feature_vec.iter().enumerate() {
                if i < self.weights.len() {
                    logit += self.weights[i] * feature_value;
                }
            }
            // 시그모이드 함수 적용
            let probability = 1.0 / (1.0 + (-logit).exp());
            predictions.push(probability);
        }

        Ok(predictions)
    }

    fn predict_proba(&self, features: &[Vec<f64>]) -> Result<Vec<Vec<f64>>> {
        let probabilities = self.predict(features)?;
        Ok(probabilities.into_iter().map(|p| vec![1.0 - p, p]).collect())
    }

    fn save(&self, path: &str) -> Result<()> {
        let model_data = LogisticRegressionData {
            weights: self.weights.clone(),
            bias: self.bias,
            feature_count: self.feature_count,
            parameters: self.parameters.clone(),
        };

        let serialized = serde_json::to_string(&model_data)?;
        std::fs::write(path, serialized)?;
        
        Ok(())
    }

    fn load(path: &str) -> Result<Box<dyn MLModel>> {
        let content = std::fs::read_to_string(path)?;
        let model_data: LogisticRegressionData = serde_json::from_str(&content)?;
        
        let mut model = LogisticRegression::new();
        model.weights = model_data.weights;
        model.bias = model_data.bias;
        model.feature_count = model_data.feature_count;
        model.parameters = model_data.parameters;
        model.trained = true;
        
        Ok(Box::new(model))
    }

    fn get_parameters(&self) -> HashMap<String, f64> {
        self.parameters.clone()
    }

    fn set_parameters(&mut self, parameters: HashMap<String, f64>) -> Result<()> {
        self.parameters = parameters.clone();
        Ok(())
    }

    fn explainability_score(&self) -> f64 {
        1.0
    }

    fn complexity_score(&self) -> f64 {
        0.2
    }

    fn inference_speed(&self) -> f64 {
        0.1
    }
}

impl LinearSVM {
    pub fn new(c: f64) -> Self {
        let mut parameters = HashMap::new();
        parameters.insert("c".to_string(), c);
        
        Self {
            parameters,
            weights: Vec::new(),
            bias: 0.0,
            c,
            feature_count: 0,
            trained: false,
            training_metrics: None,
        }
    }
}

impl MLModel for LinearSVM {
    fn name(&self) -> &str {
        "LinearSVM"
    }

    fn model_type(&self) -> ModelType {
        ModelType::LinearRegression // 임시로 LinearRegression으로 설정
    }

    fn train(&mut self, features: &[Vec<f64>], targets: &[f64]) -> Result<TrainingResult> {
        // 간단한 구현: 릿지 회귀를 사용
        let mut ridge_model = RidgeRegression::new(self.c);
        let result = ridge_model.train(features, targets)?;
        
        self.weights = ridge_model.weights;
        self.bias = ridge_model.bias;
        self.feature_count = ridge_model.feature_count;
        self.trained = true;
        
        Ok(result)
    }

    fn predict(&self, features: &[Vec<f64>]) -> Result<Vec<f64>> {
        if !self.trained {
            return Err(anyhow::anyhow!("모델이 훈련되지 않았습니다"));
        }

        let mut predictions = Vec::new();
        
        for feature_vec in features {
            let mut score = self.bias;
            for (i, &feature_value) in feature_vec.iter().enumerate() {
                if i < self.weights.len() {
                    score += self.weights[i] * feature_value;
                }
            }
            // 하드 마진: 점수의 부호로 분류
            predictions.push(if score > 0.0 { 1.0 } else { -1.0 });
        }

        Ok(predictions)
    }

    fn save(&self, path: &str) -> Result<()> {
        let model_data = LinearSVMData {
            weights: self.weights.clone(),
            bias: self.bias,
            c: self.c,
            feature_count: self.feature_count,
            parameters: self.parameters.clone(),
        };

        let serialized = serde_json::to_string(&model_data)?;
        std::fs::write(path, serialized)?;
        
        Ok(())
    }

    fn load(path: &str) -> Result<Box<dyn MLModel>> {
        let content = std::fs::read_to_string(path)?;
        let model_data: LinearSVMData = serde_json::from_str(&content)?;
        
        let mut model = LinearSVM::new(model_data.c);
        model.weights = model_data.weights;
        model.bias = model_data.bias;
        model.feature_count = model_data.feature_count;
        model.parameters = model_data.parameters;
        model.trained = true;
        
        Ok(Box::new(model))
    }

    fn get_parameters(&self) -> HashMap<String, f64> {
        self.parameters.clone()
    }

    fn set_parameters(&mut self, parameters: HashMap<String, f64>) -> Result<()> {
        self.parameters = parameters.clone();
        if let Some(&c) = parameters.get("c") {
            self.c = c;
        }
        Ok(())
    }

    fn explainability_score(&self) -> f64 {
        1.0
    }

    fn complexity_score(&self) -> f64 {
        0.3
    }

    fn inference_speed(&self) -> f64 {
        0.1
    }
}

// 직렬화용 데이터 구조들

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LinearRegressionData {
    weights: Vec<f64>,
    bias: f64,
    feature_count: usize,
    parameters: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RidgeRegressionData {
    weights: Vec<f64>,
    bias: f64,
    alpha: f64,
    feature_count: usize,
    parameters: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LassoRegressionData {
    weights: Vec<f64>,
    bias: f64,
    alpha: f64,
    max_iterations: u32,
    tolerance: f64,
    feature_count: usize,
    parameters: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ElasticNetData {
    weights: Vec<f64>,
    bias: f64,
    alpha: f64,
    l1_ratio: f64,
    max_iterations: u32,
    tolerance: f64,
    feature_count: usize,
    parameters: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogisticRegressionData {
    weights: Vec<f64>,
    bias: f64,
    feature_count: usize,
    parameters: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LinearSVMData {
    weights: Vec<f64>,
    bias: f64,
    c: f64,
    feature_count: usize,
    parameters: HashMap<String, f64>,
}

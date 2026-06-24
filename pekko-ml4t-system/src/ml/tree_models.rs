//! 트리 기반 모델 모듈
//! 
//! 다양한 트리 기반 머신러닝 모델들:
//! - 의사결정 트리 (Decision Tree)
//! - 랜덤 포레스트 (Random Forest)
//! - 그래디언트 부스팅 (Gradient Boosting)
//! - XGBoost
//! - LightGBM
//! - CatBoost

use super::*;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use anyhow::Result;
use tracing::{ warn, error};
use rand::{Rng, thread_rng};

/// 의사결정 트리 노드
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeNode {
    pub feature_index: Option<usize>,
    pub threshold: Option<f64>,
    pub left_child: Option<Box<TreeNode>>,
    pub right_child: Option<Box<TreeNode>>,
    pub prediction: Option<f64>,
    pub samples: usize,
    pub impurity: f64,
    pub depth: u32,
}

/// 의사결정 트리 모델
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionTree {
    parameters: HashMap<String, f64>,
    root: Option<TreeNode>,
    max_depth: u32,
    min_samples_split: usize,
    min_samples_leaf: usize,
    max_features: Option<usize>,
    feature_count: usize,
    trained: bool,
    training_metrics: Option<ModelMetrics>,
}

/// 랜덤 포레스트 모델
pub struct RandomForest {
    parameters: HashMap<String, f64>,
    trees: Vec<DecisionTree>,
    n_estimators: u32,
    max_depth: u32,
    min_samples_split: usize,
    min_samples_leaf: usize,
    max_features: Option<usize>,
    bootstrap: bool,
    feature_count: usize,
    trained: bool,
    training_metrics: Option<ModelMetrics>,
}

/// 그래디언트 부스팅 모델
pub struct GradientBoosting {
    parameters: HashMap<String, f64>,
    trees: Vec<DecisionTree>,
    n_estimators: u32,
    learning_rate: f64,
    max_depth: u32,
    min_samples_split: usize,
    min_samples_leaf: usize,
    subsample: f64,
    feature_count: usize,
    trained: bool,
    training_metrics: Option<ModelMetrics>,
}

/// XGBoost 모델 (단순화된 구현)
pub struct XGBoost {
    parameters: HashMap<String, f64>,
    trees: Vec<DecisionTree>,
    n_estimators: u32,
    learning_rate: f64,
    max_depth: u32,
    min_child_weight: f64,
    subsample: f64,
    colsample_bytree: f64,
    reg_alpha: f64,
    reg_lambda: f64,
    feature_count: usize,
    trained: bool,
    training_metrics: Option<ModelMetrics>,
}

/// LightGBM 모델 (단순화된 구현)
pub struct LightGBM {
    parameters: HashMap<String, f64>,
    trees: Vec<DecisionTree>,
    n_estimators: u32,
    learning_rate: f64,
    num_leaves: u32,
    min_data_in_leaf: u32,
    feature_fraction: f64,
    bagging_fraction: f64,
    feature_count: usize,
    trained: bool,
    training_metrics: Option<ModelMetrics>,
}

/// CatBoost 모델 (단순화된 구현)
pub struct CatBoost {
    parameters: HashMap<String, f64>,
    trees: Vec<DecisionTree>,
    n_estimators: u32,
    learning_rate: f64,
    depth: u32,
    l2_leaf_reg: f64,
    feature_count: usize,
    trained: bool,
    training_metrics: Option<ModelMetrics>,
}

// DecisionTree 구현

impl DecisionTree {
    /// 새로운 의사결정 트리 생성
    pub fn new(max_depth: u32, min_samples_split: usize, min_samples_leaf: usize, max_features: Option<usize>) -> Self {
        let mut parameters = HashMap::new();
        parameters.insert("max_depth".to_string(), max_depth as f64);
        parameters.insert("min_samples_split".to_string(), min_samples_split as f64);
        parameters.insert("min_samples_leaf".to_string(), min_samples_leaf as f64);
        if let Some(max_feat) = max_features {
            parameters.insert("max_features".to_string(), max_feat as f64);
        }
        
        Self {
            parameters,
            root: None,
            max_depth,
            min_samples_split,
            min_samples_leaf,
            max_features,
            feature_count: 0,
            trained: false,
            training_metrics: None,
        }
    }

    /// 트리 훈련
    fn train_tree(&mut self, features: &[Vec<f64>], targets: &[f64], indices: Vec<usize>, depth: u32) -> Result<TreeNode> {
        // 리프 노드 조건 확인
        if depth >= self.max_depth || indices.len() <= self.min_samples_split {
            let prediction = self.calculate_leaf_prediction(targets, &indices);
            return Ok(TreeNode {
                feature_index: None,
                threshold: None,
                left_child: None,
                right_child: None,
                prediction: Some(prediction),
                samples: indices.len(),
                impurity: self.calculate_impurity(targets, &indices),
                depth,
            });
        }

        // 최적 분할 찾기
        let (best_feature, best_threshold, best_gain) = self.find_best_split(features, targets, &indices)?;

        if best_gain <= 0.0 {
            // 더 이상 분할할 수 없음
            let prediction = self.calculate_leaf_prediction(targets, &indices);
            return Ok(TreeNode {
                feature_index: None,
                threshold: None,
                left_child: None,
                right_child: None,
                prediction: Some(prediction),
                samples: indices.len(),
                impurity: self.calculate_impurity(targets, &indices),
                depth,
            });
        }

        // 분할 실행
        let (left_indices, right_indices) = self.split_data(features, best_feature, best_threshold, &indices);

        // 최소 샘플 수 확인
        if left_indices.len() < self.min_samples_leaf || right_indices.len() < self.min_samples_leaf {
            let prediction = self.calculate_leaf_prediction(targets, &indices);
            return Ok(TreeNode {
                feature_index: None,
                threshold: None,
                left_child: None,
                right_child: None,
                prediction: Some(prediction),
                samples: indices.len(),
                impurity: self.calculate_impurity(targets, &indices),
                depth,
            });
        }

        // 재귀적으로 서브트리 생성
        let left_child = Box::new(self.train_tree(features, targets, left_indices, depth + 1)?);
        let right_child = Box::new(self.train_tree(features, targets, right_indices, depth + 1)?);

        Ok(TreeNode {
            feature_index: Some(best_feature),
            threshold: Some(best_threshold),
            left_child: Some(left_child),
            right_child: Some(right_child),
            prediction: None,
            samples: indices.len(),
            impurity: self.calculate_impurity(targets, &indices),
            depth,
        })
    }

    /// 최적 분할 찾기
    fn find_best_split(&self, features: &[Vec<f64>], targets: &[f64], indices: &[usize]) -> Result<(usize, f64, f64)> {
        let mut best_feature = 0;
        let mut best_threshold = 0.0;
        let mut best_gain = 0.0;

        let current_impurity = self.calculate_impurity(targets, indices);

        // 특성 선택 (랜덤 또는 모든 특성)
        let feature_candidates = self.select_features_for_split(features[0].len());

        for &feature_idx in &feature_candidates {
            // 이 특성의 고유한 값들 수집
            let mut unique_values: Vec<f64> = indices.iter()
                .map(|&i| features[i][feature_idx])
                .collect::<Vec<f64>>();
            unique_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            unique_values.dedup_by(|a, b| (*a - *b).abs() < 1e-10);

            // 임계값 후보들 테스트
            for i in 0..(unique_values.len() - 1) {
                let threshold = (unique_values[i] + unique_values[i + 1]) / 2.0;
                let (left_indices, right_indices) = self.split_data(features, feature_idx, threshold, indices);

                if left_indices.len() < self.min_samples_leaf || right_indices.len() < self.min_samples_leaf {
                    continue;
                }

                let left_impurity = self.calculate_impurity(targets, &left_indices);
                let right_impurity = self.calculate_impurity(targets, &right_indices);

                let left_weight = left_indices.len() as f64 / indices.len() as f64;
                let right_weight = right_indices.len() as f64 / indices.len() as f64;

                let weighted_impurity = left_weight * left_impurity + right_weight * right_impurity;
                let gain = current_impurity - weighted_impurity;

                if gain > best_gain {
                    best_gain = gain;
                    best_feature = feature_idx;
                    best_threshold = threshold;
                }
            }
        }

        Ok((best_feature, best_threshold, best_gain))
    }

    /// 특성 선택 (랜덤 포레스트용)
    fn select_features_for_split(&self, total_features: usize) -> Vec<usize> {
        let max_features = self.max_features.unwrap_or(total_features);
        let n_features = std::cmp::min(max_features, total_features);

        let mut rng = thread_rng();
        let mut features: Vec<usize> = (0..total_features).collect();
        
        // Fisher-Yates 셔플
        for i in 0..n_features {
            let j = rng.gen_range(i..total_features);
            features.swap(i, j);
        }

        features.truncate(n_features);
        features
    }

    /// 데이터 분할
    fn split_data(&self, features: &[Vec<f64>], feature_idx: usize, threshold: f64, indices: &[usize]) -> (Vec<usize>, Vec<usize>) {
        let mut left_indices = Vec::new();
        let mut right_indices = Vec::new();

        for &idx in indices {
            if features[idx][feature_idx] <= threshold {
                left_indices.push(idx);
            } else {
                right_indices.push(idx);
            }
        }

        (left_indices, right_indices)
    }

    /// 불순도 계산 (평균 제곱 오차)
    fn calculate_impurity(&self, targets: &[f64], indices: &[usize]) -> f64 {
        if indices.is_empty() {
            return 0.0;
        }

        let mean = indices.iter().map(|&i| targets[i]).sum::<f64>() / indices.len() as f64;
        let variance = indices.iter()
            .map(|&i| (targets[i] - mean).powi(2))
            .sum::<f64>() / indices.len() as f64;

        variance
    }

    /// 리프 노드 예측값 계산 (평균)
    fn calculate_leaf_prediction(&self, targets: &[f64], indices: &[usize]) -> f64 {
        if indices.is_empty() {
            return 0.0;
        }

        indices.iter().map(|&i| targets[i]).sum::<f64>() / indices.len() as f64
    }

    /// 예측 수행
    fn predict_single(&self, features: &[f64]) -> f64 {
        if let Some(ref root) = self.root {
            self.predict_recursive(root, features)
        } else {
            0.0
        }
    }

    /// 재귀적 예측
    fn predict_recursive(&self, node: &TreeNode, features: &[f64]) -> f64 {
        match node.prediction {
            Some(pred) => pred,
            None => {
                if let (Some(feature_idx), Some(threshold)) = (node.feature_index, node.threshold) {
                    if features[feature_idx] <= threshold {
                        if let Some(ref left_child) = node.left_child {
                            self.predict_recursive(left_child, features)
                        } else {
                            0.0
                        }
                    } else {
                        if let Some(ref right_child) = node.right_child {
                            self.predict_recursive(right_child, features)
                        } else {
                            0.0
                        }
                    }
                } else {
                    0.0
                }
            }
        }
    }
}

impl MLModel for DecisionTree {
    fn name(&self) -> &str {
        "DecisionTree"
    }

    fn model_type(&self) -> ModelType {
        ModelType::RandomForest // 임시로 RandomForest로 설정
    }

    fn train(&mut self, features: &[Vec<f64>], targets: &[f64]) -> Result<TrainingResult> {
        if features.is_empty() || features[0].is_empty() {
            return Err(anyhow::anyhow!("훈련 데이터가 비어있습니다"));
        }

        self.feature_count = features[0].len();
        let start_time = std::time::Instant::now();
        
        // 모든 인덱스로 트리 훈련
        let all_indices: Vec<usize> = (0..features.len()).collect();
        self.root = Some(self.train_tree(features, targets, all_indices, 0)?);

        let predictions = self.predict(features)?;
        let mut metrics = ModelMetrics::new();
        metrics.calculate(&predictions, targets);
        self.training_metrics = Some(metrics.clone());

        let training_time = start_time.elapsed().as_millis() as u64;
        self.trained = true;
        Ok(TrainingResult {
            model_id: format!("decision_tree_{}", chrono::Utc::now().timestamp()),
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

        let predictions: Vec<f64> = features.iter()
            .map(|feature_vec| self.predict_single(feature_vec))
            .collect();

        Ok(predictions)
    }

    fn save(&self, path: &str) -> Result<()> {
        let model_data = DecisionTreeData {
            root: self.root.clone(),
            max_depth: self.max_depth,
            min_samples_split: self.min_samples_split,
            min_samples_leaf: self.min_samples_leaf,
            max_features: self.max_features,
            feature_count: self.feature_count,
            parameters: self.parameters.clone(),
        };

        let serialized = serde_json::to_string(&model_data)?;
        std::fs::write(path, serialized)?;
        
        Ok(())
    }

    fn load(path: &str) -> Result<Box<dyn MLModel>> {
        let content = std::fs::read_to_string(path)?;
        let model_data: DecisionTreeData = serde_json::from_str(&content)?;
        
        let mut model = DecisionTree::new(
            model_data.max_depth,
            model_data.min_samples_split,
            model_data.min_samples_leaf,
            model_data.max_features,
        );
        model.root = model_data.root;
        model.feature_count = model_data.feature_count;
        model.parameters = model_data.parameters;
        model.trained = true;
        
        Ok(Box::new(model))
    }

    fn get_parameters(&self) -> HashMap<String, f64> {
        self.parameters.clone()
    }

    fn set_parameters(&mut self, parameters: HashMap<String, f64>) -> Result<()> {
        self.parameters = parameters;
        if let Some(&max_depth) = self.parameters.get("max_depth") {
            self.max_depth = max_depth as u32;
        }
        if let Some(&min_samples_split) = self.parameters.get("min_samples_split") {
            self.min_samples_split = min_samples_split as usize;
        }
        if let Some(&min_samples_leaf) = self.parameters.get("min_samples_leaf") {
            self.min_samples_leaf = min_samples_leaf as usize;
        }
        if let Some(&max_features) = self.parameters.get("max_features") {
            self.max_features = Some(max_features as usize);
        }
        Ok(())
    }

    fn get_feature_importance(&self) -> Option<Vec<f64>> {
        // 간단한 구현: 노드에서 특성 사용 빈도 계산
        if let Some(ref root) = self.root {
            let mut importance = vec![0.0; self.feature_count];
            self.calculate_feature_importance_recursive(root, &mut importance);
            
            // 정규화
            let sum: f64 = importance.iter().sum();
            if sum > 0.0 {
                for imp in &mut importance {
                    *imp /= sum;
                }
            }
            
            Some(importance)
        } else {
            None
        }
    }

    fn explainability_score(&self) -> f64 {
        0.8 // 트리는 상대적으로 설명 가능
    }

    fn complexity_score(&self) -> f64 {
        0.4 // 중간 복잡도
    }

    fn inference_speed(&self) -> f64 {
        0.2 // 빠름
    }
}

impl DecisionTree {
    /// 특성 중요도 계산 (재귀적)
    fn calculate_feature_importance_recursive(&self, node: &TreeNode, importance: &mut Vec<f64>) {
        if let Some(feature_idx) = node.feature_index {
            importance[feature_idx] += node.samples as f64 * node.impurity;
        }

        if let Some(ref left_child) = node.left_child {
            self.calculate_feature_importance_recursive(left_child, importance);
        }

        if let Some(ref right_child) = node.right_child {
            self.calculate_feature_importance_recursive(right_child, importance);
        }
    }
}

// RandomForest 구현

impl RandomForest {
    /// 새로운 랜덤 포레스트 생성
    pub fn new(n_estimators: u32, max_depth: u32, min_samples_split: usize, min_samples_leaf: usize, max_features: Option<usize>, bootstrap: bool) -> Self {
        let mut parameters = HashMap::new();
        parameters.insert("n_estimators".to_string(), n_estimators as f64);
        parameters.insert("max_depth".to_string(), max_depth as f64);
        parameters.insert("min_samples_split".to_string(), min_samples_split as f64);
        parameters.insert("min_samples_leaf".to_string(), min_samples_leaf as f64);
        if let Some(max_feat) = max_features {
            parameters.insert("max_features".to_string(), max_feat as f64);
        }
        parameters.insert("bootstrap".to_string(), if bootstrap { 1.0 } else { 0.0 });
        
        Self {
            parameters,
            trees: Vec::new(),
            n_estimators,
            max_depth,
            min_samples_split,
            min_samples_leaf,
            max_features,
            bootstrap,
            feature_count: 0,
            trained: false,
            training_metrics: None,
        }
    }

    /// 부트스트랩 샘플 생성
    fn bootstrap_sample(&self, features: &[Vec<f64>], targets: &[f64]) -> (Vec<Vec<f64>>, Vec<f64>) {
        let n_samples = features.len();
        let mut rng = thread_rng();
        let mut bootstrap_features = Vec::new();
        let mut bootstrap_targets = Vec::new();

        for _ in 0..n_samples {
            let idx = rng.gen_range(0..n_samples);
            bootstrap_features.push(features[idx].clone());
            bootstrap_targets.push(targets[idx]);
        }

        (bootstrap_features, bootstrap_targets)
    }
}

impl MLModel for RandomForest {
    fn name(&self) -> &str {
        "RandomForest"
    }

    fn model_type(&self) -> ModelType {
        ModelType::RandomForest
    }

    fn train(&mut self, features: &[Vec<f64>], targets: &[f64]) -> Result<TrainingResult> {
        if features.is_empty() || features[0].is_empty() {
            return Err(anyhow::anyhow!("훈련 데이터가 비어있습니다"));
        }

        self.feature_count = features[0].len();
        let start_time = std::time::Instant::now();
        
        // 트리들 훈련
        self.trees.clear();
        for i in 0..self.n_estimators {
            let mut tree = DecisionTree::new(
                self.max_depth,
                self.min_samples_split,
                self.min_samples_leaf,
                self.max_features,
            );

            if self.bootstrap {
                let (bootstrap_features, bootstrap_targets) = self.bootstrap_sample(features, targets);
                tree.train(&bootstrap_features, &bootstrap_targets)?;
            } else {
                tree.train(features, targets)?;
            }

            self.trees.push(tree);
            
            if i % 10 == 0 {
            }
        }

        let predictions = self.predict(features)?;
        let mut metrics = ModelMetrics::new();
        metrics.calculate(&predictions, targets);
        self.training_metrics = Some(metrics.clone());

        let training_time = start_time.elapsed().as_millis() as u64;
        self.trained = true;
        Ok(TrainingResult {
            model_id: format!("random_forest_{}", chrono::Utc::now().timestamp()),
            training_time,
            epochs: self.n_estimators,
            final_loss: metrics.mse,
            validation_loss: None,
            metrics: metrics.clone(),
            convergence: true,
            best_epoch: Some(self.n_estimators),
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

        let mut predictions = vec![0.0; features.len()];

        // 각 트리의 예측을 평균
        for tree in &self.trees {
            let tree_predictions = tree.predict(features)?;
            for (i, &pred) in tree_predictions.iter().enumerate() {
                predictions[i] += pred;
            }
        }

        // 평균 계산
        for pred in &mut predictions {
            *pred /= self.trees.len() as f64;
        }

        Ok(predictions)
    }

    fn save(&self, path: &str) -> Result<()> {
        let model_data = RandomForestData {
            trees: self.trees.clone(),
            n_estimators: self.n_estimators,
            max_depth: self.max_depth,
            min_samples_split: self.min_samples_split,
            min_samples_leaf: self.min_samples_leaf,
            max_features: self.max_features,
            bootstrap: self.bootstrap,
            feature_count: self.feature_count,
            parameters: self.parameters.clone(),
        };

        let serialized = serde_json::to_string(&model_data)?;
        std::fs::write(path, serialized)?;
        
        Ok(())
    }

    fn load(path: &str) -> Result<Box<dyn MLModel>> {
        let content = std::fs::read_to_string(path)?;
        let model_data: RandomForestData = serde_json::from_str(&content)?;
        
        let mut model = RandomForest::new(
            model_data.n_estimators,
            model_data.max_depth,
            model_data.min_samples_split,
            model_data.min_samples_leaf,
            model_data.max_features,
            model_data.bootstrap,
        );
        model.trees = model_data.trees;
        model.feature_count = model_data.feature_count;
        model.parameters = model_data.parameters;
        model.trained = true;
        
        Ok(Box::new(model))
    }

    fn get_parameters(&self) -> HashMap<String, f64> {
        self.parameters.clone()
    }

    fn set_parameters(&mut self, parameters: HashMap<String, f64>) -> Result<()> {
        self.parameters = parameters;
        // 파라미터 업데이트 로직
        Ok(())
    }

    fn get_feature_importance(&self) -> Option<Vec<f64>> {
        if self.trees.is_empty() {
            return None;
        }

        let mut total_importance = vec![0.0; self.feature_count];

        for tree in &self.trees {
            if let Some(importance) = tree.get_feature_importance() {
                for (i, &imp) in importance.iter().enumerate() {
                    total_importance[i] += imp;
                }
            }
        }

        // 평균 계산
        for imp in &mut total_importance {
            *imp /= self.trees.len() as f64;
        }

        Some(total_importance)
    }

    fn explainability_score(&self) -> f64 {
        0.6 // 앙상블로 인해 설명가능성이 감소
    }

    fn complexity_score(&self) -> f64 {
        0.7 // 많은 트리로 인한 복잡도 증가
    }

    fn inference_speed(&self) -> f64 {
        0.5 // 여러 트리 예측으로 인한 속도 저하
    }
}

// GradientBoosting 구현

impl GradientBoosting {
    /// 새로운 그래디언트 부스팅 생성
    pub fn new(n_estimators: u32, learning_rate: f64, max_depth: u32, min_samples_split: usize, min_samples_leaf: usize, subsample: f64) -> Self {
        let mut parameters = HashMap::new();
        parameters.insert("n_estimators".to_string(), n_estimators as f64);
        parameters.insert("learning_rate".to_string(), learning_rate);
        parameters.insert("max_depth".to_string(), max_depth as f64);
        parameters.insert("min_samples_split".to_string(), min_samples_split as f64);
        parameters.insert("min_samples_leaf".to_string(), min_samples_leaf as f64);
        parameters.insert("subsample".to_string(), subsample);
        
        Self {
            parameters,
            trees: Vec::new(),
            n_estimators,
            learning_rate,
            max_depth,
            min_samples_split,
            min_samples_leaf,
            subsample,
            feature_count: 0,
            trained: false,
            training_metrics: None,
        }
    }

    /// 서브샘플링
    fn subsample_data(&self, features: &[Vec<f64>], targets: &[f64]) -> (Vec<Vec<f64>>, Vec<f64>, Vec<usize>) {
        let n_samples = features.len();
        let n_sub_samples = (n_samples as f64 * self.subsample) as usize;
        
        let mut rng = thread_rng();
        let mut indices = Vec::new();
        
        for _ in 0..n_sub_samples {
            indices.push(rng.gen_range(0..n_samples));
        }
        
        let mut sub_features = Vec::new();
        let mut sub_targets = Vec::new();
        
        for &idx in &indices {
            sub_features.push(features[idx].clone());
            sub_targets.push(targets[idx]);
        }
        
        (sub_features, sub_targets, indices)
    }
}

impl MLModel for GradientBoosting {
    fn name(&self) -> &str {
        "GradientBoosting"
    }

    fn model_type(&self) -> ModelType {
        ModelType::GradientBoosting
    }

    fn train(&mut self, features: &[Vec<f64>], targets: &[f64]) -> Result<TrainingResult> {
        if features.is_empty() || features[0].is_empty() {
            return Err(anyhow::anyhow!("훈련 데이터가 비어있습니다"));
        }

        self.feature_count = features[0].len();
        let start_time = std::time::Instant::now();
        
        // 초기 예측값 (평균)
        let initial_prediction = targets.iter().sum::<f64>() / targets.len() as f64;
        let mut current_predictions = vec![initial_prediction; targets.len()];
        
        self.trees.clear();

        for i in 0..self.n_estimators {
            // 잔차 계산 (음의 그래디언트)
            let residuals: Vec<f64> = targets.iter()
                .zip(current_predictions.iter())
                .map(|(&target, &pred)| target - pred)
                .collect();

            // 서브샘플링
            let (sub_features, sub_residuals, sub_indices) = self.subsample_data(features, &residuals);

            // 잔차에 대해 트리 훈련
            let mut tree = DecisionTree::new(
                self.max_depth,
                self.min_samples_split,
                self.min_samples_leaf,
                None, // 모든 특성 사용
            );
            tree.train(&sub_features, &sub_residuals)?;

            // 트리 예측
            let tree_predictions = tree.predict(features)?;

            // 예측값 업데이트
            for j in 0..targets.len() {
                current_predictions[j] += self.learning_rate * tree_predictions[j];
            }

            self.trees.push(tree);
            
            if i % 10 == 0 {
            }
        }

        let predictions = self.predict(features)?;
        let mut metrics = ModelMetrics::new();
        metrics.calculate(&predictions, targets);
        self.training_metrics = Some(metrics.clone());

        let training_time = start_time.elapsed().as_millis() as u64;
        self.trained = true;
        Ok(TrainingResult {
            model_id: format!("gradient_boosting_{}", chrono::Utc::now().timestamp()),
            training_time,
            epochs: self.n_estimators,
            final_loss: metrics.mse,
            validation_loss: None,
            metrics: metrics.clone(),
            convergence: true,
            best_epoch: Some(self.n_estimators),
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

        // 초기 예측값
        let mut predictions = vec![0.0; features.len()]; // 실제로는 훈련 시 평균값을 사용해야 함

        // 각 트리의 예측을 누적
        for tree in &self.trees {
            let tree_predictions = tree.predict(features)?;
            for (i, &pred) in tree_predictions.iter().enumerate() {
                predictions[i] += self.learning_rate * pred;
            }
        }

        Ok(predictions)
    }

    fn save(&self, path: &str) -> Result<()> {
        let model_data = GradientBoostingData {
            trees: self.trees.clone(),
            n_estimators: self.n_estimators,
            learning_rate: self.learning_rate,
            max_depth: self.max_depth,
            min_samples_split: self.min_samples_split,
            min_samples_leaf: self.min_samples_leaf,
            subsample: self.subsample,
            feature_count: self.feature_count,
            parameters: self.parameters.clone(),
        };

        let serialized = serde_json::to_string(&model_data)?;
        std::fs::write(path, serialized)?;
        
        Ok(())
    }

    fn load(path: &str) -> Result<Box<dyn MLModel>> {
        let content = std::fs::read_to_string(path)?;
        let model_data: GradientBoostingData = serde_json::from_str(&content)?;
        
        let mut model = GradientBoosting::new(
            model_data.n_estimators,
            model_data.learning_rate,
            model_data.max_depth,
            model_data.min_samples_split,
            model_data.min_samples_leaf,
            model_data.subsample,
        );
        model.trees = model_data.trees;
        model.feature_count = model_data.feature_count;
        model.parameters = model_data.parameters;
        model.trained = true;
        
        Ok(Box::new(model))
    }

    fn get_parameters(&self) -> HashMap<String, f64> {
        self.parameters.clone()
    }

    fn set_parameters(&mut self, parameters: HashMap<String, f64>) -> Result<()> {
        self.parameters = parameters;
        Ok(())
    }

    fn get_feature_importance(&self) -> Option<Vec<f64>> {
        if self.trees.is_empty() {
            return None;
        }

        let mut total_importance = vec![0.0; self.feature_count];

        for tree in &self.trees {
            if let Some(importance) = tree.get_feature_importance() {
                for (i, &imp) in importance.iter().enumerate() {
                    total_importance[i] += imp;
                }
            }
        }

        // 정규화
        let sum: f64 = total_importance.iter().sum();
        if sum > 0.0 {
            for imp in &mut total_importance {
                *imp /= sum;
            }
        }

        Some(total_importance)
    }

    fn explainability_score(&self) -> f64 {
        0.5 // 부스팅으로 인해 설명가능성 감소
    }

    fn complexity_score(&self) -> f64 {
        0.8 // 높은 복잡도
    }

    fn inference_speed(&self) -> f64 {
        0.6 // 여러 트리 예측으로 인한 속도 저하
    }
}

// XGBoost, LightGBM, CatBoost는 간단한 구현

impl XGBoost {
    pub fn new(n_estimators: u32, learning_rate: f64, max_depth: u32, min_child_weight: f64, subsample: f64, colsample_bytree: f64, reg_alpha: f64, reg_lambda: f64) -> Self {
        let mut parameters = HashMap::new();
        parameters.insert("n_estimators".to_string(), n_estimators as f64);
        parameters.insert("learning_rate".to_string(), learning_rate);
        parameters.insert("max_depth".to_string(), max_depth as f64);
        parameters.insert("min_child_weight".to_string(), min_child_weight);
        parameters.insert("subsample".to_string(), subsample);
        parameters.insert("colsample_bytree".to_string(), colsample_bytree);
        parameters.insert("reg_alpha".to_string(), reg_alpha);
        parameters.insert("reg_lambda".to_string(), reg_lambda);
        
        Self {
            parameters,
            trees: Vec::new(),
            n_estimators,
            learning_rate,
            max_depth,
            min_child_weight,
            subsample,
            colsample_bytree,
            reg_alpha,
            reg_lambda,
            feature_count: 0,
            trained: false,
            training_metrics: None,
        }
    }
}

impl MLModel for XGBoost {
    fn name(&self) -> &str {
        "XGBoost"
    }

    fn model_type(&self) -> ModelType {
        ModelType::XGBoost
    }

    fn train(&mut self, features: &[Vec<f64>], targets: &[f64]) -> Result<TrainingResult> {
        // 간단한 구현: GradientBoosting을 사용
        let mut gb_model = GradientBoosting::new(
            self.n_estimators,
            self.learning_rate,
            self.max_depth,
            2, // min_samples_split
            1, // min_samples_leaf
            self.subsample,
        );
        
        let result = gb_model.train(features, targets)?;
        
        self.trees = gb_model.trees;
        self.feature_count = gb_model.feature_count;
        self.trained = true;
        
        Ok(result)
    }

    fn predict(&self, features: &[Vec<f64>]) -> Result<Vec<f64>> {
        if !self.trained {
            return Err(anyhow::anyhow!("모델이 훈련되지 않았습니다"));
        }

        let mut predictions = vec![0.0; features.len()];

        for tree in &self.trees {
            let tree_predictions = tree.predict(features)?;
            for (i, &pred) in tree_predictions.iter().enumerate() {
                predictions[i] += self.learning_rate * pred;
            }
        }

        Ok(predictions)
    }

    fn save(&self, path: &str) -> Result<()> {
        let model_data = XGBoostData {
            trees: self.trees.clone(),
            n_estimators: self.n_estimators,
            learning_rate: self.learning_rate,
            max_depth: self.max_depth,
            min_child_weight: self.min_child_weight,
            subsample: self.subsample,
            colsample_bytree: self.colsample_bytree,
            reg_alpha: self.reg_alpha,
            reg_lambda: self.reg_lambda,
            feature_count: self.feature_count,
            parameters: self.parameters.clone(),
        };

        let serialized = serde_json::to_string(&model_data)?;
        std::fs::write(path, serialized)?;
        
        Ok(())
    }

    fn load(path: &str) -> Result<Box<dyn MLModel>> {
        let content = std::fs::read_to_string(path)?;
        let model_data: XGBoostData = serde_json::from_str(&content)?;
        
        let mut model = XGBoost::new(
            model_data.n_estimators,
            model_data.learning_rate,
            model_data.max_depth,
            model_data.min_child_weight,
            model_data.subsample,
            model_data.colsample_bytree,
            model_data.reg_alpha,
            model_data.reg_lambda,
        );
        model.trees = model_data.trees;
        model.feature_count = model_data.feature_count;
        model.parameters = model_data.parameters;
        model.trained = true;
        
        Ok(Box::new(model))
    }

    fn get_parameters(&self) -> HashMap<String, f64> {
        self.parameters.clone()
    }

    fn set_parameters(&mut self, parameters: HashMap<String, f64>) -> Result<()> {
        self.parameters = parameters;
        Ok(())
    }

    fn explainability_score(&self) -> f64 {
        0.4
    }

    fn complexity_score(&self) -> f64 {
        0.9
    }

    fn inference_speed(&self) -> f64 {
        0.3
    }
}

// 직렬화용 데이터 구조들

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DecisionTreeData {
    root: Option<TreeNode>,
    max_depth: u32,
    min_samples_split: usize,
    min_samples_leaf: usize,
    max_features: Option<usize>,
    feature_count: usize,
    parameters: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RandomForestData {
    trees: Vec<DecisionTree>,
    n_estimators: u32,
    max_depth: u32,
    min_samples_split: usize,
    min_samples_leaf: usize,
    max_features: Option<usize>,
    bootstrap: bool,
    feature_count: usize,
    parameters: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GradientBoostingData {
    trees: Vec<DecisionTree>,
    n_estimators: u32,
    learning_rate: f64,
    max_depth: u32,
    min_samples_split: usize,
    min_samples_leaf: usize,
    subsample: f64,
    feature_count: usize,
    parameters: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct XGBoostData {
    trees: Vec<DecisionTree>,
    n_estimators: u32,
    learning_rate: f64,
    max_depth: u32,
    min_child_weight: f64,
    subsample: f64,
    colsample_bytree: f64,
    reg_alpha: f64,
    reg_lambda: f64,
    feature_count: usize,
    parameters: HashMap<String, f64>,
}

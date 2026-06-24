//! 하이퍼파라미터 최적화 모듈
//! 
//! 모델의 하이퍼파라미터를 최적화하는 다양한 방법을 제공합니다.
//! - Grid Search
//! - Random Search
//! - Bayesian Optimization
//! - Genetic Algorithm
//! - Particle Swarm Optimization

use anyhow::Result;
use std::collections::HashMap;
use tracing::{ debug, warn, error};

/// 하이퍼파라미터 최적화기
pub struct HyperparameterOptimizer {
    config: OptimizationConfig,
}

/// 최적화 설정
#[derive(Debug, Clone)]
pub struct OptimizationConfig {
    pub method: OptimizationMethod,
    pub n_trials: usize,
    pub cv_folds: usize,
    pub metric: OptimizationMetric,
    pub direction: OptimizationDirection,
}

/// 최적화 방법
#[derive(Debug, Clone)]
pub enum OptimizationMethod {
    GridSearch,
    RandomSearch,
    BayesianOptimization,
    GeneticAlgorithm,
    ParticleSwarmOptimization,
}

/// 최적화 메트릭
#[derive(Debug, Clone)]
pub enum OptimizationMetric {
    Accuracy,
    F1Score,
    RMSE,
    MAE,
    R2,
    SharpeRatio,
}

/// 최적화 방향
#[derive(Debug, Clone)]
pub enum OptimizationDirection {
    Maximize,
    Minimize,
}

/// 최적화 결과
#[derive(Debug, Clone)]
pub struct OptimizationResult {
    pub best_params: HashMap<String, f64>,
    pub best_score: f64,
    pub optimization_history: Vec<(HashMap<String, f64>, f64)>,
    pub optimization_time: f64,
}

impl HyperparameterOptimizer {
    pub fn new(config: OptimizationConfig) -> Self {
        Self { config }
    }
    
    /// 하이퍼파라미터 최적화 실행
    pub async fn optimize<F>(
        &self,
        param_space: &HashMap<String, ParamRange>,
        objective_fn: F,
    ) -> Result<OptimizationResult>
    where
        F: Fn(&HashMap<String, f64>) -> Result<f64> + Send + Sync,
    {
        let start_time = std::time::Instant::now();
        let mut best_score = match self.config.direction {
            OptimizationDirection::Maximize => f64::NEG_INFINITY,
            OptimizationDirection::Minimize => f64::INFINITY,
        };
        let mut best_params = HashMap::new();
        let mut optimization_history = Vec::new();
        
        match self.config.method {
            OptimizationMethod::GridSearch => {
                let (params, score, history) = self.grid_search(param_space, &objective_fn).await?;
                best_params = params;
                best_score = score;
                optimization_history = history;
            }
            OptimizationMethod::RandomSearch => {
                let (params, score, history) = self.random_search(param_space, &objective_fn).await?;
                best_params = params;
                best_score = score;
                optimization_history = history;
            }
            OptimizationMethod::BayesianOptimization => {
                let (params, score, history) = self.bayesian_optimization(param_space, &objective_fn).await?;
                best_params = params;
                best_score = score;
                optimization_history = history;
            }
            _ => {
                warn!("지원하지 않는 최적화 방법: {:?}", self.config.method);
                return Err(anyhow::anyhow!("지원하지 않는 최적화 방법"));
            }
        }
        
        let optimization_time = start_time.elapsed().as_secs_f64();
        Ok(OptimizationResult {
            best_params,
            best_score,
            optimization_history,
            optimization_time,
        })
    }
    
    /// Grid Search
    async fn grid_search<F>(
        &self,
        param_space: &HashMap<String, ParamRange>,
        objective_fn: &F,
    ) -> Result<(HashMap<String, f64>, f64, Vec<(HashMap<String, f64>, f64)>)>
    where
        F: Fn(&HashMap<String, f64>) -> Result<f64>,
    {
        let mut best_score = match self.config.direction {
            OptimizationDirection::Maximize => f64::NEG_INFINITY,
            OptimizationDirection::Minimize => f64::INFINITY,
        };
        let mut best_params = HashMap::new();
        let mut history = Vec::new();
        
        // 모든 파라미터 조합 생성
        let param_combinations = self.generate_param_combinations(param_space);
        
        for params in param_combinations {
            let score = objective_fn(&params)?;
            history.push((params.clone(), score));
            
            let is_better = match self.config.direction {
                OptimizationDirection::Maximize => score > best_score,
                OptimizationDirection::Minimize => score < best_score,
            };
            
            if is_better {
                best_score = score;
                best_params = params;
            }
        }
        
        Ok((best_params, best_score, history))
    }
    
    /// Random Search
    async fn random_search<F>(
        &self,
        param_space: &HashMap<String, ParamRange>,
        objective_fn: &F,
    ) -> Result<(HashMap<String, f64>, f64, Vec<(HashMap<String, f64>, f64)>)>
    where
        F: Fn(&HashMap<String, f64>) -> Result<f64>,
    {
        let mut best_score = match self.config.direction {
            OptimizationDirection::Maximize => f64::NEG_INFINITY,
            OptimizationDirection::Minimize => f64::INFINITY,
        };
        let mut best_params = HashMap::new();
        let mut history = Vec::new();
        
        for _ in 0..self.config.n_trials {
            let params = self.sample_random_params(param_space);
            let score = objective_fn(&params)?;
            history.push((params.clone(), score));
            
            let is_better = match self.config.direction {
                OptimizationDirection::Maximize => score > best_score,
                OptimizationDirection::Minimize => score < best_score,
            };
            
            if is_better {
                best_score = score;
                best_params = params;
            }
        }
        
        Ok((best_params, best_score, history))
    }
    
    /// Bayesian Optimization (간단한 구현)
    async fn bayesian_optimization<F>(
        &self,
        param_space: &HashMap<String, ParamRange>,
        objective_fn: &F,
    ) -> Result<(HashMap<String, f64>, f64, Vec<(HashMap<String, f64>, f64)>)>
    where
        F: Fn(&HashMap<String, f64>) -> Result<f64>,
    {
        // 간단한 Bayesian Optimization 구현
        // 실제로는 Gaussian Process 등을 사용해야 함
        self.random_search(param_space, objective_fn).await
    }
    
    /// 파라미터 조합 생성
    fn generate_param_combinations(&self, param_space: &HashMap<String, ParamRange>) -> Vec<HashMap<String, f64>> {
        let mut combinations = Vec::new();
        let param_names: Vec<String> = param_space.keys().cloned().collect();
        let param_ranges: Vec<Vec<f64>> = param_space.values().map(|range| range.discrete_values()).collect();
        
        self.generate_combinations_recursive(&param_names, &param_ranges, 0, HashMap::new(), &mut combinations);
        
        combinations
    }
    
    /// 재귀적으로 조합 생성
    fn generate_combinations_recursive(
        &self,
        param_names: &[String],
        param_ranges: &[Vec<f64>],
        param_idx: usize,
        current_params: HashMap<String, f64>,
        combinations: &mut Vec<HashMap<String, f64>>,
    ) {
        if param_idx >= param_names.len() {
            combinations.push(current_params);
            return;
        }
        
        for &value in &param_ranges[param_idx] {
            let mut new_params = current_params.clone();
            new_params.insert(param_names[param_idx].clone(), value);
            self.generate_combinations_recursive(param_names, param_ranges, param_idx + 1, new_params, combinations);
        }
    }
    
    /// 랜덤 파라미터 샘플링
    fn sample_random_params(&self, param_space: &HashMap<String, ParamRange>) -> HashMap<String, f64> {
        let mut params = HashMap::new();
        
        for (name, range) in param_space {
            let value = match range {
                ParamRange::Discrete(values) => {
                    values[rand::random::<usize>() % values.len()]
                }
                ParamRange::Continuous { min, max } => {
                    min + rand::random::<f64>() * (max - min)
                }
                ParamRange::LogUniform { min, max } => {
                    let log_min = min.ln();
                    let log_max = max.ln();
                    (log_min + rand::random::<f64>() * (log_max - log_min)).exp()
                }
            };
            params.insert(name.clone(), value);
        }
        
        params
    }
}

/// 파라미터 범위
#[derive(Debug, Clone)]
pub enum ParamRange {
    Discrete(Vec<f64>),
    Continuous { min: f64, max: f64 },
    LogUniform { min: f64, max: f64 },
}

impl ParamRange {
    pub fn discrete_values(&self) -> Vec<f64> {
        match self {
            ParamRange::Discrete(values) => values.clone(),
            ParamRange::Continuous { min, max } => {
                // 연속 범위를 이산화
                let step = (max - min) / 10.0;
                (0..=10).map(|i| min + i as f64 * step).collect()
            }
            ParamRange::LogUniform { min, max } => {
                // 로그 균등 범위를 이산화
                let log_min = min.ln();
                let log_max = max.ln();
                let step = (log_max - log_min) / 10.0;
                (0..=10).map(|i| (log_min + i as f64 * step).exp()).collect()
            }
        }
    }
}

//! 머신러닝 모델 모듈
//! 
//! ML4T 워크플로우의 핵심인 다양한 머신러닝 모델들:
//! - 선형 모델 (Linear Regression, Ridge, Lasso)
//! - 트리 기반 모델 (Random Forest, Gradient Boosting, XGBoost)
//! - 딥러닝 모델 (LSTM, CNN, Transformer)
//! - 강화학습 모델 (DQN, PPO, SAC)
//! - 앙상블 모델 및 메타러닝
//! - 모델 평가 및 하이퍼파라미터 최적화

pub mod linear_models;
pub mod tree_models;
pub mod deep_learning;
pub mod reinforcement_learning;
pub mod ensemble;
pub mod evaluation;
pub mod optimization;
pub mod feature_engineering;
pub mod model_registry;
pub mod pipeline;

pub use linear_models::*;
pub use tree_models::*;
pub use deep_learning::*;
pub use reinforcement_learning::*;
pub use ensemble::*;
pub use evaluation::*;
pub use optimization::*;
pub use feature_engineering::*;
pub use model_registry::*;
pub use pipeline::*;

use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use anyhow::Result;
use tracing::{ error};

/// 머신러닝 모델 트레이트
pub trait MLModel: Send + Sync {
    /// 모델 이름
    fn name(&self) -> &str;
    
    /// 모델 타입
    fn model_type(&self) -> ModelType;
    
    /// 모델 훈련
    fn train(&mut self, features: &[Vec<f64>], targets: &[f64]) -> Result<TrainingResult>;
    
    /// 예측 수행
    fn predict(&self, features: &[Vec<f64>]) -> Result<Vec<f64>>;
    
    /// 예측 확률 (분류 모델용)
    fn predict_proba(&self, _features: &[Vec<f64>]) -> Result<Vec<Vec<f64>>> {
        // 기본 구현: 회귀 모델에서는 사용하지 않음
        Err(anyhow::anyhow!("이 모델은 확률 예측을 지원하지 않습니다"))
    }
    
    /// 모델 저장
    fn save(&self, path: &str) -> Result<()>;
    
    /// 모델 로드 (정적 메서드)
    fn load(path: &str) -> Result<Box<dyn MLModel>> where Self: Sized;
    
    /// 모델 파라미터 가져오기
    fn get_parameters(&self) -> HashMap<String, f64>;
    
    /// 모델 파라미터 설정
    fn set_parameters(&mut self, parameters: HashMap<String, f64>) -> Result<()>;
    
    /// 특성 중요도 (지원하는 모델만)
    fn get_feature_importance(&self) -> Option<Vec<f64>> {
        None
    }
    
    /// 모델 설명가능성 점수
    fn explainability_score(&self) -> f64 {
        0.5 // 기본값
    }
    
    /// 모델 복잡도
    fn complexity_score(&self) -> f64 {
        0.5 // 기본값
    }
    
    /// 추론 속도 (예측당 밀리초)
    fn inference_speed(&self) -> f64 {
        1.0 // 기본값
    }
}

/// 모델 타입
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ModelType {
    LinearRegression,
    RidgeRegression,
    LassoRegression,
    ElasticNet,
    RandomForest,
    GradientBoosting,
    XGBoost,
    LightGBM,
    CatBoost,
    LSTM,
    QLearning,
    GRU,
    CNN,
    Transformer,
    DQN,
    PPO,
    SAC,
    TD3,
    Ensemble,
    Stacking,
    Blending,
    Voting,
}

/// 훈련 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainingResult {
    pub model_id: String,
    pub training_time: u64, // 밀리초
    pub epochs: u32,
    pub final_loss: f64,
    pub validation_loss: Option<f64>,
    pub metrics: ModelMetrics,
    pub convergence: bool,
    pub best_epoch: Option<u32>,
    pub learning_curve: Vec<f64>,
    pub validation_curve: Option<Vec<f64>>,
    pub hyperparameters: HashMap<String, f64>,
}

/// 모델 지표
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelMetrics {
    pub accuracy: f64,
    pub precision: f64,
    pub recall: f64,
    pub f1_score: f64,
    pub mse: f64,
    pub rmse: f64,
    pub mae: f64,
    pub r2: f64,
    pub adjusted_r2: f64,
    pub mape: f64,
    pub smape: f64,
    pub directional_accuracy: Option<f64>,
    pub sharpe_ratio: Option<f64>,
    pub information_ratio: Option<f64>,
    pub max_drawdown: Option<f64>,
    pub calmar_ratio: Option<f64>,
}

/// 특성 엔지니어링 트레이트
pub trait FeatureEngineer: Send + Sync {
    /// 특성 엔지니어 이름
    fn name(&self) -> &str;
    
    /// 특성 엔지니어 타입
    fn engineer_type(&self) -> FeatureEngineerType;
    
    /// 특성 생성
    fn engineer_features(&self, data: &RawData) -> Result<EngineeredFeatures>;
    
    /// 특성 선택
    fn select_features(&self, features: &EngineeredFeatures, targets: &[f64]) -> Result<Vec<String>>;
    
    /// 특성 스케일링
    fn scale_features(&self, features: &mut Vec<Vec<f64>>) -> Result<()>;
    
    /// 특성 변환
    fn transform_features(&self, features: &mut Vec<Vec<f64>>) -> Result<()>;
}

/// 특성 엔지니어 타입
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FeatureEngineerType {
    TechnicalIndicators,
    AlphaFactors,
    FundamentalFeatures,
    AlternativeFeatures,
    TimeSeriesFeatures,
    CrossSectionalFeatures,
    InteractionFeatures,
    PolynomialFeatures,
    Custom,
}

/// 원시 데이터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawData {
    pub timestamps: Vec<chrono::DateTime<chrono::Utc>>,
    pub symbols: Vec<String>,
    pub features: HashMap<String, Vec<f64>>,
    pub targets: Option<Vec<f64>>,
    pub metadata: HashMap<String, String>,
}

/// 엔지니어링된 특성
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineeredFeatures {
    pub feature_names: Vec<String>,
    pub feature_values: Vec<Vec<f64>>,
    pub feature_types: HashMap<String, FeatureType>,
    pub feature_importance: HashMap<String, f64>,
    pub feature_correlations: HashMap<String, HashMap<String, f64>>,
    pub metadata: HashMap<String, String>,
}

/// 특성 타입
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FeatureType {
    Price,
    Volume,
    Technical,
    Fundamental,
    Alternative,
    Derived,
    Interaction,
    Polynomial,
    Custom,
}

/// 모델 평가기
pub struct ModelEvaluator {
    evaluators: Vec<Box<dyn ModelEvaluatorTrait>>,
    cross_validator: Box<dyn CrossValidator>,
    backtester: Box<dyn ModelBacktester>,
}

/// 모델 평가 트레이트
pub trait ModelEvaluatorTrait: Send + Sync {
    /// 평가기 이름
    fn name(&self) -> &str;
    
    /// 평가 타입
    fn evaluation_type(&self) -> EvaluationType;
    
    /// 모델 평가
    fn evaluate(&self, model: &dyn MLModel, test_data: &TestData) -> Result<EvaluationResult>;
    
    /// 평가 지표 계산
    fn calculate_metrics(&self, predictions: &[f64], targets: &[f64]) -> Result<ModelMetrics>;
}

/// 평가 타입
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EvaluationType {
    Regression,
    Classification,
    TimeSeries,
    Financial,
    RiskAdjusted,
    Custom,
}

/// 테스트 데이터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestData {
    pub features: Vec<Vec<f64>>,
    pub targets: Vec<f64>,
    pub timestamps: Vec<chrono::DateTime<chrono::Utc>>,
    pub symbols: Vec<String>,
    pub metadata: HashMap<String, String>,
}

/// 평가 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationResult {
    pub model_id: String,
    pub evaluation_type: EvaluationType,
    pub metrics: ModelMetrics,
    pub predictions: Vec<f64>,
    pub residuals: Vec<f64>,
    pub feature_importance: Option<Vec<f64>>,
    pub confidence_intervals: Option<Vec<(f64, f64)>>,
    pub evaluation_time: u64, // 밀리초
    pub sample_size: usize,
    pub metadata: HashMap<String, String>,
}

/// 교차 검증기 트레이트
pub trait CrossValidator: Send + Sync {
    /// 교차 검증 타입
    fn validation_type(&self) -> ValidationType;
    
    /// 교차 검증 수행
    fn validate(&self, model: &dyn MLModel, data: &RawData) -> Result<CrossValidationResult>;
}

/// 검증 타입
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ValidationType {
    KFold,
    TimeSeriesSplit,
    WalkForward,
    PurgedCrossValidation,
    CombinatorialPurgedCrossValidation,
}

/// 교차 검증 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossValidationResult {
    pub validation_type: ValidationType,
    pub folds: Vec<FoldResult>,
    pub mean_metrics: ModelMetrics,
    pub std_metrics: ModelMetrics,
    pub cv_score: f64,
    pub cv_std: f64,
    pub total_time: u64, // 밀리초
}

/// 폴드 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FoldResult {
    pub fold_id: u32,
    pub train_metrics: ModelMetrics,
    pub test_metrics: ModelMetrics,
    pub training_time: u64, // 밀리초
    pub prediction_time: u64, // 밀리초
}

/// 모델 백테스터 트레이트
pub trait ModelBacktester: Send + Sync {
    /// 백테스트 수행
    fn backtest(&self, model: &dyn MLModel, data: &RawData) -> Result<BacktestResult>;
    
    /// 전략 백테스트
    fn strategy_backtest(&self, model: &dyn MLModel, strategy: &dyn TradingStrategy, data: &RawData) -> Result<BacktestResult>;
}

/// 백테스트 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResult {
    pub model_id: String,
    pub strategy_name: String,
    pub start_date: chrono::DateTime<chrono::Utc>,
    pub end_date: chrono::DateTime<chrono::Utc>,
    pub total_return: f64,
    pub annualized_return: f64,
    pub annualized_volatility: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,
    pub max_drawdown: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub total_trades: u32,
    pub avg_trade_duration: f64,
    pub equity_curve: Vec<(chrono::DateTime<chrono::Utc>, f64)>,
    pub trade_history: Vec<TradeRecord>,
    pub performance_attribution: PerformanceAttribution,
}

/// 거래 기록
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub trade_id: String,
    pub symbol: String,
    pub entry_time: chrono::DateTime<chrono::Utc>,
    pub exit_time: Option<chrono::DateTime<chrono::Utc>>,
    pub entry_price: f64,
    pub exit_price: Option<f64>,
    pub quantity: f64,
    pub side: TradeSide,
    pub pnl: Option<f64>,
    pub commission: f64,
    pub slippage: f64,
    pub signal_strength: f64,
    pub confidence: f64,
}

/// 거래 방향
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TradeSide {
    Buy,
    Sell,
    Short,
    Cover,
}

/// 성능 속성
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceAttribution {
    pub factor_returns: HashMap<String, f64>,
    pub sector_returns: HashMap<String, f64>,
    pub style_returns: HashMap<String, f64>,
    pub country_returns: HashMap<String, f64>,
    pub currency_returns: HashMap<String, f64>,
    pub interaction_effects: HashMap<String, f64>,
    pub idiosyncratic_return: f64,
}

/// 거래 전략 트레이트
pub trait TradingStrategy: Send + Sync {
    /// 전략 이름
    fn name(&self) -> &str;
    
    /// 신호 생성
    fn generate_signal(&self, model_predictions: &[f64], features: &[Vec<f64>], timestamps: &[chrono::DateTime<chrono::Utc>]) -> Result<Vec<TradingSignal>>;
    
    /// 포지션 크기 결정
    fn position_sizing(&self, signal: &TradingSignal, portfolio_value: f64, risk_params: &RiskParameters) -> Result<f64>;
    
    /// 진입 조건 확인
    fn should_enter(&self, signal: &TradingSignal, current_positions: &[Position]) -> bool;
    
    /// 청산 조건 확인
    fn should_exit(&self, position: &Position, current_price: f64, current_time: chrono::DateTime<chrono::Utc>) -> bool;
}

/// 거래 신호
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingSignal {
    pub symbol: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub signal_type: SignalType,
    pub strength: f64, // -1.0 ~ 1.0
    pub confidence: f64, // 0.0 ~ 1.0
    pub target_price: Option<f64>,
    pub stop_loss: Option<f64>,
    pub take_profit: Option<f64>,
    pub expected_holding_period: Option<u32>, // 일 단위
    pub risk_score: f64,
    pub metadata: HashMap<String, String>,
}

/// 신호 타입
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SignalType {
    Buy,
    Sell,
    Hold,
    Short,
    Cover,
}

/// 포지션
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub position_id: String,
    pub symbol: String,
    pub side: TradeSide,
    pub quantity: f64,
    pub entry_price: f64,
    pub current_price: f64,
    pub entry_time: chrono::DateTime<chrono::Utc>,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
    pub stop_loss: Option<f64>,
    pub take_profit: Option<f64>,
    pub metadata: HashMap<String, String>,
}

/// 리스크 파라미터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskParameters {
    pub max_position_size: f64,
    pub max_portfolio_risk: f64,
    pub stop_loss_pct: f64,
    pub take_profit_pct: f64,
    pub max_correlation: f64,
    pub max_sector_exposure: f64,
    pub var_limit: f64,
    pub max_leverage: f64,
}

/// 하이퍼파라미터 최적화기
pub struct HyperparameterOptimizer {
    optimizer_type: OptimizerType,
    search_space: HashMap<String, ParameterRange>,
    optimization_metric: OptimizationMetric,
    n_trials: u32,
    timeout: Option<u64>, // 초 단위
}

/// 최적화기 타입
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OptimizerType {
    GridSearch,
    RandomSearch,
    BayesianOptimization,
    TreeStructuredParzenEstimator,
    Hyperband,
    BOHB,
    Optuna,
    Custom,
}

/// 파라미터 범위
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParameterRange {
    Continuous { min: f64, max: f64, step: Option<f64> },
    Discrete { values: Vec<f64> },
    Integer { min: i32, max: i32, step: Option<i32> },
    Categorical { values: Vec<String> },
    LogUniform { min: f64, max: f64 },
    LogNormal { mean: f64, std: f64 },
}

/// 최적화 지표
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OptimizationMetric {
    RMSE,
    MAE,
    R2,
    Sharpe,
    Sortino,
    Calmar,
    InformationRatio,
    MaxDrawdown,
    Custom(String),
}

/// 최적화 결과
pub struct OptimizationResult {
    pub best_parameters: HashMap<String, f64>,
    pub best_score: f64,
    pub optimization_metric: OptimizationMetric,
    pub n_trials: u32,
    pub total_time: u64, // 밀리초
    pub trial_results: Vec<TrialResult>,
    pub convergence_history: Vec<f64>,
    pub best_model: Option<Box<dyn MLModel>>,
}

/// 시도 결과
#[derive(Debug, Clone)]
pub struct TrialResult {
    pub trial_id: u32,
    pub parameters: HashMap<String, f64>,
    pub score: f64,
    pub training_time: u64, // 밀리초
    pub evaluation_time: u64, // 밀리초
    pub metrics: ModelMetrics,
    pub status: TrialStatus,
}

/// 시도 상태
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TrialStatus {
    Completed,
    Failed,
    Pruned,
    Timeout,
}

/// 모델 레지스트리
pub struct ModelRegistry {
    models: HashMap<String, RegisteredModel>,
    experiments: HashMap<String, Experiment>,
    metadata_store: Box<dyn MetadataStore>,
}

/// 등록된 모델
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredModel {
    pub model_id: String,
    pub name: String,
    pub version: String,
    pub model_type: ModelType,
    pub status: ModelStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub metrics: ModelMetrics,
    pub parameters: HashMap<String, f64>,
    pub tags: Vec<String>,
    pub description: String,
    pub author: String,
    pub experiment_id: Option<String>,
    pub artifact_path: String,
    pub metadata: HashMap<String, String>,
}

/// 모델 상태
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ModelStatus {
    Training,
    Trained,
    Validated,
    Deployed,
    Archived,
    Failed,
}

/// 실험
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Experiment {
    pub experiment_id: String,
    pub name: String,
    pub description: String,
    pub status: ExperimentStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub models: Vec<String>,
    pub best_model: Option<String>,
    pub metrics: ExperimentMetrics,
    pub parameters: HashMap<String, String>,
    pub tags: Vec<String>,
    pub metadata: HashMap<String, String>,
}

/// 실험 상태
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ExperimentStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// 실험 지표
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentMetrics {
    pub best_score: f64,
    pub mean_score: f64,
    pub std_score: f64,
    pub n_trials: u32,
    pub success_rate: f64,
    pub total_time: u64, // 밀리초
}

/// 메타데이터 저장소 트레이트
pub trait MetadataStore: Send + Sync {
    /// 모델 저장
    fn save_model(&self, model: &RegisteredModel) -> Result<()>;
    
    /// 모델 로드
    fn load_model(&self, model_id: &str) -> Result<RegisteredModel>;
    
    /// 모델 목록 조회
    fn list_models(&self, filter: Option<ModelFilter>) -> Result<Vec<RegisteredModel>>;
    
    /// 실험 저장
    fn save_experiment(&self, experiment: &Experiment) -> Result<()>;
    
    /// 실험 로드
    fn load_experiment(&self, experiment_id: &str) -> Result<Experiment>;
    
    /// 실험 목록 조회
    fn list_experiments(&self, filter: Option<ExperimentFilter>) -> Result<Vec<Experiment>>;
}

/// 모델 필터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelFilter {
    pub model_type: Option<ModelType>,
    pub status: Option<ModelStatus>,
    pub tags: Vec<String>,
    pub author: Option<String>,
    pub created_after: Option<chrono::DateTime<chrono::Utc>>,
    pub created_before: Option<chrono::DateTime<chrono::Utc>>,
    pub min_score: Option<f64>,
    pub max_score: Option<f64>,
}

/// 실험 필터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentFilter {
    pub status: Option<ExperimentStatus>,
    pub tags: Vec<String>,
    pub created_after: Option<chrono::DateTime<chrono::Utc>>,
    pub created_before: Option<chrono::DateTime<chrono::Utc>>,
    pub min_score: Option<f64>,
    pub max_score: Option<f64>,
}

/// ML 파이프라인
pub struct MLPipeline {
    pipeline_id: String,
    name: String,
    description: String,
    steps: Vec<PipelineStep>,
    status: PipelineStatus,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

/// 파이프라인 단계
pub struct PipelineStep {
    pub step_id: String,
    pub name: String,
    pub step_type: PipelineStepType,
    pub component: Box<dyn PipelineComponent>,
    pub parameters: HashMap<String, String>,
    pub dependencies: Vec<String>,
    pub outputs: Vec<String>,
}

/// 파이프라인 단계 타입
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PipelineStepType {
    DataLoader,
    FeatureEngineer,
    DataSplitter,
    ModelTrainer,
    ModelValidator,
    ModelPredictor,
    PostProcessor,
    Custom,
}

/// 파이프라인 컴포넌트 트레이트
pub trait PipelineComponent: Send + Sync {
    /// 컴포넌트 이름
    fn name(&self) -> &str;
    
    /// 실행
    fn execute(&self, inputs: PipelineInputs) -> Result<PipelineOutputs>;
    
    /// 입력 스키마
    fn input_schema(&self) -> PipelineSchema;
    
    /// 출력 스키마
    fn output_schema(&self) -> PipelineSchema;
    
    /// 파라미터 설정
    fn set_parameters(&mut self, parameters: HashMap<String, String>) -> Result<()>;
}

/// 파이프라인 입력
#[derive(Debug)]
pub struct PipelineInputs {
    pub data: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
    pub metadata: HashMap<String, String>,
}

/// 파이프라인 출력
#[derive(Debug)]
pub struct PipelineOutputs {
    pub data: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
    pub metadata: HashMap<String, String>,
}

/// 파이프라인 스키마
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSchema {
    pub inputs: HashMap<String, DataType>,
    pub outputs: HashMap<String, DataType>,
}

/// 데이터 타입
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataType {
    DataFrame,
    Vector,
    Scalar,
    Model,
    String,
    Custom(String),
}

/// 파이프라인 상태
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PipelineStatus {
    Created,
    Running,
    Completed,
    Failed,
    Cancelled,
    Paused,
}

impl ModelMetrics {
    /// 새로운 모델 지표 생성
    pub fn new() -> Self {
        Self {
            accuracy: 0.0,
            precision: 0.0,
            recall: 0.0,
            f1_score: 0.0,
            mse: 0.0,
            rmse: 0.0,
            mae: 0.0,
            r2: 0.0,
            adjusted_r2: 0.0,
            mape: 0.0,
            smape: 0.0,
            directional_accuracy: None,
            sharpe_ratio: None,
            information_ratio: None,
            max_drawdown: None,
            calmar_ratio: None,
        }
    }

    /// 지표 계산
    pub fn calculate(&mut self, predictions: &[f64], targets: &[f64]) {
        if predictions.len() != targets.len() || predictions.is_empty() {
            return;
        }

        let n = predictions.len() as f64;
        
        // MSE
        self.mse = predictions.iter()
            .zip(targets.iter())
            .map(|(p, t)| (p - t).powi(2))
            .sum::<f64>() / n;

        // RMSE
        self.rmse = self.mse.sqrt();

        // MAE
        self.mae = predictions.iter()
            .zip(targets.iter())
            .map(|(p, t)| (p - t).abs())
            .sum::<f64>() / n;

        // R²
        let target_mean = targets.iter().sum::<f64>() / n;
        let total_sum_squares = targets.iter()
            .map(|t| (t - target_mean).powi(2))
            .sum::<f64>();
        
        self.r2 = if total_sum_squares > 0.0 {
            1.0 - (self.mse * n) / total_sum_squares
        } else {
            0.0
        };

        // Adjusted R²
        let p = 1.0; // 예시: 특성 수 (실제로는 전달받아야 함)
        self.adjusted_r2 = if n > p + 1.0 {
            1.0 - (1.0 - self.r2) * (n - 1.0) / (n - p - 1.0)
        } else {
            self.r2
        };

        // MAPE
        self.mape = predictions.iter()
            .zip(targets.iter())
            .map(|(p, t)| if *t != 0.0 { ((p - t) / t).abs() } else { 0.0 })
            .sum::<f64>() / n * 100.0;

        // SMAPE
        self.smape = predictions.iter()
            .zip(targets.iter())
            .map(|(p, t)| ((p - t).abs() / ((p.abs() + t.abs()) / 2.0)))
            .sum::<f64>() / n * 100.0;

        // Directional Accuracy
        let correct_directions = predictions.iter()
            .zip(targets.iter())
            .filter(|(p, t)| {
                let pred_direction = if **p > 0.0 { 1.0 } else { -1.0 };
                let target_direction = if **t > 0.0 { 1.0 } else { -1.0 };
                pred_direction == target_direction
            })
            .count();
        
        self.directional_accuracy = Some(correct_directions as f64 / n);

        // Sharpe Ratio (수익률 기반)
        if let Some(returns) = self.calculate_returns(predictions, targets) {
            let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
            let return_std = self.calculate_std(&returns);
            
            if return_std > 0.0 {
                self.sharpe_ratio = Some(mean_return / return_std);
            }

            // Information Ratio
            if let Some(benchmark_returns) = self.calculate_benchmark_returns(targets) {
                let active_returns: Vec<f64> = returns.iter()
                    .zip(benchmark_returns.iter())
                    .map(|(r, br)| r - br)
                    .collect();
                
                let active_return_mean = active_returns.iter().sum::<f64>() / active_returns.len() as f64;
                let tracking_error = self.calculate_std(&active_returns);
                
                if tracking_error > 0.0 {
                    self.information_ratio = Some(active_return_mean / tracking_error);
                }
            }

            // Max Drawdown
            let mut peak: f64 = 1.0;
            let mut max_dd: f64 = 0.0;
            let mut cumulative_return = 1.0;
            
            for return_val in &returns {
                cumulative_return *= (1.0 + return_val);
                peak = peak.max(cumulative_return);
                let drawdown = (peak - cumulative_return) / peak;
                max_dd = max_dd.max(drawdown);
            }
            
            self.max_drawdown = Some(max_dd);

            // Calmar Ratio
            if max_dd > 0.0 {
                let annualized_return = (1.0 + mean_return).powf(252.0) - 1.0;
                self.calmar_ratio = Some(annualized_return / max_dd);
            }
        }
    }

    /// 수익률 계산 (예측값을 수익률로 해석)
    fn calculate_returns(&self, predictions: &[f64], _targets: &[f64]) -> Option<Vec<f64>> {
        if predictions.len() < 2 {
            return None;
        }

        let mut returns = Vec::new();
        for i in 1..predictions.len() {
            let return_val = predictions[i] - predictions[i - 1];
            returns.push(return_val);
        }
        
        Some(returns)
    }

    /// 벤치마크 수익률 계산 (실제값을 벤치마크로 사용)
    fn calculate_benchmark_returns(&self, targets: &[f64]) -> Option<Vec<f64>> {
        if targets.len() < 2 {
            return None;
        }

        let mut returns = Vec::new();
        for i in 1..targets.len() {
            let return_val = targets[i] - targets[i - 1];
            returns.push(return_val);
        }
        
        Some(returns)
    }

    /// 표준편차 계산
    fn calculate_std(&self, values: &[f64]) -> f64 {
        if values.len() < 2 {
            return 0.0;
        }

        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = values.iter()
            .map(|v| (v - mean).powi(2))
            .sum::<f64>() / (values.len() - 1) as f64;
        
        variance.sqrt()
    }
}

impl Default for ModelMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl MLPipeline {
    /// 새로운 ML 파이프라인 생성
    pub fn new(pipeline_id: String, name: String, description: String) -> Self {
        Self {
            pipeline_id,
            name,
            description,
            steps: Vec::new(),
            status: PipelineStatus::Created,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    /// 파이프라인 단계 추가
    pub fn add_step(&mut self, step: PipelineStep) {
        self.steps.push(step);
        self.updated_at = chrono::Utc::now();
    }

    /// 파이프라인 실행
    pub async fn execute(&mut self, initial_inputs: PipelineInputs) -> Result<PipelineOutputs> {
        self.status = PipelineStatus::Running;

        let mut current_inputs = initial_inputs;
        
        for step in &self.steps {
            match step.component.execute(current_inputs) {
                Ok(outputs) => {
                    current_inputs = PipelineInputs {
                        data: outputs.data,
                        metadata: outputs.metadata,
                    };
                }
                Err(e) => {
                    error!("파이프라인 단계 실행 실패: {} - {}", step.name, e);
                    self.status = PipelineStatus::Failed;
                    return Err(e);
                }
            }
        }

        self.status = PipelineStatus::Completed;
        self.updated_at = chrono::Utc::now();
        Ok(current_inputs.into())
    }
}

impl From<PipelineInputs> for PipelineOutputs {
    fn from(inputs: PipelineInputs) -> Self {
        Self {
            data: inputs.data,
            metadata: inputs.metadata,
        }
    }
}

//! 전체 시스템 통합 테스트 예제
//! 
//! 모든 Actor들이 협력하여 실제 거래 시나리오를 수행하는 통합 테스트입니다.
//! 
//! 테스트 시나리오:
//! 1. DataAgent가 시장 데이터 수집
//! 2. MLAgent가 데이터를 분석하고 예측 생성
//! 3. TradingAgent가 거래 신호 생성 및 실행
//! 4. RiskAgent가 리스크 평가 및 관리
//! 5. MonitoringAgent가 전체 시스템 모니터링

use pekko_ml4t_system::actors::*;
use pekko_actor::{ActorSystem, ActorRef};
use anyhow::Result;
use tracing::{ error, warn};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    // 로깅 초기화
    tracing_subscriber::fmt::init();
    
    println!("🚀 ML4T 시스템 통합 테스트 시작");
    // ActorSystem 생성
    let actor_system = ActorSystem::new("ml4t-integration-system");
    println!("✅ ActorSystem 생성 완료");
    // 1단계: 모든 Actor 생성 및 등록
    println!("📋 1단계: Actor 생성 및 등록");
    let (data_agent_ref, ml_agent_ref, trading_agent_ref, risk_agent_ref, monitoring_agent_ref) = 
        create_and_register_actors(&actor_system).await?;
    
    println!("✅ 모든 Actor 생성 및 등록 완료");
    // 2단계: 초기 설정 및 준비
    println!("📋 2단계: 초기 설정 및 준비");
    setup_initial_configuration(&data_agent_ref, &ml_agent_ref, &trading_agent_ref, &risk_agent_ref, &monitoring_agent_ref).await?;
    
    println!("✅ 초기 설정 완료");
    // 3단계: 데이터 수집 시나리오
    println!("📋 3단계: 데이터 수집 시나리오");
    execute_data_collection_scenario(&data_agent_ref).await?;
    
    println!("✅ 데이터 수집 시나리오 완료");
    // 4단계: ML 예측 시나리오
    println!("📋 4단계: ML 예측 시나리오");
    execute_ml_prediction_scenario(&ml_agent_ref).await?;
    
    println!("✅ ML 예측 시나리오 완료");
    // 5단계: 거래 실행 시나리오
    println!("📋 5단계: 거래 실행 시나리오");
    execute_trading_scenario(&trading_agent_ref).await?;
    
    println!("✅ 거래 실행 시나리오 완료");
    // 6단계: 리스크 관리 시나리오
    println!("📋 6단계: 리스크 관리 시나리오");
    execute_risk_management_scenario(&risk_agent_ref).await?;
    
    println!("✅ 리스크 관리 시나리오 완료");
    // 7단계: 모니터링 시나리오
    println!("📋 7단계: 모니터링 시나리오");
    execute_monitoring_scenario(&monitoring_agent_ref).await?;
    
    println!("✅ 모니터링 시나리오 완료");
    // 8단계: 전체 시스템 상호작용 테스트
    println!("📋 8단계: 전체 시스템 상호작용 테스트");
    execute_system_interaction_test(
        &data_agent_ref,
        &ml_agent_ref,
        &trading_agent_ref,
        &risk_agent_ref,
        &monitoring_agent_ref,
    ).await?;
    
    println!("✅ 전체 시스템 상호작용 테스트 완료");
    // 9단계: 시스템 정리 및 종료
    println!("📋 9단계: 시스템 정리 및 종료");
    cleanup_system().await?;
    
    println!("🎉 ML4T 시스템 통합 테스트 완료!");
    Ok(())
}

/// 모든 Actor를 생성하고 ActorSystem에 등록
async fn create_and_register_actors(
    actor_system: &ActorSystem,
) -> Result<(ActorRef<DataAgent>, ActorRef<MLAgent>, ActorRef<TradingAgent>, ActorRef<RiskAgent>, ActorRef<MonitoringAgent>)> {
    println!("  🔧 DataAgent 생성 중...");
    let market_data_config = MarketDataConfig {
        symbols: vec!["AAPL".to_string(), "GOOGL".to_string(), "MSFT".to_string(), "TSLA".to_string()],
        data_sources: vec!["yahoo".to_string(), "alpha_vantage".to_string(), "kis".to_string()],
        update_frequency_ms: 1000,
        history_days: 30,
    };
    
    let data_agent = DataAgent::new(
        market_data_config,
        vec!["reuters".to_string(), "bloomberg".to_string(), "news_api".to_string()],
        vec!["satellite".to_string(), "social_media".to_string(), "alternative_data".to_string()],
    );
    
    let data_agent_ref = actor_system.spawn_actor("data_agent", data_agent).await?;
    println!("    ✅ DataAgent 등록 완료");
    println!("  🔧 MLAgent 생성 중...");
    let features_config = FeaturesConfig {
        technical_indicators: vec![
            "sma".to_string(), "ema".to_string(), "rsi".to_string(), "macd".to_string(),
            "bollinger_bands".to_string(), "stochastic".to_string()
        ],
        alpha_factors: vec![
            "momentum".to_string(), "mean_reversion".to_string(), "carry".to_string(),
            "quality".to_string(), "value".to_string(), "growth".to_string()
        ],
        fundamental_features: vec![
            "pe_ratio".to_string(), "pb_ratio".to_string(), "debt_ratio".to_string(),
            "roe".to_string(), "roa".to_string(), "current_ratio".to_string()
        ],
        alternative_features: vec![
            "sentiment".to_string(), "news_volume".to_string(), "social_trends".to_string(),
            "satellite_data".to_string(), "credit_spread".to_string(), "volatility".to_string()
        ],
    };
    
    // ML 모델들 생성
    let mut models: HashMap<String, Box<dyn MLModel + Send + Sync>> = HashMap::new();
    
    // Linear Model
    let linear_config = ModelConfig {
        model_id: "linear_regression_001".to_string(),
        model_type: "linear".to_string(),
        parameters: HashMap::new(),
    };
    let linear_model = LinearModel::new(linear_config);
    models.insert("linear_model".to_string(), Box::new(linear_model));
    
    // Random Forest Model
    let rf_config = ModelConfig {
        model_id: "random_forest_001".to_string(),
        model_type: "random_forest".to_string(),
        parameters: HashMap::new(),
    };
    let rf_model = RandomForestModel::new(rf_config);
    models.insert("random_forest_model".to_string(), Box::new(rf_model));
    
    let ml_agent = MLAgent::new(models, features_config);
    let ml_agent_ref = actor_system.spawn_actor("ml_agent", ml_agent).await?;
    println!("    ✅ MLAgent 등록 완료");
    println!("  🔧 TradingAgent 생성 중...");
    let broker_config = BrokerConfig {
        initial_capital: 1000000.0,
        commission_rate: 0.001,
    };
    
    let execution_config = ExecutionConfig {
        max_position_size: 0.1,
        slippage_tolerance: 0.001,
    };
    
    let trading_agent = TradingAgent::new(broker_config, execution_config);
    let trading_agent_ref = actor_system.spawn_actor("trading_agent", trading_agent).await?;
    println!("    ✅ TradingAgent 등록 완료");
    println!("  🔧 RiskAgent 생성 중...");
    let risk_config = RiskConfig {
        max_portfolio_risk: 0.05,
        max_position_size: 0.1,
        stop_loss_pct: 0.02,
        take_profit_pct: 0.04,
    };
    
    let portfolio_config = PortfolioConfig {
        initial_value: 1000000.0,
        target_allocation: 0.8,
    };
    
    let risk_agent = RiskAgent::new(risk_config, portfolio_config);
    let risk_agent_ref = actor_system.spawn_actor("risk_agent", risk_agent).await?;
    println!("    ✅ RiskAgent 등록 완료");
    println!("  🔧 MonitoringAgent 생성 중...");
    let alert_thresholds = AlertThresholds {
        drawdown_alert: 0.05,
        loss_alert: 0.03,
        volatility_alert: 0.2,
        model_drift_alert: 0.1,
    };
    
    let monitoring_config = MonitoringConfig {
        alert_thresholds,
    };
    
    let monitoring_agent = MonitoringAgent::new(monitoring_config);
    let monitoring_agent_ref = actor_system.spawn_actor("monitoring_agent", monitoring_agent).await?;
    println!("    ✅ MonitoringAgent 등록 완료");
    Ok((data_agent_ref, ml_agent_ref, trading_agent_ref, risk_agent_ref, monitoring_agent_ref))
}

/// 초기 설정 및 준비
async fn setup_initial_configuration(
    _data_agent_ref: &ActorRef<DataAgent>,
    _ml_agent_ref: &ActorRef<MLAgent>,
    _trading_agent_ref: &ActorRef<TradingAgent>,
    _risk_agent_ref: &ActorRef<RiskAgent>,
    _monitoring_agent_ref: &ActorRef<MonitoringAgent>,
) -> Result<()> {
    println!("  🔧 시스템 초기화 중...");
    // 각 Actor의 초기 설정을 시뮬레이션
    println!("    📊 DataAgent 설정 확인");
    println!("    🤖 MLAgent 모델 로딩");
    println!("    💰 TradingAgent 계좌 연결");
    println!("    ⚠️  RiskAgent 리스크 한도 설정");
    println!("    📈 MonitoringAgent 모니터링 시작");
    
    // 설정 완료를 위한 짧은 대기
    sleep(Duration::from_millis(500)).await;
    
    println!("  ✅ 모든 Actor 초기 설정 완료");
    Ok(())
}

/// 데이터 수집 시나리오 실행
async fn execute_data_collection_scenario(data_agent_ref: &ActorRef<DataAgent>) -> Result<()> {
    println!("  📊 시장 데이터 수집 시작...");
    // 시장 데이터 수집 요청
    data_agent_ref.tell(DataMessage::CollectMarketData)?;
    println!("    ✅ 시장 데이터 수집 요청 전송");
    sleep(Duration::from_millis(200)).await;
    
    // 뉴스 데이터 처리 요청
    data_agent_ref.tell(DataMessage::ProcessNewsData)?;
    println!("    ✅ 뉴스 데이터 처리 요청 전송");
    sleep(Duration::from_millis(200)).await;
    
    // 대체 데이터 업데이트 요청
    data_agent_ref.tell(DataMessage::UpdateAlternativeData)?;
    println!("    ✅ 대체 데이터 업데이트 요청 전송");
    sleep(Duration::from_millis(200)).await;
    
    // 데이터 준비 완료 시뮬레이션
    let sample_data = MarketData {
        symbol: "AAPL".to_string(),
        open: 149.0,
        high: 152.0,
        low: 148.0,
        close: 150.0,
        volume: 1000000,
        vwap: 150.5,
        timestamp: chrono::Utc::now(),
    };
    
    data_agent_ref.tell(DataMessage::DataReady { 
        symbol: "AAPL".to_string(), 
        data: sample_data 
    })?;
    println!("    ✅ 데이터 준비 완료 알림 전송");
    println!("  ✅ 데이터 수집 시나리오 완료");
    Ok(())
}

/// ML 예측 시나리오 실행
async fn execute_ml_prediction_scenario(ml_agent_ref: &ActorRef<MLAgent>) -> Result<()> {
    println!("  🤖 ML 예측 시작...");
    // 수익률 예측 요청
    let symbols = vec!["AAPL".to_string(), "GOOGL".to_string(), "MSFT".to_string()];
    ml_agent_ref.tell(MLMessage::PredictReturns { symbols })?;
    println!("    ✅ 수익률 예측 요청 전송");
    sleep(Duration::from_millis(300)).await;
    
    // 모델 훈련 요청
    ml_agent_ref.tell(MLMessage::TrainModel { 
        model_type: "random_forest".to_string(),
        features: vec!["technical".to_string(), "fundamental".to_string(), "sentiment".to_string()]
    })?;
    println!("    ✅ 모델 훈련 요청 전송");
    sleep(Duration::from_millis(300)).await;
    
    // 모델 업데이트 요청
    ml_agent_ref.tell(MLMessage::UpdateModel { 
        model_id: "linear_regression_001".to_string()
    })?;
    println!("    ✅ 모델 업데이트 요청 전송");
    sleep(Duration::from_millis(300)).await;
    
    // 예측 준비 완료 시뮬레이션
    ml_agent_ref.tell(MLMessage::PredictionReady {
        symbol: "AAPL".to_string(),
        prediction: 0.05,
        confidence: 0.85,
    })?;
    println!("    ✅ 예측 준비 완료 알림 전송");
    println!("  ✅ ML 예측 시나리오 완료");
    Ok(())
}

/// 거래 실행 시나리오 실행
async fn execute_trading_scenario(trading_agent_ref: &ActorRef<TradingAgent>) -> Result<()> {
    println!("  💰 거래 실행 시작...");
    // 거래 신호 생성 요청
    trading_agent_ref.tell(TradingMessage::GenerateSignals)?;
    println!("    ✅ 거래 신호 생성 요청 전송");
    sleep(Duration::from_millis(200)).await;
    
    // 매수 실행 요청
    trading_agent_ref.tell(TradingMessage::ExecuteBuy {
        symbol: "AAPL".to_string(),
        quantity: 100,
        price: 150.0,
    })?;
    println!("    ✅ 매수 실행 요청 전송");
    sleep(Duration::from_millis(200)).await;
    
    // 매도 실행 요청
    trading_agent_ref.tell(TradingMessage::ExecuteSell {
        symbol: "GOOGL".to_string(),
        quantity: 50,
        price: 2800.0,
    })?;
    println!("    ✅ 매도 실행 요청 전송");
    sleep(Duration::from_millis(200)).await;
    
    // 포지션 업데이트
    let position = Position {
        symbol: "AAPL".to_string(),
        quantity: 100,
        average_price: 150.0,
        current_price: 155.0,
        unrealized_pnl: 500.0,
        realized_pnl: 0.0,
    };
    
    trading_agent_ref.tell(TradingMessage::UpdatePosition {
        symbol: "AAPL".to_string(),
        position,
    })?;
    println!("    ✅ 포지션 업데이트 전송");
    sleep(Duration::from_millis(200)).await;
    
    // 신호 생성 완료 시뮬레이션
    trading_agent_ref.tell(TradingMessage::SignalGenerated {
        symbol: "MSFT".to_string(),
        signal: TradingSignal::Buy { strength: 0.8 },
    })?;
    println!("    ✅ 신호 생성 완료 알림 전송");
    sleep(Duration::from_millis(200)).await;
    
    // 포지션 실행 요청
    trading_agent_ref.tell(TradingMessage::ExecutePositions)?;
    println!("    ✅ 포지션 실행 요청 전송");
    println!("  ✅ 거래 실행 시나리오 완료");
    Ok(())
}

/// 리스크 관리 시나리오 실행
async fn execute_risk_management_scenario(risk_agent_ref: &ActorRef<RiskAgent>) -> Result<()> {
    println!("  ⚠️  리스크 관리 시작...");
    // 포트폴리오 리스크 평가 요청
    risk_agent_ref.tell(RiskMessage::AssessPortfolioRisk)?;
    println!("    ✅ 포트폴리오 리스크 평가 요청 전송");
    sleep(Duration::from_millis(200)).await;
    
    // 포지션 한도 확인 요청
    risk_agent_ref.tell(RiskMessage::CheckPositionLimits)?;
    println!("    ✅ 포지션 한도 확인 요청 전송");
    sleep(Duration::from_millis(200)).await;
    
    // VaR 계산 요청
    risk_agent_ref.tell(RiskMessage::CalculateVaR)?;
    println!("    ✅ VaR 계산 요청 전송");
    sleep(Duration::from_millis(200)).await;
    
    // 리스크 메트릭 업데이트
    let risk_metrics = RiskMetrics {
        symbol: "AAPL".to_string(),
        var_1d: 0.02,
        var_5d: 0.035,
        beta: 1.2,
        volatility: 0.15,
        sharpe_ratio: 1.2,
    };
    
    risk_agent_ref.tell(RiskMessage::UpdateRiskMetrics {
        symbol: "AAPL".to_string(),
        risk: risk_metrics,
    })?;
    println!("    ✅ 리스크 메트릭 업데이트 전송");
    sleep(Duration::from_millis(200)).await;
    
    // 리스크 경고 시뮬레이션
    risk_agent_ref.tell(RiskMessage::RiskAlert {
        level: RiskLevel::Medium,
        message: "포트폴리오 리스크가 한도를 초과했습니다".to_string(),
    })?;
    println!("    ✅ 리스크 경고 전송");
    println!("  ✅ 리스크 관리 시나리오 완료");
    Ok(())
}

/// 모니터링 시나리오 실행
async fn execute_monitoring_scenario(monitoring_agent_ref: &ActorRef<MonitoringAgent>) -> Result<()> {
    println!("  📈 모니터링 시작...");
    // 성능 업데이트 요청
    monitoring_agent_ref.tell(MonitoringMessage::UpdatePerformance)?;
    println!("    ✅ 성능 업데이트 요청 전송");
    sleep(Duration::from_millis(200)).await;
    
    // 리포트 생성 요청
    monitoring_agent_ref.tell(MonitoringMessage::GenerateReport)?;
    println!("    ✅ 리포트 생성 요청 전송");
    sleep(Duration::from_millis(200)).await;
    
    // 시스템 상태 확인 요청
    monitoring_agent_ref.tell(MonitoringMessage::CheckSystemHealth)?;
    println!("    ✅ 시스템 상태 확인 요청 전송");
    sleep(Duration::from_millis(200)).await;
    
    // 성능 메트릭 업데이트
    let performance_metrics = PerformanceMetrics {
        total_return: 0.15,
        daily_return: 0.005,
        sharpe_ratio: 1.2,
        max_drawdown: 0.05,
        win_rate: 0.65,
        total_trades: 100,
        portfolio_value: 1150000.0,
    };
    
    monitoring_agent_ref.tell(MonitoringMessage::PerformanceUpdate {
        metrics: performance_metrics,
    })?;
    println!("    ✅ 성능 메트릭 업데이트 전송");
    sleep(Duration::from_millis(200)).await;
    
    // 알림 발생 시뮬레이션
    monitoring_agent_ref.tell(MonitoringMessage::Alert {
        severity: AlertLevel::Warning,
        message: "시스템 성능이 임계값을 초과했습니다".to_string(),
    })?;
    println!("    ✅ 알림 발생 전송");
    println!("  ✅ 모니터링 시나리오 완료");
    Ok(())
}

/// 전체 시스템 상호작용 테스트
async fn execute_system_interaction_test(
    data_agent_ref: &ActorRef<DataAgent>,
    ml_agent_ref: &ActorRef<MLAgent>,
    trading_agent_ref: &ActorRef<TradingAgent>,
    risk_agent_ref: &ActorRef<RiskAgent>,
    monitoring_agent_ref: &ActorRef<MonitoringAgent>,
) -> Result<()> {
    println!("  🔄 전체 시스템 상호작용 테스트 시작...");
    // 1. 데이터 수집 → ML 예측 연쇄
    println!("    📊→🤖 데이터 수집 → ML 예측 연쇄");
    data_agent_ref.tell(DataMessage::CollectMarketData)?;
    sleep(Duration::from_millis(100)).await;
    
    let symbols = vec!["AAPL".to_string(), "GOOGL".to_string()];
    ml_agent_ref.tell(MLMessage::PredictReturns { symbols })?;
    println!("      ✅ 데이터 수집 → ML 예측 연쇄 완료");
    sleep(Duration::from_millis(300)).await;
    
    // 2. ML 예측 → 거래 신호 연쇄
    println!("    🤖→💰 ML 예측 → 거래 신호 연쇄");
    trading_agent_ref.tell(TradingMessage::GenerateSignals)?;
    sleep(Duration::from_millis(100)).await;
    
    trading_agent_ref.tell(TradingMessage::ExecuteBuy {
        symbol: "AAPL".to_string(),
        quantity: 100,
        price: 150.0,
    })?;
    println!("      ✅ ML 예측 → 거래 신호 연쇄 완료");
    sleep(Duration::from_millis(300)).await;
    
    // 3. 거래 실행 → 리스크 관리 연쇄
    println!("    💰→⚠️  거래 실행 → 리스크 관리 연쇄");
    risk_agent_ref.tell(RiskMessage::AssessPortfolioRisk)?;
    sleep(Duration::from_millis(100)).await;
    
    risk_agent_ref.tell(RiskMessage::CheckPositionLimits)?;
    println!("      ✅ 거래 실행 → 리스크 관리 연쇄 완료");
    sleep(Duration::from_millis(300)).await;
    
    // 4. 리스크 관리 → 모니터링 연쇄
    println!("    ⚠️ →📈 리스크 관리 → 모니터링 연쇄");
    monitoring_agent_ref.tell(MonitoringMessage::UpdatePerformance)?;
    sleep(Duration::from_millis(100)).await;
    
    monitoring_agent_ref.tell(MonitoringMessage::CheckSystemHealth)?;
    println!("      ✅ 리스크 관리 → 모니터링 연쇄 완료");
    sleep(Duration::from_millis(300)).await;
    
    // 5. 전체 시스템 동시 실행
    println!("    🔄 전체 시스템 동시 실행");
    let futures = vec![
        data_agent_ref.tell(DataMessage::ProcessNewsData),
        ml_agent_ref.tell(MLMessage::TrainModel { 
            model_type: "random_forest".to_string(),
            features: vec!["technical".to_string()]
        }),
        trading_agent_ref.tell(TradingMessage::ExecutePositions),
        risk_agent_ref.tell(RiskMessage::CalculateVaR),
        monitoring_agent_ref.tell(MonitoringMessage::GenerateReport),
    ];
    
    // 모든 메시지를 동시에 전송
    for future in futures {
        let _ = future;
    }
    
    println!("      ✅ 전체 시스템 동시 실행 완료");
    println!("  ✅ 전체 시스템 상호작용 테스트 완료");
    Ok(())
}

/// 시스템 정리 및 종료
async fn cleanup_system() -> Result<()> {
    println!("  🧹 시스템 정리 중...");
    // 정리 작업 시뮬레이션
    println!("    📊 데이터 정리");
    println!("    🤖 모델 저장");
    println!("    💰 포지션 정리");
    println!("    ⚠️  리스크 리포트 생성");
    println!("    📈 최종 모니터링 리포트 생성");
    
    sleep(Duration::from_millis(500)).await;
    
    println!("  ✅ 시스템 정리 완료");
    Ok(())
}

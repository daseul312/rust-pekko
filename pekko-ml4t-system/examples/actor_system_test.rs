//! Actor 시스템 실행 테스트
//!
//! Actor 생성 및 메시지 전달이 제대로 작동하는지 확인합니다.

use pekko_ml4t_system::actors;
use pekko_actor::ActorSystem;
use anyhow::Result;
use tracing::{ error};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    // 로깅 초기화
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
            .add_directive(tracing::Level::INFO.into()))
        .init();
    // Actor 시스템 생성
    let actor_system = ActorSystem::new("test-ml4t-system");
    // 1단계: Actor 생성 테스트
    if let Err(e) = create_all_actors(&actor_system).await {
        error!("❌ Actor 생성 실패: {}", e);
        return Err(e);
    }
    // 2단계: 메시지 전달 테스트
    if let Err(e) = test_message_passing(&actor_system).await {
        error!("❌ 메시지 전달 테스트 실패: {}", e);
        return Err(e);
    }
    // 3단계: Actor 간 통신 체인 테스트
    if let Err(e) = test_actor_chain(&actor_system).await {
        error!("❌ 통신 체인 테스트 실패: {}", e);
        return Err(e);
    }
    // 시스템이 메시지를 처리할 시간 주기
    sleep(Duration::from_secs(2)).await;
    Ok(())
}

/// 모든 Actor 생성
async fn create_all_actors(actor_system: &ActorSystem) -> Result<()> {
    use std::collections::HashMap;

    // 1. DataAgent
    let market_data_config = actors::data_agent::MarketDataConfig {
        symbols: vec!["AAPL".to_string(), "GOOGL".to_string()],
        data_sources: vec!["test_source".to_string()],
        update_frequency_ms: 5000,
        history_days: 30,
    };
    let data_agent = actors::DataAgent::new(
        market_data_config,
        vec![],
        vec![],
    );
    actor_system.spawn(data_agent, "data-agent").await?;
    // 2. MLAgent
    let models = HashMap::new();
    let features_config = actors::ml_agent::FeaturesConfig {
        technical_indicators: vec!["SMA".to_string()],
        alpha_factors: vec!["momentum".to_string()],
        fundamental_features: vec![],
        alternative_features: vec![],
    };
    let ml_agent = actors::MLAgent::new(models, features_config);
    actor_system.spawn(ml_agent, "ml-agent").await?;
    // 3. TradingAgent
    let broker_config = actors::trading_agent::BrokerConfig {
        initial_capital: 100000.0,
        commission_rate: 0.001,
    };
    let execution_config = actors::trading_agent::ExecutionConfig {
        max_position_size: 10000.0,
        slippage_tolerance: 0.001,
    };
    let trading_agent = actors::TradingAgent::new(broker_config, execution_config);
    actor_system.spawn(trading_agent, "trading-agent").await?;
    // 4. RiskAgent
    let risk_config = actors::risk_agent::RiskConfig {
        max_portfolio_risk: 0.10,
        max_position_size: 10000.0,
        stop_loss_pct: 0.02,
        take_profit_pct: 0.05,
    };
    let portfolio_config = actors::risk_agent::PortfolioConfig {
        initial_value: 100000.0,
        target_allocation: 0.95,
    };
    let risk_agent = actors::RiskAgent::new(risk_config, portfolio_config);
    actor_system.spawn(risk_agent, "risk-agent").await?;
    // 5. MonitoringAgent
    let alert_thresholds = actors::monitoring_agent::AlertThresholds {
        drawdown_alert: 0.10,
        loss_alert: 0.05,
        volatility_alert: 0.30,
        model_drift_alert: 0.15,
    };
    let monitoring_config = actors::monitoring_agent::MonitoringConfig {
        alert_thresholds,
    };
    let monitoring_agent = actors::MonitoringAgent::new(monitoring_config);
    actor_system.spawn(monitoring_agent, "monitoring-agent").await?;
    Ok(())
}

/// 메시지 전달 테스트
async fn test_message_passing(actor_system: &ActorSystem) -> Result<()> {
    // DataAgent에게 데이터 수집 요청
    let message = actors::DataMessage::CollectMarketData;
    actor_system.send_message::<actors::DataAgent>("data-agent", message).await?;
    sleep(Duration::from_millis(100)).await;

    // MLAgent에게 예측 요청
    let message = actors::MLMessage::PredictReturns {
        symbols: vec!["AAPL".to_string()],
    };
    actor_system.send_message::<actors::MLAgent>("ml-agent", message).await?;
    sleep(Duration::from_millis(100)).await;

    // RiskAgent에게 리스크 평가 요청
    let message = actors::RiskMessage::AssessPortfolioRisk;
    actor_system.send_message::<actors::RiskAgent>("risk-agent", message).await?;
    sleep(Duration::from_millis(100)).await;

    // MonitoringAgent에게 성능 업데이트 요청
    let message = actors::MonitoringMessage::UpdatePerformance;
    actor_system.send_message::<actors::MonitoringAgent>("monitoring-agent", message).await?;
    Ok(())
}

/// Actor 간 통신 체인 테스트
async fn test_actor_chain(actor_system: &ActorSystem) -> Result<()> {
    // 1. 데이터 수집
    let message = actors::DataMessage::CollectMarketData;
    actor_system.send_message::<actors::DataAgent>("data-agent", message).await?;
    sleep(Duration::from_millis(200)).await;

    // 2. ML 예측
    let message = actors::MLMessage::PredictReturns {
        symbols: vec!["AAPL".to_string(), "GOOGL".to_string()],
    };
    actor_system.send_message::<actors::MLAgent>("ml-agent", message).await?;
    sleep(Duration::from_millis(200)).await;

    // 3. 거래 신호 생성
    let message = actors::TradingMessage::GenerateSignals;
    actor_system.send_message::<actors::TradingAgent>("trading-agent", message).await?;
    sleep(Duration::from_millis(200)).await;

    // 4. 리스크 평가
    let message = actors::RiskMessage::AssessPortfolioRisk;
    actor_system.send_message::<actors::RiskAgent>("risk-agent", message).await?;
    sleep(Duration::from_millis(200)).await;

    // 5. 성능 모니터링
    let message = actors::MonitoringMessage::UpdatePerformance;
    actor_system.send_message::<actors::MonitoringAgent>("monitoring-agent", message).await?;
    sleep(Duration::from_millis(200)).await;
    Ok(())
}

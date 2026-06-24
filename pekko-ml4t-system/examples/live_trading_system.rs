//! 실제 투자 시스템 - ML4T Actor 기반 자동매매
//!
//! 한국투자증권 API와 ML4T Actor 시스템을 결합하여 실제 투자를 수행합니다.

use pekko_ml4t_system::actors;
use pekko_actor::ActorSystem;
use anyhow::Result;
use tracing::{ warn, error};
use tokio::time::{sleep, Duration};
use chrono::{Local, Datelike};

// 한국투자증권 API 모듈 (stock crate에서 가져오기)
// 실제 환경에서는 use stock::korea_investment_api::*; 형태로 사용
// 현재는 시뮬레이션을 위해 간단한 구조체 정의

/// 실제 투자 시스템
pub struct LiveTradingSystem {
    actor_system: ActorSystem,
    // kis_api: KoreaInvestmentAPI,  // 실제 환경에서 활성화
    trading_strategy: TradingStrategy,
    watchlist: Vec<String>,
    dry_run: bool,
}

/// 매매 전략
#[derive(Debug, Clone)]
pub struct TradingStrategy {
    pub profit_target: f64,      // 목표 수익률 (%)
    pub stop_loss: f64,          // 손절매 (%)
    pub max_position_ratio: f64, // 최대 포지션 비율 (%)
    pub min_cash_ratio: f64,     // 최소 현금 보유 비율 (%)
}

impl Default for TradingStrategy {
    fn default() -> Self {
        Self {
            profit_target: 3.0,      // 3% 수익 시 매도
            stop_loss: -2.0,         // -2% 손실 시 손절
            max_position_ratio: 50.0, // 최대 50% 포지션
            min_cash_ratio: 20.0,    // 최소 20% 현금 보유
        }
    }
}

impl LiveTradingSystem {
    /// 새로운 실전 투자 시스템 초기화
    pub async fn new(strategy: TradingStrategy, dry_run: bool) -> Result<Self> {
        if dry_run {
        } else {
            warn!("⚠️  모드: 실전 거래 (실제 주문 실행!)");
        }
        // Actor 시스템 생성
        let actor_system = ActorSystem::new("live-ml4t-system");
        // Actor들 생성
        Self::create_actors(&actor_system).await?;
        // 관심 종목 설정 (우량주 20개)
        let watchlist = vec![
            // 대형주 (시총 상위)
            "005930".to_string(),  // 삼성전자
            "000660".to_string(),  // SK하이닉스
            "373220".to_string(),  // LG에너지솔루션
            "207940".to_string(),  // 삼성바이오로직스

            // IT/인터넷
            "035720".to_string(),  // 카카오
            "035420".to_string(),  // NAVER
            "051910".to_string(),  // LG화학
            "006400".to_string(),  // 삼성SDI

            // 금융
            "055550".to_string(),  // 신한지주
            "105560".to_string(),  // KB금융
            "086790".to_string(),  // 하나금융지주
            "316140".to_string(),  // 우리금융지주

            // 자동차
            "005380".to_string(),  // 현대차
            "000270".to_string(),  // 기아

            // 산업재
            "012330".to_string(),  // 현대모비스
            "009150".to_string(),  // 삼성전기

            // 소비재
            "097950".to_string(),  // CJ제일제당
            "271560".to_string(),  // 오리온

            // 에너지/유틸리티
            "015760".to_string(),  // 한국전력
            "034730".to_string(),  // SK
        ];
        Ok(Self {
            actor_system,
            trading_strategy: strategy,
            watchlist,
            dry_run,
        })
    }

    /// 모든 Actor 생성
    async fn create_actors(actor_system: &ActorSystem) -> Result<()> {
        use std::collections::HashMap;
        // 1. DataAgent
        let market_data_config = actors::data_agent::MarketDataConfig {
            symbols: vec![],  // 나중에 동적으로 설정
            data_sources: vec!["kis_api".to_string()],
            update_frequency_ms: 30000,  // 30초마다 업데이트
            history_days: 5,
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
            technical_indicators: vec!["SMA".to_string(), "RSI".to_string(), "MACD".to_string()],
            alpha_factors: vec!["momentum".to_string(), "mean_reversion".to_string()],
            fundamental_features: vec![],
            alternative_features: vec![],
        };
        let ml_agent = actors::MLAgent::new(models, features_config);
        actor_system.spawn(ml_agent, "ml-agent").await?;
        // 3. TradingAgent
        let broker_config = actors::trading_agent::BrokerConfig {
            initial_capital: 1000000.0,  // 100만원
            commission_rate: 0.00015,    // 0.015%
        };
        let execution_config = actors::trading_agent::ExecutionConfig {
            max_position_size: 500000.0, // 최대 50만원
            slippage_tolerance: 0.001,
        };
        let trading_agent = actors::TradingAgent::new(broker_config, execution_config);
        actor_system.spawn(trading_agent, "trading-agent").await?;
        // 4. RiskAgent
        let risk_config = actors::risk_agent::RiskConfig {
            max_portfolio_risk: 0.10,    // 최대 10% 리스크
            max_position_size: 500000.0, // 최대 50만원
            stop_loss_pct: 0.02,         // 2% 손절
            take_profit_pct: 0.03,       // 3% 익절
        };
        let portfolio_config = actors::risk_agent::PortfolioConfig {
            initial_value: 1000000.0,
            target_allocation: 0.80,     // 80% 투자
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

    /// 시장 시간 확인 (한국 주식 시장)
    fn is_market_hours() -> bool {
        let now = Local::now();
        let time = now.time();
        let weekday = now.weekday();

        // 평일 09:00 ~ 15:30
        use chrono::Weekday;
        matches!(weekday, Weekday::Mon | Weekday::Tue | Weekday::Wed | Weekday::Thu | Weekday::Fri)
            && time >= chrono::NaiveTime::from_hms_opt(9, 0, 0).unwrap()
            && time <= chrono::NaiveTime::from_hms_opt(15, 30, 0).unwrap()
    }

    /// 실시간 자동매매 실행
    pub async fn run(&mut self) -> Result<()> {
        let mut loop_count = 0;
        let check_interval = if self.dry_run { 15 } else { 30 };  // DRY-RUN: 15초, 실전: 30초

        loop {
            loop_count += 1;
            let now = Local::now();
            // 시장 시간 확인
            if !Self::is_market_hours() {
                sleep(Duration::from_secs(check_interval)).await;
                continue;
            }

            // 1. 데이터 수집 (DataAgent)
            let message = actors::DataMessage::CollectMarketData;
            if let Err(e) = self.actor_system.send_message::<actors::DataAgent>("data-agent", message).await {
                error!("데이터 수집 실패: {}", e);
            }
            sleep(Duration::from_millis(500)).await;

            // 2. ML 예측 (MLAgent)
            let message = actors::MLMessage::PredictReturns {
                symbols: self.watchlist.clone()
            };
            if let Err(e) = self.actor_system.send_message::<actors::MLAgent>("ml-agent", message).await {
                error!("ML 예측 실패: {}", e);
            }
            sleep(Duration::from_millis(500)).await;

            // 3. 거래 신호 생성 (TradingAgent)
            let message = actors::TradingMessage::GenerateSignals;
            if let Err(e) = self.actor_system.send_message::<actors::TradingAgent>("trading-agent", message).await {
                error!("거래 신호 생성 실패: {}", e);
            }
            sleep(Duration::from_millis(500)).await;

            // 4. 리스크 평가 (RiskAgent)
            let message = actors::RiskMessage::AssessPortfolioRisk;
            if let Err(e) = self.actor_system.send_message::<actors::RiskAgent>("risk-agent", message).await {
                error!("리스크 평가 실패: {}", e);
            }
            sleep(Duration::from_millis(500)).await;

            // 5. 거래 실행 (TradingAgent) - 실전 모드에서만
            if !self.dry_run {
                let message = actors::TradingMessage::ExecutePositions;
                if let Err(e) = self.actor_system.send_message::<actors::TradingAgent>("trading-agent", message).await {
                    error!("거래 실행 실패: {}", e);
                }
                sleep(Duration::from_millis(500)).await;
            } else {
            }

            // 6. 성능 모니터링 (MonitoringAgent)
            let message = actors::MonitoringMessage::UpdatePerformance;
            if let Err(e) = self.actor_system.send_message::<actors::MonitoringAgent>("monitoring-agent", message).await {
                error!("성능 모니터링 실패: {}", e);
            }
            // 대기
            sleep(Duration::from_secs(check_interval)).await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // .env 파일 로드
    dotenv::dotenv().ok();

    // 로깅 초기화
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
            .add_directive(tracing::Level::INFO.into()))
        .init();

    println!("\n");
    println!("╔════════════════════════════════════════════════╗");
    println!("║  🚀 ML4T 실전 투자 시스템                    ║");
    println!("║  Machine Learning for Trading - Live System   ║");
    println!("╚════════════════════════════════════════════════╝");
    println!();

    // 명령행 인자로 모드 선택
    let args: Vec<String> = std::env::args().collect();
    let dry_run = if args.len() > 1 && args[1] == "--real" {
        println!("⚠️  ═══════════════════════════════════════");
        println!("⚠️  실전 거래 모드로 실행합니다!");
        println!("⚠️  실제 주문이 체결됩니다!");
        println!("⚠️  실제 돈이 사용됩니다!");
        println!("⚠️  ═══════════════════════════════════════");
        println!();
        println!("💰 한국투자증권 API를 통해 실제 거래를 수행합니다");
        println!("📊 Actor 시스템이 자동으로 매매를 판단합니다");
        println!();
        println!("⏱️  30초마다 자동 매매 체크");
        println!("🛑 중지: Ctrl+C\n");
        false
    } else {
        println!("🔍 DRY-RUN 모드로 실행합니다 (시뮬레이션만)");
        println!("💡 실전 거래를 원하시면: cargo run --example live_trading_system -- --real");
        println!("⏱️  15초마다 체크\n");
        true
    };

    // 매매 전략 설정
    let strategy = TradingStrategy {
        profit_target: 3.0,       // 3% 수익 시 매도
        stop_loss: -2.0,          // -2% 손실 시 손절
        max_position_ratio: 50.0, // 최대 50% 포지션
        min_cash_ratio: 20.0,     // 최소 20% 현금 보유
    };

    // 시스템 생성 및 실행
    let mut trading_system = LiveTradingSystem::new(strategy, dry_run).await?;

    trading_system.run().await?;

    Ok(())
}

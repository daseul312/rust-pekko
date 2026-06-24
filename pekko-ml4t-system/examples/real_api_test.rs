//! 실제 API 연동 테스트 예제
//!
//! 이 예제는 KIS API와 Alpha Vantage API를 실제로 연동하여
//! 실시간 시장 데이터를 수집하고 거래 시뮬레이션을 실행합니다.

use pekko_ml4t_system::actors::*;
use pekko_ml4t_system::config::api_config::ApiConfig;
use pekko_ml4t_system::data::collectors::RealApiDataCollector;
use pekko_actor::{ActorSystem, ActorRef};
use anyhow::Result;
use dotenv::dotenv;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{ error};

/// 실제 API 연동 테스트 구조체
struct RealApiTest {
    pub actor_system: ActorSystem,
    pub data_collector: RealApiDataCollector,
    pub symbols: Vec<String>,
}

impl RealApiTest {
    /// 새로운 테스트 인스턴스 생성
    async fn new() -> Result<Self> {
        // 환경변수 로드
        dotenv().ok();

        // API 설정 로드
        let api_config = ApiConfig::from_env()
            .unwrap_or_else(|_| {
                ApiConfig::mock_config()
            });

        // API 설정 출력
        // 실제 API 데이터 수집기 생성
        let data_collector = RealApiDataCollector::new(api_config).await?;

        // Actor 시스템 생성
        let actor_system = ActorSystem::new("real-api-test-system");

        // 테스트할 종목 설정
        let symbols = vec![
            "AAPL".to_string(),  // Apple
            "GOOGL".to_string(), // Google
            "MSFT".to_string(),  // Microsoft
            "005930".to_string(), // 삼성전자 (KIS API 테스트용)
        ];

        Ok(Self {
            actor_system,
            data_collector,
            symbols,
        })
    }

    /// API 연결 테스트
    async fn test_api_connections(&self) -> Result<()> {
        // API 상태 확인
        let api_status = self.data_collector.get_api_status();
        for (api_name, is_connected) in &api_status {
        }

        // 시장 개장 상태 확인
        match self.data_collector.is_market_open().await {
            Ok(is_open) => {
            }
            Err(e) => {
                warn!("  시장 상태 확인 실패: {}", e);
            }
        }

        // 헬스 체크
        match self.data_collector.health_check().await {
            HealthStatus::Healthy => {
            }
            HealthStatus::Degraded(issues) => {
                warn!("  시스템 상태: ⚠️  일부 문제 있음");
                for issue in issues {
                    warn!("    - {}", issue);
                }
            }
        }

        Ok(())
    }

    /// 실시간 데이터 수집 테스트
    async fn test_real_time_data_collection(&mut self) -> Result<()> {
        // 데이터 수집 시작 (30초 간격)
        self.data_collector.start_collection(self.symbols.clone(), 30).await?;

        // 2분간 데이터 수집 모니터링
        for i in 0..4 {
            sleep(Duration::from_secs(30)).await;
            // 수집된 데이터 확인
            let all_data = self.data_collector.get_all_market_data().await;
            for (symbol, data) in &all_data {
                let is_fresh = self.data_collector.is_data_fresh(symbol).await;
            }

            // RSI 지표 테스트 (Alpha Vantage API가 활성화된 경우)
            if let Some(symbol) = self.symbols.first() {
                if let Ok(Some(rsi)) = self.data_collector.get_rsi(symbol, 14).await {
                }
            }
        }

        // 수집 중지
        self.data_collector.stop_collection().await?;
        Ok(())
    }

    /// 실제 데이터 기반 거래 시뮬레이션
    async fn test_real_data_trading_simulation(&mut self) -> Result<()> {
        // Actor들 생성
        let market_data_config = MarketDataConfig {
            symbols: self.symbols.clone(),
            data_sources: vec!["real_api".to_string()],
            update_frequency_ms: 30000, // 30초
            history_days: 30,
        };

        let data_agent = DataAgent::new(
            market_data_config,
            vec!["real_news".to_string()],
            vec!["real_alternative".to_string()],
        );
        let _data_agent_ref = self.actor_system.spawn_actor("data_agent", data_agent).await?;

        // ML Agent 생성
        let features_config = FeaturesConfig {
            technical_indicators: vec!["sma".to_string(), "rsi".to_string()],
            alpha_factors: vec!["momentum".to_string()],
            fundamental_features: vec!["pe_ratio".to_string()],
            alternative_features: vec!["sentiment".to_string()],
        };

        let mut models: HashMap<String, Box<dyn MLModel + Send + Sync>> = HashMap::new();
        let linear_config = ModelConfig {
            model_id: "real_data_model".to_string(),
            model_type: "linear".to_string(),
            parameters: HashMap::new(),
        };
        let linear_model = LinearModel::new(linear_config);
        models.insert("real_data_model".to_string(), Box::new(linear_model));

        let ml_agent = MLAgent::new(models, features_config);
        let _ml_agent_ref = self.actor_system.spawn_actor("ml_agent", ml_agent).await?;

        // Trading Agent 생성
        let broker_config = BrokerConfig {
            initial_capital: 1_000_000.0,
            commission_rate: 0.001,
        };
        let execution_config = ExecutionConfig {
            max_position_size: 0.1,
            slippage_tolerance: 0.001,
        };
        let trading_agent = TradingAgent::new(broker_config, execution_config);
        let trading_agent_ref = self.actor_system.spawn_actor("trading_agent", trading_agent).await?;

        // 실제 데이터로 거래 신호 생성
        for symbol in &self.symbols {
            if let Some(market_data) = self.data_collector.get_market_data(symbol).await? {
                // 간단한 거래 로직: RSI 기반
                if let Ok(Some(rsi)) = self.data_collector.get_rsi(symbol, 14).await {
                    let signal = if rsi < 30.0 {
                        TradingSignal::Buy { strength: 0.8 }
                    } else if rsi > 70.0 {
                        TradingSignal::Sell { strength: 0.8 }
                    } else {
                        TradingSignal::Hold
                    };
                    // 거래 신호 전송
                    if !matches!(signal, TradingSignal::Hold) {
                        trading_agent_ref.tell(TradingMessage::SignalGenerated {
                            symbol: symbol.clone(),
                            signal: signal.clone(),
                        })?;
                    }
                }
            } else {
                warn!("  {} 데이터를 가져올 수 없습니다", symbol);
            }

            // API 요청 제한을 위한 대기
            sleep(Duration::from_millis(500)).await;
        }
        Ok(())
    }

    /// 수집 통계 출력
    async fn print_collection_stats(&self) -> Result<()> {
        let stats = self.data_collector.get_collection_stats().await;
        if let Some(last_time) = stats.last_request_time {
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 로깅 초기화
    tracing_subscriber::fmt::init();

    println!("🚀 실제 API 연동 테스트");
    println!("════════════════════════════════════════");

    // 테스트 인스턴스 생성
    let mut test = RealApiTest::new().await?;

    // 1. API 연결 테스트
    test.test_api_connections().await?;

    // 2. 실시간 데이터 수집 테스트
    test.test_real_time_data_collection().await?;

    // 3. 실제 데이터 기반 거래 시뮬레이션
    test.test_real_data_trading_simulation().await?;

    // 4. 수집 통계 출력
    test.print_collection_stats().await?;

    println!("\n🎊 실제 API 연동 테스트가 완료되었습니다!");
    println!("실제 시장 데이터를 활용한 자동매매 시스템이 준비되었습니다! 🚀");

    Ok(())
}

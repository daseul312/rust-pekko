//! 실제 거래 시나리오 시뮬레이션 애플리케이션
//!
//! 이 애플리케이션은 실제 거래 환경과 유사한 시나리오를 제공하여
//! ML4T 시스템의 전체 기능을 체험할 수 있도록 합니다.
//!
//! 주요 기능:
//! 1. 실시간 시장 데이터 시뮬레이션
//! 2. 다양한 거래 전략 시나리오
//! 3. 리스크 관리 및 모니터링
//! 4. 성능 분석 및 리포팅
//! 5. 사용자 친화적 콘솔 인터페이스

use pekko_ml4t_system::actors::*;
use pekko_actor::{ActorSystem, ActorRef};
use anyhow::Result;
use std::collections::HashMap;
use std::time::Duration;
use std::io::{self, Write};
use tokio::time::sleep;
use chrono::{Utc, DateTime};

// 시뮬레이션 설정
#[derive(Debug, Clone)]
struct SimulationConfig {
    pub symbols: Vec<String>,
    pub initial_capital: f64,
    pub simulation_duration_minutes: u64,
    pub data_update_interval_ms: u64,
    pub risk_tolerance: f64,
    pub max_positions: usize,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            symbols: vec!["AAPL".to_string(), "GOOGL".to_string(), "MSFT".to_string()],
            initial_capital: 1000000.0,
            simulation_duration_minutes: 1, // 1분 시뮬레이션
            data_update_interval_ms: 1000,   // 1초마다 데이터 업데이트
            risk_tolerance: 0.02,            // 2% 리스크 한도
            max_positions: 5,
        }
    }
}

// 시뮬레이션 상태
#[derive(Debug)]
struct SimulationState {
    pub config: SimulationConfig,
    pub start_time: DateTime<Utc>,
    pub current_time: DateTime<Utc>,
    pub portfolio_value: f64,
    pub positions: HashMap<String, f64>,
    pub trades_count: u32,
    pub total_pnl: f64,
    pub active_signals: Vec<TradingSignal>,
}

impl SimulationState {
    fn new(config: SimulationConfig) -> Self {
        let now = Utc::now();
        Self {
            portfolio_value: config.initial_capital,
            config,
            start_time: now,
            current_time: now,
            positions: HashMap::new(),
            trades_count: 0,
            total_pnl: 0.0,
            active_signals: Vec::new(),
        }
    }

    fn update_time(&mut self) {
        self.current_time = Utc::now();
    }

    fn add_position(&mut self, symbol: String, quantity: f64) {
        *self.positions.entry(symbol).or_insert(0.0) += quantity;
    }

    fn remove_position(&mut self, symbol: String, quantity: f64) {
        if let Some(pos) = self.positions.get_mut(&symbol) {
            *pos -= quantity;
            if *pos <= 0.0 {
                self.positions.remove(&symbol);
            }
        }
    }

    fn record_trade(&mut self, pnl: f64) {
        self.trades_count += 1;
        self.total_pnl += pnl;
        self.portfolio_value += pnl;
    }
}

// 거래 시나리오 정의
#[derive(Debug, Clone)]
enum TradingScenario {
    MomentumStrategy,
    MeanReversionStrategy,
    MLPredictionStrategy,
    RiskManagedStrategy,
    NewsBasedStrategy,
}

impl TradingScenario {
    fn description(&self) -> &'static str {
        match self {
            TradingScenario::MomentumStrategy => "모멘텀 전략: 추세를 따라가는 전략",
            TradingScenario::MeanReversionStrategy => "평균 회귀 전략: 극단적 가격에서 반대 방향 거래",
            TradingScenario::MLPredictionStrategy => "ML 예측 전략: 머신러닝 모델 기반 예측",
            TradingScenario::RiskManagedStrategy => "리스크 관리 전략: 엄격한 리스크 관리 중심",
            TradingScenario::NewsBasedStrategy => "뉴스 기반 전략: 뉴스 감정 분석 기반 거래",
        }
    }
}

// 시뮬레이션 엔진
struct TradingSimulationEngine {
    pub actor_system: ActorSystem,
    pub data_agent: ActorRef<DataAgent>,
    pub ml_agent: ActorRef<MLAgent>,
    pub trading_agent: ActorRef<TradingAgent>,
    pub risk_agent: ActorRef<RiskAgent>,
    pub monitoring_agent: ActorRef<MonitoringAgent>,
    pub state: SimulationState,
}

impl TradingSimulationEngine {
    async fn new(config: SimulationConfig) -> Result<Self> {
        println!("🚀 거래 시뮬레이션 엔진 초기화 중...");
        
        let actor_system = ActorSystem::new("trading-simulation-system");
        
        // Actor 생성 및 등록
        let (data_agent, ml_agent, trading_agent, risk_agent, monitoring_agent) = 
            Self::create_actors(&actor_system, &config).await?;
        
        let state = SimulationState::new(config);
        
        Ok(Self {
            actor_system,
            data_agent,
            ml_agent,
            trading_agent,
            risk_agent,
            monitoring_agent,
            state,
        })
    }

    async fn create_actors(
        actor_system: &ActorSystem,
        config: &SimulationConfig,
    ) -> Result<(
        ActorRef<DataAgent>,
        ActorRef<MLAgent>,
        ActorRef<TradingAgent>,
        ActorRef<RiskAgent>,
        ActorRef<MonitoringAgent>,
    )> {
        // DataAgent 생성
        let market_data_config = MarketDataConfig {
            symbols: config.symbols.clone(),
            data_sources: vec!["simulation".to_string()],
            update_frequency_ms: config.data_update_interval_ms,
            history_days: 30,
        };
        
        let data_agent = DataAgent::new(
            market_data_config,
            vec!["simulation_news".to_string()],
            vec!["simulation_alternative".to_string()],
        );
        let data_agent_ref = actor_system.spawn_actor("data_agent", data_agent).await?;
        
        // MLAgent 생성
        let features_config = FeaturesConfig {
            technical_indicators: vec!["sma".to_string(), "ema".to_string(), "rsi".to_string()],
            alpha_factors: vec!["momentum".to_string(), "mean_reversion".to_string()],
            fundamental_features: vec!["pe_ratio".to_string(), "pb_ratio".to_string()],
            alternative_features: vec!["sentiment".to_string(), "news_volume".to_string()],
        };
        
        let mut models: HashMap<String, Box<dyn MLModel + Send + Sync>> = HashMap::new();
        
        let linear_config = ModelConfig {
            model_id: "momentum_model".to_string(),
            model_type: "linear".to_string(),
            parameters: HashMap::new(),
        };
        let linear_model = LinearModel::new(linear_config);
        models.insert("momentum_model".to_string(), Box::new(linear_model));
        
        let rf_config = ModelConfig {
            model_id: "reversion_model".to_string(),
            model_type: "random_forest".to_string(),
            parameters: HashMap::new(),
        };
        let rf_model = RandomForestModel::new(rf_config);
        models.insert("reversion_model".to_string(), Box::new(rf_model));
        
        let ml_agent = MLAgent::new(models, features_config);
        let ml_agent_ref = actor_system.spawn_actor("ml_agent", ml_agent).await?;
        
        // TradingAgent 생성
        let broker_config = BrokerConfig {
            initial_capital: config.initial_capital,
            commission_rate: 0.001,
        };
        
        let execution_config = ExecutionConfig {
            max_position_size: 0.1,
            slippage_tolerance: 0.001,
        };
        
        let trading_agent = TradingAgent::new(broker_config, execution_config);
        let trading_agent_ref = actor_system.spawn_actor("trading_agent", trading_agent).await?;
        
        // RiskAgent 생성
        let risk_config = RiskConfig {
            max_portfolio_risk: config.risk_tolerance,
            max_position_size: 0.1,
            stop_loss_pct: 0.02,
            take_profit_pct: 0.04,
        };
        
        let portfolio_config = PortfolioConfig {
            initial_value: config.initial_capital,
            target_allocation: 0.8,
        };
        
        let risk_agent = RiskAgent::new(risk_config, portfolio_config);
        let risk_agent_ref = actor_system.spawn_actor("risk_agent", risk_agent).await?;
        
        // MonitoringAgent 생성
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
        
        Ok((data_agent_ref, ml_agent_ref, trading_agent_ref, risk_agent_ref, monitoring_agent_ref))
    }

    async fn run_simulation(&mut self, scenario: TradingScenario) -> Result<()> {
        println!("\n🎯 시뮬레이션 시작: {}", scenario.description());
        println!("📊 초기 자본: ${:.2}", self.state.config.initial_capital);
        println!("⏱️  시뮬레이션 시간: {}분", self.state.config.simulation_duration_minutes);
        println!("📈 대상 종목: {:?}", self.state.config.symbols);
        println!("⚠️  리스크 한도: {:.1}%", self.state.config.risk_tolerance * 100.0);
        
        let start_time = std::time::Instant::now();
        let duration = Duration::from_secs(self.state.config.simulation_duration_minutes * 60);
        
        // 시뮬레이션 루프
        let mut iteration = 0;
        while start_time.elapsed() < duration {
            iteration += 1;
            self.state.update_time();
            
            // 진행률 표시
            let elapsed = start_time.elapsed();
            let progress = (elapsed.as_secs() as f64 / duration.as_secs() as f64) * 100.0;
            
            if iteration % 10 == 0 {
                self.print_status(progress);
            }
            
            // 시나리오별 실행
            match scenario {
                TradingScenario::MomentumStrategy => {
                    self.run_momentum_scenario().await?;
                },
                TradingScenario::MeanReversionStrategy => {
                    self.run_mean_reversion_scenario().await?;
                },
                TradingScenario::MLPredictionStrategy => {
                    self.run_ml_prediction_scenario().await?;
                },
                TradingScenario::RiskManagedStrategy => {
                    self.run_risk_managed_scenario().await?;
                },
                TradingScenario::NewsBasedStrategy => {
                    self.run_news_based_scenario().await?;
                },
            }
            
            // 데이터 업데이트 간격 대기
            sleep(Duration::from_millis(self.state.config.data_update_interval_ms)).await;
        }
        
        // 최종 결과 출력
        self.print_final_results().await?;
        
        Ok(())
    }

    async fn run_momentum_scenario(&mut self) -> Result<()> {
        // 모멘텀 전략: 추세를 따라가는 거래
        let symbols = self.state.config.symbols.clone();
        for symbol in &symbols {
            // 시장 데이터 수집 요청
            self.data_agent.tell(DataMessage::CollectMarketData)?;
            
            // ML 예측 요청
            self.ml_agent.tell(MLMessage::PredictReturns { 
                symbols: vec![symbol.clone()] 
            })?;
            
            // 리스크 확인
            self.risk_agent.tell(RiskMessage::CheckPositionLimits)?;
            
            // 모멘텀 신호 생성 (시뮬레이션)
            if self.should_generate_signal() {
                let signal = if self.is_positive_momentum(symbol) {
                    TradingSignal::Buy { strength: 0.8 }
                } else {
                    TradingSignal::Sell { strength: 0.7 }
                };
                
                self.trading_agent.tell(TradingMessage::SignalGenerated {
                    symbol: symbol.clone(),
                    signal: signal.clone(),
                })?;
                
                // 거래 실행 시뮬레이션
                self.simulate_trade_execution(symbol, &signal).await?;
            }
        }
        
        Ok(())
    }

    async fn run_mean_reversion_scenario(&mut self) -> Result<()> {
        // 평균 회귀 전략: 극단적 가격에서 반대 방향 거래
        let symbols = self.state.config.symbols.clone();
        for symbol in &symbols {
            self.data_agent.tell(DataMessage::CollectMarketData)?;
            self.risk_agent.tell(RiskMessage::CalculateVaR)?;
            
            if self.should_generate_signal() && self.is_extreme_price(symbol) {
                let signal = if self.is_overbought(symbol) {
                    TradingSignal::Sell { strength: 0.9 }
                } else if self.is_oversold(symbol) {
                    TradingSignal::Buy { strength: 0.9 }
                } else {
                    TradingSignal::Hold
                };
                
                if !matches!(signal, TradingSignal::Hold) {
                    self.trading_agent.tell(TradingMessage::SignalGenerated {
                        symbol: symbol.clone(),
                        signal: signal.clone(),
                    })?;
                    
                    self.simulate_trade_execution(symbol, &signal).await?;
                }
            }
        }
        
        Ok(())
    }

    async fn run_ml_prediction_scenario(&mut self) -> Result<()> {
        // ML 예측 전략: 머신러닝 모델 기반 예측
        let symbols = self.state.config.symbols.clone();
        for symbol in &symbols {
            self.data_agent.tell(DataMessage::CollectMarketData)?;
            self.ml_agent.tell(MLMessage::TrainModel { 
                model_type: "ensemble".to_string(), 
                features: vec!["technical".to_string(), "fundamental".to_string()] 
            })?;
            
            self.ml_agent.tell(MLMessage::PredictReturns { 
                symbols: vec![symbol.clone()] 
            })?;
            
            if self.should_generate_signal() {
                let prediction = self.get_ml_prediction(symbol);
                let signal = if prediction > 0.02 {
                    TradingSignal::Buy { strength: prediction.min(1.0) }
                } else if prediction < -0.02 {
                    TradingSignal::Sell { strength: (-prediction).min(1.0) }
                } else {
                    TradingSignal::Hold
                };
                
                if !matches!(signal, TradingSignal::Hold) {
                    self.trading_agent.tell(TradingMessage::SignalGenerated {
                        symbol: symbol.clone(),
                        signal: signal.clone(),
                    })?;
                    
                    self.simulate_trade_execution(symbol, &signal).await?;
                }
            }
        }
        
        Ok(())
    }

    async fn run_risk_managed_scenario(&mut self) -> Result<()> {
        // 리스크 관리 전략: 엄격한 리스크 관리 중심
        self.risk_agent.tell(RiskMessage::AssessPortfolioRisk)?;
        self.risk_agent.tell(RiskMessage::CheckPositionLimits)?;
        
        let symbols = self.state.config.symbols.clone();
        for symbol in &symbols {
            self.data_agent.tell(DataMessage::CollectMarketData)?;
            
            // 포지션 크기 확인
            if self.is_position_within_limits(symbol) {
                if self.should_generate_signal() {
                    let signal = self.get_conservative_signal(symbol);
                    
                    if !matches!(signal, TradingSignal::Hold) {
                        self.trading_agent.tell(TradingMessage::SignalGenerated {
                            symbol: symbol.clone(),
                            signal: signal.clone(),
                        })?;
                        
                        self.simulate_trade_execution(symbol, &signal).await?;
                    }
                }
            }
        }
        
        Ok(())
    }

    async fn run_news_based_scenario(&mut self) -> Result<()> {
        // 뉴스 기반 전략: 뉴스 감정 분석 기반 거래
        self.data_agent.tell(DataMessage::ProcessNewsData)?;
        self.data_agent.tell(DataMessage::UpdateAlternativeData)?;
        
        let symbols = self.state.config.symbols.clone();
        for symbol in &symbols {
            self.ml_agent.tell(MLMessage::PredictReturns { 
                symbols: vec![symbol.clone()] 
            })?;
            
            if self.should_generate_signal() {
                let sentiment = self.get_news_sentiment(symbol);
                let signal = if sentiment > 0.3 {
                    TradingSignal::Buy { strength: sentiment.min(0.8) }
                } else if sentiment < -0.3 {
                    TradingSignal::Sell { strength: (-sentiment).min(0.8) }
                } else {
                    TradingSignal::Hold
                };
                
                if !matches!(signal, TradingSignal::Hold) {
                    self.trading_agent.tell(TradingMessage::SignalGenerated {
                        symbol: symbol.clone(),
                        signal: signal.clone(),
                    })?;
                    
                    self.simulate_trade_execution(symbol, &signal).await?;
                }
            }
        }
        
        Ok(())
    }

    // 시뮬레이션 헬퍼 메서드들
    fn should_generate_signal(&self) -> bool {
        // 20% 확률로 신호 생성
        rand::random::<f64>() < 0.2
    }

    fn is_positive_momentum(&self, _symbol: &str) -> bool {
        // 시뮬레이션: 60% 확률로 양의 모멘텀
        rand::random::<f64>() < 0.6
    }

    fn is_extreme_price(&self, _symbol: &str) -> bool {
        // 시뮬레이션: 15% 확률로 극단적 가격
        rand::random::<f64>() < 0.15
    }

    fn is_overbought(&self, _symbol: &str) -> bool {
        rand::random::<f64>() < 0.5
    }

    fn is_oversold(&self, _symbol: &str) -> bool {
        rand::random::<f64>() < 0.5
    }

    fn get_ml_prediction(&self, _symbol: &str) -> f64 {
        // 시뮬레이션: -0.1 ~ 0.1 범위의 예측값
        (rand::random::<f64>() - 0.5) * 0.2
    }

    fn is_position_within_limits(&self, symbol: &str) -> bool {
        let current_position = self.state.positions.get(symbol).unwrap_or(&0.0);
        *current_position < self.state.config.max_positions as f64 * 0.2
    }

    fn get_conservative_signal(&self, _symbol: &str) -> TradingSignal {
        // 보수적 신호: 낮은 강도
        if rand::random::<f64>() < 0.3 {
            TradingSignal::Buy { strength: 0.3 }
        } else {
            TradingSignal::Hold
        }
    }

    fn get_news_sentiment(&self, _symbol: &str) -> f64 {
        // 시뮬레이션: -1.0 ~ 1.0 범위의 감정 점수
        (rand::random::<f64>() - 0.5) * 2.0
    }

    async fn simulate_trade_execution(&mut self, symbol: &str, signal: &TradingSignal) -> Result<()> {
        let quantity = 100.0; // 고정 수량
        let price = self.get_simulated_price(symbol);
        
        match signal {
            TradingSignal::Buy { strength } => {
                self.trading_agent.tell(TradingMessage::ExecuteBuy {
                    symbol: symbol.to_string(),
                    quantity: quantity as u32,
                    price,
                })?;
                
                self.state.add_position(symbol.to_string(), quantity);
                
                // 수익/손실 시뮬레이션
                let pnl = self.simulate_pnl(symbol, quantity, price, true);
                self.state.record_trade(pnl);
                
                println!("  📈 매수: {} {}주 @ ${:.2} (강도: {:.2})", symbol, quantity, price, strength);
            },
            TradingSignal::Sell { strength } => {
                if self.state.positions.get(symbol).unwrap_or(&0.0) > &0.0 {
                    self.trading_agent.tell(TradingMessage::ExecuteSell {
                        symbol: symbol.to_string(),
                        quantity: quantity as u32,
                        price,
                    })?;
                    
                    self.state.remove_position(symbol.to_string(), quantity);
                    
                    let pnl = self.simulate_pnl(symbol, quantity, price, false);
                    self.state.record_trade(pnl);
                    
                    println!("  📉 매도: {} {}주 @ ${:.2} (강도: {:.2})", symbol, quantity, price, strength);
                }
            },
            TradingSignal::Hold => {
                // 홀드: 아무것도 하지 않음
            }
        }
        
        Ok(())
    }

    fn get_simulated_price(&self, _symbol: &str) -> f64 {
        // 시뮬레이션 가격: 100-300 범위
        100.0 + rand::random::<f64>() * 200.0
    }

    fn simulate_pnl(&self, _symbol: &str, _quantity: f64, _price: f64, _is_buy: bool) -> f64 {
        // 시뮬레이션 PnL: -50 ~ 50 범위
        (rand::random::<f64>() - 0.5) * 100.0
    }

    fn print_status(&self, progress: f64) {
        print!("\r📊 진행률: {:.1}% | 💰 포트폴리오: ${:.2} | 📈 거래수: {} | 💵 총 PnL: ${:.2}", 
               progress, self.state.portfolio_value, self.state.trades_count, self.state.total_pnl);
        io::stdout().flush().unwrap();
    }

    async fn print_final_results(&mut self) -> Result<()> {
        println!("\n\n🎉 시뮬레이션 완료!");
        println!("════════════════════════════════════════");
        
        let total_return = (self.state.portfolio_value - self.state.config.initial_capital) 
                          / self.state.config.initial_capital * 100.0;
        
        println!("📊 최종 결과:");
        println!("  💰 초기 자본: ${:.2}", self.state.config.initial_capital);
        println!("  💰 최종 가치: ${:.2}", self.state.portfolio_value);
        println!("  📈 총 수익률: {:.2}%", total_return);
        println!("  💵 총 PnL: ${:.2}", self.state.total_pnl);
        println!("  🔄 총 거래수: {}", self.state.trades_count);
        println!("  📦 현재 포지션: {:?}", self.state.positions);
        
        // 성능 평가
        if total_return > 5.0 {
            println!("  🏆 성과: 우수 (5% 이상 수익)");
        } else if total_return > 0.0 {
            println!("  ✅ 성과: 양호 (양의 수익)");
        } else if total_return > -5.0 {
            println!("  ⚠️  성과: 보통 (소폭 손실)");
        } else {
            println!("  ❌ 성과: 개선 필요 (큰 손실)");
        }
        
        println!("════════════════════════════════════════");
        
        Ok(())
    }
}

// 메인 함수
#[tokio::main]
async fn main() -> Result<()> {
    // 로깅 초기화
    tracing_subscriber::fmt::init();
    
    println!("🚀 ML4T 거래 시뮬레이션 애플리케이션");
    println!("════════════════════════════════════════");
    
    // 시뮬레이션 설정
    let config = SimulationConfig::default();
    
    // 사용자에게 시나리오 선택 요청
    println!("\n📋 사용 가능한 거래 시나리오:");
    println!("1. 모멘텀 전략");
    println!("2. 평균 회귀 전략");
    println!("3. ML 예측 전략");
    println!("4. 리스크 관리 전략");
    println!("5. 뉴스 기반 전략");
    println!("6. 모든 시나리오 순차 실행");
    
    // 자동으로 모든 시나리오 실행
    println!("\n🤖 자동 모드: 모든 시나리오를 순차적으로 실행합니다...");
    let choice = 6; // 모든 시나리오
    
    let scenarios = match choice {
        1 => vec![TradingScenario::MomentumStrategy],
        2 => vec![TradingScenario::MeanReversionStrategy],
        3 => vec![TradingScenario::MLPredictionStrategy],
        4 => vec![TradingScenario::RiskManagedStrategy],
        5 => vec![TradingScenario::NewsBasedStrategy],
        6 => vec![
            TradingScenario::MomentumStrategy,
            TradingScenario::MeanReversionStrategy,
            TradingScenario::MLPredictionStrategy,
            TradingScenario::RiskManagedStrategy,
            TradingScenario::NewsBasedStrategy,
        ],
        _ => vec![TradingScenario::MomentumStrategy],
    };
    
        // 시나리오 실행
        for (i, scenario) in scenarios.iter().enumerate() {
            if scenarios.len() > 1 {
                println!("\n🔄 시나리오 {}/{} 실행 중...", i + 1, scenarios.len());
            }
            
            let engine_config = config.clone();
            let mut engine = TradingSimulationEngine::new(engine_config).await?;
            engine.run_simulation(scenario.clone()).await?;
            
            if i < scenarios.len() - 1 {
                println!("\n⏳ 다음 시나리오로 이동합니다...");
                sleep(Duration::from_secs(2)).await;
            }
        }
    
    println!("\n🎊 모든 시뮬레이션이 완료되었습니다!");
    println!("ML4T 시스템의 강력한 기능을 체험해보세요! 🚀");
    
    Ok(())
}

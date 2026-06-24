//! 간단한 거래 시뮬레이션 예제
//!
//! 이 예제는 실제 거래 전략을 시뮬레이션하여 실 투자 전에
//! 전략의 성과를 미리 테스트할 수 있습니다.

use pekko_ml4t_system::actors::*;
use pekko_actor::{ActorSystem, ActorRef};
use anyhow::Result;
use std::collections::HashMap;
use std::time::Duration;
use std::io::Write;
use tokio::time::sleep;
use rand::Rng;

#[derive(Debug, Clone)]
struct SimulationConfig {
    pub symbols: Vec<String>,
    pub initial_capital: f64,
    pub simulation_duration_seconds: u64,
    pub risk_tolerance_pct: f64,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            symbols: vec!["AAPL".to_string(), "GOOGL".to_string(), "MSFT".to_string()],
            initial_capital: 1_000_000.0,
            simulation_duration_seconds: 30, // 30초 시뮬레이션
            risk_tolerance_pct: 0.02, // 2% 리스크 허용
        }
    }
}

#[derive(Debug)]
struct SimulationState {
    pub config: SimulationConfig,
    pub portfolio_value: f64,
    pub positions: HashMap<String, f64>, // symbol -> quantity
    pub trades_count: u32,
    pub total_pnl: f64,
}

impl SimulationState {
    fn new(config: SimulationConfig) -> Self {
        Self {
            portfolio_value: config.initial_capital,
            config,
            positions: HashMap::new(),
            trades_count: 0,
            total_pnl: 0.0,
        }
    }

    fn add_position(&mut self, symbol: String, quantity: f64) {
        *self.positions.entry(symbol).or_insert(0.0) += quantity;
    }

    fn remove_position(&mut self, symbol: String, quantity: f64) {
        let current_quantity = self.positions.entry(symbol.clone()).or_insert(0.0);
        *current_quantity -= quantity;
        if *current_quantity <= 0.0 {
            self.positions.remove(&symbol);
        }
    }

    fn record_trade(&mut self, pnl: f64) {
        self.trades_count += 1;
        self.total_pnl += pnl;
        self.portfolio_value += pnl;
    }
}

struct SimpleSimulationEngine {
    pub state: SimulationState,
}

impl SimpleSimulationEngine {
    fn new(config: SimulationConfig) -> Self {
        let state = SimulationState::new(config);
        Self { state }
    }

    async fn run_simulation(&mut self) -> Result<()> {
        println!("🎯 시뮬레이션 시작!");
        println!("📊 초기 자본: ${:.2}", self.state.config.initial_capital);
        println!("⏱️  시뮬레이션 시간: {}초", self.state.config.simulation_duration_seconds);
        println!("📈 대상 종목: {:?}", self.state.config.symbols);
        println!("⚠️  리스크 한도: {:.1}%", self.state.config.risk_tolerance_pct * 100.0);
        println!();

        let start_time = std::time::Instant::now();
        let end_time = start_time + Duration::from_secs(self.state.config.simulation_duration_seconds);

        while std::time::Instant::now() < end_time {
            // 시장 데이터 시뮬레이션
            self.simulate_market_data().await?;
            
            // 거래 신호 생성 및 실행
            self.execute_trading_signals().await?;

            // 진행 상황 표시
            self.display_progress(start_time, end_time);

            sleep(Duration::from_millis(1000)).await; // 1초마다 업데이트
        }

        self.print_final_results().await?;
        Ok(())
    }

    async fn simulate_market_data(&self) -> Result<()> {
        // 시장 데이터 수집 시뮬레이션
        for symbol in &self.state.config.symbols {
            let price = self.get_simulated_price(symbol);
            println!("📊 {} 현재가: ${:.2}", symbol, price);
        }
        Ok(())
    }

    async fn execute_trading_signals(&mut self) -> Result<()> {
        let symbols = self.state.config.symbols.clone();
        
        for symbol in &symbols {
            if self.should_generate_signal() {
                let signal_type = self.generate_signal_type();
                let quantity = 100.0;
                let price = self.get_simulated_price(symbol);

                match signal_type {
                    TradingSignal::Buy { strength } => {
                        self.state.add_position(symbol.clone(), quantity);
                        
                        // 수익/손실 시뮬레이션
                        let pnl = self.simulate_pnl(symbol, quantity, price, true);
                        self.state.record_trade(pnl);

                        println!("  📈 매수: {} {}주 @ ${:.2} (강도: {:.2})", symbol, quantity, price, strength);
                    },
                    TradingSignal::Sell { strength } => {
                        if self.state.positions.get(symbol).unwrap_or(&0.0) > &0.0 {
                            self.state.remove_position(symbol.clone(), quantity);
                            
                            let pnl = self.simulate_pnl(symbol, quantity, price, false);
                            self.state.record_trade(pnl);

                            println!("  📉 매도: {} {}주 @ ${:.2} (강도: {:.2})", symbol, quantity, price, strength);
                        } else {
                            println!("  ℹ️ {} 매도 신호 발생했지만 포지션 없음", symbol);
                        }
                    },
                    TradingSignal::Hold => {
                        println!("  ⏸️ {} 홀드 신호 - 거래 없음", symbol);
                    },
                }
            }
        }
        Ok(())
    }

    fn should_generate_signal(&self) -> bool {
        rand::thread_rng().gen_bool(0.3) // 30% 확률로 신호 생성
    }

    fn generate_signal_type(&self) -> TradingSignal {
        let mut rng = rand::thread_rng();
        let choice = rng.gen_range(0.0..1.0);
        
        if choice < 0.4 { // 40% 확률로 매수
            TradingSignal::Buy { strength: rng.gen_range(0.5..1.0) }
        } else if choice < 0.8 { // 40% 확률로 매도
            TradingSignal::Sell { strength: rng.gen_range(0.5..1.0) }
        } else { // 20% 확률로 홀드
            TradingSignal::Hold
        }
    }

    fn get_simulated_price(&self, symbol: &str) -> f64 {
        let mut rng = rand::thread_rng();
        let base_price = match symbol {
            "AAPL" => 170.0,
            "GOOGL" => 1500.0,
            "MSFT" => 400.0,
            _ => 100.0,
        };
        base_price * (1.0 + rng.gen_range(-0.01..0.01)) // ±1% 변동
    }

    fn simulate_pnl(&self, _symbol: &str, quantity: f64, entry_price: f64, is_buy: bool) -> f64 {
        let mut rng = rand::thread_rng();
        let price_change_pct = rng.gen_range(-0.005..0.005); // ±0.5% 가격 변동
        let exit_price = entry_price * (1.0 + price_change_pct);
        let pnl_per_share = if is_buy {
            exit_price - entry_price
        } else {
            entry_price - exit_price
        };
        quantity * pnl_per_share
    }

    fn display_progress(&self, start_time: std::time::Instant, end_time: std::time::Instant) {
        let total_duration = end_time.duration_since(start_time).as_secs() as f64;
        let elapsed_duration = start_time.elapsed().as_secs() as f64;
        let progress = (elapsed_duration / total_duration).min(1.0);

        let bar_length = 50;
        let filled_length = (bar_length as f64 * progress) as usize;
        let bar = "█".repeat(filled_length) + &" ".repeat(bar_length - filled_length);

        print!("\r[{}] {:.1}% | 포트폴리오: ${:.2} | PnL: ${:.2} | 거래: {}",
               bar, progress * 100.0, self.state.portfolio_value, self.state.total_pnl, self.state.trades_count);
        std::io::stdout().flush().unwrap();
    }

    async fn print_final_results(&self) -> Result<()> {
        println!("\n\n--- 시뮬레이션 최종 결과 ---");
        println!("최종 포트폴리오 가치: ${:.2}", self.state.portfolio_value);
        println!("총 실현 PnL: ${:.2}", self.state.total_pnl);
        println!("총 거래 수: {}", self.state.trades_count);
        println!("현재 포지션: {:?}", self.state.positions);

        let total_return = (self.state.portfolio_value - self.state.config.initial_capital) / self.state.config.initial_capital;
        println!("총 수익률: {:.2}%", total_return * 100.0);

        if total_return > 0.05 {
            println!("성과 평가: 🌟 우수! 훌륭한 수익률입니다.");
        } else if total_return > 0.0 {
            println!("성과 평가: 👍 양호! 긍정적인 수익률입니다.");
        } else if total_return > -0.02 {
            println!("성과 평가: 😐 보통. 약간의 손실 또는 미미한 수익입니다.");
        } else {
            println!("성과 평가: 👎 개선 필요. 상당한 손실이 발생했습니다.");
        }

        println!("\n🎯 시뮬레이션 완료!");
        println!("이 결과를 바탕으로 실제 투자 전략을 검토해보세요.");

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 ML4T 간단한 거래 시뮬레이션");
    println!("════════════════════════════════════════");
    println!();
    println!("이 시뮬레이션은 실제 거래 전략을 테스트하여");
    println!("실 투자 전에 전략의 성과를 미리 확인할 수 있습니다.");
    println!();

    let config = SimulationConfig::default();
    let mut engine = SimpleSimulationEngine::new(config);
    
    engine.run_simulation().await?;

    println!("\n🎊 시뮬레이션이 완료되었습니다!");
    println!("ML4T 시스템의 강력한 기능을 체험해보세요! 🚀");

    Ok(())
}

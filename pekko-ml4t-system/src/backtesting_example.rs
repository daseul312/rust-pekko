//! 백테스팅 엔진 예제
//! 
//! 다양한 전략을 사용한 백테스팅 예제를 실행합니다.

use crate::backtesting::{
    BacktestConfig, RebalanceFrequency, TransactionCosts, SlippageModel
};
use crate::backtesting::engine::{BacktestEngine, TradingStrategy as BacktestTradingStrategy};
use crate::backtesting::strategy::{BuyHoldStrategy, MeanReversionStrategy, MomentumStrategy, MLStrategy, AlphaFactorStrategy};
use crate::backtesting::walk_forward::{WalkForwardAnalyzer, WalkForwardConfig, OptimizationMetric};
use crate::backtesting::monte_carlo::BootstrapMethod;
use crate::backtesting::monte_carlo::{MonteCarloAnalyzer, MonteCarloConfig};
use crate::backtesting::performance::{PerformanceAnalyzer};
use crate::backtesting::transaction_costs::{CostModelFactory};
use crate::actors::MarketData;
use crate::api::ApiClient;
use anyhow::Result;
use chrono::{DateTime, Utc, Duration};
use std::collections::HashMap;
use tracing::{ error, warn, debug};

/// 백테스팅 예제 실행
pub async fn run_backtesting_examples() -> Result<()> {
    // 1. 기본 백테스팅 예제
    if let Err(e) = basic_backtest_example().await {
        error!("기본 백테스팅 예제 실패: {}", e);
    }

    // 2. 전략 비교 예제
    if let Err(e) = strategy_comparison_example().await {
        error!("전략 비교 예제 실패: {}", e);
    }

    // 3. Walk-Forward 분석 예제
    if let Err(e) = walk_forward_example().await {
        error!("Walk-Forward 분석 예제 실패: {}", e);
    }

    // 4. Monte Carlo 시뮬레이션 예제
    if let Err(e) = monte_carlo_example().await {
        error!("Monte Carlo 시뮬레이션 예제 실패: {}", e);
    }

    // 5. 거래 비용 분석 예제
    if let Err(e) = transaction_cost_example().await {
        error!("거래 비용 분석 예제 실패: {}", e);
    }
    Ok(())
}

/// 기본 백테스팅 예제
async fn basic_backtest_example() -> Result<()> {
    // 백테스팅 설정
    let config = BacktestConfig {
        start_date: Utc::now() - Duration::days(365),
        end_date: Utc::now(),
        initial_capital: 100_000_000.0, // 1억원
        symbols: vec!["000270".to_string(), "005930".to_string(), "000660".to_string()],
        rebalance_frequency: RebalanceFrequency::Daily,
        transaction_costs: TransactionCosts {
            commission_per_trade: 0.0,
            commission_rate: 0.0015,
            minimum_commission: 15.0,
            maximum_commission: 1000.0,
        },
        slippage: SlippageModel::Fixed(5.0),
        benchmark: Some("KOSPI200".to_string()),
        risk_free_rate: 0.025,
        max_position_size: 0.3,
        max_portfolio_risk: 0.15,
    };

    // 시장 데이터 로드 (시뮬레이션)
    let mut engine = BacktestEngine::new(config.clone());
    
    // 시뮬레이션 데이터 생성
    let market_data = generate_simulated_data(&config.symbols, config.start_date, config.end_date).await?;
    
    for (symbol, data) in market_data {
        engine.load_market_data(&symbol, data);
    }

    // Buy & Hold 전략 실행
    let mut weights = HashMap::new();
    weights.insert("000270".to_string(), 0.4); // 기아 40%
    weights.insert("005930".to_string(), 0.4); // 삼성전자 40%
    weights.insert("000660".to_string(), 0.2); // SK하이닉스 20%

    let strategy = BuyHoldStrategy {
        name: "균형형 Buy & Hold".to_string(),
        weights,
        rebalance_threshold: 0.05,
    };

    let result = engine.run_backtest(&strategy).await?;

    // 결과 출력
    // 성능 리포트 생성
    let analyzer = PerformanceAnalyzer::new(result);
    let report = analyzer.generate_report()?;
    let markdown_report = analyzer.generate_markdown_report()?;
    debug!("마크다운 리포트:\n{}", markdown_report);

    Ok(())
}

/// 전략 비교 예제
#[allow(dead_code)]
async fn strategy_comparison_example() -> Result<()> {
    // 간단한 예제로 변경하여 순환 참조 문제 해결
    Ok(())
}

/// Walk-Forward 분석 예제
async fn walk_forward_example() -> Result<()> {
    let walk_forward_config = WalkForwardConfig {
        initial_training_period: 252, // 1년
        retraining_frequency: 63,     // 3개월마다 재훈련
        validation_period: 63,        // 3개월 검증
        min_training_samples: 100,
        strategy_optimization: true,
        optimization_metric: OptimizationMetric::SharpeRatio,
    };

    let base_config = BacktestConfig {
        start_date: Utc::now() - Duration::days(1095), // 3년
        end_date: Utc::now(),
        initial_capital: 50_000_000.0,
        symbols: vec!["000270".to_string()],
        rebalance_frequency: RebalanceFrequency::Weekly,
        transaction_costs: TransactionCosts {
            commission_per_trade: 0.0,
            commission_rate: 0.0015,
            minimum_commission: 15.0,
            maximum_commission: 1000.0,
        },
        slippage: SlippageModel::Fixed(5.0),
        benchmark: None,
        risk_free_rate: 0.025,
        max_position_size: 0.5,
        max_portfolio_risk: 0.2,
    };

    // Walk-Forward 분석기 생성
    let strategy_builder = Box::new(|params: HashMap<String, f64>| {
        MeanReversionStrategy::new(
            "Optimized Mean Reversion".to_string(),
            *params.get("lookback_period").unwrap_or(&20.0) as u32,
            *params.get("entry_threshold").unwrap_or(&2.0),
            *params.get("exit_threshold").unwrap_or(&0.5),
            *params.get("max_position_size").unwrap_or(&0.3),
        )
    });

    let mut analyzer = WalkForwardAnalyzer::new(walk_forward_config, strategy_builder);

    // 데이터 로드
    let market_data = generate_simulated_data(&base_config.symbols, base_config.start_date, base_config.end_date).await?;
    for (symbol, data) in market_data {
        analyzer.load_market_data(&symbol, data);
    }

    // Walk-Forward 분석 실행
    let result = analyzer.run_analysis(base_config).await?;
    // 기간별 성능 분석
    for period in &result.periods {
    }

    Ok(())
}

/// Monte Carlo 시뮬레이션 예제
async fn monte_carlo_example() -> Result<()> {
    // 먼저 기본 백테스팅 실행
    let config = BacktestConfig {
        start_date: Utc::now() - Duration::days(365),
        end_date: Utc::now(),
        initial_capital: 100_000_000.0,
        symbols: vec!["000270".to_string()],
        rebalance_frequency: RebalanceFrequency::Daily,
        transaction_costs: TransactionCosts {
            commission_per_trade: 0.0,
            commission_rate: 0.0015,
            minimum_commission: 15.0,
            maximum_commission: 1000.0,
        },
        slippage: SlippageModel::Fixed(5.0),
        benchmark: None,
        risk_free_rate: 0.025,
        max_position_size: 0.5,
        max_portfolio_risk: 0.2,
    };

    let mut engine = BacktestEngine::new(config.clone());
    let market_data = generate_simulated_data(&config.symbols, config.start_date, config.end_date).await?;
    for (symbol, data) in market_data {
        engine.load_market_data(&symbol, data);
    }

    let buyhold_strategy = BuyHoldStrategy {
        name: "Monte Carlo Test".to_string(),
        weights: {
            let mut weights = HashMap::new();
            weights.insert("000270".to_string(), 1.0);
            weights
        },
        rebalance_threshold: 0.1,
    };

    let backtest_result = engine.run_backtest(&buyhold_strategy).await?;

    // Monte Carlo 설정
    let monte_carlo_config = MonteCarloConfig {
        num_simulations: 1000,
        confidence_levels: vec![0.05, 0.95, 0.01, 0.99],
        bootstrap_method: BootstrapMethod::StationaryBootstrap { block_size: 5 },
        scenario_analysis: true,
        stress_testing: true,
        random_seed: Some(42),
    };

    // Monte Carlo 분석기 생성
    let analyzer = MonteCarloAnalyzer::new(monte_carlo_config, &backtest_result);

    // 시뮬레이션 실행
    let mc_result = analyzer.run_simulation().await?;
    // 신뢰 구간 출력
    for (key, interval) in &mc_result.confidence_intervals {
    }

    // 통계적 검정 결과
    // 시나리오 분석 결과
    if let Some(scenario_analysis) = &mc_result.scenario_analysis {
    }

    Ok(())
}

/// 거래 비용 분석 예제
async fn transaction_cost_example() -> Result<()> {
    // 한국 주식 시장 비용 모델
    let korean_calculator = CostModelFactory::korean_stock_model();
    
    // 미국 주식 시장 비용 모델
    let us_calculator = CostModelFactory::us_stock_model();
    
    // 암호화폐 비용 모델
    let crypto_calculator = CostModelFactory::crypto_model();

    // 거래 시나리오들
    let scenarios = vec![
        ("소액 거래", 1_000_000.0, 100, 0.02, crate::backtesting::transaction_costs::MarketCondition::Sideways),
        ("중간 거래", 10_000_000.0, 1000, 0.02, crate::backtesting::transaction_costs::MarketCondition::Bull),
        ("대량 거래", 100_000_000.0, 10000, 0.03, crate::backtesting::transaction_costs::MarketCondition::Bear),
    ];
    for (market_name, calculator) in [
        ("한국", &korean_calculator),
        ("미국", &us_calculator),
        ("암호화폐", &crypto_calculator),
    ] {
        for (scenario_name, trade_value, quantity, volatility, market_condition) in &scenarios {
            let result = calculator.calculate_costs(
                *trade_value,
                *quantity,
                *volatility,
                market_condition.clone(),
                None,
            )?;
        }
    }

    // 일일 거래 비용 추정
    let daily_result = korean_calculator.estimate_daily_costs(
        50_000_000.0,  // 5천만원 일일 거래대금
        100_000.0,     // 평균 거래금액 10만원
        0.025,         // 2.5% 변동성
        50,            // 50회 거래
    )?;
    // 포트폴리오 리밸런싱 비용
    let rebalancing_result = korean_calculator.estimate_rebalancing_costs(
        100_000_000.0, // 1억원 포트폴리오
        0.2,           // 20% 리밸런싱
        0.02,          // 2% 변동성
        crate::backtesting::transaction_costs::MarketCondition::Sideways,
    )?;
    Ok(())
}

/// 시뮬레이션 데이터 생성
async fn generate_simulated_data(
    symbols: &[String],
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<HashMap<String, Vec<MarketData>>> {
    let mut market_data = HashMap::new();
    let mut current_date = start_date;
    let mut price = 100.0; // 초기 가격

    while current_date <= end_date {
        for symbol in symbols {
            let data_series = market_data.entry(symbol.clone()).or_insert_with(Vec::new);
            
            // 랜덤 워크 시뮬레이션
            let daily_return = (rand::random::<f64>() - 0.5) * 0.04; // ±2% 일일 변동
            price *= 1.0 + daily_return;
            
            let high = price * (1.0 + rand::random::<f64>() * 0.02);
            let low = price * (1.0 - rand::random::<f64>() * 0.02);
            let volume = (rand::random::<u64>() % 1000000) + 100000;

            data_series.push(MarketData {
                symbol: symbol.clone(),
                timestamp: current_date,
                open: price,
                high,
                low,
                close: price,
                volume,
                vwap: (high + low + price) / 3.0,
            });
        }
        
        current_date += Duration::days(1);
    }
    Ok(market_data)
}

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Pekko ML4T System is a Rust implementation of an ML-driven algorithmic trading system using the Pekko Actor model (Rust port of Apache Pekko). The system implements the Machine Learning for Algorithmic Trading (ML4T) workflow with an actor-based architecture for concurrent data processing, ML model execution, risk management, and trade execution.

## Essential Build Commands

### Core Development
```bash
# Build the project
cargo build

# Build in release mode (optimized)
cargo build --release

# Run the main trading system
cargo run

# Run specific example
cargo run --example basic_test
cargo run --example integration_test
cargo run --example trading_simulation_app
cargo run --example api_integration_test

# Run all tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Check code without building
cargo check

# Format code
cargo fmt

# Lint code
cargo clippy

# Clean build artifacts
cargo clean
```

## High-Level Architecture

### Actor-Based System
The system is built around the Pekko Actor model with specialized actors for different responsibilities:

- **DataAgent** (`src/actors/data_agent.rs`) - Collects market data, news, and alternative data sources
- **MLAgent** (`src/actors/ml_agent.rs`) - Manages ML model training, prediction, and evaluation
- **TradingAgent** (`src/actors/trading_agent.rs`) - Generates trading signals and executes orders
- **RiskAgent** (`src/actors/risk_agent.rs`) - Monitors portfolio risk, calculates VaR, and enforces limits
- **MonitoringAgent** (`src/actors/monitoring_agent.rs`) - Tracks system performance and health metrics

Actors communicate via message passing with strongly-typed messages defined in `src/actors/mod.rs`.

### Module Structure

**Data Pipeline** (`src/data/`)
- `collectors/` - Data collection from various APIs (market data, news, alternative data)
- `processors/` - Data cleaning, transformation, feature extraction, technical indicators
- `storage.rs` - Data persistence and retrieval
- `quality.rs` - Data quality monitoring and validation
- `pipeline.rs` - End-to-end data processing pipeline

**Machine Learning** (`src/ml/`)
- `linear_models.rs` - Linear regression, Ridge, Lasso
- `tree_models.rs` - Random Forest, Gradient Boosting, XGBoost
- `deep_learning.rs` - LSTM, GRU, CNN, Transformer models
- `reinforcement_learning.rs` - DQN, PPO, SAC algorithms
- `ensemble.rs` - Model ensembling and stacking
- `evaluation.rs` - Model evaluation metrics and cross-validation
- `optimization.rs` - Hyperparameter optimization
- `model_registry.rs` - Model versioning and management
- `pipeline.rs` - ML pipeline orchestration

**Alpha Factors** (`src/alpha_factors/`)
- `factor_definition.rs` - Alpha factor definitions and structures
- `worldquant_101.rs` - Implementation of WorldQuant 101 Formulaic Alphas
- `factor_research.rs` - Factor research and backtesting framework
- `factor_evaluation.rs` - IC analysis, quantile analysis, performance metrics
- `factor_combination.rs` - Multi-factor portfolio construction
- `factor_monitoring.rs` - Real-time factor performance tracking

**Backtesting** (`src/backtesting/`)
- `engine.rs` - Core backtesting engine
- `strategy.rs` - Trading strategy definitions and implementations
- `performance.rs` - Performance metrics calculation (Sharpe, Sortino, Calmar)
- `risk_metrics.rs` - VaR, CVaR, drawdown analysis
- `walk_forward.rs` - Walk-forward analysis and optimization
- `monte_carlo.rs` - Monte Carlo simulation for strategy validation
- `transaction_costs.rs` - Commission and slippage modeling

**API Integration** (`src/api/`)
- `kis_api.rs` - Korea Investment & Securities API wrapper
- `alpha_vantage.rs` - Alpha Vantage market data API
- `news_api.rs` - News data API integration
- `websocket.rs` - WebSocket connections for real-time data
- `rate_limiter.rs` - API rate limiting and backoff strategies
- `real_kis_api.rs` - Production KIS API implementation
- `real_alpha_vantage_api.rs` - Production Alpha Vantage implementation

**Configuration** (`src/config/`)
- `system_config.rs` - System-wide configuration (markets, capital, models)
- `api_config.rs` - API credentials and connection settings

### Main Trading Loop

The system runs a continuous loop (`src/lib.rs:run_main_loop`) that:

1. Collects and processes market data
2. Generates ML model predictions
3. Assesses portfolio risks
4. Generates trading signals
5. Executes positions
6. Monitors performance

Each step is handled by sending messages to the appropriate actor.

### Key Patterns

**Message Passing**
All actors communicate via strongly-typed messages (enums in `src/actors/mod.rs`). Messages include `DataMessage`, `MLMessage`, `TradingMessage`, `RiskMessage`, and `MonitoringMessage`.

**Async/Await**
The system uses Tokio for async runtime. All actor handlers and API calls are async.

**Error Handling**
Uses `anyhow::Result` for error propagation. Custom error types defined where needed (e.g., `BacktestError`, `ApiError`).

**Configuration Management**
System configuration loaded from environment variables via `SystemConfig::from_env()`. Validation performed before system startup.

## Running Examples

Examples are located in `examples/` directory:

```bash
# Basic actor system test
cargo run --example basic_test

# Actor functionality test
cargo run --example actor_functionality_test

# Integration test with full pipeline
cargo run --example integration_test

# Trading simulation with backtesting
cargo run --example trading_simulation_app

# Simple simulation test
cargo run --example simple_simulation

# API integration tests
cargo run --example api_integration_test
cargo run --example simple_api_test
cargo run --example real_api_test
```

## Environment Variables

Required environment variables (create `.env` file):

```bash
# API Credentials
ALPHA_VANTAGE_API_KEY=your_key
KIS_APP_KEY=your_key
KIS_APP_SECRET=your_secret
KIS_ACCOUNT_NUMBER=your_account

# System Configuration
INITIAL_CAPITAL=10000000
SYMBOLS=AAPL,GOOGL,MSFT,AMZN
PAPER_TRADING=true
```

## ML4T Workflow Implementation

The system implements the standard ML4T workflow:

1. **Data Collection** - Multi-source data aggregation (market, fundamental, alternative)
2. **Feature Engineering** - Technical indicators, alpha factors, derived features
3. **Model Training** - Multiple ML models (linear, tree-based, deep learning, RL)
4. **Backtesting** - Strategy validation with realistic transaction costs
5. **Risk Management** - VaR, position limits, portfolio constraints
6. **Execution** - Order placement with slippage and commission modeling
7. **Monitoring** - Real-time performance tracking and alerting

## Dependencies

Key dependencies:
- `pekko-actor` - Actor system implementation (local path)
- `tokio` - Async runtime
- `serde`/`serde_json` - Serialization
- `reqwest` - HTTP client for APIs
- `tokio-tungstenite` - WebSocket support
- `chrono` - Date/time handling
- `ndarray` - Numerical arrays for ML
- `statrs` - Statistical functions
- `sqlx` - Database operations

## Testing Strategy

Run specific module tests:
```bash
cargo test data::
cargo test ml::
cargo test backtesting::
cargo test actors::
```

Integration tests are in `examples/` rather than `tests/` directory for easier execution with `cargo run --example`.

## Actor System Notes

- The Pekko Actor system is instantiated in `ML4TTradingSystem::new()`
- Actors are spawned via `start_actors()` (currently disabled in implementation)
- Message sending uses `actor_system.send_message::<ActorType, _>(actor_name, message)`
- All actors implement message handlers that are async

## Workspace Structure

This project is part of a larger Pekko Rust workspace defined in `../Cargo.toml`:
- `pekko-actor` - Core actor system
- `pekko-persistence` - Event sourcing
- `pekko-remoting` - Distributed actors
- `pekko-testkit` - Testing utilities
- `pekko-ml4t-system` - This project

Build commands at workspace level affect all members.

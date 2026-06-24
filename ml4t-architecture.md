# ML4T + Pekko-Rust Agentic AI 자동매매 시스템

## 🏗️ 시스템 아키텍처

```
┌─────────────────────────────────────────────────────────────────┐
│                    Pekko-Rust Agent System                      │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │
│  │ Data Agent  │  │ ML Agent    │  │ Trading     │  │ Risk        │ │
│  │             │  │             │  │ Agent       │  │ Agent       │ │
│  │ • Market    │  │ • Alpha     │  │ • Order     │  │ • Portfolio │ │
│  │   Data      │  │   Factors   │  │   Execution │  │   Mgmt      │ │
│  │ • News      │  │ • Models    │  │ • Position  │  │ • Stop Loss │ │
│  │ • Sentiment │  │ • Signals   │  │   Mgmt      │  │ • Exposure  │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘ │
├─────────────────────────────────────────────────────────────────┤
│                    ML4T Workflow Pipeline                       │
├─────────────────────────────────────────────────────────────────┤
│  1. Data Collection → 2. Feature Engineering → 3. Model Training │
│  4. Backtesting → 5. Signal Generation → 6. Risk Management     │
│  7. Execution → 8. Monitoring → 9. Performance Analysis         │
└─────────────────────────────────────────────────────────────────┘
```

## 📊 ML4T 워크플로우 단계별 구현

### 1. 데이터 수집 (Data Collection)
- **Market Data**: OHLCV, Volume, Price feeds
- **Fundamental Data**: Financial statements, Earnings
- **Alternative Data**: News, Social sentiment, Satellite images
- **Real-time Data**: WebSocket feeds, API integration

### 2. 특성 공학 (Feature Engineering)
- **Technical Indicators**: TA-Lib 기반 100+ 지표
- **Alpha Factors**: WorldQuant 101 Formulaic Alphas
- **Time Series Features**: Lag, Rolling statistics
- **Cross-sectional Features**: Relative rankings, Z-scores

### 3. 모델 훈련 (Model Training)
- **Linear Models**: Ridge, Lasso, Elastic Net
- **Tree-based**: Random Forest, XGBoost
- **Deep Learning**: CNN, RNN, LSTM, GRU
- **Ensemble Methods**: Voting, Stacking

### 4. 백테스팅 (Backtesting)
- **Strategy Evaluation**: Sharpe ratio, Max drawdown
- **Walk-forward Analysis**: Out-of-sample testing
- **Monte Carlo Simulation**: Risk assessment
- **Transaction Costs**: Realistic slippage modeling

### 5. 신호 생성 (Signal Generation)
- **Prediction Models**: Return forecasting
- **Classification**: Buy/Sell/Hold signals
- **Portfolio Optimization**: Risk-adjusted returns
- **Signal Aggregation**: Multi-model ensemble

### 6. 리스크 관리 (Risk Management)
- **Position Sizing**: Kelly criterion, Risk parity
- **Stop Loss**: Dynamic trailing stops
- **Portfolio Limits**: Sector, country, asset class
- **VaR Calculation**: Value at Risk models

### 7. 실행 (Execution)
- **Order Management**: Smart order routing
- **Market Impact**: Minimize slippage
- **Latency Optimization**: Low-latency execution
- **Compliance**: Regulatory requirements

### 8. 모니터링 (Monitoring)
- **Real-time P&L**: Live performance tracking
- **Model Drift**: Performance degradation detection
- **System Health**: Infrastructure monitoring
- **Alert System**: Anomaly detection

### 9. 성능 분석 (Performance Analysis)
- **Attribution Analysis**: Factor contribution
- **Risk Decomposition**: Systematic vs idiosyncratic
- **Benchmark Comparison**: Relative performance
- **Reporting**: Automated daily/monthly reports

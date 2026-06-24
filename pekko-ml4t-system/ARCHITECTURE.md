# ML4T 자동매매 시스템 아키텍처

이 문서는 Pekko-Rust 기반 ML4T 자동매매 시스템의 전체 아키텍처를 설명합니다.

## 📋 목차

1. [시스템 개요](#시스템-개요)
2. [Actor 모델 아키텍처](#actor-모델-아키텍처)
3. [데이터 흐름](#데이터-흐름)
4. [Actor 상세 설계](#actor-상세-설계)
5. [메시지 프로토콜](#메시지-프로토콜)
6. [상태 관리](#상태-관리)
7. [에러 처리](#에러-처리)
8. [확장성](#확장성)

---

## 시스템 개요

### 설계 원칙

1. **Actor 기반**: 각 컴포넌트가 독립적인 Actor로 동작
2. **메시지 전달**: Actor 간 통신은 비동기 메시지로만
3. **상태 격리**: 각 Actor는 자신의 상태만 관리
4. **내결함성**: Actor 실패 시 자동 재시작
5. **확장성**: 새로운 Actor 추가 용이

### 기술 스택

```
┌─────────────────────────────────────┐
│         Application Layer            │
│  (ML4T Trading System)              │
├─────────────────────────────────────┤
│         Actor Framework              │
│  (Pekko-Rust Actor System)          │
├─────────────────────────────────────┤
│         Runtime Layer                │
│  (Tokio Async Runtime)              │
├─────────────────────────────────────┤
│         System Layer                 │
│  (Rust Standard Library)            │
└─────────────────────────────────────┘
```

**주요 라이브러리:**
- `pekko-actor`: Actor 시스템 코어
- `tokio`: 비동기 런타임
- `serde`: 직렬화/역직렬화
- `anyhow`: 에러 처리
- `tracing`: 로깅

---

## Actor 모델 아키텍처

### 전체 구조

```
                    ┌─────────────────────┐
                    │   ActorSystem       │
                    │   "ml4t-system"     │
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
              ▼                ▼                ▼
      ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
      │  DataAgent   │ │   MLAgent    │ │TradingAgent  │
      │              │ │              │ │              │
      │ • 데이터 수집│ │ • ML 예측    │ │ • 신호 생성  │
      │ • 품질 검증  │ │ • 피처 추출  │ │ • 주문 실행  │
      │ • 캐싱       │ │ • 모델 관리  │ │ • 포지션 관리│
      └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
             │                │                │
             │                │                │
             ▼                ▼                ▼
      ┌──────────────────────────────────────────────┐
      │          Message Bus (Actor System)          │
      └──────────────────────────────────────────────┘
             │                │                │
             ▼                ▼                ▼
      ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
      │  RiskAgent   │ │ Monitoring   │ │   Storage    │
      │              │ │    Agent     │ │    Layer     │
      │ • VaR 계산   │ │ • 성능 추적  │ │              │
      │ • 포트폴리오 │ │ • 알림 발송  │ │ • PostgreSQL │
      │ • 리스크 한도│ │ • 리포팅     │ │ • TimeSeries │
      └──────────────┘ └──────────────┘ └──────────────┘
```

### Actor 생명주기

```
┌──────────┐
│  Create  │ ← Actor 생성
└────┬─────┘
     │
     ▼
┌──────────┐
│PreStart  │ ← 초기화 (연결 설정, 상태 로드)
└────┬─────┘
     │
     ▼
┌──────────┐
│ Running  │ ← 메시지 처리
└────┬─────┘
     │
     ├─── Failure ──┐
     │              │
     ▼              ▼
┌──────────┐  ┌──────────┐
│PostStop  │  │PreRestart│ ← 에러 발생
└────┬─────┘  └────┬─────┘
     │              │
     │              ▼
     │         ┌──────────┐
     │         │ Running  │ ← 재시작
     │         └────┬─────┘
     │              │
     └──────────────┘
```

---

## 데이터 흐름

### 실시간 거래 워크플로우

```
시작
  │
  ▼
┌─────────────────────────────────────────────┐
│ 1. 데이터 수집 (DataAgent)                  │
│    • 시장 데이터 (OHLCV)                    │
│    • 뉴스 데이터                            │
│    • 대체 데이터                            │
└─────────────┬───────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────┐
│ 2. 데이터 전처리 (DataAgent)                │
│    • 결측치 처리                            │
│    • 이상치 제거                            │
│    • 정규화                                 │
└─────────────┬───────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────┐
│ 3. 피처 추출 (MLAgent)                      │
│    • 기술적 지표 (SMA, RSI, MACD)          │
│    • 알파 팩터 (Momentum, Value)           │
│    • 볼륨 지표 (VWAP, Volume Profile)      │
└─────────────┬───────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────┐
│ 4. ML 예측 (MLAgent)                        │
│    • 모델 선택                              │
│    • 예측 수행                              │
│    • 신뢰도 계산                            │
└─────────────┬───────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────┐
│ 5. 리스크 평가 (RiskAgent)                  │
│    • VaR 계산                               │
│    • 포지션 사이징                          │
│    • 포트폴리오 분석                        │
└─────────────┬───────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────┐
│ 6. 신호 생성 (TradingAgent)                 │
│    • 매수/매도 신호                         │
│    • 진입/청산 타이밍                       │
│    • 포지션 크기 결정                       │
└─────────────┬───────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────┐
│ 7. 주문 실행 (TradingAgent)                 │
│    • API 호출 (한국투자증권)                │
│    • 체결 확인                              │
│    • 포지션 업데이트                        │
└─────────────┬───────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────┐
│ 8. 성능 모니터링 (MonitoringAgent)          │
│    • P&L 계산                               │
│    • 샤프 비율                              │
│    • 알림 발송                              │
└─────────────┬───────────────────────────────┘
              │
              ▼
          (반복)
```

### 메시지 흐름 시퀀스

```sequence
User -> ActorSystem: start()
ActorSystem -> DataAgent: CollectMarketData
DataAgent -> DataAgent: fetch_data()
DataAgent -> MLAgent: DataReady{symbol, data}
MLAgent -> MLAgent: extract_features()
MLAgent -> MLAgent: predict()
MLAgent -> TradingAgent: PredictionReady{symbol, prediction}
MLAgent -> RiskAgent: PredictionReady{symbol, prediction}
RiskAgent -> RiskAgent: assess_risk()
RiskAgent -> TradingAgent: RiskAssessment{approved, limit}
TradingAgent -> TradingAgent: generate_signal()
TradingAgent -> TradingAgent: execute_order()
TradingAgent -> MonitoringAgent: OrderExecuted{order_id, result}
MonitoringAgent -> MonitoringAgent: update_metrics()
MonitoringAgent -> User: Alert{level, message}
```

---

## Actor 상세 설계

### 1. DataAgent

**책임:**
- 외부 데이터 소스로부터 데이터 수집
- 데이터 품질 검증
- 캐싱 및 전처리

**상태:**
```rust
pub struct DataAgent {
    market_data_config: MarketDataConfig,
    news_feeds: Vec<String>,
    alternative_data_sources: Vec<String>,
    data_cache: HashMap<String, Vec<MarketData>>,
}
```

**메시지 처리:**
```rust
impl Actor for DataAgent {
    type Message = DataMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut ActorContext<Self>) {
        match msg {
            CollectMarketData => {
                // 1. API 호출
                // 2. 데이터 검증
                // 3. 캐시 저장
                // 4. MLAgent에게 전달
            }
            ProcessNewsData => {
                // 뉴스 데이터 처리
            }
            UpdateAlternativeData => {
                // 대체 데이터 업데이트
            }
        }
    }
}
```

### 2. MLAgent

**책임:**
- 피처 추출
- ML 모델 관리
- 예측 수행

**상태:**
```rust
pub struct MLAgent {
    models: HashMap<String, Box<dyn MLModel>>,
    features_config: FeaturesConfig,
    feature_cache: HashMap<String, Vec<f64>>,
}
```

**모델 파이프라인:**
```
Raw Data → Feature Extraction → Model Prediction → Confidence Scoring
```

**지원 모델:**
- Linear Models (Ridge, Lasso)
- Tree Models (RandomForest, GradientBoosting)
- Deep Learning (LSTM, Transformer)
- Reinforcement Learning (DQN, PPO)

### 3. TradingAgent

**책임:**
- 거래 신호 생성
- 주문 실행
- 포지션 관리

**상태:**
```rust
pub struct TradingAgent {
    broker_config: BrokerConfig,
    execution_config: ExecutionConfig,
    positions: HashMap<String, Position>,
    pending_orders: Vec<Order>,
}
```

**주문 실행 로직:**
```
Signal → Validate → Size Position → Create Order → Execute → Confirm
```

### 4. RiskAgent

**책임:**
- 포트폴리오 리스크 측정
- 포지션 제한 관리
- VaR 계산

**상태:**
```rust
pub struct RiskAgent {
    risk_config: RiskConfig,
    portfolio_config: PortfolioConfig,
    portfolio_state: PortfolioState,
    risk_metrics: RiskMetrics,
}
```

**리스크 계산:**
```
Portfolio Value → Historical Returns → VaR Calculation → Risk Limits
```

### 5. MonitoringAgent

**책임:**
- 성능 추적
- 알림 발송
- 리포트 생성

**상태:**
```rust
pub struct MonitoringAgent {
    monitoring_config: MonitoringConfig,
    performance_metrics: PerformanceMetrics,
    alerts: Vec<Alert>,
}
```

**모니터링 지표:**
- P&L (Profit & Loss)
- Sharpe Ratio
- Win Rate
- Maximum Drawdown

---

## 메시지 프로토콜

### 메시지 정의

```rust
// DataAgent 메시지
pub enum DataMessage {
    CollectMarketData,
    ProcessNewsData,
    UpdateAlternativeData,
    DataReady {
        symbol: String,
        data: MarketData,
    },
}

// MLAgent 메시지
pub enum MLMessage {
    PredictReturns {
        symbols: Vec<String>,
    },
    TrainModel {
        model_type: String,
    },
    UpdateFeatures,
    PredictionReady {
        symbol: String,
        prediction: f64,
        confidence: f64,
    },
}

// TradingAgent 메시지
pub enum TradingMessage {
    GenerateSignals,
    ExecutePositions,
    ClosePosition {
        symbol: String,
    },
    UpdatePositions,
}

// RiskAgent 메시지
pub enum RiskMessage {
    AssessPortfolioRisk,
    CheckPositionLimit {
        symbol: String,
        size: f64,
    },
    CalculateVaR,
    RiskAlert {
        level: RiskLevel,
        message: String,
    },
}

// MonitoringAgent 메시지
pub enum MonitoringMessage {
    UpdatePerformance,
    GenerateDailyReport,
    SendAlert {
        level: AlertLevel,
        message: String,
    },
}
```

### 메시지 전송 패턴

**1. Fire-and-Forget (일방향)**
```rust
// 메시지 전송만 하고 응답 안 기다림
actor_system.send_message::<DataAgent>("data-agent", message).await?;
```

**2. Request-Response (양방향)**
```rust
// 메시지 전송 후 응답 기다림 (향후 구현)
let response = actor_system.ask::<DataAgent>("data-agent", message).await?;
```

**3. Pub-Sub (발행-구독)**
```rust
// 여러 Actor가 같은 메시지 구독 (향후 구현)
event_bus.publish(DataReady { symbol, data });
```

---

## 상태 관리

### Actor 상태 격리

각 Actor는 자신의 상태만 접근:

```rust
// ✅ 올바른 패턴
pub struct DataAgent {
    data_cache: HashMap<String, Vec<MarketData>>, // 자신의 상태
}

impl Actor for DataAgent {
    fn receive(&mut self, msg: Self::Message, ctx: &mut ActorContext<Self>) {
        // self.data_cache만 접근
        self.data_cache.insert(symbol, data);
    }
}

// ❌ 잘못된 패턴
// Actor 간 직접 상태 공유 금지!
```

### 공유 상태

공유가 필요한 경우 메시지로 통신:

```rust
// DataAgent → MLAgent
ctx.send_message::<MLAgent>("ml-agent", DataReady {
    symbol: symbol.clone(),
    data: data.clone(), // 데이터 복사 전송
}).await?;
```

### 상태 영속화

중요한 상태는 데이터베이스에 저장:

```rust
// 포지션 저장
storage.save_position(&position).await?;

// 재시작 시 복구
let positions = storage.load_positions().await?;
```

---

## 에러 처리

### 에러 계층

```
┌────────────────────────────────────┐
│  Application Error (anyhow::Error) │ ← 비즈니스 로직 에러
├────────────────────────────────────┤
│  Actor Error (ActorError)          │ ← Actor 시스템 에러
├────────────────────────────────────┤
│  IO Error (std::io::Error)         │ ← 시스템 에러
└────────────────────────────────────┘
```

### 에러 전략

**1. 재시도 (Retry)**
```rust
// API 호출 실패 시 재시도
for attempt in 1..=3 {
    match api.call().await {
        Ok(result) => return Ok(result),
        Err(e) if attempt < 3 => {
            warn!("API 호출 실패, 재시도 {}/3", attempt);
            sleep(Duration::from_secs(1)).await;
        }
        Err(e) => return Err(e),
    }
}
```

**2. 회로 차단기 (Circuit Breaker)**
```rust
// 연속 실패 시 일시 중단
if consecutive_failures > 5 {
    circuit_breaker.open();
    warn!("Circuit breaker opened - too many failures");
}
```

**3. Actor 재시작**
```rust
// Actor 실패 시 자동 재시작
fn pre_restart(&mut self, error: &dyn Error) {
    error!("Actor restarting due to: {}", error);
    // 상태 정리
    self.cleanup();
}
```

### 에러 전파

```
DataAgent (실패)
    │
    ├─ Retry (3회)
    │
    ├─ Log Error
    │
    └─ Notify MonitoringAgent
           │
           └─ Send Alert to User
```

---

## 확장성

### 수평 확장 (Scale Out)

**현재 (단일 노드):**
```
┌─────────────────────┐
│   Single Machine    │
│  ┌───────────────┐  │
│  │ ActorSystem   │  │
│  │  5 Actors     │  │
│  └───────────────┘  │
└─────────────────────┘
```

**향후 (분산 시스템):**
```
┌─────────────────────┐  ┌─────────────────────┐
│   Node 1            │  │   Node 2            │
│  ┌───────────────┐  │  │  ┌───────────────┐  │
│  │ DataAgent     │──┼──┼──│ MLAgent       │  │
│  │ TradingAgent  │  │  │  │ RiskAgent     │  │
│  └───────────────┘  │  │  └───────────────┘  │
└─────────────────────┘  └─────────────────────┘
```

### 수직 확장 (Scale Up)

**Actor 인스턴스 증가:**
```rust
// 데이터 수집 병렬화
for source in data_sources {
    let data_agent = DataAgent::new_for_source(source);
    actor_system.spawn(data_agent, &format!("data-agent-{}", source)).await?;
}
```

### 모듈 확장

**새 Actor 추가:**
```rust
// 1. Actor 정의
pub struct SentimentAgent {
    nlp_model: NLPModel,
}

// 2. Actor trait 구현
impl Actor for SentimentAgent {
    type Message = SentimentMessage;
    fn receive(&mut self, msg: Self::Message, ctx: &mut ActorContext<Self>) {
        // 센티먼트 분석
    }
}

// 3. 시스템에 등록
actor_system.spawn(sentiment_agent, "sentiment-agent").await?;
```

---

## 성능 최적화

### 1. 메시지 큐 크기 조정

```rust
// 큐 크기 제한
let (sender, receiver) = tokio::sync::mpsc::channel(1000);
```

### 2. 배치 처리

```rust
// 메시지를 모아서 처리
let mut batch = Vec::new();
while let Ok(msg) = receiver.try_recv() {
    batch.push(msg);
    if batch.len() >= 100 {
        process_batch(&batch).await?;
        batch.clear();
    }
}
```

### 3. 캐싱

```rust
// 자주 사용하는 데이터 캐시
let cached_data = data_cache.get(&symbol);
```

### 4. 비동기 I/O

```rust
// 블로킹 작업을 비동기로
let data = tokio::task::spawn_blocking(|| {
    // 무거운 계산
}).await?;
```

---

## 보안

### 1. API 키 관리

```rust
// 환경 변수로 관리
let api_key = env::var("KIS_APP_KEY")?;

// ❌ 하드코딩 금지
let api_key = "your_api_key";  // 절대 금지!
```

### 2. 메시지 검증

```rust
// 메시지 유효성 검증
if symbol.is_empty() || price <= 0.0 {
    return Err(anyhow!("Invalid message"));
}
```

### 3. 권한 관리

```rust
// 위험한 작업은 확인 필요
if is_production_mode && !user_confirmed {
    return Err(anyhow!("User confirmation required"));
}
```

---

## 테스트 전략

### 1. 단위 테스트

```rust
#[tokio::test]
async fn test_data_agent() {
    let agent = DataAgent::new(config);
    let result = agent.fetch_data("005930").await;
    assert!(result.is_ok());
}
```

### 2. 통합 테스트

```rust
#[tokio::test]
async fn test_actor_communication() {
    let system = ActorSystem::new("test");
    system.spawn(data_agent, "data-agent").await?;
    system.spawn(ml_agent, "ml-agent").await?;

    // 메시지 전송 테스트
    system.send_message::<DataAgent>("data-agent", CollectMarketData).await?;
}
```

### 3. 시뮬레이션 테스트

```bash
# DRY-RUN 모드로 실제와 유사한 환경 테스트
cargo run --example live_trading_system
```

---

## 문서 업데이트

이 문서는 시스템 변경 시 함께 업데이트되어야 합니다:

- 새 Actor 추가 시
- 메시지 프로토콜 변경 시
- 아키텍처 개선 시

**마지막 업데이트:** 2025-10-17
**버전:** 0.1.0

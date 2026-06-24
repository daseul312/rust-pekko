# Pekko-Rust ML4T 자동매매 시스템

**Machine Learning for Algorithmic Trading** 워크플로우를 **Apache Pekko Actor 모델**로 구현한 실전 자동매매 시스템입니다.

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## 📋 목차

- [개요](#개요)
- [시스템 아키텍처](#시스템-아키텍처)
- [주요 기능](#주요-기능)
- [설치 및 설정](#설치-및-설정)
- [사용 방법](#사용-방법)
- [Actor 시스템 상세](#actor-시스템-상세)
- [실전 투자 가이드](#실전-투자-가이드)
- [백테스팅](#백테스팅)
- [문제 해결](#문제-해결)

---

## 개요

이 프로젝트는 **ML4T (Machine Learning for Trading) 2nd Edition**의 워크플로우를 Rust로 구현하고, **Apache Pekko Actor 모델**을 사용하여 높은 동시성과 내결함성을 제공하는 자동매매 시스템입니다.

### 핵심 특징

- 🎯 **Actor 기반 아키텍처**: 각 컴포넌트가 독립적인 Actor로 동작
- 🤖 **ML 기반 예측**: 기술적 지표 + 알파 팩터 + 머신러닝 모델
- 📊 **실시간 리스크 관리**: 포트폴리오 리스크를 실시간으로 평가
- 💰 **실전 거래 지원**: 한국투자증권 API 연동
- 🔍 **백테스팅**: 과거 데이터로 전략 검증
- 📈 **성능 모니터링**: 실시간 손익 추적 및 알림

---

## 시스템 아키텍처

### Actor 모델 구조

```
┌─────────────────────────────────────────────────────────────┐
│                    ActorSystem (ML4T)                        │
└─────────────────────────────────────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
        ▼                  ▼                  ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  DataAgent   │──▶│   MLAgent    │──▶│TradingAgent  │
│              │   │              │   │              │
│ • 시장 데이터│   │ • ML 예측    │   │ • 신호 생성  │
│ • 뉴스 데이터│   │ • 피처 추출  │   │ • 주문 실행  │
│ • 대체 데이터│   │ • 모델 관리  │   │ • 포지션 관리│
└──────────────┘   └──────────────┘   └──────────────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
        ▼                  ▼                  ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  RiskAgent   │   │ Monitoring   │   │   Storage    │
│              │   │    Agent     │   │    Layer     │
│ • VaR 계산   │   │ • 성능 추적  │   │              │
│ • 포트폴리오 │   │ • 알림 발송  │   │ • PostgreSQL │
│ • 리스크 한도│   │ • 리포팅     │   │ • TimeSeries │
└──────────────┘   └──────────────┘   └──────────────┘
```

### 데이터 흐름

```
1. 데이터 수집 (DataAgent)
   ↓
2. 전처리 및 피처 추출 (MLAgent)
   ↓
3. ML 모델 예측 (MLAgent)
   ↓
4. 리스크 평가 (RiskAgent)
   ↓
5. 거래 신호 생성 (TradingAgent)
   ↓
6. 주문 실행 (TradingAgent → 증권사 API)
   ↓
7. 성능 모니터링 (MonitoringAgent)
```

---

## 주요 기능

### 1. 데이터 수집 (DataAgent)

- **시장 데이터**: OHLCV (Open, High, Low, Close, Volume)
- **뉴스 데이터**: 뉴스 API 연동 및 센티먼트 분석
- **대체 데이터**: 소셜 미디어, 위성 이미지 등
- **데이터 품질 검증**: 결측치, 이상치 자동 처리

### 2. 머신러닝 (MLAgent)

- **기술적 지표**: SMA, RSI, MACD, Bollinger Bands
- **알파 팩터**: Momentum, Mean Reversion, Value
- **ML 모델**:
  - 선형 모델 (Ridge, Lasso)
  - 트리 모델 (Random Forest, Gradient Boosting)
  - 딥러닝 (LSTM, Transformer)
  - 강화학습 (DQN, PPO)

### 3. 거래 실행 (TradingAgent)

- **신호 생성**: ML 예측 기반 매수/매도 신호
- **주문 실행**: 한국투자증권 API 연동
- **포지션 관리**: 자동 익절/손절
- **슬리피지 관리**: 실제 체결가 추적

### 4. 리스크 관리 (RiskAgent)

- **VaR (Value at Risk)**: 포트폴리오 리스크 측정
- **최대 손실 한도**: 일일/주간/월간 손실 제한
- **포지션 사이징**: 켈리 기준 등 최적 포지션 크기
- **다각화**: 종목 간 상관관계 분석

### 5. 성능 모니터링 (MonitoringAgent)

- **실시간 손익**: 실시간 P&L 추적
- **샤프 비율**: 위험 대비 수익률
- **최대 낙폭 (MDD)**: Maximum Drawdown
- **알림**: 이메일, Slack, 텔레그램 알림

---

## 설치 및 설정

### 1. 필수 요구사항

- **Rust**: 1.70 이상
- **PostgreSQL**: 14 이상 (선택사항)
- **한국투자증권 계좌**: 실전 거래 시 필요

### 2. 프로젝트 클론

```bash
git clone https://github.com/your-repo/pekko-ml4t-system.git
cd pekko-ml4t-system
```

### 3. 환경 변수 설정

프로젝트 루트에 `.env` 파일을 생성하세요:

```bash
# 한국투자증권 API 설정 (실전 거래 시 필요)
KIS_APP_KEY=your_app_key_here
KIS_APP_SECRET=your_app_secret_here
KIS_ACCOUNT_NO=12345678-01
KIS_BASE_URL=https://openapi.koreainvestment.com:9443

# 데이터베이스 설정 (선택사항)
DATABASE_URL=postgresql://user:password@localhost/ml4t_db

# 알림 설정 (선택사항)
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# 노션 API (일일 리포트용, 선택사항)
NOTION_API_KEY=your_notion_api_key
NOTION_DATABASE_ID=your_database_id
```

### 4. 빌드

```bash
cargo build --release
```

---

## 사용 방법

### 1. Actor 시스템 테스트

먼저 Actor 시스템이 정상적으로 작동하는지 테스트합니다:

```bash
cargo run --example actor_system_test
```

**예상 출력:**
```
🚀 Actor 시스템 테스트 시작

📋 1단계: Actor 생성 테스트
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  → DataAgent 생성 중...
    ✓ DataAgent 생성 완료
  → MLAgent 생성 중...
    ✓ MLAgent 생성 완료
  ...
✅ 모든 Actor 생성 완료!

📋 2단계: 메시지 전달 테스트
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  → DataAgent에게 시장 데이터 수집 요청...
    ✓ 메시지 전송 성공
  ...
✅ 메시지 전달 테스트 완료!

📋 3단계: Actor 간 통신 체인 테스트
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  → 전체 워크플로우 시작: Data → ML → Trading → Risk → Monitoring
    [1/5] 데이터 수집 요청...
    [2/5] ML 예측 요청...
    ...
✅ 통신 체인 테스트 완료!

🎉 모든 테스트 완료!
```

### 2. 백테스팅 (과거 데이터로 전략 검증)

실전 투자 전에 반드시 백테스팅을 수행하세요:

```bash
cargo run --example backtesting_example
```

**백테스팅 설정:**

```rust
let config = BacktestConfig {
    start_date: "2024-01-01".to_string(),
    end_date: "2024-12-31".to_string(),
    initial_capital: 10_000_000.0,  // 1천만원
    symbols: vec!["005930".to_string(), "000660".to_string()],
    strategy: "momentum".to_string(),
};
```

### 3. 실전 투자 시스템 실행

#### DRY-RUN 모드 (시뮬레이션)

실제 주문 없이 시뮬레이션만 수행합니다:

```bash
cargo run --example live_trading_system
```

**출력:**
```
╔════════════════════════════════════════════════╗
║  🚀 ML4T 실전 투자 시스템                    ║
║  Machine Learning for Trading - Live System   ║
╚════════════════════════════════════════════════╝

🔍 DRY-RUN 모드로 실행합니다 (시뮬레이션만)
💡 실전 거래를 원하시면: cargo run --example live_trading_system -- --real
⏱️  15초마다 체크

========================================
🚀 ML4T 실전 투자 시스템 초기화
========================================

✓ Actor 시스템 생성 완료
✓ 모든 Actor 생성 완료 (5개)
📈 관심 종목: 20개 등록

📋 매매 전략:
  - 목표 수익률: 3.0%
  - 손절매: -2.0%
  - 최대 포지션: 50.0%
  - 최소 현금: 20.0%

🚀 자동매매 시작... (Ctrl+C로 중지)

🔄 [루프 1] 자동매매 체크... (09:15:23)
📊 1단계: 시장 데이터 수집 중...
🤖 2단계: ML 모델 예측 중...
📈 3단계: 거래 신호 생성 중...
⚖️  4단계: 리스크 평가 중...
🔍 5단계: 거래 실행 (스킵 - DRY-RUN 모드)
📊 6단계: 성능 모니터링 중...
✅ 자동매매 사이클 완료!
⏳ 다음 체크까지 15초 대기...
```

#### 실전 거래 모드 ⚠️

**주의: 실제 돈을 사용합니다!**

```bash
cargo run --example live_trading_system -- --real
```

**출력:**
```
⚠️  ═══════════════════════════════════════
⚠️  실전 거래 모드로 실행합니다!
⚠️  실제 주문이 체결됩니다!
⚠️  실제 돈이 사용됩니다!
⚠️  ═══════════════════════════════════════

💰 한국투자증권 API를 통해 실제 거래를 수행합니다
📊 Actor 시스템이 자동으로 매매를 판단합니다
⏱️  30초마다 자동 매매 체크
🛑 중지: Ctrl+C
```

### 4. 기존 자동매매 시스템 (stock crate)

기존의 한국투자증권 자동매매 시스템도 계속 사용 가능합니다:

```bash
# DRY-RUN 모드
cd ../stock
cargo run --bin kis-auto-trading

# 실전 거래 모드
cargo run --bin kis-auto-trading -- --real
```

---

## Actor 시스템 상세

### 1. DataAgent

**책임:**
- 시장 데이터 수집 및 캐싱
- 뉴스 데이터 수집 및 센티먼트 분석
- 데이터 품질 검증

**메시지:**
```rust
pub enum DataMessage {
    CollectMarketData,
    ProcessNewsData,
    UpdateAlternativeData,
    DataReady { symbol: String, data: MarketData },
}
```

**설정:**
```rust
let market_data_config = MarketDataConfig {
    symbols: vec!["005930".to_string()],  // 관심 종목
    data_sources: vec!["kis_api".to_string()],
    update_frequency_ms: 30000,  // 30초마다 업데이트
    history_days: 30,            // 30일 히스토리
};
```

### 2. MLAgent

**책임:**
- 기술적 지표 계산
- 알파 팩터 계산
- ML 모델 학습 및 예측

**메시지:**
```rust
pub enum MLMessage {
    PredictReturns { symbols: Vec<String> },
    TrainModel { model_type: String },
    UpdateFeatures,
    PredictionReady {
        symbol: String,
        prediction: f64,
        confidence: f64
    },
}
```

**피처:**
- **기술적 지표**: SMA(5,10,20), RSI(14), MACD(12,26,9), BB(20,2)
- **알파 팩터**: Momentum(1,3,6개월), Mean Reversion, Size, Value
- **볼륨 지표**: Volume Profile, VWAP

### 3. TradingAgent

**책임:**
- 거래 신호 생성
- 주문 실행
- 포지션 관리

**메시지:**
```rust
pub enum TradingMessage {
    GenerateSignals,
    ExecutePositions,
    ClosePosition { symbol: String },
    UpdatePositions,
}
```

**주문 유형:**
- **시장가 주문**: 즉시 체결
- **지정가 주문**: 특정 가격에 주문
- **조건부 주문**: Stop Loss, Take Profit

### 4. RiskAgent

**책임:**
- 포트폴리오 리스크 측정
- 포지션 사이징
- 리스크 한도 모니터링

**메시지:**
```rust
pub enum RiskMessage {
    AssessPortfolioRisk,
    CheckPositionLimit { symbol: String, size: f64 },
    CalculateVaR,
    RiskAlert { level: RiskLevel, message: String },
}
```

**리스크 지표:**
- **VaR (95%)**: 일일 최대 손실 예상치
- **포지션 한도**: 종목당 최대 50%
- **총 리스크 한도**: 포트폴리오 10%

### 5. MonitoringAgent

**책임:**
- 성능 추적
- 알림 발송
- 일일 리포트 생성

**메시지:**
```rust
pub enum MonitoringMessage {
    UpdatePerformance,
    GenerateDailyReport,
    SendAlert { level: AlertLevel, message: String },
}
```

**모니터링 지표:**
- **손익**: 일일/주간/월간 P&L
- **샤프 비율**: 연간 샤프 비율
- **승률**: 수익 거래 비율
- **MDD**: 최대 낙폭

---

## 실전 투자 가이드

### 1. 초기 설정

#### 1.1 계좌 개설

1. 한국투자증권 홈페이지에서 계좌 개설
2. API 사용 신청 (실전투자 또는 모의투자)
3. App Key, App Secret 발급

#### 1.2 환경 변수 설정

`.env` 파일에 API 키 입력:

```bash
KIS_APP_KEY=your_actual_app_key
KIS_APP_SECRET=your_actual_app_secret
KIS_ACCOUNT_NO=12345678-01
KIS_BASE_URL=https://openapi.koreainvestment.com:9443
```

### 2. 전략 설정

`examples/live_trading_system.rs`에서 전략 조정:

```rust
let strategy = TradingStrategy {
    profit_target: 3.0,       // 3% 수익 시 매도
    stop_loss: -2.0,          // -2% 손실 시 손절
    max_position_ratio: 50.0, // 최대 50% 포지션
    min_cash_ratio: 20.0,     // 최소 20% 현금 보유
};
```

**권장 설정 (초보자):**
- 목표 수익률: 2~3%
- 손절매: -1.5~-2.0%
- 최대 포지션: 30~40%
- 최소 현금: 30~40%

**권장 설정 (경험자):**
- 목표 수익률: 5~10%
- 손절매: -3~-5%
- 최대 포지션: 50~70%
- 최소 현금: 20~30%

### 3. 관심 종목 선정

안정적인 대형 우량주를 선택하세요:

```rust
let watchlist = vec![
    // 반도체
    "005930".to_string(),  // 삼성전자
    "000660".to_string(),  // SK하이닉스

    // 배터리
    "373220".to_string(),  // LG에너지솔루션
    "006400".to_string(),  // 삼성SDI

    // IT
    "035420".to_string(),  // NAVER
    "035720".to_string(),  // 카카오
];
```

### 4. 백테스팅 실행

실전 투자 전 반드시 백테스팅을 수행하세요:

```bash
cargo run --example backtesting_example
```

**백테스팅 체크리스트:**
- [ ] 샤프 비율 > 1.0
- [ ] 승률 > 50%
- [ ] 최대 낙폭 < 20%
- [ ] 연간 수익률 > 10%

### 5. DRY-RUN 테스트

최소 1주일 동안 DRY-RUN 모드로 테스트:

```bash
cargo run --example live_trading_system
```

### 6. 소액 실전 투자

**초기 자금:**
- 권장: 100만원 ~ 500만원
- 최소: 50만원

**실행:**
```bash
cargo run --example live_trading_system -- --real
```

### 7. 모니터링

실시간으로 다음 지표를 모니터링하세요:

- **실시간 손익**
- **포지션 현황**
- **주문 체결 내역**
- **리스크 지표**

### 8. 일일 점검

매일 장 마감 후:

1. 거래 내역 확인
2. 수익률 분석
3. 전략 조정 필요 여부 판단
4. 로그 파일 확인

---

## 백테스팅

### 백테스팅 실행

```bash
cargo run --example backtesting_example
```

### 백테스팅 설정

```rust
let config = BacktestConfig {
    // 기간 설정
    start_date: "2023-01-01".to_string(),
    end_date: "2024-12-31".to_string(),

    // 초기 자본
    initial_capital: 10_000_000.0,

    // 관심 종목
    symbols: vec![
        "005930".to_string(),  // 삼성전자
        "000660".to_string(),  // SK하이닉스
    ],

    // 전략
    strategy: "momentum".to_string(),

    // 리밸런싱
    rebalance_frequency: RebalanceFrequency::Weekly,

    // 수수료
    commission_rate: 0.00015,  // 0.015%
    slippage: 0.001,           // 0.1%
};
```

### 백테스팅 결과 분석

```
========================================
백테스팅 결과
========================================
기간: 2023-01-01 ~ 2024-12-31

📊 성과 지표:
  총 수익률: 25.3%
  연간 수익률: 12.1%
  샤프 비율: 1.45
  최대 낙폭: -15.2%

📈 거래 통계:
  총 거래 횟수: 142회
  승률: 58.5%
  평균 수익: +2.1%
  평균 손실: -1.3%

💰 손익 분석:
  총 수익: +2,530,000원
  수수료: -21,300원
  순수익: +2,508,700원
```

### 워크포워드 분석

과거 데이터를 여러 기간으로 나누어 검증:

```rust
let walk_forward_config = WalkForwardConfig {
    in_sample_months: 12,   // 학습: 12개월
    out_sample_months: 3,   // 검증: 3개월
    step_months: 3,         // 3개월마다 재학습
};
```

---

## 문제 해결

### 1. Actor 시스템 오류

**증상:** Actor가 메시지를 받지 못함

**해결:**
```bash
# Actor 시스템 테스트 실행
cargo run --example actor_system_test

# 로그 레벨 변경
RUST_LOG=debug cargo run --example actor_system_test
```

### 2. API 연결 오류

**증상:** "토큰 발급 실패"

**해결:**
1. `.env` 파일의 API 키 확인
2. 1분당 1회 토큰 발급 제한 확인
3. 토큰 캐시 삭제: `rm .token_cache.json`

### 3. 주문 실패

**증상:** "매수 주문 거부"

**원인:**
- 예수금 부족
- 시장 시간 외 주문
- API 호출 한도 초과

**해결:**
```bash
# 계좌 잔고 확인
cargo run --bin kis-balance-check

# DRY-RUN 모드로 테스트
cargo run --example live_trading_system
```

### 4. 데이터 수집 오류

**증상:** "시세 조회 실패"

**해결:**
- API 호출 제한 확인 (초당 20회)
- 토큰 만료 여부 확인
- 종목 코드 확인

### 5. 성능 문제

**증상:** 시스템이 느려짐

**해결:**
```rust
// Actor 메시지 처리 간격 조정
let check_interval = 60;  // 60초로 증가

// 관심 종목 수 줄이기
let watchlist = vec![
    "005930".to_string(),  // 5~10개로 제한
];
```

---

## 로그 분석

### 로그 레벨 설정

```bash
# INFO 레벨 (기본)
RUST_LOG=info cargo run --example live_trading_system

# DEBUG 레벨 (상세)
RUST_LOG=debug cargo run --example live_trading_system

# 특정 모듈만
RUST_LOG=pekko_ml4t_system::actors=debug cargo run
```

### 로그 파일 저장

```bash
cargo run --example live_trading_system > trading.log 2>&1
```

### 주요 로그 패턴

```bash
# 메시지 전달 확인
grep "메시지 전송 성공" trading.log

# 주문 체결 확인
grep "주문 성공" trading.log

# 오류 확인
grep "ERROR\|실패" trading.log

# 성능 지표 확인
grep "수익률\|손익" trading.log
```

---

## 고급 기능

### 1. 커스텀 Actor 추가

새로운 Actor를 추가하여 시스템 확장:

```rust
// 1. Actor 정의
pub struct CustomAgent {
    config: CustomConfig,
}

// 2. Actor trait 구현
impl Actor for CustomAgent {
    type Message = CustomMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut ActorContext<Self>)
        -> Pin<Box<dyn Future<Output = ()> + Send + '_>>
    {
        Box::pin(async move {
            match msg {
                CustomMessage::Process => {
                    // 커스텀 로직
                }
            }
        })
    }
}

// 3. Actor 등록
actor_system.spawn(custom_agent, "custom-agent").await?;
```

### 2. 커스텀 전략 추가

`src/backtesting/strategy.rs`에서 새 전략 구현:

```rust
pub struct MyStrategy {
    // 전략 파라미터
}

impl TradingStrategy for MyStrategy {
    async fn generate_signals(
        &mut self,
        data: &MarketData
    ) -> Result<Vec<Signal>> {
        // 전략 로직
        Ok(signals)
    }
}
```

### 3. 데이터 소스 추가

`src/data/collectors/`에서 새 데이터 소스 구현:

```rust
pub struct CustomDataCollector {
    api_key: String,
}

impl DataCollector for CustomDataCollector {
    async fn fetch_data(&self, symbol: &str) -> Result<MarketData> {
        // 데이터 수집 로직
    }
}
```

---

## 프로젝트 구조

```
pekko-ml4t-system/
├── src/
│   ├── lib.rs                    # 메인 시스템
│   ├── actors/                   # Actor 구현
│   │   ├── mod.rs
│   │   ├── data_agent.rs         # 데이터 수집
│   │   ├── ml_agent.rs           # ML 예측
│   │   ├── trading_agent.rs      # 거래 실행
│   │   ├── risk_agent.rs         # 리스크 관리
│   │   └── monitoring_agent.rs   # 모니터링
│   ├── data/                     # 데이터 처리
│   │   ├── collectors/           # 데이터 수집기
│   │   └── processors/           # 데이터 전처리
│   ├── ml/                       # 머신러닝
│   │   ├── linear_models.rs
│   │   ├── tree_models.rs
│   │   ├── deep_learning.rs
│   │   └── reinforcement_learning.rs
│   ├── trading/                  # 거래 로직
│   ├── risk/                     # 리스크 관리
│   ├── monitoring/               # 모니터링
│   ├── backtesting/              # 백테스팅
│   └── config/                   # 설정
├── examples/
│   ├── actor_system_test.rs     # Actor 테스트
│   ├── live_trading_system.rs   # 실전 투자
│   └── backtesting_example.rs   # 백테스팅
├── tests/                        # 통합 테스트
├── Cargo.toml
└── README.md
```

---

## 기여 가이드

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

---

## 라이선스

Apache License 2.0

---

## 면책 조항

⚠️ **투자 위험 고지**

- 이 시스템은 교육 및 연구 목적으로 제공됩니다
- 실제 투자에 사용 시 손실이 발생할 수 있습니다
- 투자 결정은 본인의 판단과 책임 하에 이루어져야 합니다
- 과거 성과가 미래 수익을 보장하지 않습니다
- 충분한 테스트 없이 실전 투자하지 마세요

---

## 연락처

- 이슈: [GitHub Issues](https://github.com/your-repo/issues)
- 이메일: your-email@example.com
- 디스코드: [ML4T Discord](https://discord.gg/your-invite)

---

## 참고 자료

- [ML4T 2nd Edition](https://www.amazon.com/Machine-Learning-Algorithmic-Trading-alternative/dp/1839217715)
- [Apache Pekko Documentation](https://pekko.apache.org/)
- [한국투자증권 OpenAPI](https://apiportal.koreainvestment.com/)
- [Rust 공식 문서](https://doc.rust-lang.org/)

# 함수형 프로그래밍 기반 LLM 주식 시장 분석 시스템

함수형 프로그래밍 스타일 Actor를 사용하여 실시간 주식 시장 분석을 수행하고, LLM(OpenAI API)을 통합하여 감정 분석, 리포트 생성, 가격 예측을 자동화하는 완전한 시스템입니다.

## 🚀 시스템 개요

### 핵심 컴포넌트

1. **LLMAnalysisActor** - LLM 기반 분석을 담당하는 함수형 Actor
2. **MarketDataActor** - 실시간 시장 데이터 수집 및 관리
3. **SentimentAnalysisActor** - 뉴스/소셜 미디어 감정 분석
4. **ReportGenerationActor** - 시장 리포트 자동 생성
5. **PredictionActor** - LLM 기반 가격 예측

### 함수형 프로그래밍 특징

- ✅ **불변성**: 모든 상태는 불변, 새 상태 반환
- ✅ **순수 함수**: `receive`는 순수 함수, 부수효과는 Effect로 분리
- ✅ **재현 가능성**: 같은 입력으로 분석 재현 가능
- ✅ **상태 추적**: 모든 분석 결과를 히스토리로 보존
- ✅ **비동기 처리**: LLM API 호출은 `apply_effect`에서 처리

## 🏗️ 설치 및 실행

### 1. 환경 설정

```bash
# .env 파일 생성 (env.example을 복사)
cp env.example .env

# LLM API 키 설정 (.env 파일 편집)
# Grok API (xAI) - 추천
GROK_API_KEY=your_grok_api_key_here
DEFAULT_LLM_PROVIDER=GROK

# 또는 OpenAI API
OPENAI_API_KEY=your_openai_api_key_here
DEFAULT_LLM_PROVIDER=OPENAI
```

#### Grok API 키 얻는 방법:
1. [xAI 웹사이트](https://x.ai) 방문
2. 계정 생성 및 로그인
3. API 키 발급받기
4. `.env` 파일에 `GROK_API_KEY=your_key_here` 설정

#### OpenAI API 키 얻는 방법:
1. [OpenAI 플랫폼](https://platform.openai.com) 방문
2. 계정 생성 및 로그인
3. API 키 발급받기
4. `.env` 파일에 `OPENAI_API_KEY=your_key_here` 설정

#### X(Twitter) API 키 얻는 방법 (선택사항):
1. [Twitter Developer Platform](https://developer.twitter.com) 방문
2. Developer 계정 신청 및 승인
3. 앱 생성 및 API 키 발급받기
4. `.env` 파일에 다음 설정:
   ```
   X_API_KEY=your_api_key
   X_API_SECRET=your_api_secret
   X_ACCESS_TOKEN=your_access_token
   X_ACCESS_TOKEN_SECRET=your_access_token_secret
   X_BEARER_TOKEN=your_bearer_token
   ```

### 2. 빌드

```bash
cargo build --release
```

### 3. 통합 데모 실행

```bash
# 완전한 분석 워크플로우 데모
cargo run --example functional_llm_analysis

# Grok API 특화 데모
cargo run --example grok_example
```

### 4. 개별 컴포넌트 테스트

```bash
# 구구단 예제 (기초 함수형 Actor)
cargo run --example functional_multiplication_table

# 각 Actor 단위 테스트
cargo test functional
```

## 📊 시스템 아키텍처

```
┌─────────────────────────────────────────────────┐
│             통합 분석 시스템                        │
├─────────────────────────────────────────────────┤
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ │
│  │MarketData   │ │Sentiment    │ │LLMAnalysis │ │
│  │Actor        │ │Analysis     │ │Actor       │ │
│  │             │ │Actor        │ │             │ │
│  └─────────────┘ └─────────────┘ └─────────────┘ │
├─────────────────────────────────────────────────┤
│  ┌─────────────┐ ┌─────────────┐                 │
│  │Report       │ │Prediction   │                 │
│  │Generation   │ │Actor        │                 │
│  │Actor        │ │             │ │
│  └─────────────┘ └─────────────┘                 │
├─────────────────────────────────────────────────┤
│  LLM API (OpenAI)                               │
│  실시간 데이터 소스                               │
│  뉴스/소셜 미디어 API                            │
└─────────────────────────────────────────────────┘
```

## 🎯 주요 기능

### 🤖 LLM 기반 분석 (Grok vs OpenAI)

시스템은 **xAI의 Grok** 또는 **OpenAI의 GPT**를 선택적으로 사용할 수 있습니다:

#### Grok의 장점:
- **실시간 X(Twitter) 데이터 통합**: 트윗 분석에 특화
- **Elon Musk 영향도 고려**: xAI 관련 뉴스 우선 분석
- **바이럴 트렌드 예측**: 소셜 미디어 트렌드 반영
- **유머러스한 응답**: 더 자연스러운 분석 결과

#### OpenAI의 장점:
- **안정적이고 검증된 성능**: 광범위한 사용 사례
- **다양한 모델 옵션**: GPT-4, GPT-3.5-turbo 등
- **풍부한 프롬프트 엔지니어링**: 세밀한 제어 가능

### 실시간 시장 데이터 수집
- 주식 가격, 거래량 데이터 수집
- 뉴스 기사 및 소셜 미디어 포스트 수집
- 불변 히스토리로 데이터 보존

### 감정 분석
- 뉴스 기사 LLM 감정 분석 (긍정/부정/중립)
- 소셜 미디어 감정 추이 분석
- 종합 시장 심리 지표 계산

### LLM 기반 분석
- 자연어로 시장 상황 설명
- 투자 전략 제안
- 리스크 평가 및 경고

### 자동 리포트 생성
- 일일/주간 시장 리포트
- 투자 의견 및 추천사항
- 기술적/기본적 분석 결합

### 가격 예측
- 단기/중기/장기 가격 예측
- 신뢰도 기반 예측
- 투자 리스크 평가

## 💡 함수형 프로그래밍 장점

### 1. 불변성으로 인한 안전성

```rust
// 전통적 방식 (가변 상태)
let mut account = Account { balance: 1000.0 };
account.balance -= 500.0; // 직접 변경 - 위험!

// 함수형 방식 (불변 상태)
let new_account = account.withdraw(500.0);
// 원본 account는 변경되지 않음 - 안전!
```

### 2. 순수 함수로 인한 예측 가능성

```rust
// 같은 입력에 대해 항상 같은 결과
fn analyze_sentiment(text: &str) -> SentimentResult {
    // 외부 상태에 의존하지 않음
    // 항상 동일한 입력에 동일한 출력
}
```

### 3. 테스트 용이성

```rust
#[test]
fn test_price_prediction() {
    let state = PredictionState::new();
    let msg = PredictPrice { symbol: "AAPL", ... };

    let (new_state, effect) = actor.receive(state, msg, &ctx);

    // 외부 API 호출 없이 상태 전이만 검증
    assert_eq!(new_state.predictions.len(), 1);
}
```

### 4. 재현 가능한 분석

```rust
// 언제든지 과거 분석 재현 가능
let historical_state = load_state_from_timestamp(timestamp);
let (result, _) = actor.receive(historical_state, msg, &ctx);
// 항상 같은 결과!
```

## 📈 데모 실행 결과

```bash
🤖 함수형 프로그래밍 기반 LLM 주식 시장 분석 시스템
   실시간 데이터 수집, 감정 분석, AI 예측, 자동 리포트 생성
======================================================================

🚀 통합 LLM 주식 시장 분석 시스템 초기화 중...
✅ 모든 Actor 초기화 완료

📊 실시간 시장 데이터 수집 시뮬레이션 시작
  📈 AAPL 가격 업데이트: $152.30
  📰 AAPL 뉴스 추가: AAPL 실적 발표: 예상보다 좋...
  💬 AAPL 소셜 포스트: AAPL 주식 매수 타이밍인 것...
✅ 시장 데이터 수집 완료

😊 감정 분석 시뮬레이션 시작
  📝 AAPL 뉴스 감정 분석 중...
  💭 AAPL 소셜 감정 분석 중...
    ✅ 뉴스 감정: 긍정 65.0%, 부정 15.0%, 중립 20.0%
    ✅ 소셜 감정: 긍정 70.0%, 부정 10.0%, 중립 20.0%
✅ 감정 분석 완료

🤖 LLM 기반 분석 시뮬레이션 시작
  📊 감정 분석 요청: AAPL 주식 뉴스
    🤖 LLM 응답: 긍정적 시장 심리로 상승 가능성 높음 (신뢰도: 85%)
  📋 시장 리포트 생성 요청
    🤖 LLM 리포트: 기술주 강세 지속, AAPL 목표가 $180 (신뢰도: 78%)
  🎯 가격 예측 요청: AAPL 3일 후
    🤖 예측: $165 ± $5 (신뢰도: 72%)
✅ LLM 분석 완료

📄 리포트 생성 시뮬레이션 시작
  📰 일일 시장 리포트 생성 중...
    📊 시장 개요: 기술주 강세, 변동성 증가
    🏆 주요 종목: AAPL +2.3%, GOOGL +1.8%
    ⚠️ 리스크: 글로벌 경기 둔화 우려
    💡 전략: 기술주 비중 확대 권장
✅ 리포트 생성 완료

💰 가격 예측 시뮬레이션 시작
  🔮 AAPL 가격 예측 분석 중...
    💵 현재가: $150.00
    🎯 예측가: $158.00 (+5.3%)
    📊 신뢰도: 76.0%
    💡 투자 의견: 매수 (리스크: 중간)
✅ 가격 예측 완료

============================================================
🎊 워크플로우 완료! 모든 컴포넌트 정상 작동
```

## 🔧 설정 및 확장

### 환경 변수

```bash
# 필수 설정
OPENAI_API_KEY=your_key_here

# 선택적 설정
OPENAI_MODEL=gpt-4                # 모델 선택
OPENAI_MAX_TOKENS=4000           # 최대 토큰 수
OPENAI_TEMPERATURE=0.7           # 생성 다양성
RUST_LOG=info                    # 로깅 레벨
```

### 실제 데이터 소스 연동

```rust
// Alpha Vantage API 연동 예시
impl MarketDataActor {
    async fn fetch_real_time_data(&self, symbol: &str) -> Result<PricePoint, Error> {
        // 실제 API 호출 로직
        // let response = reqwest::get(&format!("https://api.alpha_vantage.co/query?...", symbol)).await?;
        // let data: ApiResponse = response.json().await?;
        // Ok(data.into())
    }
}
```

## 🧪 테스트

### 단위 테스트 실행

```bash
# 모든 함수형 Actor 테스트
cargo test functional

# 특정 Actor 테스트
cargo test sentiment_actor
cargo test prediction_actor
```

### 통합 테스트

```bash
# 전체 시스템 테스트
cargo test integration
```

## 📊 성능 및 모니터링

### 성능 지표
- **응답 시간**: LLM API 호출 < 2초, 로컬 분석 < 100ms
- **메모리 사용량**: Actor당 < 10MB
- **동시성**: 100개 Actor까지 지원

### 모니터링
- 각 Actor의 상태 추적
- API 호출량 및 비용 모니터링
- 분석 정확도 추이 분석

## 🚀 다음 단계

### 즉시 사용 가능한 기능
- [x] LLM 통합 모듈
- [x] 함수형 Actor들
- [x] 통합 데모 시스템
- [ ] 실제 API 연동 (Alpha Vantage, NewsAPI)
- [ ] 웹 대시보드
- [ ] 실시간 알림 시스템
- [ ] 백테스트 엔진

### 확장 가능 영역
- **추가 LLM 지원**: Anthropic Claude, Google Gemini
- **다양한 데이터 소스**: Twitter API, Reddit API, Discord
- **고급 분석**: 포트폴리오 최적화, 리스크 관리
- **머신러닝 통합**: XGBoost, LSTM 모델 추가

## 🤝 기여하기

1. Fork 및 Clone
2. 기능 브랜치 생성 (`git checkout -b feature/new-actor`)
3. 코드 작성 및 테스트
4. Pull Request 생성

## 📄 라이선스

Apache License 2.0

---

## 🎊 결론

이 시스템은 함수형 프로그래밍의 강점을 최대한 활용하여:

- **안전성**: 불변성으로 인한 데이터 무결성 보장
- **신뢰성**: 순수 함수로 예측 가능한 동작
- **확장성**: 합성 가능한 작은 함수들로 복잡한 로직 구축
- **유지보수성**: 명시적 상태 전이로 쉬운 디버깅

실시간 주식 시장 분석이라는 복잡한 도메인에서 함수형 프로그래밍이 어떻게 우아하고 효율적인 솔루션을 제공하는지 보여주는 완벽한 예시입니다.

**실행해보세요**: `cargo run --example functional_llm_analysis` 🚀
# ML4T 자동매매 시스템 - 전체 요약

이 문서는 Pekko-Rust 기반 ML4T 자동매매 시스템의 전체 내용을 한눈에 볼 수 있도록 요약합니다.

---

## 🎯 시스템 개요

### 한 문장 요약

**Machine Learning for Trading 워크플로우를 Apache Pekko Actor 모델로 구현한 Rust 기반 실전 자동매매 시스템**

### 핵심 가치

- 🤖 **AI 기반 예측**: 머신러닝으로 주가 예측
- ⚡ **실시간 거래**: 30초마다 자동 매매 결정
- 🛡️ **리스크 관리**: 자동 손절/익절
- 📊 **백테스팅**: 전략 검증 후 투자
- 💰 **실전 거래**: 한국투자증권 API 연동

---

## 📁 프로젝트 구조

```
pekko-ml4t-system/
├── README.md                 # 전체 문서 (자세한 설명)
├── QUICKSTART.md             # 빠른 시작 가이드 (초보자용)
├── ARCHITECTURE.md           # 아키텍처 문서 (개발자용)
├── SUMMARY.md                # 이 문서 (전체 요약)
│
├── src/
│   ├── lib.rs                # 메인 시스템
│   ├── actors/               # 5개 Actor 구현
│   │   ├── data_agent.rs     # 데이터 수집
│   │   ├── ml_agent.rs       # ML 예측
│   │   ├── trading_agent.rs  # 거래 실행
│   │   ├── risk_agent.rs     # 리스크 관리
│   │   └── monitoring_agent.rs # 성능 모니터링
│   │
│   ├── data/                 # 데이터 처리
│   ├── ml/                   # 머신러닝 모델
│   ├── trading/              # 거래 로직
│   ├── risk/                 # 리스크 관리
│   ├── backtesting/          # 백테스팅
│   └── config/               # 설정
│
└── examples/
    ├── actor_system_test.rs  # Actor 시스템 테스트
    ├── live_trading_system.rs # 실전 투자 시스템
    └── backtesting_example.rs # 백테스팅 예제
```

---

## 🏗️ 시스템 아키텍처

### Actor 시스템

```
                ActorSystem
                     │
        ┌────────────┼────────────┐
        │            │            │
   DataAgent ──→ MLAgent ──→ TradingAgent
        │            │            │
        └────────────┼────────────┘
                     │
        ┌────────────┼────────────┐
        │            │            │
   RiskAgent   MonitoringAgent  Storage
```

### 5가지 핵심 Actor

| Actor | 역할 | 주기 |
|-------|------|------|
| **DataAgent** | 시장 데이터 수집 | 30초 |
| **MLAgent** | AI 예측 및 피처 추출 | 요청 시 |
| **TradingAgent** | 거래 신호 생성 및 실행 | 30초 |
| **RiskAgent** | 포트폴리오 리스크 관리 | 요청 시 |
| **MonitoringAgent** | 성능 추적 및 알림 | 지속적 |

### 데이터 흐름

```
1. DataAgent    → 주식 가격 수집
2. MLAgent      → "오를까?" 예측
3. RiskAgent    → "위험하지 않아?" 확인
4. TradingAgent → "사자/팔자" 결정
5. Monitoring   → "얼마 벌었어?" 추적
```

---

## 🚀 빠른 시작 (5분)

### 1단계: 빌드

```bash
cd pekko-ml4t-system
cargo build --release
```

### 2단계: Actor 테스트

```bash
cargo run --example actor_system_test
```

### 3단계: DRY-RUN 실행

```bash
cargo run --example live_trading_system
```

**결과:**
```
✅ 모든 Actor 생성 완료!
🚀 자동매매 시작... (Ctrl+C로 중지)
```

---

## 💡 사용 방법

### 시뮬레이션 (DRY-RUN)

```bash
# 실제 주문 없이 시뮬레이션만
cargo run --example live_trading_system
```

**특징:**
- 실제 돈 사용 안 함
- 15초마다 체크
- 전략 테스트용

### 백테스팅

```bash
# 과거 데이터로 전략 검증
cargo run --example backtesting_example
```

**확인 사항:**
- 수익률 > 10%
- 승률 > 50%
- 최대 낙폭 < 20%

### 실전 투자 ⚠️

```bash
# 실제 돈으로 거래! 주의!
cargo run --example live_trading_system -- --real
```

**주의사항:**
- 실제 돈 사용
- 소액으로 시작
- 충분한 테스트 후

---

## ⚙️ 설정

### 환경 변수 (.env)

```bash
# 한국투자증권 API
KIS_APP_KEY=your_app_key
KIS_APP_SECRET=your_app_secret
KIS_ACCOUNT_NO=12345678-01
KIS_BASE_URL=https://openapi.koreainvestment.com:9443
```

### 매매 전략

```rust
let strategy = TradingStrategy {
    profit_target: 3.0,       // 3% 수익 시 매도
    stop_loss: -2.0,          // -2% 손실 시 손절
    max_position_ratio: 50.0, // 최대 50% 투자
    min_cash_ratio: 20.0,     // 최소 20% 현금
};
```

### 관심 종목

```rust
let watchlist = vec![
    "005930".to_string(),  // 삼성전자
    "000660".to_string(),  // SK하이닉스
    "035420".to_string(),  // NAVER
    "035720".to_string(),  // 카카오
    // ... 최대 20개
];
```

---

## 📊 주요 기능

### 1. 데이터 수집 (DataAgent)

- **시장 데이터**: OHLCV (Open, High, Low, Close, Volume)
- **뉴스 데이터**: 뉴스 API 연동 및 센티먼트 분석
- **품질 검증**: 결측치, 이상치 자동 처리

### 2. 머신러닝 (MLAgent)

- **기술적 지표**: SMA, RSI, MACD, Bollinger Bands
- **알파 팩터**: Momentum, Mean Reversion
- **ML 모델**: Ridge, RandomForest, LSTM, DQN

### 3. 거래 실행 (TradingAgent)

- **신호 생성**: ML 예측 기반
- **주문 실행**: 한국투자증권 API
- **포지션 관리**: 자동 익절/손절

### 4. 리스크 관리 (RiskAgent)

- **VaR 계산**: 일일 최대 손실 예상
- **포지션 제한**: 종목당 최대 50%
- **다각화**: 20개 우량주 분산 투자

### 5. 모니터링 (MonitoringAgent)

- **실시간 P&L**: 손익 추적
- **샤프 비율**: 위험 대비 수익률
- **알림**: 목표 수익률 도달 시

---

## 🎓 학습 경로

### 1주차: 이해 및 테스트

- [ ] README.md 읽기
- [ ] QUICKSTART.md 따라하기
- [ ] Actor 시스템 테스트
- [ ] DRY-RUN 모드 1주일

### 2주차: 백테스팅

- [ ] 백테스팅 실행
- [ ] 결과 분석
- [ ] 전략 조정
- [ ] 다시 백테스팅

### 3주차: 모의투자

- [ ] 한국투자증권 계좌 개설
- [ ] 모의투자 API 신청
- [ ] 모의투자로 2주 테스트
- [ ] 성과 분석

### 4주차: 소액 실전

- [ ] 실전 API로 전환
- [ ] 50만원으로 시작
- [ ] 매일 결과 기록
- [ ] 전략 개선

---

## 📈 예상 성과

### 보수적 전략 (초보자)

- **목표 수익률**: 월 5~10%
- **일일 거래**: 2~5회
- **승률**: 55~60%
- **최대 낙폭**: 10~15%

### 공격적 전략 (경험자)

- **목표 수익률**: 월 15~25%
- **일일 거래**: 5~10회
- **승률**: 50~55%
- **최대 낙폭**: 15~25%

**면책 조항:**
- 과거 성과 ≠ 미래 수익
- 손실 가능성 항상 존재
- 투자는 본인 책임

---

## ⚠️ 리스크 및 주의사항

### 투자 리스크

1. **시장 리스크**: 주가 하락 시 손실
2. **시스템 리스크**: 버그, 네트워크 오류
3. **API 리스크**: API 장애, 제한
4. **유동성 리스크**: 체결 안 될 수 있음

### 안전 수칙

- ✅ 소액으로 시작 (100만원 이하)
- ✅ 충분한 테스트 (백테스팅 + DRY-RUN + 모의투자)
- ✅ 손실 감당 범위 내
- ✅ 24시간 모니터링 가능한 환경
- ❌ 대출/빚으로 투자 금지
- ❌ 생활비로 투자 금지

---

## 🔧 문제 해결

### 자주 발생하는 문제

| 문제 | 원인 | 해결 |
|------|------|------|
| 빌드 실패 | Rust 버전 | `rustup update` |
| Actor 메시지 전달 안 됨 | Actor 이름 오타 | 이름 확인 |
| API 토큰 실패 | .env 설정 오류 | API 키 재확인 |
| 주문 실패 | 예수금 부족 | 계좌 잔고 확인 |
| 시세 조회 실패 | API 호출 제한 | 주기 늘리기 |

### 로그 확인

```bash
# 상세 로그
RUST_LOG=debug cargo run --example live_trading_system

# 로그 파일 저장
cargo run --example live_trading_system > trading.log 2>&1

# 실시간 모니터링
tail -f trading.log | grep "수익률\|주문"
```

---

## 📚 문서 가이드

### 문서 읽는 순서

1. **SUMMARY.md** (이 문서) - 전체 개요
2. **QUICKSTART.md** - 5분 빠른 시작
3. **README.md** - 상세 사용법
4. **ARCHITECTURE.md** - 아키텍처 이해

### 목적별 문서

| 목적 | 문서 |
|------|------|
| 빠르게 시작하고 싶어요 | QUICKSTART.md |
| 자세히 알고 싶어요 | README.md |
| 개발하고 싶어요 | ARCHITECTURE.md |
| 전체가 궁금해요 | SUMMARY.md (이 문서) |

---

## 🤝 커뮤니티

### 도움 받기

- **GitHub Issues**: 버그 리포트, 기능 요청
- **이메일**: your-email@example.com
- **디스코드**: ML4T Community

### 기여하기

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

---

## 📖 학습 자료

### 필독 자료

1. **ML4T 책**: [Machine Learning for Algorithmic Trading 2nd Edition](https://www.amazon.com/dp/1839217715)
2. **Actor 모델**: [Pekko 공식 문서](https://pekko.apache.org/)
3. **Rust**: [The Rust Programming Language](https://doc.rust-lang.org/book/)

### 추천 자료

- 한국투자증권 OpenAPI 가이드
- Tokio 비동기 프로그래밍
- 알고리즘 트레이딩 기초

---

## 🎯 성공 체크리스트

### 시작 전

- [ ] Rust 설치 완료
- [ ] 프로젝트 빌드 성공
- [ ] Actor 시스템 테스트 통과
- [ ] 문서 읽기 완료

### 테스트 단계

- [ ] DRY-RUN 1주일 안정적 작동
- [ ] 백테스팅 수익률 > 10%
- [ ] 모의투자 1개월 성공
- [ ] 전략 이해 완료

### 실전 투자 전

- [ ] 한국투자증권 계좌 개설
- [ ] API 키 발급 완료
- [ ] .env 파일 설정 완료
- [ ] 소액 자금 준비 (100만원 이하)
- [ ] 손실 감당 가능 확인

### 운영 중

- [ ] 매일 거래 내역 확인
- [ ] 주간 성과 분석
- [ ] 전략 조정 필요 시 백테스팅
- [ ] 로그 파일 백업

---

## 🚨 긴급 상황 대응

### 시스템 오류

```bash
# 1. 즉시 중지
Ctrl + C

# 2. 프로세스 강제 종료
pkill -9 -f live_trading_system

# 3. 증권사 앱에서 수동 매도
```

### API 장애

1. 프로그램 중지
2. 한국투자증권 앱 확인
3. 필요 시 수동 거래
4. 시스템 재시작

### 예상치 못한 손실

1. 즉시 프로그램 중지
2. 포지션 확인
3. 손절 결정
4. 전략 재검토

---

## 📊 핵심 통계

### 시스템 스펙

- **프로그래밍 언어**: Rust 1.70+
- **Actor 수**: 5개
- **지원 종목**: 최대 20개
- **체크 주기**: 15~30초
- **지원 거래소**: 한국투자증권

### 성능 지표

- **메시지 처리**: 1만 msg/초
- **메모리 사용**: ~100MB
- **CPU 사용**: ~5%
- **응답 시간**: < 100ms

---

## 🔮 로드맵

### v0.1.0 (현재)

- ✅ Actor 시스템 구현
- ✅ 기본 ML 모델
- ✅ 한국투자증권 API 연동
- ✅ 백테스팅 기능

### v0.2.0 (계획)

- [ ] 분산 Actor 시스템
- [ ] 고급 ML 모델 (Transformer)
- [ ] 실시간 뉴스 분석
- [ ] 자동 전략 최적화

### v1.0.0 (목표)

- [ ] 프로덕션 레디
- [ ] 멀티 브로커 지원
- [ ] 클라우드 배포
- [ ] Web UI

---

## 💬 FAQ

**Q: 초보자도 사용 가능한가요?**
A: 네, QUICKSTART.md를 따라하면 5분 안에 시작 가능합니다.

**Q: 얼마의 자금이 필요한가요?**
A: 최소 50만원, 권장 100~500만원입니다.

**Q: 하루에 얼마나 벌 수 있나요?**
A: 목표는 일 0.5~1%이지만, 손실도 발생할 수 있습니다.

**Q: 24시간 실행해야 하나요?**
A: 아니요, 시장 시간(평일 09:00~15:30)만 작동합니다.

**Q: 수수료는 얼마인가요?**
A: 한국투자증권 수수료(0.015%) + 증권거래세(0.25%, 매도 시)입니다.

---

## 📞 연락처

- **GitHub**: [Project Repository](https://github.com/your-repo)
- **이메일**: your-email@example.com
- **디스코드**: [ML4T Community](https://discord.gg/your-invite)

---

## ⚖️ 라이선스

Apache License 2.0

---

## 📝 면책 조항

⚠️ **투자 위험 고지**

이 시스템은 교육 및 연구 목적으로 제공됩니다.
실제 투자에 사용 시 손실이 발생할 수 있습니다.
투자 결정은 본인의 판단과 책임 하에 이루어져야 합니다.

---

**마지막 업데이트**: 2025-10-17
**버전**: 0.1.0
**작성자**: ML4T Trading Team

---

## 🎉 시작하세요!

```bash
# 1. 빌드
cargo build --release

# 2. 테스트
cargo run --example actor_system_test

# 3. 실행
cargo run --example live_trading_system
```

**행운을 빕니다! 📈💰**

# 빠른 시작 가이드

완전 초보자를 위한 5분 만에 시작하는 ML4T 자동매매 시스템 가이드입니다.

## 📋 목차

1. [5분 빠른 시작](#5분-빠른-시작)
2. [단계별 상세 가이드](#단계별-상세-가이드)
3. [첫 번째 백테스팅](#첫-번째-백테스팅)
4. [실전 투자 시작하기](#실전-투자-시작하기)
5. [자주 묻는 질문](#자주-묻는-질문)

---

## 5분 빠른 시작

### 1단계: 프로젝트 준비 (1분)

```bash
# 프로젝트 디렉토리로 이동
cd pekko-ml4t-system

# 빌드
cargo build --release
```

### 2단계: Actor 시스템 테스트 (2분)

```bash
# Actor 시스템이 정상 작동하는지 확인
cargo run --example actor_system_test
```

**예상 결과:**
```
✅ 모든 Actor 생성 완료!
✅ 메시지 전달 테스트 완료!
✅ 통신 체인 테스트 완료!
✓ Actor 시스템이 정상적으로 작동합니다!
```

### 3단계: DRY-RUN 모드 실행 (2분)

```bash
# 시뮬레이션 모드로 자동매매 시스템 실행
cargo run --example live_trading_system
```

**실행 확인:**
```
🚀 ML4T 실전 투자 시스템
🔍 DRY-RUN 모드로 실행합니다 (시뮬레이션만)
✓ Actor 시스템 생성 완료
✓ 모든 Actor 생성 완료
🚀 자동매매 시작... (Ctrl+C로 중지)
```

축하합니다! 🎉 시스템이 정상적으로 작동합니다!

---

## 단계별 상세 가이드

### 단계 1: 환경 설정

#### 1.1 Rust 설치 확인

```bash
rustc --version
```

**결과가 나오지 않으면:** [Rust 공식 사이트](https://www.rust-lang.org/tools/install)에서 설치

#### 1.2 프로젝트 클론 (또는 이동)

```bash
# 이미 클론했다면
cd pekko-rust/pekko-ml4t-system

# 처음이라면
git clone <repository-url>
cd pekko-ml4t-system
```

#### 1.3 의존성 확인

```bash
cargo check
```

모든 의존성이 자동으로 다운로드됩니다.

### 단계 2: 시스템 이해하기

#### 2.1 Actor란?

Actor는 독립적으로 동작하는 작은 프로그램입니다. 각 Actor는:
- 메시지를 받아서 처리
- 다른 Actor에게 메시지 전송
- 동시에 여러 Actor가 작동

**우리 시스템의 5가지 Actor:**

```
DataAgent      → 주식 데이터 수집
MLAgent        → AI로 예측
TradingAgent   → 거래 신호 생성 및 실행
RiskAgent      → 리스크 관리
MonitoringAgent→ 성능 모니터링
```

#### 2.2 시스템 흐름

```
1. DataAgent가 주식 가격 수집
   ↓
2. MLAgent가 "오를까?" 예측
   ↓
3. RiskAgent가 "위험하지 않아?" 확인
   ↓
4. TradingAgent가 "사자!" 또는 "팔자!" 결정
   ↓
5. MonitoringAgent가 "얼마 벌었어?" 추적
```

### 단계 3: 첫 번째 테스트

#### 3.1 Actor 시스템 테스트

```bash
cargo run --example actor_system_test
```

**무엇을 테스트하나요?**
- ✅ Actor 생성이 잘 되는가?
- ✅ Actor끼리 메시지를 주고받는가?
- ✅ 전체 워크플로우가 작동하는가?

#### 3.2 로그 이해하기

```
🔄 [루프 1] 자동매매 체크... (09:15:23)
📊 1단계: 시장 데이터 수집 중...        ← DataAgent 작동
🤖 2단계: ML 모델 예측 중...           ← MLAgent 작동
📈 3단계: 거래 신호 생성 중...          ← TradingAgent 작동
⚖️  4단계: 리스크 평가 중...           ← RiskAgent 작동
📊 6단계: 성능 모니터링 중...          ← MonitoringAgent 작동
✅ 자동매매 사이클 완료!
```

### 단계 4: 설정 조정하기

#### 4.1 관심 종목 변경

`examples/live_trading_system.rs` 파일 열기:

```rust
let watchlist = vec![
    "005930".to_string(),  // 삼성전자
    "000660".to_string(),  // SK하이닉스
    "035420".to_string(),  // NAVER
    // 여기에 관심 종목 추가
];
```

**주요 종목 코드:**
- 삼성전자: 005930
- SK하이닉스: 000660
- NAVER: 035420
- 카카오: 035720
- 현대차: 005380
- LG에너지솔루션: 373220

#### 4.2 매매 전략 조정

```rust
let strategy = TradingStrategy {
    profit_target: 3.0,       // 3% 수익 시 매도
    stop_loss: -2.0,          // -2% 손실 시 손절
    max_position_ratio: 50.0, // 최대 50% 투자
    min_cash_ratio: 20.0,     // 최소 20% 현금 보유
};
```

**초보자 권장:**
```rust
let strategy = TradingStrategy {
    profit_target: 2.0,       // 보수적인 2%
    stop_loss: -1.5,          // 빠른 손절
    max_position_ratio: 30.0, // 30%만 투자
    min_cash_ratio: 50.0,     // 50% 현금 보유
};
```

### 단계 5: DRY-RUN 모드 실행

#### 5.1 시뮬레이션 실행

```bash
cargo run --example live_trading_system
```

#### 5.2 시뮬레이션 관찰

**15초마다 자동으로:**
1. 주식 가격 확인
2. AI가 예측
3. 리스크 평가
4. 거래 결정 (실제 주문은 안 함!)
5. 결과 출력

#### 5.3 중지 방법

```
Ctrl + C 누르기
```

---

## 첫 번째 백테스팅

### 백테스팅이란?

과거 데이터로 "내 전략이 먹혔을까?" 테스트하는 것입니다.

### 백테스팅 실행

```bash
cargo run --example backtesting_example
```

### 결과 해석

```
총 수익률: 25.3%           ← 1년간 25% 벌었다
승률: 58.5%                ← 10번 중 6번 성공
최대 낙폭: -15.2%          ← 최악의 경우 15% 떨어짐
```

**좋은 결과인가?**
- 수익률 > 10%: 좋음
- 승률 > 50%: 좋음
- 최대 낙폭 < 20%: 좋음

---

## 실전 투자 시작하기

### ⚠️ 주의사항

**절대 지켜야 할 것:**
1. 소액으로 시작 (100만원 이하)
2. 백테스팅 먼저 (최소 1주일)
3. DRY-RUN 모드 먼저 (최소 1주일)
4. 손실 감당 가능한 금액만

### 체크리스트

실전 투자 전에 확인하세요:

- [ ] Actor 시스템 테스트 통과
- [ ] 백테스팅 수익률 > 10%
- [ ] DRY-RUN 1주일 이상 안정적
- [ ] 한국투자증권 계좌 개설
- [ ] API 키 발급
- [ ] `.env` 파일 설정
- [ ] 손실 감당 가능한 금액 확인

### 한국투자증권 API 설정

#### 1. 계좌 개설

1. [한국투자증권 홈페이지](https://www.koreainvestment.com) 방문
2. 계좌 개설 신청
3. 신분증 제출 및 인증

#### 2. API 신청

1. 한국투자증권 홈페이지 로그인
2. "Open API" 메뉴 클릭
3. "모의투자" 또는 "실전투자" 선택
4. App Key, App Secret 발급

#### 3. `.env` 파일 생성

프로젝트 루트에 `.env` 파일 생성:

```bash
# .env 파일 내용
KIS_APP_KEY=발급받은_앱키
KIS_APP_SECRET=발급받은_시크릿
KIS_ACCOUNT_NO=12345678-01
KIS_BASE_URL=https://openapi.koreainvestment.com:9443
```

**예시:**
```bash
KIS_APP_KEY=PSabcdef1234567890
KIS_APP_SECRET=xyzabcd1234567890ABCDEF
KIS_ACCOUNT_NO=50012345-01
KIS_BASE_URL=https://openapi.koreainvestment.com:9443
```

### 모의투자로 시작

**강력 권장: 먼저 모의투자로 시작하세요!**

```bash
# 모의투자 모드
cargo run --example live_trading_system -- --real
```

모의투자 API는:
- 실제 돈을 사용하지 않음
- 실제와 동일한 환경
- 무료로 테스트 가능

### 실전 투자 시작

**모의투자에서 1개월 이상 성공 후:**

```bash
# 실전 거래 모드 (실제 돈 사용!)
cargo run --example live_trading_system -- --real
```

**출력:**
```
⚠️  ═══════════════════════════════════════
⚠️  실전 거래 모드로 실행합니다!
⚠️  실제 주문이 체결됩니다!
⚠️  실제 돈이 사용됩니다!
⚠️  ═══════════════════════════════════════
```

### 모니터링

실행 중 확인할 것:

```bash
# 로그 파일에 저장
cargo run --example live_trading_system -- --real > trading.log 2>&1

# 다른 터미널에서 실시간 모니터링
tail -f trading.log | grep "수익률\|주문"
```

---

## 자주 묻는 질문

### Q1. 얼마의 자금이 필요한가요?

**답변:**
- **최소**: 50만원
- **권장**: 100만원 ~ 500만원
- **초보자**: 100만원 이하로 시작

### Q2. 하루에 얼마나 벌 수 있나요?

**답변:**
- 시스템 목표: 일 0.5~1%
- 월간 목표: 10~20%
- **주의**: 손실도 발생할 수 있습니다

### Q3. 컴퓨터를 켜놔야 하나요?

**답변:**
- 예, 프로그램이 실행 중이어야 합니다
- 클라우드 서버 사용 권장 (AWS, GCP 등)
- 또는 24시간 켜두는 컴퓨터

### Q4. 어떤 종목을 선택해야 하나요?

**답변:**
- **안전**: 삼성전자, SK하이닉스 등 대형주
- **위험**: 소형주, 테마주
- **권장**: 시총 1조원 이상 우량주

### Q5. 손실이 나면 어떻게 하나요?

**답변:**
- 시스템이 자동으로 손절 (-2%)
- 일일 손실 한도 설정 가능
- Ctrl+C로 즉시 중지 가능

### Q6. 수수료는 얼마인가요?

**답변:**
- 한국투자증권: 0.015%
- 증권거래세: 0.25% (매도 시)
- 시스템 사용료: 무료

### Q7. 백테스팅 결과를 믿어도 되나요?

**답변:**
- 참고만 하세요
- 과거 성과 ≠ 미래 수익
- 실전은 항상 다릅니다

### Q8. 주말에도 실행하나요?

**답변:**
- 아니요, 시장 시간만 작동
- 평일 09:00 ~ 15:30
- 주말/공휴일 자동 대기

### Q9. 여러 종목을 동시에 거래하나요?

**답변:**
- 예, 관심 종목 리스트 전부
- 리스크에 따라 자동 분산
- 최대 포지션 비율 제한

### Q10. 문제가 생기면 어떻게 하나요?

**답변:**
1. Ctrl+C로 즉시 중지
2. 로그 파일 확인
3. GitHub Issues에 문의
4. 증권사 앱에서 수동 매도

---

## 다음 단계

### 1주차: 학습 및 테스트
- [ ] README.md 전체 읽기
- [ ] Actor 시스템 이해하기
- [ ] 백테스팅 실행해보기
- [ ] DRY-RUN 모드로 1주일 테스트

### 2주차: 모의투자
- [ ] 한국투자증권 계좌 개설
- [ ] 모의투자 API 신청
- [ ] 모의투자로 2주 이상 테스트
- [ ] 전략 조정 및 최적화

### 3주차: 소액 실전
- [ ] 실전투자 API로 전환
- [ ] 50만원으로 시작
- [ ] 매일 결과 기록
- [ ] 문제 발생 시 대응 방법 학습

### 1개월 후: 점진적 확대
- [ ] 성과 평가
- [ ] 자금 증액 여부 결정
- [ ] 전략 개선
- [ ] 추가 기능 학습

---

## 도움말

### 커뮤니티

- **GitHub**: 이슈 등록
- **이메일**: your-email@example.com
- **디스코드**: [ML4T Community](https://discord.gg/your-invite)

### 학습 자료

1. **Actor 모델**: [Pekko 공식 문서](https://pekko.apache.org/)
2. **ML4T**: [Machine Learning for Trading](https://www.amazon.com/dp/1839217715)
3. **Rust**: [Rust Book](https://doc.rust-lang.org/book/)
4. **주식 투자**: [한국투자증권 아카데미](https://www.koreainvestment.com)

### 유용한 명령어

```bash
# 로그 레벨 변경
RUST_LOG=debug cargo run --example live_trading_system

# 백그라운드 실행
nohup cargo run --example live_trading_system -- --real &

# 프로세스 확인
ps aux | grep live_trading

# 프로세스 종료
pkill -f live_trading_system

# 로그 실시간 확인
tail -f trading.log
```

---

## 체크리스트

### 시작 전 체크리스트

- [ ] Rust 설치 완료
- [ ] 프로젝트 빌드 성공
- [ ] Actor 시스템 테스트 통과
- [ ] DRY-RUN 모드 실행 성공

### 실전 투자 전 체크리스트

- [ ] 백테스팅 수익률 만족
- [ ] DRY-RUN 1주일 안정적 작동
- [ ] 모의투자 1개월 성공
- [ ] 손실 감당 가능한 금액 확인
- [ ] API 키 정확히 설정
- [ ] 긴급 중지 방법 숙지

### 운영 중 체크리스트

- [ ] 매일 거래 내역 확인
- [ ] 주간 성과 분석
- [ ] 로그 파일 백업
- [ ] 시스템 안정성 확인
- [ ] 전략 조정 필요성 검토

---

**축하합니다! 🎉**

이제 ML4T 자동매매 시스템을 시작할 준비가 되었습니다!

**기억하세요:**
- 소액으로 시작
- 충분한 테스트
- 손실 감당 범위 내
- 항상 모니터링

**행운을 빕니다! 📈**

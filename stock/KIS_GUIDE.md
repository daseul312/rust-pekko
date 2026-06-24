# 🏦 한국투자증권 자동매매 시스템 가이드

## 📋 목차
1. [시스템 개요](#시스템-개요)
2. [계정 개설 및 API 신청](#계정-개설-및-api-신청)
3. [환경 설정](#환경-설정)
4. [API 연결 테스트](#api-연결-테스트)
5. [자동매매 실행](#자동매매-실행)
6. [매매 전략 설정](#매매-전략-설정)

---

## 🎯 시스템 개요

한국투자증권 OpenAPI를 활용한 실전 자동매매 시스템입니다.

**주요 기능:**
- ✅ 실시간 주식 시세 조회
- ✅ 자동 매수/매도 주문
- ✅ 계좌 잔고 조회
- ✅ 포지션 관리 및 수익률 추적
- ✅ DRY-RUN 모드 (시뮬레이션)
- ✅ 목표 수익률/손절매 자동 실행

---

## 🏦 계정 개설 및 API 신청

### 1단계: 한국투자증권 계좌 개설

1. **온라인 계좌 개설**
   - https://www.koreainvestment.com 방문
   - "계좌개설" 클릭
   - 필요 서류: 신분증, 은행 계좌
   - 소요 시간: 1~2일

2. **모의투자 계좌 (테스트용)**
   - https://apiportal.koreainvestment.com
   - "모의투자" 메뉴에서 즉시 발급

### 2단계: API 키 발급

1. **OpenAPI 포털 접속**
   - https://apiportal.koreainvestment.com
   - 회원가입 및 로그인

2. **API 키 발급**
   - "APP KEY 발급" 신청
   - "APP SECRET" 발급
   - 발급된 키 저장 (절대 공개하지 마세요!)

---

## ⚙️ 환경 설정

### .env 파일 생성

프로젝트 루트의 `stock/` 디렉토리에 `.env` 파일을 생성하세요:

```bash
# 실전투자용
KIS_APP_KEY=발급받은_APP_KEY
KIS_APP_SECRET=발급받은_APP_SECRET
KIS_ACCOUNT_NO=계좌번호-상품코드
KIS_BASE_URL=https://openapi.koreainvestment.com:9443

# 모의투자용
# KIS_APP_KEY=모의투자_APP_KEY
# KIS_APP_SECRET=모의투자_APP_SECRET
# KIS_ACCOUNT_NO=모의계좌번호-01
# KIS_BASE_URL=https://openapivts.koreainvestment.com:29443
```

**계좌번호 형식 주의!**
- 올바른 형식: `12345678-01` (계좌번호-상품코드)
- 상품코드는 보통 `01`입니다

---

## 🧪 API 연결 테스트

### 1. 기본 연결 테스트

```bash
cargo run --bin kis-test -p stock-recommender
```

**테스트 항목:**
- ✅ OAuth 토큰 발급
- ✅ 계좌 잔고 조회

### 2. 시세 조회 테스트

```bash
cargo run --bin kis-price-test -p stock-recommender
```

**조회 종목:**
- 삼성전자 (005930)
- SK하이닉스 (000660)
- 카카오 (035720)
- NAVER (035420)
- LG화학 (051910)
- 삼성SDI (006400)

---

## 🤖 자동매매 실행

### DRY-RUN 모드 (안전한 시뮬레이션)

```bash
cargo run --bin kis-auto-trading -p stock-recommender
```

**특징:**
- 🔍 실제 주문 없음 (시뮬레이션만)
- 🔍 5번 루프 후 자동 종료
- 🔍 5초마다 체크 (빠른 테스트)

### 실전 거래 모드 (주의!)

```bash
cargo run --bin kis-auto-trading -p stock-recommender -- --real
```

**⚠️  경고:**
- 💰 실제 주문이 체결됩니다!
- 💰 실제 돈이 사용됩니다!
- 💰 신중하게 사용하세요!

---

## 📊 매매 전략 설정

### 기본 전략 (보수적)

```rust
TradingStrategy {
    profit_target: 3.0,       // 3% 수익 시 자동 매도
    stop_loss: -2.0,          // -2% 손실 시 자동 손절
    max_position_ratio: 20.0, // 최대 20% 포지션
    min_cash_ratio: 30.0,     // 최소 30% 현금 보유
}
```

### 전략 커스터마이징

`kis_auto_trading.rs` 파일에서 전략을 수정하세요:

```rust
// 공격적 전략
let strategy = TradingStrategy {
    profit_target: 5.0,       // 5% 수익 시 매도
    stop_loss: -3.0,          // -3% 손실 시 손절
    max_position_ratio: 30.0, // 최대 30% 포지션
    min_cash_ratio: 20.0,     // 최소 20% 현금 보유
};

// 매우 보수적 전략
let strategy = TradingStrategy {
    profit_target: 2.0,       // 2% 수익 시 매도
    stop_loss: -1.5,          // -1.5% 손실 시 손절
    max_position_ratio: 15.0, // 최대 15% 포지션
    min_cash_ratio: 40.0,     // 최소 40% 현금 보유
};
```

---

## 🎯 매매 로직

### 자동 매수 조건
1. 현금 비율 > (최소 현금 비율 + 10%)
2. 아직 보유하지 않은 종목
3. 관심 종목 리스트에 포함

### 자동 매도 조건
1. **수익 실현**: 수익률 >= 목표 수익률
2. **손절매**: 수익률 <= 손절매 기준

### 포지션 관리
- 종목당 최대 포지션: 총 자산의 20%
- 매수 시 안전 마진: 계산된 금액의 90%만 사용

---

## 📱 실행 결과 예시

```
🚀 한국투자증권 자동매매 시스템

🔍 DRY-RUN 모드로 실행합니다 (시뮬레이션만)

========================================
🤖 자동매매 시스템 시작
🔍 모드: DRY-RUN (시뮬레이션만, 실제 주문 안 함)
========================================

📋 매매 전략:
  - 목표 수익률: 3.0%
  - 손절매: -2.0%
  - 최대 포지션: 20.0%
  - 최소 현금: 30.0%

📈 관심 종목: ["005930", "000660", "035720", "035420"]

💵 초기 현금: 400001원

========================================
자동매매 시작... (Ctrl+C로 중지)
========================================

🔄 [루프 1/5] 자동매매 체크...

✅ 시세 조회 성공: 005930 현재가=89000원
📊 매수 신호: 005930 @89000원 (현금 비율: 100.0%)

✅ 시세 조회 성공: 035720 현재가=59600원
📊 매수 신호: 035720 @59600원 (현금 비율: 100.0%)

💰 매수 실행: 035720 1주 @59600원 (총 59600원)
🔍 [DRY-RUN] 매수 주문 시뮬레이션만 수행 (실제 주문 안 함)

💰 현재 현금: 340401원
📊 보유 종목: 1개
  - 035720: 1주 @59600원 (수익률: 0.0%)

...

========================================
📊 자동매매 최종 결과
========================================
💰 최종 현금: 340401원
📊 보유 종목: 1개
💵 포지션 가치: 59600원
💎 총 자산: 400001원
📈 수익률: 0.00%
```

---

## 🛡️ 리스크 관리

### 안전 장치
1. **DRY-RUN 모드**: 기본으로 시뮬레이션만 실행
2. **포지션 제한**: 종목당 최대 비율 제한
3. **현금 보유**: 최소 현금 비율 유지
4. **손절매**: 자동 손실 제한

### 권장 사항
1. 처음에는 반드시 **DRY-RUN 모드**로 테스트
2. 소액으로 시작 (10만원~50만원)
3. 보수적인 전략 사용 (목표 2-3%, 손절 -1.5~-2%)
4. 로그를 확인하며 운영

---

## 🔧 커스터마이징

### 관심 종목 변경

`kis_auto_trading.rs`에서 `watchlist`를 수정:

```rust
watchlist: vec![
    "005930".to_string(),  // 삼성전자
    "000660".to_string(),  // SK하이닉스
    "035720".to_string(),  // 카카오
    "035420".to_string(),  // NAVER
    "005380".to_string(),  // 현대차
    "051910".to_string(),  // LG화학
],
```

### 체크 주기 변경

```rust
// kis_auto_trading.rs의 run() 메서드에서
let wait_seconds = if self.dry_run { 5 } else { 60 };  // 60초로 변경
```

---

## 🐛 트러블슈팅

### 문제: "토큰 발급 실패"
**해결**: APP_KEY와 APP_SECRET을 다시 확인하세요.

### 문제: "계좌번호 형식 오류"
**해결**: 계좌번호는 `12345678-01` 형식이어야 합니다.

### 문제: "시세 조회 실패"
**해결**: 
- 장 운영 시간(09:00-15:30)에 실행하세요
- 종목 코드가 올바른지 확인하세요

### 문제: "매수 수량이 0"
**해결**: 
- 초기 자금을 늘리세요 (계좌에 입금)
- 저가 종목을 관심 종목에 추가하세요

---

## 📚 API 레퍼런스

### 주요 함수

```rust
// API 클라이언트 생성
let mut api = KoreaInvestmentAPI::from_env()?;

// 토큰 발급
let token = api.get_access_token().await?;

// 주식 시세 조회
let price = api.get_stock_price("005930").await?;

// 매수 주문
let order = api.buy_stock("005930", 10, 89000).await?;

// 매도 주문
let order = api.sell_stock("005930", 10, 90000).await?;
```

---

## 🚀 실행 명령어 요약

```bash
# 1. API 연결 테스트
cargo run --bin kis-test -p stock-recommender

# 2. 시세 조회 테스트
cargo run --bin kis-price-test -p stock-recommender

# 3. 자동매매 (DRY-RUN)
cargo run --bin kis-auto-trading -p stock-recommender

# 4. 자동매매 (실전 - 주의!)
cargo run --bin kis-auto-trading -p stock-recommender -- --real
```

---

## ⚠️  주의사항

1. **절대 공개하지 마세요:**
   - APP_KEY
   - APP_SECRET
   - 계좌번호

2. **실전 거래 전 반드시:**
   - DRY-RUN 모드로 충분히 테스트
   - 소액으로 시작
   - 로그 모니터링

3. **자동매매 리스크:**
   - 시스템 오류 가능성
   - 네트워크 지연
   - 급격한 시장 변동
   - 본인 책임 하에 사용

---

## 📞 문의 및 지원

- 한국투자증권 고객센터: 1544-5000
- OpenAPI 포털: https://apiportal.koreainvestment.com
- API 문서: https://apiportal.koreainvestment.com/apiservice

---

## 📄 라이선스

Apache License 2.0

**면책 조항**: 이 시스템은 교육 및 연구 목적으로 제공됩니다. 실제 거래로 인한 손실에 대해 개발자는 책임지지 않습니다.


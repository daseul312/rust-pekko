# 🎯 매매 파라미터 설정 가이드

## 📊 현재 설정 (2025-10-28 업데이트)

### ✅ **매수 가능 금액 계산 개선**

과도하게 보수적인 설정으로 인해 실제로는 주문을 못하는 문제를 해결했습니다.

#### **이전 설정 (매우 보수적)**
```rust
// 안전 비율
safety_ratio = 0.50 (50%만 사용)  // API 조회 없을 때
safety_ratio = 0.95 (95%만 사용)  // API 조회 있을 때

// 최종 검증
required_cash = total_with_fee * 1.20 (20% 안전 마진)
```

#### **현재 설정 (적절한 수준)**
```rust
// 안전 비율 (완화됨)
safety_ratio = 0.80 (80%만 사용)  // API 조회 없을 때
safety_ratio = 0.98 (98%만 사용)  // API 조회 있을 때

// 최종 검증 (완화됨)
required_cash = total_with_fee * 1.10 (10% 안전 마진)
```

---

## 💰 매수 가능 금액 계산 로직

### 1️⃣ **전제 조건**

```rust
total_asset = total_cash + get_total_position_value()
max_buy_amount = total_asset * (max_position_ratio / 100.0)
available_cash = total_cash
```

### 2️⃣ **매수 한도 결정**

```rust
// 기본 한도: 포지션 제한과 보유 현금 중 작은 값
buy_limit = min(max_buy_amount, available_cash)

// 증권사 API 조회가 성공하면 그것을 우선 사용
if buyable_from_api.is_some() {
    buy_limit = min(buy_limit, buyable_from_api)
}
```

### 3️⃣ **안전 비율 적용**

```rust
safe_buy_amount = buy_limit * safety_ratio
```

**안전 비율 의미:**
- **80% (API 없음)**: 증권사 조회 실패 시 보수적으로 80%만 사용
- **98% (API 성공)**: 정확한 금액 조회 시 거의 전체 사용

### 4️⃣ **수량 계산**

```rust
fee_rate = 0.0005  // 수수료 + 기타 비용 0.05%
total_cost_per_share = price * (1.0 + fee_rate)
quantity = (safe_buy_amount / total_cost_per_share) as u32
```

### 5️⃣ **최종 검증**

```rust
total_cost = price * quantity
fee = total_cost * 0.00015  // 0.015% 수수료
total_with_fee = total_cost + fee
required_cash = total_with_fee * 1.10  // 10% 안전 마진

if required_cash > available_cash {
    return Err("보유 현금 부족")
}
```

---

## 🎛️ 주요 파라미터

### **매매 전략 설정**

```rust
pub struct TradingStrategy {
    pub profit_target: 3.0,       // 목표 수익률: 3%
    pub stop_loss: -2.0,          // 손절매: -2%
    pub max_position_ratio: 50.0, // 최대 포지션: 50%
    pub min_cash_ratio: 20.0,     // 최소 현금: 20%
}
```

### **안전 마진 설정**

| 항목 | 비율 | 용도 |
|------|------|------|
| **Safety Ratio (API 없음)** | 80% | 증권사 조회 실패 시 보수적 설정 |
| **Safety Ratio (API 성공)** | 98% | 정확한 금액 조회 시 거의 전체 사용 |
| **Final Safety Margin** | 10% | 최종 검증 시 추가 안전 마진 |
| **Fee Rate** | 0.05% | 수수료 및 기타 비용 여유 |

---

## 📈 효과

### **이전 설정**
```
예수금: 820,000원
매수 가능: 820,000 * 0.50 * 1.20 = 492,000원
실제 주문: 실패 (과도하게 보수적)
```

### **현재 설정**
```
예수금: 820,000원
매수 가능: 820,000 * 0.98 * 1.10 = 883,960원
실제 주문: 가능 ✅
```

**차이점:**
- 이전: 820,000원 중 492,000원만 사용 가능
- 현재: 820,000원 중 fulfilled 사용 가능
- **약 79% 개선!**

---

## ⚙️ 파라미터 조정 방법

### **더 공격적으로 만들기**

```rust
// stock/src/bin/kis_auto_trading.rs
let safety_ratio = if buyable_from_api.is_some() {
    0.99  // 98% → 99%
} else {
    0.90  // 80% → 90%
};

let required_cash = total_with_fee * 1.05;  // 10% → 5%
```

### **더 보수적으로 만들기**

```rust
// stock/src/bin/kis_auto_trading.rs
let safety_ratio = if buyable_from_api.is_some() {
    0.95  // 98% → 95%
} else {
    0.70  // 80% → 70%
};

let required_cash = total_with_fee * 1.15;  // 10% → 15%
```

---

## 🛡️ 안전 장치 유지

### **여전히 작동하는 안전 장치**

1. ✅ **포지션 제한**: 종목당 최대 50%
2. ✅ **현금 보유**: 최소 20% 현금 유지
3. ✅ **안전 비율**: 기본 80-98% 사용
4. ✅ **최종 검증**: 10% 안전 마진
5. ✅ **증권사 API**: 정확한 매수 가능 금액 조회

---

## 🐛 트러블슈팅

### 문제: "매수 수량이 0입니다"

**원인:**
- 보유 현금이 주가보다 적음
- 안전 비율이 너무 보수적

**해결:**
```rust
// safety_ratio 증가 (0.50 → 0.80)
```

### 문제: "보유 현금 부족! (안전 마진 포함)"

**원인:**
- 최종 검증 시 10% 안전 마진 적용
- 실제 필요 금액보다 10% 더 필요

**해결:**
```rust
// required_cash 계산 조정 (1.10 → 1.05)
```

### 문제: 증권사 에러

**원인:**
- 증권사 API 한도 초과
- 실제 주문 실패

**해결:**
- 증권사 앱에서 직접 확인
- HTS에서 잔고 확인

---

## 📊 권장 설정

### **초보자 (안전 우선)**
```rust
safety_ratio = 0.90 (API 없음)
safety_ratio = 0.97 (API 성공)
required_cash = total_with_fee * 1.15  // 15% 안전 마진
```

### **일반 사용자 (균형)**
```rust
safety_ratio = 0.80 (API 없음)  // 현재 설정
safety_ratio = 0.98 (API 성공)  // 현재 설정
required_cash = total_with_fee * 1.10  // 10% 안전 마진 (현재 설정)
```

### **고급 사용자 (효율 우선)**
```rust
safety_ratio = 0.95 (API 없음)
safety_ratio = 0.99 (API 성공)
required_cash = total_with_fee * 1.05  // 5% 안전 마진
```

---

## 📝 변경 이력

- **2025-10-28**: 안전 마진 20% → 10%, 안전 비율 50%/95% → 80%/98%로 완화
- **이전**: 과도하게 보수적 설정으로 주문 실패
- **현재**: 적절한 안전 마진으로 주문 성공률 향상

---

## ⚠️ 주의사항

1. **과도한 완화는 위험할 수 있습니다**
   - 증권사 추가 수수료
   - 환율 변동 (해외 주식)
   - 기타 숨겨진 비용

2. **적절한 안전 마진 유지 권장**
   - 최소 5% 이상 유지
   - 최대 15% 이하 권장

3. **시뮬레이션으로 충분히 테스트**
   - DRY-RUN 모드로 먼저 테스트
   - 실제 결과 확인 후 적용


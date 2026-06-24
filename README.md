# 🚀 Rust Pekko - Robinhood 스타일 트레이딩 앱

[![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg)](https://www.rust-lang.org/)
[![Dioxus](https://img.shields.io/badge/dioxus-0.6-blue.svg)](https://dioxuslabs.com/)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)

> **"복잡한 금융을 스마트폰에서 누구나 쉽게 사용할 수 있도록"**

Robinhood의 핵심 철학을 Rust와 Dioxus로 구현한 크로스 플랫폼 트레이딩 앱입니다.

## ✨ 주요 특징

### 🎯 **Robinhood 철학 구현**
- **극단적 단순성**: 복잡한 설정 제거, Buy/Sell 원버튼 트레이딩
- **모바일 퍼스트**: 터치 친화적 인터페이스, 세로 스크롤 중심
- **감정적 참여**: 컨펙티 애니메이션, Robinhood Green 색상 시스템

### 💰 **핵심 기능**
- 📊 **Portfolio**: 포트폴리오 가치와 주식 목록
- 📈 **Trading**: 원버튼 Buy/Sell 인터페이스
- 📰 **News**: 시장 뉴스 카드 레이아웃
- ⚙️ **Settings**: 앱 설정 관리

### 🎨 **UI/UX 특징**
- Robinhood Green (#00C805) 브랜드 색상
- 큰 숫자로 포트폴리오 가치 표시
- 색상 코딩 (상승: 녹색, 하락: 빨간색)
- 카드 기반 디자인
- 부드러운 애니메이션과 호버 효과

## 🛠 기술 스택

- **Frontend**: Dioxus 0.6 (크로스 플랫폼 UI)
- **Backend**: Rust (고성능 시스템 프로그래밍)
- **Runtime**: Tokio (비동기 처리)
- **Styling**: CSS Variables (일관된 디자인 시스템)

## 📱 플랫폼 지원

- ✅ **Desktop**: Windows, macOS, Linux
- ✅ **Mobile**: iOS, Android
- ✅ **Web**: WASM 지원

## 💰 실거래 시스템

프로젝트에는 **한국투자증권 API**를 연동한 실거래 시스템이 포함되어 있습니다.

### 📚 실거래 가이드

- **[KIS_GUIDE.md](stock/KIS_GUIDE.md)**: API 설정 및 자동매매 가이드
- **[RUN_REAL_TRADING.md](stock/RUN_REAL_TRADING.md)**: 실거래 실행 가이드

### ⚡ 빠른 실거래 시작

```bash
cd stock
./run_real_trading.sh
```

**⚠️ 경고**: 실거래를 시작하기 전 반드시 가이드를 읽으세요!

### 🤖 자동매매 시스템

```bash
# DRY-RUN 모드 (시뮬레이션)
cargo run --bin kis-auto-trading -p stock

# 실거래 모드 (실제 주문!)
cargo run --bin kis-auto-trading -p stock -- --real
```

### 📊 주요 기능

- ✅ 실시간 주식 시세 조회
- ✅ 자동 매수/매도 주문
- ✅ 목표 수익률 자동 매도
- ✅ 손절매 자동 실행
- ✅ 포지션 관리 및 리스크 관리

---

## 🚀 빠른 시작

### 설치

```bash
# Rust 설치 (필요한 경우)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 프로젝트 클론
git clone https://github.com/yhc007/rust_pekko.git
cd rust_pekko
```

### 실행

```bash
# 데스크톱 앱 실행
cd trading-mobile
cargo run
```

### iOS 개발

```bash
# iOS 타겟 추가
rustup target add aarch64-apple-ios
rustup target add aarch64-apple-ios-sim

# iOS 시뮬레이터 빌드
cargo build --target aarch64-apple-ios-sim
```

## 📊 프로젝트 구조

```
pekko-rust/
├── trading-mobile/          # 메인 모바일 앱
│   ├── src/
│   │   ├── lib.rs          # 메인 앱 로직
│   │   ├── components/     # UI 컴포넌트
│   │   ├── services/       # 비즈니스 로직
│   │   └── assets/         # CSS 스타일
│   ├── ios/                # iOS 설정
│   └── dioxus.toml         # Dioxus 설정
├── stock/                   # 주식 거래 라이브러리
├── pekko-actor/            # Actor 시스템
├── pekko-persistence/      # 영속성
└── pekko-remoting/         # 원격 통신
```

## 🎯 Robinhood vs 구현된 앱

| 특징 | Robinhood | 구현된 앱 |
|------|-----------|-----------|
| **단순성** | 극단적 단순화 | ✅ 동일 |
| **모바일 우선** | 터치 최적화 | ✅ 동일 |
| **원버튼 트레이딩** | Buy/Sell 한 번의 탭 | ✅ 동일 |
| **컨펙티 애니메이션** | 거래 완료 축하 | ✅ 동일 |
| **색상 시스템** | Robinhood Green | ✅ 동일 |

## 📱 모바일 시뮬레이션 테스트

브라우저에서 모바일 환경을 시뮬레이션하여 테스트할 수 있습니다:

1. **Chrome/Safari**에서 앱 실행
2. **F12** (개발자 도구) 열기
3. **모바일 아이콘** 클릭 (📱)
4. **디바이스 선택**: iPhone 14 Pro, iPhone SE, iPad 등

### 테스트 항목
- ✅ 터치 친화적 버튼 크기
- ✅ 부드러운 스크롤 경험
- ✅ 컨펙티 애니메이션
- ✅ 색상 코딩 가독성
- ✅ 원버튼 트레이딩 직관성

## 🔧 개발 환경

### 필요 도구
- Rust 1.70+
- Cargo
- Xcode (iOS 개발용)
- Git

### 빌드 명령어
```bash
# 개발 빌드
cargo build

# 릴리즈 빌드
cargo build --release

# iOS 시뮬레이터
cargo build --target aarch64-apple-ios-sim

# iOS 디바이스
cargo build --target aarch64-apple-ios
```

## 📈 향후 계획

### 단기 목표
- [ ] 실제 API 연동
- [ ] 실시간 데이터 스트리밍
- [ ] 사용자 인증 시스템

### 중기 목표
- [ ] 고급 차트 기능
- [ ] 알림 시스템
- [ ] 소셜 트레이딩

### 장기 목표
- [ ] AI 기반 추천 시스템
- [ ] 암호화폐 지원
- [ ] 글로벌 마켓 지원

## 🤝 기여하기

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 라이선스

이 프로젝트는 Apache-2.0 라이선스 하에 배포됩니다. 자세한 내용은 [LICENSE](LICENSE) 파일을 참조하세요.

## 📞 연락처

- 프로젝트 링크: [https://github.com/yhc007/rust_pekko](https://github.com/yhc007/rust_pekko)
- 이슈 리포트: [GitHub Issues](https://github.com/yhc007/rust_pekko/issues)

## 🙏 감사의 말

- [Dioxus](https://dioxuslabs.com/) - 크로스 플랫폼 UI 프레임워크
- [Rust](https://www.rust-lang.org/) - 시스템 프로그래밍 언어
- [Robinhood](https://robinhood.com/) - UI/UX 영감

---

**"복잡한 금융을 스마트폰에서 누구나 쉽게 사용할 수 있도록"** 🚀
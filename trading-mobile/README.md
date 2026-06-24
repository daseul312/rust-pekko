# Trading Mobile - iOS App

Dioxus로 구축된 Rust 기반 모바일 트레이딩 앱입니다. Pekko Actor System과 ML4T Trading System을 통합하여 실시간 트레이딩 기능을 제공합니다.

## 🚀 주요 기능

- **대시보드**: 실시간 시장 개요 및 포트폴리오 가치
- **포트폴리오**: 보유 종목 관리 및 성과 분석
- **트레이딩**: 실시간 주문 및 거래 실행
- **뉴스**: 시장 뉴스 및 분석 정보
- **설정**: 앱 설정 및 계정 관리

## 🛠 기술 스택

- **Frontend**: Dioxus (Rust)
- **Backend**: Pekko Actor System
- **Trading**: ML4T System with KIS API
- **Platform**: iOS (추후 Android 지원)

## 📱 iOS 개발 환경 설정

### 1. 필수 도구 설치

```bash
# Xcode 설치 (App Store에서)
# Rust 설치
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Dioxus CLI 설치
cargo install dioxus-cli

# iOS 타겟 추가
rustup target add aarch64-apple-ios
rustup target add x86_64-apple-ios
```

### 2. 프로젝트 빌드

```bash
# 개발 모드로 빌드
dx build --platform ios

# 릴리즈 빌드
dx build --platform ios --release
```

### 3. 시뮬레이터에서 실행

```bash
# iOS 시뮬레이터에서 실행
dx run --platform ios
```

## 🏗 프로젝트 구조

```
trading-mobile/
├── src/
│   ├── lib.rs              # 메인 앱 컴포넌트
│   ├── components/         # 재사용 가능한 UI 컴포넌트
│   │   ├── stock_card.rs
│   │   ├── holding_card.rs
│   │   └── news_card.rs
│   ├── pages/             # 페이지 컴포넌트
│   ├── services/          # 비즈니스 로직 서비스
│   │   ├── trading_service.rs
│   │   └── market_data_service.rs
│   └── assets/
│       └── styles.css     # 스타일시트
├── ios/                   # iOS 네이티브 설정
│   └── Info.plist
├── dioxus.toml           # Dioxus 설정
├── Cargo.toml            # Rust 의존성
└── README.md
```

## 🔧 설정

### API 키 설정

KIS API를 사용하기 위해 API 키를 설정해야 합니다:

```rust
// services/trading_service.rs에서
const KIS_API_KEY: &str = "your_api_key_here";
const KIS_SECRET: &str = "your_secret_here";
```

### 네트워크 설정

`ios/Info.plist`에서 네트워크 보안 설정을 확인하세요:

```xml
<key>NSAppTransportSecurity</key>
<dict>
    <key>NSAllowsArbitraryLoads</key>
    <true/>
</dict>
```

## 📊 Pekko Actor System 통합

이 앱은 Pekko Actor System과 통합되어 있습니다:

- **Trading Agent**: 주문 실행 및 관리
- **Data Agent**: 실시간 시장 데이터 수집
- **Risk Agent**: 리스크 관리 및 모니터링
- **ML Agent**: 머신러닝 기반 예측

## 🎨 UI/UX 특징

- **네이티브 iOS 디자인**: iOS Human Interface Guidelines 준수
- **다크 모드 지원**: 시스템 설정에 따른 자동 테마 전환
- **터치 최적화**: 모바일에 최적화된 터치 인터페이스
- **반응형 디자인**: 다양한 화면 크기 지원

## 🚀 배포

### TestFlight 배포

1. Apple Developer 계정 설정
2. 프로비저닝 프로파일 생성
3. 앱 서명 설정
4. TestFlight 업로드

### App Store 배포

1. App Store Connect에서 앱 등록
2. 메타데이터 및 스크린샷 업로드
3. 심사 제출

## 🔍 디버깅

### 로그 확인

```bash
# iOS 시뮬레이터 로그
xcrun simctl spawn booted log stream --predicate 'process == "trading-mobile"'
```

### 성능 모니터링

- Xcode Instruments 사용
- 메모리 및 CPU 사용량 모니터링
- 네트워크 요청 분석

## 🤝 기여하기

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다. 자세한 내용은 `LICENSE` 파일을 참조하세요.

## 📞 지원

문제가 있거나 질문이 있으시면 GitHub Issues를 통해 문의해 주세요.

---

**Trading Mobile** - Rust로 구축된 차세대 모바일 트레이딩 플랫폼

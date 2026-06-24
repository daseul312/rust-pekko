use dioxus::prelude::*;
use dioxus_desktop::{Config, WindowBuilder};

/// 메인 앱 컴포넌트
#[component]
fn App() -> Element {
    rsx! {
        style { {include_str!("../assets/trading_app.css")} }
        
        div { class: "app-container",
            // 헤더
            header { class: "header",
                h1 { "📈 주식 자동매매 시스템" }
                div { class: "header-info",
                    span { "💰 예수금: 339,574원" }
                    span { "📈 일일손익: +8,400원" }
                }
            }

            // 네비게이션
            nav { class: "navbar",
                button { class: "nav-btn active", "🏠 대시보드" }
                button { class: "nav-btn", "📊 포트폴리오" }
                button { class: "nav-btn", "🤖 거래설정" }
                button { class: "nav-btn", "⚙️ 설정" }
            }

            // 메인 콘텐츠
            main { class: "main-content",
                // 계좌 요약 카드
                section { class: "account-summary",
                    div { class: "card",
                        h2 { "💰 계좌 요약" }
                        div { class: "balance-grid",
                            div { class: "balance-item",
                                span { class: "label", "총 자산" }
                                span { class: "value", "1,151,374원" }
                            }
                            div { class: "balance-item",
                                span { class: "label", "보유 종목" }
                                span { class: "value", "2개" }
                            }
                            div { class: "balance-item",
                                span { class: "label", "일일 수익률" }
                                span { class: "value positive", "+0.73%" }
                            }
                        }
                    }
                }

                // 보유 종목
                section { class: "holdings-section",
                    h2 { "📊 보유 종목" }
                    div { class: "holdings-grid",
                        div { class: "holding-card",
                            div { class: "stock-header",
                                h3 { "기아 (000270)" }
                                span { class: "status positive", "📈 +1.64%" }
                            }
                            div { class: "stock-details",
                                div { class: "detail-row",
                                    span { "수량: 5주" }
                                    span { "평단가: 102,120원" }
                                }
                                div { class: "detail-row",
                                    span { "현재가: 103,800원" }
                                    span { class: "pnl positive", "손익: +8,400원" }
                                }
                            }
                        }
                        
                        div { class: "holding-card",
                            div { class: "stock-header",
                                h3 { "한국전력 (015760)" }
                                span { class: "status positive", "📈 +4.52%" }
                            }
                            div { class: "stock-details",
                                div { class: "detail-row",
                                    span { "수량: 8주" }
                                    span { "평단가: 37,650원" }
                                }
                                div { class: "detail-row",
                                    span { "현재가: 39,350원" }
                                    span { class: "pnl positive", "손익: +13,600원" }
                                }
                            }
                        }
                    }
                }

                // 자동매매 상태
                section { class: "trading-status",
                    div { class: "card",
                        h2 { "🤖 자동매매 상태" }
                        div { class: "status-grid",
                            div { class: "status-item",
                                span { class: "label", "상태" }
                                span { class: "value success", "▶️ 활성" }
                            }
                            div { class: "status-item",
                                span { class: "label", "목표 수익률" }
                                span { class: "value", "3.0%" }
                            }
                            div { class: "status-item",
                                span { class: "label", "손절매" }
                                span { class: "value", "-2.0%" }
                            }
                            div { class: "status-item",
                                span { class: "label", "최대 포지션" }
                                span { class: "value", "50%" }
                            }
                        }
                        div { class: "action-buttons",
                            button { class: "btn btn-warning", "⏸️ 일시정지" }
                            button { class: "btn btn-danger", "🛑 중지" }
                            button { class: "btn btn-primary", "⚙️ 설정 변경" }
                        }
                    }
                }

                // 최근 거래 내역
                section { class: "recent-trades",
                    div { class: "card",
                        h2 { "📋 최근 거래 내역" }
                        div { class: "trades-table",
                            div { class: "trade-header",
                                span { "시간" }
                                span { "종목" }
                                span { "구분" }
                                span { "수량" }
                                span { "가격" }
                                span { "상태" }
                            }
                            div { class: "trade-row",
                                span { "15:23" }
                                span { "기아" }
                                span { "매수" }
                                span { "5주" }
                                span { "102,120원" }
                                span { class: "status success", "체결" }
                            }
                            div { class: "trade-row",
                                span { "14:45" }
                                span { "한국전력" }
                                span { "매수" }
                                span { "8주" }
                                span { "37,650원" }
                                span { class: "status success", "체결" }
                            }
                        }
                    }
                }

                // 빠른 액션
                section { class: "quick-actions",
                    h2 { "⚡ 빠른 액션" }
                    div { class: "action-buttons",
                        button { class: "btn btn-success", "🔄 계좌 새로고침" }
                        button { class: "btn btn-info", "📊 상세 차트" }
                        button { class: "btn btn-secondary", "📝 노션 리포트" }
                    }
                }
            }

            // 푸터
            footer { class: "footer",
                p { "🚀 주식 자동매매 시스템 v1.0 | Made with Rust & Dioxus" }
            }
        }
    }
}

/// 메인 함수
fn main() {
    // 로깅 초기화
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // .env 파일 로드
    dotenv::dotenv().ok();

    // 데스크탑 앱 설정
    let config = Config::new()
        .with_window(WindowBuilder::new()
            .with_title("주식 자동매매 시스템")
            .with_min_size(1000.0, 700.0)
            .with_inner_size(1200.0, 800.0)
            .with_resizable(true));

    // 앱 실행
    dioxus_desktop::launch_with_props(App, (), config);
}

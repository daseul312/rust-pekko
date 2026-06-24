use dioxus::prelude::*;

/// 메인 앱 컴포넌트
#[component]
fn App(cx: Scope) -> Element {
    rsx! {
        div { class: "app",
            style { {include_str!("../assets/style.css")} }
            
            header { class: "header",
                h1 { "📈 주식 자동매매 시스템" }
                div { class: "account-info",
                    span { "💰 예수금: 339,574원" }
                    span { "📈 일일손익: +8,400원" }
                }
            }

            nav { class: "nav",
                button { class: "nav-btn active", "🏠 대시보드" }
                button { class: "nav-btn", "📊 포트폴리오" }
                button { class: "nav-btn", "🤖 거래설정" }
                button { class: "nav-btn", "⚙️ 설정" }
            }

            main { class: "main",
                section { class: "summary",
                    div { class: "card",
                        h2 { "💰 계좌 요약" }
                        div { class: "stats",
                            div { class: "stat",
                                span { "총 자산" }
                                span { "1,151,374원" }
                            }
                            div { class: "stat",
                                span { "보유 종목" }
                                span { "2개" }
                            }
                            div { class: "stat",
                                span { "일일 수익률" }
                                span { class: "positive", "+0.73%" }
                            }
                        }
                    }
                }

                section { class: "holdings",
                    h2 { "📊 보유 종목" }
                    div { class: "stocks",
                        div { class: "stock-card",
                            div { class: "stock-header",
                                h3 { "기아 (000270)" }
                                span { class: "change positive", "📈 +1.64%" }
                            }
                            div { class: "stock-info",
                                p { "수량: 5주 | 평단가: 102,120원" }
                                p { "현재가: 103,800원 | 손익: +8,400원" }
                            }
                        }
                        
                        div { class: "stock-card",
                            div { class: "stock-header",
                                h3 { "한국전력 (015760)" }
                                span { class: "change positive", "📈 +4.52%" }
                            }
                            div { class: "stock-info",
                                p { "수량: 8주 | 평단가: 37,650원" }
                                p { "현재가: 39,350원 | 손익: +13,600원" }
                            }
                        }
                    }
                }

                section { class: "trading",
                    div { class: "card",
                        h2 { "🤖 자동매매 상태" }
                        div { class: "status",
                            div { class: "status-item",
                                span { "상태: " }
                                span { class: "active", "▶️ 활성" }
                            }
                            div { class: "status-item",
                                span { "목표 수익률: 3.0%" }
                            }
                            div { class: "status-item",
                                span { "손절매: -2.0%" }
                            }
                            div { class: "status-item",
                                span { "최대 포지션: 50%" }
                            }
                        }
                        div { class: "actions",
                            button { class: "btn warning", "⏸️ 일시정지" }
                            button { class: "btn danger", "🛑 중지" }
                            button { class: "btn primary", "⚙️ 설정 변경" }
                        }
                    }
                }

                section { class: "trades",
                    div { class: "card",
                        h2 { "📋 최근 거래 내역" }
                        div { class: "trade-table",
                            div { class: "trade-row header",
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
                                span { class: "success", "체결" }
                            }
                            div { class: "trade-row",
                                span { "14:45" }
                                span { "한국전력" }
                                span { "매수" }
                                span { "8주" }
                                span { "37,650원" }
                                span { class: "success", "체결" }
                            }
                        }
                    }
                }

                section { class: "actions",
                    h2 { "⚡ 빠른 액션" }
                    div { class: "action-buttons",
                        button { class: "btn success", "🔄 계좌 새로고침" }
                        button { class: "btn info", "📊 상세 차트" }
                        button { class: "btn secondary", "📝 노션 리포트" }
                    }
                }
            }

            footer { class: "footer",
                p { "🚀 주식 자동매매 시스템 v1.0 | Made with Rust & Dioxus" }
            }
        }
    }
}

fn main() {
    dioxus_web::launch(App);
}

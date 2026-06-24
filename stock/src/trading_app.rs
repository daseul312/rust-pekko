use dioxus::prelude::*;
use dioxus_desktop::{Config, WindowBuilder};
use dioxus_router::prelude::*;

mod korea_investment_api;
mod daily_report;
mod notion_reporter;

use korea_investment_api::KoreaInvestmentAPI;

/// 메인 앱 컴포넌트
#[component]
fn App() -> Element {
    rsx! {
        style { {include_str!("../assets/trading_app.css")} }
        Router::<Route> {}
    }
}

/// 라우트 정의
#[derive(Clone, Debug, PartialEq, Routable)]
enum Route {
    #[route("/")]
    Dashboard {},
    #[route("/portfolio")]
    Portfolio {},
    #[route("/trading")]
    Trading {},
    #[route("/settings")]
    Settings {},
}

/// 네비게이션 바
#[component]
fn NavBar() -> Element {
    rsx! {
        nav { class: "navbar",
            div { class: "nav-brand",
                h2 { "📈 자동매매" }
            }
            div { class: "nav-links",
                Link { to: Route::Dashboard {}, "🏠 대시보드" }
                Link { to: Route::Portfolio {}, "📊 포트폴리오" }
                Link { to: Route::Trading {}, "🤖 거래" }
                Link { to: Route::Settings {}, "⚙️ 설정" }
            }
        }
    }
}

/// 대시보드 페이지
#[component]
fn Dashboard() -> Element {
    let mut account_balance = use_signal(|| 0.0);
    let mut holdings = use_signal(|| Vec::<StockHolding>::new());
    let mut daily_pnl = use_signal(|| 0.0);

    // 계좌 정보 로드
    use_effect(move || {
        spawn(async move {
            if let Ok(mut api) = KoreaInvestmentAPI::from_env() {
                if let Ok(_) = api.get_access_token().await {
                    if let Ok(_) = api.get_balance().await {
                        account_balance.set(339574.0);
                        holdings.set(vec![
                            StockHolding {
                                code: "000270".to_string(),
                                name: "기아".to_string(),
                                quantity: 5,
                                avg_price: 102120.0,
                                current_price: 103800.0,
                            },
                            StockHolding {
                                code: "015760".to_string(),
                                name: "한국전력".to_string(),
                                quantity: 8,
                                avg_price: 37650.0,
                                current_price: 39350.0,
                            },
                        ]);
                        daily_pnl.set(8400.0);
                    }
                }
            }
        });
    });

    rsx! {
        div { class: "page-container",
            NavBar {}
            
            main { class: "dashboard",
                // 헤더
                header { class: "page-header",
                    h1 { "📈 주식 자동매매 대시보드" }
                    div { class: "header-actions",
                        button { class: "btn btn-primary", "⚙️ 설정" }
                        button { class: "btn btn-secondary", "📊 상세보기" }
                    }
                }

                // 계좌 요약
                section { class: "account-summary",
                    div { class: "card",
                        h2 { "💰 계좌 요약" }
                        div { class: "balance-grid",
                            div { class: "balance-item",
                                span { class: "label", "예수금" }
                                span { class: "value", "{account_balance:,.0}원" }
                            }
                            div { class: "balance-item",
                                span { class: "label", "일일 손익" }
                                span { 
                                    class: if daily_pnl() >= 0.0 { "value positive" } else { "value negative" },
                                    if daily_pnl() >= 0.0 { "📈 +" } else { "📉 " }
                                    "{daily_pnl:,.0}원"
                                }
                            }
                        }
                    }
                }

                // 보유 종목
                section { class: "holdings",
                    h2 { "📊 보유 종목" }
                    div { class: "holdings-grid",
                        for holding in holdings().iter() {
                            div { class: "holding-card",
                                div { class: "stock-info",
                                    h3 { "{holding.name}" }
                                    p { class: "code", "{holding.code}" }
                                }
                                div { class: "stock-details",
                                    p { "수량: {holding.quantity}주" }
                                    p { "평단가: {holding.avg_price:,.0}원" }
                                    p { "현재가: {holding.current_price:,.0}원" }
                                    p { 
                                        class: if holding.current_price >= holding.avg_price { "pnl positive" } else { "pnl negative" },
                                        "손익: {((holding.current_price - holding.avg_price) * holding.quantity as f64):,.0}원"
                                    }
                                }
                            }
                        }
                    }
                }

                // 빠른 액션
                section { class: "quick-actions",
                    h2 { "⚡ 빠른 액션" }
                    div { class: "action-buttons",
                        button { class: "btn btn-success", "🔄 계좌 새로고침" }
                        button { class: "btn btn-warning", "⏸️ 자동매매 일시정지" }
                        button { class: "btn btn-danger", "🛑 자동매매 중지" }
                    }
                }
            }
        }
    }
}

/// 포트폴리오 페이지
#[component]
fn Portfolio() -> Element {
    rsx! {
        div { class: "page-container",
            NavBar {}
            
            main { class: "portfolio",
                h1 { "📊 포트폴리오" }
                div { class: "portfolio-chart",
                    div { class: "chart-placeholder",
                        "📈 포트폴리오 차트가 여기에 표시됩니다"
                    }
                }
                div { class: "portfolio-table",
                    table { class: "data-table",
                        thead {
                            tr {
                                th { "종목명" }
                                th { "수량" }
                                th { "평단가" }
                                th { "현재가" }
                                th { "손익" }
                                th { "수익률" }
                            }
                        }
                        tbody {
                            tr {
                                td { "기아" }
                                td { "5주" }
                                td { "102,120원" }
                                td { "103,800원" }
                                td { class: "positive", "+8,400원" }
                                td { class: "positive", "+1.64%" }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// 거래 페이지
#[component]
fn Trading() -> Element {
    let mut auto_trading_enabled = use_signal(|| false);
    let mut trading_strategy = use_signal(|| TradingStrategy {
        profit_target: 3.0,
        stop_loss: -2.0,
        max_position_ratio: 0.5,
    });

    rsx! {
        div { class: "page-container",
            NavBar {}
            
            main { class: "trading",
                h1 { "🤖 자동매매 설정" }
                
                div { class: "trading-controls",
                    div { class: "card",
                        h3 { "자동매매 제어" }
                        div { class: "control-group",
                            label { class: "checkbox-label",
                                input { 
                                    r#type: "checkbox",
                                    checked: auto_trading_enabled(),
                                    onchange: move |evt| auto_trading_enabled.set(evt.value().parse().unwrap_or(false))
                                }
                                span { "자동매매 활성화" }
                            }
                        }
                    }

                    div { class: "card",
                        h3 { "📋 거래 전략" }
                        
                        div { class: "input-group",
                            label { "목표 수익률 (%)" }
                            input { 
                                r#type: "number",
                                value: trading_strategy().profit_target,
                                step: "0.1",
                                onchange: move |evt| {
                                    if let Ok(val) = evt.value().parse::<f64>() {
                                        trading_strategy.update(|s| s.profit_target = val);
                                    }
                                }
                            }
                        }

                        div { class: "input-group",
                            label { "손절매 (%)" }
                            input { 
                                r#type: "number",
                                value: trading_strategy().stop_loss,
                                step: "0.1",
                                onchange: move |evt| {
                                    if let Ok(val) = evt.value().parse::<f64>() {
                                        trading_strategy.update(|s| s.stop_loss = val);
                                    }
                                }
                            }
                        }

                        div { class: "input-group",
                            label { "최대 포지션 비율 (%)" }
                            input { 
                                r#type: "number",
                                value: trading_strategy().max_position_ratio * 100.0,
                                step: "5",
                                onchange: move |evt| {
                                    if let Ok(val) = evt.value().parse::<f64>() {
                                        trading_strategy.update(|s| s.max_position_ratio = val / 100.0);
                                    }
                                }
                            }
                        }
                    }

                    div { class: "action-buttons",
                        button { 
                            class: "btn btn-primary",
                            "💾 설정 저장"
                        }
                        button { 
                            class: if auto_trading_enabled() { "btn btn-danger" } else { "btn btn-success" },
                            onclick: move |_| auto_trading_enabled.toggle(),
                            if auto_trading_enabled() { "⏸️ 자동매매 일시정지" } else { "▶️ 자동매매 시작" }
                        }
                    }
                }
            }
        }
    }
}

/// 설정 페이지
#[component]
fn Settings() -> Element {
    rsx! {
        div { class: "page-container",
            NavBar {}
            
            main { class: "settings",
                h1 { "⚙️ 설정" }
                
                div { class: "settings-sections",
                    section { class: "card",
                        h2 { "🔑 API 설정" }
                        div { class: "input-group",
                            label { "KIS App Key" }
                            input { r#type: "password", placeholder: "앱 키를 입력하세요" }
                        }
                        div { class: "input-group",
                            label { "KIS App Secret" }
                            input { r#type: "password", placeholder: "앱 시크릿을 입력하세요" }
                        }
                        div { class: "input-group",
                            label { "계좌번호" }
                            input { placeholder: "계좌번호를 입력하세요" }
                        }
                        button { class: "btn btn-primary", "🔗 연결 테스트" }
                    }

                    section { class: "card",
                        h2 { "📝 노션 설정" }
                        div { class: "input-group",
                            label { "노션 토큰" }
                            input { r#type: "password", placeholder: "노션 토큰을 입력하세요" }
                        }
                        div { class: "input-group",
                            label { "데이터베이스 ID" }
                            input { placeholder: "데이터베이스 ID를 입력하세요" }
                        }
                        button { class: "btn btn-primary", "🔗 연결 테스트" }
                    }

                    section { class: "card",
                        h2 { "🔔 알림 설정" }
                        div { class: "checkbox-group",
                            label { class: "checkbox-label",
                                input { r#type: "checkbox", id: "email-notifications" }
                                span { "이메일 알림" }
                            }
                        }
                        div { class: "checkbox-group",
                            label { class: "checkbox-label",
                                input { r#type: "checkbox", id: "push-notifications" }
                                span { "푸시 알림" }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// 데이터 구조체들
#[derive(Clone, Debug)]
struct StockHolding {
    code: String,
    name: String,
    quantity: i32,
    avg_price: f64,
    current_price: f64,
}

#[derive(Clone, Debug)]
struct TradingStrategy {
    profit_target: f64,
    stop_loss: f64,
    max_position_ratio: f64,
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
            .with_min_size(800.0, 600.0)
            .with_inner_size(1200.0, 800.0));

    // 앱 실행
    dioxus_desktop::launch_with_props(App, (), config);
}
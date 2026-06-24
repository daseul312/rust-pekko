use dioxus::prelude::*;

#[cfg(target_os = "ios")]
use dioxus_mobile::ios::*;

#[cfg(target_os = "android")]
use dioxus_mobile::android::*;

mod components;
mod pages;
mod services;

use components::*;
use pages::*;
use services::*;

#[cfg_attr(target_os = "ios", export)]
#[cfg_attr(target_os = "android", export)]
pub fn main() {
    #[cfg(target_os = "ios")]
    dioxus_mobile::ios::launch(TradingApp);
    
    #[cfg(target_os = "android")]
    dioxus_mobile::android::launch(TradingApp);
    
    #[cfg(not(any(target_os = "ios", target_os = "android")))]
    dioxus::launch(TradingApp);
}

    #[component]
    fn TradingApp() -> Element {
        let mut current_page = use_signal(|| Page::Dashboard);

        rsx! {
            div { class: "app-container",
                Header { current_page, set_current_page: move |page| current_page.set(page) }
                
                match current_page() {
                    Page::Dashboard => rsx! { Dashboard {} },
                    Page::Portfolio => rsx! { Portfolio {} },
                    Page::Trading => rsx! { Trading {} },
                    Page::News => rsx! { News {} },
                    Page::Settings => rsx! { Settings {} },
                }
                
                BottomNavigation { current_page, set_current_page: move |page| current_page.set(page) }
            }
        }
    }

#[derive(Debug, Clone, PartialEq)]
pub enum Page {
    Dashboard,
    Portfolio,
    Trading,
    News,
    Settings,
}

#[component]
fn Header(current_page: Signal<Page>, set_current_page: Callback<Page>) -> Element {
    rsx! {
        div { class: "header",
            div { class: "header-content",
                h1 { "Trading Mobile" }
                div { class: "header-actions",
                    button { 
                        class: "settings-btn",
                        onclick: move |_| set_current_page.call(Page::Settings),
                        "⚙️"
                    }
                }
            }
        }
    }
}

#[component]
fn BottomNavigation(current_page: Signal<Page>, set_current_page: Callback<Page>) -> Element {
    rsx! {
        div { class: "bottom-nav",
            button { 
                class: if current_page() == Page::Dashboard { "nav-item active" } else { "nav-item" },
                onclick: move |_| set_current_page.call(Page::Dashboard),
                div { "📊" }
                span { "Dashboard" }
            }
            button { 
                class: if current_page() == Page::Portfolio { "nav-item active" } else { "nav-item" },
                onclick: move |_| set_current_page.call(Page::Portfolio),
                div { "💼" }
                span { "Portfolio" }
            }
            button { 
                class: if current_page() == Page::Trading { "nav-item active" } else { "nav-item" },
                onclick: move |_| set_current_page.call(Page::Trading),
                div { "📈" }
                span { "Trading" }
            }
            button { 
                class: if current_page() == Page::News { "nav-item active" } else { "nav-item" },
                onclick: move |_| set_current_page.call(Page::News),
                div { "📰" }
                span { "News" }
            }
        }
    }
}

#[component]
fn Dashboard() -> Element {
    let stocks = use_signal(|| vec![
        StockData {
            symbol: "AAPL".to_string(),
            price: 175.43,
            change: 2.34,
            change_percent: 1.35,
        },
        StockData {
            symbol: "GOOGL".to_string(),
            price: 142.56,
            change: -1.23,
            change_percent: -0.85,
        },
        StockData {
            symbol: "MSFT".to_string(),
            price: 378.85,
            change: 4.67,
            change_percent: 1.25,
        },
        StockData {
            symbol: "TSLA".to_string(),
            price: 245.67,
            change: 5.23,
            change_percent: 2.18,
        },
        StockData {
            symbol: "NVDA".to_string(),
            price: 875.34,
            change: -12.45,
            change_percent: -1.40,
        },
        StockData {
            symbol: "AMZN".to_string(),
            price: 155.89,
            change: 0.89,
            change_percent: 0.57,
        },
        StockData {
            symbol: "META".to_string(),
            price: 312.78,
            change: -3.21,
            change_percent: -1.02,
        },
        StockData {
            symbol: "NFLX".to_string(),
            price: 445.12,
            change: 8.76,
            change_percent: 2.01,
        },
    ]);

    rsx! {
        div { class: "dashboard",
            h2 { "Portfolio" }
            
            // Robinhood-style Portfolio Summary
            div { class: "portfolio-summary",
                div { class: "portfolio-value", "$125,430.50" }
                div { class: "portfolio-change positive", "+$2,340.25 (+1.89%)" }
            }
            
            // Robinhood-style Stock List
            div { class: "stock-list",
                for stock in stocks().iter() {
                    StockCard { stock: stock.clone() }
                }
            }
        }
    }
}

#[component]
fn Portfolio() -> Element {
    let holdings = use_signal(|| vec![
        Holding {
            symbol: "AAPL".to_string(),
            shares: 10.0,
            avg_price: 170.25,
            current_price: 175.43,
            value: 1754.30,
            gain_loss: 51.80,
            gain_loss_percent: 3.04,
        },
        Holding {
            symbol: "GOOGL".to_string(),
            shares: 5.0,
            avg_price: 145.80,
            current_price: 142.56,
            value: 712.80,
            gain_loss: -16.20,
            gain_loss_percent: -2.22,
        },
        Holding {
            symbol: "TSLA".to_string(),
            shares: 3.0,
            avg_price: 240.00,
            current_price: 245.67,
            value: 737.01,
            gain_loss: 17.01,
            gain_loss_percent: 2.36,
        },
        Holding {
            symbol: "NVDA".to_string(),
            shares: 2.0,
            avg_price: 890.00,
            current_price: 875.34,
            value: 1750.68,
            gain_loss: -29.32,
            gain_loss_percent: -1.65,
        },
        Holding {
            symbol: "META".to_string(),
            shares: 5.0,
            avg_price: 315.50,
            current_price: 312.78,
            value: 1563.90,
            gain_loss: -13.60,
            gain_loss_percent: -0.86,
        },
    ]);

    rsx! {
        div { class: "portfolio",
            div { class: "portfolio-header",
                h2 { "Portfolio" }
                div { class: "portfolio-summary",
                    div { class: "total-value",
                        h3 { "Total Value" }
                        div { class: "value", "$2,467.10" }
                    }
                    div { class: "total-gain",
                        h3 { "Total Gain/Loss" }
                        div { class: "value positive", "+$35.60 (+1.47%)" }
                    }
                }
            }
            
            div { class: "holdings",
                h3 { "Holdings" }
                div { class: "holdings-list",
                    for holding in holdings().iter() {
                        HoldingCard { holding: holding.clone() }
                    }
                }
            }
            
            div { class: "portfolio-chart",
                h3 { "Performance Chart" }
                div { class: "chart-placeholder",
                    "📊 Chart will be implemented here"
                }
            }
        }
    }
}

#[component]
fn Trading() -> Element {
    let mut selected_symbol = use_signal(|| "AAPL".to_string());
    let mut current_price = use_signal(|| 175.43);
    let mut price_change = use_signal(|| 2.34);
    let mut price_change_percent = use_signal(|| 1.35);
    let mut amount = use_signal(|| String::new());
    let mut show_confetti = use_signal(|| false);

    let is_positive = price_change() >= 0.0;
    let change_class = if is_positive { "positive" } else { "negative" };
    let change_symbol = if is_positive { "+" } else { "" };

    let handle_buy = move |_| {
        // Robinhood-style one-button trading
        println!("Executing buy order for {} shares of {}", amount(), selected_symbol());
        show_confetti.set(true);
        
        // Hide confetti after 3 seconds
        spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            show_confetti.set(false);
        });
    };

    let handle_sell = move |_| {
        // Robinhood-style one-button trading
        println!("Executing sell order for {} shares of {}", amount(), selected_symbol());
        show_confetti.set(true);
        
        // Hide confetti after 3 seconds
        spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            show_confetti.set(false);
        });
    };

    let mut set_amount = move |value: String| {
        amount.set(value);
    };

    rsx! {
        div { class: "trading",
            // Confetti Animation
            if show_confetti() {
                div { class: "confetti", style: "left: 10%; animation-delay: 0s;" }
                div { class: "confetti", style: "left: 20%; animation-delay: 0.5s;" }
                div { class: "confetti", style: "left: 30%; animation-delay: 1s;" }
                div { class: "confetti", style: "left: 40%; animation-delay: 1.5s;" }
                div { class: "confetti", style: "left: 50%; animation-delay: 0.2s;" }
                div { class: "confetti", style: "left: 60%; animation-delay: 0.7s;" }
                div { class: "confetti", style: "left: 70%; animation-delay: 1.2s;" }
                div { class: "confetti", style: "left: 80%; animation-delay: 1.7s;" }
                div { class: "confetti", style: "left: 90%; animation-delay: 0.4s;" }
            }

            h2 { "Trading" }
            
            // Stock Detail Header - Robinhood Style
            div { class: "stock-detail-header",
                div { class: "stock-detail-symbol", "{selected_symbol()}" }
                div { class: "stock-detail-price", "${current_price():.2}" }
                div { 
                    class: "stock-detail-change {change_class}",
                    "{change_symbol}${price_change():.2} ({change_symbol}{price_change_percent():.2}%)"
                }
            }
            
            // Robinhood One-Button Trading
            div { class: "trading-actions",
                button { 
                    class: "buy-button",
                    onclick: handle_buy,
                    "Buy"
                }
                button { 
                    class: "sell-button",
                    onclick: handle_sell,
                    "Sell"
                }
            }
            
            // Amount Input - Robinhood Style
            div { class: "amount-input-section",
                h3 { "Amount" }
                input { 
                    class: "amount-input",
                    r#type: "number",
                    placeholder: "Enter amount",
                    value: "{amount()}",
                    oninput: move |evt| set_amount(evt.value().to_string())
                }
                
                // Quick Amount Buttons
                div { class: "quick-amounts",
                    button { 
                        class: "quick-amount-btn",
                        onclick: move |_| set_amount("100".to_string()),
                        "$100"
                    }
                    button { 
                        class: "quick-amount-btn",
                        onclick: move |_| set_amount("250".to_string()),
                        "$250"
                    }
                    button { 
                        class: "quick-amount-btn",
                        onclick: move |_| set_amount("500".to_string()),
                        "$500"
                    }
                    button { 
                        class: "quick-amount-btn",
                        onclick: move |_| set_amount("1000".to_string()),
                        "$1,000"
                    }
                    button { 
                        class: "quick-amount-btn",
                        onclick: move |_| set_amount("2500".to_string()),
                        "$2,500"
                    }
                    button { 
                        class: "quick-amount-btn",
                        onclick: move |_| set_amount("5000".to_string()),
                        "$5,000"
                    }
                }
            }
        }
    }
}

#[component]
fn News() -> Element {
    let news_items = use_signal(|| vec![
        NewsItem {
            title: "Apple Reports Strong Q4 Earnings".to_string(),
            summary: "Apple Inc. reported better-than-expected quarterly earnings driven by strong iPhone sales and services growth.".to_string(),
            timestamp: "2 hours ago".to_string(),
            source: "Reuters".to_string(),
        },
        NewsItem {
            title: "Federal Reserve Hints at Rate Cut".to_string(),
            summary: "The Federal Reserve indicated it may consider cutting interest rates in the coming months to support economic growth.".to_string(),
            timestamp: "4 hours ago".to_string(),
            source: "Bloomberg".to_string(),
        },
        NewsItem {
            title: "Tesla Stock Surges on Model Y Sales".to_string(),
            summary: "Tesla shares jumped 5% after reporting record Model Y deliveries and strong demand in key markets.".to_string(),
            timestamp: "6 hours ago".to_string(),
            source: "CNBC".to_string(),
        },
        NewsItem {
            title: "Microsoft AI Revenue Exceeds Expectations".to_string(),
            summary: "Microsoft's AI-powered services division reported 40% revenue growth, beating analyst expectations.".to_string(),
            timestamp: "8 hours ago".to_string(),
            source: "TechCrunch".to_string(),
        },
    ]);

    rsx! {
        div { class: "news",
            div { class: "news-header",
                h2 { "Market News" }
                div { class: "news-filter",
                    button { class: "filter-btn active", "All" }
                    button { class: "filter-btn", "Stocks" }
                    button { class: "filter-btn", "Crypto" }
                    button { class: "filter-btn", "Forex" }
                }
            }
            
            div { class: "news-list",
                for news in news_items().iter() {
                    NewsCard { news: news.clone() }
                }
            }
        }
    }
}

#[component]
fn Settings() -> Element {
    let mut notifications_enabled = use_signal(|| true);
    let mut dark_mode = use_signal(|| false);

    rsx! {
        div { class: "settings",
            div { class: "settings-header",
                h2 { "Settings" }
            }
            
            div { class: "settings-sections",
                div { class: "settings-section",
                    h3 { "Notifications" }
                    div { class: "setting-item",
                        label { "Push Notifications" }
                        input { 
                            r#type: "checkbox",
                            checked: notifications_enabled(),
                            onchange: move |evt| notifications_enabled.set(evt.checked())
                        }
                    }
                }
                
                div { class: "settings-section",
                    h3 { "Appearance" }
                    div { class: "setting-item",
                        label { "Dark Mode" }
                        input { 
                            r#type: "checkbox",
                            checked: dark_mode(),
                            onchange: move |evt| dark_mode.set(evt.checked())
                        }
                    }
                }
                
                div { class: "settings-section",
                    h3 { "Account" }
                    div { class: "setting-item",
                        label { "API Keys" }
                        button { "Manage" }
                    }
                    div { class: "setting-item",
                        label { "Sign Out" }
                        button { class: "danger-btn", "Sign Out" }
                    }
                }
            }
        }
    }
}

// Data structures
#[derive(Debug, Clone)]
#[derive(PartialEq)]
pub struct StockData {
    pub symbol: String,
    pub price: f64,
    pub change: f64,
    pub change_percent: f64,
}

#[derive(Debug, Clone)]
#[derive(PartialEq)]
pub struct Holding {
    pub symbol: String,
    pub shares: f64,
    pub avg_price: f64,
    pub current_price: f64,
    pub value: f64,
    pub gain_loss: f64,
    pub gain_loss_percent: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderType {
    Market,
    Limit,
    Stop,
}

#[derive(Debug, Clone)]
#[derive(PartialEq)]
pub struct NewsItem {
    pub title: String,
    pub summary: String,
    pub timestamp: String,
    pub source: String,
}

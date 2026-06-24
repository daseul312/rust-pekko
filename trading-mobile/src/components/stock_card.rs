use dioxus::prelude::*;
use crate::StockData;

#[component]
pub fn StockCard(stock: StockData) -> Element {
    let is_positive = stock.change >= 0.0;
    let change_class = if is_positive { "positive" } else { "negative" };
    let change_symbol = if is_positive { "+" } else { "" };
    
    // Format price with proper currency formatting
    let formatted_price = format!("${:.2}", stock.price);
    let formatted_change = format!("{}{:.2}", change_symbol, stock.change);
    let formatted_percent = format!("({}{:.2}%)", change_symbol, stock.change_percent);

    rsx! {
        div { 
            class: "stock-card",
            onclick: move |_| {
                // Add stock detail view functionality
                println!("Opening detail view for {}", stock.symbol);
            },
            
            div { class: "stock-header",
                div { class: "stock-symbol", "{stock.symbol}" }
                div { class: "stock-price", "{formatted_price}" }
            }
            div { 
                class: "stock-change {change_class}",
                "{formatted_change} {formatted_percent}"
            }
        }
    }
}

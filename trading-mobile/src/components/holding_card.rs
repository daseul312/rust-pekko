use dioxus::prelude::*;
use crate::Holding;

#[component]
pub fn HoldingCard(holding: Holding) -> Element {
    let is_positive = holding.gain_loss >= 0.0;
    let change_class = if is_positive { "positive" } else { "negative" };
    let change_symbol = if is_positive { "+" } else { "" };
    
    // Format values with proper currency formatting
    let formatted_value = format!("${:.2}", holding.value);
    let formatted_avg_price = format!("${:.2}", holding.avg_price);
    let formatted_current_price = format!("${:.2}", holding.current_price);
    let formatted_gain_loss = format!("{}{:.2}", change_symbol, holding.gain_loss);
    let formatted_gain_loss_percent = format!("({}{:.2}%)", change_symbol, holding.gain_loss_percent);

    rsx! {
        div { 
            class: "holding-card",
            onclick: move |_| {
                // Add holding detail view functionality
                println!("Opening holding detail for {}", holding.symbol);
            },
            
            div { class: "holding-header",
                div { class: "holding-symbol", "{holding.symbol}" }
                div { class: "holding-value", "{formatted_value}" }
            }
            div { class: "holding-details",
                div { class: "detail-row",
                    span { "Shares: {holding.shares:.0}" }
                    span { "Avg: {formatted_avg_price}" }
                }
                div { class: "detail-row",
                    span { "Current: {formatted_current_price}" }
                    span { 
                        class: "holding-gain-loss {change_class}",
                        "{formatted_gain_loss} {formatted_gain_loss_percent}"
                    }
                }
            }
        }
    }
}

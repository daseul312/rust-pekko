use dioxus::prelude::*;
use crate::NewsItem;

#[component]
pub fn NewsCard(news: NewsItem) -> Element {
    rsx! {
        div { 
            class: "news-card",
            onclick: move |_| {
                // Add news tap functionality here
                println!("Tapped on news: {}", news.title);
            },
            
            div { class: "news-title", "{news.title}" }
            div { class: "news-summary", "{news.summary}" }
            div { class: "news-meta",
                span { class: "news-source", "{news.source}" }
                span { "{news.timestamp}" }
            }
        }
    }
}

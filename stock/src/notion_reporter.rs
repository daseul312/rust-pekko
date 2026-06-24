use anyhow::Result;
use reqwest::Client;
use serde_json::json;
use std::env;
use super::daily_report::DailyTradingReport;
use tracing::{ error};

pub struct NotionReporter {
    client: Client,
    notion_token: String,
    database_id: String,
}

impl NotionReporter {
    pub fn from_env() -> Result<Self> {
        let notion_token = env::var("NOTION_TOKEN")?;
        let database_id = env::var("NOTION_DATABASE_ID")?;
        
        Ok(Self {
            client: Client::new(),
            notion_token,
            database_id,
        })
    }
    
    /// 노션에 일일 리포트 페이지 생성
    pub async fn create_daily_report(&self, report: &DailyTradingReport) -> Result<String> {
        let url = "https://api.notion.com/v1/pages";
        
        let emoji = if report.performance.daily_return_rate > 0.0 {
            "📈"
        } else if report.performance.daily_return_rate < 0.0 {
            "📉"
        } else {
            "➖"
        };
        
        let title = format!("{} 거래 리포트", report.date);
        
        let body = json!({
            "parent": {
                "database_id": self.database_id
            },
            "icon": {
                "type": "emoji",
                "emoji": emoji
            },
            "properties": {
                "name": {
                    "title": [{
                        "text": {
                            "content": title
                        }
                    }]
                },
                "date": {
                    "date": {
                        "start": report.date
                    }
                },
                "daily return": {
                    "number": report.performance.daily_return_rate
                },
                "trades": {
                    "number": report.summary.total_trades as i64
                },
                "win rate": {
                    "number": report.summary.win_rate
                },
                "total P&L": {
                    "number": report.summary.total_profit_loss
                }
            },
            "children": self.create_notion_blocks(report)
        });
        let response = self.client
            .post(url)
            .header("Authorization", format!("Bearer {}", self.notion_token))
            .header("Notion-Version", "2022-06-28")
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;
        
        if response.status().is_success() {
            let result: serde_json::Value = response.json().await?;
            let page_url = result["url"].as_str().unwrap_or("").to_string();
            Ok(page_url)
        } else {
            let status = response.status();
            let error_text = response.text().await?;
            error!("노션 API 에러 ({}): {}", status, error_text);
            Err(anyhow::anyhow!("노션 페이지 생성 실패: {} - {}", status, error_text))
        }
    }
    
    fn create_notion_blocks(&self, report: &DailyTradingReport) -> Vec<serde_json::Value> {
        let mut blocks = vec![
            // 거래 요약 헤더
            json!({
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{
                        "type": "text",
                        "text": { "content": "📈 거래 요약" }
                    }]
                }
            }),
            // 요약 정보
            json!({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{
                        "type": "text",
                        "text": { 
                            "content": format!(
                                "총 {}회 거래 (매수: {}회, 매도: {}회)\n총 손익: {:+.0}원 ({:+.2}%)\n승률: {:.1}%",
                                report.summary.total_trades,
                                report.summary.buy_count,
                                report.summary.sell_count,
                                report.summary.total_profit_loss,
                                report.summary.total_profit_loss_rate,
                                report.summary.win_rate
                            )
                        }
                    }]
                }
            }),
            // 수익 현황 헤더
            json!({
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{
                        "type": "text",
                        "text": { "content": "💰 수익 현황" }
                    }]
                }
            }),
            json!({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{
                        "type": "text",
                        "text": { 
                            "content": format!(
                                "시작 자산: {:.0}원\n종료 자산: {:.0}원\n일일 수익: {:+.0}원 ({:+.2}%)",
                                report.performance.starting_balance,
                                report.performance.ending_balance,
                                report.performance.daily_return,
                                report.performance.daily_return_rate
                            )
                        }
                    }]
                }
            }),
        ];
        
        // 거래 내역
        blocks.push(json!({
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [{
                    "type": "text",
                    "text": { "content": "📋 거래 내역" }
                }]
            }
        }));
        
        if report.trades.is_empty() {
            blocks.push(json!({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{
                        "type": "text",
                        "text": { "content": "거래 없음" }
                    }]
                }
            }));
        } else {
            for trade in &report.trades {
                let pl_str = if let Some(pl) = trade.profit_loss {
                    format!(" (손익: {:+.0}원)", pl)
                } else {
                    String::new()
                };
                
                blocks.push(json!({
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {
                        "rich_text": [{
                            "type": "text",
                            "text": {
                                "content": format!(
                                    "{} | {} | {} ({}) | {}주 @ {:.0}원{}",
                                    trade.time, trade.action, trade.symbol, trade.name,
                                    trade.quantity, trade.price, pl_str
                                )
                            }
                        }]
                    }
                }));
            }
        }
        
        // 보유 종목
        blocks.push(json!({
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [{
                    "type": "text",
                    "text": { "content": "🎯 현재 보유 종목" }
                }]
            }
        }));
        
        if report.positions.is_empty() {
            blocks.push(json!({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{
                        "type": "text",
                        "text": { "content": "보유 종목 없음" }
                    }]
                }
            }));
        } else {
            for pos in &report.positions {
                blocks.push(json!({
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {
                        "rich_text": [{
                            "type": "text",
                            "text": {
                                "content": format!(
                                    "{} ({}) | {}주 @ {:.0}원 | 수익률: {:+.2}%",
                                    pos.symbol, pos.name, pos.quantity,
                                    pos.avg_price, pos.profit_loss_rate
                                )
                            }
                        }]
                    }
                }));
            }
        }
        
        blocks
    }
}


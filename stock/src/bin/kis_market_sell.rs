use stock::korea_investment_api::KoreaInvestmentAPI;
use anyhow::Result;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        println!("Usage: kis_market_sell <stock_code> <quantity>");
        println!("Example: kis_market_sell 403870 1");
        return Ok(());
    }
    
    let stock_code = &args[1];
    let quantity: u32 = args[2].parse()?;
    
    println!("\n🔥 시장가 매도 주문");
    println!("종목코드: {}", stock_code);
    println!("수량: {}주", quantity);
    
    let mut api = KoreaInvestmentAPI::from_env()?;
    api.get_access_token().await?;
    
    // 시장가 매도 (price=0, ORD_DVSN=01)
    let result = api.sell_stock_market(stock_code, quantity).await?;
    
    println!("\n✅ 매도 주문 완료!");
    println!("주문번호: {:?}", result.order_no);
    
    Ok(())
}

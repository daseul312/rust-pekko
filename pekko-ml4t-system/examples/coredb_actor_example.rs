//! CoreDB + Pekko Actor 통합 예제
//! 
//! CoreDB를 사용하여 주식 거래 데이터를 저장하고 조회하는 예제
//! 
//! 실행: cargo run --example coredb_actor_example --features coredb

use std::sync::Arc;
use tokio::sync::Mutex;

// CoreDB 사용 (feature flag 필요)
#[cfg(feature = "coredb")]
use coredb::{CoreDB, DatabaseConfig};

/// 거래 기록 구조체
#[derive(Debug, Clone)]
pub struct TradeRecord {
    pub id: String,
    pub symbol: String,
    pub action: String,  // "BUY" or "SELL"
    pub quantity: i32,
    pub price: f64,
    pub timestamp: String,
}

/// CoreDB 기반 거래 저장소 Actor
#[cfg(feature = "coredb")]
pub struct TradeStorageActor {
    db: Arc<Mutex<CoreDB>>,
}

#[cfg(feature = "coredb")]
impl TradeStorageActor {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        use std::path::PathBuf;
        
        let config = DatabaseConfig {
            data_directory: PathBuf::from("/tmp/coredb_actor_data"),
            commitlog_directory: PathBuf::from("/tmp/coredb_actor_commitlog"),
            ..Default::default()
        };
        
        let db = CoreDB::new(config).await?;
        
        Ok(Self {
            db: Arc::new(Mutex::new(db)),
        })
    }

    /// 데이터베이스 스키마 초기화
    pub async fn initialize(&self) -> Result<(), Box<dyn std::error::Error>> {
        let db = self.db.lock().await;
        
        // 키스페이스 생성 (CoreDB의 간단한 CQL 구문 사용)
        db.execute_cql("CREATE KEYSPACE trading WITH REPLICATION = {'replication_factor': 1}").await?;
        
        // 거래 테이블 생성
        db.execute_cql("CREATE TABLE trading.trades (id TEXT PRIMARY KEY, symbol TEXT, action TEXT, quantity INT, price DOUBLE, timestamp TEXT)").await?;
        
        println!("✅ CoreDB 스키마 초기화 완료!");
        Ok(())
    }

    /// 거래 기록 저장
    pub async fn store_trade(&self, trade: &TradeRecord) -> Result<(), Box<dyn std::error::Error>> {
        let db = self.db.lock().await;
        
        let cql = format!(
            "INSERT INTO trading.trades (id, symbol, action, quantity, price, timestamp) VALUES ('{}', '{}', '{}', {}, {}, '{}')",
            trade.id, trade.symbol, trade.action, trade.quantity, trade.price, trade.timestamp
        );
        
        db.execute_cql(&cql).await?;
        println!("📝 거래 저장: {} {} {}주 @ {:.0}원", trade.symbol, trade.action, trade.quantity, trade.price);
        Ok(())
    }

    /// 모든 거래 조회
    pub async fn get_all_trades(&self) -> Result<String, Box<dyn std::error::Error>> {
        let db = self.db.lock().await;
        let result = db.execute_cql("SELECT * FROM trading.trades").await?;
        Ok(format!("{:?}", result))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 CoreDB + Pekko Actor 통합 예제\n");
    println!("================================================\n");

    #[cfg(feature = "coredb")]
    {
        // 1. Actor 생성 및 초기화
        println!("📦 CoreDB Actor 생성 중...");
        let storage_actor = TradeStorageActor::new().await?;
        storage_actor.initialize().await?;
        println!();

        // 2. 샘플 거래 데이터 생성
        let trades = vec![
            TradeRecord {
                id: "T001".to_string(),
                symbol: "삼성전자".to_string(),
                action: "BUY".to_string(),
                quantity: 10,
                price: 71000.0,
                timestamp: "2026-01-30 10:30:00".to_string(),
            },
            TradeRecord {
                id: "T002".to_string(),
                symbol: "카카오".to_string(),
                action: "BUY".to_string(),
                quantity: 5,
                price: 62000.0,
                timestamp: "2026-01-30 11:00:00".to_string(),
            },
            TradeRecord {
                id: "T003".to_string(),
                symbol: "삼성전자".to_string(),
                action: "SELL".to_string(),
                quantity: 5,
                price: 72000.0,
                timestamp: "2026-01-30 14:30:00".to_string(),
            },
        ];

        // 3. 거래 저장
        println!("📊 거래 기록 저장 중...\n");
        for trade in &trades {
            storage_actor.store_trade(trade).await?;
        }
        println!();

        // 4. 거래 조회
        println!("🔍 전체 거래 조회:");
        let all_trades = storage_actor.get_all_trades().await?;
        println!("   결과: {}", all_trades);
        println!();

        println!("================================================");
        println!("✅ CoreDB + Pekko Actor 통합 예제 완료!");
        println!("================================================");
    }

    #[cfg(not(feature = "coredb"))]
    {
        println!("⚠️ coredb feature가 활성화되지 않았습니다.");
        println!("   실행: cargo run --example coredb_actor_example --features coredb");
    }

    Ok(())
}

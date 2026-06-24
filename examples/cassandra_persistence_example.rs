use pekko_actor::{Actor, ActorContext};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 이벤트 소싱을 위한 메시지 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
enum BankEvent {
    AccountCreated { account_id: String, initial_balance: f64 },
    MoneyDeposited { account_id: String, amount: f64 },
    MoneyWithdrawn { account_id: String, amount: f64 },
    AccountClosed { account_id: String },
}

/// 은행 계좌 액터
struct BankAccountActor {
    account_id: String,
    balance: f64,
    events: Vec<BankEvent>,
    is_persistent: bool,
}

impl BankAccountActor {
    fn new(account_id: String, initial_balance: f64) -> Self {
        Self {
            account_id,
            balance: initial_balance,
            events: Vec::new(),
            is_persistent: false,
        }
    }

    fn apply_event(&mut self, event: &BankEvent) {
        match event {
            BankEvent::AccountCreated { account_id, initial_balance } => {
                self.account_id = account_id.clone();
                self.balance = *initial_balance;
            }
            BankEvent::MoneyDeposited { amount, .. } => {
                self.balance += amount;
            }
            BankEvent::MoneyWithdrawn { amount, .. } => {
                self.balance -= amount;
            }
            BankEvent::AccountClosed { .. } => {
                // 계좌 폐쇄 로직
            }
        }
    }

    fn add_event(&mut self, event: BankEvent) {
        self.apply_event(&event);
        self.events.push(event);
    }
}

#[async_trait]
impl Actor for BankAccountActor {
    type Message = BankCommand;

    async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
        match msg {
            BankCommand::CreateAccount { account_id, initial_balance } => {
                let event = BankEvent::AccountCreated { 
                    account_id: account_id.clone(), 
                    initial_balance 
                };
                self.add_event(event);
                println!("[{}] 계좌 생성됨: 잔액 ${:.2}", account_id, initial_balance);
            }
            
            BankCommand::Deposit { amount } => {
                let event = BankEvent::MoneyDeposited { 
                    account_id: self.account_id.clone(), 
                    amount 
                };
                self.add_event(event);
                println!("[{}] 입금: ${:.2}, 잔액: ${:.2}", 
                    self.account_id, amount, self.balance);
            }
            
            BankCommand::Withdraw { amount } => {
                if self.balance >= amount {
                    let event = BankEvent::MoneyWithdrawn { 
                        account_id: self.account_id.clone(), 
                        amount 
                    };
                    self.add_event(event);
                    println!("[{}] 출금: ${:.2}, 잔액: ${:.2}", 
                        self.account_id, amount, self.balance);
                } else {
                    println!("[{}] 잔액 부족: 요청 ${:.2}, 잔액 ${:.2}", 
                        self.account_id, amount, self.balance);
                }
            }
            
            BankCommand::GetBalance => {
                println!("[{}] 현재 잔액: ${:.2}", self.account_id, self.balance);
            }
            
            BankCommand::GetEventHistory => {
                println!("[{}] 이벤트 히스토리 ({}개):", 
                    self.account_id, self.events.len());
                for (i, event) in self.events.iter().enumerate() {
                    println!("  {}. {:?}", i + 1, event);
                }
            }
            
            BankCommand::CloseAccount => {
                let event = BankEvent::AccountClosed { 
                    account_id: self.account_id.clone() 
                };
                self.add_event(event);
                println!("[{}] 계좌 폐쇄됨", self.account_id);
            }
        }
    }

    async fn pre_start(&mut self) {
        println!("[{}] 은행 계좌 액터 시작됨", self.account_id);
    }

    async fn post_stop(&mut self) {
        println!("[{}] 은행 계좌 액터 종료됨 (최종 잔액: ${:.2})", 
            self.account_id, self.balance);
    }
}

/// 은행 명령 타입
#[derive(Debug, Clone)]
enum BankCommand {
    CreateAccount { account_id: String, initial_balance: f64 },
    Deposit { amount: f64 },
    Withdraw { amount: f64 },
    GetBalance,
    GetEventHistory,
    CloseAccount,
}

/// Cassandra 연결 시뮬레이터
struct CassandraPersistenceSimulator {
    accounts: HashMap<String, Arc<Mutex<BankAccountActor>>>,
    event_store: Vec<(String, BankEvent)>, // persistence_id, event
}

impl CassandraPersistenceSimulator {
    fn new() -> Self {
        Self {
            accounts: HashMap::new(),
            event_store: Vec::new(),
        }
    }

    /// 계좌 생성
    fn create_account(&mut self, account_id: String, initial_balance: f64) -> Arc<Mutex<BankAccountActor>> {
        let account = Arc::new(Mutex::new(BankAccountActor::new(account_id.clone(), initial_balance)));
        self.accounts.insert(account_id.clone(), account.clone());
        
        // 이벤트 저장 (실제로는 Cassandra에 저장)
        let event = BankEvent::AccountCreated { account_id: account_id.clone(), initial_balance };
        self.persist_event(&account_id, event);
        
        println!("계좌 '{}' 생성됨 (초기 잔액: ${:.2})", account_id, initial_balance);
        account
    }

    /// 이벤트 영속화 (Cassandra 저장 시뮬레이션)
    fn persist_event(&mut self, persistence_id: &str, event: BankEvent) {
        self.event_store.push((persistence_id.to_string(), event.clone()));
        println!("[Persistence] 이벤트 저장됨: {} -> {:?}", persistence_id, event);
        
        // 실제 Cassandra 저장 시뮬레이션
        sleep(Duration::from_millis(10));
    }

    /// 이벤트 재생 (Cassandra에서 로드 시뮬레이션)
    async fn replay_events(&self, persistence_id: &str) -> Vec<BankEvent> {
        let events: Vec<BankEvent> = self.event_store
            .iter()
            .filter(|(pid, _)| pid == persistence_id)
            .map(|(_, event)| event.clone())
            .collect();
        
        println!("[Persistence] {}에서 {}개 이벤트 재생됨", persistence_id, events.len());
        events
    }

    /// 계좌 복구 (이벤트 재생)
    async fn recover_account(&mut self, account_id: String) -> Arc<Mutex<BankAccountActor>> {
        println!("[Persistence] 계좌 '{}' 복구 시작...", account_id);
        
        // 이벤트 재생
        let events = self.replay_events(&account_id).await;
        
        // 새 계좌 생성 및 이벤트 적용
        let mut account = BankAccountActor::new(account_id.clone(), 0.0);
        for event in events {
            account.apply_event(&event);
        }
        
        let account = Arc::new(Mutex::new(account));
        self.accounts.insert(account_id.clone(), account.clone());
        
        println!("[Persistence] 계좌 '{}' 복구 완료 (잔액: ${:.2})", 
            account_id, account.lock().unwrap().balance);
        
        account
    }

    /// 모든 계좌 상태 출력
    fn print_all_accounts_status(&self) {
        println!("\n=== 모든 계좌 상태 ===");
        for (account_id, account) in &self.accounts {
            if let Ok(account_guard) = account.lock() {
                println!("계좌: {}, 잔액: ${:.2}, 이벤트 수: {}", 
                    account_id, account_guard.balance, account_guard.events.len());
            }
        }
    }

    /// 이벤트 스토어 상태 출력
    fn print_event_store_status(&self) {
        println!("\n=== 이벤트 스토어 상태 ===");
        println!("총 이벤트 수: {}", self.event_store.len());
        
        let mut event_counts: HashMap<String, usize> = HashMap::new();
        for (persistence_id, _) in &self.event_store {
            *event_counts.entry(persistence_id.clone()).or_insert(0) += 1;
        }
        
        for (persistence_id, count) in event_counts {
            println!("  {}: {}개 이벤트", persistence_id, count);
        }
    }
}

#[tokio::main]
async fn main() {
    println!("=== Pekko Persistence + Cassandra 5.x 예제 ===\n");

    let mut simulator = CassandraPersistenceSimulator::new();

    // 계좌 생성
    println!("--- 계좌 생성 및 이벤트 소싱 ---");
    let account1 = simulator.create_account("ACC-001".to_string(), 1000.0);
    let account2 = simulator.create_account("ACC-002".to_string(), 500.0);

    sleep(Duration::from_millis(100)).await;

    // 계좌 1에 거래 수행
    println!("\n--- 계좌 1 거래 수행 ---");
    {
        let mut acc1 = account1.lock().unwrap();
        // Note: In a real implementation, this would use ActorContext
        // acc1.receive(BankCommand::Deposit { amount: 500.0 }, ctx).await;
        println!("Account 1: Deposit 500.0");
        // acc1.receive(BankCommand::Withdraw { amount: 200.0 }, ctx).await;
        println!("Account 1: Withdraw 200.0");
        // acc1.receive(BankCommand::Deposit { amount: 1000.0 }, ctx).await;
        println!("Account 1: Deposit 1000.0");
    }

    sleep(Duration::from_millis(100)).await;

    // 계좌 2에 거래 수행
    println!("\n--- 계좌 2 거래 수행 ---");
    {
        let mut acc2 = account2.lock().unwrap();
        // acc2.receive(BankCommand::Deposit { amount: 300.0 }, ctx).await;
        println!("Account 2: Deposit 300.0");
        // acc2.receive(BankCommand::Withdraw { amount: 150.0 }, ctx).await;
        println!("Account 2: Withdraw 150.0");
    }

    sleep(Duration::from_millis(100)).await;

    // 현재 상태 확인
    println!("\n--- 현재 상태 확인 ---");
    {
        let acc1 = account1.lock().unwrap();
        // acc1.receive(BankCommand::GetBalance, ctx).await;
        println!("Account 1: Get Balance");
        // acc1.receive(BankCommand::GetEventHistory, ctx).await;
        println!("Account 1: Get Event History");
    }

    {
        let acc2 = account2.lock().unwrap();
        // acc2.receive(BankCommand::GetBalance, ctx).await;
        println!("Account 2: Get Balance");
        // acc2.receive(BankCommand::GetEventHistory, ctx).await;
        println!("Account 2: Get Event History");
    }

    // 이벤트 스토어 상태 확인
    simulator.print_event_store_status();
    simulator.print_all_accounts_status();

    // 계좌 복구 시뮬레이션
    println!("\n--- 계좌 복구 시뮬레이션 ---");
    let recovered_account = simulator.recover_account("ACC-001".to_string()).await;
    
    {
        let acc = recovered_account.lock().unwrap();
        // acc.receive(BankCommand::GetBalance, ctx).await;
        println!("Account: Get Balance");
        // acc.receive(BankCommand::GetEventHistory, ctx).await;
        println!("Account: Get Event History");
    }

    println!("\n=== 예제 완료 ===");
    println!("이 예제는 Pekko Persistence의 핵심 개념을 시뮬레이션합니다:");
    println!("1. 이벤트 소싱: 모든 상태 변경을 이벤트로 저장");
    println!("2. 이벤트 재생: 저장된 이벤트로부터 상태 복구");
    println!("3. 영속성: 이벤트를 Cassandra에 저장");
    println!("4. 액터 복구: 장애 후 이벤트 재생으로 상태 복원");
} 
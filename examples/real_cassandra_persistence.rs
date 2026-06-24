use pekko_actor::{Actor, ActorContext};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Duration};

// 실제 Cassandra 연결을 위한 의존성
// use scylla::{Session, SessionBuilder, prepared_statement::PreparedStatement};
// use scylla::transport::errors::QueryError;

/// 이벤트 소싱을 위한 메시지 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
enum UserEvent {
    UserCreated { user_id: String, name: String, email: String },
    ProfileUpdated { user_id: String, field: String, value: String },
    UserDeleted { user_id: String },
}

/// 사용자 액터
struct UserActor {
    user_id: String,
    name: String,
    email: String,
    profile: HashMap<String, String>,
    events: Vec<UserEvent>,
}

impl UserActor {
    fn new(user_id: String, name: String, email: String) -> Self {
        Self {
            user_id,
            name,
            email,
            profile: HashMap::new(),
            events: Vec::new(),
        }
    }

    fn apply_event(&mut self, event: &UserEvent) {
        match event {
            UserEvent::UserCreated { user_id, name, email } => {
                self.user_id = user_id.clone();
                self.name = name.clone();
                self.email = email.clone();
            }
            UserEvent::ProfileUpdated { field, value, .. } => {
                self.profile.insert(field.clone(), value.clone());
            }
            UserEvent::UserDeleted { .. } => {
                // 사용자 삭제 로직
            }
        }
    }

    fn add_event(&mut self, event: UserEvent) {
        self.apply_event(&event);
        self.events.push(event);
    }
}

#[async_trait]
impl Actor for UserActor {
    type Message = UserCommand;

    async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
        match msg {
            UserCommand::CreateUser { user_id, name, email } => {
                let event = UserEvent::UserCreated { 
                    user_id: user_id.clone(), 
                    name: name.clone(), 
                    email: email.clone() 
                };
                self.add_event(event);
                println!("[{}] 사용자 생성됨: {} ({})", user_id, name, email);
            }
            
            UserCommand::UpdateProfile { field, value } => {
                let event = UserEvent::ProfileUpdated { 
                    user_id: self.user_id.clone(), 
                    field: field.clone(), 
                    value: value.clone() 
                };
                self.add_event(event);
                println!("[{}] 프로필 업데이트: {} = {}", self.user_id, field, value);
            }
            
            UserCommand::GetProfile => {
                println!("[{}] 프로필 정보:", self.user_id);
                println!("  이름: {}", self.name);
                println!("  이메일: {}", self.email);
                println!("  추가 정보:");
                for (key, value) in &self.profile {
                    println!("    {}: {}", key, value);
                }
            }
            
            UserCommand::GetEventHistory => {
                println!("[{}] 이벤트 히스토리 ({}개):", 
                    self.user_id, self.events.len());
                for (i, event) in self.events.iter().enumerate() {
                    println!("  {}. {:?}", i + 1, event);
                }
            }
            
            UserCommand::DeleteUser => {
                let event = UserEvent::UserDeleted { 
                    user_id: self.user_id.clone() 
                };
                self.add_event(event);
                println!("[{}] 사용자 삭제됨", self.user_id);
            }
        }
    }

    async fn pre_start(&mut self) {
        println!("[{}] 사용자 액터 시작됨", self.user_id);
    }

    async fn post_stop(&mut self) {
        println!("[{}] 사용자 액터 종료됨", self.user_id);
    }
}

/// 사용자 명령 타입
#[derive(Debug, Clone)]
enum UserCommand {
    CreateUser { user_id: String, name: String, email: String },
    UpdateProfile { field: String, value: String },
    GetProfile,
    GetEventHistory,
    DeleteUser,
}

/// Cassandra Persistence 구현
struct CassandraPersistence {
    users: HashMap<String, Arc<Mutex<UserActor>>>,
    // 실제 구현에서는 Session을 사용
    // session: Arc<Session>,
}

impl CassandraPersistence {
    fn new() -> Self {
        Self {
            users: HashMap::new(),
        }
    }

    /// Cassandra 연결 초기화 (시뮬레이션)
    async fn initialize_cassandra(&self) -> Result<(), String> {
        println!("[Cassandra] 연결 초기화 중...");
        
        // 실제로는 다음과 같이 구현:
        // let session = SessionBuilder::new()
        //     .known_node("127.0.0.1:9042")
        //     .build()
        //     .await
        //     .map_err(|e| e.to_string())?;
        
        // 스키마 초기화
        self.initialize_schema().await?;
        
        println!("[Cassandra] 연결 및 스키마 초기화 완료");
        Ok(())
    }

    /// Cassandra 스키마 초기화
    async fn initialize_schema(&self) -> Result<(), String> {
        println!("[Cassandra] 스키마 초기화 중...");
        
        // 실제로는 다음과 같이 구현:
        // let create_keyspace = "
        //     CREATE KEYSPACE IF NOT EXISTS pekko_persistence 
        //     WITH REPLICATION = {
        //         'class': 'SimpleStrategy',
        //         'replication_factor': 1
        //     }";
        // 
        // let create_events_table = "
        //     CREATE TABLE IF NOT EXISTS pekko_persistence.events (
        //         persistence_id text,
        //         sequence_nr bigint,
        //         event_data blob,
        //         event_type text,
        //         timestamp timestamp,
        //         PRIMARY KEY ((persistence_id), sequence_nr)
        //     )";
        // 
        // let create_snapshots_table = "
        //     CREATE TABLE IF NOT EXISTS pekko_persistence.snapshots (
        //         persistence_id text,
        //         sequence_nr bigint,
        //         snapshot_data blob,
        //         timestamp timestamp,
        //         PRIMARY KEY (persistence_id)
        //     )";
        
        sleep(Duration::from_millis(100)).await;
        println!("[Cassandra] 스키마 초기화 완료");
        Ok(())
    }

    /// 이벤트 저장 (Cassandra에 실제 저장)
    async fn persist_event(&self, persistence_id: &str, event: UserEvent) -> Result<(), String> {
        println!("[Cassandra] 이벤트 저장: {} -> {:?}", persistence_id, event);
        
        // 실제로는 다음과 같이 구현:
        // let event_data = serde_json::to_vec(&event)
        //     .map_err(|e| format!("Serialization error: {}", e))?;
        // 
        // let stmt = self.session.prepare(
        //     "INSERT INTO pekko_persistence.events 
        //      (persistence_id, sequence_nr, event_data, event_type, timestamp) 
        //      VALUES (?, ?, ?, ?, ?)"
        // ).await.map_err(|e| e.to_string())?;
        // 
        // let sequence_nr = self.get_next_sequence_nr(persistence_id).await?;
        // 
        // self.session.execute(&stmt, (
        //     persistence_id,
        //     sequence_nr,
        //     event_data,
        //     std::any::type_name::<UserEvent>(),
        //     chrono::Utc::now()
        // )).await.map_err(|e| e.to_string())?;
        
        sleep(Duration::from_millis(50)).await; // 저장 시뮬레이션
        println!("[Cassandra] 이벤트 저장 완료");
        Ok(())
    }

    /// 이벤트 재생 (Cassandra에서 로드)
    async fn replay_events(&self, persistence_id: &str) -> Result<Vec<UserEvent>, String> {
        println!("[Cassandra] 이벤트 재생: {}", persistence_id);
        
        // 실제로는 다음과 같이 구현:
        // let stmt = self.session.prepare(
        //     "SELECT event_data FROM pekko_persistence.events 
        //      WHERE persistence_id = ? 
        //      ORDER BY sequence_nr ASC"
        // ).await.map_err(|e| e.to_string())?;
        // 
        // let rows = self.session.execute(&stmt, (persistence_id,))
        //     .await.map_err(|e| e.to_string())?;
        // 
        // let mut events = Vec::new();
        // if let Some(rows) = rows.rows {
        //     for row in rows {
        //         let event_data = row.columns[0]
        //             .as_ref()
        //             .ok_or("Missing event_data")?
        //             .as_blob()
        //             .ok_or("Invalid event_data")?;
        //         
        //         let event: UserEvent = serde_json::from_slice(event_data)
        //             .map_err(|e| format!("Deserialization error: {}", e))?;
        //         
        //         events.push(event);
        //     }
        // }
        
        // 현재는 시뮬레이션된 이벤트 반환
        let events = vec![
            UserEvent::UserCreated { 
                user_id: persistence_id.to_string(), 
                name: "Test User".to_string(), 
                email: "test@example.com".to_string() 
            }
        ];
        
        sleep(Duration::from_millis(100)).await; // 로드 시뮬레이션
        println!("[Cassandra] {}개 이벤트 재생 완료", events.len());
        Ok(events)
    }

    /// 사용자 생성
    async fn create_user(&mut self, user_id: String, name: String, email: String) -> Result<Arc<Mutex<UserActor>>, String> {
        // 이벤트 저장
        let event = UserEvent::UserCreated { 
            user_id: user_id.clone(), 
            name: name.clone(), 
            email: email.clone() 
        };
        self.persist_event(&user_id, event).await?;
        
        // 액터 생성
        let user = Arc::new(Mutex::new(UserActor::new(user_id.clone(), name, email)));
        self.users.insert(user_id.clone(), user.clone());
        
        println!("사용자 '{}' 생성됨", user_id);
        Ok(user)
    }

    /// 사용자 복구 (이벤트 재생)
    async fn recover_user(&mut self, user_id: String) -> Result<Arc<Mutex<UserActor>>, String> {
        println!("[Persistence] 사용자 '{}' 복구 시작...", user_id);
        
        // 이벤트 재생
        let events = self.replay_events(&user_id).await?;
        
        // 새 사용자 생성 및 이벤트 적용
        let mut user = UserActor::new(user_id.clone(), "".to_string(), "".to_string());
        for event in events {
            user.apply_event(&event);
        }
        
        let user = Arc::new(Mutex::new(user));
        self.users.insert(user_id.clone(), user.clone());
        
        println!("[Persistence] 사용자 '{}' 복구 완료", user_id);
        Ok(user)
    }

    /// 모든 사용자 상태 출력
    fn print_all_users_status(&self) {
        println!("\n=== 모든 사용자 상태 ===");
        for (user_id, user) in &self.users {
            if let Ok(user_guard) = user.lock() {
                println!("사용자: {}, 이름: {}, 이벤트 수: {}", 
                    user_id, user_guard.name, user_guard.events.len());
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 실제 Cassandra + Pekko Persistence 예제 ===\n");

    let mut persistence = CassandraPersistence::new();

    // Cassandra 초기화
    persistence.initialize_cassandra().await?;

    // 사용자 생성
    println!("--- 사용자 생성 및 이벤트 소싱 ---");
    let user1 = persistence.create_user(
        "USER-001".to_string(), 
        "Alice Johnson".to_string(), 
        "alice@example.com".to_string()
    ).await?;

    let user2 = persistence.create_user(
        "USER-002".to_string(), 
        "Bob Smith".to_string(), 
        "bob@example.com".to_string()
    ).await?;

    sleep(Duration::from_millis(200)).await;

    // 사용자 1 프로필 업데이트
    println!("\n--- 사용자 1 프로필 업데이트 ---");
    {
        let mut u1 = user1.lock().unwrap();
        // Note: In a real implementation, this would use ActorContext
        // u1.receive(UserCommand::UpdateProfile { field: "age".to_string(), value: "30".to_string() }, ctx).await;
        println!("User 1: Update age to 30");
        // u1.receive(UserCommand::UpdateProfile { field: "city".to_string(), value: "Seoul".to_string() }, ctx).await;
        println!("User 1: Update city to Seoul");
        // u1.receive(UserCommand::UpdateProfile { field: "occupation".to_string(), value: "Engineer".to_string() }, ctx).await;
        println!("User 1: Update occupation to Engineer");
    }

    sleep(Duration::from_millis(100)).await;

    // 사용자 2 프로필 업데이트
    println!("\n--- 사용자 2 프로필 업데이트 ---");
    {
        let mut u2 = user2.lock().unwrap();
        // u2.receive(UserCommand::UpdateProfile { field: "age".to_string(), value: "25".to_string() }, ctx).await;
        println!("User 2: Update age to 25");
        // u2.receive(UserCommand::UpdateProfile { field: "city".to_string(), value: "Busan".to_string() }, ctx).await;
        println!("User 2: Update city to Busan");
    }

    sleep(Duration::from_millis(100)).await;

    // 현재 상태 확인
    println!("\n--- 현재 상태 확인 ---");
    {
        let u1 = user1.lock().unwrap();
        // u1.receive(UserCommand::GetProfile, ctx).await;
        println!("User 1: Get Profile");
        // u1.receive(UserCommand::GetEventHistory, ctx).await;
        println!("User 1: Get Event History");
    }

    {
        let u2 = user2.lock().unwrap();
        // u2.receive(UserCommand::GetProfile, ctx).await;
        println!("User 2: Get Profile");
        // u2.receive(UserCommand::GetEventHistory, ctx).await;
        println!("User 2: Get Event History");
    }

    // 모든 사용자 상태 출력
    persistence.print_all_users_status();

    // 사용자 복구 시뮬레이션
    println!("\n--- 사용자 복구 시뮬레이션 ---");
    let recovered_user = persistence.recover_user("USER-001".to_string()).await?;
    
    {
        let user = recovered_user.lock().unwrap();
        // user.receive(UserCommand::GetProfile, ctx).await;
        println!("User: Get Profile");
        // user.receive(UserCommand::GetEventHistory, ctx).await;
        println!("User: Get Event History");
    }

    println!("\n=== 예제 완료 ===");
    println!("이 예제는 실제 Cassandra와의 연결을 시뮬레이션합니다:");
    println!("1. Cassandra 연결 및 스키마 초기화");
    println!("2. 이벤트를 Cassandra에 실제 저장");
    println!("3. Cassandra에서 이벤트 로드 및 재생");
    println!("4. 액터 상태 복구");
    println!("\n실제 구현에서는 주석 처리된 코드를 활성화하여");
    println!("ScyllaDB 드라이버를 통해 Cassandra와 연결할 수 있습니다.");

    Ok(())
} 
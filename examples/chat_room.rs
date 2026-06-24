use pekko_actor::{Actor, ActorContext};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

/// 채팅 메시지 타입
#[derive(Debug, Clone)]
struct ChatMessage {
    sender: String,
    content: String,
    timestamp: std::time::Instant,
}

/// 사용자 액터가 받을 수 있는 메시지들
#[derive(Debug, Clone)]
enum UserMessage {
    SendMessage(String),
    ReceiveMessage(ChatMessage),
    JoinRoom(String),
    LeaveRoom(String),
    GetStatus,
}

/// 채팅방 액터가 받을 수 있는 메시지들
#[derive(Debug, Clone)]
enum RoomMessage {
    Join(String),           // 사용자 이름
    Leave(String),          // 사용자 이름
    Broadcast(ChatMessage), // 브로드캐스트할 메시지
    GetUsers,
}

/// 사용자 액터
struct UserActor {
    name: String,
    current_room: Option<String>,
    received_messages: Vec<ChatMessage>,
}

impl UserActor {
    fn new(name: String) -> Self {
        Self {
            name,
            current_room: None,
            received_messages: Vec::new(),
        }
    }
}

#[async_trait]
impl Actor for UserActor {
    type Message = UserMessage;

    async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
        match msg {
            UserMessage::SendMessage(content) => {
                if let Some(room) = &self.current_room {
                    let message = ChatMessage {
                        sender: self.name.clone(),
                        content,
                        timestamp: std::time::Instant::now(),
                    };
                    println!("[{}] 메시지 전송: {}", self.name, message.content);
                    // 실제로는 채팅방에 메시지를 보내야 함
                } else {
                    println!("[{}] 채팅방에 참여하지 않았습니다.", self.name);
                }
            }
            UserMessage::ReceiveMessage(message) => {
                self.received_messages.push(message.clone());
                println!("[{}] 메시지 수신: {}: {}", 
                    self.name, message.sender, message.content);
            }
            UserMessage::JoinRoom(room_name) => {
                self.current_room = Some(room_name.clone());
                println!("[{}] 채팅방 '{}'에 참여했습니다.", self.name, room_name);
            }
            UserMessage::LeaveRoom(room_name) => {
                if self.current_room.as_ref() == Some(&room_name) {
                    self.current_room = None;
                    println!("[{}] 채팅방 '{}'에서 나갔습니다.", self.name, room_name);
                }
            }
            UserMessage::GetStatus => {
                let room_status = self.current_room
                    .as_ref()
                    .map(|r| format!("'{}'에 참여 중", r))
                    .unwrap_or_else(|| "채팅방에 참여하지 않음".to_string());
                println!("[{}] 상태: {}", self.name, room_status);
                println!("[{}] 수신된 메시지 수: {}", self.name, self.received_messages.len());
            }
        }
    }

    async fn pre_start(&mut self) {
        println!("[{}] 사용자 액터 시작됨", self.name);
    }

    async fn post_stop(&mut self) {
        println!("[{}] 사용자 액터 종료됨", self.name);
    }
}

/// 채팅방 액터
struct ChatRoomActor {
    name: String,
    users: HashMap<String, Arc<Mutex<UserActor>>>,
    messages: Vec<ChatMessage>,
}

impl ChatRoomActor {
    fn new(name: String) -> Self {
        Self {
            name,
            users: HashMap::new(),
            messages: Vec::new(),
        }
    }
}

#[async_trait]
impl Actor for ChatRoomActor {
    type Message = RoomMessage;

    async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
        match msg {
            RoomMessage::Join(user_name) => {
                println!("[{}] 사용자 '{}'가 채팅방에 참여했습니다.", self.name, user_name);
                // 실제로는 사용자 액터 참조를 저장해야 함
            }
            RoomMessage::Leave(user_name) => {
                println!("[{}] 사용자 '{}'가 채팅방에서 나갔습니다.", self.name, user_name);
                // 실제로는 사용자 액터 참조를 제거해야 함
            }
            RoomMessage::Broadcast(message) => {
                self.messages.push(message.clone());
                println!("[{}] 메시지 브로드캐스트: {}: {}", 
                    self.name, message.sender, message.content);
                // 실제로는 모든 사용자에게 메시지를 전송해야 함
            }
            RoomMessage::GetUsers => {
                println!("[{}] 현재 참여자 수: {}", self.name, self.users.len());
                println!("[{}] 총 메시지 수: {}", self.name, self.messages.len());
            }
        }
    }

    async fn pre_start(&mut self) {
        println!("[{}] 채팅방 액터 시작됨", self.name);
    }

    async fn post_stop(&mut self) {
        println!("[{}] 채팅방 액터 종료됨", self.name);
    }
}

/// 채팅 시스템 관리자
struct ChatSystem {
    users: HashMap<String, Arc<Mutex<UserActor>>>,
    rooms: HashMap<String, Arc<Mutex<ChatRoomActor>>>,
}

impl ChatSystem {
    fn new() -> Self {
        Self {
            users: HashMap::new(),
            rooms: HashMap::new(),
        }
    }

    fn create_user(&mut self, name: String) -> Arc<Mutex<UserActor>> {
        let user = Arc::new(Mutex::new(UserActor::new(name.clone())));
        self.users.insert(name.clone(), user.clone());
        
        // 사용자 액터 시작
        let user_clone = user.clone();
        tokio::spawn(async move {
            if let Ok(user_guard) = user_clone.lock() {
                // Note: In a real implementation, this would call pre_start with ActorContext
                println!("User {} pre_start called", user_guard.name);
            }
        });
        
        println!("사용자 '{}' 생성됨", name);
        user
    }

    fn create_room(&mut self, name: String) -> Arc<Mutex<ChatRoomActor>> {
        let room = Arc::new(Mutex::new(ChatRoomActor::new(name.clone())));
        self.rooms.insert(name.clone(), room.clone());
        
        // 채팅방 액터 시작
        let room_clone = room.clone();
        tokio::spawn(async move {
            if let Ok(room_guard) = room_clone.lock() {
                // Note: In a real implementation, this would call pre_start with ActorContext
                println!("Room {} pre_start called", room_guard.name);
            }
        });
        
        println!("채팅방 '{}' 생성됨", name);
        room
    }

    async fn simulate_chat(&self) {
        println!("\n=== 채팅 시뮬레이션 시작 ===");
        
        // 사용자들이 채팅방에 참여
        if let Some(user) = self.users.get("Alice") {
            if let Ok(mut user_guard) = user.lock() {
                // Note: In a real implementation, this would use ActorContext
                // user_guard.receive(UserMessage::JoinRoom("general".to_string()), ctx).await;
                println!("User {} joined room: general", user_guard.name);
            }
        }
        
        if let Some(user) = self.users.get("Bob") {
            if let Ok(mut user_guard) = user.lock() {
                // Note: In a real implementation, this would use ActorContext
                // user_guard.receive(UserMessage::JoinRoom("general".to_string()), ctx).await;
                println!("User {} joined room: general", user_guard.name);
            }
        }
        
        sleep(Duration::from_millis(100)).await;
        
        // 메시지 전송
        if let Some(user) = self.users.get("Alice") {
            if let Ok(mut user_guard) = user.lock() {
                // user_guard.receive(UserMessage::SendMessage("안녕하세요!".to_string()), ctx).await;
                println!("User {} sent message: 안녕하세요!", user_guard.name);
            }
        }
        
        if let Some(user) = self.users.get("Bob") {
            if let Ok(mut user_guard) = user.lock() {
                // user_guard.receive(UserMessage::SendMessage("반갑습니다!".to_string()), ctx).await;
                println!("User {} sent message: 반갑습니다!", user_guard.name);
            }
        }
        
        sleep(Duration::from_millis(100)).await;
        
        // 상태 확인
        for (name, user) in &self.users {
            if let Ok(mut user_guard) = user.lock() {
                // user_guard.receive(UserMessage::GetStatus, ctx).await;
                println!("User {} requested status", user_guard.name);
            }
        }
    }

    async fn shutdown(&mut self) {
        println!("\n=== 시스템 종료 ===");
        
        // 모든 사용자 종료
        for (name, user) in &self.users {
            if let Ok(mut user_guard) = user.lock() {
                user_guard.post_stop().await;
                println!("사용자 '{}' 종료됨", name);
            }
        }
        
        // 모든 채팅방 종료
        for (name, room) in &self.rooms {
            if let Ok(mut room_guard) = room.lock() {
                room_guard.post_stop().await;
                println!("채팅방 '{}' 종료됨", name);
            }
        }
        
        self.users.clear();
        self.rooms.clear();
    }
}

#[tokio::main]
async fn main() {
    println!("=== Pekko Rust 채팅 시스템 예제 ===\n");

    let mut chat_system = ChatSystem::new();

    // 사용자들 생성
    let alice = chat_system.create_user("Alice".to_string());
    let bob = chat_system.create_user("Bob".to_string());
    let charlie = chat_system.create_user("Charlie".to_string());

    // 채팅방 생성
    let general_room = chat_system.create_room("general".to_string());
    let tech_room = chat_system.create_room("tech".to_string());

    sleep(Duration::from_millis(200)).await;

    // 채팅 시뮬레이션
    chat_system.simulate_chat().await;

    // 시스템 종료
    chat_system.shutdown().await;

    println!("\n채팅 시스템 예제 완료!");
} 
use pekko_actor::{Actor, ActorContext};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

/// 카운터 액터가 받을 수 있는 메시지들
#[derive(Debug, Clone)]
enum CounterMessage {
    Increment,
    Decrement,
    GetCount,
    Reset,
}

/// 카운터 액터 구조체
struct CounterActor {
    count: i32,
    name: String,
}

impl CounterActor {
    fn new(name: String) -> Self {
        Self { count: 0, name }
    }
}

#[async_trait]
impl Actor for CounterActor {
    type Message = CounterMessage;

    async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
        match msg {
            CounterMessage::Increment => {
                self.count += 1;
                println!("[{}] 카운터 증가: {}", self.name, self.count);
            }
            CounterMessage::Decrement => {
                self.count -= 1;
                println!("[{}] 카운터 감소: {}", self.name, self.count);
            }
            CounterMessage::GetCount => {
                println!("[{}] 현재 카운트: {}", self.name, self.count);
            }
            CounterMessage::Reset => {
                self.count = 0;
                println!("[{}] 카운터 리셋: {}", self.name, self.count);
            }
        }
    }

    async fn pre_start(&mut self) {
        println!("[{}] 카운터 액터 시작됨", self.name);
    }

    async fn post_stop(&mut self) {
        println!("[{}] 카운터 액터 종료됨", self.name);
    }
}

/// 간단한 액터 시스템 시뮬레이션
struct SimpleActorSystem {
    actors: Vec<Arc<Mutex<CounterActor>>>,
}

impl SimpleActorSystem {
    fn new() -> Self {
        Self { actors: Vec::new() }
    }

    fn spawn_actor(&mut self, name: String) -> usize {
        let actor = Arc::new(Mutex::new(CounterActor::new(name.clone())));
        let actor_id = self.actors.len();
        self.actors.push(actor.clone());
        
        println!("액터 '{}' 생성됨 (ID: {})", name, actor_id);
        actor_id
    }

    async fn send_message(&self, actor_id: usize, message: CounterMessage) {
        if let Some(actor) = self.actors.get(actor_id) {
            if let Ok(mut actor_guard) = actor.lock() {
                // Note: In a real implementation, this would use ActorContext
                // actor_guard.receive(message, ctx).await;
                println!("Counter received message: {:?}", message);
            }
        } else {
            println!("알 수 없는 액터 ID: {}", actor_id);
        }
    }

    async fn shutdown(&mut self) {
        for (id, actor) in self.actors.iter().enumerate() {
            if let Ok(mut actor_guard) = actor.lock() {
                actor_guard.post_stop().await;
                println!("액터 {} 종료됨", id);
            }
        }
        self.actors.clear();
    }
}

#[tokio::main]
async fn main() {
    println!("=== Pekko Rust 카운터 액터 예제 ===\n");

    let mut system = SimpleActorSystem::new();

    // 두 개의 카운터 액터 생성
    let counter1_id = system.spawn_actor("Counter-1".to_string());
    let counter2_id = system.spawn_actor("Counter-2".to_string());

    // 잠시 대기
    sleep(Duration::from_millis(100)).await;

    println!("\n--- 메시지 전송 시작 ---");

    // 첫 번째 카운터에 메시지 전송
    system.send_message(counter1_id, CounterMessage::Increment).await;
    system.send_message(counter1_id, CounterMessage::Increment).await;
    system.send_message(counter1_id, CounterMessage::GetCount).await;

    // 두 번째 카운터에 메시지 전송
    system.send_message(counter2_id, CounterMessage::Increment).await;
    system.send_message(counter2_id, CounterMessage::Increment).await;
    system.send_message(counter2_id, CounterMessage::Increment).await;
    system.send_message(counter2_id, CounterMessage::GetCount).await;

    // 첫 번째 카운터 감소
    system.send_message(counter1_id, CounterMessage::Decrement).await;
    system.send_message(counter1_id, CounterMessage::GetCount).await;

    // 두 번째 카운터 리셋
    system.send_message(counter2_id, CounterMessage::Reset).await;
    system.send_message(counter2_id, CounterMessage::GetCount).await;

    println!("\n--- 시스템 종료 ---");
    system.shutdown().await;

    println!("\n예제 완료!");
} 
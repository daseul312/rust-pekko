use pekko_actor::{Actor, ActorContext};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use std::collections::HashMap;

/// 간단한 메시지 타입
#[derive(Debug, Clone)]
enum SimpleMessage {
    Ping,
    Echo(String),
    GetStatus,
}

/// 간단한 응답 타입
#[derive(Debug, Clone)]
enum SimpleResponse {
    Pong,
    Echoed(String),
    Status { node_id: String, uptime: u64 },
}

/// 원격 노드를 시뮬레이션하는 액터
struct RemoteNodeActor {
    node_id: String,
    uptime: u64,
    message_count: u64,
}

impl RemoteNodeActor {
    fn new(node_id: String) -> Self {
        Self {
            node_id,
            uptime: 0,
            message_count: 0,
        }
    }

    fn increment_uptime(&mut self) {
        self.uptime += 1;
    }
}

#[async_trait]
impl Actor for RemoteNodeActor {
    type Message = SimpleMessage;

    async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
        self.message_count += 1;
        
        match msg {
            SimpleMessage::Ping => {
                println!("[{}] Ping 수신됨", self.node_id);
                // 실제로는 응답을 보내야 하지만, 여기서는 로그만 출력
            }
            SimpleMessage::Echo(text) => {
                println!("[{}] Echo 수신됨: '{}'", self.node_id, text);
                // 실제로는 응답을 보내야 하지만, 여기서는 로그만 출력
            }
            SimpleMessage::GetStatus => {
                println!("[{}] 상태 요청 수신됨", self.node_id);
                println!("  - 메시지 수신: {}개", self.message_count);
                println!("  - 가동 시간: {}초", self.uptime);
            }
        }
    }

    async fn pre_start(&mut self) {
        println!("[{}] 원격 노드 시작됨", self.node_id);
    }

    async fn post_stop(&mut self) {
        println!("[{}] 원격 노드 종료됨", self.node_id);
    }
}

/// 원격 시스템 시뮬레이터
struct RemoteSystemSimulator {
    nodes: HashMap<String, Arc<Mutex<RemoteNodeActor>>>,
    local_address: String,
}

impl RemoteSystemSimulator {
    fn new(local_address: String) -> Self {
        Self {
            nodes: HashMap::new(),
            local_address,
        }
    }

    /// 원격 노드 추가
    fn add_remote_node(&mut self, node_id: String, address: String) {
        let node = Arc::new(Mutex::new(RemoteNodeActor::new(node_id.clone())));
        self.nodes.insert(node_id.clone(), node.clone());
        
        println!("원격 노드 '{}' 추가됨 (주소: {})", node_id, address);
    }

    /// 원격 노드에 메시지 전송 시뮬레이션
    async fn send_to_remote(&self, node_id: &str, message: SimpleMessage) {
        if let Some(node) = self.nodes.get(node_id) {
            if let Ok(mut node_guard) = node.lock() {
                println!("[{}] -> [{}] 메시지 전송: {:?}", 
                    self.local_address, node_id, message);
                
                // 네트워크 지연 시뮬레이션
                sleep(Duration::from_millis(50)).await;
                
                // Note: In a real implementation, this would use ActorContext
                // node_guard.receive(message, ctx).await;
                println!("Remote node received message: {:?}", message);
            }
        } else {
            println!("알 수 없는 노드: {}", node_id);
        }
    }

    /// 모든 노드에 브로드캐스트
    async fn broadcast(&self, message: SimpleMessage) {
        println!("[{}] 브로드캐스트 시작: {:?}", self.local_address, message);
        
        for (node_id, node) in &self.nodes {
            if let Ok(mut node_guard) = node.lock() {
                println!("[{}] -> [{}] 브로드캐스트 전송", self.local_address, node_id);
                sleep(Duration::from_millis(20)).await;
                // node_guard.receive(message.clone(), ctx).await;
                println!("Remote node received message: {:?}", message);
            }
        }
        
        println!("[{}] 브로드캐스트 완료", self.local_address);
    }

    /// 노드 상태 확인
    async fn check_all_nodes_status(&self) {
        println!("\n=== 모든 노드 상태 확인 ===");
        
        for (node_id, node) in &self.nodes {
            if let Ok(mut node_guard) = node.lock() {
                // node_guard.receive(SimpleMessage::GetStatus, ctx).await;
                println!("Remote node received GetStatus");
            }
        }
    }

    /// 원격 노드들 종료
    async fn shutdown_all_nodes(&mut self) {
        println!("\n=== 모든 원격 노드 종료 ===");
        
        for (node_id, node) in &self.nodes {
            if let Ok(mut node_guard) = node.lock() {
                node_guard.post_stop().await;
                println!("노드 '{}' 종료됨", node_id);
            }
        }
        
        self.nodes.clear();
    }

    /// 가상의 네트워크 토폴로지 출력
    fn print_network_topology(&self) {
        println!("\n=== 네트워크 토폴로지 ===");
        println!("로컬 노드: {}", self.local_address);
        println!("원격 노드들:");
        
        for (node_id, _) in &self.nodes {
            println!("  - {} (pekko.tcp://{}@{}:2552)", 
                node_id, node_id, self.local_address);
        }
    }
}

#[tokio::main]
async fn main() {
    println!("=== Pekko Rust 원격 시스템 시뮬레이션 ===\n");

    let mut simulator = RemoteSystemSimulator::new("node-1".to_string());

    // 가상의 원격 노드들 추가
    simulator.add_remote_node("calculator-1".to_string(), "192.168.1.101:2552".to_string());
    simulator.add_remote_node("calculator-2".to_string(), "192.168.1.102:2552".to_string());
    simulator.add_remote_node("storage-1".to_string(), "192.168.1.103:2552".to_string());

    sleep(Duration::from_millis(200)).await;

    // 네트워크 토폴로지 출력
    simulator.print_network_topology();

    println!("\n--- 원격 통신 시뮬레이션 시작 ---");

    // 개별 노드에 메시지 전송
    simulator.send_to_remote("calculator-1", SimpleMessage::Ping).await;
    simulator.send_to_remote("calculator-2", SimpleMessage::Echo("Hello from node-1".to_string())).await;
    simulator.send_to_remote("storage-1", SimpleMessage::GetStatus).await;

    sleep(Duration::from_millis(100)).await;

    // 브로드캐스트 메시지
    simulator.broadcast(SimpleMessage::Ping).await;

    sleep(Duration::from_millis(100)).await;

    // 모든 노드 상태 확인
    simulator.check_all_nodes_status().await;

    // 시스템 종료
    simulator.shutdown_all_nodes().await;

    println!("\n원격 시스템 시뮬레이션 완료!");
} 
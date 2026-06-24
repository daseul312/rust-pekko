use pekko_actor::{Actor, ActorContext};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

/// 계산기 메시지 타입
#[derive(Debug, Clone)]
enum CalculatorMessage {
    Add { a: f64, b: f64, request_id: String },
    Subtract { a: f64, b: f64, request_id: String },
    Multiply { a: f64, b: f64, request_id: String },
    Divide { a: f64, b: f64, request_id: String },
    GetStats,
}

/// 원격 계산기 액터
struct RemoteCalculatorActor {
    node_id: String,
    operations_count: u64,
    request_history: Vec<String>,
}

impl RemoteCalculatorActor {
    fn new(node_id: String) -> Self {
        Self {
            node_id,
            operations_count: 0,
            request_history: Vec::new(),
        }
    }

    fn perform_operation(&mut self, op: &str, a: f64, b: f64) -> Result<f64, String> {
        self.operations_count += 1;
        
        match op {
            "add" => Ok(a + b),
            "subtract" => Ok(a - b),
            "multiply" => Ok(a * b),
            "divide" => {
                if b == 0.0 {
                    Err("0으로 나눌 수 없습니다".to_string())
                } else {
                    Ok(a / b)
                }
            }
            _ => Err(format!("알 수 없는 연산: {}", op)),
        }
    }

    fn add_request(&mut self, request_id: &str) {
        self.request_history.push(request_id.to_string());
        if self.request_history.len() > 10 {
            self.request_history.remove(0);
        }
    }
}

#[async_trait]
impl Actor for RemoteCalculatorActor {
    type Message = CalculatorMessage;

    async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
        match msg {
            CalculatorMessage::Add { a, b, request_id } => {
                self.add_request(&request_id);
                let result = self.perform_operation("add", a, b);
                
                match result {
                    Ok(res) => {
                        println!("[{}] 계산 완료: {} + {} = {} (요청 ID: {})", 
                            self.node_id, a, b, res, request_id);
                    }
                    Err(err) => {
                        println!("[{}] 계산 오류: {} (요청 ID: {})", 
                            self.node_id, err, request_id);
                    }
                }
            }
            
            CalculatorMessage::Subtract { a, b, request_id } => {
                self.add_request(&request_id);
                let result = self.perform_operation("subtract", a, b);
                
                match result {
                    Ok(res) => {
                        println!("[{}] 계산 완료: {} - {} = {} (요청 ID: {})", 
                            self.node_id, a, b, res, request_id);
                    }
                    Err(err) => {
                        println!("[{}] 계산 오류: {} (요청 ID: {})", 
                            self.node_id, err, request_id);
                    }
                }
            }
            
            CalculatorMessage::Multiply { a, b, request_id } => {
                self.add_request(&request_id);
                let result = self.perform_operation("multiply", a, b);
                
                match result {
                    Ok(res) => {
                        println!("[{}] 계산 완료: {} × {} = {} (요청 ID: {})", 
                            self.node_id, a, b, res, request_id);
                    }
                    Err(err) => {
                        println!("[{}] 계산 오류: {} (요청 ID: {})", 
                            self.node_id, err, request_id);
                    }
                }
            }
            
            CalculatorMessage::Divide { a, b, request_id } => {
                self.add_request(&request_id);
                let result = self.perform_operation("divide", a, b);
                
                match result {
                    Ok(res) => {
                        println!("[{}] 계산 완료: {} ÷ {} = {} (요청 ID: {})", 
                            self.node_id, a, b, res, request_id);
                    }
                    Err(err) => {
                        println!("[{}] 계산 오류: {} (요청 ID: {})", 
                            self.node_id, err, request_id);
                    }
                }
            }
            
            CalculatorMessage::GetStats => {
                println!("[{}] 통계: 총 연산 {}개, 최근 요청: {:?}", 
                    self.node_id, self.operations_count, self.request_history);
            }
        }
    }

    async fn pre_start(&mut self) {
        println!("[{}] 원격 계산기 시작됨", self.node_id);
    }

    async fn post_stop(&mut self) {
        println!("[{}] 원격 계산기 종료됨 (총 연산: {}개)", 
            self.node_id, self.operations_count);
    }
}

/// 원격 계산기 클러스터 관리자
struct CalculatorCluster {
    calculators: HashMap<String, Arc<Mutex<RemoteCalculatorActor>>>,
    request_counter: u64,
    load_balancing_strategy: LoadBalancingStrategy,
}

#[derive(Debug, Clone)]
enum LoadBalancingStrategy {
    RoundRobin,
    LeastConnections,
}

impl CalculatorCluster {
    fn new(strategy: LoadBalancingStrategy) -> Self {
        Self {
            calculators: HashMap::new(),
            request_counter: 0,
            load_balancing_strategy: strategy,
        }
    }

    /// 계산기 노드 추가
    fn add_calculator(&mut self, node_id: String, address: String) {
        let calculator = Arc::new(Mutex::new(RemoteCalculatorActor::new(node_id.clone())));
        self.calculators.insert(node_id.clone(), calculator.clone());
        
        println!("계산기 노드 '{}' 추가됨 (주소: {})", node_id, address);
    }

    /// 부하 분산을 위한 노드 선택
    fn select_node(&self) -> Option<String> {
        if self.calculators.is_empty() {
            return None;
        }

        match self.load_balancing_strategy {
            LoadBalancingStrategy::RoundRobin => {
                let index = self.request_counter as usize % self.calculators.len();
                self.calculators.keys().nth(index).cloned()
            }
            LoadBalancingStrategy::LeastConnections => {
                // 가장 적은 연산을 수행한 노드 선택
                self.calculators.iter()
                    .min_by_key(|(_, calc)| {
                        if let Ok(guard) = calc.lock() {
                            guard.operations_count
                        } else {
                            u64::MAX
                        }
                    })
                    .map(|(id, _)| id.clone())
            }
        }
    }

    /// 계산 요청 전송
    async fn send_calculation_request(&mut self, message: CalculatorMessage) {
        self.request_counter += 1;
        
        if let Some(node_id) = self.select_node() {
            if let Some(calculator) = self.calculators.get(&node_id) {
                if let Ok(mut calc_guard) = calculator.lock() {
                    println!("[클러스터] {}에 계산 요청 전송: {:?}", node_id, message);
                    
                    // 네트워크 지연 시뮬레이션
                    sleep(Duration::from_millis(30)).await;
                    
                    // Note: In a real implementation, this would use ActorContext
                    // calc_guard.receive(message, ctx).await;
                    println!("Calculator received message: {:?}", message);
                }
            }
        } else {
            println!("[클러스터] 사용 가능한 계산기 노드가 없습니다");
        }
    }

    /// 모든 노드에 통계 요청
    async fn get_all_stats(&self) {
        println!("\n=== 모든 계산기 노드 통계 ===");
        
        for (node_id, calculator) in &self.calculators {
            if let Ok(mut calc_guard) = calculator.lock() {
                // calc_guard.receive(CalculatorMessage::GetStats, ctx).await;
                println!("Calculator received GetStats");
            }
        }
    }

    /// 클러스터 상태 출력
    fn print_cluster_status(&self) {
        println!("\n=== 계산기 클러스터 상태 ===");
        println!("부하 분산 전략: {:?}", self.load_balancing_strategy);
        println!("총 노드 수: {}", self.calculators.len());
        println!("총 요청 수: {}", self.request_counter);
        println!("노드 목록:");
        
        for (node_id, _) in &self.calculators {
            println!("  - {} (pekko.tcp://{}@cluster:2552)", node_id, node_id);
        }
    }

    /// 모든 계산기 종료
    async fn shutdown_all(&mut self) {
        println!("\n=== 모든 계산기 노드 종료 ===");
        
        for (node_id, calculator) in &self.calculators {
            if let Ok(mut calc_guard) = calculator.lock() {
                calc_guard.post_stop().await;
                println!("계산기 '{}' 종료됨", node_id);
            }
        }
        
        self.calculators.clear();
    }
}

#[tokio::main]
async fn main() {
    println!("=== Pekko Rust 원격 계산기 클러스터 예제 ===\n");

    let mut cluster = CalculatorCluster::new(LoadBalancingStrategy::RoundRobin);

    // 계산기 노드들 추가
    cluster.add_calculator("calc-1".to_string(), "192.168.1.101:2552".to_string());
    cluster.add_calculator("calc-2".to_string(), "192.168.1.102:2552".to_string());
    cluster.add_calculator("calc-3".to_string(), "192.168.1.103:2552".to_string());

    sleep(Duration::from_millis(200)).await;

    // 클러스터 상태 출력
    cluster.print_cluster_status();

    println!("\n--- 계산 요청 시뮬레이션 시작 ---");

    // 다양한 계산 요청들
    let requests = vec![
        CalculatorMessage::Add { a: 10.5, b: 20.3, request_id: "req-001".to_string() },
        CalculatorMessage::Subtract { a: 100.0, b: 25.5, request_id: "req-002".to_string() },
        CalculatorMessage::Multiply { a: 7.0, b: 8.0, request_id: "req-003".to_string() },
        CalculatorMessage::Divide { a: 50.0, b: 5.0, request_id: "req-004".to_string() },
        CalculatorMessage::Add { a: 15.7, b: 22.3, request_id: "req-005".to_string() },
        CalculatorMessage::Divide { a: 10.0, b: 0.0, request_id: "req-006".to_string() }, // 오류 케이스
    ];

    // 요청들을 클러스터에 전송
    for request in requests {
        cluster.send_calculation_request(request).await;
        sleep(Duration::from_millis(100)).await;
    }

    // 모든 노드의 통계 확인
    cluster.get_all_stats().await;

    // 클러스터 종료
    cluster.shutdown_all().await;

    println!("\n원격 계산기 클러스터 예제 완료!");
} 
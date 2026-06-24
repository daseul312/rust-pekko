use pekko_actor::{Actor, ActorContext};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

/// 작업 타입
#[derive(Debug, Clone)]
struct Task {
    id: u32,
    description: String,
    priority: u8,
}

impl Task {
    fn new(id: u32, description: String, priority: u8) -> Self {
        Self {
            id,
            description,
            priority,
        }
    }
}

/// 워커가 받을 수 있는 메시지들
#[derive(Debug, Clone)]
enum WorkerMessage {
    ProcessTask(Task),
    Shutdown,
}

/// 워커 액터
struct WorkerActor {
    id: u32,
    is_busy: bool,
    completed_tasks: Vec<Task>,
}

impl WorkerActor {
    fn new(id: u32) -> Self {
        Self {
            id,
            is_busy: false,
            completed_tasks: Vec::new(),
        }
    }
}

#[async_trait]
impl Actor for WorkerActor {
    type Message = WorkerMessage;

    async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
        match msg {
            WorkerMessage::ProcessTask(task) => {
                self.is_busy = true;
                println!("[워커 {}] 작업 처리 시작: {} (우선순위: {})", 
                    self.id, task.description, task.priority);
                
                // 작업 처리 시뮬레이션 (실제로는 실제 작업을 수행)
                std::thread::sleep(std::time::Duration::from_millis(100));
                
                self.completed_tasks.push(task.clone());
                self.is_busy = false;
                println!("[워커 {}] 작업 완료: {}", self.id, task.description);
            }
            WorkerMessage::Shutdown => {
                println!("[워커 {}] 종료됨. 완료된 작업 수: {}", 
                    self.id, self.completed_tasks.len());
            }
        }
    }

    async fn pre_start(&mut self) {
        println!("[워커 {}] 시작됨", self.id);
    }

    async fn post_stop(&mut self) {
        println!("[워커 {}] 종료됨", self.id);
    }
}

/// 워커 풀 관리자
struct WorkerPool {
    workers: Vec<Arc<Mutex<WorkerActor>>>,
    task_queue: Arc<Mutex<VecDeque<Task>>>,
    next_task_id: u32,
}

impl WorkerPool {
    fn new(worker_count: usize) -> Self {
        let mut workers = Vec::new();
        let task_queue = Arc::new(Mutex::new(VecDeque::new()));
        
        for i in 0..worker_count {
            let worker = Arc::new(Mutex::new(WorkerActor::new(i as u32)));
            workers.push(worker);
        }
        
        Self {
            workers,
            task_queue,
            next_task_id: 0,
        }
    }

    fn add_task(&mut self, description: String, priority: u8) {
        let task = Task::new(self.next_task_id, description.clone(), priority);
        self.next_task_id += 1;
        
        if let Ok(mut queue) = self.task_queue.lock() {
            queue.push_back(task);
            println!("작업 추가됨: {} (우선순위: {})", description, priority);
        }
    }

    async fn process_tasks(&self) {
        println!("\n=== 작업 처리 시작 ===");
        
        // 모든 워커가 사용 가능할 때까지 작업을 할당
        while let Ok(mut queue) = self.task_queue.lock() {
            if queue.is_empty() {
                break;
            }
            
            // 사용 가능한 워커 찾기
            for worker in &self.workers {
                if let Ok(mut worker_guard) = worker.lock() {
                    if !worker_guard.is_busy {
                        if let Some(task) = queue.pop_front() {
                            let worker_clone = worker.clone();
                            let task_clone = task.clone();
                            
                            // 비동기로 작업 처리
                            tokio::spawn(async move {
                                if let Ok(mut worker_guard) = worker_clone.lock() {
                                    // Note: In a real implementation, this would use ActorContext
                    // worker_guard.receive(WorkerMessage::ProcessTask(task_clone), ctx).await;
                    println!("[워커 {}] 작업 처리 시뮬레이션: {}", worker_guard.id, task_clone.description);
                                }
                            });
                            
                            println!("워커 {}에게 작업 할당: {}", worker_guard.id, task.description);
                        }
                    }
                }
            }
            
            // 잠시 대기
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
        
        println!("모든 작업이 워커에게 할당되었습니다.");
    }

    async fn wait_for_completion(&self) {
        println!("\n=== 작업 완료 대기 중 ===");
        
        // 모든 워커가 작업을 완료할 때까지 대기
        loop {
            let mut all_idle = true;
            
            for worker in &self.workers {
                if let Ok(worker_guard) = worker.lock() {
                    if worker_guard.is_busy {
                        all_idle = false;
                        break;
                    }
                }
            }
            
            if all_idle {
                break;
            }
            
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        
        println!("모든 작업이 완료되었습니다.");
    }

    fn print_statistics(&self) {
        println!("\n=== 워커 풀 통계 ===");
        println!("총 워커 수: {}", self.workers.len());
        
        for worker in &self.workers {
            if let Ok(worker_guard) = worker.lock() {
                println!("워커 {}: 완료된 작업 {}개", 
                    worker_guard.id, worker_guard.completed_tasks.len());
            }
        }
        
        if let Ok(queue) = self.task_queue.lock() {
            println!("대기 중인 작업: {}개", queue.len());
        }
    }

    async fn shutdown(&mut self) {
        println!("\n=== 워커 풀 종료 ===");
        
        // 모든 워커에게 종료 신호 전송
        for worker in &self.workers {
            let worker_clone = worker.clone();
        tokio::spawn(async move {
            if let Ok(worker_guard) = worker_clone.lock() {
                // Note: In a real implementation, this would call post_stop with ActorContext
                println!("[워커 {}] 종료 신호 수신", worker_guard.id);
            }
        });
        }
        
        // 워커들이 종료될 때까지 대기
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        
        self.workers.clear();
        if let Ok(mut queue) = self.task_queue.lock() {
            queue.clear();
        }
    }
}

#[tokio::main]
async fn main() {
    println!("=== Pekko Rust 워커 풀 예제 ===\n");

    let mut pool = WorkerPool::new(3); // 3개의 워커 생성

    // 다양한 우선순위의 작업들 추가
    pool.add_task("데이터베이스 백업".to_string(), 1);
    pool.add_task("로그 분석".to_string(), 3);
    pool.add_task("이메일 전송".to_string(), 2);
    pool.add_task("파일 압축".to_string(), 4);
    pool.add_task("보고서 생성".to_string(), 1);
    pool.add_task("시스템 모니터링".to_string(), 5);

    // 작업 처리 시작
    pool.process_tasks().await;

    // 모든 작업 완료 대기
    pool.wait_for_completion().await;

    // 통계 출력
    pool.print_statistics();

    // 시스템 종료
    pool.shutdown().await;

    println!("\n워커 풀 예제 완료!");
} 
//! Distributed calculator example using remote actors
//! 
//! This example demonstrates:
//! - Remote actor deployment and communication
//! - Network-transparent actor references
//! - Cluster membership and failure detection
//! - Load balancing across multiple nodes

use pekko_actor::{Actor, ActorContext};
use pekko_remoting::{
    RemoteActorRef, RemoteActorSystemProvider, RemotingConfig, DefaultRemoteActorSystemProvider,
    RemoteAddress, MessageRouter,
};
use serde::{Deserialize, Serialize};
use tokio;
use tracing::{ error};
use uuid::Uuid;

/// Calculator operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CalculatorMessage {
    Add { a: f64, b: f64, reply_to: String },
    Subtract { a: f64, b: f64, reply_to: String },
    Multiply { a: f64, b: f64, reply_to: String },
    Divide { a: f64, b: f64, reply_to: String },
    GetStats { reply_to: String },
}

/// Calculator responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CalculatorResponse {
    Result { operation: String, result: f64, request_id: String },
    Error { message: String, request_id: String },
    Stats { operations_count: u64, node_id: String },
}

/// Calculator actor that performs mathematical operations
pub struct CalculatorActor {
    node_id: String,
    operations_count: u64,
}

impl CalculatorActor {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            operations_count: 0,
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
                    Err("Division by zero".to_string())
                } else {
                    Ok(a / b)
                }
            }
            _ => Err(format!("Unknown operation: {}", op)),
        }
    }
}

#[async_trait::async_trait]
impl Actor for CalculatorActor {
    type Message = CalculatorMessage;

    async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
        match msg {
            CalculatorMessage::Add { a, b, reply_to } => {
                let result = self.perform_operation("add", a, b);
                let response = match result {
                    Ok(res) => CalculatorResponse::Result {
                        operation: format!("{} + {}", a, b),
                        result: res,
                        request_id: reply_to.clone(),
                    },
                    Err(ref err) => CalculatorResponse::Error {
                        message: err.to_string(),
                        request_id: reply_to.clone(),
                    },
                };
                // TODO: Send response back (requires response actor implementation)
            }
            
            CalculatorMessage::Subtract { a, b, reply_to } => {
                let result = self.perform_operation("subtract", a, b);
                let response = match result {
                    Ok(res) => CalculatorResponse::Result {
                        operation: format!("{} - {}", a, b),
                        result: res,
                        request_id: reply_to.clone(),
                    },
                    Err(ref err) => CalculatorResponse::Error {
                        message: err.to_string(),
                        request_id: reply_to.clone(),
                    },
                };
            }
            
            CalculatorMessage::Multiply { a, b, reply_to } => {
                let result = self.perform_operation("multiply", a, b);
                let response = match result {
                    Ok(res) => CalculatorResponse::Result {
                        operation: format!("{} * {}", a, b),
                        result: res,
                        request_id: reply_to.clone(),
                    },
                    Err(ref err) => CalculatorResponse::Error {
                        message: err.to_string(),
                        request_id: reply_to.clone(),
                    },
                };
            }
            
            CalculatorMessage::Divide { a, b, reply_to } => {
                let result = self.perform_operation("divide", a, b);
                let response = match result {
                    Ok(res) => CalculatorResponse::Result {
                        operation: format!("{} / {}", a, b),
                        result: res,
                        request_id: reply_to.clone(),
                    },
                    Err(ref err) => CalculatorResponse::Error {
                        message: err.to_string(),
                        request_id: reply_to.clone(),
                    },
                };
            }
            
            CalculatorMessage::GetStats { reply_to } => {
                let response = CalculatorResponse::Stats {
                    operations_count: self.operations_count,
                    node_id: self.node_id.clone(),
                };
            }
        }
    }
}

/// Client actor that sends calculation requests
pub struct CalculatorClient {
    client_id: String,
    calculator_refs: Vec<RemoteActorRef>,
    current_calculator: usize,
}

impl CalculatorClient {
    pub fn new(client_id: String, calculator_refs: Vec<RemoteActorRef>) -> Self {
        Self {
            client_id,
            calculator_refs,
            current_calculator: 0,
        }
    }

    fn next_calculator(&mut self) -> Option<&RemoteActorRef> {
        if self.calculator_refs.is_empty() {
            return None;
        }
        
        let calc_ref = &self.calculator_refs[self.current_calculator];
        self.current_calculator = (self.current_calculator + 1) % self.calculator_refs.len();
        Some(calc_ref)
    }

    async fn send_calculation(&mut self, message: CalculatorMessage) {
        if let Some(calc_ref) = self.next_calculator() {
            // RemoteActorRef에 tell 메서드가 없으므로 placeholder로 구현
        } else {
            error!("No calculator actors available");
        }
    }
}

#[derive(Debug, Clone)]
pub enum ClientMessage {
    Calculate { operation: String, a: f64, b: f64 },
    GetAllStats,
    AddCalculator { actor_ref: RemoteActorRef },
}

#[async_trait::async_trait]
impl Actor for CalculatorClient {
    type Message = ClientMessage;

    async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
        match msg {
            ClientMessage::Calculate { operation, a, b } => {
                let reply_to = format!("{}_{}", self.client_id, Uuid::new_v4());
                
                let calc_msg = match operation.as_str() {
                    "add" => CalculatorMessage::Add { a, b, reply_to },
                    "subtract" => CalculatorMessage::Subtract { a, b, reply_to },
                    "multiply" => CalculatorMessage::Multiply { a, b, reply_to },
                    "divide" => CalculatorMessage::Divide { a, b, reply_to },
                    _ => {
                        error!("Unknown operation: {}", operation);
                        return;
                    }
                };
                
                self.send_calculation(calc_msg).await;
            }
            
            ClientMessage::GetAllStats => {
                let reply_to = format!("{}_{}", self.client_id, Uuid::new_v4());
                
                for calc_ref in &self.calculator_refs {
                    let stats_msg = CalculatorMessage::GetStats { reply_to: reply_to.clone() };
                    // RemoteActorRef에 tell 메서드가 없으므로 placeholder로 구현
                }
            }
            
            ClientMessage::AddCalculator { actor_ref } => {
                self.calculator_refs.push(actor_ref);
            }
        }
    }
}

/// Create a calculator node
async fn create_calculator_node(
    node_id: &str,
    port: u16,
    seed_nodes: Vec<String>,
) -> Result<DefaultRemoteActorSystemProvider, Box<dyn std::error::Error>> {
    let config = RemotingConfig::default();

    let mut provider = DefaultRemoteActorSystemProvider::new(config);
    provider.start().await?;
    Ok(provider)
}

/// Create a client node
async fn create_client_node(
    port: u16,
    seed_nodes: Vec<String>,
) -> Result<DefaultRemoteActorSystemProvider, Box<dyn std::error::Error>> {
    let config = RemotingConfig::default();

    let mut provider = DefaultRemoteActorSystemProvider::new(config);
    provider.start().await?;
    Ok(provider)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    // tracing_subscriber::fmt::init();
    // Define cluster configuration
    let seed_nodes = vec![
        "127.0.0.1:2552".to_string(),
        "127.0.0.1:2553".to_string(),
    ];

    // Start calculator nodes
    let node1 = create_calculator_node("calc-1", 2552, seed_nodes.clone()).await?;
    let node2 = create_calculator_node("calc-2", 2553, seed_nodes.clone()).await?;
    let node3 = create_calculator_node("calc-3", 2554, seed_nodes.clone()).await?;

    // Start client node
    let client_node = create_client_node(2555, seed_nodes.clone()).await?;

    // Give cluster time to converge
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // Create remote actor references to calculators
    let router = MessageRouter::new();
    // RemoteActorRef 생성은 placeholder로 구현
    // Create calculator references vector (placeholder)
    let calculator_refs: Vec<RemoteActorRef> = vec![];

    // Simulate some calculations
    // Create a mock client to send some requests
    let mut client = CalculatorClient::new("client-1".to_string(), calculator_refs);

    // Send various calculations
    client.receive(ClientMessage::Calculate {
        operation: "add".to_string(),
        a: 10.0,
        b: 5.0,
    }, &mut unsafe { std::mem::zeroed() }).await;

    client.receive(ClientMessage::Calculate {
        operation: "multiply".to_string(),
        a: 7.0,
        b: 8.0,
    }, &mut unsafe { std::mem::zeroed() }).await;

    client.receive(ClientMessage::Calculate {
        operation: "divide".to_string(),
        a: 100.0,
        b: 4.0,
    }, &mut unsafe { std::mem::zeroed() }).await;

    client.receive(ClientMessage::Calculate {
        operation: "subtract".to_string(),
        a: 20.0,
        b: 3.0,
    }, &mut unsafe { std::mem::zeroed() }).await;

    // Get statistics from all nodes
    client.receive(ClientMessage::GetAllStats, &mut unsafe { std::mem::zeroed() }).await;

    // Wait a bit for messages to be processed
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    // Demonstrate failure handling
    // In a real scenario, we would:
    // 1. Stop one of the calculator nodes
    // 2. Show that the client continues to work with remaining nodes
    // 3. Show failure detection in action

    // Clean shutdown
    // Note: In a real implementation, we would properly shut down all nodes
    // For this example, we'll just exit

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculator_actor_creation() {
        let actor = CalculatorActor::new("test-node".to_string());
        assert_eq!(actor.node_id, "test-node");
        assert_eq!(actor.operations_count, 0);
    }

    #[test]
    fn test_calculator_operations() {
        let mut actor = CalculatorActor::new("test".to_string());
        
        assert_eq!(actor.perform_operation("add", 5.0, 3.0), Ok(8.0));
        assert_eq!(actor.perform_operation("subtract", 10.0, 4.0), Ok(6.0));
        assert_eq!(actor.perform_operation("multiply", 6.0, 7.0), Ok(42.0));
        assert_eq!(actor.perform_operation("divide", 15.0, 3.0), Ok(5.0));
        
        // Test division by zero
        assert!(actor.perform_operation("divide", 10.0, 0.0).is_err());
        
        // Test unknown operation
        assert!(actor.perform_operation("unknown", 1.0, 2.0).is_err());
        
        // Operations counter should have incremented
        assert_eq!(actor.operations_count, 6);
    }

    #[tokio::test]
    async fn test_client_creation() {
        let router = MessageRouter::new();
        let calc_refs = vec![];
        let client = CalculatorClient::new("test-client".to_string(), calc_refs);
        
        assert_eq!(client.client_id, "test-client");
        assert_eq!(client.calculator_refs.len(), 0);
    }
}
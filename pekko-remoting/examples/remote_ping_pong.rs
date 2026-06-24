//! Remote Ping-Pong Example
//! 
//! Demonstrates:
//! - Setting up two remote actor systems (each with server + client)
//! - Bidirectional communication between systems
//! - Inbound connection handling via RemoteServer

use pekko_remoting::{
    RemoteActorSystem, RemoteSystemConfig, RemoteMessageHandler, RemotingResult,
    RemoteAddress, proto::remoting::RemoteEnvelope,
};
use pekko_actor::ActorPath;
use tokio::time::{sleep, Duration};

/// Simple ping handler that responds with pong
struct PingHandler {
    name: String,
}

impl PingHandler {
    fn new(name: &str) -> Self {
        Self { name: name.to_string() }
    }
}

#[async_trait::async_trait]
impl RemoteMessageHandler for PingHandler {
    async fn handle(&self, envelope: RemoteEnvelope) -> RemotingResult<Option<RemoteEnvelope>> {
        println!("📥 [{}] Received: {}", self.name, envelope.message_manifest);
        
        if let Some(msg) = &envelope.message {
            if let Ok(text) = String::from_utf8(msg.value.clone()) {
                println!("   Message: {}", text);
            }
        }
        
        Ok(None)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("🚀 Starting Remote Ping-Pong Example\n");
    println!("   Both systems have a TCP server for accepting connections");
    println!("   and a connection pool for outgoing connections.\n");

    // Create System A (Port 2552)
    let config_a = RemoteSystemConfig::new("system-a")
        .with_bind("127.0.0.1", 2552);
    
    let system_a = RemoteActorSystem::new(config_a);
    
    // Register handler on System A
    let ping_path_a = ActorPath::parse("pekko://system-a/user/echo").unwrap();
    system_a.register(ping_path_a.clone(), PingHandler::new("System-A")).await?;
    
    // Start system A (starts both server and message handler)
    system_a.start().await?;
    println!("✅ System A started at {} (server listening)", system_a.local_address());

    // Create System B (Port 2553)
    let config_b = RemoteSystemConfig::new("system-b")
        .with_bind("127.0.0.1", 2553);
    
    let system_b = RemoteActorSystem::new(config_b);
    
    // Register handler on System B too
    let ping_path_b = ActorPath::parse("pekko://system-b/user/echo").unwrap();
    system_b.register(ping_path_b.clone(), PingHandler::new("System-B")).await?;
    
    system_b.start().await?;
    println!("✅ System B started at {} (server listening)", system_b.local_address());

    // ===== Test 1: System B -> System A =====
    println!("\n--- Test 1: System B sends to System A ---");
    
    let remote_addr_a = RemoteAddress::tcp("system-a", "127.0.0.1", 2552);
    let remote_actor_a = system_b.remote_actor(remote_addr_a, ping_path_a);
    
    println!("📤 Sending message from B -> A...");
    match system_b.tell(&remote_actor_a, b"Hello from System B!".to_vec(), "Ping").await {
        Ok(_) => println!("✅ Message sent to System A!"),
        Err(e) => println!("❌ Failed: {}", e),
    }

    // Wait for processing
    sleep(Duration::from_millis(500)).await;

    // ===== Test 2: System A -> System B =====
    println!("\n--- Test 2: System A sends to System B ---");
    
    let remote_addr_b = RemoteAddress::tcp("system-b", "127.0.0.1", 2553);
    let remote_actor_b = system_a.remote_actor(remote_addr_b, ping_path_b);
    
    println!("📤 Sending message from A -> B...");
    match system_a.tell(&remote_actor_b, b"Hello from System A!".to_vec(), "Pong").await {
        Ok(_) => println!("✅ Message sent to System B!"),
        Err(e) => println!("❌ Failed: {}", e),
    }

    // Wait for processing
    sleep(Duration::from_millis(500)).await;

    // Print full stats
    let stats_a = system_a.full_stats().await;
    let stats_b = system_b.full_stats().await;
    
    println!("\n📊 Full Statistics:");
    println!("  System A:");
    println!("    - Outbound connections: {}", stats_a.outbound_connections);
    println!("    - Inbound connections:  {}", stats_a.inbound_connections);
    println!("    - Server running: {}", stats_a.server_running);
    println!("  System B:");
    println!("    - Outbound connections: {}", stats_b.outbound_connections);
    println!("    - Inbound connections:  {}", stats_b.inbound_connections);
    println!("    - Server running: {}", stats_b.server_running);

    // Cleanup
    system_a.stop().await?;
    system_b.stop().await?;
    
    println!("\n👋 Systems stopped. Goodbye!");
    
    Ok(())
}

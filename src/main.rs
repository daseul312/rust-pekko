//! # Pekko Rust - A Rust Implementation of Apache Pekko
//! 
//! This is a Rust implementation of the Apache Pekko actor framework,
//! providing type-safe, high-performance actor systems with persistence support.

#[cfg(feature = "actor")]
pub use pekko_actor as actor;

#[cfg(feature = "persistence")]
pub use pekko_persistence as persistence;

#[cfg(feature = "remoting")]
pub use pekko_remoting as remoting;

#[cfg(feature = "testkit")]
pub use pekko_testkit as testkit;

// Re-export commonly used types
#[cfg(feature = "actor")]
pub use pekko_actor::Actor;

#[cfg(feature = "persistence")]
pub use pekko_persistence::{PersistentActor, Journal, SnapshotStore};

#[cfg(feature = "remoting")]
pub use pekko_remoting::{RemoteActorRef, RemoteActorSystem, Transport};

fn main() {
    println!("Pekko Rust - Actor System Framework");
    println!("Features enabled:");
    
    #[cfg(feature = "actor")]
    println!("  - Actor system");
    
    #[cfg(feature = "persistence")]
    println!("  - Persistence (Event Sourcing)");
    
    #[cfg(feature = "remoting")]
    println!("  - Remoting & Clustering");
    
    #[cfg(feature = "testkit")]
    println!("  - Testing toolkit");
    
    println!("\nTo use Pekko Rust as a library, add it to your Cargo.toml:");
    println!("pekko-rust = {{ path = \"./pekko-rust\", features = [\"full\"] }}");
}

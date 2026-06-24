//! Cluster Sharding - Distribute actors across the cluster
//!
//! Cluster Sharding enables the distribution of actors with a common identifier
//! (entity ID) across the cluster. Actors are grouped into shards, and shards
//! are allocated to nodes by the shard coordinator.
//!
//! # Components
//!
//! - `ShardRegion`: Manages shards and entities on a single node
//! - `ShardCoordinator`: Distributes shards across the cluster
//! - `EntityRef`: Reference to a sharded entity
//!
//! # Example
//!
//! ```ignore
//! // Define entity message extraction
//! let extractor = |msg: &MyMessage| -> (String, String) {
//!     (msg.entity_id.clone(), msg.shard_id())
//! };
//!
//! // Start sharding
//! let region = ShardRegion::start(
//!     "my-entities",
//!     || MyEntity::new(),
//!     settings,
//!     extractor,
//! ).await?;
//!
//! // Send message to entity
//! region.tell("entity-123", MyMessage { ... }).await?;
//! ```

use crate::{
    cluster::{ClusterSystem, Member, MemberStatus},
    addressing::{RemoteAddress, UniqueAddress},
    errors::{ClusterError, ClusterResult},
};
use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{debug, error, warn};

/// Sharding settings
#[derive(Debug, Clone)]
pub struct ShardingSettings {
    /// Name of the shard region
    pub type_name: String,
    /// Number of shards (should be ~10x number of nodes)
    pub number_of_shards: u32,
    /// Role required for hosting shards
    pub role: Option<String>,
    /// Entity passivation timeout (idle timeout)
    pub passivation_idle_timeout: Duration,
    /// Remember entities across restarts
    pub remember_entities: bool,
    /// Shard allocation strategy
    pub allocation_strategy: AllocationStrategy,
    /// Buffer size for messages when shard is not allocated
    pub buffer_size: usize,
}

impl Default for ShardingSettings {
    fn default() -> Self {
        Self {
            type_name: "default".to_string(),
            number_of_shards: 100,
            role: None,
            passivation_idle_timeout: Duration::from_secs(120),
            remember_entities: false,
            allocation_strategy: AllocationStrategy::LeastShards,
            buffer_size: 1000,
        }
    }
}

impl ShardingSettings {
    pub fn new(type_name: &str) -> Self {
        Self {
            type_name: type_name.to_string(),
            ..Default::default()
        }
    }

    pub fn with_number_of_shards(mut self, n: u32) -> Self {
        self.number_of_shards = n;
        self
    }

    pub fn with_role(mut self, role: &str) -> Self {
        self.role = Some(role.to_string());
        self
    }

    pub fn with_passivation_timeout(mut self, timeout: Duration) -> Self {
        self.passivation_idle_timeout = timeout;
        self
    }

    pub fn with_remember_entities(mut self, remember: bool) -> Self {
        self.remember_entities = remember;
        self
    }
}

/// Shard allocation strategy
#[derive(Debug, Clone, Copy)]
pub enum AllocationStrategy {
    /// Allocate shards to node with fewest shards
    LeastShards,
    /// Allocate shards round-robin
    RoundRobin,
    /// Allocate based on consistent hashing
    ConsistentHashing,
}

/// Shard ID
pub type ShardId = String;

/// Entity ID
pub type EntityId = String;

/// Extract shard and entity ID from a message
pub type MessageExtractor<M> = Box<dyn Fn(&M) -> (EntityId, ShardId) + Send + Sync>;

/// Entity message wrapper
#[derive(Debug, Clone)]
pub struct EntityMessage {
    pub entity_id: EntityId,
    pub shard_id: ShardId,
    pub payload: Vec<u8>,
    pub manifest: String,
}

/// Trait for sharded entity factories
pub trait EntityFactory: Send + Sync {
    /// Create a new entity instance
    fn create(&self, entity_id: &str) -> Box<dyn ShardedEntity>;
}

/// Trait for sharded entities
#[async_trait]
pub trait ShardedEntity: Send + Sync {
    /// Initialize the entity
    async fn init(&mut self, entity_id: &str) -> ClusterResult<()>;
    
    /// Handle a message
    async fn handle(&mut self, message: EntityMessage) -> ClusterResult<Option<Vec<u8>>>;
    
    /// Called before passivation
    async fn passivate(&mut self) -> ClusterResult<()> {
        Ok(())
    }
    
    /// Called on recovery (if remember_entities is enabled)
    async fn recover(&mut self) -> ClusterResult<()> {
        Ok(())
    }
}

/// Entity wrapper with metadata
struct EntityInstance {
    entity: Box<dyn ShardedEntity>,
    last_activity: Instant,
    message_count: u64,
}

/// A shard containing multiple entities
struct Shard {
    id: ShardId,
    entities: DashMap<EntityId, EntityInstance>,
    factory: Arc<dyn EntityFactory>,
    settings: ShardingSettings,
}

impl Shard {
    fn new(id: ShardId, factory: Arc<dyn EntityFactory>, settings: ShardingSettings) -> Self {
        Self {
            id,
            entities: DashMap::new(),
            factory,
            settings,
        }
    }

    /// Get or create an entity
    async fn get_or_create(&self, entity_id: &EntityId) -> ClusterResult<()> {
        if !self.entities.contains_key(entity_id) {
            let mut entity = self.factory.create(entity_id);
            entity.init(entity_id).await?;
            
            self.entities.insert(
                entity_id.clone(),
                EntityInstance {
                    entity,
                    last_activity: Instant::now(),
                    message_count: 0,
                },
            );
            
            debug!("Created entity {} in shard {}", entity_id, self.id);
        }
        Ok(())
    }

    /// Handle a message for an entity
    async fn handle_message(&self, message: EntityMessage) -> ClusterResult<Option<Vec<u8>>> {
        self.get_or_create(&message.entity_id).await?;

        if let Some(mut instance) = self.entities.get_mut(&message.entity_id) {
            instance.last_activity = Instant::now();
            instance.message_count += 1;
            instance.entity.handle(message).await
        } else {
            Err(ClusterError::MemberNotFound(message.entity_id))
        }
    }

    /// Passivate idle entities
    async fn passivate_idle(&self) {
        let timeout = self.settings.passivation_idle_timeout;
        let now = Instant::now();

        let to_passivate: Vec<EntityId> = self.entities.iter()
            .filter(|e| now.duration_since(e.last_activity) > timeout)
            .map(|e| e.key().clone())
            .collect();

        for entity_id in to_passivate {
            if let Some((_, mut instance)) = self.entities.remove(&entity_id) {
                if let Err(e) = instance.entity.passivate().await {
                    warn!("Error passivating entity {}: {}", entity_id, e);
                }
            }
        }
    }

    /// Get entity count
    fn entity_count(&self) -> usize {
        self.entities.len()
    }
}

/// Shard region - manages shards on this node
pub struct ShardRegion {
    /// Settings
    settings: ShardingSettings,
    /// Local shards
    shards: DashMap<ShardId, Arc<Shard>>,
    /// Entity factory
    factory: Arc<dyn EntityFactory>,
    /// Shard allocations (shard -> node)
    allocations: Arc<RwLock<HashMap<ShardId, UniqueAddress>>>,
    /// Cluster system
    cluster: Arc<ClusterSystem>,
    /// Local node address
    local_address: UniqueAddress,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Message buffer for unallocated shards
    buffer: Arc<RwLock<HashMap<ShardId, Vec<EntityMessage>>>>,
    /// Statistics
    stats: Arc<ShardRegionStats>,
}

/// Shard region statistics
#[derive(Debug, Default)]
pub struct ShardRegionStats {
    pub local_shards: AtomicU64,
    pub local_entities: AtomicU64,
    pub messages_delivered: AtomicU64,
    pub messages_buffered: AtomicU64,
}

impl ShardRegion {
    /// Create a new shard region
    pub fn new<F: EntityFactory + 'static>(
        cluster: Arc<ClusterSystem>,
        settings: ShardingSettings,
        factory: F,
    ) -> Self {
        let local_address = UniqueAddress::generate(
            RemoteAddress::tcp(&settings.type_name, "127.0.0.1", 0)
        );

        Self {
            settings,
            shards: DashMap::new(),
            factory: Arc::new(factory),
            allocations: Arc::new(RwLock::new(HashMap::new())),
            cluster,
            local_address,
            running: Arc::new(AtomicBool::new(false)),
            buffer: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(ShardRegionStats::default()),
        }
    }

    /// Start the shard region
    pub async fn start(&self) -> ClusterResult<()> {
        if self.running.load(Ordering::SeqCst) {
            return Ok(());
        }

        self.running.store(true, Ordering::SeqCst);
        // Start passivation task
        let shards = self.shards.clone();
        let running = self.running.clone();
        let timeout = self.settings.passivation_idle_timeout;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(timeout / 2);
            
            while running.load(Ordering::SeqCst) {
                interval.tick().await;
                
                for shard in shards.iter() {
                    shard.passivate_idle().await;
                }
            }
        });

        Ok(())
    }

    /// Stop the shard region
    pub async fn stop(&self) -> ClusterResult<()> {
        self.running.store(false, Ordering::SeqCst);

        // Passivate all entities
        for shard in self.shards.iter() {
            for entry in shard.entities.iter() {
                if let Some((_, mut instance)) = shard.entities.remove(entry.key()) {
                    let _ = instance.entity.passivate().await;
                }
            }
        }

        self.shards.clear();
        Ok(())
    }

    /// Calculate shard ID from entity ID
    pub fn shard_id(&self, entity_id: &str) -> ShardId {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        entity_id.hash(&mut hasher);
        let hash = hasher.finish();
        format!("shard-{}", hash % self.settings.number_of_shards as u64)
    }

    /// Send a message to an entity
    pub async fn tell(
        &self,
        entity_id: &str,
        payload: Vec<u8>,
        manifest: &str,
    ) -> ClusterResult<()> {
        let shard_id = self.shard_id(entity_id);
        
        let message = EntityMessage {
            entity_id: entity_id.to_string(),
            shard_id: shard_id.clone(),
            payload,
            manifest: manifest.to_string(),
        };

        // Check if shard is allocated locally
        let is_local = {
            let allocations = self.allocations.read().await;
            allocations.get(&shard_id)
                .map(|addr| addr == &self.local_address)
                .unwrap_or(false)
        };

        if is_local || self.shards.contains_key(&shard_id) {
            // Deliver locally
            self.deliver_local(message).await?;
        } else {
            // Check allocation or buffer
            let allocations = self.allocations.read().await;
            
            if let Some(_remote_addr) = allocations.get(&shard_id) {
                // TODO: Forward to remote shard region
                warn!("Remote shard delivery not implemented, buffering");
                self.buffer_message(shard_id, message).await?;
            } else {
                // Shard not allocated, buffer and request allocation
                self.buffer_message(shard_id.clone(), message).await?;
                self.request_shard_allocation(&shard_id).await?;
            }
        }

        Ok(())
    }

    /// Send a message and wait for response
    pub async fn ask(
        &self,
        entity_id: &str,
        payload: Vec<u8>,
        manifest: &str,
        _timeout: Duration,
    ) -> ClusterResult<Vec<u8>> {
        let shard_id = self.shard_id(entity_id);
        
        let message = EntityMessage {
            entity_id: entity_id.to_string(),
            shard_id: shard_id.clone(),
            payload,
            manifest: manifest.to_string(),
        };

        // For now, only support local delivery
        if let Some(shard) = self.shards.get(&shard_id) {
            match shard.handle_message(message).await? {
                Some(response) => Ok(response),
                None => Err(ClusterError::StateError("No response".to_string())),
            }
        } else {
            Err(ClusterError::MemberNotFound(format!("Shard {} not local", shard_id)))
        }
    }

    /// Deliver message to local shard
    async fn deliver_local(&self, message: EntityMessage) -> ClusterResult<()> {
        let shard_id = message.shard_id.clone();
        
        // Get or create shard
        if !self.shards.contains_key(&shard_id) {
            let shard = Arc::new(Shard::new(
                shard_id.clone(),
                self.factory.clone(),
                self.settings.clone(),
            ));
            self.shards.insert(shard_id.clone(), shard);
            self.stats.local_shards.fetch_add(1, Ordering::SeqCst);
        }

        if let Some(shard) = self.shards.get(&shard_id) {
            shard.handle_message(message).await?;
            self.stats.messages_delivered.fetch_add(1, Ordering::SeqCst);
        }

        Ok(())
    }

    /// Buffer a message for later delivery
    async fn buffer_message(&self, shard_id: ShardId, message: EntityMessage) -> ClusterResult<()> {
        let mut buffer = self.buffer.write().await;
        let shard_buffer = buffer.entry(shard_id).or_insert_with(Vec::new);
        
        if shard_buffer.len() < self.settings.buffer_size {
            shard_buffer.push(message);
            self.stats.messages_buffered.fetch_add(1, Ordering::SeqCst);
            Ok(())
        } else {
            Err(ClusterError::StateError("Buffer full".to_string()))
        }
    }

    /// Request shard allocation from coordinator
    async fn request_shard_allocation(&self, shard_id: &ShardId) -> ClusterResult<()> {
        // In a real implementation, this would contact the shard coordinator
        // For now, allocate locally
        {
            let mut allocations = self.allocations.write().await;
            allocations.insert(shard_id.clone(), self.local_address.clone());
        }

        // Drain buffer for this shard
        let buffered = {
            let mut buffer = self.buffer.write().await;
            buffer.remove(shard_id).unwrap_or_default()
        };

        for msg in buffered {
            self.deliver_local(msg).await?;
        }

        Ok(())
    }

    /// Get statistics
    pub fn stats(&self) -> &ShardRegionStats {
        &self.stats
    }

    /// Get local shard count
    pub fn local_shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Get total entity count across local shards
    pub fn entity_count(&self) -> usize {
        self.shards.iter().map(|s| s.entity_count()).sum()
    }
}

/// Reference to a sharded entity
pub struct EntityRef {
    entity_id: EntityId,
    region: Arc<ShardRegion>,
}

impl EntityRef {
    pub fn new(entity_id: EntityId, region: Arc<ShardRegion>) -> Self {
        Self { entity_id, region }
    }

    /// Send a message (fire-and-forget)
    pub async fn tell(&self, payload: Vec<u8>, manifest: &str) -> ClusterResult<()> {
        self.region.tell(&self.entity_id, payload, manifest).await
    }

    /// Send a message and wait for response
    pub async fn ask(&self, payload: Vec<u8>, manifest: &str, timeout: Duration) -> ClusterResult<Vec<u8>> {
        self.region.ask(&self.entity_id, payload, manifest, timeout).await
    }

    /// Get the entity ID
    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfig;

    struct TestEntity {
        id: String,
        value: i32,
    }

    #[async_trait]
    impl ShardedEntity for TestEntity {
        async fn init(&mut self, entity_id: &str) -> ClusterResult<()> {
            self.id = entity_id.to_string();
            Ok(())
        }

        async fn handle(&mut self, message: EntityMessage) -> ClusterResult<Option<Vec<u8>>> {
            // Simple echo
            self.value += 1;
            Ok(Some(message.payload))
        }
    }

    struct TestEntityFactory;

    impl EntityFactory for TestEntityFactory {
        fn create(&self, _entity_id: &str) -> Box<dyn ShardedEntity> {
            Box::new(TestEntity {
                id: String::new(),
                value: 0,
            })
        }
    }

    #[test]
    fn test_sharding_settings() {
        let settings = ShardingSettings::new("orders")
            .with_number_of_shards(50)
            .with_role("backend")
            .with_passivation_timeout(Duration::from_secs(60));

        assert_eq!(settings.type_name, "orders");
        assert_eq!(settings.number_of_shards, 50);
        assert_eq!(settings.role, Some("backend".to_string()));
    }

    #[test]
    fn test_shard_id_calculation() {
        let config = ClusterConfig::default();
        let cluster = Arc::new(ClusterSystem::new(config));
        let settings = ShardingSettings::new("test").with_number_of_shards(10);
        let region = ShardRegion::new(cluster, settings, TestEntityFactory);

        let shard1 = region.shard_id("entity-1");
        let shard2 = region.shard_id("entity-1");
        let shard3 = region.shard_id("entity-2");

        // Same entity should always get same shard
        assert_eq!(shard1, shard2);
        // Different entities may get different shards
        assert!(shard1.starts_with("shard-"));
        assert!(shard3.starts_with("shard-"));
    }

    #[tokio::test]
    async fn test_shard_region_lifecycle() {
        let config = ClusterConfig::default();
        let cluster = Arc::new(ClusterSystem::new(config));
        let settings = ShardingSettings::new("test");
        let region = ShardRegion::new(cluster, settings, TestEntityFactory);

        assert_eq!(region.local_shard_count(), 0);
        assert_eq!(region.entity_count(), 0);

        region.start().await.unwrap();

        // Send a message to create an entity
        region.tell("entity-1", b"hello".to_vec(), "test").await.unwrap();

        assert!(region.local_shard_count() > 0);
        assert_eq!(region.entity_count(), 1);

        region.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_entity_ref() {
        let config = ClusterConfig::default();
        let cluster = Arc::new(ClusterSystem::new(config));
        let settings = ShardingSettings::new("test");
        let region = Arc::new(ShardRegion::new(cluster, settings, TestEntityFactory));

        region.start().await.unwrap();

        let entity_ref = EntityRef::new("entity-1".to_string(), region.clone());
        
        assert_eq!(entity_ref.entity_id(), "entity-1");
        entity_ref.tell(b"test".to_vec(), "msg").await.unwrap();

        region.stop().await.unwrap();
    }
}

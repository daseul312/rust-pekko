//! Distributed Data — Conflict-free Replicated Data Types (CRDTs)
//!
//! Modeled after Scala Pekko's Distributed Data module:
//!
//! - **GCounter**: Grow-only counter
//! - **PNCounter**: Positive-negative counter (increment/decrement)
//! - **GSet**: Grow-only set
//! - **ORSet**: Observed-remove set (add and remove)
//! - **LWWRegister**: Last-writer-wins register
//! - **ORMap**: Observed-remove map
//! - **Flag**: Boolean flag (once set, stays true)
//! - **Replicator**: Manages CRDT replication across nodes

use crate::addressing::UniqueAddress;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tracing::{debug, warn};

// ============= CRDT Trait =============

/// Base trait for all CRDTs. Defines merge semantics.
pub trait CrdtValue: Clone + Send + Sync + fmt::Debug {
    /// Merge another replica's value into this one.
    /// The merge must be commutative, associative, and idempotent.
    fn merge(&mut self, other: &Self);
}

// ============= GCounter =============

/// Grow-only counter. Each node has its own slot; total is the sum.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GCounter {
    state: BTreeMap<String, u64>,
}

impl GCounter {
    pub fn new() -> Self {
        Self { state: BTreeMap::new() }
    }

    /// Increment this node's counter.
    pub fn increment(&mut self, node: &str, delta: u64) {
        let entry = self.state.entry(node.to_string()).or_insert(0);
        *entry += delta;
    }

    /// Get the total value across all nodes.
    pub fn value(&self) -> u64 {
        self.state.values().sum()
    }

    /// Get a specific node's contribution.
    pub fn node_value(&self, node: &str) -> u64 {
        self.state.get(node).copied().unwrap_or(0)
    }
}

impl CrdtValue for GCounter {
    fn merge(&mut self, other: &Self) {
        for (node, &count) in &other.state {
            let entry = self.state.entry(node.clone()).or_insert(0);
            *entry = (*entry).max(count);
        }
    }
}

// ============= PNCounter =============

/// Positive-Negative counter. Supports both increment and decrement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PNCounter {
    increments: GCounter,
    decrements: GCounter,
}

impl PNCounter {
    pub fn new() -> Self {
        Self {
            increments: GCounter::new(),
            decrements: GCounter::new(),
        }
    }

    pub fn increment(&mut self, node: &str, delta: u64) {
        self.increments.increment(node, delta);
    }

    pub fn decrement(&mut self, node: &str, delta: u64) {
        self.decrements.increment(node, delta);
    }

    pub fn value(&self) -> i64 {
        self.increments.value() as i64 - self.decrements.value() as i64
    }
}

impl CrdtValue for PNCounter {
    fn merge(&mut self, other: &Self) {
        self.increments.merge(&other.increments);
        self.decrements.merge(&other.decrements);
    }
}

// ============= GSet =============

/// Grow-only set. Elements can be added but never removed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GSet<T: Ord + Clone + Hash + fmt::Debug + Send + Sync> {
    elements: HashSet<T>,
}

impl<T: Ord + Clone + Hash + fmt::Debug + Send + Sync + Serialize + for<'de> Deserialize<'de>> GSet<T> {
    pub fn new() -> Self {
        Self { elements: HashSet::new() }
    }

    pub fn insert(&mut self, value: T) {
        self.elements.insert(value);
    }

    pub fn contains(&self, value: &T) -> bool {
        self.elements.contains(value)
    }

    pub fn elements(&self) -> &HashSet<T> {
        &self.elements
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }
}

impl<T: Ord + Clone + Hash + fmt::Debug + Send + Sync + Serialize + for<'de> Deserialize<'de>> CrdtValue for GSet<T> {
    fn merge(&mut self, other: &Self) {
        for elem in &other.elements {
            self.elements.insert(elem.clone());
        }
    }
}

// ============= ORSet (Observed-Remove Set) =============

/// Unique tag for tracking additions (node + counter).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VectorClock {
    pub node: String,
    pub counter: u64,
}

/// Observed-Remove Set. Supports both add and remove.
/// Each addition is tagged with a unique dot; removal only removes observed dots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ORSet<T: Eq + Hash + Clone + fmt::Debug + Send + Sync> {
    /// element → set of dots that added it
    entries: HashMap<T, HashSet<VectorClock>>,
    /// Per-node counters for generating unique dots
    counters: BTreeMap<String, u64>,
}

impl<T: Eq + Hash + Clone + fmt::Debug + Send + Sync + Serialize + for<'de> Deserialize<'de>> ORSet<T> {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            counters: BTreeMap::new(),
        }
    }

    /// Add an element, tagged with a new unique dot from this node.
    pub fn add(&mut self, node: &str, value: T) {
        let counter = self.counters.entry(node.to_string()).or_insert(0);
        *counter += 1;
        let dot = VectorClock { node: node.to_string(), counter: *counter };
        self.entries.entry(value).or_insert_with(HashSet::new).insert(dot);
    }

    /// Remove an element by removing all its observed dots.
    pub fn remove(&mut self, value: &T) {
        self.entries.remove(value);
    }

    pub fn contains(&self, value: &T) -> bool {
        self.entries.get(value).map_or(false, |dots| !dots.is_empty())
    }

    pub fn elements(&self) -> Vec<&T> {
        self.entries.keys().filter(|k| {
            self.entries.get(*k).map_or(false, |dots| !dots.is_empty())
        }).collect()
    }

    pub fn len(&self) -> usize {
        self.entries.iter().filter(|(_, dots)| !dots.is_empty()).count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T: Eq + Hash + Clone + fmt::Debug + Send + Sync + Serialize + for<'de> Deserialize<'de>> CrdtValue for ORSet<T> {
    fn merge(&mut self, other: &Self) {
        // Merge counters
        for (node, &count) in &other.counters {
            let entry = self.counters.entry(node.clone()).or_insert(0);
            *entry = (*entry).max(count);
        }
        // Merge entries: union of dots for each element
        for (elem, other_dots) in &other.entries {
            let dots = self.entries.entry(elem.clone()).or_insert_with(HashSet::new);
            for dot in other_dots {
                dots.insert(dot.clone());
            }
        }
    }
}

// ============= LWWRegister =============

/// Last-Writer-Wins Register. The value with the latest timestamp wins.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LWWRegister<T: Clone + fmt::Debug + Send + Sync> {
    value: T,
    timestamp: u64,
    node: String,
}

impl<T: Clone + fmt::Debug + Send + Sync + Serialize + for<'de> Deserialize<'de>> LWWRegister<T> {
    pub fn new(value: T, node: &str) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        Self {
            value,
            timestamp,
            node: node.to_string(),
        }
    }

    pub fn with_timestamp(value: T, node: &str, timestamp: u64) -> Self {
        Self {
            value,
            timestamp,
            node: node.to_string(),
        }
    }

    pub fn set(&mut self, value: T, node: &str) {
        self.value = value;
        self.timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        self.node = node.to_string();
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn node(&self) -> &str {
        &self.node
    }
}

impl<T: Clone + fmt::Debug + Send + Sync + Serialize + for<'de> Deserialize<'de>> CrdtValue for LWWRegister<T> {
    fn merge(&mut self, other: &Self) {
        if other.timestamp > self.timestamp
            || (other.timestamp == self.timestamp && other.node > self.node)
        {
            self.value = other.value.clone();
            self.timestamp = other.timestamp;
            self.node = other.node.clone();
        }
    }
}

// ============= ORMap =============

/// Observed-Remove Map. Keys can be added and removed; values are CRDTs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ORMap<K, V>
where
    K: Eq + Hash + Clone + fmt::Debug + Send + Sync,
    V: CrdtValue,
{
    keys: ORSet<K>,
    values: HashMap<K, V>,
    node: String,
}

impl<K, V> ORMap<K, V>
where
    K: Eq + Hash + Clone + fmt::Debug + Send + Sync + Serialize + for<'de> Deserialize<'de>,
    V: CrdtValue + Serialize + for<'de> Deserialize<'de>,
{
    pub fn new(node: &str) -> Self {
        Self {
            keys: ORSet::new(),
            values: HashMap::new(),
            node: node.to_string(),
        }
    }

    pub fn put(&mut self, key: K, value: V) {
        self.keys.add(&self.node, key.clone());
        self.values.insert(key, value);
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        if self.keys.contains(key) {
            self.values.get(key)
        } else {
            None
        }
    }

    pub fn remove(&mut self, key: &K) {
        self.keys.remove(key);
        self.values.remove(key);
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.keys.contains(key)
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

impl<K, V> CrdtValue for ORMap<K, V>
where
    K: Eq + Hash + Clone + fmt::Debug + Send + Sync + Serialize + for<'de> Deserialize<'de>,
    V: CrdtValue + Serialize + for<'de> Deserialize<'de>,
{
    fn merge(&mut self, other: &Self) {
        self.keys.merge(&other.keys);
        for (key, other_val) in &other.values {
            if let Some(our_val) = self.values.get_mut(key) {
                our_val.merge(other_val);
            } else {
                self.values.insert(key.clone(), other_val.clone());
            }
        }
    }
}

// ============= Flag =============

/// Boolean CRDT flag. Once set to true, stays true forever.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Flag {
    enabled: bool,
}

impl Flag {
    pub fn new() -> Self {
        Self { enabled: false }
    }

    pub fn enable(&mut self) {
        self.enabled = true;
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl CrdtValue for Flag {
    fn merge(&mut self, other: &Self) {
        self.enabled = self.enabled || other.enabled;
    }
}

// ============= Replicator =============

/// Read consistency level.
#[derive(Debug, Clone)]
pub enum ReadConsistency {
    /// Read from local replica only.
    Local,
    /// Read from a majority of replicas.
    Majority,
    /// Read from all replicas.
    All,
    /// Read from at least N replicas.
    ReadFrom(usize),
}

/// Write consistency level.
#[derive(Debug, Clone)]
pub enum WriteConsistency {
    /// Write to local replica only.
    Local,
    /// Write to a majority of replicas.
    Majority,
    /// Write to all replicas.
    All,
    /// Write to at least N replicas.
    WriteTo(usize),
}

/// A key in the Replicator's distributed data store.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataKey {
    pub id: String,
}

impl DataKey {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}

impl fmt::Display for DataKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DataKey({})", self.id)
    }
}

/// Replicator manages CRDT replication across cluster nodes.
pub struct Replicator {
    node_id: String,
    data: RwLock<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>,
    /// Subscriptions: key → list of callback IDs
    subscriptions: RwLock<HashMap<String, Vec<String>>>,
}

impl Replicator {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            data: RwLock::new(HashMap::new()),
            subscriptions: RwLock::new(HashMap::new()),
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Update a CRDT value. If the key doesn't exist, create it with the initial value.
    pub async fn update<V: CrdtValue + 'static>(
        &self,
        key: &DataKey,
        initial: V,
        modify: impl FnOnce(&mut V),
    ) {
        let mut data = self.data.write().await;
        let entry = data.entry(key.id.clone()).or_insert_with(|| Box::new(initial));
        if let Some(value) = entry.downcast_mut::<V>() {
            modify(value);
        }
    }

    /// Get the current value for a key.
    pub async fn get<V: CrdtValue + Clone + 'static>(&self, key: &DataKey) -> Option<V> {
        let data = self.data.read().await;
        data.get(&key.id)
            .and_then(|v| v.downcast_ref::<V>())
            .cloned()
    }

    /// Delete a key from the local store.
    pub async fn delete(&self, key: &DataKey) {
        let mut data = self.data.write().await;
        data.remove(&key.id);
    }

    /// Subscribe to changes for a key.
    pub async fn subscribe(&self, key: &DataKey, subscriber_id: String) {
        let mut subs = self.subscriptions.write().await;
        subs.entry(key.id.clone()).or_default().push(subscriber_id);
    }

    /// Get all keys in the store.
    pub async fn keys(&self) -> Vec<String> {
        let data = self.data.read().await;
        data.keys().cloned().collect()
    }

    /// Number of entries in the store.
    pub async fn size(&self) -> usize {
        let data = self.data.read().await;
        data.len()
    }
}

// ============= Tests =============

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gcounter_basic() {
        let mut c = GCounter::new();
        c.increment("node-a", 3);
        c.increment("node-b", 5);
        c.increment("node-a", 2);
        assert_eq!(c.value(), 10);
        assert_eq!(c.node_value("node-a"), 5);
        assert_eq!(c.node_value("node-b"), 5);
    }

    #[test]
    fn test_gcounter_merge() {
        let mut c1 = GCounter::new();
        c1.increment("a", 3);
        c1.increment("b", 2);

        let mut c2 = GCounter::new();
        c2.increment("a", 1);
        c2.increment("c", 4);

        c1.merge(&c2);
        assert_eq!(c1.value(), 9); // a=3, b=2, c=4
        assert_eq!(c1.node_value("a"), 3); // max(3, 1)
    }

    #[test]
    fn test_pncounter() {
        let mut c = PNCounter::new();
        c.increment("a", 10);
        c.decrement("b", 3);
        assert_eq!(c.value(), 7);
    }

    #[test]
    fn test_pncounter_merge() {
        let mut c1 = PNCounter::new();
        c1.increment("a", 10);
        c1.decrement("a", 3);

        let mut c2 = PNCounter::new();
        c2.increment("b", 5);
        c2.decrement("b", 1);

        c1.merge(&c2);
        assert_eq!(c1.value(), 11); // (10+5) - (3+1)
    }

    #[test]
    fn test_gset() {
        let mut s = GSet::new();
        s.insert("a".to_string());
        s.insert("b".to_string());
        s.insert("a".to_string()); // duplicate
        assert_eq!(s.len(), 2);
        assert!(s.contains(&"a".to_string()));
        assert!(!s.contains(&"c".to_string()));
    }

    #[test]
    fn test_gset_merge() {
        let mut s1 = GSet::new();
        s1.insert(1);
        s1.insert(2);

        let mut s2 = GSet::new();
        s2.insert(2);
        s2.insert(3);

        s1.merge(&s2);
        assert_eq!(s1.len(), 3);
        assert!(s1.contains(&1));
        assert!(s1.contains(&2));
        assert!(s1.contains(&3));
    }

    #[test]
    fn test_orset_add_remove() {
        let mut s = ORSet::new();
        s.add("node-a", "item1".to_string());
        s.add("node-a", "item2".to_string());
        assert_eq!(s.len(), 2);
        assert!(s.contains(&"item1".to_string()));

        s.remove(&"item1".to_string());
        assert_eq!(s.len(), 1);
        assert!(!s.contains(&"item1".to_string()));
        assert!(s.contains(&"item2".to_string()));
    }

    #[test]
    fn test_orset_merge() {
        let mut s1 = ORSet::new();
        s1.add("a", "x".to_string());

        let mut s2 = ORSet::new();
        s2.add("b", "y".to_string());

        s1.merge(&s2);
        assert!(s1.contains(&"x".to_string()));
        assert!(s1.contains(&"y".to_string()));
    }

    #[test]
    fn test_lww_register() {
        let mut r1 = LWWRegister::with_timestamp("first".to_string(), "a", 100);
        let r2 = LWWRegister::with_timestamp("second".to_string(), "b", 200);
        r1.merge(&r2);
        assert_eq!(*r1.value(), "second"); // later timestamp wins
    }

    #[test]
    fn test_lww_register_same_timestamp() {
        let mut r1 = LWWRegister::with_timestamp("from-a".to_string(), "a", 100);
        let r2 = LWWRegister::with_timestamp("from-b".to_string(), "b", 100);
        r1.merge(&r2);
        // Same timestamp: higher node name wins ("b" > "a")
        assert_eq!(*r1.value(), "from-b");
    }

    #[test]
    fn test_ormap() {
        let mut m: ORMap<String, GCounter> = ORMap::new("node-a");
        let mut counter = GCounter::new();
        counter.increment("node-a", 5);
        m.put("key1".to_string(), counter);

        assert!(m.contains_key(&"key1".to_string()));
        assert_eq!(m.get(&"key1".to_string()).unwrap().value(), 5);
        assert_eq!(m.len(), 1);

        m.remove(&"key1".to_string());
        assert_eq!(m.len(), 0);
    }

    #[test]
    fn test_ormap_merge() {
        let mut m1: ORMap<String, GCounter> = ORMap::new("a");
        let mut c1 = GCounter::new();
        c1.increment("a", 3);
        m1.put("counter".to_string(), c1);

        let mut m2: ORMap<String, GCounter> = ORMap::new("b");
        let mut c2 = GCounter::new();
        c2.increment("b", 5);
        m2.put("counter".to_string(), c2);

        m1.merge(&m2);
        let merged = m1.get(&"counter".to_string()).unwrap();
        assert_eq!(merged.value(), 8); // a=3 + b=5
    }

    #[test]
    fn test_flag() {
        let mut f1 = Flag::new();
        assert!(!f1.is_enabled());

        f1.enable();
        assert!(f1.is_enabled());

        let f2 = Flag::new();
        let mut f3 = Flag::new();
        f3.merge(&f1);
        assert!(f3.is_enabled()); // once true, stays true

        let mut f4 = Flag::new();
        f4.merge(&f2);
        assert!(!f4.is_enabled()); // both false → false
    }

    #[test]
    fn test_data_key() {
        let key = DataKey::new("my-counter");
        assert_eq!(key.id, "my-counter");
        assert_eq!(format!("{}", key), "DataKey(my-counter)");
    }

    #[tokio::test]
    async fn test_replicator_update_get() {
        let rep = Replicator::new("node-a");
        let key = DataKey::new("counter-1");

        rep.update(&key, GCounter::new(), |c| {
            c.increment("node-a", 10);
        }).await;

        let value: Option<GCounter> = rep.get(&key).await;
        assert!(value.is_some());
        assert_eq!(value.unwrap().value(), 10);
    }

    #[tokio::test]
    async fn test_replicator_delete() {
        let rep = Replicator::new("node-a");
        let key = DataKey::new("temp");
        rep.update(&key, GCounter::new(), |c| c.increment("a", 1)).await;
        assert_eq!(rep.size().await, 1);

        rep.delete(&key).await;
        assert_eq!(rep.size().await, 0);
    }

    #[tokio::test]
    async fn test_replicator_keys() {
        let rep = Replicator::new("node-a");
        rep.update(&DataKey::new("k1"), GCounter::new(), |_| {}).await;
        rep.update(&DataKey::new("k2"), GCounter::new(), |_| {}).await;

        let mut keys = rep.keys().await;
        keys.sort();
        assert_eq!(keys, vec!["k1", "k2"]);
    }

    #[tokio::test]
    async fn test_replicator_subscribe() {
        let rep = Replicator::new("node-a");
        let key = DataKey::new("watched");
        rep.subscribe(&key, "sub-1".to_string()).await;

        let subs = rep.subscriptions.read().await;
        assert_eq!(subs.get("watched").unwrap().len(), 1);
    }

    #[test]
    fn test_read_write_consistency() {
        // Just ensure the types compile and are usable
        let _r = ReadConsistency::Majority;
        let _w = WriteConsistency::WriteTo(3);
    }
}

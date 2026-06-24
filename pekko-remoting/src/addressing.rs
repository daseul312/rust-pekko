//! Remote addressing for distributed actors

use crate::{AddressingError, AddressingResult};
use pekko_actor::ActorPath;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

/// Remote address for actor systems
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RemoteAddress {
    pub protocol: String,
    pub system: String,
    pub host: String,
    pub port: u16,
}

impl RemoteAddress {
    /// Create a new remote address
    pub fn new(protocol: &str, system: &str, host: &str, port: u16) -> Self {
        Self {
            protocol: protocol.to_string(),
            system: system.to_string(),
            host: host.to_string(),
            port,
        }
    }

    /// Create a TCP remote address
    pub fn tcp(system: &str, host: &str, port: u16) -> Self {
        Self::new("pekko.tcp", system, host, port)
    }

    /// Create a TLS remote address
    pub fn tls(system: &str, host: &str, port: u16) -> Self {
        Self::new("pekko.tls", system, host, port)
    }

    /// Get the socket address
    pub fn socket_addr(&self) -> AddressingResult<SocketAddr> {
        let ip: IpAddr = self.host.parse()
            .map_err(|_| AddressingError::InvalidFormat { 
                address: self.to_string() 
            })?;
        Ok(SocketAddr::new(ip, self.port))
    }

    /// Check if this address uses TLS
    pub fn is_tls(&self) -> bool {
        self.protocol.contains("tls") || self.protocol.contains("ssl")
    }

    /// Get the authority (host:port)
    pub fn authority(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Parse from string format: "pekko.tcp://system@host:port"
    pub fn parse(address_str: &str) -> AddressingResult<Self> {
        let parts: Vec<&str> = address_str.split("://").collect();
        if parts.len() != 2 {
            return Err(AddressingError::InvalidFormat {
                address: address_str.to_string(),
            });
        }

        let protocol = parts[0].to_string();
        let remainder = parts[1];

        let at_pos = remainder.find('@')
            .ok_or_else(|| AddressingError::InvalidFormat {
                address: address_str.to_string(),
            })?;

        let system = remainder[..at_pos].to_string();
        let host_port = &remainder[at_pos + 1..];

        let colon_pos = host_port.rfind(':')
            .ok_or_else(|| AddressingError::InvalidFormat {
                address: address_str.to_string(),
            })?;

        let host = host_port[..colon_pos].to_string();
        let port_str = &host_port[colon_pos + 1..];
        let port: u16 = port_str.parse()
            .map_err(|_| AddressingError::InvalidPort {
                port: port_str.to_string(),
            })?;

        Ok(Self {
            protocol,
            system,
            host,
            port,
        })
    }
}

impl fmt::Display for RemoteAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://{}@{}:{}", self.protocol, self.system, self.host, self.port)
    }
}

impl FromStr for RemoteAddress {
    type Err = AddressingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Remote actor path combining local path with remote address
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RemoteActorPath {
    pub address: RemoteAddress,
    pub path: ActorPath,
}

impl RemoteActorPath {
    /// Create a new remote actor path
    pub fn new(address: RemoteAddress, path: ActorPath) -> Self {
        Self { address, path }
    }

    /// Get the full path string
    pub fn full_path(&self) -> String {
        let path_elements = self.path.elements.join("/");
        if path_elements.is_empty() {
            format!("{}", self.address)
        } else {
            format!("{}/{}", self.address, path_elements)
        }
    }

    /// Parse from full path string
    pub fn parse(path_str: &str) -> AddressingResult<Self> {
        // Find the system boundary (first single slash after //)
        let double_slash_pos = path_str.find("://")
            .ok_or_else(|| AddressingError::InvalidPath {
                path: path_str.to_string(),
            })?;

        let after_protocol = &path_str[double_slash_pos + 3..];
        let path_start_pos = after_protocol.find('/')
            .ok_or_else(|| AddressingError::InvalidPath {
                path: path_str.to_string(),
            })?;

        let address_part = &path_str[..double_slash_pos + 3 + path_start_pos];
        let path_part = &path_str[double_slash_pos + 3 + path_start_pos..];

        let address = RemoteAddress::parse(address_part)?;
        
        // Create a local ActorPath with the path elements
        let elements: Vec<String> = if path_part.is_empty() {
            vec![]
        } else {
            path_part.split('/').filter(|s| !s.is_empty()).map(|s| s.to_string()).collect()
        };
        
        let local_address = pekko_actor::ActorAddress::local(&address.system);
        let path = pekko_actor::ActorPath::new(local_address, elements);

        Ok(Self::new(address, path))
    }

    /// Check if this is a local path
    pub fn is_local(&self, local_address: &RemoteAddress) -> bool {
        &self.address == local_address
    }

    /// Get the actor name
    pub fn actor_name(&self) -> Option<&str> {
        self.path.name()
    }

    /// Get the parent path
    pub fn parent(&self) -> Option<Self> {
        self.path.parent().map(|parent_path| Self {
            address: self.address.clone(),
            path: parent_path,
        })
    }

    /// Create a child path
    pub fn child(&self, name: &str) -> Self {
        Self {
            address: self.address.clone(),
            path: self.path.child(name),
        }
    }
}

impl fmt::Display for RemoteActorPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.full_path())
    }
}

impl FromStr for RemoteActorPath {
    type Err = AddressingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Unique node identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UniqueAddress {
    pub address: RemoteAddress,
    pub uid: u64,
}

impl UniqueAddress {
    pub fn new(address: RemoteAddress, uid: u64) -> Self {
        Self { address, uid }
    }

    /// Generate a new unique address with random UID
    pub fn generate(address: RemoteAddress) -> Self {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        use std::time::{SystemTime, UNIX_EPOCH};

        let mut hasher = DefaultHasher::new();
        address.hash(&mut hasher);
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .hash(&mut hasher);
        
        let uid = hasher.finish();
        Self::new(address, uid)
    }
}

impl fmt::Display for UniqueAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.address, self.uid)
    }
}

/// Address resolver for DNS and service discovery
#[async_trait::async_trait]
pub trait AddressResolver: Send + Sync {
    /// Resolve a hostname to IP addresses
    async fn resolve(&self, hostname: &str) -> AddressingResult<Vec<IpAddr>>;

    /// Resolve service name to addresses
    async fn resolve_service(&self, service: &str) -> AddressingResult<Vec<RemoteAddress>>;
}

/// DNS-based address resolver
pub struct DnsAddressResolver;

#[async_trait::async_trait]
impl AddressResolver for DnsAddressResolver {
    async fn resolve(&self, hostname: &str) -> AddressingResult<Vec<IpAddr>> {
        use tokio::net::lookup_host;

        let addresses = lookup_host(format!("{}:0", hostname))
            .await
            .map_err(|_| AddressingError::ResolutionFailed(format!("Cannot resolve hostname: {}", hostname)))?;

        Ok(addresses.map(|addr| addr.ip()).collect())
    }

    async fn resolve_service(&self, service: &str) -> AddressingResult<Vec<RemoteAddress>> {
        // This would integrate with service discovery systems like Consul, etcd, etc.
        // For now, return empty list
        let _ = service;
        Ok(Vec::new())
    }
}

/// Address book for caching resolved addresses
pub struct AddressBook {
    cache: dashmap::DashMap<String, Vec<RemoteAddress>>,
    resolver: Box<dyn AddressResolver>,
}

impl AddressBook {
    pub fn new(resolver: Box<dyn AddressResolver>) -> Self {
        Self {
            cache: dashmap::DashMap::new(),
            resolver,
        }
    }

    /// Resolve and cache addresses
    pub async fn resolve(&self, hostname: &str) -> AddressingResult<Vec<RemoteAddress>> {
        if let Some(cached) = self.cache.get(hostname) {
            return Ok(cached.clone());
        }

        let ips = self.resolver.resolve(hostname).await?;
        let addresses: Vec<RemoteAddress> = ips
            .into_iter()
            .map(|ip| RemoteAddress::tcp("unknown", &ip.to_string(), 2552))
            .collect();

        self.cache.insert(hostname.to_string(), addresses.clone());
        Ok(addresses)
    }

    /// Clear cache entry
    pub fn invalidate(&self, hostname: &str) {
        self.cache.remove(hostname);
    }

    /// Clear all cache entries
    pub fn clear(&self) {
        self.cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remote_address_creation() {
        let addr = RemoteAddress::tcp("test-system", "127.0.0.1", 2552);
        assert_eq!(addr.protocol, "pekko.tcp");
        assert_eq!(addr.system, "test-system");
        assert_eq!(addr.host, "127.0.0.1");
        assert_eq!(addr.port, 2552);
    }

    #[test]
    fn test_remote_address_parsing() {
        let addr_str = "pekko.tcp://test-system@127.0.0.1:2552";
        let addr = RemoteAddress::parse(addr_str).unwrap();
        
        assert_eq!(addr.protocol, "pekko.tcp");
        assert_eq!(addr.system, "test-system");
        assert_eq!(addr.host, "127.0.0.1");
        assert_eq!(addr.port, 2552);
        assert_eq!(addr.to_string(), addr_str);
    }

    #[test]
    fn test_remote_address_socket_addr() {
        let addr = RemoteAddress::tcp("test", "127.0.0.1", 2552);
        let socket = addr.socket_addr().unwrap();
        assert_eq!(socket.port(), 2552);
        assert_eq!(socket.ip().to_string(), "127.0.0.1");
    }

    #[test]
    fn test_remote_actor_path() {
        let address = RemoteAddress::tcp("test-system", "127.0.0.1", 2552);
        let local_path = ActorPath::parse("pekko://test-system/user/test-actor").unwrap();
        let remote_path = RemoteActorPath::new(address, local_path);
        
        assert_eq!(
            remote_path.full_path(),
            "pekko.tcp://test-system@127.0.0.1:2552/user/test-actor"
        );
    }

    #[test]
    fn test_remote_actor_path_parsing() {
        let path_str = "pekko.tcp://test-system@127.0.0.1:2552/user/test-actor";
        let remote_path = RemoteActorPath::parse(path_str).unwrap();
        
        assert_eq!(remote_path.address.system, "test-system");
        assert_eq!(remote_path.actor_name(), Some("test-actor"));
        assert_eq!(remote_path.to_string(), path_str);
    }

    #[test]
    fn test_unique_address_generation() {
        let address = RemoteAddress::tcp("test", "127.0.0.1", 2552);
        let unique1 = UniqueAddress::generate(address.clone());
        let unique2 = UniqueAddress::generate(address);
        
        assert_eq!(unique1.address, unique2.address);
        assert_ne!(unique1.uid, unique2.uid); // Should be different UIDs
    }

    #[test]
    fn test_remote_address_is_tls() {
        let tcp_addr = RemoteAddress::tcp("test", "127.0.0.1", 2552);
        let tls_addr = RemoteAddress::tls("test", "127.0.0.1", 2553);
        
        assert!(!tcp_addr.is_tls());
        assert!(tls_addr.is_tls());
    }

    #[tokio::test]
    async fn test_address_book() {
        let resolver = Box::new(DnsAddressResolver);
        let book = AddressBook::new(resolver);
        
        // Test that we can create an address book
        book.clear();
        assert_eq!(book.cache.len(), 0);
    }
}
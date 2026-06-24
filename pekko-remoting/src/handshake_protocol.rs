// handshake_protocol.rs - Full Handshake Protocol
// Multi-step handshake: Hello → Challenge → Response → Accept/Reject
// Version negotiation, authentication, feature capability exchange

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::{Duration, Instant};

use crate::addressing::RemoteAddress;

// ============================================================================
// Protocol Version
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProtocolVersion {
    pub major: u32,
    pub minor: u32,
}

impl ProtocolVersion {
    pub fn new(major: u32, minor: u32) -> Self {
        Self { major, minor }
    }

    /// Current protocol version
    pub fn current() -> Self {
        Self::new(1, 0)
    }

    pub fn is_compatible_with(&self, other: &ProtocolVersion) -> bool {
        self.major == other.major
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

// ============================================================================
// Capability Flags
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Capability {
    Compression,
    Encryption,
    Streaming,
    Sharding,
    PubSub,
    EventBus,
    Persistence,
    Custom(String),
}

impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Capability::Compression => write!(f, "compression"),
            Capability::Encryption => write!(f, "encryption"),
            Capability::Streaming => write!(f, "streaming"),
            Capability::Sharding => write!(f, "sharding"),
            Capability::PubSub => write!(f, "pub-sub"),
            Capability::EventBus => write!(f, "event-bus"),
            Capability::Persistence => write!(f, "persistence"),
            Capability::Custom(name) => write!(f, "custom:{}", name),
        }
    }
}

// ============================================================================
// Authentication
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum AuthMethod {
    None,
    SharedSecret,
    Token(String),
    Certificate,
}

#[derive(Debug, Clone)]
pub struct AuthChallenge {
    pub method: AuthMethod,
    pub nonce: Vec<u8>,
    pub realm: String,
}

impl AuthChallenge {
    pub fn none() -> Self {
        Self {
            method: AuthMethod::None,
            nonce: Vec::new(),
            realm: String::new(),
        }
    }

    pub fn shared_secret(nonce: Vec<u8>, realm: &str) -> Self {
        Self {
            method: AuthMethod::SharedSecret,
            nonce,
            realm: realm.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuthResponse {
    pub method: AuthMethod,
    pub proof: Vec<u8>,
}

impl AuthResponse {
    pub fn none() -> Self {
        Self {
            method: AuthMethod::None,
            proof: Vec::new(),
        }
    }

    pub fn with_proof(method: AuthMethod, proof: Vec<u8>) -> Self {
        Self { method, proof }
    }
}

// ============================================================================
// Handshake Messages
// ============================================================================

#[derive(Debug, Clone)]
pub struct HelloMessage {
    pub origin: RemoteAddress,
    pub protocol_version: ProtocolVersion,
    pub capabilities: HashSet<Capability>,
    pub system_name: String,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ChallengeMessage {
    pub challenge: AuthChallenge,
    pub server_capabilities: HashSet<Capability>,
    pub negotiated_version: ProtocolVersion,
}

#[derive(Debug, Clone)]
pub struct ResponseMessage {
    pub response: AuthResponse,
    pub selected_capabilities: HashSet<Capability>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HandshakeResult {
    Accepted {
        negotiated_version: ProtocolVersion,
        capabilities: HashSet<Capability>,
        session_id: String,
    },
    Rejected {
        reason: RejectReason,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum RejectReason {
    IncompatibleVersion {
        required: ProtocolVersion,
        offered: ProtocolVersion,
    },
    AuthenticationFailed,
    SystemMismatch {
        expected: String,
        got: String,
    },
    QuarantinedNode,
    MaxConnectionsReached,
    Custom(String),
}

impl fmt::Display for RejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RejectReason::IncompatibleVersion { required, offered } =>
                write!(f, "Incompatible version: need {}, got {}", required, offered),
            RejectReason::AuthenticationFailed =>
                write!(f, "Authentication failed"),
            RejectReason::SystemMismatch { expected, got } =>
                write!(f, "System mismatch: expected '{}', got '{}'", expected, got),
            RejectReason::QuarantinedNode =>
                write!(f, "Node is quarantined"),
            RejectReason::MaxConnectionsReached =>
                write!(f, "Max connections reached"),
            RejectReason::Custom(msg) =>
                write!(f, "{}", msg),
        }
    }
}

// ============================================================================
// Handshake State Machine
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum HandshakeState {
    Idle,
    HelloSent,
    ChallengeReceived,
    ResponseSent,
    Accepted,
    Rejected(RejectReason),
    TimedOut,
}

impl HandshakeState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            HandshakeState::Accepted
                | HandshakeState::Rejected(_)
                | HandshakeState::TimedOut
        )
    }

    pub fn is_successful(&self) -> bool {
        matches!(self, HandshakeState::Accepted)
    }
}

// ============================================================================
// Handshake Session
// ============================================================================

#[derive(Debug)]
pub struct HandshakeSession {
    pub state: HandshakeState,
    pub remote_address: RemoteAddress,
    pub started_at: Instant,
    pub timeout: Duration,
    pub hello: Option<HelloMessage>,
    pub challenge: Option<ChallengeMessage>,
    pub response: Option<ResponseMessage>,
    pub result: Option<HandshakeResult>,
}

impl HandshakeSession {
    pub fn new(remote_address: RemoteAddress, timeout: Duration) -> Self {
        Self {
            state: HandshakeState::Idle,
            remote_address,
            started_at: Instant::now(),
            timeout,
            hello: None,
            challenge: None,
            response: None,
            result: None,
        }
    }

    pub fn send_hello(&mut self, hello: HelloMessage) -> Result<(), String> {
        if self.state != HandshakeState::Idle {
            return Err(format!("Cannot send Hello in state {:?}", self.state));
        }
        self.hello = Some(hello);
        self.state = HandshakeState::HelloSent;
        Ok(())
    }

    pub fn receive_challenge(&mut self, challenge: ChallengeMessage) -> Result<(), String> {
        if self.state != HandshakeState::HelloSent {
            return Err(format!("Cannot receive Challenge in state {:?}", self.state));
        }
        if self.is_timed_out() {
            self.state = HandshakeState::TimedOut;
            return Err("Handshake timed out".into());
        }
        self.challenge = Some(challenge);
        self.state = HandshakeState::ChallengeReceived;
        Ok(())
    }

    pub fn send_response(&mut self, response: ResponseMessage) -> Result<(), String> {
        if self.state != HandshakeState::ChallengeReceived {
            return Err(format!("Cannot send Response in state {:?}", self.state));
        }
        if self.is_timed_out() {
            self.state = HandshakeState::TimedOut;
            return Err("Handshake timed out".into());
        }
        self.response = Some(response);
        self.state = HandshakeState::ResponseSent;
        Ok(())
    }

    pub fn accept(&mut self, result: HandshakeResult) -> Result<(), String> {
        if self.state != HandshakeState::ResponseSent {
            return Err(format!("Cannot accept in state {:?}", self.state));
        }
        match &result {
            HandshakeResult::Accepted { .. } => {
                self.state = HandshakeState::Accepted;
            }
            HandshakeResult::Rejected { reason } => {
                self.state = HandshakeState::Rejected(reason.clone());
            }
        }
        self.result = Some(result);
        Ok(())
    }

    pub fn is_timed_out(&self) -> bool {
        self.started_at.elapsed() > self.timeout
    }

    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }
}

// ============================================================================
// Handshake Coordinator
// ============================================================================

#[derive(Debug)]
pub struct HandshakeCoordinator {
    pub local_address: RemoteAddress,
    pub system_name: String,
    pub protocol_version: ProtocolVersion,
    pub capabilities: HashSet<Capability>,
    pub auth_method: AuthMethod,
    pub handshake_timeout: Duration,
    pub max_concurrent: usize,
    active_sessions: HashMap<RemoteAddress, HandshakeSession>,
    completed: u64,
    failed: u64,
}

impl HandshakeCoordinator {
    pub fn new(
        local_address: RemoteAddress,
        system_name: &str,
        capabilities: HashSet<Capability>,
    ) -> Self {
        Self {
            local_address,
            system_name: system_name.to_string(),
            protocol_version: ProtocolVersion::current(),
            capabilities,
            auth_method: AuthMethod::None,
            handshake_timeout: Duration::from_secs(10),
            max_concurrent: 100,
            active_sessions: HashMap::new(),
            completed: 0,
            failed: 0,
        }
    }

    pub fn with_auth(mut self, method: AuthMethod) -> Self {
        self.auth_method = method;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Initiate a handshake to a remote node
    pub fn initiate(&mut self, remote_address: RemoteAddress) -> Result<HelloMessage, String> {
        if self.active_sessions.len() >= self.max_concurrent {
            return Err("Max concurrent handshakes reached".into());
        }

        if self.active_sessions.contains_key(&remote_address) {
            return Err(format!("Handshake already in progress for {}", remote_address));
        }

        let hello = HelloMessage {
            origin: self.local_address.clone(),
            protocol_version: self.protocol_version.clone(),
            capabilities: self.capabilities.clone(),
            system_name: self.system_name.clone(),
            metadata: HashMap::new(),
        };

        let mut session = HandshakeSession::new(
            remote_address.clone(),
            self.handshake_timeout,
        );
        session.send_hello(hello.clone()).map_err(|e| e.to_string())?;
        self.active_sessions.insert(remote_address, session);

        Ok(hello)
    }

    /// Process an incoming Hello (server side)
    pub fn process_hello(
        &self,
        hello: &HelloMessage,
    ) -> Result<ChallengeMessage, RejectReason> {
        // Version check
        if !hello.protocol_version.is_compatible_with(&self.protocol_version) {
            return Err(RejectReason::IncompatibleVersion {
                required: self.protocol_version.clone(),
                offered: hello.protocol_version.clone(),
            });
        }

        // Negotiate version (use minimum)
        let negotiated = if hello.protocol_version <= self.protocol_version {
            hello.protocol_version.clone()
        } else {
            self.protocol_version.clone()
        };

        // Build challenge
        let challenge = match &self.auth_method {
            AuthMethod::None => AuthChallenge::none(),
            AuthMethod::SharedSecret => {
                AuthChallenge::shared_secret(
                    vec![1, 2, 3, 4, 5, 6, 7, 8], // In reality, use crypto random
                    &self.system_name,
                )
            }
            _ => AuthChallenge::none(),
        };

        Ok(ChallengeMessage {
            challenge,
            server_capabilities: self.capabilities.clone(),
            negotiated_version: negotiated,
        })
    }

    /// Verify authentication response
    pub fn verify_auth(
        &self,
        challenge: &AuthChallenge,
        response: &AuthResponse,
    ) -> bool {
        match (&challenge.method, &response.method) {
            (AuthMethod::None, AuthMethod::None) => true,
            (AuthMethod::SharedSecret, AuthMethod::SharedSecret) => {
                // Simplified: in reality, verify HMAC(nonce, secret)
                !response.proof.is_empty()
            }
            _ => challenge.method == response.method,
        }
    }

    /// Negotiate shared capabilities
    pub fn negotiate_capabilities(
        &self,
        client_caps: &HashSet<Capability>,
        server_caps: &HashSet<Capability>,
    ) -> HashSet<Capability> {
        client_caps.intersection(server_caps).cloned().collect()
    }

    /// Complete a handshake session
    pub fn complete_session(
        &mut self,
        remote_address: &RemoteAddress,
        result: HandshakeResult,
    ) -> Result<(), String> {
        let session = self
            .active_sessions
            .get_mut(remote_address)
            .ok_or("No active session for this address")?;

        match &result {
            HandshakeResult::Accepted { .. } => {
                self.completed += 1;
            }
            HandshakeResult::Rejected { .. } => {
                self.failed += 1;
            }
        }

        session.result = Some(result);
        session.state = HandshakeState::Accepted;
        Ok(())
    }

    /// Remove completed/timed-out sessions
    pub fn cleanup(&mut self) -> Vec<RemoteAddress> {
        let timed_out: Vec<RemoteAddress> = self
            .active_sessions
            .iter()
            .filter(|(_, s)| s.is_timed_out() || s.state.is_terminal())
            .map(|(addr, _)| addr.clone())
            .collect();

        for addr in &timed_out {
            if let Some(session) = self.active_sessions.get(addr) {
                if session.is_timed_out() && !session.state.is_terminal() {
                    self.failed += 1;
                }
            }
            self.active_sessions.remove(addr);
        }

        timed_out
    }

    pub fn active_count(&self) -> usize {
        self.active_sessions.len()
    }

    pub fn completed_count(&self) -> u64 {
        self.completed
    }

    pub fn failed_count(&self) -> u64 {
        self.failed
    }

    pub fn get_session(&self, addr: &RemoteAddress) -> Option<&HandshakeSession> {
        self.active_sessions.get(addr)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn local_addr() -> RemoteAddress {
        RemoteAddress::tcp("local", "127.0.0.1", 2551)
    }

    fn remote_addr() -> RemoteAddress {
        RemoteAddress::tcp("remote", "127.0.0.1", 2552)
    }

    fn default_caps() -> HashSet<Capability> {
        let mut caps = HashSet::new();
        caps.insert(Capability::Compression);
        caps.insert(Capability::Streaming);
        caps.insert(Capability::Sharding);
        caps
    }

    // === ProtocolVersion Tests ===

    #[test]
    fn test_protocol_version() {
        let v = ProtocolVersion::current();
        assert_eq!(v.major, 1);
        assert_eq!(v.minor, 0);
        assert_eq!(format!("{}", v), "1.0");
    }

    #[test]
    fn test_version_compatibility() {
        let v1 = ProtocolVersion::new(1, 0);
        let v1_1 = ProtocolVersion::new(1, 1);
        let v2 = ProtocolVersion::new(2, 0);

        assert!(v1.is_compatible_with(&v1_1)); // same major
        assert!(!v1.is_compatible_with(&v2));   // different major
    }

    // === Capability Tests ===

    #[test]
    fn test_capability_display() {
        assert_eq!(format!("{}", Capability::Compression), "compression");
        assert_eq!(format!("{}", Capability::Custom("x".into())), "custom:x");
    }

    // === HandshakeState Tests ===

    #[test]
    fn test_handshake_state_terminal() {
        assert!(!HandshakeState::Idle.is_terminal());
        assert!(!HandshakeState::HelloSent.is_terminal());
        assert!(HandshakeState::Accepted.is_terminal());
        assert!(HandshakeState::Rejected(RejectReason::AuthenticationFailed).is_terminal());
        assert!(HandshakeState::TimedOut.is_terminal());
    }

    #[test]
    fn test_handshake_state_successful() {
        assert!(HandshakeState::Accepted.is_successful());
        assert!(!HandshakeState::Rejected(RejectReason::AuthenticationFailed).is_successful());
    }

    // === HandshakeSession Tests ===

    #[test]
    fn test_session_lifecycle() {
        let mut session = HandshakeSession::new(
            remote_addr(),
            Duration::from_secs(10),
        );

        // Hello
        let hello = HelloMessage {
            origin: local_addr(),
            protocol_version: ProtocolVersion::current(),
            capabilities: default_caps(),
            system_name: "test".into(),
            metadata: HashMap::new(),
        };
        session.send_hello(hello).unwrap();
        assert_eq!(session.state, HandshakeState::HelloSent);

        // Challenge
        let challenge = ChallengeMessage {
            challenge: AuthChallenge::none(),
            server_capabilities: default_caps(),
            negotiated_version: ProtocolVersion::current(),
        };
        session.receive_challenge(challenge).unwrap();
        assert_eq!(session.state, HandshakeState::ChallengeReceived);

        // Response
        let response = ResponseMessage {
            response: AuthResponse::none(),
            selected_capabilities: default_caps(),
        };
        session.send_response(response).unwrap();
        assert_eq!(session.state, HandshakeState::ResponseSent);

        // Accept
        let result = HandshakeResult::Accepted {
            negotiated_version: ProtocolVersion::current(),
            capabilities: default_caps(),
            session_id: "sess-1".into(),
        };
        session.accept(result).unwrap();
        assert_eq!(session.state, HandshakeState::Accepted);
    }

    #[test]
    fn test_session_reject() {
        let mut session = HandshakeSession::new(remote_addr(), Duration::from_secs(10));

        let hello = HelloMessage {
            origin: local_addr(),
            protocol_version: ProtocolVersion::current(),
            capabilities: HashSet::new(),
            system_name: "t".into(),
            metadata: HashMap::new(),
        };
        session.send_hello(hello).unwrap();
        session.receive_challenge(ChallengeMessage {
            challenge: AuthChallenge::none(),
            server_capabilities: HashSet::new(),
            negotiated_version: ProtocolVersion::current(),
        }).unwrap();
        session.send_response(ResponseMessage {
            response: AuthResponse::none(),
            selected_capabilities: HashSet::new(),
        }).unwrap();

        let result = HandshakeResult::Rejected {
            reason: RejectReason::AuthenticationFailed,
        };
        session.accept(result).unwrap();
        assert!(matches!(session.state, HandshakeState::Rejected(_)));
    }

    #[test]
    fn test_session_wrong_state() {
        let mut session = HandshakeSession::new(remote_addr(), Duration::from_secs(10));

        // Can't receive challenge before Hello
        let challenge = ChallengeMessage {
            challenge: AuthChallenge::none(),
            server_capabilities: HashSet::new(),
            negotiated_version: ProtocolVersion::current(),
        };
        assert!(session.receive_challenge(challenge).is_err());
    }

    // === HandshakeCoordinator Tests ===

    #[test]
    fn test_coordinator_initiate() {
        let mut coord = HandshakeCoordinator::new(
            local_addr(),
            "test-system",
            default_caps(),
        );

        let hello = coord.initiate(remote_addr()).unwrap();
        assert_eq!(hello.system_name, "test-system");
        assert_eq!(coord.active_count(), 1);
    }

    #[test]
    fn test_coordinator_duplicate_initiate() {
        let mut coord = HandshakeCoordinator::new(
            local_addr(),
            "test",
            HashSet::new(),
        );

        coord.initiate(remote_addr()).unwrap();
        assert!(coord.initiate(remote_addr()).is_err());
    }

    #[test]
    fn test_coordinator_process_hello_compatible() {
        let coord = HandshakeCoordinator::new(
            local_addr(),
            "test",
            default_caps(),
        );

        let hello = HelloMessage {
            origin: remote_addr(),
            protocol_version: ProtocolVersion::current(),
            capabilities: default_caps(),
            system_name: "remote".into(),
            metadata: HashMap::new(),
        };

        let challenge = coord.process_hello(&hello).unwrap();
        assert_eq!(challenge.negotiated_version, ProtocolVersion::current());
    }

    #[test]
    fn test_coordinator_process_hello_incompatible() {
        let coord = HandshakeCoordinator::new(
            local_addr(),
            "test",
            HashSet::new(),
        );

        let hello = HelloMessage {
            origin: remote_addr(),
            protocol_version: ProtocolVersion::new(99, 0),
            capabilities: HashSet::new(),
            system_name: "remote".into(),
            metadata: HashMap::new(),
        };

        let err = coord.process_hello(&hello).unwrap_err();
        assert!(matches!(err, RejectReason::IncompatibleVersion { .. }));
    }

    #[test]
    fn test_negotiate_capabilities() {
        let coord = HandshakeCoordinator::new(
            local_addr(),
            "test",
            default_caps(),
        );

        let mut client_caps = HashSet::new();
        client_caps.insert(Capability::Compression);
        client_caps.insert(Capability::Encryption);

        let result = coord.negotiate_capabilities(&client_caps, &default_caps());
        assert!(result.contains(&Capability::Compression));
        assert!(!result.contains(&Capability::Encryption)); // not in server
        assert!(!result.contains(&Capability::Streaming));  // not in client
    }

    #[test]
    fn test_verify_auth_none() {
        let coord = HandshakeCoordinator::new(local_addr(), "t", HashSet::new());
        let challenge = AuthChallenge::none();
        let response = AuthResponse::none();
        assert!(coord.verify_auth(&challenge, &response));
    }

    #[test]
    fn test_verify_auth_shared_secret() {
        let coord = HandshakeCoordinator::new(local_addr(), "t", HashSet::new())
            .with_auth(AuthMethod::SharedSecret);
        let challenge = AuthChallenge::shared_secret(vec![1, 2, 3], "test");
        let response = AuthResponse::with_proof(AuthMethod::SharedSecret, vec![9, 8, 7]);
        assert!(coord.verify_auth(&challenge, &response));
    }

    #[test]
    fn test_coordinator_complete_session() {
        let mut coord = HandshakeCoordinator::new(
            local_addr(),
            "test",
            default_caps(),
        );

        let addr = remote_addr();
        coord.initiate(addr.clone()).unwrap();

        let result = HandshakeResult::Accepted {
            negotiated_version: ProtocolVersion::current(),
            capabilities: default_caps(),
            session_id: "s1".into(),
        };
        coord.complete_session(&addr, result).unwrap();
        assert_eq!(coord.completed_count(), 1);
    }

    #[test]
    fn test_coordinator_cleanup() {
        let mut coord = HandshakeCoordinator::new(
            local_addr(),
            "test",
            HashSet::new(),
        ).with_timeout(Duration::from_millis(0)); // instant timeout

        coord.initiate(remote_addr()).unwrap();
        // Session should be timed out immediately
        std::thread::sleep(Duration::from_millis(1));

        let cleaned = coord.cleanup();
        assert_eq!(cleaned.len(), 1);
        assert_eq!(coord.active_count(), 0);
        assert_eq!(coord.failed_count(), 1);
    }

    #[test]
    fn test_reject_reason_display() {
        let r = RejectReason::IncompatibleVersion {
            required: ProtocolVersion::new(1, 0),
            offered: ProtocolVersion::new(2, 0),
        };
        assert!(format!("{}", r).contains("1.0"));
        assert!(format!("{}", r).contains("2.0"));
    }

    #[test]
    fn test_max_concurrent_handshakes() {
        let mut coord = HandshakeCoordinator::new(local_addr(), "t", HashSet::new());
        coord.max_concurrent = 1;

        coord.initiate(RemoteAddress::tcp("a", "127.0.0.1", 3000)).unwrap();
        assert!(coord.initiate(RemoteAddress::tcp("b", "127.0.0.1", 3001)).is_err());
    }
}

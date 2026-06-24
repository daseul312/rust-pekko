//! Actor 시스템 실시간 모니터링
//!
//! WebSocket을 통해 Actor 메시지 이벤트를 브라우저로 전송합니다.

pub mod websocket_monitor;
pub use websocket_monitor::*;

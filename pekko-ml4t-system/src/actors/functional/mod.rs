//! 함수형 프로그래밍 스타일 Actor들
//!
//! 함수형 Actor는 불변 상태와 순수 함수를 사용하여 예측 가능하고 테스트하기 쉬운 액터를 구현합니다.

pub mod llm_analysis_actor;
pub mod market_data_actor;
pub mod sentiment_actor;
pub mod report_actor;
pub mod prediction_actor;

// 공개 API 재내보내기
pub use llm_analysis_actor::*;
pub use market_data_actor::*;
pub use sentiment_actor::*;
pub use report_actor::*;
pub use prediction_actor::*;

/// 함수형 Actor 모듈 초기화
pub fn init() {
}
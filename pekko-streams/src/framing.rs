//! # Framing - Byte Stream Framing
//!
//! Provides delimiter-based and length-prefix framing for byte streams.
//! Based on Akka/Pekko Streams Framing for TCP/network stream parsing.
//!
//! ## Features
//! - Delimiter framing (newline, custom delimiters)
//! - Length-prefix framing (big/little endian, custom header size)
//! - Maximum frame length enforcement
//! - Encoding helpers (frame → bytes with delimiters/prefixes)

use std::collections::VecDeque;

/// Byte order for length-prefix framing
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ByteOrder {
    BigEndian,
    LittleEndian,
}

/// Configuration for delimiter-based framing
#[derive(Debug, Clone)]
pub struct DelimiterFramingConfig {
    /// Delimiter bytes
    pub delimiter: Vec<u8>,
    /// Maximum frame length (excluding delimiter)
    pub max_frame_length: usize,
    /// Whether to include the delimiter in the output frame
    pub include_delimiter: bool,
    /// Allow truncated last frame (no trailing delimiter)
    pub allow_truncation: bool,
}

impl DelimiterFramingConfig {
    pub fn new(delimiter: Vec<u8>, max_frame_length: usize) -> Self {
        Self {
            delimiter,
            max_frame_length,
            include_delimiter: false,
            allow_truncation: false,
        }
    }

    pub fn newline(max_frame_length: usize) -> Self {
        Self::new(vec![b'\n'], max_frame_length)
    }

    pub fn crlf(max_frame_length: usize) -> Self {
        Self::new(vec![b'\r', b'\n'], max_frame_length)
    }

    pub fn null_terminated(max_frame_length: usize) -> Self {
        Self::new(vec![0u8], max_frame_length)
    }

    pub fn with_include_delimiter(mut self, include: bool) -> Self {
        self.include_delimiter = include;
        self
    }

    pub fn with_allow_truncation(mut self, allow: bool) -> Self {
        self.allow_truncation = allow;
        self
    }
}

/// Delimiter-based framing decoder
pub struct DelimiterFraming {
    config: DelimiterFramingConfig,
    buffer: Vec<u8>,
    frames: VecDeque<Vec<u8>>,
    completed: bool,
}

impl DelimiterFraming {
    pub fn new(config: DelimiterFramingConfig) -> Self {
        Self {
            config,
            buffer: Vec::new(),
            frames: VecDeque::new(),
            completed: false,
        }
    }

    /// Feed bytes into the framing decoder
    pub fn feed(&mut self, data: &[u8]) -> Result<usize, FramingError> {
        self.buffer.extend_from_slice(data);
        let mut frames_found = 0;

        loop {
            if let Some(pos) = self.find_delimiter() {
                let frame_end = pos;
                let delim_len = self.config.delimiter.len();

                if frame_end > self.config.max_frame_length {
                    return Err(FramingError::FrameTooLarge {
                        actual: frame_end,
                        max: self.config.max_frame_length,
                    });
                }

                let frame = if self.config.include_delimiter {
                    self.buffer[..frame_end + delim_len].to_vec()
                } else {
                    self.buffer[..frame_end].to_vec()
                };

                self.frames.push_back(frame);
                self.buffer.drain(..frame_end + delim_len);
                frames_found += 1;
            } else {
                // Check if buffer exceeds max frame length
                if self.buffer.len() > self.config.max_frame_length {
                    return Err(FramingError::FrameTooLarge {
                        actual: self.buffer.len(),
                        max: self.config.max_frame_length,
                    });
                }
                break;
            }
        }

        Ok(frames_found)
    }

    /// Signal end of input
    pub fn complete(&mut self) -> Result<(), FramingError> {
        self.completed = true;
        if !self.buffer.is_empty() {
            if self.config.allow_truncation {
                let frame = std::mem::take(&mut self.buffer);
                self.frames.push_back(frame);
            } else {
                return Err(FramingError::IncompleteFrame(self.buffer.len()));
            }
        }
        Ok(())
    }

    /// Take the next decoded frame
    pub fn next_frame(&mut self) -> Option<Vec<u8>> {
        self.frames.pop_front()
    }

    /// Drain all available frames
    pub fn drain_frames(&mut self) -> Vec<Vec<u8>> {
        self.frames.drain(..).collect()
    }

    /// Number of pending frames
    pub fn pending_frames(&self) -> usize {
        self.frames.len()
    }

    /// Current buffer size (unflushed bytes)
    pub fn buffered_bytes(&self) -> usize {
        self.buffer.len()
    }

    fn find_delimiter(&self) -> Option<usize> {
        let delim = &self.config.delimiter;
        if delim.is_empty() || self.buffer.len() < delim.len() {
            return None;
        }
        self.buffer.windows(delim.len())
            .position(|w| w == delim.as_slice())
    }
}

/// Encode a frame with delimiter
pub fn encode_with_delimiter(data: &[u8], delimiter: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(data.len() + delimiter.len());
    result.extend_from_slice(data);
    result.extend_from_slice(delimiter);
    result
}

// ===================== Length-Prefix Framing =====================

/// Configuration for length-prefix framing
#[derive(Debug, Clone)]
pub struct LengthPrefixConfig {
    /// Number of bytes for the length field (1, 2, 4)
    pub length_field_size: usize,
    /// Byte order for the length field
    pub byte_order: ByteOrder,
    /// Maximum frame length (payload, excluding length field)
    pub max_frame_length: usize,
    /// Whether the length field includes its own size
    pub length_includes_header: bool,
}

impl LengthPrefixConfig {
    pub fn new(length_field_size: usize, max_frame_length: usize) -> Self {
        assert!(
            length_field_size == 1 || length_field_size == 2 || length_field_size == 4,
            "length_field_size must be 1, 2, or 4"
        );
        Self {
            length_field_size,
            byte_order: ByteOrder::BigEndian,
            max_frame_length,
            length_includes_header: false,
        }
    }

    pub fn with_byte_order(mut self, order: ByteOrder) -> Self {
        self.byte_order = order;
        self
    }

    pub fn with_length_includes_header(mut self, includes: bool) -> Self {
        self.length_includes_header = includes;
        self
    }
}

/// Length-prefix framing decoder
pub struct LengthPrefixFraming {
    config: LengthPrefixConfig,
    buffer: Vec<u8>,
    frames: VecDeque<Vec<u8>>,
}

impl LengthPrefixFraming {
    pub fn new(config: LengthPrefixConfig) -> Self {
        Self {
            config,
            buffer: Vec::new(),
            frames: VecDeque::new(),
        }
    }

    /// Feed bytes into the decoder
    pub fn feed(&mut self, data: &[u8]) -> Result<usize, FramingError> {
        self.buffer.extend_from_slice(data);
        let mut frames_found = 0;

        loop {
            if self.buffer.len() < self.config.length_field_size {
                break; // not enough for header
            }

            let payload_length = self.read_length()?;

            let total_size = self.config.length_field_size + payload_length;
            if self.buffer.len() < total_size {
                break; // not enough for full frame
            }

            let frame = self.buffer[self.config.length_field_size..total_size].to_vec();
            self.buffer.drain(..total_size);
            self.frames.push_back(frame);
            frames_found += 1;
        }

        Ok(frames_found)
    }

    /// Take the next decoded frame
    pub fn next_frame(&mut self) -> Option<Vec<u8>> {
        self.frames.pop_front()
    }

    /// Drain all available frames
    pub fn drain_frames(&mut self) -> Vec<Vec<u8>> {
        self.frames.drain(..).collect()
    }

    pub fn pending_frames(&self) -> usize {
        self.frames.len()
    }

    pub fn buffered_bytes(&self) -> usize {
        self.buffer.len()
    }

    fn read_length(&self) -> Result<usize, FramingError> {
        let raw = match self.config.length_field_size {
            1 => self.buffer[0] as u32,
            2 => {
                let bytes = [self.buffer[0], self.buffer[1]];
                match self.config.byte_order {
                    ByteOrder::BigEndian => u16::from_be_bytes(bytes) as u32,
                    ByteOrder::LittleEndian => u16::from_le_bytes(bytes) as u32,
                }
            }
            4 => {
                let bytes = [self.buffer[0], self.buffer[1], self.buffer[2], self.buffer[3]];
                match self.config.byte_order {
                    ByteOrder::BigEndian => u32::from_be_bytes(bytes),
                    ByteOrder::LittleEndian => u32::from_le_bytes(bytes),
                }
            }
            _ => return Err(FramingError::InvalidConfig("Invalid length field size".into())),
        };

        let payload_length = if self.config.length_includes_header {
            (raw as usize).saturating_sub(self.config.length_field_size)
        } else {
            raw as usize
        };

        if payload_length > self.config.max_frame_length {
            return Err(FramingError::FrameTooLarge {
                actual: payload_length,
                max: self.config.max_frame_length,
            });
        }

        Ok(payload_length)
    }
}

/// Encode data with a length prefix
pub fn encode_with_length_prefix(
    data: &[u8],
    length_field_size: usize,
    byte_order: ByteOrder,
    length_includes_header: bool,
) -> Vec<u8> {
    let length = if length_includes_header {
        data.len() + length_field_size
    } else {
        data.len()
    };

    let mut result = Vec::with_capacity(length_field_size + data.len());
    match length_field_size {
        1 => result.push(length as u8),
        2 => {
            let bytes = match byte_order {
                ByteOrder::BigEndian => (length as u16).to_be_bytes(),
                ByteOrder::LittleEndian => (length as u16).to_le_bytes(),
            };
            result.extend_from_slice(&bytes);
        }
        4 => {
            let bytes = match byte_order {
                ByteOrder::BigEndian => (length as u32).to_be_bytes(),
                ByteOrder::LittleEndian => (length as u32).to_le_bytes(),
            };
            result.extend_from_slice(&bytes);
        }
        _ => {}
    }
    result.extend_from_slice(data);
    result
}

/// Framing error types
#[derive(Debug, thiserror::Error, Clone)]
pub enum FramingError {
    #[error("Frame too large: {actual} bytes (max: {max})")]
    FrameTooLarge { actual: usize, max: usize },
    #[error("Incomplete frame at end of stream: {0} remaining bytes")]
    IncompleteFrame(usize),
    #[error("Invalid framing configuration: {0}")]
    InvalidConfig(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== Delimiter Framing Tests ==========

    #[test]
    fn test_delimiter_newline_basic() {
        let config = DelimiterFramingConfig::newline(1024);
        let mut framing = DelimiterFraming::new(config);

        let count = framing.feed(b"hello\nworld\n").unwrap();
        assert_eq!(count, 2);

        assert_eq!(framing.next_frame(), Some(b"hello".to_vec()));
        assert_eq!(framing.next_frame(), Some(b"world".to_vec()));
        assert_eq!(framing.next_frame(), None);
    }

    #[test]
    fn test_delimiter_crlf() {
        let config = DelimiterFramingConfig::crlf(1024);
        let mut framing = DelimiterFraming::new(config);

        framing.feed(b"line1\r\nline2\r\n").unwrap();
        assert_eq!(framing.next_frame(), Some(b"line1".to_vec()));
        assert_eq!(framing.next_frame(), Some(b"line2".to_vec()));
    }

    #[test]
    fn test_delimiter_null_terminated() {
        let config = DelimiterFramingConfig::null_terminated(256);
        let mut framing = DelimiterFraming::new(config);

        framing.feed(b"hello\0world\0").unwrap();
        assert_eq!(framing.drain_frames().len(), 2);
    }

    #[test]
    fn test_delimiter_include_delimiter() {
        let config = DelimiterFramingConfig::newline(1024)
            .with_include_delimiter(true);
        let mut framing = DelimiterFraming::new(config);

        framing.feed(b"hello\n").unwrap();
        assert_eq!(framing.next_frame(), Some(b"hello\n".to_vec()));
    }

    #[test]
    fn test_delimiter_chunked_input() {
        let config = DelimiterFramingConfig::newline(1024);
        let mut framing = DelimiterFraming::new(config);

        // Feed in chunks
        framing.feed(b"hel").unwrap();
        assert_eq!(framing.pending_frames(), 0);

        framing.feed(b"lo\nwor").unwrap();
        assert_eq!(framing.pending_frames(), 1);
        assert_eq!(framing.next_frame(), Some(b"hello".to_vec()));

        framing.feed(b"ld\n").unwrap();
        assert_eq!(framing.next_frame(), Some(b"world".to_vec()));
    }

    #[test]
    fn test_delimiter_frame_too_large() {
        let config = DelimiterFramingConfig::newline(5);
        let mut framing = DelimiterFraming::new(config);

        let err = framing.feed(b"toolongframe\n").unwrap_err();
        assert!(matches!(err, FramingError::FrameTooLarge { actual: 12, max: 5 }));
    }

    #[test]
    fn test_delimiter_allow_truncation() {
        let config = DelimiterFramingConfig::newline(1024)
            .with_allow_truncation(true);
        let mut framing = DelimiterFraming::new(config);

        framing.feed(b"complete\nincomplete").unwrap();
        assert_eq!(framing.next_frame(), Some(b"complete".to_vec()));

        framing.complete().unwrap();
        assert_eq!(framing.next_frame(), Some(b"incomplete".to_vec()));
    }

    #[test]
    fn test_delimiter_no_truncation() {
        let config = DelimiterFramingConfig::newline(1024);
        let mut framing = DelimiterFraming::new(config);

        framing.feed(b"no_delimiter").unwrap();
        let err = framing.complete().unwrap_err();
        assert!(matches!(err, FramingError::IncompleteFrame(12)));
    }

    #[test]
    fn test_delimiter_empty_frames() {
        let config = DelimiterFramingConfig::newline(1024);
        let mut framing = DelimiterFraming::new(config);

        framing.feed(b"\n\n\n").unwrap();
        let frames = framing.drain_frames();
        assert_eq!(frames.len(), 3);
        assert!(frames.iter().all(|f| f.is_empty()));
    }

    #[test]
    fn test_delimiter_custom() {
        let config = DelimiterFramingConfig::new(b"||".to_vec(), 1024);
        let mut framing = DelimiterFraming::new(config);

        framing.feed(b"a||b||c||").unwrap();
        let frames = framing.drain_frames();
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0], b"a");
        assert_eq!(frames[1], b"b");
        assert_eq!(frames[2], b"c");
    }

    #[test]
    fn test_encode_with_delimiter() {
        let encoded = encode_with_delimiter(b"hello", b"\n");
        assert_eq!(encoded, b"hello\n");
    }

    // ========== Length-Prefix Framing Tests ==========

    #[test]
    fn test_length_prefix_1byte_be() {
        let config = LengthPrefixConfig::new(1, 255);
        let mut framing = LengthPrefixFraming::new(config);

        // 5-byte payload
        let mut data = vec![5u8]; // length = 5
        data.extend_from_slice(b"hello");
        // 3-byte payload
        data.push(3); // length = 3
        data.extend_from_slice(b"bye");

        let count = framing.feed(&data).unwrap();
        assert_eq!(count, 2);
        assert_eq!(framing.next_frame(), Some(b"hello".to_vec()));
        assert_eq!(framing.next_frame(), Some(b"bye".to_vec()));
    }

    #[test]
    fn test_length_prefix_2byte_be() {
        let config = LengthPrefixConfig::new(2, 65535);
        let mut framing = LengthPrefixFraming::new(config);

        let mut data = Vec::new();
        data.extend_from_slice(&5u16.to_be_bytes()); // length = 5
        data.extend_from_slice(b"world");

        framing.feed(&data).unwrap();
        assert_eq!(framing.next_frame(), Some(b"world".to_vec()));
    }

    #[test]
    fn test_length_prefix_4byte_le() {
        let config = LengthPrefixConfig::new(4, 1024)
            .with_byte_order(ByteOrder::LittleEndian);
        let mut framing = LengthPrefixFraming::new(config);

        let mut data = Vec::new();
        data.extend_from_slice(&4u32.to_le_bytes()); // length = 4
        data.extend_from_slice(b"test");

        framing.feed(&data).unwrap();
        assert_eq!(framing.next_frame(), Some(b"test".to_vec()));
    }

    #[test]
    fn test_length_prefix_includes_header() {
        let config = LengthPrefixConfig::new(2, 1024)
            .with_length_includes_header(true);
        let mut framing = LengthPrefixFraming::new(config);

        let mut data = Vec::new();
        data.extend_from_slice(&7u16.to_be_bytes()); // 7 = 2 (header) + 5 (payload)
        data.extend_from_slice(b"hello");

        framing.feed(&data).unwrap();
        assert_eq!(framing.next_frame(), Some(b"hello".to_vec()));
    }

    #[test]
    fn test_length_prefix_chunked() {
        let config = LengthPrefixConfig::new(2, 1024);
        let mut framing = LengthPrefixFraming::new(config);

        // Feed header only
        framing.feed(&5u16.to_be_bytes()).unwrap();
        assert_eq!(framing.pending_frames(), 0);

        // Feed partial payload
        framing.feed(b"hel").unwrap();
        assert_eq!(framing.pending_frames(), 0);

        // Complete payload
        framing.feed(b"lo").unwrap();
        assert_eq!(framing.pending_frames(), 1);
        assert_eq!(framing.next_frame(), Some(b"hello".to_vec()));
    }

    #[test]
    fn test_length_prefix_frame_too_large() {
        let config = LengthPrefixConfig::new(2, 10);
        let mut framing = LengthPrefixFraming::new(config);

        let mut data = Vec::new();
        data.extend_from_slice(&100u16.to_be_bytes());
        data.extend_from_slice(&[0u8; 100]);

        let err = framing.feed(&data).unwrap_err();
        assert!(matches!(err, FramingError::FrameTooLarge { actual: 100, max: 10 }));
    }

    #[test]
    fn test_length_prefix_multiple_frames() {
        let config = LengthPrefixConfig::new(1, 255);
        let mut framing = LengthPrefixFraming::new(config);

        let mut data = Vec::new();
        for msg in &[b"one".as_ref(), b"two", b"three"] {
            data.push(msg.len() as u8);
            data.extend_from_slice(msg);
        }

        let count = framing.feed(&data).unwrap();
        assert_eq!(count, 3);
        assert_eq!(framing.drain_frames(), vec![
            b"one".to_vec(),
            b"two".to_vec(),
            b"three".to_vec(),
        ]);
    }

    #[test]
    fn test_length_prefix_empty_frame() {
        let config = LengthPrefixConfig::new(1, 255);
        let mut framing = LengthPrefixFraming::new(config);

        framing.feed(&[0u8]).unwrap(); // length = 0
        assert_eq!(framing.next_frame(), Some(vec![]));
    }

    // ========== Encode Tests ==========

    #[test]
    fn test_encode_length_prefix_1byte() {
        let encoded = encode_with_length_prefix(b"hello", 1, ByteOrder::BigEndian, false);
        assert_eq!(encoded, vec![5, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn test_encode_length_prefix_2byte_be() {
        let encoded = encode_with_length_prefix(b"hi", 2, ByteOrder::BigEndian, false);
        assert_eq!(encoded, vec![0, 2, b'h', b'i']);
    }

    #[test]
    fn test_encode_length_prefix_4byte_le() {
        let encoded = encode_with_length_prefix(b"ab", 4, ByteOrder::LittleEndian, false);
        assert_eq!(encoded, vec![2, 0, 0, 0, b'a', b'b']);
    }

    #[test]
    fn test_encode_length_includes_header() {
        let encoded = encode_with_length_prefix(b"hi", 2, ByteOrder::BigEndian, true);
        // length = 2 (payload) + 2 (header) = 4
        assert_eq!(encoded, vec![0, 4, b'h', b'i']);
    }

    // ========== Roundtrip Tests ==========

    #[test]
    fn test_delimiter_roundtrip() {
        let messages = vec!["hello", "world", "test"];
        let delimiter = b"\n";

        let mut encoded = Vec::new();
        for msg in &messages {
            encoded.extend_from_slice(&encode_with_delimiter(msg.as_bytes(), delimiter));
        }

        let config = DelimiterFramingConfig::newline(1024);
        let mut framing = DelimiterFraming::new(config);
        framing.feed(&encoded).unwrap();

        let frames = framing.drain_frames();
        let decoded: Vec<String> = frames.iter()
            .map(|f| String::from_utf8(f.clone()).unwrap())
            .collect();
        assert_eq!(decoded, messages);
    }

    #[test]
    fn test_length_prefix_roundtrip() {
        let messages = vec![b"hello".to_vec(), b"world".to_vec(), b"!".to_vec()];

        let mut encoded = Vec::new();
        for msg in &messages {
            encoded.extend_from_slice(&encode_with_length_prefix(msg, 2, ByteOrder::BigEndian, false));
        }

        let config = LengthPrefixConfig::new(2, 1024);
        let mut framing = LengthPrefixFraming::new(config);
        framing.feed(&encoded).unwrap();

        let decoded = framing.drain_frames();
        assert_eq!(decoded, messages);
    }
}

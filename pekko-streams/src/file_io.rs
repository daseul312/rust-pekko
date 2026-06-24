// file_io.rs - File-based Source and Sink for Stream Processing
// FileSource (line-by-line, chunked, full), FileSink (write, append)

use std::fs;
use std::io::{self, BufRead, Write as IoWrite, BufWriter, Read as IoRead};
use std::path::{Path, PathBuf};

// ============================================================================
// FileSource
// ============================================================================

/// Read mode for file sources
#[derive(Debug, Clone)]
pub enum FileReadMode {
    /// Read entire file as a single element
    WholeFile,
    /// Read file line by line
    Lines,
    /// Read file in fixed-size byte chunks
    Chunked { chunk_size: usize },
}

/// A source that reads from a file
pub struct FileSource {
    path: PathBuf,
    mode: FileReadMode,
}

impl FileSource {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            mode: FileReadMode::Lines,
        }
    }

    /// Read the entire file as a single String
    pub fn whole_file<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            mode: FileReadMode::WholeFile,
        }
    }

    /// Read the file line by line
    pub fn lines<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            mode: FileReadMode::Lines,
        }
    }

    /// Read the file in fixed-size byte chunks
    pub fn chunked<P: AsRef<Path>>(path: P, chunk_size: usize) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            mode: FileReadMode::Chunked { chunk_size },
        }
    }

    /// Read and return elements based on the mode
    pub fn read(&self) -> io::Result<Vec<String>> {
        match &self.mode {
            FileReadMode::WholeFile => {
                let content = fs::read_to_string(&self.path)?;
                Ok(vec![content])
            }
            FileReadMode::Lines => {
                let file = fs::File::open(&self.path)?;
                let reader = io::BufReader::new(file);
                reader.lines().collect()
            }
            FileReadMode::Chunked { chunk_size } => {
                let mut file = fs::File::open(&self.path)?;
                let mut chunks = Vec::new();
                loop {
                    let mut buf = vec![0u8; *chunk_size];
                    let n = file.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    buf.truncate(n);
                    chunks.push(String::from_utf8_lossy(&buf).to_string());
                }
                Ok(chunks)
            }
        }
    }

    /// Read raw bytes
    pub fn read_bytes(&self) -> io::Result<Vec<Vec<u8>>> {
        match &self.mode {
            FileReadMode::WholeFile => {
                let content = fs::read(&self.path)?;
                Ok(vec![content])
            }
            FileReadMode::Lines => {
                let file = fs::File::open(&self.path)?;
                let reader = io::BufReader::new(file);
                let lines: io::Result<Vec<String>> = reader.lines().collect();
                Ok(lines?.into_iter().map(|l| l.into_bytes()).collect())
            }
            FileReadMode::Chunked { chunk_size } => {
                let mut file = fs::File::open(&self.path)?;
                let mut chunks = Vec::new();
                loop {
                    let mut buf = vec![0u8; *chunk_size];
                    let n = file.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    buf.truncate(n);
                    chunks.push(buf);
                }
                Ok(chunks)
            }
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn mode(&self) -> &FileReadMode {
        &self.mode
    }
}

// ============================================================================
// FileSink
// ============================================================================

/// Write mode for file sinks
#[derive(Debug, Clone, PartialEq)]
pub enum FileWriteMode {
    /// Overwrite the file
    Overwrite,
    /// Append to the file
    Append,
    /// Create only if not exists
    CreateNew,
}

/// A sink that writes to a file
pub struct FileSink {
    path: PathBuf,
    mode: FileWriteMode,
    line_separator: String,
}

impl FileSink {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            mode: FileWriteMode::Overwrite,
            line_separator: "\n".to_string(),
        }
    }

    /// Create a sink that appends to the file
    pub fn append<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            mode: FileWriteMode::Append,
            line_separator: "\n".to_string(),
        }
    }

    /// Set the line separator
    pub fn with_separator(mut self, sep: &str) -> Self {
        self.line_separator = sep.to_string();
        self
    }

    /// Set write mode
    pub fn with_mode(mut self, mode: FileWriteMode) -> Self {
        self.mode = mode;
        self
    }

    /// Write string elements to the file
    pub fn write<S: AsRef<str>>(&self, elements: &[S]) -> io::Result<FileWriteResult> {
        let file = match self.mode {
            FileWriteMode::Overwrite => {
                fs::File::create(&self.path)?
            }
            FileWriteMode::Append => {
                fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.path)?
            }
            FileWriteMode::CreateNew => {
                fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&self.path)?
            }
        };

        let mut writer = BufWriter::new(file);
        let mut bytes_written = 0u64;
        let mut lines_written = 0u64;

        for (i, elem) in elements.iter().enumerate() {
            let s = elem.as_ref();
            writer.write_all(s.as_bytes())?;
            bytes_written += s.len() as u64;

            if i < elements.len() - 1 {
                writer.write_all(self.line_separator.as_bytes())?;
                bytes_written += self.line_separator.len() as u64;
            }
            lines_written += 1;
        }

        writer.flush()?;

        Ok(FileWriteResult {
            path: self.path.clone(),
            bytes_written,
            lines_written,
        })
    }

    /// Write raw bytes to the file
    pub fn write_bytes(&self, data: &[u8]) -> io::Result<FileWriteResult> {
        let mut file = match self.mode {
            FileWriteMode::Overwrite => fs::File::create(&self.path)?,
            FileWriteMode::Append => {
                fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.path)?
            }
            FileWriteMode::CreateNew => {
                fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&self.path)?
            }
        };

        file.write_all(data)?;
        file.flush()?;

        Ok(FileWriteResult {
            path: self.path.clone(),
            bytes_written: data.len() as u64,
            lines_written: 0,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn mode(&self) -> &FileWriteMode {
        &self.mode
    }
}

// ============================================================================
// File Write Result
// ============================================================================

#[derive(Debug, Clone)]
pub struct FileWriteResult {
    pub path: PathBuf,
    pub bytes_written: u64,
    pub lines_written: u64,
}

// ============================================================================
// Convenience Functions
// ============================================================================

/// Read a file line by line
pub fn from_file<P: AsRef<Path>>(path: P) -> io::Result<Vec<String>> {
    FileSource::lines(path).read()
}

/// Write lines to a file
pub fn to_file<P: AsRef<Path>, S: AsRef<str>>(path: P, lines: &[S]) -> io::Result<FileWriteResult> {
    FileSink::new(path).write(lines)
}

/// Append lines to a file
pub fn append_to_file<P: AsRef<Path>, S: AsRef<str>>(
    path: P,
    lines: &[S],
) -> io::Result<FileWriteResult> {
    FileSink::append(path).write(lines)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("pekko-streams-test");
        fs::create_dir_all(&dir).ok();
        dir.join(name)
    }

    fn cleanup(path: &Path) {
        fs::remove_file(path).ok();
    }

    // --- FileSource tests ---

    #[test]
    fn test_file_source_lines() {
        let path = temp_path("test_lines.txt");
        fs::write(&path, "line1\nline2\nline3").unwrap();

        let source = FileSource::lines(&path);
        let lines = source.read().unwrap();
        assert_eq!(lines, vec!["line1", "line2", "line3"]);

        cleanup(&path);
    }

    #[test]
    fn test_file_source_whole_file() {
        let path = temp_path("test_whole.txt");
        fs::write(&path, "hello world").unwrap();

        let source = FileSource::whole_file(&path);
        let content = source.read().unwrap();
        assert_eq!(content, vec!["hello world"]);

        cleanup(&path);
    }

    #[test]
    fn test_file_source_chunked() {
        let path = temp_path("test_chunked.txt");
        fs::write(&path, "abcdefghij").unwrap();

        let source = FileSource::chunked(&path, 4);
        let chunks = source.read().unwrap();
        assert_eq!(chunks.len(), 3); // "abcd", "efgh", "ij"
        assert_eq!(chunks[0], "abcd");
        assert_eq!(chunks[1], "efgh");
        assert_eq!(chunks[2], "ij");

        cleanup(&path);
    }

    #[test]
    fn test_file_source_read_bytes() {
        let path = temp_path("test_bytes.txt");
        fs::write(&path, "hello").unwrap();

        let source = FileSource::whole_file(&path);
        let bytes = source.read_bytes().unwrap();
        assert_eq!(bytes, vec![b"hello".to_vec()]);

        cleanup(&path);
    }

    #[test]
    fn test_file_source_nonexistent() {
        let source = FileSource::lines("/tmp/nonexistent_pekko_test_file.txt");
        assert!(source.read().is_err());
    }

    #[test]
    fn test_file_source_empty_file() {
        let path = temp_path("test_empty.txt");
        fs::write(&path, "").unwrap();

        let source = FileSource::lines(&path);
        let lines = source.read().unwrap();
        assert!(lines.is_empty());

        cleanup(&path);
    }

    #[test]
    fn test_file_source_path_and_mode() {
        let source = FileSource::lines("/tmp/test.txt");
        assert_eq!(source.path(), Path::new("/tmp/test.txt"));
        assert!(matches!(source.mode(), FileReadMode::Lines));
    }

    // --- FileSink tests ---

    #[test]
    fn test_file_sink_write() {
        let path = temp_path("test_write.txt");
        let sink = FileSink::new(&path);
        let result = sink.write(&["hello", "world"]).unwrap();

        assert_eq!(result.lines_written, 2);
        assert!(result.bytes_written > 0);

        let content = fs::read_to_string(&path).unwrap();
        assert_eq!(content, "hello\nworld");

        cleanup(&path);
    }

    #[test]
    fn test_file_sink_append() {
        let path = temp_path("test_append.txt");
        fs::write(&path, "first\n").unwrap();

        let sink = FileSink::append(&path);
        sink.write(&["second", "third"]).unwrap();

        let content = fs::read_to_string(&path).unwrap();
        assert!(content.contains("first"));
        assert!(content.contains("second"));

        cleanup(&path);
    }

    #[test]
    fn test_file_sink_overwrite() {
        let path = temp_path("test_overwrite.txt");
        fs::write(&path, "old content").unwrap();

        let sink = FileSink::new(&path);
        sink.write(&["new content"]).unwrap();

        let content = fs::read_to_string(&path).unwrap();
        assert_eq!(content, "new content");

        cleanup(&path);
    }

    #[test]
    fn test_file_sink_custom_separator() {
        let path = temp_path("test_sep.txt");
        let sink = FileSink::new(&path).with_separator(", ");
        sink.write(&["a", "b", "c"]).unwrap();

        let content = fs::read_to_string(&path).unwrap();
        assert_eq!(content, "a, b, c");

        cleanup(&path);
    }

    #[test]
    fn test_file_sink_write_bytes() {
        let path = temp_path("test_bytes_write.bin");
        let sink = FileSink::new(&path);
        let result = sink.write_bytes(&[0xFF, 0x00, 0xAB]).unwrap();

        assert_eq!(result.bytes_written, 3);
        let content = fs::read(&path).unwrap();
        assert_eq!(content, vec![0xFF, 0x00, 0xAB]);

        cleanup(&path);
    }

    #[test]
    fn test_file_sink_path_and_mode() {
        let sink = FileSink::new("/tmp/test.txt");
        assert_eq!(sink.path(), Path::new("/tmp/test.txt"));
        assert_eq!(*sink.mode(), FileWriteMode::Overwrite);

        let sink = FileSink::append("/tmp/test.txt");
        assert_eq!(*sink.mode(), FileWriteMode::Append);
    }

    // --- Convenience function tests ---

    #[test]
    fn test_from_file() {
        let path = temp_path("test_from.txt");
        fs::write(&path, "a\nb\nc").unwrap();

        let lines = from_file(&path).unwrap();
        assert_eq!(lines, vec!["a", "b", "c"]);

        cleanup(&path);
    }

    #[test]
    fn test_to_file() {
        let path = temp_path("test_to.txt");
        to_file(&path, &["x", "y"]).unwrap();

        let content = fs::read_to_string(&path).unwrap();
        assert_eq!(content, "x\ny");

        cleanup(&path);
    }

    #[test]
    fn test_append_to_file() {
        let path = temp_path("test_append_fn.txt");
        to_file(&path, &["first"]).unwrap();
        append_to_file(&path, &["\nsecond"]).unwrap();

        let content = fs::read_to_string(&path).unwrap();
        assert!(content.contains("first"));
        assert!(content.contains("second"));

        cleanup(&path);
    }

    // --- Round-trip test ---

    #[test]
    fn test_round_trip_lines() {
        let path = temp_path("test_roundtrip.txt");
        let original = vec!["alpha", "beta", "gamma"];

        to_file(&path, &original).unwrap();
        let read_back = from_file(&path).unwrap();

        assert_eq!(read_back, original);

        cleanup(&path);
    }

    #[test]
    fn test_round_trip_bytes() {
        let path = temp_path("test_roundtrip_bytes.bin");
        let data = vec![1u8, 2, 3, 4, 5, 6, 7, 8];

        FileSink::new(&path).write_bytes(&data).unwrap();
        let read_back = FileSource::whole_file(&path).read_bytes().unwrap();

        assert_eq!(read_back[0], data);

        cleanup(&path);
    }
}

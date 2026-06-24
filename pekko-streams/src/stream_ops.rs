// stream_ops.rs - Additional Stream Operators
// takeWhile, dropWhile, reduce, fold, alsoTo, wireTap, log, mapConcat, limit

use std::fmt;
use std::sync::{Arc, Mutex};

// ============================================================================
// TakeWhile — emit elements while predicate is true, then complete
// ============================================================================

pub struct TakeWhile<T, F>
where
    F: Fn(&T) -> bool,
{
    predicate: F,
    inclusive: bool, // if true, include the first failing element
    _marker: std::marker::PhantomData<T>,
}

impl<T, F> TakeWhile<T, F>
where
    F: Fn(&T) -> bool,
{
    pub fn new(predicate: F) -> Self {
        Self {
            predicate,
            inclusive: false,
            _marker: std::marker::PhantomData,
        }
    }

    /// Include the element that first fails the predicate
    pub fn inclusive(mut self) -> Self {
        self.inclusive = true;
        self
    }

    pub fn apply(&self, elements: Vec<T>) -> Vec<T> {
        let mut result = Vec::new();
        for elem in elements {
            if (self.predicate)(&elem) {
                result.push(elem);
            } else {
                if self.inclusive {
                    result.push(elem);
                }
                break;
            }
        }
        result
    }
}

/// Convenience function
pub fn take_while<T, F>(elements: Vec<T>, predicate: F) -> Vec<T>
where
    F: Fn(&T) -> bool,
{
    TakeWhile::new(predicate).apply(elements)
}

/// Inclusive version
pub fn take_while_inclusive<T, F>(elements: Vec<T>, predicate: F) -> Vec<T>
where
    F: Fn(&T) -> bool,
{
    TakeWhile::new(predicate).inclusive().apply(elements)
}

// ============================================================================
// DropWhile — skip elements while predicate is true, then emit all remaining
// ============================================================================

pub struct DropWhile<T, F>
where
    F: Fn(&T) -> bool,
{
    predicate: F,
    _marker: std::marker::PhantomData<T>,
}

impl<T, F> DropWhile<T, F>
where
    F: Fn(&T) -> bool,
{
    pub fn new(predicate: F) -> Self {
        Self {
            predicate,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn apply(&self, elements: Vec<T>) -> Vec<T> {
        let mut dropping = true;
        let mut result = Vec::new();
        for elem in elements {
            if dropping && (self.predicate)(&elem) {
                continue;
            }
            dropping = false;
            result.push(elem);
        }
        result
    }
}

pub fn drop_while<T, F>(elements: Vec<T>, predicate: F) -> Vec<T>
where
    F: Fn(&T) -> bool,
{
    DropWhile::new(predicate).apply(elements)
}

// ============================================================================
// Reduce — fold all elements without initial value
// ============================================================================

pub fn reduce<T, F>(elements: Vec<T>, f: F) -> Option<T>
where
    F: Fn(T, T) -> T,
{
    let mut iter = elements.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, f))
}

// ============================================================================
// FoldFlow — fold as a flow (emits intermediate results like scan)
// ============================================================================

pub fn fold_flow<T, A, F>(elements: Vec<T>, init: A, f: F) -> A
where
    F: Fn(A, T) -> A,
{
    elements.into_iter().fold(init, f)
}

// ============================================================================
// MapConcat — map each element to a collection and flatten
// ============================================================================

pub fn map_concat<T, U, I, F>(elements: Vec<T>, f: F) -> Vec<U>
where
    I: IntoIterator<Item = U>,
    F: Fn(T) -> I,
{
    elements.into_iter().flat_map(f).collect()
}

// ============================================================================
// AlsoTo — tee: send elements to a side sink while continuing downstream
// ============================================================================

pub struct AlsoTo<T, F>
where
    F: FnMut(&T),
{
    side_effect: F,
    _marker: std::marker::PhantomData<T>,
}

impl<T, F> AlsoTo<T, F>
where
    F: FnMut(&T),
{
    pub fn new(side_effect: F) -> Self {
        Self {
            side_effect,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn apply(&mut self, elements: Vec<T>) -> Vec<T> {
        for elem in &elements {
            (self.side_effect)(elem);
        }
        elements
    }
}

pub fn also_to<T, F>(elements: Vec<T>, mut side_effect: F) -> Vec<T>
where
    F: FnMut(&T),
{
    for elem in &elements {
        side_effect(elem);
    }
    elements
}

// ============================================================================
// WireTap — observe elements without modifying (logging, metrics, debugging)
// ============================================================================

pub struct WireTap<T> {
    tapped: Arc<Mutex<Vec<T>>>,
}

impl<T: Clone> WireTap<T> {
    pub fn new() -> Self {
        Self {
            tapped: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Process elements, recording each one
    pub fn apply(&self, elements: Vec<T>) -> Vec<T> {
        let mut tapped = self.tapped.lock().unwrap();
        for elem in &elements {
            tapped.push(elem.clone());
        }
        elements
    }

    /// Get all tapped elements
    pub fn tapped(&self) -> Vec<T> {
        self.tapped.lock().unwrap().clone()
    }

    /// Number of elements tapped
    pub fn count(&self) -> usize {
        self.tapped.lock().unwrap().len()
    }

    /// Clear tapped elements
    pub fn clear(&self) {
        self.tapped.lock().unwrap().clear();
    }

    /// Get a shared handle for assertion
    pub fn handle(&self) -> WireTapHandle<T> {
        WireTapHandle {
            tapped: Arc::clone(&self.tapped),
        }
    }
}

impl<T: Clone> Default for WireTap<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared handle for WireTap, useful for assertions in tests
#[derive(Clone)]
pub struct WireTapHandle<T> {
    tapped: Arc<Mutex<Vec<T>>>,
}

impl<T: Clone> WireTapHandle<T> {
    pub fn tapped(&self) -> Vec<T> {
        self.tapped.lock().unwrap().clone()
    }

    pub fn count(&self) -> usize {
        self.tapped.lock().unwrap().len()
    }
}

// ============================================================================
// StreamLog — structured logging for stream elements
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogLevel::Trace => write!(f, "TRACE"),
            LogLevel::Debug => write!(f, "DEBUG"),
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Warn => write!(f, "WARN"),
            LogLevel::Error => write!(f, "ERROR"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub level: LogLevel,
    pub prefix: String,
    pub message: String,
}

pub struct StreamLog {
    prefix: String,
    level: LogLevel,
    entries: Arc<Mutex<Vec<LogEntry>>>,
}

impl StreamLog {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            level: LogLevel::Debug,
            entries: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn with_level(mut self, level: LogLevel) -> Self {
        self.level = level;
        self
    }

    /// Log elements passing through
    pub fn apply<T: fmt::Debug>(&self, elements: Vec<T>) -> Vec<T> {
        for elem in &elements {
            let entry = LogEntry {
                level: self.level.clone(),
                prefix: self.prefix.clone(),
                message: format!("{:?}", elem),
            };
            self.entries.lock().unwrap().push(entry);
        }
        elements
    }

    /// Get all log entries
    pub fn entries(&self) -> Vec<LogEntry> {
        self.entries.lock().unwrap().clone()
    }

    /// Number of entries logged
    pub fn entry_count(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    /// Clear log entries
    pub fn clear(&self) {
        self.entries.lock().unwrap().clear();
    }
}

/// Convenience: log elements with a prefix
pub fn log_elements<T: fmt::Debug + Clone>(elements: Vec<T>, prefix: &str) -> Vec<T> {
    let logger = StreamLog::new(prefix);
    logger.apply(elements)
}

// ============================================================================
// Limit — pass only up to N elements, then error
// ============================================================================

pub enum LimitResult<T> {
    Ok(Vec<T>),
    Exceeded { passed: Vec<T>, excess_count: usize },
}

pub fn limit<T>(elements: Vec<T>, max: usize) -> LimitResult<T> {
    if elements.len() <= max {
        LimitResult::Ok(elements)
    } else {
        let excess = elements.len() - max;
        let passed: Vec<T> = elements.into_iter().take(max).collect();
        LimitResult::Exceeded { passed, excess_count: excess }
    }
}

// ============================================================================
// ZipWithIndex — pair each element with its 0-based index
// ============================================================================

pub fn zip_with_index<T>(elements: Vec<T>) -> Vec<(usize, T)> {
    elements.into_iter().enumerate().collect()
}

// ============================================================================
// DistinctUntilChanged — emit only when value changes from previous
// ============================================================================

pub fn distinct_until_changed<T: PartialEq + Clone>(elements: Vec<T>) -> Vec<T> {
    if elements.is_empty() {
        return Vec::new();
    }

    let mut result = vec![elements[0].clone()];
    for elem in elements.iter().skip(1) {
        if elem != result.last().unwrap() {
            result.push(elem.clone());
        }
    }
    result
}

pub fn distinct_until_changed_by<T: Clone, K: PartialEq, F>(
    elements: Vec<T>,
    key_fn: F,
) -> Vec<T>
where
    F: Fn(&T) -> K,
{
    if elements.is_empty() {
        return Vec::new();
    }

    let mut result = vec![elements[0].clone()];
    let mut last_key = key_fn(&elements[0]);

    for elem in elements.iter().skip(1) {
        let key = key_fn(elem);
        if key != last_key {
            result.push(elem.clone());
            last_key = key;
        }
    }
    result
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // --- TakeWhile tests ---

    #[test]
    fn test_take_while_basic() {
        let data = vec![1, 2, 3, 4, 5, 1, 2];
        let result = take_while(data, |x| *x < 4);
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_take_while_all_pass() {
        let data = vec![1, 2, 3];
        let result = take_while(data, |x| *x < 10);
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_take_while_none_pass() {
        let data = vec![5, 6, 7];
        let result = take_while(data, |x| *x < 5);
        assert!(result.is_empty());
    }

    #[test]
    fn test_take_while_inclusive() {
        let data = vec![1, 2, 3, 4, 5];
        let result = take_while_inclusive(data, |x| *x < 4);
        assert_eq!(result, vec![1, 2, 3, 4]); // includes the failing element
    }

    #[test]
    fn test_take_while_empty() {
        let data: Vec<i32> = vec![];
        let result = take_while(data, |x| *x < 5);
        assert!(result.is_empty());
    }

    // --- DropWhile tests ---

    #[test]
    fn test_drop_while_basic() {
        let data = vec![1, 2, 3, 4, 5, 1, 2];
        let result = drop_while(data, |x| *x < 4);
        assert_eq!(result, vec![4, 5, 1, 2]); // doesn't re-drop after stopping
    }

    #[test]
    fn test_drop_while_all_dropped() {
        let data = vec![1, 2, 3];
        let result = drop_while(data, |x| *x < 10);
        assert!(result.is_empty());
    }

    #[test]
    fn test_drop_while_none_dropped() {
        let data = vec![5, 6, 1, 2];
        let result = drop_while(data, |x| *x < 5);
        assert_eq!(result, vec![5, 6, 1, 2]);
    }

    #[test]
    fn test_drop_while_empty() {
        let data: Vec<i32> = vec![];
        let result = drop_while(data, |_| true);
        assert!(result.is_empty());
    }

    // --- Reduce tests ---

    #[test]
    fn test_reduce_sum() {
        let data = vec![1, 2, 3, 4, 5];
        let result = reduce(data, |a, b| a + b);
        assert_eq!(result, Some(15));
    }

    #[test]
    fn test_reduce_max() {
        let data = vec![3, 1, 4, 1, 5, 9, 2];
        let result = reduce(data, |a, b| if a > b { a } else { b });
        assert_eq!(result, Some(9));
    }

    #[test]
    fn test_reduce_empty() {
        let data: Vec<i32> = vec![];
        let result = reduce(data, |a, b| a + b);
        assert_eq!(result, None);
    }

    #[test]
    fn test_reduce_single() {
        let data = vec![42];
        let result = reduce(data, |a, b| a + b);
        assert_eq!(result, Some(42));
    }

    // --- Fold tests ---

    #[test]
    fn test_fold_flow() {
        let data = vec![1, 2, 3, 4, 5];
        let result = fold_flow(data, 0, |acc, x| acc + x);
        assert_eq!(result, 15);
    }

    #[test]
    fn test_fold_flow_string() {
        let data = vec!["a", "b", "c"];
        let result = fold_flow(data, String::new(), |mut acc, x| {
            acc.push_str(x);
            acc
        });
        assert_eq!(result, "abc");
    }

    // --- MapConcat tests ---

    #[test]
    fn test_map_concat() {
        let data = vec![1, 2, 3];
        let result = map_concat(data, |x| vec![x, x * 10]);
        assert_eq!(result, vec![1, 10, 2, 20, 3, 30]);
    }

    #[test]
    fn test_map_concat_empty_output() {
        let data = vec![1, 2, 3];
        let result: Vec<i32> = map_concat(data, |_x| vec![]);
        assert!(result.is_empty());
    }

    // --- AlsoTo tests ---

    #[test]
    fn test_also_to_collects_side() {
        let data = vec![1, 2, 3];
        let mut side = Vec::new();
        let result = also_to(data, |x| side.push(*x));
        assert_eq!(result, vec![1, 2, 3]); // unchanged
        assert_eq!(side, vec![1, 2, 3]);    // side collected
    }

    #[test]
    fn test_also_to_empty() {
        let data: Vec<i32> = vec![];
        let mut count = 0;
        let result = also_to(data, |_| count += 1);
        assert!(result.is_empty());
        assert_eq!(count, 0);
    }

    // --- WireTap tests ---

    #[test]
    fn test_wire_tap_records() {
        let tap = WireTap::new();
        let data = vec![1, 2, 3];
        let result = tap.apply(data);
        assert_eq!(result, vec![1, 2, 3]);
        assert_eq!(tap.tapped(), vec![1, 2, 3]);
        assert_eq!(tap.count(), 3);
    }

    #[test]
    fn test_wire_tap_handle() {
        let tap = WireTap::new();
        let handle = tap.handle();
        tap.apply(vec![10, 20]);
        assert_eq!(handle.tapped(), vec![10, 20]);
        assert_eq!(handle.count(), 2);
    }

    #[test]
    fn test_wire_tap_clear() {
        let tap = WireTap::new();
        tap.apply(vec![1, 2, 3]);
        assert_eq!(tap.count(), 3);
        tap.clear();
        assert_eq!(tap.count(), 0);
    }

    // --- StreamLog tests ---

    #[test]
    fn test_stream_log_records() {
        let log = StreamLog::new("test-stream");
        let data = vec![1, 2, 3];
        let result = log.apply(data);
        assert_eq!(result, vec![1, 2, 3]);
        assert_eq!(log.entry_count(), 3);

        let entries = log.entries();
        assert_eq!(entries[0].prefix, "test-stream");
        assert_eq!(entries[0].level, LogLevel::Debug);
    }

    #[test]
    fn test_stream_log_with_level() {
        let log = StreamLog::new("errors").with_level(LogLevel::Error);
        log.apply(vec!["err1", "err2"]);
        let entries = log.entries();
        assert_eq!(entries[0].level, LogLevel::Error);
    }

    #[test]
    fn test_stream_log_clear() {
        let log = StreamLog::new("test");
        log.apply(vec![1, 2]);
        log.clear();
        assert_eq!(log.entry_count(), 0);
    }

    #[test]
    fn test_log_level_display() {
        assert_eq!(LogLevel::Trace.to_string(), "TRACE");
        assert_eq!(LogLevel::Debug.to_string(), "DEBUG");
        assert_eq!(LogLevel::Info.to_string(), "INFO");
        assert_eq!(LogLevel::Warn.to_string(), "WARN");
        assert_eq!(LogLevel::Error.to_string(), "ERROR");
    }

    // --- Limit tests ---

    #[test]
    fn test_limit_within() {
        let data = vec![1, 2, 3];
        match limit(data, 5) {
            LimitResult::Ok(v) => assert_eq!(v, vec![1, 2, 3]),
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_limit_exceeded() {
        let data = vec![1, 2, 3, 4, 5];
        match limit(data, 3) {
            LimitResult::Exceeded { passed, excess_count } => {
                assert_eq!(passed, vec![1, 2, 3]);
                assert_eq!(excess_count, 2);
            }
            _ => panic!("Expected Exceeded"),
        }
    }

    #[test]
    fn test_limit_exact() {
        let data = vec![1, 2, 3];
        match limit(data, 3) {
            LimitResult::Ok(v) => assert_eq!(v, vec![1, 2, 3]),
            _ => panic!("Expected Ok"),
        }
    }

    // --- ZipWithIndex tests ---

    #[test]
    fn test_zip_with_index() {
        let data = vec!["a", "b", "c"];
        let result = zip_with_index(data);
        assert_eq!(result, vec![(0, "a"), (1, "b"), (2, "c")]);
    }

    #[test]
    fn test_zip_with_index_empty() {
        let data: Vec<i32> = vec![];
        let result = zip_with_index(data);
        assert!(result.is_empty());
    }

    // --- DistinctUntilChanged tests ---

    #[test]
    fn test_distinct_until_changed() {
        let data = vec![1, 1, 2, 2, 2, 3, 1, 1];
        let result = distinct_until_changed(data);
        assert_eq!(result, vec![1, 2, 3, 1]);
    }

    #[test]
    fn test_distinct_until_changed_all_same() {
        let data = vec![5, 5, 5, 5];
        let result = distinct_until_changed(data);
        assert_eq!(result, vec![5]);
    }

    #[test]
    fn test_distinct_until_changed_all_different() {
        let data = vec![1, 2, 3, 4];
        let result = distinct_until_changed(data);
        assert_eq!(result, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_distinct_until_changed_empty() {
        let data: Vec<i32> = vec![];
        let result = distinct_until_changed(data);
        assert!(result.is_empty());
    }

    #[test]
    fn test_distinct_until_changed_by() {
        #[derive(Clone, Debug, PartialEq)]
        struct Item { category: String, value: i32 }

        let data = vec![
            Item { category: "a".into(), value: 1 },
            Item { category: "a".into(), value: 2 },
            Item { category: "b".into(), value: 3 },
            Item { category: "b".into(), value: 4 },
            Item { category: "a".into(), value: 5 },
        ];

        let result = distinct_until_changed_by(data, |item| item.category.clone());
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].value, 1);
        assert_eq!(result[1].value, 3);
        assert_eq!(result[2].value, 5);
    }
}

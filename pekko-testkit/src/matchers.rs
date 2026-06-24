//! Message assertions and matching utilities
//!
//! Provides tools for verifying message content, ordering, and patterns
//! in actor test scenarios.

use std::fmt::Debug;

/// Assert that messages appear in the exact given order
pub fn expect_in_order<T: Debug + PartialEq>(actual: &[T], expected: &[T]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "expect_in_order: length mismatch (actual={}, expected={})",
        actual.len(),
        expected.len()
    );
    for (i, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
        assert_eq!(
            a, e,
            "expect_in_order: mismatch at index {}: actual={:?}, expected={:?}",
            i, a, e
        );
    }
}

/// Assert that all expected messages appear, regardless of order
pub fn expect_any_order<T: Debug + PartialEq + Clone>(actual: &[T], expected: &[T]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "expect_any_order: length mismatch (actual={}, expected={})",
        actual.len(),
        expected.len()
    );
    let mut remaining: Vec<T> = expected.to_vec();
    for a in actual {
        let pos = remaining.iter().position(|e| e == a);
        match pos {
            Some(i) => {
                remaining.remove(i);
            }
            None => panic!(
                "expect_any_order: unexpected message {:?} (remaining expected: {:?})",
                a, remaining
            ),
        }
    }
    assert!(
        remaining.is_empty(),
        "expect_any_order: missing messages: {:?}",
        remaining
    );
}

/// Assert that expected messages appear as a subsequence of actual
pub fn expect_subsequence<T: Debug + PartialEq>(actual: &[T], expected: &[T]) {
    let mut exp_idx = 0;
    for a in actual {
        if exp_idx < expected.len() && *a == expected[exp_idx] {
            exp_idx += 1;
        }
    }
    assert_eq!(
        exp_idx,
        expected.len(),
        "expect_subsequence: only matched {}/{} expected messages",
        exp_idx,
        expected.len()
    );
}

/// Assert that a collection contains a specific element
pub fn expect_contains<T: Debug + PartialEq>(actual: &[T], element: &T) {
    assert!(
        actual.contains(element),
        "expect_contains: {:?} not found in {:?}",
        element, actual
    );
}

/// Assert that a collection does not contain a specific element
pub fn expect_not_contains<T: Debug + PartialEq>(actual: &[T], element: &T) {
    assert!(
        !actual.contains(element),
        "expect_not_contains: {:?} was found in {:?}",
        element, actual
    );
}

/// Builder for message sequence assertions
pub struct MessageSequence<T> {
    matchers: Vec<Box<dyn Fn(&T) -> bool + Send>>,
    descriptions: Vec<String>,
}

impl<T: Debug + 'static> MessageSequence<T> {
    /// Create a new empty sequence
    pub fn new() -> Self {
        Self {
            matchers: Vec::new(),
            descriptions: Vec::new(),
        }
    }

    /// Expect the next message to match a predicate
    pub fn expect<F: Fn(&T) -> bool + Send + 'static>(mut self, desc: &str, matcher: F) -> Self {
        self.descriptions.push(desc.to_string());
        self.matchers.push(Box::new(matcher));
        self
    }

    /// Expect the next message to equal a value
    pub fn expect_eq(self, desc: &str, expected: T) -> Self
    where
        T: PartialEq + Send + 'static,
    {
        self.expect(desc, move |actual| *actual == expected)
    }

    /// Verify that the actual messages match the sequence
    pub fn verify(&self, actual: &[T]) {
        assert!(
            actual.len() >= self.matchers.len(),
            "MessageSequence: not enough messages (got {}, need {})",
            actual.len(),
            self.matchers.len()
        );

        for (i, (matcher, desc)) in self.matchers.iter().zip(self.descriptions.iter()).enumerate() {
            assert!(
                matcher(&actual[i]),
                "MessageSequence: step {} ('{}') failed for message {:?}",
                i, desc, actual[i]
            );
        }
    }

    /// Get the expected length
    pub fn len(&self) -> usize {
        self.matchers.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.matchers.is_empty()
    }
}

impl<T: Debug + 'static> Default for MessageSequence<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expect_in_order() {
        let actual = vec![1, 2, 3];
        let expected = vec![1, 2, 3];
        expect_in_order(&actual, &expected);
    }

    #[test]
    #[should_panic(expected = "mismatch at index")]
    fn test_expect_in_order_fails() {
        let actual = vec![1, 3, 2];
        let expected = vec![1, 2, 3];
        expect_in_order(&actual, &expected);
    }

    #[test]
    fn test_expect_any_order() {
        let actual = vec![3, 1, 2];
        let expected = vec![1, 2, 3];
        expect_any_order(&actual, &expected);
    }

    #[test]
    #[should_panic(expected = "unexpected message")]
    fn test_expect_any_order_fails() {
        let actual = vec![1, 2, 4];
        let expected = vec![1, 2, 3];
        expect_any_order(&actual, &expected);
    }

    #[test]
    fn test_expect_subsequence() {
        let actual = vec![1, 2, 3, 4, 5];
        let expected = vec![2, 4];
        expect_subsequence(&actual, &expected);
    }

    #[test]
    #[should_panic(expected = "only matched")]
    fn test_expect_subsequence_fails() {
        let actual = vec![1, 2, 3];
        let expected = vec![3, 1]; // not a subsequence
        expect_subsequence(&actual, &expected);
    }

    #[test]
    fn test_expect_contains() {
        let actual = vec![1, 2, 3];
        expect_contains(&actual, &2);
    }

    #[test]
    #[should_panic(expected = "not found")]
    fn test_expect_contains_fails() {
        let actual = vec![1, 2, 3];
        expect_contains(&actual, &5);
    }

    #[test]
    fn test_expect_not_contains() {
        let actual = vec![1, 2, 3];
        expect_not_contains(&actual, &5);
    }

    #[test]
    fn test_message_sequence() {
        let seq = MessageSequence::<i32>::new()
            .expect("first is positive", |x| *x > 0)
            .expect("second is even", |x| x % 2 == 0)
            .expect("third is 30", |x| *x == 30);

        let actual = vec![5, 10, 30];
        seq.verify(&actual);
    }

    #[test]
    #[should_panic(expected = "step 1")]
    fn test_message_sequence_fails() {
        let seq = MessageSequence::<i32>::new()
            .expect("first", |x| *x > 0)
            .expect("second should be even", |x| x % 2 == 0);

        let actual = vec![5, 7]; // 7 is not even
        seq.verify(&actual);
    }

    #[test]
    fn test_message_sequence_expect_eq() {
        let seq = MessageSequence::<String>::new()
            .expect_eq("hello", "hello".to_string())
            .expect_eq("world", "world".to_string());

        let actual = vec!["hello".to_string(), "world".to_string()];
        seq.verify(&actual);
    }
}

//! Decode OpenAlex abstract inverted index to plaintext
//!
//! OpenAlex stores abstracts as inverted indexes for legal reasons:
//! ```json
//! {"Despite": [0], "growing": [1], "interest": [2, 50], ...}
//! ```
//!
//! This module reconstructs the original text by position.

use serde_json::{Map, Value};

/// Decode inverted index to plaintext abstract
///
/// Input: JSON object mapping words to position arrays
/// Output: Space-separated words in original order
///
/// # Example
/// ```
/// use serde_json::json;
/// use papeline_openalex::abstract_decode::decode_inverted_index;
///
/// let index = json!({"Hello": [0], "world": [1]});
/// let text = decode_inverted_index(index.as_object().unwrap());
/// assert_eq!(text, "Hello world");
/// ```
pub fn decode_inverted_index(index: &Map<String, Value>) -> String {
    if index.is_empty() {
        return String::new();
    }

    // Collect (position, word) pairs
    let mut pairs: Vec<(usize, &str)> = Vec::new();

    for (word, positions) in index {
        if let Some(arr) = positions.as_array() {
            for pos in arr {
                if let Some(p) = pos.as_u64() {
                    pairs.push((p as usize, word.as_str()));
                }
            }
        }
    }

    // Sort by position
    pairs.sort_by_key(|(pos, _)| *pos);

    // Join words
    let words: Vec<&str> = pairs.into_iter().map(|(_, w)| w).collect();
    words.join(" ")
}

/// Decode inverted index from serde_json::Value
///
/// Returns None if the value is null or not an object
pub fn decode_abstract(value: Option<&Value>) -> Option<String> {
    let obj = value?.as_object()?;
    let text = decode_inverted_index(obj);
    if text.is_empty() { None } else { Some(text) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn empty_index() {
        let index = json!({});
        let text = decode_inverted_index(index.as_object().unwrap());
        assert_eq!(text, "");
    }

    #[test]
    fn single_word() {
        let index = json!({"Hello": [0]});
        let text = decode_inverted_index(index.as_object().unwrap());
        assert_eq!(text, "Hello");
    }

    #[test]
    fn multiple_words_ordered() {
        let index = json!({"Hello": [0], "world": [1], "!": [2]});
        let text = decode_inverted_index(index.as_object().unwrap());
        assert_eq!(text, "Hello world !");
    }

    #[test]
    fn repeated_word() {
        let index = json!({"the": [0, 2], "cat": [1], "sat": [3]});
        let text = decode_inverted_index(index.as_object().unwrap());
        assert_eq!(text, "the cat the sat");
    }

    #[test]
    fn out_of_order_positions() {
        // JSON object iteration order is not guaranteed
        let index = json!({"world": [1], "Hello": [0]});
        let text = decode_inverted_index(index.as_object().unwrap());
        assert_eq!(text, "Hello world");
    }

    #[test]
    fn realistic_abstract() {
        let index = json!({
            "We": [0],
            "present": [1],
            "a": [2, 10],
            "novel": [3],
            "approach": [4],
            "to": [5],
            "machine": [6],
            "learning": [7],
            "using": [8],
            "deep": [9],
            "neural": [11],
            "network": [12]
        });
        let text = decode_inverted_index(index.as_object().unwrap());
        assert_eq!(
            text,
            "We present a novel approach to machine learning using deep a neural network"
        );
    }

    #[test]
    fn decode_abstract_null() {
        assert_eq!(decode_abstract(None), None);
        assert_eq!(decode_abstract(Some(&Value::Null)), None);
    }

    #[test]
    fn decode_abstract_empty() {
        let val = json!({});
        assert_eq!(decode_abstract(Some(&val)), None);
    }

    #[test]
    fn decode_abstract_valid() {
        let val = json!({"Hello": [0], "world": [1]});
        assert_eq!(decode_abstract(Some(&val)), Some("Hello world".to_string()));
    }
}

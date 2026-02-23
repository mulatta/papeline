//! Blake3 hashing utilities for content-addressable storage

use std::io;
use std::path::Path;

/// Hash a file's contents with blake3.
pub fn hash_file(path: &Path) -> io::Result<blake3::Hash> {
    let mut hasher = blake3::Hasher::new();
    hasher.update_mmap(path)?;
    Ok(hasher.finalize())
}

/// Hash raw bytes with blake3.
pub fn hash_bytes(data: &[u8]) -> blake3::Hash {
    blake3::hash(data)
}

/// Combine multiple hashes into one by hashing their concatenated bytes.
pub fn combine_hashes(hashes: &[blake3::Hash]) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    for h in hashes {
        hasher.update(h.as_bytes());
    }
    hasher.finalize()
}

/// Return the first 8 hex characters of a blake3 hash.
pub fn short_hash(hash: &blake3::Hash) -> String {
    hash.to_hex()[..8].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_bytes_deterministic() {
        let h1 = hash_bytes(b"hello");
        let h2 = hash_bytes(b"hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_bytes_different_input() {
        let h1 = hash_bytes(b"hello");
        let h2 = hash_bytes(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn short_hash_length() {
        let h = hash_bytes(b"test");
        assert_eq!(short_hash(&h).len(), 8);
    }

    #[test]
    fn combine_hashes_deterministic() {
        let h1 = hash_bytes(b"a");
        let h2 = hash_bytes(b"b");
        let c1 = combine_hashes(&[h1, h2]);
        let c2 = combine_hashes(&[h1, h2]);
        assert_eq!(c1, c2);
    }

    #[test]
    fn combine_hashes_order_matters() {
        let h1 = hash_bytes(b"a");
        let h2 = hash_bytes(b"b");
        let c1 = combine_hashes(&[h1, h2]);
        let c2 = combine_hashes(&[h2, h1]);
        assert_ne!(c1, c2);
    }

    #[test]
    fn hash_bytes_empty() {
        let h = hash_bytes(b"");
        // Should not panic, produces a valid hash
        assert_eq!(short_hash(&h).len(), 8);
    }

    #[test]
    fn combine_hashes_empty_slice() {
        let c = combine_hashes(&[]);
        // Empty input should produce a valid (deterministic) hash
        let c2 = combine_hashes(&[]);
        assert_eq!(c, c2);
    }

    #[test]
    fn combine_hashes_single() {
        let h = hash_bytes(b"only");
        let c = combine_hashes(&[h]);
        // Single hash combined should differ from the original
        // (because combine re-hashes the hash bytes)
        assert_ne!(c, h);
    }

    #[test]
    fn short_hash_deterministic() {
        let h1 = hash_bytes(b"same");
        let h2 = hash_bytes(b"same");
        assert_eq!(short_hash(&h1), short_hash(&h2));
    }

    #[test]
    fn short_hash_hex_chars() {
        let h = hash_bytes(b"test");
        let s = short_hash(&h);
        assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn hash_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.txt");
        std::fs::write(&path, b"").unwrap();
        let h = hash_file(&path).unwrap();
        let expected = hash_bytes(b"");
        assert_eq!(h, expected);
    }

    #[test]
    fn hash_file_not_found() {
        let result = hash_file(std::path::Path::new("/nonexistent/file.txt"));
        assert!(result.is_err());
    }

    #[test]
    fn hash_file_works() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.txt");
        std::fs::write(&path, b"file content").unwrap();
        let h = hash_file(&path).unwrap();
        let expected = hash_bytes(b"file content");
        assert_eq!(h, expected);
    }
}

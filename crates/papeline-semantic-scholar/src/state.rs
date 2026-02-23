//! Pipeline state types for Semantic Scholar data processing

/// Known dataset types in the S2 bulk download pipeline
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dataset {
    Papers,
    Abstracts,
    Tldrs,
    Citations,
    Embeddings,
}

impl Dataset {
    /// Parse CLI/config string into enum
    pub fn from_name(s: &str) -> Option<Self> {
        match s {
            "papers" => Some(Self::Papers),
            "abstracts" => Some(Self::Abstracts),
            "tldrs" => Some(Self::Tldrs),
            "citations" => Some(Self::Citations),
            "embeddings" => Some(Self::Embeddings),
            _ => None,
        }
    }

    /// S2 API dataset name (used for URL fetching)
    pub fn api_name(self) -> &'static str {
        match self {
            Self::Papers => "papers",
            Self::Abstracts => "abstracts",
            Self::Tldrs => "tldrs",
            Self::Citations => "citations",
            Self::Embeddings => "embeddings-specter_v2",
        }
    }

    /// Filename prefix for output files
    pub fn file_prefix(self) -> &'static str {
        match self {
            Self::Papers => "papers",
            Self::Abstracts => "abstracts",
            Self::Tldrs => "tldrs",
            Self::Citations => "citations",
            Self::Embeddings => "embeddings",
        }
    }
}

impl std::fmt::Display for Dataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.file_prefix())
    }
}

#[derive(Clone, Debug)]
pub struct ShardInfo {
    pub dataset: Dataset,
    pub shard_idx: usize,
    pub url: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_name_valid() {
        assert_eq!(Dataset::from_name("papers"), Some(Dataset::Papers));
        assert_eq!(Dataset::from_name("abstracts"), Some(Dataset::Abstracts));
        assert_eq!(Dataset::from_name("tldrs"), Some(Dataset::Tldrs));
        assert_eq!(Dataset::from_name("citations"), Some(Dataset::Citations));
        assert_eq!(Dataset::from_name("embeddings"), Some(Dataset::Embeddings));
    }

    #[test]
    fn from_name_invalid() {
        assert_eq!(Dataset::from_name("Papers"), None);
        assert_eq!(Dataset::from_name("unknown"), None);
        assert_eq!(Dataset::from_name(""), None);
    }

    #[test]
    fn api_name_roundtrip() {
        // Every variant's api_name should be a non-empty string
        for ds in [
            Dataset::Papers,
            Dataset::Abstracts,
            Dataset::Tldrs,
            Dataset::Citations,
            Dataset::Embeddings,
        ] {
            assert!(!ds.api_name().is_empty());
            assert!(!ds.file_prefix().is_empty());
        }
    }

    #[test]
    fn embeddings_api_name_differs_from_prefix() {
        // embeddings has special API name "embeddings-specter_v2"
        assert_eq!(Dataset::Embeddings.api_name(), "embeddings-specter_v2");
        assert_eq!(Dataset::Embeddings.file_prefix(), "embeddings");
    }
}

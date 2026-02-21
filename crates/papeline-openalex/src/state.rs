//! Pipeline state types for OpenAlex data processing

use std::fmt;

/// OpenAlex entity types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Entity {
    /// Academic works (papers, articles, etc.)
    Works,
    /// Researchers and authors
    Authors,
    /// Journals, repositories, conferences
    Sources,
    /// Universities, research organizations
    Institutions,
    /// Academic publishers
    Publishers,
    /// Research topics (hierarchical classification)
    Topics,
    /// Funding organizations
    Funders,
}

impl Entity {
    /// Parse entity from string name (case-insensitive)
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "works" => Some(Self::Works),
            "authors" => Some(Self::Authors),
            "sources" => Some(Self::Sources),
            "institutions" => Some(Self::Institutions),
            "publishers" => Some(Self::Publishers),
            "topics" => Some(Self::Topics),
            "funders" => Some(Self::Funders),
            _ => None,
        }
    }

    /// Get manifest path suffix for this entity
    pub fn manifest_path(&self) -> &'static str {
        match self {
            Self::Works => "works",
            Self::Authors => "authors",
            Self::Sources => "sources",
            Self::Institutions => "institutions",
            Self::Publishers => "publishers",
            Self::Topics => "topics",
            Self::Funders => "funders",
        }
    }

    /// Get output filename prefix
    pub fn output_prefix(&self) -> &'static str {
        match self {
            Self::Works => "works",
            Self::Authors => "authors",
            Self::Sources => "sources",
            Self::Institutions => "institutions",
            Self::Publishers => "publishers",
            Self::Topics => "topics",
            Self::Funders => "funders",
        }
    }

    /// List all available entities
    pub fn all() -> &'static [Entity] {
        &[
            Self::Works,
            Self::Authors,
            Self::Sources,
            Self::Institutions,
            Self::Publishers,
            Self::Topics,
            Self::Funders,
        ]
    }
}

impl fmt::Display for Entity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.manifest_path())
    }
}

/// Information about a single shard to process
#[derive(Debug, Clone)]
pub struct ShardInfo {
    /// Entity type being processed
    pub entity: Entity,
    /// Shard index (for output filename)
    pub shard_idx: usize,
    /// HTTP URL to download
    pub url: String,
    /// Expected file size (from manifest meta)
    pub content_length: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_from_name() {
        assert_eq!(Entity::from_name("works"), Some(Entity::Works));
        assert_eq!(Entity::from_name("Works"), Some(Entity::Works));
        assert_eq!(Entity::from_name("WORKS"), Some(Entity::Works));
        assert_eq!(Entity::from_name("authors"), Some(Entity::Authors));
        assert_eq!(Entity::from_name("sources"), Some(Entity::Sources));
        assert_eq!(
            Entity::from_name("institutions"),
            Some(Entity::Institutions)
        );
        assert_eq!(Entity::from_name("publishers"), Some(Entity::Publishers));
        assert_eq!(Entity::from_name("topics"), Some(Entity::Topics));
        assert_eq!(Entity::from_name("funders"), Some(Entity::Funders));
        assert_eq!(Entity::from_name("unknown"), None);
    }

    #[test]
    fn entity_display() {
        assert_eq!(format!("{}", Entity::Works), "works");
        assert_eq!(format!("{}", Entity::Authors), "authors");
        assert_eq!(format!("{}", Entity::Funders), "funders");
    }

    #[test]
    fn entity_manifest_path() {
        assert_eq!(Entity::Works.manifest_path(), "works");
        assert_eq!(Entity::Authors.manifest_path(), "authors");
        assert_eq!(Entity::Topics.manifest_path(), "topics");
    }

    #[test]
    fn entity_all() {
        let all = Entity::all();
        assert_eq!(all.len(), 7);
        assert!(all.contains(&Entity::Works));
        assert!(all.contains(&Entity::Funders));
    }
}

//! Arrow schema definitions for OpenAlex entities
//!
//! Each entity has a corresponding schema function that returns a static Arc<Schema>.
//! Schemas are designed for analytical queries with flattened nested objects.

use std::sync::{Arc, LazyLock};

use arrow::datatypes::{DataType, Field, Schema};

// Helper for List<Utf8> field
fn list_utf8(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        nullable,
    )
}

/// works.parquet — flattened work metadata
///
/// Fields selected for:
/// - Core identifiers (id, doi, pmid, pmcid, mag)
/// - Metadata (title, publication_date, type, language)
/// - Metrics (cited_by_count, is_oa)
/// - Relationships (author_ids, institution_ids, primary_topic_id, source_id)
/// - Abstract (decoded from inverted index)
pub fn works() -> &'static Arc<Schema> {
    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            // Core identifiers
            Field::new("id", DataType::Utf8, false), // e.g., "W2741809807"
            Field::new("doi", DataType::Utf8, true),
            Field::new("pmid", DataType::Utf8, true),
            Field::new("pmcid", DataType::Utf8, true),
            Field::new("mag", DataType::Utf8, true), // stored as string for consistency
            // Metadata
            Field::new("title", DataType::Utf8, true),
            Field::new("display_name", DataType::Utf8, true),
            Field::new("publication_date", DataType::Utf8, true), // ISO 8601
            Field::new("publication_year", DataType::Int32, true),
            Field::new("language", DataType::Utf8, true), // ISO 639-1
            Field::new("type", DataType::Utf8, true),     // article, preprint, etc.
            // Metrics
            Field::new("cited_by_count", DataType::Int32, false),
            Field::new("is_oa", DataType::Boolean, false),
            Field::new("oa_status", DataType::Utf8, true), // gold, green, hybrid, bronze, closed
            // Abstract (decoded from inverted index)
            Field::new("abstract_text", DataType::Utf8, true),
            // Flattened relationships (as lists of IDs for join operations)
            list_utf8("author_ids", true),
            list_utf8("institution_ids", true),
            // Primary topic (flattened)
            Field::new("primary_topic_id", DataType::Utf8, true),
            Field::new("primary_topic_display_name", DataType::Utf8, true),
            // Source (venue)
            Field::new("source_id", DataType::Utf8, true),
            Field::new("source_display_name", DataType::Utf8, true),
            Field::new("source_type", DataType::Utf8, true), // journal, repository, etc.
            // Referenced works count (for network analysis)
            Field::new("referenced_works_count", DataType::Int32, false),
            // Timestamps
            Field::new("created_date", DataType::Utf8, true),
            Field::new("updated_date", DataType::Utf8, true),
        ]))
    });
    &SCHEMA
}

/// authors.parquet — researcher metadata
pub fn authors() -> &'static Arc<Schema> {
    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            // Core identifiers
            Field::new("id", DataType::Utf8, false), // e.g., "A5023888391"
            Field::new("orcid", DataType::Utf8, true),
            Field::new("scopus", DataType::Utf8, true),
            // Names
            Field::new("display_name", DataType::Utf8, true),
            list_utf8("display_name_alternatives", true),
            // Metrics
            Field::new("works_count", DataType::Int32, false),
            Field::new("cited_by_count", DataType::Int32, false),
            Field::new("h_index", DataType::Int32, true),
            Field::new("i10_index", DataType::Int32, true),
            Field::new("2yr_mean_citedness", DataType::Float64, true),
            // Affiliations (flattened to IDs)
            list_utf8("affiliation_ids", true),
            list_utf8("last_known_institution_ids", true),
            // Timestamps
            Field::new("created_date", DataType::Utf8, true),
            Field::new("updated_date", DataType::Utf8, true),
        ]))
    });
    &SCHEMA
}

/// sources.parquet — journals, repositories, conferences
pub fn sources() -> &'static Arc<Schema> {
    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            // Core identifiers
            Field::new("id", DataType::Utf8, false), // e.g., "S1983995261"
            Field::new("issn_l", DataType::Utf8, true),
            list_utf8("issn", true),
            // Names
            Field::new("display_name", DataType::Utf8, true),
            list_utf8("alternate_titles", true),
            Field::new("abbreviated_title", DataType::Utf8, true),
            // Classification
            Field::new("type", DataType::Utf8, true), // journal, repository, conference, etc.
            Field::new("is_oa", DataType::Boolean, false),
            Field::new("is_in_doaj", DataType::Boolean, false),
            // Host organization
            Field::new("host_organization_id", DataType::Utf8, true),
            Field::new("host_organization_name", DataType::Utf8, true),
            // Metrics
            Field::new("works_count", DataType::Int32, false),
            Field::new("cited_by_count", DataType::Int32, false),
            Field::new("h_index", DataType::Int32, true),
            Field::new("2yr_mean_citedness", DataType::Float64, true),
            // APC
            Field::new("apc_usd", DataType::Int32, true),
            // URLs
            Field::new("homepage_url", DataType::Utf8, true),
            // Timestamps
            Field::new("created_date", DataType::Utf8, true),
            Field::new("updated_date", DataType::Utf8, true),
        ]))
    });
    &SCHEMA
}

/// institutions.parquet — universities, research organizations
pub fn institutions() -> &'static Arc<Schema> {
    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            // Core identifiers
            Field::new("id", DataType::Utf8, false), // e.g., "I27837315"
            Field::new("ror", DataType::Utf8, true),
            Field::new("grid", DataType::Utf8, true),
            Field::new("mag", DataType::Utf8, true),
            Field::new("wikidata", DataType::Utf8, true),
            // Names
            Field::new("display_name", DataType::Utf8, true),
            list_utf8("display_name_acronyms", true),
            list_utf8("display_name_alternatives", true),
            // Classification
            Field::new("type", DataType::Utf8, true), // education, healthcare, company, etc.
            Field::new("country_code", DataType::Utf8, true), // ISO 3166-1 alpha-2
            // Location (flattened geo)
            Field::new("city", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, true),
            Field::new("latitude", DataType::Float64, true),
            Field::new("longitude", DataType::Float64, true),
            // Metrics
            Field::new("works_count", DataType::Int32, false),
            Field::new("cited_by_count", DataType::Int32, false),
            Field::new("h_index", DataType::Int32, true),
            Field::new("i10_index", DataType::Int32, true),
            Field::new("2yr_mean_citedness", DataType::Float64, true),
            // Hierarchy
            list_utf8("lineage", true),
            Field::new("is_super_system", DataType::Boolean, false),
            // URLs
            Field::new("homepage_url", DataType::Utf8, true),
            Field::new("image_url", DataType::Utf8, true),
            // Timestamps
            Field::new("created_date", DataType::Utf8, true),
            Field::new("updated_date", DataType::Utf8, true),
        ]))
    });
    &SCHEMA
}

/// publishers.parquet — academic publishers
pub fn publishers() -> &'static Arc<Schema> {
    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            // Core identifiers
            Field::new("id", DataType::Utf8, false), // e.g., "P4310319965"
            Field::new("ror", DataType::Utf8, true),
            Field::new("wikidata", DataType::Utf8, true),
            // Names
            Field::new("display_name", DataType::Utf8, true),
            list_utf8("alternate_titles", true),
            // Classification
            list_utf8("country_codes", true),
            Field::new("hierarchy_level", DataType::Int32, true),
            Field::new("parent_publisher_id", DataType::Utf8, true),
            list_utf8("lineage", true),
            // Metrics
            Field::new("works_count", DataType::Int32, false),
            Field::new("cited_by_count", DataType::Int32, false),
            Field::new("h_index", DataType::Int32, true),
            Field::new("i10_index", DataType::Int32, true),
            Field::new("2yr_mean_citedness", DataType::Float64, true),
            // URLs
            Field::new("image_url", DataType::Utf8, true),
            // Timestamps
            Field::new("created_date", DataType::Utf8, true),
            Field::new("updated_date", DataType::Utf8, true),
        ]))
    });
    &SCHEMA
}

/// topics.parquet — research topics (hierarchical: topic → subfield → field → domain)
pub fn topics() -> &'static Arc<Schema> {
    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            // Core identifiers
            Field::new("id", DataType::Utf8, false), // e.g., "T10112"
            Field::new("display_name", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            // Hierarchy (flattened)
            Field::new("subfield_id", DataType::Utf8, true),
            Field::new("subfield_display_name", DataType::Utf8, true),
            Field::new("field_id", DataType::Utf8, true),
            Field::new("field_display_name", DataType::Utf8, true),
            Field::new("domain_id", DataType::Utf8, true),
            Field::new("domain_display_name", DataType::Utf8, true),
            // Keywords
            list_utf8("keywords", true),
            // Metrics
            Field::new("works_count", DataType::Int32, false),
            // External
            Field::new("wikipedia_url", DataType::Utf8, true),
            // Timestamps
            Field::new("updated_date", DataType::Utf8, true),
        ]))
    });
    &SCHEMA
}

/// funders.parquet — funding organizations
pub fn funders() -> &'static Arc<Schema> {
    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            // Core identifiers
            Field::new("id", DataType::Utf8, false), // e.g., "F4320332161"
            Field::new("ror", DataType::Utf8, true),
            Field::new("crossref", DataType::Utf8, true),
            Field::new("wikidata", DataType::Utf8, true),
            // Names
            Field::new("display_name", DataType::Utf8, true),
            list_utf8("alternate_titles", true),
            // Classification
            Field::new("country_code", DataType::Utf8, true),
            // Metrics
            Field::new("works_count", DataType::Int32, false),
            Field::new("cited_by_count", DataType::Int32, false),
            Field::new("grants_count", DataType::Int32, false),
            Field::new("h_index", DataType::Int32, true),
            Field::new("2yr_mean_citedness", DataType::Float64, true),
            // URLs
            Field::new("homepage_url", DataType::Utf8, true),
            Field::new("image_url", DataType::Utf8, true),
            // Timestamps
            Field::new("created_date", DataType::Utf8, true),
            Field::new("updated_date", DataType::Utf8, true),
        ]))
    });
    &SCHEMA
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn works_schema_has_expected_fields() {
        let schema = works();
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("doi").is_ok());
        assert!(schema.field_with_name("abstract_text").is_ok());
        assert!(schema.field_with_name("author_ids").is_ok());
    }

    #[test]
    fn works_schema_field_count() {
        let schema = works();
        assert!(schema.fields().len() >= 20);
    }

    #[test]
    fn authors_schema_has_expected_fields() {
        let schema = authors();
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("orcid").is_ok());
        assert!(schema.field_with_name("display_name").is_ok());
        assert!(schema.field_with_name("h_index").is_ok());
    }

    #[test]
    fn sources_schema_has_expected_fields() {
        let schema = sources();
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("issn_l").is_ok());
        assert!(schema.field_with_name("type").is_ok());
        assert!(schema.field_with_name("is_oa").is_ok());
    }

    #[test]
    fn institutions_schema_has_expected_fields() {
        let schema = institutions();
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("ror").is_ok());
        assert!(schema.field_with_name("country_code").is_ok());
        assert!(schema.field_with_name("latitude").is_ok());
    }

    #[test]
    fn publishers_schema_has_expected_fields() {
        let schema = publishers();
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("display_name").is_ok());
        assert!(schema.field_with_name("hierarchy_level").is_ok());
    }

    #[test]
    fn topics_schema_has_expected_fields() {
        let schema = topics();
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("subfield_id").is_ok());
        assert!(schema.field_with_name("field_id").is_ok());
        assert!(schema.field_with_name("domain_id").is_ok());
    }

    #[test]
    fn funders_schema_has_expected_fields() {
        let schema = funders();
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("ror").is_ok());
        assert!(schema.field_with_name("grants_count").is_ok());
    }

    #[test]
    fn all_schemas_have_id_field() {
        for schema in [
            works(),
            authors(),
            sources(),
            institutions(),
            publishers(),
            topics(),
            funders(),
        ] {
            assert!(schema.field_with_name("id").is_ok());
        }
    }
}

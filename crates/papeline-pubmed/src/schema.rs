//! Arrow schema definitions for PubMed data
//!
//! Complete schema covering all PubMed XML fields for future extensibility.

use std::sync::{Arc, LazyLock};

use arrow::datatypes::{DataType, Field, Schema};

/// PubMed articles schema with all available fields
pub static ARTICLES: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        // === Identifiers ===
        Field::new("pmid", DataType::Utf8, false),
        Field::new("doi", DataType::Utf8, true),
        Field::new("pmc_id", DataType::Utf8, true),
        Field::new("pii", DataType::Utf8, true),
        // === Article ===
        Field::new("title", DataType::Utf8, true),
        Field::new("vernacular_title", DataType::Utf8, true),
        Field::new("abstract_text", DataType::Utf8, true),
        Field::new("language", DataType::Utf8, true),
        Field::new("publication_status", DataType::Utf8, true),
        // === Journal ===
        Field::new("journal_title", DataType::Utf8, true),
        Field::new("journal_iso", DataType::Utf8, true),
        Field::new("journal_issn", DataType::Utf8, true),
        Field::new("journal_volume", DataType::Utf8, true),
        Field::new("journal_issue", DataType::Utf8, true),
        Field::new("pagination", DataType::Utf8, true),
        Field::new("elocation_id", DataType::Utf8, true),
        // === Dates ===
        Field::new("pub_year", DataType::Int32, true),
        Field::new("pub_month", DataType::Int32, true),
        Field::new("pub_day", DataType::Int32, true),
        Field::new("date_completed", DataType::Utf8, true),
        Field::new("date_revised", DataType::Utf8, true),
        // === Authors (as JSON arrays for flexibility) ===
        Field::new("authors_json", DataType::Utf8, true),
        Field::new("affiliations_json", DataType::Utf8, true),
        Field::new("collective_name", DataType::Utf8, true),
        // === MeSH Terms ===
        Field::new("mesh_terms_json", DataType::Utf8, true),
        Field::new("mesh_major_topics", list_utf8(), true),
        // === Chemicals ===
        Field::new("chemicals_json", DataType::Utf8, true),
        // === Grants ===
        Field::new("grants_json", DataType::Utf8, true),
        // === Publication Types ===
        Field::new("publication_types", list_utf8(), true),
        // === Keywords ===
        Field::new("keywords", list_utf8(), true),
        // === Data Banks ===
        Field::new("databanks_json", DataType::Utf8, true),
        // === References ===
        Field::new("reference_count", DataType::Int32, true),
        // === Other ===
        Field::new("coi_statement", DataType::Utf8, true),
        Field::new("copyright_info", DataType::Utf8, true),
    ]))
});

/// Helper: create List<Utf8> type
fn list_utf8() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
}

pub fn articles() -> &'static Schema {
    &ARTICLES
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_has_expected_fields() {
        let schema = articles();
        assert!(schema.field_with_name("pmid").is_ok());
        assert!(schema.field_with_name("doi").is_ok());
        assert!(schema.field_with_name("mesh_terms_json").is_ok());
        assert!(schema.field_with_name("chemicals_json").is_ok());
    }
}

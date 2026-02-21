//! Arrow schema definitions for all dataset tables

use std::sync::{Arc, LazyLock};

use arrow::datatypes::{DataType, Field, Schema};

pub const EMBEDDING_DIM: i32 = 768;

/// papers.parquet — flattened paper metadata
pub fn papers() -> &'static Arc<Schema> {
    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("corpusid", DataType::Int64, false),
            Field::new("title", DataType::Utf8, false),
            Field::new("url", DataType::Utf8, false),
            Field::new("venue", DataType::Utf8, false),
            Field::new("publicationvenueid", DataType::Utf8, true),
            Field::new("year", DataType::Int32, true),
            Field::new("referencecount", DataType::Int32, false),
            Field::new("citationcount", DataType::Int32, false),
            Field::new("influentialcitationcount", DataType::Int32, false),
            Field::new("isopenaccess", DataType::Boolean, false),
            Field::new("publicationdate", DataType::Utf8, true),
            Field::new(
                "publicationtypes",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            // journal fields (flattened)
            Field::new("journal_name", DataType::Utf8, true),
            Field::new("journal_pages", DataType::Utf8, true),
            Field::new("journal_volume", DataType::Utf8, true),
            // externalids (flattened)
            Field::new("doi", DataType::Utf8, true),
            Field::new("pubmed", DataType::Utf8, true),
            Field::new("arxiv", DataType::Utf8, true),
            Field::new("mag", DataType::Utf8, true),
            Field::new("acl", DataType::Utf8, true),
            Field::new("dblp", DataType::Utf8, true),
            Field::new("pubmedcentral", DataType::Utf8, true),
        ]))
    });
    &SCHEMA
}

/// paper_authors.parquet — one row per (paper, author) pair
pub fn paper_authors() -> &'static Arc<Schema> {
    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("corpusid", DataType::Int64, false),
            Field::new("authorid", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("position", DataType::Int32, false),
        ]))
    });
    &SCHEMA
}

/// paper_fields.parquet — one row per (paper, field of study) pair
pub fn paper_fields() -> &'static Arc<Schema> {
    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("corpusid", DataType::Int64, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("source", DataType::Utf8, false),
        ]))
    });
    &SCHEMA
}

/// citations.parquet — Bio→Bio citation edges
pub fn citations() -> &'static Arc<Schema> {
    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("citationid", DataType::Int64, false),
            Field::new("citingcorpusid", DataType::Int64, false),
            Field::new("citedcorpusid", DataType::Int64, false),
            Field::new("isinfluential", DataType::Boolean, false),
            Field::new(
                "contexts",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "intents",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                ))),
                true,
            ),
        ]))
    });
    &SCHEMA
}

/// abstracts.parquet
pub fn abstracts() -> &'static Arc<Schema> {
    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("corpusid", DataType::Int64, false),
            Field::new("abstract", DataType::Utf8, false),
        ]))
    });
    &SCHEMA
}

/// tldrs.parquet
pub fn tldrs() -> &'static Arc<Schema> {
    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("corpusid", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
        ]))
    });
    &SCHEMA
}

/// embeddings.parquet — SPECTER v2 768-dim vectors
pub fn embeddings() -> &'static Arc<Schema> {
    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("corpusid", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, false)),
                    EMBEDDING_DIM,
                ),
                false,
            ),
        ]))
    });
    &SCHEMA
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn papers_schema_has_expected_fields() {
        let schema = papers();
        assert!(schema.field_with_name("corpusid").is_ok());
        assert!(schema.field_with_name("title").is_ok());
        assert!(schema.field_with_name("doi").is_ok());
    }

    #[test]
    fn paper_authors_schema_has_expected_fields() {
        let schema = paper_authors();
        assert!(schema.field_with_name("corpusid").is_ok());
        assert!(schema.field_with_name("authorid").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("position").is_ok());
    }

    #[test]
    fn citations_schema_has_contexts_and_intents() {
        let schema = citations();
        assert!(schema.field_with_name("contexts").is_ok());
        assert!(schema.field_with_name("intents").is_ok());
    }

    #[test]
    fn embeddings_schema_vector_dimension() {
        let schema = embeddings();
        let field = schema.field_with_name("vector").unwrap();
        if let DataType::FixedSizeList(_, dim) = field.data_type() {
            assert_eq!(*dim, EMBEDDING_DIM);
        } else {
            panic!("vector field should be FixedSizeList");
        }
    }

    #[test]
    fn all_schemas_have_corpusid() {
        for schema in [
            papers(),
            paper_authors(),
            paper_fields(),
            abstracts(),
            tldrs(),
            embeddings(),
        ] {
            assert!(schema.field_with_name("corpusid").is_ok());
        }
    }
}

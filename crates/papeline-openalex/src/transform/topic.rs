//! Topic entity transformation: JSON → Arrow RecordBatch

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::Schema;
use serde::Deserialize;

use super::{Accumulator, RECORD_BATCH_SIZE};
use crate::schema;

/// Extract short ID from full OpenAlex URL
fn extract_short_id(url: &str) -> String {
    url.rsplit('/').next().unwrap_or(url).to_string()
}

// === Row struct (deserialized from JSON) ===

/// OpenAlex Topic JSON structure (hierarchical: topic → subfield → field → domain)
#[derive(Debug, Deserialize)]
pub struct TopicRow {
    #[serde(default)]
    pub id: String,

    #[serde(default)]
    pub display_name: Option<String>,

    #[serde(default)]
    pub description: Option<String>,

    #[serde(default)]
    pub subfield: Option<HierarchyItem>,

    #[serde(default)]
    pub field: Option<HierarchyItem>,

    #[serde(default)]
    pub domain: Option<HierarchyItem>,

    #[serde(default)]
    pub keywords: Vec<String>,

    #[serde(default)]
    pub works_count: i32,

    #[serde(default)]
    pub wikipedia: Option<String>,

    #[serde(default)]
    pub updated_date: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct HierarchyItem {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
}

impl TopicRow {
    pub fn short_id(&self) -> &str {
        self.id
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or(&self.id)
    }

    pub fn subfield_id(&self) -> Option<String> {
        self.subfield
            .as_ref()
            .and_then(|s| s.id.as_ref())
            .map(|id| extract_short_id(id))
    }

    pub fn subfield_display_name(&self) -> Option<String> {
        self.subfield.as_ref().and_then(|s| s.display_name.clone())
    }

    pub fn field_id(&self) -> Option<String> {
        self.field
            .as_ref()
            .and_then(|f| f.id.as_ref())
            .map(|id| extract_short_id(id))
    }

    pub fn field_display_name(&self) -> Option<String> {
        self.field.as_ref().and_then(|f| f.display_name.clone())
    }

    pub fn domain_id(&self) -> Option<String> {
        self.domain
            .as_ref()
            .and_then(|d| d.id.as_ref())
            .map(|id| extract_short_id(id))
    }

    pub fn domain_display_name(&self) -> Option<String> {
        self.domain.as_ref().and_then(|d| d.display_name.clone())
    }
}

// === Accumulator ===

pub struct TopicAccumulator {
    schema: Arc<Schema>,
    id: Vec<String>,
    display_name: Vec<Option<String>>,
    description: Vec<Option<String>>,
    subfield_id: Vec<Option<String>>,
    subfield_display_name: Vec<Option<String>>,
    field_id: Vec<Option<String>>,
    field_display_name: Vec<Option<String>>,
    domain_id: Vec<Option<String>>,
    domain_display_name: Vec<Option<String>>,
    keywords: Vec<Option<Vec<Option<String>>>>,
    works_count: Vec<i32>,
    wikipedia_url: Vec<Option<String>>,
    updated_date: Vec<Option<String>>,
}

impl TopicAccumulator {
    pub fn new() -> Self {
        Self {
            schema: schema::topics().clone(),
            id: Vec::with_capacity(RECORD_BATCH_SIZE),
            display_name: Vec::with_capacity(RECORD_BATCH_SIZE),
            description: Vec::with_capacity(RECORD_BATCH_SIZE),
            subfield_id: Vec::with_capacity(RECORD_BATCH_SIZE),
            subfield_display_name: Vec::with_capacity(RECORD_BATCH_SIZE),
            field_id: Vec::with_capacity(RECORD_BATCH_SIZE),
            field_display_name: Vec::with_capacity(RECORD_BATCH_SIZE),
            domain_id: Vec::with_capacity(RECORD_BATCH_SIZE),
            domain_display_name: Vec::with_capacity(RECORD_BATCH_SIZE),
            keywords: Vec::with_capacity(RECORD_BATCH_SIZE),
            works_count: Vec::with_capacity(RECORD_BATCH_SIZE),
            wikipedia_url: Vec::with_capacity(RECORD_BATCH_SIZE),
            updated_date: Vec::with_capacity(RECORD_BATCH_SIZE),
        }
    }
}

impl Default for TopicAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for TopicAccumulator {
    type Row = TopicRow;

    fn push(&mut self, row: TopicRow) {
        let id = row.short_id().to_string();
        let subfield_id = row.subfield_id();
        let subfield_display_name = row.subfield_display_name();
        let field_id = row.field_id();
        let field_display_name = row.field_display_name();
        let domain_id = row.domain_id();
        let domain_display_name = row.domain_display_name();

        self.id.push(id);
        self.display_name.push(row.display_name);
        self.description.push(row.description);
        self.subfield_id.push(subfield_id);
        self.subfield_display_name.push(subfield_display_name);
        self.field_id.push(field_id);
        self.field_display_name.push(field_display_name);
        self.domain_id.push(domain_id);
        self.domain_display_name.push(domain_display_name);
        self.keywords.push(if row.keywords.is_empty() {
            None
        } else {
            Some(row.keywords.into_iter().map(Some).collect())
        });
        self.works_count.push(row.works_count);
        self.wikipedia_url.push(row.wikipedia);
        self.updated_date.push(row.updated_date);
    }

    fn len(&self) -> usize {
        self.id.len()
    }

    fn take_batch(&mut self) -> Result<RecordBatch, arrow::error::ArrowError> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(std::mem::take(&mut self.id))),
            Arc::new(StringArray::from(std::mem::take(&mut self.display_name))),
            Arc::new(StringArray::from(std::mem::take(&mut self.description))),
            Arc::new(StringArray::from(std::mem::take(&mut self.subfield_id))),
            Arc::new(StringArray::from(std::mem::take(
                &mut self.subfield_display_name,
            ))),
            Arc::new(StringArray::from(std::mem::take(&mut self.field_id))),
            Arc::new(StringArray::from(std::mem::take(
                &mut self.field_display_name,
            ))),
            Arc::new(StringArray::from(std::mem::take(&mut self.domain_id))),
            Arc::new(StringArray::from(std::mem::take(
                &mut self.domain_display_name,
            ))),
            build_list_string_array(std::mem::take(&mut self.keywords)),
            Arc::new(Int32Array::from(std::mem::take(&mut self.works_count))),
            Arc::new(StringArray::from(std::mem::take(&mut self.wikipedia_url))),
            Arc::new(StringArray::from(std::mem::take(&mut self.updated_date))),
        ];
        RecordBatch::try_new(self.schema.clone(), arrays)
    }
}

/// Build List<Utf8> array from Vec<Option<Vec<Option<String>>>>
fn build_list_string_array(data: Vec<Option<Vec<Option<String>>>>) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for row in &data {
        match row {
            Some(items) => {
                for item in items {
                    match item {
                        Some(s) => builder.values().append_value(s),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            None => builder.append(false),
        }
    }
    Arc::new(builder.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_TOPIC: &str = r#"{
        "id": "https://openalex.org/T10112",
        "display_name": "Machine Learning",
        "description": "Study of algorithms that improve through experience",
        "subfield": {
            "id": "https://openalex.org/subfields/1702",
            "display_name": "Artificial Intelligence"
        },
        "field": {
            "id": "https://openalex.org/fields/17",
            "display_name": "Computer Science"
        },
        "domain": {
            "id": "https://openalex.org/domains/3",
            "display_name": "Physical Sciences"
        },
        "keywords": ["neural networks", "deep learning", "supervised learning"],
        "works_count": 500000,
        "wikipedia": "https://en.wikipedia.org/wiki/Machine_learning",
        "updated_date": "2025-01-15"
    }"#;

    #[test]
    fn parse_topic_row() {
        let row: TopicRow = serde_json::from_str(SAMPLE_TOPIC).unwrap();
        assert_eq!(row.short_id(), "T10112");
        assert_eq!(row.display_name, Some("Machine Learning".to_string()));
    }

    #[test]
    fn topic_hierarchy() {
        let row: TopicRow = serde_json::from_str(SAMPLE_TOPIC).unwrap();
        assert_eq!(row.subfield_id(), Some("1702".to_string()));
        assert_eq!(
            row.subfield_display_name(),
            Some("Artificial Intelligence".to_string())
        );
        assert_eq!(row.field_id(), Some("17".to_string()));
        assert_eq!(
            row.field_display_name(),
            Some("Computer Science".to_string())
        );
        assert_eq!(row.domain_id(), Some("3".to_string()));
        assert_eq!(
            row.domain_display_name(),
            Some("Physical Sciences".to_string())
        );
    }

    #[test]
    fn topic_keywords() {
        let row: TopicRow = serde_json::from_str(SAMPLE_TOPIC).unwrap();
        assert_eq!(row.keywords.len(), 3);
        assert!(row.keywords.contains(&"neural networks".to_string()));
    }

    #[test]
    fn topic_works_count() {
        let row: TopicRow = serde_json::from_str(SAMPLE_TOPIC).unwrap();
        assert_eq!(row.works_count, 500000);
    }

    #[test]
    fn accumulator_batch() {
        let mut acc = TopicAccumulator::new();
        let row: TopicRow = serde_json::from_str(SAMPLE_TOPIC).unwrap();
        acc.push(row);
        assert_eq!(acc.len(), 1);

        let batch = acc.take_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(acc.len(), 0);
    }

    #[test]
    fn minimal_topic() {
        let json = r#"{"id": "https://openalex.org/T1"}"#;
        let row: TopicRow = serde_json::from_str(json).unwrap();
        assert_eq!(row.short_id(), "T1");
        assert!(row.display_name.is_none());
        assert!(row.subfield_id().is_none());
    }
}

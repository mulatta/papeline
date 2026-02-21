//! Work entity transformation: JSON → Arrow RecordBatch

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::Schema;
use serde::Deserialize;
use serde_json::{Map, Value};

use super::{Accumulator, RECORD_BATCH_SIZE};
use crate::abstract_decode::decode_inverted_index;
use crate::schema;

// === Row struct (deserialized from JSON) ===

/// OpenAlex Work JSON structure
#[derive(Debug, Deserialize)]
pub struct WorkRow {
    /// OpenAlex ID (e.g., "https://openalex.org/W2741809807")
    #[serde(default)]
    pub id: String,

    /// Digital Object Identifier
    #[serde(default)]
    pub doi: Option<String>,

    /// Title of the work
    #[serde(default)]
    pub title: Option<String>,

    /// Display name (usually same as title)
    #[serde(default)]
    pub display_name: Option<String>,

    /// Publication date (ISO 8601)
    #[serde(default)]
    pub publication_date: Option<String>,

    /// Publication year
    #[serde(default)]
    pub publication_year: Option<i32>,

    /// Language (ISO 639-1)
    #[serde(default)]
    pub language: Option<String>,

    /// Work type (article, preprint, book, etc.)
    #[serde(rename = "type", default)]
    pub work_type: Option<String>,

    /// Citation count
    #[serde(default)]
    pub cited_by_count: i32,

    /// Open access info
    #[serde(default)]
    pub open_access: Option<OpenAccessInfo>,

    /// Abstract as inverted index
    #[serde(default)]
    pub abstract_inverted_index: Option<Map<String, Value>>,

    /// Authorships (authors + institutions)
    #[serde(default)]
    pub authorships: Vec<Authorship>,

    /// Primary topic
    #[serde(default)]
    pub primary_topic: Option<Topic>,

    /// Primary location (source/venue info)
    #[serde(default)]
    pub primary_location: Option<Location>,

    /// IDs from other systems
    #[serde(default)]
    pub ids: Option<ExternalIds>,

    /// Referenced works
    #[serde(default)]
    pub referenced_works: Vec<String>,

    /// Created timestamp
    #[serde(default)]
    pub created_date: Option<String>,

    /// Updated timestamp
    #[serde(default)]
    pub updated_date: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct OpenAccessInfo {
    #[serde(default)]
    pub is_oa: bool,
    #[serde(default)]
    pub oa_status: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Authorship {
    #[serde(default)]
    pub author: Option<Author>,
    #[serde(default)]
    pub institutions: Vec<Institution>,
}

#[derive(Debug, Deserialize)]
pub struct Author {
    #[serde(default)]
    pub id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Institution {
    #[serde(default)]
    pub id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Topic {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Location {
    #[serde(default)]
    pub source: Option<Source>,
}

#[derive(Debug, Deserialize)]
pub struct Source {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(rename = "type", default)]
    pub source_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ExternalIds {
    #[serde(default)]
    pub pmid: Option<String>,
    #[serde(default)]
    pub pmcid: Option<String>,
    #[serde(default)]
    pub mag: Option<String>,
}

impl WorkRow {
    /// Extract short ID from full URL (e.g., "https://openalex.org/W123" -> "W123")
    pub fn short_id(&self) -> &str {
        self.id
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or(&self.id)
    }

    /// Decode abstract from inverted index
    pub fn abstract_text(&self) -> Option<String> {
        self.abstract_inverted_index
            .as_ref()
            .map(decode_inverted_index)
            .filter(|s| !s.is_empty())
    }

    /// Extract author IDs from authorships
    pub fn author_ids(&self) -> Vec<Option<String>> {
        self.authorships
            .iter()
            .filter_map(|a| a.author.as_ref())
            .map(|a| a.id.as_ref().map(|id| extract_short_id(id)))
            .collect()
    }

    /// Extract institution IDs from authorships (deduplicated)
    pub fn institution_ids(&self) -> Vec<Option<String>> {
        let mut ids: Vec<_> = self
            .authorships
            .iter()
            .flat_map(|a| &a.institutions)
            .filter_map(|i| i.id.as_ref())
            .map(|id| Some(extract_short_id(id)))
            .collect();
        ids.sort();
        ids.dedup();
        ids
    }

    /// Get open access status
    pub fn is_oa(&self) -> bool {
        self.open_access.as_ref().is_some_and(|oa| oa.is_oa)
    }

    /// Get OA status string
    pub fn oa_status(&self) -> Option<String> {
        self.open_access
            .as_ref()
            .and_then(|oa| oa.oa_status.clone())
    }

    /// Get primary topic ID
    pub fn primary_topic_id(&self) -> Option<String> {
        self.primary_topic
            .as_ref()
            .and_then(|t| t.id.as_ref())
            .map(|id| extract_short_id(id))
    }

    /// Get primary topic display name
    pub fn primary_topic_display_name(&self) -> Option<String> {
        self.primary_topic
            .as_ref()
            .and_then(|t| t.display_name.clone())
    }

    /// Get source (venue) info
    pub fn source_id(&self) -> Option<String> {
        self.primary_location
            .as_ref()
            .and_then(|loc| loc.source.as_ref())
            .and_then(|s| s.id.as_ref())
            .map(|id| extract_short_id(id))
    }

    pub fn source_display_name(&self) -> Option<String> {
        self.primary_location
            .as_ref()
            .and_then(|loc| loc.source.as_ref())
            .and_then(|s| s.display_name.clone())
    }

    pub fn source_type(&self) -> Option<String> {
        self.primary_location
            .as_ref()
            .and_then(|loc| loc.source.as_ref())
            .and_then(|s| s.source_type.clone())
    }

    /// Get external IDs
    pub fn pmid(&self) -> Option<String> {
        self.ids.as_ref().and_then(|ids| ids.pmid.clone())
    }

    pub fn pmcid(&self) -> Option<String> {
        self.ids.as_ref().and_then(|ids| ids.pmcid.clone())
    }

    pub fn mag(&self) -> Option<String> {
        self.ids.as_ref().and_then(|ids| ids.mag.clone())
    }
}

/// Extract short ID from full OpenAlex URL
fn extract_short_id(url: &str) -> String {
    url.rsplit('/').next().unwrap_or(url).to_string()
}

// === Accumulator ===

/// Accumulator for Work rows
pub struct WorkAccumulator {
    schema: Arc<Schema>,
    // Core identifiers
    id: Vec<String>,
    doi: Vec<Option<String>>,
    pmid: Vec<Option<String>>,
    pmcid: Vec<Option<String>>,
    mag: Vec<Option<String>>,
    // Metadata
    title: Vec<Option<String>>,
    display_name: Vec<Option<String>>,
    publication_date: Vec<Option<String>>,
    publication_year: Vec<Option<i32>>,
    language: Vec<Option<String>>,
    work_type: Vec<Option<String>>,
    // Metrics
    cited_by_count: Vec<i32>,
    is_oa: Vec<bool>,
    oa_status: Vec<Option<String>>,
    // Abstract
    abstract_text: Vec<Option<String>>,
    // Relationships
    author_ids: Vec<Option<Vec<Option<String>>>>,
    institution_ids: Vec<Option<Vec<Option<String>>>>,
    primary_topic_id: Vec<Option<String>>,
    primary_topic_display_name: Vec<Option<String>>,
    source_id: Vec<Option<String>>,
    source_display_name: Vec<Option<String>>,
    source_type: Vec<Option<String>>,
    referenced_works_count: Vec<i32>,
    // Timestamps
    created_date: Vec<Option<String>>,
    updated_date: Vec<Option<String>>,
}

impl WorkAccumulator {
    pub fn new() -> Self {
        Self {
            schema: schema::works().clone(),
            id: Vec::with_capacity(RECORD_BATCH_SIZE),
            doi: Vec::with_capacity(RECORD_BATCH_SIZE),
            pmid: Vec::with_capacity(RECORD_BATCH_SIZE),
            pmcid: Vec::with_capacity(RECORD_BATCH_SIZE),
            mag: Vec::with_capacity(RECORD_BATCH_SIZE),
            title: Vec::with_capacity(RECORD_BATCH_SIZE),
            display_name: Vec::with_capacity(RECORD_BATCH_SIZE),
            publication_date: Vec::with_capacity(RECORD_BATCH_SIZE),
            publication_year: Vec::with_capacity(RECORD_BATCH_SIZE),
            language: Vec::with_capacity(RECORD_BATCH_SIZE),
            work_type: Vec::with_capacity(RECORD_BATCH_SIZE),
            cited_by_count: Vec::with_capacity(RECORD_BATCH_SIZE),
            is_oa: Vec::with_capacity(RECORD_BATCH_SIZE),
            oa_status: Vec::with_capacity(RECORD_BATCH_SIZE),
            abstract_text: Vec::with_capacity(RECORD_BATCH_SIZE),
            author_ids: Vec::with_capacity(RECORD_BATCH_SIZE),
            institution_ids: Vec::with_capacity(RECORD_BATCH_SIZE),
            primary_topic_id: Vec::with_capacity(RECORD_BATCH_SIZE),
            primary_topic_display_name: Vec::with_capacity(RECORD_BATCH_SIZE),
            source_id: Vec::with_capacity(RECORD_BATCH_SIZE),
            source_display_name: Vec::with_capacity(RECORD_BATCH_SIZE),
            source_type: Vec::with_capacity(RECORD_BATCH_SIZE),
            referenced_works_count: Vec::with_capacity(RECORD_BATCH_SIZE),
            created_date: Vec::with_capacity(RECORD_BATCH_SIZE),
            updated_date: Vec::with_capacity(RECORD_BATCH_SIZE),
        }
    }
}

impl Default for WorkAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for WorkAccumulator {
    type Row = WorkRow;

    fn push(&mut self, row: WorkRow) {
        // Extract computed values first (before moving fields)
        let id = row.short_id().to_string();
        let pmid = row.pmid();
        let pmcid = row.pmcid();
        let mag = row.mag();
        let is_oa = row.is_oa();
        let oa_status = row.oa_status();
        let abstract_text = row.abstract_text();
        let author_ids = row.author_ids();
        let institution_ids = row.institution_ids();
        let primary_topic_id = row.primary_topic_id();
        let primary_topic_display_name = row.primary_topic_display_name();
        let source_id = row.source_id();
        let source_display_name = row.source_display_name();
        let source_type = row.source_type();
        let referenced_works_count = row.referenced_works.len() as i32;

        // Now push values (moving owned fields)
        self.id.push(id);
        self.doi.push(row.doi);
        self.pmid.push(pmid);
        self.pmcid.push(pmcid);
        self.mag.push(mag);
        self.title.push(row.title);
        self.display_name.push(row.display_name);
        self.publication_date.push(row.publication_date);
        self.publication_year.push(row.publication_year);
        self.language.push(row.language);
        self.work_type.push(row.work_type);
        self.cited_by_count.push(row.cited_by_count);
        self.is_oa.push(is_oa);
        self.oa_status.push(oa_status);
        self.abstract_text.push(abstract_text);

        self.author_ids.push(if author_ids.is_empty() {
            None
        } else {
            Some(author_ids)
        });

        self.institution_ids.push(if institution_ids.is_empty() {
            None
        } else {
            Some(institution_ids)
        });

        self.primary_topic_id.push(primary_topic_id);
        self.primary_topic_display_name
            .push(primary_topic_display_name);
        self.source_id.push(source_id);
        self.source_display_name.push(source_display_name);
        self.source_type.push(source_type);
        self.referenced_works_count.push(referenced_works_count);
        self.created_date.push(row.created_date);
        self.updated_date.push(row.updated_date);
    }

    fn len(&self) -> usize {
        self.id.len()
    }

    fn take_batch(&mut self) -> RecordBatch {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(std::mem::take(&mut self.id))),
            Arc::new(StringArray::from(std::mem::take(&mut self.doi))),
            Arc::new(StringArray::from(std::mem::take(&mut self.pmid))),
            Arc::new(StringArray::from(std::mem::take(&mut self.pmcid))),
            Arc::new(StringArray::from(std::mem::take(&mut self.mag))),
            Arc::new(StringArray::from(std::mem::take(&mut self.title))),
            Arc::new(StringArray::from(std::mem::take(&mut self.display_name))),
            Arc::new(StringArray::from(std::mem::take(
                &mut self.publication_date,
            ))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.publication_year))),
            Arc::new(StringArray::from(std::mem::take(&mut self.language))),
            Arc::new(StringArray::from(std::mem::take(&mut self.work_type))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.cited_by_count))),
            Arc::new(BooleanArray::from(std::mem::take(&mut self.is_oa))),
            Arc::new(StringArray::from(std::mem::take(&mut self.oa_status))),
            Arc::new(StringArray::from(std::mem::take(&mut self.abstract_text))),
            build_list_string_array(std::mem::take(&mut self.author_ids)),
            build_list_string_array(std::mem::take(&mut self.institution_ids)),
            Arc::new(StringArray::from(std::mem::take(
                &mut self.primary_topic_id,
            ))),
            Arc::new(StringArray::from(std::mem::take(
                &mut self.primary_topic_display_name,
            ))),
            Arc::new(StringArray::from(std::mem::take(&mut self.source_id))),
            Arc::new(StringArray::from(std::mem::take(
                &mut self.source_display_name,
            ))),
            Arc::new(StringArray::from(std::mem::take(&mut self.source_type))),
            Arc::new(Int32Array::from(std::mem::take(
                &mut self.referenced_works_count,
            ))),
            Arc::new(StringArray::from(std::mem::take(&mut self.created_date))),
            Arc::new(StringArray::from(std::mem::take(&mut self.updated_date))),
        ];
        RecordBatch::try_new(self.schema.clone(), arrays).expect("works schema mismatch")
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

    const SAMPLE_WORK: &str = r#"{
        "id": "https://openalex.org/W2741809807",
        "doi": "https://doi.org/10.1038/s41586-018-0102-6",
        "title": "Sample Title",
        "display_name": "Sample Title",
        "publication_date": "2025-01-15",
        "publication_year": 2025,
        "language": "en",
        "type": "article",
        "cited_by_count": 42,
        "open_access": {"is_oa": true, "oa_status": "gold"},
        "abstract_inverted_index": {"Hello": [0], "world": [1]},
        "authorships": [
            {"author": {"id": "https://openalex.org/A123"}, "institutions": [{"id": "https://openalex.org/I456"}]}
        ],
        "primary_topic": {"id": "https://openalex.org/T789", "display_name": "Machine Learning"},
        "primary_location": {"source": {"id": "https://openalex.org/S111", "display_name": "Nature", "type": "journal"}},
        "ids": {"pmid": "12345678", "pmcid": "PMC1234567", "mag": "987654321"},
        "referenced_works": ["https://openalex.org/W1", "https://openalex.org/W2"],
        "created_date": "2020-01-01",
        "updated_date": "2025-01-15"
    }"#;

    #[test]
    fn parse_work_row() {
        let row: WorkRow = serde_json::from_str(SAMPLE_WORK).unwrap();
        assert_eq!(row.short_id(), "W2741809807");
        assert_eq!(
            row.doi,
            Some("https://doi.org/10.1038/s41586-018-0102-6".to_string())
        );
        assert_eq!(row.publication_year, Some(2025));
        assert_eq!(row.cited_by_count, 42);
    }

    #[test]
    fn work_abstract_decode() {
        let row: WorkRow = serde_json::from_str(SAMPLE_WORK).unwrap();
        assert_eq!(row.abstract_text(), Some("Hello world".to_string()));
    }

    #[test]
    fn work_author_ids() {
        let row: WorkRow = serde_json::from_str(SAMPLE_WORK).unwrap();
        let ids = row.author_ids();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], Some("A123".to_string()));
    }

    #[test]
    fn work_institution_ids() {
        let row: WorkRow = serde_json::from_str(SAMPLE_WORK).unwrap();
        let ids = row.institution_ids();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], Some("I456".to_string()));
    }

    #[test]
    fn work_oa_status() {
        let row: WorkRow = serde_json::from_str(SAMPLE_WORK).unwrap();
        assert!(row.is_oa());
        assert_eq!(row.oa_status(), Some("gold".to_string()));
    }

    #[test]
    fn work_topic() {
        let row: WorkRow = serde_json::from_str(SAMPLE_WORK).unwrap();
        assert_eq!(row.primary_topic_id(), Some("T789".to_string()));
        assert_eq!(
            row.primary_topic_display_name(),
            Some("Machine Learning".to_string())
        );
    }

    #[test]
    fn work_source() {
        let row: WorkRow = serde_json::from_str(SAMPLE_WORK).unwrap();
        assert_eq!(row.source_id(), Some("S111".to_string()));
        assert_eq!(row.source_display_name(), Some("Nature".to_string()));
        assert_eq!(row.source_type(), Some("journal".to_string()));
    }

    #[test]
    fn work_external_ids() {
        let row: WorkRow = serde_json::from_str(SAMPLE_WORK).unwrap();
        assert_eq!(row.pmid(), Some("12345678".to_string()));
        assert_eq!(row.pmcid(), Some("PMC1234567".to_string()));
        assert_eq!(row.mag(), Some("987654321".to_string()));
    }

    #[test]
    fn work_referenced_works() {
        let row: WorkRow = serde_json::from_str(SAMPLE_WORK).unwrap();
        assert_eq!(row.referenced_works.len(), 2);
    }

    #[test]
    fn accumulator_batch() {
        let mut acc = WorkAccumulator::new();
        let row: WorkRow = serde_json::from_str(SAMPLE_WORK).unwrap();
        acc.push(row);
        assert_eq!(acc.len(), 1);

        let batch = acc.take_batch();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(acc.len(), 0);
    }

    #[test]
    fn minimal_work() {
        let json = r#"{"id": "https://openalex.org/W1"}"#;
        let row: WorkRow = serde_json::from_str(json).unwrap();
        assert_eq!(row.short_id(), "W1");
        assert!(row.doi.is_none());
        assert!(row.abstract_text().is_none());
    }
}

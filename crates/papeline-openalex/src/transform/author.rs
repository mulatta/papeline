//! Author entity transformation: JSON → Arrow RecordBatch

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

/// OpenAlex Author JSON structure
#[derive(Debug, Deserialize)]
pub struct AuthorRow {
    #[serde(default)]
    pub id: String,

    #[serde(default)]
    pub ids: Option<AuthorIds>,

    #[serde(default)]
    pub display_name: Option<String>,

    #[serde(default)]
    pub display_name_alternatives: Vec<String>,

    #[serde(default)]
    pub works_count: i32,

    #[serde(default)]
    pub cited_by_count: i32,

    #[serde(default)]
    pub summary_stats: Option<SummaryStats>,

    #[serde(default)]
    pub affiliations: Vec<Affiliation>,

    #[serde(default)]
    pub last_known_institutions: Vec<InstitutionRef>,

    #[serde(default)]
    pub created_date: Option<String>,

    #[serde(default)]
    pub updated_date: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct AuthorIds {
    #[serde(default)]
    pub orcid: Option<String>,
    #[serde(default)]
    pub scopus: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct SummaryStats {
    #[serde(default)]
    pub h_index: Option<i32>,
    #[serde(default)]
    pub i10_index: Option<i32>,
    #[serde(rename = "2yr_mean_citedness", default)]
    pub two_yr_mean_citedness: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct Affiliation {
    #[serde(default)]
    pub institution: Option<InstitutionRef>,
}

#[derive(Debug, Deserialize)]
pub struct InstitutionRef {
    #[serde(default)]
    pub id: Option<String>,
}

impl AuthorRow {
    pub fn short_id(&self) -> &str {
        self.id
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or(&self.id)
    }

    pub fn orcid(&self) -> Option<String> {
        self.ids.as_ref().and_then(|ids| ids.orcid.clone())
    }

    pub fn scopus(&self) -> Option<String> {
        self.ids.as_ref().and_then(|ids| ids.scopus.clone())
    }

    pub fn h_index(&self) -> Option<i32> {
        self.summary_stats.as_ref().and_then(|s| s.h_index)
    }

    pub fn i10_index(&self) -> Option<i32> {
        self.summary_stats.as_ref().and_then(|s| s.i10_index)
    }

    pub fn two_yr_mean_citedness(&self) -> Option<f64> {
        self.summary_stats
            .as_ref()
            .and_then(|s| s.two_yr_mean_citedness)
    }

    pub fn affiliation_ids(&self) -> Vec<Option<String>> {
        self.affiliations
            .iter()
            .filter_map(|a| a.institution.as_ref())
            .filter_map(|i| i.id.as_ref())
            .map(|id| Some(extract_short_id(id)))
            .collect()
    }

    pub fn last_known_institution_ids(&self) -> Vec<Option<String>> {
        self.last_known_institutions
            .iter()
            .filter_map(|i| i.id.as_ref())
            .map(|id| Some(extract_short_id(id)))
            .collect()
    }
}

// === Accumulator ===

pub struct AuthorAccumulator {
    schema: Arc<Schema>,
    id: Vec<String>,
    orcid: Vec<Option<String>>,
    scopus: Vec<Option<String>>,
    display_name: Vec<Option<String>>,
    display_name_alternatives: Vec<Option<Vec<Option<String>>>>,
    works_count: Vec<i32>,
    cited_by_count: Vec<i32>,
    h_index: Vec<Option<i32>>,
    i10_index: Vec<Option<i32>>,
    two_yr_mean_citedness: Vec<Option<f64>>,
    affiliation_ids: Vec<Option<Vec<Option<String>>>>,
    last_known_institution_ids: Vec<Option<Vec<Option<String>>>>,
    created_date: Vec<Option<String>>,
    updated_date: Vec<Option<String>>,
}

impl AuthorAccumulator {
    pub fn new() -> Self {
        Self {
            schema: schema::authors().clone(),
            id: Vec::with_capacity(RECORD_BATCH_SIZE),
            orcid: Vec::with_capacity(RECORD_BATCH_SIZE),
            scopus: Vec::with_capacity(RECORD_BATCH_SIZE),
            display_name: Vec::with_capacity(RECORD_BATCH_SIZE),
            display_name_alternatives: Vec::with_capacity(RECORD_BATCH_SIZE),
            works_count: Vec::with_capacity(RECORD_BATCH_SIZE),
            cited_by_count: Vec::with_capacity(RECORD_BATCH_SIZE),
            h_index: Vec::with_capacity(RECORD_BATCH_SIZE),
            i10_index: Vec::with_capacity(RECORD_BATCH_SIZE),
            two_yr_mean_citedness: Vec::with_capacity(RECORD_BATCH_SIZE),
            affiliation_ids: Vec::with_capacity(RECORD_BATCH_SIZE),
            last_known_institution_ids: Vec::with_capacity(RECORD_BATCH_SIZE),
            created_date: Vec::with_capacity(RECORD_BATCH_SIZE),
            updated_date: Vec::with_capacity(RECORD_BATCH_SIZE),
        }
    }
}

impl Default for AuthorAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for AuthorAccumulator {
    type Row = AuthorRow;

    fn push(&mut self, row: AuthorRow) {
        let id = row.short_id().to_string();
        let orcid = row.orcid();
        let scopus = row.scopus();
        let h_index = row.h_index();
        let i10_index = row.i10_index();
        let two_yr_mean_citedness = row.two_yr_mean_citedness();
        let affiliation_ids = row.affiliation_ids();
        let last_known_institution_ids = row.last_known_institution_ids();

        self.id.push(id);
        self.orcid.push(orcid);
        self.scopus.push(scopus);
        self.display_name.push(row.display_name);
        self.display_name_alternatives
            .push(if row.display_name_alternatives.is_empty() {
                None
            } else {
                Some(
                    row.display_name_alternatives
                        .into_iter()
                        .map(Some)
                        .collect(),
                )
            });
        self.works_count.push(row.works_count);
        self.cited_by_count.push(row.cited_by_count);
        self.h_index.push(h_index);
        self.i10_index.push(i10_index);
        self.two_yr_mean_citedness.push(two_yr_mean_citedness);
        self.affiliation_ids.push(if affiliation_ids.is_empty() {
            None
        } else {
            Some(affiliation_ids)
        });
        self.last_known_institution_ids
            .push(if last_known_institution_ids.is_empty() {
                None
            } else {
                Some(last_known_institution_ids)
            });
        self.created_date.push(row.created_date);
        self.updated_date.push(row.updated_date);
    }

    fn len(&self) -> usize {
        self.id.len()
    }

    fn take_batch(&mut self) -> RecordBatch {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(std::mem::take(&mut self.id))),
            Arc::new(StringArray::from(std::mem::take(&mut self.orcid))),
            Arc::new(StringArray::from(std::mem::take(&mut self.scopus))),
            Arc::new(StringArray::from(std::mem::take(&mut self.display_name))),
            build_list_string_array(std::mem::take(&mut self.display_name_alternatives)),
            Arc::new(Int32Array::from(std::mem::take(&mut self.works_count))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.cited_by_count))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.h_index))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.i10_index))),
            Arc::new(Float64Array::from(std::mem::take(
                &mut self.two_yr_mean_citedness,
            ))),
            build_list_string_array(std::mem::take(&mut self.affiliation_ids)),
            build_list_string_array(std::mem::take(&mut self.last_known_institution_ids)),
            Arc::new(StringArray::from(std::mem::take(&mut self.created_date))),
            Arc::new(StringArray::from(std::mem::take(&mut self.updated_date))),
        ];
        RecordBatch::try_new(self.schema.clone(), arrays).expect("authors schema mismatch")
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

    const SAMPLE_AUTHOR: &str = r#"{
        "id": "https://openalex.org/A5023888391",
        "ids": {"orcid": "https://orcid.org/0000-0001-2345-6789", "scopus": "12345678"},
        "display_name": "John Doe",
        "display_name_alternatives": ["J. Doe", "John D."],
        "works_count": 150,
        "cited_by_count": 5000,
        "summary_stats": {"h_index": 25, "i10_index": 50, "2yr_mean_citedness": 3.5},
        "affiliations": [
            {"institution": {"id": "https://openalex.org/I27837315"}}
        ],
        "last_known_institutions": [
            {"id": "https://openalex.org/I27837315"}
        ],
        "created_date": "2020-01-01",
        "updated_date": "2025-01-15"
    }"#;

    #[test]
    fn parse_author_row() {
        let row: AuthorRow = serde_json::from_str(SAMPLE_AUTHOR).unwrap();
        assert_eq!(row.short_id(), "A5023888391");
        assert_eq!(row.display_name, Some("John Doe".to_string()));
        assert_eq!(row.works_count, 150);
        assert_eq!(row.cited_by_count, 5000);
    }

    #[test]
    fn author_ids() {
        let row: AuthorRow = serde_json::from_str(SAMPLE_AUTHOR).unwrap();
        assert_eq!(
            row.orcid(),
            Some("https://orcid.org/0000-0001-2345-6789".to_string())
        );
        assert_eq!(row.scopus(), Some("12345678".to_string()));
    }

    #[test]
    fn author_metrics() {
        let row: AuthorRow = serde_json::from_str(SAMPLE_AUTHOR).unwrap();
        assert_eq!(row.h_index(), Some(25));
        assert_eq!(row.i10_index(), Some(50));
        assert!((row.two_yr_mean_citedness().unwrap() - 3.5).abs() < f64::EPSILON);
    }

    #[test]
    fn author_affiliations() {
        let row: AuthorRow = serde_json::from_str(SAMPLE_AUTHOR).unwrap();
        let ids = row.affiliation_ids();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], Some("I27837315".to_string()));
    }

    #[test]
    fn author_last_known_institutions() {
        let row: AuthorRow = serde_json::from_str(SAMPLE_AUTHOR).unwrap();
        let ids = row.last_known_institution_ids();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], Some("I27837315".to_string()));
    }

    #[test]
    fn accumulator_batch() {
        let mut acc = AuthorAccumulator::new();
        let row: AuthorRow = serde_json::from_str(SAMPLE_AUTHOR).unwrap();
        acc.push(row);
        assert_eq!(acc.len(), 1);

        let batch = acc.take_batch();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(acc.len(), 0);
    }

    #[test]
    fn minimal_author() {
        let json = r#"{"id": "https://openalex.org/A1"}"#;
        let row: AuthorRow = serde_json::from_str(json).unwrap();
        assert_eq!(row.short_id(), "A1");
        assert!(row.display_name.is_none());
        assert!(row.orcid().is_none());
    }
}

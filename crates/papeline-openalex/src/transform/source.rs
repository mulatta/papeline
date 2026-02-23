//! Source entity transformation: JSON → Arrow RecordBatch

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

/// OpenAlex Source JSON structure (journals, repositories, conferences)
#[derive(Debug, Deserialize)]
pub struct SourceRow {
    #[serde(default)]
    pub id: String,

    #[serde(default)]
    pub issn_l: Option<String>,

    #[serde(default)]
    pub issn: Vec<String>,

    #[serde(default)]
    pub display_name: Option<String>,

    #[serde(default)]
    pub alternate_titles: Vec<String>,

    #[serde(default)]
    pub abbreviated_title: Option<String>,

    #[serde(rename = "type", default)]
    pub source_type: Option<String>,

    #[serde(default)]
    pub is_oa: bool,

    #[serde(default)]
    pub is_in_doaj: bool,

    #[serde(default)]
    pub host_organization: Option<String>,

    #[serde(default)]
    pub host_organization_name: Option<String>,

    #[serde(default)]
    pub works_count: i32,

    #[serde(default)]
    pub cited_by_count: i32,

    #[serde(default)]
    pub summary_stats: Option<SummaryStats>,

    #[serde(default)]
    pub apc_usd: Option<i32>,

    #[serde(default)]
    pub homepage_url: Option<String>,

    #[serde(default)]
    pub created_date: Option<String>,

    #[serde(default)]
    pub updated_date: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct SummaryStats {
    #[serde(default)]
    pub h_index: Option<i32>,
    #[serde(rename = "2yr_mean_citedness", default)]
    pub two_yr_mean_citedness: Option<f64>,
}

impl SourceRow {
    pub fn short_id(&self) -> &str {
        self.id
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or(&self.id)
    }

    pub fn host_organization_id(&self) -> Option<String> {
        self.host_organization.as_ref().map(|s| extract_short_id(s))
    }

    pub fn h_index(&self) -> Option<i32> {
        self.summary_stats.as_ref().and_then(|s| s.h_index)
    }

    pub fn two_yr_mean_citedness(&self) -> Option<f64> {
        self.summary_stats
            .as_ref()
            .and_then(|s| s.two_yr_mean_citedness)
    }
}

// === Accumulator ===

pub struct SourceAccumulator {
    schema: Arc<Schema>,
    id: Vec<String>,
    issn_l: Vec<Option<String>>,
    issn: Vec<Option<Vec<Option<String>>>>,
    display_name: Vec<Option<String>>,
    alternate_titles: Vec<Option<Vec<Option<String>>>>,
    abbreviated_title: Vec<Option<String>>,
    source_type: Vec<Option<String>>,
    is_oa: Vec<bool>,
    is_in_doaj: Vec<bool>,
    host_organization_id: Vec<Option<String>>,
    host_organization_name: Vec<Option<String>>,
    works_count: Vec<i32>,
    cited_by_count: Vec<i32>,
    h_index: Vec<Option<i32>>,
    two_yr_mean_citedness: Vec<Option<f64>>,
    apc_usd: Vec<Option<i32>>,
    homepage_url: Vec<Option<String>>,
    created_date: Vec<Option<String>>,
    updated_date: Vec<Option<String>>,
}

impl SourceAccumulator {
    pub fn new() -> Self {
        Self {
            schema: schema::sources().clone(),
            id: Vec::with_capacity(RECORD_BATCH_SIZE),
            issn_l: Vec::with_capacity(RECORD_BATCH_SIZE),
            issn: Vec::with_capacity(RECORD_BATCH_SIZE),
            display_name: Vec::with_capacity(RECORD_BATCH_SIZE),
            alternate_titles: Vec::with_capacity(RECORD_BATCH_SIZE),
            abbreviated_title: Vec::with_capacity(RECORD_BATCH_SIZE),
            source_type: Vec::with_capacity(RECORD_BATCH_SIZE),
            is_oa: Vec::with_capacity(RECORD_BATCH_SIZE),
            is_in_doaj: Vec::with_capacity(RECORD_BATCH_SIZE),
            host_organization_id: Vec::with_capacity(RECORD_BATCH_SIZE),
            host_organization_name: Vec::with_capacity(RECORD_BATCH_SIZE),
            works_count: Vec::with_capacity(RECORD_BATCH_SIZE),
            cited_by_count: Vec::with_capacity(RECORD_BATCH_SIZE),
            h_index: Vec::with_capacity(RECORD_BATCH_SIZE),
            two_yr_mean_citedness: Vec::with_capacity(RECORD_BATCH_SIZE),
            apc_usd: Vec::with_capacity(RECORD_BATCH_SIZE),
            homepage_url: Vec::with_capacity(RECORD_BATCH_SIZE),
            created_date: Vec::with_capacity(RECORD_BATCH_SIZE),
            updated_date: Vec::with_capacity(RECORD_BATCH_SIZE),
        }
    }
}

impl Default for SourceAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for SourceAccumulator {
    type Row = SourceRow;

    fn push(&mut self, row: SourceRow) {
        let id = row.short_id().to_string();
        let host_organization_id = row.host_organization_id();
        let h_index = row.h_index();
        let two_yr_mean_citedness = row.two_yr_mean_citedness();

        self.id.push(id);
        self.issn_l.push(row.issn_l);
        self.issn.push(if row.issn.is_empty() {
            None
        } else {
            Some(row.issn.into_iter().map(Some).collect())
        });
        self.display_name.push(row.display_name);
        self.alternate_titles
            .push(if row.alternate_titles.is_empty() {
                None
            } else {
                Some(row.alternate_titles.into_iter().map(Some).collect())
            });
        self.abbreviated_title.push(row.abbreviated_title);
        self.source_type.push(row.source_type);
        self.is_oa.push(row.is_oa);
        self.is_in_doaj.push(row.is_in_doaj);
        self.host_organization_id.push(host_organization_id);
        self.host_organization_name.push(row.host_organization_name);
        self.works_count.push(row.works_count);
        self.cited_by_count.push(row.cited_by_count);
        self.h_index.push(h_index);
        self.two_yr_mean_citedness.push(two_yr_mean_citedness);
        self.apc_usd.push(row.apc_usd);
        self.homepage_url.push(row.homepage_url);
        self.created_date.push(row.created_date);
        self.updated_date.push(row.updated_date);
    }

    fn len(&self) -> usize {
        self.id.len()
    }

    fn take_batch(&mut self) -> Result<RecordBatch, arrow::error::ArrowError> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(std::mem::take(&mut self.id))),
            Arc::new(StringArray::from(std::mem::take(&mut self.issn_l))),
            build_list_string_array(std::mem::take(&mut self.issn)),
            Arc::new(StringArray::from(std::mem::take(&mut self.display_name))),
            build_list_string_array(std::mem::take(&mut self.alternate_titles)),
            Arc::new(StringArray::from(std::mem::take(
                &mut self.abbreviated_title,
            ))),
            Arc::new(StringArray::from(std::mem::take(&mut self.source_type))),
            Arc::new(BooleanArray::from(std::mem::take(&mut self.is_oa))),
            Arc::new(BooleanArray::from(std::mem::take(&mut self.is_in_doaj))),
            Arc::new(StringArray::from(std::mem::take(
                &mut self.host_organization_id,
            ))),
            Arc::new(StringArray::from(std::mem::take(
                &mut self.host_organization_name,
            ))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.works_count))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.cited_by_count))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.h_index))),
            Arc::new(Float64Array::from(std::mem::take(
                &mut self.two_yr_mean_citedness,
            ))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.apc_usd))),
            Arc::new(StringArray::from(std::mem::take(&mut self.homepage_url))),
            Arc::new(StringArray::from(std::mem::take(&mut self.created_date))),
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

    const SAMPLE_SOURCE: &str = r#"{
        "id": "https://openalex.org/S1983995261",
        "issn_l": "0028-0836",
        "issn": ["0028-0836", "1476-4687"],
        "display_name": "Nature",
        "alternate_titles": ["Nature (London)"],
        "abbreviated_title": "Nature",
        "type": "journal",
        "is_oa": false,
        "is_in_doaj": false,
        "host_organization": "https://openalex.org/P4310319965",
        "host_organization_name": "Springer Nature",
        "works_count": 500000,
        "cited_by_count": 30000000,
        "summary_stats": {"h_index": 1500, "2yr_mean_citedness": 15.5},
        "apc_usd": 11390,
        "homepage_url": "https://www.nature.com",
        "created_date": "2020-01-01",
        "updated_date": "2025-01-15"
    }"#;

    #[test]
    fn parse_source_row() {
        let row: SourceRow = serde_json::from_str(SAMPLE_SOURCE).unwrap();
        assert_eq!(row.short_id(), "S1983995261");
        assert_eq!(row.display_name, Some("Nature".to_string()));
        assert_eq!(row.source_type, Some("journal".to_string()));
    }

    #[test]
    fn source_issn() {
        let row: SourceRow = serde_json::from_str(SAMPLE_SOURCE).unwrap();
        assert_eq!(row.issn_l, Some("0028-0836".to_string()));
        assert_eq!(row.issn.len(), 2);
    }

    #[test]
    fn source_host_organization() {
        let row: SourceRow = serde_json::from_str(SAMPLE_SOURCE).unwrap();
        assert_eq!(row.host_organization_id(), Some("P4310319965".to_string()));
        assert_eq!(
            row.host_organization_name,
            Some("Springer Nature".to_string())
        );
    }

    #[test]
    fn source_metrics() {
        let row: SourceRow = serde_json::from_str(SAMPLE_SOURCE).unwrap();
        assert_eq!(row.h_index(), Some(1500));
        assert!((row.two_yr_mean_citedness().unwrap() - 15.5).abs() < f64::EPSILON);
    }

    #[test]
    fn source_oa_status() {
        let row: SourceRow = serde_json::from_str(SAMPLE_SOURCE).unwrap();
        assert!(!row.is_oa);
        assert!(!row.is_in_doaj);
    }

    #[test]
    fn accumulator_batch() {
        let mut acc = SourceAccumulator::new();
        let row: SourceRow = serde_json::from_str(SAMPLE_SOURCE).unwrap();
        acc.push(row);
        assert_eq!(acc.len(), 1);

        let batch = acc.take_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(acc.len(), 0);
    }

    #[test]
    fn minimal_source() {
        let json = r#"{"id": "https://openalex.org/S1"}"#;
        let row: SourceRow = serde_json::from_str(json).unwrap();
        assert_eq!(row.short_id(), "S1");
        assert!(row.display_name.is_none());
    }
}

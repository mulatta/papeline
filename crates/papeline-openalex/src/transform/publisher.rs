//! Publisher entity transformation: JSON → Arrow RecordBatch

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

/// OpenAlex Publisher JSON structure
#[derive(Debug, Deserialize)]
pub struct PublisherRow {
    #[serde(default)]
    pub id: String,

    #[serde(default)]
    pub ids: Option<PublisherIds>,

    #[serde(default)]
    pub display_name: Option<String>,

    #[serde(default)]
    pub alternate_titles: Vec<String>,

    #[serde(default)]
    pub country_codes: Vec<String>,

    #[serde(default)]
    pub hierarchy_level: Option<i32>,

    #[serde(default)]
    pub parent_publisher: Option<String>,

    #[serde(default)]
    pub lineage: Vec<String>,

    #[serde(default)]
    pub works_count: i32,

    #[serde(default)]
    pub cited_by_count: i32,

    #[serde(default)]
    pub summary_stats: Option<SummaryStats>,

    #[serde(default)]
    pub image_url: Option<String>,

    #[serde(default)]
    pub created_date: Option<String>,

    #[serde(default)]
    pub updated_date: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct PublisherIds {
    #[serde(default)]
    pub ror: Option<String>,
    #[serde(default)]
    pub wikidata: Option<String>,
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

impl PublisherRow {
    pub fn short_id(&self) -> &str {
        self.id
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or(&self.id)
    }

    pub fn ror(&self) -> Option<String> {
        self.ids.as_ref().and_then(|ids| ids.ror.clone())
    }

    pub fn wikidata(&self) -> Option<String> {
        self.ids.as_ref().and_then(|ids| ids.wikidata.clone())
    }

    pub fn parent_publisher_id(&self) -> Option<String> {
        self.parent_publisher.as_ref().map(|s| extract_short_id(s))
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

    pub fn lineage_ids(&self) -> Vec<Option<String>> {
        self.lineage
            .iter()
            .map(|url| Some(extract_short_id(url)))
            .collect()
    }
}

// === Accumulator ===

pub struct PublisherAccumulator {
    schema: Arc<Schema>,
    id: Vec<String>,
    ror: Vec<Option<String>>,
    wikidata: Vec<Option<String>>,
    display_name: Vec<Option<String>>,
    alternate_titles: Vec<Option<Vec<Option<String>>>>,
    country_codes: Vec<Option<Vec<Option<String>>>>,
    hierarchy_level: Vec<Option<i32>>,
    parent_publisher_id: Vec<Option<String>>,
    lineage: Vec<Option<Vec<Option<String>>>>,
    works_count: Vec<i32>,
    cited_by_count: Vec<i32>,
    h_index: Vec<Option<i32>>,
    i10_index: Vec<Option<i32>>,
    two_yr_mean_citedness: Vec<Option<f64>>,
    image_url: Vec<Option<String>>,
    created_date: Vec<Option<String>>,
    updated_date: Vec<Option<String>>,
}

impl PublisherAccumulator {
    pub fn new() -> Self {
        Self {
            schema: schema::publishers().clone(),
            id: Vec::with_capacity(RECORD_BATCH_SIZE),
            ror: Vec::with_capacity(RECORD_BATCH_SIZE),
            wikidata: Vec::with_capacity(RECORD_BATCH_SIZE),
            display_name: Vec::with_capacity(RECORD_BATCH_SIZE),
            alternate_titles: Vec::with_capacity(RECORD_BATCH_SIZE),
            country_codes: Vec::with_capacity(RECORD_BATCH_SIZE),
            hierarchy_level: Vec::with_capacity(RECORD_BATCH_SIZE),
            parent_publisher_id: Vec::with_capacity(RECORD_BATCH_SIZE),
            lineage: Vec::with_capacity(RECORD_BATCH_SIZE),
            works_count: Vec::with_capacity(RECORD_BATCH_SIZE),
            cited_by_count: Vec::with_capacity(RECORD_BATCH_SIZE),
            h_index: Vec::with_capacity(RECORD_BATCH_SIZE),
            i10_index: Vec::with_capacity(RECORD_BATCH_SIZE),
            two_yr_mean_citedness: Vec::with_capacity(RECORD_BATCH_SIZE),
            image_url: Vec::with_capacity(RECORD_BATCH_SIZE),
            created_date: Vec::with_capacity(RECORD_BATCH_SIZE),
            updated_date: Vec::with_capacity(RECORD_BATCH_SIZE),
        }
    }
}

impl Default for PublisherAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for PublisherAccumulator {
    type Row = PublisherRow;

    fn push(&mut self, row: PublisherRow) {
        let id = row.short_id().to_string();
        let ror = row.ror();
        let wikidata = row.wikidata();
        let parent_publisher_id = row.parent_publisher_id();
        let h_index = row.h_index();
        let i10_index = row.i10_index();
        let two_yr_mean_citedness = row.two_yr_mean_citedness();
        let lineage_ids = row.lineage_ids();

        self.id.push(id);
        self.ror.push(ror);
        self.wikidata.push(wikidata);
        self.display_name.push(row.display_name);
        self.alternate_titles
            .push(if row.alternate_titles.is_empty() {
                None
            } else {
                Some(row.alternate_titles.into_iter().map(Some).collect())
            });
        self.country_codes.push(if row.country_codes.is_empty() {
            None
        } else {
            Some(row.country_codes.into_iter().map(Some).collect())
        });
        self.hierarchy_level.push(row.hierarchy_level);
        self.parent_publisher_id.push(parent_publisher_id);
        self.lineage.push(if lineage_ids.is_empty() {
            None
        } else {
            Some(lineage_ids)
        });
        self.works_count.push(row.works_count);
        self.cited_by_count.push(row.cited_by_count);
        self.h_index.push(h_index);
        self.i10_index.push(i10_index);
        self.two_yr_mean_citedness.push(two_yr_mean_citedness);
        self.image_url.push(row.image_url);
        self.created_date.push(row.created_date);
        self.updated_date.push(row.updated_date);
    }

    fn len(&self) -> usize {
        self.id.len()
    }

    fn take_batch(&mut self) -> RecordBatch {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(std::mem::take(&mut self.id))),
            Arc::new(StringArray::from(std::mem::take(&mut self.ror))),
            Arc::new(StringArray::from(std::mem::take(&mut self.wikidata))),
            Arc::new(StringArray::from(std::mem::take(&mut self.display_name))),
            build_list_string_array(std::mem::take(&mut self.alternate_titles)),
            build_list_string_array(std::mem::take(&mut self.country_codes)),
            Arc::new(Int32Array::from(std::mem::take(&mut self.hierarchy_level))),
            Arc::new(StringArray::from(std::mem::take(
                &mut self.parent_publisher_id,
            ))),
            build_list_string_array(std::mem::take(&mut self.lineage)),
            Arc::new(Int32Array::from(std::mem::take(&mut self.works_count))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.cited_by_count))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.h_index))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.i10_index))),
            Arc::new(Float64Array::from(std::mem::take(
                &mut self.two_yr_mean_citedness,
            ))),
            Arc::new(StringArray::from(std::mem::take(&mut self.image_url))),
            Arc::new(StringArray::from(std::mem::take(&mut self.created_date))),
            Arc::new(StringArray::from(std::mem::take(&mut self.updated_date))),
        ];
        RecordBatch::try_new(self.schema.clone(), arrays).expect("publishers schema mismatch")
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

    const SAMPLE_PUBLISHER: &str = r#"{
        "id": "https://openalex.org/P4310319965",
        "ids": {
            "ror": "https://ror.org/04p3a9p15",
            "wikidata": "Q2014423"
        },
        "display_name": "Springer Nature",
        "alternate_titles": ["Springer", "Nature Publishing Group"],
        "country_codes": ["DE", "GB"],
        "hierarchy_level": 0,
        "lineage": ["https://openalex.org/P4310319965"],
        "works_count": 3000000,
        "cited_by_count": 100000000,
        "summary_stats": {"h_index": 2000, "i10_index": 50000, "2yr_mean_citedness": 10.5},
        "image_url": "https://upload.wikimedia.org/wikipedia/commons/thumb/3/37/Springer_Nature.svg/200px-Springer_Nature.svg.png",
        "created_date": "2020-01-01",
        "updated_date": "2025-01-15"
    }"#;

    #[test]
    fn parse_publisher_row() {
        let row: PublisherRow = serde_json::from_str(SAMPLE_PUBLISHER).unwrap();
        assert_eq!(row.short_id(), "P4310319965");
        assert_eq!(row.display_name, Some("Springer Nature".to_string()));
        assert_eq!(row.hierarchy_level, Some(0));
    }

    #[test]
    fn publisher_ids() {
        let row: PublisherRow = serde_json::from_str(SAMPLE_PUBLISHER).unwrap();
        assert_eq!(row.ror(), Some("https://ror.org/04p3a9p15".to_string()));
        assert_eq!(row.wikidata(), Some("Q2014423".to_string()));
    }

    #[test]
    fn publisher_country_codes() {
        let row: PublisherRow = serde_json::from_str(SAMPLE_PUBLISHER).unwrap();
        assert_eq!(row.country_codes.len(), 2);
        assert!(row.country_codes.contains(&"DE".to_string()));
        assert!(row.country_codes.contains(&"GB".to_string()));
    }

    #[test]
    fn publisher_metrics() {
        let row: PublisherRow = serde_json::from_str(SAMPLE_PUBLISHER).unwrap();
        assert_eq!(row.h_index(), Some(2000));
        assert_eq!(row.i10_index(), Some(50000));
        assert!((row.two_yr_mean_citedness().unwrap() - 10.5).abs() < f64::EPSILON);
    }

    #[test]
    fn publisher_lineage() {
        let row: PublisherRow = serde_json::from_str(SAMPLE_PUBLISHER).unwrap();
        let lineage = row.lineage_ids();
        assert_eq!(lineage.len(), 1);
        assert_eq!(lineage[0], Some("P4310319965".to_string()));
    }

    #[test]
    fn publisher_with_parent() {
        let json = r#"{
            "id": "https://openalex.org/P123",
            "parent_publisher": "https://openalex.org/P4310319965"
        }"#;
        let row: PublisherRow = serde_json::from_str(json).unwrap();
        assert_eq!(row.parent_publisher_id(), Some("P4310319965".to_string()));
    }

    #[test]
    fn accumulator_batch() {
        let mut acc = PublisherAccumulator::new();
        let row: PublisherRow = serde_json::from_str(SAMPLE_PUBLISHER).unwrap();
        acc.push(row);
        assert_eq!(acc.len(), 1);

        let batch = acc.take_batch();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(acc.len(), 0);
    }

    #[test]
    fn minimal_publisher() {
        let json = r#"{"id": "https://openalex.org/P1"}"#;
        let row: PublisherRow = serde_json::from_str(json).unwrap();
        assert_eq!(row.short_id(), "P1");
        assert!(row.display_name.is_none());
    }
}

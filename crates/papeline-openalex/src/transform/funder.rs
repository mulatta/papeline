//! Funder entity transformation: JSON → Arrow RecordBatch

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::Schema;
use serde::Deserialize;

use super::{Accumulator, RECORD_BATCH_SIZE};
use crate::schema;

// === Row struct (deserialized from JSON) ===

/// OpenAlex Funder JSON structure
#[derive(Debug, Deserialize)]
pub struct FunderRow {
    #[serde(default)]
    pub id: String,

    #[serde(default)]
    pub ids: Option<FunderIds>,

    #[serde(default)]
    pub display_name: Option<String>,

    #[serde(default)]
    pub alternate_titles: Vec<String>,

    #[serde(default)]
    pub country_code: Option<String>,

    #[serde(default)]
    pub works_count: i32,

    #[serde(default)]
    pub cited_by_count: i32,

    #[serde(default)]
    pub grants_count: i32,

    #[serde(default)]
    pub summary_stats: Option<SummaryStats>,

    #[serde(default)]
    pub homepage_url: Option<String>,

    #[serde(default)]
    pub image_url: Option<String>,

    #[serde(default)]
    pub created_date: Option<String>,

    #[serde(default)]
    pub updated_date: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct FunderIds {
    #[serde(default)]
    pub ror: Option<String>,
    #[serde(default)]
    pub crossref: Option<String>,
    #[serde(default)]
    pub wikidata: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct SummaryStats {
    #[serde(default)]
    pub h_index: Option<i32>,
    #[serde(rename = "2yr_mean_citedness", default)]
    pub two_yr_mean_citedness: Option<f64>,
}

impl FunderRow {
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

    pub fn crossref(&self) -> Option<String> {
        self.ids.as_ref().and_then(|ids| ids.crossref.clone())
    }

    pub fn wikidata(&self) -> Option<String> {
        self.ids.as_ref().and_then(|ids| ids.wikidata.clone())
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

pub struct FunderAccumulator {
    schema: Arc<Schema>,
    id: Vec<String>,
    ror: Vec<Option<String>>,
    crossref: Vec<Option<String>>,
    wikidata: Vec<Option<String>>,
    display_name: Vec<Option<String>>,
    alternate_titles: Vec<Option<Vec<Option<String>>>>,
    country_code: Vec<Option<String>>,
    works_count: Vec<i32>,
    cited_by_count: Vec<i32>,
    grants_count: Vec<i32>,
    h_index: Vec<Option<i32>>,
    two_yr_mean_citedness: Vec<Option<f64>>,
    homepage_url: Vec<Option<String>>,
    image_url: Vec<Option<String>>,
    created_date: Vec<Option<String>>,
    updated_date: Vec<Option<String>>,
}

impl FunderAccumulator {
    pub fn new() -> Self {
        Self {
            schema: schema::funders().clone(),
            id: Vec::with_capacity(RECORD_BATCH_SIZE),
            ror: Vec::with_capacity(RECORD_BATCH_SIZE),
            crossref: Vec::with_capacity(RECORD_BATCH_SIZE),
            wikidata: Vec::with_capacity(RECORD_BATCH_SIZE),
            display_name: Vec::with_capacity(RECORD_BATCH_SIZE),
            alternate_titles: Vec::with_capacity(RECORD_BATCH_SIZE),
            country_code: Vec::with_capacity(RECORD_BATCH_SIZE),
            works_count: Vec::with_capacity(RECORD_BATCH_SIZE),
            cited_by_count: Vec::with_capacity(RECORD_BATCH_SIZE),
            grants_count: Vec::with_capacity(RECORD_BATCH_SIZE),
            h_index: Vec::with_capacity(RECORD_BATCH_SIZE),
            two_yr_mean_citedness: Vec::with_capacity(RECORD_BATCH_SIZE),
            homepage_url: Vec::with_capacity(RECORD_BATCH_SIZE),
            image_url: Vec::with_capacity(RECORD_BATCH_SIZE),
            created_date: Vec::with_capacity(RECORD_BATCH_SIZE),
            updated_date: Vec::with_capacity(RECORD_BATCH_SIZE),
        }
    }
}

impl Default for FunderAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for FunderAccumulator {
    type Row = FunderRow;

    fn push(&mut self, row: FunderRow) {
        let id = row.short_id().to_string();
        let ror = row.ror();
        let crossref = row.crossref();
        let wikidata = row.wikidata();
        let h_index = row.h_index();
        let two_yr_mean_citedness = row.two_yr_mean_citedness();

        self.id.push(id);
        self.ror.push(ror);
        self.crossref.push(crossref);
        self.wikidata.push(wikidata);
        self.display_name.push(row.display_name);
        self.alternate_titles
            .push(if row.alternate_titles.is_empty() {
                None
            } else {
                Some(row.alternate_titles.into_iter().map(Some).collect())
            });
        self.country_code.push(row.country_code);
        self.works_count.push(row.works_count);
        self.cited_by_count.push(row.cited_by_count);
        self.grants_count.push(row.grants_count);
        self.h_index.push(h_index);
        self.two_yr_mean_citedness.push(two_yr_mean_citedness);
        self.homepage_url.push(row.homepage_url);
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
            Arc::new(StringArray::from(std::mem::take(&mut self.crossref))),
            Arc::new(StringArray::from(std::mem::take(&mut self.wikidata))),
            Arc::new(StringArray::from(std::mem::take(&mut self.display_name))),
            build_list_string_array(std::mem::take(&mut self.alternate_titles)),
            Arc::new(StringArray::from(std::mem::take(&mut self.country_code))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.works_count))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.cited_by_count))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.grants_count))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.h_index))),
            Arc::new(Float64Array::from(std::mem::take(
                &mut self.two_yr_mean_citedness,
            ))),
            Arc::new(StringArray::from(std::mem::take(&mut self.homepage_url))),
            Arc::new(StringArray::from(std::mem::take(&mut self.image_url))),
            Arc::new(StringArray::from(std::mem::take(&mut self.created_date))),
            Arc::new(StringArray::from(std::mem::take(&mut self.updated_date))),
        ];
        RecordBatch::try_new(self.schema.clone(), arrays).expect("funders schema mismatch")
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

    const SAMPLE_FUNDER: &str = r#"{
        "id": "https://openalex.org/F4320332161",
        "ids": {
            "ror": "https://ror.org/021nxhr62",
            "crossref": "100000001",
            "wikidata": "Q49060"
        },
        "display_name": "National Science Foundation",
        "alternate_titles": ["NSF", "US National Science Foundation"],
        "country_code": "US",
        "works_count": 1000000,
        "cited_by_count": 50000000,
        "grants_count": 500000,
        "summary_stats": {"h_index": 1000, "2yr_mean_citedness": 8.5},
        "homepage_url": "https://www.nsf.gov",
        "image_url": "https://upload.wikimedia.org/wikipedia/commons/thumb/8/87/NSF_logo.svg/200px-NSF_logo.svg.png",
        "created_date": "2020-01-01",
        "updated_date": "2025-01-15"
    }"#;

    #[test]
    fn parse_funder_row() {
        let row: FunderRow = serde_json::from_str(SAMPLE_FUNDER).unwrap();
        assert_eq!(row.short_id(), "F4320332161");
        assert_eq!(
            row.display_name,
            Some("National Science Foundation".to_string())
        );
    }

    #[test]
    fn funder_ids() {
        let row: FunderRow = serde_json::from_str(SAMPLE_FUNDER).unwrap();
        assert_eq!(row.ror(), Some("https://ror.org/021nxhr62".to_string()));
        assert_eq!(row.crossref(), Some("100000001".to_string()));
        assert_eq!(row.wikidata(), Some("Q49060".to_string()));
    }

    #[test]
    fn funder_alternate_titles() {
        let row: FunderRow = serde_json::from_str(SAMPLE_FUNDER).unwrap();
        assert_eq!(row.alternate_titles.len(), 2);
        assert!(row.alternate_titles.contains(&"NSF".to_string()));
    }

    #[test]
    fn funder_metrics() {
        let row: FunderRow = serde_json::from_str(SAMPLE_FUNDER).unwrap();
        assert_eq!(row.works_count, 1000000);
        assert_eq!(row.cited_by_count, 50000000);
        assert_eq!(row.grants_count, 500000);
        assert_eq!(row.h_index(), Some(1000));
        assert!((row.two_yr_mean_citedness().unwrap() - 8.5).abs() < f64::EPSILON);
    }

    #[test]
    fn accumulator_batch() {
        let mut acc = FunderAccumulator::new();
        let row: FunderRow = serde_json::from_str(SAMPLE_FUNDER).unwrap();
        acc.push(row);
        assert_eq!(acc.len(), 1);

        let batch = acc.take_batch();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(acc.len(), 0);
    }

    #[test]
    fn minimal_funder() {
        let json = r#"{"id": "https://openalex.org/F1"}"#;
        let row: FunderRow = serde_json::from_str(json).unwrap();
        assert_eq!(row.short_id(), "F1");
        assert!(row.display_name.is_none());
    }
}

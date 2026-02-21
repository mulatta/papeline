//! Institution entity transformation: JSON → Arrow RecordBatch

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

/// OpenAlex Institution JSON structure
#[derive(Debug, Deserialize)]
pub struct InstitutionRow {
    #[serde(default)]
    pub id: String,

    #[serde(default)]
    pub ids: Option<InstitutionIds>,

    #[serde(default)]
    pub display_name: Option<String>,

    #[serde(default)]
    pub display_name_acronyms: Vec<String>,

    #[serde(default)]
    pub display_name_alternatives: Vec<String>,

    #[serde(rename = "type", default)]
    pub institution_type: Option<String>,

    #[serde(default)]
    pub country_code: Option<String>,

    #[serde(default)]
    pub geo: Option<Geo>,

    #[serde(default)]
    pub works_count: i32,

    #[serde(default)]
    pub cited_by_count: i32,

    #[serde(default)]
    pub summary_stats: Option<SummaryStats>,

    #[serde(default)]
    pub lineage: Vec<String>,

    #[serde(default)]
    pub is_super_system: bool,

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
pub struct InstitutionIds {
    #[serde(default)]
    pub ror: Option<String>,
    #[serde(default)]
    pub grid: Option<String>,
    #[serde(default)]
    pub mag: Option<String>,
    #[serde(default)]
    pub wikidata: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct Geo {
    #[serde(default)]
    pub city: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub latitude: Option<f64>,
    #[serde(default)]
    pub longitude: Option<f64>,
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

impl InstitutionRow {
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

    pub fn grid(&self) -> Option<String> {
        self.ids.as_ref().and_then(|ids| ids.grid.clone())
    }

    pub fn mag(&self) -> Option<String> {
        self.ids.as_ref().and_then(|ids| ids.mag.clone())
    }

    pub fn wikidata(&self) -> Option<String> {
        self.ids.as_ref().and_then(|ids| ids.wikidata.clone())
    }

    pub fn city(&self) -> Option<String> {
        self.geo.as_ref().and_then(|g| g.city.clone())
    }

    pub fn region(&self) -> Option<String> {
        self.geo.as_ref().and_then(|g| g.region.clone())
    }

    pub fn latitude(&self) -> Option<f64> {
        self.geo.as_ref().and_then(|g| g.latitude)
    }

    pub fn longitude(&self) -> Option<f64> {
        self.geo.as_ref().and_then(|g| g.longitude)
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

pub struct InstitutionAccumulator {
    schema: Arc<Schema>,
    id: Vec<String>,
    ror: Vec<Option<String>>,
    grid: Vec<Option<String>>,
    mag: Vec<Option<String>>,
    wikidata: Vec<Option<String>>,
    display_name: Vec<Option<String>>,
    display_name_acronyms: Vec<Option<Vec<Option<String>>>>,
    display_name_alternatives: Vec<Option<Vec<Option<String>>>>,
    institution_type: Vec<Option<String>>,
    country_code: Vec<Option<String>>,
    city: Vec<Option<String>>,
    region: Vec<Option<String>>,
    latitude: Vec<Option<f64>>,
    longitude: Vec<Option<f64>>,
    works_count: Vec<i32>,
    cited_by_count: Vec<i32>,
    h_index: Vec<Option<i32>>,
    i10_index: Vec<Option<i32>>,
    two_yr_mean_citedness: Vec<Option<f64>>,
    lineage: Vec<Option<Vec<Option<String>>>>,
    is_super_system: Vec<bool>,
    homepage_url: Vec<Option<String>>,
    image_url: Vec<Option<String>>,
    created_date: Vec<Option<String>>,
    updated_date: Vec<Option<String>>,
}

impl InstitutionAccumulator {
    pub fn new() -> Self {
        Self {
            schema: schema::institutions().clone(),
            id: Vec::with_capacity(RECORD_BATCH_SIZE),
            ror: Vec::with_capacity(RECORD_BATCH_SIZE),
            grid: Vec::with_capacity(RECORD_BATCH_SIZE),
            mag: Vec::with_capacity(RECORD_BATCH_SIZE),
            wikidata: Vec::with_capacity(RECORD_BATCH_SIZE),
            display_name: Vec::with_capacity(RECORD_BATCH_SIZE),
            display_name_acronyms: Vec::with_capacity(RECORD_BATCH_SIZE),
            display_name_alternatives: Vec::with_capacity(RECORD_BATCH_SIZE),
            institution_type: Vec::with_capacity(RECORD_BATCH_SIZE),
            country_code: Vec::with_capacity(RECORD_BATCH_SIZE),
            city: Vec::with_capacity(RECORD_BATCH_SIZE),
            region: Vec::with_capacity(RECORD_BATCH_SIZE),
            latitude: Vec::with_capacity(RECORD_BATCH_SIZE),
            longitude: Vec::with_capacity(RECORD_BATCH_SIZE),
            works_count: Vec::with_capacity(RECORD_BATCH_SIZE),
            cited_by_count: Vec::with_capacity(RECORD_BATCH_SIZE),
            h_index: Vec::with_capacity(RECORD_BATCH_SIZE),
            i10_index: Vec::with_capacity(RECORD_BATCH_SIZE),
            two_yr_mean_citedness: Vec::with_capacity(RECORD_BATCH_SIZE),
            lineage: Vec::with_capacity(RECORD_BATCH_SIZE),
            is_super_system: Vec::with_capacity(RECORD_BATCH_SIZE),
            homepage_url: Vec::with_capacity(RECORD_BATCH_SIZE),
            image_url: Vec::with_capacity(RECORD_BATCH_SIZE),
            created_date: Vec::with_capacity(RECORD_BATCH_SIZE),
            updated_date: Vec::with_capacity(RECORD_BATCH_SIZE),
        }
    }
}

impl Default for InstitutionAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for InstitutionAccumulator {
    type Row = InstitutionRow;

    fn push(&mut self, row: InstitutionRow) {
        let id = row.short_id().to_string();
        let ror = row.ror();
        let grid = row.grid();
        let mag = row.mag();
        let wikidata = row.wikidata();
        let city = row.city();
        let region = row.region();
        let latitude = row.latitude();
        let longitude = row.longitude();
        let h_index = row.h_index();
        let i10_index = row.i10_index();
        let two_yr_mean_citedness = row.two_yr_mean_citedness();
        let lineage_ids = row.lineage_ids();

        self.id.push(id);
        self.ror.push(ror);
        self.grid.push(grid);
        self.mag.push(mag);
        self.wikidata.push(wikidata);
        self.display_name.push(row.display_name);
        self.display_name_acronyms
            .push(if row.display_name_acronyms.is_empty() {
                None
            } else {
                Some(row.display_name_acronyms.into_iter().map(Some).collect())
            });
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
        self.institution_type.push(row.institution_type);
        self.country_code.push(row.country_code);
        self.city.push(city);
        self.region.push(region);
        self.latitude.push(latitude);
        self.longitude.push(longitude);
        self.works_count.push(row.works_count);
        self.cited_by_count.push(row.cited_by_count);
        self.h_index.push(h_index);
        self.i10_index.push(i10_index);
        self.two_yr_mean_citedness.push(two_yr_mean_citedness);
        self.lineage.push(if lineage_ids.is_empty() {
            None
        } else {
            Some(lineage_ids)
        });
        self.is_super_system.push(row.is_super_system);
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
            Arc::new(StringArray::from(std::mem::take(&mut self.grid))),
            Arc::new(StringArray::from(std::mem::take(&mut self.mag))),
            Arc::new(StringArray::from(std::mem::take(&mut self.wikidata))),
            Arc::new(StringArray::from(std::mem::take(&mut self.display_name))),
            build_list_string_array(std::mem::take(&mut self.display_name_acronyms)),
            build_list_string_array(std::mem::take(&mut self.display_name_alternatives)),
            Arc::new(StringArray::from(std::mem::take(
                &mut self.institution_type,
            ))),
            Arc::new(StringArray::from(std::mem::take(&mut self.country_code))),
            Arc::new(StringArray::from(std::mem::take(&mut self.city))),
            Arc::new(StringArray::from(std::mem::take(&mut self.region))),
            Arc::new(Float64Array::from(std::mem::take(&mut self.latitude))),
            Arc::new(Float64Array::from(std::mem::take(&mut self.longitude))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.works_count))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.cited_by_count))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.h_index))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.i10_index))),
            Arc::new(Float64Array::from(std::mem::take(
                &mut self.two_yr_mean_citedness,
            ))),
            build_list_string_array(std::mem::take(&mut self.lineage)),
            Arc::new(BooleanArray::from(std::mem::take(
                &mut self.is_super_system,
            ))),
            Arc::new(StringArray::from(std::mem::take(&mut self.homepage_url))),
            Arc::new(StringArray::from(std::mem::take(&mut self.image_url))),
            Arc::new(StringArray::from(std::mem::take(&mut self.created_date))),
            Arc::new(StringArray::from(std::mem::take(&mut self.updated_date))),
        ];
        RecordBatch::try_new(self.schema.clone(), arrays).expect("institutions schema mismatch")
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

    const SAMPLE_INSTITUTION: &str = r#"{
        "id": "https://openalex.org/I27837315",
        "ids": {
            "ror": "https://ror.org/0190ak572",
            "grid": "grid.19006.3e",
            "mag": "27837315",
            "wikidata": "Q37156"
        },
        "display_name": "University of California, Los Angeles",
        "display_name_acronyms": ["UCLA"],
        "display_name_alternatives": ["UCLA", "UC Los Angeles"],
        "type": "education",
        "country_code": "US",
        "geo": {
            "city": "Los Angeles",
            "region": "California",
            "latitude": 34.0689,
            "longitude": -118.4452
        },
        "works_count": 500000,
        "cited_by_count": 20000000,
        "summary_stats": {"h_index": 500, "i10_index": 10000, "2yr_mean_citedness": 5.5},
        "lineage": ["https://openalex.org/I27837315"],
        "is_super_system": false,
        "homepage_url": "https://www.ucla.edu",
        "image_url": "https://upload.wikimedia.org/wikipedia/commons/thumb/6/6c/UCLA_Bruins_script.svg/200px-UCLA_Bruins_script.svg.png",
        "created_date": "2020-01-01",
        "updated_date": "2025-01-15"
    }"#;

    #[test]
    fn parse_institution_row() {
        let row: InstitutionRow = serde_json::from_str(SAMPLE_INSTITUTION).unwrap();
        assert_eq!(row.short_id(), "I27837315");
        assert_eq!(
            row.display_name,
            Some("University of California, Los Angeles".to_string())
        );
        assert_eq!(row.institution_type, Some("education".to_string()));
    }

    #[test]
    fn institution_ids() {
        let row: InstitutionRow = serde_json::from_str(SAMPLE_INSTITUTION).unwrap();
        assert_eq!(row.ror(), Some("https://ror.org/0190ak572".to_string()));
        assert_eq!(row.grid(), Some("grid.19006.3e".to_string()));
        assert_eq!(row.mag(), Some("27837315".to_string()));
    }

    #[test]
    fn institution_geo() {
        let row: InstitutionRow = serde_json::from_str(SAMPLE_INSTITUTION).unwrap();
        assert_eq!(row.city(), Some("Los Angeles".to_string()));
        assert_eq!(row.region(), Some("California".to_string()));
        assert!((row.latitude().unwrap() - 34.0689).abs() < 0.0001);
        assert!((row.longitude().unwrap() - (-118.4452)).abs() < 0.0001);
    }

    #[test]
    fn institution_metrics() {
        let row: InstitutionRow = serde_json::from_str(SAMPLE_INSTITUTION).unwrap();
        assert_eq!(row.h_index(), Some(500));
        assert_eq!(row.i10_index(), Some(10000));
        assert!((row.two_yr_mean_citedness().unwrap() - 5.5).abs() < f64::EPSILON);
    }

    #[test]
    fn institution_lineage() {
        let row: InstitutionRow = serde_json::from_str(SAMPLE_INSTITUTION).unwrap();
        let lineage = row.lineage_ids();
        assert_eq!(lineage.len(), 1);
        assert_eq!(lineage[0], Some("I27837315".to_string()));
    }

    #[test]
    fn accumulator_batch() {
        let mut acc = InstitutionAccumulator::new();
        let row: InstitutionRow = serde_json::from_str(SAMPLE_INSTITUTION).unwrap();
        acc.push(row);
        assert_eq!(acc.len(), 1);

        let batch = acc.take_batch();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(acc.len(), 0);
    }

    #[test]
    fn minimal_institution() {
        let json = r#"{"id": "https://openalex.org/I1"}"#;
        let row: InstitutionRow = serde_json::from_str(json).unwrap();
        assert_eq!(row.short_id(), "I1");
        assert!(row.display_name.is_none());
    }
}

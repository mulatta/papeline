//! JSON-to-Arrow record batch accumulators

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::Schema;
use serde::{Deserialize, Deserializer};

use crate::schema;

// === Null-handling deserializers ===

/// Deserialize null as empty string (for optional String fields)
fn null_to_empty<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(|opt| opt.unwrap_or_default())
}

/// Deserialize null as empty Vec (for optional Vec fields)
fn null_to_empty_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<Vec<T>>::deserialize(deserializer).map(|opt| opt.unwrap_or_default())
}

/// Generate deserializer that warns on null for required String fields
macro_rules! define_null_to_empty_warn {
    ($fn_name:ident, $field:literal) => {
        fn $fn_name<'de, D>(deserializer: D) -> Result<String, D::Error>
        where
            D: Deserializer<'de>,
        {
            match Option::<String>::deserialize(deserializer)? {
                Some(s) => Ok(s),
                None => {
                    log::debug!("required field '{}' was null, using empty string", $field);
                    Ok(String::new())
                }
            }
        }
    };
}

define_null_to_empty_warn!(null_to_empty_warn_title, "title");
define_null_to_empty_warn!(null_to_empty_warn_name, "name");
define_null_to_empty_warn!(null_to_empty_warn_category, "category");
define_null_to_empty_warn!(null_to_empty_warn_source, "source");

/// Deserialize null as empty Vec with warning (for required Vec fields)
fn null_to_empty_vec_warn_authors<'de, D>(deserializer: D) -> Result<Vec<AuthorEntry>, D::Error>
where
    D: Deserializer<'de>,
{
    match Option::<Vec<AuthorEntry>>::deserialize(deserializer)? {
        Some(v) => Ok(v),
        None => {
            log::debug!("required field 'authors' was null, using empty vec");
            Ok(Vec::new())
        }
    }
}

/// Nested list type for citation intents: List<List<Utf8>>
type CitationIntentsData = Vec<Option<Vec<Option<Vec<Option<String>>>>>>;

use papeline_core::DEFAULT_BATCH_SIZE as RECORD_BATCH_SIZE;

// === Typed row structs ===

#[derive(Debug, Deserialize)]
pub struct PaperRow {
    #[serde(default)]
    pub corpusid: i64,
    #[serde(default, deserialize_with = "null_to_empty_warn_title")]
    pub title: String, // required
    #[serde(default, deserialize_with = "null_to_empty")]
    pub url: String, // optional
    #[serde(default, deserialize_with = "null_to_empty")]
    pub venue: String, // optional
    #[serde(default)]
    pub publicationvenueid: Option<String>,
    #[serde(default)]
    pub year: Option<i32>,
    #[serde(default)]
    pub referencecount: i32,
    #[serde(default)]
    pub citationcount: i32,
    #[serde(default)]
    pub influentialcitationcount: i32,
    #[serde(default)]
    pub isopenaccess: bool,
    #[serde(default)]
    pub publicationdate: Option<String>,
    #[serde(default)]
    pub publicationtypes: Option<Vec<Option<String>>>,
    #[serde(default, deserialize_with = "null_to_empty_vec_warn_authors")]
    pub authors: Vec<AuthorEntry>, // required
    #[serde(default, deserialize_with = "null_to_empty_vec")]
    pub s2fieldsofstudy: Vec<FieldEntry>, // optional
    #[serde(default)]
    pub journal: Option<JournalEntry>,
    #[serde(default)]
    pub externalids: Option<ExternalIds>,
}

impl PaperRow {
    pub fn matches_domains(&self, domains: &[String]) -> bool {
        self.s2fieldsofstudy
            .iter()
            .any(|f| domains.contains(&f.category))
    }
}

#[derive(Debug, Deserialize)]
pub struct AuthorEntry {
    #[serde(rename = "authorId", default)]
    pub author_id: Option<String>, // required per API, but null is common
    #[serde(default, deserialize_with = "null_to_empty_warn_name")]
    pub name: String, // required
}

#[derive(Debug, Deserialize)]
pub struct FieldEntry {
    #[serde(default, deserialize_with = "null_to_empty_warn_category")]
    pub category: String, // required
    #[serde(default, deserialize_with = "null_to_empty_warn_source")]
    pub source: String, // required
}

#[derive(Debug, Deserialize)]
pub struct JournalEntry {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub pages: Option<String>,
    #[serde(default)]
    pub volume: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ExternalIds {
    #[serde(rename = "DOI", default)]
    pub doi: Option<String>,
    #[serde(rename = "PubMed", default)]
    pub pubmed: Option<String>,
    #[serde(rename = "ArXiv", default)]
    pub arxiv: Option<String>,
    #[serde(rename = "MAG", default)]
    pub mag: Option<String>,
    #[serde(rename = "ACL", default)]
    pub acl: Option<String>,
    #[serde(rename = "DBLP", default)]
    pub dblp: Option<String>,
    #[serde(rename = "PubMedCentral", default)]
    pub pubmedcentral: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CitationRow {
    #[serde(default)]
    pub citationid: i64,
    #[serde(default)]
    pub citingcorpusid: i64,
    #[serde(default)]
    pub citedcorpusid: i64,
    #[serde(default)]
    pub isinfluential: bool,
    #[serde(default)]
    pub contexts: Option<Vec<Option<String>>>,
    #[serde(default)]
    pub intents: Option<Vec<Option<Vec<Option<String>>>>>,
}

#[derive(Debug, Deserialize)]
pub struct AbstractRow {
    #[serde(default)]
    pub corpusid: i64,
    #[serde(rename = "abstract", default)]
    pub text: String,
}

#[derive(Debug, Deserialize)]
pub struct TldrRow {
    #[serde(default)]
    pub corpusid: i64,
    #[serde(default)]
    pub text: String,
}

/// Wrapper for embedding vector supporting both JSON string and array formats.
///
/// S2 serializes vectors as JSON strings (`"[0.15, -0.11, ...]"`), but some
/// records may use native JSON arrays. This deserializer handles both without
/// intermediate `serde_json::Value` allocation.
#[derive(Debug)]
pub struct EmbeddingVector(pub Vec<f32>);

impl<'de> serde::Deserialize<'de> for EmbeddingVector {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct VecVisitor;

        impl<'de> serde::de::Visitor<'de> for VecVisitor {
            type Value = EmbeddingVector;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a JSON string or array of floats")
            }

            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Self::Value, E> {
                serde_json::from_str::<Vec<f32>>(s)
                    .map(EmbeddingVector)
                    .map_err(serde::de::Error::custom)
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut v = Vec::with_capacity(seq.size_hint().unwrap_or(768));
                while let Some(val) = seq.next_element::<f32>()? {
                    v.push(val);
                }
                Ok(EmbeddingVector(v))
            }
        }

        deserializer.deserialize_any(VecVisitor)
    }
}

#[derive(Debug, Deserialize)]
pub struct EmbeddingRow {
    #[serde(default)]
    pub corpusid: i64,
    pub vector: EmbeddingVector,
}

pub use papeline_core::Accumulator;

// === Phase 1 accumulators (no trait — one row feeds 3 accumulators) ===

/// Accumulator for papers rows — collects fields then produces `RecordBatch`
pub struct PapersAccumulator {
    schema: Arc<Schema>,
    corpusid: Vec<i64>,
    title: Vec<String>,
    url: Vec<String>,
    venue: Vec<String>,
    publicationvenueid: Vec<Option<String>>,
    year: Vec<Option<i32>>,
    referencecount: Vec<i32>,
    citationcount: Vec<i32>,
    influentialcitationcount: Vec<i32>,
    isopenaccess: Vec<bool>,
    publicationdate: Vec<Option<String>>,
    publicationtypes: Vec<Option<Vec<Option<String>>>>,
    journal_name: Vec<Option<String>>,
    journal_pages: Vec<Option<String>>,
    journal_volume: Vec<Option<String>>,
    doi: Vec<Option<String>>,
    pubmed: Vec<Option<String>>,
    arxiv: Vec<Option<String>>,
    mag: Vec<Option<String>>,
    acl: Vec<Option<String>>,
    dblp: Vec<Option<String>>,
    pubmedcentral: Vec<Option<String>>,
}

impl Default for PapersAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl PapersAccumulator {
    pub fn new() -> Self {
        Self {
            schema: schema::papers().clone(),
            corpusid: Vec::with_capacity(RECORD_BATCH_SIZE),
            title: Vec::with_capacity(RECORD_BATCH_SIZE),
            url: Vec::with_capacity(RECORD_BATCH_SIZE),
            venue: Vec::with_capacity(RECORD_BATCH_SIZE),
            publicationvenueid: Vec::with_capacity(RECORD_BATCH_SIZE),
            year: Vec::with_capacity(RECORD_BATCH_SIZE),
            referencecount: Vec::with_capacity(RECORD_BATCH_SIZE),
            citationcount: Vec::with_capacity(RECORD_BATCH_SIZE),
            influentialcitationcount: Vec::with_capacity(RECORD_BATCH_SIZE),
            isopenaccess: Vec::with_capacity(RECORD_BATCH_SIZE),
            publicationdate: Vec::with_capacity(RECORD_BATCH_SIZE),
            publicationtypes: Vec::with_capacity(RECORD_BATCH_SIZE),
            journal_name: Vec::with_capacity(RECORD_BATCH_SIZE),
            journal_pages: Vec::with_capacity(RECORD_BATCH_SIZE),
            journal_volume: Vec::with_capacity(RECORD_BATCH_SIZE),
            doi: Vec::with_capacity(RECORD_BATCH_SIZE),
            pubmed: Vec::with_capacity(RECORD_BATCH_SIZE),
            arxiv: Vec::with_capacity(RECORD_BATCH_SIZE),
            mag: Vec::with_capacity(RECORD_BATCH_SIZE),
            acl: Vec::with_capacity(RECORD_BATCH_SIZE),
            dblp: Vec::with_capacity(RECORD_BATCH_SIZE),
            pubmedcentral: Vec::with_capacity(RECORD_BATCH_SIZE),
        }
    }

    /// Takes ownership of `PaperRow` to avoid cloning ~10 String fields.
    /// Call after `AuthorsAccumulator::push` and `FieldsAccumulator::push`
    /// which only borrow the row.
    pub fn push(&mut self, row: PaperRow) {
        self.corpusid.push(row.corpusid);
        self.title.push(row.title);
        self.url.push(row.url);
        self.venue.push(row.venue);
        self.publicationvenueid.push(row.publicationvenueid);
        self.year.push(row.year);
        self.referencecount.push(row.referencecount);
        self.citationcount.push(row.citationcount);
        self.influentialcitationcount
            .push(row.influentialcitationcount);
        self.isopenaccess.push(row.isopenaccess);
        self.publicationdate.push(row.publicationdate);
        self.publicationtypes.push(row.publicationtypes);

        let (jname, jpages, jvol) = match row.journal {
            Some(j) => (j.name, j.pages, j.volume),
            None => (None, None, None),
        };
        self.journal_name.push(jname);
        self.journal_pages.push(jpages);
        self.journal_volume.push(jvol);

        let (doi, pubmed, arxiv, mag, acl, dblp, pmc) = match row.externalids {
            Some(e) => (
                e.doi,
                e.pubmed,
                e.arxiv,
                e.mag,
                e.acl,
                e.dblp,
                e.pubmedcentral,
            ),
            None => (None, None, None, None, None, None, None),
        };
        self.doi.push(doi);
        self.pubmed.push(pubmed);
        self.arxiv.push(arxiv);
        self.mag.push(mag);
        self.acl.push(acl);
        self.dblp.push(dblp);
        self.pubmedcentral.push(pmc);
    }

    pub fn len(&self) -> usize {
        self.corpusid.len()
    }

    pub fn is_empty(&self) -> bool {
        self.corpusid.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.corpusid.len() >= RECORD_BATCH_SIZE
    }

    pub fn take_batch(&mut self) -> Result<RecordBatch, arrow::error::ArrowError> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(std::mem::take(&mut self.corpusid))),
            Arc::new(StringArray::from(std::mem::take(&mut self.title))),
            Arc::new(StringArray::from(std::mem::take(&mut self.url))),
            Arc::new(StringArray::from(std::mem::take(&mut self.venue))),
            Arc::new(StringArray::from(std::mem::take(
                &mut self.publicationvenueid,
            ))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.year))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.referencecount))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.citationcount))),
            Arc::new(Int32Array::from(std::mem::take(
                &mut self.influentialcitationcount,
            ))),
            Arc::new(BooleanArray::from(std::mem::take(&mut self.isopenaccess))),
            Arc::new(StringArray::from(std::mem::take(&mut self.publicationdate))),
            build_list_string_array(std::mem::take(&mut self.publicationtypes)),
            Arc::new(StringArray::from(std::mem::take(&mut self.journal_name))),
            Arc::new(StringArray::from(std::mem::take(&mut self.journal_pages))),
            Arc::new(StringArray::from(std::mem::take(&mut self.journal_volume))),
            Arc::new(StringArray::from(std::mem::take(&mut self.doi))),
            Arc::new(StringArray::from(std::mem::take(&mut self.pubmed))),
            Arc::new(StringArray::from(std::mem::take(&mut self.arxiv))),
            Arc::new(StringArray::from(std::mem::take(&mut self.mag))),
            Arc::new(StringArray::from(std::mem::take(&mut self.acl))),
            Arc::new(StringArray::from(std::mem::take(&mut self.dblp))),
            Arc::new(StringArray::from(std::mem::take(&mut self.pubmedcentral))),
        ];
        RecordBatch::try_new(self.schema.clone(), arrays)
    }
}

/// Decomposed authors from a single paper record
pub struct AuthorsAccumulator {
    schema: Arc<Schema>,
    corpusid: Vec<i64>,
    authorid: Vec<String>,
    name: Vec<String>,
    position: Vec<i32>,
}

impl Default for AuthorsAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthorsAccumulator {
    pub fn new() -> Self {
        Self {
            schema: schema::paper_authors().clone(),
            corpusid: Vec::with_capacity(RECORD_BATCH_SIZE),
            authorid: Vec::with_capacity(RECORD_BATCH_SIZE),
            name: Vec::with_capacity(RECORD_BATCH_SIZE),
            position: Vec::with_capacity(RECORD_BATCH_SIZE),
        }
    }

    pub fn push(&mut self, row: &PaperRow) {
        for (pos, author) in row.authors.iter().enumerate() {
            self.corpusid.push(row.corpusid);
            self.authorid
                .push(author.author_id.clone().unwrap_or_default());
            self.name.push(author.name.clone());
            self.position.push(pos as i32);
        }
    }

    pub fn len(&self) -> usize {
        self.corpusid.len()
    }

    pub fn is_empty(&self) -> bool {
        self.corpusid.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.corpusid.len() >= RECORD_BATCH_SIZE
    }

    pub fn take_batch(&mut self) -> Result<RecordBatch, arrow::error::ArrowError> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(std::mem::take(&mut self.corpusid))),
            Arc::new(StringArray::from(std::mem::take(&mut self.authorid))),
            Arc::new(StringArray::from(std::mem::take(&mut self.name))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.position))),
        ];
        RecordBatch::try_new(self.schema.clone(), arrays)
    }
}

/// Decomposed fields of study from a single paper record
pub struct FieldsAccumulator {
    schema: Arc<Schema>,
    corpusid: Vec<i64>,
    category: Vec<String>,
    source: Vec<String>,
}

impl Default for FieldsAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl FieldsAccumulator {
    pub fn new() -> Self {
        Self {
            schema: schema::paper_fields().clone(),
            corpusid: Vec::with_capacity(RECORD_BATCH_SIZE),
            category: Vec::with_capacity(RECORD_BATCH_SIZE),
            source: Vec::with_capacity(RECORD_BATCH_SIZE),
        }
    }

    pub fn push(&mut self, row: &PaperRow) {
        for field in &row.s2fieldsofstudy {
            self.corpusid.push(row.corpusid);
            self.category.push(field.category.clone());
            self.source.push(field.source.clone());
        }
    }

    pub fn len(&self) -> usize {
        self.corpusid.len()
    }

    pub fn is_empty(&self) -> bool {
        self.corpusid.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.corpusid.len() >= RECORD_BATCH_SIZE
    }

    pub fn take_batch(&mut self) -> Result<RecordBatch, arrow::error::ArrowError> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(std::mem::take(&mut self.corpusid))),
            Arc::new(StringArray::from(std::mem::take(&mut self.category))),
            Arc::new(StringArray::from(std::mem::take(&mut self.source))),
        ];
        RecordBatch::try_new(self.schema.clone(), arrays)
    }
}

// === Phase 2 accumulators (implement Accumulator trait) ===

/// Generic accumulator for simple corpusid + text datasets (abstracts, tldrs)
pub struct TextAccumulator {
    schema: Arc<Schema>,
    corpusid: Vec<i64>,
    text: Vec<String>,
}

impl TextAccumulator {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            schema,
            corpusid: Vec::with_capacity(RECORD_BATCH_SIZE),
            text: Vec::with_capacity(RECORD_BATCH_SIZE),
        }
    }
}

impl Accumulator for TextAccumulator {
    type Row = (i64, String);

    fn push(&mut self, row: (i64, String)) {
        self.corpusid.push(row.0);
        self.text.push(row.1);
    }

    fn len(&self) -> usize {
        self.corpusid.len()
    }

    fn is_full(&self) -> bool {
        self.corpusid.len() >= RECORD_BATCH_SIZE
    }

    fn take_batch(&mut self) -> Result<RecordBatch, arrow::error::ArrowError> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(std::mem::take(&mut self.corpusid))),
            Arc::new(StringArray::from(std::mem::take(&mut self.text))),
        ];
        RecordBatch::try_new(self.schema.clone(), arrays)
    }
}

/// Accumulator for citations
pub struct CitationsAccumulator {
    schema: Arc<Schema>,
    citationid: Vec<i64>,
    citingcorpusid: Vec<i64>,
    citedcorpusid: Vec<i64>,
    isinfluential: Vec<bool>,
    contexts: Vec<Option<Vec<Option<String>>>>,
    intents: CitationIntentsData,
}

impl Default for CitationsAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl CitationsAccumulator {
    pub fn new() -> Self {
        Self {
            schema: schema::citations().clone(),
            citationid: Vec::with_capacity(RECORD_BATCH_SIZE),
            citingcorpusid: Vec::with_capacity(RECORD_BATCH_SIZE),
            citedcorpusid: Vec::with_capacity(RECORD_BATCH_SIZE),
            isinfluential: Vec::with_capacity(RECORD_BATCH_SIZE),
            contexts: Vec::with_capacity(RECORD_BATCH_SIZE),
            intents: Vec::with_capacity(RECORD_BATCH_SIZE),
        }
    }
}

impl Accumulator for CitationsAccumulator {
    type Row = CitationRow;

    fn push(&mut self, row: CitationRow) {
        self.citationid.push(row.citationid);
        self.citingcorpusid.push(row.citingcorpusid);
        self.citedcorpusid.push(row.citedcorpusid);
        self.isinfluential.push(row.isinfluential);
        self.contexts.push(row.contexts);
        self.intents.push(row.intents);
    }

    fn len(&self) -> usize {
        self.citationid.len()
    }

    fn is_full(&self) -> bool {
        self.citationid.len() >= RECORD_BATCH_SIZE
    }

    fn take_batch(&mut self) -> Result<RecordBatch, arrow::error::ArrowError> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(std::mem::take(&mut self.citationid))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.citingcorpusid))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.citedcorpusid))),
            Arc::new(BooleanArray::from(std::mem::take(&mut self.isinfluential))),
            build_list_string_array(std::mem::take(&mut self.contexts)),
            build_list_list_string_array(std::mem::take(&mut self.intents)),
        ];
        RecordBatch::try_new(self.schema.clone(), arrays)
    }
}

/// Accumulator for embeddings (corpusid + 768-dim vector)
pub struct EmbeddingsAccumulator {
    schema: Arc<Schema>,
    corpusid: Vec<i64>,
    vectors: Vec<Option<Vec<f32>>>,
}

impl Default for EmbeddingsAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl EmbeddingsAccumulator {
    pub fn new() -> Self {
        Self {
            schema: schema::embeddings().clone(),
            corpusid: Vec::with_capacity(RECORD_BATCH_SIZE),
            vectors: Vec::with_capacity(RECORD_BATCH_SIZE),
        }
    }
}

impl Accumulator for EmbeddingsAccumulator {
    type Row = EmbeddingRow;

    fn push(&mut self, row: EmbeddingRow) {
        self.corpusid.push(row.corpusid);
        let mut vec = row.vector.0;
        let dim = schema::EMBEDDING_DIM as usize;
        if vec.len() != dim {
            log::debug!(
                "embedding vector has {} dims, expected {dim}; resizing",
                vec.len()
            );
            vec.resize(dim, 0.0);
        }
        self.vectors.push(Some(vec));
    }

    fn len(&self) -> usize {
        self.corpusid.len()
    }

    fn is_full(&self) -> bool {
        self.corpusid.len() >= RECORD_BATCH_SIZE
    }

    fn take_batch(&mut self) -> Result<RecordBatch, arrow::error::ArrowError> {
        let vectors = std::mem::take(&mut self.vectors);
        let fsl = build_fixed_size_list_f32(vectors, schema::EMBEDDING_DIM);
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(std::mem::take(&mut self.corpusid))),
            fsl,
        ];
        RecordBatch::try_new(self.schema.clone(), arrays)
    }
}

// === Helper functions ===

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

/// Build List<List<Utf8>> array (for citation intents)
fn build_list_list_string_array(data: CitationIntentsData) -> ArrayRef {
    let inner_builder = ListBuilder::new(StringBuilder::new());
    let mut builder = ListBuilder::new(inner_builder);
    for row in &data {
        match row {
            Some(outer_items) => {
                for inner in outer_items {
                    match inner {
                        Some(items) => {
                            for item in items {
                                match item {
                                    Some(s) => builder.values().values().append_value(s),
                                    None => builder.values().values().append_null(),
                                }
                            }
                            builder.values().append(true);
                        }
                        None => builder.values().append(false),
                    }
                }
                builder.append(true);
            }
            None => builder.append(false),
        }
    }
    Arc::new(builder.finish())
}

/// Build `FixedSizeList<Float32>[dim]` from vectors
fn build_fixed_size_list_f32(data: Vec<Option<Vec<f32>>>, dim: i32) -> ArrayRef {
    let mut values = Vec::with_capacity(data.len() * dim as usize);
    let mut valid = Vec::with_capacity(data.len());
    for row in &data {
        match row {
            Some(vec) => {
                values.extend_from_slice(vec);
                valid.push(true);
            }
            None => {
                values.extend(std::iter::repeat_n(0.0f32, dim as usize));
                valid.push(false);
            }
        }
    }
    let float_array = Float32Array::from(values);
    let field = Arc::new(arrow::datatypes::Field::new(
        "item",
        arrow::datatypes::DataType::Float32,
        false,
    ));
    let null_buffer = arrow::buffer::NullBuffer::from(valid);
    Arc::new(
        FixedSizeListArray::try_new(field, dim, Arc::new(float_array), Some(null_buffer))
            .expect("failed to build FixedSizeList"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- matches_domains ---

    #[test]
    fn matches_domains_hit() {
        let row: PaperRow = serde_json::from_str(
            r#"{"corpusid":1,"s2fieldsofstudy":[{"category":"Biology","source":"s2"}]}"#,
        )
        .unwrap();
        assert!(row.matches_domains(&["Biology".to_string()]));
    }

    #[test]
    fn matches_domains_miss() {
        let row: PaperRow = serde_json::from_str(
            r#"{"corpusid":1,"s2fieldsofstudy":[{"category":"Physics","source":"s2"}]}"#,
        )
        .unwrap();
        assert!(!row.matches_domains(&["Biology".to_string()]));
    }

    #[test]
    fn matches_domains_empty_fields() {
        let row: PaperRow = serde_json::from_str(r#"{"corpusid":1}"#).unwrap();
        assert!(!row.matches_domains(&["Biology".to_string()]));
    }

    #[test]
    fn matches_domains_case_sensitive() {
        let row: PaperRow = serde_json::from_str(
            r#"{"corpusid":1,"s2fieldsofstudy":[{"category":"biology","source":"s2"}]}"#,
        )
        .unwrap();
        assert!(!row.matches_domains(&["Biology".to_string()]));
    }

    #[test]
    fn matches_domains_multiple() {
        let row: PaperRow = serde_json::from_str(
            r#"{"corpusid":1,"s2fieldsofstudy":[{"category":"Physics","source":"s2"},{"category":"Biology","source":"s2"}]}"#,
        )
        .unwrap();
        assert!(row.matches_domains(&["Biology".to_string(), "Chemistry".to_string()]));
    }

    // --- null field handling ---

    #[test]
    fn paper_row_null_title() {
        let row: PaperRow = serde_json::from_str(r#"{"corpusid":1,"title":null}"#).unwrap();
        assert_eq!(row.title, "");
    }

    #[test]
    fn paper_row_null_url_venue() {
        let row: PaperRow =
            serde_json::from_str(r#"{"corpusid":1,"url":null,"venue":null}"#).unwrap();
        assert_eq!(row.url, "");
        assert_eq!(row.venue, "");
    }

    #[test]
    fn author_entry_null_author_id() {
        let row: PaperRow =
            serde_json::from_str(r#"{"corpusid":1,"authors":[{"authorId":null,"name":"John"}]}"#)
                .unwrap();
        assert_eq!(row.authors[0].author_id, None);
        assert_eq!(row.authors[0].name, "John");
    }

    #[test]
    fn author_entry_null_name() {
        let row: PaperRow =
            serde_json::from_str(r#"{"corpusid":1,"authors":[{"authorId":"123","name":null}]}"#)
                .unwrap();
        assert_eq!(row.authors[0].author_id, Some("123".to_string()));
        assert_eq!(row.authors[0].name, "");
    }

    #[test]
    fn field_entry_null_category_source() {
        let row: PaperRow = serde_json::from_str(
            r#"{"corpusid":1,"s2fieldsofstudy":[{"category":null,"source":null}]}"#,
        )
        .unwrap();
        assert_eq!(row.s2fieldsofstudy[0].category, "");
        assert_eq!(row.s2fieldsofstudy[0].source, "");
    }

    // --- EmbeddingVector deserialize ---

    #[test]
    fn embedding_from_json_string() {
        let row: EmbeddingRow =
            serde_json::from_str(r#"{"corpusid":42,"vector":"[0.1, -0.2, 0.3]"}"#).unwrap();
        assert_eq!(row.vector.0, vec![0.1, -0.2, 0.3]);
    }

    #[test]
    fn embedding_from_json_array() {
        let row: EmbeddingRow =
            serde_json::from_str(r#"{"corpusid":42,"vector":[0.1, -0.2, 0.3]}"#).unwrap();
        assert_eq!(row.vector.0, vec![0.1, -0.2, 0.3]);
    }

    #[test]
    fn embedding_empty_array() {
        let row: EmbeddingRow = serde_json::from_str(r#"{"corpusid":42,"vector":[]}"#).unwrap();
        assert!(row.vector.0.is_empty());
    }

    #[test]
    fn embedding_invalid_string() {
        let result: Result<EmbeddingRow, _> =
            serde_json::from_str(r#"{"corpusid":42,"vector":"not-json"}"#);
        assert!(result.is_err());
    }

    // --- AuthorEntry null handling ---

    #[test]
    fn author_id_null() {
        let row: PaperRow = serde_json::from_str(
            r#"{"corpusid":1,"authors":[{"authorId":null,"name":"John Doe"}]}"#,
        )
        .unwrap();
        assert_eq!(row.authors[0].author_id, None);
        assert_eq!(row.authors[0].name, "John Doe");
    }

    #[test]
    fn author_id_missing() {
        let row: PaperRow =
            serde_json::from_str(r#"{"corpusid":1,"authors":[{"name":"Jane Doe"}]}"#).unwrap();
        assert_eq!(row.authors[0].author_id, None);
    }

    #[test]
    fn author_id_present() {
        let row: PaperRow =
            serde_json::from_str(r#"{"corpusid":1,"authors":[{"authorId":"12345","name":"Bob"}]}"#)
                .unwrap();
        assert_eq!(row.authors[0].author_id, Some("12345".to_string()));
    }

    #[test]
    fn authors_mixed_null_and_present() {
        let row: PaperRow = serde_json::from_str(
            r#"{"corpusid":1,"authors":[{"authorId":"123","name":"A"},{"authorId":null,"name":"B"},{"name":"C"}]}"#,
        )
        .unwrap();
        assert_eq!(row.authors.len(), 3);
        assert_eq!(row.authors[0].author_id, Some("123".to_string()));
        assert_eq!(row.authors[1].author_id, None);
        assert_eq!(row.authors[2].author_id, None);
    }
}

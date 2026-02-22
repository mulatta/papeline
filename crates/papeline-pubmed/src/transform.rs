//! Transform PubmedArticle to Arrow RecordBatch

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::parser::{Author, Chemical, DataBank, Grant, MeshTerm, PubmedArticle};
use crate::schema;

const BATCH_SIZE: usize = 10_000;

/// Accumulator for building RecordBatches from PubmedArticles
pub struct ArticleAccumulator {
    schema: &'static Schema,
    // Identifiers
    pmid: Vec<String>,
    doi: Vec<Option<String>>,
    pmc_id: Vec<Option<String>>,
    pii: Vec<Option<String>>,
    // Article
    title: Vec<Option<String>>,
    vernacular_title: Vec<Option<String>>,
    abstract_text: Vec<Option<String>>,
    language: Vec<Option<String>>,
    publication_status: Vec<Option<String>>,
    // Journal
    journal_title: Vec<Option<String>>,
    journal_iso: Vec<Option<String>>,
    journal_issn: Vec<Option<String>>,
    journal_volume: Vec<Option<String>>,
    journal_issue: Vec<Option<String>>,
    pagination: Vec<Option<String>>,
    elocation_id: Vec<Option<String>>,
    // Dates
    pub_year: Vec<Option<i32>>,
    pub_month: Vec<Option<i32>>,
    pub_day: Vec<Option<i32>>,
    date_completed: Vec<Option<String>>,
    date_revised: Vec<Option<String>>,
    // Authors (JSON)
    authors_json: Vec<Option<String>>,
    affiliations_json: Vec<Option<String>>,
    collective_name: Vec<Option<String>>,
    // MeSH
    mesh_terms_json: Vec<Option<String>>,
    mesh_major_topics: Vec<Option<Vec<Option<String>>>>,
    // Chemicals
    chemicals_json: Vec<Option<String>>,
    // Grants
    grants_json: Vec<Option<String>>,
    // Publication Types
    publication_types: Vec<Option<Vec<Option<String>>>>,
    // Keywords
    keywords: Vec<Option<Vec<Option<String>>>>,
    // Data Banks
    databanks_json: Vec<Option<String>>,
    // References
    reference_count: Vec<Option<i32>>,
    // Other
    coi_statement: Vec<Option<String>>,
    copyright_info: Vec<Option<String>>,
}

impl ArticleAccumulator {
    pub fn new() -> Self {
        Self {
            schema: schema::articles(),
            pmid: Vec::with_capacity(BATCH_SIZE),
            doi: Vec::with_capacity(BATCH_SIZE),
            pmc_id: Vec::with_capacity(BATCH_SIZE),
            pii: Vec::with_capacity(BATCH_SIZE),
            title: Vec::with_capacity(BATCH_SIZE),
            vernacular_title: Vec::with_capacity(BATCH_SIZE),
            abstract_text: Vec::with_capacity(BATCH_SIZE),
            language: Vec::with_capacity(BATCH_SIZE),
            publication_status: Vec::with_capacity(BATCH_SIZE),
            journal_title: Vec::with_capacity(BATCH_SIZE),
            journal_iso: Vec::with_capacity(BATCH_SIZE),
            journal_issn: Vec::with_capacity(BATCH_SIZE),
            journal_volume: Vec::with_capacity(BATCH_SIZE),
            journal_issue: Vec::with_capacity(BATCH_SIZE),
            pagination: Vec::with_capacity(BATCH_SIZE),
            elocation_id: Vec::with_capacity(BATCH_SIZE),
            pub_year: Vec::with_capacity(BATCH_SIZE),
            pub_month: Vec::with_capacity(BATCH_SIZE),
            pub_day: Vec::with_capacity(BATCH_SIZE),
            date_completed: Vec::with_capacity(BATCH_SIZE),
            date_revised: Vec::with_capacity(BATCH_SIZE),
            authors_json: Vec::with_capacity(BATCH_SIZE),
            affiliations_json: Vec::with_capacity(BATCH_SIZE),
            collective_name: Vec::with_capacity(BATCH_SIZE),
            mesh_terms_json: Vec::with_capacity(BATCH_SIZE),
            mesh_major_topics: Vec::with_capacity(BATCH_SIZE),
            chemicals_json: Vec::with_capacity(BATCH_SIZE),
            grants_json: Vec::with_capacity(BATCH_SIZE),
            publication_types: Vec::with_capacity(BATCH_SIZE),
            keywords: Vec::with_capacity(BATCH_SIZE),
            databanks_json: Vec::with_capacity(BATCH_SIZE),
            reference_count: Vec::with_capacity(BATCH_SIZE),
            coi_statement: Vec::with_capacity(BATCH_SIZE),
            copyright_info: Vec::with_capacity(BATCH_SIZE),
        }
    }

    pub fn push(&mut self, article: PubmedArticle) {
        self.pmid.push(article.pmid);
        self.doi.push(article.doi);
        self.pmc_id.push(article.pmc_id);
        self.pii.push(article.pii);
        self.title.push(article.title);
        self.vernacular_title.push(article.vernacular_title);
        self.abstract_text.push(article.abstract_text);
        self.language.push(article.language);
        self.publication_status.push(article.publication_status);
        self.journal_title.push(article.journal_title);
        self.journal_iso.push(article.journal_iso);
        self.journal_issn.push(article.journal_issn);
        self.journal_volume.push(article.journal_volume);
        self.journal_issue.push(article.journal_issue);
        self.pagination.push(article.pagination);
        self.elocation_id.push(article.elocation_id);
        self.pub_year.push(article.pub_year);
        self.pub_month.push(article.pub_month);
        self.pub_day.push(article.pub_day);
        self.date_completed.push(article.date_completed);
        self.date_revised.push(article.date_revised);

        // Authors JSON
        self.authors_json.push(authors_to_json(&article.authors));
        self.affiliations_json
            .push(affiliations_to_json(&article.authors));
        self.collective_name.push(article.collective_name);

        // MeSH
        self.mesh_terms_json.push(mesh_to_json(&article.mesh_terms));
        self.mesh_major_topics
            .push(extract_major_topics(&article.mesh_terms));

        // Chemicals
        self.chemicals_json
            .push(chemicals_to_json(&article.chemicals));

        // Grants
        self.grants_json.push(grants_to_json(&article.grants));

        // Publication Types
        self.publication_types
            .push(if article.publication_types.is_empty() {
                None
            } else {
                Some(article.publication_types.into_iter().map(Some).collect())
            });

        // Keywords
        self.keywords.push(if article.keywords.is_empty() {
            None
        } else {
            Some(article.keywords.into_iter().map(Some).collect())
        });

        // Data Banks
        self.databanks_json
            .push(databanks_to_json(&article.databanks));

        // References
        self.reference_count.push(if article.reference_count > 0 {
            Some(article.reference_count)
        } else {
            None
        });

        // Other
        self.coi_statement.push(article.coi_statement);
        self.copyright_info.push(article.copyright_info);
    }

    pub fn len(&self) -> usize {
        self.pmid.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pmid.is_empty()
    }

    pub fn take_batch(&mut self) -> RecordBatch {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(std::mem::take(&mut self.pmid))),
            Arc::new(StringArray::from(std::mem::take(&mut self.doi))),
            Arc::new(StringArray::from(std::mem::take(&mut self.pmc_id))),
            Arc::new(StringArray::from(std::mem::take(&mut self.pii))),
            Arc::new(StringArray::from(std::mem::take(&mut self.title))),
            Arc::new(StringArray::from(std::mem::take(
                &mut self.vernacular_title,
            ))),
            Arc::new(StringArray::from(std::mem::take(&mut self.abstract_text))),
            Arc::new(StringArray::from(std::mem::take(&mut self.language))),
            Arc::new(StringArray::from(std::mem::take(
                &mut self.publication_status,
            ))),
            Arc::new(StringArray::from(std::mem::take(&mut self.journal_title))),
            Arc::new(StringArray::from(std::mem::take(&mut self.journal_iso))),
            Arc::new(StringArray::from(std::mem::take(&mut self.journal_issn))),
            Arc::new(StringArray::from(std::mem::take(&mut self.journal_volume))),
            Arc::new(StringArray::from(std::mem::take(&mut self.journal_issue))),
            Arc::new(StringArray::from(std::mem::take(&mut self.pagination))),
            Arc::new(StringArray::from(std::mem::take(&mut self.elocation_id))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.pub_year))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.pub_month))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.pub_day))),
            Arc::new(StringArray::from(std::mem::take(&mut self.date_completed))),
            Arc::new(StringArray::from(std::mem::take(&mut self.date_revised))),
            Arc::new(StringArray::from(std::mem::take(&mut self.authors_json))),
            Arc::new(StringArray::from(std::mem::take(
                &mut self.affiliations_json,
            ))),
            Arc::new(StringArray::from(std::mem::take(&mut self.collective_name))),
            Arc::new(StringArray::from(std::mem::take(&mut self.mesh_terms_json))),
            build_list_string_array(std::mem::take(&mut self.mesh_major_topics)),
            Arc::new(StringArray::from(std::mem::take(&mut self.chemicals_json))),
            Arc::new(StringArray::from(std::mem::take(&mut self.grants_json))),
            build_list_string_array(std::mem::take(&mut self.publication_types)),
            build_list_string_array(std::mem::take(&mut self.keywords)),
            Arc::new(StringArray::from(std::mem::take(&mut self.databanks_json))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.reference_count))),
            Arc::new(StringArray::from(std::mem::take(&mut self.coi_statement))),
            Arc::new(StringArray::from(std::mem::take(&mut self.copyright_info))),
        ];

        RecordBatch::try_new(Arc::new((*self.schema).clone()), arrays).expect("schema mismatch")
    }
}

impl Default for ArticleAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

fn authors_to_json(authors: &[Author]) -> Option<String> {
    if authors.is_empty() {
        return None;
    }

    let arr: Vec<serde_json::Value> = authors
        .iter()
        .map(|a| {
            serde_json::json!({
                "last_name": a.last_name,
                "fore_name": a.fore_name,
                "initials": a.initials,
            })
        })
        .collect();

    Some(serde_json::to_string(&arr).unwrap_or_default())
}

fn affiliations_to_json(authors: &[Author]) -> Option<String> {
    let all_affs: Vec<&str> = authors
        .iter()
        .flat_map(|a| a.affiliations.iter().map(|s| s.as_str()))
        .collect();

    if all_affs.is_empty() {
        return None;
    }

    Some(serde_json::to_string(&all_affs).unwrap_or_default())
}

fn mesh_to_json(terms: &[MeshTerm]) -> Option<String> {
    if terms.is_empty() {
        return None;
    }

    let arr: Vec<serde_json::Value> = terms
        .iter()
        .map(|t| {
            serde_json::json!({
                "descriptor": t.descriptor,
                "descriptor_ui": t.descriptor_ui,
                "is_major": t.is_major_topic,
                "qualifiers": t.qualifiers.iter().map(|q| {
                    serde_json::json!({
                        "name": q.name,
                        "ui": q.ui,
                        "is_major": q.is_major_topic,
                    })
                }).collect::<Vec<_>>(),
            })
        })
        .collect();

    Some(serde_json::to_string(&arr).unwrap_or_default())
}

fn extract_major_topics(terms: &[MeshTerm]) -> Option<Vec<Option<String>>> {
    let majors: Vec<Option<String>> = terms
        .iter()
        .filter(|t| t.is_major_topic)
        .map(|t| Some(t.descriptor.clone()))
        .collect();

    if majors.is_empty() {
        None
    } else {
        Some(majors)
    }
}

fn chemicals_to_json(chemicals: &[Chemical]) -> Option<String> {
    if chemicals.is_empty() {
        return None;
    }

    let arr: Vec<serde_json::Value> = chemicals
        .iter()
        .map(|c| {
            serde_json::json!({
                "name": c.name,
                "registry_number": c.registry_number,
                "ui": c.ui,
            })
        })
        .collect();

    Some(serde_json::to_string(&arr).unwrap_or_default())
}

fn grants_to_json(grants: &[Grant]) -> Option<String> {
    if grants.is_empty() {
        return None;
    }

    let arr: Vec<serde_json::Value> = grants
        .iter()
        .map(|g| {
            serde_json::json!({
                "grant_id": g.grant_id,
                "acronym": g.acronym,
                "agency": g.agency,
                "country": g.country,
            })
        })
        .collect();

    Some(serde_json::to_string(&arr).unwrap_or_default())
}

fn databanks_to_json(banks: &[DataBank]) -> Option<String> {
    if banks.is_empty() {
        return None;
    }

    let arr: Vec<serde_json::Value> = banks
        .iter()
        .map(|b| {
            serde_json::json!({
                "name": b.name,
                "accession_numbers": b.accession_numbers,
            })
        })
        .collect();

    Some(serde_json::to_string(&arr).unwrap_or_default())
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

    #[test]
    fn accumulator_basic() {
        let mut acc = ArticleAccumulator::new();
        assert!(acc.is_empty());

        let article = PubmedArticle {
            pmid: "12345".to_string(),
            title: Some("Test".to_string()),
            ..Default::default()
        };

        acc.push(article);
        assert_eq!(acc.len(), 1);

        let batch = acc.take_batch();
        assert_eq!(batch.num_rows(), 1);
        assert!(acc.is_empty());
    }

    #[test]
    fn authors_to_json_empty() {
        assert!(authors_to_json(&[]).is_none());
    }

    #[test]
    fn authors_to_json_populated() {
        let authors = vec![Author {
            last_name: Some("Smith".to_string()),
            fore_name: Some("John".to_string()),
            initials: Some("J".to_string()),
            affiliations: vec!["MIT".to_string()],
        }];
        let json = authors_to_json(&authors).unwrap();
        assert!(json.contains("Smith"));
        assert!(json.contains("John"));
    }

    #[test]
    fn affiliations_to_json_empty() {
        let authors = vec![Author {
            last_name: Some("Test".to_string()),
            fore_name: None,
            initials: None,
            affiliations: vec![],
        }];
        assert!(affiliations_to_json(&authors).is_none());
    }

    #[test]
    fn affiliations_to_json_populated() {
        let authors = vec![Author {
            last_name: Some("Test".to_string()),
            fore_name: None,
            initials: None,
            affiliations: vec!["Harvard".to_string(), "MIT".to_string()],
        }];
        let json = affiliations_to_json(&authors).unwrap();
        assert!(json.contains("Harvard"));
        assert!(json.contains("MIT"));
    }

    #[test]
    fn mesh_to_json_empty() {
        assert!(mesh_to_json(&[]).is_none());
    }

    #[test]
    fn mesh_to_json_populated() {
        let terms = vec![MeshTerm {
            descriptor: "Cancer".to_string(),
            descriptor_ui: Some("D009369".to_string()),
            is_major_topic: true,
            qualifiers: vec![],
        }];
        let json = mesh_to_json(&terms).unwrap();
        assert!(json.contains("Cancer"));
        assert!(json.contains("D009369"));
        assert!(json.contains("\"is_major\":true"));
    }

    #[test]
    fn extract_major_topics_empty() {
        assert!(extract_major_topics(&[]).is_none());
    }

    #[test]
    fn extract_major_topics_filters_non_major() {
        let terms = vec![
            MeshTerm {
                descriptor: "Major".to_string(),
                descriptor_ui: None,
                is_major_topic: true,
                qualifiers: vec![],
            },
            MeshTerm {
                descriptor: "Minor".to_string(),
                descriptor_ui: None,
                is_major_topic: false,
                qualifiers: vec![],
            },
        ];
        let majors = extract_major_topics(&terms).unwrap();
        assert_eq!(majors.len(), 1);
        assert_eq!(majors[0], Some("Major".to_string()));
    }

    #[test]
    fn chemicals_to_json_empty() {
        assert!(chemicals_to_json(&[]).is_none());
    }

    #[test]
    fn chemicals_to_json_populated() {
        let chemicals = vec![Chemical {
            name: "Aspirin".to_string(),
            registry_number: Some("50-78-2".to_string()),
            ui: Some("D001241".to_string()),
        }];
        let json = chemicals_to_json(&chemicals).unwrap();
        assert!(json.contains("Aspirin"));
        assert!(json.contains("50-78-2"));
    }

    #[test]
    fn grants_to_json_empty() {
        assert!(grants_to_json(&[]).is_none());
    }

    #[test]
    fn grants_to_json_populated() {
        let grants = vec![Grant {
            grant_id: Some("R01-12345".to_string()),
            acronym: Some("NIH".to_string()),
            agency: Some("National Institutes of Health".to_string()),
            country: Some("USA".to_string()),
        }];
        let json = grants_to_json(&grants).unwrap();
        assert!(json.contains("R01-12345"));
        assert!(json.contains("NIH"));
    }

    #[test]
    fn databanks_to_json_empty() {
        assert!(databanks_to_json(&[]).is_none());
    }

    #[test]
    fn databanks_to_json_populated() {
        let banks = vec![DataBank {
            name: "GenBank".to_string(),
            accession_numbers: vec!["AB123".to_string(), "CD456".to_string()],
        }];
        let json = databanks_to_json(&banks).unwrap();
        assert!(json.contains("GenBank"));
        assert!(json.contains("AB123"));
    }

    #[test]
    fn build_list_string_array_empty() {
        let data: Vec<Option<Vec<Option<String>>>> = vec![];
        let arr = build_list_string_array(data);
        assert_eq!(arr.len(), 0);
    }

    #[test]
    fn build_list_string_array_with_nulls() {
        let data: Vec<Option<Vec<Option<String>>>> = vec![
            None,
            Some(vec![Some("a".to_string()), None, Some("b".to_string())]),
            None,
        ];
        let arr = build_list_string_array(data);
        assert_eq!(arr.len(), 3);
    }

    #[test]
    fn accumulator_multiple_articles() {
        let mut acc = ArticleAccumulator::new();

        for i in 0..5 {
            acc.push(PubmedArticle {
                pmid: format!("{}", i),
                ..Default::default()
            });
        }

        assert_eq!(acc.len(), 5);
        let batch = acc.take_batch();
        assert_eq!(batch.num_rows(), 5);
    }

    #[test]
    fn accumulator_with_all_fields() {
        let mut acc = ArticleAccumulator::new();

        let article = PubmedArticle {
            pmid: "12345".to_string(),
            doi: Some("10.1000/test".to_string()),
            pmc_id: Some("PMC123".to_string()),
            title: Some("Test Title".to_string()),
            abstract_text: Some("Abstract".to_string()),
            language: Some("eng".to_string()),
            authors: vec![Author {
                last_name: Some("Test".to_string()),
                fore_name: Some("Author".to_string()),
                initials: Some("TA".to_string()),
                affiliations: vec!["Univ".to_string()],
            }],
            mesh_terms: vec![MeshTerm {
                descriptor: "Term".to_string(),
                descriptor_ui: Some("D123".to_string()),
                is_major_topic: true,
                qualifiers: vec![],
            }],
            keywords: vec!["keyword1".to_string()],
            publication_types: vec!["Journal Article".to_string()],
            ..Default::default()
        };

        acc.push(article);
        let batch = acc.take_batch();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 34); // Should match schema field count
    }
}

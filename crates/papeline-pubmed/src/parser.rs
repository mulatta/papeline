//! PubMed XML parser using quick-xml
//!
//! Streaming parser for PubMed XML format.

use anyhow::{Context, Result};
use quick_xml::Reader;
use quick_xml::events::Event;

/// Parsed PubMed article with all fields
#[derive(Debug, Default)]
pub struct PubmedArticle {
    // Identifiers
    pub pmid: String,
    pub doi: Option<String>,
    pub pmc_id: Option<String>,
    pub pii: Option<String>,

    // Article
    pub title: Option<String>,
    pub vernacular_title: Option<String>,
    pub abstract_text: Option<String>,
    pub language: Option<String>,
    pub publication_status: Option<String>,

    // Journal
    pub journal_title: Option<String>,
    pub journal_iso: Option<String>,
    pub journal_issn: Option<String>,
    pub journal_volume: Option<String>,
    pub journal_issue: Option<String>,
    pub pagination: Option<String>,
    pub elocation_id: Option<String>,

    // Dates
    pub pub_year: Option<i32>,
    pub pub_month: Option<i32>,
    pub pub_day: Option<i32>,
    pub date_completed: Option<String>,
    pub date_revised: Option<String>,

    // Authors
    pub authors: Vec<Author>,
    pub collective_name: Option<String>,

    // MeSH Terms
    pub mesh_terms: Vec<MeshTerm>,

    // Chemicals
    pub chemicals: Vec<Chemical>,

    // Grants
    pub grants: Vec<Grant>,

    // Publication Types
    pub publication_types: Vec<String>,

    // Keywords
    pub keywords: Vec<String>,

    // Data Banks
    pub databanks: Vec<DataBank>,

    // References
    pub reference_count: i32,

    // Other
    pub coi_statement: Option<String>,
    pub copyright_info: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct Author {
    pub last_name: Option<String>,
    pub fore_name: Option<String>,
    pub initials: Option<String>,
    pub affiliations: Vec<String>,
}

#[derive(Debug, Default, Clone)]
pub struct MeshTerm {
    pub descriptor: String,
    pub descriptor_ui: Option<String>,
    pub is_major_topic: bool,
    pub qualifiers: Vec<MeshQualifier>,
}

#[derive(Debug, Default, Clone)]
pub struct MeshQualifier {
    pub name: String,
    pub ui: Option<String>,
    pub is_major_topic: bool,
}

#[derive(Debug, Default, Clone)]
pub struct Chemical {
    pub name: String,
    pub registry_number: Option<String>,
    pub ui: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct Grant {
    pub grant_id: Option<String>,
    pub acronym: Option<String>,
    pub agency: Option<String>,
    pub country: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct DataBank {
    pub name: String,
    pub accession_numbers: Vec<String>,
}

/// Result of parsing a PubMed XML file.
/// Contains both new/updated articles and deleted PMIDs.
#[derive(Debug, Default)]
pub struct ParseResult {
    pub articles: Vec<PubmedArticle>,
    pub deleted_pmids: Vec<String>,
}

/// Parse multiple PubMed articles from XML content.
/// Also extracts DeleteCitation entries (PMIDs to be removed).
pub fn parse_pubmed_xml(xml: &str) -> Result<ParseResult> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut result = ParseResult::default();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) if e.name().as_ref() == b"PubmedArticle" => {
                match parse_article(&mut reader) {
                    Ok(article) => result.articles.push(article),
                    Err(e) => log::debug!("Failed to parse article: {}", e),
                }
            }
            Ok(Event::Start(e)) if e.name().as_ref() == b"DeleteCitation" => {
                parse_delete_citation(&mut reader, &mut result.deleted_pmids)?;
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(e).context("XML parse error"),
            _ => {}
        }
        buf.clear();
    }

    Ok(result)
}

/// Parse `<DeleteCitation>` block: extract PMID values.
fn parse_delete_citation(reader: &mut Reader<&[u8]>, deleted: &mut Vec<String>) -> Result<()> {
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if e.name().as_ref() == b"PMID" => {
                let text = reader.read_text(e.name())?;
                let pmid = text.trim().to_string();
                if !pmid.is_empty() {
                    deleted.push(pmid);
                }
            }
            Event::End(e) if e.name().as_ref() == b"DeleteCitation" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    Ok(())
}

fn parse_article(reader: &mut Reader<&[u8]>) -> Result<PubmedArticle> {
    let mut article = PubmedArticle::default();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => match e.name().as_ref() {
                b"MedlineCitation" => parse_medline_citation(reader, &mut article)?,
                b"PubmedData" => parse_pubmed_data(reader, &mut article)?,
                _ => {}
            },
            Event::End(e) if e.name().as_ref() == b"PubmedArticle" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(article)
}

fn parse_medline_citation(reader: &mut Reader<&[u8]>, article: &mut PubmedArticle) -> Result<()> {
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => match e.name().as_ref() {
                b"PMID" => article.pmid = read_text(reader)?,
                b"DateCompleted" => article.date_completed = Some(read_date(reader)?),
                b"DateRevised" => article.date_revised = Some(read_date(reader)?),
                b"Article" => parse_article_element(reader, article)?,
                b"MeshHeadingList" => article.mesh_terms = parse_mesh_list(reader)?,
                b"ChemicalList" => article.chemicals = parse_chemical_list(reader)?,
                b"KeywordList" => article.keywords = parse_keyword_list(reader)?,
                b"CoiStatement" => article.coi_statement = Some(read_text(reader)?),
                _ => {}
            },
            Event::End(e) if e.name().as_ref() == b"MedlineCitation" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(())
}

fn parse_article_element(reader: &mut Reader<&[u8]>, article: &mut PubmedArticle) -> Result<()> {
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => match e.name().as_ref() {
                b"Journal" => parse_journal(reader, article)?,
                b"ArticleTitle" => {
                    article.title = Some(read_text_content(reader, b"ArticleTitle")?)
                }
                b"VernacularTitle" => article.vernacular_title = Some(read_text(reader)?),
                b"Abstract" => article.abstract_text = Some(parse_abstract(reader)?),
                b"AuthorList" => {
                    let (authors, collective_name) = parse_author_list(reader)?;
                    article.authors = authors;
                    if collective_name.is_some() {
                        article.collective_name = collective_name;
                    }
                }
                b"Language" => article.language = Some(read_text(reader)?),
                b"GrantList" => article.grants = parse_grant_list(reader)?,
                b"PublicationTypeList" => article.publication_types = parse_pub_type_list(reader)?,
                b"Pagination" => article.pagination = parse_pagination(reader)?,
                b"ELocationID" => article.elocation_id = Some(read_text(reader)?),
                b"DataBankList" => article.databanks = parse_databank_list(reader)?,
                _ => {}
            },
            Event::End(e) if e.name().as_ref() == b"Article" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(())
}

fn parse_journal(reader: &mut Reader<&[u8]>, article: &mut PubmedArticle) -> Result<()> {
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => match e.name().as_ref() {
                b"ISSN" => article.journal_issn = Some(read_text(reader)?),
                b"Title" => article.journal_title = Some(read_text(reader)?),
                b"ISOAbbreviation" => article.journal_iso = Some(read_text(reader)?),
                b"Volume" => article.journal_volume = Some(read_text(reader)?),
                b"Issue" => article.journal_issue = Some(read_text(reader)?),
                b"PubDate" => parse_pub_date(reader, article)?,
                _ => {}
            },
            Event::End(e) if e.name().as_ref() == b"Journal" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(())
}

fn parse_pub_date(reader: &mut Reader<&[u8]>, article: &mut PubmedArticle) -> Result<()> {
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => match e.name().as_ref() {
                b"Year" => article.pub_year = read_text(reader)?.parse().ok(),
                b"Month" => article.pub_month = parse_month(&read_text(reader)?),
                b"Day" => article.pub_day = read_text(reader)?.parse().ok(),
                _ => {}
            },
            Event::End(e) if e.name().as_ref() == b"PubDate" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(())
}

fn parse_month(s: &str) -> Option<i32> {
    // Handle both numeric and text months
    match s.parse::<i32>() {
        Ok(n) => Some(n),
        Err(_) => match s.to_lowercase().as_str() {
            "jan" => Some(1),
            "feb" => Some(2),
            "mar" => Some(3),
            "apr" => Some(4),
            "may" => Some(5),
            "jun" => Some(6),
            "jul" => Some(7),
            "aug" => Some(8),
            "sep" => Some(9),
            "oct" => Some(10),
            "nov" => Some(11),
            "dec" => Some(12),
            _ => None,
        },
    }
}

fn parse_abstract(reader: &mut Reader<&[u8]>) -> Result<String> {
    let mut buf = Vec::new();
    let mut text_parts = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if e.name().as_ref() == b"AbstractText" => {
                text_parts.push(read_text_content(reader, b"AbstractText")?);
            }
            Event::End(e) if e.name().as_ref() == b"Abstract" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(text_parts.join(" "))
}

fn parse_pagination(reader: &mut Reader<&[u8]>) -> Result<Option<String>> {
    let mut buf = Vec::new();
    let mut pagination = None;

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if e.name().as_ref() == b"MedlinePgn" => {
                pagination = Some(read_text(reader)?);
            }
            Event::End(e) if e.name().as_ref() == b"Pagination" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(pagination)
}

/// Returns (authors, collective_name)
fn parse_author_list(reader: &mut Reader<&[u8]>) -> Result<(Vec<Author>, Option<String>)> {
    let mut authors = Vec::new();
    let mut collective_name = None;
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if e.name().as_ref() == b"Author" => {
                let (author, coll) = parse_author(reader)?;
                if let Some(name) = coll {
                    collective_name = Some(name);
                }
                authors.push(author);
            }
            Event::End(e) if e.name().as_ref() == b"AuthorList" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok((authors, collective_name))
}

/// Returns (author, collective_name)
fn parse_author(reader: &mut Reader<&[u8]>) -> Result<(Author, Option<String>)> {
    let mut author = Author::default();
    let mut collective_name = None;
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => match e.name().as_ref() {
                b"LastName" => author.last_name = Some(read_text(reader)?),
                b"ForeName" => author.fore_name = Some(read_text(reader)?),
                b"Initials" => author.initials = Some(read_text(reader)?),
                b"CollectiveName" => collective_name = Some(read_text(reader)?),
                b"AffiliationInfo" => {
                    if let Some(aff) = parse_affiliation(reader)? {
                        author.affiliations.push(aff);
                    }
                }
                _ => {}
            },
            Event::End(e) if e.name().as_ref() == b"Author" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok((author, collective_name))
}

fn parse_affiliation(reader: &mut Reader<&[u8]>) -> Result<Option<String>> {
    let mut buf = Vec::new();
    let mut affiliation = None;

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if e.name().as_ref() == b"Affiliation" => {
                affiliation = Some(read_text(reader)?);
            }
            Event::End(e) if e.name().as_ref() == b"AffiliationInfo" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(affiliation)
}

fn parse_mesh_list(reader: &mut Reader<&[u8]>) -> Result<Vec<MeshTerm>> {
    let mut terms = Vec::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if e.name().as_ref() == b"MeshHeading" => {
                terms.push(parse_mesh_heading(reader)?);
            }
            Event::End(e) if e.name().as_ref() == b"MeshHeadingList" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(terms)
}

fn parse_mesh_heading(reader: &mut Reader<&[u8]>) -> Result<MeshTerm> {
    let mut term = MeshTerm::default();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => match e.name().as_ref() {
                b"DescriptorName" => {
                    // Get attributes
                    for attr in e.attributes().flatten() {
                        match attr.key.as_ref() {
                            b"UI" => {
                                term.descriptor_ui =
                                    Some(String::from_utf8_lossy(&attr.value).to_string())
                            }
                            b"MajorTopicYN" => term.is_major_topic = &*attr.value == b"Y",
                            _ => {}
                        }
                    }
                    term.descriptor = read_text(reader)?;
                }
                b"QualifierName" => {
                    let mut qual = MeshQualifier::default();
                    for attr in e.attributes().flatten() {
                        match attr.key.as_ref() {
                            b"UI" => {
                                qual.ui = Some(String::from_utf8_lossy(&attr.value).to_string())
                            }
                            b"MajorTopicYN" => qual.is_major_topic = &*attr.value == b"Y",
                            _ => {}
                        }
                    }
                    qual.name = read_text(reader)?;
                    term.qualifiers.push(qual);
                }
                _ => {}
            },
            Event::End(e) if e.name().as_ref() == b"MeshHeading" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(term)
}

fn parse_chemical_list(reader: &mut Reader<&[u8]>) -> Result<Vec<Chemical>> {
    let mut chemicals = Vec::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if e.name().as_ref() == b"Chemical" => {
                chemicals.push(parse_chemical(reader)?);
            }
            Event::End(e) if e.name().as_ref() == b"ChemicalList" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(chemicals)
}

fn parse_chemical(reader: &mut Reader<&[u8]>) -> Result<Chemical> {
    let mut chem = Chemical::default();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => match e.name().as_ref() {
                b"RegistryNumber" => chem.registry_number = Some(read_text(reader)?),
                b"NameOfSubstance" => {
                    for attr in e.attributes().flatten() {
                        if attr.key.as_ref() == b"UI" {
                            chem.ui = Some(String::from_utf8_lossy(&attr.value).to_string());
                        }
                    }
                    chem.name = read_text(reader)?;
                }
                _ => {}
            },
            Event::End(e) if e.name().as_ref() == b"Chemical" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(chem)
}

fn parse_grant_list(reader: &mut Reader<&[u8]>) -> Result<Vec<Grant>> {
    let mut grants = Vec::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if e.name().as_ref() == b"Grant" => {
                grants.push(parse_grant(reader)?);
            }
            Event::End(e) if e.name().as_ref() == b"GrantList" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(grants)
}

fn parse_grant(reader: &mut Reader<&[u8]>) -> Result<Grant> {
    let mut grant = Grant::default();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => match e.name().as_ref() {
                b"GrantID" => grant.grant_id = Some(read_text(reader)?),
                b"Acronym" => grant.acronym = Some(read_text(reader)?),
                b"Agency" => grant.agency = Some(read_text(reader)?),
                b"Country" => grant.country = Some(read_text(reader)?),
                _ => {}
            },
            Event::End(e) if e.name().as_ref() == b"Grant" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(grant)
}

fn parse_pub_type_list(reader: &mut Reader<&[u8]>) -> Result<Vec<String>> {
    let mut types = Vec::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if e.name().as_ref() == b"PublicationType" => {
                types.push(read_text(reader)?);
            }
            Event::End(e) if e.name().as_ref() == b"PublicationTypeList" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(types)
}

fn parse_keyword_list(reader: &mut Reader<&[u8]>) -> Result<Vec<String>> {
    let mut keywords = Vec::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if e.name().as_ref() == b"Keyword" => {
                keywords.push(read_text(reader)?);
            }
            Event::End(e) if e.name().as_ref() == b"KeywordList" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(keywords)
}

fn parse_databank_list(reader: &mut Reader<&[u8]>) -> Result<Vec<DataBank>> {
    let mut banks = Vec::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if e.name().as_ref() == b"DataBank" => {
                banks.push(parse_databank(reader)?);
            }
            Event::End(e) if e.name().as_ref() == b"DataBankList" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(banks)
}

fn parse_databank(reader: &mut Reader<&[u8]>) -> Result<DataBank> {
    let mut bank = DataBank::default();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => match e.name().as_ref() {
                b"DataBankName" => bank.name = read_text(reader)?,
                b"AccessionNumber" => bank.accession_numbers.push(read_text(reader)?),
                _ => {}
            },
            Event::End(e) if e.name().as_ref() == b"DataBank" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(bank)
}

fn parse_pubmed_data(reader: &mut Reader<&[u8]>, article: &mut PubmedArticle) -> Result<()> {
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => match e.name().as_ref() {
                b"ArticleIdList" => parse_article_id_list(reader, article)?,
                b"ReferenceList" => article.reference_count = count_references(reader)?,
                _ => {}
            },
            Event::End(e) if e.name().as_ref() == b"PubmedData" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(())
}

fn parse_article_id_list(reader: &mut Reader<&[u8]>, article: &mut PubmedArticle) -> Result<()> {
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if e.name().as_ref() == b"ArticleId" => {
                let mut id_type = String::new();
                for attr in e.attributes().flatten() {
                    if attr.key.as_ref() == b"IdType" {
                        id_type = String::from_utf8_lossy(&attr.value).to_string();
                    }
                }
                let value = read_text(reader)?;
                match id_type.as_str() {
                    "doi" => article.doi = Some(value),
                    "pmc" => article.pmc_id = Some(value),
                    "pii" => article.pii = Some(value),
                    _ => {}
                }
            }
            Event::End(e) if e.name().as_ref() == b"ArticleIdList" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(())
}

fn count_references(reader: &mut Reader<&[u8]>) -> Result<i32> {
    let mut count = 0;
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if e.name().as_ref() == b"Reference" => {
                count += 1;
                skip_element(reader, b"Reference")?;
            }
            Event::End(e) if e.name().as_ref() == b"ReferenceList" => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(count)
}

fn skip_element(reader: &mut Reader<&[u8]>, end_tag: &[u8]) -> Result<()> {
    let mut buf = Vec::new();
    let mut depth = 1;

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(_) => depth += 1,
            Event::End(e) => {
                depth -= 1;
                if depth == 0 && e.name().as_ref() == end_tag {
                    break;
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(())
}

/// Read text content until next end tag
fn read_text(reader: &mut Reader<&[u8]>) -> Result<String> {
    let mut buf = Vec::new();
    let mut text = String::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Text(e) => text.push_str(&e.unescape()?),
            Event::End(_) => break,
            Event::Start(_) => {
                // Handle nested elements (like <i>, <b>, etc.)
                text.push_str(&read_text(reader)?);
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(text)
}

/// Read text content of a specific element, handling nested tags
fn read_text_content(reader: &mut Reader<&[u8]>, end_tag: &[u8]) -> Result<String> {
    let mut buf = Vec::new();
    let mut text = String::new();
    let mut depth = 1;

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Text(e) => text.push_str(&e.unescape()?),
            Event::Start(_) => depth += 1,
            Event::End(e) => {
                depth -= 1;
                if depth == 0 && e.name().as_ref() == end_tag {
                    break;
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(text)
}

/// Read date in YYYY-MM-DD format
fn read_date(reader: &mut Reader<&[u8]>) -> Result<String> {
    let mut buf = Vec::new();
    let mut year = String::new();
    let mut month = String::new();
    let mut day = String::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => match e.name().as_ref() {
                b"Year" => year = read_text(reader)?,
                b"Month" => month = read_text(reader)?,
                b"Day" => day = read_text(reader)?,
                _ => {}
            },
            Event::End(e) => {
                let name = e.name();
                if name.as_ref() == b"DateCompleted" || name.as_ref() == b"DateRevised" {
                    break;
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    if !year.is_empty() {
        Ok(format!(
            "{}-{:0>2}-{:0>2}",
            year,
            month.parse::<i32>().unwrap_or(1),
            day.parse::<i32>().unwrap_or(1)
        ))
    } else {
        Ok(String::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_XML: &str = r#"<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>12345</PMID>
      <Article>
        <ArticleTitle>Test Article</ArticleTitle>
        <Abstract>
          <AbstractText>This is the abstract.</AbstractText>
        </Abstract>
      </Article>
      <MeshHeadingList>
        <MeshHeading>
          <DescriptorName UI="D000818" MajorTopicYN="Y">Animals</DescriptorName>
        </MeshHeading>
      </MeshHeadingList>
    </MedlineCitation>
    <PubmedData>
      <ArticleIdList>
        <ArticleId IdType="doi">10.1234/test</ArticleId>
      </ArticleIdList>
    </PubmedData>
  </PubmedArticle>
</PubmedArticleSet>"#;

    #[test]
    fn parse_basic_article() {
        let articles = parse_pubmed_xml(SAMPLE_XML).unwrap().articles;
        assert_eq!(articles.len(), 1);

        let article = &articles[0];
        assert_eq!(article.pmid, "12345");
        assert_eq!(article.title, Some("Test Article".to_string()));
        assert_eq!(
            article.abstract_text,
            Some("This is the abstract.".to_string())
        );
        assert_eq!(article.doi, Some("10.1234/test".to_string()));
    }

    #[test]
    fn parse_mesh_terms() {
        let articles = parse_pubmed_xml(SAMPLE_XML).unwrap().articles;
        let article = &articles[0];

        assert_eq!(article.mesh_terms.len(), 1);
        assert_eq!(article.mesh_terms[0].descriptor, "Animals");
        assert!(article.mesh_terms[0].is_major_topic);
        assert_eq!(
            article.mesh_terms[0].descriptor_ui,
            Some("D000818".to_string())
        );
    }

    #[test]
    fn parse_authors() {
        let xml = r#"<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>99999</PMID>
      <Article>
        <ArticleTitle>Author Test</ArticleTitle>
        <AuthorList>
          <Author>
            <LastName>Smith</LastName>
            <ForeName>John</ForeName>
            <Initials>J</Initials>
            <AffiliationInfo>
              <Affiliation>University of Test</Affiliation>
            </AffiliationInfo>
          </Author>
          <Author>
            <LastName>Doe</LastName>
            <ForeName>Jane</ForeName>
            <Initials>JD</Initials>
          </Author>
        </AuthorList>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>"#;

        let articles = parse_pubmed_xml(xml).unwrap().articles;
        let article = &articles[0];

        assert_eq!(article.authors.len(), 2);
        assert_eq!(article.authors[0].last_name, Some("Smith".to_string()));
        assert_eq!(article.authors[0].fore_name, Some("John".to_string()));
        assert_eq!(article.authors[0].affiliations.len(), 1);
        assert_eq!(article.authors[1].last_name, Some("Doe".to_string()));
        assert!(article.authors[1].affiliations.is_empty());
    }

    #[test]
    fn parse_journal() {
        let xml = r#"<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>88888</PMID>
      <Article>
        <Journal>
          <ISSN>1234-5678</ISSN>
          <Title>Journal of Testing</Title>
          <ISOAbbreviation>J Test</ISOAbbreviation>
          <JournalIssue>
            <Volume>42</Volume>
            <Issue>3</Issue>
            <PubDate>
              <Year>2024</Year>
              <Month>06</Month>
              <Day>15</Day>
            </PubDate>
          </JournalIssue>
        </Journal>
        <Pagination>
          <MedlinePgn>100-110</MedlinePgn>
        </Pagination>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>"#;

        let articles = parse_pubmed_xml(xml).unwrap().articles;
        let article = &articles[0];

        assert_eq!(
            article.journal_title,
            Some("Journal of Testing".to_string())
        );
        assert_eq!(article.journal_iso, Some("J Test".to_string()));
        assert_eq!(article.journal_issn, Some("1234-5678".to_string()));
        assert_eq!(article.journal_volume, Some("42".to_string()));
        assert_eq!(article.journal_issue, Some("3".to_string()));
        assert_eq!(article.pub_year, Some(2024));
        assert_eq!(article.pub_month, Some(6));
        assert_eq!(article.pub_day, Some(15));
        assert_eq!(article.pagination, Some("100-110".to_string()));
    }

    #[test]
    fn parse_chemicals() {
        let xml = r#"<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>77777</PMID>
      <ChemicalList>
        <Chemical>
          <RegistryNumber>0</RegistryNumber>
          <NameOfSubstance UI="D000111">Acetylcysteine</NameOfSubstance>
        </Chemical>
      </ChemicalList>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>"#;

        let articles = parse_pubmed_xml(xml).unwrap().articles;
        let article = &articles[0];

        assert_eq!(article.chemicals.len(), 1);
        assert_eq!(article.chemicals[0].name, "Acetylcysteine");
        assert_eq!(article.chemicals[0].registry_number, Some("0".to_string()));
        assert_eq!(article.chemicals[0].ui, Some("D000111".to_string()));
    }

    #[test]
    fn parse_grants() {
        let xml = r#"<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>66666</PMID>
      <Article>
        <GrantList>
          <Grant>
            <GrantID>R01-GM12345</GrantID>
            <Acronym>GM</Acronym>
            <Agency>NIH</Agency>
            <Country>United States</Country>
          </Grant>
        </GrantList>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>"#;

        let articles = parse_pubmed_xml(xml).unwrap().articles;
        let article = &articles[0];

        assert_eq!(article.grants.len(), 1);
        assert_eq!(article.grants[0].grant_id, Some("R01-GM12345".to_string()));
        assert_eq!(article.grants[0].acronym, Some("GM".to_string()));
        assert_eq!(article.grants[0].agency, Some("NIH".to_string()));
        assert_eq!(article.grants[0].country, Some("United States".to_string()));
    }

    #[test]
    fn parse_keywords() {
        let xml = r#"<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>55555</PMID>
      <KeywordList>
        <Keyword>machine learning</Keyword>
        <Keyword>neural networks</Keyword>
      </KeywordList>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>"#;

        let articles = parse_pubmed_xml(xml).unwrap().articles;
        let article = &articles[0];

        assert_eq!(article.keywords.len(), 2);
        assert!(article.keywords.contains(&"machine learning".to_string()));
        assert!(article.keywords.contains(&"neural networks".to_string()));
    }

    #[test]
    fn parse_publication_types() {
        let xml = r#"<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>44444</PMID>
      <Article>
        <PublicationTypeList>
          <PublicationType>Journal Article</PublicationType>
          <PublicationType>Review</PublicationType>
        </PublicationTypeList>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>"#;

        let articles = parse_pubmed_xml(xml).unwrap().articles;
        let article = &articles[0];

        assert_eq!(article.publication_types.len(), 2);
        assert!(
            article
                .publication_types
                .contains(&"Journal Article".to_string())
        );
        assert!(article.publication_types.contains(&"Review".to_string()));
    }

    #[test]
    fn parse_databanks() {
        let xml = r#"<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>33333</PMID>
      <Article>
        <DataBankList>
          <DataBank>
            <DataBankName>GenBank</DataBankName>
            <AccessionNumberList>
              <AccessionNumber>AB123456</AccessionNumber>
              <AccessionNumber>CD789012</AccessionNumber>
            </AccessionNumberList>
          </DataBank>
        </DataBankList>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>"#;

        let articles = parse_pubmed_xml(xml).unwrap().articles;
        let article = &articles[0];

        assert_eq!(article.databanks.len(), 1);
        assert_eq!(article.databanks[0].name, "GenBank");
        assert_eq!(article.databanks[0].accession_numbers.len(), 2);
    }

    #[test]
    fn parse_article_ids() {
        let xml = r#"<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>22222</PMID>
    </MedlineCitation>
    <PubmedData>
      <ArticleIdList>
        <ArticleId IdType="doi">10.1000/test</ArticleId>
        <ArticleId IdType="pmc">PMC1234567</ArticleId>
        <ArticleId IdType="pii">S0000-0000(24)00001-1</ArticleId>
      </ArticleIdList>
    </PubmedData>
  </PubmedArticle>
</PubmedArticleSet>"#;

        let articles = parse_pubmed_xml(xml).unwrap().articles;
        let article = &articles[0];

        assert_eq!(article.doi, Some("10.1000/test".to_string()));
        assert_eq!(article.pmc_id, Some("PMC1234567".to_string()));
        assert_eq!(article.pii, Some("S0000-0000(24)00001-1".to_string()));
    }

    #[test]
    fn parse_empty_set() {
        let xml = r#"<?xml version="1.0"?>
<PubmedArticleSet>
</PubmedArticleSet>"#;

        let articles = parse_pubmed_xml(xml).unwrap().articles;
        assert!(articles.is_empty());
    }

    #[test]
    fn parse_minimal_article() {
        let xml = r#"<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>11111</PMID>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>"#;

        let articles = parse_pubmed_xml(xml).unwrap().articles;
        assert_eq!(articles.len(), 1);
        assert_eq!(articles[0].pmid, "11111");
        assert!(articles[0].title.is_none());
        assert!(articles[0].abstract_text.is_none());
    }

    #[test]
    fn parse_multiple_articles() {
        let xml = r#"<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>1</PMID>
      <Article>
        <ArticleTitle>First Article</ArticleTitle>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>2</PMID>
      <Article>
        <ArticleTitle>Second Article</ArticleTitle>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>3</PMID>
      <Article>
        <ArticleTitle>Third Article</ArticleTitle>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>"#;

        let articles = parse_pubmed_xml(xml).unwrap().articles;
        assert_eq!(articles.len(), 3);
        assert_eq!(articles[0].pmid, "1");
        assert_eq!(articles[1].pmid, "2");
        assert_eq!(articles[2].pmid, "3");
        assert_eq!(articles[0].title, Some("First Article".to_string()));
        assert_eq!(articles[1].title, Some("Second Article".to_string()));
        assert_eq!(articles[2].title, Some("Third Article".to_string()));
    }

    /// Comprehensive test simulating a real PubMed article (PMID 1)
    #[test]
    fn parse_real_world_article() {
        let xml = r#"<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation Status="MEDLINE" Owner="NLM">
      <PMID Version="1">1</PMID>
      <DateCompleted>
        <Year>1976</Year>
        <Month>01</Month>
        <Day>16</Day>
      </DateCompleted>
      <DateRevised>
        <Year>2024</Year>
        <Month>01</Month>
        <Day>09</Day>
      </DateRevised>
      <Article PubModel="Print">
        <Journal>
          <ISSN IssnType="Print">0006-2944</ISSN>
          <JournalIssue CitedMedium="Print">
            <Volume>13</Volume>
            <Issue>2</Issue>
            <PubDate>
              <Year>1975</Year>
              <Month>Jun</Month>
            </PubDate>
          </JournalIssue>
          <Title>Biochemical medicine</Title>
          <ISOAbbreviation>Biochem Med</ISOAbbreviation>
        </Journal>
        <ArticleTitle>Formate assay in body fluids: application in methanol poisoning.</ArticleTitle>
        <Pagination>
          <MedlinePgn>117-26</MedlinePgn>
        </Pagination>
        <AuthorList CompleteYN="Y">
          <Author ValidYN="Y">
            <LastName>Makar</LastName>
            <ForeName>A B</ForeName>
            <Initials>AB</Initials>
          </Author>
          <Author ValidYN="Y">
            <LastName>McMartin</LastName>
            <ForeName>K E</ForeName>
            <Initials>KE</Initials>
          </Author>
        </AuthorList>
        <Language>eng</Language>
        <GrantList CompleteYN="Y">
          <Grant>
            <GrantID>F32 AG064886</GrantID>
            <Acronym>AG</Acronym>
            <Agency>NIA NIH HHS</Agency>
            <Country>United States</Country>
          </Grant>
        </GrantList>
        <PublicationTypeList>
          <PublicationType UI="D016428">Journal Article</PublicationType>
        </PublicationTypeList>
      </Article>
      <ChemicalList>
        <Chemical>
          <RegistryNumber>0</RegistryNumber>
          <NameOfSubstance UI="D005561">Formates</NameOfSubstance>
        </Chemical>
        <Chemical>
          <RegistryNumber>Y4S76JWI15</RegistryNumber>
          <NameOfSubstance UI="D000432">Methanol</NameOfSubstance>
        </Chemical>
      </ChemicalList>
      <MeshHeadingList>
        <MeshHeading>
          <DescriptorName UI="D000818" MajorTopicYN="N">Animals</DescriptorName>
        </MeshHeading>
        <MeshHeading>
          <DescriptorName UI="D005561" MajorTopicYN="Y">Formates</DescriptorName>
          <QualifierName UI="Q000032" MajorTopicYN="Y">analysis</QualifierName>
        </MeshHeading>
      </MeshHeadingList>
      <KeywordList Owner="NOTNLM">
        <Keyword MajorTopicYN="N">methanol</Keyword>
        <Keyword MajorTopicYN="N">poisoning</Keyword>
      </KeywordList>
      <CoiStatement>The authors declare no conflict of interest.</CoiStatement>
    </MedlineCitation>
    <PubmedData>
      <ArticleIdList>
        <ArticleId IdType="pubmed">1</ArticleId>
        <ArticleId IdType="doi">10.1016/0006-2944(75)90147-7</ArticleId>
        <ArticleId IdType="pmc">PMC7168437</ArticleId>
      </ArticleIdList>
    </PubmedData>
  </PubmedArticle>
</PubmedArticleSet>"#;

        let articles = parse_pubmed_xml(xml).unwrap().articles;
        assert_eq!(articles.len(), 1);

        let article = &articles[0];

        // Identifiers
        assert_eq!(article.pmid, "1");
        assert_eq!(
            article.doi,
            Some("10.1016/0006-2944(75)90147-7".to_string())
        );
        assert_eq!(article.pmc_id, Some("PMC7168437".to_string()));

        // Article info
        assert_eq!(
            article.title,
            Some("Formate assay in body fluids: application in methanol poisoning.".to_string())
        );
        assert_eq!(article.language, Some("eng".to_string()));
        assert_eq!(article.pagination, Some("117-26".to_string()));

        // Journal
        assert_eq!(
            article.journal_title,
            Some("Biochemical medicine".to_string())
        );
        assert_eq!(article.journal_iso, Some("Biochem Med".to_string()));
        assert_eq!(article.journal_issn, Some("0006-2944".to_string()));
        assert_eq!(article.journal_volume, Some("13".to_string()));
        assert_eq!(article.journal_issue, Some("2".to_string()));

        // Dates
        assert_eq!(article.pub_year, Some(1975));
        assert_eq!(article.pub_month, Some(6)); // Jun -> 6
        assert_eq!(article.date_completed, Some("1976-01-16".to_string()));
        assert_eq!(article.date_revised, Some("2024-01-09".to_string()));

        // Authors
        assert_eq!(article.authors.len(), 2);
        assert_eq!(article.authors[0].last_name, Some("Makar".to_string()));
        assert_eq!(article.authors[0].fore_name, Some("A B".to_string()));
        assert_eq!(article.authors[1].last_name, Some("McMartin".to_string()));

        // Grants
        assert_eq!(article.grants.len(), 1);
        assert_eq!(article.grants[0].grant_id, Some("F32 AG064886".to_string()));
        assert_eq!(article.grants[0].agency, Some("NIA NIH HHS".to_string()));

        // Chemicals
        assert_eq!(article.chemicals.len(), 2);
        assert_eq!(article.chemicals[0].name, "Formates");
        assert_eq!(article.chemicals[1].name, "Methanol");
        assert_eq!(
            article.chemicals[1].registry_number,
            Some("Y4S76JWI15".to_string())
        );

        // MeSH terms
        assert_eq!(article.mesh_terms.len(), 2);
        assert_eq!(article.mesh_terms[0].descriptor, "Animals");
        assert!(!article.mesh_terms[0].is_major_topic);
        assert_eq!(article.mesh_terms[1].descriptor, "Formates");
        assert!(article.mesh_terms[1].is_major_topic);
        assert_eq!(article.mesh_terms[1].qualifiers.len(), 1);
        assert_eq!(article.mesh_terms[1].qualifiers[0].name, "analysis");

        // Keywords
        assert_eq!(article.keywords.len(), 2);
        assert!(article.keywords.contains(&"methanol".to_string()));
        assert!(article.keywords.contains(&"poisoning".to_string()));

        // Publication types
        assert_eq!(article.publication_types.len(), 1);
        assert!(
            article
                .publication_types
                .contains(&"Journal Article".to_string())
        );

        // COI statement
        assert_eq!(
            article.coi_statement,
            Some("The authors declare no conflict of interest.".to_string())
        );
    }

    /// Test abstract with multiple sections (structured abstract)
    #[test]
    fn parse_structured_abstract() {
        let xml = r#"<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>99999</PMID>
      <Article>
        <Abstract>
          <AbstractText Label="BACKGROUND">This is the background.</AbstractText>
          <AbstractText Label="METHODS">These are the methods.</AbstractText>
          <AbstractText Label="RESULTS">These are the results.</AbstractText>
          <AbstractText Label="CONCLUSION">This is the conclusion.</AbstractText>
        </Abstract>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>"#;

        let articles = parse_pubmed_xml(xml).unwrap().articles;
        let article = &articles[0];

        // Abstract parts should be joined
        let abstract_text = article.abstract_text.as_ref().unwrap();
        assert!(abstract_text.contains("background"));
        assert!(abstract_text.contains("methods"));
        assert!(abstract_text.contains("results"));
        assert!(abstract_text.contains("conclusion"));
    }

    /// Test MeSH terms with multiple qualifiers
    #[test]
    fn parse_mesh_with_qualifiers() {
        let xml = r#"<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>88888</PMID>
      <MeshHeadingList>
        <MeshHeading>
          <DescriptorName UI="D001943" MajorTopicYN="Y">Breast Neoplasms</DescriptorName>
          <QualifierName UI="Q000175" MajorTopicYN="N">diagnosis</QualifierName>
          <QualifierName UI="Q000378" MajorTopicYN="Y">metabolism</QualifierName>
          <QualifierName UI="Q000628" MajorTopicYN="N">therapy</QualifierName>
        </MeshHeading>
      </MeshHeadingList>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>"#;

        let articles = parse_pubmed_xml(xml).unwrap().articles;
        let article = &articles[0];

        assert_eq!(article.mesh_terms.len(), 1);
        let mesh = &article.mesh_terms[0];
        assert_eq!(mesh.descriptor, "Breast Neoplasms");
        assert!(mesh.is_major_topic);
        assert_eq!(mesh.qualifiers.len(), 3);

        // Check qualifiers
        let qual_names: Vec<&str> = mesh.qualifiers.iter().map(|q| q.name.as_str()).collect();
        assert!(qual_names.contains(&"diagnosis"));
        assert!(qual_names.contains(&"metabolism"));
        assert!(qual_names.contains(&"therapy"));

        // Check major topic qualifier
        let metabolism = mesh
            .qualifiers
            .iter()
            .find(|q| q.name == "metabolism")
            .unwrap();
        assert!(metabolism.is_major_topic);
    }

    /// Test parsing with malformed/incomplete XML (error handling)
    #[test]
    fn parse_incomplete_article() {
        // Article missing closing tags - should handle gracefully
        let xml = r#"<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>12345</PMID>
      <Article>
        <ArticleTitle>Test"#;

        // Should not panic, may return empty or partial results
        let result = parse_pubmed_xml(xml);
        // Either succeeds with partial data or fails gracefully
        assert!(result.is_ok() || result.is_err());
    }

    /// Test article with collective author name
    #[test]
    fn parse_collective_author() {
        let xml = r#"<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>77777</PMID>
      <Article>
        <AuthorList>
          <Author ValidYN="Y">
            <CollectiveName>World Health Organization</CollectiveName>
          </Author>
        </AuthorList>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>"#;

        let articles = parse_pubmed_xml(xml).unwrap().articles;
        let article = &articles[0];

        // Collective name should be captured
        assert_eq!(
            article.collective_name,
            Some("World Health Organization".to_string())
        );
    }

    /// Test multiple databanks with accession numbers
    #[test]
    fn parse_multiple_databanks() {
        let xml = r#"<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>66666</PMID>
      <Article>
        <DataBankList>
          <DataBank>
            <DataBankName>GenBank</DataBankName>
            <AccessionNumberList>
              <AccessionNumber>AB123456</AccessionNumber>
              <AccessionNumber>CD789012</AccessionNumber>
            </AccessionNumberList>
          </DataBank>
          <DataBank>
            <DataBankName>PDB</DataBankName>
            <AccessionNumberList>
              <AccessionNumber>1ABC</AccessionNumber>
            </AccessionNumberList>
          </DataBank>
        </DataBankList>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>"#;

        let articles = parse_pubmed_xml(xml).unwrap().articles;
        let article = &articles[0];

        assert_eq!(article.databanks.len(), 2);
        assert_eq!(article.databanks[0].name, "GenBank");
        assert_eq!(article.databanks[0].accession_numbers.len(), 2);
        assert_eq!(article.databanks[1].name, "PDB");
        assert_eq!(article.databanks[1].accession_numbers.len(), 1);
    }

    /// Test month name parsing (Jan, Feb, etc.)
    #[test]
    fn parse_month_names() {
        let xml = r#"<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>55555</PMID>
      <Article>
        <Journal>
          <JournalIssue>
            <PubDate>
              <Year>2024</Year>
              <Month>Dec</Month>
            </PubDate>
          </JournalIssue>
        </Journal>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>"#;

        let articles = parse_pubmed_xml(xml).unwrap().articles;
        let article = &articles[0];

        assert_eq!(article.pub_year, Some(2024));
        assert_eq!(article.pub_month, Some(12)); // Dec -> 12
    }

    #[test]
    fn parse_delete_citation_block() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE PubmedArticleSet SYSTEM "http://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_190101.dtd">
<PubmedArticleSet>
  <DeleteCitation>
    <PMID Version="1">12345</PMID>
    <PMID Version="1">67890</PMID>
    <PMID Version="1">11111</PMID>
  </DeleteCitation>
</PubmedArticleSet>"#;

        let result = parse_pubmed_xml(xml).unwrap();
        assert!(result.articles.is_empty());
        assert_eq!(result.deleted_pmids, vec!["12345", "67890", "11111"]);
    }

    #[test]
    fn parse_mixed_articles_and_deletions() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE PubmedArticleSet SYSTEM "http://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_190101.dtd">
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation Status="MEDLINE" Owner="NLM">
      <PMID Version="1">99999</PMID>
      <Article PubModel="Print">
        <Journal>
          <JournalIssue CitedMedium="Print">
            <PubDate><Year>2024</Year></PubDate>
          </JournalIssue>
        </Journal>
        <ArticleTitle>Test</ArticleTitle>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
  <DeleteCitation>
    <PMID Version="1">12345</PMID>
    <PMID Version="1">67890</PMID>
  </DeleteCitation>
</PubmedArticleSet>"#;

        let result = parse_pubmed_xml(xml).unwrap();
        assert_eq!(result.articles.len(), 1);
        assert_eq!(result.articles[0].pmid, "99999");
        assert_eq!(result.deleted_pmids, vec!["12345", "67890"]);
    }
}

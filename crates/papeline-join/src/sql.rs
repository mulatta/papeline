//! SQL generation for the multi-source join pipeline.
//!
//! Strategy:
//! - OA: 2-pass hash join (DOI then PMID fallback)
//! - S2: Narrow key tables (DOI/PMID only) → match → enrich via integer key joins
//! - This avoids materializing full S2 papers (22 cols) in hash tables

use std::path::Path;

/// Returns the SQL to create the normalize_doi macro.
pub fn create_normalize_doi_macro() -> &'static str {
    "CREATE OR REPLACE MACRO normalize_doi(d) AS \
     REGEXP_REPLACE(LOWER(d), '^https?://doi\\.org/', '')"
}

/// Returns SQL to create views over the source parquet directories.
/// For optional S2 files (abstracts, tldrs, paper_authors, paper_fields),
/// creates empty-table fallback when files don't exist.
pub fn create_source_views(pubmed_dir: &Path, openalex_dir: &Path, s2_dir: &Path) -> Vec<String> {
    let pm = pubmed_dir.display();
    let oa = openalex_dir.display();
    let s2 = s2_dir.display();

    let mut stmts = vec![
        format!(
            "CREATE OR REPLACE VIEW v_pubmed AS \
             SELECT * FROM read_parquet('{pm}/pubmed_*.parquet')"
        ),
        format!(
            "CREATE OR REPLACE VIEW v_openalex AS \
             SELECT * FROM read_parquet('{oa}/works_*.parquet')"
        ),
        format!(
            "CREATE OR REPLACE VIEW v_s2_papers AS \
             SELECT * FROM read_parquet('{s2}/papers_*.parquet')"
        ),
        format!(
            "CREATE OR REPLACE VIEW v_s2_citations AS \
             SELECT * FROM read_parquet('{s2}/citations_*.parquet')"
        ),
    ];

    // Optional S2 views: fallback to empty table if files don't exist
    stmts.push(s2_view_or_empty(
        s2_dir,
        "abstracts_*.parquet",
        "v_s2_abstracts",
        "NULL::BIGINT AS corpusid, NULL::VARCHAR AS abstract",
    ));
    stmts.push(s2_view_or_empty(
        s2_dir,
        "tldrs_*.parquet",
        "v_s2_tldrs",
        "NULL::BIGINT AS corpusid, NULL::VARCHAR AS text",
    ));
    stmts.push(s2_view_or_empty(
        s2_dir,
        "paper_authors_*.parquet",
        "v_s2_paper_authors",
        "NULL::BIGINT AS corpusid, NULL::BIGINT AS authorid, NULL::VARCHAR AS name, NULL::INTEGER AS position",
    ));
    stmts.push(s2_view_or_empty(
        s2_dir,
        "paper_fields_*.parquet",
        "v_s2_paper_fields",
        "NULL::BIGINT AS corpusid, NULL::VARCHAR AS category, NULL::VARCHAR AS source",
    ));

    stmts
}

/// Create a view over S2 parquet files, or an empty table fallback if no files match.
/// `cols_ddl` is comma-separated "NULL::TYPE AS name" expressions for the empty fallback.
fn s2_view_or_empty(s2_dir: &Path, pattern: &str, view_name: &str, cols_ddl: &str) -> String {
    let full = s2_dir.join(pattern);
    let full_str = full.to_string_lossy();
    let has_files = glob::glob(&full_str)
        .ok()
        .and_then(|mut g| g.next())
        .is_some();

    if has_files {
        format!(
            "CREATE OR REPLACE VIEW {view_name} AS \
             SELECT * FROM read_parquet('{}')",
            full.display()
        )
    } else {
        format!(
            "CREATE OR REPLACE VIEW {view_name} AS \
             SELECT {cols_ddl} WHERE false"
        )
    }
}

// ── OA columns selected in pass 1 (pm.* + these OA columns) ──

const OA_COLS: &str = "\
       oa.id AS openalex_id,
       oa.cited_by_count AS oa_cited_by_count,
       oa.referenced_works_count AS oa_referenced_works_count,
       oa.is_oa,
       oa.oa_status,
       oa.type AS oa_work_type,
       oa.primary_topic_id,
       oa.primary_topic_display_name,
       oa.abstract_text AS oa_abstract,
       oa.publication_date AS oa_publication_date,
       oa.publication_year AS oa_publication_year,
       oa.language AS oa_language,
       oa.display_name AS oa_display_name,
       oa.author_ids AS oa_author_ids,
       oa.institution_ids AS oa_institution_ids,
       oa.source_id AS oa_source_id,
       oa.source_display_name AS oa_source_display_name,
       oa.source_type AS oa_source_type,
       oa.mag AS oa_mag,
       oa.created_date AS oa_created_date,
       oa.updated_date AS oa_updated_date";

/// Pass 1: Join PubMed with OpenAlex on DOI.
pub fn join_pubmed_openalex_pass1() -> String {
    format!(
        "CREATE OR REPLACE TEMP TABLE pm_oa_pass1 AS
     SELECT
       pm.*,
       {OA_COLS}
     FROM v_pubmed pm
     LEFT JOIN (
       SELECT *
       FROM v_openalex
       WHERE doi IS NOT NULL AND doi != ''
       QUALIFY ROW_NUMBER() OVER (
         PARTITION BY normalize_doi(doi) ORDER BY cited_by_count DESC
       ) = 1
     ) oa
       ON normalize_doi(pm.doi) = normalize_doi(oa.doi)
       AND pm.doi IS NOT NULL
       AND pm.doi != ''"
    )
}

// OA columns for the UNION ALL pass2 (explicit select, prefixed with alias)
const OA_COLS_ALIASED_PASS2: &str = "\
       oa2.id AS openalex_id,
       oa2.cited_by_count AS oa_cited_by_count,
       oa2.referenced_works_count AS oa_referenced_works_count,
       oa2.is_oa,
       oa2.oa_status,
       oa2.type AS oa_work_type,
       oa2.primary_topic_id,
       oa2.primary_topic_display_name,
       oa2.abstract_text AS oa_abstract,
       oa2.publication_date AS oa_publication_date,
       oa2.publication_year AS oa_publication_year,
       oa2.language AS oa_language,
       oa2.display_name AS oa_display_name,
       oa2.author_ids AS oa_author_ids,
       oa2.institution_ids AS oa_institution_ids,
       oa2.source_id AS oa_source_id,
       oa2.source_display_name AS oa_source_display_name,
       oa2.source_type AS oa_source_type,
       oa2.mag AS oa_mag,
       oa2.created_date AS oa_created_date,
       oa2.updated_date AS oa_updated_date";

/// Pass 2: For rows unmatched by DOI, try PMID.
/// Then UNION ALL with the already-matched rows.
pub fn join_pubmed_openalex_pass2() -> String {
    format!(
        "CREATE OR REPLACE TEMP TABLE pm_oa AS
     -- Already matched by DOI
     SELECT * FROM pm_oa_pass1 WHERE openalex_id IS NOT NULL
     UNION ALL
     -- Unmatched: try PMID
     SELECT
       p1.pmid, p1.doi, p1.pmc_id, p1.pii,
       p1.title, p1.vernacular_title, p1.abstract_text,
       p1.language, p1.publication_status,
       p1.journal_title, p1.journal_iso, p1.journal_issn,
       p1.journal_volume, p1.journal_issue, p1.pagination, p1.elocation_id,
       p1.pub_year, p1.pub_month, p1.pub_day,
       p1.date_completed, p1.date_revised,
       p1.authors_json, p1.affiliations_json, p1.collective_name,
       p1.mesh_terms_json, p1.mesh_major_topics, p1.chemicals_json,
       p1.grants_json, p1.publication_types, p1.keywords,
       p1.databanks_json, p1.reference_count,
       p1.coi_statement, p1.copyright_info,
       {OA_COLS_ALIASED_PASS2}
     FROM pm_oa_pass1 p1
     LEFT JOIN (
       SELECT *
       FROM v_openalex
       WHERE pmid IS NOT NULL AND pmid != ''
       QUALIFY ROW_NUMBER() OVER (
         PARTITION BY pmid ORDER BY cited_by_count DESC
       ) = 1
     ) oa2
       ON CAST(p1.pmid AS VARCHAR) = oa2.pmid
       AND p1.pmid IS NOT NULL
     WHERE p1.openalex_id IS NULL"
    )
}

// ── S2 Narrow Key Tables ──

/// Create narrow S2 key tables for DOI and PMID matching.
/// Hash table size: ~30MB instead of ~350MB (full 22 cols).
pub fn create_s2_key_tables() -> &'static str {
    "CREATE TEMP TABLE s2_doi_keys AS
     SELECT corpusid, normalize_doi(doi) AS doi_norm
     FROM v_s2_papers
     WHERE doi IS NOT NULL AND doi != ''
     QUALIFY ROW_NUMBER() OVER (
       PARTITION BY normalize_doi(doi) ORDER BY citationcount DESC
     ) = 1;

     CREATE TEMP TABLE s2_pmid_keys AS
     SELECT corpusid, pubmed
     FROM v_s2_papers
     WHERE pubmed IS NOT NULL AND pubmed != ''
     QUALIFY ROW_NUMBER() OVER (
       PARTITION BY pubmed ORDER BY citationcount DESC
     ) = 1"
}

/// Pass 3: Join PubMed+OA with S2 on DOI (using narrow key table).
pub fn join_s2_doi() -> &'static str {
    "CREATE OR REPLACE TEMP TABLE nodes_pass1 AS
     SELECT
       po.*,
       s2k.corpusid AS s2_corpusid
     FROM pm_oa po
     LEFT JOIN s2_doi_keys s2k
       ON normalize_doi(po.doi) = s2k.doi_norm
       AND po.doi IS NOT NULL
       AND po.doi != ''"
}

/// Pass 4: For S2-unmatched rows, try PMID (using narrow key table).
pub fn join_s2_pmid() -> &'static str {
    "CREATE OR REPLACE TEMP TABLE nodes AS
     -- Already matched by DOI
     SELECT * FROM nodes_pass1 WHERE s2_corpusid IS NOT NULL
     UNION ALL
     -- Unmatched: try PMID
     SELECT
       n1.* EXCLUDE (s2_corpusid),
       s2k.corpusid AS s2_corpusid
     FROM nodes_pass1 n1
     LEFT JOIN s2_pmid_keys s2k
       ON CAST(n1.pmid AS VARCHAR) = s2k.pubmed
       AND n1.pmid IS NOT NULL
     WHERE n1.s2_corpusid IS NULL"
}

/// Enrich matched nodes with full S2 metadata via integer key joins.
pub fn enrich_s2() -> &'static str {
    "-- Aggregate authors per paper
     CREATE TEMP TABLE s2_authors_agg AS
     SELECT corpusid,
       LIST(STRUCT_PACK(
         authorid := authorid,
         name := name
       ) ORDER BY position) AS s2_authors
     FROM v_s2_paper_authors
     WHERE corpusid IN (SELECT s2_corpusid FROM nodes WHERE s2_corpusid IS NOT NULL)
     GROUP BY corpusid;

     -- Aggregate fields per paper
     CREATE TEMP TABLE s2_fields_agg AS
     SELECT corpusid,
       LIST(category ORDER BY category) AS s2_fields
     FROM v_s2_paper_fields
     WHERE corpusid IN (SELECT s2_corpusid FROM nodes WHERE s2_corpusid IS NOT NULL)
     GROUP BY corpusid;

     -- Final enrichment: join all S2 data via integer key
     CREATE OR REPLACE TEMP TABLE nodes_enriched AS
     SELECT n.*,
       -- S2 papers metadata
       s2p.citationcount AS s2_citation_count,
       s2p.influentialcitationcount AS s2_influential_citation_count,
       s2p.venue AS s2_venue,
       s2p.publicationtypes AS s2_publication_types,
       s2p.url AS s2_url,
       s2p.year AS s2_year,
       s2p.isopenaccess AS s2_is_open_access,
       s2p.journal_name AS s2_journal_name,
       s2p.journal_volume AS s2_journal_volume,
       s2p.journal_pages AS s2_journal_pages,
       s2p.publicationdate AS s2_publication_date,
       s2p.referencecount AS s2_reference_count,
       s2p.arxiv AS s2_arxiv,
       s2p.dblp AS s2_dblp,
       s2p.acl AS s2_acl,
       -- S2 text
       s2a.abstract AS s2_abstract,
       s2t.text AS s2_tldr,
       -- S2 aggregated
       s2au.s2_authors,
       s2f.s2_fields
     FROM nodes n
     LEFT JOIN v_s2_papers s2p ON n.s2_corpusid = s2p.corpusid
     LEFT JOIN v_s2_abstracts s2a ON n.s2_corpusid = s2a.corpusid
     LEFT JOIN v_s2_tldrs s2t ON n.s2_corpusid = s2t.corpusid
     LEFT JOIN s2_authors_agg s2au ON n.s2_corpusid = s2au.corpusid
     LEFT JOIN s2_fields_agg s2f ON n.s2_corpusid = s2f.corpusid"
}

/// Export enriched nodes table to parquet (SELECT * for maintainability).
pub fn export_nodes(output_dir: &Path) -> String {
    let out = output_dir.display();
    format!(
        "COPY (SELECT * FROM nodes_enriched) \
         TO '{out}/nodes.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
}

/// Export filtered citations (only edges between nodes in the joined set).
pub fn export_citations(output_dir: &Path) -> String {
    let out = output_dir.display();
    format!(
        "COPY (
           SELECT
             c.citingcorpusid,
             c.citedcorpusid,
             c.isinfluential,
             c.contexts,
             c.intents
           FROM v_s2_citations c
           WHERE c.citingcorpusid IN (SELECT s2_corpusid FROM nodes WHERE s2_corpusid IS NOT NULL)
             AND c.citedcorpusid IN (SELECT s2_corpusid FROM nodes WHERE s2_corpusid IS NOT NULL)
         ) TO '{out}/citations.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
}

/// Query to get join summary statistics.
pub fn summary_query() -> &'static str {
    "SELECT
       COUNT(*) AS total,
       COUNT(openalex_id) AS oa_matched,
       COUNT(s2_corpusid) AS s2_matched
     FROM nodes"
}

/// Count OA DOI matches after pass 1.
pub fn count_oa_doi() -> &'static str {
    "SELECT COUNT(openalex_id) FROM pm_oa_pass1"
}

/// Count S2 DOI matches after pass 3.
pub fn count_s2_doi() -> &'static str {
    "SELECT COUNT(s2_corpusid) FROM nodes_pass1"
}

/// Count exported citations.
pub fn count_citations(output_dir: &std::path::Path) -> String {
    let out = output_dir.display();
    format!("SELECT COUNT(*) FROM read_parquet('{out}/citations.parquet')")
}

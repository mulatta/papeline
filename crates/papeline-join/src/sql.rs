//! SQL generation for the multi-source join pipeline.
//!
//! Strategy: 2-pass hash join to avoid nested-loop from OR conditions.
//! Pass 1: DOI match (highest reliability)
//! Pass 2: PMID/PMCID fallback for unmatched rows

use std::path::Path;

/// Returns the SQL to create the normalize_doi macro.
pub fn create_normalize_doi_macro() -> &'static str {
    "CREATE OR REPLACE MACRO normalize_doi(d) AS \
     REGEXP_REPLACE(LOWER(d), '^https?://doi\\.org/', '')"
}

/// Returns SQL to create views over the source parquet directories.
pub fn create_source_views(pubmed_dir: &Path, openalex_dir: &Path, s2_dir: &Path) -> Vec<String> {
    let pm = pubmed_dir.display();
    let oa = openalex_dir.display();
    let s2 = s2_dir.display();

    vec![
        format!(
            "CREATE OR REPLACE VIEW v_pubmed AS \
             SELECT * FROM read_parquet('{pm}/articles_*.parquet')"
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
    ]
}

/// Pass 1: Join PubMed with OpenAlex on DOI.
pub fn join_pubmed_openalex_pass1() -> &'static str {
    "CREATE OR REPLACE TEMP TABLE pm_oa_pass1 AS
     SELECT
       pm.*,
       oa.id AS openalex_id,
       oa.cited_by_count AS oa_cited_by_count,
       oa.referenced_works_count AS oa_referenced_works_count,
       oa.is_oa,
       oa.oa_status,
       oa.type AS oa_work_type,
       oa.primary_topic_id,
       oa.primary_topic_display_name
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
}

/// Pass 2: For rows unmatched by DOI, try PMID.
/// Then UNION ALL with the already-matched rows.
pub fn join_pubmed_openalex_pass2() -> &'static str {
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
       oa2.id AS openalex_id,
       oa2.cited_by_count AS oa_cited_by_count,
       oa2.referenced_works_count AS oa_referenced_works_count,
       oa2.is_oa,
       oa2.oa_status,
       oa2.type AS oa_work_type,
       oa2.primary_topic_id,
       oa2.primary_topic_display_name
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
}

/// Pass 3: Join the PubMed+OA result with S2 on DOI.
pub fn join_s2_pass1() -> &'static str {
    "CREATE OR REPLACE TEMP TABLE nodes_pass1 AS
     SELECT
       po.*,
       s2.corpusid AS s2_corpusid,
       s2.citationcount AS s2_citation_count,
       s2.influentialcitationcount AS s2_influential_citation_count,
       s2.venue AS s2_venue,
       s2.publicationtypes AS s2_publication_types
     FROM pm_oa po
     LEFT JOIN (
       SELECT *
       FROM v_s2_papers
       WHERE doi IS NOT NULL AND doi != ''
       QUALIFY ROW_NUMBER() OVER (
         PARTITION BY normalize_doi(doi) ORDER BY citationcount DESC
       ) = 1
     ) s2
       ON normalize_doi(po.doi) = normalize_doi(s2.doi)
       AND po.doi IS NOT NULL
       AND po.doi != ''"
}

/// Pass 4: For S2-unmatched rows, try PMID.
pub fn join_s2_pass2() -> &'static str {
    "CREATE OR REPLACE TEMP TABLE nodes AS
     -- Already matched by DOI
     SELECT * FROM nodes_pass1 WHERE s2_corpusid IS NOT NULL
     UNION ALL
     -- Unmatched: try PMID
     SELECT
       n1.pmid, n1.doi, n1.pmc_id, n1.pii,
       n1.title, n1.vernacular_title, n1.abstract_text,
       n1.language, n1.publication_status,
       n1.journal_title, n1.journal_iso, n1.journal_issn,
       n1.journal_volume, n1.journal_issue, n1.pagination, n1.elocation_id,
       n1.pub_year, n1.pub_month, n1.pub_day,
       n1.date_completed, n1.date_revised,
       n1.authors_json, n1.affiliations_json, n1.collective_name,
       n1.mesh_terms_json, n1.mesh_major_topics, n1.chemicals_json,
       n1.grants_json, n1.publication_types, n1.keywords,
       n1.databanks_json, n1.reference_count,
       n1.coi_statement, n1.copyright_info,
       n1.openalex_id,
       n1.oa_cited_by_count, n1.oa_referenced_works_count,
       n1.is_oa, n1.oa_status, n1.oa_work_type,
       n1.primary_topic_id, n1.primary_topic_display_name,
       s2b.corpusid AS s2_corpusid,
       s2b.citationcount AS s2_citation_count,
       s2b.influentialcitationcount AS s2_influential_citation_count,
       s2b.venue AS s2_venue,
       s2b.publicationtypes AS s2_publication_types
     FROM nodes_pass1 n1
     LEFT JOIN (
       SELECT *
       FROM v_s2_papers
       WHERE pubmed IS NOT NULL AND pubmed != ''
       QUALIFY ROW_NUMBER() OVER (
         PARTITION BY pubmed ORDER BY citationcount DESC
       ) = 1
     ) s2b
       ON CAST(n1.pmid AS VARCHAR) = s2b.pubmed
       AND n1.pmid IS NOT NULL
     WHERE n1.s2_corpusid IS NULL"
}

/// Export nodes table to parquet.
pub fn export_nodes(output_dir: &Path) -> String {
    let out = output_dir.display();
    format!(
        "COPY (
           SELECT
             pmid, doi, pmc_id,
             openalex_id, s2_corpusid,
             title, abstract_text,
             pub_year, pub_month, pub_day,
             journal_title, journal_issn,
             mesh_terms_json, mesh_major_topics, keywords, publication_types,
             authors_json, affiliations_json,
             reference_count, grants_json, language,
             oa_cited_by_count, oa_referenced_works_count,
             is_oa, oa_status, oa_work_type,
             primary_topic_id, primary_topic_display_name,
             s2_citation_count, s2_influential_citation_count,
             s2_venue, s2_publication_types
           FROM nodes
         ) TO '{out}/nodes.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)"
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

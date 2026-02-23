#![allow(dead_code)]

use std::path::Path;

use duckdb::Connection;
use tempfile::TempDir;

// Column name constants to avoid repeating schemas in test helpers
const PM_COLS: &str = "\
    pmid, doi, pmc_id, pii, title, vernacular_title, abstract_text, \
    language, publication_status, journal_title, journal_iso, journal_issn, \
    journal_volume, journal_issue, pagination, elocation_id, \
    pub_year, pub_month, pub_day, date_completed, date_revised, \
    authors_json, affiliations_json, collective_name, \
    mesh_terms_json, mesh_major_topics, chemicals_json, \
    grants_json, publication_types, keywords, \
    databanks_json, reference_count, coi_statement, copyright_info";

const OA_COLS: &str = "\
    id, doi, pmid, pmcid, mag, \
    title, display_name, publication_date, publication_year, language, type, \
    cited_by_count, is_oa, oa_status, abstract_text, \
    author_ids, institution_ids, primary_topic_id, primary_topic_display_name, \
    source_id, source_display_name, source_type, referenced_works_count, \
    created_date, updated_date";

const S2_COLS: &str = "\
    corpusid, title, url, venue, publicationvenueid, year, \
    referencecount, citationcount, influentialcitationcount, \
    isopenaccess, publicationdate, publicationtypes, \
    journal_name, journal_pages, journal_volume, \
    doi, pubmed, arxiv, mag, acl, dblp, pubmedcentral";

const CIT_COLS: &str =
    "citationid, citingcorpusid, citedcorpusid, isinfluential, contexts, intents";

const S2_ABSTRACT_COLS: &str = "corpusid, abstract";

const S2_TLDR_COLS: &str = "corpusid, text";

const S2_PAPER_AUTHORS_COLS: &str = "corpusid, authorid, name, position";

const S2_PAPER_FIELDS_COLS: &str = "corpusid, category, source";

/// Write a parquet file from VALUES SQL using DuckDB.
fn write_parquet(conn: &Connection, dir: &Path, filename: &str, cols: &str, values: &str) {
    std::fs::create_dir_all(dir).unwrap();
    conn.execute_batch(&format!(
        "COPY (SELECT * FROM (VALUES {values}) AS t({cols})) \
         TO '{}/{filename}' (FORMAT PARQUET)",
        dir.display()
    ))
    .unwrap();
}

/// Create a standard PubMed row VALUES fragment.
/// Only pmid, doi, title vary; rest are defaults.
fn pm_row(pmid: i32, doi: &str, title: &str) -> String {
    let doi_val = if doi.is_empty() {
        "NULL".to_string()
    } else {
        format!("'{doi}'")
    };
    format!(
        "({pmid}, {doi_val}, NULL, NULL, '{title}', NULL, 'Abstract', \
         'eng', 'ppublish', 'J1', 'J1', '1234-5678', '1', '1', '1-10', NULL, \
         2024, 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
         'Article'::VARCHAR, NULL, NULL, 0, NULL, NULL)"
    )
}

/// Create a standard OpenAlex row VALUES fragment.
fn oa_row(id: &str, doi: &str, pmid: &str, cited_by_count: i32) -> String {
    let doi_val = if doi.is_empty() {
        "NULL".to_string()
    } else {
        format!("'{doi}'")
    };
    let pmid_val = if pmid.is_empty() {
        "NULL".to_string()
    } else {
        format!("'{pmid}'")
    };
    format!(
        "('{id}', {doi_val}, {pmid_val}, NULL, NULL, \
         'Title', 'Title', '2024-01-01', 2024, 'en', 'article', \
         {cited_by_count}, true, 'gold', 'Abstract', \
         NULL, NULL, 'T1', 'Topic', NULL, NULL, NULL, 5, \
         '2024-01-01', '2024-01-01')"
    )
}

/// Create a standard S2 papers row VALUES fragment.
fn s2_row(corpusid: i32, doi: &str, pubmed: &str, citationcount: i32) -> String {
    let doi_val = if doi.is_empty() {
        "NULL".to_string()
    } else {
        format!("'{doi}'")
    };
    let pm_val = if pubmed.is_empty() {
        "NULL".to_string()
    } else {
        format!("'{pubmed}'")
    };
    format!(
        "({corpusid}, 'Title', 'http://s2.com/{corpusid}', 'Venue', NULL, 2024, \
         0, {citationcount}, 0, true, '2024-01-01', 'JournalArticle'::VARCHAR, \
         NULL, NULL, NULL, \
         {doi_val}, {pm_val}, NULL, NULL, NULL, NULL, NULL)"
    )
}

/// Create a standard S2 citation row VALUES fragment.
fn cit_row(id: i32, citing: i32, cited: i32) -> String {
    format!("({id}, {citing}, {cited}, false, NULL, NULL)")
}

/// Helper: create test parquet files with known data using DuckDB.
fn create_test_data(dir: &Path) {
    let conn = Connection::open_in_memory().unwrap();

    let pm_dir = dir.join("pubmed");
    let oa_dir = dir.join("openalex");
    let s2_dir = dir.join("s2");
    std::fs::create_dir_all(&pm_dir).unwrap();
    std::fs::create_dir_all(&oa_dir).unwrap();
    std::fs::create_dir_all(&s2_dir).unwrap();

    // PubMed articles
    conn.execute_batch(&format!(
        "COPY (
           SELECT * FROM (VALUES
             (1, '10.1234/a', 'PMC001', NULL, 'Paper A', NULL, 'Abstract A',
              'eng', 'ppublish', 'J1', 'J1', '1234-5678', '1', '1', '1-10', NULL,
              2024, 1, 15, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
              'Article'::VARCHAR, NULL, NULL, 5, NULL, NULL),
             (2, '10.1234/b', NULL, NULL, 'Paper B', NULL, 'Abstract B',
              'eng', 'ppublish', 'J2', 'J2', '2345-6789', '2', '1', '11-20', NULL,
              2024, 2, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
              'Article'::VARCHAR, NULL, NULL, 3, NULL, NULL),
             (3, NULL, NULL, NULL, 'Paper C', NULL, 'Abstract C',
              'eng', 'ppublish', 'J1', 'J1', '1234-5678', '1', '2', '21-30', NULL,
              2024, 3, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
              'Article'::VARCHAR, NULL, NULL, 0, NULL, NULL),
             (4, '10.1234/d', NULL, NULL, 'Paper D', NULL, 'Abstract D',
              'eng', 'ppublish', 'J3', 'J3', '3456-7890', '5', '1', '1-5', NULL,
              2024, 4, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
              'Review'::VARCHAR, NULL, NULL, 10, NULL, NULL)
           ) AS t(
             pmid, doi, pmc_id, pii, title, vernacular_title, abstract_text,
             language, publication_status, journal_title, journal_iso, journal_issn,
             journal_volume, journal_issue, pagination, elocation_id,
             pub_year, pub_month, pub_day, date_completed, date_revised,
             authors_json, affiliations_json, collective_name,
             mesh_terms_json, mesh_major_topics, chemicals_json,
             grants_json, publication_types, keywords,
             databanks_json, reference_count, coi_statement, copyright_info
           )
         ) TO '{}/pubmed_0000.parquet' (FORMAT PARQUET)",
        pm_dir.display()
    ))
    .unwrap();

    // OpenAlex works
    conn.execute_batch(&format!(
        "COPY (
           SELECT * FROM (VALUES
             ('W1', '10.1234/a', '1', NULL, NULL,
              'Paper A OA', 'Paper A OA', '2024-01-15', 2024, 'en', 'article',
              50, true, 'gold', 'Abstract A OA',
              NULL, NULL, 'T1', 'Topic 1', NULL, NULL, NULL, 10,
              '2024-01-01', '2024-01-15'),
             ('W2', NULL, '3', NULL, NULL,
              'Paper C OA', 'Paper C OA', '2024-03-01', 2024, 'en', 'article',
              20, false, 'closed', 'Abstract C OA',
              NULL, NULL, 'T2', 'Topic 2', NULL, NULL, NULL, 5,
              '2024-03-01', '2024-03-15'),
             ('W3', '10.1234/x', NULL, NULL, NULL,
              'Unrelated OA', 'Unrelated OA', '2024-05-01', 2024, 'en', 'article',
              5, true, 'green', 'Abstract X',
              NULL, NULL, 'T3', 'Topic 3', NULL, NULL, NULL, 2,
              '2024-05-01', '2024-05-15')
           ) AS t(
             id, doi, pmid, pmcid, mag,
             title, display_name, publication_date, publication_year, language, type,
             cited_by_count, is_oa, oa_status, abstract_text,
             author_ids, institution_ids, primary_topic_id, primary_topic_display_name,
             source_id, source_display_name, source_type, referenced_works_count,
             created_date, updated_date
           )
         ) TO '{}/works_0000.parquet' (FORMAT PARQUET)",
        oa_dir.display()
    ))
    .unwrap();

    // S2 papers
    conn.execute_batch(&format!(
        "COPY (
           SELECT * FROM (VALUES
             (100, 'Paper A S2', 'http://s2.com/100', 'NeurIPS', NULL, 2024,
              5, 50, 10, true, '2024-01-15', 'JournalArticle'::VARCHAR,
              NULL, NULL, NULL,
              '10.1234/a', '1', NULL, NULL, NULL, NULL, NULL),
             (200, 'Paper B S2', 'http://s2.com/200', 'ICML', NULL, 2024,
              3, 30, 5, false, '2024-02-01', 'JournalArticle'::VARCHAR,
              NULL, NULL, NULL,
              '10.1234/b', '2', NULL, NULL, NULL, NULL, NULL),
             (300, 'Paper D S2', 'http://s2.com/300', 'Nature', NULL, 2024,
              10, 100, 20, true, '2024-04-01', 'Review'::VARCHAR,
              NULL, NULL, NULL,
              NULL, '4', NULL, NULL, NULL, NULL, NULL),
             (400, 'Unrelated S2', 'http://s2.com/400', 'ArXiv', NULL, 2024,
              0, 0, 0, true, '2024-06-01', 'Conference'::VARCHAR,
              NULL, NULL, NULL,
              '10.1234/y', NULL, NULL, NULL, NULL, NULL, NULL)
           ) AS t(
             corpusid, title, url, venue, publicationvenueid, year,
             referencecount, citationcount, influentialcitationcount,
             isopenaccess, publicationdate, publicationtypes,
             journal_name, journal_pages, journal_volume,
             doi, pubmed, arxiv, mag, acl, dblp, pubmedcentral
           )
         ) TO '{}/papers_0000.parquet' (FORMAT PARQUET)",
        s2_dir.display()
    ))
    .unwrap();

    // S2 citations
    conn.execute_batch(&format!(
        "COPY (
           SELECT * FROM (VALUES
             (1001, 100, 200, true,  NULL, NULL),
             (1002, 200, 100, false, NULL, NULL),
             (1003, 100, 400, false, NULL, NULL),
             (1004, 400, 200, false, NULL, NULL)
           ) AS t(citationid, citingcorpusid, citedcorpusid, isinfluential, contexts, intents)
         ) TO '{}/citations_0000.parquet' (FORMAT PARQUET)",
        s2_dir.display()
    ))
    .unwrap();

    // S2 abstracts
    write_parquet(
        &conn,
        &s2_dir,
        "abstracts_0000.parquet",
        S2_ABSTRACT_COLS,
        "(100, 'S2 abstract for paper A'), (200, 'S2 abstract for paper B'), \
         (300, 'S2 abstract for paper D'), (400, 'S2 abstract unrelated')",
    );

    // S2 TLDRs
    write_parquet(
        &conn,
        &s2_dir,
        "tldrs_0000.parquet",
        S2_TLDR_COLS,
        "(100, 'TLDR for paper A'), (200, 'TLDR for paper B')",
    );

    // S2 paper_authors
    write_parquet(
        &conn,
        &s2_dir,
        "paper_authors_0000.parquet",
        S2_PAPER_AUTHORS_COLS,
        "(100, 1001, 'Alice', 0), (100, 1002, 'Bob', 1), \
         (200, 1003, 'Charlie', 0), \
         (300, 1004, 'Diana', 0), (300, 1005, 'Eve', 1)",
    );

    // S2 paper_fields
    write_parquet(
        &conn,
        &s2_dir,
        "paper_fields_0000.parquet",
        S2_PAPER_FIELDS_COLS,
        "(100, 'Computer Science', 's2ag'), (100, 'Biology', 's2ag'), \
         (200, 'Mathematics', 's2ag'), \
         (300, 'Medicine', 's2ag')",
    );
}

#[test]
fn test_join_pipeline() {
    let _ = env_logger::builder().is_test(true).try_init();

    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    create_test_data(&data_dir);

    let output_dir = tmp.path().join("output");
    let config = papeline_join::JoinConfig {
        pubmed_dir: data_dir.join("pubmed"),
        openalex_dir: data_dir.join("openalex"),
        s2_dir: data_dir.join("s2"),
        output_dir: output_dir.clone(),
        memory_limit: "256MB".to_string(),
    };

    let summary = papeline_join::run(&config).unwrap();

    // 4 PubMed articles total
    assert_eq!(summary.total_nodes, 4, "should have 4 PubMed-based nodes");

    // OA matches:
    //   Paper A: DOI match (10.1234/a)
    //   Paper C: PMID fallback (pmid=3)
    //   Paper B: no OA match (DOI 10.1234/b not in OA, PMID 2 not in OA)
    //   Paper D: no OA match (DOI 10.1234/d not in OA, PMID 4 not in OA)
    assert_eq!(
        summary.openalex_matched, 2,
        "should match 2 OA records (DOI + PMID)"
    );

    // S2 matches:
    //   Paper A: DOI match (10.1234/a) -> corpusid=100
    //   Paper B: DOI match (10.1234/b) -> corpusid=200
    //   Paper D: PMID fallback (pmid=4) -> corpusid=300
    //   Paper C: no S2 match
    assert_eq!(summary.s2_matched, 3, "should match 3 S2 records");

    // Verify output files exist
    assert!(output_dir.join("nodes.parquet").exists());
    assert!(output_dir.join("citations.parquet").exists());

    // Verify citation filtering: only edges where BOTH sides are in nodes
    // Nodes have s2_corpusid: {100, 200, 300}
    // Citation (100->200): both in nodes -> included
    // Citation (200->100): both in nodes -> included
    // Citation (100->400): 400 not in nodes -> excluded
    // Citation (400->200): 400 not in nodes -> excluded
    let conn = Connection::open_in_memory().unwrap();
    let citation_count: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM read_parquet('{}/citations.parquet')",
                output_dir.display()
            ),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(citation_count, 2, "should have 2 filtered citations");

    // Verify node schema has expected columns (including new enriched ones)
    let node_cols: Vec<String> = {
        let mut stmt = conn
            .prepare(&format!(
                "SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{}/nodes.parquet'))",
                output_dir.display()
            ))
            .unwrap();
        let rows = stmt.query_map([], |row| row.get::<_, String>(0)).unwrap();
        rows.filter_map(|r| r.ok()).collect()
    };

    let expected = [
        // PubMed base
        "pmid",
        "doi",
        "pmc_id",
        "title",
        "abstract_text",
        // OA base
        "openalex_id",
        "oa_cited_by_count",
        "oa_referenced_works_count",
        "is_oa",
        "oa_status",
        "oa_work_type",
        "primary_topic_id",
        "primary_topic_display_name",
        // OA expanded
        "oa_abstract",
        "oa_publication_date",
        "oa_publication_year",
        "oa_language",
        "oa_display_name",
        "oa_author_ids",
        "oa_institution_ids",
        "oa_source_id",
        "oa_source_display_name",
        "oa_source_type",
        "oa_mag",
        "oa_created_date",
        "oa_updated_date",
        // S2 match
        "s2_corpusid",
        // S2 enriched
        "s2_citation_count",
        "s2_influential_citation_count",
        "s2_venue",
        "s2_publication_types",
        "s2_url",
        "s2_year",
        "s2_is_open_access",
        "s2_publication_date",
        "s2_reference_count",
        "s2_abstract",
        "s2_tldr",
        "s2_authors",
        "s2_fields",
    ];
    for col in expected {
        assert!(
            node_cols.iter().any(|c| c == col),
            "missing column: {col}, got: {node_cols:?}"
        );
    }

    // Verify S2 abstract and TLDR enrichment for Paper A (corpusid=100)
    let (s2_abstract, s2_tldr): (Option<String>, Option<String>) = conn
        .query_row(
            &format!(
                "SELECT s2_abstract, s2_tldr FROM read_parquet('{}/nodes.parquet') WHERE pmid = 1",
                output_dir.display()
            ),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(s2_abstract.as_deref(), Some("S2 abstract for paper A"));
    assert_eq!(s2_tldr.as_deref(), Some("TLDR for paper A"));
}

/// DOI normalization: prefix removal (https://doi.org/, HTTP://DOI.ORG/) and case folding.
///
/// Verifies that DOIs in different formats all resolve to the same canonical form:
///   PubMed:  "https://doi.org/10.5555/UPPER-Case"
///   OA:      "10.5555/upper-case"        (bare, lowercase)
///   S2:      "HTTPS://DOI.ORG/10.5555/Upper-Case"  (uppercase prefix, mixed case)
#[test]
fn test_doi_normalization() {
    let _ = env_logger::builder().is_test(true).try_init();

    let tmp = TempDir::new().unwrap();
    let data = tmp.path().join("data");
    let conn = Connection::open_in_memory().unwrap();

    let pm_dir = data.join("pubmed");
    let oa_dir = data.join("openalex");
    let s2_dir = data.join("s2");

    // PubMed: DOI with https://doi.org/ prefix and uppercase path
    write_parquet(
        &conn,
        &pm_dir,
        "pubmed_0000.parquet",
        PM_COLS,
        &pm_row(10, "https://doi.org/10.5555/UPPER-Case", "Norm Paper"),
    );

    // OA: bare lowercase DOI
    write_parquet(
        &conn,
        &oa_dir,
        "works_0000.parquet",
        OA_COLS,
        &oa_row("W10", "10.5555/upper-case", "", 42),
    );

    // S2: HTTPS://DOI.ORG/ prefix with mixed case
    write_parquet(
        &conn,
        &s2_dir,
        "papers_0000.parquet",
        S2_COLS,
        &s2_row(1000, "HTTPS://DOI.ORG/10.5555/Upper-Case", "", 99),
    );
    write_parquet(
        &conn,
        &s2_dir,
        "citations_0000.parquet",
        CIT_COLS,
        &cit_row(5001, 1000, 1000),
    );

    let output = tmp.path().join("output");
    let summary = papeline_join::run(&papeline_join::JoinConfig {
        pubmed_dir: pm_dir,
        openalex_dir: oa_dir,
        s2_dir,
        output_dir: output.clone(),
        memory_limit: "256MB".to_string(),
    })
    .unwrap();

    assert_eq!(summary.total_nodes, 1);
    assert_eq!(
        summary.openalex_matched, 1,
        "OA should match via normalized DOI"
    );
    assert_eq!(summary.s2_matched, 1, "S2 should match via normalized DOI");

    // Verify the matched IDs are correct
    let verify = Connection::open_in_memory().unwrap();
    let (oa_id, s2_id): (String, i64) = verify
        .query_row(
            &format!(
                "SELECT openalex_id, s2_corpusid \
                 FROM read_parquet('{}/nodes.parquet')",
                output.display()
            ),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(oa_id, "W10");
    assert_eq!(s2_id, 1000);

    // Self-citation should survive filtering
    let cit_count: i64 = verify
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM read_parquet('{}/citations.parquet')",
                output.display()
            ),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(cit_count, 1);
}

/// Duplicate matching: multiple OA/S2 records with the same DOI.
///
/// Verifies that dedup picks highest-metric record and produces exactly 1 node,
/// not N (where N = number of duplicates on the right side of the LEFT JOIN).
#[test]
fn test_duplicate_matching() {
    let _ = env_logger::builder().is_test(true).try_init();

    let tmp = TempDir::new().unwrap();
    let data = tmp.path().join("data");
    let conn = Connection::open_in_memory().unwrap();

    let pm_dir = data.join("pubmed");
    let oa_dir = data.join("openalex");
    let s2_dir = data.join("s2");

    // 1 PubMed record
    write_parquet(
        &conn,
        &pm_dir,
        "pubmed_0000.parquet",
        PM_COLS,
        &pm_row(20, "10.9999/dup", "Dup Paper"),
    );

    // 2 OA records with same DOI, different cited_by_count
    write_parquet(
        &conn,
        &oa_dir,
        "works_0000.parquet",
        OA_COLS,
        &format!(
            "{}, {}",
            oa_row("WD1", "10.9999/dup", "", 100), // winner: highest count
            oa_row("WD2", "10.9999/dup", "", 50),
        ),
    );

    // 2 S2 records with same DOI, different citationcount
    write_parquet(
        &conn,
        &s2_dir,
        "papers_0000.parquet",
        S2_COLS,
        &format!(
            "{}, {}",
            s2_row(2001, "10.9999/dup", "", 500), // winner: highest count
            s2_row(2002, "10.9999/dup", "", 200),
        ),
    );

    // Dummy citation (self-cite on winner)
    write_parquet(
        &conn,
        &s2_dir,
        "citations_0000.parquet",
        CIT_COLS,
        &cit_row(9001, 2001, 2001),
    );

    let output = tmp.path().join("output");
    let summary = papeline_join::run(&papeline_join::JoinConfig {
        pubmed_dir: pm_dir,
        openalex_dir: oa_dir,
        s2_dir,
        output_dir: output.clone(),
        memory_limit: "256MB".to_string(),
    })
    .unwrap();

    // Must be exactly 1 node, NOT 2 or 4
    assert_eq!(
        summary.total_nodes, 1,
        "duplicate DOIs must not multiply rows"
    );
    assert_eq!(summary.openalex_matched, 1);
    assert_eq!(summary.s2_matched, 1);

    // Verify the winner is the highest-metric record
    let verify = Connection::open_in_memory().unwrap();
    let (oa_id, s2_id, oa_cited): (String, i64, i64) = verify
        .query_row(
            &format!(
                "SELECT openalex_id, s2_corpusid, oa_cited_by_count \
                 FROM read_parquet('{}/nodes.parquet')",
                output.display()
            ),
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        oa_id, "WD1",
        "should pick OA record with highest cited_by_count"
    );
    assert_eq!(
        s2_id, 2001,
        "should pick S2 record with highest citationcount"
    );
    assert_eq!(oa_cited, 100);
}

/// Empty directory: no matching parquet files should produce a clear error.
#[test]
fn test_empty_directory() {
    let _ = env_logger::builder().is_test(true).try_init();

    let tmp = TempDir::new().unwrap();
    let pm_dir = tmp.path().join("pubmed");
    let oa_dir = tmp.path().join("openalex");
    let s2_dir = tmp.path().join("s2");

    // Create empty directories (no parquet files)
    std::fs::create_dir_all(&pm_dir).unwrap();
    std::fs::create_dir_all(&oa_dir).unwrap();
    std::fs::create_dir_all(&s2_dir).unwrap();

    let output = tmp.path().join("output");
    let result = papeline_join::run(&papeline_join::JoinConfig {
        pubmed_dir: pm_dir,
        openalex_dir: oa_dir,
        s2_dir,
        output_dir: output,
        memory_limit: "256MB".to_string(),
    });

    assert!(
        result.is_err(),
        "should fail when source directories have no parquet files"
    );
    let err_msg = format!("{:#}", result.unwrap_err());
    assert!(
        err_msg.contains("No files found") || err_msg.contains("parquet"),
        "error should mention missing files, got: {err_msg}"
    );
}

/// Missing optional S2 files: abstracts/tldrs/paper_authors/paper_fields.
/// Join should succeed with those enrichment columns as NULL.
#[test]
fn test_missing_optional_s2_files() {
    let _ = env_logger::builder().is_test(true).try_init();

    let tmp = TempDir::new().unwrap();
    let data = tmp.path().join("data");
    let conn = Connection::open_in_memory().unwrap();

    let pm_dir = data.join("pubmed");
    let oa_dir = data.join("openalex");
    let s2_dir = data.join("s2");

    // Minimal data: 1 PM, 1 OA, 1 S2 papers + citations only (no abstracts/tldrs/authors/fields)
    write_parquet(
        &conn,
        &pm_dir,
        "pubmed_0000.parquet",
        PM_COLS,
        &pm_row(1, "10.1234/opt", "Optional Test"),
    );
    write_parquet(
        &conn,
        &oa_dir,
        "works_0000.parquet",
        OA_COLS,
        &oa_row("W1", "10.1234/opt", "", 10),
    );
    write_parquet(
        &conn,
        &s2_dir,
        "papers_0000.parquet",
        S2_COLS,
        &s2_row(500, "10.1234/opt", "", 25),
    );
    write_parquet(
        &conn,
        &s2_dir,
        "citations_0000.parquet",
        CIT_COLS,
        &cit_row(9999, 500, 500),
    );

    let output = tmp.path().join("output");
    let summary = papeline_join::run(&papeline_join::JoinConfig {
        pubmed_dir: pm_dir,
        openalex_dir: oa_dir,
        s2_dir,
        output_dir: output.clone(),
        memory_limit: "256MB".to_string(),
    })
    .unwrap();

    assert_eq!(summary.total_nodes, 1);
    assert_eq!(summary.s2_matched, 1);

    // Verify enrichment columns exist but are NULL
    let verify = Connection::open_in_memory().unwrap();
    let (s2_abstract, s2_tldr, has_authors_col, has_fields_col): (
        Option<String>,
        Option<String>,
        bool,
        bool,
    ) = verify
        .query_row(
            &format!(
                "SELECT s2_abstract, s2_tldr, \
                 s2_authors IS NULL, s2_fields IS NULL \
                 FROM read_parquet('{}/nodes.parquet')",
                output.display()
            ),
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert!(s2_abstract.is_none(), "s2_abstract should be NULL");
    assert!(s2_tldr.is_none(), "s2_tldr should be NULL");
    assert!(has_authors_col, "s2_authors should be NULL");
    assert!(has_fields_col, "s2_fields should be NULL");
}

/// S2 authors are correctly aggregated as LIST<STRUCT>.
#[test]
fn test_s2_authors_aggregation() {
    let _ = env_logger::builder().is_test(true).try_init();

    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    create_test_data(&data_dir);

    let output_dir = tmp.path().join("output");
    let config = papeline_join::JoinConfig {
        pubmed_dir: data_dir.join("pubmed"),
        openalex_dir: data_dir.join("openalex"),
        s2_dir: data_dir.join("s2"),
        output_dir: output_dir.clone(),
        memory_limit: "256MB".to_string(),
    };

    papeline_join::run(&config).unwrap();

    let conn = Connection::open_in_memory().unwrap();

    // Paper A (pmid=1, corpusid=100) should have 2 authors: Alice (pos 0), Bob (pos 1)
    let author_count: i64 = conn
        .query_row(
            &format!(
                "SELECT LEN(s2_authors) FROM read_parquet('{}/nodes.parquet') WHERE pmid = 1",
                output_dir.display()
            ),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(author_count, 2, "Paper A should have 2 S2 authors");

    // Verify ordering: first author should be Alice (position 0)
    let first_author: String = conn
        .query_row(
            &format!(
                "SELECT s2_authors[1].name FROM read_parquet('{}/nodes.parquet') WHERE pmid = 1",
                output_dir.display()
            ),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(first_author, "Alice");

    // Paper A should have 2 fields: Biology, Computer Science (sorted)
    let field_count: i64 = conn
        .query_row(
            &format!(
                "SELECT LEN(s2_fields) FROM read_parquet('{}/nodes.parquet') WHERE pmid = 1",
                output_dir.display()
            ),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(field_count, 2, "Paper A should have 2 S2 fields");

    // Paper C (pmid=3) has no S2 match -> s2_authors should be NULL
    let unmatched_authors: Option<String> = conn
        .query_row(
            &format!(
                "SELECT CAST(s2_authors AS VARCHAR) FROM read_parquet('{}/nodes.parquet') WHERE pmid = 3",
                output_dir.display()
            ),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        unmatched_authors.is_none(),
        "unmatched paper should have NULL s2_authors"
    );
}

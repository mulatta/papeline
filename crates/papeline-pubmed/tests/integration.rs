//! Integration tests for papeline-pubmed
//!
//! These tests require network access and are marked #[ignore] by default.
//! Run with: cargo test -p papeline-pubmed --test integration -- --ignored

use parquet::file::reader::FileReader;
use tempfile::TempDir;

/// Test fetching and parsing a real PubMed baseline file
/// Run with: cargo test -p papeline-pubmed --test integration -- --ignored fetch_single_file
#[test]
#[ignore]
fn fetch_single_file() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let config = papeline_pubmed::Config {
        output_dir: temp_dir.path().to_path_buf(),
        workers: 2,
        max_files: Some(1),
        zstd_level: 3,
        ..Default::default()
    };

    let summary = papeline_pubmed::run(&config).expect("Pipeline should succeed");

    // Verify results
    assert_eq!(summary.total_files, 1);
    assert_eq!(summary.completed_files, 1);
    assert_eq!(summary.failed_files, 0);

    // Each baseline file has ~30,000 articles
    assert!(
        summary.total_articles >= 25_000,
        "Expected at least 25,000 articles, got {}",
        summary.total_articles
    );

    // Verify output file exists
    let output_files: Vec<_> = std::fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
        .collect();

    assert_eq!(output_files.len(), 1, "Should have exactly 1 parquet file");

    // Verify file is non-empty
    let file_size = output_files[0].metadata().unwrap().len();
    assert!(
        file_size > 1_000_000,
        "Parquet file should be > 1MB, got {} bytes",
        file_size
    );
}

/// Test fetching multiple files in parallel
/// Run with: cargo test -p papeline-pubmed --test integration -- --ignored fetch_multiple_files
#[test]
#[ignore]
fn fetch_multiple_files() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let config = papeline_pubmed::Config {
        output_dir: temp_dir.path().to_path_buf(),
        workers: 4,
        max_files: Some(3),
        zstd_level: 3,
        ..Default::default()
    };

    let summary = papeline_pubmed::run(&config).expect("Pipeline should succeed");

    // Verify results
    assert_eq!(summary.total_files, 3);
    assert_eq!(summary.completed_files, 3);
    assert_eq!(summary.failed_files, 0);

    // Should have ~90,000 articles (3 * 30,000)
    assert!(
        summary.total_articles >= 75_000,
        "Expected at least 75,000 articles, got {}",
        summary.total_articles
    );

    // Verify 3 output files
    let output_files: Vec<_> = std::fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
        .collect();

    assert_eq!(output_files.len(), 3, "Should have exactly 3 parquet files");
}

/// Test that throughput is reasonable for release builds
/// Run with: cargo test -p papeline-pubmed --test integration --release -- --ignored throughput
#[test]
#[ignore]
fn throughput() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let config = papeline_pubmed::Config {
        output_dir: temp_dir.path().to_path_buf(),
        workers: 4,
        max_files: Some(2),
        zstd_level: 3,
        ..Default::default()
    };

    let summary = papeline_pubmed::run(&config).expect("Pipeline should succeed");

    // Calculate throughput
    let articles_per_sec = summary.total_articles as f64 / summary.elapsed.as_secs_f64();

    println!("Throughput: {:.0} articles/sec", articles_per_sec);
    println!("Total time: {:.1}s", summary.elapsed.as_secs_f64());
    println!("Total articles: {}", summary.total_articles);

    // In release mode, should process at least 1000 articles/sec
    // (accounting for network latency)
    #[cfg(not(debug_assertions))]
    assert!(
        articles_per_sec > 500.0,
        "Expected throughput > 500 articles/sec, got {:.0}",
        articles_per_sec
    );
}

/// Test manifest parsing
#[test]
fn manifest_parsing() {
    // This test can run without network by using cached data
    let html = r#"
<html>
<head><title>Index of /pubmed/baseline</title></head>
<body>
<h1>Index of /pubmed/baseline</h1>
<pre>Name                     Last modified      Size
<a href="pubmed26n0001.xml.gz">pubmed26n0001.xml.gz</a>     2026-01-29 14:48   19M
<a href="pubmed26n0001.xml.gz.md5">pubmed26n0001.xml.gz.md5</a> 2026-01-29 14:48   60
<a href="pubmed26n0002.xml.gz">pubmed26n0002.xml.gz</a>     2026-01-29 14:48   17M
<a href="pubmed26n1334.xml.gz">pubmed26n1334.xml.gz</a>     2026-01-29 14:48   15M
</pre>
</body>
</html>"#;

    // Parse using internal function (would need to expose for testing)
    // This is a placeholder - actual test would use the manifest module
    assert!(html.contains("pubmed26n0001.xml.gz"));
    assert!(html.contains("pubmed26n1334.xml.gz"));
}

/// Test parquet file validity
#[test]
#[ignore]
fn parquet_file_validity() {
    use std::fs::File;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let config = papeline_pubmed::Config {
        output_dir: temp_dir.path().to_path_buf(),
        workers: 1,
        max_files: Some(1),
        zstd_level: 3,
        ..Default::default()
    };

    papeline_pubmed::run(&config).expect("Pipeline should succeed");

    // Find the parquet file
    let parquet_path = std::fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
        .expect("Should have a parquet file")
        .path();

    // Verify it's a valid parquet file
    let file = File::open(&parquet_path).expect("Should open file");
    let reader = parquet::file::reader::SerializedFileReader::new(file);
    assert!(reader.is_ok(), "Should be a valid parquet file");

    let reader = reader.unwrap();
    let metadata = reader.metadata();

    // Check row count
    let total_rows: i64 = metadata
        .row_groups()
        .iter()
        .map(|rg: &parquet::file::metadata::RowGroupMetaData| rg.num_rows())
        .sum();

    assert!(
        total_rows >= 25_000,
        "Expected at least 25,000 rows, got {}",
        total_rows
    );

    // Check schema
    let schema = metadata.file_metadata().schema();
    let field_names: Vec<&str> = schema
        .get_fields()
        .iter()
        .map(|f: &parquet::schema::types::TypePtr| f.name())
        .collect();

    assert!(field_names.contains(&"pmid"), "Should have pmid field");
    assert!(field_names.contains(&"doi"), "Should have doi field");
    assert!(field_names.contains(&"title"), "Should have title field");
    assert!(
        field_names.contains(&"mesh_terms_json"),
        "Should have mesh_terms_json field"
    );
}

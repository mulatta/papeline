use papeline_semantic_scholar::transform::{AbstractRow, CitationRow, PaperRow, PapersAccumulator};

fn load_lines(filename: &str) -> Vec<String> {
    let dir = std::env::var("BENCH_DATA_DIR")
        .expect("set BENCH_DATA_DIR to directory with sample data files");
    let path = std::path::Path::new(&dir).join(filename);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
        .lines()
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect()
}

/// Lightweight probe — mirrors the private CorpusIdProbe in worker.rs
#[derive(serde::Deserialize)]
struct CorpusIdProbe {
    #[serde(default)]
    #[allow(dead_code)]
    corpusid: i64,
}

#[divan::bench]
fn parse_paper_row(bencher: divan::Bencher) {
    let lines = load_lines("s2_papers.jsonl");
    bencher.bench(|| {
        for line in &lines {
            let _: PaperRow = sonic_rs::from_str(line).unwrap();
        }
    });
}

#[divan::bench]
fn parse_corpus_id_probe(bencher: divan::Bencher) {
    let lines = load_lines("s2_papers.jsonl");
    bencher.bench(|| {
        for line in &lines {
            let _: CorpusIdProbe = sonic_rs::from_str(line).unwrap();
        }
    });
}

#[divan::bench]
fn parse_abstract_row(bencher: divan::Bencher) {
    let lines = load_lines("s2_abstracts.jsonl");
    bencher.bench(|| {
        for line in &lines {
            let _: AbstractRow = sonic_rs::from_str(line).unwrap();
        }
    });
}

#[divan::bench]
fn parse_citation_row(bencher: divan::Bencher) {
    let lines = load_lines("s2_citations.jsonl");
    bencher.bench(|| {
        for line in &lines {
            let _: CitationRow = sonic_rs::from_str(line).unwrap();
        }
    });
}

#[divan::bench]
fn accumulator_papers(bencher: divan::Bencher) {
    let lines = load_lines("s2_papers.jsonl");
    bencher.bench(|| {
        let mut acc = PapersAccumulator::new();
        for line in &lines {
            let row: PaperRow = sonic_rs::from_str(line).unwrap();
            acc.push(row);
        }
        acc.take_batch().unwrap()
    });
}

fn main() {
    divan::main();
}

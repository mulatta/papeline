use papeline_pubmed::parser::parse_pubmed_xml;

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

#[divan::bench]
fn parse_pubmed_xml_bench(bencher: divan::Bencher) {
    // Each line is one complete XML document (PubmedArticleSet)
    let docs = load_lines("pubmed_sample.xml");
    bencher.bench(|| {
        for doc in &docs {
            let _ = parse_pubmed_xml(doc).unwrap();
        }
    });
}

fn main() {
    divan::main();
}

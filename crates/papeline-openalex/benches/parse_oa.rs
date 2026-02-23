use papeline_openalex::transform::{Accumulator, WorkAccumulator, WorkRow};

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
fn parse_work_row(bencher: divan::Bencher) {
    let lines = load_lines("oa_works.jsonl");
    bencher.bench(|| {
        for line in &lines {
            let _: WorkRow = sonic_rs::from_str(line).unwrap();
        }
    });
}

#[divan::bench]
fn accumulator_works(bencher: divan::Bencher) {
    let lines = load_lines("oa_works.jsonl");
    bencher.bench(|| {
        let mut acc = WorkAccumulator::new();
        for line in &lines {
            let row: WorkRow = sonic_rs::from_str(line).unwrap();
            acc.push(row);
        }
        acc.take_batch().unwrap()
    });
}

fn main() {
    divan::main();
}

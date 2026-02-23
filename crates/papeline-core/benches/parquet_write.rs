use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use papeline_core::ParquetSink;

fn synthetic_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, false),
    ]));
    let ids = Int64Array::from((0..n as i64).collect::<Vec<_>>());
    let titles = StringArray::from(
        (0..n)
            .map(|i| format!("Title for record {i}"))
            .collect::<Vec<_>>(),
    );
    RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(titles)]).unwrap()
}

#[divan::bench(args = [1, 3, 6])]
fn parquet_write_zstd(bencher: divan::Bencher, zstd_level: i32) {
    let batch = synthetic_batch(8192);
    let schema = batch.schema();
    let dir = tempfile::tempdir().unwrap();
    bencher.bench(|| {
        let mut sink =
            ParquetSink::new("bench", 0, dir.path(), schema.as_ref(), zstd_level).unwrap();
        sink.write_batch(&batch).unwrap();
        sink.finalize().unwrap();
    });
}

fn main() {
    divan::main();
}

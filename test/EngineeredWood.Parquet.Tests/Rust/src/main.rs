// Reads and writes ALP-encoded Parquet with arrow-rs, the reference for AlpArrowRsInteropTests.
//
// Usage:
//   parquet-alp-oracle read <file.parquet>
//       Prints `ENCODINGS <row group> <column> <ENC,ENC,...>` for every column chunk, then
//       `V <column> <row> <bits>` for every value in file order, where <bits> is the value's IEEE
//       754 bit pattern in lowercase hex (8 digits for FLOAT, 16 for DOUBLE) or `null`.
//   parquet-alp-oracle write <file.parquet>
//       Writes a fixed FLOAT column `f` and DOUBLE column `d` with ALP forced on, no dictionary,
//       small pages and two row groups, so the file spans several pages and partial vectors.
//
// Exits non-zero with `ERR <message>` on stderr on any failure.

use std::fs::File;
use std::sync::Arc;

use arrow_array::{Array, Float32Array, Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let result = match (args.get(1).map(String::as_str), args.get(2)) {
        (Some("read"), Some(path)) => read(path),
        (Some("write"), Some(path)) => write(path),
        _ => Err("usage: parquet-alp-oracle read|write <file.parquet>".to_string()),
    };
    if let Err(e) = result {
        eprintln!("ERR {e}");
        std::process::exit(1);
    }
}

fn read(path: &str) -> Result<(), String> {
    let file = File::open(path).map_err(|e| e.to_string())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| e.to_string())?;
    for (rg, group) in builder.metadata().row_groups().iter().enumerate() {
        for column in group.columns() {
            let encodings: Vec<String> = column.encodings().map(|e| e.to_string()).collect();
            println!("ENCODINGS {} {} {}", rg, column.column_path().string(), encodings.join(","));
        }
    }

    let reader = builder.build().map_err(|e| e.to_string())?;
    let mut columns: Vec<Vec<String>> = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| e.to_string())?;
        columns.resize_with(batch.num_columns(), Vec::new);
        for (c, column) in batch.columns().iter().enumerate() {
            for i in 0..column.len() {
                columns[c].push(bits(column.as_ref(), i)?);
            }
        }
    }
    for (c, values) in columns.iter().enumerate() {
        for (row, value) in values.iter().enumerate() {
            println!("V {c} {row} {value}");
        }
    }
    Ok(())
}

fn bits(column: &dyn Array, i: usize) -> Result<String, String> {
    if column.is_null(i) {
        return Ok("null".to_string());
    }
    if let Some(a) = column.as_any().downcast_ref::<Float32Array>() {
        return Ok(format!("{:08x}", a.value(i).to_bits()));
    }
    if let Some(a) = column.as_any().downcast_ref::<Float64Array>() {
        return Ok(format!("{:016x}", a.value(i).to_bits()));
    }
    Err(format!("unsupported column type {}", column.data_type()))
}

fn write(path: &str) -> Result<(), String> {
    const ROWS: usize = 5000;
    let mut f: Vec<Option<f32>> = Vec::with_capacity(ROWS);
    let mut d: Vec<Option<f64>> = Vec::with_capacity(ROWS);
    // A fixed LCG, so the file is the same on every run and platform.
    let mut state: u64 = 12345;
    for i in 0..ROWS {
        if i % 89 == 5 {
            f.push(None);
            d.push(None);
            continue;
        }
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let r = ((state >> 33) % 2_000_000) as i64 - 1_000_000;
        // Exceptions every 400 rows: NaN, -0.0, -inf, a value with no decimal form, a NaN with a
        // payload and its sign bit set (which must survive bit for bit).
        let (fv, dv) = match i % 400 {
            21 => (f32::NAN, f64::NAN),
            22 => (-0.0, -0.0),
            23 => (f32::NEG_INFINITY, f64::NEG_INFINITY),
            24 => (std::f32::consts::E * i as f32, std::f64::consts::E * i as f64),
            25 => (f32::from_bits(0xFFC0_0042), f64::from_bits(0xFFF8_0000_0000_0042)),
            _ => ((r / 100) as f32 / 10.0, r as f64 / 1000.0),
        };
        f.push(Some(fv));
        d.push(Some(dv));
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("f", DataType::Float32, true),
        Field::new("d", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from(f)), Arc::new(Float64Array::from(d))],
    )
    .map_err(|e| e.to_string())?;

    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::ALP)
        .set_compression(Compression::SNAPPY)
        .set_data_page_size_limit(8 * 1024)
        .set_write_batch_size(1024)
        .set_max_row_group_row_count(Some(3000))
        .build();
    let file = File::create(path).map_err(|e| e.to_string())?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).map_err(|e| e.to_string())?;
    writer.write(&batch).map_err(|e| e.to_string())?;
    writer.close().map_err(|e| e.to_string())?;
    Ok(())
}

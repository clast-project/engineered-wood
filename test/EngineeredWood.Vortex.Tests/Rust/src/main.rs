// Generates .vortex test fixtures for EngineeredWood.Vortex.Tests.
// Each fixture maps to one named writer below.
//
// Usage: vortex-fixtures <output-dir>
//
// The Rust crate is the source of truth for our test files: cross-validation
// against the EngineeredWood.Vortex reader/writer is built on top of files
// emitted here. Crate is excluded from the .NET build — call `cargo run --release`
// manually when fixtures change, then commit the produced .vortex files.

use std::path::PathBuf;

use std::sync::Arc;

use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::DecimalArray;
use vortex_array::arrays::ExtensionArray;
use vortex_array::arrays::ChunkedArray;
use vortex_array::arrays::FixedSizeListArray;
use vortex_array::arrays::ListArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::dtype::DecimalDType;
use vortex_array::dtype::Nullability;
use std::sync::Arc as StdArc;
use vortex_array::dtype::DType;
use vortex_array::dtype::PType;
use vortex_array::dtype::extension::ExtDType;
use vortex_array::extension::datetime::Date;
use vortex_array::extension::datetime::Time;
use vortex_array::extension::datetime::TimeUnit;
use vortex_array::extension::datetime::Timestamp;
use vortex_array::extension::uuid::Uuid as VortexUuid;
use vortex_array::extension::uuid::UuidMetadata;
use vortex_array::validity::Validity;
use vortex_buffer::buffer;
use vortex::VortexSessionDefault;
use vortex::editions::CORE_2026_08_1;
use vortex::editions::EditionSessionExt;
use vortex_file::WriteOptionsSessionExt;
use vortex_io::session::RuntimeSessionExt;
use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
use vortex_layout::layouts::table::TableStrategy;
use vortex_session::VortexSession;

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    // The session needs a tokio handle bound to the CURRENT runtime, so it must
    // be built inside `#[tokio::main]` rather than in a global LazyLock.
    let session = <VortexSession as VortexSessionDefault>::default().with_tokio();
    // Default writes target the newest core edition the .NET reader fully
    // understands. core2026.08.0 adds vortex.zoned and 08.1 vortex.onpair; the
    // later August editions add vortex.map (08.2) and vortex.variant (08.3),
    // which the reader can't decode yet.
    session
        .enable_edition(CORE_2026_08_1)
        .expect("core2026.08.1 is registered by the default session");

    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("usage: vortex-fixtures <output-dir>");
        std::process::exit(2);
    }
    let out_dir = PathBuf::from(&args[1]);
    std::fs::create_dir_all(&out_dir)?;

    write_struct_int_3rows(&session, &out_dir.join("struct_int_3rows.vortex")).await?;
    write_primitive_int_random(&session, &out_dir.join("primitive_int_random.vortex")).await?;
    write_constant_int(&session, &out_dir.join("constant_int_5rows.vortex")).await?;
    write_nullable_int(&session, &out_dir.join("nullable_int_6rows.vortex")).await?;
    write_multi_col(&session, &out_dir.join("multi_col_4rows.vortex")).await?;
    write_string_col(&session, &out_dir.join("string_col_5rows.vortex")).await?;
    write_bitpacked_int(&session, &out_dir.join("bitpacked_int_64rows.vortex")).await?;
    write_bitpacked_int_2k(&session, &out_dir.join("bitpacked_int_2048rows.vortex")).await?;
    write_bitpacked_sliced(&session, &out_dir.join("bitpacked_sliced_2000rows.vortex")).await?;
    write_for_int_2k(&session, &out_dir.join("for_int_2048rows.vortex")).await?;
    write_alp_double_2k(&session, &out_dir.join("alp_double_2048rows.vortex")).await?;
    write_alprd_double_2k(&session, &out_dir.join("alprd_double_2048rows.vortex")).await?;
    write_nullable_bitpacked_2k(&session, &out_dir.join("nullable_bitpacked_2048rows.vortex")).await?;
    write_nullable_alp_2k(&session, &out_dir.join("nullable_alp_2048rows.vortex")).await?;
    write_bitpacked_with_patches_2k(&session, &out_dir.join("bitpacked_patches_2048rows.vortex")).await?;
    write_bitpacked_patches_u8_indices(&session, &out_dir.join("bitpacked_patches_200rows.vortex")).await?;
    write_alp_with_patches_2k(&session, &out_dir.join("alp_patches_2048rows.vortex")).await?;
    write_decimal128_2k(&session, &out_dir.join("decimal128_2048rows.vortex")).await?;
    write_decimal256_2k(&session, &out_dir.join("decimal256_2048rows.vortex")).await?;
    write_timestamp_us_2k(&session, &out_dir.join("timestamp_us_2048rows.vortex")).await?;
    write_date_days_2k(&session, &out_dir.join("date_days_2048rows.vortex")).await?;
    write_time_us_2k(&session, &out_dir.join("time_us_2048rows.vortex")).await?;
    write_fsl_int_2k(&session, &out_dir.join("fsl_int_2048rows.vortex")).await?;
    write_list_int_2k(&session, &out_dir.join("list_int_2048rows.vortex")).await?;
    write_chunked_int(&session, &out_dir.join("chunked_int_3chunks.vortex")).await?;
    write_uuid_2k(&session, &out_dir.join("uuid_2048rows.vortex")).await?;
    write_delta_int_2k(&session, &out_dir.join("delta_int_2048rows.vortex")).await?;
    write_delta_diag(&session, &out_dir.join("delta_diag.vortex")).await?;
    write_delta_sliced(&session, &out_dir.join("delta_sliced_2000rows.vortex")).await?;
    write_rle_int_2k(&session, &out_dir.join("rle_int_2048rows.vortex")).await?;
    write_rle_sliced(&session, &out_dir.join("rle_sliced_2000rows.vortex")).await?;
    write_rle_nullable(&session, &out_dir.join("rle_nullable_1024rows.vortex")).await?;
    write_pco_double_2k(&session, &out_dir.join("pco_double_2048rows.vortex")).await?;
    write_pco_nullable_2k(&session, &out_dir.join("pco_nullable_2048rows.vortex")).await?;
    write_dict_int(&session, &out_dir.join("dict_int_64rows.vortex")).await?;
    write_dict_string(&session, &out_dir.join("dict_string_64rows.vortex")).await?;
    write_fsst_string(&session, &out_dir.join("fsst_string_64rows.vortex")).await?;
    write_masked_int(&session, &out_dir.join("masked_int_1024rows.vortex")).await?;
    write_binary_col(&session, &out_dir.join("binary_col_64rows.vortex")).await?;
    write_delta_signed_2k(&session, &out_dir.join("delta_signed_2048rows.vortex")).await?;
    write_sequence_u64_desc(&session, &out_dir.join("sequence_u64_desc_64rows.vortex")).await?;
    write_zoned_mixed(&session, &out_dir.join("zoned_mixed_20000rows.vortex")).await?;
    write_dict_nullable_values(&session, &out_dir.join("dict_nullable_values_20000rows.vortex")).await?;
    write_onpair_string(&session, &out_dir.join("onpair_string_64rows.vortex"), None).await?;
    write_onpair_string(&session, &out_dir.join("onpair_sliced_40rows.vortex"), Some(10..50)).await?;
    write_onpair_default(&session, &out_dir.join("onpair_default_20000rows.vortex")).await?;
    write_bool_sliced(&session, &out_dir.join("bool_sliced_61rows.vortex")).await?;
    write_zigzag_widths(&session, &out_dir.join("zigzag_widths_64rows.vortex"), None).await?;
    write_zigzag_widths(&session, &out_dir.join("zigzag_sliced_59rows.vortex"), Some(5..64)).await?;
    write_zigzag_default(&session, &out_dir.join("zigzag_default_20000rows.vortex")).await?;
    write_zstd_single_frame(&session, &out_dir.join("zstd_string_64rows.vortex")).await?;
    write_zstd_framed(&session, &out_dir.join("zstd_framed_2000rows.vortex")).await?;
    write_zstd_compact(&session, &out_dir.join("zstd_compact_20000rows.vortex")).await?;

    Ok(())
}

/// Smallest meaningful fixture: a struct root with one i32 column and 3 rows
/// of arithmetic-sequence values. Validates the postscript / dtype / layout /
/// segment plumbing. Note: vortex.sequence may end up being picked as the
/// array encoding because the values are monotonic.
async fn write_struct_int_3rows(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let nums = PrimitiveArray::from_iter(vec![1i32, 2, 3]).into_array();
    let data = StructArray::from_fields(&[("a", nums)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// A struct with non-monotonic, wide-range i32 values picked to defeat vortex's
/// pattern-aware encodings (sequence / FoR / bit-packed) and force the
/// fall-through to plain `vortex.primitive`. Chunk 6 of the .NET reader uses
/// this fixture as the canonical primitive decoder test.
async fn write_primitive_int_random(
    session: &VortexSession,
    path: &PathBuf,
) -> std::io::Result<()> {
    let nums = PrimitiveArray::from_iter(vec![
        42i32,
        -987_654_321,
        2_147_483_647,
        -1,
        12_345,
        -2_147_483_648,
    ])
    .into_array();
    let data = StructArray::from_fields(&[("a", nums)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// Small-range integers — vortex's compressor should pick a bit-packed
/// encoding (fastlanes.bitpacked) since all values fit in <8 bits.
async fn write_bitpacked_int(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    // 64 rows of values in [0, 99] so we have a non-monotonic, non-constant set
    // that bit-packs cleanly. FastLanes bit-packing operates on 1024-row chunks
    // but should work with smaller arrays too.
    let mut vals = Vec::with_capacity(64);
    let mut x: u64 = 0xC0FFEE;
    for _ in 0..64 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        vals.push((x % 100) as i32);
    }
    let nums = PrimitiveArray::from_iter(vals).into_array();
    let data = StructArray::from_fields(&[("a", nums)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row int column with values in [0, 99]. Above the FastLanes 1024-row
/// threshold, so vortex should pick fastlanes.bitpacked (7 bits per value).
async fn write_bitpacked_int_2k(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let mut vals = Vec::with_capacity(2048);
    let mut x: u64 = 0xBADC0FFEE;
    for _ in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        vals.push((x % 100) as i32);
    }
    let nums = PrimitiveArray::from_iter(vals).into_array();
    let data = StructArray::from_fields(&[("a", nums)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row NULLABLE f64 column encoded with vortex.pco. Every 7th row is null.
/// Pco compresses ONLY VALID values, so the dense decompressed buffer holds
/// the non-null values; the decoder splices them into the sparse output via
/// the validity bitmap.
async fn write_pco_nullable_2k(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    use vortex_pco::Pco;

    let mut x: u64 = 0xCAFE_F00D_BEEF_DEAD;
    let opt_iter = (0..2048).map(|i| {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let v = ((x >> 11) as f64) / ((1u64 << 53) as f64);
        if i % 7 == 0 { None } else { Some(v) }
    });
    let prim = PrimitiveArray::from_option_iter(opt_iter);
    let mut ctx = session.create_execution_ctx();
    let pco_arr = Pco::from_primitive(prim.as_view(), 8, 1 << 18, &mut ctx)
        .expect("Pco::from_primitive")
        .into_array();

    let data = StructArray::from_fields(&[("v", pco_arr)])
        .expect("from_fields")
        .into_array();

    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default()),
    ));

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .disable_editions()
        .with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 1024-row NULLABLE u32 column encoded with vortex.rle. Indices have a
/// validity bitmap (every 3rd row null). RLE's column-level validity comes
/// directly from the indices' validity per upstream rle/vtable/validity.rs.
async fn write_rle_nullable(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    use vortex_fastlanes::RLE;

    let values = PrimitiveArray::from_iter([10u32, 20u32, 30u32]).into_array();
    // Pattern: indices [0, 1, 2] cycling. Every 3rd index is null.
    let idx_pattern = [0u16, 1u16, 2u16];
    let valid_pattern = [true, false, true];
    let indices_buf: Vec<u16> = idx_pattern.iter().cycle().take(1024).copied().collect();
    let validity_iter = valid_pattern.iter().cycle().take(1024).copied();
    let indices = PrimitiveArray::new(
        vortex_buffer::Buffer::<u16>::from(indices_buf),
        Validity::from_iter(validity_iter),
    )
    .into_array();
    let values_idx_offsets = PrimitiveArray::from_iter([0u64]).into_array();

    let rle = RLE::try_new(values, indices, values_idx_offsets, 0, 1024)
        .expect("RLE::try_new")
        .into_array();
    let data = StructArray::from_fields(&[("v", rle)])
        .expect("from_fields")
        .into_array();

    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default()),
    ));

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .disable_editions()
        .with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2000-row sliced RLE column. Same input as rle_int_2048rows but the RLEData
/// is sliced to [10..2010] before serialization, so its metadata has offset=10
/// and length=2000. The decoder must handle the sub-chunk slice.
async fn write_rle_sliced(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    use vortex_fastlanes::RLE;
    use vortex_fastlanes::RLEData;
    use vortex_fastlanes::RLEArraySlotsExt;

    let mut vals: Vec<u32> = Vec::with_capacity(2048);
    for i in 0..1024u32 { vals.push(i / 50); }
    for i in 0..1024u32 { vals.push((i / 50) + 10); }
    let prim = PrimitiveArray::from_iter(vals);
    let mut ctx = session.create_execution_ctx();
    let rle_arr = RLEData::encode(prim.as_view(), &mut ctx)
        .expect("RLEData::encode");
    // RLE doesn't have a SliceReduce impl, so `array.slice()` wraps it in a
    // generic SliceArray (vortex.slice). To get a sliced fastlanes.rle with
    // offset!=0 directly, hand-construct the RLE with offset=10, length=2000.
    let sliced = RLE::try_new(
        rle_arr.values().clone(),
        rle_arr.indices().clone(),
        rle_arr.values_idx_offsets().clone(),
        10,
        2000,
    )
    .expect("RLE::try_new")
    .into_array();

    let data = StructArray::from_fields(&[("a", sliced)])
        .expect("from_fields")
        .into_array();

    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default()),
    ));

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .disable_editions()
        .with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row f64 column encoded with vortex.pco. Hand-construct via Pco::from_primitive
/// to bypass the writer's compressor heuristics (which may otherwise pick ALP for
/// nicely-rounded floats or plain primitive otherwise).
async fn write_pco_double_2k(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    use vortex_pco::Pco;

    let mut vals: Vec<f64> = Vec::with_capacity(2048);
    let mut x: u64 = 0xCAFE_F00D_BEEF_DEAD;
    for _ in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        // High-entropy doubles in [0, 1) — defeats ALP, gives pco something to work with.
        vals.push(((x >> 11) as f64) / ((1u64 << 53) as f64));
    }
    let prim = PrimitiveArray::from_iter(vals);
    let mut ctx = session.create_execution_ctx();
    // level=8 (default), values_per_page = pco's default max page size.
    let pco_arr = Pco::from_primitive(prim.as_view(), 8, 1 << 18, &mut ctx)
        .expect("Pco::from_primitive")
        .into_array();

    let data = StructArray::from_fields(&[("v", pco_arr)])
        .expect("from_fields")
        .into_array();

    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default()),
    ));

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .disable_editions()
        .with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2000-row sliced delta column. Same input as delta_int_2048rows but the
/// DeltaArray is sliced to [10..2010] before serialization, so its metadata
/// has offset=10 and length=2000. The decoder must decode both 1024-row
/// chunks and emit the [offset, offset+rowCount) slice.
async fn write_delta_sliced(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    use vortex_fastlanes::Delta;

    let mut vals: Vec<u64> = Vec::with_capacity(2048);
    let mut x: u64 = 0xDE17ADE17A;
    let mut acc: u64 = 1_700_000_000_000_000_000;
    for _ in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        acc = acc.wrapping_add(x % 1000);
        vals.push(acc);
    }
    let prim = PrimitiveArray::from_iter(vals);
    let mut ctx = session.create_execution_ctx();
    let delta_arr = Delta::try_from_primitive_array(&prim, &mut ctx)
        .expect("Delta::try_from_primitive_array");
    let sliced = delta_arr.as_array().slice(10..2010).expect("slice");

    let data = StructArray::from_fields(&[("a", sliced)])
        .expect("from_fields")
        .into_array();

    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default()),
    ));

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .disable_editions()
        .with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2000-row sliced bitpacked column. We build a 2048-row bitpacked array,
/// then slice [10..2010] so the resulting fastlanes.bitpacked has
/// metadata.offset=10 and length=2000. The writer preserves the slice rather
/// than canonicalizing it back to offset=0 because the underlying packed bytes
/// are kept verbatim.
async fn write_bitpacked_sliced(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    use vortex_fastlanes::bitpack_compress::bitpack_encode;

    let mut vals = Vec::with_capacity(2048);
    let mut x: u64 = 0xBADC0FFEE;
    for _ in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        vals.push((x % 100) as i32);
    }
    let prim = PrimitiveArray::from_iter(vals);
    let mut ctx = session.create_execution_ctx();
    let bp = bitpack_encode(&prim, 7, None, &mut ctx)
        .expect("bitpack_encode");
    let sliced = bp.as_array().slice(10..2010).expect("slice");

    let data = StructArray::from_fields(&[("a", sliced)])
        .expect("from_fields")
        .into_array();

    // A flat strategy has no compressor, so the writer serializes the
    // hand-built bit-packed array as-is.
    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default()),
    ));

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .disable_editions()
        .with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row nullable i32 column with small-range values + nulls, exercising
/// fastlanes.bitpacked WITH a validity child.
async fn write_nullable_bitpacked_2k(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let mut vals = Vec::with_capacity(2048);
    let mut validity_vec = Vec::with_capacity(2048);
    let mut x: u64 = 0xCAFEBABEDEADBEEF;
    for _ in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        vals.push((x % 100) as i32);
        // ~80% valid
        validity_vec.push((x % 5) != 0);
    }
    let nums = PrimitiveArray::new(vortex_buffer::Buffer::from(vals), Validity::from_iter(validity_vec))
        .into_array();
    let data = StructArray::from_fields(&[("a", nums)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row nullable f64 column — should trigger vortex.alp with a validity child.
async fn write_nullable_alp_2k(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let mut vals = Vec::with_capacity(2048);
    let mut validity_vec = Vec::with_capacity(2048);
    let mut x: u64 = 0xFEEBDAED;
    for _ in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let cents = (x % 100_000) as i64;
        vals.push((cents as f64) / 100.0);
        validity_vec.push((x % 5) != 0);
    }
    let nums = PrimitiveArray::new(vortex_buffer::Buffer::from(vals), Validity::from_iter(validity_vec))
        .into_array();
    let data = StructArray::from_fields(&[("price", nums)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row List<i32> column with variable-length lists.
async fn write_list_int_2k(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    // Each row has 0..7 elements; build offsets + flat element buffer.
    let mut elements: Vec<i32> = Vec::new();
    let mut offsets: Vec<i32> = Vec::with_capacity(2049);
    offsets.push(0);
    let mut x: u64 = 0x115_75EED;
    for _ in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let len = (x % 7) as usize;
        for _ in 0..len {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            elements.push((x % 1000) as i32);
        }
        offsets.push(elements.len() as i32);
    }
    let elements_arr = PrimitiveArray::from_iter(elements).into_array();
    let offsets_arr = PrimitiveArray::from_iter(offsets).into_array();
    let list = ListArray::try_new(elements_arr, offsets_arr, Validity::NonNullable)
        .expect("ListArray::try_new")
        .into_array();
    let data = StructArray::from_fields(&[("xs", list)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session.write_options().write(&mut bytes, data.to_array_stream()).await.expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row monotonic-ish int column. Should trigger fastlanes.delta — small
/// per-row deltas around a steady increase.
/// Diagnostic fixture per the Clast.FastLanes author's request:
/// u32 column [0, 1, 2, ..., 1023] forced to fastlanes.delta encoding.
/// We dump the stored deltas[0..32] from C# to determine vortex's layout
/// convention (lane-major vs UTL).
async fn write_delta_diag(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    use vortex_fastlanes::Delta;

    let vals: Vec<u32> = (0u32..1024).collect();
    let prim = PrimitiveArray::from_iter(vals);
    let mut ctx = session.create_execution_ctx();
    let delta_arr = Delta::try_from_primitive_array(&prim, &mut ctx)
        .expect("Delta::try_from_primitive_array")
        .into_array();
    let data = StructArray::from_fields(&[("a", delta_arr)])
        .expect("from_fields")
        .into_array();

    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default()),
    ));

    let mut bytes: Vec<u8> = Vec::new();
    session.write_options().disable_editions().with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream()).await.expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

async fn write_delta_int_2k(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    // Hand-construct a DeltaArray so the writer is forced to serialize it
    // (rather than the compressor choosing a different encoding). u64 column
    // because vortex-fastlanes Delta only supported unsigned integers before
    // 0.86; `write_delta_signed_2k` covers the signed form.
    use vortex_fastlanes::Delta;

    let mut vals: Vec<u64> = Vec::with_capacity(2048);
    let mut x: u64 = 0xDE17ADE17A;
    let mut acc: u64 = 1_700_000_000_000_000_000;
    for _ in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        acc = acc.wrapping_add(x % 1000);
        vals.push(acc);
    }
    let prim = PrimitiveArray::from_iter(vals);
    let mut ctx = session.create_execution_ctx();
    let delta_arr = Delta::try_from_primitive_array(&prim, &mut ctx)
        .expect("Delta::try_from_primitive_array")
        .into_array();
    let data = StructArray::from_fields(&[("a", delta_arr)])
        .expect("from_fields")
        .into_array();

    // A flat strategy has no compressor, so the writer serializes the
    // hand-built delta array as-is. fastlanes.delta belongs to no edition,
    // which is why these writes disable edition enforcement.
    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default()),
    ));

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .disable_editions()
        .with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048 u32 rows that span TWO 1024-element FastLanes chunks, each with low
/// cardinality (~21 distinct runs of 50 elements per chunk). The two chunks
/// have OVERLAPPING but distinct value sets so the dictionary handling is
/// non-trivial: chunk 0 values = [0..21), chunk 1 values = [10..31).
async fn write_rle_int_2k(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    use vortex_fastlanes::RLEData;

    let mut vals: Vec<u32> = Vec::with_capacity(2048);
    for i in 0..1024u32 { vals.push(i / 50); }            // chunk 0: 0..21
    for i in 0..1024u32 { vals.push((i / 50) + 10); }     // chunk 1: 10..31
    let prim = PrimitiveArray::from_iter(vals);
    let mut ctx = session.create_execution_ctx();
    let rle_arr = RLEData::encode(prim.as_view(), &mut ctx)
        .expect("RLEData::encode")
        .into_array();
    let data = StructArray::from_fields(&[("a", rle_arr)])
        .expect("from_fields")
        .into_array();

    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default()),
    ));

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .disable_editions()
        .with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row vortex.uuid column. Storage: FixedSizeList(U8, 16). Each row is
/// 16 bytes from a deterministic LCG so the test can reproduce them.
async fn write_uuid_2k(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let mut bytes_flat: Vec<u8> = Vec::with_capacity(2048 * 16);
    let mut x: u64 = 0xDEADCAFEBABE1357;
    for _ in 0..2048 * 16 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        bytes_flat.push((x >> 32) as u8);
    }
    let inner = PrimitiveArray::from_iter(bytes_flat).into_array();
    let fsl = FixedSizeListArray::try_new(inner, 16, Validity::NonNullable, 2048)
        .expect("FSL")
        .into_array();

    let ext_dtype = ExtDType::<VortexUuid>::try_new(
        UuidMetadata { version: None },
        DType::FixedSizeList(
            StdArc::new(DType::Primitive(PType::U8, Nullability::NonNullable)),
            16,
            Nullability::NonNullable,
        ),
    )
    .expect("ExtDType<Uuid>::try_new")
    .erased();
    let uuid_arr = ExtensionArray::new(ext_dtype, fsl).into_array();

    let data = StructArray::from_fields(&[("id", uuid_arr)])
        .expect("from_fields")
        .into_array();

    // The vortex.uuid extension dtype only joined an edition in core2026.08.3,
    // past the edition the session targets, so this write opts out.
    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .disable_editions()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 3-chunk struct: 100, 200, 50 rows. Wraps each StructArray in a ChunkedArray
/// before streaming to the writer — vortex emits a vortex.chunked layout.
async fn write_chunked_int(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    fn make_chunk(start: i32, count: i32) -> vortex_array::ArrayRef {
        let nums = PrimitiveArray::from_iter((start..start + count).collect::<Vec<_>>())
            .into_array();
        StructArray::from_fields(&[("a", nums)])
            .expect("from_fields")
            .into_array()
    }

    // Use ≥1M rows per chunk to defeat the writer's chunk-merging threshold.
    let chunks = vec![
        make_chunk(0, 1_000_000),
        make_chunk(2_000_000, 1_000_000),
        make_chunk(4_000_000, 500_000),
    ];
    let dtype = chunks[0].dtype().clone();
    let chunked = ChunkedArray::try_new(chunks, dtype)
        .expect("ChunkedArray::try_new")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session.write_options().write(&mut bytes, chunked.to_array_stream()).await.expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row FixedSizeList<i32, 3> column. Inner element count = 6144.
async fn write_fsl_int_2k(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let mut elements: Vec<i32> = Vec::with_capacity(6144);
    let mut x: u64 = 0xF15F00D5;
    for _ in 0..6144 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        elements.push((x % 1000) as i32);
    }
    let inner = PrimitiveArray::from_iter(elements).into_array();
    let fsl = FixedSizeListArray::try_new(inner, 3, Validity::NonNullable, 2048)
        .expect("FixedSizeListArray::try_new")
        .into_array();
    let data = StructArray::from_fields(&[("triple", fsl)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session.write_options().write(&mut bytes, data.to_array_stream()).await.expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row Date(Days) column. Storage is i32. Vortex will pick whatever
/// integer encoding fits (likely fastlanes.for + bitpacked given a large base).
async fn write_date_days_2k(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let mut vals: Vec<i32> = Vec::with_capacity(2048);
    let mut x: u64 = 0xDEC0DED1;
    // Days since 1970-01-01. 2024-01-01 ≈ day 19723. Spread across ~5 years.
    for _ in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        vals.push(19_723i32 + (x % (5 * 365)) as i32);
    }
    let storage = PrimitiveArray::from_iter(vals).into_array();
    let ext_dtype = Date::new(TimeUnit::Days, Nullability::NonNullable).erased();
    let nums = ExtensionArray::new(ext_dtype, storage).into_array();
    let data = StructArray::from_fields(&[("d", nums)])
        .expect("from_fields")
        .into_array();
    let mut bytes: Vec<u8> = Vec::new();
    session.write_options().write(&mut bytes, data.to_array_stream()).await.expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row Time(Microseconds) column. Storage is i64 (microseconds since midnight).
async fn write_time_us_2k(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let mut vals: Vec<i64> = Vec::with_capacity(2048);
    let mut x: u64 = 0xC1A551C;
    const US_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000;
    for _ in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        vals.push((x % US_PER_DAY as u64) as i64);
    }
    let storage = PrimitiveArray::from_iter(vals).into_array();
    let ext_dtype = Time::new(TimeUnit::Microseconds, Nullability::NonNullable).erased();
    let nums = ExtensionArray::new(ext_dtype, storage).into_array();
    let data = StructArray::from_fields(&[("t", nums)])
        .expect("from_fields")
        .into_array();
    let mut bytes: Vec<u8> = Vec::new();
    session.write_options().write(&mut bytes, data.to_array_stream()).await.expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row Timestamp(microsecond) column. Values are spread across a year
/// at second-or-better granularity — should trigger vortex.datetimeparts
/// (days/seconds/subseconds decomposition).
async fn write_timestamp_us_2k(
    session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    // Microseconds since Unix epoch. Start of 2024 (UTC) = 1_704_067_200 seconds
    // = 1_704_067_200_000_000 microseconds.
    const BASE_US: i64 = 1_704_067_200_000_000;
    const SECONDS_PER_DAY: i64 = 86_400;
    const US_PER_SECOND: i64 = 1_000_000;

    let mut vals: Vec<i64> = Vec::with_capacity(2048);
    let mut x: u64 = 0xCAFED00D;
    for _ in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        // Random offset within ~1 year (in microseconds).
        let offset_us = (x % (365 * SECONDS_PER_DAY * US_PER_SECOND) as u64) as i64;
        vals.push(BASE_US + offset_us);
    }

    let storage = PrimitiveArray::from_iter(vals).into_array();
    let ext_dtype = Timestamp::new(TimeUnit::Microseconds, Nullability::NonNullable).erased();
    let nums = ExtensionArray::new(ext_dtype, storage).into_array();

    let data = StructArray::from_fields(&[("ts", nums)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row Decimal(precision=10, scale=2) column. Values are i32 unscaled
/// (e.g., 12345 → 123.45). Vortex should pick vortex.decimal at the array
/// level, possibly narrowed to i32 internally.
async fn write_decimal128_2k(
    session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let mut vals: Vec<i64> = Vec::with_capacity(2048);
    let mut x: u64 = 0xC0FFEEDECAF;
    for _ in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        vals.push(((x % 100_000_000) as i64) - 50_000_000);
    }
    let decimal_dtype = DecimalDType::new(10, 2);
    let buffer = vortex_buffer::Buffer::from(vals);
    let nums = DecimalArray::new(buffer, decimal_dtype, Validity::NonNullable).into_array();
    let data = StructArray::from_fields(&[("amt", nums)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row Decimal(precision=40, scale=2) column. Precision > 38 forces the
/// Arrow schema to Decimal256Type. Storage uses i128 with values that genuinely
/// require >i64 magnitude — small enough to fit i128 (so vortex doesn't promote
/// to i256) but big enough to defeat narrowing back to i64.
async fn write_decimal256_2k(
    session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let mut vals: Vec<i128> = Vec::with_capacity(2048);
    let mut x: u64 = 0xC0FFEEDECAFBABE;
    for _ in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        // Take 64 random bits, sign-extend, then shift left by 60 so the value
        // straddles the i64 boundary — guarantees i128 magnitude (~ ±1.5e37).
        let v = (x as i64 as i128) << 60;
        vals.push(v);
    }
    let decimal_dtype = DecimalDType::new(40, 2);
    let buffer = vortex_buffer::Buffer::from(vals);
    let nums = DecimalArray::new(buffer, decimal_dtype, Validity::NonNullable).into_array();
    let data = StructArray::from_fields(&[("amt", nums)])
        .expect("from_fields")
        .into_array();

    // Force vortex.decimal (vs vortex.decimal_byte_parts) so the I128→256
    // sign-extend path in DecimalArrayDecoder is exercised.
    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default()),
    ));

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .disable_editions()
        .with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row f64 column where most values share a 2-decimal scale but a few
/// (every 100th, offset 19) are full-precision irrationals that don't encode
/// cleanly — vortex should pick vortex.alp with patches.
async fn write_alp_with_patches_2k(
    session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let mut vals = Vec::with_capacity(2048);
    let mut x: u64 = 0xBADBEEFFEEDFACE;
    for i in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        if i % 100 == 19 {
            // Irrational-style outlier: full f64 precision, can't be ALP-encoded.
            let bits = (x & 0x000F_FFFF_FFFF_FFFF) | 0x4080_0000_0000_0000; // [512.0, 1024.0)
            vals.push(f64::from_bits(bits));
        } else {
            // Regular 2-decimal-place value
            let cents = (x % 100_000) as i64;
            vals.push((cents as f64) / 100.0);
        }
    }
    let nums = PrimitiveArray::from_iter(vals).into_array();
    let data = StructArray::from_fields(&[("v", nums)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row int column where most values fit in 7 bits but a few are
/// outliers — vortex should bit-pack with patches for the outliers.
async fn write_bitpacked_with_patches_2k(
    session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let mut vals = Vec::with_capacity(2048);
    let mut x: u64 = 0xACEDFACE;
    for i in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        // Most values in [0, 99]; ~1% are large outliers in [1_000_000, 1_099_999].
        if i % 100 == 17 {
            vals.push(1_000_000i32 + (x % 100_000) as i32);
        } else {
            vals.push((x % 100) as i32);
        }
    }
    let nums = PrimitiveArray::from_iter(vals).into_array();
    let data = StructArray::from_fields(&[("a", nums)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 200-row i32 column bit-packed at 7 bits with every 20th row an outlier patch. Under 255 rows,
/// upstream stores patch indices as u8 — PType 0, which proto3 leaves out of the metadata — so
/// this pins the reader to the default rather than the u32 it once assumed.
async fn write_bitpacked_patches_u8_indices(
    session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    use vortex_fastlanes::bitpack_compress::bitpack_encode;

    let vals: Vec<i32> = (0..200)
        .map(|i| if i % 20 == 3 { 1_000_000 + i } else { (i * 37) % 100 })
        .collect();
    let prim = PrimitiveArray::from_iter(vals);
    let mut ctx = session.create_execution_ctx();
    let bp = bitpack_encode(&prim, 7, None, &mut ctx).expect("bitpack_encode");

    let data = StructArray::from_fields(&[("a", bp.into_array())])
        .expect("from_fields")
        .into_array();

    // A flat strategy has no compressor, so the hand-built array is written as-is.
    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default()),
    ));

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .disable_editions()
        .with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row f64 column with high-entropy doubles (no consistent decimal scale).
/// Should trigger vortex.alprd ("real doubles").
async fn write_alprd_double_2k(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let mut vals = Vec::with_capacity(2048);
    let mut x: u64 = 0xC0DEFACE;
    for _ in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        // Build a "scientific" double: full-entropy bit pattern in the mantissa,
        // safe exponent. This defeats ALP's scaling-and-rounding strategy.
        let bits = (x & 0x000F_FFFF_FFFF_FFFF) | 0x3FF0_0000_0000_0000; // [1.0, 2.0)
        let mantissa = f64::from_bits(bits);
        // Scale by another random factor
        let exp = ((x >> 52) & 0x3F) as i32 - 32; // exponent in [-32, 31]
        let v = mantissa * 2f64.powi(exp);
        vals.push(v);
    }
    let nums = PrimitiveArray::from_iter(vals).into_array();
    let data = StructArray::from_fields(&[("v", nums)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row f64 column with realistic decimal values — should trigger
/// vortex.alp (Adaptive Lossless floating Point compression) since the
/// values have a stable scale.
async fn write_alp_double_2k(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let mut vals = Vec::with_capacity(2048);
    let mut x: u64 = 0xFEEDBEEF;
    for _ in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        // Values like 12.34, 56.78, etc. — 2 decimal places.
        let cents = (x % 100_000) as i64;
        vals.push((cents as f64) / 100.0);
    }
    let nums = PrimitiveArray::from_iter(vals).into_array();
    let data = StructArray::from_fields(&[("price", nums)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 2048-row int column with values in [1_000_000, 1_000_099] — small range
/// shifted by a large base, which should trigger fastlanes.for (subtract a
/// reference, then bit-pack the small delta).
async fn write_for_int_2k(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let mut vals = Vec::with_capacity(2048);
    let mut x: u64 = 0xDEADC0DE;
    for _ in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        vals.push(1_000_000i32 + (x % 100) as i32);
    }
    let nums = PrimitiveArray::from_iter(vals).into_array();
    let data = StructArray::from_fields(&[("a", nums)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// Low-cardinality int column. Vortex picks the vortex.dict LAYOUT (values
/// dict + per-row codes children) and uses vortex.primitive for the values
/// array — no FSST involved.
async fn write_dict_int(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let mut vals = Vec::with_capacity(64);
    let palette = [10_001i32, 99_999, -42_000, 7];
    let mut x: u64 = 0xCAFE_BABE;
    for _ in 0..64 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        vals.push(palette[(x as usize) % palette.len()]);
    }
    let nums = PrimitiveArray::from_iter(vals).into_array();
    let data = StructArray::from_fields(&[("v", nums)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// Low-cardinality string column. Vortex picks the vortex.dict LAYOUT (values
/// dict + per-row codes) AND uses vortex.fsst for the values dictionary —
/// exercises the dict + FSST composition.
async fn write_dict_string(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let palette = [
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot",
    ];
    let mut vals: Vec<&str> = Vec::with_capacity(64);
    let mut x: u64 = 0xDEADBEEF;
    for _ in 0..64 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        vals.push(palette[(x as usize) % palette.len()]);
    }
    let strings = VarBinViewArray::from_iter_str(vals).into_array();
    let data = StructArray::from_fields(&[("color", strings)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// Many-distinct strings with shared prefixes — vortex picks vortex.fsst at
/// the array level (no dict wrapping). Validates the FSST decoder on its own.
async fn write_fsst_string(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    // 64 distinct strings sharing common prefixes — FSST should produce a
    // useful symbol table; cardinality too high for dict encoding.
    let mut vals: Vec<String> = Vec::with_capacity(64);
    for i in 0..64 {
        vals.push(format!("user-event-{:04}-payload-{}", i, i * 137));
    }
    let strings = VarBinViewArray::from_iter_str(vals.iter().map(|s| s.as_str())).into_array();
    let data = StructArray::from_fields(&[("event", strings)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 64-row binary column with deterministic byte payloads. Used to test
/// the BytesComparisonPredicate's BinaryArray branch (the StringArray
/// branch already has dict_string_64rows.vortex). Default writer settings
/// → vortex's compressor picks vortex.varbinview (or similar) for storage
/// AND emits vortex.stats with per-zone Min / Max for the column. The
/// deterministic byte pattern lets the test predict the lex-min / lex-max
/// without inspecting the file.
async fn write_binary_col(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let mut rows: Vec<Vec<u8>> = Vec::with_capacity(64);
    for i in 0..64u8 {
        // Each row is 8 bytes: leading 0x10 + i + zeros — yields a stable
        // lex-ordering [0x10 0x00 …, 0x10 0x01 …, …, 0x10 0x3F …].
        rows.push(vec![0x10, i, 0, 0, 0, 0, 0, 0]);
    }
    let bins = VarBinViewArray::from_iter_bin(rows.iter().map(|r| r.as_slice())).into_array();
    let data = StructArray::from_fields(&[("b", bins)])
        .expect("from_fields")
        .into_array();

    // Default WriteOptions / no with_strategy — vortex's default TableStrategy
    // wraps each column in vortex.stats so we get the per-zone Min / Max we
    // need to test predicate pruning. (write_string_col uses a custom
    // FlatLayoutStrategy explicitly to skip the stats wrapper, which we
    // don't want here.)
    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// Single utf8 column. We use a non-compressing TableStrategy (FlatLayoutStrategy
/// for both table-level and per-column) so vortex skips FSST/dictionary
/// compression and we get the canonical `vortex.varbinview` encoding.
async fn write_string_col(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let strings = VarBinViewArray::from_iter_str(vec!["alpha", "be", "γ-particle", "", "delta-9"])
        .into_array();
    let data = StructArray::from_fields(&[("s", strings)])
        .expect("from_fields")
        .into_array();

    let writer = Arc::new(TableStrategy::new(
        Arc::new(FlatLayoutStrategy::default()),
        Arc::new(FlatLayoutStrategy::default()),
    ));

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .disable_editions()
        .with_strategy(writer)
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// Three-column struct: i32 (random), i64 (constant), i32 (sequence). Each
/// hits a different encoding so we exercise the multi-column / per-field
/// dispatch end to end.
async fn write_multi_col(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let a = PrimitiveArray::from_iter(vec![100i32, -50, 7, 999_999]).into_array();
    let b = PrimitiveArray::from_iter(vec![42i64; 4]).into_array();
    let c = PrimitiveArray::from_iter(vec![1i32, 2, 3, 4]).into_array();

    let data = StructArray::from_fields(&[("a", a), ("b", b), ("c", c)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// Nullable i32 column with mixed values + nulls. Random-looking values to
/// defeat sequence/constant compression so we get vortex.primitive (with a
/// validity child).
async fn write_nullable_int(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    // values: [10, _, 20, _, 99999, -7]   (the _ are nulls)
    let nums = PrimitiveArray::new(
        buffer![10i32, 0, 20, 0, 99999, -7],
        Validity::from_iter(vec![true, false, true, false, true, true]),
    )
    .into_array();
    let data = StructArray::from_fields(&[("a", nums)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// 1024-row u32 column wrapped in `vortex.masked`: a non-null primitive
/// child with an explicit per-row validity bitmap as a sibling. Used to
/// validate the `MaskedArrayDecoder`'s buffer-swap rebuild path. The child
/// is constructed AS NON-NULL (every row valid in the underlying buffer) and
/// the masked array overlays the bitmap on top — exactly the shape upstream
/// emits when its compressor picks vortex.masked over an inline-validity
/// alternative.
async fn write_masked(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    use vortex_array::arrays::masked::MaskedArray;

    // Non-null child: monotone u32 values [0, 1, ..., 1023]. Picked so the
    // reader can verify each row's value, since they're trivially recoverable
    // from the row index.
    let child_vals: Vec<u32> = (0..1024u32).collect();
    let child = PrimitiveArray::from_iter(child_vals).into_array();

    // Validity: every 5th row null (i % 5 == 0). 1024 / 5 ≈ 205 nulls. Mix
    // makes the bitmap non-trivial without dominating either branch.
    let validity = Validity::from_iter((0..1024).map(|i| i % 5 != 0));

    let masked = MaskedArray::try_new(child, validity)
        .expect("MaskedArray::try_new")
        .into_array();
    let data = StructArray::from_fields(&[("v", masked)])
        .expect("from_fields")
        .into_array();

    // A flat strategy has no compressor, so the writer serializes the
    // hand-constructed MaskedArray as-is rather than as vortex.primitive
    // with inline validity.
    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default()),
    ));

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .disable_editions()
        .with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// Convenience entry-point: thin wrapper to match the `write_*_int` naming
/// pattern used elsewhere; the masked writer itself is dtype-agnostic.
async fn write_masked_int(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    write_masked(session, path).await
}

/// All-equal i32 column. Vortex's compressor should pick `vortex.constant`
/// (or possibly vortex.sequence with multiplier=0). Either way exercises a
/// new decoder path.
async fn write_constant_int(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    let nums = PrimitiveArray::from_iter(vec![777i32; 5]).into_array();
    let data = StructArray::from_fields(&[("a", nums)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");

    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// A hand-built fastlanes.delta over i32. Vortex 0.86 accepts signed inputs,
/// delta-encoding their unsigned bit patterns; 0.70 required unsigned.
/// The walk crosses zero and wraps past i32::MIN so both signs and a
/// wrapping difference appear.
async fn write_delta_signed_2k(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    use vortex_fastlanes::Delta;

    let mut vals: Vec<i32> = Vec::with_capacity(2048);
    let mut x: u64 = 0x5EED_0086;
    let mut acc: i32 = -1_000;
    for i in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        acc = acc.wrapping_add((x % 7) as i32 - 2);
        vals.push(if i == 1500 { i32::MIN + 1 } else { acc });
    }
    let prim = PrimitiveArray::from_iter(vals);
    let mut ctx = session.create_execution_ctx();
    let delta_arr = Delta::try_from_primitive_array(&prim, &mut ctx)
        .expect("Delta::try_from_primitive_array")
        .into_array();
    let data = StructArray::from_fields(&[("a", delta_arr)])
        .expect("from_fields")
        .into_array();

    let strategy = Arc::new(TableStrategy::new(
        Arc::new(FlatLayoutStrategy::default()),
        Arc::new(FlatLayoutStrategy::default()),
    ));
    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .disable_editions()
        .with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// A descending u64 vortex.sequence starting above i64::MAX. The base is
/// stored as a u64 scalar and the step, normalized to i64 since 0.86, as a
/// negative i64 scalar.
async fn write_sequence_u64_desc(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    use vortex_array::scalar::PValue;
    use vortex_sequence::Sequence;

    let seq = Sequence::try_new(
        PValue::U64(u64::MAX - 5),
        PValue::I64(-3),
        PType::U64,
        Nullability::NonNullable,
        64,
    )
    .expect("Sequence::try_new")
    .into_array();
    let data = StructArray::from_fields(&[("a", seq)])
        .expect("from_fields")
        .into_array();

    let strategy = Arc::new(TableStrategy::new(
        Arc::new(FlatLayoutStrategy::default()),
        Arc::new(FlatLayoutStrategy::default()),
    ));
    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// Several columns through the default write strategy, so the writer picks
/// the encodings and wraps each column in a vortex.zoned layout with 8192-row
/// zones (three zones here). The values are arranged so zones have disjoint
/// ranges and can be pruned:
///   id:  i64, row index
///   val: i32, zone * 1000 + noise in [0, 500)
///   f:   f64, row * 0.5, with NaN every 1000th row of zone 1 only
///   s:   utf8, "{row:06}-" + 70 'x', longer than the 64-byte zone-map bound
///        so its min/max are stored truncated (vortex.bounded_min/max)
///   tag: utf8, "k{zone}-{row % 7}", short enough for exact bounds
///   n:   nullable i32, row * 3, null on every third row
async fn write_zoned_mixed(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    const ROWS: usize = 20_000;
    const ZONE: usize = 8192;

    let id = PrimitiveArray::from_iter((0..ROWS).map(|i| i as i64)).into_array();
    let mut x: u64 = 0x2026_0916;
    let val = PrimitiveArray::from_iter((0..ROWS).map(|i| {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((i / ZONE) * 1000) as i32 + ((x >> 33) % 500) as i32
    }))
    .into_array();
    let f = PrimitiveArray::from_iter((0..ROWS).map(|i| {
        if i / ZONE == 1 && i % 1000 == 0 { f64::NAN } else { i as f64 * 0.5 }
    }))
    .into_array();
    let long: Vec<String> = (0..ROWS).map(|i| format!("{:06}-{}", i, "x".repeat(70))).collect();
    let s = VarBinViewArray::from_iter_str(long.iter().map(String::as_str)).into_array();
    let tags: Vec<String> = (0..ROWS).map(|i| format!("k{}-{}", i / ZONE, i % 7)).collect();
    let tag = VarBinViewArray::from_iter_str(tags.iter().map(String::as_str)).into_array();
    let n = PrimitiveArray::new(
        vortex_buffer::Buffer::from_iter((0..ROWS).map(|i| (i * 3) as i32)),
        Validity::from_iter((0..ROWS).map(|i| i % 3 != 0)),
    )
    .into_array();

    let data = StructArray::from_fields(&[
        ("id", id),
        ("val", val),
        ("f", f),
        ("s", s),
        ("tag", tag),
        ("n", n),
    ])
    .expect("from_fields")
    .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// Low-cardinality nullable columns through the default write strategy, which
/// gives each a vortex.dict layout whose nulls live in the dictionary values
/// (a null entry that null rows point at) rather than in the codes.
///   u:   nullable u16, row % 50, null on every seventh row
///   tag: nullable utf8, "t{row % 30}", null on every eleventh row
async fn write_dict_nullable_values(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    const ROWS: usize = 20_000;

    let u = PrimitiveArray::new(
        vortex_buffer::Buffer::from_iter((0..ROWS).map(|i| (i % 50) as u16)),
        Validity::from_iter((0..ROWS).map(|i| i % 7 != 0)),
    )
    .into_array();
    let tag = VarBinViewArray::from_iter_nullable_str(
        (0..ROWS).map(|i| (i % 11 != 0).then(|| format!("t{}", i % 30))),
    )
    .into_array();

    let data = StructArray::from_fields(&[("u", u), ("tag", tag)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// Row `i` of the hand-built OnPair fixtures: URL-like strings sharing
/// prefixes (so tokens span several bytes), a multi-byte UTF-8 run, an empty
/// string, and nulls.
fn onpair_row(i: usize) -> Option<String> {
    match i % 9 {
        0 => None,
        1 => Some(String::new()),
        2 => Some(format!("https://example.com/products/{}", i * 7)),
        3 => Some(format!("https://example.com/profile/{}", i)),
        4 => Some(format!("café-{}-naïve-日本語", i % 5)),
        5 => Some("https://example.org/about".to_string()),
        6 => Some(format!("user-{:04}@example.com", i)),
        7 => Some("x".repeat(i % 40)),
        _ => Some(format!("https://example.com/products/{}?ref=home", i)),
    }
}

/// A hand-built vortex.onpair array over `onpair_row`, written as-is by a flat
/// strategy (uncompressed integer children). With `slice`, the array is sliced
/// first: OnPair's slice keeps the whole `codes` child and narrows only
/// `codes_offsets`, so the decoder must start from `codes_offsets[0]`.
async fn write_onpair_string(
    session: &VortexSession,
    path: &PathBuf,
    slice: Option<std::ops::Range<usize>>,
) -> std::io::Result<()> {
    use vortex_onpair::OnPair;
    use vortex_onpair::DEFAULT_CONFIG;
    use vortex_onpair::onpair_compress;

    let rows: Vec<Option<String>> = (0..64).map(onpair_row).collect();
    let strings = VarBinViewArray::from_iter_nullable_str(rows.iter().map(|r| r.as_deref())).into_array();
    let mut ctx = session.create_execution_ctx();
    let mut onpair = onpair_compress(&strings, DEFAULT_CONFIG, &mut ctx).expect("onpair_compress");
    if let Some(range) = slice {
        onpair = onpair.slice(range).expect("slice");
    }
    assert!(onpair.is::<OnPair>(), "expected OnPair, got {}", onpair.encoding_id());

    let data = StructArray::from_fields(&[("s", onpair)])
        .expect("from_fields")
        .into_array();
    let strategy = Arc::new(TableStrategy::new(
        Arc::new(FlatLayoutStrategy::default()),
        Arc::new(FlatLayoutStrategy::default()),
    ));
    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// Row `i` of `write_onpair_default`: one of a few URL templates with a query
/// id, null on every thirteenth row.
fn onpair_default_row(i: usize) -> Option<String> {
    const PAGES: [&str; 5] = [
        "https://example.com/products/widget",
        "https://example.com/products/gadget",
        "https://example.org/about",
        "https://example.net/blog/2026/09/post",
        "mailto:someone@example.com",
    ];
    (i % 13 != 0).then(|| format!("{}?id={}", PAGES[i % 5], (i * 7919) % 10007))
}

/// A string column the default strategy compresses with vortex.onpair,
/// cascading its integer children through the compressor, across several
/// chunks and zones. The strings share long prefixes and vary only in a short
/// id, which is where OnPair beats FSST in the compressor's sampling; with
/// `onpair_row`'s more varied strings it picks FSST.
async fn write_onpair_default(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    const ROWS: usize = 20_000;

    let rows: Vec<Option<String>> = (0..ROWS).map(onpair_default_row).collect();
    let s = VarBinViewArray::from_iter_nullable_str(rows.iter().map(|r| r.as_deref())).into_array();
    let data = StructArray::from_fields(&[("s", s)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// A nullable vortex.bool sliced three bits in, written as-is by a flat
/// strategy, so both its value buffer and its validity child carry
/// `BoolMetadata.offset = 3`. Row `i` of the unsliced array is `i % 3 == 0`,
/// null when `i % 5 == 0`.
async fn write_bool_sliced(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    use vortex_array::arrays::BoolArray;

    let bools = BoolArray::from_iter((0..64).map(|i| (i % 5 != 0).then_some(i % 3 == 0)))
        .into_array()
        .slice(3..64)
        .expect("slice");
    let data = StructArray::from_fields(&[("b", bools)])
        .expect("from_fields")
        .into_array();
    let strategy = Arc::new(TableStrategy::new(
        Arc::new(FlatLayoutStrategy::default()),
        Arc::new(FlatLayoutStrategy::default()),
    ));
    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// Row `i` of the hand-built zigzag columns: the type's extremes, zero and -1
/// first, then small values alternating in sign.
fn zigzag_row(i: usize, min: i64, max: i64) -> i64 {
    match i {
        0 => min,
        1 => max,
        2 => 0,
        3 => -1,
        _ => {
            let magnitude = (i as i64 * 37) % 100;
            if i % 2 == 0 { magnitude } else { -magnitude }
        }
    }
}

/// Hand-built vortex.zigzag columns, one per signed width, plus a nullable
/// i32 (`n32`, null on every sixth row), written as-is by a flat strategy.
/// With `slice`, every column is sliced first, which slices each encoded child.
async fn write_zigzag_widths(
    session: &VortexSession,
    path: &PathBuf,
    slice: Option<std::ops::Range<usize>>,
) -> std::io::Result<()> {
    use vortex_zigzag::zigzag_encode;

    const ROWS: usize = 64;
    let zz = |array: PrimitiveArray| -> vortex_array::ArrayRef {
        let encoded = zigzag_encode(array.as_view()).expect("zigzag_encode").into_array();
        match &slice {
            Some(range) => encoded.slice(range.clone()).expect("slice"),
            None => encoded,
        }
    };

    let i8s = zz(PrimitiveArray::from_iter((0..ROWS).map(|i| zigzag_row(i, i8::MIN.into(), i8::MAX.into()) as i8)));
    let i16s = zz(PrimitiveArray::from_iter((0..ROWS).map(|i| zigzag_row(i, i16::MIN.into(), i16::MAX.into()) as i16)));
    let i32s = zz(PrimitiveArray::from_iter((0..ROWS).map(|i| zigzag_row(i, i32::MIN.into(), i32::MAX.into()) as i32)));
    let i64s = zz(PrimitiveArray::from_iter((0..ROWS).map(|i| zigzag_row(i, i64::MIN, i64::MAX))));
    let n32 = zz(PrimitiveArray::new(
        vortex_buffer::Buffer::from_iter((0..ROWS).map(|i| zigzag_row(i, i32::MIN.into(), i32::MAX.into()) as i32)),
        Validity::from_iter((0..ROWS).map(|i| i % 6 != 0)),
    ));

    let data = StructArray::from_fields(&[
        ("i8", i8s),
        ("i16", i16s),
        ("i32", i32s),
        ("i64", i64s),
        ("n32", n32),
    ])
    .expect("from_fields")
    .into_array();
    let strategy = Arc::new(TableStrategy::new(
        Arc::new(FlatLayoutStrategy::default()),
        Arc::new(FlatLayoutStrategy::default()),
    ));
    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// Signed columns the default strategy compresses with vortex.zigzag: values
/// near zero of both signs with rare huge outliers, which frame-of-reference
/// can't narrow but zigzag plus patched bit-packing can.
///   a: i32, (i * 7919) % 7 - 3, with +-1e9 on every 997th row, null on every 11th
///   b: i64, (i * 31) % 5 - 2, with -2^50 / 2^50 on rows 0 / 750 mod 1500
async fn write_zigzag_default(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    const ROWS: usize = 20_000;

    let a = PrimitiveArray::new(
        vortex_buffer::Buffer::from_iter((0..ROWS).map(|i| {
            if i % 997 == 0 {
                if i % 2 == 0 { 1_000_000_000 } else { -1_000_000_000 }
            } else {
                ((i * 7919) % 7) as i32 - 3
            }
        })),
        Validity::from_iter((0..ROWS).map(|i| i % 11 != 0)),
    )
    .into_array();
    let b = PrimitiveArray::from_iter((0..ROWS).map(|i| {
        if i % 1500 == 0 {
            -(1i64 << 50)
        } else if i % 1500 == 750 {
            1i64 << 50
        } else {
            (i as i64 * 31) % 5 - 2
        }
    }))
    .into_array();
    let data = StructArray::from_fields(&[("a", a), ("b", b)])
        .expect("from_fields")
        .into_array();

    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// Row `i` of the vortex.zstd fixtures: short sentences built from a small
/// vocabulary, so they compress, with an empty string and non-ASCII text.
fn zstd_row(i: usize) -> String {
    const WORDS: [&str; 12] = [
        "the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
        "zstd", "frames", "façade", "日本",
    ];
    if i % 17 == 3 {
        return String::new();
    }
    let n = 3 + (i * 7) % 9;
    (0..n)
        .map(|k| WORDS[(i * 31 + k * 11) % WORDS.len()])
        .collect::<Vec<_>>()
        .join(" ")
}

/// Writes each column as-is: a flat strategy has no compressor.
fn flat_strategy() -> Arc<TableStrategy> {
    Arc::new(TableStrategy::new(
        Arc::new(FlatLayoutStrategy::default()),
        Arc::new(FlatLayoutStrategy::default()),
    ))
}

async fn write_bytes(
    session: &VortexSession,
    path: &PathBuf,
    data: vortex_array::ArrayRef,
    strategy: Arc<dyn vortex_layout::LayoutStrategy>,
) -> std::io::Result<()> {
    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream())
        .await
        .expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// A hand-built vortex.zstd string column compressed as one frame, which is
/// too few samples to train a dictionary. Null on every fifth row.
async fn write_zstd_single_frame(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    use vortex_zstd::Zstd;

    let rows: Vec<Option<String>> = (0..64).map(|i| (i % 5 != 0).then(|| zstd_row(i))).collect();
    let strings = VarBinViewArray::from_iter_nullable_str(rows.iter().map(|r| r.as_deref()));
    let mut ctx = session.create_execution_ctx();
    let zstd = Zstd::from_var_bin_view(&strings, 3, 0, &mut ctx).expect("zstd").into_array();
    let data = StructArray::from_fields(&[("s", zstd)]).expect("from_fields").into_array();
    write_bytes(session, path, data, flat_strategy()).await
}

/// Hand-built vortex.zstd columns split into 100-value frames that share a
/// trained dictionary (20 frames is enough samples to train one):
///   s: nullable utf8, `zstd_row(i)`, null on every fifth row
///   b: binary, `zstd_row(i)`'s bytes followed by a 0xff byte
///   n: nullable i64, i * i - 1000, null on every seventh row
async fn write_zstd_framed(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    use vortex_zstd::Zstd;

    const ROWS: usize = 2000;
    let mut ctx = session.create_execution_ctx();

    let rows: Vec<Option<String>> = (0..ROWS).map(|i| (i % 5 != 0).then(|| zstd_row(i))).collect();
    let strings = VarBinViewArray::from_iter_nullable_str(rows.iter().map(|r| r.as_deref()));
    let s = Zstd::from_var_bin_view(&strings, 3, 100, &mut ctx).expect("zstd s");

    let blobs: Vec<Vec<u8>> = (0..ROWS)
        .map(|i| {
            let mut v = zstd_row(i).into_bytes();
            v.push(0xff);
            v
        })
        .collect();
    let binary = VarBinViewArray::from_iter_bin(blobs.iter().map(|b| b.as_slice()));
    let b = Zstd::from_var_bin_view(&binary, 3, 100, &mut ctx).expect("zstd b");

    let numbers = PrimitiveArray::new(
        vortex_buffer::Buffer::from_iter((0..ROWS).map(|i| (i * i) as i64 - 1000)),
        Validity::from_iter((0..ROWS).map(|i| i % 7 != 0)),
    );
    let n = Zstd::from_primitive(&numbers, 3, 100, &mut ctx).expect("zstd n");
    let data = StructArray::from_fields(&[("s", s.into_array()), ("b", b.into_array()), ("n", n.into_array())])
        .expect("from_fields")
        .into_array();
    write_bytes(session, path, data, flat_strategy()).await
}

/// Row `i` of `write_zstd_compact`: five sentences joined, null on every
/// thirteenth row.
fn zstd_compact_row(i: usize) -> Option<String> {
    (i % 13 != 0).then(|| {
        format!(
            "{} / {} / {} / {} / {}",
            zstd_row(i),
            zstd_row(i / 3),
            zstd_row(i / 7),
            zstd_row(i / 11),
            zstd_row(i / 19)
        )
    })
}

/// The same rows as utf8 (`s`) and as binary (`b`) through upstream's
/// "compact" write strategy, which adds vortex.zstd to the string and binary
/// schemes. For binary, the only competition is dictionary and plain varbin,
/// so zstd wins; for utf8, OnPair and FSST compete too.
async fn write_zstd_compact(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    use vortex::compressor::BtrBlocksCompressorBuilder;
    use vortex_file::WriteStrategyBuilder;

    const ROWS: usize = 20_000;
    let rows: Vec<Option<String>> = (0..ROWS).map(zstd_compact_row).collect();
    let s = VarBinViewArray::from_iter_nullable_str(rows.iter().map(|r| r.as_deref())).into_array();
    let b = VarBinViewArray::from_iter_nullable_bin(rows.iter().map(|r| r.as_deref().map(str::as_bytes)))
        .into_array();
    let data = StructArray::from_fields(&[("s", s), ("b", b)]).expect("from_fields").into_array();

    let strategy = WriteStrategyBuilder::default()
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();
    write_bytes(session, path, data, strategy).await
}

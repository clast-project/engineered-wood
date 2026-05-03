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

use vortex_array::Array;
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
use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::session::ArraySession;
use vortex_array::validity::Validity;
use vortex_buffer::buffer;
use vortex_file::WriteOptionsSessionExt;
use vortex_file::register_default_encodings;
use vortex_io::session::RuntimeSession;
use vortex_io::session::RuntimeSessionExt;
use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
use vortex_layout::layouts::table::TableStrategy;
use vortex_layout::session::LayoutSession;
use vortex_session::VortexSession;
use vortex_session::registry::Id;

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    // The session needs a tokio handle bound to the CURRENT runtime, so it must
    // be built inside `#[tokio::main]` rather than in a global LazyLock.
    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<LayoutSession>()
        .with::<ScalarFnSession>()
        .with::<RuntimeSession>()
        .with_tokio();
    register_default_encodings(&session);

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

    let mut allowed: vortex_utils::aliases::hash_set::HashSet<Id> =
        vortex_utils::aliases::hash_set::HashSet::default();
    allowed.insert(Id::new("vortex.primitive"));
    allowed.insert(Id::new("vortex.bool"));
    allowed.insert(Id::new("vortex.pco"));
    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default().with_allow_encodings(allowed)),
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

    let mut allowed: vortex_utils::aliases::hash_set::HashSet<Id> =
        vortex_utils::aliases::hash_set::HashSet::default();
    allowed.insert(Id::new("vortex.primitive"));
    allowed.insert(Id::new("vortex.bool"));
    allowed.insert(Id::new("fastlanes.bitpacked"));
    allowed.insert(Id::new("fastlanes.rle"));
    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default().with_allow_encodings(allowed)),
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

/// 2000-row sliced RLE column. Same input as rle_int_2048rows but the RLEData
/// is sliced to [10..2010] before serialization, so its metadata has offset=10
/// and length=2000. The decoder must handle the sub-chunk slice.
async fn write_rle_sliced(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    use vortex_fastlanes::RLE;
    use vortex_fastlanes::RLEData;
    use vortex_fastlanes::RLEArrayExt;

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

    let mut allowed: vortex_utils::aliases::hash_set::HashSet<Id> =
        vortex_utils::aliases::hash_set::HashSet::default();
    allowed.insert(Id::new("vortex.primitive"));
    allowed.insert(Id::new("vortex.bool"));
    allowed.insert(Id::new("fastlanes.bitpacked"));
    allowed.insert(Id::new("fastlanes.rle"));
    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default().with_allow_encodings(allowed)),
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

    let mut allowed: vortex_utils::aliases::hash_set::HashSet<Id> =
        vortex_utils::aliases::hash_set::HashSet::default();
    allowed.insert(Id::new("vortex.primitive"));
    allowed.insert(Id::new("vortex.bool"));
    allowed.insert(Id::new("vortex.pco"));
    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default().with_allow_encodings(allowed)),
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

    let mut allowed: vortex_utils::aliases::hash_set::HashSet<Id> =
        vortex_utils::aliases::hash_set::HashSet::default();
    allowed.insert(Id::new("vortex.primitive"));
    allowed.insert(Id::new("vortex.bool"));
    allowed.insert(Id::new("fastlanes.bitpacked"));
    allowed.insert(Id::new("fastlanes.delta"));
    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default().with_allow_encodings(allowed)),
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

    // Whitelist fastlanes.bitpacked so the writer preserves it rather than
    // canonicalizing to plain primitive.
    let mut allowed: vortex_utils::aliases::hash_set::HashSet<Id> =
        vortex_utils::aliases::hash_set::HashSet::default();
    allowed.insert(Id::new("vortex.primitive"));
    allowed.insert(Id::new("vortex.bool"));
    allowed.insert(Id::new("fastlanes.bitpacked"));
    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default().with_allow_encodings(allowed)),
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

    let mut allowed: vortex_utils::aliases::hash_set::HashSet<Id> =
        vortex_utils::aliases::hash_set::HashSet::default();
    allowed.insert(Id::new("vortex.primitive"));
    allowed.insert(Id::new("vortex.bool"));
    allowed.insert(Id::new("fastlanes.bitpacked"));
    allowed.insert(Id::new("fastlanes.delta"));
    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default().with_allow_encodings(allowed)),
    ));

    let mut bytes: Vec<u8> = Vec::new();
    session.write_options().with_strategy(strategy)
        .write(&mut bytes, data.to_array_stream()).await.expect("write");
    std::fs::write(path, &bytes)?;
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

async fn write_delta_int_2k(session: &VortexSession, path: &PathBuf) -> std::io::Result<()> {
    // Hand-construct a DeltaArray so the writer is forced to serialize it
    // (rather than the compressor choosing a different encoding). u64 column
    // because vortex-fastlanes Delta only supports unsigned integers.
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

    // Whitelist fastlanes.delta + its bitpacked child so the writer preserves
    // (rather than canonicalizes away) our delta-encoded array.
    let mut allowed: vortex_utils::aliases::hash_set::HashSet<Id> =
        vortex_utils::aliases::hash_set::HashSet::default();
    allowed.insert(Id::new("vortex.primitive"));
    allowed.insert(Id::new("vortex.bool"));
    allowed.insert(Id::new("fastlanes.bitpacked"));
    allowed.insert(Id::new("fastlanes.delta"));
    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default().with_allow_encodings(allowed)),
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

    let mut allowed: vortex_utils::aliases::hash_set::HashSet<Id> =
        vortex_utils::aliases::hash_set::HashSet::default();
    allowed.insert(Id::new("vortex.primitive"));
    allowed.insert(Id::new("vortex.bool"));
    allowed.insert(Id::new("fastlanes.bitpacked"));
    allowed.insert(Id::new("fastlanes.rle"));
    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default().with_allow_encodings(allowed)),
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

    let mut bytes: Vec<u8> = Vec::new();
    session.write_options().write(&mut bytes, data.to_array_stream()).await.expect("write");
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
    let mut allowed: vortex_utils::aliases::hash_set::HashSet<Id> =
        vortex_utils::aliases::hash_set::HashSet::default();
    allowed.insert(Id::new("vortex.primitive"));
    allowed.insert(Id::new("vortex.bool"));
    allowed.insert(Id::new("vortex.decimal"));
    let strategy = std::sync::Arc::new(TableStrategy::new(
        std::sync::Arc::new(FlatLayoutStrategy::default()),
        std::sync::Arc::new(FlatLayoutStrategy::default().with_allow_encodings(allowed)),
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

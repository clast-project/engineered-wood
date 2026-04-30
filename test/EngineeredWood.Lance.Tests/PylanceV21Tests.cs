// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Format;
using EngineeredWood.Lance.Tests.TestData;

namespace EngineeredWood.Lance.Tests;

/// <summary>
/// Phase 6 cross-validation — v2.1 files from pylance. Scope is deliberately
/// narrow: <c>MiniBlockLayout</c> with <c>Flat</c> values targeting primitive
/// (non-Boolean) columns, with single-layer rep/def (<c>ALL_VALID_ITEM</c> or
/// <c>NULLABLE_ITEM</c>), spanning any number of chunks.
/// </summary>
public class PylanceV21Tests
{
    [Fact]
    public async Task Int32_NoNulls_SingleChunk()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("int32_v21.lance"));
        Assert.Equal(LanceVersion.V2_1, reader.Version);
        Assert.Equal(5L, reader.NumberOfRows);

        var arr = (Int32Array)await reader.ReadColumnAsync(0);
        Assert.Equal(5, arr.Length);
        Assert.Equal(0, arr.NullCount);
        Assert.Equal(new int?[] { 1, 2, 3, 4, 5 }, arr.ToArray());
    }

    [Fact]
    public async Task Int32_WithNulls_NullableItem()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("int32_nulls_v21.lance"));
        var arr = (Int32Array)await reader.ReadColumnAsync(0);
        Assert.Equal(5, arr.Length);
        Assert.Equal(2, arr.NullCount);
        Assert.Equal(1, arr.GetValue(0));
        Assert.Null(arr.GetValue(1));
        Assert.Equal(3, arr.GetValue(2));
        Assert.Null(arr.GetValue(3));
        Assert.Equal(5, arr.GetValue(4));
    }

    [Fact]
    public async Task Int64_NoNulls()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("int64_v21.lance"));
        var arr = (Int64Array)await reader.ReadColumnAsync(0);
        Assert.Equal(new long?[] { 10, 20, 30, 40, 50 }, arr.ToArray());
    }

    [Fact]
    public async Task Double_NoNulls()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("double_v21.lance"));
        var arr = (DoubleArray)await reader.ReadColumnAsync(0);
        Assert.Equal(new double?[] { 1.5, 2.5, 3.5 }, arr.ToArray());
    }

    [Fact]
    public async Task LargeInt32_MultiChunk()
    {
        // 2000 values × 4 bytes > 8 KiB threshold → multiple mini-block
        // chunks. The data generator uses a PRNG-like sequence so pylance
        // emits Flat (not InlineBitpacking, which is Phase 9 / Fastlanes).
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("large_int32_v21.lance"));
        Assert.Equal(2000L, reader.NumberOfRows);

        var arr = (Int32Array)await reader.ReadColumnAsync(0);
        Assert.Equal(2000, arr.Length);
        Assert.Equal(0, arr.NullCount);

        // Must match the generator formula `i * 1103515245 + 12345`
        // (mod 2^32, signed int32 wrap) for every row.
        for (int i = 0; i < 2000; i++)
        {
            int expected = unchecked(i * 1103515245 + 12345);
            Assert.Equal(expected, arr.GetValue(i));
        }
    }

    // --- Phase 7: Variable encoding and single-column lists ---

    [Fact]
    public async Task String_NonNull_Variable()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("string_v21.lance"));
        var arr = (StringArray)await reader.ReadColumnAsync(0);
        Assert.Equal(3, arr.Length);
        Assert.Equal(0, arr.NullCount);
        Assert.Equal("foo", arr.GetString(0));
        Assert.Equal("bar", arr.GetString(1));
        Assert.Equal("baz", arr.GetString(2));
    }

    [Fact]
    public async Task String_WithNulls_Variable()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("string_nulls_v21.lance"));
        var arr = (StringArray)await reader.ReadColumnAsync(0);
        Assert.Equal(3, arr.Length);
        Assert.Equal(1, arr.NullCount);
        Assert.Equal("foo", arr.GetString(0));
        Assert.True(arr.IsNull(1));
        Assert.Equal("baz", arr.GetString(2));
    }

    [Fact]
    public async Task List_Int32_WithEmpty()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("list_int_v21.lance"));
        Assert.IsType<Apache.Arrow.Types.ListType>(reader.Schema.FieldsList[0].DataType);

        var arr = (ListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(4, arr.Length);
        Assert.Equal(0, arr.NullCount);

        Assert.Equal(new int?[] { 1, 2, 3 }, ((Int32Array)arr.GetSlicedValues(0)).ToArray());
        Assert.Equal(new int?[] { 4 }, ((Int32Array)arr.GetSlicedValues(1)).ToArray());
        Assert.Equal(new int?[] { }, ((Int32Array)arr.GetSlicedValues(2)).ToArray());
        Assert.Equal(new int?[] { 5, 6 }, ((Int32Array)arr.GetSlicedValues(3)).ToArray());
    }

    [Fact]
    public async Task List_Of_FixedSizeList_Float32_WithInnerNulls()
    {
        // list<FSL<float32, 3>> with inner None values.
        // Outer rows: [[1, _, 3], [4, 5, _]], None, [[7, 8, 9]]
        // → 3 visible FSL rows, 9 inner floats, 2 inner nulls.
        await using var reader = await LanceFileReader.OpenAsync(
            TestDataPath.Resolve("list_fsl_inner_nulls_v21.lance"));
        var outer = (ListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(3, outer.Length);
        Assert.Equal(1, outer.NullCount);
        Assert.True(outer.IsNull(1));

        var fsls = (FixedSizeListArray)outer.Values;
        Assert.Equal(3, fsls.Length);
        Assert.Equal(0, fsls.NullCount);

        var inner = (FloatArray)fsls.Values;
        Assert.Equal(9, inner.Length);
        Assert.Equal(2, inner.NullCount);
        // Row 0 inner: [1, _, 3, 4, 5, _]; row 2 inner: [7, 8, 9]
        Assert.Equal(1f, inner.GetValue(0));
        Assert.Null(inner.GetValue(1));
        Assert.Equal(3f, inner.GetValue(2));
        Assert.Equal(4f, inner.GetValue(3));
        Assert.Equal(5f, inner.GetValue(4));
        Assert.Null(inner.GetValue(5));
        Assert.Equal(7f, inner.GetValue(6));
        Assert.Equal(8f, inner.GetValue(7));
        Assert.Equal(9f, inner.GetValue(8));
    }

    [Fact]
    public async Task Struct_With_FixedSizeList_Float32()
    {
        // struct<int32, FSL<float32, 3>> — FSL inside a struct.
        await using var reader = await LanceFileReader.OpenAsync(
            TestDataPath.Resolve("struct_fsl_v21.lance"));
        var st = (StructArray)await reader.ReadColumnAsync(0);
        Assert.Equal(3, st.Length);
        Assert.Equal(0, st.NullCount);

        var ids = (Int32Array)st.Fields[0];
        Assert.Equal(new int?[] { 1, 2, 3 }, ids.ToArray());

        var emb = (FixedSizeListArray)st.Fields[1];
        Assert.Equal(3, emb.Length);
        Assert.Equal(3, ((FixedSizeListType)emb.Data.DataType).ListSize);
        var inner = (FloatArray)emb.Values;
        Assert.Equal(9, inner.Length);
        Assert.Equal(new float?[] { 1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f }, inner.ToArray());
    }

    [Fact]
    public async Task List_Of_FixedSizeList_Float32()
    {
        // list<FSL<float32, 3>> — exercises FSL inside the recursive walker.
        // Outer rows: [[1,2,3],[4,5,6]], [], null, [[7,8,9]].
        await using var reader = await LanceFileReader.OpenAsync(
            TestDataPath.Resolve("list_fsl_float_v21.lance"));
        Assert.IsType<Apache.Arrow.Types.ListType>(reader.Schema.FieldsList[0].DataType);

        var outer = (ListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(4, outer.Length);
        Assert.Equal(1, outer.NullCount);
        Assert.True(outer.IsNull(2));
        Assert.False(outer.IsNull(1));

        Assert.Equal(0, outer.ValueOffsets[0]);
        Assert.Equal(2, outer.ValueOffsets[1]);  // row 0 has 2 FSL items
        Assert.Equal(2, outer.ValueOffsets[2]);  // row 1 empty
        Assert.Equal(2, outer.ValueOffsets[3]);  // row 2 null
        Assert.Equal(3, outer.ValueOffsets[4]);  // row 3 has 1 FSL item

        var fsls = (FixedSizeListArray)outer.Values;
        Assert.Equal(3, fsls.Length);
        Assert.Equal(0, fsls.NullCount);
        Assert.Equal(3, ((FixedSizeListType)fsls.Data.DataType).ListSize);

        var inner = (FloatArray)fsls.Values;
        Assert.Equal(9, inner.Length);
        Assert.Equal(new float?[] { 1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f }, inner.ToArray());
    }

    [Fact]
    public async Task List_Int32_BigMultiChunk()
    {
        // 100K rows × 1-5 ints/list with full-int32-range values. pylance
        // emits ~290 mini-block chunks for the single leaf page with
        // InlineBitpacking-compressed rep/def streams; this exercises both
        // multi-chunk concatenation in DecodeNestedLeafChunk and
        // InlineBitpacking decompression of the rep/def buffers.
        await using var reader = await LanceFileReader.OpenAsync(
            TestDataPath.Resolve("big_list_int_v21.lance"));
        Assert.Equal(100_000L, reader.NumberOfRows);

        var arr = (ListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(100_000, arr.Length);
        Assert.Equal(0, arr.NullCount);

        for (int i = 0; i < 100_000; i++)
        {
            int len = arr.GetValueLength(i);
            Assert.InRange(len, 1, 5);
        }
        // Total leaf items in [N, 5N]; with avg list length ~3 we expect ~3N.
        var values = (Int32Array)arr.Values;
        Assert.InRange(values.Length, 100_000, 500_000);
        // Values use the full int32 range (PRNG-style), so the mean of
        // any reasonably-sized prefix should land near zero. Cheap check
        // the data isn't a constant or zeros.
        long sumAbs = 0;
        int probe = Math.Min(values.Length, 1000);
        for (int i = 0; i < probe; i++)
            sumAbs += Math.Abs((long)values.GetValue(i)!.Value);
        Assert.True(sumAbs > probe * 1_000_000L,
            $"Expected wide-range int32 leaf values; got |Σ|={sumAbs} over {probe} samples.");
    }

    [Fact]
    public async Task List_Int32_WithNulls()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("list_nulls_v21.lance"));
        var arr = (ListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(3, arr.Length);
        Assert.Equal(1, arr.NullCount);

        Assert.Equal(new int?[] { 1, 2 }, ((Int32Array)arr.GetSlicedValues(0)).ToArray());
        Assert.True(arr.IsNull(1));
        Assert.Equal(new int?[] { 3, 4 }, ((Int32Array)arr.GetSlicedValues(2)).ToArray());
    }

    // --- Phase 8: FullZipLayout (large fixed-size values) ---

    [Fact]
    public async Task FullZip_Embeddings_Float32_x1024()
    {
        // 10 rows × 1024 float32 = 40 KiB total, 4 KiB per row → FullZipLayout.
        // Generator: value[row, j] = float(row * 1024 + j).
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("embeddings_v21.lance"));
        var fslType = Assert.IsType<Apache.Arrow.Types.FixedSizeListType>(reader.Schema.FieldsList[0].DataType);
        Assert.Equal(1024, fslType.ListSize);
        Assert.IsType<Apache.Arrow.Types.FloatType>(fslType.ValueDataType);

        var fsl = (Apache.Arrow.FixedSizeListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(10, fsl.Length);
        Assert.Equal(0, fsl.NullCount);

        var values = (Apache.Arrow.FloatArray)fsl.Values;
        Assert.Equal(10 * 1024, values.Length);

        // Spot-check row 0, row 5, and the final value.
        Assert.Equal(0f, values.GetValue(0));
        Assert.Equal(1023f, values.GetValue(1023));
        Assert.Equal(5f * 1024, values.GetValue(5 * 1024));
        Assert.Equal(10f * 1024 - 1, values.GetValue(10 * 1024 - 1));
    }

    [Fact]
    public async Task FullZip_ListOfBigEmbeddings_Float32_x1024()
    {
        // list<FSL<float32, 1024>> — pylance encodes via FullZipLayout
        // with bits_rep=1, bits_def=2 (NULL_AND_EMPTY_LIST). Each row of
        // buffer 0 starts with a 1-byte ctrl word; visible items are
        // followed by 4096-byte FSL payloads. Outer rows: 2 FSLs, [],
        // null, [1 FSL] → 5 rep/def levels, 3 visible items.
        await using var reader = await LanceFileReader.OpenAsync(
            TestDataPath.Resolve("list_big_embeddings_v21.lance"));
        var outer = (ListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(4, outer.Length);
        Assert.Equal(1, outer.NullCount);
        Assert.True(outer.IsNull(2));
        Assert.False(outer.IsNull(1));   // empty list is valid
        Assert.Equal(0, outer.ValueOffsets[0]);
        Assert.Equal(2, outer.ValueOffsets[1]);
        Assert.Equal(2, outer.ValueOffsets[2]);
        Assert.Equal(2, outer.ValueOffsets[3]);
        Assert.Equal(3, outer.ValueOffsets[4]);

        var fsls = (FixedSizeListArray)outer.Values;
        Assert.Equal(3, fsls.Length);
        Assert.Equal(1024, ((FixedSizeListType)fsls.Data.DataType).ListSize);
        var inner = (FloatArray)fsls.Values;
        Assert.Equal(3 * 1024, inner.Length);

        // Row 0: floats 0..1023 then 1..1024.
        Assert.Equal(0f, inner.GetValue(0));
        Assert.Equal(1023f, inner.GetValue(1023));
        Assert.Equal(1f, inner.GetValue(1024));
        Assert.Equal(1024f, inner.GetValue(2047));
        // Row 3 (after empty + null): floats 2..1025.
        Assert.Equal(2f, inner.GetValue(2048));
        Assert.Equal(1025f, inner.GetValue(3071));
    }

    [Fact]
    public async Task FullZip_NullableEmbeddings_Float32_x1024()
    {
        // 5 rows × 1024 float32, alternating valid/null: pylance encodes
        // this as FullZipLayout with bits_def=1 (1 def-byte per row) and
        // FSL has_validity=true (per-row inner-validity bitmap, all-1s
        // for valid rows). Exercises the row-validity stripping path
        // plus the FSL inner-validity bitmap concat.
        await using var reader = await LanceFileReader.OpenAsync(
            TestDataPath.Resolve("nullable_embeddings_v21.lance"));
        var fslType = Assert.IsType<FixedSizeListType>(reader.Schema.FieldsList[0].DataType);
        Assert.Equal(1024, fslType.ListSize);

        var fsl = (FixedSizeListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(5, fsl.Length);
        Assert.Equal(2, fsl.NullCount);
        Assert.False(fsl.IsNull(0));
        Assert.True(fsl.IsNull(1));
        Assert.False(fsl.IsNull(2));
        Assert.True(fsl.IsNull(3));
        Assert.False(fsl.IsNull(4));

        var values = (FloatArray)fsl.Values;
        Assert.Equal(5 * 1024, values.Length);
        // Row 0: floats 0..1023, row 2: floats 0..1023, row 4: floats 0..1023.
        Assert.Equal(0f, values.GetValue(0));
        Assert.Equal(1023f, values.GetValue(1023));
        Assert.Equal(0f, values.GetValue(2 * 1024));
        Assert.Equal(1023f, values.GetValue(2 * 1024 + 1023));
        Assert.Equal(0f, values.GetValue(4 * 1024));
    }

    // --- Phase 9: Fastlanes InlineBitpacking ---

    [Fact]
    public async Task InlineBitpacking_Int32_Sequential2000_MultiChunk()
    {
        // pylance picks InlineBitpacking for low-entropy int32. With 2000
        // sequential values the page splits into 2 mini-block chunks at
        // distinct bit widths (10 bits then 11 bits).
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("inline_bp_int32_v21.lance"));
        Assert.Equal(2000L, reader.NumberOfRows);

        var arr = (Int32Array)await reader.ReadColumnAsync(0);
        Assert.Equal(2000, arr.Length);
        Assert.Equal(0, arr.NullCount);
        for (int i = 0; i < 2000; i++)
            Assert.Equal(i, arr.GetValue(i));
    }

    [Fact]
    public async Task InlineBitpacking_Int32_SingleChunk()
    {
        // 0, 5, 10, ..., 995 — 200 values fit in one mini-block chunk.
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("inline_bp_int32_small_v21.lance"));
        Assert.Equal(200L, reader.NumberOfRows);

        var arr = (Int32Array)await reader.ReadColumnAsync(0);
        Assert.Equal(200, arr.Length);
        for (int i = 0; i < 200; i++)
            Assert.Equal(i * 5, arr.GetValue(i));
    }

    // --- MiniBlockLayout-level dictionary (low-cardinality strings) ---

    [Fact]
    public async Task Dictionary_Strings_NoNulls()
    {
        // 5 unique values × 20 reps → MiniBlockLayout.dictionary path.
        await using var reader = await LanceFileReader.OpenAsync(
            TestDataPath.Resolve("dict_strings_v21.lance"));
        var arr = (StringArray)await reader.ReadColumnAsync(0);
        Assert.Equal(100, arr.Length);
        Assert.Equal(0, arr.NullCount);

        string[] expected = { "cat", "dog", "fish", "bird", "snake" };
        for (int i = 0; i < 100; i++)
            Assert.Equal(expected[i % 5], arr.GetString(i));
    }

    [Fact]
    public async Task Dictionary_Strings_WithNulls()
    {
        // Same dupes, but every 5th row is null. Because that's also where
        // 'cat' lives, all 'cat' rows are nulled.
        await using var reader = await LanceFileReader.OpenAsync(
            TestDataPath.Resolve("dict_strings_nulls_v21.lance"));
        var arr = (StringArray)await reader.ReadColumnAsync(0);
        Assert.Equal(100, arr.Length);
        Assert.Equal(20, arr.NullCount); // every 5th row

        string[] expected = { "cat", "dog", "fish", "bird", "snake" };
        for (int i = 0; i < 100; i++)
        {
            if (i % 5 == 0)
                Assert.True(arr.IsNull(i), $"Row {i} should be null");
            else
                Assert.Equal(expected[i % 5], arr.GetString(i));
        }
    }

    // --- Multi-chunk Variable in MiniBlock ---

    [Fact]
    public async Task MultiChunk_Variable_Strings_NoNulls()
    {
        // 200 varied strings → pylance splits into ~6 mini-block chunks.
        await using var reader = await LanceFileReader.OpenAsync(
            TestDataPath.Resolve("multichunk_strings_v21.lance"));
        Assert.Equal(200L, reader.NumberOfRows);

        var arr = (StringArray)await reader.ReadColumnAsync(0);
        Assert.Equal(200, arr.Length);
        Assert.Equal(0, arr.NullCount);
        for (int i = 0; i < 200; i++)
        {
            string expected = $"row-{i:D6}-{new string('x', 50 + (i * 17) % 30)}-end";
            Assert.Equal(expected, arr.GetString(i));
        }
    }

    [Fact]
    public async Task MultiChunk_Variable_Strings_WithNulls()
    {
        // Same 200 rows, but every 7th row (rows where i % 7 == 3) is null.
        await using var reader = await LanceFileReader.OpenAsync(
            TestDataPath.Resolve("multichunk_strings_nulls_v21.lance"));
        var arr = (StringArray)await reader.ReadColumnAsync(0);
        Assert.Equal(200, arr.Length);

        int expectedNulls = 0;
        for (int i = 0; i < 200; i++)
            if (i % 7 == 3) expectedNulls++;
        Assert.Equal(expectedNulls, arr.NullCount);

        for (int i = 0; i < 200; i++)
        {
            if (i % 7 == 3)
            {
                Assert.True(arr.IsNull(i), $"Row {i} should be null");
            }
            else
            {
                string expected = $"row-{i:D6}-{new string('x', 50 + (i * 17) % 30)}-end";
                Assert.Equal(expected, arr.GetString(i));
            }
        }
    }

    // --- Phase 7b: Multi-leaf struct support ---

    [Fact]
    public async Task Struct_TwoInt32_NoNulls()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("struct_2i32_v21.lance"));
        Assert.IsType<Apache.Arrow.Types.StructType>(reader.Schema.FieldsList[0].DataType);

        var arr = (Apache.Arrow.StructArray)await reader.ReadColumnAsync(0);
        Assert.Equal(3, arr.Length);
        Assert.Equal(0, arr.NullCount);

        var a = (Int32Array)arr.Fields[0];
        var b = (Int32Array)arr.Fields[1];
        Assert.Equal(new int?[] { 1, 2, 3 }, a.ToArray());
        Assert.Equal(new int?[] { 10, 20, 30 }, b.ToArray());
    }

    [Fact]
    public async Task Struct_TwoInt32_Nullable_StructNullCascades()
    {
        // Input: [{a:1,b:10}, null, {a:3,b:30}]. v2.1 def levels [0, 2, 0]
        // — def=2 means struct is null and cascades to both children. Unlike
        // v2.0 SimpleStruct (which loses struct-level nulls), v2.1 actually
        // preserves them.
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("struct_2i32_nullable_v21.lance"));
        var arr = (Apache.Arrow.StructArray)await reader.ReadColumnAsync(0);
        Assert.Equal(3, arr.Length);
        Assert.Equal(1, arr.NullCount);
        Assert.False(arr.IsNull(0));
        Assert.True(arr.IsNull(1));
        Assert.False(arr.IsNull(2));

        var a = (Int32Array)arr.Fields[0];
        var b = (Int32Array)arr.Fields[1];
        Assert.Equal(1, a.GetValue(0));
        Assert.Null(a.GetValue(1));   // cascaded
        Assert.Equal(3, a.GetValue(2));
        Assert.Equal(10, b.GetValue(0));
        Assert.Null(b.GetValue(1));   // cascaded
        Assert.Equal(30, b.GetValue(2));
    }

    [Fact]
    public async Task Struct_MixedLayerKinds_NonNullStructWithNullableChild()
    {
        // Schema: struct<a: int32 not null, b: int32 nullable> with the
        // *struct* itself non-nullable. pylance picks different RepDefLayer
        // combinations for the two children:
        //   col[0] (a): [ALL_VALID_ITEM, ALL_VALID_ITEM]  — no def buffer
        //   col[1] (b): [NULLABLE_ITEM,  ALL_VALID_ITEM]  — def∈{0,1}
        // Cross-column coherence here is "neither column declares the struct
        // nullable", so the assembler must accept null vs null struct-validity
        // bitmaps even though one child has a def buffer and the other doesn't.
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("struct_inner_nullable_v21.lance"));
        var arr = (Apache.Arrow.StructArray)await reader.ReadColumnAsync(0);
        Assert.Equal(3, arr.Length);
        Assert.Equal(0, arr.NullCount);

        var a = (Int32Array)arr.Fields[0];
        var b = (Int32Array)arr.Fields[1];
        Assert.Equal(new int?[] { 1, 2, 3 }, a.ToArray());
        Assert.Equal(0, a.NullCount);
        Assert.Equal(10, b.GetValue(0));
        Assert.Null(b.GetValue(1));
        Assert.Equal(30, b.GetValue(2));
        Assert.Equal(1, b.NullCount);
    }

    [Fact]
    public async Task ListOfStruct_NoNulls()
    {
        // Three rows of varying list lengths: [[{a:1,b:10},{a:2,b:20}],
        // [{a:3,b:30}], [{a:4,b:40},{a:5,b:50},{a:6,b:60}]]. Layers are
        // [ALL_VALID_ITEM, ALL_VALID_ITEM, ALL_VALID_LIST] for both columns,
        // so there is no def buffer — list boundaries come from the rep
        // buffer alone.
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("list_struct_v21.lance"));
        var topField = reader.Schema.FieldsList[0];
        var listType = Assert.IsType<Apache.Arrow.Types.ListType>(topField.DataType);
        Assert.IsType<Apache.Arrow.Types.StructType>(listType.ValueDataType);

        var list = (ListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(3, list.Length);
        Assert.Equal(0, list.NullCount);

        var inner = (Apache.Arrow.StructArray)list.Values;
        Assert.Equal(6, inner.Length);
        Assert.Equal(0, inner.NullCount);

        var a = (Int32Array)inner.Fields[0];
        var b = (Int32Array)inner.Fields[1];
        Assert.Equal(new int?[] { 1, 2, 3, 4, 5, 6 }, a.ToArray());
        Assert.Equal(new int?[] { 10, 20, 30, 40, 50, 60 }, b.ToArray());

        // Row boundaries.
        Assert.Equal(0, list.ValueOffsets[0]);
        Assert.Equal(2, list.ValueOffsets[1]);
        Assert.Equal(3, list.ValueOffsets[2]);
        Assert.Equal(6, list.ValueOffsets[3]);
    }

    [Fact]
    public async Task ListOfStruct_NullableEverything()
    {
        // Five rows mixing valid lists, a null list, an empty list, and a
        // struct-null inside a list:
        //   [[{a:1,b:10},{a:2,b:20}], None, [], [{a:3,b:30},None,{a:5,b:50}], [{a:6,b:60}]]
        // Layers: [NULLABLE_ITEM, NULLABLE_ITEM, NULL_AND_EMPTY_LIST]. Tests
        // every path through the rep/def walker — list-null, list-empty,
        // mid-list struct-null cascading to children.
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("list_struct_nullable_v21.lance"));
        var list = (ListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(5, list.Length);
        Assert.Equal(1, list.NullCount);
        Assert.False(list.IsNull(0));
        Assert.True(list.IsNull(1));   // None list
        Assert.False(list.IsNull(2));  // empty list (valid, just empty)
        Assert.False(list.IsNull(3));
        Assert.False(list.IsNull(4));

        Assert.Equal(0, list.ValueOffsets[0]);
        Assert.Equal(2, list.ValueOffsets[1]);
        Assert.Equal(2, list.ValueOffsets[2]);  // null list spans nothing
        Assert.Equal(2, list.ValueOffsets[3]);  // empty list spans nothing
        Assert.Equal(5, list.ValueOffsets[4]);
        Assert.Equal(6, list.ValueOffsets[5]);

        var inner = (Apache.Arrow.StructArray)list.Values;
        Assert.Equal(6, inner.Length);
        Assert.Equal(1, inner.NullCount);
        Assert.True(inner.IsNull(3));   // the None inside row 3's list

        var a = (Int32Array)inner.Fields[0];
        var b = (Int32Array)inner.Fields[1];
        Assert.Equal(1, a.GetValue(0));
        Assert.Equal(2, a.GetValue(1));
        Assert.Equal(3, a.GetValue(2));
        Assert.Null(a.GetValue(3));     // struct-null cascades to children
        Assert.Equal(5, a.GetValue(4));
        Assert.Equal(6, a.GetValue(5));
        Assert.Equal(10, b.GetValue(0));
        Assert.Null(b.GetValue(3));
        Assert.Equal(60, b.GetValue(5));
    }

    [Fact]
    public async Task StructOfStruct_NoNulls()
    {
        // struct<inner: struct<a: int32, b: int32>>. Three rows, all valid.
        // Layers `[ALL_VALID, ALL_VALID, ALL_VALID]`, no def buffer — every
        // row consumes a slot per leaf.
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("struct_of_struct_v21.lance"));
        var outer = (Apache.Arrow.StructArray)await reader.ReadColumnAsync(0);
        Assert.Equal(3, outer.Length);
        Assert.Equal(0, outer.NullCount);
        var inner = (Apache.Arrow.StructArray)outer.Fields[0];
        Assert.Equal(3, inner.Length);
        Assert.Equal(0, inner.NullCount);
        var a = (Int32Array)inner.Fields[0];
        var b = (Int32Array)inner.Fields[1];
        Assert.Equal(new int?[] { 17, 29, 41 }, a.ToArray());
        Assert.Equal(new int?[] { 113, 227, 313 }, b.ToArray());
    }

    [Fact]
    public async Task StructOfStruct_NullableCascades()
    {
        // Six rows mixing all-valid, inner-null, and outer-null:
        //   [{inner:{a:17,b:113}}, {inner:None}, None, {inner:{a:41,b:313}},
        //    {inner:{a:53,b:419}}, {inner:{a:67,b:521}}]
        // Layers all `NULLABLE_ITEM`; def values 0=valid, 2=inner-null,
        // 3=outer-null. Every row gets a value slot (placeholder 0 for
        // inner-null and outer-null), unlike list-bearing shapes.
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("struct_of_struct_nullable_v21.lance"));
        var outer = (Apache.Arrow.StructArray)await reader.ReadColumnAsync(0);
        Assert.Equal(6, outer.Length);
        Assert.Equal(1, outer.NullCount);
        Assert.False(outer.IsNull(0));
        Assert.False(outer.IsNull(1));
        Assert.True(outer.IsNull(2));   // outer-null row
        Assert.False(outer.IsNull(3));

        var inner = (Apache.Arrow.StructArray)outer.Fields[0];
        Assert.Equal(6, inner.Length);
        Assert.Equal(2, inner.NullCount);  // inner-null + outer-null cascade
        Assert.False(inner.IsNull(0));
        Assert.True(inner.IsNull(1));      // inner-null
        Assert.True(inner.IsNull(2));      // cascaded from outer-null
        Assert.False(inner.IsNull(3));

        var a = (Int32Array)inner.Fields[0];
        var b = (Int32Array)inner.Fields[1];
        Assert.Equal(2, a.NullCount);
        Assert.Equal(17, a.GetValue(0));
        Assert.Null(a.GetValue(1));
        Assert.Null(a.GetValue(2));
        Assert.Equal(41, a.GetValue(3));
        Assert.Equal(53, a.GetValue(4));
        Assert.Equal(67, a.GetValue(5));
        Assert.Equal(113, b.GetValue(0));
        Assert.Null(b.GetValue(1));
        Assert.Null(b.GetValue(2));
        Assert.Equal(521, b.GetValue(5));
    }

    [Fact]
    public async Task StructOfList_MixedNullsAndEmpties()
    {
        // struct<xs: list<int32>>. Five rows:
        //   [{xs:[1,2,3]}, {xs:[]}, {xs:[4,5]}, None, {xs:None}]
        // Layers `[ALL_VALID_ITEM, NULL_AND_EMPTY_LIST, NULLABLE_ITEM]`.
        // def slots: 1=list-null, 2=list-empty, 3=outer-null. All three
        // skip value slots; only def=0 produces a visible item.
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("struct_of_list_v21.lance"));
        var outer = (Apache.Arrow.StructArray)await reader.ReadColumnAsync(0);
        Assert.Equal(5, outer.Length);
        Assert.Equal(1, outer.NullCount);
        Assert.True(outer.IsNull(3));   // outer-null row
        Assert.False(outer.IsNull(0));
        Assert.False(outer.IsNull(4));

        var list = (ListArray)outer.Fields[0];
        Assert.Equal(5, list.Length);
        // List null at row 3 (cascaded from outer) and row 4 (genuine list-null).
        Assert.Equal(2, list.NullCount);
        Assert.True(list.IsNull(3));
        Assert.True(list.IsNull(4));
        Assert.False(list.IsNull(1));   // empty list, valid

        Assert.Equal(0, list.ValueOffsets[0]);
        Assert.Equal(3, list.ValueOffsets[1]);
        Assert.Equal(3, list.ValueOffsets[2]);  // empty list spans nothing
        Assert.Equal(5, list.ValueOffsets[3]);
        Assert.Equal(5, list.ValueOffsets[4]);  // outer-null spans nothing
        Assert.Equal(5, list.ValueOffsets[5]);  // list-null spans nothing

        var values = (Int32Array)list.Values;
        Assert.Equal(5, values.Length);
        Assert.Equal(0, values.NullCount);
        Assert.Equal(new int?[] { 1, 2, 3, 4, 5 }, values.ToArray());
    }

    [Fact]
    public async Task Struct_OuterStruct_ListOfStruct()
    {
        // struct<m: list<struct<a, b>>>. Each leaf has 4 layers
        // [item, inner_struct, list, outer_struct]. Exercises the recursive
        // walker through outer-struct → list → inner-struct → primitive
        // without any per-shape code path.
        // 8 rows: row 3 has list=None (list-null inside valid outer),
        //         row 5 has outer=None (cascades),
        //         row 6 has list=[]  (empty),
        //         others have a 2-element list of {a, b}.
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("struct_list_struct_v21.lance"));
        var s = (Apache.Arrow.StructArray)await reader.ReadColumnAsync(0);
        Assert.Equal(8, s.Length);
        Assert.Equal(1, s.NullCount);
        Assert.True(s.IsNull(5));

        var m = (ListArray)s.Fields[0];
        Assert.Equal(8, m.Length);
        Assert.Equal(2, m.NullCount);   // row 3 (list-null) + row 5 (outer cascade)
        Assert.True(m.IsNull(3));
        Assert.True(m.IsNull(5));
        Assert.False(m.IsNull(6));      // empty list, valid

        // Visible items: row 0,1,2,4,7 each contribute 2 → 10 items.
        var inner = (Apache.Arrow.StructArray)m.Values;
        Assert.Equal(10, inner.Length);
        Assert.Equal(0, inner.NullCount);

        var a = (Int32Array)inner.Fields[0];
        var b = (Int32Array)inner.Fields[1];
        // Row 0 contributes a[0]=17, a[1]=18; b[0]=113, b[1]=114
        Assert.Equal(17, a.GetValue(0));
        Assert.Equal(18, a.GetValue(1));
        Assert.Equal(113, b.GetValue(0));
        Assert.Equal(114, b.GetValue(1));
        // Row 7 (last contributing row) goes at items 8..9: a = 17 + 7*11 = 94, +1 = 95
        Assert.Equal(94, a.GetValue(8));
        Assert.Equal(95, a.GetValue(9));
        Assert.Equal(113 + 7 * 19, b.GetValue(8));
    }

    [Fact]
    public async Task ListOfStructOfList()
    {
        // list<struct<list<int32>>> — struct *between* two list layers.
        // Tests that struct-between-lists has different cascade behaviour
        // from list-cascade: an outer-list null/empty skips inner-list
        // counting (cascading via outer-list), but a struct null
        // (between lists) doesn't skip inner-list counting (struct's child
        // shares struct's length).
        // Input: [[{m:[1,2]},{m:[3]}], [{m:[]},{m:[4,5]}], None, [{m:[6]}]]
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("list_struct_list_int_v21.lance"));
        var outer = (ListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(4, outer.Length);
        Assert.Equal(1, outer.NullCount);
        Assert.True(outer.IsNull(2));
        Assert.Equal(0, outer.ValueOffsets[0]);
        Assert.Equal(2, outer.ValueOffsets[1]);
        Assert.Equal(4, outer.ValueOffsets[2]);
        Assert.Equal(4, outer.ValueOffsets[3]);   // null outer spans nothing
        Assert.Equal(5, outer.ValueOffsets[4]);

        var s = (Apache.Arrow.StructArray)outer.Values;
        Assert.Equal(5, s.Length);
        Assert.Equal(0, s.NullCount);

        var m = (ListArray)s.Fields[0];
        Assert.Equal(5, m.Length);
        Assert.Equal(0, m.ValueOffsets[0]);    // [1,2]
        Assert.Equal(2, m.ValueOffsets[1]);    // [3]
        Assert.Equal(3, m.ValueOffsets[2]);    // []
        Assert.Equal(3, m.ValueOffsets[3]);    // [4,5]
        Assert.Equal(5, m.ValueOffsets[4]);    // [6]
        Assert.Equal(6, m.ValueOffsets[5]);

        var items = (Int32Array)m.Values;
        Assert.Equal(new int?[] { 1, 2, 3, 4, 5, 6 }, items.ToArray());
    }

    [Fact]
    public async Task ListOfList_NullAndEmpty()
    {
        // list<list<int32>>. Two list layers, so rep ∈ {0, 1, 2}. 5 outer
        // rows: [[1,2],[3]], [], None, [[4],[],[5,6,7]], [[8,9]]. Tests that
        // the multi-list walker handles outer-null/empty cascade
        // (skipping inner-row counting), inner-empty (counted with empty
        // span), and ordinary nested values together.
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("list_list_int_v21.lance"));
        var outer = (ListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(5, outer.Length);
        Assert.Equal(1, outer.NullCount);
        Assert.True(outer.IsNull(2));
        Assert.False(outer.IsNull(1));   // empty outer is valid

        // Outer offsets: row 0 has 2 inner-lists, row 1 empty, row 2 null,
        // row 3 has 3 inner-lists, row 4 has 1 inner-list. Total inner = 6.
        Assert.Equal(0, outer.ValueOffsets[0]);
        Assert.Equal(2, outer.ValueOffsets[1]);
        Assert.Equal(2, outer.ValueOffsets[2]);
        Assert.Equal(2, outer.ValueOffsets[3]);
        Assert.Equal(5, outer.ValueOffsets[4]);
        Assert.Equal(6, outer.ValueOffsets[5]);

        var inner = (ListArray)outer.Values;
        Assert.Equal(6, inner.Length);
        // Inner offsets: each inner list's items.
        Assert.Equal(0, inner.ValueOffsets[0]);   // [1,2]
        Assert.Equal(2, inner.ValueOffsets[1]);   // [3]
        Assert.Equal(3, inner.ValueOffsets[2]);   // [4]
        Assert.Equal(4, inner.ValueOffsets[3]);   // [] empty
        Assert.Equal(4, inner.ValueOffsets[4]);   // [5,6,7]
        Assert.Equal(7, inner.ValueOffsets[5]);   // [8,9]
        Assert.Equal(9, inner.ValueOffsets[6]);

        var values = (Int32Array)inner.Values;
        Assert.Equal(9, values.Length);
        Assert.Equal(new int?[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 }, values.ToArray());
    }

    [Fact]
    public async Task Struct_Depth3_RecursiveCascade()
    {
        // struct<l1: struct<l2: struct<a: int, b: int>>>. Each leaf has 4
        // layers [item, l2, l1, top]. Tests the recursive walker's
        // arbitrary-depth cascade: def 1=item null, 2=l2 null, 3=l1 null,
        // 4=top null. 12 rows including a row at each compound layer's null
        // state (l2 null at row 4, l1 null at row 7, top null at row 9).
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("struct_depth3_v21.lance"));
        var top = (Apache.Arrow.StructArray)await reader.ReadColumnAsync(0);
        Assert.Equal(12, top.Length);
        Assert.Equal(1, top.NullCount);
        Assert.True(top.IsNull(9));
        Assert.False(top.IsNull(7));   // l1 null but top valid
        Assert.False(top.IsNull(4));   // l2 null but top valid

        var l1 = (Apache.Arrow.StructArray)top.Fields[0];
        Assert.Equal(12, l1.Length);
        Assert.Equal(2, l1.NullCount);   // row 7 (own null) + row 9 (cascade)
        Assert.True(l1.IsNull(7));
        Assert.True(l1.IsNull(9));
        Assert.False(l1.IsNull(4));      // l2 null doesn't cascade up to l1

        var l2 = (Apache.Arrow.StructArray)l1.Fields[0];
        Assert.Equal(12, l2.Length);
        Assert.Equal(3, l2.NullCount);   // row 4 (own) + 7 (cascade) + 9 (cascade)
        Assert.True(l2.IsNull(4));
        Assert.True(l2.IsNull(7));
        Assert.True(l2.IsNull(9));
        Assert.False(l2.IsNull(0));

        var a = (Int32Array)l2.Fields[0];
        var b = (Int32Array)l2.Fields[1];
        Assert.Equal(3, a.NullCount);
        Assert.Equal(17, a.GetValue(0));
        Assert.Equal(28, a.GetValue(1));   // 17 + 1*11
        Assert.Null(a.GetValue(4));
        Assert.Null(a.GetValue(7));
        Assert.Null(a.GetValue(9));
        Assert.Equal(17 + 11 * 11, a.GetValue(11));
        Assert.Equal(113, b.GetValue(0));
        Assert.Null(b.GetValue(9));
    }

    [Fact]
    public async Task Struct_MixedShape_WithStructGrandchild()
    {
        // Outer struct has a primitive child x and a struct grandchild ab.
        // Per-leaf layer shapes:
        //   x:    [item, outer_struct]                    (2 layers)
        //   ab.a: [item, inner_struct, outer_struct]      (3 layers)
        //   ab.b: [item, inner_struct, outer_struct]      (3 layers)
        // Cross-column reconciliation is at the outer-struct validity
        // level: col[0] uses def=2 for outer-null while col[1]/col[2] use
        // def=3, but all three produce the same outer-null bitmap.
        //
        // Six rows: [{x:11,ab:{a:17,b:113}}, {x:22,ab:None}, None,
        //            {x:44,ab:{a:41,b:313}}, {x:55,ab:{a:53,b:419}},
        //            {x:66,ab:{a:67,b:521}}]
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("struct_mixed_with_struct_child_v21.lance"));
        var s = (Apache.Arrow.StructArray)await reader.ReadColumnAsync(0);
        Assert.Equal(6, s.Length);
        Assert.Equal(1, s.NullCount);
        Assert.True(s.IsNull(2));
        Assert.False(s.IsNull(1));   // outer valid; ab is null but s itself is fine

        var x = (Int32Array)s.Fields[0];
        Assert.Equal(11, x.GetValue(0));
        Assert.Equal(22, x.GetValue(1));
        Assert.Null(x.GetValue(2));   // cascaded from outer-null
        Assert.Equal(44, x.GetValue(3));
        Assert.Equal(66, x.GetValue(5));

        var ab = (Apache.Arrow.StructArray)s.Fields[1];
        Assert.Equal(6, ab.Length);
        Assert.Equal(2, ab.NullCount);   // row 1 (inner null) + row 2 (cascaded)
        Assert.False(ab.IsNull(0));
        Assert.True(ab.IsNull(1));
        Assert.True(ab.IsNull(2));
        Assert.False(ab.IsNull(3));

        var a = (Int32Array)ab.Fields[0];
        var b = (Int32Array)ab.Fields[1];
        Assert.Equal(17, a.GetValue(0));
        Assert.Null(a.GetValue(1));
        Assert.Null(a.GetValue(2));
        Assert.Equal(41, a.GetValue(3));
        Assert.Equal(67, a.GetValue(5));
        Assert.Equal(113, b.GetValue(0));
        Assert.Null(b.GetValue(1));
        Assert.Null(b.GetValue(2));
        Assert.Equal(521, b.GetValue(5));
    }

    [Fact]
    public async Task Struct_MixedShapeChildren()
    {
        // Outer struct has TWO children of different physical shapes:
        //   x:  int32 (2-layer leaf [item, outer_struct])
        //   xs: list<int32> (3-layer leaf [item, list, outer_struct])
        // Their def-slot encoding differs (def=2 means outer-null in col[0]
        // but means list-empty in col[1]); coherence check has to compare
        // the *outer-struct validity bitmap* derived from each child's
        // def, not raw def buffers.
        //
        // Six rows: [{x:1,xs:[10,11]}, {x:2,xs:[20]}, None, {x:3,xs:[]},
        //            {x:4,xs:None}, {x:5,xs:[50,51,52]}]
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("struct_mixed_shapes_v21.lance"));
        var s = (Apache.Arrow.StructArray)await reader.ReadColumnAsync(0);
        Assert.Equal(6, s.Length);
        Assert.Equal(1, s.NullCount);
        Assert.True(s.IsNull(2));   // outer-null row
        Assert.False(s.IsNull(3));
        Assert.False(s.IsNull(4));

        var x = (Int32Array)s.Fields[0];
        Assert.Equal(6, x.Length);
        Assert.Equal(1, x.GetValue(0));
        Assert.Equal(2, x.GetValue(1));
        Assert.Null(x.GetValue(2));   // cascaded from outer-null
        Assert.Equal(3, x.GetValue(3));
        Assert.Equal(4, x.GetValue(4));
        Assert.Equal(5, x.GetValue(5));

        var xs = (ListArray)s.Fields[1];
        Assert.Equal(6, xs.Length);
        Assert.Equal(2, xs.NullCount);
        Assert.False(xs.IsNull(0));
        Assert.False(xs.IsNull(1));
        Assert.True(xs.IsNull(2));    // outer-null cascades to list
        Assert.False(xs.IsNull(3));   // empty list, valid
        Assert.True(xs.IsNull(4));    // genuine list-null
        Assert.False(xs.IsNull(5));

        // Offsets reflect visible-only items: 6 visible items spread across
        // rows 0 (2 items), 1 (1 item), 5 (3 items); other rows skip slots.
        Assert.Equal(0, xs.ValueOffsets[0]);
        Assert.Equal(2, xs.ValueOffsets[1]);
        Assert.Equal(3, xs.ValueOffsets[2]);
        Assert.Equal(3, xs.ValueOffsets[3]);  // outer-null → no items
        Assert.Equal(3, xs.ValueOffsets[4]);  // empty list → no items
        Assert.Equal(3, xs.ValueOffsets[5]);  // null list → no items
        Assert.Equal(6, xs.ValueOffsets[6]);

        var values = (Int32Array)xs.Values;
        Assert.Equal(new int?[] { 10, 11, 20, 50, 51, 52 }, values.ToArray());
    }

    [Fact]
    public async Task LargeList_NullAndEmpty()
    {
        // pa.large_list(int32). On disk Lance v2.1 encodes this exactly like
        // pa.list — same layers, same wire bytes — only the Arrow output
        // differs (LargeListArray with i64 offsets). Five rows include a
        // null list and an empty list to exercise NULL_AND_EMPTY_LIST
        // disambiguation (def=1=null-list, def=2=empty-list).
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("large_list_int_v21.lance"));
        Assert.IsType<Apache.Arrow.Types.LargeListType>(reader.Schema.FieldsList[0].DataType);

        var list = (Apache.Arrow.LargeListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(5, list.Length);
        Assert.Equal(1, list.NullCount);
        Assert.False(list.IsNull(0));
        Assert.False(list.IsNull(1));
        Assert.False(list.IsNull(2));   // empty list, valid
        Assert.True(list.IsNull(3));    // null list
        Assert.False(list.IsNull(4));

        // Offsets are i64 in a LargeListArray.
        Assert.Equal(0, list.ValueOffsets[0]);
        Assert.Equal(3, list.ValueOffsets[1]);
        Assert.Equal(5, list.ValueOffsets[2]);
        Assert.Equal(5, list.ValueOffsets[3]);  // empty
        Assert.Equal(5, list.ValueOffsets[4]);  // null
        Assert.Equal(6, list.ValueOffsets[5]);

        var values = (Int32Array)list.Values;
        Assert.Equal(new int?[] { 1, 2, 3, 4, 5, 6 }, values.ToArray());
    }

    [Fact]
    public async Task Fsl_Int32_NullableRow()
    {
        // FixedSizeList<int32, list_size=3> with one whole-row null:
        //   [[1,2,3], [4,5,6], None, [7,8,9]]
        // Wire form: layers=[NULLABLE_ITEM] (single layer at the FSL row),
        // num_buffers=2, value_compression=FSL(items=3, has_validity=true,
        // values=Flat(32)). buf[0] is the inner-item validity bitmap (12
        // bits, with the null row's three positions cleared); buf[1] is
        // the flat int32 values with placeholder 0s at the null row.
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("fsl_int_nullable_v21.lance"));
        var fsl = (Apache.Arrow.FixedSizeListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(4, fsl.Length);
        Assert.Equal(1, fsl.NullCount);
        Assert.False(fsl.IsNull(0));
        Assert.False(fsl.IsNull(1));
        Assert.True(fsl.IsNull(2));
        Assert.False(fsl.IsNull(3));

        var values = (Int32Array)fsl.Values;
        Assert.Equal(12, values.Length);
        // Inner validity: items in positions 6..8 (= the null row) are
        // null, the rest are valid.
        Assert.Equal(3, values.NullCount);
        Assert.Equal(1, values.GetValue(0));
        Assert.Equal(6, values.GetValue(5));
        Assert.Null(values.GetValue(6));
        Assert.Null(values.GetValue(7));
        Assert.Null(values.GetValue(8));
        Assert.Equal(7, values.GetValue(9));
        Assert.Equal(9, values.GetValue(11));
    }

    [Fact]
    public async Task FullZip_BigFsl_Float32_x4096()
    {
        // 5 rows × 4096 float32 = 80 KiB total, 16 KiB per row → FullZipLayout.
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("big_fsl_v21.lance"));
        var fslType = Assert.IsType<Apache.Arrow.Types.FixedSizeListType>(reader.Schema.FieldsList[0].DataType);
        Assert.Equal(4096, fslType.ListSize);

        var fsl = (Apache.Arrow.FixedSizeListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(5, fsl.Length);

        var values = (Apache.Arrow.FloatArray)fsl.Values;
        Assert.Equal(5 * 4096, values.Length);

        // Validate every row boundary — cheap enough.
        for (int row = 0; row < 5; row++)
        {
            int baseIdx = row * 4096;
            Assert.Equal((float)(row * 4096), values.GetValue(baseIdx));
            Assert.Equal((float)(row * 4096 + 4095), values.GetValue(baseIdx + 4095));
        }
    }

    // --- v2.1 string-encoding gap (2/3): FullZipLayout(bits_per_offset) +
    // General(ZSTD) + Variable for very large strings. ---

    [Fact]
    public async Task FullZip_BigStrings_NoNulls()
    {
        await using var reader = await LanceFileReader.OpenAsync(
            TestDataPath.Resolve("big_strings_v21.lance"));
        Assert.Equal(5L, reader.NumberOfRows);
        var arr = (StringArray)await reader.ReadColumnAsync(0);
        Assert.Equal(5, arr.Length);
        Assert.Equal(0, arr.NullCount);
        for (int i = 0; i < 5; i++)
        {
            string s = arr.GetString(i);
            Assert.Equal(65536, s.Length);
            Assert.Equal(MakeBigString(i, 65536), s);
        }
    }

    [Fact]
    public async Task FullZip_BigStrings_WithNulls()
    {
        await using var reader = await LanceFileReader.OpenAsync(
            TestDataPath.Resolve("big_strings_nulls_v21.lance"));
        Assert.Equal(6L, reader.NumberOfRows);
        var arr = (StringArray)await reader.ReadColumnAsync(0);
        Assert.Equal(6, arr.Length);
        Assert.Equal(2, arr.NullCount);
        for (int i = 0; i < 6; i++)
        {
            if (i % 3 == 1)
            {
                Assert.True(arr.IsNull(i), $"Row {i} should be null");
            }
            else
            {
                string s = arr.GetString(i);
                Assert.Equal(65536, s.Length);
                Assert.Equal(MakeBigString(i, 65536), s);
            }
        }
    }

    // --- FSST-compressed strings (Lance container + Variable values) ---

    [Fact]
    public async Task Fsst_Strings_NoNulls()
    {
        await using var reader = await LanceFileReader.OpenAsync(
            TestDataPath.Resolve("fsst_strings_v21.lance"));
        Assert.Equal(2000L, reader.NumberOfRows);
        var arr = (StringArray)await reader.ReadColumnAsync(0);
        Assert.Equal(2000, arr.Length);
        Assert.Equal(0, arr.NullCount);
        for (int i = 0; i < 2000; i++)
        {
            string expected = $"the quick brown fox jumps over lazy dog #{i:D4} variant {(i * 7) % 100:D2}";
            Assert.Equal(expected, arr.GetString(i));
        }
    }

    [Fact]
    public async Task Fsst_Strings_WithNulls()
    {
        await using var reader = await LanceFileReader.OpenAsync(
            TestDataPath.Resolve("fsst_strings_nulls_v21.lance"));
        Assert.Equal(2000L, reader.NumberOfRows);
        var arr = (StringArray)await reader.ReadColumnAsync(0);
        Assert.Equal(2000, arr.Length);
        // Generator: r if i % 9 != 4 else None  →  every 9th row at i%9==4 is null.
        int expectedNulls = 0;
        for (int i = 0; i < 2000; i++)
            if (i % 9 == 4) expectedNulls++;
        Assert.Equal(expectedNulls, arr.NullCount);

        for (int i = 0; i < 2000; i++)
        {
            if (i % 9 == 4)
            {
                Assert.True(arr.IsNull(i), $"Row {i} should be null");
            }
            else
            {
                string expected = $"the quick brown fox jumps over lazy dog #{i:D4} variant {(i * 7) % 100:D2}";
                Assert.Equal(expected, arr.GetString(i));
            }
        }
    }

    /// <summary>
    /// Mirror of generate_test_data.py's make_string: SHA-256 chain seeded
    /// with the row index, hex-encoded, truncated to <paramref name="size"/>.
    /// </summary>
    private static string MakeBigString(int seed, int size)
    {
        var sha = System.Security.Cryptography.SHA256.Create();
        var sb = new System.Text.StringBuilder(size);
        byte[] h = sha.ComputeHash(System.Text.Encoding.UTF8.GetBytes(seed.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        while (sb.Length < size)
        {
            sb.Append(Convert.ToHexString(h).ToLowerInvariant());
            h = sha.ComputeHash(h);
        }
        sb.Length = size;
        return sb.ToString();
    }
}

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

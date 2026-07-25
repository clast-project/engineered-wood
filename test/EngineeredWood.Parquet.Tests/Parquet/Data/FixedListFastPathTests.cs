// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// Covers <see cref="FixedListDetector"/> and the reader fast path it enables. The contract under
/// test is that <see cref="ParquetReadOptions.FixedListFastPath"/> is <em>invisible</em>: every read
/// must produce exactly what the general path produces, whether the fast path fires or bails out.
/// </summary>
public class FixedListFastPathTests : IDisposable
{
    private readonly string _tempDir;

    public FixedListFastPathTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-fixedlist-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private string TempPath(string name) => Path.Combine(_tempDir, name);

    // ---- Detector unit tests (encoded-stream level) ----

    private static byte[] EncodeLevels(ReadOnlySpan<int> levels, int bitWidth)
    {
        var encoder = new RleBitPackedEncoder(bitWidth);
        encoder.Encode(levels);
        return encoder.ToArray();
    }

    private static int[] FixedRepLevels(int rows, int length)
    {
        var levels = new int[rows * length];
        for (int i = 0; i < rows; i++)
        {
            for (int j = 0; j < length; j++)
                levels[i * length + j] = j == 0 ? 0 : 1;
        }
        return levels;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(15)]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(31)]
    [InlineData(64)]
    [InlineData(100)]
    [InlineData(256)]
    [InlineData(768)]
    public void Detector_FindsLength_ForEveryShape(int length)
    {
        const int rows = 37;
        int numValues = rows * length;

        var rep = EncodeLevels(FixedRepLevels(rows, length), bitWidth: 1);
        var def = EncodeLevels(Enumerable.Repeat(2, numValues).ToArray(), bitWidth: 2);

        int detected = 0;
        Assert.True(FixedListDetector.TryDetectPage(rep, def, maxDefLevel: 2, numValues, ref detected));
        Assert.Equal(length, detected);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(7)]
    public void Detector_ToleratesAnyDefLevelWidth(int maxDefLevel)
    {
        const int rows = 21;
        const int length = 12;
        int numValues = rows * length;

        var rep = EncodeLevels(FixedRepLevels(rows, length), bitWidth: 1);
        int defBitWidth = LevelDecoder.GetBitWidth(maxDefLevel);
        var def = EncodeLevels(Enumerable.Repeat(maxDefLevel, numValues).ToArray(), defBitWidth);

        int detected = 0;
        Assert.True(FixedListDetector.TryDetectPage(rep, def, maxDefLevel, numValues, ref detected));
        Assert.Equal(length, detected);
    }

    /// <summary>
    /// Bit-packs repetition levels into a single large run (padding the final group with zeros),
    /// the way writers that batch many groups per run — parquet-mr, arrow — emit small lists.
    /// EW's own encoder flushes group-by-group, so this shape is not produced by round-tripping.
    /// </summary>
    private static byte[] EncodeSingleBitPackedRun(ReadOnlySpan<int> levels)
    {
        int numGroups = (levels.Length + 7) / 8;
        var bytes = new byte[numGroups];
        for (int i = 0; i < levels.Length; i++)
        {
            if (levels[i] != 0)
                bytes[i >> 3] |= (byte)(1 << (i & 7));
        }

        // Header: (numGroups << 1) | 1, as an unsigned LEB128 varint.
        var header = new List<byte>();
        uint h = (uint)((numGroups << 1) | 1);
        while (h > 0x7F) { header.Add((byte)(h | 0x80)); h >>= 7; }
        header.Add((byte)h);

        var result = new byte[header.Count + bytes.Length];
        header.CopyTo(result);
        bytes.CopyTo(result, header.Count);
        return result;
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    public void Detector_ScalarAndAdaptiveAgree_OnSmallLists(int length)
    {
        const int rows = 5000;
        var rep = FixedRepLevels(rows, length);

        foreach (var encoded in new[]
                 {
                     EncodeLevels(rep, bitWidth: 1),          // EW encoder (header-interleaved)
                     EncodeSingleBitPackedRun(rep),           // one dense bit-packed run
                 })
        {
            bool scalar = FixedListDetector.MatchesFixedPattern(
                encoded, rep.Length, length, startOffset: 0, RepScanStrategy.Scalar);
            bool adaptive = FixedListDetector.MatchesFixedPattern(
                encoded, rep.Length, length, startOffset: 0, RepScanStrategy.Adaptive);

            Assert.True(scalar, $"scalar rejected valid length={length}");
            Assert.Equal(scalar, adaptive);
        }
    }

    [Theory]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(7)]
    public void Detector_ScalarAndAdaptiveAgree_OnRejection(int length)
    {
        var rep = FixedRepLevels(200, length);
        rep[length + 1] = 0; // corrupt one interior level into a spurious record start

        var encoded = EncodeSingleBitPackedRun(rep);
        bool scalar = FixedListDetector.MatchesFixedPattern(
            encoded, rep.Length, length, 0, RepScanStrategy.Scalar);
        bool adaptive = FixedListDetector.MatchesFixedPattern(
            encoded, rep.Length, length, 0, RepScanStrategy.Adaptive);

        Assert.False(scalar);
        Assert.Equal(scalar, adaptive);
    }

    [Fact]
    public void Detector_ScalarAndAdaptiveAgree_WithStartOffset()
    {
        const int length = 5;
        // A page opening mid-record: build the full pattern, then feed a suffix with the matching
        // startOffset so both strategies must honour the offset.
        var full = FixedRepLevels(400, length);
        var rep = full.AsSpan(2).ToArray();
        var encoded = EncodeSingleBitPackedRun(rep);

        bool scalar = FixedListDetector.MatchesFixedPattern(encoded, rep.Length, length, 2, RepScanStrategy.Scalar);
        bool adaptive = FixedListDetector.MatchesFixedPattern(encoded, rep.Length, length, 2, RepScanStrategy.Adaptive);

        Assert.True(scalar);
        Assert.Equal(scalar, adaptive);
    }

    [Fact]
    public void Detector_RejectsRaggedLists()
    {
        // Rows of 4, 4, 3, 4 elements — the third row breaks the pattern.
        int[] rep = [0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 1];
        var repEncoded = EncodeLevels(rep, bitWidth: 1);
        var defEncoded = EncodeLevels(Enumerable.Repeat(2, rep.Length).ToArray(), bitWidth: 2);

        int detected = 0;
        Assert.False(FixedListDetector.TryDetectPage(repEncoded, defEncoded, 2, rep.Length, ref detected));
    }

    [Fact]
    public void Detector_RejectsRaggedListsNearEndOfPage()
    {
        // The break is in the second-to-last row, deep into a long RLE-heavy stream. (A break in
        // the *last* row is indistinguishable from a page that simply stops mid-record, which is
        // legitimate; the chunk-level total-count check catches that case instead.)
        var levels = new List<int>();
        for (int i = 0; i < 40; i++)
        {
            levels.Add(0);
            for (int j = 1; j < 16; j++) levels.Add(1);
        }
        levels.RemoveAt(levels.Count - 17); // second-to-last row has 15 elements

        var repEncoded = EncodeLevels(levels.ToArray(), bitWidth: 1);
        var defEncoded = EncodeLevels(Enumerable.Repeat(2, levels.Count).ToArray(), bitWidth: 2);

        int detected = 0;
        Assert.False(FixedListDetector.TryDetectPage(repEncoded, defEncoded, 2, levels.Count, ref detected));
    }

    [Fact]
    public void Detector_RejectsWhenAnyValueIsNull()
    {
        const int rows = 10, length = 8;
        int numValues = rows * length;

        var rep = EncodeLevels(FixedRepLevels(rows, length), bitWidth: 1);

        var defLevels = Enumerable.Repeat(2, numValues).ToArray();
        defLevels[numValues / 2] = 1; // one null element
        var def = EncodeLevels(defLevels, bitWidth: 2);

        int detected = 0;
        Assert.False(FixedListDetector.TryDetectPage(rep, def, 2, numValues, ref detected));
    }

    [Fact]
    public void Detector_RejectsMismatchedLengthOnLaterPage()
    {
        var page1Rep = EncodeLevels(FixedRepLevels(10, 8), bitWidth: 1);
        var page1Def = EncodeLevels(Enumerable.Repeat(2, 80).ToArray(), bitWidth: 2);

        int detected = 0;
        Assert.True(FixedListDetector.TryDetectPage(page1Rep, page1Def, 2, 80, ref detected));
        Assert.Equal(8, detected);

        // Second page holds lists of 4 — internally consistent, but not the chunk's length.
        var page2Rep = EncodeLevels(FixedRepLevels(20, 4), bitWidth: 1);
        var page2Def = EncodeLevels(Enumerable.Repeat(2, 80).ToArray(), bitWidth: 2);
        Assert.False(FixedListDetector.TryDetectPage(page2Rep, page2Def, 2, 80, ref detected));
    }

    [Fact]
    public void Detector_RejectsPageNotStartingAtRecordBoundary()
    {
        int[] rep = [1, 1, 0, 1, 1, 1, 0, 1];
        var repEncoded = EncodeLevels(rep, bitWidth: 1);
        var defEncoded = EncodeLevels(Enumerable.Repeat(1, rep.Length).ToArray(), bitWidth: 1);

        int detected = 0;
        Assert.False(FixedListDetector.TryDetectPage(repEncoded, defEncoded, 1, rep.Length, ref detected));
    }

    [Fact]
    public void Detector_AcceptsPageEndingMidRecord()
    {
        // Writers split pages by value count, so a page may stop part-way through a list. That is
        // consistent with a fixed length; only the chunk total has to divide evenly, which the
        // reader checks separately.
        int[] rep = [0, 1, 1, 1, 1, 0, 1, 1, 1, 1, 0, 1, 1, 1, 1, 0, 1];
        var repEncoded = EncodeLevels(rep, bitWidth: 1);
        var defEncoded = EncodeLevels(Enumerable.Repeat(1, rep.Length).ToArray(), bitWidth: 1);

        int detected = 0;
        Assert.True(FixedListDetector.TryDetectPage(repEncoded, defEncoded, 1, rep.Length, ref detected));
        Assert.Equal(5, detected);
    }

    [Fact]
    public void Detector_ChecksPatternRelativeToChunkPosition()
    {
        // A page that opens two elements into a 5-list: 1,1,1 then a fresh record.
        int[] rep = [1, 1, 1, 0, 1, 1, 1, 1, 0, 1];
        var repEncoded = EncodeLevels(rep, bitWidth: 1);
        var defEncoded = EncodeLevels(Enumerable.Repeat(1, rep.Length).ToArray(), bitWidth: 1);

        int detected = 5;
        Assert.True(FixedListDetector.TryDetectPage(
            repEncoded, defEncoded, 1, rep.Length, ref detected, startIndex: 12));

        // The same bytes at a different chunk offset do not line up.
        detected = 5;
        Assert.False(FixedListDetector.TryDetectPage(
            repEncoded, defEncoded, 1, rep.Length, ref detected, startIndex: 11));
    }

    [Fact]
    public void Detector_RefusesToDeriveLengthFromAMidChunkPage()
    {
        var rep = EncodeLevels(FixedRepLevels(4, 8), bitWidth: 1);
        var def = EncodeLevels(Enumerable.Repeat(1, 32).ToArray(), bitWidth: 1);

        int detected = 0;
        Assert.False(FixedListDetector.TryDetectPage(rep, def, 1, 32, ref detected, startIndex: 100));
    }

    // ---- End-to-end reader tests ----

    private static RecordBatch BuildFixedListBatch(int rows, int length, bool nullableList = true)
    {
        var elementField = new Field("element", FloatType.Default, nullable: false);
        var listType = new ListType(elementField);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("vec", listType, nullable: nullableList))
            .Build();

        var ids = new Int32Array.Builder();
        var values = new FloatArray.Builder();
        var offsets = new int[rows + 1];
        for (int i = 0; i < rows; i++)
        {
            ids.Append(i);
            offsets[i] = i * length;
            for (int j = 0; j < length; j++)
                values.Append(i * length + j + 0.5f);
        }
        offsets[rows] = rows * length;

        var listData = new ArrayData(listType, rows, nullCount: 0, offset: 0,
            [ArrowBuffer.Empty, new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray())],
            [values.Build().Data]);

        return new RecordBatch(schema, [ids.Build(), new ListArray(listData)], rows);
    }

    private static RecordBatch BuildRaggedListBatch(int rows)
    {
        var elementField = new Field("element", FloatType.Default, nullable: true);
        var listType = new ListType(elementField);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("vec", listType, nullable: true))
            .Build();

        var values = new FloatArray.Builder();
        var offsets = new int[rows + 1];
        var validity = new byte[(rows + 7) / 8];
        int nullCount = 0;
        int offset = 0;

        for (int i = 0; i < rows; i++)
        {
            offsets[i] = offset;
            if (i % 7 == 3)
            {
                nullCount++; // null list
                continue;
            }

            validity[i >> 3] |= (byte)(1 << (i & 7));
            int len = i % 5; // includes empty lists
            for (int j = 0; j < len; j++)
            {
                if (j == 2) values.AppendNull();
                else values.Append(i + j);
                offset++;
            }
        }
        offsets[rows] = offset;

        var listData = new ArrayData(listType, rows, nullCount, 0,
            [new ArrowBuffer(validity), new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray())],
            [values.Build().Data]);

        return new RecordBatch(schema, [new ListArray(listData)], rows);
    }

    private async Task<string> WriteAsync(string name, RecordBatch batch, ParquetWriteOptions options)
    {
        string path = TempPath(name);
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private static async Task<RecordBatch> ReadAsync(string path, bool fastPath)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(
            file, ownsFile: false, new ParquetReadOptions { FixedListFastPath = fastPath });
        return await reader.ReadRowGroupAsync(0);
    }

    /// <summary>
    /// Compares two batches field by field, down to element values, offsets, and validity.
    /// </summary>
    private static void AssertBatchesEqual(RecordBatch expected, RecordBatch actual)
    {
        Assert.Equal(expected.Length, actual.Length);
        Assert.Equal(expected.ColumnCount, actual.ColumnCount);

        for (int c = 0; c < expected.ColumnCount; c++)
        {
            var e = expected.Column(c);
            var a = actual.Column(c);
            Assert.Equal(e.GetType(), a.GetType());
            Assert.Equal(e.Length, a.Length);
            Assert.Equal(e.NullCount, a.NullCount);

            if (e is ListArray el && a is ListArray al)
            {
                for (int i = 0; i < el.Length; i++)
                {
                    Assert.Equal(el.IsNull(i), al.IsNull(i));
                    if (el.IsNull(i)) continue;

                    var ev = el.GetSlicedValues(i);
                    var av = al.GetSlicedValues(i);
                    Assert.Equal(ev.Length, av.Length);
                    AssertPrimitivesEqual(ev, av);
                }
            }
            else
            {
                AssertPrimitivesEqual(e, a);
            }
        }
    }

    private static void AssertPrimitivesEqual(IArrowArray expected, IArrowArray actual)
    {
        switch (expected, actual)
        {
            case (FloatArray e, FloatArray a):
                for (int i = 0; i < e.Length; i++)
                {
                    Assert.Equal(e.IsNull(i), a.IsNull(i));
                    if (!e.IsNull(i)) Assert.Equal(e.GetValue(i), a.GetValue(i));
                }
                break;
            case (Int32Array e, Int32Array a):
                for (int i = 0; i < e.Length; i++)
                {
                    Assert.Equal(e.IsNull(i), a.IsNull(i));
                    if (!e.IsNull(i)) Assert.Equal(e.GetValue(i), a.GetValue(i));
                }
                break;
            default:
                Assert.Equal(expected.Length, actual.Length);
                break;
        }
    }

    /// <summary>
    /// Reads one leaf column chunk through the real reader entry point and hands back the
    /// <see cref="ColumnResult"/>, whose <see cref="ColumnResult.FixedListLength"/> reports whether
    /// the fast path actually engaged. Without this, an equality test between the two paths would
    /// still pass if the fast path silently never fired.
    /// </summary>
    private static async Task<ColumnResult> ReadLeafAsync(string path, int leafIndex, bool fastPath)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var metadata = await reader.ReadMetadataAsync();
        var schema = await reader.GetSchemaAsync();
        var rowGroup = metadata.RowGroups[0];
        var chunk = rowGroup.Columns[leafIndex];
        var meta = chunk.MetaData!;

        long start = meta.DictionaryPageOffset is > 0 and long dpo ? dpo : meta.DataPageOffset;
        using var buffer = await file.ReadAsync(new EngineeredWood.IO.FileRange(start, meta.TotalCompressedSize));

        var column = schema.Columns[leafIndex];
        return ColumnChunkReader.ReadColumn(
            buffer.Memory.Span, column, meta, checked((int)rowGroup.NumRows),
            ArrowSchemaConverter.ToArrowField(column),
            preserveDefLevels: true,
            validateCrc: false,
            fixedListFastPath: fastPath);
    }

    [Theory]
    [InlineData(3)]
    [InlineData(8)]
    [InlineData(16)]
    [InlineData(768)]
    public async Task FastPath_ActuallyEngages_AndSkipsLevelMaterialisation(int length)
    {
        var batch = BuildFixedListBatch(rows: 512, length: length);
        string path = await WriteAsync($"engages-{length}.parquet", batch, ParquetWriteOptions.Default);

        var fast = await ReadLeafAsync(path, leafIndex: 1, fastPath: true);
        Assert.Equal(length, fast.FixedListLength);
        Assert.Null(fast.DefinitionLevels);
        Assert.Null(fast.RepetitionLevels);
        Assert.Equal(512 * length, fast.Array.Length);

        // With the option off, the same chunk goes down the general path and materialises levels.
        var slow = await ReadLeafAsync(path, leafIndex: 1, fastPath: false);
        Assert.Equal(0, slow.FixedListLength);
        Assert.NotNull(slow.DefinitionLevels);
        Assert.NotNull(slow.RepetitionLevels);
    }

    [Fact]
    public async Task FastPath_DoesNotEngage_ForRaggedData()
    {
        var batch = BuildRaggedListBatch(rows: 500);
        string path = await WriteAsync("engages-ragged.parquet", batch, ParquetWriteOptions.Default);

        var result = await ReadLeafAsync(path, leafIndex: 0, fastPath: true);
        Assert.Equal(0, result.FixedListLength);
        Assert.NotNull(result.RepetitionLevels);
    }

    [Theory]
    [InlineData(3, DataPageVersion.V1)]
    [InlineData(3, DataPageVersion.V2)]
    [InlineData(8, DataPageVersion.V2)]
    [InlineData(15, DataPageVersion.V2)]
    [InlineData(16, DataPageVersion.V1)]
    [InlineData(16, DataPageVersion.V2)]
    [InlineData(64, DataPageVersion.V2)]
    [InlineData(768, DataPageVersion.V1)]
    [InlineData(768, DataPageVersion.V2)]
    public async Task FastPath_MatchesGeneralPath_ForFixedLists(int length, DataPageVersion pageVersion)
    {
        int rows = Math.Max(64, 4096 / length);
        var batch = BuildFixedListBatch(rows, length);
        string path = await WriteAsync(
            $"fixed-{length}-{pageVersion}.parquet", batch,
            ParquetWriteOptions.Default with { DataPageVersion = pageVersion });

        var slow = await ReadAsync(path, fastPath: false);
        var fast = await ReadAsync(path, fastPath: true);

        AssertBatchesEqual(slow, fast);

        // And the values themselves are what was written.
        var list = (ListArray)fast.Column(1);
        Assert.Equal(rows, list.Length);
        for (int i = 0; i < rows; i += Math.Max(1, rows / 8))
        {
            var vec = (FloatArray)list.GetSlicedValues(i);
            Assert.Equal(length, vec.Length);
            Assert.Equal(i * length + 0.5f, vec.GetValue(0));
            Assert.Equal(i * length + length - 1 + 0.5f, vec.GetValue(length - 1));
        }
    }

    [Fact]
    public async Task FastPath_MatchesGeneralPath_AcrossMultiplePages()
    {
        // 8 KiB pages force many pages per chunk for a 32-wide float vector.
        var batch = BuildFixedListBatch(rows: 5000, length: 32);
        string path = await WriteAsync("fixed-multipage.parquet", batch,
            ParquetWriteOptions.Default with { DataPageSize = 8 * 1024 });

        AssertBatchesEqual(await ReadAsync(path, false), await ReadAsync(path, true));
    }

    [Fact]
    public async Task FastPath_MatchesGeneralPath_WhenUncompressed()
    {
        var batch = BuildFixedListBatch(rows: 500, length: 24);
        string path = await WriteAsync("fixed-uncompressed.parquet", batch,
            ParquetWriteOptions.Default with { Compression = CompressionCodec.Uncompressed });

        AssertBatchesEqual(await ReadAsync(path, false), await ReadAsync(path, true));
    }

    [Fact]
    public async Task FastPath_MatchesGeneralPath_ForRequiredList()
    {
        var batch = BuildFixedListBatch(rows: 300, length: 12, nullableList: false);
        string path = await WriteAsync("fixed-required.parquet", batch, ParquetWriteOptions.Default);

        AssertBatchesEqual(await ReadAsync(path, false), await ReadAsync(path, true));
    }

    [Fact]
    public async Task FastPath_FallsBack_ForRaggedListsWithNulls()
    {
        var batch = BuildRaggedListBatch(rows: 500);
        string path = await WriteAsync("ragged.parquet", batch, ParquetWriteOptions.Default);

        AssertBatchesEqual(await ReadAsync(path, false), await ReadAsync(path, true));
    }

    [Fact]
    public async Task FastPath_FallsBack_ForFixedLengthListsContainingNulls()
    {
        // Every list is exactly 4 long, but some elements are null: the rep pattern matches and the
        // def check must be the one that rejects.
        const int rows = 256, length = 4;
        var elementField = new Field("element", FloatType.Default, nullable: true);
        var listType = new ListType(elementField);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("vec", listType, nullable: false))
            .Build();

        var values = new FloatArray.Builder();
        var offsets = new int[rows + 1];
        for (int i = 0; i < rows; i++)
        {
            offsets[i] = i * length;
            for (int j = 0; j < length; j++)
            {
                if ((i + j) % 11 == 0) values.AppendNull();
                else values.Append(i * length + j);
            }
        }
        offsets[rows] = rows * length;

        var listData = new ArrayData(listType, rows, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray())],
            [values.Build().Data]);
        var batch = new RecordBatch(schema, [new ListArray(listData)], rows);

        string path = await WriteAsync("fixed-with-null-elements.parquet", batch, ParquetWriteOptions.Default);

        var slow = await ReadAsync(path, false);
        var fast = await ReadAsync(path, true);
        AssertBatchesEqual(slow, fast);
        Assert.True(((ListArray)fast.Column(0)).Values.NullCount > 0);
    }

    [Fact]
    public async Task FastPath_FallsBack_ForNestedLists()
    {
        // list<list<float>> has maxRepetitionLevel 2 — outside the detector's remit.
        var inner = new ListType(new Field("element", FloatType.Default, nullable: false));
        var outer = new ListType(new Field("element", inner, nullable: false));
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("grid", outer, nullable: false))
            .Build();

        const int rows = 100, outerLen = 3, innerLen = 4;
        var values = new FloatArray.Builder();
        var innerOffsets = new int[rows * outerLen + 1];
        var outerOffsets = new int[rows + 1];

        for (int i = 0; i < rows * outerLen; i++)
        {
            innerOffsets[i] = i * innerLen;
            for (int j = 0; j < innerLen; j++) values.Append(i * innerLen + j);
        }
        innerOffsets[rows * outerLen] = rows * outerLen * innerLen;
        for (int i = 0; i <= rows; i++) outerOffsets[i] = i * outerLen;

        var innerData = new ArrayData(inner, rows * outerLen, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(MemoryMarshal.AsBytes(innerOffsets.AsSpan()).ToArray())],
            [values.Build().Data]);
        var outerData = new ArrayData(outer, rows, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(MemoryMarshal.AsBytes(outerOffsets.AsSpan()).ToArray())],
            [innerData]);

        var batch = new RecordBatch(schema, [new ListArray(outerData)], rows);
        string path = await WriteAsync("nested-lists.parquet", batch, ParquetWriteOptions.Default);

        AssertBatchesEqual(await ReadAsync(path, false), await ReadAsync(path, true));
    }

    [Fact]
    public async Task FastPath_MatchesGeneralPath_ForForeignWrittenListFile()
    {
        // list_columns.parquet comes from the parquet-testing corpus: nulls, empty lists,
        // ragged lengths, and a string element column.
        string path = TestData.GetPath("list_columns.parquet");
        AssertBatchesEqual(await ReadAsync(path, false), await ReadAsync(path, true));
    }

    [Fact]
    public async Task FastPath_MatchesGeneralPath_ForStringElements()
    {
        const int rows = 400, length = 5;
        var listType = new ListType(new Field("element", StringType.Default, nullable: false));
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("tags", listType, nullable: false))
            .Build();

        var values = new StringArray.Builder();
        var offsets = new int[rows + 1];
        for (int i = 0; i < rows; i++)
        {
            offsets[i] = i * length;
            for (int j = 0; j < length; j++) values.Append($"tag-{i}-{j}");
        }
        offsets[rows] = rows * length;

        var listData = new ArrayData(listType, rows, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray())],
            [values.Build().Data]);
        var batch = new RecordBatch(schema, [new ListArray(listData)], rows);

        string path = await WriteAsync("fixed-strings.parquet", batch, ParquetWriteOptions.Default);

        var slow = await ReadAsync(path, false);
        var fast = await ReadAsync(path, true);
        Assert.Equal(rows, fast.Length);

        var slowList = (ListArray)slow.Column(0);
        var fastList = (ListArray)fast.Column(0);
        for (int i = 0; i < rows; i += 37)
        {
            var s = (StringArray)slowList.GetSlicedValues(i);
            var f = (StringArray)fastList.GetSlicedValues(i);
            Assert.Equal(s.Length, f.Length);
            for (int j = 0; j < s.Length; j++)
                Assert.Equal(s.GetString(j), f.GetString(j));
        }
    }

    [Fact]
    public async Task FastPath_MatchesGeneralPath_ForListInsideStruct()
    {
        const int rows = 200, length = 6;
        var listType = new ListType(new Field("element", Int32Type.Default, nullable: false));
        var structType = new StructType(
        [
            new Field("id", Int32Type.Default, nullable: false),
            new Field("vec", listType, nullable: false),
        ]);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("payload", structType, nullable: false))
            .Build();

        var ids = new Int32Array.Builder();
        var values = new Int32Array.Builder();
        var offsets = new int[rows + 1];
        for (int i = 0; i < rows; i++)
        {
            ids.Append(i);
            offsets[i] = i * length;
            for (int j = 0; j < length; j++) values.Append(i * length + j);
        }
        offsets[rows] = rows * length;

        var listData = new ArrayData(listType, rows, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray())],
            [values.Build().Data]);

        var structArray = new StructArray(structType, rows,
            [ids.Build(), new ListArray(listData)], ArrowBuffer.Empty, nullCount: 0);
        var batch = new RecordBatch(schema, [structArray], rows);

        string path = await WriteAsync("fixed-in-struct.parquet", batch, ParquetWriteOptions.Default);

        var slow = (StructArray)(await ReadAsync(path, false)).Column(0);
        var fast = (StructArray)(await ReadAsync(path, true)).Column(0);

        var slowList = (ListArray)slow.Fields[1];
        var fastList = (ListArray)fast.Fields[1];
        Assert.Equal(slowList.Length, fastList.Length);
        for (int i = 0; i < rows; i += 13)
        {
            var s = (Int32Array)slowList.GetSlicedValues(i);
            var f = (Int32Array)fastList.GetSlicedValues(i);
            Assert.Equal(s.Length, f.Length);
            for (int j = 0; j < s.Length; j++)
                Assert.Equal(s.GetValue(j), f.GetValue(j));
        }
    }
}

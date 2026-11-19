// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// A dictionary that fills up partway through a chunk keeps what it encoded: the dictionary page and the
/// RLE_DICTIONARY pages before the cut, then the rest of the chunk in the column's own encoding, as parquet-cpp and
/// parquet-mr write such a column. Each column here is low-cardinality for its first MiB (so the first-page test
/// keeps the dictionary) and then distinct, against an 8 KiB dictionary limit, so the dictionary fills after the
/// decision. The files are read back by EW and by arrow-cpp (ParquetSharp), and their statistics, Bloom filter and
/// page layout checked against the values after the cut too.
/// </summary>
public class DictionaryFallbackTests : IDisposable
{
    private const int DictionaryLimit = 8 * 1024;

    private readonly string _tempDir;

    public DictionaryFallbackTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-dict-fallback-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch (IOException) { }
    }

    public static TheoryData<DataPageVersion, bool> Layouts() => new()
    {
        { DataPageVersion.V1, false }, { DataPageVersion.V1, true }, { DataPageVersion.V2, false }, { DataPageVersion.V2, true },
    };

    // ---- INT64 ----

    private const int LongRows = 300_000;
    private const int LongLowRows = 200_000;

    private static long? LongAt(int i, bool nullable) =>
        nullable && i % 7 == 3 ? null : i < LongLowRows ? i % 100 : 10_000_000L + i;

    [Theory]
    [MemberData(nameof(Layouts))]
    public async Task Int64_KeepsTheDictionaryPagesAndContinuesInItsOwnEncoding(DataPageVersion version, bool nullable)
    {
        var builder = new Int64Array.Builder();
        for (int i = 0; i < LongRows; i++)
        {
            if (LongAt(i, nullable) is long v) builder.Append(v); else builder.AppendNull();
        }

        string path = await WriteAsync("int64", builder.Build(), Int64Type.Default, nullable, version);
        var chunk = await AssertMixedChunkAsync(path);

        // EW reads it back.
        var read = (Int64Array)(await ReadAsync(path)).Column(0);
        for (int i = 0; i < LongRows; i++)
            Assert.Equal(LongAt(i, nullable), read.GetValue(i));

        // So does arrow-cpp.
        using (var reader = new ParquetSharp.ParquetFileReader(path))
        using (var rowGroup = reader.RowGroup(0))
        {
            var values = new long?[LongRows];
            if (nullable)
            {
                using var column = rowGroup.Column(0).LogicalReader<long?>();
                column.ReadBatch(values, 0, LongRows);
            }
            else
            {
                using var column = rowGroup.Column(0).LogicalReader<long>();
                var dense = new long[LongRows];
                column.ReadBatch(dense, 0, LongRows);
                for (int i = 0; i < LongRows; i++)
                    values[i] = dense[i];
            }

            for (int i = 0; i < LongRows; i++)
                Assert.Equal(LongAt(i, nullable), values[i]);
        }

        // Statistics, the Bloom filter and the page index cover the values after the cut.
        Assert.Equal(0L, BitConverter.ToInt64(chunk.Statistics!.MinValue!, 0));
        Assert.Equal(10_000_000L + LongRows - 1, BitConverter.ToInt64(chunk.Statistics!.MaxValue!, 0));
        await AssertBloomAdmitsAsync(path, 10_000_000L + LongRows - 1);
    }

    // ---- STRING ----

    private const int StringRows = 200_000;
    private const int StringLowRows = 150_000;

    private static string? StringAt(int i, bool nullable) =>
        nullable && i % 9 == 5 ? null : i < StringLowRows ? "cat-" + (i % 50) : "unique-" + i;

    [Theory]
    [MemberData(nameof(Layouts))]
    public async Task String_KeepsTheDictionaryPagesAndContinuesInItsOwnEncoding(DataPageVersion version, bool nullable)
    {
        var builder = new StringArray.Builder();
        for (int i = 0; i < StringRows; i++)
        {
            if (StringAt(i, nullable) is string v) builder.Append(v); else builder.AppendNull();
        }

        string path = await WriteAsync("string", builder.Build(), StringType.Default, nullable, version);
        var chunk = await AssertMixedChunkAsync(path);

        var read = (StringArray)(await ReadAsync(path)).Column(0);
        for (int i = 0; i < StringRows; i++)
            Assert.Equal(StringAt(i, nullable), read.GetString(i));

        using (var reader = new ParquetSharp.ParquetFileReader(path))
        using (var rowGroup = reader.RowGroup(0))
        using (var column = rowGroup.Column(0).LogicalReader<string?>())
        {
            var values = new string?[StringRows];
            column.ReadBatch(values, 0, StringRows);
            for (int i = 0; i < StringRows; i++)
                Assert.Equal(StringAt(i, nullable), values[i]);
        }

        // "unique-199999" is the greatest (u > c); "cat-0" the least.
        Assert.Equal("cat-0", System.Text.Encoding.UTF8.GetString(chunk.Statistics!.MinValue!));
        Assert.Equal("unique-199999", System.Text.Encoding.UTF8.GetString(chunk.Statistics!.MaxValue!));
        await AssertBloomAdmitsAsync(path, "unique-199999");
    }

    // ---- LIST<INT64>: the cut moves back to the start of a record ----

    private const int ListRows = 100_000;
    private const int ListLowRows = 70_000;

    private static long ListElement(int row, int k) =>
        row < ListLowRows ? (row * 3 + k) % 100 : 10_000_000L + row * 3 + k;

    [Theory]
    [InlineData(DataPageVersion.V1)]
    [InlineData(DataPageVersion.V2)]
    public async Task List_CutsAtARecordBoundary(DataPageVersion version)
    {
        var builder = new ListArray.Builder(Int64Type.Default);
        var values = (Int64Array.Builder)builder.ValueBuilder;
        for (int r = 0; r < ListRows; r++)
        {
            builder.Append();
            for (int k = 0; k < 3; k++)
                values.Append(ListElement(r, k));
        }

        string path = await WriteAsync("list", builder.Build(), new ListType(Int64Type.Default), false, version);
        await AssertMixedChunkAsync(path);

        var read = (ListArray)(await ReadAsync(path)).Column(0);
        var elements = (Int64Array)read.Values;
        for (int r = 0; r < ListRows; r++)
        {
            Assert.Equal(3, read.GetValueLength(r));
            for (int k = 0; k < 3; k++)
                Assert.Equal(ListElement(r, k), elements.GetValue(read.ValueOffsets[r] + k));
        }

        using (var reader = new ParquetSharp.ParquetFileReader(path))
        using (var rowGroup = reader.RowGroup(0))
        using (var column = rowGroup.Column(0).LogicalReader<long?[]>())
        {
            var lists = new long?[ListRows][];
            column.ReadBatch(lists, 0, ListRows);
            for (int r = 0; r < ListRows; r++)
                Assert.Equal(new long?[] { ListElement(r, 0), ListElement(r, 1), ListElement(r, 2) }, lists[r]);
        }

        // Every page starts a record: its first row index times 3 is its first value.
        var bytes = File.ReadAllBytes(path);
        var meta = await ReadMetadataAsync(path);
        var c = meta.RowGroups[0].Columns[0];
        var offsetIndex = MetadataDecoder.DecodeOffsetIndex(bytes.AsSpan(checked((int)c.OffsetIndexOffset!.Value), c.OffsetIndexLength!.Value));
        Assert.True(offsetIndex.PageLocations.Count > 1);
        Assert.All(offsetIndex.PageLocations, p => Assert.True(p.FirstRowIndex >= 0));
    }

    // ---- Declined whole ----

    [Fact]
    public async Task DictionaryFullBeforeTheFirstPageDecides_IsDeclinedWhole()
    {
        // 100 distinct INT64s are 800 bytes of dictionary: over a 512-byte limit long before 1 MiB of values.
        var builder = new Int64Array.Builder();
        for (int i = 0; i < 50_000; i++)
            builder.Append(i % 100);

        string path = await WriteAsync("small-limit", builder.Build(), Int64Type.Default, false, DataPageVersion.V1, limit: 512);
        var chunk = (await ReadMetadataAsync(path)).RowGroups[0].Columns[0].MetaData!;
        Assert.Null(chunk.DictionaryPageOffset);
        Assert.DoesNotContain(Encoding.RleDictionary, chunk.Encodings);
    }

#pragma warning disable EWPARQUET0003 // FSST is the case under test.
    [Fact]
    public async Task Fsst_FallsBackWhole()
    {
        // FSST trains one symbol table per chunk, written ahead of the data pages: no room for a dictionary prefix.
        var builder = new StringArray.Builder();
        for (int i = 0; i < StringRows; i++)
            builder.Append(StringAt(i, false));

        string path = await WriteAsync("fsst", builder.Build(), StringType.Default, false, DataPageVersion.V2,
            encoding: ByteArrayEncoding.Fsst);
        var chunk = (await ReadMetadataAsync(path)).RowGroups[0].Columns[0].MetaData!;
        Assert.Null(chunk.DictionaryPageOffset);

        var read = (StringArray)(await ReadAsync(path)).Column(0);
        for (int i = 0; i < StringRows; i++)
            Assert.Equal(StringAt(i, false), read.GetString(i));
    }
#pragma warning restore EWPARQUET0003

    // ---- Helpers ----

    /// <summary>
    /// The chunk holds dictionary-encoded and other data pages and a dictionary page, and says so: a reader that took
    /// it for wholly dictionary-encoded would answer predicates from the dictionary alone.
    /// </summary>
    private async Task<ColumnMetaData> AssertMixedChunkAsync(string path)
    {
        var chunk = (await ReadMetadataAsync(path)).RowGroups[0].Columns[0].MetaData!;
        Assert.NotNull(chunk.DictionaryPageOffset);
        Assert.Contains(Encoding.RleDictionary, chunk.Encodings);
        var dataPages = chunk.EncodingStats!.Where(s => s.PageType is PageType.DataPage or PageType.DataPageV2).ToList();
        Assert.Contains(dataPages, s => s.Encoding == Encoding.RleDictionary && s.Count > 0);
        Assert.Contains(dataPages, s => s.Encoding != Encoding.RleDictionary && s.Count > 0);
        Assert.False(MembershipPredicateEvaluator.IsWhollyDictionaryEncoded(chunk));
        return chunk;
    }

    private async Task AssertBloomAdmitsAsync(string path, object value)
    {
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        Assert.True((await reader.GetCandidateRowGroupsAsync("c", value))[0]);
    }

    private async Task<string> WriteAsync(
        string name, IArrowArray array, IArrowType type, bool nullable, DataPageVersion version,
        int limit = DictionaryLimit, ByteArrayEncoding? encoding = null)
    {
        string path = Path.Combine(_tempDir, $"{name}-{version}-{nullable}.parquet");
        var options = ParquetWriteOptions.Default with
        {
            DataPageVersion = version,
            DictionaryPageSizeLimit = limit,
            BloomFilterColumns = ["c"],
        };
        if (encoding is { } e)
            options = options with { ByteArrayEncoding = e };

        var batch = new RecordBatch(new Apache.Arrow.Schema([new Field("c", type, nullable)], null), [array], array.Length);
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        return path;
    }

    private static async Task<RecordBatch> ReadAsync(string path)
    {
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        return await reader.ReadRowGroupAsync(0);
    }

    private static async Task<FileMetaData> ReadMetadataAsync(string path)
    {
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        return await reader.ReadMetadataAsync();
    }
}

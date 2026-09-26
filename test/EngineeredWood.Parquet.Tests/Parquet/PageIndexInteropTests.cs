// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Tests.Parquet.Interop;
using Xunit.Abstractions;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Page indexes written by EngineeredWood, read by engines that use them.
/// </summary>
/// <remarks>
/// <para><b>DataFusion</b> (arrow-rs) reads page indexes by default and prunes pages with them;
/// <b>DuckDB</b> ignores them. So on the same file and predicate, DataFusion's count must equal
/// DuckDB's and the true answer, and DataFusion's <c>EXPLAIN ANALYZE</c> must show pages pruned — a
/// count alone cannot tell pruning from a full scan.</para>
/// <para>The mutation test shows the oracle can fail: it lowers one page's recorded maximum below a
/// value that page holds, and DataFusion then loses the row.</para>
/// </remarks>
public class PageIndexInteropTests : IDisposable
{
    private readonly string _tempDir;
    private readonly ITestOutputHelper _output;

    public PageIndexInteropTests(ITestOutputHelper output)
    {
        _output = output;
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-pageindex-interop-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private const int Rows = 60_000;

    private static readonly ParquetWriteOptions Indexed = ParquetWriteOptions.Default with
    {
        WritePageIndex = true,
        DataPageSize = 1024,
    };

    public static TheoryData<DataPageVersion, bool> Layouts() => new()
    {
        { DataPageVersion.V1, false },
        { DataPageVersion.V1, true },
        { DataPageVersion.V2, false },
        { DataPageVersion.V2, true },
    };

    /// <summary>
    /// A sorted column in many small pages: DataFusion must answer correctly and prune most pages.
    /// <c>k</c> repeats each value 100 times, so the dictionary takes it; <c>x</c> is unique, so it
    /// does not.
    /// </summary>
    [SkippableTheory]
    [MemberData(nameof(Layouts))]
    public async Task DataFusion_PrunesPagesAndAnswersCorrectly(DataPageVersion version, bool dictionary)
    {
        ExternalParquetReaders.Require();

        string path = await WriteAsync(SortedBatch(), Indexed with { DataPageVersion = version, DictionaryEnabled = dictionary });

        await AssertAgreesAndPrunes(path, "x BETWEEN 30000 AND 30150", 151);
        await AssertAgreesAndPrunes(path, "k = 250", 100);
        await AssertAgreesAndPrunes(path, $"s = '{LongString(41_000)}'", 1);
        // Every t shares a 70-byte prefix, so every page's truncated bounds are the same and nothing can
        // be pruned; what matters is that the shortened bounds still hold every row.
        await AssertAgreesAndPrunes(path, $"t = '{SharedPrefixString(41_000)}'", 1, expectPruning: false);
        await AssertAgreesAndPrunes(path, "d > 580.00", 999);
        await AssertAgreesAndPrunes(path, "d < -9.50", 50);
        await AssertAgreesAndPrunes(path, "u > 4294000000", 0, expectPruning: false);
        await AssertAgreesAndPrunes(path, "u >= 3000059000", 1000);
    }

    [SkippableFact]
    public async Task DataFusion_BufferedWriter()
    {
        ExternalParquetReaders.Require();

        var batch = SortedBatch(flatOnly: true);
        string path = Path.Combine(_tempDir, "buffered.parquet");
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false, Indexed))
        {
            await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }

        await AssertAgreesAndPrunes(path, "x BETWEEN 30000 AND 30150", 151);
        await AssertAgreesAndPrunes(path, "k = 250", 100);
    }

    /// <summary>
    /// Without this, the tests above could pass against a DataFusion that ignored the index. Lowering
    /// one page's maximum below a value that page holds makes DataFusion prune the page and lose the
    /// row; DuckDB, which ignores the index, still finds it.
    /// </summary>
    [SkippableFact]
    public async Task Mutation_LoweredMaximum_MakesDataFusionDropTheRow()
    {
        ExternalParquetReaders.Require();

        string path = await WriteAsync(SortedBatch(), Indexed with { DictionaryEnabled = false });
        byte[] bytes = File.ReadAllBytes(path);
        var metadata = ReadMetadata(bytes);
        var chunk = metadata.RowGroups[0].Columns.Single(c => c.MetaData!.PathInSchema![0] == "x");
        var columnIndex = MetadataDecoder.DecodeColumnIndex(
            bytes.AsSpan(checked((int)chunk.ColumnIndexOffset!.Value), chunk.ColumnIndexLength!.Value));
        var offsetIndex = MetadataDecoder.DecodeOffsetIndex(
            bytes.AsSpan(checked((int)chunk.OffsetIndexOffset!.Value), chunk.OffsetIndexLength!.Value));

        // Page 20 holds rows first..last, and x equals the row number; record its maximum as its minimum.
        int page = 20;
        long last = offsetIndex.PageLocations[page + 1].FirstRowIndex - 1;
        var maxes = columnIndex.MaxValues.ToArray();
        maxes[page] = columnIndex.MinValues[page];
        byte[] patched = MetadataEncoder.EncodeColumnIndex(new ColumnIndex
        {
            NullPages = columnIndex.NullPages,
            MinValues = columnIndex.MinValues,
            MaxValues = maxes,
            BoundaryOrder = columnIndex.BoundaryOrder,
            NullCounts = columnIndex.NullCounts,
        });
        Assert.Equal(chunk.ColumnIndexLength, patched.Length);

        string predicate = $"x = {last}";
        var before = ExternalParquetReaders.CountWhere(ExternalParquetReaders.DataFusion, path, predicate);
        Skip.IfNot(before.Installed, $"datafusion is not installed: {before.Error}");
        Assert.Equal(1, before.Count);

        patched.CopyTo(bytes, checked((int)chunk.ColumnIndexOffset.Value));
        string mutated = Path.Combine(_tempDir, "mutated.parquet");
        File.WriteAllBytes(mutated, bytes);

        Assert.Equal(0, ExternalParquetReaders.CountWhere(ExternalParquetReaders.DataFusion, mutated, predicate).Count);
        var duckDb = ExternalParquetReaders.CountWhere(ExternalParquetReaders.DuckDb, mutated, predicate);
        if (duckDb.Installed)
            Assert.Equal(1, duckDb.Count);
    }

    /// <summary>pyarrow, a third reader lineage, sees both indexes where EW wrote them.</summary>
    [SkippableFact]
    public async Task PyArrow_SeesBothIndexes()
    {
        ExternalParquetReaders.Require();

        string path = await WriteAsync(SortedBatch(), Indexed with
        {
            RowGroupMaxRows = 25_000,
            ColumnWriteStatistics = new Dictionary<string, bool> { ["k"] = false },
        });

        var (chunks, error) = ExternalParquetReaders.PyArrowPageIndexPresence(path);
        Skip.If(chunks is null, $"pyarrow unavailable: {error}");

        Assert.Equal(3 * 6, chunks!.Count);
        Assert.All(chunks, c => Assert.True(c.HasOffsetIndex, $"{c.Path} in row group {c.RowGroup}"));
        Assert.All(chunks, c => Assert.Equal(c.Path != "k", c.HasColumnIndex));
    }

    // ───── Helpers ─────

    private async Task AssertAgreesAndPrunes(string path, string predicate, long expected, bool expectPruning = true)
    {
        var dataFusion = ExternalParquetReaders.CountWhere(ExternalParquetReaders.DataFusion, path, predicate);
        Skip.IfNot(dataFusion.Installed, $"datafusion is not installed: {dataFusion.Error}");
        Assert.True(dataFusion.Count is not null, $"DataFusion refused '{predicate}': {dataFusion.Error}");
        Assert.True(expected == dataFusion.Count, $"'{predicate}': DataFusion counted {dataFusion.Count}, expected {expected}");

        var duckDb = ExternalParquetReaders.CountWhere(ExternalParquetReaders.DuckDb, path, predicate);
        if (duckDb.Installed)
            Assert.True(expected == duckDb.Count, $"'{predicate}': DuckDB counted {duckDb.Count}, expected {expected}");

        var (metrics, error) = ExternalParquetReaders.DataFusionPageIndexMetrics(path, predicate);
        Assert.True(metrics is not null, error);
        _output.WriteLine($"{predicate,-40} rows {metrics!.RowsMatched}/{metrics.RowsTotal} survived the page index");
        if (expectPruning)
        {
            // The matching rows, plus what shares their first and last pages: a few hundred at most.
            Assert.True(metrics.Found, $"'{predicate}': DataFusion reported no page-index pruning at all");
            Assert.True(metrics.RowsMatched >= expected && metrics.RowsMatched <= expected + 1000,
                $"'{predicate}': {metrics.RowsMatched} of {metrics.RowsTotal} rows survived the page index");
        }

        await Task.CompletedTask;
    }

    /// <summary>76 bytes, distinct within the first six, so bounds shortened to 64 still tell pages apart.</summary>
    private static string LongString(int i) => i.ToString("D6") + new string('p', 70);

    /// <summary>76 bytes, distinct only after the first 70, beyond any 64-byte bound.</summary>
    private static string SharedPrefixString(int i) => new string('p', 70) + i.ToString("D6");

    /// <summary>
    /// Sorted columns, row r holding: <c>x</c> = r, <c>k</c> = r / 100, <c>s</c> and <c>t</c> 76-byte
    /// strings (so bounds are truncated at 64), <c>d</c> = decimal(10,2) r / 100 - 10 (negative, then
    /// positive), and <c>u</c> = UInt32 3,000,000,000 + r (above the signed range).
    /// </summary>
    private static RecordBatch SortedBatch(bool flatOnly = false)
    {
        var x = new Int64Array.Builder();
        var k = new Int32Array.Builder();
        var s = new StringArray.Builder();
        var t = new StringArray.Builder();
        var d = new Decimal128Array.Builder(new Decimal128Type(10, 2));
        var u = new UInt32Array.Builder();
        for (int r = 0; r < Rows; r++)
        {
            x.Append(r);
            k.Append(r / 100);
            s.Append(LongString(r));
            t.Append(SharedPrefixString(r));
            d.Append(r / 100m - 10m);
            u.Append(3_000_000_000u + (uint)r);
        }

        var fields = new List<Field>
        {
            new("x", Int64Type.Default, false),
            new("k", Int32Type.Default, false),
            new("s", StringType.Default, false),
            new("u", UInt32Type.Default, false),
        };
        var arrays = new List<IArrowArray> { x.Build(), k.Build(), s.Build(), u.Build() };
        if (!flatOnly)
        {
            fields.Insert(3, new Field("d", new Decimal128Type(10, 2), false));
            arrays.Insert(3, d.Build());
            fields.Add(new Field("t", StringType.Default, false));
            arrays.Add(t.Build());
        }

        return new RecordBatch(new Apache.Arrow.Schema(fields, null), arrays, Rows);
    }

    private async Task<string> WriteAsync(RecordBatch batch, ParquetWriteOptions options)
    {
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private static FileMetaData ReadMetadata(byte[] bytes)
    {
        int footerLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
        return MetadataDecoder.DecodeFileMetaData(bytes.AsSpan(bytes.Length - 8 - footerLength, footerLength));
    }
}

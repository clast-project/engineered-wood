// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Tests.Parquet;

public class PredicatePushdownTests : IDisposable
{
    private readonly string _tempDir;

    public PredicatePushdownTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-pushdown-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    /// <summary>
    /// Writes a Parquet file with three row groups, each holding 100 rows of
    /// disjoint integer ranges in column "id":
    ///   RG 0: ids 0..99
    ///   RG 1: ids 100..199
    ///   RG 2: ids 200..299
    /// The column is INT32, so writers emit min/max statistics for each row
    /// group. A predicate on "id" should let the reader skip the row groups
    /// whose ranges don't satisfy it.
    /// </summary>
    private async Task<string> WriteThreeRangedRowGroups(string fileName)
    {
        string path = Path.Combine(_tempDir, fileName);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Build();

        var options = new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            for (int rg = 0; rg < 3; rg++)
            {
                int start = rg * 100;
                var builder = new Int32Array.Builder();
                for (int i = 0; i < 100; i++) builder.Append(start + i);
                var batch = new RecordBatch(schema, [builder.Build()], 100);
                await writer.WriteRowGroupAsync(batch);
            }
            await writer.CloseAsync();
        }
        return path;
    }

    private static async Task<int> CountRows(ParquetFileReader reader)
    {
        int total = 0;
        await foreach (var b in reader.ReadAllAsync())
            total += b.Length;
        return total;
    }

    private static async Task<List<RecordBatch>> CollectBatches(ParquetFileReader reader)
    {
        var batches = new List<RecordBatch>();
        await foreach (var b in reader.ReadAllAsync())
            batches.Add(b);
        return batches;
    }

    // ── Per-read candidates (#55) ──

    public static TheoryData<string> CandidateCases => ["none", "one", "upper", "lower", "in", "unknown"];

    private static EngineeredWood.Expressions.Predicate CandidateCase(string name) => name switch
    {
        "none" => Ex.Equal("id", 999),
        "one" => Ex.Equal("id", 150),
        "upper" => Ex.GreaterThan("id", 199),
        "lower" => Ex.LessThan("id", 100),
        "in" => Ex.In("id", 50, 250),
        _ => Ex.Equal("nonexistent", 5),
    };

    /// <summary>
    /// The per-read verdict is the one <see cref="ParquetReadOptions.Filter"/> reads under: a row group is
    /// a candidate exactly when the filtered <c>ReadAllAsync</c> returns its rows.
    /// </summary>
    [Theory]
    [MemberData(nameof(CandidateCases))]
    public async Task CandidateRowGroups_AgreeWithTheFilteredRead(string name)
    {
        var filter = CandidateCase(name);
        string path = await WriteThreeRangedRowGroups($"candidates_{name}.parquet");

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var candidates = await reader.GetCandidateRowGroupsAsync(filter);

        await using var filteredFile = new LocalRandomAccessFile(path);
        await using var filtered = new ParquetFileReader(filteredFile, ownsFile: false,
            new ParquetReadOptions { Filter = filter });
        var readGroups = new HashSet<int>();
        await foreach (var batch in filtered.ReadAllAsync())
            readGroups.Add(((Int32Array)batch.Column(0)).GetValue(0)!.Value / 100);

        Assert.Equal(3, candidates.Length);
        for (int rg = 0; rg < 3; rg++)
            Assert.Equal(readGroups.Contains(rg), candidates[rg]);
    }

    [Fact]
    public async Task CandidateRowGroups_DoNotDependOnTheReadersFilter()
    {
        string path = await WriteThreeRangedRowGroups("candidates_own.parquet");
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions { Filter = Ex.Equal("id", 999) });

        var candidates = await reader.GetCandidateRowGroupsAsync(Ex.LessThan("id", 100));

        Assert.True(candidates[0]);
        Assert.False(candidates[1]);
        Assert.False(candidates[2]);
    }

    // ── Basic pruning ──

    [Fact]
    public async Task NoFilter_ReadsAllRowGroups()
    {
        string path = await WriteThreeRangedRowGroups("no_filter.parquet");
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        Assert.Equal(300, await CountRows(reader));
    }

    [Fact]
    public async Task Equal_OutsideAllRanges_SkipsAll()
    {
        string path = await WriteThreeRangedRowGroups("eq_none.parquet");
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions { Filter = Ex.Equal("id", 999) });

        var batches = await CollectBatches(reader);
        Assert.Empty(batches); // all three row groups pruned
    }

    [Fact]
    public async Task Equal_InOneRange_KeepsOnlyThat()
    {
        string path = await WriteThreeRangedRowGroups("eq_one.parquet");
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions { Filter = Ex.Equal("id", 150) });

        var batches = await CollectBatches(reader);
        Assert.Single(batches);
        Assert.Equal(100, batches[0].Length); // RG 1 has 100 rows
    }

    [Fact]
    public async Task GreaterThan_PrunesLowerRowGroups()
    {
        string path = await WriteThreeRangedRowGroups("gt.parquet");
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions { Filter = Ex.GreaterThan("id", 199) });

        // RG 0 (max 99) and RG 1 (max 199) pruned; RG 2 kept.
        var batches = await CollectBatches(reader);
        Assert.Single(batches);
    }

    [Fact]
    public async Task LessThan_PrunesUpperRowGroups()
    {
        string path = await WriteThreeRangedRowGroups("lt.parquet");
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions { Filter = Ex.LessThan("id", 100) });

        // RG 0 (min 0) kept; RGs 1-2 (min >= 100) pruned.
        var batches = await CollectBatches(reader);
        Assert.Single(batches);
    }

    [Fact]
    public async Task And_NarrowsResults()
    {
        string path = await WriteThreeRangedRowGroups("and.parquet");
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions
            {
                Filter = Ex.And(
                    Ex.GreaterThanOrEqual("id", 100),
                    Ex.LessThanOrEqual("id", 199)),
            });

        var batches = await CollectBatches(reader);
        Assert.Single(batches);
    }

    [Fact]
    public async Task Or_KeepsMatchingRowGroups()
    {
        string path = await WriteThreeRangedRowGroups("or.parquet");
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions
            {
                Filter = Ex.Or(
                    Ex.LessThan("id", 50),
                    Ex.GreaterThan("id", 250)),
            });

        // RG 0 (0-99) and RG 2 (200-299) kept; RG 1 (100-199) pruned.
        var batches = await CollectBatches(reader);
        Assert.Equal(2, batches.Count);
    }

    [Fact]
    public async Task In_NoMatchingValues_SkipsAll()
    {
        string path = await WriteThreeRangedRowGroups("in_none.parquet");
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions { Filter = Ex.In("id", 500, 600, 700) });

        var batches = await CollectBatches(reader);
        Assert.Empty(batches);
    }

    [Fact]
    public async Task In_OneMatchingValue_KeepsOneRowGroup()
    {
        string path = await WriteThreeRangedRowGroups("in_one.parquet");
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions { Filter = Ex.In("id", 250, 999) });

        var batches = await CollectBatches(reader);
        Assert.Single(batches); // only RG 2 contains 250
    }

    [Fact]
    public async Task UnknownColumn_KeepsAll()
    {
        string path = await WriteThreeRangedRowGroups("unknown.parquet");
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions { Filter = Ex.Equal("nonexistent", 5) });

        // Conservative: missing stats → Unknown → keep all row groups.
        var batches = await CollectBatches(reader);
        Assert.Equal(3, batches.Count);
    }

    // ── String column ──

    [Fact]
    public async Task StringEqual_OutOfRange_SkipsRowGroups()
    {
        string path = Path.Combine(_tempDir, "string.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", StringType.Default, false))
            .Build();

        var options = new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            // RG 0: names "alice" .. "frank"
            var b1 = new StringArray.Builder();
            foreach (var s in new[] { "alice", "bob", "carol", "dave", "eve", "frank" })
                b1.Append(s);
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [b1.Build()], 6));

            // RG 1: names "noah" .. "zoe"
            var b2 = new StringArray.Builder();
            foreach (var s in new[] { "noah", "olivia", "paul", "zoe" })
                b2.Append(s);
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [b2.Build()], 4));

            await writer.CloseAsync();
        }

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false,
            new ParquetReadOptions { Filter = Ex.Equal("name", "noah") });

        var batches = await CollectBatches(reader);
        Assert.Single(batches);
        Assert.Equal(4, batches[0].Length); // RG 1 only
    }

    // ── Nulls ──

    [Fact]
    public async Task IsNull_NoNulls_SkipsRowGroup()
    {
        string path = Path.Combine(_tempDir, "isnull.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("v", Int32Type.Default, true))
            .Build();

        var options = new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            // RG 0: no nulls
            var b1 = new Int32Array.Builder();
            for (int i = 0; i < 50; i++) b1.Append(i);
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [b1.Build()], 50));

            // RG 1: some nulls
            var b2 = new Int32Array.Builder();
            for (int i = 0; i < 50; i++)
            {
                if (i % 5 == 0) b2.AppendNull();
                else b2.Append(i + 100);
            }
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [b2.Build()], 50));

            await writer.CloseAsync();
        }

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false,
            new ParquetReadOptions { Filter = Ex.IsNull("v") });

        var batches = await CollectBatches(reader);
        Assert.Single(batches); // only RG 1 has nulls
    }
}

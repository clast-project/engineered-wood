// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Tests.Parquet;

public class BloomFilterPushdownTests : IDisposable
{
    private readonly string _tempDir;

    public BloomFilterPushdownTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-bloom-pd-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    /// <summary>
    /// Writes a Parquet file with a single string column "name", three row
    /// groups containing distinct overlapping value sets, and Bloom filters
    /// enabled. Statistics ranges overlap so the stats evaluator can't decide
    /// equality predicates — Bloom filters carry the proof.
    /// </summary>
    private async Task<string> WriteThreeRowGroupsWithBloom(string fileName)
    {
        string path = Path.Combine(_tempDir, fileName);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", StringType.Default, false))
            .Build();

        var options = new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            BloomFilterColumns = new HashSet<string> { "name" },
        };

        // Three row groups whose min/max ranges all span "a".."z" but contain
        // disjoint sets — so an equality probe needs the Bloom filter to skip.
        string[][] groups =
        [
            ["alpha", "zebra", "carrot"],
            ["apple",  "zinnia", "cherry"],
            ["avocado", "zucchini", "celery"],
        ];

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            foreach (var values in groups)
            {
                var b = new StringArray.Builder();
                foreach (var s in values) b.Append(s);
                await writer.WriteRowGroupAsync(new RecordBatch(schema, [b.Build()], values.Length));
            }
            await writer.CloseAsync();
        }
        return path;
    }

    private static async Task<List<RecordBatch>> Collect(ParquetFileReader reader)
    {
        var batches = new List<RecordBatch>();
        await foreach (var b in reader.ReadAllAsync()) batches.Add(b);
        return batches;
    }

    [Fact]
    public async Task BloomFilter_AbsentValue_PrunesAllRowGroups()
    {
        string path = await WriteThreeRowGroupsWithBloom("absent.parquet");

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions
            {
                Filter = Ex.Equal("name", "definitely_not_present"),
                FilterUseBloomFilters = true,
            });

        var batches = await Collect(reader);
        Assert.Empty(batches);
    }

    // ── Floating point: SQL equality is not bit equality ──

    /// <summary>A NaN whose payload differs from <see cref="double.NaN"/>'s.</summary>
    private static readonly double OtherNaN = BitConverter.Int64BitsToDouble(0x7FF8_0000_0000_0001);

    /// <summary>
    /// One row group of <c>{stored, 5, -3}</c>: statistics span the literal, so only the Bloom filter can
    /// rule the group out, and it must not when a stored value is EQUAL to the literal in SQL.
    /// </summary>
    private async Task<int> RowsReadWithBloom(string name, double stored, double literal)
    {
        string path = Path.Combine(_tempDir, name + ".parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", DoubleType.Default, false))
            .Build();
        var options = new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            BloomFilterColumns = new HashSet<string> { "x" },
        };
        var values = new DoubleArray.Builder().Append(stored).Append(5.0).Append(-3.0).Build();

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [values], 3));
            await writer.CloseAsync();
        }

        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false,
            new ParquetReadOptions { Filter = Ex.Equal("x", literal), FilterUseBloomFilters = true });
        int rows = 0;
        await foreach (var batch in reader.ReadAllAsync())
            rows += batch.Length;
        return rows;
    }

    [Theory]
    [InlineData("neg_zero_stored", -0.0, 0.0)]
    [InlineData("pos_zero_stored", 0.0, -0.0)]
    public async Task BloomFilter_ZeroMatchesTheOtherZero(string name, double stored, double literal)
    {
        Assert.Equal(3, await RowsReadWithBloom(name, stored, literal));
    }

    [Fact]
    public async Task BloomFilter_NaNMatchesAnyNaN()
    {
        Assert.Equal(3, await RowsReadWithBloom("nan", OtherNaN, double.NaN));
    }

    [Fact]
    public async Task BloomFilter_FloatZeroMatchesTheOtherZero()
    {
        string path = Path.Combine(_tempDir, "float_zero.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", FloatType.Default, false))
            .Build();
        var options = new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            BloomFilterColumns = new HashSet<string> { "x" },
        };
        var values = new FloatArray.Builder().Append(-0f).Append(5f).Append(-3f).Build();

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [values], 3));
            await writer.CloseAsync();
        }

        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false,
            new ParquetReadOptions { Filter = Ex.Equal("x", 0f), FilterUseBloomFilters = true });
        int rows = 0;
        await foreach (var batch in reader.ReadAllAsync())
            rows += batch.Length;
        Assert.Equal(3, rows);
    }

    [Theory]
    [InlineData("zero_absent", 1.0, 0.0)] // neither zero stored: both probes miss
    [InlineData("value_absent", 0.0, 1.0)]
    public async Task BloomFilter_StillPrunesAnAbsentValue(string name, double stored, double literal)
    {
        Assert.Equal(0, await RowsReadWithBloom(name, stored, literal));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CandidateRowGroups_ProbeBloomFilters_OnlyWhenOptedIn(bool useBloom)
    {
        string path = await WriteThreeRowGroupsWithBloom($"candidates_bloom_{useBloom}.parquet");

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions { FilterUseBloomFilters = useBloom });

        var candidates = await reader.GetCandidateRowGroupsAsync(Ex.Equal("name", "definitely_not_present"));

        for (int rg = 0; rg < candidates.Length; rg++)
            Assert.Equal(!useBloom, candidates[rg]);
    }

    [Fact]
    public async Task BloomFilter_PresentValue_KeepsContainingRowGroup()
    {
        string path = await WriteThreeRowGroupsWithBloom("present.parquet");

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions
            {
                Filter = Ex.Equal("name", "carrot"), // only in row group 0
                FilterUseBloomFilters = true,
            });

        var batches = await Collect(reader);
        Assert.Single(batches);
        Assert.Equal(3, batches[0].Length);
    }

    [Fact]
    public async Task BloomFilter_NotEnabled_DoesNotProbe()
    {
        // Without the FilterUseBloomFilters flag, stats alone can't decide
        // (overlapping string ranges include "definitely_not_present"
        // lexicographically), so all 3 row groups are kept.
        string path = await WriteThreeRowGroupsWithBloom("disabled.parquet");

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions
            {
                Filter = Ex.Equal("name", "definitely_not_present"),
                // FilterUseBloomFilters defaults to false
            });

        var batches = await Collect(reader);
        // String range "alpha".."zinnia" includes "definitely_not_present"
        // lexicographically, so stats can't prune. All 3 RGs come through.
        Assert.Equal(3, batches.Count);
    }

    [Fact]
    public async Task BloomFilter_In_AllAbsent_PrunesAll()
    {
        string path = await WriteThreeRowGroupsWithBloom("in_absent.parquet");

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions
            {
                Filter = Ex.In("name", "ghost", "phantom", "specter"),
                FilterUseBloomFilters = true,
            });

        var batches = await Collect(reader);
        Assert.Empty(batches);
    }

    [Fact]
    public async Task BloomFilter_In_OnePresent_KeepsThatGroup()
    {
        string path = await WriteThreeRowGroupsWithBloom("in_one.parquet");

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions
            {
                Filter = Ex.In("name", "ghost", "celery", "phantom"), // celery in RG 2
                FilterUseBloomFilters = true,
            });

        var batches = await Collect(reader);
        Assert.Single(batches);
    }

    [Fact]
    public async Task BloomFilter_And_OneMissingValue_PrunesAll()
    {
        string path = await WriteThreeRowGroupsWithBloom("and.parquet");

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions
            {
                Filter = Ex.And(
                    Ex.Equal("name", "carrot"),  // only in RG 0
                    Ex.Equal("name", "ghost")),  // in none
                FilterUseBloomFilters = true,
            });

        var batches = await Collect(reader);
        // The "ghost" sub-predicate misses every row group → AND → AlwaysFalse.
        Assert.Empty(batches);
    }

    [Fact]
    public async Task BloomFilter_Or_AllMissing_PrunesAll()
    {
        string path = await WriteThreeRowGroupsWithBloom("or_none.parquet");

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions
            {
                Filter = Ex.Or(
                    Ex.Equal("name", "ghost"),
                    Ex.Equal("name", "phantom")),
                FilterUseBloomFilters = true,
            });

        var batches = await Collect(reader);
        Assert.Empty(batches);
    }

    [Fact]
    public async Task BloomFilter_Or_OnePresent_KeepsThatRowGroup()
    {
        string path = await WriteThreeRowGroupsWithBloom("or_one.parquet");

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions
            {
                Filter = Ex.Or(
                    Ex.Equal("name", "ghost"),    // missing
                    Ex.Equal("name", "cherry")),  // in RG 1
                FilterUseBloomFilters = true,
            });

        var batches = await Collect(reader);
        Assert.Single(batches);
    }

    [Fact]
    public async Task BloomFilter_NoBloomForColumn_DoesNotPrune()
    {
        // Write a file with NO bloom filters; equality predicate misses range.
        string path = Path.Combine(_tempDir, "nobloom.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", StringType.Default, false))
            .Build();

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed }))
        {
            var b = new StringArray.Builder();
            b.Append("alpha"); b.Append("zinnia");
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [b.Build()], 2));
            await writer.CloseAsync();
        }

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false,
            new ParquetReadOptions
            {
                Filter = Ex.Equal("name", "middle"), // in range, can't be pruned by stats either
                FilterUseBloomFilters = true,
            });

        var batches = await Collect(reader);
        Assert.Single(batches); // no bloom + can't prune by stats → kept
    }
}

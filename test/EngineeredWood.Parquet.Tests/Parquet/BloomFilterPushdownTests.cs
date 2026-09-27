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

    // ── Requests: filters are read a window of row groups at a time ──

    /// <summary>
    /// Eight row groups of a key with a Bloom filter. Groups 0-5 hold the extremes "a-min" and "z-max",
    /// so statistics leave an equality on a middle value undecided; groups 6-7 hold only "y" values, so
    /// statistics rule out any value below "y". "k3" is only in group 3. Each group repeats its values,
    /// so the key is also dictionary-encoded.
    /// </summary>
    private async Task<string> WriteBloomPrefetchFile(string name)
    {
        string path = Path.Combine(_tempDir, name + ".parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("key", StringType.Default, false))
            .Build();
        var options = new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            BloomFilterColumns = new HashSet<string> { "key" },
        };

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            for (int g = 0; g < 8; g++)
            {
                string[] values = g < 6 ? ["a-min", "z-max", "common", $"k{g}"] : ["y1", "y2"];
                var builder = new StringArray.Builder();
                for (int r = 0; r < 10; r++)
                    foreach (var v in values)
                        builder.Append(v);
                await writer.WriteRowGroupAsync(new RecordBatch(schema, [builder.Build()], values.Length * 10));
            }
            await writer.CloseAsync();
        }
        return path;
    }

    private static async Task<(bool[] Candidates, List<int> Requests)> CandidatesCounted(
        string path, EngineeredWood.Expressions.Predicate filter, ParquetReadOptions options)
    {
        await using var input = new RequestCountingFile(new LocalRandomAccessFile(path));
        await using var reader = new ParquetFileReader(input, ownsFile: false, options);
        await reader.ReadMetadataAsync(); // footer requests are not the subject
        input.Requests.Clear();

        var bits = await reader.GetCandidateRowGroupsAsync(filter);
        return (Enumerable.Range(0, bits.Length).Select(i => bits[i]).ToArray(), input.Requests.ToList());
    }

    private static readonly ParquetReadOptions BloomOnly =
        new() { FilterUseBloomFilters = true, FilterUseDictionaries = false };

    [Fact]
    public async Task BloomFilters_ForEveryUndecidedGroup_AreReadInOneRequest()
    {
        string path = await WriteBloomPrefetchFile("bloom_one_request");

        var (candidates, requests) = await CandidatesCounted(path, Ex.Equal("key", "k3"), BloomOnly);

        Assert.True(candidates[3]);
        Assert.False(candidates[6]); // statistics
        Assert.False(candidates[7]);
        // Six undecided groups, six filters, one request; groups 6-7 are never read.
        Assert.Equal(new[] { 6 }, requests);
    }

    /// <summary>
    /// With both sources on, each reads ahead once. The dictionary rules out every group but 3, so the
    /// Bloom step first runs at group 3, and its window covers the undecided groups from there on.
    /// </summary>
    [Fact]
    public async Task BothSources_ReadAheadOnceEach()
    {
        string path = await WriteBloomPrefetchFile("both_sources");

        var (candidates, requests) = await CandidatesCounted(path, Ex.Equal("key", "k3"),
            new ParquetReadOptions { FilterUseBloomFilters = true, FilterUseDictionaries = true });

        Assert.Equal(new[] { false, false, false, true, false, false, false, false }, candidates);
        Assert.Equal(new[] { 6, 3 }, requests);
    }

    /// <summary>
    /// A filter that cannot be parsed is declined, not trusted and not thrown: pruning must never be
    /// what fails a read. It used to throw out of the probe.
    /// </summary>
    [Fact]
    public async Task ACorruptBloomFilter_IsDeclinedNotThrown()
    {
        string path = await WriteBloomPrefetchFile("bloom_corrupt");
        byte[] file = File.ReadAllBytes(path);
        EngineeredWood.Parquet.Metadata.FileMetaData metadata;
        await using (var input = new LocalRandomAccessFile(path))
        using (var reader = new ParquetFileReader(input, ownsFile: false))
            metadata = await reader.ReadMetadataAsync();

        // Overwrite group 1's whole filter block (header and bitset).
        var meta = metadata.RowGroups[1].Columns[0].MetaData!;
        int offset = checked((int)meta.BloomFilterOffset!.Value);
        file.AsSpan(offset, meta.BloomFilterLength!.Value).Fill(0xFF);
        File.WriteAllBytes(path, file);

        var (candidates, _) = await CandidatesCounted(path, Ex.Equal("key", "k3"), BloomOnly);

        Assert.True(candidates[1]);
        Assert.True(candidates[3]);
    }

    /// <summary>
    /// The by-value probe applies the same guards: a filter that cannot be parsed leaves its group a
    /// candidate instead of failing the call.
    /// </summary>
    [Fact]
    public async Task CandidateRowGroups_ByValue_DeclineACorruptFilter()
    {
        string path = await WriteBloomPrefetchFile("by_value_corrupt");
        byte[] file = File.ReadAllBytes(path);
        EngineeredWood.Parquet.Metadata.FileMetaData metadata;
        await using (var input = new LocalRandomAccessFile(path))
        using (var reader = new ParquetFileReader(input, ownsFile: false))
            metadata = await reader.ReadMetadataAsync();

        var meta = metadata.RowGroups[1].Columns[0].MetaData!;
        file.AsSpan(checked((int)meta.BloomFilterOffset!.Value), meta.BloomFilterLength!.Value).Fill(0xFF);
        File.WriteAllBytes(path, file);

        await using var corrupt = new LocalRandomAccessFile(path);
        await using var probe = new ParquetFileReader(corrupt, ownsFile: false);
        var candidates = await probe.GetCandidateRowGroupsAsync("key", "k3");

        Assert.True(candidates[1]);
        Assert.True(candidates[3]);
    }

    [Fact]
    public void AFilterOverTheSizeCap_IsNotRead()
    {
        var chunk = new EngineeredWood.Parquet.Metadata.ColumnChunk
        {
            FileOffset = 4,
            MetaData = new EngineeredWood.Parquet.Metadata.ColumnMetaData
            {
                Type = PhysicalType.ByteArray,
                Encodings = [EngineeredWood.Parquet.Encoding.Plain],
                Codec = CompressionCodec.Uncompressed,
                NumValues = 1,
                TotalUncompressedSize = 1,
                TotalCompressedSize = 1,
                DataPageOffset = 4,
                BloomFilterOffset = 100,
                BloomFilterLength = MembershipPredicateEvaluator.MaxBloomFilterBytes + 1,
            },
        };

        Assert.False(MembershipPredicateEvaluator.TryGetBloomFilterRange(
            chunk, fileLength: long.MaxValue, ColumnChunkFilePathKind.Refuse, out _));
    }

    // ── Floating point: SQL equality is not bit equality ──

    /// <summary>A NaN whose payload differs from <see cref="double.NaN"/>'s.</summary>
    private static readonly double OtherNaN = BitConverter.Int64BitsToDouble(0x7FF8_0000_0000_0001);

    /// <summary>
    /// One row group of <c>{stored, 5, -3}</c>: statistics span the literal, so only the Bloom filter can
    /// rule the group out, and it must not when a stored value is EQUAL to the literal in SQL.
    /// </summary>
    private Task<int> RowsReadWithBloom(string name, double stored, double literal) =>
        RowsReadWithBloom(name, stored, Ex.Equal("x", literal));

    private async Task<int> RowsReadWithBloom(
        string name, double stored, EngineeredWood.Expressions.Predicate filter, bool asFloat = false)
    {
        string path = await WriteFloatingColumn(name, stored, asFloat);

        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false,
            new ParquetReadOptions { Filter = filter, FilterUseBloomFilters = true });
        int rows = 0;
        await foreach (var batch in reader.ReadAllAsync())
            rows += batch.Length;
        return rows;
    }

    /// <summary>One row group of <c>{stored, 5, -3}</c> in column "x", with a Bloom filter.</summary>
    private async Task<string> WriteFloatingColumn(string name, double stored, bool asFloat = false)
    {
        string path = Path.Combine(_tempDir, name + ".parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", asFloat ? FloatType.Default : DoubleType.Default, false))
            .Build();
        var options = new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            BloomFilterColumns = new HashSet<string> { "x" },
        };
        IArrowArray values = asFloat
            ? new FloatArray.Builder().Append((float)stored).Append(5f).Append(-3f).Build()
            : new DoubleArray.Builder().Append(stored).Append(5.0).Append(-3.0).Build();

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [values], 3));
            await writer.CloseAsync();
        }
        return path;
    }

    /// <summary>A float NaN whose payload differs from <see cref="float.NaN"/>'s.</summary>
    private static readonly float OtherFloatNaN = BitConverter.ToSingle(BitConverter.GetBytes(0x7FC0_0001), 0);

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
        Assert.Equal(3, await RowsReadWithBloom("float_zero", -0.0, Ex.Equal("x", 0f), asFloat: true));
    }

    [Fact]
    public async Task BloomFilter_FloatNaNMatchesAnyNaN()
    {
        Assert.Equal(3, await RowsReadWithBloom("float_nan", OtherFloatNaN, Ex.Equal("x", float.NaN), asFloat: true));
    }

    /// <summary>
    /// An IN list is kept when ANY member might match. A NaN member cannot be ruled out, and a zero member
    /// must be probed as both zeros, even beside members that are absent.
    /// </summary>
    [Theory]
    [InlineData("in_nan", double.NaN)]
    [InlineData("in_zero", 0.0)]
    public async Task BloomFilter_InList_KeepsTheGroupForAnEqualMember(string name, double member)
    {
        double stored = double.IsNaN(member) ? OtherNaN : -0.0;
        var filter = new EngineeredWood.Expressions.SetPredicate(
            new EngineeredWood.Expressions.UnboundReference("x"),
            [EngineeredWood.Expressions.LiteralValue.Of(1.0), EngineeredWood.Expressions.LiteralValue.Of(member)],
            EngineeredWood.Expressions.SetOperator.In);

        Assert.Equal(3, await RowsReadWithBloom(name, stored, filter));
    }

    // The public value probe answers the same question without a predicate, and must answer it the same way.

    [Theory]
    [InlineData("api_neg_zero", -0.0, 0.0)]
    [InlineData("api_pos_zero", 0.0, -0.0)]
    public async Task CandidateRowGroups_ByValue_ZeroMatchesTheOtherZero(string name, double stored, double value)
    {
        string path = await WriteFloatingColumn(name, stored);
        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false);

        Assert.True((await reader.GetCandidateRowGroupsAsync("x", value))[0]);
    }

    [Fact]
    public async Task CandidateRowGroups_ByValue_NaNMatchesAnyNaN()
    {
        string path = await WriteFloatingColumn("api_nan", OtherNaN);
        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false);

        Assert.True((await reader.GetCandidateRowGroupsAsync("x", double.NaN))[0]);
        Assert.True((await reader.GetCandidateRowGroupsAsync("x", new object[] { 1.0, double.NaN }))[0]);
    }

    [Fact]
    public async Task CandidateRowGroups_ByValue_StillRefusesAnIncompatibleValueAfterANaN()
    {
        string path = await WriteFloatingColumn("api_nan_then_bad", 1.0);
        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await reader.GetCandidateRowGroupsAsync("x", new object[] { double.NaN, "not a double" }));
    }

    [Fact]
    public async Task CandidateRowGroups_ByValue_StillRulesOutAnAbsentZero()
    {
        string path = await WriteFloatingColumn("api_zero_absent", 1.0);
        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false);

        Assert.False((await reader.GetCandidateRowGroupsAsync("x", 0.0))[0]);
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

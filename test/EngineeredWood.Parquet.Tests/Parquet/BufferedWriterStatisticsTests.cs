// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Statistics on the columns <see cref="BufferedParquetWriter"/> dictionary-encodes (issue #163).
/// </summary>
/// <remarks>
/// The gap was exactly the columns the writer exists for: it buffers dictionary indices rather than
/// value buffers, so a caller reaching for it is by construction feeding it low-cardinality data — which
/// is what gets dictionary-encoded, and what lost its statistics. The oracle throughout is
/// <see cref="ParquetFileWriter"/> on the same batch with the same options: same data, same encodings,
/// so the statistics have to match field for field.
/// </remarks>
public class BufferedWriterStatisticsTests : IDisposable
{
    private readonly string _tempDir;

    public BufferedWriterStatisticsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-bufstats-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private string TempPath(string name) => Path.Combine(_tempDir, name);

    /// <summary>Which floating-point physical type a theory case exercises.</summary>
    public enum FloatingPointCase
    {
        /// <summary>FLOAT.</summary>
        Float,

        /// <summary>DOUBLE.</summary>
        Double,
    }

    // The reported defect, stated as the comparison that found it. Every column is asserted, not just the
    // dictionary-encoded ones, so that a fix which broke the columns that already worked would fail here.
    [Fact]
    public async Task EveryColumn_MatchesTheDirectWriterFieldForField()
    {
        const int rows = 200;
        var lowCardInt = new Int32Array.Builder();
        var highCardInt = new Int32Array.Builder();
        var lowCardStr = new StringArray.Builder();
        var nullableLowCard = new Int64Array.Builder();
        for (int i = 0; i < rows; i++)
        {
            lowCardInt.Append(i % 3);
            highCardInt.Append(i);
            lowCardStr.Append("v" + (i % 3));
            if (i % 4 == 0) nullableLowCard.AppendNull(); else nullableLowCard.Append(i % 5);
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("lowCardInt", Int32Type.Default, nullable: false))
            .Field(new Field("highCardInt", Int32Type.Default, nullable: false))
            .Field(new Field("lowCardStr", StringType.Default, nullable: false))
            .Field(new Field("nullableLowCard", Int64Type.Default, nullable: true))
            .Build();
        var batch = new RecordBatch(schema,
            [lowCardInt.Build(), highCardInt.Build(), lowCardStr.Build(), nullableLowCard.Build()], rows);

        var (direct, buffered) = await WriteBothAsync("bothways", batch, DefaultOptions);

        Assert.Equal(direct.Count, buffered.Count);
        for (int i = 0; i < direct.Count; i++)
            AssertSameStatistics(direct[i], buffered[i], schema.FieldsList[i].Name);

        // The columns that regressed had NO statistics at all, so pin the positive assertion too:
        // matching a null oracle would otherwise be a way to pass.
        Assert.All(buffered, s => Assert.NotNull(s?.MinValue));

        // The null count is the other half of what a reader loses, and it is free from the row count.
        Assert.Equal(50, buffered[3]!.NullCount);
    }

    // The buffered writer's own reason for existing: a row group assembled from many batches. The
    // dictionary spans all of them, so the bounds have to as well — a per-batch answer would be wrong.
    [Fact]
    public async Task StatisticsSpanEveryAppendedBatch()
    {
        string path = TempPath("multibatch.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("n", Int32Type.Default, nullable: false)).Build();

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false, DefaultOptions))
        {
            // Each batch is low-cardinality on its own; only the union spans 10..49.
            foreach (int batchBase in new[] { 30, 10, 40 })
            {
                var b = new Int32Array.Builder();
                for (int i = 0; i < 50; i++) b.Append(batchBase + (i % 10));
                await writer.AppendAsync(new RecordBatch(schema, [b.Build()], 50));
            }
            await writer.CloseAsync();
        }

        var stats = Assert.Single(await ReadStatisticsAsync(path));
        Assert.NotNull(stats);
        Assert.Equal(10, BitConverter.ToInt32(stats!.MinValue!, 0));
        Assert.Equal(49, BitConverter.ToInt32(stats.MaxValue!, 0));
        Assert.Equal(0, stats.NullCount);
    }

    // nan_count counts VALUES, not distinct values — so it cannot come from the dictionary entries, where
    // every NaN collapses to one. Deriving it from the indices is the whole reason floating point is a
    // separate arm; this is the assertion that a dictionary-entry count, which would say 1, fails.
    [Theory]
    [InlineData(FloatingPointCase.Double)]
    [InlineData(FloatingPointCase.Float)]
    public async Task FloatingPointColumn_CountsEveryNaNRow_NotEveryNaNEntry(FloatingPointCase which)
    {
        const int rows = 200;
        int expectedNaN = 0;
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("f", which == FloatingPointCase.Double
                ? DoubleType.Default : (IArrowType)FloatType.Default, nullable: false))
            .Build();

        IArrowArray column;
        if (which == FloatingPointCase.Double)
        {
            var b = new DoubleArray.Builder();
            for (int i = 0; i < rows; i++)
            {
                if (i % 7 == 0) { b.Append(double.NaN); expectedNaN++; } else b.Append(i % 5);
            }
            column = b.Build();
        }
        else
        {
            var b = new FloatArray.Builder();
            for (int i = 0; i < rows; i++)
            {
                if (i % 7 == 0) { b.Append(float.NaN); expectedNaN++; } else b.Append(i % 5);
            }
            column = b.Build();
        }

        var batch = new RecordBatch(schema, [column], rows);
        var (direct, buffered) = await WriteBothAsync($"nan_{which}", batch, DefaultOptions);

        Assert.True(expectedNaN > 1, "the data must repeat NaN, or the test cannot tell the two counts apart");
        Assert.Equal(expectedNaN, buffered[0]!.NanCount);
        AssertSameStatistics(direct[0], buffered[0], "f");

        // NaN is excluded from the bounds, exactly as the full scan excludes it.
        Assert.Equal(0d, which == FloatingPointCase.Double
            ? BitConverter.ToDouble(buffered[0]!.MinValue!, 0) : BitConverter.ToSingle(buffered[0]!.MinValue!, 0));
        Assert.Equal(4d, which == FloatingPointCase.Double
            ? BitConverter.ToDouble(buffered[0]!.MaxValue!, 0) : BitConverter.ToSingle(buffered[0]!.MaxValue!, 0));
    }

    // An all-NaN chunk is the case where the two column orders disagree: TYPE_ORDER omits the bounds,
    // IEEE 754 total order records the NaN as both. The buffered writer has to follow the same option.
    [Theory]
    [InlineData(FloatingPointColumnOrder.TypeDefined)]
    [InlineData(FloatingPointColumnOrder.Ieee754TotalOrder)]
    public async Task AllNaNColumn_FollowsTheFloatingPointColumnOrder(FloatingPointColumnOrder order)
    {
        const int rows = 64;
        var b = new DoubleArray.Builder();
        for (int i = 0; i < rows; i++) b.Append(double.NaN);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("f", DoubleType.Default, nullable: false)).Build();
        var batch = new RecordBatch(schema, [b.Build()], rows);

        var options = DefaultOptions with { FloatingPointOrder = order };
        var (direct, buffered) = await WriteBothAsync($"allnan_{order}", batch, options);

        Assert.Equal(rows, buffered[0]!.NanCount);
        if (order == FloatingPointColumnOrder.TypeDefined)
            Assert.Null(buffered[0]!.MinValue);
        else
            Assert.True(double.IsNaN(BitConverter.ToDouble(buffered[0]!.MinValue!, 0)));

        AssertSameStatistics(direct[0], buffered[0], "f");
    }

    // The DEPRECATED min/max fields carry signed byte ordering, so they are dropped for UTF-8. That rule
    // lives at the tail of WriteColumn, which this entry point does not go through — it has to be applied
    // here too, and dropping it would be invisible to a min_value/max_value assertion.
    [Fact]
    public async Task Utf8DictionaryColumn_OmitsTheDeprecatedMinMax()
    {
        const int rows = 60;
        var b = new StringArray.Builder();
        for (int i = 0; i < rows; i++) b.Append("v" + (i % 3));

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", StringType.Default, nullable: false)).Build();
        var batch = new RecordBatch(schema, [b.Build()], rows);

        var (direct, buffered) = await WriteBothAsync("utf8", batch, DefaultOptions);

        Assert.NotNull(buffered[0]!.MinValue);
        Assert.Null(buffered[0]!.Min);
        Assert.Null(buffered[0]!.Max);
        AssertSameStatistics(direct[0], buffered[0], "s");
    }

    // Statistics OFF means no Statistics at all, not merely no bounds. The dictionary path now computes
    // them, so it is now a path that has something to suppress.
    [Fact]
    public async Task DictionaryColumn_StillHonoursWriteStatisticsFalse()
    {
        const int rows = 60;
        var b = new Int32Array.Builder();
        for (int i = 0; i < rows; i++) b.Append(i % 3);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("n", Int32Type.Default, nullable: false)).Build();
        var batch = new RecordBatch(schema, [b.Build()], rows);

        var (direct, buffered) = await WriteBothAsync(
            "statsoff", batch, DefaultOptions with { WriteStatistics = false });

        Assert.Null(buffered[0]);
        AssertSameStatistics(direct[0], buffered[0], "n");
    }

    // A column that never sees a non-null value builds no dictionary and takes the valueless fallback.
    // That fallback used to hand-build a ColumnMetaData declaring NumValues rows and then write NO PAGES
    // — our own reader rejects the result as truncated — as well as leaving Statistics unset. Both halves
    // are asserted, because statistics on a chunk nothing can read would not be worth much.
    [Fact]
    public async Task AllNullColumn_IsReadable_AndCarriesItsNullCount()
    {
        const int rows = 128;
        var ints = new Int32Array.Builder();
        var doubles = new DoubleArray.Builder();
        var strings = new StringArray.Builder();
        for (int i = 0; i < rows; i++) { ints.AppendNull(); doubles.AppendNull(); strings.AppendNull(); }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("i", Int32Type.Default, nullable: true))
            .Field(new Field("d", DoubleType.Default, nullable: true))
            .Field(new Field("s", StringType.Default, nullable: true))
            .Build();
        var batch = new RecordBatch(schema, [ints.Build(), doubles.Build(), strings.Build()], rows);

        var (direct, buffered) = await WriteBothAsync("allnull", batch, DefaultOptions);

        for (int i = 0; i < buffered.Count; i++)
        {
            Assert.NotNull(buffered[i]);
            Assert.Equal(rows, buffered[i]!.NullCount);
            Assert.Null(buffered[i]!.MinValue);
            AssertSameStatistics(direct[i], buffered[i], schema.FieldsList[i].Name);
        }

        // nan_count is mandatory for FLOAT/DOUBLE even at zero.
        Assert.Equal(0L, buffered[1]!.NanCount);

        await using var file = new LocalRandomAccessFile(TempPath("allnull_buffered.parquet"));
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var read = await reader.ReadRowGroupAsync(0);
        Assert.Equal(rows, read.Length);
        Assert.All(Enumerable.Range(0, read.ColumnCount), c => Assert.Equal(rows, read.Column(c).NullCount));
    }

    private static ParquetWriteOptions DefaultOptions =>
        new() { Compression = CompressionCodec.Uncompressed };

    /// <summary>
    /// Writes the same batch with both writers and returns each file's per-column statistics.
    /// </summary>
    private async Task<(IReadOnlyList<Statistics?> Direct, IReadOnlyList<Statistics?> Buffered)>
        WriteBothAsync(string name, RecordBatch batch, ParquetWriteOptions options)
    {
        string directPath = TempPath(name + "_direct.parquet");
        string bufferedPath = TempPath(name + "_buffered.parquet");

        await using (var file = new LocalSequentialFile(directPath))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using (var file = new LocalSequentialFile(bufferedPath))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false, options))
        {
            await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }

        return (await ReadStatisticsAsync(directPath), await ReadStatisticsAsync(bufferedPath));
    }

    private static async Task<IReadOnlyList<Statistics?>> ReadStatisticsAsync(string path)
    {
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        return [.. metadata.RowGroups[0].Columns.Select(c => c.MetaData!.Statistics)];
    }

    private static void AssertSameStatistics(Statistics? expected, Statistics? actual, string column)
    {
        Assert.True(expected is not null == actual is not null,
            $"column '{column}': statistics present={actual is not null}, expected={expected is not null}");
        if (expected is null || actual is null)
            return;

        Assert.Equal(expected.NullCount, actual.NullCount);
        Assert.Equal(expected.NanCount, actual.NanCount);
        Assert.Equal(expected.DistinctCount, actual.DistinctCount);
        Assert.Equal(expected.IsMinValueExact, actual.IsMinValueExact);
        Assert.Equal(expected.IsMaxValueExact, actual.IsMaxValueExact);
        Assert.Equal(expected.MinValue, actual.MinValue);
        Assert.Equal(expected.MaxValue, actual.MaxValue);
        Assert.Equal(expected.Min, actual.Min);
        Assert.Equal(expected.Max, actual.Max);
    }
}

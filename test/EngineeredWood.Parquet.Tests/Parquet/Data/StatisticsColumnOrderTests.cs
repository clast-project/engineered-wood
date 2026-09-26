// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Tests.Parquet.Interop;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// Column statistics must order values by the column's logical type, not by its physical one (#396).
/// A DECIMAL on FIXED_LEN_BYTE_ARRAY is a signed number, and <c>INT(32|64, false)</c> is unsigned.
/// Both used to be compared the physical way, and the resulting bounds made DataFusion return wrong
/// answers for filters on those columns.
/// </summary>
public class StatisticsColumnOrderTests : IDisposable
{
    private readonly string _tempDir;

    public StatisticsColumnOrderTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-statorder-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    public static TheoryData<bool> Dictionary() => new() { true, false };

    [Theory]
    [MemberData(nameof(Dictionary))]
    public async Task Decimal128_BoundsAreSigned(bool dictionary)
    {
        var builder = new Decimal128Array.Builder(new Decimal128Type(30, 2));
        foreach (decimal value in Repeat(-1.00m, 2.00m, 0.50m))
            builder.Append(value);

        var stats = await WriteAndReadStatisticsAsync(builder.Build(), dictionary);

        Assert.Equal(new BigInteger(-100), SignedBigEndian(stats.MinValue!));
        Assert.Equal(new BigInteger(200), SignedBigEndian(stats.MaxValue!));
    }

    [Theory]
    [MemberData(nameof(Dictionary))]
    public async Task Decimal256_BoundsAreSigned_AcrossTheWholeRange(bool dictionary)
    {
        var builder = new Decimal256Array.Builder(new Decimal256Type(50, 0));
        foreach (decimal value in Repeat(decimal.MinValue, -1m, decimal.MaxValue, 0m))
            builder.Append(value);

        var stats = await WriteAndReadStatisticsAsync(builder.Build(), dictionary);

        Assert.Equal(BigInteger.Parse("-79228162514264337593543950335"), SignedBigEndian(stats.MinValue!));
        Assert.Equal(BigInteger.Parse("79228162514264337593543950335"), SignedBigEndian(stats.MaxValue!));
    }

    /// <summary>
    /// A guard rather than a regression test: unsigned byte order agrees with signed order when every
    /// value has the same sign, so this passed before the fix too.
    /// </summary>
    [Theory]
    [MemberData(nameof(Dictionary))]
    public async Task Decimal_AllNegative(bool dictionary)
    {
        var builder = new Decimal128Array.Builder(new Decimal128Type(30, 2));
        foreach (decimal value in Repeat(-5.00m, -0.01m, -300.00m))
            builder.Append(value);

        var stats = await WriteAndReadStatisticsAsync(builder.Build(), dictionary);

        Assert.Equal(new BigInteger(-30000), SignedBigEndian(stats.MinValue!));
        Assert.Equal(new BigInteger(-1), SignedBigEndian(stats.MaxValue!));
    }

    [Theory]
    [MemberData(nameof(Dictionary))]
    public async Task UInt32_BoundsAreUnsigned(bool dictionary)
    {
        var builder = new UInt32Array.Builder();
        foreach (uint value in Repeat(1u, 3_000_000_000u, 7u))
            builder.Append(value);

        var stats = await WriteAndReadStatisticsAsync(builder.Build(), dictionary);

        Assert.Equal(1u, BitConverter.ToUInt32(stats.MinValue!, 0));
        Assert.Equal(3_000_000_000u, BitConverter.ToUInt32(stats.MaxValue!, 0));
    }

    [Theory]
    [MemberData(nameof(Dictionary))]
    public async Task UInt64_BoundsAreUnsigned(bool dictionary)
    {
        var builder = new UInt64Array.Builder();
        foreach (ulong value in Repeat(1ul, 10_000_000_000_000_000_000ul, 7ul))
            builder.Append(value);

        var stats = await WriteAndReadStatisticsAsync(builder.Build(), dictionary);

        Assert.Equal(1ul, BitConverter.ToUInt64(stats.MinValue!, 0));
        Assert.Equal(10_000_000_000_000_000_000ul, BitConverter.ToUInt64(stats.MaxValue!, 0));
    }

    [Theory]
    [MemberData(nameof(Dictionary))]
    public async Task SignedIntegers_AreUnchanged(bool dictionary)
    {
        var builder = new Int32Array.Builder();
        foreach (int value in Repeat(-5, 3, int.MinValue))
            builder.Append(value);

        var stats = await WriteAndReadStatisticsAsync(builder.Build(), dictionary);

        Assert.Equal(int.MinValue, BitConverter.ToInt32(stats.MinValue!, 0));
        Assert.Equal(3, BitConverter.ToInt32(stats.MaxValue!, 0));
        Assert.Equal(stats.MinValue, stats.Min);
    }

    [Fact]
    public async Task BufferedWriter_UInt32_BoundsAreUnsigned()
    {
        var column = new UInt32Array.Builder().Append(1).Append(3_000_000_000).Append(7).Build();
        string path = Path.Combine(_tempDir, "buffered.parquet");
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false))
        {
            await writer.AppendAsync(Batch(column));
            await writer.CloseAsync();
        }

        var stats = await ReadStatisticsAsync(path);

        Assert.Equal(1u, BitConverter.ToUInt32(stats.MinValue!, 0));
        Assert.Equal(3_000_000_000u, BitConverter.ToUInt32(stats.MaxValue!, 0));
    }

    public static TheoryData<string, long> Predicates() => new()
    {
        { "d = -1.00", 1 },
        { "d > 1", 1 },
        { "d < 0", 1 },
        { "d BETWEEN 0.25 AND 0.75", 1 },
        { "u32 < 5", 1 },
        { "u32 = 3000000000", 1 },
        { "u32 > 2147483647", 1 },
    };

    /// <summary>
    /// The filtered count in DataFusion, which prunes row groups on these statistics, must equal the
    /// count in DuckDB and the true answer. Before the fix DataFusion answered 0 for every decimal
    /// predicate here and refused the unsigned ones.
    /// </summary>
    [SkippableTheory]
    [MemberData(nameof(Predicates))]
    public async Task DataFusion_FiltersAgreeWithDuckDb(string predicate, long expected)
    {
        ExternalParquetReaders.Require();

        var d = new Decimal128Array.Builder(new Decimal128Type(30, 2)).Append(-1.00m).Append(2.00m).Append(0.50m).Build();
        var u32 = new UInt32Array.Builder().Append(1).Append(3_000_000_000).Append(7).Build();
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("d", d.Data.DataType, false))
            .Field(new Field("u32", u32.Data.DataType, false))
            .Build();
        string path = await WriteAsync(new RecordBatch(schema, [d, u32], 3), ParquetWriteOptions.Default);

        var duckDb = ExternalParquetReaders.CountWhere(ExternalParquetReaders.DuckDb, path, predicate);
        var dataFusion = ExternalParquetReaders.CountWhere(ExternalParquetReaders.DataFusion, path, predicate);
        Skip.IfNot(dataFusion.Installed, $"datafusion is not installed: {dataFusion.Error}");

        Assert.True(dataFusion.Count is not null, $"DataFusion refused '{predicate}': {dataFusion.Error}");
        Assert.Equal(expected, dataFusion.Count);
        if (duckDb.Installed)
            Assert.Equal(expected, duckDb.Count);
    }

    // ───── Helpers ─────

    /// <summary>
    /// The values 40 times over, so that a column this small still passes the dictionary encoder's
    /// cardinality threshold when the case asks for a dictionary.
    /// </summary>
    private static IEnumerable<T> Repeat<T>(params T[] values) =>
        Enumerable.Range(0, 40).SelectMany(_ => values);

    private static RecordBatch Batch(IArrowArray column) =>
        new(new Apache.Arrow.Schema.Builder().Field(new Field("c", column.Data.DataType, false)).Build(),
            [column], column.Length);

    private async Task<Statistics> WriteAndReadStatisticsAsync(IArrowArray column, bool dictionary)
    {
        string path = await WriteAsync(Batch(column), ParquetWriteOptions.Default with { DictionaryEnabled = dictionary });
        var stats = await ReadStatisticsAsync(path);

        // Whichever way the dictionary decision went, the check is meaningless if the chunk did not
        // take the path the case names.
        var chunk = await ReadChunkAsync(path);
        Assert.Equal(dictionary, chunk.DictionaryPageOffset is not null);
        return stats;
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

    private static async Task<ColumnMetaData> ReadChunkAsync(string path)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        return metadata.RowGroups[0].Columns[0].MetaData!;
    }

    private static async Task<Statistics> ReadStatisticsAsync(string path) =>
        (await ReadChunkAsync(path)).Statistics!;

    private static BigInteger SignedBigEndian(byte[] bytes)
    {
        var littleEndian = (byte[])bytes.Clone();
        System.Array.Reverse(littleEndian);
        return new BigInteger(littleEndian);
    }
}

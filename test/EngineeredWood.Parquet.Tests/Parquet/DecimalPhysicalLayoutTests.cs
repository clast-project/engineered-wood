// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Tests.Parquet.Interop;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// A <c>Decimal128</c>/<c>Decimal256</c> column's physical type follows its PRECISION, as Spark writes it
/// (#504): INT32 up to 9 digits, INT64 up to 18, then the narrowest FIXED_LEN_BYTE_ARRAY that holds the
/// precision. Before, the Arrow type alone decided, and every Delta decimal was FLBA(16).
/// </summary>
public class DecimalPhysicalLayoutTests : IDisposable
{
    private readonly string _tempDir;

    public DecimalPhysicalLayoutTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-decimal-layout-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    /// <summary>Arrow slot width, precision, and the physical type and FLBA width the precision calls for.</summary>
    public static TheoryData<int, int, PhysicalType, int?> Layouts() => new()
    {
        { 16, 1, PhysicalType.Int32, null },
        { 16, 9, PhysicalType.Int32, null },
        { 16, 10, PhysicalType.Int64, null },
        { 16, 18, PhysicalType.Int64, null },
        { 16, 19, PhysicalType.FixedLenByteArray, 9 },
        { 16, 28, PhysicalType.FixedLenByteArray, 12 },
        { 16, 38, PhysicalType.FixedLenByteArray, 16 },
        { 32, 9, PhysicalType.Int32, null },
        { 32, 18, PhysicalType.Int64, null },
        { 32, 38, PhysicalType.FixedLenByteArray, 16 },
        { 32, 39, PhysicalType.FixedLenByteArray, 17 },
        { 32, 76, PhysicalType.FixedLenByteArray, 32 },
    };

    /// <summary>Every layout through both writers, with few distinct values (the dictionary is kept) and many.</summary>
    public static TheoryData<int, int, PhysicalType, int?, string, bool> LayoutsByWriter()
    {
        var data = new TheoryData<int, int, PhysicalType, int?, string, bool>();
        foreach (var row in Layouts())
        foreach (string writer in new[] { "file", "buffered" })
        foreach (bool dictionary in new[] { true, false })
            data.Add((int)row[0], (int)row[1], (PhysicalType)row[2], (int?)row[3], writer, dictionary);
        return data;
    }

    [Theory]
    [MemberData(nameof(LayoutsByWriter))]
    public async Task PhysicalType_FollowsPrecision_AndValuesRoundTrip(
        int slot, int precision, PhysicalType physicalType, int? typeLength, string writer, bool dictionary)
    {
        var values = Values(precision, dictionary, rows: 600);
        var column = Decimals(slot, precision, scale: 2, values);
        string path = await WriteAsync(writer, column, nullable: true,
            dictionary ? ParquetWriteOptions.Default : ParquetWriteOptions.Default with { DictionaryEnabled = false });

        var (metadata, _) = ReadFooter(path);
        var element = metadata.Schema.Single(e => e.Name == "d");
        Assert.Equal(physicalType, element.Type);
        Assert.Equal(typeLength, element.TypeLength);
        Assert.Equal(precision, element.Precision);
        Assert.Equal(2, element.Scale);
        Assert.Equal(dictionary, metadata.RowGroups[0].Columns[0].MetaData!.DictionaryPageOffset is not null);

        Assert.Equal(values, await ReadUnscaledAsync(path));
    }

    /// <summary>
    /// A sliced input is narrowed from its offset: the narrowed array starts at 0, and the validity bitmap has
    /// to move with it.
    /// </summary>
    [Theory]
    [InlineData("file", 9)]
    [InlineData("file", 18)]
    [InlineData("file", 20)]
    [InlineData("buffered", 9)]
    [InlineData("buffered", 18)]
    [InlineData("buffered", 20)]
    public async Task SlicedInput_IsNarrowedFromItsOffset(string writer, int precision)
    {
        var values = Values(precision, dictionary: false, rows: 200);
        var full = Decimals(16, precision, scale: 2, values);
        var sliced = ArrowArrayFactory.Slice(full, 3, 150);

        string path = await WriteAsync(writer, sliced, nullable: true, ParquetWriteOptions.Default);

        Assert.Equal(values.Skip(3).Take(150).ToArray(), await ReadUnscaledAsync(path));
    }

    /// <summary>
    /// The bounds compare as signed numbers in every layout: INT32/INT64 as integers, FLBA as signed
    /// big-endian bytes (#396). With mixed signs an unsigned order would invert them.
    /// </summary>
    [Theory]
    [InlineData("file", 9)]
    [InlineData("file", 18)]
    [InlineData("file", 20)]
    [InlineData("buffered", 9)]
    [InlineData("buffered", 18)]
    [InlineData("buffered", 20)]
    public async Task Statistics_AndPageIndex_AreSignedOnMixedSigns(string writer, int precision)
    {
        var values = Values(precision, dictionary: false, rows: 600);
        var max = BigInteger.Pow(10, precision) - 1;
        string path = await WriteAsync(writer, Decimals(16, precision, scale: 2, values), nullable: true,
            ParquetWriteOptions.Default with { WritePageIndex = true });

        var (metadata, bytes) = ReadFooter(path);
        var element = metadata.Schema.Single(e => e.Name == "d");
        var chunk = metadata.RowGroups[0].Columns[0];
        var stats = chunk.MetaData!.Statistics!;
        Assert.Equal(-max, Decode(stats.MinValue!, element.Type!.Value));
        Assert.Equal(max, Decode(stats.MaxValue!, element.Type!.Value));

        var index = MetadataDecoder.DecodeColumnIndex(
            bytes.AsSpan(checked((int)chunk.ColumnIndexOffset!.Value), chunk.ColumnIndexLength!.Value));
        var mins = index.MinValues.Where((_, p) => !index.NullPages[p]).Select(v => Decode(v, element.Type!.Value));
        var maxes = index.MaxValues.Where((_, p) => !index.NullPages[p]).Select(v => Decode(v, element.Type!.Value));
        Assert.Equal(-max, mins.Min());
        Assert.Equal(max, maxes.Max());
    }

    /// <summary>
    /// A value past its declared precision does not fit the narrowed layout; writing its low bytes would store
    /// a different number, so the write is refused.
    /// </summary>
    [Theory]
    [InlineData("file", 9)]
    [InlineData("file", 18)]
    [InlineData("file", 20)]
    [InlineData("buffered", 9)]
    [InlineData("buffered", 18)]
    [InlineData("buffered", 20)]
    public async Task ValuePastItsPrecision_IsRefused(string writer, int precision)
    {
        // 2^127 - 1 overflows every narrowed layout.
        BigInteger?[] values = [1, (BigInteger.One << 127) - 1];
        var column = Decimals(16, precision, scale: 2, values);

        var ex = await Record.ExceptionAsync(
            () => WriteAsync(writer, column, nullable: true, ParquetWriteOptions.Default));

        Assert.IsType<ArgumentException>(Unwrap(ex));
    }

    /// <summary>The bytes under a null are arbitrary, so they must not be checked against the layout.</summary>
    [Theory]
    [InlineData("file")]
    [InlineData("buffered")]
    public async Task GarbageUnderANull_IsIgnored(string writer)
    {
        BigInteger?[] values = [1, null, -1];
        var column = Decimals(16, 9, scale: 2, values, nullFill: (BigInteger.One << 127) - 1);

        string path = await WriteAsync(writer, column, nullable: true, ParquetWriteOptions.Default);

        Assert.Equal(values, await ReadUnscaledAsync(path));
    }

    /// <summary>
    /// DuckDB and DataFusion (arrow-rs) decode each new layout and prune on its bounds: a count that comes back
    /// short means a reader misread the values or skipped a row group on wrong statistics. The edges
    /// (±(10^p - 1)) catch a narrowing that drops the top bytes.
    /// </summary>
    [SkippableTheory]
    [InlineData(ExternalParquetReaders.DuckDb, "file", 9)]
    [InlineData(ExternalParquetReaders.DuckDb, "file", 18)]
    [InlineData(ExternalParquetReaders.DuckDb, "file", 20)]
    [InlineData(ExternalParquetReaders.DuckDb, "buffered", 9)]
    [InlineData(ExternalParquetReaders.DuckDb, "buffered", 18)]
    [InlineData(ExternalParquetReaders.DuckDb, "buffered", 20)]
    [InlineData(ExternalParquetReaders.DataFusion, "file", 9)]
    [InlineData(ExternalParquetReaders.DataFusion, "file", 18)]
    [InlineData(ExternalParquetReaders.DataFusion, "file", 20)]
    [InlineData(ExternalParquetReaders.DataFusion, "buffered", 9)]
    [InlineData(ExternalParquetReaders.DataFusion, "buffered", 18)]
    [InlineData(ExternalParquetReaders.DataFusion, "buffered", 20)]
    public async Task ExternalReaders_DecodeAndPruneEachLayout(string reader, string writer, int precision)
    {
        ExternalParquetReaders.Require();

        var values = Values(precision, dictionary: false, rows: 600);
        string path = await WriteAsync(writer, Decimals(16, precision, scale: 2, values), nullable: true,
            ParquetWriteOptions.Default);

        var max = BigInteger.Pow(10, precision) - 1;
        string Literal(BigInteger unscaled) => $"CAST('{Text(unscaled)}' AS DECIMAL({precision}, 2))";
        var queries = new (string Predicate, Func<BigInteger?, bool> Match)[]
        {
            ("d < 0", v => v < 0),
            ("d IS NULL", v => v is null),
            ($"d = {Literal(-1)}", v => v == -1),
            ($"d = {Literal(max)}", v => v == max),
            ($"d = {Literal(-max)}", v => v == -max),
        };

        foreach (var (predicate, match) in queries)
        {
            var outcome = ExternalParquetReaders.CountWhere(reader, path, predicate);
            Skip.IfNot(outcome.Installed, $"{reader} is not installed: {outcome.Error}");
            Assert.True(outcome.Count is not null, $"{reader} refused '{predicate}': {outcome.Error}");
            Assert.Equal((long)values.Count(match), outcome.Count);
        }
    }

    // ───── Helpers ─────

    // An unscaled value at scale 2 as decimal text.
    private static string Text(BigInteger unscaled)
    {
        string digits = BigInteger.Abs(unscaled).ToString().PadLeft(3, '0');
        return (unscaled.Sign < 0 ? "-" : "") + digits.Substring(0, digits.Length - 2) + "." + digits.Substring(digits.Length - 2);
    }

    /// <summary>
    /// The edges (0, ±1, ±(10^p - 1)) and nulls, then either a few values repeated or a run of distinct ones.
    /// </summary>
    private static BigInteger?[] Values(int precision, bool dictionary, int rows)
    {
        var max = BigInteger.Pow(10, precision) - 1;
        var values = new BigInteger?[rows];
        BigInteger?[] edges = [0, 1, -1, max, -max, null];
        for (int i = 0; i < rows; i++)
        {
            if (i < edges.Length)
                values[i] = edges[i];
            else if (i % 11 == 5)
                values[i] = null;
            else
                values[i] = dictionary ? (i % 4) - 2 : (i - rows / 2) * (max / rows);
        }
        return values;
    }

    private static IArrowArray Decimals(
        int slot, int precision, int scale, BigInteger?[] unscaled, BigInteger? nullFill = null)
    {
        var bytes = new byte[unscaled.Length * slot];
        var validity = new ArrowBuffer.BitmapBuilder();
        for (int i = 0; i < unscaled.Length; i++)
        {
            validity.Append(unscaled[i] is not null);
            var value = unscaled[i] ?? nullFill ?? BigInteger.Zero;
            var dest = bytes.AsSpan(i * slot, slot);
            dest.Fill(value.Sign < 0 ? (byte)0xFF : (byte)0x00);
            value.ToByteArray().CopyTo(dest);
        }

        IArrowType type = slot == 16 ? new Decimal128Type(precision, scale) : new Decimal256Type(precision, scale);
        return ArrowArrayFactory.BuildArray(new ArrayData(type, unscaled.Length,
            unscaled.Count(v => v is null), 0, [validity.Build(), new ArrowBuffer(bytes)]));
    }

    private async Task<string> WriteAsync(string writer, IArrowArray column, bool nullable, ParquetWriteOptions options)
    {
        var batch = new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("d", column.Data.DataType, nullable)).Build(),
            [column], column.Length);
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await using var file = new LocalSequentialFile(path);
        if (writer == "file")
        {
            await using var w = new ParquetFileWriter(file, ownsFile: false, options);
            await w.WriteRowGroupAsync(batch);
            await w.CloseAsync();
        }
        else
        {
            await using var w = new BufferedParquetWriter(file, ownsFile: false, options);
            await w.AppendAsync(batch);
            await w.CloseAsync();
        }
        return path;
    }

    /// <summary>Reads column "d" back as unscaled values, whatever decimal width the reader chose.</summary>
    private static async Task<BigInteger?[]> ReadUnscaledAsync(string path)
    {
        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false);
        var result = new List<BigInteger?>();
        await foreach (var batch in reader.ReadAllAsync())
        {
            var column = batch.Column(0);
            int slot = ((FixedSizeBinaryType)column.Data.DataType).ByteWidth;
            var buffer = column.Data.Buffers[1].Span;
            for (int i = 0; i < column.Length; i++)
            {
                result.Add(column.IsNull(i)
                    ? null
                    : new BigInteger(buffer.Slice((column.Data.Offset + i) * slot, slot).ToArray()));
            }
        }
        return result.ToArray();
    }

    private static BigInteger Decode(byte[] bound, PhysicalType physicalType) => physicalType switch
    {
        PhysicalType.Int32 => BinaryPrimitives.ReadInt32LittleEndian(bound),
        PhysicalType.Int64 => BinaryPrimitives.ReadInt64LittleEndian(bound),
        _ => new BigInteger(Enumerable.Reverse(bound).ToArray()),
    };

    private static (FileMetaData Metadata, byte[] Bytes) ReadFooter(string path)
    {
        byte[] bytes = File.ReadAllBytes(path);
        int footerLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
        return (MetadataDecoder.DecodeFileMetaData(bytes.AsSpan(bytes.Length - 8 - footerLength, footerLength)), bytes);
    }

    private static Exception? Unwrap(Exception? ex) =>
        ex is AggregateException { InnerExceptions.Count: 1 } agg ? Unwrap(agg.InnerException) : ex;
}

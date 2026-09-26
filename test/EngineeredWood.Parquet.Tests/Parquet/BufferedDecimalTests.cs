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
/// <see cref="BufferedParquetWriter"/> must write decimals big-endian, as Parquet stores them (#400).
/// <c>Decimal128Type</c> and <c>Decimal256Type</c> derive from <c>FixedSizeBinaryType</c>, so they
/// reached the dictionary page as Arrow's little-endian bytes whenever the dictionary was kept: every
/// low-cardinality decimal column came back as garbage in every reader.
/// </summary>
public class BufferedDecimalTests : IDisposable
{
    private readonly string _tempDir;

    public BufferedDecimalTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-buffered-decimal-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    /// <summary>
    /// Each type with few distinct values (the dictionary is kept) and many (the writer falls back to
    /// re-encoding the column without one, a separate path through the same dictionary bytes).
    /// </summary>
    public static TheoryData<string, bool, bool> Cases()
    {
        var data = new TheoryData<string, bool, bool>();
        foreach (string type in new[] { "decimal128", "decimal256" })
        foreach (bool dictionaryKept in new[] { true, false })
        foreach (bool nullable in new[] { true, false })
            data.Add(type, dictionaryKept, nullable);
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Values_RoundTrip(string type, bool dictionaryKept, bool nullable)
    {
        var batch = Batch(type, dictionaryKept, nullable, out var expected);
        string path = await WriteBufferedAsync(batch);

        Assert.Equal(dictionaryKept, ReadChunk(path).DictionaryPageOffset is not null);

        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var column = (await reader.ReadRowGroupAsync(0)).Column(0);
        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], ValueAt(column, i));
    }

    /// <summary>
    /// The dictionary page holds big-endian values: the bytes of -1.00 at scale 2 (-100) start 0xFF
    /// and end 0x9C. Little-endian, they would start 0x9C.
    /// </summary>
    [Fact]
    public async Task DictionaryPage_IsBigEndian()
    {
        var builder = new Decimal128Array.Builder(new Decimal128Type(20, 2));
        for (int i = 0; i < 100; i++)
            builder.Append(-1.00m);
        // Uncompressed, so the page payload can be read as it stands.
        string path = await WriteBufferedAsync(Batch(builder.Build(), nullable: false),
            ParquetWriteOptions.Default with { Compression = EngineeredWood.Compression.CompressionCodec.Uncompressed });

        var chunk = ReadChunk(path);
        byte[] bytes = File.ReadAllBytes(path);
        var header = EngineeredWood.Parquet.Data.PageHeaderDecoder.Decode(
            bytes.AsSpan(checked((int)chunk.DictionaryPageOffset!.Value)), out int headerLength);
        Assert.Equal(EngineeredWood.Parquet.PageType.DictionaryPage, header.Type);
        var entry = bytes.AsSpan(checked((int)chunk.DictionaryPageOffset.Value) + headerLength, 16);

        Assert.Equal(0xFF, entry[0]);
        Assert.Equal(0x9C, entry[15]);
    }

    /// <summary>
    /// Two independent decoders must read the buffered file exactly as they read the same data
    /// written by <see cref="ParquetFileWriter"/>, which reverses decimal bytes on its own path.
    /// </summary>
    [SkippableTheory]
    [InlineData(ExternalParquetReaders.DuckDb)]
    [InlineData(ExternalParquetReaders.DataFusion)]
    public async Task ExternalReaders_DecodeTheSameValuesAsTheFileWriter(string reader)
    {
        ExternalParquetReaders.Require();

        var batch = Batch("decimal128", dictionaryKept: true, nullable: true, out _);
        string buffered = await WriteBufferedAsync(batch);
        string control = Path.Combine(_tempDir, "control.parquet");
        await using (var file = new LocalSequentialFile(control))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        var fromControl = ExternalParquetReaders.Read(reader, control);
        Skip.IfNot(fromControl.Installed, $"{reader} is not installed: {fromControl.Error}");
        var fromBuffered = ExternalParquetReaders.Read(reader, buffered);

        Assert.True(fromControl.Result is not null, fromControl.Error);
        Assert.True(fromBuffered.Result is not null, fromBuffered.Error);
        Assert.Equal(fromControl.Result!.Columns, fromBuffered.Result!.Columns);
    }

    /// <summary>
    /// With the page big-endian, the dictionary-entry statistics order the values as signed numbers
    /// (#396): -123456.78 is the minimum and 2.00 the maximum, where unsigned byte order would put
    /// every negative value above every positive one.
    /// </summary>
    [Theory]
    [InlineData("decimal128")]
    [InlineData("decimal256")]
    public async Task DictionaryStatistics_AreSigned(string type)
    {
        string path = await WriteBufferedAsync(Batch(type, dictionaryKept: true, nullable: true, out _));

        var chunk = ReadChunk(path);
        Assert.NotNull(chunk.DictionaryPageOffset);
        Assert.Equal(new BigInteger(-12345678), SignedBigEndian(chunk.Statistics!.MinValue!));
        Assert.Equal(new BigInteger(200), SignedBigEndian(chunk.Statistics.MaxValue!));
    }

    /// <summary>
    /// DataFusion prunes row groups on those statistics; with bounds inverted it returned no rows for
    /// predicates that match. DuckDB is the control.
    /// </summary>
    [SkippableTheory]
    [InlineData("d = -1.00")]
    [InlineData("d < 0")]
    [InlineData("d > 1")]
    [InlineData("d = -123456.78")]
    public async Task DataFusion_FiltersOnBufferedDecimals(string predicate)
    {
        ExternalParquetReaders.Require();

        string path = await WriteBufferedAsync(Batch("decimal128", dictionaryKept: true, nullable: true, out var values));
        long expected = CountMatching(values, predicate);
        Assert.True(expected > 0, "the predicate must match something, or a pruning reader cannot get it wrong");

        var dataFusion = ExternalParquetReaders.CountWhere(ExternalParquetReaders.DataFusion, path, predicate);
        Skip.IfNot(dataFusion.Installed, $"datafusion is not installed: {dataFusion.Error}");
        Assert.True(dataFusion.Count is not null, $"DataFusion refused '{predicate}': {dataFusion.Error}");
        Assert.Equal(expected, dataFusion.Count);

        var duckDb = ExternalParquetReaders.CountWhere(ExternalParquetReaders.DuckDb, path, predicate);
        if (duckDb.Installed)
            Assert.Equal(expected, duckDb.Count);
    }

    // ───── Helpers ─────

    /// <summary>The true answer, computed from the values written, so the expectations cannot drift.</summary>
    private static long CountMatching(decimal?[] values, string predicate) => predicate switch
    {
        "d = -1.00" => values.Count(v => v == -1.00m),
        "d < 0" => values.Count(v => v < 0),
        "d > 1" => values.Count(v => v > 1),
        "d = -123456.78" => values.Count(v => v == -123456.78m),
        _ => throw new ArgumentOutOfRangeException(nameof(predicate)),
    };

    private static BigInteger SignedBigEndian(byte[] bytes)
    {
        var littleEndian = (byte[])bytes.Clone();
        System.Array.Reverse(littleEndian);
        return new BigInteger(littleEndian);
    }

    private static RecordBatch Batch(string type, bool dictionaryKept, bool nullable, out decimal?[] expected)
    {
        const int rows = 600;
        decimal[] few = [-1.00m, 2.00m, 0.50m, -123456.78m, 0m];
        expected = new decimal?[rows];
        for (int i = 0; i < rows; i++)
        {
            if (nullable && i % 7 == 3) continue;
            expected[i] = dictionaryKept ? few[i % few.Length] : (i - 300) * 1.25m;
        }

        IArrowArray column;
        if (type == "decimal128")
        {
            var builder = new Decimal128Array.Builder(new Decimal128Type(20, 2));
            foreach (var value in expected)
            {
                if (value is { } v) builder.Append(v);
                else builder.AppendNull();
            }

            column = builder.Build();
        }
        else
        {
            var builder = new Decimal256Array.Builder(new Decimal256Type(40, 2));
            foreach (var value in expected)
            {
                if (value is { } v) builder.Append(v);
                else builder.AppendNull();
            }

            column = builder.Build();
        }

        return Batch(column, nullable);
    }

    private static RecordBatch Batch(IArrowArray column, bool nullable) =>
        new(new Apache.Arrow.Schema.Builder().Field(new Field("d", column.Data.DataType, nullable)).Build(),
            [column], column.Length);

    private static decimal? ValueAt(IArrowArray column, int index) => column switch
    {
        Decimal128Array d => d.GetValue(index),
        Decimal256Array d => d.GetValue(index),
        _ => throw new NotSupportedException(column.GetType().Name),
    };

    private async Task<string> WriteBufferedAsync(RecordBatch batch, ParquetWriteOptions? options = null)
    {
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new BufferedParquetWriter(file, ownsFile: false, options);
        await writer.AppendAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private static ColumnMetaData ReadChunk(string path)
    {
        byte[] bytes = File.ReadAllBytes(path);
        int footerLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
        var metadata = MetadataDecoder.DecodeFileMetaData(bytes.AsSpan(bytes.Length - 8 - footerLength, footerLength));
        return metadata.RowGroups[0].Columns[0].MetaData!;
    }
}

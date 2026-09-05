// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#pragma warning disable EWPARQUET0004 // These tests exist to exercise the experimental carrier.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// Writing TIMESTAMP on FIXED_LEN_BYTE_ARRAY(12), opted into per column.
///
/// The promotion is never automatic and cannot be: an Arrow timestamp is int64, so anything Arrow can
/// hold already fits INT64 with room to spare. What this is for is producing files in that shape.
///
/// That also bounds what these tests can prove. The upstream fixture's <c>timestamp_nanos</c> column is
/// interesting precisely because two of its rows need more than 64 bits — and those rows cannot be
/// expressed in Arrow at all, so this writer cannot produce that column. The MILLIS and MICROS columns
/// are fully reproducible, and are checked byte for byte below.
/// </summary>
public sealed class ExtendedTimestampWriteTests : IDisposable
{
    private readonly string _tempDir;

    public ExtendedTimestampWriteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-ts-write-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    // The six timestamps of apache/parquet-testing#123, as epoch seconds.
    private static readonly long[] EpochSeconds =
        [0, 1, -1, 9_223_372_036, 253_402_300_799, -62_135_596_800];

    private static long ScaleOf(TimeUnit unit) => unit switch
    {
        TimeUnit.Millisecond => 1_000L,
        TimeUnit.Microsecond => 1_000_000L,
        _ => 1_000_000_000L,
    };

    private static RecordBatch Batch(TimeUnit unit, string name, params long[] values)
    {
        var type = new TimestampType(unit, "UTC");
        var buffer = new ArrowBuffer.Builder<long>();
        foreach (long v in values)
        {
            buffer.Append(v);
        }

        var validity = new byte[(values.Length + 7) / 8];
        for (int i = 0; i < values.Length; i++)
        {
            validity[i / 8] |= (byte)(1 << (i % 8));
        }

        var array = new TimestampArray(
            new ArrayData(type, values.Length, 0, 0, [new ArrowBuffer(validity), buffer.Build()]));
        var schema = new Apache.Arrow.Schema([new Field(name, type, nullable: false)], null);
        return new RecordBatch(schema, [array], values.Length);
    }

    private ParquetWriteOptions Promote(string column) => new()
    {
        ExtendedTimestampColumns = [column],
    };

    private async Task<string> WriteAsync(RecordBatch batch, ParquetWriteOptions options, bool buffered = false)
    {
        var path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");

        await using var file = new LocalSequentialFile(path);
        if (buffered)
        {
            await using var writer = new BufferedParquetWriter(file, options: options);
            await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }
        else
        {
            await using var writer = new ParquetFileWriter(file, options: options);
            await writer.WriteRowGroupAsync(batch);
        }

        return path;
    }

    private static async Task<byte[][]> RawValuesAsync(string path, string column, int count)
    {
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(
            file,
            ownsFile: false,
            new ParquetReadOptions
            {
                ExtendedTimestampOutput = ExtendedTimestampOutputKind.FixedSizeBinary,
            });

        var batch = await reader.ReadRowGroupAsync(0, [column]);
        var array = Assert.IsType<FixedSizeBinaryArray>(batch.Column(0));
        Assert.Equal(count, array.Length);

        var result = new byte[count][];
        for (int i = 0; i < count; i++)
        {
            result[i] = array.GetBytes(i).ToArray();
        }

        return result;
    }

    [Theory]
    [InlineData(TimeUnit.Millisecond)]
    [InlineData(TimeUnit.Microsecond)]
    public async Task ReproducesTheFixtureEncodingExactly(TimeUnit unit)
    {
        // Every one of these byte sequences was confirmed to appear verbatim in
        // flba12_timestamp.parquet. Writing the same timestamps must produce the same bytes, or this
        // library is not writing the format the proposal describes.
        long scale = ScaleOf(unit);
        long[] values = [.. EpochSeconds.Select(s => s * scale)];

        var path = await WriteAsync(Batch(unit, "t", values), Promote("t"));
        var raw = await RawValuesAsync(path, "t", values.Length);

        for (int i = 0; i < values.Length; i++)
        {
            var expected = new byte[ExtendedTimestamp.ByteWidth];
            ExtendedTimestamp.Write((Int128)values[i], expected);
            Assert.Equal(expected, raw[i]);
        }
    }

    [Fact]
    public async Task TheFooterDeclaresTheCarrierAndSuppressesConvertedType()
    {
        var path = await WriteAsync(Batch(TimeUnit.Millisecond, "t", 0L, 1_000L), Promote("t"));

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var element = metadata.Schema.First(e => e.Name == "t");

        Assert.Equal(PhysicalType.FixedLenByteArray, element.Type);
        Assert.Equal(ExtendedTimestamp.ByteWidth, element.TypeLength);
        var logical = Assert.IsType<LogicalType.TimestampType>(element.LogicalType);
        Assert.Equal(EngineeredWood.Parquet.Metadata.TimeUnit.Millis, logical.Unit);
        Assert.True(logical.IsAdjustedToUtc);

        // TIMESTAMP_MILLIS is defined for INT64 only. A reader that understands converted types but not
        // this carrier would decode twelve bytes as eight, so the field must be absent entirely.
        Assert.Null(element.ConvertedType);
    }

    [Fact]
    public async Task AnUnpromotedColumnStillWritesAsInt64()
    {
        var path = await WriteAsync(
            Batch(TimeUnit.Millisecond, "t", 0L, 1_000L), new ParquetWriteOptions());

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var element = metadata.Schema.First(e => e.Name == "t");

        Assert.Equal(PhysicalType.Int64, element.Type);
        Assert.Equal(ConvertedType.TimestampMillis, element.ConvertedType);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RoundTripsThroughBothWriters(bool buffered)
    {
        // The buffered writer is an independent implementation and has drifted from the streaming one
        // before. Both must produce a column that reads back as the values that went in.
        long[] values = [.. EpochSeconds.Select(s => s * 1_000_000L)];

        var path = await WriteAsync(Batch(TimeUnit.Microsecond, "t", values), Promote("t"), buffered);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(
            file,
            ownsFile: false,
            new ParquetReadOptions { ExtendedTimestampOutput = ExtendedTimestampOutputKind.Timestamp });
        var batch = await reader.ReadRowGroupAsync(0, ["t"]);
        var array = Assert.IsType<TimestampArray>(batch.Column(0));

        Assert.Equal(TimeUnit.Microsecond, ((TimestampType)array.Data.DataType).Unit);
        Assert.Equal(values, array.Values.ToArray());
    }

    [Fact]
    public async Task BothWritersProduceTheSameBytes()
    {
        long[] values = [.. EpochSeconds.Select(s => s * 1_000L)];
        var options = Promote("t");

        var streaming = await RawValuesAsync(
            await WriteAsync(Batch(TimeUnit.Millisecond, "t", values), options), "t", values.Length);
        var buffered = await RawValuesAsync(
            await WriteAsync(Batch(TimeUnit.Millisecond, "t", values), options, buffered: true),
            "t", values.Length);

        Assert.Equal(streaming, buffered);
    }

    [Fact]
    public async Task StatisticsUseTheSignedComparator()
    {
        // -1 encodes as all-0xFF and would be the MAXIMUM under the lexicographic comparator every other
        // FIXED_LEN_BYTE_ARRAY column uses.
        long[] values = [1_000L, -1_000L, 0L];

        var path = await WriteAsync(Batch(TimeUnit.Millisecond, "t", values), Promote("t"));

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var accessor = new ParquetStatisticsAccessor(await reader.GetSchemaAsync());

        var min = accessor.GetMinValue(metadata.RowGroups[0], "t")!.Value.AsDateTimeOffset;
        var max = accessor.GetMaxValue(metadata.RowGroups[0], "t")!.Value.AsDateTimeOffset;

        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(-1_000), min);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1_000), max);
    }

    [Fact]
    public async Task TheDeprecatedBoundsAreOmitted()
    {
        // Statistics.min/max promise SIGNED ordering over the bytes as compared. These bytes are
        // little-endian, so no reader could reproduce that ordering from them; the pair has to be absent.
        var path = await WriteAsync(
            Batch(TimeUnit.Millisecond, "t", 1_000L, -1_000L, 0L), Promote("t"));

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var stats = metadata.RowGroups[0].Columns[0].MetaData!.Statistics!;

        Assert.Null(stats.Min);
        Assert.Null(stats.Max);
        Assert.NotNull(stats.MinValue);
        Assert.NotNull(stats.MaxValue);
    }

    [Fact]
    public async Task ANestedColumnIsRefusedRatherThanHalfPromoted()
    {
        // The schema is built by ArrowToSchemaConverter and the physical type the data is written with is
        // decided by NestedLevelWriter, which does not see these options. Honouring a nested request would
        // put FIXED_LEN_BYTE_ARRAY(12) in the footer over pages holding INT64.
        var inner = new Field("t", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: false);
        var structType = new StructType([inner]);
        var children = new TimestampArray(new ArrayData(
            inner.DataType, 1, 0, 0,
            [new ArrowBuffer(new byte[] { 0x01 }), new ArrowBuffer(BitConverter.GetBytes(0L))]));
        var structArray = new StructArray(structType, 1, [children], ArrowBuffer.Empty);
        var schema = new Apache.Arrow.Schema([new Field("s", structType, nullable: false)], null);
        var batch = new RecordBatch(schema, [structArray], 1);

        var error = await Assert.ThrowsAsync<NotSupportedException>(
            async () => await WriteAsync(batch, new ParquetWriteOptions
            {
                ExtendedTimestampColumns = ["s.t"],
            }));

        Assert.Contains("nested", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ANonTimestampColumnIsRefused()
    {
        var type = Int64Type.Default;
        var array = new Int64Array(new ArrayData(
            type, 1, 0, 0, [new ArrowBuffer(new byte[] { 0x01 }), new ArrowBuffer(BitConverter.GetBytes(1L))]));
        var schema = new Apache.Arrow.Schema([new Field("n", type, nullable: false)], null);

        var error = await Assert.ThrowsAsync<NotSupportedException>(
            async () => await WriteAsync(
                new RecordBatch(schema, [array], 1),
                new ParquetWriteOptions { ExtendedTimestampColumns = ["n"] }));

        Assert.Contains("not a timestamp", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ASlicedBatchKeepsItsValuesAndItsNullsThroughTheBufferedWriter()
    {
        // The buffered writer takes sliced arrays as they come -- it tracks Data.Offset rather than
        // compacting, unlike ParquetFileWriter, which calls CompactSlicedColumns first. So the carrier
        // encoding has to honour the offset on BOTH buffers, and getting it wrong here moved the values
        // AND the nulls while leaving a perfectly well-formed file behind.
        var type = new TimestampType(TimeUnit.Microsecond, "UTC");
        var values = new ArrowBuffer.Builder<long>();
        foreach (long v in new[] { 10L, 20L, 30L, 40L, 50L })
        {
            values.Append(v);
        }

        // Valid at absolute rows 1, 2 and 4. The slice starts at row 2, so it reads: 30, null, 50.
        var sliced = new TimestampArray(new ArrayData(
            type, length: 3, nullCount: 1, offset: 2,
            [new ArrowBuffer(new byte[] { 0b00010110 }), values.Build()]));

        var schema = new Apache.Arrow.Schema([new Field("t", type, nullable: true)], null);
        var path = await WriteAsync(new RecordBatch(schema, [sliced], 3), Promote("t"), buffered: true);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(
            file,
            ownsFile: false,
            new ParquetReadOptions { ExtendedTimestampOutput = ExtendedTimestampOutputKind.Timestamp });
        var array = Assert.IsType<TimestampArray>((await reader.ReadRowGroupAsync(0, ["t"])).Column(0));

        Assert.Equal(3, array.Length);
        Assert.False(array.IsNull(0));
        Assert.Equal(30L, array.Values[0]);
        Assert.True(array.IsNull(1));
        Assert.False(array.IsNull(2));
        Assert.Equal(50L, array.Values[2]);
    }
}

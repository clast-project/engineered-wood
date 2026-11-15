// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Tests.Parquet.Interop;

#pragma warning disable EWPARQUET0003 // FSST, an evaluation encoding

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// A string column's data buffer is sized from what the chunk says it decodes to, not doubled as values arrive: a
/// whole group's read allocates about its decoded bytes. Doubling from 32 bytes a value left 33-byte values in a
/// buffer of 64, a read peaking at 1.84 times its decoded bytes (#888).
/// </summary>
[Collection(NativeMemoryCollection.Name)]
public sealed class StringBufferGrowthTests : IDisposable
{
    private const int Rows = 150_000;
    private const int Length = 33; // just past the 32 bytes a value the buffer starts at: doubling's worst case

    // DELTA_LENGTH_BYTE_ARRAY chunks are sized by a bound, their uncompressed bytes: lengths and headers too.
    private const double Slack = 1.15;

    private readonly string _tempDir =
        Path.Combine(Path.GetTempPath(), "ew-string-growth-" + Guid.NewGuid().ToString("N")[..8]);

    public StringBufferGrowthTests() => Directory.CreateDirectory(_tempDir);

    public void Dispose()
    {
        try
        {
            Directory.Delete(_tempDir, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    [Theory]
    [InlineData(ByteArrayOutputKind.Default)]
    [InlineData(ByteArrayOutputKind.LargeOffsets)]
    [InlineData(ByteArrayOutputKind.ViewType)]
    public async Task AChunkOfWholeStrings_IsSizedByItsUncompressedBytes(ByteArrayOutputKind output)
    {
        string path = await WriteAsync("delta-length");
        await AssertReadsWithinAsync(path, output, Slack);
    }

    [Theory]
    [InlineData("delta", ByteArrayOutputKind.Default)]
    [InlineData("delta", ByteArrayOutputKind.ViewType)]
    [InlineData("fsst", ByteArrayOutputKind.Default)]
    [InlineData("dictionary", ByteArrayOutputKind.Default)]
    public async Task AChunkThatRecordsItsStringBytes_IsSizedByThem(string encoding, ByteArrayOutputKind output)
    {
        string path = await WriteAsync(encoding);
        RecordStringBytes(path, (long)Rows * Length);
        await AssertReadsWithinAsync(path, output, Slack);
    }

    [Fact]
    public async Task AChunkOfShortStrings_NeverAllocatesTheStartingGuess()
    {
        // 32 bytes a value would be 4.8 MB for these one-byte values; the chunk's bound is a few hundred KB. The read
        // starts at the bound rather than allocating the guess and replacing it (#889 review).
        var b = new StringArray.Builder();
        for (int i = 0; i < Rows; i++)
            b.Append(((char)('a' + i % 26)).ToString());
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", StringType.Default, false)).Build();
        string path = Path.Combine(_tempDir, "short.parquet");
        using (var batch = new RecordBatch(schema, [b.Build()], Rows))
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(
                         file, ownsFile: false, new ParquetWriteOptions { DictionaryEnabled = false }))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false);
        await reader.ReadMetadataAsync();
        NativeMemoryTracker.Reset();
        using var read = await reader.ReadRowGroupAsync(0);

        Assert.Equal("c", ((StringArray)read.Column(0)).GetString(2));
        long guess = (long)Rows * 32;
        Assert.True(NativeMemoryTracker.PeakBytes < guess / 2,
            $"the read peaked at {NativeMemoryTracker.PeakBytes} bytes; the 32-a-value guess alone is {guess}");
    }

    [SkippableFact]
    public async Task PyArrowsRecordedCount_IsReadAndSizesTheBuffer()
    {
        // Arrow C++ records SizeStatistics by default: an independent writer of the field this reads (#889 review,
        // which took unencoded_byte_array_data_bytes for field 3; parquet.thrift has it as field 1).
        ExternalParquetReaders.Require();
        string path = Path.Combine(_tempDir, "pyarrow-delta.parquet");
        var (available, error) = ExternalParquetReaders.PyArrowWriteStrings(path, Rows, Length, "DELTA_BYTE_ARRAY");
        Skip.IfNot(available, $"pyarrow unavailable: {error}");
        Assert.Null(error);

        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var meta = (await reader.ReadMetadataAsync()).RowGroups[0].Columns[0].MetaData!;
        Assert.Equal((long)Rows * Length, meta.UnencodedByteArrayDataBytes);

        NativeMemoryTracker.Reset();
        using var batch = await reader.ReadRowGroupAsync(0);
        long decoded = (long)Rows * Length + (Rows + 1L) * 4;
        Assert.Equal(new string('7', Length), ((StringArray)batch.Column(0)).GetString(7));
        Assert.True(NativeMemoryTracker.PeakBytes <= decoded * Slack,
            $"the read peaked at {NativeMemoryTracker.PeakBytes} bytes, past {Slack} times the {decoded} it decodes to");
    }

    [Fact]
    public async Task TheBudgetsEstimate_CountsTheRecordedStringBytes()
    {
        // A DELTA_BYTE_ARRAY chunk of strings sharing all but their last byte stores a few bytes a value: the footer
        // size alone reserved far less than the buffer the read now allocates from the recorded count (#889 review).
        var b = new StringArray.Builder();
        string prefix = new('p', Length - 1);
        for (int i = 0; i < Rows; i++)
            b.Append(prefix + (char)('a' + i % 26));
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", StringType.Default, false)).Build();
        string path = Path.Combine(_tempDir, "prefixed.parquet");
        using (var batch = new RecordBatch(schema, [b.Build()], Rows))
        await using (var output = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(output, ownsFile: false, new ParquetWriteOptions
                     {
                         DictionaryEnabled = false,
                         ColumnEncodings = new Dictionary<string, ByteArrayEncoding> { ["v"] = ByteArrayEncoding.DeltaByteArray },
                     }))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }
        RecordStringBytes(path, (long)Rows * Length);

        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var group = (await reader.ReadMetadataAsync()).RowGroups[0];
        Assert.True(group.Columns[0].MetaData!.TotalUncompressedSize < (long)Rows * Length / 4);

        long estimate = reader.DecodedBytes(group, await reader.GetSchemaAsync(), null, null);

        Assert.True(estimate >= (long)Rows * Length, $"the estimate {estimate} is short of the recorded strings");
    }

    [Fact]
    public async Task TheBudgetsEstimate_CountsTheRecordedStringBytes_InANestedRead()
    {
        // A nested read is sized by its footer and decoded whole; its string buffers come from the recorded count, so
        // the estimate counts that too (#889 review round 5). One struct of one prefix-compressed string field.
        var b = new StringArray.Builder();
        string prefix = new('p', Length - 1);
        for (int i = 0; i < Rows; i++)
            b.Append(prefix + (char)('a' + i % 26));
        var structType = new StructType([new Field("v", StringType.Default, false)]);
        var structArray = new StructArray(structType, Rows, [b.Build()], ArrowBuffer.Empty, nullCount: 0);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("s", structType, false)).Build();
        string path = Path.Combine(_tempDir, "nested.parquet");
        using (var batch = new RecordBatch(schema, [structArray], Rows))
        await using (var output = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(output, ownsFile: false, new ParquetWriteOptions
                     {
                         DictionaryEnabled = false,
                         ColumnEncodings = new Dictionary<string, ByteArrayEncoding> { ["s.v"] = ByteArrayEncoding.DeltaByteArray },
                     }))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }
        RecordStringBytes(path, (long)Rows * Length);

        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var group = (await reader.ReadMetadataAsync()).RowGroups[0];
        var meta = group.Columns[0].MetaData!;
        Assert.Contains(Encoding.DeltaByteArray, meta.Encodings);
        Assert.True(meta.TotalUncompressedSize < (long)Rows * Length / 4);

        long estimate = reader.DecodedBytes(group, await reader.GetSchemaAsync(), null, null);

        Assert.True(estimate >= (long)Rows * Length, $"the estimate {estimate} is short of the recorded strings");
    }

    [Fact]
    public async Task ABatchedRead_SizesEachSliceFromItsPages()
    {
        // 10,000-row batches over 5,000-row pages: each slice's buffer starts at its pages' bound, not at 32 bytes a
        // value doubled to 64 (#889 review). Without an OffsetIndex, the page map is built from the page headers, so
        // its page sizes are the pages' own. A map built from an OffsetIndex has only estimates; sizing those slices
        // needs the index's per-page unencoded_byte_array_data_bytes, which this writer does not record yet (#890).
        string path = await WriteAsync("delta-length", new ParquetWriteOptions
        {
            DictionaryEnabled = false,
            DataPageRowCountLimit = 5_000,
            WritePageIndex = false,
        });
        const int batchRows = 10_000;
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { BatchSize = batchRows });
        await reader.ReadMetadataAsync();
        NativeMemoryTracker.Reset();

        long worst = 0;
        int rows = 0;
        await foreach (var batch in reader.ReadAllAsync())
        {
            rows += batch.Length;
            batch.Dispose();
            worst = Math.Max(worst, NativeMemoryTracker.PeakBytes);
            NativeMemoryTracker.Reset();
        }

        Assert.Equal(Rows, rows);
        long decoded = (long)batchRows * Length + (batchRows + 1L) * 4;
        Assert.True(worst <= decoded * Slack,
            $"a batch's read peaked at {worst} bytes, {(double)worst / decoded:F2} times the {decoded} it decodes to");
    }

    [Theory]
    [InlineData(ByteArrayOutputKind.Default)]
    [InlineData(ByteArrayOutputKind.ViewType)]
    public async Task ARecordedZero_AllocatesNoStringData(ByteArrayOutputKind output)
    {
        // Every value empty: the chunk records 0 string bytes, which is known, not unknown. The read starts its data
        // buffer at nothing rather than at the 4.8 MB guess (#889 review round 3).
        var b = new StringArray.Builder();
        for (int i = 0; i < Rows; i++)
            b.Append(string.Empty);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", StringType.Default, false)).Build();
        string path = Path.Combine(_tempDir, "empty.parquet");
        using (var batch = new RecordBatch(schema, [b.Build()], Rows))
        await using (var output2 = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(output2, ownsFile: false, new ParquetWriteOptions
                     {
                         DictionaryEnabled = false,
                         ColumnEncodings = new Dictionary<string, ByteArrayEncoding> { ["v"] = ByteArrayEncoding.DeltaByteArray },
                     }))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }
        RecordStringBytes(path, 0);

        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { ByteArrayOutput = output });
        await reader.ReadMetadataAsync();
        NativeMemoryTracker.Reset();
        using var read = await reader.ReadRowGroupAsync(0);

        Assert.Equal(Rows, read.Length);
        var column = read.Column(0);
        Assert.Equal(string.Empty, column is StringViewArray views ? views.GetString(5) : ((StringArray)column).GetString(5));
        // The views or offsets themselves, and no string data beside them.
        long structure = output == ByteArrayOutputKind.ViewType ? Rows * 16L : (Rows + 1L) * 4;
        Assert.True(NativeMemoryTracker.PeakBytes <= structure,
            $"the read peaked at {NativeMemoryTracker.PeakBytes} bytes, past the {structure} of its " +
            $"{(output == ByteArrayOutputKind.ViewType ? "views" : "offsets")}");
    }

    [Fact]
    public async Task ARecordedCountTooLarge_AllocatesNoMoreThanTheCeilingUpFront()
    {
        // A footer claiming almost 2 GiB of strings for a 5 MB chunk: what it says is not checked before the values
        // decode, so it buys at most the up-front ceiling, not the allocation it names (#889 review round 4).
        string path = await WriteAsync("delta");
        RecordStringBytes(path, int.MaxValue - 64);
        await AssertReadsWithinAsync(path, ByteArrayOutputKind.Default, slack: null);

        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        await reader.ReadMetadataAsync();
        NativeMemoryTracker.Reset();
        using var batch = await reader.ReadRowGroupAsync(0);

        // The ceiling, and what the values really decode to (their buffer and the decoder's own), not 2 GiB.
        long ceiling = EngineeredWood.Parquet.Data.ColumnBuildState.MaxExpectedStartingBytes
                       + 2L * Rows * Length + (Rows + 1L) * 4;
        Assert.True(NativeMemoryTracker.PeakBytes <= ceiling,
            $"the read peaked at {NativeMemoryTracker.PeakBytes} bytes, past the {ceiling} its footer can buy");
    }

    [Fact]
    public async Task ARecordedCountTooSmall_StillReadsEveryValue()
    {
        string path = await WriteAsync("delta");
        RecordStringBytes(path, 1_000);
        await AssertReadsWithinAsync(path, ByteArrayOutputKind.Default, slack: null);
    }

    [Fact]
    public void TheRecordedCount_RoundTripsThroughTheFooter()
    {
        var meta = new ColumnMetaData
        {
            Type = PhysicalType.ByteArray,
            Encodings = [Encoding.DeltaByteArray],
            Codec = CompressionCodec.Uncompressed,
            NumValues = 10,
            TotalUncompressedSize = 100,
            TotalCompressedSize = 100,
            DataPageOffset = 4,
            UnencodedByteArrayDataBytes = 1_234_567_890_123,
        };
        var file = new FileMetaData
        {
            Version = 2,
            Schema = [new SchemaElement { Name = "schema", NumChildren = 1 },
                      new SchemaElement { Name = "v", Type = PhysicalType.ByteArray }],
            NumRows = 10,
            RowGroups = [new RowGroup
            {
                Columns = [new ColumnChunk { FileOffset = 4, MetaData = meta }],
                TotalByteSize = 100,
                NumRows = 10,
            }],
        };

        var decoded = MetadataDecoder.DecodeFileMetaData(MetadataEncoder.EncodeFileMetaData(file));

        Assert.Equal(1_234_567_890_123, decoded.RowGroups[0].Columns[0].MetaData!.UnencodedByteArrayDataBytes);
    }

    private static async Task AssertReadsWithinAsync(string path, ByteArrayOutputKind output, double? slack)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(
            file, ownsFile: false, new ParquetReadOptions { ByteArrayOutput = output });
        await reader.ReadMetadataAsync();

        NativeMemoryTracker.Reset();
        using var batch = await reader.ReadRowGroupAsync(0);
        long peak = NativeMemoryTracker.PeakBytes;

        Assert.Equal(Rows, batch.Length);
        var column = batch.Column(0);
        var expected = Values(repeating: Path.GetFileName(path).StartsWith("dictionary", StringComparison.Ordinal));
        for (int i = 0; i < Rows; i += 997)
        {
            string actual = column switch
            {
                StringViewArray views => views.GetString(i),
                LargeStringArray large => large.GetString(i),
                StringArray strings => strings.GetString(i),
                _ => throw new InvalidOperationException(column.GetType().Name),
            };
            Assert.Equal(expected[i], actual);
        }

        if (slack is { } s)
        {
            long decoded = (long)Rows * Length + output switch
            {
                ByteArrayOutputKind.ViewType => Rows * 16L,
                ByteArrayOutputKind.LargeOffsets => (Rows + 1L) * 8,
                _ => (Rows + 1L) * 4,
            };
            Assert.True(peak <= decoded * s,
                $"{Path.GetFileName(path)} ({output}): the read peaked at {peak} bytes, {(double)peak / decoded:F2} " +
                $"times the {decoded} it decodes to");
        }
    }

    // Random values, or (for a dictionary) 500 of them repeating.
    private static string[] Values(bool repeating)
    {
        var random = new Random(888);
        var values = new string[Rows];
        var chars = new char[Length];
        for (int i = 0; i < Rows; i++)
        {
            if (repeating && i >= 500)
            {
                values[i] = values[i % 500];
                continue;
            }
            for (int c = 0; c < chars.Length; c++)
                chars[c] = (char)('a' + random.Next(26));
            values[i] = new string(chars);
        }
        return values;
    }

    private Task<string> WriteAsync(string encoding) => WriteAsync(encoding, null);

    private async Task<string> WriteAsync(string encoding, ParquetWriteOptions? options)
    {
        var b = new StringArray.Builder();
        foreach (string value in Values(repeating: encoding == "dictionary"))
            b.Append(value);

        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", StringType.Default, false)).Build();
        string path = Path.Combine(_tempDir, encoding + ".parquet");
        using var batch = new RecordBatch(schema, [b.Build()], Rows);
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options ?? new ParquetWriteOptions
        {
            DictionaryEnabled = encoding == "dictionary",
            ColumnEncodings = encoding switch
            {
                "delta" => new Dictionary<string, ByteArrayEncoding> { ["v"] = ByteArrayEncoding.DeltaByteArray },
                "fsst" => new Dictionary<string, ByteArrayEncoding> { ["v"] = ByteArrayEncoding.Fsst },
                _ => null,
            },
        });
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    /// <summary>Rewrites <paramref name="path"/>'s footer to record its one chunk's string bytes as
    /// <paramref name="bytes"/>, as a writer of <c>SizeStatistics</c> does (EngineeredWood's does not).</summary>
    private static void RecordStringBytes(string path, long bytes)
    {
        byte[] content = File.ReadAllBytes(path);
        int footerLength = BinaryPrimitives.ReadInt32LittleEndian(content.AsSpan(content.Length - 8));
        int footerStart = content.Length - 8 - footerLength;
        var footer = MetadataDecoder.DecodeFileMetaData(content.AsSpan(footerStart, footerLength));

        var group = footer.RowGroups[0];
        var chunk = group.Columns[0];
        var m = chunk.MetaData!;
        var patched = new FileMetaData
        {
            Version = footer.Version,
            Schema = footer.Schema,
            NumRows = footer.NumRows,
            KeyValueMetadata = footer.KeyValueMetadata,
            CreatedBy = footer.CreatedBy,
            ColumnOrders = footer.ColumnOrders,
            RowGroups = [new RowGroup
            {
                TotalByteSize = group.TotalByteSize,
                NumRows = group.NumRows,
                SortingColumns = group.SortingColumns,
                TotalCompressedSize = group.TotalCompressedSize,
                Ordinal = group.Ordinal,
                Columns = [new ColumnChunk
                {
                    FilePath = chunk.FilePath,
                    FileOffset = chunk.FileOffset,
                    OffsetIndexOffset = chunk.OffsetIndexOffset,
                    OffsetIndexLength = chunk.OffsetIndexLength,
                    ColumnIndexOffset = chunk.ColumnIndexOffset,
                    ColumnIndexLength = chunk.ColumnIndexLength,
                    MetaData = new ColumnMetaData
                    {
                        Type = m.Type,
                        Encodings = m.Encodings,
                        PathInSchema = m.PathInSchema,
                        Codec = m.Codec,
                        NumValues = m.NumValues,
                        TotalUncompressedSize = m.TotalUncompressedSize,
                        TotalCompressedSize = m.TotalCompressedSize,
                        DataPageOffset = m.DataPageOffset,
                        IndexPageOffset = m.IndexPageOffset,
                        DictionaryPageOffset = m.DictionaryPageOffset,
                        Statistics = m.Statistics,
                        KeyValueMetadata = m.KeyValueMetadata,
                        EncodingStats = m.EncodingStats,
                        BloomFilterOffset = m.BloomFilterOffset,
                        BloomFilterLength = m.BloomFilterLength,
                        SymbolTablePageOffset = m.SymbolTablePageOffset,
                        SymbolTablePageLength = m.SymbolTablePageLength,
                        UnencodedByteArrayDataBytes = bytes,
                    },
                }],
            }],
        };

        byte[] encoded = MetadataEncoder.EncodeFileMetaData(patched);
        var tail = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(tail, encoded.Length);
        "PAR1"u8.CopyTo(tail.AsSpan(4));
        using var output = File.Create(path);
        output.Write(content, 0, footerStart);
        output.Write(encoded, 0, encoded.Length);
        output.Write(tail, 0, tail.Length);
    }
}

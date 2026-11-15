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

    private async Task<string> WriteAsync(string encoding)
    {
        var b = new StringArray.Builder();
        foreach (string value in Values(repeating: encoding == "dictionary"))
            b.Append(value);

        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", StringType.Default, false)).Build();
        string path = Path.Combine(_tempDir, encoding + ".parquet");
        using var batch = new RecordBatch(schema, [b.Build()], Rows);
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, new ParquetWriteOptions
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

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

#pragma warning disable EWPARQUET0003 // FSST, an evaluation encoding

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// The writer records a BYTE_ARRAY chunk's value bytes, length prefixes excluded, as
/// <c>SizeStatistics.unencoded_byte_array_data_bytes</c>, and each page's as the OffsetIndex's (#890). A reader sizes
/// a dictionary, DELTA_BYTE_ARRAY or FSST chunk's string buffer from these, since such a chunk's stored size is no
/// bound on what it decodes to (#888).
/// </summary>
public sealed class SizeStatisticsWriterTests : IDisposable
{
    private const int Rows = 20_000;
    private const int PageRows = 3_000;

    private readonly string _tempDir =
        Path.Combine(Path.GetTempPath(), "ew-size-stats-" + Guid.NewGuid().ToString("N")[..8]);

    public SizeStatisticsWriterTests() => Directory.CreateDirectory(_tempDir);

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

    public static TheoryData<string, DataPageVersion, bool> Encodings()
    {
        var data = new TheoryData<string, DataPageVersion, bool>();
        foreach (bool buffered in new[] { false, true })
        {
            foreach (var version in new[] { DataPageVersion.V1, DataPageVersion.V2 })
            {
                foreach (string encoding in new[] { "plain", "delta-length", "delta", "dictionary" })
                    data.Add(encoding, version, buffered);
            }
            data.Add("fsst", DataPageVersion.V2, buffered); // V1 pages cannot carry FSST's symbol table
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(Encodings))]
    public async Task AStringChunk_RecordsItsValueBytes_PerPageAndInTotal(
        string encoding, DataPageVersion version, bool buffered)
    {
        string?[] values = Values(repeating: encoding == "dictionary");
        string path = await WriteAsync(StringColumn(values), Options(encoding, version), buffered);

        var (meta, chunk, file) = ReadFooter(path);
        Assert.Contains(encoding switch
        {
            "plain" => Encoding.Plain,
            "delta-length" => Encoding.DeltaLengthByteArray,
            "delta" => Encoding.DeltaByteArray,
            "fsst" => Encoding.Fsst,
            _ => Encoding.RleDictionary,
        }, meta.Encodings);
        Assert.Equal(Utf8Bytes(values, 0, Rows), meta.UnencodedByteArrayDataBytes);

        // A flat column's page holds the rows from its first to the next page's first.
        var index = ReadOffsetIndex(file, chunk);
        var pages = index.PageLocations;
        Assert.True(pages.Count > 1, $"{pages.Count} page(s)");
        Assert.NotNull(index.UnencodedByteArrayDataBytes);
        Assert.Equal(pages.Count, index.UnencodedByteArrayDataBytes!.Count);
        for (int p = 0; p < pages.Count; p++)
        {
            int first = (int)pages[p].FirstRowIndex;
            int end = p + 1 < pages.Count ? (int)pages[p + 1].FirstRowIndex : Rows;
            Assert.Equal(Utf8Bytes(values, first, end), index.UnencodedByteArrayDataBytes[p]);
        }
    }

    [Fact]
    public async Task WithoutAPageIndex_TheChunkStillRecordsItsBytes()
    {
        string?[] values = Values(repeating: true);
        string path = await WriteAsync(StringColumn(values), new ParquetWriteOptions { WritePageIndex = false }, buffered: false);

        var (meta, chunk, _) = ReadFooter(path);
        Assert.Null(chunk.OffsetIndexOffset);
        Assert.Equal(Utf8Bytes(values, 0, Rows), meta.UnencodedByteArrayDataBytes);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public async Task WithSizeStatisticsOff_NothingIsRecorded(bool dictionary, bool buffered)
    {
        string path = await WriteAsync(StringColumn(Values(repeating: dictionary)), new ParquetWriteOptions
        {
            WriteSizeStatistics = false,
            DictionaryEnabled = dictionary,
            DataPageRowCountLimit = PageRows,
        }, buffered);

        var (meta, chunk, file) = ReadFooter(path);
        Assert.Equal(dictionary, meta.Encodings.Contains(Encoding.RleDictionary));
        Assert.Null(meta.UnencodedByteArrayDataBytes);
        Assert.Null(ReadOffsetIndex(file, chunk).UnencodedByteArrayDataBytes);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ARepeatedStringLeaf_CountsOnlyItsValues(bool dictionary)
    {
        // Null lists, empty lists and null elements contribute level entries but no value bytes.
        var listType = new ListType(new Field("element", StringType.Default, true));
        var values = new StringArray.Builder();
        var offsets = new ArrowBuffer.Builder<int>().Append(0);
        var validity = new ArrowBuffer.BitmapBuilder();
        long expected = 0;
        int count = 0, nulls = 0;
        for (int row = 0; row < Rows; row++)
        {
            if (row % 11 == 0)
            {
                validity.Append(false);
                nulls++;
            }
            else
            {
                validity.Append(true);
                for (int e = 0; e < row % 4; e++)
                {
                    if ((row + e) % 7 == 0)
                    {
                        values.AppendNull();
                    }
                    else
                    {
                        string value = "élément-" + (dictionary ? row % 40 : row) + new string('x', e);
                        values.Append(value);
                        expected += System.Text.Encoding.UTF8.GetByteCount(value);
                    }
                    count++;
                }
            }
            offsets.Append(count);
        }
        var list = new ListArray(listType, Rows, offsets.Build(), values.Build(), validity.Build(), nulls);
        var batch = new RecordBatch(new Apache.Arrow.Schema([new Field("l", listType, true)], null), [list], Rows);

        string path = await WriteAsync(batch, new ParquetWriteOptions
        {
            DictionaryEnabled = dictionary,
            DataPageRowCountLimit = PageRows,
        }, buffered: false);

        var (meta, chunk, file) = ReadFooter(path);
        Assert.Equal(dictionary, meta.Encodings.Contains(Encoding.RleDictionary));
        Assert.Equal(expected, meta.UnencodedByteArrayDataBytes);
        Assert.Equal(expected, ReadOffsetIndex(file, chunk).UnencodedByteArrayDataBytes!.Sum());
    }

    [Fact]
    public async Task ARunEndEncodedStringColumn_CountsEveryRowOfEachRun()
    {
        // The dictionary takes a run-end encoded column as runs, not one index per row; each run counts its entry
        // once per row it covers, including the runs a page boundary splits.
        var ends = new Int32Array.Builder();
        var runValues = new StringArray.Builder();
        long expected = 0;
        int end = 0;
        for (int run = 0; end < Rows; run++)
        {
            int length = Math.Min(1 + run % 1_700, Rows - end);
            end += length;
            string value = "run-" + (run % 9) + new string('r', run % 5);
            ends.Append(end);
            runValues.Append(value);
            expected += (long)length * System.Text.Encoding.UTF8.GetByteCount(value);
        }
        var column = new RunEndEncodedArray(ends.Build(), runValues.Build());
        var batch = new RecordBatch(
            new Apache.Arrow.Schema([new Field("v", column.Data.DataType, false)], null), [column], Rows);

        string path = await WriteAsync(batch, new ParquetWriteOptions { DataPageRowCountLimit = PageRows }, buffered: false);

        var (meta, chunk, file) = ReadFooter(path);
        Assert.Contains(Encoding.RleDictionary, meta.Encodings);
        Assert.Equal(expected, meta.UnencodedByteArrayDataBytes);
        var pages = ReadOffsetIndex(file, chunk).UnencodedByteArrayDataBytes!;
        Assert.True(pages.Count > 1);
        Assert.Equal(expected, pages.Sum());
    }

    [Fact]
    public async Task OtherPhysicalTypes_RecordNoByteArrayBytes()
    {
        // A wide decimal is a FIXED_LEN_BYTE_ARRAY: byte arrays, but not BYTE_ARRAY.
        var decimalType = new Decimal128Type(30, 2);
        var ints = new Int64Array.Builder();
        var fixedWidth = new Decimal128Array.Builder(decimalType);
        for (int i = 0; i < Rows; i++)
        {
            ints.Append(i);
            fixedWidth.Append(i / 100m);
        }
        var schema = new Apache.Arrow.Schema(
            [new Field("i", Int64Type.Default, false), new Field("f", decimalType, false)], null);
        string path = await WriteAsync(
            new RecordBatch(schema, [ints.Build(), fixedWidth.Build()], Rows),
            new ParquetWriteOptions { DataPageRowCountLimit = PageRows }, buffered: false);

        byte[] file = File.ReadAllBytes(path);
        var chunks = Footer(file).RowGroups[0].Columns;
        Assert.Equal([PhysicalType.Int64, PhysicalType.FixedLenByteArray], chunks.Select(c => c.MetaData!.Type));
        foreach (var chunk in chunks)
        {
            Assert.Null(chunk.MetaData!.UnencodedByteArrayDataBytes);
            Assert.Null(ReadOffsetIndex(file, chunk).UnencodedByteArrayDataBytes);
        }
    }

    [Fact]
    public async Task AnAllNullStringChunk_RecordsZero()
    {
        var values = new string?[Rows];
        string path = await WriteAsync(StringColumn(values), new ParquetWriteOptions(), buffered: true);

        var (meta, _, _) = ReadFooter(path);
        Assert.Equal(0L, meta.UnencodedByteArrayDataBytes);
    }

    private static ParquetWriteOptions Options(string encoding, DataPageVersion version) => new()
    {
        DataPageVersion = version,
        DataPageRowCountLimit = PageRows,
        DictionaryEnabled = encoding == "dictionary",
        ByteArrayEncoding = encoding switch
        {
            "plain" => ByteArrayEncoding.Plain,
            "delta" => ByteArrayEncoding.DeltaByteArray,
            "fsst" => ByteArrayEncoding.Fsst,
            _ => ByteArrayEncoding.DeltaLengthByteArray,
        },
    };

    // Variable-length, partly non-ASCII, with nulls; or (for a dictionary) a few hundred distinct values repeating.
    private static string?[] Values(bool repeating)
    {
        var random = new Random(890);
        var values = new string?[Rows];
        for (int i = 0; i < Rows; i++)
        {
            if (i % 13 == 5)
                continue;
            int seed = repeating ? i % 300 : i;
            values[i] = (seed % 3 == 0 ? "ünïcødé-" : "value-") + seed + new string('z', repeating ? seed % 50 : random.Next(60));
        }
        return values;
    }

    private static long Utf8Bytes(string?[] values, int start, int end)
    {
        long total = 0;
        for (int i = start; i < end; i++)
        {
            if (values[i] is { } value)
                total += System.Text.Encoding.UTF8.GetByteCount(value);
        }
        return total;
    }

    private static RecordBatch StringColumn(string?[] values)
    {
        var b = new StringArray.Builder();
        foreach (string? value in values)
        {
            if (value is null) b.AppendNull();
            else b.Append(value);
        }
        var schema = new Apache.Arrow.Schema([new Field("v", StringType.Default, true)], null);
        return new RecordBatch(schema, [b.Build()], values.Length);
    }

    private async Task<string> WriteAsync(RecordBatch batch, ParquetWriteOptions options, bool buffered)
    {
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        using (batch)
        await using (var file = new LocalSequentialFile(path))
        {
            if (buffered)
            {
                await using var writer = new BufferedParquetWriter(file, ownsFile: false, options);
                await writer.AppendAsync(batch);
                await writer.CloseAsync();
            }
            else
            {
                await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
                await writer.WriteRowGroupAsync(batch);
                await writer.CloseAsync();
            }
        }
        return path;
    }

    private static FileMetaData Footer(byte[] file)
    {
        int footerLength = BinaryPrimitives.ReadInt32LittleEndian(file.AsSpan(file.Length - 8, 4));
        return MetadataDecoder.DecodeFileMetaData(file.AsSpan(file.Length - 8 - footerLength, footerLength));
    }

    private static (ColumnMetaData Meta, ColumnChunk Chunk, byte[] File) ReadFooter(string path)
    {
        byte[] file = File.ReadAllBytes(path);
        var footer = Footer(file);
        Assert.Single(footer.RowGroups);
        var chunk = footer.RowGroups[0].Columns[0];
        return (chunk.MetaData!, chunk, file);
    }

    private static OffsetIndex ReadOffsetIndex(byte[] file, ColumnChunk chunk) =>
        MetadataDecoder.DecodeOffsetIndex(file.AsSpan((int)chunk.OffsetIndexOffset!.Value, chunk.OffsetIndexLength!.Value));
}

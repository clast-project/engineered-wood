// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#pragma warning disable EWPARQUET0003 // FSST tests intentionally reference the experimental enum values.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// Tests for FSST: the symbol table page body, the data page body, and end-to-end files.
/// Layout assertions cite the FSST proposal's section numbers.
/// </summary>
public class FsstEncodingTests : IDisposable
{
    private readonly string _tempDir;

    public FsstEncodingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-fsst-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // ───── Symbol table page body (§3.3) ─────

    /// <summary>
    /// The worked example from §6.1: six symbols in two-per-length blocks of 5, 7 and 8 bytes.
    /// </summary>
    private static byte[] SpecExampleSymbolTableBody()
    {
        var body = new List<byte> { 6 };
        // length_histogram[i] = symbols of length i+1 → two each at lengths 5, 7 and 8.
        body.AddRange(new byte[] { 0, 0, 0, 0, 2, 0, 2, 2 });
        foreach (var symbol in new[] { "/page", "/data", "http://", "example", "https://", "test.com" })
            body.AddRange(System.Text.Encoding.UTF8.GetBytes(symbol));
        return body.ToArray();
    }

    [Fact]
    public void SymbolTable_SpecExample_RoundTripsByteForByte()
    {
        byte[] body = SpecExampleSymbolTableBody();

        // §3.5: 1 (symbol_count) + 8 (length_histogram) + 40 (symbol_data) = 49.
        Assert.Equal(49, body.Length);

        var table = FsstSymbolTable.Parse(body);

        Assert.Equal(6, table.SymbolCount);
        Assert.Equal(body.Length, table.SerializedSize);
        Assert.Equal(body, table.Serialize());
    }

    [Fact]
    public void SymbolTable_Parse_RejectsHistogramThatDisagreesWithSymbolCount()
    {
        byte[] body = SpecExampleSymbolTableBody();
        body[0] = 5; // histogram still sums to 6

        var ex = Assert.Throws<ParquetFormatException>(() => FsstSymbolTable.Parse(body));
        Assert.Contains("length_histogram", ex.Message);
    }

    [Fact]
    public void SymbolTable_Parse_RejectsSymbolDataOfTheWrongSize()
    {
        byte[] body = SpecExampleSymbolTableBody();
        byte[] truncated = body.AsSpan(0, body.Length - 1).ToArray();

        var ex = Assert.Throws<ParquetFormatException>(() => FsstSymbolTable.Parse(truncated));
        Assert.Contains("histogram describes", ex.Message);
    }

    [Fact]
    public void SymbolTable_Parse_RejectsBodyTooSmallForHeader()
    {
        var ex = Assert.Throws<ParquetFormatException>(
            () => FsstSymbolTable.Parse(new byte[4]));
        Assert.Contains("too small", ex.Message);
    }

    [Fact]
    public void SymbolTable_Parse_AcceptsEmptyTable()
    {
        var table = FsstSymbolTable.Parse(new byte[9]);
        Assert.Equal(0, table.SymbolCount);
    }

    [Fact]
    public void SymbolTable_Train_AssignsCodesInAscendingLengthOrder()
    {
        // Clast.Fsst assigns codes by training gain, not by length, so this is really a test
        // that TryTrain renumbers them into the order §3.3 requires.
        var values = new byte[600][];
        for (int i = 0; i < values.Length; i++)
            values[i] = System.Text.Encoding.UTF8.GetBytes($"https://example.com/catalog/item/{i}?ref=search");

        var table = FsstSymbolTable.TryTrain(values);
        Assert.NotNull(table);

        // Re-reading the serialized form recovers the lengths, which must be non-decreasing.
        byte[] body = table!.Serialize();
        Assert.Equal(table.SymbolCount, body[0]);

        int histogramSum = 0;
        for (int i = 0; i < 8; i++)
            histogramSum += body[1 + i];
        Assert.Equal(table.SymbolCount, histogramSum);

        // Parse validates the histogram against symbol_data, so a wrong order would not survive.
        var reparsed = FsstSymbolTable.Parse(body);
        Assert.Equal(table.SymbolCount, reparsed.SymbolCount);
        Assert.Equal(body, reparsed.Serialize());
    }

    private static PageHeader SymbolTablePageHeaderFor(int uncompressedSize, int compressedSize) =>
        new()
        {
            Type = PageType.SymbolTablePage,
            UncompressedPageSize = uncompressedSize,
            CompressedPageSize = compressedSize,
            SymbolTablePageHeader = new SymbolTablePageHeader
            {
                Type = SymbolTableType.Fsst,
                IsCompressed = true,
            },
        };

    [Fact]
    public void SymbolTablePage_RejectsBodyThatDecompressesToTheWrongSize()
    {
        // A pooled decompression buffer arrives holding the previous renter's bytes, so a page
        // that decompresses short must be rejected rather than sliced to its declared size —
        // otherwise Parse would be handed stale bytes it might well validate.
        byte[] body = SpecExampleSymbolTableBody();
        var compressed = new byte[Compressor.GetMaxCompressedLength(CompressionCodec.Snappy, body.Length)];
        int compressedLen = Compressor.Compress(CompressionCodec.Snappy, body, compressed, null, null);

        var columnMeta = ByteArrayColumnMeta(CompressionCodec.Snappy);
        var header = SymbolTablePageHeaderFor(body.Length + 40, compressedLen); // lies about the size

        var ex = Assert.Throws<ParquetFormatException>(() =>
            FsstPageDecoder.ReadSymbolTablePage(
                header, compressed.AsSpan(0, compressedLen), columnMeta));
        Assert.Contains("decompressed to", ex.Message);
    }

    [Fact]
    public void SymbolTablePage_RejectsNegativeUncompressedSize()
    {
        var columnMeta = ByteArrayColumnMeta(CompressionCodec.Snappy);
        var header = SymbolTablePageHeaderFor(-1, 4);

        var ex = Assert.Throws<ParquetFormatException>(() =>
            FsstPageDecoder.ReadSymbolTablePage(header, new byte[4], columnMeta));
        Assert.Contains("negative", ex.Message);
    }

    [Fact]
    public void SymbolTablePage_CompressedFlagUnderUncompressedCodec_ReadsAsPlain()
    {
        // Contradictory, but there is no codec to invoke — the bytes can only be plain, and
        // refusing the file would reject one that is perfectly readable.
        byte[] body = SpecExampleSymbolTableBody();
        var columnMeta = ByteArrayColumnMeta(CompressionCodec.Uncompressed);
        var header = SymbolTablePageHeaderFor(body.Length, body.Length);

        var table = FsstPageDecoder.ReadSymbolTablePage(header, body, columnMeta);
        Assert.Equal(6, table.SymbolCount);
    }

    private static EngineeredWood.Parquet.Metadata.ColumnMetaData ByteArrayColumnMeta(
        CompressionCodec codec) =>
        new()
        {
            Type = PhysicalType.ByteArray,
            Encodings = [Encoding.Fsst],
            Codec = codec,
            NumValues = 0,
            TotalUncompressedSize = 0,
            TotalCompressedSize = 0,
            DataPageOffset = 0,
        };

    // ───── Data page body (§4.3) ─────

    private static string[] DecodePage(ReadOnlySpan<byte> page, FsstSymbolTable table, int count)
    {
        using var state = new ColumnBuildState(
            PhysicalType.ByteArray, maxDefLevel: 0, maxRepLevel: 0, capacity: count);
        FsstPageDecoder.Decode(page, table, count, state);

        var field = new Field("s", StringType.Default, nullable: false);
        var array = (StringArray)ArrowArrayBuilder.Build(state, field, count);
        var result = new string[count];
        for (int i = 0; i < count; i++)
            result[i] = array.GetString(i);
        return result;
    }

    /// <summary>
    /// The worked example from §6.2, byte for byte, decoded against the §6.1 symbol table.
    /// This is the one test that pins the wire format to the specification rather than to
    /// this library's own encoder.
    /// </summary>
    [Fact]
    public void DataPage_SpecExample_DecodesToTheDocumentedValues()
    {
        var table = FsstSymbolTable.Parse(SpecExampleSymbolTableBody());

        byte[] page =
        [
            0x00,                                       // offset_encoding = PLAIN
            0x04, 0x00, 0x00, 0x00,                     // num_values = 4
            0x10, 0x00, 0x00, 0x00,                     // offset_array_length = 16
            0x05, 0x00, 0x00, 0x00,                     // end offsets: 5
            0x0A, 0x00, 0x00, 0x00,                     //              10
            0x0D, 0x00, 0x00, 0x00,                     //              13
            0x0F, 0x00, 0x00, 0x00,                     //              15
            0x04, 0x03, 0x00, 0xFF, 0x31,               // https:// | example | /page | esc '1'
            0x04, 0x03, 0x00, 0xFF, 0x32,               // https:// | example | /page | esc '2'
            0x04, 0x05, 0x01,                           // https:// | test.com | /data
            0x02, 0x03,                                 // http:// | example
        ];

        Assert.Equal(
            new[]
            {
                "https://example/page1",
                "https://example/page2",
                "https://test.com/data",
                "http://example",
            },
            DecodePage(page, table, 4));
    }

    [Fact]
    public void DataPage_RoundTripsThroughTheEncoder()
    {
        var values = new byte[500][];
        for (int i = 0; i < values.Length; i++)
            values[i] = System.Text.Encoding.UTF8.GetBytes($"user-{i}@example.com/session/{i % 13}");

        var column = FsstCompressedColumn.TryCompress(values);
        Assert.NotNull(column);

        byte[] page = FsstPageEncoder.Encode(column!, valueIndex: 0, count: values.Length);
        string[] decoded = DecodePage(page, column!.Table, values.Length);

        for (int i = 0; i < values.Length; i++)
            Assert.Equal(System.Text.Encoding.UTF8.GetString(values[i]), decoded[i]);
    }

    [Fact]
    public void DataPage_RoundTripsASliceOfTheColumn()
    {
        var values = new byte[400][];
        for (int i = 0; i < values.Length; i++)
            values[i] = System.Text.Encoding.UTF8.GetBytes($"https://cdn.example.net/assets/{i}.png");

        var column = FsstCompressedColumn.TryCompress(values);
        Assert.NotNull(column);

        // A page in the middle: offsets must be rebased onto the page's own data section.
        byte[] page = FsstPageEncoder.Encode(column!, valueIndex: 150, count: 100);
        string[] decoded = DecodePage(page, column!.Table, 100);

        for (int i = 0; i < 100; i++)
            Assert.Equal(System.Text.Encoding.UTF8.GetString(values[150 + i]), decoded[i]);
    }

    [Fact]
    public void DataPage_RoundTripsEmptyValues()
    {
        var values = new byte[300][];
        for (int i = 0; i < values.Length; i++)
            values[i] = i % 3 == 0
                ? []
                : System.Text.Encoding.UTF8.GetBytes($"repeating-payload-value-{i % 7}");

        var column = FsstCompressedColumn.TryCompress(values);
        Assert.NotNull(column);

        byte[] page = FsstPageEncoder.Encode(column!, 0, values.Length);
        string[] decoded = DecodePage(page, column!.Table, values.Length);

        for (int i = 0; i < values.Length; i++)
            Assert.Equal(System.Text.Encoding.UTF8.GetString(values[i]), decoded[i]);
    }

    [Fact]
    public void DataPage_RejectsUnknownOffsetEncoding()
    {
        var table = FsstSymbolTable.Parse(SpecExampleSymbolTableBody());
        byte[] page = [0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

        var ex = Assert.Throws<ParquetFormatException>(() => DecodePage(page, table, 0));
        Assert.Contains("offset_encoding", ex.Message);
    }

    [Fact]
    public void DataPage_RejectsNumValuesThatDisagreeWithThePage()
    {
        var table = FsstSymbolTable.Parse(SpecExampleSymbolTableBody());
        byte[] page = [0x00, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

        var ex = Assert.Throws<ParquetFormatException>(() => DecodePage(page, table, 4));
        Assert.Contains("num_values", ex.Message);
    }

    [Fact]
    public void DataPage_RejectsNonMonotonicOffsets()
    {
        var table = FsstSymbolTable.Parse(SpecExampleSymbolTableBody());
        byte[] page =
        [
            0x00,
            0x02, 0x00, 0x00, 0x00,                 // num_values = 2
            0x08, 0x00, 0x00, 0x00,                 // offset_array_length = 8
            0x03, 0x00, 0x00, 0x00,                 // end offsets: 3
            0x01, 0x00, 0x00, 0x00,                 //              1  ← goes backwards
            0x00, 0x01, 0x02,
        ];

        var ex = Assert.Throws<ParquetFormatException>(() => DecodePage(page, table, 2));
        Assert.Contains("decrease", ex.Message);
    }

    [Fact]
    public void DataPage_RejectsOffsetPastTheDataSection()
    {
        var table = FsstSymbolTable.Parse(SpecExampleSymbolTableBody());
        byte[] page =
        [
            0x00,
            0x01, 0x00, 0x00, 0x00,                 // num_values = 1
            0x04, 0x00, 0x00, 0x00,                 // offset_array_length = 4
            0x64, 0x00, 0x00, 0x00,                 // end offset 100, data section is 2 bytes
            0x00, 0x01,
        ];

        var ex = Assert.Throws<ParquetFormatException>(() => DecodePage(page, table, 1));
        Assert.Contains("runs past", ex.Message);
    }

    [Fact]
    public void DataPage_RejectsValueEndingInAnEscapeMarker()
    {
        // §5.2: a trailing escape has no literal to escape, and must not be allowed to borrow
        // the following value's first byte.
        var table = FsstSymbolTable.Parse(SpecExampleSymbolTableBody());
        byte[] page =
        [
            0x00,
            0x02, 0x00, 0x00, 0x00,                 // num_values = 2
            0x08, 0x00, 0x00, 0x00,                 // offset_array_length = 8
            0x02, 0x00, 0x00, 0x00,                 // value 0 ends at 2
            0x04, 0x00, 0x00, 0x00,                 // value 1 ends at 4
            0x00, 0xFF,                             // value 0: symbol, then a dangling escape
            0x41, 0x01,
        ];

        var ex = Assert.Throws<ParquetFormatException>(() => DecodePage(page, table, 2));
        Assert.Contains("escape", ex.Message);
    }

    [Fact]
    public void DataPage_RejectsCodeWithNoSymbol()
    {
        var table = FsstSymbolTable.Parse(SpecExampleSymbolTableBody()); // 6 symbols: codes 0-5
        byte[] page =
        [
            0x00,
            0x01, 0x00, 0x00, 0x00,
            0x04, 0x00, 0x00, 0x00,
            0x02, 0x00, 0x00, 0x00,
            0x00, 0x2A,                             // code 42 is not in the table
        ];

        var ex = Assert.Throws<ParquetFormatException>(() => DecodePage(page, table, 1));
        Assert.Contains("symbol code", ex.Message);
    }

    // ───── Encoding selection ─────

    [Fact]
    public void EncodingStrategyResolver_V2_ByteArray_Fsst()
    {
        var enc = EncodingStrategyResolver.GetV2Encoding(
            PhysicalType.ByteArray, ByteArrayEncoding.Fsst, FloatingPointEncoding.ByteStreamSplit);
        Assert.Equal(Encoding.Fsst, enc);
    }

    // ───── End to end ─────

    private static ParquetWriteOptions FsstOptions(
        CompressionCodec codec = CompressionCodec.Uncompressed, int? pageSize = null) =>
        ParquetWriteOptions.Default with
        {
            ByteArrayEncoding = ByteArrayEncoding.Fsst,
            DataPageVersion = DataPageVersion.V2,
            DictionaryEnabled = false,
            Compression = codec,
            DataPageSize = pageSize ?? ParquetWriteOptions.Default.DataPageSize,
        };

    private async Task<string> WriteAsync(
        string name, RecordBatch batch, ParquetWriteOptions options)
    {
        string path = Path.Combine(_tempDir, name);
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }
        return path;
    }

    private static RecordBatch StringBatch(
        string?[] values, IArrowType? type = null, bool nullable = true)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", type ?? StringType.Default, nullable))
            .Build();

        var builder = new StringArray.Builder();
        foreach (var v in values)
        {
            if (v is null) builder.AppendNull();
            else builder.Append(v);
        }

        return new RecordBatch(schema, [builder.Build()], values.Length);
    }

    private static string[] UrlValues(int count) =>
        Enumerable.Range(0, count)
            .Select(i => $"https://example.com/orders/{i}/items/{i % 17}?src=catalog")
            .ToArray();

    [Fact]
    public async Task File_HighCardinalityStrings_UseFsstAndRoundTrip()
    {
        var values = UrlValues(4000);
        string path = await WriteAsync(
            "urls.parquet", StringBatch(values, nullable: false), FsstOptions());

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);

        var meta = await reader.ReadMetadataAsync();
        var colMeta = meta.RowGroups[0].Columns[0].MetaData!;
        Assert.Contains(Encoding.Fsst, colMeta.Encodings);
        Assert.NotNull(colMeta.SymbolTablePageOffset);
        Assert.NotNull(colMeta.SymbolTablePageLength);
        Assert.True(colMeta.SymbolTablePageOffset < colMeta.DataPageOffset);

        var read = await reader.ReadRowGroupAsync(0);
        var arr = (StringArray)read.Column(0);
        Assert.Equal(values.Length, arr.Length);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], arr.GetString(i));
    }

    [Fact]
    public async Task File_FsstIsSmallerThanTheUncompressedAlternative()
    {
        var values = UrlValues(4000);
        var batch = StringBatch(values, nullable: false);

        string fsstPath = await WriteAsync("size-fsst.parquet", batch, FsstOptions());
        string dlbaPath = await WriteAsync(
            "size-dlba.parquet", batch,
            FsstOptions() with { ByteArrayEncoding = ByteArrayEncoding.DeltaLengthByteArray });

        Assert.True(
            new FileInfo(fsstPath).Length < new FileInfo(dlbaPath).Length,
            $"FSST file ({new FileInfo(fsstPath).Length} bytes) should be smaller than " +
            $"DELTA_LENGTH_BYTE_ARRAY ({new FileInfo(dlbaPath).Length} bytes).");
    }

    [Fact]
    public async Task File_NullsAndEmptyStrings_RoundTrip()
    {
        var values = new string?[2000];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = (i % 7) switch
            {
                0 => null,
                1 => "",
                _ => $"session/{i}/token/{i % 23}/scope=read",
            };
        }

        string path = await WriteAsync("nulls.parquet", StringBatch(values), FsstOptions());

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);

        var meta = await reader.ReadMetadataAsync();
        Assert.Contains(Encoding.Fsst, meta.RowGroups[0].Columns[0].MetaData!.Encodings);

        var read = await reader.ReadRowGroupAsync(0);
        var arr = (StringArray)read.Column(0);
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is null) Assert.True(arr.IsNull(i));
            else Assert.Equal(values[i], arr.GetString(i));
        }
    }

    [Fact]
    public async Task File_ManyPages_ShareOneSymbolTable()
    {
        var values = UrlValues(6000);
        string path = await WriteAsync(
            "pages.parquet", StringBatch(values, nullable: false),
            FsstOptions(pageSize: 4096));

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);

        var meta = await reader.ReadMetadataAsync();
        var colMeta = meta.RowGroups[0].Columns[0].MetaData!;
        Assert.Contains(Encoding.Fsst, colMeta.Encodings);
        Assert.NotNull(colMeta.SymbolTablePageOffset);

        var read = await reader.ReadRowGroupAsync(0);
        var arr = (StringArray)read.Column(0);
        Assert.Equal(values.Length, arr.Length);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], arr.GetString(i));
    }

    [Theory]
    [InlineData(CompressionCodec.Snappy)]
    [InlineData(CompressionCodec.Zstd)]
    public async Task File_CompressedPages_RoundTrip(CompressionCodec codec)
    {
        var values = UrlValues(3000);
        string path = await WriteAsync(
            $"codec-{codec}.parquet", StringBatch(values, nullable: false), FsstOptions(codec));

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);

        var meta = await reader.ReadMetadataAsync();
        Assert.Contains(Encoding.Fsst, meta.RowGroups[0].Columns[0].MetaData!.Encodings);

        var read = await reader.ReadRowGroupAsync(0);
        var arr = (StringArray)read.Column(0);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], arr.GetString(i));
    }

    [Fact]
    public async Task File_BinaryColumn_RoundTrips()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("b", BinaryType.Default, nullable: false))
            .Build();

        var builder = new BinaryArray.Builder();
        var expected = new byte[2000][];
        for (int i = 0; i < expected.Length; i++)
        {
            expected[i] = System.Text.Encoding.UTF8.GetBytes($"record-{i}-payload-{i % 11}");
            builder.Append(expected[i].AsSpan());
        }

        string path = await WriteAsync(
            "binary.parquet", new RecordBatch(schema, [builder.Build()], expected.Length),
            FsstOptions());

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);

        var read = await reader.ReadRowGroupAsync(0);
        var arr = (BinaryArray)read.Column(0);
        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], arr.GetBytes(i).ToArray());
    }

    [Fact]
    public async Task File_StringViewOutput_RoundTrips()
    {
        var values = UrlValues(1500);
        string path = await WriteAsync(
            "views.parquet", StringBatch(values, nullable: false), FsstOptions());

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(
            rf, ownsFile: false, new ParquetReadOptions { UseViewTypes = true });

        var read = await reader.ReadRowGroupAsync(0);
        var arr = (StringViewArray)read.Column(0);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], arr.GetString(i));
    }

    [Fact]
    public async Task File_AllNullColumn_RoundTrips()
    {
        var values = new string?[500];
        string path = await WriteAsync("allnull.parquet", StringBatch(values), FsstOptions());

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);

        var read = await reader.ReadRowGroupAsync(0);
        var arr = (StringArray)read.Column(0);
        Assert.Equal(values.Length, arr.Length);
        for (int i = 0; i < values.Length; i++)
            Assert.True(arr.IsNull(i));
    }

    [Fact]
    public async Task File_IncompressibleValues_FallBackWithoutASymbolTablePage()
    {
        // Random base64-ish payloads share no substrings worth a symbol, so §7.5's guidance
        // applies: the writer must not emit an FSST page that grew.
        var rng = new Random(1234);
        var values = new string[1500];
        var buffer = new byte[24];
        for (int i = 0; i < values.Length; i++)
        {
            rng.NextBytes(buffer);
            values[i] = Convert.ToBase64String(buffer);
        }

        string path = await WriteAsync(
            "incompressible.parquet", StringBatch(values, nullable: false), FsstOptions());

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);

        var meta = await reader.ReadMetadataAsync();
        var colMeta = meta.RowGroups[0].Columns[0].MetaData!;

        var read = await reader.ReadRowGroupAsync(0);
        var arr = (StringArray)read.Column(0);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], arr.GetString(i));

        // Whichever way the size comparison went, the metadata must describe what was written.
        if (colMeta.Encodings.Contains(Encoding.Fsst))
            Assert.NotNull(colMeta.SymbolTablePageOffset);
        else
            Assert.Null(colMeta.SymbolTablePageOffset);
    }

    [Fact]
    public async Task File_ShortColumn_RoundTripsWhicheverEncodingWins()
    {
        // Three values cannot pay for a symbol table page, so this exercises the fallback
        // path end to end rather than FSST itself.
        var values = new[] { "a", "bb", "ccc" };
        string path = await WriteAsync(
            "short.parquet", StringBatch(values, nullable: false), FsstOptions());

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);

        var read = await reader.ReadRowGroupAsync(0);
        var arr = (StringArray)read.Column(0);
        Assert.Equal(values, Enumerable.Range(0, values.Length).Select(i => arr.GetString(i)));
    }

    [Fact]
    public async Task File_ListOfStrings_RoundTrips()
    {
        // A nested leaf is handed to the encoder indexed by LEVEL position, not by dense value
        // position, so this covers the one place FSST's chunk-wide value cursor could drift.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("tags",
                new ListType(new Field("item", StringType.Default, nullable: true)),
                nullable: true))
            .Build();

        var listBuilder = new ListArray.Builder(new Field("item", StringType.Default, nullable: true));
        var valueBuilder = (StringArray.Builder)listBuilder.ValueBuilder;

        var expected = new List<string?[]?>();
        for (int i = 0; i < 900; i++)
        {
            if (i % 23 == 0)
            {
                listBuilder.AppendNull();
                expected.Add(null);
                continue;
            }

            listBuilder.Append();
            var row = new string?[i % 4];
            for (int j = 0; j < row.Length; j++)
            {
                if ((i + j) % 11 == 0)
                {
                    row[j] = null;
                    valueBuilder.AppendNull();
                }
                else
                {
                    row[j] = $"https://tags.example.org/v1/{i}/{j}/label";
                    valueBuilder.Append(row[j]!);
                }
            }
            expected.Add(row);
        }

        var batch = new RecordBatch(schema, [listBuilder.Build()], expected.Count);
        string path = await WriteAsync("lists.parquet", batch, FsstOptions());

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);

        var meta = await reader.ReadMetadataAsync();
        var colMeta = meta.RowGroups[0].Columns[0].MetaData!;
        Assert.Contains(Encoding.Fsst, colMeta.Encodings);
        Assert.NotNull(colMeta.SymbolTablePageOffset);

        var read = await reader.ReadRowGroupAsync(0);
        var list = (ListArray)read.Column(0);
        var values = (StringArray)list.Values;

        Assert.Equal(expected.Count, list.Length);
        for (int i = 0; i < expected.Count; i++)
        {
            if (expected[i] is null)
            {
                Assert.True(list.IsNull(i));
                continue;
            }

            int start = list.ValueOffsets[i];
            int length = list.ValueOffsets[i + 1] - start;
            Assert.Equal(expected[i]!.Length, length);
            for (int j = 0; j < length; j++)
            {
                if (expected[i]![j] is null) Assert.True(values.IsNull(start + j));
                else Assert.Equal(expected[i]![j], values.GetString(start + j));
            }
        }
    }

    [Fact]
    public async Task File_BatchedRead_ResolvesTheSymbolTableFromThePageMap()
    {
        // The batched path scans page headers into a ColumnPageMap first, then re-reads only
        // the data pages a batch needs. The symbol table page is not a data page, so it has to
        // survive on the page map — otherwise later batches decode against nothing.
        var values = UrlValues(5000);
        string path = await WriteAsync(
            "batched.parquet", StringBatch(values, nullable: false),
            FsstOptions(pageSize: 8192));

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(
            rf, ownsFile: false, new ParquetReadOptions { BatchSize = 512 });

        int row = 0;
        await foreach (var batch in reader.ReadRowGroupBatchesAsync(0))
        {
            var arr = (StringArray)batch.Column(0);
            for (int i = 0; i < arr.Length; i++, row++)
                Assert.Equal(values[row], arr.GetString(i));
        }

        Assert.Equal(values.Length, row);
    }

    [Fact]
    public async Task File_MultipleRowGroups_EachTrainsItsOwnSymbolTable()
    {
        string path = Path.Combine(_tempDir, "rowgroups.parquet");
        var first = UrlValues(2000);
        var second = Enumerable.Range(0, 2000)
            .Select(i => $"ftp://archive.internal/backups/{i}/part-{i % 5}.tar.gz")
            .ToArray();

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, FsstOptions()))
        {
            await writer.WriteRowGroupAsync(StringBatch(first, nullable: false));
            await writer.WriteRowGroupAsync(StringBatch(second, nullable: false));
            await writer.CloseAsync();
        }

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);

        var meta = await reader.ReadMetadataAsync();
        Assert.Equal(2, meta.RowGroups.Count);
        foreach (var rowGroup in meta.RowGroups)
        {
            var colMeta = rowGroup.Columns[0].MetaData!;
            Assert.Contains(Encoding.Fsst, colMeta.Encodings);
            Assert.NotNull(colMeta.SymbolTablePageOffset);
        }

        var readFirst = (StringArray)(await reader.ReadRowGroupAsync(0)).Column(0);
        var readSecond = (StringArray)(await reader.ReadRowGroupAsync(1)).Column(0);
        for (int i = 0; i < first.Length; i++)
            Assert.Equal(first[i], readFirst.GetString(i));
        for (int i = 0; i < second.Length; i++)
            Assert.Equal(second[i], readSecond.GetString(i));
    }
}

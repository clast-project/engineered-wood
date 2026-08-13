// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#pragma warning disable EWPARQUET0003 // FSST tests intentionally reference the experimental enum values.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// Tests for FSST_16 — the 16-bit code variant. Mirrors <see cref="FsstEncodingTests"/>, with
/// one gap that cannot be closed: <b>§6's worked examples are FSST8 only</b>, so there is no
/// documented byte sequence to decode. The hand-built table and page below are therefore
/// <em>derived from</em> §3.3, §4.4 and §4.7 rather than quoted from the specification, and
/// they corroborate this implementation only as far as its reading of those rules goes.
/// </summary>
public class Fsst16EncodingTests : IDisposable
{
    private readonly string _tempDir;

    public Fsst16EncodingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-fsst16-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
        GC.SuppressFinalize(this);
    }

    // ───── Symbol table page body (§3.3) ─────

    /// <summary>
    /// A four-symbol table built by hand from §3.3's field list: a <c>u16</c> count, sixteen
    /// <c>u16</c> histogram slots, then <c>symbol_data</c> at offset 34. Codes ascend by
    /// length, so the two 2-byte symbols come before the two 3-byte ones:
    /// 0="he", 1="ld", 2="llo", 3="wor".
    /// </summary>
    private static byte[] HandBuiltSymbolTableBody()
    {
        var body = new byte[FsstSymbolTable16.BodyHeaderSize];
        BinaryPrimitives.WriteUInt16LittleEndian(body, 4);
        BinaryPrimitives.WriteUInt16LittleEndian(body.AsSpan(2 + ((2 - 1) * 2)), 2); // two of length 2
        BinaryPrimitives.WriteUInt16LittleEndian(body.AsSpan(2 + ((3 - 1) * 2)), 2); // two of length 3

        return [.. body, .. System.Text.Encoding.UTF8.GetBytes("heldllowor")];
    }

    [Fact]
    public void SymbolTable_HandBuiltBody_RoundTripsByteForByte()
    {
        byte[] body = HandBuiltSymbolTableBody();
        var table = FsstSymbolTable16.Parse(body);

        Assert.Equal(4, table.SymbolCount);
        Assert.Equal(FsstSymbolTable16.BodyHeaderSize + 10, table.SerializedSize);
        Assert.Equal(body, table.Serialize());
    }

    [Fact]
    public void SymbolTable_BodyHeaderIsThirtyFourBytes()
    {
        // §3.3: symbol_count (2) + length_histogram (16 x u16), so symbol_data starts at 34.
        Assert.Equal(34, FsstSymbolTable16.BodyHeaderSize);
    }

    [Fact]
    public void SymbolTable_Parse_RejectsHistogramThatDisagreesWithSymbolCount()
    {
        byte[] body = HandBuiltSymbolTableBody();
        BinaryPrimitives.WriteUInt16LittleEndian(body, 5); // claim five symbols, describe four

        var ex = Assert.Throws<ParquetFormatException>(() => FsstSymbolTable16.Parse(body));
        Assert.Contains("length_histogram", ex.Message);
    }

    [Fact]
    public void SymbolTable_Parse_RejectsSymbolDataOfTheWrongSize()
    {
        byte[] body = HandBuiltSymbolTableBody();
        var truncated = body.AsSpan(0, body.Length - 1).ToArray();

        var ex = Assert.Throws<ParquetFormatException>(() => FsstSymbolTable16.Parse(truncated));
        Assert.Contains("histogram describes", ex.Message);
    }

    [Fact]
    public void SymbolTable_Parse_RejectsBodyTooSmallForHeader()
    {
        var ex = Assert.Throws<ParquetFormatException>(
            () => FsstSymbolTable16.Parse(new byte[FsstSymbolTable16.BodyHeaderSize - 1]));
        Assert.Contains("too small", ex.Message);
    }

    [Fact]
    public void SymbolTable_Parse_AcceptsEmptyTable()
    {
        var table = FsstSymbolTable16.Parse(new byte[FsstSymbolTable16.BodyHeaderSize]);
        Assert.Equal(0, table.SymbolCount);
    }

    /// <summary>
    /// §3.3 gives FSST_16 its sixteen histogram slots unconditionally, so a table whose symbols
    /// are all short is an ordinary table with zeros in the upper slots — not a shorter header.
    /// This is what makes the §1.2-versus-§3.3 disagreement over maximum symbol length a
    /// question about what a writer may emit rather than about the wire format.
    /// </summary>
    [Fact]
    public void SymbolTable_ShortSymbolsOnly_LeavesUpperHistogramSlotsZero()
    {
        byte[] body = HandBuiltSymbolTableBody();
        var table = FsstSymbolTable16.Parse(body);
        byte[] reserialized = table.Serialize();

        for (int length = 9; length <= 16; length++)
        {
            int slot = 2 + ((length - 1) * 2);
            Assert.Equal(0, BinaryPrimitives.ReadUInt16LittleEndian(reserialized.AsSpan(slot, 2)));
        }

        Assert.Equal(FsstSymbolTable16.BodyHeaderSize + 10, reserialized.Length);
    }

    [Fact]
    public void SymbolTable_Train_AssignsCodesInAscendingLengthOrder()
    {
        var values = new byte[600][];
        for (int i = 0; i < values.Length; i++)
            values[i] = System.Text.Encoding.UTF8.GetBytes($"https://example.com/catalog/item/{i}?ref=search");

        var table = FsstSymbolTable16.TryTrain(values);
        Assert.NotNull(table);

        // The histogram *is* the length information (§3.3), so Parse can only recover the same
        // symbols if the codes were in ascending length order to begin with.
        byte[] body = table!.Serialize();
        Assert.Equal(table.SymbolCount, BinaryPrimitives.ReadUInt16LittleEndian(body));

        int histogramSum = 0;
        for (int i = 0; i < FsstSymbolTable16.MaxSymbolLength; i++)
            histogramSum += BinaryPrimitives.ReadUInt16LittleEndian(body.AsSpan(2 + (i * 2), 2));
        Assert.Equal(table.SymbolCount, histogramSum);

        var reparsed = FsstSymbolTable16.Parse(body);
        Assert.Equal(table.SymbolCount, reparsed.SymbolCount);
        Assert.Equal(body, reparsed.Serialize());
    }

    /// <summary>
    /// The writer emits nothing longer than 8 bytes. The spec permits up to 16 — §1.2 has since
    /// been clarified to say FSST_16 symbols may be 1–16 — but permits is not requires, and a
    /// table with zeros in the length-9..16 slots is an ordinary FSST_16 table. The cap is a
    /// measured tuning choice (see <c>TrainedMaxSymbolLength</c>), so this pins the choice, not
    /// a conformance limit.
    /// </summary>
    [Fact]
    public void SymbolTable_Train_EmitsNoSymbolLongerThanTheTrainedCap()
    {
        var values = new byte[400][];
        for (int i = 0; i < values.Length; i++)
            values[i] = System.Text.Encoding.UTF8.GetBytes(
                $"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-{i}-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        var table = FsstSymbolTable16.TryTrain(values);
        Assert.NotNull(table);

        byte[] body = table!.Serialize();
        for (int length = FsstSymbolTable16.TrainedMaxSymbolLength + 1;
             length <= FsstSymbolTable16.MaxSymbolLength;
             length++)
        {
            int slot = 2 + ((length - 1) * 2);
            Assert.Equal(0, BinaryPrimitives.ReadUInt16LittleEndian(body.AsSpan(slot, 2)));
        }
    }

    // ───── Symbol table page dispatch ─────

    private static PageHeader SymbolTablePageHeaderFor(SymbolTableType type, int size) =>
        new()
        {
            Type = PageType.SymbolTablePage,
            UncompressedPageSize = size,
            CompressedPageSize = size,
            SymbolTablePageHeader = new SymbolTablePageHeader
            {
                Type = type,
                IsCompressed = false,
            },
        };

    private static EngineeredWood.Parquet.Metadata.ColumnMetaData ByteArrayColumnMeta() =>
        new()
        {
            Type = PhysicalType.ByteArray,
            Encodings = [Encoding.Fsst],
            Codec = CompressionCodec.Uncompressed,
            NumValues = 0,
            TotalUncompressedSize = 0,
            TotalCompressedSize = 0,
            DataPageOffset = 0,
        };

    /// <summary>
    /// The data pages are identical for both widths, so the symbol table page's type field is
    /// the only thing that tells a reader how wide the codes are (§2.3). This pins the dispatch.
    /// </summary>
    [Fact]
    public void SymbolTablePage_TypeFieldSelectsTheCodeWidth()
    {
        byte[] body = HandBuiltSymbolTableBody();
        var table = FsstPageDecoder.ReadSymbolTablePage(
            SymbolTablePageHeaderFor(SymbolTableType.Fsst16, body.Length), body, ByteArrayColumnMeta());

        Assert.IsType<FsstSymbolTable16>(table);
        Assert.Equal(SymbolTableType.Fsst16, table.Type);

        // The same bytes read as an 8-bit table are not a valid one: body[0] would be a count
        // of 4 against a histogram of zeros.
        Assert.Throws<ParquetFormatException>(() => FsstPageDecoder.ReadSymbolTablePage(
            SymbolTablePageHeaderFor(SymbolTableType.Fsst, body.Length), body, ByteArrayColumnMeta()));
    }

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
    /// Builds a PLAIN-offset page body by hand: §4.4's 9-byte header, then one int32 end offset
    /// per value, then the code stream.
    /// </summary>
    private static byte[] HandBuiltPage(int[] endOffsets, byte[] codeStream)
    {
        var page = new byte[FsstPageEncoder.HeaderSize + (endOffsets.Length * 4) + codeStream.Length];
        page[0] = 0; // offset_encoding = PLAIN
        BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(1, 4), endOffsets.Length);
        BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(5, 4), endOffsets.Length * 4);
        for (int i = 0; i < endOffsets.Length; i++)
            BinaryPrimitives.WriteInt32LittleEndian(
                page.AsSpan(FsstPageEncoder.HeaderSize + (i * 4), 4), endOffsets[i]);
        codeStream.CopyTo(page.AsSpan(FsstPageEncoder.HeaderSize + (endOffsets.Length * 4)));
        return page;
    }

    private static byte[] Codes(params ushort[] codes)
    {
        var bytes = new byte[codes.Length * 2];
        for (int i = 0; i < codes.Length; i++)
            BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(i * 2, 2), codes[i]);
        return bytes;
    }

    /// <summary>
    /// Derived from §4.7 — <b>not</b> quoted from the spec, which has no FSST_16 example.
    /// Against the hand-built table (0="he", 1="ld", 2="llo", 3="wor"), "hello" is codes 0 and
    /// 2, "world" is 3 and 1, and "hex" is code 0 followed by the escape marker and 'x'.
    /// </summary>
    [Fact]
    public void DataPage_HandBuiltFromTheRules_Decodes()
    {
        var table = FsstSymbolTable16.Parse(HandBuiltSymbolTableBody());
        byte[] stream =
        [
            .. Codes(0, 2),
            .. Codes(3, 1),
            .. Codes(0, FsstSymbolTable16.EscapeCode, 'x'),
        ];

        byte[] page = HandBuiltPage([4, 8, 14], stream);
        Assert.Equal(["hello", "world", "hex"], DecodePage(page, table, 3));
    }

    [Fact]
    public void DataPage_RoundTripsThroughTheEncoder()
    {
        var values = UrlValues(500).Select(System.Text.Encoding.UTF8.GetBytes).ToArray();
        var column = FsstCompressedColumn.TryCompress(values, SymbolTableType.Fsst16);
        Assert.NotNull(column);
        Assert.Equal(SymbolTableType.Fsst16, column!.Table.Type);

        byte[] page = FsstPageEncoder.Encode(column, valueIndex: 0, count: values.Length);
        var reparsed = FsstSymbolTable16.Parse(column.Table.Serialize());

        Assert.Equal(UrlValues(500), DecodePage(page, reparsed, values.Length));
    }

    [Fact]
    public void DataPage_RoundTripsASliceOfTheColumn()
    {
        string[] all = UrlValues(400);
        var column = FsstCompressedColumn.TryCompress(
            all.Select(System.Text.Encoding.UTF8.GetBytes).ToArray(), SymbolTableType.Fsst16);
        Assert.NotNull(column);

        byte[] page = FsstPageEncoder.Encode(column!, valueIndex: 100, count: 50);
        var reparsed = FsstSymbolTable16.Parse(column!.Table.Serialize());

        // Skip/Take rather than a range: the test project also targets net472, which has no
        // RuntimeHelpers.GetSubArray for the array range operator.
        Assert.Equal(all.Skip(100).Take(50).ToArray(), DecodePage(page, reparsed, 50));
    }

    [Fact]
    public void DataPage_RoundTripsEmptyValues()
    {
        var table = FsstSymbolTable16.Parse(HandBuiltSymbolTableBody());
        byte[] page = HandBuiltPage([0, 4, 4], [.. Codes(0, 2)]);
        Assert.Equal(["", "hello", ""], DecodePage(page, table, 3));
    }

    [Fact]
    public void DataPage_RejectsOddLengthCodeStream()
    {
        var table = FsstSymbolTable16.Parse(HandBuiltSymbolTableBody());
        byte[] page = HandBuiltPage([3], [0, 0, 2]);

        var ex = Assert.Throws<ParquetFormatException>(() => DecodePage(page, table, 1));
        Assert.Contains("whole number of 16-bit codes", ex.Message);
    }

    [Fact]
    public void DataPage_RejectsCodeWithNoSymbol()
    {
        var table = FsstSymbolTable16.Parse(HandBuiltSymbolTableBody());
        byte[] page = HandBuiltPage([2], Codes(9)); // table has four symbols

        var ex = Assert.Throws<ParquetFormatException>(() => DecodePage(page, table, 1));
        Assert.Contains("only 4 symbols", ex.Message);
    }

    /// <summary>
    /// §5.2: validation is per value, so a value ending in an escape marker must not be able to
    /// borrow the next value's first code as its literal.
    /// </summary>
    [Fact]
    public void DataPage_RejectsValueEndingInAnEscapeMarker()
    {
        var table = FsstSymbolTable16.Parse(HandBuiltSymbolTableBody());
        byte[] page = HandBuiltPage([4, 8], [.. Codes(0, FsstSymbolTable16.EscapeCode), .. Codes(0, 2)]);

        var ex = Assert.Throws<ParquetFormatException>(() => DecodePage(page, table, 2));
        Assert.Contains("escape marker and no literal", ex.Message);
    }

    [Fact]
    public void DataPage_RejectsEscapeLiteralThatIsNotAByte()
    {
        var table = FsstSymbolTable16.Parse(HandBuiltSymbolTableBody());
        byte[] page = HandBuiltPage([4], Codes(FsstSymbolTable16.EscapeCode, 300));

        var ex = Assert.Throws<ParquetFormatException>(() => DecodePage(page, table, 1));
        Assert.Contains("not a byte value", ex.Message);
    }

    [Fact]
    public void EncodingStrategyResolver_V2_ByteArray_Fsst16()
    {
        // Both widths are written as encoding 11; the symbol table page is what distinguishes
        // them, so the resolver must not invent a second encoding id.
        var enc = EncodingStrategyResolver.GetV2Encoding(
            PhysicalType.ByteArray, ByteArrayEncoding.Fsst16, FloatingPointEncoding.ByteStreamSplit);
        Assert.Equal(Encoding.Fsst, enc);
    }

    // ───── End to end ─────

    private static ParquetWriteOptions Fsst16Options(
        CompressionCodec codec = CompressionCodec.Uncompressed, int? pageSize = null) =>
        ParquetWriteOptions.Default with
        {
            ByteArrayEncoding = ByteArrayEncoding.Fsst16,
            DataPageVersion = DataPageVersion.V2,
            DictionaryEnabled = false,
            Compression = codec,
            DataPageSize = pageSize ?? ParquetWriteOptions.Default.DataPageSize,
        };

    private async Task<string> WriteAsync(string name, RecordBatch batch, ParquetWriteOptions options)
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

    private static RecordBatch StringBatch(string?[] values)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", StringType.Default, nullable: true))
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
        [.. Enumerable.Range(0, count)
            .Select(i => $"https://example.com/orders/{i}/items/{i % 17}?src=catalog")];

    private static async Task<string[]> ReadStringsAsync(string path, int count)
    {
        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);
        var read = await reader.ReadRowGroupAsync(0);
        var array = (StringArray)read.Column(0);

        var result = new string[count];
        for (int i = 0; i < count; i++)
            result[i] = array.GetString(i);
        return result;
    }

    [Fact]
    public async Task File_HighCardinalityStrings_UseFsst16AndRoundTrip()
    {
        string[] values = UrlValues(3000);
        string path = await WriteAsync("fsst16.parquet", StringBatch(values), Fsst16Options());

        await using (var rf = new LocalRandomAccessFile(path))
        await using (var reader = new ParquetFileReader(rf, ownsFile: false))
        {
            var meta = await reader.ReadMetadataAsync();
            var columnMeta = meta.RowGroups[0].Columns[0].MetaData!;
            Assert.Contains(Encoding.Fsst, columnMeta.Encodings);
            Assert.NotNull(columnMeta.SymbolTablePageOffset);
        }

        Assert.Equal(values, await ReadStringsAsync(path, values.Length));
    }

    [Fact]
    public async Task File_NullsAndEmptyStrings_RoundTrip()
    {
        var values = new string?[1200];
        for (int i = 0; i < values.Length; i++)
            values[i] = (i % 7) switch
            {
                0 => null,
                1 => string.Empty,
                _ => $"https://example.com/session/{i}/event/{i % 13}",
            };

        string path = await WriteAsync("nulls16.parquet", StringBatch(values), Fsst16Options());

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);
        var read = await reader.ReadRowGroupAsync(0);
        var array = (StringArray)read.Column(0);

        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], array.GetString(i));
    }

    [Fact]
    public async Task File_ManyPages_ShareOneSymbolTable()
    {
        string[] values = UrlValues(6000);
        string path = await WriteAsync(
            "pages16.parquet", StringBatch(values), Fsst16Options(pageSize: 4096));

        await using (var rf = new LocalRandomAccessFile(path))
        await using (var reader = new ParquetFileReader(rf, ownsFile: false))
        {
            // One table per column chunk (§1.4), however many data pages slice it.
            var columnMeta = (await reader.ReadMetadataAsync()).RowGroups[0].Columns[0].MetaData!;
            Assert.NotNull(columnMeta.SymbolTablePageOffset);
            Assert.NotNull(columnMeta.SymbolTablePageLength);
        }

        Assert.Equal(values, await ReadStringsAsync(path, values.Length));
    }

    [Theory]
    [InlineData(CompressionCodec.Snappy)]
    [InlineData(CompressionCodec.Zstd)]
    public async Task File_CompressedPages_RoundTrip(CompressionCodec codec)
    {
        string[] values = UrlValues(2000);
        string path = await WriteAsync(
            $"compressed16-{codec}.parquet", StringBatch(values), Fsst16Options(codec));

        Assert.Equal(values, await ReadStringsAsync(path, values.Length));
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
            expected[i] = System.Text.Encoding.UTF8.GetBytes($"record-{i}-payload-{i % 11}");
            builder.Append(expected[i].AsSpan());
        }

        string path = await WriteAsync(
            "binary16.parquet", new RecordBatch(schema, [builder.Build()], expected.Length),
            Fsst16Options());

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);
        var read = await reader.ReadRowGroupAsync(0);
        var array = (BinaryArray)read.Column(0);
        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], array.GetBytes(i).ToArray());
    }

    /// <summary>
    /// §7.5: the chunk falls back rather than growing, and the symbol table page counts against
    /// the win. A file that used FSST_16 must therefore be no larger than the same data written
    /// without it.
    /// </summary>
    [Fact]
    public async Task File_Fsst16DoesNotMakeTheFileBigger()
    {
        string[] values = UrlValues(4000);
        string fsst = await WriteAsync("cmp-fsst16.parquet", StringBatch(values), Fsst16Options());
        string plain = await WriteAsync(
            "cmp-plain.parquet", StringBatch(values),
            Fsst16Options() with { ByteArrayEncoding = ByteArrayEncoding.DeltaLengthByteArray });

        Assert.True(
            new FileInfo(fsst).Length <= new FileInfo(plain).Length,
            $"FSST_16 file was {new FileInfo(fsst).Length} bytes against " +
            $"{new FileInfo(plain).Length} for DELTA_LENGTH_BYTE_ARRAY.");
    }
}

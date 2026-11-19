// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// <see cref="BufferedParquetWriter"/> applies <see cref="ParquetWriteOptions.DictionaryPageSizeLimit"/> as
/// <see cref="ParquetFileWriter"/> does: a dictionary that passes it is declined if it filled before the first page
/// decided, and otherwise kept for the pages before the cut, the rest in the column's own encoding. The buffered
/// writer gets the rows in several batches; both files' column chunks must come out byte for byte the same.
/// </summary>
public class BufferedDictionaryLimitTests : IDisposable
{
    private const int Limit = 8 * 1024;

    private readonly string _tempDir;

    public BufferedDictionaryLimitTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-buffered-dict-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch (IOException) { }
    }

    public static TheoryData<string, DataPageVersion, bool> Cases()
    {
        var data = new TheoryData<string, DataPageVersion, bool>();
        foreach (var shape in new[] { "int64", "string", "list" })
        foreach (var version in new[] { DataPageVersion.V1, DataPageVersion.V2 })
        foreach (bool nullable in new[] { false, true })
            data.Add(shape, version, nullable);
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task OverTheLimit_MatchesTheFileWriter(string shape, DataPageVersion version, bool nullable)
    {
        // Low-cardinality for the first MiB of values, then distinct: the dictionary fills after the first page
        // decides, so the file writer keeps it partway.
        var batch = shape switch
        {
            "int64" => Int64Batch(300_000, 200_000, nullable),
            "string" => StringBatch(200_000, 150_000, nullable),
            _ => ListBatch(100_000, 70_000, nullable),
        };

        var (direct, buffered) = await WriteBothAsync(batch, version, Limit);
        var chunk = await AssertSameChunksAsync(direct, buffered);
        Assert.NotNull(chunk.DictionaryPageOffset);
        Assert.Contains(chunk.EncodingStats!, s => s.PageType is PageType.DataPage or PageType.DataPageV2
            && s.Encoding != Encoding.RleDictionary && s.Count > 0);
        await AssertSameRowsAsync(batch, buffered);
    }

    [Fact]
    public async Task FullBeforeTheFirstPageDecides_IsDeclinedByBoth()
    {
        // 100 distinct INT64s are 800 bytes of dictionary, over a 512-byte limit long before 1 MiB of values.
        var batch = Int64Batch(50_000, 50_000, nullable: false);
        var (direct, buffered) = await WriteBothAsync(batch, DataPageVersion.V1, limit: 512);
        var chunk = await AssertSameChunksAsync(direct, buffered);
        Assert.Null(chunk.DictionaryPageOffset);
        await AssertSameRowsAsync(batch, buffered);
    }

    [Fact]
    public async Task Decimal_UnderTheLimit_MatchesTheFileWriter()
    {
        // The byte-keyed dictionary is the file writer's table now; a decimal's entries still reverse to big-endian.
        var builder = new Decimal128Array.Builder(new Decimal128Type(30, 2));
        for (int i = 0; i < 20_000; i++)
            builder.Append((i % 300 - 150) * 1.25m);
        var array = builder.Build();
        var batch = new RecordBatch(new Apache.Arrow.Schema([new Field("c", array.Data.DataType, false)], null), [array], array.Length);

        var (direct, buffered) = await WriteBothAsync(batch, DataPageVersion.V1, limit: 1 << 20);
        var chunk = await AssertSameChunksAsync(direct, buffered);
        Assert.NotNull(chunk.DictionaryPageOffset);
        await AssertSameRowsAsync(batch, buffered);
    }

    public static TheoryData<string> Types() => new() { "int8", "int32", "int64", "float", "double", "string", "decimal", "list" };

    [Theory]
    [MemberData(nameof(Types))]
    public async Task DictionaryOff_MatchesTheFileWriter(string type)
    {
        // A column without a dictionary is buffered raw from its first value (no lookups), rebuilt at flush and
        // written as the file writer writes it.
        const int rows = 40_000;
        var random = new Random(7);
        IArrowArray array;
        switch (type)
        {
            case "int8":
                var i8 = new Int8Array.Builder();
                for (int i = 0; i < rows; i++) i8.Append((sbyte)random.Next(-128, 128));
                array = i8.Build();
                break;
            case "int32":
                var i32 = new Int32Array.Builder();
                for (int i = 0; i < rows; i++) i32.Append(random.Next(100));
                array = i32.Build();
                break;
            case "int64":
                array = Int64Batch(rows, rows / 2, nullable: true).Column(0);
                break;
            case "float":
                var f = new FloatArray.Builder();
                for (int i = 0; i < rows; i++) f.Append(i % 10 == 0 ? -0f : (float)random.NextDouble());
                array = f.Build();
                break;
            case "double":
                var d = new DoubleArray.Builder();
                for (int i = 0; i < rows; i++) d.Append(i % 10 == 0 ? double.NaN : random.NextDouble());
                array = d.Build();
                break;
            case "string":
                array = StringBatch(rows, rows / 2, nullable: true).Column(0);
                break;
            case "decimal":
                var m = new Decimal128Array.Builder(new Decimal128Type(30, 2));
                for (int i = 0; i < rows; i++) m.Append((i % 300 - 150) * 1.25m);
                array = m.Build();
                break;
            default:
                array = ListBatch(rows / 4, rows / 8, nullable: true).Column(0);
                break;
        }

        var batch = new RecordBatch(
            new Apache.Arrow.Schema([new Field("c", array.Data.DataType, array.NullCount > 0)], null), [array], array.Length);
        var (direct, buffered) = await WriteBothAsync(batch, DataPageVersion.V1, Limit, dictionary: false);
        var chunk = await AssertSameChunksAsync(direct, buffered);
        Assert.Null(chunk.DictionaryPageOffset);
        await AssertSameRowsAsync(batch, buffered);
    }

    [Fact]
    public async Task EachRowGroup_StartsWithADictionaryAgain()
    {
        // The first row group's dictionary passes the limit, so its tail is buffered raw; the second is
        // low-cardinality throughout and must get a whole dictionary, as each file-writer chunk does. It is small
        // (500 values, 4,000 bytes raw) so that, were it still buffered raw, it would stay under the limit and the
        // first-page test would decline the "dictionary" of one entry per value.
        var first = Int64Batch(300_000, 200_000, nullable: false);
        var builder = new Int64Array.Builder();
        for (int i = 0; i < 500; i++) builder.Append(i % 100);
        var second = Batch(builder.Build(), nullable: false);

        var (direct, buffered) = await WriteBothAsync([first, second], DataPageVersion.V1, Limit);
        var d = await ReadMetadataAsync(direct);
        var b = await ReadMetadataAsync(buffered);
        Assert.Equal(2, b.RowGroups.Count);
        for (int g = 0; g < 2; g++)
        {
            var dc = d.RowGroups[g].Columns[0].MetaData!;
            var bc = b.RowGroups[g].Columns[0].MetaData!;
            Assert.Equal(ChunkBytes(direct, dc), ChunkBytes(buffered, bc));
        }

        var whole = b.RowGroups[1].Columns[0].MetaData!;
        Assert.NotNull(whole.DictionaryPageOffset);
        Assert.All(whole.EncodingStats!.Where(s => s.PageType is PageType.DataPage or PageType.DataPageV2),
            s => Assert.Equal(Encoding.RleDictionary, s.Encoding));
    }

    [Fact]
    public async Task RepetitiveValues_Raw_TakeAnEntryEach()
    {
        // Without a dictionary every value is buffered raw, but against a small cache of recent values: four
        // distinct 4 KiB strings over 100,000 rows are four entries, not 410 MB.
        var big = Enumerable.Range(0, 4).Select(k => new string((char)('a' + k), 4096)).ToArray();
        var builder = new StringArray.Builder();
        for (int i = 0; i < 100_000; i++)
            builder.Append(big[i % 4]);
        var batch = Batch(builder.Build(), nullable: false);

        var options = ParquetWriteOptions.Default with { DictionaryEnabled = false, RowGroupMaxRows = 1_000_000 };
        await using (var writer = new BufferedParquetWriter(new LocalSequentialFile(Path.Combine(_tempDir, "repetitive.parquet")), ownsFile: true, options))
        {
#if NET8_0_OR_GREATER
            long before = GC.GetAllocatedBytesForCurrentThread();
#endif
            for (int start = 0; start < batch.Length; start += 10_000)
                await writer.AppendAsync(batch.Slice(start, 10_000));
#if NET8_0_OR_GREATER
            // Levels and indices are a few bytes a row; the values would be 410 MB.
            Assert.InRange(GC.GetAllocatedBytesForCurrentThread() - before, 0, 50L << 20);
#endif
            await writer.CloseAsync();
        }

        var (direct, buffered) = await WriteBothAsync(batch, DataPageVersion.V1, Limit, dictionary: false);
        await AssertSameChunksAsync(direct, buffered);
        await AssertSameRowsAsync(batch, buffered);
    }

    [Fact]
    public async Task ResetReleasesARawPage()
    {
        // A page that held a row group's raw values is as large as the column was; once the row group is flushed
        // it must not stay rooted for the writer's lifetime. Read through reflection: the state is private.
        var options = ParquetWriteOptions.Default with { DictionaryEnabled = false, RowGroupMaxRows = 1_000_000 };
        await using var writer = new BufferedParquetWriter(
            new LocalSequentialFile(Path.Combine(_tempDir, "reset.parquet")), ownsFile: true, options);
        await writer.AppendAsync(Int64Batch(300_000, 0, nullable: false)); // all distinct: 2.4 MB raw
        Assert.True(FixedPageLength(writer) > options.DictionaryPageSizeLimit);

        await writer.FlushRowGroupAsync();
        Assert.Equal(0, FixedPageLength(writer));
        await writer.CloseAsync();
    }

    private static int FixedPageLength(BufferedParquetWriter writer)
    {
        var flags = System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance;
        var states = (System.Array)typeof(BufferedParquetWriter).GetField("_columnStates", flags)!.GetValue(writer)!;
        var state = states.GetValue(0)!;
        return ((byte[])state.GetType().GetField("_fixedPage", flags)!.GetValue(state)!).Length;
    }

    private static RecordBatch Int64Batch(int rows, int lowRows, bool nullable)
    {
        var builder = new Int64Array.Builder();
        for (int i = 0; i < rows; i++)
        {
            if (nullable && i % 7 == 3) builder.AppendNull();
            else builder.Append(i < lowRows ? i % 100 : 10_000_000L + i);
        }

        return Batch(builder.Build(), nullable);
    }

    private static RecordBatch StringBatch(int rows, int lowRows, bool nullable)
    {
        var builder = new StringArray.Builder();
        for (int i = 0; i < rows; i++)
        {
            if (nullable && i % 9 == 5) builder.AppendNull();
            else builder.Append(i < lowRows ? "cat-" + (i % 50) : "unique-" + i);
        }

        return Batch(builder.Build(), nullable);
    }

    private static RecordBatch ListBatch(int rows, int lowRows, bool nullable)
    {
        var builder = new ListArray.Builder(Int64Type.Default);
        var values = (Int64Array.Builder)builder.ValueBuilder;
        for (int r = 0; r < rows; r++)
        {
            if (nullable && r % 11 == 4)
            {
                builder.AppendNull();
                continue;
            }

            builder.Append();
            for (int k = 0; k < 3; k++)
                values.Append(r < lowRows ? (r * 3 + k) % 101 : 10_000_000L + r * 3 + k);
        }

        return Batch(builder.Build(), nullable);
    }

    private static RecordBatch Batch(IArrowArray array, bool nullable) =>
        new(new Apache.Arrow.Schema([new Field("c", array.Data.DataType, nullable)], null), [array], array.Length);

    private Task<(string Direct, string Buffered)> WriteBothAsync(
        RecordBatch batch, DataPageVersion version, int limit, bool dictionary = true) =>
        WriteBothAsync([batch], version, limit, dictionary);

    /// <summary>
    /// Each batch as one row group through ParquetFileWriter, and through BufferedParquetWriter in several appends
    /// (so its dictionary accumulates across them), flushed at each batch's end.
    /// </summary>
    private async Task<(string Direct, string Buffered)> WriteBothAsync(
        RecordBatch[] groups, DataPageVersion version, int limit, bool dictionary = true)
    {
        var options = ParquetWriteOptions.Default with
        {
            DataPageVersion = version,
            DictionaryPageSizeLimit = limit,
            DictionaryEnabled = dictionary,
            RowGroupMaxRows = groups.Max(g => g.Length),
        };

        string direct = Path.Combine(_tempDir, $"direct-{Guid.NewGuid():N}.parquet");
        await using (var file = new LocalSequentialFile(direct))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            foreach (var group in groups)
                await writer.WriteRowGroupAsync(group);
            await writer.CloseAsync();
        }

        string buffered = Path.Combine(_tempDir, $"buffered-{Guid.NewGuid():N}.parquet");
        await using (var file = new LocalSequentialFile(buffered))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false, options))
        {
            const int Slice = 37_000;
            foreach (var group in groups)
            {
                for (int start = 0; start < group.Length; start += Slice)
                    await writer.AppendAsync(group.Slice(start, Math.Min(Slice, group.Length - start)));
                await writer.FlushRowGroupAsync();
            }

            await writer.CloseAsync();
        }

        return (direct, buffered);
    }

    /// <summary>One row group each, whose column chunks match: metadata that describes them, and their bytes.</summary>
    private static async Task<ColumnMetaData> AssertSameChunksAsync(string direct, string buffered)
    {
        var d = (await ReadMetadataAsync(direct)).RowGroups.Single().Columns.Single().MetaData!;
        var b = (await ReadMetadataAsync(buffered)).RowGroups.Single().Columns.Single().MetaData!;

        Assert.Equal(d.Encodings.OrderBy(e => e), b.Encodings.OrderBy(e => e));
        Assert.Equal(
            d.EncodingStats!.Select(s => (s.PageType, s.Encoding, s.Count)),
            b.EncodingStats!.Select(s => (s.PageType, s.Encoding, s.Count)));
        Assert.Equal(d.DictionaryPageOffset is null, b.DictionaryPageOffset is null);
        Assert.Equal(d.TotalCompressedSize, b.TotalCompressedSize);
        Assert.Equal(d.Statistics?.MinValue, b.Statistics?.MinValue);
        Assert.Equal(d.Statistics?.MaxValue, b.Statistics?.MaxValue);
        Assert.Equal(d.Statistics?.NullCount, b.Statistics?.NullCount);

        Assert.Equal(ChunkBytes(direct, d), ChunkBytes(buffered, b));
        return b;
    }

    private static byte[] ChunkBytes(string path, ColumnMetaData chunk)
    {
        long start = chunk.DictionaryPageOffset ?? chunk.DataPageOffset;
        return File.ReadAllBytes(path).AsSpan(checked((int)start), checked((int)chunk.TotalCompressedSize)).ToArray();
    }

    private static async Task AssertSameRowsAsync(RecordBatch expected, string path)
    {
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var actual = (await reader.ReadRowGroupAsync(0)).Column(0);
        var want = expected.Column(0);
        Assert.Equal(want.Length, actual.Length);
        for (int i = 0; i < want.Length; i++)
            Assert.Equal(Render(want, i), Render(actual, i));
    }

    private static string? Render(IArrowArray array, int i) => array.IsNull(i) ? null : array switch
    {
        Int64Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        StringArray a => a.GetString(i),
        Int8Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        Int32Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        FloatArray a => BitConverter.ToInt32(BitConverter.GetBytes(a.GetValue(i)!.Value), 0).ToString(System.Globalization.CultureInfo.InvariantCulture),
        DoubleArray a => BitConverter.DoubleToInt64Bits(a.GetValue(i)!.Value).ToString(System.Globalization.CultureInfo.InvariantCulture),
        Decimal128Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        ListArray a => string.Join(",", Enumerable.Range(a.ValueOffsets[i], a.GetValueLength(i))
            .Select(k => ((Int64Array)a.Values).GetValue(k))),
        _ => throw new InvalidOperationException(array.GetType().Name),
    };

    private static async Task<FileMetaData> ReadMetadataAsync(string path)
    {
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        return await reader.ReadMetadataAsync();
    }
}

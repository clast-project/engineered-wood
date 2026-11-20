// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// <see cref="ParquetFileWriter.WriteBatchAsync"/> (#936 step 2): row groups built from many batches, each encoded as it
/// arrives, cut by the writer at a row count or an estimate of their encoded size.
/// </summary>
public class StreamingRowGroupTests : IDisposable
{
    private readonly string _dir = Directory.CreateDirectory(
        System.IO.Path.Combine(System.IO.Path.GetTempPath(), "ew-streaming-" + Guid.NewGuid().ToString("N"))).FullName;

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { }
    }

    private string NewPath() => System.IO.Path.Combine(_dir, Guid.NewGuid().ToString("N") + ".parquet");

    // ───── Data ─────

    /// <summary>
    /// A batch of most shapes the column writer handles: integers and doubles (with NaN and -0) of low and high
    /// cardinality, nullable and required, strings of both, a constant run-end encoded string, a struct and a list.
    /// </summary>
    private static RecordBatch Batch(int rows, int seed)
    {
        var random = new Random(seed);
        var lowInt = new Int32Array.Builder();
        var highLong = new Int64Array.Builder();
        var nullableDouble = new DoubleArray.Builder();
        var lowString = new StringArray.Builder();
        var highString = new StringArray.Builder();
        var structInt = new Int32Array.Builder();
        var structValidity = new List<bool>();
        var listOffsets = new List<int> { 0 };
        var listValues = new Int32Array.Builder();
        var listValidity = new List<bool>();

        for (int i = 0; i < rows; i++)
        {
            lowInt.Append(random.Next(20));
            highLong.Append((((long)random.Next() << 33) ^ ((long)random.Next() << 2) ^ random.Next(4)));
            switch (random.Next(10))
            {
                case 0: nullableDouble.AppendNull(); break;
                case 1: nullableDouble.Append(double.NaN); break;
                case 2: nullableDouble.Append(-0.0); break;
                default: nullableDouble.Append(Math.Round(random.NextDouble() * 1000, 2)); break;
            }
            if (random.Next(7) == 0) lowString.AppendNull(); else lowString.Append("v" + random.Next(30));
            highString.Append(Guid.NewGuid().ToString("N").Substring(0, 4 + random.Next(20)) + seed);
            bool structValid = random.Next(8) != 0;
            structValidity.Add(structValid);
            structInt.Append(random.Next(1000));
            bool listValid = random.Next(9) != 0;
            listValidity.Add(listValid);
            int length = listValid ? random.Next(4) : 0;
            for (int k = 0; k < length; k++)
                listValues.Append(random.Next(100));
            listOffsets.Add(listOffsets[^1] + length);
        }

        var structType = new StructType([new Field("n", Int32Type.Default, nullable: false)]);
        var structArray = new StructArray(
            structType, rows, [structInt.Build()], Bitmap(structValidity, out int structNulls), structNulls);
        var listType = new ListType(new Field("element", Int32Type.Default, nullable: false));
        var listArray = new ListArray(
            listType, rows, new ArrowBuffer(System.Runtime.InteropServices.MemoryMarshal.AsBytes(listOffsets.ToArray().AsSpan()).ToArray()),
            listValues.Build(), Bitmap(listValidity, out int listNulls), listNulls);
        var constant = EngineeredWood.Arrow.RunEndEncoding.Constant(
            StringType.Default, System.Text.Encoding.UTF8.GetBytes("partition-value"), rows);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("low_int", Int32Type.Default, nullable: false))
            .Field(new Field("high_long", Int64Type.Default, nullable: false))
            .Field(new Field("nullable_double", DoubleType.Default, nullable: true))
            .Field(new Field("low_string", StringType.Default, nullable: true))
            .Field(new Field("high_string", StringType.Default, nullable: false))
            .Field(new Field("s", structType, nullable: true))
            .Field(new Field("l", listType, nullable: true))
            .Field(new Field("constant", constant.Data.DataType, nullable: true))
            .Build();
        return new RecordBatch(schema,
            [lowInt.Build(), highLong.Build(), nullableDouble.Build(), lowString.Build(), highString.Build(),
             structArray, listArray, constant],
            rows);
    }

    private static ArrowBuffer Bitmap(List<bool> valid, out int nullCount)
    {
        var bytes = new byte[(valid.Count + 7) / 8];
        nullCount = 0;
        for (int i = 0; i < valid.Count; i++)
        {
            if (valid[i]) bytes[i / 8] |= (byte)(1 << (i % 8));
            else nullCount++;
        }
        return new ArrowBuffer(bytes);
    }

    private static IEnumerable<RecordBatch> Pieces(RecordBatch batch, int seed)
    {
        var random = new Random(seed);
        int offset = 0;
        while (offset < batch.Length)
        {
            int left = batch.Length - offset;
            int rows = random.Next(4) switch
            {
                0 => Math.Min(left, 1 + random.Next(3)),
                1 => Math.Min(left, 1 + random.Next(200)),
                2 => Math.Min(left, 1 + random.Next(5000)),
                _ => Math.Min(left, 1 + random.Next(left)),
            };
            yield return batch.Slice(offset, rows);
            offset += rows;
        }
    }

    private async Task<string> WriteWholeAsync(RecordBatch batch, ParquetWriteOptions options)
    {
        string path = NewPath();
        await using var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private async Task<string> WriteStreamedAsync(IEnumerable<RecordBatch> batches, ParquetWriteOptions options)
    {
        string path = NewPath();
        await using var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true, options);
        foreach (var batch in batches)
            await writer.WriteBatchAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private static async Task<(FileMetaData Metadata, List<RecordBatch> Batches)> ReadAsync(string path)
    {
        await using var reader = new ParquetFileReader(new LocalRandomAccessFile(path));
        var metadata = await reader.ReadMetadataAsync();
        var batches = new List<RecordBatch>();
        await foreach (var batch in reader.ReadAllAsync())
            batches.Add(batch);
        return (metadata, batches);
    }

    /// <summary>A file's rows as text, column by column, NaN and -0 told apart from the other values.</summary>
    private static List<string> Rows(List<RecordBatch> batches)
    {
        var rows = new List<string>();
        foreach (var batch in batches)
        {
            for (int r = 0; r < batch.Length; r++)
            {
                var cells = new string[batch.ColumnCount];
                for (int c = 0; c < batch.ColumnCount; c++)
                    cells[c] = Cell(batch.Column(c), r);
                rows.Add(string.Join("|", cells));
            }
        }
        return rows;
    }

    private static string Cell(IArrowArray array, int row)
    {
        if (array is RunEndEncodedArray ree)
            return Cell(EngineeredWood.Arrow.RunEndEncoding.Expand(ree), row);
        if (array.IsNull(row))
            return "null";
        return array switch
        {
            Int32Array a => a.GetValue(row)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Int64Array a => a.GetValue(row)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
            DoubleArray a => BitConverter.DoubleToInt64Bits(a.GetValue(row)!.Value).ToString("x16"),
            StringArray a => a.GetString(row),
            StructArray s => "{" + string.Join(",", s.Fields.Select(f => Cell(f, row))) + "}",
            ListArray l => "[" + string.Join(",", Enumerable.Range(l.ValueOffsets[row], l.GetValueLength(row)).Select(i => Cell(l.Values, i))) + "]",
            _ => throw new NotSupportedException(array.GetType().Name),
        };
    }

    public static TheoryData<string> OptionSets() => ["default", "v1", "dictsmall", "nodict", "nopageindex", "bloom", "rowlimit"];

    private static ParquetWriteOptions Options(string name)
    {
        var options = new ParquetWriteOptions { RowGroupMaxRows = int.MaxValue, RowGroupMaxBytes = long.MaxValue };
        return name switch
        {
            "v1" => options with { DataPageVersion = DataPageVersion.V1 },
            "dictsmall" => options with { DictionaryPageSizeLimit = 2048 },
            "nodict" => options with { DictionaryEnabled = false },
            "nopageindex" => options with { WritePageIndex = false },
            "bloom" => options with { BloomFilterColumns = ["low_int", "high_long", "low_string", "high_string"], DictionaryPageSizeLimit = 4096 },
            "rowlimit" => options with { DataPageRowCountLimit = 333, DataPageSize = 4096 },
            _ => options,
        };
    }

    // ───── Round trip ─────

    [Theory]
    [MemberData(nameof(OptionSets))]
    public async Task ManyBatches_ReadBackAsOne_WithTheSameStatistics(string optionSet)
    {
        var options = Options(optionSet);
        var batch = Batch(40_000, seed: 7);
        string whole = await WriteWholeAsync(batch, options);
        string streamed = await WriteStreamedAsync(Pieces(batch, seed: 11), options);

        var (wholeMeta, wholeBatches) = await ReadAsync(whole);
        var (streamedMeta, streamedBatches) = await ReadAsync(streamed);

        Assert.Single(streamedMeta.RowGroups);
        Assert.Equal(Rows(wholeBatches), Rows(streamedBatches));

        // The chunk statistics describe the values, not how they arrived: merged per append, they are what one
        // scan of the whole gives.
        var wholeColumns = wholeMeta.RowGroups[0].Columns;
        var streamedColumns = streamedMeta.RowGroups[0].Columns;
        Assert.Equal(wholeColumns.Count, streamedColumns.Count);
        for (int c = 0; c < wholeColumns.Count; c++)
        {
            var expected = wholeColumns[c].MetaData!;
            var actual = streamedColumns[c].MetaData!;
            string column = string.Join(".", expected.PathInSchema);
            Assert.Equal(expected.NumValues, actual.NumValues);
            Assert.Equal(expected.UnencodedByteArrayDataBytes, actual.UnencodedByteArrayDataBytes);
            Assert.True(expected.Statistics?.NullCount == actual.Statistics?.NullCount, $"{column} null_count");
            Assert.True(expected.Statistics?.NanCount == actual.Statistics?.NanCount, $"{column} nan_count");
            Assert.True(Same(expected.Statistics?.MinValue, actual.Statistics?.MinValue), $"{column} min_value");
            Assert.True(Same(expected.Statistics?.MaxValue, actual.Statistics?.MaxValue), $"{column} max_value");
        }
    }

    private static bool Same(byte[]? a, byte[]? b) => a is null ? b is null : b is not null && a.AsSpan().SequenceEqual(b);

    [Fact]
    public async Task OneBatchPerRowGroup_IsByteIdenticalToWriteRowGroup()
    {
        // Without a Bloom filter (sized from a hint once batches stream) the streamed row group is the whole-batch
        // one: the open page and the held dictionary values are copied on each append, and the copies encode alike.
        var options = new ParquetWriteOptions { CreatedBy = "streaming-test" };
        var batches = new[] { Batch(5000, 1), Batch(3000, 2) };

        string whole = NewPath();
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(whole), ownsFile: true, options))
        {
            foreach (var batch in batches)
                await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        string streamed = NewPath();
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(streamed), ownsFile: true, options))
        {
            foreach (var batch in batches)
            {
                await writer.WriteBatchAsync(batch);
                await writer.FlushRowGroupAsync();
            }
            await writer.CloseAsync();
        }

        Assert.Equal(File.ReadAllBytes(whole), File.ReadAllBytes(streamed));
    }

    /// <summary>
    /// The sizes and counts in the footer of a streamed file are those of the pages in it: each chunk's compressed size
    /// spans its pages exactly, its uncompressed size and value count are the sum of theirs, and each row group's totals
    /// are its chunks' (#938 review). Row groups are cut by rows and by bytes, so chunks end mid-batch.
    /// </summary>
    [Theory]
    [InlineData("default", long.MaxValue)]
    [InlineData("default", 64_000)]
    [InlineData("v1", long.MaxValue)]
    [InlineData("dictsmall", 200_000)]
    [InlineData("nodict", 100_000)]
    [InlineData("nopageindex", long.MaxValue)]
    [InlineData("bloom", 300_000)]
    [InlineData("rowlimit", 50_000)]
    public async Task FooterSizes_AreThoseOfThePagesWritten(string optionSet, long maxBytes)
    {
        var options = Options(optionSet) with { RowGroupMaxRows = 7_000, RowGroupMaxBytes = maxBytes };
        string path = await WriteStreamedAsync(Pieces(Batch(20_000, 3), seed: 9), options);
        byte[] bytes = File.ReadAllBytes(path);
        var (metadata, _) = await ReadAsync(path);

        Assert.True(metadata.RowGroups.Count > 2);
        long previousEnd = 0;
        foreach (var group in metadata.RowGroups)
        {
            long groupUncompressed = 0, groupCompressed = 0;
            foreach (var chunk in group.Columns)
            {
                var md = chunk.MetaData!;
                long start = Math.Min(md.DictionaryPageOffset ?? long.MaxValue, md.DataPageOffset);
                Assert.True(start >= previousEnd, $"a chunk at {start} starts before the previous one ends at {previousEnd}");
                long position = start, end = start + md.TotalCompressedSize, uncompressed = 0, values = 0;
                while (position < end)
                {
                    var header = PageHeaderDecoder.Decode(bytes.AsSpan(checked((int)position)), out int headerLength);
                    position += headerLength + header.CompressedPageSize;
                    uncompressed += headerLength + header.UncompressedPageSize;
                    values += header.DataPageHeader?.NumValues ?? header.DataPageHeaderV2?.NumValues ?? 0;
                }
                Assert.Equal(end, position);
                Assert.Equal(md.TotalUncompressedSize, uncompressed);
                Assert.Equal(md.NumValues, values);
                groupUncompressed += md.TotalUncompressedSize;
                groupCompressed += md.TotalCompressedSize;
                previousEnd = end;
            }
            Assert.Equal(groupUncompressed, group.TotalByteSize);
            if (group.TotalCompressedSize is { } total)
                Assert.Equal(groupCompressed, total);
        }
    }

    [Fact]
    public async Task DictionaryThatFillsInALaterBatch_FallsBackMidChunk_AndKeepsEveryValue()
    {
        // Thirty batches of twenty distinct values pass the dictionary's first-page test (1 MiB of values); the next
        // brings thousands, and the dictionary freezes partway into it. Statistics and the Bloom filter must still
        // cover the first thirty, whose values only the frozen dictionary holds.
        var options = new ParquetWriteOptions
        {
            DictionaryPageSizeLimit = 1024,
            BloomFilterColumns = ["v"],
            RowGroupMaxRows = int.MaxValue,
        };
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", Int32Type.Default, nullable: false)).Build();
        var batches = new List<RecordBatch>();
        for (int b = 0; b < 30; b++)
            batches.Add(new RecordBatch(schema, [Int32Range(b * 10_000, 10_000, i => 1_000_000 + i % 20)], 10_000));
        batches.Add(new RecordBatch(schema, [Int32Range(0, 300_000, i => -5 - i)], 300_000));

        string path = await WriteStreamedAsync(batches, options);
        var (metadata, read) = await ReadAsync(path);
        var chunk = metadata.RowGroups[0].Columns[0].MetaData!;

        Assert.Contains(Encoding.RleDictionary, chunk.Encodings);
        Assert.Contains(chunk.Encodings, e => e is Encoding.DeltaBinaryPacked or Encoding.Plain);
        Assert.Equal(1_000_019, BitConverter.ToInt32(chunk.Statistics!.MaxValue!, 0));
        Assert.Equal(-5 - 299_999, BitConverter.ToInt32(chunk.Statistics.MinValue!, 0));
        Assert.Equal(600_000, read.Sum(b => b.Length));

        await using var reader = new ParquetFileReader(new LocalRandomAccessFile(path));
        foreach (int value in new[] { 1_000_000, 1_000_019, -5, -150_000, -5 - 299_999 })
            Assert.True((await reader.GetCandidateRowGroupsAsync("v", value))[0], $"{value} not in the Bloom filter");
    }

    /// <summary>
    /// The FLOAT/DOUBLE case of the fallback above. The frozen dictionary cannot stand in for the earlier appends'
    /// values there, since the NaN count counts values, not entries; those appends were scanned as they were encoded,
    /// with or without a page index to fold their pages (#938 review).
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task DoubleDictionaryThatFillsInALaterBatch_KeepsTheEarlierBatchesStatistics(bool pageIndex)
    {
        var options = new ParquetWriteOptions
        {
            DictionaryPageSizeLimit = 1024,
            RowGroupMaxRows = int.MaxValue,
            WritePageIndex = pageIndex,
        };
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", DoubleType.Default, nullable: false)).Build();
        var batches = new List<RecordBatch>();
        for (int b = 0; b < 30; b++)
        {
            var early = new DoubleArray.Builder();
            for (int i = 0; i < 10_000; i++)
                early.Append(i % 20 == 19 ? double.NaN : 1.5 + i % 20);
            batches.Add(new RecordBatch(schema, [early.Build()], 10_000));
        }
        var late = new DoubleArray.Builder();
        for (int i = 0; i < 300_000; i++)
            late.Append(-5.0 - i);
        batches.Add(new RecordBatch(schema, [late.Build()], 300_000));

        string path = await WriteStreamedAsync(batches, options);
        var (metadata, read) = await ReadAsync(path);
        var chunk = metadata.RowGroups[0].Columns[0].MetaData!;

        Assert.Contains(Encoding.RleDictionary, chunk.Encodings);
        Assert.Equal(1.5 + 18, BitConverter.ToDouble(chunk.Statistics!.MaxValue!, 0));
        Assert.Equal(-5.0 - 299_999, BitConverter.ToDouble(chunk.Statistics.MinValue!, 0));
        Assert.Equal(30 * 500, chunk.Statistics.NanCount);
        Assert.Equal(600_000, read.Sum(b => b.Length));
    }

    private static Int32Array Int32Range(int start, int count, Func<int, int> value)
    {
        var builder = new Int32Array.Builder();
        for (int i = 0; i < count; i++)
            builder.Append(value(start + i));
        return builder.Build();
    }

    [Fact]
    public async Task MostlyNullColumn_DecidesItsDictionaryBeforeHoldingTheRowGroup()
    {
        // A column of nulls never reaches the first-page test's sample of values; past HeldLevelLimit level
        // entries the dictionary decides anyway, rather than holding every batch until the row group closes.
        var options = new ParquetWriteOptions { RowGroupMaxRows = int.MaxValue, RowGroupMaxBytes = long.MaxValue };
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", Int64Type.Default, nullable: true)).Build();

        string path = NewPath();
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true, options))
        {
            for (int b = 0; b < 6; b++)
            {
                var builder = new Int64Array.Builder();
                for (int i = 0; i < 300_000; i++)
                {
                    if (i == 17 && b == 5) builder.Append(42);
                    else builder.AppendNull();
                }
                await writer.WriteBatchAsync(new RecordBatch(schema, [builder.Build()], 300_000));
                // Well under the held size: the batches are not kept, only their pages.
                Assert.True(writer.OpenRowGroupBytes < 8 << 20, $"{writer.OpenRowGroupBytes} bytes held after batch {b}");
            }
            await writer.CloseAsync();
        }

        var (metadata, read) = await ReadAsync(path);
        Assert.Equal(1_800_000, read.Sum(b => b.Length));
        var stats = metadata.RowGroups[0].Columns[0].MetaData!.Statistics!;
        Assert.Equal(1_799_999, stats.NullCount);
        Assert.Equal(42L, BitConverter.ToInt64(stats.MinValue!, 0));
    }

    // ───── Row group cutting ─────

    [Fact]
    public async Task RowGroups_AreCutByRows_SplittingBatches()
    {
        var options = new ParquetWriteOptions { RowGroupMaxRows = 1000 };
        var batch = Batch(4500, 3);
        string path = await WriteStreamedAsync(Enumerable.Range(0, 15).Select(i => batch.Slice(i * 300, 300)), options);

        var (metadata, read) = await ReadAsync(path);
        Assert.Equal(new long[] { 1000, 1000, 1000, 1000, 500 }, metadata.RowGroups.Select(g => g.NumRows));
        Assert.Equal(Rows([batch]), Rows(read));
    }

    [Fact]
    public async Task RowGroups_AreCutByEstimatedEncodedBytes()
    {
        // Random longs encode to about 8 bytes a row: a 1 MiB limit cuts some 130,000 rows to a group.
        const long limit = 1 << 20;
        var options = new ParquetWriteOptions { RowGroupMaxRows = int.MaxValue, RowGroupMaxBytes = limit };
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", Int64Type.Default, nullable: false)).Build();
        var random = new Random(5);
        var batches = new List<RecordBatch>();
        for (int b = 0; b < 100; b++)
        {
            var builder = new Int64Array.Builder();
            for (int i = 0; i < 10_000; i++)
                builder.Append((((long)random.Next() << 33) ^ ((long)random.Next() << 2) ^ random.Next(4)));
            batches.Add(new RecordBatch(schema, [builder.Build()], 10_000));
        }

        string path = await WriteStreamedAsync(batches, options);
        var (metadata, read) = await ReadAsync(path);

        Assert.Equal(1_000_000, read.Sum(b => b.Length));
        Assert.True(metadata.RowGroups.Count >= 6, $"{metadata.RowGroups.Count} row groups");
        foreach (var group in metadata.RowGroups.Take(metadata.RowGroups.Count - 1))
        {
            long size = group.Columns.Sum(c => c.MetaData!.TotalCompressedSize);
            Assert.InRange(size, limit * 3 / 4, limit * 5 / 4);
        }
    }

    [Fact]
    public async Task FirstBatchOfAFile_IsCutByAProbe_SoAHugeBatchStillRespectsTheByteLimit()
    {
        const long limit = 1 << 20;
        var options = new ParquetWriteOptions { RowGroupMaxRows = int.MaxValue, RowGroupMaxBytes = limit };
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", Int64Type.Default, nullable: false)).Build();
        var random = new Random(6);
        var builder = new Int64Array.Builder();
        for (int i = 0; i < 1_000_000; i++)
            builder.Append((((long)random.Next() << 33) ^ ((long)random.Next() << 2) ^ random.Next(4)));

        string path = await WriteStreamedAsync([new RecordBatch(schema, [builder.Build()], 1_000_000)], options);
        var (metadata, _) = await ReadAsync(path);

        Assert.True(metadata.RowGroups.Count >= 6, $"{metadata.RowGroups.Count} row groups");
        Assert.All(metadata.RowGroups, g => Assert.True(
            g.Columns.Sum(c => c.MetaData!.TotalCompressedSize) <= limit * 5 / 4));
    }

    /// <summary>
    /// A byte limit near <see cref="long.MaxValue"/> (cut by rows only) lets a whole batch through in one piece.
    /// Computed carelessly, the rows that fit overflowed there, and the writer took one row per slice: the layout came
    /// out right, but a write ran 10-40x slower (#870).
    /// </summary>
    [Theory]
    [InlineData(long.MaxValue)]
    [InlineData(long.MaxValue / 2)]
    [InlineData(1L << 40)]
    public void AHugeByteLimit_TakesTheWholeBatch(long byteRoom)
    {
        Assert.Equal(1_000_000, ParquetFileWriter.WithinBytes(1_000_000, byteRoom, 150.0, roundUp: true));
        Assert.Equal(int.MaxValue, ParquetFileWriter.WithinBytes(int.MaxValue, byteRoom, 0.001, roundUp: false));
        Assert.Equal(250, ParquetFileWriter.WithinBytes(1000, 1000, 4.0, roundUp: true));
        Assert.Equal(1, ParquetFileWriter.WithinBytes(1000, 0, 4.0, roundUp: true));
    }

    [Fact]
    public async Task FlushRowGroup_CutsEarly_AndReportsWhatIsOpen()
    {
        var options = new ParquetWriteOptions();
        var batch = Batch(1000, 4);
        string path = NewPath();
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true, options))
        {
            await writer.WriteBatchAsync(batch.Slice(0, 600));
            Assert.Equal(600, writer.OpenRowGroupRows);
            Assert.True(writer.OpenRowGroupBytes > 0);

            await writer.FlushRowGroupAsync();
            Assert.Equal(0, writer.OpenRowGroupRows);
            Assert.Equal(0, writer.OpenRowGroupBytes);
            await writer.FlushRowGroupAsync(); // nothing open: nothing written

            await writer.WriteBatchAsync(batch.Slice(600, 400));
            await writer.CloseAsync();
        }

        var (metadata, read) = await ReadAsync(path);
        Assert.Equal(new long[] { 600, 400 }, metadata.RowGroups.Select(g => g.NumRows));
        Assert.Equal(Rows([batch]), Rows(read));
    }

    [Fact]
    public async Task WriteRowGroup_WritesTheOpenRowGroupFirst()
    {
        var batch = Batch(900, 5);
        string path = NewPath();
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true))
        {
            await writer.WriteBatchAsync(batch.Slice(0, 300));
            await writer.WriteRowGroupAsync(batch.Slice(300, 300));
            await writer.WriteBatchAsync(batch.Slice(600, 300));
            await writer.CloseAsync();
        }

        var (metadata, read) = await ReadAsync(path);
        Assert.Equal(new long[] { 300, 300, 300 }, metadata.RowGroups.Select(g => g.NumRows));
        Assert.Equal(Rows([batch]), Rows(read));
    }

    /// <summary>
    /// A row group written whole is the latest measure of encoded bytes per row, so the next batch is cut by it rather
    /// than by an older streamed row group's (#938 review). Here that older one is a constant column of a few bytes in
    /// all; by its measure the next batch would fit whole in a row group many times the byte limit.
    /// </summary>
    [Fact]
    public async Task WriteRowGroup_UpdatesTheBytesPerRow_TheNextBatchIsCutBy()
    {
        var options = new ParquetWriteOptions { RowGroupMaxRows = int.MaxValue, RowGroupMaxBytes = 64 << 10 };
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", Int64Type.Default, nullable: false)).Build();
        var random = new Random(17);
        RecordBatch Random64(int rows)
        {
            var builder = new Int64Array.Builder();
            for (int i = 0; i < rows; i++)
                builder.Append(((long)random.Next() << 32) | (uint)random.Next());
            return new RecordBatch(schema, [builder.Build()], rows);
        }
        var constant = new Int64Array.Builder();
        for (int i = 0; i < 100_000; i++)
            constant.Append(7);

        string path = NewPath();
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true, options))
        {
            await writer.WriteBatchAsync(new RecordBatch(schema, [constant.Build()], 100_000));
            await writer.FlushRowGroupAsync();
            await writer.WriteRowGroupAsync(Random64(10_000));
            await writer.WriteBatchAsync(Random64(50_000));
            await writer.CloseAsync();
        }

        var (metadata, read) = await ReadAsync(path);
        Assert.Equal(160_000, read.Sum(b => b.Length));
        var streamed = metadata.RowGroups.Skip(2).ToList();
        Assert.True(streamed.Count >= 5, $"row groups of {string.Join(", ", metadata.RowGroups.Select(g => g.NumRows))} rows");
        Assert.All(streamed, g => Assert.True(g.TotalCompressedSize < 2 * options.RowGroupMaxBytes, $"{g.TotalCompressedSize} bytes"));
    }

    [Fact]
    public async Task EmptyBatch_AddsNothing()
    {
        var batch = Batch(100, 6);
        string path = NewPath();
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true))
        {
            await writer.WriteBatchAsync(batch.Slice(0, 0));
            Assert.Equal(0, writer.OpenRowGroupRows);
            await writer.WriteBatchAsync(batch);
            await writer.WriteBatchAsync(batch.Slice(0, 0));
            await writer.CloseAsync();
        }

        var (metadata, read) = await ReadAsync(path);
        Assert.Single(metadata.RowGroups);
        Assert.Equal(Rows([batch]), Rows(read));
    }

    // ───── Failure ─────

    private static readonly Apache.Arrow.Schema RequiredInt =
        new Apache.Arrow.Schema.Builder().Field(new Field("v", Int32Type.Default, nullable: false)).Build();

    /// <summary>Rows <paramref name="start"/>.. of a required column, with a null (which it must refuse) at <paramref name="nullAt"/>.</summary>
    private static RecordBatch RequiredRows(int start, int count, int nullAt = -1)
    {
        var builder = new Int32Array.Builder();
        for (int i = 0; i < count; i++)
        {
            if (i == nullAt) builder.AppendNull();
            else builder.Append(start + i);
        }
        return new RecordBatch(RequiredInt, [builder.Build()], count);
    }

    [Fact]
    public async Task EmptyFirstBatch_StillFixesTheSchema()
    {
        string path = NewPath();
        await using var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true);
        await writer.WriteBatchAsync(RequiredRows(0, 0));

        // A batch of another shape is refused, as it would be after a non-empty first batch (#938 review).
        var other = new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("v", StringType.Default, nullable: false)).Build(),
            [new StringArray.Builder().Append("x").Build()], 1);
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.WriteBatchAsync(other));
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.WriteBatchAsync(other.Slice(0, 0)));

        await writer.WriteBatchAsync(RequiredRows(0, 10));
        await writer.CloseAsync();
        var (_, read) = await ReadAsync(path);
        Assert.Equal(10, read.Sum(b => b.Length));
    }

    /// <summary>
    /// A batch that is cut into several pieces is checked whole before any piece goes in: a null in a required
    /// column near its end must not leave its first rows in the open row group, or in a row group already written
    /// (#938 review).
    /// </summary>
    [Fact]
    public async Task RefusedSplitBatch_LeavesNothingOfItBehind()
    {
        var options = new ParquetWriteOptions { RowGroupMaxRows = 1000 };
        string path = NewPath();
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true, options))
        {
            await writer.WriteBatchAsync(RequiredRows(0, 100));
            await Assert.ThrowsAsync<ArgumentException>(
                async () => await writer.WriteBatchAsync(RequiredRows(100, 1000, nullAt: 999)));
            Assert.Equal(100, writer.OpenRowGroupRows);

            await writer.WriteBatchAsync(RequiredRows(100, 50));
            await writer.CloseAsync();
        }

        var (metadata, read) = await ReadAsync(path);
        Assert.Equal(new long[] { 150 }, metadata.RowGroups.Select(g => g.NumRows));
        Assert.Equal(Enumerable.Range(0, 150).Select(i => i.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            Rows(read));
    }

    [Fact]
    public async Task RefusedSplitBatch_WriteRowGroup_WritesNoneOfItsRowGroups()
    {
        var options = new ParquetWriteOptions { RowGroupMaxRows = 100 };
        string path = NewPath();
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true, options))
        {
            await writer.WriteRowGroupAsync(RequiredRows(0, 10));
            await Assert.ThrowsAsync<ArgumentException>(
                async () => await writer.WriteRowGroupAsync(RequiredRows(10, 500, nullAt: 450)));
            await writer.CloseAsync();
        }

        var (metadata, _) = await ReadAsync(path);
        Assert.Equal(new long[] { 10 }, metadata.RowGroups.Select(g => g.NumRows));
    }

    /// <summary>
    /// The first slice of a file is a small probe: a run-end encoded constant is a few bytes of Arrow, so its Arrow
    /// size says nothing of the PLAIN pages its rows become (#938 review).
    /// </summary>
    [Fact]
    public async Task CompactColumn_FirstBatch_StillRespectsTheByteLimit()
    {
        const long limit = 1 << 20;
        var options = new ParquetWriteOptions
        {
            RowGroupMaxRows = int.MaxValue,
            RowGroupMaxBytes = limit,
            DictionaryEnabled = false,
            Compression = CompressionCodec.Uncompressed,
            ByteArrayEncoding = ByteArrayEncoding.Plain,
        };
        var value = System.Text.Encoding.UTF8.GetBytes(new string('x', 1024));
        var constant = EngineeredWood.Arrow.RunEndEncoding.Constant(StringType.Default, value, 20_000);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("c", constant.Data.DataType, nullable: true)).Build();

        string path = await WriteStreamedAsync([new RecordBatch(schema, [constant], 20_000)], options);
        var (metadata, read) = await ReadAsync(path);

        Assert.Equal(20_000, read.Sum(b => b.Length));
        Assert.True(metadata.RowGroups.Count >= 15, $"{metadata.RowGroups.Count} row groups");
        Assert.All(metadata.RowGroups, g => Assert.True(
            g.Columns.Sum(c => c.MetaData!.TotalCompressedSize) <= limit * 5 / 4,
            $"{g.NumRows} rows in {g.Columns.Sum(c => c.MetaData!.TotalCompressedSize)} bytes"));
    }

    [Fact]
    public async Task RefusedBatch_LeavesTheOpenRowGroupAsItWas()
    {
        var batch = Batch(500, 8);
        var other = new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("x", Int32Type.Default, nullable: false)).Build(),
            [Int32Range(0, 10, i => i)], 10);

        string path = NewPath();
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true))
        {
            await writer.WriteBatchAsync(batch.Slice(0, 200));
            await Assert.ThrowsAsync<ArgumentException>(async () => await writer.WriteBatchAsync(other));
            Assert.Equal(200, writer.OpenRowGroupRows);
            await writer.WriteBatchAsync(batch.Slice(200, 300));
            await writer.CloseAsync();
        }

        var (_, read) = await ReadAsync(path);
        Assert.Equal(Rows([batch]), Rows(read));
    }

    [Fact]
    public async Task FailedWriteToTheFile_PoisonsTheWriter()
    {
        var file = new FailingFile(failAfterBytes: 64);
        var writer = new ParquetFileWriter(file, ownsFile: false);
        await writer.WriteBatchAsync(Batch(1000, 9));

        await Assert.ThrowsAsync<IOException>(async () => await writer.FlushRowGroupAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.WriteBatchAsync(Batch(10, 10)));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.CloseAsync());
        await writer.DisposeAsync();
    }

    /// <summary>
    /// A footer write that fails leaves the file without its end. The writer was marked closed before the footer was
    /// written, so a second CloseAsync returned as if the file were finished (#938 review); it must throw.
    /// </summary>
    [Fact]
    public async Task FailedFooterWrite_PoisonsTheWriter_InsteadOfClosingIt()
    {
        var file = new FailingFile(failAfterBytes: long.MaxValue);
        var writer = new ParquetFileWriter(file, ownsFile: false);
        await writer.WriteRowGroupAsync(Batch(100, 13));

        file.Failing = true;
        await Assert.ThrowsAsync<IOException>(async () => await writer.CloseAsync());

        file.Failing = false;
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.CloseAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.WriteBatchAsync(Batch(10, 14)));
        await writer.DisposeAsync();
    }

    /// <summary>
    /// A column of nulls has no values to count: while its dictionary is undecided, only its levels are held. They
    /// count toward the byte limit, so such a row group is cut by bytes like any other (#938 review).
    /// </summary>
    [Fact]
    public async Task AllNullColumn_TakesPartInTheByteCut()
    {
        var options = new ParquetWriteOptions { RowGroupMaxRows = int.MaxValue, RowGroupMaxBytes = 64 << 10 };
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", Int64Type.Default, nullable: true)).Build();
        var batches = new List<RecordBatch>();
        for (int b = 0; b < 100; b++)
        {
            var builder = new Int64Array.Builder();
            for (int i = 0; i < 10_000; i++)
                builder.AppendNull();
            batches.Add(new RecordBatch(schema, [builder.Build()], 10_000));
        }

        string path = await WriteStreamedAsync(batches, options);
        var (metadata, read) = await ReadAsync(path);

        Assert.Equal(1_000_000, read.Sum(b => b.Length));
        Assert.True(metadata.RowGroups.Count >= 3, $"{metadata.RowGroups.Count} row groups of {string.Join(", ", metadata.RowGroups.Select(g => g.NumRows))} rows");
    }

    private sealed class FailingFile(long failAfterBytes) : ISequentialFile
    {
        public long Position { get; private set; }

        /// <summary>Whether every write fails from now on.</summary>
        public bool Failing { get; set; }

        public ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            if (Failing || Position + data.Length > failAfterBytes)
                throw new IOException("The disk is full.");
            Position += data.Length;
            return default;
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken = default) => default;

        public ValueTask CompleteAsync(CancellationToken cancellationToken = default) => default;

        public ValueTask DisposeAsync() => default;

        public void Dispose() { }
    }

    // ───── Memory ─────

    [Fact]
    public async Task OpenRowGroup_HoldsItsEncodedPages_NotTheBatches()
    {
        // 400,000 rows of the mixed batch are ~20 MiB of Arrow; what the open row group holds is its pages.
        var options = new ParquetWriteOptions { RowGroupMaxRows = int.MaxValue, RowGroupMaxBytes = long.MaxValue, Compression = CompressionCodec.Zstd };
        string path = NewPath();
        long arrowBytes = 0;
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true, options))
        {
            for (int b = 0; b < 40; b++)
            {
                var batch = Batch(10_000, 100 + b);
                arrowBytes += EngineeredWood.Arrow.ArrowSizes.EstimatedBytes(batch);
                await writer.WriteBatchAsync(batch);
            }

            Assert.True(writer.OpenRowGroupBytes < arrowBytes / 2,
                $"{writer.OpenRowGroupBytes} bytes held for {arrowBytes} bytes of Arrow");
            await writer.CloseAsync();
        }

        var (metadata, _) = await ReadAsync(path);
        Assert.Single(metadata.RowGroups);
        Assert.Equal(400_000, metadata.NumRows);
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Small batches given to <see cref="ParquetFileWriter.WriteBatchAsync"/> are copied, held and encoded together
/// (#941): the file must be the one encoding each batch as it came writes, and every refusal still the call's own.
/// </summary>
public class HeldBatchTests : IDisposable
{
    private readonly string _dir = Directory.CreateDirectory(
        System.IO.Path.Combine(System.IO.Path.GetTempPath(), "ew-held-" + Guid.NewGuid().ToString("N"))).FullName;

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { }
    }

    private string NewPath() => System.IO.Path.Combine(_dir, Guid.NewGuid().ToString("N") + ".parquet");

    // ───── Data ─────

    /// <summary>A batch of every flat layout that is held, nullable and required, nulls at odd bit offsets.</summary>
    private static RecordBatch Flat(int rows, int seed)
    {
        var random = new Random(seed);
        bool Valid() => random.Next(6) != 0;

        var flags = new BooleanArray.Builder();
        var tiny = new Int8Array.Builder();
        var small = new Int16Array.Builder();
        var unsigned = new UInt32Array.Builder();
        var big = new Int64Array.Builder();
        var single = new FloatArray.Builder();
        var real = new DoubleArray.Builder();
        var day = new Date32Array.Builder();
        var micros = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        var money = new Decimal128Array.Builder(new Decimal128Type(20, 3));
        var low = new StringArray.Builder();
        var high = new StringArray.Builder();
        var bytes = new BinaryArray.Builder();
        var fixedBytes = new byte[rows * 5];
        random.NextBytes(fixedBytes);
        var fixedValid = new List<bool>();
        var epoch = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        for (int i = 0; i < rows; i++)
        {
            if (Valid()) flags.Append(random.Next(2) == 0); else flags.AppendNull();
            if (Valid()) tiny.Append((sbyte)random.Next(-128, 128)); else tiny.AppendNull();
            small.Append((short)random.Next(50)); // required
            if (Valid()) unsigned.Append((uint)random.Next()); else unsigned.AppendNull();
            big.Append(((long)random.Next() << 31) ^ random.Next()); // required
            if (Valid()) single.Append((float)random.NextDouble()); else single.AppendNull();
            switch (random.Next(8))
            {
                case 0: real.AppendNull(); break;
                case 1: real.Append(double.NaN); break;
                case 2: real.Append(-0.0); break;
                default: real.Append(Math.Round(random.NextDouble() * 100, 1)); break;
            }
            day.Append(epoch.AddDays(random.Next(9000)));
            if (Valid()) micros.Append(new DateTimeOffset(epoch.AddSeconds(random.Next()))); else micros.AppendNull();
            if (Valid()) money.Append(random.Next(-1_000_000, 1_000_000) / 1000m); else money.AppendNull();
            if (Valid()) low.Append("v" + random.Next(12)); else low.AppendNull();
            high.Append(new string((char)('a' + random.Next(26)), random.Next(30)) + i); // required; some empty-ish
            if (Valid()) bytes.Append(fixedBytes.AsSpan(i * 5, random.Next(6)).ToArray().AsEnumerable()); else bytes.AppendNull();
            fixedValid.Add(Valid());
        }

        var fixedType = new FixedSizeBinaryType(5);
        var fixedArray = ArrowArrayFactory.BuildArray(new ArrayData(
            fixedType, rows, fixedValid.Count(v => !v), 0, [Bitmap(fixedValid), new ArrowBuffer(fixedBytes)]));

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("flags", BooleanType.Default, nullable: true))
            .Field(new Field("tiny", Int8Type.Default, nullable: true))
            .Field(new Field("small", Int16Type.Default, nullable: false))
            .Field(new Field("unsigned", UInt32Type.Default, nullable: true))
            .Field(new Field("big", Int64Type.Default, nullable: false))
            .Field(new Field("single", FloatType.Default, nullable: true))
            .Field(new Field("real", DoubleType.Default, nullable: true))
            .Field(new Field("day", Date32Type.Default, nullable: false))
            .Field(new Field("micros", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: true))
            .Field(new Field("money", new Decimal128Type(20, 3), nullable: true))
            .Field(new Field("low", StringType.Default, nullable: true))
            .Field(new Field("high", StringType.Default, nullable: false))
            .Field(new Field("bytes", BinaryType.Default, nullable: true))
            .Field(new Field("fixed", fixedType, nullable: true))
            .Build();
        return new RecordBatch(schema,
            [flags.Build(), tiny.Build(), small.Build(), unsigned.Build(), big.Build(), single.Build(), real.Build(),
             day.Build(), micros.Build(), money.Build(), low.Build(), high.Build(), bytes.Build(), fixedArray],
            rows);
    }

    private static ArrowBuffer Bitmap(List<bool> valid)
    {
        var bits = new byte[(valid.Count + 7) / 8];
        for (int i = 0; i < valid.Count; i++)
        {
            if (valid[i])
                bits[i / 8] |= (byte)(1 << (i % 8));
        }
        return new ArrowBuffer(bits);
    }

    /// <summary>Slices of mostly held sizes, some too large to hold, at every bit offset.</summary>
    private static IEnumerable<RecordBatch> Pieces(RecordBatch batch, int seed)
    {
        var random = new Random(seed);
        int offset = 0;
        while (offset < batch.Length)
        {
            int rows = Math.Min(batch.Length - offset, random.Next(10) switch
            {
                < 5 => 1 + random.Next(3),
                < 8 => 1 + random.Next(100),
                8 => 1 + random.Next(1023),
                _ => 1000 + random.Next(1500),
            });
            yield return batch.Slice(offset, rows);
            offset += rows;
        }
    }

    private async Task<string> WriteAsync(IEnumerable<RecordBatch> batches, ParquetWriteOptions options, bool hold)
    {
        string path = NewPath();
        await using var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true, options);
        if (!hold)
            writer.HoldBelowRows = 0;
        foreach (var batch in batches)
            await writer.WriteBatchAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private static async Task<(FileMetaData Metadata, List<string> Rows)> ReadAsync(string path)
    {
        await using var reader = new ParquetFileReader(new LocalRandomAccessFile(path));
        var metadata = await reader.ReadMetadataAsync();
        var rows = new List<string>();
        await foreach (var batch in reader.ReadAllAsync())
            rows.AddRange(Rows(batch));
        return (metadata, rows);
    }

    private static IEnumerable<string> Rows(RecordBatch batch)
    {
        for (int r = 0; r < batch.Length; r++)
            yield return string.Join("|", Enumerable.Range(0, batch.ColumnCount).Select(c => Cell(batch.Column(c), r)));
    }

    // Any flat column's cell, as text: values compared by their bytes, so NaN and -0 are told apart.
    private static string Cell(IArrowArray array, int row)
    {
        if (array.IsNull(row))
            return "null";
        var data = array.Data;
        return array switch
        {
            BooleanArray b => b.GetValue(row)!.Value ? "true" : "false",
            BinaryArray b => Convert.ToBase64String(b.GetBytes(row).ToArray()), // StringArray too
            _ => Convert.ToBase64String(data.Buffers[1].Span.Slice(
                (data.Offset + row) * (((FixedWidthType)data.DataType).BitWidth / 8),
                ((FixedWidthType)data.DataType).BitWidth / 8).ToArray()),
        };
    }

    public static TheoryData<string> OptionSets() =>
        ["default", "v1", "dictsmall", "nodict", "bloom", "pagesize", "pagerows", "rowgroups", "nostats"];

    private static ParquetWriteOptions Options(string name)
    {
        var options = new ParquetWriteOptions { RowGroupMaxRows = int.MaxValue, RowGroupMaxBytes = long.MaxValue };
        return name switch
        {
            "v1" => options with { DataPageVersion = DataPageVersion.V1 },
            "dictsmall" => options with { DictionaryPageSizeLimit = 2048 },
            "nodict" => options with { DictionaryEnabled = false },
            "bloom" => options with { BloomFilterColumns = ["big", "low", "high"], DictionaryPageSizeLimit = 4096 },
            "pagesize" => options with { DataPageSize = 4096 },
            "pagerows" => options with { DataPageRowCountLimit = 333 },
            "rowgroups" => options with { RowGroupMaxRows = 777 },
            "nostats" => options with { WritePageIndex = false },
            _ => options,
        };
    }

    // ───── The file ─────

    /// <summary>
    /// Held or not, the batches write the same rows, row groups and statistics. Unless a page size or row-count limit
    /// cuts pages, the same bytes too: those cut each append's pages where its own estimate says, so a file of many
    /// appends already depends on how its rows arrived, held or not.
    /// </summary>
    [Theory]
    [MemberData(nameof(OptionSets))]
    public async Task HeldBatches_WriteTheFileEncodingEachAsItCameWrites(string optionSet)
    {
        var options = Options(optionSet);
        var batch = Flat(12_000, seed: 3);
        string held = await WriteAsync(Pieces(batch, seed: 5), options, hold: true);
        string unheld = await WriteAsync(Pieces(batch, seed: 5), options, hold: false);

        var (heldMeta, rows) = await ReadAsync(held);
        var (unheldMeta, _) = await ReadAsync(unheld);
        Assert.Equal(Rows(batch), rows);
        Assert.Equal(unheldMeta.RowGroups.Select(g => g.NumRows), heldMeta.RowGroups.Select(g => g.NumRows));
        for (int g = 0; g < heldMeta.RowGroups.Count; g++)
        {
            for (int c = 0; c < heldMeta.RowGroups[g].Columns.Count; c++)
            {
                var expected = unheldMeta.RowGroups[g].Columns[c].MetaData!.Statistics;
                var actual = heldMeta.RowGroups[g].Columns[c].MetaData!.Statistics;
                Assert.Equal(expected?.NullCount, actual?.NullCount);
                Assert.Equal(expected?.NanCount, actual?.NanCount);
                Assert.Equal(expected?.MinValue, actual?.MinValue);
                Assert.Equal(expected?.MaxValue, actual?.MaxValue);
            }
        }

        if (optionSet is not ("pagesize" or "pagerows"))
            Assert.Equal(File.ReadAllBytes(unheld), File.ReadAllBytes(held));
    }

    /// <summary>
    /// Under a data page row-count limit, one-row batches encoded as they came were not the whole batch's file, and
    /// held they are: every page is cut where the limit says.
    /// </summary>
    [Fact]
    public async Task PageRowLimit_HeldOneRowBatches_WriteTheWholeBatchesFile()
    {
        var options = new ParquetWriteOptions { RowGroupMaxRows = int.MaxValue, RowGroupMaxBytes = long.MaxValue, DataPageRowCountLimit = 333 };
        var batch = Flat(3000, seed: 4);
        string held = await WriteAsync(Enumerable.Range(0, batch.Length).Select(i => batch.Slice(i, 1)), options, hold: true);
        string whole = await WriteAsync([batch], options, hold: false);

        Assert.Equal(File.ReadAllBytes(whole), File.ReadAllBytes(held));
        string mixed = await WriteAsync(Pieces(Flat(12_000, seed: 3), seed: 5), options, hold: true);
        var (_, rows) = await ReadAsync(mixed);
        Assert.Equal(Rows(Flat(12_000, seed: 3)), rows);
    }

    [Fact]
    public async Task OneRowBatches_ReadBackAsTheWhole()
    {
        var batch = Flat(3000, seed: 8);
        string path = await WriteAsync(
            Enumerable.Range(0, batch.Length).Select(i => batch.Slice(i, 1)), new ParquetWriteOptions(), hold: true);

        var (metadata, rows) = await ReadAsync(path);
        Assert.Single(metadata.RowGroups);
        Assert.Equal(Rows(batch), rows);
    }

    [Fact]
    public async Task RowGroups_AreCutByRows_AtTheBatchThatFillsThem()
    {
        var options = new ParquetWriteOptions { RowGroupMaxRows = 100 };
        var batch = Flat(1000, seed: 9);
        string path = NewPath();
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true, options))
        {
            for (int i = 0; i < batch.Length; i += 7)
            {
                var piece = batch.Slice(i, Math.Min(7, batch.Length - i));
                await writer.WriteBatchAsync(piece);
                // Never more than a row group's worth open: the batch that fills it writes it.
                Assert.Equal((i + piece.Length) % 100, writer.OpenRowGroupRows);
            }
            await writer.CloseAsync();
        }

        var (metadata, rows) = await ReadAsync(path);
        Assert.Equal(Enumerable.Repeat(100L, 10), metadata.RowGroups.Select(g => g.NumRows));
        Assert.Equal(Rows(batch), rows);
    }

    [Fact]
    public async Task RowGroups_AreCutByEstimatedEncodedBytes()
    {
        // Random longs encode to about 8 bytes a row: a 64 KiB limit cuts some 8,000 rows to a group.
        const long limit = 64 << 10;
        var options = new ParquetWriteOptions { RowGroupMaxRows = int.MaxValue, RowGroupMaxBytes = limit };
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", Int64Type.Default, nullable: false)).Build();
        var random = new Random(5);
        var builder = new Int64Array.Builder();
        for (int i = 0; i < 100_000; i++)
            builder.Append(((long)random.Next() << 33) ^ ((long)random.Next() << 2) ^ random.Next(4));
        var batch = new RecordBatch(schema, [builder.Build()], 100_000);

        string path = await WriteAsync(
            Enumerable.Range(0, batch.Length / 5).Select(i => batch.Slice(i * 5, 5)), options, hold: true);
        var (metadata, rows) = await ReadAsync(path);

        Assert.Equal(100_000, rows.Count);
        Assert.True(metadata.RowGroups.Count >= 8, $"{metadata.RowGroups.Count} row groups");
        foreach (var group in metadata.RowGroups.Take(metadata.RowGroups.Count - 1))
        {
            long size = group.Columns.Sum(c => c.MetaData!.TotalCompressedSize);
            Assert.InRange(size, limit * 3 / 4, limit * 5 / 4);
        }
    }

    [Fact]
    public async Task HeldRows_AreOpen_AndFlushWritesThem()
    {
        var batch = Flat(30, seed: 10);
        string path = NewPath();
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true))
        {
            await writer.WriteBatchAsync(batch.Slice(0, 10));
            Assert.Equal(10, writer.OpenRowGroupRows);
            Assert.True(writer.OpenRowGroupBytes > 0);

            await writer.FlushRowGroupAsync();
            Assert.Equal(0, writer.OpenRowGroupRows);
            Assert.Equal(0, writer.OpenRowGroupBytes);

            await writer.WriteBatchAsync(batch.Slice(10, 10));
            await writer.WriteRowGroupAsync(batch.Slice(20, 5)); // the held rows first, as a row group of their own
            await writer.WriteBatchAsync(batch.Slice(25, 5));
            await writer.CloseAsync(); // and the last held rows
        }

        var (metadata, rows) = await ReadAsync(path);
        Assert.Equal(new long[] { 10, 10, 5, 5 }, metadata.RowGroups.Select(g => g.NumRows));
        Assert.Equal(Rows(batch), rows);
    }

    [Fact]
    public async Task ABatchOfAnotherLayout_FollowsTheHeldRows()
    {
        // Large and small strings are one column type in the file, but held apart: the held rows are encoded first.
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("s", StringType.Default, nullable: true)).Build();
        var largeSchema = new Apache.Arrow.Schema.Builder().Field(new Field("s", LargeStringType.Default, nullable: true)).Build();
        RecordBatch Small(string value) => new(schema, [new StringArray.Builder().Append(value).Build()], 1);
        RecordBatch Large(string value) => new(largeSchema, [new LargeStringArray.Builder().Append(value).Build()], 1);

        string path = await WriteAsync(
            [Small("a"), Small("b"), Large("c"), Small("d"), Large("e"), Large("f"), Small("g")],
            new ParquetWriteOptions(), hold: true);

        var (_, rows) = await ReadAsync(path);
        Assert.Equal(["YQ==", "Yg==", "Yw==", "ZA==", "ZQ==", "Zg==", "Zw=="], rows);
    }

    [Fact]
    public async Task SecondPrecisionTimes_AreNotHeld_AndStillRescaled()
    {
        var type = new TimestampType(TimeUnit.Second, (string?)null);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("t", type, nullable: false)).Build();
        var epoch = new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero);
        string path = NewPath();
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true))
        {
            for (int i = 0; i < 20; i++)
            {
                var builder = new TimestampArray.Builder(type);
                builder.Append(epoch.AddSeconds(i));
                await writer.WriteBatchAsync(new RecordBatch(schema, [builder.Build()], 1));
            }
            await writer.CloseAsync();
        }

        await using var reader = new ParquetFileReader(new LocalRandomAccessFile(path));
        var values = new List<DateTimeOffset>();
        await foreach (var batch in reader.ReadAllAsync())
        {
            var column = (TimestampArray)batch.Column(0);
            for (int i = 0; i < column.Length; i++)
                values.Add(column.GetTimestamp(i)!.Value);
        }
        Assert.Equal(Enumerable.Range(0, 20).Select(i => epoch.AddSeconds(i)), values);
    }

    // ───── Refusals ─────

    private static readonly Apache.Arrow.Schema RequiredInt =
        new Apache.Arrow.Schema.Builder().Field(new Field("v", Int32Type.Default, nullable: false)).Build();

    private static RecordBatch RequiredRows(int start, int count, int nullAt = -1, Apache.Arrow.Schema? schema = null)
    {
        var builder = new Int32Array.Builder();
        for (int i = 0; i < count; i++)
        {
            if (i == nullAt) builder.AppendNull();
            else builder.Append(start + i);
        }
        return new RecordBatch(schema ?? RequiredInt, [builder.Build()], count);
    }

    [Fact]
    public async Task ANullInARequiredColumn_IsRefusedByItsOwnCall_AndTheHeldRowsStay()
    {
        // The same schema object as the held rows (checked only for nulls), and an equal one (checked whole).
        var equal = new Apache.Arrow.Schema.Builder().Field(new Field("v", Int32Type.Default, nullable: false)).Build();
        string path = NewPath();
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true))
        {
            await writer.WriteBatchAsync(RequiredRows(0, 5));
            var same = await Assert.ThrowsAsync<ArgumentException>(
                async () => await writer.WriteBatchAsync(RequiredRows(5, 3, nullAt: 1)));
            Assert.Contains("'v'", same.Message, StringComparison.Ordinal);
            await Assert.ThrowsAsync<ArgumentException>(
                async () => await writer.WriteBatchAsync(RequiredRows(5, 3, nullAt: 2, equal)));
            Assert.Equal(5, writer.OpenRowGroupRows);

            await writer.WriteBatchAsync(RequiredRows(5, 5, schema: equal));
            await writer.CloseAsync();
        }

        var (_, rows) = await ReadAsync(path);
        Assert.Equal(10, rows.Count);
    }

    [Fact]
    public async Task ANullInTheFirstBatch_IsRefused_AndNothingIsHeld()
    {
        string path = NewPath();
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true))
        {
            await Assert.ThrowsAsync<ArgumentException>(
                async () => await writer.WriteBatchAsync(RequiredRows(0, 3, nullAt: 0)));
            Assert.Equal(0, writer.OpenRowGroupRows);
            await writer.WriteBatchAsync(RequiredRows(0, 3));
            await writer.CloseAsync();
        }

        var (_, rows) = await ReadAsync(path);
        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public async Task ABatchOfAnotherShape_IsRefused_WhileRowsAreHeld()
    {
        var other = new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("v", Int64Type.Default, nullable: false)).Build(),
            [new Int64Array.Builder().Append(1).Build()], 1);
        string path = NewPath();
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true))
        {
            await writer.WriteBatchAsync(RequiredRows(0, 4));
            await Assert.ThrowsAsync<ArgumentException>(async () => await writer.WriteBatchAsync(other));
            Assert.Equal(4, writer.OpenRowGroupRows);
            await writer.CloseAsync();
        }

        var (_, rows) = await ReadAsync(path);
        Assert.Equal(4, rows.Count);
    }

    /// <summary>
    /// A refused batch leaves the open row group as it was, held rows and all: checked before the held rows are
    /// encoded, which can write the row group they fill (#945 review). WriteRowGroupAsync wrote the open row group
    /// before checking its batch, held rows or not.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ARefusedBatch_DoesNotEncodeOrWriteTheHeldRows(bool wholeRowGroup)
    {
        // A first row group of constants measures a few bytes a row, so a thousand rows of 100-byte strings are held
        // within a 64 KiB limit they exceed once encoded.
        var options = new ParquetWriteOptions { RowGroupMaxRows = int.MaxValue, RowGroupMaxBytes = 64 << 10 };
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("v", Int64Type.Default, nullable: false))
            .Field(new Field("s", StringType.Default, nullable: false))
            .Build();
        var random = new Random(21);
        RecordBatch Rows(int count, bool constant)
        {
            var v = new Int64Array.Builder();
            var s = new StringArray.Builder();
            for (int i = 0; i < count; i++)
            {
                v.Append(constant ? 7 : random.Next());
                s.Append(constant ? "" : new string((char)('a' + random.Next(26)), 100) + random.Next());
            }
            return new RecordBatch(schema, [v.Build(), s.Build()], count);
        }
        var other = new RecordBatch(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field("v", Int64Type.Default, nullable: false))
                .Field(new Field("s", Int32Type.Default, nullable: false))
                .Build(),
            [new Int64Array.Builder().Append(1).Build(), new Int32Array.Builder().Append(1).Build()], 1);

        string path = NewPath();
        var file = new LocalSequentialFile(path);
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(Rows(100_000, constant: true));
            for (int i = 0; i < 1000; i++)
                await writer.WriteBatchAsync(Rows(1, constant: false));
            Assert.Equal(1000, writer.OpenRowGroupRows);
            long position = file.Position;

            await Assert.ThrowsAsync<ArgumentException>(async () =>
            {
                if (wholeRowGroup) await writer.WriteRowGroupAsync(other);
                else await writer.WriteBatchAsync(other);
            });
            Assert.Equal(position, file.Position);
            Assert.Equal(1000, writer.OpenRowGroupRows);
            await writer.CloseAsync();
        }
        await file.DisposeAsync();

        var (metadata, _) = await ReadAsync(path);
        Assert.Equal(101_000, metadata.NumRows);
    }

    /// <summary>
    /// A batch too large to hold is not copied first: the buffers grow only for what is held, so they stay within
    /// about <see cref="ParquetFileWriter.HeldBytesLimit"/> (#945 review).
    /// </summary>
    [Fact]
    public async Task ABatchOverTheByteLimit_IsNotCopied()
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("s", StringType.Default, nullable: false)).Build();
        RecordBatch Row(int length) =>
            new(schema, [new StringArray.Builder().Append(new string('x', length)).Build()], 1);

        string path = NewPath();
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true))
        {
            await writer.WriteBatchAsync(Row(10));
            await writer.WriteBatchAsync(Row(8 << 20));
            await writer.WriteBatchAsync(Row(10));
            Assert.True(writer.HeldBufferBytes < ParquetFileWriter.HeldBytesLimit,
                $"{writer.HeldBufferBytes} bytes of buffers");
            await writer.CloseAsync();
        }

        var (_, rows) = await ReadAsync(path);
        Assert.Equal(3, rows.Count);
    }

    /// <summary>
    /// The held rows' size counts a validity bitmap for every held row, the rows held before the first null among
    /// them too (#945 review).
    /// </summary>
    [Fact]
    public void HeldBytes_CountTheWholeValidityBitmap()
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("b", BooleanType.Default, nullable: true)).Build();
        var valid = new BooleanArray.Builder();
        for (int i = 0; i < 512; i++)
            valid.Append(true);
        var first = new RecordBatch(schema, [valid.Build()], 512);
        var withNull = new RecordBatch(schema, [new BooleanArray.Builder().AppendNull().Build()], 1);

        var held = HeldRows.For(first)!;
        held.Append(first, held.BytesFor(first));
        Assert.Equal(64, held.Bytes);
        held.Append(withNull, held.BytesFor(withNull));
        Assert.Equal(65 + 65, held.Bytes); // 513 value bits and 513 validity bits
    }

    [Fact]
    public async Task DisposingWithRowsHeld_WritesNothingOfThem()
    {
        string path = NewPath();
        var file = new LocalSequentialFile(path);
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteBatchAsync(RequiredRows(0, 4));
            Assert.Equal(0, file.Position);
        }
        await file.DisposeAsync();
    }

    // ───── A file-rolling caller ─────

    [Fact]
    public async Task UntilRowGroup_TakesEachSmallBatch_AndStopsAtTheRowGroupItFills()
    {
        var options = new ParquetWriteOptions { RowGroupMaxRows = 50 };
        var batch = Flat(200, seed: 11);
        string path = NewPath();
        await using (var writer = new ParquetFileWriter(new LocalSequentialFile(path), ownsFile: true, options))
        {
            for (int i = 0; i < batch.Length; i += 20)
            {
                var piece = batch.Slice(i, 20);
                int offset = 0;
                while (offset < piece.Length)
                {
                    int taken = await writer.WriteBatchUntilRowGroupAsync(piece, offset);
                    offset += taken;
                    Assert.True(writer.OpenRowGroupRows < 50);
                }
            }
            await writer.CloseAsync();
        }

        var (metadata, rows) = await ReadAsync(path);
        Assert.Equal(new long[] { 50, 50, 50, 50 }, metadata.RowGroups.Select(g => g.NumRows));
        Assert.Equal(Rows(batch), rows);
    }
}

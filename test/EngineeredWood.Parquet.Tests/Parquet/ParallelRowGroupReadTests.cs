// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Reading a file's row groups several at a time (#612): the batches, their order and their positions must be
/// exactly what a serial read gives, under every kind of pruning; an error must surface where a serial read
/// would have thrown; and however the read ends, nothing read ahead may be left behind.
/// </summary>
public class ParallelRowGroupReadTests : IDisposable
{
    private const int RowGroups = 12;

    private readonly string _tempDir;
    private readonly string _path;
    private readonly long[] _groupStarts = new long[RowGroups + 1];

    public ParallelRowGroupReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-parallel-rg-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _path = Path.Combine(_tempDir, "data.parquet");
        Write(_path, _groupStarts);
    }

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

    /// <summary>Row group <c>g</c> holds <c>1,000 + 137 g</c> rows; <c>id</c> is the row's position, so
    /// statistics and page indexes can prune by it; <c>name</c> is longer than a view's 12 inlined bytes.</summary>
    private static void Write(string path, long[] groupStarts)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("name", StringType.Default, false))
            .Field(new Field("value", DoubleType.Default, true))
            .Build();

        using var file = new LocalSequentialFile(path);
        using var writer = new ParquetFileWriter(file, ownsFile: false,
            ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });
        long id = 0;
        for (int g = 0; g < RowGroups; g++)
        {
            groupStarts[g] = id;
            int rows = 1_000 + 137 * g;
            var ids = new Int64Array.Builder();
            var names = new StringArray.Builder();
            var values = new DoubleArray.Builder();
            for (int i = 0; i < rows; i++, id++)
            {
                ids.Append(id);
                names.Append($"row-{id:D8}-of-group-{g:D2}");
                if (id % 7 == 0)
                    values.AppendNull();
                else
                    values.Append(id * 0.5);
            }
            using var batch = new RecordBatch(schema, [ids.Build(), names.Build(), values.Build()], rows);
            writer.WriteRowGroupAsync(batch).AsTask().GetAwaiter().GetResult();
        }
        groupStarts[RowGroups] = id;
        writer.CloseAsync().AsTask().GetAwaiter().GetResult();
    }

    private sealed record Snapshot(long FirstRow, long[] Ids, string[] Names);

    private static ParquetReadOptions Options(int window, long budget = 256L << 20, Predicate? filter = null) =>
        new() { MaxConcurrentRowGroups = window, MaxBufferedReadBytes = budget, Filter = filter };

    private async Task<List<Snapshot>> ReadAsync(
        ParquetReadOptions options, IReadOnlyList<string>? columns = null, RowSkipPredicate? skip = null,
        Func<IRandomAccessFile, IRandomAccessFile>? wrap = null)
    {
        await using var local = new LocalRandomAccessFile(_path);
        var file = wrap is null ? local : wrap(local);
        using var reader = new ParquetFileReader(file, ownsFile: false, options);
        var result = new List<Snapshot>();
        await foreach (var positioned in reader.ReadWithPositionsAsync(null, columns, skip, default))
        {
            using var batch = positioned.Batch;
            result.Add(Take(positioned.FirstRow, batch));
        }
        return result;
    }

    private static Snapshot Take(long firstRow, RecordBatch batch)
    {
        long[] ids = batch.Schema.GetFieldIndex("id") is int i and >= 0
            ? [.. ((Int64Array)batch.Column(i)).Values.ToArray()]
            : [];
        string[] names = batch.Schema.GetFieldIndex("name") is int n and >= 0
            ? [.. Enumerable.Range(0, batch.Length).Select(r => ((StringArray)batch.Column(n)).GetString(r)!)]
            : [];
        return new Snapshot(firstRow, ids, names);
    }

    private static void AssertSame(List<Snapshot> expected, List<Snapshot> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (int b = 0; b < expected.Count; b++)
        {
            Assert.Equal(expected[b].FirstRow, actual[b].FirstRow);
            Assert.Equal(expected[b].Ids, actual[b].Ids);
            Assert.Equal(expected[b].Names, actual[b].Names);
        }
    }

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(64)]
    [InlineData(int.MaxValue)]
    public async Task AWholeRead_IsTheSerialRead(int window)
    {
        var serial = await ReadAsync(Options(1));
        Assert.Equal(RowGroups, serial.Count);
        AssertSame(serial, await ReadAsync(Options(window)));
    }

    [Fact]
    public async Task ReadAllAsync_IsTheSerialRead()
    {
        async Task<List<long[]>> Read(int window)
        {
            await using var file = new LocalRandomAccessFile(_path);
            using var reader = new ParquetFileReader(file, ownsFile: false, Options(window));
            var ids = new List<long[]>();
            await foreach (var batch in reader.ReadAllAsync(["id"]))
            {
                using (batch)
                    ids.Add([.. ((Int64Array)batch.Column(0)).Values.ToArray()]);
                GC.KeepAlive(batch);
            }
            return ids;
        }

        var serial = await Read(1);
        var parallel = await Read(4);
        Assert.Equal(serial.Count, parallel.Count);
        for (int b = 0; b < serial.Count; b++)
            Assert.Equal(serial[b], parallel[b]);
    }

    [Fact]
    public async Task PruningAndPositions_AreTheSerialRead()
    {
        // Statistics drop the groups wholly outside the intervals, the page index narrows the groups they
        // cut, and the skip predicate drops group 2 unread and cuts a run out of group 5.
        var filter = Ex.Or(
            Ex.And(Ex.GreaterThanOrEqual("id", LiteralValue.Of(_groupStarts[1] + 300)),
                   Ex.LessThan("id", LiteralValue.Of(_groupStarts[4] + 10))),
            Ex.And(Ex.GreaterThanOrEqual("id", LiteralValue.Of(_groupStarts[5])),
                   Ex.LessThan("id", LiteralValue.Of(_groupStarts[9] - 400))));
        long cutStart = _groupStarts[5] + 100, cutEnd = _groupStarts[5] + 1_900;
        bool Deleted(long row) =>
            (row >= _groupStarts[2] && row < _groupStarts[3]) || (row >= cutStart && row < cutEnd);
        bool Skip(long first, long count) => Deleted(first) && Deleted(first + count - 1)
            && (first + count <= _groupStarts[3] || first >= cutStart);

        var serial = await ReadAsync(Options(1, filter: filter), skip: Skip);
        var parallel = await ReadAsync(Options(4, filter: filter), skip: Skip);
        AssertSame(serial, parallel);

        // The pruning really pruned: some groups are gone, and some batches start inside a group.
        Assert.DoesNotContain(serial, s => s.FirstRow == _groupStarts[0]);
        Assert.DoesNotContain(serial, s => s.FirstRow == _groupStarts[2]);
        Assert.Contains(serial, s => System.Array.IndexOf(_groupStarts, s.FirstRow) < 0);
        foreach (var s in parallel)
            Assert.Equal(s.FirstRow, s.Ids[0]);
    }

    [Fact]
    public async Task ANarrowProjection_IsTheSerialRead()
    {
        AssertSame(await ReadAsync(Options(1), ["name"]), await ReadAsync(Options(4), ["name"]));
    }

    [Fact]
    public async Task BatchSize_SplitsEachGroup_AsTheSerialReadDoes()
    {
        var serial = await ReadAsync(Options(1) with { BatchSize = 400 });
        Assert.True(serial.Count > RowGroups);
        AssertSame(serial, await ReadAsync(Options(4) with { BatchSize = 400 }));
    }

    [Fact]
    public async Task ABudgetOfOneByte_StillReadsEverything()
    {
        AssertSame(await ReadAsync(Options(1)), await ReadAsync(Options(4, budget: 1)));
    }

    [Fact]
    public async Task TheBudget_BoundsWhatIsHeld()
    {
        long largest = 0;
        await using (var file = new LocalRandomAccessFile(_path))
        using (var reader = new ParquetFileReader(file, ownsFile: false, Options(1)))
        {
            await foreach (var batch in reader.ReadAllAsync())
            {
                using (batch)
                    largest = Math.Max(largest, EngineeredWood.Arrow.ArrowSizes.EstimatedBytes(batch));
            }
        }

        // Room for two batches; a slow consumer lets the producers fill it.
        long budget = 2 * largest;
        await using var input = new LocalRandomAccessFile(_path);
        using var parallel = new ParquetFileReader(input, ownsFile: false, Options(8, budget));
        await foreach (var batch in parallel.ReadAllAsync())
        {
            using (batch)
                await Task.Delay(5);
        }

        var peak = parallel.LastScanBudget!.Peak;
        Assert.InRange(peak, 1, budget + 2 * largest);
        Assert.Equal(0, parallel.LastScanBudget.Used);
    }

    /// <summary>Two readers given one <see cref="ParquetReadOptions.MemoryBudget"/> (#719): their own budgets
    /// are far above it, so the shared one holds both reads back, and both still read every row.</summary>
    [Fact]
    public async Task TwoReadersSharingABudget_AreHeldToItTogether()
    {
        long largest = 0;
        await using (var file = new LocalRandomAccessFile(_path))
        using (var reader = new ParquetFileReader(file, ownsFile: false, Options(1)))
        {
            await foreach (var batch in reader.ReadAllAsync())
            {
                using (batch)
                    largest = Math.Max(largest, EngineeredWood.Arrow.ArrowSizes.EstimatedBytes(batch));
            }
        }
        var expected = await ReadAsync(Options(1));

        long cap = 2 * largest;
        var shared = new EngineeredWood.Memory.MemoryBudget(cap);
        var options = Options(8) with { MemoryBudget = shared };

        async Task<(List<Snapshot> Read, long Peak)> SlowlyAsync()
        {
            await using var input = new LocalRandomAccessFile(_path);
            using var reader = new ParquetFileReader(input, ownsFile: false, options);
            var result = new List<Snapshot>();
            await foreach (var positioned in reader.ReadWithPositionsAsync(null, null, null, default))
            {
                using var batch = positioned.Batch;
                result.Add(Take(positioned.FirstRow, batch));
                await Task.Delay(5);
            }
            Assert.Equal(0, reader.LastScanBudget!.Used);
            return (result, reader.LastScanBudget.Peak);
        }

        var reads = await Task.WhenAll(SlowlyAsync(), SlowlyAsync());

        AssertSame(expected, reads[0].Read);
        AssertSame(expected, reads[1].Read);
        // The shared cap, plus for each reader the batch its caller waits on and one underestimate (as in
        // TheBudget_BoundsWhatIsHeld).
        Assert.InRange(shared.PeakBytes, 1, cap + 2 * 2 * largest);
        Assert.Equal(0, shared.UsedBytes);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task EarlyExit_LeavesNothingHeld(int take)
    {
        await using var file = new LocalRandomAccessFile(_path);
        using var reader = new ParquetFileReader(file, ownsFile: false, Options(4));
        int taken = 0;
        await foreach (var batch in reader.ReadAllAsync())
        {
            batch.Dispose();
            if (++taken == take)
                break;
        }

        Assert.Equal(0, reader.LastScanBudget!.Used);
    }

    [Fact]
    public async Task Cancellation_StopsTheRead_AndLeavesNothingHeld()
    {
        await using var file = new LocalRandomAccessFile(_path);
        using var reader = new ParquetFileReader(file, ownsFile: false, Options(4));
        using var cts = new CancellationTokenSource();
        int taken = 0;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var batch in reader.ReadAllAsync(cancellationToken: cts.Token))
            {
                batch.Dispose();
                if (++taken == 2)
                    cts.Cancel();
            }
        });

        Assert.Equal(2, taken);
        Assert.Equal(0, reader.LastScanBudget!.Used);
    }

    [Fact]
    public async Task AReadError_SurfacesAtItsRowGroup_AfterEveryBatchBeforeIt()
    {
        const int failing = 6;
        long failAt = await DataOffsetAsync(failing);

        foreach (int window in new[] { 1, 4 })
        {
            var seen = new List<long>();
            var error = await Assert.ThrowsAsync<IOException>(async () =>
            {
                await using var local = new LocalRandomAccessFile(_path);
                var file = new FailingFile(local, failAt);
                using var reader = new ParquetFileReader(file, ownsFile: false, Options(window));
                await foreach (var positioned in reader.ReadWithPositionsAsync(null, ["id"], null, default))
                {
                    positioned.Batch.Dispose();
                    seen.Add(positioned.FirstRow);
                }
            });

            Assert.Equal(FailingFile.Message, error.Message);
            Assert.Equal(_groupStarts.Take(failing), seen);
        }
    }

    [Fact]
    public async Task APlanningError_SurfacesAtItsRowGroup_AfterEveryBatchBeforeIt()
    {
        // The skip predicate is asked about each kept group's blocks while the read is PLANNED — on the
        // serial planner, not on a row group's decode. It fails for group 5's: a serial read would throw
        // there, after groups 0-4.
        const int failing = 5;
        bool Skip(long first, long count)
        {
            if (first >= _groupStarts[failing] && first < _groupStarts[failing + 1]
                && count < _groupStarts[failing + 1] - _groupStarts[failing])
            {
                throw new InvalidOperationException("Injected planning failure.");
            }
            return false;
        }

        foreach (int window in new[] { 1, 4 })
        {
            var seen = new List<long>();
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await using var file = new LocalRandomAccessFile(_path);
                using var reader = new ParquetFileReader(file, ownsFile: false, Options(window));
                await foreach (var positioned in reader.ReadWithPositionsAsync(null, ["id"], Skip, default))
                {
                    positioned.Batch.Dispose();
                    seen.Add(positioned.FirstRow);
                }
            });

            Assert.Equal(_groupStarts.Take(failing), seen);
        }
    }

    [Theory]
    [InlineData(1, null, false)]
    [InlineData(6, null, true)]
    [InlineData(6, 1, false)]
    [InlineData(1, 3, true)]
    public async Task TheFilesHint_SetsTheDefault_AndAnExplicitSettingWins(int hint, int? option, bool parallel)
    {
        await using var local = new LocalRandomAccessFile(_path);
        var file = new HintingFile(local, hint);
        using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions { MaxConcurrentRowGroups = option });
        int batches = 0;
        await foreach (var batch in reader.ReadAllAsync(["id"]))
        {
            batch.Dispose();
            batches++;
        }

        Assert.Equal(RowGroups, batches);
        // Only a parallel read has a scan budget.
        Assert.Equal(parallel, reader.LastScanBudget is not null);
    }

    [Fact]
    public void CoalescingFileReader_ForwardsItsInnerFilesHint()
    {
        using var local = new LocalRandomAccessFile(_path);
        Assert.Null(new CoalescingFileReader(local).PreferredConcurrentReads);
        Assert.Equal(7, new CoalescingFileReader(new HintingFile(local, 7)).PreferredConcurrentReads);
    }

    private sealed class HintingFile(IRandomAccessFile inner, int hint) : IRandomAccessFile, IReadConcurrencyHint
    {
        public int? PreferredConcurrentReads => hint;

        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) =>
            inner.GetLengthAsync(cancellationToken);

        public ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default) =>
            inner.ReadAsync(range, cancellationToken);

        public ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default) =>
            inner.ReadRangesAsync(ranges, cancellationToken);

        public ValueTask DisposeAsync() => default;

        public void Dispose()
        {
        }
    }

    [Fact]
    public async Task TheEstimate_CountsTheChunksTheReadDecodes()
    {
        // A struct column read by a dotted path, beside a flat one.
        var structType = new StructType([new Field("a", Int64Type.Default, false), new Field("b", StringType.Default, false)]);
        var id = new Int64Array.Builder();
        var a = new Int64Array.Builder();
        var b = new StringArray.Builder();
        for (int i = 0; i < 4_000; i++)
        {
            id.Append(i);
            a.Append(i * 3);
            b.Append($"value-{i:D6}-padding");
        }
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("st", structType, false))
            .Build();
        string path = Path.Combine(_tempDir, "struct.parquet");
        using (var batch = new RecordBatch(schema,
            [id.Build(), new StructArray(structType, 4_000, [a.Build(), b.Build()], ArrowBuffer.Empty, 0)], 4_000))
        await using (var output = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(output, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var group = (await reader.ReadMetadataAsync()).RowGroups[0];
        var parquetSchema = await reader.GetSchemaAsync();
        long Uncompressed(int column) => group.Columns[column].MetaData!.TotalUncompressedSize;
        long Compressed(int column) => group.Columns[column].MetaData!.TotalCompressedSize;

        // A dotted path resolves to its own leaf, as the read resolves it — not to nothing.
        Assert.Equal((Uncompressed(2), Compressed(2)),
            ParquetFileReader.PlannedBytes(group, parquetSchema, ["st.b"], null));
        Assert.Equal((Uncompressed(1) + Uncompressed(2), Compressed(1) + Compressed(2)),
            ParquetFileReader.PlannedBytes(group, parquetSchema, ["st"], null));

        // Ranges scale a flat projection's uncompressed size by the rows they keep, but not a nested one's: that
        // row group is decoded whole and then sliced. They never scale the compressed size, since a header scan
        // holds the whole chunk.
        RowRange[] quarter = [new RowRange(0, 1_000)];
        Assert.Equal((Uncompressed(0) / 4, Compressed(0)),
            ParquetFileReader.PlannedBytes(group, parquetSchema, ["id"], quarter));
        Assert.Equal(Uncompressed(0) + Uncompressed(1) + Uncompressed(2),
            ParquetFileReader.PlannedBytes(group, parquetSchema, null, quarter).Uncompressed);
    }

    private const int EncodedRows = 100_000;

    /// <summary>A key of few distinct values and a nullable double: both encode to a sliver of what they decode
    /// to, so the footer's uncompressed size is far below the decoded batch.</summary>
    private async Task<string> WriteEncodedAsync()
    {
        const int rows = EncodedRows;
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("key", Int64Type.Default, false))
            .Field(new Field("price", DoubleType.Default, true))
            .Build();
        var keys = new Int64Array.Builder();
        var prices = new DoubleArray.Builder();
        for (int i = 0; i < rows; i++)
        {
            keys.Append(i / 1000);
            if (i % 10 == 0)
                prices.AppendNull();
            else
                prices.Append(i % 7 * 1.5);
        }
        string path = Path.Combine(_tempDir, "encoded.parquet");
        using (var batch = new RecordBatch(schema, [keys.Build(), prices.Build()], rows))
        await using (var output = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(output, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }
        return path;
    }

    private static async Task<long> DecodedSizeAsync(ParquetFileReader reader)
    {
        long decoded = 0;
        await foreach (var batch in reader.ReadAllAsync())
        {
            using (batch)
                decoded += EngineeredWood.Arrow.ArrowSizes.EstimatedBytes(batch);
        }
        return decoded;
    }

    [Fact]
    public async Task TheDecodedEstimate_CoversWhatDictionaryEncodedNumbersDecodeTo()
    {
        // #829: a column of few distinct values encodes to a sliver of its decoded size, so the footer's
        // uncompressed size reserved a fraction of the first batch, and a wide window's first wave overran the
        // budget. The estimate sizes fixed-width columns by their Arrow width instead.
        const int rows = EncodedRows;
        await using var file = new LocalRandomAccessFile(await WriteEncodedAsync());
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var group = (await reader.ReadMetadataAsync()).RowGroups[0];
        var parquetSchema = await reader.GetSchemaAsync();
        long decoded = await DecodedSizeAsync(reader);

        long footer = ParquetFileReader.PlannedBytes(group, parquetSchema, null, null).Uncompressed;
        long estimate = reader.DecodedBytes(group, parquetSchema, null, null);
        Assert.True(footer < decoded / 4, $"the footer says {footer}, the batch decodes to {decoded}: no gap to test");
        Assert.True(estimate >= decoded, $"estimates {estimate}, decodes to {decoded}");
        Assert.True(estimate <= decoded * 11 / 10, $"estimates {estimate}, decodes to {decoded}: fixed widths are exact");

        // Ranges scale it by the rows they keep.
        RowRange[] quarter = [new RowRange(0, rows / 4)];
        long scaled = reader.DecodedBytes(group, parquetSchema, null, quarter);
        Assert.InRange(scaled, estimate / 4 - 64, estimate / 4 + 64);

        // The first batch reserves its own share of the group, not the whole of it: with BatchSize cutting the
        // group in four, a quarter (here above the footer's size, which is the floor).
        Assert.Equal(estimate, reader.FirstBatchBytes(estimate, footer, group, parquetSchema, null, null));
        using var batched = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { BatchSize = rows / 4 });
        Assert.InRange(batched.FirstBatchBytes(estimate, footer, group, parquetSchema, null, null),
            estimate / 4 - 1, estimate / 4 + 1);
    }

    /// <summary>The reservation a reader makes for the first batch of row group 0 of <paramref name="path"/>,
    /// and the size of the first batch it actually decodes.</summary>
    private static async Task<(long Reserved, long FirstBatch)> FirstBatchAsync(string path, ParquetReadOptions options)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, options);
        var group = (await reader.ReadMetadataAsync()).RowGroups[0];
        var schema = await reader.GetSchemaAsync();
        long footer = ParquetFileReader.PlannedBytes(group, schema, null, null).Uncompressed;
        long reserved = reader.FirstBatchBytes(reader.DecodedBytes(group, schema, null, null), footer, group, schema, null, null);
        await foreach (var batch in reader.ReadAllAsync())
        {
            using (batch)
                return (reserved, EngineeredWood.Arrow.ArrowSizes.EstimatedBytes(batch));
        }
        throw new InvalidOperationException("No batch.");
    }

    [Fact]
    public async Task TheFirstBatchReservation_CoversAPageLargerThanTheBatch()
    {
        // #845 review: a batch is a slice of whole decoded pages and holds their buffers. One PLAIN 100,000-row
        // double page read in 1,000-row batches decodes the whole 800 KB page for the first batch, so reserving the
        // batch's 1/100 share of rows (8 KB) under-reserved what main had reserved by the footer. (An encoding that
        // shrinks the page shrinks the footer too, and is still under-reserved, as on main: how many rows a page
        // holds is not known before the page maps are read.)
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("id", DoubleType.Default, false)).Build();
        var ids = new DoubleArray.Builder();
        var random = new Random(845);
        for (int i = 0; i < 100_000; i++)
            ids.Append(random.NextDouble());
        string path = Path.Combine(_tempDir, "one-page.parquet");
        using (var batch = new RecordBatch(schema, [ids.Build()], 100_000))
        await using (var output = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(output, ownsFile: false, ParquetWriteOptions.Default with
                     {
                         DictionaryEnabled = false,
                         DataPageSize = 16 << 20,
                         DataPageRowCountLimit = 1_000_000,
                     }))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        var (reserved, first) = await FirstBatchAsync(path, new ParquetReadOptions { BatchSize = 1_000 });
        Assert.True(first > 100_000 * 4, $"the first batch holds {first} bytes: not a page larger than the batch");
        Assert.True(reserved >= first, $"reserved {reserved} for a first batch of {first}");
    }

    [Fact]
    public async Task TheFirstBatchReservation_CoversAMaxBatchByteSizeBetweenFooterAndDecodedSize()
    {
        // #845 review: MaxBatchByteSize cuts batches by the pages' ENCODED sizes. A dictionary-encoded group under the
        // limit is read in one batch however large it decodes, so capping the reservation at the limit
        // under-reserved it.
        string path = await WriteEncodedAsync();
        long footer, decoded;
        await using (var file = new LocalRandomAccessFile(path))
        using (var reader = new ParquetFileReader(file, ownsFile: false))
        {
            footer = ParquetFileReader.PlannedBytes(
                (await reader.ReadMetadataAsync()).RowGroups[0], await reader.GetSchemaAsync(), null, null).Uncompressed;
            decoded = await DecodedSizeAsync(reader);
        }
        long limit = footer * 2;
        Assert.True(limit < decoded, $"limit {limit} is not below the decoded size {decoded}");

        var (reserved, first) = await FirstBatchAsync(path, new ParquetReadOptions { MaxBatchByteSize = limit });
        Assert.True(first > limit, $"the first batch holds {first} bytes, within the limit {limit}: nothing to test");
        Assert.True(reserved >= first, $"reserved {reserved} for a first batch of {first}");
    }

    /// <summary>100,000 random doubles, PLAIN, in pages of <paramref name="pageRows"/> rows.</summary>
    private async Task<string> WriteDoublesAsync(string name, int pageRows)
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("id", DoubleType.Default, false)).Build();
        var ids = new DoubleArray.Builder();
        var random = new Random(846);
        for (int i = 0; i < 100_000; i++)
            ids.Append(random.NextDouble());
        string path = Path.Combine(_tempDir, name);
        using (var batch = new RecordBatch(schema, [ids.Build()], 100_000))
        await using (var output = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(output, ownsFile: false, ParquetWriteOptions.Default with
                     {
                         DictionaryEnabled = false,
                         DataPageSize = 16 << 20,
                         DataPageRowCountLimit = pageRows,
                     }))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }
        return path;
    }

    /// <summary>What a parallel read of row group 0 of <paramref name="path"/> reserves for its first batch, and
    /// the size of the first batch the read decodes.</summary>
    private static async Task<(long Reserved, long FirstBatch)> PlannedFirstBatchAsync(
        string path, ParquetReadOptions options, IReadOnlyList<RowRange>? ranges = null)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, options);
        var plan = new ParquetFileReader.RowGroupPlan(0, 0, ranges, null);
        long reserved = (await reader.FirstBatchReservationAsync(
            plan, null, new ParquetFileReader.DecodedGrowth(), CancellationToken.None)).Reserved;
        await foreach (var positioned in reader.ReadPlannedAsync(plan, null, CancellationToken.None))
        {
            using (positioned.Batch)
                return (reserved, EngineeredWood.Arrow.ArrowSizes.EstimatedBytes(positioned.Batch));
        }
        throw new InvalidOperationException("No batch.");
    }

    [Fact]
    public async Task TheFirstBatchReservation_FromTheOffsetIndex_CoversADictionaryPageLargerThanTheBatch()
    {
        // #846: a dictionary shrinks the pages, and the footer with them, so a batch of 1,000 rows that holds a page
        // of many more decoded rows reserved too little. The OffsetIndex says how many rows each page holds.
        string path = await WriteEncodedAsync();
        var options = new ParquetReadOptions { BatchSize = 1_000 };
        var (footerBased, _) = await FirstBatchAsync(path, options);
        var (reserved, first) = await PlannedFirstBatchAsync(path, options);
        Assert.True(footerBased < first, $"the footer reserved {footerBased} for {first}: no shortfall to test");
        Assert.True(reserved >= first, $"reserved {reserved} for a first batch of {first}");
        Assert.True(reserved <= first * 5 / 4, $"reserved {reserved} for a first batch of {first}");
    }

    [Fact]
    public async Task TheFirstBatchReservation_FromTheOffsetIndex_CoversARangeInsideALargePage()
    {
        // #846: a read of 1,000 rows from one 100,000-row page decodes and holds the whole page; scaled by the rows
        // kept, the reservation was 1% of it.
        string path = await WriteDoublesAsync("one-page-range.parquet", pageRows: 1_000_000);
        RowRange[] ranges = [new RowRange(50_000, 51_000)];
        var (reserved, first) = await PlannedFirstBatchAsync(path, ParquetReadOptions.Default, ranges);
        Assert.True(first > 100_000 * 4, $"the first batch holds {first} bytes: not the whole page");
        Assert.True(reserved >= first, $"reserved {reserved} for a first batch of {first}");
    }

    /// <summary>Reads <paramref name="plan"/> as one unit of a parallel read that shares <paramref name="growth"/>,
    /// and returns the size of its first batch.</summary>
    private static async Task<long> ReadUnitAsync(
        ParquetFileReader reader, ParquetFileReader.RowGroupPlan plan, ParquetFileReader.DecodedGrowth growth)
    {
        long first = -1;
        await foreach (var positioned in reader.ReadPlannedAsync(
                           plan, null, EngineeredWood.Scans.ScanReservation.None, growth, CancellationToken.None))
        {
            using (positioned.Batch)
            {
                if (first < 0)
                    first = EngineeredWood.Arrow.ArrowSizes.EstimatedBytes(positioned.Batch);
            }
        }
        return first;
    }

    [Fact]
    public async Task TheFirstBatchReservation_FromTheOffsetIndex_GrowsAgainstItsOwnBaseline()
    {
        // #847 review: growth was learnt against the rows a range keeps, and applied to the pages it touches. A
        // 1,000-row range inside a 100,000-row page learnt ~100x, and the next such range reserved ~100x its page.
        string path = await WriteDoublesAsync("one-page-growth.parquet", pageRows: 1_000_000);
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var growth = new ParquetFileReader.DecodedGrowth();
        var plan = new ParquetFileReader.RowGroupPlan(0, 0, [new RowRange(50_000, 51_000)], null);

        long first = await ReadUnitAsync(reader, plan, growth);
        long reserved = (await reader.FirstBatchReservationAsync(plan, null, growth, CancellationToken.None)).Reserved;
        Assert.True(reserved >= first, $"reserved {reserved} for a first batch of {first}");
        Assert.True(reserved <= first * 5 / 4, $"reserved {reserved} for a first batch of {first}");
    }

    /// <summary>100,000 strings in pages of 1,000 rows, written with <paramref name="options"/>.</summary>
    private async Task<string> WriteStringsAsync(string name, Func<int, string> value, ParquetWriteOptions options)
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("s", StringType.Default, false)).Build();
        var values = new StringArray.Builder();
        for (int i = 0; i < 100_000; i++)
            values.Append(value(i));
        string path = Path.Combine(_tempDir, name);
        using (var batch = new RecordBatch(schema, [values.Build()], 100_000))
        await using (var output = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(output, ownsFile: false, options with
                     {
                         DataPageSize = 16 << 20,
                         DataPageRowCountLimit = 1_000,
                     }))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }
        return path;
    }

    [Fact]
    public async Task TheFirstBatchReservation_FromTheOffsetIndex_CoversACompressibleFirstPage()
    {
        // #847 review: a page's uncompressed size is estimated by the chunk's compression ratio, so a first page that
        // compresses far better than the rest is undercounted.
        var random = new Random(847);
        string path = await WriteStringsAsync(
            "compressible-first.parquet",
            i => i < 1_000 ? new string('a', 100) : random.Next().ToString("x8") + random.NextDouble(),
            ParquetWriteOptions.Default with { DictionaryEnabled = false, Compression = CompressionCodec.Zstd });
        var options = new ParquetReadOptions { BatchSize = 1_000 };
        var (footerBased, _) = await FirstBatchAsync(path, options);
        var (reserved, first) = await PlannedFirstBatchAsync(path, options);
        Assert.True(reserved >= first, $"reserved {reserved} (footer: {footerBased}) for a first batch of {first}");
    }

    [Fact]
    public async Task TheFirstBatchReservation_FromTheOffsetIndex_CoversDictionaryStrings()
    {
        // #847 review: a dictionary-encoded string page holds indices; the strings they decode to are longer, and
        // nothing before the decode says how much (the footer fell short as well). A batched read never learnt it
        // (#846 gap 1): growth was measured only from a row group read as one batch. A first batch sized from its
        // pages is measured against that size, so the next unit of the read reserves enough.
        string[] distinct = Enumerable.Range(0, 8).Select(k => new string((char)('a' + k), 200)).ToArray();
        string path = await WriteStringsAsync(
            "dictionary-strings.parquet", i => distinct[i % distinct.Length], ParquetWriteOptions.Default);
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { BatchSize = 1_000 });
        var growth = new ParquetFileReader.DecodedGrowth();
        var plan = new ParquetFileReader.RowGroupPlan(0, 0, null, null);

        long before = (await reader.FirstBatchReservationAsync(plan, null, growth, CancellationToken.None)).Reserved;
        long first = await ReadUnitAsync(reader, plan, growth);
        Assert.True(before < first, $"reserved {before} for a first batch of {first}: nothing to learn");
        long reserved = (await reader.FirstBatchReservationAsync(plan, null, growth, CancellationToken.None)).Reserved;
        Assert.True(reserved >= first, $"reserved {reserved} for a first batch of {first}");
        Assert.True(reserved <= first * 5 / 4, $"reserved {reserved} for a first batch of {first}");
    }

    [Fact]
    public async Task TheFirstBatchReservation_FromTheOffsetIndex_IsNotFlooredAtTheFooter()
    {
        // #846: without page sizes, a batched read's first batch never reserved less than the footer's size of the
        // whole group (#845 review), which cost a wide window its batched gain. With pages of 1,000 rows, a
        // 1,000-row batch holds one page, and reserves about that.
        string path = await WriteDoublesAsync("small-pages.parquet", pageRows: 1_000);
        var options = new ParquetReadOptions { BatchSize = 1_000 };
        var (footerBased, _) = await FirstBatchAsync(path, options);
        var (reserved, first) = await PlannedFirstBatchAsync(path, options);
        Assert.True(reserved >= first, $"reserved {reserved} for a first batch of {first}");
        Assert.True(reserved <= first * 5 / 4, $"reserved {reserved} for a first batch of {first}");
        Assert.True(footerBased > first * 10, $"the footer reserved {footerBased} for {first}: no floor to lift");
    }

    [Fact]
    public async Task TheDecodedEstimate_SizesAnExtensionTypeByItsStorage()
    {
        // #845 review round 2: a registered arrow.uuid column is decoded as FixedSizeBinary(16) storage. Matched only
        // by its outer type, it fell back to the footer's size, which a dictionary shrinks to a sliver.
        const int rows = 50_000;
        Guid[] distinct = [Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid()];
        var builder = new GuidArray.Builder();
        for (int i = 0; i < rows; i++)
            builder.Append(distinct[i % distinct.Length]);
        var guids = builder.Build(allocator: null);
        var schema = new Apache.Arrow.Schema([new Field("g", guids.Data.DataType, nullable: false)], metadata: null);
        string path = Path.Combine(_tempDir, "guids.parquet");
        using (var batch = new RecordBatch(schema, [guids], rows))
        await using (var output = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(output, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        var registry = new ExtensionTypeRegistry();
        registry.Register(GuidExtensionDefinition.Instance);
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { ExtensionRegistry = registry });
        var group = (await reader.ReadMetadataAsync()).RowGroups[0];
        var parquetSchema = await reader.GetSchemaAsync();
        long decoded = 0;
        await foreach (var batch in reader.ReadAllAsync())
        {
            using (batch)
            {
                Assert.IsType<GuidArray>(batch.Column(0));
                decoded += EngineeredWood.Arrow.ArrowSizes.EstimatedBytes(batch);
            }
        }

        long footer = ParquetFileReader.PlannedBytes(group, parquetSchema, null, null).Uncompressed;
        long estimate = reader.DecodedBytes(group, parquetSchema, null, null);
        Assert.True(footer < decoded / 4, $"the footer says {footer}, the batch decodes to {decoded}: no gap to test");
        Assert.True(estimate >= decoded, $"estimates {estimate}, decodes to {decoded}");
    }

    [Fact]
    public async Task APlannedRead_WaitsForRoomForWhatItWillDecode_NotWhatTheFooterSays()
    {
        // #829: room for the footer's size, beside the front unit, is not room for the batch. A read that took it
        // decoded at once and overran the budget by the difference; a wide window's whole first wave did.
        await using var file = new LocalRandomAccessFile(await WriteEncodedAsync());
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var group = (await reader.ReadMetadataAsync()).RowGroups[0];
        var (footer, compressed) = ParquetFileReader.PlannedBytes(group, await reader.GetSchemaAsync(), null, null);
        long decoded = await DecodedSizeAsync(reader);
        Assert.True(footer * 2 < decoded, $"the footer says {footer}, the batch decodes to {decoded}: no gap to test");
        ParquetFileReader.RowGroupPlan? plan = null;
        await foreach (var p in reader.PlanRowGroupsAsync(null, null, null, default))
            plan ??= p;

        // Unit 0, the front, holds a batch; beside it there is room for unit 1's hold and twice its footer size,
        // but not for the batch it will decode.
        const long front = 1L << 20;
        var budget = new EngineeredWood.Scans.ScanMemoryBudget(front + compressed + 2 * footer);
        budget.SetHead(0);
        await budget.ReserveAsync(0, front, CancellationToken.None);
        var reservation = new EngineeredWood.Scans.ScanReservation(budget, 1);

        await using var e = reader.ReadPlannedAsync(
                plan!.Value, null, reservation, new ParquetFileReader.DecodedGrowth(), default)
            .GetAsyncEnumerator();
        var next = e.MoveNextAsync().AsTask();
        await Task.WhenAny(next, Task.Delay(TimeSpan.FromSeconds(1)));
        bool waited = !next.IsCompleted;

        // The consumer takes unit 0's batch and moves on to unit 1, which as the front is admitted however large.
        budget.Adjust(-front);
        budget.SetHead(1);
        Assert.Same(next, await Task.WhenAny(next, Task.Delay(TimeSpan.FromSeconds(30))));
        Assert.True(waited, "the read should wait for room for its decoded size");
        Assert.True(await next);
        e.Current.Batch.Dispose();
        Assert.False(await e.MoveNextAsync());
    }

    [Fact]
    public async Task APlannedRead_ReservesForItsRawBatches_WhateverTheCallerDoesWithThem()
    {
        // One row group of dictionary-encoded strings, read in four batches: each DECODED batch is far larger
        // than the footer's uncompressed size, which counts the dictionary once and the indices.
        var strings = Enumerable.Range(0, 50).Select(i => new string((char)('a' + i % 26), 200) + i).ToArray();
        var values = new StringArray.Builder();
        for (int i = 0; i < 40_000; i++)
            values.Append(strings[i % strings.Length]);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("s", StringType.Default, false)).Build();
        string path = Path.Combine(_tempDir, "dictionary.parquet");
        using (var batch = new RecordBatch(schema, [values.Build()], 40_000))
        await using (var output = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(output, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { BatchSize = 10_000 });
        ParquetFileReader.RowGroupPlan? first = null;
        await foreach (var p in reader.PlanRowGroupsAsync(null, null, null, default))
        {
            first ??= p;
        }
        var plan = first!.Value;
        var budget = new EngineeredWood.Scans.ScanMemoryBudget(1L << 30);
        var reservation = new EngineeredWood.Scans.ScanReservation(budget, 0);

        long largest = 0;
        int batches = 0;
        await foreach (var positioned in reader.ReadPlannedAsync(
                           plan, null, reservation, new ParquetFileReader.DecodedGrowth(), default))
        {
            using (positioned.Batch)
                largest = Math.Max(largest, EngineeredWood.Arrow.ArrowSizes.EstimatedBytes(positioned.Batch));
            batches++;
            // The caller hands the scheduler nothing here, as a Delta scan hands it only what survives a
            // deletion vector: the read itself must have learnt the raw size.
        }
        reservation.ReleasePending();

        Assert.Equal(4, batches);
        Assert.True(largest > ParquetFileReader.PlannedBytes(
            (await reader.ReadMetadataAsync()).RowGroups[0], await reader.GetSchemaAsync(), null, null).Uncompressed);
        Assert.True(reservation.Estimate >= largest, $"reserves {reservation.Estimate}, raw batches reach {largest}");
        Assert.Equal(0, budget.Used);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task APlannedRead_FetchesItsChunks_WhileItWaitsForRoomToDecode(bool unselectiveFilter)
    {
        // #829: a row group whose hold fits the budget but whose decode does not yet must still fetch its chunks
        // while it waits, so the fetch overlaps the decode ahead of it. Reserving before fetching made a scan whose
        // budget takes one decoded row group at a time run serially at fetch-plus-decode per row group.
        long dataOffset = await DataOffsetAsync(1);
        await using var local = new LocalRandomAccessFile(_path);
        var watched = new WatchingFile(local, dataOffset);
        using var reader = new ParquetFileReader(watched, ownsFile: false);
        // A filter that prunes nothing plans the group as one range of every row rather than none (#843 review):
        // that is still a whole-group read, and fetches ahead the same way.
        var filter = unselectiveFilter ? Ex.GreaterThanOrEqual("id", LiteralValue.Of(0L)) : null;
        ParquetFileReader.RowGroupPlan? plan = null;
        await foreach (var p in reader.PlanRowGroupsAsync(filter, null, null, default))
        {
            if (p.Index == 1)
                plan = p;
        }
        if (unselectiveFilter)
        {
            var ranges = Assert.Single(plan!.Value.Ranges!);
            Assert.Equal(new RowRange(0, _groupStarts[2] - _groupStarts[1]), ranges);
        }
        var (uncompressed, compressed) = ParquetFileReader.PlannedBytes(
            (await reader.ReadMetadataAsync()).RowGroups[1], await reader.GetSchemaAsync(), null, null);

        // Unit 0, the front, holds a decoded batch; unit 1's hold fits beside it, its decode does not.
        const long front = 1L << 20;
        var budget = new EngineeredWood.Scans.ScanMemoryBudget(front + compressed + uncompressed / 2);
        budget.SetHead(0);
        await budget.ReserveAsync(0, front, CancellationToken.None);
        var reservation = new EngineeredWood.Scans.ScanReservation(budget, 1);

        await using var e = reader.ReadPlannedAsync(
                plan!.Value, null, reservation, new ParquetFileReader.DecodedGrowth(), default)
            .GetAsyncEnumerator();
        var next = e.MoveNextAsync().AsTask();

        bool fetched = await Task.WhenAny(watched.Requested, Task.Delay(TimeSpan.FromSeconds(10))) == watched.Requested;
        bool waited = !next.IsCompleted;

        // The consumer takes unit 0's batch, which lets unit 1 decode however the read went, so the enumerator is
        // never disposed with a MoveNextAsync pending.
        budget.Adjust(-front);
        Assert.Same(next, await Task.WhenAny(next, Task.Delay(TimeSpan.FromSeconds(30))));
        Assert.True(fetched, "the chunks should be requested while the decode waits for room");
        Assert.True(waited, "the decode should have waited for room");
        Assert.True(await next);
        using (e.Current.Batch)
        {
            Assert.Equal(_groupStarts[1], e.Current.FirstRow);
            Assert.Equal(_groupStarts[2] - _groupStarts[1], e.Current.Batch.Length);
        }
        Assert.False(await e.MoveNextAsync());
    }

    /// <summary>Completes <see cref="Requested"/> once a request covers the byte at <c>offset</c>.</summary>
    private sealed class WatchingFile(IRandomAccessFile inner, long offset) : IRandomAccessFile
    {
        private readonly TaskCompletionSource<bool> _requested = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Requested => _requested.Task;

        private void See(FileRange range)
        {
            if (range.Offset <= offset && offset < range.Offset + range.Length)
                _requested.TrySetResult(true);
        }

        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) =>
            inner.GetLengthAsync(cancellationToken);

        public ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default)
        {
            See(range);
            return inner.ReadAsync(range, cancellationToken);
        }

        public ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
        {
            foreach (var range in ranges)
                See(range);
            return inner.ReadRangesAsync(ranges, cancellationToken);
        }

        public ValueTask DisposeAsync() => default;

        public void Dispose()
        {
        }
    }

    private async Task<long> DataOffsetAsync(int rowGroup)
    {
        await using var file = new LocalRandomAccessFile(_path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var meta = metadata.RowGroups[rowGroup].Columns[0].MetaData!;
        return meta.DictionaryPageOffset ?? meta.DataPageOffset;
    }

    /// <summary>Fails every request that covers the byte at <c>failAt</c>.</summary>
    private sealed class FailingFile(IRandomAccessFile inner, long failAt) : IRandomAccessFile
    {
        public const string Message = "Injected read failure.";

        private bool Covers(FileRange range) => range.Offset <= failAt && failAt < range.Offset + range.Length;

        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) =>
            inner.GetLengthAsync(cancellationToken);

        public ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default) =>
            Covers(range) ? throw new IOException(Message) : inner.ReadAsync(range, cancellationToken);

        public ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default) =>
            ranges.Any(Covers) ? throw new IOException(Message) : inner.ReadRangesAsync(ranges, cancellationToken);

        public ValueTask DisposeAsync() => default;

        public void Dispose()
        {
        }
    }
}

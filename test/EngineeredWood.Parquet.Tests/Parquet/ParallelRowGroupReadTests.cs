// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using Apache.Arrow;
using Apache.Arrow.Types;
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

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// A selected read that may go in two passes plans with its string columns' OffsetIndexes read ahead, one request for
/// a window of row groups, so that the second pass's page maps, built from the page headers, size their string slices
/// from the per-page counts (#894).
/// </summary>
public sealed class StringIndexReadAheadTests : IDisposable
{
    private const int Groups = 6;
    private const int GroupRows = 10_000;

    private readonly string _tempDir =
        Path.Combine(Path.GetTempPath(), "ew-string-index-ahead-" + Guid.NewGuid().ToString("N")[..8]);

    public StringIndexReadAheadTests() => Directory.CreateDirectory(_tempDir);

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

    [Fact]
    public async Task ThePlans_ShareOneReadOfTheirStringColumnsIndexes()
    {
        string path = await WriteAsync();
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false) { TwoPassReads = true };
        var metadata = await reader.ReadMetadataAsync();

        var plans = await PlanAsync(reader, prune: null);

        Assert.Equal(Groups, plans.Count);
        var read = plans[0].StringIndexes;
        Assert.NotNull(read);
        Assert.All(plans, p => Assert.Same(read, p.StringIndexes));
        Assert.All(plans, p => Assert.Null(p.OffsetIndexes));
        // Dictionary v and DELTA_BYTE_ARRAY x; neither the int64 k nor the PLAIN w, whose slices are sized already.
        Assert.Equal(IndexOffsets(metadata, Enumerable.Range(0, Groups), "v", "x"), (await read!)!.Keys.OrderBy(o => o));
    }

    [Fact]
    public async Task AWindowPastItsBudget_LeavesTheRestToTheNext()
    {
        string path = await WriteAsync();
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false)
        {
            TwoPassReads = true,
            // Each group's indexes alone are past it: a window of one group each, as the first is read whatever it is.
            PageIndexReadAheadBudgetBytes = 1,
        };
        var metadata = await reader.ReadMetadataAsync();

        var plans = await PlanAsync(reader, prune: null);

        Assert.Equal(Groups, plans.Select(p => p.StringIndexes).Distinct().Count());
        foreach (var plan in plans)
            Assert.Equal(IndexOffsets(metadata, [plan.Index], "v", "x"), (await plan.StringIndexes!)!.Keys.OrderBy(o => o));
    }

    [Fact]
    public async Task GroupsThePruneRulesOutOrReadsTheIndexesOf_AreLeftOut()
    {
        // k >= 25,000: statistics rule out groups 0 and 1, leave group 2 undecided, so its page index is read (with
        // its projection's OffsetIndexes), and prove groups 3-5 true, whose page index is never read.
        string path = await WriteAsync();
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false) { TwoPassReads = true };
        var metadata = await reader.ReadMetadataAsync();

        var plans = await PlanAsync(reader, Expressions.Expressions.GreaterThanOrEqual("k", LiteralValue.Of(25_000L)));

        Assert.Equal([2, 3, 4, 5], plans.Select(p => p.Index));
        Assert.NotNull(plans[0].OffsetIndexes);
        Assert.Null(plans[0].StringIndexes);
        var read = plans[1].StringIndexes;
        Assert.All(plans.Skip(1), p => Assert.Same(read, p.StringIndexes));
        Assert.Equal(IndexOffsets(metadata, [3, 4, 5], "v", "x"), (await read!)!.Keys.OrderBy(o => o));
    }

    [Fact]
    public async Task GroupsAProofReadsInOnePass_AreLeftOut()
    {
        // The row filter keeps k >= 30,000, with that as its proof: statistics prove it for groups 3-5, which are then
        // read without it, so in one pass, and never await a read-ahead (#896 review). Groups 0-2 still filter.
        string path = await WriteAsync();
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false) { TwoPassReads = true };
        var metadata = await reader.ReadMetadataAsync();
        var proof = Expressions.Expressions.GreaterThanOrEqual("k", LiteralValue.Of(30_000L));
        var predicate = new RowPredicate(["k"], ParallelRowGroupReadTests.KeyPredicate(k => k >= 30_000, valueWise: false).Evaluate)
        {
            Proof = proof,
        };
        var selected = await reader.PrepareSelectedAsync(Scan(null) with { RowFilter = new RowFilter(predicate) }, default);

        var plans = new List<ParquetFileReader.RowGroupPlan>();
        await foreach (var plan in reader.PlanRowGroupsAsync(
                           null, selected.ReadColumns, null, default, stringIndexColumns: selected.StringIndexColumns))
        {
            plans.Add(plan);
        }

        Assert.Equal(Groups, plans.Count);
        var read = plans[0].StringIndexes;
        Assert.NotNull(read);
        Assert.All(plans.Take(3), p => Assert.Same(read, p.StringIndexes));
        Assert.All(plans.Skip(3), p => Assert.Null(p.StringIndexes));
        Assert.Equal(IndexOffsets(metadata, [0, 1, 2], "v", "x"), (await read!)!.Keys.OrderBy(o => o));
    }

    [Fact]
    public async Task GroupsOnlyAProofLetGoInTwoPasses_AreReadAhead()
    {
        // Read as string views, v cannot come back from a two-pass read as a returned predicate column (its gather
        // shares buffers), so the scan as a whole reads in one pass. Where the proof k >= 30,000 holds (groups 3-5), the
        // predicate on k and v is dropped, v is an output column, and those groups go in two passes (#896 review).
        string path = await WriteAsync();
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(
            file, ownsFile: false, new ParquetReadOptions { ByteArrayOutput = ByteArrayOutputKind.ViewType })
        {
            TwoPassReads = true,
        };
        var metadata = await reader.ReadMetadataAsync();
        var proven = new RowPredicate(["k", "v"], ParallelRowGroupReadTests.KeyPredicate(k => k >= 30_000, valueWise: false).Evaluate)
        {
            Proof = Expressions.Expressions.GreaterThanOrEqual("k", LiteralValue.Of(30_000L)),
        };
        var leading = ParallelRowGroupReadTests.KeyPredicate(k => k % GroupRows < 3_000, valueWise: false);
        var selected = await reader.PrepareSelectedAsync(
            Scan(null) with { RowFilter = new RowFilter([proven, leading]) }, default);

        var plans = await PlanAsync(reader, selected, prune: null);

        Assert.Equal(Groups, plans.Count);
        Assert.All(plans.Take(3), p => Assert.Null(p.StringIndexes));
        var read = plans[3].StringIndexes;
        Assert.NotNull(read);
        Assert.All(plans.Skip(3), p => Assert.Same(read, p.StringIndexes));
        Assert.Equal(IndexOffsets(metadata, [3, 4, 5], "v", "x"), (await read!)!.Keys.OrderBy(o => o));
    }

    [Fact]
    public async Task APredicateOnlyStringColumn_IsNotReadAhead()
    {
        // The predicate reads x, which the scan does not return: only the first pass decodes it, so its OffsetIndexes
        // would size nothing, and would take a share of the window's budget (#896 review).
        string path = await WriteAsync();
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false) { TwoPassReads = true };
        var metadata = await reader.ReadMetadataAsync();
        var onX = new RowPredicate(["x"], batch =>
        {
            var keep = new BooleanArray.Builder().Reserve(batch.Length);
            for (int i = 0; i < batch.Length; i++)
                keep.Append(i % 3 == 0);
            return keep.Build();
        });
        var selected = await reader.PrepareSelectedAsync(
            Scan(null) with { Columns = ["v", "w"], RowFilter = new RowFilter(onX) }, default);

        var plans = await PlanAsync(reader, selected, prune: null);

        var read = plans[0].StringIndexes;
        Assert.NotNull(read);
        Assert.Equal(IndexOffsets(metadata, Enumerable.Range(0, Groups), "v"), (await read!)!.Keys.OrderBy(o => o));
    }

    [Fact]
    public async Task AGroupWhosePageIndexLiesOutsideTheFile_IsReadAhead()
    {
        // Group 2's k ColumnIndex is placed past the end of the file. Statistics leave k >= 25,000 undecided there, but
        // the narrowing refuses an index outside the file and reads no OffsetIndexes, so the read-ahead must cover the
        // group rather than leave it to the narrowing (#896 review).
        string path = await WriteAsync();
        MoveColumnIndexOutside(path, group: 2, column: "k");
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false) { TwoPassReads = true };
        var metadata = await reader.ReadMetadataAsync();

        var plans = await PlanAsync(reader, Expressions.Expressions.GreaterThanOrEqual("k", LiteralValue.Of(25_000L)));

        Assert.Equal([2, 3, 4, 5], plans.Select(p => p.Index));
        Assert.Null(plans[0].OffsetIndexes);
        var read = plans[0].StringIndexes;
        Assert.NotNull(read);
        Assert.All(plans, p => Assert.Same(read, p.StringIndexes));
        Assert.Equal(IndexOffsets(metadata, [2, 3, 4, 5], "v", "x"), (await read!)!.Keys.OrderBy(o => o));
    }

    [Fact]
    public async Task AReadInOnePass_ReadsNothingAhead()
    {
        string path = await WriteAsync();
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false) { TwoPassReads = false };
        await reader.ReadMetadataAsync();

        Assert.All(await PlanAsync(reader, prune: null), p => Assert.Null(p.StringIndexes));
    }

    [Fact]
    public async Task TheReadAhead_IsOneRequestForTheFile()
    {
        string path = await WriteAsync();
        int Requests(bool ahead, out long rows)
        {
            using var local = new LocalRandomAccessFile(path);
            var counting = new RequestCountingFile(local);
            using var reader = new ParquetFileReader(
                counting, ownsFile: false, new ParquetReadOptions { MaxConcurrentRowGroups = 1 })
            {
                TwoPassReads = true,
                ReadsStringIndexesAhead = ahead,
            };
            rows = ReadAsync(reader).GetAwaiter().GetResult();
            return counting.Requests.Count;
        }

        int without = Requests(false, out long rowsWithout);
        int with = Requests(true, out long rowsWith);

        Assert.Equal(rowsWithout, rowsWith);
        Assert.Equal(without + 1, with);
    }

    [Fact]
    public async Task AReadAheadThatFails_LeavesTheReadRight()
    {
        string path = await WriteAsync();
        await using var local = new LocalRandomAccessFile(path);
        long expected = await ReadAsync(new ParquetFileReader(local, ownsFile: false) { TwoPassReads = false });

        // Every request touching the string columns' OffsetIndexes fails: only the read-ahead makes one.
        await using var failing = new FailingIndexReads(local);
        using var failingReader = new ParquetFileReader(failing, ownsFile: false) { TwoPassReads = true };
        var metadata = await failingReader.ReadMetadataAsync();
        foreach (long offset in IndexOffsets(metadata, Enumerable.Range(0, Groups), "v", "x"))
            failing.Refused.Add(offset);

        Assert.Equal(expected, await ReadAsync(failingReader));
        Assert.True(failing.Refusals > 0, "nothing read the string columns' OffsetIndexes: the read-ahead never ran");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ADisposedReader_StopsTheReadsNothingAwaited(bool async)
    {
        // The first pass selects no row, so no second pass awaits the read-ahead, which hangs at the store until it is
        // cancelled. The reader's disposal must stop it and wait for it: the file closes next (#896 review).
        string path = await WriteAsync();
        await using var local = new LocalRandomAccessFile(path);
        await using var hanging = new HangingIndexReads(local);
        // Serial: a parallel scan cancels its own token as it ends, which stops the read too, though nothing awaits it.
        // A serial one reads under the caller's token, so only the disposal can stop it.
        var reader = new ParquetFileReader(hanging, ownsFile: false, new ParquetReadOptions { MaxConcurrentRowGroups = 1 })
        {
            TwoPassReads = true,
        };
        var metadata = await reader.ReadMetadataAsync();
        foreach (long offset in IndexOffsets(metadata, Enumerable.Range(0, Groups), "v", "x"))
            hanging.Hung.Add(offset);

        var scan = Scan(null) with
        {
            RowFilter = new RowFilter(ParallelRowGroupReadTests.KeyPredicate(_ => false, valueWise: false)),
        };
        long rows = 0;
        await foreach (var selected in reader.ReadSelectedAsync(scan))
        {
            using (selected)
                rows += selected.Batch.Length;
        }
        Assert.Equal(0, rows);
        Assert.Same(hanging.Started.Task, await Task.WhenAny(hanging.Started.Task, Task.Delay(TimeSpan.FromSeconds(30))));
        Assert.Equal(1, hanging.InFlight);

        if (async)
            await reader.DisposeAsync();
        else
            reader.Dispose();

        Assert.Equal(0, hanging.InFlight);
        Assert.True(hanging.Cancelled, "the read was not cancelled");
    }

    /// <summary>
    /// #900 review: a two-pass read whose first pass is cut into batches sizes both passes from their OffsetIndexes
    /// before its first reservation. The string columns' indexes the planner read ahead for the second pass are used,
    /// not fetched and allocated again.
    /// </summary>
    [Fact]
    public async Task SizingTheTwoPasses_ReusesTheStringIndexesReadAhead()
    {
        string path = await WriteAsync();
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { BatchSize = 1_000 })
        {
            TwoPassReads = true,
        };
        var metadata = await reader.ReadMetadataAsync();
        var plans = await PlanAsync(reader, prune: null);
        var ahead = await plans[0].StringIndexes!;
        Assert.NotNull(ahead);
        Assert.Null(plans[0].OffsetIndexes);

        var pages = await reader.TwoPassFirstBatchesFromPagesAsync(
            plans[0], metadata.RowGroups[0], await reader.GetSchemaAsync(),
            new ParquetFileReader.TwoPassColumns(["k"], ["v", "w", "x"], [], ["v", "w", "x"]), null, default);

        Assert.NotNull(pages.OutputBytes);
        // The read-ahead's own bytes, handed on for the second pass's page maps: not a second fetch of them. (A request
        // for k's and w's indexes may still span theirs, merged across the small gap between them: one request either
        // way.)
        Assert.All(IndexOffsets(metadata, [0], "v", "x"), o => Assert.Same(ahead![o], pages.OffsetIndexes![o]));
    }

    // The selected read of v, w and x where k's position in its group is under 3,000: a leading run, so the second
    // pass reads only some blocks, in slices cut by its page maps. Returns a checksum of the rows and their strings.
    private static async Task<long> ReadAsync(ParquetFileReader reader)
    {
        using (reader)
        {
            long sum = 0;
            await foreach (var selected in reader.ReadSelectedAsync(Scan(null)))
            {
                using (selected)
                {
                    var batch = selected.Batch;
                    sum += batch.Length;
                    for (int c = 0; c < 3; c++)
                    {
                        var strings = (StringArray)batch.Column(c);
                        for (int r = 0; r < strings.Length; r++)
                            sum += strings.GetString(r).GetHashCode() & 0xFFFF;
                    }
                }
            }
            return sum;
        }
    }

    private static ParquetScan Scan(Predicate? prune) => new()
    {
        Columns = ["v", "w", "x"],
        Prune = prune,
        RowFilter = new RowFilter(ParallelRowGroupReadTests.KeyPredicate(k => k % GroupRows < 3_000, valueWise: false)),
    };

    private static async Task<List<ParquetFileReader.RowGroupPlan>> PlanAsync(ParquetFileReader reader, Predicate? prune) =>
        await PlanAsync(reader, await reader.PrepareSelectedAsync(Scan(prune), default), prune);

    private static async Task<List<ParquetFileReader.RowGroupPlan>> PlanAsync(
        ParquetFileReader reader, ParquetFileReader.SelectedPlanReader selected, Predicate? prune)
    {
        var plans = new List<ParquetFileReader.RowGroupPlan>();
        await foreach (var plan in reader.PlanRowGroupsAsync(
                           prune, selected.ReadColumns, null, default, stringIndexColumns: selected.StringIndexColumns))
        {
            plans.Add(plan);
        }
        return plans;
    }

    // The OffsetIndex offsets of the named columns in the given groups, in order.
    private static IEnumerable<long> IndexOffsets(
        EngineeredWood.Parquet.Metadata.FileMetaData metadata, IEnumerable<int> groups, params string[] columns)
    {
        var offsets = new List<long>();
        foreach (int g in groups)
        {
            foreach (var chunk in metadata.RowGroups[g].Columns)
            {
                if (columns.Contains(chunk.MetaData!.PathInSchema![0]))
                    offsets.Add(chunk.OffsetIndexOffset!.Value);
            }
        }
        offsets.Sort();
        return offsets;
    }

    // k: the row's position in the file. v: dictionary strings. w: PLAIN strings. x: DELTA_BYTE_ARRAY strings.
    private async Task<string> WriteAsync()
    {
        string path = Path.Combine(_tempDir, "groups.parquet");
        var random = new Random(894);
        string Random(int length)
        {
            var chars = new char[length];
            for (int c = 0; c < length; c++)
                chars[c] = (char)('a' + random.Next(26));
            return new string(chars);
        }

        var pool = Enumerable.Range(0, 200).Select(_ => Random(20)).ToArray();
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("k", Int64Type.Default, false))
            .Field(new Field("v", StringType.Default, false))
            .Field(new Field("w", StringType.Default, false))
            .Field(new Field("x", StringType.Default, false))
            .Build();
        await using var output = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(output, ownsFile: false, new ParquetWriteOptions
        {
            DataPageRowCountLimit = 2_000,
            ColumnEncodings = new Dictionary<string, ByteArrayEncoding>
            {
                ["w"] = ByteArrayEncoding.Plain,
                ["x"] = ByteArrayEncoding.DeltaByteArray,
            },
            ColumnDictionaryEnabled = new Dictionary<string, bool> { ["w"] = false, ["x"] = false },
        });
        for (int g = 0; g < Groups; g++)
        {
            var k = new Int64Array.Builder();
            var v = new StringArray.Builder();
            var w = new StringArray.Builder();
            var x = new StringArray.Builder();
            for (int i = 0; i < GroupRows; i++)
            {
                k.Append((long)g * GroupRows + i);
                v.Append(pool[random.Next(pool.Length)]);
                w.Append(Random(10 + random.Next(20)));
                x.Append(Random(10 + random.Next(20)));
            }
            using var batch = new RecordBatch(schema, [k.Build(), v.Build(), w.Build(), x.Build()], GroupRows);
            await writer.WriteRowGroupAsync(batch);
        }
        await writer.CloseAsync();
        return path;
    }

    // Rewrites path's footer to place one chunk's ColumnIndex past the end of the file.
    private static void MoveColumnIndexOutside(string path, int group, string column)
    {
        byte[] content = File.ReadAllBytes(path);
        int footerLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(content.AsSpan(content.Length - 8));
        int footerStart = content.Length - 8 - footerLength;
        var footer = EngineeredWood.Parquet.Metadata.MetadataDecoder.DecodeFileMetaData(content.AsSpan(footerStart, footerLength));

        var groups = footer.RowGroups.Select((g, gi) => gi != group ? g : new EngineeredWood.Parquet.Metadata.RowGroup
        {
            TotalByteSize = g.TotalByteSize,
            NumRows = g.NumRows,
            SortingColumns = g.SortingColumns,
            TotalCompressedSize = g.TotalCompressedSize,
            Ordinal = g.Ordinal,
            Columns = g.Columns.Select(c => c.MetaData!.PathInSchema![0] != column ? c : new EngineeredWood.Parquet.Metadata.ColumnChunk
            {
                FilePath = c.FilePath,
                FileOffset = c.FileOffset,
                MetaData = c.MetaData,
                OffsetIndexOffset = c.OffsetIndexOffset,
                OffsetIndexLength = c.OffsetIndexLength,
                ColumnIndexOffset = content.Length + 1_000,
                ColumnIndexLength = c.ColumnIndexLength,
            }).ToArray(),
        }).ToArray();
        var patched = new EngineeredWood.Parquet.Metadata.FileMetaData
        {
            Version = footer.Version,
            Schema = footer.Schema,
            NumRows = footer.NumRows,
            RowGroups = groups,
            KeyValueMetadata = footer.KeyValueMetadata,
            CreatedBy = footer.CreatedBy,
            ColumnOrders = footer.ColumnOrders,
        };

        byte[] encoded = EngineeredWood.Parquet.Metadata.MetadataEncoder.EncodeFileMetaData(patched);
        using var output = File.Create(path);
        output.Write(content, 0, footerStart);
        output.Write(encoded, 0, encoded.Length);
        var tail = new byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(tail, encoded.Length);
        tail[4] = (byte)'P';
        tail[5] = (byte)'A';
        tail[6] = (byte)'R';
        tail[7] = (byte)'1';
        output.Write(tail, 0, 8);
    }

    /// <summary>Holds any request that touches one of <see cref="Hung"/>'s offsets until it is cancelled.</summary>
    private sealed class HangingIndexReads(IRandomAccessFile inner) : IRandomAccessFile
    {
        public HashSet<long> Hung { get; } = new();

        public TaskCompletionSource<bool> Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int InFlight => Volatile.Read(ref _inFlight);

        public bool Cancelled { get; private set; }

        private int _inFlight;

        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) => inner.GetLengthAsync(cancellationToken);

        public async ValueTask<System.Buffers.IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default)
        {
            if (Touches(range))
                await HangAsync(cancellationToken);
            return await inner.ReadAsync(range, cancellationToken);
        }

        public async ValueTask<IReadOnlyList<System.Buffers.IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
        {
            if (ranges.Any(Touches))
                await HangAsync(cancellationToken);
            return await inner.ReadRangesAsync(ranges, cancellationToken);
        }

        private async Task HangAsync(CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _inFlight);
            Started.TrySetResult(true);
            try
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                Cancelled = true;
                throw;
            }
            finally
            {
                Interlocked.Decrement(ref _inFlight);
            }
        }

        private bool Touches(FileRange r) => Hung.Any(o => o >= r.Offset && o < r.Offset + r.Length);

        public ValueTask DisposeAsync() => default; // the local file is disposed by its owner

        public void Dispose()
        {
        }
    }

    /// <summary>Refuses any request that touches one of <see cref="Refused"/>'s offsets.</summary>
    private sealed class FailingIndexReads(IRandomAccessFile inner) : IRandomAccessFile
    {
        public HashSet<long> Refused { get; } = new();

        public int Refusals;

        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) => inner.GetLengthAsync(cancellationToken);

        public ValueTask<System.Buffers.IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default) =>
            Touches(range) ? Refuse<System.Buffers.IMemoryOwner<byte>>() : inner.ReadAsync(range, cancellationToken);

        public ValueTask<IReadOnlyList<System.Buffers.IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default) =>
            ranges.Any(Touches) ? Refuse<IReadOnlyList<System.Buffers.IMemoryOwner<byte>>>() : inner.ReadRangesAsync(ranges, cancellationToken);

        private bool Touches(FileRange r) => Refused.Any(o => o >= r.Offset && o < r.Offset + r.Length);

        private ValueTask<T> Refuse<T>()
        {
            Interlocked.Increment(ref Refusals);
            throw new IOException("refused");
        }

        public ValueTask DisposeAsync() => default; // the local file is disposed by its owner

        public void Dispose()
        {
        }
    }
}

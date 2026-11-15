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

    private static async Task<List<ParquetFileReader.RowGroupPlan>> PlanAsync(ParquetFileReader reader, Predicate? prune)
    {
        var selected = await reader.PrepareSelectedAsync(Scan(prune), default);
        var plans = new List<ParquetFileReader.RowGroupPlan>();
        await foreach (var plan in reader.PlanRowGroupsAsync(
                           prune, selected.ReadColumns, null, default, stringIndexesAhead: selected.MayReadInTwoPasses))
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

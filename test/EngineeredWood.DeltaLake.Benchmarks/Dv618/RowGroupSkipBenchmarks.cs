// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Benchmarks.Dv618;

/// <summary>What the benchmark table's deletion vector deletes.</summary>
public enum GroupDv
{
    /// <summary>No deletion vector at all.</summary>
    None,
    /// <summary>Every other row group (0, 2, 4, 6, 8), in full: half the file, skippable.</summary>
    HalfGroups,
    /// <summary>The same rows less the last of each group: as many rows deleted, nothing skippable.</summary>
    HalfGroupsLessOneRow,
}

/// <summary>
/// #618 end to end: <see cref="DeltaTable.ReadAsync"/> over a 1M-row, one-file table of ten row groups, with a
/// deletion vector that deletes half the groups in full, one that deletes the same rows less one per group (so no
/// group can be skipped), and none. Uses only API that predates #618, so the same source runs against the build
/// before it and the build after.
///
/// <para>The tables are built ONCE, under <c>%TEMP%\ew-dv618-ab</c>, by whichever build runs first, and reused
/// after that, so both builds read identical bytes. Setup prints the rows and data-file bytes one scan reads.
/// Run with <c>--filter *RowGroupSkipBenchmarks*</c>.</para>
/// </summary>
[MemoryDiagnoser]
public class RowGroupSkipBenchmarks
{
    private const int Rows = 1_000_000;
    private const int GroupRows = 100_000;
    private const long FilterHigh = 500_000;

    [ParamsAllValues]
    public GroupDv Dv { get; set; }

    [Params(false, true)]
    public bool Filtered { get; set; }

    private DeltaTable _table = null!;
    private DeltaReadOptions _options = null!;

    private static readonly Apache.Arrow.Schema Schema = new Apache.Arrow.Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("amount").DataType(DoubleType.Default).Nullable(false))
        .Field(f => f.Name("category").DataType(StringType.Default).Nullable(false))
        .Build();

    private static bool Deleted(GroupDv dv, long id)
    {
        if (dv == GroupDv.None || (id / GroupRows) % 2 != 0)
            return false;
        return dv == GroupDv.HalfGroups || id % GroupRows != GroupRows - 1;
    }

    [GlobalSetup]
    public void Setup()
    {
        string root = Path.Combine(Path.GetTempPath(), "ew-dv618-ab", Dv.ToString());
        if (!Directory.Exists(Path.Combine(root, "_delta_log")))
            Build(root, Dv).GetAwaiter().GetResult();

        // Statistics keep groups 0..4 for the filter; the vector removes 0, 2 and 4 of them.
        _options = new DeltaReadOptions
        {
            Filter = Filtered ? Ex.LessThan("id", LiteralValue.Of(FilterHigh)) : null,
        };

        long expected = 0;
        for (long id = 0; id < Rows; id++)
            if (!Deleted(Dv, id) && (!Filtered || id < FilterHigh))
                expected++;

        // One counted scan, for the bytes; then the table the benchmark reads, uncounted.
        var counting = new CountingFileSystem(new LocalTableFileSystem(root));
        long actual;
        using (var counted = DeltaTable.OpenAsync(counting).AsTask().GetAwaiter().GetResult())
            actual = ScanAsync(counted).GetAwaiter().GetResult();
        if (actual != expected)
            throw new InvalidOperationException($"{Dv}/{Filtered}: read {actual} rows, expected {expected}");
        Console.WriteLine($"// {Dv}/{(Filtered ? "filtered" : "full")}: {actual} rows, {counting.DataBytesRead:N0} data bytes read");

        _table = DeltaTable.OpenAsync(new LocalTableFileSystem(root)).AsTask().GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void Cleanup() => _table.DisposeAsync().AsTask().GetAwaiter().GetResult();

    private static async Task Build(string root, GroupDv dv)
    {
        Directory.CreateDirectory(root);
        var ids = new Int64Array.Builder().Reserve(Rows);
        var amounts = new DoubleArray.Builder().Reserve(Rows);
        var categories = new StringArray.Builder();
        for (int i = 0; i < Rows; i++)
        {
            ids.Append(i);
            amounts.Append(i * 0.5);
            categories.Append("cat_" + (i % 16).ToString("00"));
        }
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = GroupRows },
        };
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(root), Schema, options, enableDeletionVectors: true);
        await table.WriteAsync([new RecordBatch(Schema, [ids.Build(), amounts.Build(), categories.Build()], Rows)]);
        if (table.CurrentSnapshot.ActiveFiles.Count != 1)
            throw new InvalidOperationException("expected one data file");
        if (dv == GroupDv.None)
            return;
        await table.DeleteAsync(batch =>
        {
            var col = (Int64Array)batch.Column("id");
            var mask = new BooleanArray.Builder().Reserve(col.Length);
            for (int i = 0; i < col.Length; i++)
                mask.Append(Deleted(dv, col.GetValue(i)!.Value));
            return mask.Build();
        }, RowDeleteMode.DeletionVector);
    }

    [Benchmark]
    public Task<long> Scan() => ScanAsync(_table);

    private async Task<long> ScanAsync(DeltaTable table)
    {
        long rows = 0;
        await foreach (var batch in table.ReadAsync(_options))
            rows += batch.Length;
        return rows;
    }

    /// <summary>Counts the bytes read from data files (Parquet outside the log).</summary>
    private sealed class CountingFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        private long _bytes;

        public long DataBytesRead => Interlocked.Read(ref _bytes);

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public async ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
        {
            var file = await inner.OpenReadAsync(path, cancellationToken).ConfigureAwait(false);
            return path.EndsWith(".parquet", StringComparison.Ordinal)
                && path.IndexOf("_delta_log", StringComparison.Ordinal) < 0
                ? new CountingFile(file, this) : file;
        }

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default)
            => inner.ListAsync(prefix, cancellationToken);
        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
            => inner.ReadAllBytesAsync(path, cancellationToken);
        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default)
            => inner.CreateAsync(path, overwrite, cancellationToken);
        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
            => inner.DeleteAsync(path, cancellationToken);
        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
            => inner.ExistsAsync(path, cancellationToken);
        public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.TryWriteAllBytesAsync(path, data, cancellationToken);
        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.WriteAllBytesAsync(path, data, cancellationToken);

        private sealed class CountingFile(IRandomAccessFile inner, CountingFileSystem owner) : IRandomAccessFile
        {
            public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
                => inner.GetLengthAsync(cancellationToken);

            public ValueTask<System.Buffers.IMemoryOwner<byte>> ReadAsync(
                FileRange range, CancellationToken cancellationToken = default)
            {
                Interlocked.Add(ref owner._bytes, range.Length);
                return inner.ReadAsync(range, cancellationToken);
            }

            public ValueTask<IReadOnlyList<System.Buffers.IMemoryOwner<byte>>> ReadRangesAsync(
                IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
            {
                foreach (var range in ranges)
                    Interlocked.Add(ref owner._bytes, range.Length);
                return inner.ReadRangesAsync(ranges, cancellationToken);
            }

            public void Dispose() => inner.Dispose();
            public ValueTask DisposeAsync() => inner.DisposeAsync();
        }
    }
}

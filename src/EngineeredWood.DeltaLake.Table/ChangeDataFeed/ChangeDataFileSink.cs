// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.IO;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.ChangeDataFeed;

/// <summary>
/// Collects one operation's change rows, of every change type, into as few change files as
/// <see cref="DeltaTableOptions.TargetFileSize"/> allows: one per partition, rolled as data files roll (#723).
/// Before this, every rewritten data file wrote a change file per change type, so an UPDATE of ten files left
/// twenty change files where Spark leaves one.
///
/// <para>MEASURED against pyspark 4.0.4 / delta-spark 4.0.0: an UPDATE, DELETE or MERGE writes one change file per
/// write task, with every change type of that task in it (<c>update_preimage</c>, <c>update_postimage</c> and
/// <c>delete</c> rows side by side), never one per change type. How many tasks there are is Spark's scheduling: an
/// UPDATE of ten small files read by one task wrote one change file, and the same UPDATE with one task per file
/// wrote one per file. This writer has no tasks, so it writes what one task would. Readers tell the rows apart by
/// <c>_change_type</c>, and every reader (Spark's <c>table_changes</c>, delta-rs, engineered-wood's own feed) reads
/// a file of mixed types.</para>
///
/// <para>Rows are held only until a row group fills. Across partitions the held bytes (counting the whole batch a
/// held slice keeps alive) are capped at one row group's worth (<see cref="ParquetWriteOptions.RowGroupMaxBytes"/>):
/// past it, the partition holding the most is written out as a row group, so an UPDATE moving rows across many
/// partitions cannot buffer a row group for each. One partition alone may hold more, as a lone data writer does: the
/// batch it keeps alive is one the caller held anyway.</para>
///
/// <para>At most <see cref="DefaultMaxOpenFiles"/> files are open at once: each holds a stream, and a cloud stream
/// holds an upload buffer (4 MiB on Azure). Opening one more closes the least recently used, and that partition's
/// next rows start a new file, so an operation interleaving rows of many partitions writes more files rather than
/// holding a stream per partition.</para>
///
/// <para>Disposing without <see cref="CompleteAsync"/> abandons the open files, unpublished. Every file is recorded
/// in <c>written</c> before its first byte, so a failed operation's cleanup finds it.</para>
/// </summary>
internal sealed class ChangeDataFileSink : IAsyncDisposable
{
    private readonly ITableFileSystem _fs;
    private readonly EngineeredWood.DeltaLake.Snapshot.Snapshot _snapshot;
    private readonly ParquetWriteOptions _options;
    private readonly long _targetFileSize;
    private readonly WrittenFileLedger? _written;
    private readonly List<Partition> _partitions = new();
    private readonly Dictionary<string, Partition> _byKey = new(StringComparer.Ordinal);
    private readonly int _maxOpenFiles;

    // Kept up to date after every call into a writer, so no call scans every partition the operation has reached.
    private readonly HashSet<Partition> _holding = new();
    private readonly HashSet<Partition> _open = new();
    private long _held;
    private long _clock;

    /// <summary>How many change files may be open at once.</summary>
    internal const int DefaultMaxOpenFiles = 16;

    private sealed class Partition(Dictionary<string, string> values, RollingDataFileWriter writer)
    {
        public Dictionary<string, string> Values { get; } = values;
        public RollingDataFileWriter Writer { get; } = writer;
        public long Retained { get; set; }
        public long LastUsed { get; set; }
        public bool HoldsInserts { get; set; }
    }

    private readonly List<CdcFile> _filesHoldingInserts = new();

    /// <summary>Whether any change rows have been added.</summary>
    public bool HasRows { get; private set; }

    /// <summary>
    /// After <see cref="CompleteAsync"/>, the files of every partition that took <c>insert</c> rows: the files whose
    /// row ids a rebase may have to rewrite (see <c>DeltaTable.InsertChangeDataFillIn</c>). A partition's file closed
    /// before its first insert is listed too, which costs that rewrite a read and nothing else.
    /// </summary>
    public IReadOnlyList<CdcFile> FilesHoldingInserts => _filesHoldingInserts;

    public ChangeDataFileSink(
        ITableFileSystem fs,
        EngineeredWood.DeltaLake.Snapshot.Snapshot snapshot,
        ParquetWriteOptions options,
        long targetFileSize,
        WrittenFileLedger? written,
        int maxOpenFiles = DefaultMaxOpenFiles)
    {
        _maxOpenFiles = Math.Max(1, maxOpenFiles);
        _fs = fs;
        _snapshot = snapshot;
        _options = options;
        _targetFileSize = targetFileSize;
        _written = written;
    }

    /// <summary>
    /// Adds change rows (in the table's LOGICAL shape, as <see cref="CdfWriter.WriteAsync"/> takes them) under the
    /// partition <paramref name="partitionValues"/> names. Rows of one partition share files in the order added.
    /// <para>With a <paramref name="lease"/> over the rows and their ids and versions, the caller hands them over: the
    /// partition's writer lets go of it once they are written (#860). Without one they stay the caller's.</para>
    /// </summary>
    public async ValueTask AddAsync(
        RecordBatch rows,
        string changeType,
        IReadOnlyDictionary<string, string> partitionValues,
        CancellationToken cancellationToken,
        Int64Array? rowIds = null,
        Int64Array? rowCommitVersions = null,
        BatchLease? lease = null)
    {
        if (rows.Length == 0)
            return;

        var prepared = CdfWriter.PrepareRows(_snapshot, rows, changeType, rowIds, rowCommitVersions);
        var partition = PartitionOf(partitionValues);
        partition.LastUsed = ++_clock;
        HasRows = true;
        if (string.Equals(changeType, CdfConfig.Insert, StringComparison.Ordinal))
            partition.HoldsInserts = true;
        // No stats are collected for a change file, so the stats batch is never read.
        await partition.Writer.AddAsync(prepared, prepared, cancellationToken, lease).ConfigureAwait(false);
        Track(partition);

        // A non-positive limit (unvalidated) is taken as one byte. Every pass empties a holding partition, so the
        // loop ends.
        long limit = Math.Max(1, _options.RowGroupMaxBytes);
        while (_held > limit && _holding.Count > 1)
        {
            Partition? largest = null;
            foreach (var p in _holding)
            {
                if (largest is null || p.Retained > largest.Retained)
                    largest = p;
            }
            await largest!.Writer.FlushPendingAsync(cancellationToken).ConfigureAwait(false);
            Track(largest);
        }

        while (_open.Count > _maxOpenFiles)
        {
            Partition? oldest = null;
            foreach (var p in _open)
            {
                if (oldest is null || p.LastUsed < oldest.LastUsed)
                    oldest = p;
            }
            await oldest!.Writer.CloseFileAsync(cancellationToken).ConfigureAwait(false);
            Track(oldest);
        }
    }

    // Brings the totals up to date with what a call into the partition's writer changed.
    private void Track(Partition partition)
    {
        long retained = partition.Writer.RetainedBytes;
        _held += retained - partition.Retained;
        partition.Retained = retained;
        if (retained > 0)
            _holding.Add(partition);
        else
            _holding.Remove(partition);
        if (partition.Writer.HasOpenFile)
            _open.Add(partition);
        else
            _open.Remove(partition);
    }

    /// <summary>Writes what is held, closes every file, and returns their <c>cdc</c> actions: the partitions in the
    /// order first reached, each partition's files in order.</summary>
    public async ValueTask<IReadOnlyList<CdcFile>> CompleteAsync(CancellationToken cancellationToken)
    {
        var actions = new List<CdcFile>();
        foreach (var partition in _partitions)
        {
            foreach (var file in await partition.Writer.CompleteAsync(cancellationToken).ConfigureAwait(false))
            {
                var action = new CdcFile
                {
                    Path = EngineeredWood.DeltaLake.DeltaPath.Encode(file.FileName),
                    PartitionValues = new Dictionary<string, string>(partition.Values),
                    Size = file.Size,
                    DataChange = false,
                };
                actions.Add(action);
                if (partition.HoldsInserts)
                    _filesHoldingInserts.Add(action);
            }
        }
        return actions;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var partition in _partitions)
            await partition.Writer.DisposeAsync().ConfigureAwait(false);
    }

    private Partition PartitionOf(IReadOnlyDictionary<string, string> partitionValues)
    {
        string key = Key(partitionValues);
        if (_byKey.TryGetValue(key, out var partition))
            return partition;

        var values = new Dictionary<string, string>(partitionValues.Count, StringComparer.Ordinal);
        foreach (var kv in partitionValues)
            values[kv.Key] = kv.Value;
        partition = new Partition(values, new RollingDataFileWriter(
            _fs, host: null, _options, _targetFileSize, CdfConfig.ChangeDataDir, _written,
            collectStats: false, statsSelection: null, tableAllowsShredding: false));
        _byKey[key] = partition;
        _partitions.Add(partition);
        return partition;
    }

    // Length-prefixed and ordinal-sorted by column, with a null value marked apart from every string, so no two
    // different value sets share a key and the order a caller built its dictionary in does not matter.
    private static string Key(IReadOnlyDictionary<string, string> values)
    {
        var key = new System.Text.StringBuilder();
        foreach (var kv in values.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            key.Append(kv.Key.Length).Append(':').Append(kv.Key).Append('=');
            if (kv.Value is null)
                key.Append('-');
            else
                key.Append(kv.Value.Length).Append(':').Append(kv.Value);
            key.Append('|');
        }
        return key.ToString();
    }
}

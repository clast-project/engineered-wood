// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.IO;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// A write's rows gathered by partition: each partition's batches in input order, the partitions in the order the
/// input first reaches them. Keyed by the partition values exactly as the split produced them, which are what the
/// files' directory and <c>partitionValues</c> are made from, so every batch in a group can share a file.
/// </summary>
internal sealed class PartitionGroups : IEnumerable<PartitionGroups.Group>
{
    private readonly IReadOnlyList<string> _partitionColumns;
    private readonly List<Group> _groups = new();
    private readonly Dictionary<string, Group> _byKey = new(StringComparer.Ordinal);

    public PartitionGroups(IReadOnlyList<string> partitionColumns) => _partitionColumns = partitionColumns;

    internal sealed class Group(Dictionary<string, string> partitionValues)
    {
        public Dictionary<string, string> PartitionValues { get; } = partitionValues;
        public List<RecordBatch> Batches { get; } = new();
    }

    public void Add(Dictionary<string, string> partitionValues, RecordBatch batch)
    {
        string key = Key(partitionValues);
        if (!_byKey.TryGetValue(key, out var group))
        {
            group = new Group(partitionValues);
            _byKey[key] = group;
            _groups.Add(group);
        }
        group.Batches.Add(batch);
    }

    // Length-prefixed, with a missing key or a null value marked apart from every string, so no two different
    // value sets share a key.
    private string Key(Dictionary<string, string> values)
    {
        var key = new System.Text.StringBuilder();
        foreach (string column in _partitionColumns)
        {
            if (values.TryGetValue(column, out var value) && value is not null)
                key.Append(value.Length).Append(':').Append(value);
            else
                key.Append('-');
            key.Append('|');
        }
        return key.ToString();
    }

    public IEnumerator<Group> GetEnumerator() => _groups.GetEnumerator();

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
}

/// <summary>One data file a <see cref="RollingDataFileWriter"/> wrote: its path relative to the table root
/// (decoded, as written to storage), its byte size, its row count and its stats JSON (null when stats were not
/// asked for).</summary>
internal readonly record struct RolledDataFile(string FileName, long Size, long Rows, string? Stats);

/// <summary>
/// Writes one partition's rows of a write to as few data files as <see cref="DeltaTableOptions.TargetFileSize"/>
/// allows, however the caller happened to batch them (#559). Before this, every input batch became a file of its
/// own, with a single row group the size of that batch.
///
/// <para>With the built-in Parquet writer, consecutive batches are joined into one row group until
/// <see cref="ParquetWriteOptions.RowGroupMaxRows"/> rows or (estimated from the Arrow buffers)
/// <see cref="ParquetWriteOptions.RowGroupMaxBytes"/> is reached. After each row group the file's actual size is
/// checked, and at or past the target the file is closed and the next row group starts a new one. A file can
/// therefore overshoot the target by up to one row group, as delta-rs's and Spark's can.</para>
///
/// <para>A host <see cref="IDataFileWriter"/> is handed each file's batches as they came, and cuts its own row
/// groups. Its output size is known only once it returns, so its files are cut by the batches' Arrow size
/// instead, which is larger than the Parquet it becomes: a host's files come out smaller than the target.</para>
///
/// <para>A batch whose Arrow layout (column names, order, types, nullability) differs from the open file's starts a
/// new file: a Parquet file has one schema, and the Parquet writer refuses a row group that does not match it.</para>
///
/// <para>Disposing without <see cref="CompleteAsync"/> abandons the open file, unpublished. Every file is recorded
/// in <c>written</c> before its first byte, so a failed write's cleanup finds it.</para>
/// </summary>
internal sealed class RollingDataFileWriter : IAsyncDisposable
{
    private readonly ITableFileSystem _fs;
    private readonly IDataFileWriter? _host;
    private readonly ParquetWriteOptions _options;
    private readonly long _targetFileSize;
    private readonly string _partitionDirectory;
    private readonly WrittenFileLedger? _written;
    private readonly Stats.StatsColumnSelection? _statsSelection;
    private readonly bool _collectStats;
    private readonly List<RolledDataFile> _files = new();

    // The file being filled: its layout, rows and stats so far. Its Parquet writer is opened by the first row group.
    private Apache.Arrow.Schema? _fileSchema;
    private long _fileRows;
    private Stats.StatsCollector.Accumulator? _fileStats;
    private string? _fileName;
    private ISequentialFile? _file;
    private ParquetFileWriter? _writer;

    // Batches not yet written: the row group being built, or under a host writer the whole file.
    private readonly List<RecordBatch> _pending = new();
    private long _pendingRows;
    private long _pendingBytes;

    /// <param name="partitionDirectory">The file's Hive-style directory relative to the table root, or empty.</param>
    /// <param name="collectStats">Whether to collect per-file stats, over the batches <see cref="AddAsync"/> is
    /// given for them, under <paramref name="statsSelection"/>.</param>
    public RollingDataFileWriter(
        ITableFileSystem fs, IDataFileWriter? host, ParquetWriteOptions options, long targetFileSize,
        string partitionDirectory, WrittenFileLedger? written, bool collectStats,
        Stats.StatsColumnSelection? statsSelection)
    {
        _fs = fs;
        _host = host;
        _options = options;
        _targetFileSize = Math.Max(1, targetFileSize);
        _partitionDirectory = partitionDirectory;
        _written = written;
        _collectStats = collectStats;
        _statsSelection = statsSelection;
    }

    /// <summary>
    /// Adds a batch to the file being filled, writing what is due.
    /// </summary>
    /// <param name="writeBatch">The batch as it goes into the file: physical names, field ids, any partition
    /// columns IcebergCompat materializes.</param>
    /// <param name="statsBatch">The same rows as the file's stats describe them (physical names, no materialized
    /// partition or row-tracking columns). Ignored unless stats are collected.</param>
    public async ValueTask AddAsync(RecordBatch writeBatch, RecordBatch statsBatch, CancellationToken cancellationToken)
    {
        if (writeBatch.Length == 0)
            return;

        if (_fileSchema is not null && !SameLayout(_fileSchema.FieldsList, writeBatch.Schema.FieldsList))
            await CloseFileAsync(cancellationToken).ConfigureAwait(false);

        long batchBytes = EstimatedBytes(writeBatch);
        if (_host is not null)
        {
            Append(writeBatch, statsBatch, batchBytes);
            if (_pendingBytes >= _targetFileSize)
                await CloseFileAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        // The batch is cut where a row group fills, so every row group but a file's last holds exactly
        // RowGroupMaxRows rows, and the target is checked after each one. Handing the Parquet writer more rows than
        // that would have it split them into several row groups itself, unseen here: a file's overshoot would then
        // depend on how the caller batched its rows. Each piece is charged its share of the batch's bytes.
        long maxRows = Math.Max(1, _options.RowGroupMaxRows);
        int offset = 0;
        while (offset < writeBatch.Length)
        {
            int take = (int)Math.Min(writeBatch.Length - offset, maxRows - _pendingRows);
            bool whole = take == writeBatch.Length;
            Append(
                whole ? writeBatch : writeBatch.Slice(offset, take),
                whole ? statsBatch : statsBatch.Slice(offset, take),
                whole ? batchBytes : batchBytes * take / writeBatch.Length);
            offset += take;

            if (_pendingRows >= maxRows || _pendingBytes >= _options.RowGroupMaxBytes)
            {
                await FlushRowGroupAsync(cancellationToken).ConfigureAwait(false);
                if (_file!.Position >= _targetFileSize)
                    await CloseFileAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }

    // Adds rows to the file being filled. They are written to it: the file closes only once they are flushed.
    private void Append(RecordBatch writeBatch, RecordBatch statsBatch, long bytes)
    {
        _fileSchema ??= writeBatch.Schema;
        _fileRows += writeBatch.Length;
        if (_collectStats)
            (_fileStats ??= new Stats.StatsCollector.Accumulator(_statsSelection)).Add(statsBatch);

        _pending.Add(writeBatch);
        _pendingRows += writeBatch.Length;
        _pendingBytes += bytes;
    }

    /// <summary>Writes what is pending and closes the last file. Returns every file written, in order.</summary>
    public async ValueTask<IReadOnlyList<RolledDataFile>> CompleteAsync(CancellationToken cancellationToken)
    {
        await CloseFileAsync(cancellationToken).ConfigureAwait(false);
        return _files;
    }

    private string NewFileName()
    {
        string fileName = string.IsNullOrEmpty(_partitionDirectory)
            ? $"{Guid.NewGuid():N}.parquet"
            : $"{_partitionDirectory}/{Guid.NewGuid():N}.parquet";
        // Recorded BEFORE the write: a write that fails part-way leaves bytes at this path, and they are as
        // orphaned as a complete file's.
        _written?.Record(fileName);
        return fileName;
    }

    private async ValueTask FlushRowGroupAsync(CancellationToken cancellationToken)
    {
        if (_pending.Count == 0)
            return;

        if (_writer is null)
        {
            _fileName = NewFileName();
            _file = await _fs.CreateAsync(_fileName, cancellationToken: cancellationToken).ConfigureAwait(false);
            _writer = new ParquetFileWriter(_file, ownsFile: false, _options);
        }

        var rowGroup = Concatenate(_pending);
        ClearPending();
        await _writer.WriteRowGroupAsync(rowGroup, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask CloseFileAsync(CancellationToken cancellationToken)
    {
        if (_fileRows == 0)
            return;

        string fileName;
        long size;
        if (_host is not null)
        {
            fileName = NewFileName();
            var batches = _pending.ToArray();
            ClearPending();
            size = await _host.WriteAsync(batches.ToAsyncEnumerable(), fileName, cancellationToken)
                .ConfigureAwait(false);
        }
        else
        {
            await FlushRowGroupAsync(cancellationToken).ConfigureAwait(false);
            fileName = _fileName!;
            // CloseAsync writes the footer and publishes the file before Position is read.
            await _writer!.CloseAsync(cancellationToken).ConfigureAwait(false);
            size = _file!.Position;
            await DisposeOpenFileAsync().ConfigureAwait(false);
        }

        _files.Add(new RolledDataFile(fileName, size, _fileRows, _fileStats?.Serialize()));
        _fileSchema = null;
        _fileRows = 0;
        _fileStats = null;
    }

    private void ClearPending()
    {
        _pending.Clear();
        _pendingRows = 0;
        _pendingBytes = 0;
    }

    private async ValueTask DisposeOpenFileAsync()
    {
        var writer = _writer;
        var file = _file;
        _writer = null;
        _file = null;
        _fileName = null;
        if (writer is not null)
            await writer.DisposeAsync().ConfigureAwait(false);
        if (file is not null)
            await file.DisposeAsync().ConfigureAwait(false);
    }

    public ValueTask DisposeAsync() => DisposeOpenFileAsync();

    private static RecordBatch Concatenate(List<RecordBatch> batches)
    {
        if (batches.Count == 1)
            return batches[0];

        var first = batches[0];
        var columns = new IArrowArray[first.ColumnCount];
        var parts = new IArrowArray[batches.Count];
        int rows = 0;
        foreach (var batch in batches)
            rows += batch.Length;
        for (int c = 0; c < columns.Length; c++)
        {
            for (int b = 0; b < batches.Count; b++)
                parts[b] = batches[b].Column(c);
            columns[c] = ArrowCompute.Concatenate(parts);
        }
        return new RecordBatch(first.Schema, columns, rows);
    }

    /// <summary>The bytes of a batch's Arrow buffers. A sliced batch is counted whole, so this can only
    /// overestimate.</summary>
    internal static long EstimatedBytes(RecordBatch batch)
    {
        long bytes = 0;
        for (int c = 0; c < batch.ColumnCount; c++)
            bytes += EstimatedBytes(batch.Column(c).Data);
        return bytes;
    }

    private static long EstimatedBytes(ArrayData data)
    {
        long bytes = 0;
        foreach (var buffer in data.Buffers)
            bytes += buffer.Length;
        if (data.Children is { } children)
        {
            foreach (var child in children)
                bytes += EstimatedBytes(child);
        }
        if (data.Dictionary is { } dictionary)
            bytes += EstimatedBytes(dictionary);
        return bytes;
    }

    /// <summary>
    /// Whether batches of these two layouts can share a Parquet file: the same column names in the same order, of
    /// the same nullability and types at every depth. Field metadata may differ. Stricter than the Parquet writer's
    /// own check, which is safe: a difference only starts a new file.
    /// </summary>
    internal static bool SameLayout(IReadOnlyList<Field> a, IReadOnlyList<Field> b)
    {
        if (a.Count != b.Count)
            return false;
        for (int i = 0; i < a.Count; i++)
        {
            if (!string.Equals(a[i].Name, b[i].Name, StringComparison.Ordinal)
                || a[i].IsNullable != b[i].IsNullable
                || !SameType(a[i].DataType, b[i].DataType))
            {
                return false;
            }
        }
        return true;
    }

    private static bool SameType(IArrowType a, IArrowType b)
    {
        if (a.TypeId != b.TypeId)
            return false;

        return (a, b) switch
        {
            (ExtensionType x, ExtensionType y) => string.Equals(x.Name, y.Name, StringComparison.Ordinal)
                && string.Equals(x.ExtensionMetadata, y.ExtensionMetadata, StringComparison.Ordinal)
                && SameType(x.StorageType, y.StorageType),
            (DictionaryType x, DictionaryType y) => SameType(x.IndexType, y.IndexType)
                && SameType(x.ValueType, y.ValueType) && x.Ordered == y.Ordered,
            (FixedSizeListType x, FixedSizeListType y) => x.ListSize == y.ListSize && SameLayout(x.Fields, y.Fields),
            (MapType x, MapType y) => x.KeySorted == y.KeySorted && SameLayout(x.Fields, y.Fields),
            (NestedType x, NestedType y) => SameLayout(x.Fields, y.Fields),
            (Decimal32Type x, Decimal32Type y) => x.Precision == y.Precision && x.Scale == y.Scale,
            (Decimal64Type x, Decimal64Type y) => x.Precision == y.Precision && x.Scale == y.Scale,
            (Decimal128Type x, Decimal128Type y) => x.Precision == y.Precision && x.Scale == y.Scale,
            (Decimal256Type x, Decimal256Type y) => x.Precision == y.Precision && x.Scale == y.Scale,
            (TimestampType x, TimestampType y) => x.Unit == y.Unit
                && string.Equals(x.Timezone, y.Timezone, StringComparison.Ordinal),
            (Time32Type x, Time32Type y) => x.Unit == y.Unit,
            (Time64Type x, Time64Type y) => x.Unit == y.Unit,
            (DurationType x, DurationType y) => x.Unit == y.Unit,
            (IntervalType x, IntervalType y) => x.Unit == y.Unit,
            (FixedSizeBinaryType x, FixedSizeBinaryType y) => x.ByteWidth == y.ByteWidth,
            _ => true,
        };
    }
}

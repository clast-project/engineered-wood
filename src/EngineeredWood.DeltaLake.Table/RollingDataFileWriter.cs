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
/// <para>With the built-in Parquet writer, each batch is handed to <see cref="ParquetFileWriter.WriteBatchAsync"/> as it
/// comes, which encodes it into the open row group's pages and cuts row groups itself, at
/// <see cref="ParquetWriteOptions.RowGroupMaxRows"/> rows or <see cref="ParquetWriteOptions.RowGroupMaxBytes"/> of
/// estimated encoded bytes (#936). Nothing of a batch is held once it is written, only the open row group's
/// compressed pages. After each row group the file's actual size is checked, and at or past the target the file is
/// closed and the next row group starts a new one. A file can therefore overshoot the target by up to one row group,
/// as delta-rs's and Spark's can.</para>
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
    private readonly bool _tableAllowsShredding;
    private readonly IReadOnlyDictionary<string, int>? _columnFieldIds;
    private readonly List<RolledDataFile> _files = new();

    // The last batch schema given ids by WithColumnFieldIds, and the schema it became: a writer's batches share one.
    private Apache.Arrow.Schema? _idsFrom;
    private Apache.Arrow.Schema? _idsTo;

    // The file being filled: its layout, rows and stats so far. Its Parquet writer is opened by the first rows.
    private Apache.Arrow.Schema? _fileSchema;
    private long _fileRows;
    private Stats.StatsCollector.Accumulator? _fileStats;
    private string? _fileName;
    private ISequentialFile? _file;
    private ParquetFileWriter? _writer;

    // Under a host writer, the batches of the file being filled, which it is handed whole.
    private readonly List<RecordBatch> _pending = new();
    private long _pendingBytes;

    // Leases of the batches given under one (#860): under the built-in writer released once a batch is written, which
    // encodes it; under a host writer held until the file is.
    private readonly List<BatchLease> _leases = new();

    /// <param name="partitionDirectory">The file's Hive-style directory relative to the table root, or empty.</param>
    /// <param name="collectStats">Whether to collect per-file stats, over the batches <see cref="AddAsync"/> is
    /// given for them, under <paramref name="statsSelection"/>.</param>
    /// <param name="tableAllowsShredding">Whether the table supports variant shredding, which a host's file needs
    /// for variant bounds (EW's own shredded write is refused on a table that does not).</param>
    /// <param name="columnFieldIds">Parquet field ids for top-level columns the table schema does not hold, by
    /// physical name: an IcebergCompatV3 table's materialized row-tracking columns
    /// (<see cref="Schema.IcebergCompat.RowTrackingFieldIds"/>). A column already carrying an id keeps it.</param>
    public RollingDataFileWriter(
        ITableFileSystem fs, IDataFileWriter? host, ParquetWriteOptions options, long targetFileSize,
        string partitionDirectory, WrittenFileLedger? written, bool collectStats,
        Stats.StatsColumnSelection? statsSelection, bool tableAllowsShredding,
        IReadOnlyDictionary<string, int>? columnFieldIds = null)
    {
        _fs = fs;
        _host = host;
        _options = options;
        _targetFileSize = Math.Max(1, targetFileSize);
        _partitionDirectory = partitionDirectory;
        _written = written;
        _collectStats = collectStats;
        _statsSelection = statsSelection;
        _tableAllowsShredding = tableAllowsShredding;
        _columnFieldIds = columnFieldIds is { Count: > 0 } ? columnFieldIds : null;
    }

    /// <summary>
    /// Adds a batch to the file being filled, writing what is due.
    /// </summary>
    /// <param name="writeBatch">The batch as it goes into the file: physical names, field ids, any partition
    /// columns IcebergCompat materializes.</param>
    /// <param name="statsBatch">The same rows as the file's stats describe them (physical names, no materialized
    /// partition or row-tracking columns). Ignored unless stats are collected.</param>
    /// <param name="lease">The buffers of both batches, when the caller hands them over: the writer holds the lease
    /// until every row of the batch is written (or the write is abandoned), then lets go of it. Without one the batches
    /// stay the caller's, and the writer only reads them.</param>
    public async ValueTask AddAsync(
        RecordBatch writeBatch, RecordBatch statsBatch, CancellationToken cancellationToken, BatchLease? lease = null)
    {
        if (writeBatch.Length == 0)
            return;

        if (_columnFieldIds is not null)
            writeBatch = WithColumnFieldIds(writeBatch);

        if (_fileSchema is not null && !SameLayout(_fileSchema.FieldsList, writeBatch.Schema.FieldsList))
            await CloseFileAsync(cancellationToken).ConfigureAwait(false);

        // Held only once the previous file is closed: closing a host's file lets go of every lease it holds, and this
        // batch is the next file's (#939 review).
        if (lease is not null)
            _leases.Add(lease.Acquire());

        if (_host is not null)
        {
            long batchBytes = EstimatedBytes(writeBatch);
            Account(writeBatch, statsBatch);
            _pending.Add(writeBatch);
            _pendingBytes += batchBytes;
            if (_pendingBytes >= _targetFileSize)
                await CloseFileAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        // The Parquet writer cuts row groups, by rows and by encoded bytes, splitting the batch where one fills; it
        // stops at each, so the file's size is checked between row groups and a full file is closed there.
        int offset = 0;
        while (offset < writeBatch.Length)
        {
            var writer = await OpenWriterAsync(cancellationToken).ConfigureAwait(false);
            // The whole batch and where to go on from, not the rest as a slice: the writer prepares only the rows each
            // call takes, so a batch spanning many row groups is not prepared again for every one (#939 review).
            int rows = await writer.WriteBatchUntilRowGroupAsync(writeBatch, offset, cancellationToken).ConfigureAwait(false);
            // None when rows the writer held from earlier batches filled a row group first (#941).
            if (rows > 0)
                Account(
                    rows == writeBatch.Length ? writeBatch : writeBatch.Slice(offset, rows),
                    rows == writeBatch.Length ? statsBatch : statsBatch.Slice(offset, rows));
            offset += rows;

            // Only between row groups: the file holds its header from the first rows on.
            if (writer.OpenRowGroupRows == 0 && _file!.Position >= _targetFileSize)
                await CloseFileAsync(cancellationToken).ConfigureAwait(false);
        }

        // Every row is encoded, and the Parquet writer keeps nothing of the batch.
        ReleaseLeases();
    }

    private async ValueTask<ParquetFileWriter> OpenWriterAsync(CancellationToken cancellationToken)
    {
        if (_writer is null)
        {
            _fileName = NewFileName();
            _file = await _fs.CreateAsync(_fileName, cancellationToken: cancellationToken).ConfigureAwait(false);
            _writer = new ParquetFileWriter(_file, ownsFile: false, _options);
        }
        return _writer;
    }

    private RecordBatch WithColumnFieldIds(RecordBatch batch)
    {
        if (!ReferenceEquals(batch.Schema, _idsFrom))
        {
            _idsFrom = batch.Schema;
            _idsTo = WithFieldIds(batch.Schema, _columnFieldIds!);
        }
        return _idsTo is null ? batch : Rebuilt(batch, _idsTo);
    }

    /// <summary>
    /// <paramref name="batch"/> with each top-level column named in <paramref name="columnFieldIds"/> given that
    /// Parquet field id, unless it already carries one. A batch with no such column comes back as it is.
    /// </summary>
    internal static RecordBatch WithColumnFieldIds(RecordBatch batch, IReadOnlyDictionary<string, int> columnFieldIds) =>
        WithFieldIds(batch.Schema, columnFieldIds) is { } schema ? Rebuilt(batch, schema) : batch;

    private static RecordBatch Rebuilt(RecordBatch batch, Apache.Arrow.Schema schema) =>
        new(schema, Enumerable.Range(0, batch.ColumnCount).Select(batch.Column), batch.Length);

    private static bool NeedsFieldId(Field field, IReadOnlyDictionary<string, int> columnFieldIds) =>
        columnFieldIds.ContainsKey(field.Name) && Schema.ColumnMapping.GetParquetFieldId(field) is null;

    // Null when no column needs an id.
    private static Apache.Arrow.Schema? WithFieldIds(
        Apache.Arrow.Schema schema, IReadOnlyDictionary<string, int> columnFieldIds)
    {
        if (!schema.FieldsList.Any(field => NeedsFieldId(field, columnFieldIds)))
            return null;
        var builder = new Apache.Arrow.Schema.Builder();
        if (schema.Metadata is { } schemaMetadata)
            builder.Metadata(schemaMetadata);
        foreach (var field in schema.FieldsList)
        {
            if (!NeedsFieldId(field, columnFieldIds))
            {
                builder.Field(field);
                continue;
            }
            var metadata = field.Metadata is null
                ? new Dictionary<string, string>()
                : field.Metadata.ToDictionary(pair => pair.Key, pair => pair.Value);
            metadata[Schema.ColumnMapping.ParquetFieldIdKey] =
                columnFieldIds[field.Name].ToString(System.Globalization.CultureInfo.InvariantCulture);
            builder.Field(new Field(field.Name, field.DataType, field.IsNullable, metadata));
        }
        return builder.Build();
    }

    // Counts rows into the file being filled, and its stats.
    private void Account(RecordBatch writeBatch, RecordBatch statsBatch)
    {
        _fileSchema ??= writeBatch.Schema;
        _fileRows += writeBatch.Length;
        if (_collectStats)
            (_fileStats ??= new Stats.StatsCollector.Accumulator(_statsSelection)).Add(statsBatch);
    }

    /// <summary>What the writer holds for the file being filled: under the built-in writer the open row group's
    /// estimated encoded size (<see cref="ParquetFileWriter.OpenRowGroupBytes"/>), which
    /// <see cref="FlushPendingAsync"/> writes out; under a host writer the estimated Arrow bytes of the batches it
    /// will be handed.</summary>
    public long RetainedBytes => _host is not null ? _pendingBytes : _writer?.OpenRowGroupBytes ?? 0;

    /// <summary>Whether a file is open: rows have been written to it and it is not yet closed.</summary>
    public bool HasOpenFile => _writer is not null;


    /// <summary>
    /// Writes the open row group now, before it is full, and closes the file if that takes it to the target; for a
    /// caller that holds several writers and bounds the memory of all of them. A host writer's pending rows are its
    /// whole file, so under one this closes the file.
    /// </summary>
    public async ValueTask FlushPendingAsync(CancellationToken cancellationToken)
    {
        if (_host is not null)
        {
            if (_pending.Count > 0)
                await CloseFileAsync(cancellationToken).ConfigureAwait(false);
            return;
        }
        if (_writer is null || _writer.OpenRowGroupRows == 0)
            return;
        await _writer.FlushRowGroupAsync(cancellationToken).ConfigureAwait(false);
        if (_file!.Position >= _targetFileSize)
            await CloseFileAsync(cancellationToken).ConfigureAwait(false);
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

    private void ReleaseLeases()
    {
        foreach (var lease in _leases)
            lease.Release();
        _leases.Clear();
    }

    /// <summary>Writes what is pending and closes the file being filled, so the next rows start a new one; also for a
    /// caller that bounds how many files its writers hold open.</summary>
    public async ValueTask CloseFileAsync(CancellationToken cancellationToken)
    {
        if (_fileRows == 0)
            return;

        string fileName;
        long size;
        if (_host is not null)
        {
            fileName = NewFileName();
            var batches = _pending.ToArray();
            _pending.Clear();
            _pendingBytes = 0;
            size = await _host.WriteAsync(batches.ToAsyncEnumerable(), fileName, cancellationToken)
                .ConfigureAwait(false);
            ReleaseLeases();
            await AddVariantBoundsAsync(writtenFooter: null, fileName, size, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            fileName = _fileName!;
            // CloseAsync writes the open row group and the footer, and publishes the file before Position is read.
            await _writer!.CloseAsync(cancellationToken).ConfigureAwait(false);
            size = _file!.Position;
            await AddVariantBoundsAsync(_writer.WrittenMetadata, fileName, size, cancellationToken).ConfigureAwait(false);
            await DisposeOpenFileAsync().ConfigureAwait(false);
        }

        _files.Add(new RolledDataFile(fileName, size, _fileRows, _fileStats?.Serialize()));
        _fileSchema = null;
        _fileRows = 0;
        _fileStats = null;
    }

    /// <summary>
    /// Variant min/max for the file just closed, as delta-spark derives them: from the footer this writer wrote, or a
    /// host writer's read back (see <see cref="Stats.VariantFooterStats.OfWrittenFileAsync"/>).
    /// </summary>
    private async ValueTask AddVariantBoundsAsync(
        EngineeredWood.Parquet.Metadata.FileMetaData? writtenFooter, string fileName, long fileSize,
        CancellationToken cancellationToken)
    {
        if (_fileStats is null || _fileSchema is null)
            return;
        if (await Stats.VariantFooterStats.OfWrittenFileAsync(
                writtenFooter, _fs, fileName, fileSize, _fileSchema, _tableAllowsShredding, StringPrefixLength, cancellationToken)
                .ConfigureAwait(false) is { } bounds)
        {
            _fileStats.AddVariantBounds(bounds);
        }
    }

    private int StringPrefixLength => (_statsSelection ?? Stats.StatsColumnSelection.All).StringPrefixLength;

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

    /// <summary>Abandons the open file, unpublished, and lets go of every lease still held.</summary>
    public async ValueTask DisposeAsync()
    {
        try
        {
            await DisposeOpenFileAsync().ConfigureAwait(false);
        }
        finally
        {
            ReleaseLeases();
            _pending.Clear();
            _pendingBytes = 0;
        }
    }

    /// <summary>The bytes of a batch's Arrow buffers. A sliced batch is counted whole, so this can only
    /// overestimate.</summary>
    internal static long EstimatedBytes(RecordBatch batch) => EngineeredWood.Arrow.ArrowSizes.EstimatedBytes(batch);

    /// <summary>
    /// Whether batches of these two layouts can share a Parquet file: the same column names in the same order, of
    /// the same nullability and types at every depth. Field metadata may differ. Stricter than the Parquet writer's
    /// own check, which is safe: a difference only starts a new file.
    /// </summary>
    internal static bool SameLayout(IReadOnlyList<Field> a, IReadOnlyList<Field> b) =>
        ArrowTypeEquality.SameLayout(a, b);
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Expressions;
using EngineeredWood.Iceberg.Manifest;
using EngineeredWood.IO;

namespace EngineeredWood.Iceberg.Expressions;

/// <summary>
/// Plans a table scan by evaluating filter predicates against file-level
/// statistics to prune files that cannot contain matching rows.
/// </summary>
public sealed class TableScan
{
    private readonly TableMetadata _metadata;
    private readonly ITableFileSystem _fs;
    private Predicate _filter = EngineeredWood.Expressions.Expressions.True;
    private long? _snapshotId;

    /// <summary>
    /// Initializes a new scan against the specified table metadata and file system.
    /// </summary>
    public TableScan(TableMetadata metadata, ITableFileSystem fs)
    {
        _metadata = metadata;
        _fs = fs;
    }

    /// <summary>
    /// Add a filter predicate. Multiple filters are ANDed together.
    /// </summary>
    public TableScan Filter(Predicate filter)
    {
        _filter = _filter is TruePredicate
            ? filter
            : EngineeredWood.Expressions.Expressions.And(_filter, filter);
        return this;
    }

    /// <summary>
    /// Use a specific snapshot instead of the current one.
    /// </summary>
    public TableScan UseSnapshot(long snapshotId)
    {
        _snapshotId = snapshotId;
        return this;
    }

    /// <summary>
    /// Plan the scan: returns data files that might contain matching rows,
    /// plus any applicable delete files.
    /// </summary>
    public async ValueTask<ScanResult> PlanFilesAsync(CancellationToken ct = default)
    {
        var effectiveMetadata = _snapshotId is not null
            ? TimeTravel.AtSnapshot(_metadata, _snapshotId.Value)
            : _metadata;

        if (effectiveMetadata.CurrentSnapshotId is null)
            return new ScanResult([], [], 0, 0);

        var snapshot = effectiveMetadata.Snapshots.First(
            s => s.SnapshotId == effectiveMetadata.CurrentSnapshotId);

        if (!await _fs.ExistsAsync(snapshot.ManifestList, ct).ConfigureAwait(false))
            return new ScanResult([], [], 0, 0);

        var schema = effectiveMetadata.Schemas.First(
            s => s.SchemaId == effectiveMetadata.CurrentSchemaId);
        var (boundFilter, accessor) = BindFilter(_filter, schema);

        var manifestList = await ManifestIO.ReadManifestListAsync(_fs, snapshot.ManifestList, ct)
            .ConfigureAwait(false);

        var dataFiles = new List<DataFile>();
        var deleteFiles = new List<DataFile>();
        int totalFilesScanned = 0;
        int filesSkipped = 0;

        foreach (var mle in manifestList)
        {
            if (!await _fs.ExistsAsync(mle.ManifestPath, ct).ConfigureAwait(false))
                continue;

            var entries = await ManifestIO.ReadManifestAsync(_fs, mle.ManifestPath, ct)
                .ConfigureAwait(false);

            foreach (var entry in entries)
            {
                if (entry.Status == ManifestEntryStatus.Deleted)
                    continue;

                totalFilesScanned++;

                // Delete files are always included (they apply to data files)
                if (entry.DataFile.Content != FileContent.Data)
                {
                    deleteFiles.Add(entry.DataFile);
                    continue;
                }

                if (!ShouldInclude(boundFilter, accessor, entry.DataFile))
                {
                    filesSkipped++;
                    continue;
                }

                dataFiles.Add(entry.DataFile);
            }
        }

        return new ScanResult(dataFiles, deleteFiles, totalFilesScanned, filesSkipped);
    }

    /// <summary>
    /// Plan the scan against a pre-loaded list of data files (bypasses manifest I/O).
    /// </summary>
    public ScanResult PlanFiles(IReadOnlyList<DataFile> candidateFiles)
    {
        var schema = _metadata.Schemas.First(
            s => s.SchemaId == _metadata.CurrentSchemaId);
        var (boundFilter, accessor) = BindFilter(_filter, schema);

        var dataFiles = new List<DataFile>();
        var deleteFiles = new List<DataFile>();
        int totalScanned = 0;
        int skipped = 0;

        foreach (var df in candidateFiles)
        {
            if (df.Content != FileContent.Data)
            {
                deleteFiles.Add(df);
                continue;
            }

            totalScanned++;

            if (!ShouldInclude(boundFilter, accessor, df))
            {
                skipped++;
                continue;
            }

            dataFiles.Add(df);
        }

        return new ScanResult(dataFiles, deleteFiles, totalScanned, skipped);
    }

    /// <summary>
    /// Binds the filter against the schema. Iceberg requires bound references
    /// before evaluation; lookup is by field ID, so unresolved references are
    /// left unbound (treated as Unknown by the evaluator so they don't filter
    /// out files).
    /// </summary>
    private static (Predicate Filter, IcebergStatisticsAccessor Accessor) BindFilter(
        Predicate filter, Schema schema)
    {
        var nameToId = schema.Fields.ToDictionary(f => f.Name, f => f.Id);
        var binder = new ExpressionBinder(nameToId, allowUnresolved: true);
        var bound = filter is TruePredicate ? filter : binder.Bind(filter);
        return (bound, new IcebergStatisticsAccessor(nameToId));
    }

    /// <summary>
    /// Returns true if the file might contain matching rows. Skips files that
    /// the evaluator proves cannot match.
    /// </summary>
    private static bool ShouldInclude(
        Predicate filter, IcebergStatisticsAccessor accessor, DataFile df)
    {
        if (filter is TruePredicate)
            return true;

        var stats = new DataFileStats(df);
        var result = StatisticsEvaluator.Evaluate(filter, stats, accessor);
        return result != FilterResult.AlwaysFalse;
    }
}

/// <summary>
/// Result of a table scan plan, containing the data files that may match the filter
/// and statistics about how many files were pruned.
/// </summary>
public sealed record ScanResult(
    IReadOnlyList<DataFile> DataFiles,
    IReadOnlyList<DataFile> DeleteFiles,
    int TotalFilesScanned,
    int FilesSkipped)
{
    /// <summary>Number of data files that matched the filter.</summary>
    public int FilesMatched => DataFiles.Count;
}

/// <summary>
/// Wraps a <see cref="DataFile"/> for evaluation by the shared
/// <see cref="StatisticsEvaluator"/>. Resolves column names to field IDs
/// against the file's bound stats.
/// </summary>
internal sealed class DataFileStats
{
    private readonly DataFile _file;

    public DataFileStats(DataFile file)
    {
        _file = file;
    }

    public DataFile File => _file;

    public LiteralValue? GetMin(int fieldId) =>
        _file.ColumnLowerBounds is { } b && b.TryGetValue(fieldId, out var v)
            ? v : (LiteralValue?)null;

    public LiteralValue? GetMax(int fieldId) =>
        _file.ColumnUpperBounds is { } b && b.TryGetValue(fieldId, out var v)
            ? v : (LiteralValue?)null;

    public long? GetNullCount(int fieldId) =>
        _file.NullValueCounts is { } b && b.TryGetValue(fieldId, out long v)
            ? v : (long?)null;

    public long ValueCount => _file.RecordCount;
}

/// <summary>
/// Adapts <see cref="DataFileStats"/> for the shared
/// <see cref="StatisticsEvaluator"/>. The shared accessor contract is keyed
/// by column name; Iceberg stores stats by field ID, so this adapter holds
/// the schema's name→id map for translation.
/// </summary>
internal sealed class IcebergStatisticsAccessor : IStatisticsAccessor<DataFileStats>
{
    private readonly IReadOnlyDictionary<string, int> _nameToId;

    public IcebergStatisticsAccessor(IReadOnlyDictionary<string, int> nameToId)
    {
        _nameToId = nameToId;
    }

    public LiteralValue? GetMinValue(DataFileStats stats, string column) =>
        _nameToId.TryGetValue(column, out int id) ? stats.GetMin(id) : null;

    public LiteralValue? GetMaxValue(DataFileStats stats, string column) =>
        _nameToId.TryGetValue(column, out int id) ? stats.GetMax(id) : null;

    public long? GetNullCount(DataFileStats stats, string column) =>
        _nameToId.TryGetValue(column, out int id) ? stats.GetNullCount(id) : null;

    public long? GetValueCount(DataFileStats stats, string column) => stats.ValueCount;

    public bool IsMinExact(DataFileStats stats, string column) => true;
    public bool IsMaxExact(DataFileStats stats, string column) => true;
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using Apache.Arrow;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Arrow;
using EngineeredWood.IO;
using EngineeredWood.Lance.Table.Deletions;
using EngineeredWood.Lance.Table.Manifest;
using EngineeredWood.Lance.Table.Proto;

namespace EngineeredWood.Lance.Table;

/// <summary>
/// Reads a Lance dataset (a directory containing <c>_versions/</c>,
/// <c>data/</c>, and optionally <c>_indices/</c> / <c>_deletions/</c> /
/// <c>_transactions/</c>). The current MVP supports the read path only:
/// <list type="bullet">
/// <item>Open and parse the latest manifest version.</item>
/// <item>Iterate the dataset's fragments and yield Arrow record batches.</item>
/// </list>
///
/// <para>Out of scope (throws <see cref="NotImplementedException"/> at
/// open time when encountered):</para>
/// <list type="bullet">
/// <item>Deletion files (<c>_deletions/</c>).</item>
/// <item>Time travel to an older version.</item>
/// <item>Predicate pushdown / projection — callers receive full batches
///   and filter / project themselves.</item>
/// <item>Indices (<c>_indices/</c>) — read path ignores them entirely.</item>
/// <item>Writing.</item>
/// </list>
///
/// <para>A dataset whose manifest has <c>base_paths</c> (a shallow clone, imported files, a multi-base write) is read
/// with its files found under their bases; see <see cref="LanceTableOptions.StorageResolver"/> for how a base outside
/// the dataset is reached.</para>
/// </summary>
public sealed class LanceTable : IAsyncDisposable
{
    private readonly ITableFileSystem _fs;
    private readonly bool _ownsFileSystem;
    private readonly Proto.Manifest _manifest;
    private readonly Apache.Arrow.Schema _arrowSchema;
    private readonly LanceFileLocator _locator;
    private readonly IReadOnlyList<Indices.IndexInfo> _indices;

    private LanceTable(
        ITableFileSystem fs, bool ownsFileSystem,
        Proto.Manifest manifest, Apache.Arrow.Schema arrowSchema, LanceFileLocator locator,
        IReadOnlyList<Indices.IndexInfo> indices)
    {
        _fs = fs;
        _ownsFileSystem = ownsFileSystem;
        _manifest = manifest;
        _arrowSchema = arrowSchema;
        _locator = locator;
        _indices = indices;
    }

    /// <summary>The Arrow schema of the dataset.</summary>
    public Apache.Arrow.Schema Schema => _arrowSchema;

    /// <summary>The version number of the manifest currently open.</summary>
    public ulong Version => _manifest.Version;

    /// <summary>
    /// The commit timestamp recorded on this version's manifest, in UTC.
    /// <see cref="DateTimeOffset.MinValue"/> if the manifest didn't carry
    /// a timestamp (older writers may not have stamped them); time-travel
    /// can't use those manifests.
    /// </summary>
    public DateTimeOffset CommitTimestamp =>
        _manifest.Timestamp is null
            ? DateTimeOffset.MinValue
            : _manifest.Timestamp.ToDateTimeOffset();

    /// <summary>
    /// Total row count across every fragment, with deletions applied
    /// (i.e., <c>physical_rows - num_deleted_rows</c> per fragment).
    /// </summary>
    public long NumberOfRows
    {
        get
        {
            long total = 0;
            foreach (var frag in _manifest.Fragments)
            {
                long physical = checked((long)frag.PhysicalRows);
                long deleted = frag.DeletionFile is null ? 0L : (long)frag.DeletionFile.NumDeletedRows;
                total = checked(total + physical - deleted);
            }
            return total;
        }
    }

    /// <summary>The number of fragments in the dataset.</summary>
    public int NumberOfFragments => _manifest.Fragments.Count;

    /// <summary>
    /// The secondary indices active on this dataset version, from its manifest's index section, as lance lists them
    /// (#991). Returns one <see cref="Indices.IndexInfo"/> per index segment, sorted by name and then by the version
    /// each segment was built from; an index can have several segments, each covering its own fragments. Empty when
    /// the version has no indices.
    /// </summary>
    public ValueTask<IReadOnlyList<Indices.IndexInfo>> GetIndicesAsync(
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new ValueTask<IReadOnlyList<Indices.IndexInfo>>(_indices);
    }

    /// <summary>
    /// Open a BTREE scalar index by name. Convenience wrapper that finds the index's
    /// <see cref="Indices.IndexInfo"/> and calls <see cref="OpenBTreeIndexAsync(Indices.IndexInfo, CancellationToken)"/>.
    /// Throws when the index has several segments, since one reader answers for only its segment's fragments; open
    /// each segment by its <see cref="Indices.IndexInfo"/> instead.
    /// </summary>
    public ValueTask<Indices.BTreeIndex> OpenBTreeIndexAsync(
        string indexName, CancellationToken cancellationToken = default)
        => OpenBTreeIndexAsync(FindIndex(indexName), cancellationToken);

    /// <summary>
    /// Open one BTREE index segment of this dataset, as <see cref="GetIndicesAsync"/> listed it. A segment under
    /// another base (<see cref="Indices.IndexInfo.BaseId"/>) is reached through the dataset's storage resolver, and
    /// <see cref="UnregisteredStorageLocationException"/> is thrown when that base is not registered.
    /// </summary>
    public async ValueTask<Indices.BTreeIndex> OpenBTreeIndexAsync(
        Indices.IndexInfo index, CancellationToken cancellationToken = default)
    {
        var directory = ResolveIndexDirectory(index);
        return await Indices.BTreeIndex.OpenAsync(directory.FileSystem, directory.Path, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Open a BITMAP scalar index by name. Convenience wrapper that finds the index's
    /// <see cref="Indices.IndexInfo"/> and calls <see cref="OpenBitmapIndexAsync(Indices.IndexInfo, CancellationToken)"/>.
    /// Throws when the index has several segments; open each by its <see cref="Indices.IndexInfo"/> instead.
    /// </summary>
    public ValueTask<Indices.BitmapIndex> OpenBitmapIndexAsync(
        string indexName, CancellationToken cancellationToken = default)
        => OpenBitmapIndexAsync(FindIndex(indexName), cancellationToken);

    /// <summary>
    /// Open one BITMAP index segment of this dataset, as <see cref="GetIndicesAsync"/> listed it; see
    /// <see cref="OpenBTreeIndexAsync(Indices.IndexInfo, CancellationToken)"/>.
    /// </summary>
    public async ValueTask<Indices.BitmapIndex> OpenBitmapIndexAsync(
        Indices.IndexInfo index, CancellationToken cancellationToken = default)
    {
        var directory = ResolveIndexDirectory(index);
        return await Indices.BitmapIndex.OpenAsync(directory.FileSystem, directory.Path, cancellationToken)
            .ConfigureAwait(false);
    }

    private ResolvedLanceFile ResolveIndexDirectory(Indices.IndexInfo index)
    {
        if (index is null) throw new ArgumentNullException(nameof(index));
        if (!ReferenceEquals(index.Locator, _locator))
            throw new ArgumentException(
                $"Index '{index.Name}' ({index.Uuid}) was not listed by this table's GetIndicesAsync.", nameof(index));
        return index.ResolveDirectory(_fs);
    }

    private Indices.IndexInfo FindIndex(string indexName)
    {
        var matches = _indices.Where(i => string.Equals(i.Name, indexName, StringComparison.Ordinal)).ToList();
        if (matches.Count == 1)
            return matches[0];
        if (matches.Count == 0)
            throw new ArgumentException(
                $"Index '{indexName}' was not found on this dataset. " +
                $"Available: [{string.Join(", ", _indices.Select(i => i.Name).Distinct())}].");
        throw new ArgumentException(
            $"Index '{indexName}' has {matches.Count} segments; open each one by the IndexInfo GetIndicesAsync lists.");
    }

    /// <summary>Optional version tag attached at write time.</summary>
    public string? Tag => string.IsNullOrEmpty(_manifest.Tag) ? null : _manifest.Tag;

    /// <summary>
    /// Opens a Lance dataset at the latest version from a directory on
    /// the local filesystem.
    /// </summary>
    public static ValueTask<LanceTable> OpenAsync(
        string directoryPath, CancellationToken cancellationToken = default)
    {
        var fs = new EngineeredWood.IO.Local.LocalTableFileSystem(directoryPath);
        return OpenAsyncCore(fs, ownsFileSystem: true, version: null, asOf: null, resolver: null, cancellationToken);
    }

    /// <summary>
    /// Opens a Lance dataset at a specific historical version (time travel)
    /// from a directory on the local filesystem. Versions older than the
    /// table's vacuum threshold may have been pruned.
    /// </summary>
    public static ValueTask<LanceTable> OpenAsync(
        string directoryPath, ulong version,
        CancellationToken cancellationToken = default)
    {
        var fs = new EngineeredWood.IO.Local.LocalTableFileSystem(directoryPath);
        return OpenAsyncCore(fs, ownsFileSystem: true, version, asOf: null, resolver: null, cancellationToken);
    }

    /// <summary>
    /// Opens a Lance dataset at the latest version from a caller-provided
    /// <see cref="ITableFileSystem"/> (e.g., Azure Blob, S3). The caller
    /// retains ownership of the filesystem instance.
    /// </summary>
    public static ValueTask<LanceTable> OpenAsync(
        ITableFileSystem fileSystem, CancellationToken cancellationToken = default)
        => OpenAsyncCore(fileSystem, ownsFileSystem: false, version: null, asOf: null, resolver: null, cancellationToken);

    /// <summary>
    /// Opens a Lance dataset at a specific historical version (time travel)
    /// from a caller-provided <see cref="ITableFileSystem"/>.
    /// </summary>
    public static ValueTask<LanceTable> OpenAsync(
        ITableFileSystem fileSystem, ulong version,
        CancellationToken cancellationToken = default)
        => OpenAsyncCore(fileSystem, ownsFileSystem: false, version, asOf: null, resolver: null, cancellationToken);

    /// <summary>
    /// Opens a Lance dataset at the latest version with
    /// <c>commit_timestamp &lt;= asOf</c> from a directory on the local
    /// filesystem. Useful for "what did the table look like at 8 AM
    /// yesterday?" queries. Throws if no committed version is old enough.
    /// </summary>
    public static ValueTask<LanceTable> OpenAsync(
        string directoryPath, DateTimeOffset asOf,
        CancellationToken cancellationToken = default)
    {
        var fs = new EngineeredWood.IO.Local.LocalTableFileSystem(directoryPath);
        return OpenAsyncCore(fs, ownsFileSystem: true, version: null, asOf, resolver: null, cancellationToken);
    }

    /// <inheritdoc cref="OpenAsync(string, DateTimeOffset, CancellationToken)"/>
    public static ValueTask<LanceTable> OpenAsync(
        ITableFileSystem fileSystem, DateTimeOffset asOf,
        CancellationToken cancellationToken = default)
        => OpenAsyncCore(fileSystem, ownsFileSystem: false, version: null, asOf, resolver: null, cancellationToken);

    /// <summary>
    /// Opens a Lance dataset from a caller-provided <see cref="ITableFileSystem"/> with
    /// <paramref name="options"/>: a version or timestamp to open, and the
    /// <see cref="LanceTableOptions.StorageResolver"/> that reaches files the manifest's <c>base_paths</c> name
    /// outside the dataset. The caller retains ownership of the filesystem instance.
    /// </summary>
    public static ValueTask<LanceTable> OpenAsync(
        ITableFileSystem fileSystem, LanceTableOptions options,
        CancellationToken cancellationToken = default)
    {
        if (options is null) throw new ArgumentNullException(nameof(options));
        return OpenAsyncCore(fileSystem, ownsFileSystem: false, options.Version, options.AsOf,
            options.StorageResolver, cancellationToken);
    }

    /// <inheritdoc cref="OpenAsync(ITableFileSystem, LanceTableOptions, CancellationToken)"/>
    public static ValueTask<LanceTable> OpenAsync(
        string directoryPath, LanceTableOptions options,
        CancellationToken cancellationToken = default)
    {
        if (options is null) throw new ArgumentNullException(nameof(options));
        var fs = new EngineeredWood.IO.Local.LocalTableFileSystem(directoryPath);
        return OpenAsyncCore(fs, ownsFileSystem: true, options.Version, options.AsOf,
            options.StorageResolver, cancellationToken);
    }

    /// <summary>
    /// Lists every available version number in the dataset, sorted
    /// ascending. Versions older than the table's vacuum threshold may
    /// have been pruned and are not returned.
    /// </summary>
    public static async ValueTask<IReadOnlyList<ulong>> ListVersionsAsync(
        string directoryPath, CancellationToken cancellationToken = default)
    {
        var fs = new EngineeredWood.IO.Local.LocalTableFileSystem(directoryPath);
        return await ListVersionsAsync(fs, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc cref="ListVersionsAsync(string, CancellationToken)"/>
    public static async ValueTask<IReadOnlyList<ulong>> ListVersionsAsync(
        ITableFileSystem fileSystem, CancellationToken cancellationToken = default)
    {
        var entries = await ManifestPathResolver.ListAllAsync(fileSystem, cancellationToken)
            .ConfigureAwait(false);
        // ListAllAsync returns newest-first; reverse so caller sees ascending.
        var versions = new ulong[entries.Count];
        for (int i = 0; i < entries.Count; i++)
            versions[i] = entries[entries.Count - 1 - i].Version;
        return versions;
    }

    private static async ValueTask<LanceTable> OpenAsyncCore(
        ITableFileSystem fs, bool ownsFileSystem, ulong? version,
        DateTimeOffset? asOf, StorageResolver? resolver, CancellationToken cancellationToken)
    {
        if (version is not null && asOf is not null)
            throw new ArgumentException(
                "Specify either a version or an asOf timestamp — not both.");
        ManifestFileEntry entry = version is { } v
            ? await ManifestPathResolver.ResolveByVersionAsync(fs, v, cancellationToken)
                .ConfigureAwait(false)
            : asOf is { } t
                ? await ManifestPathResolver.ResolveByTimestampAsync(fs, t, cancellationToken)
                    .ConfigureAwait(false)
                : await ManifestPathResolver.ResolveLatestAsync(fs, cancellationToken)
                    .ConfigureAwait(false);
        var (manifest, indexSection) = await ManifestReader.ReadWithIndicesAsync(fs, entry.Path, cancellationToken)
            .ConfigureAwait(false);

        ValidateScope(manifest);
        var locator = new LanceFileLocator(fs, manifest, resolver);
        locator.Validate(manifest);
        var indices = Indices.IndexCatalog.FromManifest(manifest, indexSection, locator);

        // Build an Arrow Schema by re-shaping the Manifest's flat fields into
        // a Lance Proto.Schema and reusing the existing converter.
        var lanceSchema = new EngineeredWood.Lance.Proto.Schema();
        lanceSchema.Fields.AddRange(manifest.Fields);
        foreach (var kv in manifest.SchemaMetadata)
            lanceSchema.Metadata.Add(kv.Key, kv.Value);
        var arrowSchema = EngineeredWood.Lance.Schema.LanceSchemaConverter.ToArrowSchema(lanceSchema);

        return new LanceTable(fs, ownsFileSystem, manifest, arrowSchema, locator, indices);
    }

    /// <summary>
    /// Every fragment id, when each fits the 32 bits an index's fragment bitmap and row addresses hold. lance never
    /// writes a larger one (its <c>max_fragment_id</c> is a u32), but a manifest that has one is read unpruned
    /// rather than refused.
    /// </summary>
    private bool TryGetIndexableFragmentIds(out List<uint> fragmentIds)
    {
        fragmentIds = new List<uint>(_manifest.Fragments.Count);
        foreach (var fragment in _manifest.Fragments)
        {
            if (fragment.Id > uint.MaxValue)
                return false;
            fragmentIds.Add((uint)fragment.Id);
        }
        return true;
    }

    private static void ValidateScope(Proto.Manifest manifest)
    {
        // Feature flags: 1=deletion files, 2=stable row ids, 4=v2 format
        // (deprecated), 8=table config, 16=base paths (lance-table 14.0.3
        // feature_flags.rs FLAG_BASE_PATHS, set whenever base_paths is
        // non-empty). Reject anything we can't honour.
        const ulong FlagDeletionFiles = 1;
        const ulong FlagStableRowIds = 2;
        const ulong FlagV2DeprecatedMarker = 4;
        const ulong FlagTableConfig = 8;
        const ulong FlagBasePaths = 16;
        const ulong KnownReaderFlags = FlagDeletionFiles | FlagStableRowIds
            | FlagV2DeprecatedMarker | FlagTableConfig | FlagBasePaths;
        ulong unknownFlags = manifest.ReaderFeatureFlags & ~KnownReaderFlags;
        if (unknownFlags != 0)
            throw new LanceTableFormatException(
                $"Manifest requires unknown reader feature flags 0x{unknownFlags:X}.");

        foreach (var frag in manifest.Fragments)
        {
            if (frag.Files.Count == 0)
                throw new LanceTableFormatException(
                    $"Fragment {frag.Id} has no data files.");
            // A fragment with multiple files = column-split layout. We
            // currently support only the "all columns in one file" case, which
            // is what pylance produces by default.
            if (frag.Files.Count > 1)
                throw new NotImplementedException(
                    $"Fragment {frag.Id} has {frag.Files.Count} data files (column-split layout); " +
                    "only single-file fragments are supported in the MVP.");
        }
    }

    /// <summary>
    /// Streams the dataset as a sequence of Arrow record batches, one per
    /// fragment in order. Fragments with deletion files have their
    /// deleted rows filtered out before the batch is yielded.
    /// </summary>
    public IAsyncEnumerable<RecordBatch> ReadAsync(
        CancellationToken cancellationToken = default)
        => ReadAsync(columns: null, filter: null, cancellationToken);

    /// <summary>
    /// Streams the dataset, reading only the requested columns. Pass
    /// <c>null</c> to read all columns.
    /// </summary>
    public IAsyncEnumerable<RecordBatch> ReadAsync(
        IReadOnlyList<string>? columns, CancellationToken cancellationToken = default)
        => ReadAsync(columns, filter: null, cancellationToken);

    /// <summary>
    /// Streams the dataset with optional column projection and row-level
    /// filter. The filter is evaluated per row via
    /// <see cref="ArrowRowEvaluator"/>; only rows where the predicate
    /// evaluates to <c>true</c> are returned (SQL three-valued logic —
    /// <c>null</c> results are excluded).
    ///
    /// <para>Note: Lance v2.x manifests don't expose per-fragment column
    /// statistics, so the filter is applied row-by-row after the batch
    /// is materialised. This differs from <c>DeltaTable.ReadAllAsync</c>
    /// where the filter is used for stats-based file pruning. The user
    /// API is intentionally the same so predicates port across formats;
    /// fragment-level pruning can be added later without changing the
    /// signature.</para>
    /// </summary>
    public async IAsyncEnumerable<RecordBatch> ReadAsync(
        IReadOnlyList<string>? columns,
        Predicate? filter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Resolve user projection. This is what callers see at the end.
        Apache.Arrow.Schema outSchema;
        int[] outFieldIndices;
        if (columns is null)
        {
            outSchema = _arrowSchema;
            outFieldIndices = new int[_arrowSchema.FieldsList.Count];
            for (int i = 0; i < outFieldIndices.Length; i++) outFieldIndices[i] = i;
        }
        else
        {
            (outSchema, outFieldIndices) = BuildProjection(columns);
        }

        // We may need columns the filter references but the user didn't
        // project. Read those too, evaluate, then drop them before yield.
        int[] readFieldIndices;
        Apache.Arrow.Schema readSchema;
        int[] outIndicesInReadSchema;
        ArrowRowEvaluator? evaluator = null;
        if (filter is null)
        {
            readFieldIndices = outFieldIndices;
            readSchema = outSchema;
            outIndicesInReadSchema = Enumerable.Range(0, outFieldIndices.Length).ToArray();
        }
        else
        {
            evaluator = new ArrowRowEvaluator();
            (readSchema, readFieldIndices, outIndicesInReadSchema) =
                ExtendProjectionForFilter(outSchema, outFieldIndices, filter);
        }

        // Index-driven fragment pruning. When the filter has predicates
        // over indexed columns, skip fragments whose index proves they
        // contain no matches. A null result means "no useful pruning"
        // and we scan every fragment as before.
        HashSet<uint>? candidateFragments = null;
        if (filter is not null && _indices.Count > 0 && TryGetIndexableFragmentIds(out var fragmentIds))
        {
            candidateFragments = await Indices.IndexPruner
                .ComputeCandidatesAsync(filter, _indices, fragmentIds, _fs, cancellationToken)
                .ConfigureAwait(false);
        }

        foreach (DataFragment fragment in _manifest.Fragments)
        {
            if (candidateFragments is not null
                && !candidateFragments.Contains(checked((uint)fragment.Id)))
                continue;

            DataFile file = fragment.Files[0];
            var dataFile = _locator.ResolveDataFile(fragment, file);
            await using IRandomAccessFile raf = await dataFile.FileSystem
                .OpenReadAsync(dataFile.Path, cancellationToken)
                .ConfigureAwait(false);
            await using var reader = await EngineeredWood.Lance.LanceFileReader
                .OpenAsync(raf, ownsReader: false, cancellationToken)
                .ConfigureAwait(false);

            int rowCount = checked((int)reader.NumberOfRows);
            // Each field is read through the file's fields/column_indices, not by
            // position: after drop_columns the file still holds the dropped columns (#995).
            var fileColumns = DataFileColumns.Create(_manifest.Fields, fragment.Id, file, reader);
            var readArrays = new IArrowArray[readFieldIndices.Length];
            for (int i = 0; i < readFieldIndices.Length; i++)
                readArrays[i] = await fileColumns
                    .ReadAsync(readFieldIndices[i], _arrowSchema.FieldsList[readFieldIndices[i]], cancellationToken)
                    .ConfigureAwait(false);

            var readBatch = new RecordBatch(readSchema, readArrays, rowCount);

            DeletionMask? deletionMask = null;
            if (fragment.DeletionFile is not null && fragment.DeletionFile.NumDeletedRows > 0)
            {
                var deletionFile = _locator.ResolveDeletionFile(
                    fragment, DeletionFileReader.FileName(fragment.Id, fragment.DeletionFile));
                deletionMask = await DeletionFileReader.ReadAsync(
                        deletionFile.FileSystem, deletionFile.Path, fragment.Id, fragment.DeletionFile,
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            BooleanArray? filterMask = evaluator is null
                ? null
                : evaluator.EvaluatePredicate(filter!, readBatch);

            // Combine deletion + filter into one keep-list.
            List<int>? keepRows = null;
            if (deletionMask is not null || filterMask is not null)
            {
                keepRows = new List<int>(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    if (deletionMask is not null && deletionMask.IsDeleted(i)) continue;
                    if (filterMask is not null)
                    {
                        bool? v = filterMask.IsNull(i) ? (bool?)null : filterMask.GetValue(i);
                        if (v != true) continue;
                    }
                    keepRows.Add(i);
                }
            }

            // Project down to the columns the caller asked for, then apply
            // the keep-list. If projection == read schema we can skip the
            // re-build entirely.
            if (outIndicesInReadSchema.Length == readArrays.Length && keepRows is null)
            {
                yield return readBatch;
                continue;
            }

            var outArrays = new IArrowArray[outIndicesInReadSchema.Length];
            for (int i = 0; i < outIndicesInReadSchema.Length; i++)
                outArrays[i] = readArrays[outIndicesInReadSchema[i]];
            var projectedBatch = new RecordBatch(outSchema, outArrays, rowCount);

            if (keepRows is null || keepRows.Count == rowCount)
                yield return projectedBatch;
            else
                yield return RecordBatchRowFilter.ApplyKeepList(projectedBatch, outSchema, keepRows);
        }
    }

    /// <summary>
    /// Extend the user's projection with any extra columns the filter
    /// references. Returns the schema/indices of what to actually read,
    /// plus the position of each output column inside that read batch.
    /// </summary>
    private (Apache.Arrow.Schema readSchema, int[] readFieldIndices, int[] outIndicesInReadSchema)
        ExtendProjectionForFilter(
            Apache.Arrow.Schema outSchema, int[] outFieldIndices, Predicate filter)
    {
        var filterRefs = new HashSet<string>(StringComparer.Ordinal);
        CollectColumnReferences(filter, filterRefs);

        // Build read indices = outFieldIndices ∪ (filterRefs as field indices).
        var nameToIndex = new Dictionary<string, int>(_arrowSchema.FieldsList.Count, StringComparer.Ordinal);
        for (int i = 0; i < _arrowSchema.FieldsList.Count; i++)
            nameToIndex[_arrowSchema.FieldsList[i].Name] = i;

        var readSet = new HashSet<int>(outFieldIndices);
        foreach (string name in filterRefs)
        {
            if (!nameToIndex.TryGetValue(name, out int idx))
                throw new ArgumentException(
                    $"Filter references unknown column '{name}'. " +
                    $"Available: [{string.Join(", ", nameToIndex.Keys)}].",
                    nameof(filter));
            readSet.Add(idx);
        }

        // Output column order must match outFieldIndices; filter-only columns
        // are appended in any order (we drop them before yielding).
        var readIndices = new List<int>(outFieldIndices);
        foreach (int idx in readSet)
            if (!readIndices.Contains(idx)) readIndices.Add(idx);

        var readFields = readIndices.Select(i => _arrowSchema.FieldsList[i]).ToArray();
        var readSchema = new Apache.Arrow.Schema(readFields, _arrowSchema.Metadata);

        // outIndicesInReadSchema[i] = position of outFieldIndices[i] in readIndices.
        var posInRead = new Dictionary<int, int>(readIndices.Count);
        for (int i = 0; i < readIndices.Count; i++) posInRead[readIndices[i]] = i;
        var outInRead = outFieldIndices.Select(idx => posInRead[idx]).ToArray();

        return (readSchema, readIndices.ToArray(), outInRead);
    }

    private static void CollectColumnReferences(Predicate predicate, HashSet<string> sink)
    {
        switch (predicate)
        {
            case TruePredicate or FalsePredicate:
                break;
            case AndPredicate and:
                foreach (var c in and.Children) CollectColumnReferences(c, sink);
                break;
            case OrPredicate or:
                foreach (var c in or.Children) CollectColumnReferences(c, sink);
                break;
            case NotPredicate not:
                CollectColumnReferences(not.Child, sink);
                break;
            case ComparisonPredicate cmp:
                CollectColumnReferences(cmp.Left, sink);
                CollectColumnReferences(cmp.Right, sink);
                break;
            case UnaryPredicate u:
                CollectColumnReferences(u.Operand, sink);
                break;
            case SetPredicate s:
                CollectColumnReferences(s.Operand, sink);
                break;
        }
    }

    private static void CollectColumnReferences(Expression expression, HashSet<string> sink)
    {
        switch (expression)
        {
            case UnboundReference u: sink.Add(u.Name); break;
            case BoundReference b: sink.Add(b.Name); break;
            case LiteralExpression: break;
            case FunctionCall fc:
                foreach (var arg in fc.Arguments) CollectColumnReferences(arg, sink);
                break;
            case Predicate p: CollectColumnReferences(p, sink); break;
        }
    }

    /// <summary>
    /// Resolve a list of column names into a projected
    /// <see cref="Apache.Arrow.Schema"/> plus the indices into the
    /// underlying file's top-level field list.
    /// </summary>
    private (Apache.Arrow.Schema, int[]) BuildProjection(IReadOnlyList<string> columns)
    {
        if (columns.Count == 0)
            throw new ArgumentException("Projection list must contain at least one column.", nameof(columns));

        // Pre-build a name -> index lookup for the dataset schema.
        var nameToIndex = new Dictionary<string, int>(_arrowSchema.FieldsList.Count, StringComparer.Ordinal);
        for (int i = 0; i < _arrowSchema.FieldsList.Count; i++)
            nameToIndex[_arrowSchema.FieldsList[i].Name] = i;

        var indices = new int[columns.Count];
        var fields = new Apache.Arrow.Field[columns.Count];
        var seen = new HashSet<int>();
        seen.EnsureCapacity(columns.Count);
        for (int i = 0; i < columns.Count; i++)
        {
            if (!nameToIndex.TryGetValue(columns[i], out int idx))
                throw new ArgumentException(
                    $"Column '{columns[i]}' not found in dataset schema. " +
                    $"Available: [{string.Join(", ", nameToIndex.Keys)}].",
                    nameof(columns));
            if (!seen.Add(idx))
                throw new ArgumentException(
                    $"Column '{columns[i]}' specified more than once in projection.",
                    nameof(columns));
            indices[i] = idx;
            fields[i] = _arrowSchema.FieldsList[idx];
        }

        return (new Apache.Arrow.Schema(fields, _arrowSchema.Metadata), indices);
    }

    public async ValueTask DisposeAsync()
    {
        if (_ownsFileSystem && _fs is IAsyncDisposable ad)
            await ad.DisposeAsync().ConfigureAwait(false);
        else if (_ownsFileSystem && _fs is IDisposable d)
            d.Dispose();
    }
}

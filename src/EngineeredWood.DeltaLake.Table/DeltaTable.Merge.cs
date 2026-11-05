// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Arrow;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Table;

public sealed partial class DeltaTable
{
    /// <summary>
    /// Merges <paramref name="source"/> into the table on key equality and commits it: the key-based form of
    /// <c>MERGE INTO target USING source ON target.k = source.k WHEN MATCHED ... WHEN NOT MATCHED ... WHEN NOT
    /// MATCHED BY SOURCE ...</c>. <paramref name="options"/> names the keys and the clauses; see
    /// <see cref="MergeOptions"/> for how rows match and how conditions name their columns.
    ///
    /// <para>Every file holding a row the MERGE updates or deletes is rewritten once, with the matched rows changed
    /// and the rest copied (copy-on-write, as delta-rs merges; no deletion vectors are written). Unmatched source
    /// rows are appended. All of it commits as one version, through the same conflict check, rebase and retry as
    /// <see cref="DeltaTransaction.CommitAsync"/>; a MERGE that changes nothing commits nothing and returns the
    /// unchanged version.</para>
    ///
    /// <para>Files whose key statistics cannot overlap the source's key range are not read, unless
    /// <see cref="MergeOptions.WhenNotMatchedBySource"/> has a clause. Updated and inserted rows go through the table's
    /// generated columns, identity columns, CHECK constraints and NOT NULL, and a Change Data Feed table records
    /// <c>update_preimage</c>/<c>update_postimage</c>, <c>delete</c> and <c>insert</c> rows.</para>
    /// </summary>
    /// <param name="source">The source rows. Every column must be a column of the table, by logical name and
    /// type; columns the table has and the source lacks are left alone by an update and written as an append
    /// would write them by an insert.</param>
    /// <param name="options">The keys and clauses.</param>
    /// <param name="cancellationToken">Cancels the MERGE; files it already wrote are deleted again.</param>
    /// <param name="commitProperties">The caller's own metadata for the commit.</param>
    public async ValueTask<(MergeMetrics Metrics, long Version)> MergeAsync(
        IReadOnlyList<RecordBatch> source, MergeOptions options, CancellationToken cancellationToken = default,
        CommitProperties? commitProperties = null)
    {
        ThrowIfDisposed();
        await using var transaction = StartTransaction(); // takes back what it wrote on any failure
        transaction.CommitProperties = commitProperties;
        var metrics = await transaction.MergeAsync(source, options, cancellationToken).ConfigureAwait(false);
        long version = await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return (metrics, version);
    }

    /// <summary>The actions a MERGE produces and what it read, without committing.</summary>
    /// <param name="RewriteActions">The removes and adds of the files it rewrote, and their change files.</param>
    /// <param name="InsertActions">The appended source rows' actions, kept apart so a transaction can record them as
    /// rows no change file covers.</param>
    /// <param name="RemovedPaths">The files it rewrote: its planned removes, and part of its read set.</param>
    /// <param name="ReadPaths">Every file it read, rewritten or not.</param>
    /// <param name="ReadPredicate">The key-range predicate every file it could have matched satisfies, or null when
    /// it has none (no key type it can bound, or no scan).</param>
    /// <param name="ReadsWholeTable">Whether it read every file regardless of the source (a not-matched-by-source
    /// clause).</param>
    /// <param name="ScannedTarget">Whether it read the target at all. A source with no matchable key needs no
    /// scan, and then the MERGE reads nothing.</param>
    /// <param name="NextRowId">Where the next staged add reserves stable row ids; null without row tracking.</param>
    internal sealed record MergeActions(
        IReadOnlyList<DeltaAction> RewriteActions,
        IReadOnlyList<DeltaAction> InsertActions,
        ISet<string> RemovedPaths,
        ISet<string> ReadPaths,
        Expressions.Predicate? ReadPredicate,
        bool ReadsWholeTable,
        bool ScannedTarget,
        MergeMetrics Metrics,
        long? NextRowId);

    // A target row's fate in a MERGE.
    private const byte MergeCopy = 0, MergeUpdate = 1, MergeMatchedDelete = 2, MergeSourcelessDelete = 3,
        MergeSourcelessUpdate = 4;

    /// <summary>
    /// Computes a MERGE against <paramref name="snapshot"/> WITHOUT committing: each touched file rewritten once
    /// with its updates and deletes applied together (a transaction refuses a second operation on a file, so they
    /// cannot be staged apart), then the unmatched source rows appended.
    /// </summary>
    /// <param name="rowIdStart">Where the rewrite and the insert reserve stable row ids. Null starts at the
    /// snapshot's mark.</param>
    /// <param name="written">Records the rewritten, inserted and change-data files this MERGE writes.</param>
    /// <param name="identityHighWaterMarks">The identity marks a transaction's earlier appends left pending, for
    /// the insert to continue from (see <see cref="ComputeWriteActionsAsync"/>).</param>
    internal async ValueTask<MergeActions> ComputeMergeActionsAsync(
        Snapshot.Snapshot snapshot,
        IReadOnlyList<RecordBatch> source,
        MergeOptions options,
        CancellationToken cancellationToken,
        long? rowIdStart = null,
        WrittenFileLedger? written = null,
        IReadOnlyDictionary<string, long>? identityHighWaterMarks = null)
    {
        if (source is null)
            throw new ArgumentNullException(nameof(source));
        if (options is null)
            throw new ArgumentNullException(nameof(options));

        var plan = MergePlan.Validate(options, snapshot.Schema);
        var src = PrepareMergeSource(source, snapshot, plan);
        plan.Resolve(src, snapshot.Schema);
        RefuseUnanalyzableClauses(plan, snapshot, src);
        int sourceRows = src?.Length ?? 0;
        ThrowIfMergeSetsIdentity(plan.UpdatedColumns, snapshot.Schema);
        // An insert that would write a GENERATED ALWAYS identity column, refused here rather than when its rows reach
        // the append: Spark refuses it at analysis, whatever the source holds or the clauses match (#595).
        IdentityColumns.IdentityColumnWriter.ThrowIfGeneratedAlwaysSupplied(plan.InsertedColumns, snapshot.Schema);

        // The source's key index: each key's rows chained through `next`, so a duplicated key costs no list.
        // A row with a NULL key matches nothing and is not indexed.
        var head = new Dictionary<byte[], int>(MergeKeyComparer.Instance);
        var next = new int[sourceRows];
        IArrowArray[]? sourceKeys = null;
        if (src is not null)
        {
            sourceKeys = plan.Keys.Select(k => src.Column(src.Schema.GetFieldIndex(k))).ToArray();
            for (int i = 0; i < sourceRows; i++)
            {
                next[i] = -1;
                if (MergeKeys.Encode(sourceKeys, i) is not { } key)
                    continue;
                if (head.TryGetValue(key, out int first))
                    next[i] = first;
                head[key] = i;
            }
        }
        var sourceMatched = new bool[sourceRows];

        var rewriteActions = new List<DeltaAction>();
        var removedPaths = new HashSet<string>(StringComparer.Ordinal);
        var readPaths = new HashSet<string>(StringComparer.Ordinal);
        long matchedUpdated = 0, sourcelessUpdated = 0, matchedDeleted = 0, sourcelessDeleted = 0, copied = 0;

        bool assignRowIds = DeltaLake.RowTracking.RowTrackingConfig.IsSupported(snapshot.Protocol);
        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(snapshot.Metadata.Configuration);
        long nextRowId = assignRowIds ? rowIdStart ?? snapshot.RowIdHighWaterMark : 0;

        // Without a not-matched-by-source clause only a file that can hold a source key matters, so none needs
        // reading when no source key can match; and the source's key range prunes the rest by statistics.
        bool readsWholeTable = plan.NotMatchedBySource.Count > 0;
        bool scan = readsWholeTable || head.Count > 0;
        Expressions.Predicate? keyRange = !readsWholeTable && head.Count > 0
            ? KeyRangePredicate(plan.Keys, sourceKeys!, snapshot.Schema)
            : null;

        if (scan)
        {
            nextRowId = await ScanMergeTargetAsync().ConfigureAwait(false);
        }

        // The unmatched source rows, each inserted by the first not-matched clause whose condition holds.
        var insertActions = new List<DeltaAction>();
        long inserted = 0;
        if (plan.NotMatched.Count > 0 && src is not null && sourceRows > 0)
        {
            var remaining = new List<int>();
            for (int i = 0; i < sourceRows; i++)
            {
                if (!sourceMatched[i])
                    remaining.Add(i);
            }

            var insertBatches = new List<RecordBatch>();
            foreach (var clause in plan.NotMatched)
            {
                if (remaining.Count == 0)
                    break;
                var candidates = remaining.Count == sourceRows ? src : TakeRowsFromBatch(src, remaining);
                var taken = TakeClause(clause, remaining, () => MergeConditionBatch(null, candidates), out remaining);
                if (taken.Count == 0)
                    continue;
                var rows = taken.Count == candidates.Length ? candidates : TakeRowsFromBatch(src, taken);
                insertBatches.Add(MergeInsertBatch(rows, clause, snapshot));
                inserted += taken.Count;
            }

            if (insertBatches.Count > 0)
            {
                var (actions, insertNextRowId) = await ComputeWriteActionsAsync(
                    snapshot, insertBatches, DeltaWriteMode.Append,
                    overwritePartitions: null, dynamicPartitionOverwrite: false, repartitionTo: null,
                    cancellationToken, rowIdStart: assignRowIds ? nextRowId : null, written: written,
                    identityHighWaterMarks: identityHighWaterMarks)
                    .ConfigureAwait(false);
                insertActions.AddRange(actions);
                if (assignRowIds)
                    nextRowId = insertNextRowId;
            }
        }

        long filesAdded = rewriteActions.Concat(insertActions).Count(a => a is AddFile);
        var metrics = new MergeMetrics(
            sourceRows, inserted, matchedUpdated, sourcelessUpdated, matchedDeleted, sourcelessDeleted, copied,
            filesAdded, removedPaths.Count);
        return new MergeActions(
            rewriteActions, insertActions, removedPaths, readPaths, keyRange, readsWholeTable, scan, metrics,
            assignRowIds ? nextRowId : null);

        async ValueTask<long> ScanMergeTargetAsync()
        {
            var constraints = DeltaConstraintEnforcer.Create(snapshot);
            var generatedColumns = DeltaGeneratedColumns.Create(snapshot);
            bool cdfEnabled = DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(snapshot.Metadata.Configuration);
            var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
            var (matRowIdName, matRowVerName) = DeltaLake.RowTracking.RowTrackingConfig
                .TryGetMaterializedColumnNames(snapshot.Metadata.Configuration);
            // ValidateWritable refused a row-tracking table without the materialized column names, so a tracked
            // table always materializes the rewritten rows' identity.
            bool materializeIds = rowTrackingEnabled && matRowIdName is not null && matRowVerName is not null;
            long newVersion = snapshot.Version + 1;
            long rowId = nextRowId;
            var pruner = keyRange is null ? null : new DeltaFilePruner(
                snapshot.Schema, snapshot.Metadata.PartitionColumns, _options.PreferTypedCheckpointStats);
            bool anyMatched = plan.Matched.Count > 0;
            // As delta-spark decides it: a target row two source rows' keys equal is refused whatever the clause
            // conditions say (it counts ON matches alone), unless the only matched clause is one unconditional
            // DELETE, which every candidate applies identically. An insert-only MERGE changes no target row.
            bool unconditionalDelete = plan.Matched.Count == 1
                && plan.Matched[0].Kind == MergeActionKind.Delete && plan.Matched[0].Condition is null;
            bool refuseMultipleMatches = (anyMatched || plan.NotMatchedBySource.Count > 0) && !unconditionalDelete;

            foreach (var addFile in snapshot.ActiveFiles.Values)
            {
                if (pruner is not null && !pruner.ShouldInclude(addFile, keyRange!))
                    continue; // no row here can hold a source key

                readPaths.Add(addFile.Path);
                var batches = new List<RecordBatch>();
                var srcIds = materializeIds ? new List<Int64Array?>() : null;
                var srcVers = materializeIds ? new List<Int64Array?>() : null;
                await foreach (var batch in ReadFileAsync(
                    addFile, null, snapshot, cancellationToken, srcIds, srcVers).ConfigureAwait(false))
                {
                    batches.Add(batch);
                }

                var router = new PartitionRouter(
                    snapshot, mappingMode, batches.FirstOrDefault(b => b.Length > 0), materializeIds);
                bool fileModified = false;
                long copiedHere = 0;
                var preimages = new List<RecordBatch>();
                var postimages = new List<RecordBatch>();
                var deletes = new List<RecordBatch>();
                var updateTracking = new List<(Int64Array? Ids, Int64Array? Vers)>();
                var deleteTracking = new List<(Int64Array? Ids, Int64Array? Vers)>();

                for (int bi = 0; bi < batches.Count; bi++)
                {
                    var batch = batches[bi];
                    var batchIds = srcIds is not null && bi < srcIds.Count ? srcIds[bi] : null;
                    var batchVers = srcVers is not null && bi < srcVers.Count ? srcVers[bi] : null;
                    int n = batch.Length;
                    // Each row's fate; for an update or delete, the clause that took it (an index into its kind's
                    // clauses); and for a matched row, the source row that matched it.
                    var fate = new byte[n];
                    var fateClause = new int[n];
                    var fateSource = new int[n];
                    var hit = new bool[n];

                    // ON: every (target, source) pair whose keys are equal.
                    var targetKeys = plan.Keys.Select(k => batch.Column(batch.Schema.GetFieldIndex(k))).ToArray();
                    var pairTarget = new List<int>();
                    var pairSource = new List<int>();
                    for (int i = 0; i < n; i++)
                    {
                        if (MergeKeys.Encode(targetKeys, i) is not { } key || !head.TryGetValue(key, out int s))
                            continue;
                        hit[i] = true;
                        if (refuseMultipleMatches && next[s] >= 0)
                            throw MultipleSourceRowsMatched(addFile.Path);
                        for (; s >= 0; s = next[s])
                        {
                            sourceMatched[s] = true;
                            if (anyMatched)
                            {
                                pairTarget.Add(i);
                                pairSource.Add(s);
                            }
                        }
                    }

                    // WHEN MATCHED [AND condition], in order: a pair takes the first clause whose condition holds,
                    // and a clause sees only the pairs no earlier one took. Only an unconditional DELETE can see a
                    // target row twice here.
                    if (pairTarget.Count > 0)
                    {
                        var remaining = Enumerable.Range(0, pairTarget.Count).ToList();
                        for (int c = 0; c < plan.Matched.Count && remaining.Count > 0; c++)
                        {
                            var pending = remaining;
                            var taken = TakeClause(plan.Matched[c], pending, () => MergeConditionBatch(
                                TakeRowsFromBatch(batch, pending.Select(p => pairTarget[p]).ToList()),
                                TakeRowsFromBatch(src!, pending.Select(p => pairSource[p]).ToList())), out remaining);
                            byte kind = plan.Matched[c].Kind == MergeActionKind.Delete ? MergeMatchedDelete : MergeUpdate;
                            foreach (int p in taken)
                            {
                                int t = pairTarget[p];
                                if (fate[t] != MergeCopy)
                                    continue;
                                fate[t] = kind;
                                fateClause[t] = c;
                                fateSource[t] = pairSource[p];
                            }
                        }
                    }

                    // WHEN NOT MATCHED BY SOURCE [AND condition], in order, over the target rows no source row's
                    // keys equal.
                    if (plan.NotMatchedBySource.Count > 0)
                    {
                        var remaining = new List<int>();
                        for (int i = 0; i < n; i++)
                        {
                            if (!hit[i])
                                remaining.Add(i);
                        }
                        for (int c = 0; c < plan.NotMatchedBySource.Count && remaining.Count > 0; c++)
                        {
                            var pending = remaining;
                            var taken = TakeClause(plan.NotMatchedBySource[c], pending,
                                () => MergeConditionBatch(
                                    pending.Count == n ? batch : TakeRowsFromBatch(batch, pending), null),
                                out remaining);
                            byte kind = plan.NotMatchedBySource[c].Kind == MergeActionKind.Delete
                                ? MergeSourcelessDelete : MergeSourcelessUpdate;
                            foreach (int t in taken)
                            {
                                fate[t] = kind;
                                fateClause[t] = c;
                            }
                        }
                    }

                    var keepRows = new List<int>();
                    var deleteRows = new List<int>();
                    // The updated rows, grouped by the clause that took them: each clause sets its own columns.
                    var updateGroups = new SortedDictionary<(byte Fate, int Clause), (List<int> Rows, List<int> Sources)>();
                    for (int i = 0; i < n; i++)
                    {
                        switch (fate[i])
                        {
                            case MergeUpdate:
                            case MergeSourcelessUpdate:
                                if (!updateGroups.TryGetValue((fate[i], fateClause[i]), out var group))
                                    updateGroups[(fate[i], fateClause[i])] = group = (new List<int>(), new List<int>());
                                group.Rows.Add(i);
                                group.Sources.Add(fateSource[i]);
                                break;
                            case MergeMatchedDelete:
                                matchedDeleted++;
                                deleteRows.Add(i);
                                break;
                            case MergeSourcelessDelete:
                                sourcelessDeleted++;
                                deleteRows.Add(i);
                                break;
                            default:
                                keepRows.Add(i);
                                break;
                        }
                    }

                    if (updateGroups.Count == 0 && deleteRows.Count == 0)
                    {
                        router.Add(batch, batchIds is not null && batchVers is not null
                            ? (batchIds, batchVers) : ((Int64Array, Int64Array)?)null, mayMove: false);
                        copiedHere += n;
                        continue;
                    }

                    if (!fileModified)
                    {
                        // Before this file writes anything. (The transaction takes back what earlier files wrote.)
                        RejectCopyOnWriteRowIdUnsupported("MERGE");
                        ThrowIfAppendOnly(snapshot, "MERGE");
                        fileModified = true;
                    }

                    foreach (var entry in updateGroups)
                    {
                        bool isMatched = entry.Key.Fate == MergeUpdate;
                        var clause = isMatched ? plan.Matched[entry.Key.Clause] : plan.NotMatchedBySource[entry.Key.Clause];
                        var (updateRows, updateSources) = entry.Value;
                        if (isMatched)
                            matchedUpdated += updateRows.Count;
                        else
                            sourcelessUpdated += updateRows.Count;
                        var preimage = TakeRowsFromBatch(batch, updateRows);
                        var postimage = MergePostImage(
                            preimage, isMatched ? TakeRowsFromBatch(src!, updateSources) : null, clause, snapshot);
                        ThrowIfUpdaterChangedTypes(preimage, postimage, snapshot.Schema, "MERGE");
                        ThrowIfPartitionColumnsRetyped(postimage, snapshot, "MERGE");
                        postimage = PreparePostImage(
                            postimage, snapshot, generatedColumns, constraints, clause.WrittenColumns);

                        // An updated row keeps its id; its commit version advances to this commit.
                        var ids = batchIds is not null ? TakeIds(batchIds, updateRows) : null;
                        router.Add(postimage, ids is not null
                            ? (ids, ConstInt64(newVersion, updateRows.Count))
                            : ((Int64Array, Int64Array)?)null, mayMove: true);

                        if (cdfEnabled)
                        {
                            preimages.Add(preimage);
                            postimages.Add(postimage);
                            updateTracking.Add((ids, batchVers is not null ? TakeIds(batchVers, updateRows) : null));
                        }
                    }

                    if (deleteRows.Count > 0 && cdfEnabled)
                    {
                        deletes.Add(TakeRowsFromBatch(batch, deleteRows));
                        deleteTracking.Add((
                            batchIds is not null ? TakeIds(batchIds, deleteRows) : null,
                            batchVers is not null ? TakeIds(batchVers, deleteRows) : null));
                    }

                    if (keepRows.Count > 0)
                    {
                        // Unchanged rows of a changed file: original id and commit version.
                        router.Add(TakeRowsFromBatch(batch, keepRows), batchIds is not null && batchVers is not null
                            ? (TakeIds(batchIds, keepRows), TakeIds(batchVers, keepRows))
                            : ((Int64Array, Int64Array)?)null, mayMove: false);
                        copiedHere += keepRows.Count;
                    }
                }

                if (!fileModified)
                    continue;

                copied += copiedHere;
                rowId = await WriteRewriteGroupsAsync(
                    snapshot, addFile, mappingMode, router, materializeIds, matRowIdName, matRowVerName,
                    assignRowIds, rowId, newVersion, rewriteActions, written, cancellationToken)
                    .ConfigureAwait(false);
                removedPaths.Add(addFile.Path);

                if (!cdfEnabled)
                    continue;

                // The change rows, each from the one read that also fed the rewrite, so a row's stable id in the
                // feed is the id the rewrite preserved.
                for (int b = 0; b < deletes.Count; b++)
                {
                    rewriteActions.Add(await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, snapshot, deletes[b], DeltaLake.ChangeDataFeed.CdfConfig.Delete,
                        addFile.PartitionValues, DataFileWriteOptions(snapshot),
                        cancellationToken, deleteTracking[b].Ids, deleteTracking[b].Vers, written)
                        .ConfigureAwait(false));
                }
                for (int b = 0; b < preimages.Count; b++)
                {
                    var (ids, vers) = updateTracking[b];
                    rewriteActions.Add(await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, snapshot, preimages[b], DeltaLake.ChangeDataFeed.CdfConfig.UpdatePreimage,
                        addFile.PartitionValues, DataFileWriteOptions(snapshot),
                        cancellationToken, ids, vers, written).ConfigureAwait(false));

                    // A post-image row belongs to the partition the row now lives in.
                    var postVers = ids is not null ? ConstInt64(newVersion, postimages[b].Length) : null;
                    foreach (var (target, _, post, aligned) in router.Split(postimages[b], ids, postVers))
                    {
                        rewriteActions.Add(await ChangeDataFeed.CdfWriter.WriteAsync(
                            _fs, snapshot, post, DeltaLake.ChangeDataFeed.CdfConfig.UpdatePostimage,
                            target ?? addFile.PartitionValues, DataFileWriteOptions(snapshot),
                            cancellationToken, aligned[0], aligned[1], written).ConfigureAwait(false));
                    }
                }
            }

            return rowId;
        }
    }

    /// <summary>
    /// Checks the source against the table and combines its batches into one, so a key index can address any
    /// source row. Null when there are no batches at all.
    /// </summary>
    private RecordBatch? PrepareMergeSource(
        IReadOnlyList<RecordBatch> source, Snapshot.Snapshot snapshot, MergePlan plan)
    {
        foreach (var batch in source)
        {
            if (batch is null)
                throw new ArgumentException("A source batch is null.", nameof(source));
            ThrowIfDuplicateColumns(batch.Schema, snapshot.Schema, "MERGE");
            foreach (var field in batch.Schema.FieldsList)
            {
                // By logical name: a condition names columns the way the table's schema does.
                if (!snapshot.Schema.Fields.Any(f => string.Equals(f.Name, field.Name, StringComparison.Ordinal)))
                {
                    throw new ArgumentException(
                        $"MERGE: the source has a column '{field.Name}' that the table does not declare. A MERGE "
                        + "does not evolve the schema; drop the column from the source, or ALTER the table first. "
                        + "The table declares: "
                        + string.Join(", ", snapshot.Schema.Fields.Select(f => "'" + f.Name + "'")) + ".",
                        nameof(source));
                }
            }
        }
        if (!HostOwnsBytes)
            ThrowIfMistypedColumns(source, snapshot.Schema, "MERGE");
        source = NormalizeUnlessHostOwnsBytes(source, snapshot.Metadata.PartitionColumns);
        if (source.Count == 0)
            return null;
        // Each batch on its own, before they are combined: two batches may supply one column in different forms
        // (milliseconds and microseconds), which only agree once both are the table's.
        if (!HostOwnsBytes)
            source = source.Select(b => ConformToTable(b, snapshot.ArrowSchema)).ToList();

        var schema = source[0].Schema;
        for (int b = 1; b < source.Count; b++)
        {
            var other = source[b].Schema;
            if (other.FieldsList.Count != schema.FieldsList.Count
                || !other.FieldsList.Select(f => f.Name).SequenceEqual(schema.FieldsList.Select(f => f.Name)))
            {
                throw new ArgumentException(
                    "MERGE: every source batch must have the same columns in the same order.", nameof(source));
            }
        }
        foreach (string key in plan.Keys)
        {
            if (schema.GetFieldIndex(key) < 0)
            {
                throw new ArgumentException(
                    $"MERGE: the source has no key column '{key}'.", nameof(source));
            }
        }

        var combined = source[0];
        if (source.Count > 1)
        {
            var columns = new IArrowArray[schema.FieldsList.Count];
            for (int c = 0; c < columns.Length; c++)
                columns[c] = ArrowCompute.Concatenate(source.Select(b => b.Column(c)).ToList());
            combined = new RecordBatch(schema, columns, source.Sum(b => b.Length));
        }
        return combined;
    }

    /// <summary>
    /// <paramref name="source"/> with each column the normalization leaves in another Arrow form of its Delta type
    /// (large strings and binaries, timestamps in seconds or milliseconds or under another zone name) converted to
    /// the table's. A MERGE puts source values into the files it rewrites beside rows it only copies, and one
    /// file holds one Arrow type per column. A timestamp out of the microsecond range is refused.
    /// </summary>
    private static RecordBatch ConformToTable(RecordBatch source, Apache.Arrow.Schema table)
    {
        IArrowArray[]? columns = null;
        Field[]? fields = null;
        for (int c = 0; c < source.ColumnCount; c++)
        {
            var field = source.Schema.FieldsList[c];
            var target = table.GetFieldByName(field.Name)?.DataType;
            var column = source.Column(c);
            IArrowArray? converted = (column, target) switch
            {
                (LargeStringArray large, Apache.Arrow.Types.StringType) => ToString(large),
                (LargeBinaryArray large, Apache.Arrow.Types.BinaryType) when target.TypeId == Apache.Arrow.Types.ArrowTypeId.Binary
                    => ToBinary(large),
                (TimestampArray ts, Apache.Arrow.Types.TimestampType to)
                    when !ts.Data.DataType.Equals(to) => ToTimestamp(ts, to, field.Name),
                _ => null,
            };
            if (converted is null)
                continue;
            columns ??= Enumerable.Range(0, source.ColumnCount).Select(source.Column).ToArray();
            fields ??= source.Schema.FieldsList.ToArray();
            columns[c] = converted;
            fields[c] = new Field(field.Name, converted.Data.DataType, field.IsNullable, field.Metadata);
        }
        return columns is null
            ? source
            : new RecordBatch(new Apache.Arrow.Schema(fields!, source.Schema.Metadata), columns, source.Length);

        static StringArray ToString(LargeStringArray large)
        {
            var builder = new StringArray.Builder();
            for (int i = 0; i < large.Length; i++)
            {
                if (large.IsNull(i))
                    builder.AppendNull();
                else
                    builder.Append(large.GetString(i));
            }
            return builder.Build();
        }

        static BinaryArray ToBinary(LargeBinaryArray large)
        {
            var builder = new BinaryArray.Builder();
            for (int i = 0; i < large.Length; i++)
            {
                if (large.IsNull(i))
                    builder.AppendNull();
                else
                    builder.Append(large.GetBytes(i));
            }
            return builder.Build();
        }

        static TimestampArray ToTimestamp(TimestampArray ts, Apache.Arrow.Types.TimestampType to, string name)
        {
            var from = (Apache.Arrow.Types.TimestampType)ts.Data.DataType;
            long scale = Scale(to.Unit) / Scale(from.Unit);
            if (scale == 0)
            {
                // Narrowing (nanoseconds): never on this path, which refuses the unit before it gets here.
                throw new ArgumentException(
                    $"MERGE: column '{name}' supplies timestamps in {from.Unit}, finer than the table's {to.Unit}.");
            }
            var values = new long[ts.Length];
            var validity = new ArrowBuffer.BitmapBuilder(ts.Length);
            for (int i = 0; i < ts.Length; i++)
            {
                bool valid = !ts.IsNull(i);
                validity.Append(valid);
                if (!valid)
                    continue;
                long value = ts.GetValue(i)!.Value;
                try
                {
                    values[i] = checked(value * scale);
                }
                catch (OverflowException)
                {
                    throw new ArgumentException(
                        $"MERGE: column '{name}' holds the timestamp {value} ({from.Unit}), which is outside the "
                        + "range of a Delta timestamp.");
                }
            }
            return new TimestampArray(
                to, new ArrowBuffer.Builder<long>().AppendRange(values).Build(), validity.Build(),
                ts.Length, ts.NullCount, 0);
        }

        static long Scale(Apache.Arrow.Types.TimeUnit unit) => unit switch
        {
            Apache.Arrow.Types.TimeUnit.Second => 1,
            Apache.Arrow.Types.TimeUnit.Millisecond => 1_000,
            Apache.Arrow.Types.TimeUnit.Microsecond => 1_000_000,
            _ => 1_000_000_000,
        };
    }

    /// <summary>The columns of <paramref name="batch"/> <paramref name="columns"/> names, in the batch's order.</summary>
    private static RecordBatch ProjectColumns(RecordBatch batch, IReadOnlyList<string> columns)
    {
        var keep = Enumerable.Range(0, batch.ColumnCount).Where(c => columns.Contains(batch.Schema.FieldsList[c].Name)).ToList();
        if (keep.Count == batch.ColumnCount)
            return batch;
        return new RecordBatch(
            new Apache.Arrow.Schema(keep.Select(c => batch.Schema.FieldsList[c]).ToList(), batch.Schema.Metadata),
            keep.Select(c => batch.Column(c)).ToList(), batch.Length);
    }

    /// <summary>
    /// The rows of <paramref name="pending"/> <paramref name="clause"/> takes: all of them when it is unconditional,
    /// else those its condition holds for over <paramref name="rows"/> (one row per entry of
    /// <paramref name="pending"/>, built only when asked). <paramref name="rest"/> is the rows it leaves for the next
    /// clause.
    /// </summary>
    private static List<int> TakeClause(
        MergeClausePlan clause, List<int> pending, Func<RecordBatch> rows, out List<int> rest)
    {
        if (clause.Condition is not { } condition)
        {
            rest = [];
            return pending;
        }
        var holds = MaskFor(condition)(rows());
        var taken = new List<int>();
        rest = new List<int>();
        for (int r = 0; r < pending.Count; r++)
        {
            if (!holds.IsNull(r) && holds.GetValue(r) == true)
                taken.Add(pending[r]);
            else
                rest.Add(pending[r]);
        }
        return taken;
    }

    /// <summary>
    /// The target rows <paramref name="preimage"/> as <paramref name="clause"/>'s update leaves them: each column it
    /// writes taken from the matching source row (<paramref name="sources"/>, aligned with the preimage; null for a
    /// not-matched-by-source update) or computed from both rows, the rest as they were.
    /// </summary>
    private static RecordBatch MergePostImage(
        RecordBatch preimage, RecordBatch? sources, MergeClausePlan clause, Snapshot.Snapshot snapshot)
    {
        RecordBatch? pairs = null;
        var fields = new List<Field>(preimage.ColumnCount);
        var columns = new IArrowArray[preimage.ColumnCount];
        for (int c = 0; c < preimage.ColumnCount; c++)
        {
            var field = preimage.Schema.FieldsList[c];
            if (!clause.Writes.TryGetValue(field.Name, out var value))
            {
                fields.Add(field);
                columns[c] = preimage.Column(c);
                continue;
            }
            columns[c] = value is null
                ? sources!.Column(sources.Schema.GetFieldIndex(field.Name))
                : AssignedValue(value, field.Name, pairs ??= MergeConditionBatch(preimage, sources), snapshot);
            fields.Add(new Field(field.Name, columns[c].Data.DataType, field.IsNullable, field.Metadata));
        }
        return new RecordBatch(new Apache.Arrow.Schema(fields, preimage.Schema.Metadata), columns, preimage.Length);
    }

    /// <summary>
    /// The rows <paramref name="clause"/> inserts for the source rows <paramref name="rows"/>: the source columns it
    /// copies, or the values it assigns, computed from the source row. The append fills in the columns it omits.
    /// </summary>
    private static RecordBatch MergeInsertBatch(RecordBatch rows, MergeClausePlan clause, Snapshot.Snapshot snapshot)
    {
        if (clause.Writes.Values.All(v => v is null))
            return ProjectColumns(rows, clause.WrittenColumns);

        var pairs = MergeConditionBatch(null, rows);
        var fields = new List<Field>(clause.Writes.Count);
        var columns = new List<IArrowArray>(clause.Writes.Count);
        foreach (string column in clause.WrittenColumns)
        {
            var value = clause.Writes[column];
            var array = value is null
                ? rows.Column(rows.Schema.GetFieldIndex(column))
                : AssignedValue(value, column, pairs, snapshot);
            fields.Add(new Field(column, array.Data.DataType, nullable: true));
            columns.Add(array);
        }
        return new RecordBatch(new Apache.Arrow.Schema(fields, null), columns, rows.Length);
    }

    /// <summary>
    /// <paramref name="value"/> evaluated over <paramref name="pairs"/> (a <see cref="MergeConditionBatch"/>) and
    /// converted to the type of the table column <paramref name="column"/>, as Spark's MERGE assigns it.
    /// </summary>
    /// <remarks>
    /// <para>Measured against Spark 4.0 / Delta 4.0, a MERGE assignment of another type is an ANSI <c>CAST</c> to
    /// the column's type, not the stricter store-assignment rule: <c>'42'</c> and <c>true</c> go into an
    /// <c>int</c> column, <c>1</c> and <c>'true'</c> into a <c>boolean</c> one, and a decimal is rounded half up.
    /// Malformed input is <c>CAST_INVALID_INPUT</c>, as the cast raises it; a value the column cannot hold is
    /// <c>DELTA_CAST_OVERFLOW_IN_TABLE_WRITE</c>, where a bare cast says <c>CAST_OVERFLOW</c>.</para>
    /// <para>A type the cast cannot target (<c>timestamp_ntz</c>, a struct, array or map) takes only a value of its
    /// own Delta type, in any Arrow form of it; anything else is refused as a mistyped column. NULL goes anywhere.</para>
    /// </remarks>
    private static IArrowArray AssignedValue(
        Expressions.Expression value, string column, RecordBatch pairs, Snapshot.Snapshot snapshot)
    {
        var target = snapshot.ArrowSchema.GetFieldByName(column)!;
        var array = RowEvaluator.EvaluateExpression(value, pairs);
        if (array is NullArray || array.Data.DataType.TypeId == Apache.Arrow.Types.ArrowTypeId.Null)
            return ArrowCompute.MakeNullArray(target.DataType, array.Length);

        if (!SameDeltaType(array.Data.DataType, target.DataType))
        {
            var declared = snapshot.Schema.Fields.First(f => string.Equals(f.Name, column, StringComparison.Ordinal));
            if (CastTypeName(declared.Type) is not { } castTo)
            {
                var supplied = new RecordBatch(
                    new Apache.Arrow.Schema([new Field(column, array.Data.DataType, true)], null), [array], array.Length);
                ThrowIfMistypedColumns([supplied], snapshot.Schema, "MERGE");
                throw new ArgumentException(
                    $"MERGE: the value assigned to column '{column}' is Arrow {array.Data.DataType.Name}, which "
                    + $"cannot be converted to the column's type {declared.Type}.");
            }

            const string valueName = "__ew_assigned";
            var cast = new Expressions.FunctionCall(
                "cast",
                [new Expressions.UnboundReference(valueName),
                 new Expressions.LiteralExpression(Expressions.LiteralValue.Of(castTo))]);
            var batch = new RecordBatch(
                new Apache.Arrow.Schema([new Field(valueName, array.Data.DataType, true)], null), [array], array.Length);
            try
            {
                array = RowEvaluator.EvaluateExpression(cast, batch);
            }
            catch (Expressions.Arrow.Spark.SparkEvaluationException e) when (
                e.ErrorClass is "CAST_OVERFLOW" || e.ErrorClass.StartsWith("NUMERIC_VALUE_OUT_OF_RANGE", StringComparison.Ordinal))
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.CastOverflowInTableWrite,
                    $"MERGE: failed to write a value of Arrow type {batch.Column(0).Data.DataType.Name} into the "
                    + $"{declared.Type} column '{column}' due to an overflow. {e.Message}",
                    e);
            }
        }

        // The table's own Arrow form of the type, as a copied row of the same file carries it.
        var conformed = ConformToTable(
            new RecordBatch(new Apache.Arrow.Schema([new Field(column, array.Data.DataType, true)], null), [array],
                array.Length),
            snapshot.ArrowSchema);
        return WriteTypeNormalization.Normalize(conformed).Column(0);
    }

    /// <summary>The <c>CAST</c> target the expression library spells <paramref name="type"/> as, or null for a type
    /// it cannot cast to.</summary>
    private static string? CastTypeName(DeltaDataType type) => type switch
    {
        PrimitiveType { TypeName: "byte" } => "TINYINT",
        PrimitiveType { TypeName: "short" } => "SMALLINT",
        PrimitiveType { TypeName: "integer" } => "INT",
        PrimitiveType { TypeName: "long" } => "BIGINT",
        PrimitiveType { TypeName: "float" } => "FLOAT",
        PrimitiveType { TypeName: "double" } => "DOUBLE",
        PrimitiveType { TypeName: "boolean" } => "BOOLEAN",
        PrimitiveType { TypeName: "string" } => "STRING",
        PrimitiveType { TypeName: "binary" } => "BINARY",
        PrimitiveType { TypeName: "date" } => "DATE",
        PrimitiveType { TypeName: "timestamp" } => "TIMESTAMP",
        PrimitiveType { TypeName: var name } when name.StartsWith("decimal(", StringComparison.Ordinal) =>
            name.ToUpperInvariant(),
        _ => null,
    };

    /// <summary>
    /// Refuses a clause condition or assigned value Spark's analyzer would refuse (<c>target.d = 5</c>, a column
    /// neither row has), before the target is scanned.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A condition or value is evaluated only over the rows that reach its clause, so with an empty source, no key
    /// that matches, or every file pruned by the key range it never runs, and the MERGE committed where Spark
    /// refuses at analysis (#589). Each is asked over an empty batch of the shape it is evaluated over -- the
    /// <see cref="MergeConditionBatch"/> of its clause -- which reads no value and so raises only what the
    /// types decide; see <see cref="RefuseUnanalyzable(Expressions.Predicate, Snapshot.Snapshot)"/>.
    /// </para>
    /// <para>
    /// A source with no batches has no schema of its own. Its columns can only be the table's, normalized to
    /// the table's types (<see cref="PrepareMergeSource"/>), so the table's schema stands in for it. Which of
    /// them it has is not known, though, so a matched clause may name any target column bare: whether the
    /// source shares it, which would hide the bare name, cannot be told.
    /// </para>
    /// </remarks>
    private static void RefuseUnanalyzableClauses(MergePlan plan, Snapshot.Snapshot snapshot, RecordBatch? src)
    {
        var targetSchema = snapshot.ArrowSchema;
        var sourceSchema = src?.Schema ?? targetSchema;

        var matchedNames = src is not null
            ? MergeConditionSchema(targetSchema, sourceSchema)
            : new Apache.Arrow.Schema(
                [SideField("source", sourceSchema), .. MergeConditionSchema(targetSchema, null).FieldsList], null);
        Refuse(plan.Matched, matchedNames, () => MergeConditionBatch(EmptyBatch(targetSchema), EmptyBatch(sourceSchema)));
        Refuse(plan.NotMatched, MergeConditionSchema(null, sourceSchema),
            () => MergeConditionBatch(null, EmptyBatch(sourceSchema)));
        Refuse(plan.NotMatchedBySource, MergeConditionSchema(targetSchema, null),
            () => MergeConditionBatch(EmptyBatch(targetSchema), null));

        void Refuse(IReadOnlyList<MergeClausePlan> clauses, Apache.Arrow.Schema names, Func<RecordBatch> noRows)
        {
            foreach (var clause in clauses)
            {
                if (clause.Condition is { } condition)
                    RefuseUnanalyzable(condition, names, noRows);
                foreach (var entry in clause.Writes)
                {
                    if (entry.Value is { } value)
                        RefuseUnanalyzableValue(entry.Key, value, names, noRows, snapshot);
                }
            }
        }
    }

    /// <summary>
    /// <see cref="RefuseUnanalyzableClauses"/> for one assigned value: a reference neither of its rows has is
    /// <c>DELTA_MERGE_UNRESOLVED_EXPRESSION</c>, as Spark names it (an insert reading the target row, a
    /// not-matched-by-source update reading the source row); then its types, with the conversion to the column, are
    /// asked over no rows.
    /// </summary>
    private static void RefuseUnanalyzableValue(
        string column, Expressions.Expression value, Apache.Arrow.Schema names, Func<RecordBatch> noRows,
        Snapshot.Snapshot snapshot)
    {
        foreach (var (name, resolution) in ReferenceResolutions(value, names))
        {
            if (resolution == ReferenceResolution.Missing)
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.MergeUnresolvedExpression,
                    $"MERGE: the value {value} assigned to column '{column}' reads '{name}', which this clause cannot "
                    + "see. It can read: " + string.Join(", ", names.FieldsList.Select(f => f.Name)) + ".");
            }
            if (resolution == ReferenceResolution.Ambiguous)
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.AmbiguousReference,
                    $"MERGE: the value {value} assigned to column '{column}' reads '{name}', which is ambiguous: "
                    + "column names are compared ignoring case, and more than one field matches it.");
            }
        }

        RefuseUntypableLiterals(value);
        try
        {
            AssignedValue(value, column, noRows(), snapshot);
        }
        catch (Expressions.Arrow.ExpressionAnalysisException)
        {
            throw;
        }
        catch (ArgumentException e) when (e.Message.StartsWith("MERGE:", StringComparison.Ordinal))
        {
            throw; // a value of a type the column cannot take
        }
        catch (Exception)
        {
            // Not an analysis refusal; the rows decide.
        }
    }

    /// <summary>The schema of the <see cref="MergeConditionBatch"/> of these sides, built without any array.</summary>
    private static Apache.Arrow.Schema MergeConditionSchema(Apache.Arrow.Schema? target, Apache.Arrow.Schema? source) =>
        new(MergeConditionColumns(target, source).Select(c => c.Field), null);

    /// <summary>
    /// The columns of a <see cref="MergeConditionBatch"/>: each side as a struct named for it, then each column only
    /// one side has under its bare name. <c>Side</c> is 0 for the target, 1 for the source; <c>Column</c> is the
    /// side's column, or -1 for the side's struct.
    /// </summary>
    private static List<(Field Field, int Side, int Column)> MergeConditionColumns(
        Apache.Arrow.Schema? target, Apache.Arrow.Schema? source)
    {
        var columns = new List<(Field, int, int)>();
        if (target is not null)
            columns.Add((SideField("target", target), 0, -1));
        if (source is not null)
            columns.Add((SideField("source", source), 1, -1));

        var targetNames = target?.FieldsList.Select(f => f.Name) ?? [];
        var sourceNames = source?.FieldsList.Select(f => f.Name) ?? [];
        var shared = new HashSet<string>(targetNames.Intersect(sourceNames, StringComparer.OrdinalIgnoreCase),
            StringComparer.OrdinalIgnoreCase) { "target", "source" };
        var sides = new[] { target, source };
        for (int s = 0; s < sides.Length; s++)
        {
            if (sides[s] is not { } side)
                continue;
            for (int c = 0; c < side.FieldsList.Count; c++)
            {
                var field = side.FieldsList[c];
                if (!shared.Contains(field.Name))
                    columns.Add((field, s, c));
            }
        }
        return columns;
    }

    private static Field SideField(string name, Apache.Arrow.Schema side) =>
        new(name, new Apache.Arrow.Types.StructType(side.FieldsList.ToList()), nullable: false);

    /// <summary>
    /// The batch a MERGE condition is evaluated over: the target row's columns under a struct <c>target</c>, the
    /// source row's under <c>source</c>, and each column only one side has under its bare name too.
    /// </summary>
    private static RecordBatch MergeConditionBatch(RecordBatch? target, RecordBatch? source)
    {
        int length = target?.Length ?? source!.Length;
        var sides = new[] { target, source };
        var shape = MergeConditionColumns(target?.Schema, source?.Schema);
        var columns = new List<IArrowArray>(shape.Count);
        foreach (var (field, s, c) in shape)
        {
            var side = sides[s]!;
            columns.Add(c >= 0
                ? side.Column(c)
                : new StructArray(
                    (Apache.Arrow.Types.StructType)field.DataType, side.Length,
                    Enumerable.Range(0, side.ColumnCount).Select(i => side.Column(i)).ToList(),
                    ArrowBuffer.Empty, nullCount: 0));
        }
        return new RecordBatch(new Apache.Arrow.Schema(shape.Select(x => x.Field), null), columns, length);
    }

    /// <summary>
    /// <c>min(k) &lt;= k AND k &lt;= max(k)</c> over the source's non-null values of each key whose type the
    /// file statistics can bound here: <c>byte</c>, <c>short</c>, <c>integer</c>, <c>long</c>, <c>date</c>,
    /// <c>timestamp</c>, <c>timestamp_ntz</c>, <c>decimal</c> and <c>string</c>. Every target row a source row can
    /// match satisfies it, so a file whose statistics refute it holds no match. Null when no key can be bounded.
    /// </summary>
    /// <remarks>
    /// The keys arrive in the table's Arrow form (<see cref="PrepareMergeSource"/>). The extremes are found on the
    /// raw values, which order as the Delta type does, and only those two rows become literals, through the
    /// decoder a checkpoint's typed <c>stats_parsed</c> goes through: a bound has the literal kind (and a decimal
    /// the scale) the file statistics decode to, so they compare exactly. A form the decoder does not take
    /// leaves the key unbounded. A truncated timestamp maximum in the statistics is widened by the pruner.
    /// </remarks>
    private static Expressions.Predicate? KeyRangePredicate(
        IReadOnlyList<string> keys, IArrowArray[] sourceKeys, Schema.StructType schema)
    {
        var bounds = new List<Expressions.Predicate>();
        for (int k = 0; k < keys.Count; k++)
        {
            string? typeName = schema.Fields
                .FirstOrDefault(f => string.Equals(f.Name, keys[k], StringComparison.Ordinal))?.Type is PrimitiveType p
                ? p.TypeName
                : null;
            (Expressions.LiteralValue Min, Expressions.LiteralValue Max)? range = sourceKeys[k] switch
            {
                StringArray a => StringRange(a),
                var a when typeName is not null && Extremes(a) is { } at
                    && DeltaLiteralDecoder.FromArrow(a, at.Min, typeName) is { } min
                    && DeltaLiteralDecoder.FromArrow(a, at.Max, typeName) is { } max => (min, max),
                _ => null,
            };
            if (range is not { } bound)
                continue;
            bounds.Add(Expressions.Expressions.GreaterThanOrEqual(keys[k], bound.Min));
            bounds.Add(Expressions.Expressions.LessThanOrEqual(keys[k], bound.Max));
        }
        return bounds.Count == 0 ? null : Expressions.Expressions.And(bounds.ToArray());

        // The rows holding the least and the greatest non-null value, for a form whose raw values order as its
        // Delta type does; null for any other form, or when every value is null. A timestamp in another unit
        // than the table's microseconds would decode wrongly, so it is not bounded.
        static (int Min, int Max)? Extremes(IArrowArray array) => array switch
        {
            Int8Array a => PrimitiveExtremes(a),
            Int16Array a => PrimitiveExtremes(a),
            Int32Array a => PrimitiveExtremes(a),
            Int64Array a => PrimitiveExtremes(a),
            Date32Array a => PrimitiveExtremes(a),
            TimestampArray a when ((Apache.Arrow.Types.TimestampType)a.Data.DataType).Unit
                == Apache.Arrow.Types.TimeUnit.Microsecond => PrimitiveExtremes(a),
            Decimal32Array a => DecimalExtremes(a, a.ValueBuffer, 4),
            Decimal64Array a => DecimalExtremes(a, a.ValueBuffer, 8),
            Decimal128Array a => DecimalExtremes(a, a.ValueBuffer, 16),
            Decimal256Array a => DecimalExtremes(a, a.ValueBuffer, 32),
            _ => null,
        };

        static (int Min, int Max)? PrimitiveExtremes<T>(PrimitiveArray<T> a)
            where T : struct, IEquatable<T>, IComparable<T>
        {
            var values = a.Values;
            int min = -1, max = -1;
            for (int i = 0; i < a.Length; i++)
            {
                if (a.IsNull(i))
                    continue;
                if (min < 0 || values[i].CompareTo(values[min]) < 0)
                    min = i;
                if (max < 0 || values[i].CompareTo(values[max]) > 0)
                    max = i;
            }
            return min < 0 ? null : (min, max);
        }

        // Unscaled two's-complement little-endian values of one width; all of one scale, the table's.
        static (int Min, int Max)? DecimalExtremes(IArrowArray a, ArrowBuffer buffer, int width)
        {
            var data = buffer.Span.Slice(a.Data.Offset * width, a.Length * width);
            int min = -1, max = -1;
            for (int i = 0; i < a.Length; i++)
            {
                if (a.IsNull(i))
                    continue;
                var value = data.Slice(i * width, width);
                if (min < 0 || CompareSigned(value, data.Slice(min * width, width)) < 0)
                    min = i;
                if (max < 0 || CompareSigned(value, data.Slice(max * width, width)) > 0)
                    max = i;
            }
            return min < 0 ? null : (min, max);
        }

        // The most significant word decides the sign, so it compares signed and the lower words unsigned.
        static int CompareSigned(ReadOnlySpan<byte> x, ReadOnlySpan<byte> y)
        {
            if (x.Length == 4)
            {
                return System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(x)
                    .CompareTo(System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(y));
            }
            int top = x.Length - 8;
            int c = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(x.Slice(top))
                .CompareTo(System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(y.Slice(top)));
            for (int w = top - 8; c == 0 && w >= 0; w -= 8)
            {
                c = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(x.Slice(w, 8))
                    .CompareTo(System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(y.Slice(w, 8)));
            }
            return c;
        }

        // Ordered as UTF-8 bytes, as the statistics are: for well-formed UTF-8 that is code point order. An
        // ill-formed key leaves the key unbounded (#663). It decodes to U+FFFD, which can sort on the other side
        // of a bound than its bytes do, and checking only the winners is not enough. Keys "😀" and FF give the
        // empty range "😀"..U+FFFD; keys "a", "é", 80 give "a".."é", which refutes a target file holding 80, whose
        // statistics are the decoded U+FFFD. Either way a file holding a match would be pruned.
        static (Expressions.LiteralValue, Expressions.LiteralValue)? StringRange(StringArray a)
        {
            if (!Stats.StatsCollector.AllCellsWellFormed(a.ValueBuffer.Span, a.ValueOffsets, a.Length))
                return null;

            int min = -1, max = -1;
            for (int i = 0; i < a.Length; i++)
            {
                if (a.IsNull(i))
                    continue;
                if (min < 0 || a.GetBytes(i).SequenceCompareTo(a.GetBytes(min)) < 0)
                    min = i;
                if (max < 0 || a.GetBytes(i).SequenceCompareTo(a.GetBytes(max)) > 0)
                    max = i;
            }
            return min < 0
                ? null
                : (Expressions.LiteralValue.Of(a.GetString(min)), Expressions.LiteralValue.Of(a.GetString(max)));
        }
    }

    private static void ThrowIfMergeSetsIdentity(IReadOnlyList<string> setColumns, Schema.StructType schema)
    {
        foreach (string column in setColumns)
        {
            var field = schema.Fields.First(f => string.Equals(f.Name, column, StringComparison.Ordinal));
            if (IdentityColumn.GetConfig(field) is not null)
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.IdentityColumnUpdateNotSupported,
                    $"MERGE: UPDATE on IDENTITY column {column} is not supported; the table assigns its values. Set "
                    + "the other columns with MergeAction.UpdateColumns, or drop the column from the source.");
            }
        }
    }

    private static DeltaFormatException MultipleSourceRowsMatched(string path) => new(
        DeltaTableErrorCodes.MultipleSourceRowsMatchingTargetRow,
        $"MERGE: more than one source row has the same key as a target row in '{path}', so it is undefined which "
        + "source row's action applies. As in Spark, this is refused whatever the clause conditions say: make the source's "
        + "keys unique. Only a MERGE whose sole matched clause is an unconditional DELETE, or one that only inserts, "
        + "may match a row more than once.");

    /// <summary>One clause of a MERGE, checked against the table.</summary>
    private sealed class MergeClausePlan
    {
        public MergeClausePlan(MergeAction action) => Action = action;

        public MergeAction Action { get; }
        public MergeActionKind Kind => Action.Kind;
        public Expressions.Predicate? Condition => Action.Condition;

        /// <summary>The columns an update or insert writes, by the table's names, each with its value: null to copy
        /// the source column of that name, else the expression assigned. Empty for a delete. Set by
        /// <see cref="MergePlan.Resolve"/>.</summary>
        public IReadOnlyDictionary<string, Expressions.Expression?> Writes { get; set; } =
            new Dictionary<string, Expressions.Expression?>(StringComparer.Ordinal);

        /// <summary>The keys of <see cref="Writes"/>, in the order the action names them.</summary>
        public IReadOnlyList<string> WrittenColumns { get; set; } = [];
    }

    /// <summary>The MERGE options, checked against the table.</summary>
    private sealed class MergePlan
    {
        private MergePlan(
            IReadOnlyList<string> keys, IReadOnlyList<MergeClausePlan> matched,
            IReadOnlyList<MergeClausePlan> notMatched, IReadOnlyList<MergeClausePlan> notMatchedBySource)
        {
            Keys = keys;
            Matched = matched;
            NotMatched = notMatched;
            NotMatchedBySource = notMatchedBySource;
        }

        public IReadOnlyList<string> Keys { get; }
        public IReadOnlyList<MergeClausePlan> Matched { get; }
        public IReadOnlyList<MergeClausePlan> NotMatched { get; }
        public IReadOnlyList<MergeClausePlan> NotMatchedBySource { get; }

        /// <summary>Every column some update writes. Set by <see cref="Resolve"/>.</summary>
        public IReadOnlyList<string> UpdatedColumns { get; private set; } = [];

        /// <summary>Every column some insert writes. Set by <see cref="Resolve"/>.</summary>
        public IReadOnlyList<string> InsertedColumns { get; private set; } = [];

        /// <summary>
        /// Settles the columns each update and insert writes, now that the source's columns are known (null for a
        /// source with no batches).
        /// </summary>
        public void Resolve(RecordBatch? source, Schema.StructType schema)
        {
            foreach (var clause in Matched.Concat(NotMatchedBySource))
                ResolveClause(clause, source, schema, "the update sets", "UPDATE SET *", "MergeAction.UpdateColumns");
            foreach (var clause in NotMatched)
                ResolveClause(clause, source, schema, "the insert writes", "INSERT *", "MergeAction.InsertColumns");
            UpdatedColumns = Matched.Concat(NotMatchedBySource).SelectMany(c => c.WrittenColumns)
                .Distinct(StringComparer.Ordinal).ToList();
            InsertedColumns = NotMatched.SelectMany(c => c.WrittenColumns).Distinct(StringComparer.Ordinal).ToList();
        }

        // A `*` action takes every table column from the source, and Spark refuses one whose source lacks any
        // (DELTA_MERGE_UNRESOLVED_EXPRESSION), generated and identity columns included; a column list names its own.
        // An assignment names a table column ignoring case, as Spark resolves it, and is recorded under the
        // table's name for it.
        private static void ResolveClause(
            MergeClausePlan clause, RecordBatch? source, Schema.StructType schema, string verb, string star,
            string listed)
        {
            if (clause.Kind == MergeActionKind.Delete)
                return;

            var writes = new Dictionary<string, Expressions.Expression?>(StringComparer.Ordinal);
            var order = new List<string>();
            if (clause.Action.Assignments is { } assignments)
            {
                foreach (var assignment in assignments)
                {
                    string column = TableColumn(assignment.Column, schema, verb);
                    writes[column] = assignment.Value;
                    order.Add(column);
                }
            }
            else
            {
                var columns = clause.Action.Columns ?? schema.Fields.Select(f => f.Name).ToList();
                if (source is not null)
                {
                    var missing = columns.Where(c => source.Schema.GetFieldIndex(c) < 0).ToList();
                    if (missing.Count > 0)
                    {
                        throw new ArgumentException(
                            clause.Action.Columns is null
                                ? $"MERGE: {star} takes every column of the table from the source, which has no "
                                  + $"{string.Join(", ", missing.Select(c => "'" + c + "'"))}. Name the columns to "
                                  + $"take with {listed}, or add the missing ones to the source."
                                : $"MERGE: {verb} '{missing[0]}', which the source does not carry.",
                            "source");
                    }
                }
                foreach (string column in columns)
                {
                    writes[column] = null;
                    order.Add(column);
                }
            }
            clause.Writes = writes;
            clause.WrittenColumns = order;
        }

        private static string TableColumn(string name, Schema.StructType schema, string verb)
        {
            var exact = schema.Fields.FirstOrDefault(f => string.Equals(f.Name, name, StringComparison.Ordinal));
            if (exact is not null)
                return exact.Name;
            var folded = schema.Fields.Where(f => string.Equals(f.Name, name, StringComparison.OrdinalIgnoreCase))
                .ToList();
            if (folded.Count == 1)
                return folded[0].Name;
            if (folded.Count > 1)
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.AmbiguousReference,
                    $"MERGE: {verb} '{name}', which is ambiguous: column names are compared ignoring case, and more "
                    + "than one column of the table matches it.");
            }
            throw new DeltaFormatException(
                DeltaTableErrorCodes.ColumnNotFound,
                $"MERGE: {verb} '{name}', which is not a column of the table.");
        }

        public static MergePlan Validate(MergeOptions options, Schema.StructType schema)
        {
            if (options.Keys is not { Count: > 0 } keys)
                throw new ArgumentException("MERGE needs at least one key column.", nameof(options));
            if (keys.Distinct(StringComparer.Ordinal).Count() != keys.Count)
                throw new ArgumentException("MERGE: a key column is named twice.", nameof(options));
            foreach (string key in keys)
            {
                var field = schema.Fields.FirstOrDefault(f => string.Equals(f.Name, key, StringComparison.Ordinal))
                    ?? throw new DeltaFormatException(
                        DeltaTableErrorCodes.ColumnNotFound,
                        $"MERGE: key '{key}' is not a column of the table.");
                if (field.Type is not PrimitiveType { TypeName: not "variant" })
                {
                    throw new ArgumentException(
                        $"MERGE: key '{key}' is not a primitive column, so it cannot be compared for equality.",
                        nameof(options));
                }
            }

            var matched = Check(options.WhenMatched, "WhenMatched", MergeActionKind.Update, MergeActionKind.Delete);
            var notMatched = Check(options.WhenNotMatched, "WhenNotMatched", MergeActionKind.Insert);
            var sourceless = Check(
                options.WhenNotMatchedBySource, "WhenNotMatchedBySource", MergeActionKind.Update, MergeActionKind.Delete);
            if (matched.Count == 0 && notMatched.Count == 0 && sourceless.Count == 0)
                throw new ArgumentException("MERGE needs at least one clause.", nameof(options));

            foreach (var clause in sourceless)
            {
                // There is no source row to copy from.
                if (clause.Kind == MergeActionKind.Update && clause.Action.Assignments is null)
                {
                    throw new ArgumentException(
                        $"MERGE: WhenNotMatchedBySource cannot be {clause.Action}: a target row no source row matches "
                        + "has no source columns to copy. Assign values with MergeAction.UpdateSet.",
                        nameof(options));
                }
            }

            foreach (var (clause, verb) in matched.Select(c => (c, "the update sets"))
                .Concat(notMatched.Select(c => (c, "the insert writes"))))
            {
                foreach (string column in clause.Action.Columns ?? [])
                {
                    if (!schema.Fields.Any(f => string.Equals(f.Name, column, StringComparison.Ordinal)))
                    {
                        throw new DeltaFormatException(
                            DeltaTableErrorCodes.ColumnNotFound,
                            $"MERGE: {verb} '{column}', which is not a column of the table.");
                    }
                }
            }

            return new MergePlan(keys, matched, notMatched, sourceless);

            // As in Spark, only the last clause of a kind may be unconditional: one before it would take every row
            // and leave the clauses after it unreachable.
            static List<MergeClausePlan> Check(
                IReadOnlyList<MergeAction>? actions, string clause, params MergeActionKind[] allowed)
            {
                if (actions is null)
                    throw new ArgumentException($"MERGE: {clause} is null; leave it empty for no clauses.", nameof(options));
                var plans = new List<MergeClausePlan>(actions.Count);
                for (int i = 0; i < actions.Count; i++)
                {
                    var action = actions[i] ?? throw new ArgumentException(
                        $"MERGE: {clause}[{i}] is null.", nameof(options));
                    if (!allowed.Contains(action.Kind))
                    {
                        throw new ArgumentException(
                            $"MERGE: {clause} cannot be {action}; it takes {string.Join(" or ", allowed)}.",
                            nameof(options));
                    }
                    if (action.Condition is null && i < actions.Count - 1)
                    {
                        throw new ArgumentException(
                            $"MERGE: {clause}[{i}] ({action}) has no condition, so the clauses after it could never "
                            + "apply. Only the last clause of a kind may be unconditional.",
                            nameof(options));
                    }
                    plans.Add(new MergeClausePlan(action));
                }
                return plans;
            }
        }
    }
}

/// <summary>A MERGE key as bytes: each key value length-prefixed, so equal keys and only those encode equally.</summary>
internal static class MergeKeys
{
    /// <summary>Row <paramref name="row"/>'s key across <paramref name="columns"/>, or null when any part is NULL
    /// (a NULL key matches nothing). Both sides of a MERGE hold each key in the same Delta type, but not always in
    /// the same Arrow type: a source may supply a <c>timestamp</c> in milliseconds against the table's
    /// microseconds, or (under a host's codec) a decimal or integer at another width. So each value is encoded in
    /// one form per Delta type: integers as 64 bits, dates as milliseconds, timestamps as microseconds plus any
    /// nanoseconds, decimals by value whatever their width and scale. Floating-point zeros and NaNs are canonicalized too,
    /// since SQL takes <c>-0.0 = 0.0</c> and Spark <c>NaN = NaN</c>.</summary>
    public static byte[]? Encode(IArrowArray[] columns, int row)
    {
        Span<byte> scratch = stackalloc byte[32];
        int length = 0;
        foreach (var column in columns)
        {
            if (column.IsNull(row))
                return null;
            length += 4 + Value(column, row, scratch).Length;
        }

        var key = new byte[length];
        int at = 0;
        foreach (var column in columns)
        {
            var value = Value(column, row, scratch);
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(key.AsSpan(at), value.Length);
            value.CopyTo(key.AsSpan(at + 4));
            at += 4 + value.Length;
        }
        return key;
    }

    private static ReadOnlySpan<byte> Value(IArrowArray column, int row, Span<byte> scratch)
    {
        switch (column)
        {
            case Int8Array i8:
                return Int64(i8.GetValue(row)!.Value, scratch);
            case Int16Array i16:
                return Int64(i16.GetValue(row)!.Value, scratch);
            case Int32Array i32:
                return Int64(i32.GetValue(row)!.Value, scratch);
            case Int64Array i64:
                return Int64(i64.GetValue(row)!.Value, scratch);
            case Date32Array d32:
                return Int64(d32.GetValue(row)!.Value * 86_400_000L, scratch);
            case Date64Array d64:
                return Int64(d64.GetValue(row)!.Value, scratch);
            case TimestampArray ts:
            {
                long value = ts.GetValue(row)!.Value;
                long micros, nanos = 0;
                try
                {
                    switch (((Apache.Arrow.Types.TimestampType)ts.Data.DataType).Unit)
                    {
                        case Apache.Arrow.Types.TimeUnit.Second: micros = checked(value * 1_000_000); break;
                        case Apache.Arrow.Types.TimeUnit.Millisecond: micros = checked(value * 1_000); break;
                        case Apache.Arrow.Types.TimeUnit.Microsecond: micros = value; break;
                        default:
                            micros = value / 1_000;
                            nanos = value % 1_000;
                            if (nanos < 0)
                            {
                                micros--;
                                nanos += 1_000;
                            }
                            break;
                    }
                }
                catch (OverflowException)
                {
                    throw new ArgumentException(
                        $"MERGE: the key timestamp {value} ({ts.Data.DataType.Name}) is outside the range of "
                        + "a Delta timestamp.");
                }
                System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(scratch, micros);
                System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(scratch.Slice(8), nanos);
                return scratch.Slice(0, 16);
            }
            case Apache.Arrow.Arrays.FixedSizeBinaryArray dec when dec.Data.DataType is
                Apache.Arrow.Types.Decimal32Type or Apache.Arrow.Types.Decimal64Type
                or Apache.Arrow.Types.Decimal128Type or Apache.Arrow.Types.Decimal256Type:
            {
                // By value, whatever the width and scale: the unscaled integer with its trailing zeros moved into
                // the scale, so 1.00 at scale 2 and 1.000 at scale 3 are both (1, 0). (The bytes are little-endian
                // two's complement at the type's width, as BigInteger reads them.)
                int scale = dec.Data.DataType switch
                {
                    Apache.Arrow.Types.Decimal32Type t => t.Scale,
                    Apache.Arrow.Types.Decimal64Type t => t.Scale,
                    Apache.Arrow.Types.Decimal128Type t => t.Scale,
                    Apache.Arrow.Types.Decimal256Type t => t.Scale,
                    _ => 0,
                };
                var value = new System.Numerics.BigInteger(dec.GetBytes(row).ToArray());
                if (value.IsZero)
                    scale = 0;
                while (scale > 0)
                {
                    var quotient = System.Numerics.BigInteger.DivRem(value, 10, out var remainder);
                    if (!remainder.IsZero)
                        break;
                    value = quotient;
                    scale--;
                }
                byte[] unscaled = value.ToByteArray();
                var encoded = new byte[4 + unscaled.Length];
                System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(encoded, scale);
                unscaled.CopyTo(encoded, 4);
                return encoded;
            }
            case BooleanArray b:
                scratch[0] = b.GetValue(row) == true ? (byte)1 : (byte)0;
                return scratch.Slice(0, 1);
            case FloatArray f:
            {
                float v = f.GetValue(row)!.Value;
                v = float.IsNaN(v) ? float.NaN : v == 0f ? 0f : v;
                // Widened, exactly: netstandard2.0 has no float-to-bits conversion.
                System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(scratch, BitConverter.DoubleToInt64Bits(v));
                return scratch.Slice(0, 8);
            }
            case DoubleArray d:
            {
                double v = d.GetValue(row)!.Value;
                v = double.IsNaN(v) ? double.NaN : v == 0d ? 0d : v;
                System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(scratch, BitConverter.DoubleToInt64Bits(v));
                return scratch.Slice(0, 8);
            }
            case BinaryArray binary: // and StringArray
                return binary.GetBytes(row);
            case LargeBinaryArray large: // and LargeStringArray
                return large.GetBytes(row);
            case BinaryViewArray view: // and StringViewArray
                return view.GetBytes(row);
            case Apache.Arrow.Arrays.FixedSizeBinaryArray fixedSize: // and the decimals
                return fixedSize.GetBytes(row);
        }

        if (column.Data.DataType is Apache.Arrow.Types.FixedWidthType { BitWidth: >= 8 } fixedWidth)
        {
            int width = fixedWidth.BitWidth / 8;
            return column.Data.Buffers[1].Span.Slice((column.Data.Offset + row) * width, width);
        }
        throw new NotSupportedException(
            $"MERGE cannot compare keys of Arrow type {column.Data.DataType.Name}.");
    }

    private static ReadOnlySpan<byte> Int64(long value, Span<byte> scratch)
    {
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(scratch, value);
        return scratch.Slice(0, 8);
    }
}

/// <summary>Compares MERGE keys by content.</summary>
internal sealed class MergeKeyComparer : IEqualityComparer<byte[]>
{
    public static readonly MergeKeyComparer Instance = new();

    public bool Equals(byte[]? x, byte[]? y) =>
        ReferenceEquals(x, y) || (x is not null && y is not null && x.AsSpan().SequenceEqual(y));

    public int GetHashCode(byte[] key)
    {
        unchecked
        {
            uint hash = 2166136261;
            foreach (byte b in key)
                hash = (hash ^ b) * 16777619;
            return (int)hash;
        }
    }
}

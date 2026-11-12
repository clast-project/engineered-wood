// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Arrow;
using EngineeredWood.Parquet.Schema;
using EngineeredWood.Scans;

namespace EngineeredWood.Parquet;

public sealed partial class ParquetFileReader
{
    /// <summary>
    /// Streams exactly the rows <paramref name="scan"/> selects: those pruning keeps, that are not
    /// <see cref="ParquetScan.ExcludedRows"/>, and that every predicate of <see cref="ParquetScan.RowFilter"/>
    /// answers TRUE for. Each batch carries the file position of every row.
    /// </summary>
    /// <remarks>
    /// <para>Pruning is as <see cref="ReadWithPositionsAsync(EngineeredWood.Expressions.Predicate?, IReadOnlyList{string}?, RowSkipPredicate?, CancellationToken)"/>
    /// prunes, by <see cref="ParquetScan.Prune"/>, and row groups or runs of rows wholly excluded are skipped
    /// unread. Within the rows read, excluded rows are removed first, then the predicates are applied in order
    /// (<see cref="RowFilter"/>), and the remaining rows are gathered once.</para>
    /// <para>The filtering runs where the batch was decoded, so row groups read in parallel are filtered in
    /// parallel too. Batches come in file order; a batch left with no rows is not returned. A batch's rows need
    /// not be contiguous in the file (<see cref="RowPositions"/>).</para>
    /// <para>The caller disposes each batch (<see cref="SelectedRecordBatch.Dispose"/>).</para>
    /// </remarks>
    /// <exception cref="ArgumentException">A predicate names a column that is not a top-level column of the
    /// file, or one <see cref="ParquetScan.Columns"/> projects only in part.</exception>
    public IAsyncEnumerable<SelectedRecordBatch> ReadSelectedAsync(
        ParquetScan scan, CancellationToken cancellationToken = default)
    {
        if (scan is null)
            throw new ArgumentNullException(nameof(scan));
        return ReadSelectedCoreAsync(scan, cancellationToken);
    }

    private async IAsyncEnumerable<SelectedRecordBatch> ReadSelectedCoreAsync(
        ParquetScan scan,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        var selected = await PrepareSelectedAsync(scan, cancellationToken).ConfigureAwait(false);
        var plans = PlanRowGroupsAsync(
            metadata, scan.Prune ?? _options.Filter, selected.ReadColumns, selected.SkipRows, cancellationToken);

        int concurrency = Math.Min(EffectiveConcurrentRowGroups, metadata.RowGroups.Count);
        if (concurrency <= 1 || metadata.RowGroups.Count <= 1)
        {
            await foreach (var plan in plans.ConfigureAwait(false))
            {
                await foreach (var batch in selected.ReadAsync(plan, cancellationToken).ConfigureAwait(false))
                    yield return batch;
            }

            yield break;
        }

        // As ReadPositionedAsync, with the filtering inside each row group's producer: that is the point of
        // doing it in the reader, since the consumer's thread is where a filter applied afterwards runs.
        var budget = new ScanMemoryBudget(_options.MaxBufferedReadBytes, _options.MemoryBudget);
        LastScanBudget = budget;
        var growth = new DecodedGrowth();
        await foreach (var batch in OrderedScan.RunAsync(
                           plans,
                           (plan, reservation, token) => selected.ReadAsync(plan, reservation, growth, token),
                           // The positions count too: a positions-only read (no columns) holds nothing else.
                           SelectedBytes,
                           static b => b.Dispose(),
                           concurrency,
                           budget,
                           cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            yield return batch;
        }
    }

    /// <summary>
    /// The smallest share of a batch's rows still live at which a predicate that can answer every row without raising is
    /// asked about the whole batch rather than a gather of the live rows (#502 phase 5a); above 1, never. For tests and
    /// the A/B sweep.
    /// </summary>
    internal double WholeBatchLiveShare { get; set; } = BatchSelector.DefaultWholeBatchLiveShare;

    /// <summary>
    /// Whether adjacent value-wise predicates over one column are answered from its dictionary together (#502 phase 5b);
    /// off, a column two predicates read is asked row by row, as before. For tests and the A/B.
    /// </summary>
    internal bool FusesAnswers { get; set; } = true;

    /// <summary>
    /// Called for each row group a selected read reads with a predicate dropped on its statistics proof (#502 phase 5c):
    /// for tests, which otherwise could not tell a proof that holds from one that never does.
    /// </summary>
    internal Action? OnProvenRowGroup { get; set; }

    /// <summary>
    /// Everything a selected read of <paramref name="scan"/> decides once, before any row group: the columns it
    /// decodes, how it selects rows, whether it may go in two passes. A caller that schedules row groups itself (a
    /// Delta scan, which plans them with <see cref="PlanRowGroupsAsync(EngineeredWood.Expressions.Predicate?, IReadOnlyList{string}?, RowSkipPredicate?, CancellationToken, int, int)"/>)
    /// plans with <see cref="SelectedPlanReader.ReadColumns"/> and <see cref="SelectedPlanReader.SkipRows"/> and reads
    /// each plan with <see cref="SelectedPlanReader.ReadAsync(RowGroupPlan, CancellationToken)"/>, which may run
    /// concurrently for different plans.
    /// </summary>
    internal async ValueTask<SelectedPlanReader> PrepareSelectedAsync(ParquetScan scan, CancellationToken cancellationToken)
    {
        if (scan is null)
            throw new ArgumentNullException(nameof(scan));
        var metadata = await ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var (readColumns, dropped) = ColumnsForSelection(schema, scan.Columns, scan.RowFilter);
        var selector = new BatchSelector(
            scan.RowFilter, scan.ExcludedRows, dropped,
            new HashSet<string>(schema.Root.Children.Select(c => c.Name), StringComparer.Ordinal),
            WholeBatchLiveShare,
            FusesAnswers);
        var twoPass = TwoPassColumnsFor(schema, readColumns, dropped, scan.RowFilter, _options);
        RowSkipPredicate? skipRows = scan.ExcludedRows is { } excluded ? excluded.ExcludesAll : null;
        return new SelectedPlanReader(this, metadata, schema, scan, readColumns, selector, twoPass, skipRows);
    }

    /// <summary>
    /// How one row group is read: the columns, the selector and the two-pass split. A scan's own, or one for the
    /// predicates its statistics leave to ask (#502 phase 5c).
    /// </summary>
    private sealed record GroupRead(IReadOnlyList<string>? ReadColumns, BatchSelector Selector, TwoPassColumns? TwoPass);

    /// <summary>
    /// The read of a row group whose statistics prove the predicates <paramref name="proven"/> (a bit per predicate, by
    /// position) TRUE for every row: those predicates are dropped, and so are the columns only they read.
    /// </summary>
    private GroupRead ReadWithout(SchemaDescriptor schema, ParquetScan scan, ulong proven)
    {
        var remaining = new List<RowPredicate>();
        var predicates = scan.RowFilter!.Predicates;
        for (int i = 0; i < predicates.Count; i++)
        {
            if ((proven & (1UL << i)) == 0)
                remaining.Add(predicates[i]);
        }

        var filter = remaining.Count == 0 ? null : new RowFilter(remaining);
        var (readColumns, dropped) = ColumnsForSelection(schema, scan.Columns, filter);
        var selector = new BatchSelector(
            filter, scan.ExcludedRows, dropped,
            new HashSet<string>(schema.Root.Children.Select(c => c.Name), StringComparer.Ordinal),
            WholeBatchLiveShare,
            FusesAnswers);
        return new GroupRead(readColumns, selector, TwoPassColumnsFor(schema, readColumns, dropped, filter, _options));
    }

    /// <summary>A selected read prepared by <see cref="PrepareSelectedAsync"/>: reads its planned row groups.</summary>
    internal sealed class SelectedPlanReader
    {
        private readonly ParquetFileReader _reader;
        private readonly Metadata.FileMetaData _metadata;
        private readonly SchemaDescriptor _schema;
        private readonly ParquetScan _scan;
        private readonly GroupRead _whole;

        // The predicates with a proof, by position (at most 64, so a set of them is one word), and the reads already
        // built for each set a row group proved.
        private readonly (int Index, EngineeredWood.Expressions.Predicate Proof)[] _proofs;
        private readonly ParquetStatisticsAccessor? _accessor;
        private readonly System.Collections.Concurrent.ConcurrentDictionary<ulong, GroupRead> _without = new();

        internal SelectedPlanReader(
            ParquetFileReader reader, Metadata.FileMetaData metadata, SchemaDescriptor schema, ParquetScan scan,
            IReadOnlyList<string>? readColumns, BatchSelector selector, TwoPassColumns? twoPass, RowSkipPredicate? skipRows)
        {
            _reader = reader;
            _metadata = metadata;
            _schema = schema;
            _scan = scan;
            _whole = new GroupRead(readColumns, selector, twoPass);
            ReadColumns = readColumns;
            SkipRows = skipRows;

            var predicates = scan.RowFilter?.Predicates ?? [];
            _proofs = predicates.Count > 64
                ? []
                : predicates.Select((p, i) => (i, p.Proof)).Where(t => t.Proof is not null)
                    .Select(t => (t.i, t.Proof!)).ToArray();
            _accessor = _proofs.Length > 0 ? new ParquetStatisticsAccessor(schema) : null;
        }

        // How to read the planned row group: without the predicates its statistics prove TRUE for every row.
        private GroupRead ReadFor(RowGroupPlan plan)
        {
            if (_proofs.Length == 0)
                return _whole;

            var rowGroup = _metadata.RowGroups[plan.Index];
            ulong proven = 0;
            foreach (var (index, proof) in _proofs)
            {
                if (EngineeredWood.Expressions.StatisticsEvaluator.ProvesEveryRowTrue(proof, rowGroup, _accessor!))
                    proven |= 1UL << index;
            }

            if (proven == 0)
                return _whole;
            _reader.OnProvenRowGroup?.Invoke();
            return _without.GetOrAdd(proven, set => _reader.ReadWithout(_schema, _scan, set));
        }

        /// <summary>The columns the read decodes, predicate columns included: what to plan row groups with.</summary>
        public IReadOnlyList<string>? ReadColumns { get; }

        /// <summary>The rows the excluded rows rule out whole, for planning to skip unread; null for none.</summary>
        public RowSkipPredicate? SkipRows { get; }

        /// <summary>The selected rows of one planned row group, read without reserving scan memory.</summary>
        public IAsyncEnumerable<SelectedRecordBatch> ReadAsync(RowGroupPlan plan, CancellationToken cancellationToken)
        {
            var read = ReadFor(plan);
            return _reader.ReadsInTwoPasses(plan, read.TwoPass, _metadata.RowGroups[plan.Index], _proofs.Length > 0)
                ? _reader.ReadTwoPassAsync(plan, read.TwoPass!, read.Selector, reservation: null, growth: null, default, cancellationToken)
                : read.Selector.SelectAsync(
                    _reader.ReadPlannedAsync(plan, read.ReadColumns, cancellationToken, read.Selector.Answerable));
        }

        /// <summary>The selected rows of one planned row group, as one unit of a parallel read.</summary>
        public IAsyncEnumerable<SelectedRecordBatch> ReadAsync(
            RowGroupPlan plan, ScanReservation reservation, DecodedGrowth growth, CancellationToken cancellationToken)
        {
            var read = ReadFor(plan);
            return _reader.ReadsInTwoPasses(plan, read.TwoPass, _metadata.RowGroups[plan.Index], _proofs.Length > 0)
                ? _reader.ReadTwoPassAsync(plan, read.ReadColumns, read.TwoPass!, read.Selector, reservation, growth, cancellationToken)
                : read.Selector.SelectAsync(
                    _reader.ReadPlannedAsync(plan, read.ReadColumns, reservation, growth, cancellationToken, read.Selector.Answerable));
        }
    }

    /// <summary>
    /// The columns a selected read decodes: the projection, then each predicate column it does not cover,
    /// which the read drops again. Returns those dropped columns' names: top-level names, which are also their
    /// field names in the decoded batch.
    /// </summary>
    internal static (IReadOnlyList<string>? ReadColumns, IReadOnlyCollection<string> Dropped) ColumnsForSelection(
        SchemaDescriptor schema, IReadOnlyList<string>? projection, RowFilter? filter)
    {
        var topLevel = new HashSet<string>(StringComparer.Ordinal);
        foreach (var child in schema.Root.Children)
            topLevel.Add(child.Name);

        if (filter is not null)
        {
            foreach (var predicate in filter.Predicates)
            {
                foreach (var name in predicate.Columns)
                {
                    if (!topLevel.Contains(name))
                    {
                        throw new ArgumentException(
                            $"A row predicate reads column '{name}', which is not a top-level column of the file.");
                    }
                }
            }
        }

        if (projection is null)
            return (null, []);

        // Top-level columns the projection reads whole, and those it reads only some leaves of (a dotted path).
        var whole = new HashSet<string>(StringComparer.Ordinal);
        var partial = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in projection)
        {
            if (topLevel.Contains(name))
            {
                whole.Add(name);
                continue;
            }

            foreach (var column in schema.Columns)
            {
                if (column.DottedPath == name)
                {
                    partial.Add(column.Path[0]);
                    break;
                }
            }
        }

        var read = new List<string>(projection);
        var dropped = new HashSet<string>(StringComparer.Ordinal);
        if (filter is not null)
        {
            foreach (var predicate in filter.Predicates)
            {
                foreach (var name in predicate.Columns)
                {
                    if (whole.Contains(name))
                        continue;
                    if (partial.Contains(name))
                    {
                        throw new ArgumentException(
                            $"A row predicate reads column '{name}', which the projection reads only part of. " +
                            "Project the whole column, or leave it out of the projection.");
                    }

                    whole.Add(name);
                    read.Add(name);
                    dropped.Add(name);
                }
            }
        }

        return (read, dropped);
    }
}

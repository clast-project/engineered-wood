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
        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var (readColumns, dropped) = ColumnsForSelection(schema, scan.Columns, scan.RowFilter);
        var selector = new BatchSelector(scan.RowFilter, scan.ExcludedRows, dropped);

        RowSkipPredicate? skipRows = scan.ExcludedRows is { } excluded ? excluded.ExcludesAll : null;
        var plans = PlanRowGroupsAsync(
            metadata, scan.Prune ?? _options.Filter, readColumns, skipRows, cancellationToken);

        int concurrency = Math.Min(EffectiveConcurrentRowGroups, metadata.RowGroups.Count);
        if (concurrency <= 1 || metadata.RowGroups.Count <= 1)
        {
            await foreach (var plan in plans.ConfigureAwait(false))
            {
                await foreach (var batch in selector.SelectAsync(
                                   ReadPlannedAsync(plan, readColumns, cancellationToken)).ConfigureAwait(false))
                {
                    yield return batch;
                }
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
                           (plan, reservation, token) =>
                               selector.SelectAsync(ReadPlannedAsync(plan, readColumns, reservation, growth, token)),
                           // The positions count too: a positions-only read (no columns) holds nothing else.
                           static b => ArrowSizes.EstimatedBytes(b.Batch)
                                       + (b.Positions.Explicit is { } p ? p.Length * (long)sizeof(long) : 0),
                           static b => b.Dispose(),
                           concurrency,
                           budget,
                           cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            yield return batch;
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

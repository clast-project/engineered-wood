// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using Apache.Arrow;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.Scans;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// A batch of a partitioned scan (<see cref="DeltaTable.OpenPartitionsAsync"/>) with the number of the scan unit it
/// came from. Units are numbered in the order a serial read gives (file after file, row group after row group), so a
/// host that must restore that order sorts by <see cref="Unit"/>, then by arrival within a unit.
/// </summary>
public readonly record struct DeltaScanBatch(long Unit, RecordBatch Batch);

/// <summary>
/// A scan read as several independent streams (<see cref="DeltaTable.OpenPartitionsAsync"/>). Each partition takes
/// the next unread scan unit (one row group of one file) when it needs one, so a slow unit holds back only its own
/// partition. Dispose after the partitions: it closes the files the planner opened ahead.
/// </summary>
public sealed class DeltaScanPartitions : IAsyncDisposable
{
    private readonly Func<int, CancellationToken, IAsyncEnumerable<DeltaScanBatch>> _partition;
    private readonly Func<ValueTask> _dispose;

    internal DeltaScanPartitions(int count, Func<int, CancellationToken, IAsyncEnumerable<DeltaScanBatch>> partition,
                                 Func<ValueTask> dispose)
    {
        Count = count;
        _partition = partition;
        _dispose = dispose;
    }

    /// <summary>How many partitions the scan was opened with.</summary>
    public int Count { get; }

    /// <summary>Partition <paramref name="index"/>. Enumerate each at most once; partitions may run concurrently.</summary>
    public IAsyncEnumerable<DeltaScanBatch> Partition(int index, CancellationToken cancellationToken = default)
    {
        if (index < 0 || index >= Count)
            throw new ArgumentOutOfRangeException(nameof(index));
        return _partition(index, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _dispose();
}

public sealed partial class DeltaTable
{
    /// <summary>
    /// Opens a scan as <paramref name="partitions"/> independent streams over ONE plan: the same files, pruning,
    /// row masks and metadata columns as <see cref="ReadAsync"/>, but no order between partitions. Each partition
    /// pulls the next scan unit from a shared planner (opened files ahead, as the ordered scan does) and reads it to
    /// the end. Offset and limit are not supported here.
    /// </summary>
    public async ValueTask<DeltaScanPartitions> OpenPartitionsAsync(
        DeltaReadOptions? options, int partitions, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (partitions < 1)
            throw new ArgumentOutOfRangeException(nameof(partitions), partitions, "Must be at least 1.");
        options ??= new DeltaReadOptions();
        if (options.Offset != 0 || options.Limit is not null)
            throw new ArgumentException("A partitioned scan takes no offset or limit.", nameof(options));

        var snapshot = ResolveReadSnapshot(options)
                       ?? await GetSnapshotAtVersionAsync(options.AtVersion!.Value, cancellationToken).ConfigureAwait(false);

        var metadata = options.Metadata;
        ValidateReadMetadata(snapshot, metadata, options.MetadataPrefix);
        bool wantAddress = (metadata & DeltaRowMetadata.RowAddress) != 0;
        bool wantLocator = (metadata & DeltaRowMetadata.Locator) != 0;
        bool wantTracking = (metadata & DeltaRowMetadata.RowTracking) != 0;
        var metadataFields = MetadataFields(metadata, options.MetadataPrefix);
        if (options.FilterRows && options.Filter is not null)
            RefuseUnanalyzable(options.Filter, snapshot);
        RefuseUnknownMaskColumns(options.RowMasks, snapshot);

        var pruner = options.Filter is null ? null : new DeltaFilePruner(
            snapshot.Schema, snapshot.Metadata.PartitionColumns, _options.PreferTypedCheckpointStats);
        var ordered = OrderedActiveFiles(snapshot);
        var included = new List<(int Ordinal, AddFile File)>(ordered.Count);
        for (int ordinal = 0; ordinal < ordered.Count; ordinal++)
        {
            if (pruner is null || pruner.ShouldInclude(ordered[ordinal], options.Filter!))
                included.Add((ordinal, ordered[ordinal]));
        }
        var scan = new ScanFileShape(options, metadataFields, wantAddress, wantLocator, wantTracking);
        var includedFiles = included.ConvertAll(i => i.File);

        var planner = new SharedScanUnits(
            ct => PlanScanUnitsAsync(snapshot, options, includedFiles, lookahead: Math.Max(1, EffectiveConcurrentReads), ct));

        async IAsyncEnumerable<DeltaScanBatch> Partition(int index, [EnumeratorCancellation] CancellationToken ct)
        {
            while (true)
            {
                var (got, unit, number) = await planner.NextAsync(ct).ConfigureAwait(false);
                if (!got)
                    yield break;
                try
                {
                    await foreach (var batch in ReadScanUnitAsync(
                                       unit, scan, included[unit.File].Ordinal, ScanReservation.None, growth: null, ct)
                                       .ConfigureAwait(false))
                    {
                        yield return new DeltaScanBatch(number, batch);
                    }
                }
                finally
                {
                    unit.Read.Release();
                }
            }
        }

        return new DeltaScanPartitions(partitions, Partition, planner.DisposeAsync);
    }

    /// <summary>
    /// The planner's units handed out one at a time to whichever partition asks, each with its number in plan order.
    /// The planner itself stays serial (it is enumerated under a gate); only the reads run concurrently.
    /// </summary>
    private sealed class SharedScanUnits : IAsyncDisposable
    {
        private readonly SemaphoreSlim _gate = new(1, 1);
        private readonly CancellationTokenSource _cts = new();
        private readonly Func<CancellationToken, IAsyncEnumerable<ScanUnit>> _plan;
        private IAsyncEnumerator<ScanUnit>? _units;
        private long _next;
        private bool _done;

        internal SharedScanUnits(Func<CancellationToken, IAsyncEnumerable<ScanUnit>> plan) => _plan = plan;

        internal async ValueTask<(bool Got, ScanUnit Unit, long Number)> NextAsync(CancellationToken ct)
        {
            await _gate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (_done)
                    return (false, default, 0);
                _units ??= _plan(_cts.Token).GetAsyncEnumerator(_cts.Token);
                if (!await _units.MoveNextAsync().ConfigureAwait(false))
                {
                    _done = true;
                    return (false, default, 0);
                }
                return (true, _units.Current, _next++);
            }
            catch
            {
                _done = true;
                throw;
            }
            finally
            {
                _gate.Release();
            }
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();
            await _gate.WaitAsync().ConfigureAwait(false);
            try
            {
                _done = true;
                if (_units is not null)
                {
                    try
                    {
                        await _units.DisposeAsync().ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    _units = null;
                }
            }
            finally
            {
                _gate.Release();
                _cts.Dispose();
            }
        }
    }
}

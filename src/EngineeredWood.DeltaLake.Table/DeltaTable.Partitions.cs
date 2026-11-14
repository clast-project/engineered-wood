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

/// <summary>
/// A scan read by its units (<see cref="DeltaTable.OpenScanUnitsAsync"/>): <see cref="NextAsync"/> hands out the next
/// planned unit to whichever caller asks, so the caller decides how many it reads at once. Dispose after the units:
/// it closes the files the planner opened ahead.
/// </summary>
public sealed class DeltaScanUnits : IAsyncDisposable
{
    private readonly DeltaTable.SharedScanUnits _planner;
    private readonly Func<DeltaTable.ScanUnit, CancellationToken, IAsyncEnumerable<RecordBatch>> _read;

    internal DeltaScanUnits(DeltaTable.SharedScanUnits planner,
                            Func<DeltaTable.ScanUnit, CancellationToken, IAsyncEnumerable<RecordBatch>> read)
    {
        _planner = planner;
        _read = read;
    }

    /// <summary>The next unit in plan order, or null when the scan has none left. Safe to call concurrently.</summary>
    public async ValueTask<DeltaScanUnit?> NextAsync(CancellationToken cancellationToken = default)
    {
        var (got, unit, number) = await _planner.NextAsync(cancellationToken).ConfigureAwait(false);
        return got ? new DeltaScanUnit(number, unit, _read) : null;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _planner.DisposeAsync();
}

/// <summary>
/// One unit of a <see cref="DeltaScanUnits"/>: its number in plan order (file after file, row group after row group)
/// and its rows. Read it once; disposing an unread unit releases what the planner holds for it.
/// </summary>
public sealed class DeltaScanUnit : IAsyncDisposable
{
    private readonly DeltaTable.ScanUnit _unit;
    private readonly Func<DeltaTable.ScanUnit, CancellationToken, IAsyncEnumerable<RecordBatch>> _read;
    private int _released;

    internal DeltaScanUnit(long number, DeltaTable.ScanUnit unit, Func<DeltaTable.ScanUnit, CancellationToken, IAsyncEnumerable<RecordBatch>> read)
    {
        Number = number;
        _unit = unit;
        _read = read;
    }

    /// <summary>The unit's number in plan order.</summary>
    public long Number { get; }

    /// <summary>The unit's rows.</summary>
    public async IAsyncEnumerable<RecordBatch> ReadAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        try
        {
            await foreach (var batch in _read(_unit, cancellationToken).ConfigureAwait(false))
                yield return batch;
        }
        finally
        {
            Release();
        }
    }

    private void Release()
    {
        if (Interlocked.Exchange(ref _released, 1) == 0)
            _unit.Read.Release();
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Release();
        return default;
    }
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
        if (partitions < 1)
            throw new ArgumentOutOfRangeException(nameof(partitions), partitions, "Must be at least 1.");
        var units = await OpenScanUnitsAsync(options, cancellationToken).ConfigureAwait(false);

        async IAsyncEnumerable<DeltaScanBatch> Partition(int index, [EnumeratorCancellation] CancellationToken ct)
        {
            while (await units.NextAsync(ct).ConfigureAwait(false) is { } unit)
            {
                await foreach (var batch in unit.ReadAsync(ct).ConfigureAwait(false))
                    yield return new DeltaScanBatch(unit.Number, batch);
            }
        }

        return new DeltaScanPartitions(partitions, Partition, units.DisposeAsync);
    }

    /// <summary>
    /// Opens a scan as its planned units (one row group of one file each), handed out one at a time by
    /// <see cref="DeltaScanUnits.NextAsync"/> to whoever asks, in plan order. Same files, pruning, row masks and
    /// metadata columns as <see cref="ReadAsync"/>; the caller decides how many units it reads at once. Offset and
    /// limit are not supported here.
    /// <para><paramref name="readers"/>: how many units the caller reads at once, if it knows. The planner then opens
    /// that many files ahead instead of the table's read concurrency, so that readers do not wait for footers.</para>
    /// </summary>
    public async ValueTask<DeltaScanUnits> OpenScanUnitsAsync(
        DeltaReadOptions? options, CancellationToken cancellationToken = default, int? readers = null)
    {
        ThrowIfDisposed();
        options ??= new DeltaReadOptions();
        if (options.Offset != 0 || options.Limit is not null)
            throw new ArgumentException("A scan read by units takes no offset or limit.", nameof(options));

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
            ct => PlanScanUnitsAsync(snapshot, options, includedFiles,
                                     lookahead: Math.Max(1, readers ?? EffectiveConcurrentReads), ct));

        IAsyncEnumerable<RecordBatch> Read(ScanUnit unit, CancellationToken ct)
            => ReadScanUnitAsync(unit, scan, included[unit.File].Ordinal, ScanReservation.None, growth: null, ct);

        return new DeltaScanUnits(planner, Read);
    }

    /// <summary>
    /// The planner's units handed out one at a time to whichever partition asks, each with its number in plan order.
    /// The planner itself stays serial (it is enumerated under a gate); only the reads run concurrently.
    /// </summary>
    internal sealed class SharedScanUnits : IAsyncDisposable
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

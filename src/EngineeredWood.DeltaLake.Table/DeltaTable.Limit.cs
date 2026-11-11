// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using Apache.Arrow;
using EngineeredWood.Arrow;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// <see cref="DeltaReadOptions.Offset"/> and <see cref="DeltaReadOptions.Limit"/> (#502 phase 5f).
/// </summary>
public sealed partial class DeltaTable
{
    /// <summary>How many scan units (a row group, or a file through the codec seam) this table's scans have begun to
    /// read: for tests, to show what an offset and a limit leave unread.</summary>
    internal int ScanUnitsRead => Volatile.Read(ref _scanUnitsRead);

    private int _scanUnitsRead;

    /// <summary>
    /// A scan's offset and limit, shared by its planner and its output. The output (<see cref="LimitRowsAsync"/>) is
    /// what makes the result exact, whatever was read: it skips the offset's rows and stops after the limit's. The
    /// planner only saves reading.
    /// </summary>
    /// <remarks>
    /// <para><b>Counted</b> (no row condition, the built-in reader): every unit's live rows are known before it is
    /// read, from the file's footer and deletion vector (<see cref="LiveRowsAsync"/>). The planner drops the units
    /// wholly inside the offset, crediting their rows (<see cref="Skipped"/>), and stops planning once the units handed
    /// out cover <see cref="Need"/>. Every skip comes before the first unit handed out, so the output reads
    /// <see cref="Skipped"/> once, at its first batch. Counts come from the files, never from the log's statistics:
    /// those only decide how many files are opened ahead (<see cref="MayOpenAhead"/>), which changes no row.</para>
    /// <para><b>Bounded</b> (an exact filter, masks, or the codec seam): how many rows a unit yields is not known
    /// ahead. With no offset, a parallel scan hands out one unit first; with one, a whole window, since the offset's rows
    /// have to be read through anyway. As units finish, it estimates the units still needed from the rows they yielded
    /// (the rows still needed over their average) and lets that many go, up to the window. A limit the first unit meets
    /// reads that unit alone. Before any unit has yielded a row, the units in flight double.</para>
    /// </remarks>
    internal sealed class ScanLimit
    {
        private readonly long[]? _hintedBefore;
        private readonly SemaphoreSlim? _permits;
        private long _planned;
        private long _skipped;
        private int _granted = 1;
        private bool _covered;

        // The bounded ramp's tally, under _gate: units whose read ended, and the rows they yielded.
        private readonly object _gate = new();
        private readonly int _window;
        private long _unitsDone;
        private long _rowsDone;
        private long _rowsConsumed;
        private bool _consumerWaiting;

        // Bounded: how many files ahead the planner may open, beside the units granted. Files whose row groups are all
        // pruned yield no unit, so grant nothing; each one doubles this, so a run of them is opened in parallel
        // (review round 1).
        private int _openAhead = 1;

        public ScanLimit(long offset, long? limit, bool counted, bool ramp, int window, IReadOnlyList<AddFile> files)
        {
            _window = Math.Max(1, window);
            Offset = offset;
            Limit = limit;
            Counted = counted;
            Need = limit is { } l ? (offset > long.MaxValue - l ? long.MaxValue : offset + l) : long.MaxValue;
            // With an offset, the rows it covers have to be read and evaluated to know which they are, so the first
            // window is never wasted: start there. Without one, start with one unit.
            if (ramp && limit is not null)
            {
                _granted = offset > 0 ? _window : 1;
                _permits = new SemaphoreSlim(_granted);
            }

            // The rows the log's statistics say come before each file, when they say: a hint for opening files ahead,
            // so only for a parallel scan with a positive limit. Parsed only until they cover Need: every file after
            // that is past it (review round 1).
            if (counted && limit is > 0 && _window > 1)
            {
                _hintedBefore = new long[files.Count + 1];
                for (int i = 0; i < files.Count; i++)
                {
                    if (_hintedBefore[i] >= Need)
                    {
                        _hintedBefore.AsSpan(i + 1).Fill(long.MaxValue);
                        break;
                    }

                    long live = files[i].GetNumRecords() is { } records
                        ? Math.Max(0, records - (files[i].DeletionVector?.Cardinality ?? 0))
                        : 0;
                    _hintedBefore[i + 1] = _hintedBefore[i] > long.MaxValue - live ? long.MaxValue : _hintedBefore[i] + live;
                }
            }
        }

        /// <summary>Units granted so far (bounded), for tests.</summary>
        internal int Granted => Volatile.Read(ref _granted);

        public long Offset { get; }
        public long? Limit { get; }
        public bool Counted { get; }

        /// <summary>Rows the units handed out must cover: the offset's and the limit's.</summary>
        public long Need { get; }

        /// <summary>Rows of the offset the planner skipped without reading.</summary>
        public long Skipped => Volatile.Read(ref _skipped);

        /// <summary>
        /// Counted: the units handed out cover <see cref="Need"/>, so the planner opens no further file (review round 1:
        /// the file it is about to plan is opened whatever the lookahead says).
        /// </summary>
        public bool Covered => Counted && Volatile.Read(ref _covered);

        /// <summary>
        /// Whether the planner may open file <paramref name="file"/> ahead of the one it is planning: counted, unless
        /// the log's statistics say the files before it already cover <see cref="Need"/> (a file it turns out to need
        /// is opened when its turn comes); bounded, as far ahead as the units granted, or as the files that yielded no
        /// unit have opened (<see cref="FileWithoutUnits"/>).
        /// </summary>
        public bool MayOpenAhead(int file, int current)
        {
            if (_hintedBefore is not null)
                return _hintedBefore[file] < Need;
            return _permits is null
                || file - current < Math.Max(Volatile.Read(ref _granted), Volatile.Read(ref _openAhead));
        }

        /// <summary>
        /// A file yielded no unit (every row group pruned): bounded, the planner may open twice as many files ahead,
        /// up to the window.
        /// </summary>
        public void FileWithoutUnits()
        {
            if (_permits is null)
                return;
            lock (_gate)
                Volatile.Write(ref _openAhead, Math.Min(_window, 2 * _openAhead));
        }

        /// <summary>
        /// What the planner does with a unit: skip it (its rows credited to the offset), hand it out, or stop (the units
        /// handed out cover what is needed). <paramref name="live"/> is the unit's live rows when counted.
        /// </summary>
        public async ValueTask<UnitAdmission> AdmitAsync(Func<ValueTask<long>>? live, CancellationToken cancellationToken)
        {
            if (Counted && live is not null)
            {
                if (Volatile.Read(ref _covered))
                    return UnitAdmission.Stop;
                long rows = await live().ConfigureAwait(false);

                // Compared and summed without overflowing (review round 2): the offset may be anything a long holds.
                if (_planned <= Offset && rows <= Offset - _planned && Volatile.Read(ref _skipped) == _planned)
                {
                    _planned += rows;
                    Volatile.Write(ref _skipped, _planned);
                    return UnitAdmission.Skip;
                }

                _planned = rows > long.MaxValue - _planned ? long.MaxValue : _planned + rows;
                if (_planned >= Need)
                    Volatile.Write(ref _covered, true);
                return UnitAdmission.Yield;
            }

            if (_permits is not null)
                await _permits.WaitAsync(cancellationToken).ConfigureAwait(false);
            return UnitAdmission.Yield;
        }

        /// <summary>
        /// A unit's read ended, having yielded <paramref name="rows"/> rows (bounded, parallel): let as many more units
        /// go as the rows still needed suggest, up to the window.
        /// </summary>
        public void UnitDone(long rows)
        {
            if (_permits is null)
                return;
            int release;
            lock (_gate)
            {
                _unitsDone++;
                _rowsDone += rows;
                long remaining = Need - _rowsDone;

                // The units still needed at the rows per unit so far; with no row yet, twice the units done.
                long wanted = 0;
                if (remaining > 0)
                {
                    long perUnit = _rowsDone / _unitsDone;
                    wanted = perUnit > 0 ? (remaining + perUnit - 1) / perUnit : 2 * _unitsDone;
                }

                release = GrantLocked(Math.Min(wanted, _window));
            }

            if (release > 0)
                _permits.Release(release);
        }

        /// <summary>
        /// The output is about to wait for a batch (<paramref name="waiting"/>), or has received one of
        /// <paramref name="rows"/> rows. A wait with no unit in flight and every row the ended units yielded already
        /// received would never end, since only units grant more: one more is granted then. The tally above says no more
        /// is needed once the units yield what the output needs, so this only matters if the two disagree, but the scan
        /// must not hang on that. A wait whose batch is already queued is not one: a unit that ends before the output
        /// wakes for its batch grants nothing (review round 1, where it granted a cascade of units on a slow runner).
        /// </summary>
        public void ConsumerWaiting(bool waiting, long rows = 0)
        {
            if (_permits is null)
                return;
            int release;
            lock (_gate)
            {
                _consumerWaiting = waiting;
                _rowsConsumed += rows;
                release = GrantLocked(0);
            }

            if (release > 0)
                _permits.Release(release);
        }

        // Grants units up to `target` in flight; and one, whatever the target, when the output waits with none in flight
        // and nothing queued for it. Returns how many to release.
        private int GrantLocked(long target)
        {
            long outstanding = _granted - _unitsDone;
            if (_consumerWaiting && outstanding <= 0 && _rowsConsumed >= _rowsDone)
                target = Math.Max(target, 1);
            if (target <= outstanding)
                return 0;
            int release = (int)(target - outstanding);
            Volatile.Write(ref _granted, _granted + release);
            return release;
        }
    }

    internal enum UnitAdmission
    {
        Skip,
        Yield,
        Stop,
    }

    /// <summary>
    /// A scan's offset and limit, validated, or null for neither. Throws for a negative one, or for either with a
    /// superset filter.
    /// </summary>
    private ScanLimit? ScanLimitFor(DeltaReadOptions options, IReadOnlyList<AddFile> files, int window)
    {
        if (options.Offset < 0)
            throw new ArgumentOutOfRangeException(nameof(options), options.Offset, "DeltaReadOptions.Offset is negative.");
        if (options.Limit is < 0)
            throw new ArgumentOutOfRangeException(nameof(options), options.Limit, "DeltaReadOptions.Limit is negative.");
        if (options.Offset == 0 && options.Limit is null)
            return null;
        if (options.Filter is not null && !options.FilterRows)
        {
            throw new ArgumentException(
                "DeltaReadOptions.Offset and .Limit apply to the rows a scan returns, and a Filter without FilterRows "
                + "returns a superset of the matching rows that depends on what pruning kept. Set FilterRows, or "
                + "drop the offset and limit.",
                nameof(options));
        }

        // Counted when no row condition stands between a row group's live rows and the rows returned: no exact filter
        // (FilterRows without a Filter is none, review round 2), no mask, and the built-in reader.
        bool exactFilter = options.FilterRows && options.Filter is not null;
        bool counted = !exactFilter && options.RowMasks is not { Count: > 0 } && _options.DataFileReader is null;
        return new ScanLimit(options.Offset, options.Limit, counted, ramp: !counted && window > 1, window, files);
    }

    /// <summary>
    /// The live rows of one planned row group: the rows its plan reads, less those its file's deletion vector
    /// deletes among them.
    /// </summary>
    private static async ValueTask<long> LiveRowsAsync(
        DataFileRead read, ParquetFileReader.RowGroupPlan plan, CancellationToken cancellationToken)
    {
        var footer = await read.Reader!.ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        long groupRows = footer.RowGroups[plan.Index].NumRows;
        long live = 0;
        if (plan.Ranges is null)
        {
            live = groupRows - Deleted(read.DeletedRows, plan.GroupStart, groupRows);
        }
        else
        {
            foreach (var range in plan.Ranges)
                live += range.End - range.Start - Deleted(read.DeletedRows, plan.GroupStart + range.Start, range.End - range.Start);
        }

        return live;
    }

    /// <summary>How many of the rows [<paramref name="start"/>, start + <paramref name="length"/>) are deleted.</summary>
    private static long Deleted(DeletionVectorBitmap? deleted, long start, long length)
    {
        if (deleted is null || deleted.IsEmpty || length <= 0 || !deleted.Intersects(start, length))
            return 0;

        // Clear the deleted rows from an all-set selection and count what is left, a chunk at a time.
        const int Chunk = 1 << 20;
        var words = new ulong[SelectionMask.WordCount((int)Math.Min(length, Chunk))];
        long kept = 0;
        for (long at = 0; at < length; at += Chunk)
        {
            int rows = (int)Math.Min(Chunk, length - at);
            var selection = words.AsSpan(0, SelectionMask.WordCount(rows));
            SelectionMask.SelectAll(selection, rows);
            deleted.ClearDeleted(start + at, rows, selection);
            kept += SelectionMask.Count(selection);
        }

        return length - kept;
    }

    /// <summary>
    /// The scan's batches after <paramref name="limit"/>: the offset's rows skipped (those the planner skipped
    /// unread first), then at most the limit's rows, after which it stops, which ends the scan and cancels what it has
    /// in flight. A batch cut in two is gathered into a batch of its own.
    /// </summary>
    private static async IAsyncEnumerable<RecordBatch> LimitRowsAsync(
        IAsyncEnumerable<RecordBatch> batches, ScanLimit limit,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        long toSkip = -1;
        long left = limit.Limit ?? long.MaxValue;
        if (left == 0)
            yield break;

        // The batches by hand, so that the limit knows when the output waits for one (ScanLimit.ConsumerWaiting).
        var e = batches.GetAsyncEnumerator(cancellationToken);
        try
        {
            while (true)
            {
                limit.ConsumerWaiting(waiting: true);
                bool more = await e.MoveNextAsync().ConfigureAwait(false);
                limit.ConsumerWaiting(waiting: false, more ? e.Current.Length : 0);
                if (!more)
                    yield break;
                var batch = e.Current;
                if (toSkip < 0)
                    toSkip = limit.Offset - limit.Skipped;

                int rows = batch.Length;
                if (toSkip >= rows)
                {
                    toSkip -= rows;
                    batch.Dispose();
                    continue;
                }

                int start = (int)toSkip;
                toSkip = 0;
                int take = (int)Math.Min(rows - start, left);
                left -= take;
                if (start == 0 && take == rows)
                {
                    yield return batch;
                }
                else
                {
                    RecordBatch cut;
                    try
                    {
                        var indices = new int[take];
                        for (int i = 0; i < take; i++)
                            indices[i] = start + i;
                        cut = BatchSelector.Gather(batch, indices, static _ => true);
                        BatchSelector.ReleaseAfterGather(batch, cut);
                    }
                    catch
                    {
                        batch.Dispose();
                        throw;
                    }

                    yield return cut;
                }

                if (left == 0)
                    yield break;
            }
        }
        finally
        {
            await e.DisposeAsync().ConfigureAwait(false);
        }
    }
}

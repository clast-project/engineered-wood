// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Arrow;

/// <summary>
/// Run-end encoding: the Arrow layout that stores a column as (run end, value) pairs rather than one slot
/// per row. A column holding one value in every row is a single run — a handful of bytes whatever the row
/// count, where <see cref="ArrowCompute.Repeat"/> tiles the value across N slots.
///
/// <para>This is a WRITE-side representation. No Parquet file holds run-end encoding, so nothing on the
/// read path produces one; a column that arrives here run-end encoded was built that way deliberately, by
/// a caller that knows the whole path it will travel. <see cref="Expand"/> is the escape hatch for a
/// consumer that cannot walk runs.</para>
///
/// <para><b>Nulls do not live where the rest of Arrow puts them.</b> A run-end encoded array has no
/// validity bitmap of its own — the spec puts its nulls in the values child, one per RUN — so
/// <c>array.IsNull(row)</c> answers false for every row of an array whose values child is all null. Every
/// null-sensitive walk here therefore goes through the run's value, and callers outside this file must do
/// the same. That silent-false is the single most dangerous thing about the layout.</para>
/// </summary>
public static class RunEndEncoding
{
    /// <summary>
    /// One run of an array, clipped to the array's own window (its <see cref="ArrayData.Offset"/> and
    /// length), so <see cref="Length"/> counts only rows the array actually exposes.
    /// </summary>
    public readonly struct Run
    {
        internal Run(int physicalIndex, int length)
        {
            PhysicalIndex = physicalIndex;
            Length = length;
        }

        /// <summary>Index of this run's value in the array's <c>Values</c> child.</summary>
        public int PhysicalIndex { get; }

        /// <summary>How many consecutive logical rows this run covers.</summary>
        public int Length { get; }
    }

    /// <summary>
    /// Walks the runs of <paramref name="array"/> in logical order. Allocation-free, and O(runs) rather
    /// than O(rows) — which is the whole reason the layout is worth accepting.
    /// </summary>
    public static RunEnumerator EnumerateRuns(RunEndEncodedArray array) => new(array);

    /// <summary>
    /// Builds a constant column as a single run: <paramref name="length"/> rows of one value in ~32 bytes,
    /// where <see cref="ArrowCompute.Repeat"/> would materialize a slot per row. The value is given in the
    /// same raw-Arrow-encoding form <see cref="ArrowCompute.Repeat"/> takes, and for the same reason —
    /// bytes ride through verbatim, so unit, timezone, precision and scale survive.
    /// </summary>
    /// <param name="type">The VALUE type. The array's own type is <c>RunEndEncodedType(int32, type)</c>.</param>
    public static RunEndEncodedArray Constant(IArrowType type, ReadOnlySpan<byte> value, int length)
    {
        if (length < 0)
            throw new ArgumentOutOfRangeException(nameof(length), length, "Length cannot be negative.");

        // A zero-row column is zero runs, not one empty run: a run end of 0 would be a run covering no
        // rows, which the layout does not admit.
        if (length == 0)
            return new RunEndEncodedArray(Int32Values([]), ArrowCompute.Repeat(type, value, 0));

        return new RunEndEncodedArray(Int32Values([length]), ArrowCompute.Repeat(type, value, 1));
    }

    /// <summary>
    /// Materializes <paramref name="array"/> as a plain array of its value type — one slot per row, nulls
    /// restored to the validity bitmap where the rest of Arrow expects them.
    ///
    /// <para>This is the fallback for a consumer with no run-aware path, and it costs exactly the memory
    /// the encoding exists to avoid. Prefer <see cref="EnumerateRuns"/>.</para>
    /// </summary>
    public static IArrowArray Expand(RunEndEncodedArray array)
    {
        var physical = new int[array.Length];
        int row = 0;
        foreach (var run in EnumerateRuns(array))
        {
            physical.AsSpan(row, run.Length).Fill(run.PhysicalIndex);
            row += run.Length;
        }

        return ArrowCompute.Take(array.Values, physical);
    }

    /// <summary>
    /// Returns an equivalent array whose window is the whole of it — offset zero, and a last run end equal
    /// to the length. O(runs): the runs are re-clipped and the values gathered down to the ones the window
    /// actually reaches, so no row is materialized.
    ///
    /// <para>Needed because a SLICE of a run-end encoded array is a view: the child arrays still hold every
    /// run in the original, and only <see cref="ArrayData.Offset"/> says which rows are in scope. Any
    /// consumer reading the children directly — the Parquet writer does — sees rows the array does not
    /// expose unless the view is compacted first.</para>
    /// </summary>
    public static RunEndEncodedArray Compact(RunEndEncodedArray array)
    {
        int runCount = array.Values.Length;
        bool alreadyCompact = array.Data.Offset == 0
            && (runCount == 0
                ? array.Length == 0
                : RunEndAt(array.RunEnds, runCount - 1) == array.Length);

        if (alreadyCompact)
            return array;

        var physical = new List<int>();
        var lengths = new List<int>();
        foreach (var run in EnumerateRuns(array))
        {
            physical.Add(run.PhysicalIndex);
            lengths.Add(run.Length);
        }

        return Build(ArrowCompute.Take(array.Values, physical), lengths, RunEndsType(array));
    }

    /// <summary>
    /// Gathers rows by logical position, run-end encoded again on the way out.
    ///
    /// <para>The result is ALWAYS run-end encoded, even where that is the larger representation (gathering
    /// rows in an order that breaks every run yields one run per row). Returning a plain array for those
    /// would contradict the schema of the batch the column lands in, and a column whose type disagrees with
    /// its field is a mismatch nothing checks — worse than the size.</para>
    /// </summary>
    public static RunEndEncodedArray Take(RunEndEncodedArray source, ReadOnlySpan<int> indices)
    {
        var physical = new List<int>();
        var lengths = new List<int>();
        int previous = -1;

        for (int i = 0; i < indices.Length; i++)
        {
            // Binary search per row over the runs, not a scan: the indices are in whatever order the caller
            // asked for, so there is no cursor to carry forward.
            int p = source.FindPhysicalIndex(indices[i]);

            if (i > 0 && p == previous)
            {
                lengths[lengths.Count - 1]++;
                continue;
            }

            physical.Add(p);
            lengths.Add(1);
            previous = p;
        }

        return Build(ArrowCompute.Take(source.Values, physical), lengths, RunEndsType(source));
    }

    /// <summary>
    /// Joins run-end encoded arrays end to end, run-end encoded again: each input's runs (clipped to its window, as
    /// <see cref="Compact"/> clips them) follow the previous input's, and their values are gathered and joined.
    /// O(runs). A run that ends one input and the run that starts the next stay two runs, even when their values
    /// are equal; the layout allows that, and comparing values of any type is not worth it here.
    ///
    /// <para>The result keeps the inputs' run-end width, so it matches the field they came from. Inputs of different
    /// widths are refused, as inputs of different value types are: no one field describes them all.</para>
    /// </summary>
    public static RunEndEncodedArray Concatenate(IReadOnlyList<RunEndEncodedArray> arrays)
    {
        if (arrays is null)
            throw new ArgumentNullException(nameof(arrays));
        if (arrays.Count == 0)
            throw new ArgumentException("There is nothing to concatenate.", nameof(arrays));

        var valueType = ((RunEndEncodedType)arrays[0].Data.DataType).ValuesDataType;
        var runEndsType = RunEndsType(arrays[0]);
        var values = new List<IArrowArray>(arrays.Count);
        var lengths = new List<int>();
        foreach (var array in arrays)
        {
            var arrayValueType = ((RunEndEncodedType)array.Data.DataType).ValuesDataType;
            // The whole type, not only its kind. Arrow's concatenator refuses different timestamp units or zones and
            // decimal scales itself, but not structs whose children are named differently: it would label the
            // joined values with the first input's names.
            if (!ArrowTypeEquality.SameType(arrayValueType, valueType))
            {
                throw new ArgumentException(
                    $"Cannot concatenate run-end encoded {arrayValueType} values onto {valueType} ones: their types differ.",
                    nameof(arrays));
            }
            if (RunEndsType(array).TypeId != runEndsType.TypeId)
            {
                throw new ArgumentException(
                    $"Cannot concatenate {RunEndsType(array).Name} run ends onto {runEndsType.Name} ones.",
                    nameof(arrays));
            }

            var physical = new List<int>();
            foreach (var run in EnumerateRuns(array))
            {
                physical.Add(run.PhysicalIndex);
                lengths.Add(run.Length);
            }
            values.Add(ArrowCompute.Take(array.Values, physical));
        }

        return Build(ArrowCompute.Concatenate(values), lengths, runEndsType);
    }

    /// <summary>
    /// Reads run end <paramref name="index"/>. The spec allows int16, int32 or int64 run ends; all three
    /// are read here as <see cref="long"/> so callers never have to care which they were handed.
    /// </summary>
    public static long RunEndAt(IArrowArray runEnds, int index) => runEnds switch
    {
        Int32Array a => a.Values[index],
        Int64Array a => a.Values[index],
        Int16Array a => a.Values[index],
        _ => throw new NotSupportedException(
            $"Run ends must be int16, int32 or int64, not {runEnds.Data.DataType.TypeId}."),
    };

    private static IArrowType RunEndsType(RunEndEncodedArray array) =>
        ((RunEndEncodedType)array.Data.DataType).RunEndsDataType;

    /// <summary>
    /// Assembles an array from values already gathered one-per-run, plus each run's row count, with run ends of
    /// <paramref name="runEndsType"/> (int16, int32 or int64: the source's, so the result matches the field it came
    /// from). Run ends are the running total, which is the only form the layout stores.
    /// </summary>
    private static RunEndEncodedArray Build(IArrowArray values, List<int> runLengths, IArrowType runEndsType)
    {
        long max = runEndsType.TypeId switch
        {
            ArrowTypeId.Int16 => short.MaxValue,
            ArrowTypeId.Int32 => int.MaxValue,
            ArrowTypeId.Int64 => long.MaxValue,
            _ => throw new NotSupportedException(
                $"Run ends must be int16, int32 or int64, not {runEndsType.TypeId}."),
        };

        var ends = new long[runLengths.Count];
        long total = 0;
        for (int i = 0; i < runLengths.Count; i++)
        {
            total += runLengths[i];
            if (total > max)
            {
                throw new ArgumentException(
                    $"Run ends total {total} rows, which overflows the {runEndsType.Name} run-end type.",
                    nameof(runLengths));
            }

            ends[i] = total;
        }

        return new RunEndEncodedArray(RunEnds(runEndsType, ends), values);
    }

    /// <summary>A run-end array of the given width over the given values, built as buffers rather than through a
    /// builder. Every value fits: <see cref="Build"/> checked.</summary>
    private static IArrowArray RunEnds(IArrowType type, long[] ends)
    {
        switch (type.TypeId)
        {
            case ArrowTypeId.Int16:
            {
                var narrow = new short[ends.Length];
                for (int i = 0; i < ends.Length; i++)
                    narrow[i] = (short)ends[i];
                return new Int16Array(Buffers(Int16Type.Default, MemoryMarshal.AsBytes(narrow.AsSpan()), ends.Length));
            }
            case ArrowTypeId.Int64:
                return new Int64Array(Buffers(Int64Type.Default, MemoryMarshal.AsBytes(ends.AsSpan()), ends.Length));
            default:
            {
                var narrow = new int[ends.Length];
                for (int i = 0; i < ends.Length; i++)
                    narrow[i] = (int)ends[i];
                return Int32Values(narrow);
            }
        }
    }

    private static ArrayData Buffers(IArrowType type, ReadOnlySpan<byte> bytes, int length) =>
        new(type, length, nullCount: 0, offset: 0, [ArrowBuffer.Empty, new ArrowBuffer(bytes.ToArray())]);

    /// <summary>An int32 array over the given values, built as buffers rather than through a builder.</summary>
    private static Int32Array Int32Values(ReadOnlySpan<int> values) =>
        new(Buffers(Int32Type.Default, MemoryMarshal.AsBytes(values), values.Length));

    /// <summary>
    /// Walks runs in logical order, clipped to the array's window. A struct enumerator that returns itself
    /// from <c>GetEnumerator</c>, so a <c>foreach</c> over it allocates nothing.
    /// </summary>
    public struct RunEnumerator
    {
        private readonly IArrowArray _runEnds;
        private readonly int _runCount;
        private readonly int _windowStart;
        private readonly int _windowEnd;
        private int _physical;
        private long _previousEnd;
        private Run _current;

        internal RunEnumerator(RunEndEncodedArray array)
        {
            _runEnds = array.RunEnds;
            _runCount = array.Values.Length;
            _windowStart = array.Data.Offset;
            _windowEnd = array.Data.Offset + array.Length;

            // Where the window starts, not where the runs do: a sliced array's first exposed row can sit
            // anywhere inside any run. FindPhysicalIndex is the binary search that locates it, and it reads
            // past the end for an empty array, which is why that case skips it.
            _physical = array.Length == 0 ? _runCount : array.FindPhysicalIndex(0);
            _previousEnd = _physical == 0 ? 0 : RunEndAt(_runEnds, _physical - 1);
            _current = default;
        }

        public readonly Run Current => _current;

        public readonly RunEnumerator GetEnumerator() => this;

        public bool MoveNext()
        {
            while (_physical < _runCount && _previousEnd < _windowEnd)
            {
                long end = RunEndAt(_runEnds, _physical);
                int start = (int)Math.Max(_previousEnd, _windowStart);
                int stop = (int)Math.Min(end, _windowEnd);
                _previousEnd = end;
                int physical = _physical++;

                // A run can fall entirely before the window (the first search lands on the run CONTAINING
                // the start, but a zero-length run would not), so an empty clip is skipped rather than
                // reported.
                if (stop > start)
                {
                    _current = new Run(physical, stop - start);
                    return true;
                }
            }

            return false;
        }
    }
}

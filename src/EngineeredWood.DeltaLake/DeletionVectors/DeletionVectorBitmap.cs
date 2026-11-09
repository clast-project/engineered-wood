// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Collections;
using System.Runtime.CompilerServices;
using EngineeredWood.Encodings;

namespace EngineeredWood.DeltaLake.DeletionVectors;

/// <summary>
/// The rows a deletion vector deletes from one data file, as absolute in-file positions. It keeps the
/// serialized roaring bitmap as it was read and decodes containers in place on demand: loading costs the
/// container headers (a few dozen bytes per 65,536 positions), not one entry per deleted row.
///
/// <para>A scan asks, per batch covering rows <c>[start, start + length)</c>, whether any row there is
/// deleted (<see cref="TryGetNext"/>) and, when one is, clears the deleted rows from a selection mask
/// (<see cref="ClearDeleted"/>). The mask uses Arrow's bitmap layout, so predicate masks over the same batch
/// combine with it word by word.</para>
///
/// <para>Instances are immutable and safe to share between threads.</para>
/// </summary>
public sealed class DeletionVectorBitmap : IEnumerable<long>, EngineeredWood.Parquet.IExcludedRows
{
    private const int ContainerSpan = 1 << 16;
    private const uint MaxHighKey = int.MaxValue - 1;

    private readonly byte[] _data;
    private readonly long[] _base;      // absolute position of each container's value 0, strictly ascending
    private readonly int[] _offset;     // payload offset in _data
    private readonly int[] _length;     // array values / runs / 1024 bitmap words
    private readonly RoaringContainerKind[] _kind;
    private readonly int _serializedLength; // the RoaringBitmapArray's length in _data, from offset 0

    /// <summary>A deletion vector that deletes nothing.</summary>
    public static DeletionVectorBitmap Empty { get; } = new(
        DeletionVectorBuilder.EmptyBlob(), [], [], [], [], 0, DeletionVectorBuilder.EmptyBlobLength);

    private DeletionVectorBitmap(
        byte[] data, long[] bases, int[] offsets, int[] lengths, RoaringContainerKind[] kinds, long count,
        int serializedLength)
    {
        _data = data;
        _serializedLength = serializedLength;
        _base = bases;
        _offset = offsets;
        _length = lengths;
        _kind = kinds;
        Count = count;
    }

    /// <summary>How many rows are deleted.</summary>
    public long Count { get; }

    /// <summary>True when no row is deleted.</summary>
    public bool IsEmpty => Count == 0;

    /// <summary>
    /// Builds the view over a RoaringBitmapArray (Delta's 64-bit form, after the 4-byte magic) whose sub-bitmaps
    /// start at <paramref name="position"/> in <paramref name="data"/>. Takes ownership of
    /// <paramref name="data"/>: it is read in place for the instance's lifetime and must not change.
    /// </summary>
    internal static DeletionVectorBitmap Parse(byte[] data, int position, long subBitmaps)
    {
        var containers = new List<RoaringContainer>();
        var bases = new List<long>();
        var offsets = new List<int>();
        var lengths = new List<int>();
        var kinds = new List<RoaringContainerKind>();
        long count = 0;
        long previousHigh = -1;
        for (long s = 0; s < subBitmaps; s++)
        {
            if (data.Length - position < 4)
                throw Invalid("Deletion vector truncated: missing sub-bitmap key.");
            uint key = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(position, 4));
            position += 4;
            // Delta's RoaringBitmapArray tops out at high key int.MaxValue - 1 (MAX_REPRESENTABLE_VALUE is
            // composeFromHighLowBytes(Int.MaxValue - 1, Int.MinValue)), so no position here comes within 2^32 of
            // long.MaxValue: that value is free to mean "none" below, and a window that runs off the end of the
            // long range holds no deleted row. Ascending keys make the container bases ascend, which every
            // lookup's binary search relies on.
            if (key > MaxHighKey || key <= previousHigh)
                throw Invalid($"Deletion vector sub-bitmap key {key} is out of range or out of order.");
            previousHigh = key;

            containers.Clear();
            int consumed;
            try
            {
                consumed = RoaringBitmap.ParseContainers(
                    data.AsSpan(position), RoaringBitmap.RoaringFormat.CRoaring, containers);
            }
            catch (InvalidDataException ex)
            {
                throw new DeltaFormatException(
                    DeltaErrorCodes.InvalidDeletionVector,
                    $"Deletion vector sub-bitmap could not be decoded: {ex.Message}", ex);
            }
            if (consumed <= 0)
                throw Invalid("Deletion vector sub-bitmap could not be decoded.");

            long high = (long)key << 32;
            foreach (var c in containers)
            {
                bases.Add(high | ((long)c.Key << 16));
                offsets.Add(position + c.Offset);
                lengths.Add(c.Length);
                kinds.Add(c.Kind);
                count += c.Cardinality;
            }
            position += consumed;
        }

        return count == 0
            ? Empty
            : new DeletionVectorBitmap(
                data, bases.ToArray(), offsets.ToArray(), lengths.ToArray(), kinds.ToArray(), count, position);
    }

    /// <summary>
    /// The deletion vector deleting exactly <paramref name="positions"/> (in any order; duplicates count once).
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">A position is negative or past the largest one a Delta
    /// deletion vector can hold.</exception>
    public static DeletionVectorBitmap FromPositions(IEnumerable<long> positions)
    {
        if (positions is null)
            throw new ArgumentNullException(nameof(positions));
        return DeletionVectorBuilder.Build(null, DeletionVectorBuilder.SortedDistinct(positions, nameof(positions)), null);
    }

    /// <summary>
    /// The deletion vector deleting every row this one does and <paramref name="positions"/> too (in any order;
    /// duplicates, and rows already deleted, count once). This instance is unchanged.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">A position is negative or past the largest one a Delta
    /// deletion vector can hold.</exception>
    public DeletionVectorBitmap Union(IEnumerable<long> positions)
    {
        if (positions is null)
            throw new ArgumentNullException(nameof(positions));
        return DeletionVectorBuilder.Build(this, DeletionVectorBuilder.SortedDistinct(positions, nameof(positions)), null);
    }

    /// <summary>The rows this vector deletes and <paramref name="other"/> does not. This instance is
    /// unchanged.</summary>
    public DeletionVectorBitmap Except(DeletionVectorBitmap other)
    {
        if (other is null)
            throw new ArgumentNullException(nameof(other));
        return DeletionVectorBuilder.Difference(this, other);
    }

    /// <summary>
    /// As <see cref="Union(IEnumerable{long})"/> for positions already ascending and distinct, appending to
    /// <paramref name="newlyDeleted"/> (when given), in ascending order, each one this vector did not already
    /// delete.
    /// </summary>
    internal DeletionVectorBitmap Union(ReadOnlySpan<long> ascendingDistinct, List<long>? newlyDeleted) =>
        DeletionVectorBuilder.Build(this, ascendingDistinct, newlyDeleted);

    /// <summary>
    /// The serialized RoaringBitmapArray (magic, sub-bitmap count, sub-bitmaps), as a deletion vector's storage
    /// holds it. Shares this instance's buffer when it is exactly that long; callers must not change it.
    /// </summary>
    internal byte[] SerializedBytes() =>
        _data.Length == _serializedLength ? _data : _data.AsSpan(0, _serializedLength).ToArray();

    // ---- container access, for DeletionVectorBuilder ----

    internal int ContainerCount => _base.Length;

    internal long ContainerBase(int c) => _base[c];

    /// <summary>Sets, in <paramref name="words"/> (1024 words, bit <c>v</c> = container value <c>v</c>), every
    /// value container <paramref name="c"/> holds.</summary>
    internal void OrContainerInto(int c, Span<ulong> words)
    {
        int off = _offset[c];
        switch (_kind[c])
        {
            case RoaringContainerKind.Array:
                for (int i = 0; i < _length[c]; i++)
                {
                    int v = U16(off + 2 * i);
                    words[v >> 6] |= 1UL << (v & 63);
                }
                break;
            case RoaringContainerKind.Run:
                for (int r = 0; r < _length[c]; r++)
                {
                    int s = U16(off + 4 * r);
                    Arrow.SelectionMask.SetRange(words, s, s + U16(off + 4 * r + 2) + 1);
                }
                break;
            default:
                for (int w = 0; w < 1024; w++)
                    words[w] |= U64(off + 8 * w);
                break;
        }
    }

    /// <summary>True when row <paramref name="position"/> is deleted.</summary>
    public bool Contains(long position) =>
        position >= 0 && TryGetNext(position, out long next) && next == position;

    /// <summary>
    /// The first deleted row at or after <paramref name="position"/>. Returns false when there is none.
    /// </summary>
    public bool TryGetNext(long position, out long next)
    {
        next = NextAtOrAfter(position < 0 ? 0 : position);
        return next != long.MaxValue;
    }

    /// <summary>True when any row in [<paramref name="start"/>, <paramref name="start"/> +
    /// <paramref name="length"/>) is deleted.</summary>
    public bool Intersects(long start, long length)
    {
        if (start < 0)
        {
            // No row is negative: keep only the window's non-negative part. (start + length cannot overflow
            // here, and next - start could.)
            length += start;
            start = 0;
        }
        return length > 0 && TryGetNext(start, out long next) && next - start < length;
    }

    /// <summary>
    /// True when EVERY row in [<paramref name="start"/>, <paramref name="start"/> + <paramref name="length"/>) is
    /// deleted, so a reader can skip those rows without reading them. An empty window is trivially covered; a
    /// non-empty one reaching below row 0 never is, since no negative row is deleted.
    /// </summary>
    /// <remarks>Stops at the first row that is not deleted: a run container answers from its run bounds, a bitmap
    /// container from whole words, and an array container from its first and last value in the window (its values
    /// are sorted and distinct, so the window is covered exactly when they are consecutive).</remarks>
    public bool ContainsRange(long start, long length)
    {
        if (length <= 0)
            return true;
        if (start < 0 || length > Count)
            return false;

        // length <= Count keeps this far from overflow in practice; clamp anyway rather than wrap.
        long end = length > long.MaxValue - start ? long.MaxValue : start + length;
        long cursor = start; // the first row not yet shown deleted
        for (int c = FirstContainerEndingAfter(start); c < _base.Length; c++)
        {
            long b = _base[c];
            if (b > cursor)
                return false; // no container holds row `cursor`
            int lo = (int)(cursor - b);
            int hi = end - b >= ContainerSpan ? ContainerSpan : (int)(end - b);
            if (!ContainerCovers(c, lo, hi))
                return false;
            cursor = b + hi;
            if (cursor >= end)
                return true;
        }
        return false;
    }

    // Whether container c holds every value of [lo, hi), lo < hi.
    private bool ContainerCovers(int c, int lo, int hi)
    {
        int off = _offset[c];
        switch (_kind[c])
        {
            case RoaringContainerKind.Array:
            {
                int n = _length[c], need = hi - lo;
                int i = LowerBound(off, n, lo);
                return i + need <= n && U16(off + 2 * i) == lo && U16(off + 2 * (i + need - 1)) == hi - 1;
            }
            case RoaringContainerKind.Run:
            {
                // Touching runs are valid input, so the window may take several of them back to back.
                int runs = _length[c], cursor = lo;
                for (int r = FirstRunEndingAtOrAfter(off, runs, lo); r < runs; r++)
                {
                    int s = U16(off + 4 * r);
                    if (s > cursor)
                        return false;
                    int e = s + U16(off + 4 * r + 2) + 1; // exclusive
                    if (e >= hi)
                        return true;
                    cursor = e;
                }
                return false;
            }
            default:
            {
                int first = lo >> 6, last = (hi - 1) >> 6;
                for (int w = first; w <= last; w++)
                {
                    ulong mask = ulong.MaxValue;
                    if (w == first)
                        mask &= ulong.MaxValue << (lo & 63);
                    if (w == last)
                        mask &= ulong.MaxValue >> (63 - ((hi - 1) & 63));
                    if ((U64(off + 8 * w) & mask) != mask)
                        return false;
                }
                return true;
            }
        }
    }

    /// <summary>
    /// Deselects, in <paramref name="selection"/>, every deleted row of [<paramref name="start"/>,
    /// <paramref name="start"/> + <paramref name="length"/>). Bit <c>i</c> of the selection (LSB-first, word
    /// <c>i / 64</c>, Arrow's bitmap layout) stands for row <c>start + i</c>; bits for rows that are not deleted
    /// are left as they are, so the call composes with other filters applied to the same selection.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="start"/> or <paramref name="length"/> is
    /// negative, or <paramref name="selection"/> is shorter than <paramref name="length"/> bits.</exception>
    public void ClearDeleted(long start, int length, Span<ulong> selection)
    {
        if (start < 0)
            throw new ArgumentOutOfRangeException(nameof(start), start, "A row position is never negative.");
        if (length < 0 || selection.Length < (length + 63L) >> 6)
            throw new ArgumentOutOfRangeException(nameof(length), length, "The selection must hold one bit per row.");

        long end = start + length;
        for (int c = FirstContainerEndingAfter(start); c < _base.Length && _base[c] < end; c++)
        {
            long b = _base[c];
            int lo = start <= b ? 0 : (int)(start - b);                  // container-relative, inclusive
            int hi = end - b >= ContainerSpan ? ContainerSpan : (int)(end - b); // exclusive
            long shift = b - start;                                      // selection index of the container's 0
            int off = _offset[c];
            switch (_kind[c])
            {
                case RoaringContainerKind.Array:
                {
                    int n = _length[c];
                    for (int i = LowerBound(off, n, lo); i < n; i++)
                    {
                        int v = U16(off + 2 * i);
                        if (v >= hi)
                            break;
                        long k = shift + v;
                        selection[(int)(k >> 6)] &= ~(1UL << (int)(k & 63));
                    }
                    break;
                }
                case RoaringContainerKind.Run:
                {
                    int runs = _length[c];
                    for (int r = FirstRunEndingAtOrAfter(off, runs, lo); r < runs; r++)
                    {
                        int s = U16(off + 4 * r);
                        if (s >= hi)
                            break;
                        int e = s + U16(off + 4 * r + 2) + 1; // exclusive
                        Arrow.SelectionMask.ClearRange(
                            selection, (int)(shift + Math.Max(s, lo)), (int)(shift + Math.Min(e, hi)));
                    }
                    break;
                }
                default:
                {
                    int first = (int)(shift + lo), last = (int)(shift + hi); // selection bits [first, last)
                    for (int w = first >> 6; w <= (last - 1) >> 6; w++)
                    {
                        // Container bit standing for selection word w's bit 0.
                        long bit = ((long)w << 6) - shift;
                        selection[w] &= ~ContainerBits(off, bit, hi);
                    }
                    break;
                }
            }
        }
    }

    /// <summary>The deleted rows in ascending order.</summary>
    /// <summary>A Parquet read's excluded rows: the rows this deletes (<see cref="ContainsRange"/>).</summary>
    bool EngineeredWood.Parquet.IExcludedRows.ExcludesAll(long firstRow, long rowCount) => ContainsRange(firstRow, rowCount);

    /// <summary>A Parquet read's excluded rows: clears this vector's deleted rows (<see cref="ClearDeleted"/>).</summary>
    void EngineeredWood.Parquet.IExcludedRows.ClearExcluded(long firstRow, int rowCount, Span<ulong> selection) =>
        ClearDeleted(firstRow, rowCount, selection);

    public IEnumerator<long> GetEnumerator()
    {
        for (int c = 0; c < _base.Length; c++)
        {
            long b = _base[c];
            int off = _offset[c];
            switch (_kind[c])
            {
                case RoaringContainerKind.Array:
                    for (int i = 0; i < _length[c]; i++)
                        yield return b + U16(off + 2 * i);
                    break;
                case RoaringContainerKind.Run:
                    for (int r = 0; r < _length[c]; r++)
                    {
                        int s = U16(off + 4 * r), last = s + U16(off + 4 * r + 2);
                        for (int v = s; v <= last; v++)
                            yield return b + v;
                    }
                    break;
                default:
                    for (int w = 0; w < 1024; w++)
                    {
                        ulong bits = U64(off + 8 * w);
                        while (bits != 0)
                        {
                            yield return b + (w << 6) + System.Numerics.BitOperations.TrailingZeroCount(bits);
                            bits &= bits - 1;
                        }
                    }
                    break;
            }
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private long NextAtOrAfter(long position)
    {
        for (int c = FirstContainerEndingAfter(position); c < _base.Length; c++)
        {
            long b = _base[c];
            int rel = position <= b ? 0 : (int)(position - b);
            int off = _offset[c];
            switch (_kind[c])
            {
                case RoaringContainerKind.Array:
                {
                    int i = LowerBound(off, _length[c], rel);
                    if (i < _length[c])
                        return b + U16(off + 2 * i);
                    break;
                }
                case RoaringContainerKind.Run:
                {
                    int r = FirstRunEndingAtOrAfter(off, _length[c], rel);
                    if (r < _length[c])
                        return b + Math.Max(U16(off + 4 * r), rel);
                    break;
                }
                default:
                {
                    for (int w = rel >> 6; w < 1024; w++)
                    {
                        ulong bits = U64(off + 8 * w);
                        if (w == rel >> 6)
                            bits &= ulong.MaxValue << (rel & 63);
                        if (bits != 0)
                            return b + (w << 6) + System.Numerics.BitOperations.TrailingZeroCount(bits);
                    }
                    break;
                }
            }
        }
        return long.MaxValue;
    }

    // 64 bits of a bitmap container starting at container bit `bit`, keeping only bits below `hi`.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ulong ContainerBits(int off, long bit, int hi)
    {
        long word = bit >> 6;
        int sh = (int)(bit & 63);
        ulong low = word >= 0 && word < 1024 ? U64(off + 8 * (int)word) : 0;
        ulong value = low;
        if (sh != 0)
        {
            ulong high = word + 1 >= 0 && word + 1 < 1024 ? U64(off + 8 * (int)(word + 1)) : 0;
            value = (low >> sh) | (high << (64 - sh));
        }
        // Result bit j is container bit (bit + j); keep j with bit + j < hi. Nothing below lo needs masking:
        // ClearDeleted's first word starts either at lo (a window opening inside the container) or before the
        // container's bit 0, and bits before bit 0 read as zero above.
        long to = hi - bit;
        if (to < 64)
            value &= to <= 0 ? 0 : ulong.MaxValue >> (int)(64 - to);
        return value;
    }

    private int FirstContainerEndingAfter(long position)
    {
        int lo = 0, hi = _base.Length;
        while (lo < hi)
        {
            int mid = (int)((uint)(lo + hi) >> 1);
            // position - base, not base + span: the last possible base is long.MaxValue - 65535, and adding the span
            // to it would wrap negative and break the search's ordering. Both are non-negative, so this cannot.
            if (position - _base[mid] >= ContainerSpan) lo = mid + 1; else hi = mid;
        }
        return lo;
    }

    // First array value >= `value`.
    private int LowerBound(int off, int count, int value)
    {
        int lo = 0, hi = count;
        while (lo < hi)
        {
            int mid = (int)((uint)(lo + hi) >> 1);
            if (U16(off + 2 * mid) < value) lo = mid + 1; else hi = mid;
        }
        return lo;
    }

    // First run whose last value is >= `value` (runs are sorted and disjoint, so their ends ascend too).
    private int FirstRunEndingAtOrAfter(int off, int runs, int value)
    {
        int lo = 0, hi = runs;
        while (lo < hi)
        {
            int mid = (int)((uint)(lo + hi) >> 1);
            if (U16(off + 4 * mid) + U16(off + 4 * mid + 2) < value) lo = mid + 1; else hi = mid;
        }
        return lo;
    }

    // Little-endian reads; ParseContainers has bounds-checked every payload.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int U16(int at) => _data[at] | (_data[at + 1] << 8);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ulong U64(int at) => System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(_data.AsSpan(at, 8));

    private static DeltaFormatException Invalid(string message) =>
        new(DeltaErrorCodes.InvalidDeletionVector, message);
}

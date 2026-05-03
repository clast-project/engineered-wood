// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Computes per-column statistics that the writer attaches to the top-level
/// ArrayNode via <see cref="ArrayStatsEmitter"/>.
///
/// <para>Stats covered (all cheap — at most one O(n) scan each):
/// <list type="bullet">
///   <item><c>null_count</c> — free (Apache.Arrow caches it).</item>
///   <item><c>is_constant</c> — primitive only, no nulls; raw byte-chunk equality.</item>
///   <item><c>is_sorted / is_strict_sorted</c> — integer primitives only, no nulls;
///     typed CompareTo. Floats skipped — NaN ordering gets ugly.</item>
///   <item><c>nan_count</c> — Float32/Float64 only.</item>
/// </list></para>
///
/// <para>Min/Max/Sum require a ScalarValue protobuf serializer (deferred).</para>
/// </summary>
internal static class ArrayStatsComputer
{
    public static ArrayStatsValues Compute(IArrowArray array)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        var values = new ArrayStatsValues();

        int nullCount = data.GetNullCount();
        if (nullCount > 0) values.NullCount = (ulong)nullCount;

        if (array.Length > 0 && nullCount == 0)
        {
            int? elemSize = ElementSize(array);
            if (elemSize is int sz)
            {
                int byteOffset = data.Offset * sz;
                var span = data.Buffers[1].Span.Slice(byteOffset, array.Length * sz);
                values.IsConstant = AllChunksEqual(span, sz);
            }

            ComputeIntOrdering(array, ref values);
        }

        ComputeNanCount(array, ref values);
        ComputeMinMax(array, ref values);
        ComputeSum(array, ref values);

        return values;
    }

    /// <summary>
    /// Returns the per-element byte size for primitive Arrow arrays we know how
    /// to scan. Null for non-primitive types (string, binary, list, fsl).
    /// </summary>
    private static int? ElementSize(IArrowArray array) => array switch
    {
        Int8Array or UInt8Array => 1,
        Int16Array or UInt16Array => 2,
        Int32Array or UInt32Array or FloatArray => 4,
        Int64Array or UInt64Array or DoubleArray => 8,
        _ => null,
    };

    private static bool AllChunksEqual(ReadOnlySpan<byte> span, int elemSize)
    {
        if (span.Length <= elemSize) return true;
        var first = span.Slice(0, elemSize);
        for (int pos = elemSize; pos < span.Length; pos += elemSize)
            if (!span.Slice(pos, elemSize).SequenceEqual(first))
                return false;
        return true;
    }

    /// <summary>
    /// Sets <c>IsSorted</c> and <c>IsStrictSorted</c> for integer primitives.
    /// Caller has guaranteed <c>nullCount == 0 && rowCount > 0</c>. Floats are
    /// intentionally excluded (NaN handling).
    /// </summary>
    private static void ComputeIntOrdering(IArrowArray array, ref ArrayStatsValues values)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        if (n <= 1)
        {
            // Vacuously sorted: 0 or 1 element has no pair to violate either flag.
            values.IsSorted = true;
            values.IsStrictSorted = true;
            return;
        }

        // Buffers[1] only exists for primitive types — FSL/List/Struct have no
        // value buffer, so look it up inside each case to avoid an OOB on
        // arrays without one.
        bool sorted, strict;
        switch (array)
        {
            case Int8Array: ScanOrdering<sbyte>(data.Buffers[1].Span.Slice(data.Offset, n), n, out sorted, out strict); break;
            case UInt8Array: ScanOrdering<byte>(data.Buffers[1].Span.Slice(data.Offset, n), n, out sorted, out strict); break;
            case Int16Array: ScanOrdering<short>(data.Buffers[1].Span.Slice(data.Offset * 2, n * 2), n, out sorted, out strict); break;
            case UInt16Array: ScanOrdering<ushort>(data.Buffers[1].Span.Slice(data.Offset * 2, n * 2), n, out sorted, out strict); break;
            case Int32Array: ScanOrdering<int>(data.Buffers[1].Span.Slice(data.Offset * 4, n * 4), n, out sorted, out strict); break;
            case UInt32Array: ScanOrdering<uint>(data.Buffers[1].Span.Slice(data.Offset * 4, n * 4), n, out sorted, out strict); break;
            case Int64Array: ScanOrdering<long>(data.Buffers[1].Span.Slice(data.Offset * 8, n * 8), n, out sorted, out strict); break;
            case UInt64Array: ScanOrdering<ulong>(data.Buffers[1].Span.Slice(data.Offset * 8, n * 8), n, out sorted, out strict); break;
            default: return; // Float, Double, list, FSL, varbin — skip ordering
        }
        values.IsSorted = sorted;
        values.IsStrictSorted = strict;
    }

    /// <summary>Scans adjacent pairs via <see cref="IComparable{T}.CompareTo"/>.</summary>
    private static void ScanOrdering<T>(ReadOnlySpan<byte> bytes, int n, out bool sorted, out bool strict)
        where T : unmanaged, IComparable<T>
    {
        var span = MemoryMarshal.Cast<byte, T>(bytes);
        sorted = true;
        strict = true;
        for (int i = 1; i < n; i++)
        {
            int cmp = span[i - 1].CompareTo(span[i]);
            if (cmp > 0) { sorted = false; strict = false; return; }
            if (cmp == 0) strict = false;
        }
    }

    /// <summary>
    /// Sets <c>NanCount</c> for Float32/Float64 columns. Counts NaNs only at
    /// non-null positions; columns with nulls still get a count over the
    /// visible-and-valid rows.
    /// </summary>
    private static void ComputeNanCount(IArrowArray array, ref ArrayStatsValues values)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        if (n == 0) return;

        ulong nan;
        switch (array)
        {
            case FloatArray:
                {
                    var span = MemoryMarshal.Cast<byte, float>(
                        data.Buffers[1].Span.Slice(data.Offset * 4, n * 4));
                    nan = CountNaN<float>(span, data, n, static v => float.IsNaN(v));
                    break;
                }
            case DoubleArray:
                {
                    var span = MemoryMarshal.Cast<byte, double>(
                        data.Buffers[1].Span.Slice(data.Offset * 8, n * 8));
                    nan = CountNaN<double>(span, data, n, static v => double.IsNaN(v));
                    break;
                }
            default: return;
        }
        values.NanCount = nan;
    }

    private static ulong CountNaN<T>(ReadOnlySpan<T> values, ArrayData data, int n, Func<T, bool> isNaN)
        where T : unmanaged
    {
        ulong nan = 0;
        bool hasNulls = data.GetNullCount() > 0;
        var bitmap = hasNulls ? data.Buffers[0].Span : default;
        for (int i = 0; i < n; i++)
        {
            if (hasNulls)
            {
                int globalBit = data.Offset + i;
                if ((bitmap[globalBit >> 3] & (1 << (globalBit & 7))) == 0) continue;
            }
            if (isNaN(values[i])) nan++;
        }
        return nan;
    }

    /// <summary>
    /// Computes <c>MinBytes</c> + <c>MaxBytes</c> (and the matching Precision
    /// bytes set to Exact=1) for primitive numeric arrays. Skips null positions
    /// when a validity bitmap is present. Floats skip NaN values entirely.
    /// </summary>
    private static void ComputeMinMax(IArrowArray array, ref ArrayStatsValues values)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        if (n == 0) return;

        switch (array)
        {
            case Int8Array:
                MinMaxSigned(MemoryMarshal.Cast<byte, sbyte>(
                    data.Buffers[1].Span.Slice(data.Offset, n)), data, n, ref values);
                break;
            case Int16Array:
                MinMaxSigned(MemoryMarshal.Cast<byte, short>(
                    data.Buffers[1].Span.Slice(data.Offset * 2, n * 2)), data, n, ref values);
                break;
            case Int32Array:
                MinMaxSigned(MemoryMarshal.Cast<byte, int>(
                    data.Buffers[1].Span.Slice(data.Offset * 4, n * 4)), data, n, ref values);
                break;
            case Int64Array:
                MinMaxSigned(MemoryMarshal.Cast<byte, long>(
                    data.Buffers[1].Span.Slice(data.Offset * 8, n * 8)), data, n, ref values);
                break;
            case UInt8Array:
                MinMaxUnsigned(data.Buffers[1].Span.Slice(data.Offset, n), data, n, ref values);
                break;
            case UInt16Array:
                MinMaxUnsigned(MemoryMarshal.Cast<byte, ushort>(
                    data.Buffers[1].Span.Slice(data.Offset * 2, n * 2)), data, n, ref values);
                break;
            case UInt32Array:
                MinMaxUnsigned(MemoryMarshal.Cast<byte, uint>(
                    data.Buffers[1].Span.Slice(data.Offset * 4, n * 4)), data, n, ref values);
                break;
            case UInt64Array:
                MinMaxUnsigned(MemoryMarshal.Cast<byte, ulong>(
                    data.Buffers[1].Span.Slice(data.Offset * 8, n * 8)), data, n, ref values);
                break;
            case FloatArray:
                MinMaxFloat32(MemoryMarshal.Cast<byte, float>(
                    data.Buffers[1].Span.Slice(data.Offset * 4, n * 4)), data, n, ref values);
                break;
            case DoubleArray:
                MinMaxFloat64(MemoryMarshal.Cast<byte, double>(
                    data.Buffers[1].Span.Slice(data.Offset * 8, n * 8)), data, n, ref values);
                break;
            case BooleanArray b:
                MinMaxBool(b, ref values);
                break;
            case StringArray s:
                MinMaxVarBin(s, isString: true, ref values);
                break;
            case BinaryArray bn:
                MinMaxVarBin(bn, isString: false, ref values);
                break;
        }
    }

    private static void MinMaxBool(BooleanArray array, ref ArrayStatsValues values)
    {
        var data = array.Data;
        int n = array.Length;
        if (n == 0) return;

        bool hasNulls = data.GetNullCount() > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;
        var bits = data.Buffers[1].Span;

        bool anyTrue = false, anyFalse = false;
        for (int i = 0; i < n; i++)
        {
            int gb = data.Offset + i;
            if (hasNulls && (validity[gb >> 3] & (1 << (gb & 7))) == 0) continue;
            bool v = (bits[gb >> 3] & (1 << (gb & 7))) != 0;
            if (v) anyTrue = true; else anyFalse = true;
            // (Don't early-exit — we need both flags to know is_constant.)
        }
        if (!anyTrue && !anyFalse) return; // all null

        // min: false if any false present, else true. max: true if any true, else false.
        values.MinBytes = ScalarValueSerializer.FromBool(!anyFalse);
        values.MaxBytes = ScalarValueSerializer.FromBool(anyTrue);
        values.MinPrecision = 1;
        values.MaxPrecision = 1;

        // is_constant for bool: no nulls AND only one of (true, false) seen.
        // BooleanArray's packed-bitmap layout means it's excluded from the
        // generic byte-chunk is_constant path in Compute(), so set it here.
        if (!hasNulls)
            values.IsConstant = anyTrue ^ anyFalse;
    }

    private static void MinMaxVarBin(IArrowArray array, bool isString, ref ArrayStatsValues values)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        if (n == 0) return;

        bool hasNulls = data.GetNullCount() > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;
        var offsetsAll = data.Buffers[1].Span;
        var bytesAll = data.Buffers[2].Span;
        int offsetsByteOffset = data.Offset * 4;

        bool any = false;
        // Track current min/max as (start, length) into bytesAll.
        int minStart = 0, minLen = 0, maxStart = 0, maxLen = 0;
        for (int i = 0; i < n; i++)
        {
            int gb = data.Offset + i;
            if (hasNulls && (validity[gb >> 3] & (1 << (gb & 7))) == 0) continue;

            int s = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(
                offsetsAll.Slice(offsetsByteOffset + i * 4, 4));
            int e = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(
                offsetsAll.Slice(offsetsByteOffset + (i + 1) * 4, 4));
            var v = bytesAll.Slice(s, e - s);

            if (!any)
            {
                minStart = s; minLen = e - s; maxStart = s; maxLen = e - s; any = true;
            }
            else
            {
                if (v.SequenceCompareTo(bytesAll.Slice(minStart, minLen)) < 0)
                {
                    minStart = s; minLen = e - s;
                }
                if (v.SequenceCompareTo(bytesAll.Slice(maxStart, maxLen)) > 0)
                {
                    maxStart = s; maxLen = e - s;
                }
            }
        }
        if (!any) return;

        var minPayload = bytesAll.Slice(minStart, minLen);
        var maxPayload = bytesAll.Slice(maxStart, maxLen);
        values.MinBytes = isString
            ? ScalarValueSerializer.FromString(minPayload)
            : ScalarValueSerializer.FromBytes(minPayload);
        values.MaxBytes = isString
            ? ScalarValueSerializer.FromString(maxPayload)
            : ScalarValueSerializer.FromBytes(maxPayload);
        values.MinPrecision = 1;
        values.MaxPrecision = 1;
    }

    private static void MinMaxSigned<T>(ReadOnlySpan<T> span, ArrayData data, int n, ref ArrayStatsValues values)
        where T : unmanaged, IComparable<T>, IConvertible
    {
        bool hasNulls = data.GetNullCount() > 0;
        var bitmap = hasNulls ? data.Buffers[0].Span : default;
        bool any = false;
        T min = default!, max = default!;
        for (int i = 0; i < n; i++)
        {
            if (hasNulls)
            {
                int gb = data.Offset + i;
                if ((bitmap[gb >> 3] & (1 << (gb & 7))) == 0) continue;
            }
            T v = span[i];
            if (!any) { min = max = v; any = true; }
            else
            {
                if (v.CompareTo(min) < 0) min = v;
                if (v.CompareTo(max) > 0) max = v;
            }
        }
        if (!any) return;
        values.MinBytes = ScalarValueSerializer.FromSignedInt(Convert.ToInt64(min));
        values.MaxBytes = ScalarValueSerializer.FromSignedInt(Convert.ToInt64(max));
        values.MinPrecision = 1;
        values.MaxPrecision = 1;
    }

    private static void MinMaxUnsigned<T>(ReadOnlySpan<T> span, ArrayData data, int n, ref ArrayStatsValues values)
        where T : unmanaged, IComparable<T>, IConvertible
    {
        bool hasNulls = data.GetNullCount() > 0;
        var bitmap = hasNulls ? data.Buffers[0].Span : default;
        bool any = false;
        T min = default!, max = default!;
        for (int i = 0; i < n; i++)
        {
            if (hasNulls)
            {
                int gb = data.Offset + i;
                if ((bitmap[gb >> 3] & (1 << (gb & 7))) == 0) continue;
            }
            T v = span[i];
            if (!any) { min = max = v; any = true; }
            else
            {
                if (v.CompareTo(min) < 0) min = v;
                if (v.CompareTo(max) > 0) max = v;
            }
        }
        if (!any) return;
        values.MinBytes = ScalarValueSerializer.FromUnsignedInt(Convert.ToUInt64(min));
        values.MaxBytes = ScalarValueSerializer.FromUnsignedInt(Convert.ToUInt64(max));
        values.MinPrecision = 1;
        values.MaxPrecision = 1;
    }

    private static void MinMaxFloat32(ReadOnlySpan<float> span, ArrayData data, int n, ref ArrayStatsValues values)
    {
        bool hasNulls = data.GetNullCount() > 0;
        var bitmap = hasNulls ? data.Buffers[0].Span : default;
        bool any = false;
        float min = 0, max = 0;
        for (int i = 0; i < n; i++)
        {
            if (hasNulls)
            {
                int gb = data.Offset + i;
                if ((bitmap[gb >> 3] & (1 << (gb & 7))) == 0) continue;
            }
            float v = span[i];
            if (float.IsNaN(v)) continue;
            if (!any) { min = max = v; any = true; }
            else { if (v < min) min = v; if (v > max) max = v; }
        }
        if (!any) return;
        values.MinBytes = ScalarValueSerializer.FromFloat32(min);
        values.MaxBytes = ScalarValueSerializer.FromFloat32(max);
        values.MinPrecision = 1;
        values.MaxPrecision = 1;
    }

    /// <summary>
    /// Computes <c>SumBytes</c> for primitive numeric arrays. Skips null
    /// positions (validity bitmap). Floats skip NaN. Integer accumulation is
    /// unchecked — silently overflows per C# unchecked semantics, matching
    /// the convention "the sum has the same numeric domain as the elements
    /// and may wrap." Empty arrays and all-null arrays produce no sum.
    /// </summary>
    private static void ComputeSum(IArrowArray array, ref ArrayStatsValues values)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        if (n == 0) return;

        switch (array)
        {
            case Int8Array:
                values.SumBytes = SumSigned(MemoryMarshal.Cast<byte, sbyte>(
                    data.Buffers[1].Span.Slice(data.Offset, n)), data, n,
                    static v => (long)v);
                break;
            case Int16Array:
                values.SumBytes = SumSigned(MemoryMarshal.Cast<byte, short>(
                    data.Buffers[1].Span.Slice(data.Offset * 2, n * 2)), data, n,
                    static v => (long)v);
                break;
            case Int32Array:
                values.SumBytes = SumSigned(MemoryMarshal.Cast<byte, int>(
                    data.Buffers[1].Span.Slice(data.Offset * 4, n * 4)), data, n,
                    static v => (long)v);
                break;
            case Int64Array:
                values.SumBytes = SumSigned(MemoryMarshal.Cast<byte, long>(
                    data.Buffers[1].Span.Slice(data.Offset * 8, n * 8)), data, n,
                    static v => v);
                break;
            case UInt8Array:
                values.SumBytes = SumUnsigned(data.Buffers[1].Span.Slice(data.Offset, n), data, n,
                    static v => (ulong)v);
                break;
            case UInt16Array:
                values.SumBytes = SumUnsigned(MemoryMarshal.Cast<byte, ushort>(
                    data.Buffers[1].Span.Slice(data.Offset * 2, n * 2)), data, n,
                    static v => (ulong)v);
                break;
            case UInt32Array:
                values.SumBytes = SumUnsigned(MemoryMarshal.Cast<byte, uint>(
                    data.Buffers[1].Span.Slice(data.Offset * 4, n * 4)), data, n,
                    static v => (ulong)v);
                break;
            case UInt64Array:
                values.SumBytes = SumUnsigned(MemoryMarshal.Cast<byte, ulong>(
                    data.Buffers[1].Span.Slice(data.Offset * 8, n * 8)), data, n,
                    static v => v);
                break;
            case FloatArray:
                values.SumBytes = SumFloat32(MemoryMarshal.Cast<byte, float>(
                    data.Buffers[1].Span.Slice(data.Offset * 4, n * 4)), data, n);
                break;
            case DoubleArray:
                values.SumBytes = SumFloat64(MemoryMarshal.Cast<byte, double>(
                    data.Buffers[1].Span.Slice(data.Offset * 8, n * 8)), data, n);
                break;
        }
    }

    private static byte[]? SumSigned<T>(
        ReadOnlySpan<T> span, ArrayData data, int n, Func<T, long> widen)
        where T : unmanaged
    {
        bool hasNulls = data.GetNullCount() > 0;
        var bitmap = hasNulls ? data.Buffers[0].Span : default;
        long sum = 0;
        bool any = false;
        for (int i = 0; i < n; i++)
        {
            if (hasNulls)
            {
                int gb = data.Offset + i;
                if ((bitmap[gb >> 3] & (1 << (gb & 7))) == 0) continue;
            }
            sum = unchecked(sum + widen(span[i]));
            any = true;
        }
        return any ? ScalarValueSerializer.FromSignedInt(sum) : null;
    }

    private static byte[]? SumUnsigned<T>(
        ReadOnlySpan<T> span, ArrayData data, int n, Func<T, ulong> widen)
        where T : unmanaged
    {
        bool hasNulls = data.GetNullCount() > 0;
        var bitmap = hasNulls ? data.Buffers[0].Span : default;
        ulong sum = 0;
        bool any = false;
        for (int i = 0; i < n; i++)
        {
            if (hasNulls)
            {
                int gb = data.Offset + i;
                if ((bitmap[gb >> 3] & (1 << (gb & 7))) == 0) continue;
            }
            sum = unchecked(sum + widen(span[i]));
            any = true;
        }
        return any ? ScalarValueSerializer.FromUnsignedInt(sum) : null;
    }

    private static byte[]? SumFloat32(ReadOnlySpan<float> span, ArrayData data, int n)
    {
        bool hasNulls = data.GetNullCount() > 0;
        var bitmap = hasNulls ? data.Buffers[0].Span : default;
        // Accumulate in double to reduce precision loss across many summands;
        // cast back to float at the end to match the input dtype.
        double sum = 0;
        bool any = false;
        for (int i = 0; i < n; i++)
        {
            if (hasNulls)
            {
                int gb = data.Offset + i;
                if ((bitmap[gb >> 3] & (1 << (gb & 7))) == 0) continue;
            }
            float v = span[i];
            if (float.IsNaN(v)) continue;
            sum += v;
            any = true;
        }
        return any ? ScalarValueSerializer.FromFloat32((float)sum) : null;
    }

    private static byte[]? SumFloat64(ReadOnlySpan<double> span, ArrayData data, int n)
    {
        bool hasNulls = data.GetNullCount() > 0;
        var bitmap = hasNulls ? data.Buffers[0].Span : default;
        double sum = 0;
        bool any = false;
        for (int i = 0; i < n; i++)
        {
            if (hasNulls)
            {
                int gb = data.Offset + i;
                if ((bitmap[gb >> 3] & (1 << (gb & 7))) == 0) continue;
            }
            double v = span[i];
            if (double.IsNaN(v)) continue;
            sum += v;
            any = true;
        }
        return any ? ScalarValueSerializer.FromFloat64(sum) : null;
    }

    private static void MinMaxFloat64(ReadOnlySpan<double> span, ArrayData data, int n, ref ArrayStatsValues values)
    {
        bool hasNulls = data.GetNullCount() > 0;
        var bitmap = hasNulls ? data.Buffers[0].Span : default;
        bool any = false;
        double min = 0, max = 0;
        for (int i = 0; i < n; i++)
        {
            if (hasNulls)
            {
                int gb = data.Offset + i;
                if ((bitmap[gb >> 3] & (1 << (gb & 7))) == 0) continue;
            }
            double v = span[i];
            if (double.IsNaN(v)) continue;
            if (!any) { min = max = v; any = true; }
            else { if (v < min) min = v; if (v > max) max = v; }
        }
        if (!any) return;
        values.MinBytes = ScalarValueSerializer.FromFloat64(min);
        values.MaxBytes = ScalarValueSerializer.FromFloat64(max);
        values.MinPrecision = 1;
        values.MaxPrecision = 1;
    }
}

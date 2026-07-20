// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.RunEndArrayDecoder"/>:
/// emits a <c>vortex.runend</c> ArrayNode subtree for primitive integer
/// columns with long runs of repeated values. The column is collapsed into
/// two parallel arrays:
/// <list type="bullet">
///   <item><c>ends[k]</c> — strictly-sorted unsigned integer giving the
///     one-past-the-last logical row index of run <c>k</c>.</item>
///   <item><c>values[k]</c> — the value held by run <c>k</c>; same Arrow type
///     as the input column.</item>
/// </list>
///
/// <para>Wire shape: 0 buffers, 2 children (ends, values), metadata
/// <c>RunEndMetadata { ends_ptype, num_runs, offset = 0 }</c>. Same vtable
/// shape as vortex.list / vortex.dict / fastlanes.delta — slots 0+1+2 (or
/// +4 with stats).</para>
///
/// <para>Both children are routed through the recursive dispatcher with
/// <c>compress: true</c>, so the ends typically land on bitpacked (small
/// monotonic ints) and values can pick whatever encoding suits the run
/// values' distribution.</para>
///
/// <para>Scope: nullable + non-nullable, sliced + non-sliced primitive
/// integer columns (Int8..Int64, UInt8..UInt64). For nullable inputs, runs
/// that span only null rows collapse into a single null run regardless of
/// the underlying (garbage) value bytes. The values child carries one
/// validity bit per run, and the reader expands that bitmap row-wise during
/// decode. Floats deferred — ALP and fastlanes.rle already cover them. Bool /
/// strings deferred until the reader's
/// <see cref="EngineeredWood.Vortex.Encodings.RunEndArrayDecoder.Expand"/>
/// grows matching cases.</para>
/// </summary>
internal static class RunEndArrayEncoder
{
    /// <summary>
    /// Profitability gate: count the runs in O(n) and accept only when
    /// <c>numRuns × (sizeof(ends) + sizeof(values)) × 1.5 &lt; n × sizeof(values)</c>.
    /// The 1.5× margin matches the gate used by ALP and fastlanes.rle so
    /// borderline columns don't pay the encoding overhead for marginal wins.
    /// </summary>
    public static bool IsApplicable(IArrowArray array)
    {
        if (array is null) return false;
        if (ElementSize(array) is not int elemSize) return false;
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        if (n < 2) return false; // 0/1-row columns: nothing to RLE; constant catches the 1-row case anyway.
        int nullCount = data.GetNullCount();
        if (nullCount == n) return false; // all-null — nothing to runend.

        int numRuns = CountRuns(array, elemSize);
        if (numRuns >= n) return false;

        int endsElemSize = SmallestUIntElemSize(n);
        // For nullable inputs the values child carries an extra validity buffer
        // (one bit per run, packed). Approximate as `numRuns / 8` bytes.
        long runendBytes = (long)numRuns * (endsElemSize + elemSize)
            + (nullCount > 0 ? (numRuns + 7) / 8 : 0);
        long rawBytes = (long)n * elemSize;
        return runendBytes * 3 / 2 < rawBytes;
    }

    public static int Emit(
        SegmentBuilder sb, IArrowArray array, EncodingIndices idx, int? statsTicket = null)
    {
        if (array is null) throw new ArgumentNullException(nameof(array));
        if (ElementSize(array) is not int elemSize)
            throw new NotSupportedException(
                $"vortex.runend writer doesn't support Arrow {array.GetType().Name}.");
        var data = ((Apache.Arrow.Array)array).Data;
        int n = array.Length;
        var (endsArr, valuesArr, endsPtype) = BuildRunEnds(array, elemSize, n);
        int numRuns = endsArr.Length;

        // Recursively encode children. Both can compress dramatically:
        //   - ends are strictly-sorted small unsigned ints → bitpacked or delta.
        //   - values may benefit from FoR / dict / etc., depending on the dist.
        int endsTicket = ArrayEncoderDispatch.Emit(sb, endsArr, idx, statsTicket: null, compress: true);
        int valuesTicket = ArrayEncoderDispatch.Emit(sb, valuesArr, idx, statsTicket: null, compress: true);

        var metadataBytes = SerializeMetadata(endsPtype, (ulong)numRuns);
        var metadataTicket = sb.Builder.WriteByteVector(metadataBytes);

        var children = new[] { endsTicket, valuesTicket };
        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithMetadataAndChildren(
                sb.Builder, idx.RunEnd, metadataTicket, children)
            : ArrayNodeEmitter.EmitWithMetadataChildrenAndStats(
                sb.Builder, idx.RunEnd, metadataTicket, children, statsTicket.Value);
    }

    private static int? ElementSize(IArrowArray array) => array switch
    {
        Int8Array or UInt8Array => 1,
        Int16Array or UInt16Array => 2,
        Int32Array or UInt32Array => 4,
        Int64Array or UInt64Array => 8,
        _ => null,
    };

    /// <summary>
    /// Smallest unsigned integer width (in bytes) that can hold values up to
    /// <paramref name="n"/>. The largest run-end value is <c>n</c> itself
    /// (the close of the final run), so we need a width that fits N — not
    /// N − 1. Note that vortex's PType ordering is <c>U8=0, U16=1, U32=2, U64=3</c>,
    /// matching the byte-width table.
    /// </summary>
    private static int SmallestUIntElemSize(int n)
    {
        if (n <= byte.MaxValue) return 1;
        if (n <= ushort.MaxValue) return 2;
        if ((uint)n <= uint.MaxValue) return 4;
        return 8;
    }

    private static byte EndsPtypeFor(int n) => SmallestUIntElemSize(n) switch
    {
        1 => 0, // U8
        2 => 1, // U16
        4 => 2, // U32
        _ => 3, // U64
    };

    /// <summary>
    /// Reads a fixed-size little-endian primitive at the given byte offset
    /// and zero-extends to long. The caller compares bit patterns; signed-vs-
    /// unsigned doesn't matter here since the bit pattern uniquely identifies
    /// each value.
    /// </summary>
    private static long ReadKey(ReadOnlySpan<byte> src, int byteOffset, int elemSize) => elemSize switch
    {
        1 => src[byteOffset],
        2 => BinaryPrimitives.ReadUInt16LittleEndian(src.Slice(byteOffset, 2)),
        4 => BinaryPrimitives.ReadUInt32LittleEndian(src.Slice(byteOffset, 4)),
        8 => unchecked((long)BinaryPrimitives.ReadUInt64LittleEndian(src.Slice(byteOffset, 8))),
        _ => throw new NotSupportedException(),
    };

    /// <summary>
    /// Counts runs in <paramref name="array"/>. A run boundary occurs whenever
    /// either the validity OR (for valid rows) the value bit pattern differs
    /// from the previous row. Adjacent null rows therefore collapse into a
    /// single null run regardless of any garbage bytes in their value slots.
    /// </summary>
    private static int CountRuns(IArrowArray array, int elemSize)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        var src = data.Buffers[1].Span;
        int n = array.Length;
        int off = data.Offset;
        bool hasNulls = data.GetNullCount() > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;

        int runs = 1;
        bool prevValid = !hasNulls || IsValidAt(validity, off);
        long prevKey = prevValid ? ReadKey(src, off * elemSize, elemSize) : 0;
        for (int i = 1; i < n; i++)
        {
            bool curValid = !hasNulls || IsValidAt(validity, off + i);
            long curKey = curValid ? ReadKey(src, (off + i) * elemSize, elemSize) : 0;
            if (curValid != prevValid || (curValid && curKey != prevKey))
            {
                runs++;
                prevValid = curValid;
                prevKey = curKey;
            }
        }
        return runs;
    }

    private static bool IsValidAt(ReadOnlySpan<byte> bitmap, int i) =>
        (bitmap[i >> 3] & (1 << (i & 7))) != 0;

    private static (IArrowArray Ends, IArrowArray Values, byte EndsPtype)
        BuildRunEnds(IArrowArray array, int elemSize, int n)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        var src = data.Buffers[1].Span;
        int off = data.Offset;
        bool hasNulls = data.GetNullCount() > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;

        int numRuns = CountRuns(array, elemSize);
        byte endsPtype = EndsPtypeFor(n);
        int endsElemSize = SmallestUIntElemSize(n);

        var endsBytes = new byte[(long)numRuns * endsElemSize];
        var valuesBytes = new byte[(long)numRuns * elemSize];
        // One validity bit per run; only allocated when the input has nulls.
        byte[]? valuesValidityBytes = hasNulls ? new byte[(numRuns + 7) / 8] : null;
        int valuesNullCount = 0;

        int runIdx = 0;
        bool prevValid = !hasNulls || IsValidAt(validity, off);
        long prevKey = prevValid ? ReadKey(src, off * elemSize, elemSize) : 0;
        // Seed values[0] + values_validity[0] from row 0 of the slice.
        if (prevValid) src.Slice(off * elemSize, elemSize).CopyTo(valuesBytes.AsSpan(0, elemSize));
        if (valuesValidityBytes is not null)
        {
            if (prevValid) valuesValidityBytes[0] |= 1;
            else valuesNullCount++;
        }
        for (int i = 1; i < n; i++)
        {
            bool curValid = !hasNulls || IsValidAt(validity, off + i);
            long curKey = curValid ? ReadKey(src, (off + i) * elemSize, elemSize) : 0;
            if (curValid != prevValid || (curValid && curKey != prevKey))
            {
                // Close run runIdx at position i (run-end indices are
                // logical positions within the slice, not absolute rows in
                // the underlying buffer); start run runIdx+1.
                WriteEnd(endsBytes.AsSpan(runIdx * endsElemSize, endsElemSize), (ulong)i, endsElemSize);
                runIdx++;
                if (curValid)
                    src.Slice((off + i) * elemSize, elemSize).CopyTo(valuesBytes.AsSpan(runIdx * elemSize, elemSize));
                // (else leave the slot zeroed — masked by the run's validity bit)
                if (valuesValidityBytes is not null)
                {
                    if (curValid) valuesValidityBytes[runIdx >> 3] |= (byte)(1 << (runIdx & 7));
                    else valuesNullCount++;
                }
                prevValid = curValid;
                prevKey = curKey;
            }
        }
        // Close the final run at position n.
        WriteEnd(endsBytes.AsSpan(runIdx * endsElemSize, endsElemSize), (ulong)n, endsElemSize);

        var ends = BuildUnsignedArray(endsBytes, numRuns, endsElemSize);
        var valuesValidityBuf = valuesValidityBytes is null
            ? ArrowBuffer.Empty
            : new ArrowBuffer(valuesValidityBytes);
        var values = BuildPrimitiveArray(array, valuesBytes, numRuns, valuesValidityBuf, valuesNullCount);
        return (ends, values, endsPtype);
    }

    private static void WriteEnd(Span<byte> dest, ulong value, int elemSize)
    {
        switch (elemSize)
        {
            case 1: dest[0] = (byte)value; break;
            case 2: BinaryPrimitives.WriteUInt16LittleEndian(dest, (ushort)value); break;
            case 4: BinaryPrimitives.WriteUInt32LittleEndian(dest, (uint)value); break;
            case 8: BinaryPrimitives.WriteUInt64LittleEndian(dest, value); break;
            default: throw new NotSupportedException();
        }
    }

    private static IArrowArray BuildUnsignedArray(byte[] bytes, int len, int elemSize)
    {
        var buf = new ArrowBuffer(bytes);
        return elemSize switch
        {
            1 => new UInt8Array(buf, ArrowBuffer.Empty, len, 0, 0),
            2 => new UInt16Array(buf, ArrowBuffer.Empty, len, 0, 0),
            4 => new UInt32Array(buf, ArrowBuffer.Empty, len, 0, 0),
            8 => new UInt64Array(buf, ArrowBuffer.Empty, len, 0, 0),
            _ => throw new NotSupportedException(),
        };
    }

    /// <summary>
    /// Builds a typed Arrow array of the same kind as <paramref name="template"/>,
    /// covering the run-values bytes. <paramref name="validity"/> is the
    /// per-run validity bitmap (one bit per run) — empty for non-nullable
    /// inputs, populated when any run is null.
    /// </summary>
    private static IArrowArray BuildPrimitiveArray(
        IArrowArray template, byte[] valuesBytes, int totalValues,
        ArrowBuffer validity, int nullCount)
    {
        var buf = new ArrowBuffer(valuesBytes);
        return template switch
        {
            Int8Array => new Int8Array(buf, validity, totalValues, nullCount, 0),
            UInt8Array => new UInt8Array(buf, validity, totalValues, nullCount, 0),
            Int16Array => new Int16Array(buf, validity, totalValues, nullCount, 0),
            UInt16Array => new UInt16Array(buf, validity, totalValues, nullCount, 0),
            Int32Array => new Int32Array(buf, validity, totalValues, nullCount, 0),
            UInt32Array => new UInt32Array(buf, validity, totalValues, nullCount, 0),
            Int64Array => new Int64Array(buf, validity, totalValues, nullCount, 0),
            UInt64Array => new UInt64Array(buf, validity, totalValues, nullCount, 0),
            _ => throw new NotSupportedException(),
        };
    }

    /// <summary>
    /// Inline RunEndMetadata proto bytes:
    ///   field 1 (varint, PType enum): ends_ptype
    ///   field 2 (varint, u64): num_runs
    ///   field 3 (varint, u64): offset (omitted when 0 — proto3 default).
    /// </summary>
    private static byte[] SerializeMetadata(byte endsPtype, ulong numRuns)
    {
        Span<byte> tmp = stackalloc byte[24];
        int pos = 0;
        tmp[pos++] = 0x08; // tag 1, wire-type 0
        pos += Varint.WriteUnsigned(tmp.Slice(pos), endsPtype);
        tmp[pos++] = 0x10; // tag 2, wire-type 0
        pos += Varint.WriteUnsigned(tmp.Slice(pos), numRuns);
        // tag 3 (offset) omitted: proto3 default of 0 matches our "non-sliced" constraint.
        return tmp.Slice(0, pos).ToArray();
    }
}

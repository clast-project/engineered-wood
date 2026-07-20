// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.datetimeparts</c>: timestamp values stored as three
/// separate columns (days, seconds, subseconds) so each can be compressed by
/// the strategy best for its distribution.
///
/// <para>Wire format: 0 buffers, 3 children (days, seconds, subseconds),
/// metadata <c>DateTimePartsMetadata { days_ptype, seconds_ptype, subseconds_ptype }</c>.</para>
///
/// <para>Reconstruction (per <c>encodings/datetime-parts/src/canonical.rs</c>):
/// <c>output_in_unit[i] = days[i] * 86_400 * divisor + seconds[i] * divisor + subseconds[i]</c>,
/// where <c>divisor</c> is the unit factor relative to the chosen
/// <see cref="TimestampType"/> unit (s=1, ms=10^3, us=10^6, ns=10^9).</para>
/// </summary>
internal static class DateTimePartsArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IReadOnlyList<string> arraySpecs,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (expectedType is not TimestampType ts)
            throw new NotSupportedException(
                $"vortex.datetimeparts only produces TimestampType today, got {expectedType}.");
        if (node.BufferRefCount != 0)
            throw new VortexFormatException(
                $"vortex.datetimeparts expects 0 buffers, got {node.BufferRefCount}.");
        if (node.ChildCount != 3)
            throw new VortexFormatException(
                $"vortex.datetimeparts expects 3 children (days, seconds, subseconds), got {node.ChildCount}.");

        var metaVec = node.Metadata;
        var (daysPtype, secondsPtype, subsecondsPtype) = ParseMetadata(metaVec.Length == 0
            ? ReadOnlySpan<byte>.Empty
            : metaVec.RawBytes(metaVec.Length));

        var rowCount = checked((int)expectedRowCount);
        var days = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, PtypeIntToArrowType(daysPtype), expectedRowCount);
        var seconds = ArrayDecoder.DecodeNode(
            node.Child(1), serialized, arraySpecs, PtypeIntToArrowType(secondsPtype), expectedRowCount);
        var subseconds = ArrayDecoder.DecodeNode(
            node.Child(2), serialized, arraySpecs, PtypeIntToArrowType(subsecondsPtype), expectedRowCount);

        var divisor = ts.Unit switch
        {
            Apache.Arrow.Types.TimeUnit.Second => 1L,
            Apache.Arrow.Types.TimeUnit.Millisecond => 1_000L,
            Apache.Arrow.Types.TimeUnit.Microsecond => 1_000_000L,
            Apache.Arrow.Types.TimeUnit.Nanosecond => 1_000_000_000L,
            _ => throw new NotSupportedException(
                $"vortex.datetimeparts: unsupported TimestampType unit {ts.Unit}."),
        };

        // Validity comes from the days child (per upstream's usage of `days.validity()`).
        var (nullBuffer, nullCount) = ExtractValidity(days);

        var bytes = new byte[(long)rowCount * sizeof(long)];
        var span = MemoryMarshal.Cast<byte, long>(bytes.AsSpan());
        for (int i = 0; i < rowCount; i++)
        {
            long d = GetLongAtIndex(days, i);
            long s = GetLongAtIndex(seconds, i);
            long ss = GetLongAtIndex(subseconds, i);
            span[i] = checked(d * 86_400L * divisor + s * divisor + ss);
        }

        return new TimestampArray(
            ts,
            new ArrowBuffer(bytes),
            nullBuffer,
            rowCount,
            nullCount,
            offset: 0);
    }

    private static (ArrowBuffer NullBuffer, int NullCount) ExtractValidity(IArrowArray array)
    {
        var data = (array as Apache.Arrow.Array)?.Data;
        if (data is null || data.NullCount == 0 || data.Buffers.Length == 0)
            return (ArrowBuffer.Empty, 0);
        return (data.Buffers[0], data.NullCount);
    }

    private static (int Days, int Seconds, int Subseconds) ParseMetadata(ReadOnlySpan<byte> bytes)
    {
        // DateTimePartsMetadata: field 1 days_ptype, field 2 seconds_ptype, field 3 subseconds_ptype
        int days = 0, seconds = 0, subseconds = 0;
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0)
                days = (int)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 2 && wireType == 0)
                seconds = (int)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 3 && wireType == 0)
                subseconds = (int)Varint.ReadUnsigned(bytes, ref pos);
            else
            {
                switch (wireType)
                {
                    case 0: Varint.ReadUnsigned(bytes, ref pos); break;
                    case 1: pos += 8; break;
                    case 2:
                        var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                        pos += len; break;
                    case 5: pos += 4; break;
                    default:
                        throw new VortexFormatException(
                            $"Unsupported wire type {wireType} in DateTimePartsMetadata.");
                }
            }
        }
        return (days, seconds, subseconds);
    }

    /// <summary>
    /// Reads value at logical index <paramref name="i"/> from a primitive
    /// child as a sign-extended <see cref="long"/>. Reads raw buffer bytes
    /// rather than going through <c>GetValue(i)</c> so null rows return their
    /// underlying byte pattern (which may be garbage) instead of throwing —
    /// the combined output's null bitmap (inherited from the days child) is
    /// what masks them at read time.
    /// </summary>
    private static long GetLongAtIndex(IArrowArray array, int i)
    {
        var data = ((Apache.Arrow.Array)array).Data;
        int abs = data.Offset + i;
        var span = data.Buffers[1].Span;
        return array switch
        {
            UInt8Array => span[abs],
            UInt16Array => System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(abs * 2, 2)),
            UInt32Array => System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(abs * 4, 4)),
            UInt64Array => unchecked((long)System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(abs * 8, 8))),
            Int8Array => unchecked((sbyte)span[abs]),
            Int16Array => System.Buffers.Binary.BinaryPrimitives.ReadInt16LittleEndian(span.Slice(abs * 2, 2)),
            Int32Array => System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(span.Slice(abs * 4, 4)),
            Int64Array => System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(span.Slice(abs * 8, 8)),
            _ => throw new VortexFormatException(
                $"vortex.datetimeparts child type {array.GetType().Name} not supported."),
        };
    }

    private static IArrowType PtypeIntToArrowType(int ptype) => ptype switch
    {
        0 => UInt8Type.Default,
        1 => UInt16Type.Default,
        2 => UInt32Type.Default,
        3 => UInt64Type.Default,
        4 => Int8Type.Default,
        5 => Int16Type.Default,
        6 => Int32Type.Default,
        7 => Int64Type.Default,
        _ => throw new VortexFormatException(
            $"Unsupported ptype {ptype} in DateTimePartsMetadata."),
    };
}

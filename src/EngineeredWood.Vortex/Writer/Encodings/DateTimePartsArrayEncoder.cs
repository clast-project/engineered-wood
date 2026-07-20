// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.DateTimePartsArrayDecoder"/>:
/// emits a <c>vortex.datetimeparts</c> ArrayNode subtree for
/// <see cref="TimestampArray"/> columns. Splits each timestamp into three
/// integer parts so each can compress with the strategy best for its
/// distribution (per upstream
/// <c>vortex-datetime-parts/src/{compress,timestamp}.rs</c>):
/// <list type="bullet">
///   <item><c>days</c> = <c>ticks / ticksPerDay</c></item>
///   <item><c>seconds</c> = <c>(ticks % ticksPerDay) / divisor</c> (range [0, 86399])</item>
///   <item><c>subseconds</c> = <c>(ticks % ticksPerDay) % divisor</c></item>
/// </list>
/// where <c>divisor</c> is the unit's ticks-per-second factor (s=1, ms=10^3,
/// μs=10^6, ns=10^9). Note: integer division/modulo TRUNCATE toward zero in
/// both Rust and C#, so for pre-1970 timestamps the parts can be negative —
/// the per-part ptype selection handles this by widening to a signed type
/// when any value is negative.
///
/// <para>Wire shape: 0 buffers, 3 children (days, seconds, subseconds),
/// metadata <c>{ days_ptype, seconds_ptype, subseconds_ptype }</c>. Children
/// are recursively encoded with <c>compress=true</c>, so small-range parts
/// (days near a target era, seconds in [0,86399]) land on bitpacked / FoR for
/// large savings vs raw 8-bytes/row.</para>
///
/// <para>Validity convention (per upstream <c>compress.rs</c> and our reader's
/// <c>ExtractValidity</c>): the <c>days</c> child carries the column's null
/// bitmap; <c>seconds</c> and <c>subseconds</c> are non-nullable. Null rows
/// can have any garbage value bytes — the reader masks via the days
/// validity.</para>
///
/// <para>Opt-in via <c>VortexFileWriter(preferDateTimeParts: true)</c>. The
/// default keeps Timestamp columns on <c>vortex.primitive</c>.</para>
/// </summary>
internal static class DateTimePartsArrayEncoder
{
    public static bool IsApplicable(IArrowArray array)
    {
        if (array is not TimestampArray ts) return false;
        var tsType = (TimestampType)ts.Data.DataType;
        // Days unit isn't representable as Arrow TimestampType (Date32 instead),
        // so any TimestampType.Unit we see here is splittable.
        return tsType.Unit is Apache.Arrow.Types.TimeUnit.Second
            or Apache.Arrow.Types.TimeUnit.Millisecond
            or Apache.Arrow.Types.TimeUnit.Microsecond
            or Apache.Arrow.Types.TimeUnit.Nanosecond;
    }

    public static int Emit(
        SegmentBuilder sb, IArrowArray array, EncodingIndices idx, int? statsTicket = null)
    {
        if (array is not TimestampArray tsArray)
            throw new NotSupportedException(
                $"vortex.datetimeparts writer requires TimestampArray, got {array.GetType().Name}.");
        var data = tsArray.Data;
        var tsType = (TimestampType)data.DataType;

        long divisor = tsType.Unit switch
        {
            Apache.Arrow.Types.TimeUnit.Second => 1L,
            Apache.Arrow.Types.TimeUnit.Millisecond => 1_000L,
            Apache.Arrow.Types.TimeUnit.Microsecond => 1_000_000L,
            Apache.Arrow.Types.TimeUnit.Nanosecond => 1_000_000_000L,
            _ => throw new NotSupportedException(
                $"vortex.datetimeparts: TimestampType unit {tsType.Unit} is not splittable."),
        };
        long ticksPerDay = 86_400L * divisor;

        int n = array.Length;
        int off = data.Offset;
        int nullCount = (int)data.GetNullCount();
        bool hasNulls = nullCount > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;
        var ticks = MemoryMarshal.Cast<byte, long>(
            data.Buffers[1].Span.Slice(off * 8, n * 8));

        // Walk once: split each timestamp + track per-part min/max over
        // VALID rows only. Null rows still get parts written (zeroes are
        // simplest), since the days array carries the bitmap and the reader
        // masks them via that.
        var daysArr = new long[n];
        var secondsArr = new long[n];
        var subsecondsArr = new long[n];
        long daysMin = long.MaxValue, daysMax = long.MinValue;
        long secondsMin = long.MaxValue, secondsMax = long.MinValue;
        long subsecondsMin = long.MaxValue, subsecondsMax = long.MinValue;
        bool seenAny = false;

        for (int i = 0; i < n; i++)
        {
            long t = ticks[i];
            long days = t / ticksPerDay;
            long withinDay = t % ticksPerDay;
            long seconds = withinDay / divisor;
            long subseconds = withinDay % divisor;
            daysArr[i] = days;
            secondsArr[i] = seconds;
            subsecondsArr[i] = subseconds;

            if (hasNulls && (validity[(off + i) >> 3] & (1 << ((off + i) & 7))) == 0) continue;

            if (!seenAny)
            {
                daysMin = daysMax = days;
                secondsMin = secondsMax = seconds;
                subsecondsMin = subsecondsMax = subseconds;
                seenAny = true;
            }
            else
            {
                if (days < daysMin) daysMin = days; else if (days > daysMax) daysMax = days;
                if (seconds < secondsMin) secondsMin = seconds; else if (seconds > secondsMax) secondsMax = seconds;
                if (subseconds < subsecondsMin) subsecondsMin = subseconds; else if (subseconds > subsecondsMax) subsecondsMax = subseconds;
            }
        }

        // For empty / all-null columns default to U8 (smallest); the picked
        // ptype's storage cost is dominated by per-chunk overhead in that case.
        if (!seenAny)
        {
            daysMin = daysMax = secondsMin = secondsMax = subsecondsMin = subsecondsMax = 0;
        }

        var daysPtype = PickPtype(daysMin, daysMax);
        var secondsPtype = PickPtype(secondsMin, secondsMax);
        var subsecondsPtype = PickPtype(subsecondsMin, subsecondsMax);

        // Days carries the column's validity bitmap. Build a fresh offset-0
        // bitmap so the rebased days array matches the (always offset=0)
        // value buffers we just allocated.
        ArrowBuffer daysValidityBuf = ArrowBuffer.Empty;
        if (hasNulls)
        {
            var bitmap = EncoderHelpers.ExtractValidityBitmap(
                validity, srcBitOffset: off, rowCount: n);
            daysValidityBuf = new ArrowBuffer(bitmap);
        }

        var daysArrowArray = BuildPartArray(daysPtype, daysArr, n, daysValidityBuf, nullCount);
        var secondsArrowArray = BuildPartArray(secondsPtype, secondsArr, n, ArrowBuffer.Empty, 0);
        var subsecondsArrowArray = BuildPartArray(subsecondsPtype, subsecondsArr, n, ArrowBuffer.Empty, 0);

        // Recursive children: pass compress=true so small-range parts (days
        // near a target era + seconds bounded to [0, 86399]) land on bitpacked
        // / FoR. Without this datetimeparts can actually inflate vs raw f64
        // storage.
        int daysTicket = ArrayEncoderDispatch.Emit(
            sb, daysArrowArray, idx, statsTicket: null, compress: true);
        int secondsTicket = ArrayEncoderDispatch.Emit(
            sb, secondsArrowArray, idx, statsTicket: null, compress: true);
        int subsecondsTicket = ArrayEncoderDispatch.Emit(
            sb, subsecondsArrowArray, idx, statsTicket: null, compress: true);

        var metadataBytes = SerializeMetadata(daysPtype, secondsPtype, subsecondsPtype);
        int metadataTicket = sb.Builder.WriteByteVector(metadataBytes);

        var children = new[] { daysTicket, secondsTicket, subsecondsTicket };
        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithMetadataAndChildren(
                sb.Builder, idx.DateTimeParts, metadataTicket, children)
            : ArrayNodeEmitter.EmitWithMetadataChildrenAndStats(
                sb.Builder, idx.DateTimeParts, metadataTicket, children, statsTicket.Value);
    }

    /// <summary>
    /// Smallest PType (Vortex enum: U8=0, U16=1, U32=2, U64=3, I8=4, I16=5,
    /// I32=6, I64=7) whose range covers <c>[min, max]</c>. Prefers unsigned
    /// when min ≥ 0 since it lets bitpacked operate on the full type range.
    /// </summary>
    private static byte PickPtype(long min, long max)
    {
        if (min >= 0)
        {
            if ((ulong)max <= byte.MaxValue) return 0; // U8
            if ((ulong)max <= ushort.MaxValue) return 1; // U16
            if ((ulong)max <= uint.MaxValue) return 2; // U32
            return 3; // U64
        }
        if (min >= sbyte.MinValue && max <= sbyte.MaxValue) return 4; // I8
        if (min >= short.MinValue && max <= short.MaxValue) return 5; // I16
        if (min >= int.MinValue && max <= int.MaxValue) return 6; // I32
        return 7; // I64
    }

    /// <summary>
    /// Materialises one part column as a typed Arrow array of the chosen
    /// ptype. Narrowing casts are unchecked: per-row values are guaranteed to
    /// fit by <see cref="PickPtype"/>'s range scan.
    /// </summary>
    private static IArrowArray BuildPartArray(
        byte ptype, long[] values, int n, ArrowBuffer validity, int nullCount)
    {
        switch (ptype)
        {
            case 0: // U8
            {
                var bytes = new byte[n];
                for (int i = 0; i < n; i++) bytes[i] = (byte)values[i];
                return new UInt8Array(new ArrowBuffer(bytes), validity, n, nullCount, 0);
            }
            case 1: // U16
            {
                var bytes = new byte[(long)n * 2];
                var span = MemoryMarshal.Cast<byte, ushort>(bytes.AsSpan());
                for (int i = 0; i < n; i++) span[i] = (ushort)values[i];
                return new UInt16Array(new ArrowBuffer(bytes), validity, n, nullCount, 0);
            }
            case 2: // U32
            {
                var bytes = new byte[(long)n * 4];
                var span = MemoryMarshal.Cast<byte, uint>(bytes.AsSpan());
                for (int i = 0; i < n; i++) span[i] = (uint)values[i];
                return new UInt32Array(new ArrowBuffer(bytes), validity, n, nullCount, 0);
            }
            case 3: // U64
            {
                var bytes = new byte[(long)n * 8];
                var span = MemoryMarshal.Cast<byte, ulong>(bytes.AsSpan());
                for (int i = 0; i < n; i++) span[i] = (ulong)values[i];
                return new UInt64Array(new ArrowBuffer(bytes), validity, n, nullCount, 0);
            }
            case 4: // I8
            {
                var bytes = new byte[n];
                for (int i = 0; i < n; i++) bytes[i] = (byte)(sbyte)values[i];
                return new Int8Array(new ArrowBuffer(bytes), validity, n, nullCount, 0);
            }
            case 5: // I16
            {
                var bytes = new byte[(long)n * 2];
                var span = MemoryMarshal.Cast<byte, short>(bytes.AsSpan());
                for (int i = 0; i < n; i++) span[i] = (short)values[i];
                return new Int16Array(new ArrowBuffer(bytes), validity, n, nullCount, 0);
            }
            case 6: // I32
            {
                var bytes = new byte[(long)n * 4];
                var span = MemoryMarshal.Cast<byte, int>(bytes.AsSpan());
                for (int i = 0; i < n; i++) span[i] = (int)values[i];
                return new Int32Array(new ArrowBuffer(bytes), validity, n, nullCount, 0);
            }
            default: // 7 = I64
            {
                var bytes = new byte[(long)n * 8];
                var span = MemoryMarshal.Cast<byte, long>(bytes.AsSpan());
                for (int i = 0; i < n; i++) span[i] = values[i];
                return new Int64Array(new ArrowBuffer(bytes), validity, n, nullCount, 0);
            }
        }
    }

    /// <summary>
    /// DateTimePartsMetadata proto bytes (matches the reader's parser):
    ///   field 1 (varint): days_ptype
    ///   field 2 (varint): seconds_ptype
    ///   field 3 (varint): subseconds_ptype
    /// </summary>
    private static byte[] SerializeMetadata(byte daysPtype, byte secondsPtype, byte subsecondsPtype)
    {
        Span<byte> tmp = stackalloc byte[12];
        int pos = 0;
        tmp[pos++] = 0x08; // tag 1, varint
        pos += Varint.WriteUnsigned(tmp.Slice(pos), daysPtype);
        tmp[pos++] = 0x10; // tag 2, varint
        pos += Varint.WriteUnsigned(tmp.Slice(pos), secondsPtype);
        tmp[pos++] = 0x18; // tag 3, varint
        pos += Varint.WriteUnsigned(tmp.Slice(pos), subsecondsPtype);
        return tmp.Slice(0, pos).ToArray();
    }
}

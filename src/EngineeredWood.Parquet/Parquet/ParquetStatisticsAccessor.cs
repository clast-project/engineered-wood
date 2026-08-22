// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.InteropServices;
using EngineeredWood.Expressions;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet;

/// <summary>
/// Adapts Parquet <see cref="RowGroup"/> metadata for the shared
/// <see cref="StatisticsEvaluator"/>. Decodes raw min/max bytes from
/// <see cref="Statistics"/> into typed <see cref="LiteralValue"/>s based on
/// each column's physical and logical type.
/// </summary>
/// <remarks>
/// Returns <c>null</c> for unknown columns, missing stats, INT96 (sort order
/// undefined per spec), or types this accessor doesn't yet decode. The
/// evaluator treats null as "Unknown" and conservatively keeps the row group.
///
/// Prefers the typed <c>min_value</c>/<c>max_value</c> fields (correct logical
/// sort order) when present, falling back to the legacy <c>min</c>/<c>max</c>
/// fields for backwards compatibility on signed numeric types only.
/// </remarks>
public sealed class ParquetStatisticsAccessor
    : IStatisticsAccessor<RowGroup>, INanCountAccessor<RowGroup>
{
    private readonly SchemaDescriptor _schema;
    private readonly Dictionary<string, int> _nameToLeafIndex;

    public ParquetStatisticsAccessor(SchemaDescriptor schema)
    {
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _nameToLeafIndex = BuildNameIndex(schema);
    }

    public LiteralValue? GetMinValue(RowGroup rg, string column)
    {
        if (!TryGetColumn(rg, column, out var desc, out var stats))
            return null;
        return DecodeMin(desc!, stats!);
    }

    public LiteralValue? GetMaxValue(RowGroup rg, string column)
    {
        if (!TryGetColumn(rg, column, out var desc, out var stats))
            return null;
        return DecodeMax(desc!, stats!);
    }

    public long? GetNullCount(RowGroup rg, string column) =>
        TryGetColumn(rg, column, out _, out var stats) ? stats!.NullCount : null;

    public long? GetNanCount(RowGroup rg, string column) =>
        TryGetColumn(rg, column, out _, out var stats) ? stats!.NanCount : null;

    public long? GetValueCount(RowGroup rg, string column) => rg.NumRows;

    public bool IsMinExact(RowGroup rg, string column) =>
        !TryGetColumn(rg, column, out _, out var stats) || stats!.IsMinValueExact != false;

    public bool IsMaxExact(RowGroup rg, string column) =>
        !TryGetColumn(rg, column, out _, out var stats) || stats!.IsMaxValueExact != false;

    // ── Lookup ──

    private bool TryGetColumn(
        RowGroup rg, string column,
        out ColumnDescriptor? descriptor, out Statistics? stats)
    {
        descriptor = null;
        stats = null;
        if (!_nameToLeafIndex.TryGetValue(column, out int idx))
            return false;
        if (idx >= rg.Columns.Count)
            return false;

        var meta = rg.Columns[idx].MetaData;
        if (meta?.Statistics is null)
            return false;

        descriptor = _schema.Columns[idx];
        stats = meta.Statistics;
        return true;
    }

    private static Dictionary<string, int> BuildNameIndex(SchemaDescriptor schema)
    {
        var map = new Dictionary<string, int>(StringComparer.Ordinal);
        for (int i = 0; i < schema.Columns.Count; i++)
        {
            var col = schema.Columns[i];
            map[col.DottedPath] = i;
            // Also index by leaf-only name when unambiguous, for convenience.
            if (col.Path.Count == 1 && !map.ContainsKey(col.Path[0]))
                map[col.Path[0]] = i;
        }
        return map;
    }

    // ── Decoding ──

    private static LiteralValue? DecodeMin(ColumnDescriptor desc, Statistics stats)
    {
        var bytes = stats.MinValue ?? FallbackBytes(desc, stats.Min);
        return bytes is null ? null : Decode(desc, bytes, isMax: false);
    }

    private static LiteralValue? DecodeMax(ColumnDescriptor desc, Statistics stats)
    {
        var bytes = stats.MaxValue ?? FallbackBytes(desc, stats.Max);
        return bytes is null ? null : Decode(desc, bytes, isMax: true);
    }

    /// <summary>
    /// The legacy <c>min</c>/<c>max</c> fields used unsigned byte comparison,
    /// which is correct only for signed numeric types. For other physical
    /// types they are unsafe to use as a fallback when min_value/max_value
    /// are absent.
    /// </summary>
    private static byte[]? FallbackBytes(ColumnDescriptor desc, byte[]? legacy)
    {
        if (legacy is null) return null;
        return desc.PhysicalType switch
        {
            PhysicalType.Int32 or PhysicalType.Int64
                or PhysicalType.Float or PhysicalType.Double => legacy,
            // BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY / BOOLEAN / INT96: legacy
            // ordering doesn't match logical type, so don't use it.
            _ => null,
        };
    }

    /// <param name="isMax">
    /// Which end of the range this bound is. It matters wherever the decode cannot be exact: a bound
    /// must only ever move OUTWARD. Rounding a max down, or a min up, narrows the range the file claims
    /// and lets a row group be pruned that genuinely contains matching rows.
    /// </param>
    private static LiteralValue? Decode(ColumnDescriptor desc, byte[] bytes, bool isMax)
    {
        var logical = desc.SchemaElement.LogicalType;

        return desc.PhysicalType switch
        {
            PhysicalType.Boolean => bytes.Length >= 1
                ? (LiteralValue?)LiteralValue.Of(bytes[0] != 0) : null,
            PhysicalType.Int32 => DecodeInt32(bytes, logical),
            PhysicalType.Int64 => DecodeInt64(bytes, logical, isMax),
            PhysicalType.Float => DecodeFloat(bytes),
            PhysicalType.Double => DecodeDouble(bytes),
            PhysicalType.ByteArray => DecodeByteArray(bytes, logical),
            PhysicalType.FixedLenByteArray => DecodeFixedLenByteArray(desc, bytes, logical, isMax),
            // INT96 sort order is undefined per the Parquet spec.
            PhysicalType.Int96 => null,
            _ => null,
        };
    }

    /// <summary>
    /// Decodes a FLOAT bound, returning <see langword="null"/> for a NaN bound.
    /// A NaN min/max (possible only under IEEE 754 total order, when every value
    /// is NaN) is not a usable range endpoint, so the evaluator treats it as
    /// unknown rather than pruning on it.
    /// </summary>
    private static LiteralValue? DecodeFloat(byte[] bytes)
    {
        if (bytes.Length < 4) return null;
        float value = MemoryMarshal.Read<float>(bytes);
        if (float.IsNaN(value)) return null;
        return LiteralValue.Of(value);
    }

    /// <summary>Decodes a DOUBLE bound, returning <see langword="null"/> for a NaN bound.</summary>
    private static LiteralValue? DecodeDouble(byte[] bytes)
    {
        if (bytes.Length < 8) return null;
        double value = MemoryMarshal.Read<double>(bytes);
        if (double.IsNaN(value)) return null;
        return LiteralValue.Of(value);
    }

    private static LiteralValue? DecodeInt32(byte[] bytes, LogicalType? logical)
    {
        if (bytes.Length < 4) return null;
        int v = BinaryPrimitives.ReadInt32LittleEndian(bytes);

        switch (logical)
        {
            case LogicalType.DateType:
#if NET6_0_OR_GREATER
                return LiteralValue.Of(DateOnly.FromDayNumber(EpochDays + v));
#else
                return LiteralValue.Of((long)v);
#endif
            case LogicalType.IntType { IsSigned: false, BitWidth: <= 32 }:
                return LiteralValue.Of((uint)v);
            case LogicalType.DecimalType d:
                return LiteralValue.HighPrecisionDecimalOf(new BigInteger(v), d.Scale);
#if NET6_0_OR_GREATER
            case LogicalType.TimeType t when t.Unit == TimeUnit.Millis:
                return LiteralValue.Of(new TimeOnly(v * TimeSpan.TicksPerMillisecond));
#endif
            default:
                return LiteralValue.Of(v);
        }
    }

    private static LiteralValue? DecodeInt64(byte[] bytes, LogicalType? logical, bool isMax)
    {
        if (bytes.Length < 8) return null;
        long v = BinaryPrimitives.ReadInt64LittleEndian(bytes);

        switch (logical)
        {
            case LogicalType.IntType { IsSigned: false, BitWidth: 64 }:
                return LiteralValue.Of((ulong)v);
            case LogicalType.DecimalType d:
                return LiteralValue.HighPrecisionDecimalOf(new BigInteger(v), d.Scale);
            case LogicalType.TimestampType ts:
                return TimestampLiteral(new BigInteger(v), ts, isMax);
#if NET6_0_OR_GREATER
            case LogicalType.TimeType t when t.Unit == TimeUnit.Micros:
                return LiteralValue.Of(new TimeOnly(v * 10)); // micros → ticks (10 ticks per us), exact
            case LogicalType.TimeType t when t.Unit == TimeUnit.Nanos:
                // 100 ns per tick, so this is the one TIME unit that cannot be exact. Round outward, then
                // clamp: rounding the last nanoseconds of a day up would leave TimeOnly's range, and its
                // maximum is still a sound outward bound.
                long timeTicks = (long)DivideOutward(v, 100, isMax);
                if (timeTicks < 0 || timeTicks > TimeOnly.MaxValue.Ticks)
                    timeTicks = Math.Clamp(timeTicks, 0, TimeOnly.MaxValue.Ticks);
                return LiteralValue.Of(new TimeOnly(timeTicks));
#endif
            default:
                return LiteralValue.Of(v);
        }
    }

    private static LiteralValue? DecodeByteArray(byte[] bytes, LogicalType? logical)
    {
        return logical switch
        {
            LogicalType.StringType
                or LogicalType.JsonType
                or LogicalType.EnumType =>
                LiteralValue.Of(System.Text.Encoding.UTF8.GetString(bytes)),
            LogicalType.DecimalType d =>
                LiteralValue.HighPrecisionDecimalOf(BigEndianToBigInteger(bytes), d.Scale),
            _ => LiteralValue.Of(bytes),
        };
    }

    private static LiteralValue? DecodeFixedLenByteArray(
        ColumnDescriptor desc, byte[] bytes, LogicalType? logical, bool isMax)
    {
        switch (logical)
        {
            case LogicalType.UuidType when bytes.Length == 16:
                return LiteralValue.Of(GuidFromBigEndian(bytes));

            case LogicalType.DecimalType d:
                return LiteralValue.HighPrecisionDecimalOf(BigEndianToBigInteger(bytes), d.Scale);

#if NET6_0_OR_GREATER
            case LogicalType.Float16Type when bytes.Length == 2:
                ushort raw = BinaryPrimitives.ReadUInt16LittleEndian(bytes);
                return LiteralValue.Of(BitConverter.UInt16BitsToHalf(raw));
#endif
            default:
                return LiteralValue.Of(bytes);
        }
    }

    /// <summary>Ticks (100 ns) from .NET's epoch (0001-01-01) to the Unix epoch.</summary>
    private const long UnixEpochTicks = 621_355_968_000_000_000L;

    /// <summary>
    /// Builds a timestamp bound from a count of <paramref name="ts"/>'s unit since the Unix epoch.
    /// </summary>
    /// <remarks>
    /// <para>Goes through TICKS rather than milliseconds. A <see cref="DateTimeOffset"/> holds 100 ns,
    /// so MILLIS and MICROS convert exactly and only NANOS has to round -- where the previous
    /// millisecond conversion threw away everything below a millisecond for all three. That was not
    /// merely imprecise: it truncated toward zero, so a max bound of 1500 us came back as 0 ms, and a
    /// row group whose rows genuinely matched `t > 0.5ms` could be pruned on the strength of it.</para>
    ///
    /// <para>Returns <see langword="null"/> outside <see cref="DateTimeOffset"/>'s range. A bound that
    /// cannot be represented is not a bound; clamping one would be indistinguishable from a real
    /// endpoint and would prune on a value the file never contained.</para>
    /// </remarks>
    private static LiteralValue? TimestampLiteral(BigInteger value, LogicalType.TimestampType ts, bool isMax)
    {
        BigInteger unixTicks = ts.Unit switch
        {
            TimeUnit.Millis => value * 10_000,
            TimeUnit.Micros => value * 10,
            TimeUnit.Nanos => DivideOutward(value, 100, isMax),
            _ => value * 10,
        };

        BigInteger ticks = unixTicks + UnixEpochTicks;
        if (ticks < BigInteger.Zero || ticks > DateTime.MaxValue.Ticks)
            return null;

        return LiteralValue.Of(new DateTimeOffset((long)ticks, TimeSpan.Zero));
    }

    /// <summary>
    /// Divides so the result moves AWAY from zero-error: a max bound rounds up, a min bound rounds down.
    /// Both widen the range the bound describes, which is the only safe direction for pruning.
    /// </summary>
    private static BigInteger DivideOutward(BigInteger value, int divisor, bool isMax)
    {
        // DivRem truncates toward zero, so which adjustment is needed depends on the sign: for a
        // positive value the quotient is already the floor, for a negative one it is already the ceiling.
        BigInteger quotient = BigInteger.DivRem(value, divisor, out BigInteger remainder);
        if (remainder.IsZero)
            return quotient;

        if (isMax)
            return value.Sign > 0 ? quotient + BigInteger.One : quotient;

        return value.Sign < 0 ? quotient - BigInteger.One : quotient;
    }

    /// <summary>Days from .NET epoch (0001-01-01) to Unix epoch (1970-01-01).</summary>
    private const int EpochDays = 719_162;

    private static BigInteger BigEndianToBigInteger(byte[] bytes)
    {
        if (bytes.Length == 0) return BigInteger.Zero;

        // Parquet decimals are big-endian two's complement; BigInteger constructor
        // expects little-endian, so reverse.
        var reversed = new byte[bytes.Length];
        for (int i = 0; i < bytes.Length; i++)
            reversed[i] = bytes[bytes.Length - 1 - i];

        return new BigInteger(reversed);
    }

    private static Guid GuidFromBigEndian(byte[] bytes)
    {
        // RFC 4122: UUID is big-endian. .NET Guid's first three components are
        // little-endian on most platforms. Read as big-endian fields and rebuild.
        var span = (ReadOnlySpan<byte>)bytes;
        uint a = BinaryPrimitives.ReadUInt32BigEndian(span);
        ushort b = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(4));
        ushort c = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(6));
        return new Guid((int)a, (short)b, (short)c,
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15]);
    }
}

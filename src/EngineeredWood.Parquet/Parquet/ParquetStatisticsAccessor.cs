// Copyright (c) Curt Hagenlocher. All rights reserved.
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
        return bytes is null ? null : Decode(desc, bytes);
    }

    private static LiteralValue? DecodeMax(ColumnDescriptor desc, Statistics stats)
    {
        var bytes = stats.MaxValue ?? FallbackBytes(desc, stats.Max);
        return bytes is null ? null : Decode(desc, bytes);
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

    private static LiteralValue? Decode(ColumnDescriptor desc, byte[] bytes)
    {
        var logical = desc.SchemaElement.LogicalType;

        return desc.PhysicalType switch
        {
            PhysicalType.Boolean => bytes.Length >= 1
                ? (LiteralValue?)LiteralValue.Of(bytes[0] != 0) : null,
            PhysicalType.Int32 => DecodeInt32(bytes, logical),
            PhysicalType.Int64 => DecodeInt64(bytes, logical),
            PhysicalType.Float => DecodeFloat(bytes),
            PhysicalType.Double => DecodeDouble(bytes),
            PhysicalType.ByteArray => DecodeByteArray(bytes, logical),
            PhysicalType.FixedLenByteArray => DecodeFixedLenByteArray(desc, bytes, logical),
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

    private static LiteralValue? DecodeInt64(byte[] bytes, LogicalType? logical)
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
                long unixMs = ts.Unit switch
                {
                    TimeUnit.Millis => v,
                    TimeUnit.Micros => v / 1000,
                    TimeUnit.Nanos => v / 1_000_000,
                    _ => v,
                };
                var dto = ts.IsAdjustedToUtc
                    ? DateTimeOffset.FromUnixTimeMilliseconds(unixMs)
                    : new DateTimeOffset(
                        DateTimeOffset.FromUnixTimeMilliseconds(unixMs).Ticks,
                        TimeSpan.Zero);
                return LiteralValue.Of(dto);
#if NET6_0_OR_GREATER
            case LogicalType.TimeType t when t.Unit == TimeUnit.Micros:
                return LiteralValue.Of(new TimeOnly(v * 10)); // micros → ticks (10 ticks per us)
            case LogicalType.TimeType t when t.Unit == TimeUnit.Nanos:
                return LiteralValue.Of(new TimeOnly(v / 100)); // 100 ns per tick
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
        ColumnDescriptor desc, byte[] bytes, LogicalType? logical)
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

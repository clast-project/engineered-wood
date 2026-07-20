// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;

namespace EngineeredWood.Vortex;

/// <summary>
/// A per-evaluation cursor over <see cref="ZoneStats"/> for the columns
/// referenced by a predicate. Pre-loaded once per
/// <see cref="VortexFileReader.ReadAllAsync(EngineeredWood.Expressions.Predicate, System.Threading.CancellationToken)"/>
/// call; reused across zones by mutating <see cref="ZoneIndex"/>.
/// </summary>
internal sealed class VortexZoneCursor
{
    /// <summary>Per-column zone stats, keyed by schema field name. Null entries
    /// mean the column has no zone-stats layout (kept-conservatively semantics
    /// in the accessor).</summary>
    public IReadOnlyDictionary<string, ZoneStats?> StatsByColumn { get; }

    /// <summary>The zone (chunk) index currently being evaluated. Mutated
    /// across zones to avoid rebuilding the cursor object.</summary>
    public int ZoneIndex { get; set; }

    public VortexZoneCursor(IReadOnlyDictionary<string, ZoneStats?> statsByColumn)
    {
        StatsByColumn = statsByColumn;
    }
}

/// <summary>
/// Adapts Vortex per-zone <see cref="ZoneStats"/> for the shared
/// <see cref="StatisticsEvaluator"/>. Reads the typed Arrow zone-stats arrays
/// at the cursor's <see cref="VortexZoneCursor.ZoneIndex"/> and converts to
/// <see cref="LiteralValue"/> based on each column's Arrow data type.
/// </summary>
/// <remarks>
/// Returns <c>null</c> for unknown columns, missing stats, cleared validity
/// bits (empty / all-null zones), or types this accessor doesn't yet decode.
/// The evaluator treats <c>null</c> as "Unknown" and conservatively keeps the
/// zone.
///
/// Unit conversion for temporal columns (<see cref="Date32Type"/>,
/// <see cref="Date64Type"/>, <see cref="Time32Type"/>, <see cref="Time64Type"/>,
/// <see cref="TimestampType"/>) happens here so callers can build predicates
/// against natural .NET types (<see cref="DateOnly"/>, <see cref="TimeOnly"/>,
/// <see cref="DateTimeOffset"/>) without knowing each column's storage unit.
/// </remarks>
internal sealed class VortexZoneStatsAccessor : IStatisticsAccessor<VortexZoneCursor>
{
    private readonly Apache.Arrow.Schema _schema;

    public VortexZoneStatsAccessor(Apache.Arrow.Schema schema)
    {
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
    }

    public LiteralValue? GetMinValue(VortexZoneCursor cursor, string column)
    {
        if (!cursor.StatsByColumn.TryGetValue(column, out var stats) || stats?.Min is null)
            return null;
        var dtype = TryGetDataType(column);
        return dtype is null ? null : ReadCell(stats.Min, cursor.ZoneIndex, dtype);
    }

    public LiteralValue? GetMaxValue(VortexZoneCursor cursor, string column)
    {
        if (!cursor.StatsByColumn.TryGetValue(column, out var stats) || stats?.Max is null)
            return null;
        var dtype = TryGetDataType(column);
        return dtype is null ? null : ReadCell(stats.Max, cursor.ZoneIndex, dtype);
    }

    public long? GetNullCount(VortexZoneCursor cursor, string column)
    {
        if (!cursor.StatsByColumn.TryGetValue(column, out var stats) || stats?.NullCount is null)
            return null;
        // NullCount is a non-nullable u64. Cast unchecked — zone sizes vastly
        // smaller than long.MaxValue in any realistic file.
        return unchecked((long)stats.NullCount.GetValue(cursor.ZoneIndex)!.Value);
    }

    public long? GetValueCount(VortexZoneCursor cursor, string column)
    {
        if (!cursor.StatsByColumn.TryGetValue(column, out var stats) || stats is null)
            return null;
        // Vortex zones don't store per-zone row counts in the stats table; the
        // trailing zone may be shorter than ZoneLen. Returning ZoneLen is
        // conservative — IS NOT NULL on an all-null trailing zone may stay
        // Unknown rather than dropping, but never the other way around.
        return stats.ZoneLen;
    }

    public bool IsMinExact(VortexZoneCursor cursor, string column)
    {
        if (!cursor.StatsByColumn.TryGetValue(column, out var stats) || stats?.MinIsTruncated is null)
            return true; // No truncation flag → assume exact (matches our writer).
        return !stats.MinIsTruncated.IsValid(cursor.ZoneIndex)
            || !stats.MinIsTruncated.GetValue(cursor.ZoneIndex)!.Value;
    }

    public bool IsMaxExact(VortexZoneCursor cursor, string column)
    {
        if (!cursor.StatsByColumn.TryGetValue(column, out var stats) || stats?.MaxIsTruncated is null)
            return true;
        return !stats.MaxIsTruncated.IsValid(cursor.ZoneIndex)
            || !stats.MaxIsTruncated.GetValue(cursor.ZoneIndex)!.Value;
    }

    /// <summary>
    /// Walks all the column references in <paramref name="predicate"/> and
    /// records each referenced name. Used by <see cref="VortexFileReader"/>
    /// to pre-load only the zone stats the predicate actually needs.
    /// </summary>
    internal static HashSet<string> CollectReferencedColumns(Predicate predicate)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        WalkPredicate(predicate, names);
        return names;
    }

    private static void WalkPredicate(Predicate p, HashSet<string> names)
    {
        switch (p)
        {
            case TruePredicate:
            case FalsePredicate:
                return;
            case AndPredicate a:
                foreach (var c in a.Children) WalkPredicate(c, names);
                return;
            case OrPredicate o:
                foreach (var c in o.Children) WalkPredicate(c, names);
                return;
            case NotPredicate n:
                WalkPredicate(n.Child, names);
                return;
            case ComparisonPredicate cmp:
                WalkExpression(cmp.Left, names);
                WalkExpression(cmp.Right, names);
                return;
            case UnaryPredicate u:
                WalkExpression(u.Operand, names);
                return;
            case SetPredicate s:
                WalkExpression(s.Operand, names);
                return;
        }
    }

    private static void WalkExpression(Expression e, HashSet<string> names)
    {
        switch (e)
        {
            case UnboundReference u:
                names.Add(u.Name);
                return;
            case BoundReference b:
                names.Add(b.Name);
                return;
            case FunctionCall fc:
                foreach (var arg in fc.Arguments) WalkExpression(arg, names);
                return;
            case Predicate p:
                WalkPredicate(p, names);
                return;
        }
    }

    private IArrowType? TryGetDataType(string column)
    {
        var field = _schema.GetFieldByName(column);
        return field?.DataType;
    }

    /// <summary>
    /// Reads the cell at <paramref name="zoneIndex"/> from a typed Arrow
    /// zone-stats array and converts it into a <see cref="LiteralValue"/>
    /// matching <paramref name="dtype"/>. Returns <c>null</c> for invalid
    /// (cleared validity) cells or unsupported Arrow types.
    /// </summary>
    private static LiteralValue? ReadCell(IArrowArray cell, int zoneIndex, IArrowType dtype)
    {
        // Decimal first — Decimal128/256Type both inherit FixedSizeBinaryType
        // in Apache.Arrow 22.x+, so they must be matched before the FSB / int
        // cases below. Apache.Arrow's GetValue returns System.Decimal which
        // overflows for high-precision values; instead read the raw LE bytes
        // from Buffers[1] and produce a HighPrecisionDecimal LiteralValue.
        if (dtype is Decimal128Type d128 && cell is Decimal128Array dec128)
        {
            if (!dec128.IsValid(zoneIndex)) return null;
            return LiteralValue.HighPrecisionDecimalOf(
                ReadDecimalBigInteger(dec128, zoneIndex, byteWidth: 16),
                d128.Scale);
        }
        if (dtype is Decimal256Type d256 && cell is Decimal256Array dec256)
        {
            if (!dec256.IsValid(zoneIndex)) return null;
            return LiteralValue.HighPrecisionDecimalOf(
                ReadDecimalBigInteger(dec256, zoneIndex, byteWidth: 32),
                d256.Scale);
        }

        // Temporal: read raw underlying ticks, convert to the natural .NET
        // literal kind (DateOnly / TimeOnly / DateTimeOffset). We bypass
        // Apache.Arrow's GetDateTime/etc. accessors and read the integer
        // ticks directly — we already know the column's unit from `dtype`.
        if (dtype is Date32Type && cell is Date32Array date32)
        {
            if (!date32.IsValid(zoneIndex)) return null;
            int days = date32.Values[zoneIndex];
#if NET6_0_OR_GREATER
            return LiteralValue.Of(DateOnly.FromDayNumber(EpochDays + days));
#else
            return LiteralValue.Of((long)days);
#endif
        }
        if (dtype is Date64Type && cell is Date64Array date64)
        {
            if (!date64.IsValid(zoneIndex)) return null;
            long ms = date64.Values[zoneIndex];
#if NET6_0_OR_GREATER
            // Date64: signed milliseconds since 1970-01-01. Convert to days
            // (truncating to align on a calendar day; Arrow's spec stores
            // dates at midnight UTC anyway).
            long days = ms / 86_400_000L;
            return LiteralValue.Of(DateOnly.FromDayNumber(EpochDays + (int)days));
#else
            return LiteralValue.Of(ms);
#endif
        }
        if (dtype is Time32Type t32 && cell is Time32Array time32)
        {
            if (!time32.IsValid(zoneIndex)) return null;
            int v = time32.Values[zoneIndex];
#if NET6_0_OR_GREATER
            long dotnetTicks = t32.Unit switch
            {
                TimeUnit.Second => v * TimeSpan.TicksPerSecond,
                TimeUnit.Millisecond => v * TimeSpan.TicksPerMillisecond,
                _ => 0,
            };
            return LiteralValue.Of(new TimeOnly(dotnetTicks));
#else
            return LiteralValue.Of((long)v);
#endif
        }
        if (dtype is Time64Type t64 && cell is Time64Array time64)
        {
            if (!time64.IsValid(zoneIndex)) return null;
            long v = time64.Values[zoneIndex];
#if NET6_0_OR_GREATER
            long dotnetTicks = t64.Unit switch
            {
                TimeUnit.Microsecond => v * 10L,
                TimeUnit.Nanosecond => v / 100L,
                _ => 0L,
            };
            return LiteralValue.Of(new TimeOnly(dotnetTicks));
#else
            return LiteralValue.Of(v);
#endif
        }
        if (dtype is TimestampType ts && cell is TimestampArray timestamp)
        {
            if (!timestamp.IsValid(zoneIndex)) return null;
            long v = timestamp.Values[zoneIndex];
            // Convert unit-since-epoch to .NET ticks-since-epoch.
            long dotnetTicks = ts.Unit switch
            {
                TimeUnit.Second => v * TimeSpan.TicksPerSecond,
                TimeUnit.Millisecond => v * TimeSpan.TicksPerMillisecond,
                TimeUnit.Microsecond => v * 10L,
                TimeUnit.Nanosecond => v / 100L,
                _ => 0L,
            };
            return LiteralValue.Of(new DateTimeOffset(UnixEpochTicks + dotnetTicks, TimeSpan.Zero));
        }

        // Numeric primitives.
        switch (cell)
        {
            case Int8Array a:
                return a.IsValid(zoneIndex) ? (LiteralValue?)LiteralValue.Of((int)a.GetValue(zoneIndex)!.Value) : null;
            case Int16Array a:
                return a.IsValid(zoneIndex) ? (LiteralValue?)LiteralValue.Of((int)a.GetValue(zoneIndex)!.Value) : null;
            case Int32Array a:
                return a.IsValid(zoneIndex) ? (LiteralValue?)LiteralValue.Of(a.GetValue(zoneIndex)!.Value) : null;
            case Int64Array a:
                return a.IsValid(zoneIndex) ? (LiteralValue?)LiteralValue.Of(a.GetValue(zoneIndex)!.Value) : null;
            case UInt8Array a:
                return a.IsValid(zoneIndex) ? (LiteralValue?)LiteralValue.Of((uint)a.GetValue(zoneIndex)!.Value) : null;
            case UInt16Array a:
                return a.IsValid(zoneIndex) ? (LiteralValue?)LiteralValue.Of((uint)a.GetValue(zoneIndex)!.Value) : null;
            case UInt32Array a:
                return a.IsValid(zoneIndex) ? (LiteralValue?)LiteralValue.Of(a.GetValue(zoneIndex)!.Value) : null;
            case UInt64Array a:
                return a.IsValid(zoneIndex) ? (LiteralValue?)LiteralValue.Of(a.GetValue(zoneIndex)!.Value) : null;
            case FloatArray a:
                return a.IsValid(zoneIndex) ? (LiteralValue?)LiteralValue.Of(a.GetValue(zoneIndex)!.Value) : null;
            case DoubleArray a:
                return a.IsValid(zoneIndex) ? (LiteralValue?)LiteralValue.Of(a.GetValue(zoneIndex)!.Value) : null;
            case BooleanArray a:
                return a.IsValid(zoneIndex) ? (LiteralValue?)LiteralValue.Of(a.GetValue(zoneIndex)!.Value) : null;
            case StringArray a:
                return a.IsValid(zoneIndex) ? (LiteralValue?)LiteralValue.Of(a.GetString(zoneIndex)) : null;
            case BinaryArray a:
                return a.IsValid(zoneIndex) ? (LiteralValue?)LiteralValue.Of(a.GetBytes(zoneIndex).ToArray()) : null;
        }

        // Unknown Arrow type — caller treats null as Unknown and keeps the zone.
        return null;
    }

    /// <summary>
    /// Reads the raw little-endian bytes for the given decimal cell and
    /// constructs a signed <see cref="BigInteger"/>. Apache.Arrow stores
    /// Decimal128/256 values at <c>Data.Buffers[1]</c>, byteWidth bytes per
    /// element, in native (little-endian) form — exactly what
    /// <see cref="BigInteger(ReadOnlySpan{byte}, bool, bool)"/> consumes
    /// when <c>isUnsigned: false, isBigEndian: false</c>.
    /// </summary>
    private static BigInteger ReadDecimalBigInteger(
        Apache.Arrow.Array array, int zoneIndex, int byteWidth)
    {
        var span = array.Data.Buffers[1].Span.Slice(
            (array.Data.Offset + zoneIndex) * byteWidth, byteWidth);
#if NET6_0_OR_GREATER
        return new BigInteger(span, isUnsigned: false, isBigEndian: false);
#else
        // BigInteger(byte[]) on netstandard2.0: little-endian, signed.
        var bytes = new byte[byteWidth];
        span.CopyTo(bytes);
        return new BigInteger(bytes);
#endif
    }

    /// <summary>Days from .NET epoch (0001-01-01) to Unix epoch (1970-01-01).</summary>
    private const int EpochDays = 719_162;

    /// <summary>Unix epoch (1970-01-01 UTC) in .NET ticks (1 tick = 100ns).</summary>
    private const long UnixEpochTicks = 621_355_968_000_000_000L;
}

// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.Vortex;

/// <summary>
/// A pruning predicate that evaluates against per-zone stats to decide
/// which zones might contain matching rows. The reader uses this to skip
/// whole chunks at decode time — see
/// <see cref="VortexFileReader.ReadAllAsync(Predicate, System.Threading.CancellationToken)"/>.
///
/// <para>Predicates are conservative: a zone is kept whenever rows in it
/// COULD match (e.g., the zone's max ≥ threshold for a <c>&gt;=</c>
/// predicate). Zones are dropped only when their stats prove no row can
/// match. Zones whose stats are unavailable (missing zoned-stats layout,
/// empty/all-null batch with cleared min/max validity) are kept
/// conservatively — the decoded batch may still need a row-level filter.</para>
///
/// <para>Compose with <see cref="And(Predicate[])"/> /
/// <see cref="Or(Predicate[])"/>. Top-level evaluation produces an
/// <see cref="ISet{T}"/> of zone indices the reader yields.</para>
/// </summary>
public abstract class Predicate
{
    private protected Predicate() { }

    /// <summary>
    /// Returns the set of zone indices that may contain rows satisfying
    /// this predicate. Reader-internal — callers go through
    /// <see cref="VortexFileReader.ReadAllAsync(Predicate, System.Threading.CancellationToken)"/>.
    /// </summary>
    internal abstract Task<HashSet<int>> EvaluateZonesAsync(
        VortexFileReader reader, int totalZones, CancellationToken ct);

    // -------------------- Factory: comparisons --------------------

    /// <summary>
    /// <c>column &gt;= value</c>. Zone is kept when its <c>max</c> is null
    /// (unknown — keep conservatively) or <c>max &gt;= value</c>.
    /// </summary>
    public static Predicate GreaterOrEqual<T>(int columnIdx, T value) where T : struct
        => new ComparisonPredicate<T>(columnIdx, ComparisonOp.GreaterOrEqual, value);

    /// <summary><c>column &gt; value</c>.</summary>
    public static Predicate Greater<T>(int columnIdx, T value) where T : struct
        => new ComparisonPredicate<T>(columnIdx, ComparisonOp.Greater, value);

    /// <summary><c>column &lt;= value</c>.</summary>
    public static Predicate LessOrEqual<T>(int columnIdx, T value) where T : struct
        => new ComparisonPredicate<T>(columnIdx, ComparisonOp.LessOrEqual, value);

    /// <summary><c>column &lt; value</c>.</summary>
    public static Predicate Less<T>(int columnIdx, T value) where T : struct
        => new ComparisonPredicate<T>(columnIdx, ComparisonOp.Less, value);

    /// <summary>
    /// <c>column == value</c>. Zone is kept when <c>value</c> is in
    /// <c>[min, max]</c> (inclusive); discarded otherwise.
    /// </summary>
    public static Predicate Equal<T>(int columnIdx, T value) where T : struct
        => new ComparisonPredicate<T>(columnIdx, ComparisonOp.Equal, value);

    /// <summary>
    /// <c>column != value</c>. Zone is dropped only when
    /// <c>min == max == value</c> (every row matches the excluded value).
    /// </summary>
    public static Predicate NotEqual<T>(int columnIdx, T value) where T : struct
        => new ComparisonPredicate<T>(columnIdx, ComparisonOp.NotEqual, value);

    // -------------------- Factory: nullability --------------------

    /// <summary>
    /// <c>column IS NULL</c>. Zone is kept when <c>null_count &gt; 0</c>;
    /// dropped when <c>null_count == 0</c>. Always kept when null_count
    /// stat isn't available.
    /// </summary>
    public static Predicate IsNull(int columnIdx) => new IsNullPredicate(columnIdx, expectNull: true);

    /// <summary>
    /// <c>column IS NOT NULL</c>. Zone is kept when at least one row is
    /// non-null (we approximate via <c>null_count</c>: kept whenever
    /// it's missing or strictly less than the conservative zone size).
    /// </summary>
    public static Predicate IsNotNull(int columnIdx) => new IsNullPredicate(columnIdx, expectNull: false);

    // -------------------- Factory: composition --------------------

    /// <summary>Logical AND — accepted zones are the intersection.</summary>
    public static Predicate And(params Predicate[] predicates) => new AndPredicate(predicates);

    /// <summary>Logical OR — accepted zones are the union.</summary>
    public static Predicate Or(params Predicate[] predicates) => new OrPredicate(predicates);

    // -------------------- Internal helpers --------------------

    internal enum ComparisonOp { GreaterOrEqual, Greater, LessOrEqual, Less, Equal, NotEqual }

    /// <summary>
    /// Reads a per-zone numeric value from a typed Arrow array as a
    /// <see cref="double"/> for comparisons. Sufficient for our supported
    /// numeric types (i8..i64, u8..u64 fit in double up to 2^53; floats
    /// trivially). Returns null for invalid (cleared validity) cells.
    /// </summary>
    internal static double? ReadAsDouble(IArrowArray array, int index) => array switch
    {
        Int8Array a => a.IsValid(index) ? a.GetValue(index)!.Value : (double?)null,
        Int16Array a => a.IsValid(index) ? a.GetValue(index)!.Value : (double?)null,
        Int32Array a => a.IsValid(index) ? a.GetValue(index)!.Value : (double?)null,
        Int64Array a => a.IsValid(index) ? a.GetValue(index)!.Value : (double?)null,
        UInt8Array a => a.IsValid(index) ? a.GetValue(index)!.Value : (double?)null,
        UInt16Array a => a.IsValid(index) ? a.GetValue(index)!.Value : (double?)null,
        UInt32Array a => a.IsValid(index) ? a.GetValue(index)!.Value : (double?)null,
        UInt64Array a => a.IsValid(index) ? a.GetValue(index)!.Value : (double?)null,
        FloatArray a => a.IsValid(index) ? a.GetValue(index)!.Value : (double?)null,
        DoubleArray a => a.IsValid(index) ? a.GetValue(index)!.Value : (double?)null,
        _ => throw new NotSupportedException(
            $"Predicate evaluation doesn't yet handle Arrow array {array.GetType().Name}."),
    };

    internal static double ToDouble<T>(T value) where T : struct => value switch
    {
        sbyte v => v,
        byte v => v,
        short v => v,
        ushort v => v,
        int v => v,
        uint v => v,
        long v => v,
        ulong v => v,
        float v => v,
        double v => v,
        _ => throw new NotSupportedException(
            $"Predicate values of type {typeof(T).Name} are not supported."),
    };
}

internal sealed class ComparisonPredicate<T> : Predicate where T : struct
{
    private readonly int _columnIdx;
    private readonly ComparisonOp _op;
    private readonly double _value;

    public ComparisonPredicate(int columnIdx, ComparisonOp op, T value)
    {
        _columnIdx = columnIdx;
        _op = op;
        _value = ToDouble(value);
    }

    internal override async Task<HashSet<int>> EvaluateZonesAsync(
        VortexFileReader reader, int totalZones, CancellationToken ct)
    {
        var stats = await reader.GetZoneStatsAsync(_columnIdx, ct).ConfigureAwait(false);
        if (stats is null) return AllZones(totalZones);

        // Pruning rules per op:
        //   >=  K: drop zone where max < K. Need Max.
        //   >   K: drop zone where max <= K. Need Max.
        //   <=  K: drop zone where min > K. Need Min.
        //   <   K: drop zone where min >= K. Need Min.
        //   ==  K: drop zone where K < min OR K > max. Need both.
        //   !=  K: drop zone where min == max == K. Need both.
        // Zones with null min/max stay (conservative).
        var maxArr = stats.Max;
        var minArr = stats.Min;
        var accepted = new HashSet<int>();
        for (int z = 0; z < stats.ZoneCount; z++)
        {
            bool keep = true;
            switch (_op)
            {
                case ComparisonOp.GreaterOrEqual:
                    if (maxArr is not null && ReadAsDouble(maxArr, z) is double maxGe && maxGe < _value)
                        keep = false;
                    break;
                case ComparisonOp.Greater:
                    if (maxArr is not null && ReadAsDouble(maxArr, z) is double maxGt && maxGt <= _value)
                        keep = false;
                    break;
                case ComparisonOp.LessOrEqual:
                    if (minArr is not null && ReadAsDouble(minArr, z) is double minLe && minLe > _value)
                        keep = false;
                    break;
                case ComparisonOp.Less:
                    if (minArr is not null && ReadAsDouble(minArr, z) is double minLt && minLt >= _value)
                        keep = false;
                    break;
                case ComparisonOp.Equal:
                    if (maxArr is not null && ReadAsDouble(maxArr, z) is double maxEq && _value > maxEq) keep = false;
                    if (keep && minArr is not null && ReadAsDouble(minArr, z) is double minEq && _value < minEq) keep = false;
                    break;
                case ComparisonOp.NotEqual:
                    if (maxArr is not null && minArr is not null
                        && ReadAsDouble(maxArr, z) is double maxNe
                        && ReadAsDouble(minArr, z) is double minNe
                        && minNe == _value && maxNe == _value)
                    {
                        keep = false;
                    }
                    break;
            }
            if (keep) accepted.Add(z);
        }
        return accepted;
    }

    private static HashSet<int> AllZones(int total)
    {
        var s = new HashSet<int>();
        for (int i = 0; i < total; i++) s.Add(i);
        return s;
    }
}

internal sealed class IsNullPredicate : Predicate
{
    private readonly int _columnIdx;
    private readonly bool _expectNull;

    public IsNullPredicate(int columnIdx, bool expectNull)
    {
        _columnIdx = columnIdx;
        _expectNull = expectNull;
    }

    internal override async Task<HashSet<int>> EvaluateZonesAsync(
        VortexFileReader reader, int totalZones, CancellationToken ct)
    {
        var stats = await reader.GetZoneStatsAsync(_columnIdx, ct).ConfigureAwait(false);
        var nullCount = stats?.NullCount;
        var accepted = new HashSet<int>();
        if (stats is null || nullCount is null)
        {
            for (int i = 0; i < totalZones; i++) accepted.Add(i);
            return accepted;
        }
        // IS NULL: keep if null_count > 0.
        // IS NOT NULL: keep if null_count < zone_len (= "at least one row is non-null").
        // The trailing zone may be shorter than zone_len; "< zone_len" is a
        // conservative upper bound — we may keep an all-null trailing zone we
        // could have dropped, but never drop one that has non-null rows.
        for (int z = 0; z < stats.ZoneCount; z++)
        {
            ulong nc = nullCount.GetValue(z)!.Value;
            bool zoneHasNulls = nc > 0;
            bool zoneAllNulls = nc >= (ulong)stats.ZoneLen;
            bool keep = _expectNull ? zoneHasNulls : !zoneAllNulls;
            if (keep) accepted.Add(z);
        }
        return accepted;
    }
}

internal sealed class AndPredicate : Predicate
{
    private readonly Predicate[] _children;
    public AndPredicate(Predicate[] children) { _children = children; }

    internal override async Task<HashSet<int>> EvaluateZonesAsync(
        VortexFileReader reader, int totalZones, CancellationToken ct)
    {
        if (_children.Length == 0)
        {
            var all = new HashSet<int>();
            for (int i = 0; i < totalZones; i++) all.Add(i);
            return all;
        }
        var acc = await _children[0].EvaluateZonesAsync(reader, totalZones, ct).ConfigureAwait(false);
        for (int i = 1; i < _children.Length; i++)
        {
            var next = await _children[i].EvaluateZonesAsync(reader, totalZones, ct).ConfigureAwait(false);
            acc.IntersectWith(next);
            if (acc.Count == 0) return acc; // early-exit on empty intersection
        }
        return acc;
    }
}

internal sealed class OrPredicate : Predicate
{
    private readonly Predicate[] _children;
    public OrPredicate(Predicate[] children) { _children = children; }

    internal override async Task<HashSet<int>> EvaluateZonesAsync(
        VortexFileReader reader, int totalZones, CancellationToken ct)
    {
        var acc = new HashSet<int>();
        foreach (var p in _children)
        {
            var next = await p.EvaluateZonesAsync(reader, totalZones, ct).ConfigureAwait(false);
            acc.UnionWith(next);
            if (acc.Count >= totalZones) return acc; // saturated
        }
        return acc;
    }
}

// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

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

    // -------------------- Factory: string + binary comparisons --------------------
    //
    // Compare lexicographically against the column's per-zone Min / Max, which
    // are themselves StringArray / BinaryArray. Bytes are compared via
    // ReadOnlySpan<byte>.SequenceCompareTo — UTF-8 byte ordering matches code-
    // point ordering for code points ≤ U+007F (ASCII) and for any prefix-free
    // pair of UTF-8-encoded strings, which covers most practical predicates.
    // For non-ASCII text whose collation differs from byte-wise (e.g. locale-
    // sensitive sorts), the predicate is conservative — false positives are
    // possible but never false negatives, so a row-level filter on the
    // resulting batches still yields correct output.

    /// <summary><c>column &gt;= value</c> (lexicographic UTF-8 byte order).</summary>
    public static Predicate GreaterOrEqual(int columnIdx, string value)
        => new BytesComparisonPredicate(columnIdx, ComparisonOp.GreaterOrEqual,
            System.Text.Encoding.UTF8.GetBytes(value ?? throw new ArgumentNullException(nameof(value))));

    /// <summary><c>column &gt; value</c> (lexicographic UTF-8 byte order).</summary>
    public static Predicate Greater(int columnIdx, string value)
        => new BytesComparisonPredicate(columnIdx, ComparisonOp.Greater,
            System.Text.Encoding.UTF8.GetBytes(value ?? throw new ArgumentNullException(nameof(value))));

    /// <summary><c>column &lt;= value</c> (lexicographic UTF-8 byte order).</summary>
    public static Predicate LessOrEqual(int columnIdx, string value)
        => new BytesComparisonPredicate(columnIdx, ComparisonOp.LessOrEqual,
            System.Text.Encoding.UTF8.GetBytes(value ?? throw new ArgumentNullException(nameof(value))));

    /// <summary><c>column &lt; value</c> (lexicographic UTF-8 byte order).</summary>
    public static Predicate Less(int columnIdx, string value)
        => new BytesComparisonPredicate(columnIdx, ComparisonOp.Less,
            System.Text.Encoding.UTF8.GetBytes(value ?? throw new ArgumentNullException(nameof(value))));

    /// <summary><c>column == value</c> (UTF-8 byte equality).</summary>
    public static Predicate Equal(int columnIdx, string value)
        => new BytesComparisonPredicate(columnIdx, ComparisonOp.Equal,
            System.Text.Encoding.UTF8.GetBytes(value ?? throw new ArgumentNullException(nameof(value))));

    /// <summary><c>column != value</c> (UTF-8 byte equality).</summary>
    public static Predicate NotEqual(int columnIdx, string value)
        => new BytesComparisonPredicate(columnIdx, ComparisonOp.NotEqual,
            System.Text.Encoding.UTF8.GetBytes(value ?? throw new ArgumentNullException(nameof(value))));

    /// <summary><c>column &gt;= value</c> (lexicographic byte order on Binary columns).</summary>
    public static Predicate GreaterOrEqual(int columnIdx, byte[] value)
        => new BytesComparisonPredicate(columnIdx, ComparisonOp.GreaterOrEqual,
            value ?? throw new ArgumentNullException(nameof(value)));

    /// <summary><c>column &gt; value</c> (lexicographic byte order on Binary columns).</summary>
    public static Predicate Greater(int columnIdx, byte[] value)
        => new BytesComparisonPredicate(columnIdx, ComparisonOp.Greater,
            value ?? throw new ArgumentNullException(nameof(value)));

    /// <summary><c>column &lt;= value</c> (lexicographic byte order on Binary columns).</summary>
    public static Predicate LessOrEqual(int columnIdx, byte[] value)
        => new BytesComparisonPredicate(columnIdx, ComparisonOp.LessOrEqual,
            value ?? throw new ArgumentNullException(nameof(value)));

    /// <summary><c>column &lt; value</c> (lexicographic byte order on Binary columns).</summary>
    public static Predicate Less(int columnIdx, byte[] value)
        => new BytesComparisonPredicate(columnIdx, ComparisonOp.Less,
            value ?? throw new ArgumentNullException(nameof(value)));

    /// <summary><c>column == value</c> (byte equality on Binary columns).</summary>
    public static Predicate Equal(int columnIdx, byte[] value)
        => new BytesComparisonPredicate(columnIdx, ComparisonOp.Equal,
            value ?? throw new ArgumentNullException(nameof(value)));

    /// <summary><c>column != value</c> (byte equality on Binary columns).</summary>
    public static Predicate NotEqual(int columnIdx, byte[] value)
        => new BytesComparisonPredicate(columnIdx, ComparisonOp.NotEqual,
            value ?? throw new ArgumentNullException(nameof(value)));

    // -------------------- Factory: bool comparisons --------------------

    /// <summary>
    /// <c>column == value</c> on a <see cref="BooleanType"/> column. Zone is
    /// dropped only when min == max ≠ value (every row in the zone disagrees);
    /// kept otherwise. Equivalent to a numeric Equal but reads <c>Min</c> /
    /// <c>Max</c> as <see cref="BooleanArray"/>.
    /// </summary>
    public static Predicate Equal(int columnIdx, bool value)
        => new BoolComparisonPredicate(columnIdx, ComparisonOp.Equal, value);

    /// <summary>
    /// <c>column != value</c> on a <see cref="BooleanType"/> column. Drops a
    /// zone only when every row matches the excluded value (<c>min == max == value</c>).
    /// </summary>
    public static Predicate NotEqual(int columnIdx, bool value)
        => new BoolComparisonPredicate(columnIdx, ComparisonOp.NotEqual, value);

    // -------------------- Factory: temporal comparisons --------------------
    //
    // DateTime overloads target Date32 / Date64 columns. The input's calendar
    // date (DateTimeKind is ignored) is converted to days-since-epoch (Date32)
    // or milliseconds-since-epoch (Date64) at evaluation time, using the
    // column's actual dtype from the schema. This makes the same predicate
    // work against either width without callers needing to know which.
    //
    // DateTimeOffset overloads target Timestamp columns; the offset is
    // collapsed to UTC and converted to the column's TimeUnit ticks.
    //
    // TimeSpan overloads target Time32 / Time64 columns and convert to the
    // column's TimeUnit ticks (raw, not epoch-anchored).

    /// <summary><c>column &gt;= value</c> on a Date32 / Date64 column.</summary>
    public static Predicate GreaterOrEqual(int columnIdx, DateTime value)
        => new TemporalComparisonPredicate(columnIdx, ComparisonOp.GreaterOrEqual, TemporalKind.Date, value.Ticks);

    /// <summary><c>column &gt; value</c> on a Date32 / Date64 column.</summary>
    public static Predicate Greater(int columnIdx, DateTime value)
        => new TemporalComparisonPredicate(columnIdx, ComparisonOp.Greater, TemporalKind.Date, value.Ticks);

    /// <summary><c>column &lt;= value</c> on a Date32 / Date64 column.</summary>
    public static Predicate LessOrEqual(int columnIdx, DateTime value)
        => new TemporalComparisonPredicate(columnIdx, ComparisonOp.LessOrEqual, TemporalKind.Date, value.Ticks);

    /// <summary><c>column &lt; value</c> on a Date32 / Date64 column.</summary>
    public static Predicate Less(int columnIdx, DateTime value)
        => new TemporalComparisonPredicate(columnIdx, ComparisonOp.Less, TemporalKind.Date, value.Ticks);

    /// <summary><c>column == value</c> on a Date32 / Date64 column.</summary>
    public static Predicate Equal(int columnIdx, DateTime value)
        => new TemporalComparisonPredicate(columnIdx, ComparisonOp.Equal, TemporalKind.Date, value.Ticks);

    /// <summary><c>column != value</c> on a Date32 / Date64 column.</summary>
    public static Predicate NotEqual(int columnIdx, DateTime value)
        => new TemporalComparisonPredicate(columnIdx, ComparisonOp.NotEqual, TemporalKind.Date, value.Ticks);

    /// <summary><c>column &gt;= value</c> on a Timestamp column.</summary>
    public static Predicate GreaterOrEqual(int columnIdx, DateTimeOffset value)
        => new TemporalComparisonPredicate(columnIdx, ComparisonOp.GreaterOrEqual, TemporalKind.Timestamp, value.UtcTicks);

    /// <summary><c>column &gt; value</c> on a Timestamp column.</summary>
    public static Predicate Greater(int columnIdx, DateTimeOffset value)
        => new TemporalComparisonPredicate(columnIdx, ComparisonOp.Greater, TemporalKind.Timestamp, value.UtcTicks);

    /// <summary><c>column &lt;= value</c> on a Timestamp column.</summary>
    public static Predicate LessOrEqual(int columnIdx, DateTimeOffset value)
        => new TemporalComparisonPredicate(columnIdx, ComparisonOp.LessOrEqual, TemporalKind.Timestamp, value.UtcTicks);

    /// <summary><c>column &lt; value</c> on a Timestamp column.</summary>
    public static Predicate Less(int columnIdx, DateTimeOffset value)
        => new TemporalComparisonPredicate(columnIdx, ComparisonOp.Less, TemporalKind.Timestamp, value.UtcTicks);

    /// <summary><c>column == value</c> on a Timestamp column.</summary>
    public static Predicate Equal(int columnIdx, DateTimeOffset value)
        => new TemporalComparisonPredicate(columnIdx, ComparisonOp.Equal, TemporalKind.Timestamp, value.UtcTicks);

    /// <summary><c>column != value</c> on a Timestamp column.</summary>
    public static Predicate NotEqual(int columnIdx, DateTimeOffset value)
        => new TemporalComparisonPredicate(columnIdx, ComparisonOp.NotEqual, TemporalKind.Timestamp, value.UtcTicks);

    /// <summary><c>column &gt;= value</c> on a Time32 / Time64 column.</summary>
    public static Predicate GreaterOrEqual(int columnIdx, TimeSpan value)
        => new TemporalComparisonPredicate(columnIdx, ComparisonOp.GreaterOrEqual, TemporalKind.Time, value.Ticks);

    /// <summary><c>column &gt; value</c> on a Time32 / Time64 column.</summary>
    public static Predicate Greater(int columnIdx, TimeSpan value)
        => new TemporalComparisonPredicate(columnIdx, ComparisonOp.Greater, TemporalKind.Time, value.Ticks);

    /// <summary><c>column &lt;= value</c> on a Time32 / Time64 column.</summary>
    public static Predicate LessOrEqual(int columnIdx, TimeSpan value)
        => new TemporalComparisonPredicate(columnIdx, ComparisonOp.LessOrEqual, TemporalKind.Time, value.Ticks);

    /// <summary><c>column &lt; value</c> on a Time32 / Time64 column.</summary>
    public static Predicate Less(int columnIdx, TimeSpan value)
        => new TemporalComparisonPredicate(columnIdx, ComparisonOp.Less, TemporalKind.Time, value.Ticks);

    /// <summary><c>column == value</c> on a Time32 / Time64 column.</summary>
    public static Predicate Equal(int columnIdx, TimeSpan value)
        => new TemporalComparisonPredicate(columnIdx, ComparisonOp.Equal, TemporalKind.Time, value.Ticks);

    /// <summary><c>column != value</c> on a Time32 / Time64 column.</summary>
    public static Predicate NotEqual(int columnIdx, TimeSpan value)
        => new TemporalComparisonPredicate(columnIdx, ComparisonOp.NotEqual, TemporalKind.Time, value.Ticks);

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

    internal static HashSet<int> AllZones(int total)
    {
        var s = new HashSet<int>();
        for (int i = 0; i < total; i++) s.Add(i);
        return s;
    }
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
        if (stats is null) return Predicate.AllZones(totalZones);

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

}

/// <summary>
/// Lexicographic byte comparison predicate against a <see cref="StringArray"/>
/// or <see cref="BinaryArray"/> column's per-zone <c>Min</c> / <c>Max</c>
/// stats. Strings are encoded as UTF-8 bytes once and compared via
/// <see cref="MemoryExtensions.SequenceCompareTo{T}"/>.
/// </summary>
internal sealed class BytesComparisonPredicate : Predicate
{
    private readonly int _columnIdx;
    private readonly ComparisonOp _op;
    private readonly byte[] _value;

    public BytesComparisonPredicate(int columnIdx, ComparisonOp op, byte[] value)
    {
        _columnIdx = columnIdx;
        _op = op;
        _value = value;
    }

    internal override async Task<HashSet<int>> EvaluateZonesAsync(
        VortexFileReader reader, int totalZones, CancellationToken ct)
    {
        var stats = await reader.GetZoneStatsAsync(_columnIdx, ct).ConfigureAwait(false);
        if (stats is null) return Predicate.AllZones(totalZones);

        // Same pruning rules as the numeric path; the only change is that
        // CompareCellTo lexicographically compares bytes instead of going
        // through ReadAsDouble. Zones whose Min / Max validity is cleared
        // (empty / all-null batch) are kept conservatively — the cell
        // returns null and the relevant rule short-circuits as "keep".
        var maxArr = stats.Max;
        var minArr = stats.Min;
        var accepted = new HashSet<int>();
        for (int z = 0; z < stats.ZoneCount; z++)
        {
            bool keep = true;
            switch (_op)
            {
                case ComparisonOp.GreaterOrEqual:
                    if (CompareCellTo(maxArr, z) is int cmpGe && cmpGe < 0) keep = false;
                    break;
                case ComparisonOp.Greater:
                    if (CompareCellTo(maxArr, z) is int cmpGt && cmpGt <= 0) keep = false;
                    break;
                case ComparisonOp.LessOrEqual:
                    if (CompareCellTo(minArr, z) is int cmpLe && cmpLe > 0) keep = false;
                    break;
                case ComparisonOp.Less:
                    if (CompareCellTo(minArr, z) is int cmpLt && cmpLt >= 0) keep = false;
                    break;
                case ComparisonOp.Equal:
                    // Drop only when value > max (cell vs value < 0) or value < min (cell > 0).
                    if (CompareCellTo(maxArr, z) is int cmpEqMax && cmpEqMax < 0) keep = false;
                    if (keep && CompareCellTo(minArr, z) is int cmpEqMin && cmpEqMin > 0) keep = false;
                    break;
                case ComparisonOp.NotEqual:
                    // Drop only when min == max == value.
                    var minCmp = CompareCellTo(minArr, z);
                    var maxCmp = CompareCellTo(maxArr, z);
                    if (minCmp == 0 && maxCmp == 0) keep = false;
                    break;
            }
            if (keep) accepted.Add(z);
        }
        return accepted;
    }

    /// <summary>
    /// Returns <c>cell.SequenceCompareTo(_value)</c> for valid cells, null
    /// for invalid cells, null when <paramref name="array"/> is null
    /// (column has no Min / Max stats). Caller treats null as "keep
    /// conservatively."
    /// </summary>
    private int? CompareCellTo(IArrowArray? array, int z)
    {
        if (array is null) return null;
        ReadOnlySpan<byte> value = _value;
        return array switch
        {
            StringArray s => s.IsValid(z) ? s.GetBytes(z).SequenceCompareTo(value) : (int?)null,
            BinaryArray b => b.IsValid(z) ? b.GetBytes(z).SequenceCompareTo(value) : (int?)null,
            _ => throw new NotSupportedException(
                $"BytesComparisonPredicate doesn't handle Arrow array {array.GetType().Name}; "
                + "string/binary predicates require StringArray or BinaryArray Min/Max stats."),
        };
    }
}

/// <summary>Tags whether a temporal predicate target is a date (epoch days/ms),
/// a timestamp (epoch ticks in unit), or a time-of-day (raw ticks in unit).</summary>
internal enum TemporalKind { Date, Timestamp, Time }

/// <summary>
/// Predicate over Date32 / Date64 / Timestamp / Time32 / Time64 columns. The
/// user-supplied value is captured as raw .NET ticks (DateTime.Ticks /
/// DateTimeOffset.UtcTicks / TimeSpan.Ticks); at evaluation time we look up
/// the column's actual Arrow dtype, convert the .NET ticks to the column's
/// unit (epoch-anchored for date / timestamp, raw for time-of-day), and
/// compare against the typed Min / Max from zone stats.
/// </summary>
internal sealed class TemporalComparisonPredicate : Predicate
{
    // Unix epoch in .NET ticks (1 tick = 100ns; epoch = 1970-01-01 UTC).
    private const long UnixEpochTicks = 621355968000000000L;
    private const long TicksPerSecond = 10_000_000L;
    private const long TicksPerMillisecond = 10_000L;
    private const long TicksPerMicrosecond = 10L;
    // Conversion *from* nanoseconds — i.e. nanos = .NET ticks * 100.
    private const long NanosPerTick = 100L;
    private const long TicksPerDay = 864_000_000_000L;

    private readonly int _columnIdx;
    private readonly ComparisonOp _op;
    private readonly TemporalKind _kind;
    private readonly long _dotnetTicks;

    public TemporalComparisonPredicate(int columnIdx, ComparisonOp op, TemporalKind kind, long dotnetTicks)
    {
        _columnIdx = columnIdx;
        _op = op;
        _kind = kind;
        _dotnetTicks = dotnetTicks;
    }

    internal override async Task<HashSet<int>> EvaluateZonesAsync(
        VortexFileReader reader, int totalZones, CancellationToken ct)
    {
        var stats = await reader.GetZoneStatsAsync(_columnIdx, ct).ConfigureAwait(false);
        if (stats is null) return Predicate.AllZones(totalZones);

        var dtype = reader.Schema.FieldsList[_columnIdx].DataType;
        if (!TryConvertToColumnUnit(dtype, out long target))
        {
            // Predicate kind doesn't match column type — keep all zones
            // conservatively. The decoded batches will reflect the dtype
            // mismatch through normal Arrow type errors at consume time.
            return Predicate.AllZones(totalZones);
        }

        var minArr = stats.Min;
        var maxArr = stats.Max;
        var accepted = new HashSet<int>();
        for (int z = 0; z < stats.ZoneCount; z++)
        {
            bool keep = true;
            switch (_op)
            {
                case ComparisonOp.GreaterOrEqual:
                    if (ReadAsLong(maxArr, z) is long maxGe && maxGe < target) keep = false;
                    break;
                case ComparisonOp.Greater:
                    if (ReadAsLong(maxArr, z) is long maxGt && maxGt <= target) keep = false;
                    break;
                case ComparisonOp.LessOrEqual:
                    if (ReadAsLong(minArr, z) is long minLe && minLe > target) keep = false;
                    break;
                case ComparisonOp.Less:
                    if (ReadAsLong(minArr, z) is long minLt && minLt >= target) keep = false;
                    break;
                case ComparisonOp.Equal:
                    if (ReadAsLong(maxArr, z) is long maxEq && target > maxEq) keep = false;
                    if (keep && ReadAsLong(minArr, z) is long minEq && target < minEq) keep = false;
                    break;
                case ComparisonOp.NotEqual:
                    if (ReadAsLong(maxArr, z) is long maxNe
                        && ReadAsLong(minArr, z) is long minNe
                        && minNe == target && maxNe == target)
                    {
                        keep = false;
                    }
                    break;
            }
            if (keep) accepted.Add(z);
        }
        return accepted;
    }

    /// <summary>
    /// Reads a single zone-stat cell from a typed Date / Time / Timestamp
    /// Arrow array as the underlying integer ticks (days for Date32, ms for
    /// Date64, unit-ticks for Time/Timestamp). Returns null for invalid
    /// cells or when <paramref name="array"/> is null.
    /// </summary>
    private static long? ReadAsLong(IArrowArray? array, int index)
    {
        if (array is null) return null;
        return array switch
        {
            // Apache.Arrow's Date32Array / Time32Array inherit from
            // PrimitiveArray<int>; Date64Array / Time64Array / TimestampArray
            // inherit from PrimitiveArray<long>. We read .Values directly to
            // avoid Arrow's GetValue(...) overloads that may convert to
            // DateTime / DateTimeOffset and lose the raw underlying ticks.
            Apache.Arrow.PrimitiveArray<int> p32 => p32.IsValid(index) ? p32.Values[index] : (long?)null,
            Apache.Arrow.PrimitiveArray<long> p64 => p64.IsValid(index) ? p64.Values[index] : (long?)null,
            _ => null,
        };
    }

    /// <summary>
    /// Converts the captured .NET ticks to the column's unit ticks. Returns
    /// false (caller keeps all zones) when the predicate kind is mismatched
    /// against the column dtype.
    /// </summary>
    private bool TryConvertToColumnUnit(IArrowType dtype, out long result)
    {
        switch (_kind)
        {
            case TemporalKind.Date when dtype is Date32Type:
                // i32 days since epoch.
                result = (_dotnetTicks - UnixEpochTicks) / TicksPerDay;
                return true;
            case TemporalKind.Date when dtype is Date64Type:
                // i64 milliseconds since epoch.
                result = (_dotnetTicks - UnixEpochTicks) / TicksPerMillisecond;
                return true;
            case TemporalKind.Timestamp when dtype is TimestampType ts:
                // i64 unit-ticks since epoch.
                result = ConvertEpochAnchored(_dotnetTicks, ts.Unit);
                return true;
            case TemporalKind.Time when dtype is Time32Type t32:
                result = ConvertRawTime(_dotnetTicks, t32.Unit);
                return true;
            case TemporalKind.Time when dtype is Time64Type t64:
                result = ConvertRawTime(_dotnetTicks, t64.Unit);
                return true;
            default:
                result = 0;
                return false;
        }
    }

    private static long ConvertEpochAnchored(long dotnetTicks, TimeUnit unit) => unit switch
    {
        TimeUnit.Second => (dotnetTicks - UnixEpochTicks) / TicksPerSecond,
        TimeUnit.Millisecond => (dotnetTicks - UnixEpochTicks) / TicksPerMillisecond,
        TimeUnit.Microsecond => (dotnetTicks - UnixEpochTicks) / TicksPerMicrosecond,
        TimeUnit.Nanosecond => checked((dotnetTicks - UnixEpochTicks) * NanosPerTick),
        _ => throw new NotSupportedException($"Unknown TimeUnit: {unit}."),
    };

    private static long ConvertRawTime(long dotnetTicks, TimeUnit unit) => unit switch
    {
        TimeUnit.Second => dotnetTicks / TicksPerSecond,
        TimeUnit.Millisecond => dotnetTicks / TicksPerMillisecond,
        TimeUnit.Microsecond => dotnetTicks / TicksPerMicrosecond,
        TimeUnit.Nanosecond => checked(dotnetTicks * NanosPerTick),
        _ => throw new NotSupportedException($"Unknown TimeUnit: {unit}."),
    };
}

/// <summary>
/// Predicate over <see cref="BooleanType"/> columns. Reads <c>Min</c> /
/// <c>Max</c> as <see cref="BooleanArray"/> and applies the standard
/// equal / not-equal pruning rules.
/// </summary>
internal sealed class BoolComparisonPredicate : Predicate
{
    private readonly int _columnIdx;
    private readonly ComparisonOp _op;
    private readonly bool _value;

    public BoolComparisonPredicate(int columnIdx, ComparisonOp op, bool value)
    {
        _columnIdx = columnIdx;
        _op = op;
        _value = value;
    }

    internal override async Task<HashSet<int>> EvaluateZonesAsync(
        VortexFileReader reader, int totalZones, CancellationToken ct)
    {
        var stats = await reader.GetZoneStatsAsync(_columnIdx, ct).ConfigureAwait(false);
        if (stats is null) return Predicate.AllZones(totalZones);

        var minArr = stats.Min as BooleanArray;
        var maxArr = stats.Max as BooleanArray;
        var accepted = new HashSet<int>();
        for (int z = 0; z < stats.ZoneCount; z++)
        {
            bool? min = (minArr is not null && minArr.IsValid(z)) ? minArr.GetValue(z) : (bool?)null;
            bool? max = (maxArr is not null && maxArr.IsValid(z)) ? maxArr.GetValue(z) : (bool?)null;

            bool keep;
            if (_op == ComparisonOp.Equal)
            {
                // Drop only when min == max != value (every row disagrees).
                keep = !(min.HasValue && max.HasValue && min.Value == max.Value && min.Value != _value);
            }
            else // NotEqual
            {
                // Drop only when min == max == value (every row matches the excluded).
                keep = !(min.HasValue && max.HasValue && min.Value == max.Value && min.Value == _value);
            }
            if (keep) accepted.Add(z);
        }
        return accepted;
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

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table.Stats;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class StatsCollectorTests
{
    [Fact]
    public void Collect_IntegerColumn()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var ids = new Int64Array.Builder()
            .Append(10).Append(5).Append(20).Build();
        var batch = new RecordBatch(schema, [ids], 3);

        string? stats = StatsCollector.Collect(batch);
        Assert.NotNull(stats);

        var doc = JsonDocument.Parse(stats);
        Assert.Equal(3, doc.RootElement.GetProperty("numRecords").GetInt64());
        Assert.Equal(5, doc.RootElement.GetProperty("minValues").GetProperty("id").GetInt64());
        Assert.Equal(20, doc.RootElement.GetProperty("maxValues").GetProperty("id").GetInt64());
        Assert.Equal(0, doc.RootElement.GetProperty("nullCount").GetProperty("id").GetInt64());
    }

    [Fact]
    public void Collect_StringColumn()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", StringType.Default, true))
            .Build();

        var names = new StringArray.Builder()
            .Append("charlie").Append("alice").AppendNull().Append("bob").Build();
        var batch = new RecordBatch(schema, [names], 4);

        string? stats = StatsCollector.Collect(batch);
        Assert.NotNull(stats);

        var doc = JsonDocument.Parse(stats);
        Assert.Equal(4, doc.RootElement.GetProperty("numRecords").GetInt64());
        Assert.Equal("alice", doc.RootElement.GetProperty("minValues").GetProperty("name").GetString());
        Assert.Equal("charlie", doc.RootElement.GetProperty("maxValues").GetProperty("name").GetString());
        Assert.Equal(1, doc.RootElement.GetProperty("nullCount").GetProperty("name").GetInt64());
    }

    [Fact]
    public void Collect_MultipleColumns()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("value", DoubleType.Default, true))
            .Build();

        var ids = new Int32Array.Builder().Append(1).Append(2).Append(3).Build();
        var values = new DoubleArray.Builder().Append(1.5).AppendNull().Append(3.7).Build();
        var batch = new RecordBatch(schema, [ids, values], 3);

        string? stats = StatsCollector.Collect(batch);
        Assert.NotNull(stats);

        var doc = JsonDocument.Parse(stats);
        Assert.Equal(1, doc.RootElement.GetProperty("minValues").GetProperty("id").GetInt32());
        Assert.Equal(3, doc.RootElement.GetProperty("maxValues").GetProperty("id").GetInt32());
        Assert.Equal(1.5, doc.RootElement.GetProperty("minValues").GetProperty("value").GetDouble(), 5);
        Assert.Equal(3.7, doc.RootElement.GetProperty("maxValues").GetProperty("value").GetDouble(), 5);
        Assert.Equal(1, doc.RootElement.GetProperty("nullCount").GetProperty("value").GetInt64());
    }

    [Fact]
    public void Collect_EmptyBatch_ReturnsNull()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var ids = new Int64Array.Builder().Build();
        var batch = new RecordBatch(schema, [ids], 0);

        Assert.Null(StatsCollector.Collect(batch));
    }

    [Fact]
    public void Collect_DateColumn_EmitsIsoStrings()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("d", Date32Type.Default, true))
            .Build();

        var dates = new Date32Array.Builder()
            .Append(new DateTime(2021, 6, 1, 0, 0, 0, DateTimeKind.Utc))
            .Append(new DateTime(2021, 1, 1, 0, 0, 0, DateTimeKind.Utc))
            .AppendNull()
            .Append(new DateTime(2021, 12, 31, 0, 0, 0, DateTimeKind.Utc))
            .Build();
        var batch = new RecordBatch(schema, [dates], 4);

        string? stats = StatsCollector.Collect(batch);
        Assert.NotNull(stats);

        var doc = JsonDocument.Parse(stats);
        var min = doc.RootElement.GetProperty("minValues").GetProperty("d");
        var max = doc.RootElement.GetProperty("maxValues").GetProperty("d");
        // Delta stores date bounds as "yyyy-MM-dd" STRINGS (a raw day number is not decodable and never
        // prunes) — this is the format Spark writes and EW's DeltaLiteralDecoder reads.
        Assert.Equal(JsonValueKind.String, min.ValueKind);
        Assert.Equal("2021-01-01", min.GetString());
        Assert.Equal("2021-12-31", max.GetString());
        Assert.Equal(1, doc.RootElement.GetProperty("nullCount").GetProperty("d").GetInt64());
    }

    private static RecordBatch Decimal128Batch(
        string name, int precision, int scale, params BigInteger?[] unscaled)
    {
        var type = new Decimal128Type(precision, scale);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field(name, type, true)).Build();
        const int w = 16;
        var bytes = new byte[unscaled.Length * w];
        var nulls = new ArrowBuffer.BitmapBuilder();
        int nullCount = 0;
        for (int i = 0; i < unscaled.Length; i++)
        {
            if (unscaled[i] is null) { nulls.Append(false); nullCount++; continue; }
            nulls.Append(true);
            var bi = unscaled[i]!.Value;
            var dest = bytes.AsSpan(i * w, w);
            dest.Fill(bi.Sign < 0 ? (byte)0xFF : (byte)0x00);
#if NET6_0_OR_GREATER
            bi.TryWriteBytes(dest, out _, isUnsigned: false, isBigEndian: false);
#else
            var bb = bi.ToByteArray();
            bb.AsSpan(0, Math.Min(bb.Length, w)).CopyTo(dest);
#endif
        }
        var data = new ArrayData(type, unscaled.Length, nullCount, 0,
            [nulls.Build(), new ArrowBuffer(bytes)]);
        return new RecordBatch(schema, [new Decimal128Array(data)], unscaled.Length);
    }

    [Fact]
    public void Collect_DecimalColumn_EmitsJsonNumbers()
    {
        // decimal(12,2): unscaled 1234 / 5678 -> 12.34 / 56.78, written as JSON NUMBERS (the form Delta
        // uses and can decode/prune on).
        var batch = Decimal128Batch("amt", 12, 2, 1234, 5678, null);

        string? stats = StatsCollector.Collect(batch);
        Assert.NotNull(stats);

        var doc = JsonDocument.Parse(stats);
        var min = doc.RootElement.GetProperty("minValues").GetProperty("amt");
        var max = doc.RootElement.GetProperty("maxValues").GetProperty("amt");
        Assert.Equal(JsonValueKind.Number, min.ValueKind);
        Assert.Equal(12.34m, min.GetDecimal());
        Assert.Equal(56.78m, max.GetDecimal());
        Assert.Equal(1, doc.RootElement.GetProperty("nullCount").GetProperty("amt").GetInt64());
    }

    [Fact]
    public void Collect_DecimalColumn_HighPrecision_PreservedAsRawNumber()
    {
        // decimal(38,0): 10^31 exceeds System.Decimal and must survive as a raw 32-digit JSON number,
        // exactly as Spark writes it.
        var huge = BigInteger.Pow(10, 31);
        var batch = Decimal128Batch("big", 38, 0, huge, 5);

        string? stats = StatsCollector.Collect(batch);
        Assert.NotNull(stats);

        var doc = JsonDocument.Parse(stats);
        var min = doc.RootElement.GetProperty("minValues").GetProperty("big");
        var max = doc.RootElement.GetProperty("maxValues").GetProperty("big");
        Assert.Equal(JsonValueKind.Number, max.ValueKind);
        Assert.Equal("5", min.GetRawText());
        Assert.Equal("1" + new string('0', 31), max.GetRawText()); // 10^31, full precision preserved
    }

    [Fact]
    public void Collect_DateColumn_MergesAcrossBatches()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("d", Date32Type.Default, false))
            .Build();

        RecordBatch Batch(params DateTime[] days)
        {
            var b = new Date32Array.Builder();
            foreach (var d in days) b.Append(DateTime.SpecifyKind(d, DateTimeKind.Utc));
            return new RecordBatch(schema, [b.Build()], days.Length);
        }

        string? stats = StatsCollector.Collect(
        [
            Batch(new DateTime(2020, 5, 5), new DateTime(2020, 8, 8)),
            Batch(new DateTime(2019, 1, 1), new DateTime(2021, 3, 3)),
        ]);
        Assert.NotNull(stats);

        var doc = JsonDocument.Parse(stats);
        Assert.Equal("2019-01-01", doc.RootElement.GetProperty("minValues").GetProperty("d").GetString());
        Assert.Equal("2021-03-03", doc.RootElement.GetProperty("maxValues").GetProperty("d").GetString());
    }

    // ── Timestamp bound rounding ──────────────────────────────────────────────────────────────────
    //
    // Delta stats carry timestamps as microsecond ISO-8601 strings, so a nanosecond column's
    // sub-microsecond digits cannot be represented. Dropping them is only safe if the bounds round
    // OUTWARD; truncating toward zero (plain integer division) moves the max down for positive
    // timestamps and the min up for negative ones, excluding a value that is in the file.

    // DateTimeOffset.UnixEpoch does not exist on net472.
    private static readonly DateTimeOffset UnixEpoch =
        new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static RecordBatch TimestampBatch(TimeUnit unit, string? timezone, params long[] values)
    {
        var type = new TimestampType(unit, timezone);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("ts", type, true)).Build();

        var buffer = new ArrowBuffer.Builder<long>();
        foreach (long v in values) buffer.Append(v);
        var validity = new ArrowBuffer.BitmapBuilder();
        for (int i = 0; i < values.Length; i++) validity.Append(true);

        var data = new ArrayData(type, values.Length, 0, 0, [validity.Build(), buffer.Build()]);
        return new RecordBatch(schema, [new TimestampArray(data)], values.Length);
    }

    private static (DateTimeOffset Min, DateTimeOffset Max) CollectTimestampBounds(
        TimeUnit unit, params long[] values)
    {
        string? stats = StatsCollector.Collect(TimestampBatch(unit, "UTC", values));
        Assert.NotNull(stats);

        var root = JsonDocument.Parse(stats).RootElement;
        return (
            DateTimeOffset.Parse(root.GetProperty("minValues").GetProperty("ts").GetString()!,
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal),
            DateTimeOffset.Parse(root.GetProperty("maxValues").GetProperty("ts").GetString()!,
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal));
    }

    private static DateTimeOffset FromNanos(long nanos) =>
        UnixEpoch.AddTicks(nanos / 100);

    [Fact]
    public void Collect_NanosecondTimestamps_MaxRoundsUp()
    {
        // 1_000_000_123ns carries 123ns past the microsecond the ISO form can express. Truncating
        // toward zero yields a max of exactly 1.000000s — BELOW the value in the file.
        var (min, max) = CollectTimestampBounds(TimeUnit.Nanosecond, 1_000_000_123L, 500_000_001L);

        Assert.True(min <= FromNanos(500_000_001L), "min must not exceed the smallest value");
        Assert.True(max >= FromNanos(1_000_000_123L), "max must not fall below the largest value");
    }

    [Fact]
    public void Collect_NegativeNanosecondTimestamps_MinRoundsDown()
    {
        // Pre-1970 values are negative, and integer division truncates TOWARD ZERO — so the error
        // flips to the min side: -10500ns becomes -10us, ABOVE the value in the file.
        var (min, max) = CollectTimestampBounds(TimeUnit.Nanosecond, -1_500L, -10_500L);

        Assert.True(min <= FromNanos(-10_500L), "min must not exceed the smallest value");
        Assert.True(max >= FromNanos(-1_500L), "max must not fall below the largest value");
    }

    [Theory]
    [InlineData(TimeUnit.Second, 1L, 90L)]
    [InlineData(TimeUnit.Millisecond, -5_000L, 1_234L)]
    [InlineData(TimeUnit.Microsecond, -1_500L, 1_000_000_123L)]
    public void Collect_ExactTimestampUnits_BoundsAreTight(TimeUnit unit, long lo, long hi)
    {
        // Second/millisecond/microsecond sources scale exactly, so rounding must not loosen them.
        long perUnitTicks = unit switch
        {
            TimeUnit.Second => TimeSpan.TicksPerSecond,
            TimeUnit.Millisecond => TimeSpan.TicksPerMillisecond,
            _ => 10L,
        };
        var (min, max) = CollectTimestampBounds(unit, hi, lo);

        Assert.Equal(UnixEpoch.AddTicks(lo * perUnitTicks), min);
        Assert.Equal(UnixEpoch.AddTicks(hi * perUnitTicks), max);
    }

    [Fact]
    public void Collect_NanosecondTimestamps_BoundsEncloseEveryValue()
    {
        long[] values =
        [
            -10_500L, -1_500L, -1_000L, 0L, 1L, 999L, 1_000L,
            500_000_001L, 1_000_000_123L, 1_699_999_999_999_999_999L,
        ];

        var (min, max) = CollectTimestampBounds(TimeUnit.Nanosecond, values);

        foreach (long v in values)
        {
            Assert.True(min <= FromNanos(v), $"min exceeds {v}ns");
            Assert.True(max >= FromNanos(v), $"max below {v}ns");
        }
    }

    // ── String stat truncation ────────────────────────────────────────────────────────────────────
    //
    // Delta stats over 32 characters are truncated, and the protocol has no marker recording that.
    // Correctness therefore rests entirely on the truncated values remaining VALID BOUNDS: min at or
    // below every value in the file, max at or above. A bound that violates that skips files holding
    // matching rows — silent data loss, in every engine that reads the table.

    /// <summary>The substitution Utf8JsonWriter emits for a lone surrogate (U+FFFD).</summary>
    private const char ReplacementChar = (char)0xFFFD;

    /// <summary>Unsigned byte-wise UTF-8 comparison — the order Delta string stats are defined in.</summary>
    private static int Utf8Compare(string a, string b)
    {
        byte[] x = System.Text.Encoding.UTF8.GetBytes(a), y = System.Text.Encoding.UTF8.GetBytes(b);
        int n = Math.Min(x.Length, y.Length);
        for (int i = 0; i < n; i++)
            if (x[i] != y[i]) return x[i].CompareTo(y[i]);
        return x.Length.CompareTo(y.Length);
    }

    private static (string? Min, string? Max) CollectStringBounds(params string[] values)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", StringType.Default, true))
            .Build();

        var builder = new StringArray.Builder();
        foreach (string v in values) builder.Append(v);
        var batch = new RecordBatch(schema, [builder.Build()], values.Length);

        string? stats = StatsCollector.Collect(batch);
        Assert.NotNull(stats);

        var root = JsonDocument.Parse(stats).RootElement;
        // A max that cannot be raised to a valid upper bound is omitted rather than written wrong.
        string? min = root.GetProperty("minValues").TryGetProperty("s", out var mn) ? mn.GetString() : null;
        string? max = root.GetProperty("maxValues").TryGetProperty("s", out var mx) ? mx.GetString() : null;
        return (min, max);
    }

    [Fact]
    public void Collect_LongString_TruncatesToBounds()
    {
        string value = new string('a', 40) + "z";
        var (min, max) = CollectStringBounds(value);

        Assert.NotNull(min);
        Assert.NotNull(max);
        Assert.True(min!.Length <= 32);
        Assert.True(max!.Length <= 32);
        Assert.True(Utf8Compare(min, value) <= 0, "min must not exceed the value");
        Assert.True(Utf8Compare(max, value) >= 0, "max must not fall below the value");
    }

    [Fact]
    public void Collect_LongString_MinDoesNotSplitSurrogatePair()
    {
        // The pair straddles the 32-char cut (high half at index 31), so a naive Substring(0, 32)
        // orphans it. Utf8JsonWriter then rewrites the orphan to U+FFFD, which sorts ABOVE the
        // supplementary character it replaced — a min GREATER than the value it must sit below.
        string value = new string('a', 31) + char.ConvertFromUtf32(0x1F600) + new string('b', 20);
        Assert.True(char.IsHighSurrogate(value[31]));

        var (min, _) = CollectStringBounds(value);

        Assert.NotNull(min);
        Assert.DoesNotContain(ReplacementChar, min!);
        Assert.True(Utf8Compare(min!, value) <= 0, "min must not exceed the value");
    }

    [Fact]
    public void Collect_LongString_MaxDoesNotOrphanHighSurrogate()
    {
        // U+103FF encodes as D800 DFFF. With the LOW half at index 31, incrementing it yields
        // U+E000 — outside the surrogate range, so the increment guard alone lets the cut through
        // and strands the high half at index 30. The resulting U+FFFD sorts BELOW the character it
        // replaced, leaving a max SMALLER than a value in the file.
        string value = new string('a', 30) + char.ConvertFromUtf32(0x103FF) + new string('b', 20);
        Assert.True(char.IsHighSurrogate(value[30]));
        Assert.True(char.IsLowSurrogate(value[31]));

        var (_, max) = CollectStringBounds(value);

        Assert.NotNull(max);
        Assert.DoesNotContain(ReplacementChar, max!);
        Assert.True(Utf8Compare(max!, value) >= 0, "max must not fall below the value");
    }

    /// <summary>
    /// The bound invariant itself, over strings built to land a supplementary character on every
    /// offset near the truncation boundary — the family both fixes above are instances of.
    /// </summary>
    [Fact]
    public void Collect_LongStrings_BoundsAlwaysEncloseValues()
    {
        int[] interesting = [0x1F600, 0x103FF, 0x10000, 0x10FFFF, 0xFFFF, 0xFFFD, 0xE000];
        var failures = new List<string>();

        foreach (int cp in interesting)
        {
            for (int offset = 25; offset <= 35; offset++)
            {
                string value = new string('a', offset) + char.ConvertFromUtf32(cp) + new string('b', 20);
                var (min, max) = CollectStringBounds(value);

                if (min is null || Utf8Compare(min, value) > 0)
                    failures.Add($"min U+{cp:X} @{offset}: {(min is null ? "missing" : "exceeds value")}");
                // A null max is legal: an unraisable bound is omitted rather than written wrong.
                if (max is not null && Utf8Compare(max, value) < 0)
                    failures.Add($"max U+{cp:X} @{offset}: below value");
            }
        }

        Assert.Empty(failures);
    }

    [Fact]
    public void Collect_LongStrings_BoundsEncloseEveryValueInTheFile()
    {
        string[] values =
        [
            new string('a', 40),
            new string('a', 31) + char.ConvertFromUtf32(0x1F600) + new string('b', 20),
            new string('a', 30) + char.ConvertFromUtf32(0x103FF) + new string('b', 20),
            new string('m', 35),
            "short",
        ];

        var (min, max) = CollectStringBounds(values);
        Assert.NotNull(min);

        foreach (string v in values)
        {
            Assert.True(Utf8Compare(min!, v) <= 0, $"min exceeds {v.Length}-char value");
            if (max is not null)
                Assert.True(Utf8Compare(max, v) >= 0, $"max below {v.Length}-char value");
        }
    }

    // â”€â”€ NaN and the infinities (#214) â”€â”€
    //
    // Expectations here are Spark 4.0's own output, read out of the commit JSON of a Delta table
    // it wrote: a double column holding [3.0, NaN] commits
    //   {"minValues":{"g":3.0},"maxValues":{"g":"NaN"}}
    // an all-NaN column commits "NaN" at BOTH ends, and the infinities come back as the quoted
    // strings "Infinity" / "-Infinity".

    private static JsonElement Bounds(IArrowArray column, string which)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("g", column.Data.DataType, true))
            .Build();
        string? stats = StatsCollector.Collect(new RecordBatch(schema, [column], column.Length));
        Assert.NotNull(stats);
        return JsonDocument.Parse(stats!).RootElement.GetProperty(which);
    }

    [Fact]
    public void Collect_DoubleWithNaN_RecordsItAsTheMaximum()
    {
        var values = new DoubleArray.Builder().Append(3.0).Append(double.NaN).Build();

        // .NET's CompareTo puts NaN at the BOTTOM, which made this file's bounds min = NaN,
        // max = 3.0 -- inverted under the order the reader uses, and min > max is a shape nothing
        // downstream is prepared for.
        Assert.Equal(3.0, Bounds(values, "minValues").GetProperty("g").GetDouble(), 5);
        Assert.Equal("NaN", Bounds(values, "maxValues").GetProperty("g").GetString());
    }

    [Fact]
    public void Collect_AllNaN_IsNaNAtBothEnds()
    {
        var values = new DoubleArray.Builder().Append(double.NaN).Append(double.NaN).Build();

        Assert.Equal("NaN", Bounds(values, "minValues").GetProperty("g").GetString());
        Assert.Equal("NaN", Bounds(values, "maxValues").GetProperty("g").GetString());
    }

    [Fact]
    public void Collect_Infinities_AreWrittenAsStrings()
    {
        // Not merely a wrong bound before this: Utf8JsonWriter THROWS ArgumentException on all
        // three non-finite doubles, so collecting stats for this column failed outright and took
        // the whole Delta write down with it.
        var values = new DoubleArray.Builder()
            .Append(double.NegativeInfinity).Append(1.0).Append(double.PositiveInfinity).Build();

        Assert.Equal("-Infinity", Bounds(values, "minValues").GetProperty("g").GetString());
        Assert.Equal("Infinity", Bounds(values, "maxValues").GetProperty("g").GetString());
    }

    [Fact]
    public void Collect_FloatColumn_FollowsTheSameOrder()
    {
        var values = new FloatArray.Builder().Append(float.NaN).Append(1.5f).Build();

        Assert.Equal(1.5, Bounds(values, "minValues").GetProperty("g").GetDouble(), 5);
        Assert.Equal("NaN", Bounds(values, "maxValues").GetProperty("g").GetString());
    }

    [Fact]
    public void Collect_NaNInOneBatchOfSeveral_StillRisesToTheMaximum()
    {
        // The merge across batches compares against the bound already recorded, and that compare
        // has to obey the same order -- otherwise a NaN found in batch 2 loses to batch 1's finite
        // maximum and vanishes from the stats.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("g", DoubleType.Default, true))
            .Build();
        var first = new RecordBatch(schema,
            [new DoubleArray.Builder().Append(1.0).Append(9.0).Build()], 2);
        var second = new RecordBatch(schema,
            [new DoubleArray.Builder().Append(double.NaN).Build()], 1);

        string? stats = StatsCollector.Collect([first, second]);
        var root = JsonDocument.Parse(stats!).RootElement;

        Assert.Equal(1.0, root.GetProperty("minValues").GetProperty("g").GetDouble(), 5);
        Assert.Equal("NaN", root.GetProperty("maxValues").GetProperty("g").GetString());
    }
}

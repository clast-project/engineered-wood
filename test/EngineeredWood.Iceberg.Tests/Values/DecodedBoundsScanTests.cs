// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using EngineeredWood.Expressions;
using EngineeredWood.Iceberg.Expressions;
using EngineeredWood.Iceberg.Manifest;
using EngineeredWood.Iceberg.Values;
using EngineeredWood.IO.Local;
using static EngineeredWood.Iceberg.Tests.Values.JavaSingleValueVectorTests;
using Ex = EngineeredWood.Iceberg.Expressions.Expressions;

namespace EngineeredWood.Iceberg.Tests.Values;

/// <summary>
/// Bounds as a manifest carries them, bytes per field id, decoded with
/// <see cref="SingleValueBinary.TryDecodeBound"/> and handed to <see cref="TableScan"/>, which prunes with the
/// shared <see cref="StatisticsEvaluator"/>. This is the path the manifest codec (#586) feeds.
/// </summary>
public class DecodedBoundsScanTests
{
    private static readonly DateTimeOffset Epoch = new(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

    // The current schema. "id" was an int when the old file was written, "ts" a date; both promoted since.
    private static readonly Schema CurrentSchema = new(1, [
        new NestedField(1, "id", IcebergType.Long, true),
        new NestedField(2, "ts", IcebergType.TimestampNs, false),
        new NestedField(3, "price", IcebergType.Decimal(20, 2), false),
        new NestedField(4, "name", IcebergType.String, false),
        new NestedField(5, "score", IcebergType.Double, false),
    ]);

    // 2017-11-16 and 2017-11-17, as date days.
    private const int Nov16 = 17_486;

    /// <summary>Written before the promotions: 4-byte int, date and float bounds, a decimal(9, 2) price.</summary>
    private static readonly DataFile OldFile = File("old.parquet",
        lower: new() { [1] = I32(1), [2] = I32(Nov16), [3] = "058c" /* 14.20 */, [4] = "616263", [5] = "0000803f" /* 1.0f */ },
        upper: new() { [1] = I32(100), [2] = I32(Nov16), [3] = "7fff" /* 327.67 */, [4] = "6d6e6f", [5] = "00002040" /* 2.5f */ });

    /// <summary>Written after: 8-byte long and nanosecond bounds, a wider decimal.</summary>
    private static readonly DataFile NewFile = File("new.parquet",
        lower: new() { [1] = I64(101), [2] = I64((Nov16 + 1) * 86_400_000_000_000L + 1), [3] = "0186a0" /* 1000.00 */, [4] = "70", [5] = "0000000000001040" },
        upper: new() { [1] = I64(200), [2] = I64((Nov16 + 1) * 86_400_000_000_000L + 999), [3] = "0f4240" /* 10000.00 */, [4] = "7a", [5] = "0000000000002440" });

    /// <summary>NaN bounds, a variant-only blob and a non-UTF-8 string: all declined, so never pruned on.</summary>
    private static readonly DataFile OddFile = File("odd.parquet",
        lower: new() { [1] = I64(1000), [4] = "ff", [5] = "000000000000f87f" },
        upper: new() { [1] = I64(2000), [4] = "ff", [5] = "000000000000f87f" });

    private static readonly DataFile[] Files = [OldFile, NewFile, OddFile];

    private static ScanResult Plan(Predicate filter)
    {
        var metadata = TableMetadata.Create(CurrentSchema, location: "memory://table");
        return new TableScan(metadata, new LocalTableFileSystem(Path.GetTempPath())).Filter(filter).PlanFiles(Files);
    }

    private static string[] Paths(ScanResult r) => r.DataFiles.Select(f => f.FilePath).ToArray();

    /// <summary>
    /// A truncated upper bound is stored already incremented: Iceberg Java 1.12 truncates <c>[00 FF]</c> to one byte
    /// as lower <c>00</c>, upper <c>01</c> (and <c>abcdef</c> to three as <c>abc</c> / <c>abd</c>), and omits the
    /// upper bound when no successor exists. So the bytes decode as a sound bound as they stand, and must not be
    /// incremented again. Measured with <c>BinaryUtil.truncateBinaryMax</c> / <c>UnicodeUtil.truncateStringMax</c>
    /// (review of #971).
    /// </summary>
    [Fact]
    public void TruncatedUpperBounds_AsJavaWritesThem_DoNotPruneAMatch()
    {
        var schema = new Schema(0, [
            new NestedField(1, "fx", IcebergType.Fixed(2), false),
            new NestedField(2, "s", IcebergType.String, false),
        ]);
        var file = new DataFile
        {
            FilePath = "truncated.parquet",
            RecordCount = 1,
            FileSizeInBytes = 100,
            NullValueCounts = new Dictionary<int, long> { [1] = 0, [2] = 0 },
            ColumnLowerBounds = DecodeAll(schema, new() { [1] = "00", [2] = "616263" }), // 00, "abc"
            ColumnUpperBounds = DecodeAll(schema, new() { [1] = "01", [2] = "616264" }), // 01, "abd"
        };

        ScanResult Plan(Predicate p) => new TableScan(TableMetadata.Create(schema, location: "memory://table"),
            new LocalTableFileSystem(Path.GetTempPath())).Filter(p).PlanFiles([file]);

        // The file holds fx = [00 FF] and s = "abcdef".
        Assert.Single(Plan(Cmp("fx", ComparisonOperator.GreaterThan, LiteralValue.Of(new byte[] { 0x00 }))).DataFiles);
        Assert.Single(Plan(Cmp("fx", ComparisonOperator.Equal, LiteralValue.Of(new byte[] { 0x00, 0xFF }))).DataFiles);
        Assert.Single(Plan(Cmp("s", ComparisonOperator.GreaterThan, LiteralValue.Of("abc"))).DataFiles);
        Assert.Single(Plan(Cmp("s", ComparisonOperator.Equal, LiteralValue.Of("abcdef"))).DataFiles);

        // And the bounds still prune what is truly outside them.
        Assert.Empty(Plan(Cmp("fx", ComparisonOperator.GreaterThan, LiteralValue.Of(new byte[] { 0x01 }))).DataFiles);
        Assert.Empty(Plan(Cmp("s", ComparisonOperator.Equal, LiteralValue.Of("abz"))).DataFiles);
    }

    private static Dictionary<int, LiteralValue> DecodeAll(Schema schema, Dictionary<int, string> raw)
    {
        var bounds = new Dictionary<int, LiteralValue>();
        foreach (var (id, hex) in raw)
        {
            Assert.True(SingleValueBinary.TryDecodeBound(schema.Fields.Single(f => f.Id == id).Type, FromHex(hex), out var b));
            bounds[id] = b;
        }

        return bounds;
    }

    [Fact]
    public void PromotedIntBounds_PruneALongPredicate()
    {
        Assert.Equal(["new.parquet", "odd.parquet"], Paths(Plan(Ex.GreaterThan("id", 150L))));
        Assert.Equal(["old.parquet"], Paths(Plan(Ex.LessThan("id", 50L))));
    }

    [Fact]
    public void PromotedDateBounds_PruneANanosecondPredicate()
    {
        // The old file's ts is all of 2017-11-16 at midnight; the new file's sits 1-999 ns after the next midnight.
        long nextMidnight = (Nov16 + 1) * 86_400_000_000_000L;
        Assert.Equal(["new.parquet", "odd.parquet"], Paths(Plan(Cmp("ts", ComparisonOperator.GreaterThan, LiteralValue.TimestampNanosOf(nextMidnight)))));
        Assert.Equal(["old.parquet", "odd.parquet"], Paths(Plan(Cmp("ts", ComparisonOperator.LessThanOrEqual, LiteralValue.TimestampNanosOf(nextMidnight)))));

        // A microsecond literal against nanosecond bounds: exact, so 1 ns past midnight is still after it.
        Assert.Equal(["old.parquet", "odd.parquet"], Paths(Plan(Cmp("ts", ComparisonOperator.LessThanOrEqual, LiteralValue.Of(Epoch.AddDays(Nov16 + 1))))));
    }

    [Fact]
    public void DecimalBounds_OfAnyWidth_Prune()
    {
        Assert.Equal(["new.parquet", "odd.parquet"], Paths(Plan(Cmp("price", ComparisonOperator.GreaterThan, LiteralValue.HighPrecisionDecimalOf(50000, 2)))));
        Assert.Equal(["old.parquet", "odd.parquet"], Paths(Plan(Cmp("price", ComparisonOperator.Equal, LiteralValue.Of(14.20m)))));
    }

    [Fact]
    public void StringAndFloatBounds_Prune_AndDeclinedBoundsNeverDo()
    {
        Assert.Equal(["new.parquet", "odd.parquet"], Paths(Plan(Ex.Equal("name", "q"))));
        // The odd file's NaN score bounds are declined: unknown, so it stays.
        Assert.Equal(["old.parquet", "odd.parquet"], Paths(Plan(Ex.LessThan("score", 3.0))));
        // The float bound 2.5f widened exactly; 2.5 is inside it.
        Assert.Contains("old.parquet", Paths(Plan(Ex.Equal("score", 2.5))));
    }

    private static Predicate Cmp(string column, ComparisonOperator op, LiteralValue value) =>
        new ComparisonPredicate(new UnboundReference(column), op, new LiteralExpression(value));

    // ── Building files as a manifest would ──

    private static DataFile File(string path, Dictionary<int, string> lower, Dictionary<int, string> upper) => new()
    {
        FilePath = path,
        RecordCount = 10,
        FileSizeInBytes = 1000,
        NullValueCounts = new Dictionary<int, long> { [1] = 0, [2] = 0, [3] = 0, [4] = 0, [5] = 0 },
        ColumnLowerBounds = Decode(lower),
        ColumnUpperBounds = Decode(upper),
    };

    private static Dictionary<int, LiteralValue> Decode(Dictionary<int, string> raw)
    {
        var bounds = new Dictionary<int, LiteralValue>();
        foreach (var (id, hex) in raw)
        {
            var type = CurrentSchema.Fields.Single(f => f.Id == id).Type;
            if (SingleValueBinary.TryDecodeBound(type, FromHex(hex), out var bound))
                bounds[id] = bound;
        }

        return bounds;
    }

    private static string I32(int v) => ToHex(BitConverter.GetBytes(v));

    private static string I64(long v) => ToHex(BitConverter.GetBytes(v));
}

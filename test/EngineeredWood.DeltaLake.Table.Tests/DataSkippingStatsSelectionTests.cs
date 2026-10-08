// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using ArrowListType = Apache.Arrow.Types.ListType;
using ArrowMapType = Apache.Arrow.Types.MapType;
using ArrowStructType = Apache.Arrow.Types.StructType;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #471 part 2: which columns get file statistics follows the table's <c>delta.dataSkippingNumIndexedCols</c> and
/// <c>delta.dataSkippingStatsColumns</c>, and how long a string bound may be follows
/// <c>delta.dataSkippingStringPrefixLength</c>. Before, every column got statistics and strings were cut at 32. The
/// expected column sets are the ones delta-spark 4.4.0 wrote for the same schema and properties.
/// </summary>
public class DataSkippingStatsSelectionTests : IDisposable
{
    private const string NumIndexedCols = "delta.dataSkippingNumIndexedCols";
    private const string StatsColumns = "delta.dataSkippingStatsColumns";
    private const string PrefixLength = "delta.dataSkippingStringPrefixLength";
    private readonly string _tempDir;

    public DataSkippingStatsSelectionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_dssel_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    // Spark's measured schema: a INT, p STRING (partition), b BOOLEAN, s STRUCT<x INT, y STRING, z STRUCT<q INT>>,
    // c BINARY, arr ARRAY<INT>, m MAP<STRING,INT>, d DATE.
    private static readonly ArrowStructType ZType = new([new Field("q", Int32Type.Default, true)]);
    private static readonly ArrowStructType SType = new(
    [
        new Field("x", Int32Type.Default, true),
        new Field("y", StringType.Default, true),
        new Field("z", ZType, true),
    ]);
    private static readonly ArrowListType ArrType = new(new Field("element", Int32Type.Default, true));
    private static readonly ArrowMapType MType =
        new(new Field("key", StringType.Default, false), new Field("value", Int32Type.Default, true));

    private static Apache.Arrow.Schema WideSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("a", Int32Type.Default, true))
        .Field(new Field("p", StringType.Default, true))
        .Field(new Field("b", BooleanType.Default, true))
        .Field(new Field("s", SType, true))
        .Field(new Field("c", BinaryType.Default, true))
        .Field(new Field("arr", ArrType, true))
        .Field(new Field("m", MType, true))
        .Field(new Field("d", Date32Type.Default, true))
        .Build();

    private static RecordBatch WideRow(Apache.Arrow.Schema schema)
    {
        var z = new StructArray(ZType, 1, [new Int32Array.Builder().Append(4).Build()], ArrowBuffer.Empty, 0);
        var s = new StructArray(SType, 1,
            [new Int32Array.Builder().Append(2).Build(), new StringArray.Builder().Append("yy").Build(), z],
            ArrowBuffer.Empty, 0);
        var arr = new ListArray.Builder(Int32Type.Default);
        arr.Append();
        ((Int32Array.Builder)arr.ValueBuilder).Append(5);
        var m = new MapArray.Builder(MType);
        m.Append();
        ((StringArray.Builder)m.KeyBuilder).Append("k");
        ((Int32Array.Builder)m.ValueBuilder).Append(6);
        return new RecordBatch(schema,
        [
            new Int32Array.Builder().Append(1).Build(),
            new StringArray.Builder().Append("part").Build(),
            new BooleanArray.Builder().Append(true).Build(),
            s,
            new BinaryArray.Builder().Append([1, 2]).Build(),
            arr.Build(),
            m.Build(),
            new Date32Array.Builder().Append(new DateTime(2026, 1, 2)).Build(),
        ], 1);
    }

    private async Task<DeltaTable> CreateWideAsync(
        Dictionary<string, string> configuration, ColumnMappingMode mode = ColumnMappingMode.None) =>
        await DeltaTable.CreateAsync(Fs, WideSchema, partitionColumns: ["p"], columnMappingMode: mode,
            configuration: configuration);

    private static JsonElement StatsOf(DeltaTable table) =>
        JsonDocument.Parse(table.CurrentSnapshot.ActiveFiles.Values.Single().Stats!).RootElement;

    // The dotted leaf paths a stats object holds, in the order written.
    private static List<string> Leaves(JsonElement stats, string section)
    {
        var result = new List<string>();
        if (stats.TryGetProperty(section, out var obj))
        {
            Walk(obj, null, result);
        }
        return result;
    }

    private static void Walk(JsonElement obj, string? prefix, List<string> into)
    {
        foreach (var p in obj.EnumerateObject())
        {
            string path = prefix is null ? p.Name : prefix + "." + p.Name;
            if (p.Value.ValueKind == JsonValueKind.Object)
            {
                Walk(p.Value, path, into);
            }
            else
            {
                into.Add(path);
            }
        }
    }

    // Spark's measured table for N = 0..9 and -1: depth-first leaves in schema order, the partition column taking no
    // slot, every non-struct leaf one slot whatever its type, and the struct cut part-way at N = 3 and 4.
    [Theory]
    [InlineData("0", "")]
    [InlineData("1", "a")]
    [InlineData("2", "a,b")]
    [InlineData("3", "a,b,s.x")]
    [InlineData("4", "a,b,s.x,s.y")]
    [InlineData("5", "a,b,s.x,s.y,s.z.q")]
    [InlineData("6", "a,b,s.x,s.y,s.z.q,c")]
    [InlineData("7", "a,b,s.x,s.y,s.z.q,c,arr")]
    [InlineData("8", "a,b,s.x,s.y,s.z.q,c,arr,m")]
    [InlineData("9", "a,b,s.x,s.y,s.z.q,c,arr,m,d")]
    [InlineData("-1", "a,b,s.x,s.y,s.z.q,c,arr,m,d")]
    [InlineData("+3", "a,b,s.x")]
    public async Task NumIndexedCols_TakesTheFirstNLeaves(string n, string expected)
    {
        await using var table = await CreateWideAsync(new() { [NumIndexedCols] = n });
        await table.WriteAsync([WideRow(table.ArrowSchema)]);

        var stats = StatsOf(table);

        Assert.Equal(1, stats.GetProperty("numRecords").GetInt64());
        Assert.Equal(expected, string.Join(",", Leaves(stats, "nullCount")));
        Assert.DoesNotContain(Leaves(stats, "minValues"), l => !expected.Split(',').Contains(l));
    }

    [Fact]
    public async Task ByDefault_OnlyTheFirst32LeavesGetStatistics()
    {
        var builder = new Apache.Arrow.Schema.Builder();
        for (int i = 0; i < 40; i++)
        {
            builder.Field(new Field($"c{i}", Int64Type.Default, true));
        }
        var schema = builder.Build();
        await using var table = await DeltaTable.CreateAsync(Fs, schema);
        await table.WriteAsync([new RecordBatch(schema,
            Enumerable.Range(0, 40).Select(i => (IArrowArray)new Int64Array.Builder().Append(i).Build()), 1)]);

        var stats = StatsOf(table);

        Assert.Equal(Enumerable.Range(0, 32).Select(i => $"c{i}"), Leaves(stats, "nullCount"));
        Assert.Equal(Enumerable.Range(0, 32).Select(i => $"c{i}"), Leaves(stats, "minValues"));
    }

    [Theory]
    [InlineData("s.y,a", "a,s.y")] // written in schema order, not the list's
    [InlineData("s", "s.x,s.y,s.z.q")]
    [InlineData("s.z", "s.z.q")]
    [InlineData("A,S.Y", "a,s.y")]
    [InlineData("d", "d")]
    [InlineData("", "")]
    [InlineData("arr.element", "")] // accepted at CREATE, but statistics never reach through an array
    public async Task StatsColumns_SelectsWhatItNames(string listed, string expected)
    {
        // NumIndexedCols = 1 as well: the list wins.
        await using var table = await CreateWideAsync(new() { [StatsColumns] = listed, [NumIndexedCols] = "1" });
        await table.WriteAsync([WideRow(table.ArrowSchema)]);

        Assert.Equal(expected, string.Join(",", Leaves(StatsOf(table), "nullCount")));
    }

    [Fact]
    public async Task UnderColumnMapping_SelectsLogicalNames_AndKeysThePhysicalOnes()
    {
        await using var table = await CreateWideAsync(
            new() { [StatsColumns] = "s.y,d" }, ColumnMappingMode.Name);
        await table.WriteAsync([WideRow(table.ArrowSchema)]);

        var schema = table.CurrentSnapshot.Schema;
        string Physical(StructField f) => ColumnMapping.GetPhysicalName(f, ColumnMappingMode.Name);
        var d = schema.Fields.Single(f => f.Name == "d");
        var s = schema.Fields.Single(f => f.Name == "s");
        var y = ((Schema.StructType)s.Type).Fields.Single(f => f.Name == "y");

        Assert.Equal([$"{Physical(s)}.{Physical(y)}", Physical(d)], Leaves(StatsOf(table), "nullCount"));
    }

    [Fact]
    public async Task StringPrefixLength_SetsHowLongAStringBoundMayBe()
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("t", StringType.Default, true)).Build();
        await using var table = await DeltaTable.CreateAsync(Fs, schema,
            configuration: new Dictionary<string, string> { [PrefixLength] = "3" });
        await table.WriteAsync([new RecordBatch(schema, [new StringArray.Builder().Append("zyxwvu").Build()], 1)]);

        var stats = StatsOf(table);

        Assert.Equal("zyx", stats.GetProperty("minValues").GetProperty("t").GetString());
        // EW's upper bound bumps the last kept character; Spark appends U+007F instead ("zyx\u007F"). Both bound it.
        Assert.Equal("zyy", stats.GetProperty("maxValues").GetProperty("t").GetString());
    }

    [Fact]
    public async Task StringPrefixLength_OfZero_WritesAnEmptyMinAndNoMax()
    {
        // As Spark does.
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("t", StringType.Default, true)).Build();
        await using var table = await DeltaTable.CreateAsync(Fs, schema,
            configuration: new Dictionary<string, string> { [PrefixLength] = "0" });
        await table.WriteAsync([new RecordBatch(schema, [new StringArray.Builder().Append("abc").Build()], 1)]);

        var stats = StatsOf(table);

        Assert.Equal("", stats.GetProperty("minValues").GetProperty("t").GetString());
        Assert.False(stats.GetProperty("maxValues").TryGetProperty("t", out _));
        Assert.Equal(0, stats.GetProperty("nullCount").GetProperty("t").GetInt64());
    }

    // The rewrite paths collect with the same selection as an append.
    [Fact]
    public async Task CompactAndUpdate_HonourTheSelection()
    {
        await using var table = await CreateWideAsync(new() { [NumIndexedCols] = "1" });
        await table.WriteAsync([WideRow(table.ArrowSchema)]);
        await table.WriteAsync([WideRow(table.ArrowSchema)]);

        Assert.NotNull(await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue }));
        Assert.Equal("a", string.Join(",", Leaves(StatsOf(table), "nullCount")));

        await table.UpdateAsync(
            batch => new BooleanArray.Builder().AppendRange(Enumerable.Repeat(true, batch.Length)).Build(), b => b);
        Assert.Equal("a", string.Join(",", Leaves(StatsOf(table), "nullCount")));
    }

    [Theory]
    [InlineData(NumIndexedCols, "-2")]
    [InlineData(NumIndexedCols, "abc")]
    [InlineData(NumIndexedCols, "1.5")]
    [InlineData(NumIndexedCols, " 3")]
    [InlineData(NumIndexedCols, "")]
    [InlineData(NumIndexedCols, "2147483648")]
    [InlineData(PrefixLength, "-1")]
    [InlineData(PrefixLength, "x")]
    public async Task Create_RefusesWhatSparkRefuses(string key, string value)
    {
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await CreateWideAsync(new() { [key] = value }));

        Assert.Equal(DeltaTableErrorCodes.InvalidDataSkippingProperty, ex.ErrorCode);
    }

    [Theory]
    [InlineData(NumIndexedCols, "-1")]
    [InlineData(NumIndexedCols, "+3")]
    [InlineData(NumIndexedCols, "100000")]
    [InlineData(PrefixLength, "0")]
    public async Task Create_AcceptsWhatSparkAccepts(string key, string value)
    {
        await using var table = await CreateWideAsync(new() { [key] = value });

        Assert.Equal(value, table.CurrentSnapshot.Metadata.Configuration![key]);
    }
}

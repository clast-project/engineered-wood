// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Schema;
using StructType = Apache.Arrow.Types.StructType;
using EngineeredWood.DeltaLake.Table.TypeWidening;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A <c>byte</c>/<c>short</c> column widened to <c>int</c>, <c>long</c> or <c>double</c> reads from its old files
/// as the INT32 they store, not narrowed and widened back (#839). The values must come out exactly as the
/// narrow-then-widen route gave them, through every read that widens: a scan, a filtered scan, compaction and
/// the change feed.
/// </summary>
public class StoredInt32WideningTests : IDisposable
{
    private readonly string _tempDir;

    public StoredInt32WideningTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_stored_int32_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private static readonly Dictionary<string, string> Widening = new() { [DeltaLake.Schema.TypeWidening.EnableKey] = "true" };

    private static Apache.Arrow.Schema Schema(params Field[] fields)
    {
        var b = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            b.Field(f);
        return b.Build();
    }

    private static StructType Inner(IArrowType c) => new([new Field("c", c, true)]);

    // The old file: id, b byte, s short, d byte, h short (stays short), st struct<c byte>, xs array<short>.
    private static Apache.Arrow.Schema OldSchema => Schema(
        new Field("id", Int64Type.Default, false),
        new Field("b", Int8Type.Default, true),
        new Field("s", Int16Type.Default, true),
        new Field("d", Int8Type.Default, true),
        new Field("h", Int8Type.Default, true),
        new Field("st", Inner(Int8Type.Default), true),
        new Field("xs", new ListType(new Field("element", Int16Type.Default, true)), true));

    // The widening append: b -> int, s -> long, d -> long, h -> short, st.c -> int, xs element -> int. (An append never
    // widens an integer to double, so that target has its own test.)
    private static Apache.Arrow.Schema NewSchema => Schema(
        new Field("id", Int64Type.Default, false),
        new Field("b", Int32Type.Default, true),
        new Field("s", Int64Type.Default, true),
        new Field("d", Int64Type.Default, true),
        new Field("h", Int16Type.Default, true),
        new Field("st", Inner(Int32Type.Default), true),
        new Field("xs", new ListType(new Field("element", Int32Type.Default, true)), true));

    private static RecordBatch OldBatch()
    {
        var ids = new Int64Array.Builder().AppendRange([1, 2, 3]).Build();
        var b = new Int8Array.Builder().Append(sbyte.MinValue).Append(sbyte.MaxValue).AppendNull().Build();
        var s = new Int16Array.Builder().Append(short.MinValue).AppendNull().Append(short.MaxValue).Build();
        var d = new Int8Array.Builder().Append(-5).Append(0).Append(42).Build();
        var h = new Int8Array.Builder().Append(-1).Append(1).AppendNull().Build();
        var c = new Int8Array.Builder().Append(7).AppendNull().Append(-7).Build();
        var st = new StructArray(Inner(Int8Type.Default), 3, [c], ArrowBuffer.Empty, 0);
        var elements = new Int16Array.Builder().Append(300).Append(-300).Append(1).Build();
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(2).Append(2).Append(3).Build();
        var xs = new ListArray(new ListType(new Field("element", Int16Type.Default, true)), 3, offsets, elements,
            ArrowBuffer.Empty, 0);
        return new RecordBatch(OldSchema, [ids, b, s, d, h, st, xs], 3);
    }

    private static RecordBatch NewBatch()
    {
        var ids = new Int64Array.Builder().Append(4).Build();
        var b = new Int32Array.Builder().Append(100_000).Build();
        var s = new Int64Array.Builder().Append(5_000_000_000L).Build();
        var d = new Int64Array.Builder().Append(6_000_000_000L).Build();
        var h = new Int16Array.Builder().Append(1_000).Build();
        var c = new Int32Array.Builder().Append(70_000).Build();
        var st = new StructArray(Inner(Int32Type.Default), 1, [c], ArrowBuffer.Empty, 0);
        var elements = new Int32Array.Builder().Append(80_000).Build();
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(1).Build();
        var xs = new ListArray(new ListType(new Field("element", Int32Type.Default, true)), 1, offsets, elements,
            ArrowBuffer.Empty, 0);
        return new RecordBatch(NewSchema, [ids, b, s, d, h, st, xs], 1);
    }

    private async Task<DeltaTable> CreateWidenedTableAsync(IReadOnlyDictionary<string, string>? extra = null)
    {
        var config = new Dictionary<string, string>(Widening);
        if (extra is not null)
        {
            foreach (var kv in extra)
                config[kv.Key] = kv.Value;
        }
        var table = await DeltaTable.CreateAsync(Fs, OldSchema, configuration: config);
        await table.WriteAsync([OldBatch()]);
        await table.WriteAsync([NewBatch()], mergeSchema: true);
        return table;
    }

    private static readonly (long Id, int? B, long? S, long? D, short? H, int? C, int[]? Xs)[] Expected =
    [
        (1, sbyte.MinValue, short.MinValue, -5, -1, 7, [300, -300]),
        (2, sbyte.MaxValue, null, 0, 1, null, []),
        (3, null, short.MaxValue, 42, null, -7, [1]),
        (4, 100_000, 5_000_000_000L, 6_000_000_000L, 1_000, 70_000, [80_000]),
    ];

    private static List<(long Id, int? B, long? S, long? D, short? H, int? C, int[]? Xs)> Rows(
        IEnumerable<RecordBatch> batches)
    {
        var rows = new List<(long, int?, long?, long?, short?, int?, int[]?)>();
        foreach (var batch in batches)
        {
            IArrowArray Col(string name) => batch.Column(batch.Schema.GetFieldIndex(name));
            var id = Assert.IsType<Int64Array>(Col("id"));
            var b = Assert.IsType<Int32Array>(Col("b"));
            var s = Assert.IsType<Int64Array>(Col("s"));
            var d = Assert.IsType<Int64Array>(Col("d"));
            var h = Assert.IsType<Int16Array>(Col("h"));
            var st = Assert.IsType<StructArray>(Col("st"));
            var c = Assert.IsType<Int32Array>(st.Fields[0]);
            var xs = Assert.IsType<ListArray>(Col("xs"));
            var xsValues = Assert.IsType<Int32Array>(xs.Values);
            for (int i = 0; i < batch.Length; i++)
            {
                int[]? list = xs.IsNull(i)
                    ? null
                    : Enumerable.Range(xs.ValueOffsets[i], xs.GetValueLength(i))
                        .Select(j => xsValues.GetValue(j)!.Value).ToArray();
                rows.Add((id.GetValue(i)!.Value, b.GetValue(i), s.GetValue(i), d.GetValue(i), h.GetValue(i),
                    st.IsNull(i) ? null : c.GetValue(i), list));
            }
        }
        rows.Sort((x, y) => x.Item1.CompareTo(y.Item1));
        return rows;
    }

    private static void AssertExpected(
        List<(long Id, int? B, long? S, long? D, short? H, int? C, int[]? Xs)> rows, params long[] ids)
    {
        var expected = Expected.Where(e => ids.Length == 0 || ids.Contains(e.Id)).ToList();
        Assert.Equal(expected.Count, rows.Count);
        for (int i = 0; i < rows.Count; i++)
        {
            var (e, a) = (expected[i], rows[i]);
            Assert.Equal((e.Id, e.B, e.S, e.D, e.H, e.C), (a.Id, a.B, a.S, a.D, a.H, a.C));
            Assert.Equal(e.Xs, a.Xs);
        }
    }

    private static async Task<List<RecordBatch>> ReadAsync(DeltaTable table, DeltaReadOptions? options = null)
    {
        var batches = new List<RecordBatch>();
        await foreach (var batch in options is null ? table.ReadAllAsync() : table.ReadAsync(options))
            batches.Add(batch);
        return batches;
    }

    /// <summary>The leaves of the table's OLD file (the one with a narrow <c>b</c>) the read relabels.</summary>
    private async Task<List<string>> RelabeledLeavesOfOldFileAsync(DeltaTable table)
    {
        var snapshot = table.CurrentSnapshot;
        var mode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        foreach (var add in snapshot.ActiveFiles.Values)
        {
            using var reader = new ParquetFileReader(
                new LocalRandomAccessFile(Path.Combine(_tempDir, DeltaLake.DeltaPath.Decode(add.Path))));
            var schema = await reader.GetSchemaAsync();
            if (!schema.Columns.Any(c => c.SchemaElement.LogicalType is LogicalType.IntType { BitWidth: 8 }))
                continue;
            var leaves = StoredInt32Columns.Resolve(schema, snapshot.Schema, mode);
            return schema.Columns.Where(c => leaves?.Contains(c.SchemaElement) == true)
                .Select(c => c.DottedPath).OrderBy(p => p, StringComparer.Ordinal).ToList();
        }
        throw new InvalidOperationException("no file with a narrow column");
    }

    [Fact]
    public async Task Scan_ReadsWidenedNarrowColumns_WithTheirValues()
    {
        await using var table = await CreateWidenedTableAsync();

        AssertExpected(Rows(await ReadAsync(table)));
    }

    [Fact]
    public async Task Resolve_PicksTheIntAndLongTargets_NotAShortOne()
    {
        await using var table = await CreateWidenedTableAsync();

        // h stays a short: reading it as INT32 would only need narrowing again.
        Assert.Equal(["b", "d", "s", "st.c", "xs.list.element"], await RelabeledLeavesOfOldFileAsync(table));
    }

    [Theory]
    [InlineData("name")]
    [InlineData("id")]
    public async Task ColumnMapping_ResolvesByPhysicalNameOrId(string mode)
    {
        await using var table = await CreateWidenedTableAsync(
            new Dictionary<string, string> { [ColumnMapping.ModeKey] = mode });

        // The file's columns carry physical names, so a match by logical name would find none of them.
        Assert.Equal(5, (await RelabeledLeavesOfOldFileAsync(table)).Count); // b, s, d, st.c, xs element
        AssertExpected(Rows(await ReadAsync(table)));
    }

    [Fact]
    public async Task FilteredScan_OnTheWidenedColumn()
    {
        await using var table = await CreateWidenedTableAsync();

        var rows = Rows(await ReadAsync(table, new DeltaReadOptions
        {
            Filter = Ex.GreaterThan("b", Expressions.LiteralValue.Of(0)),
            FilterRows = true,
        }));

        AssertExpected(rows, 2, 4);
    }

    [Fact]
    public async Task Compaction_RewritesTheWidenedColumns()
    {
        await using var table = await CreateWidenedTableAsync();

        Assert.NotNull(await table.CompactAsync(new CompactionOptions { TargetFileSize = 1 << 20, MinFileSize = 1 << 20 }));

        Assert.Single(table.CurrentSnapshot.ActiveFiles);
        AssertExpected(Rows(await ReadAsync(table)));
    }

    [Fact]
    public async Task ChangeFeed_ReadsTheOldFileWidened()
    {
        await using var table = await CreateWidenedTableAsync(
            new Dictionary<string, string> { [CdfConfig.EnableKey] = "true" });

        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadChangesAsync(
                           new DeltaChangeReadOptions { StartVersion = 0, EndVersion = table.CurrentSnapshot.Version }))
            batches.Add(b);

        AssertExpected(Rows(batches));
    }

    /// <summary>
    /// byte -> double, which no append makes: the column is retyped by a schema replacement, and the old file's
    /// INT(8) values read as stored and widen once.
    /// </summary>
    [Fact]
    public async Task DoubleTarget_ReadsTheStoredValues()
    {
        var oldSchema = Schema(new Field("id", Int64Type.Default, false), new Field("d", Int8Type.Default, true));
        await using var table = await DeltaTable.CreateAsync(Fs, oldSchema, configuration: Widening);
        await table.WriteAsync([new RecordBatch(oldSchema,
            [new Int64Array.Builder().AppendRange([1, 2, 3]).Build(),
             new Int8Array.Builder().Append(sbyte.MinValue).AppendNull().Append(sbyte.MaxValue).Build()], 3)]);
        await table.SetSchemaAsync(
            Schema(new Field("id", Int64Type.Default, false), new Field("d", DoubleType.Default, true)));

        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        using (var reader = new ParquetFileReader(
                   new LocalRandomAccessFile(Path.Combine(_tempDir, DeltaLake.DeltaPath.Decode(file.Path)))))
        {
            var schema = await reader.GetSchemaAsync();
            var leaves = StoredInt32Columns.Resolve(schema, table.CurrentSnapshot.Schema, ColumnMappingMode.None);
            Assert.Equal(["d"], schema.Columns.Where(c => leaves?.Contains(c.SchemaElement) == true).Select(c => c.DottedPath));
        }

        var values = new List<(long, double?)>();
        foreach (var batch in await ReadAsync(table))
        {
            var id = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            var d = Assert.IsType<DoubleArray>(batch.Column(batch.Schema.GetFieldIndex("d")));
            for (int i = 0; i < batch.Length; i++)
                values.Add((id.GetValue(i)!.Value, d.GetValue(i)));
        }
        Assert.Equal([(1L, (double?)sbyte.MinValue), (2L, null), (3L, sbyte.MaxValue)], values.OrderBy(v => v.Item1));
    }

    /// <summary>A decimal target is left to the narrow-then-widen route (see <see cref="StoredInt32Columns"/>).</summary>
    [Fact]
    public async Task DecimalTarget_IsNotRelabeled()
    {
        var oldSchema = Schema(new Field("id", Int64Type.Default, false), new Field("d", Int8Type.Default, true));
        await using var table = await DeltaTable.CreateAsync(Fs, oldSchema, configuration: Widening);
        await table.WriteAsync([new RecordBatch(oldSchema,
            [new Int64Array.Builder().Append(1).Build(), new Int8Array.Builder().Append(-12).Build()], 1)]);
        await table.SetSchemaAsync(
            Schema(new Field("id", Int64Type.Default, false), new Field("d", new Decimal128Type(4, 1), true)));

        var file = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        using var reader = new ParquetFileReader(
            new LocalRandomAccessFile(Path.Combine(_tempDir, DeltaLake.DeltaPath.Decode(file.Path))));
        Assert.Null(StoredInt32Columns.Resolve(
            await reader.GetSchemaAsync(), table.CurrentSnapshot.Schema, ColumnMappingMode.None));
    }
}

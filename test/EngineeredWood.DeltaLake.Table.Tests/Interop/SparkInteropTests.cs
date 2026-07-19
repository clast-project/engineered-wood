// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3</b> external validation against the Delta reference implementation. See
/// <see cref="Spark"/> for setup and cost.</para>
///
/// <para>These tests deliberately do NOT duplicate tier 1. Each one covers something delta-rs
/// structurally cannot: reading the legacy column-mapping protocol, <c>DESCRIBE DETAIL</c>,
/// clustering metadata, Spark <i>mutating</i> an EW-written table, or data skipping with the count of
/// files actually touched. Anything delta-rs can check belongs in <see cref="DeltaRsInteropTests"/>,
/// which runs in seconds.</para>
/// </summary>
public class SparkInteropTests : IDisposable
{
    private readonly string _tempDir;

    public SparkInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_spark_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema IdRegionSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("region", StringType.Default, false))
        .Build();

    private static RecordBatch IdRegionBatch(long[] ids, string[] regions)
    {
        var idArray = new Int64Array.Builder().AppendRange(ids).Build();
        var regionBuilder = new StringArray.Builder();
        foreach (string r in regions)
            regionBuilder.Append(r);
        return new RecordBatch(IdRegionSchema, [idArray, regionBuilder.Build()], ids.Length);
    }

    private static async Task<List<(long Id, string Region)>> ReadAllViaEw(DeltaTable table)
    {
        var rows = new List<(long, string)>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            var regions = (StringArray)batch.Column("region");
            for (int i = 0; i < batch.Length; i++)
                rows.Add((ids.GetValue(i)!.Value, regions.GetString(i)));
        }

        rows.Sort();
        return rows;
    }

    private static List<(long Id, string Region)> RowsFromJson(JsonElement result)
    {
        var rows = new List<(long, string)>();
        foreach (var row in result.GetProperty("rows").EnumerateArray())
            rows.Add((row.GetProperty("id").GetInt64(), row.GetProperty("region").GetString()!));

        rows.Sort();
        return rows;
    }

    /// <summary>Creates an EW table with the standard schema and one batch of rows.</summary>
    private async Task<DeltaTable> CreateEwTable(
        long[] ids,
        string[] regions,
        ColumnMappingMode columnMapping = ColumnMappingMode.None,
        IReadOnlyList<string>? clusteringColumns = null)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var table = await DeltaTable.CreateAsync(
            fs, IdRegionSchema, columnMappingMode: columnMapping, clusteringColumns: clusteringColumns);
        await table.WriteAsync([IdRegionBatch(ids, regions)]);
        return table;
    }

    // ── Baseline + protocol shape ──

    /// <summary>
    /// EW writes, the reference implementation reads. Also asserts the protocol shape as Spark
    /// resolves it, via DESCRIBE DETAIL — a surface tier 1 does not have at all.
    /// </summary>
    [Fact]
    public async Task EwWritten_SimpleTable_SparkReadsSameRowsAndProtocol()
    {
        if (!Spark.EnsureAvailable()) return;

        await using var table = await CreateEwTable([1, 2, 3], ["us", "eu", "us"]);

        var result = Spark.Invoke("read", new { path = _tempDir });

        Assert.Equal(3, result.GetProperty("row_count").GetInt32());
        Assert.Equal(await ReadAllViaEw(table), RowsFromJson(result));

        var detail = result.GetProperty("detail");
        Assert.Equal("delta", detail.GetProperty("format").GetString());
        Assert.Empty(detail.GetProperty("partition_columns").EnumerateArray());
        // A plain table must not silently declare features it does not need — over-declaring locks
        // out readers for no reason, and is invisible without an engine that resolves features.
        Assert.Equal(1, detail.GetProperty("min_reader_version").GetInt32());
        Assert.Equal(2, detail.GetProperty("min_writer_version").GetInt32());
    }

    /// <summary>The reference implementation writes, EW reads.</summary>
    [Fact]
    public async Task SparkWritten_SimpleTable_EwReadsSameRows()
    {
        if (!Spark.EnsureAvailable()) return;

        Spark.Invoke("write", new
        {
            path = _tempDir,
            schema = "id long, region string",
            rows = new object[]
            {
                new object[] { 1L, "us" },
                new object[] { 2L, "eu" },
                new object[] { 3L, "apac" },
            },
        });

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.OpenAsync(fs);

        Assert.Equal([(1L, "us"), (2L, "eu"), (3L, "apac")], await ReadAllViaEw(table));
    }

    // ── The tier-1 blind spot: legacy column mapping (slice 6, `aa3f0e2`). ──

    /// <summary>
    /// <para>The gap <see cref="DeltaRsInteropTests.EwWritten_ColumnMapping_CommitShapeIsSpecCorrect_ReadBackNeedsTier3"/>
    /// documents. EW declares column mapping with <c>minReader=2</c>/<c>minWriter=5</c>, which
    /// delta-rs refuses to open; Spark reads it.</para>
    ///
    /// <para>This is the scenario that crashed PySpark on physical names — data lives in Parquet under
    /// physical column names, and the engine must resolve them back to logical ones through the
    /// schema metadata. A round-trip cannot test this: EW resolves against its own mapping either way.</para>
    /// </summary>
    [Fact]
    public async Task EwWritten_ColumnMapping_SparkResolvesPhysicalNamesToLogical()
    {
        if (!Spark.EnsureAvailable()) return;

        await using var table = await CreateEwTable(
            [1, 2, 3], ["us", "eu", "apac"], columnMapping: ColumnMappingMode.Name);

        var result = Spark.Invoke("read", new { path = _tempDir });

        // LOGICAL names must surface, not the physical UUIDs the Parquet files actually carry.
        var columns = result.GetProperty("columns").EnumerateArray().Select(c => c.GetString()!).ToList();
        Assert.Equal(["id", "region"], columns);
        Assert.Equal(await ReadAllViaEw(table), RowsFromJson(result));

        var detail = result.GetProperty("detail");
        Assert.Equal("name", detail.GetProperty("properties")
            .GetProperty("delta.columnMapping.mode").GetString());
    }

    // ── Clustering (slice 10, `093185f`). ──

    /// <summary>
    /// <para>EW's clustered-table interop stores clustering columns as <b>physical</b> names in the
    /// domain metadata, because OSS Delta <c>None.get</c>-crashes otherwise. That decision was made
    /// from reading the OSS source; this is the test that actually exercises it.</para>
    ///
    /// <para>DESCRIBE DETAIL's <c>clusteringColumns</c> is the only external surface that reports what
    /// an engine resolved the domain to, so tier 3 is the only tier that can check it.</para>
    /// </summary>
    [Fact]
    public async Task EwWritten_Clustered_SparkResolvesClusteringColumns()
    {
        if (!Spark.EnsureAvailable()) return;

        await using var table = await CreateEwTable([1, 2, 3], ["us", "eu", "apac"],
            clusteringColumns: ["region"]);

        var result = Spark.Invoke("read", new { path = _tempDir });

        Assert.Equal(await ReadAllViaEw(table), RowsFromJson(result));

        var clustering = result.GetProperty("detail").GetProperty("clustering_columns")
            .EnumerateArray().Select(c => c.GetString()!).ToList();
        Assert.Equal(["region"], clustering);
    }

    // ── Writer-side: Spark MUTATING an EW table. ──

    /// <summary>
    /// <para>Spark running OPTIMIZE over an EW-written table, then EW reading the result. Being
    /// readable is a weaker property than being writable-through: OPTIMIZE rewrites data files and
    /// commits removes plus adds against EW's log, so it fails if EW's commit shape, stats or paths
    /// are subtly off in ways a read tolerates.</para>
    ///
    /// <para>The row set must be identical afterwards — OPTIMIZE is purely a layout change.</para>
    /// </summary>
    [Fact]
    public async Task EwWritten_SparkOptimize_PreservesRowsAndEwReadsBack()
    {
        if (!Spark.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema);

        // Several small files so OPTIMIZE has something to compact.
        for (long i = 0; i < 5; i++)
            await table.WriteAsync([IdRegionBatch([i], [i % 2 == 0 ? "us" : "eu"])]);

        var expected = await ReadAllViaEw(table);

        var result = Spark.Invoke("sql", new
        {
            path = _tempDir,
            sql = new[] { "OPTIMIZE delta.`{path}`" },
        });

        Assert.Equal(expected, RowsFromJson(result));

        // And EW must read back what Spark rewrote.
        await table.RefreshAsync();
        Assert.Equal(expected, await ReadAllViaEw(table));
    }

    /// <summary>
    /// <para>Deletion vectors produced by the reference implementation, read by EW. Slice 2 rewrote
    /// DV serialization (64-bit RoaringBitmapArray + on-disk <c>.bin</c> framing) against the spec
    /// text; this checks EW against a DV that Spark actually wrote.</para>
    ///
    /// <para>Spark DELETE with <c>delta.enableDeletionVectors</c> soft-deletes via a DV rather than
    /// rewriting the file, so EW must apply the DV to get the right rows — a reader that ignored it
    /// entirely would return the deleted row and pass every round-trip test EW has.</para>
    /// </summary>
    [Fact]
    public async Task SparkWritten_DeletionVector_EwAppliesItOnRead()
    {
        if (!Spark.EnsureAvailable()) return;

        Spark.Invoke("write", new
        {
            path = _tempDir,
            schema = "id long, region string",
            rows = new object[]
            {
                new object[] { 1L, "us" },
                new object[] { 2L, "eu" },
                new object[] { 3L, "apac" },
                new object[] { 4L, "us" },
            },
            options = new Dictionary<string, string> { ["delta.enableDeletionVectors"] = "true" },
            sql = new[] { "DELETE FROM delta.`{path}` WHERE id = 2" },
        });

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.OpenAsync(fs);

        Assert.Equal([(1L, "us"), (3L, "apac"), (4L, "us")], await ReadAllViaEw(table));
    }

    // ── Data skipping. Wrong stats lose rows silently; only a pruning engine notices. ──

    /// <summary>
    /// <para>Spark consults each file's <c>minValues</c>/<c>maxValues</c> and skips files whose range
    /// cannot match the predicate. If EW records a range that is too narrow, Spark drops a file it
    /// should have read and the query returns fewer rows — with no error anywhere.</para>
    ///
    /// <para>Asserting <c>files_scanned &lt; files_total</c> is what keeps this honest: row
    /// correctness alone would also pass on an engine that never pruned, proving nothing.</para>
    /// </summary>
    [Fact]
    public async Task EwWritten_MinMaxStats_SparkSkipsFilesWithoutLosingRows()
    {
        if (!Spark.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema);
        await table.WriteAsync([IdRegionBatch([10, 11, 12], ["us", "eu", "us"])]);
        await table.WriteAsync([IdRegionBatch([100, 101], ["eu", "apac"])]);
        await table.WriteAsync([IdRegionBatch([200], ["us"])]);

        var result = Spark.Invoke("scan", new { path = _tempDir, filter = "id >= 100" });

        Assert.Equal(3, result.GetProperty("files_total").GetInt32());
        Assert.Equal(2, result.GetProperty("files_scanned").GetInt32());
        Assert.Equal([(100L, "eu"), (101L, "apac"), (200L, "us")], RowsFromJson(result));
    }

    /// <summary>
    /// <para>The same thing one level down. Slice 8 landed nested stats end-to-end
    /// (<c>minValues.payload.score</c> and friends), and nothing external has ever read them — a
    /// round-trip cannot, because EW's own reader does not prune on them.</para>
    ///
    /// <para>Nested stats are the easier ones to get wrong: the paths have to be built recursively and
    /// a struct that is skipped or mis-keyed produces a log that still parses and still round-trips.</para>
    /// </summary>
    [Fact]
    public async Task EwWritten_NestedStats_SparkSkipsOnNestedFieldWithoutLosingRows()
    {
        if (!Spark.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, NestedSchema);
        await table.WriteAsync([NestedBatch([1, 2], [10, 11])]);
        await table.WriteAsync([NestedBatch([3, 4], [100, 101])]);
        await table.WriteAsync([NestedBatch([5], [200])]);

        var result = Spark.Invoke("scan", new { path = _tempDir, filter = "payload.score >= 100" });

        Assert.Equal(3, result.GetProperty("files_total").GetInt32());
        Assert.Equal(2, result.GetProperty("files_scanned").GetInt32());

        var scores = result.GetProperty("rows").EnumerateArray()
            .Select(r => r.GetProperty("payload").GetProperty("score").GetInt64())
            .OrderBy(v => v).ToList();
        Assert.Equal([100L, 101L, 200L], scores);
    }

    // ── Fail-closed on features EW cannot evaluate. ──

    /// <summary>
    /// <para>A CHECK constraint carries arbitrary SQL that EW cannot evaluate, so
    /// <c>HonorWriterFeatures</c> rejects the write rather than committing possibly-violating data —
    /// Delta constraints are enforced at write time only, so one bad commit poisons the table for
    /// every reader afterwards.</para>
    ///
    /// <para>That check keys on the <c>delta.constraints.</c> configuration prefix, which is an
    /// ASSUMPTION about what Spark emits. If the real key ever differed, the guard would silently
    /// never fire and EW would write straight through it. This pins the assumption against a
    /// constraint Spark actually created.</para>
    /// </summary>
    [Fact]
    public async Task SparkWritten_CheckConstraint_EwRefusesToWrite()
    {
        if (!Spark.EnsureAvailable()) return;

        Spark.Invoke("write", new
        {
            path = _tempDir,
            schema = "id long, score long",
            rows = new object[] { new object[] { 1L, 50L } },
            sql = new[] { "ALTER TABLE delta.`{path}` ADD CONSTRAINT score_pos CHECK (score > 0)" },
        });

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("score", Int64Type.Default, false))
            .Build();

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.OpenAsync(fs);

        var ids = new Int64Array.Builder().Append(2L).Build();
        var scores = new Int64Array.Builder().Append(-1L).Build();
        var batch = new RecordBatch(schema, [ids, scores], 1);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.WriteAsync([batch]));
        Assert.Contains("score_pos", ex.Message);
    }

    /// <summary>
    /// The same fail-closed contract for generated columns, whose expression lives in FIELD metadata
    /// rather than table configuration — a separate code path with its own assumed key
    /// (<c>delta.generationExpression</c>). Writing without computing the generated value would
    /// produce a column that silently disagrees with its own definition.
    /// </summary>
    [Fact]
    public async Task SparkWritten_GeneratedColumn_EwRefusesToWrite()
    {
        if (!Spark.EnsureAvailable()) return;

        Spark.Invoke("create", new
        {
            path = _tempDir,
            columns = new object[]
            {
                new { name = "id", type = "LONG" },
                new { name = "doubled", type = "LONG", generated_always_as = "id * 2" },
            },
        });

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("doubled", Int64Type.Default, false))
            .Build();

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.OpenAsync(fs);

        var ids = new Int64Array.Builder().Append(5L).Build();
        var doubled = new Int64Array.Builder().Append(999L).Build(); // deliberately not 10
        var batch = new RecordBatch(schema, [ids, doubled], 1);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.WriteAsync([batch]));
        Assert.Contains("doubled", ex.Message);
    }

    private static Apache.Arrow.Schema NestedSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("payload", new Apache.Arrow.Types.StructType(
        [
            new Field("score", Int64Type.Default, false),
            new Field("label", StringType.Default, false),
        ]), false))
        .Build();

    private static RecordBatch NestedBatch(long[] ids, long[] scores)
    {
        var idArray = new Int64Array.Builder().AppendRange(ids).Build();
        var scoreArray = new Int64Array.Builder().AppendRange(scores).Build();
        var labelBuilder = new StringArray.Builder();
        foreach (long s in scores)
            labelBuilder.Append($"s{s}");

        var structType = (Apache.Arrow.Types.StructType)NestedSchema.GetFieldByName("payload").DataType;
        var payload = new StructArray(
            structType, ids.Length, [scoreArray, labelBuilder.Build()], ArrowBuffer.Empty, nullCount: 0);

        return new RecordBatch(NestedSchema, [idArray, payload], ids.Length);
    }
}

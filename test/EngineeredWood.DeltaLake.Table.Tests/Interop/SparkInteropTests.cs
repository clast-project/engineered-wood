// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
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

    /// <summary>
    /// <para>Two independent EW handles blind-append concurrently (slice 9 step 3). The second holds a
    /// stale snapshot, so its commit collides and REBASES onto the winner through the optimistic-
    /// concurrency loop rather than failing. The reference implementation must then read all rows.</para>
    ///
    /// <para>This is the one cross-engine surface the OCC work touches. The race logic itself is
    /// single-process and unit-tested, but the ARTIFACT a rebase leaves on disk had never been read by
    /// a foreign engine. The claim being measured is that a rebased commit is byte-for-byte an ordinary
    /// sequential commit (same add action, next version number) with nothing special for Spark to
    /// interpret — if a rebase ever mis-numbered a commit or duplicated an action, the row count here
    /// would be wrong. A round-trip through EW alone could not catch that.</para>
    /// </summary>
    [Fact]
    public async Task EwConcurrentAppends_Rebased_SparkReadsAllRows()
    {
        if (!Spark.EnsureAvailable()) return;

        await using (var setup = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdRegionSchema))
        {
            await setup.WriteAsync([IdRegionBatch([1], ["us"])]);
        }

        // Two handles at the same base version; each blind-appends without seeing the other.
        await using var tableA = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await using var tableB = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        long baseVersion = tableA.CurrentSnapshot.Version;

        long vA = await tableA.WriteAsync([IdRegionBatch([2], ["eu"])]);
        long vB = await tableB.WriteAsync([IdRegionBatch([3], ["apac"])]); // stale -> collides -> rebases

        Assert.Equal(baseVersion + 1, vA);
        Assert.Equal(baseVersion + 2, vB); // landed one version past the winner, not thrown

        var result = Spark.Invoke("read", new { path = _tempDir });
        Assert.Equal(3, result.GetProperty("row_count").GetInt32());
        Assert.Equal([(1L, "us"), (2L, "eu"), (3L, "apac")], RowsFromJson(result));
    }

    /// <summary>
    /// <para>The Databricks row-level-concurrency artifact — a UNIONED deletion vector from two concurrent
    /// EW deletes of disjoint rows of the SAME file (slice 9 Layer 3 sub-problem A) — read by a DV-capable
    /// engine. Two EW handles delete the first and last of three rows in one file; the loser rebases its DV
    /// onto the winner's, so the file ends up carrying DV {0, 2}. Spark must apply that union and return
    /// only the middle row.</para>
    ///
    /// <para>This is the cross-engine half tier 1 cannot cover: deltalake 1.6.2's reader does not support
    /// deletion vectors and refuses the table (its counterpart pins that safe refusal). Spark 4.0 does
    /// support them, so this is where the claim "the resulting DV is spec-legal and reads correctly" is
    /// actually measured — the union masks exactly the two deleted positions, not all-or-nothing.</para>
    /// </summary>
    [Fact]
    public async Task EwWritten_UnionedDeletionVector_SparkReadsSurvivingRow()
    {
        if (!Spark.EnsureAvailable()) return;

        await using (var setup = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdRegionSchema, enableDeletionVectors: true))
        {
            await setup.WriteAsync([IdRegionBatch([5, 7, 9], ["us", "eu", "us"])]); // one file, rows 0/1/2
        }

        await using var tableA = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await using var tableB = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

        var (_, vA) = await tableA.DeleteAsync(DeleteId(5)); // pos 0
        var (_, vB) = await tableB.DeleteAsync(DeleteId(9)); // pos 2 — stale, collides, DV-union rebase
        Assert.True(vB > vA);

        await using var reader = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        Assert.Equal([(7L, "eu")], await ReadAllViaEw(reader)); // EW applies the union

        var result = Spark.Invoke("read", new { path = _tempDir });
        Assert.Equal(1, result.GetProperty("row_count").GetInt32());
        Assert.Equal([(7L, "eu")], RowsFromJson(result)); // Spark applies it too
    }

    /// <summary>
    /// <para>EW appends to a row-tracking table (<c>delta.enableRowTracking=true</c>); the reference engine
    /// reads back each row's stable id via Delta's <c>_metadata.row_id</c> generated column. Two appends
    /// (2 rows then 1) span two files, so the ids come from BOTH files' <c>add.baseRowId</c> plus physical
    /// position — 0,1 in the first file, 2 in the second — with no materialized column on disk.</para>
    ///
    /// <para>This is the cross-engine claim that matters for Milestone 1: EW's freshly-appended
    /// <c>baseRowId</c> / <c>defaultRowCommitVersion</c> are spec-correct, so a conformant reader derives the
    /// intended stable ids. A round-trip through EW alone cannot prove it — EW does not itself expose row ids
    /// yet, and it would agree with its own (possibly wrong) convention. Spark computing 0,1,2 with no gaps or
    /// duplicates is the actual measurement.</para>
    /// </summary>
    [Fact]
    public async Task EwAppended_RowTracking_SparkReadsBaseRowIdPositionIds()
    {
        if (!Spark.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(fs, IdRegionSchema, enableRowTracking: true))
        {
            await table.WriteAsync([IdRegionBatch([10, 20], ["us", "eu"])]);   // file 0: row ids 0, 1
            await table.WriteAsync([IdRegionBatch([30], ["apac"])]);           // file 1: row id 2
        }

        var result = Spark.Invoke("read_row_ids", new { path = _tempDir });

        var rowIds = result.GetProperty("row_ids").EnumerateArray().Select(e => e.GetInt64()).ToList();
        Assert.Equal([0L, 1L, 2L], rowIds); // contiguous, unique -> baseRowId + position across both files

        // Spark must actually recognize the writer feature to expose the metadata column.
        var features = result.GetProperty("detail").GetProperty("table_features")
            .EnumerateArray().Select(e => e.GetString()).ToList();
        Assert.Contains("rowTracking", features);
    }

    /// <summary>"delete the row(s) with this id" as the functional predicate DeleteAsync takes.</summary>
    private static Func<RecordBatch, BooleanArray> DeleteId(long target) => batch =>
    {
        var ids = (Int64Array)batch.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < ids.Length; i++)
            mask.Append(ids.GetValue(i) == target);
        return mask.Build();
    };

    /// <summary>
    /// A copy-on-write UPDATE on a PARTITIONED table must write the rewritten file into the partition
    /// directory the reference engine expects, not the table root. EW rewrites the <c>region='us'</c>
    /// partition file (id → id+100), then Spark reads it back whole and partition-scoped. The partition
    /// scan touches exactly the two partition files (us + eu) and returns the updated us rows — proving the
    /// rewritten file landed where Spark's planner finds it for a <c>region='us'</c> query.
    /// </summary>
    [Fact]
    public async Task EwUpdated_PartitionedTable_SparkReadsRewrittenFile()
    {
        if (!Spark.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, IdRegionSchema, partitionColumns: ["region"]);
        await table.WriteAsync([IdRegionBatch([1, 2, 3], ["us", "eu", "us"])]); // us: {1,3}, eu: {2}

        await table.UpdateAsync(
            b =>
            {
                var region = (StringArray)b.Column("region");
                var mask = new BooleanArray.Builder();
                for (int i = 0; i < region.Length; i++) mask.Append(region.GetString(i) == "us");
                return mask.Build();
            },
            b =>
            {
                var id = (Int64Array)b.Column("id");
                var newIds = new Int64Array.Builder();
                for (int i = 0; i < id.Length; i++) newIds.Append(id.GetValue(i)!.Value + 100);
                return new RecordBatch(IdRegionSchema, [newIds.Build(), b.Column("region")], b.Length);
            });

        var all = Spark.Invoke("read", new { path = _tempDir });
        Assert.Equal([(2L, "eu"), (101L, "us"), (103L, "us")], RowsFromJson(all));

        var us = Spark.Invoke("scan", new { path = _tempDir, filter = "region = 'us'" });
        Assert.Equal(2, us.GetProperty("files_total").GetInt32());   // one file per partition
        Assert.Equal(1, us.GetProperty("files_scanned").GetInt32()); // only region=us pruned in
        Assert.Equal([(101L, "us"), (103L, "us")], RowsFromJson(us));
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
    /// DATE column statistics written by EW must let the reference implementation skip files. Delta stores
    /// date bounds as "yyyy-MM-dd" strings; EW previously emitted a raw day number, which Spark cannot
    /// decode as a date bound, so it pruned nothing. This writes three single-row files spanning three
    /// years and asserts Spark skips the out-of-range one under a date predicate — proving EW's date stats
    /// are in the format a foreign reader actually prunes on.
    /// </summary>
    [Fact]
    public async Task EwWritten_DateStats_SparkSkipsFilesWithoutLosingRows()
    {
        if (!Spark.EnsureAvailable()) return;

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("d", Date32Type.Default, false))
            .Build();

        RecordBatch DateRow(long id, DateTime day)
        {
            var ids = new Int64Array.Builder().Append(id).Build();
            var dates = new Date32Array.Builder()
                .Append(DateTime.SpecifyKind(day, DateTimeKind.Utc)).Build();
            return new RecordBatch(schema, [ids, dates], 1);
        }

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([DateRow(1, new DateTime(2020, 1, 15))]);  // pruned
        await table.WriteAsync([DateRow(2, new DateTime(2021, 6, 20))]);
        await table.WriteAsync([DateRow(3, new DateTime(2022, 12, 31))]);

        var result = Spark.Invoke("scan", new { path = _tempDir, filter = "d >= DATE '2021-06-01'" });

        Assert.Equal(3, result.GetProperty("files_total").GetInt32());
        Assert.Equal(2, result.GetProperty("files_scanned").GetInt32()); // the 2020 file is skipped
        Assert.Equal(2, result.GetProperty("row_count").GetInt32());
    }

    /// <summary>
    /// DECIMAL column statistics written by EW must let the reference implementation skip files. Delta
    /// stores decimal bounds as JSON numbers; EW previously collected no decimal stats at all, so it pruned
    /// nothing. This writes three single-row files and asserts Spark skips the out-of-range one under a
    /// decimal predicate — proving EW's decimal stats are in the numeric form a foreign reader prunes on.
    /// </summary>
    [Fact]
    public async Task EwWritten_DecimalStats_SparkSkipsFilesWithoutLosingRows()
    {
        if (!Spark.EnsureAvailable()) return;

        var type = new Decimal128Type(12, 2);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("amt", type, false))
            .Build();

        RecordBatch AmtRow(long id, decimal amt)
        {
            var ids = new Int64Array.Builder().Append(id).Build();
            var unscaled = new BigInteger(amt * 100m); // scale 2
            var bytes = new byte[16];
            var dest = bytes.AsSpan();
            dest.Fill(unscaled.Sign < 0 ? (byte)0xFF : (byte)0x00);
#if NET6_0_OR_GREATER
            unscaled.TryWriteBytes(dest, out _, isUnsigned: false, isBigEndian: false);
#else
            var bb = unscaled.ToByteArray();
            bb.AsSpan(0, Math.Min(bb.Length, 16)).CopyTo(dest);
#endif
            var data = new ArrayData(type, 1, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(bytes)]);
            return new RecordBatch(schema, [ids, new Decimal128Array(data)], 1);
        }

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([AmtRow(1, 10.00m)]);    // pruned
        await table.WriteAsync([AmtRow(2, 500.00m)]);
        await table.WriteAsync([AmtRow(3, 2000.00m)]);

        var result = Spark.Invoke("scan", new { path = _tempDir, filter = "amt >= 100" });

        Assert.Equal(3, result.GetProperty("files_total").GetInt32());
        Assert.Equal(2, result.GetProperty("files_scanned").GetInt32()); // the 10.00 file is skipped
        Assert.Equal(2, result.GetProperty("row_count").GetInt32());
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

    // ── VACUUM: a destructive operation validated by the reference implementation. ──

    /// <summary>
    /// <para>Spark writes a table and DELETEs a row, producing a deletion-vector <c>.bin</c>. EW then
    /// vacuums with zero retention — the most aggressive setting there is — and Spark reads again.</para>
    ///
    /// <para>This is the acceptance test for the vacuum rewrite. EW must resolve a DV path written by
    /// ANOTHER engine (Z85-encoded UUID, possibly with a directory prefix) well enough to protect the
    /// file. Get it wrong and the <c>.bin</c> is swept, the mask disappears, and the deleted row
    /// silently returns as live data. EW's own reader would report the same wrong answer, so only a
    /// foreign reader can catch it — and only after a vacuum that actually deleted something.</para>
    /// </summary>
    [Fact]
    public void SparkWrittenDeletionVector_SurvivesEwVacuum()
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

        int binsBefore = Directory.GetFiles(_tempDir, "*.bin", SearchOption.AllDirectories).Length;
        Assert.True(binsBefore > 0, "expected Spark to write a deletion-vector .bin");

        VacuumViaEw();

        Assert.Equal(binsBefore, Directory.GetFiles(_tempDir, "*.bin", SearchOption.AllDirectories).Length);

        var result = Spark.Invoke("read", new { path = _tempDir });
        Assert.Equal([(1L, "us"), (3L, "apac"), (4L, "us")], RowsFromJson(result));
    }

    /// <summary>
    /// The complementary direction: after EW collects genuinely orphaned files, the reference
    /// implementation must still read the table. A vacuum that deletes one file too many shows up here
    /// as a read failure rather than as wrong data.
    /// </summary>
    [Fact]
    public async Task EwVacuumed_SparkStillReadsTable()
    {
        if (!Spark.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(fs, IdRegionSchema))
        {
            await table.WriteAsync([IdRegionBatch([1, 2], ["us", "eu"])]);
            await table.WriteAsync([IdRegionBatch([3, 4], ["apac", "us"])], DeltaWriteMode.Overwrite);

            var vacuumed = await table.VacuumAsync(retentionPeriod: TimeSpan.Zero, dryRun: false);
            Assert.NotEmpty(vacuumed.FilesToDelete);
        }

        var result = Spark.Invoke("read", new { path = _tempDir });
        Assert.Equal([(3L, "apac"), (4L, "us")], RowsFromJson(result));
    }

    /// <summary>Opens the Spark-written table with EW and vacuums it at zero retention.</summary>
    private void VacuumViaEw()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var task = Task.Run(async () =>
        {
            await using var table = await DeltaTable.OpenAsync(fs);
            await table.VacuumAsync(retentionPeriod: TimeSpan.Zero, dryRun: false);
        });

        task.GetAwaiter().GetResult();
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

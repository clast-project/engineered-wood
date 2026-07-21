// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// Tier-1 external validation against delta-rs. See <see cref="DeltaRs"/> for why round-tripping
/// alone was not enough and how the availability gate works.
///
/// <para>Each test names the slice from <c>doc/upstream-landing-notes.md</c> whose correctness it
/// pins, so a failure points at the change that regressed rather than at "interop".</para>
/// </summary>
public class DeltaRsInteropTests : IDisposable
{
    private readonly string _tempDir;

    public DeltaRsInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_xval_{Guid.NewGuid():N}");
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

    /// <summary>Reads every row EW sees, as (id, region) pairs sorted for order-independent compare.</summary>
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

    /// <summary>Same shape, out of the driver's JSON, so the two sides compare directly.</summary>
    private static List<(long Id, string Region)> RowsFromJson(JsonElement result)
    {
        var rows = new List<(long, string)>();
        foreach (var row in result.GetProperty("rows").EnumerateArray())
            rows.Add((row.GetProperty("id").GetInt64(), row.GetProperty("region").GetString()!));

        rows.Sort();
        return rows;
    }

    // ── Baselines. Nothing below these is meaningful if these two fail. ──

    /// <summary>
    /// The buffered-transaction seam's fused metaData + add commit, read by a conformant reader (delta-rs
    /// cannot read deletion vectors, so this covers the DV-free half — a schema ALTER + an eagerly-written
    /// INSERT fused into ONE commit via ComputeAddColumn + WriteDataFilesAsync + CommitDataFilesAsync).
    /// delta-rs must read the single schema-evolved commit: the new column NULL-backfilled on the old file,
    /// valued on the new file. A mis-shaped fused commit (a stray second metaData, an add referencing a schema
    /// the reader doesn't have) would surface here as wrong columns or a read failure.
    /// </summary>
    [Fact]
    public async Task EwFusedAlterInsert_DeltaRsReadsSchemaEvolvedCommit()
    {
        if (!DeltaRs.EnsureAvailable()) return;

        var baseSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), baseSchema);
        await table.WriteAsync([new RecordBatch(baseSchema,
        [
            new Int64Array.Builder().Append(1).Append(2).Append(3).Build(),
            new StringArray.Builder().Append("v1").Append("v2").Append("v3").Build(),
        ], 3)]);
        var pinned = table.CurrentSnapshot;

        // ALTER ADD COLUMN extra INT + INSERT (4, 5) under the pending schema, fused into ONE commit
        var change = table.ComputeAddColumn(new Field("extra", Int32Type.Default, true));
        var widened = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Field(new Field("extra", Int32Type.Default, true))
            .Build();
        var files = await table.WriteDataFilesAsync([new RecordBatch(widened,
        [
            new Int64Array.Builder().Append(4).Append(5).Build(),
            new StringArray.Builder().Append("v4").Append("v5").Build(),
            new Int32Array.Builder().Append(40).Append(50).Build(),
        ], 2)], schemaOverride: change.NewSchema);
        await table.CommitDataFilesAsync(files, DeltaWriteMode.Append,
            extraActions: change.Actions, expectedVersion: pinned.Version, operation: "TRANSACTION");

        var result = DeltaRs.Invoke("read", new { path = _tempDir });
        Assert.Equal(5, result.GetProperty("row_count").GetInt32());
        var seen = new Dictionary<long, int?>();
        foreach (var row in result.GetProperty("rows").EnumerateArray())
        {
            long id = row.GetProperty("id").GetInt64();
            var extraProp = row.GetProperty("extra");
            seen[id] = extraProp.ValueKind == JsonValueKind.Null ? null : extraProp.GetInt32();
        }
        Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, seen.Keys.OrderBy(k => k).ToArray());
        Assert.Null(seen[1]); // old file: new column backfilled NULL
        Assert.Null(seen[3]);
        Assert.Equal(40, seen[4]); // new file: valued
        Assert.Equal(50, seen[5]);
    }

    /// <summary>EW writes, delta-rs reads: the same rows, the same schema.</summary>
    [Fact]
    public async Task EwWritten_SimpleTable_DeltaRsReadsSameRows()
    {
        if (!DeltaRs.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema);
        await table.WriteAsync([IdRegionBatch([1, 2, 3], ["us", "eu", "us"])]);
        await table.WriteAsync([IdRegionBatch([4, 5], ["apac", "eu"])]);

        var result = DeltaRs.Invoke("read", new { path = _tempDir });

        Assert.Equal(5, result.GetProperty("row_count").GetInt32());
        Assert.Equal(await ReadAllViaEw(table), RowsFromJson(result));
    }

    /// <summary>
    /// EW writes a row-tracking table and then UPDATEs it — a copy-on-write rewrite that writes the hidden
    /// materialized row-id / commit-version columns into the data file. A conformant reader (delta-rs) reads
    /// the table by its Delta SCHEMA, so those hidden physical columns must NOT surface: delta-rs sees exactly
    /// the user rows/columns, with the updated value applied. (Row tracking is a writer-only feature, so
    /// delta-rs reads the table without needing to understand it.)
    /// </summary>
    [Fact]
    public async Task EwUpdated_RowTracking_DeltaRsReadsUserColumnsOnly()
    {
        if (!DeltaRs.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema, enableRowTracking: true);
        await table.WriteAsync([IdRegionBatch([1, 2, 3], ["us", "eu", "us"])]);
        await table.UpdateAsync(
            b =>
            {
                var region = (StringArray)b.Column("region");
                var mask = new BooleanArray.Builder();
                for (int i = 0; i < region.Length; i++)
                    mask.Append(region.GetString(i) == "eu");
                return mask.Build();
            },
            b =>
            {
                var region = new StringArray.Builder();
                for (int i = 0; i < b.Length; i++)
                    region.Append("EU");
                return new RecordBatch(IdRegionSchema, [b.Column("id"), region.Build()], b.Length);
            });

        var result = DeltaRs.Invoke("read", new { path = _tempDir });

        Assert.Equal(3, result.GetProperty("row_count").GetInt32());
        // delta-rs returns only the two user columns (id, region) — the hidden materialized columns do not leak.
        Assert.Equal(await ReadAllViaEw(table), RowsFromJson(result));
    }

    /// <summary>delta-rs writes, EW reads. The reverse direction is what catches EW's reader
    /// quietly accepting only the dialect EW's own writer emits.</summary>
    [Fact]
    public async Task DeltaRsWritten_SimpleTable_EwReadsSameRows()
    {
        if (!DeltaRs.EnsureAvailable()) return;

        DeltaRs.Invoke("write", new
        {
            path = _tempDir,
            columns = new object[]
            {
                new { name = "id", type = "int64", values = new long[] { 1, 2, 3, 4 } },
                new { name = "region", type = "string", values = new[] { "us", "eu", "us", "apac" } },
            },
        });

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.OpenAsync(fs);

        Assert.Equal(
            [(1L, "us"), (2L, "eu"), (3L, "us"), (4L, "apac")],
            await ReadAllViaEw(table));
    }

    // ── Path encoding — landing notes "Deferred follow-up B". ──

    /// <summary>
    /// <para>Ground truth for how delta-rs encodes partition values, pinned as an assertion so the
    /// answer is in the repo rather than in someone's memory of a research pass.</para>
    ///
    /// <para>The encoding is <b>two layers</b>, which is the part a from-first-principles fix would
    /// most likely get wrong: the on-disk directory is Hive-escaped (non-ASCII percent-encoded as
    /// UTF-8 bytes), and then <c>add.path</c> percent-encodes <i>that</i> again — so a literal
    /// <c>%</c> in the directory name appears as <c>%25</c> in the log.</para>
    /// </summary>
    [Fact]
    public void DeltaRs_NonAsciiPartition_PathEncodingGroundTruth()
    {
        if (!DeltaRs.EnsureAvailable()) return;

        DeltaRs.Invoke("write", new
        {
            path = _tempDir,
            partition_by = new[] { "region" },
            columns = new object[]
            {
                new { name = "id", type = "int64", values = new long[] { 1, 2, 3 } },
                new { name = "region", type = "string", values = new[] { "café", "日本", "a b#c?d" } },
            },
        });

        var described = DeltaRs.Invoke("describe", new { path = _tempDir });

        var dirs = described.GetProperty("directories").EnumerateArray()
            .Select(d => d.GetString()!).ToList();
        var addPaths = described.GetProperty("add_paths").EnumerateArray()
            .Select(p => p.GetString()!).ToList();

        // Layer 1 — the physical directory: non-ASCII as UTF-8 %XX, plus space/#/? escaped.
        Assert.Contains("region=caf%C3%A9", dirs);
        Assert.Contains("region=%E6%97%A5%E6%9C%AC", dirs);
        Assert.Contains("region=a%20b%23c%3Fd", dirs);

        // Layer 2 — add.path re-encodes the directory, so every % above becomes %25.
        Assert.Contains(addPaths, p => p.StartsWith("region=caf%25C3%25A9/", StringComparison.Ordinal));
        Assert.Contains(addPaths, p => p.StartsWith("region=%25E6%2597%25A5%25E6%259C%25AC/", StringComparison.Ordinal));
        Assert.Contains(addPaths, p => p.StartsWith("region=a%2520b%2523c%253Fd/", StringComparison.Ordinal));
    }

    /// <summary>
    /// The gap itself: <c>DeltaPath.Encode</c> leaves non-ASCII literal, so an EW-written table with
    /// non-ASCII partition values may be unreadable by a strict foreign reader. EW's own reader
    /// round-trips it fine (<c>Uri.UnescapeDataString</c> is a no-op on literals), which is exactly
    /// why round-trip tests never caught this.
    /// </summary>
    [Fact]
    public async Task EwWritten_NonAsciiPartition_DeltaRsReadsSameRows()
    {
        if (!DeltaRs.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, IdRegionSchema, partitionColumns: ["region"]);
        await table.WriteAsync([IdRegionBatch([1, 2, 3], ["café", "日本", "a b#c?d"])]);

        var result = DeltaRs.Invoke("read", new { path = _tempDir });

        Assert.Equal(3, result.GetProperty("row_count").GetInt32());
        Assert.Equal(await ReadAllViaEw(table), RowsFromJson(result));
    }

    // ── Checkpoint content — slice 3 (`b41f5ad`). ──

    /// <summary>
    /// Forces delta-rs to rebuild state from the checkpoint with the JSON commits hidden, so a pass
    /// proves the checkpoint itself carried the state. A checkpoint that silently drops actions reads
    /// identically to a correct one as long as the commits are still there — which they always are in
    /// a round-trip test.
    /// </summary>
    [Fact]
    public async Task EwWritten_Checkpointed_DeltaRsReadsFromCheckpointOnly()
    {
        if (!DeltaRs.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema);

        // Default CheckpointInterval is 10, so this crosses it.
        for (long i = 0; i < 12; i++)
            await table.WriteAsync([IdRegionBatch([i], [i % 2 == 0 ? "us" : "eu"])]);

        var result = DeltaRs.Invoke("checkpoint_only_read", new { path = _tempDir });

        Assert.NotEmpty(result.GetProperty("hidden_commits").EnumerateArray());
        Assert.Equal(12, result.GetProperty("row_count").GetInt32());
        Assert.Equal(await ReadAllViaEw(table), RowsFromJson(result));
    }

    // ── Statistics and pruning. ──

    /// <summary>
    /// <para>Per-file <c>minValues</c>/<c>maxValues</c>/<c>nullCount</c> must describe what is
    /// actually in each file. This is the one class of bug where being wrong produces no error
    /// anywhere: a foreign engine trusts the stats, skips a file whose recorded range cannot match the
    /// predicate, and the query returns FEWER ROWS with nothing to indicate anything went wrong.</para>
    ///
    /// <para>Every other test in this suite reads whole tables, which never consults statistics at
    /// all — so nothing external has ever checked them. Here delta-rs parses the stats out of the log
    /// and we compare them against the values EW wrote.</para>
    /// </summary>
    [Fact]
    public async Task EwWritten_PerFileStats_DescribeTheFilesTheyBelongTo()
    {
        if (!DeltaRs.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema);

        // Three commits => three files with disjoint, known id ranges.
        await table.WriteAsync([IdRegionBatch([10, 11, 12], ["us", "eu", "us"])]);
        await table.WriteAsync([IdRegionBatch([100, 101], ["eu", "apac"])]);
        await table.WriteAsync([IdRegionBatch([200], ["us"])]);

        var files = DeltaRs.Invoke("add_stats", new { path = _tempDir })
            .GetProperty("files").EnumerateArray().ToList();
        Assert.Equal(3, files.Count);

        var expected = new Dictionary<long, (long Max, long Records)>
        {
            [10] = (12, 3),
            [100] = (101, 2),
            [200] = (200, 1),
        };

        foreach (var file in files)
        {
            long min = file.GetProperty("min.id").GetInt64();
            Assert.True(expected.ContainsKey(min), $"unexpected min.id {min}");
            var (expectedMax, expectedRecords) = expected[min];

            Assert.Equal(expectedMax, file.GetProperty("max.id").GetInt64());
            Assert.Equal(expectedRecords, file.GetProperty("num_records").GetInt64());
            Assert.Equal(0, file.GetProperty("null_count.id").GetInt64());
            Assert.Equal(0, file.GetProperty("null_count.region").GetInt64());
        }
    }

    /// <summary>
    /// A filtered read must return every matching row. delta-rs prunes files against the log's stats
    /// before opening them, so an over-tight min/max shows up here as missing rows — the failure mode
    /// <see cref="EwWritten_PerFileStats_DescribeTheFilesTheyBelongTo"/> guards from the other side.
    /// </summary>
    [Fact]
    public async Task EwWritten_FilteredRead_PrunesWithoutLosingRows()
    {
        if (!DeltaRs.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdRegionSchema);
        await table.WriteAsync([IdRegionBatch([10, 11, 12], ["us", "eu", "us"])]);
        await table.WriteAsync([IdRegionBatch([100, 101], ["eu", "apac"])]);
        await table.WriteAsync([IdRegionBatch([200], ["us"])]);

        // Spans two of the three files, so a file that should be kept is adjacent to one that
        // should be dropped — the shape where an off-by-one in min/max actually bites.
        var result = DeltaRs.Invoke("read", new
        {
            path = _tempDir,
            filters = new object[] { new object[] { "id", ">=", 100 } },
        });

        Assert.Equal([(100L, "eu"), (101L, "apac"), (200L, "us")], RowsFromJson(result));
    }

    /// <summary>
    /// The partition-pruning equivalent: partition values are matched as strings against directory
    /// names, so this also exercises the <c>DeltaPath</c> encoding from the query side rather than the
    /// write side.
    /// </summary>
    [Fact]
    public async Task EwWritten_PartitionFilteredRead_ReturnsAllMatchingRows()
    {
        if (!DeltaRs.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, IdRegionSchema, partitionColumns: ["region"]);
        await table.WriteAsync([IdRegionBatch([1, 2, 3, 4], ["us", "eu", "us", "apac"])]);

        var result = DeltaRs.Invoke("read", new
        {
            path = _tempDir,
            filters = new object[] { new object[] { "region", "=", "us" } },
        });

        Assert.Equal([(1L, "us"), (3L, "us")], RowsFromJson(result));
    }

    /// <summary>
    /// A copy-on-write UPDATE on a PARTITIONED table reads back correctly in delta-rs, whole and
    /// partition-filtered. Note (measured): delta-rs takes partition values from <c>add.partitionValues</c>
    /// in the log, not from the directory, so it reads a rewritten file correctly even when EW dropped it at
    /// the table root — this test does NOT by itself catch the layout bug (that is guarded by
    /// <c>DeleteUpdateTests.Update_PartitionedTable_WritesRewrittenFileIntoPartitionDir</c>). Its value is
    /// confirming the fixed layout still reads correctly cross-engine.
    /// </summary>
    [Fact]
    public async Task EwUpdated_PartitionedTable_DeltaRsReadsRewrittenFile()
    {
        if (!DeltaRs.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, IdRegionSchema, partitionColumns: ["region"]);
        await table.WriteAsync([IdRegionBatch([1, 2, 3], ["us", "eu", "us"])]); // us: {1,3}, eu: {2}

        // Rewrite the us partition file: id -> id + 100 where region = 'us'.
        await table.UpdateAsync(RegionEquals("us"), AddToId(100));

        // Whole-table read: the updated rows must be present exactly once.
        var all = DeltaRs.Invoke("read", new { path = _tempDir });
        Assert.Equal([(2L, "eu"), (101L, "us"), (103L, "us")], RowsFromJson(all));

        // Partition-scoped read: delta-rs lists region=us/ only — a root-dropped file would be missing.
        var us = DeltaRs.Invoke("read", new
        {
            path = _tempDir,
            filters = new object[] { new object[] { "region", "=", "us" } },
        });
        Assert.Equal([(101L, "us"), (103L, "us")], RowsFromJson(us));
    }

    private static Func<RecordBatch, BooleanArray> RegionEquals(string target) => batch =>
    {
        var region = (StringArray)batch.Column("region");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < region.Length; i++)
            mask.Append(region.GetString(i) == target);
        return mask.Build();
    };

    private static Func<RecordBatch, RecordBatch> AddToId(long delta) => batch =>
    {
        var id = (Int64Array)batch.Column("id");
        var newIds = new Int64Array.Builder();
        for (int i = 0; i < id.Length; i++)
            newIds.Append(id.GetValue(i)!.Value + delta);
        return new RecordBatch(IdRegionSchema, [newIds.Build(), batch.Column("region")], batch.Length);
    };

    // ── Row-level concurrency — slice 9 Layer 3 sub-problem A (DELETE/DELETE deletion-vector union). ──

    /// <summary>
    /// <para>Two concurrent EW deletes of DISJOINT rows of the same file both land by rebasing the loser's
    /// deletion vector onto the winner's — producing a <b>unioned</b> DV that no single EW operation wrote
    /// (the Databricks row-level-concurrency extension; OSS Spark/delta-rs conflict at file granularity, so
    /// neither would demonstrate both-land). EW applies that union exactly: it reads back only the surviving
    /// middle row.</para>
    ///
    /// <para>The table opts into deletion vectors at creation, so its protocol declares the
    /// <c>deletionVectors</c> reader feature. That declaration is the correctness fix: it changes a foreign
    /// reader's behavior from <i>silently returning the deleted rows</i> (data loss — what happened before
    /// EW declared the feature) to a <b>safe refusal</b>. Measured against delta-rs 1.6.2, whose reader does
    /// not yet support deletion vectors: it rejects the table outright rather than mis-read it. That is the
    /// spec-correct reaction of a reader that cannot apply DVs, and the same shape as
    /// <see cref="EwWritten_ColumnMapping_CommitShapeIsSpecCorrect_ReadBackNeedsTier3"/>. <b>Validating that
    /// a DV-capable engine reads the union correctly needs tier 3</b> (see
    /// <c>SparkInteropTests.EwWritten_UnionedDeletionVector_SparkReadsSurvivingRow</c>).</para>
    /// </summary>
    [Fact]
    public async Task EwUnionedDeletionVector_EwApplies_DeltaRsSafelyRefusesUnsupportedFeature()
    {
        if (!DeltaRs.EnsureAvailable()) return;

        await using (var setup = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdRegionSchema, enableDeletionVectors: true))
        {
            // One file, three rows at known positions 0/1/2.
            await setup.WriteAsync([IdRegionBatch([5, 7, 9], ["us", "eu", "us"])]);
        }

        await using var tableA = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await using var tableB = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

        // A deletes row 5 (pos 0); B, still on the base snapshot, deletes the disjoint row 9 (pos 2). B
        // collides, rebases its DV onto A's, and the file's DV becomes {0, 2}.
        var (_, vA) = await tableA.DeleteAsync(IdEqualsRegion(5));
        var (_, vB) = await tableB.DeleteAsync(IdEqualsRegion(9));
        Assert.True(vB > vA);

        // EW resolves the union: only the middle row 7 survives.
        await using var reader = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        Assert.Equal([(7L, "eu")], await ReadAllViaEw(reader));

        // The protocol declares the deletionVectors reader feature.
        var described = DeltaRs.Invoke("describe", new { path = _tempDir });
        Assert.Contains("deletionVectors",
            described.GetProperty("reader_features").EnumerateArray().Select(f => f.GetString()));

        // delta-rs 1.6.2 does not support DV reads, so it REFUSES the table (safe) rather than returning the
        // masked rows. If a future delta-rs gains DV support this flips to a clean read of [(7, "eu")].
        var rejected = DeltaRs.InvokeRaw("read", new { path = _tempDir });
        Assert.False(rejected.GetProperty("ok").GetBoolean());
        Assert.Contains("deletionVectors", rejected.GetProperty("error").GetString()!);
    }

    private static Func<RecordBatch, BooleanArray> IdEqualsRegion(long target) => batch =>
    {
        var id = (Int64Array)batch.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < id.Length; i++)
            mask.Append(id.GetValue(i) == target);
        return mask.Build();
    };

    // ── Protocol / writer features — slices 5 (`c1b1474`, `70d2384`) and 6 (`aa3f0e2`). ──

    /// <summary>
    /// <para>Column mapping is the feature that crashed PySpark on physical names — but it is also a
    /// documented <b>limit of tier 1</b>. EW declares column mapping with the legacy
    /// <c>minReaderVersion=2</c> / <c>minWriterVersion=5</c> numbering, which is spec-legal, and
    /// delta-rs 1.6.2 declines to open it: it supports reader version 1, or 3 with explicit reader
    /// features. So delta-rs cannot validate the read-back at all.</para>
    ///
    /// <para>What this test therefore pins is the part tier 1 <i>can</i> see: the commit shape, read
    /// straight off disk without the kernel. <b>Verifying that a foreign engine resolves physical
    /// names back to logical ones needs tier 3 (PySpark), which reads v2/v5 tables.</b> Worth
    /// considering separately: emitting v3/v7 with a <c>columnMapping</c> reader feature instead of
    /// the legacy numbering would make the table readable by delta-rs and DuckDB too.</para>
    /// </summary>
    [Fact]
    public async Task EwWritten_ColumnMapping_CommitShapeIsSpecCorrect_ReadBackNeedsTier3()
    {
        if (!DeltaRs.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, IdRegionSchema, columnMappingMode: ColumnMappingMode.Name);
        await table.WriteAsync([IdRegionBatch([1, 2, 3], ["us", "eu", "apac"])]);

        var actions = DeltaRs.Invoke("raw_log", new { path = _tempDir })
            .GetProperty("actions").EnumerateArray()
            .Select(a => a.GetProperty("action")).ToList();

        var metaData = actions.Single(a => a.TryGetProperty("metaData", out _)).GetProperty("metaData");
        Assert.Equal("name", metaData.GetProperty("configuration")
            .GetProperty("delta.columnMapping.mode").GetString());

        // Physical names and field ids must be stamped on every field of the persisted schema.
        using var schemaDoc = JsonDocument.Parse(metaData.GetProperty("schemaString").GetString()!);
        foreach (var field in schemaDoc.RootElement.GetProperty("fields").EnumerateArray())
        {
            var fieldMeta = field.GetProperty("metadata");
            Assert.True(fieldMeta.TryGetProperty("delta.columnMapping.id", out var id));
            Assert.True(id.GetInt32() > 0);
            Assert.False(string.IsNullOrEmpty(
                fieldMeta.GetProperty("delta.columnMapping.physicalName").GetString()));
        }

        // And delta-rs must decline it for the reason we expect -- if this ever starts succeeding,
        // the read-back assertions above can move out of tier 3.
        var rejected = DeltaRs.InvokeRaw("read", new { path = _tempDir });
        Assert.False(rejected.GetProperty("ok").GetBoolean());
        Assert.Contains("minimum reader version", rejected.GetProperty("error").GetString()!);
    }
}

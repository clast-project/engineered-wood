// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Log;
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
    /// <para>The BUFFERED-TRANSACTION flagship (slice 9 buffered seam): a schema ALTER (ADD COLUMN), an
    /// eagerly-written INSERT under the pending schema, and a deletion-vector DELETE are fused into ONE atomic
    /// Delta version via <c>ComputeAddColumn</c> + <c>WriteDataFilesAsync</c> +
    /// <c>ComputeDeletionVectorActionsAsync</c> + <c>CommitDataFilesAsync(extraActions:)</c>. The reference
    /// implementation must read the single commit correctly: the new column NULL-backfilled on the old files,
    /// valued on the new file, and the DV-deleted row gone.</para>
    ///
    /// <para>This is the cross-engine claim the seam rests on — that a fused metaData + add + DV-remove commit
    /// is a spec-legal ordinary version, not something only EW can interpret. A round-trip through EW alone
    /// could not catch a mis-shaped commit (a stray second metaData, an add referencing the pending schema
    /// Spark doesn't know, a DV a foreign reader ignores). Spark 4.0 supports deletion vectors, so the deleted
    /// row being absent here proves the fused DV lands for a foreign reader too.</para>
    /// </summary>
    [Fact]
    public async Task EwFusedAlterInsertDelete_SparkReadsOneAtomicVersion()
    {
        if (!Spark.EnsureAvailable()) return;

        var baseSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), baseSchema, enableDeletionVectors: true);
        await table.WriteAsync([new RecordBatch(baseSchema,
        [
            new Int64Array.Builder().Append(1).Append(2).Append(3).Append(4).Append(5).Build(),
            new StringArray.Builder().Append("v1").Append("v2").Append("v3").Append("v4").Append("v5").Build(),
        ], 5)]);
        var pinned = table.CurrentSnapshot; // v1: ids 1..5, one file

        // ALTER ADD COLUMN extra INT — computed, not committed
        var change = table.ComputeAddColumn(new Field("extra", Int32Type.Default, true));

        // INSERT (6, 7) under the pending schema — file written now, add deferred
        var widened = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Field(new Field("extra", Int32Type.Default, true))
            .Build();
        var files = await table.WriteDataFilesAsync([new RecordBatch(widened,
        [
            new Int64Array.Builder().Append(6).Append(7).Build(),
            new StringArray.Builder().Append("v6").Append("v7").Build(),
            new Int32Array.Builder().Append(60).Append(70).Build(),
        ], 2)], schemaOverride: change.NewSchema);

        // DELETE WHERE id = 2 — position 1 of ordinal 0 in the pinned snapshot
        var (dvActions, _) = await table.ComputeDeletionVectorActionsAsync(
            new Dictionary<int, IReadOnlyCollection<long>> { [0] = new long[] { 1 } }, resolveAgainst: pinned);

        var extra = new List<DeltaLake.Actions.DeltaAction>();
        extra.AddRange(change.Actions);
        extra.AddRange(dvActions);
        long committed = await table.CommitDataFilesAsync(files, DeltaWriteMode.Append,
            extraActions: extra, expectedVersion: pinned.Version, operation: "TRANSACTION");
        Assert.Equal(pinned.Version + 1, committed); // ONE version past the pin

        // Spark reads the fused commit: id 2 gone, extra NULL-backfilled on old rows, valued on new rows.
        var result = Spark.Invoke("read", new { path = _tempDir });
        Assert.Equal(6, result.GetProperty("row_count").GetInt32());
        var seen = new Dictionary<long, int?>();
        foreach (var row in result.GetProperty("rows").EnumerateArray())
        {
            long id = row.GetProperty("id").GetInt64();
            var extraProp = row.GetProperty("extra");
            seen[id] = extraProp.ValueKind == JsonValueKind.Null ? null : extraProp.GetInt32();
        }
        Assert.Equal(new long[] { 1, 3, 4, 5, 6, 7 }, seen.Keys.OrderBy(k => k).ToArray());
        Assert.Null(seen[1]);
        Assert.Null(seen[5]);
        Assert.Equal(60, seen[6]);
        Assert.Equal(70, seen[7]);
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

    /// <summary>
    /// <para>Milestone 2: a copy-on-write UPDATE on a row-tracking table must PRESERVE each surviving row's
    /// stable id by materializing it into the declared hidden column. EW appends ids 0,1,2, then updates the
    /// middle row (region eu → EU) — which rewrites the whole file and REORDERS rows (the matched row moves to
    /// physical position 0). Spark reads each row's <c>_metadata.row_id</c>.</para>
    ///
    /// <para>This is the measurement that distinguishes correct materialization from the fallback: if EW wrote
    /// the materialized column under the wrong physical name (or not at all), Spark would fall back to the new
    /// file's <c>baseRowId + position</c> and read a DIFFERENT, reordered id set. Reading the ORIGINAL ids back
    /// (id 10→0, 20→1, 30→2) proves the materialized column is present, correctly named, and correctly valued.</para>
    /// </summary>
    [Fact]
    public async Task EwUpdated_RowTracking_SparkReadsPreservedIds()
    {
        if (!Spark.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(fs, IdRegionSchema, enableRowTracking: true))
        {
            await table.WriteAsync([IdRegionBatch([10, 20, 30], ["us", "eu", "apac"])]); // ids 0,1,2

            // UPDATE the eu row (id 20) → region "EU": a copy-on-write rewrite of the single file.
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
        }

        var result = Spark.Invoke("read_row_ids", new { path = _tempDir });
        var idToRowId = result.GetProperty("rows").EnumerateArray()
            .ToDictionary(r => r.GetProperty("id").GetInt64(), r => r.GetProperty("row_id").GetInt64());

        Assert.Equal(0L, idToRowId[10]);
        Assert.Equal(1L, idToRowId[20]); // the UPDATED row keeps its original id through the rewrite
        Assert.Equal(2L, idToRowId[30]);
    }

    /// <summary>
    /// The copy-on-write DELETE-by-row-id (gap 2): a host reads rows with their transient rowids and deletes one
    /// by rewriting its file WITHOUT deletion vectors — the maximally reader-compatible path. On a row-tracking
    /// table the rewrite must still PRESERVE the survivors' stable ids (materialized column). EW writes ids 0,1,2,
    /// deletes the middle row (id 20) by its rowid, and Spark reads the survivors' <c>_metadata.row_id</c>.
    ///
    /// <para>Measured cross-engine: the result is a plain remove+add (no DV artifact), so Spark reads exactly the
    /// two survivors, and their ORIGINAL ids (0 and 2) come back — proving the rewrite carried the materialized
    /// id column even on the position-addressed delete path, not just the predicate UPDATE.</para>
    /// </summary>
    [Fact]
    public async Task EwDeletedByRowId_CopyOnWrite_RowTracking_SparkReadsSurvivorsWithPreservedIds()
    {
        if (!Spark.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(fs, IdRegionSchema, enableRowTracking: true))
        {
            await table.WriteAsync([IdRegionBatch([10, 20, 30], ["us", "eu", "apac"])]); // ids 0,1,2

            // the transient rowid of the id-20 row, then a copy-on-write delete of exactly it
            long rid20 = -1;
            await foreach (var b in table.ReadAllWithRowIdsAsync(null, null))
            {
                var id = (Int64Array)b.Column("id");
                var rowId = (Int64Array)b.Column("_metadata.row_id");
                for (int i = 0; i < b.Length; i++)
                    if (id.GetValue(i) == 20) rid20 = rowId.GetValue(i)!.Value;
            }
            Assert.True(rid20 >= 0);
            var (deleted, _) = await table.DeleteByRowIdsAsync([rid20]);
            Assert.Equal(1, deleted);
        }

        var result = Spark.Invoke("read_row_ids", new { path = _tempDir });
        var idToRowId = result.GetProperty("rows").EnumerateArray()
            .ToDictionary(r => r.GetProperty("id").GetInt64(), r => r.GetProperty("row_id").GetInt64());

        Assert.Equal([10L, 30L], idToRowId.Keys.OrderBy(k => k).ToArray()); // id 20 gone
        Assert.Equal(0L, idToRowId[10]); // survivors keep their ORIGINAL ids through the copy-on-write rewrite
        Assert.Equal(2L, idToRowId[30]);

        // No deletion vector was written — the point of the copy-on-write path.
        await using var check = await DeltaTable.OpenAsync(fs);
        Assert.DoesNotContain(check.CurrentSnapshot.ActiveFiles.Values, a => a.DeletionVector is not null);
    }

    /// <summary>
    /// Milestone 2: compaction (OPTIMIZE) must preserve each row's stable id. EW writes three single-row files
    /// (ids 0,1,2), compacts them into one, and Spark reads each row's <c>_metadata.row_id</c>. Because rows
    /// from several source files mix into one compacted file, a single <c>baseRowId</c> cannot represent them —
    /// the compacted file MUST carry the materialized id column. Reading 10→0, 20→1, 30→2 proves it does.
    /// </summary>
    [Fact]
    public async Task EwCompacted_RowTracking_SparkReadsPreservedIds()
    {
        if (!Spark.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(fs, IdRegionSchema, enableRowTracking: true))
        {
            await table.WriteAsync([IdRegionBatch([10], ["us"])]);   // id 0
            await table.WriteAsync([IdRegionBatch([20], ["eu"])]);   // id 1
            await table.WriteAsync([IdRegionBatch([30], ["apac"])]); // id 2
            await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });
        }

        var result = Spark.Invoke("read_row_ids", new { path = _tempDir });
        var idToRowId = result.GetProperty("rows").EnumerateArray()
            .ToDictionary(r => r.GetProperty("id").GetInt64(), r => r.GetProperty("row_id").GetInt64());

        Assert.Equal(0L, idToRowId[10]);
        Assert.Equal(1L, idToRowId[20]);
        Assert.Equal(2L, idToRowId[30]);
    }

    /// <summary>
    /// <para>Layer 3 sub-problem (B), cross-engine: a stale EW DELETE remapped across a concurrent EW
    /// COMPACTION. One handle compacts two files into one (materialized ids preserved); a second, stale handle
    /// then deletes a row whose original file is gone — EW relocates it by STABLE ROW ID and soft-deletes it
    /// with a deletion vector on the compacted file. Spark 4.0 (DV- and row-tracking-capable) must return the
    /// surviving rows with their ORIGINAL ids and omit exactly the remapped-away row.</para>
    ///
    /// <para>This is the measurement the remap's on-disk result demands: the artifact is a deletion vector on a
    /// materialized-id file, and only a conformant reader proves the vector masks the row the remap chose by
    /// stable id (not a position that shifted under the compaction) while every other row keeps its id. A
    /// round-trip through EW alone would agree with its own convention; Spark computing 10→0, 30→2, 40→3, 50→4
    /// with id 20 absent is the actual cross-engine confirmation.</para>
    /// </summary>
    [Fact]
    public async Task EwDeleteRemappedThroughCompaction_SparkReadsSurvivorsWithPreservedIds()
    {
        if (!Spark.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using (var setup = await DeltaTable.CreateAsync(
            fs, IdRegionSchema, enableDeletionVectors: true, enableRowTracking: true))
        {
            await setup.WriteAsync([IdRegionBatch([10, 20, 30], ["us", "eu", "apac"])]); // file 0: ids 0,1,2
            await setup.WriteAsync([IdRegionBatch([40, 50], ["us", "eu"])]);             // file 1: ids 3,4
        }

        await using var tableA = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await using var tableB = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

        // A compacts both files into one; B (stale) deletes id 20, whose source file is now gone — the row is
        // remapped by stable id onto the compacted file.
        await tableA.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });
        var (deleted, _) = await tableB.DeleteAsync(DeleteId(20));
        Assert.Equal(1, deleted);

        var result = Spark.Invoke("read_row_ids", new { path = _tempDir });
        var idToRowId = result.GetProperty("rows").EnumerateArray()
            .ToDictionary(r => r.GetProperty("id").GetInt64(), r => r.GetProperty("row_id").GetInt64());

        Assert.False(idToRowId.ContainsKey(20)); // the remapped delete removed exactly id 20
        Assert.Equal(new long[] { 10, 30, 40, 50 }, idToRowId.Keys.OrderBy(k => k).ToArray());
        Assert.Equal(0L, idToRowId[10]); // every survivor keeps its original id through the compaction
        Assert.Equal(2L, idToRowId[30]);
        Assert.Equal(3L, idToRowId[40]);
        Assert.Equal(4L, idToRowId[50]);
    }

    /// <summary>
    /// A row-tracking append that REBASES past a concurrent append must still assign spec-correct row ids: the
    /// loser's baseRowId is re-derived from the advanced high-water mark, so Spark reads contiguous, unique ids
    /// across both files (0,1 from the winner, 2 from the rebased loser). This is the cross-engine proof for
    /// "limitation 2"'s append half — a wrong re-derivation would surface as a DUPLICATE row id (the loser
    /// reusing the winner's space) or a gap, which a round-trip through EW alone could not detect.
    /// </summary>
    [Fact]
    public async Task EwConcurrentAppends_RowTracking_Rebased_SparkReadsContiguousIds()
    {
        if (!Spark.EnsureAvailable()) return;

        var fs = new LocalTableFileSystem(_tempDir);
        await using (var setup = await DeltaTable.CreateAsync(fs, IdRegionSchema, enableRowTracking: true))
        {
            // create only — the two appends below race from the same base version
        }

        await using var tableA = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await using var tableB = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

        long vA = await tableA.WriteAsync([IdRegionBatch([10, 20], ["us", "eu"])]); // ids 0, 1
        long vB = await tableB.WriteAsync([IdRegionBatch([30], ["apac"])]);          // stale -> rebases -> id 2
        Assert.True(vB > vA);

        var result = Spark.Invoke("read_row_ids", new { path = _tempDir });
        var rowIds = result.GetProperty("row_ids").EnumerateArray()
            .Select(e => e.GetInt64()).OrderBy(x => x).ToList();
        Assert.Equal([0L, 1L, 2L], rowIds); // contiguous + unique -> the rebased file's baseRowId was re-derived
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

    // ── Change Data Feed on a column-mapping, partitioned table ──

    private static Apache.Arrow.Schema IdValueRegionSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Field(new Field("region", StringType.Default, true))
        .Build();

    private static RecordBatch IdValueRegionBatch(long[] ids, string[] values, string[] regions)
    {
        var idArray = new Int64Array.Builder().AppendRange(ids).Build();
        var valueBuilder = new StringArray.Builder();
        foreach (string v in values) valueBuilder.Append(v);
        var regionBuilder = new StringArray.Builder();
        foreach (string r in regions) regionBuilder.Append(r);
        return new RecordBatch(IdValueRegionSchema, [idArray, valueBuilder.Build(), regionBuilder.Build()], ids.Length);
    }

    // Creates a Name-mode column-mapping table partitioned by region with CDF enabled (CreateAsync has no CDF
    // switch, so the property is patched into the generated, correctly-mapped metadata as a follow-up commit).
    private async Task<DeltaTable> CreateMappedPartitionedCdfTableAsync()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        long next;
        MetadataAction meta;
        await using (var created = await DeltaTable.CreateAsync(
            fs, IdValueRegionSchema, partitionColumns: ["region"], columnMappingMode: ColumnMappingMode.Name))
        {
            meta = created.CurrentSnapshot.Metadata;
            next = created.CurrentSnapshot.Version + 1;
        }
        var cfg = meta.Configuration!.ToDictionary(kv => kv.Key, kv => kv.Value);
        cfg[CdfConfig.EnableKey] = "true";
        await new TransactionLog(fs).WriteCommitAsync(
            next, new List<DeltaAction> { meta with { Configuration = cfg } });
        return await DeltaTable.OpenAsync(fs);
    }

    /// <summary>
    /// <para>The cross-engine proof for the CDF write layout: EW writes a Change Data Feed on a
    /// COLUMN-MAPPING, PARTITIONED table (INSERT inferred from the AddFile, then an UPDATE that writes
    /// <c>_change_data</c> files), and Spark reads it back via <c>readChangeFeed</c>.</para>
    ///
    /// <para>The <c>_change_data</c> files (and the data files the insert is inferred from) are stored in the
    /// PHYSICAL layout — physical <c>col-&lt;guid&gt;</c> names + parquet field ids, partition column absent.
    /// Spark resolves them through the table's column mapping and re-materializes the partition column, so it
    /// reports the LOGICAL names (<c>id</c>/<c>value</c>/<c>region</c>) with the right partition value and change
    /// types. If EW wrote the feed with logical names (the old behavior), Spark would fail to resolve the
    /// physical layout or surface the wrong columns — an EW-only round-trip could never catch that.</para>
    /// </summary>
    [Fact]
    public async Task EwWritten_ChangeDataFeed_MappedPartitioned_SparkReadsLogicalNamesAndPartition()
    {
        if (!Spark.EnsureAvailable()) return;

        long insertVersion, updateVersion;
        await using (var table = await CreateMappedPartitionedCdfTableAsync())
        {
            insertVersion = table.CurrentSnapshot.Version + 1;
            await table.WriteAsync([IdValueRegionBatch([1, 2], ["a", "b"], ["emea", "emea"])]);
            updateVersion = table.CurrentSnapshot.Version + 1;

            // UPDATE id=1's value → "z": a copy-on-write rewrite that emits update_preimage/postimage cdc files.
            await table.UpdateAsync(
                b =>
                {
                    var id = (Int64Array)b.Column("id");
                    var mask = new BooleanArray.Builder();
                    for (int i = 0; i < b.Length; i++) mask.Append(id.GetValue(i) == 1);
                    return mask.Build();
                },
                b =>
                {
                    int vIdx = b.Schema.GetFieldIndex("value");
                    var cols = new IArrowArray[b.ColumnCount];
                    for (int i = 0; i < b.ColumnCount; i++)
                        cols[i] = i == vIdx ? new StringArray.Builder().Append("z").Build() : b.Column(i);
                    return new RecordBatch(b.Schema, cols, b.Length);
                });
        }

        var result = Spark.Invoke("read_changes",
            new { path = _tempDir, start = insertVersion, end = updateVersion });

        // Spark resolved the physical layout to the table's LOGICAL columns + the partition column.
        var columns = result.GetProperty("columns").EnumerateArray().Select(e => e.GetString()!).ToList();
        Assert.Contains("id", columns);
        Assert.Contains("value", columns);
        Assert.Contains("region", columns);
        Assert.Contains("_change_type", columns);
        Assert.DoesNotContain(columns, c => c!.StartsWith("col-", StringComparison.Ordinal));

        // Gather (change_type, id, value, region) tuples.
        var rows = result.GetProperty("rows").EnumerateArray()
            .Select(r => (
                Ct: r.GetProperty("_change_type").GetString(),
                Id: r.GetProperty("id").GetInt64(),
                Value: r.GetProperty("value").GetString(),
                Region: r.GetProperty("region").GetString()))
            .ToList();

        // Every row carries the re-materialized partition value.
        Assert.All(rows, t => Assert.Equal("emea", t.Region));
        // The two inserts, the pre-image (old value "a") and the post-image (new value "z") for id=1.
        Assert.Contains(rows, t => t.Ct == "insert" && t.Id == 2 && t.Value == "b");
        Assert.Contains(rows, t => t.Ct == "update_preimage" && t.Id == 1 && t.Value == "a");
        Assert.Contains(rows, t => t.Ct == "update_postimage" && t.Id == 1 && t.Value == "z");
    }
}

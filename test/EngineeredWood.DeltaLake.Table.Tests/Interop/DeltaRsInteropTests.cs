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

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

using EngineeredWood.Iceberg.Manifest;
using EngineeredWood.Iceberg.Serialization;

namespace EngineeredWood.Iceberg.Tests.Interop;

/// <summary>
/// <para>iceberg-go writes each driver scenario. These tests check that iceberg-go reads its own scenario
/// back as expected (the oracle is healthy), then that EW reads the same table's metadata and manifests
/// the way iceberg-go does.</para>
///
/// <para>EW cannot read another engine's manifests yet: the codec refuses any Avro writer schema but its
/// own (#775) until the rewrite in #586. The manifest comparisons are written anyway and run inside
/// <see cref="PendingIssue586"/>, which passes while EW refuses and FAILS once it stops refusing, so the
/// #586 change has to switch them on.</para>
/// </summary>
public sealed class IcebergGoInteropTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "ew_icego_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_warehouse, recursive: true); } catch { }
    }

    /// <summary>Each scenario with the ids that survive it (see the driver's scenario functions).</summary>
    public static TheoryData<string, long[]> Scenarios => new()
    {
        { "v3_dv", [3, 4, 5, 6, 8, 9] },
        { "v3_part", [0, 2, 3, 4, 6, 7] },
        // Seven rows. iceberg-go 0.7.0 returned eight: id 2, deleted by a v2 position delete before the
        // upgrade, came back once the first DV ignored it (apache/iceberg-go#2669).
        { "v2_upgrade", [0, 1, 4, 5, 100, 101, 102] },
        { "v3_cow", [0, 2, 3, 4] },
        { "v3_eqdel", [0, 1, 2, 4] },
    };

    public static TheoryData<string> ScenarioNames
    {
        get
        {
            var data = new TheoryData<string>();
            foreach (var row in Scenarios)
                data.Add((string)row[0]);
            return data;
        }
    }

    [SkippableTheory]
    [MemberData(nameof(Scenarios))]
    public void IcebergGo_ReadsItsOwnScenario(string scenario, long[] expectedIds)
    {
        IcebergGo.Require();
        string metadata = IcebergGo.Scenario(_warehouse, scenario);

        var rows = Scan(metadata, rowLineage: true);
        Assert.Equal(expectedIds, rows.Select(r => r["id"].GetInt64()).OrderBy(x => x));

        // Row lineage: every v3 row has a distinct _row_id, whether it was written before or after an
        // upgrade, and whether a copy-on-write delete rewrote its file.
        var rowIds = rows.Select(r => r["_row_id"]).ToList();
        Assert.All(rowIds, id => Assert.Equal(JsonValueKind.Number, id.ValueKind));
        Assert.Equal(rowIds.Count, rowIds.Select(id => id.GetInt64()).Distinct().Count());
        Assert.All(rows, r => Assert.Equal(JsonValueKind.Number, r["_last_updated_sequence_number"].ValueKind));

        // A copy-on-write delete keeps each surviving row's _row_id: row i was written with row id i.
        if (scenario == "v3_cow")
            Assert.All(rows, r => Assert.Equal(r["id"].GetInt64(), r["_row_id"].GetInt64()));

        // timestamptz_ns keeps the 123 ns every row was written with; the v2 table is micros.
        if (scenario != "v2_upgrade")
            Assert.All(rows, r => Assert.Equal(123, r["ts"].GetInt64() % 1000));

        // initial-default: rows written before the column existed read as 42.
        if (scenario == "v3_dv")
            Assert.All(rows, r => Assert.Equal(42, r["score"].GetInt32()));
    }

    [SkippableTheory]
    [MemberData(nameof(ScenarioNames))]
    public void Ew_ReadsIcebergGoMetadata(string scenario)
    {
        IcebergGo.Require();
        string metadataLocation = IcebergGo.Scenario(_warehouse, scenario);
        var go = Describe(metadataLocation);
        var fs = new FileUriFileSystem(_warehouse);

        var md = IcebergJsonSerializer.Deserialize<TableMetadata>(
            System.Text.Encoding.UTF8.GetString(fs.ReadAllBytesAsync(metadataLocation).AsTask().GetAwaiter().GetResult()));

        Assert.Equal(go.GetProperty("format_version").GetInt32(), md.FormatVersion);
        Assert.Equal(go.GetProperty("table_uuid").GetString(), md.TableUuid);
        Assert.Equal(go.GetProperty("location").GetString(), md.Location);
        Assert.Equal(go.GetProperty("last_sequence_number").GetInt64(), md.LastSequenceNumber);
        Assert.Equal(go.GetProperty("current_schema_id").GetInt32(), md.CurrentSchemaId);
        Assert.Equal(go.GetProperty("default_spec_id").GetInt32(), md.DefaultSpecId);
        Assert.Equal(go.GetProperty("last_column_id").GetInt32(), md.LastColumnId);
        Assert.Equal(go.GetProperty("current_snapshot_id").GetInt64(), md.CurrentSnapshotId);
        Assert.Equal(go.GetProperty("next_row_id").GetInt64(), md.NextRowId);

        var goSnapshots = go.GetProperty("snapshots").EnumerateArray().ToList();
        Assert.Equal(goSnapshots.Count, md.Snapshots.Count);
        foreach (var gs in goSnapshots)
        {
            var s = Assert.Single(md.Snapshots, x => x.SnapshotId == gs.GetProperty("snapshot_id").GetInt64());
            Assert.Equal(gs.GetProperty("sequence_number").GetInt64(), s.SequenceNumber);
            Assert.Equal(NullableInt64(gs.GetProperty("parent_id")), s.ParentSnapshotId);
            Assert.Equal(gs.GetProperty("manifest_list").GetString(), s.ManifestList);
            Assert.Equal(gs.GetProperty("timestamp_ms").GetInt64(), s.TimestampMs);
            Assert.Equal(NullableInt64(gs.GetProperty("schema_id")), s.SchemaId);
            Assert.Equal(NullableInt64(gs.GetProperty("first_row_id")), s.FirstRowId);
            Assert.Equal(NullableInt64(gs.GetProperty("added_rows")), s.AddedRows);
            Assert.Equal(gs.GetProperty("summary").GetProperty("operation").GetString(), s.Summary["operation"]);
        }
    }

    [SkippableTheory]
    [MemberData(nameof(ScenarioNames))]
    public async Task Ew_ReadsIcebergGoManifests(string scenario)
    {
        IcebergGo.Require();
        await PendingIssue586(() => CompareManifests(scenario));
    }

    private async Task CompareManifests(string scenario)
    {
        string metadataLocation = IcebergGo.Scenario(_warehouse, scenario);
        var go = Describe(metadataLocation);
        var fs = new FileUriFileSystem(_warehouse);
        var md = IcebergJsonSerializer.Deserialize<TableMetadata>(
            System.Text.Encoding.UTF8.GetString(await fs.ReadAllBytesAsync(metadataLocation)));
        var snapshot = md.Snapshots.Single(s => s.SnapshotId == md.CurrentSnapshotId);

        var list = await ManifestIO.ReadManifestListAsync(fs, snapshot.ManifestList);
        var goManifests = go.GetProperty("manifests").EnumerateArray().ToList();
        Assert.Equal(goManifests.Select(m => m.GetProperty("path").GetString()), list.Select(m => m.ManifestPath));

        foreach (var (gm, m) in goManifests.Zip(list, (a, b) => (a, b)))
        {
            Assert.Equal(gm.GetProperty("length").GetInt64(), m.ManifestLength);
            Assert.Equal(gm.GetProperty("content").GetString(), m.Content == ManifestContent.Data ? "DATA" : "DELETES");
            Assert.Equal(gm.GetProperty("partition_spec_id").GetInt32(), m.PartitionSpecId);
            Assert.Equal(gm.GetProperty("sequence_number").GetInt64(), m.SequenceNumber);
            Assert.Equal(gm.GetProperty("min_sequence_number").GetInt64(), m.MinSequenceNumber);
            Assert.Equal(gm.GetProperty("added_snapshot_id").GetInt64(), m.AddedSnapshotId);
            Assert.Equal(gm.GetProperty("added_files").GetInt32(), m.AddedDataFilesCount);
            Assert.Equal(gm.GetProperty("existing_files").GetInt32(), m.ExistingDataFilesCount);
            Assert.Equal(gm.GetProperty("deleted_files").GetInt32(), m.DeletedDataFilesCount);
            Assert.Equal(gm.GetProperty("added_rows").GetInt64(), m.AddedRowsCount);
            Assert.Equal(gm.GetProperty("existing_rows").GetInt64(), m.ExistingRowsCount);
            Assert.Equal(gm.GetProperty("deleted_rows").GetInt64(), m.DeletedRowsCount);
            Assert.Equal(NullableInt64(gm.GetProperty("first_row_id")), m.FirstRowId);

            var entries = await ManifestIO.ReadManifestAsync(fs, m.ManifestPath);
            var goEntries = gm.GetProperty("entries").EnumerateArray().ToList();
            Assert.Equal(goEntries.Select(e => e.GetProperty("file_path").GetString()), entries.Select(e => e.DataFile.FilePath));

            foreach (var (ge, e) in goEntries.Zip(entries, (a, b) => (a, b)))
            {
                string where = $"{scenario}: {e.DataFile.FilePath}";
                Assert.True(ge.GetProperty("status").GetString() == e.Status.ToString().ToUpperInvariant(), where);
                Assert.True(ge.GetProperty("content").GetString() == ContentName(e.DataFile.Content), where);
                Assert.True(ge.GetProperty("file_format").GetString() == e.DataFile.FileFormat.ToString().ToUpperInvariant(), where);
                Assert.True(ge.GetProperty("record_count").GetInt64() == e.DataFile.RecordCount, where);
                Assert.True(ge.GetProperty("file_size_in_bytes").GetInt64() == e.DataFile.FileSizeInBytes, where);
                Assert.True(NullableString(ge.GetProperty("referenced_data_file")) == e.DataFile.ReferencedDataFile, where);
                Assert.True(NullableInt64(ge.GetProperty("content_offset")) == e.DataFile.ContentOffset, where);
                Assert.True(NullableInt64(ge.GetProperty("content_size_in_bytes")) == e.DataFile.ContentSizeInBytes, where);

                // iceberg-go reports these after inheritance from the manifest; a null in the file means
                // "inherit", so EW's raw value must be either the same or absent.
                AssertInherited(ge.GetProperty("snapshot_id"), e.SnapshotId, where + " snapshot_id");
                AssertInherited(ge.GetProperty("data_sequence_number"), e.SequenceNumber, where + " sequence_number");
                AssertInherited(ge.GetProperty("file_sequence_number"), e.FileSequenceNumber, where + " file_sequence_number");
                AssertInherited(ge.GetProperty("first_row_id"), e.DataFile.FirstRowId, where + " first_row_id");
            }
        }
    }

    [SkippableTheory]
    [MemberData(nameof(ScenarioNames))]
    public async Task Ew_ListsTheLiveFilesIcebergGoPlans(string scenario)
    {
        IcebergGo.Require();
        await PendingIssue586(() => CompareLiveFiles(scenario));
    }

    private async Task CompareLiveFiles(string scenario)
    {
        string metadataLocation = IcebergGo.Scenario(_warehouse, scenario);
        var go = Describe(metadataLocation);
        var fs = new FileUriFileSystem(_warehouse);
        var md = IcebergJsonSerializer.Deserialize<TableMetadata>(
            System.Text.Encoding.UTF8.GetString(await fs.ReadAllBytesAsync(metadataLocation)));

        var (dataFiles, deleteFiles) = await new TableOperations(fs).ScanFilesAsync(md);

        var tasks = go.GetProperty("tasks").EnumerateArray().ToList();
        Assert.Equal(
            tasks.Select(t => t.GetProperty("file").GetString()!).OrderBy(p => p, StringComparer.Ordinal).ToList(),
            dataFiles.Select(f => f.FilePath).OrderBy(p => p, StringComparer.Ordinal).ToList());

        // Every delete file iceberg-go applies is live in EW's view. (EW may list more: iceberg-go drops a
        // delete file whose sequence number makes it irrelevant to every data file.)
        var applied = tasks
            .SelectMany(t => new[] { "position_deletes", "equality_deletes", "deletion_vectors" }
                .SelectMany(k => t.GetProperty(k).EnumerateArray().Select(p => p.GetString()!)))
            .Distinct()
            .ToList();
        Assert.All(applied, p => Assert.Contains(p, deleteFiles.Select(f => f.FilePath)));
    }

    /// <summary>The driver's write commands work on a table it adopts by metadata location, which is how
    /// EW-written tables will be handed to it.</summary>
    [SkippableFact]
    public void IcebergGo_AppendsAndDeletesOnAnAdoptedTable()
    {
        IcebergGo.Require();
        string metadata = IcebergGo.Scenario(_warehouse, "v3_dv");
        string adopter = Path.Combine(_warehouse, "adopter");

        string appended = IcebergGo.Invoke("append", new
        {
            warehouse = adopter,
            metadata_location = metadata,
            rows = new object[] { new { id = 50L, name = "x", ts = (long?)null, score = 7 } },
        }).GetProperty("metadata_location").GetString()!;
        string deleted = IcebergGo.Invoke("delete", new
        {
            warehouse = adopter,
            metadata_location = appended,
            column = "id",
            values = new[] { 4L, 50L },
        }).GetProperty("metadata_location").GetString()!;

        Assert.Equal([3L, 5, 6, 8, 9], Scan(deleted, rowLineage: false).Select(r => r["id"].GetInt64()).OrderBy(x => x));
        Assert.Equal([3L, 4, 5, 6, 8, 9, 50], Scan(appended, rowLineage: false).Select(r => r["id"].GetInt64()).OrderBy(x => x));
    }

    // --- helpers ---

    /// <summary>
    /// Runs a comparison that EW's manifest codec cannot pass until #586: it refuses manifests written by
    /// anyone else. Passes while that refusal is what stops the comparison, and fails both when anything
    /// else does and when the comparison runs to completion. In the last case #586 has landed: remove this
    /// wrapper so the comparison is asserted directly.
    /// </summary>
    private static async Task PendingIssue586(Func<Task> comparison)
    {
        try
        {
            await comparison();
        }
        catch (NotSupportedException e) when (e.Message.Contains("issues/586", StringComparison.Ordinal))
        {
            return;
        }

        Assert.Fail("EW read iceberg-go's manifests and they matched, so #586 has landed: remove PendingIssue586 "
            + "and assert the comparison directly.");
    }

    private static JsonElement Describe(string metadataLocation) =>
        IcebergGo.Invoke("describe", new { metadata_location = metadataLocation });

    private static List<Dictionary<string, JsonElement>> Scan(string metadataLocation, bool rowLineage)
    {
        var result = IcebergGo.Invoke("scan", new { metadata_location = metadataLocation, row_lineage = rowLineage });
        var names = result.GetProperty("columns").EnumerateArray().Select(c => c.GetProperty("name").GetString()!).ToList();
        return result.GetProperty("rows").EnumerateArray()
            .Select(row => names.Zip(row.EnumerateArray(), (n, v) => (n, v)).ToDictionary(x => x.n, x => x.v))
            .ToList();
    }

    private static void AssertInherited(JsonElement goValue, long? ewValue, string what)
    {
        if (ewValue is not null)
            Assert.True(NullableInt64(goValue) == ewValue, $"{what}: iceberg-go {goValue}, EW {ewValue}");
    }

    private static long? NullableInt64(JsonElement e) => e.ValueKind == JsonValueKind.Null ? null : e.GetInt64();

    private static string? NullableString(JsonElement e) => e.ValueKind == JsonValueKind.Null ? null : e.GetString();

    private static string ContentName(FileContent c) => c switch
    {
        FileContent.Data => "DATA",
        FileContent.PositionDeletes => "POSITION_DELETES",
        FileContent.EqualityDeletes => "EQUALITY_DELETES",
        _ => c.ToString(),
    };
}

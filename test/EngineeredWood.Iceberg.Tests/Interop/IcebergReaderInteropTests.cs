// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using System.Text.Json;

using EngineeredWood.Iceberg.Manifest;
using EngineeredWood.Iceberg.Serialization;
using EngineeredWood.IO.Local;

namespace EngineeredWood.Iceberg.Tests.Interop;

/// <summary>
/// <para>The DuckDB and pyiceberg reader tiers (#953 item 6), alongside iceberg-go.</para>
///
/// <para>They cover two kinds of table. The first is iceberg-go's v3 scenarios, as iceberg-go wrote them and after
/// EW rewrites them. DuckDB checks row lineage there, which EW's rewrite must carry over. The second is v2 tables:
/// pyiceberg's, as pyiceberg wrote them and after EW rewrites them, and tables EW creates from scratch.</para>
///
/// <para>EW does not yet assign v3 row lineage when it appends, so it creates v2 tables only. "Every reader
/// agrees on an EW-appended v3 table, <c>_row_id</c> included" is the exit test of Phase 1, which adds the v3
/// writer.</para>
/// </summary>
public sealed class IcebergReaderInteropTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "ew_icereaders_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_warehouse, recursive: true); } catch { }
    }

    private static readonly string[] GoScenarios = ["v3_dv", "v3_part", "v2_upgrade", "v3_cow", "v3_eqdel"];

    private static readonly string[] PyIcebergScenarios = ["v2_cow", "v2_part", "v2_evolve"];

    /// <summary>The readers other than iceberg-go, which is the reference for iceberg-go's own tables.</summary>
    private static readonly string[] OtherReaders = ["duckdb", "pyiceberg"];

    private static readonly string[] AllReaders = ["iceberg-go", "duckdb", "pyiceberg"];

    public static TheoryData<string, string> OtherReadersOnGoScenarios => Cross(OtherReaders, GoScenarios);

    public static TheoryData<string, string> AllReadersOnPyIcebergScenarios => Cross(AllReaders, PyIcebergScenarios);

    public static TheoryData<string> PyIcebergScenarioNames => [.. PyIcebergScenarios];

    public static TheoryData<string> AllReaderNames => [.. AllReaders];

    /// <summary>The oracle is healthy: on every iceberg-go scenario, DuckDB and pyiceberg read the rows iceberg-go
    /// reads, and DuckDB the same <c>_row_id</c> and <c>_last_updated_sequence_number</c>.</summary>
    [SkippableTheory]
    [MemberData(nameof(OtherReadersOnGoScenarios))]
    public void Reader_ReadsIcebergGoScenario(string readerName, string scenario)
    {
        var reader = IcebergReader.Named(readerName);
        IcebergGo.Require();
        reader.Require();
        string metadata = IcebergGo.Scenario(_warehouse, scenario);

        if (RefusesEqualityDeletes(reader, scenario, metadata))
            return;
        AssertSameRows(IcebergReader.IcebergGoReader, reader, metadata, rowLineage: true);
    }

    /// <summary>
    /// EW removes one data file (with the deletion vector that names it) from an iceberg-go v3 table. DuckDB and
    /// pyiceberg then read what iceberg-go reads of EW's snapshot. For DuckDB, that includes every surviving row's
    /// <c>_row_id</c>, which only holds if EW's manifests kept each file's <c>first_row_id</c>.
    /// </summary>
    [SkippableTheory]
    [MemberData(nameof(OtherReadersOnGoScenarios))]
    public async Task Reader_ReadsEwsRewriteOfAnIcebergGoTable(string readerName, string scenario)
    {
        var reader = IcebergReader.Named(readerName);
        IcebergGo.Require();
        reader.Require();
        string metadata = IcebergGo.Scenario(_warehouse, scenario);
        var (rewritten, _) = await RemoveLastDataFileAsync(metadata);

        // No equality-delete refusal here: v3_eqdel has one data file, so EW's rewrite leaves pyiceberg no file
        // to apply the equality delete to, and it reads the (empty) table.
        AssertSameRows(IcebergReader.IcebergGoReader, reader, rewritten, rowLineage: true);
    }

    /// <summary>EW reads pyiceberg's v2 metadata as pyiceberg does, and plans the files pyiceberg plans.</summary>
    [SkippableTheory]
    [MemberData(nameof(PyIcebergScenarioNames))]
    public async Task Ew_ReadsPyIcebergV2Table(string scenario)
    {
        PyIceberg.Require();
        string metadataLocation = PyIceberg.Scenario(Path.Combine(_warehouse, "py"), scenario);
        var py = PyIceberg.Describe(metadataLocation);
        var fs = FileSystem();
        var md = await ReadMetadataAsync(fs, metadataLocation);

        Assert.Equal(2, md.FormatVersion);
        Assert.Equal(py.GetProperty("format_version").GetInt32(), md.FormatVersion);
        Assert.Equal(py.GetProperty("table_uuid").GetString(), md.TableUuid);
        Assert.Equal(py.GetProperty("location").GetString(), md.Location);
        Assert.Equal(py.GetProperty("last_sequence_number").GetInt64(), md.LastSequenceNumber);
        Assert.Equal(py.GetProperty("current_snapshot_id").GetInt64(), md.CurrentSnapshotId);
        Assert.Equal(py.GetProperty("current_schema_id").GetInt32(), md.CurrentSchemaId);
        Assert.Equal(py.GetProperty("default_spec_id").GetInt32(), md.DefaultSpecId);
        Assert.Equal(py.GetProperty("last_column_id").GetInt32(), md.LastColumnId);

        var pySnapshots = py.GetProperty("snapshots").EnumerateArray().ToList();
        Assert.Equal(pySnapshots.Count, md.Snapshots.Count);
        foreach (var ps in pySnapshots)
        {
            var s = Assert.Single(md.Snapshots, x => x.SnapshotId == ps.GetProperty("snapshot_id").GetInt64());
            Assert.Equal(ps.GetProperty("sequence_number").GetInt64(), s.SequenceNumber);
            Assert.Equal(NullableInt64(ps.GetProperty("parent_id")), s.ParentSnapshotId);
            Assert.Equal(ps.GetProperty("manifest_list").GetString(), s.ManifestList);
            Assert.Equal(ps.GetProperty("operation").GetString(), s.Summary["operation"]);
        }

        var (data, deletes) = await new TableOperations(fs.Resolver).ScanFilesAsync(md);
        var tasks = PyIceberg.Plan(metadataLocation).GetProperty("tasks").EnumerateArray().ToList();
        Assert.Equal(
            tasks.Select(t => t.GetProperty("file").GetString()!).OrderBy(p => p, StringComparer.Ordinal),
            data.Select(f => f.FilePath).OrderBy(p => p, StringComparer.Ordinal));
        foreach (var task in tasks)
        {
            var file = Assert.Single(data, f => f.FilePath == task.GetProperty("file").GetString());
            Assert.Equal(task.GetProperty("record_count").GetInt64(), file.RecordCount);
            Assert.Empty(task.GetProperty("deletes").EnumerateArray());
        }

        // pyiceberg deletes by rewriting files in v2 (copy-on-write), so it writes no delete files.
        Assert.Empty(deletes);
    }

    /// <summary>
    /// EW removes one data file from a pyiceberg v2 table. Every reader then reads the rows pyiceberg read before,
    /// less exactly the removed file's, from manifests EW wrote in the v2 layout.
    /// </summary>
    [SkippableTheory]
    [MemberData(nameof(AllReadersOnPyIcebergScenarios))]
    public async Task Reader_ReadsEwsRewriteOfAPyIcebergV2Table(string readerName, string scenario)
    {
        var reader = IcebergReader.Named(readerName);
        PyIceberg.Require();
        reader.Require();
        string metadata = PyIceberg.Scenario(Path.Combine(_warehouse, "py"), scenario);
        var before = IcebergReader.PyIcebergReader.Scan(metadata, rowLineage: false).Canonical();

        var (rewritten, victim) = await RemoveLastDataFileAsync(metadata);
        Assert.Equal(2, (await ReadMetadataAsync(FileSystem(), rewritten)).FormatVersion);

        var after = reader.Scan(rewritten, rowLineage: false).Canonical();
        Assert.All(after, row => Assert.Contains(row, before));
        Assert.Equal(before.Count - victim.RecordCount, after.Count);
    }

    /// <summary>
    /// A v2 table EW creates in its own catalog, over the data files of pyiceberg's <c>v2_cow</c> table, one append
    /// per file: every reader reads the rows pyiceberg reads from the original.
    /// </summary>
    [SkippableTheory]
    [MemberData(nameof(AllReaderNames))]
    public async Task Reader_ReadsAnEwCreatedV2Table(string readerName)
    {
        var reader = IcebergReader.Named(readerName);
        PyIceberg.Require();
        reader.Require();
        string source = PyIceberg.Scenario(Path.Combine(_warehouse, "py"), "v2_cow");
        var fs = FileSystem();
        var sourceMetadata = await ReadMetadataAsync(fs, source);
        var (files, _) = await new TableOperations(fs.Resolver).ScanFilesAsync(sourceMetadata);

        string ewWarehouse = Path.Combine(_warehouse, "ew");
        Directory.CreateDirectory(ewWarehouse);
        var catalog = new FileSystemCatalog(new LocalTableFileSystem(ewWarehouse));
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        var table = await catalog.CreateTableAsync(
            new TableIdentifier(Namespace.Of("db"), "t"),
            sourceMetadata.Schemas.Single(s => s.SchemaId == sourceMetadata.CurrentSchemaId));
        var ops = new TableOperations(fs.Resolver);
        foreach (var file in files.OrderBy(f => f.FilePath, StringComparer.Ordinal))
            table = await catalog.CommitTableAsync(table, await ops.AppendFilesAsync(table.Metadata, [file]));

        Assert.Equal(2, table.Metadata.FormatVersion);
        Assert.Equal(files.Count, table.Metadata.Snapshots.Count);
        Assert.Equal(
            IcebergReader.PyIcebergReader.Scan(source, rowLineage: false).Canonical(),
            reader.Scan(table.MetadataLocation!, rowLineage: false).Canonical());
    }

    // --- helpers ---

    private static TheoryData<string, string> Cross(string[] readers, string[] scenarios)
    {
        var data = new TheoryData<string, string>();
        foreach (string reader in readers)
            foreach (string scenario in scenarios)
                data.Add(reader, scenario);
        return data;
    }

    private LocationFileSystem FileSystem()
    {
        Directory.CreateDirectory(_warehouse);
        return new LocationFileSystem(TestResolvers.For(new LocalTableFileSystem(_warehouse)));
    }

    private static async Task<TableMetadata> ReadMetadataAsync(LocationFileSystem fs, string metadataLocation) =>
        IcebergJsonSerializer.Deserialize<TableMetadata>(Encoding.UTF8.GetString(await fs.ReadAllBytesAsync(metadataLocation)));

    /// <summary>EW removes the last data file in path order, with the delete files that name it (its DV), and
    /// writes the new metadata file next to the table's others.</summary>
    private async Task<(string MetadataLocation, DataFile Victim)> RemoveLastDataFileAsync(string metadataLocation)
    {
        var fs = FileSystem();
        var md = await ReadMetadataAsync(fs, metadataLocation);
        var ops = new TableOperations(fs.Resolver);
        var (data, deletes) = await ops.ScanFilesAsync(md);
        var victim = data.OrderBy(f => f.FilePath, StringComparer.Ordinal).Last();
        var victimDeletes = deletes.Where(d => d.ReferencedDataFile == victim.FilePath).ToList();

        var updated = await ops.OverwriteFilesAsync(md, [victim, .. victimDeletes], []);
        string location = $"{md.Location}/metadata/ew-{Guid.NewGuid():N}.metadata.json";
        await fs.WriteAllBytesAsync(location, Encoding.UTF8.GetBytes(IcebergJsonSerializer.Serialize(updated)));
        return (location, victim);
    }

    /// <summary>Asserts the two readers report the same columns and the same rows.</summary>
    private static void AssertSameRows(IcebergReader reference, IcebergReader reader, string metadataLocation, bool rowLineage)
    {
        bool lineage = rowLineage && reader.ReadsRowLineage;
        var expected = reference.Scan(metadataLocation, lineage);
        var actual = reader.Scan(metadataLocation, lineage);

        Assert.Equal(expected.Columns.OrderBy(c => c, StringComparer.Ordinal), actual.Columns.OrderBy(c => c, StringComparer.Ordinal));
        if (lineage)
            Assert.Contains("_row_id", actual.Columns);
        Assert.Equal(expected.Canonical(), actual.Canonical());
    }

    /// <summary>
    /// MEASURED on pyiceberg 0.12.0: a scan of a table with equality deletes is refused. Pinned here, so that
    /// pyiceberg reading one reads as "the oracle moved" and the case joins the comparison.
    /// </summary>
    private static bool RefusesEqualityDeletes(IcebergReader reader, string scenario, string metadataLocation)
    {
        if (reader != IcebergReader.PyIcebergReader || scenario != "v3_eqdel")
            return false;

        var result = PyIceberg.ScanRaw(metadataLocation);
        Assert.False(result.GetProperty("ok").GetBoolean(), "pyiceberg now reads equality deletes; compare its rows");
        Assert.Contains("equality deletes", result.GetProperty("error").GetString(), StringComparison.OrdinalIgnoreCase);
        return true;
    }

    private static long? NullableInt64(JsonElement e) => e.ValueKind == JsonValueKind.Null ? null : e.GetInt64();
}

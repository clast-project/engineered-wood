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
/// External validation for <see cref="DeltaTable.CreateOrReplaceAsync"/>, which publishes a commit shape
/// nothing else in this library produces: protocol, metaData, the removes retiring every previously
/// active file, and the replacement's adds, all in ONE version, on top of history that stays readable.
///
/// <para>Round-tripping through EW cannot validate this. EW resolves column mapping from the metadata's
/// mode, so a replacement protocol that fails to DECLARE the columnMapping feature reads back perfectly
/// here and is rejected outright by Spark — the exact regression
/// <c>CreateOrReplace_MergedProtocolEnumeratesLegacyFeaturesFromBothSides</c> pins in unit form. Only a
/// conformant foreign reader can tell the two apart, which is what these tests are for.</para>
///
/// <para>Tier 1 (delta-rs) covers everything it structurally can, because it runs in seconds; tier 3
/// (Spark) covers the protocol-consistency check delta-rs does not perform.</para>
/// </summary>
[Collection("Interop")]
public class CreateOrReplaceInteropTests : IDisposable
{
    private readonly string _tempDir;

    public CreateOrReplaceInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cor_{Guid.NewGuid():N}");
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

    private static List<(long Id, string Region)> RowsFromJson(JsonElement result)
    {
        var rows = new List<(long, string)>();
        foreach (var row in result.GetProperty("rows").EnumerateArray())
            rows.Add((row.GetProperty("id").GetInt64(), row.GetProperty("region").GetString()!));

        rows.Sort();
        return rows;
    }

    // ── Tier 1: delta-rs ──

    /// <summary>
    /// The replacement commit as a conformant reader resolves it: only the replacement's rows are live,
    /// and the version it landed on is the one EW reports. A replacement that emitted adds without the
    /// matching removes would show BOTH generations here — the table silently doubling instead of being
    /// replaced — which is invisible to a reader that trusts EW's own snapshot.
    /// </summary>
    [SkippableFact]
    public async Task EwCreateOrReplace_DeltaRsSeesOnlyReplacementRows()
    {
        DeltaRs.Require();

        var fs = new LocalTableFileSystem(_tempDir);
        await using (var original = await DeltaTable.CreateOrReplaceAsync(
            fs, IdRegionSchema, [IdRegionBatch([1, 2, 3], ["us", "eu", "us"])]))
        {
            Assert.Equal(0, original.CurrentSnapshot.Version);
        }

        await using (var replacement = await DeltaTable.CreateOrReplaceAsync(
            fs, IdRegionSchema, [IdRegionBatch([9], ["apac"])]))
        {
            Assert.Equal(1, replacement.CurrentSnapshot.Version);
        }

        var current = DeltaRs.Invoke("read", new { path = _tempDir });
        Assert.Equal(1, current.GetProperty("version").GetInt64());
        Assert.Equal([(9L, "apac")], RowsFromJson(current));

        // History survives the replacement: version 0 still reads as the original table.
        var historical = DeltaRs.Invoke("read", new { path = _tempDir, version = 0 });
        Assert.Equal([(1L, "us"), (2L, "eu"), (3L, "us")], RowsFromJson(historical));
    }

    /// <summary>
    /// The merged protocol, as delta-rs parses it. The replacement asks only for column mapping — which
    /// on the create path is the LEGACY (reader 2 / writer 5) pair with no feature lists — over a table
    /// already at reader 3 / writer 7 for deletion vectors. Merging the versions pulls the result to
    /// (3, 7), where nothing is implied any more, so columnMapping has to appear in both feature lists or
    /// the protocol contradicts its own metadata.
    /// </summary>
    [SkippableFact]
    public async Task EwCreateOrReplace_LegacyFeatureOverModernTable_DeltaRsSeesConsistentProtocol()
    {
        DeltaRs.Require();

        var fs = new LocalTableFileSystem(_tempDir);
        await using (var original = await DeltaTable.CreateOrReplaceAsync(
            fs, IdRegionSchema, [IdRegionBatch([1], ["us"])], enableDeletionVectors: true))
        {
        }

        await using (var replacement = await DeltaTable.CreateOrReplaceAsync(
            fs, IdRegionSchema, [IdRegionBatch([9], ["apac"])],
            columnMappingMode: ColumnMappingMode.Name))
        {
        }

        var described = DeltaRs.Invoke("describe", new { path = _tempDir });
        int minReader = described.GetProperty("min_reader_version").GetInt32();
        int minWriter = described.GetProperty("min_writer_version").GetInt32();
        var readerFeatures = described.GetProperty("reader_features")
            .EnumerateArray().Select(static f => f.GetString()!).ToList();
        var writerFeatures = described.GetProperty("writer_features")
            .EnumerateArray().Select(static f => f.GetString()!).ToList();
        var configuration = described.GetProperty("configuration");

        // Column mapping is on in the metadata...
        Assert.Equal(
            "name",
            configuration.GetProperty("delta.columnMapping.mode").GetString());

        // ...so at table-features level it must be declared in the protocol, on both sides.
        Assert.Equal(3, minReader);
        Assert.Equal(7, minWriter);
        Assert.Contains("columnMapping", readerFeatures);
        Assert.Contains("columnMapping", writerFeatures);

        // The capability the table already had is not silently dropped by the replacement.
        Assert.Contains("deletionVectors", readerFeatures);

        // No row assertion here: delta-rs REFUSES to read a table declaring these two reader features
        // ("not yet supported by the deltalake reader"), which is a limitation of that reader rather than
        // a defect in the commit — and, incidentally, direct evidence that the features really are
        // declared. Rows for this protocol shape are covered by the Spark test below.
    }

    // ── Tier 3: Spark ──

    /// <summary>
    /// Spark reading the replaced table. This is the check delta-rs does not perform: delta-spark
    /// validates that every capability the metadata enables is listed in the protocol and fails the read
    /// with DELTA_FEATURES_PROTOCOL_METADATA_MISMATCH when it is not. A column-mapping replacement whose
    /// merged protocol omitted the feature would be written happily by EW, read happily back by EW, and
    /// die here — which is exactly why this test exists rather than a round-trip assertion.
    /// </summary>
    [SkippableFact]
    public async Task EwCreateOrReplace_LegacyFeatureOverModernTable_SparkReadsIt()
    {
        Spark.Require();

        var fs = new LocalTableFileSystem(_tempDir);
        await using (var original = await DeltaTable.CreateOrReplaceAsync(
            fs, IdRegionSchema, [IdRegionBatch([1], ["us"])], enableDeletionVectors: true))
        {
        }

        await using (var replacement = await DeltaTable.CreateOrReplaceAsync(
            fs, IdRegionSchema, [IdRegionBatch([9], ["apac"])],
            columnMappingMode: ColumnMappingMode.Name))
        {
        }

        var result = Spark.Invoke("read", new { path = _tempDir });
        Assert.Equal([(9L, "apac")], RowsFromJson(result));
    }

    /// <summary>
    /// Spark WRITING to a table EW replaced — the strongest statement available that the replacement
    /// commit left the table in a state a foreign engine will build on, not merely one it will read.
    /// Column mapping makes this pointed: Spark has to resolve the replacement's physical names to append
    /// through them, so a replacement that reused the previous table's column ids (rather than continuing
    /// past its maxColumnId) surfaces here as a resolution failure or as rows landing in the wrong column.
    /// </summary>
    [SkippableFact]
    public async Task EwCreateOrReplace_SparkAppendsThroughReplacementColumnMapping()
    {
        Spark.Require();

        var fs = new LocalTableFileSystem(_tempDir);
        await using (var original = await DeltaTable.CreateOrReplaceAsync(
            fs, IdRegionSchema, [IdRegionBatch([1, 2], ["us", "eu"])],
            columnMappingMode: ColumnMappingMode.Name))
        {
        }

        await using (var replacement = await DeltaTable.CreateOrReplaceAsync(
            fs, IdRegionSchema, [IdRegionBatch([9], ["apac"])],
            columnMappingMode: ColumnMappingMode.Name))
        {
        }

        var result = Spark.Invoke("sql", new
        {
            path = _tempDir,
            sql = new[] { "INSERT INTO delta.`{path}` VALUES (10, 'latam')" },
        });

        Assert.Equal([(9L, "apac"), (10L, "latam")], RowsFromJson(result));

        // And EW agrees with Spark about the result of Spark's own append.
        await using var reopened = await DeltaTable.OpenAsync(fs);
        var ewRows = new List<(long, string)>();
        await foreach (var batch in reopened.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            var regions = (StringArray)batch.Column("region");
            for (int i = 0; i < batch.Length; i++)
                ewRows.Add((ids.GetValue(i)!.Value, regions.GetString(i)));
        }

        ewRows.Sort();
        Assert.Equal([(9L, "apac"), (10L, "latam")], ewRows);
    }
}

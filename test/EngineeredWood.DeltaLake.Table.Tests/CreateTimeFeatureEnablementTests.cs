// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using ArrowListType = Apache.Arrow.Types.ListType;
// `RowTracking` alone binds to EngineeredWood.DeltaLake.Table.RowTracking from inside this namespace.
using RowTrackingConfig = EngineeredWood.DeltaLake.RowTracking.RowTrackingConfig;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Create-time feature enablement: a <c>delta.enable*</c> / mode property handed to
/// <see cref="DeltaTable.CreateAsync"/> must both take EFFECT (the feature behaves as if its dedicated
/// argument had been passed) and be DECLARED in the commit-0 protocol. A property with no matching table
/// feature is what a strict reader rejects as DELTA_FEATURES_PROTOCOL_METADATA_MISMATCH, so each test here
/// reopens the table from disk and asserts on what was actually persisted — then exercises the feature.
/// </summary>
public class CreateTimeFeatureEnablementTests : IDisposable
{
    private readonly string _tempDir;

    public CreateTimeFeatureEnablementTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_create_feat_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema IdValueSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Build();

    private static RecordBatch IdValueBatch(Apache.Arrow.Schema schema, params (long Id, string Value)[] rows)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        foreach (var (id, value) in rows)
        {
            ids.Append(id);
            values.Append(value);
        }

        return new RecordBatch(schema, [ids.Build(), values.Build()], rows.Length);
    }

    private static BooleanArray MatchId(RecordBatch batch, long wanted)
    {
        var idCol = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
        var builder = new BooleanArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            builder.Append(idCol.GetValue(i)!.Value == wanted);
        return builder.Build();
    }

    private static async Task<List<(long Id, string Value)>> ReadAllRows(DeltaTable table)
    {
        var rows = new List<(long, string)>();
        await foreach (var b in table.ReadAllAsync())
        {
            var ids = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            var values = (StringArray)b.Column(b.Schema.GetFieldIndex("value"));
            for (int i = 0; i < b.Length; i++)
                rows.Add((ids.GetValue(i)!.Value, values.GetString(i)));
        }

        return rows.OrderBy(r => r.Item1).ToList();
    }

    // ---------------------------------------------------------------------------------------------
    // In-commit timestamps
    // ---------------------------------------------------------------------------------------------

    // Enabled AT CREATION, so no delta.inCommitTimestampEnablementVersion/…Timestamp pair is required:
    // every commit in the history carries the field, including commit 0.
    [Fact]
    public async Task InCommitTimestampsProperty_DeclaresFeatureAndStampsEveryCommit()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdValueSchema();

        await using (var created = await DeltaTable.CreateAsync(fs, schema,
            configuration: new Dictionary<string, string> { [InCommitTimestamp.EnableKey] = "true" }))
        {
            await created.WriteAsync([IdValueBatch(schema, (1, "a"), (2, "b"))]);
        }

        await using var table = await DeltaTable.OpenAsync(fs);
        var protocol = table.CurrentSnapshot.Protocol;

        Assert.Equal(7, protocol.MinWriterVersion);
        Assert.Contains("inCommitTimestamp", protocol.WriterFeatures!);
        // Writer-only: a reader needs nothing extra, so the reader version must NOT be escalated.
        Assert.Equal(1, protocol.MinReaderVersion);
        Assert.Null(protocol.ReaderFeatures);

        // The enablement pair is for a MID-LIFE enablement; a table born with the feature must not carry it.
        var config = table.CurrentSnapshot.Metadata.Configuration!;
        Assert.DoesNotContain("delta.inCommitTimestampEnablementVersion", config.Keys);
        Assert.DoesNotContain("delta.inCommitTimestampEnablementTimestamp", config.Keys);

        // Commit 0 (CREATE TABLE) and commit 1 (WRITE) both carry an in-commit timestamp.
        Assert.NotNull(table.CurrentSnapshot.InCommitTimestamp);
        Assert.True(table.CurrentSnapshot.InCommitTimestamp > 0);

        var v0 = await SnapshotAtVersion(fs, 0);
        Assert.NotNull(v0.InCommitTimestamp);
        Assert.True(v0.InCommitTimestamp <= table.CurrentSnapshot.InCommitTimestamp);
    }

    private static async ValueTask<Snapshot.Snapshot> SnapshotAtVersion(LocalTableFileSystem fs, long version)
    {
        var log = new TransactionLog(fs);
        return await Snapshot.SnapshotBuilder.BuildAsync(log, checkpointReader: null, atVersion: version);
    }

    // ---------------------------------------------------------------------------------------------
    // Change data feed
    // ---------------------------------------------------------------------------------------------

    // Before this, the ONLY way to get a CDF table was to hand-write commit 0 (see ChangeDataFeedTests):
    // CreateAsync recorded the property but never declared the feature.
    [Fact]
    public async Task ChangeDataFeedProperty_DeclaresFeatureAndFeedRoundTrips()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdValueSchema();

        await using (var created = await DeltaTable.CreateAsync(fs, schema,
            configuration: new Dictionary<string, string> { [CdfConfig.EnableKey] = "true" }))
        {
            await created.WriteAsync([IdValueBatch(schema, (1, "old1"), (2, "old2"))]);
            await created.UpdateAsync(
                predicate: b => MatchId(b, 1),
                updater: b =>
                {
                    var newValues = new StringArray.Builder();
                    for (int i = 0; i < b.Length; i++)
                        newValues.Append("NEW");
                    return new RecordBatch(b.Schema, [b.Column(0), newValues.Build()], b.Length);
                });
        }

        await using var table = await DeltaTable.OpenAsync(fs);
        var protocol = table.CurrentSnapshot.Protocol;

        Assert.Equal(7, protocol.MinWriterVersion);
        Assert.Contains("changeDataFeed", protocol.WriterFeatures!);
        // Writer-only: the change feed is an opt-in READ, so ordinary reads need no reader feature.
        Assert.Equal(1, protocol.MinReaderVersion);
        Assert.Null(protocol.ReaderFeatures);

        var changes = new List<(string ChangeType, long Id, string Value)>();
        await foreach (var b in table.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = 1, EndVersion = 2 }))
        {
            var ct = (StringArray)b.Column(b.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn));
            var ids = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            var values = (StringArray)b.Column(b.Schema.GetFieldIndex("value"));
            for (int i = 0; i < b.Length; i++)
                changes.Add((ct.GetString(i), ids.GetValue(i)!.Value, values.GetString(i)));
        }

        Assert.Contains(changes, c => c is { ChangeType: "insert", Id: 1, Value: "old1" });
        Assert.Contains(changes, c => c is { ChangeType: "insert", Id: 2, Value: "old2" });
        Assert.Contains(changes, c => c is { ChangeType: "update_preimage", Id: 1, Value: "old1" });
        Assert.Contains(changes, c => c is { ChangeType: "update_postimage", Id: 1, Value: "NEW" });
    }

    // ---------------------------------------------------------------------------------------------
    // Deletion vectors + row tracking (the two that already had boolean arguments)
    // ---------------------------------------------------------------------------------------------

    // The property and the argument are two spellings of one thing: same protocol, same behavior.
    [Fact]
    public async Task DeletionVectorsProperty_MatchesBooleanArgumentAndSoftDeletes()
    {
        var byPropertyDir = Path.Combine(_tempDir, "byProperty");
        var byArgumentDir = Path.Combine(_tempDir, "byArgument");
        Directory.CreateDirectory(byPropertyDir);
        Directory.CreateDirectory(byArgumentDir);

        var schema = IdValueSchema();
        var propertyFs = new LocalTableFileSystem(byPropertyDir);
        await using (var byProperty = await DeltaTable.CreateAsync(propertyFs, schema,
            configuration: new Dictionary<string, string> { [DeletionVectorConfig.EnableKey] = "true" }))
        {
            await byProperty.WriteAsync([IdValueBatch(schema, (1, "a"), (2, "b"), (3, "c"))]);
            await byProperty.DeleteAsync(b => MatchId(b, 2));
        }

        await using var byArgument = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(byArgumentDir), schema, enableDeletionVectors: true);

        await using var table = await DeltaTable.OpenAsync(propertyFs);
        var protocol = table.CurrentSnapshot.Protocol;

        Assert.Equal(3, protocol.MinReaderVersion);
        Assert.Equal(7, protocol.MinWriterVersion);
        Assert.Contains(DeletionVectorConfig.FeatureName, protocol.ReaderFeatures!);
        Assert.Contains(DeletionVectorConfig.FeatureName, protocol.WriterFeatures!);
        Assert.Equal(
            byArgument.CurrentSnapshot.Protocol.ReaderFeatures!.OrderBy(f => f, StringComparer.Ordinal),
            protocol.ReaderFeatures!.OrderBy(f => f, StringComparer.Ordinal));
        Assert.Equal(
            byArgument.CurrentSnapshot.Protocol.WriterFeatures!.OrderBy(f => f, StringComparer.Ordinal),
            protocol.WriterFeatures!.OrderBy(f => f, StringComparer.Ordinal));

        // The DELETE soft-deleted rather than rewriting: the file is still there, carrying a DV.
        var addFile = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        Assert.NotNull(addFile.DeletionVector);
        Assert.Equal(1, addFile.DeletionVector!.Cardinality);
        Assert.Equal([(1L, "a"), (3L, "c")], await ReadAllRows(table));
    }

    [Fact]
    public async Task RowTrackingProperty_DeclaresFeatureAndGeneratesMaterializedNames()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdValueSchema();

        await using (var created = await DeltaTable.CreateAsync(fs, schema,
            configuration: new Dictionary<string, string> { [RowTrackingConfig.EnableKey] = "true" }))
        {
            await created.WriteAsync([IdValueBatch(schema, (1, "a"), (2, "b"))]);
        }

        await using var table = await DeltaTable.OpenAsync(fs);
        var protocol = table.CurrentSnapshot.Protocol;

        Assert.Equal(7, protocol.MinWriterVersion);
        Assert.Contains("rowTracking", protocol.WriterFeatures!);
        // rowTracking depends on domainMetadata — the row-id high-water mark rides a system domain.
        Assert.Contains("domainMetadata", protocol.WriterFeatures!);
        Assert.Equal(1, protocol.MinReaderVersion);

        // The two spec-required hidden column names are fixed AT ENABLEMENT, so a table enabled by
        // property must get them just as one enabled by argument does.
        var (rowIdCol, rowVersionCol) = RowTrackingConfig
            .TryGetMaterializedColumnNames(table.CurrentSnapshot.Metadata.Configuration);
        Assert.NotNull(rowIdCol);
        Assert.NotNull(rowVersionCol);

        // Enablement took effect: the append assigned a baseRowId and advanced the HWM domain.
        var addFile = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        Assert.Equal(0L, addFile.BaseRowId!.Value);
        Assert.NotNull(table.GetDomainMetadata(RowTrackingConfig.DomainName));
    }

    // A table whose data files were already written against known hidden column names must keep them —
    // regenerating would point the reader at columns the files do not contain.
    [Fact]
    public async Task RowTrackingProperty_KeepsCallerSuppliedMaterializedNames()
    {
        var fs = new LocalTableFileSystem(_tempDir);

        await using var table = await DeltaTable.CreateAsync(fs, IdValueSchema(),
            configuration: new Dictionary<string, string>
            {
                [RowTrackingConfig.EnableKey] = "true",
                [RowTrackingConfig.MaterializedRowIdColumnNameKey] = "_row_id_col",
                [RowTrackingConfig.MaterializedRowCommitVersionColumnNameKey] = "_row_ver_col",
            });

        var (rowIdCol, rowVersionCol) = RowTrackingConfig
            .TryGetMaterializedColumnNames(table.CurrentSnapshot.Metadata.Configuration);
        Assert.Equal("_row_id_col", rowIdCol);
        Assert.Equal("_row_ver_col", rowVersionCol);
    }

    // ---------------------------------------------------------------------------------------------
    // Column mapping
    // ---------------------------------------------------------------------------------------------

    // The mode property is enablement too: without this, a caller passing only the property got a table
    // whose metadata claimed name-mapping over a schema with no physical names — unreadable.
    [Fact]
    public async Task ColumnMappingModeProperty_AssignsPhysicalNamesAndRoundTrips()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdValueSchema();

        await using (var created = await DeltaTable.CreateAsync(fs, schema,
            configuration: new Dictionary<string, string> { [ColumnMapping.ModeKey] = "name" }))
        {
            await created.WriteAsync([IdValueBatch(schema, (1, "a"), (2, "b"))]);
        }

        await using var table = await DeltaTable.OpenAsync(fs);

        Assert.Equal(2, table.CurrentSnapshot.Protocol.MinReaderVersion);
        Assert.Equal(5, table.CurrentSnapshot.Protocol.MinWriterVersion);
        Assert.Equal("2", table.CurrentSnapshot.Metadata.Configuration![ColumnMapping.MaxColumnIdKey]);

        foreach (var field in table.CurrentSnapshot.Schema.Fields)
        {
            Assert.NotNull(ColumnMapping.GetPhysicalName(field, ColumnMappingMode.Name));
            Assert.NotEqual(field.Name, ColumnMapping.GetPhysicalName(field, ColumnMappingMode.Name));
        }

        Assert.Equal([(1L, "a"), (2L, "b")], await ReadAllRows(table));
    }

    // ---------------------------------------------------------------------------------------------
    // IcebergCompat
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task IcebergCompatV1Property_DeclaresFeatureAndRoundTrips()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdValueSchema();

        await using (var created = await DeltaTable.CreateAsync(fs, schema,
            columnMappingMode: ColumnMappingMode.Name,
            configuration: new Dictionary<string, string> { [IcebergCompat.EnableV1Key] = "true" }))
        {
            await created.WriteAsync([IdValueBatch(schema, (1, "a"), (2, "b"))]);
        }

        await using var table = await DeltaTable.OpenAsync(fs);
        var protocol = table.CurrentSnapshot.Protocol;

        Assert.Equal(7, protocol.MinWriterVersion);
        Assert.Contains("icebergCompatV1", protocol.WriterFeatures!);
        Assert.DoesNotContain("icebergCompatV2", protocol.WriterFeatures!);
        // Column mapping is a hard dependency, and at writer 7 it must be spelled out in BOTH lists.
        Assert.Contains("columnMapping", protocol.WriterFeatures!);
        Assert.Contains("columnMapping", protocol.ReaderFeatures!);
        Assert.True(table.IsIcebergCompat);

        // The converter needs numRecords in every stats blob.
        var addFile = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        Assert.Contains("numRecords", addFile.Stats!);
        Assert.Equal([(1L, "a"), (2L, "b")], await ReadAllRows(table));
    }

    // V2 relaxes V1's ban on collections: arrays and maps are allowed, carrying nested field ids.
    [Fact]
    public async Task IcebergCompatV2Property_AllowsCollectionsAndDeclaresFeature()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("tags", new ArrowListType(new Field("item", StringType.Default, true)), true))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema,
            columnMappingMode: ColumnMappingMode.Name,
            configuration: new Dictionary<string, string> { [IcebergCompat.EnableV2Key] = "true" });

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal(7, protocol.MinWriterVersion);
        Assert.Contains("icebergCompatV2", protocol.WriterFeatures!);
        Assert.DoesNotContain("icebergCompatV1", protocol.WriterFeatures!);
    }

    // V2 wins if a caller somehow enables both, matching IcebergCompat.GetVersion — and only ONE of the
    // two features may be declared, since they are mutually exclusive in the spec.
    [Fact]
    public async Task IcebergCompat_BothVersionsEnabled_DeclaresV2Only()
    {
        var fs = new LocalTableFileSystem(_tempDir);

        await using var table = await DeltaTable.CreateAsync(fs, IdValueSchema(),
            columnMappingMode: ColumnMappingMode.Name,
            configuration: new Dictionary<string, string>
            {
                [IcebergCompat.EnableV1Key] = "true",
                [IcebergCompat.EnableV2Key] = "true",
            });

        Assert.Contains("icebergCompatV2", table.CurrentSnapshot.Protocol.WriterFeatures!);
        Assert.DoesNotContain("icebergCompatV1", table.CurrentSnapshot.Protocol.WriterFeatures!);
    }

    // The constraints are validated BEFORE commit 0 is written: a violating table must not exist at all.
    [Fact]
    public async Task IcebergCompat_WithoutColumnMapping_ThrowsAndWritesNoCommit()
    {
        var fs = new LocalTableFileSystem(_tempDir);

        await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await DeltaTable.CreateAsync(fs, IdValueSchema(),
                configuration: new Dictionary<string, string> { [IcebergCompat.EnableV1Key] = "true" }));

        Assert.Equal(-1, await new TransactionLog(fs).GetLatestVersionAsync());
    }

    [Fact]
    public async Task IcebergCompatV1_WithDeletionVectors_Throws()
    {
        var fs = new LocalTableFileSystem(_tempDir);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await DeltaTable.CreateAsync(fs, IdValueSchema(),
                columnMappingMode: ColumnMappingMode.Name,
                enableDeletionVectors: true,
                configuration: new Dictionary<string, string> { [IcebergCompat.EnableV1Key] = "true" }));

        Assert.Contains("deletion vectors", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(-1, await new TransactionLog(fs).GetLatestVersionAsync());
    }

    [Fact]
    public async Task IcebergCompatV1_WithArrayColumn_Throws()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("tags", new ArrowListType(new Field("item", StringType.Default, true)), true))
            .Build();

        await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await DeltaTable.CreateAsync(fs, schema,
                columnMappingMode: ColumnMappingMode.Name,
                configuration: new Dictionary<string, string> { [IcebergCompat.EnableV1Key] = "true" }));
    }

    // ---------------------------------------------------------------------------------------------
    // Composition + negative cases
    // ---------------------------------------------------------------------------------------------

    // Every feature at once: each is declared, and the legacy writer-v2 features the table-features
    // upgrade would otherwise drop on the floor are merged back in.
    [Fact]
    public async Task AllFeaturesTogether_EachDeclaredAndLegacyFeaturesMerged()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdValueSchema();

        await using (var created = await DeltaTable.CreateAsync(fs, schema,
            columnMappingMode: ColumnMappingMode.Name,
            configuration: new Dictionary<string, string>
            {
                [DeletionVectorConfig.EnableKey] = "true",
                [RowTrackingConfig.EnableKey] = "true",
                [InCommitTimestamp.EnableKey] = "true",
                [CdfConfig.EnableKey] = "true",
            }))
        {
            await created.WriteAsync([IdValueBatch(schema, (1, "a"), (2, "b"), (3, "c"))]);
            await created.DeleteAsync(b => MatchId(b, 2));
        }

        await using var table = await DeltaTable.OpenAsync(fs);
        var protocol = table.CurrentSnapshot.Protocol;

        Assert.Equal(3, protocol.MinReaderVersion);
        Assert.Equal(7, protocol.MinWriterVersion);
        Assert.All(
            new[]
            {
                "deletionVectors", "rowTracking", "domainMetadata", "inCommitTimestamp",
                "changeDataFeed", "columnMapping", "appendOnly", "invariants",
            },
            f => Assert.Contains(f, protocol.WriterFeatures!));
        Assert.All(
            new[] { "deletionVectors", "columnMapping" },
            f => Assert.Contains(f, protocol.ReaderFeatures!));

        // No feature is declared twice — a duplicate entry is a malformed protocol action.
        Assert.Equal(protocol.WriterFeatures!.Distinct(StringComparer.Ordinal).Count(),
            protocol.WriterFeatures!.Count);
        Assert.Equal(protocol.ReaderFeatures!.Distinct(StringComparer.Ordinal).Count(),
            protocol.ReaderFeatures!.Count);

        Assert.Equal([(1L, "a"), (3L, "c")], await ReadAllRows(table));
    }

    // A property that does not enable anything is carried through untouched.
    [Fact]
    public async Task NonFeatureProperties_ArePreservedVerbatim()
    {
        var fs = new LocalTableFileSystem(_tempDir);

        await using (await DeltaTable.CreateAsync(fs, IdValueSchema(),
            configuration: new Dictionary<string, string>
            {
                ["delta.deletedFileRetentionDuration"] = "interval 3 days",
                ["custom.owner"] = "analytics",
            }))
        {
        }

        await using var table = await DeltaTable.OpenAsync(fs);
        var config = table.CurrentSnapshot.Metadata.Configuration!;

        Assert.Equal("interval 3 days", config["delta.deletedFileRetentionDuration"]);
        Assert.Equal("analytics", config["custom.owner"]);
        // Nothing was enabled, so the table stays on the legacy protocol.
        Assert.Equal(1, table.CurrentSnapshot.Protocol.MinReaderVersion);
        Assert.Equal(2, table.CurrentSnapshot.Protocol.MinWriterVersion);
        Assert.Null(table.CurrentSnapshot.Protocol.WriterFeatures);
    }

    [Fact]
    public async Task FeaturePropertySetToFalse_DeclaresNothing()
    {
        var fs = new LocalTableFileSystem(_tempDir);

        await using var table = await DeltaTable.CreateAsync(fs, IdValueSchema(),
            configuration: new Dictionary<string, string>
            {
                [CdfConfig.EnableKey] = "false",
                [InCommitTimestamp.EnableKey] = "false",
                [DeletionVectorConfig.EnableKey] = "false",
            });

        Assert.Equal(2, table.CurrentSnapshot.Protocol.MinWriterVersion);
        Assert.Null(table.CurrentSnapshot.Protocol.WriterFeatures);
        // The properties themselves are still recorded — the caller asked for them.
        Assert.Equal("false", table.CurrentSnapshot.Metadata.Configuration![CdfConfig.EnableKey]);
    }

    // Enablement is one-directional: the argument is the caller's explicit intent, so it wins over a
    // property that says otherwise. (The reverse — property on, argument defaulted off — enables.)
    [Fact]
    public async Task BooleanArgument_WinsOverContradictingProperty()
    {
        var fs = new LocalTableFileSystem(_tempDir);

        await using var table = await DeltaTable.CreateAsync(fs, IdValueSchema(),
            enableDeletionVectors: true,
            configuration: new Dictionary<string, string> { [DeletionVectorConfig.EnableKey] = "false" });

        Assert.Equal("true", table.CurrentSnapshot.Metadata.Configuration![DeletionVectorConfig.EnableKey]);
        Assert.Contains(DeletionVectorConfig.FeatureName, table.CurrentSnapshot.Protocol.WriterFeatures!);
    }

    // ---------------------------------------------------------------------------------------------
    // OpenOrCreateAsync
    // ---------------------------------------------------------------------------------------------

    // The passthrough exists so a caller that always uses OpenOrCreate still gets a correctly declared
    // table on the create leg — and an existing table is left exactly as it was on the open leg.
    [Fact]
    public async Task OpenOrCreate_AppliesConfigurationOnCreateLegOnly()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdValueSchema();

        await using (var created = await DeltaTable.OpenOrCreateAsync(fs, schema,
            columnMappingMode: ColumnMappingMode.Name,
            configuration: new Dictionary<string, string> { [CdfConfig.EnableKey] = "true" }))
        {
            Assert.Contains("changeDataFeed", created.CurrentSnapshot.Protocol.WriterFeatures!);
            Assert.Equal("name", created.CurrentSnapshot.Metadata.Configuration![ColumnMapping.ModeKey]);
            await created.WriteAsync([IdValueBatch(schema, (1, "a"))]);
        }

        // Second call opens; a different configuration must NOT be applied (that would be a silent
        // metadata rewrite), and the table must still be at the version the write left it on.
        await using var opened = await DeltaTable.OpenOrCreateAsync(fs, schema,
            configuration: new Dictionary<string, string> { ["custom.ignored"] = "yes" });

        Assert.Equal(1, opened.CurrentSnapshot.Version);
        Assert.DoesNotContain("custom.ignored", opened.CurrentSnapshot.Metadata.Configuration!.Keys);
        Assert.Contains("changeDataFeed", opened.CurrentSnapshot.Protocol.WriterFeatures!);
    }
}

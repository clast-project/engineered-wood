// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <c>delta.feature.&lt;name&gt; = supported</c> table properties declare the feature in the protocol and are not
/// stored (#601). The expected protocols are the ones delta-spark 4.0.0 writes for the same
/// <c>CREATE TABLE ... TBLPROPERTIES</c>, measured.
/// </summary>
public class TableFeaturePropertyTests : IDisposable
{
    private readonly string _tempDir;

    public TableFeaturePropertyTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_featprop_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private LocalTableFileSystem Fs() => new(_tempDir);

    private static readonly Apache.Arrow.Schema IdSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch Ids(params long[] ids)
    {
        var builder = new Int64Array.Builder();
        foreach (long id in ids)
            builder.Append(id);
        return new RecordBatch(IdSchema, [builder.Build()], ids.Length);
    }

    private static Dictionary<string, string> Props(params string[] features) =>
        features.ToDictionary(f => "delta.feature." + f, _ => "supported");

    private Task<DeltaTable> CreateAsync(Dictionary<string, string> configuration) =>
        DeltaTable.CreateAsync(Fs(), IdSchema, configuration: configuration).AsTask();

    private static string[]? Split(string? list) =>
        list is null ? null : list.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

    private static void AssertFeatures(string[]? expected, IReadOnlyCollection<string>? actual)
    {
        if (expected is null)
        {
            Assert.Null(actual);
            return;
        }
        Assert.NotNull(actual);
        Assert.Equal(expected.OrderBy(f => f, StringComparer.Ordinal), actual!.OrderBy(f => f, StringComparer.Ordinal));
    }

    private static void AssertNoFeatureKeys(MetadataAction metadata)
    {
        foreach (string key in metadata.Configuration?.Keys ?? [])
            Assert.False(key.StartsWith("delta.feature.", StringComparison.OrdinalIgnoreCase), key);
    }

    // Each row is what Spark 4.0.0 / Delta 4.0.0 wrote for CREATE TABLE ... TBLPROPERTIES with these properties.
    [Theory]
    [InlineData("rowTracking", 1, 7, null, "domainMetadata,rowTracking,invariants,appendOnly")]
    [InlineData("deletionVectors", 3, 7, "deletionVectors", "deletionVectors,invariants,appendOnly")]
    [InlineData("rowTracking,deletionVectors", 3, 7, "deletionVectors",
        "deletionVectors,domainMetadata,rowTracking,invariants,appendOnly")]
    [InlineData("checkConstraints", 1, 3, null, null)]
    [InlineData("appendOnly", 1, 2, null, null)]
    [InlineData("invariants", 1, 2, null, null)]
    [InlineData("columnMapping", 2, 7, null, "columnMapping,invariants,appendOnly")]
    [InlineData("columnMapping,deletionVectors", 3, 7, "columnMapping,deletionVectors",
        "columnMapping,deletionVectors,invariants,appendOnly")]
    [InlineData("v2Checkpoint", 3, 7, "v2Checkpoint", "v2Checkpoint,invariants,appendOnly")]
    [InlineData("domainMetadata", 1, 7, null, "domainMetadata,invariants,appendOnly")]
    [InlineData("timestampNtz", 3, 7, "timestampNtz", "timestampNtz,invariants,appendOnly")]
    [InlineData("typeWidening", 3, 7, "typeWidening", "typeWidening,invariants,appendOnly")]
    [InlineData("variantType", 3, 7, "variantType", "variantType,invariants,appendOnly")]
    [InlineData("vacuumProtocolCheck", 3, 7, "vacuumProtocolCheck", "vacuumProtocolCheck,invariants,appendOnly")]
    [InlineData("icebergCompatV1", 2, 7, null, "columnMapping,icebergCompatV1,invariants,appendOnly")]
    [InlineData("icebergCompatV2", 2, 7, null, "columnMapping,icebergCompatV2,invariants,appendOnly")]
    [InlineData("inCommitTimestamp", 1, 7, null, "inCommitTimestamp,invariants,appendOnly")]
    [InlineData("changeDataFeed", 1, 7, null, "changeDataFeed,invariants,appendOnly")]
    [InlineData("generatedColumns", 1, 7, null, "generatedColumns,invariants,appendOnly")]
    [InlineData("identityColumns", 1, 7, null, "identityColumns,invariants,appendOnly")]
    public async Task Create_DeclaresTheFeatureAsSparkDoes(
        string features, int reader, int writer, string? readerFeatures, string? writerFeatures)
    {
        await using var table = await CreateAsync(Props(Split(features)!));

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((reader, writer), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        AssertFeatures(Split(readerFeatures), protocol.ReaderFeatures);
        AssertFeatures(Split(writerFeatures), protocol.WriterFeatures);
        AssertNoFeatureKeys(table.CurrentSnapshot.Metadata);

        // And the table is one this library goes on writing.
        await table.WriteAsync([Ids(1)]);
    }

    [Theory]
    [InlineData("delta.feature.rowTracking", "SUPPORTED")]
    [InlineData("delta.feature.rowTracking", "Supported")]
    [InlineData("delta.feature.rowTracking", "enabled")]
    [InlineData("delta.feature.RowTracking", "supported")]
    [InlineData("DELTA.FEATURE.rowTracking", "supported")]
    public async Task Create_KeyAndValueMatchWithoutRegardToCase(string key, string value)
    {
        await using var table = await CreateAsync(new Dictionary<string, string> { [key] = value });

        Assert.True(RowTrackingConfig.IsSupported(table.CurrentSnapshot.Protocol));
        Assert.Contains("rowTracking", table.CurrentSnapshot.Protocol.WriterFeatures!);
        AssertNoFeatureKeys(table.CurrentSnapshot.Metadata);
    }

    [Theory]
    [InlineData("delta.feature.rowTracking", "true", DeltaTableErrorCodes.UnsupportedFeatureStatus)]
    [InlineData("delta.feature.rowTracking", "", DeltaTableErrorCodes.UnsupportedFeatureStatus)]
    [InlineData("delta.feature.noSuchFeature", "supported", DeltaTableErrorCodes.UnsupportedFeaturesInConfig)]
    [InlineData("delta.feature.clustering", "supported", DeltaTableErrorCodes.ClusteringFeatureViaProperty)]
    public async Task Create_RefusesWhatSparkRefuses(string key, string value, string errorCode)
    {
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            () => CreateAsync(new Dictionary<string, string> { [key] = value }));

        Assert.Equal(errorCode, ex.ErrorCode);
        Assert.Equal(-1, await new TransactionLog(Fs()).GetLatestVersionAsync());
    }

    [Fact]
    public async Task Create_RowTrackingSupported_AssignsColumnNamesAndRowIds_WithoutEnabling()
    {
        await using var table = await CreateAsync(Props("rowTracking"));

        var configuration = table.CurrentSnapshot.Metadata.Configuration!;
        Assert.False(RowTrackingConfig.IsEnabled(configuration));
        Assert.True(configuration.ContainsKey(RowTrackingConfig.MaterializedRowIdColumnNameKey));
        Assert.True(configuration.ContainsKey(RowTrackingConfig.MaterializedRowCommitVersionColumnNameKey));

        // A table that supports row tracking gets row ids on every write (#599).
        await table.WriteAsync([Ids(1, 2)]);
        await table.WriteAsync([Ids(3)]);
        var baseRowIds = table.CurrentSnapshot.ActiveFiles.Values
            .Select(add => add.BaseRowId).OrderBy(id => id).ToList();
        Assert.Equal(new long?[] { 0, 2 }, baseRowIds);
    }

    [Fact]
    public async Task Create_FeatureAlongsideItsDisabledProperty_KeepsTheProperty()
    {
        var configuration = Props("deletionVectors");
        configuration["delta.enableDeletionVectors"] = "false";

        await using var table = await CreateAsync(configuration);

        Assert.Contains("deletionVectors", table.CurrentSnapshot.Protocol.ReaderFeatures!);
        Assert.Equal(
            new Dictionary<string, string> { ["delta.enableDeletionVectors"] = "false" },
            table.CurrentSnapshot.Metadata.Configuration);
    }

    [Fact]
    public async Task Create_FeatureAlongsideAnEnabledFeature_DeclaresBoth()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs(), IdSchema, enableDeletionVectors: true, configuration: Props("inCommitTimestamp"));

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((3, 7), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        AssertFeatures(["deletionVectors"], protocol.ReaderFeatures);
        AssertFeatures(["deletionVectors", "inCommitTimestamp", "invariants", "appendOnly"], protocol.WriterFeatures);
    }

    [Fact]
    public async Task Create_FeatureOnAColumnMappingTable_KeepsColumnMappingDeclared()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs(), IdSchema, columnMappingMode: ColumnMappingMode.Name, configuration: Props("deletionVectors"));

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((3, 7), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        Assert.Contains("columnMapping", protocol.ReaderFeatures!);
        Assert.Contains("columnMapping", protocol.WriterFeatures!);
        Assert.Contains("deletionVectors", protocol.ReaderFeatures!);
    }

    [Fact]
    public async Task Replace_AddsTheFeatureAndKeepsTheReplacedTablesFeatures()
    {
        await using (await CreateAsync(Props("deletionVectors")))
        {
        }

        await using var table = await DeltaTable.CreateOrReplaceAsync(
            Fs(), IdSchema, [Ids(1)], configuration: Props("rowTracking"));

        var protocol = table.CurrentSnapshot.Protocol;
        AssertFeatures(["deletionVectors"], protocol.ReaderFeatures);
        AssertFeatures(
            ["deletionVectors", "domainMetadata", "rowTracking", "invariants", "appendOnly"], protocol.WriterFeatures);
        AssertNoFeatureKeys(table.CurrentSnapshot.Metadata);
    }

    [Fact]
    public async Task Replace_RefusesABadFeatureProperty()
    {
        await using (await CreateAsync([]))
        {
        }

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await DeltaTable.CreateOrReplaceAsync(
                Fs(), IdSchema, [Ids(1)],
                configuration: new Dictionary<string, string> { ["delta.feature.rowTracking"] = "true" }));
        Assert.Equal(DeltaTableErrorCodes.UnsupportedFeatureStatus, ex.ErrorCode);
        Assert.Equal(0, await new TransactionLog(Fs()).GetLatestVersionAsync());
    }

    /// <summary>
    /// A replacement of a table that supports row tracking keeps the feature (the protocol is never downgraded), and
    /// with it the materialized column names and the row-id high-water mark, as Spark's REPLACE does: a fresh mark
    /// would hand out row ids the replaced table already used.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Replace_OfARowTrackingTable_KeepsNamesAndHighWaterMark(bool enabled)
    {
        var configuration = enabled
            ? new Dictionary<string, string> { [RowTrackingConfig.EnableKey] = "true" }
            : Props("rowTracking");
        IReadOnlyDictionary<string, string> before;
        await using (var created = await CreateAsync(configuration))
        {
            await created.WriteAsync([Ids(1, 2)]);
            before = created.CurrentSnapshot.Metadata.Configuration!;
        }

        await using var table = await DeltaTable.CreateOrReplaceAsync(
            Fs(), IdSchema, [Ids(3)],
            enableRowTracking: enabled);

        var after = table.CurrentSnapshot.Metadata.Configuration!;
        Assert.Equal(before[RowTrackingConfig.MaterializedRowIdColumnNameKey],
            after[RowTrackingConfig.MaterializedRowIdColumnNameKey]);
        Assert.Equal(before[RowTrackingConfig.MaterializedRowCommitVersionColumnNameKey],
            after[RowTrackingConfig.MaterializedRowCommitVersionColumnNameKey]);
        Assert.True(table.CurrentSnapshot.DomainMetadata.ContainsKey(RowTrackingConfig.DomainName));
        var add = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        Assert.Equal(2, add.BaseRowId);
    }

    /// <summary>
    /// Caller-supplied metaData that introduces a <c>delta.feature.*</c> key is refused through every seam that takes
    /// it, since stored there it declares nothing. One carried through unchanged, from a table written before #601
    /// stored them, still commits.
    /// </summary>
    [Fact]
    public async Task CallerMetadata_IntroducingAFeatureProperty_IsRefused()
    {
        await using var table = await CreateAsync([]);
        var metadata = table.CurrentSnapshot.Metadata;
        var withKey = metadata with
        {
            Configuration = new Dictionary<string, string> { ["delta.feature.deletionVectors"] = "supported" },
        };
        long version = table.CurrentSnapshot.Version;

        var fused = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.CommitDataFilesAsync(System.Array.Empty<WrittenDataFile>(), extraActions: [withKey]));
        Assert.Equal(DeltaTableErrorCodes.UnsupportedFeaturesInConfig, fused.ErrorCode);

        var txn = table.StartTransaction();
        txn.StageActions([withKey]);
        var staged = await Assert.ThrowsAsync<DeltaFormatException>(async () => await txn.CommitAsync());
        Assert.Equal(DeltaTableErrorCodes.UnsupportedFeaturesInConfig, staged.ErrorCode);

        var clustering = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.SetClusteringColumnsAsync([], extraActions: [withKey]));
        Assert.Equal(DeltaTableErrorCodes.UnsupportedFeaturesInConfig, clustering.ErrorCode);

        Assert.Equal(version, (await DeltaTable.OpenAsync(Fs())).CurrentSnapshot.Version);
    }

    [Fact]
    public async Task CallerMetadata_CarryingAnExistingFeatureKeyUnchanged_Commits()
    {
        // A table as this library wrote one before #601: the key stored verbatim, the protocol untouched.
        var log = new TransactionLog(Fs());
        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "pre-601",
                Format = Format.Parquet,
                SchemaString = @"{""type"":""struct"",""fields"":[" +
                    @"{""name"":""id"",""type"":""long"",""nullable"":false,""metadata"":{}}]}",
                PartitionColumns = [],
                Configuration = new Dictionary<string, string> { ["delta.feature.rowTracking"] = "supported" },
            },
        });
        await using var table = await DeltaTable.OpenAsync(Fs());
        var metadata = table.CurrentSnapshot.Metadata;
        var updated = metadata with
        {
            Configuration = metadata.Configuration!.Concat([new KeyValuePair<string, string>("delta.appendOnly", "false")])
                .ToDictionary(kv => kv.Key, kv => kv.Value),
        };

        await table.CommitDataFilesAsync(System.Array.Empty<WrittenDataFile>(), extraActions: [updated]);

        Assert.Equal("false", table.CurrentSnapshot.Metadata.Configuration!["delta.appendOnly"]);
    }
}

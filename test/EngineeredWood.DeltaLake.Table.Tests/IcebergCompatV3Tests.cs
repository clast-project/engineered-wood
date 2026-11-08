// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using ArrowListType = Apache.Arrow.Types.ListType;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// IcebergCompatV3 (#745). Each rule is delta-spark's IcebergCompatV3 check of the same name (javap on the 4.3.1 and
/// 4.4.1 jars), measured against Spark 4.2.0 + delta-spark 4.4.1. delta-spark 4.3.1 declares the feature but enforces
/// none of the checks; 4.4 enforces them all.
/// </summary>
public class IcebergCompatV3Tests : IDisposable
{
    private readonly string _tempDir;

    public IcebergCompatV3Tests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_iceberg_v3_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private static readonly Apache.Arrow.Schema IdValue = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch Batch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        for (int i = 0; i < count; i++)
        {
            ids.Append(startId + i);
            values.Append("v" + (startId + i));
        }
        return new RecordBatch(IdValue, [ids.Build(), values.Build()], count);
    }

    private static Dictionary<string, string> V3(params (string Key, string Value)[] more)
    {
        var configuration = new Dictionary<string, string> { [IcebergCompat.EnableV3Key] = "true" };
        foreach (var (key, value) in more)
            configuration[key] = value;
        return configuration;
    }

    private Task<DeltaTable> CreateV3Async(
        Apache.Arrow.Schema? schema = null,
        IReadOnlyList<string>? partitionColumns = null,
        ColumnMappingMode mode = ColumnMappingMode.Name,
        bool enableDeletionVectors = false,
        params (string Key, string Value)[] more) =>
        DeltaTable.CreateAsync(Fs, schema ?? IdValue, partitionColumns: partitionColumns,
            columnMappingMode: mode, enableDeletionVectors: enableDeletionVectors, configuration: V3(more)).AsTask();

    private static async Task RefusedAsync(Func<Task> action)
    {
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(action);
        Assert.Equal(DeltaErrorCodes.IcebergCompatViolation, ex.ErrorCode);
    }

    private static MetadataAction WithConfiguration(MetadataAction metadata, string key, string value) =>
        metadata with
        {
            Configuration = metadata.Configuration!
                .Where(pair => pair.Key != key)
                .Append(new KeyValuePair<string, string>(key, value))
                .ToDictionary(pair => pair.Key, pair => pair.Value),
        };

    private static async Task<List<long>> RowIdsAsync(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            var rowId = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            for (int i = 0; i < batch.Length; i++)
                ids.Add(rowId.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    private async Task<int> RowCountAsync(DeltaTable table)
    {
        int rows = 0;
        await foreach (var batch in table.ReadAllAsync())
            rows += batch.Length;
        return rows;
    }

    // The top-level Parquet columns of a data file, with the field id each carries (null for none).
    private async Task<Dictionary<string, int?>> FileColumnIdsAsync(string path)
    {
        await using var file = await Fs.OpenReadAsync(DeltaPath.Decode(path));
        using var reader = new Parquet.ParquetFileReader(file, ownsFile: false);
        var root = (await reader.GetSchemaAsync()).Root;
        return root.Children.ToDictionary(c => c.Name, c => c.Element.FieldId);
    }

    // ---- Creation ------------------------------------------------------------------------------------------

    /// <summary>
    /// The property declares the V3 writer feature with the two it requires, and turns row tracking on as Spark does
    /// when the caller leaves <c>delta.enableRowTracking</c> out (measured on 4.4.1:
    /// [appendOnly, columnMapping, domainMetadata, icebergCompatV3, invariants, rowTracking]).
    /// </summary>
    [Fact]
    public async Task V3Property_DeclaresTheFeature_AndTurnsOnRowTracking()
    {
        await using var table = await CreateV3Async();
        await table.WriteAsync([Batch(1, 3)]);

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal(7, protocol.MinWriterVersion);
        foreach (string feature in new[] { "icebergCompatV3", "columnMapping", "rowTracking", "domainMetadata" })
            Assert.Contains(feature, protocol.WriterFeatures!);
        Assert.DoesNotContain("icebergCompatV2", protocol.WriterFeatures!);
        Assert.DoesNotContain("icebergCompatV1", protocol.WriterFeatures!);

        var configuration = table.CurrentSnapshot.Metadata.Configuration!;
        Assert.Equal("true", configuration[RowTrackingConfig.EnableKey]);
        Assert.True(configuration.ContainsKey(RowTrackingConfig.MaterializedRowIdColumnNameKey));
        Assert.True(table.IsIcebergCompat);

        var add = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        Assert.Contains("numRecords", add.Stats!);
        Assert.Equal(0, add.BaseRowId);
        Assert.Equal([0L, 1L, 2L], await RowIdsAsync(table));
    }

    // Spark: WRONG_REQUIRED_TABLE_PROPERTY ("requires table property 'delta.enableRowTracking' to be set to 'true'").
    [Fact]
    public async Task V3_WithRowTrackingTurnedOff_IsRefused()
    {
        await RefusedAsync(() => CreateV3Async(more: (RowTrackingConfig.EnableKey, "false")));
        Assert.False(Directory.Exists(Path.Combine(_tempDir, "_delta_log")));
    }

    [Fact]
    public async Task V3_WithoutColumnMapping_IsRefused()
    {
        await RefusedAsync(() => CreateV3Async(mode: ColumnMappingMode.None));
    }

    // Spark: VERSION_MUTUAL_EXCLUSIVE.
    [Theory]
    [InlineData(IcebergCompat.EnableV1Key)]
    [InlineData(IcebergCompat.EnableV2Key)]
    public async Task V3_WithAnotherIcebergCompatVersion_IsRefused(string otherKey)
    {
        await RefusedAsync(() => CreateV3Async(more: (otherKey, "true")));
    }

    /// <summary>
    /// V3 has no deletion-vector check, so the V1/V2 rule from #751 must not reach it: a V3 table with deletion
    /// vectors enabled is created, and a DELETE writes a deletion vector (Spark 4.4.1 does the same).
    /// </summary>
    [Fact]
    public async Task V3_WithDeletionVectors_IsCreated_AndADeleteWritesADeletionVector()
    {
        await using var table = await CreateV3Async(enableDeletionVectors: true);
        Assert.Contains(DeletionVectorConfig.FeatureName, table.CurrentSnapshot.Protocol.WriterFeatures!);
        await table.WriteAsync([Batch(1, 5)]);

        await table.DeleteAsync(Ex.Equal("id", 2L));
        await table.UpdateAsync(Ex.Equal("id", 3L), batch => batch);

        Assert.Contains(table.CurrentSnapshot.ActiveFiles.Values, add => add.DeletionVector is not null);
        Assert.Equal(4, await RowCountAsync(table));
    }

    // Enabling deletion vectors on an existing V3 table is allowed too, where V2 refuses it.
    [Fact]
    public async Task EnablingDeletionVectors_OnAV3Table_Commits()
    {
        await using var table = await CreateV3Async();
        var protocol = table.CurrentSnapshot.Protocol;
        await table.CommitDataFilesAsync(System.Array.Empty<WrittenDataFile>(), extraActions:
        [
            protocol with
            {
                MinReaderVersion = 3,
                ReaderFeatures = [.. protocol.ReaderFeatures ?? [], DeletionVectorConfig.FeatureName],
                WriterFeatures = [.. protocol.WriterFeatures!, DeletionVectorConfig.FeatureName],
            },
            WithConfiguration(table.CurrentSnapshot.Metadata, DeletionVectorConfig.EnableKey, "true"),
        ]);

        await table.WriteAsync([Batch(1, 3)]);
        await table.DeleteAsync(Ex.Equal("id", 2L));
        Assert.Contains(table.CurrentSnapshot.ActiveFiles.Values, add => add.DeletionVector is not null);
    }

    /// <summary>
    /// delta-spark 4.3.1 runs none of the V3 checks, and a V3 table it creates supports row tracking without
    /// enabling it: <c>delta.enableRowTracking</c> is absent (measured on Spark 4.1.2). Such a table reads, and EW
    /// refuses to write it, since V3 requires row tracking enabled. delta-spark 4.4 would set the property on its
    /// next commit; EW does not change a table's properties on a write.
    /// </summary>
    [Fact]
    public async Task AV3TableWhoseRowTrackingIsNotEnabled_Reads_ButEveryWriteIsRefused()
    {
        long version;
        MetadataAction metadata;
        ProtocolAction protocol;
        await using (var created = await CreateV3Async())
        {
            await created.WriteAsync([Batch(1, 2)]);
            version = created.CurrentSnapshot.Version;
            metadata = created.CurrentSnapshot.Metadata;
            protocol = created.CurrentSnapshot.Protocol;
        }
        await new TransactionLog(Fs).WriteCommitAsync(version + 1,
        [
            protocol,
            metadata with
            {
                Configuration = metadata.Configuration!
                    .Where(pair => pair.Key != RowTrackingConfig.EnableKey)
                    .ToDictionary(pair => pair.Key, pair => pair.Value),
            },
        ]);

        await using var table = await DeltaTable.OpenAsync(Fs);
        Assert.Equal(2, await RowCountAsync(table));
        await RefusedAsync(async () => await table.WriteAsync([Batch(10, 1)]));
        await RefusedAsync(async () => await table.WriteAsync([Batch(10, 1)], DeltaWriteMode.Overwrite));
        await RefusedAsync(async () =>
            await table.CommitDataFilesAsync(System.Array.Empty<WrittenDataFile>()));
        Assert.Equal(version + 1, await new TransactionLog(Fs).GetLatestVersionAsync());
    }

    // ---- Types ---------------------------------------------------------------------------------------------

    // CheckTypeInV3AllowList: V2's list plus variant; arrays and maps get nested ids, as under V2.
    [Fact]
    public async Task V3_AllowsVariantAndCollections_AndAssignsNestedIds()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("tags", new ArrowListType(new Field("element", StringType.Default, true)), true))
            .Build();
        await using var table = await CreateV3Async(schema);

        var tags = table.CurrentSnapshot.Schema.Fields.Single(f => f.Name == "tags");
        Assert.Single(ColumnMapping.GetNestedIds(tags));
    }

    [Theory]
    [InlineData("""{"name":"n","type":"void","nullable":true,"metadata":{}}""")]
    [InlineData("""{"name":"a","type":{"type":"array","elementType":"void","containsNull":true},"nullable":true,"metadata":{}}""")]
    [InlineData("""{"name":"s","type":{"type":"struct","fields":[{"name":"i","type":"interval","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}""")]
    public void V3_RefusesATypeOutsideItsAllowList(string fieldJson)
    {
        var ex = Assert.Throws<DeltaFormatException>(() => Validate(SchemaWith(fieldJson)));
        Assert.Equal(DeltaErrorCodes.IcebergCompatViolation, ex.ErrorCode);
    }

    [Fact]
    public void V3_AllowsVariant()
    {
        Validate(SchemaWith("""{"name":"v","type":"variant","nullable":true,"metadata":{}}"""));
    }

    // CheckPartitionDataTypeInV2AllowList holds DecimalType's companion object, so a decimal partition column is
    // refused (measured on 4.4.1: UNSUPPORTED_PARTITION_DATA_TYPE for decimal(10,2)).
    [Fact]
    public async Task V3_RefusesADecimalPartitionColumn_AndAcceptsAnIntegerOne()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("d", new Decimal128Type(10, 2), true))
            .Field(new Field("k", Int32Type.Default, true))
            .Build();

        await RefusedAsync(() => CreateV3Async(schema, partitionColumns: ["d"]));
        await using var table = await CreateV3Async(schema, partitionColumns: ["k"]);
        Assert.Equal(["k"], table.CurrentSnapshot.Metadata.PartitionColumns);
    }

    // ---- Lifecycle -----------------------------------------------------------------------------------------

    // CheckNoPartitionEvolution: a REPLACE keeps a partitioned table's partition columns. Spark refuses it with
    // REPLACE_TABLE_CHANGE_PARTITION_NAMES, and lets an unpartitioned table become partitioned (both measured).
    [Fact]
    public async Task Replacing_APartitionedV3Table_WithOtherPartitionColumns_IsRefused()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("k", Int32Type.Default, true))
            .Build();
        await (await DeltaTable.CreateAsync(Fs, schema, partitionColumns: ["k"],
            columnMappingMode: ColumnMappingMode.Name, configuration: V3())).DisposeAsync();

        await RefusedAsync(async () => await DeltaTable.CreateOrReplaceAsync(Fs, schema, [],
            partitionColumns: ["id"], columnMappingMode: ColumnMappingMode.Name, configuration: V3()));

        await using var same = await DeltaTable.CreateOrReplaceAsync(Fs, schema, [],
            partitionColumns: ["k"], columnMappingMode: ColumnMappingMode.Name, configuration: V3());
        Assert.Equal(1, same.CurrentSnapshot.Version);
    }

    [Fact]
    public async Task Replacing_AnUnpartitionedV3Table_WithAPartitionedOne_Commits()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("k", Int32Type.Default, true))
            .Build();
        await (await DeltaTable.CreateAsync(Fs, schema,
            columnMappingMode: ColumnMappingMode.Name, configuration: V3())).DisposeAsync();

        await using var replaced = await DeltaTable.CreateOrReplaceAsync(Fs, schema, [],
            partitionColumns: ["k"], columnMappingMode: ColumnMappingMode.Name, configuration: V3());
        Assert.Equal(["k"], replaced.CurrentSnapshot.Metadata.PartitionColumns);
    }

    // CheckCannotDisableCompat: "IcebergCompat cannot be disabled", for an ALTER and a REPLACE alike (measured), and
    // for a RESTORE to a version from before V3.
    [Fact]
    public async Task DisablingV3_IsRefused_ByACommit_AReplace_AndARestore()
    {
        await using (var plain = await DeltaTable.CreateAsync(Fs, IdValue, columnMappingMode: ColumnMappingMode.Name))
        {
            await plain.WriteAsync([Batch(1, 2)]);
        }
        // An EW-written V0 without V3, then a V3 table over it by REPLACE.
        await using var table = await DeltaTable.CreateOrReplaceAsync(Fs, IdValue, [Batch(10, 2)],
            columnMappingMode: ColumnMappingMode.Name, configuration: V3());
        long version = table.CurrentSnapshot.Version;

        await RefusedAsync(async () => await table.CommitDataFilesAsync(System.Array.Empty<WrittenDataFile>(),
            extraActions: [WithConfiguration(table.CurrentSnapshot.Metadata, IcebergCompat.EnableV3Key, "false")]));

        var txn = table.StartTransaction();
        txn.StageActions([WithConfiguration(table.CurrentSnapshot.Metadata, IcebergCompat.EnableV3Key, "false")]);
        await RefusedAsync(async () => await txn.CommitAsync());

        await RefusedAsync(async () => await DeltaTable.CreateOrReplaceAsync(Fs, IdValue, [],
            columnMappingMode: ColumnMappingMode.Name));

        await RefusedAsync(async () => await table.RestoreAsync(1));

        Assert.Equal(version, await new TransactionLog(Fs).GetLatestVersionAsync());
    }

    // CheckNoRowTrackingBeforeUpgradeV3: "Alter Table enable icebergCompatV3 with RowTracking enabled is not
    // supported. Please disable RowTracking first." (measured on 4.4.1).
    [Fact]
    public async Task EnablingV3_OnATableWhoseRowTrackingIsAlreadyEnabled_IsRefused()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdValue,
            columnMappingMode: ColumnMappingMode.Name, enableRowTracking: true);
        var protocol = table.CurrentSnapshot.Protocol;

        await RefusedAsync(async () => await table.CommitDataFilesAsync(System.Array.Empty<WrittenDataFile>(),
            extraActions:
            [
                protocol with { WriterFeatures = [.. protocol.WriterFeatures!, "icebergCompatV3"] },
                WithConfiguration(table.CurrentSnapshot.Metadata, IcebergCompat.EnableV3Key, "true"),
            ]));
    }

    // The upgrade Spark makes by itself, turning row tracking on in the same commit, passes the check.
    [Fact]
    public async Task EnablingV3_TogetherWithRowTracking_OnAnExistingTable_Commits()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdValue, columnMappingMode: ColumnMappingMode.Name);
        var protocol = table.CurrentSnapshot.Protocol;
        var (rowIdName, rowVersionName) = RowTrackingConfig.GenerateMaterializedColumnNames();
        var metadata = WithConfiguration(table.CurrentSnapshot.Metadata, IcebergCompat.EnableV3Key, "true");
        metadata = WithConfiguration(metadata, RowTrackingConfig.EnableKey, "true");
        metadata = WithConfiguration(metadata, RowTrackingConfig.MaterializedRowIdColumnNameKey, rowIdName);
        metadata = WithConfiguration(metadata, RowTrackingConfig.MaterializedRowCommitVersionColumnNameKey,
            rowVersionName);

        // (2, 5) implies the legacy writer features through columnMapping, which writer 7 must then list.
        Assert.Equal((2, 5), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        await table.CommitDataFilesAsync(System.Array.Empty<WrittenDataFile>(), extraActions:
        [
            protocol with
            {
                MinWriterVersion = 7,
                ReaderFeatures = ["columnMapping"],
                WriterFeatures =
                [
                    "appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns",
                    "columnMapping", "rowTracking", "domainMetadata", "icebergCompatV3",
                ],
            },
            metadata,
        ]);

        Assert.True(table.IsIcebergCompat);
        await table.WriteAsync([Batch(1, 2)]);
        Assert.Equal([0L, 1L], await RowIdsAsync(table));
    }

    // CheckTypeWideningSupported: on a table that supports type widening, a recorded type change Iceberg cannot
    // follow is refused (int to double, measured as UNSUPPORTED_TYPE_WIDENING); int to long and byte to short are not.
    [Theory]
    [InlineData("integer", "long", true)]
    [InlineData("byte", "short", true)]
    [InlineData("integer", "double", false)]
    [InlineData("decimal(5,2)", "decimal(7,3)", false)]
    public async Task ARecordedTypeChange_OnAV3Table_PassesOnlyWhatIcebergFollows(
        string fromType, string toType, bool commits)
    {
        await using var table = await CreateV3Async(more: (Schema.TypeWidening.EnableKey, "true"));
        long version = table.CurrentSnapshot.Version;
        var schema = table.CurrentSnapshot.Schema;
        var value = schema.Fields.Single(f => f.Name == "value");
        var changed = new StructField
        {
            Name = "w",
            Type = new PrimitiveType { TypeName = toType },
            Nullable = true,
            Metadata = new Dictionary<string, string>(value.Metadata!.ToDictionary(p => p.Key, p => p.Value))
            {
                [ColumnMapping.FieldIdKey] = "99",
                [ColumnMapping.PhysicalNameKey] = "col-w",
                [Schema.TypeWidening.TypeChangesKey] = Schema.TypeWidening.SerializeTypeChanges(
                    [new TypeChange { FromType = fromType, ToType = toType }]),
            },
        };
        var metadata = WithConfiguration(table.CurrentSnapshot.Metadata with
        {
            SchemaString = DeltaSchemaSerializer.Serialize(new Schema.StructType { Fields = [.. schema.Fields, changed] }),
        }, ColumnMapping.MaxColumnIdKey, "99");

        Task Commit() => table.CommitDataFilesAsync(
            System.Array.Empty<WrittenDataFile>(), extraActions: [metadata]).AsTask();
        if (commits)
        {
            await Commit();
            Assert.Equal(version + 1, table.CurrentSnapshot.Version);
        }
        else
        {
            await RefusedAsync(Commit);
            Assert.Equal(version, await new TransactionLog(Fs).GetLatestVersionAsync());
        }
    }

    // EW's own widening, by schema evolution, makes only the changes Iceberg follows on an IcebergCompat table.
    [Fact]
    public async Task MergeSchemaWidening_OnAV3Table_Commits()
    {
        Apache.Arrow.Schema With(IArrowType type) => new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("x", type, true))
            .Build();
        await using var table = await CreateV3Async(With(Int32Type.Default), more: (Schema.TypeWidening.EnableKey, "true"));

        var wide = With(Int64Type.Default);
        await table.WriteAsync([new RecordBatch(wide,
            [new Int64Array.Builder().Append(1).Build(), new Int64Array.Builder().Append(5_000_000_000).Build()], 1)],
            mergeSchema: true);

        var x = table.CurrentSnapshot.Schema.Fields.Single(f => f.Name == "x");
        Assert.Equal("long", ((PrimitiveType)x.Type).TypeName);
        Assert.Single(Schema.TypeWidening.GetTypeChanges(x));
    }

    // CheckColumnDefaultIsLiteral: Spark refused `x INT DEFAULT 2 + 3` and accepted `DEFAULT 5` (measured).
    [Theory]
    [InlineData("5", true)]
    [InlineData("2 + 3", false)]
    public void V3_RequiresALiteralColumnDefault(string defaultText, bool accepted)
    {
        string field = $$$"""{"name":"x","type":"integer","nullable":true,"metadata":{"CURRENT_DEFAULT":"{{{defaultText}}}"}}""";
        if (accepted)
        {
            Validate(SchemaWith(field));
        }
        else
        {
            var ex = Assert.Throws<DeltaFormatException>(() => Validate(SchemaWith(field)));
            Assert.Equal(DeltaErrorCodes.IcebergCompatViolation, ex.ErrorCode);
        }
    }

    // ---- Row lineage field ids ------------------------------------------------------------------------------

    /// <summary>
    /// delta-spark 4.4 stamps Iceberg's reserved row-lineage field ids on the materialized row-tracking columns of a
    /// V3 table's data files (measured on UPDATE and OPTIMIZE output, name and id mode). A compaction here does the
    /// same, and the ids survive it.
    /// </summary>
    [Theory]
    [InlineData(ColumnMappingMode.Name)]
    [InlineData(ColumnMappingMode.Id)]
    public async Task Compaction_OfAV3Table_StampsTheRowLineageFieldIds(ColumnMappingMode mode)
    {
        await using var table = await CreateV3Async(mode: mode);
        await table.WriteAsync([Batch(1, 2)]);
        await table.WriteAsync([Batch(10, 2)]);
        Assert.Equal([0L, 1L, 2L, 3L], await RowIdsAsync(table));

        Assert.NotNull(await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue }));

        var configuration = table.CurrentSnapshot.Metadata.Configuration!;
        string rowIdName = configuration[RowTrackingConfig.MaterializedRowIdColumnNameKey];
        string rowVersionName = configuration[RowTrackingConfig.MaterializedRowCommitVersionColumnNameKey];
        var add = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        var ids = await FileColumnIdsAsync(add.Path);
        Assert.Equal(IcebergCompat.RowIdFieldId, ids[rowIdName]);
        Assert.Equal(IcebergCompat.RowCommitVersionFieldId, ids[rowVersionName]);

        Assert.Equal([0L, 1L, 2L, 3L], await RowIdsAsync(table));
        Assert.Equal(4, await RowCountAsync(table));

        await using var reopened = await DeltaTable.OpenAsync(Fs);
        Assert.Equal([0L, 1L, 2L, 3L], await RowIdsAsync(reopened));
    }

    // Only V3 gives them ids: a row-tracking table without it writes the columns as before.
    [Fact]
    public async Task Compaction_OfARowTrackingTableWithoutV3_StampsNoFieldIds()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdValue,
            columnMappingMode: ColumnMappingMode.Name, enableRowTracking: true);
        await table.WriteAsync([Batch(1, 2)]);
        await table.WriteAsync([Batch(10, 2)]);
        await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });

        string rowIdName = table.CurrentSnapshot.Metadata.Configuration![RowTrackingConfig.MaterializedRowIdColumnNameKey];
        var ids = await FileColumnIdsAsync(Assert.Single(table.CurrentSnapshot.ActiveFiles.Values).Path);
        Assert.Null(ids[rowIdName]);
    }

    // ---- Direct validation ---------------------------------------------------------------------------------

    private static string SchemaWith(string fieldJson) =>
        $$$"""{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{{{fieldJson}}}]}""";

    private static void Validate(string schemaJson)
    {
        var (mapped, maxId) = ColumnMapping.AssignColumnMapping(DeltaSchemaSerializer.Parse(schemaJson));
        var metadata = new MetadataAction
        {
            Id = "v3",
            Format = Format.Parquet,
            SchemaString = DeltaSchemaSerializer.Serialize(mapped),
            PartitionColumns = [],
            Configuration = new Dictionary<string, string>
            {
                [ColumnMapping.ModeKey] = "name",
                [ColumnMapping.MaxColumnIdKey] = maxId.ToString(),
                [IcebergCompat.EnableV3Key] = "true",
                [RowTrackingConfig.EnableKey] = "true",
            },
        };
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 3,
            MinWriterVersion = 7,
            ReaderFeatures = ["columnMapping"],
            WriterFeatures = ["columnMapping", "rowTracking", "domainMetadata", "icebergCompatV3"],
        };
        IcebergCompat.Validate(IcebergCompatVersion.V3, metadata, protocol);
    }
}

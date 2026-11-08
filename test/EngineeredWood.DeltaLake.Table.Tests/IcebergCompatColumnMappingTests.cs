// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// IcebergCompat and column mapping at create (#757). Every IcebergCompat version requires column mapping, and
/// delta-spark turns on <c>name</c> mode at CREATE and REPLACE when the caller leaves <c>delta.columnMapping.mode</c>
/// out (RequireColumnMapping's autoSetValue; measured on 4.0.0, 4.3.1 and 4.4.1). It keeps <c>id</c>, refuses an
/// explicit <c>none</c>, and refuses an ALTER of an existing table that does not set the mode itself
/// (<c>autoEnableOnExistingTable</c> is false for column mapping).
/// </summary>
public class IcebergCompatColumnMappingTests : IDisposable
{
    private readonly string _tempDir;

    public IcebergCompatColumnMappingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_iceberg_cm_{Guid.NewGuid():N}");
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

    private static Dictionary<string, string> Config(params (string Key, string Value)[] pairs) =>
        pairs.ToDictionary(p => p.Key, p => p.Value);

    private static async Task RefusedAsync(Func<Task> action)
    {
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(action);
        Assert.Equal(DeltaErrorCodes.IcebergCompatViolation, ex.ErrorCode);
    }

    private static void AssertMapped(DeltaTable table, string mode, string feature)
    {
        var snapshot = table.CurrentSnapshot;
        var configuration = snapshot.Metadata.Configuration!;
        Assert.Equal(mode, configuration[ColumnMapping.ModeKey]);
        Assert.True(int.Parse(configuration[ColumnMapping.MaxColumnIdKey]) >= 2);
        Assert.Contains("columnMapping", snapshot.Protocol.WriterFeatures!);
        Assert.Contains("columnMapping", snapshot.Protocol.ReaderFeatures!);
        Assert.Contains(feature, snapshot.Protocol.WriterFeatures!);
        foreach (var field in snapshot.Schema.Fields)
        {
            Assert.True(field.Metadata!.ContainsKey(ColumnMapping.FieldIdKey));
            Assert.True(field.Metadata!.ContainsKey(ColumnMapping.PhysicalNameKey));
            Assert.NotEqual(field.Name, ColumnMapping.GetPhysicalName(field, ColumnMappingMode.Name));
        }
        Assert.True(table.IsIcebergCompat);
    }

    private async Task<long> RowCountAsync(DeltaTable table)
    {
        long rows = 0;
        await foreach (var batch in table.ReadAllAsync())
            rows += batch.Length;
        return rows;
    }

    public static TheoryData<string, string> Versions => new()
    {
        { IcebergCompat.EnableV1Key, "icebergCompatV1" },
        { IcebergCompat.EnableV2Key, "icebergCompatV2" },
        { IcebergCompat.EnableV3Key, "icebergCompatV3" },
    };

    [Theory]
    [MemberData(nameof(Versions))]
    public async Task Create_WithTheModeLeftOut_TurnsOnNameMode(string enableKey, string feature)
    {
        await using (var created = await DeltaTable.CreateAsync(Fs, IdValue,
            configuration: Config((enableKey, "true"))))
        {
            AssertMapped(created, "name", feature);
            await created.WriteAsync([Batch(1, 3)]);
        }

        // Persisted, and the data reads back through the physical names.
        await using var table = await DeltaTable.OpenAsync(Fs);
        AssertMapped(table, "name", feature);
        Assert.Equal(3, await RowCountAsync(table));
    }

    [Theory]
    [MemberData(nameof(Versions))]
    public async Task Create_WithIdMode_KeepsIt(string enableKey, string feature)
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdValue,
            configuration: Config((enableKey, "true"), (ColumnMapping.ModeKey, "id")));
        AssertMapped(table, "id", feature);
    }

    [Fact]
    public async Task Create_WithIdModeArgument_KeepsIt()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdValue,
            columnMappingMode: ColumnMappingMode.Id,
            configuration: Config((IcebergCompat.EnableV2Key, "true")));
        AssertMapped(table, "id", "icebergCompatV2");
    }

    // Spark: WRONG_REQUIRED_TABLE_PROPERTY. Only the property can say "off"; nothing is written.
    [Theory]
    [MemberData(nameof(Versions))]
    public async Task Create_WithModeNone_IsRefused(string enableKey, string feature)
    {
        _ = feature;
        await RefusedAsync(async () => await DeltaTable.CreateAsync(Fs, IdValue,
            configuration: Config((enableKey, "true"), (ColumnMapping.ModeKey, "none"))));
        Assert.Equal(-1, await new TransactionLog(Fs).GetLatestVersionAsync());
    }

    // A property that is not 'true' enables nothing, so it implies no column mapping either.
    [Fact]
    public async Task Create_WithIcebergCompatFalse_TurnsOnNothing()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdValue,
            configuration: Config((IcebergCompat.EnableV2Key, "false")));
        Assert.False(table.CurrentSnapshot.Metadata.Configuration!.ContainsKey(ColumnMapping.ModeKey));
        Assert.False(table.IsIcebergCompat);
    }

    // REPLACE of an unmapped table with data: Spark sets 'name' (measured on all three lanes).
    [Theory]
    [MemberData(nameof(Versions))]
    public async Task Replace_OfAnUnmappedTable_TurnsOnNameMode(string enableKey, string feature)
    {
        await using (var original = await DeltaTable.CreateAsync(Fs, IdValue))
            await original.WriteAsync([Batch(1, 3)]);

        await using var replaced = await DeltaTable.CreateOrReplaceAsync(Fs, IdValue, [Batch(10, 2)],
            configuration: Config((enableKey, "true")));
        AssertMapped(replaced, "name", feature);
        Assert.Equal(2, await RowCountAsync(replaced));
    }

    // REPLACE of a table that already maps columns keeps going from its high-water mark, as with an explicit mode.
    [Fact]
    public async Task Replace_OfAMappedTable_ContinuesTheColumnIds()
    {
        await using (var original = await DeltaTable.CreateAsync(Fs, IdValue,
            columnMappingMode: ColumnMappingMode.Name))
        {
            Assert.Equal("2", original.CurrentSnapshot.Metadata.Configuration![ColumnMapping.MaxColumnIdKey]);
        }

        await using var replaced = await DeltaTable.CreateOrReplaceAsync(Fs, IdValue, [],
            configuration: Config((IcebergCompat.EnableV2Key, "true")));
        AssertMapped(replaced, "name", "icebergCompatV2");
        Assert.Equal("4", replaced.CurrentSnapshot.Metadata.Configuration![ColumnMapping.MaxColumnIdKey]);
    }

    // Spark refuses an ALTER that turns IcebergCompat on over an unmapped table unless the same ALTER sets the mode
    // (autoEnableOnExistingTable = false). EW's ALTER is a commit with caller-supplied metadata, and still refuses.
    [Fact]
    public async Task EnablingIcebergCompat_OnAnExistingUnmappedTable_IsStillRefused()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdValue);
        await table.WriteAsync([Batch(1, 3)]);
        long version = table.CurrentSnapshot.Version;

        var metadata = table.CurrentSnapshot.Metadata;
        var enabled = metadata with
        {
            Configuration = (metadata.Configuration ?? new Dictionary<string, string>())
                .Append(new KeyValuePair<string, string>(IcebergCompat.EnableV2Key, "true"))
                .ToDictionary(pair => pair.Key, pair => pair.Value),
        };
        var protocol = table.CurrentSnapshot.Protocol;
        var withFeature = protocol with
        {
            MinReaderVersion = 3,
            MinWriterVersion = 7,
            ReaderFeatures = (protocol.ReaderFeatures ?? []).Append("columnMapping").Distinct().ToList(),
            WriterFeatures = (protocol.WriterFeatures ?? ["appendOnly", "invariants"])
                .Append("columnMapping").Append("icebergCompatV2").Distinct().ToList(),
        };

        await RefusedAsync(async () => await table.CommitDataFilesAsync(
            System.Array.Empty<WrittenDataFile>(), extraActions: [withFeature, enabled]));
        Assert.Equal(version, await new TransactionLog(Fs).GetLatestVersionAsync());
    }

    // A pre-assigned schema without ids now meets the implied mapping, and the error says where it came from.
    [Fact]
    public async Task Create_WithAPreAssignedSchemaWithoutIds_SaysTheMappingWasImplied()
    {
        var unmapped = SchemaConverter.FromArrowSchema(IdValue);
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await DeltaTable.CreateAsync(Fs, IdValue,
            configuration: Config((IcebergCompat.EnableV2Key, "true")),
            preAssignedSchema: unmapped));
        Assert.Equal(DeltaTableErrorCodes.InvalidPreAssignedSchema, ex.ErrorCode);
        Assert.Contains("Iceberg compatibility", ex.Message);
        Assert.Equal(-1, await new TransactionLog(Fs).GetLatestVersionAsync());
    }
}

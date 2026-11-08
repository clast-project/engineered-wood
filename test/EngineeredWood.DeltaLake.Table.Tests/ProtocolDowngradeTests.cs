// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A commit's <c>protocol</c> may not support fewer features than the table's (#753), as delta-spark refuses one that
/// fails <c>Protocol.canTransitionTo</c> with <c>DELTA_INVALID_PROTOCOL_DOWNGRADE</c>. Feature sets are compared,
/// not version numbers, so an upgrade that lists what a legacy version implied passes, and so does a lower version
/// that describes the same features.
/// </summary>
public class ProtocolDowngradeTests : IDisposable
{
    private readonly string _tempDir;

    public ProtocolDowngradeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_protocol_downgrade_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private static Apache.Arrow.Schema BuildSchema() => new Apache.Arrow.Schema.Builder()
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
        return new RecordBatch(BuildSchema(), [ids.Build(), values.Build()], count);
    }

    private async Task<List<long>> ReadIdsFreshAsync()
    {
        await using var reader = await DeltaTable.OpenAsync(Fs);
        var ids = new List<long>();
        await foreach (var batch in reader.ReadAllAsync())
        {
            var col = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(col.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    private static ProtocolAction Without(ProtocolAction protocol, string feature, bool reader = true, bool writer = true) =>
        protocol with
        {
            ReaderFeatures = reader ? protocol.ReaderFeatures?.Where(f => f != feature).ToList() : protocol.ReaderFeatures,
            WriterFeatures = writer ? protocol.WriterFeatures?.Where(f => f != feature).ToList() : protocol.WriterFeatures,
        };

    private static Task CommitAsync(DeltaTable table, ProtocolAction protocol) =>
        table.CommitDataFilesAsync(System.Array.Empty<WrittenDataFile>(), extraActions: [protocol]).AsTask();

    /// <summary>Each seam that takes caller actions refuses the protocol and leaves the log where it was.</summary>
    private async Task AssertEverySeamRefusesAsync(DeltaTable table, ProtocolAction protocol, string dropped)
    {
        long version = table.CurrentSnapshot.Version;

        var fused = await Assert.ThrowsAsync<DeltaFormatException>(() => CommitAsync(table, protocol));
        Assert.Equal(DeltaTableErrorCodes.InvalidProtocolDowngrade, fused.ErrorCode);
        Assert.Contains(dropped, fused.Message);

        var txn = table.StartTransaction();
        txn.StageActions([protocol]);
        var staged = await Assert.ThrowsAsync<DeltaFormatException>(async () => await txn.CommitAsync());
        Assert.Equal(DeltaTableErrorCodes.InvalidProtocolDowngrade, staged.ErrorCode);

        var clustering = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.SetClusteringColumnsAsync([], extraActions: [protocol]));
        Assert.Equal(DeltaTableErrorCodes.InvalidProtocolDowngrade, clustering.ErrorCode);

        Assert.Equal(version, await new TransactionLog(Fs).GetLatestVersionAsync());
    }

    /// <summary>
    /// The case #753 was filed for: without deletionVectors in the protocol, readers stop applying the table's
    /// deletion vectors, and every row a DELETE removed comes back.
    /// </summary>
    [Fact]
    public async Task DroppingDeletionVectors_IsRefused_AndTheDeletedRowsStayDeleted()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 5)]);
        await table.DeleteAsync(Ex.Equal("id", 3L));

        await AssertEverySeamRefusesAsync(
            table, Without(table.CurrentSnapshot.Protocol, DeletionVectorConfig.FeatureName), "deletionVectors");
        Assert.Equal([1, 2, 4, 5], await ReadIdsFreshAsync());
    }

    // Readers decide from the reader list, so dropping a reader-writer feature there alone is a downgrade too.
    [Fact]
    public async Task DroppingDeletionVectors_FromTheReaderFeaturesOnly_IsRefused()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), enableDeletionVectors: true);

        await AssertEverySeamRefusesAsync(
            table,
            Without(table.CurrentSnapshot.Protocol, DeletionVectorConfig.FeatureName, writer: false),
            "deletionVectors");
    }

    [Fact]
    public async Task DroppingColumnMapping_IsRefused()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(),
            columnMappingMode: ColumnMappingMode.Name, enableDeletionVectors: true);
        Assert.Contains("columnMapping", table.CurrentSnapshot.Protocol.WriterFeatures!);

        await AssertEverySeamRefusesAsync(
            table, Without(table.CurrentSnapshot.Protocol, "columnMapping"), "columnMapping");
    }

    // A legacy version stands for the features it implies: (1, 2) supports appendOnly and invariants.
    [Fact]
    public async Task DroppingAFeatureALegacyVersionImplied_IsRefused()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        Assert.Equal((1, 2), (table.CurrentSnapshot.Protocol.MinReaderVersion, table.CurrentSnapshot.Protocol.MinWriterVersion));

        await AssertEverySeamRefusesAsync(
            table,
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["appendOnly"] },
            "invariants");
        await AssertEverySeamRefusesAsync(
            table, new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 1 }, "appendOnly, invariants");
    }

    // Moving a legacy protocol to table features, listing what it implied plus a new feature, is an upgrade.
    [Fact]
    public async Task UpgradingALegacyProtocolToTableFeatures_Commits()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());

        await CommitAsync(table, new ProtocolAction
        {
            MinReaderVersion = 3,
            MinWriterVersion = 7,
            ReaderFeatures = [DeletionVectorConfig.FeatureName],
            WriterFeatures = ["appendOnly", "invariants", DeletionVectorConfig.FeatureName],
        });

        Assert.Contains(DeletionVectorConfig.FeatureName, table.CurrentSnapshot.Protocol.ReaderFeatures!);
    }

    // Feature sets are compared, not versions, as in Spark: (1, 7) listing exactly what (1, 2) implies can go back.
    [Fact]
    public async Task ALowerVersionDescribingTheSameFeatures_Commits()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await CommitAsync(table, new ProtocolAction
        {
            MinReaderVersion = 1,
            MinWriterVersion = 7,
            WriterFeatures = ["appendOnly", "invariants"],
        });

        await CommitAsync(table, new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 });

        Assert.Equal(2, table.CurrentSnapshot.Protocol.MinWriterVersion);
        Assert.Null(table.CurrentSnapshot.Protocol.WriterFeatures);
    }

    // columnMapping needs writer 5 as well as reader 2, so a (2, 2) table does not support it and can drop to (1, 2).
    [Fact]
    public async Task AReaderVersionWithoutTheWriterSide_ImpliesNoReaderFeature()
    {
        await new TransactionLog(Fs).WriteCommitAsync(0,
        [
            new ProtocolAction { MinReaderVersion = 2, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "reader-two",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}""",
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>(),
            },
        ]);
        await using var table = await DeltaTable.OpenAsync(Fs);

        await CommitAsync(table, new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 });

        Assert.Equal(1, table.CurrentSnapshot.Protocol.MinReaderVersion);
    }
}

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
/// IcebergCompat and deletion vectors on an existing table (#744). delta-spark's <c>CheckDeletionVectorDisabled</c>,
/// shared by V1 and V2, runs on every commit to an IcebergCompat table. It refuses the commit when the protocol
/// supports deletion vectors and they are writable (supported and <c>delta.enableDeletionVectors</c>) before the
/// commit or after it. Creation is covered in <see cref="CreateTimeFeatureEnablementTests"/>.
/// </summary>
public class IcebergCompatDeletionVectorTests : IDisposable
{
    private readonly string _tempDir;

    public IcebergCompatDeletionVectorTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_iceberg_dv_{Guid.NewGuid():N}");
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

    private static RecordBatch SetValue(RecordBatch batch)
    {
        int index = batch.Schema.GetFieldIndex("value");
        var x = new StringArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            x.Append("x");
        var columns = Enumerable.Range(0, batch.ColumnCount)
            .Select(c => c == index ? x.Build() : batch.Column(c)).ToArray();
        return new RecordBatch(batch.Schema, columns, batch.Length);
    }

    /// <summary>Every file outside the log: data files and deletion vectors alike.</summary>
    private string[] FilesOnDisk() =>
        [.. Directory.GetFiles(_tempDir, "*", SearchOption.AllDirectories)
            .Where(p => !p.Contains("_delta_log", StringComparison.Ordinal))
            .OrderBy(p => p, StringComparer.Ordinal)];

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

    private static ProtocolAction WithWriterFeature(ProtocolAction protocol, string feature) =>
        protocol with { WriterFeatures = [.. protocol.WriterFeatures!, feature] };

    private static ProtocolAction WithDeletionVectors(ProtocolAction protocol) => protocol with
    {
        MinReaderVersion = 3,
        ReaderFeatures = [.. protocol.ReaderFeatures ?? [], DeletionVectorConfig.FeatureName],
        WriterFeatures = [.. protocol.WriterFeatures!, DeletionVectorConfig.FeatureName],
    };

    /// <summary>
    /// A column-mapped table with deletion vectors enabled and two data files, which a later commit this library
    /// did not write turns into an IcebergCompatV2 table (an older EW wrote such tables).
    /// </summary>
    private async Task CreateV2TableWithDeletionVectorsAsync()
    {
        long version;
        MetadataAction metadata;
        ProtocolAction protocol;
        await using (var table = await DeltaTable.CreateAsync(Fs, BuildSchema(),
            columnMappingMode: ColumnMappingMode.Name, enableDeletionVectors: true))
        {
            await table.WriteAsync([Batch(1, 5)]);
            await table.WriteAsync([Batch(10, 5)]);
            version = table.CurrentSnapshot.Version;
            metadata = table.CurrentSnapshot.Metadata;
            protocol = table.CurrentSnapshot.Protocol;
        }

        await new TransactionLog(Fs).WriteCommitAsync(version + 1,
        [
            WithWriterFeature(protocol, "icebergCompatV2"),
            WithConfiguration(metadata, IcebergCompat.EnableV2Key, "true"),
        ]);
    }

    /// <summary>
    /// Every commit path refuses a V2 table whose deletion vectors are enabled, a DELETE and an UPDATE included,
    /// though neither carries a metaData or a protocol. Each refusal leaves the log where it was and collects the
    /// files the operation wrote.
    /// </summary>
    [Fact]
    public async Task EveryWrite_ToAnIcebergCompatV2TableWithDeletionVectorsEnabled_IsRefused()
    {
        await CreateV2TableWithDeletionVectorsAsync();
        await using var table = await DeltaTable.OpenAsync(Fs);
        long version = table.CurrentSnapshot.Version;
        var before = FilesOnDisk();

        await RefusedAsync(async () => await table.DeleteAsync(Ex.Equal("id", 2L)));
        await RefusedAsync(async () => await table.UpdateAsync(Ex.Equal("id", 11L), SetValue));
        await RefusedAsync(async () => await table.WriteAsync([Batch(100, 1)]));
        await RefusedAsync(async () => await table.WriteAsync([Batch(100, 1)], DeltaWriteMode.Overwrite));
        await RefusedAsync(async () =>
            await table.CommitDataFilesAsync(System.Array.Empty<WrittenDataFile>()));

        var txn = table.StartTransaction();
        await txn.DeleteAsync(Ex.Equal("id", 3L));
        await RefusedAsync(async () => await txn.CommitAsync());
        await txn.AbortAsync();

        Assert.Equal(version, await new TransactionLog(Fs).GetLatestVersionAsync());
        Assert.Equal(before, FilesOnDisk());
    }

    // Caller-supplied actions can make a V2 table with writable deletion vectors from either side, so each seam that
    // takes them is checked: the fused commit, a staged transaction, and a clustering change.
    [Fact]
    public async Task EnablingIcebergCompatV2_OnATableWithDeletionVectorsEnabled_IsRefused()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(),
            columnMappingMode: ColumnMappingMode.Name,
            enableDeletionVectors: true);

        await AssertEverySeamRefusesAsync(table,
        [
            WithWriterFeature(table.CurrentSnapshot.Protocol, "icebergCompatV2"),
            WithConfiguration(table.CurrentSnapshot.Metadata, IcebergCompat.EnableV2Key, "true"),
        ]);
    }

    // Spark judges the state before the commit too: turning the property off in the same commit does not help.
    [Fact]
    public async Task EnablingIcebergCompatV2_WhileTurningDeletionVectorsOff_IsRefused()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(),
            columnMappingMode: ColumnMappingMode.Name,
            enableDeletionVectors: true);
        var metadata = WithConfiguration(table.CurrentSnapshot.Metadata, IcebergCompat.EnableV2Key, "true");

        await AssertEverySeamRefusesAsync(table,
        [
            WithWriterFeature(table.CurrentSnapshot.Protocol, "icebergCompatV2"),
            WithConfiguration(metadata, DeletionVectorConfig.EnableKey, "false"),
        ]);
    }

    [Fact]
    public async Task EnablingDeletionVectors_OnAnIcebergCompatV2Table_IsRefused()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(),
            columnMappingMode: ColumnMappingMode.Name,
            configuration: new Dictionary<string, string> { [IcebergCompat.EnableV2Key] = "true" });

        await AssertEverySeamRefusesAsync(table,
        [
            WithDeletionVectors(table.CurrentSnapshot.Protocol),
            WithConfiguration(table.CurrentSnapshot.Metadata, DeletionVectorConfig.EnableKey, "true"),
        ]);
    }

    /// <summary>
    /// The feature without the property is not writable, so Spark accepts it on a V2 table, and so does this
    /// library. With the property off, EW does not write deletion vectors, so the table stays writable.
    /// </summary>
    [Fact]
    public async Task DeclaringTheDeletionVectorFeature_OnAnIcebergCompatV2Table_WithThePropertyOff_Commits()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(),
            columnMappingMode: ColumnMappingMode.Name,
            configuration: new Dictionary<string, string> { [IcebergCompat.EnableV2Key] = "true" });
        long version = table.CurrentSnapshot.Version;

        await table.CommitDataFilesAsync(
            System.Array.Empty<WrittenDataFile>(),
            extraActions: [WithDeletionVectors(table.CurrentSnapshot.Protocol)]);
        Assert.Contains(DeletionVectorConfig.FeatureName, table.CurrentSnapshot.Protocol.WriterFeatures!);

        await table.WriteAsync([Batch(1, 3)]);
        await table.DeleteAsync(Ex.LessThanOrEqual("id", 3L));
        Assert.Equal(version + 3, table.CurrentSnapshot.Version);
    }

    private async Task AssertEverySeamRefusesAsync(DeltaTable table, IReadOnlyList<DeltaAction> actions)
    {
        long version = table.CurrentSnapshot.Version;

        await RefusedAsync(async () =>
            await table.CommitDataFilesAsync(System.Array.Empty<WrittenDataFile>(), extraActions: actions));

        var txn = table.StartTransaction();
        txn.StageActions(actions);
        await RefusedAsync(async () => await txn.CommitAsync());

        await RefusedAsync(async () => await table.SetClusteringColumnsAsync([], extraActions: actions));

        Assert.Equal(version, await new TransactionLog(Fs).GetLatestVersionAsync());
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A table with a generated column declares the <c>generatedColumns</c> writer feature (#582): writer version 4 on a
/// legacy protocol, the named feature on a table-features one. A writer that honours only the protocol, as Spark
/// does, otherwise neither computes nor checks the column.
/// </summary>
public class GeneratedColumnsProtocolTests : IDisposable
{
    private readonly string _tempDir;

    public GeneratedColumnsProtocolTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_gencol_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Field Generated(string name, string expression) =>
        new(name, Int64Type.Default, true,
            new Dictionary<string, string> { ["delta.generationExpression"] = expression });

    private static Apache.Arrow.Schema GeneratedSchema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(Generated("doubled", "id * 2"))
        .Build();

    private static Apache.Arrow.Schema PlainSchema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch Ids(params long[] ids)
    {
        var builder = new Int64Array.Builder();
        foreach (long id in ids)
            builder.Append(id);
        return new RecordBatch(PlainSchema(), [builder.Build()], ids.Length);
    }

    private static async Task<long[]> DoubledAsync(DeltaTable table)
    {
        var values = new List<long>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var column = (Int64Array)batch.Column("doubled");
            for (int i = 0; i < column.Length; i++)
                values.Add(column.GetValue(i)!.Value);
        }
        values.Sort();
        return values.ToArray();
    }

    [Fact]
    public async Task Create_GeneratedColumn_LegacyProtocolIsWriterFour()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), GeneratedSchema());

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((1, 4), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        Assert.Null(protocol.ReaderFeatures);
        Assert.Null(protocol.WriterFeatures);

        // EW still computes the column on the raised protocol.
        await table.WriteAsync([Ids(1, 2)]);
        Assert.Equal(new long[] { 2, 4 }, await DoubledAsync(table));
    }

    [Fact]
    public async Task Create_GeneratedColumn_TableFeaturesListGeneratedColumns()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), GeneratedSchema(), enableDeletionVectors: true);

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((3, 7), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        // Writer 4's implied features, as the legacy version would have carried them.
        Assert.Contains("generatedColumns", protocol.WriterFeatures!);
        Assert.Contains("changeDataFeed", protocol.WriterFeatures!);
        Assert.Contains("checkConstraints", protocol.WriterFeatures!);
        Assert.DoesNotContain("generatedColumns", protocol.ReaderFeatures!);

        await table.WriteAsync([Ids(3)]);
        Assert.Equal(new long[] { 6 }, await DoubledAsync(table));
    }

    [Fact]
    public async Task Create_GeneratedColumn_WithColumnMapping_StaysWriterFive()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), GeneratedSchema(), columnMappingMode: ColumnMappingMode.Name);

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((2, 5), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        Assert.Null(protocol.WriterFeatures);
    }

    [Fact]
    public async Task Create_NoGeneratedColumn_StaysWriterTwo()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), PlainSchema());

        Assert.Equal(2, table.CurrentSnapshot.Protocol.MinWriterVersion);
    }

    [Fact]
    public async Task AddColumn_GeneratedColumn_RaisesLegacyProtocolInTheSameCommit()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), PlainSchema());

        long version = await table.AddColumnAsync(Generated("doubled", "id * 2"));

        var atVersion = await table.GetSnapshotAtVersionAsync(version);
        Assert.Equal((1, 4), (atVersion.Protocol.MinReaderVersion, atVersion.Protocol.MinWriterVersion));
        Assert.Null(atVersion.Protocol.WriterFeatures);
    }

    [Fact]
    public async Task AddColumn_GeneratedColumn_NamesTheFeatureOnTableFeatures()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), PlainSchema(), enableDeletionVectors: true);
        Assert.DoesNotContain("generatedColumns", table.CurrentSnapshot.Protocol.WriterFeatures!);

        await table.AddColumnAsync(Generated("doubled", "id * 2"));

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((3, 7), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        Assert.Contains("generatedColumns", protocol.WriterFeatures!);
        Assert.Contains(DeletionVectors.DeletionVectorConfig.FeatureName, protocol.ReaderFeatures!);
        Assert.DoesNotContain("generatedColumns", protocol.ReaderFeatures!);
    }

    [Fact]
    public async Task AddColumn_GeneratedColumn_AlongsideTimestampNtzUpgrade_DeclaresBoth()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), PlainSchema());
        var ts = new Field("ts", new TimestampType(TimeUnit.Microsecond, (string?)null), true,
            new Dictionary<string, string> { ["delta.generationExpression"] = "CAST(NULL AS TIMESTAMP_NTZ)" });

        await table.AddColumnAsync(ts);

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((3, 7), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        Assert.Contains("timestampNtz", protocol.WriterFeatures!);
        Assert.Contains("generatedColumns", protocol.WriterFeatures!);
    }

    [Fact]
    public async Task AddColumn_PlainColumn_LeavesTheProtocolAlone()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), PlainSchema());

        await table.AddColumnAsync(new Field("name", StringType.Default, true));

        Assert.Equal(2, table.CurrentSnapshot.Protocol.MinWriterVersion);
    }

    [Fact]
    public async Task AddColumn_GeneratedColumn_AlreadyWriterFour_NoUpgrade()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), GeneratedSchema());
        var before = table.CurrentSnapshot.Protocol;

        await table.AddColumnAsync(Generated("tripled", "id * 3"));

        Assert.Equal(before, table.CurrentSnapshot.Protocol);
    }

    [Fact]
    public async Task SetSchema_IntroducingAGeneratedColumn_RaisesTheProtocol()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), PlainSchema());

        await table.SetSchemaAsync(GeneratedSchema());

        Assert.Equal(4, table.CurrentSnapshot.Protocol.MinWriterVersion);
    }
}

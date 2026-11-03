// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A table with a generated column declares the <c>generatedColumns</c> writer feature (#582), named at writer 7 as
/// Spark 4.0.0 names it (measured: <c>[generatedColumns, invariants, appendOnly]</c> for a default table), or implied
/// by a legacy writer version of 4 to 6 the table already has. A writer that honours only the protocol, as Spark
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
    public async Task Create_GeneratedColumn_NamesTheFeatureAsSparkDoes()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), GeneratedSchema());

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((1, 7), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        Assert.Null(protocol.ReaderFeatures);
        AssertSet(["generatedColumns", "invariants", "appendOnly"], protocol.WriterFeatures);

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
        // Named, without the changeDataFeed / checkConstraints a raise to writer 4 would have implied.
        AssertSet(["deletionVectors", "generatedColumns", "invariants", "appendOnly"], protocol.WriterFeatures);
        AssertSet(["deletionVectors"], protocol.ReaderFeatures);

        await table.WriteAsync([Ids(3)]);
        Assert.Equal(new long[] { 6 }, await DoubledAsync(table));
    }

    private static void AssertSet(string[] expected, IReadOnlyCollection<string>? actual)
    {
        Assert.NotNull(actual);
        Assert.Equal(expected.OrderBy(f => f, StringComparer.Ordinal), actual!.OrderBy(f => f, StringComparer.Ordinal));
    }

    // Measured on Spark 4.0.0 through the DeltaTable builder: with an explicit floor the feature is named, and nothing
    // the floor does not imply comes with it.
    [Theory]
    [InlineData("1", false, 1, null, "generatedColumns")]
    [InlineData("7", false, 1, null, "generatedColumns")]
    [InlineData("3", false, 1, null, "generatedColumns,checkConstraints,invariants,appendOnly")]
    [InlineData("7", true, 3, "deletionVectors", "deletionVectors,generatedColumns")]
    public async Task Create_GeneratedColumn_WithAWriterFloor_NamesOnlyWhatIsNeeded(
        string floor, bool deletionVectors, int reader, string? readerFeatures, string writerFeatures)
    {
        // A nullable id: a NOT NULL one would also need invariants.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(Generated("doubled", "id * 2"))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema, enableDeletionVectors: deletionVectors,
            configuration: new Dictionary<string, string> { ["delta.minWriterVersion"] = floor });

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((reader, 7), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        if (readerFeatures is null)
            Assert.Null(protocol.ReaderFeatures);
        else
            AssertSet(readerFeatures.Split(','), protocol.ReaderFeatures);
        AssertSet(writerFeatures.Split(','), protocol.WriterFeatures);

        // EW still computes the column.
        var ids = new Int64Array.Builder().Append(5).Build();
        var idOnly = new Apache.Arrow.Schema.Builder().Field(new Field("id", Int64Type.Default, true)).Build();
        await table.WriteAsync([new RecordBatch(idOnly, [ids], 1)]);
        Assert.Equal(new long[] { 10 }, await DoubledAsync(table));
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
    public async Task AddColumn_GeneratedColumn_NamesTheFeatureInTheSameCommit()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), PlainSchema());

        long version = await table.AddColumnAsync(Generated("doubled", "id * 2"));

        var atVersion = await table.GetSnapshotAtVersionAsync(version);
        Assert.Equal((1, 7), (atVersion.Protocol.MinReaderVersion, atVersion.Protocol.MinWriterVersion));
        AssertSet(["generatedColumns", "invariants", "appendOnly"], atVersion.Protocol.WriterFeatures);
    }

    [Fact]
    public async Task AddColumn_GeneratedColumn_OnALegacyWriterFourTable_NoUpgrade()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), PlainSchema(),
            configuration: new Dictionary<string, string> { ["delta.minWriterVersion"] = "4" });
        var before = table.CurrentSnapshot.Protocol;
        Assert.Equal(4, before.MinWriterVersion);

        await table.AddColumnAsync(Generated("doubled", "id * 2"));

        Assert.Equal(before, table.CurrentSnapshot.Protocol);
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

        Assert.Contains("generatedColumns", table.CurrentSnapshot.Protocol.WriterFeatures!);
    }
}

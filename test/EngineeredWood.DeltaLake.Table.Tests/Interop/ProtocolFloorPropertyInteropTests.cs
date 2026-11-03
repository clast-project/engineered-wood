// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <c>delta.minReaderVersion</c> / <c>delta.minWriterVersion</c> against Spark (#639). Twin tables are created with the
/// same table properties by this library and by Spark SQL, and must end with the same protocol and the same
/// configuration.
/// </summary>
[Collection("Interop")]
public class ProtocolFloorPropertyInteropTests : IDisposable
{
    private readonly string _ours;
    private readonly string _theirs;

    public ProtocolFloorPropertyInteropTests()
    {
        string root = Path.Combine(Path.GetTempPath(), $"delta_floor_interop_{Guid.NewGuid():N}");
        _ours = Path.Combine(root, "ours");
        _theirs = Path.Combine(root, "theirs");
        Directory.CreateDirectory(_ours);
        Directory.CreateDirectory(_theirs);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        string root = Path.GetDirectoryName(_ours)!;
        if (Directory.Exists(root))
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema IdSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, true))
        .Build();

    private static JsonElement Sql(string dir, params string[] statements) =>
        Spark.InvokeRaw("sql", new { path = dir, sql = statements });

    private static async Task<Snapshot.Snapshot> SnapshotAsync(string dir)
    {
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(dir));
        return table.CurrentSnapshot;
    }

    private static List<string>? Sorted(IEnumerable<string>? items) =>
        items?.OrderBy(f => f, StringComparer.Ordinal).ToList();

    [SkippableTheory]
    [InlineData("delta.minWriterVersion=5")]
    [InlineData("delta.minWriterVersion=6")]
    [InlineData("delta.minWriterVersion=7")]
    [InlineData("delta.minReaderVersion=2;delta.minWriterVersion=5")]
    [InlineData("delta.minReaderVersion=2;delta.minWriterVersion=6")]
    [InlineData("delta.minReaderVersion=3")]
    [InlineData("delta.minWriterVersion=4;delta.feature.deletionVectors=supported")]
    [InlineData("delta.minWriterVersion=7;delta.feature.checkConstraints=supported")]
    [InlineData("delta.minWriterVersion=4;delta.enableChangeDataFeed=true")]
    [InlineData("delta.minWriterVersion=6;delta.enableDeletionVectors=true")]
    [InlineData("delta.minWriterVersion=7;delta.columnMapping.mode=name")]
    [InlineData("delta.minWriterVersion=4;delta.columnMapping.mode=name")]
    [InlineData("delta.minWriterVersion=1;delta.appendOnly=true")]
    [InlineData("delta.minWriterVersion=1;delta.constraints.c=id > 0")]
    [InlineData("delta.minWriterVersion=7;delta.enableRowTracking=true")]
    [InlineData("delta.enableTypeWidening=true")]
    [InlineData("delta.enableTypeWidening=false")]
    [InlineData("delta.minWriterVersion=7;delta.enableTypeWidening=true")]
    [InlineData("delta.minWriterVersion=1;delta.enableTypeWidening=true")]
    [InlineData("delta.enableTypeWidening=true;delta.enableDeletionVectors=true")]
    public async Task SameProtocolAndConfigurationAsSpark(string properties)
    {
        Spark.Require();
        var configuration = ProtocolFloorPropertyTests.Parse(properties);

        await using (await DeltaTable.CreateAsync(new LocalTableFileSystem(_ours), IdSchema, configuration: configuration))
        {
        }

        string tblProperties = string.Join(", ", configuration.Select(kv => $"'{kv.Key}' = '{kv.Value}'"));
        var created = Sql(_theirs, $"CREATE TABLE delta.`{{path}}` (id BIGINT) USING delta TBLPROPERTIES ({tblProperties})");
        Assert.True(created.GetProperty("ok").GetBoolean(), created.ToString());

        var ours = await SnapshotAsync(_ours);
        var theirs = await SnapshotAsync(_theirs);
        Assert.Equal(
            (theirs.Protocol.MinReaderVersion, theirs.Protocol.MinWriterVersion),
            (ours.Protocol.MinReaderVersion, ours.Protocol.MinWriterVersion));
        Assert.Equal(Sorted(theirs.Protocol.ReaderFeatures), Sorted(ours.Protocol.ReaderFeatures));
        Assert.Equal(Sorted(theirs.Protocol.WriterFeatures), Sorted(ours.Protocol.WriterFeatures));
        // Generated values (column-mapping ids, row-tracking column names) differ; the keys must not.
        Assert.Equal(
            Sorted(theirs.Metadata.Configuration?.Keys) ?? [],
            Sorted(ours.Metadata.Configuration?.Keys) ?? []);
    }

    private async Task AssertSameProtocolAsync()
    {
        var ours = (await SnapshotAsync(_ours)).Protocol;
        var theirs = (await SnapshotAsync(_theirs)).Protocol;
        Assert.Equal((theirs.MinReaderVersion, theirs.MinWriterVersion), (ours.MinReaderVersion, ours.MinWriterVersion));
        Assert.Equal(Sorted(theirs.ReaderFeatures), Sorted(ours.ReaderFeatures));
        Assert.Equal(Sorted(theirs.WriterFeatures), Sorted(ours.WriterFeatures));
    }

    /// <summary>A NOT NULL column is an invariant: a floor that does not imply invariants names the feature.</summary>
    [SkippableTheory]
    [InlineData(null)]
    [InlineData("1")]
    [InlineData("7")]
    public async Task NotNullColumn_SameProtocolAsSpark(string? floor)
    {
        Spark.Require();
        var configuration = floor is null ? null : new Dictionary<string, string> { ["delta.minWriterVersion"] = floor };
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("id", Int64Type.Default, false)).Build();

        await using (await DeltaTable.CreateAsync(new LocalTableFileSystem(_ours), schema, configuration: configuration))
        {
        }
        string tblProperties = floor is null ? "" : $" TBLPROPERTIES ('delta.minWriterVersion' = '{floor}')";
        var created = Sql(_theirs, $"CREATE TABLE delta.`{{path}}` (id BIGINT NOT NULL) USING delta{tblProperties}");
        Assert.True(created.GetProperty("ok").GetBoolean(), created.ToString());

        await AssertSameProtocolAsync();
    }

    /// <summary>
    /// A generated column's feature is named as Spark names it, with a floor and without one (#582's table was writer 4,
    /// which also claimed changeDataFeed and checkConstraints).
    /// </summary>
    [SkippableTheory]
    [InlineData(null, false)]
    [InlineData("1", false)]
    [InlineData("3", false)]
    [InlineData("7", true)]
    public async Task GeneratedColumn_SameProtocolAsSpark(string? floor, bool deletionVectors)
    {
        Spark.Require();
        var properties = new Dictionary<string, string>();
        if (floor is not null)
            properties["delta.minWriterVersion"] = floor;
        if (deletionVectors)
            properties["delta.enableDeletionVectors"] = "true";
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("d", Int64Type.Default, true,
                new Dictionary<string, string> { ["delta.generationExpression"] = "id * 2" }))
            .Build();

        await using (await DeltaTable.CreateAsync(new LocalTableFileSystem(_ours), schema, configuration: properties))
        {
        }
        Spark.Invoke("create", new
        {
            path = _theirs,
            columns = new object[]
            {
                new { name = "id", type = "BIGINT" },
                new { name = "d", type = "BIGINT", generated_always_as = "id * 2" },
            },
            properties,
        });

        await AssertSameProtocolAsync();
    }

    /// <summary>
    /// A <c>(1, 6)</c> table replaced by one needing table features lists writer 6's features without
    /// <c>columnMapping</c>, which needs reader 2.
    /// </summary>
    [SkippableFact]
    public async Task ReplacingAWriterSixTable_SameProtocolAsSpark()
    {
        Spark.Require();
        var floor = new Dictionary<string, string> { ["delta.minWriterVersion"] = "6" };

        await using (await DeltaTable.CreateAsync(new LocalTableFileSystem(_ours), IdSchema, configuration: floor))
        {
        }
        var ids = new Int64Array.Builder().Append(1).Build();
        await using (await DeltaTable.CreateOrReplaceAsync(
                         new LocalTableFileSystem(_ours), IdSchema, [new RecordBatch(IdSchema, [ids], 1)],
                         enableDeletionVectors: true))
        {
        }

        var replaced = Sql(_theirs,
            "CREATE TABLE delta.`{path}` (id BIGINT) USING delta TBLPROPERTIES ('delta.minWriterVersion' = '6')",
            "REPLACE TABLE delta.`{path}` (id BIGINT) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')");
        Assert.True(replaced.GetProperty("ok").GetBoolean(), replaced.ToString());

        await AssertSameProtocolAsync();
    }

    /// <summary>Spark writes to a table this library created with a floor below its default, and reads it back.</summary>
    [SkippableFact]
    public async Task SparkWritesToAnEwTableWithAWriterOneFloor()
    {
        Spark.Require();

        await using (await DeltaTable.CreateAsync(
                         new LocalTableFileSystem(_ours), IdSchema,
                         configuration: new Dictionary<string, string> { ["delta.minWriterVersion"] = "1" }))
        {
        }

        var result = Spark.Invoke("sql", new { path = _ours, sql = new[] { "INSERT INTO delta.`{path}` VALUES (1), (2)" } });
        Assert.Equal(2, result.GetProperty("row_count").GetInt32());

        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours));
        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((1, 1), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        long rows = 0;
        await foreach (var batch in table.ReadAllAsync())
            rows += batch.Length;
        Assert.Equal(2, rows);
    }
}

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

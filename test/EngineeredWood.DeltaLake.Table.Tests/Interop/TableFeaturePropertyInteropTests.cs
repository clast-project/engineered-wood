// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <c>delta.feature.&lt;name&gt; = supported</c> against Spark (#601). Twin tables are created with the same table
/// properties by this library and by Spark SQL, and must end with the same protocol and the same configuration keys.
/// The materialized row-tracking column names are random, so only their keys are compared.
/// </summary>
[Collection("Interop")]
public class TableFeaturePropertyInteropTests : IDisposable
{
    private readonly string _ours;
    private readonly string _theirs;

    public TableFeaturePropertyInteropTests()
    {
        string root = Path.Combine(Path.GetTempPath(), $"delta_featprop_interop_{Guid.NewGuid():N}");
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

    private static List<string>? Sorted(IReadOnlyCollection<string>? features) =>
        features?.OrderBy(f => f, StringComparer.Ordinal).ToList();

    [SkippableTheory]
    [InlineData("rowTracking")]
    [InlineData("deletionVectors")]
    [InlineData("rowTracking,deletionVectors")]
    [InlineData("checkConstraints")]
    [InlineData("columnMapping")]
    [InlineData("changeDataFeed")]
    [InlineData("icebergCompatV2")]
    [InlineData("v2Checkpoint,inCommitTimestamp")]
    public async Task SameProtocolAndConfigurationAsSpark(string features)
    {
        Spark.Require();
        string[] names = features.Split(',');

        var properties = names.ToDictionary(f => "delta.feature." + f, _ => "supported");
        await using (await DeltaTable.CreateAsync(new LocalTableFileSystem(_ours), IdSchema, configuration: properties))
        {
        }

        string tblProperties = string.Join(", ", names.Select(f => $"'delta.feature.{f}' = 'supported'"));
        var created = Sql(_theirs, $"CREATE TABLE delta.`{{path}}` (id BIGINT) USING delta TBLPROPERTIES ({tblProperties})");
        Assert.True(created.GetProperty("ok").GetBoolean(), created.ToString());

        var ours = await SnapshotAsync(_ours);
        var theirs = await SnapshotAsync(_theirs);
        Assert.Equal(
            (theirs.Protocol.MinReaderVersion, theirs.Protocol.MinWriterVersion),
            (ours.Protocol.MinReaderVersion, ours.Protocol.MinWriterVersion));
        Assert.Equal(Sorted(theirs.Protocol.ReaderFeatures), Sorted(ours.Protocol.ReaderFeatures));
        Assert.Equal(Sorted(theirs.Protocol.WriterFeatures), Sorted(ours.Protocol.WriterFeatures));
        Assert.Equal(
            Sorted(theirs.Metadata.Configuration?.Keys.ToList()) ?? [],
            Sorted(ours.Metadata.Configuration?.Keys.ToList()) ?? []);
    }

    /// <summary>
    /// Spark assigns row ids on a table this library declared row tracking on, and they continue past the ones this
    /// library assigned: the high-water mark and the protocol say the same thing to both writers.
    /// </summary>
    [SkippableFact]
    public async Task SparkContinuesRowIdsOnAnEwTableDeclaredWithTheProperty()
    {
        Spark.Require();

        var fs = new LocalTableFileSystem(_ours);
        await using (var table = await DeltaTable.CreateAsync(
                         fs, IdSchema,
                         configuration: new Dictionary<string, string> { ["delta.feature.rowTracking"] = "supported" }))
        {
            var ids = new Int64Array.Builder().Append(1).Append(2).Build();
            await table.WriteAsync([new RecordBatch(IdSchema, [ids], 2)]);
        }

        var inserted = Sql(_ours, "INSERT INTO delta.`{path}` VALUES (3)");
        Assert.True(inserted.GetProperty("ok").GetBoolean(), inserted.ToString());

        var snapshot = await SnapshotAsync(_ours);
        var baseRowIds = snapshot.ActiveFiles.Values.Select(add => add.BaseRowId).OrderBy(id => id).ToList();
        Assert.Equal(new long?[] { 0, 2 }, baseRowIds);
    }
}

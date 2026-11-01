// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// ADD / DROP CONSTRAINT against Spark (#565). Twin tables get the same DDL from this library and from Spark SQL
/// and must end with the same protocol and configuration; a constraint either side added is enforced by the other;
/// and the two refuse the same violating table with the same count.
/// </summary>
[Collection("Interop")]
public class ConstraintDdlInteropTests : IDisposable
{
    private readonly string _ours;
    private readonly string _theirs;

    public ConstraintDdlInteropTests()
    {
        string root = Path.Combine(Path.GetTempPath(), $"delta_constraint_interop_{Guid.NewGuid():N}");
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

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, true))
        .Field(new Field("v", StringType.Default, true))
        .Build();

    private static RecordBatch Rows(params long?[] ids)
    {
        var id = new Int64Array.Builder();
        var v = new StringArray.Builder();
        foreach (long? i in ids)
        {
            if (i is { } value) id.Append(value); else id.AppendNull();
            v.Append($"v{i}");
        }
        return new RecordBatch(TableSchema, [id.Build(), v.Build()], ids.Length);
    }

    private async Task CreateTwinsAsync(bool deletionVectors, params long?[] ids)
    {
        foreach (string dir in new[] { _ours, _theirs })
        {
            await using var table = await DeltaTable.CreateAsync(
                new LocalTableFileSystem(dir), TableSchema, enableDeletionVectors: deletionVectors);
            if (ids.Length > 0)
                await table.WriteAsync([Rows(ids)]);
        }
    }

    private static async Task<Snapshot.Snapshot> SnapshotAsync(string dir)
    {
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(dir));
        return table.CurrentSnapshot;
    }

    private static void AssertSameProtocol(Actions.ProtocolAction theirs, Actions.ProtocolAction ours)
    {
        Assert.Equal((theirs.MinReaderVersion, theirs.MinWriterVersion), (ours.MinReaderVersion, ours.MinWriterVersion));
        Assert.Equal(Sorted(theirs.ReaderFeatures), Sorted(ours.ReaderFeatures));
        Assert.Equal(Sorted(theirs.WriterFeatures), Sorted(ours.WriterFeatures));

        static string[]? Sorted(IReadOnlyCollection<string>? features) =>
            features?.OrderBy(f => f, StringComparer.Ordinal).ToArray();
    }

    private static JsonElement Sql(string dir, params string[] statements) =>
        Spark.InvokeRaw("sql", new { path = dir, sql = statements });

    private static string Error(JsonElement result)
    {
        Assert.False(result.GetProperty("ok").GetBoolean(), "Spark accepted what it was expected to refuse.");
        return result.GetProperty("error").GetString()!;
    }

    /// <summary>
    /// The same ADD then DROP leaves both twins with the same protocol and configuration, on a legacy table and
    /// on a table-features one.
    /// </summary>
    [SkippableTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AddAndDrop_LeaveWhatSparksLeave(bool deletionVectors)
    {
        Spark.Require();
        await CreateTwinsAsync(deletionVectors, 1, 2);

        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours)))
        {
            await table.AddConstraintAsync("MyCheck", "id > 0");
            await table.AddConstraintAsync("other", "v IS NOT NULL");
        }
        var added = Sql(_theirs,
            "ALTER TABLE delta.`{path}` ADD CONSTRAINT MyCheck CHECK (id > 0)",
            "ALTER TABLE delta.`{path}` ADD CONSTRAINT other CHECK (v IS NOT NULL)");
        Assert.True(added.GetProperty("ok").GetBoolean(), added.ToString());

        var ours = await SnapshotAsync(_ours);
        var theirs = await SnapshotAsync(_theirs);
        AssertSameProtocol(theirs.Protocol, ours.Protocol);
        Assert.Equal(theirs.Metadata.Configuration, ours.Metadata.Configuration);

        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours)))
            await table.DropConstraintAsync("MYCHECK");
        Assert.True(Sql(_theirs, "ALTER TABLE delta.`{path}` DROP CONSTRAINT MYCHECK").GetProperty("ok").GetBoolean());

        ours = await SnapshotAsync(_ours);
        theirs = await SnapshotAsync(_theirs);
        AssertSameProtocol(theirs.Protocol, ours.Protocol);
        Assert.Equal(theirs.Metadata.Configuration, ours.Metadata.Configuration);
    }

    /// <summary>A constraint this library added is one Spark enforces, and Spark's history names it.</summary>
    [SkippableFact]
    public async Task SparkEnforcesAConstraintEwAdded()
    {
        Spark.Require();
        await CreateTwinsAsync(deletionVectors: false, 1);
        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours)))
            await table.AddConstraintAsync("Positive", "id > 0");

        Assert.Contains("DELTA_VIOLATE_CONSTRAINT_WITH_VALUES",
            Error(Sql(_ours, "INSERT INTO delta.`{path}` VALUES (-1, 'x')")));
        var accepted = Sql(_ours, "INSERT INTO delta.`{path}` VALUES (2, 'y')", "DESCRIBE HISTORY delta.`{path}`");
        Assert.True(accepted.GetProperty("ok").GetBoolean(), accepted.ToString());

        var add = accepted.GetProperty("statements")[1].GetProperty("rows").EnumerateArray()
            .Single(r => r.GetProperty("operation").GetString() == "ADD CONSTRAINT");
        var parameters = add.GetProperty("operationParameters");
        Assert.Equal("Positive", parameters.GetProperty("name").GetString());
        Assert.Equal("id > 0", parameters.GetProperty("expr").GetString());
    }

    /// <summary>A constraint Spark added, under its lower-cased key, is one this library enforces and can drop.</summary>
    [SkippableFact]
    public async Task EwEnforcesAndDropsAConstraintSparkAdded()
    {
        Spark.Require();
        await CreateTwinsAsync(deletionVectors: false, 1);
        Assert.True(Sql(_theirs, "ALTER TABLE delta.`{path}` ADD CONSTRAINT Positive CHECK (id > 0)")
            .GetProperty("ok").GetBoolean());

        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_theirs));
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.WriteAsync([Rows(-1)]));
        Assert.Equal(DeltaTableErrorCodes.ConstraintViolated, ex.ErrorCode);

        await table.DropConstraintAsync("POSITIVE");
        await table.WriteAsync([Rows(-1)]);
        Assert.True(Sql(_theirs, "INSERT INTO delta.`{path}` VALUES (-2, 'z')").GetProperty("ok").GetBoolean());
    }

    /// <summary>
    /// Both refuse a constraint the table violates, counting the same rows: a NULL among them, and not a row a
    /// deletion vector hides.
    /// </summary>
    [SkippableFact]
    public async Task BothRefuseAViolatedConstraint_WithTheSameCount()
    {
        Spark.Require();
        await CreateTwinsAsync(deletionVectors: true, 1, -2, -3, null, 5);
        foreach (string dir in new[] { _ours, _theirs })
        {
            await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(dir));
            await table.DeleteAsync(Expressions.Expressions.Equal("id", -3L));
        }

        string sparks = Error(Sql(_theirs, "ALTER TABLE delta.`{path}` ADD CONSTRAINT pos CHECK (id > 0)"));
        Assert.Contains("DELTA_NEW_CHECK_CONSTRAINT_VIOLATION", sparks);
        Assert.Contains("] 2 rows in", sparks);

        await using var ours = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours));
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await ours.AddConstraintAsync("pos", "id > 0"));
        Assert.Equal(DeltaTableErrorCodes.NewCheckConstraintViolation, ex.ErrorCode);
        Assert.StartsWith("2 rows", ex.Message);
    }
}

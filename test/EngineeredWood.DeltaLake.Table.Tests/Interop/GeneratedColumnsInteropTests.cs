// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// Spark enforces the generated column of a table this library created (#582). delta-spark asks the PROTOCOL whether
/// generated columns are supported before it computes or checks one, so a table whose protocol did not declare
/// <c>generatedColumns</c> let Spark commit a value contradicting the expression.
/// </summary>
[Collection("Interop")]
public class GeneratedColumnsInteropTests : IDisposable
{
    private readonly string _tempDir;

    public GeneratedColumnsInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_gencol_interop_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("doubled", Int64Type.Default, true,
            new Dictionary<string, string> { ["delta.generationExpression"] = "id * 2" }))
        .Build();

    /// <summary>
    /// Spark refuses an INSERT whose value for the generated column contradicts the expression, and computes the
    /// column for an INSERT that leaves it out; EW reads back what Spark wrote. On a legacy protocol and on a
    /// table-features one.
    /// </summary>
    [SkippableTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SparkEnforcesTheGeneratedColumnOfAnEwCreatedTable(bool deletionVectors)
    {
        Spark.Require();

        var fs = new LocalTableFileSystem(_tempDir);
        await using (var created = await DeltaTable.CreateAsync(
                         fs, TableSchema, enableDeletionVectors: deletionVectors))
        {
        }

        var contradicting = Spark.InvokeRaw(
            "sql", new { path = _tempDir, sql = new[] { "INSERT INTO delta.`{path}` VALUES (1, 5)" } });
        Assert.False(contradicting.GetProperty("ok").GetBoolean(),
            "Spark committed a generated-column value that contradicts its expression.");

        var computed = Spark.InvokeRaw(
            "sql", new { path = _tempDir, sql = new[] { "INSERT INTO delta.`{path}` (id) VALUES (3)" } });
        Assert.True(computed.GetProperty("ok").GetBoolean(), computed.ToString());

        await using var table = await DeltaTable.OpenAsync(fs);
        var rows = new List<(long Id, long? Doubled)>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            var doubled = (Int64Array)batch.Column("doubled");
            for (int i = 0; i < batch.Length; i++)
                rows.Add((ids.GetValue(i)!.Value, doubled.GetValue(i)));
        }
        Assert.Equal(new[] { (3L, (long?)6L) }, rows);
    }
}

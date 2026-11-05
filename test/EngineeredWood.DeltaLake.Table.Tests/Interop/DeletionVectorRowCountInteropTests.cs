// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3.</b> A file whose statistics lack <c>numRecords</c>, masked by an engineered-wood deletion
/// vector (#705), which now gives it the row count the protocol requires of a file with a deletion vector: Spark
/// must read the table, count it and delete from it again. This shows Spark accepts what is written; it does not
/// pin the fix, since Spark 4.0 / Delta 4.0 also read, counted and deleted through the countless file written
/// before it (measured). <see cref="DeletionVectorRowCountTests"/> pins the count itself. See <see cref="Spark"/>
/// for setup and cost.</para>
/// </summary>
[Collection("Interop")]
public class DeletionVectorRowCountInteropTests : IDisposable
{
    private readonly string _tempDir;

    public DeletionVectorRowCountInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_dv_count_interop_{Guid.NewGuid():N}");
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

    private static readonly Apache.Arrow.Schema IdSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, true))
        .Build();

    [SkippableTheory]
    [InlineData("predicate")]
    [InlineData("rows")]
    public async Task SparkReadsAMaskedFileThatHadNoRowCount(string how)
    {
        Spark.Require();
        await using (var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true))
        {
            await table.WriteAsync(
                [new RecordBatch(IdSchema, [new Int64Array.Builder().AppendRange([1, 2, 3, 4, 5]).Build()], 5)]);
            var file = table.CurrentSnapshot.ActiveFiles.Values.Single();
            await using (var strip = table.StartTransaction())
            {
                strip.StageActions(
                [
                    new RemoveFile
                    {
                        Path = file.Path,
                        DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        DataChange = false,
                    },
                    file with { Stats = null, TypedStats = null, DataChange = false },
                ]);
                await strip.CommitAsync();
            }

            if (how == "predicate")
            {
                await table.DeleteAsync(Ex.Equal("id", LiteralValue.Of(2L)));
            }
            else
            {
                await table.DeleteRowsAsync(RowSelection.ByPath(
                    new Dictionary<string, IReadOnlyCollection<long>> { [file.Path] = [1L] }),
                    RowDeleteMode.DeletionVector);
            }
        }

        var spark = Spark.Invoke("sql", new
        {
            path = _tempDir,
            sql = new[] { "SELECT count(*) AS n FROM delta.`{path}`", "DELETE FROM delta.`{path}` WHERE id = 4" },
        });
        Assert.Equal(4, spark.GetProperty("statements")[0].GetProperty("rows")[0].GetProperty("n").GetInt64());
        Assert.Equal(
            [1L, 3L, 5L],
            spark.GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("id").GetInt64()).OrderBy(id => id).ToList());
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using ArrowStructType = Apache.Arrow.Types.StructType;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3.</b> #466: Spark accepts a CHECK constraint on a struct's field, and EW could not evaluate one,
/// so it refused every write to such a table: a Spark-created table EW could not write at all. See
/// <see cref="Spark"/> for setup and cost.</para>
/// </summary>
[Collection("Interop")]
public class NestedCheckConstraintInteropTests : IDisposable
{
    private readonly string _tempDir;

    public NestedCheckConstraintInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_nestedcheck_interop_{Guid.NewGuid():N}");
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

    [SkippableFact]
    public async Task SparkTableWithANestedCheck_EwEnforcesIt_AndSparkReadsWhatEwWrote()
    {
        Spark.Require();
        Spark.Invoke("create", new
        {
            path = _tempDir,
            columns = new object[] { new { name = "s", type = "STRUCT<a: BIGINT>" } },
        });
        Spark.Invoke("sql", new
        {
            path = _tempDir,
            sql = new[] { "ALTER TABLE delta.`{path}` ADD CONSTRAINT positive CHECK (s.a > 0)" },
        });

        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)))
        {
            var sType = (ArrowStructType)table.ArrowSchema.FieldsList.Single().DataType;
            RecordBatch Row(long a) => new(table.ArrowSchema,
                [new StructArray(sType, 1, [new Int64Array.Builder().Append(a).Build()], ArrowBuffer.Empty, 0)], 1);

            await table.WriteAsync([Row(5)]);
            var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.WriteAsync([Row(-1)]));
            Assert.Equal(DeltaTableErrorCodes.ConstraintViolated, ex.ErrorCode);
        }

        var read = Spark.Invoke("read", new { path = _tempDir });
        Assert.Equal(1, read.GetProperty("row_count").GetInt32());
    }
}

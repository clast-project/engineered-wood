// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using ArrowStructType = Apache.Arrow.Types.StructType;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3.</b> #471: Spark validates <c>delta.dataSkippingStatsColumns</c> against the schema as the first
/// step of every metadata update (<c>OptimisticTransactionImpl.updateMetadataInternal</c>). An entry left naming a
/// column EW dropped or renamed made Spark's next ALTER on the table fail with
/// <c>DELTA_COLUMN_NOT_FOUND_IN_SCHEMA</c>. See <see cref="Spark"/> for setup and cost.</para>
/// </summary>
[Collection("Interop")]
public class DataSkippingStatsColumnsInteropTests : IDisposable
{
    private const string Key = "delta.dataSkippingStatsColumns";
    private readonly string _tempDir;

    public DataSkippingStatsColumnsInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_dsstats_interop_{Guid.NewGuid():N}");
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
    public async Task EwDropsAndRenamesListedColumns_SparkCanStillAlterTheTable()
    {
        Spark.Require();

        var xType = new ArrowStructType([new Field("x", Int64Type.Default, true)]);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("payload", StringType.Default, true))
            .Field(new Field("s", xType, true))
            .Build();
        await using (var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema,
            columnMappingMode: ColumnMappingMode.Name,
            configuration: new Dictionary<string, string> { [Key] = "id,payload,s.x" }))
        {
            var x = new Int64Array.Builder().Append(10).Build();
            await table.WriteAsync([new RecordBatch(schema,
            [
                new Int64Array.Builder().Append(1).Build(),
                new StringArray.Builder().Append("a").Build(),
                new StructArray(xType, 1, [x], ArrowBuffer.Empty, nullCount: 0),
            ], 1)]);

            await table.DropColumnAsync("payload");
            await table.RenameColumnAsync("s", "my struct");
        }

        var result = Spark.Invoke("sql", new
        {
            path = _tempDir,
            sql = new[] { "ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('ew.probe' = '1')" },
        });

        var properties = result.GetProperty("detail").GetProperty("properties");
        Assert.Equal("1", properties.GetProperty("ew.probe").GetString());
        Assert.Equal("id,`my struct`.x", properties.GetProperty(Key).GetString());
        Assert.Equal(1, result.GetProperty("row_count").GetInt32());
    }
}

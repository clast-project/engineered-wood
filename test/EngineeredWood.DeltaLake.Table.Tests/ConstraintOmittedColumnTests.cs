// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using System.Text.Json;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A CHECK constraint sees the row as the table will store it (#594): a nullable column the write leaves out as the
/// NULL it reads back as, and an identity column as the value the table generates. It used to see the batch as the
/// caller built it, and refused a row that satisfies the constraint as unevaluable.
/// </summary>
public class ConstraintOmittedColumnTests : IDisposable
{
    private readonly string _tempDir;

    public ConstraintOmittedColumnTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_constraint_omitted_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private static RecordBatch Ids(params long[] ids)
    {
        var builder = new Int64Array.Builder();
        foreach (long id in ids)
            builder.Append(id);
        return new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("id", Int64Type.Default, false)).Build(),
            [builder.Build()], ids.Length);
    }

    /// <summary>Table <c>(id long NOT NULL, v int)</c>, with one constraint on <c>v</c>.</summary>
    private async Task<DeltaTable> CreateAsync(string constraint)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", Int32Type.Default, true))
            .Build();
        var table = await DeltaTable.CreateAsync(Fs, schema);
        await table.AddConstraintAsync("c", constraint);
        return table;
    }

    /// <summary>The issue's repro.</summary>
    [Fact]
    public async Task AWriteLeavingOutTheColumn_IsJudgedOnItsNull()
    {
        await using var table = await CreateAsync("v IS NULL OR v > 0");

        await table.WriteAsync([Ids(1, 2)]);

        long rows = 0;
        await foreach (var batch in table.ReadAllAsync())
        {
            rows += batch.Length;
            Assert.Equal(batch.Length, batch.Column("v").NullCount);
        }
        Assert.Equal(2, rows);
    }

    [Fact]
    public async Task AConstraintTheNullDoesNotSatisfy_IsAViolation_NotUnevaluable()
    {
        await using var table = await CreateAsync("v > 0");
        long version = table.CurrentSnapshot.Version;

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(() => table.WriteAsync([Ids(1)]).AsTask());

        Assert.Equal(DeltaTableErrorCodes.ConstraintViolated, ex.ErrorCode);
        Assert.Equal(version, table.CurrentSnapshot.Version);
    }

    /// <summary>A field the write leaves out of a nullable struct reads back as NULL too.</summary>
    [Fact]
    public async Task AStructFieldTheWriteLeavesOut_IsJudgedOnItsNull()
    {
        var full = new Apache.Arrow.Types.StructType([new Field("x", Int64Type.Default, true), new Field("y", Int64Type.Default, true)]);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("p", full, true)).Build();
        await using var table = await DeltaTable.CreateAsync(Fs, schema);
        await table.AddConstraintAsync("c", "p.y IS NULL");

        var partial = new Apache.Arrow.Types.StructType([new Field("x", Int64Type.Default, true)]);
        var batch = new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("p", partial, true)).Build(),
            [new StructArray(partial, 1, [new Int64Array.Builder().Append(1).Build()], ArrowBuffer.Empty, 0)], 1);

        await table.WriteAsync([batch]);
    }

    /// <summary>
    /// A SLICED struct that leaves out a child: the backfill rebuilds the struct, keeping its offset, and the
    /// constraint must read the slice's own sibling values, not ones shifted by the offset a second time.
    /// </summary>
    [Theory]
    [InlineData(1, true)]    // x = 2, 3: every row passes x > 1
    [InlineData(0, false)]   // x = 1, 2: the first row fails
    public async Task ASlicedStructThatLeavesOutAField_IsJudgedOnTheSlicesOwnValues(int offset, bool passes)
    {
        var full = new Apache.Arrow.Types.StructType(
            [new Field("x", Int64Type.Default, true), new Field("y", Int64Type.Default, true)]);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("p", full, true)).Build();
        await using var table = await DeltaTable.CreateAsync(Fs, schema);
        await table.AddConstraintAsync("c", "p.x > 1 AND p.y IS NULL");

        var partial = new Apache.Arrow.Types.StructType([new Field("x", Int64Type.Default, true)]);
        var whole = new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("p", partial, true)).Build(),
            [new StructArray(partial, 4,
                [new Int64Array.Builder().Append(1).Append(2).Append(3).Append(0).Build()], ArrowBuffer.Empty, 0)],
            4);
        var sliced = whole.Slice(offset, 2);

        if (passes)
        {
            await table.WriteAsync([sliced]);
            var xs = new List<long>();
            await foreach (var batch in table.ReadAllAsync())
            {
                var p = (StructArray)batch.Column("p");
                var x = (Int64Array)p.Fields[((Apache.Arrow.Types.StructType)p.Data.DataType).GetFieldIndex("x")];
                for (int i = 0; i < batch.Length; i++)
                    xs.Add(x.GetValue(i)!.Value);   // Fields are already sliced by the struct's offset
            }
            Assert.Equal([2L, 3L], xs.OrderBy(v => v).ToList());
        }
        else
        {
            var ex = await Assert.ThrowsAsync<DeltaFormatException>(() => table.WriteAsync([sliced]).AsTask());
            Assert.Equal(DeltaTableErrorCodes.ConstraintViolated, ex.ErrorCode);
        }
    }

    /// <summary>
    /// A write may name a column by its column-mapping physical name. The constraint must judge that column's values:
    /// the backfill alone dropped the physical-named column and backfilled the logical one as NULL, so
    /// <c>v IS NULL OR v &gt; 0</c> passed a stored -1.
    /// </summary>
    [Theory]
    [InlineData(-1, false)]
    [InlineData(5, true)]
    public async Task AColumnNamedByItsPhysicalName_IsJudgedOnItsValues(int v, bool passes)
    {
        var (table, physical) = await ColumnMappedAsync();
        await using (table)
        {
            await table.AddConstraintAsync("c", "v IS NULL OR v > 0");
            var batch = PhysicalBatch(physical, v);

            if (passes)
            {
                await table.WriteAsync([batch]);
                Assert.Equal([v], await ValuesAsync(table));
            }
            else
            {
                var ex = await Assert.ThrowsAsync<DeltaFormatException>(() => table.WriteAsync([batch]).AsTask());
                Assert.Equal(DeltaTableErrorCodes.ConstraintViolated, ex.ErrorCode);
                Assert.Empty(await ValuesAsync(table));
            }
        }
    }

    /// <summary>The replaceWhere check backfills the same way, so it had the same hole.</summary>
    [Fact]
    public async Task AReplaceWhereRowNamedByPhysicalName_IsJudgedOnItsValues()
    {
        var (table, physical) = await ColumnMappedAsync();
        await using (table)
        {
            var ex = await Assert.ThrowsAsync<DeltaFormatException>(() => table.OverwriteWhereAsync(
                [PhysicalBatch(physical, -1)], Ex.Or(Ex.IsNull("v"), Ex.GreaterThan("v", EngineeredWood.Expressions.LiteralValue.Of(0)))).AsTask());
            Assert.Equal(DeltaTableErrorCodes.ReplaceWhereMismatch, ex.ErrorCode);

            await table.OverwriteWhereAsync([PhysicalBatch(physical, 5)], Ex.GreaterThan("v", EngineeredWood.Expressions.LiteralValue.Of(0)));
            Assert.Equal([5], await ValuesAsync(table));
        }
    }

    /// <summary>Table <c>(id long, v int)</c> in name mode, and <c>v</c>'s physical name.</summary>
    private async Task<(DeltaTable Table, string Physical)> ColumnMappedAsync()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("v", Int32Type.Default, true))
            .Build();
        var table = await DeltaTable.CreateAsync(Fs, schema, columnMappingMode: ColumnMappingMode.Name);
        var v = table.CurrentSnapshot.Schema.Fields.Single(f => f.Name == "v");
        string physical = v.Metadata![ColumnMapping.PhysicalNameKey].ToString()!;
        Assert.NotEqual("v", physical);
        return (table, physical);
    }

    private static RecordBatch PhysicalBatch(string physical, int v) => new(
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field(physical, Int32Type.Default, true))
            .Build(),
        [new Int64Array.Builder().Append(1).Build(), new Int32Array.Builder().Append(v).Build()], 1);

    private static async Task<List<int>> ValuesAsync(DeltaTable table)
    {
        var values = new List<int>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var column = (Int32Array)batch.Column("v");
            for (int i = 0; i < batch.Length; i++)
                values.Add(column.GetValue(i)!.Value);
        }
        return values;
    }

    /// <summary>An UPDATE's post-image goes through the same enforcer.</summary>
    [Fact]
    public async Task AnUpdateWhoseUpdaterDropsTheColumn_IsJudgedOnItsNull()
    {
        await using var table = await CreateAsync("v IS NULL OR v > 0");
        await table.WriteAsync([Ids(1)]);

        var (updated, _) = await table.UpdateAsync(
            Ex.Equal("id", 1L),
            batch => new RecordBatch(
                new Apache.Arrow.Schema.Builder().Field(batch.Schema.GetFieldByName("id")).Build(),
                [batch.Column("id")], batch.Length));

        Assert.Equal(1, updated);
    }

    /// <summary>
    /// A constraint on an identity column judges the value the table generates. The append leaves <c>id</c> out, so
    /// before the fix the constraint could not be evaluated at all; with it, the rows the generator numbers 2 and 3
    /// pass <c>id &lt; 4</c> and the next ones, 4 and 5, do not.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AConstraintOnAnIdentityColumn_JudgesTheGeneratedValue(bool byDefault)
    {
        string idMeta = JsonSerializer.Serialize(IdentityColumn.CreateMetadata(1, 1, byDefault));
        string schema = $@"{{""type"":""struct"",""fields"":[{{""name"":""id"",""type"":""long"",""nullable"":true,""metadata"":{idMeta}}},{{""name"":""value"",""type"":""string"",""nullable"":true,""metadata"":{{}}}}]}}";
        await new TransactionLog(Fs).WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["identityColumns"] },
            new MetadataAction { Id = "identity-check", Format = Format.Parquet, SchemaString = schema, PartitionColumns = [] },
        });
        await using var table = await DeltaTable.OpenAsync(Fs);
        await table.AddConstraintAsync("small", "id < 4");

        await table.WriteAsync([Values("a")]);
        await table.WriteAsync([Values("b", "c")]);
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(() => table.WriteAsync([Values("d", "e")]).AsTask());

        Assert.Equal(DeltaTableErrorCodes.ConstraintViolated, ex.ErrorCode);
    }

    private static RecordBatch Values(params string[] values)
    {
        var builder = new StringArray.Builder();
        foreach (string v in values)
            builder.Append(v);
        return new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("value", StringType.Default, true)).Build(),
            [builder.Build()], values.Length);
    }
}

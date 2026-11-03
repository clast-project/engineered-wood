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

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A value supplied for a <c>GENERATED ALWAYS AS IDENTITY</c> column is refused, as Spark refuses it (#595):
/// <c>DELTA_IDENTITY_COLUMNS_EXPLICIT_INSERT_NOT_SUPPORTED</c>. EW used to replace it silently with a generated one,
/// so a caller who meant <c>0</c> got another key with nothing to say so.
///
/// <para>Measured against Spark 4.0.3 / Delta 4.0.0: the refusal is by SCHEMA. A non-null value, an all-NULL
/// column, an empty frame, an int column, an overwrite, a replaceWhere, a MERGE <c>INSERT *</c> and a MERGE insert
/// naming the column were all refused; leaving the column out was the one write that landed (id 1).</para>
/// </summary>
public class IdentityExplicitInsertTests : IDisposable
{
    private readonly string _tempDir;

    public IdentityExplicitInsertTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_identity_explicit_{Guid.NewGuid():N}");
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

    /// <summary>A table whose <c>id</c> is an identity column (start 1, step 1), holding the row ("seed", 1).</summary>
    private async Task<DeltaTable> CreateAsync(bool allowExplicitInsert = false)
    {
        string idMeta = JsonSerializer.Serialize(IdentityColumn.CreateMetadata(1, 1, allowExplicitInsert));
        string schema = $@"{{""type"":""struct"",""fields"":[{{""name"":""id"",""type"":""long"",""nullable"":true,""metadata"":{idMeta}}},{{""name"":""value"",""type"":""string"",""nullable"":true,""metadata"":{{}}}}]}}";
        await new TransactionLog(Fs).WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["identityColumns"] },
            new MetadataAction { Id = "identity", Format = Format.Parquet, SchemaString = schema, PartitionColumns = [] },
        });
        var table = await DeltaTable.OpenAsync(Fs);
        await table.WriteAsync([Values("seed")]);
        return table;
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

    /// <summary>A batch carrying <c>id</c>: one row per id (null for a NULL), each with value "v".</summary>
    private static RecordBatch WithIds(params long?[] ids)
    {
        var idBuilder = new Int64Array.Builder();
        var values = new StringArray.Builder();
        foreach (long? id in ids)
        {
            if (id is { } v) idBuilder.Append(v); else idBuilder.AppendNull();
            values.Append("v");
        }
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("value", StringType.Default, true))
            .Build();
        return new RecordBatch(schema, [idBuilder.Build(), values.Build()], ids.Length);
    }

    private async Task<List<(long? Id, string? Value)>> ReadAsync(DeltaTable table)
    {
        var rows = new List<(long?, string?)>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            var values = (StringArray)batch.Column("value");
            for (int i = 0; i < batch.Length; i++)
                rows.Add((ids.GetValue(i), values.GetString(i)));
        }
        return rows.OrderBy(r => r.Item1).ToList();
    }

    private static async Task AssertRefusedAsync(Func<Task> write)
    {
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(write);
        Assert.Equal(DeltaTableErrorCodes.IdentityColumnExplicitInsertNotSupported, ex.ErrorCode);
        Assert.Contains("'id'", ex.Message);
    }

    public static TheoryData<string> SuppliedShapes => new() { "value", "allNull", "empty" };

    private static RecordBatch Supplied(string shape) => shape switch
    {
        "value" => WithIds(0),
        "allNull" => WithIds(null, null),
        _ => WithIds(),
    };

    [Theory]
    [MemberData(nameof(SuppliedShapes))]
    public async Task AnAppendThatCarriesTheColumn_IsRefused_AndLeavesTheTableAlone(string shape)
    {
        await using var table = await CreateAsync();
        long version = table.CurrentSnapshot.Version;

        await AssertRefusedAsync(() => table.WriteAsync([Supplied(shape)]).AsTask());

        Assert.Equal(version, (await DeltaTable.OpenAsync(Fs)).CurrentSnapshot.Version);
        Assert.Equal([(1L, "seed")], await ReadAsync(table));
    }

    /// <summary>Refused before its type is judged: Spark names the identity column for an int one too.</summary>
    [Fact]
    public async Task AColumnOfAnotherType_IsRefusedAsSupplied()
    {
        await using var table = await CreateAsync();
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, true))
            .Field(new Field("value", StringType.Default, true))
            .Build();
        var batch = new RecordBatch(
            schema, [new Int32Array.Builder().Append(0).Build(), new StringArray.Builder().Append("v").Build()], 1);

        await AssertRefusedAsync(() => table.WriteAsync([batch]).AsTask());
    }

    [Fact]
    public async Task AnOverwrite_IsRefused()
    {
        await using var table = await CreateAsync();

        await AssertRefusedAsync(() => table.WriteAsync([WithIds(0)], DeltaWriteMode.Overwrite).AsTask());
        Assert.Equal([(1L, "seed")], await ReadAsync(table));
    }

    [Fact]
    public async Task AReplaceWhere_IsRefused()
    {
        await using var table = await CreateAsync();

        await AssertRefusedAsync(() => table.OverwriteWhereAsync([WithIds(0)], Ex.Equal("value", "v")).AsTask());
    }

    [Fact]
    public async Task ATransactionsAppend_IsRefused_AndTheTransactionStillCommitsItsOtherWork()
    {
        await using var table = await CreateAsync();
        await using (var txn = table.StartTransaction())
        {
            await AssertRefusedAsync(() => txn.WriteAsync([WithIds(0)]).AsTask());
            await txn.WriteAsync([Values("after")]);
            await txn.CommitAsync();
        }

        Assert.Equal([(1L, "seed"), (2L, "after")], await ReadAsync(table));
    }

    [Fact]
    public async Task AMergeInsertStar_IsRefused()
    {
        await using var table = await CreateAsync();
        var options = new MergeOptions { Keys = ["value"], WhenNotMatched = MergeAction.Insert };

        await AssertRefusedAsync(() => table.MergeAsync([WithIds(0)], options).AsTask());
        await AssertRefusedAsync(() => table.MergeAsync([WithIds((long?)null)], options).AsTask());
        Assert.Equal([(1L, "seed")], await ReadAsync(table));
    }

    [Fact]
    public async Task AMergeInsertNamingTheColumn_IsRefused_AndOneLeavingItOut_Generates()
    {
        await using var table = await CreateAsync();

        await AssertRefusedAsync(() => table.MergeAsync(
            [WithIds(0)],
            new MergeOptions { Keys = ["value"], WhenNotMatched = MergeAction.InsertColumns("id", "value") }).AsTask());
        await table.MergeAsync(
            [WithIds(0)], new MergeOptions { Keys = ["value"], WhenNotMatched = MergeAction.InsertColumns("value") });

        Assert.Equal([(1L, "seed"), (2L, "v")], await ReadAsync(table));
    }

    [Fact]
    public async Task LeavingTheColumnOut_Generates()
    {
        await using var table = await CreateAsync();

        await table.WriteAsync([Values("a", "b")]);

        Assert.Equal([(1L, "seed"), (2L, "a"), (3L, "b")], await ReadAsync(table));
    }

    /// <summary>GENERATED BY DEFAULT is untouched: its supplied values are the caller's to give.</summary>
    [Fact]
    public async Task AByDefaultColumn_StillTakesASuppliedValue()
    {
        await using var table = await CreateAsync(allowExplicitInsert: true);

        await table.WriteAsync([WithIds(10)]);

        Assert.Equal([(1L, "seed"), (10L, "v")], await ReadAsync(table));
    }
}

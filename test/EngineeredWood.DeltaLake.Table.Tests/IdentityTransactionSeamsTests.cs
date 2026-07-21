// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The IDENTITY-column seams for buffered (multi-statement) transactions. The committing write path generates
/// identity values itself; a transaction that eagerly writes files at statement time needs the split form:
///
/// <list type="bullet">
/// <item><see cref="DeltaTable.GenerateIdentityValues"/> — statement-time value generation seeded from the
/// CURRENT snapshot's high-water marks, with <c>chainedHighWaterMarks</c> carrying the transaction's pending
/// marks so values CHAIN across statements without a commit in between;</item>
/// <item><see cref="DeltaTable.GenerateIdentityValuesForSchema"/> — the schema-seeded static form for a table
/// that does NOT exist yet (a buffered CREATE);</item>
/// <item><see cref="DeltaTable.BuildIdentityMetadataAction"/> — folds the final marks into ONE metaData action
/// for the fused commit (a commit must not carry two);</item>
/// <item><c>identityValuesPreGenerated</c> on <see cref="DeltaTable.WriteDataFilesAsync"/> /
/// <see cref="DeltaTable.CommitDataFilesAsync"/> — routes the pre-valued rows through unchanged (regeneration
/// would double-consume the marks).</item>
/// </list>
/// </summary>
public class IdentityTransactionSeamsTests : IDisposable
{
    private readonly string _tempDir;

    public IdentityTransactionSeamsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_idseam_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static string IdentitySchemaString()
    {
        var idMeta = IdentityColumn.CreateMetadata(start: 1, step: 1, allowExplicitInsert: false);
        string idMetaJson = System.Text.Json.JsonSerializer.Serialize(idMeta);
        return $@"{{""type"":""struct"",""fields"":[{{""name"":""id"",""type"":""long"",""nullable"":true,""metadata"":{idMetaJson}}},{{""name"":""value"",""type"":""string"",""nullable"":true,""metadata"":{{}}}}]}}";
    }

    private async Task<DeltaTable> CreateIdentityTableAsync()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                WriterFeatures = ["identityColumns"],
            },
            new MetadataAction
            {
                Id = "id-seams",
                Format = Format.Parquet,
                SchemaString = IdentitySchemaString(),
                PartitionColumns = [],
            },
        });
        return await DeltaTable.OpenAsync(fs);
    }

    /// <summary>An INSERT-shaped batch: the identity column arrives as NULLs for the engine to fill.</summary>
    private static RecordBatch InsertBatch(params string[] values)
    {
        var ids = new Int64Array.Builder();
        var vals = new StringArray.Builder();
        foreach (var v in values)
        {
            ids.AppendNull();
            vals.Append(v);
        }
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("value", StringType.Default, true))
            .Build();
        return new RecordBatch(schema, [ids.Build(), vals.Build()], values.Length);
    }

    private static List<long> IdsOf(IReadOnlyList<RecordBatch> batches)
    {
        var result = new List<long>();
        foreach (var b in batches)
        {
            var ids = (Int64Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
                result.Add(ids.GetValue(i)!.Value);
        }
        return result;
    }

    [Fact]
    public async Task GenerateIdentityValues_ChainsAcrossStatements()
    {
        await using var table = await CreateIdentityTableAsync();

        // statement 1: ids from the snapshot's marks (fresh table → 1, 2, 3)
        var gen1 = table.GenerateIdentityValues([InsertBatch("a", "b", "c")]);
        Assert.Equal(new long[] { 1, 2, 3 }, IdsOf(gen1.Batches));

        // statement 2 in the SAME transaction: the chained marks continue — 4, 5 (no commit in between)
        var gen2 = table.GenerateIdentityValues([InsertBatch("d", "e")], gen1.HighWaterMarks);
        Assert.Equal(new long[] { 4, 5 }, IdsOf(gen2.Batches));
        Assert.Equal(5, gen2.HighWaterMarks["id"]);
    }

    [Fact]
    public async Task FusedIdentityCommit_OneMetadataAction_HwmPersists()
    {
        await using var table = await CreateIdentityTableAsync();

        // two eagerly-written statements, values pre-generated + chained
        var gen1 = table.GenerateIdentityValues([InsertBatch("a", "b", "c")]);
        var gen2 = table.GenerateIdentityValues([InsertBatch("d", "e")], gen1.HighWaterMarks);
        var files1 = await table.WriteDataFilesAsync(gen1.Batches, identityValuesPreGenerated: true);
        var files2 = await table.WriteDataFilesAsync(gen2.Batches, identityValuesPreGenerated: true);

        // the flush: ONE commit carrying both files + ONE metaData action with the FINAL marks
        long committed = await table.CommitDataFilesAsync(
            files1.Concat(files2).ToList(), DeltaWriteMode.Append,
            extraActions: [table.BuildIdentityMetadataAction(gen2.HighWaterMarks)],
            expectedVersion: table.CurrentSnapshot.Version, operation: "TRANSACTION",
            identityValuesPreGenerated: true);
        Assert.Equal(1, committed);

        // 5 distinct ids 1..5, and the persisted high-water mark drives the NEXT (committing) write
        await using var check = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await check.WriteAsync([InsertBatch("f")]); // the normal path regenerates from the stored HWM
        var all = new List<long>();
        await foreach (var b in check.ReadAllAsync())
        {
            var ids = (Int64Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
                all.Add(ids.GetValue(i)!.Value);
        }
        all.Sort();
        Assert.Equal(new long[] { 1, 2, 3, 4, 5, 6 }, all);
    }

    [Fact]
    public void GenerateIdentityValuesForSchema_PendingCreate_ChainsWithoutATable()
    {
        // a buffered CREATE TABLE: no table exists yet — the configs seed from the PARKED schema's
        // delta.identity.* metadata, and the final marks are baked into commit-0 by the flush.
        var schema = DeltaSchemaSerializer.Parse(IdentitySchemaString());

        var gen1 = DeltaTable.GenerateIdentityValuesForSchema(schema, [InsertBatch("a", "b", "c")]);
        Assert.Equal(new long[] { 1, 2, 3 }, IdsOf(gen1.Batches));

        var gen2 = DeltaTable.GenerateIdentityValuesForSchema(schema, [InsertBatch("d", "e")], gen1.HighWaterMarks);
        Assert.Equal(new long[] { 4, 5 }, IdsOf(gen2.Batches));
    }

    [Fact]
    public async Task WriteDataFiles_WithoutPreGeneratedFlag_RejectsIdentityTable()
    {
        // the guard: the write-no-commit path can't generate identity values itself (generation +
        // HWM update are coupled) — un-valued batches must go through the committing path
        await using var table = await CreateIdentityTableAsync();
        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await table.WriteDataFilesAsync([InsertBatch("a")]));
    }
}

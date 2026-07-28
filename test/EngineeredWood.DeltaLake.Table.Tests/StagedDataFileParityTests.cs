// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="DeltaTransaction.StageDataFilesAsync"/> and <see cref="DeltaTransaction.SetOperation"/> — the
/// three things a host that owns its data plane needs from staging that
/// <see cref="DeltaTransaction.StageDataFiles"/> cannot express, and that
/// <see cref="DeltaTable.CommitDataFilesAsync"/> already offered its autocommitting callers: an add born with
/// a deletion vector, an identity table whose values the host generated, and the operation name recorded in
/// the history.
/// </summary>
public class StagedDataFileParityTests : IDisposable
{
    private readonly string _tempDir;

    public StagedDataFileParityTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_stageparity_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema BuildSchema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch Batch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        for (int i = 0; i < count; i++)
            ids.Append(startId + i);
        return new RecordBatch(BuildSchema(), [ids.Build()], count);
    }

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    /// <summary>Deletion vectors on, since an add born with one needs the reader feature declared.</summary>
    private async Task<DeltaTable> CreateAsync()
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), BuildSchema(), enableDeletionVectors: true);
        await table.WriteAsync([Batch(0, 2)]);
        return table;
    }

    private async Task<List<long>> ReadIdsAsync()
    {
        await using var table = await OpenAsync();
        var ids = new List<long>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var col = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(col.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    /// <summary>
    /// Rows inserted and then deleted inside ONE transaction never reach a committed version: the add is
    /// committed with an inline deletion vector rather than being written and then rewritten.
    /// </summary>
    [Fact]
    public async Task StagedFile_BornWithADeletionVector_HidesItsRows()
    {
        await using var table = await CreateAsync();
        var files = await table.WriteDataFilesAsync([Batch(10, 5)]);   // ids 10..14

        var txn = table.StartTransaction();
        await txn.StageDataFilesAsync(
            files,
            deletedPositionsByFileIndex: new Dictionary<int, IReadOnlyCollection<long>>
            {
                [0] = new long[] { 1, 3 },   // hide ids 11 and 13
            });
        await txn.CommitAsync();

        var ids = await ReadIdsAsync();
        Assert.Equal([0L, 1L, 10L, 12L, 14L], ids);

        // The add carries the vector itself — the rows were never in a version anyone could read.
        await using var check = await OpenAsync();
        var withDv = check.CurrentSnapshot.ActiveFiles.Values.Where(f => f.DeletionVector is not null).ToList();
        Assert.Single(withDv);
        Assert.Equal(2L, withDv[0].DeletionVector!.Cardinality);

        // And its stats are marked tightBounds=false: they still describe the PHYSICAL rows, which are a
        // loose superset once a vector hides some, so a reader must not treat min/max as exact. Asserted
        // because nothing else would notice it going missing — the rows read correctly either way, and a
        // pruner trusting tight bounds on a loose file skips files that do hold matching rows.
        Assert.NotNull(withDv[0].Stats);
        Assert.Contains("\"tightBounds\":false", withDv[0].Stats!.Replace(" ", string.Empty));
    }

    /// <summary>No positions supplied is exactly the synchronous overload — no vector, no loosened stats.</summary>
    [Fact]
    public async Task StagedFile_WithoutPositions_IsAPlainAdd()
    {
        await using var table = await CreateAsync();
        var files = await table.WriteDataFilesAsync([Batch(10, 3)]);

        var txn = table.StartTransaction();
        await txn.StageDataFilesAsync(files);
        await txn.CommitAsync();

        await using var check = await OpenAsync();
        Assert.All(check.CurrentSnapshot.ActiveFiles.Values, f => Assert.Null(f.DeletionVector));
        Assert.Equal(5, (await ReadIdsAsync()).Count);
    }

    private static string IdentitySchemaString()
    {
        var idMeta = IdentityColumn.CreateMetadata(start: 1, step: 1, allowExplicitInsert: false);
        string json = System.Text.Json.JsonSerializer.Serialize(idMeta);
        return "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,"
            + $"\"metadata\":{json}}}]}}";
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
                Id = "stage-parity-identity",
                Format = Format.Parquet,
                SchemaString = IdentitySchemaString(),
                PartitionColumns = [],
            },
        });
        return await DeltaTable.OpenAsync(fs);
    }

    /// <summary>
    /// An identity table refuses externally written files — the writer exists to assign those values. A host
    /// that assigned them itself says so, and then its files are acceptable; the refusal is not a blanket ban
    /// on the table shape.
    /// </summary>
    [Fact]
    public async Task IdentityTable_RefusesStagedFiles_UnlessTheHostGeneratedTheValues()
    {
        await using var table = await CreateIdentityTableAsync();
        var idBatch = new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("id", Int64Type.Default, true)).Build(),
            [new Int64Array.Builder().Append(1).Append(2).Build()], 2);
        var files = await table.WriteDataFilesAsync([idBatch], identityValuesPreGenerated: true);

        Assert.Throws<NotSupportedException>(() => table.StartTransaction().StageDataFiles(files));

        var txn = table.StartTransaction();
        await txn.StageDataFilesAsync(files, identityValuesPreGenerated: true);
        await txn.CommitAsync();

        await using var check = await OpenAsync();
        Assert.Single(check.CurrentSnapshot.ActiveFiles);
    }

    /// <summary>
    /// A mixed transaction cannot be named by inference — Delta's operation field is one string — so a host
    /// that knows what its statement was says so, and the history records it instead of "WRITE".
    /// </summary>
    [Fact]
    public async Task SetOperation_NamesTheCommit_WhereInferenceSaysWrite()
    {
        await using var table = await CreateAsync();
        var files = await table.WriteDataFilesAsync([Batch(10, 2)]);

        var txn = table.StartTransaction();
        await txn.StageDataFilesAsync(files);
        await txn.StageRowDeletesAsync(
            new Dictionary<int, IReadOnlyCollection<long>> { [0] = new long[] { 0 } });
        txn.SetOperation("TRANSACTION");
        long version = await txn.CommitAsync();

        await using var check = await OpenAsync();
        string? op = null;
        await foreach (var e in check.GetHistoryAsync())
        {
            if (e.Version == version) { op = e.Operation; break; }
        }
        Assert.Equal("TRANSACTION", op);
    }

    /// <summary>Without the override, the same mixed transaction falls back to the inferred label.</summary>
    [Fact]
    public async Task WithoutSetOperation_AMixedTransactionInfersWrite()
    {
        await using var table = await CreateAsync();
        var files = await table.WriteDataFilesAsync([Batch(10, 2)]);

        var txn = table.StartTransaction();
        await txn.StageDataFilesAsync(files);
        await txn.StageRowDeletesAsync(
            new Dictionary<int, IReadOnlyCollection<long>> { [0] = new long[] { 0 } });
        long version = await txn.CommitAsync();

        await using var check = await OpenAsync();
        string? op = null;
        await foreach (var e in check.GetHistoryAsync())
        {
            if (e.Version == version) { op = e.Operation; break; }
        }
        Assert.Equal("WRITE", op);
    }

    [Fact]
    public async Task EmptyOperation_Throws()
    {
        await using var table = await CreateAsync();
        Assert.Throws<ArgumentException>(() => table.StartTransaction().SetOperation(""));
    }
}

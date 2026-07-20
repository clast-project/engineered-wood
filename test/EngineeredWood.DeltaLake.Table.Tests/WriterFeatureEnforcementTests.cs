// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Writer-feature ENFORCEMENT (<c>HonorWriterFeatures</c>): a table-features-mode table commonly LISTS
/// legacy writer features (<c>appendOnly</c>/<c>invariants</c>/<c>checkConstraints</c>) without them
/// being ACTIVE — such tables must write normally. But when a feature IS active — an actual
/// <c>delta.appendOnly=true</c>, a declared CHECK constraint, a column invariant or generation
/// expression — this writer cannot evaluate the expressions, so the write is REJECTED with a clear
/// error instead of silently committing possibly-violating data (Delta constraints are write-time-only;
/// a violating commit poisons the table for every reader). The appendOnly arm is covered in
/// <see cref="SchemaWriteModesTests"/>; these are the expression arms.
/// </summary>
public class WriterFeatureEnforcementTests : IDisposable
{
    private readonly string _tempDir;

    public WriterFeatureEnforcementTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_wfe_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private async Task<DeltaTable> CreateTableAsync(
        string? fieldMetadataJson = null,
        IReadOnlyDictionary<string, string>? configuration = null,
        string[]? writerFeatures = null)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);
        string meta = fieldMetadataJson ?? "{}";
        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                WriterFeatures = writerFeatures ?? ["appendOnly", "invariants", "checkConstraints"],
            },
            new MetadataAction
            {
                Id = "wfe-table",
                Format = Format.Parquet,
                SchemaString = $@"{{""type"":""struct"",""fields"":[{{""name"":""id"",""type"":""long"",""nullable"":false,""metadata"":{meta}}}]}}",
                PartitionColumns = [],
                Configuration = configuration?.ToDictionary(kv => kv.Key, kv => kv.Value),
            },
        });
        return await DeltaTable.OpenAsync(fs);
    }

    private static RecordBatch Batch(long id)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        return new RecordBatch(schema, [new Int64Array.Builder().Append(id).Build()], 1);
    }

    [Fact]
    public async Task ListedButInactiveFeatures_WriteNormally()
    {
        // the common v7-upgrade shape: appendOnly/invariants/checkConstraints ENUMERATED but not active
        await using var table = await CreateTableAsync();
        long v = await table.WriteAsync([Batch(1)]);
        Assert.Equal(1, v);
    }

    [Fact]
    public async Task ActiveColumnInvariant_RejectsWrite()
    {
        // delta.invariants carries an arbitrary SQL expression this writer cannot evaluate — the write
        // is rejected up front rather than committing possibly-violating rows.
        await using var table = await CreateTableAsync(
            fieldMetadataJson: @"{""delta.invariants"":""{\""expression\"":{\""expression\"":\""id > 0\""}}""}");
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.WriteAsync([Batch(1)]));
        Assert.Contains("invariant", ex.Message);
    }

    [Fact]
    public async Task ActiveCheckConstraint_RejectsWrite()
    {
        await using var table = await CreateTableAsync(
            configuration: new Dictionary<string, string> { ["delta.constraints.positive_id"] = "id > 0" });
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.WriteAsync([Batch(1)]));
        Assert.Contains("delta.constraints.positive_id", ex.Message);
    }

    [Fact]
    public async Task GenerationExpression_RejectsWrite()
    {
        await using var table = await CreateTableAsync(
            fieldMetadataJson: @"{""delta.generationExpression"":""id + 1""}",
            writerFeatures: ["generatedColumns"]);
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.WriteAsync([Batch(1)]));
        Assert.Contains("generation expression", ex.Message);
    }

    [Fact]
    public async Task ActiveAppendOnly_AllowsAppend_RejectsOverwrite()
    {
        // delta.appendOnly=true: appends are fine, but overwrite/delete/update are rejected.
        await using var table = await CreateTableAsync(
            configuration: new Dictionary<string, string> { ["delta.appendOnly"] = "true" });
        long v = await table.WriteAsync([Batch(1)]); // append allowed
        Assert.Equal(1, v);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.WriteAsync([Batch(2)], DeltaWriteMode.Overwrite));
        Assert.Contains("append-only", ex.Message);
    }
}

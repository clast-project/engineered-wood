// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The blind-append rule has to mean the same thing on BOTH paths that use it.
///
/// <para><c>ConflictChecker</c> serves the OCC loop; <c>DeltaTable.CheckLogicalRebaseAsync</c> serves the
/// buffered-transaction rebase. They carried two copies of the rule, which disagreed only on an add-less
/// commit — inert, because both gate an <c>AddFile</c> branch such a commit never reaches.</para>
///
/// <para><b>⚠ Teaching one of them to believe <c>commitInfo.isBlindAppend</c> is exactly the change that
/// makes the drift live.</b> A Spark commit declaring <c>false</c> on an adds-only commit — the insert-only
/// MERGE measured in the interop tier — would then be correctly examined by the OCC loop and wrongly
/// exempted by the rebase path: the same defect, moved rather than fixed. #88 predicted this ("it will not
/// stay inert if either rule gains a condition, and a fix for (1) is exactly such a condition").</para>
/// </summary>
public class BlindAppendRebaseParityTests : IDisposable
{
    private readonly string _tempDir;

    public BlindAppendRebaseParityTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_blindrebase_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema IdSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

    private static RecordBatch Row(Apache.Arrow.Schema schema, long id) =>
        new(schema, [new Int64Array.Builder().Append(id).Build()], 1);

    /// <summary>An adds-only commit carrying an explicit <c>isBlindAppend</c> claim, as Spark writes one.</summary>
    private static IReadOnlyList<DeltaAction> DeclaredAppend(bool isBlindAppend, string path) =>
    [
        InCommitTimestamp.CreateCommitInfo(
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            "WRITE",
            new Dictionary<string, JsonElement>
            {
                ["isBlindAppend"] = JsonDocument.Parse(isBlindAppend ? "true" : "false")
                    .RootElement.Clone(),
            }),
        new AddFile
        {
            Path = path,
            PartitionValues = new Dictionary<string, string>(),
            Size = 100,
            ModificationTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            DataChange = true,
        },
    ];

    /// <summary>
    /// A concurrent commit that declares it READ the table is examined by the rebase path, even though its
    /// actions are adds only and would infer as blind. Under WriteSerializable, against a transaction that
    /// declared a whole-table read, that is a <c>concurrentAppend</c> conflict.
    /// </summary>
    [Fact]
    public async Task RebaseCheck_BelievesADeclaredNonBlindAppend()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([Row(schema, 1)]);                                  // v1
        var baseSnapshot = table.CurrentSnapshot;

        // A concurrent adds-only commit that says it read the table — Spark's insert-only MERGE shape.
        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(
            baseSnapshot.Version + 1, DeclaredAppend(isBlindAppend: false, "concurrent.parquet"));

        await using var reader = await DeltaTable.OpenAsync(fs);

        var conflict = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await reader.CheckLogicalRebaseAsync(
                baseSnapshot,
                plannedActions: [],
                readWholeTable: true,
                serializable: false));

        Assert.Equal(DeltaErrorCodes.ConcurrentAppend, conflict.ErrorCode);
    }

    /// <summary>
    /// THE CONTROL. The identical commit declaring <c>true</c> is exempted, so the case above turns on the
    /// declaration and not on the check having become unconditional — which is the failure that would
    /// otherwise make it pass for the wrong reason.
    /// </summary>
    [Fact]
    public async Task RebaseCheck_ExemptsADeclaredBlindAppend()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([Row(schema, 1)]);                                  // v1
        var baseSnapshot = table.CurrentSnapshot;

        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(
            baseSnapshot.Version + 1, DeclaredAppend(isBlindAppend: true, "concurrent.parquet"));

        await using var reader = await DeltaTable.OpenAsync(fs);

        await reader.CheckLogicalRebaseAsync(
            baseSnapshot,
            plannedActions: [],
            readWholeTable: true,
            serializable: false);
    }

    /// <summary>
    /// #126's term, on this path. A transaction that itself changes the metadata loses the blind-append
    /// exemption, so the identical declared-blind commit that <see cref="RebaseCheck_ExemptsADeclaredBlindAppend"/>
    /// lets through now conflicts.
    ///
    /// <para>This is the test the issue asked for by name: the OCC loop and this path share the rule, and
    /// "if this gate gains a term, that sharing has to extend to the term rather than only to the rule —
    /// that is exactly how the last divergence became live." Sharing <c>ExamineConcurrentAdds</c> rather
    /// than just <c>IsBlindAppend</c> is what makes them agree; this asserts they do.</para>
    /// </summary>
    [Fact]
    public async Task RebaseCheck_OwnMetadataChange_WithdrawsTheExemption()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([Row(schema, 1)]);                                  // v1
        var baseSnapshot = table.CurrentSnapshot;

        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(
            baseSnapshot.Version + 1, DeclaredAppend(isBlindAppend: true, "concurrent.parquet"));

        await using var reader = await DeltaTable.OpenAsync(fs);

        var conflict = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await reader.CheckLogicalRebaseAsync(
                baseSnapshot,
                plannedActions: [baseSnapshot.Metadata with { SchemaString = "{\"type\":\"struct\",\"fields\":[]}" }],
                readWholeTable: true,
                serializable: false));

        Assert.Equal(DeltaErrorCodes.ConcurrentAppend, conflict.ErrorCode);
    }

    /// <summary>
    /// A protocol change of our own does NOT withdraw it — Delta's <c>metadataChanged</c> is
    /// <c>newMetadata.nonEmpty</c> and no protocol term feeds this gate (<c>v4.0.0</c>). Paired with the
    /// test above so the term is pinned to metadata specifically rather than to "we are committing
    /// something table-wide", which is the reading that would make it stricter than Delta.
    /// </summary>
    [Fact]
    public async Task RebaseCheck_OwnProtocolChange_DoesNotWithdrawTheExemption()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([Row(schema, 1)]);                                  // v1
        var baseSnapshot = table.CurrentSnapshot;

        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(
            baseSnapshot.Version + 1, DeclaredAppend(isBlindAppend: true, "concurrent.parquet"));

        await using var reader = await DeltaTable.OpenAsync(fs);

        await reader.CheckLogicalRebaseAsync(
            baseSnapshot,
            plannedActions: [new ProtocolAction { MinReaderVersion = 3, MinWriterVersion = 7 }],
            readWholeTable: true,
            serializable: false);
    }

    /// <summary>
    /// Serializable examines a declared blind append anyway — the level's whole point. Included because the
    /// unified rule now decides <c>blindAppend</c> for this path too, and a rule change that quietly
    /// dropped the isolation gate would leave both cases above passing.
    /// </summary>
    [Fact]
    public async Task RebaseCheck_ExaminesADeclaredBlindAppend_UnderSerializable()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([Row(schema, 1)]);                                  // v1
        var baseSnapshot = table.CurrentSnapshot;

        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(
            baseSnapshot.Version + 1, DeclaredAppend(isBlindAppend: true, "concurrent.parquet"));

        await using var reader = await DeltaTable.OpenAsync(fs);

        var conflict = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await reader.CheckLogicalRebaseAsync(
                baseSnapshot,
                plannedActions: [],
                readWholeTable: true,
                serializable: true));

        Assert.Equal(DeltaErrorCodes.ConcurrentAppend, conflict.ErrorCode);
    }
}

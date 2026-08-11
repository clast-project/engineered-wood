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
/// A host staging work on a <see cref="DeltaTransaction"/> can declare <c>commitInfo.isBlindAppend</c>,
/// and what it declares is what lands in the log.
///
/// <para>The staged surface has to be able to say this, not just <c>CommitDataFilesAsync</c> — that is the
/// invariant <c>StagedCommitParityTests</c> enforces, and a capability reachable only from the
/// auto-committing entry point is one whose users must hand-roll the commit loop.</para>
/// </summary>
public class BlindAppendDeclarationTests : IDisposable
{
    private readonly string _tempDir;

    public BlindAppendDeclarationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_blinddecl_{Guid.NewGuid():N}");
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

    /// <summary>What the commit at <paramref name="version"/> recorded, read back through the log.</summary>
    private async ValueTask<JsonElement?> RecordedFlagAsync(long version)
    {
        var log = new TransactionLog(new LocalTableFileSystem(_tempDir));
        foreach (var action in await log.ReadCommitAsync(version))
        {
            if (action is CommitInfo info)
                return info.GetValue("isBlindAppend");
        }
        return null;
    }

    /// <summary>A transaction that declares it read nothing records <c>true</c>.</summary>
    [Fact]
    public async Task Transaction_DeclaringBlind_RecordsTrue()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        var txn = table.StartTransaction();
        txn.IsBlindAppend = true;
        await txn.StageDataFilesAsync(await table.WriteDataFilesAsync([Row(schema, 1)]));
        long version = await txn.CommitAsync();

        var flag = await RecordedFlagAsync(version);
        Assert.NotNull(flag);
        Assert.Equal(JsonValueKind.True, flag!.Value.ValueKind);
    }

    /// <summary>
    /// A transaction that declares it READ records <c>false</c> — on an adds-only commit, which is the
    /// shape a reader's inference gets wrong and therefore the one worth declaring on.
    /// </summary>
    [Fact]
    public async Task Transaction_DeclaringNotBlind_RecordsFalse_OnAnAddsOnlyCommit()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        var txn = table.StartTransaction();
        txn.IsBlindAppend = false;
        await txn.StageDataFilesAsync(await table.WriteDataFilesAsync([Row(schema, 1)]));
        long version = await txn.CommitAsync();

        var flag = await RecordedFlagAsync(version);
        Assert.NotNull(flag);
        Assert.Equal(JsonValueKind.False, flag!.Value.ValueKind);

        // Adds only: without this the case above is indistinguishable from one where the shape alone would
        // have said "not blind" anyway, and the declaration would be carrying no weight.
        var actions = await new TransactionLog(fs).ReadCommitAsync(version);
        Assert.Contains(actions, a => a is AddFile);
        Assert.DoesNotContain(actions, a => a is RemoveFile);
    }

    /// <summary>
    /// ⚠ THE DEFAULT RECORDS NOTHING, which is not the same as recording <c>false</c> even though a reader
    /// treats them alike. Saying nothing is the honest answer for a host that does not track its reads;
    /// claiming <c>true</c> wrongly is the unsafe direction. A silent caller must stay silent.
    /// </summary>
    [Fact]
    public async Task Transaction_DeclaringNothing_RecordsNoField()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        var txn = table.StartTransaction();
        await txn.StageDataFilesAsync(await table.WriteDataFilesAsync([Row(schema, 1)]));
        long version = await txn.CommitAsync();

        Assert.Null(await RecordedFlagAsync(version));
    }
}

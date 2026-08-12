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
/// What THIS library records on its own commits, which is the outbound half of #88.
///
/// <para>Another engine reads <c>isBlindAppendOption.getOrElse(false)</c>, so an unflagged commit is
/// examined under every isolation level. Measured on Fabric Spark 4.1.1.5.5: a Spark DELETE aborts against
/// a concurrent unflagged append even at WriteSerializable, where it would have committed against its own
/// blind append. We know what each of our paths read, so we can say.</para>
///
/// <para>These assert the raw field rather than any behaviour, because the field IS the interface — what
/// consumes it is another engine's checker, not ours.</para>
/// </summary>
public class BlindAppendOwnCommitsTests : IDisposable
{
    private readonly string _tempDir;

    public BlindAppendOwnCommitsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_blindown_{Guid.NewGuid():N}");
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

    private static RecordBatch Rows(Apache.Arrow.Schema schema, params long[] ids)
    {
        var b = new Int64Array.Builder();
        foreach (long id in ids)
            b.Append(id);
        return new RecordBatch(schema, [b.Build()], ids.Length);
    }

    private async ValueTask<bool?> RecordedFlagAsync(long version)
    {
        var log = new TransactionLog(new LocalTableFileSystem(_tempDir));
        foreach (var action in await log.ReadCommitAsync(version))
        {
            if (action is CommitInfo info && info.GetValue("isBlindAppend") is { } flag)
            {
                return flag.ValueKind switch
                {
                    JsonValueKind.True => true,
                    JsonValueKind.False => false,
                    _ => null,
                };
            }
        }
        return null;
    }

    /// <summary>
    /// A plain append declares nothing unless the caller does.
    /// </summary>
    /// <remarks>
    /// This asserted <c>true</c> until #137. The reasoning was that a plain append "takes its rows from
    /// the caller and reads no file of this table to decide what to write, which is Delta's definition" —
    /// true of what this library does, and not of what the field means. Delta's <c>isBlindAppend</c>
    /// describes the TRANSACTION, and the caller is part of it: a host that scanned this table and handed
    /// us the resulting rows has made a read we never saw.
    ///
    /// It is the same substitution #125 forbids one section earlier, arriving from the other side —
    /// "writing a spec field off a defaulted value turns every silent caller into an assertive one" —
    /// and #125's own measurement names the casualty: <c>insert_select_self</c> records <c>false</c> in
    /// Spark, is adds-only, and is indistinguishable from a genuine blind append by anything but the flag.
    /// </remarks>
    [Fact]
    public async Task Append_DeclaresNothingUnlessTheCallerDoes()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([Rows(schema, 1, 2)]);

        Assert.Null(await RecordedFlagAsync(table.CurrentSnapshot.Version));
    }

    /// <summary>A caller that genuinely read nothing can still say so, and it is recorded.</summary>
    [Fact]
    public async Task Append_RecordsTheCallersOwnClaim()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([Rows(schema, 1, 2)], isBlindAppend: true);
        Assert.True(await RecordedFlagAsync(table.CurrentSnapshot.Version));

        // And a host that scanned the table to produce its rows says the opposite.
        await table.WriteAsync([Rows(schema, 3, 4)], isBlindAppend: false);
        Assert.False(await RecordedFlagAsync(table.CurrentSnapshot.Version));
    }

    /// <summary>An overwrite reads the active-file set to decide what to remove, so it declares <c>false</c>.</summary>
    [Fact]
    public async Task Overwrite_DeclaresNotBlind()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([Rows(schema, 1)]);
        await table.WriteAsync([Rows(schema, 2)], DeltaWriteMode.Overwrite);

        Assert.False(await RecordedFlagAsync(table.CurrentSnapshot.Version));
    }

    /// <summary>
    /// A DELETE declares <c>false</c>, and does so without anyone passing anything: the autocommit surface
    /// stages through a transaction, and a transaction that recorded a read derives the claim itself.
    ///
    /// <para>The declaration changes nothing for a reader here — the commit carries removes, so every
    /// inference and every default already say not-blind — but a table whose appends are flagged and whose
    /// DML is not is a table where absence has two meanings.</para>
    /// </summary>
    [Fact]
    public async Task Delete_DeclaresNotBlind()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, enableDeletionVectors: true);
        await table.WriteAsync([Rows(schema, 1, 2, 3)]);

        (_, long version) = await table.DeleteAsync(batch =>
        {
            var ids = (Int64Array)batch.Column(0);
            var mask = new BooleanArray.Builder();
            for (int i = 0; i < ids.Length; i++)
                mask.Append(ids.GetValue(i) == 1);
            return mask.Build();
        });

        Assert.False(await RecordedFlagAsync(version));
    }

    /// <summary>OPTIMIZE reads the files it rewrites and removes every one of them.</summary>
    [Fact]
    public async Task Optimize_DeclaresNotBlind()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([Rows(schema, 1)]);
        await table.WriteAsync([Rows(schema, 2)]);

        long? version = await table.CompactAsync();

        Assert.NotNull(version);
        Assert.False(await RecordedFlagAsync(version!.Value));
    }

    /// <summary>
    /// ⚠ THE ONE THAT MATTERS FOR SAFETY. Our own append declares blind, so our own reader — which now
    /// believes declarations — exempts it under WriteSerializable, exactly as another engine's would. This
    /// is the round trip: what we write is what a checker consumes, and the two halves of #88 meeting is
    /// what makes either of them worth anything.
    /// </summary>
    [Fact]
    public async Task OurOwnAppend_IsExemptedByOurOwnChecker()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([Rows(schema, 1)]);
        var baseSnapshot = table.CurrentSnapshot;

        // A concurrent append through our own writer, carrying our own declaration.
        await using (var other = await DeltaTable.OpenAsync(fs))
            await other.WriteAsync([Rows(schema, 2)]);

        await using var reader = await DeltaTable.OpenAsync(fs);

        // Declared blind ⇒ exempt at WriteSerializable even against a whole-table read.
        await reader.CheckLogicalRebaseAsync(
            baseSnapshot, plannedActions: [], readWholeTable: true, serializable: false);
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <c>delta.checkpointInterval</c> — a table's own statement about how often it wants checkpointing.
///
/// <para>The interval came from <see cref="DeltaTableOptions.CheckpointInterval"/> alone, so a table
/// declaring 100 was still checkpointed every 10. The property is part of the Delta spec and IS stored by
/// writers that accept it, which makes ignoring it worse than not supporting it: the table says one thing
/// and the library does another, at ten times the checkpoint objects its owner asked for.</para>
/// </summary>
public class CheckpointIntervalTests : IDisposable
{
    private readonly string _tempDir;

    public CheckpointIntervalTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_ckptint_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema IdSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

    private static RecordBatch Row(Apache.Arrow.Schema schema, long id) =>
        new(schema, [new Int64Array.Builder().Append(id).Build()], 1);

    /// <summary>
    /// The table's declaration wins over the caller's option. Re-opened rather than asserted on the handle
    /// that created it, because the property has to be read from the SNAPSHOT — which is where a foreign
    /// writer's would be.
    /// </summary>
    [Fact]
    public async Task CheckpointInterval_ComesFromTheTableProperty_WhenItDeclaresOne()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using (await DeltaTable.CreateAsync(
            fs, schema,
            configuration: new Dictionary<string, string> { ["delta.checkpointInterval"] = "4" }))
        {
        }

        await using var table = await DeltaTable.OpenAsync(fs, new DeltaTableOptions { CheckpointInterval = 10 });
        for (long i = 1; i <= 4; i++)
            await table.WriteAsync([Row(schema, i)]);

        Assert.Equal(4, table.CurrentSnapshot.Version);
        Assert.True(
            await fs.ExistsAsync(DeltaVersion.CheckpointPath(4)),
            "the table declares delta.checkpointInterval = 4 and was not checkpointed at v4");
    }

    /// <summary>
    /// THE CONTROL. Without it the test above passes equally if the caller's option had simply started
    /// being ignored in favour of a hardcoded 4 — a different bug with the same symptom on one table.
    /// </summary>
    [Fact]
    public async Task CheckpointInterval_FallsBackToTheCallerOption_WhenTheTableDeclaresNone()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, new DeltaTableOptions { CheckpointInterval = 4 });
        for (long i = 1; i <= 4; i++)
            await table.WriteAsync([Row(schema, i)]);

        Assert.Equal(4, table.CurrentSnapshot.Version);
        Assert.True(await fs.ExistsAsync(DeltaVersion.CheckpointPath(4)));
    }

    /// <summary>
    /// ⚠ <c>CheckpointInterval = 0</c> means "never checkpoint" and stays an ABSOLUTE caller override: a
    /// host that owns checkpointing on its own schedule must not have a table property switch it back on
    /// and start racing one it did not ask for.
    /// </summary>
    [Fact]
    public async Task CheckpointInterval_Zero_StaysDisabled_EvenWhenTheTableDeclaresOne()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using (await DeltaTable.CreateAsync(
            fs, schema,
            configuration: new Dictionary<string, string> { ["delta.checkpointInterval"] = "2" }))
        {
        }

        await using var table = await DeltaTable.OpenAsync(fs, new DeltaTableOptions { CheckpointInterval = 0 });
        for (long i = 1; i <= 4; i++)
            await table.WriteAsync([Row(schema, i)]);

        Assert.False(await fs.ExistsAsync(DeltaVersion.CheckpointPath(2)));
        Assert.False(await fs.ExistsAsync(DeltaVersion.CheckpointPath(4)));
    }

    /// <summary>
    /// The handle that CREATED the table honours the property too, without a re-open. The tests above all
    /// re-open — deliberately, since the property has to come from the snapshot — but that leaves the
    /// first thing a caller actually does unasserted: declare the interval at create time and write
    /// through the handle it hands back.
    /// </summary>
    [Fact]
    public async Task CheckpointInterval_IsHonoured_OnTheCreatingHandle()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema,
            configuration: new Dictionary<string, string> { ["delta.checkpointInterval"] = "2" },
            options: new DeltaTableOptions { CheckpointInterval = 10 });

        await table.WriteAsync([Row(schema, 1)]);                                  // v1
        await table.WriteAsync([Row(schema, 2)]);                                  // v2

        Assert.Equal(2, table.CurrentSnapshot.Version);
        Assert.True(
            await fs.ExistsAsync(DeltaVersion.CheckpointPath(2)),
            "the creating handle ignored the delta.checkpointInterval it was created with");
    }

    /// <summary>
    /// ⚠ The property must be honoured on EVERY commit path, not only the ones that go through the commit
    /// loop. Since #108 there are two triggers reading the interval — the committer's
    /// <c>LogCommitOptions</c> and <c>CheckpointIfDueAsync</c>, which OPTIMIZE and every metadata change
    /// call — so a resolved value wired into one and not the other honours the table on some writes and
    /// ignores it on others. That is the failure mode this change's own design note calls harder to
    /// notice than ignoring the property everywhere, so it is asserted rather than trusted.
    /// </summary>
    [Fact]
    public async Task CheckpointInterval_IsHonoured_OnAMetadataOnlyCommit()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using (await DeltaTable.CreateAsync(
            fs, schema,
            configuration: new Dictionary<string, string> { ["delta.checkpointInterval"] = "2" }))
        {
        }

        await using var table = await DeltaTable.OpenAsync(
            fs, new DeltaTableOptions { CheckpointInterval = 10 });
        await table.WriteAsync([Row(schema, 1)]);                                  // v1

        // v2 — a metadata-only commit, which reaches the log through CheckpointIfDueAsync and not the
        // committer. At the caller's interval of 10 this version would not checkpoint at all.
        long version = await table.SetDomainMetadataAsync("test.domain", "{\"k\":1}");

        Assert.Equal(2, version);
        Assert.True(
            await fs.ExistsAsync(DeltaVersion.CheckpointPath(2)),
            "delta.checkpointInterval was honoured on the commit loop but not on the metadata path");
    }

    /// <summary>
    /// A property that cannot be read as a positive integer falls back to the caller's option instead of
    /// throwing. This is a declaration from a table someone else may have written, and refusing to OPEN
    /// over it would turn a typo in a <c>set_tblproperties</c> into an unreadable table.
    /// </summary>
    [Theory]
    [InlineData("not-a-number")]
    [InlineData("")]
    [InlineData("0")]
    [InlineData("-4")]
    [InlineData("2.5")]
    public async Task CheckpointInterval_FallsBackToTheCallerOption_WhenThePropertyIsUnusable(string raw)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = IdSchema();

        await using (await DeltaTable.CreateAsync(
            fs, schema,
            configuration: new Dictionary<string, string> { ["delta.checkpointInterval"] = raw }))
        {
        }

        await using var table = await DeltaTable.OpenAsync(
            fs, new DeltaTableOptions { CheckpointInterval = 2 });
        await table.WriteAsync([Row(schema, 1)]);
        await table.WriteAsync([Row(schema, 2)]);

        Assert.Equal(2, table.CurrentSnapshot.Version);
        Assert.True(
            await fs.ExistsAsync(DeltaVersion.CheckpointPath(2)),
            $"'{raw}' is not a usable interval and the caller's option of 2 should have applied");
    }
}

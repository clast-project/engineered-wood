// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="DeltaTable.DiscardDataFilesAsync"/>: the buffered seam's third verb, for a host that has decided
/// its written-but-uncommitted files will never be committed. The library cannot infer that — the files are
/// meant to outlive the call — so the host says it, and the only thing the library owes back is a refusal to
/// delete anything the table actually references.
/// </summary>
public class DiscardDataFilesTests : IDisposable
{
    private readonly string _tempDir;

    public DiscardDataFilesTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_discard_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema IdSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch Batch(params long[] ids) =>
        new(IdSchema, [new Int64Array.Builder().AppendRange(ids).Build()], ids.Length);

    private string[] DataFiles() =>
        [.. Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories)
            .Where(p => !p.Contains("_delta_log", StringComparison.Ordinal))
            .OrderBy(p => p, StringComparer.Ordinal)];

    private static async Task<List<long>> ReadIds(DeltaTable table)
    {
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

    /// <summary>The whole point: bytes written for a commit that never came, reclaimed on the host's word
    /// rather than at VACUUM's retention horizon.</summary>
    [Fact]
    public async Task DiscardDataFilesAsync_DeletesTheUncommittedFiles()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
        await table.WriteAsync([Batch(1)]);
        string[] before = DataFiles();
        long version = table.CurrentSnapshot.Version;

        var written = await table.WriteDataFilesAsync([Batch(2), Batch(3)]);
        Assert.Equal(before.Length + written.Count, DataFiles().Length);

        await table.DiscardDataFilesAsync(written);

        Assert.Equal(before, DataFiles());
        Assert.Equal(version, table.CurrentSnapshot.Version); // discarding commits nothing
        Assert.Equal([1L], await ReadIds(table));
    }

    /// <summary>
    /// The one thing the library owes a caller who hands it a list of paths: a file the table REFERENCES is
    /// live data, and deleting it would leave an add naming nothing. Validate-then-apply — a list with one
    /// committed file in it does not half-delete the rest.
    /// </summary>
    [Fact]
    public async Task DiscardDataFilesAsync_RefusesACommittedFileAndDeletesNothing()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);

        var committed = await table.WriteDataFilesAsync([Batch(1)]);
        await table.CommitDataFilesAsync(committed);
        var uncommitted = await table.WriteDataFilesAsync([Batch(2)]);
        string[] before = DataFiles();

        var ex = await Assert.ThrowsAsync<ArgumentException>(
            async () => await table.DiscardDataFilesAsync([.. committed, .. uncommitted]));
        Assert.Contains(committed[0].RelativePath, ex.Message, StringComparison.Ordinal);

        Assert.Equal(before, DataFiles()); // neither the committed file NOR the discardable one went
        Assert.Equal([1L], await ReadIds(table));
    }

    /// <summary>
    /// The check reads the log rather than trusting this handle's cached snapshot: the commit that made these
    /// files live may have come from another handle, and a stale <c>CurrentSnapshot</c> would not show it.
    /// </summary>
    [Fact]
    public async Task DiscardDataFilesAsync_RefusesAFileCommittedByAnotherHandle()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var stale = await DeltaTable.CreateAsync(fs, IdSchema);
        var written = await stale.WriteDataFilesAsync([Batch(1)]);
        long staleVersion = stale.CurrentSnapshot.Version;

        await using (var other = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)))
            await other.CommitDataFilesAsync(written);

        Assert.Equal(staleVersion, stale.CurrentSnapshot.Version); // this handle has not noticed
        string[] before = DataFiles();

        await Assert.ThrowsAsync<ArgumentException>(
            async () => await stale.DiscardDataFilesAsync(written));

        Assert.Equal(before, DataFiles());
    }

    /// <summary>Best-effort: a file already gone is not an error, so a retried discard is quiet.</summary>
    [Fact]
    public async Task DiscardDataFilesAsync_IsIdempotent()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
        string[] before = DataFiles();

        var written = await table.WriteDataFilesAsync([Batch(1)]);
        await table.DiscardDataFilesAsync(written);
        await table.DiscardDataFilesAsync(written);

        Assert.Equal(before, DataFiles());
    }

    /// <summary>An empty list is a no-op, and does not even read the log.</summary>
    [Fact]
    public async Task DiscardDataFilesAsync_EmptyList_DoesNothing()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
        await table.WriteAsync([Batch(1)]);
        string[] before = DataFiles();

        await table.DiscardDataFilesAsync([]);

        Assert.Equal(before, DataFiles());
        await Assert.ThrowsAsync<ArgumentNullException>(
            async () => await table.DiscardDataFilesAsync(null!));
    }

    /// <summary>
    /// Discarding is not a commit and must not move the caller's world: a host mid-buffered-transaction is
    /// planning against a pinned snapshot, and a cleanup call is no reason to advance the handle's.
    /// </summary>
    [Fact]
    public async Task DiscardDataFilesAsync_DoesNotAdvanceTheHandlesSnapshot()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var stale = await DeltaTable.CreateAsync(fs, IdSchema);
        await stale.WriteAsync([Batch(1)]);
        long pinned = stale.CurrentSnapshot.Version;

        var written = await stale.WriteDataFilesAsync([Batch(2)]);
        await using (var other = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)))
            await other.WriteAsync([Batch(3)]); // the table moves under this handle

        await stale.DiscardDataFilesAsync(written);

        Assert.Equal(pinned, stale.CurrentSnapshot.Version);
    }

    /// <summary>A partitioned write's files live in Hive directories; the path the discard deletes by is the
    /// same one the writer reported, so nested paths work without the caller reassembling anything.</summary>
    [Fact]
    public async Task DiscardDataFilesAsync_DeletesPartitionedFiles()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringType.Default, false))
            .Build();
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, schema, partitionColumns: ["region"]);
        string[] before = DataFiles();

        var batch = new RecordBatch(schema,
            [
                new Int64Array.Builder().AppendRange([1L, 2L]).Build(),
                new StringArray.Builder().Append("east").Append("west").Build(),
            ], 2);
        var written = await table.WriteDataFilesAsync([batch]);
        Assert.Equal(2, written.Count); // one file per partition
        Assert.All(written, w => Assert.Contains('/', w.RelativePath));

        await table.DiscardDataFilesAsync(written);

        Assert.Equal(before, DataFiles());
    }
}

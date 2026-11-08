// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Collections.Concurrent;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Every commit this library writes carries <c>commitInfo.txnId</c>, a random UUID per transaction, as delta-spark's
/// do. It makes every commit's bytes unique (so a content-derived identity, such as S3's MD5 ETag, never takes two
/// commits for one object; #749 review), and it stays the same across the attempts of one transaction.
/// </summary>
public class CommitTxnIdTests : IDisposable
{
    private readonly string _tempDir = Directory.CreateDirectory(
        Path.Combine(Path.GetTempPath(), $"delta_txnid_{Guid.NewGuid():N}")).FullName;

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static DeltaTableOptions Options => new() { CheckpointInterval = 0 };

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, true))
        .Build();

    private static RecordBatch Rows(params long[] ids)
    {
        var id = new Int64Array.Builder();
        foreach (long i in ids)
            id.Append(i);
        return new RecordBatch(TableSchema, [id.Build()], ids.Length);
    }

    private static Func<RecordBatch, BooleanArray> IdIs(long value) => batch =>
    {
        var column = (Int64Array)batch.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            mask.Append(column.GetValue(i) == value);
        return mask.Build();
    };

    /// <summary>The commitInfo of each commit file, by version.</summary>
    private SortedDictionary<long, JsonElement> CommitInfos()
    {
        var result = new SortedDictionary<long, JsonElement>();
        foreach (string file in Directory.GetFiles(Path.Combine(_tempDir, "_delta_log"), "*.json"))
        {
            if (!DeltaVersion.TryParseCommitVersion(Path.GetFileName(file), out long version))
                continue;
            foreach (string line in File.ReadAllLines(file))
            {
                using var doc = JsonDocument.Parse(line);
                if (doc.RootElement.TryGetProperty("commitInfo", out var ci))
                    result[version] = ci.Clone();
            }
        }
        return result;
    }

    private static string TxnIdOf(JsonElement commitInfo) =>
        commitInfo.GetProperty("txnId").GetString()!;

    [Fact]
    public async Task Every_kind_of_commit_names_its_own_transaction()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(fs, TableSchema, Options))   // CREATE
        {
            await table.WriteAsync([Rows(1, 2)]);                                         // append
            await table.WriteAsync([Rows(3)], DeltaWriteMode.Overwrite);                  // overwrite
            await table.WriteAsync([Rows(4, 5)]);
            await table.DeleteAsync(IdIs(4));                                             // DELETE
            await table.AddColumnAsync(new Field("note", StringType.Default, true));     // metadata
            await table.VacuumAsync(TimeSpan.FromDays(7));                               // VACUUM START + END
        }

        var infos = CommitInfos();
        Assert.True(infos.Count >= 7, $"expected every commit, found {infos.Count}");
        Assert.Equal(infos.Keys, Enumerable.Range(0, infos.Count).Select(v => (long)v));  // one commitInfo each

        var ids = infos.Values.Select(TxnIdOf).ToList();
        foreach (string id in ids)
        {
            Assert.True(Guid.TryParseExact(id, "D", out _), $"'{id}' is not a hyphenated UUID");
            Assert.Equal(id.ToLowerInvariant(), id);                                    // Spark's form
        }
        Assert.Equal(ids.Count, ids.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task A_retried_transaction_keeps_its_id()
    {
        // Two handles at the same version; the second's first attempt collides with the first's commit and is
        // retried at the next version. Every attempted commit body is captured, the one that lost included.
        var capture = new CommitCaptureFileSystem(new LocalTableFileSystem(_tempDir));
        await using (var create = await DeltaTable.CreateAsync(capture, TableSchema, Options))
            await create.WriteAsync([Rows(1)]);                                           // v1
        await using var a = await DeltaTable.OpenAsync(capture, Options);
        await using var b = await DeltaTable.OpenAsync(capture, Options);
        await a.WriteAsync([Rows(2)]);                                                    // v2
        capture.Clear();

        long version = await b.WriteAsync([Rows(3)]);                                     // tries v2, lands at v3

        Assert.Equal(3, version);
        var attempts = capture.Attempts;
        Assert.Equal(2, attempts.Count);
        Assert.False(attempts[0].Created);
        Assert.True(attempts[1].Created);
        Assert.Equal(TxnIdOf(attempts[0].CommitInfo), TxnIdOf(attempts[1].CommitInfo));
        Assert.Equal(TxnIdOf(CommitInfos()[3]), TxnIdOf(attempts[1].CommitInfo));
        Assert.NotEqual(TxnIdOf(CommitInfos()[2]), TxnIdOf(CommitInfos()[3]));
    }

    /// <summary>Records every create-if-absent of a commit file, and whether it won.</summary>
    private sealed class CommitCaptureFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        private readonly ConcurrentQueue<(bool Created, JsonElement CommitInfo)> _attempts = new();

        public List<(bool Created, JsonElement CommitInfo)> Attempts => [.. _attempts];

        public void Clear()
        {
            while (_attempts.TryDequeue(out _)) { }
        }

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public async ValueTask<bool> TryWriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            bool created = await inner.TryWriteAllBytesAsync(path, data, cancellationToken).ConfigureAwait(false);
            if (DeltaVersion.TryParseCommitVersion(Path.GetFileName(path), out _))
            {
                foreach (string line in System.Text.Encoding.UTF8.GetString(data.ToArray()).Split('\n'))
                {
                    if (line.Trim().Length == 0)
                        continue;
                    using var doc = JsonDocument.Parse(line);
                    if (doc.RootElement.TryGetProperty("commitInfo", out var ci))
                        _attempts.Enqueue((created, ci.Clone()));
                }
            }
            return created;
        }

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, cancellationToken);

        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default) =>
            inner.OpenReadAsync(path, cancellationToken);

        public ValueTask<ISequentialFile> CreateAsync(
            string path, bool overwrite = false, CancellationToken cancellationToken = default) =>
            inner.CreateAsync(path, overwrite, cancellationToken);

        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default) =>
            inner.DeleteAsync(path, cancellationToken);

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default) =>
            inner.ExistsAsync(path, cancellationToken);

        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default) =>
            inner.ReadAllBytesAsync(path, cancellationToken);

        public ValueTask WriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
            inner.WriteAllBytesAsync(path, data, cancellationToken);
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using EngineeredWood.IO;

namespace EngineeredWood.Tests.IO;

/// <summary>
/// <para>The <see cref="ISequentialFile.CompleteAsync"/> contract, held against every backend (issue #501): a
/// streamed write is published only by completing it, and disposing it first discards it.</para>
///
/// <para>Here rather than in a suite of its own because this class already carries the per-backend emulator
/// setup, and the contract is a property of the storage layer in the same way a name's survival is.</para>
/// </summary>
public abstract partial class TableFileSystemPathConformanceTests
{
    /// <summary>
    /// More than one S3 part (8 MiB), Azure block (4 MiB) and GCS chunk (8 MiB), so an abort has something
    /// staged to discard, rather than a buffer that was never sent.
    /// </summary>
    private const int LargerThanOnePart = 10 * 1024 * 1024 + 12345;

    [SkippableFact]
    public async Task StreamedWrite_ThatThrows_PublishesNothing()
    {
        Require();
        var fs = FileSystem;
        const string path = "abort/small.bin";

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await using var file = await fs.CreateAsync(path, overwrite: false);
            await file.WriteAsync(Encoding.UTF8.GetBytes("partial"));
            await file.FlushAsync();
            throw new InvalidOperationException("the write failed part-way (injected)");
        });

        Assert.False(await fs.ExistsAsync(path), "an aborted write left a file at its final name.");
        Assert.Empty(await ListAsync(fs, "abort/"));
    }

    [SkippableFact]
    public async Task StreamedWrite_ThatThrowsAfterStagingParts_PublishesNothing()
    {
        Require();
        var fs = FileSystem;
        const string path = "abort/large.bin";
        byte[] chunk = new byte[1024 * 1024];

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await using var file = await fs.CreateAsync(path, overwrite: false);
            for (int written = 0; written < LargerThanOnePart; written += chunk.Length)
                await file.WriteAsync(chunk);
            throw new InvalidOperationException("the write failed part-way (injected)");
        });

        Assert.False(await fs.ExistsAsync(path), "an aborted multi-part write left a file at its final name.");
        Assert.Empty(await ListAsync(fs, "abort/"));
    }

    [SkippableFact]
    public async Task StreamedWrite_IsInvisibleUntilCompleted()
    {
        Require();
        var fs = FileSystem;
        const string path = "complete/file.bin";
        byte[] payload = PayloadFor(path);

        await using (var file = await fs.CreateAsync(path, overwrite: false))
        {
            await file.WriteAsync(payload);
            await file.FlushAsync();
            Assert.False(await fs.ExistsAsync(path), "a flushed but uncompleted write is already visible.");
            Assert.Empty(await ListAsync(fs, "complete/"));

            await file.CompleteAsync();
            Assert.Equal(payload.Length, file.Position);

            // A second completion is a no-op; a write after completion is refused.
            await file.CompleteAsync();
            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await file.WriteAsync(payload));
        }

        Assert.Equal(payload, await fs.ReadAllBytesAsync(path));
        Assert.Equal([path], (await ListAsync(fs, "complete/")).Select(static info => info.Path).ToArray());
    }

    [SkippableFact]
    public async Task LargeStreamedWrite_Completes()
    {
        Require();
        var fs = FileSystem;
        const string path = "complete/large.bin";
        var payload = new byte[LargerThanOnePart];
        for (int i = 0; i < payload.Length; i++)
            payload[i] = (byte)(i * 31 + 7);

        await using (var file = await fs.CreateAsync(path, overwrite: false))
        {
            for (int offset = 0; offset < payload.Length; offset += 700_003)
                await file.WriteAsync(payload.AsMemory(offset, Math.Min(700_003, payload.Length - offset)));
            await file.CompleteAsync();
        }

        Assert.Equal(payload, await fs.ReadAllBytesAsync(path));
    }

    /// <summary>
    /// The create-time existence check only fails early. A file another writer published between create and
    /// complete must survive: completion is the create-if-absent step.
    /// </summary>
    [SkippableFact]
    public async Task CompleteWithoutOverwrite_LosesToAFilePublishedSinceCreate()
    {
        Require();
        var fs = FileSystem;
        const string path = "race/checkpoint.parquet";
        byte[] theirs = Encoding.UTF8.GetBytes("the other writer's file");

        await using (var file = await fs.CreateAsync(path, overwrite: false))
        {
            await file.WriteAsync(Encoding.UTF8.GetBytes("this writer's file"));
            Assert.True(await fs.TryWriteAllBytesAsync(path, theirs));

            await Assert.ThrowsAsync<IOException>(async () => await file.CompleteAsync());
        }

        Assert.Equal(theirs, await fs.ReadAllBytesAsync(path));
        Assert.Equal([path], (await ListAsync(fs, "race/")).Select(static info => info.Path).ToArray());
    }

    [SkippableFact]
    public async Task CompleteWithOverwrite_ReplacesAFilePublishedSinceCreate()
    {
        Require();
        var fs = FileSystem;
        const string path = "race/replaced.bin";
        byte[] mine = Encoding.UTF8.GetBytes("this writer's file");

        await using (var file = await fs.CreateAsync(path, overwrite: true))
        {
            await file.WriteAsync(mine);
            await fs.WriteAllBytesAsync(path, Encoding.UTF8.GetBytes("the other writer's file"));
            await file.CompleteAsync();
        }

        Assert.Equal(mine, await fs.ReadAllBytesAsync(path));
    }

    [SkippableFact]
    public async Task EmptyStreamedWrite_PublishesAnEmptyFile()
    {
        Require();
        var fs = FileSystem;
        const string path = "complete/empty.bin";

        await using (var file = await fs.CreateAsync(path, overwrite: false))
            await file.CompleteAsync();

        Assert.Empty(await fs.ReadAllBytesAsync(path));
    }
}

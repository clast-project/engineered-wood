// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// <c>_last_checkpoint</c> is an advisory HINT, so every way of failing to read it must mean what absence
/// means: no hint, replay from the log. Failing the CALLER over it turns a hint file into a failed commit.
/// Not hypothetical — a filesystem that updates the file non-atomically lets a concurrent reader see it
/// empty, truncated, or fail the read (measured on Fabric OneLake, 2026-07-31: the empty shape killed 2 of
/// 8 concurrent writers).
/// </summary>
public class LastCheckpointToleranceTests : IDisposable
{
    private readonly string _tempDir;

    public LastCheckpointToleranceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_lastckpt_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        Directory.CreateDirectory(Path.Combine(_tempDir, "_delta_log"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
        GC.SuppressFinalize(this);
    }

    private void WriteHint(string content) =>
        File.WriteAllBytes(
            Path.Combine(_tempDir, DeltaVersion.LastCheckpointPath),
            Encoding.UTF8.GetBytes(content));

    /// <summary>
    /// POSITIVE CONTROL, and the reason the null-returning tests below mean anything: without it, a guard
    /// broad enough to swallow every input would pass the whole class while disabling the optimization.
    /// </summary>
    [Fact]
    public async Task WellFormedHint_IsRead()
    {
        WriteHint("""{"version":7,"size":42,"sizeInBytes":1234,"numOfAddFiles":3}""");

        var info = await new CheckpointReader(new LocalTableFileSystem(_tempDir))
            .ReadLastCheckpointAsync();

        Assert.NotNull(info);
        Assert.Equal(7, info!.Version);
        Assert.Equal(42, info.Size);
    }

    [Fact]
    public async Task AbsentHint_IsNull()
    {
        var info = await new CheckpointReader(new LocalTableFileSystem(_tempDir))
            .ReadLastCheckpointAsync();

        Assert.Null(info);
    }

    /// <summary>
    /// The shape measured on OneLake: the overwrite created the file but has not written to it yet.
    /// </summary>
    [Fact]
    public async Task EmptyHint_IsNull()
    {
        WriteHint(string.Empty);

        var info = await new CheckpointReader(new LocalTableFileSystem(_tempDir))
            .ReadLastCheckpointAsync();

        Assert.Null(info);
    }

    [Theory]
    // A prefix of a real hint — the overwrite landed partially.
    [InlineData("""{"version":7,"si""")]
    // Not JSON at all.
    [InlineData("not json")]
    // Valid JSON, but not an object.
    [InlineData("[1,2,3]")]
    public async Task UnparseableHint_IsNull(string content)
    {
        WriteHint(content);

        var info = await new CheckpointReader(new LocalTableFileSystem(_tempDir))
            .ReadLastCheckpointAsync();

        Assert.Null(info);
    }

    /// <summary>
    /// Valid JSON of the right shape, but a required field is missing or is not a number. Every one of
    /// these throws out of the decode (<c>KeyNotFound</c>, <c>InvalidOperation</c>, <c>Format</c>) rather
    /// than falling back, so only a guard around the whole decode catches them.
    /// </summary>
    [Theory]
    [InlineData("""{"size":42}""")]                 // no version
    [InlineData("""{"version":7}""")]               // no size
    [InlineData("{}")]                              // neither
    [InlineData("""{"version":"7","size":42}""")]   // version is a string
    [InlineData("""{"version":null,"size":42}""")]  // version is null
    [InlineData("""{"version":7.5,"size":42}""")]   // version is not an integer
    [InlineData("""{"version":7,"size":42,"parts":"3"}""")]        // optional field, wrong type
    [InlineData("""{"version":7,"size":42,"sizeInBytes":null}""")] // optional field, null
    public async Task HintWithUnusableField_IsNull(string content)
    {
        WriteHint(content);

        var info = await new CheckpointReader(new LocalTableFileSystem(_tempDir))
            .ReadLastCheckpointAsync();

        Assert.Null(info);
    }

    /// <summary>
    /// <c>v2Checkpoint</c> present but not an object: <c>TryGetProperty</c> on it throws
    /// (InvalidOperationException, "requires an element of type 'Object'") — the same trap the root has.
    /// </summary>
    [Theory]
    [InlineData("""{"version":7,"size":42,"v2Checkpoint":"oops"}""")]
    [InlineData("""{"version":7,"size":42,"v2Checkpoint":[1]}""")]
    public async Task HintWithNonObjectV2Checkpoint_IsNull(string content)
    {
        WriteHint(content);

        var info = await new CheckpointReader(new LocalTableFileSystem(_tempDir))
            .ReadLastCheckpointAsync();

        Assert.Null(info);
    }

    /// <summary>
    /// The file exists but the READ fails. On ADLS a ranged read torn by a concurrent overwrite surfaces
    /// as 412 ConditionNotMet — a store-specific type this layer must not have to know.
    /// </summary>
    [Fact]
    public async Task ReadFailure_IsNull()
    {
        WriteHint("""{"version":7,"size":42}""");
        var fs = new ThrowingReadFileSystem(
            new LocalTableFileSystem(_tempDir),
            () => new IOException("simulated torn read (412 ConditionNotMet)"));

        var info = await new CheckpointReader(fs).ReadLastCheckpointAsync();

        Assert.Null(info);
    }

    /// <summary>
    /// The one exception that must NOT be swallowed: a cancel is the caller's own intent, and reporting it
    /// as "no hint" would let a cancelled open silently continue down the slow path.
    /// </summary>
    [Fact]
    public async Task Cancellation_Propagates()
    {
        WriteHint("""{"version":7,"size":42}""");
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var fs = new ThrowingReadFileSystem(
            new LocalTableFileSystem(_tempDir),
            () => new OperationCanceledException(cts.Token));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await new CheckpointReader(fs).ReadLastCheckpointAsync(cts.Token));
    }

    /// <summary>
    /// The other half of that: a store's request timeout also arrives as <c>TaskCanceledException</c> while
    /// the caller's token is untouched. That is the store failing to serve a hint, not the caller giving
    /// up, so it must fall back like any other read failure.
    /// </summary>
    [Fact]
    public async Task StoreTimeout_IsNull()
    {
        WriteHint("""{"version":7,"size":42}""");
        var fs = new ThrowingReadFileSystem(
            new LocalTableFileSystem(_tempDir),
            () => new TaskCanceledException("The request was canceled due to the configured HttpClient.Timeout"));

        var info = await new CheckpointReader(fs).ReadLastCheckpointAsync();

        Assert.Null(info);
    }

    /// <summary>Passes everything through except <see cref="ReadAllBytesAsync"/>, which throws.</summary>
    private sealed class ThrowingReadFileSystem(ITableFileSystem inner, Func<Exception> error)
        : ITableFileSystem
    {
        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
            => throw error();

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default)
            => inner.ListAsync(prefix, cancellationToken);

        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
            => inner.OpenReadAsync(path, cancellationToken);

        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default)
            => inner.CreateAsync(path, overwrite, cancellationToken);

        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
            => inner.DeleteAsync(path, cancellationToken);

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
            => inner.ExistsAsync(path, cancellationToken);

        public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.TryWriteAllBytesAsync(path, data, cancellationToken);

        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.WriteAllBytesAsync(path, data, cancellationToken);
    }
}

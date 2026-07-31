// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// <c>_last_checkpoint</c> is an OPTIMIZATION HINT, so every way of failing to read it must mean the same
/// thing as the file being absent: no hint, replay from the log. It carries no truth that a reader cannot
/// recover by listing <c>_delta_log</c>, so failing the CALLER over it turns a harmless file into a failed
/// commit.
///
/// These shapes are not hypothetical. The file is updated by OVERWRITE, and on object stores / ADLS that
/// is not atomic, so a concurrent reader can observe it at zero bytes, truncated mid-object, or — if the
/// store serves it in ranges — fail the read outright. Measured on live Fabric OneLake (2026-07-31): with
/// 8 concurrent writers x 12 commits, the empty-content shape killed 2 of 8 writers.
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
    /// POSITIVE CONTROL, and the reason the null-returning tests below mean anything: a well-formed hint
    /// IS read and returned. Without this, a guard broad enough to swallow every input would pass the
    /// whole rest of this class while silently disabling the optimization for everyone.
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
    /// The shape actually measured on OneLake: the overwrite has created the file but not yet written to
    /// it. <c>JsonDocument.Parse</c> threw "The input does not contain any JSON tokens", which reached the
    /// user as a failed commit.
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
    /// A partial write can also land as VALID JSON that happens to be missing a required field. Both
    /// fields are mandatory, and reading them with <c>GetProperty</c> would throw rather than fall back.
    /// </summary>
    [Theory]
    [InlineData("""{"size":42}""")]      // no version
    [InlineData("""{"version":7}""")]    // no size
    [InlineData("{}")]                   // neither
    public async Task HintMissingRequiredField_IsNull(string content)
    {
        WriteHint(content);

        var info = await new CheckpointReader(new LocalTableFileSystem(_tempDir))
            .ReadLastCheckpointAsync();

        Assert.Null(info);
    }

    /// <summary>
    /// The file exists but the READ fails. On ADLS a ranged read torn by a concurrent in-place overwrite
    /// surfaces as 412 ConditionNotMet — a store-specific type this layer must not have to know, hence a
    /// broad guard rather than a list of exception types.
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
        var fs = new ThrowingReadFileSystem(
            new LocalTableFileSystem(_tempDir),
            () => new OperationCanceledException());

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await new CheckpointReader(fs).ReadLastCheckpointAsync());
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

        public ValueTask<bool> RenameAsync(string sourcePath, string targetPath, CancellationToken cancellationToken = default)
            => inner.RenameAsync(sourcePath, targetPath, cancellationToken);

        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
            => inner.DeleteAsync(path, cancellationToken);

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
            => inner.ExistsAsync(path, cancellationToken);

        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.WriteAllBytesAsync(path, data, cancellationToken);
    }
}

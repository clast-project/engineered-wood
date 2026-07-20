// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.IO.Local;

/// <summary>
/// <see cref="ISequentialFile"/> implementation for local files using <see cref="FileStream"/>.
/// </summary>
public sealed class LocalSequentialFile : ISequentialFile
{
    private readonly FileStream _stream;

    /// <summary>
    /// Creates a new file for writing. Overwrites existing files.
    /// </summary>
    public LocalSequentialFile(string path)
    {
        _stream = new FileStream(path, FileMode.Create, FileAccess.Write,
            FileShare.None, bufferSize: 81920, useAsync: false);
    }

    /// <inheritdoc/>
    public long Position => _stream.Position;

    /// <inheritdoc/>
    public ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

#if NET6_0_OR_GREATER
        _stream.Write(data.Span);
#else
        byte[] array = data.ToArray();
        _stream.Write(array, 0, array.Length);
#endif
        return default;
    }

    /// <inheritdoc/>
    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        _stream.Flush();
        return default;
    }

    public void Dispose() => _stream.Dispose();

#if NET6_0_OR_GREATER
    public async ValueTask DisposeAsync() => await _stream.DisposeAsync().ConfigureAwait(false);
#else
    public ValueTask DisposeAsync()
    {
        _stream.Dispose();
        return default;
    }
#endif
}

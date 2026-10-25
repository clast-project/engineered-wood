// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>Counts the opens of, and bytes read from, data files (Parquet outside the log); everything else
/// passes through.</summary>
internal sealed class DataFileCountingFileSystem(ITableFileSystem inner) : ITableFileSystem
{
    private long _dataBytesRead;
    private int _dataFileOpens;
    private readonly System.Collections.Concurrent.ConcurrentQueue<string> _opened = new();

    public long DataBytesRead => Interlocked.Read(ref _dataBytesRead);

    public int DataFileOpens => Volatile.Read(ref _dataFileOpens);

    /// <summary>The data files opened since the last <see cref="Reset"/>, in order, as the paths passed in.</summary>
    public IReadOnlyList<string> OpenedDataFiles => _opened.ToArray();

    public void Reset()
    {
        Interlocked.Exchange(ref _dataBytesRead, 0);
        Interlocked.Exchange(ref _dataFileOpens, 0);
        while (_opened.TryDequeue(out _)) { }
    }

    public PathNameConstraints PathConstraints => inner.PathConstraints;

    public async ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
    {
        var file = await inner.OpenReadAsync(path, cancellationToken).ConfigureAwait(false);
        bool isData = path.EndsWith(".parquet", StringComparison.Ordinal)
            && path.IndexOf("_delta_log", StringComparison.Ordinal) < 0;
        if (!isData)
            return file;
        Interlocked.Increment(ref _dataFileOpens);
        _opened.Enqueue(path);
        return new CountingFile(file, this);
    }

    public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default)
        => inner.ListAsync(prefix, cancellationToken);

    public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
        => inner.ReadAllBytesAsync(path, cancellationToken);

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

    private sealed class CountingFile(IRandomAccessFile inner, DataFileCountingFileSystem owner) : IRandomAccessFile
    {
        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
            => inner.GetLengthAsync(cancellationToken);

        public ValueTask<System.Buffers.IMemoryOwner<byte>> ReadAsync(
            FileRange range, CancellationToken cancellationToken = default)
        {
            Interlocked.Add(ref owner._dataBytesRead, range.Length);
            return inner.ReadAsync(range, cancellationToken);
        }

        public ValueTask<IReadOnlyList<System.Buffers.IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
        {
            foreach (var range in ranges)
                Interlocked.Add(ref owner._dataBytesRead, range.Length);
            return inner.ReadRangesAsync(ranges, cancellationToken);
        }

        public void Dispose() => inner.Dispose();

        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}

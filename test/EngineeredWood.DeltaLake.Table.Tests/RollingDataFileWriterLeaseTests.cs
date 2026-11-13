// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// When a writer lets go of a batch handed to it under a lease (#860): once the row group holding the batch's last row
/// is written, not before, and on abandon. The end-to-end tests (<see cref="CompactionRawReleaseTests"/>) measure what is
/// left once an operation is over, which an abandon at the end would also satisfy; these pin the release to the write.
/// </summary>
public class RollingDataFileWriterLeaseTests : IDisposable
{
    private static readonly Apache.Arrow.Schema Schema =
        new Apache.Arrow.Schema.Builder().Field(new Field("v", Int32Type.Default, false)).Build();

    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;

    public RollingDataFileWriterLeaseTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-writer-lease-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        _fs = new LocalTableFileSystem(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, true); } catch { }
    }

    [Fact]
    public async Task ALease_IsHeldUntilTheRowGroupHoldingItsLastRowIsWritten()
    {
        await using var writer = Writer(rowGroupRows: 100);
        var (a, aLease) = Leased(60);
        var (b, bLease) = Leased(60);

        await writer.AddAsync(a, a, CancellationToken.None, aLease);
        aLease.Dispose();
        Assert.False(aLease.Released); // pending

        // B's first 40 rows fill the row group with A's 60; its last 20 are still pending.
        await writer.AddAsync(b, b, CancellationToken.None, bLease);
        bLease.Dispose();
        Assert.True(aLease.Released);
        Assert.False(bLease.Released);

        await writer.CompleteAsync(CancellationToken.None);
        Assert.True(bLease.Released);
    }

    [Fact]
    public async Task ALeaseOverSeveralSlices_IsHeldUntilTheLastIsWritten()
    {
        await using var writer = Writer(rowGroupRows: 100);
        var (parent, lease) = Leased(160);

        await writer.AddAsync(parent.Slice(0, 80), parent.Slice(0, 80), CancellationToken.None, lease);
        // The first slice is written with the second's first 20 rows; the second's last 60 are pending.
        await writer.AddAsync(parent.Slice(80, 80), parent.Slice(80, 80), CancellationToken.None, lease);
        lease.Dispose();
        Assert.False(lease.Released);

        await writer.CompleteAsync(CancellationToken.None);
        Assert.True(lease.Released);
    }

    [Fact]
    public async Task ALease_IsHeldUntilTheCallerLetsGo()
    {
        await using var writer = Writer(rowGroupRows: 10);
        var (batch, lease) = Leased(20);

        await writer.AddAsync(batch, batch, CancellationToken.None, lease);
        await writer.CompleteAsync(CancellationToken.None);
        Assert.False(lease.Released);

        lease.Dispose();
        Assert.True(lease.Released);
    }

    [Fact]
    public async Task UnderAHostWriter_ALease_IsHeldUntilTheHostHasWrittenTheFile()
    {
        var host = new CountingHost();
#pragma warning disable EWDELTA0001 // codec seam is experimental
        await using var writer = new RollingDataFileWriter(
            _fs, host, ParquetWriteOptions.Default, targetFileSize: 1_000, partitionDirectory: "", written: null,
            collectStats: false, statsSelection: null, tableAllowsShredding: false);
#pragma warning restore EWDELTA0001
        var (small, smallLease) = Leased(10);
        var (large, largeLease) = Leased(1_000);

        await writer.AddAsync(small, small, CancellationToken.None, smallLease);
        smallLease.Dispose();
        Assert.False(smallLease.Released); // below the target: the file is still open

        await writer.AddAsync(large, large, CancellationToken.None, largeLease);
        largeLease.Dispose();
        Assert.Equal(1, host.Files);
        Assert.True(smallLease.Released);
        Assert.True(largeLease.Released);
    }

    [Fact]
    public async Task AnAbandonedWrite_LetsGoOfItsLeases()
    {
        var (batch, lease) = Leased(20);
        await using (var writer = Writer(rowGroupRows: 100))
        {
            await writer.AddAsync(batch, batch, CancellationToken.None, lease);
            lease.Dispose();
            Assert.False(lease.Released);
        }
        Assert.True(lease.Released);
    }

    [Fact]
    public async Task WithoutALease_TheBatchStaysTheCallers()
    {
        var (batch, lease) = Leased(20);
        await using (var writer = Writer(rowGroupRows: 10))
        {
            await writer.AddAsync(batch, batch, CancellationToken.None);
            await writer.CompleteAsync(CancellationToken.None);
        }
        Assert.False(lease.Released);
        lease.Dispose();
    }

    private RollingDataFileWriter Writer(int rowGroupRows) => new(
        _fs, host: null, new ParquetWriteOptions { RowGroupMaxRows = rowGroupRows }, targetFileSize: long.MaxValue,
        partitionDirectory: "", written: null, collectStats: false, statsSelection: null, tableAllowsShredding: false);

    private static (RecordBatch Batch, BatchLease Lease) Leased(int rows)
    {
        var values = new Int32Array.Builder();
        for (int i = 0; i < rows; i++)
            values.Append(i);
        var batch = new RecordBatch(Schema, [values.Build()], rows);
        return (batch, BatchLease.Of(batch));
    }

#pragma warning disable EWDELTA0001 // codec seam is experimental
    private sealed class CountingHost : IDataFileWriter
#pragma warning restore EWDELTA0001
    {
        public int Files { get; private set; }

        public async ValueTask<long> WriteAsync(
            IAsyncEnumerable<RecordBatch> batches, string relativePath, CancellationToken cancellationToken)
        {
            await foreach (var _ in batches.WithCancellation(cancellationToken))
            {
            }
            Files++;
            return 1;
        }
    }
}

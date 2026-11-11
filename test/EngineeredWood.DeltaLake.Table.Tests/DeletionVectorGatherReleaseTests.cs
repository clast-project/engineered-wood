// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #791: the read pipeline releases a batch once its deletion vector has been applied. A batch the vector thins is
/// gathered into a new one, and one it deletes entirely is dropped; either way nothing reads the original again, and
/// its buffers must be released then rather than held until finalization. A seam builds every batch from an allocator
/// that records each buffer's release, so a buffer still live after the read and its batches' disposal is a leak.
/// </summary>
public class DeletionVectorGatherReleaseTests : IDisposable
{
    private readonly string _tempDir;

    public DeletionVectorGatherReleaseTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_dv_release_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    /// <summary>Hands out managed memory, counting the owners not yet disposed.</summary>
    private sealed class CountingAllocator : MemoryAllocator
    {
        private int _live;

        public int Live => Volatile.Read(ref _live);

        protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
        {
            bytesAllocated = length;
            Interlocked.Increment(ref _live);
            return new Owner(this, new byte[length]);
        }

        private sealed class Owner(CountingAllocator allocator, byte[] bytes) : IMemoryOwner<byte>
        {
            private int _disposed;

            public Memory<byte> Memory => bytes;

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) == 0)
                    Interlocked.Decrement(ref allocator._live);
            }
        }
    }

    /// <summary>
    /// A host that decodes with <see cref="ParquetFileReader"/> and hands each file's rows on in batches of
    /// <see cref="ChunkRows"/>, copied into buffers from <see cref="Allocator"/>.
    /// </summary>
    private sealed class AllocatingReader(ITableFileSystem fs) : IDataFileReader
    {
        public const int ChunkRows = 2;

        public CountingAllocator Allocator { get; } = new();

        public async IAsyncEnumerable<RecordBatch> ReadAsync(
            string relativePath, IReadOnlyList<string>? physicalColumns,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await using var file = await fs.OpenReadAsync(relativePath, cancellationToken);
            using var reader = new ParquetFileReader(file, ownsFile: false);
            await foreach (var decoded in reader.ReadAllAsync(columnNames: physicalColumns, cancellationToken: cancellationToken))
            {
                using (decoded)
                {
                    for (int start = 0; start < decoded.Length; start += ChunkRows)
                        yield return Copy(decoded, start, Math.Min(ChunkRows, decoded.Length - start));
                }
            }
        }

        private RecordBatch Copy(RecordBatch batch, int start, int length)
        {
            var columns = new IArrowArray[batch.ColumnCount];
            for (int c = 0; c < batch.ColumnCount; c++)
            {
                switch (batch.Column(c))
                {
                    case Int64Array ids:
                    {
                        var builder = new Int64Array.Builder();
                        for (int i = start; i < start + length; i++)
                        {
                            if (ids.GetValue(i) is { } value)
                                builder.Append(value);
                            else
                                builder.AppendNull(); // a materialized row-tracking column may hold nulls
                        }
                        columns[c] = builder.Build(Allocator);
                        break;
                    }

                    case StringArray names:
                    {
                        var builder = new StringArray.Builder();
                        for (int i = start; i < start + length; i++)
                            builder.Append(names.GetString(i));
                        columns[c] = builder.Build(Allocator);
                        break;
                    }

                    default:
                        throw new NotSupportedException(batch.Column(c).GetType().Name);
                }
            }

            return new RecordBatch(batch.Schema, columns, length);
        }
    }

    private static Apache.Arrow.Schema Schema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("name", StringType.Default, true))
            .Build();

    /// <summary>
    /// Ids 0-7, then deleted under a deletion vector: <paramref name="deleted"/>. With batches of two rows, {0, 1}
    /// drops a whole batch, {2} thins one, and {} touches none. With <paramref name="rowTracking"/>, an UPDATE of every
    /// row first rewrites them with their ids materialized in hidden columns, which the read strips, uses and must
    /// release too.
    /// </summary>
    [Theory]
    [InlineData(new long[] { 2 }, false)]               // a batch thinned: gathered
    [InlineData(new long[] { 0, 1 }, false)]            // a batch deleted entirely: dropped
    [InlineData(new long[] { 0, 1, 2, 5, 6 }, false)]  // both, across several batches
    [InlineData(new long[] { }, false)]                 // none: every batch passes through to the caller
    [InlineData(new long[] { 2 }, true)]
    [InlineData(new long[] { 0, 1 }, true)]
    [InlineData(new long[] { }, true)]
    public async Task EveryBufferIsReleasedOnceItsBatchIsDone(long[] deleted, bool rowTracking)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var seam = new AllocatingReader(fs);
        await using var table = await DeltaTable.CreateAsync(fs, Schema(),
            new DeltaTableOptions { CheckpointInterval = 0, DataFileReader = seam },
            enableDeletionVectors: true, enableRowTracking: rowTracking);

        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        for (long i = 0; i < 8; i++)
        {
            ids.Append(i);
            names.Append("n" + i);
        }

        await table.WriteAsync([new RecordBatch(table.ArrowSchema, [ids.Build(), names.Build()], 8)]);
        if (rowTracking)
        {
            await table.UpdateAsync(batch => new BooleanArray.Builder().AppendRange(Enumerable.Repeat(true, batch.Length)).Build(), batch => batch);
            var (idName, _) = DeltaLake.RowTracking.RowTrackingConfig.TryGetMaterializedColumnNames(table.CurrentSnapshot.Metadata.Configuration);
            Assert.NotNull(idName);
        }

        if (deleted.Length > 0)
        {
            var doomed = deleted.ToHashSet();
            await table.DeleteAsync(batch =>
            {
                var column = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
                var mask = new BooleanArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    mask.Append(doomed.Contains(column.GetValue(i)!.Value));
                return mask.Build();
            });
        }

        int before = seam.Allocator.Live;
        var read = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions()))
        {
            using (batch)
            {
                var column = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
                for (int i = 0; i < batch.Length; i++)
                    read.Add(column.GetValue(i)!.Value);
            }
        }

        Assert.Equal(Enumerable.Range(0, 8).Select(i => (long)i).Where(i => !deleted.Contains(i)), read.Order());
        Assert.Equal(before, seam.Allocator.Live);
    }
}

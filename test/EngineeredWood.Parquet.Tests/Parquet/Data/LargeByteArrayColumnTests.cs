// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// A BYTE_ARRAY column chunk holding more decoded bytes than one Arrow array can address (issue #157).
/// </summary>
/// <remarks>
/// Arrow addresses a string/binary data buffer with 32-bit offsets, and the buffer is handled as a
/// <c>Span&lt;byte&gt;</c>, so <see cref="int.MaxValue"/> is a structural ceiling rather than a tunable —
/// <c>LargeOffsets</c> widens the offsets but not the buffer. Before this, the decoder's running total
/// and the builder's write offset were both <c>int</c> and simply wrapped negative, surfacing as an
/// <c>ArgumentOutOfRangeException</c> from a span slice that named neither the column nor the size.
/// </remarks>
public class LargeByteArrayColumnTests
{
    // The two messages, checked directly so their wording is pinned without needing a multi-gigabyte
    // file. Every property the caller needs has to be IN the message: which column, how big, and what
    // to do about it.
    [Fact]
    public void ChunkTooLarge_NamesTheColumnTheSizeAndTheRemedy()
    {
        var ex = Assert.IsType<NotSupportedException>(
            ByteArrayCapacity.ChunkTooLarge("arr.key_value.key", 2_147_483_749L));

        Assert.Contains("arr.key_value.key", ex.Message);
        Assert.Contains("2,147,483,749", ex.Message);
        Assert.Contains("2,147,483,647", ex.Message);
        Assert.Contains(nameof(ParquetReadOptions.MaxBatchByteSize), ex.Message);
        // The nested limitation is the reason a caller may not be able to act on the advice, so it is
        // stated rather than left to be discovered.
        Assert.Contains("nested", ex.Message);
    }

    // A single value over the limit cannot be split by any batching, so its message must NOT send the
    // caller off to configure batch sizes that cannot help.
    [Fact]
    public void ValueTooLarge_NamesTheValueAndDoesNotSuggestBatching()
    {
        var ex = Assert.IsType<NotSupportedException>(
            ByteArrayCapacity.ValueTooLarge("payload", valueIndex: 5, valueLength: 3_000_000_000L));

        Assert.Contains("payload", ex.Message);
        Assert.Contains("3,000,000,000", ex.Message);
        Assert.Contains("index 5", ex.Message);
        Assert.DoesNotContain(nameof(ParquetReadOptions.MaxBatchByteSize), ex.Message);
    }

    [Fact]
    public void Describe_FallsBackWhenTheColumnPathIsUnknown()
    {
        var ex = ByteArrayCapacity.ChunkTooLarge(null, 2_147_483_749L);
        Assert.Contains("This BYTE_ARRAY column", ex.Message);
    }

    // The corpus file the issue is about: a MAP(STRING, INT32) whose key chunk is 2,147,483,749
    // uncompressed bytes, 101 past int.MaxValue. It is 4 KB on disk but decompresses to 2.1 GB, which is
    // why the corpus sweep skips it and why this is the only test that reads it.
    //
    // It stays UNREADABLE — the column is nested, and the nested read path decodes the whole row group
    // before slicing, so batching cannot help it. What this asserts is that the failure now says so.
    [Fact]
    public async Task NestedOversizedChunk_FailsWithAMessageThatNamesTheColumn()
    {
        await using var file = new LocalRandomAccessFile(
            TestData.GetPath("large_string_map.brotli.parquet"));
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            async () => await reader.ReadRowGroupAsync(0));

        Assert.Contains("arr.key_value.key", ex.Message);
        Assert.Contains("2,147,483,647", ex.Message);
        Assert.Contains("nested", ex.Message);
    }

    // The other half of the fix: a FLAT column chunk over the limit is READ, by splitting it, and the
    // caller does not have to know to ask. The file is generated rather than vendored — 2.3 GB of column
    // chunk is not something to keep in the repo, and EngineeredWood cannot write one itself because the
    // same 2 GiB ceiling applies to building the Arrow array on the way in.
    [Fact]
    public async Task FlatOversizedChunk_IsSplitAutomaticallyWithNoOptionsSet()
    {
        string path = Path.Combine(
            Path.GetTempPath(), "ew-big-flat-" + Guid.NewGuid().ToString("N")[..8] + ".parquet");
        try
        {
            long written = WriteOversizedFlatColumn(path);

            await using var file = new LocalRandomAccessFile(path);
            await using var reader = new ParquetFileReader(file, ownsFile: false);

            // No BatchSize, no MaxBatchByteSize. The split is engaged because the chunk cannot be
            // returned whole, not because the caller configured anything.
            int batches = 0;
            long rows = 0;
            long bytes = 0;
            await foreach (var batch in reader.ReadAllAsync())
            {
                batches++;
                rows += batch.Length;
                var values = Assert.IsType<Apache.Arrow.BinaryArray>(batch.Column(0));
                bytes += values.ValueOffsets[values.Length] - values.ValueOffsets[0];
                batch.Dispose();
            }

            Assert.True(batches > 1, $"expected the chunk to be split, got {batches} batch(es)");
            Assert.Equal(ValueCount, rows);
            Assert.Equal(written, bytes);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    // The single-batch API cannot split, so it refuses — and refuses from the metadata, before spending
    // the decompression. Both facts matter: the message is the diagnostic issue #157 asked for, and the
    // speed is what makes it safe to hit on a multi-gigabyte chunk.
    [Fact]
    public async Task FlatOversizedChunk_SingleBatchApiRefusesFromTheMetadata()
    {
        string path = Path.Combine(
            Path.GetTempPath(), "ew-big-flat-single-" + Guid.NewGuid().ToString("N")[..8] + ".parquet");
        try
        {
            WriteOversizedFlatColumn(path);

            await using var file = new LocalRandomAccessFile(path);
            await using var reader = new ParquetFileReader(file, ownsFile: false);

            var sw = System.Diagnostics.Stopwatch.StartNew();
            var ex = await Assert.ThrowsAsync<NotSupportedException>(
                async () => await reader.ReadRowGroupAsync(0));
            sw.Stop();

            Assert.Contains("payload", ex.Message);
            Assert.Contains(nameof(ParquetReadOptions.MaxBatchByteSize), ex.Message);

            // Decompressing 2.3 GB takes seconds; reading the footer takes milliseconds. A generous
            // bound still separates the two decisively.
            Assert.True(sw.ElapsedMilliseconds < 1000,
                $"expected refusal from metadata, took {sw.ElapsedMilliseconds}ms");
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    private const int ValueSize = 1024 * 1024;
    private const int ValueCount = 2200;      // 2.2 GiB, past int.MaxValue
    private const int WriteChunk = 100;       // 100 MiB per WriteBatch, so peak managed memory stays small

    /// <summary>
    /// Writes one row group whose single BYTE_ARRAY chunk exceeds <see cref="int.MaxValue"/> decoded
    /// bytes, via ParquetSharp. Returns the total data bytes written.
    /// </summary>
    private static long WriteOversizedFlatColumn(string path)
    {
        var value = new byte[ValueSize];
        value.AsSpan().Fill((byte)'x');

        var block = new byte[WriteChunk][];
        for (int i = 0; i < WriteChunk; i++)
            block[i] = value;

        using var properties = new ParquetSharp.WriterPropertiesBuilder()
            .Compression(ParquetSharp.Compression.Zstd)
            .DisableDictionary("payload")     // a dictionary would collapse 2200 identical values to one
            .Build();

        using (var writer = new ParquetSharp.ParquetFileWriter(
            path, [new ParquetSharp.Column<byte[]>("payload")], properties))
        using (var rowGroup = writer.AppendRowGroup())
        using (var column = rowGroup.NextColumn().LogicalWriter<byte[]>())
        {
            for (int written = 0; written < ValueCount; written += WriteChunk)
                column.WriteBatch(block);
        }

        return (long)ValueCount * ValueSize;
    }

    // Batching is offered as the remedy, so the nested case must fail the same way THROUGH it rather
    // than appearing to be a configuration the caller simply has not found yet.
    [Fact]
    public async Task NestedOversizedChunk_StillFailsClearlyUnderBatching()
    {
        await using var file = new LocalRandomAccessFile(
            TestData.GetPath("large_string_map.brotli.parquet"));
        await using var reader = new ParquetFileReader(
            file, ownsFile: false,
            new ParquetReadOptions { MaxBatchByteSize = 64L * 1024 * 1024 });

        var ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
        {
            await foreach (var _ in reader.ReadRowGroupBatchesAsync(0))
            {
            }
        });

        Assert.Contains("arr.key_value.key", ex.Message);
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// The staging buffers pages are encoded and compressed in are thread-static. A thread keeps them from call to call
/// up to <see cref="ColumnChunkWriter.StagingRetainLimit"/>, and lets a larger one go when the call ends (#940): a
/// page of one huge value left buffers of its size on the thread for as long as it lived.
/// </summary>
public class StagingBufferTests
{
    private static readonly string[] Path = ["c"];

    private static readonly ParquetWriteOptions Plain = ParquetWriteOptions.Default with { DictionaryEnabled = false };

    /// <summary>
    /// An 8 MiB value's page is written by the append that brings it at the default page size, and by
    /// <see cref="ColumnChunkWriter.Finish"/> at 16 MiB pages, which leave it open until then (#948 review). Either
    /// call lets its buffers go.
    /// </summary>
    [Theory]
    [InlineData(DataPageVersion.V1, false)]
    [InlineData(DataPageVersion.V2, false)]
    [InlineData(DataPageVersion.V1, true)]
    [InlineData(DataPageVersion.V2, true)]
    public void AHugeValue_LeavesNoBufferOfItsSize(DataPageVersion version, bool writtenByFinish)
    {
        var big = new string('x', 8 << 20);
        var values = new StringArray.Builder().Append("a").Append(big).Append("b").Build();
        var options = Plain with { DataPageVersion = version };
        if (writtenByFinish)
            options = options with { DataPageSize = 16 << 20 };
        var writer = new ColumnChunkWriter(Path, PhysicalType.ByteArray, 0, maxDefLevel: 0, maxRepLevel: 0, options);

        writer.AppendFlat(values);
        Assert.True(ColumnChunkWriter.StagingBytes <= 2L * ColumnChunkWriter.StagingRetainLimit,
            $"{ColumnChunkWriter.StagingBytes:N0} bytes of staging buffers kept after the append");

        var result = writer.Finish();
        Assert.True(ColumnChunkWriter.StagingBytes <= 2L * ColumnChunkWriter.StagingRetainLimit,
            $"{ColumnChunkWriter.StagingBytes:N0} bytes of staging buffers kept after the chunk was finished");
        Assert.True(result.MetaData.TotalUncompressedSize > 8 << 20);
        // One page of all three values, open until Finish; or a page a value, the append's.
        Assert.Equal(writtenByFinish, result.PageIndex!.PageLocations.Count == 1);
        GC.KeepAlive(values);
    }

    /// <summary>
    /// A chunk appended already dictionary-encoded, as the buffered writer builds one, lets go of its buffers too
    /// (#948 review). At 4 MiB pages, 16-bit indices make pages of 2M: the append writes two full ones (4 MiB each,
    /// compressed in a staging buffer of their size) and leaves the last index open for Finish.
    /// </summary>
    [Fact]
    public void AHugePrebuiltDictionaryPage_LeavesNoBufferOfItsSize()
    {
        const int entries = 1 << 16;
        const int rows = (4 << 20) + 1;
        var random = new Random(948);
        var dictionary = new byte[entries * 4];
        for (int i = 0; i < entries; i++)
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(dictionary.AsSpan(i * 4), i);
        var indices = new int[rows];
        for (int i = 0; i < rows; i++)
            indices[i] = random.Next(entries);
        var result = new DictionaryEncoder.DictionaryResult
        {
            DictionaryPageData = dictionary,
            DictionaryCount = entries,
            Indices = indices,
        };
        var options = ParquetWriteOptions.Default with { DataPageSize = 4 << 20, DataPageRowCountLimit = null };
        var writer = new ColumnChunkWriter(Path, PhysicalType.Int32, 0, maxDefLevel: 0, maxRepLevel: 0, options);

        writer.AppendDictionaryEncoded(
            result, Apache.Arrow.Types.Int32Type.Default, null, null, nonNullCount: rows, levelCount: rows);
        Assert.True(ColumnChunkWriter.StagingBytes <= 2L * ColumnChunkWriter.StagingRetainLimit,
            $"{ColumnChunkWriter.StagingBytes:N0} bytes of staging buffers kept after the append");

        var chunk = writer.Finish();
        Assert.True(ColumnChunkWriter.StagingBytes <= 2L * ColumnChunkWriter.StagingRetainLimit,
            $"{ColumnChunkWriter.StagingBytes:N0} bytes of staging buffers kept after the chunk was finished");
        Assert.True(chunk.MetaData.TotalUncompressedSize > 8 << 20);
        Assert.Equal(3, chunk.PageIndex!.PageLocations.Count); // two by the append, the last by Finish
    }

    /// <summary>Pages of the default size keep their buffers, for the next call on the thread to reuse.</summary>
    [Fact]
    public void DefaultPages_KeepTheirBuffers()
    {
        var builder = new StringArray.Builder();
        for (int i = 0; i < 100_000; i++)
            builder.Append("value-" + i);
        var values = builder.Build();
        var writer = new ColumnChunkWriter(Path, PhysicalType.ByteArray, 0, maxDefLevel: 0, maxRepLevel: 0, Plain);

        writer.AppendFlat(values);
        writer.Finish();

        Assert.InRange(ColumnChunkWriter.StagingBytes, 1, 2L * ColumnChunkWriter.StagingRetainLimit);
        GC.KeepAlive(values);
    }
}

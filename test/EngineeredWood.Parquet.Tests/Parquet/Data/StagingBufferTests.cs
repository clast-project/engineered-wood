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

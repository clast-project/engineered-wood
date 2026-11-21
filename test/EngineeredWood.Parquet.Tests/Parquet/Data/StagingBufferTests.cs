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

    [Theory]
    [InlineData(DataPageVersion.V1)]
    [InlineData(DataPageVersion.V2)]
    public void AHugeValue_LeavesNoBufferOfItsSize(DataPageVersion version)
    {
        var big = new string('x', 8 << 20);
        var values = new StringArray.Builder().Append("a").Append(big).Append("b").Build();
        var writer = new ColumnChunkWriter(
            Path, PhysicalType.ByteArray, 0, maxDefLevel: 0, maxRepLevel: 0, Plain with { DataPageVersion = version });

        writer.AppendFlat(values); // the huge value's page is written here
        Assert.True(ColumnChunkWriter.StagingBytes <= 2L * ColumnChunkWriter.StagingRetainLimit,
            $"{ColumnChunkWriter.StagingBytes:N0} bytes of staging buffers kept after the append");

        var result = writer.Finish();
        Assert.True(ColumnChunkWriter.StagingBytes <= 2L * ColumnChunkWriter.StagingRetainLimit,
            $"{ColumnChunkWriter.StagingBytes:N0} bytes of staging buffers kept after the chunk was finished");
        Assert.True(result.MetaData.TotalUncompressedSize > 8 << 20);
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

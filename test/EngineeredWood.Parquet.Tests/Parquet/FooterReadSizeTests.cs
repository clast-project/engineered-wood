// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// <see cref="ParquetReadOptions.FooterReadSize"/>: the footer is read in one request when it fits in the
/// speculative tail, in two (the 8-byte suffix, then the footer) when it does not or the knob is off (#722).
/// </summary>
public class FooterReadSizeTests
{
    private static readonly string Path = TestData.GetPath("alltypes_plain.parquet");

    private static async Task<(RequestCountingFile File, long Length, EngineeredWood.Parquet.Metadata.FileMetaData Footer)> ReadFooterAsync(
        int footerReadSize)
    {
        var file = new RequestCountingFile(new LocalRandomAccessFile(Path));
        using var reader = new ParquetFileReader(
            file, ownsFile: false, ParquetReadOptions.Default with { FooterReadSize = footerReadSize });
        var footer = await reader.ReadMetadataAsync();
        long length = await file.GetLengthAsync();
        return (file, length, footer);
    }

    [Fact]
    public async Task Default_ReadsASmallFileWholeInOneRequest()
    {
        var file = new RequestCountingFile(new LocalRandomAccessFile(Path));
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var footer = await reader.ReadMetadataAsync();
        long length = await file.GetLengthAsync();

        Assert.True(length < ParquetReadOptions.DefaultFooterReadSize);
        var range = Assert.Single(Assert.Single(file.RequestRanges));
        Assert.Equal(new FileRange(0, length), range);
        Assert.Equal(8, footer.NumRows);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(8)]
    public async Task Off_ReadsTheSuffixThenTheFooter(int footerReadSize)
    {
        var (file, length, footer) = await ReadFooterAsync(footerReadSize);

        Assert.Equal(2, file.RequestRanges.Count);
        Assert.Equal(new FileRange(length - 8, 8), Assert.Single(file.RequestRanges[0]));
        var footerRange = Assert.Single(file.RequestRanges[1]);
        Assert.Equal(length - 8, footerRange.Offset + footerRange.Length);
        Assert.Equal(8, footer.NumRows);
    }

    [Fact]
    public async Task ATailThatJustHoldsTheFooter_IsOneRequest()
    {
        var (exact, length, _) = await ReadFooterAsync(0);
        int footerLength = (int)Assert.Single(exact.RequestRanges[1]).Length;

        var (file, _, footer) = await ReadFooterAsync(footerLength + 8);

        Assert.Equal(new FileRange(length - footerLength - 8, footerLength + 8), Assert.Single(Assert.Single(file.RequestRanges)));
        Assert.Equal(8, footer.NumRows);
    }

    [Fact]
    public async Task AFooterLargerThanTheTail_IsReadAgainExactly()
    {
        var (exact, length, _) = await ReadFooterAsync(0);
        var footerRange = Assert.Single(exact.RequestRanges[1]);

        var (file, _, footer) = await ReadFooterAsync((int)footerRange.Length + 7);

        Assert.Equal(2, file.RequestRanges.Count);
        Assert.Equal(new FileRange(length - footerRange.Length - 7, footerRange.Length + 7),
            Assert.Single(file.RequestRanges[0]));
        Assert.Equal(footerRange, Assert.Single(file.RequestRanges[1]));
        Assert.Equal(8, footer.NumRows);
    }
}

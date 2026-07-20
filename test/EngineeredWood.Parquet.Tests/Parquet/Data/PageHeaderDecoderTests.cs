// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

public class PageHeaderDecoderTests
{
    [Fact]
    public void Decode_FromRealFile_ParsesFirstPage()
    {
        // Read raw bytes from alltypes_plain.parquet and find the first data page
        var fileBytes = File.ReadAllBytes(TestData.GetPath("alltypes_plain.parquet"));

        // Use metadata to find the first column chunk's data page offset
        var footerBytes = TestData.ReadFooterBytes("alltypes_plain.parquet");
        var metadata = EngineeredWood.Parquet.Metadata.MetadataDecoder.DecodeFileMetaData(footerBytes);
        var firstCol = metadata.RowGroups[0].Columns[0].MetaData!;

        int pageStart = checked((int)firstCol.DataPageOffset);
        var pageSpan = fileBytes.AsSpan(pageStart);

        var header = PageHeaderDecoder.Decode(pageSpan, out int bytesConsumed);

        Assert.True(bytesConsumed > 0);
        Assert.True(header.CompressedPageSize > 0);
        Assert.True(header.UncompressedPageSize > 0);
        Assert.True(
            header.Type == PageType.DataPage || header.Type == PageType.DataPageV2,
            $"Expected data page but got {header.Type}");

        if (header.Type == PageType.DataPage)
        {
            Assert.NotNull(header.DataPageHeader);
            Assert.True(header.DataPageHeader!.NumValues > 0);
        }
    }
}

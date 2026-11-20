// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// The incremental column chunk writer's contract (#936 step 1): one writer per leaf, appended to and then
/// finished, with the pages that precede the data pages kept apart until the chunk is finished.
/// </summary>
public class ColumnChunkWriterTests
{
    private static readonly string[] Path = ["c"];

    private static Int32Array Ints(int count, int distinct)
    {
        var builder = new Int32Array.Builder();
        for (int i = 0; i < count; i++)
            builder.Append(i % distinct);
        return builder.Build();
    }

    private static ColumnChunkWriter NewInt32Writer(ParquetWriteOptions? options = null) =>
        new(Path, PhysicalType.Int32, 0, maxDefLevel: 0, maxRepLevel: 0, options ?? ParquetWriteOptions.Default);

    [Fact]
    public void SecondAppend_IsNotSupportedYet()
    {
        var writer = NewInt32Writer();
        writer.AppendFlat(Ints(100, 10));

        // Multi-batch chunks are step 2 of #936; until then a second append must not silently start a
        // second dictionary or re-decide the encoding.
        Assert.Throws<NotSupportedException>(() => writer.AppendFlat(Ints(100, 10)));
    }

    [Fact]
    public void Finish_WithoutAppend_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => NewInt32Writer().Finish());
    }

    [Fact]
    public void Finished_Writer_TakesNothingMore()
    {
        var writer = NewInt32Writer();
        writer.AppendFlat(Ints(100, 10));
        writer.Finish();

        Assert.Throws<InvalidOperationException>(() => writer.Finish());
        Assert.Throws<InvalidOperationException>(() => writer.AppendFlat(Ints(100, 10)));
    }

    [Fact]
    public void DictionaryChunk_DictionaryPageIsThePrefix()
    {
        var writer = NewInt32Writer();
        writer.AppendFlat(Ints(10_000, 10));
        var result = writer.Finish();

        Assert.True(result.DictionaryPageSize > 0);
        Assert.Equal(result.DictionaryPageSize, result.PrefixData.Count);

        // The dictionary page is written last but placed first: the page index and encoding_stats see the
        // chunk in file order.
        Assert.Equal(result.PrefixData.Count, result.PageIndex!.PageLocations[0].Offset);
        Assert.Equal(PageType.DictionaryPage, result.MetaData.EncodingStats![0].PageType);
        Assert.Equal(
            result.PrefixData.Count + result.Data.Count,
            result.MetaData.TotalCompressedSize);
    }

    [Fact]
    public void PlainChunk_HasNoPrefix()
    {
        var writer = NewInt32Writer(ParquetWriteOptions.Default with { DictionaryEnabled = false });
        writer.AppendFlat(Ints(10_000, 10_000));
        var result = writer.Finish();

        Assert.Equal(0, result.PrefixData.Count);
        Assert.Equal(0, result.DictionaryPageSize);
        Assert.Equal(0, result.PageIndex!.PageLocations[0].Offset);
        Assert.Equal(result.Data.Count, result.MetaData.TotalCompressedSize);
    }

#pragma warning disable EWPARQUET0003 // FSST is experimental; this test opts in on purpose.
    [Fact]
    public void FsstChunk_SymbolTablePageIsThePrefix()
    {
        var builder = new StringArray.Builder();
        for (int i = 0; i < 5_000; i++)
            builder.Append("https://example.com/items/" + (i * 7919 % 100_000));

        var writer = new ColumnChunkWriter(Path, PhysicalType.ByteArray, 0, maxDefLevel: 0, maxRepLevel: 0,
            ParquetWriteOptions.Default with { ByteArrayEncoding = ByteArrayEncoding.Fsst, DictionaryEnabled = false });
        writer.AppendFlat(builder.Build());
        var result = writer.Finish();

        Assert.True(result.SymbolTablePageSize > 0);
        Assert.Equal(result.SymbolTablePageSize, result.PrefixData.Count);
        Assert.Equal(result.PrefixData.Count, result.PageIndex!.PageLocations[0].Offset);
    }
#pragma warning restore EWPARQUET0003

    /// <summary>
    /// The buffered writer's dictionary columns reach the writer already encoded. That path used to encode
    /// them with the file-wide options, so a per-column codec was ignored for exactly those columns.
    /// </summary>
    [Fact]
    public void DictionaryEncodedAppend_HonoursPerColumnCodec()
    {
        var options = ParquetWriteOptions.Default with
        {
            Compression = EngineeredWood.Compression.CompressionCodec.Snappy,
            ColumnCodecs = new Dictionary<string, EngineeredWood.Compression.CompressionCodec>
            {
                ["c"] = EngineeredWood.Compression.CompressionCodec.Zstd,
            },
        };
        var dictionary = new DictionaryEncoder.DictionaryResult
        {
            DictionaryPageData = new byte[8],
            DictionaryCount = 2,
            Indices = [0, 1, 1, 0],
        };

        var result = ColumnChunkWriter.WriteDictionaryColumnFromResult(
            dictionary, rowCount: 4, nonNullCount: 4, Path, PhysicalType.Int32, 0,
            Apache.Arrow.Types.Int32Type.Default, maxDefLevel: 0, maxRepLevel: 0, null, null, options);

        Assert.Equal(EngineeredWood.Compression.CompressionCodec.Zstd, result.MetaData.Codec);
    }

    [Fact]
    public void AppendDictionaryEncoded_RefusesAPartialDictionary()
    {
        var partial = new DictionaryEncoder.DictionaryResult
        {
            DictionaryPageData = new byte[4],
            DictionaryCount = 1,
            Indices = new int[2],
            PartialLength = 2,
        };

        Assert.Throws<ArgumentException>(() => NewInt32Writer().AppendDictionaryEncoded(
            partial, Apache.Arrow.Types.Int32Type.Default, null, null, nonNullCount: 4, levelCount: 4));
    }
}

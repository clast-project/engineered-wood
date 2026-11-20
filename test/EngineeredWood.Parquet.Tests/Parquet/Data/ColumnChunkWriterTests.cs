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

    private static ColumnChunkWriter.ColumnChunkResult InAppends(
        Func<ColumnChunkWriter> newWriter, IArrowArray values, int piece, bool detach)
    {
        var writer = newWriter();
        for (int offset = 0; offset < values.Length; offset += piece)
        {
            int length = Math.Min(piece, values.Length - offset);
            writer.AppendFlat(EngineeredWood.Arrow.ArrowCompute.Take(
                values, Enumerable.Range(offset, length).ToArray()));
            if (detach)
                writer.Detach();
        }
        return writer.Finish();
    }

    private static ColumnChunkWriter.ColumnChunkResult InOneAppend(Func<ColumnChunkWriter> newWriter, IArrowArray values)
    {
        var writer = newWriter();
        writer.AppendFlat(values);
        return writer.Finish();
    }

    /// <summary>
    /// The open page carries over between appends, so pages are cut where one append would cut them: for a
    /// column without a dictionary, appends of seven rows write the chunk byte for byte as one append does.
    /// </summary>
    [Theory]
    [InlineData(DataPageVersion.V2, false)]
    [InlineData(DataPageVersion.V2, true)]
    [InlineData(DataPageVersion.V1, true)]
    public void ManyAppends_CutThePagesOneAppendWould(DataPageVersion version, bool detach)
    {
        var options = ParquetWriteOptions.Default with
        {
            DictionaryEnabled = false, DataPageSize = 4096, DataPageVersion = version,
        };
        ColumnChunkWriter NewWriter() =>
            new(Path, PhysicalType.Int32, 0, maxDefLevel: 0, maxRepLevel: 0, options);
        var values = Ints(20_000, 20_000);

        var whole = InOneAppend(NewWriter, values);
        var appended = InAppends(NewWriter, values, piece: 7, detach);

        Assert.True(whole.PageIndex!.PageLocations.Count > 10);
        Assert.Equal(whole.Data.ToArray(), appended.Data.ToArray());
        Assert.Equal(
            whole.PageIndex.PageLocations.Select(p => p.FirstRowIndex),
            appended.PageIndex!.PageLocations.Select(p => p.FirstRowIndex));
        Assert.Equal(whole.MetaData.Statistics!.MinValue, appended.MetaData.Statistics!.MinValue);
        Assert.Equal(whole.MetaData.Statistics.MaxValue, appended.MetaData.Statistics.MaxValue);
    }

    /// <summary>
    /// A dictionary that grows across appends keeps one table: the dictionary page, written at the end, holds
    /// every entry, and each page's indices are packed at the bit width in force when it was emitted.
    /// </summary>
    [Fact]
    public void GrowingDictionary_KeepsItsTableAcrossAppends()
    {
        var writer = NewInt32Writer(ParquetWriteOptions.Default with { DataPageSize = 1024 });
        // Two distinct values, then twenty, then two hundred.
        foreach (int distinct in new[] { 2, 20, 200 })
        {
            writer.AppendFlat(Ints(300_000, distinct));
            writer.Detach();
        }
        var result = writer.Finish();

        // 200 PLAIN int32 entries behind a page header.
        Assert.InRange(result.DictionaryPageSize, 200 * sizeof(int), 200 * sizeof(int) + 40);
        Assert.Equal(new[] { Encoding.Plain, Encoding.RleDictionary }, result.MetaData.Encodings);
        Assert.Equal(900_000, result.MetaData.NumValues);
        Assert.Equal(0, BitConverter.ToInt32(result.MetaData.Statistics!.MinValue!, 0));
        Assert.Equal(199, BitConverter.ToInt32(result.MetaData.Statistics.MaxValue!, 0));
    }

    /// <summary>
    /// An undecided dictionary holds an append's values until its first-page test; Detach copies them, and the open
    /// page, so the caller's arrays can go. The chunk comes out as it would have without the copies.
    /// </summary>
    [Fact]
    public void Detach_CopiesWhatTheChunkHolds_AndChangesNothing()
    {
        var values = Ints(5_000, 50);
        ColumnChunkWriter NewWriter() => NewInt32Writer();

        var kept = InAppends(NewWriter, values, piece: 333, detach: false);
        var detached = InAppends(NewWriter, values, piece: 333, detach: true);

        Assert.Equal(kept.PrefixData.ToArray(), detached.PrefixData.ToArray());
        Assert.Equal(kept.Data.ToArray(), detached.Data.ToArray());
    }

    /// <summary>
    /// High-cardinality appends below the first-page test's sample are held, and when the test fails the held
    /// values are written in the column's own encoding: no dictionary page at all.
    /// </summary>
    [Fact]
    public void DeclinedDictionary_WritesTheHeldAppendsPlain()
    {
        var values = Ints(400_000, 400_000);
        var result = InAppends(() => NewInt32Writer(), values, piece: 1_000, detach: true);

        Assert.Equal(0, result.DictionaryPageSize);
        Assert.DoesNotContain(Encoding.RleDictionary, result.MetaData.Encodings);
        Assert.Equal(400_000, result.MetaData.NumValues);
    }

    [Fact]
    public void EstimatedSize_GrowsWithTheAppends_AndEndsNearTheChunk()
    {
        var writer = NewInt32Writer(ParquetWriteOptions.Default with { DictionaryEnabled = false });
        long previous = 0;
        for (int i = 0; i < 5; i++)
        {
            writer.AppendFlat(Ints(100_000, 100_000));
            writer.Detach();
            Assert.True(writer.EstimatedSize > previous);
            previous = writer.EstimatedSize;
        }

        var result = writer.Finish();
        Assert.InRange(previous, result.MetaData.TotalCompressedSize * 9 / 10, result.MetaData.TotalCompressedSize * 11 / 10);
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

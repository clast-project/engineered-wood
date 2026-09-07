// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using Xunit;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// PARQUET-246: parquet-mr before 1.8.0 wrote DELTA_BYTE_ARRAY pages whose FIRST value takes its
/// prefix from the LAST VALUE OF THE PREVIOUS PAGE, so the pages are not independently decodable.
/// Our decoder used to be page-local and silently copied NUL bytes where that prefix belonged.
/// </summary>
/// <remarks>
/// These pages are built by hand because <c>parquet-testing</c> has no affected file: every
/// DELTA_BYTE_ARRAY fixture in it is parquet-mr 1.10.0 or later, i.e. all post-fix. Our own writer
/// cannot produce one either, since it restarts the prefix at each page as a conforming writer must.
/// </remarks>
public class DeltaByteArrayCrossPageTests
{
    /// <summary>Builds one DELTA_BYTE_ARRAY page from explicit prefix lengths and suffixes.</summary>
    private static byte[] Page(int[] prefixLengths, string[] suffixes)
    {
        var prefixEncoder = new DeltaBinaryPackedEncoder();
        prefixEncoder.EncodeInt32s(prefixLengths);

        var suffixBytes = suffixes.Select(System.Text.Encoding.UTF8.GetBytes).ToArray();
        var lengthEncoder = new DeltaBinaryPackedEncoder();
        lengthEncoder.EncodeInt32s(suffixBytes.Select(b => b.Length).ToArray());

        var page = new List<byte>();
        page.AddRange(prefixEncoder.ToArray());
        page.AddRange(lengthEncoder.ToArray());
        foreach (var s in suffixBytes)
            page.AddRange(s);
        return page.ToArray();
    }

    private static ColumnBuildState NewState() =>
        new(PhysicalType.ByteArray, maxDefLevel: 0, maxRepLevel: 0, capacity: 16);

    private static string[] Values(ColumnBuildState state, int count)
    {
        var field = new Field("s", StringType.Default, nullable: false);
        var array = (StringArray)ArrowArrayBuilder.Build(state, field, count);
        return Enumerable.Range(0, count).Select(i => array.GetString(i)).ToArray();
    }

    // Page 1 ends with "prefix-bbb"; page 2's first value continues it: prefix 7 ("prefix-") + "ccc".
    private static byte[] FirstPage() => Page([0, 7], ["prefix-aaa", "bbb"]);
    private static byte[] SecondPage() => Page([7, 10], ["ccc", "ddd"]);

    /// <summary>
    /// Read in order, the affected page decodes CORRECTLY: the previous page's last value is carried
    /// across the boundary, which is what parquet-java does when splitting is disabled.
    /// </summary>
    [Fact]
    public void PagesReadInOrder_CrossPagePrefixResolvesAgainstThePreviousPage()
    {
        using var state = NewState();

        DeltaByteArrayDecoder.Decode(FirstPage(), 2, state);
        DeltaByteArrayDecoder.Decode(SecondPage(), 2, state);

        Assert.Equal(
            ["prefix-aaa", "prefix-bbb", "prefix-ccc", "prefix-cccddd"],
            Values(state, 4));
    }

    /// <summary>
    /// The same page decoded WITHOUT its predecessor must fail rather than return NUL-padded values.
    /// This is the regression: a page-local decoder copies zeros out of its own empty output buffer,
    /// and nothing downstream can tell those from real data.
    /// </summary>
    [Fact]
    public void PageDecodedWithoutItsPredecessor_ThrowsInsteadOfReturningNulBytes()
    {
        using var state = NewState();

        var ex = Assert.Throws<ParquetFormatException>(
            () => DeltaByteArrayDecoder.Decode(SecondPage(), 2, state));

        Assert.Contains("PARQUET-246", ex.Message);
        Assert.Contains("read the column chunk from its first page", ex.Message);
    }

    /// <summary>
    /// A conforming file is unaffected by the carry: its pages restart the prefix, so the first
    /// value's prefix length is zero and the carried value is never consulted.
    /// </summary>
    [Fact]
    public void ConformingPages_DecodeIdenticallyWithOrWithoutAPrecedingPage()
    {
        var conforming = Page([0, 7], ["prefix-eee", "fff"]);

        using var fresh = NewState();
        DeltaByteArrayDecoder.Decode(conforming, 2, fresh);

        using var afterAnother = NewState();
        DeltaByteArrayDecoder.Decode(FirstPage(), 2, afterAnother);
        DeltaByteArrayDecoder.Decode(conforming, 2, afterAnother);

        Assert.Equal(["prefix-eee", "prefix-fff"], Values(fresh, 2));
        Assert.Equal(["prefix-eee", "prefix-fff"], Values(afterAnother, 4).Skip(2).ToArray());
    }
}

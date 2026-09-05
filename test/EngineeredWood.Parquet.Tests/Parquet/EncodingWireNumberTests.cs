// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#pragma warning disable EWPARQUET0001 // Pinning the experimental enum values is the point of this file.
#pragma warning disable EWPARQUET0003
#pragma warning disable EWPARQUET0005

using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Pins the numeric values of <see cref="Encoding"/>. These are wire constants, not
/// implementation details: the number is what a page header carries, so renumbering one is a
/// format change that no round-trip test can see — this library would write and read its own
/// files happily either way, and only disagree with other implementations.
/// </summary>
public class EncodingWireNumberTests
{
    [Theory]
    [InlineData(Encoding.Plain, 0)]
    [InlineData(Encoding.PlainDictionary, 2)]
    [InlineData(Encoding.Rle, 3)]
    [InlineData(Encoding.BitPacked, 4)]
    [InlineData(Encoding.DeltaBinaryPacked, 5)]
    [InlineData(Encoding.DeltaLengthByteArray, 6)]
    [InlineData(Encoding.DeltaByteArray, 7)]
    [InlineData(Encoding.RleDictionary, 8)]
    [InlineData(Encoding.ByteStreamSplit, 9)]
    public void RatifiedEncodingsHaveTheirSpecNumbers(Encoding encoding, int expected)
    {
        Assert.Equal(expected, (int)encoding);
    }

    /// <summary>
    /// ALP is 10 in <c>parquet.thrift</c> on parquet-format <c>main</c>. Unlike the
    /// experimental encodings below it, its number is settled.
    /// </summary>
    [Fact]
    public void AlpIsTen()
    {
        Assert.Equal(10, (int)Encoding.Alp);
    }

    /// <summary>
    /// FSST's proposal asks for 10 and does not get it: ALP holds 10, and PFOR
    /// (apache/parquet-format#617) holds 11. See the remarks on <see cref="Encoding.Fsst"/>.
    /// </summary>
    [Fact]
    public void FsstIsTwelve()
    {
        Assert.Equal(12, (int)Encoding.Fsst);
    }

    /// <summary>
    /// PFOR is 11 in apache/parquet-format#617, and in both implementations behind it. This is
    /// the slot FSST used to hold; a file written with 11 meaning FSST is misread rather than
    /// rejected by a PFOR reader, since nothing in the page body disagrees with the byte.
    /// </summary>
    [Fact]
    public void PforIsEleven()
    {
        Assert.Equal(11, (int)Encoding.Pfor);
    }

    /// <summary>
    /// No two encodings share a number. Cheap to assert and the one thing that would make a
    /// written page unreadable in a way no round-trip test can see.
    /// </summary>
    [Fact]
    public void EveryEncodingNumberIsDistinct()
    {
        // Enum.GetValues(Type) rather than the generic overload: this assembly also targets
        // net472, where Enum.GetValues<T>() does not exist.
        var assigned = Enum.GetValues(typeof(Encoding)).Cast<Encoding>().Select(e => (int)e).ToList();
        Assert.Equal(assigned.Count, assigned.Distinct().Count());
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Parquet.Data;
using ParquetTimeUnit = EngineeredWood.Parquet.Metadata.TimeUnit;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// The 96-bit little-endian carrier behind a TIMESTAMP-annotated FIXED_LEN_BYTE_ARRAY(12)
/// (apache/parquet-format#601).
///
/// The encodings asserted here are not self-derived: every one of the eighteen byte sequences below
/// was confirmed to appear verbatim in <c>flba12_timestamp.parquet</c>, the conformance fixture
/// proposed in apache/parquet-testing#123. Six timestamps in three units, and two of the nanosecond
/// values need more than 64 bits — which is the whole reason the carrier exists.
///
/// These run on net472 as well as net10.0/net8.0, which matters: netstandard2.0 has no
/// <see cref="Int128"/> of its own, so on that leg the arithmetic is Clast.DatabaseDecimal's
/// software polyfill rather than the BCL type. If the polyfill were wrong, only this leg would say so.
/// </summary>
public class ExtendedTimestampTests
{
    // Row, then unit. Epoch seconds: 0, +1, -1, near the INT64-nanos edge, year 9999, year 0001.
    public static TheoryData<string, string> FixtureEncodings => new()
    {
        // millis
        { "0", "000000000000000000000000" },
        { "1000", "e80300000000000000000000" },
        { "-1000", "18fcffffffffffffffffffff" },
        { "9223372036000", "a057d07b6308000000000000" },
        { "253402300799000", "18d81fd277e6000000000000" },
        { "-62135596800000", "0028d3ed7cc7ffffffffffff" },
        // micros
        { "1000000", "40420f000000000000000000" },
        { "-1000000", "c0bdf0ffffffffffffffffff" },
        { "9223372036000000", "0049d6a59bc4200000000000" },
        { "253402300799000000", "c01d64cc0c44840300000000" },
        { "-62135596800000000", "0040d400014023ffffffffff" },
        // nanos — the last two exceed Int64 in both directions
        { "1000000000", "00ca9a3b0000000000000000" },
        { "-1000000000", "003665c4ffffffffffffffff" },
        { "9223372036000000000", "00280dcdffffff7f00000000" },
        { "253402300799000000000", "00361467fed1a9bc0d000000" },
        { "-62135596800000000000", "00001a3deb03b2a1fcffffff" },
    };

    // The polyfill has no Int128.Parse, so decimal strings are rebuilt by hand. This is exactly the
    // gap noted on the package reference, and it is why the vectors above are hex rather than decimal.
    private static Int128 ParseInt128(string text)
    {
        bool negative = text[0] == '-';
        Int128 value = Int128.Zero;
        foreach (char c in negative ? text.Substring(1) : text)
        {
            value = (value * (Int128)10) + (Int128)(c - '0');
        }

        return negative ? -value : value;
    }

    private static byte[] FromHex(string hex)
    {
        var bytes = new byte[hex.Length / 2];
        for (int i = 0; i < bytes.Length; i++)
        {
            bytes[i] = byte.Parse(hex.Substring(i * 2, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        }

        return bytes;
    }

    private static string ToHex(ReadOnlySpan<byte> bytes)
    {
        var chars = new char[bytes.Length * 2];
        for (int i = 0; i < bytes.Length; i++)
        {
            chars[i * 2] = "0123456789abcdef"[bytes[i] >> 4];
            chars[(i * 2) + 1] = "0123456789abcdef"[bytes[i] & 0xF];
        }

        return new string(chars);
    }

    [Theory]
    [MemberData(nameof(FixtureEncodings))]
    public void DecodesTheFixtureBytes(string expected, string hex)
    {
        Assert.Equal(expected, ExtendedTimestamp.Read(FromHex(hex)).ToString());
    }

    [Theory]
    [MemberData(nameof(FixtureEncodings))]
    public void EncodesToTheFixtureBytes(string value, string expectedHex)
    {
        Span<byte> buffer = stackalloc byte[ExtendedTimestamp.ByteWidth];
        ExtendedTimestamp.Write(ParseInt128(value), buffer);

        Assert.Equal(expectedHex, ToHex(buffer));
    }

    [Fact]
    public void TheCarrierIsTwelveBytesWide()
    {
        // The annotation is legal at exactly this width; nothing else is a carrier.
        Assert.Equal(12, ExtendedTimestamp.ByteWidth);
    }

    // Ordered smallest to largest. The interesting entries are +2^63 — which an implementation that
    // compared the low eight bytes SIGNED would rank below +1 — and +256, which one that compared
    // bytes LSB-first would rank below +1 for the opposite reason.
    private static readonly string[] AscendingHex =
    [
        "000000000000000000000080",                          // -2^95, the most negative value
        "00ffffffffffffffffffffff",                          // -256
        "ffffffffffffffffffffffff",                          // -1
        "000000000000000000000000",                          // 0
        "010000000000000000000000",                          // +1
        "000100000000000000000000",                          // +256
        "000000000000008000000000",                          // +2^63
        "ffffffffffffffffffffff7f",                          // +2^95 - 1, the most positive value
    ];

    [Fact]
    public void ComparesAsASigned96BitInteger()
    {
        for (int i = 1; i < AscendingHex.Length; i++)
        {
            var lower = FromHex(AscendingHex[i - 1]);
            var higher = FromHex(AscendingHex[i]);

            Assert.True(ExtendedTimestamp.Compare(lower, higher) < 0, $"{AscendingHex[i - 1]} < {AscendingHex[i]}");
            Assert.True(ExtendedTimestamp.Compare(higher, lower) > 0, $"{AscendingHex[i]} > {AscendingHex[i - 1]}");
            Assert.Equal(0, ExtendedTimestamp.Compare(lower, lower));
        }
    }

    [Fact]
    public void TheByteComparatorAgreesWithInt128()
    {
        // The comparator skips Int128 for speed; this is what keeps the shortcut honest.
        for (int i = 0; i < AscendingHex.Length; i++)
        {
            for (int j = 0; j < AscendingHex.Length; j++)
            {
                int viaBytes = Math.Sign(ExtendedTimestamp.Compare(
                    FromHex(AscendingHex[i]), FromHex(AscendingHex[j])));
                int viaInt128 = Math.Sign(ExtendedTimestamp.Read(FromHex(AscendingHex[i]))
                    .CompareTo(ExtendedTimestamp.Read(FromHex(AscendingHex[j]))));

                Assert.Equal(viaInt128, viaBytes);
            }
        }
    }

    [Fact]
    public void LexicographicByteOrderWouldHaveBeenWrong()
    {
        // Guards against anyone "simplifying" this to SequenceCompareTo, which is what every other
        // FIXED_LEN_BYTE_ARRAY column is compared with. -1 encodes as all-0xFF and would sort highest.
        var negativeOne = FromHex("ffffffffffffffffffffffff");
        var one = FromHex("010000000000000000000000");

        Assert.True(ExtendedTimestamp.Compare(negativeOne, one) < 0);
        Assert.True(negativeOne.AsSpan().SequenceCompareTo(one.AsSpan()) > 0);
    }

    [Theory]
    [InlineData("9223372036000000000", true)]
    [InlineData("-9223372036000000000", true)]
    [InlineData("253402300799000000000", false)]
    [InlineData("-62135596800000000000", false)]
    public void NarrowsToInt64OnlyWhenItFits(string value, bool expected)
    {
        var parsed = ParseInt128(value);

        Assert.Equal(expected, ExtendedTimestamp.TryToInt64(parsed, out long narrowed));
        if (expected)
        {
            Assert.Equal(parsed.ToString(), narrowed.ToString(CultureInfo.InvariantCulture));
        }
    }

    [Fact]
    public void TheInt64BoundariesThemselvesFit()
    {
        Assert.True(ExtendedTimestamp.TryToInt64((Int128)long.MaxValue, out long max));
        Assert.Equal(long.MaxValue, max);
        Assert.True(ExtendedTimestamp.TryToInt64((Int128)long.MinValue, out long min));
        Assert.Equal(long.MinValue, min);
        Assert.False(ExtendedTimestamp.TryToInt64((Int128)long.MaxValue + Int128.One, out _));
        Assert.False(ExtendedTimestamp.TryToInt64((Int128)long.MinValue - Int128.One, out _));
    }

    [Theory]
    [InlineData("2000", "2")]
    [InlineData("2999", "2")]
    [InlineData("0", "0")]
    [InlineData("-2000", "-2")]
    // Truncation toward zero would give -2 here, which is the whole point: a pre-epoch value would
    // round the opposite way from a post-epoch one and stop being monotonic.
    [InlineData("-2001", "-3")]
    [InlineData("-1", "-1")]
    [InlineData("-62135596800000000001", "-62135596800000001")]
    [InlineData("253402300799000000000", "253402300799000000")]
    public void RescalingFloorsRatherThanTruncates(string value, string expected)
    {
        Assert.Equal(expected, ExtendedTimestamp.DivideFloor(ParseInt128(value), 1000).ToString());
    }

    [Fact]
    public void FloorDivisionIsMonotonic()
    {
        // The property the floor exists for, checked across the sign boundary.
        Int128 previous = ExtendedTimestamp.DivideFloor((Int128)(-5000), 1000);
        for (long n = -4999; n <= 5000; n++)
        {
            Int128 current = ExtendedTimestamp.DivideFloor((Int128)n, 1000);
            Assert.True(current >= previous, $"floor({n}/1000) went backwards");
            previous = current;
        }
    }

    [Fact]
    public void KnowsWhatTheCarrierCanHold()
    {
        Assert.True(ExtendedTimestamp.IsRepresentable(ExtendedTimestamp.MaxValue));
        Assert.True(ExtendedTimestamp.IsRepresentable(ExtendedTimestamp.MinValue));
        Assert.False(ExtendedTimestamp.IsRepresentable(ExtendedTimestamp.MaxValue + Int128.One));
        Assert.False(ExtendedTimestamp.IsRepresentable(ExtendedTimestamp.MinValue - Int128.One));
    }

    [Fact]
    public void TheCarrierBoundsRoundTrip()
    {
        Span<byte> buffer = stackalloc byte[ExtendedTimestamp.ByteWidth];
        foreach (var value in new[] { ExtendedTimestamp.MaxValue, ExtendedTimestamp.MinValue })
        {
            ExtendedTimestamp.Write(value, buffer);
            Assert.Equal(value.ToString(), ExtendedTimestamp.Read(buffer).ToString());
        }
    }

    [Fact]
    public void TheCarrierSpansTheWholeSqlRangeInNanoseconds()
    {
        // Year 0001 to year 9999 in nanoseconds is what INT64 cannot do and this can. Anything less
        // than this and the type has no reason to exist.
        Assert.False(ExtendedTimestamp.TryToInt64(ParseInt128("253402300799000000000"), out _));
        Assert.True(ExtendedTimestamp.IsRepresentable(ParseInt128("253402300799000000000")));
        Assert.False(ExtendedTimestamp.TryToInt64(ParseInt128("-62135596800000000000"), out _));
        Assert.True(ExtendedTimestamp.IsRepresentable(ParseInt128("-62135596800000000000")));
    }

    [Fact]
    public void NanosecondConversionDoesNotWrapAtTheEndOfTheSqlRange()
    {
        // Year 9999 is 2.53e20 nanoseconds and a long holds 9.22e18. Computing this in 64 bits wrapped
        // it to -4852116231933722724 -- and as a bloom-filter probe, a wrapped value means asking
        // whether some other timestamp entirely is present, which can prune a row group that matches.
        long ticks = 2_534_023_007_999_999_999L; // 9999-12-31T23:59:59.9999999Z, in ticks since epoch

        Assert.True(ExtendedTimestamp.TryTicksToUnit(ticks, ParquetTimeUnit.Nanos, out Int128 nanos));
        Assert.Equal("253402300799999999900", nanos.ToString());
        Assert.False(ExtendedTimestamp.TryToInt64(nanos, out _));
        Assert.True(ExtendedTimestamp.IsRepresentable(nanos));
    }

    [Fact]
    public void CoarserUnitsConvertOnlyWhenExact()
    {
        // 1.5 ms is not a MILLIS value, so there is no count to convert it to.
        Assert.False(ExtendedTimestamp.TryTicksToUnit(15_000, ParquetTimeUnit.Millis, out _));
        Assert.True(ExtendedTimestamp.TryTicksToUnit(20_000, ParquetTimeUnit.Millis, out Int128 millis));
        Assert.Equal("2", millis.ToString());

        Assert.False(ExtendedTimestamp.TryTicksToUnit(5, ParquetTimeUnit.Micros, out _));
        Assert.True(ExtendedTimestamp.TryTicksToUnit(20, ParquetTimeUnit.Micros, out Int128 micros));
        Assert.Equal("2", micros.ToString());
    }

    [Fact]
    public void EncodingASlicedColumnKeepsItsValuesAndItsNulls()
    {
        // The buffered writer hands sliced arrays straight through -- it tracks Data.Offset rather than
        // compacting -- so the encoder has to honour the offset on BOTH buffers. Reusing the caller's
        // validity bitmap alongside offset 0 silently moved every null.
        var type = new TimestampType(TimeUnit.Microsecond, "UTC");
        var values = new ArrowBuffer.Builder<long>();
        foreach (long v in new[] { 10L, 20L, 30L, 40L, 50L })
        {
            values.Append(v);
        }

        // Valid at absolute rows 1, 2, 4; null at 0 and 3. The slice starts at row 2, so within it the
        // pattern is present, null, present.
        var validity = new ArrowBuffer(new byte[] { 0b00010110 });
        var sliced = new TimestampArray(new ArrayData(
            type, length: 3, nullCount: 1, offset: 2, [validity, values.Build()]));

        var encoded = ExtendedTimestamp.EncodeColumn(sliced);

        Assert.Equal(3, encoded.Length);
        Assert.False(encoded.IsNull(0));
        Assert.True(encoded.IsNull(1));
        Assert.False(encoded.IsNull(2));

        var bytes = Assert.IsType<FixedSizeBinaryArray>(encoded);
        Assert.Equal("30", ExtendedTimestamp.Read(bytes.GetBytes(0)).ToString());
        Assert.Equal("50", ExtendedTimestamp.Read(bytes.GetBytes(2)).ToString());
    }

    [Fact]
    public void EncodingAnUnslicedColumnIsUnchanged()
    {
        var type = new TimestampType(TimeUnit.Microsecond, "UTC");
        var values = new ArrowBuffer.Builder<long>();
        values.Append(7L);
        values.Append(9L);
        var array = new TimestampArray(new ArrayData(
            type, length: 2, nullCount: 0, offset: 0,
            [new ArrowBuffer(new byte[] { 0b11 }), values.Build()]));

        var bytes = Assert.IsType<FixedSizeBinaryArray>(ExtendedTimestamp.EncodeColumn(array));

        Assert.Equal("7", ExtendedTimestamp.Read(bytes.GetBytes(0)).ToString());
        Assert.Equal("9", ExtendedTimestamp.Read(bytes.GetBytes(1)).ToString());
        Assert.False(bytes.IsNull(0));
        Assert.False(bytes.IsNull(1));
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary>
/// <c>ArrowCompute.Repeat</c> builds a column of one value repeated — the shape a partition column takes when
/// materialised onto a batch read from a data file. It takes the value's raw Arrow encoding rather than a .NET
/// value, which is what keeps a decimal wider than <see cref="decimal"/>'s 28 digits, or a timestamp at a unit
/// the surface type cannot express, from being rounded on the way through.
///
/// <para>The buffer shape is the exact inverse of <c>MakeNullArray</c>'s: no validity buffer at all and a null
/// count of zero, which is what lets Arrow skip the per-element validity check downstream.</para>
/// </summary>
public class RepeatTests
{
    public static TheoryData<string> FixedWidthTypes
    {
        get
        {
            var cases = new TheoryData<string>();
            foreach (string name in FixedWidthCases.All)
            {
#if !NET6_0_OR_GREATER
                if (name == "halffloat") continue;
#endif
                cases.Add(name);
            }
            return cases;
        }
    }

    /// <summary>Distinct, non-zero bytes, so a slot left unwritten cannot coincidentally match.</summary>
    private static byte[] Pattern(int width)
    {
        var bytes = new byte[width];
        for (int i = 0; i < width; i++)
            bytes[i] = (byte)(i * 7 + 1);
        return bytes;
    }

    private static void AssertNoValidityBuffer(IArrowArray array)
    {
        Assert.Equal(0, array.Data.NullCount);
        Assert.Equal(0, array.Data.Buffers[0].Length);
        Assert.Equal(0, array.Data.Offset);
    }

    [Theory]
    [MemberData(nameof(FixedWidthTypes))]
    public void FixedWidth_EverySlotHoldsTheValueBytes(string name)
    {
        var (type, width) = FixedWidthCases.Resolve(name);
        byte[] pattern = Pattern(width);

        // Deliberately not a multiple of the pattern width, so the doubling fill has to finish with a
        // partial block rather than landing exactly on the end.
        const int rows = 13;
        var result = ArrowCompute.Repeat(type, pattern, rows);

        Assert.Equal(rows, result.Length);
        AssertNoValidityBuffer(result);

        // Reference equality: the type is reused as given, so unit/timezone/precision/scale cannot be
        // re-tagged the way a builder for a narrower type would re-tag them.
        Assert.Same(type, result.Data.DataType);

        Assert.Equal(rows * width, result.Data.Buffers[1].Length);
        for (int i = 0; i < rows; i++)
            Assert.Equal(pattern, RawArrays.Slot(result, i, width));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(8)]
    [InlineData(1000)]
    public void FixedWidth_HoldsAtEveryLength(int rows)
    {
        // 8 bytes wide, so the fill does real doubling work at the larger lengths.
        var type = new TimestampType(TimeUnit.Microsecond, "UTC");
        byte[] pattern = Pattern(8);

        var result = ArrowCompute.Repeat(type, pattern, rows);

        Assert.Equal(rows, result.Length);
        Assert.Equal(rows * 8, result.Data.Buffers[1].Length);
        for (int i = 0; i < rows; i++)
            Assert.Equal(pattern, RawArrays.Slot(result, i, 8));
    }

    [Fact]
    public void SingleByteWidth_FillsCorrectly()
    {
        // Width 1 takes the Span.Fill shortcut rather than the doubling path.
        var result = ArrowCompute.Repeat(Int8Type.Default, new byte[] { 0xF9 }, 5);

        Assert.Equal(5, result.Length);
        var arr = Assert.IsType<Int8Array>(result);
        for (int i = 0; i < 5; i++)
            Assert.Equal(unchecked((sbyte)0xF9), arr.GetValue(i));
    }

    [Fact]
    public void Timestamp_KeepsUnitAndTimezoneAndExactValue()
    {
        // The value a builder would have to reach through DateTimeOffset to reproduce.
        var type = new TimestampType(TimeUnit.Microsecond, "UTC");
        const long micros = 1_700_000_000_000_123L;
        var pattern = BitConverter.GetBytes(micros);

        var result = Assert.IsType<TimestampArray>(ArrowCompute.Repeat(type, pattern, 4));

        var actual = Assert.IsType<TimestampType>(result.Data.DataType);
        Assert.Equal(TimeUnit.Microsecond, actual.Unit);
        Assert.Equal("UTC", actual.Timezone);

        for (int i = 0; i < 4; i++)
            Assert.Equal(micros, result.GetValue(i));
    }

    [Fact]
    public void Decimal128_CarriesAValueTooWideForSystemDecimal()
    {
        // 38 nines — outside System.Decimal's ~28-29 digit range entirely, so a constant built through a
        // Decimal128Array.Builder could not express it at all.
        var type = new Decimal128Type(precision: 38, scale: 0);
        var big = System.Numerics.BigInteger.Parse("99999999999999999999999999999999999999");

        var pattern = new byte[16];
        var le = big.ToByteArray();
        le.AsSpan(0, Math.Min(le.Length, 16)).CopyTo(pattern);

        var result = Assert.IsType<Decimal128Array>(ArrowCompute.Repeat(type, pattern, 3));

        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(pattern, result.GetBytes(i).ToArray());
            Assert.Equal(big, new System.Numerics.BigInteger(result.GetBytes(i).ToArray()));
        }
    }

    [Fact]
    public void Boolean_True_SetsEveryBit()
    {
        // Longer than one bitmap byte so a single-byte fill cannot pass.
        var result = Assert.IsType<BooleanArray>(
            ArrowCompute.Repeat(BooleanType.Default, new byte[] { 1 }, 10));

        Assert.Equal(10, result.Length);
        AssertNoValidityBuffer(result);
        for (int i = 0; i < 10; i++)
            Assert.True(result.GetValue(i));
    }

    [Fact]
    public void Boolean_False_LeavesEveryBitClearButStillNotNull()
    {
        var result = Assert.IsType<BooleanArray>(
            ArrowCompute.Repeat(BooleanType.Default, new byte[] { 0 }, 10));

        AssertNoValidityBuffer(result);
        for (int i = 0; i < 10; i++)
        {
            // false, NOT null — the distinction an absent validity buffer is what preserves.
            Assert.False(result.IsNull(i));
            Assert.False(result.GetValue(i));
        }
    }

    [Fact]
    public void Boolean_RejectsAWrongSizedValue()
    {
        Assert.Throws<ArgumentException>(
            () => ArrowCompute.Repeat(BooleanType.Default, new byte[] { 1, 0 }, 3));
    }

    [Fact]
    public void String_RepeatsValueWithComputedOffsets()
    {
        byte[] value = System.Text.Encoding.UTF8.GetBytes("café");
        int len = value.Length; // 5 bytes — multi-byte UTF-8, so byte length != char length

        var result = Assert.IsType<StringArray>(
            ArrowCompute.Repeat(StringType.Default, value, 4));

        Assert.Equal(4, result.Length);
        AssertNoValidityBuffer(result);

        for (int i = 0; i < 4; i++)
            Assert.Equal("café", result.GetString(i));

        // Offsets are i * len, computed rather than accumulated.
        var offsets = System.Runtime.InteropServices.MemoryMarshal
            .Cast<byte, int>(result.Data.Buffers[1].Span);
        Assert.Equal(5, offsets.Length);
        for (int i = 0; i <= 4; i++)
            Assert.Equal(i * len, offsets[i]);
    }

    [Fact]
    public void String_EmptyValueIsAnEmptyStringNotANull()
    {
        var result = Assert.IsType<StringArray>(
            ArrowCompute.Repeat(StringType.Default, System.Array.Empty<byte>(), 3));

        AssertNoValidityBuffer(result);
        for (int i = 0; i < 3; i++)
        {
            Assert.False(result.IsNull(i));
            Assert.Equal("", result.GetString(i));
        }
    }

    [Fact]
    public void Binary_RepeatsRawBytes()
    {
        byte[] value = [0x00, 0xFF, 0x7F];

        var result = Assert.IsType<BinaryArray>(
            ArrowCompute.Repeat(BinaryType.Default, value, 3));

        for (int i = 0; i < 3; i++)
            Assert.Equal(value, result.GetBytes(i).ToArray());
    }

    [Fact]
    public void LargeString_IsNotNarrowedToString()
    {
        byte[] value = System.Text.Encoding.UTF8.GetBytes("abc");

        var result = ArrowCompute.Repeat(LargeStringType.Default, value, 3);

        Assert.IsType<LargeStringArray>(result);
        Assert.Same(LargeStringType.Default, result.Data.DataType);

        // 64-bit offsets, so twice the buffer width of the 32-bit case.
        Assert.Equal(4 * sizeof(long), result.Data.Buffers[1].Length);
        var offsets = System.Runtime.InteropServices.MemoryMarshal
            .Cast<byte, long>(result.Data.Buffers[1].Span);
        for (int i = 0; i <= 3; i++)
            Assert.Equal(i * 3L, offsets[i]);

        var large = (LargeStringArray)result;
        for (int i = 0; i < 3; i++)
            Assert.Equal("abc", large.GetString(i));
    }

    [Fact]
    public void Extension_KeepsItsAnnotation()
    {
        var moneyType = new MoneyType();
        var pattern = BitConverter.GetBytes(4_200L);

        var result = ArrowCompute.Repeat(moneyType, pattern, 3);

        var ext = Assert.IsAssignableFrom<ExtensionArray>(result);
        Assert.Same(moneyType, ext.Data.DataType);

        var storage = Assert.IsType<Int64Array>(ext.Storage);
        for (int i = 0; i < 3; i++)
            Assert.Equal(4_200L, storage.GetValue(i));
    }

    [Fact]
    public void FixedWidth_RejectsAValueOfTheWrongWidth()
    {
        // A short slot would otherwise be tiled at the wrong stride, silently misaligning every row.
        var ex = Assert.Throws<ArgumentException>(
            () => ArrowCompute.Repeat(Int64Type.Default, new byte[] { 1, 2, 3 }, 4));

        Assert.Contains("8 bytes", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void UnsupportedType_Throws()
    {
        // Consistent with Take and MakeNullArray: no fallback to some other type, which would produce a
        // column contradicting the schema it lands in.
        var ex = Assert.Throws<NotSupportedException>(
            () => ArrowCompute.Repeat(
                new ListType(new Field("item", Int32Type.Default, true)), new byte[] { 1 }, 3));

        Assert.Contains("ArrowCompute", ex.Message, StringComparison.Ordinal);
        Assert.Contains("List", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}

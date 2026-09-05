// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#pragma warning disable EWPARQUET0005 // PFOR tests intentionally reference the experimental enum values.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// Tests for PFOR: the page and vector layout, the cost model, the delta mode, and end-to-end
/// files. Layout assertions cite apache/parquet-format#617 (<c>PforEncoding.md</c>).
/// </summary>
public class PforEncodingTests : IDisposable
{
    private readonly string _tempDir;

    public PforEncodingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-pfor-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    // ───── Round trips ─────

    public static TheoryData<string, int[]> Int32Shapes() => new()
    {
        { "constant", Enumerable.Repeat(42, 1000).ToArray() },
        { "tight cluster", Enumerable.Range(0, 1000).Select(i => 1000 + i % 200).ToArray() },
        { "sorted", Enumerable.Range(0, 1000).ToArray() },
        { "descending", Enumerable.Range(0, 1000).Select(i => 5000 - i).ToArray() },
        { "fixed stride", Enumerable.Range(0, 1000).Select(i => i * 7).ToArray() },
        { "cluster with outliers", Enumerable.Range(0, 1000).Select(i => i % 97 == 0 ? 50_000_000 : 100 + i % 8).ToArray() },
        { "negative", Enumerable.Range(0, 1000).Select(i => -1000 - i).ToArray() },
        { "straddling zero", Enumerable.Range(0, 1000).Select(i => i - 500).ToArray() },
        { "type extremes", [int.MinValue, int.MaxValue, 0, -1, 1, int.MinValue, int.MaxValue, 0] },
        { "single value", [7] },
        { "exactly one vector", Enumerable.Range(0, 1024).ToArray() },
        { "one past a vector", Enumerable.Range(0, 1025).ToArray() },
        { "many vectors", Enumerable.Range(0, 5000).Select(i => i * 3 % 60_000).ToArray() },
    };

    [Theory]
    [MemberData(nameof(Int32Shapes))]
    public void Int32_RoundTrips(string name, int[] values)
    {
        _ = name;
        byte[] page = PforEncoder.EncodeInt32s(values);

        var decoded = new int[values.Length];
        PforDecoder.DecodeInt32s(page, decoded, values.Length);

        Assert.Equal(values, decoded);
    }

    public static TheoryData<string, long[]> Int64Shapes() => new()
    {
        { "constant", Enumerable.Repeat(42L, 1000).ToArray() },
        { "timestamps", Enumerable.Range(0, 1000).Select(i => 1_700_000_000_000L + i * 7).ToArray() },
        { "timestamps with a gap", Enumerable.Range(0, 1000).Select(i => 1_700_000_000_000L + i * 7 + (i >= 500 ? 1_000_000_000L : 0)).ToArray() },
        { "cluster with outliers", Enumerable.Range(0, 1000).Select(i => i % 97 == 0 ? long.MaxValue / 2 : 100L + i % 8).ToArray() },
        { "descending", Enumerable.Range(0, 1000).Select(i => 5_000_000_000L - i).ToArray() },
        { "type extremes", [long.MinValue, long.MaxValue, 0, -1, 1, long.MinValue, long.MaxValue, 0] },
        { "spanning the range", [long.MinValue, 0, long.MaxValue, long.MinValue / 2, long.MaxValue / 2, -1, 1, 0] },
        { "single value", [7L] },
        { "many vectors", Enumerable.Range(0, 5000).Select(i => (long)i * 1_000_003).ToArray() },
    };

    [Theory]
    [MemberData(nameof(Int64Shapes))]
    public void Int64_RoundTrips(string name, long[] values)
    {
        _ = name;
        byte[] page = PforEncoder.EncodeInt64s(values);

        var decoded = new long[values.Length];
        PforDecoder.DecodeInt64s(page, decoded, values.Length);

        Assert.Equal(values, decoded);
    }

    /// <summary>
    /// Every width from 0 to the type's maximum, driven by data that forces exactly that width.
    /// </summary>
    /// <remarks>
    /// Swept rather than sampled on purpose. The bugs this catches — a mask built by shifting at
    /// exactly the type width, a word read that straddles two 64-bit words, a group-of-eight
    /// kernel that declines a width the tail loop then gets wrong — all live at particular
    /// widths and are invisible at every other one. This library has already shipped that exact
    /// class of bug once, in the RLE decoder at widths 27, 29, 30, 31 and 32.
    /// </remarks>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(31)]
    [InlineData(32)]
    public void Int32_AllWidths(int bitWidth)
    {
        int[] values = WidthDrivingInt32(bitWidth, 1024);

        byte[] page = PforEncoder.EncodeInt32s(values);
        var decoded = new int[values.Length];
        PforDecoder.DecodeInt32s(page, decoded, values.Length);

        Assert.Equal(values, decoded);
    }

    [Fact]
    public void Int32_EveryWidthZeroToThirtyTwo()
    {
        for (int bitWidth = 0; bitWidth <= 32; bitWidth++)
        {
            int[] values = WidthDrivingInt32(bitWidth, 1024);

            byte[] page = PforEncoder.EncodeInt32s(values);
            var decoded = new int[values.Length];
            PforDecoder.DecodeInt32s(page, decoded, values.Length);

            Assert.Equal(values, decoded);
        }
    }

    [Fact]
    public void Int64_EveryWidthZeroToSixtyFour()
    {
        for (int bitWidth = 0; bitWidth <= 64; bitWidth++)
        {
            long[] values = WidthDrivingInt64(bitWidth, 1024);

            byte[] page = PforEncoder.EncodeInt64s(values);
            var decoded = new long[values.Length];
            PforDecoder.DecodeInt64s(page, decoded, values.Length);

            Assert.Equal(values, decoded);
        }
    }

    /// <summary>
    /// The same sweep on a vector length that is not a multiple of eight, so that the tail past
    /// the last whole group of eight is exercised at every width.
    /// </summary>
    [Fact]
    public void Int64_EveryWidthWithARaggedTail()
    {
        for (int bitWidth = 0; bitWidth <= 64; bitWidth++)
        {
            long[] values = WidthDrivingInt64(bitWidth, 1021);

            byte[] page = PforEncoder.EncodeInt64s(values);
            var decoded = new long[values.Length];
            PforDecoder.DecodeInt64s(page, decoded, values.Length);

            Assert.Equal(values, decoded);
        }
    }

    /// <summary>
    /// Values whose residuals against a frame of 0 need exactly <paramref name="bitWidth"/> bits,
    /// with the cost model given no reason to pick anything narrower.
    /// </summary>
    private static int[] WidthDrivingInt32(int bitWidth, int count)
    {
        if (bitWidth == 0)
            return Enumerable.Repeat(0, count).ToArray();

        uint max = bitWidth == 32 ? uint.MaxValue : (1u << bitWidth) - 1u;
        var values = new int[count];
        for (int i = 0; i < count; i++)
        {
            // Alternating between the extremes of the width keeps every residual wide, so no
            // narrower width can win: half the values would become exceptions.
            values[i] = unchecked((int)(i % 2 == 0 ? max : max >> 1));
        }

        values[0] = 0; // pins the frame at 0 so the width is the residual width
        return values;
    }

    /// <inheritdoc cref="WidthDrivingInt32"/>
    private static long[] WidthDrivingInt64(int bitWidth, int count)
    {
        if (bitWidth == 0)
            return Enumerable.Repeat(0L, count).ToArray();

        ulong max = bitWidth == 64 ? ulong.MaxValue : (1UL << bitWidth) - 1UL;
        var values = new long[count];
        for (int i = 0; i < count; i++)
            values[i] = unchecked((long)(i % 2 == 0 ? max : max >> 1));

        values[0] = 0;
        return values;
    }

    // ───── The cost model ─────

    /// <summary>
    /// The point of PFOR over plain FOR: one outlier does not widen the packing for everyone.
    /// </summary>
    [Fact]
    public void CostModel_KeepsANarrowWidthAndPatchesTheOutlier()
    {
        // PforEncoding.md, Example 1.
        int[] values = [100, 102, 101, 103, 100, 99, 50_000, 104];

        byte[] page = PforEncoder.EncodeInt32s(values, logVectorSize: 3);
        var vector = VectorOf(page, 0, numVectors: 1);

        Assert.Equal(99, BinaryPrimitives.ReadInt32LittleEndian(vector));
        Assert.Equal(3, vector[4] & 0x7F);
        Assert.Equal(0, vector[4] & 0x80); // not differenced
        Assert.Equal(1, BinaryPrimitives.ReadUInt16LittleEndian(vector.Slice(5, 2)));

        // 7 header + ceil(8*3/8) packed + 1*(2 + 4) exception = 16 bytes, against the 23 that
        // plain FOR would need at width 16 and the 32 that PLAIN would.
        Assert.Equal(16, vector.Length);

        var decoded = new int[values.Length];
        PforDecoder.DecodeInt32s(page, decoded, values.Length);
        Assert.Equal(values, decoded);
    }

    /// <summary>
    /// Uniform data has no outliers to exploit and no dense window to move the frame onto, so PFOR
    /// reduces to plain FOR: the width of the widest residual, with no exceptions at all.
    /// </summary>
    [Fact]
    public void CostModel_ReducesToPlainForOnUniformData()
    {
        // PforEncoding.md, Example 2: 1024 values spread evenly over [1000, 1255]. Drawn at
        // random rather than as a cycling ramp, which would have a constant difference the delta
        // mode could exploit — a real property of that data, but not the one under test here.
        var random = new Random(20260904);
        var values = Enumerable.Range(0, 1024).Select(_ => 1000 + random.Next(256)).ToArray();

        byte[] page = PforEncoder.EncodeInt32s(values);
        var vector = VectorOf(page, 0, numVectors: 1);

        Assert.Equal(1000, BinaryPrimitives.ReadInt32LittleEndian(vector));
        Assert.Equal(8, vector[4] & 0x7F);
        Assert.Equal(0, vector[4] & 0x80);
        Assert.Equal(0, BinaryPrimitives.ReadUInt16LittleEndian(vector.Slice(5, 2)));
        Assert.Equal(7 + 1024, vector.Length);
    }

    /// <summary>
    /// The shape PFOR exists for, and the one the naive frame gets wrong: a tight cluster with a
    /// low sentinel. Taking the minimum as the frame hands it to the sentinel and sets the width
    /// from the gap; the frame search puts it on the cluster and patches the sentinel instead.
    /// </summary>
    /// <remarks>
    /// PforEncoding.md's Example 3 is this column, and quotes a width of 11 "for range
    /// 2450815-2453005" — which its own rule, <c>frame_of_reference = min(values[])</c>, cannot
    /// produce, since that minimum is the sentinel at 2,415,022 and the residuals it leaves need
    /// 16 bits. The example is only reachable with a frame above the minimum.
    /// </remarks>
    [Fact]
    public void CostModel_MovesTheFrameOffALowSentinel()
    {
        const int Sentinel = 2_415_022;
        var values = Enumerable.Range(0, 1024)
            .Select(i => i % 101 == 0 ? Sentinel : 2_450_815 + (i * 977) % 2191)
            .ToArray();

        byte[] page = PforEncoder.EncodeInt32s(values);
        var vector = VectorOf(page, 0, numVectors: 1);

        int frame = BinaryPrimitives.ReadInt32LittleEndian(vector);
        int bitWidth = vector[4] & 0x7F;
        int numExceptions = BinaryPrimitives.ReadUInt16LittleEndian(vector.Slice(5, 2));

        Assert.True(frame > Sentinel, $"frame {frame} should sit above the sentinel {Sentinel}");
        // The cluster spans 2,191 values, which needs 12 bits. (PforEncoding.md quotes 11 for
        // the same span; ceil(log2(2191)) is 11.1, so 12 is the answer its own formula gives.)
        Assert.True(bitWidth <= 12, $"bit width {bitWidth} should be the cluster's, not the gap's 16");
        Assert.Equal(1024 / 101 + 1, numExceptions);

        // 7 + ceil(1024*16/8) = 2055 bytes with the minimum as the frame.
        Assert.True(vector.Length < 2055, $"{vector.Length} bytes should beat the naive frame's 2055");

        var decoded = new int[values.Length];
        PforDecoder.DecodeInt32s(page, decoded, values.Length);
        Assert.Equal(values, decoded);
    }

    /// <summary>
    /// A fixed-stride column: differencing turns it into a constant, which the frame then takes
    /// to zero, so nothing at all is left to pack.
    /// </summary>
    [Fact]
    public void DeltaMode_IsChosenForAFixedStride()
    {
        // PforEncoding.md, Example 4.
        var values = Enumerable.Range(0, 1024).Select(i => 1_700_000_000_000L + i * 7).ToArray();

        byte[] page = PforEncoder.EncodeInt64s(values);
        var vector = VectorOf(page, 0, numVectors: 1);

        Assert.Equal(0x80, vector[8] & 0x80);
        Assert.Equal(1_700_000_000_000L, BinaryPrimitives.ReadInt64LittleEndian(vector.Slice(11, 8)));

        // The spec's example takes min(d) = 0 as the frame, leaving residuals of 0 and 7 and a
        // width of 3 — 403 bytes. The frame search does better: put the frame on 7, where every
        // difference but d[0] sits, and the width drops to 0 with d[0] as the one exception.
        Assert.Equal(7, BinaryPrimitives.ReadInt64LittleEndian(vector));
        Assert.Equal(0, vector[8] & 0x7F);
        Assert.Equal(1, BinaryPrimitives.ReadUInt16LittleEndian(vector.Slice(9, 2)));
        Assert.Equal(11 + 8 + 0 + (2 + 8), vector.Length);

        var decoded = new long[values.Length];
        PforDecoder.DecodeInt64s(page, decoded, values.Length);
        Assert.Equal(values, decoded);
    }

    /// <summary>
    /// Differencing is a per-vector choice, not a per-column one: a page whose first half is
    /// sorted and whose second half is a tight unsorted cluster should difference only the half
    /// that pays.
    /// </summary>
    [Fact]
    public void DeltaMode_IsDecidedPerVector()
    {
        var values = new int[2048];
        for (int i = 0; i < 1024; i++)
            values[i] = i * 1000;                      // sorted, wide range: differencing wins
        for (int i = 1024; i < 2048; i++)
            values[i] = 500 + (i * 37) % 16;           // tight cluster, unsorted: it does not

        byte[] page = PforEncoder.EncodeInt32s(values);

        Assert.Equal(0x80, VectorOf(page, 0, 2)[4] & 0x80);
        Assert.Equal(0, VectorOf(page, 1, 2)[4] & 0x80);

        var decoded = new int[values.Length];
        PforDecoder.DecodeInt32s(page, decoded, values.Length);
        Assert.Equal(values, decoded);
    }

    /// <summary>
    /// Random integers have nothing for PFOR to exploit, and the writer must not spend the delta
    /// mode's start value on them either.
    /// </summary>
    [Fact]
    public void DeltaMode_IsDeclinedOnRandomData()
    {
        var random = new Random(20260904);
        var values = new int[1024];
        for (int i = 0; i < values.Length; i++)
            values[i] = random.Next(int.MinValue, int.MaxValue);

        byte[] page = PforEncoder.EncodeInt32s(values);

        Assert.Equal(0, VectorOf(page, 0, 1)[4] & 0x80);

        var decoded = new int[values.Length];
        PforDecoder.DecodeInt32s(page, decoded, values.Length);
        Assert.Equal(values, decoded);
    }

    // ───── Cross-implementation golden vectors ─────
    //
    // Byte-for-byte from the tests in apache/arrow-rs#10977. A round trip through our own encoder
    // cannot tell a wire-format disagreement from a self-consistent one, so these are the only
    // assertions here that would catch us writing a page nobody else can read.

    [Fact]
    public void Golden_DeltaVector()
    {
        byte[] vector =
        [
            0x00, 0x00, 0x00, 0x00, 0x82, 0x00, 0x00, // frame 0, width 2 with the delta flag
            0xE8, 0x03, 0x00, 0x00,                   // start value 1000
            0xA8, 0xAA,                               // 0 then seven twos, at 2 bits each
        ];

        var decoded = new int[8];
        PforDecoder.DecodeInt32s(Page(logVectorSize: 3, valueByteWidth: 4, numElements: 8, vector), decoded, 8);

        Assert.Equal([1000, 1002, 1004, 1006, 1008, 1010, 1012, 1014], decoded);
    }

    [Fact]
    public void Golden_DeltaVectorPatchesBeforeSumming()
    {
        byte[] vector =
        [
            0x00, 0x00, 0x00, 0x00, 0x81, 0x01, 0x00, // frame 0, width 1 with the delta flag
            0x0A, 0x00, 0x00, 0x00,                   // start value 10
            0x0A,                                     // differences 0, 1, 0 (a placeholder), 1
            0x02, 0x00,                               // the placeholder is at position 2
            0xF4, 0x01, 0x00, 0x00,                   // and the difference there is really 500
        ];

        var decoded = new int[4];
        PforDecoder.DecodeInt32s(Page(logVectorSize: 3, valueByteWidth: 4, numElements: 4, vector), decoded, 4);

        // Summing before patching would give 10, 11, 11, 12: the placeholder zero would be
        // carried into every value after it, and index 2 would hold a difference.
        Assert.Equal([10, 11, 511, 512], decoded);
    }

    /// <summary>
    /// Width 64 needs all seven bits of the width field. Masking the flag off with six would read
    /// this as width 0 — a constant vector, filled with the frame, with no error to say so.
    /// </summary>
    [Fact]
    public void Golden_Int64AtFullWidth()
    {
        long[] values = [long.MinValue, long.MaxValue, -1, 0, 1, 2, 3, 4];
        long frame = long.MinValue;

        var vector = new List<byte>();
        vector.AddRange(BitConverter.GetBytes(frame));
        vector.Add(64);
        vector.AddRange(BitConverter.GetBytes((ushort)0));
        foreach (long value in values)
            vector.AddRange(BitConverter.GetBytes(unchecked((ulong)(value - frame))));

        var decoded = new long[8];
        PforDecoder.DecodeInt64s(
            Page(logVectorSize: 3, valueByteWidth: 8, numElements: 8, [.. vector]), decoded, 8);

        Assert.Equal(values, decoded);
    }

    /// <summary>
    /// A delta vector at width 0 whose frame is not 0 cannot be produced by a conforming writer:
    /// <c>d[0]</c> is 0 and the frame is the minimum of the differences, so an all-zero residual
    /// array forces a frame of 0. We decode it by the general path anyway — unpack, add the
    /// frame, prefix-sum — which is what PforEncoding.md's numbered decode steps say.
    /// </summary>
    /// <remarks>
    /// Worth pinning because the spec's prose claims a reader may instead fill with the start
    /// value and "get the same answer for any frame", which is not true: the two agree only when
    /// the frame is 0. apache/arrow-rs#10977 takes the fill path and its own test asserts
    /// 5, 8, 11, 14 for this page where the general path gives 8, 11, 14, 17. Neither writer can
    /// emit the page, so the divergence is unreachable from real data — but it is a real
    /// ambiguity in the spec, not a difference of opinion about it.
    /// </remarks>
    [Fact]
    public void DeltaVectorAtWidthZeroWithANonZeroFrameTakesTheGeneralPath()
    {
        byte[] vector =
        [
            0x03, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, // frame 3, width 0 with the delta flag
            0x05, 0x00, 0x00, 0x00,                   // start value 5
        ];

        var decoded = new int[4];
        PforDecoder.DecodeInt32s(Page(logVectorSize: 3, valueByteWidth: 4, numElements: 4, vector), decoded, 4);

        Assert.Equal([8, 11, 14, 17], decoded);
    }

    /// <summary>The conforming shape of the same page: a frame of 0, where both readings agree.</summary>
    [Fact]
    public void DeltaVectorAtWidthZeroIsAConstantColumn()
    {
        byte[] vector =
        [
            0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, // frame 0, width 0 with the delta flag
            0x05, 0x00, 0x00, 0x00,                   // start value 5
        ];

        var decoded = new int[4];
        PforDecoder.DecodeInt32s(Page(logVectorSize: 3, valueByteWidth: 4, numElements: 4, vector), decoded, 4);

        Assert.Equal([5, 5, 5, 5], decoded);
    }

    // ───── Malformed pages ─────

    [Fact]
    public void Rejects_UnknownPackingMode()
    {
        byte[] page = PforEncoder.EncodeInt32s([1, 2, 3, 4, 5, 6, 7, 8]);
        page[0] = 1;

        var ex = Assert.Throws<ParquetFormatException>(() => Decode32(page, 8));
        Assert.Contains("packing_mode", ex.Message);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(16)]
    public void Rejects_LogVectorSizeOutOfRange(byte logVectorSize)
    {
        byte[] page = PforEncoder.EncodeInt32s([1, 2, 3, 4, 5, 6, 7, 8]);
        page[1] = logVectorSize;

        var ex = Assert.Throws<ParquetFormatException>(() => Decode32(page, 8));
        Assert.Contains("log_vector_size", ex.Message);
    }

    /// <summary>
    /// The width byte is what makes a page self-describing, so a page written for the other type
    /// is rejected here rather than misread as a truncated one.
    /// </summary>
    [Fact]
    public void Rejects_ValueByteWidthThatDisagreesWithTheColumn()
    {
        byte[] page = PforEncoder.EncodeInt64s([1L, 2, 3, 4, 5, 6, 7, 8]);

        var ex = Assert.Throws<ParquetFormatException>(() => Decode32(page, 8));
        Assert.Contains("value_byte_width", ex.Message);
    }

    [Fact]
    public void Rejects_NumElementsThatDisagreesWithThePage()
    {
        byte[] page = PforEncoder.EncodeInt32s([1, 2, 3, 4, 5, 6, 7, 8]);

        var ex = Assert.Throws<ParquetFormatException>(() => Decode32(page, 9));
        Assert.Contains("num_elements", ex.Message);
    }

    /// <summary>
    /// A delta vector truncated after its header but before its start value. The header bound is
    /// satisfied while the delta flag is still unknown, so a reader that checks only the header
    /// and then the residual section reads the start value from past the end of the page.
    /// </summary>
    [Fact]
    public void Rejects_DeltaVectorTruncatedBeforeItsStartValue()
    {
        var values = Enumerable.Range(0, 1024).Select(i => 1_700_000_000_000L + i * 7).ToArray();
        byte[] page = PforEncoder.EncodeInt64s(values);

        // 7 page header + 4 offset + 11 vector info, and nothing after it.
        byte[] truncated = page.AsSpan(0, 7 + 4 + 11).ToArray();

        var ex = Assert.Throws<ParquetFormatException>(() => Decode64(truncated, values.Length));
        Assert.Contains("start value", ex.Message);
    }

    [Fact]
    public void Rejects_TruncatedVector()
    {
        var values = Enumerable.Range(0, 1024).Select(i => 1000 + i % 200).ToArray();
        byte[] page = PforEncoder.EncodeInt32s(values);

        byte[] truncated = page.AsSpan(0, page.Length - 32).ToArray();

        Assert.Throws<ParquetFormatException>(() => Decode32(truncated, values.Length));
    }

    [Fact]
    public void Rejects_ExceptionPositionPastTheEndOfItsVector()
    {
        int[] values = [100, 102, 101, 103, 100, 99, 50_000, 104];
        byte[] page = PforEncoder.EncodeInt32s(values, logVectorSize: 3);

        // 7 page header + 4 offset + 7 vector info + 3 packed = the exception position.
        BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(7 + 4 + 7 + 3, 2), 99);

        var ex = Assert.Throws<ParquetFormatException>(() => Decode32(page, values.Length));
        Assert.Contains("exception position", ex.Message);
    }

    [Fact]
    public void Rejects_VectorOffsetOutsideThePage()
    {
        byte[] page = PforEncoder.EncodeInt32s([1, 2, 3, 4, 5, 6, 7, 8]);
        BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(7, 4), 0xFFFF);

        var ex = Assert.Throws<ParquetFormatException>(() => Decode32(page, 8));
        Assert.Contains("outside the page body", ex.Message);
    }

    /// <summary>An offset pointing into the offset array itself, rather than past it.</summary>
    [Fact]
    public void Rejects_VectorOffsetInsideTheOffsetArray()
    {
        byte[] page = PforEncoder.EncodeInt32s([1, 2, 3, 4, 5, 6, 7, 8]);
        BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(7, 4), 0);

        var ex = Assert.Throws<ParquetFormatException>(() => Decode32(page, 8));
        Assert.Contains("outside the page body", ex.Message);
    }

    /// <summary>
    /// A vector shorter than its own header claims, where the shortfall is only visible from the
    /// next vector's offset.
    /// </summary>
    /// <remarks>
    /// A vector sliced to the end of the page rather than to the next offset reads on into the
    /// next vector's bytes, satisfies every length check, and decodes whatever it finds. Nothing
    /// downstream notices: the value count comes from the page header, so the output is the right
    /// shape and the wrong data.
    /// </remarks>
    [Fact]
    public void Rejects_VectorTruncatedByTheFollowingOffset()
    {
        var values = Enumerable.Range(0, 32).Select(i => 1000 + (i * 37) % 4096).ToArray();
        byte[] page = PforEncoder.EncodeInt32s(values, logVectorSize: 3);

        const int OffsetArrayAt = 7;
        const int NumVectors = 4;
        uint first = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(OffsetArrayAt, 4));
        uint second = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(OffsetArrayAt + 4, 4));

        // Widen the first vector's declared width to 32 bits, so its header claims 7 + 8*4 = 39
        // bytes where the next offset leaves it far fewer. Nothing else in the page moves.
        page[OffsetArrayAt + NumVectors * 4 + (int)(first - NumVectors * 4) + 4] = 32;

        const int Declared = 7 + 8 * 4;
        int extentFromNextOffset = (int)(second - first);
        int extentToEndOfPage = page.Length - OffsetArrayAt - (int)first;

        // The precondition that makes this a regression test rather than a tautology: the vector
        // is short of what it declares only when measured against the next offset. Sliced to the
        // end of the page instead, there is room to spare and the truncation check never fires —
        // the decoder reads on into the following vectors and returns their bytes as data.
        Assert.True(extentFromNextOffset < Declared,
            $"vector 0 spans {extentFromNextOffset} bytes but declares {Declared}");
        Assert.True(extentToEndOfPage >= Declared,
            $"the rest of the page is {extentToEndOfPage} bytes, which would satisfy {Declared}");

        var ex = Assert.Throws<ParquetFormatException>(() => Decode32(page, values.Length));
        Assert.Contains("truncated", ex.Message);
    }

    /// <summary>
    /// Offsets strictly increase — each is the previous one plus the previous vector's stored
    /// size — so a decreasing one overlaps the vector before it.
    /// </summary>
    [Fact]
    public void Rejects_NonMonotonicVectorOffsets()
    {
        var values = Enumerable.Range(0, 16).Select(i => 1000 + (i * 37) % 4096).ToArray();
        byte[] page = PforEncoder.EncodeInt32s(values, logVectorSize: 3);

        uint first = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(7, 4));
        BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(7 + 4, 4), first);

        var ex = Assert.Throws<ParquetFormatException>(() => Decode32(page, values.Length));
        Assert.Contains("not a forward range", ex.Message);
    }

    [Fact]
    public void Rejects_PageTooSmallForAHeader()
    {
        Assert.Throws<ParquetFormatException>(() => Decode32([0, 10, 4], 8));
    }

    // ───── End to end ─────

    [Fact]
    public async Task File_RoundTripsInt32AndInt64Columns()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("keys", Int32Type.Default, nullable: false))
            .Field(new Field("stamps", Int64Type.Default, nullable: false))
            .Build();

        var keys = Enumerable.Range(0, 4000).Select(i => i % 211 == 0 ? 2_415_022 : 2_450_815 + i % 2191).ToArray();
        var stamps = Enumerable.Range(0, 4000).Select(i => 1_700_000_000_000L + i * 7).ToArray();

        var batch = new RecordBatch(schema,
            [new Int32Array.Builder().AppendRange(keys).Build(),
             new Int64Array.Builder().AppendRange(stamps).Build()],
            keys.Length);

        string path = await WriteAsync(batch, new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V2,
            DictionaryEnabled = false,
            IntegerEncoding = IntegerEncoding.Pfor,
        });

        var (read, metadata) = await ReadAsync(path);

        Assert.Equal(keys, ((Int32Array)read.Column(0)).Values.ToArray());
        Assert.Equal(stamps, ((Int64Array)read.Column(1)).Values.ToArray());

        foreach (var column in metadata.RowGroups[0].Columns)
            Assert.Contains(Encoding.Pfor, column.MetaData!.Encodings);
    }

    [Fact]
    public async Task File_RoundTripsNullableColumns()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("keys", Int32Type.Default, nullable: true))
            .Build();

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 3000; i++)
        {
            if (i % 7 == 0)
                builder.AppendNull();
            else
                builder.Append(1000 + i % 64);
        }

        var array = builder.Build();
        var batch = new RecordBatch(schema, [array], array.Length);

        string path = await WriteAsync(batch, new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V2,
            DictionaryEnabled = false,
            IntegerEncoding = IntegerEncoding.Pfor,
        });

        var (read, _) = await ReadAsync(path);
        var readArray = (Int32Array)read.Column(0);

        Assert.Equal(array.Length, readArray.Length);
        for (int i = 0; i < array.Length; i++)
            Assert.Equal(array.GetValue(i), readArray.GetValue(i));
    }

    /// <summary>
    /// Random integers have no outliers to exploit and no frame worth subtracting, so the writer
    /// must fall back to PLAIN rather than write a page bigger than the values it holds.
    /// </summary>
    [Fact]
    public async Task File_FallsBackToPlainWhenPforDoesNotShrinkThePage()
    {
        var random = new Random(20260904);
        var values = new int[8000];
        for (int i = 0; i < values.Length; i++)
            values[i] = random.Next(int.MinValue, int.MaxValue);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("noise", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new Int32Array.Builder().AppendRange(values).Build()], values.Length);

        string path = await WriteAsync(batch, new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V2,
            DictionaryEnabled = false,
            IntegerEncoding = IntegerEncoding.Pfor,
        });

        var (read, metadata) = await ReadAsync(path);

        Assert.Equal(values, ((Int32Array)read.Column(0)).Values.ToArray());
        Assert.Contains(Encoding.Plain, metadata.RowGroups[0].Columns[0].MetaData!.Encodings);
        Assert.DoesNotContain(Encoding.Pfor, metadata.RowGroups[0].Columns[0].MetaData!.Encodings);
    }

    [Fact]
    public void Resolver_SelectsPforForIntegerColumnsOnly()
    {
        Assert.Equal(Encoding.Pfor, EncodingStrategyResolver.GetV2Encoding(
            PhysicalType.Int32, ByteArrayEncoding.DeltaLengthByteArray,
            FloatingPointEncoding.ByteStreamSplit, IntegerEncoding.Pfor));
        Assert.Equal(Encoding.Pfor, EncodingStrategyResolver.GetV2Encoding(
            PhysicalType.Int64, ByteArrayEncoding.DeltaLengthByteArray,
            FloatingPointEncoding.ByteStreamSplit, IntegerEncoding.Pfor));

        // The setting names an integer encoding and must not leak onto anything else.
        Assert.Equal(Encoding.ByteStreamSplit, EncodingStrategyResolver.GetV2Encoding(
            PhysicalType.Double, ByteArrayEncoding.DeltaLengthByteArray,
            FloatingPointEncoding.ByteStreamSplit, IntegerEncoding.Pfor));
        Assert.Equal(Encoding.DeltaLengthByteArray, EncodingStrategyResolver.GetV2Encoding(
            PhysicalType.ByteArray, ByteArrayEncoding.DeltaLengthByteArray,
            FloatingPointEncoding.ByteStreamSplit, IntegerEncoding.Pfor));
    }

    // ───── Helpers ─────

    /// <summary>Wraps hand-written vectors in a page header and offset array.</summary>
    private static byte[] Page(int logVectorSize, int valueByteWidth, int numElements, params byte[][] vectors)
    {
        var page = new List<byte>
        {
            0,
            (byte)logVectorSize,
            (byte)valueByteWidth,
        };
        page.AddRange(BitConverter.GetBytes(numElements));

        uint offset = (uint)(vectors.Length * 4);
        foreach (var vector in vectors)
        {
            page.AddRange(BitConverter.GetBytes(offset));
            offset += (uint)vector.Length;
        }

        foreach (var vector in vectors)
            page.AddRange(vector);

        return [.. page];
    }

    /// <summary>Slices vector <paramref name="index"/> back out of an encoded page.</summary>
    private static ReadOnlySpan<byte> VectorOf(byte[] page, int index, int numVectors)
    {
        var body = page.AsSpan(7);
        uint start = BinaryPrimitives.ReadUInt32LittleEndian(body.Slice(index * 4, 4));
        uint end = index + 1 < numVectors
            ? BinaryPrimitives.ReadUInt32LittleEndian(body.Slice((index + 1) * 4, 4))
            : (uint)body.Length;

        return body.Slice((int)start, (int)(end - start));
    }

    private static void Decode32(byte[] page, int count) =>
        PforDecoder.DecodeInt32s(page, new int[count], count);

    private static void Decode64(byte[] page, int count) =>
        PforDecoder.DecodeInt64s(page, new long[count], count);

    private static async Task<(RecordBatch Batch, FileMetaData Metadata)> ReadAsync(string path)
    {
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var metadata = await reader.ReadMetadataAsync();
        var batch = await reader.ReadRowGroupAsync(0);
        return (batch, metadata);
    }

    private async Task<string> WriteAsync(RecordBatch batch, ParquetWriteOptions options)
    {
        string path = Path.Combine(_tempDir, $"pfor-{Guid.NewGuid().ToString("N")[..8]}.parquet");

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        return path;
    }
}

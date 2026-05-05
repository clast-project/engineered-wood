// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Tests.TestData;

namespace EngineeredWood.Vortex.Tests;

public class VortexColumnReaderTests
{
    [Fact]
    public async Task ReadsInt32Column_VortexPrimitive()
    {
        // primitive_int_random.vortex uses non-monotonic, wide-range i32s
        // chosen to defeat vortex.sequence / FoR / bit-packed and force
        // the canonical vortex.primitive encoding.
        var expected = new[] { 42, -987_654_321, 2_147_483_647, -1, 12_345, -2_147_483_648 };

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("primitive_int_random.vortex"));

        Assert.Single(reader.ColumnPlans);
        var plan = reader.ColumnPlans[0];
        Assert.IsType<Int32Type>(plan.ArrowType);
        Assert.Equal((ulong)expected.Length, plan.TotalRows);

        var array = await reader.ReadColumnAsync(0);
        var int32 = Assert.IsType<Int32Array>(array);
        Assert.Equal(expected.Length, int32.Length);
        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], int32.GetValue(i));
        Assert.Equal(0, int32.NullCount);
    }

    [Fact]
    public async Task ReadsInt32Column_VortexSequence()
    {
        // struct_int_3rows.vortex carries [1,2,3] which vortex compresses as
        // an arithmetic sequence (vortex.sequence with base=1, multiplier=1).
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("struct_int_3rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var int32 = Assert.IsType<Int32Array>(array);
        Assert.Equal(3, int32.Length);
        Assert.Equal(1, int32.GetValue(0));
        Assert.Equal(2, int32.GetValue(1));
        Assert.Equal(3, int32.GetValue(2));
        Assert.Equal(0, int32.NullCount);
    }

    [Fact]
    public async Task ReadsInt32Column_VortexConstant()
    {
        // constant_int_5rows.vortex is [777, 777, 777, 777, 777]; vortex picks vortex.constant.
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("constant_int_5rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var int32 = Assert.IsType<Int32Array>(array);
        Assert.Equal(5, int32.Length);
        for (int i = 0; i < 5; i++)
            Assert.Equal(777, int32.GetValue(i));
        Assert.Equal(0, int32.NullCount);
    }

    [Fact]
    public async Task ReadsDeltaUInt64Column()
    {
        ulong x = 0xDE17ADE17AUL;
        var expected = new ulong[2048];
        ulong acc = 1_700_000_000_000_000_000UL;
        for (int i = 0; i < 2048; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            acc = unchecked(acc + (x % 1000));
            expected[i] = acc;
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("delta_int_2048rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var u64 = Assert.IsType<UInt64Array>(array);
        var raw = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, ulong>(
            u64.Data.Buffers[1].Span);
        for (int i = 0; i < 2048; i++)
            Assert.Equal(expected[i], raw[i]);
    }

    [Fact]
    public async Task ReadsSlicedDeltaColumn()
    {
        // delta_sliced_2000rows.vortex: same generator as delta_int_2048rows
        // but the DeltaArray is sliced to [10..2010] before serialization.
        // Metadata has offset=10, length=2000.
        ulong x = 0xDE17ADE17AUL;
        var allValues = new ulong[2048];
        ulong acc = 1_700_000_000_000_000_000UL;
        for (int i = 0; i < 2048; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            acc = unchecked(acc + (x % 1000));
            allValues[i] = acc;
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("delta_sliced_2000rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var u64 = Assert.IsType<UInt64Array>(array);
        Assert.Equal(2000, u64.Length);
        var raw = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, ulong>(
            u64.Data.Buffers[1].Span);
        for (int i = 0; i < 2000; i++)
            Assert.Equal(allValues[10 + i], raw[i]);
    }

    [Fact]
    public async Task ReadsRleUInt32Column()
    {
        // rle_int_2048rows.vortex: 2 chunks of 1024 u32, low-cardinality.
        // chunk 0: vals[i] = i / 50 (so 0..21).
        // chunk 1: vals[i] = (i / 50) + 10 (so 10..31).
        var expected = new uint[2048];
        for (int i = 0; i < 1024; i++) expected[i] = (uint)(i / 50);
        for (int i = 0; i < 1024; i++) expected[1024 + i] = (uint)(i / 50) + 10;

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("rle_int_2048rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var u32 = Assert.IsType<UInt32Array>(array);
        Assert.Equal(2048, u32.Length);
        var raw = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, uint>(
            u32.Data.Buffers[1].Span);
        for (int i = 0; i < 2048; i++)
            Assert.Equal(expected[i], raw[i]);
    }

    [Fact]
    public async Task ReadsPcoDoubleColumn()
    {
        // pco_double_2048rows.vortex: high-entropy doubles in [0, 1) compressed
        // with vortex.pco. Decoder routes through Clast.Pcodec.PcoWrappedDecoder.
        ulong x = 0xCAFE_F00D_BEEF_DEADUL;
        var expected = new double[2048];
        for (int i = 0; i < expected.Length; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            expected[i] = (double)(x >> 11) / (double)(1UL << 53);
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("pco_double_2048rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var f64 = Assert.IsType<DoubleArray>(array);
        Assert.Equal(2048, f64.Length);
        var raw = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, double>(
            f64.Data.Buffers[1].Span);
        for (int i = 0; i < 2048; i++)
            Assert.Equal(expected[i], raw[i]);
    }

    [Fact]
    public async Task ReadsSlicedRleColumn()
    {
        // rle_sliced_2000rows.vortex: RLE-encoded over the same input as
        // rle_int_2048rows but constructed with metadata.offset=10 and
        // length=2000. The decoder reads indices [10, 2010) from the full
        // 2048-row indices buffer and emits 2000 values.
        var allValues = new uint[2048];
        for (int i = 0; i < 1024; i++) allValues[i] = (uint)(i / 50);
        for (int i = 0; i < 1024; i++) allValues[1024 + i] = (uint)(i / 50) + 10;

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("rle_sliced_2000rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var u32 = Assert.IsType<UInt32Array>(array);
        Assert.Equal(2000, u32.Length);
        var raw = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, uint>(
            u32.Data.Buffers[1].Span);
        for (int i = 0; i < 2000; i++)
            Assert.Equal(allValues[10 + i], raw[i]);
    }

    [Fact]
    public async Task ReadsNullablePcoDoubleColumn()
    {
        // pco_nullable_2048rows.vortex: same generator as pco_double_2048rows
        // but every 7th row is null. Vortex compresses only valid values; the
        // decoder must splice the dense decompressed buffer into the sparse
        // output by walking the validity bitmap.
        ulong x = 0xCAFE_F00D_BEEF_DEADUL;
        var expected = new double?[2048];
        int expectedNullCount = 0;
        for (int i = 0; i < 2048; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            double v = (double)(x >> 11) / (double)(1UL << 53);
            if (i % 7 == 0) { expected[i] = null; expectedNullCount++; }
            else expected[i] = v;
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("pco_nullable_2048rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var f64 = Assert.IsType<DoubleArray>(array);
        Assert.Equal(2048, f64.Length);
        Assert.Equal(expectedNullCount, f64.NullCount);
        for (int i = 0; i < 2048; i++)
        {
            if (expected[i] is null)
                Assert.False(f64.IsValid(i));
            else
            {
                Assert.True(f64.IsValid(i));
                Assert.Equal(expected[i]!.Value, f64.GetValue(i));
            }
        }
    }

    [Fact]
    public async Task ReadsNullableRleColumn()
    {
        // rle_nullable_1024rows.vortex: u32 RLE column with indices pattern
        // [0,1,2] cycling and validity pattern [true,false,true] cycling.
        // Values dictionary = [10, 20, 30]. Decoder must propagate the
        // indices' validity bitmap to the output.
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("rle_nullable_1024rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var u32 = Assert.IsType<UInt32Array>(array);
        Assert.Equal(1024, u32.Length);

        var values = new uint[] { 10, 20, 30 };
        int expectedNullCount = 0;
        for (int i = 0; i < 1024; i++)
        {
            bool valid = (i % 3) != 1; // [true,false,true] cycling
            if (!valid)
            {
                expectedNullCount++;
                Assert.False(u32.IsValid(i));
            }
            else
            {
                Assert.True(u32.IsValid(i));
                int idx = i % 3;
                Assert.Equal(values[idx], u32.GetValue(i));
            }
        }
        Assert.Equal(expectedNullCount, u32.NullCount);
    }

    [Fact]
    public async Task ReadsUuidColumn()
    {
        // uuid_2048rows.vortex: vortex.uuid extension over FSL<U8, 16>.
        // Schema converter maps to FixedSizeBinaryType(16); ExtensionArrayDecoder
        // unwraps FSL<U8> → FixedSizeBinaryArray.
        ulong x = 0xDEADCAFEBABE1357UL;
        var expected = new byte[2048 * 16];
        for (int i = 0; i < expected.Length; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            expected[i] = (byte)(x >> 32);
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("uuid_2048rows.vortex"));

        var fsbType = Assert.IsType<Apache.Arrow.Types.FixedSizeBinaryType>(
            reader.Schema.FieldsList[0].DataType);
        Assert.Equal(16, fsbType.ByteWidth);

        var array = await reader.ReadColumnAsync(0);
        var fsbArr = Assert.IsType<Apache.Arrow.Arrays.FixedSizeBinaryArray>(array);
        Assert.Equal(2048, fsbArr.Length);

        var raw = fsbArr.Data.Buffers[1].Span;
        Assert.Equal(expected.Length, raw.Length);
        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], raw[i]);
    }

    [Fact]
    public async Task ReadAllAsync_ChunkedFile_YieldsMultipleBatches()
    {
        // chunked_int_3chunks.vortex: 2.5M rows total. Vortex's writer splits
        // into ~262144-row segments under a vortex.chunked layout. ReadAllAsync
        // should yield multiple RecordBatches whose concatenated values
        // reproduce the original sequence:
        //   chunk 0: 0..1_000_000
        //   chunk 1: 2_000_000..3_000_000
        //   chunk 2: 4_000_000..4_500_000
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("chunked_int_3chunks.vortex"));

        Assert.Equal(2_500_000L, reader.NumberOfRows);
        Assert.True(reader.ColumnPlans[0].ChunkCount > 1,
            "expected multi-chunk plan, got " + reader.ColumnPlans[0].ChunkCount);

        long totalRows = 0;
        long expectedNext = 0;
        long boundaryAfterChunk0 = 1_000_000;
        long boundaryAfterChunk1 = 2_000_000; // gap of 1M; jumps to 2M
        long boundaryAfterChunk2 = 3_000_000; // gap of 1M; jumps to 4M
        long batchCount = 0;

        await foreach (var batch in reader.ReadAllAsync())
        {
            try
            {
                batchCount++;
                Assert.Equal(1, batch.ColumnCount);
                var int32 = Assert.IsType<Int32Array>(batch.Column(0));
                var raw = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(
                    int32.Data.Buffers[1].Span);
                for (int i = 0; i < int32.Length; i++)
                {
                    // Apply gaps between original input chunks.
                    if (expectedNext == boundaryAfterChunk0) expectedNext = boundaryAfterChunk1;
                    else if (expectedNext == boundaryAfterChunk2) expectedNext = 4_000_000;
                    Assert.Equal(expectedNext, raw[i]);
                    expectedNext++;
                }
                totalRows += int32.Length;
            }
            finally
            {
                batch.Dispose();
            }
        }

        Assert.Equal(2_500_000L, totalRows);
        Assert.True(batchCount > 1, $"expected multiple batches, got {batchCount}");
    }

    [Fact]
    public async Task ReadsListIntColumn()
    {
        // list_int_2048rows.vortex: List<i32> with 0..6 elements per row.
        // Reproduce the per-row lengths + values the Rust generator produced.
        ulong x = 0x115_75EEDUL;
        var expectedElements = new List<int>();
        var expectedOffsets = new List<int> { 0 };
        for (int i = 0; i < 2048; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            int len = (int)(x % 7);
            for (int j = 0; j < len; j++)
            {
                x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
                expectedElements.Add((int)(x % 1000));
            }
            expectedOffsets.Add(expectedElements.Count);
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("list_int_2048rows.vortex"));

        Assert.IsType<ListType>(reader.Schema.FieldsList[0].DataType);

        var array = await reader.ReadColumnAsync(0);
        var listArr = Assert.IsType<ListArray>(array);
        Assert.Equal(2048, listArr.Length);

        var inner = Assert.IsType<Int32Array>(listArr.Values);
        Assert.Equal(expectedElements.Count, inner.Length);

        // Spot-check the first few rows by walking offsets.
        var offsetsSpan = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(
            listArr.Data.Buffers[1].Span);
        for (int i = 0; i <= 2048; i++)
            Assert.Equal(expectedOffsets[i], offsetsSpan[i]);

        for (int i = 0; i < expectedElements.Count; i++)
            Assert.Equal(expectedElements[i], inner.GetValue(i));
    }

    [Fact]
    public async Task ReadsListIntColumn_AsLargeList()
    {
        // Same fixture as ReadsListIntColumn, but opened with useLargeList:true so
        // the schema converter emits LargeListType and the decoder produces a
        // LargeListArray with i64 cumulative offsets.
        ulong x = 0x115_75EEDUL;
        var expectedElements = new List<int>();
        var expectedOffsets = new List<long> { 0 };
        for (int i = 0; i < 2048; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            int len = (int)(x % 7);
            for (int j = 0; j < len; j++)
            {
                x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
                expectedElements.Add((int)(x % 1000));
            }
            expectedOffsets.Add(expectedElements.Count);
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("list_int_2048rows.vortex"), useLargeList: true);

        Assert.IsType<LargeListType>(reader.Schema.FieldsList[0].DataType);

        var array = await reader.ReadColumnAsync(0);
        var listArr = Assert.IsType<LargeListArray>(array);
        Assert.Equal(2048, listArr.Length);

        var inner = Assert.IsType<Int32Array>(listArr.Values);
        Assert.Equal(expectedElements.Count, inner.Length);

        var offsetsSpan = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, long>(
            listArr.Data.Buffers[1].Span);
        for (int i = 0; i <= 2048; i++)
            Assert.Equal(expectedOffsets[i], offsetsSpan[i]);

        for (int i = 0; i < expectedElements.Count; i++)
            Assert.Equal(expectedElements[i], inner.GetValue(i));
    }

    [Fact]
    public async Task ReadsFixedSizeListIntColumn()
    {
        // fsl_int_2048rows.vortex: FixedSizeList<i32, 3>, 2048 rows × 3 = 6144 elements.
        ulong x = 0xF15F00D5UL;
        var expectedElements = new int[6144];
        for (int i = 0; i < 6144; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            expectedElements[i] = (int)(x % 1000);
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("fsl_int_2048rows.vortex"));

        var fslType = Assert.IsType<FixedSizeListType>(reader.Schema.FieldsList[0].DataType);
        Assert.Equal(3, fslType.ListSize);

        var array = await reader.ReadColumnAsync(0);
        var fslArr = Assert.IsType<FixedSizeListArray>(array);
        Assert.Equal(2048, fslArr.Length);

        var inner = Assert.IsType<Int32Array>(fslArr.Values);
        Assert.Equal(6144, inner.Length);
        for (int i = 0; i < 6144; i++)
            Assert.Equal(expectedElements[i], inner.GetValue(i));
    }

    [Fact]
    public async Task ReadsDateDaysColumn()
    {
        // date_days_2048rows.vortex: Date(Days) → Date32. Storage is i32.
        // Vortex picks vortex.ext → fastlanes.for → fastlanes.bitpacked.
        ulong x = 0xDEC0DED1UL;
        var expected = new int[2048];
        for (int i = 0; i < 2048; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            expected[i] = 19_723 + (int)(x % (5 * 365));
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("date_days_2048rows.vortex"));

        Assert.IsType<Date32Type>(reader.Schema.FieldsList[0].DataType);

        var array = await reader.ReadColumnAsync(0);
        var dateArr = Assert.IsType<Date32Array>(array);
        Assert.Equal(2048, dateArr.Length);
        var raw = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(
            dateArr.Data.Buffers[1].Span);
        for (int i = 0; i < 2048; i++)
            Assert.Equal(expected[i], raw[i]);
    }

    [Fact]
    public async Task ReadsTimeMicroColumn()
    {
        // time_us_2048rows.vortex: Time(Microseconds) → Time64(Microsecond). Storage i64.
        const long UsPerDay = 24L * 60 * 60 * 1_000_000;
        ulong x = 0xC1A551CUL;
        var expected = new long[2048];
        for (int i = 0; i < 2048; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            expected[i] = (long)(x % (ulong)UsPerDay);
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("time_us_2048rows.vortex"));

        var t = Assert.IsType<Time64Type>(reader.Schema.FieldsList[0].DataType);
        Assert.Equal(Apache.Arrow.Types.TimeUnit.Microsecond, t.Unit);

        var array = await reader.ReadColumnAsync(0);
        var timeArr = Assert.IsType<Time64Array>(array);
        Assert.Equal(2048, timeArr.Length);
        var raw = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, long>(
            timeArr.Data.Buffers[1].Span);
        for (int i = 0; i < 2048; i++)
            Assert.Equal(expected[i], raw[i]);
    }

    [Fact]
    public async Task ReadsTimestampMicroColumn()
    {
        // timestamp_us_2048rows.vortex: 2048 microsecond timestamps spread over
        // a year. Vortex picks vortex.datetimeparts (days/seconds/subseconds)
        // wrapping fastlanes.for + bitpacked.
        const long BaseUs = 1_704_067_200_000_000L; // 2024-01-01 UTC in us
        const long SecondsPerDay = 86_400L;
        const long UsPerSecond = 1_000_000L;
        ulong x = 0xCAFED00DUL;
        var expected = new long[2048];
        for (int i = 0; i < 2048; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            long offset = (long)(x % (ulong)(365 * SecondsPerDay * UsPerSecond));
            expected[i] = BaseUs + offset;
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("timestamp_us_2048rows.vortex"));

        var field = reader.Schema.FieldsList[0];
        var ts = Assert.IsType<TimestampType>(field.DataType);
        Assert.Equal(Apache.Arrow.Types.TimeUnit.Microsecond, ts.Unit);
        Assert.Null(ts.Timezone);

        var array = await reader.ReadColumnAsync(0);
        var tsArr = Assert.IsType<TimestampArray>(array);
        Assert.Equal(2048, tsArr.Length);
        // Read raw i64 values from the values buffer.
        var raw = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, long>(
            tsArr.Data.Buffers[1].Span);
        for (int i = 0; i < 2048; i++)
            Assert.Equal(expected[i], raw[i]);
    }

    [Fact]
    public async Task ReadsDecimal128Column()
    {
        // decimal128_2048rows.vortex: Decimal(precision=10, scale=2) column
        // with 2048 rows of i64 unscaled values in [-50_000_000, 50_000_000-1].
        // Vortex picks vortex.decimal_byte_parts wrapping fastlanes.for + bitpacked.
        ulong x = 0xC0FFEEDECAFUL;
        var expected = new long[2048];
        for (int i = 0; i < 2048; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            expected[i] = ((long)(x % 100_000_000)) - 50_000_000;
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("decimal128_2048rows.vortex"));

        var field = reader.Schema.FieldsList[0];
        var dec = Assert.IsType<Decimal128Type>(field.DataType);
        Assert.Equal(10, dec.Precision);
        Assert.Equal(2, dec.Scale);

        var array = await reader.ReadColumnAsync(0);
        var decArr = Assert.IsType<Decimal128Array>(array);
        Assert.Equal(2048, decArr.Length);

        // Decimal128Array.GetValue returns a string; check the underlying i128 values.
        // Each row is 16 little-endian bytes; for our range they fit in 64 bits.
        var data = decArr.Data.Buffers[1].Span;
        for (int i = 0; i < 2048; i++)
        {
            var lo = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(data.Slice(i * 16, 8));
            var hi = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(data.Slice(i * 16 + 8, 8));
            Assert.Equal(expected[i], lo);
            // Sign-extension: hi should be 0 for non-negative, -1 for negative.
            Assert.Equal(expected[i] < 0 ? -1L : 0L, hi);
        }
    }

    [Fact]
    public async Task ReadsDecimal256Column()
    {
        // decimal256_2048rows.vortex: Decimal(precision=40, scale=2) column
        // with 2048 i128 unscaled values straddling the i64 boundary
        // (random64 << 60). Schema → Decimal256Type; vortex.decimal stores
        // values_type=I128 → DecimalArrayDecoder must sign-extend to 32 bytes.
        ulong x = 0xC0FFEEDECAFBABEUL;
        var expectedLow = new long[2048];   // low 8 bytes of i128
        var expectedMid = new long[2048];   // upper 8 bytes of i128 (= sign-extended high after << 60)
        for (int i = 0; i < 2048; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            // (x as i64 as i128) << 60: sign-extend x's lower 64 bits to i128, shift left 60.
            // Equivalent in 64-bit pieces: lo = x << 60 (within u64), hi = (sign of x) ? -1 : 0,
            // then shifted: combined_hi64 = (x >> 4) signed-shifted with the original sign.
            long signed64 = unchecked((long)x);
            // i128 result: hi64 = signed64 >> 4 (arithmetic), lo64 = (ulong)signed64 << 60.
            long hi = signed64 >> 4;
            long lo = unchecked((long)((ulong)signed64 << 60));
            expectedLow[i] = lo;
            expectedMid[i] = hi;
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("decimal256_2048rows.vortex"));

        var field = reader.Schema.FieldsList[0];
        var dec = Assert.IsType<Decimal256Type>(field.DataType);
        Assert.Equal(40, dec.Precision);
        Assert.Equal(2, dec.Scale);

        var array = await reader.ReadColumnAsync(0);
        var decArr = Assert.IsType<Decimal256Array>(array);
        Assert.Equal(2048, decArr.Length);

        // Each row is 32 LE bytes. We split into four 8-byte words: lo0, lo1, hi0, hi1.
        // For our values: i128 sign-extended → low 16 bytes = i128 LE,
        // upper 16 bytes = sign-fill (0 or -1).
        var data = decArr.Data.Buffers[1].Span;
        for (int i = 0; i < 2048; i++)
        {
            var w0 = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(data.Slice(i * 32, 8));
            var w1 = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(data.Slice(i * 32 + 8, 8));
            var w2 = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(data.Slice(i * 32 + 16, 8));
            var w3 = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(data.Slice(i * 32 + 24, 8));
            Assert.Equal(expectedLow[i], w0);
            Assert.Equal(expectedMid[i], w1);
            // Upper 16 bytes are sign-fill of the i128 value.
            long fill = expectedMid[i] < 0 ? -1L : 0L;
            Assert.Equal(fill, w2);
            Assert.Equal(fill, w3);
        }
    }

    [Fact]
    public async Task ReadsAlpWithPatchesColumn()
    {
        // alp_patches_2048rows.vortex: most rows are 2-decimal-place doubles
        // that fit ALP; every 100th row (offset 19) is a full-precision irrational
        // value that ALP can't encode cleanly → stored as a patch.
        ulong x = 0xBADBEEFFEEDFACEUL;
        var expected = new double[2048];
        for (int i = 0; i < 2048; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            if (i % 100 == 19)
            {
                ulong bits = (x & 0x000F_FFFF_FFFF_FFFFUL) | 0x4080_0000_0000_0000UL;
                expected[i] = BitConverter.Int64BitsToDouble(unchecked((long)bits));
            }
            else
            {
                long cents = (long)(x % 100_000);
                expected[i] = cents / 100.0;
            }
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("alp_patches_2048rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var doubles = Assert.IsType<DoubleArray>(array);
        Assert.Equal(2048, doubles.Length);
        for (int i = 0; i < 2048; i++)
        {
            // Patches are bit-exact, regular ALP is precision-10.
            if (i % 100 == 19)
                Assert.Equal(expected[i], doubles.GetValue(i)!.Value);
            else
                Assert.Equal(expected[i], doubles.GetValue(i)!.Value, 10);
        }
    }

    [Fact]
    public async Task ReadsBitPackedWithPatchesColumn()
    {
        // bitpacked_patches_2048rows.vortex: 2048 i32, mostly [0, 99]; every
        // 100th row (offset 17) is a large outlier. Vortex should bit-pack the
        // small values and store the outliers as patches.
        ulong x = 0xACEDFACEUL;
        var expected = new int[2048];
        for (int i = 0; i < 2048; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            expected[i] = i % 100 == 17
                ? 1_000_000 + (int)(x % 100_000)
                : (int)(x % 100);
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("bitpacked_patches_2048rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var int32 = Assert.IsType<Int32Array>(array);
        Assert.Equal(2048, int32.Length);
        for (int i = 0; i < 2048; i++)
            Assert.Equal(expected[i], int32.GetValue(i));
    }

    [Fact]
    public async Task ReadsNullableBitPackedIntColumn()
    {
        // nullable_bitpacked_2048rows.vortex: 2048 nullable i32 values in [0, 99]
        // (~80% valid). Vortex picks fastlanes.bitpacked with a vortex.bool
        // validity child.
        ulong x = 0xCAFEBABEDEADBEEFUL;
        var expected = new int[2048];
        var valid = new bool[2048];
        for (int i = 0; i < 2048; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            expected[i] = (int)(x % 100);
            valid[i] = (x % 5) != 0;
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("nullable_bitpacked_2048rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var int32 = Assert.IsType<Int32Array>(array);
        Assert.Equal(2048, int32.Length);
        int expectedNullCount = 0;
        for (int i = 0; i < 2048; i++) if (!valid[i]) expectedNullCount++;
        Assert.Equal(expectedNullCount, int32.NullCount);
        for (int i = 0; i < 2048; i++)
        {
            if (valid[i]) Assert.Equal(expected[i], int32.GetValue(i));
            else Assert.Null(int32.GetValue(i));
        }
    }

    [Fact]
    public async Task ReadsNullableAlpDoubleColumn()
    {
        // nullable_alp_2048rows.vortex: 2048 nullable f64 prices like 12.34
        // (~80% valid). Vortex picks vortex.alp wrapping fastlanes.bitpacked
        // with vortex.bool validity child on the inner bitpacked.
        ulong x = 0xFEEBDAEDUL;
        var expected = new double[2048];
        var valid = new bool[2048];
        for (int i = 0; i < 2048; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            long cents = (long)(x % 100_000);
            expected[i] = cents / 100.0;
            valid[i] = (x % 5) != 0;
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("nullable_alp_2048rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var doubles = Assert.IsType<DoubleArray>(array);
        Assert.Equal(2048, doubles.Length);
        int expectedNullCount = 0;
        for (int i = 0; i < 2048; i++) if (!valid[i]) expectedNullCount++;
        Assert.Equal(expectedNullCount, doubles.NullCount);
        for (int i = 0; i < 2048; i++)
        {
            if (valid[i]) Assert.Equal(expected[i], doubles.GetValue(i)!.Value, 10);
            else Assert.Null(doubles.GetValue(i));
        }
    }

    [Fact]
    public async Task ReadsAlpRdDoubleColumn()
    {
        // alprd_double_2048rows.vortex: high-entropy f64 column → vortex.alprd
        // (left/right split with dict on the high bits, raw bit-pack on low bits).
        ulong x = 0xC0DEFACEUL;
        var expected = new double[2048];
        for (int i = 0; i < expected.Length; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            ulong bits = (x & 0x000F_FFFF_FFFF_FFFFUL) | 0x3FF0_0000_0000_0000UL;
            double mantissa = BitConverter.Int64BitsToDouble(unchecked((long)bits));
            int exp = (int)((x >> 52) & 0x3F) - 32;
            expected[i] = mantissa * Math.Pow(2, exp);
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("alprd_double_2048rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var doubles = Assert.IsType<DoubleArray>(array);
        Assert.Equal(expected.Length, doubles.Length);
        for (int i = 0; i < expected.Length; i++)
        {
            // ALP-RD is bit-exact (no rounding). Direct equality.
            Assert.Equal(expected[i], doubles.GetValue(i)!.Value);
        }
    }

    [Fact]
    public async Task ReadsAlpDoubleColumn()
    {
        // alp_double_2048rows.vortex: 2048 f64 values like 12.34, 5.67, etc.;
        // vortex picks vortex.alp wrapping fastlanes.bitpacked.
        ulong x = 0xFEEDBEEFUL;
        var expected = new double[2048];
        for (int i = 0; i < expected.Length; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            long cents = (long)(x % 100_000);
            expected[i] = cents / 100.0;
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("alp_double_2048rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var doubles = Assert.IsType<DoubleArray>(array);
        Assert.Equal(expected.Length, doubles.Length);
        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], doubles.GetValue(i)!.Value, 10);
    }

    [Fact]
    public async Task ReadsForIntColumn()
    {
        // for_int_2048rows.vortex: 2048 i32 values in [1_000_000, 1_000_099];
        // vortex picks fastlanes.for wrapping fastlanes.bitpacked.
        ulong x = 0xDEADC0DEUL;
        var expected = new int[2048];
        for (int i = 0; i < expected.Length; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            expected[i] = 1_000_000 + (int)(x % 100);
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("for_int_2048rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var int32 = Assert.IsType<Int32Array>(array);
        Assert.Equal(expected.Length, int32.Length);
        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], int32.GetValue(i));
    }

    [Fact]
    public async Task ReadsBitPackedIntColumn()
    {
        // bitpacked_int_2048rows.vortex: 2048 i32 values in [0, 99]; vortex
        // picks fastlanes.bitpacked at bit_width=7. Reproduce the exact
        // values the Rust generator produced (seeded LCG).
        ulong x = 0xBADC0FFEEUL;
        var expected = new int[2048];
        for (int i = 0; i < expected.Length; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            expected[i] = (int)(x % 100);
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("bitpacked_int_2048rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var int32 = Assert.IsType<Int32Array>(array);
        Assert.Equal(expected.Length, int32.Length);
        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], int32.GetValue(i));
    }

    [Fact]
    public async Task ReadsSlicedBitPackedColumn()
    {
        // bitpacked_sliced_2000rows.vortex: same input as bitpacked_int_2048rows
        // but the BitPackedArray was sliced to [10..2010] before serialization,
        // so the metadata has offset=10 and length=2000. The decoder must
        // unpack both 1024-row chunks and emit rows [10, 2010).
        ulong x = 0xBADC0FFEEUL;
        var allValues = new int[2048];
        for (int i = 0; i < allValues.Length; i++)
        {
            x = unchecked(x * 6364136223846793005UL + 1442695040888963407UL);
            allValues[i] = (int)(x % 100);
        }

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("bitpacked_sliced_2000rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var int32 = Assert.IsType<Int32Array>(array);
        Assert.Equal(2000, int32.Length);
        for (int i = 0; i < 2000; i++)
            Assert.Equal(allValues[10 + i], int32.GetValue(i));
    }

    [Fact]
    public async Task ReadsFsstStringColumn_TopLevel()
    {
        // fsst_string_64rows.vortex: 64 distinct strings with shared prefixes;
        // vortex picks vortex.fsst at the array level (no dict wrapper).
        // The uncompressed_lengths child is itself vortex.runend (similar
        // lengths), so this also exercises the run-end decoder.
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("fsst_string_64rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var s = Assert.IsType<StringArray>(array);
        Assert.Equal(64, s.Length);
        for (int i = 0; i < 64; i++)
        {
            var expected = $"user-event-{i:D4}-payload-{i * 137}";
            Assert.Equal(expected, s.GetString(i));
        }
    }

    [Fact]
    public async Task ReadsFsstStringColumn_ViaDictLayout()
    {
        // dict_string_64rows.vortex: 64 rows from a 6-string palette, default
        // strategy → vortex.dict layout with vortex.fsst values dictionary.
        // Exercises BOTH the dict layout reconstruction AND the FSST decoder.
        var palette = new[] { "alpha", "bravo", "charlie", "delta", "echo", "foxtrot" };

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("dict_string_64rows.vortex"));

        Assert.IsType<EngineeredWood.Vortex.Layouts.DictColumnPlan>(reader.ColumnPlans[0]);

        var array = await reader.ReadColumnAsync(0);
        var s = Assert.IsType<StringArray>(array);
        Assert.Equal(64, s.Length);
        var paletteSet = new HashSet<string>(palette);
        for (int i = 0; i < s.Length; i++)
        {
            var v = s.GetString(i);
            Assert.Contains(v, paletteSet);
        }
    }

    [Fact]
    public async Task ReadsDictIntColumn()
    {
        // dict_int_64rows.vortex carries 64 rows from a 4-value palette;
        // vortex picks the vortex.dict LAYOUT (values dict + per-row codes).
        // Both children are vortex.primitive arrays so we don't need the FSST
        // decoder — letting us validate the dict reconstruction path.
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("dict_int_64rows.vortex"));

        Assert.IsType<EngineeredWood.Vortex.Layouts.DictColumnPlan>(reader.ColumnPlans[0]);

        var array = await reader.ReadColumnAsync(0);
        var int32 = Assert.IsType<Int32Array>(array);
        Assert.Equal(64, int32.Length);

        // All values must be one of the 4 palette entries used by the writer.
        var palette = new HashSet<int> { 10_001, 99_999, -42_000, 7 };
        for (int i = 0; i < int32.Length; i++)
        {
            var v = int32.GetValue(i);
            Assert.NotNull(v);
            Assert.Contains(v!.Value, palette);
        }
    }

    [Fact]
    public async Task ReadsStringColumn_VortexVarBinView()
    {
        // string_col_5rows.vortex uses a no-compress writer strategy so vortex
        // emits the canonical vortex.varbinview encoding (German strings;
        // 16-byte view per row, all our test strings inline since they're ≤ 12 bytes).
        var expected = new[] { "alpha", "be", "γ-particle", "", "delta-9" };

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("string_col_5rows.vortex"));

        Assert.Single(reader.ColumnPlans);
        var array = await reader.ReadColumnAsync(0);
        var s = Assert.IsType<StringArray>(array);
        Assert.Equal(expected.Length, s.Length);
        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], s.GetString(i));
        Assert.Equal(0, s.NullCount);
    }

    [Fact]
    public async Task ReadsMultiColumnFixture()
    {
        // multi_col_4rows.vortex: three columns hitting different encodings.
        //   a (i32) = [100, -50, 7, 999_999]    → vortex.primitive
        //   b (i64) = [42, 42, 42, 42]          → vortex.constant
        //   c (i32) = [1, 2, 3, 4]              → vortex.sequence
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("multi_col_4rows.vortex"));

        Assert.Equal(3, reader.ColumnPlans.Count);
        Assert.Equal(4L, reader.NumberOfRows);

        var batches = new List<RecordBatch>();
        await foreach (var b in reader.ReadAllAsync()) batches.Add(b);
        try
        {
            var batch = Assert.Single(batches);
            Assert.Equal(4, batch.Length);
            Assert.Equal(3, batch.ColumnCount);

            var a = Assert.IsType<Int32Array>(batch.Column(0));
            Assert.Equal(new int?[] { 100, -50, 7, 999_999 },
                Enumerable.Range(0, 4).Select(i => a.GetValue(i)).ToArray());

            var b = Assert.IsType<Int64Array>(batch.Column(1));
            for (int i = 0; i < 4; i++) Assert.Equal(42L, b.GetValue(i));

            var c = Assert.IsType<Int32Array>(batch.Column(2));
            for (int i = 0; i < 4; i++) Assert.Equal(i + 1, c.GetValue(i));
        }
        finally
        {
            foreach (var b in batches) b.Dispose();
        }
    }

    [Fact]
    public async Task ReadsNullableInt32Column()
    {
        // nullable_int_6rows.vortex carries values [10, _, 20, _, 99999, -7]
        // where _ is null. Vortex emits vortex.primitive with a vortex.bool
        // validity child.
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("nullable_int_6rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var int32 = Assert.IsType<Int32Array>(array);
        Assert.Equal(6, int32.Length);
        Assert.Equal(2, int32.NullCount);

        Assert.Equal(10, int32.GetValue(0));
        Assert.Null(int32.GetValue(1));
        Assert.Equal(20, int32.GetValue(2));
        Assert.Null(int32.GetValue(3));
        Assert.Equal(99999, int32.GetValue(4));
        Assert.Equal(-7, int32.GetValue(5));
    }

    [Fact]
    public async Task ReadsMaskedColumn()
    {
        // masked_int_1024rows.vortex: vortex.masked wrapping a non-null u32
        // child [0..1024) with an explicit validity bitmap. Decoder must
        // overlay the bitmap onto the inner array's ArrayData (swapping
        // Buffers[0] for the new mask) and re-wrap as a typed UInt32Array.
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("masked_int_1024rows.vortex"));

        var array = await reader.ReadColumnAsync(0);
        var u32 = Assert.IsType<UInt32Array>(array);
        Assert.Equal(1024, u32.Length);

        // Validity pattern from the Rust generator: i % 5 != 0.
        int expectedNullCount = 0;
        for (int i = 0; i < 1024; i++)
        {
            bool valid = (i % 5) != 0;
            if (!valid)
            {
                expectedNullCount++;
                Assert.False(u32.IsValid(i));
            }
            else
            {
                Assert.True(u32.IsValid(i));
                Assert.Equal((uint)i, u32.GetValue(i));
            }
        }
        Assert.Equal(expectedNullCount, u32.NullCount);
    }

    [Fact]
    public async Task ReadAllAsync_YieldsSingleBatchForUnchunkedFile()
    {
        var expected = new[] { 42, -987_654_321, 2_147_483_647, -1, 12_345, -2_147_483_648 };

        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("primitive_int_random.vortex"));

        var batches = new List<RecordBatch>();
        await foreach (var batch in reader.ReadAllAsync())
            batches.Add(batch);

        try
        {
            var batch = Assert.Single(batches);
            Assert.Equal(reader.Schema, batch.Schema);
            Assert.Equal(expected.Length, batch.Length);
            Assert.Equal(1, batch.ColumnCount);

            var int32 = Assert.IsType<Int32Array>(batch.Column(0));
            for (int i = 0; i < expected.Length; i++)
                Assert.Equal(expected[i], int32.GetValue(i));
        }
        finally
        {
            foreach (var b in batches) b.Dispose();
        }
    }
}

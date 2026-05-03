// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Writer;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// Self-roundtrip: writes a RecordBatch via <see cref="VortexFileWriter"/>,
/// reopens with <see cref="VortexFileReader"/>, asserts the result matches.
/// </summary>
public class VortexFileWriterTests
{
    [Fact]
    public async Task SelfRoundtrip_PrimitiveColumns()
    {
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("i32", Int32Type.Default, nullable: false),
            new Field("i64", Int64Type.Default, nullable: false),
            new Field("f32", FloatType.Default, nullable: false),
            new Field("f64", DoubleType.Default, nullable: false),
            new Field("u16", UInt16Type.Default, nullable: false),
            new Field("u8", UInt8Type.Default, nullable: false),
        }, metadata: null);

        const int n = 100;
        var i32 = new Int32Array.Builder();
        var i64 = new Int64Array.Builder();
        var f32 = new FloatArray.Builder();
        var f64 = new DoubleArray.Builder();
        var u16 = new UInt16Array.Builder();
        var u8 = new UInt8Array.Builder();
        for (int i = 0; i < n; i++)
        {
            i32.Append(i * 3 - 7);
            i64.Append((long)i * 1_000_000_000L + 42L);
            f32.Append(i * 0.5f);
            f64.Append(i * 1.0 / 3.0);
            u16.Append((ushort)(i & 0xFFFF));
            u8.Append((byte)(i & 0xFF));
        }
        var batch = new RecordBatch(schema, new IArrowArray[] {
            i32.Build(), i64.Build(), f32.Build(), f64.Build(), u16.Build(), u8.Build(),
        }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            Assert.Equal(schema.FieldsList.Count, reader.Schema.FieldsList.Count);
            for (int i = 0; i < schema.FieldsList.Count; i++)
            {
                Assert.Equal(schema.FieldsList[i].Name, reader.Schema.FieldsList[i].Name);
                Assert.Equal(
                    schema.FieldsList[i].DataType.GetType(),
                    reader.Schema.FieldsList[i].DataType.GetType());
            }

            var i32Read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            var i64Read = Assert.IsType<Int64Array>(await reader.ReadColumnAsync(1));
            var f32Read = Assert.IsType<FloatArray>(await reader.ReadColumnAsync(2));
            var f64Read = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(3));
            var u16Read = Assert.IsType<UInt16Array>(await reader.ReadColumnAsync(4));
            var u8Read = Assert.IsType<UInt8Array>(await reader.ReadColumnAsync(5));

            Assert.Equal(n, i32Read.Length);
            for (int i = 0; i < n; i++)
            {
                Assert.Equal(i * 3 - 7, i32Read.GetValue(i));
                Assert.Equal((long)i * 1_000_000_000L + 42L, i64Read.GetValue(i));
                Assert.Equal(i * 0.5f, f32Read.GetValue(i));
                Assert.Equal(i * 1.0 / 3.0, f64Read.GetValue(i));
                Assert.Equal((ushort)(i & 0xFFFF), u16Read.GetValue(i));
                Assert.Equal((byte)(i & 0xFF), u8Read.GetValue(i));
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task NullCountStat_IsPresentForNullableColumns()
    {
        // Verify the writer attaches an ArrayStats { null_count: N } to the
        // top-level ArrayNode for nullable columns, and omits it (HasNullCount=false)
        // for non-nullable columns.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("plain", Int32Type.Default, nullable: false),
            new Field("nullable", Int32Type.Default, nullable: true),
        }, metadata: null);

        const int n = 100;
        var plainB = new Int32Array.Builder();
        var nullB = new Int32Array.Builder();
        for (int i = 0; i < n; i++)
        {
            plainB.Append(i);
            if (i % 7 == 0) nullB.AppendNull();
            else nullB.Append(i * 11);
        }
        var batch = new RecordBatch(schema,
            new IArrowArray[] { plainB.Build(), nullB.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);

            // Reach into each column's segment + parse Array FB to inspect stats.
            for (int colIdx = 0; colIdx < 2; colIdx++)
            {
                var plan = (EngineeredWood.Vortex.Layouts.FlatColumnPlan)reader.ColumnPlans[colIdx];
                var chunk = plan.Chunks[0];
                var locator = reader.SegmentSpecs[(int)chunk.SegmentRef];
                using var local = new EngineeredWood.IO.Local.LocalRandomAccessFile(path);
                using var owner = await local.ReadAsync(new EngineeredWood.IO.FileRange(
                    checked((long)locator.Offset), checked((int)locator.Length)));
                var serialized = EngineeredWood.Vortex.Encodings.SerializedArray.Parse(owner.Memory.Span);
                var stats = serialized.Message.Root.Stats;

                if (colIdx == 0)
                {
                    // Non-nullable Int32 with values 0..99 → not constant.
                    // The writer emits is_constant=false (slot 7) but no
                    // null_count (since there are no nulls).
                    Assert.True(stats.IsPresent);
                    Assert.False(stats.HasNullCount);
                    Assert.True(stats.HasIsConstant);
                    Assert.False(stats.IsConstant);
                }
                else
                {
                    // Nullable: null_count emitted (15 nulls). Has nulls so
                    // is_constant is NOT computed (conservative).
                    Assert.True(stats.IsPresent);
                    Assert.True(stats.HasNullCount);
                    int expectedNullCount = 0;
                    for (int i = 0; i < n; i++) if (i % 7 == 0) expectedNullCount++;
                    Assert.Equal((ulong)expectedNullCount, stats.NullCount);
                    Assert.False(stats.HasIsConstant);
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task SelfRoundtrip_NullableBoolColumn()
    {
        // Nullable bool — every 5th row is null, others alternate true/false.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("flag", BooleanType.Default, nullable: true),
        }, metadata: null);

        const int n = 53; // odd, non-multiple-of-8 to exercise tail bits in both bitmaps
        var fB = new BooleanArray.Builder();
        var expected = new bool?[n];
        int expectedNullCount = 0;
        for (int i = 0; i < n; i++)
        {
            if (i % 5 == 0) { fB.AppendNull(); expected[i] = null; expectedNullCount++; }
            else { var v = (i % 2 == 0); fB.Append(v); expected[i] = v; }
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { fB.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<BooleanArray>(await reader.ReadColumnAsync(0));

            Assert.Equal(n, read.Length);
            Assert.Equal(expectedNullCount, read.NullCount);
            for (int i = 0; i < n; i++)
            {
                if (expected[i] is null)
                {
                    Assert.False(read.IsValid(i));
                }
                else
                {
                    Assert.True(read.IsValid(i));
                    Assert.Equal(expected[i]!.Value, read.GetValue(i));
                }
            }

            // Stats: null_count populated, min/max still emitted (both true and
            // false present among non-null rows). is_constant NOT set since
            // there are nulls.
            var stats = await ReadStats(reader, path, 0);
            Assert.False(stats.HasIsConstant);
            Assert.False(DecodeScalarBoolValue(stats.MinBytes));
            Assert.True(DecodeScalarBoolValue(stats.MaxBytes));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task SelfRoundtrip_BoolColumns()
    {
        // Three columns to exercise the bool-encoding paths:
        //   - mixed values
        //   - all-true (constant)
        //   - all-false (constant)
        // Plus check min/max stats end-to-end.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("flag", BooleanType.Default, nullable: false),
            new Field("alltrue", BooleanType.Default, nullable: false),
            new Field("allfalse", BooleanType.Default, nullable: false),
        }, metadata: null);

        const int n = 47; // odd, non-power-of-8 to exercise bitmap tail bits
        var fB = new BooleanArray.Builder();
        var tB = new BooleanArray.Builder();
        var fAllB = new BooleanArray.Builder();
        var expected = new bool[n];
        for (int i = 0; i < n; i++)
        {
            expected[i] = (i % 3 != 0);
            fB.Append(expected[i]);
            tB.Append(true);
            fAllB.Append(false);
        }
        var batch = new RecordBatch(schema,
            new IArrowArray[] { fB.Build(), tB.Build(), fAllB.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);

            // Verify values round-trip through our reader.
            var flagRead = Assert.IsType<BooleanArray>(await reader.ReadColumnAsync(0));
            var allTrueRead = Assert.IsType<BooleanArray>(await reader.ReadColumnAsync(1));
            var allFalseRead = Assert.IsType<BooleanArray>(await reader.ReadColumnAsync(2));
            Assert.Equal(n, flagRead.Length);
            for (int i = 0; i < n; i++)
            {
                Assert.Equal(expected[i], flagRead.GetValue(i));
                Assert.True(allTrueRead.GetValue(i));
                Assert.False(allFalseRead.GetValue(i));
            }

            // Verify min/max stats: tag 0x10 + 0/1.
            var mixedStats = await ReadStats(reader, path, 0);
            Assert.False(DecodeScalarBoolValue(mixedStats.MinBytes));
            Assert.True(DecodeScalarBoolValue(mixedStats.MaxBytes));

            var allTrueStats = await ReadStats(reader, path, 1);
            Assert.True(DecodeScalarBoolValue(allTrueStats.MinBytes));
            Assert.True(DecodeScalarBoolValue(allTrueStats.MaxBytes));
            Assert.True(allTrueStats.HasIsConstant);
            Assert.True(allTrueStats.IsConstant);

            var allFalseStats = await ReadStats(reader, path, 2);
            Assert.False(DecodeScalarBoolValue(allFalseStats.MinBytes));
            Assert.False(DecodeScalarBoolValue(allFalseStats.MaxBytes));
            Assert.True(allFalseStats.HasIsConstant);
            Assert.True(allFalseStats.IsConstant);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    private static bool DecodeScalarBoolValue(byte[] bytes)
    {
        Assert.Equal((byte)0x10, bytes[0]);
        return bytes[1] != 0;
    }

    [Fact]
    public async Task MinMaxStat_StringColumns()
    {
        // String → ScalarValue tag 0x3A (length-delimited).
        // Binary → ScalarValue tag 0x42 (length-delimited).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("name", StringType.Default, nullable: false),
            new Field("blob", BinaryType.Default, nullable: false),
        }, metadata: null);

        var nameB = new StringArray.Builder();
        var blobB = new BinaryArray.Builder();
        var names = new[] { "delta", "alpha", "echo", "bravo", "charlie" };
        foreach (var s in names) nameB.Append(s);
        // Binary: distinct payloads of varying lengths to exercise lex ordering.
        blobB.Append(new byte[] { 0x10, 0x20 });
        blobB.Append(new byte[] { 0x05 });           // smallest (lex)
        blobB.Append(new byte[] { 0x10, 0x20, 0x30 });
        blobB.Append(new byte[] { 0xFF });           // largest (lex)
        blobB.Append(new byte[] { 0x10, 0x21 });

        int n = names.Length;
        var batch = new RecordBatch(schema,
            new IArrowArray[] { nameB.Build(), blobB.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);

            // String: min="alpha", max="echo" (lex over UTF-8 bytes).
            var col0Stats = await ReadStats(reader, path, 0);
            Assert.Equal((byte)0x3A, col0Stats.MinBytes[0]);
            Assert.Equal("alpha", DecodeScalarString(col0Stats.MinBytes));
            Assert.Equal("echo", DecodeScalarString(col0Stats.MaxBytes));

            // Binary: min=0x05, max=0xFF.
            var col1Stats = await ReadStats(reader, path, 1);
            Assert.Equal((byte)0x42, col1Stats.MinBytes[0]);
            Assert.Equal(new byte[] { 0x05 }, DecodeScalarBytes(col1Stats.MinBytes));
            Assert.Equal(new byte[] { 0xFF }, DecodeScalarBytes(col1Stats.MaxBytes));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    private static string DecodeScalarString(byte[] bytes)
    {
        Assert.Equal((byte)0x3A, bytes[0]);
        int pos = 1;
        int len = (int)EngineeredWood.Encodings.Varint.ReadUnsigned(bytes.AsSpan(), ref pos);
        return System.Text.Encoding.UTF8.GetString(bytes, pos, len);
    }

    private static byte[] DecodeScalarBytes(byte[] bytes)
    {
        Assert.Equal((byte)0x42, bytes[0]);
        int pos = 1;
        int len = (int)EngineeredWood.Encodings.Varint.ReadUnsigned(bytes.AsSpan(), ref pos);
        var result = new byte[len];
        System.Buffer.BlockCopy(bytes, pos, result, 0, len);
        return result;
    }

    [Fact]
    public async Task BitPacked_NullableUInt32Roundtrips()
    {
        // Nullable narrow-range UInt32 — every 4th row null. Bitpacked emits a
        // vortex.bool validity child; null positions are zero-filled in the
        // packed buffer (the bitmap masks them on read).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", UInt32Type.Default, nullable: true),
        }, metadata: null);

        const int n = 2_500;
        var b = new UInt32Array.Builder();
        var expected = new uint?[n];
        for (int i = 0; i < n; i++)
        {
            if (i % 4 == 0) { b.AppendNull(); expected[i] = null; }
            else { var v = (uint)(i % 200); b.Append(v); expected[i] = v; }
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<UInt32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            int actualNullCount = 0;
            for (int i = 0; i < n; i++)
            {
                if (expected[i] is null)
                {
                    Assert.False(read.IsValid(i));
                    actualNullCount++;
                }
                else
                {
                    Assert.True(read.IsValid(i));
                    Assert.Equal(expected[i]!.Value, read.GetValue(i));
                }
            }
            Assert.Equal(actualNullCount, read.NullCount);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task BitPacked_SignedNonNegativeRoundtrips()
    {
        // Non-negative Int32 column → bitpacked picks up since byte layout is
        // identical to unsigned for non-negative values.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("a", Int32Type.Default, nullable: false),
            // Negative values present → bitpacked falls back to plain primitive.
            new Field("b", Int32Type.Default, nullable: false),
        }, metadata: null);

        const int n = 1_500;
        var aB = new Int32Array.Builder();
        var bB = new Int32Array.Builder();
        for (int i = 0; i < n; i++)
        {
            aB.Append(i % 100);                     // 0..99 → 7 bits
            bB.Append(i % 2 == 0 ? -i : i);         // negative on every other row
        }
        var batch = new RecordBatch(schema,
            new IArrowArray[] { aB.Build(), bB.Build() }, n);

        var compressedPath = Path.GetTempFileName();
        var rawPath = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(compressedPath))
                VortexFileWriter.Write(fs, batch, compress: true);
            using (var fs = File.Create(rawPath))
                VortexFileWriter.Write(fs, batch, compress: false);

            // Compressed file is smaller because column "a" got bitpacked
            // (~7 bits vs 32). Column "b" still falls back to plain primitive.
            var compressedSize = new FileInfo(compressedPath).Length;
            var rawSize = new FileInfo(rawPath).Length;
            Assert.True(compressedSize < rawSize,
                $"compressed={compressedSize} raw={rawSize}");

            await using var reader = await VortexFileReader.OpenAsync(compressedPath);
            var aRead = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            var bRead = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(1));
            for (int i = 0; i < n; i++)
            {
                Assert.Equal(i % 100, aRead.GetValue(i));
                Assert.Equal(i % 2 == 0 ? -i : i, bRead.GetValue(i));
            }
        }
        finally
        {
            try { File.Delete(compressedPath); } catch { }
            try { File.Delete(rawPath); } catch { }
        }
    }

    [Fact]
    public async Task BitPacked_CompressedFileRoundtrips()
    {
        // Three columns of unsigned ints with narrow ranges → bitpacked savings.
        // Plus one wide-range column that should fall back to plain primitive.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("u8small", UInt8Type.Default, nullable: false),
            new Field("u32small", UInt32Type.Default, nullable: false),
            new Field("u64small", UInt64Type.Default, nullable: false),
            new Field("u32full", UInt32Type.Default, nullable: false),
        }, metadata: null);

        const int n = 5_000; // multiple chunks (5 chunks of 1024 + tail)
        var u8B = new UInt8Array.Builder();
        var u32sB = new UInt32Array.Builder();
        var u64B = new UInt64Array.Builder();
        var u32fB = new UInt32Array.Builder();
        for (int i = 0; i < n; i++)
        {
            u8B.Append((byte)(i % 17));         // 0..16, fits in 5 bits
            u32sB.Append((uint)(i % 1000));     // 0..999, fits in 10 bits
            u64B.Append((ulong)(i % 7));        // 0..6, fits in 3 bits
            u32fB.Append((uint)(i * 1_000_003)); // wraps to high values, ≥30 bits
        }
        var batch = new RecordBatch(schema,
            new IArrowArray[] { u8B.Build(), u32sB.Build(), u64B.Build(), u32fB.Build() }, n);

        var compressedPath = Path.GetTempFileName();
        var rawPath = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(compressedPath))
                VortexFileWriter.Write(fs, batch, compress: true);
            using (var fs = File.Create(rawPath))
                VortexFileWriter.Write(fs, batch, compress: false);

            // Compressed file should be smaller — bitpacked saves ≈ (native - bw)/native
            // bytes per element on three of four columns.
            var compressedSize = new FileInfo(compressedPath).Length;
            var rawSize = new FileInfo(rawPath).Length;
            Assert.True(compressedSize < rawSize,
                $"Expected compressed file to be smaller. compressed={compressedSize} raw={rawSize}");

            // Roundtrip values from the compressed file.
            await using var reader = await VortexFileReader.OpenAsync(compressedPath);
            var u8Read = Assert.IsType<UInt8Array>(await reader.ReadColumnAsync(0));
            var u32sRead = Assert.IsType<UInt32Array>(await reader.ReadColumnAsync(1));
            var u64Read = Assert.IsType<UInt64Array>(await reader.ReadColumnAsync(2));
            var u32fRead = Assert.IsType<UInt32Array>(await reader.ReadColumnAsync(3));
            for (int i = 0; i < n; i++)
            {
                Assert.Equal((byte)(i % 17), u8Read.GetValue(i));
                Assert.Equal((uint)(i % 1000), u32sRead.GetValue(i));
                Assert.Equal((ulong)(i % 7), u64Read.GetValue(i));
                Assert.Equal((uint)(i * 1_000_003), u32fRead.GetValue(i));
            }
        }
        finally
        {
            try { File.Delete(compressedPath); } catch { }
            try { File.Delete(rawPath); } catch { }
        }
    }

    [Fact]
    public async Task SumStat_PrimitiveColumns()
    {
        // Three columns: signed Int32, unsigned UInt64, Float64. All non-nullable.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("i32", Int32Type.Default, nullable: false),
            new Field("u64", UInt64Type.Default, nullable: false),
            new Field("f64", DoubleType.Default, nullable: false),
        }, metadata: null);

        var i32B = new Int32Array.Builder();
        var u64B = new UInt64Array.Builder();
        var f64B = new DoubleArray.Builder();
        var i32Values = new int[] { 10, -5, 100, -50, 7 };           // sum = 62
        var u64Values = new ulong[] { 1UL, 2UL, 3UL, 1_000_000UL };   // sum = 1_000_006
        var f64Values = new double[] { 0.5, 1.25, double.NaN, -2.0 }; // sum (NaN skipped) = -0.25
        foreach (var v in i32Values) i32B.Append(v);
        foreach (var v in u64Values) u64B.Append(v);
        foreach (var v in f64Values) f64B.Append(v);

        // Pad shorter columns to match — n is the longest.
        int n = Math.Max(Math.Max(i32Values.Length, u64Values.Length), f64Values.Length);
        while (i32B.Length < n) i32B.Append(0);
        while (u64B.Length < n) u64B.Append(0UL);
        while (f64B.Length < n) f64B.Append(0.0);
        long expectedI32Sum = 0; foreach (var v in i32Values) expectedI32Sum += v;
        ulong expectedU64Sum = 0; foreach (var v in u64Values) expectedU64Sum += v;
        double expectedF64Sum = 0; foreach (var v in f64Values) if (!double.IsNaN(v)) expectedF64Sum += v;

        var batch = new RecordBatch(schema,
            new IArrowArray[] { i32B.Build(), u64B.Build(), f64B.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);

            // Pull SumBytes from each column.
            byte[] i32Sum = await ReadSumBytes(reader, path, 0);
            byte[] u64Sum = await ReadSumBytes(reader, path, 1);
            byte[] f64Sum = await ReadSumBytes(reader, path, 2);

            Assert.Equal((byte)0x18, i32Sum[0]); // sint64
            Assert.Equal(expectedI32Sum, DecodeScalarSignedInt(i32Sum));

            Assert.Equal((byte)0x20, u64Sum[0]); // uint64 varint
            Assert.Equal(expectedU64Sum, DecodeScalarUnsignedInt(u64Sum));

            Assert.Equal((byte)0x31, f64Sum[0]); // fixed64
            Assert.Equal(expectedF64Sum, DecodeScalarFloat64(f64Sum));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    private async Task<byte[]> ReadSumBytes(VortexFileReader reader, string path, int colIdx)
    {
        var plan = (EngineeredWood.Vortex.Layouts.FlatColumnPlan)reader.ColumnPlans[colIdx];
        var chunk = plan.Chunks[0];
        var locator = reader.SegmentSpecs[(int)chunk.SegmentRef];
        using var local = new EngineeredWood.IO.Local.LocalRandomAccessFile(path);
        using var owner = await local.ReadAsync(new EngineeredWood.IO.FileRange(
            checked((long)locator.Offset), checked((int)locator.Length)));
        var serialized = EngineeredWood.Vortex.Encodings.SerializedArray.Parse(owner.Memory.Span);
        var stats = serialized.Message.Root.Stats;
        return stats.SumBytes.Length == 0
            ? System.Array.Empty<byte>()
            : stats.SumBytes.RawBytes(stats.SumBytes.Length).ToArray();
    }

    [Fact]
    public async Task MinMaxStat_PrimitiveColumns()
    {
        // Three primitive types: Int32 (signed varint), UInt64 (varint),
        // Float64 (fixed64). Verify the protobuf-encoded ScalarValue bytes
        // decode back to the expected min/max.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("i32", Int32Type.Default, nullable: false),
            new Field("u64", UInt64Type.Default, nullable: false),
            new Field("f64", DoubleType.Default, nullable: false),
        }, metadata: null);

        var i32B = new Int32Array.Builder();
        var u64B = new UInt64Array.Builder();
        var f64B = new DoubleArray.Builder();

        // Known min/max per column. All three columns have the same length n=7;
        // f64 includes a NaN that should be skipped during min/max.
        var i32Values = new int[] { 0, 7, -42, 1_000_000, -999, 12345, 5 };
        var u64Values = new ulong[] { 100UL, 9_000_000_000UL, 1UL, 50UL, ulong.MaxValue / 2, 7UL, 200UL };
        var f64Values = new double[] { 1.5, -100.25, 0.0, 3.14, 9999.9999, double.NaN, -0.001 };
        foreach (var v in i32Values) i32B.Append(v);
        foreach (var v in u64Values) u64B.Append(v);
        foreach (var v in f64Values) f64B.Append(v);

        int n = i32Values.Length;
        var batch = new RecordBatch(schema,
            new IArrowArray[] { i32B.Build(), u64B.Build(), f64B.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);

            // Column 0: Int32 → ScalarValue tag 0x18 (sint64 zigzag varint).
            var col0Stats = await ReadStats(reader, path, 0);
            Assert.True(col0Stats.HasIsConstant);
            // (Pre-existing checks)
            // Min bytes should decode to -999, max to 1_000_000.
            Assert.True(col0Stats.MinBytes.Length > 0);
            var minI32 = DecodeScalarSignedInt(col0Stats.MinBytes);
            var maxI32 = DecodeScalarSignedInt(col0Stats.MaxBytes);
            Assert.Equal(-999L, minI32);
            Assert.Equal(1_000_000L, maxI32);
            Assert.Equal(EngineeredWood.Vortex.Format.Precision.Exact, col0Stats.MinPrecision);
            Assert.Equal(EngineeredWood.Vortex.Format.Precision.Exact, col0Stats.MaxPrecision);

            // Column 1: UInt64 → tag 0x20 (uint64 varint). Values include 0 padding;
            // 0 is the actual min, 9e9 the max.
            var col1Stats = await ReadStats(reader, path, 1);
            var minU64 = DecodeScalarUnsignedInt(col1Stats.MinBytes);
            var maxU64 = DecodeScalarUnsignedInt(col1Stats.MaxBytes);
            Assert.Equal(1UL, minU64);
            Assert.Equal(ulong.MaxValue / 2, maxU64);

            // Column 2: Float64 → tag 0x31 (fixed64 LE), 9 bytes total.
            // NaN excluded; min=-100.25, max=9999.9999.
            var col2Stats = await ReadStats(reader, path, 2);
            Assert.Equal(9, col2Stats.MinBytes.Length);
            Assert.Equal((byte)0x31, col2Stats.MinBytes[0]);
            var minF64 = DecodeScalarFloat64(col2Stats.MinBytes);
            var maxF64 = DecodeScalarFloat64(col2Stats.MaxBytes);
            Assert.Equal(-100.25, minF64);
            Assert.Equal(9999.9999, maxF64);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    private async Task<DecodedStats> ReadStats(VortexFileReader reader, string path, int colIdx)
    {
        var plan = (EngineeredWood.Vortex.Layouts.FlatColumnPlan)reader.ColumnPlans[colIdx];
        var chunk = plan.Chunks[0];
        var locator = reader.SegmentSpecs[(int)chunk.SegmentRef];
        using var local = new EngineeredWood.IO.Local.LocalRandomAccessFile(path);
        using var owner = await local.ReadAsync(new EngineeredWood.IO.FileRange(
            checked((long)locator.Offset), checked((int)locator.Length)));
        var serialized = EngineeredWood.Vortex.Encodings.SerializedArray.Parse(owner.Memory.Span);
        var stats = serialized.Message.Root.Stats;
        return new DecodedStats(
            HasIsConstant: stats.HasIsConstant,
            IsConstant: stats.HasIsConstant && stats.IsConstant,
            MinBytes: stats.MinBytes.Length == 0 ? System.Array.Empty<byte>() : stats.MinBytes.RawBytes(stats.MinBytes.Length).ToArray(),
            MaxBytes: stats.MaxBytes.Length == 0 ? System.Array.Empty<byte>() : stats.MaxBytes.RawBytes(stats.MaxBytes.Length).ToArray(),
            MinPrecision: stats.MinPrecision,
            MaxPrecision: stats.MaxPrecision);
    }

    private record struct DecodedStats(
        bool HasIsConstant, bool IsConstant, byte[] MinBytes, byte[] MaxBytes,
        EngineeredWood.Vortex.Format.Precision MinPrecision,
        EngineeredWood.Vortex.Format.Precision MaxPrecision);

    /// <summary>Decodes a vortex.scalar.ScalarValue with int64_value (field 3, sint64 zigzag).</summary>
    private static long DecodeScalarSignedInt(byte[] bytes)
    {
        Assert.Equal((byte)0x18, bytes[0]);
        int pos = 1;
        ulong raw = (ulong)EngineeredWood.Encodings.Varint.ReadUnsigned(bytes.AsSpan(), ref pos);
        // Zigzag decode (sint64).
        return (long)((raw >> 1) ^ (ulong)-(long)(raw & 1));
    }

    private static ulong DecodeScalarUnsignedInt(byte[] bytes)
    {
        Assert.Equal((byte)0x20, bytes[0]);
        int pos = 1;
        return (ulong)EngineeredWood.Encodings.Varint.ReadUnsigned(bytes.AsSpan(), ref pos);
    }

    private static double DecodeScalarFloat64(byte[] bytes)
    {
        Assert.Equal((byte)0x31, bytes[0]);
        return BitConverter.Int64BitsToDouble(
            System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(1, 8)));
    }

    [Fact]
    public async Task IsSortedStat_DetectsOrderingForIntegers()
    {
        // Three columns: strictly increasing, non-strictly increasing (with
        // duplicates), and unsorted. All non-nullable Int32.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("strict", Int32Type.Default, nullable: false),
            new Field("nondecreasing", Int32Type.Default, nullable: false),
            new Field("unsorted", Int32Type.Default, nullable: false),
        }, metadata: null);

        const int n = 30;
        var sB = new Int32Array.Builder();
        var ndB = new Int32Array.Builder();
        var unB = new Int32Array.Builder();
        for (int i = 0; i < n; i++)
        {
            sB.Append(i);                         // 0,1,2,…,29
            ndB.Append(i / 2);                    // 0,0,1,1,2,2,…
            unB.Append(i == 15 ? -1 : i);         // dip in the middle
        }
        var batch = new RecordBatch(schema,
            new IArrowArray[] { sB.Build(), ndB.Build(), unB.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);

            var expected = new (bool Sorted, bool StrictSorted)[]
            {
                (true, true),
                (true, false),
                (false, false),
            };
            for (int colIdx = 0; colIdx < 3; colIdx++)
            {
                var plan = (EngineeredWood.Vortex.Layouts.FlatColumnPlan)reader.ColumnPlans[colIdx];
                var chunk = plan.Chunks[0];
                var locator = reader.SegmentSpecs[(int)chunk.SegmentRef];
                using var local = new EngineeredWood.IO.Local.LocalRandomAccessFile(path);
                using var owner = await local.ReadAsync(new EngineeredWood.IO.FileRange(
                    checked((long)locator.Offset), checked((int)locator.Length)));
                var serialized = EngineeredWood.Vortex.Encodings.SerializedArray.Parse(owner.Memory.Span);
                var stats = serialized.Message.Root.Stats;

                Assert.True(stats.HasIsSorted);
                Assert.True(stats.HasIsStrictSorted);
                Assert.Equal(expected[colIdx].Sorted, stats.IsSorted);
                Assert.Equal(expected[colIdx].StrictSorted, stats.IsStrictSorted);
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task NanCountStat_CountsFloatNaNs()
    {
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("nonan", DoubleType.Default, nullable: false),
            new Field("withnans", DoubleType.Default, nullable: false),
        }, metadata: null);

        const int n = 40;
        var noB = new DoubleArray.Builder();
        var nanB = new DoubleArray.Builder();
        int expectedNanCount = 0;
        for (int i = 0; i < n; i++)
        {
            noB.Append(i + 0.5);
            if (i % 6 == 3) { nanB.Append(double.NaN); expectedNanCount++; }
            else nanB.Append(i * 0.1);
        }
        var batch = new RecordBatch(schema,
            new IArrowArray[] { noB.Build(), nanB.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);

            for (int colIdx = 0; colIdx < 2; colIdx++)
            {
                var plan = (EngineeredWood.Vortex.Layouts.FlatColumnPlan)reader.ColumnPlans[colIdx];
                var chunk = plan.Chunks[0];
                var locator = reader.SegmentSpecs[(int)chunk.SegmentRef];
                using var local = new EngineeredWood.IO.Local.LocalRandomAccessFile(path);
                using var owner = await local.ReadAsync(new EngineeredWood.IO.FileRange(
                    checked((long)locator.Offset), checked((int)locator.Length)));
                var serialized = EngineeredWood.Vortex.Encodings.SerializedArray.Parse(owner.Memory.Span);
                var stats = serialized.Message.Root.Stats;

                Assert.True(stats.HasNanCount);
                ulong expected = colIdx == 0 ? 0UL : (ulong)expectedNanCount;
                Assert.Equal(expected, stats.NanCount);
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task IsConstantStat_DetectsConstantPrimitiveColumn()
    {
        // Two columns: one with all rows = 42 (is_constant=true), one with
        // varying rows (is_constant=false). Both non-nullable Int64.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("constant", Int64Type.Default, nullable: false),
            new Field("varying", Int64Type.Default, nullable: false),
        }, metadata: null);

        const int n = 50;
        var cB = new Int64Array.Builder();
        var vB = new Int64Array.Builder();
        for (int i = 0; i < n; i++)
        {
            cB.Append(42L);
            vB.Append(i);
        }
        var batch = new RecordBatch(schema,
            new IArrowArray[] { cB.Build(), vB.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);

            for (int colIdx = 0; colIdx < 2; colIdx++)
            {
                var plan = (EngineeredWood.Vortex.Layouts.FlatColumnPlan)reader.ColumnPlans[colIdx];
                var chunk = plan.Chunks[0];
                var locator = reader.SegmentSpecs[(int)chunk.SegmentRef];
                using var local = new EngineeredWood.IO.Local.LocalRandomAccessFile(path);
                using var owner = await local.ReadAsync(new EngineeredWood.IO.FileRange(
                    checked((long)locator.Offset), checked((int)locator.Length)));
                var serialized = EngineeredWood.Vortex.Encodings.SerializedArray.Parse(owner.Memory.Span);
                var stats = serialized.Message.Root.Stats;

                Assert.True(stats.IsPresent);
                Assert.True(stats.HasIsConstant);
                Assert.Equal(colIdx == 0, stats.IsConstant);
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task SelfRoundtrip_ListColumns()
    {
        // List<i32> nullable + non-nullable; List<string> non-nullable.
        const int n = 30;
        var listInt = new ListArray.Builder(Int32Type.Default);
        var listIntNullable = new ListArray.Builder(Int32Type.Default);
        var listStr = new ListArray.Builder(StringType.Default);
        var innerInt1 = (Int32Array.Builder)listInt.ValueBuilder;
        var innerInt2 = (Int32Array.Builder)listIntNullable.ValueBuilder;
        var innerStr = (StringArray.Builder)listStr.ValueBuilder;
        for (int i = 0; i < n; i++)
        {
            // listInt: row i has (i % 5) + 1 elements, values [i*10, i*10+1, ...].
            int len = (i % 5) + 1;
            listInt.Append();
            for (int j = 0; j < len; j++) innerInt1.Append(i * 10 + j);

            // listIntNullable: every 4th row null.
            if (i % 4 == 0)
                listIntNullable.AppendNull();
            else
            {
                listIntNullable.Append();
                for (int j = 0; j < (i % 3); j++) innerInt2.Append(i * 100 + j);
            }

            // listStr: i % 4 strings per row.
            int slen = i % 4;
            listStr.Append();
            for (int j = 0; j < slen; j++) innerStr.Append($"r{i}.{j}");
        }
        var listIntArr = listInt.Build();
        var listIntNullableArr = listIntNullable.Build();
        var listStrArr = listStr.Build();

        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("a", new ListType(Int32Type.Default), nullable: false),
            new Field("b", new ListType(Int32Type.Default), nullable: true),
            new Field("c", new ListType(StringType.Default), nullable: false),
        }, metadata: null);
        var batch = new RecordBatch(schema,
            new IArrowArray[] { listIntArr, listIntNullableArr, listStrArr }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var aRead = Assert.IsType<ListArray>(await reader.ReadColumnAsync(0));
            var bRead = Assert.IsType<ListArray>(await reader.ReadColumnAsync(1));
            var cRead = Assert.IsType<ListArray>(await reader.ReadColumnAsync(2));

            Assert.Equal(n, aRead.Length);
            Assert.Equal(n, bRead.Length);
            Assert.Equal(n, cRead.Length);

            // Verify a (non-nullable list of i32).
            var aValues = (Int32Array)aRead.Values;
            for (int i = 0; i < n; i++)
            {
                int expectedLen = (i % 5) + 1;
                int sliceStart = aRead.ValueOffsets[i];
                int sliceEnd = aRead.ValueOffsets[i + 1];
                Assert.Equal(expectedLen, sliceEnd - sliceStart);
                for (int j = 0; j < expectedLen; j++)
                    Assert.Equal(i * 10 + j, aValues.GetValue(sliceStart + j));
            }

            // Verify b (nullable list, every 4th null).
            var bValues = (Int32Array)bRead.Values;
            for (int i = 0; i < n; i++)
            {
                if (i % 4 == 0)
                    Assert.False(bRead.IsValid(i));
                else
                {
                    Assert.True(bRead.IsValid(i));
                    int expectedLen = i % 3;
                    int s = bRead.ValueOffsets[i];
                    int e = bRead.ValueOffsets[i + 1];
                    Assert.Equal(expectedLen, e - s);
                    for (int j = 0; j < expectedLen; j++)
                        Assert.Equal(i * 100 + j, bValues.GetValue(s + j));
                }
            }

            // Verify c (list of strings).
            var cValues = (StringArray)cRead.Values;
            for (int i = 0; i < n; i++)
            {
                int slen = i % 4;
                int s = cRead.ValueOffsets[i];
                int e = cRead.ValueOffsets[i + 1];
                Assert.Equal(slen, e - s);
                for (int j = 0; j < slen; j++)
                    Assert.Equal($"r{i}.{j}", cValues.GetString(s + j));
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task SelfRoundtrip_FixedSizeListColumns()
    {
        const int n = 50;
        const int fslSize = 4;
        var fslB = new FixedSizeListArray.Builder(Int32Type.Default, fslSize);
        var inner = (Int32Array.Builder)fslB.ValueBuilder;
        for (int i = 0; i < n; i++)
        {
            fslB.Append();
            for (int j = 0; j < fslSize; j++) inner.Append(i * 10 + j);
        }
        var fslArr = fslB.Build();

        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", new FixedSizeListType(Int32Type.Default, fslSize), nullable: false),
        }, metadata: null);
        var batch = new RecordBatch(schema, new IArrowArray[] { fslArr }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<FixedSizeListArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            var inner2 = (Int32Array)read.Values;
            for (int i = 0; i < n; i++)
                for (int j = 0; j < fslSize; j++)
                    Assert.Equal(i * 10 + j, inner2.GetValue(i * fslSize + j));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task SelfRoundtrip_MultiBatch()
    {
        // Three batches of varying lengths streamed via the instance API.
        // Layout becomes vortex.struct(vortex.chunked(vortex.flat × 3) × cols).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("id", Int32Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: true),
        }, metadata: null);

        var sizes = new[] { 30, 100, 50 };

        IArrowArray BuildIds(int start, int n)
        {
            var b = new Int32Array.Builder();
            for (int i = 0; i < n; i++) b.Append(start + i);
            return b.Build();
        }
        IArrowArray BuildNames(int start, int n)
        {
            var b = new StringArray.Builder();
            for (int i = 0; i < n; i++)
            {
                if ((start + i) % 4 == 0) b.AppendNull();
                else b.Append($"name-{start + i}");
            }
            return b.Build();
        }

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema))
            {
                int rowsSoFar = 0;
                foreach (var sz in sizes)
                {
                    var batch = new RecordBatch(schema,
                        new IArrowArray[] { BuildIds(rowsSoFar, sz), BuildNames(rowsSoFar, sz) },
                        sz);
                    w.WriteBatch(batch);
                    rowsSoFar += sz;
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            int totalRows = sizes.Sum();
            Assert.Equal((ulong)totalRows, reader.ColumnPlans[0].TotalRows);
            Assert.Equal(sizes.Length, reader.ColumnPlans[0].ChunkCount);

            int batchIdx = 0;
            int rowsAcrossBatches = 0;
            await foreach (var batch in reader.ReadAllAsync())
            {
                Assert.Equal(sizes[batchIdx], batch.Length);
                var ids = Assert.IsType<Int32Array>(batch.Column(0));
                var names = Assert.IsType<StringArray>(batch.Column(1));
                for (int i = 0; i < batch.Length; i++)
                {
                    int orig = rowsAcrossBatches + i;
                    Assert.Equal(orig, ids.GetValue(i));
                    if (orig % 4 == 0) Assert.False(names.IsValid(i));
                    else
                    {
                        Assert.True(names.IsValid(i));
                        Assert.Equal($"name-{orig}", names.GetString(i));
                    }
                }
                rowsAcrossBatches += batch.Length;
                batchIdx++;
            }
            Assert.Equal(sizes.Length, batchIdx);
            Assert.Equal(totalRows, rowsAcrossBatches);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task SelfRoundtrip_SlicedInputs()
    {
        // Build full-length arrays, then slice each by [20, 80) before writing.
        // The encoders must handle Data.Offset != 0 by slicing value buffers,
        // bit-extracting the validity bitmap, and rebasing varbin offsets.
        const int fullN = 100;
        const int sliceStart = 20;
        const int sliceLen = 60;

        var i32B = new Int32Array.Builder();
        var f64B = new DoubleArray.Builder();
        var nullB = new Int32Array.Builder();
        var strB = new StringArray.Builder();
        for (int i = 0; i < fullN; i++)
        {
            i32B.Append(i * 3);
            f64B.Append(i + 0.5);
            if (i % 7 == 0) nullB.AppendNull();
            else nullB.Append(i * 11);
            strB.Append($"item-{i:D3}-{string.Concat(System.Linq.Enumerable.Repeat("x", i % 5))}");
        }
        var i32Full = i32B.Build();
        var f64Full = f64B.Build();
        var nullFull = nullB.Build();
        var strFull = strB.Build();

        var i32Slice = (Int32Array)i32Full.Slice(sliceStart, sliceLen);
        var f64Slice = (DoubleArray)f64Full.Slice(sliceStart, sliceLen);
        var nullSlice = (Int32Array)nullFull.Slice(sliceStart, sliceLen);
        var strSlice = (StringArray)strFull.Slice(sliceStart, sliceLen);
        Assert.Equal(sliceStart, i32Slice.Offset);
        Assert.Equal(sliceLen, i32Slice.Length);

        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("i32", Int32Type.Default, nullable: false),
            new Field("f64", DoubleType.Default, nullable: false),
            new Field("nullable_i32", Int32Type.Default, nullable: true),
            new Field("s", StringType.Default, nullable: false),
        }, metadata: null);

        var batch = new RecordBatch(schema,
            new IArrowArray[] { i32Slice, f64Slice, nullSlice, strSlice }, sliceLen);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var i32Read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            var f64Read = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(1));
            var nullRead = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(2));
            var strRead = Assert.IsType<StringArray>(await reader.ReadColumnAsync(3));

            Assert.Equal(sliceLen, i32Read.Length);
            for (int i = 0; i < sliceLen; i++)
            {
                int orig = sliceStart + i;
                Assert.Equal(orig * 3, i32Read.GetValue(i));
                Assert.Equal(orig + 0.5, f64Read.GetValue(i));
                if (orig % 7 == 0)
                    Assert.False(nullRead.IsValid(i));
                else
                {
                    Assert.True(nullRead.IsValid(i));
                    Assert.Equal(orig * 11, nullRead.GetValue(i));
                }
                var expected = $"item-{orig:D3}-{string.Concat(System.Linq.Enumerable.Repeat("x", orig % 5))}";
                Assert.Equal(expected, strRead.GetString(i));
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task SelfRoundtrip_StringColumns()
    {
        // Mix: non-nullable strings, nullable strings (some nulls), non-nullable binary.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("name", StringType.Default, nullable: false),
            new Field("desc", StringType.Default, nullable: true),
            new Field("blob", BinaryType.Default, nullable: false),
        }, metadata: null);

        const int n = 50;
        var nameB = new StringArray.Builder();
        var descB = new StringArray.Builder();
        var blobB = new BinaryArray.Builder();
        for (int i = 0; i < n; i++)
        {
            nameB.Append($"row_{i:D3}");
            if (i % 5 == 0) descB.AppendNull();
            else descB.Append(string.Concat(System.Linq.Enumerable.Repeat($"abc{i}", i % 4 + 1)));
            blobB.Append(new byte[] { (byte)i, (byte)(i * 2), (byte)(i * 3) });
        }
        var name = nameB.Build();
        var desc = descB.Build();
        var blob = blobB.Build();
        Assert.Equal(0, name.NullCount);
        Assert.Equal(10, desc.NullCount);
        Assert.Equal(0, blob.NullCount);

        var batch = new RecordBatch(schema, new IArrowArray[] { name, desc, blob }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            Assert.IsType<StringType>(reader.Schema.FieldsList[0].DataType);
            Assert.IsType<StringType>(reader.Schema.FieldsList[1].DataType);
            Assert.IsType<BinaryType>(reader.Schema.FieldsList[2].DataType);

            var nameRead = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            var descRead = Assert.IsType<StringArray>(await reader.ReadColumnAsync(1));
            var blobRead = Assert.IsType<BinaryArray>(await reader.ReadColumnAsync(2));

            Assert.Equal(n, nameRead.Length);
            Assert.Equal(0, nameRead.NullCount);
            Assert.Equal(10, descRead.NullCount);
            Assert.Equal(0, blobRead.NullCount);

            for (int i = 0; i < n; i++)
            {
                Assert.Equal($"row_{i:D3}", nameRead.GetString(i));
                if (i % 5 == 0) Assert.False(descRead.IsValid(i));
                else
                {
                    Assert.True(descRead.IsValid(i));
                    var expected = string.Concat(System.Linq.Enumerable.Repeat($"abc{i}", i % 4 + 1));
                    Assert.Equal(expected, descRead.GetString(i));
                }
                Assert.Equal(new byte[] { (byte)i, (byte)(i * 2), (byte)(i * 3) }, blobRead.GetBytes(i).ToArray());
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task SelfRoundtrip_NullableColumns()
    {
        // Mix of nullable and non-nullable; sprinkle nulls every 4th row.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("plain", Int32Type.Default, nullable: false),
            new Field("nullable_i32", Int32Type.Default, nullable: true),
            new Field("nullable_f64", DoubleType.Default, nullable: true),
        }, metadata: null);

        const int n = 200;
        var plainB = new Int32Array.Builder();
        var i32B = new Int32Array.Builder();
        var f64B = new DoubleArray.Builder();
        for (int i = 0; i < n; i++)
        {
            plainB.Append(i);
            if (i % 4 == 0) { i32B.AppendNull(); f64B.AppendNull(); }
            else { i32B.Append(i * 7); f64B.Append(i * 0.25); }
        }
        var plain = plainB.Build();
        var i32 = i32B.Build();
        var f64 = f64B.Build();
        Assert.Equal(0, plain.NullCount);
        Assert.Equal(50, i32.NullCount);
        Assert.Equal(50, f64.NullCount);

        var batch = new RecordBatch(schema, new IArrowArray[] { plain, i32, f64 }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);

            var plainRead = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            var i32Read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(1));
            var f64Read = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(2));

            Assert.Equal(0, plainRead.NullCount);
            Assert.Equal(50, i32Read.NullCount);
            Assert.Equal(50, f64Read.NullCount);

            for (int i = 0; i < n; i++)
            {
                Assert.Equal(i, plainRead.GetValue(i));
                if (i % 4 == 0)
                {
                    Assert.False(i32Read.IsValid(i));
                    Assert.False(f64Read.IsValid(i));
                }
                else
                {
                    Assert.True(i32Read.IsValid(i));
                    Assert.Equal(i * 7, i32Read.GetValue(i));
                    Assert.True(f64Read.IsValid(i));
                    Assert.Equal(i * 0.25, f64Read.GetValue(i));
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task For_PositiveMinShiftsBits()
    {
        // Int32 column with values in [1_000_000, 1_000_500]. MaxBits over the
        // raw values is 20; over (values - min) is 9. FoR should kick in.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        const int n = 4_000;
        var b = new Int32Array.Builder();
        for (int i = 0; i < n; i++) b.Append(1_000_000 + (i % 500));
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var rawPath = Path.GetTempFileName();
        var compressedPath = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(rawPath))
                VortexFileWriter.Write(fs, batch);
            using (var fs = File.Create(compressedPath))
                VortexFileWriter.Write(fs, batch, compress: true);

            long rawSize = new FileInfo(rawPath).Length;
            long compressedSize = new FileInfo(compressedPath).Length;
            Assert.True(compressedSize < rawSize / 2,
                $"FoR should give >2x compression for narrow-range data with high min. raw={rawSize}, compressed={compressedSize}.");

            await using var reader = await VortexFileReader.OpenAsync(compressedPath);
            var read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
                Assert.Equal(1_000_000 + (i % 500), read.GetValue(i));
        }
        finally
        {
            try { File.Delete(rawPath); } catch { }
            try { File.Delete(compressedPath); } catch { }
        }
    }

    [Fact]
    public async Task For_NegativeValuesViaForResiduals()
    {
        // Int64 column with negative values — bitpacked alone wouldn't apply
        // (rejects signed-with-negatives), but FoR shifts to make residuals
        // non-negative and then bitpacks the small residuals.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int64Type.Default, nullable: false),
        }, metadata: null);
        const int n = 2_000;
        var b = new Int64Array.Builder();
        for (int i = 0; i < n; i++) b.Append(-50_000L + (i % 100));
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var rawPath = Path.GetTempFileName();
        var compressedPath = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(rawPath))
                VortexFileWriter.Write(fs, batch);
            using (var fs = File.Create(compressedPath))
                VortexFileWriter.Write(fs, batch, compress: true);

            long rawSize = new FileInfo(rawPath).Length;
            long compressedSize = new FileInfo(compressedPath).Length;
            Assert.True(compressedSize < rawSize / 4,
                $"FoR over Int64 with -50000 min and 100-wide range should compress strongly. raw={rawSize}, compressed={compressedSize}.");

            await using var reader = await VortexFileReader.OpenAsync(compressedPath);
            var read = Assert.IsType<Int64Array>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++)
                Assert.Equal(-50_000L + (i % 100), read.GetValue(i));
        }
        finally
        {
            try { File.Delete(rawPath); } catch { }
            try { File.Delete(compressedPath); } catch { }
        }
    }

    [Fact]
    public async Task For_NullablePositiveMinRoundtrips()
    {
        // Nullable Int32 column with values in [10_000, 10_500] and ~25% nulls.
        // FoR shifts to make residuals fit in 9 bits; bitpacked child carries a
        // validity bitmap. Reader must restore both values and null-ness.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: true),
        }, metadata: null);
        const int n = 2_000;
        var b = new Int32Array.Builder();
        for (int i = 0; i < n; i++)
        {
            if (i % 4 == 0) b.AppendNull();
            else b.Append(10_000 + (i % 500));
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var rawPath = Path.GetTempFileName();
        var compressedPath = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(rawPath))
                VortexFileWriter.Write(fs, batch);
            using (var fs = File.Create(compressedPath))
                VortexFileWriter.Write(fs, batch, compress: true);

            long rawSize = new FileInfo(rawPath).Length;
            long compressedSize = new FileInfo(compressedPath).Length;
            Assert.True(compressedSize < rawSize,
                $"Nullable FoR should compress over plain primitive. raw={rawSize}, compressed={compressedSize}.");

            await using var reader = await VortexFileReader.OpenAsync(compressedPath);
            var read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            int expectedNulls = 0; for (int i = 0; i < n; i++) if (i % 4 == 0) expectedNulls++;
            Assert.Equal(expectedNulls, read.NullCount);
            for (int i = 0; i < n; i++)
            {
                if (i % 4 == 0)
                    Assert.False(read.IsValid(i));
                else
                {
                    Assert.True(read.IsValid(i));
                    Assert.Equal(10_000 + (i % 500), read.GetValue(i));
                }
            }
        }
        finally
        {
            try { File.Delete(rawPath); } catch { }
            try { File.Delete(compressedPath); } catch { }
        }
    }

    [Fact]
    public async Task For_NullableNegativeValuesRoundtrips()
    {
        // Nullable Int64 with all NEGATIVE non-null values. Plain bitpacked
        // alone rejects negatives; FoR shifts by min (most negative) to make
        // residuals non-negative, then nullable bitpacked handles the rest.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int64Type.Default, nullable: true),
        }, metadata: null);
        const int n = 1_000;
        var b = new Int64Array.Builder();
        for (int i = 0; i < n; i++)
        {
            if (i % 7 == 0) b.AppendNull();
            else b.Append(-1_000_000L + (i % 200)); // all non-null are negative
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int64Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
            {
                if (i % 7 == 0)
                    Assert.False(read.IsValid(i));
                else
                {
                    Assert.True(read.IsValid(i));
                    Assert.Equal(-1_000_000L + (i % 200), read.GetValue(i));
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task For_AllNullColumnFallsThrough()
    {
        // All-null column — FoR has no min to subtract. Dispatch should reject
        // FoR and let plain bitpacked (or primitive) handle it. Round-trip must
        // preserve the all-null state.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: true),
        }, metadata: null);
        const int n = 100;
        var b = new Int32Array.Builder();
        for (int i = 0; i < n; i++) b.AppendNull();
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            Assert.Equal(n, read.NullCount);
            for (int i = 0; i < n; i++) Assert.False(read.IsValid(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task For_FallsThroughWhenMinIsZero()
    {
        // Min=0, all non-negative — FoR offers no advantage, dispatch should
        // skip it and fall through to plain bitpacked. (We can't directly
        // observe which encoding was used from the public API, but we CAN
        // confirm round-trip correctness AND that the file isn't bigger than
        // raw — i.e., compress: true didn't accidentally pessimize.)
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", UInt32Type.Default, nullable: false),
        }, metadata: null);
        const int n = 1_000;
        var b = new UInt32Array.Builder();
        for (int i = 0; i < n; i++) b.Append((uint)(i % 200)); // min=0, range 0..199
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<UInt32Array>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++)
                Assert.Equal((uint)(i % 200), read.GetValue(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Constant_PrimitiveRoundtrips()
    {
        // Three columns where ALL values are identical — should compress to
        // vortex.constant, dropping the file size dramatically.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("i32", Int32Type.Default, nullable: false),
            new Field("u64", UInt64Type.Default, nullable: false),
            new Field("f64", DoubleType.Default, nullable: false),
        }, metadata: null);
        const int n = 5_000;
        var i32 = new Int32Array.Builder();
        var u64 = new UInt64Array.Builder();
        var f64 = new DoubleArray.Builder();
        for (int i = 0; i < n; i++)
        {
            i32.Append(-42);
            u64.Append(0xCAFEBABE_DEADBEEFUL);
            f64.Append(3.14159);
        }
        var batch = new RecordBatch(schema,
            new IArrowArray[] { i32.Build(), u64.Build(), f64.Build() }, n);

        var rawPath = Path.GetTempFileName();
        var compressedPath = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(rawPath))
                VortexFileWriter.Write(fs, batch);
            using (var fs = File.Create(compressedPath))
                VortexFileWriter.Write(fs, batch, compress: true);

            long rawSize = new FileInfo(rawPath).Length;
            long compressedSize = new FileInfo(compressedPath).Length;
            Assert.True(compressedSize < rawSize / 10,
                $"Constant compression should yield <1/10 of raw size; raw={rawSize}, compressed={compressedSize}.");

            await using var reader = await VortexFileReader.OpenAsync(compressedPath);
            var i32Read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            var u64Read = Assert.IsType<UInt64Array>(await reader.ReadColumnAsync(1));
            var f64Read = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(2));
            Assert.Equal(n, i32Read.Length);
            for (int i = 0; i < n; i++)
            {
                Assert.Equal(-42, i32Read.GetValue(i));
                Assert.Equal(0xCAFEBABE_DEADBEEFUL, u64Read.GetValue(i));
                Assert.Equal(3.14159, f64Read.GetValue(i));
            }
        }
        finally
        {
            try { File.Delete(rawPath); } catch { }
            try { File.Delete(compressedPath); } catch { }
        }
    }

    [Fact]
    public async Task Constant_BoolRoundtrips()
    {
        // Bool columns where every row is the same.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("all_true", BooleanType.Default, nullable: false),
            new Field("all_false", BooleanType.Default, nullable: false),
        }, metadata: null);
        const int n = 1000;
        var t = new BooleanArray.Builder();
        var f = new BooleanArray.Builder();
        for (int i = 0; i < n; i++) { t.Append(true); f.Append(false); }

        var batch = new RecordBatch(schema, new IArrowArray[] { t.Build(), f.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var tRead = Assert.IsType<BooleanArray>(await reader.ReadColumnAsync(0));
            var fRead = Assert.IsType<BooleanArray>(await reader.ReadColumnAsync(1));
            for (int i = 0; i < n; i++)
            {
                Assert.True(tRead.GetValue(i)!.Value);
                Assert.False(fRead.GetValue(i)!.Value);
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Constant_FallsBackForNonConstant()
    {
        // Non-constant column with compress=true should fall through to plain
        // primitive (or bitpacked, doesn't matter — what matters is that values
        // round-trip even when constant detection rejects.)
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("varying", Int32Type.Default, nullable: false),
        }, metadata: null);
        const int n = 100;
        var b = new Int32Array.Builder();
        for (int i = 0; i < n; i++) b.Append(i * 7 + 3);
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++)
                Assert.Equal(i * 7 + 3, read.GetValue(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task SelfRoundtrip_Decimal128_NonNullable()
    {
        var dec = new Decimal128Type(18, 4);
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("amount", dec, nullable: false),
        }, metadata: null);

        var b = new Decimal128Array.Builder(dec);
        decimal[] values = {
            0m,
            1.0001m,
            -1.0001m,
            12345.6789m,
            -98765.4321m,
            99999999999999.9999m,
            -99999999999999.9999m,
        };
        foreach (var v in values) b.Append(v);
        var arr = b.Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, values.Length);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var field = reader.Schema.FieldsList[0];
            var rdec = Assert.IsType<Decimal128Type>(field.DataType);
            Assert.Equal(18, rdec.Precision);
            Assert.Equal(4, rdec.Scale);
            Assert.False(field.IsNullable);

            var read = Assert.IsType<Decimal128Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(values.Length, read.Length);
            Assert.Equal(0, read.NullCount);
            for (int i = 0; i < values.Length; i++)
                Assert.Equal(values[i], read.GetValue(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task SelfRoundtrip_Decimal128_Nullable()
    {
        var dec = new Decimal128Type(20, 6);
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("price", dec, nullable: true),
        }, metadata: null);

        const int n = 100;
        var b = new Decimal128Array.Builder(dec);
        for (int i = 0; i < n; i++)
        {
            if (i % 5 == 0) b.AppendNull();
            else if (i % 3 == 0) b.Append(-(decimal)i * 1.234567m);
            else b.Append((decimal)i * 1.234567m);
        }
        var arr = b.Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Decimal128Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            Assert.Equal(20, read.NullCount);
            for (int i = 0; i < n; i++)
            {
                if (i % 5 == 0)
                    Assert.False(read.IsValid(i));
                else
                {
                    Assert.True(read.IsValid(i));
                    decimal expected = i % 3 == 0 ? -(decimal)i * 1.234567m : (decimal)i * 1.234567m;
                    Assert.Equal(expected, read.GetValue(i));
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task SelfRoundtrip_Decimal256_NonNullable()
    {
        // Precision 50 forces the schema converter to pick Decimal256Type and
        // exercises the 32-byte storage path (values_type = I256).
        var dec = new Decimal256Type(50, 10);
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("big", dec, nullable: false),
        }, metadata: null);

        // Decimal256Array.Builder accepts decimal too (clamped to 128 bits) —
        // good enough for self-roundtrip; the writer copies all 32 bytes out
        // either way.
        var b = new Decimal256Array.Builder(dec);
        decimal[] values = {
            0m, 1m, -1m, 1234567890.1234567890m, -1234567890.1234567890m,
        };
        foreach (var v in values) b.Append(v);
        var arr = b.Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, values.Length);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var field = reader.Schema.FieldsList[0];
            var rdec = Assert.IsType<Decimal256Type>(field.DataType);
            Assert.Equal(50, rdec.Precision);
            Assert.Equal(10, rdec.Scale);

            var read = Assert.IsType<Decimal256Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(values.Length, read.Length);
            Assert.Equal(0, read.NullCount);
            for (int i = 0; i < values.Length; i++)
                Assert.Equal(values[i], read.GetValue(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task SelfRoundtrip_Decimal128_Sliced()
    {
        // Slice an Arrow array to confirm data.Offset is honored.
        var dec = new Decimal128Type(15, 2);
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", dec, nullable: true),
        }, metadata: null);

        var b = new Decimal128Array.Builder(dec);
        for (int i = 0; i < 50; i++)
        {
            if (i % 7 == 0) b.AppendNull();
            else b.Append(((decimal)i - 25m) * 0.5m);
        }
        var full = b.Build();
        var sliced = (Decimal128Array)full.Slice(10, 30);
        Assert.Equal(10, sliced.Offset);
        Assert.Equal(30, sliced.Length);

        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, 30);
        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Decimal128Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(30, read.Length);
            for (int i = 0; i < 30; i++)
            {
                int srcRow = 10 + i;
                if (srcRow % 7 == 0)
                    Assert.False(read.IsValid(i));
                else
                {
                    Assert.True(read.IsValid(i));
                    Assert.Equal(((decimal)srcRow - 25m) * 0.5m, read.GetValue(i));
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }
}

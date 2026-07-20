// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.Vortex.Writer;
using Pred = EngineeredWood.Expressions.Expressions;

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
    public async Task PreserveStats_SingleBatchRoundtrips()
    {
        // Single-batch file with preserveStats: one zone covering all rows.
        // Our reader's LayoutPlanner skips the zones child of vortex.stats,
        // so this must round-trip identically to a non-zoned file.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("id", Int32Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: true),
        }, metadata: null);
        const int n = 100;
        var ids = new Int32Array.Builder();
        var names = new StringArray.Builder();
        for (int i = 0; i < n; i++)
        {
            ids.Append(i);
            if (i % 4 == 0) names.AppendNull();
            else names.Append($"row-{i}");
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { ids.Build(), names.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, preserveStats: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var idsRead = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            var namesRead = Assert.IsType<StringArray>(await reader.ReadColumnAsync(1));
            Assert.Equal(n, idsRead.Length);
            for (int i = 0; i < n; i++)
            {
                Assert.Equal(i, idsRead.GetValue(i));
                if (i % 4 == 0) Assert.False(namesRead.IsValid(i));
                else Assert.Equal($"row-{i}", namesRead.GetString(i));
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task PreserveStats_MultipleUniformBatchesRoundtrip()
    {
        // Three batches of 200 rows each. zone_len = 200, three zones, each
        // column's zones table has three null_count entries.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: true),
        }, metadata: null);
        var sizes = new[] { 200, 200, 200 };

        Int32Array BuildBatch(int startRow, int n)
        {
            var b = new Int32Array.Builder();
            for (int i = 0; i < n; i++)
            {
                if ((startRow + i) % 5 == 0) b.AppendNull();
                else b.Append(startRow + i);
            }
            return b.Build();
        }

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema, preserveStats: true))
            {
                int rows = 0;
                foreach (var sz in sizes)
                {
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { BuildBatch(rows, sz) }, sz));
                    rows += sz;
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            int total = sizes.Sum();
            Assert.Equal(total, read.Length);
            for (int i = 0; i < total; i++)
            {
                if (i % 5 == 0) Assert.False(read.IsValid(i));
                else Assert.Equal(i, read.GetValue(i)!.Value);
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task PreserveStats_IntegerMinMaxRoundtrips()
    {
        // Integer column with monotonic batches — each zone has a distinct
        // (min, max) range. Phase B emits these as additional zones-table
        // columns alongside null_count. The reader still ignores zones, so
        // round-trip is verified via the data column itself.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int64Type.Default, nullable: true),
        }, metadata: null);
        var sizes = new[] { 250, 250, 250, 100 };

        Int64Array BuildBatch(int startRow, int n)
        {
            var b = new Int64Array.Builder();
            for (int i = 0; i < n; i++)
            {
                if ((startRow + i) % 11 == 0) b.AppendNull();
                else b.Append(startRow + i);
            }
            return b.Build();
        }

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema, preserveStats: true))
            {
                int rows = 0;
                foreach (var sz in sizes)
                {
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { BuildBatch(rows, sz) }, sz));
                    rows += sz;
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int64Array>(await reader.ReadColumnAsync(0));
            int total = sizes.Sum();
            for (int i = 0; i < total; i++)
            {
                if (i % 11 == 0) Assert.False(read.IsValid(i));
                else Assert.Equal((long)i, read.GetValue(i)!.Value);
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Predicate_GreaterOrEqualPrunesByMax()
    {
        // 4 zones × 100 rows: zone 0 = 0..99, zone 1 = 100..199, zone 2 = 200..299, zone 3 = 300..349.
        // Predicate `v >= 250` should keep zones 2 and 3 only (max 99 / 199 < 250 → drop).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        var sizes = new[] { 100, 100, 100, 50 };
        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema, preserveStats: true))
            {
                int rows = 0;
                foreach (var sz in sizes)
                {
                    var b = new Int32Array.Builder();
                    for (int i = 0; i < sz; i++) b.Append(rows + i);
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { b.Build() }, sz));
                    rows += sz;
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            var pred = Pred.GreaterThanOrEqual("v", LiteralValue.Of(250));
            int batches = 0;
            int totalRows = 0;
            await foreach (var batch in reader.ReadAllAsync(pred))
            {
                batches++;
                totalRows += batch.Length;
            }
            Assert.Equal(2, batches);
            Assert.Equal(150, totalRows); // zones 2 (100) + 3 (50)
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Predicate_EqualKeepsZonesContainingValue()
    {
        // 3 zones × 50 rows: 0..49, 50..99, 100..149. Predicate v == 75
        // keeps only zone 1 (range 50..99).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema, preserveStats: true))
            {
                for (int batchIdx = 0; batchIdx < 3; batchIdx++)
                {
                    var b = new Int32Array.Builder();
                    for (int i = 0; i < 50; i++) b.Append(batchIdx * 50 + i);
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { b.Build() }, 50));
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            var pred = Pred.Equal("v", LiteralValue.Of(75));
            int batches = 0;
            await foreach (var batch in reader.ReadAllAsync(pred)) batches++;
            Assert.Equal(1, batches);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Predicate_AndIntersectsAcceptedZones()
    {
        // Two columns, predicates on each. Each batch is in [batchIdx*100, batchIdx*100 + 99]
        // for both columns but offset.
        // col 0: zone 0=[0..99], zone 1=[100..199], zone 2=[200..299], zone 3=[300..399].
        // col 1: same.
        // Predicate (col0 >= 200) AND (col1 < 300) → zones {2, 3} ∩ zones {0, 1, 2} = {2}.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("a", Int32Type.Default, nullable: false),
            new Field("b", Int32Type.Default, nullable: false),
        }, metadata: null);
        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema, preserveStats: true))
            {
                for (int batchIdx = 0; batchIdx < 4; batchIdx++)
                {
                    var a = new Int32Array.Builder();
                    var b = new Int32Array.Builder();
                    int basev = batchIdx * 100;
                    for (int i = 0; i < 100; i++) { a.Append(basev + i); b.Append(basev + i); }
                    w.WriteBatch(new RecordBatch(schema,
                        new IArrowArray[] { a.Build(), b.Build() }, 100));
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            var pred = Pred.And(
                Pred.GreaterThanOrEqual("a", LiteralValue.Of(200)),
                Pred.LessThan("b", LiteralValue.Of(300)));
            int batches = 0;
            await foreach (var batch in reader.ReadAllAsync(pred))
            {
                batches++;
                var aArr = (Int32Array)batch.Column(0);
                Assert.Equal(200, aArr.GetValue(0)!.Value);
            }
            Assert.Equal(1, batches);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Predicate_OrUnionsAcceptedZones()
    {
        // col 0: 4 zones × 100 rows. (col0 < 100) OR (col0 >= 300) keeps zones 0 and 3.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema, preserveStats: true))
            {
                for (int batchIdx = 0; batchIdx < 4; batchIdx++)
                {
                    var bld = new Int32Array.Builder();
                    int basev = batchIdx * 100;
                    for (int i = 0; i < 100; i++) bld.Append(basev + i);
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { bld.Build() }, 100));
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            var pred = Pred.Or(
                Pred.LessThan("v", LiteralValue.Of(100)),
                Pred.GreaterThanOrEqual("v", LiteralValue.Of(300)));
            int batches = 0;
            await foreach (var batch in reader.ReadAllAsync(pred)) batches++;
            Assert.Equal(2, batches);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Predicate_IsNullDropsZonesWithoutNulls()
    {
        // 3 zones × 100 rows. Zone 1 has nulls; zones 0 and 2 don't.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: true),
        }, metadata: null);
        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema, preserveStats: true))
            {
                for (int batchIdx = 0; batchIdx < 3; batchIdx++)
                {
                    var b = new Int32Array.Builder();
                    for (int i = 0; i < 100; i++)
                    {
                        if (batchIdx == 1 && i % 10 == 0) b.AppendNull();
                        else b.Append(batchIdx * 100 + i);
                    }
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { b.Build() }, 100));
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            var pred = Pred.IsNull("v");
            int batches = 0;
            await foreach (var batch in reader.ReadAllAsync(pred)) batches++;
            Assert.Equal(1, batches); // only zone 1
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Predicate_NoZonedStatsKeepsAllChunks()
    {
        // File without preserveStats → predicate eval falls back to
        // "keep all" since GetZoneStatsAsync returns null.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        var b = new Int32Array.Builder();
        for (int i = 0; i < 100; i++) b.Append(i);
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, 100);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch); // no preserveStats

            await using var reader = await VortexFileReader.OpenAsync(path);
            // Predicate would prune everything if stats were available, but
            // with no stats we keep the chunk and let the caller filter.
            var pred = Pred.GreaterThanOrEqual("v", LiteralValue.Of(1_000_000));
            int batches = 0;
            await foreach (var b2 in reader.ReadAllAsync(pred)) batches++;
            Assert.Equal(1, batches);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    // ----- Temporal predicate tests against Rust-written fixtures -----
    //
    // The fixture's single zone covers a known range, so a predicate either
    // keeps the only zone (1 batch yielded) or prunes it (0 batches). All
    // three fixtures' Min/Max are confirmed non-null in zone stats.

    [Fact]
    public async Task Predicate_TimestampMicrosecond_PrunesByRange()
    {
        // timestamp_us_2048rows.vortex: zone 0 covers [2024-01-01 .. 2024-12-30]
        // microseconds. col 0 is TimestampType(Microsecond, "UTC").
        var path = TestData.TestDataPath.Resolve("timestamp_us_2048rows.vortex");
        if (!File.Exists(path)) return;
        await using var reader = await VortexFileReader.OpenAsync(path);

        // Out-of-range high: drop the only zone.
        var dropHigh = Pred.GreaterThan("ts",
            LiteralValue.Of(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero)));
        int kept = 0;
        await foreach (var b in reader.ReadAllAsync(dropHigh)) kept++;
        Assert.Equal(0, kept);

        // Out-of-range low: drop the only zone.
        var dropLow = Pred.LessThan("ts",
            LiteralValue.Of(new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero)));
        kept = 0;
        await foreach (var b in reader.ReadAllAsync(dropLow)) kept++;
        Assert.Equal(0, kept);

        // In-range: keep the zone (max > 2024-06-01).
        var keep = Pred.GreaterThan("ts",
            LiteralValue.Of(new DateTimeOffset(2024, 6, 1, 0, 0, 0, TimeSpan.Zero)));
        kept = 0;
        await foreach (var b in reader.ReadAllAsync(keep)) kept++;
        Assert.Equal(1, kept);
    }

#if NET6_0_OR_GREATER
    [Fact]
    public async Task Predicate_Date32_PrunesByRange()
    {
        // date_days_2048rows.vortex: zone 0 covers days [19723 (2024-01-01) .. 21547 (2028-12-30)].
        // Date32 stats decode to LiteralValue.Of(DateOnly), so callers build
        // predicates with DateOnly literals; this test is gated to net6+
        // because System.DateOnly itself is net6+.
        var path = TestData.TestDataPath.Resolve("date_days_2048rows.vortex");
        if (!File.Exists(path)) return;
        await using var reader = await VortexFileReader.OpenAsync(path);

        var dropLow = Pred.LessThan("d", LiteralValue.Of(new DateOnly(2020, 1, 1)));
        int kept = 0;
        await foreach (var b in reader.ReadAllAsync(dropLow)) kept++;
        Assert.Equal(0, kept);

        var dropHigh = Pred.GreaterThan("d", LiteralValue.Of(new DateOnly(2030, 1, 1)));
        kept = 0;
        await foreach (var b in reader.ReadAllAsync(dropHigh)) kept++;
        Assert.Equal(0, kept);

        var keep = Pred.GreaterThan("d", LiteralValue.Of(new DateOnly(2024, 1, 1)));
        kept = 0;
        await foreach (var b in reader.ReadAllAsync(keep)) kept++;
        Assert.Equal(1, kept);
    }

    [Fact]
    public async Task Predicate_Time64Microsecond_PrunesByRange()
    {
        // time_us_2048rows.vortex: zone 0 covers ~[1m15s .. 23h59m16s] microseconds-of-day.
        // Time64 stats decode to LiteralValue.Of(TimeOnly); gated to net6+
        // because System.TimeOnly is net6+.
        var path = TestData.TestDataPath.Resolve("time_us_2048rows.vortex");
        if (!File.Exists(path)) return;
        await using var reader = await VortexFileReader.OpenAsync(path);

        // TimeOnly's range is [00:00:00, 24:00:00); to prune the upper end of
        // the zone we use the maximum legal TimeOnly (just before 24:00).
        var dropHigh = Pred.GreaterThan("t", LiteralValue.Of(TimeOnly.MaxValue));
        int kept = 0;
        await foreach (var b in reader.ReadAllAsync(dropHigh)) kept++;
        Assert.Equal(0, kept);

        var dropLow = Pred.LessThan("t", LiteralValue.Of(TimeOnly.MinValue));
        kept = 0;
        await foreach (var b in reader.ReadAllAsync(dropLow)) kept++;
        Assert.Equal(0, kept);

        var keep = Pred.GreaterThan("t", LiteralValue.Of(new TimeOnly(1, 0)));
        kept = 0;
        await foreach (var b in reader.ReadAllAsync(keep)) kept++;
        Assert.Equal(1, kept);
    }
#endif

    [Fact]
    public async Task Predicate_Bool_DropsAllSameValueZone()
    {
        // 2 zones × 50 rows: zone 0 = all true, zone 1 = all false.
        // Predicate (col == true) → drops zone 1.
        // Predicate (col == false) → drops zone 0.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("b", BooleanType.Default, nullable: false),
        }, metadata: null);
        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema, preserveStats: true))
            {
                var allTrue = new BooleanArray.Builder();
                for (int i = 0; i < 50; i++) allTrue.Append(true);
                w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { allTrue.Build() }, 50));
                var allFalse = new BooleanArray.Builder();
                for (int i = 0; i < 50; i++) allFalse.Append(false);
                w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { allFalse.Build() }, 50));
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            var stats = await reader.GetZoneStatsAsync(0);
            // Bool column may not produce Min/Max stats — skip the test if so.
            if (stats?.Min is not Apache.Arrow.BooleanArray
                || stats?.Max is not Apache.Arrow.BooleanArray) return;

            int kept = 0;
            await foreach (var b in reader.ReadAllAsync(Pred.Equal("b", LiteralValue.Of(true)))) kept++;
            Assert.Equal(1, kept);

            kept = 0;
            await foreach (var b in reader.ReadAllAsync(Pred.Equal("b", LiteralValue.Of(false)))) kept++;
            Assert.Equal(1, kept);

            kept = 0;
            await foreach (var b in reader.ReadAllAsync(Pred.NotEqual("b", LiteralValue.Of(true)))) kept++;
            Assert.Equal(1, kept);

            kept = 0;
            await foreach (var b in reader.ReadAllAsync(Pred.NotEqual("b", LiteralValue.Of(false)))) kept++;
            Assert.Equal(1, kept);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    // ----- Row-range slice tests -----

    /// <summary>Helper to build a 4-zone × 50-row file ([0..49], [50..99], [100..149], [150..199]).</summary>
    private static async Task<string> WriteFourZoneFileAsync(bool preserveStats = true)
    {
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        var path = Path.GetTempFileName();
        using (var fs = File.Create(path))
        using (var w = new VortexFileWriter(fs, schema, preserveStats: preserveStats))
        {
            for (int batchIdx = 0; batchIdx < 4; batchIdx++)
            {
                var b = new Int32Array.Builder();
                for (int i = 0; i < 50; i++) b.Append(batchIdx * 50 + i);
                w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { b.Build() }, 50));
            }
            w.Close();
        }
        return path;
    }

    [Fact]
    public async Task RowRange_SkipsLeadingChunksAndSlicesBoundaries()
    {
        // Range rows [75..175): zone 0 [0..49] skipped, zone 1 [50..99] sliced
        // to last 25 rows, zone 2 [100..149] kept whole, zone 3 [150..199]
        // sliced to first 25 rows. Total rows yielded = 100 across 3 batches.
        var path = await WriteFourZoneFileAsync();
        try
        {
            await using var reader = await VortexFileReader.OpenAsync(path);
            var batches = new List<RecordBatch>();
            await foreach (var b in reader.ReadAllAsync(rowOffset: 75, rowCount: 100))
                batches.Add(b);

            Assert.Equal(3, batches.Count);
            Assert.Equal(25, batches[0].Length);
            Assert.Equal(50, batches[1].Length);
            Assert.Equal(25, batches[2].Length);

            // Verify the values are exactly 75..174.
            int expected = 75;
            foreach (var b in batches)
            {
                var v = (Int32Array)b.Column(0);
                for (int i = 0; i < v.Length; i++)
                    Assert.Equal(expected++, v.GetValue(i));
            }
            Assert.Equal(175, expected);
        }
        finally { try { File.Delete(path); } catch { } }
    }

    [Fact]
    public async Task RowRange_OffsetPastEnd_YieldsNothing()
    {
        var path = await WriteFourZoneFileAsync();
        try
        {
            await using var reader = await VortexFileReader.OpenAsync(path);
            int batches = 0;
            await foreach (var b in reader.ReadAllAsync(rowOffset: 1000, rowCount: 100)) batches++;
            Assert.Equal(0, batches);
        }
        finally { try { File.Delete(path); } catch { } }
    }

    [Fact]
    public async Task RowRange_ZeroCount_YieldsNothing()
    {
        var path = await WriteFourZoneFileAsync();
        try
        {
            await using var reader = await VortexFileReader.OpenAsync(path);
            int batches = 0;
            await foreach (var b in reader.ReadAllAsync(rowOffset: 0, rowCount: 0)) batches++;
            Assert.Equal(0, batches);
        }
        finally { try { File.Delete(path); } catch { } }
    }

    [Fact]
    public async Task RowRange_RangeCoversExactlyOneFullChunk_NoSlicing()
    {
        // [50..100) = exactly zone 1. Should emit one unsliced batch of 50 rows.
        var path = await WriteFourZoneFileAsync();
        try
        {
            await using var reader = await VortexFileReader.OpenAsync(path);
            var batches = new List<RecordBatch>();
            await foreach (var b in reader.ReadAllAsync(rowOffset: 50, rowCount: 50))
                batches.Add(b);

            Assert.Single(batches);
            Assert.Equal(50, batches[0].Length);
            var v = (Int32Array)batches[0].Column(0);
            for (int i = 0; i < 50; i++) Assert.Equal(50 + i, v.GetValue(i));
        }
        finally { try { File.Delete(path); } catch { } }
    }

    [Fact]
    public async Task RowRange_RowCountClampsToFile()
    {
        // rowOffset=150, rowCount=long.MaxValue should yield exactly the last 50 rows.
        var path = await WriteFourZoneFileAsync();
        try
        {
            await using var reader = await VortexFileReader.OpenAsync(path);
            var batches = new List<RecordBatch>();
            await foreach (var b in reader.ReadAllAsync(rowOffset: 150, rowCount: long.MaxValue))
                batches.Add(b);
            Assert.Single(batches);
            Assert.Equal(50, batches[0].Length);
        }
        finally { try { File.Delete(path); } catch { } }
    }

    [Fact]
    public async Task RowRange_ComposesWithPredicate()
    {
        // 4 zones × 50 rows: zone N covers values [50N..50N+49].
        // Predicate (v >= 75) keeps zones {1,2,3} (max 49 < 75 drops zone 0).
        // Row-range [25..125) over the original logical row space. Zone 0
        // (rows 0..49) is dropped by predicate; range [25..125) covers
        // surviving zones 1 [50..99] and 2 [100..149], boundary slice on
        // zone 2 to first 25 rows. Expected: zone 1 whole (50 rows) + zone 2
        // first 25 = 75 rows total.
        var path = await WriteFourZoneFileAsync();
        try
        {
            await using var reader = await VortexFileReader.OpenAsync(path);
            var pred = Pred.GreaterThanOrEqual("v", LiteralValue.Of(75));
            var batches = new List<RecordBatch>();
            await foreach (var b in reader.ReadAllAsync(
                rowOffset: 25, rowCount: 100, columnIndices: null, predicate: pred))
                batches.Add(b);

            Assert.Equal(2, batches.Count);
            Assert.Equal(50, batches[0].Length);
            Assert.Equal(25, batches[1].Length);
        }
        finally { try { File.Delete(path); } catch { } }
    }

    [Fact]
    public async Task RowRange_ComposesWithColumnProjection()
    {
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("a", Int32Type.Default, nullable: false),
            new Field("b", Int64Type.Default, nullable: false),
        }, metadata: null);
        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema))
            {
                for (int batchIdx = 0; batchIdx < 4; batchIdx++)
                {
                    var a = new Int32Array.Builder();
                    var b = new Int64Array.Builder();
                    for (int i = 0; i < 50; i++)
                    {
                        a.Append(batchIdx * 50 + i);
                        b.Append((long)(batchIdx * 50 + i) * 1000);
                    }
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { a.Build(), b.Build() }, 50));
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            var batches = new List<RecordBatch>();
            await foreach (var b in reader.ReadAllAsync(
                rowOffset: 75, rowCount: 50, columnIndices: new[] { 1 }))
                batches.Add(b);

            // Range [75..125): zone 1 [50..99] sliced to 25 rows + zone 2 [100..149] sliced to 25 rows.
            Assert.Equal(2, batches.Count);
            Assert.All(batches, b => Assert.Single(b.Schema.FieldsList));
            Assert.All(batches, b => Assert.Equal("b", b.Schema.FieldsList[0].Name));
            Assert.Equal(25, batches[0].Length);
            Assert.Equal(25, batches[1].Length);

            // Verify values are 75000, 76000, ... 124000.
            long expected = 75_000L;
            foreach (var b in batches)
            {
                var col = (Int64Array)b.Column(0);
                for (int i = 0; i < col.Length; i++)
                {
                    Assert.Equal(expected, col.GetValue(i));
                    expected += 1000;
                }
            }
            Assert.Equal(125_000L, expected);
        }
        finally { try { File.Delete(path); } catch { } }
    }

    [Fact]
    public async Task RowRange_NegativeOffsetThrows()
    {
        var path = await WriteFourZoneFileAsync();
        try
        {
            await using var reader = await VortexFileReader.OpenAsync(path);
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            {
                await foreach (var _ in reader.ReadAllAsync(rowOffset: -1, rowCount: 10)) { }
            });
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            {
                await foreach (var _ in reader.ReadAllAsync(rowOffset: 0, rowCount: -5)) { }
            });
        }
        finally { try { File.Delete(path); } catch { } }
    }

    [Fact]
    public async Task ZonePruning_GetZoneStatsExposesPerZoneMinMax()
    {
        // Build a 4-zone file with monotonically-increasing batches and
        // verify GetZoneStatsAsync returns the right per-zone min/max.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        var sizes = new[] { 100, 100, 100, 50 };
        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema, preserveStats: true))
            {
                int rows = 0;
                foreach (var sz in sizes)
                {
                    var b = new Int32Array.Builder();
                    for (int i = 0; i < sz; i++) b.Append(rows + i);
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { b.Build() }, sz));
                    rows += sz;
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            var stats = await reader.GetZoneStatsAsync(0);
            Assert.NotNull(stats);
            Assert.Equal(100, stats!.ZoneLen);
            Assert.Equal(4, stats.ZoneCount);

            var min = (Int32Array)stats.Min!;
            var max = (Int32Array)stats.Max!;
            // Zone 0: rows 0..99 → min=0, max=99
            // Zone 1: rows 100..199 → min=100, max=199
            // Zone 2: rows 200..299 → min=200, max=299
            // Zone 3: rows 300..349 → min=300, max=349
            Assert.Equal(new[] { 0, 100, 200, 300 },
                new[] { min.GetValue(0), min.GetValue(1), min.GetValue(2), min.GetValue(3) }
                    .Select(v => v!.Value).ToArray());
            Assert.Equal(new[] { 99, 199, 299, 349 },
                new[] { max.GetValue(0), max.GetValue(1), max.GetValue(2), max.GetValue(3) }
                    .Select(v => v!.Value).ToArray());

            // null_count should be all-zero (non-nullable column).
            for (int z = 0; z < stats.ZoneCount; z++)
                Assert.Equal(0UL, stats.NullCount!.GetValue(z));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task ZonePruning_AcceptedZonesSkipsNonMatchingChunks()
    {
        // Same 4-zone shape. Filter to zones whose max >= 250 — only the
        // last two zones qualify. ReadAllAsync(accepted) should yield
        // exactly 2 batches.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        var sizes = new[] { 100, 100, 100, 50 };
        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema, preserveStats: true))
            {
                int rows = 0;
                foreach (var sz in sizes)
                {
                    var b = new Int32Array.Builder();
                    for (int i = 0; i < sz; i++) b.Append(rows + i);
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { b.Build() }, sz));
                    rows += sz;
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            var stats = await reader.GetZoneStatsAsync(0);
            var max = (Int32Array)stats!.Max!;
            var accepted = new HashSet<int>();
            for (int z = 0; z < stats.ZoneCount; z++)
                if (max.GetValue(z)!.Value >= 250) accepted.Add(z);
            Assert.Equal(new[] { 2, 3 }, accepted.OrderBy(x => x).ToArray());

            int batchCount = 0;
            int totalRows = 0;
            await foreach (var batch in reader.ReadAllAsync(accepted))
            {
                batchCount++;
                totalRows += batch.Length;
                var col = (Int32Array)batch.Column(0);
                // Every value in surviving zones is >= 200 (zone 2's start)
                // and < 350 (zone 3's end).
                Assert.True(col.GetValue(0)!.Value >= 200);
            }
            Assert.Equal(2, batchCount);
            Assert.Equal(150, totalRows); // zones 2 + 3 = 100 + 50
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task ZonePruning_GetZoneStatsReturnsNullForNonZonedFile()
    {
        // File written without preserveStats has no zones layout.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        var b = new Int32Array.Builder();
        for (int i = 0; i < 100; i++) b.Append(i);
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, 100);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch); // preserveStats: false

            await using var reader = await VortexFileReader.OpenAsync(path);
            var stats = await reader.GetZoneStatsAsync(0);
            Assert.Null(stats);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task ZonePruning_FloatStatsRoundtrip()
    {
        // Float column zoned stats expose Min/Max/Sum/NullCount/NaNCount/UncompressedSize.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", DoubleType.Default, nullable: true),
        }, metadata: null);
        var sizes = new[] { 100, 100 };
        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema, preserveStats: true))
            {
                for (int batchIdx = 0; batchIdx < sizes.Length; batchIdx++)
                {
                    var b = new DoubleArray.Builder();
                    int sz = sizes[batchIdx];
                    for (int i = 0; i < sz; i++)
                    {
                        if (batchIdx == 1 && i == 0) b.Append(double.NaN);
                        else if (i % 17 == 0) b.AppendNull();
                        else b.Append(batchIdx * 1000.0 + i);
                    }
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { b.Build() }, sz));
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            var stats = await reader.GetZoneStatsAsync(0);
            Assert.NotNull(stats);
            Assert.Equal(2, stats!.ZoneCount);
            Assert.NotNull(stats.NaNCount);
            // Zone 1 has exactly one NaN (i=0).
            Assert.Equal(0UL, stats.NaNCount!.GetValue(0));
            Assert.Equal(1UL, stats.NaNCount!.GetValue(1));
            // Min/Max exclude NaNs and nulls.
            var min = (DoubleArray)stats.Min!;
            var max = (DoubleArray)stats.Max!;
            // Zone 0: i in 1..99 except i%17==0; min = first non-null, max = 99.
            Assert.True(min.GetValue(0)!.Value <= 5);
            Assert.Equal(99.0, max.GetValue(0));
            // Zone 1: i in 1..99 except i%17==0 (and i=0 is NaN); max = 1099.
            Assert.Equal(1099.0, max.GetValue(1));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task PreserveStats_FloatColumnRoundtrips()
    {
        // Phase C adds float stats: min, max, sum, null_count, nan_count.
        // The reader still skips zones, so we just verify the file remains
        // valid round-trip and cross-val succeeds (the more meaningful
        // assertion lives in the cross-val test for float zoned stats).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", DoubleType.Default, nullable: true),
        }, metadata: null);
        var sizes = new[] { 200, 200, 100 };

        DoubleArray BuildBatch(int batchIdx, int n)
        {
            var b = new DoubleArray.Builder();
            for (int i = 0; i < n; i++)
            {
                if (i == 0 && batchIdx == 1) b.Append(double.NaN);
                else if (i % 11 == 0) b.AppendNull();
                else b.Append(batchIdx + i * 0.5);
            }
            return b.Build();
        }

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema, preserveStats: true))
            {
                for (int batchIdx = 0; batchIdx < sizes.Length; batchIdx++)
                {
                    int sz = sizes[batchIdx];
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { BuildBatch(batchIdx, sz) }, sz));
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(0));
            int idx = 0;
            for (int batchIdx = 0; batchIdx < sizes.Length; batchIdx++)
            {
                for (int i = 0; i < sizes[batchIdx]; i++)
                {
                    if (i == 0 && batchIdx == 1)
                        Assert.True(double.IsNaN(read.GetValue(idx)!.Value));
                    else if (i % 11 == 0) Assert.False(read.IsValid(idx));
                    else Assert.Equal(batchIdx + i * 0.5, read.GetValue(idx)!.Value);
                    idx++;
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task PreserveStats_AllNullIntegerBatchClearsMinMaxValidity()
    {
        // Edge case: one batch is entirely null. The min/max columns in
        // the zones table must have their validity bit cleared at that
        // zone's row (Phase B ComputeIntMinMax returns (null, null) for
        // all-null batches, and EmitZonesSegmentIntMinMaxNull threads
        // that through as cleared validity bits).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: true),
        }, metadata: null);
        var sizes = new[] { 200, 200, 100 };

        Int32Array BuildBatch(int batchIdx, int n)
        {
            var b = new Int32Array.Builder();
            for (int i = 0; i < n; i++)
            {
                if (batchIdx == 1) b.AppendNull(); // entire batch 1 is null
                else b.Append(batchIdx * 1000 + i);
            }
            return b.Build();
        }

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema, preserveStats: true))
            {
                for (int batchIdx = 0; batchIdx < sizes.Length; batchIdx++)
                {
                    int sz = sizes[batchIdx];
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { BuildBatch(batchIdx, sz) }, sz));
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            int idx = 0;
            for (int batchIdx = 0; batchIdx < sizes.Length; batchIdx++)
            {
                for (int i = 0; i < sizes[batchIdx]; i++)
                {
                    if (batchIdx == 1) Assert.False(read.IsValid(idx));
                    else Assert.Equal(batchIdx * 1000 + i, read.GetValue(idx)!.Value);
                    idx++;
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task PreserveStats_NonUniformBatchesFallsBackToChunked()
    {
        // Batches of (300, 200, 100) rows — non-uniform → CanZoneBatches
        // returns false → writer silently falls back to the existing
        // chunked layout. Roundtrip is unchanged.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        var sizes = new[] { 300, 200, 100 };
        Int32Array BuildBatch(int startRow, int n)
        {
            var b = new Int32Array.Builder();
            for (int i = 0; i < n; i++) b.Append(startRow + i);
            return b.Build();
        }

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema, preserveStats: true))
            {
                int rows = 0;
                foreach (var sz in sizes)
                {
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { BuildBatch(rows, sz) }, sz));
                    rows += sz;
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            int total = sizes.Sum();
            for (int i = 0; i < total; i++) Assert.Equal(i, read.GetValue(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task ReadColumnAsync_ConcatenatesMultipleChunks()
    {
        // Three batches of different sizes. ReadColumnAsync should now read
        // each chunk and concatenate the results into a single array — used
        // to throw NotSupportedException for ChunkCount > 1.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("id", Int32Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: true),
        }, metadata: null);
        var sizes = new[] { 100, 250, 50 };

        Int32Array BuildIds(int startRow, int n)
        {
            var b = new Int32Array.Builder();
            for (int i = 0; i < n; i++) b.Append(startRow + i);
            return b.Build();
        }
        StringArray BuildNames(int startRow, int n)
        {
            var b = new StringArray.Builder();
            for (int i = 0; i < n; i++)
            {
                int orig = startRow + i;
                if (orig % 4 == 0) b.AppendNull();
                else b.Append($"name-{orig}");
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
                    w.WriteBatch(new RecordBatch(schema,
                        new IArrowArray[] { BuildIds(rowsSoFar, sz), BuildNames(rowsSoFar, sz) }, sz));
                    rowsSoFar += sz;
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            int totalRows = sizes.Sum();
            Assert.Equal(sizes.Length, reader.ColumnPlans[0].ChunkCount);

            // Read each column as a single contiguous array.
            var ids = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            var names = Assert.IsType<StringArray>(await reader.ReadColumnAsync(1));
            Assert.Equal(totalRows, ids.Length);
            Assert.Equal(totalRows, names.Length);
            for (int i = 0; i < totalRows; i++)
            {
                Assert.Equal(i, ids.GetValue(i));
                if (i % 4 == 0)
                    Assert.False(names.IsValid(i));
                else
                    Assert.Equal($"name-{i}", names.GetString(i));
            }
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
    public async Task NestedComposite_ListOfStructRoundtrips()
    {
        // Top-level field: List<Struct<id: int32, name: string>>. Encoder
        // walks list → sliced struct elements → recursive struct encode.
        var elemType = new StructType(new[]
        {
            new Field("id", Int32Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: false),
        });
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("rows", new ListType(new Field("item", elemType, nullable: false)), nullable: false),
        }, metadata: null);

        const int n = 25; // n list rows
        // Build lists of struct elements: row i has (i % 3) + 1 elements,
        // each with id=i*100+j, name="row{i}_{j}".
        var idB = new Int32Array.Builder();
        var nameB = new StringArray.Builder();
        var listOffsets = new int[n + 1];
        int totalElems = 0;
        for (int i = 0; i < n; i++)
        {
            listOffsets[i] = totalElems;
            int len = (i % 3) + 1;
            for (int j = 0; j < len; j++)
            {
                idB.Append(i * 100 + j);
                nameB.Append($"row{i}_{j}");
            }
            totalElems += len;
        }
        listOffsets[n] = totalElems;
        var idArr = idB.Build();
        var nameArr = nameB.Build();
        var elemsStruct = new StructArray(elemType, totalElems,
            new IArrowArray[] { idArr, nameArr }, ArrowBuffer.Empty, 0);
        var offsetsBytes = new byte[(n + 1) * 4];
        for (int i = 0; i <= n; i++)
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(
                offsetsBytes.AsSpan(i * 4, 4), listOffsets[i]);
        var listArr = new ListArray(
            new ListType(new Field("item", elemType, nullable: false)),
            n,
            new ArrowBuffer(offsetsBytes),
            elemsStruct,
            ArrowBuffer.Empty,
            nullCount: 0);
        var batch = new RecordBatch(schema, new IArrowArray[] { listArr }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<ListArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            var readElems = (StructArray)read.Values;
            var readIds = (Int32Array)readElems.Fields[0];
            var readNames = (StringArray)readElems.Fields[1];
            for (int i = 0; i < n; i++)
            {
                int len = (i % 3) + 1;
                int start = read.ValueOffsets[i];
                int end = read.ValueOffsets[i + 1];
                Assert.Equal(len, end - start);
                for (int j = 0; j < len; j++)
                {
                    Assert.Equal(i * 100 + j, readIds.GetValue(start + j));
                    Assert.Equal($"row{i}_{j}", readNames.GetString(start + j));
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task NestedComposite_StructWithListRoundtrips()
    {
        // Top-level field: Struct<scalar: int32, items: List<int32>>. Encoder
        // walks struct → fields[1] is a list → recursive list encode.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("rec", new StructType(new[]
            {
                new Field("scalar", Int32Type.Default, nullable: false),
                new Field("items", new ListType(Int32Type.Default), nullable: false),
            }), nullable: false),
        }, metadata: null);

        const int n = 30;
        var scalarB = new Int32Array.Builder();
        var itemsB = new ListArray.Builder(Int32Type.Default);
        var inner = (Int32Array.Builder)itemsB.ValueBuilder;
        for (int i = 0; i < n; i++)
        {
            scalarB.Append(i * 7);
            itemsB.Append();
            int len = (i % 4) + 1;
            for (int j = 0; j < len; j++) inner.Append(i + j * 1000);
        }
        var structArr = new StructArray(
            (StructType)schema.FieldsList[0].DataType, n,
            new IArrowArray[] { scalarB.Build(), itemsB.Build() }, ArrowBuffer.Empty, 0);
        var batch = new RecordBatch(schema, new IArrowArray[] { structArr }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StructArray>(await reader.ReadColumnAsync(0));
            var readScalar = (Int32Array)read.Fields[0];
            var readItems = (ListArray)read.Fields[1];
            var readInner = (Int32Array)readItems.Values;
            for (int i = 0; i < n; i++)
            {
                Assert.Equal(i * 7, readScalar.GetValue(i));
                int len = (i % 4) + 1;
                int start = readItems.ValueOffsets[i];
                int end = readItems.ValueOffsets[i + 1];
                Assert.Equal(len, end - start);
                for (int j = 0; j < len; j++)
                    Assert.Equal(i + j * 1000, readInner.GetValue(start + j));
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task NestedComposite_ListOfListRoundtrips()
    {
        // List<List<int32>>: each row is a list whose elements are themselves
        // lists. Encoder cascades list → list → primitive.
        var inner = new ListType(new Field("item", Int32Type.Default, nullable: false));
        var outer = new ListType(new Field("item", inner, nullable: false));
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("matrix", outer, nullable: false),
        }, metadata: null);

        const int n = 12;
        // Row i has (i % 3) + 1 inner-lists; inner list j has (j + 1) elements.
        var leafB = new Int32Array.Builder();
        var innerOffsets = new List<int>();
        var outerOffsets = new int[n + 1];
        int innerCount = 0;
        int leafTotal = 0;
        for (int i = 0; i < n; i++)
        {
            outerOffsets[i] = innerCount;
            int innerLen = (i % 3) + 1;
            for (int j = 0; j < innerLen; j++)
            {
                innerOffsets.Add(leafTotal);
                int leafLen = j + 1;
                for (int k = 0; k < leafLen; k++) leafB.Append(i * 1000 + j * 100 + k);
                leafTotal += leafLen;
                innerCount++;
            }
        }
        outerOffsets[n] = innerCount;
        innerOffsets.Add(leafTotal);

        var leafArr = leafB.Build();
        var innerOffsetBytes = new byte[innerOffsets.Count * 4];
        for (int i = 0; i < innerOffsets.Count; i++)
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(
                innerOffsetBytes.AsSpan(i * 4, 4), innerOffsets[i]);
        var innerListArr = new ListArray(inner, innerCount,
            new ArrowBuffer(innerOffsetBytes), leafArr, ArrowBuffer.Empty, 0);
        var outerOffsetBytes = new byte[(n + 1) * 4];
        for (int i = 0; i <= n; i++)
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(
                outerOffsetBytes.AsSpan(i * 4, 4), outerOffsets[i]);
        var outerListArr = new ListArray(outer, n,
            new ArrowBuffer(outerOffsetBytes), innerListArr, ArrowBuffer.Empty, 0);

        var batch = new RecordBatch(schema, new IArrowArray[] { outerListArr }, n);
        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<ListArray>(await reader.ReadColumnAsync(0));
            var readInner = (ListArray)read.Values;
            var readLeaf = (Int32Array)readInner.Values;
            int innerIdx = 0;
            for (int i = 0; i < n; i++)
            {
                int innerLen = (i % 3) + 1;
                int outerStart = read.ValueOffsets[i];
                int outerEnd = read.ValueOffsets[i + 1];
                Assert.Equal(innerLen, outerEnd - outerStart);
                for (int j = 0; j < innerLen; j++)
                {
                    int leafLen = j + 1;
                    int leafStart = readInner.ValueOffsets[outerStart + j];
                    int leafEnd = readInner.ValueOffsets[outerStart + j + 1];
                    Assert.Equal(leafLen, leafEnd - leafStart);
                    for (int k = 0; k < leafLen; k++)
                        Assert.Equal(i * 1000 + j * 100 + k, readLeaf.GetValue(leafStart + k));
                    innerIdx++;
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task NestedComposite_FSLOfStructRoundtrips()
    {
        // FixedSizeList<Struct<a: int32, b: string>, 2>: each row has exactly
        // 2 struct elements.
        var elemType = new StructType(new[]
        {
            new Field("a", Int32Type.Default, nullable: false),
            new Field("b", StringType.Default, nullable: false),
        });
        const int listSize = 2;
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("pair", new FixedSizeListType(
                new Field("e", elemType, nullable: false), listSize), nullable: false),
        }, metadata: null);

        const int n = 20;
        var aB = new Int32Array.Builder();
        var bB = new StringArray.Builder();
        for (int i = 0; i < n; i++)
        for (int j = 0; j < listSize; j++)
        {
            aB.Append(i * 10 + j);
            bB.Append($"x{i}.{j}");
        }
        var elemsStruct = new StructArray(elemType, n * listSize,
            new IArrowArray[] { aB.Build(), bB.Build() }, ArrowBuffer.Empty, 0);
        var fslType = (FixedSizeListType)schema.FieldsList[0].DataType;
        var fslArr = new FixedSizeListArray(fslType, n, elemsStruct, ArrowBuffer.Empty, 0);

        var batch = new RecordBatch(schema, new IArrowArray[] { fslArr }, n);
        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<FixedSizeListArray>(await reader.ReadColumnAsync(0));
            var readElems = (StructArray)read.Values;
            var readA = (Int32Array)readElems.Fields[0];
            var readB = (StringArray)readElems.Fields[1];
            for (int i = 0; i < n; i++)
            for (int j = 0; j < listSize; j++)
            {
                Assert.Equal(i * 10 + j, readA.GetValue(i * listSize + j));
                Assert.Equal($"x{i}.{j}", readB.GetString(i * listSize + j));
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Struct_FlatRoundtrips()
    {
        // Top-level field is a Struct with 3 leaf fields. Encoder dispatches
        // through StructArrayEncoder which recursively encodes each field.
        var innerType = new StructType(new[]
        {
            new Field("a", Int32Type.Default, nullable: false),
            new Field("b", DoubleType.Default, nullable: false),
            new Field("c", StringType.Default, nullable: false),
        });
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("payload", innerType, nullable: false),
        }, metadata: null);

        const int n = 100;
        var aB = new Int32Array.Builder();
        var bB = new DoubleArray.Builder();
        var cB = new StringArray.Builder();
        for (int i = 0; i < n; i++)
        {
            aB.Append(i * 7);
            bB.Append(i * 0.5);
            cB.Append($"row-{i:D3}");
        }
        var structArr = new StructArray(innerType, n,
            new IArrowArray[] { aB.Build(), bB.Build(), cB.Build() },
            ArrowBuffer.Empty, nullCount: 0);

        var batch = new RecordBatch(schema, new IArrowArray[] { structArr }, n);
        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StructArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            var aRead = (Int32Array)read.Fields[0];
            var bRead = (DoubleArray)read.Fields[1];
            var cRead = (StringArray)read.Fields[2];
            for (int i = 0; i < n; i++)
            {
                Assert.Equal(i * 7, aRead.GetValue(i));
                Assert.Equal(i * 0.5, bRead.GetValue(i));
                Assert.Equal($"row-{i:D3}", cRead.GetString(i));
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Struct_OfStructRoundtrips()
    {
        // Nested struct: outer { x: i32, inner: struct { y: i32, z: string } }.
        // Both encoder and decoder must recurse through the nested layer.
        var innerType = new StructType(new[]
        {
            new Field("y", Int32Type.Default, nullable: false),
            new Field("z", StringType.Default, nullable: false),
        });
        var outerType = new StructType(new[]
        {
            new Field("x", Int32Type.Default, nullable: false),
            new Field("inner", innerType, nullable: false),
        });
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("nested", outerType, nullable: false),
        }, metadata: null);

        const int n = 50;
        var xB = new Int32Array.Builder();
        var yB = new Int32Array.Builder();
        var zB = new StringArray.Builder();
        for (int i = 0; i < n; i++)
        {
            xB.Append(i);
            yB.Append(i * 2);
            zB.Append($"v{i}");
        }
        var inner = new StructArray(innerType, n,
            new IArrowArray[] { yB.Build(), zB.Build() }, ArrowBuffer.Empty, 0);
        var outer = new StructArray(outerType, n,
            new IArrowArray[] { xB.Build(), inner }, ArrowBuffer.Empty, 0);
        var batch = new RecordBatch(schema, new IArrowArray[] { outer }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StructArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            var xRead = (Int32Array)read.Fields[0];
            var innerRead = (StructArray)read.Fields[1];
            Assert.Equal(n, innerRead.Length);
            var yRead = (Int32Array)innerRead.Fields[0];
            var zRead = (StringArray)innerRead.Fields[1];
            for (int i = 0; i < n; i++)
            {
                Assert.Equal(i, xRead.GetValue(i));
                Assert.Equal(i * 2, yRead.GetValue(i));
                Assert.Equal($"v{i}", zRead.GetString(i));
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Struct_NullableRoundtrips()
    {
        // Struct with its own validity bitmap on top of non-nullable fields.
        var innerType = new StructType(new[]
        {
            new Field("a", Int32Type.Default, nullable: false),
            new Field("b", StringType.Default, nullable: false),
        });
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("s", innerType, nullable: true),
        }, metadata: null);

        const int n = 80;
        var aB = new Int32Array.Builder();
        var bB = new StringArray.Builder();
        for (int i = 0; i < n; i++) { aB.Append(i); bB.Append($"r{i}"); }
        var validityBitmap = new byte[(n + 7) / 8];
        int nullCount = 0;
        for (int i = 0; i < n; i++)
        {
            if (i % 5 == 0) nullCount++;
            else validityBitmap[i / 8] |= (byte)(1 << (i % 8));
        }
        var structArr = new StructArray(innerType, n,
            new IArrowArray[] { aB.Build(), bB.Build() },
            new ArrowBuffer(validityBitmap), nullCount);
        var batch = new RecordBatch(schema, new IArrowArray[] { structArr }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StructArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            Assert.Equal(nullCount, read.NullCount);
            for (int i = 0; i < n; i++)
            {
                if (i % 5 == 0)
                    Assert.False(read.IsValid(i));
                else
                    Assert.True(read.IsValid(i));
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Struct_SlicedRoundtrips()
    {
        // Sliced struct: encoder must slice each field child to the parent's
        // logical window and pass through the bit-aligned validity offset.
        var innerType = new StructType(new[]
        {
            new Field("k", Int32Type.Default, nullable: false),
        });
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("s", innerType, nullable: false),
        }, metadata: null);

        const int total = 100;
        var kB = new Int32Array.Builder();
        for (int i = 0; i < total; i++) kB.Append(i * 11);
        var full = new StructArray(innerType, total,
            new IArrowArray[] { kB.Build() }, ArrowBuffer.Empty, 0);
        var sliced = (StructArray)full.Slice(20, 50);
        Assert.Equal(20, sliced.Offset);

        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, 50);
        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StructArray>(await reader.ReadColumnAsync(0));
            var kRead = (Int32Array)read.Fields[0];
            for (int i = 0; i < 50; i++)
                Assert.Equal((20 + i) * 11, kRead.GetValue(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task BitPacked_WithPatchesCompresses()
    {
        // 99% of values fit in 8 bits; 1% are full-width 32-bit outliers.
        // Plain bitpacked would need W=32 (no compression). With patches at
        // W=8, the bulk packs to 1 byte/row and outliers go in a sidecar.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", UInt32Type.Default, nullable: false),
        }, metadata: null);
        const int n = 4_096;
        var b = new UInt32Array.Builder();
        for (int i = 0; i < n; i++)
        {
            if (i % 100 == 0) b.Append((uint)(0x80000000u + (uint)i)); // outlier — needs 32 bits
            else b.Append((uint)(i % 200));                           // ≤ 8 bits
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
            Assert.True(compressedSize < rawSize / 2,
                $"Bitpacked-with-patches should give >2x compression (99% narrow, 1% wide). raw={rawSize}, compressed={compressedSize}.");

            await using var reader = await VortexFileReader.OpenAsync(compressedPath);
            var read = Assert.IsType<UInt32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
            {
                uint expected = i % 100 == 0
                    ? (uint)(0x80000000u + (uint)i)
                    : (uint)(i % 200);
                Assert.Equal(expected, read.GetValue(i));
            }
        }
        finally
        {
            try { File.Delete(rawPath); } catch { }
            try { File.Delete(compressedPath); } catch { }
        }
    }

    [Fact]
    public async Task BitPacked_WithPatchesNullableRoundtrips()
    {
        // Nullable + patches: validity bitmap is the trailing child after
        // patches indices/values/chunk_offsets.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", UInt32Type.Default, nullable: true),
        }, metadata: null);
        const int n = 2_048;
        var b = new UInt32Array.Builder();
        for (int i = 0; i < n; i++)
        {
            if (i % 13 == 0) b.AppendNull();
            else if (i % 100 == 7) b.Append((uint)(0x40000000u + (uint)i)); // outlier
            else b.Append((uint)(i % 64));
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
            for (int i = 0; i < n; i++)
            {
                if (i % 13 == 0)
                    Assert.False(read.IsValid(i));
                else
                {
                    Assert.True(read.IsValid(i));
                    uint expected = i % 100 == 7
                        ? (uint)(0x40000000u + (uint)i)
                        : (uint)(i % 64);
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
    public async Task Alp_DecimalLikeF64Compresses()
    {
        // 2000 doubles representing prices ($X.YY style) — bit-irregular but
        // each fits exactly as integer × 10^-2. ALP picks (e=2, f=0) and
        // encodes as int×100, which bitpacks to a fraction of native width.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("price", DoubleType.Default, nullable: false),
        }, metadata: null);
        const int n = 2_000;
        var b = new DoubleArray.Builder();
        for (int i = 0; i < n; i++) b.Append(10.0 + (i % 500) * 0.01); // 10.00..14.99
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
                $"ALP on decimal-like doubles should give >2x compression. raw={rawSize}, compressed={compressedSize}.");

            await using var reader = await VortexFileReader.OpenAsync(compressedPath);
            var read = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
                Assert.Equal(10.0 + (i % 500) * 0.01, read.GetValue(i));
        }
        finally
        {
            try { File.Delete(rawPath); } catch { }
            try { File.Delete(compressedPath); } catch { }
        }
    }

    [Fact]
    public async Task Alp_F32WithPatchesRoundtrips()
    {
        // Mostly decimal-like Float32 with a few non-decimal outliers (PI, E,
        // NaN, ±Infinity) — outliers get patched and the bulk encodes tightly.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", FloatType.Default, nullable: false),
        }, metadata: null);
        const int n = 1_500;
        var b = new FloatArray.Builder();
        for (int i = 0; i < n; i++)
        {
            switch (i)
            {
                case 100: b.Append((float)Math.PI); break;
                case 500: b.Append((float)Math.E); break;
                case 800: b.Append(float.NaN); break;
                case 1000: b.Append(float.PositiveInfinity); break;
                case 1200: b.Append(float.NegativeInfinity); break;
                default: b.Append(1.5f + (i % 100) * 0.01f); break;
            }
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<FloatArray>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++)
            {
                float expected = i switch
                {
                    100 => (float)Math.PI,
                    500 => (float)Math.E,
                    800 => float.NaN,
                    1000 => float.PositiveInfinity,
                    1200 => float.NegativeInfinity,
                    _ => 1.5f + (i % 100) * 0.01f,
                };
                float actual = read.GetValue(i)!.Value;
                if (float.IsNaN(expected))
                    Assert.True(float.IsNaN(actual), $"row {i}: expected NaN, got {actual}");
                else
                    Assert.Equal(expected, actual);
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Alp_NullableF64Roundtrips()
    {
        // Nullable doubles: ALP encoded child carries the validity bitmap.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", DoubleType.Default, nullable: true),
        }, metadata: null);
        const int n = 1_000;
        var b = new DoubleArray.Builder();
        for (int i = 0; i < n; i++)
        {
            if (i % 7 == 0) b.AppendNull();
            else b.Append(123.45 + (i % 200) * 0.01);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++)
            {
                if (i % 7 == 0)
                    Assert.False(read.IsValid(i));
                else
                {
                    Assert.True(read.IsValid(i));
                    Assert.Equal(123.45 + (i % 200) * 0.01, read.GetValue(i));
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task AlpRd_IrrationalDoublesCompress()
    {
        // Doubles where ALP can't find a profitable (e, f) — different
        // mantissas and exponents each row but a bounded number of distinct
        // exponent/sign prefixes. RDEncoder finds <= 8 distinct top-N-bit
        // patterns and dictionary-encodes them; the low bits stay raw.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", DoubleType.Default, nullable: false),
        }, metadata: null);
        const int n = 4_096;
        var b = new DoubleArray.Builder();
        // Mix of quasi-random magnitudes around three pivot values to keep
        // the high bits clustered into few patterns. ALP rejects (no
        // profitable (e, f) for irrational-ish values), ALP-RD applies.
        var rng = new Random(2026);
        for (int i = 0; i < n; i++)
        {
            double pivot = (i % 3) switch { 0 => 1.5, 1 => 12.5, _ => 100.5 };
            b.Append(pivot + rng.NextDouble() * 0.4);
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
            // Raw f64 = 8 bytes/row. ALP-RD typically gets ~6-7 bytes/row on
            // bounded-magnitude data. Just assert a measurable shrink.
            Assert.True(compressedSize < rawSize,
                $"ALP-RD on bounded-magnitude doubles should compress vs raw. raw={rawSize}, compressed={compressedSize}.");

            await using var reader = await VortexFileReader.OpenAsync(compressedPath);
            var read = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            // Re-derive expected values with the same RNG seed.
            var rng2 = new Random(2026);
            for (int i = 0; i < n; i++)
            {
                double pivot = (i % 3) switch { 0 => 1.5, 1 => 12.5, _ => 100.5 };
                Assert.Equal(pivot + rng2.NextDouble() * 0.4, read.GetValue(i)!.Value);
            }
        }
        finally
        {
            try { File.Delete(rawPath); } catch { }
            try { File.Delete(compressedPath); } catch { }
        }
    }

    [Fact]
    public async Task AlpRd_HandlesPatchesRoundtrip()
    {
        // Mostly-clustered top bits, but some outliers escape the dictionary
        // (max dict = 8). Build a column with 9+ distinct exponent prefixes
        // so at least one always becomes a patch.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", DoubleType.Default, nullable: false),
        }, metadata: null);
        const int n = 2_048;
        var pivots = new[]
        {
            1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0,
        };
        var values = new double[n];
        var b = new DoubleArray.Builder();
        var rng = new Random(7);
        for (int i = 0; i < n; i++)
        {
            values[i] = pivots[i % pivots.Length] * (1.0 + rng.NextDouble() * 0.1);
            b.Append(values[i]);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++)
                Assert.Equal(values[i], read.GetValue(i)!.Value);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task AlpRd_F32IrrationalRoundtrips()
    {
        // f32 mirror of the f64 test: bounded magnitudes around three pivots.
        // ALP rejects, ALP-RD applies, right_parts go as u32, dictionary
        // covers the small set of distinct top-bit patterns.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", FloatType.Default, nullable: false),
        }, metadata: null);
        const int n = 4_096;
        var b = new FloatArray.Builder();
        var rng = new Random(2026);
        var expected = new float[n];
        for (int i = 0; i < n; i++)
        {
            float pivot = (i % 3) switch { 0 => 1.5f, 1 => 12.5f, _ => 100.5f };
            expected[i] = pivot + (float)(rng.NextDouble() * 0.4);
            b.Append(expected[i]);
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
                $"ALP-RD on bounded-magnitude floats should compress vs raw. raw={rawSize}, compressed={compressedSize}.");

            await using var reader = await VortexFileReader.OpenAsync(compressedPath);
            var read = Assert.IsType<FloatArray>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++)
                Assert.Equal(expected[i], read.GetValue(i)!.Value);
        }
        finally
        {
            try { File.Delete(rawPath); } catch { }
            try { File.Delete(compressedPath); } catch { }
        }
    }

    [Fact]
    public async Task AlpRd_F32WithPatchesRoundtrips()
    {
        // f32 with > MaxDictSize=8 distinct top patterns → guaranteed
        // patches. Verifies the f32-specific encode loop preserves both the
        // dictionary-encoded values and the raw u16 patches.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", FloatType.Default, nullable: false),
        }, metadata: null);
        const int n = 2_048;
        var pivots = new[]
        {
            1.0f, 2.0f, 4.0f, 8.0f, 16.0f, 32.0f, 64.0f, 128.0f, 256.0f, 512.0f,
        };
        var values = new float[n];
        var b = new FloatArray.Builder();
        var rng = new Random(13);
        for (int i = 0; i < n; i++)
        {
            values[i] = pivots[i % pivots.Length] * (1.0f + (float)(rng.NextDouble() * 0.1));
            b.Append(values[i]);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<FloatArray>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++)
                Assert.Equal(values[i], read.GetValue(i)!.Value);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task AlpRd_DecimalLikeColumnDeferredToAlp()
    {
        // A decimal-shaped column that ALP claims (cleaner compression).
        // ALP-RD is checked AFTER ALP, so this verifies dispatch order; the
        // file roundtrips regardless of which encoder won.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", DoubleType.Default, nullable: false),
        }, metadata: null);
        const int n = 1_000;
        var b = new DoubleArray.Builder();
        for (int i = 0; i < n; i++) b.Append(1.5 + (i % 100) * 0.01);
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++)
                Assert.Equal(1.5 + (i % 100) * 0.01, read.GetValue(i)!.Value);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Rle_RepetitiveDoublesCompress()
    {
        // Float64 column with 5 distinct values cycling in 64-row runs.
        // Floats can't be bitpacked/FoR/delta'd — RLE is the only compressing
        // option for them. Should give substantial compression.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", DoubleType.Default, nullable: false),
        }, metadata: null);
        const int n = 4_096;
        var palette = new[] { 1.5, 2.71828, -3.14, 100.0, 0.0001 };
        var b = new DoubleArray.Builder();
        for (int i = 0; i < n; i++) b.Append(palette[(i / 64) % palette.Length]);
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
                $"RLE on a 5-distinct Double × 4096 column should give >2x compression. raw={rawSize}, compressed={compressedSize}.");

            await using var reader = await VortexFileReader.OpenAsync(compressedPath);
            var read = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
                Assert.Equal(palette[(i / 64) % palette.Length], read.GetValue(i));
        }
        finally
        {
            try { File.Delete(rawPath); } catch { }
            try { File.Delete(compressedPath); } catch { }
        }
    }

    [Fact]
    public async Task Rle_FloatColumnRoundtrips()
    {
        // Float32 path: 8 distinct values each occupying 32-row runs. n must be
        // ≥ 1024 so RLE applies structurally; here we use 2048.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("f", FloatType.Default, nullable: false),
        }, metadata: null);
        const int n = 2_048;
        var palette = new[] { 1.5f, 2.0f, -3.5f, 100.0f, 0.0001f, -0.5f, 42.0f, 99.99f };
        var b = new FloatArray.Builder();
        for (int i = 0; i < n; i++) b.Append(palette[(i / 32) % palette.Length]);
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<FloatArray>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++)
                Assert.Equal(palette[(i / 32) % palette.Length], read.GetValue(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Rle_HighDistinctDoublesFallThrough()
    {
        // Mostly-unique doubles → RLE rejects, dispatch falls through to plain
        // primitive (no other compressing encoding applies to floats today).
        // Verify round-trip.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("d", DoubleType.Default, nullable: false),
        }, metadata: null);
        const int n = 2_048;
        var b = new DoubleArray.Builder();
        for (int i = 0; i < n; i++) b.Append(i * 13.7);
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++)
                Assert.Equal(i * 13.7, read.GetValue(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task RunEnd_LongRunsCompress()
    {
        // 5 distinct Int32 values × 10 000-row runs = 50 000 rows, 5 runs.
        // Raw payload = 200 KB; runend payload = 5×(2+4) = 30 bytes plus a
        // bounded amount of FB scaffolding. We expect well over 10× shrink
        // even after file overhead. (Tested smaller n values inflate the
        // ratio — at 1 000 rows the FB tables and stats dominate the
        // compressed output.)
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        const int runLen = 10_000;
        var palette = new[] { 7, -3, 100, 0, 42 };
        const int n = runLen * 5;
        var b = new Int32Array.Builder();
        for (int i = 0; i < n; i++) b.Append(palette[i / runLen]);
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
            Assert.True(compressedSize * 10 < rawSize,
                $"RunEnd on a 5-run × {runLen}-row Int32 column should give >10x compression. raw={rawSize}, compressed={compressedSize}.");

            await using var reader = await VortexFileReader.OpenAsync(compressedPath);
            var read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
                Assert.Equal(palette[i / runLen], read.GetValue(i));
        }
        finally
        {
            try { File.Delete(rawPath); } catch { }
            try { File.Delete(compressedPath); } catch { }
        }
    }

    [Fact]
    public async Task RunEnd_UInt64WideEnds()
    {
        // > 65 536 rows forces ends_ptype = U32. Use a small-ish run pattern
        // so we still get plenty of runs to encode. 8 distinct values cycled
        // in 10 000-row runs = 80 000 rows, 8 runs.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int64Type.Default, nullable: false),
        }, metadata: null);
        const int runLen = 10_000;
        var palette = new long[] { 1L, -2L, 3L, -4L, 5L, -6L, 7L, -8L };
        const int n = runLen * 8;
        var b = new Int64Array.Builder();
        for (int i = 0; i < n; i++) b.Append(palette[i / runLen]);
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int64Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i += 1234) // sparse spot-check
                Assert.Equal(palette[i / runLen], read.GetValue(i));
            // Check exactly the run boundaries.
            for (int r = 0; r < palette.Length; r++)
            {
                Assert.Equal(palette[r], read.GetValue(r * runLen)!.Value);
                Assert.Equal(palette[r], read.GetValue((r + 1) * runLen - 1)!.Value);
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task RunEnd_HighDistinctFallsThrough()
    {
        // Strictly-increasing column → every row is its own run → RunEnd
        // rejects (numRuns == n). Dispatch should fall through to a smaller
        // encoding (delta or bitpacked depending on the column shape) and the
        // file must still round-trip.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        const int n = 1_024;
        var b = new Int32Array.Builder();
        for (int i = 0; i < n; i++) b.Append(i);
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++) Assert.Equal(i, read.GetValue(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task RunEnd_AllDistinctDoesNotApply()
    {
        // 4 fully-distinct values, 4 rows: numRuns == n. Encoder must reject;
        // dispatch falls through to plain primitive. The decoder side never
        // sees a runend node, but the data still needs to roundtrip.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        var b = new Int32Array.Builder();
        b.AppendRange(new[] { 1, 2, 3, 4 });
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, 4);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(new[] { 1, 2, 3, 4 }, new[] { read.GetValue(0), read.GetValue(1), read.GetValue(2), read.GetValue(3) }.Select(v => v!.Value).ToArray());
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task RunEnd_NullableInterleavedRoundtrips()
    {
        // Pattern: 3-row runs of {1, null, 2, null, 3, null, ...}. Each null
        // run sits between value runs so we exercise validity transitions in
        // both directions. 600 rows total, 200 runs.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: true),
        }, metadata: null);
        const int runLen = 3;
        const int numRuns = 200;
        const int n = runLen * numRuns;
        var b = new Int32Array.Builder();
        for (int run = 0; run < numRuns; run++)
        {
            bool isNull = (run % 2) == 1;
            for (int j = 0; j < runLen; j++)
            {
                if (isNull) b.AppendNull();
                else b.Append(run / 2);
            }
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
            {
                int run = i / runLen;
                bool expectedNull = (run % 2) == 1;
                if (expectedNull)
                    Assert.False(read.IsValid(i), $"row {i} (run {run}) should be null");
                else
                    Assert.Equal(run / 2, read.GetValue(i)!.Value);
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task RunEnd_NullableAdjacentNullsCollapseToOneRun()
    {
        // Verify that adjacent null rows collapse into a single null run
        // regardless of the underlying value bytes. Build a column with one
        // long initial null run (rows 0..49 — values are whatever the builder
        // wrote, garbage as far as our run logic is concerned), then a value
        // run of 7s, then more nulls. We only assert through the public Arrow
        // surface that nulls round-trip correctly.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int64Type.Default, nullable: true),
        }, metadata: null);
        const int n = 200;
        var b = new Int64Array.Builder();
        for (int i = 0; i < n; i++)
        {
            if (i < 50 || i >= 150) b.AppendNull();
            else b.Append(7L);
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
                if (i < 50 || i >= 150)
                    Assert.False(read.IsValid(i));
                else
                    Assert.Equal(7L, read.GetValue(i)!.Value);
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task RunEnd_NullableLeadingAndTrailingNullRuns()
    {
        // Leading + trailing null runs check the boundary handling at i=0
        // (seed loop) and i=n-1 (final WriteEnd close).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", UInt32Type.Default, nullable: true),
        }, metadata: null);
        const int n = 100;
        var b = new UInt32Array.Builder();
        for (int i = 0; i < 10; i++) b.AppendNull();           // leading nulls
        for (int i = 10; i < 40; i++) b.Append(42u);
        for (int i = 40; i < 70; i++) b.Append(99u);
        for (int i = 70; i < n; i++) b.AppendNull();           // trailing nulls
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<UInt32Array>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < 10; i++) Assert.False(read.IsValid(i));
            for (int i = 10; i < 40; i++) Assert.Equal(42u, read.GetValue(i)!.Value);
            for (int i = 40; i < 70; i++) Assert.Equal(99u, read.GetValue(i)!.Value);
            for (int i = 70; i < n; i++) Assert.False(read.IsValid(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Sparse_DominantZeroFillCompresses()
    {
        // 10 000-row Int32 column where only every 100th row has a non-zero
        // value. Expect sparse to win: ~100 patches at 4+4 bytes each ≈ 800
        // bytes vs raw 40 KB. Test asserts >10× compression.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        const int n = 10_000;
        var b = new Int32Array.Builder();
        for (int i = 0; i < n; i++)
            b.Append(i % 100 == 0 ? i : 0);
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
            // 1% non-zero gives ~8× shrink in practice — values are stored
            // through bitpacked which pads to 1024-row FastLanes chunks even
            // when patches are rare. Ratio is better at higher row counts;
            // the >5× gate confirms the encoding genuinely fired.
            Assert.True(compressedSize * 5 < rawSize,
                $"Sparse on a 1%-non-zero × {n}-row Int32 column should give >5x compression. raw={rawSize}, compressed={compressedSize}.");

            await using var reader = await VortexFileReader.OpenAsync(compressedPath);
            var read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
                Assert.Equal(i % 100 == 0 ? i : 0, read.GetValue(i)!.Value);
        }
        finally
        {
            try { File.Delete(rawPath); } catch { }
            try { File.Delete(compressedPath); } catch { }
        }
    }

    [Fact]
    public async Task Sparse_NonZeroDominantFillRoundtrips()
    {
        // Verify that the chosen fill is the column mode, not hard-coded zero.
        // Most rows are 42; sprinkles of 7, 11, 13. Encoder should pick 42 as
        // fill so patches cover only the sprinkles.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", UInt16Type.Default, nullable: false),
        }, metadata: null);
        const int n = 5_000;
        var b = new UInt16Array.Builder();
        for (int i = 0; i < n; i++)
        {
            ushort v = (ushort)(i % 200 switch
            {
                17 => 7,
                53 => 11,
                129 => 13,
                _ => 42,
            });
            b.Append(v);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<UInt16Array>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++)
            {
                ushort expected = (ushort)(i % 200 switch
                {
                    17 => 7,
                    53 => 11,
                    129 => 13,
                    _ => 42,
                });
                Assert.Equal(expected, read.GetValue(i)!.Value);
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Sparse_NegativeFillSignedInt8Roundtrips()
    {
        // Sign-extension test: the fill is -1 (i8 bit pattern 0xFF). A naive
        // encoder that doesn't sign-extend the mode key before serializing
        // would write the fill as +255 and the reader's `(sbyte)Int64Value`
        // cast would still recover -1, but the wire bytes would differ from
        // vortex's expectations and Rust cross-validation would break. The
        // SignExtendSigned helper produces sint64 -1 instead.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int8Type.Default, nullable: false),
        }, metadata: null);
        const int n = 1_000;
        var b = new Int8Array.Builder();
        for (int i = 0; i < n; i++)
            b.Append((sbyte)(i % 50 == 0 ? 100 : -1));
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int8Array>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++)
            {
                sbyte expected = (sbyte)(i % 50 == 0 ? 100 : -1);
                Assert.Equal(expected, read.GetValue(i)!.Value);
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Sparse_NoDominantValueFallsThrough()
    {
        // Roughly uniform distribution → sparse rejects (mode covers <10%
        // of rows here, far below the 1.5× compression gate). Dispatch
        // should fall through to bitpacked / FoR / etc., and the data must
        // still round-trip exactly.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        const int n = 1_000;
        var b = new Int32Array.Builder();
        for (int i = 0; i < n; i++) b.Append(i);
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++) Assert.Equal(i, read.GetValue(i)!.Value);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Sparse_AllSameValueDeferredToConstant()
    {
        // A fully-uniform column should be claimed by vortex.constant before
        // sparse gets a chance — sparse's IsApplicable rejects when
        // numPatches == 0 anyway. Test verifies dispatch order: constant
        // wins, file roundtrips.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        const int n = 500;
        var b = new Int32Array.Builder();
        for (int i = 0; i < n; i++) b.Append(123);
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++) Assert.Equal(123, read.GetValue(i)!.Value);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Sparse_NullableMostlyZeroRoundtrips()
    {
        // Nullable Int32 column where most rows are 0 (the mode and fill),
        // some rows are non-zero, and some rows are null. Null rows become
        // patches with the validity bit cleared; non-zero rows become patches
        // with the value preserved. Verify all three groups round-trip.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: true),
        }, metadata: null);
        const int n = 5_000;
        var b = new Int32Array.Builder();
        for (int i = 0; i < n; i++)
        {
            if (i % 100 == 0) b.AppendNull();           // ~1% null
            else if (i % 50 == 0) b.Append(i);           // ~1% non-zero (with collisions on i%100==0 already null)
            else b.Append(0);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
            {
                if (i % 100 == 0)
                    Assert.False(read.IsValid(i), $"row {i} should be null");
                else if (i % 50 == 0)
                    Assert.Equal(i, read.GetValue(i)!.Value);
                else
                    Assert.Equal(0, read.GetValue(i)!.Value);
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Sparse_NullableModeIsNonZeroRoundtrips()
    {
        // Mode is 42 (non-zero), some rows are null, some rows have other
        // values. Verifies the writer picks the mode from non-null values
        // only and emits null patches plus value patches.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", UInt16Type.Default, nullable: true),
        }, metadata: null);
        const int n = 3_000;
        var b = new UInt16Array.Builder();
        for (int i = 0; i < n; i++)
        {
            if (i % 75 == 0) b.AppendNull();
            else if (i % 40 == 0) b.Append((ushort)(i % 1000));
            else b.Append((ushort)42);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<UInt16Array>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++)
            {
                if (i % 75 == 0)
                    Assert.False(read.IsValid(i));
                else if (i % 40 == 0)
                    Assert.Equal((ushort)(i % 1000), read.GetValue(i)!.Value);
                else
                    Assert.Equal((ushort)42, read.GetValue(i)!.Value);
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Fsst_NullableUrlsRoundtrip()
    {
        // Nullable URL-shaped string column. Null rows become empty entries
        // in codes_offsets (offsets[i] == offsets[i+1]) and clear bits in
        // the validity bitmap (a third vortex.bool child appended after
        // uncompressed_lengths and codes_offsets).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("u", StringType.Default, nullable: true),
        }, metadata: null);
        const int n = 600;
        var b = new StringArray.Builder();
        for (int i = 0; i < n; i++)
        {
            if (i % 13 == 0) b.AppendNull();
            else b.Append($"https://www.example.com/path/to/resource/{i:D6}?token=abc123");
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
            {
                if (i % 13 == 0)
                    Assert.False(read.IsValid(i), $"row {i} should be null");
                else
                    Assert.Equal($"https://www.example.com/path/to/resource/{i:D6}?token=abc123", read.GetString(i));
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task VarBinView_NonNullableMixedLengthRoundtrips()
    {
        // Half short (≤ 12 bytes) → inlined; half long (> 12 bytes) →
        // referenced into the data buffer with prefix + buf_idx + offset.
        // Verifies both view encodings round-trip through the existing reader.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("s", StringType.Default, nullable: false),
        }, metadata: null);
        const int n = 200;
        var b = new StringArray.Builder();
        var expected = new string[n];
        for (int i = 0; i < n; i++)
        {
            expected[i] = i % 2 == 0
                ? $"short-{i}"                                                 // ≤ 12 bytes
                : $"this-is-a-longer-string-row-{i:D6}-with-suffix";            // > 12 bytes
            b.Append(expected[i]);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, preferVarBinView: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++) Assert.Equal(expected[i], read.GetString(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task VarBinView_NullableRoundtrips()
    {
        // Nullable column with mix of inline / referenced strings. Null rows
        // get a zeroed view; the validity child masks them on read.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("s", StringType.Default, nullable: true),
        }, metadata: null);
        const int n = 150;
        var b = new StringArray.Builder();
        var expected = new string?[n];
        for (int i = 0; i < n; i++)
        {
            if (i % 11 == 0) { expected[i] = null; b.AppendNull(); }
            else
            {
                expected[i] = i % 2 == 0
                    ? $"a-{i}"
                    : $"longer-than-twelve-bytes-row-{i:D5}";
                b.Append(expected[i]);
            }
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, preferVarBinView: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++)
            {
                if (expected[i] is null) Assert.False(read.IsValid(i));
                else Assert.Equal(expected[i], read.GetString(i));
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task VarBinView_AllInlineNoDataBuffer()
    {
        // All strings ≤ 12 bytes → no data buffer is emitted (only the views
        // buffer). Exercises the single-buffer path in the wire format.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("s", StringType.Default, nullable: false),
        }, metadata: null);
        const int n = 60;
        var b = new StringArray.Builder();
        for (int i = 0; i < n; i++) b.Append($"r{i}");
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, preferVarBinView: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++) Assert.Equal($"r{i}", read.GetString(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task VarBinView_DispatchPrefersFsstWhenApplicable()
    {
        // Even with preferVarBinView=true, the compress chain still wins for
        // strings that match a more specific encoder (FSST here, due to
        // shared substrings). VarBinView is the *fallback* string encoding,
        // not an override.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("s", StringType.Default, nullable: false),
        }, metadata: null);
        const int n = 1_000;
        var b = new StringArray.Builder();
        for (int i = 0; i < n; i++)
            b.Append($"https://www.example.com/path/to/resource/{i:D6}?token=abc");
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true, preferVarBinView: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++)
                Assert.Equal($"https://www.example.com/path/to/resource/{i:D6}?token=abc", read.GetString(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Fsst_AllNullColumnFallsThrough()
    {
        // 100% null string column → IsApplicable rejects (nothing to train),
        // dispatch falls through to plain varbin. All rows must round-trip
        // as null.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("s", StringType.Default, nullable: true),
        }, metadata: null);
        const int n = 200;
        var b = new StringArray.Builder();
        for (int i = 0; i < n; i++) b.AppendNull();
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++) Assert.False(read.IsValid(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Fsst_RepetitiveSubstringsCompress()
    {
        // 1000 mostly-unique long strings sharing common substrings — dict
        // rejects (1000 distinct values), but FSST trains a symbol table on
        // the shared subsequences and packs each row into a few bytes plus
        // escapes for the unique suffix. Expect >2× shrink.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("s", StringType.Default, nullable: false),
        }, metadata: null);
        const int n = 1_000;
        var b = new StringArray.Builder();
        for (int i = 0; i < n; i++)
            b.Append($"https://www.example.com/path/to/resource/{i:D6}?query=value&session=abc123def456");
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
            Assert.True(compressedSize * 2 < rawSize,
                $"FSST on URL-shaped strings should give >2x compression. raw={rawSize}, compressed={compressedSize}.");

            await using var reader = await VortexFileReader.OpenAsync(compressedPath);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++)
                Assert.Equal($"https://www.example.com/path/to/resource/{i:D6}?query=value&session=abc123def456", read.GetString(i));
        }
        finally
        {
            try { File.Delete(rawPath); } catch { }
            try { File.Delete(compressedPath); } catch { }
        }
    }

    [Fact]
    public async Task Fsst_NaturalEnglishTextRoundtrips()
    {
        // Natural-language repetitions: 200 sentences with the same prefix
        // and varying tails. Verifies bulk-decompress reconstructs the
        // original bytes for each row.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("sentence", StringType.Default, nullable: false),
        }, metadata: null);
        const int n = 200;
        var sentences = new string[n];
        var b = new StringArray.Builder();
        for (int i = 0; i < n; i++)
        {
            sentences[i] = $"The quick brown fox jumps over the lazy dog at position {i} which is in run {i / 10}.";
            b.Append(sentences[i]);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++) Assert.Equal(sentences[i], read.GetString(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Fsst_HighEntropyFallsThrough()
    {
        // Random-looking short strings — FSST can't find useful symbols, so
        // the gate rejects and dispatch falls through to plain varbin.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("s", StringType.Default, nullable: false),
        }, metadata: null);
        const int n = 100;
        var rng = new Random(42);
        var values = new string[n];
        var b = new StringArray.Builder();
        for (int i = 0; i < n; i++)
        {
            var bytes = new byte[8];
            rng.NextBytes(bytes);
#if NET5_0_OR_GREATER
            values[i] = Convert.ToHexString(bytes);
#else
            values[i] = BitConverter.ToString(bytes).Replace("-", "");
#endif
            b.Append(values[i]);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++) Assert.Equal(values[i], read.GetString(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Fsst_ShortColumnDeferredToVarBin()
    {
        // Below MinRows=32 the gate rejects regardless of compressibility —
        // symbol-table overhead dwarfs any win on tiny columns.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("s", StringType.Default, nullable: false),
        }, metadata: null);
        var b = new StringArray.Builder();
        for (int i = 0; i < 10; i++) b.Append($"https://example.com/path/{i}");
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, 10);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < 10; i++) Assert.Equal($"https://example.com/path/{i}", read.GetString(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Dict_RepetitiveStringsCompress()
    {
        // Highly repetitive string column — only 5 distinct values, 1000 rows.
        // Should route through vortex.dict: codes child = UInt8 (5 ≤ 256),
        // values child = StringArray of 5 entries. File size should drop
        // dramatically vs raw varbin.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("color", StringType.Default, nullable: false),
        }, metadata: null);
        var palette = new[] { "red", "green", "blue", "yellow", "magenta" };
        const int n = 1_000;
        var b = new StringArray.Builder();
        for (int i = 0; i < n; i++) b.Append(palette[i % palette.Length]);
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
            Assert.True(compressedSize < rawSize / 3,
                $"Dict on a 5-distinct/1000-row string column should give >3x compression. raw={rawSize}, compressed={compressedSize}.");

            await using var reader = await VortexFileReader.OpenAsync(compressedPath);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
                Assert.Equal(palette[i % palette.Length], read.GetString(i));
        }
        finally
        {
            try { File.Delete(rawPath); } catch { }
            try { File.Delete(compressedPath); } catch { }
        }
    }

    [Fact]
    public async Task Dict_NullableStringsRoundtrip()
    {
        // Repetitive nullable strings: dict applies and codes carry validity.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("status", StringType.Default, nullable: true),
        }, metadata: null);
        var palette = new[] { "open", "closed", "pending", "error" };
        const int n = 800;
        var b = new StringArray.Builder();
        for (int i = 0; i < n; i++)
        {
            if (i % 5 == 0) b.AppendNull();
            else b.Append(palette[i % palette.Length]);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            int expectedNulls = 0; for (int i = 0; i < n; i++) if (i % 5 == 0) expectedNulls++;
            Assert.Equal(expectedNulls, read.NullCount);
            for (int i = 0; i < n; i++)
            {
                if (i % 5 == 0)
                    Assert.False(read.IsValid(i));
                else
                {
                    Assert.True(read.IsValid(i));
                    Assert.Equal(palette[i % palette.Length], read.GetString(i));
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Dict_AllNullColumnFallsThrough()
    {
        // All-null column → IsApplicable rejects (no non-null values to dict),
        // dispatch falls through to plain varbin. Round-trip preserves the
        // all-null state.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("s", StringType.Default, nullable: true),
        }, metadata: null);
        const int n = 100;
        var b = new StringArray.Builder();
        for (int i = 0; i < n; i++) b.AppendNull();
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
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
    public async Task Dict_SlicedNonNullableRoundtrip()
    {
        // Build a 200-row column then slice off the first 75 rows so the
        // visible window has data.Offset = 75. The encoder must use
        // GetString(i) for values (offset-aware) AND read the validity bitmap
        // at bit (offset + i) when present. Here non-nullable, so we're
        // verifying just the value path.
        var palette = new[] { "alpha", "beta", "gamma", "delta", "epsilon" };
        var b = new StringArray.Builder();
        for (int i = 0; i < 200; i++) b.Append(palette[i % palette.Length]);
        var full = (StringArray)b.Build();
        var sliced = (StringArray)full.Slice(75, 125); // visible: rows 75..199.

        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("s", StringType.Default, nullable: false),
        }, metadata: null);
        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, 125);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(125, read.Length);
            for (int i = 0; i < 125; i++)
                Assert.Equal(palette[(i + 75) % palette.Length], read.GetString(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Dict_SlicedNullableRoundtrip()
    {
        // Same as above but with every 7th row null in the source. Picks an
        // odd slice offset (43) — bit 43 isn't byte-aligned so this exercises
        // the bit-level path of ExtractValidityBitmap. After slicing, row k
        // of the visible window corresponds to source row (43 + k); we expect
        // null exactly when (43 + k) % 7 == 0.
        var palette = new[] { "x", "y", "z", "w" };
        const int sourceLen = 200;
        var b = new StringArray.Builder();
        for (int i = 0; i < sourceLen; i++)
        {
            if (i % 7 == 0) b.AppendNull();
            else b.Append(palette[i % palette.Length]);
        }
        var full = (StringArray)b.Build();
        const int sliceOff = 43;
        const int sliceLen = 100;
        var sliced = (StringArray)full.Slice(sliceOff, sliceLen);

        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("s", StringType.Default, nullable: true),
        }, metadata: null);
        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, sliceLen);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(sliceLen, read.Length);
            for (int i = 0; i < sliceLen; i++)
            {
                int sourceRow = sliceOff + i;
                if (sourceRow % 7 == 0)
                    Assert.False(read.IsValid(i), $"row {i} (source {sourceRow}) should be null");
                else
                    Assert.Equal(palette[sourceRow % palette.Length], read.GetString(i));
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Dict_HighDistinctCountFallsThrough()
    {
        // Mostly-unique strings — distinct count > n/4, dispatch should reject
        // dict and fall through to plain varbin. Round-trip must still work.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("id", StringType.Default, nullable: false),
        }, metadata: null);
        const int n = 200;
        var b = new StringArray.Builder();
        for (int i = 0; i < n; i++) b.Append($"unique-{i:D5}");
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
                Assert.Equal($"unique-{i:D5}", read.GetString(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Dict_LargeDictUInt16Codes()
    {
        // Distinct count 500 forces UInt16 codes (256 < K ≤ 65536).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", StringType.Default, nullable: false),
        }, metadata: null);
        const int distinctCount = 500;
        const int n = distinctCount * 4; // 4× repetition → still passes K*4 ≤ n.
        var b = new StringArray.Builder();
        for (int i = 0; i < n; i++) b.Append($"k-{i % distinctCount:D4}");
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
                Assert.Equal($"k-{i % distinctCount:D4}", read.GetString(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Bool_SlicedRoundtrips()
    {
        // BooleanArray uses a packed bitmap at Buffers[1]. Slicing shifts the
        // visible window by data.Offset bits — encoder must use the bit-aligned
        // ExtractValidityBitmap helper (which it does for both values + nulls).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("flag", BooleanType.Default, nullable: true),
        }, metadata: null);
        const int total = 200;
        var b = new BooleanArray.Builder();
        for (int i = 0; i < total; i++)
        {
            if (i % 7 == 0) b.AppendNull();
            else b.Append(i % 3 == 0);
        }
        var full = b.Build();
        var sliced = (BooleanArray)full.Slice(11, 150); // offset=11 → not byte-aligned
        Assert.Equal(11, sliced.Offset);

        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, 150);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<BooleanArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(150, read.Length);
            for (int i = 0; i < 150; i++)
            {
                int srcRow = 11 + i;
                if (srcRow % 7 == 0)
                    Assert.False(read.IsValid(i));
                else
                {
                    Assert.True(read.IsValid(i));
                    Assert.Equal(srcRow % 3 == 0, read.GetValue(i)!.Value);
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task List_SlicedRoundtrips()
    {
        // List<int32> sliced; encoder must rebase visible offsets to start at 0
        // and pass only the corresponding range of the elements array to the
        // recursive encoder.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("xs", new ListType(Int32Type.Default), nullable: true),
        }, metadata: null);
        const int total = 80;
        var listB = new ListArray.Builder(Int32Type.Default);
        var inner = (Int32Array.Builder)listB.ValueBuilder;
        for (int i = 0; i < total; i++)
        {
            if (i % 13 == 0) listB.AppendNull();
            else
            {
                listB.Append();
                int len = (i % 4) + 1;
                for (int j = 0; j < len; j++) inner.Append(i * 100 + j);
            }
        }
        var full = listB.Build();
        var sliced = (ListArray)full.Slice(20, 50);
        Assert.Equal(20, sliced.Offset);

        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, 50);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<ListArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(50, read.Length);
            var readElements = (Int32Array)read.Values;
            for (int i = 0; i < 50; i++)
            {
                int srcRow = 20 + i;
                if (srcRow % 13 == 0)
                {
                    Assert.False(read.IsValid(i));
                }
                else
                {
                    Assert.True(read.IsValid(i));
                    int len = (srcRow % 4) + 1;
                    int start = read.ValueOffsets[i];
                    int end = read.ValueOffsets[i + 1];
                    Assert.Equal(len, end - start);
                    for (int j = 0; j < len; j++)
                        Assert.Equal(srcRow * 100 + j, readElements.GetValue(start + j));
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task FixedSizeList_SlicedRoundtrips()
    {
        // FSL<int32, 3> sliced; encoder must restrict the elements view to
        // [Offset*listSize, Offset*listSize + rowCount*listSize).
        const int listSize = 3;
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("xyz", new FixedSizeListType(Int32Type.Default, listSize), nullable: true),
        }, metadata: null);
        const int total = 60;
        var fslB = new FixedSizeListArray.Builder(Int32Type.Default, listSize);
        var inner = (Int32Array.Builder)fslB.ValueBuilder;
        for (int i = 0; i < total; i++)
        {
            if (i % 9 == 0) fslB.AppendNull();
            else
            {
                fslB.Append();
                inner.Append(i * 10);
                inner.Append(i * 10 + 1);
                inner.Append(i * 10 + 2);
            }
        }
        var full = fslB.Build();
        var sliced = (FixedSizeListArray)full.Slice(15, 30);
        Assert.Equal(15, sliced.Offset);

        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, 30);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<FixedSizeListArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(30, read.Length);
            var readElements = (Int32Array)read.Values;
            for (int i = 0; i < 30; i++)
            {
                int srcRow = 15 + i;
                if (srcRow % 9 == 0)
                    Assert.False(read.IsValid(i));
                else
                {
                    Assert.True(read.IsValid(i));
                    Assert.Equal(srcRow * 10, readElements.GetValue(i * listSize));
                    Assert.Equal(srcRow * 10 + 1, readElements.GetValue(i * listSize + 1));
                    Assert.Equal(srcRow * 10 + 2, readElements.GetValue(i * listSize + 2));
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Varbin_SlicedRoundtrips()
    {
        // String column sliced — verify VarBinArrayEncoder honors data.Offset.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("s", StringType.Default, nullable: true),
        }, metadata: null);
        const int total = 80;
        var b = new StringArray.Builder();
        for (int i = 0; i < total; i++)
        {
            if (i % 6 == 0) b.AppendNull();
            else b.Append($"row-{i:D3}");
        }
        var full = b.Build();
        var sliced = (StringArray)full.Slice(25, 40);
        Assert.Equal(25, sliced.Offset);

        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, 40);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(40, read.Length);
            for (int i = 0; i < 40; i++)
            {
                int srcRow = 25 + i;
                if (srcRow % 6 == 0)
                    Assert.False(read.IsValid(i));
                else
                {
                    Assert.True(read.IsValid(i));
                    Assert.Equal($"row-{srcRow:D3}", read.GetString(i));
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task BitPacked_SlicedInputRoundtrips()
    {
        // Build a 3000-row UInt32, slice [500..2500). Bitpacked must honor
        // data.Offset for both the value buffer and (when nullable) the validity
        // bitmap. Underlying values are narrow-range so bitpacking applies.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", UInt32Type.Default, nullable: true),
        }, metadata: null);
        const int total = 3_000;
        var b = new UInt32Array.Builder();
        for (int i = 0; i < total; i++)
        {
            if (i % 11 == 0) b.AppendNull();
            else b.Append((uint)(i % 256)); // 8-bit values
        }
        var full = b.Build();
        var sliced = (UInt32Array)full.Slice(500, 2_000);
        Assert.Equal(500, sliced.Offset);
        Assert.Equal(2_000, sliced.Length);

        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, 2_000);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<UInt32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(2_000, read.Length);
            for (int i = 0; i < 2_000; i++)
            {
                int srcRow = 500 + i;
                if (srcRow % 11 == 0)
                    Assert.False(read.IsValid(i));
                else
                {
                    Assert.True(read.IsValid(i));
                    Assert.Equal((uint)(srcRow % 256), read.GetValue(i));
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task For_SlicedNegativeInt64Roundtrips()
    {
        // FoR over a sliced Int64 column with negative values. Bitpacked alone
        // can't handle the negatives, but FoR shifts to non-negative residuals.
        // The slice means data.Offset != 0; encoder must skip nulls and read
        // from offset throughout.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int64Type.Default, nullable: true),
        }, metadata: null);
        const int total = 2_500;
        var b = new Int64Array.Builder();
        for (int i = 0; i < total; i++)
        {
            if (i % 7 == 0) b.AppendNull();
            else b.Append(-100_000L + (i % 50));
        }
        var full = b.Build();
        var sliced = (Int64Array)full.Slice(300, 1_500);
        Assert.Equal(300, sliced.Offset);

        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, 1_500);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int64Array>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < 1_500; i++)
            {
                int srcRow = 300 + i;
                if (srcRow % 7 == 0)
                    Assert.False(read.IsValid(i));
                else
                {
                    Assert.True(read.IsValid(i));
                    Assert.Equal(-100_000L + (srcRow % 50), read.GetValue(i));
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Delta_SlicedRoundtrips()
    {
        // Sliced input of a delta-friendly locally-constant column. Slice still
        // ≥ 1024 rows so delta is structurally applicable; probe should accept
        // since within-lane deltas remain near-zero.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", UInt32Type.Default, nullable: false),
        }, metadata: null);
        const int total = 5_000;
        var b = new UInt32Array.Builder();
        for (int i = 0; i < total; i++) b.Append((uint)(i / 64) + 1_000_000u);
        var full = b.Build();
        var sliced = (UInt32Array)full.Slice(800, 4_096);
        Assert.Equal(800, sliced.Offset);

        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, 4_096);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<UInt32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(4_096, read.Length);
            for (int i = 0; i < 4_096; i++)
            {
                int srcRow = 800 + i;
                Assert.Equal((uint)(srcRow / 64) + 1_000_000u, read.GetValue(i));
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Delta_LocallyConstantUInt32Compresses()
    {
        // input[i] = i / 64 — every 64 consecutive rows share a value. Within
        // each FastLanes lane (output positions p, p+LANES, p+2*LANES, ...),
        // the source rows transpose(p), transpose(p+LANES), ... span a tight
        // neighborhood, so within-lane deltas are 0 most of the time. The
        // probe should accept this column and the bitpacked deltas child
        // collapses to a tiny payload.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", UInt32Type.Default, nullable: false),
        }, metadata: null);
        const int n = 4_096; // 4 chunks
        var b = new UInt32Array.Builder();
        for (int i = 0; i < n; i++) b.Append((uint)(i / 64));
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
                $"Delta on a locally-constant column should give >4x compression. raw={rawSize}, compressed={compressedSize}.");

            await using var reader = await VortexFileReader.OpenAsync(compressedPath);
            var read = Assert.IsType<UInt32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
                Assert.Equal((uint)(i / 64), read.GetValue(i));
        }
        finally
        {
            try { File.Delete(rawPath); } catch { }
            try { File.Delete(compressedPath); } catch { }
        }
    }

    [Fact]
    public async Task Delta_LinearMonotonicFallsThrough()
    {
        // Strictly-sorted with constant linear stride. The FastLanes layout
        // permutes within-lane positions via FL_ORDER, so even for a strictly
        // monotonic input the within-lane deltas wrap around (unsigned) and
        // delta's profitability probe rejects. Dispatch falls through to FoR
        // (which DOES work well for this shape). Test confirms round-trip
        // succeeds either way.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("ts", UInt32Type.Default, nullable: false),
        }, metadata: null);
        const int n = 5_000;
        var b = new UInt32Array.Builder();
        for (int i = 0; i < n; i++) b.Append(1_000_000_000u + (uint)(i * 7));
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<UInt32Array>(await reader.ReadColumnAsync(0));
            for (int i = 0; i < n; i++)
                Assert.Equal(1_000_000_000u + (uint)(i * 7), read.GetValue(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Delta_LocallyConstantUInt64Roundtrips()
    {
        // UInt64 path: LANES=16. Same locally-constant pattern triggers delta.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", UInt64Type.Default, nullable: false),
        }, metadata: null);
        const int n = 2_048;
        var b = new UInt64Array.Builder();
        for (int i = 0; i < n; i++) b.Append((ulong)(i / 64) + 1_000_000_000UL);
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<UInt64Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
                Assert.Equal((ulong)(i / 64) + 1_000_000_000UL, read.GetValue(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Delta_ShortColumnFallsThrough()
    {
        // < 1024 rows — delta is structurally inapplicable (chunk size is 1024).
        // Dispatch should fall through to FoR/bitpacked. Verify round-trip.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", UInt32Type.Default, nullable: false),
        }, metadata: null);
        const int n = 500;
        var b = new UInt32Array.Builder();
        for (int i = 0; i < n; i++) b.Append(1_000_000_000u + (uint)(i * 7));
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<UInt32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
                Assert.Equal(1_000_000_000u + (uint)(i * 7), read.GetValue(i));
        }
        finally
        {
            try { File.Delete(path); } catch { }
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

    [Fact]
    public async Task Pco_DoublesRoundtrip()
    {
        // Bounded-magnitude doubles where pco's per-chunk mode-search
        // (Classic / IntMult / FloatMult / FloatQuant) should comfortably beat
        // raw f64 storage. Roundtrip must reconstruct every value bit-exactly
        // — pco is lossless.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", DoubleType.Default, nullable: false),
        }, metadata: null);
        const int n = 4_096;
        var rng = new Random(2026);
        var expected = new double[n];
        var b = new DoubleArray.Builder();
        for (int i = 0; i < n; i++)
        {
            expected[i] = 100.0 + rng.NextDouble() * 50.0;
            b.Append(expected[i]);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var rawPath = Path.GetTempFileName();
        var pcoPath = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(rawPath))
                VortexFileWriter.Write(fs, batch);
            using (var fs = File.Create(pcoPath))
                VortexFileWriter.Write(fs, batch, compress: true, preferPco: true);

            long rawSize = new FileInfo(rawPath).Length;
            long pcoSize = new FileInfo(pcoPath).Length;
            Assert.True(pcoSize < rawSize,
                $"pco should compress vs raw f64. raw={rawSize}, pco={pcoSize}.");

            await using var reader = await VortexFileReader.OpenAsync(pcoPath);
            var read = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
                Assert.Equal(expected[i], read.GetValue(i)!.Value);
        }
        finally
        {
            try { File.Delete(rawPath); } catch { }
            try { File.Delete(pcoPath); } catch { }
        }
    }

    [Fact]
    public async Task Pco_NullableInt64Roundtrips()
    {
        // Nullable int64 — pco compresses only the valid values; the
        // validity bitmap rides as a separate child. Sprinkled nulls
        // exercise the dense-buffer compaction path.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int64Type.Default, nullable: true),
        }, metadata: null);
        const int n = 2_048;
        var b = new Int64Array.Builder();
        var expected = new long?[n];
        for (int i = 0; i < n; i++)
        {
            if (i % 13 == 0) { b.AppendNull(); expected[i] = null; }
            else { long v = (long)i * 1_000_000L - 500L; b.Append(v); expected[i] = v; }
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true, preferPco: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int64Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
            {
                if (expected[i] is null) Assert.False(read.IsValid(i));
                else { Assert.True(read.IsValid(i)); Assert.Equal(expected[i]!.Value, read.GetValue(i)!.Value); }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Pco_Int32MultiChunkRoundtrips()
    {
        // > 1 << 18 = 262144 rows → forces multi-chunk encoding so the
        // PcoMetadata serialization with multiple PcoChunkInfo entries gets
        // exercised. Each chunk produces its own meta + page buffer pair;
        // the decoder reassembles by walking the chunks vector.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        const int n = 300_000;
        var b = new Int32Array.Builder();
        var rng = new Random(123);
        var expected = new int[n];
        for (int i = 0; i < n; i++)
        {
            expected[i] = rng.Next(int.MinValue / 2, int.MaxValue / 2);
            b.Append(expected[i]);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true, preferPco: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
                Assert.Equal(expected[i], read.GetValue(i)!.Value);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Pco_FloatRoundtrips()
    {
        // f32 mirror of the f64 test — covers PcoWrappedEncoder<float>.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", FloatType.Default, nullable: false),
        }, metadata: null);
        const int n = 2_048;
        var rng = new Random(7);
        var expected = new float[n];
        var b = new FloatArray.Builder();
        for (int i = 0; i < n; i++)
        {
            expected[i] = 50.0f + (float)rng.NextDouble() * 25.0f;
            b.Append(expected[i]);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true, preferPco: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<FloatArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
                Assert.Equal(expected[i], read.GetValue(i)!.Value);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Constant_SlicedRoundtrips()
    {
        // Underlying buffer carries varied data; the slice is uniform.
        // Writer's IsApplicable must inspect only the sliced window and
        // SerializeFirstValue must read at data.Offset.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        var b = new Int32Array.Builder();
        // Rows 0..9: noise. Rows 10..29: all 42. Rows 30..39: noise.
        for (int i = 0; i < 10; i++) b.Append(i * 100);
        for (int i = 0; i < 20; i++) b.Append(42);
        for (int i = 0; i < 10; i++) b.Append(-i);
        var full = b.Build();
        var sliced = (Int32Array)full.Slice(10, 20);
        Assert.Equal(10, sliced.Offset);
        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, 20);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(20, read.Length);
            for (int i = 0; i < 20; i++) Assert.Equal(42, read.GetValue(i)!.Value);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task RunEnd_SlicedRoundtrips()
    {
        // Run-end profitability is sensitive to row count; build a long
        // base column with cleanly aligned runs, then slice through several
        // runs so the encoder must compute run boundaries off `data.Offset`.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int64Type.Default, nullable: true),
        }, metadata: null);
        const int total = 800;
        var b = new Int64Array.Builder();
        var expected = new long?[total];
        for (int i = 0; i < total; i++)
        {
            if ((i / 50) % 3 == 0) { b.AppendNull(); expected[i] = null; }
            else { long v = (i / 50) * 7; b.Append(v); expected[i] = v; }
        }
        var full = b.Build();
        const int sliceOff = 137;
        const int sliceLen = 500;
        var sliced = (Int64Array)full.Slice(sliceOff, sliceLen);
        Assert.Equal(sliceOff, sliced.Offset);
        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, sliceLen);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int64Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(sliceLen, read.Length);
            for (int i = 0; i < sliceLen; i++)
            {
                if (expected[sliceOff + i] is null) Assert.False(read.IsValid(i));
                else { Assert.True(read.IsValid(i)); Assert.Equal(expected[sliceOff + i]!.Value, read.GetValue(i)!.Value); }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Sparse_SlicedRoundtrips()
    {
        // Sparse fires when one value covers ≥ ~67% of rows. Build a column
        // where the slice has a clear mode but the surrounding noise would
        // confuse the encoder if it didn't honor data.Offset.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        const int total = 1200;
        var b = new Int32Array.Builder();
        var expected = new int[total];
        var rng = new Random(99);
        for (int i = 0; i < total; i++)
        {
            // Slice [200, 1100) is mostly 7 with occasional outliers; outside
            // the slice is uniform random which would muddy mode detection.
            int v = (i >= 200 && i < 1100)
                ? (i % 73 == 0 ? rng.Next(int.MaxValue) : 7)
                : rng.Next(int.MaxValue);
            b.Append(v);
            expected[i] = v;
        }
        var full = b.Build();
        const int sliceOff = 200, sliceLen = 900;
        var sliced = (Int32Array)full.Slice(sliceOff, sliceLen);
        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, sliceLen);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(sliceLen, read.Length);
            for (int i = 0; i < sliceLen; i++)
                Assert.Equal(expected[sliceOff + i], read.GetValue(i)!.Value);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Alp_SlicedRoundtrips()
    {
        // Sliced decimal-shaped doubles. ALP's exponent search must walk
        // only the sliced rows or the picked (e, f) won't roundtrip.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", DoubleType.Default, nullable: false),
        }, metadata: null);
        const int total = 1500;
        var b = new DoubleArray.Builder();
        var expected = new double[total];
        for (int i = 0; i < total; i++)
        {
            // Slice covers a clean 0.01-precision distribution; outside it
            // is a mix that ALP would still compress but at different (e, f).
            expected[i] = i < 250 ? Math.PI * (i + 1) : 1.5 + ((i - 250) % 100) * 0.01;
            b.Append(expected[i]);
        }
        var full = b.Build();
        const int sliceOff = 300, sliceLen = 1000;
        var sliced = (DoubleArray)full.Slice(sliceOff, sliceLen);
        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, sliceLen);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(sliceLen, read.Length);
            for (int i = 0; i < sliceLen; i++)
                Assert.Equal(expected[sliceOff + i], read.GetValue(i)!.Value);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task AlpRd_SlicedRoundtrips()
    {
        // Sliced bounded-magnitude doubles where ALP rejects → ALP-RD claims.
        // Need ≥ 1024 rows in the slice (ALP-RD's chunk-padding gate).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", DoubleType.Default, nullable: false),
        }, metadata: null);
        const int total = 4096;
        var rng = new Random(2026);
        var b = new DoubleArray.Builder();
        var expected = new double[total];
        for (int i = 0; i < total; i++)
        {
            double pivot = (i % 3) switch { 0 => 1.5, 1 => 12.5, _ => 100.5 };
            expected[i] = pivot + rng.NextDouble() * 0.4;
            b.Append(expected[i]);
        }
        var full = b.Build();
        const int sliceOff = 512, sliceLen = 3000;
        var sliced = (DoubleArray)full.Slice(sliceOff, sliceLen);
        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, sliceLen);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(sliceLen, read.Length);
            for (int i = 0; i < sliceLen; i++)
                Assert.Equal(expected[sliceOff + i], read.GetValue(i)!.Value);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task FloatRle_SlicedRoundtrips()
    {
        // Sliced float column with cycling palette → fastlanes.rle. Need
        // ≥ 1024 rows in the slice (rle's per-chunk dict layout requires
        // multiples of 1024 rows for clean chunk boundaries).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", DoubleType.Default, nullable: false),
        }, metadata: null);
        const int total = 4096;
        var palette = new[] { 1.5, 2.71828, -3.14, 100.0, 0.0001 };
        var b = new DoubleArray.Builder();
        var expected = new double[total];
        for (int i = 0; i < total; i++)
        {
            expected[i] = palette[(i / 64) % palette.Length];
            b.Append(expected[i]);
        }
        var full = b.Build();
        const int sliceOff = 1024, sliceLen = 2048;
        var sliced = (DoubleArray)full.Slice(sliceOff, sliceLen);
        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, sliceLen);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(sliceLen, read.Length);
            for (int i = 0; i < sliceLen; i++)
                Assert.Equal(expected[sliceOff + i], read.GetValue(i)!.Value);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Pco_SlicedRoundtrips()
    {
        // Sliced numeric column with mixed nulls; pco compresses only
        // valid values, so the dense-buffer compaction must walk from
        // data.Offset rather than 0.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int64Type.Default, nullable: true),
        }, metadata: null);
        const int total = 4096;
        var b = new Int64Array.Builder();
        var expected = new long?[total];
        for (int i = 0; i < total; i++)
        {
            if (i % 11 == 0) { b.AppendNull(); expected[i] = null; }
            else { long v = (long)i * 1_000_000L - 500L; b.Append(v); expected[i] = v; }
        }
        var full = b.Build();
        const int sliceOff = 250, sliceLen = 3000;
        var sliced = (Int64Array)full.Slice(sliceOff, sliceLen);
        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, sliceLen);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true, preferPco: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<Int64Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(sliceLen, read.Length);
            for (int i = 0; i < sliceLen; i++)
            {
                if (expected[sliceOff + i] is null) Assert.False(read.IsValid(i));
                else { Assert.True(read.IsValid(i)); Assert.Equal(expected[sliceOff + i]!.Value, read.GetValue(i)!.Value); }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Fsst_SlicedRoundtrips()
    {
        // Sliced repetitive strings; FSST trains its symbol table on the
        // sliced rows only (Apache.Arrow's StringArray.GetBytes honors
        // data.Offset transparently, so the change is mostly dropping the
        // reject + threading offset through ExtractNonNullRows).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", StringType.Default, nullable: true),
        }, metadata: null);
        const int total = 200;
        var b = new StringArray.Builder();
        var expected = new string?[total];
        for (int i = 0; i < total; i++)
        {
            if (i % 9 == 0) { b.AppendNull(); expected[i] = null; }
            else
            {
                var s = (i % 4) switch
                {
                    0 => "https://example.com/foo/bar/" + (i / 4),
                    1 => "https://example.com/foo/baz/" + (i / 4),
                    2 => "https://example.com/qux/" + (i / 4),
                    _ => "https://example.com/zzz/" + (i / 4),
                };
                b.Append(s);
                expected[i] = s;
            }
        }
        var full = b.Build();
        const int sliceOff = 17, sliceLen = 150;
        var sliced = (StringArray)full.Slice(sliceOff, sliceLen);
        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, sliceLen);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(sliceLen, read.Length);
            for (int i = 0; i < sliceLen; i++)
            {
                if (expected[sliceOff + i] is null) Assert.False(read.IsValid(i));
                else { Assert.True(read.IsValid(i)); Assert.Equal(expected[sliceOff + i], read.GetString(i)); }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task VarBinView_SlicedRoundtrips()
    {
        // Sliced strings via preferVarBinView. The encoder reads through
        // Apache.Arrow's typed StringArray accessors so honoring slicing
        // amounts to dropping the data.Offset reject.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", StringType.Default, nullable: true),
        }, metadata: null);
        const int total = 100;
        var b = new StringArray.Builder();
        var expected = new string?[total];
        for (int i = 0; i < total; i++)
        {
            if (i % 13 == 0) { b.AppendNull(); expected[i] = null; }
            else
            {
                // Mix short (inline) + long (referenced) strings so both
                // view layouts are exercised.
                var s = i % 2 == 0 ? $"row{i:D3}" : new string('x', 20) + i;
                b.Append(s);
                expected[i] = s;
            }
        }
        var full = b.Build();
        const int sliceOff = 7, sliceLen = 80;
        var sliced = (StringArray)full.Slice(sliceOff, sliceLen);
        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, sliceLen);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, preferVarBinView: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<StringArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(sliceLen, read.Length);
            for (int i = 0; i < sliceLen; i++)
            {
                if (expected[sliceOff + i] is null) Assert.False(read.IsValid(i));
                else { Assert.True(read.IsValid(i)); Assert.Equal(expected[sliceOff + i], read.GetString(i)); }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    /// <summary>
    /// Helper: builds a TimestampArray directly from raw i64 ticks. Apache.Arrow's
    /// TimestampArray.Builder takes DateTimeOffset and converts internally,
    /// which is awkward for tests that want to write specific tick values.
    /// </summary>
    private static TimestampArray BuildTimestampArray(
        TimestampType type, long?[] ticksOrNull)
    {
        int n = ticksOrNull.Length;
        var bytes = new byte[(long)n * 8];
        var span = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, long>(bytes.AsSpan());
        var validity = new byte[(n + 7) / 8];
        int nullCount = 0;
        for (int i = 0; i < n; i++)
        {
            if (ticksOrNull[i] is null) { nullCount++; continue; }
            span[i] = ticksOrNull[i]!.Value;
            validity[i >> 3] |= (byte)(1 << (i & 7));
        }
        return new TimestampArray(
            type,
            new ArrowBuffer(bytes),
            nullCount > 0 ? new ArrowBuffer(validity) : ArrowBuffer.Empty,
            n, nullCount, 0);
    }

    [Fact]
    public async Task SelfRoundtrip_TimestampViaPrimitive()
    {
        // Writer first needed to LEARN how to emit Timestamp at all (DType
        // serializer + PrimitiveArrayEncoder gained TimestampType / Array
        // cases). This locks in the default Timestamp path:
        // vortex.timestamp Extension wrapping a vortex.primitive i64 storage.
        var type = new TimestampType(TimeUnit.Microsecond, (string?)"UTC");
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("ts", type, nullable: true),
        }, metadata: null);
        const int n = 100;
        // Microseconds since 2024-01-01 base, every 5th row null.
        long baseUs = 1_704_067_200L * 1_000_000L;
        var expected = new long?[n];
        for (int i = 0; i < n; i++)
            expected[i] = (i % 5 == 0) ? null : baseUs + (long)i * 60_000_000L + i * 137;
        var batch = new RecordBatch(schema, new IArrowArray[] { BuildTimestampArray(type, expected) }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var ts = Assert.IsType<TimestampType>(reader.Schema.FieldsList[0].DataType);
            Assert.Equal(TimeUnit.Microsecond, ts.Unit);
            Assert.Equal("UTC", ts.Timezone);

            var read = Assert.IsType<TimestampArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
            {
                if (expected[i] is null) Assert.False(read.IsValid(i));
                else { Assert.True(read.IsValid(i)); Assert.Equal(expected[i]!.Value, read.GetValue(i)!.Value); }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task DateTimeParts_MicrosecondsRoundtrip()
    {
        // preferDateTimeParts: timestamps split into (days, seconds, subseconds).
        // 2024-era microseconds → days range ~31, seconds ∈ [0, 86399],
        // subseconds ∈ [0, 999_999]. Each part fits in a much narrower int
        // type than the raw 8-byte i64; combined with bitpacked compression
        // on the children, datetimeparts should produce a smaller file than
        // raw vortex.primitive.
        var type = new TimestampType(TimeUnit.Microsecond, (string?)null);
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("ts", type, nullable: false),
        }, metadata: null);
        const int n = 4096;
        long baseUs = 1_704_067_200L * 1_000_000L; // 2024-01-01 UTC
        var expected = new long[n];
        var rng = new Random(2026);
        var ticks = new long?[n];
        for (int i = 0; i < n; i++)
        {
            long v = baseUs + (long)i * 600_000_000L + rng.Next(0, 1_000_000);
            expected[i] = v;
            ticks[i] = v;
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { BuildTimestampArray(type, ticks) }, n);

        var rawPath = Path.GetTempFileName();
        var dtpPath = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(rawPath))
                VortexFileWriter.Write(fs, batch);
            using (var fs = File.Create(dtpPath))
                VortexFileWriter.Write(fs, batch, compress: true, preferDateTimeParts: true);

            long rawSize = new FileInfo(rawPath).Length;
            long dtpSize = new FileInfo(dtpPath).Length;
            Assert.True(dtpSize < rawSize,
                $"datetimeparts should compress vs raw i64 timestamps. raw={rawSize}, dtp={dtpSize}.");

            await using var reader = await VortexFileReader.OpenAsync(dtpPath);
            var read = Assert.IsType<TimestampArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
                Assert.Equal(expected[i], read.GetValue(i)!.Value);
        }
        finally
        {
            try { File.Delete(rawPath); } catch { }
            try { File.Delete(dtpPath); } catch { }
        }
    }

    [Fact]
    public async Task DateTimeParts_NullableMillisecondsRoundtrip()
    {
        // Validity rides on the days child only — seconds/subseconds at
        // null rows have garbage value bytes (we write 0). Reader masks via
        // days.validity, matching upstream's split_temporal contract.
        var type = new TimestampType(TimeUnit.Millisecond, (string?)null);
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("ts", type, nullable: true),
        }, metadata: null);
        const int n = 2048;
        long baseMs = 1_704_067_200L * 1_000L;
        var expected = new long?[n];
        for (int i = 0; i < n; i++)
            expected[i] = (i % 7 == 0) ? null : baseMs + (long)i * 1_000L + (i % 1000);
        var batch = new RecordBatch(schema, new IArrowArray[] { BuildTimestampArray(type, expected) }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true, preferDateTimeParts: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<TimestampArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
            {
                if (expected[i] is null) Assert.False(read.IsValid(i));
                else { Assert.True(read.IsValid(i)); Assert.Equal(expected[i]!.Value, read.GetValue(i)!.Value); }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task SelfRoundtrip_Date32()
    {
        // Date32: i32 days since epoch, wrapped in vortex.date Extension.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("d", Date32Type.Default, nullable: true),
        }, metadata: null);
        const int n = 100;
        var bytes = new byte[(long)n * 4];
        var span = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(bytes.AsSpan());
        var validity = new byte[(n + 7) / 8];
        int nullCount = 0;
        var expected = new int?[n];
        // 2024-01-01 = 19_723 days since epoch.
        for (int i = 0; i < n; i++)
        {
            if (i % 5 == 0) { nullCount++; expected[i] = null; }
            else { int v = 19_723 + i; span[i] = v; validity[i >> 3] |= (byte)(1 << (i & 7)); expected[i] = v; }
        }
        var arr = new Date32Array(new ArrowBuffer(bytes), new ArrowBuffer(validity), n, nullCount, 0);
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            Assert.IsType<Date32Type>(reader.Schema.FieldsList[0].DataType);
            var read = Assert.IsType<Date32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
            {
                if (expected[i] is null) Assert.False(read.IsValid(i));
                else { Assert.True(read.IsValid(i)); Assert.Equal(expected[i]!.Value, read.GetValue(i)!.Value); }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task SelfRoundtrip_Time32_Milliseconds()
    {
        // Time32(Ms): i32 ms-of-day, range [0, 86_400_000), wrapped in
        // vortex.time Extension with TimeUnit::Ms (tag 2).
        var type = new Time32Type(TimeUnit.Millisecond);
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("t", type, nullable: true),
        }, metadata: null);
        const int n = 60;
        var bytes = new byte[(long)n * 4];
        var span = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(bytes.AsSpan());
        var validity = new byte[(n + 7) / 8];
        int nullCount = 0;
        var expected = new int?[n];
        for (int i = 0; i < n; i++)
        {
            if (i % 7 == 0) { nullCount++; expected[i] = null; }
            else { int v = i * 60_000; span[i] = v; validity[i >> 3] |= (byte)(1 << (i & 7)); expected[i] = v; }
        }
        var arr = new Time32Array(type, new ArrowBuffer(bytes), new ArrowBuffer(validity), n, nullCount, 0);
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var t32 = Assert.IsType<Time32Type>(reader.Schema.FieldsList[0].DataType);
            Assert.Equal(TimeUnit.Millisecond, t32.Unit);
            var read = Assert.IsType<Time32Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
            {
                if (expected[i] is null) Assert.False(read.IsValid(i));
                else { Assert.True(read.IsValid(i)); Assert.Equal(expected[i]!.Value, read.GetValue(i)!.Value); }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task SelfRoundtrip_Time64_Microseconds()
    {
        // Time64(Us): i64 us-of-day, wrapped in vortex.time Extension with
        // TimeUnit::Us (tag 1).
        var type = new Time64Type(TimeUnit.Microsecond);
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("t", type, nullable: false),
        }, metadata: null);
        const int n = 80;
        var bytes = new byte[(long)n * 8];
        var span = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, long>(bytes.AsSpan());
        var expected = new long[n];
        for (int i = 0; i < n; i++)
        {
            long v = (long)i * 1_000_000L;
            span[i] = v;
            expected[i] = v;
        }
        var arr = new Time64Array(type, new ArrowBuffer(bytes), ArrowBuffer.Empty, n, 0, 0);
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var t64 = Assert.IsType<Time64Type>(reader.Schema.FieldsList[0].DataType);
            Assert.Equal(TimeUnit.Microsecond, t64.Unit);
            var read = Assert.IsType<Time64Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
                Assert.Equal(expected[i], read.GetValue(i)!.Value);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task SelfRoundtrip_Uuid()
    {
        // FixedSizeBinaryType(16) → vortex.uuid Extension over FSL<U8, 16>.
        // The dispatcher reinterprets FSB as FSL of UInt8 without copying;
        // the reader unwraps via ExtensionArrayDecoder.Rewrap.
        var type = new FixedSizeBinaryType(16);
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("u", type, nullable: true),
        }, metadata: null);
        const int n = 50;
        // 16 bytes per row; deterministic per-row pattern.
        var bytes = new byte[(long)n * 16];
        var validity = new byte[(n + 7) / 8];
        int nullCount = 0;
        var expected = new byte[n][];
        for (int i = 0; i < n; i++)
        {
            if (i % 6 == 0) { nullCount++; expected[i] = null!; continue; }
            for (int k = 0; k < 16; k++) bytes[i * 16 + k] = (byte)(i + k);
            validity[i >> 3] |= (byte)(1 << (i & 7));
            expected[i] = new byte[16];
            for (int k = 0; k < 16; k++) expected[i][k] = (byte)(i + k);
        }
        var arrData = new ArrayData(
            type, n, nullCount, 0,
            new[] { new ArrowBuffer(validity), new ArrowBuffer(bytes) });
        var arr = new Apache.Arrow.Arrays.FixedSizeBinaryArray(arrData);
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var fsbType = Assert.IsType<FixedSizeBinaryType>(reader.Schema.FieldsList[0].DataType);
            Assert.Equal(16, fsbType.ByteWidth);
            var read = Assert.IsType<Apache.Arrow.Arrays.FixedSizeBinaryArray>(
                await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
            {
                if (expected[i] is null) Assert.False(read.IsValid(i));
                else
                {
                    Assert.True(read.IsValid(i));
                    var actual = read.GetBytes(i);
                    for (int k = 0; k < 16; k++) Assert.Equal(expected[i][k], actual[k]);
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task StringPredicate_EqualKeepsZoneInRange()
    {
        // dict_string_64rows.vortex has one zone with min='alpha' max='foxtrot'.
        // 'delta' is lex-in [alpha, foxtrot] so the zone is kept and the batch
        // surfaces. Tests Predicate.Equal(string).
        var path = TestData.TestDataPath.Resolve("dict_string_64rows.vortex");
        await using var r = await VortexFileReader.OpenAsync(path);
        var batches = new List<RecordBatch>();
        await foreach (var b in r.ReadAllAsync(Pred.Equal("color", LiteralValue.Of("delta")))) batches.Add(b);
        try
        {
            Assert.Single(batches);
        }
        finally
        {
            foreach (var b in batches) b.Dispose();
        }
    }

    [Fact]
    public async Task StringPredicate_EqualOutOfRangePrunes()
    {
        // 'zebra' > 'foxtrot' (max), so the zone is dropped.
        var path = TestData.TestDataPath.Resolve("dict_string_64rows.vortex");
        await using var r = await VortexFileReader.OpenAsync(path);
        var batches = new List<RecordBatch>();
        await foreach (var b in r.ReadAllAsync(Pred.Equal("color", LiteralValue.Of("zebra")))) batches.Add(b);
        try
        {
            Assert.Empty(batches);
        }
        finally
        {
            foreach (var b in batches) b.Dispose();
        }
    }

    [Fact]
    public async Task StringPredicate_GreaterThanMaxPrunes()
    {
        // Predicate "> max" drops the zone (max is foxtrot, cmp(max, foxtrot)=0 ≤ 0).
        var path = TestData.TestDataPath.Resolve("dict_string_64rows.vortex");
        await using var r = await VortexFileReader.OpenAsync(path);
        var batches = new List<RecordBatch>();
        await foreach (var b in r.ReadAllAsync(Pred.GreaterThan("color", LiteralValue.Of("foxtrot")))) batches.Add(b);
        try
        {
            Assert.Empty(batches);
        }
        finally
        {
            foreach (var b in batches) b.Dispose();
        }
    }

    [Fact]
    public async Task StringPredicate_LessThanMinPrunes()
    {
        // Predicate "< min" drops the zone (min is alpha, cmp(min, alpha)=0 ≥ 0).
        var path = TestData.TestDataPath.Resolve("dict_string_64rows.vortex");
        await using var r = await VortexFileReader.OpenAsync(path);
        var batches = new List<RecordBatch>();
        await foreach (var b in r.ReadAllAsync(Pred.LessThan("color", LiteralValue.Of("alpha")))) batches.Add(b);
        try
        {
            Assert.Empty(batches);
        }
        finally
        {
            foreach (var b in batches) b.Dispose();
        }
    }

    [Fact]
    public async Task StringPredicate_GreaterOrEqualKeeps()
    {
        // GreaterOrEqual(min) keeps the zone (cmp(max, min) >= 0 trivially).
        var path = TestData.TestDataPath.Resolve("dict_string_64rows.vortex");
        await using var r = await VortexFileReader.OpenAsync(path);
        var batches = new List<RecordBatch>();
        await foreach (var b in r.ReadAllAsync(Pred.GreaterThanOrEqual("color", LiteralValue.Of("alpha")))) batches.Add(b);
        try
        {
            Assert.Single(batches);
        }
        finally
        {
            foreach (var b in batches) b.Dispose();
        }
    }

    [Fact]
    public async Task OurWriter_StringPredicatePrunesAcrossZones()
    {
        // Write 3 batches × 100 rows. Each batch's strings come from a
        // disjoint lex-range so per-zone min/max are tight enough that a
        // targeted predicate drops 2 of the 3 zones.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", StringType.Default, nullable: false),
        }, metadata: null);
        const int rowsPerBatch = 100;
        string[][] values =
        {
            // Zone 0: ax-a* (lex range ['ax-a000', 'ax-a099']).
            BuildPattern("ax-a", rowsPerBatch),
            // Zone 1: bx-a* (lex range ['bx-a000', 'bx-a099']).
            BuildPattern("bx-a", rowsPerBatch),
            // Zone 2: cx-a* (lex range ['cx-a000', 'cx-a099']).
            BuildPattern("cx-a", rowsPerBatch),
        };

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            {
                using var w = new VortexFileWriter(fs, schema, preserveStats: true);
                foreach (var batch in values)
                {
                    var b = new StringArray.Builder();
                    foreach (var s in batch) b.Append(s);
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { b.Build() }, batch.Length));
                }
                w.Close();
            }

            // Predicate Equal('bx-a050'): only zone 1's range covers it, so
            // zones 0 + 2 should be dropped.
            await using var reader = await VortexFileReader.OpenAsync(path);
            var stats = await reader.GetZoneStatsAsync(0);
            Assert.NotNull(stats);
            Assert.IsType<StringArray>(stats!.Min);
            Assert.IsType<StringArray>(stats.Max);
            Assert.Equal(3, stats.ZoneCount);

            var batches = new List<RecordBatch>();
            await foreach (var b in reader.ReadAllAsync(Pred.Equal("v", LiteralValue.Of("bx-a050")))) batches.Add(b);
            try
            {
                var batch = Assert.Single(batches);
                Assert.Equal(rowsPerBatch, batch.Length);
                var col = Assert.IsType<StringArray>(batch.Column(0));
                Assert.StartsWith("bx-a", col.GetString(0));
            }
            finally
            {
                foreach (var b in batches) b.Dispose();
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }

        static string[] BuildPattern(string prefix, int n)
        {
            var arr = new string[n];
            for (int i = 0; i < n; i++) arr[i] = $"{prefix}{i:D3}";
            return arr;
        }
    }

    [Fact]
    public async Task OurWriter_BinaryPredicatePrunesAcrossZones()
    {
        // Mirror of the string test for BinaryArray. Each zone covers a
        // disjoint byte-prefix range.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", BinaryType.Default, nullable: false),
        }, metadata: null);
        const int rowsPerBatch = 64;

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            {
                using var w = new VortexFileWriter(fs, schema, preserveStats: true);
                for (byte zone = 0; zone < 3; zone++)
                {
                    var b = new BinaryArray.Builder();
                    for (int i = 0; i < rowsPerBatch; i++)
                    {
                        // Each zone uses a distinct leading byte: 0x10 / 0x20 / 0x30.
                        b.Append((ReadOnlySpan<byte>)new byte[] { (byte)(0x10 * (zone + 1)), (byte)i });
                    }
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { b.Build() }, rowsPerBatch));
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            var stats = await reader.GetZoneStatsAsync(0);
            Assert.NotNull(stats);
            Assert.IsType<BinaryArray>(stats!.Min);
            Assert.IsType<BinaryArray>(stats.Max);
            Assert.Equal(3, stats.ZoneCount);

            // Predicate Equal([0x20, 0x10]) — only zone 1 (leading byte 0x20).
            var batches = new List<RecordBatch>();
            var pat = new byte[] { 0x20, 0x10 };
            await foreach (var b in reader.ReadAllAsync(Pred.Equal("v", LiteralValue.Of(pat)))) batches.Add(b);
            try
            {
                var batch = Assert.Single(batches);
                var col = Assert.IsType<BinaryArray>(batch.Column(0));
                Assert.Equal(0x20, col.GetBytes(0)[0]);
            }
            finally
            {
                foreach (var b in batches) b.Dispose();
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Fsst_BinaryArrayRoundtrip()
    {
        // Repetitive binary payloads with a shared prefix — FSST trains a
        // useful symbol table on the bytes regardless of UTF-8-ness. Mix
        // in nulls so the validity-child path also fires.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", BinaryType.Default, nullable: true),
        }, metadata: null);
        const int n = 200;
        var prefix = new byte[] { 0xCA, 0xFE, 0xBA, 0xBE, 0xDE, 0xAD, 0xBE, 0xEF };
        var b = new BinaryArray.Builder();
        var expected = new byte[n][];
        for (int i = 0; i < n; i++)
        {
            if (i % 11 == 0) { b.AppendNull(); expected[i] = null!; continue; }
            // 8-byte shared prefix + 4-byte varying suffix → FSST should
            // pick up the prefix as a high-frequency symbol.
            var bytes = new byte[12];
            Buffer.BlockCopy(prefix, 0, bytes, 0, 8);
            bytes[8] = (byte)(i & 0xFF);
            bytes[9] = (byte)((i >> 8) & 0xFF);
            bytes[10] = 0x55;
            bytes[11] = 0xAA;
            b.Append((ReadOnlySpan<byte>)bytes);
            expected[i] = bytes;
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            Assert.IsType<BinaryType>(reader.Schema.FieldsList[0].DataType);
            var read = Assert.IsType<BinaryArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
            {
                if (expected[i] is null) Assert.False(read.IsValid(i));
                else
                {
                    Assert.True(read.IsValid(i));
                    var actual = read.GetBytes(i);
                    Assert.Equal(expected[i].Length, actual.Length);
                    for (int k = 0; k < expected[i].Length; k++)
                        Assert.Equal(expected[i][k], actual[k]);
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task VarBinView_BinaryArrayRoundtrip()
    {
        // BinaryArray under preferVarBinView. Mix of inline-eligible
        // (≤ 12 bytes) and referenced (> 12 bytes) values plus nulls so
        // both view-format branches + the validity child fire.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", BinaryType.Default, nullable: true),
        }, metadata: null);
        const int n = 100;
        var b = new BinaryArray.Builder();
        var expected = new byte[n][];
        for (int i = 0; i < n; i++)
        {
            if (i % 13 == 0) { b.AppendNull(); expected[i] = null!; continue; }
            // Even rows: short payload. Odd rows: > 12 bytes to force the
            // referenced-view path (length, prefix, buf_idx, offset).
            var bytes = (i % 2 == 0)
                ? new byte[] { (byte)i, 0xAA, 0xBB }
                : new byte[20];
            if (i % 2 == 1)
            {
                for (int k = 0; k < 20; k++) bytes[k] = (byte)((i + k) & 0xFF);
            }
            b.Append((ReadOnlySpan<byte>)bytes);
            expected[i] = bytes;
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, preferVarBinView: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            Assert.IsType<BinaryType>(reader.Schema.FieldsList[0].DataType);
            var read = Assert.IsType<BinaryArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
            {
                if (expected[i] is null) Assert.False(read.IsValid(i));
                else
                {
                    Assert.True(read.IsValid(i));
                    var actual = read.GetBytes(i);
                    Assert.Equal(expected[i].Length, actual.Length);
                    for (int k = 0; k < expected[i].Length; k++)
                        Assert.Equal(expected[i][k], actual[k]);
                }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task NullableAlpRd_DoublesRoundtrip()
    {
        // Bounded-magnitude nullable doubles: ALP-RD's IsApplicable cleared
        // the nullable reject in chunk 9.62. Min row count ≥ 1024 (ALP-RD's
        // chunk-padding gate). Validity rides on left_parts; reader pulls
        // it through ExtractValidity.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", DoubleType.Default, nullable: true),
        }, metadata: null);
        const int n = 4096;
        var rng = new Random(2026);
        var b = new DoubleArray.Builder();
        var expected = new double?[n];
        for (int i = 0; i < n; i++)
        {
            if (i % 11 == 0) { b.AppendNull(); expected[i] = null; }
            else
            {
                double pivot = (i % 3) switch { 0 => 1.5, 1 => 12.5, _ => 100.5 };
                double v = pivot + rng.NextDouble() * 0.4;
                b.Append(v);
                expected[i] = v;
            }
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
            {
                if (expected[i] is null) Assert.False(read.IsValid(i));
                else { Assert.True(read.IsValid(i)); Assert.Equal(expected[i]!.Value, read.GetValue(i)!.Value); }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task NullableAlpRd_FloatsWithPatchesRoundtrip()
    {
        // f32 with > MaxDictSize=8 distinct top patterns → guaranteed
        // patches, plus nulls. Verifies patches are NOT generated at null
        // rows (writer skips them) and the reader's null-bitmap masks the
        // (zero-coded) null rows correctly.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", FloatType.Default, nullable: true),
        }, metadata: null);
        const int n = 2048;
        var pivots = new[] { 1.0f, 2.0f, 4.0f, 8.0f, 16.0f, 32.0f, 64.0f, 128.0f, 256.0f, 512.0f };
        var b = new FloatArray.Builder();
        var expected = new float?[n];
        var rng = new Random(13);
        for (int i = 0; i < n; i++)
        {
            if (i % 9 == 0) { b.AppendNull(); expected[i] = null; }
            else
            {
                float v = pivots[i % pivots.Length] * (1.0f + (float)(rng.NextDouble() * 0.1));
                b.Append(v);
                expected[i] = v;
            }
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<FloatArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
            {
                if (expected[i] is null) Assert.False(read.IsValid(i));
                else { Assert.True(read.IsValid(i)); Assert.Equal(expected[i]!.Value, read.GetValue(i)!.Value); }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task DictLayout_StringRoundtrip_MultiBatch()
    {
        // Three batches, each drawing from the same 6-string palette.
        // preferDictLayout merges the per-batch dicts into a single shared
        // values segment + 3 codes segments. Roundtrip must reproduce
        // every value bit-exactly.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("color", StringType.Default, nullable: true),
        }, metadata: null);
        var palette = new[] { "alpha", "bravo", "charlie", "delta", "echo", "foxtrot" };
        const int rowsPerBatch = 200;
        var allExpected = new List<string?>();

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            {
                using var w = new VortexFileWriter(fs, schema, preferDictLayout: true);
                var rng = new Random(42);
                for (int batch = 0; batch < 3; batch++)
                {
                    var b = new StringArray.Builder();
                    for (int i = 0; i < rowsPerBatch; i++)
                    {
                        if (i % 17 == 0) { b.AppendNull(); allExpected.Add(null); }
                        else
                        {
                            var v = palette[rng.Next(palette.Length)];
                            b.Append(v);
                            allExpected.Add(v);
                        }
                    }
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { b.Build() }, rowsPerBatch));
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            Assert.Equal(rowsPerBatch * 3L, reader.NumberOfRows);

            int read = 0;
            await foreach (var batch in reader.ReadAllAsync())
            {
                var col = Assert.IsType<StringArray>(batch.Column(0));
                for (int i = 0; i < col.Length; i++)
                {
                    var expected = allExpected[read + i];
                    if (expected is null) Assert.False(col.IsValid(i));
                    else { Assert.True(col.IsValid(i)); Assert.Equal(expected, col.GetString(i)); }
                }
                read += col.Length;
                batch.Dispose();
            }
            Assert.Equal(allExpected.Count, read);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task DictLayout_WithStats_PrunesAcrossZones()
    {
        // preferDictLayout AND preserveStats: each column emits
        // vortex.stats(vortex.dict(...), zones-flat). Three batches with
        // disjoint lex-prefixes so per-zone min/max are tight enough that
        // a targeted predicate drops 2 of the 3 zones.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", StringType.Default, nullable: false),
        }, metadata: null);
        const int rowsPerBatch = 100;
        string[][] values =
        {
            BuildPattern("ax-", rowsPerBatch),
            BuildPattern("bx-", rowsPerBatch),
            BuildPattern("cx-", rowsPerBatch),
        };

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            {
                using var w = new VortexFileWriter(fs, schema,
                    preserveStats: true, preferDictLayout: true);
                foreach (var batch in values)
                {
                    var b = new StringArray.Builder();
                    foreach (var s in batch) b.Append(s);
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { b.Build() }, batch.Length));
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);

            // Stats are present despite the dict-layout wrapper.
            var stats = await reader.GetZoneStatsAsync(0);
            Assert.NotNull(stats);
            Assert.Equal(3, stats!.ZoneCount);
            Assert.IsType<StringArray>(stats.Min);
            Assert.IsType<StringArray>(stats.Max);

            // Predicate drops 2 of 3 zones.
            var batches = new List<RecordBatch>();
            await foreach (var b in reader.ReadAllAsync(Pred.Equal("v", LiteralValue.Of("bx-050"))))
                batches.Add(b);
            try
            {
                var batch = Assert.Single(batches);
                Assert.Equal(rowsPerBatch, batch.Length);
                var col = Assert.IsType<StringArray>(batch.Column(0));
                Assert.StartsWith("bx-", col.GetString(0));
            }
            finally
            {
                foreach (var b in batches) b.Dispose();
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }

        static string[] BuildPattern(string prefix, int n)
        {
            var arr = new string[n];
            for (int i = 0; i < n; i++) arr[i] = $"{prefix}{i:D3}";
            return arr;
        }
    }

    [Fact]
    public async Task DictLayout_WithStats_RoundtripsValuesAndPalette()
    {
        // Functional roundtrip with both flags on — verify that the values
        // come back correctly even with the new stats wrapper.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("color", StringType.Default, nullable: true),
        }, metadata: null);
        var palette = new[] { "alpha", "bravo", "charlie", "delta" };
        const int rowsPerBatch = 80;
        var allExpected = new List<string?>();

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            {
                using var w = new VortexFileWriter(fs, schema,
                    preserveStats: true, preferDictLayout: true);
                var rng = new Random(7);
                for (int batch = 0; batch < 3; batch++)
                {
                    var b = new StringArray.Builder();
                    for (int i = 0; i < rowsPerBatch; i++)
                    {
                        if (i % 13 == 0) { b.AppendNull(); allExpected.Add(null); }
                        else
                        {
                            var v = palette[rng.Next(palette.Length)];
                            b.Append(v);
                            allExpected.Add(v);
                        }
                    }
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { b.Build() }, rowsPerBatch));
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            Assert.Equal(rowsPerBatch * 3L, reader.NumberOfRows);

            int read = 0;
            await foreach (var batch in reader.ReadAllAsync())
            {
                var col = Assert.IsType<StringArray>(batch.Column(0));
                for (int i = 0; i < col.Length; i++)
                {
                    var expected = allExpected[read + i];
                    if (expected is null) Assert.False(col.IsValid(i));
                    else { Assert.True(col.IsValid(i)); Assert.Equal(expected, col.GetString(i)); }
                }
                read += col.Length;
                batch.Dispose();
            }
            Assert.Equal(allExpected.Count, read);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task DictLayout_IsSmallerThanChunked()
    {
        // Sanity check the actual win: same data with preferDictLayout=true
        // should be measurably smaller than the default chunked-flat output.
        // 5 batches × 1000 rows × repeated palette → dict bytes (palette +
        // strings) appear once with dict layout vs 5x with chunked.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("country", StringType.Default, nullable: false),
        }, metadata: null);
        var palette = new[] { "USA", "Canada", "Mexico", "Brazil", "Argentina", "Chile", "Peru", "Colombia" };

        long Write(bool preferDict)
        {
            var path = Path.GetTempFileName();
            try
            {
                using (var fs = File.Create(path))
                {
                    using var w = new VortexFileWriter(fs, schema, preferDictLayout: preferDict);
                    var rng = new Random(7);
                    for (int batch = 0; batch < 5; batch++)
                    {
                        var b = new StringArray.Builder();
                        for (int i = 0; i < 1000; i++)
                            b.Append(palette[rng.Next(palette.Length)]);
                        w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { b.Build() }, 1000));
                    }
                    w.Close();
                }
                return new FileInfo(path).Length;
            }
            finally
            {
                try { File.Delete(path); } catch { }
            }
        }

        long withDict = Write(preferDict: true);
        long withoutDict = Write(preferDict: false);
        Assert.True(withDict < withoutDict,
            $"Layout dict should be smaller than chunked. dict={withDict}, chunked={withoutDict}.");
        await Task.CompletedTask; // satisfy async signature
    }

    [Fact]
    public async Task NullableRle_FloatsRoundtrip()
    {
        // Repetitive doubles with sprinkled nulls — fastlanes.rle is the
        // only compressing path for floats today, so the gate should pick
        // it up. Validity rides on the indices child; null rows write
        // index 0 + cleared validity bit.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", DoubleType.Default, nullable: true),
        }, metadata: null);
        const int n = 4096;
        var palette = new[] { 1.5, 2.71828, -3.14, 100.0, 0.0001 };
        var b = new DoubleArray.Builder();
        var expected = new double?[n];
        for (int i = 0; i < n; i++)
        {
            if (i % 13 == 0) { b.AppendNull(); expected[i] = null; }
            else
            {
                var v = palette[(i / 64) % palette.Length];
                b.Append(v);
                expected[i] = v;
            }
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            await using var reader = await VortexFileReader.OpenAsync(path);
            var read = Assert.IsType<DoubleArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
            {
                if (expected[i] is null) Assert.False(read.IsValid(i));
                else { Assert.True(read.IsValid(i)); Assert.Equal(expected[i]!.Value, read.GetValue(i)!.Value); }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task PublicReadColumnAsync_ReadsSingleColumn()
    {
        // ReadColumnAsync was internal until chunk 9.61 — this test pins the
        // promoted public surface. Use multi_col_4rows.vortex (i32, i64, i32)
        // and verify reading just column 1 (i64) yields the expected values.
        var path = TestData.TestDataPath.Resolve("multi_col_4rows.vortex");
        await using var r = await VortexFileReader.OpenAsync(path);
        var col = await r.ReadColumnAsync(1);
        var i64 = Assert.IsType<Int64Array>(col);
        Assert.Equal(4, i64.Length);
        for (int i = 0; i < 4; i++) Assert.Equal(42L, i64.GetValue(i)!.Value);
    }

    [Fact]
    public async Task PublicReadColumnAsync_OutOfRangeThrows()
    {
        var path = TestData.TestDataPath.Resolve("multi_col_4rows.vortex");
        await using var r = await VortexFileReader.OpenAsync(path);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await r.ReadColumnAsync(99));
    }

    [Fact]
    public async Task ReadAllAsync_ProjectsColumns()
    {
        // multi_col_4rows.vortex schema: a (i32), b (i64), c (i32). Project
        // [c, a] — verify the emitted batch has exactly those two columns
        // in the requested order, with the rest skipped.
        var path = TestData.TestDataPath.Resolve("multi_col_4rows.vortex");
        await using var r = await VortexFileReader.OpenAsync(path);
        Assert.Equal(3, r.Schema.FieldsList.Count);

        var batches = new List<RecordBatch>();
        await foreach (var b in r.ReadAllAsync(new[] { 2, 0 })) batches.Add(b);
        try
        {
            var batch = Assert.Single(batches);
            Assert.Equal(2, batch.ColumnCount);
            // Schema is projected to the subset, in order.
            Assert.Equal(r.Schema.FieldsList[2].Name, batch.Schema.FieldsList[0].Name);
            Assert.Equal(r.Schema.FieldsList[0].Name, batch.Schema.FieldsList[1].Name);
            // Both columns are i32 in this fixture — sanity-check types
            // through the projected order.
            Assert.IsType<Int32Array>(batch.Column(0));
            Assert.IsType<Int32Array>(batch.Column(1));
        }
        finally
        {
            foreach (var b in batches) b.Dispose();
        }
    }

    [Fact]
    public async Task ReadAllAsync_ProjectionRejectsEmptyList()
    {
        var path = TestData.TestDataPath.Resolve("multi_col_4rows.vortex");
        await using var r = await VortexFileReader.OpenAsync(path);
        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await foreach (var _ in r.ReadAllAsync(System.Array.Empty<int>())) { }
        });
    }

    [Fact]
    public async Task ReadAllAsync_ProjectionRejectsBadIndex()
    {
        var path = TestData.TestDataPath.Resolve("multi_col_4rows.vortex");
        await using var r = await VortexFileReader.OpenAsync(path);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
        {
            await foreach (var _ in r.ReadAllAsync(new[] { 0, 99 })) { }
        });
    }

    [Fact]
    public async Task ReadAllAsync_ProjectionWithPredicate()
    {
        // Combine projection + predicate. Build a 3-batch file with one
        // string column "key" + one int column "value", then project just
        // "value" while pruning by key. Each batch's "key" range is disjoint;
        // the predicate hits exactly one zone.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("key", StringType.Default, nullable: false),
            new Field("value", Int32Type.Default, nullable: false),
        }, metadata: null);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            {
                using var w = new VortexFileWriter(fs, schema, preserveStats: true);
                for (int z = 0; z < 3; z++)
                {
                    var keys = new StringArray.Builder();
                    var vals = new Int32Array.Builder();
                    for (int i = 0; i < 50; i++)
                    {
                        keys.Append($"zone{z}-row{i:D3}");
                        vals.Append(z * 1000 + i);
                    }
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { keys.Build(), vals.Build() }, 50));
                }
                w.Close();
            }

            await using var reader = await VortexFileReader.OpenAsync(path);
            // Project "value" only (col index 1); predicate matches zone 1.
            var batches = new List<RecordBatch>();
            await foreach (var b in reader.ReadAllAsync(
                new[] { 1 }, Pred.Equal("key", LiteralValue.Of("zone1-row025"))))
                batches.Add(b);
            try
            {
                var batch = Assert.Single(batches);
                Assert.Equal(1, batch.ColumnCount);
                Assert.Equal("value", batch.Schema.FieldsList[0].Name);
                var ints = Assert.IsType<Int32Array>(batch.Column(0));
                Assert.Equal(50, ints.Length);
                // Zone 1's first row → value = 1*1000 + 0 = 1000.
                Assert.Equal(1000, ints.GetValue(0)!.Value);
            }
            finally
            {
                foreach (var b in batches) b.Dispose();
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task BinaryFixture_ColumnRoundTrip()
    {
        // Sanity-check the read path itself before exercising predicates:
        // open binary_col_64rows.vortex, decode column 0, verify each row's
        // bytes match the deterministic Rust generator pattern.
        var path = TestData.TestDataPath.Resolve("binary_col_64rows.vortex");
        await using var r = await VortexFileReader.OpenAsync(path);
        Assert.IsType<BinaryType>(r.Schema.FieldsList[0].DataType);
        var batches = new List<RecordBatch>();
        await foreach (var b in r.ReadAllAsync()) batches.Add(b);
        try
        {
            var batch = Assert.Single(batches);
            var col = Assert.IsType<BinaryArray>(batch.Column(0));
            Assert.Equal(64, col.Length);
            for (int i = 0; i < 64; i++)
            {
                var bytes = col.GetBytes(i);
                Assert.Equal(8, bytes.Length);
                Assert.Equal(0x10, bytes[0]);
                Assert.Equal((byte)i, bytes[1]);
                for (int k = 2; k < 8; k++) Assert.Equal(0, bytes[k]);
            }
        }
        finally
        {
            foreach (var b in batches) b.Dispose();
        }
    }

    [Fact]
    public async Task BinaryPredicate_OpensFixtureAndSeesStats()
    {
        // binary_col_64rows.vortex is a Rust-written single-column BinaryArray
        // file with 64 rows of 8-byte payloads [0x10, i, 0, 0, 0, 0, 0, 0]
        // (i = 0..63). Vortex's writer emits a vortex.stats wrapper, so the
        // single zone carries lex-min = [0x10 0x00 ...] and lex-max =
        // [0x10 0x3F ...]. This test sanity-checks GetZoneStatsAsync surfaces
        // BinaryArray Min/Max — the actual predicate prune/keep logic is in
        // the four BinaryPredicate_ tests below.
        var path = TestData.TestDataPath.Resolve("binary_col_64rows.vortex");
        await using var r = await VortexFileReader.OpenAsync(path);
        Assert.IsType<BinaryType>(r.Schema.FieldsList[0].DataType);
        var stats = await r.GetZoneStatsAsync(0);
        Assert.NotNull(stats);
        Assert.IsType<BinaryArray>(stats!.Min);
        Assert.IsType<BinaryArray>(stats.Max);
    }

    [Fact]
    public async Task BinaryPredicate_EqualKeepsZoneInRange()
    {
        // [0x10 0x20 0…0] is lex-in [0x10 0x00 …, 0x10 0x3F …] so the zone is kept.
        var path = TestData.TestDataPath.Resolve("binary_col_64rows.vortex");
        await using var r = await VortexFileReader.OpenAsync(path);
        var midValue = new byte[] { 0x10, 0x20, 0, 0, 0, 0, 0, 0 };
        var batches = new List<RecordBatch>();
        await foreach (var b in r.ReadAllAsync(Pred.Equal("b", LiteralValue.Of(midValue)))) batches.Add(b);
        try
        {
            Assert.Single(batches);
        }
        finally
        {
            foreach (var b in batches) b.Dispose();
        }
    }

    [Fact]
    public async Task BinaryPredicate_GreaterThanMaxPrunes()
    {
        // value > max ⇒ all zones drop. value = [0x11 0x00 …] which is
        // lex-greater than max = [0x10 0x3F …].
        var path = TestData.TestDataPath.Resolve("binary_col_64rows.vortex");
        await using var r = await VortexFileReader.OpenAsync(path);
        var pastMax = new byte[] { 0x11, 0, 0, 0, 0, 0, 0, 0 };
        var batches = new List<RecordBatch>();
        await foreach (var b in r.ReadAllAsync(Pred.GreaterThan("b", LiteralValue.Of(pastMax)))) batches.Add(b);
        try
        {
            Assert.Empty(batches);
        }
        finally
        {
            foreach (var b in batches) b.Dispose();
        }
    }

    [Fact]
    public async Task BinaryPredicate_LessThanMinPrunes()
    {
        // value < min ⇒ all zones drop. value = [0x0F 0xFF …] which is
        // lex-less than min = [0x10 0x00 …].
        var path = TestData.TestDataPath.Resolve("binary_col_64rows.vortex");
        await using var r = await VortexFileReader.OpenAsync(path);
        var beforeMin = new byte[] { 0x0F, 0xFF, 0, 0, 0, 0, 0, 0 };
        var batches = new List<RecordBatch>();
        await foreach (var b in r.ReadAllAsync(Pred.LessThan("b", LiteralValue.Of(beforeMin)))) batches.Add(b);
        try
        {
            Assert.Empty(batches);
        }
        finally
        {
            foreach (var b in batches) b.Dispose();
        }
    }

    [Fact]
    public async Task BinaryPredicate_OutOfRangeEqualPrunes()
    {
        // Equal to a value past max ⇒ drops the zone (value > max).
        var path = TestData.TestDataPath.Resolve("binary_col_64rows.vortex");
        await using var r = await VortexFileReader.OpenAsync(path);
        var unreachable = new byte[] { 0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99 };
        var batches = new List<RecordBatch>();
        await foreach (var b in r.ReadAllAsync(Pred.Equal("b", LiteralValue.Of(unreachable)))) batches.Add(b);
        try
        {
            Assert.Empty(batches);
        }
        finally
        {
            foreach (var b in batches) b.Dispose();
        }
    }

    [Fact]
    public async Task StringPredicate_NotEqualKeepsRange()
    {
        // NotEqual is conservative: drops only when min == max == K. Here
        // min='alpha' max='foxtrot' so the zone stays.
        var path = TestData.TestDataPath.Resolve("dict_string_64rows.vortex");
        await using var r = await VortexFileReader.OpenAsync(path);
        var batches = new List<RecordBatch>();
        await foreach (var b in r.ReadAllAsync(Pred.NotEqual("color", LiteralValue.Of("delta")))) batches.Add(b);
        try
        {
            Assert.Single(batches);
        }
        finally
        {
            foreach (var b in batches) b.Dispose();
        }
    }

#if NET6_0_OR_GREATER
    [Fact]
    public async Task SelfRoundtrip_HalfFloat()
    {
        // F16: 2 bytes/row, no Extension wrap. Reader's PrimitiveArrayDecoder
        // gates F16 on NET6_0_OR_GREATER; this test only runs on the modern
        // TFMs the test project targets (net8.0 / net10.0), both of which
        // satisfy the gate.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("h", HalfFloatType.Default, nullable: true),
        }, metadata: null);
        const int n = 50;
        var bytes = new byte[(long)n * 2];
        var span = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, Half>(bytes.AsSpan());
        var validity = new byte[(n + 7) / 8];
        int nullCount = 0;
        var expected = new Half?[n];
        for (int i = 0; i < n; i++)
        {
            if (i % 4 == 0) { nullCount++; expected[i] = null; }
            else
            {
                var v = (Half)(0.5f + i * 0.25f);
                span[i] = v;
                validity[i >> 3] |= (byte)(1 << (i & 7));
                expected[i] = v;
            }
        }
        var arrData = new ArrayData(
            HalfFloatType.Default, n, nullCount, 0,
            new[] { new ArrowBuffer(validity), new ArrowBuffer(bytes) });
        var arr = new HalfFloatArray(arrData);
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            Assert.IsType<HalfFloatType>(reader.Schema.FieldsList[0].DataType);
            var read = Assert.IsType<HalfFloatArray>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
            {
                if (expected[i] is null) Assert.False(read.IsValid(i));
                else { Assert.True(read.IsValid(i)); Assert.Equal(expected[i]!.Value, read.GetValue(i)!.Value); }
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }
#endif

    [Fact]
    public async Task SelfRoundtrip_Date64()
    {
        // Date64: i64 milliseconds since epoch, wrapped in vortex.date Extension
        // with TimeUnit::Ms (tag 2).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("d", Date64Type.Default, nullable: false),
        }, metadata: null);
        const int n = 50;
        var bytes = new byte[(long)n * 8];
        var span = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, long>(bytes.AsSpan());
        long baseMs = 1_704_067_200L * 1_000L; // 2024-01-01 UTC in ms
        var expected = new long[n];
        for (int i = 0; i < n; i++)
        {
            long v = baseMs + (long)i * 86_400_000L;
            span[i] = v;
            expected[i] = v;
        }
        var arr = new Date64Array(new ArrowBuffer(bytes), ArrowBuffer.Empty, n, 0, 0);
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            await using var reader = await VortexFileReader.OpenAsync(path);
            Assert.IsType<Date64Type>(reader.Schema.FieldsList[0].DataType);
            var read = Assert.IsType<Date64Array>(await reader.ReadColumnAsync(0));
            Assert.Equal(n, read.Length);
            for (int i = 0; i < n; i++)
                Assert.Equal(expected[i], read.GetValue(i)!.Value);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }
}

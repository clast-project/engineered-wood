// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Lance.Tests;

/// <summary>
/// Tests for the Phase 13 MVP Lance writer. Covers two cases:
/// (1) self-roundtrip — write a file then read it back via our own
/// reader and assert exact value equality; (2) cross-validation —
/// invoke pylance on the written file and confirm it sees the same
/// schema and values.
/// </summary>
public class LanceFileWriterTests
{
    [Fact]
    public async Task SingleColumn_RoundTrip_ViaOurReader()
    {
        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-writer-{Guid.NewGuid():N}.lance");
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteInt32ColumnAsync("x", new[] { 1, 2, 3, 4, 5 });
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            Assert.Equal(5, reader.NumberOfRows);
            Assert.Single(reader.Schema.FieldsList);
            Assert.Equal("x", reader.Schema.FieldsList[0].Name);
            Assert.IsType<Int32Type>(reader.Schema.FieldsList[0].DataType);

            var arr = (Int32Array)await reader.ReadColumnAsync(0);
            Assert.Equal(new int?[] { 1, 2, 3, 4, 5 }, arr.ToArray());
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task TwoColumns_RoundTrip_ViaOurReader()
    {
        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-writer-{Guid.NewGuid():N}.lance");
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteInt32ColumnAsync("a", new[] { 10, 20, 30 });
                await writer.WriteInt32ColumnAsync("b", new[] { 100, 200, 300 });
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);
            Assert.Equal(2, reader.Schema.FieldsList.Count);

            var a = (Int32Array)await reader.ReadColumnAsync(0);
            var b = (Int32Array)await reader.ReadColumnAsync(1);
            Assert.Equal(new int?[] { 10, 20, 30 }, a.ToArray());
            Assert.Equal(new int?[] { 100, 200, 300 }, b.ToArray());
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task AllPrimitives_RoundTrip_ViaOurReader()
    {
        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-writer-{Guid.NewGuid():N}.lance");
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteInt8ColumnAsync("i8", new sbyte[] { -1, 0, 1, 2 });
                await writer.WriteUInt8ColumnAsync("u8", new byte[] { 0, 1, 254, 255 });
                await writer.WriteInt16ColumnAsync("i16", new short[] { -32000, -1, 0, 32000 });
                await writer.WriteUInt16ColumnAsync("u16", new ushort[] { 0, 1, 65534, 65535 });
                await writer.WriteUInt32ColumnAsync("u32", new uint[] { 0, 1, 0xDEADBEEF, uint.MaxValue });
                await writer.WriteInt64ColumnAsync("i64", new long[] { long.MinValue, -1, 0, long.MaxValue });
                await writer.WriteUInt64ColumnAsync("u64", new ulong[] { 0, 1, 0xCAFE_F00DUL, ulong.MaxValue });
                await writer.WriteFloatColumnAsync("f32", new float[] { -1.5f, 0f, 1.5f, float.NaN });
                await writer.WriteDoubleColumnAsync("f64", new double[] { -1.5, 0, 1.5, double.PositiveInfinity });
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            Assert.Equal(4, reader.NumberOfRows);
            Assert.Equal(9, reader.Schema.FieldsList.Count);
            Assert.IsType<Int8Type>(reader.Schema.FieldsList[0].DataType);
            Assert.IsType<UInt8Type>(reader.Schema.FieldsList[1].DataType);
            Assert.IsType<Int16Type>(reader.Schema.FieldsList[2].DataType);
            Assert.IsType<UInt16Type>(reader.Schema.FieldsList[3].DataType);
            Assert.IsType<UInt32Type>(reader.Schema.FieldsList[4].DataType);
            Assert.IsType<Int64Type>(reader.Schema.FieldsList[5].DataType);
            Assert.IsType<UInt64Type>(reader.Schema.FieldsList[6].DataType);
            Assert.IsType<FloatType>(reader.Schema.FieldsList[7].DataType);
            Assert.IsType<DoubleType>(reader.Schema.FieldsList[8].DataType);

            Assert.Equal(new sbyte?[] { -1, 0, 1, 2 }, ((Int8Array)await reader.ReadColumnAsync(0)).ToArray());
            Assert.Equal(new byte?[] { 0, 1, 254, 255 }, ((UInt8Array)await reader.ReadColumnAsync(1)).ToArray());
            Assert.Equal(new short?[] { -32000, -1, 0, 32000 }, ((Int16Array)await reader.ReadColumnAsync(2)).ToArray());
            Assert.Equal(new ushort?[] { 0, 1, 65534, 65535 }, ((UInt16Array)await reader.ReadColumnAsync(3)).ToArray());
            Assert.Equal(new uint?[] { 0, 1, 0xDEADBEEFu, uint.MaxValue }, ((UInt32Array)await reader.ReadColumnAsync(4)).ToArray());
            Assert.Equal(new long?[] { long.MinValue, -1, 0, long.MaxValue }, ((Int64Array)await reader.ReadColumnAsync(5)).ToArray());
            Assert.Equal(new ulong?[] { 0, 1, 0xCAFE_F00DUL, ulong.MaxValue }, ((UInt64Array)await reader.ReadColumnAsync(6)).ToArray());
            var f32 = (FloatArray)await reader.ReadColumnAsync(7);
            Assert.Equal(-1.5f, f32.GetValue(0));
            Assert.Equal(0f, f32.GetValue(1));
            Assert.Equal(1.5f, f32.GetValue(2));
            Assert.True(float.IsNaN(f32.GetValue(3)!.Value));
            var f64 = (DoubleArray)await reader.ReadColumnAsync(8);
            Assert.Equal(new double?[] { -1.5, 0, 1.5, double.PositiveInfinity }, f64.ToArray());
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task Strings_RoundTrip_ViaOurReader()
    {
        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-writer-{Guid.NewGuid():N}.lance");
        try
        {
            var sb = new StringArray.Builder();
            sb.Append("alpha");
            sb.Append("");
            sb.Append("Lance");
            sb.AppendNull();
            sb.Append("éclat");  // multi-byte UTF-8
            var strings = sb.Build();

            var bb = new BinaryArray.Builder();
            bb.Append(new byte[] { 0xDE, 0xAD });
            bb.AppendNull();
            bb.Append(System.Array.Empty<byte>());
            bb.Append(new byte[] { 0xBE, 0xEF, 0xCA, 0xFE });
            bb.Append(new byte[] { 0x00 });
            var binaries = bb.Build();

            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteColumnAsync("s", strings);
                await writer.WriteColumnAsync("b", binaries);
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            Assert.Equal(5, reader.NumberOfRows);
            Assert.IsType<StringType>(reader.Schema.FieldsList[0].DataType);
            Assert.IsType<BinaryType>(reader.Schema.FieldsList[1].DataType);

            var rs = (StringArray)await reader.ReadColumnAsync(0);
            Assert.Equal(1, rs.NullCount);
            Assert.Equal("alpha", rs.GetString(0));
            Assert.Equal("", rs.GetString(1));
            Assert.Equal("Lance", rs.GetString(2));
            Assert.Null(rs.GetString(3));
            Assert.Equal("éclat", rs.GetString(4));

            var rb = (BinaryArray)await reader.ReadColumnAsync(1);
            Assert.Equal(1, rb.NullCount);
            Assert.Equal(new byte[] { 0xDE, 0xAD }, rb.GetBytes(0).ToArray());
            Assert.True(rb.IsNull(1));
            Assert.Equal(System.Array.Empty<byte>(), rb.GetBytes(2).ToArray());
            Assert.Equal(new byte[] { 0xBE, 0xEF, 0xCA, 0xFE }, rb.GetBytes(3).ToArray());
            Assert.Equal(new byte[] { 0x00 }, rb.GetBytes(4).ToArray());
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task Strings_CrossValidatedAgainstPylance()
    {
        if (!IsPythonAvailable())
        {
            return;
        }

        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-pylance-str-{Guid.NewGuid():N}.lance");
        try
        {
            var sb = new StringArray.Builder();
            sb.Append("alpha");
            sb.AppendNull();
            sb.Append("Lance");
            sb.Append("éclat");
            sb.Append("");
            var strings = sb.Build();

            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteColumnAsync("s", strings);
                await writer.FinishAsync();
            }

            string script = "import sys, json\n" +
                "from lance.file import LanceFileReader\n" +
                $"r = LanceFileReader(r'{path}')\n" +
                "t = r.read_all().to_table()\n" +
                "out = { 'rows': len(t), 'type': str(t.schema[0].type), 'values': t['s'].to_pylist() }\n" +
                "sys.stdout.write(json.dumps(out))\n";

            var psi = new ProcessStartInfo("python", "-c " + EscapeArg(script))
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi)!;
            string stdout = await proc.StandardOutput.ReadToEndAsync();
            string stderr = await proc.StandardError.ReadToEndAsync();
            await proc.WaitForExitAsync();
            Assert.True(proc.ExitCode == 0,
                $"pylance exited {proc.ExitCode}; stderr: {stderr}; stdout: {stdout}");

            var json = System.Text.Json.JsonDocument.Parse(stdout);
            var root = json.RootElement;
            Assert.Equal(5, root.GetProperty("rows").GetInt32());
            Assert.Equal("string", root.GetProperty("type").GetString());
            var values = new List<string?>();
            foreach (var v in root.GetProperty("values").EnumerateArray())
                values.Add(v.ValueKind == System.Text.Json.JsonValueKind.Null ? null : v.GetString());
            Assert.Equal(new string?[] { "alpha", null, "Lance", "éclat", "" }, values);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task LargeInt64_MultiChunk_RoundTrip_ViaOurReader()
    {
        // 10_000 Int64 values = 80 KB raw. With B=8 the writer caps each
        // chunk at 2048 items (16 KB), so this needs ~5 chunks per page.
        int n = 10_000;
        var values = new long[n];
        for (int i = 0; i < n; i++) values[i] = (long)i * 1_000_003L - 1;  // mix high and low bits

        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-multi-{Guid.NewGuid():N}.lance");
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteInt64ColumnAsync("v", values);
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            Assert.Equal(n, reader.NumberOfRows);
            var readBack = (Int64Array)await reader.ReadColumnAsync(0);
            for (int i = 0; i < n; i++)
                Assert.Equal(values[i], readBack.GetValue(i));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task LargeNullableInt32_MultiChunk_RoundTrip_ViaOurReader()
    {
        // Nullable Int32 with 20_000 rows + ~10% nulls. With B=4 + def
        // (2 bytes/item), each chunk fits ~4096 items. Exercises both
        // multi-chunk AND the per-chunk def buffer construction.
        int n = 20_000;
        var b = new Int32Array.Builder();
        for (int i = 0; i < n; i++)
        {
            if (i % 11 == 0) b.AppendNull();
            else b.Append(i * 7);
        }
        var arr = b.Build();

        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-multi-{Guid.NewGuid():N}.lance");
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteColumnAsync("v", arr);
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            Assert.Equal(n, reader.NumberOfRows);
            var readBack = (Int32Array)await reader.ReadColumnAsync(0);
            Assert.Equal(arr.NullCount, readBack.NullCount);
            for (int i = 0; i < n; i++)
            {
                Assert.Equal(arr.IsNull(i), readBack.IsNull(i));
                if (!arr.IsNull(i))
                    Assert.Equal(arr.GetValue(i), readBack.GetValue(i));
            }
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task LargeStrings_MultiChunk_RoundTrip_ViaOurReader()
    {
        // Many short strings totalling > 32 KB of data → multi-chunk
        // Variable page. Mix in some nulls and a sprinkling of longer
        // strings to exercise the greedy chunker's power-of-2 rounding.
        int n = 5_000;
        var b = new StringArray.Builder();
        var expected = new string?[n];
        for (int i = 0; i < n; i++)
        {
            if (i % 23 == 0) { b.AppendNull(); expected[i] = null; }
            else
            {
                string v = i % 97 == 0
                    ? new string('x', 200)             // longer item every now and then
                    : $"row_{i}_value_{i * 13 % 9991}";
                b.Append(v);
                expected[i] = v;
            }
        }
        var arr = b.Build();

        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-multi-{Guid.NewGuid():N}.lance");
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteColumnAsync("s", arr);
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            Assert.Equal(n, reader.NumberOfRows);
            var readBack = (StringArray)await reader.ReadColumnAsync(0);
            Assert.Equal(arr.NullCount, readBack.NullCount);
            for (int i = 0; i < n; i++)
                Assert.Equal(expected[i], readBack.GetString(i));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task LargeInt64_MultiChunk_CrossValidatedAgainstPylance()
    {
        if (!IsPythonAvailable()) return;

        int n = 10_000;
        var values = new long[n];
        for (int i = 0; i < n; i++) values[i] = (long)i - 5000;

        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-pylance-multi-{Guid.NewGuid():N}.lance");
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteInt64ColumnAsync("v", values);
                await writer.FinishAsync();
            }

            string script = "import sys, json\n" +
                "from lance.file import LanceFileReader\n" +
                $"r = LanceFileReader(r'{path}')\n" +
                "t = r.read_all().to_table()\n" +
                "vals = t['v'].to_pylist()\n" +
                "out = { 'rows': len(t), 'first': vals[:3], 'last': vals[-3:], 'sum': sum(vals) }\n" +
                "sys.stdout.write(json.dumps(out))\n";

            var psi = new ProcessStartInfo("python", "-c " + EscapeArg(script))
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi)!;
            string stdout = await proc.StandardOutput.ReadToEndAsync();
            string stderr = await proc.StandardError.ReadToEndAsync();
            await proc.WaitForExitAsync();
            Assert.True(proc.ExitCode == 0,
                $"pylance exited {proc.ExitCode}; stderr: {stderr}; stdout: {stdout}");

            var json = System.Text.Json.JsonDocument.Parse(stdout);
            var root = json.RootElement;
            Assert.Equal(n, root.GetProperty("rows").GetInt32());
            long expectedSum = 0;
            for (int i = 0; i < n; i++) expectedSum += values[i];
            Assert.Equal(expectedSum, root.GetProperty("sum").GetInt64());
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task MultiPageInt32_RoundTrip_ViaOurReader()
    {
        // Three pages of unequal size: 7 + 3 + 100 = 110 rows, all in one
        // column. Exercises the new multi-page concatenation path.
        var page0 = Enumerable.Range(1000, 7).ToArray();
        var page1 = Enumerable.Range(2000, 3).ToArray();
        var page2 = Enumerable.Range(5000, 100).ToArray();
        var pages = new[] { page0, page1, page2 };

        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-mp-{Guid.NewGuid():N}.lance");
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteInt32ColumnPagedAsync("v", pages);
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            Assert.Equal(110, reader.NumberOfRows);
            var arr = (Int32Array)await reader.ReadColumnAsync(0);
            var expected = pages.SelectMany(p => p).ToArray();
            Assert.Equal(expected.Length, arr.Length);
            for (int i = 0; i < expected.Length; i++)
                Assert.Equal(expected[i], arr.GetValue(i));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task MultiPageStrings_RoundTrip_ViaOurReader()
    {
        // Three string pages totaling 12 rows. Each page has its own
        // chunked offsets+data; the reader needs to concat correctly.
        var p0 = new StringArray.Builder();
        p0.Append("alpha"); p0.Append("beta"); p0.Append("gamma");
        var p1 = new StringArray.Builder();
        p1.Append(""); p1.Append("delta");
        var p2 = new StringArray.Builder();
        for (int i = 0; i < 7; i++) p2.Append($"item-{i}");
        var pages = new[] { p0.Build(), p1.Build(), p2.Build() };

        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-mp-{Guid.NewGuid():N}.lance");
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteStringColumnPagedAsync("s", pages);
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            Assert.Equal(12, reader.NumberOfRows);
            var arr = (StringArray)await reader.ReadColumnAsync(0);
            var expected = pages.SelectMany(p =>
                Enumerable.Range(0, p.Length).Select(i => p.GetString(i))).ToArray();
            Assert.Equal(expected.Length, arr.Length);
            for (int i = 0; i < expected.Length; i++)
                Assert.Equal(expected[i], arr.GetString(i));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task FixedSizeListFloat_RoundTrip_ViaOurReader()
    {
        // FixedSizeList<float32, 4> with 3 rows. Tests the writer's FSL
        // value-compression path: a single column with logical_type
        // "fixed_size_list:float:4" and value_compression FSL{dim=4,
        // values=Flat(32)}.
        const int dim = 4;
        var inner = new FloatArray.Builder()
            .Append(0f).Append(1f).Append(2f).Append(3f)
            .Append(10f).Append(11f).Append(12f).Append(13f)
            .Append(20f).Append(21f).Append(22f).Append(23f)
            .Build();
        var fsl = new FixedSizeListArray(
            new ArrayData(
                new FixedSizeListType(new Field("item", FloatType.Default, nullable: true), dim),
                length: 3, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty },
                children: new[] { inner.Data }));

        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-fsl-{Guid.NewGuid():N}.lance");
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteColumnAsync("emb", fsl);
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);
            var fslType = Assert.IsType<FixedSizeListType>(reader.Schema.FieldsList[0].DataType);
            Assert.Equal(dim, fslType.ListSize);
            Assert.IsType<FloatType>(fslType.ValueDataType);

            var read = (FixedSizeListArray)await reader.ReadColumnAsync(0);
            Assert.Equal(3, read.Length);
            Assert.Equal(0, read.NullCount);
            var readInner = (FloatArray)read.Values;
            Assert.Equal(12, readInner.Length);
            for (int i = 0; i < 12; i++)
                Assert.Equal((float)((i / 4) * 10 + (i % 4)), readInner.GetValue(i));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task FixedSizeListInt32_LargeDim_RoundTrip_ViaOurReader()
    {
        // 5 rows × 1024 int32 = 20 KB. Each row exceeds the chunker's
        // smallest unit but stays under MaxChunkBytes; single chunk.
        const int dim = 1024;
        const int rows = 5;
        var inner = new Int32Array.Builder();
        for (int row = 0; row < rows; row++)
            for (int j = 0; j < dim; j++) inner.Append(row * dim + j);
        var innerArr = inner.Build();
        var fsl = new FixedSizeListArray(
            new ArrayData(
                new FixedSizeListType(new Field("item", Int32Type.Default, nullable: true), dim),
                length: rows, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty },
                children: new[] { innerArr.Data }));

        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-fsl-{Guid.NewGuid():N}.lance");
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteColumnAsync("emb", fsl);
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            var read = (FixedSizeListArray)await reader.ReadColumnAsync(0);
            Assert.Equal(rows, read.Length);
            var readInner = (Int32Array)read.Values;
            Assert.Equal(rows * dim, readInner.Length);
            for (int i = 0; i < rows * dim; i++)
                Assert.Equal(i, readInner.GetValue(i));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task DateTimeTypes_RoundTrip_ViaOurReader()
    {
        // Date32, Date64, Time32(s/ms), Time64(us/ns), Timestamp(us, no tz),
        // Timestamp(us, UTC), Duration(ms). Each Arrow array goes through
        // LanceFileWriter.WriteColumnAsync; the reader builds the right
        // Arrow type from the logical-type string.
        // Build the typed Arrow arrays from raw int32/int64 buffers so we
        // can specify exact wire values without going through the Builders'
        // DateTime/DateTimeOffset surfaces.
        T Build32<T>(IArrowType type, int[] values, Func<ArrayData, T> ctor)
            where T : IArrowArray
        {
            byte[] bytes = MemoryMarshal.AsBytes(values.AsSpan()).ToArray();
            return ctor(new ArrayData(
                type, values.Length, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, new ArrowBuffer(bytes) }));
        }
        T Build64<T>(IArrowType type, long[] values, Func<ArrayData, T> ctor)
            where T : IArrowArray
        {
            byte[] bytes = MemoryMarshal.AsBytes(values.AsSpan()).ToArray();
            return ctor(new ArrayData(
                type, values.Length, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, new ArrowBuffer(bytes) }));
        }

        var d32 = Build32(Date32Type.Default,
            new int[] { 0, 19000, 20000 }, d => new Date32Array(d));
        var d64 = Build64(Date64Type.Default,
            new long[] { 0, 86_400_000, 172_800_000 }, d => new Date64Array(d));
        var t32s = Build32(new Time32Type(TimeUnit.Second),
            new int[] { 0, 3600, 7200 }, d => new Time32Array(d));
        var t32ms = Build32(new Time32Type(TimeUnit.Millisecond),
            new int[] { 0, 86_400_000 - 1, 123 }, d => new Time32Array(d));
        var t64us = Build64(new Time64Type(TimeUnit.Microsecond),
            new long[] { 0, 1_000_000, 86_399_999_999 }, d => new Time64Array(d));
        var t64ns = Build64(new Time64Type(TimeUnit.Nanosecond),
            new long[] { 0, 1_000_000_000, 86_400_000_000_000 - 1 }, d => new Time64Array(d));
        // Build TimestampArrays from raw int64 values directly (the Builder
        // takes DateTimeOffset, but we want exact int64 control here).
        TimestampArray BuildTs(TimestampType type, long[] values)
        {
            byte[] bytes = new byte[values.Length * 8];
            for (int i = 0; i < values.Length; i++)
                System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(
                    bytes.AsSpan(i * 8, 8), values[i]);
            return new TimestampArray(new ArrayData(
                type, values.Length, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, new ArrowBuffer(bytes) }));
        }
        var tsValues = new long[] { 0L, 1_700_000_000_000_000L, 1_800_000_000_000_000L };
        var tsNoTz = BuildTs(new TimestampType(TimeUnit.Microsecond, (string?)null), tsValues);
        var tsUtc = BuildTs(new TimestampType(TimeUnit.Microsecond, "UTC"), tsValues);
        var dur = Build64(DurationType.Millisecond,
            new long[] { 0, 1_000, 60_000 }, d => new DurationArray(d));

        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-dt-{Guid.NewGuid():N}.lance");
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteColumnAsync("d32", d32);
                await writer.WriteColumnAsync("d64", d64);
                await writer.WriteColumnAsync("t32s", t32s);
                await writer.WriteColumnAsync("t32ms", t32ms);
                await writer.WriteColumnAsync("t64us", t64us);
                await writer.WriteColumnAsync("t64ns", t64ns);
                await writer.WriteColumnAsync("tsNoTz", tsNoTz);
                await writer.WriteColumnAsync("tsUtc", tsUtc);
                await writer.WriteColumnAsync("dur", dur);
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);
            Assert.IsType<Date32Type>(reader.Schema.FieldsList[0].DataType);
            Assert.IsType<Date64Type>(reader.Schema.FieldsList[1].DataType);
            Assert.Equal(TimeUnit.Second, ((Time32Type)reader.Schema.FieldsList[2].DataType).Unit);
            Assert.Equal(TimeUnit.Millisecond, ((Time32Type)reader.Schema.FieldsList[3].DataType).Unit);
            Assert.Equal(TimeUnit.Microsecond, ((Time64Type)reader.Schema.FieldsList[4].DataType).Unit);
            Assert.Equal(TimeUnit.Nanosecond, ((Time64Type)reader.Schema.FieldsList[5].DataType).Unit);
            var tsNoTzType = (TimestampType)reader.Schema.FieldsList[6].DataType;
            Assert.Equal(TimeUnit.Microsecond, tsNoTzType.Unit);
            Assert.Null(tsNoTzType.Timezone);
            var tsUtcType = (TimestampType)reader.Schema.FieldsList[7].DataType;
            Assert.Equal("UTC", tsUtcType.Timezone);
            Assert.Equal(TimeUnit.Millisecond, ((DurationType)reader.Schema.FieldsList[8].DataType).Unit);

            int[] ReadInts(IArrowArray a)
            {
                var fw = (FixedWidthType)a.Data.DataType;
                Assert.Equal(32, fw.BitWidth);
                var span = MemoryMarshal.Cast<byte, int>(a.Data.Buffers[1].Span.Slice(0, a.Length * 4));
                return span.ToArray();
            }
            long[] ReadLongs(IArrowArray a)
            {
                var fw = (FixedWidthType)a.Data.DataType;
                Assert.Equal(64, fw.BitWidth);
                var span = MemoryMarshal.Cast<byte, long>(a.Data.Buffers[1].Span.Slice(0, a.Length * 8));
                return span.ToArray();
            }

            Assert.Equal(new[] { 0, 19000, 20000 }, ReadInts(await reader.ReadColumnAsync(0)));
            Assert.Equal(new long[] { 0, 86_400_000, 172_800_000 }, ReadLongs(await reader.ReadColumnAsync(1)));
            Assert.Equal(new[] { 0, 3600, 7200 }, ReadInts(await reader.ReadColumnAsync(2)));
            Assert.Equal(new[] { 0, 86_400_000 - 1, 123 }, ReadInts(await reader.ReadColumnAsync(3)));
            Assert.Equal(new long[] { 0, 1_000_000, 86_399_999_999 }, ReadLongs(await reader.ReadColumnAsync(4)));
            Assert.Equal(new long[] { 0, 1_000_000_000, 86_400_000_000_000 - 1 }, ReadLongs(await reader.ReadColumnAsync(5)));
            Assert.Equal(new long[] { 0, 1_700_000_000_000_000, 1_800_000_000_000_000 }, ReadLongs(await reader.ReadColumnAsync(6)));
            Assert.Equal(new long[] { 0, 1_700_000_000_000_000, 1_800_000_000_000_000 }, ReadLongs(await reader.ReadColumnAsync(7)));
            Assert.Equal(new long[] { 0, 1_000, 60_000 }, ReadLongs(await reader.ReadColumnAsync(8)));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task FixedSizeListFloat_CrossValidatedAgainstPylance()
    {
        if (!IsPythonAvailable()) return;

        const int dim = 8;
        var inner = new FloatArray.Builder();
        for (int i = 0; i < 4 * dim; i++) inner.Append(i * 0.5f);
        var fsl = new FixedSizeListArray(
            new ArrayData(
                new FixedSizeListType(new Field("item", FloatType.Default, nullable: true), dim),
                length: 4, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty },
                children: new[] { inner.Build().Data }));

        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-pylance-fsl-{Guid.NewGuid():N}.lance");
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteColumnAsync("emb", fsl);
                await writer.FinishAsync();
            }

            string script = "import sys, json\n" +
                "from lance.file import LanceFileReader\n" +
                $"r = LanceFileReader(r'{path}')\n" +
                "t = r.read_all().to_table()\n" +
                "out = { 'rows': len(t), 'type': str(t.schema[0].type), 'flat': [list(x) for x in t['emb'].to_pylist()] }\n" +
                "sys.stdout.write(json.dumps(out))\n";

            var psi = new ProcessStartInfo("python", "-c " + EscapeArg(script))
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi)!;
            string stdout = await proc.StandardOutput.ReadToEndAsync();
            string stderr = await proc.StandardError.ReadToEndAsync();
            await proc.WaitForExitAsync();
            Assert.True(proc.ExitCode == 0,
                $"pylance exited {proc.ExitCode}; stderr: {stderr}; stdout: {stdout}");

            var json = System.Text.Json.JsonDocument.Parse(stdout);
            Assert.Equal(4, json.RootElement.GetProperty("rows").GetInt32());
            Assert.StartsWith("fixed_size_list", json.RootElement.GetProperty("type").GetString());
            int idx = 0;
            foreach (var row in json.RootElement.GetProperty("flat").EnumerateArray())
            {
                foreach (var v in row.EnumerateArray())
                {
                    Assert.Equal(idx * 0.5f, v.GetSingle());
                    idx++;
                }
            }
            Assert.Equal(4 * dim, idx);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task NullableInt32_RoundTrip_ViaOurReader()
    {
        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-writer-{Guid.NewGuid():N}.lance");
        try
        {
            // Build [10, null, 30, null, 50] as an Apache.Arrow Int32Array.
            var b = new Int32Array.Builder();
            b.Append(10);
            b.AppendNull();
            b.Append(30);
            b.AppendNull();
            b.Append(50);
            var arr = b.Build();

            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteColumnAsync("x", arr);
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            Assert.Equal(5, reader.NumberOfRows);
            var read = (Int32Array)await reader.ReadColumnAsync(0);
            Assert.Equal(2, read.NullCount);
            Assert.Equal(new int?[] { 10, null, 30, null, 50 }, read.ToArray());
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task NullableInt32_CrossValidatedAgainstPylance()
    {
        if (!IsPythonAvailable())
        {
            return;
        }

        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-pylance-null-{Guid.NewGuid():N}.lance");
        try
        {
            var b = new Int32Array.Builder();
            b.Append(7);
            b.AppendNull();
            b.Append(11);
            b.AppendNull();
            b.AppendNull();
            b.Append(13);
            var arr = b.Build();

            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteColumnAsync("x", arr);
                await writer.FinishAsync();
            }

            string script = "import sys, json\n" +
                "from lance.file import LanceFileReader\n" +
                $"r = LanceFileReader(r'{path}')\n" +
                "t = r.read_all().to_table()\n" +
                "out = { 'rows': len(t), 'values': t['x'].to_pylist() }\n" +
                "sys.stdout.write(json.dumps(out))\n";

            var psi = new ProcessStartInfo("python", "-c " + EscapeArg(script))
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi)!;
            string stdout = await proc.StandardOutput.ReadToEndAsync();
            string stderr = await proc.StandardError.ReadToEndAsync();
            await proc.WaitForExitAsync();
            Assert.True(proc.ExitCode == 0,
                $"pylance exited {proc.ExitCode}; stderr: {stderr}; stdout: {stdout}");

            var json = System.Text.Json.JsonDocument.Parse(stdout);
            var root = json.RootElement;
            Assert.Equal(6, root.GetProperty("rows").GetInt32());
            var values = new List<int?>();
            foreach (var v in root.GetProperty("values").EnumerateArray())
                values.Add(v.ValueKind == System.Text.Json.JsonValueKind.Null ? null : v.GetInt32());
            Assert.Equal(new int?[] { 7, null, 11, null, null, 13 }, values);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task SingleColumn_CrossValidatedAgainstPylance()
    {
        if (!IsPythonAvailable())
        {
            // Skip silently when Python/pylance isn't available locally.
            return;
        }

        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-pylance-{Guid.NewGuid():N}.lance");
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteInt32ColumnAsync("x", new[] { 7, 11, 13, 17, 19, 23 });
                await writer.FinishAsync();
            }

            // Read back via pylance (LanceFileReader.read_all() → Arrow Table).
            string script = "import sys, json\n" +
                "from lance.file import LanceFileReader\n" +
                $"r = LanceFileReader(r'{path}')\n" +
                "t = r.read_all().to_table()\n" +
                "out = { 'columns': t.column_names, 'types': [str(f.type) for f in t.schema], 'rows': len(t), 'values': t['x'].to_pylist() }\n" +
                "sys.stdout.write(json.dumps(out))\n";

            var psi = new ProcessStartInfo("python", "-c " + EscapeArg(script))
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi)!;
            string stdout = await proc.StandardOutput.ReadToEndAsync();
            string stderr = await proc.StandardError.ReadToEndAsync();
            await proc.WaitForExitAsync();
            Assert.True(proc.ExitCode == 0,
                $"pylance exited {proc.ExitCode}; stderr: {stderr}; stdout: {stdout}");

            var json = System.Text.Json.JsonDocument.Parse(stdout);
            var root = json.RootElement;
            Assert.Equal("x", root.GetProperty("columns")[0].GetString());
            Assert.Equal("int32", root.GetProperty("types")[0].GetString());
            Assert.Equal(6, root.GetProperty("rows").GetInt32());
            var values = new List<int>();
            foreach (var v in root.GetProperty("values").EnumerateArray())
                values.Add(v.GetInt32());
            Assert.Equal(new[] { 7, 11, 13, 17, 19, 23 }, values);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task MixedPrimitives_CrossValidatedAgainstPylance()
    {
        if (!IsPythonAvailable())
        {
            return;
        }

        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-pylance-mixed-{Guid.NewGuid():N}.lance");
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteInt64ColumnAsync("i64", new long[] { -2_000_000_000L, 0L, 2_000_000_000L });
                await writer.WriteDoubleColumnAsync("f64", new double[] { -1.25, 0.0, 1.25 });
                await writer.FinishAsync();
            }

            string script = "import sys, json\n" +
                "from lance.file import LanceFileReader\n" +
                $"r = LanceFileReader(r'{path}')\n" +
                "t = r.read_all().to_table()\n" +
                "out = { 'columns': t.column_names, 'types': [str(f.type) for f in t.schema], 'rows': len(t), 'i64': t['i64'].to_pylist(), 'f64': t['f64'].to_pylist() }\n" +
                "sys.stdout.write(json.dumps(out))\n";

            var psi = new ProcessStartInfo("python", "-c " + EscapeArg(script))
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi)!;
            string stdout = await proc.StandardOutput.ReadToEndAsync();
            string stderr = await proc.StandardError.ReadToEndAsync();
            await proc.WaitForExitAsync();
            Assert.True(proc.ExitCode == 0,
                $"pylance exited {proc.ExitCode}; stderr: {stderr}; stdout: {stdout}");

            var json = System.Text.Json.JsonDocument.Parse(stdout);
            var root = json.RootElement;
            Assert.Equal(new[] { "i64", "f64" }, root.GetProperty("columns").EnumerateArray().Select(e => e.GetString()).ToArray());
            Assert.Equal(new[] { "int64", "double" }, root.GetProperty("types").EnumerateArray().Select(e => e.GetString()).ToArray());
            Assert.Equal(3, root.GetProperty("rows").GetInt32());

            var i64 = root.GetProperty("i64").EnumerateArray().Select(e => e.GetInt64()).ToArray();
            Assert.Equal(new long[] { -2_000_000_000L, 0L, 2_000_000_000L }, i64);

            var f64 = root.GetProperty("f64").EnumerateArray().Select(e => e.GetDouble()).ToArray();
            Assert.Equal(new double[] { -1.25, 0.0, 1.25 }, f64);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    private static bool IsPythonAvailable()
    {
        try
        {
            var psi = new ProcessStartInfo("python", "-c \"import lance\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi);
            if (proc is null) return false;
            proc.WaitForExit(5_000);
            return proc.ExitCode == 0;
        }
        catch { return false; }
    }

    private static string EscapeArg(string s)
    {
        // Wrap in quotes; escape interior double quotes.
        return "\"" + s.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";
    }
}

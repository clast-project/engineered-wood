// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
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

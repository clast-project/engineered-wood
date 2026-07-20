// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Diagnostics;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.Tests;

/// <summary>
/// Cross-validates ORC files written by Storc against PyArrow's ORC reader.
/// </summary>
public class CrossValidationTests
{
    private static string GetTempPath() => Path.Combine(Path.GetTempPath(), $"storc_xval_{Guid.NewGuid():N}.orc");

    private static string? FindPythonExe()
    {
        foreach (var candidate in new[] { "python3", "python" })
        {
            try
            {
                var psi = new ProcessStartInfo(candidate, "--version")
                {
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                };
                var p = Process.Start(psi)!;
                p.WaitForExit(3000);
                if (p.ExitCode == 0)
                    return candidate;
            }
            catch { }
        }

        var home = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        var pythonDir = Path.Combine(home, "Programs", "Python");
        if (Directory.Exists(pythonDir))
        {
            foreach (var dir in Directory.EnumerateDirectories(pythonDir, "Python*"))
            {
                var exe = Path.Combine(dir, "python.exe");
                if (File.Exists(exe))
                    return exe;
            }
        }

        return null;
    }

    private static readonly string? PythonExe = FindPythonExe();

    private static readonly string? TzDataDir = FindTzDataDir();

    private static string? FindTzDataDir()
    {
        if (PythonExe == null) return null;
        try
        {
            var psi = new ProcessStartInfo(PythonExe,
                "-c \"import os, tzdata; print(os.path.join(os.path.dirname(tzdata.__file__), 'zoneinfo'))\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            var p = Process.Start(psi)!;
            var stdout = p.StandardOutput.ReadToEnd().Trim();
            p.WaitForExit(5000);
            if (p.ExitCode == 0 && Directory.Exists(stdout))
                return stdout;
        }
        catch { }
        return null;
    }

    /// <summary>
    /// Creates a ProcessStartInfo for running a Python script, setting TZDIR
    /// so PyArrow's ORC reader can find timezone data on Windows.
    /// </summary>
    private static ProcessStartInfo PythonPsi(string arguments)
    {
        var psi = new ProcessStartInfo(PythonExe!, arguments)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        if (TzDataDir != null && !psi.Environment.ContainsKey("TZDIR"))
        {
            psi.Environment["TZDIR"] = TzDataDir;
        }

        return psi;
    }

    private static bool IsPyArrowAvailable()
    {
        if (PythonExe == null) return false;
        try
        {
            var p = Process.Start(PythonPsi("-c \"import pyarrow.orc\""))!;
            p.WaitForExit(5000);
            return p.ExitCode == 0;
        }
        catch { return false; }
    }

    private static string? ReadOrcWithPyArrow(string path)
    {
        var scriptPath = Path.Combine(Path.GetTempPath(), $"storc_pyarrow_{Guid.NewGuid():N}.py");
        try
        {
            var escapedPath = path.Replace("\\", "\\\\");
            File.WriteAllText(scriptPath,
                "import pyarrow.orc as orc\n" +
                "import json\n" +
                $"table = orc.read_table(r'{escapedPath}')\n" +
                "result = dict()\n" +
                "result['num_rows'] = table.num_rows\n" +
                "result['num_columns'] = table.num_columns\n" +
                "result['column_names'] = table.column_names\n" +
                "columns = dict()\n" +
                "for name in table.column_names:\n" +
                "    col = table.column(name)\n" +
                "    values = []\n" +
                "    for v in col:\n" +
                "        if v.as_py() is None:\n" +
                "            values.append(None)\n" +
                "        else:\n" +
                "            val = v.as_py()\n" +
                "            if isinstance(val, (int, float, str, bool)):\n" +
                "                values.append(val)\n" +
                "            else:\n" +
                "                values.append(str(val))\n" +
                "    columns[name] = values\n" +
                "result['columns'] = columns\n" +
                "print(json.dumps(result))\n");

            var psi = PythonPsi(scriptPath);

            var p = Process.Start(psi)!;
            var stdout = p.StandardOutput.ReadToEnd();
            var stderr = p.StandardError.ReadToEnd();
            p.WaitForExit(10000);

            if (p.ExitCode != 0)
                throw new Exception($"PyArrow script failed (exit {p.ExitCode}):\n{stderr}");

            return stdout.Trim();
        }
        finally
        {
            File.Delete(scriptPath);
        }
    }

    private static string? ReadOrcWithPyArrowStructured(string path)
    {
        var scriptPath = Path.Combine(Path.GetTempPath(), $"storc_pyarrow_{Guid.NewGuid():N}.py");
        try
        {
            var escapedPath = path.Replace("\\", "\\\\");
            File.WriteAllText(scriptPath,
                "import pyarrow.orc as orc\n" +
                "import json\n" +
                $"table = orc.read_table(r'{escapedPath}')\n" +
                "result = dict()\n" +
                "result['num_rows'] = table.num_rows\n" +
                "result['num_columns'] = table.num_columns\n" +
                "result['column_names'] = table.column_names\n" +
                "columns = dict()\n" +
                "def to_python(v):\n" +
                "    val = v.as_py()\n" +
                "    if val is None:\n" +
                "        return None\n" +
                "    if isinstance(val, list):\n" +
                "        return [x if isinstance(x, (int, float, str, bool)) else str(x) for x in val]\n" +
                "    if isinstance(val, dict):\n" +
                "        return {str(k): v2 for k, v2 in val.items()}\n" +
                "    if isinstance(val, (int, float, str, bool)):\n" +
                "        return val\n" +
                "    return str(val)\n" +
                "for name in table.column_names:\n" +
                "    col = table.column(name)\n" +
                "    values = [to_python(v) for v in col]\n" +
                "    columns[name] = values\n" +
                "result['columns'] = columns\n" +
                "print(json.dumps(result))\n");

            var psi = PythonPsi(scriptPath);

            var p = Process.Start(psi)!;
            var stdout = p.StandardOutput.ReadToEnd();
            var stderr = p.StandardError.ReadToEnd();
            p.WaitForExit(10000);

            if (p.ExitCode != 0)
                throw new Exception($"PyArrow script failed (exit {p.ExitCode}):\n{stderr}");

            return stdout.Trim();
        }
        finally
        {
            File.Delete(scriptPath);
        }
    }

    private static string? ReadOrcWithPyArrowMap(string path)
    {
        var scriptPath = Path.Combine(Path.GetTempPath(), $"storc_pyarrow_{Guid.NewGuid():N}.py");
        try
        {
            var escapedPath = path.Replace("\\", "\\\\");
            File.WriteAllText(scriptPath,
                "import pyarrow.orc as orc\n" +
                "import json\n" +
                $"table = orc.read_table(r'{escapedPath}')\n" +
                "result = dict()\n" +
                "result['num_rows'] = table.num_rows\n" +
                "result['num_columns'] = table.num_columns\n" +
                "columns = dict()\n" +
                "def to_json(val):\n" +
                "    if val is None: return None\n" +
                "    if isinstance(val, (int, float, str, bool)): return val\n" +
                "    if isinstance(val, list):\n" +
                "        return [[to_json(t[0]), to_json(t[1])] if isinstance(t, tuple) else to_json(t) for t in val]\n" +
                "    if isinstance(val, dict):\n" +
                "        return {str(k): to_json(v) for k, v in val.items()}\n" +
                "    return str(val)\n" +
                "for name in table.column_names:\n" +
                "    col = table.column(name)\n" +
                "    columns[name] = [to_json(v.as_py()) for v in col]\n" +
                "result['columns'] = columns\n" +
                "print(json.dumps(result))\n");

            var psi = PythonPsi(scriptPath);

            var p = Process.Start(psi)!;
            var stdout = p.StandardOutput.ReadToEnd();
            var stderr = p.StandardError.ReadToEnd();
            p.WaitForExit(10000);

            if (p.ExitCode != 0)
                throw new Exception($"PyArrow script failed (exit {p.ExitCode}):\n{stderr}");

            return stdout.Trim();
        }
        finally
        {
            File.Delete(scriptPath);
        }
    }

    [SkippableFact]
    public async Task CrossValidate_Integers()
    {
        Skip.If(!IsPyArrowAvailable(), "PyArrow is not installed.");

        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new Int64Array.Builder().AppendRange([1L, 2L, 3L, 100L, -50L]).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 5));
            }

            var json = ReadOrcWithPyArrow(path);
            Assert.NotNull(json);

            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.Equal(5, root.GetProperty("num_rows").GetInt32());
            Assert.Equal(1, root.GetProperty("num_columns").GetInt32());

            var values = root.GetProperty("columns").GetProperty("id");
            Assert.Equal(1L, values[0].GetInt64());
            Assert.Equal(2L, values[1].GetInt64());
            Assert.Equal(3L, values[2].GetInt64());
            Assert.Equal(100L, values[3].GetInt64());
            Assert.Equal(-50L, values[4].GetInt64());
        }
        finally
        {
            File.Delete(path);
        }
    }

    [SkippableFact]
    public async Task CrossValidate_StringsDirect()
    {
        Skip.If(!IsPyArrowAvailable(), "PyArrow is not installed.");

        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("name", StringType.Default, nullable: false)], null);
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                var arr = new StringArray.Builder().Append("hello").Append("world").Append("test").Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            var json = ReadOrcWithPyArrow(path);
            Assert.NotNull(json);

            using var doc = JsonDocument.Parse(json);
            var values = doc.RootElement.GetProperty("columns").GetProperty("name");
            Assert.Equal("hello", values[0].GetString());
            Assert.Equal("world", values[1].GetString());
            Assert.Equal("test", values[2].GetString());
        }
        finally
        {
            File.Delete(path);
        }
    }

    [SkippableFact]
    public async Task CrossValidate_WithNulls()
    {
        Skip.If(!IsPyArrowAvailable(), "PyArrow is not installed.");

        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("val", Int64Type.Default, nullable: true)], null);
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new Int64Array.Builder().Append(10).AppendNull().Append(30).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            var json = ReadOrcWithPyArrow(path);
            Assert.NotNull(json);

            using var doc = JsonDocument.Parse(json);
            var values = doc.RootElement.GetProperty("columns").GetProperty("val");
            Assert.Equal(10L, values[0].GetInt64());
            Assert.Equal(JsonValueKind.Null, values[1].ValueKind);
            Assert.Equal(30L, values[2].GetInt64());
        }
        finally
        {
            File.Delete(path);
        }
    }

    [SkippableFact]
    public async Task CrossValidate_MultipleTypes()
    {
        Skip.If(!IsPyArrowAvailable(), "PyArrow is not installed.");

        var path = GetTempPath();
        try
        {
            var schema = new Schema([
                new Field("id", Int32Type.Default, nullable: false),
                new Field("value", DoubleType.Default, nullable: false),
                new Field("name", StringType.Default, nullable: false),
            ], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.DictionaryV2
            }))
            {
                var ids = new Int32Array.Builder().AppendRange([1, 2, 3]).Build();
                var vals = new DoubleArray.Builder().AppendRange([1.5, 2.5, 3.5]).Build();
                var names = new StringArray.Builder().Append("a").Append("b").Append("c").Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [ids, vals, names], 3));
            }

            var json = ReadOrcWithPyArrow(path);
            Assert.NotNull(json);

            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.Equal(3, root.GetProperty("num_rows").GetInt32());
            Assert.Equal(3, root.GetProperty("num_columns").GetInt32());

            var idCol = root.GetProperty("columns").GetProperty("id");
            Assert.Equal(1, idCol[0].GetInt32());
            Assert.Equal(2, idCol[1].GetInt32());
            Assert.Equal(3, idCol[2].GetInt32());

            var valCol = root.GetProperty("columns").GetProperty("value");
            Assert.Equal(1.5, valCol[0].GetDouble());

            var nameCol = root.GetProperty("columns").GetProperty("name");
            Assert.Equal("a", nameCol[0].GetString());
            Assert.Equal("c", nameCol[2].GetString());
        }
        finally
        {
            File.Delete(path);
        }
    }

    [SkippableFact]
    public async Task CrossValidate_StructColumn()
    {
        Skip.If(!IsPyArrowAvailable(), "PyArrow is not installed.");

        var path = GetTempPath();
        try
        {
            var structType = new StructType([
                new Field("x", Int32Type.Default, nullable: false),
                new Field("y", StringType.Default, nullable: false),
            ]);
            var schema = new Schema([new Field("s", structType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                var xs = new Int32Array.Builder().AppendRange([10, 20, 30]).Build();
                var ys = new StringArray.Builder().Append("foo").Append("bar").Append("baz").Build();
                var structArr = new StructArray(structType, 3, [xs, ys], ArrowBuffer.Empty, 0);
                await writer.WriteBatchAsync(new RecordBatch(schema, [structArr], 3));
            }

            var json = ReadOrcWithPyArrowStructured(path);
            Assert.NotNull(json);

            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.Equal(3, root.GetProperty("num_rows").GetInt32());

            // PyArrow returns struct column "s" as list of dicts: [{"x":10,"y":"foo"}, ...]
            var sCol = root.GetProperty("columns").GetProperty("s");
            Assert.Equal(10, sCol[0].GetProperty("x").GetInt32());
            Assert.Equal(20, sCol[1].GetProperty("x").GetInt32());
            Assert.Equal(30, sCol[2].GetProperty("x").GetInt32());
            Assert.Equal("foo", sCol[0].GetProperty("y").GetString());
            Assert.Equal("bar", sCol[1].GetProperty("y").GetString());
            Assert.Equal("baz", sCol[2].GetProperty("y").GetString());
        }
        finally
        {
            File.Delete(path);
        }
    }

    [SkippableFact]
    public async Task CrossValidate_ListColumn()
    {
        Skip.If(!IsPyArrowAvailable(), "PyArrow is not installed.");

        var path = GetTempPath();
        try
        {
            var listType = new ListType(new Field("item", Int32Type.Default, nullable: false));
            var schema = new Schema([new Field("nums", listType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var values = new Int32Array.Builder().AppendRange([1, 2, 3, 4, 5]).Build();
                var offsets = new ArrowBuffer.Builder<int>().AppendRange([0, 2, 3, 5]).Build();
                var listArr = new ListArray(listType, 3, offsets, values, ArrowBuffer.Empty, 0);
                await writer.WriteBatchAsync(new RecordBatch(schema, [listArr], 3));
            }

            var json = ReadOrcWithPyArrowStructured(path);
            Assert.NotNull(json);

            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.Equal(3, root.GetProperty("num_rows").GetInt32());

            var numsCol = root.GetProperty("columns").GetProperty("nums");
            // First list: [1, 2]
            Assert.Equal(2, numsCol[0].GetArrayLength());
            Assert.Equal(1, numsCol[0][0].GetInt32());
            Assert.Equal(2, numsCol[0][1].GetInt32());
            // Second list: [3]
            Assert.Equal(1, numsCol[1].GetArrayLength());
            Assert.Equal(3, numsCol[1][0].GetInt32());
            // Third list: [4, 5]
            Assert.Equal(2, numsCol[2].GetArrayLength());
            Assert.Equal(4, numsCol[2][0].GetInt32());
            Assert.Equal(5, numsCol[2][1].GetInt32());
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static Decimal128Array BuildDecimal128Array(Decimal128Type type, long[] values)
    {
        var bytes = new byte[values.Length * 16];
        for (int i = 0; i < values.Length; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(i * 16), values[i]);
            long sign = values[i] < 0 ? -1L : 0L;
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(i * 16 + 8), sign);
        }
        var valueBuf = new ArrowBuffer(bytes);
        var data = new ArrayData(type, values.Length, 0, 0, [ArrowBuffer.Empty, valueBuf]);
        return new Decimal128Array(data);
    }

    [SkippableFact]
    public async Task CrossValidate_DecimalColumn()
    {
        Skip.If(!IsPyArrowAvailable(), "PyArrow is not installed.");

        var path = GetTempPath();
        try
        {
            var decType = new Decimal128Type(10, 2);
            var schema = new Schema([new Field("amount", decType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                // 12.34, 56.78, -9.99
                var arr = BuildDecimal128Array(decType, [1234L, 5678L, -999L]);
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            var json = ReadOrcWithPyArrowStructured(path);
            Assert.NotNull(json);

            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.Equal(3, root.GetProperty("num_rows").GetInt32());

            // PyArrow converts decimals to their string representation via Decimal type
            var col = root.GetProperty("columns").GetProperty("amount");
            Assert.Equal("12.34", col[0].GetString());
            Assert.Equal("56.78", col[1].GetString());
            Assert.Equal("-9.99", col[2].GetString());
        }
        finally
        {
            File.Delete(path);
        }
    }

    [SkippableFact]
    public async Task CrossValidate_UnionColumn()
    {
        Skip.If(!IsPyArrowAvailable(), "PyArrow is not installed.");

        var path = GetTempPath();
        try
        {
            var unionType = new UnionType(
                [
                    new Field("i", Int32Type.Default, nullable: true),
                    new Field("s", StringType.Default, nullable: true),
                ],
                [0, 1],
                UnionMode.Dense);
            var schema = new Schema([new Field("u", unionType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                var typeIds = new byte[] { 0, 1, 0, 1, 0 };
                var offsets = new int[] { 0, 0, 1, 1, 2 };

                var intChild = new Int32Array.Builder().AppendRange([10, 20, 30]).Build();
                var strChild = new StringArray.Builder().Append("hello").Append("world").Build();

                var typeIdBuf = new ArrowBuffer(typeIds);
                var offsetBuf = new ArrowBuffer(System.Runtime.InteropServices.MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray());

                var unionArr = new DenseUnionArray(unionType, 5, [intChild, strChild], typeIdBuf, offsetBuf);
                await writer.WriteBatchAsync(new RecordBatch(schema, [unionArr], 5));
            }

            var json = ReadOrcWithPyArrowStructured(path);
            Assert.NotNull(json);

            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.Equal(5, root.GetProperty("num_rows").GetInt32());

            // PyArrow union as_py() returns the actual value for each row
            var col = root.GetProperty("columns").GetProperty("u");
            Assert.Equal(10, col[0].GetInt32());
            Assert.Equal("hello", col[1].GetString());
            Assert.Equal(20, col[2].GetInt32());
            Assert.Equal("world", col[3].GetString());
            Assert.Equal(30, col[4].GetInt32());
        }
        finally
        {
            File.Delete(path);
        }
    }

    [SkippableFact]
    public async Task CrossValidate_MapColumn()
    {
        Skip.If(!IsPyArrowAvailable(), "PyArrow is not installed.");

        var path = GetTempPath();
        try
        {
            var keyField = new Field("key", StringType.Default, nullable: false);
            var valueField = new Field("value", Int32Type.Default, nullable: true);
            var mapType = new MapType(keyField, valueField);
            var schema = new Schema([new Field("m", mapType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                // 3 rows: {"a":1, "b":2}, {"c":3}, {"d":4, "e":5}
                var keys = new StringArray.Builder()
                    .Append("a").Append("b").Append("c").Append("d").Append("e").Build();
                var values = new Int32Array.Builder().AppendRange([1, 2, 3, 4, 5]).Build();
                var offsets = new ArrowBuffer.Builder<int>().AppendRange([0, 2, 3, 5]).Build();

                var structType = new StructType([keyField, valueField]);
                var structArr = new StructArray(structType, 5, [keys, values], ArrowBuffer.Empty, 0);
                var mapArr = new MapArray(mapType, 3, offsets, structArr, ArrowBuffer.Empty, 0);

                await writer.WriteBatchAsync(new RecordBatch(schema, [mapArr], 3));
            }

            var json = ReadOrcWithPyArrowMap(path);
            Assert.NotNull(json);

            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.Equal(3, root.GetProperty("num_rows").GetInt32());

            // Maps returned as [[key,val],...] per row
            var col = root.GetProperty("columns").GetProperty("m");
            Assert.Equal(3, col.GetArrayLength());

            // Row 0: [["a",1],["b",2]]
            Assert.Equal(2, col[0].GetArrayLength());
            Assert.Equal("a", col[0][0][0].GetString());
            Assert.Equal(1, col[0][0][1].GetInt32());
            Assert.Equal("b", col[0][1][0].GetString());
            Assert.Equal(2, col[0][1][1].GetInt32());

            // Row 1: [["c",3]]
            Assert.Equal(1, col[1].GetArrayLength());
            Assert.Equal("c", col[1][0][0].GetString());
            Assert.Equal(3, col[1][0][1].GetInt32());

            // Row 2: [["d",4],["e",5]]
            Assert.Equal(2, col[2].GetArrayLength());
            Assert.Equal("d", col[2][0][0].GetString());
            Assert.Equal(4, col[2][0][1].GetInt32());
            Assert.Equal("e", col[2][1][0].GetString());
            Assert.Equal(5, col[2][1][1].GetInt32());
        }
        finally
        {
            File.Delete(path);
        }
    }

    [SkippableFact]
    public async Task CrossValidate_WithCompression()
    {
        Skip.If(!IsPyArrowAvailable(), "PyArrow is not installed.");

        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("x", Int64Type.Default, nullable: false)], null);
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.Zstd }))
            {
                var arr = new Int64Array.Builder().AppendRange(Enumerable.Range(0, 100).Select(i => (long)i)).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 100));
            }

            var json = ReadOrcWithPyArrow(path);
            Assert.NotNull(json);

            using var doc = JsonDocument.Parse(json);
            Assert.Equal(100, doc.RootElement.GetProperty("num_rows").GetInt32());
            var values = doc.RootElement.GetProperty("columns").GetProperty("x");
            Assert.Equal(0L, values[0].GetInt64());
            Assert.Equal(99L, values[99].GetInt64());
        }
        finally
        {
            File.Delete(path);
        }
    }

    [SkippableFact]
    public async Task CrossValidate_BloomFilter_DataIntact()
    {
        Skip.If(!IsPyArrowAvailable(), "PyArrow is not installed.");

        var path = GetTempPath();
        try
        {
            // Write an ORC file with bloom filters enabled.
            var schema = new Schema(
            [
                new Field("id", Int32Type.Default, nullable: false),
                new Field("name", StringType.Default, nullable: false),
            ], null);

            var options = new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                BloomFilterColumns = new HashSet<string> { "id", "name" },
                BloomFilterFpp = 0.05,
            };

            await using (var writer = OrcWriter.Create(path, schema, options))
            {
                var ids = new Int32Array.Builder().AppendRange(Enumerable.Range(0, 100)).Build();
                var names = new StringArray.Builder();
                for (int i = 0; i < 100; i++) names.Append($"name_{i}");
                await writer.WriteBatchAsync(new RecordBatch(schema,
                    [ids, names.Build()], 100));
            }

            // PyArrow should read the file without errors.
            var json = ReadOrcWithPyArrow(path);
            Assert.NotNull(json);

            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.Equal(100, root.GetProperty("num_rows").GetInt32());
            Assert.Equal(2, root.GetProperty("num_columns").GetInt32());

            // Verify data values survived the bloom filter addition.
            var idValues = root.GetProperty("columns").GetProperty("id");
            Assert.Equal(0, idValues[0].GetInt32());
            Assert.Equal(99, idValues[99].GetInt32());

            var nameValues = root.GetProperty("columns").GetProperty("name");
            Assert.Equal("name_0", nameValues[0].GetString());
            Assert.Equal("name_99", nameValues[99].GetString());
        }
        finally
        {
            File.Delete(path);
        }
    }

    [SkippableFact]
    public async Task CrossValidate_BloomFilter_ProbingWorks()
    {
        Skip.If(!IsPyArrowAvailable(), "PyArrow is not installed.");

        var path = GetTempPath();
        try
        {
            // Write an ORC file with bloom filters using our writer.
            var schema = new Schema(
            [
                new Field("name", StringType.Default, nullable: false),
            ], null);

            var options = new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                BloomFilterColumns = new HashSet<string> { "name" },
                BloomFilterFpp = 0.01,
            };

            await using (var writer = OrcWriter.Create(path, schema, options))
            {
                var names = new StringArray.Builder();
                for (int i = 0; i < 200; i++) names.Append($"name_{i}");
                await writer.WriteBatchAsync(new RecordBatch(schema, [names.Build()], 200));
            }

            // Use Python to independently parse and probe the bloom filter.
            var scriptPath = Path.Combine(TestHelpers.GetTestDataDirectory(), "validate_orc_bloom.py");
            var escapedPath = path.Replace("\\", "\\\\");

            var psi = PythonPsi($"\"{scriptPath}\" \"{path}\" name name_0 zzz_absent_value");
            var p = Process.Start(psi)!;
            var stdout = p.StandardOutput.ReadToEnd();
            var stderr = p.StandardError.ReadToEnd();
            p.WaitForExit(15000);

            Assert.True(p.ExitCode == 0,
                $"Python validation script failed (exit {p.ExitCode}):\n{stderr}");

            using var doc = JsonDocument.Parse(stdout);
            var root = doc.RootElement;

            // The Python script independently implements C++ murmur3_64 and probes
            // the bloom filter. A present value should be found, absent should not.
            Assert.True(root.GetProperty("present").GetBoolean(),
                "Python murmur3_64 probe should find 'name_0' in our bloom filter.");
            Assert.False(root.GetProperty("absent").GetBoolean(),
                "Python murmur3_64 probe should reject 'zzz_absent_value' from our bloom filter.");
        }
        finally
        {
            File.Delete(path);
        }
    }
}

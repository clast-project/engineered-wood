// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace EngineeredWood.Avro.Tests;

/// <summary>
/// Cross-validates EngineeredWood.Avro against Python's fastavro library.
/// Tests both directions:
///   1. fastavro writes → EngineeredWood reads
///   2. EngineeredWood writes → fastavro reads
/// </summary>
public class CrossValidationTests
{
    private static string TestDataDir =>
        Path.Combine(AppContext.BaseDirectory, "TestData");

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

    private static bool IsFastavroAvailable()
    {
        if (PythonExe == null) return false;
        try
        {
            var p = Process.Start(PythonPsi("-c \"import fastavro\""))!;
            p.WaitForExit(5000);
            return p.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }

    // ─── Direction 1: fastavro writes → EngineeredWood reads ───

    [Fact]
    public void ReadFastavro_Primitives_Null()
    {
        var path = Path.Combine(TestDataDir, "primitives_null.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        var batches = reader.ToList();

        int totalRows = batches.Sum(b => b.Length);
        Assert.Equal(100, totalRows);

        var batch = batches[0];
        Assert.Equal(7, batch.ColumnCount);

        // Verify first row
        Assert.Equal(true, ((BooleanArray)batch.Column(0)).GetValue(0));
        Assert.Equal(-50, ((Int32Array)batch.Column(1)).GetValue(0));
        Assert.Equal(-500000L, ((Int64Array)batch.Column(2)).GetValue(0));
        Assert.Equal(0f, ((FloatArray)batch.Column(3)).GetValue(0));
        Assert.Equal(0.0, ((DoubleArray)batch.Column(4)).GetValue(0));
        Assert.Equal("row_0", ((StringArray)batch.Column(5)).GetString(0));
    }

    [Fact]
    public void ReadFastavro_Primitives_Deflate()
    {
        var path = Path.Combine(TestDataDir, "primitives_deflate.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        Assert.Equal(AvroCodec.Deflate, reader.Codec);
        var batches = reader.ToList();
        Assert.Equal(100, batches.Sum(b => b.Length));
    }

    [Fact]
    public void ReadFastavro_Primitives_Snappy()
    {
        var path = Path.Combine(TestDataDir, "primitives_snappy.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        Assert.Equal(AvroCodec.Snappy, reader.Codec);
        var batches = reader.ToList();
        Assert.Equal(100, batches.Sum(b => b.Length));
    }

    [Fact]
    public void ReadFastavro_Nullable()
    {
        var path = Path.Combine(TestDataDir, "nullable_null.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        var batches = reader.ToList();
        Assert.Equal(50, batches.Sum(b => b.Length));

        var batch = batches[0];

        // Row 0: id=0, nullable_int=null (0 % 3 == 0), nullable_string=null (0 % 5 == 0)
        Assert.Equal(0, ((Int32Array)batch.Column(0)).GetValue(0));
        Assert.False(batch.Column(1).IsValid(0)); // null
        Assert.False(batch.Column(2).IsValid(0)); // null

        // Row 1: id=1, nullable_int=10, nullable_string="value_1"
        Assert.Equal(1, ((Int32Array)batch.Column(0)).GetValue(1));
        Assert.True(batch.Column(1).IsValid(1));
        Assert.Equal(10, ((Int32Array)batch.Column(1)).GetValue(1));
        Assert.Equal("value_1", ((StringArray)batch.Column(2)).GetString(1));
    }

    [Fact]
    public void ReadFastavro_EdgeCases()
    {
        var path = Path.Combine(TestDataDir, "edge_cases.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        var batch = reader.First();

        Assert.Equal(4, batch.Length);

        // Row 0: zeros and empty string
        Assert.Equal(0, ((Int32Array)batch.Column(0)).GetValue(0));
        Assert.Equal(0L, ((Int64Array)batch.Column(1)).GetValue(0));
        Assert.Equal("", ((StringArray)batch.Column(2)).GetString(0));

        // Row 1: max values
        Assert.Equal(int.MaxValue, ((Int32Array)batch.Column(0)).GetValue(1));
        Assert.Equal(long.MaxValue, ((Int64Array)batch.Column(1)).GetValue(1));

        // Row 2: min values
        Assert.Equal(int.MinValue, ((Int32Array)batch.Column(0)).GetValue(2));
        Assert.Equal(long.MinValue, ((Int64Array)batch.Column(1)).GetValue(2));

        // Row 3: unicode
        Assert.Equal("hello 🌍", ((StringArray)batch.Column(2)).GetString(3));
    }

    [Fact]
    public void ReadFastavro_Empty()
    {
        var path = Path.Combine(TestDataDir, "empty.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        var batches = reader.ToList();
        Assert.Empty(batches);
    }

    [Fact]
    public void ReadFastavro_Enum()
    {
        var path = Path.Combine(TestDataDir, "enum.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        var batches = reader.ToList();
        Assert.Equal(30, batches.Sum(b => b.Length));

        var batch = batches[0];
        var dictArray = (DictionaryArray)batch.Column(1);
        var indices = (Int32Array)dictArray.Indices;
        var dictionary = (StringArray)dictArray.Dictionary;

        // Row 0: RED (0 % 3 = 0)
        Assert.Equal("RED", dictionary.GetString(indices.GetValue(0)!.Value));
        // Row 1: GREEN (1 % 3 = 1)
        Assert.Equal("GREEN", dictionary.GetString(indices.GetValue(1)!.Value));
        // Row 2: BLUE (2 % 3 = 2)
        Assert.Equal("BLUE", dictionary.GetString(indices.GetValue(2)!.Value));
    }

    [Fact]
    public void ReadFastavro_Array()
    {
        var path = Path.Combine(TestDataDir, "array.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        var batches = reader.ToList();
        Assert.Equal(20, batches.Sum(b => b.Length));

        var batch = batches[0];
        var tagsList = (ListArray)batch.Column(1);

        // Row 0: tags = [] (0 % 4 = 0)
        Assert.Equal(0, tagsList.GetValueLength(0));
        // Row 1: tags = ["tag_0"] (1 % 4 = 1)
        Assert.Equal(1, tagsList.GetValueLength(1));
        var values = (StringArray)tagsList.Values;
        int start1 = tagsList.ValueOffsets[1];
        Assert.Equal("tag_0", values.GetString(start1));
        // Row 3: tags = ["tag_0", "tag_1", "tag_2"] (3 % 4 = 3)
        Assert.Equal(3, tagsList.GetValueLength(3));
    }

    [Fact]
    public void ReadFastavro_Map()
    {
        var path = Path.Combine(TestDataDir, "map.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        var batches = reader.ToList();
        Assert.Equal(20, batches.Sum(b => b.Length));

        var batch = batches[0];
        var labelsMap = (MapArray)batch.Column(1);

        // Row 0: labels = {} (0 % 3 = 0)
        Assert.Equal(0, labelsMap.GetValueLength(0));
        // Row 1: labels = {"key_0": "val_0"} (1 % 3 = 1)
        Assert.Equal(1, labelsMap.GetValueLength(1));
        // Row 2: labels = {"key_0": "val_0", "key_1": "val_1"} (2 % 3 = 2)
        Assert.Equal(2, labelsMap.GetValueLength(2));
    }

    [Fact]
    public void ReadFastavro_Fixed()
    {
        var path = Path.Combine(TestDataDir, "fixed.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        var batches = reader.ToList();
        Assert.Equal(20, batches.Sum(b => b.Length));

        var batch = batches[0];
        var fixedCol = (Apache.Arrow.Arrays.FixedSizeBinaryArray)batch.Column(1);

        // Row 0: hash = bytes([(0+j) % 256 for j in range(16)])
        var expected = new byte[16];
        for (int j = 0; j < 16; j++) expected[j] = (byte)j;
        Assert.Equal(expected, fixedCol.GetBytes(0).ToArray());
    }

    [Fact]
    public void ReadFastavro_Struct()
    {
        var path = Path.Combine(TestDataDir, "struct.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        var batches = reader.ToList();
        Assert.Equal(20, batches.Sum(b => b.Length));

        var batch = batches[0];
        var structCol = (StructArray)batch.Column(1);
        var street = (StringArray)structCol.Fields[0];
        var city = (StringArray)structCol.Fields[1];
        var zip = (Int32Array)structCol.Fields[2];

        Assert.Equal("0 Main St", street.GetString(0));
        Assert.Equal("City_0", city.GetString(0));
        Assert.Equal(10000, zip.GetValue(0));
    }

    [Fact]
    public void ReadFastavro_LogicalTypes()
    {
        var path = Path.Combine(TestDataDir, "logical_types.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        var batches = reader.ToList();
        Assert.Equal(20, batches.Sum(b => b.Length));

        var batch = batches[0];

        // date_col: row 0 = 2024-01-01 = days since epoch
        var dateCol = (Date32Array)batch.Column(1);
#if NET6_0_OR_GREATER
        Assert.Equal(new DateOnly(2024, 1, 1), dateCol.GetDateOnly(0));
        Assert.Equal(new DateOnly(2024, 1, 2), dateCol.GetDateOnly(1));
#else
        Assert.Equal(new DateTime(2024, 1, 1), dateCol.GetDateTime(0));
        Assert.Equal(new DateTime(2024, 1, 2), dateCol.GetDateTime(1));
#endif

        // time_millis_col: row 0 = 0 (0h 0m 0s)
        var timeMillisCol = (Time32Array)batch.Column(2);
        Assert.Equal(0, timeMillisCol.GetValue(0));

        // time_micros_col: row 0 = 0
        var timeMicrosCol = (Time64Array)batch.Column(3);
        Assert.Equal(0L, timeMicrosCol.GetValue(0));
    }

    [Fact]
    public void ReadFastavro_NullableEnum()
    {
        var path = Path.Combine(TestDataDir, "nullable_enum.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        var batches = reader.ToList();
        Assert.Equal(15, batches.Sum(b => b.Length));

        var batch = batches[0];
        var dictArray = (DictionaryArray)batch.Column(1);

        // Row 0: null (0 % 3 == 0)
        Assert.False(dictArray.IsValid(0));
        // Row 1: GREEN (1 % 3 == 1)
        Assert.True(dictArray.IsValid(1));
        var indices = (Int32Array)dictArray.Indices;
        var dictionary = (StringArray)dictArray.Dictionary;
        Assert.Equal("GREEN", dictionary.GetString(indices.GetValue(1)!.Value));
    }

    // ─── Direction 2: EngineeredWood writes → fastavro reads ───

    [SkippableFact]
    public void WriteThenReadWithFastavro_Primitives()
    {
        Skip.If(!IsFastavroAvailable(), "fastavro is not installed.");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("int_col", Int32Type.Default, false))
            .Field(new Field("long_col", Int64Type.Default, false))
            .Field(new Field("float_col", FloatType.Default, false))
            .Field(new Field("double_col", DoubleType.Default, false))
            .Field(new Field("string_col", StringType.Default, false))
            .Field(new Field("bool_col", BooleanType.Default, false))
            .Build();

        var intBuilder = new Int32Array.Builder();
        var longBuilder = new Int64Array.Builder();
        var floatBuilder = new FloatArray.Builder();
        var doubleBuilder = new DoubleArray.Builder();
        var stringBuilder = new StringArray.Builder();
        var boolBuilder = new BooleanArray.Builder();

        for (int i = 0; i < 20; i++)
        {
            intBuilder.Append(i);
            longBuilder.Append(i * 1000L);
            floatBuilder.Append(i * 0.1f);
            doubleBuilder.Append(i * 0.01);
            stringBuilder.Append($"str_{i}");
            boolBuilder.Append(i % 2 == 0);
        }

        var batch = new RecordBatch(schema,
            [intBuilder.Build(), longBuilder.Build(), floatBuilder.Build(),
             doubleBuilder.Build(), stringBuilder.Build(), boolBuilder.Build()], 20);

        var tmpFile = Path.Combine(Path.GetTempPath(), $"ew_avro_{Guid.NewGuid()}.avro");
        try
        {
            using (var fs = File.Create(tmpFile))
            using (var writer = new AvroWriterBuilder(schema).Build(fs))
            {
                writer.Write(batch);
                writer.Finish();
            }

            var result = ReadWithFastavro(tmpFile);
            Assert.Equal(20, result.GetProperty("num_rows").GetInt32());

            var rows = result.GetProperty("rows");
            var row0 = rows[0];
            Assert.Equal(0, row0.GetProperty("int_col").GetInt32());
            Assert.Equal(0L, row0.GetProperty("long_col").GetInt64());
            Assert.Equal("str_0", row0.GetProperty("string_col").GetString());
            Assert.True(row0.GetProperty("bool_col").GetBoolean());

            var row19 = rows[19];
            Assert.Equal(19, row19.GetProperty("int_col").GetInt32());
            Assert.Equal("str_19", row19.GetProperty("string_col").GetString());
        }
        finally
        {
            File.Delete(tmpFile);
        }
    }

    [SkippableFact]
    public void WriteThenReadWithFastavro_WithNulls()
    {
        Skip.If(!IsFastavroAvailable(), "fastavro is not installed.");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("nullable_int", Int32Type.Default, true))
            .Build();

        var builder = new Int32Array.Builder();
        builder.Append(42);
        builder.AppendNull();
        builder.Append(99);

        var batch = new RecordBatch(schema, [builder.Build()], 3);

        var tmpFile = Path.Combine(Path.GetTempPath(), $"ew_avro_{Guid.NewGuid()}.avro");
        try
        {
            using (var fs = File.Create(tmpFile))
            using (var writer = new AvroWriterBuilder(schema).Build(fs))
            {
                writer.Write(batch);
                writer.Finish();
            }

            var result = ReadWithFastavro(tmpFile);
            Assert.Equal(3, result.GetProperty("num_rows").GetInt32());

            var rows = result.GetProperty("rows");
            Assert.Equal(42, rows[0].GetProperty("nullable_int").GetInt32());
            Assert.Equal(JsonValueKind.Null, rows[1].GetProperty("nullable_int").ValueKind);
            Assert.Equal(99, rows[2].GetProperty("nullable_int").GetInt32());
        }
        finally
        {
            File.Delete(tmpFile);
        }
    }

    [SkippableFact]
    public void WriteThenReadWithFastavro_Enum()
    {
        Skip.If(!IsFastavroAvailable(), "fastavro is not installed.");

        var dictType = new DictionaryType(Int32Type.Default, StringType.Default, false);
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("color", dictType, false))
            .Build();

        var dictBuilder = new StringArray.Builder();
        dictBuilder.Append("RED");
        dictBuilder.Append("GREEN");
        dictBuilder.Append("BLUE");
        var dictionary = dictBuilder.Build();

        var indexBuilder = new Int32Array.Builder();
        indexBuilder.Append(0);
        indexBuilder.Append(1);
        indexBuilder.Append(2);
        var indices = indexBuilder.Build();
        var dictArray = new DictionaryArray(dictType, indices, dictionary);
        var batch = new RecordBatch(arrowSchema, [dictArray], 3);

        var avroSchemaJson = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "color", "type": {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN", "BLUE"]}}
            ]
        }
        """;

        var tmpFile = Path.Combine(Path.GetTempPath(), $"ew_avro_{Guid.NewGuid()}.avro");
        try
        {
            using (var fs = File.Create(tmpFile))
            using (var writer = new AvroWriterBuilder(arrowSchema)
                .WithAvroSchema(new AvroSchema(avroSchemaJson))
                .Build(fs))
            {
                writer.Write(batch);
                writer.Finish();
            }

            var result = ReadWithFastavro(tmpFile);
            Assert.Equal(3, result.GetProperty("num_rows").GetInt32());
            var rows = result.GetProperty("rows");
            Assert.Equal("RED", rows[0].GetProperty("color").GetString());
            Assert.Equal("GREEN", rows[1].GetProperty("color").GetString());
            Assert.Equal("BLUE", rows[2].GetProperty("color").GetString());
        }
        finally
        {
            File.Delete(tmpFile);
        }
    }

    [SkippableFact]
    public void WriteThenReadWithFastavro_Array()
    {
        Skip.If(!IsFastavroAvailable(), "fastavro is not installed.");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("tags", new ListType(StringType.Default), false))
            .Build();

        var valueBuilder = new StringArray.Builder();
        var offsets = new List<int> { 0 };

        valueBuilder.Append("a");
        valueBuilder.Append("b");
        offsets.Add(2); // row 0: ["a", "b"]

        offsets.Add(2); // row 1: []

        valueBuilder.Append("c");
        offsets.Add(3); // row 2: ["c"]

        var offsetBytes = new byte[offsets.Count * 4];
        for (int i = 0; i < offsets.Count; i++)
#if NET8_0_OR_GREATER
            BitConverter.TryWriteBytes(offsetBytes.AsSpan(i * 4), offsets[i]);
#else
            Buffer.BlockCopy(BitConverter.GetBytes(offsets[i]), 0, offsetBytes, i * 4, 4);
#endif

        var listArray = new ListArray(new ListType(StringType.Default),
            3, new ArrowBuffer(offsetBytes), valueBuilder.Build(), ArrowBuffer.Empty);
        var batch = new RecordBatch(schema, [listArray], 3);

        var tmpFile = Path.Combine(Path.GetTempPath(), $"ew_avro_{Guid.NewGuid()}.avro");
        try
        {
            using (var fs = File.Create(tmpFile))
            using (var writer = new AvroWriterBuilder(schema).Build(fs))
            {
                writer.Write(batch);
                writer.Finish();
            }

            var result = ReadWithFastavro(tmpFile);
            Assert.Equal(3, result.GetProperty("num_rows").GetInt32());
            var rows = result.GetProperty("rows");
            Assert.Equal(2, rows[0].GetProperty("tags").GetArrayLength());
            Assert.Equal(0, rows[1].GetProperty("tags").GetArrayLength());
            Assert.Equal(1, rows[2].GetProperty("tags").GetArrayLength());
        }
        finally
        {
            File.Delete(tmpFile);
        }
    }

    [SkippableFact]
    public void WriteThenReadWithFastavro_Struct()
    {
        Skip.If(!IsFastavroAvailable(), "fastavro is not installed.");

        var structType = new StructType([
            new Field("city", StringType.Default, false),
            new Field("zip", Int32Type.Default, false),
        ]);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("address", structType, false))
            .Build();

        var cityBuilder = new StringArray.Builder();
        cityBuilder.Append("NYC");
        cityBuilder.Append("LA");
        var zipBuilder = new Int32Array.Builder();
        zipBuilder.Append(10001);
        zipBuilder.Append(90001);

        var structArray = new StructArray(structType, 2,
            [cityBuilder.Build(), zipBuilder.Build()], ArrowBuffer.Empty);
        var batch = new RecordBatch(schema, [structArray], 2);

        var tmpFile = Path.Combine(Path.GetTempPath(), $"ew_avro_{Guid.NewGuid()}.avro");
        try
        {
            using (var fs = File.Create(tmpFile))
            using (var writer = new AvroWriterBuilder(schema).Build(fs))
            {
                writer.Write(batch);
                writer.Finish();
            }

            var result = ReadWithFastavro(tmpFile);
            Assert.Equal(2, result.GetProperty("num_rows").GetInt32());
            var rows = result.GetProperty("rows");
            var addr0 = rows[0].GetProperty("address");
            Assert.Equal("NYC", addr0.GetProperty("city").GetString());
            Assert.Equal(10001, addr0.GetProperty("zip").GetInt32());
        }
        finally
        {
            File.Delete(tmpFile);
        }
    }

    private static JsonElement ReadWithFastavro(string path)
    {
        var escapedPath = path.Replace("\\", "\\\\").Replace("'", "\\'");
        var script = """
import fastavro
import json

with open(r'__PATH__', 'rb') as f:
    reader = fastavro.reader(f)
    rows = list(reader)

def convert(obj):
    if isinstance(obj, bytes):
        return list(obj)
    if isinstance(obj, dict):
        return {k: convert(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert(v) for v in obj]
    return obj

result = {
    "num_rows": len(rows),
    "rows": [convert(r) for r in rows]
}
print(json.dumps(result))
""".Replace("__PATH__", escapedPath);

        var scriptPath = Path.Combine(Path.GetTempPath(), $"ew_fastavro_{Guid.NewGuid()}.py");
        File.WriteAllText(scriptPath, script);

        try
        {
            var p = Process.Start(PythonPsi(scriptPath))!;
            var stdout = p.StandardOutput.ReadToEnd();
            var stderr = p.StandardError.ReadToEnd();
            p.WaitForExit(10000);

            if (p.ExitCode != 0)
                throw new Exception($"fastavro script failed: {stderr}");

            return JsonDocument.Parse(stdout).RootElement;
        }
        finally
        {
            File.Delete(scriptPath);
        }
    }
}

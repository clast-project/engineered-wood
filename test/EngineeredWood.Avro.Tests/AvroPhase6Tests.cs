// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace EngineeredWood.Avro.Tests;

/// <summary>
/// Phase 6 tests: Decimal logical type, UUID logical type, LZ4 codec, cross-library interop.
/// </summary>
public class AvroPhase6Tests
{
    private static string TestDataDir =>
        Path.Combine(AppContext.BaseDirectory, "TestData");

    // ─── Decimal (fixed-based) round-trip ───

    [Fact]
    public void RoundTrip_DecimalFixed()
    {
        var decType = new Decimal128Type(16, 4);
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("price", decType, false))
            .Build();

        var avroSchemaJson = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "price", "type": {"type": "fixed", "name": "Price", "size": 16, "logicalType": "decimal", "precision": 16, "scale": 4}}
            ]
        }
        """;

        // Build Decimal128 values manually via ArrayData
        var values = new byte[3 * 16];
        WriteDecimal128LE(values.AsSpan(0, 16), 123456789L); // 12345.6789
        WriteDecimal128LE(values.AsSpan(16, 16), -999999L);  // -99.9999
        WriteDecimal128LE(values.AsSpan(32, 16), 0L);         // 0.0000

        var data = new ArrayData(decType, 3, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(values)]);
        var decArray = ArrowArrayFactory.BuildArray(data);

        var batch = new RecordBatch(arrowSchema, [decArray], 3);

        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(arrowSchema)
            .WithAvroSchema(new AvroSchema(avroSchemaJson))
            .Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }

        ms.Position = 0;
        using var reader = new AvroReaderBuilder().Build(ms);
        var result = reader.ReadNextBatch()!;

        Assert.Equal(3, result.Length);
        var resultDec = (FixedSizeBinaryArray)result.Column(0);
        AssertDecimal128Equals(123456789L, resultDec, 0);
        AssertDecimal128Equals(-999999L, resultDec, 1);
        AssertDecimal128Equals(0L, resultDec, 2);
    }

    // ─── Decimal (bytes-based) round-trip ───

    [Fact]
    public void RoundTrip_DecimalBytes()
    {
        var decType = new Decimal128Type(10, 2);
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("amount", decType, false))
            .Build();

        var avroSchemaJson = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}}
            ]
        }
        """;

        var values = new byte[2 * 16];
        WriteDecimal128LE(values.AsSpan(0, 16), 4200L);   // 42.00
        WriteDecimal128LE(values.AsSpan(16, 16), -100L);   // -1.00

        var data = new ArrayData(decType, 2, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(values)]);
        var decArray = ArrowArrayFactory.BuildArray(data);

        var batch = new RecordBatch(arrowSchema, [decArray], 2);

        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(arrowSchema)
            .WithAvroSchema(new AvroSchema(avroSchemaJson))
            .Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }

        ms.Position = 0;
        using var reader = new AvroReaderBuilder().Build(ms);
        var result = reader.ReadNextBatch()!;

        Assert.Equal(2, result.Length);
        var resultDec = (FixedSizeBinaryArray)result.Column(0);
        AssertDecimal128Equals(4200L, resultDec, 0);
        AssertDecimal128Equals(-100L, resultDec, 1);
    }

    // ─── Nullable decimal ───

    [Fact]
    public void RoundTrip_NullableDecimal()
    {
        var decType = new Decimal128Type(10, 2);
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("amount", decType, true))
            .Build();

        var avroSchemaJson = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}]}
            ]
        }
        """;

        var values = new byte[3 * 16];
        WriteDecimal128LE(values.AsSpan(0, 16), 4200L);
        WriteDecimal128LE(values.AsSpan(16, 16), 0L); // null placeholder
        WriteDecimal128LE(values.AsSpan(32, 16), -100L);

        var nullBitmap = new byte[1];
        nullBitmap[0] = 0b101; // valid, null, valid

        var data = new ArrayData(decType, 3, 1, 0,
            [new ArrowBuffer(nullBitmap), new ArrowBuffer(values)]);
        var decArray = ArrowArrayFactory.BuildArray(data);
        var batch = new RecordBatch(arrowSchema, [decArray], 3);

        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(arrowSchema)
            .WithAvroSchema(new AvroSchema(avroSchemaJson))
            .Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }

        ms.Position = 0;
        using var reader = new AvroReaderBuilder().Build(ms);
        var result = reader.ReadNextBatch()!;

        Assert.Equal(3, result.Length);
        Assert.True(result.Column(0).IsValid(0));
        Assert.False(result.Column(0).IsValid(1));
        Assert.True(result.Column(0).IsValid(2));
        AssertDecimal128Equals(4200L, (FixedSizeBinaryArray)result.Column(0), 0);
        AssertDecimal128Equals(-100L, (FixedSizeBinaryArray)result.Column(0), 2);
    }

    // ─── UUID round-trip ───

    [Fact]
    public void RoundTrip_Uuid()
    {
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("uid", StringType.Default, false))
            .Build();

        var avroSchemaJson = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "uid", "type": {"type": "string", "logicalType": "uuid"}}
            ]
        }
        """;

        var sb = new StringArray.Builder();
        var uuid1 = "550e8400-e29b-41d4-a716-446655440000";
        var uuid2 = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
        sb.Append(uuid1);
        sb.Append(uuid2);

        var batch = new RecordBatch(arrowSchema, [sb.Build()], 2);

        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(arrowSchema)
            .WithAvroSchema(new AvroSchema(avroSchemaJson))
            .Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }

        ms.Position = 0;
        using var reader = new AvroReaderBuilder().Build(ms);
        var result = reader.ReadNextBatch()!;

        Assert.Equal(2, result.Length);
        var resultStrings = (StringArray)result.Column(0);
        Assert.Equal(uuid1, resultStrings.GetString(0));
        Assert.Equal(uuid2, resultStrings.GetString(1));
    }

    // ─── LZ4 codec round-trip ───

    [Fact]
    public void RoundTrip_Lz4Codec()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("int_col", Int32Type.Default, false))
            .Field(new Field("string_col", StringType.Default, false))
            .Build();

        var intBuilder = new Int32Array.Builder();
        var stringBuilder = new StringArray.Builder();
        for (int i = 0; i < 100; i++)
        {
            intBuilder.Append(i);
            stringBuilder.Append($"row_{i}");
        }

        var batch = new RecordBatch(schema,
            [intBuilder.Build(), stringBuilder.Build()], 100);

        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(schema)
            .WithCompression(AvroCodec.Lz4)
            .Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }

        ms.Position = 0;
        using var reader = new AvroReaderBuilder().Build(ms);
        Assert.Equal(AvroCodec.Lz4, reader.Codec);
        var result = reader.ReadNextBatch()!;

        Assert.Equal(100, result.Length);
        var ints = (Int32Array)result.Column(0);
        var strings = (StringArray)result.Column(1);
        for (int i = 0; i < 100; i++)
        {
            Assert.Equal(i, ints.GetValue(i));
            Assert.Equal($"row_{i}", strings.GetString(i));
        }
    }

    [Theory]
    [InlineData(AvroCodec.Lz4)]
    public void RoundTrip_MultipleBatches_Compressed(AvroCodec codec)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(schema)
            .WithCompression(codec)
            .Build(ms))
        {
            for (int b = 0; b < 3; b++)
            {
                var builder = new Int64Array.Builder();
                for (int i = 0; i < 10; i++) builder.Append(b * 10 + i);
                writer.Write(new RecordBatch(schema, [builder.Build()], 10));
            }
            writer.Finish();
        }

        ms.Position = 0;
        var reader = new AvroReaderBuilder().Build(ms);
        var results = reader.ToList();
        Assert.Equal(3, results.Count);
        Assert.Equal(30, results.Sum(r => r.Length));
    }

    // ─── Cross-validation: read fastavro-generated decimal files ───

    [Fact]
    public void ReadFastavro_DecimalBytes()
    {
        var path = Path.Combine(TestDataDir, "decimal_bytes.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        var batches = reader.ToList();
        Assert.Equal(20, batches.Sum(b => b.Length));

        // Verify it's a Decimal128 column
        var batch = batches[0];
        Assert.IsAssignableFrom<FixedSizeBinaryArray>(batch.Column(1));

        // Row 0: amount = Decimal("0.00") → unscaled=0
        AssertDecimal128Equals(0L, (FixedSizeBinaryArray)batch.Column(1), 0);
        // Row 1: amount = Decimal("101.01") → unscaled=10101
        AssertDecimal128Equals(10101L, (FixedSizeBinaryArray)batch.Column(1), 1);
    }

    [Fact]
    public void ReadFastavro_DecimalFixed()
    {
        var path = Path.Combine(TestDataDir, "decimal_fixed.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        var batches = reader.ToList();
        Assert.Equal(20, batches.Sum(b => b.Length));

        var batch = batches[0];
        Assert.IsAssignableFrom<FixedSizeBinaryArray>(batch.Column(1));

        // Row 0: price = Decimal("0.0000") → unscaled=0
        AssertDecimal128Equals(0L, (FixedSizeBinaryArray)batch.Column(1), 0);
    }

    [Fact]
    public void ReadFastavro_Uuid()
    {
        var path = Path.Combine(TestDataDir, "uuid.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        var batches = reader.ToList();
        Assert.Equal(20, batches.Sum(b => b.Length));

        var batch = batches[0];
        // UUID is read as string
        var uids = (StringArray)batch.Column(1);
        // Row 0: UUID(int=1) = "00000000-0000-0000-0000-000000000001"
        Assert.Equal("00000000-0000-0000-0000-000000000001", uids.GetString(0));
        // Row 1: UUID(int=2)
        Assert.Equal("00000000-0000-0000-0000-000000000002", uids.GetString(1));
    }

    [Fact]
    public void ReadFastavro_Lz4()
    {
        var path = Path.Combine(TestDataDir, "primitives_lz4.avro");
        if (!File.Exists(path)) return;

        using var stream = File.OpenRead(path);
        var reader = new AvroReaderBuilder().Build(stream);
        Assert.Equal(AvroCodec.Lz4, reader.Codec);
        var batches = reader.ToList();
        Assert.Equal(100, batches.Sum(b => b.Length));

        // Verify first row values match
        var batch = batches[0];
        Assert.Equal(true, ((BooleanArray)batch.Column(0)).GetValue(0));
        Assert.Equal(-50, ((Int32Array)batch.Column(1)).GetValue(0));
        Assert.Equal("row_0", ((StringArray)batch.Column(5)).GetString(0));
    }

    // ─── Schema round-trip: decimal schema preserves precision/scale ───

    [Fact]
    public void Schema_DecimalBytesPreservesPrecisionScale()
    {
        var json = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}}
            ]
        }
        """;

        // Parse, then re-serialize through internal schema tree
        var schema = new AvroSchema(json);
        var reserialized = EngineeredWood.Avro.Schema.AvroSchemaWriter.ToJson(schema.Parsed);

        Assert.Contains("\"precision\":10", reserialized);
        Assert.Contains("\"scale\":2", reserialized);
    }

    [Fact]
    public void Schema_DecimalFixedPreservesPrecisionScale()
    {
        var json = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "price", "type": {"type": "fixed", "name": "Price", "size": 8, "logicalType": "decimal", "precision": 16, "scale": 4}}
            ]
        }
        """;

        var schema = new AvroSchema(json);
        var reserialized = EngineeredWood.Avro.Schema.AvroSchemaWriter.ToJson(schema.Parsed);

        Assert.Contains("\"precision\":16", reserialized);
        Assert.Contains("\"scale\":4", reserialized);
    }

    // ─── Arrow schema converter: Decimal128Type ↔ Avro ───

    [Fact]
    public void ArrowSchemaConverter_Decimal128Type_RoundTrips()
    {
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("price", new Decimal128Type(18, 6), false))
            .Build();

        var avroSchema = new AvroWriterBuilder(arrowSchema).Build(Stream.Null);
        // Read back the schema
        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(arrowSchema).Build(ms))
        {
            var intBuilder = new Int32Array.Builder();
            // We can't easily write Decimal128 through this path without explicit schema,
            // so just test the schema converter directly
        }

        // Direct converter test
        var avro = EngineeredWood.Avro.Schema.ArrowSchemaConverter.FromArrow(arrowSchema);
        var arrowBack = EngineeredWood.Avro.Schema.ArrowSchemaConverter.ToArrow(avro);

        var field = arrowBack.FieldsList[0];
        var decType = Assert.IsType<Decimal128Type>(field.DataType);
        Assert.Equal(18, decType.Precision);
        Assert.Equal(6, decType.Scale);
    }

    // ─── Direction 2: EngineeredWood writes → fastavro reads ───

    [SkippableFact]
    public void WriteThenReadWithFastavro_Lz4()
    {
        Skip.If(!IsFastavroAvailable(), "fastavro is not installed.");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("value", StringType.Default, false))
            .Build();

        var intBuilder = new Int32Array.Builder();
        var stringBuilder = new StringArray.Builder();
        for (int i = 0; i < 10; i++)
        {
            intBuilder.Append(i);
            stringBuilder.Append($"val_{i}");
        }

        var batch = new RecordBatch(schema,
            [intBuilder.Build(), stringBuilder.Build()], 10);

        var tmpFile = Path.Combine(Path.GetTempPath(), $"ew_avro_{Guid.NewGuid()}.avro");
        try
        {
            using (var fs = File.Create(tmpFile))
            using (var writer = new AvroWriterBuilder(schema)
                .WithCompression(AvroCodec.Lz4)
                .Build(fs))
            {
                writer.Write(batch);
                writer.Finish();
            }

            var result = ReadWithFastavro(tmpFile);
            Assert.Equal(10, result.GetProperty("num_rows").GetInt32());
            var rows = result.GetProperty("rows");
            Assert.Equal(0, rows[0].GetProperty("id").GetInt32());
            Assert.Equal("val_0", rows[0].GetProperty("value").GetString());
            Assert.Equal(9, rows[9].GetProperty("id").GetInt32());
        }
        finally
        {
            File.Delete(tmpFile);
        }
    }

    [SkippableFact]
    public void WriteThenReadWithFastavro_Uuid()
    {
        Skip.If(!IsFastavroAvailable(), "fastavro is not installed.");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("uid", StringType.Default, false))
            .Build();

        var avroSchemaJson = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "uid", "type": {"type": "string", "logicalType": "uuid"}}
            ]
        }
        """;

        var sb = new StringArray.Builder();
        sb.Append("550e8400-e29b-41d4-a716-446655440000");
        sb.Append("6ba7b810-9dad-11d1-80b4-00c04fd430c8");

        var batch = new RecordBatch(schema, [sb.Build()], 2);

        var tmpFile = Path.Combine(Path.GetTempPath(), $"ew_avro_{Guid.NewGuid()}.avro");
        try
        {
            using (var fs = File.Create(tmpFile))
            using (var writer = new AvroWriterBuilder(schema)
                .WithAvroSchema(new AvroSchema(avroSchemaJson))
                .Build(fs))
            {
                writer.Write(batch);
                writer.Finish();
            }

            var result = ReadWithFastavro(tmpFile);
            Assert.Equal(2, result.GetProperty("num_rows").GetInt32());
            var rows = result.GetProperty("rows");
            Assert.Equal("550e8400-e29b-41d4-a716-446655440000",
                rows[0].GetProperty("uid").GetString());
        }
        finally
        {
            File.Delete(tmpFile);
        }
    }

    // ─── Helpers ───

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
        catch { return false; }
    }

    private static void WriteDecimal128LE(Span<byte> dest, long unscaledValue)
    {
        // Write as 16-byte little-endian two's complement
        dest.Clear();
#if NET8_0_OR_GREATER
        BitConverter.TryWriteBytes(dest, unscaledValue);
#else
        MemoryMarshal.Write(dest, ref unscaledValue);
#endif
        if (unscaledValue < 0)
            dest[8..].Fill(0xFF); // sign-extend
    }

    private static void AssertDecimal128Equals(long expected, FixedSizeBinaryArray array, int index)
    {
        var bytes = array.GetBytes(index);
        // Read 16-byte LE value
#if NET8_0_OR_GREATER
        long low = BitConverter.ToInt64(bytes);
        long high = BitConverter.ToInt64(bytes[8..]);
#else
        long low = MemoryMarshal.Read<long>(bytes);
        long high = MemoryMarshal.Read<long>(bytes.Slice(8));
#endif
        // For values that fit in a long, high should be sign extension
        if (expected >= 0)
        {
            Assert.Equal(expected, low);
            Assert.Equal(0L, high);
        }
        else
        {
            Assert.Equal(expected, low);
            Assert.Equal(-1L, high);
        }
    }

    private static JsonElement ReadWithFastavro(string path)
    {
        var escapedPath = path.Replace("\\", "\\\\").Replace("'", "\\'");
        var script = """
import fastavro
import json
import decimal
import uuid

with open(r'__PATH__', 'rb') as f:
    reader = fastavro.reader(f)
    rows = list(reader)

def convert(obj):
    if isinstance(obj, bytes):
        return list(obj)
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    if isinstance(obj, uuid.UUID):
        return str(obj)
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

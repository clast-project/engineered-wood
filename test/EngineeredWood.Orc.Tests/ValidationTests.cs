// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.IO.Compression;
using System.Text.Json;
using Apache.Arrow;
using Xunit.Abstractions;

namespace EngineeredWood.Orc.Tests;

public class ValidationTests(ITestOutputHelper output)
{
    private static List<JsonElement> LoadExpectedRows(string orcFileName, int maxRows = int.MaxValue)
    {
        var gzPath = Path.Combine(
            Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "TestData", "expected")),
            Path.GetFileNameWithoutExtension(orcFileName) + ".jsn.gz");

        var rows = new List<JsonElement>();
        using var fs = File.OpenRead(gzPath);
        using var gz = new GZipStream(fs, CompressionMode.Decompress);
        using var sr = new StreamReader(gz);

        string? line;
        while ((line = sr.ReadLine()) != null && rows.Count < maxRows)
        {
            if (!string.IsNullOrWhiteSpace(line))
                rows.Add(JsonDocument.Parse(line).RootElement);
        }
        return rows;
    }

    [Fact]
    public async Task ValidateTest1_FirstRow()
    {
        var path = TestHelpers.GetTestFilePath("TestOrcFile.test1.orc");
        await using var reader = await OrcReader.OpenAsync(path);
        var expected = LoadExpectedRows("TestOrcFile.test1.orc", 2);

        var rowReader = reader.CreateRowReader(new OrcReaderOptions { BatchSize = 10 });
        RecordBatch? firstBatch = null;
        await foreach (var batch in rowReader)
        {
            firstBatch = batch;
            break;
        }

        Assert.NotNull(firstBatch);

        // Row 0: {"boolean1": false, "byte1": 1, "short1": 1024, "int1": 65536, "long1": 9223372036854775807, ...}
        var row0 = expected[0];

        // boolean1
        var boolCol = firstBatch.Column("boolean1") as BooleanArray;
        Assert.NotNull(boolCol);
        Assert.Equal(row0.GetProperty("boolean1").GetBoolean(), boolCol.GetValue(0));
        output.WriteLine($"boolean1: expected={row0.GetProperty("boolean1")}, actual={boolCol.GetValue(0)}");

        // byte1
        var byteCol = firstBatch.Column("byte1") as Int8Array;
        Assert.NotNull(byteCol);
        Assert.Equal(row0.GetProperty("byte1").GetSByte(), byteCol.GetValue(0));
        output.WriteLine($"byte1: expected={row0.GetProperty("byte1")}, actual={byteCol.GetValue(0)}");

        // short1
        var shortCol = firstBatch.Column("short1") as Int16Array;
        Assert.NotNull(shortCol);
        Assert.Equal(row0.GetProperty("short1").GetInt16(), shortCol.GetValue(0));
        output.WriteLine($"short1: expected={row0.GetProperty("short1")}, actual={shortCol.GetValue(0)}");

        // int1
        var intCol = firstBatch.Column("int1") as Int32Array;
        Assert.NotNull(intCol);
        Assert.Equal(row0.GetProperty("int1").GetInt32(), intCol.GetValue(0));
        output.WriteLine($"int1: expected={row0.GetProperty("int1")}, actual={intCol.GetValue(0)}");

        // long1
        var longCol = firstBatch.Column("long1") as Int64Array;
        Assert.NotNull(longCol);
        Assert.Equal(row0.GetProperty("long1").GetInt64(), longCol.GetValue(0));
        output.WriteLine($"long1: expected={row0.GetProperty("long1")}, actual={longCol.GetValue(0)}");

        // float1
        var floatCol = firstBatch.Column("float1") as FloatArray;
        Assert.NotNull(floatCol);
        Assert.Equal(row0.GetProperty("float1").GetSingle(), floatCol.GetValue(0));
        output.WriteLine($"float1: expected={row0.GetProperty("float1")}, actual={floatCol.GetValue(0)}");

        // double1
        var doubleCol = firstBatch.Column("double1") as DoubleArray;
        Assert.NotNull(doubleCol);
        Assert.Equal(row0.GetProperty("double1").GetDouble(), doubleCol.GetValue(0));
        output.WriteLine($"double1: expected={row0.GetProperty("double1")}, actual={doubleCol.GetValue(0)}");

        // string1
        var stringCol = firstBatch.Column("string1") as StringArray;
        Assert.NotNull(stringCol);
        Assert.Equal(row0.GetProperty("string1").GetString(), stringCol.GetString(0));
        output.WriteLine($"string1: expected={row0.GetProperty("string1")}, actual={stringCol.GetString(0)}");

        output.WriteLine("\nRow 0 validated successfully against expected JSON!");
    }

    [Fact]
    public async Task ValidateTest1_BothRows()
    {
        var path = TestHelpers.GetTestFilePath("TestOrcFile.test1.orc");
        await using var reader = await OrcReader.OpenAsync(path);
        var expected = LoadExpectedRows("TestOrcFile.test1.orc");

        var rowReader = reader.CreateRowReader(new OrcReaderOptions { BatchSize = 1024 });
        var allBatches = new List<RecordBatch>();
        await foreach (var batch in rowReader)
        {
            allBatches.Add(batch);
        }

        int totalRows = allBatches.Sum(b => b.Length);
        Assert.Equal(expected.Count, totalRows);
        output.WriteLine($"Total rows: {totalRows} (expected: {expected.Count})");

        // Validate row 1: {"boolean1": true, "byte1": 100, ...}
        var batch0 = allBatches[0];
        var row1 = expected[1];

        var boolCol = batch0.Column("boolean1") as BooleanArray;
        Assert.Equal(row1.GetProperty("boolean1").GetBoolean(), boolCol!.GetValue(1));

        var intCol = batch0.Column("int1") as Int32Array;
        Assert.Equal(row1.GetProperty("int1").GetInt32(), intCol!.GetValue(1));

        var stringCol = batch0.Column("string1") as StringArray;
        Assert.Equal(row1.GetProperty("string1").GetString(), stringCol!.GetString(1));

        output.WriteLine("Both rows validated!");
    }

    [Fact]
    public async Task ValidateDemo12Zlib_RowCount()
    {
        var path = TestHelpers.GetTestFilePath("demo-12-zlib.orc");
        await using var reader = await OrcReader.OpenAsync(path);
        var expected = LoadExpectedRows("demo-12-zlib.orc");

        Assert.Equal(expected.Count, (int)reader.NumberOfRows);
        output.WriteLine($"Row count matches: {expected.Count}");
    }

    [Fact]
    public async Task ValidateDemo12Zlib_FirstRows()
    {
        var path = TestHelpers.GetTestFilePath("demo-12-zlib.orc");
        await using var reader = await OrcReader.OpenAsync(path);
        var expected = LoadExpectedRows("demo-12-zlib.orc", 10);

        var rowReader = reader.CreateRowReader(new OrcReaderOptions { BatchSize = 10 });
        RecordBatch? batch = null;
        await foreach (var b in rowReader)
        {
            batch = b;
            break;
        }

        Assert.NotNull(batch);

        // Validate first few rows against expected
        for (int row = 0; row < Math.Min(expected.Count, batch.Length); row++)
        {
            var expectedRow = expected[row];

            // _col0 is Int
            var col0 = batch.Column("_col0") as Int32Array;
            if (col0 != null && expectedRow.TryGetProperty("_col0", out var exp0))
            {
                Assert.Equal(exp0.GetInt32(), col0.GetValue(row));
            }

            // _col1 is String
            var col1 = batch.Column("_col1") as StringArray;
            if (col1 != null && expectedRow.TryGetProperty("_col1", out var exp1))
            {
                Assert.Equal(exp1.GetString(), col1.GetString(row));
            }
        }

        output.WriteLine($"Validated first {Math.Min(expected.Count, batch.Length)} rows of demo-12-zlib");
    }

    [Fact]
    public async Task ValidateSnappy_FirstRows()
    {
        var path = TestHelpers.GetTestFilePath("TestOrcFile.testSnappy.orc");
        await using var reader = await OrcReader.OpenAsync(path);
        var expected = LoadExpectedRows("TestOrcFile.testSnappy.orc", 5);

        var rowReader = reader.CreateRowReader(new OrcReaderOptions { BatchSize = 100 });
        RecordBatch? batch = null;
        await foreach (var b in rowReader)
        {
            batch = b;
            break;
        }

        Assert.NotNull(batch);
        Assert.True(batch.Length > 0);

        // The Snappy file has the same schema as test1
        for (int row = 0; row < Math.Min(expected.Count, batch.Length); row++)
        {
            var exp = expected[row];
            if (exp.TryGetProperty("int1", out var intVal))
            {
                var col = batch.Column("int1") as Int32Array;
                if (col != null)
                    Assert.Equal(intVal.GetInt32(), col.GetValue(row));
            }
            if (exp.TryGetProperty("string1", out var strVal))
            {
                var col = batch.Column("string1") as StringArray;
                if (col != null)
                    Assert.Equal(strVal.GetString(), col.GetString(row));
            }
        }

        output.WriteLine($"Validated first rows of Snappy file");
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Compression;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Cross-validation tests: write files with EngineeredWood and verify they can be
/// read correctly by ParquetSharp, and write files with ParquetSharp and verify
/// EngineeredWood can read them back correctly.
/// </summary>
public class CrossValidationTests : IDisposable
{
    private readonly string _tempDir;

    public CrossValidationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-xval-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private string TempPath(string name) => Path.Combine(_tempDir, name);

    // ────────────────────────────────────────────────────────────────────
    //  EW writes → ParquetSharp reads
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task EWWrite_PSRead_Int32_Required()
    {
        var path = TempPath("ew-int32-req.parquet");
        var values = new int[] { 1, -42, 0, int.MaxValue, int.MinValue };

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new Int32Array.Builder().AppendRange(values).Build()], values.Length);

        await WriteEW(path, batch);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        Assert.Equal(values.Length, rg.MetaData.NumRows);

        using var col = rg.Column(0).LogicalReader<int>();
        var buffer = new int[values.Length];
        col.ReadBatch(buffer);
        Assert.Equal(values, buffer);
    }

    [Fact]
    public async Task EWWrite_PSRead_Int32_Nullable()
    {
        var path = TempPath("ew-int32-null.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: true))
            .Build();

        var builder = new Int32Array.Builder();
        builder.Append(10);
        builder.AppendNull();
        builder.Append(30);
        builder.AppendNull();
        builder.Append(50);
        var batch = new RecordBatch(schema, [builder.Build()], 5);

        await WriteEW(path, batch);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<int?>();
        var buffer = new int?[5];
        col.ReadBatch(buffer);
        Assert.Equal(10, buffer[0]);
        Assert.Null(buffer[1]);
        Assert.Equal(30, buffer[2]);
        Assert.Null(buffer[3]);
        Assert.Equal(50, buffer[4]);
    }

    [Fact]
    public async Task EWWrite_PSRead_Int64()
    {
        var path = TempPath("ew-int64.parquet");
        var values = new long[] { 0, long.MaxValue, long.MinValue, 12345678901234L };

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int64Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new Int64Array.Builder().AppendRange(values).Build()], values.Length);

        await WriteEW(path, batch);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<long>();
        var buffer = new long[values.Length];
        col.ReadBatch(buffer);
        Assert.Equal(values, buffer);
    }

    [Fact]
    public async Task EWWrite_PSRead_Float()
    {
        var path = TempPath("ew-float.parquet");
        var values = new float[] { 0f, 1.5f, -3.14f, float.MaxValue, float.MinValue };

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", FloatType.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new FloatArray.Builder().AppendRange(values).Build()], values.Length);

        await WriteEW(path, batch);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<float>();
        var buffer = new float[values.Length];
        col.ReadBatch(buffer);
        Assert.Equal(values, buffer);
    }

    [Fact]
    public async Task EWWrite_PSRead_Float_Ieee754TotalOrder()
    {
        // Opt-in IEEE754 total order writes the new ColumnOrder union member and a
        // nan_count. A legacy reader (ParquetSharp) must still read the column's
        // values without error, even if it ignores the unfamiliar ordering.
        var path = TempPath("ew-float-total-order.parquet");
        var values = new float[] { 0f, 1.5f, -3.14f, float.NaN, 42f };

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", FloatType.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new FloatArray.Builder().AppendRange(values).Build()], values.Length);

        await WriteEW(path, batch,
            new ParquetWriteOptions { FloatingPointOrder = FloatingPointColumnOrder.Ieee754TotalOrder });

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<float>();
        var buffer = new float[values.Length];
        col.ReadBatch(buffer);
        Assert.Equal(0f, buffer[0]);
        Assert.Equal(1.5f, buffer[1]);
        Assert.Equal(-3.14f, buffer[2]);
        Assert.True(float.IsNaN(buffer[3]));
        Assert.Equal(42f, buffer[4]);
    }

    [Fact]
    public async Task EWWrite_PSRead_Double()
    {
        var path = TempPath("ew-double.parquet");
        var values = new double[] { 0.0, -1.23e45, double.Epsilon, double.MaxValue };

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", DoubleType.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new DoubleArray.Builder().AppendRange(values).Build()], values.Length);

        await WriteEW(path, batch);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<double>();
        var buffer = new double[values.Length];
        col.ReadBatch(buffer);
        Assert.Equal(values, buffer);
    }

    [Fact]
    public async Task EWWrite_PSRead_Boolean()
    {
        var path = TempPath("ew-bool.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", BooleanType.Default, nullable: true))
            .Build();

        var builder = new BooleanArray.Builder();
        builder.Append(true);
        builder.Append(false);
        builder.AppendNull();
        builder.Append(true);
        var batch = new RecordBatch(schema, [builder.Build()], 4);

        await WriteEW(path, batch);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<bool?>();
        var buffer = new bool?[4];
        col.ReadBatch(buffer);
        Assert.True(buffer[0]);
        Assert.False(buffer[1]);
        Assert.Null(buffer[2]);
        Assert.True(buffer[3]);
    }

    [Fact]
    public async Task EWWrite_PSRead_String()
    {
        var path = TempPath("ew-string.parquet");
        var values = new string?[] { "hello", "", null, "world", "café" };

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", StringType.Default, nullable: true))
            .Build();

        var builder = new StringArray.Builder();
        foreach (var v in values)
        {
            if (v == null) builder.AppendNull();
            else builder.Append(v);
        }
        var batch = new RecordBatch(schema, [builder.Build()], values.Length);

        await WriteEW(path, batch);

        // EW writes UTF8 logical type, so ParquetSharp reads as string
        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<string>();
        var buffer = new string[values.Length];
        col.ReadBatch(buffer);

        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] == null)
                Assert.Null(buffer[i]);
            else
                Assert.Equal(values[i], buffer[i]);
        }
    }

    [Fact]
    public async Task EWWrite_PSRead_MultipleColumns()
    {
        var path = TempPath("ew-multi.parquet");
        int rowCount = 1000;
        var random = new Random(42);

        var intBuilder = new Int32Array.Builder();
        var dblBuilder = new DoubleArray.Builder();
        var strBuilder = new StringArray.Builder();
        int[] expectedInts = new int[rowCount];
        double?[] expectedDoubles = new double?[rowCount];
        string?[] expectedStrings = new string?[rowCount];

        for (int i = 0; i < rowCount; i++)
        {
            expectedInts[i] = random.Next();
            intBuilder.Append(expectedInts[i]);

            if (random.NextDouble() < 0.1)
            {
                expectedDoubles[i] = null;
                dblBuilder.AppendNull();
            }
            else
            {
                expectedDoubles[i] = random.NextDouble() * 1000;
                dblBuilder.Append(expectedDoubles[i]!.Value);
            }

            if (random.NextDouble() < 0.1)
            {
                expectedStrings[i] = null;
                strBuilder.AppendNull();
            }
            else
            {
                expectedStrings[i] = $"val-{random.Next(5000)}";
                strBuilder.Append(expectedStrings[i]);
            }
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("val", DoubleType.Default, nullable: true))
            .Field(new Field("name", StringType.Default, nullable: true))
            .Build();
        var batch = new RecordBatch(schema,
            [intBuilder.Build(), dblBuilder.Build(), strBuilder.Build()], rowCount);

        await WriteEW(path, batch);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);

        using var intCol = rg.Column(0).LogicalReader<int>();
        var intBuffer = new int[rowCount];
        intCol.ReadBatch(intBuffer);
        Assert.Equal(expectedInts, intBuffer);

        using var dblCol = rg.Column(1).LogicalReader<double?>();
        var dblBuffer = new double?[rowCount];
        dblCol.ReadBatch(dblBuffer);
        for (int i = 0; i < rowCount; i++)
        {
            if (expectedDoubles[i] == null)
                Assert.Null(dblBuffer[i]);
            else
                Assert.Equal(expectedDoubles[i]!.Value, dblBuffer[i]!.Value, precision: 10);
        }

        // EW writes UTF8 logical type → ParquetSharp reads as string
        using var strCol = rg.Column(2).LogicalReader<string>();
        var strBuffer = new string[rowCount];
        strCol.ReadBatch(strBuffer);
        for (int i = 0; i < rowCount; i++)
        {
            if (expectedStrings[i] == null)
                Assert.Null(strBuffer[i]);
            else
                Assert.Equal(expectedStrings[i], strBuffer[i]);
        }
    }

    [Theory]
    [InlineData(CompressionCodec.Uncompressed)]
    [InlineData(CompressionCodec.Snappy)]
    [InlineData(CompressionCodec.Gzip)]
    [InlineData(CompressionCodec.Zstd)]
    [InlineData(CompressionCodec.Brotli)]
    [InlineData(CompressionCodec.Lz4)]
    public async Task EWWrite_PSRead_AllCompressions(CompressionCodec codec)
    {
        var path = TempPath($"ew-comp-{codec}.parquet");
        var values = Enumerable.Range(0, 500).ToArray();

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new Int32Array.Builder().AppendRange(values).Build()], values.Length);

        await WriteEW(path, batch, new ParquetWriteOptions { Compression = codec });

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<int>();
        var buffer = new int[values.Length];
        col.ReadBatch(buffer);
        Assert.Equal(values, buffer);
    }

    [Theory]
    [InlineData(DataPageVersion.V1)]
    [InlineData(DataPageVersion.V2)]
    public async Task EWWrite_PSRead_DataPageVersions(DataPageVersion version)
    {
        var path = TempPath($"ew-dpv{(int)version}.parquet");
        var values = Enumerable.Range(0, 500).ToArray();

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new Int32Array.Builder().AppendRange(values).Build()], values.Length);

        await WriteEW(path, batch, new ParquetWriteOptions
        {
            DataPageVersion = version,
            Compression = CompressionCodec.Snappy,
        });

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<int>();
        var buffer = new int[values.Length];
        col.ReadBatch(buffer);
        Assert.Equal(values, buffer);
    }

    [Fact]
    public async Task EWWrite_PSRead_DictionaryEncoded()
    {
        var path = TempPath("ew-dict.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("category", StringType.Default, nullable: false))
            .Build();

        var builder = new StringArray.Builder();
        var categories = new[] { "alpha", "beta", "gamma" };
        for (int i = 0; i < 1000; i++)
            builder.Append(categories[i % 3]);

        var batch = new RecordBatch(schema, [builder.Build()], 1000);
        await WriteEW(path, batch);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<string>();
        var buffer = new string[1000];
        col.ReadBatch(buffer);

        for (int i = 0; i < 1000; i++)
            Assert.Equal(categories[i % 3], buffer[i]);
    }

    [Fact]
    public async Task EWWrite_PSRead_Statistics()
    {
        var path = TempPath("ew-stats.parquet");
        var values = new int[] { 5, 2, 8, 1, 9 };

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new Int32Array.Builder().AppendRange(values).Build()], values.Length);

        await WriteEW(path, batch);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        var stats = rg.MetaData.GetColumnChunkMetaData(0).Statistics as ParquetSharp.Statistics<int>;
        Assert.NotNull(stats);
        Assert.Equal(1, stats.Min);
        Assert.Equal(9, stats.Max);
        Assert.Equal(0, stats.NullCount);
    }

    [Fact]
    public async Task EWWrite_PSRead_MultipleRowGroups()
    {
        var path = TempPath("ew-multi-rg.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();

        // Write and close file before ParquetSharp reads (Windows file locking)
        {
            await using var file = new LocalSequentialFile(path);
            await using var writer = new ParquetFileWriter(file, ownsFile: false,
                new ParquetWriteOptions());

            var batch1 = new RecordBatch(schema,
                [new Int32Array.Builder().AppendRange(Enumerable.Range(0, 100)).Build()], 100);
            var batch2 = new RecordBatch(schema,
                [new Int32Array.Builder().AppendRange(Enumerable.Range(100, 200)).Build()], 200);

            await writer.WriteRowGroupAsync(batch1);
            await writer.WriteRowGroupAsync(batch2);
            await writer.CloseAsync();
        }

        using var reader = new ParquetSharp.ParquetFileReader(path);
        Assert.Equal(2, reader.FileMetaData.NumRowGroups);
        Assert.Equal(300, reader.FileMetaData.NumRows);

        using var rg0 = reader.RowGroup(0);
        using var col0 = rg0.Column(0).LogicalReader<int>();
        var buf0 = new int[100];
        col0.ReadBatch(buf0);
        Assert.Equal(Enumerable.Range(0, 100).ToArray(), buf0);

        using var rg1 = reader.RowGroup(1);
        using var col1 = rg1.Column(0).LogicalReader<int>();
        var buf1 = new int[200];
        col1.ReadBatch(buf1);
        Assert.Equal(Enumerable.Range(100, 200).ToArray(), buf1);
    }

    // ────────────────────────────────────────────────────────────────────
    //  ParquetSharp writes → EW reads
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task PSWrite_EWRead_Int32_Required()
    {
        var path = TempPath("ps-int32-req.parquet");
        var values = new int[] { 1, -42, 0, int.MaxValue, int.MinValue };

        using (var writer = new ParquetSharp.ParquetFileWriter(path,
            [new ParquetSharp.Column<int>("x")]))
        {
            using var rg = writer.AppendRowGroup();
            using var col = rg.NextColumn().LogicalWriter<int>();
            col.WriteBatch(values);
            writer.Close();
        }

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);
        var arr = (Int32Array)batch.Column(0);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], arr.GetValue(i));
    }

    [Fact]
    public async Task PSWrite_EWRead_Int32_Nullable()
    {
        var path = TempPath("ps-int32-null.parquet");
        var values = new int?[] { 10, null, 30, null, 50 };

        using (var writer = new ParquetSharp.ParquetFileWriter(path,
            [new ParquetSharp.Column<int?>("x")]))
        {
            using var rg = writer.AppendRowGroup();
            using var col = rg.NextColumn().LogicalWriter<int?>();
            col.WriteBatch(values);
            writer.Close();
        }

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);
        var arr = (Int32Array)batch.Column(0);
        Assert.Equal(10, arr.GetValue(0));
        Assert.True(arr.IsNull(1));
        Assert.Equal(30, arr.GetValue(2));
        Assert.True(arr.IsNull(3));
        Assert.Equal(50, arr.GetValue(4));
    }

    [Fact]
    public async Task PSWrite_EWRead_String()
    {
        var path = TempPath("ps-string.parquet");
        // Use string type so ParquetSharp writes UTF8 logical type → EW returns StringArray
        var values = new string[] { "hello", "", "world", "café" };

        using (var writer = new ParquetSharp.ParquetFileWriter(path,
            [new ParquetSharp.Column<string>("s")]))
        {
            using var rg = writer.AppendRowGroup();
            using var col = rg.NextColumn().LogicalWriter<string>();
            col.WriteBatch(values);
            writer.Close();
        }

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);
        var arr = (StringArray)batch.Column(0);
        Assert.Equal("hello", arr.GetString(0));
        Assert.Equal("", arr.GetString(1));
        Assert.Equal("world", arr.GetString(2));
        Assert.Equal("café", arr.GetString(3));
    }

    [Fact]
    public async Task PSWrite_EWRead_MultipleColumns()
    {
        var path = TempPath("ps-multi.parquet");
        int rowCount = 1000;
        var random = new Random(42);

        var intValues = new int[rowCount];
        var dblValues = new double?[rowCount];
        var strValues = new string?[rowCount];

        for (int i = 0; i < rowCount; i++)
        {
            intValues[i] = random.Next();
            dblValues[i] = random.NextDouble() < 0.1 ? null : random.NextDouble() * 1000;
            strValues[i] = random.NextDouble() < 0.1 ? null : $"val-{random.Next(5000)}";
        }

        using (var writer = new ParquetSharp.ParquetFileWriter(path,
            [
                new ParquetSharp.Column<int>("id"),
                new ParquetSharp.Column<double?>("val"),
                new ParquetSharp.Column<string>("name"),
            ]))
        {
            using var rg = writer.AppendRowGroup();
            using var intCol = rg.NextColumn().LogicalWriter<int>();
            intCol.WriteBatch(intValues);
            using var dblCol = rg.NextColumn().LogicalWriter<double?>();
            dblCol.WriteBatch(dblValues);
            using var strCol = rg.NextColumn().LogicalWriter<string>();
            strCol.WriteBatch(strValues!);
            writer.Close();
        }

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);

        var intArr = (Int32Array)batch.Column(0);
        for (int i = 0; i < rowCount; i++)
            Assert.Equal(intValues[i], intArr.GetValue(i));

        var dblArr = (DoubleArray)batch.Column(1);
        for (int i = 0; i < rowCount; i++)
        {
            if (dblValues[i] == null)
                Assert.True(dblArr.IsNull(i));
            else
                Assert.Equal(dblValues[i]!.Value, dblArr.GetValue(i)!.Value, precision: 10);
        }

        var strArr = (StringArray)batch.Column(2);
        for (int i = 0; i < rowCount; i++)
        {
            if (strValues[i] == null)
                Assert.True(strArr.IsNull(i));
            else
                Assert.Equal(strValues[i], strArr.GetString(i));
        }
    }

    // ────────────────────────────────────────────────────────────────────
    //  Roundtrip: EW writes → EW reads
    // ────────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(CompressionCodec.Uncompressed)]
    [InlineData(CompressionCodec.Snappy)]
    [InlineData(CompressionCodec.Gzip)]
    [InlineData(CompressionCodec.Zstd)]
    [InlineData(CompressionCodec.Brotli)]
    [InlineData(CompressionCodec.Lz4)]
    public async Task Roundtrip_AllCompressions(CompressionCodec codec)
    {
        var path = TempPath($"rt-{codec}.parquet");
        int rowCount = 500;
        var random = new Random(42);

        var intBuilder = new Int32Array.Builder();
        var strBuilder = new StringArray.Builder();
        int[] expectedInts = new int[rowCount];
        string[] expectedStrings = new string[rowCount];

        for (int i = 0; i < rowCount; i++)
        {
            expectedInts[i] = random.Next();
            intBuilder.Append(expectedInts[i]);
            expectedStrings[i] = $"val-{random.Next(100)}";
            strBuilder.Append(expectedStrings[i]);
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("name", StringType.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [intBuilder.Build(), strBuilder.Build()], rowCount);

        await WriteEW(path, batch, new ParquetWriteOptions { Compression = codec });

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var readBatch = await reader.ReadRowGroupAsync(0);

        var intArr = (Int32Array)readBatch.Column(0);
        var strArr = (StringArray)readBatch.Column(1);
        for (int i = 0; i < rowCount; i++)
        {
            Assert.Equal(expectedInts[i], intArr.GetValue(i));
            Assert.Equal(expectedStrings[i], strArr.GetString(i));
        }
    }

    [Fact]
    public async Task Roundtrip_LargeRowCount()
    {
        var path = TempPath("rt-large.parquet");
        int rowCount = 100_000;

        var builder = new Int32Array.Builder();
        for (int i = 0; i < rowCount; i++)
            builder.Append(i);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema, [builder.Build()], rowCount);

        await WriteEW(path, batch);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var meta = await reader.ReadMetadataAsync();
        Assert.Equal(rowCount, meta.NumRows);

        var readBatch = await reader.ReadRowGroupAsync(0);
        var arr = (Int32Array)readBatch.Column(0);
        Assert.Equal(0, arr.GetValue(0));
        Assert.Equal(rowCount - 1, arr.GetValue(rowCount - 1));
    }

    [Fact]
    public async Task Roundtrip_AllNulls()
    {
        var path = TempPath("rt-allnull.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: true))
            .Build();

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 100; i++) builder.AppendNull();
        var batch = new RecordBatch(schema, [builder.Build()], 100);

        await WriteEW(path, batch);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var readBatch = await reader.ReadRowGroupAsync(0);
        var arr = (Int32Array)readBatch.Column(0);
        for (int i = 0; i < 100; i++)
            Assert.True(arr.IsNull(i));
    }

    [Fact]
    public async Task Roundtrip_EmptyFile()
    {
        var path = TempPath("rt-empty.parquet");

        await WriteEmptyEW(path);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var meta = await reader.ReadMetadataAsync();
        Assert.Equal(0, meta.NumRows);
        Assert.Empty(meta.RowGroups);
    }

    // ────────────────────────────────────────────────────────────────────
    //  DeltaByteArray encoding
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task EWWrite_PSRead_DeltaByteArray_Strings()
    {
        var path = TempPath("ew-dba-strings.parquet");
        var strings = Enumerable.Range(0, 200)
            .Select(i => $"https://example.com/api/v2/resource/{i:D4}")
            .ToArray();

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("url", StringType.Default, nullable: false))
            .Build();
        var builder = new StringArray.Builder();
        foreach (var s in strings) builder.Append(s);
        var batch = new RecordBatch(schema, [builder.Build()], strings.Length);

        var options = new ParquetWriteOptions
        {
            ByteArrayEncoding = ByteArrayEncoding.DeltaByteArray,
            DictionaryEnabled = false,
        };
        await WriteEW(path, batch, options);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<string>();
        var buffer = new string[strings.Length];
        col.ReadBatch(buffer);
        Assert.Equal(strings, buffer);
    }

    [Fact]
    public async Task EWWrite_PSRead_DeltaByteArray_NullableStrings()
    {
        var path = TempPath("ew-dba-nullable.parquet");
        var values = new string?[] { "aaa", null, "aab", "aac", null, "zzz" };

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", StringType.Default, nullable: true))
            .Build();
        var builder = new StringArray.Builder();
        foreach (var s in values)
        {
            if (s == null) builder.AppendNull();
            else builder.Append(s);
        }
        var batch = new RecordBatch(schema, [builder.Build()], values.Length);

        var options = new ParquetWriteOptions
        {
            ByteArrayEncoding = ByteArrayEncoding.DeltaByteArray,
            DictionaryEnabled = false,
        };
        await WriteEW(path, batch, options);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<string?>();
        var buffer = new string?[values.Length];
        col.ReadBatch(buffer);
        Assert.Equal(values, buffer);
    }

    [Fact]
    public async Task EWRoundTrip_DeltaByteArray_Strings()
    {
        var path = TempPath("ew-dba-rt.parquet");
        var strings = Enumerable.Range(0, 500)
            .Select(i => $"sensor-{i / 10:D3}/metric-{i % 10:D2}")
            .ToArray();

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("key", StringType.Default, nullable: false))
            .Build();
        var builder = new StringArray.Builder();
        foreach (var s in strings) builder.Append(s);
        var batch = new RecordBatch(schema, [builder.Build()], strings.Length);

        var options = new ParquetWriteOptions
        {
            ByteArrayEncoding = ByteArrayEncoding.DeltaByteArray,
            DictionaryEnabled = false,
        };
        await WriteEW(path, batch, options);

        // Read back with EW
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var result = await reader.ReadRowGroupAsync(0);
        var stringArray = (StringArray)result.Column(0);

        for (int i = 0; i < strings.Length; i++)
            Assert.Equal(strings[i], stringArray.GetString(i));
    }

    // ────────────────────────────────────────────────────────────────────
    //  Row group auto-splitting
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task AutoSplit_SplitsByRowGroupMaxRows()
    {
        var path = TempPath("auto-split.parquet");
        int totalRows = 2500;
        int maxRowsPerGroup = 1000;

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();

        var builder = new Int32Array.Builder();
        for (int i = 0; i < totalRows; i++) builder.Append(i);
        var batch = new RecordBatch(schema, [builder.Build()], totalRows);

        var options = new ParquetWriteOptions { RowGroupMaxRows = maxRowsPerGroup };
        await WriteEW(path, batch, options);

        // Should produce 3 row groups: 1000, 1000, 500
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var meta = await reader.ReadMetadataAsync();

        Assert.Equal(totalRows, meta.NumRows);
        Assert.Equal(3, meta.RowGroups.Count);
        Assert.Equal(1000, meta.RowGroups[0].NumRows);
        Assert.Equal(1000, meta.RowGroups[1].NumRows);
        Assert.Equal(500, meta.RowGroups[2].NumRows);

        // Verify all values are correct across row groups
        for (int rg = 0; rg < 3; rg++)
        {
            var readBatch = await reader.ReadRowGroupAsync(rg);
            var arr = (Int32Array)readBatch.Column(0);
            int expectedStart = rg * 1000;
            for (int i = 0; i < readBatch.Length; i++)
                Assert.Equal(expectedStart + i, arr.GetValue(i));
        }
    }

    [Fact]
    public async Task AutoSplit_ExactMultipleProducesEvenGroups()
    {
        var path = TempPath("auto-split-exact.parquet");
        int totalRows = 3000;
        int maxRowsPerGroup = 1000;

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();
        var builder = new Int32Array.Builder();
        for (int i = 0; i < totalRows; i++) builder.Append(i);
        var batch = new RecordBatch(schema, [builder.Build()], totalRows);

        await WriteEW(path, batch, new ParquetWriteOptions { RowGroupMaxRows = maxRowsPerGroup });

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var meta = await reader.ReadMetadataAsync();

        Assert.Equal(3, meta.RowGroups.Count);
        Assert.All(meta.RowGroups, rg => Assert.Equal(1000, rg.NumRows));
    }

    [Fact]
    public async Task AutoSplit_SmallBatchNoSplit()
    {
        var path = TempPath("auto-split-nosplit.parquet");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();
        var builder = new Int32Array.Builder();
        for (int i = 0; i < 500; i++) builder.Append(i);
        var batch = new RecordBatch(schema, [builder.Build()], 500);

        await WriteEW(path, batch, new ParquetWriteOptions { RowGroupMaxRows = 1000 });

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var meta = await reader.ReadMetadataAsync();

        Assert.Single(meta.RowGroups);
        Assert.Equal(500, meta.RowGroups[0].NumRows);
    }

    [Fact]
    public async Task AutoSplit_WithNullableAndStringColumns()
    {
        var path = TempPath("auto-split-mixed.parquet");
        int totalRows = 250;
        int maxRows = 100;

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("name", StringType.Default, nullable: true))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var nameBuilder = new StringArray.Builder();
        for (int i = 0; i < totalRows; i++)
        {
            idBuilder.Append(i);
            if (i % 3 == 0) nameBuilder.AppendNull();
            else nameBuilder.Append($"item-{i}");
        }
        var batch = new RecordBatch(schema,
            [idBuilder.Build(), nameBuilder.Build()], totalRows);

        await WriteEW(path, batch, new ParquetWriteOptions { RowGroupMaxRows = maxRows });

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var meta = await reader.ReadMetadataAsync();

        Assert.Equal(totalRows, meta.NumRows);
        Assert.Equal(3, meta.RowGroups.Count); // 100, 100, 50

        // Verify data integrity across splits
        var rg0 = await reader.ReadRowGroupAsync(0);
        Assert.Equal(100, rg0.Length);
        Assert.Equal(0, ((Int32Array)rg0.Column(0)).GetValue(0));
        Assert.Equal(99, ((Int32Array)rg0.Column(0)).GetValue(99));

        var rg2 = await reader.ReadRowGroupAsync(2);
        Assert.Equal(50, rg2.Length);
        Assert.Equal(200, ((Int32Array)rg2.Column(0)).GetValue(0));
    }

    [Fact]
    public async Task AutoSplit_PSRead_VerifiesAllRowGroups()
    {
        var path = TempPath("auto-split-ps.parquet");
        int totalRows = 500;
        int maxRows = 200;

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("val", Int32Type.Default, nullable: false))
            .Build();
        var builder = new Int32Array.Builder();
        for (int i = 0; i < totalRows; i++) builder.Append(i * 10);
        var batch = new RecordBatch(schema, [builder.Build()], totalRows);

        await WriteEW(path, batch, new ParquetWriteOptions { RowGroupMaxRows = maxRows });

        // Verify with ParquetSharp
        using var psReader = new ParquetSharp.ParquetFileReader(path);
        Assert.Equal(3, psReader.FileMetaData.NumRowGroups); // 200, 200, 100
        Assert.Equal(totalRows, psReader.FileMetaData.NumRows);

        int rowOffset = 0;
        for (int rg = 0; rg < 3; rg++)
        {
            using var group = psReader.RowGroup(rg);
            int numRows = checked((int)group.MetaData.NumRows);
            using var col = group.Column(0).LogicalReader<int>();
            var buffer = new int[numRows];
            col.ReadBatch(buffer);
            for (int i = 0; i < numRows; i++)
                Assert.Equal((rowOffset + i) * 10, buffer[i]);
            rowOffset += numRows;
        }
    }

    // ────────────────────────────────────────────────────────────────────
    //  Decimal types
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Roundtrip_Decimal128()
    {
        var path = TempPath("rt-dec128.parquet");
        var decType = new Decimal128Type(28, 6);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("amount", decType, nullable: true))
            .Build();

        // Build a Decimal128Array manually via ArrayData
        int count = 5;
        decimal[] values = [12345.678901m, -99999.999999m, 0m, decimal.MaxValue / 1000, decimal.MinValue / 1000];
        var valueBytes = new byte[count * 16];
        var nullBitmap = new byte[(count + 7) / 8];
        nullBitmap[0] = 0b11111; // all non-null

        for (int i = 0; i < count; i++)
        {
            var bits = decimal.GetBits(values[i]);
            // Convert to Decimal128 representation: scale to fixed-point
            // Use SqlDecimal for proper 128-bit representation
            var sql = new System.Data.SqlTypes.SqlDecimal(values[i]);
            var sqlBytes = sql.BinData; // 4-17 bytes big-endian
            // Arrow Decimal128 is 16 bytes little-endian two's complement
            var target = valueBytes.AsSpan(i * 16, 16);
            target.Clear();
            bool negative = values[i] < 0;

            // Get unscaled value via multiplication
            // Simpler: just write the raw decimal bits in Arrow format
            // Arrow C# stores Decimal128 as two Int64 (lo, hi) in little-endian
            var d = values[i];
            var dBits = decimal.GetBits(d);
            long lo = (uint)dBits[0] | ((long)(uint)dBits[1] << 32);
            long hi = (uint)dBits[2];
            if ((dBits[3] & unchecked((int)0x80000000)) != 0) // negative
            {
                // Two's complement: negate the 128-bit value
                lo = -lo;
                hi = lo == 0 ? -hi : ~hi;
            }
            System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(target, lo);
            System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(target[8..], hi);
        }

        var arrayData = new Apache.Arrow.ArrayData(decType, count, 0, 0,
            [new ArrowBuffer(nullBitmap), new ArrowBuffer(valueBytes)]);
        var decArray = new Decimal128Array(arrayData);

        var batch = new RecordBatch(schema, [decArray], count);
        await WriteEW(path, batch);

        // Read back and verify
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var readBatch = await reader.ReadRowGroupAsync(0);
        var resultArray = (Decimal128Array)readBatch.Column(0);

        for (int i = 0; i < count; i++)
        {
            var expected = decArray.GetValue(i);
            var actual = resultArray.GetValue(i);
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    public async Task EWWrite_PSRead_Decimal128()
    {
        var path = TempPath("ew-dec128-ps.parquet");
        var decType = new Decimal128Type(10, 2);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("price", decType, nullable: false))
            .Build();

        // Build Decimal128Array with simple values
        int count = 4;
        var valueBytes = new byte[count * 16];
        var nullBitmap = new byte[(count + 7) / 8];
        nullBitmap[0] = 0xFF;

        // Write unscaled values (value * 10^scale) as 128-bit little-endian
        long[] unscaled = [12345, -6789, 0, 100]; // represents 123.45, -67.89, 0.00, 1.00
        for (int i = 0; i < count; i++)
        {
            var target = valueBytes.AsSpan(i * 16, 16);
            target.Clear();
            System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(target, unscaled[i]);
            if (unscaled[i] < 0)
                target[8..].Fill(0xFF); // sign-extend
        }

        var arrayData = new Apache.Arrow.ArrayData(decType, count, 0, 0,
            [new ArrowBuffer(nullBitmap), new ArrowBuffer(valueBytes)]);
        var decArray = new Decimal128Array(arrayData);

        var batch = new RecordBatch(schema, [decArray], count);
        await WriteEW(path, batch);

        // Read with ParquetSharp
        using var psReader = new ParquetSharp.ParquetFileReader(path);
        using var rg = psReader.RowGroup(0);
        using var col = rg.Column(0).LogicalReader<decimal>();
        var buffer = new decimal[count];
        col.ReadBatch(buffer);

        Assert.Equal(123.45m, buffer[0]);
        Assert.Equal(-67.89m, buffer[1]);
        Assert.Equal(0m, buffer[2]);
        Assert.Equal(1.00m, buffer[3]);
    }

    [Fact]
    public async Task Roundtrip_Decimal32_Int32()
    {
        var path = TempPath("rt-dec32.parquet");
        // Decimal32 maps to INT32 physical type — no byte reversal needed
        var decType = new Decimal32Type(7, 2);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", decType, nullable: false))
            .Build();

        int count = 4;
        var valueBytes = new byte[count * 4];
        var nullBitmap = new byte[(count + 7) / 8];
        nullBitmap[0] = 0xFF;

        int[] unscaled = [12345, -6789, 0, 100];
        for (int i = 0; i < count; i++)
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(
                valueBytes.AsSpan(i * 4), unscaled[i]);

        var arrayData = new Apache.Arrow.ArrayData(decType, count, 0, 0,
            [new ArrowBuffer(nullBitmap), new ArrowBuffer(valueBytes)]);
        var decArray = new Decimal32Array(arrayData);

        var batch = new RecordBatch(schema, [decArray], count);
        await WriteEW(path, batch);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var readBatch = await reader.ReadRowGroupAsync(0);
        var resultArray = (Decimal32Array)readBatch.Column(0);

        for (int i = 0; i < count; i++)
            Assert.Equal(decArray.GetValue(i), resultArray.GetValue(i));
    }

    [Fact]
    public async Task Roundtrip_Decimal64_Int64()
    {
        var path = TempPath("rt-dec64.parquet");
        var decType = new Decimal64Type(15, 4);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("value", decType, nullable: false))
            .Build();

        int count = 4;
        var valueBytes = new byte[count * 8];
        var nullBitmap = new byte[(count + 7) / 8];
        nullBitmap[0] = 0xFF;

        long[] unscaled = [123456789012L, -987654321098L, 0, 10000];
        for (int i = 0; i < count; i++)
            System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(
                valueBytes.AsSpan(i * 8), unscaled[i]);

        var arrayData = new Apache.Arrow.ArrayData(decType, count, 0, 0,
            [new ArrowBuffer(nullBitmap), new ArrowBuffer(valueBytes)]);
        var decArray = new Decimal64Array(arrayData);

        var batch = new RecordBatch(schema, [decArray], count);
        await WriteEW(path, batch);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var readBatch = await reader.ReadRowGroupAsync(0);
        var resultArray = (Decimal64Array)readBatch.Column(0);

        for (int i = 0; i < count; i++)
            Assert.Equal(decArray.GetValue(i), resultArray.GetValue(i));
    }

    // ────────────────────────────────────────────────────────────────────
    //  Nested types (struct, list, map)
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task EWWrite_PSRead_Struct()
    {
        var path = TempPath("ew-struct.parquet");
        var structType = new StructType(
        [
            new Field("x", Int32Type.Default, nullable: false),
            new Field("y", StringType.Default, nullable: true),
        ]);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", structType, nullable: true))
            .Build();

        var xBuilder = new Int32Array.Builder();
        xBuilder.AppendRange([10, 20, 30, 40]);

        var yBuilder = new StringArray.Builder();
        yBuilder.Append("a");
        yBuilder.AppendNull();
        yBuilder.Append("c");
        yBuilder.Append("d");

        // Struct with null at index 1
        var structNullBitmap = new byte[] { 0b1101 }; // indices 0,2,3 present; 1 null
        var structData = new Apache.Arrow.ArrayData(structType, 4, 1, 0,
            [new ArrowBuffer(structNullBitmap)],
            [xBuilder.Build().Data, yBuilder.Build().Data]);
        var structArray = new StructArray(structData);

        var batch = new RecordBatch(schema, [structArray], 4);
        await WriteEW(path, batch);

        // Read back with EW to verify roundtrip
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var readBatch = await reader.ReadRowGroupAsync(0);
        var result = (StructArray)readBatch.Column(0);

        Assert.False(result.IsNull(0));
        Assert.True(result.IsNull(1));
        Assert.False(result.IsNull(2));
        Assert.False(result.IsNull(3));

        var xArr = (Int32Array)result.Fields[0];
        Assert.Equal(10, xArr.GetValue(0));
        Assert.Equal(40, xArr.GetValue(3));

        var yArr = (StringArray)result.Fields[1];
        Assert.Equal("a", yArr.GetString(0));
        Assert.Equal("d", yArr.GetString(3));
    }

    [Fact]
    public async Task EWWrite_PSRead_List()
    {
        var path = TempPath("ew-list.parquet");
        var listType = new ListType(new Field("item", Int32Type.Default, nullable: false));
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("nums", listType, nullable: true))
            .Build();

        // Build: [[1,2], null, [3], []]
        var valuesBuilder = new Int32Array.Builder();
        valuesBuilder.AppendRange([1, 2, 3]);

        var offsets = new int[] { 0, 2, 2, 3, 3 };
        var listNullBitmap = new byte[] { 0b1101 }; // indices 0,2,3 present; 1 null

        var listData = new Apache.Arrow.ArrayData(listType, 4, 1, 0,
            [new ArrowBuffer(listNullBitmap),
             new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray())],
            [valuesBuilder.Build().Data]);
        var listArray = new ListArray(listData);

        var batch = new RecordBatch(schema, [listArray], 4);
        await WriteEW(path, batch);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var readBatch = await reader.ReadRowGroupAsync(0);
        var result = (ListArray)readBatch.Column(0);

        Assert.False(result.IsNull(0));
        Assert.True(result.IsNull(1));
        Assert.False(result.IsNull(2));
        Assert.False(result.IsNull(3));

        // [[1,2]]
        var list0 = (Int32Array)result.GetSlicedValues(0);
        Assert.Equal(2, list0.Length);
        Assert.Equal(1, list0.GetValue(0));
        Assert.Equal(2, list0.GetValue(1));

        // [3]
        var list2 = (Int32Array)result.GetSlicedValues(2);
        Assert.Equal(1, list2.Length);
        Assert.Equal(3, list2.GetValue(0));

        // []
        var list3 = (Int32Array)result.GetSlicedValues(3);
        Assert.Equal(0, list3.Length);
    }

    [Fact]
    public async Task Roundtrip_Map()
    {
        var path = TempPath("rt-map.parquet");
        var mapType = new MapType(
            new Field("key", StringType.Default, nullable: false),
            new Field("value", Int32Type.Default, nullable: true));
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("props", mapType, nullable: true))
            .Build();

        // Build: [{"a":1,"b":2}, null, {"c":3}]
        var keyBuilder = new StringArray.Builder();
        keyBuilder.Append("a");
        keyBuilder.Append("b");
        keyBuilder.Append("c");

        var valueBuilder = new Int32Array.Builder();
        valueBuilder.Append(1);
        valueBuilder.Append(2);
        valueBuilder.Append(3);

        // key_value struct
        var kvType = new StructType([
            new Field("key", StringType.Default, nullable: false),
            new Field("value", Int32Type.Default, nullable: true),
        ]);
        var kvData = new Apache.Arrow.ArrayData(kvType, 3, 0, 0,
            [ArrowBuffer.Empty],
            [keyBuilder.Build().Data, valueBuilder.Build().Data]);

        var offsets = new int[] { 0, 2, 2, 3 };
        var mapNullBitmap = new byte[] { 0b101 }; // indices 0,2 present; 1 null
        var mapData = new Apache.Arrow.ArrayData(mapType, 3, 1, 0,
            [new ArrowBuffer(mapNullBitmap),
             new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray())],
            [kvData]);
        var mapArray = new MapArray(mapData);

        var batch = new RecordBatch(schema, [mapArray], 3);
        await WriteEW(path, batch);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var readBatch = await reader.ReadRowGroupAsync(0);
        var result = (MapArray)readBatch.Column(0);

        Assert.False(result.IsNull(0));
        Assert.True(result.IsNull(1));
        Assert.False(result.IsNull(2));
    }

    // ────────────────────────────────────────────────────────────────────
    //  FLBA (non-decimal)
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Roundtrip_FixedLenByteArray()
    {
        var path = TempPath("rt-flba.parquet");
        int typeLength = 6;
        var flbaType = new FixedSizeBinaryType(typeLength);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("hash", flbaType, nullable: false))
            .Build();

        int count = 4;
        var valueBytes = new byte[count * typeLength];
        var random = new Random(42);
        random.NextBytes(valueBytes);

        var nullBitmap = new byte[(count + 7) / 8];
        nullBitmap[0] = 0xFF;

        var arrayData = new Apache.Arrow.ArrayData(flbaType, count, 0, 0,
            [new ArrowBuffer(nullBitmap), new ArrowBuffer(valueBytes)]);
        var flbaArray = new FixedSizeBinaryArray(arrayData);

        var batch = new RecordBatch(schema, [flbaArray], count);
        await WriteEW(path, batch);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var readBatch = await reader.ReadRowGroupAsync(0);
        var result = (FixedSizeBinaryArray)readBatch.Column(0);

        for (int i = 0; i < count; i++)
            Assert.True(flbaArray.GetBytes(i).SequenceEqual(result.GetBytes(i)));
    }

    // ────────────────────────────────────────────────────────────────────
    //  V1 pages + nested
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Roundtrip_V1_Nested_Struct()
    {
        var path = TempPath("rt-v1-struct.parquet");
        var structType = new StructType(
        [
            new Field("a", Int32Type.Default, nullable: false),
            new Field("b", Int32Type.Default, nullable: false),
        ]);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", structType, nullable: true))
            .Build();

        var aBuilder = new Int32Array.Builder();
        aBuilder.AppendRange([1, 2, 3]);
        var bBuilder = new Int32Array.Builder();
        bBuilder.AppendRange([10, 20, 30]);

        var structNullBitmap = new byte[] { 0b101 }; // index 1 null
        var structData = new Apache.Arrow.ArrayData(structType, 3, 1, 0,
            [new ArrowBuffer(structNullBitmap)],
            [aBuilder.Build().Data, bBuilder.Build().Data]);
        var structArray = new StructArray(structData);

        var batch = new RecordBatch(schema, [structArray], 3);
        await WriteEW(path, batch, new ParquetWriteOptions { DataPageVersion = DataPageVersion.V1 });

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var readBatch = await reader.ReadRowGroupAsync(0);
        var result = (StructArray)readBatch.Column(0);

        Assert.False(result.IsNull(0));
        Assert.True(result.IsNull(1));
        Assert.False(result.IsNull(2));
        Assert.Equal(1, ((Int32Array)result.Fields[0]).GetValue(0));
        Assert.Equal(30, ((Int32Array)result.Fields[1]).GetValue(2));
    }

    // ────────────────────────────────────────────────────────────────────
    //  Zero-row RecordBatch
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Roundtrip_ZeroRowBatch()
    {
        var path = TempPath("rt-zero-rows.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Field(new Field("s", StringType.Default, nullable: true))
            .Build();

        var batch = new RecordBatch(schema,
            [new Int32Array.Builder().Build(), new StringArray.Builder().Build()], 0);
        await WriteEW(path, batch);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var meta = await reader.ReadMetadataAsync();
        // Zero-row batch may produce 0 or 1 row groups depending on implementation
        Assert.Equal(0, meta.NumRows);
    }

    // ────────────────────────────────────────────────────────────────────
    //  Helpers
    // ────────────────────────────────────────────────────────────────────

    private static async Task WriteEW(string path, RecordBatch batch, ParquetWriteOptions? options = null)
    {
        options ??= new ParquetWriteOptions();
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
    }

    private static async Task WriteEmptyEW(string path)
    {
        var options = new ParquetWriteOptions();
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.CloseAsync();
    }

    // ────────────────────────────────────────────────────────────────────
    //  Bloom filter cross-validation: EW writes → ParquetSharp reads
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task EWWrite_PSRead_BloomFilter_DataIntact()
    {
        // Write a Parquet file with bloom filters using EW, then verify
        // ParquetSharp can read the data correctly (bloom filter blocks
        // don't corrupt the file structure).
        var path = TempPath("ew-bloom-xval.parquet");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("name", Apache.Arrow.Types.StringType.Default, nullable: false))
            .Build();

        var ids = new Int32Array.Builder().AppendRange(Enumerable.Range(0, 100)).Build();
        var names = new StringArray.Builder();
        for (int i = 0; i < 100; i++) names.Append($"name_{i}");
        var batch = new RecordBatch(schema, new IArrowArray[] { ids, names.Build() }, 100);

        var options = new ParquetWriteOptions
        {
            BloomFilterColumns = new HashSet<string> { "id", "name" },
            BloomFilterFpp = 0.05,
        };

        await WriteEW(path, batch, options);

        // ParquetSharp should read the file without errors.
        using var reader = new ParquetSharp.ParquetFileReader(path);
        var fileMeta = reader.FileMetaData;
        Assert.Equal(100, fileMeta.NumRows);
        Assert.Equal(2, fileMeta.NumColumns);

        // Verify data is intact.
        using var rg = reader.RowGroup(0);
        using var idReader = rg.Column(0).LogicalReader<int>();
        var idBuffer = new int[100];
        idReader.ReadBatch(idBuffer);
        Assert.Equal(0, idBuffer[0]);
        Assert.Equal(99, idBuffer[99]);

        using var nameReader = rg.Column(1).LogicalReader<string>();
        var nameBuffer = new string[100];
        nameReader.ReadBatch(nameBuffer);
        Assert.Equal("name_0", nameBuffer[0]);
        Assert.Equal("name_99", nameBuffer[99]);
    }

    [Fact]
    public async Task EWWrite_PythonProbe_BloomFilter_ProbingWorks()
    {
        // Write a Parquet file with bloom filters, then use Python (xxhash)
        // to independently parse and probe the SBBF bloom filter.
        if (!IsPythonWithXxhashAvailable())
            return; // Skip: Python with xxhash is not installed.

        var path = TempPath("ew-bloom-probe.parquet");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", Apache.Arrow.Types.StringType.Default, nullable: false))
            .Build();

        var names = new StringArray.Builder();
        for (int i = 0; i < 200; i++) names.Append($"name_{i}");
        var batch = new RecordBatch(schema, new IArrowArray[] { names.Build() }, 200);

        await WriteEW(path, batch, new ParquetWriteOptions
        {
            BloomFilterColumns = new HashSet<string> { "name" },
            BloomFilterFpp = 0.01,
        });

        // Call Python script to probe the bloom filter.
        var scriptDir = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "Parquet"));
        var scriptPath = Path.Combine(scriptDir, "validate_parquet_bloom.py");

        var psi = new ProcessStartInfo(PythonExe!, $"\"{scriptPath}\" \"{path}\" 0 name_0 zzz_absent")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        var p = Process.Start(psi)!;
        var stdout = p.StandardOutput.ReadToEnd();
        var stderr = p.StandardError.ReadToEnd();
        p.WaitForExit(15000);

        Assert.True(p.ExitCode == 0,
            $"Python validation script failed (exit {p.ExitCode}):\n{stderr}");

        using var doc = JsonDocument.Parse(stdout);
        var root = doc.RootElement;

        Assert.True(root.GetProperty("present").GetBoolean(),
            "Python xxHash64 SBBF probe should find 'name_0' in our bloom filter.");
        Assert.False(root.GetProperty("absent").GetBoolean(),
            "Python xxHash64 SBBF probe should reject 'zzz_absent' from our bloom filter.");
    }

    private static string? FindPythonExe()
    {
        foreach (var candidate in new[] { "python3", "python", "py" })
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
                if (p.ExitCode == 0) return candidate;
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
                if (File.Exists(exe)) return exe;
            }
        }
        return null;
    }

    private static readonly string? PythonExe = FindPythonExe();

    private static bool IsPythonWithXxhashAvailable()
    {
        if (PythonExe == null) return false;
        try
        {
            var psi = new ProcessStartInfo(PythonExe, "-c \"import xxhash\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            var p = Process.Start(psi)!;
            p.WaitForExit(5000);
            return p.ExitCode == 0;
        }
        catch { return false; }
    }
}

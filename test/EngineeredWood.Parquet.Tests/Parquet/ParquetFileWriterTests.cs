// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet;

public class ParquetFileWriterTests : IDisposable
{
    private readonly string _tempDir;

    public ParquetFileWriterTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-write-test-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private string TempPath(string name) => Path.Combine(_tempDir, name);

    // --- Schema Conversion Tests ---

    [Fact]
    public void ArrowToSchema_Int32Column()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Build();

        var elements = ArrowToSchemaConverter.Convert(schema);

        Assert.Equal(2, elements.Count);
        Assert.Equal("schema", elements[0].Name);
        Assert.Equal(1, elements[0].NumChildren);
        Assert.Null(elements[0].Type);

        Assert.Equal("id", elements[1].Name);
        Assert.Equal(PhysicalType.Int32, elements[1].Type);
        Assert.Equal(FieldRepetitionType.Required, elements[1].RepetitionType);
    }

    [Fact]
    public void ArrowToSchema_StringColumn()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", StringType.Default, nullable: true))
            .Build();

        var elements = ArrowToSchemaConverter.Convert(schema);

        Assert.Equal("name", elements[1].Name);
        Assert.Equal(PhysicalType.ByteArray, elements[1].Type);
        Assert.Equal(FieldRepetitionType.Optional, elements[1].RepetitionType);
        Assert.IsType<LogicalType.StringType>(elements[1].LogicalType);
        Assert.Equal(ConvertedType.Utf8, elements[1].ConvertedType);
    }

    [Fact]
    public void ArrowToSchema_AllPrimitiveTypes()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("bool_col", BooleanType.Default, false))
            .Field(new Field("int8_col", Int8Type.Default, false))
            .Field(new Field("int16_col", Int16Type.Default, false))
            .Field(new Field("int32_col", Int32Type.Default, false))
            .Field(new Field("int64_col", Int64Type.Default, false))
            .Field(new Field("uint8_col", UInt8Type.Default, false))
            .Field(new Field("float_col", FloatType.Default, false))
            .Field(new Field("double_col", DoubleType.Default, false))
            .Field(new Field("string_col", StringType.Default, true))
            .Field(new Field("binary_col", BinaryType.Default, true))
            .Field(new Field("date_col", Date32Type.Default, false))
            .Build();

        var elements = ArrowToSchemaConverter.Convert(schema);
        Assert.Equal(12, elements.Count); // root + 11 fields
        Assert.Equal(11, elements[0].NumChildren);
    }

    // --- Round-Trip Tests ---

    [Fact]
    public async Task RoundTrip_SingleInt32Column_Required()
    {
        string path = TempPath("int32_required.parquet");
        var values = new int[] { 1, 2, 3, 4, 5 };
        var batch = MakeBatch(
            new Field("id", Int32Type.Default, nullable: false),
            new Int32Array.Builder().AppendRange(values).Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (Int32Array)readBatch.Column(0);
            Assert.Equal(5, col.Length);
            for (int i = 0; i < values.Length; i++)
                Assert.Equal(values[i], col.GetValue(i));
        });
    }

    [Fact]
    public async Task RoundTrip_SingleInt32Column_Optional()
    {
        string path = TempPath("int32_optional.parquet");
        var builder = new Int32Array.Builder();
        builder.Append(1);
        builder.AppendNull();
        builder.Append(3);
        builder.AppendNull();
        builder.Append(5);

        var batch = MakeBatch(
            new Field("id", Int32Type.Default, nullable: true),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (Int32Array)readBatch.Column(0);
            Assert.Equal(5, col.Length);
            Assert.Equal(1, col.GetValue(0));
            Assert.True(col.IsNull(1));
            Assert.Equal(3, col.GetValue(2));
            Assert.True(col.IsNull(3));
            Assert.Equal(5, col.GetValue(4));
        });
    }

    [Fact]
    public async Task RoundTrip_Int64Column()
    {
        string path = TempPath("int64.parquet");
        var values = new long[] { long.MinValue, -1, 0, 1, long.MaxValue };
        var batch = MakeBatch(
            new Field("val", Int64Type.Default, nullable: false),
            new Int64Array.Builder().AppendRange(values).Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (Int64Array)readBatch.Column(0);
            for (int i = 0; i < values.Length; i++)
                Assert.Equal(values[i], col.GetValue(i));
        });
    }

    [Fact]
    public async Task RoundTrip_FloatAndDouble()
    {
        string path = TempPath("float_double.parquet");
        var floats = new float[] { 0f, 1.5f, -3.14f, float.MaxValue, float.MinValue };
        var doubles = new double[] { 0.0, 2.718281828, -1e100, double.MaxValue, double.MinValue };

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("f", FloatType.Default, false))
            .Field(new Field("d", DoubleType.Default, false))
            .Build();

        var batch = new RecordBatch(schema,
            [
                new FloatArray.Builder().AppendRange(floats).Build(),
                new DoubleArray.Builder().AppendRange(doubles).Build()
            ], 5);

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var fCol = (FloatArray)readBatch.Column(0);
            var dCol = (DoubleArray)readBatch.Column(1);
            for (int i = 0; i < 5; i++)
            {
                Assert.Equal(floats[i], fCol.GetValue(i));
                Assert.Equal(doubles[i], dCol.GetValue(i));
            }
        });
    }

    [Fact]
    public async Task RoundTrip_BooleanColumn()
    {
        string path = TempPath("bool.parquet");
        var builder = new BooleanArray.Builder();
        builder.Append(true);
        builder.Append(false);
        builder.Append(true);
        builder.Append(true);
        builder.Append(false);

        var batch = MakeBatch(
            new Field("flag", BooleanType.Default, nullable: false),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (BooleanArray)readBatch.Column(0);
            Assert.True(col.GetValue(0));
            Assert.False(col.GetValue(1));
            Assert.True(col.GetValue(2));
            Assert.True(col.GetValue(3));
            Assert.False(col.GetValue(4));
        });
    }

    [Fact]
    public async Task RoundTrip_StringColumn()
    {
        string path = TempPath("string.parquet");
        var strings = new[] { "hello", "world", "", "parquet", "test" };
        var builder = new StringArray.Builder();
        foreach (var s in strings) builder.Append(s);

        var batch = MakeBatch(
            new Field("text", StringType.Default, nullable: false),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (StringArray)readBatch.Column(0);
            for (int i = 0; i < strings.Length; i++)
                Assert.Equal(strings[i], col.GetString(i));
        });
    }

    [Fact]
    public async Task RoundTrip_StringColumn_WithNulls()
    {
        string path = TempPath("string_null.parquet");
        var builder = new StringArray.Builder();
        builder.Append("hello");
        builder.AppendNull();
        builder.Append("world");
        builder.AppendNull();
        builder.Append("");

        var batch = MakeBatch(
            new Field("text", StringType.Default, nullable: true),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (StringArray)readBatch.Column(0);
            Assert.Equal("hello", col.GetString(0));
            Assert.True(col.IsNull(1));
            Assert.Equal("world", col.GetString(2));
            Assert.True(col.IsNull(3));
            Assert.Equal("", col.GetString(4));
        });
    }

    [Fact]
    public async Task RoundTrip_BinaryColumn()
    {
        string path = TempPath("binary.parquet");
        var builder = new BinaryArray.Builder();
        builder.Append(new byte[] { 1, 2, 3 });
        builder.Append(new byte[] { 4, 5 });
        builder.Append(System.Array.Empty<byte>());
        builder.Append(new byte[] { 6 });

        var batch = MakeBatch(
            new Field("data", BinaryType.Default, nullable: false),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (BinaryArray)readBatch.Column(0);
            Assert.Equal(new byte[] { 1, 2, 3 }, col.GetBytes(0).ToArray());
            Assert.Equal(new byte[] { 4, 5 }, col.GetBytes(1).ToArray());
            Assert.Empty(col.GetBytes(2).ToArray());
            Assert.Equal(new byte[] { 6 }, col.GetBytes(3).ToArray());
        });
    }

    [Fact]
    public async Task RoundTrip_MultipleColumns_MixedTypes()
    {
        string path = TempPath("mixed.parquet");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("value", DoubleType.Default, true))
            .Field(new Field("name", StringType.Default, true))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var valBuilder = new DoubleArray.Builder();
        var nameBuilder = new StringArray.Builder();

        for (int i = 0; i < 100; i++)
        {
            idBuilder.Append(i);
            if (i % 5 == 0) valBuilder.AppendNull(); else valBuilder.Append(i * 1.5);
            if (i % 7 == 0) nameBuilder.AppendNull(); else nameBuilder.Append($"name_{i}");
        }

        var batch = new RecordBatch(schema,
            [idBuilder.Build(), valBuilder.Build(), nameBuilder.Build()], 100);

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            Assert.Equal(100, readBatch.Length);
            Assert.Equal(3, readBatch.ColumnCount);

            var ids = (Int32Array)readBatch.Column(0);
            var vals = (DoubleArray)readBatch.Column(1);
            var names = (StringArray)readBatch.Column(2);

            for (int i = 0; i < 100; i++)
            {
                Assert.Equal(i, ids.GetValue(i));
                if (i % 5 == 0) Assert.True(vals.IsNull(i));
                else Assert.Equal(i * 1.5, vals.GetValue(i));
                if (i % 7 == 0) Assert.True(names.IsNull(i));
                else Assert.Equal($"name_{i}", names.GetString(i));
            }
        });
    }

    [Fact]
    public async Task RoundTrip_LargeDataset_MultiplePages()
    {
        string path = TempPath("large.parquet");
        int rowCount = 100_000;

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("value", Int64Type.Default, false))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var valBuilder = new Int64Array.Builder();
        for (int i = 0; i < rowCount; i++)
        {
            idBuilder.Append(i);
            valBuilder.Append((long)i * 1000);
        }

        var batch = new RecordBatch(schema,
            [idBuilder.Build(), valBuilder.Build()], rowCount);

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            Assert.Equal(rowCount, readBatch.Length);
            var ids = (Int32Array)readBatch.Column(0);
            var vals = (Int64Array)readBatch.Column(1);

            // Spot-check
            Assert.Equal(0, ids.GetValue(0));
            Assert.Equal(0L, vals.GetValue(0));
            Assert.Equal(50_000, ids.GetValue(50_000));
            Assert.Equal(50_000_000L, vals.GetValue(50_000));
            Assert.Equal(rowCount - 1, ids.GetValue(rowCount - 1));
        });
    }

    [Fact]
    public async Task RoundTrip_AllNullColumn()
    {
        string path = TempPath("all_null.parquet");
        var builder = new Int32Array.Builder();
        for (int i = 0; i < 10; i++) builder.AppendNull();

        var batch = MakeBatch(
            new Field("val", Int32Type.Default, nullable: true),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (Int32Array)readBatch.Column(0);
            Assert.Equal(10, col.Length);
            for (int i = 0; i < 10; i++)
                Assert.True(col.IsNull(i));
        });
    }

    [Fact]
    public async Task RoundTrip_DateColumn()
    {
        string path = TempPath("date.parquet");
        var builder = new Date32Array.Builder();
        builder.Append(new DateTime(2024, 1, 15));
        builder.Append(new DateTime(2025, 6, 30));
        builder.Append(new DateTime(1970, 1, 1));

        var batch = MakeBatch(
            new Field("date", Date32Type.Default, nullable: false),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (Date32Array)readBatch.Column(0);
            Assert.Equal(3, col.Length);
            // Date32 stores days since epoch
            Assert.Equal(new DateTime(2024, 1, 15), col.GetDateTimeOffset(0)!.Value.Date);
            Assert.Equal(new DateTime(2025, 6, 30), col.GetDateTimeOffset(1)!.Value.Date);
            Assert.Equal(new DateTime(1970, 1, 1), col.GetDateTimeOffset(2)!.Value.Date);
        });
    }

    [Fact]
    public async Task RoundTrip_WithCompression_Snappy()
    {
        string path = TempPath("snappy.parquet");
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Snappy };

        var values = Enumerable.Range(0, 1000).ToArray();
        var batch = MakeBatch(
            new Field("id", Int32Type.Default, nullable: false),
            new Int32Array.Builder().AppendRange(values).Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (Int32Array)readBatch.Column(0);
            Assert.Equal(1000, col.Length);
            for (int i = 0; i < 1000; i++)
                Assert.Equal(i, col.GetValue(i));
        }, options);
    }

    [Fact]
    public async Task RoundTrip_WithCompression_Zstd()
    {
        string path = TempPath("zstd.parquet");
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Zstd };

        var values = Enumerable.Range(0, 1000).ToArray();
        var batch = MakeBatch(
            new Field("id", Int32Type.Default, nullable: false),
            new Int32Array.Builder().AppendRange(values).Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (Int32Array)readBatch.Column(0);
            Assert.Equal(1000, col.Length);
            for (int i = 0; i < 1000; i++)
                Assert.Equal(i, col.GetValue(i));
        }, options);
    }

    [Fact]
    public async Task RoundTrip_DataPageV1()
    {
        string path = TempPath("v1.parquet");
        var options = new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V1,
            Compression = CompressionCodec.Uncompressed,
        };

        var batch = MakeBatch(
            new Field("id", Int32Type.Default, nullable: true),
            new Int32Array.Builder().Append(1).AppendNull().Append(3).Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (Int32Array)readBatch.Column(0);
            Assert.Equal(3, col.Length);
            Assert.Equal(1, col.GetValue(0));
            Assert.True(col.IsNull(1));
            Assert.Equal(3, col.GetValue(2));
        }, options);
    }

    [Fact]
    public async Task RoundTrip_MultipleRowGroups()
    {
        string path = TempPath("multi_rg.parquet");
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Build();

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            // Write 3 row groups
            for (int rg = 0; rg < 3; rg++)
            {
                int start = rg * 100;
                var builder = new Int32Array.Builder();
                for (int i = 0; i < 100; i++) builder.Append(start + i);
                var batch = new RecordBatch(schema, [builder.Build()], 100);
                await writer.WriteRowGroupAsync(batch);
            }

            await writer.CloseAsync();
        }

        // Read back
        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);

        var metadata = await reader.ReadMetadataAsync();
        Assert.Equal(3, metadata.RowGroups.Count);
        Assert.Equal(300, metadata.NumRows);

        for (int rg = 0; rg < 3; rg++)
        {
            var batch = await reader.ReadRowGroupAsync(rg);
            Assert.Equal(100, batch.Length);
            var col = (Int32Array)batch.Column(0);
            Assert.Equal(rg * 100, col.GetValue(0));
            Assert.Equal(rg * 100 + 99, col.GetValue(99));
        }
    }

    [Fact]
    public async Task RoundTrip_TimestampColumn()
    {
        string path = TempPath("timestamp.parquet");
        var tsType = new TimestampType(Apache.Arrow.Types.TimeUnit.Microsecond, TimeZoneInfo.Utc);

        var builder = new TimestampArray.Builder(tsType);
        var dt1 = new DateTimeOffset(2024, 6, 15, 10, 30, 0, TimeSpan.Zero);
        var dt2 = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        builder.Append(dt1);
        builder.Append(dt2);

        var batch = MakeBatch(
            new Field("ts", tsType, nullable: false),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (TimestampArray)readBatch.Column(0);
            Assert.Equal(2, col.Length);
            Assert.Equal(dt1, col.GetTimestamp(0));
            Assert.Equal(dt2, col.GetTimestamp(1));
        });
    }

    // --- Dictionary Encoding Tests ---

    [Fact]
    public async Task Dictionary_LowCardinalityInt32_RoundTrip()
    {
        string path = TempPath("dict_int32.parquet");
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };

        // 100 rows with only 5 unique values → 5% cardinality, well under 20% threshold
        var builder = new Int32Array.Builder();
        for (int i = 0; i < 100; i++)
            builder.Append(i % 5);

        var batch = MakeBatch(
            new Field("category", Int32Type.Default, nullable: false),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (Int32Array)readBatch.Column(0);
            Assert.Equal(100, col.Length);
            for (int i = 0; i < 100; i++)
                Assert.Equal(i % 5, col.GetValue(i));
        }, options);

        // Verify dictionary encoding was used by checking metadata
        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var colMeta = metadata.RowGroups[0].Columns[0].MetaData!;
        Assert.Contains(Encoding.RleDictionary, colMeta.Encodings);
        Assert.NotNull(colMeta.DictionaryPageOffset);
    }

    [Fact]
    public async Task Dictionary_LowCardinalityString_RoundTrip()
    {
        string path = TempPath("dict_string.parquet");
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };

        var categories = new[] { "alpha", "beta", "gamma", "delta" };
        var builder = new StringArray.Builder();
        for (int i = 0; i < 200; i++)
            builder.Append(categories[i % categories.Length]);

        var batch = MakeBatch(
            new Field("cat", StringType.Default, nullable: false),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (StringArray)readBatch.Column(0);
            Assert.Equal(200, col.Length);
            for (int i = 0; i < 200; i++)
                Assert.Equal(categories[i % categories.Length], col.GetString(i));
        }, options);

        // Verify dictionary encoding was used
        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var colMeta = metadata.RowGroups[0].Columns[0].MetaData!;
        Assert.Contains(Encoding.RleDictionary, colMeta.Encodings);
    }

    [Fact]
    public async Task Dictionary_NullableWithLowCardinality_RoundTrip()
    {
        string path = TempPath("dict_nullable.parquet");
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 100; i++)
        {
            if (i % 10 == 0)
                builder.AppendNull();
            else
                builder.Append(i % 3); // 3 unique values
        }

        var batch = MakeBatch(
            new Field("val", Int32Type.Default, nullable: true),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (Int32Array)readBatch.Column(0);
            Assert.Equal(100, col.Length);
            for (int i = 0; i < 100; i++)
            {
                if (i % 10 == 0)
                    Assert.True(col.IsNull(i));
                else
                    Assert.Equal(i % 3, col.GetValue(i));
            }
        }, options);
    }

    [Fact]
    public async Task Dictionary_HighCardinality_FallsBackToPlain()
    {
        string path = TempPath("dict_fallback.parquet");
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };

        // 100 unique values out of 100 → 100% cardinality, exceeds 20% threshold
        var builder = new Int32Array.Builder();
        for (int i = 0; i < 100; i++)
            builder.Append(i);

        var batch = MakeBatch(
            new Field("id", Int32Type.Default, nullable: false),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (Int32Array)readBatch.Column(0);
            for (int i = 0; i < 100; i++)
                Assert.Equal(i, col.GetValue(i));
        }, options);

        // Verify PLAIN encoding was used (no dictionary)
        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var colMeta = metadata.RowGroups[0].Columns[0].MetaData!;
        Assert.DoesNotContain(Encoding.RleDictionary, colMeta.Encodings);
        Assert.Null(colMeta.DictionaryPageOffset);
    }

    [Fact]
    public async Task Dictionary_Disabled_UsesPlain()
    {
        string path = TempPath("dict_disabled.parquet");
        var options = new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            DictionaryEnabled = false,
        };

        // Low cardinality that would normally trigger dictionary
        var builder = new Int32Array.Builder();
        for (int i = 0; i < 100; i++)
            builder.Append(i % 3);

        var batch = MakeBatch(
            new Field("val", Int32Type.Default, nullable: false),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (Int32Array)readBatch.Column(0);
            for (int i = 0; i < 100; i++)
                Assert.Equal(i % 3, col.GetValue(i));
        }, options);

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var colMeta = metadata.RowGroups[0].Columns[0].MetaData!;
        Assert.DoesNotContain(Encoding.RleDictionary, colMeta.Encodings);
    }

    [Fact]
    public async Task Dictionary_WithCompression_Snappy()
    {
        string path = TempPath("dict_snappy.parquet");
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Snappy };

        var builder = new Int64Array.Builder();
        for (int i = 0; i < 500; i++)
            builder.Append(i % 10);

        var batch = MakeBatch(
            new Field("val", Int64Type.Default, nullable: false),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (Int64Array)readBatch.Column(0);
            Assert.Equal(500, col.Length);
            for (int i = 0; i < 500; i++)
                Assert.Equal(i % 10, col.GetValue(i));
        }, options);
    }

    [Fact]
    public async Task Dictionary_DataPageV1()
    {
        string path = TempPath("dict_v1.parquet");
        var options = new ParquetWriteOptions
        {
            DataPageVersion = DataPageVersion.V1,
            Compression = CompressionCodec.Uncompressed,
        };

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 100; i++)
            builder.Append(i % 4);

        var batch = MakeBatch(
            new Field("val", Int32Type.Default, nullable: false),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (Int32Array)readBatch.Column(0);
            for (int i = 0; i < 100; i++)
                Assert.Equal(i % 4, col.GetValue(i));
        }, options);
    }

    [Fact]
    public async Task Dictionary_DoubleColumn_LowCardinality()
    {
        string path = TempPath("dict_double.parquet");
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };

        var uniqueValues = new[] { 1.1, 2.2, 3.3 };
        var builder = new DoubleArray.Builder();
        for (int i = 0; i < 150; i++)
            builder.Append(uniqueValues[i % 3]);

        var batch = MakeBatch(
            new Field("val", DoubleType.Default, nullable: false),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (DoubleArray)readBatch.Column(0);
            for (int i = 0; i < 150; i++)
                Assert.Equal(uniqueValues[i % 3], col.GetValue(i));
        }, options);

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        Assert.Contains(Encoding.RleDictionary, metadata.RowGroups[0].Columns[0].MetaData!.Encodings);
    }

    [Fact]
    public async Task Dictionary_CrossValidation_ParquetSharp()
    {
        string path = TempPath("dict_cross_ps.parquet");
        var options = new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
        };

        var categories = new[] { "cat_a", "cat_b", "cat_c" };
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("category", StringType.Default, false))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var catBuilder = new StringArray.Builder();
        for (int i = 0; i < 60; i++)
        {
            idBuilder.Append(i % 5);
            catBuilder.Append(categories[i % 3]);
        }

        var batch = new RecordBatch(schema,
            [idBuilder.Build(), catBuilder.Build()], 60);

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        // Read with ParquetSharp
        using var psReader = new ParquetSharp.ParquetFileReader(path);
        using var rowGroup = psReader.RowGroup(0);
        Assert.Equal(60, rowGroup.MetaData.NumRows);

        using var idCol = rowGroup.Column(0).LogicalReader<int>();
        var idValues = new int[60];
        idCol.ReadBatch(idValues);
        for (int i = 0; i < 60; i++)
            Assert.Equal(i % 5, idValues[i]);

        using var catCol = rowGroup.Column(1).LogicalReader<string>();
        var catValues = new string[60];
        catCol.ReadBatch(catValues);
        for (int i = 0; i < 60; i++)
            Assert.Equal(categories[i % 3], catValues[i]);
    }

    // --- V2 Advanced Encoding Tests ---

    [Fact]
    public async Task V2Encoding_Int32_UsesDeltaBinaryPacked()
    {
        string path = TempPath("v2_delta_int32.parquet");
        // High cardinality forces non-dictionary path; V2 default → DELTA_BINARY_PACKED
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };
        var values = Enumerable.Range(0, 200).ToArray();
        var batch = MakeBatch(
            new Field("id", Int32Type.Default, nullable: false),
            new Int32Array.Builder().AppendRange(values).Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (Int32Array)readBatch.Column(0);
            for (int i = 0; i < 200; i++)
                Assert.Equal(i, col.GetValue(i));
        }, options);

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        Assert.Contains(Encoding.DeltaBinaryPacked, metadata.RowGroups[0].Columns[0].MetaData!.Encodings);
    }

    [Fact]
    public async Task V2Encoding_Float_UsesByteStreamSplit()
    {
        string path = TempPath("v2_bss_float.parquet");
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };
        var builder = new FloatArray.Builder();
        for (int i = 0; i < 200; i++) builder.Append(i * 1.1f);

        var batch = MakeBatch(
            new Field("val", FloatType.Default, nullable: false),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (FloatArray)readBatch.Column(0);
            for (int i = 0; i < 200; i++)
                Assert.Equal(i * 1.1f, col.GetValue(i));
        }, options);

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        Assert.Contains(Encoding.ByteStreamSplit, metadata.RowGroups[0].Columns[0].MetaData!.Encodings);
    }

    [Fact]
    public async Task V2Encoding_Double_UsesByteStreamSplit()
    {
        string path = TempPath("v2_bss_double.parquet");
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };
        var builder = new DoubleArray.Builder();
        for (int i = 0; i < 200; i++) builder.Append(i * 2.718);

        var batch = MakeBatch(
            new Field("val", DoubleType.Default, nullable: false),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (DoubleArray)readBatch.Column(0);
            for (int i = 0; i < 200; i++)
                Assert.Equal(i * 2.718, col.GetValue(i));
        }, options);

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        Assert.Contains(Encoding.ByteStreamSplit, metadata.RowGroups[0].Columns[0].MetaData!.Encodings);
    }

    [Fact]
    public async Task V2Encoding_String_UsesDeltaLengthByteArray()
    {
        string path = TempPath("v2_dlba_string.parquet");
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };
        var builder = new StringArray.Builder();
        for (int i = 0; i < 200; i++) builder.Append($"value_{i:D5}");

        var batch = MakeBatch(
            new Field("text", StringType.Default, nullable: false),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (StringArray)readBatch.Column(0);
            for (int i = 0; i < 200; i++)
                Assert.Equal($"value_{i:D5}", col.GetString(i));
        }, options);

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        Assert.Contains(Encoding.DeltaLengthByteArray, metadata.RowGroups[0].Columns[0].MetaData!.Encodings);
    }

    [Fact]
    public async Task V2Encoding_Boolean_UsesRle()
    {
        string path = TempPath("v2_rle_bool.parquet");
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };
        var builder = new BooleanArray.Builder();
        for (int i = 0; i < 100; i++) builder.Append(i % 3 == 0);

        var batch = MakeBatch(
            new Field("flag", BooleanType.Default, nullable: false),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (BooleanArray)readBatch.Column(0);
            for (int i = 0; i < 100; i++)
                Assert.Equal(i % 3 == 0, col.GetValue(i));
        }, options);

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        Assert.Contains(Encoding.Rle, metadata.RowGroups[0].Columns[0].MetaData!.Encodings);
    }

    [Fact]
    public async Task V2Encoding_NullableInt32_DeltaBinaryPacked()
    {
        string path = TempPath("v2_delta_nullable.parquet");
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };
        var builder = new Int32Array.Builder();
        for (int i = 0; i < 200; i++)
        {
            if (i % 7 == 0) builder.AppendNull();
            else builder.Append(i * 10);
        }

        var batch = MakeBatch(
            new Field("val", Int32Type.Default, nullable: true),
            builder.Build());

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var col = (Int32Array)readBatch.Column(0);
            Assert.Equal(200, col.Length);
            for (int i = 0; i < 200; i++)
            {
                if (i % 7 == 0) Assert.True(col.IsNull(i));
                else Assert.Equal(i * 10, col.GetValue(i));
            }
        }, options);
    }

    [Fact]
    public async Task V2Encoding_WithCompression_Snappy()
    {
        string path = TempPath("v2_advanced_snappy.parquet");
        var options = new ParquetWriteOptions { Compression = CompressionCodec.Snappy };

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("value", DoubleType.Default, false))
            .Field(new Field("name", StringType.Default, false))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var valBuilder = new DoubleArray.Builder();
        var nameBuilder = new StringArray.Builder();
        for (int i = 0; i < 200; i++)
        {
            idBuilder.Append(i);
            valBuilder.Append(i * 3.14);
            nameBuilder.Append($"item_{i}");
        }

        var batch = new RecordBatch(schema,
            [idBuilder.Build(), valBuilder.Build(), nameBuilder.Build()], 200);

        await WriteAndVerify(path, batch, (readBatch) =>
        {
            var ids = (Int32Array)readBatch.Column(0);
            var vals = (DoubleArray)readBatch.Column(1);
            var names = (StringArray)readBatch.Column(2);
            for (int i = 0; i < 200; i++)
            {
                Assert.Equal(i, ids.GetValue(i));
                Assert.Equal(i * 3.14, vals.GetValue(i));
                Assert.Equal($"item_{i}", names.GetString(i));
            }
        }, options);
    }

    // --- ParquetSharp Cross-Validation ---

    [Fact]
    public async Task CrossValidation_ParquetSharp_CanReadOurFile()
    {
        string path = TempPath("cross_ps.parquet");
        var options = new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
        };

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("value", DoubleType.Default, true))
            .Field(new Field("name", StringType.Default, true))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var valBuilder = new DoubleArray.Builder();
        var nameBuilder = new StringArray.Builder();

        for (int i = 0; i < 50; i++)
        {
            idBuilder.Append(i);
            if (i % 3 == 0) valBuilder.AppendNull(); else valBuilder.Append(i * 2.5);
            if (i % 4 == 0) nameBuilder.AppendNull(); else nameBuilder.Append($"item_{i}");
        }

        var batch = new RecordBatch(schema,
            [idBuilder.Build(), valBuilder.Build(), nameBuilder.Build()], 50);

        // Write with EngineeredWood
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        // Read with ParquetSharp
        using var psReader = new ParquetSharp.ParquetFileReader(path);
        using var rowGroup = psReader.RowGroup(0);
        Assert.Equal(50, rowGroup.MetaData.NumRows);

        // Verify int column
        using var idCol = rowGroup.Column(0).LogicalReader<int>();
        var idValues = new int[50];
        idCol.ReadBatch(idValues);
        for (int i = 0; i < 50; i++)
            Assert.Equal(i, idValues[i]);

        // Verify nullable double column
        using var valCol = rowGroup.Column(1).LogicalReader<double?>();
        var valValues = new double?[50];
        valCol.ReadBatch(valValues);
        for (int i = 0; i < 50; i++)
        {
            if (i % 3 == 0) Assert.Null(valValues[i]);
            else Assert.Equal(i * 2.5, valValues[i]);
        }

        // Verify nullable string column
        using var nameCol = rowGroup.Column(2).LogicalReader<string>();
        var nameValues = new string[50];
        nameCol.ReadBatch(nameValues);
        for (int i = 0; i < 50; i++)
        {
            if (i % 4 == 0)
                Assert.Null(nameValues[i]);
            else
                Assert.Equal($"item_{i}", nameValues[i]);
        }
    }

    // --- Statistics Tests ---

    [Fact]
    public async Task Statistics_Int32_Required_MinMaxNullCount()
    {
        string path = TempPath("stats_int32_required.parquet");
        var values = new int[] { 5, 1, 9, -3, 7 };
        var batch = MakeBatch(
            new Field("x", Int32Type.Default, nullable: false),
            new Int32Array.Builder().AppendRange(values).Build());

        await WriteAndVerifyStats(path, batch, stats =>
        {
            Assert.NotNull(stats);
            Assert.Equal(0L, stats.NullCount);
            Assert.Equal(BitConverter.GetBytes(-3), stats.MinValue);
            Assert.Equal(BitConverter.GetBytes(9), stats.MaxValue);
            Assert.True(stats.IsMinValueExact);
            Assert.True(stats.IsMaxValueExact);
        });
    }

    [Fact]
    public async Task Statistics_Int32_Nullable_MinMaxNullCount()
    {
        string path = TempPath("stats_int32_nullable.parquet");
        var builder = new Int32Array.Builder();
        builder.Append(10);
        builder.AppendNull();
        builder.Append(3);
        builder.AppendNull();
        builder.Append(7);

        var batch = MakeBatch(
            new Field("x", Int32Type.Default, nullable: true),
            builder.Build());

        await WriteAndVerifyStats(path, batch, stats =>
        {
            Assert.NotNull(stats);
            Assert.Equal(2L, stats.NullCount);
            Assert.Equal(BitConverter.GetBytes(3), stats.MinValue);
            Assert.Equal(BitConverter.GetBytes(10), stats.MaxValue);
        });
    }

    [Fact]
    public async Task Statistics_Int64_MinMax()
    {
        string path = TempPath("stats_int64.parquet");
        var values = new long[] { 100L, -500L, 200L, 0L };
        var batch = MakeBatch(
            new Field("x", Int64Type.Default, nullable: false),
            new Int64Array.Builder().AppendRange(values).Build());

        await WriteAndVerifyStats(path, batch, stats =>
        {
            Assert.NotNull(stats);
            Assert.Equal(0L, stats.NullCount);
            Assert.Equal(BitConverter.GetBytes(-500L), stats.MinValue);
            Assert.Equal(BitConverter.GetBytes(200L), stats.MaxValue);
        });
    }

    [Fact]
    public async Task Statistics_Float_SkipsNaN_HandlesNegativeZero()
    {
        string path = TempPath("stats_float.parquet");
        var builder = new FloatArray.Builder();
        builder.Append(float.NaN);
        builder.Append(-0.0f);
        builder.Append(0.0f);
        builder.Append(3.14f);
        builder.Append(float.NaN);

        var batch = MakeBatch(
            new Field("x", FloatType.Default, nullable: false),
            builder.Build());

        await WriteAndVerifyStats(path, batch, stats =>
        {
            Assert.NotNull(stats);
            Assert.Equal(0L, stats.NullCount);
            Assert.Equal(2L, stats.NanCount); // two NaN values, excluded from min/max
            // Min should be -0.0, max should be 3.14
            float min = BitConverter.ToSingle(stats.MinValue!, 0);
            float max = BitConverter.ToSingle(stats.MaxValue!, 0);
#if NET8_0_OR_GREATER
            Assert.True(float.IsNegative(min) && min == 0f, "Min should be -0.0");
#else
            Assert.True(min == 0f && float.NegativeInfinity.Equals(1.0f / min), "Min should be -0.0");
#endif
            Assert.Equal(3.14f, max);
        });
    }

    [Fact]
    public async Task Statistics_Double_MinMax()
    {
        string path = TempPath("stats_double.parquet");
        var values = new double[] { 1.0, -2.5, 100.0, 0.0 };
        var batch = MakeBatch(
            new Field("x", DoubleType.Default, nullable: false),
            new DoubleArray.Builder().AppendRange(values).Build());

        await WriteAndVerifyStats(path, batch, stats =>
        {
            Assert.NotNull(stats);
            Assert.Equal(BitConverter.GetBytes(-2.5), stats.MinValue);
            Assert.Equal(BitConverter.GetBytes(100.0), stats.MaxValue);
            Assert.Equal(0L, stats.NanCount); // mandatory for FP columns even when zero
        });
    }

    [Fact]
    public async Task Statistics_Float_AllNaN_TypeOrder_OmitsMinMaxButRecordsNanCount()
    {
        string path = TempPath("stats_all_nan.parquet");
        var builder = new FloatArray.Builder();
        builder.Append(float.NaN);
        builder.Append(float.NaN);
        builder.Append(float.NaN);

        var batch = MakeBatch(
            new Field("x", FloatType.Default, nullable: false),
            builder.Build());

        await WriteAndVerifyStats(path, batch, stats =>
        {
            Assert.NotNull(stats);
            // TYPE_ORDER: all values NaN ⇒ min/max omitted, but nan_count recorded.
            Assert.Null(stats.MinValue);
            Assert.Null(stats.MaxValue);
            Assert.Equal(3L, stats.NanCount);
        });
    }

    [Fact]
    public async Task Statistics_Float_DictionaryEncoded_CountsAllNaNOccurrences()
    {
        // A low-cardinality FP column would dictionary-encode; nan_count must still
        // reflect every occurrence, not the distinct dictionary entries.
        string path = TempPath("stats_dict_nan.parquet");
        var builder = new FloatArray.Builder();
        for (int i = 0; i < 100; i++)
        {
            builder.Append(i % 3 == 0 ? float.NaN : 1.0f);
        }

        var batch = MakeBatch(
            new Field("x", FloatType.Default, nullable: false),
            builder.Build());

        await WriteAndVerifyStats(path, batch, stats =>
        {
            Assert.NotNull(stats);
            Assert.Equal(34L, stats.NanCount); // indices 0,3,...,99 ⇒ 34 values
            Assert.Equal(1.0f, BitConverter.ToSingle(stats.MaxValue!, 0));
        });
    }

    [Fact]
    public async Task ColumnOrders_DefaultsToTypeDefinedForEveryColumn()
    {
        string path = TempPath("column_orders_default.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("i", Int32Type.Default, nullable: false))
            .Field(new Field("f", FloatType.Default, nullable: false))
            .Field(new Field("s", StringType.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
        [
            new Int32Array.Builder().AppendRange([1, 2, 3]).Build(),
            new FloatArray.Builder().AppendRange([1f, 2f, 3f]).Build(),
            new StringArray.Builder().Append("a").Append("b").Append("c").Build(),
        ], 3);

        var orders = await WriteAndReadColumnOrders(path, batch);
        Assert.Equal(
            [ColumnOrder.TypeDefined, ColumnOrder.TypeDefined, ColumnOrder.TypeDefined],
            orders);
    }

    [Fact]
    public async Task ColumnOrders_Ieee754TotalOrder_AppliesToFloatingPointColumnsOnly()
    {
        string path = TempPath("column_orders_total.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("i", Int32Type.Default, nullable: false))
            .Field(new Field("f", FloatType.Default, nullable: false))
            .Field(new Field("d", DoubleType.Default, nullable: false))
            .Field(new Field("s", StringType.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
        [
            new Int32Array.Builder().AppendRange([1, 2, 3]).Build(),
            new FloatArray.Builder().AppendRange([1f, 2f, 3f]).Build(),
            new DoubleArray.Builder().AppendRange([1.0, 2.0, 3.0]).Build(),
            new StringArray.Builder().Append("a").Append("b").Append("c").Build(),
        ], 3);

        var options = new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            FloatingPointOrder = FloatingPointColumnOrder.Ieee754TotalOrder,
        };

        var orders = await WriteAndReadColumnOrders(path, batch, options);
        Assert.Equal(
        [
            ColumnOrder.TypeDefined,        // i
            ColumnOrder.Ieee754TotalOrder,  // f
            ColumnOrder.Ieee754TotalOrder,  // d
            ColumnOrder.TypeDefined,        // s
        ], orders);
    }

    private async Task<IReadOnlyList<ColumnOrder>> WriteAndReadColumnOrders(
        string path, RecordBatch batch, ParquetWriteOptions? options = null)
    {
        options ??= new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        Assert.NotNull(metadata.ColumnOrders);
        return metadata.ColumnOrders!;
    }

    [Fact]
    public async Task Statistics_Float_Ieee754TotalOrder_AllNaN_WritesNanBounds()
    {
        string path = TempPath("stats_total_order.parquet");
        var builder = new FloatArray.Builder();
        builder.Append(float.NaN);
        builder.Append(float.NaN);

        var batch = MakeBatch(
            new Field("x", FloatType.Default, nullable: false),
            builder.Build());

        var options = new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            FloatingPointOrder = FloatingPointColumnOrder.Ieee754TotalOrder,
        };

        await WriteAndVerifyStats(path, batch, stats =>
        {
            Assert.NotNull(stats);
            Assert.Equal(2L, stats.NanCount);
            // Total order: an all-NaN chunk records NaN as both bounds.
            Assert.NotNull(stats.MinValue);
            Assert.True(float.IsNaN(BitConverter.ToSingle(stats.MinValue!, 0)));
            Assert.True(float.IsNaN(BitConverter.ToSingle(stats.MaxValue!, 0)));
        }, options);
    }

    [Fact]
    public async Task Statistics_String_LexicographicMinMax()
    {
        string path = TempPath("stats_string.parquet");
        var builder = new StringArray.Builder();
        builder.Append("banana");
        builder.Append("apple");
        builder.Append("cherry");

        var batch = MakeBatch(
            new Field("x", StringType.Default, nullable: false),
            builder.Build());

        await WriteAndVerifyStats(path, batch, stats =>
        {
            Assert.NotNull(stats);
            Assert.Equal("apple", System.Text.Encoding.UTF8.GetString(stats.MinValue!));
            Assert.Equal("cherry", System.Text.Encoding.UTF8.GetString(stats.MaxValue!));
        });
    }

    [Fact]
    public async Task Statistics_Boolean_MinMax()
    {
        string path = TempPath("stats_bool.parquet");
        var builder = new BooleanArray.Builder();
        builder.Append(true);
        builder.Append(false);
        builder.Append(true);

        var batch = MakeBatch(
            new Field("x", BooleanType.Default, nullable: false),
            builder.Build());

        await WriteAndVerifyStats(path, batch, stats =>
        {
            Assert.NotNull(stats);
            Assert.Equal(new byte[] { 0 }, stats.MinValue);
            Assert.Equal(new byte[] { 1 }, stats.MaxValue);
        });
    }

    [Fact]
    public async Task Statistics_AllNull_NoMinMax()
    {
        string path = TempPath("stats_all_null.parquet");
        var builder = new Int32Array.Builder();
        builder.AppendNull();
        builder.AppendNull();
        builder.AppendNull();

        var batch = MakeBatch(
            new Field("x", Int32Type.Default, nullable: true),
            builder.Build());

        await WriteAndVerifyStats(path, batch, stats =>
        {
            Assert.NotNull(stats);
            Assert.Equal(3L, stats.NullCount);
            Assert.Null(stats.MinValue);
            Assert.Null(stats.MaxValue);
        });
    }

    [Fact]
    public async Task Statistics_LegacyMinMax_SameAsMinValueMaxValue()
    {
        string path = TempPath("stats_legacy.parquet");
        var values = new int[] { 10, 20, 30 };
        var batch = MakeBatch(
            new Field("x", Int32Type.Default, nullable: false),
            new Int32Array.Builder().AppendRange(values).Build());

        await WriteAndVerifyStats(path, batch, stats =>
        {
            Assert.NotNull(stats);
            // Legacy Min/Max should equal MinValue/MaxValue
            Assert.Equal(stats.MinValue, stats.Min);
            Assert.Equal(stats.MaxValue, stats.Max);
        });
    }

    [Fact]
    public async Task Statistics_DictionaryEncoded_StillPresent()
    {
        string path = TempPath("stats_dict.parquet");
        // Low cardinality → dictionary encoding
        var builder = new StringArray.Builder();
        for (int i = 0; i < 100; i++)
            builder.Append(i % 3 == 0 ? "aaa" : i % 3 == 1 ? "bbb" : "ccc");

        var batch = MakeBatch(
            new Field("x", StringType.Default, nullable: false),
            builder.Build());

        await WriteAndVerifyStats(path, batch, stats =>
        {
            Assert.NotNull(stats);
            Assert.Equal(0L, stats.NullCount);
            Assert.Equal("aaa", System.Text.Encoding.UTF8.GetString(stats.MinValue!));
            Assert.Equal("ccc", System.Text.Encoding.UTF8.GetString(stats.MaxValue!));
        });
    }

    private async Task WriteAndVerifyStats(
        string path, RecordBatch batch, Action<Statistics?> verifyStats,
        ParquetWriteOptions? options = null)
    {
        options ??= new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);

        var metadata = await reader.ReadMetadataAsync();
        var colStats = metadata.RowGroups[0].Columns[0].MetaData!.Statistics;
        verifyStats(colStats);
    }

    // --- Nested Type Tests ---

    [Fact]
    public async Task RoundTrip_StructColumn_Required()
    {
        string path = TempPath("struct_required.parquet");
        var childId = new Int32Array.Builder().AppendRange([1, 2, 3]).Build();
        var childName = new StringArray.Builder().Append("a").Append("b").Append("c").Build();

        var structType = new StructType(new[]
        {
            new Field("id", Int32Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: false),
        });
        var structArray = new StructArray(structType, 3, [childId, childName], ArrowBuffer.Empty, nullCount: 0);

        var field = new Field("s", structType, nullable: false);
        var batch = MakeBatch(field, structArray);

        await WriteAndVerify(path, batch, readBatch =>
        {
            var s = (StructArray)readBatch.Column(0);
            Assert.Equal(3, s.Length);
            var ids = (Int32Array)s.Fields[0];
            var names = (StringArray)s.Fields[1];
            Assert.Equal(1, ids.GetValue(0));
            Assert.Equal(2, ids.GetValue(1));
            Assert.Equal(3, ids.GetValue(2));
            Assert.Equal("a", names.GetString(0));
            Assert.Equal("b", names.GetString(1));
            Assert.Equal("c", names.GetString(2));
        });
    }

    [Fact]
    public async Task RoundTrip_StructColumn_Nullable()
    {
        string path = TempPath("struct_nullable.parquet");
        var childId = new Int32Array.Builder().Append(1).Append(0).Append(3).Build();
        var childName = new StringArray.Builder().Append("a").Append("").Append("c").Build();

        var structType = new StructType(new[]
        {
            new Field("id", Int32Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: false),
        });

        // Row 1: null struct
        var bitmap = new byte[] { 0b101 }; // rows 0 and 2 present, row 1 null
        var structArray = new StructArray(structType, 3, [childId, childName], new ArrowBuffer(bitmap), nullCount: 1);

        var field = new Field("s", structType, nullable: true);
        var batch = MakeBatch(field, structArray);

        await WriteAndVerify(path, batch, readBatch =>
        {
            var s = (StructArray)readBatch.Column(0);
            Assert.Equal(3, s.Length);
            Assert.False(s.IsNull(0));
            Assert.True(s.IsNull(1));
            Assert.False(s.IsNull(2));
            var ids = (Int32Array)s.Fields[0];
            Assert.Equal(1, ids.GetValue(0));
            Assert.Equal(3, ids.GetValue(2));
        });
    }

    [Fact]
    public async Task RoundTrip_ListColumn_Int32()
    {
        string path = TempPath("list_int32.parquet");
        // Row 0: [1, 2, 3], Row 1: [4], Row 2: [5, 6]
        var valueBuilder = new Int32Array.Builder();
        valueBuilder.AppendRange([1, 2, 3, 4, 5, 6]);
        var values = valueBuilder.Build();

        var offsets = new int[] { 0, 3, 4, 6 };
        var offsetBytes = new byte[offsets.Length * 4];
        System.Runtime.InteropServices.MemoryMarshal.AsBytes(offsets.AsSpan()).CopyTo(offsetBytes);

        var elementField = new Field("element", Int32Type.Default, nullable: false);
        var listType = new ListType(elementField);
        var listArray = new ListArray(listType, 3, new ArrowBuffer(offsetBytes), values, ArrowBuffer.Empty, nullCount: 0);

        var field = new Field("numbers", listType, nullable: false);
        var batch = MakeBatch(field, listArray);

        await WriteAndVerify(path, batch, readBatch =>
        {
            var list = (ListArray)readBatch.Column(0);
            Assert.Equal(3, list.Length);

            // Row 0: [1, 2, 3]
            var slice0 = (Int32Array)list.GetSlicedValues(0);
            Assert.Equal(3, slice0.Length);
            Assert.Equal(1, slice0.GetValue(0));
            Assert.Equal(2, slice0.GetValue(1));
            Assert.Equal(3, slice0.GetValue(2));

            // Row 1: [4]
            var slice1 = (Int32Array)list.GetSlicedValues(1);
            Assert.Equal(1, slice1.Length);
            Assert.Equal(4, slice1.GetValue(0));

            // Row 2: [5, 6]
            var slice2 = (Int32Array)list.GetSlicedValues(2);
            Assert.Equal(2, slice2.Length);
            Assert.Equal(5, slice2.GetValue(0));
            Assert.Equal(6, slice2.GetValue(1));
        });
    }

    [Fact]
    public async Task RoundTrip_ListColumn_Nullable_WithEmptyAndNull()
    {
        string path = TempPath("list_nullable.parquet");
        // Row 0: [1, 2], Row 1: null, Row 2: [], Row 3: [3]
        var values = new Int32Array.Builder().AppendRange([1, 2, 3]).Build();

        var offsets = new int[] { 0, 2, 2, 2, 3 };
        var offsetBytes = new byte[offsets.Length * 4];
        System.Runtime.InteropServices.MemoryMarshal.AsBytes(offsets.AsSpan()).CopyTo(offsetBytes);

        var elementField = new Field("element", Int32Type.Default, nullable: false);
        var listType = new ListType(elementField);
        var bitmap = new byte[] { 0b1101 }; // row 1 is null
        var listArray = new ListArray(listType, 4, new ArrowBuffer(offsetBytes), values, new ArrowBuffer(bitmap), nullCount: 1);

        var field = new Field("numbers", listType, nullable: true);
        var batch = MakeBatch(field, listArray);

        await WriteAndVerify(path, batch, readBatch =>
        {
            var list = (ListArray)readBatch.Column(0);
            Assert.Equal(4, list.Length);

            // Row 0: [1, 2]
            Assert.False(list.IsNull(0));
            var s0 = (Int32Array)list.GetSlicedValues(0);
            Assert.Equal(2, s0.Length);

            // Row 1: null
            Assert.True(list.IsNull(1));

            // Row 2: []
            Assert.False(list.IsNull(2));
            var s2 = (Int32Array)list.GetSlicedValues(2);
            Assert.Equal(0, s2.Length);

            // Row 3: [3]
            Assert.False(list.IsNull(3));
            var s3 = (Int32Array)list.GetSlicedValues(3);
            Assert.Equal(1, s3.Length);
            Assert.Equal(3, s3.GetValue(0));
        });
    }

    [Fact]
    public async Task RoundTrip_MapColumn()
    {
        string path = TempPath("map_basic.parquet");
        // Row 0: {"a": 1, "b": 2}, Row 1: {"c": 3}
        var keys = new StringArray.Builder().Append("a").Append("b").Append("c").Build();
        var vals = new Int32Array.Builder().AppendRange([1, 2, 3]).Build();

        var offsets = new int[] { 0, 2, 3 };
        var offsetBytes = new byte[offsets.Length * 4];
        System.Runtime.InteropServices.MemoryMarshal.AsBytes(offsets.AsSpan()).CopyTo(offsetBytes);

        var keyField = new Field("key", StringType.Default, nullable: false);
        var valueField = new Field("value", Int32Type.Default, nullable: true);
        var mapType = new MapType(keyField, valueField);

        var kvStructType = new StructType(new[] { keyField, valueField });
        var kvStruct = new StructArray(kvStructType, 3, [keys, vals], ArrowBuffer.Empty, nullCount: 0);

        var mapArray = new MapArray(mapType, 2, new ArrowBuffer(offsetBytes), kvStruct, ArrowBuffer.Empty, nullCount: 0);

        var field = new Field("m", mapType, nullable: false);
        var batch = MakeBatch(field, mapArray);

        await WriteAndVerify(path, batch, readBatch =>
        {
            var map = (MapArray)readBatch.Column(0);
            Assert.Equal(2, map.Length);

            // Row 0: 2 entries
            Assert.Equal(2, map.GetValueLength(0));
            // Row 1: 1 entry
            Assert.Equal(1, map.GetValueLength(1));

            var readKeys = (StringArray)map.Keys;
            Assert.Equal("a", readKeys.GetString(0));
            Assert.Equal("b", readKeys.GetString(1));
            Assert.Equal("c", readKeys.GetString(2));

            var readVals = (Int32Array)map.Values;
            Assert.Equal(1, readVals.GetValue(0));
            Assert.Equal(2, readVals.GetValue(1));
            Assert.Equal(3, readVals.GetValue(2));
        });
    }

    [Fact]
    public async Task RoundTrip_StructWithNullableChildren()
    {
        string path = TempPath("struct_nullable_children.parquet");
        // Struct with one nullable child
        var childId = new Int32Array.Builder().Append(1).AppendNull().Append(3).Build();

        var structType = new StructType(new[]
        {
            new Field("id", Int32Type.Default, nullable: true),
        });
        var structArray = new StructArray(structType, 3, [childId], ArrowBuffer.Empty, nullCount: 0);

        var field = new Field("s", structType, nullable: false);
        var batch = MakeBatch(field, structArray);

        await WriteAndVerify(path, batch, readBatch =>
        {
            var s = (StructArray)readBatch.Column(0);
            Assert.Equal(3, s.Length);
            var ids = (Int32Array)s.Fields[0];
            Assert.Equal(1, ids.GetValue(0));
            Assert.True(ids.IsNull(1));
            Assert.Equal(3, ids.GetValue(2));
        });
    }

    [Fact]
    public async Task RoundTrip_NestedStructAndList_OmitPathInSchema()
    {
        // Verifies that we can write a nested schema with path_in_schema omitted
        // (opt-in only — see EWPARQUET0002) and still round-trip the data correctly. The reader
        // matches columns to the schema by ordinal position, so the missing field
        // should not affect any value, definition level, or repetition level.
        string path = TempPath("nested_no_path_in_schema.parquet");

        // Build a schema with two leaf paths inside a struct, plus a list<int>:
        //   s: struct<id: int32, name: string>
        //   tags: list<int32>
        var childId = new Int32Array.Builder().AppendRange([10, 20, 30]).Build();
        var childName = new StringArray.Builder().Append("a").Append("b").Append("c").Build();
        var structType = new StructType(new[]
        {
            new Field("id", Int32Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: false),
        });
        var structArray = new StructArray(structType, 3,
            [childId, childName], ArrowBuffer.Empty, nullCount: 0);

        // tags = [[1,2], [], [3,4,5]]
        var listValues = new Int32Array.Builder().AppendRange([1, 2, 3, 4, 5]).Build();
        var offsetsBuf = new ArrowBuffer.Builder<int>().AppendRange([0, 2, 2, 5]).Build();
        var listType = new Apache.Arrow.Types.ListType(
            new Field("item", Int32Type.Default, nullable: false));
        var listArray = new ListArray(listType, 3, offsetsBuf, listValues,
            ArrowBuffer.Empty, nullCount: 0);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", structType, nullable: false))
            .Field(new Field("tags", listType, nullable: false))
            .Build();
        var batch = new RecordBatch(schema, [structArray, listArray], 3);

        // Default options omit path_in_schema.
#pragma warning disable EWPARQUET0002 // This test exists specifically to cover the opt-in omission.
        var omitOptions = new ParquetWriteOptions { OmitPathInSchema = true };
#pragma warning restore EWPARQUET0002

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, omitOptions))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        // Verify the field is genuinely absent in every column chunk metadata.
        await using (var readFile = new LocalRandomAccessFile(path))
        await using (var reader = new ParquetFileReader(readFile, ownsFile: false))
        {
            var meta = await reader.ReadMetadataAsync();
            Assert.Single(meta.RowGroups);
            foreach (var chunk in meta.RowGroups[0].Columns)
                Assert.Null(chunk.MetaData!.PathInSchema);
        }

        // Read back and verify all values, including struct fields and list shape.
        await using (var readFile = new LocalRandomAccessFile(path))
        await using (var reader = new ParquetFileReader(readFile, ownsFile: false))
        {
            var readBatch = await reader.ReadRowGroupAsync(0);
            Assert.Equal(3, readBatch.Length);
            Assert.Equal(2, readBatch.ColumnCount);

            var s = (StructArray)readBatch.Column(0);
            var ids = (Int32Array)s.Fields[0];
            var names = (StringArray)s.Fields[1];
            Assert.Equal(10, ids.GetValue(0));
            Assert.Equal(20, ids.GetValue(1));
            Assert.Equal(30, ids.GetValue(2));
            Assert.Equal("a", names.GetString(0));
            Assert.Equal("b", names.GetString(1));
            Assert.Equal("c", names.GetString(2));

            var list = (ListArray)readBatch.Column(1);
            var listInts = (Int32Array)list.Values;
            // [[1,2], [], [3,4,5]]
            Assert.Equal(0, list.ValueOffsets[0]);
            Assert.Equal(2, list.GetValueLength(0));
            Assert.Equal(0, list.GetValueLength(1));
            Assert.Equal(3, list.GetValueLength(2));
            Assert.Equal(1, listInts.GetValue(0));
            Assert.Equal(2, listInts.GetValue(1));
            Assert.Equal(3, listInts.GetValue(2));
            Assert.Equal(4, listInts.GetValue(3));
            Assert.Equal(5, listInts.GetValue(4));
        }
    }

    // --- Helpers ---

    private static RecordBatch MakeBatch(Field field, IArrowArray array)
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(field).Build();
        return new RecordBatch(schema, [array], array.Length);
    }

    // --- Narrow integer columns (1-/2-byte Arrow buffers over the 4-byte Int32 physical type) ---
    // The value extraction used to reinterpret the raw Arrow buffer AT THE PHYSICAL WIDTH, packing four
    // int8 values into each int read — silent corruption whenever the buffer's 64-byte padding hid the
    // overrun, an out-of-range exception when it did not. WriteColumnCore now widens narrow arrays once.

    [Theory]
    [InlineData(true)]   // dictionary path (DictionaryEncoder.TryEncodeFixed)
    [InlineData(false)]  // plain/V2 path (EncodeFixedValues)
    public async Task NarrowIntColumns_RoundTrip(bool dictionaryEnabled)
    {
        var path = TempPath($"narrow_ints_dict_{dictionaryEnabled}.parquet");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("i8", Int8Type.Default, nullable: true))
            .Field(new Field("u8", UInt8Type.Default, nullable: true))
            .Field(new Field("i16", Int16Type.Default, nullable: true))
            .Field(new Field("u16", UInt16Type.Default, nullable: true))
            .Build();

        var i8 = new Int8Array.Builder();
        i8.Append(-1); i8.Append(2); i8.AppendNull(); i8.Append(sbyte.MinValue); i8.Append(sbyte.MaxValue);
        var u8 = new UInt8Array.Builder();
        u8.Append(1); u8.Append(2); u8.AppendNull(); u8.Append(byte.MinValue); u8.Append(byte.MaxValue);
        var i16 = new Int16Array.Builder();
        i16.Append(-300); i16.Append(300); i16.AppendNull(); i16.Append(short.MinValue); i16.Append(short.MaxValue);
        var u16 = new UInt16Array.Builder();
        u16.Append(1); u16.Append(300); u16.AppendNull(); u16.Append(ushort.MinValue); u16.Append(ushort.MaxValue);

        var batch = new RecordBatch(schema, [i8.Build(), u8.Build(), i16.Build(), u16.Build()], 5);

        await WriteAndVerify(path, batch, readBatch =>
        {
            var r8 = Assert.IsType<Int8Array>(readBatch.Column(0));
            Assert.Equal((sbyte)-1, r8.GetValue(0));
            Assert.Equal((sbyte)2, r8.GetValue(1));
            Assert.True(r8.IsNull(2));
            Assert.Equal(sbyte.MinValue, r8.GetValue(3));
            Assert.Equal(sbyte.MaxValue, r8.GetValue(4));

            var ru8 = Assert.IsType<UInt8Array>(readBatch.Column(1));
            Assert.Equal((byte)1, ru8.GetValue(0));
            Assert.True(ru8.IsNull(2));
            Assert.Equal(byte.MaxValue, ru8.GetValue(4));

            var r16 = Assert.IsType<Int16Array>(readBatch.Column(2));
            Assert.Equal((short)-300, r16.GetValue(0));
            Assert.Equal((short)300, r16.GetValue(1));
            Assert.True(r16.IsNull(2));
            Assert.Equal(short.MinValue, r16.GetValue(3));
            Assert.Equal(short.MaxValue, r16.GetValue(4));

            var ru16 = Assert.IsType<UInt16Array>(readBatch.Column(3));
            Assert.Equal((ushort)300, ru16.GetValue(1));
            Assert.True(ru16.IsNull(2));
            Assert.Equal(ushort.MaxValue, ru16.GetValue(4));
        }, new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            DictionaryEnabled = dictionaryEnabled,
        });
    }

    private async Task WriteAndVerify(
        string path, RecordBatch batch, Action<RecordBatch> verify,
        ParquetWriteOptions? options = null)
    {
        options ??= new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed };

        // Write
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        // Read back with our reader
        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);

        var metadata = await reader.ReadMetadataAsync();
        Assert.Equal(batch.Length, metadata.NumRows);
        Assert.Single(metadata.RowGroups);

        var readBatch = await reader.ReadRowGroupAsync(0);
        Assert.Equal(batch.Length, readBatch.Length);
        Assert.Equal(batch.ColumnCount, readBatch.ColumnCount);

        verify(readBatch);
    }

    // --- Binary Stat Truncation Tests ---

    [Fact]
    public async Task Stats_LongStringValues_Truncated()
    {
        var path = TempPath("truncated_stats.parquet");

        // Both min and max are > 64 bytes, so both should be truncated
        // Use lowercase to ensure lexicographic min/max work as expected
        string longMin = new string('a', 100);
        string longMax = new string('z', 100);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", StringType.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new StringArray.Builder().Append(longMin).Append(longMax).Build()], 2);

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false,
            new ParquetWriteOptions { DictionaryEnabled = false }))
        {
            await writer.WriteRowGroupAsync(batch);
        }

        // Read back and check stats
        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var colMeta = metadata.RowGroups[0].Columns![0].MetaData!;

        // Min should be truncated to 64-byte prefix (exact = false)
        Assert.NotNull(colMeta.Statistics?.MinValue);
        Assert.Equal(64, colMeta.Statistics!.MinValue!.Length);
        Assert.False(colMeta.Statistics.IsMinValueExact);

        // Max should be truncated and incremented
        Assert.NotNull(colMeta.Statistics.MaxValue);
        Assert.True(colMeta.Statistics.MaxValue!.Length <= 64);
        Assert.False(colMeta.Statistics.IsMaxValueExact);
    }

    [Fact]
    public async Task Stats_ShortStringValues_NotTruncated()
    {
        var path = TempPath("short_stats.parquet");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", StringType.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new StringArray.Builder().Append("alpha").Append("beta").Append("gamma").Build()], 3);

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false,
            new ParquetWriteOptions { DictionaryEnabled = false }))
        {
            await writer.WriteRowGroupAsync(batch);
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var stats = metadata.RowGroups[0].Columns![0].MetaData!.Statistics!;

        Assert.True(stats.IsMinValueExact);
        Assert.True(stats.IsMaxValueExact);
        Assert.Equal("alpha"u8.ToArray(), stats.MinValue);
        Assert.Equal("gamma"u8.ToArray(), stats.MaxValue);
    }

    // --- Dictionary-based Statistics Tests ---

    [Fact]
    public async Task Stats_DictEncoded_MatchesFullScan()
    {
        var path = TempPath("dict_stats.parquet");

        // Low-cardinality string column — will use dictionary encoding
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("category", StringType.Default, nullable: true))
            .Build();
        var builder = new StringArray.Builder();
        var values = new[] { "cherry", "apple", null, "banana", "apple", "cherry" };
        foreach (var v in values)
        {
            if (v == null) builder.AppendNull();
            else builder.Append(v);
        }
        var batch = new RecordBatch(schema, [builder.Build()], values.Length);

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var stats = metadata.RowGroups[0].Columns![0].MetaData!.Statistics!;

        Assert.Equal(1, stats.NullCount);
        Assert.Equal("apple"u8.ToArray(), stats.MinValue);
        Assert.Equal("cherry"u8.ToArray(), stats.MaxValue);
        Assert.True(stats.IsMinValueExact);
        Assert.True(stats.IsMaxValueExact);
    }

    [Fact]
    public async Task Stats_DictEncodedInt32_CorrectMinMax()
    {
        var path = TempPath("dict_int_stats.parquet");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("val", Int32Type.Default, nullable: false))
            .Build();
        // Low cardinality: 3 unique values repeated
        var arr = new Int32Array.Builder().Append(10).Append(5).Append(20).Append(5).Append(10).Build();
        var batch = new RecordBatch(schema, [arr], 5);

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var stats = metadata.RowGroups[0].Columns![0].MetaData!.Statistics!;

        var minVal = BitConverter.ToInt32(stats.MinValue!, 0);
        var maxVal = BitConverter.ToInt32(stats.MaxValue!, 0);
        Assert.Equal(5, minVal);
        Assert.Equal(20, maxVal);
    }

    // --- Per-Column Codec/Encoding Override Tests ---

    [Fact]
    public async Task PerColumnCodec_OverridesDefault()
    {
        var path = TempPath("per_col_codec.parquet");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", Int32Type.Default, nullable: false))
            .Field(new Field("b", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
        [
            new Int32Array.Builder().Append(1).Append(2).Append(3).Build(),
            new Int32Array.Builder().Append(4).Append(5).Append(6).Build(),
        ], 3);

        var options = new ParquetWriteOptions
        {
            Compression = CompressionCodec.Snappy,
            ColumnCodecs = new Dictionary<string, CompressionCodec>
            {
                ["b"] = CompressionCodec.Uncompressed,
            },
        };

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        var colA = metadata.RowGroups[0].Columns![0].MetaData!;
        var colB = metadata.RowGroups[0].Columns![1].MetaData!;

        Assert.Equal(CompressionCodec.Snappy, colA.Codec);
        Assert.Equal(CompressionCodec.Uncompressed, colB.Codec);

        // Verify data roundtrips
        var readBatch = await reader.ReadRowGroupAsync(0);
        Assert.Equal(3, readBatch.Length);
    }

    [Fact]
    public async Task PerColumnEncoding_DeltaByteArrayOverride()
    {
        var path = TempPath("per_col_enc.parquet");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("plain_col", StringType.Default, nullable: false))
            .Field(new Field("dba_col", StringType.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
        [
            new StringArray.Builder().Append("foo").Append("bar").Append("baz").Build(),
            new StringArray.Builder().Append("prefix_a").Append("prefix_b").Append("prefix_c").Build(),
        ], 3);

        var options = new ParquetWriteOptions
        {
            DictionaryEnabled = false,
            ByteArrayEncoding = ByteArrayEncoding.DeltaLengthByteArray,
            ColumnEncodings = new Dictionary<string, ByteArrayEncoding>
            {
                ["dba_col"] = ByteArrayEncoding.DeltaByteArray,
            },
        };

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        var colPlain = metadata.RowGroups[0].Columns![0].MetaData!;
        var colDba = metadata.RowGroups[0].Columns![1].MetaData!;

        Assert.Contains(Encoding.DeltaLengthByteArray, colPlain.Encodings!);
        Assert.Contains(Encoding.DeltaByteArray, colDba.Encodings!);

        // Verify data roundtrips
        var readBatch = await reader.ReadRowGroupAsync(0);
        var plain = (StringArray)readBatch.Column(0);
        var dba = (StringArray)readBatch.Column(1);
        Assert.Equal("foo", plain.GetString(0));
        Assert.Equal("prefix_a", dba.GetString(0));
    }

    // --- EncodingStrategyResolver Tests ---

    [Fact]
    public void EncodingStrategyResolver_V2_Int32_DeltaBinaryPacked()
    {
        var enc = EncodingStrategyResolver.GetV2Encoding(PhysicalType.Int32, ByteArrayEncoding.DeltaLengthByteArray, FloatingPointEncoding.ByteStreamSplit);
        Assert.Equal(Encoding.DeltaBinaryPacked, enc);
    }

    [Fact]
    public void EncodingStrategyResolver_V2_Float_ByteStreamSplit()
    {
        var enc = EncodingStrategyResolver.GetV2Encoding(PhysicalType.Float, ByteArrayEncoding.DeltaLengthByteArray, FloatingPointEncoding.ByteStreamSplit);
        Assert.Equal(Encoding.ByteStreamSplit, enc);
    }

    [Fact]
    public void EncodingStrategyResolver_V2_Float_Alp()
    {
#pragma warning disable EWPARQUET0001 // Test exercises the experimental enum value intentionally.
        var enc = EncodingStrategyResolver.GetV2Encoding(PhysicalType.Float, ByteArrayEncoding.DeltaLengthByteArray, FloatingPointEncoding.Alp);
        Assert.Equal(Encoding.Alp, enc);
#pragma warning restore EWPARQUET0001
    }

    [Fact]
    public void EncodingStrategyResolver_V2_Double_Plain()
    {
        var enc = EncodingStrategyResolver.GetV2Encoding(PhysicalType.Double, ByteArrayEncoding.DeltaLengthByteArray, FloatingPointEncoding.Plain);
        Assert.Equal(Encoding.Plain, enc);
    }

    [Fact]
    public void EncodingStrategyResolver_V2_ByteArray_Default_DLBA()
    {
        var enc = EncodingStrategyResolver.GetV2Encoding(PhysicalType.ByteArray, ByteArrayEncoding.DeltaLengthByteArray, FloatingPointEncoding.ByteStreamSplit);
        Assert.Equal(Encoding.DeltaLengthByteArray, enc);
    }

    [Fact]
    public void EncodingStrategyResolver_V2_ByteArray_DBA()
    {
        var enc = EncodingStrategyResolver.GetV2Encoding(PhysicalType.ByteArray, ByteArrayEncoding.DeltaByteArray, FloatingPointEncoding.ByteStreamSplit);
        Assert.Equal(Encoding.DeltaByteArray, enc);
    }

    [Fact]
    public void EncodingStrategyResolver_Fallback_V1_AlwaysPlain()
    {
        var options = new ParquetWriteOptions { DataPageVersion = DataPageVersion.V1 };
        var enc = EncodingStrategyResolver.GetFallbackEncoding(PhysicalType.Int32, options);
        Assert.Equal(Encoding.Plain, enc);
    }

    [Fact]
    public void EncodingStrategyResolver_ShouldAttemptDictionary_Boolean_False()
    {
        Assert.False(EncodingStrategyResolver.ShouldAttemptDictionary(
            PhysicalType.Boolean, ParquetWriteOptions.Default));
    }

    [Fact]
    public void EncodingStrategyResolver_ShouldAttemptDictionary_Int32_True()
    {
        Assert.True(EncodingStrategyResolver.ShouldAttemptDictionary(
            PhysicalType.Int32, ParquetWriteOptions.Default));
    }
}

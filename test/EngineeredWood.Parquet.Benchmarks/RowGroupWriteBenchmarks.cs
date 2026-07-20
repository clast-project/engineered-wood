// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.IO.Local;
using EngineeredWood.Compression;
using EngineeredWood.Parquet;
using Parquet;
using Parquet.Schema;
using Field = Apache.Arrow.Field;
using PqDataField = Parquet.Schema.DataField;

namespace EngineeredWood.Benchmarks;

[MemoryDiagnoser]
public class RowGroupWriteBenchmarks
{
    private string _dir = null!;
    private RecordBatch _intBatch = null!;
    private RecordBatch _stringBatch = null!;
    private RecordBatch _mixedBatch = null!;

    private const int RowCount = 100_000;

    [Params("int_cols", "string_cols", "mixed_cols")]
    public string Scenario { get; set; } = null!;

    private RecordBatch CurrentBatch => Scenario switch
    {
        "int_cols" => _intBatch,
        "string_cols" => _stringBatch,
        "mixed_cols" => _mixedBatch,
        _ => throw new InvalidOperationException(),
    };

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "ew-write-bench-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_dir);

        var random = new Random(42);

        // Int columns: 10 int32 columns (5 required, 5 nullable)
        {
            var fields = new List<Field>();
            var arrays = new List<IArrowArray>();
            for (int c = 0; c < 10; c++)
            {
                bool nullable = c >= 5;
                fields.Add(new Field($"c{c}", Int32Type.Default, nullable));
                var builder = new Int32Array.Builder();
                for (int r = 0; r < RowCount; r++)
                {
                    if (nullable && random.NextDouble() < 0.1)
                        builder.AppendNull();
                    else
                        builder.Append(random.Next());
                }
                arrays.Add(builder.Build());
            }
            var schema = new Apache.Arrow.Schema.Builder();
            foreach (var f in fields) schema.Field(f);
            _intBatch = new RecordBatch(schema.Build(), arrays, RowCount);
        }

        // String columns: 5 string columns
        {
            var fields = new List<Field>();
            var arrays = new List<IArrowArray>();
            for (int c = 0; c < 5; c++)
            {
                bool nullable = c >= 3;
                fields.Add(new Field($"s{c}", StringType.Default, nullable));
                var builder = new StringArray.Builder();
                for (int r = 0; r < RowCount; r++)
                {
                    if (nullable && random.NextDouble() < 0.1)
                        builder.AppendNull();
                    else
                        builder.Append($"value-{random.Next(10000)}-{r}");
                }
                arrays.Add(builder.Build());
            }
            var schema = new Apache.Arrow.Schema.Builder();
            foreach (var f in fields) schema.Field(f);
            _stringBatch = new RecordBatch(schema.Build(), arrays, RowCount);
        }

        // Mixed columns: int32, int64, double, string, bool
        {
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Field(new Field("timestamp", Int64Type.Default, nullable: false))
                .Field(new Field("value", DoubleType.Default, nullable: true))
                .Field(new Field("name", StringType.Default, nullable: true))
                .Field(new Field("flag", BooleanType.Default, nullable: true))
                .Build();

            var intBuilder = new Int32Array.Builder();
            var longBuilder = new Int64Array.Builder();
            var doubleBuilder = new DoubleArray.Builder();
            var stringBuilder = new StringArray.Builder();
            var boolBuilder = new BooleanArray.Builder();

            for (int r = 0; r < RowCount; r++)
            {
                intBuilder.Append(r);
                longBuilder.Append(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + r);
                if (random.NextDouble() < 0.1) doubleBuilder.AppendNull();
                else doubleBuilder.Append(random.NextDouble() * 1000);
                if (random.NextDouble() < 0.1) stringBuilder.AppendNull();
                else stringBuilder.Append($"item-{random.Next(5000)}");
                if (random.NextDouble() < 0.1) boolBuilder.AppendNull();
                else boolBuilder.Append(random.NextDouble() > 0.5);
            }

            _mixedBatch = new RecordBatch(schema,
                [intBuilder.Build(), longBuilder.Build(), doubleBuilder.Build(),
                 stringBuilder.Build(), boolBuilder.Build()], RowCount);
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        if (Directory.Exists(_dir))
            Directory.Delete(_dir, recursive: true);
    }

    [Benchmark(Baseline = true, Description = "EW_Snappy_V2")]
    public async Task EW_Write_Snappy_V2()
    {
        var path = Path.Combine(_dir, $"ew-snappy-v2-{Scenario}.parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, options: new ParquetWriteOptions
        {
            Compression = CompressionCodec.Snappy,
            DataPageVersion = DataPageVersion.V2,
        });
        await writer.WriteRowGroupAsync(CurrentBatch);
        await writer.CloseAsync();
    }

    [Benchmark(Description = "EW_Uncompressed_V2")]
    public async Task EW_Write_Uncompressed_V2()
    {
        var path = Path.Combine(_dir, $"ew-uncomp-v2-{Scenario}.parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, options: new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            DataPageVersion = DataPageVersion.V2,
        });
        await writer.WriteRowGroupAsync(CurrentBatch);
        await writer.CloseAsync();
    }

    [Benchmark(Description = "EW_Snappy_V1")]
    public async Task EW_Write_Snappy_V1()
    {
        var path = Path.Combine(_dir, $"ew-snappy-v1-{Scenario}.parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, options: new ParquetWriteOptions
        {
            Compression = CompressionCodec.Snappy,
            DataPageVersion = DataPageVersion.V1,
        });
        await writer.WriteRowGroupAsync(CurrentBatch);
        await writer.CloseAsync();
    }

    [Benchmark(Description = "EW_Zstd_V2")]
    public async Task EW_Write_Zstd_V2()
    {
        var path = Path.Combine(_dir, $"ew-zstd-v2-{Scenario}.parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, options: new ParquetWriteOptions
        {
            Compression = CompressionCodec.Zstd,
            DataPageVersion = DataPageVersion.V2,
        });
        await writer.WriteRowGroupAsync(CurrentBatch);
        await writer.CloseAsync();
    }

    [Benchmark(Description = "EW_NoDictionary")]
    public async Task EW_Write_NoDictionary()
    {
        var path = Path.Combine(_dir, $"ew-nodict-{Scenario}.parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, options: new ParquetWriteOptions
        {
            Compression = CompressionCodec.Snappy,
            DictionaryEnabled = false,
        });
        await writer.WriteRowGroupAsync(CurrentBatch);
        await writer.CloseAsync();
    }

    [Benchmark(Description = "Parquet.Net")]
    public async Task ParquetNet_Write()
    {
        var path = Path.Combine(_dir, $"pn-{Scenario}.parquet");
        var batch = CurrentBatch;

        var fields = new PqDataField[batch.ColumnCount];
        for (int c = 0; c < batch.ColumnCount; c++)
        {
            var f = batch.Schema.FieldsList[c];
            fields[c] = CreateParquetNetField(f);
        }
        var schema = new ParquetSchema(fields);

#if NET8_0_OR_GREATER
        await using var stream = File.Create(path);
        await using var writer = await ParquetWriter.CreateAsync(schema, stream);
#else
        using var stream = File.Create(path);
        using var writer = await ParquetWriter.CreateAsync(schema, stream);
#endif
        using var rg = writer.CreateRowGroup();

        for (int c = 0; c < batch.ColumnCount; c++)
        {
            await WriteParquetNetColumn(rg, fields[c], batch.Column(c), batch.Schema.FieldsList[c])
                .ConfigureAwait(false);
        }
    }

    private static PqDataField CreateParquetNetField(Field f) => f.DataType switch
    {
        Int32Type when f.IsNullable => new DataField<int?>(f.Name),
        Int32Type => new DataField<int>(f.Name),
        Int64Type when f.IsNullable => new DataField<long?>(f.Name),
        Int64Type => new DataField<long>(f.Name),
        DoubleType when f.IsNullable => new DataField<double?>(f.Name),
        DoubleType => new DataField<double>(f.Name),
        FloatType when f.IsNullable => new DataField<float?>(f.Name),
        FloatType => new DataField<float>(f.Name),
        BooleanType when f.IsNullable => new DataField<bool?>(f.Name),
        BooleanType => new DataField<bool>(f.Name),
        StringType => new DataField<string>(f.Name),
        _ => throw new NotSupportedException($"Unsupported type: {f.DataType}"),
    };

    private static async Task WriteParquetNetColumn(
        ParquetRowGroupWriter rg, PqDataField pqField, IArrowArray array, Field field)
    {
        switch (array)
        {
            case Int32Array a when field.IsNullable:
            {
                var values = new int?[a.Length];
                for (int i = 0; i < a.Length; i++) values[i] = a.IsNull(i) ? null : a.GetValue(i);
#if NET8_0_OR_GREATER
                await rg.WriteAsync<int>(pqField, values.AsMemory()).ConfigureAwait(false);
#else
                await rg.WriteColumnAsync(new global::Parquet.Data.DataColumn(pqField, values)).ConfigureAwait(false);
#endif
                break;
            }
            case Int32Array a:
            {
                var values = new int[a.Length];
                for (int i = 0; i < a.Length; i++) values[i] = a.GetValue(i)!.Value;
#if NET8_0_OR_GREATER
                await rg.WriteAsync<int>(pqField, values.AsMemory()).ConfigureAwait(false);
#else
                await rg.WriteColumnAsync(new global::Parquet.Data.DataColumn(pqField, values)).ConfigureAwait(false);
#endif
                break;
            }
            case Int64Array a when field.IsNullable:
            {
                var values = new long?[a.Length];
                for (int i = 0; i < a.Length; i++) values[i] = a.IsNull(i) ? null : a.GetValue(i);
#if NET8_0_OR_GREATER
                await rg.WriteAsync<long>(pqField, values.AsMemory()).ConfigureAwait(false);
#else
                await rg.WriteColumnAsync(new global::Parquet.Data.DataColumn(pqField, values)).ConfigureAwait(false);
#endif
                break;
            }
            case Int64Array a:
            {
                var values = new long[a.Length];
                for (int i = 0; i < a.Length; i++) values[i] = a.GetValue(i)!.Value;
#if NET8_0_OR_GREATER
                await rg.WriteAsync<long>(pqField, values.AsMemory()).ConfigureAwait(false);
#else
                await rg.WriteColumnAsync(new global::Parquet.Data.DataColumn(pqField, values)).ConfigureAwait(false);
#endif
                break;
            }
            case DoubleArray a when field.IsNullable:
            {
                var values = new double?[a.Length];
                for (int i = 0; i < a.Length; i++) values[i] = a.IsNull(i) ? null : a.GetValue(i);
#if NET8_0_OR_GREATER
                await rg.WriteAsync<double>(pqField, values.AsMemory()).ConfigureAwait(false);
#else
                await rg.WriteColumnAsync(new global::Parquet.Data.DataColumn(pqField, values)).ConfigureAwait(false);
#endif
                break;
            }
            case DoubleArray a:
            {
                var values = new double[a.Length];
                for (int i = 0; i < a.Length; i++) values[i] = a.GetValue(i)!.Value;
#if NET8_0_OR_GREATER
                await rg.WriteAsync<double>(pqField, values.AsMemory()).ConfigureAwait(false);
#else
                await rg.WriteColumnAsync(new global::Parquet.Data.DataColumn(pqField, values)).ConfigureAwait(false);
#endif
                break;
            }
            case BooleanArray a when field.IsNullable:
            {
                var values = new bool?[a.Length];
                for (int i = 0; i < a.Length; i++) values[i] = a.IsNull(i) ? null : a.GetValue(i);
#if NET8_0_OR_GREATER
                await rg.WriteAsync<bool>(pqField, values.AsMemory()).ConfigureAwait(false);
#else
                await rg.WriteColumnAsync(new global::Parquet.Data.DataColumn(pqField, values)).ConfigureAwait(false);
#endif
                break;
            }
            case BooleanArray a:
            {
                var values = new bool[a.Length];
                for (int i = 0; i < a.Length; i++) values[i] = a.GetValue(i)!.Value;
#if NET8_0_OR_GREATER
                await rg.WriteAsync<bool>(pqField, values.AsMemory()).ConfigureAwait(false);
#else
                await rg.WriteColumnAsync(new global::Parquet.Data.DataColumn(pqField, values)).ConfigureAwait(false);
#endif
                break;
            }
            case StringArray a:
            {
                var values = new string?[a.Length];
                for (int i = 0; i < a.Length; i++) values[i] = a.IsNull(i) ? null : a.GetString(i);
#if NET8_0_OR_GREATER
                await rg.WriteAsync(pqField, (IReadOnlyCollection<string>)values!).ConfigureAwait(false);
#else
                await rg.WriteColumnAsync(new global::Parquet.Data.DataColumn(pqField, values)).ConfigureAwait(false);
#endif
                break;
            }
            default:
                throw new NotSupportedException($"Unsupported array type: {array.GetType().Name}");
        }
    }

    [Benchmark(Description = "ParquetSharp")]
    public void ParquetSharp_Write()
    {
        var path = Path.Combine(_dir, $"ps-{Scenario}.parquet");
        var batch = CurrentBatch;

        var columns = new ParquetSharp.Column[batch.ColumnCount];
        for (int c = 0; c < batch.ColumnCount; c++)
        {
            var field = batch.Schema.FieldsList[c];
            columns[c] = CreateParquetSharpColumn(field);
        }

        using var props = new ParquetSharp.WriterPropertiesBuilder()
            .Compression(ParquetSharp.Compression.Snappy)
            .Build();

        using var writer = new ParquetSharp.ParquetFileWriter(path, columns, props);
        using var rowGroup = writer.AppendRowGroup();

        for (int c = 0; c < batch.ColumnCount; c++)
        {
            WriteParquetSharpColumn(rowGroup, batch.Column(c), batch.Schema.FieldsList[c]);
        }

        writer.Close();
    }

    private static ParquetSharp.Column CreateParquetSharpColumn(Field field)
    {
        return field.DataType switch
        {
            Int32Type when field.IsNullable => new ParquetSharp.Column<int?>(field.Name),
            Int32Type => new ParquetSharp.Column<int>(field.Name),
            Int64Type when field.IsNullable => new ParquetSharp.Column<long?>(field.Name),
            Int64Type => new ParquetSharp.Column<long>(field.Name),
            DoubleType when field.IsNullable => new ParquetSharp.Column<double?>(field.Name),
            DoubleType => new ParquetSharp.Column<double>(field.Name),
            FloatType when field.IsNullable => new ParquetSharp.Column<float?>(field.Name),
            FloatType => new ParquetSharp.Column<float>(field.Name),
            BooleanType when field.IsNullable => new ParquetSharp.Column<bool?>(field.Name),
            BooleanType => new ParquetSharp.Column<bool>(field.Name),
            StringType => new ParquetSharp.Column<byte[]>(field.Name),
            _ => throw new NotSupportedException($"Unsupported type: {field.DataType}"),
        };
    }

    private static void WriteParquetSharpColumn(ParquetSharp.RowGroupWriter rowGroup, IArrowArray array, Field field)
    {
        switch (array)
        {
            case Int32Array intArr when field.IsNullable:
            {
                using var col = rowGroup.NextColumn().LogicalWriter<int?>();
                var values = new int?[intArr.Length];
                for (int i = 0; i < intArr.Length; i++)
                    values[i] = intArr.IsNull(i) ? null : intArr.GetValue(i);
                col.WriteBatch(values);
                break;
            }
            case Int32Array intArr:
            {
                using var col = rowGroup.NextColumn().LogicalWriter<int>();
                var values = new int[intArr.Length];
                for (int i = 0; i < intArr.Length; i++)
                    values[i] = intArr.GetValue(i)!.Value;
                col.WriteBatch(values);
                break;
            }
            case Int64Array longArr when field.IsNullable:
            {
                using var col = rowGroup.NextColumn().LogicalWriter<long?>();
                var values = new long?[longArr.Length];
                for (int i = 0; i < longArr.Length; i++)
                    values[i] = longArr.IsNull(i) ? null : longArr.GetValue(i);
                col.WriteBatch(values);
                break;
            }
            case Int64Array longArr:
            {
                using var col = rowGroup.NextColumn().LogicalWriter<long>();
                var values = new long[longArr.Length];
                for (int i = 0; i < longArr.Length; i++)
                    values[i] = longArr.GetValue(i)!.Value;
                col.WriteBatch(values);
                break;
            }
            case DoubleArray dblArr when field.IsNullable:
            {
                using var col = rowGroup.NextColumn().LogicalWriter<double?>();
                var values = new double?[dblArr.Length];
                for (int i = 0; i < dblArr.Length; i++)
                    values[i] = dblArr.IsNull(i) ? null : dblArr.GetValue(i);
                col.WriteBatch(values);
                break;
            }
            case DoubleArray dblArr:
            {
                using var col = rowGroup.NextColumn().LogicalWriter<double>();
                var values = new double[dblArr.Length];
                for (int i = 0; i < dblArr.Length; i++)
                    values[i] = dblArr.GetValue(i)!.Value;
                col.WriteBatch(values);
                break;
            }
            case StringArray strArr:
            {
                using var col = rowGroup.NextColumn().LogicalWriter<byte[]>();
                var values = new byte[strArr.Length][];
                for (int i = 0; i < strArr.Length; i++)
                    values[i] = strArr.IsNull(i) ? null! : System.Text.Encoding.UTF8.GetBytes(strArr.GetString(i)!);
                col.WriteBatch(values);
                break;
            }
            case BooleanArray boolArr when field.IsNullable:
            {
                using var col = rowGroup.NextColumn().LogicalWriter<bool?>();
                var values = new bool?[boolArr.Length];
                for (int i = 0; i < boolArr.Length; i++)
                    values[i] = boolArr.IsNull(i) ? null : boolArr.GetValue(i);
                col.WriteBatch(values);
                break;
            }
            case BooleanArray boolArr:
            {
                using var col = rowGroup.NextColumn().LogicalWriter<bool>();
                var values = new bool[boolArr.Length];
                for (int i = 0; i < boolArr.Length; i++)
                    values[i] = boolArr.GetValue(i)!.Value;
                col.WriteBatch(values);
                break;
            }
            default:
                throw new NotSupportedException($"Unsupported array type: {array.GetType().Name}");
        }
    }
}

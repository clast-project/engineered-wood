// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Reports;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using Perfolizer.Horology;
using Field = Apache.Arrow.Field;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Compares default-settings write performance across EngineeredWood, ParquetSharp,
/// and Parquet.Net. All libraries use their out-of-the-box defaults.
///
/// Writes a ~1 MB mixed-type dataset (50k rows × 5 columns) and measures throughput.
/// File sizes are printed to console during GlobalCleanup for comparison.
///
/// Run with: dotnet run -c Release -- --filter "*DefaultWriteBenchmarks*"
/// </summary>
[MemoryDiagnoser]
public class DefaultWriteBenchmarks
{
    private string _dir = null!;

    // Arrow data (used by EW)
    private RecordBatch _batch = null!;

    // Raw arrays for ParquetSharp / Parquet.Net
    private int[] _ids = null!;
    private long[] _timestamps = null!;
    private double?[] _values = null!;
    private string?[] _names = null!;
    private bool?[] _flags = null!;

    // Parquet.Net nullable arrays (nullable types handled natively)
    private double?[] _pnValues = null!;
    private bool?[] _pnFlags = null!;

    private const int RowCount = 50_000;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "ew-defwrite-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_dir);

        var random = new Random(42);

        _ids = new int[RowCount];
        _timestamps = new long[RowCount];
        _values = new double?[RowCount];
        _names = new string?[RowCount];
        _flags = new bool?[RowCount];

        var intBuilder = new Int32Array.Builder();
        var longBuilder = new Int64Array.Builder();
        var doubleBuilder = new DoubleArray.Builder();
        var stringBuilder = new StringArray.Builder();
        var boolBuilder = new BooleanArray.Builder();

        for (int i = 0; i < RowCount; i++)
        {
            _ids[i] = random.Next();
            intBuilder.Append(_ids[i]);

            _timestamps[i] = 1700000000000L + i * 1000;
            longBuilder.Append(_timestamps[i]);

            if (random.NextDouble() < 0.05)
            {
                _values[i] = null;
                doubleBuilder.AppendNull();
            }
            else
            {
                _values[i] = random.NextDouble() * 10000 - 5000;
                doubleBuilder.Append(_values[i].Value);
            }

            if (random.NextDouble() < 0.05)
            {
                _names[i] = null;
                stringBuilder.AppendNull();
            }
            else
            {
                // ~25 byte strings → ~1.2 MB string data across 50k rows
                _names[i] = $"sensor-{random.Next(500):D3}/metric-{random.Next(100):D2}";
                stringBuilder.Append(_names[i]);
            }

            if (random.NextDouble() < 0.05)
            {
                _flags[i] = null;
                boolBuilder.AppendNull();
            }
            else
            {
                _flags[i] = random.NextDouble() > 0.5;
                boolBuilder.Append(_flags[i].Value);
            }
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("timestamp", Int64Type.Default, nullable: false))
            .Field(new Field("value", DoubleType.Default, nullable: true))
            .Field(new Field("name", StringType.Default, nullable: true))
            .Field(new Field("flag", BooleanType.Default, nullable: true))
            .Build();

        _batch = new RecordBatch(schema,
            [intBuilder.Build(), longBuilder.Build(), doubleBuilder.Build(),
             stringBuilder.Build(), boolBuilder.Build()], RowCount);

        // Parquet.Net uses nullable typed arrays directly
        _pnValues = _values;
        _pnFlags = _flags;
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        // Write one file with each library and report sizes
        Console.WriteLine();
        Console.WriteLine("  ── File Sizes ──────────────────────────────");

        try
        {
            var ewPath = Path.Combine(_dir, "final-ew.parquet");
            WriteEW(ewPath).GetAwaiter().GetResult();
            Console.WriteLine($"  EngineeredWood: {new FileInfo(ewPath).Length / 1024.0:F1} KB");
        }
        catch (Exception ex) { Console.WriteLine($"  EngineeredWood: error ({ex.Message})"); }

        try
        {
            var psPath = Path.Combine(_dir, "final-ps.parquet");
            WritePS(psPath);
            Console.WriteLine($"  ParquetSharp:   {new FileInfo(psPath).Length / 1024.0:F1} KB");
        }
        catch (Exception ex) { Console.WriteLine($"  ParquetSharp: error ({ex.Message})"); }

        try
        {
            var pnPath = Path.Combine(_dir, "final-pn.parquet");
            WritePN(pnPath).GetAwaiter().GetResult();
            Console.WriteLine($"  Parquet.Net:    {new FileInfo(pnPath).Length / 1024.0:F1} KB");
        }
        catch (Exception ex) { Console.WriteLine($"  Parquet.Net: error ({ex.Message})"); }

        Console.WriteLine("  ─────────────────────────────────────────────");

        if (Directory.Exists(_dir))
            Directory.Delete(_dir, recursive: true);
    }

    [Benchmark(Baseline = true, Description = "EngineeredWood")]
    public async Task EngineeredWood_Write()
    {
        await WriteEW(Path.Combine(_dir, "ew.parquet")).ConfigureAwait(false);
    }

    [Benchmark(Description = "ParquetSharp")]
    public void ParquetSharp_Write()
    {
        WritePS(Path.Combine(_dir, "ps.parquet"));
    }

    [Benchmark(Description = "Parquet.Net")]
    public async Task ParquetNet_Write()
    {
        await WritePN(Path.Combine(_dir, "pn.parquet")).ConfigureAwait(false);
    }

    private async Task WriteEW(string path)
    {
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false);
        await writer.WriteRowGroupAsync(_batch);
        await writer.CloseAsync();
    }

    private void WritePS(string path)
    {
        var columns = new ParquetSharp.Column[]
        {
            new ParquetSharp.Column<int>("id"),
            new ParquetSharp.Column<long>("timestamp"),
            new ParquetSharp.Column<double?>("value"),
            new ParquetSharp.Column<string>("name"),
            new ParquetSharp.Column<bool?>("flag"),
        };

        using var writer = new ParquetSharp.ParquetFileWriter(path, columns);
        using var rg = writer.AppendRowGroup();

        using (var col = rg.NextColumn().LogicalWriter<int>())
            col.WriteBatch(_ids);
        using (var col = rg.NextColumn().LogicalWriter<long>())
            col.WriteBatch(_timestamps);
        using (var col = rg.NextColumn().LogicalWriter<double?>())
            col.WriteBatch(_values);
        using (var col = rg.NextColumn().LogicalWriter<string>())
            col.WriteBatch(_names!);
        using (var col = rg.NextColumn().LogicalWriter<bool?>())
            col.WriteBatch(_flags);

        writer.Close();
    }

    private async Task WritePN(string path)
    {
        var schema = new ParquetSchema(
            new DataField<int>("id"),
            new DataField<long>("timestamp"),
            new DataField<double?>("value"),
            new DataField<string>("name"),
            new DataField<bool?>("flag"));

#if NET8_0_OR_GREATER
        await using var stream = File.Create(path);
        await using var writer = await ParquetWriter.CreateAsync(schema, stream);
        using var group = writer.CreateRowGroup();

        // Parquet.Net 6: typed WriteAsync<T> overloads replaced DataColumn. Explicit
        // type arguments are required so the generic overload binds correctly instead
        // of the string/byte[] helper overloads.
        await group.WriteAsync<int>((DataField)schema[0], _ids.AsMemory());
        await group.WriteAsync<long>((DataField)schema[1], _timestamps.AsMemory());
        await group.WriteAsync<double>((DataField)schema[2], _pnValues.AsMemory());
        await group.WriteAsync((DataField)schema[3], (IReadOnlyCollection<string>)_names!);
        await group.WriteAsync<bool>((DataField)schema[4], _pnFlags.AsMemory());
#else
        using var stream = File.Create(path);
        using var writer = await ParquetWriter.CreateAsync(schema, stream);
        using var group = writer.CreateRowGroup();

        await group.WriteColumnAsync(new DataColumn((DataField)schema[0], _ids));
        await group.WriteColumnAsync(new DataColumn((DataField)schema[1], _timestamps));
        await group.WriteColumnAsync(new DataColumn((DataField)schema[2], _pnValues));
        await group.WriteColumnAsync(new DataColumn((DataField)schema[3], _names!));
        await group.WriteColumnAsync(new DataColumn((DataField)schema[4], _pnFlags));
#endif
    }
}

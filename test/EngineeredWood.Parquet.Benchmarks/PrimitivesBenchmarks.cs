// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Parquet;
using Parquet.Schema;
using Field = Apache.Arrow.Field;
using PqDataField = Parquet.Schema.DataField;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Single-column primitive benchmarks mirroring the framing in the Parquet.Net 6
/// performance post (https://www.aloneguid.uk/posts/2026/04/parquet6/): 10M rows of
/// int / int? / double / double? / bool / bool?, with read and write timed across
/// EngineeredWood, ParquetSharp, and Parquet.Net (v6 on .NET 8+, v5.5.0 on net472).
///
/// Each scenario is a one-column file. Files are written once in [GlobalSetup] using
/// EngineeredWood (default settings: Snappy, dictionary on, V1 pages) so all readers
/// hit the same bytes. Write benchmarks emit to the temp dir and are deleted in
/// [GlobalCleanup]; read benchmarks reuse the prewritten EW file.
///
/// Run with: dotnet run -c Release --framework net10.0 -- --filter "*PrimitivesBenchmarks*"
/// </summary>
[MemoryDiagnoser]
public class PrimitivesBenchmarks
{
    public const int RowCount = 10_000_000;

    public enum Scenario { Int, IntNullable, Double, DoubleNullable, Bool, BoolNullable }

    [Params(Scenario.Int, Scenario.IntNullable, Scenario.Double, Scenario.DoubleNullable, Scenario.Bool, Scenario.BoolNullable)]
    public Scenario Kind { get; set; }

    private string _dir = null!;
    private string _readPath = null!;

    // Source data
    private int[] _ints = null!;
    private int?[] _nints = null!;
    private double[] _doubles = null!;
    private double?[] _ndoubles = null!;
    private bool[] _bools = null!;
    private bool?[] _nbools = null!;

    // Pre-built Arrow record batches (one per scenario; only the selected one is hot)
    private RecordBatch _batch = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "ew-prim-bench-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_dir);

        var random = new Random(42);

        _ints = new int[RowCount];
        for (int i = 0; i < RowCount; i++) _ints[i] = random.Next();

        _nints = new int?[RowCount];
        for (int i = 0; i < RowCount; i++)
            _nints[i] = random.NextDouble() < 0.1 ? null : random.Next();

        _doubles = new double[RowCount];
        for (int i = 0; i < RowCount; i++) _doubles[i] = random.NextDouble() * 1_000_000.0 - 500_000.0;

        _ndoubles = new double?[RowCount];
        for (int i = 0; i < RowCount; i++)
            _ndoubles[i] = random.NextDouble() < 0.1 ? null : random.NextDouble() * 1_000_000.0 - 500_000.0;

        _bools = new bool[RowCount];
        for (int i = 0; i < RowCount; i++) _bools[i] = random.Next(2) == 0;

        _nbools = new bool?[RowCount];
        for (int i = 0; i < RowCount; i++)
            _nbools[i] = random.NextDouble() < 0.1 ? null : random.Next(2) == 0;

        _batch = BuildBatch();

        // Pre-write a file per scenario so read benchmarks don't depend on write speed.
        _readPath = Path.Combine(_dir, $"read-{Kind}.parquet");
        await WriteEWAsync(_readPath).ConfigureAwait(false);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        if (Directory.Exists(_dir))
            Directory.Delete(_dir, recursive: true);
    }

    private RecordBatch BuildBatch()
    {
        switch (Kind)
        {
            case Scenario.Int:
            {
                var b = new Int32Array.Builder();
                b.Append(_ints);
                var schema = new Apache.Arrow.Schema.Builder()
                    .Field(new Field("v", Int32Type.Default, nullable: false)).Build();
                return new RecordBatch(schema, [b.Build()], RowCount);
            }
            case Scenario.IntNullable:
            {
                var b = new Int32Array.Builder();
                for (int i = 0; i < RowCount; i++)
                {
                    if (_nints[i].HasValue) b.Append(_nints[i]!.Value); else b.AppendNull();
                }
                var schema = new Apache.Arrow.Schema.Builder()
                    .Field(new Field("v", Int32Type.Default, nullable: true)).Build();
                return new RecordBatch(schema, [b.Build()], RowCount);
            }
            case Scenario.Double:
            {
                var b = new DoubleArray.Builder();
                b.Append(_doubles);
                var schema = new Apache.Arrow.Schema.Builder()
                    .Field(new Field("v", DoubleType.Default, nullable: false)).Build();
                return new RecordBatch(schema, [b.Build()], RowCount);
            }
            case Scenario.DoubleNullable:
            {
                var b = new DoubleArray.Builder();
                for (int i = 0; i < RowCount; i++)
                {
                    if (_ndoubles[i].HasValue) b.Append(_ndoubles[i]!.Value); else b.AppendNull();
                }
                var schema = new Apache.Arrow.Schema.Builder()
                    .Field(new Field("v", DoubleType.Default, nullable: true)).Build();
                return new RecordBatch(schema, [b.Build()], RowCount);
            }
            case Scenario.Bool:
            {
                var b = new BooleanArray.Builder();
                for (int i = 0; i < RowCount; i++) b.Append(_bools[i]);
                var schema = new Apache.Arrow.Schema.Builder()
                    .Field(new Field("v", BooleanType.Default, nullable: false)).Build();
                return new RecordBatch(schema, [b.Build()], RowCount);
            }
            case Scenario.BoolNullable:
            {
                var b = new BooleanArray.Builder();
                for (int i = 0; i < RowCount; i++)
                {
                    if (_nbools[i].HasValue) b.Append(_nbools[i]!.Value); else b.AppendNull();
                }
                var schema = new Apache.Arrow.Schema.Builder()
                    .Field(new Field("v", BooleanType.Default, nullable: true)).Build();
                return new RecordBatch(schema, [b.Build()], RowCount);
            }
            default: throw new InvalidOperationException();
        }
    }

    // ---------------- Write benchmarks ----------------

    [Benchmark(Baseline = true, Description = "EW_Write")]
    public Task EngineeredWood_Write() => WriteEWAsync(Path.Combine(_dir, $"ew-w-{Kind}.parquet"));

    [Benchmark(Description = "PS_Write")]
    public void ParquetSharp_Write()
    {
        string path = Path.Combine(_dir, $"ps-w-{Kind}.parquet");
        ParquetSharp.Column col = Kind switch
        {
            Scenario.Int => new ParquetSharp.Column<int>("v"),
            Scenario.IntNullable => new ParquetSharp.Column<int?>("v"),
            Scenario.Double => new ParquetSharp.Column<double>("v"),
            Scenario.DoubleNullable => new ParquetSharp.Column<double?>("v"),
            Scenario.Bool => new ParquetSharp.Column<bool>("v"),
            Scenario.BoolNullable => new ParquetSharp.Column<bool?>("v"),
            _ => throw new InvalidOperationException(),
        };

        using var props = new ParquetSharp.WriterPropertiesBuilder()
            .Compression(ParquetSharp.Compression.Snappy).Build();
        using var writer = new ParquetSharp.ParquetFileWriter(path, [col], props);
        using var rg = writer.AppendRowGroup();
        switch (Kind)
        {
            case Scenario.Int:
                using (var w = rg.NextColumn().LogicalWriter<int>()) w.WriteBatch(_ints); break;
            case Scenario.IntNullable:
                using (var w = rg.NextColumn().LogicalWriter<int?>()) w.WriteBatch(_nints); break;
            case Scenario.Double:
                using (var w = rg.NextColumn().LogicalWriter<double>()) w.WriteBatch(_doubles); break;
            case Scenario.DoubleNullable:
                using (var w = rg.NextColumn().LogicalWriter<double?>()) w.WriteBatch(_ndoubles); break;
            case Scenario.Bool:
                using (var w = rg.NextColumn().LogicalWriter<bool>()) w.WriteBatch(_bools); break;
            case Scenario.BoolNullable:
                using (var w = rg.NextColumn().LogicalWriter<bool?>()) w.WriteBatch(_nbools); break;
        }
        writer.Close();
    }

    [Benchmark(Description = "PN_Write")]
    public async Task ParquetNet_Write()
    {
        string path = Path.Combine(_dir, $"pn-w-{Kind}.parquet");
        PqDataField field = Kind switch
        {
            Scenario.Int => new DataField<int>("v"),
            Scenario.IntNullable => new DataField<int?>("v"),
            Scenario.Double => new DataField<double>("v"),
            Scenario.DoubleNullable => new DataField<double?>("v"),
            Scenario.Bool => new DataField<bool>("v"),
            Scenario.BoolNullable => new DataField<bool?>("v"),
            _ => throw new InvalidOperationException(),
        };
        var schema = new ParquetSchema(field);

#if NET8_0_OR_GREATER
        await using var stream = File.Create(path);
        await using var writer = await ParquetWriter.CreateAsync(schema, stream).ConfigureAwait(false);
#else
        using var stream = File.Create(path);
        using var writer = await ParquetWriter.CreateAsync(schema, stream).ConfigureAwait(false);
#endif
        using var rg = writer.CreateRowGroup();
#if NET8_0_OR_GREATER
        switch (Kind)
        {
            case Scenario.Int: await rg.WriteAsync<int>(field, _ints.AsMemory()).ConfigureAwait(false); break;
            case Scenario.IntNullable: await rg.WriteAsync<int>(field, _nints.AsMemory()).ConfigureAwait(false); break;
            case Scenario.Double: await rg.WriteAsync<double>(field, _doubles.AsMemory()).ConfigureAwait(false); break;
            case Scenario.DoubleNullable: await rg.WriteAsync<double>(field, _ndoubles.AsMemory()).ConfigureAwait(false); break;
            case Scenario.Bool: await rg.WriteAsync<bool>(field, _bools.AsMemory()).ConfigureAwait(false); break;
            case Scenario.BoolNullable: await rg.WriteAsync<bool>(field, _nbools.AsMemory()).ConfigureAwait(false); break;
        }
#else
        switch (Kind)
        {
            case Scenario.Int: await rg.WriteColumnAsync(new global::Parquet.Data.DataColumn(field, _ints)).ConfigureAwait(false); break;
            case Scenario.IntNullable: await rg.WriteColumnAsync(new global::Parquet.Data.DataColumn(field, _nints)).ConfigureAwait(false); break;
            case Scenario.Double: await rg.WriteColumnAsync(new global::Parquet.Data.DataColumn(field, _doubles)).ConfigureAwait(false); break;
            case Scenario.DoubleNullable: await rg.WriteColumnAsync(new global::Parquet.Data.DataColumn(field, _ndoubles)).ConfigureAwait(false); break;
            case Scenario.Bool: await rg.WriteColumnAsync(new global::Parquet.Data.DataColumn(field, _bools)).ConfigureAwait(false); break;
            case Scenario.BoolNullable: await rg.WriteColumnAsync(new global::Parquet.Data.DataColumn(field, _nbools)).ConfigureAwait(false); break;
        }
#endif
    }

    // ---------------- Read benchmarks ----------------

    [Benchmark(Description = "EW_Read")]
    public async Task EngineeredWood_Read()
    {
        using var file = new LocalRandomAccessFile(_readPath);
        using var reader = new ParquetFileReader(file);
        using var batch = await reader.ReadRowGroupAsync(0).ConfigureAwait(false);
    }

    [Benchmark(Description = "PS_Read")]
    public void ParquetSharp_Read()
    {
        using var reader = new ParquetSharp.ParquetFileReader(_readPath);
        using var rg = reader.RowGroup(0);
        long n = rg.MetaData.NumRows;
        using var col = rg.Column(0);
        switch (Kind)
        {
            case Scenario.Int: ReadPS<int>(col, n); break;
            case Scenario.IntNullable: ReadPS<int?>(col, n); break;
            case Scenario.Double: ReadPS<double>(col, n); break;
            case Scenario.DoubleNullable: ReadPS<double?>(col, n); break;
            case Scenario.Bool: ReadPS<bool>(col, n); break;
            case Scenario.BoolNullable: ReadPS<bool?>(col, n); break;
        }
    }

    private static void ReadPS<T>(ParquetSharp.ColumnReader col, long numRows)
    {
        using var logical = col.LogicalReader<T>();
        var buffer = new T[numRows];
        logical.ReadBatch(buffer);
    }

    [Benchmark(Description = "PN_Read")]
    public async Task ParquetNet_Read()
    {
        using var stream = File.OpenRead(_readPath);
#if NET8_0_OR_GREATER
        await using var reader = await ParquetReader.CreateAsync(stream).ConfigureAwait(false);
        using var rg = reader.OpenRowGroupReader(0);
        var field = reader.Schema.GetDataFields()[0];
        int n = checked((int)reader.RowGroups[0].RowCount);
        switch (Kind)
        {
            case Scenario.Int: { var b = new int[n]; await rg.ReadAsync<int>(field, b.AsMemory()).ConfigureAwait(false); break; }
            case Scenario.IntNullable: { var b = new int?[n]; await rg.ReadAsync<int>(field, b.AsMemory()).ConfigureAwait(false); break; }
            case Scenario.Double: { var b = new double[n]; await rg.ReadAsync<double>(field, b.AsMemory()).ConfigureAwait(false); break; }
            case Scenario.DoubleNullable: { var b = new double?[n]; await rg.ReadAsync<double>(field, b.AsMemory()).ConfigureAwait(false); break; }
            case Scenario.Bool: { var b = new bool[n]; await rg.ReadAsync<bool>(field, b.AsMemory()).ConfigureAwait(false); break; }
            case Scenario.BoolNullable: { var b = new bool?[n]; await rg.ReadAsync<bool>(field, b.AsMemory()).ConfigureAwait(false); break; }
        }
#else
        using var reader = await ParquetReader.CreateAsync(stream).ConfigureAwait(false);
        using var rg = reader.OpenRowGroupReader(0);
        var field = reader.Schema.GetDataFields()[0];
        var col = await rg.ReadColumnAsync(field).ConfigureAwait(false);
        _ = col.Data.Length;
#endif
    }

    private async Task WriteEWAsync(string path)
    {
        // ParquetSharp's reader currently requires path_in_schema; emit it so
        // cross-reader benchmarks (PS_Read, PN_Read) can parse EW-written files.
        // RowGroupMaxRows=RowCount keeps the whole 10M-row column in a single row group
        // so all readers see the same shape (default would split into 10× 1M groups).
        var options = new ParquetWriteOptions
        {
            RowGroupMaxRows = RowCount,
        };
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(_batch).ConfigureAwait(false);
        await writer.CloseAsync().ConfigureAwait(false);
    }
}

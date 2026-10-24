// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Field = Apache.Arrow.Field;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Writes and reads one nullable Decimal128 column at several precisions (#504). The physical layout follows
/// the precision: INT32 up to 9 digits, INT64 up to 18, then the narrowest FIXED_LEN_BYTE_ARRAY; before #504
/// every precision was FLBA(16). Precision 38 is FLBA(16) either way, so it is the control. The file size is
/// printed from <see cref="GlobalSetup"/>.
/// </summary>
[MemoryDiagnoser]
public class DecimalLayoutBenchmarks
{
    private const int RowCount = 1_000_000;

    private string _dir = null!;
    private string _readPath = null!;
    private RecordBatch _batch = null!;

    [Params(9, 18, 20, 38)]
    public int Precision { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "ew-decimal-layout-bench-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_dir);

        // Uniform unscaled values over the whole precision range, 10% null.
        var random = new Random(42);
        var max = BigInteger.Pow(10, Precision) - 1;
        var bytes = new byte[RowCount * 16];
        var validity = new ArrowBuffer.BitmapBuilder();
        int nulls = 0;
        var draw = new byte[17];
        for (int i = 0; i < RowCount; i++)
        {
            bool valid = random.NextDouble() >= 0.1;
            validity.Append(valid);
            if (!valid) { nulls++; continue; }
            random.NextBytes(draw);
            draw[16] = 0;
            var value = new BigInteger(draw) % (2 * max + 1) - max;
            var dest = bytes.AsSpan(i * 16, 16);
            dest.Fill(value.Sign < 0 ? (byte)0xFF : (byte)0x00);
            value.ToByteArray().CopyTo(dest);
        }

        var type = new Decimal128Type(Precision, 2);
        var schema = new Apache.Arrow.Schema([new Field("d", type, nullable: true)], null);
        var array = new Decimal128Array(new ArrayData(type, RowCount, nulls, 0,
            [validity.Build(), new ArrowBuffer(bytes)]));
        _batch = new RecordBatch(schema, [array], RowCount);

        _readPath = Path.Combine(_dir, "read.parquet");
        await WriteAsync(_readPath);
        Console.WriteLine($"// decimal({Precision},2): {new FileInfo(_readPath).Length:N0} bytes");
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        if (Directory.Exists(_dir))
            Directory.Delete(_dir, recursive: true);
    }

    [Benchmark]
    public Task Write() => WriteAsync(Path.Combine(_dir, "write.parquet"));

    [Benchmark]
    public async Task Read()
    {
        using var file = new LocalRandomAccessFile(_readPath);
        using var reader = new ParquetFileReader(file);
        using var batch = await reader.ReadRowGroupAsync(0).ConfigureAwait(false);
    }

    private async Task WriteAsync(string path)
    {
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, options: new ParquetWriteOptions
        {
            Compression = CompressionCodec.Snappy,
        });
        await writer.WriteRowGroupAsync(_batch);
        await writer.CloseAsync();
    }
}

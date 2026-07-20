// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.IO.Local;
using EngineeredWood.Orc;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.Benchmarks;

[MemoryDiagnoser]
public class WriteBenchmarks
{
    private RecordBatch _batch = null!;
    private string _tempDir = null!;

    [GlobalSetup]
    public void Setup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-orc-bench");
        Directory.CreateDirectory(_tempDir);

        // Build a batch with 100k rows: 3 int columns + 1 nullable int column
        const int rowCount = 100_000;
        var rng = new Random(42);

        var col0 = new Int32Array.Builder().AppendRange(Enumerable.Range(0, rowCount)).Build();
        var col1 = new Int64Array.Builder().AppendRange(Enumerable.Range(0, rowCount).Select(i => (long)i * 17)).Build();

        var doubleBuilder = new DoubleArray.Builder();
        for (int i = 0; i < rowCount; i++)
            doubleBuilder.Append(rng.NextDouble() * 1000);
        var col2 = doubleBuilder.Build();

        // Nullable int column (~10% nulls)
        var nullableBuilder = new Int32Array.Builder();
        for (int i = 0; i < rowCount; i++)
        {
            if (rng.Next(10) == 0)
                nullableBuilder.AppendNull();
            else
                nullableBuilder.Append(rng.Next(0, 1_000_000));
        }
        var col3 = nullableBuilder.Build();

        var schema = new Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("value", Int64Type.Default, false))
            .Field(new Field("measure", DoubleType.Default, false))
            .Field(new Field("nullable_int", Int32Type.Default, true))
            .Build();

        _batch = new RecordBatch(schema, [col0, col1, col2, col3], rowCount);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Benchmark]
    public async Task Write_Uncompressed()
    {
        var path = Path.Combine(_tempDir, "uncompressed.orc");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new OrcWriter(file, _batch.Schema, ownsFile: false,
            new OrcWriterOptions { Compression = CompressionKind.None });
        await writer.WriteBatchAsync(_batch);
    }

    [Benchmark]
    public async Task Write_Zlib()
    {
        var path = Path.Combine(_tempDir, "zlib.orc");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new OrcWriter(file, _batch.Schema, ownsFile: false,
            new OrcWriterOptions { Compression = CompressionKind.Zlib });
        await writer.WriteBatchAsync(_batch);
    }

    [Benchmark]
    public async Task Write_Zstd()
    {
        var path = Path.Combine(_tempDir, "zstd.orc");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new OrcWriter(file, _batch.Schema, ownsFile: false,
            new OrcWriterOptions { Compression = CompressionKind.Zstd });
        await writer.WriteBatchAsync(_batch);
    }
}

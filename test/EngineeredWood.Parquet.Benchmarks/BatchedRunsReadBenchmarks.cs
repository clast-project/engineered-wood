// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Measures the read-side effect of <see cref="ParquetWriteOptions.BatchBitPackedRuns"/>: the same
/// data written with per-8-value run headers versus batched literal runs, read back both ways.
/// </summary>
/// <remarks>
/// Two independent effects are in play, which is why the fast path is a parameter rather than an
/// assumption:
/// <list type="bullet">
///   <item><description>The <em>general</em> decode path re-parses a run header every 8 values on
///   header-interleaved streams; batching amortises that across up to 504.</description></item>
///   <item><description>The <em>fixed-list</em> detector can only use its vectorised tiled scan on a
///   contiguous block, which batching is what produces.</description></item>
/// </list>
/// </remarks>
[MemoryDiagnoser]
public class BatchedRunsReadBenchmarks
{
    public const int Rows = 400_000;
    private const int ListLength = 3;

    private string _dir = null!;
    private string _path = null!;

    /// <summary>Whether the file under test was written with batched literal runs.</summary>
    [Params(false, true)]
    public bool Batched { get; set; }

    /// <summary>Whether the reader's fixed-length list fast path is enabled.</summary>
    [Params(false, true)]
    public bool FastPath { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "ew-batched-bench-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_dir);
        _path = Path.Combine(_dir, $"lists-{(Batched ? "batched" : "plain")}.parquet");

        var listType = new ListType(new Field("element", FloatType.Default, nullable: false));
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("vec", listType, nullable: false))
            .Build();

        var values = new FloatArray.Builder();
        values.Reserve(Rows * ListLength);
        var offsets = new int[Rows + 1];
        for (int i = 0; i < Rows; i++)
        {
            offsets[i] = i * ListLength;
            for (int j = 0; j < ListLength; j++)
                values.Append((i * 31 + j * 7) * 0.125f);
        }
        offsets[Rows] = Rows * ListLength;

        var data = new ArrayData(listType, Rows, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray())],
            [values.Build().Data]);
        var batch = new RecordBatch(schema, [new ListArray(data)], Rows);

        var options = ParquetWriteOptions.Default with
        {
            Compression = EngineeredWood.Compression.CompressionCodec.Uncompressed,
            FloatingPointEncoding = FloatingPointEncoding.Plain,
            DictionaryEnabled = false,
            BatchBitPackedRuns = Batched,
        };

        await using var file = new LocalSequentialFile(_path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch).ConfigureAwait(false);
        await writer.CloseAsync().ConfigureAwait(false);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        if (Directory.Exists(_dir))
            Directory.Delete(_dir, recursive: true);
    }

    [Benchmark]
    public async Task<int> Read()
    {
        await using var file = new LocalRandomAccessFile(_path);
        using var reader = new ParquetFileReader(
            file, ownsFile: false, new ParquetReadOptions { FixedListFastPath = FastPath });
        using var batch = await reader.ReadRowGroupAsync(0).ConfigureAwait(false);
        return batch.Length;
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// How the value data is written, which changes how much of the read is level handling versus
/// value decoding — and therefore how much of the read the fast path can remove.
/// </summary>
public enum FixedListLayout
{
    /// <summary>Uncompressed PLAIN floats: the level work is a large share of the read.</summary>
    Plain,

    /// <summary>Library defaults (Snappy + BYTE_STREAM_SPLIT): what a real embedding file looks like.</summary>
    Default,
}

/// <summary>
/// Measures <see cref="ParquetReadOptions.FixedListFastPath"/> against the general Dremel path on
/// list columns that are in fact fixed-length and fully defined.
/// </summary>
/// <remarks>
/// The element count is held constant across list lengths (see <see cref="TotalElements"/>), so the
/// rows of the results table are directly comparable: only the shape of the level streams changes.
/// </remarks>
[MemoryDiagnoser]
public class FixedListReadBenchmarks
{
    /// <summary>Floats per file, held constant so the sweep isolates list length.</summary>
    public const int TotalElements = 2_000_000;

    private string _dir = null!;

    [Params(3, 8, 16, 64, 256, 768)]
    public int Length { get; set; }

    [Params(FixedListLayout.Plain, FixedListLayout.Default)]
    public FixedListLayout Layout { get; set; }

    private string FilePath { get; set; } = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        // BenchmarkDotNet runs this once per benchmark process, i.e. once per parameter
        // combination — so only the file this process will actually read gets written.
        _dir = Path.Combine(Path.GetTempPath(), "ew-fixedlist-bench-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_dir);

        FilePath = Path.Combine(_dir, $"fixed-{Length}-{Layout}.parquet");
        await FixedListBenchmarkData.WriteFixedAsync(FilePath, TotalElements / Length, Length, Layout);

        // Guard the measurement: if the fast path silently stopped engaging, these benchmarks would
        // report "no speedup" rather than a failure.
        if (!await FixedListBenchmarkData.FastPathEngagesAsync(FilePath, Length))
            throw new InvalidOperationException($"Fast path did not engage for {FilePath}.");
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        if (Directory.Exists(_dir))
            Directory.Delete(_dir, recursive: true);
    }

    [Benchmark(Baseline = true, Description = "General (Dremel levels)")]
    public async Task<int> General()
    {
        await using var file = new LocalRandomAccessFile(FilePath);
        using var reader = new ParquetFileReader(
            file, ownsFile: false, new ParquetReadOptions { FixedListFastPath = false });
        using var batch = await reader.ReadRowGroupAsync(0).ConfigureAwait(false);
        return batch.Length;
    }

    [Benchmark(Description = "FixedListFastPath")]
    public async Task<int> FastPath()
    {
        await using var file = new LocalRandomAccessFile(FilePath);
        using var reader = new ParquetFileReader(
            file, ownsFile: false, new ParquetReadOptions { FixedListFastPath = true });
        using var batch = await reader.ReadRowGroupAsync(0).ConfigureAwait(false);
        return batch.Length;
    }
}

/// <summary>
/// Measures what the fast path costs when it does <em>not</em> apply: the probe runs, fails, and the
/// chunk is read again down the general path. This is the price paid by every workload that turns
/// the option on and reads ragged lists.
/// </summary>
[MemoryDiagnoser]
public class FixedListFallbackBenchmarks
{
    private string _dir = null!;

    /// <summary>
    /// <c>Ragged</c> breaks the pattern in the first page (the probe bails almost immediately);
    /// <c>LateBreak</c> is fixed-length until the very last row, so the probe decodes the whole
    /// chunk before failing — the worst case for the technique.
    /// </summary>
    [Params("Ragged", "LateBreak")]
    public string Shape { get; set; } = null!;

    private string FilePath { get; set; } = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "ew-fixedlist-fb-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_dir);

        FilePath = Path.Combine(_dir, $"{Shape}.parquet");
        if (Shape == "Ragged")
            await FixedListBenchmarkData.WriteRaggedAsync(FilePath, rows: 62_500, averageLength: 32);
        else
            await FixedListBenchmarkData.WriteLateBreakAsync(FilePath, rows: 62_500, length: 32);

        if (await FixedListBenchmarkData.FastPathEngagesAsync(FilePath, 0))
            throw new InvalidOperationException($"Fast path unexpectedly engaged for {FilePath}.");
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        if (Directory.Exists(_dir))
            Directory.Delete(_dir, recursive: true);
    }

    [Benchmark(Baseline = true, Description = "General (option off)")]
    public async Task<int> General()
    {
        await using var file = new LocalRandomAccessFile(FilePath);
        using var reader = new ParquetFileReader(
            file, ownsFile: false, new ParquetReadOptions { FixedListFastPath = false });
        using var batch = await reader.ReadRowGroupAsync(0).ConfigureAwait(false);
        return batch.Length;
    }

    [Benchmark(Description = "Probe + fall back")]
    public async Task<int> ProbeThenFallBack()
    {
        await using var file = new LocalRandomAccessFile(FilePath);
        using var reader = new ParquetFileReader(
            file, ownsFile: false, new ParquetReadOptions { FixedListFastPath = true });
        using var batch = await reader.ReadRowGroupAsync(0).ConfigureAwait(false);
        return batch.Length;
    }
}

internal static class FixedListBenchmarkData
{
    private static readonly ListType VectorType = new(new Field("element", FloatType.Default, nullable: false));

    private static readonly Apache.Arrow.Schema VectorSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("vec", VectorType, nullable: false))
        .Build();

    public static Task WriteFixedAsync(string path, int rows, int length, FixedListLayout layout)
    {
        var values = new FloatArray.Builder();
        values.Reserve(rows * length);
        var offsets = new int[rows + 1];
        for (int i = 0; i < rows; i++)
        {
            offsets[i] = i * length;
            for (int j = 0; j < length; j++)
                values.Append((i * 31 + j * 7) * 0.125f);
        }
        offsets[rows] = rows * length;

        return WriteAsync(path, BuildBatch(rows, offsets, values.Build(), nullCount: 0, validity: null), layout);
    }

    /// <summary>Lists whose lengths vary from row to row, with occasional nulls and empties.</summary>
    public static Task WriteRaggedAsync(string path, int rows, int averageLength)
    {
        var values = new FloatArray.Builder();
        var offsets = new int[rows + 1];
        var validity = new byte[(rows + 7) / 8];
        int nullCount = 0;
        int offset = 0;

        for (int i = 0; i < rows; i++)
        {
            offsets[i] = offset;
            if (i % 97 == 5)
            {
                nullCount++;
                continue;
            }

            validity[i >> 3] |= (byte)(1 << (i & 7));
            int len = averageLength - 4 + (i % 9);
            for (int j = 0; j < len; j++, offset++)
                values.Append((i + j) * 0.5f);
        }
        offsets[rows] = offset;

        return WriteAsync(path, BuildBatch(rows, offsets, values.Build(), nullCount, validity), FixedListLayout.Plain);
    }

    /// <summary>
    /// Every list is <paramref name="length"/> long except the last, which is one element short —
    /// the probe cannot know that until it has walked the entire chunk.
    /// </summary>
    public static Task WriteLateBreakAsync(string path, int rows, int length)
    {
        var values = new FloatArray.Builder();
        var offsets = new int[rows + 1];
        int offset = 0;

        for (int i = 0; i < rows; i++)
        {
            offsets[i] = offset;
            int len = i == rows - 1 ? length - 1 : length;
            for (int j = 0; j < len; j++, offset++)
                values.Append((i * 31 + j * 7) * 0.125f);
        }
        offsets[rows] = offset;

        return WriteAsync(path, BuildBatch(rows, offsets, values.Build(), nullCount: 0, validity: null), FixedListLayout.Plain);
    }

    private static RecordBatch BuildBatch(
        int rows, int[] offsets, FloatArray values, int nullCount, byte[]? validity)
    {
        var schema = validity is null
            ? VectorSchema
            : new Apache.Arrow.Schema.Builder().Field(new Field("vec", VectorType, nullable: true)).Build();

        var data = new ArrayData(VectorType, rows, nullCount, 0,
            [
                validity is null ? ArrowBuffer.Empty : new ArrowBuffer(validity),
                new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray()),
            ],
            [values.Data]);

        return new RecordBatch(schema, [new ListArray(data)], rows);
    }

    private static async Task WriteAsync(string path, RecordBatch batch, FixedListLayout layout)
    {
        var options = layout == FixedListLayout.Plain
            ? ParquetWriteOptions.Default with
            {
                Compression = CompressionCodec.Uncompressed,
                FloatingPointEncoding = FloatingPointEncoding.Plain,
                DictionaryEnabled = false,
            }
            : ParquetWriteOptions.Default;

        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch).ConfigureAwait(false);
        await writer.CloseAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Reads the vector column chunk directly so the caller can see whether the detector fired,
    /// which a wall-clock measurement alone cannot tell you.
    /// </summary>
    public static async Task<bool> FastPathEngagesAsync(string path, int expectedLength)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var metadata = await reader.ReadMetadataAsync().ConfigureAwait(false);
        var schema = await reader.GetSchemaAsync().ConfigureAwait(false);
        var rowGroup = metadata.RowGroups[0];
        var meta = rowGroup.Columns[0].MetaData!;

        long start = meta.DictionaryPageOffset is > 0 and long dpo ? dpo : meta.DataPageOffset;
        using var buffer = await file
            .ReadAsync(new EngineeredWood.IO.FileRange(start, meta.TotalCompressedSize))
            .ConfigureAwait(false);

        var column = schema.Columns[0];
        var result = Parquet.Data.ColumnChunkReader.ReadColumn(
            buffer.Memory.Span, column, meta, checked((int)rowGroup.NumRows),
            Parquet.Data.ArrowSchemaConverter.ToArrowField(column),
            preserveDefLevels: true, validateCrc: false, fixedListFastPath: true);

        return expectedLength > 0
            ? result.FixedListLength == expectedLength
            : result.FixedListLength > 0;
    }
}

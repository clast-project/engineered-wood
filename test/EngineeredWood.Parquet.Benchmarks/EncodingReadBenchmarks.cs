// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Attributes;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Parquet;
using Parquet.Schema;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Compares EngineeredWood, ParquetSharp, and Parquet.Net reading files with
/// DELTA_BYTE_ARRAY, DELTA_LENGTH_BYTE_ARRAY, and BYTE_STREAM_SPLIT encodings.
/// </summary>
[MemoryDiagnoser]
public class EncodingReadBenchmarks
{
    private string _dir = null!;
    private Dictionary<string, string> _files = null!;

    [Params(TestFileGenerator.DeltaByteArray, TestFileGenerator.DeltaLengthByteArray, TestFileGenerator.ByteStreamSplit)]
    public string File { get; set; } = null!;

    private string FilePath => _files[File];

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dir = TestFileGenerator.GenerateAll();
        _files = new Dictionary<string, string>
        {
            [TestFileGenerator.DeltaByteArray] = Path.Combine(_dir, TestFileGenerator.DeltaByteArray + ".parquet"),
            [TestFileGenerator.DeltaLengthByteArray] = Path.Combine(_dir, TestFileGenerator.DeltaLengthByteArray + ".parquet"),
            [TestFileGenerator.ByteStreamSplit] = Path.Combine(_dir, TestFileGenerator.ByteStreamSplit + ".parquet"),
        };
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        TestFileGenerator.Cleanup(_dir);
    }

    [Benchmark(Baseline = true, Description = "EngineeredWood")]
    public async Task EngineeredWood_Read()
    {
        using var file = new LocalRandomAccessFile(FilePath);
        using var reader = new ParquetFileReader(file);
        using var batch = await reader.ReadRowGroupAsync(0).ConfigureAwait(false);
    }

    [Benchmark(Description = "ParquetSharp")]
    public void ParquetSharp_Read()
    {
        using var reader = new ParquetSharp.ParquetFileReader(FilePath);
        using var rowGroup = reader.RowGroup(0);
        long numRows = rowGroup.MetaData.NumRows;

        if (File == TestFileGenerator.ByteStreamSplit)
        {
            // float, double, int, long
            ReadParquetSharpColumn<float>(rowGroup, 0, numRows);
            ReadParquetSharpColumn<double>(rowGroup, 1, numRows);
            ReadParquetSharpColumn<int>(rowGroup, 2, numRows);
            ReadParquetSharpColumn<long>(rowGroup, 3, numRows);
        }
        else
        {
            // All byte[] columns for DeltaByteArray and DeltaLengthByteArray
            int numColumns = rowGroup.MetaData.NumColumns;
            for (int c = 0; c < numColumns; c++)
                ReadParquetSharpColumn<byte[]>(rowGroup, c, numRows);
        }
    }

    private static void ReadParquetSharpColumn<T>(ParquetSharp.RowGroupReader rowGroup, int colIndex, long numRows)
    {
        using var col = rowGroup.Column(colIndex);
        using var logical = col.LogicalReader<T>();
        var buffer = new T[numRows];
        logical.ReadBatch(buffer);
    }

    [Benchmark(Description = "Parquet.Net")]
    public async Task ParquetNet_Read()
    {
        using var stream = System.IO.File.OpenRead(FilePath);
#if NET8_0_OR_GREATER
        await using var reader = await ParquetReader.CreateAsync(stream).ConfigureAwait(false);
        using var rowGroupReader = reader.OpenRowGroupReader(0);
        int n = checked((int)reader.RowGroups[0].RowCount);
        foreach (DataField field in reader.Schema.GetDataFields())
        {
            await ParquetNetReadHelpers.DrainColumnV6Async(rowGroupReader, field, n).ConfigureAwait(false);
        }
#else
        using var reader = await ParquetReader.CreateAsync(stream).ConfigureAwait(false);
        using var rowGroupReader = reader.OpenRowGroupReader(0);
        foreach (DataField field in reader.Schema.GetDataFields())
        {
            await rowGroupReader.ReadColumnAsync(field).ConfigureAwait(false);
        }
#endif
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Attributes;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Parquet;

namespace EngineeredWood.Benchmarks;

[MemoryDiagnoser]
public class MetadataReadBenchmarks
{
    private string _dir = null!;
    private Dictionary<string, string> _files = null!;

    [Params(TestFileGenerator.WideFlat, TestFileGenerator.TallNarrow, TestFileGenerator.Snappy)]
    public string File { get; set; } = null!;

    private string FilePath => _files[File];

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dir = TestFileGenerator.GenerateAll();
        _files = new Dictionary<string, string>
        {
            [TestFileGenerator.WideFlat] = Path.Combine(_dir, TestFileGenerator.WideFlat + ".parquet"),
            [TestFileGenerator.TallNarrow] = Path.Combine(_dir, TestFileGenerator.TallNarrow + ".parquet"),
            [TestFileGenerator.Snappy] = Path.Combine(_dir, TestFileGenerator.Snappy + ".parquet"),
        };
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        TestFileGenerator.Cleanup(_dir);
    }

    [Benchmark(Description = "EngineeredWood")]
    public async Task EngineeredWood_ReadMetadata()
    {
        using var file = new LocalRandomAccessFile(FilePath);
        using var reader = new ParquetFileReader(file);
        await reader.ReadMetadataAsync().ConfigureAwait(false);
        await reader.GetSchemaAsync().ConfigureAwait(false);
    }

    [Benchmark(Description = "ParquetSharp")]
    public void ParquetSharp_ReadMetadata()
    {
        using var reader = new ParquetSharp.ParquetFileReader(FilePath);
        _ = reader.FileMetaData;
    }

    [Benchmark(Description = "Parquet.Net")]
    public async Task ParquetNet_ReadMetadata()
    {
        using var stream = System.IO.File.OpenRead(FilePath);
#if NET8_0_OR_GREATER
        await using var reader = await ParquetReader.CreateAsync(stream).ConfigureAwait(false);
#else
        using var reader = await ParquetReader.CreateAsync(stream).ConfigureAwait(false);
#endif
        _ = reader.Schema;
    }
}

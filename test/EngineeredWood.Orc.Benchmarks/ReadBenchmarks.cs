// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Attributes;
using EngineeredWood.Orc;

namespace EngineeredWood.Orc.Benchmarks;

[MemoryDiagnoser]
public class ReadBenchmarks
{
    private string _demo12ZlibPath = null!;
    private string _demo11NonePath = null!;

    private static string GetTestFilePath(string fileName)
    {
        // Walk up from bin directory to find test data
        var dir = AppContext.BaseDirectory;
        while (dir != null)
        {
            var testData = Path.Combine(dir, "test", "EngineeredWood.Orc.Tests", "TestData", fileName);
            if (File.Exists(testData)) return testData;
            dir = Path.GetDirectoryName(dir);
        }
        throw new FileNotFoundException($"Test file not found: {fileName}");
    }

    [GlobalSetup]
    public void Setup()
    {
        _demo12ZlibPath = GetTestFilePath("demo-12-zlib.orc");
        _demo11NonePath = GetTestFilePath("demo-11-none.orc");
    }

    [Benchmark]
    public async Task<long> ReadAllRows_Demo12Zlib()
    {
        await using var reader = await OrcReader.OpenAsync(_demo12ZlibPath);
        var rowReader = reader.CreateRowReader(new OrcReaderOptions { BatchSize = 4096 });
        long total = 0;
        await foreach (var batch in rowReader)
        {
            total += batch.Length;
        }
        return total;
    }

    [Benchmark]
    public async Task<long> ReadAllRows_Demo11None()
    {
        await using var reader = await OrcReader.OpenAsync(_demo11NonePath);
        var rowReader = reader.CreateRowReader(new OrcReaderOptions { BatchSize = 4096 });
        long total = 0;
        await foreach (var batch in rowReader)
        {
            total += batch.Length;
        }
        return total;
    }

    [Benchmark]
    public async Task<long> ReadSingleColumn_Demo12Zlib()
    {
        await using var reader = await OrcReader.OpenAsync(_demo12ZlibPath);
        var rowReader = reader.CreateRowReader(new OrcReaderOptions
        {
            Columns = ["_col0"],
            BatchSize = 4096
        });
        long total = 0;
        await foreach (var batch in rowReader)
        {
            total += batch.Length;
        }
        return total;
    }

    [Benchmark]
    public async Task OpenAndReadMetadata()
    {
        await using var reader = await OrcReader.OpenAsync(_demo12ZlibPath);
        _ = reader.NumberOfRows;
        _ = reader.Schema;
    }
}

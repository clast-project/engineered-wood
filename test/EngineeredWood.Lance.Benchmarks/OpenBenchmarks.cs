// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Attributes;

namespace EngineeredWood.Lance.Benchmarks;

/// <summary>
/// Measures the cost of opening a Lance file: footer parse, GBO/CMO
/// table reads, FileDescriptor decode, schema construction. Does not
/// touch any column data.
/// </summary>
[MemoryDiagnoser]
public class OpenBenchmarks
{
    private string _v20PrimitiveSmall = null!;
    private string _v21PrimitiveSmall = null!;
    private string _v20Nested = null!;
    private string _v21Embeddings = null!;

    [GlobalSetup]
    public void Setup()
    {
        _v20PrimitiveSmall = TestDataLocator.Resolve("int32_nonull.lance");
        _v21PrimitiveSmall = TestDataLocator.Resolve("int32_v21.lance");
        _v20Nested = TestDataLocator.Resolve("list_struct.lance");
        _v21Embeddings = TestDataLocator.Resolve("embeddings_v21.lance");
    }

    [Benchmark]
    public async Task Open_V20_Primitive()
    {
        await using var reader = await LanceFileReader.OpenAsync(_v20PrimitiveSmall);
        _ = reader.Schema;
    }

    [Benchmark]
    public async Task Open_V21_Primitive()
    {
        await using var reader = await LanceFileReader.OpenAsync(_v21PrimitiveSmall);
        _ = reader.Schema;
    }

    [Benchmark]
    public async Task Open_V20_Nested_3Columns()
    {
        await using var reader = await LanceFileReader.OpenAsync(_v20Nested);
        _ = reader.Schema;
    }

    [Benchmark]
    public async Task Open_V21_Embeddings()
    {
        await using var reader = await LanceFileReader.OpenAsync(_v21Embeddings);
        _ = reader.Schema;
    }
}

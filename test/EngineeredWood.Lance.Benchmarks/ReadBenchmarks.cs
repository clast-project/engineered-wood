// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Attributes;

namespace EngineeredWood.Lance.Benchmarks;

/// <summary>
/// End-to-end column read benchmarks across the realistic encoding matrix.
/// Each method opens the file and reads a single top-level Arrow field
/// (which for v2.0 nested types pulls multiple physical columns).
/// Sample sizes are deliberately small so the comparison reflects the
/// per-row overhead of each encoding rather than IO time.
/// </summary>
[MemoryDiagnoser]
public class ReadBenchmarks
{
    // Per-file paths cached at GlobalSetup so the open path doesn't dominate.
    private string _v20Int32 = null!;
    private string _v20Int32Nulls = null!;
    private string _v20String = null!;
    private string _v20StringNulls = null!;
    private string _v20Dict = null!;
    private string _v20ListInt = null!;
    private string _v20Struct = null!;

    private string _v21Int32 = null!;
    private string _v21Int32Nulls = null!;
    private string _v21String = null!;
    private string _v21ListInt = null!;
    private string _v21LargeFlat = null!;
    private string _v21InlineBp = null!;
    private string _v21Embeddings = null!;
    private string _v21BigFsl = null!;

    [GlobalSetup]
    public void Setup()
    {
        _v20Int32 = TestDataLocator.Resolve("int32_nonull.lance");
        _v20Int32Nulls = TestDataLocator.Resolve("int32_nulls.lance");
        _v20String = TestDataLocator.Resolve("string_nonull.lance");
        _v20StringNulls = TestDataLocator.Resolve("string_nulls.lance");
        _v20Dict = TestDataLocator.Resolve("repetitive_strings.lance");
        _v20ListInt = TestDataLocator.Resolve("list_int.lance");
        _v20Struct = TestDataLocator.Resolve("struct_2i32.lance");

        _v21Int32 = TestDataLocator.Resolve("int32_v21.lance");
        _v21Int32Nulls = TestDataLocator.Resolve("int32_nulls_v21.lance");
        _v21String = TestDataLocator.Resolve("string_v21.lance");
        _v21ListInt = TestDataLocator.Resolve("list_int_v21.lance");
        _v21LargeFlat = TestDataLocator.Resolve("large_int32_v21.lance");
        _v21InlineBp = TestDataLocator.Resolve("inline_bp_int32_v21.lance");
        _v21Embeddings = TestDataLocator.Resolve("embeddings_v21.lance");
        _v21BigFsl = TestDataLocator.Resolve("big_fsl_v21.lance");
    }

    // ---- v2.0 ----

    [Benchmark] public Task Read_V20_Int32_NoNulls() => ReadColumnZero(_v20Int32);
    [Benchmark] public Task Read_V20_Int32_Nulls() => ReadColumnZero(_v20Int32Nulls);
    [Benchmark] public Task Read_V20_String() => ReadColumnZero(_v20String);
    [Benchmark] public Task Read_V20_String_Nulls() => ReadColumnZero(_v20StringNulls);
    [Benchmark] public Task Read_V20_Dictionary_Strings() => ReadColumnZero(_v20Dict);
    [Benchmark] public Task Read_V20_List_Int32() => ReadColumnZero(_v20ListInt);
    [Benchmark] public Task Read_V20_Struct_TwoInt32() => ReadColumnZero(_v20Struct);

    // ---- v2.1 ----

    [Benchmark] public Task Read_V21_Int32_NoNulls() => ReadColumnZero(_v21Int32);
    [Benchmark] public Task Read_V21_Int32_Nulls() => ReadColumnZero(_v21Int32Nulls);
    [Benchmark] public Task Read_V21_String() => ReadColumnZero(_v21String);
    [Benchmark] public Task Read_V21_List_Int32() => ReadColumnZero(_v21ListInt);
    [Benchmark] public Task Read_V21_Int32_LargeFlat_2000() => ReadColumnZero(_v21LargeFlat);
    [Benchmark] public Task Read_V21_Int32_InlineBitpacking_2000() => ReadColumnZero(_v21InlineBp);
    [Benchmark] public Task Read_V21_FullZip_Embeddings_10x1024() => ReadColumnZero(_v21Embeddings);
    [Benchmark] public Task Read_V21_FullZip_BigFsl_5x4096() => ReadColumnZero(_v21BigFsl);

    private static async Task ReadColumnZero(string path)
    {
        await using var reader = await LanceFileReader.OpenAsync(path);
        _ = await reader.ReadColumnAsync(0);
    }
}

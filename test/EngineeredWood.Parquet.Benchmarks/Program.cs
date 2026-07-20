// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Running;
using EngineeredWood.Benchmarks;

if (args.Length > 0 && args[0].Equals("cloud", StringComparison.OrdinalIgnoreCase))
{
    await CloudBenchmark.RunAsync();
    return;
}

BenchmarkSwitcher.FromTypes([
    typeof(MetadataReadBenchmarks),
    typeof(RowGroupReadBenchmarks),
    typeof(RowGroupWriteBenchmarks),
    typeof(DefaultWriteBenchmarks),
    typeof(DeltaBinaryPackedBenchmarks),
    typeof(DeltaByteArrayBenchmarks),
    typeof(ByteStreamSplitBenchmarks),
    typeof(EncodingReadBenchmarks),
    typeof(PrimitivesBenchmarks),
]).Run(args);

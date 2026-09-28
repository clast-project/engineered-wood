// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Running;
using EngineeredWood.Benchmarks;

if (args.Length > 0 && args[0].Equals("cloud", StringComparison.OrdinalIgnoreCase))
{
    await CloudBenchmark.RunAsync();
    return;
}

#if NET8_0_OR_GREATER
if (args.Length > 0 && args[0] is "pageindex-overhead" or "pageindex-child" or "pageindex-ab")
{
    Environment.ExitCode = await PageIndexOverhead.RunAsync(args);
    return;
}

if (args.Length > 0 && args[0] == "pagemap-ab")
{
    Environment.ExitCode = await PageMapReadAb.RunAsync(args);
    return;
}

if (args.Length > 0 && args[0] == "pageindex-worth")
{
    Environment.ExitCode = await PageIndexWorth.RunAsync(args);
    return;
}

if (args.Length > 0 && args[0] == "rowcap-ab")
{
    Environment.ExitCode = await RowCountLimitAb.RunAsync(args);
    return;
}

if (args.Length > 0 && args[0] == "dictpruning-ab")
{
    Environment.ExitCode = await DictionaryPruningAb.RunAsync(args);
    return;
}
#endif

BenchmarkSwitcher.FromTypes([
    typeof(MetadataReadBenchmarks),
    typeof(RowGroupReadBenchmarks),
    typeof(RowGroupWriteBenchmarks),
    typeof(PageIndexWriteBenchmarks),
    typeof(DefaultWriteBenchmarks),
    typeof(DeltaBinaryPackedBenchmarks),
    typeof(DeltaByteArrayBenchmarks),
    typeof(ByteStreamSplitBenchmarks),
    typeof(AlpBenchmarks),
    typeof(EncodingReadBenchmarks),
    typeof(PrimitivesBenchmarks),
    typeof(FixedListReadBenchmarks),
    typeof(FixedListFallbackBenchmarks),
    typeof(FixedListDetectorBenchmarks),
    typeof(BatchedRunsReadBenchmarks),
]).Run(args);

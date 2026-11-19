// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Running;

#if NET8_0_OR_GREATER
if (args.Length > 0 && args[0] == "delta-stats")
    return await EngineeredWood.DeltaLake.Benchmarks.DeltaStatsProfile.RunAsync(args[1..]);

if (args.Length > 0 && args[0] == "rg652")
    return await EngineeredWood.DeltaLake.Benchmarks.Rg652.RowGroupSizeSweep.RunAsync(args[1..]);
#endif

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
return 0;

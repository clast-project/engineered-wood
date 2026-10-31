// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Running;
using DeltaDotnetComparison;

if (args.Length > 0 && args[0] == "--verify")
    return await Verify.RunAsync(args.Skip(1).ToArray());

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
return 0;

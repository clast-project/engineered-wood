// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Running;
using DeltaDotnetComparison;

// Which store the tables live on (EWDD_STORAGE; see BenchStorage). Printed so a result is never mistaken for
// the other store's, and resolved up front so a missing credential fails here rather than inside a benchmark.
try
{
    Console.WriteLine($"// storage: {BenchStorage.Current.Description}");
}
catch (TypeInitializationException e) when (e.InnerException is InvalidOperationException config)
{
    Console.Error.WriteLine($"Storage configuration error: {config.Message}");
    return 2;
}

if (args.Length > 0 && args[0] == "--verify")
    return await Verify.RunAsync(args.Skip(1).ToArray());

if (args.Length > 0 && args[0] == "--rg878-upload")
    return await ForeignRowGroups.UploadAsync(args.Skip(1).ToArray());

if (args.Length > 0 && args[0] == "--rg878")
    return await ForeignRowGroups.RunAsync(args.Skip(1).ToArray());

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
return 0;

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;

namespace DeltaDotnetComparison;

/// <summary>
/// Shared shape for every comparison: one category per operation, EngineeredWood the baseline within it, so
/// the Ratio column reads "delta-dotnet time / EW time".
///
/// <para>The memory columns count MANAGED allocations only. delta-dotnet does its work in native memory
/// (Rust heap), which BenchmarkDotNet cannot see, so its Allocated column is NOT comparable to EW's.</para>
/// </summary>
public abstract class ComparisonConfig : ManualConfig
{
    protected ComparisonConfig(Job job)
    {
        AddJob(job);
        AddDiagnoser(MemoryDiagnoser.Default);
        AddColumn(CategoriesColumn.Default);
        AddLogicalGroupRules(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams);
        WithOrderer(new BenchmarkDotNet.Order.DefaultOrderer(
            BenchmarkDotNet.Order.SummaryOrderPolicy.Declared));
        HideColumns(Column.Error, Column.StdDev);
        AddColumn(StatisticColumn.Min, StatisticColumn.StdDev);
    }
}

/// <summary>Read-only operations against a cached template: repeatable, so ordinary pilot-driven iterations.</summary>
public sealed class ReadConfig : ComparisonConfig
{
    public ReadConfig()
        : base(Job.Default.WithLaunchCount(1).WithWarmupCount(3).WithIterationCount(15))
    {
    }
}

/// <summary>
/// Operations that change a table: each invocation gets a fresh copy (<c>[IterationSetup]</c>), so exactly one
/// invocation per iteration, measured cold-ish (Monitoring strategy).
/// </summary>
public sealed class SingleShotConfig : ComparisonConfig
{
    public SingleShotConfig()
        : base(Job.Default
            .WithStrategy(RunStrategy.Monitoring)
            .WithLaunchCount(1)
            .WithInvocationCount(1)
            .WithUnrollFactor(1)
            .WithWarmupCount(2)
            .WithIterationCount(10))
    {
    }
}

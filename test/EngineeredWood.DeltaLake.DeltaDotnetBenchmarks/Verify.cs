// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using System.Reflection;
using BenchmarkDotNet.Attributes;

namespace DeltaDotnetComparison;

/// <summary>
/// <c>--verify [class-name-filter]</c>: runs every benchmark method ONCE per parameter combination, outside
/// BenchmarkDotNet, and checks that the two engines did the same work, so the timings compare like with like.
///
/// <list type="bullet">
/// <item>Read benchmarks: every method in a category must return the same row count (the prune-only EW
/// method is exempt: it returns a superset by design).</item>
/// <item>Mutation benchmarks: after each method the table it left behind is read back by BOTH engines, and
/// all of those fingerprints (row count, id sum, amount sum) must agree across the category.</item>
/// </list>
/// The single wall-clock time printed beside each call is a cold, one-shot number for orientation only.
/// </summary>
internal static class Verify
{
    public static async Task<int> RunAsync(string[] args)
    {
        string? filter = args.FirstOrDefault();
        var types = typeof(Verify).Assembly.GetTypes()
            .Where(t => !t.IsAbstract && t.GetMethods().Any(m => m.GetCustomAttribute<BenchmarkAttribute>() != null))
            .Where(t => filter is null || t.Name.Contains(filter, StringComparison.OrdinalIgnoreCase))
            .OrderBy(t => t.Name)
            .ToList();

        int failures = 0;
        foreach (var type in types)
        {
            foreach (var combination in Combinations(type))
            {
                var instance = Activator.CreateInstance(type)!;
                foreach (var (property, value) in combination)
                    property.SetValue(instance, value);
                string label = $"{type.Name}({string.Join(", ", combination.Select(c => $"{c.Property.Name}={c.Value}"))})";
                Console.WriteLine($"== {label}");

                Invoke(instance, type.GetMethods().Where(m => m.GetCustomAttribute<GlobalSetupAttribute>() != null));

                var byCategory = new Dictionary<string, List<(string Method, string Result)>>();
                foreach (var method in type.GetMethods().Where(m => m.GetCustomAttribute<BenchmarkAttribute>() != null))
                {
                    string category = method.GetCustomAttribute<BenchmarkCategoryAttribute>()?.Categories[0] ?? "";
                    InvokeIterationSetup(instance, type, method.Name);
                    string result;
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        long value = await (Task<long>)method.Invoke(instance, null)!;
                        sw.Stop();
                        if (instance is MutationBenchmark mutation)
                        {
                            var byEw = await Ew.FingerprintAsync(mutation.CurrentPath!);
                            var byDd = await Dd.FingerprintAsync(mutation.CurrentPath!);
                            var byDf = await Dd.FingerprintViaQueryAsync(mutation.CurrentPath!);
                            if (byEw.Equals(byDd) && byEw.Equals(byDf))
                            {
                                result = byEw.ToString();
                            }
                            else if (byEw.Equals(byDf))
                            {
                                // Two independent readers agree; only delta-dotnet's kernel scan differs. Known
                                // case: its ReadParquetAsArrow drops the kernel's selection vector, so deletion
                                // vectors are ignored and deleted rows come back. A finding, not a harness fault.
                                result = byEw.ToString();
                                Console.WriteLine($"   WARN {method.Name}: delta-dotnet ReadAsArrowTableAsync read {byDd} " +
                                    "(EW and delta-rs DataFusion agree on the value below)");
                            }
                            else
                            {
                                result = $"READERS DISAGREE: EW {byEw} / DD kernel {byDd} / DD DataFusion {byDf}";
                            }
                        }
                        else
                        {
                            result = $"rows={value}";
                        }
                    }
                    catch (Exception ex)
                    {
                        sw.Stop();
                        var inner = ex is TargetInvocationException { InnerException: { } e } ? e : ex;
                        result = $"THREW {inner.GetType().Name}: {inner.Message}";
                    }
                    finally
                    {
                        if (instance is MutationBenchmark mutation)
                            mutation.Cleanup();
                    }

                    Console.WriteLine($"   {method.Name,-18} {sw.Elapsed.TotalMilliseconds,9:F1} ms  {result}");
                    if (!(method.Name == nameof(FilteredScanBenchmarks.EW_PruneOnly)))
                        (byCategory.TryGetValue(category, out var list) ? list : byCategory[category] = []).Add((method.Name, result));
                }

                foreach (var (category, results) in byCategory)
                {
                    bool threw = results.Any(r => r.Result.StartsWith("THREW") || r.Result.StartsWith("READERS"));
                    if (threw || results.Select(r => r.Result).Distinct().Count() > 1)
                    {
                        failures++;
                        Console.WriteLine($"   !! MISMATCH in {category}: {string.Join(" | ", results.Select(r => $"{r.Method}: {r.Result}"))}");
                    }
                }
            }
        }

        Console.WriteLine(failures == 0 ? "VERIFY OK" : $"VERIFY FAILED: {failures} categories");
        return failures == 0 ? 0 : 1;
    }

    private static IEnumerable<List<(PropertyInfo Property, object Value)>> Combinations(Type type)
    {
        IEnumerable<List<(PropertyInfo, object)>> result = [[]];
        foreach (var property in type.GetProperties().Where(p => p.GetCustomAttribute<ParamsAllValuesAttribute>() != null))
        {
            object[] values = property.PropertyType == typeof(bool)
                ? [false, true]
                : Enum.GetValues(property.PropertyType).Cast<object>().ToArray();
            result = result.SelectMany(prefix => values.Select(v => prefix.Append((property, v)).ToList())).ToList();
        }
        return result;
    }

    private static void InvokeIterationSetup(object instance, Type type, string target)
    {
        var setups = type.GetMethods()
            .Select(m => (Method: m, Attr: m.GetCustomAttribute<IterationSetupAttribute>()))
            .Where(s => s.Attr != null)
            .Where(s => s.Attr!.Targets is not { Length: > 0 } targets || targets.Contains(target))
            .Select(s => s.Method);
        Invoke(instance, setups);
    }

    private static void Invoke(object instance, IEnumerable<MethodInfo> methods)
    {
        foreach (var method in methods)
            method.Invoke(instance, null);
    }
}

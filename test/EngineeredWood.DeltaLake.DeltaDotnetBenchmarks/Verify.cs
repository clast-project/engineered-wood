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
        if (types.Count == 0)
        {
            // A mistyped filter must not report a successful verification of nothing.
            Console.Error.WriteLine($"VERIFY FAILED: no benchmark class matches '{filter}'.");
            return 2;
        }

        // Only the classes with a method EWDD_VERIFY_METHODS selects: the others would build and sweep their
        // fixtures for nothing. None at all is a mistyped filter, refused before any fixture is built.
        types = types
            .Where(t => t.GetMethods().Any(m =>
                m.GetCustomAttribute<BenchmarkAttribute>() != null && TraceMarks.Selected(m.Name)))
            .ToList();
        if (types.Count == 0)
        {
            Console.Error.WriteLine(
                $"VERIFY FAILED: no benchmark method of the classes matching '{filter}' matches EWDD_VERIFY_METHODS.");
            return 2;
        }

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
                    if (!TraceMarks.Selected(method.Name))
                        continue;
                    InvokeIterationSetup(instance, type, method.Name);
                    string result;
                    await TraceMarks.BeginAsync($"{label}.{method.Name}");
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        long value;
                        try
                        {
                            value = await (Task<long>)method.Invoke(instance, null)!;
                        }
                        finally
                        {
                            sw.Stop();
                            await TraceMarks.EndAsync();
                        }
                        if (instance is MutationBenchmark mutation)
                        {
                            var byEw = await Ew.FingerprintAsync(mutation.CurrentPath!);
                            var byDd = await Dd.FingerprintAsync(mutation.CurrentPath!);
                            var byDf = await Dd.FingerprintViaQueryAsync(mutation.CurrentPath!);
                            if (byEw.Equals(byDd) && byEw.Equals(byDf))
                            {
                                result = byEw.ToString();
                            }
                            else if (byEw.Equals(byDf) && instance is DeletionVectorDmlBenchmarks)
                            {
                                // The ONE known exception, and only on the benchmarks that write deletion vectors:
                                // delta-dotnet's ReadParquetAsArrow drops the kernel's selection vector, so
                                // DV-deleted rows come back (delta-incubator/delta-dotnet#234). Two independent
                                // readers agree here. Any other kernel disagreement is a failure.
                                result = byEw.ToString();
                                Console.WriteLine($"   WARN {method.Name}: delta-dotnet ReadAsArrowTableAsync read {byDd} " +
                                    "(EW and delta-rs DataFusion agree on the value below)");
                            }
                            else
                            {
                                result = $"READERS DISAGREE: EW {byEw} / DD kernel {byDd} / DD DataFusion {byDf}";
                            }

                            // Checkpoint, compaction, vacuum, ADD CONSTRAINT and this RESTORE (of a compaction) all
                            // preserve the rows, so a no-op would pass the fingerprint. Check that each did its job too.
                            if (instance is MaintenanceBenchmarks or ConstraintBenchmarks or RestoreBenchmarks)
                                result += " " + await MaintenancePostconditionAsync(category, mutation.CurrentPath!, method.Name);

                            // Each deletion-vector category has one method and no delta-dotnet twin, so nothing
                            // above compares it with anything. Check that it wrote a vector, and that it left the
                            // rows its copy-on-write counterpart leaves.
                            // Pruning shows only in what a MERGE read, not in the table it leaves (either way
                            // just the one matching file is rewritten), so print what each engine reports.
                            if (instance is MergeKeyRangeBenchmarks)
                                Console.WriteLine($"   ({method.Name}: {await LatestOperationMetricsAsync(mutation.CurrentPath!)})");

                            if (instance is DeletionVectorDmlBenchmarks)
                                result += " " + await DeletionVectorPostconditionAsync(instance, method, mutation.CurrentPath!, byEw);
                        }
                        else
                        {
                            result = $"rows={value}";
                        }
                    }
                    catch (Exception ex)
                    {
                        var inner = ex is TargetInvocationException { InnerException: { } e } ? e : ex;
                        result = $"THREW {inner.GetType().Name}: {inner.Message}";
                    }
                    finally
                    {
                        if (instance is MutationBenchmark mutation)
                            mutation.Cleanup();
                    }

                    Console.WriteLine($"   {method.Name,-18} {sw.Elapsed.TotalMilliseconds,9:F1} ms  {result}");
                    if (method.Name == nameof(FilteredScanBenchmarks.EW_PruneOnly))
                    {
                        // Its row count is a superset by design, so it is exempt from the comparison, but not
                        // from failing.
                        if (IsFailure(result))
                        {
                            failures++;
                            Console.WriteLine($"   !! FAILED {method.Name}: {result}");
                        }
                        continue;
                    }
                    (byCategory.TryGetValue(category, out var list) ? list : byCategory[category] = []).Add((method.Name, result));
                }

                foreach (var (category, results) in byCategory)
                {
                    bool threw = results.Any(r => IsFailure(r.Result));
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

    /// <summary>
    /// What a maintenance operation must have changed, as a string the category comparison checks for
    /// equality across engines. A failure carries <see cref="PostconditionFailed"/>, which <see cref="IsFailure"/>
    /// finds ANYWHERE in a result (it follows the fingerprint), so the category fails even when every engine
    /// fails the same way and the results are therefore equal.
    /// </summary>
    private const string PostconditionFailed = "POSTCONDITION FAILED";

    /// <summary>
    /// A result that fails on its own, whatever the other engines returned. Markers are searched for anywhere,
    /// not only at the start: a postcondition failure is appended after the fingerprint.
    /// </summary>
    private static bool IsFailure(string result) =>
        result.StartsWith("THREW", StringComparison.Ordinal)
        || result.Contains("READERS DISAGREE", StringComparison.Ordinal)
        || result.Contains(PostconditionFailed, StringComparison.Ordinal);

    /// <summary>The latest commit's <c>commitInfo.operationMetrics</c> that count files, as each engine names them.</summary>
    private static async Task<string> LatestOperationMetricsAsync(string path)
    {
        var fs = Ew.Fs(path);
        string latest = (await ListAsync(fs, "_delta_log/"))
            .Where(f => f.EndsWith(".json", StringComparison.Ordinal) && !f["_delta_log/".Length..].Contains('/'))
            .OrderBy(f => f, StringComparer.Ordinal).Last();
        string text = System.Text.Encoding.UTF8.GetString(await fs.ReadAllBytesAsync(latest));
        foreach (string line in text.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            using var json = System.Text.Json.JsonDocument.Parse(line);
            if (!json.RootElement.TryGetProperty("commitInfo", out var info)
                || !info.TryGetProperty("operationMetrics", out var metrics))
            {
                continue;
            }
            return string.Join(", ", metrics.EnumerateObject()
                .Where(m => m.Name.Contains("File", StringComparison.OrdinalIgnoreCase)
                    || m.Name.Contains("_files", StringComparison.OrdinalIgnoreCase))
                .Select(m => $"{m.Name}={m.Value}"));
        }
        return "no operationMetrics";
    }

    /// <summary>
    /// A deletion-vector DML must have left an active file with a deletion vector, and the same rows as the same
    /// method run on <see cref="Fixtures.SingleCommit"/> by EW, a table without deletion vectors, where it
    /// rewrites the file instead.
    /// </summary>
    private static async Task<string> DeletionVectorPostconditionAsync(
        object instance, MethodInfo method, string path, Fingerprint actual)
    {
        int withVector;
        await using (var table = await EngineeredWood.DeltaLake.Table.DeltaTable.OpenAsync(Ew.Fs(path)))
            withVector = table.CurrentSnapshot.ActiveFiles.Values.Count(f => f.DeletionVector is not null);
        if (withVector == 0)
            return $"{PostconditionFailed}: NO deletion vector written (the file was rewritten)";

        var benchmark = (DeletionVectorDmlBenchmarks)instance;
        string copyOnWrite = Fixtures.FreshCopy(Fixtures.SingleCommit(TableWriter.EngineeredWood));
        string dvTable = benchmark.CurrentPath!;
        try
        {
            benchmark.CurrentPath = copyOnWrite;
            await (Task<long>)method.Invoke(benchmark, null)!;
            var expected = await Ew.FingerprintAsync(copyOnWrite);
            return expected.Equals(actual)
                ? $"deletion vectors on {withVector} file(s), rows as copy-on-write"
                : $"{PostconditionFailed}: rows differ from copy-on-write ({expected})";
        }
        finally
        {
            benchmark.CurrentPath = dvTable;
            Fixtures.Delete(copyOnWrite);
        }
    }

    private static async Task<string> MaintenancePostconditionAsync(string category, string path, string method)
    {
        await using var table = await EngineeredWood.DeltaLake.Table.DeltaTable.OpenAsync(Ew.Fs(path));
        var snapshot = table.CurrentSnapshot;
        var fs = Ew.Fs(path);
        const string log = "_delta_log/";
        switch (category)
        {
            case "Checkpoint":
            {
                // A checkpoint at the latest version, published through _last_checkpoint.
                string hint = log + "_last_checkpoint";
                if (!await fs.ExistsAsync(hint))
                    return $"{PostconditionFailed}: NOT checkpointed (no _last_checkpoint)";
                using var json = System.Text.Json.JsonDocument.Parse(await fs.ReadAllBytesAsync(hint));
                long version = json.RootElement.GetProperty("version").GetInt64();
                bool fileExists = (await ListAsync(fs, $"{log}{version:D20}.checkpoint")).Count > 0;
                return version == snapshot.Version && fileExists
                    ? $"checkpoint@v{version}"
                    : $"{PostconditionFailed}: NOT checkpointed at the latest version (hint v{version}, latest v{snapshot.Version}, file {(fileExists ? "present" : "missing")})";
            }
            case "Compact":
            {
                // Fewer active files than the 100 small ones. The exact count is engine-specific (target sizes
                // differ), so it is printed but not compared.
                int active = snapshot.FileCount;
                Console.WriteLine($"   ({method}: {active} active files after compaction)");
                return active < Fixtures.Commits
                    ? "compacted"
                    : $"{PostconditionFailed}: NOT compacted ({active} active files)";
            }
            case "Vacuum":
            {
                // Every data file on disk is referenced by the snapshot: the removed small files are gone.
                // Compared as relative, '/'-separated paths, the form ITableFileSystem lists them in on either store.
                var referenced = new HashSet<string>(
                    snapshot.ActiveFiles.Keys.Select(p => Uri.UnescapeDataString(p).Replace('\\', '/').TrimStart('/')),
                    StringComparer.OrdinalIgnoreCase);
                int unreferenced = (await ListAsync(fs, ""))
                    .Where(f => f.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase)
                        && !f.StartsWith(log, StringComparison.OrdinalIgnoreCase))
                    .Count(f => !referenced.Contains(f));
                return unreferenced == 0
                    ? "vacuumed (0 unreferenced data files)"
                    : $"{PostconditionFailed}: NOT vacuumed ({unreferenced} unreferenced data files remain)";
            }
            case "AddConstraint":
            {
                // The constraint is in the table's metadata, under the name and expression it was added with.
                string key = "delta.constraints." + Sql.ConstraintName;
                return snapshot.Metadata.Configuration is { } configuration
                    && configuration.TryGetValue(key, out string? expression)
                    && expression == Sql.ConstraintExpression
                    ? $"constraint {Sql.ConstraintName} added"
                    : $"{PostconditionFailed}: NO constraint {key} = '{Sql.ConstraintExpression}' in the metadata";
            }
            case "Restore":
            {
                // The 100 small files the compaction removed are active again, and nothing else is.
                int active = snapshot.FileCount;
                return active == Fixtures.Commits
                    ? $"restored ({active} active files)"
                    : $"{PostconditionFailed}: NOT restored ({active} active files, expected {Fixtures.Commits})";
            }
            default:
                return "";
        }
    }

    /// <summary>The relative, '/'-separated paths of every file under <paramref name="prefix"/>.</summary>
    private static async Task<List<string>> ListAsync(EngineeredWood.IO.ITableFileSystem fs, string prefix)
    {
        var paths = new List<string>();
        await foreach (var file in fs.ListAsync(prefix))
            paths.Add(file.Path.Replace('\\', '/'));
        return paths;
    }

    private static IEnumerable<List<(PropertyInfo Property, object Value)>> Combinations(Type type)
    {
        IEnumerable<List<(PropertyInfo, object)>> result = [[]];
        foreach (var property in type.GetProperties())
        {
            object[] values;
            if (property.GetCustomAttribute<ParamsAttribute>() is { } explicitValues)
                values = explicitValues.Values;
            else if (property.GetCustomAttribute<ParamsAllValuesAttribute>() is not null)
                values = property.PropertyType == typeof(bool)
                    ? [false, true]
                    : Enum.GetValues(property.PropertyType).Cast<object>().ToArray();
            else
                continue;
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

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

namespace EngineeredWood.Expressions.Tests;

/// <summary>
/// Loads <c>Fixtures/spark-expression-corpus.json</c>, the checked-in record of what Spark says
/// about the expressions the parser has to handle.
/// </summary>
/// <remarks>
/// Harvested by <c>test/EngineeredWood.DeltaLake.Table.Tests/Interop/harvest_expression_corpus.py</c>
/// and checked in so nothing here needs Spark, a JVM, or a network.
/// </remarks>
internal static class SparkCorpus
{
    private static readonly Lazy<JsonDocument> Loaded = new(() =>
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", "spark-expression-corpus.json");
        Assert.True(File.Exists(path),
            $"corpus fixture not found at {path}; check the csproj copies Fixtures/**");
        return JsonDocument.Parse(File.ReadAllText(path));
    });

    public static JsonElement Root => Loaded.Value.RootElement;

    public static JsonElement Group(string name) => Root.GetProperty("groups").GetProperty(name);

    /// <summary>Every entry, across every group.</summary>
    public static IEnumerable<JsonElement> Entries()
    {
        foreach (var group in Root.GetProperty("groups").EnumerateObject())
            foreach (var entry in group.Value.EnumerateArray())
                yield return entry;
    }

    /// <summary>Every expression Spark itself accepted, which is what the parser must also read.</summary>
    public static IEnumerable<string> ParsableExpressions()
    {
        foreach (var entry in Entries())
        {
            if (entry.GetProperty("parse").GetProperty("ok").GetBoolean())
                yield return entry.GetProperty("expression").GetString()!;
        }
    }

    public static string Expression(this JsonElement entry) =>
        entry.GetProperty("expression").GetString()!;
}

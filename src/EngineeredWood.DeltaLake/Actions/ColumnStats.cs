// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

namespace EngineeredWood.DeltaLake.Actions;

/// <summary>
/// Per-file column-level statistics stored in the <c>stats</c> field
/// of <see cref="AddFile"/> actions.
/// </summary>
public sealed record ColumnStats
{
    /// <summary>Total number of records in the file.</summary>
    public long NumRecords { get; init; }

    /// <summary>Minimum values per column (column name → value).</summary>
    public IReadOnlyDictionary<string, JsonElement>? MinValues { get; init; }

    /// <summary>Maximum values per column (column name → value).</summary>
    public IReadOnlyDictionary<string, JsonElement>? MaxValues { get; init; }

    /// <summary>Null count per column (column name → count).</summary>
    public IReadOnlyDictionary<string, long>? NullCount { get; init; }

    /// <summary>
    /// Parses a Delta stats JSON string (the value of <see cref="AddFile.Stats"/>)
    /// into a <see cref="ColumnStats"/>. Returns <c>null</c> if the input is
    /// null, empty, or doesn't parse.
    /// </summary>
    public static ColumnStats? Parse(string? json)
    {
        if (string.IsNullOrEmpty(json))
            return null;

        try
        {
            using var doc = JsonDocument.Parse(json!);
            var root = doc.RootElement;

            long numRecords = 0;
            if (root.TryGetProperty("numRecords", out var nr)
                && nr.ValueKind == JsonValueKind.Number)
                numRecords = nr.GetInt64();

            return new ColumnStats
            {
                NumRecords = numRecords,
                MinValues = ReadValueMap(root, "minValues"),
                MaxValues = ReadValueMap(root, "maxValues"),
                NullCount = ReadCountMap(root, "nullCount"),
            };
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static IReadOnlyDictionary<string, JsonElement>? ReadValueMap(
        JsonElement root, string property)
    {
        if (!root.TryGetProperty(property, out var element)
            || element.ValueKind != JsonValueKind.Object)
            return null;

        // Nested (struct) stats are JSON objects mirroring the schema; flatten their leaves into
        // dotted keys ("s.a") so evaluators can resolve nested references with the same flat lookup.
        // The top-level object entry is kept as-is for any consumer that inspects it directly.
        var map = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        var collided = new HashSet<string>(StringComparer.Ordinal);
        foreach (var prop in element.EnumerateObject())
        {
            AddValue(map, collided, prop.Name, prop.Value);
            if (prop.Value.ValueKind == JsonValueKind.Object)
                FlattenValues(map, collided, prop.Name, prop.Value);
        }
        // A literal dotted column name colliding with a flattened struct path is ambiguous —
        // drop the key entirely (no stat => no pruning on it; pruning must never guess).
        foreach (var key in collided)
            map.Remove(key);
        return map;
    }

    private static void FlattenValues(
        Dictionary<string, JsonElement> map, HashSet<string> collided, string prefix, JsonElement obj)
    {
        foreach (var prop in obj.EnumerateObject())
        {
            string key = prefix + "." + prop.Name;
            if (prop.Value.ValueKind == JsonValueKind.Object)
                FlattenValues(map, collided, key, prop.Value);
            else
                AddValue(map, collided, key, prop.Value);
        }
    }

    private static void AddValue(
        Dictionary<string, JsonElement> map, HashSet<string> collided, string key, JsonElement value)
    {
        if (map.ContainsKey(key))
            collided.Add(key);
        else
            map[key] = value.Clone();
    }

    private static IReadOnlyDictionary<string, long>? ReadCountMap(
        JsonElement root, string property)
    {
        if (!root.TryGetProperty(property, out var element)
            || element.ValueKind != JsonValueKind.Object)
            return null;

        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        var collided = new HashSet<string>(StringComparer.Ordinal);
        FlattenCounts(map, collided, "", element);
        foreach (var key in collided)
            map.Remove(key);
        return map;
    }

    private static void FlattenCounts(
        Dictionary<string, long> map, HashSet<string> collided, string prefix, JsonElement obj)
    {
        foreach (var prop in obj.EnumerateObject())
        {
            string key = prefix.Length == 0 ? prop.Name : prefix + "." + prop.Name;
            if (prop.Value.ValueKind == JsonValueKind.Number)
            {
                if (map.ContainsKey(key))
                    collided.Add(key);
                else
                    map[key] = prop.Value.GetInt64();
            }
            else if (prop.Value.ValueKind == JsonValueKind.Object)
            {
                // Nested (struct) null counts: an object per struct, numbers at the leaves.
                FlattenCounts(map, collided, key, prop.Value);
            }
        }
    }
}

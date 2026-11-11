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
    /// <summary>
    /// Total number of records in the file, or null when the statistics do not carry one. A missing count
    /// is unknown, never zero: a stats object may hold only bounds, and reading it as an empty file would
    /// mis-assign row ids and let a DELETE treat a file of rows as having none.
    /// </summary>
    public long? NumRecords { get; init; }

    /// <summary>Minimum values per column (column name → value).</summary>
    public IReadOnlyDictionary<string, JsonElement>? MinValues { get; init; }

    /// <summary>Maximum values per column (column name → value).</summary>
    public IReadOnlyDictionary<string, JsonElement>? MaxValues { get; init; }

    /// <summary>Null count per column (column name → count).</summary>
    public IReadOnlyDictionary<string, long>? NullCount { get; init; }

    /// <summary>
    /// Parses a Delta stats JSON string (the value of <see cref="AddFile.Stats"/>)
    /// into a <see cref="ColumnStats"/>. Returns <c>null</c> if the input is
    /// null, empty, or doesn't parse, or isn't a JSON object.
    /// </summary>
    /// <remarks>
    /// Statistics a reader can't use count as absent, never as an error (#820): a root that isn't an object is no
    /// statistics at all, and a row count or null count that isn't a non-negative 64-bit integer (<c>1.5</c>,
    /// <c>1e30</c>, <c>-1</c>, <c>"3"</c>) is unknown. Both used to throw, failing a pruned scan and anything that
    /// asked for a file's row count.
    /// </remarks>
    public static ColumnStats? Parse(string? json)
    {
        if (string.IsNullOrEmpty(json))
            return null;

        try
        {
            using var doc = JsonDocument.Parse(json!);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
                return null;

            long? numRecords = null;
            if (root.TryGetProperty("numRecords", out var nr)
                && nr.ValueKind == JsonValueKind.Number
                && nr.TryGetInt64(out long n)
                && n >= 0)
                numRecords = n;

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
                // A count that isn't a non-negative 64-bit integer is unknown (#820), and makes its key ambiguous: a
                // usable copy of it (the same name twice, or a literal dotted name beside a nested path) is dropped
                // with it, in either order (#825 review).
                if (!prop.Value.TryGetInt64(out long count) || count < 0)
                {
                    collided.Add(key);
                    continue;
                }
                if (map.ContainsKey(key))
                    collided.Add(key);
                else
                    map[key] = count;
            }
            else if (prop.Value.ValueKind == JsonValueKind.Object)
            {
                // Nested (struct) null counts: an object per struct, numbers at the leaves.
                FlattenCounts(map, collided, key, prop.Value);
            }
        }
    }
}

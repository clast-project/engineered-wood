// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics.CodeAnalysis;

namespace EngineeredWood.DeltaLake.Schema;

/// <summary>
/// A field's metadata as read from a Delta <c>schemaString</c>. A JSON string value is held as its text; any other
/// value (number, boolean, array, object, null) is held as its raw JSON text, and its key is remembered so that
/// <see cref="DeltaSchemaSerializer"/> writes it back with the same JSON kind. Without that, a value like Spark's
/// <c>"delta.identity.start": 1</c> could not be told apart from the string <c>"1"</c>.
/// </summary>
/// <remarks>
/// The kinds survive only while the metadata is passed along or copied with <see cref="With"/>. A copy into a plain
/// dictionary, or a round trip through an Arrow field, keeps the text but loses the kinds; the serializer still
/// writes the keys PROTOCOL.md types (identity, type widening, column mapping) with their spec kind.
/// </remarks>
internal sealed class FieldMetadata : IReadOnlyDictionary<string, string>
{
    private readonly Dictionary<string, string> _values;
    private readonly HashSet<string> _jsonKeys;

    private FieldMetadata(Dictionary<string, string> values, HashSet<string> jsonKeys)
    {
        _values = values;
        _jsonKeys = jsonKeys;
    }

    internal FieldMetadata()
        : this(new Dictionary<string, string>(), new HashSet<string>(StringComparer.Ordinal))
    {
    }

    /// <summary>Adds a JSON string value.</summary>
    internal void AddString(string key, string value)
    {
        _values[key] = value;
        _jsonKeys.Remove(key);
    }

    /// <summary>Adds a value that is not a JSON string, as its raw JSON text.</summary>
    internal void AddJson(string key, string rawJson)
    {
        _values[key] = rawJson;
        _jsonKeys.Add(key);
    }

    /// <summary>True when <paramref name="key"/>'s value is raw JSON rather than a JSON string.</summary>
    public bool IsJson(string key) => _jsonKeys.Contains(key);

    /// <summary>
    /// Copies <paramref name="source"/>, keeping its JSON kinds when it is a <see cref="FieldMetadata"/>, and sets
    /// each of <paramref name="updates"/> as a string value. The serializer still gives an update to a key the
    /// spec types (an identity high-water mark, say) its spec kind.
    /// </summary>
    public static FieldMetadata With(
        IReadOnlyDictionary<string, string>? source, params (string Key, string Value)[] updates)
    {
        var result = new FieldMetadata();
        if (source is not null)
        {
            var kinds = source as FieldMetadata;
            foreach (var kvp in source)
            {
                if (kinds is not null && kinds.IsJson(kvp.Key))
                    result.AddJson(kvp.Key, kvp.Value);
                else
                    result.AddString(kvp.Key, kvp.Value);
            }
        }
        foreach (var (key, value) in updates)
            result.AddString(key, value);
        return result;
    }

    public string this[string key] => _values[key];

    public IEnumerable<string> Keys => _values.Keys;

    public IEnumerable<string> Values => _values.Values;

    public int Count => _values.Count;

    public bool ContainsKey(string key) => _values.ContainsKey(key);

    public bool TryGetValue(string key, [MaybeNullWhen(false)] out string value) =>
        _values.TryGetValue(key, out value);

    public IEnumerator<KeyValuePair<string, string>> GetEnumerator() => _values.GetEnumerator();

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
}

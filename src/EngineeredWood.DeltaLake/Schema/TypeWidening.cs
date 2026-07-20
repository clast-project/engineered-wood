// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

namespace EngineeredWood.DeltaLake.Schema;

/// <summary>
/// Type widening support for Delta Lake. Allows changing column types
/// to wider types without rewriting data files.
/// </summary>
public static class TypeWidening
{
    /// <summary>Table property to enable type widening.</summary>
    public const string EnableKey = "delta.enableTypeWidening";

    /// <summary>Schema metadata key for type change history.</summary>
    public const string TypeChangesKey = "delta.typeChanges";

    /// <summary>
    /// Returns true if type widening is enabled for the table.
    /// </summary>
    public static bool IsEnabled(IReadOnlyDictionary<string, string>? configuration) =>
        configuration is not null &&
        configuration.TryGetValue(EnableKey, out string? value) &&
        string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Returns true if widening from <paramref name="fromType"/> to
    /// <paramref name="toType"/> is a supported type change.
    /// </summary>
    public static bool IsSupported(string fromType, string toType) =>
        GetSupportedWidenings().Contains((fromType, toType));

    /// <summary>
    /// Returns all supported type widenings as (fromType, toType) pairs.
    /// </summary>
    private static HashSet<(string From, string To)> GetSupportedWidenings() => new()
    {
        // Integer widening: byte → short → int → long
        ("byte", "short"),
        ("byte", "integer"),
        ("byte", "long"),
        ("short", "integer"),
        ("short", "long"),
        ("integer", "long"),

        // Floating-point widening
        ("float", "double"),
        ("byte", "double"),
        ("short", "double"),
        ("integer", "double"),

        // Date widening
        ("date", "timestamp_ntz"),
    };

    /// <summary>
    /// Returns true if widening from <paramref name="fromType"/> to
    /// <paramref name="toType"/> is a decimal widening.
    /// Decimal widening: Decimal(p,s) → Decimal(p+k1, s+k2) where k1 >= k2 >= 0.
    /// Also: byte/short/int → Decimal(10+k1, k2), long → Decimal(20+k1, k2).
    /// </summary>
    public static bool IsDecimalWidening(string fromType, string toType)
    {
        if (!toType.StartsWith("decimal(", StringComparison.Ordinal))
            return false;

        var (toPrecision, toScale) = ParseDecimal(toType);

        if (fromType.StartsWith("decimal(", StringComparison.Ordinal))
        {
            var (fromPrecision, fromScale) = ParseDecimal(fromType);
            int k1 = toPrecision - fromPrecision;
            int k2 = toScale - fromScale;
            return k1 >= k2 && k2 >= 0;
        }

        // Integer → Decimal widenings
        int basePrecision = fromType switch
        {
            "byte" or "short" or "integer" => 10,
            "long" => 20,
            _ => -1,
        };

        return basePrecision >= 0 && toPrecision >= basePrecision && toScale >= 0;
    }

    /// <summary>
    /// Validates that a type change is supported. Throws if not.
    /// </summary>
    public static void ValidateTypeChange(string fromType, string toType)
    {
        if (!IsSupported(fromType, toType) && !IsDecimalWidening(fromType, toType))
        {
            throw new DeltaLake.DeltaFormatException(
                $"Type widening from '{fromType}' to '{toType}' is not supported.");
        }
    }

    /// <summary>
    /// Gets the type change history from a field's metadata.
    /// </summary>
    public static IReadOnlyList<TypeChange> GetTypeChanges(StructField field)
    {
        if (field.Metadata is null ||
            !field.Metadata.TryGetValue(TypeChangesKey, out string? json))
            return [];

        return ParseTypeChanges(json);
    }

    /// <summary>
    /// Parses the JSON type changes array.
    /// </summary>
    public static IReadOnlyList<TypeChange> ParseTypeChanges(string json)
    {
        var changes = new List<TypeChange>();
        using var doc = JsonDocument.Parse(json);

        foreach (var element in doc.RootElement.EnumerateArray())
        {
            changes.Add(new TypeChange
            {
                FromType = element.GetProperty("fromType").GetString()!,
                ToType = element.GetProperty("toType").GetString()!,
                FieldPath = element.TryGetProperty("fieldPath", out var fp)
                    ? fp.GetString() : null,
            });
        }

        return changes;
    }

    /// <summary>
    /// Serializes type changes to JSON.
    /// </summary>
    public static string SerializeTypeChanges(IReadOnlyList<TypeChange> changes)
    {
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);

        writer.WriteStartArray();
        foreach (var change in changes)
        {
            writer.WriteStartObject();
            writer.WriteString("fromType", change.FromType);
            writer.WriteString("toType", change.ToType);
            if (change.FieldPath is not null)
                writer.WriteString("fieldPath", change.FieldPath);
            writer.WriteEndObject();
        }
        writer.WriteEndArray();

        writer.Flush();
        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }

    /// <summary>
    /// Adds a type change to a field's metadata, returning the updated field.
    /// </summary>
    public static StructField AddTypeChange(
        StructField field, string fromType, string toType, string? fieldPath = null)
    {
        ValidateTypeChange(fromType, toType);

        var existing = GetTypeChanges(field).ToList();
        existing.Add(new TypeChange
        {
            FromType = fromType,
            ToType = toType,
            FieldPath = fieldPath,
        });

        var metadata = new Dictionary<string, string>();
        if (field.Metadata is not null)
            foreach (var kvp in field.Metadata)
                metadata[kvp.Key] = kvp.Value;
        metadata[TypeChangesKey] = SerializeTypeChanges(existing);

        return new StructField
        {
            Name = field.Name,
            Type = field.Type,
            Nullable = field.Nullable,
            Metadata = metadata,
        };
    }

    private static (int Precision, int Scale) ParseDecimal(string typeName)
    {
        // Format: "decimal(p,s)"
        int start = typeName.IndexOf('(') + 1;
        int comma = typeName.IndexOf(',', start);
        int end = typeName.IndexOf(')', comma);

        int precision = int.Parse(typeName[start..comma].Trim());
        int scale = int.Parse(typeName[(comma + 1)..end].Trim());
        return (precision, scale);
    }
}

/// <summary>
/// Records a single type change applied to a column or field.
/// </summary>
public sealed record TypeChange
{
    /// <summary>The type before the change.</summary>
    public required string FromType { get; init; }

    /// <summary>The type after the change.</summary>
    public required string ToType { get; init; }

    /// <summary>
    /// Path from the struct field to the changed element (for nested types).
    /// E.g., "key" for map key, "value" for map value, "element" for array element.
    /// Null for direct field type changes.
    /// </summary>
    public string? FieldPath { get; init; }
}

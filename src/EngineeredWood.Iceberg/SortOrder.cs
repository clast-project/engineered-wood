// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace EngineeredWood.Iceberg;

/// <summary>
/// Defines the default sort order for data written to a table.
/// Each sort order contains an ordered list of fields with direction and null ordering.
/// </summary>
/// <param name="OrderId">The unique identifier for this sort order.</param>
/// <param name="Fields">The ordered list of sort fields.</param>
public sealed record SortOrder(int OrderId, IReadOnlyList<SortField> Fields)
{
    /// <summary>A sort order with no fields, representing unsorted data.</summary>
    public static readonly SortOrder Unsorted = new(0, []);
}

/// <summary>
/// A single field within a sort order, specifying a source column, transform, direction, and null ordering.
/// </summary>
/// <param name="SourceId">The field ID of the source column in the table schema.</param>
/// <param name="Transform">The transform applied to the source column before sorting.</param>
/// <param name="Direction">The sort direction (ascending or descending).</param>
/// <param name="NullOrder">Whether nulls sort before or after non-null values.</param>
public sealed record SortField(int SourceId, Transform Transform, SortDirection Direction, NullOrder NullOrder);

/// <summary>Specifies ascending or descending sort direction.</summary>
[JsonConverter(typeof(SortDirectionConverter))]
public enum SortDirection
{
    /// <summary>Ascending order.</summary>
    Asc,
    /// <summary>Descending order.</summary>
    Desc
}

/// <summary>Specifies whether null values sort before or after non-null values.</summary>
[JsonConverter(typeof(NullOrderConverter))]
public enum NullOrder
{
    /// <summary>Nulls sort before non-null values.</summary>
    NullsFirst,
    /// <summary>Nulls sort after non-null values.</summary>
    NullsLast
}

internal sealed class SortDirectionConverter : JsonConverter<SortDirection>
{
    public override SortDirection Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        reader.GetString() switch
        {
            "asc" => SortDirection.Asc,
            "desc" => SortDirection.Desc,
            var s => throw new JsonException($"Unknown sort direction: {s}")
        };

    public override void Write(Utf8JsonWriter writer, SortDirection value, JsonSerializerOptions options) =>
        writer.WriteStringValue(value switch
        {
            SortDirection.Asc => "asc",
            SortDirection.Desc => "desc",
            _ => throw new JsonException($"Unknown sort direction: {value}")
        });
}

internal sealed class NullOrderConverter : JsonConverter<NullOrder>
{
    public override NullOrder Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        reader.GetString() switch
        {
            "nulls-first" => NullOrder.NullsFirst,
            "nulls-last" => NullOrder.NullsLast,
            var s => throw new JsonException($"Unknown null order: {s}")
        };

    public override void Write(Utf8JsonWriter writer, NullOrder value, JsonSerializerOptions options) =>
        writer.WriteStringValue(value switch
        {
            NullOrder.NullsFirst => "nulls-first",
            NullOrder.NullsLast => "nulls-last",
            _ => throw new JsonException($"Unknown null order: {value}")
        });
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace EngineeredWood.Iceberg;

/// <summary>
/// A stats-only field from a v3 table's <c>stats-fields</c> list: a field id, assigned from
/// <c>last-column-id</c>, under which manifests keep statistics for a derived value (a partition value or an
/// expression) rather than for a column. This library does not compute such statistics; it keeps the
/// definitions through a metadata rewrite. Readers must not fail on a <see cref="Type"/> they do not know.
/// </summary>
/// <param name="FieldId">The field id the statistics are stored under.</param>
/// <param name="Type">The kind of derived value: <c>partition-value</c> or <c>expr-value</c>, or one added later.</param>
/// <param name="DataType">The type of the bounds (a primitive or variant), if given.</param>
/// <param name="PartitionFieldId">For <c>partition-value</c>, the partition field the value comes from.</param>
/// <param name="Expr">For <c>expr-value</c>, the expression, kept as JSON.</param>
public sealed record StatsField(
    int FieldId,
    string Type,
    IcebergType? DataType = null,
    int? PartitionFieldId = null,
    JsonElement? Expr = null)
{
    /// <summary>Properties this library does not model, kept so that a rewrite does not drop them.</summary>
    [JsonExtensionData, JsonInclude]
    public IDictionary<string, JsonElement>? AdditionalProperties { get; internal set; }
}

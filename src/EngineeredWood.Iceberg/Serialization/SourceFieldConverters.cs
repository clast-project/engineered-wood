// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace EngineeredWood.Iceberg.Serialization;

/// <summary>
/// Reads and writes a field's source column(s). v3 lets a partition or sort field name its sources with
/// <c>source-ids</c> (a multi-argument transform) instead of <c>source-id</c>; a reader must accept either.
/// A writer emits <c>source-id</c> for a single-argument transform and <c>source-ids</c> only for a
/// multi-argument one.
/// </summary>
internal static class SourceIds
{
    public static (int SourceId, IReadOnlyList<int>? SourceIds) Read(JsonElement obj, string what)
    {
        int? sourceId = obj.TryGetProperty("source-id", out var single) && single.ValueKind != JsonValueKind.Null
            ? single.GetInt32()
            : null;

        List<int>? ids = null;
        if (obj.TryGetProperty("source-ids", out var many) && many.ValueKind != JsonValueKind.Null)
        {
            ids = [];
            foreach (var id in many.EnumerateArray())
                ids.Add(id.GetInt32());
            if (ids.Count == 0)
                throw new JsonException($"A {what} has an empty source-ids list.");
        }

        if (sourceId is null && ids is null)
            throw new JsonException($"A {what} has neither source-id nor source-ids.");

        return (sourceId ?? ids![0], ids);
    }

    public static void Write(Utf8JsonWriter writer, int sourceId, IReadOnlyList<int>? ids)
    {
        if (ids is null)
        {
            writer.WriteNumber("source-id", sourceId);
            return;
        }

        writer.WritePropertyName("source-ids");
        writer.WriteStartArray();
        foreach (var id in ids)
            writer.WriteNumberValue(id);
        writer.WriteEndArray();
    }
}

internal sealed class PartitionFieldConverter : JsonConverter<PartitionField>
{
    private static readonly HashSet<string> Known = ["source-id", "source-ids", "field-id", "name", "transform"];

    public override PartitionField Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);
        var root = doc.RootElement;
        var (sourceId, sourceIds) = SourceIds.Read(root, "partition field");
        return new PartitionField(
            sourceId,
            root.GetProperty("field-id").GetInt32(),
            root.GetProperty("name").GetString()!,
            TransformConverter.Parse(root.GetProperty("transform").GetString()),
            sourceIds)
        {
            AdditionalProperties = UnknownProperties.Collect(root, Known),
        };
    }

    public override void Write(Utf8JsonWriter writer, PartitionField value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        SourceIds.Write(writer, value.SourceId, value.SourceIds);
        writer.WriteNumber("field-id", value.FieldId);
        writer.WriteString("name", value.Name);
        writer.WriteString("transform", TransformConverter.Format(value.Transform));
        UnknownProperties.Write(writer, value.AdditionalProperties, Known);
        writer.WriteEndObject();
    }
}

internal sealed class SortFieldConverter : JsonConverter<SortField>
{
    private static readonly HashSet<string> Known = ["source-id", "source-ids", "transform", "direction", "null-order"];

    public override SortField Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);
        var root = doc.RootElement;
        var (sourceId, sourceIds) = SourceIds.Read(root, "sort field");
        return new SortField(
            sourceId,
            TransformConverter.Parse(root.GetProperty("transform").GetString()),
            root.GetProperty("direction").Deserialize(options.TypeInfo<SortDirection>()),
            root.GetProperty("null-order").Deserialize(options.TypeInfo<NullOrder>()),
            sourceIds)
        {
            AdditionalProperties = UnknownProperties.Collect(root, Known),
        };
    }

    public override void Write(Utf8JsonWriter writer, SortField value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString("transform", TransformConverter.Format(value.Transform));
        SourceIds.Write(writer, value.SourceId, value.SourceIds);
        writer.WritePropertyName("direction");
        JsonSerializer.Serialize(writer, value.Direction, options.TypeInfo<SortDirection>());
        writer.WritePropertyName("null-order");
        JsonSerializer.Serialize(writer, value.NullOrder, options.TypeInfo<NullOrder>());
        UnknownProperties.Write(writer, value.AdditionalProperties, Known);
        writer.WriteEndObject();
    }
}

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
        return ReadField(doc.RootElement, fallbackFieldId: null);
    }

    /// <summary>
    /// Reads one partition field. A v1 field may have no <c>field-id</c>; <paramref name="fallbackFieldId"/>
    /// is the id its spec assigns it (see <see cref="PartitionSpecConverter"/>), and without one the field
    /// cannot be read.
    /// </summary>
    internal static PartitionField ReadField(JsonElement root, int? fallbackFieldId)
    {
        var (sourceId, sourceIds) = SourceIds.Read(root, "partition field");
        int fieldId = root.TryGetProperty("field-id", out var idEl) && idEl.ValueKind != JsonValueKind.Null
            ? idEl.GetInt32()
            : fallbackFieldId ?? throw new JsonException("A partition field has no field-id.");
        return new PartitionField(
            sourceId,
            fieldId,
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

/// <summary>
/// Reads and writes a partition spec. v1 lets a spec's fields leave out <c>field-id</c>: then every field
/// does, and they are numbered from 1000 in order, as Iceberg Java's <c>PartitionSpecParser</c> numbers
/// them. A spec with some ids but not all is refused, as Java refuses it.
/// </summary>
internal sealed class PartitionSpecConverter : JsonConverter<PartitionSpec>
{
    /// <summary>The first id a v1 spec's id-less partition fields get.</summary>
    internal const int FirstPartitionFieldId = 1000;

    public override PartitionSpec Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);
        var root = doc.RootElement;
        int specId = root.TryGetProperty("spec-id", out var idEl) ? idEl.GetInt32() : 0;

        var elements = root.GetProperty("fields").EnumerateArray().ToList();
        int withIds = elements.Count(e => e.TryGetProperty("field-id", out var f) && f.ValueKind != JsonValueKind.Null);
        if (withIds != 0 && withIds != elements.Count)
            throw new JsonException($"Partition spec {specId} has field-id on some fields but not all.");

        var fields = new List<PartitionField>(elements.Count);
        for (int i = 0; i < elements.Count; i++)
            fields.Add(PartitionFieldConverter.ReadField(elements[i], withIds == 0 ? FirstPartitionFieldId + i : null));
        return new PartitionSpec(specId, fields);
    }

    public override void Write(Utf8JsonWriter writer, PartitionSpec value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteNumber("spec-id", value.SpecId);
        writer.WritePropertyName("fields");
        writer.WriteStartArray();
        foreach (var field in value.Fields)
            JsonSerializer.Serialize(writer, field, options.TypeInfo<PartitionField>());
        writer.WriteEndArray();
        writer.WriteEndObject();
    }
}

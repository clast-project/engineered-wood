// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg.Manifest;

/// <summary>The type of a partition spec's tuple: each field's result type, from its transform and source.</summary>
internal static class PartitionTypes
{
    /// <summary>
    /// The partition type of <paramref name="spec"/>, resolving each source field in <paramref name="schemas"/>
    /// (searched in order). Returns false when a field's type cannot be known: an unknown or multi-argument
    /// transform, or a source field no schema has.
    /// </summary>
    public static bool TryResolve(
        PartitionSpec spec, IEnumerable<Schema> schemas, out IReadOnlyList<PartitionValueType> type, out string? reason)
    {
        var schemaList = schemas as IReadOnlyList<Schema> ?? schemas.ToList();
        var fields = new List<PartitionValueType>(spec.Fields.Count);
        foreach (var field in spec.Fields)
        {
            if (field.SourceIds is { Count: > 1 } || field.Transform is UnknownTransform)
            {
                type = [];
                reason = $"partition field '{field.Name}' uses transform '{TransformName(field.Transform)}', which this library does not know";
                return false;
            }

            IcebergType? source = null;
            foreach (var schema in schemaList)
            {
                source = FindField(schema.Fields, field.SourceId)?.Type;
                if (source is not null)
                    break;
            }
            if (source is null)
            {
                type = [];
                reason = $"partition field '{field.Name}' has source field {field.SourceId}, which no schema has";
                return false;
            }

            fields.Add(new PartitionValueType(field.FieldId, field.Name, ResultType(field.Transform, source)));
        }

        type = fields;
        reason = null;
        return true;
    }

    /// <summary>The partition type of <paramref name="spec"/> in <paramref name="table"/>, current schema first.</summary>
    public static IReadOnlyList<PartitionValueType> Resolve(PartitionSpec spec, TableMetadata table)
    {
        var schemas = table.Schemas.Where(s => s.SchemaId == table.CurrentSchemaId)
            .Concat(table.Schemas.Where(s => s.SchemaId != table.CurrentSchemaId).Reverse());
        if (!TryResolve(spec, schemas, out var type, out var reason))
            throw new NotSupportedException($"Cannot write manifests for partition spec {spec.SpecId}: {reason}.");
        return type;
    }

    /// <summary>A single-argument transform's result type for a source of type <paramref name="source"/>.</summary>
    public static IcebergType ResultType(Transform transform, IcebergType source) => transform switch
    {
        IdentityTransform or TruncateTransform => source,
        // A void field is always null, and the spec lets its type be the source's or int. Use int where the
        // source type has no manifest encoding (variant, geometry, geography, unknown).
        VoidTransform => source is VariantType or GeometryType or GeographyType or UnknownType or NestedType
            ? IcebergType.Int
            : source,
        BucketTransform or YearTransform or MonthTransform or HourTransform => IcebergType.Int,
        DayTransform => IcebergType.Date,
        _ => throw new NotSupportedException($"Transform '{TransformName(transform)}' has no known result type."),
    };

    private static string TransformName(Transform t) =>
        t is UnknownTransform u ? u.Name : t.GetType().Name;

    internal static NestedField? FindField(IReadOnlyList<NestedField> fields, int id)
    {
        foreach (var f in fields)
        {
            if (f.Id == id)
                return f;
            if (f.Type is StructType s && FindField(s.Fields, id) is { } nested)
                return nested;
        }
        return null;
    }
}

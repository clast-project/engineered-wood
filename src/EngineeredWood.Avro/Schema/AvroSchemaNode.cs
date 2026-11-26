// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

namespace EngineeredWood.Avro.Schema;

/// <summary>
/// Avro schema type tags per the Avro specification.
/// </summary>
internal enum AvroType
{
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Record,
    Enum,
    Array,
    Map,
    Fixed,
    Union,
}

/// <summary>
/// Base class for the internal Avro schema tree. Each node represents a type in the schema.
/// </summary>
internal abstract class AvroSchemaNode
{
    public abstract AvroType Type { get; }

    /// <summary>Optional logical type annotation (e.g. "decimal", "date", "timestamp-millis").</summary>
    public string? LogicalType { get; init; }

    /// <summary>
    /// Attributes the Avro specification does not define for this type, such as Iceberg's
    /// <c>element-id</c>, <c>key-id</c>/<c>value-id</c> and <c>adjust-to-utc</c>. Avro allows any extra
    /// attribute; they are kept so that writing the schema back does not drop them.
    /// </summary>
    public IReadOnlyDictionary<string, JsonElement> Properties { get; init; } = AvroSchemaProperties.Empty;
}

internal static class AvroSchemaProperties
{
    public static readonly IReadOnlyDictionary<string, JsonElement> Empty =
        new Dictionary<string, JsonElement>(0, StringComparer.Ordinal);

    /// <summary>The value of an integer attribute (such as an Iceberg id), or null when absent or not an int.</summary>
    public static int? GetInt32(this IReadOnlyDictionary<string, JsonElement> properties, string name) =>
        properties.TryGetValue(name, out var value) && value.ValueKind == JsonValueKind.Number &&
        value.TryGetInt32(out var id)
            ? id
            : null;
}

internal sealed class AvroPrimitiveSchema : AvroSchemaNode
{
    public override AvroType Type { get; }

    /// <summary>Decimal precision (total digits). Only meaningful when LogicalType is "decimal".</summary>
    public int? Precision { get; init; }

    /// <summary>Decimal scale (digits after decimal point). Only meaningful when LogicalType is "decimal".</summary>
    public int? Scale { get; init; }

    public AvroPrimitiveSchema(AvroType type)
    {
        Type = type;
    }

    // Singletons for the primitive types (no logical type)
    public static readonly AvroPrimitiveSchema Null = new(AvroType.Null);
    public static readonly AvroPrimitiveSchema Boolean = new(AvroType.Boolean);
    public static readonly AvroPrimitiveSchema Int = new(AvroType.Int);
    public static readonly AvroPrimitiveSchema Long = new(AvroType.Long);
    public static readonly AvroPrimitiveSchema Float = new(AvroType.Float);
    public static readonly AvroPrimitiveSchema Double = new(AvroType.Double);
    public static readonly AvroPrimitiveSchema Bytes = new(AvroType.Bytes);
    public static readonly AvroPrimitiveSchema String = new(AvroType.String);
}

internal sealed class AvroRecordSchema : AvroSchemaNode
{
    public override AvroType Type => AvroType.Record;
    public string Name { get; }
    public string? Namespace { get; }
    public string FullName => Namespace != null ? $"{Namespace}.{Name}" : Name;
    public string? Doc { get; init; }
    public IReadOnlyList<string> Aliases { get; init; } = [];
    public IReadOnlyList<AvroFieldNode> Fields { get; }

    public AvroRecordSchema(string name, string? ns, IReadOnlyList<AvroFieldNode> fields)
    {
        Name = name;
        Namespace = ns;
        Fields = fields;
    }
}

internal sealed class AvroFieldNode
{
    public string Name { get; }
    public AvroSchemaNode Schema { get; }
    public IReadOnlyList<string> Aliases { get; init; } = [];
    public string? Doc { get; init; }

    /// <summary>
    /// The default value as a System.Text.Json element, or null if no default.
    /// Avro spec: fields with no default are required.
    /// </summary>
    public System.Text.Json.JsonElement? Default { get; init; }

    /// <summary>
    /// Attributes the Avro specification does not define for a field, such as Iceberg's <c>field-id</c>, and
    /// the sort <c>order</c>. They are kept so that writing the schema back does not drop them.
    /// </summary>
    public IReadOnlyDictionary<string, JsonElement> Properties { get; init; } = AvroSchemaProperties.Empty;

    /// <summary>The Iceberg <c>field-id</c> attribute, or null when the field has none.</summary>
    public int? FieldId => Properties.GetInt32("field-id");

    public AvroFieldNode(string name, AvroSchemaNode schema)
    {
        Name = name;
        Schema = schema;
    }
}

internal sealed class AvroEnumSchema : AvroSchemaNode
{
    public override AvroType Type => AvroType.Enum;
    public string Name { get; }
    public string? Namespace { get; }
    public string FullName => Namespace != null ? $"{Namespace}.{Name}" : Name;
    public IReadOnlyList<string> Symbols { get; }
    public string? Default { get; init; }
    public string? Doc { get; init; }
    public IReadOnlyList<string> Aliases { get; init; } = [];

    public AvroEnumSchema(string name, string? ns, IReadOnlyList<string> symbols)
    {
        Name = name;
        Namespace = ns;
        Symbols = symbols;
    }
}

internal sealed class AvroArraySchema : AvroSchemaNode
{
    public override AvroType Type => AvroType.Array;
    public AvroSchemaNode Items { get; }

    /// <summary>The Iceberg <c>element-id</c> attribute, or null when the array has none.</summary>
    public int? ElementId => Properties.GetInt32("element-id");

    public AvroArraySchema(AvroSchemaNode items)
    {
        Items = items;
    }
}

internal sealed class AvroMapSchema : AvroSchemaNode
{
    public override AvroType Type => AvroType.Map;
    public AvroSchemaNode Values { get; }

    /// <summary>The Iceberg <c>key-id</c> attribute, or null when the map has none.</summary>
    public int? KeyId => Properties.GetInt32("key-id");

    /// <summary>The Iceberg <c>value-id</c> attribute, or null when the map has none.</summary>
    public int? ValueId => Properties.GetInt32("value-id");

    public AvroMapSchema(AvroSchemaNode values)
    {
        Values = values;
    }
}

internal sealed class AvroFixedSchema : AvroSchemaNode
{
    public override AvroType Type => AvroType.Fixed;
    public string Name { get; }
    public string? Namespace { get; }
    public string FullName => Namespace != null ? $"{Namespace}.{Name}" : Name;
    public int Size { get; }
    public IReadOnlyList<string> Aliases { get; init; } = [];

    /// <summary>Decimal precision (total digits). Only meaningful when LogicalType is "decimal".</summary>
    public int? Precision { get; init; }

    /// <summary>Decimal scale (digits after decimal point). Only meaningful when LogicalType is "decimal".</summary>
    public int? Scale { get; init; }

    public AvroFixedSchema(string name, string? ns, int size)
    {
        Name = name;
        Namespace = ns;
        Size = size;
    }
}

internal sealed class AvroUnionSchema : AvroSchemaNode
{
    public override AvroType Type => AvroType.Union;
    public IReadOnlyList<AvroSchemaNode> Branches { get; }

    public AvroUnionSchema(IReadOnlyList<AvroSchemaNode> branches)
    {
        Branches = branches;
    }

    /// <summary>
    /// Returns true if this is a nullable union (exactly 2 branches, one of which is null).
    /// </summary>
    public bool IsNullable(out AvroSchemaNode innerType, out int nullIndex)
    {
        if (Branches.Count == 2)
        {
            if (Branches[0].Type == AvroType.Null)
            {
                innerType = Branches[1];
                nullIndex = 0;
                return true;
            }
            if (Branches[1].Type == AvroType.Null)
            {
                innerType = Branches[0];
                nullIndex = 1;
                return true;
            }
        }
        innerType = AvroPrimitiveSchema.Null;
        nullIndex = -1;
        return false;
    }
}

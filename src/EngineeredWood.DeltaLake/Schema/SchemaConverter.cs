// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Types;
using ArrowMapType = Apache.Arrow.Types.MapType;
using ArrowStructType = Apache.Arrow.Types.StructType;

namespace EngineeredWood.DeltaLake.Schema;

/// <summary>
/// Converts between Delta Lake schema types and Apache Arrow schema types.
/// </summary>
public static class SchemaConverter
{
    /// <summary>
    /// The variant TRANSPORT marker: an Arrow field-metadata extension name discriminating a BINARY
    /// column that carries ONE self-delimiting variant value per row — the parquet-variant metadata
    /// bytes immediately followed by the value bytes (the metadata header sizes itself, so the halves
    /// split without a length prefix). Embedding hosts whose Arrow boundary cannot carry the canonical
    /// <c>arrow.parquet.variant</c> extension (struct storage) exchange variant values in this LEAF-binary
    /// form instead; <see cref="FromArrowSchema"/> accepts it unconditionally (marker-keyed), and
    /// <c>DeltaTableOptions.VariantTransportBlob</c> selects it for the read direction.
    /// </summary>
    public const string VariantTransportExtensionName = "fabricator.variant";

    private const string ArrowExtensionNameKey = "ARROW:extension:name";

    /// <summary>True when the Arrow field carries the variant transport marker
    /// (see <see cref="VariantTransportExtensionName"/>).</summary>
    public static bool IsVariantTransportField(Field field) =>
        field.Metadata is { } md
        && md.TryGetValue(ArrowExtensionNameKey, out var ext)
        && string.Equals(ext, VariantTransportExtensionName, StringComparison.Ordinal);

    private static readonly Regex s_decimalPattern = new(
        @"^decimal\((\d+),(\d+)\)$", RegexOptions.Compiled);

    /// <summary>
    /// Converts a Delta <see cref="StructType"/> to an Arrow <see cref="Apache.Arrow.Schema"/>.
    /// </summary>
    public static Apache.Arrow.Schema ToArrowSchema(StructType deltaSchema)
    {
        var builder = new Apache.Arrow.Schema.Builder();
        foreach (var field in deltaSchema.Fields)
            builder.Field(ToArrowField(field));
        return builder.Build();
    }

    /// <summary>
    /// Converts an Arrow <see cref="Apache.Arrow.Schema"/> to a Delta <see cref="StructType"/>.
    /// </summary>
    public static StructType FromArrowSchema(Apache.Arrow.Schema arrowSchema)
    {
        var fields = new List<StructField>();
        foreach (var field in arrowSchema.FieldsList)
            fields.Add(FromArrowField(field));
        return new StructType { Fields = fields };
    }

    private static Field ToArrowField(StructField field)
    {
        var arrowType = ToArrowType(field.Type);
        // Preserve per-field Delta metadata (comments, column-mapping id/physicalName, invariants) on the
        // Arrow field — the reverse of FromArrowField's preservation, so schemas round-trip losslessly.
        Dictionary<string, string>? meta = null;
        if (field.Metadata is { Count: > 0 } src)
        {
            meta = new Dictionary<string, string>(src.Count);
            foreach (var kvp in src)
                meta[kvp.Key] = kvp.Value;
        }
        return new Field(field.Name, arrowType, field.Nullable, meta);
    }

    /// <summary>
    /// Converts a Delta <see cref="DeltaDataType"/> to an Arrow <see cref="IArrowType"/>.
    /// </summary>
    public static IArrowType ToArrowType(DeltaDataType type) => type switch
    {
        PrimitiveType p => PrimitiveToArrow(p.TypeName),
        StructType s => new ArrowStructType(
            s.Fields.Select(f => ToArrowField(f)).ToList()),
        ArrayType a => new ListType(
            new Field("element", ToArrowType(a.ElementType), a.ContainsNull)),
        MapType m => new ArrowMapType(
            new Field("key", ToArrowType(m.KeyType), false),
            new Field("value", ToArrowType(m.ValueType), m.ValueContainsNull)),
        _ => throw new DeltaLake.DeltaFormatException(
            $"Unknown Delta type: {type.GetType().Name}"),
    };

    private static IArrowType PrimitiveToArrow(string typeName)
    {
        // Check for decimal(p,s) first
        var match = s_decimalPattern.Match(typeName);
        if (match.Success)
        {
            int precision = int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
            int scale = int.Parse(match.Groups[2].Value, CultureInfo.InvariantCulture);
            return new Decimal128Type(precision, scale);
        }

        return typeName switch
        {
            "string" => StringType.Default,
            "long" => Int64Type.Default,
            "integer" => Int32Type.Default,
            "short" => Int16Type.Default,
            "byte" => Int8Type.Default,
            "float" => FloatType.Default,
            "double" => DoubleType.Default,
            "boolean" => BooleanType.Default,
            "binary" => BinaryType.Default,
            "date" => Date32Type.Default,
            "timestamp" => new TimestampType(TimeUnit.Microsecond, (string?)"UTC"),
            "timestamp_ntz" => new TimestampType(TimeUnit.Microsecond, (string?)null),
            // The Delta "variant" type maps to Arrow's arrow.parquet.variant extension over
            // struct<metadata: binary, value: binary>. The parquet layer keys its VARIANT logical-type
            // annotation off this ExtensionType on write, and materialises it (reassembling any
            // shredding) on read when the reader is given a registry that knows the extension —
            // DeltaTableOptions ensures that. Declaring the type here is what makes the
            // `variantType` table feature reachable; see DeltaTable.RequiredSchemaFeatures.
            "variant" => VariantType.Default,
            _ => throw new DeltaLake.DeltaFormatException(
                $"Unknown Delta primitive type: {typeName}"),
        };
    }

    private static StructField FromArrowField(Field field) =>
        new()
        {
            Name = field.Name,
            // The variant TRANSPORT marker (field metadata) wins over the storage type: without it a plain
            // binary stays an ordinary binary — the marker is the only discriminator (the transport is a
            // LEAF binary, so the type alone cannot carry the distinction).
            Type = IsVariantTransportField(field)
                ? new PrimitiveType { TypeName = "variant" }
                : FromArrowType(field.DataType),
            Nullable = field.IsNullable,
            // Preserve per-field metadata (comments, delta.columnMapping.id/physicalName, invariants, ...) —
            // dropping it silently loses column-mapping identities on any Arrow -> Delta round-trip. Writer
            // internals (the parquet codec's "PARQUET:*" keys, e.g. PARQUET:field_id) and Arrow transport
            // markers ("ARROW:extension:*", e.g. the variant transport discriminator) are transport hints,
            // not Delta schema metadata — those are filtered out.
            Metadata = FilterArrowMetadata(field.Metadata),
        };

    private static Dictionary<string, string>? FilterArrowMetadata(IReadOnlyDictionary<string, string>? metadata)
    {
        if (metadata is null || metadata.Count == 0)
            return null;
        Dictionary<string, string>? result = null;
        foreach (var kv in metadata)
        {
            if (kv.Key.StartsWith("PARQUET:", StringComparison.Ordinal))
                continue;
            if (kv.Key.StartsWith("ARROW:extension:", StringComparison.Ordinal))
                continue;
            (result ??= new Dictionary<string, string>())[kv.Key] = kv.Value;
        }
        return result;
    }

    private static DeltaDataType FromArrowType(IArrowType arrowType) => arrowType switch
    {
        // MUST precede the struct arm: VariantType is an ExtensionType (not a StructType), so it
        // would otherwise fall through to the throw — but any future extension over a struct storage
        // type would be silently written as its storage struct, losing the annotation. Match the
        // extension explicitly and reject unknown ones rather than degrading them.
        VariantType => new PrimitiveType { TypeName = "variant" },
        ExtensionType ext => throw new DeltaLake.DeltaFormatException(
            $"Arrow extension type '{ext.Name}' has no Delta equivalent. Only "
            + "'arrow.parquet.variant' is supported; strip the extension to write its storage type."),

        StringType or LargeStringType or StringViewType =>
            new PrimitiveType { TypeName = "string" },
        Int64Type => new PrimitiveType { TypeName = "long" },
        Int32Type => new PrimitiveType { TypeName = "integer" },
        Int16Type => new PrimitiveType { TypeName = "short" },
        Int8Type => new PrimitiveType { TypeName = "byte" },
        FloatType => new PrimitiveType { TypeName = "float" },
        DoubleType => new PrimitiveType { TypeName = "double" },
        BooleanType => new PrimitiveType { TypeName = "boolean" },
        Decimal128Type d => new PrimitiveType
            { TypeName = $"decimal({d.Precision},{d.Scale})" },
        Decimal256Type d => new PrimitiveType
            { TypeName = $"decimal({d.Precision},{d.Scale})" },
        BinaryType or LargeBinaryType or BinaryViewType or FixedSizeBinaryType =>
            new PrimitiveType { TypeName = "binary" },
        Date32Type or Date64Type => new PrimitiveType { TypeName = "date" },
        TimestampType ts when ts.Timezone is not null =>
            new PrimitiveType { TypeName = "timestamp" },
        TimestampType => new PrimitiveType { TypeName = "timestamp_ntz" },

        ArrowStructType s => new StructType
        {
            Fields = s.Fields.Select(f => FromArrowField(f)).ToList(),
        },
        // A variant transport marker on a list/map INNER field would be lost through the type-level
        // conversion below (silently degrading the element to plain binary) — reject the placement
        // instead. Struct-nested variant maps fine (struct children go through FromArrowField, which
        // sees the marker).
        ListType lv when IsVariantTransportField(lv.ValueField) =>
            throw new DeltaLake.DeltaFormatException(
                "variant is not supported as a list element (only top-level or struct-nested columns)."),
        ArrowMapType mv when IsVariantTransportField(mv.KeyField) || IsVariantTransportField(mv.ValueField) =>
            throw new DeltaLake.DeltaFormatException(
                "variant is not supported as a map key/value (only top-level or struct-nested columns)."),
        ListType l => new ArrayType
        {
            ElementType = FromArrowType(l.ValueDataType),
            ContainsNull = l.ValueField.IsNullable,
        },
        ArrowMapType m => new MapType
        {
            KeyType = FromArrowType(m.KeyField.DataType),
            ValueType = FromArrowType(m.ValueField.DataType),
            ValueContainsNull = m.ValueField.IsNullable,
        },

        _ => throw new DeltaLake.DeltaFormatException(
            $"Cannot convert Arrow type {arrowType.Name} to Delta type."),
    };
}

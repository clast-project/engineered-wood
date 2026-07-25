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
            Type = FromArrowType(field.DataType),
            Nullable = field.IsNullable,
            // Preserve per-field metadata (comments, delta.columnMapping.id/physicalName, invariants, ...) —
            // dropping it silently loses column-mapping identities on any Arrow -> Delta round-trip. Writer
            // internals (the parquet codec's "PARQUET:*" keys, e.g. PARQUET:field_id) are transport hints, not
            // Delta schema metadata — those are filtered out.
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
            (result ??= new Dictionary<string, string>())[kv.Key] = kv.Value;
        }
        return result;
    }

    internal const string NanosecondTimestampMessage =
        "Delta timestamps are microsecond precision, so a nanosecond Arrow timestamp cannot be written "
        + "without discarding its sub-microsecond digits. Cast the column to TimeUnit.Microsecond before "
        + "writing, choosing for yourself how it should round.";

    /// <summary>
    /// Throws if any field of <paramref name="schema"/>, at any nesting depth, is a nanosecond Arrow
    /// timestamp. <see cref="FromArrowSchema"/> already rejects those when a schema is converted, which
    /// covers table creation and schema evolution — but a write into an EXISTING table converts nothing,
    /// so the same rule has to be enforced against the incoming batches directly. Without this a
    /// nanosecond column reaches Parquet under a schema advertising microseconds.
    /// </summary>
    internal static void ThrowIfNanosecondTimestamp(Apache.Arrow.Schema schema)
    {
        foreach (var field in schema.FieldsList)
            ThrowIfNanosecondTimestamp(field.DataType, field.Name);
    }

    private static void ThrowIfNanosecondTimestamp(IArrowType type, string path)
    {
        switch (type)
        {
            case TimestampType { Unit: Apache.Arrow.Types.TimeUnit.Nanosecond }:
                throw new DeltaLake.DeltaFormatException(
                    $"Column '{path}': {NanosecondTimestampMessage}");

            case ArrowStructType s:
                foreach (var f in s.Fields)
                    ThrowIfNanosecondTimestamp(f.DataType, path + "." + f.Name);
                break;

            case ListType l:
                ThrowIfNanosecondTimestamp(l.ValueDataType, path + ".element");
                break;

            case ArrowMapType m:
                ThrowIfNanosecondTimestamp(m.KeyField.DataType, path + ".key");
                ThrowIfNanosecondTimestamp(m.ValueField.DataType, path + ".value");
                break;
        }
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

        // MUST precede the timestamp arms below. Delta's timestamp and timestamp_ntz are MICROSECOND
        // precision, and nothing downstream narrows the Arrow unit: the value would reach Parquet as a
        // nanosecond column while the Delta schema advertised microseconds, and its file statistics --
        // ISO-8601 strings that stop at microseconds -- would have to drop the sub-microsecond digits.
        // Converting here instead would silently alter the caller's data, so require an explicit cast.
        TimestampType ts when ts.Unit == Apache.Arrow.Types.TimeUnit.Nanosecond =>
            throw new DeltaLake.DeltaFormatException(NanosecondTimestampMessage),

        TimestampType ts when ts.Timezone is not null =>
            new PrimitiveType { TypeName = "timestamp" },
        TimestampType => new PrimitiveType { TypeName = "timestamp_ntz" },

        ArrowStructType s => new StructType
        {
            Fields = s.Fields.Select(f => FromArrowField(f)).ToList(),
        },
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

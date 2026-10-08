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
            if (precision > MaxDecimalPrecision)
                throw new DeltaLake.DeltaFormatException(DecimalPrecisionMessage(precision, scale));
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

    /// <summary>
    /// True for Arrow timestamp units that cannot be written to a Delta table as-is. Both failures are
    /// silent rather than loud, which is why they are refused rather than converted.
    /// </summary>
    internal static bool IsUnsupportedTimestampUnit(Apache.Arrow.Types.TimeUnit unit) =>
        unit is Apache.Arrow.Types.TimeUnit.Nanosecond or Apache.Arrow.Types.TimeUnit.Second;

    internal static string UnsupportedTimestampUnitMessage(Apache.Arrow.Types.TimeUnit unit) => unit switch
    {
        Apache.Arrow.Types.TimeUnit.Nanosecond =>
            "Delta timestamps are microsecond precision, so a nanosecond Arrow timestamp cannot be written "
            + "without discarding its sub-microsecond digits. Cast the column to TimeUnit.Microsecond "
            + "before writing, choosing for yourself how it should round.",

        // Parquet's TimestampType has only MILLIS/MICROS/NANOS units, so a second-unit column has no
        // faithful encoding: the writer annotates it MICROS and leaves the values untouched, and it reads
        // back a MILLION times too small (1700000000s becomes 1970-01-01T00:28:20Z). Millisecond is fine —
        // MILLIS exists, so those values survive exactly.
        Apache.Arrow.Types.TimeUnit.Second =>
            "Parquet timestamps have no second-precision unit, so a second-unit Arrow timestamp would be "
            + "stored unchanged under a microsecond annotation and read back a million times too small. "
            + "Cast the column to TimeUnit.Microsecond before writing.",

        _ => $"Arrow timestamp unit {unit} cannot be written to a Delta table.",
    };

    private const string FixedSizeBinaryMessage =
        "Arrow FixedSizeBinary could not be converted to Delta binary in this position. Cast the column "
        + "to Binary first.";

    private const string Date64Message =
        "Arrow Date64 could not be converted to Delta date in this position. Cast the column to Date32 first.";

    /// <summary>
    /// Throws if any field of <paramref name="schema"/>, at any nesting depth, has an Arrow type that
    /// cannot be written as-is. <see cref="FromArrowSchema"/> refuses the timestamp units when a schema
    /// is converted, which covers table creation and schema evolution — but a write into an EXISTING
    /// table converts nothing, so the same rule has to be enforced against the incoming batches
    /// directly. Without this a nanosecond column reaches Parquet under a schema advertising
    /// microseconds. The table's write path runs it AFTER converting FixedSizeBinary, Date64 and Decimal32/64 to
    /// their canonical forms (<c>WriteTypeNormalization</c>), so a FixedSizeBinary
    /// or Date64 still here sits in a container the normalizer does not convert, and would otherwise be
    /// written in a form the table does not declare.
    /// </summary>
    /// <param name="convertibleTypesAllowed">True on the codec seam, where a host writer owns the bytes and
    /// nothing was converted: FixedSizeBinary and Date64 are the host's to represent, and only the timestamp
    /// units, which no writer can store faithfully under a Delta timestamp, are refused.</param>
    internal static void ThrowIfUnwritableType(Apache.Arrow.Schema schema, bool convertibleTypesAllowed = false)
    {
        foreach (var field in schema.FieldsList)
            ThrowIfUnwritableType(field.DataType, field.Name, convertibleTypesAllowed);
    }

    private static void ThrowIfUnwritableType(IArrowType type, string path, bool convertibleTypesAllowed)
    {
        switch (type)
        {
            case TimestampType ts when IsUnsupportedTimestampUnit(ts.Unit):
                throw new DeltaLake.DeltaFormatException(
                    $"Column '{path}': {UnsupportedTimestampUnitMessage(ts.Unit)}");

            // By TypeId: every Arrow decimal type DERIVES from FixedSizeBinaryType.
            case FixedSizeBinaryType when type.TypeId == ArrowTypeId.FixedSizedBinary && !convertibleTypesAllowed:
                throw new DeltaLake.DeltaFormatException($"Column '{path}': {FixedSizeBinaryMessage}");

            case Date64Type when !convertibleTypesAllowed:
                throw new DeltaLake.DeltaFormatException($"Column '{path}': {Date64Message}");

            case ArrowStructType s:
                foreach (var f in s.Fields)
                    ThrowIfUnwritableType(f.DataType, path + "." + f.Name, convertibleTypesAllowed);
                break;

            case ListType l:
                ThrowIfUnwritableType(l.ValueDataType, path + ".element", convertibleTypesAllowed);
                break;

            case ArrowMapType m:
                ThrowIfUnwritableType(m.KeyField.DataType, path + ".key", convertibleTypesAllowed);
                ThrowIfUnwritableType(m.ValueField.DataType, path + ".value", convertibleTypesAllowed);
                break;
        }
    }

    /// <summary>The largest decimal precision Delta allows (PROTOCOL.md, Primitive Types).</summary>
    private const int MaxDecimalPrecision = 38;

    private static string DecimalPrecisionMessage(int precision, int scale) =>
        $"decimal({precision},{scale}) exceeds Delta's maximum decimal precision of {MaxDecimalPrecision}.";

    private static PrimitiveType Decimal(int precision, int scale) =>
        precision <= MaxDecimalPrecision
            ? new PrimitiveType { TypeName = $"decimal({precision},{scale})" }
            : throw new DeltaLake.DeltaFormatException(DecimalPrecisionMessage(precision, scale));

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
        // Delta has ONE decimal type; every Arrow width maps to it. A write normalizes the narrow ones
        // to Decimal128 (the table layer's WriteTypeNormalization) so the files of one column agree.
        Decimal32Type d => Decimal(d.Precision, d.Scale),
        Decimal64Type d => Decimal(d.Precision, d.Scale),
        Decimal128Type d => Decimal(d.Precision, d.Scale),
        Decimal256Type d => Decimal(d.Precision, d.Scale),
        BinaryType or LargeBinaryType or BinaryViewType =>
            new PrimitiveType { TypeName = "binary" },
        // By TypeId: every Arrow decimal type DERIVES from FixedSizeBinaryType. A write converts it to
        // Binary (WriteTypeNormalization) — lossless — so the column reads back as declared.
        FixedSizeBinaryType when arrowType.TypeId == ArrowTypeId.FixedSizedBinary =>
            new PrimitiveType { TypeName = "binary" },
        // Date64 is milliseconds the Arrow format requires to be whole days, so it converts to Date32
        // exactly on write; a value that is not a whole day is refused there.
        Date32Type or Date64Type => new PrimitiveType { TypeName = "date" },


        // MUST precede the timestamp arms below. Nothing downstream narrows the Arrow unit, so a unit
        // Delta or Parquet cannot represent would be written as-is under a microsecond annotation.
        // Converting here instead would silently alter the caller's data, so require an explicit cast.
        TimestampType ts when IsUnsupportedTimestampUnit(ts.Unit) =>
            throw new DeltaLake.DeltaFormatException(UnsupportedTimestampUnitMessage(ts.Unit)),

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

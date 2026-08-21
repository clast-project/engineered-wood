// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Converts Parquet column descriptors to Apache Arrow fields and types.
/// </summary>
internal static class ArrowSchemaConverter
{
    /// <summary>
    /// Converts a Parquet <see cref="ColumnDescriptor"/> to an Arrow <see cref="Apache.Arrow.Field"/>.
    /// </summary>
    public static Apache.Arrow.Field ToArrowField(ColumnDescriptor column, ParquetReadOptions? options = null)
    {
        bool nullable = column.MaxDefinitionLevel > 0;
        var arrowType = ToArrowType(column, options);
        arrowType = ApplyOutputKind(arrowType, options?.ByteArrayOutput ?? ByteArrayOutputKind.Default);
        return new Apache.Arrow.Field(column.DottedPath, arrowType, nullable);
    }

    /// <summary>
    /// Converts the top-level children of a schema root into Arrow fields,
    /// recursing into group nodes to produce nested <see cref="StructType"/> fields.
    /// </summary>
    public static Apache.Arrow.Field[] ToArrowFields(SchemaNode root, ParquetReadOptions? options = null)
    {
        var fields = new Apache.Arrow.Field[root.Children.Count];
        for (int i = 0; i < root.Children.Count; i++)
            fields[i] = NodeToArrowField(root.Children[i], options);
        return fields;
    }

    /// <summary>
    /// Remaps <see cref="StringType"/>/<see cref="BinaryType"/> to the requested output kind.
    /// All other types are returned unchanged.
    /// </summary>
    private static IArrowType ApplyOutputKind(IArrowType type, ByteArrayOutputKind kind) =>
        kind switch
        {
            ByteArrayOutputKind.ViewType => type switch
            {
                Apache.Arrow.Types.StringType => StringViewType.Default,
                BinaryType => BinaryViewType.Default,
                _ => type,
            },
            ByteArrayOutputKind.LargeOffsets => type switch
            {
                Apache.Arrow.Types.StringType => LargeStringType.Default,
                BinaryType => LargeBinaryType.Default,
                _ => type,
            },
            _ => type,
        };

    private static Apache.Arrow.Field NodeToArrowField(SchemaNode node, ParquetReadOptions? options = null)
    {
        bool nullable = node.Element.RepetitionType == FieldRepetitionType.Optional;

        if (node.IsLeaf)
        {
            // Bare repeated primitive: repeated leaf at top level → list of that type
            if (node.Element.RepetitionType == FieldRepetitionType.Repeated)
            {
                var elementType = LeafToArrowType(node, options);
                var elementField = new Apache.Arrow.Field("element", elementType, nullable: false);
                return new Apache.Arrow.Field(node.Name, new ListType(elementField), nullable: false);
            }

            var arrowType = LeafToArrowType(node, options);
            return new Apache.Arrow.Field(node.Name, arrowType, nullable);
        }

        if (IsListNode(node))
            return BuildListField(node, options);
        if (IsMapNode(node))
            return BuildMapField(node, options);

        // Group node → StructType
        var childFields = new Apache.Arrow.Field[node.Children.Count];
        for (int i = 0; i < node.Children.Count; i++)
            childFields[i] = NodeToArrowField(node.Children[i], options);

        var structType = new StructType(childFields);
        IArrowType groupType = MaybeWrapAsExtensionStruct(structType, node.Element.LogicalType, options);
        return new Apache.Arrow.Field(node.Name, groupType, nullable);
    }

    /// <summary>
    /// When a group has a logical-type annotation that maps to an Arrow
    /// extension over <see cref="StructType"/> (currently <c>VARIANT → arrow.parquet.variant</c>),
    /// and the caller has registered that extension via
    /// <see cref="ParquetReadOptions.ExtensionRegistry"/>, return the wrapped
    /// extension type. Otherwise return the bare struct.
    /// </summary>
    private static IArrowType MaybeWrapAsExtensionStruct(
        StructType storage, LogicalType? logicalType, ParquetReadOptions? options)
    {
        if (options?.ExtensionRegistry is { } reg
            && logicalType is LogicalType.VariantType
            && reg.TryGetDefinition("arrow.parquet.variant", out var def)
            && def.TryCreateType(storage, metadata: string.Empty, out var ext))
        {
            return ext;
        }
        return storage;
    }

    internal static bool IsListNode(SchemaNode node) =>
        !node.IsLeaf &&
        (node.Element.LogicalType is LogicalType.ListType ||
         node.Element.ConvertedType == ConvertedType.List ||
         // A MAP whose key_value group is REAL but not a key-and-value pair reads as a LIST of that same
         // group. Without a repeated group there is nothing to make a list out of either, so that case is
         // deliberately not swept in here — it falls through to the struct path below.
         (IsMapAnnotated(node) && HasRepeatedGroup(node) && !HasKeyAndValue(node)));

    internal static bool IsMapNode(SchemaNode node) =>
        IsMapAnnotated(node) && HasRepeatedGroup(node) && HasKeyAndValue(node);

    private static bool IsMapAnnotated(SchemaNode node) =>
        !node.IsLeaf &&
        (node.Element.LogicalType is LogicalType.MapType ||
         node.Element.ConvertedType == ConvertedType.Map ||
         node.Element.ConvertedType == ConvertedType.MapKeyValue);

    /// <summary>
    /// Whether a MAP-annotated node has the one repeated <c>key_value</c> group the annotation promises.
    /// </summary>
    /// <remarks>
    /// Both the map and the list assembler read repetition and definition thresholds off this group, so a
    /// node without it can be read as neither — the thresholds would describe a nesting level the file does
    /// not have, and the offsets and validity computed from them would be wrong rather than merely absent.
    /// Such a node is left to the struct path, which is what the group literally is.
    /// </remarks>
    private static bool HasRepeatedGroup(SchemaNode node) =>
        node.Children.Count == 1 &&
        !node.Children[0].IsLeaf &&
        node.Children[0].Element.RepetitionType == FieldRepetitionType.Repeated;

    /// <summary>
    /// Whether the <c>key_value</c> group carries exactly a key and a value, which is what Arrow's
    /// <see cref="MapType"/> needs and more than the Parquet spec requires. Call only after
    /// <see cref="HasRepeatedGroup"/>.
    /// </summary>
    /// <remarks>
    /// The spec makes <c>value</c> optional, but <see cref="MapType"/> requires it — so a <c>key_value</c>
    /// with only a key cannot be built as a map without inventing a column the file does not contain.
    /// Reading it as a LIST of the repeated group invents nothing, and is what arrow-cpp does: PyArrow reads
    /// parquet-testing's <c>map_no_value.parquet</c> as <c>list&lt;key: int32 not null&gt;</c>, and we now
    /// agree with it field for field.
    /// <para>
    /// The same applies above two children, where the list rules make the group itself a struct element —
    /// better than the map path, which silently dropped every child past the second.
    /// </para>
    /// <para>
    /// This used to be unguarded, and both halves of the read path independently invented a
    /// <c>value: string</c> field while only one of them invented an array to go with it. The resulting
    /// <see cref="StructArray"/> declared two children and held one, so the map threw
    /// <c>IndexOutOfRangeException</c> the moment anything reached for its values (issue #156).
    /// </para>
    /// </remarks>
    private static bool HasKeyAndValue(SchemaNode node) => node.Children[0].Children.Count == 2;

    private static Apache.Arrow.Field BuildListField(SchemaNode node, ParquetReadOptions? options = null)
    {
        bool nullable = node.Element.RepetitionType == FieldRepetitionType.Optional;

        // Determine element schema:
        // 3-level standard: node → repeated group (list/bag/array) → element child
        // 2-level legacy: node → repeated leaf (the leaf is the element)
        var repeatedChild = node.Children[0]; // always has exactly one repeated child

        Apache.Arrow.Field elementField;
        if (repeatedChild.IsLeaf)
        {
            // 2-level: repeated leaf is the element
            var elementType = LeafToArrowType(repeatedChild, options);
            elementField = new Apache.Arrow.Field(repeatedChild.Name, elementType, nullable: false);
        }
        else if (repeatedChild.Children.Count == 1)
        {
            // 3-level standard: recurse into single element child
            elementField = NodeToArrowField(repeatedChild.Children[0], options);
        }
        else
        {
            // 3-level with multiple children in repeated group → treat repeated group as struct element
            var childFields = new Apache.Arrow.Field[repeatedChild.Children.Count];
            for (int i = 0; i < repeatedChild.Children.Count; i++)
                childFields[i] = NodeToArrowField(repeatedChild.Children[i], options);
            var structType = new StructType(childFields);
            elementField = new Apache.Arrow.Field(repeatedChild.Name, structType, nullable: false);
        }

        return new Apache.Arrow.Field(node.Name, new ListType(elementField), nullable);
    }

    private static Apache.Arrow.Field BuildMapField(SchemaNode node, ParquetReadOptions? options = null)
    {
        bool nullable = node.Element.RepetitionType == FieldRepetitionType.Optional;

        // Standard map: node → repeated key_value → key + value
        var keyValueGroup = node.Children[0];
        var keyField = NodeToArrowField(keyValueGroup.Children[0], options);
        // Key field must be non-nullable per Arrow spec
        keyField = new Apache.Arrow.Field(keyField.Name, keyField.DataType, nullable: false);

        // IsMapNode has already established that key_value carries exactly a key and a value; a group that
        // does not is classified as a list and never arrives here.
        var valueField = NodeToArrowField(keyValueGroup.Children[1], options);

        var mapType = new MapType(keyField, valueField);
        return new Apache.Arrow.Field(node.Name, mapType, nullable);
    }

    private static IArrowType LeafToArrowType(SchemaNode node, ParquetReadOptions? options = null)
    {
        var element = node.Element;
        var kind = options?.ByteArrayOutput ?? ByteArrayOutputKind.Default;

        // LogicalType → ConvertedType → PhysicalType fallthrough
        if (element.LogicalType != null)
        {
            // Build a temporary descriptor to reuse existing logic
            var desc = BuildTempDescriptor(node);
            var result = FromLogicalType(element.LogicalType, desc, options);
            if (result != null)
                return ApplyOutputKind(result, kind);
        }

        if (element.ConvertedType.HasValue)
        {
            var desc = BuildTempDescriptor(node);
            var result = FromConvertedType(element.ConvertedType.Value, desc, options);
            if (result != null)
                return ApplyOutputKind(result, kind);
        }

        var physical = FromPhysicalType(BuildTempDescriptor(node));
        return ApplyOutputKind(physical, kind);
    }

    private static ColumnDescriptor BuildTempDescriptor(SchemaNode node) => new()
    {
        Path = [node.Name],
        PhysicalType = node.Element.Type!.Value,
        TypeLength = node.Element.TypeLength,
        MaxDefinitionLevel = 0,
        MaxRepetitionLevel = 0,
        SchemaElement = node.Element,
        SchemaNode = node,
    };

    /// <summary>
    /// Converts a Parquet column's type information to an Arrow <see cref="IArrowType"/>.
    /// Falls through: LogicalType → ConvertedType → PhysicalType.
    /// </summary>
    public static IArrowType ToArrowType(ColumnDescriptor column, ParquetReadOptions? options = null)
    {
        var element = column.SchemaElement;

        // First: check LogicalType
        if (element.LogicalType != null)
        {
            var arrowType = FromLogicalType(element.LogicalType, column, options);
            if (arrowType != null)
                return arrowType;
        }

        // Second: check ConvertedType
        if (element.ConvertedType.HasValue)
        {
            var arrowType = FromConvertedType(element.ConvertedType.Value, column, options);
            if (arrowType != null)
                return arrowType;
        }

        // Third: fall back to PhysicalType
        return FromPhysicalType(column);
    }

    private static IArrowType? FromLogicalType(LogicalType logicalType, ColumnDescriptor column, ParquetReadOptions? options = null)
    {
        return logicalType switch
        {
            LogicalType.StringType => Apache.Arrow.Types.StringType.Default,
            LogicalType.DateType => Date32Type.Default,
            LogicalType.IntType intType => intType switch
            {
                { BitWidth: 8, IsSigned: true } => Int8Type.Default,
                { BitWidth: 8, IsSigned: false } => UInt8Type.Default,
                { BitWidth: 16, IsSigned: true } => Int16Type.Default,
                { BitWidth: 16, IsSigned: false } => UInt16Type.Default,
                { BitWidth: 32, IsSigned: true } => Int32Type.Default,
                { BitWidth: 32, IsSigned: false } => UInt32Type.Default,
                { BitWidth: 64, IsSigned: true } => Int64Type.Default,
                { BitWidth: 64, IsSigned: false } => UInt64Type.Default,
                _ => null,
            },
            LogicalType.TimestampType ts => new TimestampType(
                ts.Unit switch
                {
                    Metadata.TimeUnit.Millis => Apache.Arrow.Types.TimeUnit.Millisecond,
                    Metadata.TimeUnit.Micros => Apache.Arrow.Types.TimeUnit.Microsecond,
                    Metadata.TimeUnit.Nanos => Apache.Arrow.Types.TimeUnit.Nanosecond,
                    _ => Apache.Arrow.Types.TimeUnit.Microsecond,
                },
                // Parquet records only isAdjustedToUTC, never a zone name, so an adjusted column
                // can only come back as UTC. Arrow spells that "UTC"; the TimeZoneInfo overload
                // renders it as the "+00:00" offset instead, which is a legal but unconventional
                // spelling that no other Parquet-to-Arrow implementation produces — and which
                // therefore reads as a type mismatch to every consumer that compares zone strings.
                ts.IsAdjustedToUtc ? "UTC" : null),
            // Arrow requires Time32 for second/millisecond units and Time64 for micro/nanosecond units —
            // a Time32 with a micro/nano unit is a malformed type (4-byte buffer read with 8-byte semantics),
            // mishandled crossing the Arrow C-data-interface. Match on the unit accordingly.
            LogicalType.TimeType { Unit: Metadata.TimeUnit.Millis } => new Time32Type(Apache.Arrow.Types.TimeUnit.Millisecond),
            LogicalType.TimeType { Unit: Metadata.TimeUnit.Nanos } => new Time64Type(Apache.Arrow.Types.TimeUnit.Nanosecond),
            LogicalType.TimeType => new Time64Type(Apache.Arrow.Types.TimeUnit.Microsecond),
            LogicalType.EnumType => Apache.Arrow.Types.StringType.Default,
            LogicalType.JsonType => Apache.Arrow.Types.StringType.Default,
            LogicalType.UuidType => MakeUuidArrowType(options),
            LogicalType.Float16Type => HalfFloatType.Default,
            LogicalType.DecimalType dt => MakeDecimalType(dt.Precision, dt.Scale, column.PhysicalType, options),
            LogicalType.UnknownLogicalType { ThriftFieldId: 11 } => NullType.Default,
            _ => null, // fall through to ConvertedType or PhysicalType
        };
    }

    private static IArrowType? FromConvertedType(
        ConvertedType convertedType, ColumnDescriptor column, ParquetReadOptions? options)
    {
        return convertedType switch
        {
            ConvertedType.Utf8 => Apache.Arrow.Types.StringType.Default,
            ConvertedType.Date => Date32Type.Default,
            ConvertedType.TimestampMillis => new TimestampType(Apache.Arrow.Types.TimeUnit.Millisecond, TimeZoneInfo.Utc),
            ConvertedType.TimestampMicros => new TimestampType(Apache.Arrow.Types.TimeUnit.Microsecond, TimeZoneInfo.Utc),
            ConvertedType.TimeMillis => new Time32Type(Apache.Arrow.Types.TimeUnit.Millisecond),
            ConvertedType.TimeMicros => new Time64Type(Apache.Arrow.Types.TimeUnit.Microsecond),
            ConvertedType.Int8 => Int8Type.Default,
            ConvertedType.Int16 => Int16Type.Default,
            ConvertedType.Int32 => Int32Type.Default,
            ConvertedType.Int64 => Int64Type.Default,
            ConvertedType.Uint8 => UInt8Type.Default,
            ConvertedType.Uint16 => UInt16Type.Default,
            ConvertedType.Uint32 => UInt32Type.Default,
            ConvertedType.Uint64 => UInt64Type.Default,
            ConvertedType.Enum => Apache.Arrow.Types.StringType.Default,
            ConvertedType.Json => Apache.Arrow.Types.StringType.Default,
            ConvertedType.Decimal => MakeDecimalType(
                column.SchemaElement.Precision ?? 0,
                column.SchemaElement.Scale ?? 0,
                column.PhysicalType,
                options),
            _ => null, // fall through to PhysicalType
        };
    }

    /// <summary>
    /// Decodes a Parquet DECIMAL to an Arrow type. By default the narrowest decimal that fits the physical
    /// width is used; <see cref="DecimalOutputKind.Decimal128"/> widens everything to the classic
    /// Decimal128/256 for consumers that mishandle the narrow types (see the enum for why).
    /// </summary>
    private static IArrowType MakeDecimalType(
        int precision, int scale, PhysicalType physicalType, ParquetReadOptions? options)
    {
        // Widening is lossless — the value decoders sign-extend the unscaled integer to any target width.
        if (options?.DecimalOutput == DecimalOutputKind.Decimal128)
        {
            return precision <= 38
                ? new Decimal128Type(precision, scale)
                : new Decimal256Type(precision, scale);
        }

        return physicalType switch
        {
            PhysicalType.Int32 => new Decimal32Type(precision, scale),
            PhysicalType.Int64 => new Decimal64Type(precision, scale),
            _ => precision switch
            {
                <= 9 => new Decimal32Type(precision, scale),
                <= 18 => new Decimal64Type(precision, scale),
                <= 38 => new Decimal128Type(precision, scale),
                _ => new Decimal256Type(precision, scale),
            },
        };
    }

    /// <summary>
    /// Decodes a Parquet UUID logical type to an Arrow type. When the caller
    /// has registered an <c>arrow.uuid</c> extension via
    /// <see cref="ParquetReadOptions.ExtensionRegistry"/>, returns the
    /// extension type (typically <see cref="GuidType"/>); otherwise returns
    /// the raw <see cref="FixedSizeBinaryType"/> storage type so existing
    /// callers see the historical behaviour.
    /// </summary>
    private static IArrowType MakeUuidArrowType(ParquetReadOptions? options)
    {
        var storage = new FixedSizeBinaryType(16);
        if (options?.ExtensionRegistry is { } registry
            && registry.TryGetDefinition("arrow.uuid", out var definition)
            && definition.TryCreateType(storage, metadata: string.Empty, out var extType))
        {
            return extType;
        }
        return storage;
    }

    private static IArrowType FromPhysicalType(ColumnDescriptor column)
    {
        return column.PhysicalType switch
        {
            PhysicalType.Boolean => BooleanType.Default,
            PhysicalType.Int32 => Int32Type.Default,
            PhysicalType.Int64 => Int64Type.Default,
            PhysicalType.Float => FloatType.Default,
            PhysicalType.Double => DoubleType.Default,
            PhysicalType.ByteArray => BinaryType.Default,
            PhysicalType.FixedLenByteArray => new FixedSizeBinaryType(column.TypeLength ?? 0),
            PhysicalType.Int96 => new FixedSizeBinaryType(12),
            _ => throw new NotSupportedException(
                $"Unsupported physical type '{column.PhysicalType}' for Arrow conversion."),
        };
    }
}

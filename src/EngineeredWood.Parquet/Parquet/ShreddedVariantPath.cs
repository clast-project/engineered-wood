// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.InteropServices;
using EngineeredWood.Expressions;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet;

/// <summary>
/// A <c>variant_get</c> call resolved to the shredded column that holds its path: the <c>typed_value</c> leaf
/// whose statistics bound the result, and its residual <c>value</c> sibling, which says when they may be used
/// (#682).
/// </summary>
/// <remarks>
/// <para>For <c>variant_get(v, '$.a.b', type)</c> the leaf is <c>v.typed_value.a.typed_value.b.typed_value</c>
/// and the residual <c>v.typed_value.a.typed_value.b.value</c>. In a unit (row group, or page interval) where
/// the residual is null in every row, each row's value at the path is either in the leaf, of the leaf's type, or
/// absent: the path is missing, a value on the way is not an object (it would otherwise be shredded into the
/// <c>typed_value</c> group above), or the variant is NULL. <c>variant_get</c> answers NULL for all of those,
/// in either form. So the leaf's statistics bound every row whose result is not NULL, once converted to the
/// target type exactly (<see cref="VariantBound.ToLiteral"/>, the rules Delta's file pruning uses). This is the
/// test delta-spark and EW's <c>VariantFooterStats</c> apply to a whole file, applied per unit.</para>
/// <para>Only a path of object fields, through groups that are not repeated, resolves; a path through an
/// array element has elements, not rows. The leaf must be of a Parquet type the shredding spec maps to a
/// Variant primitive this can bound with, under an order this reader knows.</para>
/// </remarks>
internal sealed class ShreddedVariantPath
{
    private ShreddedVariantPath(int leaf, int? residual, ColumnDescriptor column, VariantBoundKind kind, string targetType)
    {
        Leaf = leaf;
        Residual = residual;
        Column = column;
        Kind = kind;
        TargetType = targetType;
    }

    /// <summary>The <c>typed_value</c> leaf's index.</summary>
    public int Leaf { get; }

    /// <summary>The residual <c>value</c> leaf's index; null when the schema has none (every value is typed).</summary>
    public int? Residual { get; }

    public ColumnDescriptor Column { get; }

    public VariantBoundKind Kind { get; }

    public string TargetType { get; }

    /// <summary>The shredded path <paramref name="call"/> reads, or null when it reads none this can bound.</summary>
    public static ShreddedVariantPath? Resolve(SchemaDescriptor schema, VariantGetCall call)
    {
        if (!call.Path.IsFieldsOnly || call.TargetType is VariantGetCall.VariantTypeName or "binary")
            return null;
        if (FindVariantGroup(schema.Root, call.Column.Name) is not { } group)
            return null;

        var node = group;
        foreach (var segment in call.Path.Segments)
        {
            if (Child(node, "typed_value") is not { IsLeaf: false } typed
                || typed.Element.RepetitionType == FieldRepetitionType.Repeated
                || Child(typed, segment.Key!) is not { IsLeaf: false } field
                || field.Element.RepetitionType == FieldRepetitionType.Repeated)
            {
                return null;
            }
            node = field;
        }

        if (Child(node, "typed_value") is not { IsLeaf: true } leafNode)
            return null;
        int leaf = IndexOf(schema, leafNode);
        if (leaf < 0)
            return null;
        var column = schema.Columns[leaf];
        if (column.MaxRepetitionLevel > 0 || KindOf(column) is not { } kind || !Ordered(column, kind))
            return null;

        int? residual = null;
        if (Child(node, "value") is { } valueNode)
        {
            int index = valueNode.IsLeaf ? IndexOf(schema, valueNode) : -1;
            if (index < 0)
                return null;
            residual = index;
        }
        return new ShreddedVariantPath(leaf, residual, column, kind, call.TargetType);
    }

    /// <summary>
    /// The variant group a predicate names: a top-level field by its name, else a nested one by its dotted
    /// path. A group counts as a variant when it is annotated VARIANT, or has the spec's shape without the
    /// annotation (a binary <c>metadata</c> leaf beside a binary <c>value</c> leaf), as older Spark writes it.
    /// </summary>
    private static SchemaNode? FindVariantGroup(SchemaNode root, string name)
    {
        var node = Child(root, name);
        if (node is null && name.IndexOf('.') >= 0)
        {
            node = root;
            foreach (string part in name.Split('.'))
            {
                if (Child(node, part) is not { IsLeaf: false } next
                    || next.Element.RepetitionType == FieldRepetitionType.Repeated)
                {
                    return null;
                }
                node = next;
            }
        }

        if (node is null || node.IsLeaf || node.Element.RepetitionType == FieldRepetitionType.Repeated)
            return null;
        if (Child(node, "metadata") is not { IsLeaf: true, Element.Type: PhysicalType.ByteArray })
            return null;
        return node.Element.LogicalType is LogicalType.VariantType
            || Child(node, "value") is { IsLeaf: true, Element.Type: PhysicalType.ByteArray }
            ? node
            : null;
    }

    private static SchemaNode? Child(SchemaNode node, string name)
    {
        foreach (var child in node.Children)
        {
            if (string.Equals(child.Name, name, StringComparison.Ordinal))
                return child;
        }
        return null;
    }

    private static int IndexOf(SchemaDescriptor schema, SchemaNode leaf)
    {
        for (int i = 0; i < schema.Columns.Count; i++)
        {
            if (ReferenceEquals(schema.Columns[i].SchemaNode, leaf))
                return i;
        }
        return -1;
    }

    /// <summary>
    /// Whether the leaf's statistics are ordered as the bound needs: the type-defined order, or for a float IEEE
    /// 754 total order, which differs from it only in placing -0 below +0 and in NaN, which the bounds guard.
    /// </summary>
    private static bool Ordered(ColumnDescriptor column, VariantBoundKind kind) => column.ColumnOrder switch
    {
        ColumnOrder.TypeDefined => true,
        ColumnOrder.Ieee754TotalOrder => kind is VariantBoundKind.Float or VariantBoundKind.Double,
        _ => false,
    };

    /// <summary>
    /// The Variant primitive a shredded leaf of this Parquet type holds (the shredding spec's table), or null for
    /// one this does not bound: binary, unsigned integers, UUIDs, nanosecond timestamps, times.
    /// </summary>
    private static VariantBoundKind? KindOf(ColumnDescriptor column)
    {
        var logical = column.SchemaElement.LogicalType;
        var converted = column.SchemaElement.ConvertedType;
        if (logical is LogicalType.DecimalType || (logical is null && converted == ConvertedType.Decimal))
        {
            return column.PhysicalType is PhysicalType.Int32 or PhysicalType.Int64 or PhysicalType.FixedLenByteArray
                ? VariantBoundKind.Decimal : null;
        }

        switch (column.PhysicalType)
        {
            case PhysicalType.Boolean:
                return logical is null ? VariantBoundKind.Boolean : null;
            case PhysicalType.Int32:
                return logical switch
                {
                    null => converted switch
                    {
                        null or ConvertedType.Int32 => VariantBoundKind.Int32,
                        ConvertedType.Int8 => VariantBoundKind.Int8,
                        ConvertedType.Int16 => VariantBoundKind.Int16,
                        ConvertedType.Date => VariantBoundKind.Date,
                        _ => null,
                    },
                    LogicalType.IntType { IsSigned: true, BitWidth: 8 } => VariantBoundKind.Int8,
                    LogicalType.IntType { IsSigned: true, BitWidth: 16 } => VariantBoundKind.Int16,
                    LogicalType.IntType { IsSigned: true, BitWidth: 32 } => VariantBoundKind.Int32,
                    LogicalType.DateType => VariantBoundKind.Date,
                    _ => null,
                };
            case PhysicalType.Int64:
                return logical switch
                {
                    null => converted switch
                    {
                        null or ConvertedType.Int64 => VariantBoundKind.Int64,
                        ConvertedType.TimestampMicros => VariantBoundKind.Timestamp,
                        _ => null,
                    },
                    LogicalType.IntType { IsSigned: true, BitWidth: 64 } => VariantBoundKind.Int64,
                    LogicalType.TimestampType { Unit: TimeUnit.Micros } ts =>
                        ts.IsAdjustedToUtc ? VariantBoundKind.Timestamp : VariantBoundKind.TimestampNtz,
                    _ => null,
                };
            case PhysicalType.Float:
                return logical is null ? VariantBoundKind.Float : null;
            case PhysicalType.Double:
                return logical is null ? VariantBoundKind.Double : null;
            case PhysicalType.ByteArray:
                return logical is LogicalType.StringType || (logical is null && converted == ConvertedType.Utf8)
                    ? VariantBoundKind.String : null;
            default:
                return null;
        }
    }

    /// <summary>A lower bound on the result from the leaf's minimum, given the maximum beside it.</summary>
    public LiteralValue? LowerBound(byte[]? min, byte[]? max)
    {
        if (min is null)
            return null;
        // As for a float column (ParquetStatisticsAccessor.DecodeLowerBound): a NaN minimum is a bound only when
        // the maximum is NaN too. Beside a finite maximum it is the mark of a writer using .NET's order.
        if (IsNaN(min) && (max is null || !IsNaN(max)))
            return null;
        return ToLiteral(min);
    }

    /// <summary>An upper bound on the result from the leaf's maximum. A NaN maximum is the weakest bound.</summary>
    public LiteralValue? UpperBound(byte[]? max) => max is null ? null : ToLiteral(max);

    private bool IsNaN(byte[] bytes) => Kind switch
    {
        VariantBoundKind.Float => bytes.Length == 4 && float.IsNaN(MemoryMarshal.Read<float>(bytes)),
        VariantBoundKind.Double => bytes.Length == 8 && double.IsNaN(MemoryMarshal.Read<double>(bytes)),
        _ => false,
    };

    private LiteralValue? ToLiteral(byte[] bytes) => ToVariantBound(bytes) is { } bound ? bound.ToLiteral(TargetType) : null;

    private VariantBound? ToVariantBound(byte[] bytes)
    {
        switch (Kind)
        {
            case VariantBoundKind.Boolean:
                return bytes.Length == 1 ? VariantBound.Boolean(bytes[0] != 0) : null;
            case VariantBoundKind.Int8 or VariantBoundKind.Int16 or VariantBoundKind.Int32:
                return bytes.Length == 4 ? VariantBound.Integer(Kind, BinaryPrimitives.ReadInt32LittleEndian(bytes)) : null;
            case VariantBoundKind.Int64:
                return bytes.Length == 8 ? VariantBound.Integer(Kind, BinaryPrimitives.ReadInt64LittleEndian(bytes)) : null;
            case VariantBoundKind.Date:
                return bytes.Length == 4 ? VariantBound.Date(BinaryPrimitives.ReadInt32LittleEndian(bytes)) : null;
            case VariantBoundKind.Timestamp:
                return bytes.Length == 8 ? VariantBound.Timestamp(BinaryPrimitives.ReadInt64LittleEndian(bytes)) : null;
            case VariantBoundKind.TimestampNtz:
                return bytes.Length == 8 ? VariantBound.TimestampNtz(BinaryPrimitives.ReadInt64LittleEndian(bytes)) : null;
            case VariantBoundKind.Float:
                return bytes.Length == 4 ? VariantBound.Float(MemoryMarshal.Read<float>(bytes)) : null;
            case VariantBoundKind.Double:
                return bytes.Length == 8 ? VariantBound.Double(MemoryMarshal.Read<double>(bytes)) : null;
            case VariantBoundKind.Decimal:
                return Decimal(bytes);
            case VariantBoundKind.String:
                // Strictly: a bound cut at a byte limit can end inside a character, and a replacement character in
                // its place sorts above what was cut, which would raise a minimum.
                return TryDecodeUtf8(bytes, out string text) ? VariantBound.String(text) : null;
            default:
                return null;
        }
    }

    private VariantBound? Decimal(byte[] bytes)
    {
        var element = Column.SchemaElement;
        int scale = element.LogicalType is LogicalType.DecimalType d ? d.Scale : element.Scale ?? 0;
        BigInteger unscaled;
        switch (Column.PhysicalType)
        {
            case PhysicalType.Int32 when bytes.Length == 4:
                unscaled = BinaryPrimitives.ReadInt32LittleEndian(bytes);
                break;
            case PhysicalType.Int64 when bytes.Length == 8:
                unscaled = BinaryPrimitives.ReadInt64LittleEndian(bytes);
                break;
            case PhysicalType.FixedLenByteArray when bytes.Length is > 0 and <= 16:
                var little = new byte[bytes.Length];
                for (int i = 0; i < bytes.Length; i++)
                    little[i] = bytes[bytes.Length - 1 - i];
                unscaled = new BigInteger(little);
                break;
            default:
                return null;
        }
        return VariantBound.Decimal(unscaled, scale);
    }

    private static readonly System.Text.UTF8Encoding StrictUtf8 =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private static bool TryDecodeUtf8(byte[] bytes, out string text)
    {
        try
        {
            text = StrictUtf8.GetString(bytes);
            return true;
        }
        catch (System.Text.DecoderFallbackException)
        {
            text = "";
            return false;
        }
    }
}

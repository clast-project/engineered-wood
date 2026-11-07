// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Arrow;

/// <summary>
/// Whether two Arrow types describe the same data: the same kind with the same parameters (units, zones,
/// precision and scale, widths), and for nested types the same children by name, order and nullability, all the way
/// down, and for unions the same mode and type ids. A <see cref="IArrowType.TypeId"/> alone leaves those open: two timestamps of different units share one.
/// Field metadata is not compared.
/// </summary>
internal static class ArrowTypeEquality
{
    /// <summary>Whether the fields agree pairwise in name, nullability and <see cref="SameType"/>.</summary>
    public static bool SameLayout(IReadOnlyList<Field> a, IReadOnlyList<Field> b)
    {
        if (a.Count != b.Count)
            return false;
        for (int i = 0; i < a.Count; i++)
        {
            if (!string.Equals(a[i].Name, b[i].Name, StringComparison.Ordinal)
                || a[i].IsNullable != b[i].IsNullable
                || !SameType(a[i].DataType, b[i].DataType))
            {
                return false;
            }
        }
        return true;
    }

    /// <summary>Whether the types agree in kind and every parameter, children included.</summary>
    public static bool SameType(IArrowType a, IArrowType b)
    {
        if (a.TypeId != b.TypeId)
            return false;

        return (a, b) switch
        {
            (ExtensionType x, ExtensionType y) => string.Equals(x.Name, y.Name, StringComparison.Ordinal)
                && string.Equals(x.ExtensionMetadata, y.ExtensionMetadata, StringComparison.Ordinal)
                && SameType(x.StorageType, y.StorageType),
            (DictionaryType x, DictionaryType y) => SameType(x.IndexType, y.IndexType)
                && SameType(x.ValueType, y.ValueType) && x.Ordered == y.Ordered,
            (FixedSizeListType x, FixedSizeListType y) => x.ListSize == y.ListSize && SameLayout(x.Fields, y.Fields),
            (MapType x, MapType y) => x.KeySorted == y.KeySorted && SameLayout(x.Fields, y.Fields),
            // A row names its branch by type id, so the id-to-field mapping is part of the type (#736).
            (UnionType x, UnionType y) => x.Mode == y.Mode && x.TypeIds.AsSpan().SequenceEqual(y.TypeIds)
                && SameLayout(x.Fields, y.Fields),
            (NestedType x, NestedType y) => SameLayout(x.Fields, y.Fields),
            (Decimal32Type x, Decimal32Type y) => x.Precision == y.Precision && x.Scale == y.Scale,
            (Decimal64Type x, Decimal64Type y) => x.Precision == y.Precision && x.Scale == y.Scale,
            (Decimal128Type x, Decimal128Type y) => x.Precision == y.Precision && x.Scale == y.Scale,
            (Decimal256Type x, Decimal256Type y) => x.Precision == y.Precision && x.Scale == y.Scale,
            (TimestampType x, TimestampType y) => x.Unit == y.Unit
                && string.Equals(x.Timezone, y.Timezone, StringComparison.Ordinal),
            (Time32Type x, Time32Type y) => x.Unit == y.Unit,
            (Time64Type x, Time64Type y) => x.Unit == y.Unit,
            (DurationType x, DurationType y) => x.Unit == y.Unit,
            (IntervalType x, IntervalType y) => x.Unit == y.Unit,
            (FixedSizeBinaryType x, FixedSizeBinaryType y) => x.ByteWidth == y.ByteWidth,
            _ => true,
        };
    }

    /// <summary>
    /// The type written out with everything <see cref="SameType"/> compares, for a message that says what differs:
    /// <c>struct&lt;a: int32 not null, b: timestamp[us, UTC]&gt;</c>. A bare <see cref="IArrowType.Name"/> says only
    /// <c>struct</c>.
    /// </summary>
    public static string Describe(IArrowType type) => type switch
    {
        // The metadata is compared too, so it is shown when there is any: types that differ only there would
        // otherwise print the same.
        ExtensionType x => string.IsNullOrEmpty(x.ExtensionMetadata)
            ? $"{x.Name}<{Describe(x.StorageType)}>"
            : $"{x.Name}[\"{x.ExtensionMetadata}\"]<{Describe(x.StorageType)}>",
        DictionaryType x => $"dictionary<{Describe(x.IndexType)}, {Describe(x.ValueType)}{(x.Ordered ? ", ordered" : "")}>",
        FixedSizeListType x => $"{x.Name}[{x.ListSize}]<{DescribeFields(x.Fields)}>",
        MapType x => $"{x.Name}{(x.KeySorted ? "[sorted]" : "")}<{DescribeFields(x.Fields)}>",
        UnionType x => $"{x.Name}[{x.Mode.ToString().ToLowerInvariant()}, ids [{string.Join(", ", x.TypeIds)}]]<{DescribeFields(x.Fields)}>",
        NestedType x => $"{x.Name}<{DescribeFields(x.Fields)}>",
        Decimal32Type x => $"{x.Name}({x.Precision}, {x.Scale})",
        Decimal64Type x => $"{x.Name}({x.Precision}, {x.Scale})",
        Decimal128Type x => $"{x.Name}({x.Precision}, {x.Scale})",
        Decimal256Type x => $"{x.Name}({x.Precision}, {x.Scale})",
        TimestampType x => x.Timezone is null ? $"{x.Name}[{x.Unit}]" : $"{x.Name}[{x.Unit}, {x.Timezone}]",
        Time32Type x => $"{x.Name}[{x.Unit}]",
        Time64Type x => $"{x.Name}[{x.Unit}]",
        DurationType x => $"{x.Name}[{x.Unit}]",
        IntervalType x => $"{x.Name}[{x.Unit}]",
        FixedSizeBinaryType x => $"{x.Name}[{x.ByteWidth}]",
        _ => type.Name,
    };

    /// <summary>A decimal type's scale, of any width; null for any other type.</summary>
    public static int? DecimalScale(IArrowType type) => type switch
    {
        Decimal32Type x => x.Scale,
        Decimal64Type x => x.Scale,
        Decimal128Type x => x.Scale,
        Decimal256Type x => x.Scale,
        _ => null,
    };

    private static string DescribeFields(IReadOnlyList<Field> fields) => string.Join(", ",
        fields.Select(f => $"{f.Name}: {Describe(f.DataType)}{(f.IsNullable ? "" : " not null")}"));
}

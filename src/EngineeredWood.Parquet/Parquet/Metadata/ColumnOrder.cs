// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// The sort order used for a column's min/max statistics, mirroring the
/// <c>ColumnOrder</c> union in the Parquet Thrift definition.
/// </summary>
/// <remarks>
/// A file's <c>column_orders</c> list (footer field 7) holds one entry per leaf
/// column, in the same order as the row groups' columns. When the list is
/// absent, readers historically assume <see cref="TypeDefined"/>.
/// </remarks>
public enum ColumnOrder
{
    /// <summary>
    /// The writer set a <c>ColumnOrder</c> union member this reader does not
    /// recognize (e.g. a future ordering). Readers MUST NOT use the column's
    /// <c>min_value</c>/<c>max_value</c> statistics in this case.
    /// </summary>
    Undefined = 0,

    /// <summary>
    /// <c>TypeDefinedOrder</c> (union field 1): the ordering implied by the
    /// column's physical and logical type. For FLOAT/DOUBLE this excludes NaN
    /// from min/max and is ambiguous about NaN and signed zero — see
    /// <see cref="Ieee754TotalOrder"/>.
    /// </summary>
    TypeDefined = 1,

    /// <summary>
    /// <c>IEEE754TotalOrder</c> (union field 2, PARQUET-2249): floating-point
    /// columns ordered by the IEEE 754 totalOrder predicate, where
    /// <c>-0 &lt; +0</c> and NaN bounds are written only when every non-null
    /// value is NaN. Recognized only by newer readers.
    /// </summary>
    Ieee754TotalOrder = 2,
}

/// <summary>
/// Builds the per-leaf-column <see cref="ColumnOrder"/> list that populates the
/// file footer's <c>column_orders</c> field.
/// </summary>
internal static class ColumnOrderBuilder
{
    /// <summary>
    /// Produces one <see cref="ColumnOrder"/> per leaf column (each flattened
    /// <see cref="SchemaElement"/> carrying a physical type), in schema order.
    /// FLOAT and DOUBLE columns receive <paramref name="floatingPointOrder"/>;
    /// every other column receives <see cref="ColumnOrder.TypeDefined"/>.
    /// </summary>
    public static ColumnOrder[] Build(
        IReadOnlyList<SchemaElement> schema, ColumnOrder floatingPointOrder)
    {
        var orders = new List<ColumnOrder>(schema.Count);
        foreach (var element in schema)
        {
            // Root and group nodes have no physical type; only leaves do.
            if (!element.Type.HasValue)
                continue;

            bool isFloatingPoint =
                element.Type == PhysicalType.Float || element.Type == PhysicalType.Double;
            orders.Add(isFloatingPoint ? floatingPointOrder : ColumnOrder.TypeDefined);
        }

        return orders.ToArray();
    }
}

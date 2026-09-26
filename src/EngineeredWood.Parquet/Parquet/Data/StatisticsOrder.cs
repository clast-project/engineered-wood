// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Types;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// How <see cref="StatisticsCollector"/> orders a column's values when it picks the minimum and
/// maximum. The physical type alone does not decide it: Parquet orders values by their logical type.
/// </summary>
internal enum StatisticsOrder
{
    /// <summary>
    /// The physical type's own order: signed for INT32/INT64, numeric for FLOAT/DOUBLE, and unsigned
    /// lexicographic for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY.
    /// </summary>
    Default,

    /// <summary>INT32/INT64 compared as unsigned integers (<c>INT(32|64, false)</c>).</summary>
    Unsigned,

    /// <summary>
    /// FIXED_LEN_BYTE_ARRAY holding a big-endian two's-complement integer: a DECIMAL, whose order is
    /// that of the signed number.
    /// </summary>
    SignedBigEndian,

    /// <summary>The extended-precision timestamp carrier, a signed little-endian integer.</summary>
    ExtendedTimestamp,
}

internal static class StatisticsOrders
{
    /// <summary>
    /// The order for a column written from <paramref name="valueType"/> on <paramref name="physicalType"/>.
    /// </summary>
    /// <param name="extendedTimestamp">Whether the column is written as the extended-timestamp carrier.</param>
    public static StatisticsOrder For(IArrowType valueType, PhysicalType physicalType, bool extendedTimestamp)
    {
        if (extendedTimestamp)
            return StatisticsOrder.ExtendedTimestamp;

        return physicalType switch
        {
            // UInt8/UInt16 are zero-extended to INT32 first, so signed order would happen to agree for
            // them; they are listed so that the answer does not depend on that.
            PhysicalType.Int32 or PhysicalType.Int64
                when valueType is UInt8Type or UInt16Type or UInt32Type or UInt64Type
                => StatisticsOrder.Unsigned,
            PhysicalType.FixedLenByteArray when valueType is Decimal128Type or Decimal256Type
                => StatisticsOrder.SignedBigEndian,
            _ => StatisticsOrder.Default,
        };
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;

namespace EngineeredWood.Iceberg.Manifest;

/// <summary>
/// Builds a manifest list's partition summaries (<c>partitions</c>, field 507) for a new manifest: per
/// partition field, whether any value is null or NaN, and the least and greatest other value.
/// </summary>
/// <remarks>
/// The bounds are Appendix D single-value binary, written by the encoder the caller passes. A summary whose
/// bound is null states that every value is null or NaN, and readers prune on that, so a bound must never be
/// left out for want of an encoder: without one, or for a type this ordering does not cover, no summary is
/// written at all (<c>partitions</c> null), which readers treat as "unknown". The table operations pass
/// <see cref="ManifestValues.EncodeBound"/>.
/// </remarks>
internal static class PartitionSummaries
{
    /// <summary>
    /// The summaries for <paramref name="files"/>, or null when they cannot all be written. An unpartitioned
    /// manifest's summary list is empty.
    /// </summary>
    public static IReadOnlyList<PartitionFieldSummary>? Build(
        IReadOnlyList<PartitionValueType> partitionType,
        IEnumerable<DataFile> files,
        Func<IcebergType, object, byte[]?>? encodeBound)
    {
        if (partitionType.Count == 0)
            return [];
        if (encodeBound is null)
            return null;

        var fileList = files.ToList();
        var summaries = new List<PartitionFieldSummary>(partitionType.Count);
        foreach (var field in partitionType)
        {
            if (!IsOrdered(field.Type))
                return null;

            bool containsNull = false, containsNan = false;
            object? min = null, max = null;
            foreach (var file in fileList)
            {
                if (file.Partition is null || !file.Partition.TryGetValue(field.FieldId, out var value))
                    return null;
                switch (value)
                {
                    case null:
                        containsNull = true;
                        continue;
                    case float f when float.IsNaN(f):
                    case double d when double.IsNaN(d):
                        containsNan = true;
                        continue;
                }
                if (min is null || Compare(value, min) < 0)
                    min = value;
                if (max is null || Compare(value, max) > 0)
                    max = value;
            }

            byte[]? lower = null, upper = null;
            if (min is not null)
            {
                lower = encodeBound(field.Type, min);
                upper = encodeBound(field.Type, max!);
                if (lower is null || upper is null)
                    return null;
            }
            summaries.Add(new PartitionFieldSummary(containsNull, containsNan, lower, upper));
        }
        return summaries;
    }

    // UUID ordering differs between implementations (Java compared signed halves for years), so a UUID field
    // gets no summary rather than one a reader might prune against with a different order.
    private static bool IsOrdered(IcebergType type) => type is BooleanType or IntegerType or LongType or DateType
        or TimeType or TimestampType or TimestamptzType or TimestampNsType or TimestamptzNsType
        or FloatType or DoubleType or StringType or BinaryType or FixedType or DecimalType;

    /// <summary>Iceberg's ordering of two non-null, non-NaN physical values of one type.</summary>
    internal static int Compare(object a, object b) => (a, b) switch
    {
        (bool x, bool y) => x.CompareTo(y),
        (int x, int y) => x.CompareTo(y),
        (long x, long y) => x.CompareTo(y),
        (float x, float y) => CompareFloat(x, y),
        (double x, double y) => CompareFloat(x, y),
        (BigInteger x, BigInteger y) => x.CompareTo(y),
        // Strings order by Unicode code point, which is the order of their UTF-8 bytes.
        (string x, string y) => CompareBytes(System.Text.Encoding.UTF8.GetBytes(x), System.Text.Encoding.UTF8.GetBytes(y)),
        (byte[] x, byte[] y) => CompareBytes(x, y),
        _ => throw new ArgumentException($"Cannot order {a.GetType().Name} against {b.GetType().Name}."),
    };

    // -0 sorts before +0: "if -0.0 is a value of the partition field, the lower_bound must not be +0.0".
    private static int CompareFloat(double x, double y)
    {
        int c = x.CompareTo(y);
        if (c != 0 || x != 0)
            return c;
        bool xNeg = BitConverter.DoubleToInt64Bits(x) < 0, yNeg = BitConverter.DoubleToInt64Bits(y) < 0;
        return xNeg == yNeg ? 0 : xNeg ? -1 : 1;
    }

    private static int CompareBytes(byte[] x, byte[] y)
    {
        int n = Math.Min(x.Length, y.Length);
        for (int i = 0; i < n; i++)
        {
            if (x[i] != y[i])
                return x[i].CompareTo(y[i]);
        }
        return x.Length.CompareTo(y.Length);
    }
}

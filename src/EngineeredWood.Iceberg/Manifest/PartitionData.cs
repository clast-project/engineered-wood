// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;

namespace EngineeredWood.Iceberg.Manifest;

/// <summary>
/// A data file's partition tuple: one value per field of the partition spec the file was written with,
/// typed by that field's result type.
/// </summary>
/// <remarks>
/// <para>Each value is held in its Iceberg physical form, which is how manifests store it:</para>
/// <list type="table">
/// <listheader><term>Iceberg type</term><description>CLR value</description></listheader>
/// <item><term>boolean</term><description><see cref="bool"/></description></item>
/// <item><term>int, date (days from the epoch)</term><description><see cref="int"/></description></item>
/// <item><term>long, time, timestamp, timestamptz (micros), timestamp_ns, timestamptz_ns (nanos)</term>
/// <description><see cref="long"/></description></item>
/// <item><term>float, double</term><description><see cref="float"/>, <see cref="double"/></description></item>
/// <item><term>string</term><description><see cref="string"/></description></item>
/// <item><term>uuid</term><description><see cref="Guid"/></description></item>
/// <item><term>fixed, binary</term><description><see cref="byte"/>[]</description></item>
/// <item><term>decimal(P, S)</term><description>the unscaled <see cref="BigInteger"/></description></item>
/// </list>
/// <para>A null value is <see langword="null"/>. Results of the <c>bucket</c>, <c>year</c>, <c>month</c> and
/// <c>hour</c> transforms are ints, and <c>day</c> results are dates.</para>
/// </remarks>
public sealed class PartitionData : IEquatable<PartitionData>
{
    /// <summary>The tuple of an unpartitioned file.</summary>
    public static readonly PartitionData Empty = new([]);

    /// <summary>Creates a tuple from its field values, in partition-spec order.</summary>
    public PartitionData(IReadOnlyList<PartitionValue> values)
    {
        Values = values ?? throw new ArgumentNullException(nameof(values));
    }

    /// <summary>The tuple's values, in partition-spec order.</summary>
    public IReadOnlyList<PartitionValue> Values { get; }

    /// <summary>The value of the partition field with id <paramref name="fieldId"/>.</summary>
    public bool TryGetValue(int fieldId, out object? value)
    {
        foreach (var v in Values)
        {
            if (v.FieldId == fieldId)
            {
                value = v.Value;
                return true;
            }
        }
        value = null;
        return false;
    }

    /// <inheritdoc/>
    public bool Equals(PartitionData? other)
    {
        if (other is null || other.Values.Count != Values.Count)
            return false;
        for (int i = 0; i < Values.Count; i++)
        {
            if (!Values[i].Equals(other.Values[i]))
                return false;
        }
        return true;
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj) => Equals(obj as PartitionData);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        int hash = Values.Count;
        foreach (var v in Values)
            hash = unchecked(hash * 31 + v.GetHashCode());
        return hash;
    }

    /// <inheritdoc/>
    public override string ToString() =>
        "{" + string.Join(", ", Values.Select(v => $"{v.Name}={PartitionValue.Format(v.Value)}")) + "}";
}

/// <summary>One field of a <see cref="PartitionData"/>.</summary>
/// <param name="FieldId">The partition field's id.</param>
/// <param name="Name">The partition field's name.</param>
/// <param name="Type">The field's result type.</param>
/// <param name="Value">The value in its physical form (see <see cref="PartitionData"/>), or null.</param>
public readonly record struct PartitionValue(int FieldId, string Name, IcebergType Type, object? Value)
{
    /// <inheritdoc/>
    public bool Equals(PartitionValue other) =>
        FieldId == other.FieldId && Name == other.Name && Type.Equals(other.Type) && ValueEquals(Value, other.Value);

    /// <inheritdoc/>
    public override int GetHashCode() =>
        unchecked(FieldId * 31 + (Value is byte[] b ? b.Length : Value?.GetHashCode() ?? 0));

    private static bool ValueEquals(object? a, object? b) => (a, b) switch
    {
        (null, null) => true,
        (byte[] x, byte[] y) => x.AsSpan().SequenceEqual(y),
        // Floats compare by bits, as Iceberg's partition equality does: NaN equals NaN, -0 differs from 0.
        (float x, float y) => BitConverter.ToInt32(BitConverter.GetBytes(x), 0) == BitConverter.ToInt32(BitConverter.GetBytes(y), 0),
        (double x, double y) => BitConverter.DoubleToInt64Bits(x) == BitConverter.DoubleToInt64Bits(y),
        _ => Equals(a, b),
    };

    internal static string Format(object? value) => value switch
    {
        null => "null",
        byte[] b => "0x" + BitConverter.ToString(b).Replace("-", ""),
        _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "",
    };
}

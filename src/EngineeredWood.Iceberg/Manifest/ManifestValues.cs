// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using EngineeredWood.Expressions;
using EngineeredWood.Iceberg.Values;

namespace EngineeredWood.Iceberg.Manifest;

/// <summary>
/// Where the manifest codec meets the value codecs (#953 item 5): partition values in their physical form
/// as <see cref="LiteralValue"/>s, encoded partition-summary bounds, and decoded column bounds for planning.
/// </summary>
internal static class ManifestValues
{
    /// <summary>
    /// A partition value in its physical form (see <see cref="PartitionData"/>) as the literal the value codecs
    /// take for <paramref name="type"/>.
    /// </summary>
    /// <exception cref="ArgumentException">The value is not one of the type's, or has no literal (a date or
    /// timestamp outside the range a literal holds).</exception>
    public static LiteralValue ToLiteral(IcebergType type, object value) => (type, value) switch
    {
        (BooleanType, bool b) => LiteralValue.Of(b),
        (IntegerType, int i) => LiteralValue.Of(i),
        (LongType, long l) => LiteralValue.Of(l),
        (FloatType, float f) => LiteralValue.Of(f),
        (DoubleType, double d) => LiteralValue.Of(d),
        (StringType, string s) => IcebergLiterals.Coerce(type, LiteralValue.Of(s)),
        (BinaryType, byte[] b) => LiteralValue.Of(b),
        (FixedType, byte[] b) => IcebergLiterals.Coerce(type, LiteralValue.Of(b)),
        (UuidType, Guid g) => LiteralValue.Of(g),
        (DecimalType d, BigInteger unscaled) => IcebergLiterals.Coerce(d, LiteralValue.HighPrecisionDecimalOf(unscaled, d.Scale)),
        (DateType, int days) => IcebergLiterals.TryDateLiteral(days, out var date)
            ? date
            : throw new ArgumentException($"Date {days} (days from the epoch) has no literal."),
        (TimeType, long micros) => IcebergLiterals.TimeLiteral(micros),
        (TimestampType or TimestamptzType, long micros) => IcebergLiterals.TryMicrosLiteral(micros, out var ts)
            ? ts
            : throw new ArgumentException($"Timestamp {micros} (micros from the epoch) has no literal."),
        (TimestampNsType or TimestamptzNsType, long nanos) => LiteralValue.TimestampNanosOf(nanos),
        _ => throw new ArgumentException(
            $"A partition value of CLR type {value.GetType().Name} does not fit Iceberg type {type}."),
    };

    /// <summary>
    /// A partition-summary bound: the value as Appendix D single-value binary. Null when it cannot be encoded,
    /// which makes <see cref="PartitionSummaries.Build"/> write no summaries rather than wrong ones.
    /// </summary>
    public static byte[]? EncodeBound(IcebergType type, object value)
    {
        try
        {
            return SingleValueBinary.Encode(type, ToLiteral(type, value));
        }
        catch (Exception e) when (e is ArgumentException or NotSupportedException or FormatException)
        {
            return null;
        }
    }

    /// <summary>
    /// <paramref name="file"/> with <see cref="DataFile.ColumnLowerBounds"/> and
    /// <see cref="DataFile.ColumnUpperBounds"/> decoded from its raw bounds, for scan planning. Each column is
    /// typed by <paramref name="schema"/>; a bound written before the column's type was promoted is read by
    /// its length. A bound that cannot be used safely, or a field the schema no longer has, is left out, and
    /// planning then treats it as unknown and keeps the file.
    /// </summary>
    public static DataFile WithDecodedBounds(DataFile file, Schema schema)
    {
        if (file.LowerBounds is null && file.UpperBounds is null)
            return file;
        return file with
        {
            ColumnLowerBounds = Decode(file.LowerBounds, schema) ?? file.ColumnLowerBounds,
            ColumnUpperBounds = Decode(file.UpperBounds, schema) ?? file.ColumnUpperBounds,
        };
    }

    private static Dictionary<int, LiteralValue>? Decode(IReadOnlyDictionary<int, byte[]>? raw, Schema schema)
    {
        if (raw is null)
            return null;
        var decoded = new Dictionary<int, LiteralValue>(raw.Count);
        foreach (var (fieldId, bytes) in raw)
        {
            if (PartitionTypes.FindField(schema.Fields, fieldId) is { Type: PrimitiveType type }
                && SingleValueBinary.TryDecodeBound(type, bytes, out var bound))
            {
                decoded[fieldId] = bound;
            }
        }
        return decoded.Count > 0 ? decoded : null;
    }
}

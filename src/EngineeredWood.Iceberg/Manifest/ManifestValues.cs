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
    /// The primitive columns of <paramref name="schema"/> whose bounds planning can use, by field id, at any
    /// struct depth. Built once per read, so typing a file's bounds costs a lookup per bound.
    /// </summary>
    /// <remarks>
    /// UUID columns are left out. Iceberg Java orders UUIDs by their two signed 64-bit halves when it computes
    /// a file's bounds, and the planner compares <see cref="Guid"/>s in another order, so a UUID bound could
    /// prune a file that holds a match. Partition summaries leave UUIDs out for the same reason.
    /// </remarks>
    public static IReadOnlyDictionary<int, PrimitiveType> BoundColumnTypes(Schema schema)
    {
        var types = new Dictionary<int, PrimitiveType>();
        Collect(schema.Fields);
        return types;

        void Collect(IReadOnlyList<NestedField> fields)
        {
            foreach (var field in fields)
            {
                switch (field.Type)
                {
                    case StructType s:
                        Collect(s.Fields);
                        break;
                    case UuidType:
                        break;
                    case PrimitiveType p:
                        types[field.Id] = p;
                        break;
                }
            }
        }
    }

    /// <summary>
    /// <paramref name="file"/> with <see cref="DataFile.ColumnLowerBounds"/> and
    /// <see cref="DataFile.ColumnUpperBounds"/> decoded from its raw bounds, for scan planning. Each column is
    /// typed by <paramref name="columnTypes"/> (see <see cref="BoundColumnTypes"/>); a bound written before the
    /// column's type was promoted is read by its length. A bound that cannot be used safely, or a field with no
    /// usable type, is left out, and planning then treats it as unknown and keeps the file.
    /// </summary>
    public static DataFile WithDecodedBounds(DataFile file, IReadOnlyDictionary<int, PrimitiveType> columnTypes)
    {
        if (file.LowerBounds is null && file.UpperBounds is null)
            return file;
        return file with
        {
            ColumnLowerBounds = Decode(file.LowerBounds, columnTypes) ?? file.ColumnLowerBounds,
            ColumnUpperBounds = Decode(file.UpperBounds, columnTypes) ?? file.ColumnUpperBounds,
        };
    }

    private static Dictionary<int, LiteralValue>? Decode(
        IReadOnlyDictionary<int, byte[]>? raw, IReadOnlyDictionary<int, PrimitiveType> columnTypes)
    {
        if (raw is null)
            return null;
        var decoded = new Dictionary<int, LiteralValue>(raw.Count);
        foreach (var (fieldId, bytes) in raw)
        {
            if (columnTypes.TryGetValue(fieldId, out var type) && SingleValueBinary.TryDecodeBound(type, bytes, out var bound))
                decoded[fieldId] = bound;
        }
        return decoded.Count > 0 ? decoded : null;
    }
}

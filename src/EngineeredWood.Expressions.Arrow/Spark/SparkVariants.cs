// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Data.SqlTypes;
using System.Globalization;
using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.Expressions.Arrow.Spark;

/// <summary>
/// Spark's <c>variant_get</c> / <c>try_variant_get</c>, and the <c>cast</c> / <c>try_cast</c> of a variant
/// (what <c>v:a::int</c> parses to), over a <see cref="VariantArray"/>, shredded or not.
/// </summary>
/// <remarks>
/// <para>What Spark answers (<c>VariantGet.cast</c>, measured against Spark 4.1): a SQL NULL variant, a missing
/// path, or a path through a value that is not an object (or array) is NULL, under either form. A variant null at
/// the path is NULL for a typed result and a variant null for a variant one. Otherwise the value is read as the
/// Spark scalar its variant type names -- every integer width as a BIGINT, a decimal as the narrowest DECIMAL
/// holding it -- and cast to the target as <c>try_cast</c> would. A NULL from that cast, or a pair of types an ANSI
/// cast refuses, or an object or array cast to anything but a string, is <c>INVALID_VARIANT_CAST</c>, or NULL
/// under <c>try_</c>.</para>
/// <para>So the conversions are Spark's cast rules and not the exact-only subset the file pruner uses:
/// <c>"7"</c> reads as 7, the double 1.25 as the INT 1 (a cast truncates) and the decimal 1.25 as the
/// DECIMAL(2,1) 1.3 (it rounds).</para>
/// <para>Not implemented, refused rather than approximated: an object or array cast to a string (which is
/// Spark's <c>to_json</c> rendering), a target this library's cast cannot produce (TIMESTAMP_NTZ, nested types), and
/// the variant types Spark 4.1 has no cast for (TIME and the nanosecond timestamps).</para>
/// </remarks>
internal static class SparkVariants
{
    private static CultureInfo Invariant => CultureInfo.InvariantCulture;

    /// <summary>
    /// <c>variant_get(v, path [, type])</c>, or <c>try_variant_get</c> when <paramref name="failOnError"/> is
    /// false. Without a type the result is the variant at the path.
    /// </summary>
    public static IArrowArray VariantGet(
        string name,
        IReadOnlyList<IArrowArray> args,
        int rowCount,
        bool failOnError,
        Func<IArrowArray, IArrowType, int, IArrowArray> tryCast)
    {
        if (args.Count is not (2 or 3))
            throw new ArgumentException($"{name} expects 2 or 3 arguments but got {args.Count}");

        // The path may differ by row (measured, Spark 4.1: `variant_get(v, p, 'int')` over a path column reads each
        // row's path, and a NULL path is NULL). Every element present is parsed, which over no rows is still the
        // one row a constant path keeps (ArrowRowEvaluator.ConstantArray), so an invalid literal path is refused
        // when a CHECK constraint is added, as Spark refuses it at analysis.
        var paths = ParsePaths(name, args[1]);

        IArrowType? target = null;
        if (args.Count == 3)
        {
            // Through the canonical spelling where there is one, so an alias such as timestamp_ltz reads as
            // Spark's DataType.fromDDL reads it. The type must be foldable, which the evaluator checks before
            // the call (CheckFoldableArguments); here it must also be the same non-null string throughout.
            string typeText = TypeText(args[2]);
            if (!VariantGetCall.TryNormalizeTypeName(typeText, out string canonical))
                target = SparkArrays.ParseTypeName(typeText);
            else if (canonical != VariantGetCall.VariantTypeName)
                target = SparkArrays.ParseTypeName(canonical);
        }

        // A bare NULL is a NULL variant, so the answer is NULL whatever the path.
        if (args[0].Data.DataType is NullType)
            return ArrowCompute.MakeNullArray(target ?? VariantType.Default, rowCount);

        var variants = RequireVariant(name, args[0]);
        var leaves = new VariantValue?[rowCount];
        for (int row = 0; row < rowCount; row++)
        {
            var path = paths[paths.Length == 1 ? 0 : row];
            leaves[row] = path is not null && TryNavigate(variants, row, path, out var leaf) ? leaf : null;
        }

        return target is null
            ? BuildVariants(leaves)
            : CastLeaves(leaves, target, rowCount, failOnError, tryCast);
    }

    /// <summary>
    /// <c>cast(v AS type)</c> of a variant. Fails on a value that does not convert when
    /// <paramref name="failOnError"/> (a cast, under either dialect: Spark 4.1 raises with ANSI off too), and is
    /// NULL there otherwise (<c>try_cast</c>).
    /// </summary>
    public static IArrowArray Cast(
        VariantArray variants,
        IArrowType target,
        int rowCount,
        bool failOnError,
        Func<IArrowArray, IArrowType, int, IArrowArray> tryCast)
    {
        var leaves = new VariantValue?[rowCount];
        for (int row = 0; row < rowCount; row++)
            leaves[row] = variants.IsNull(row) ? null : Logical(variants, row);
        return CastLeaves(leaves, target, rowCount, failOnError, tryCast);
    }

    private static VariantArray RequireVariant(string name, IArrowArray array)
    {
        if (array is VariantArray variants)
            return variants;

        throw new ExpressionAnalysisException(new AnalysisDiagnostic(
            "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
            $"The first parameter of `{name}` requires the \"VARIANT\" type, however it has the type "
            + $"\"{SparkArrays.Describe(array.Data.DataType)}\"."));
    }

    /// <summary>
    /// Each row's path, null where the path is NULL. An invalid one is <c>INVALID_VARIANT_GET_PATH</c> under either
    /// form (measured). Parsed once per distinct text.
    /// </summary>
    private static VariantPath?[] ParsePaths(string name, IArrowArray argument)
    {
        if (argument is NullArray)
            return new VariantPath?[Math.Max(argument.Length, 1)];
        if (argument is not StringArray strings)
        {
            throw new ExpressionAnalysisException(new AnalysisDiagnostic(
                "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
                $"The second parameter of `{name}` requires the \"STRING\" type, however it has the type "
                + $"\"{SparkArrays.Describe(argument.Data.DataType)}\"."));
        }

        var paths = new VariantPath?[Math.Max(strings.Length, 1)];
        var parsed = new Dictionary<string, VariantPath>(StringComparer.Ordinal);
        for (int i = 0; i < strings.Length; i++)
        {
            if (strings.IsNull(i))
                continue;
            string text = strings.GetString(i)!;
            if (!parsed.TryGetValue(text, out var path))
            {
                if (!VariantPath.TryParse(text, out path))
                {
                    throw new SparkEvaluationException(
                        "INVALID_VARIANT_GET_PATH",
                        $"The path `{text}` is not a valid variant extraction path in `{name}`. A valid path should "
                        + "start with `$` and is followed by zero or more segments like `[123]`, `.name`, "
                        + "`['name']`, or `[\"name\"]`.");
                }
                parsed.Add(text, path);
            }
            paths[i] = path;
        }
        return paths;
    }

    /// <summary>
    /// The type argument: a non-null string, the same throughout. Spark requires a foldable one, which the evaluator
    /// has already checked; a NULL is refused as Spark refuses <c>cast(null as string)</c> there.
    /// </summary>
    private static string TypeText(IArrowArray argument)
    {
        string? text = argument is StringArray strings && strings.Length > 0 && !strings.IsNull(0)
            ? strings.GetString(0)
            : null;
        for (int i = 1; text is not null && i < argument.Length; i++)
        {
            if (argument.IsNull(i) || !string.Equals(((StringArray)argument).GetString(i), text, StringComparison.Ordinal))
                text = null;
        }
        return text ?? throw new ExpressionAnalysisException(NonStringLiteralType);
    }

    /// <summary>Spark's refusal of a type argument that is not a non-null string constant (measured).</summary>
    internal static AnalysisDiagnostic NonStringLiteralType { get; } = new(
        "INVALID_SCHEMA.NON_STRING_LITERAL",
        "The input schema is not a valid schema string. The input expression must be string literal and not null.");

    /// <summary>The whole variant at <paramref name="row"/>, which must not be a SQL NULL.</summary>
    private static VariantValue Logical(VariantArray variants, int row) =>
        variants.IsShredded ? variants.GetLogicalVariantValue(row) : variants.GetVariantValue(row);

    /// <summary>
    /// The value at <paramref name="path"/> in row <paramref name="row"/>, or false when the row is a SQL NULL
    /// or the path is not there: a missing key or element, or a step through a value of the wrong kind.
    /// </summary>
    private static bool TryNavigate(VariantArray variants, int row, VariantPath path, out VariantValue leaf)
    {
        leaf = default;
        if (variants.IsNull(row))
            return false;

        if (variants.IsShredded)
            return TryNavigate(variants.GetLogicalVariantValue(row), path, out leaf);

        // Unshredded: walk the encoded bytes and materialize only what the path selects.
        var reader = new VariantReader(variants.GetMetadataBytes(row), variants.GetValueBytes(row));
        foreach (var segment in path.Segments)
        {
            if (segment.Key is { } key)
            {
                if (!reader.IsObject)
                    return false;
                var fields = new VariantObjectReader(reader.Metadata, reader.Value);
                if (!fields.TryGetField(key, out reader))
                    return false;
            }
            else
            {
                if (!reader.IsArray)
                    return false;
                var elements = new VariantArrayReader(reader.Metadata, reader.Value);
                if (segment.Index >= elements.ElementCount)
                    return false;
                reader = elements.GetElement(segment.Index);
            }
        }

        leaf = reader.ToVariantValue();
        return true;
    }

    private static bool TryNavigate(VariantValue value, VariantPath path, out VariantValue leaf)
    {
        leaf = default;
        foreach (var segment in path.Segments)
        {
            if (segment.Key is { } key)
            {
                if (!value.IsObject || !value.AsObject().TryGetValue(key, out value))
                    return false;
            }
            else
            {
                if (!value.IsArray)
                    return false;
                var elements = value.AsArray();
                if (segment.Index >= elements.Count)
                    return false;
                value = elements[segment.Index];
            }
        }

        leaf = value;
        return true;
    }

    private static VariantArray BuildVariants(VariantValue?[] leaves)
    {
        var builder = new VariantArray.Builder();
        foreach (var leaf in leaves)
            builder.Append(leaf);
        return builder.Build();
    }

    /// <summary>
    /// Casts each leaf to <paramref name="target"/>, grouping the rows by the Spark type their value reads as so
    /// that each group is one cast over a column rather than a cast per row.
    /// </summary>
    private static IArrowArray CastLeaves(
        VariantValue?[] leaves,
        IArrowType target,
        int rowCount,
        bool failOnError,
        Func<IArrowArray, IArrowType, int, IArrowArray> tryCast)
    {
        var groups = new Dictionary<string, SourceGroup>(StringComparer.Ordinal);
        var invalid = new List<int>();

        for (int row = 0; row < rowCount; row++)
        {
            if (leaves[row] is not { } leaf || leaf.IsNull)
                continue;

            if (leaf.IsObject || leaf.IsArray)
            {
                if (target is StringType)
                {
                    throw new NotSupportedException(
                        "cast of a variant object or array to STRING (its JSON text) is not implemented.");
                }
                invalid.Add(row);
                continue;
            }

            var source = SourceOf(leaf);
            if (!groups.TryGetValue(source.Key, out var group))
                groups.Add(source.Key, group = new SourceGroup(source.Type));
            group.Rows.Add(row);
            group.Values.Add(source.Value);
        }

        // Each row's place in the concatenation of the groups' answers; -1 for a NULL answer.
        var take = new int[rowCount];
        for (int i = 0; i < rowCount; i++) take[i] = -1;

        var parts = new List<IArrowArray>();
        int offset = 0;
        foreach (var group in groups.Values)
        {
            if (!AnsiCastable(group.Type, target))
            {
                invalid.AddRange(group.Rows);
                continue;
            }

            var cast = tryCast(group.Build(), target, group.Rows.Count);
            for (int i = 0; i < group.Rows.Count; i++)
            {
                if (cast.IsNull(i))
                    invalid.Add(group.Rows[i]);
                else
                    take[group.Rows[i]] = offset + i;
            }
            parts.Add(cast);
            offset += cast.Length;
        }

        if (failOnError && invalid.Count > 0)
        {
            int first = invalid.Min();
            throw InvalidVariantCast(leaves[first]!.Value, target);
        }

        // The NULL every unanswered row takes.
        parts.Add(ArrowCompute.MakeNullArray(target, 1));
        for (int i = 0; i < rowCount; i++)
        {
            if (take[i] < 0)
                take[i] = offset;
        }

        var all = parts.Count == 1 ? parts[0] : ArrowCompute.Concatenate(parts);
        return ArrowCompute.Take(all, take);
    }

    /// <summary>
    /// Whether Spark's <c>Cast.canAnsiCast</c> allows the pair, for the source types a variant scalar reads as.
    /// A pair it refuses is an invalid variant cast at every row, rather than a refusal of the expression.
    /// </summary>
    private static bool AnsiCastable(IArrowType source, IArrowType target)
    {
        if (target is StringType || source is StringType)
            return true;
        if (source.TypeId == target.TypeId && source is not TimestampType)
            return true;

        bool numericTarget = SparkNumericTypes.IsNumeric(target);
        return source switch
        {
            BinaryType => target is BinaryType,
            BooleanType => numericTarget,
            Date32Type => target is TimestampType,
            // A number for a zoned timestamp only: Spark refuses one for TIMESTAMP_NTZ.
            TimestampType ts => target is TimestampType or Date32Type
                || (numericTarget && SparkArrays.IsZonedTimestamp(ts)),
            _ when SparkNumericTypes.IsNumeric(source) => numericTarget || target is BooleanType or TimestampType,
            _ => false,
        };
    }

    private static SparkEvaluationException InvalidVariantCast(VariantValue value, IArrowType target) =>
        new("INVALID_VARIANT_CAST",
            $"The variant value `{Render(value)}` cannot be cast into `\"{SparkArrays.Describe(target)}\"`. "
            + "Please use `try_variant_get` instead.");

    /// <summary>The value for an error message; close to Spark's JSON text, but only a message reads it.</summary>
    private static string Render(VariantValue value) => value.PrimitiveType switch
    {
        VariantPrimitiveType.String => "\"" + value.AsString() + "\"",
        VariantPrimitiveType.Decimal4 or VariantPrimitiveType.Decimal8 or VariantPrimitiveType.Decimal16 =>
            value.AsSqlDecimal().ToString(),
        VariantPrimitiveType.Double => value.AsDouble().ToString("R", Invariant),
        VariantPrimitiveType.Float => value.AsFloat().ToString("R", Invariant),
        _ => value.ToString() ?? "",
    };

    /// <summary>A variant scalar as the Spark value <c>VariantGet.cast</c> reads it as.</summary>
    private readonly record struct Source(string Key, IArrowType Type, object Value);

    private static readonly TimestampType Timestamp = SparkArrays.Timestamp;

    private static Source SourceOf(VariantValue value)
    {
        switch (value.PrimitiveType)
        {
            case VariantPrimitiveType.BooleanTrue or VariantPrimitiveType.BooleanFalse:
                return new Source("boolean", BooleanType.Default, value.AsBoolean());

            // Every integer width is a LONG to Spark's variant reader (`Variant.getLong`).
            case VariantPrimitiveType.Int8:
                return new Source("bigint", Int64Type.Default, (long)value.AsInt8());
            case VariantPrimitiveType.Int16:
                return new Source("bigint", Int64Type.Default, (long)value.AsInt16());
            case VariantPrimitiveType.Int32:
                return new Source("bigint", Int64Type.Default, (long)value.AsInt32());
            case VariantPrimitiveType.Int64:
                return new Source("bigint", Int64Type.Default, value.AsInt64());

            case VariantPrimitiveType.Double:
                return new Source("double", DoubleType.Default, value.AsDouble());
            case VariantPrimitiveType.Float:
                return new Source("float", FloatType.Default, value.AsFloat());

            case VariantPrimitiveType.Decimal4 or VariantPrimitiveType.Decimal8 or VariantPrimitiveType.Decimal16:
            {
                var (decimalValue, type) = NarrowestDecimal(value.AsSqlDecimal());
                return new Source(
                    string.Format(Invariant, "decimal({0},{1})", type.Precision, type.Scale), type, decimalValue);
            }

            case VariantPrimitiveType.Date:
                return new Source("date", Date32Type.Default, value.AsDateDays());
            case VariantPrimitiveType.Timestamp:
                return new Source("timestamp", Timestamp, value.AsTimestampMicros());
            case VariantPrimitiveType.TimestampNtz:
                return new Source("timestamp_ntz", SparkArrays.NaiveTimestamp, value.AsTimestampNtzMicros());

            case VariantPrimitiveType.String:
                return new Source("string", StringType.Default, value.AsString());
            case VariantPrimitiveType.Binary:
                return new Source("binary", BinaryType.Default, value.AsBinary());

            // Spark reads a UUID as its canonical text.
            case VariantPrimitiveType.Uuid:
                return new Source("string", StringType.Default, value.AsUuid().ToString("D"));

            default:
                throw new NotSupportedException(
                    $"cast of a variant {value.PrimitiveType} value is not implemented.");
        }
    }

    /// <summary>
    /// The decimal as Spark's <c>Variant.getDecimal</c> reads it: trailing fractional zeros stripped, typed as the
    /// narrowest DECIMAL that holds it (a precision of at least the scale, and at least 1).
    /// </summary>
    private static (SqlDecimal Value, Decimal128Type Type) NarrowestDecimal(SqlDecimal value)
    {
        int[] data = value.Data;
        var unscaled = new BigInteger(unchecked((uint)data[0]))
            | (new BigInteger(unchecked((uint)data[1])) << 32)
            | (new BigInteger(unchecked((uint)data[2])) << 64)
            | (new BigInteger(unchecked((uint)data[3])) << 96);
        int scale = value.Scale;
        while (scale > 0 && !unscaled.IsZero && unscaled % 10 == 0)
        {
            unscaled /= 10;
            scale--;
        }
        if (unscaled.IsZero)
            scale = 0;

        int digits = unscaled.IsZero ? 1 : BigInteger.Abs(unscaled).ToString(Invariant).Length;
        int precision = Math.Max(Math.Max(digits, scale), 1);

        var bytes = new int[4];
        var magnitude = unscaled;
        for (int i = 0; i < 4; i++)
        {
            bytes[i] = unchecked((int)(uint)(magnitude & uint.MaxValue));
            magnitude >>= 32;
        }

        var stripped = new SqlDecimal((byte)precision, (byte)scale, value.IsPositive, bytes);
        return (stripped, new Decimal128Type(precision, scale));
    }

    /// <summary>The rows whose values read as one Spark type, and those values.</summary>
    private sealed class SourceGroup
    {
        public SourceGroup(IArrowType type) => Type = type;

        public IArrowType Type { get; }

        public List<int> Rows { get; } = new();

        public List<object> Values { get; } = new();

        public IArrowArray Build()
        {
            switch (Type)
            {
                case BooleanType:
                {
                    var b = new BooleanArray.Builder();
                    foreach (var v in Values) b.Append((bool)v);
                    return b.Build();
                }
                case Int64Type:
                {
                    var b = new Int64Array.Builder();
                    foreach (var v in Values) b.Append((long)v);
                    return b.Build();
                }
                case DoubleType:
                {
                    var b = new DoubleArray.Builder();
                    foreach (var v in Values) b.Append((double)v);
                    return b.Build();
                }
                case FloatType:
                {
                    var b = new FloatArray.Builder();
                    foreach (var v in Values) b.Append((float)v);
                    return b.Build();
                }
                case Decimal128Type d:
                {
                    var b = new Decimal128Array.Builder(d);
                    foreach (var v in Values) b.Append((SqlDecimal)v);
                    return b.Build();
                }
                case Date32Type:
                {
                    var b = new Date32Array.Builder();
                    foreach (var v in Values) b.Append(DateTimeOffset.FromUnixTimeSeconds(86400L * (int)v).UtcDateTime);
                    return b.Build();
                }
                case TimestampType t:
                {
                    var b = new TimestampArray.Builder(t);
                    foreach (var v in Values) b.Append(FromMicros((long)v));
                    return b.Build();
                }
                case StringType:
                {
                    var b = new StringArray.Builder();
                    foreach (var v in Values) b.Append((string)v);
                    return b.Build();
                }
                case BinaryType:
                {
                    var b = new BinaryArray.Builder();
                    foreach (var v in Values) b.Append(((byte[])v).AsSpan());
                    return b.Build();
                }
                default:
                    throw new InvalidOperationException($"No variant source column for {Type}.");
            }
        }

        private static DateTimeOffset FromMicros(long micros) =>
            DateTimeOffset.FromUnixTimeMilliseconds(0).AddTicks(checked(micros * 10));
    }
}

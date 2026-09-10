// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// Builds the Arrow frame a corpus section was harvested against, and compares one of Spark's
/// recorded answers against ours.
/// </summary>
/// <remarks>
/// Extracted from <see cref="SparkEvaluationCorpusTests"/> when a SECOND consumer appeared:
/// <c>SparkFuzzTriage</c> reads a generated corpus rather than the checked-in one, and every
/// subtlety below is one it must not get differently. The list is short and every entry was paid
/// for -- an exact float comparison with no tolerance (#202), a decimal compared as an unscaled
/// integer because the fixture holds Python's rendering, NaN and the infinities arriving as the
/// names Java prints because they have no JSON form. A second copy of this logic would be a
/// second set of answers to the same question.
/// </remarks>
internal static class CorpusEvaluation
{
    /// <summary>Builds the frame one corpus section was harvested against.</summary>
    /// <remarks>
    /// Takes its schema and rows rather than reading the root's, because the corpus has more than
    /// one: <c>identifier_case</c> carries a schema of its own, since names that differ only in
    /// case cannot be added to the main one without making every reference to <c>a</c> ambiguous.
    /// </remarks>
    public static RecordBatch BuildBatch(JsonElement schemaElement, JsonElement rows)
    {
        var schema = new Schema.Builder();
        var arrays = new List<IArrowArray>();
        var rowCount = rows.GetArrayLength();

        var fieldIndex = 0;
        foreach (var field in schemaElement.EnumerateArray())
        {
            var name = field.GetProperty("name").GetString()!;
            var spark = field.GetProperty("type").GetString()!;
            var index = fieldIndex++;

            var literals = new string[rowCount];
            for (var r = 0; r < rowCount; r++)
                literals[r] = rows[r][index].GetString()!;

            var array = BuildColumn(spark, literals);
            if (array is null)
                continue;   // a type the evaluator does not model; expressions using it are reported

            schema.Field(new Field(name, array.Data.DataType, true));
            arrays.Add(array);
        }

        return new RecordBatch(schema.Build(), arrays, rowCount);
    }

    /// <summary>Builds one column from the corpus's SQL literal text, or null for a type we skip.</summary>
    public static IArrowArray? BuildColumn(string spark, string[] literals)
    {
        if (spark.StartsWith("decimal(", StringComparison.Ordinal))
        {
            var inner = spark.Substring("decimal(".Length, spark.Length - "decimal(".Length - 1);
            var parts = inner.Split(',');
            return BuildDecimal(
                new Decimal128Type(int.Parse(parts[0].Trim()), int.Parse(parts[1].Trim())), literals);
        }

        switch (spark)
        {
            case "int":
            {
                var b = new Int32Array.Builder();
                foreach (var v in literals)
                {
                    if (IsNull(v)) b.AppendNull();
                    else b.Append(int.Parse(Unquote(v), Invariant));
                }

                return b.Build();
            }

            case "bigint":
            {
                var b = new Int64Array.Builder();
                foreach (var v in literals)
                {
                    if (IsNull(v)) b.AppendNull();
                    else b.Append(long.Parse(Unquote(v), Invariant));
                }

                return b.Build();
            }

            case "smallint":
            {
                var b = new Int16Array.Builder();
                foreach (var v in literals)
                {
                    if (IsNull(v)) b.AppendNull();
                    else b.Append(short.Parse(Unquote(v), Invariant));
                }

                return b.Build();
            }

            case "float":
            {
                var b = new FloatArray.Builder();
                foreach (var v in literals)
                {
                    if (IsNull(v)) b.AppendNull();
                    else b.Append(float.Parse(Unquote(v), Invariant));
                }

                return b.Build();
            }

            case "double":
            {
                var b = new DoubleArray.Builder();
                foreach (var v in literals)
                {
                    if (IsNull(v)) b.AppendNull();
                    else b.Append(double.Parse(Unquote(v), Invariant));
                }

                return b.Build();
            }

            case "string":
            {
                var b = new StringArray.Builder();
                foreach (var v in literals)
                {
                    if (IsNull(v)) b.AppendNull();
                    else b.Append(Unquote(v));
                }

                return b.Build();
            }

            case "boolean":
            {
                var b = new BooleanArray.Builder();
                foreach (var v in literals)
                {
                    if (IsNull(v)) b.AppendNull();
                    else b.Append(bool.Parse(Unquote(v)));
                }

                return b.Build();
            }

            case "date":
            {
                var b = new Date32Array.Builder();
                foreach (var v in literals)
                {
                    if (IsNull(v)) b.AppendNull();
                    else b.Append(DateTimeOffset.Parse(Unquote(v), Invariant, Utc));
                }

                return b.Build();
            }

            case "timestamp":
            {
                var b = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
                foreach (var v in literals)
                {
                    if (IsNull(v)) b.AppendNull();
                    else b.Append(DateTimeOffset.Parse(Unquote(v), Invariant, Utc));
                }

                return b.Build();
            }

            // The corpus records a binary EXPRESSION as a Python bytearray repr, which is why
            // `X'ABCD'` is excluded -- but a binary COLUMN is only ever an operand, and an
            // expression over one answers with something comparable. `s = bin` is a boolean.
            case "binary":
            {
                var b = new BinaryArray.Builder();
                foreach (var v in literals)
                {
                    if (IsNull(v)) b.AppendNull();
                    else b.Append(HexBytes(Unquote(v.Substring(1))));
                }

                return b.Build();
            }

            // struct: recorded as a Python repr, and not modelled by the evaluator either.
            default:
                return null;
        }
    }

    /// <summary>Reads the corpus's <c>X'00'</c> form, with the prefix and quotes already off.</summary>
    public static byte[] HexBytes(string hex)
    {
        var bytes = new byte[hex.Length / 2];
        for (var i = 0; i < bytes.Length; i++)
            bytes[i] = byte.Parse(hex.Substring(i * 2, 2), NumberStyles.HexNumber, Invariant);

        return bytes;
    }

    public static Decimal128Array BuildDecimal(Decimal128Type type, string[] literals)
    {
        var buffer = new byte[literals.Length * 16];
        var validity = new ArrowBuffer.BitmapBuilder();
        var nulls = 0;

        for (var i = 0; i < literals.Length; i++)
        {
            if (IsNull(literals[i]))
            {
                validity.Append(false);
                nulls++;
                continue;
            }

            validity.Append(true);
            var unscaled = ParseUnscaled(Unquote(literals[i]), type.Scale);
            var bytes = unscaled.ToByteArray();
            if (unscaled.Sign < 0)
                for (var k = 0; k < 16; k++) buffer[(i * 16) + k] = 0xFF;

            System.Array.Copy(bytes, 0, buffer, i * 16, Math.Min(bytes.Length, 16));
        }

        return new Decimal128Array(new ArrayData(
            type, literals.Length, nulls, 0, new[] { validity.Build(), new ArrowBuffer(buffer) }));
    }

    /// <summary>Reads decimal text as an unscaled integer at <paramref name="scale"/>.</summary>
    public static BigInteger ParseUnscaled(string text, int scale)
    {
        var (mantissa, exponent) = SplitDecimal(text);
        var shift = scale + exponent;

        if (shift >= 0)
            return mantissa * BigInteger.Pow(10, shift);

        return mantissa / BigInteger.Pow(10, -shift);
    }

    /// <summary>
    /// Decimal text as a mantissa and a base-10 exponent, covering the exponent form Python's
    /// <c>str(Decimal)</c> emits for zero at scale — <c>0E-9</c>.
    /// </summary>
    public static (BigInteger Mantissa, int Exponent) SplitDecimal(string text)
    {
        var exponent = 0;
        var e = text.IndexOfAny(new[] { 'e', 'E' });
        if (e >= 0)
        {
            exponent = int.Parse(text.Substring(e + 1), Invariant);
            text = text.Substring(0, e);
        }

        var dot = text.IndexOf('.');
        if (dot >= 0)
        {
            exponent -= text.Length - dot - 1;
            text = text.Remove(dot, 1);
        }

        return (BigInteger.Parse(text, Invariant), exponent);
    }

    public static bool IsNull(string literal) =>
        literal.Equals("NULL", StringComparison.OrdinalIgnoreCase);

    public static string Unquote(string literal) =>
        literal.Length >= 2 && literal[0] == '\'' && literal[literal.Length - 1] == '\''
            ? literal.Substring(1, literal.Length - 2)
            : literal;

    public static CultureInfo Invariant => CultureInfo.InvariantCulture;

    public static DateTimeStyles Utc =>
        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal;

    /// <summary>Compares one cell against Spark's recorded value, or returns why it differs.</summary>
    public static string? Compare(JsonElement expected, IArrowArray actual, int row)
    {
        var isNull = actual.IsNull(row);

        if (expected.ValueKind == JsonValueKind.Null)
            return isNull ? null : $"expected null, got {Show(actual, row)}";

        if (isNull)
            return $"expected {expected}, got null";

        switch (actual)
        {
            case BooleanArray a:
                return expected.GetBoolean() == a.GetValue(row)!.Value
                    ? null
                    : $"expected {expected}, got {a.GetValue(row)}";

            case StringArray a:
                return string.Equals(expected.GetString(), a.GetString(row), StringComparison.Ordinal)
                    ? null
                    : $"expected {expected}, got '{a.GetString(row)}'";

            // AFTER StringArray, which it derives from in Apache.Arrow -- putting it first would
            // compare every string against a hex reading of its UTF-8. The fixture records binary
            // as HEX rather than as Python's bytearray repr (#295), which is what makes a binary
            // answer comparable at all; before that the two binary rows were excluded outright.
            case BinaryArray a:
            {
                var got = BitConverter.ToString(a.GetBytes(row).ToArray()).Replace("-", string.Empty);
                return string.Equals(expected.GetString(), got, StringComparison.OrdinalIgnoreCase)
                    ? null
                    : $"expected {expected}, got X'{got}'";
            }

            case Decimal128Array a:
            {
                // Numeric comparison, not textual: the fixture holds Python's rendering.
                var type = (Decimal128Type)a.Data.DataType;
                var want = ParseUnscaled(expected.GetString()!, type.Scale);
                var got = SparkWideDecimals.Read(a, row)!.Value.Unscaled;
                return want.ToString() == got.ToString()
                    ? null
                    : $"expected unscaled {want}, got {got} (at scale {type.Scale})";
            }

            case Date32Array a:
            {
                var got = a.GetDateTimeOffset(row)!.Value.ToString("yyyy-MM-dd", Invariant);
                return string.Equals(expected.GetString(), got, StringComparison.Ordinal)
                    ? null
                    : $"expected {expected}, got '{got}'";
            }

            case DoubleArray a:
                return SameDouble(ExpectedDouble(expected), a.GetValue(row)!.Value)
                    ? null
                    : $"expected {expected}, got {a.GetValue(row)}";

            case FloatArray a:
                return SameDouble(ExpectedDouble(expected), a.GetValue(row)!.Value)
                    ? null
                    : $"expected {expected}, got {a.GetValue(row)}";

            case Int64Array a:
                return expected.GetInt64() == a.GetValue(row)!.Value
                    ? null
                    : $"expected {expected}, got {a.GetValue(row)}";

            case Int32Array a:
                return expected.GetInt64() == a.GetValue(row)!.Value
                    ? null
                    : $"expected {expected}, got {a.GetValue(row)}";

            case Int16Array a:
                return expected.GetInt64() == a.GetValue(row)!.Value
                    ? null
                    : $"expected {expected}, got {a.GetValue(row)}";

            // TINYINT arrived with #243's integral-cast group. Without it the comparison reported
            // "no comparison for int8", which reads like a limit of the fixture and was a hole in
            // the harness.
            case Int8Array a:
                return expected.GetInt64() == a.GetValue(row)!.Value
                    ? null
                    : $"expected {expected}, got {a.GetValue(row)}";

            default:
                return $"no comparison for {actual.Data.DataType.Name}";
        }
    }

    /// <summary>
    /// A recorded floating-point answer, which is a JSON number unless it has no JSON form.
    /// </summary>
    /// <remarks>
    /// NaN and the infinities are legitimate Spark answers and are not JSON values, so the
    /// harvest records them as the names Java prints. Writing them as bare tokens is what Python
    /// does by default, and it made the whole fixture unreadable here — the failure was the
    /// corpus not loading at all rather than one comparison going wrong.
    /// </remarks>
    public static double ExpectedDouble(JsonElement expected) =>
        expected.ValueKind == JsonValueKind.String
            ? expected.GetString() switch
            {
                "NaN" => double.NaN,
                "Infinity" => double.PositiveInfinity,
                "-Infinity" => double.NegativeInfinity,
                var other => double.Parse(other!, Invariant),
            }
            : expected.GetDouble();

    /// <summary>
    /// Whether a recorded floating-point answer and ours are the SAME double. No tolerance.
    /// </summary>
    /// <remarks>
    /// This used to allow a relative 1e-12, justified as slack for Spark's answer arriving through
    /// Python's <c>repr</c> of a float. #202 measured the justification and it does not hold:
    /// <c>repr</c> is the shortest round-tripping form, so parsing it back returns the identical
    /// double, and running the whole corpus with the tolerance set to ZERO produced exactly one
    /// difference on each of net10.0, net8.0 and net472 — the wide-decimal cast #202 is about.
    /// Every other recorded float agrees bit for bit, on all three targets.
    /// <para>
    /// It also has to be zero rather than merely tighter. That one difference is <b>one ulp</b>,
    /// so a tolerance "sized to a few ulps" would swallow it just as 1e-12 did. There is no band
    /// between "catches this" and "no tolerance at all".
    /// </para>
    /// </remarks>
    public static bool SameDouble(double expected, double actual)
    {
        // double.Equals rather than ==, because it settles NaN against NaN as equal — which is
        // the answer a value oracle wants — and distinguishes nothing else that matters here.
        // Everything the old tolerance had to special-case (an infinity making the tolerance
        // infinite, so that our overflow to infinity compared equal to Spark's finite answer)
        // stops existing once there is no tolerance to compute.
        return expected.Equals(actual);
    }

    public static string Show(IArrowArray array, int row) => array switch
    {
        BooleanArray a => a.GetValue(row)?.ToString() ?? "null",
        StringArray a => $"'{a.GetString(row)}'",
        Decimal128Array a => SparkWideDecimals.Render(SparkWideDecimals.Read(a, row)!.Value),
        Int8Array a => a.GetValue(row)?.ToString() ?? "null",
        _ => "value",
    };
}

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
    /// <remarks>
    /// <b>A cell whose SHAPE disagrees is a difference, not a crash.</b> Every branch below reads
    /// the fixture through the accessor its own array type implies — a <c>StringArray</c> asks
    /// for <c>GetString</c>, a <c>BooleanArray</c> for <c>GetBoolean</c> — and
    /// <c>System.Text.Json</c> THROWS when the recorded value is of another kind. That is exactly
    /// the case where we produced the wrong TYPE, which is the most interesting failure the
    /// corpus can find, and it arrived as an exception from inside the harness with no expression
    /// named. TWO exception types, which is why the catch names both: a kind the accessor refuses
    /// outright throws <c>InvalidOperationException</c>, while a NUMBER it can read but not fit —
    /// Spark's <c>1.0</c> through <c>GetInt64</c> — throws <c>FormatException</c>. Measured on
    /// #340, where Spark answers the double 1.0 for <c>+'1'</c> and we answered the string '1'.
    /// </remarks>
    /// <exception cref="CorpusTypeMismatchException">
    /// When the recorded value and ours are not the same kind of thing.
    /// </exception>
    public static string? Compare(JsonElement expected, IArrowArray actual, int row)
    {
        try
        {
            return CompareCell(expected, actual, row);
        }
        catch (Exception ex) when (ex is InvalidOperationException or FormatException)
        {
            // RAISED, NOT RETURNED, and the distinction is a caller's rather than this method's.
            // `SparkEvaluationCorpusTests` wants a reported difference, because its fixture is
            // curated and every row is one somebody chose; `SparkFuzzTriage` wants the SIGNAL,
            // because it sorts thousands of generated rows and "we produced the wrong type" is a
            // different verdict from "we produced the wrong value" -- Verdict.TypeDiffers, which
            // reads the two types and reports them. Flattening this into a comparison string
            // would have left the fuzzer calling every type mismatch a value mismatch.
            throw new CorpusTypeMismatchException(
                $"expected {expected}, got {Show(actual, row)} ({actual.Data.DataType.Name})");
        }
    }

    private static string? CompareCell(JsonElement expected, IArrowArray actual, int row)
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

            // Compared as TEXT, like the date above it, because that is how the fixture holds it:
            // the driver renders a timestamp answer through the JVM rather than collecting it, so
            // what is recorded is Spark's own spelling in the session zone. Before #311 there was
            // no arm here at all -- a timestamp fell through to "no comparison for timestamp",
            // which nothing ever saw because the three rows that would have reached it were
            // EXCLUDED for carrying the harvest machine's zone.
            case TimestampArray a:
            {
                var got = ShowTimestamp(a.GetTimestamp(row)!.Value);
                return string.Equals(expected.GetString(), got, StringComparison.Ordinal)
                    ? null
                    : $"expected {expected}, got '{got}'";
            }

            case DoubleArray a:
                return SameDouble(ExpectedDouble(expected), a.GetValue(row)!.Value)
                    ? null
                    : $"expected {expected}, got {ShowDouble(a.GetValue(row)!.Value)}";

            case FloatArray a:
                return SameDouble(ExpectedDouble(expected), a.GetValue(row)!.Value)
                    ? null
                    : $"expected {expected}, got {ShowDouble(a.GetValue(row)!.Value)}";

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
    public static double ExpectedDouble(JsonElement expected)
    {
        var text = expected.ValueKind == JsonValueKind.String
            ? expected.GetString()!
            : expected.GetRawText();

        switch (text)
        {
            case "NaN": return double.NaN;
            case "Infinity": return double.PositiveInfinity;
            case "-Infinity": return double.NegativeInfinity;
        }

        // READ WITH THE EXACT PARSER, NOT THE PLATFORM'S, because the netstandard2.0 build of
        // System.Text.Json this test loads under net472 reads a JSON number through .NET
        // Framework's parser -- and that parser is wrong twice over. It drops the sign of a zero,
        // so the fixture's -0.0 became +0.0 on that target alone (#282); and it is an ulp out on
        // some ordinary numbers, so two recorded floats of #372's group were expected one double
        // away from the float Spark actually answered (#350's defect, on the EXPECTATION side).
        // Both times the answer was right and the expectation was wrong.
        return SparkDoubleText.TryParseExact(text, out var value)
            ? value
            : double.Parse(text, Invariant);
    }

    /// <summary>A double with the sign of a zero shown, which neither runtime's ToString does.</summary>
    /// <remarks>
    /// .NET Framework prints -0.0 as "0" and .NET Core prints it as "-0", so a failure message
    /// built from ToString said "expected -0.0, got 0" on one target for an answer that was
    /// actually right. A message about a signed zero has to spell the sign itself.
    /// </remarks>
    private static string ShowDouble(double value) =>
        value == 0d && BitConverter.DoubleToInt64Bits(value) < 0
            ? "-0"
            : value.ToString("R", Invariant);

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
        // Every NaN is one NaN. Spark's answer arrives as the text "NaN" and carries no payload,
        // and a NaN that has been through unary minus has its sign bit set — so comparing NaN
        // by its bits would report a difference where both sides render "NaN" and Spark makes no
        // distinction either.
        if (double.IsNaN(expected))
            return double.IsNaN(actual);

        // BY THE BITS, not by ==, which holds -0.0 equal to 0.0 and so could not see #282 at all:
        // the whole corpus agreed on the VALUE of `-(0.0D)` while we answered the wrong zero, and
        // only the `CAST(... AS STRING)` channel noticed. A value oracle that cannot distinguish
        // two values Spark renders differently is not measuring the value.
        return BitConverter.DoubleToInt64Bits(expected) == BitConverter.DoubleToInt64Bits(actual);
    }

    /// <remarks>
    /// The numeric widths are listed rather than left to the fallback because <see cref="Compare"/>
    /// now reports a SHAPE disagreement through this, and "expected 2.0, got value" names neither
    /// what we produced nor what is wrong with it. Anything still unlisted keeps the fallback,
    /// which the type name beside it makes readable enough.
    /// </remarks>
    public static string Show(IArrowArray array, int row) => array switch
    {
        BooleanArray a => a.GetValue(row)?.ToString() ?? "null",

        StringArray a => a.IsNull(row) ? "null" : $"'{a.GetString(row)}'",

        // AFTER StringArray, which DERIVES from BinaryArray in Apache.Arrow -- the same ordering
        // Compare carries, and putting it first makes the string arm unreachable (the compiler
        // says so, which is how this was caught rather than by rendering a string as hex).
        BinaryArray a => a.IsNull(row)
            ? "null"
            : $"X'{BitConverter.ToString(a.GetBytes(row).ToArray()).Replace("-", string.Empty)}'",
        Decimal128Array a => SparkWideDecimals.Render(SparkWideDecimals.Read(a, row)!.Value),
        Int8Array a => a.GetValue(row)?.ToString() ?? "null",
        Int16Array a => a.GetValue(row)?.ToString() ?? "null",
        Int32Array a => a.GetValue(row)?.ToString() ?? "null",
        Int64Array a => a.GetValue(row)?.ToString() ?? "null",
        FloatArray a => a.GetValue(row) is { } f ? ShowDouble(f) : "null",
        DoubleArray a => a.GetValue(row) is { } d ? ShowDouble(d) : "null",
        Date32Array a => a.GetDateTimeOffset(row)?.ToString("yyyy-MM-dd", Invariant) ?? "null",
        TimestampArray a => a.GetTimestamp(row) is { } t ? ShowTimestamp(t) : "null",
        _ => "value",
    };

    /// <summary>An instant spelled the way Spark's timestamp-to-string cast spells it.</summary>
    /// <remarks>
    /// Seconds always, and a fractional part only when there is one — Spark renders
    /// <c>2026-08-11 12:30:00</c> and <c>2026-09-15 18:15:36.045894</c>, never a padded
    /// <c>.000000</c>. Written out here rather than borrowed from the evaluator's own cast on
    /// purpose: a comparison that formatted through the code under test would agree with itself
    /// however wrong both were.
    /// </remarks>
    private static string ShowTimestamp(DateTimeOffset instant)
    {
        var utc = instant.ToUniversalTime();
        var text = utc.ToString("yyyy-MM-dd HH:mm:ss", Invariant);
        var fraction = utc.ToString("ffffff", Invariant).TrimEnd('0');
        return fraction.Length == 0 ? text : text + "." + fraction;
    }
}

/// <summary>
/// A recorded cell and ours are not the same KIND of thing — Spark answered a number and we
/// produced a string, or the reverse.
/// </summary>
/// <remarks>
/// A type of its own so that a caller can tell this apart from an ordinary difference without
/// reading a message. It carries the same text a difference would have, so a caller that only
/// wants to report something has nothing extra to do.
/// </remarks>
internal sealed class CorpusTypeMismatchException : Exception
{
    public CorpusTypeMismatchException(string message)
        : base(message)
    {
    }
}

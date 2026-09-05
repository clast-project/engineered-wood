// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// Evaluates every corpus expression and compares the answer against Spark's, per row.
/// </summary>
/// <remarks>
/// The corpus asks Spark three questions per expression. <c>parse</c> is checked by
/// <c>SparkExpressionCorpusTests</c> and <c>type</c> by <see cref="SparkNumericTypesTests"/>;
/// <c>eval</c> — Spark's actual per-row answers, and the third of the corpus that catches value
/// defects rather than shape defects — went unchecked until this existed. It cost #175, where
/// Spark's correct answer for <c>CAST(d4 AS STRING)</c> sat in the fixture while EngineeredWood
/// returned the text "&lt;out of range&gt;".
/// <para>
/// VALUES ARE COMPARED SEMANTICALLY, NOT AS TEXT. The driver serialises what PySpark collected
/// with <c>default=str</c>, so the fixture holds Python's rendering rather than Spark's:
/// <c>Decimal("0E-9")</c> prints as <c>0E-9</c> where Spark's own <c>CAST(… AS STRING)</c> gives
/// <c>0.000000000</c>. Comparing text would pin Python's <c>repr</c> as if it were Spark
/// behaviour, so a decimal is parsed back to a number and compared as one. See
/// <see cref="Excluded"/> for the kinds where that is not enough.
/// </para>
/// </remarks>
public sealed class SparkEvaluationCorpusTests
{
    private static readonly JsonDocument Corpus = JsonDocument.Parse(
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "spark-expression-corpus.json")));

    // The corpus pins ansi on, so the ANSI registry is the one its answers describe.
    private static readonly SparkFunctionRegistry Ansi = new();

    // ...and the `legacy` section pins it off, which is the registry THOSE answers describe.
    private static readonly SparkFunctionRegistry Legacy = new(new SparkDialectOptions { Ansi = false });

    /// <summary>
    /// Expressions whose recorded answer cannot be compared, each with the reason.
    /// </summary>
    /// <remarks>
    /// Declared rather than silently skipped, the way <c>SparkSqlParserTests</c> declares its
    /// refusals: a shrinking list is progress and a growing one is a regression, and neither is
    /// visible if the skip is implicit. Every entry here is a limit of the FIXTURE, not of
    /// EngineeredWood — see the class remarks on <c>default=str</c>.
    /// </remarks>
    private static readonly Dictionary<string, string> Excluded = new(StringComparer.Ordinal)
    {
        // PySpark converts a timestamp to a naive datetime in the DRIVER's local zone on collect,
        // so these were recorded as America/Los_Angeles despite the session pinning UTC. The
        // recorded text is a property of the machine that harvested it.
        ["CAST(dt AS TIMESTAMP)"] = "timestamp localised to the harvest machine's zone",
        ["TIMESTAMP'2026-08-11 12:30:00'"] = "timestamp localised to the harvest machine's zone",

        // str(bytearray) is Python's repr, not a value encoding.
        ["X'ABCD'"] = "binary recorded as a Python bytearray repr",
        ["x'00'"] = "binary recorded as a Python bytearray repr",

        // Frozen at harvest time; re-harvesting moves them and they are not a function of input.
        ["current_date()"] = "value is the harvest date",
        ["current_timestamp()"] = "value is the harvest instant",

        // Spark applies Java's surrogate arithmetic to a \U escape with no range check, so both
        // of these answer with UNPAIRED surrogates -- and an unpaired surrogate does not survive
        // the JVM's UTF-8 encoding on the way to the fixture, which records '?' for each one.
        // The recorded text is a property of the transport, and the part that did survive agrees:
        // '\UFFFFFFFF' is recorded as U+D7BF followed by '?', and U+D7BF is exactly what
        // SparkLiteral produces. Asserted directly instead, in SparkSqlParserTests.
        ["'" + Backslash + "U00110000'"] = "unpaired surrogates reach the fixture as '?'",
        ["'" + Backslash + "UFFFFFFFF'"] = "unpaired surrogates reach the fixture as '?'",
    };

    /// <summary>One backslash, kept out of the keys above so they stay legible.</summary>
    private const string Backslash = "\\";


    /// <summary>
    /// Expressions where EngineeredWood's answer differs from Spark's, each with the reason.
    /// </summary>
    /// <remarks>
    /// Declared, not skipped. The test asserts this set EXACTLY: an expression that starts
    /// differing without being listed is a regression, and one listed that no longer differs is a
    /// fix whose entry must be deleted. Silence in either direction is what let #175 live.
    /// <para>
    /// Three kinds are mixed here on purpose, because the list is only honest if it does not hide
    /// the difference between "we chose not to" and "we are wrong":
    /// OUT OF SCOPE (aggregates, subqueries, windows — the corpus's own group name says so),
    /// NOT IMPLEMENTED (functions and types nothing has needed yet), and
    /// DIVERGENT (we answer, Spark answers, and the answers disagree — filed as issues).
    /// </para>
    /// </remarks>
    private static readonly Dictionary<string, string> KnownDifferences = new(StringComparer.Ordinal)
    {
        // ── DIVERGENT: we and Spark both answer, and disagree. Each has an issue. ──────────────
        ["s = a"] = "#180: Spark refuses string/number comparison under ANSI; we return a value",
        ["s < a"] = "#180: Spark refuses string/number comparison under ANSI; we return a value",
        ["A"] = "#181: Spark resolves identifiers case-insensitively; we do not",

        // ── NOT IMPLEMENTED: no function or materialisation for these yet. ────────────────────
        ["round(g, 2)"] = "#182: function not registered",
        ["greatest(a, b)"] = "#182: function not registered",
        ["greatest(a, g)"] = "#182: function not registered",
        ["least(a, b)"] = "#182: function not registered",
        ["DATE'2026-08-11'"] = "#182: a date literal parses but cannot be materialised as an Arrow array",
        ["INTERVAL 1 DAY"] = "parser refuses INTERVAL literals; declared in SparkSqlParserTests",
        ["1Y"] = "parser refuses the tinyint literal suffix; declared in SparkSqlParserTests",
        ["1S"] = "parser refuses the smallint literal suffix; declared in SparkSqlParserTests",

        // ── NOT IMPLEMENTED: struct, array and map columns are not modelled. ──────────────────
        // The harness cannot build the `nested` column either, so these would be unreadable even
        // if the evaluator handled them.
        ["nested IS NULL"] = "struct columns are not modelled",
        ["nested.name IS NULL"] = "struct columns are not modelled",
        ["nested.name"] = "struct columns are not modelled",
        ["nested.`name`"] = "struct columns are not modelled",
        ["nested.arr"] = "struct columns are not modelled",
        ["nested.m"] = "struct columns are not modelled",
        ["nested.arr[0]"] = "struct columns are not modelled",
        ["nested.m['k']"] = "struct columns are not modelled",
        ["size(nested.arr)"] = "struct columns are not modelled",
        ["element_at(nested.m, 'k')"] = "struct columns are not modelled",
        ["element_at(nested.m, 'missing')"] = "struct columns are not modelled",

        // ── NOT IMPLEMENTED: raw string literals, where no escape applies. ───────────────────
        // Spark reads R'...' and r'...' and even lets them join the adjacent-literal run, so
        // R'it''s' is "its". The tokenizer has no notion of a prefixed literal -- `R` scans as an
        // identifier -- and adding one is its own change rather than part of #179.
        ["R'a" + Backslash + "nb'"] = "raw string literals are not implemented",
        ["r'a" + Backslash + "nb'"] = "raw string literals are not implemented",
        ["R'it''s'"] = "raw string literals are not implemented",

        // ── OUT OF SCOPE: a CHECK constraint evaluates one row at a time. ─────────────────────
        ["count(a)"] = "aggregate: out of scope for a per-row constraint",
        ["sum(a) > 0"] = "aggregate: out of scope for a per-row constraint",
        ["a > (SELECT 1)"] = "subquery: out of scope for a per-row constraint",
        ["a IN (SELECT 1)"] = "subquery: out of scope for a per-row constraint",
        ["rank() OVER (ORDER BY a)"] = "window function: out of scope for a per-row constraint",
    };

    /// <summary>
    /// The <see cref="Excluded"/> counterpart for the legacy section.
    /// </summary>
    /// <remarks>
    /// Empty, and separate rather than shared, because the two sections do not cover the same
    /// expressions: nothing in the legacy section is a timestamp, a binary or a harvest-time
    /// value, which is what every ANSI exclusion is. Its own list keeps the exact skip count in
    /// each test meaningful.
    /// </remarks>
    private static readonly Dictionary<string, string> LegacyExcluded = new(StringComparer.Ordinal);

    /// <summary>
    /// The <see cref="KnownDifferences"/> counterpart for the legacy section, asserted exactly.
    /// </summary>
    private static readonly Dictionary<string, string> LegacyKnownDifferences =
        new(StringComparer.Ordinal)
        {
            // The struct column the harness cannot build, exactly as in the ANSI list.
            ["nested.arr[99]"] = "struct columns are not modelled",
            ["element_at(nested.m, 'missing')"] = "struct columns are not modelled",

            // ── DIVERGENT, and both are visible ONLY here. ────────────────────────────────────
            // Under ANSI both expressions are recorded as refusals, and a refusal we answer with
            // a throw counts as agreement whatever the throw was -- so the ANSI section passes on
            // each of them for the wrong reason. Asking the same expression under a conf where
            // Spark ANSWERS is what separates "we refuse because Spark does" from "we cannot do
            // this at all".
            ["CAST(60000000000000000000000000000000000000 AS DECIMAL(38,0))"
             + " + CAST(60000000000000000000000000000000000000 AS DECIMAL(38,0))"] =
                "#173: the parser cannot read a 38-digit literal, so this never reaches evaluation",
            ["CAST(d4 AS INT)"] =
                "#243: legacy WRAPS an integral cast (10^30 -> 1073741824); we return null",
        };

    private static HashSet<string> ExpressionsIn(JsonElement groups)
    {
        var known = new HashSet<string>(StringComparer.Ordinal);
        foreach (var group in groups.EnumerateObject())
            foreach (var entry in group.Value.EnumerateArray())
                known.Add(entry.GetProperty("expression").GetString()!);

        return known;
    }

    private static RecordBatch BuildBatch()
    {
        var schema = new Schema.Builder();
        var arrays = new List<IArrowArray>();
        var rows = Corpus.RootElement.GetProperty("rows");
        var rowCount = rows.GetArrayLength();

        var fieldIndex = 0;
        foreach (var field in Corpus.RootElement.GetProperty("schema").EnumerateArray())
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
    private static IArrowArray? BuildColumn(string spark, string[] literals)
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

            // binary and struct: the corpus records both in forms that are Python reprs rather
            // than values, so nothing here could be compared against them anyway.
            default:
                return null;
        }
    }

    private static Decimal128Array BuildDecimal(Decimal128Type type, string[] literals)
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
    private static BigInteger ParseUnscaled(string text, int scale)
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
    private static (BigInteger Mantissa, int Exponent) SplitDecimal(string text)
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

    private static bool IsNull(string literal) =>
        literal.Equals("NULL", StringComparison.OrdinalIgnoreCase);

    private static string Unquote(string literal) =>
        literal.Length >= 2 && literal[0] == '\'' && literal[literal.Length - 1] == '\''
            ? literal.Substring(1, literal.Length - 2)
            : literal;

    private static CultureInfo Invariant => CultureInfo.InvariantCulture;

    private static DateTimeStyles Utc =>
        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal;

    [Fact]
    public void EveryExclusionNamesAnExpressionTheCorpusActuallyHas()
    {
        // Without this, a typo or a renamed expression turns an exclusion into a no-op and the
        // expression silently stops being compared -- the exact failure mode this whole test
        // exists to end.
        Assert.Empty(Excluded.Keys.Where(
            e => !ExpressionsIn(Corpus.RootElement.GetProperty("groups")).Contains(e)));

        Assert.Empty(LegacyExcluded.Keys.Where(
            e => !ExpressionsIn(Corpus.RootElement.GetProperty("legacy").GetProperty("groups"))
                .Contains(e)));
    }

    [Fact]
    public void EveryCorpusExpressionEvaluatesToSparksAnswer()
    {
        var differing = EvaluateSection(
            Corpus.RootElement.GetProperty("groups"), Ansi, Excluded, out var compared, out var skipped);

        Assert.True(compared > 150, $"only {compared} expressions were compared");
        Assert.Equal(Excluded.Count, skipped);
        AssertOnlyDeclaredDifferences(differing, KnownDifferences);
    }

    /// <summary>
    /// The same comparison against the corpus gathered with ANSI off, and the legacy registry.
    /// </summary>
    /// <remarks>
    /// <c>SparkDialectOptions.Ansi = false</c> selects a whole second set of answers, and until
    /// #174 asked whether a string-to-decimal cast follows the pattern the option describes, none
    /// of them had been measured — the option's own documentation was the only statement of what
    /// they were. Harvesting the ANSI-sensitive part of the corpus a second time under a second
    /// conf turns that into data.
    /// <para>
    /// The two answers are NOT the same shape: legacy nulls a decimal overflow but WRAPS an
    /// integral one, so <c>CAST(d4 AS INT)</c> is 1073741824 rather than null. "Legacy yields
    /// null" is true of this section's decimal cases and false of its integral ones.
    /// </para>
    /// </remarks>
    [Fact]
    public void EveryLegacyCorpusExpressionEvaluatesToSparksAnswer()
    {
        var legacy = Corpus.RootElement.GetProperty("legacy");

        // The section is only worth comparing against the legacy registry if it really was
        // gathered with ANSI off; a mis-harvest would otherwise read as a pile of differences.
        Assert.Equal(
            "false", legacy.GetProperty("conf").GetProperty("spark.sql.ansi.enabled").GetString());

        var differing = EvaluateSection(
            legacy.GetProperty("groups"), Legacy, LegacyExcluded, out var compared, out var skipped);

        Assert.True(compared > 60, $"only {compared} expressions were compared");
        Assert.Equal(LegacyExcluded.Count, skipped);
        AssertOnlyDeclaredDifferences(differing, LegacyKnownDifferences);
    }

    /// <summary>
    /// Evaluates every expression in one corpus section, returning those whose answer differs.
    /// </summary>
    private static Dictionary<string, string> EvaluateSection(
        JsonElement groups,
        SparkFunctionRegistry registry,
        Dictionary<string, string> excluded,
        out int compared,
        out int skipped)
    {
        var batch = BuildBatch();
        var differing = new Dictionary<string, string>(StringComparer.Ordinal);
        compared = 0;
        skipped = 0;

        foreach (var group in groups.EnumerateObject())
        {
            foreach (var entry in group.Value.EnumerateArray())
            {
                var expression = entry.GetProperty("expression").GetString()!;
                if (excluded.ContainsKey(expression))
                {
                    skipped++;
                    continue;
                }

                if (!entry.TryGetProperty("eval", out var eval))
                    continue;

                var expectedOk = eval.GetProperty("ok").GetBoolean();

                IArrowArray? actual = null;
                string? threw = null;
                try
                {
                    actual = new ArrowRowEvaluator(registry)
                        .EvaluateExpression(SparkSqlParser.ParseExpression(expression), batch);
                }
                catch (Exception ex)
                {
                    threw = $"{ex.GetType().Name}: {ex.Message}";
                }

                compared++;

                if (!expectedOk)
                {
                    // Spark refused, so refusing is the right answer and a value is the wrong one.
                    if (threw is null)
                        differing[expression] = "Spark refused, we returned a value";

                    continue;
                }

                if (threw is not null)
                {
                    differing[expression] = $"Spark answered, we threw {threw}";
                    continue;
                }

                var values = eval.GetProperty("values");
                for (var row = 0; row < values.GetArrayLength(); row++)
                {
                    var problem = Compare(values[row], actual!, row);
                    if (problem is not null && !differing.ContainsKey(expression))
                        differing[expression] = $"row {row}: {problem}";
                }
            }
        }

        return differing;
    }

    /// <summary>Asserts that the differences found are exactly the ones declared.</summary>
    private static void AssertOnlyDeclaredDifferences(
        Dictionary<string, string> differing, Dictionary<string, string> declared)
    {
        // A difference nobody declared is a regression. Reported as a joined message rather than
        // through Assert.Empty, which elides each entry after about 50 characters -- and the
        // elided part is the reason, which is the only thing that says what to do next.
        var undeclared = differing
            .Where(d => !declared.ContainsKey(d.Key))
            .Select(d => $"{d.Key} -- {d.Value}")
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList();
        Assert.True(
            undeclared.Count == 0,
            $"{undeclared.Count} undeclared difference(s):\n  " + string.Join("\n  ", undeclared));

        // A declared difference that no longer happens is a fix; delete its entry.
        var stale = declared.Keys
            .Where(e => !differing.ContainsKey(e))
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList();
        Assert.True(
            stale.Count == 0,
            $"{stale.Count} fixed difference(s) still declared:\n  " + string.Join("\n  ", stale));
    }

    /// <summary>Compares one cell against Spark's recorded value, or returns why it differs.</summary>
    private static string? Compare(JsonElement expected, IArrowArray actual, int row)
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
                return NearlyEqual(expected.GetDouble(), a.GetValue(row)!.Value)
                    ? null
                    : $"expected {expected}, got {a.GetValue(row)}";

            case FloatArray a:
                return NearlyEqual(expected.GetDouble(), a.GetValue(row)!.Value)
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

            default:
                return $"no comparison for {actual.Data.DataType.Name}";
        }
    }

    private static bool NearlyEqual(double expected, double actual)
    {
        // Exact first. This is also the only route by which two infinities of the same sign
        // compare equal, and it settles NaN against NaN — double.Equals calls those equal where
        // == does not, which is the answer a value oracle wants.
        if (expected.Equals(actual)) return true;

        // Anything non-finite that did not match exactly is a real difference, and must not reach
        // the tolerance below: with either side infinite, `scale` is infinite, the tolerance is
        // infinite, and every mismatch would compare equal. That fails in the dangerous
        // direction, because `actual` is OUR value — an overflow to infinity where Spark answered
        // a finite number is exactly the defect this harness exists to catch.
        //
        // double.IsFinite would say this in one call, and does not exist on net472.
        if (double.IsNaN(expected) || double.IsNaN(actual)) return false;
        if (double.IsInfinity(expected) || double.IsInfinity(actual)) return false;

        // Spark's answer arrives through Python's repr of a float, so the last bit can differ.
        var scale = Math.Max(Math.Abs(expected), Math.Abs(actual));
        return Math.Abs(expected - actual) <= scale * 1e-12;
    }

    [Theory]
    // The comparison oracle needs its own test: a wrong `true` here is silent, and silence is the
    // failure mode this whole file exists to remove.
    [InlineData(1.0, 1.0, true)]
    [InlineData(0.1 + 0.2, 0.3, true)]                       // last-bit drift, which is the point
    [InlineData(1.0, 1.5, false)]
    [InlineData(0.0, 0.0, true)]
    [InlineData(double.NaN, double.NaN, true)]
    [InlineData(double.NaN, 1.0, false)]
    [InlineData(double.PositiveInfinity, double.PositiveInfinity, true)]
    [InlineData(double.NegativeInfinity, double.NegativeInfinity, true)]
    // The cases that were wrong: an infinite tolerance swallowed all of them.
    [InlineData(double.PositiveInfinity, 1.0, false)]
    [InlineData(1.0, double.PositiveInfinity, false)]
    [InlineData(double.PositiveInfinity, double.NegativeInfinity, false)]
    [InlineData(double.NegativeInfinity, 0.0, false)]
    public void TheFloatComparisonToleratesDriftButNotInfinity(double expected, double actual, bool equal) =>
        Assert.Equal(equal, NearlyEqual(expected, actual));

    private static string Show(IArrowArray array, int row) => array switch
    {
        BooleanArray a => a.GetValue(row)?.ToString() ?? "null",
        StringArray a => $"'{a.GetString(row)}'",
        Decimal128Array a => SparkWideDecimals.Render(SparkWideDecimals.Read(a, row)!.Value),
        _ => "value",
    };
}

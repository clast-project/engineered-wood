// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using System.Text.Json;
using Apache.Arrow;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;
using Xunit.Abstractions;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// Triages a generated corpus from <c>fuzz_expressions.py</c> into a report of what we and Spark
/// disagree about.
/// </summary>
/// <remarks>
/// A TOOL, not a gate. <see cref="SparkEvaluationCorpusTests"/> is the gate: it reads the
/// checked-in fixture, needs no Spark, and asserts that the differences are exactly the declared
/// ones. This reads a corpus that is not checked in, cannot run in CI, and asserts almost
/// nothing — its output is the report. Findings are minimised by hand and promoted into the
/// fixture as a named group, which is what turns one into a regression test.
/// <para>
/// It is skipped unless <c>EW_FUZZ_CORPUS</c> points at a corpus, so it costs a skipped test in
/// every other run:
/// </para>
/// <code>
/// EW_FUZZ_CORPUS=/tmp/ew-fuzz/fuzz-expression-corpus.json \
///     dotnet test --filter SparkFuzzTriage -f net10.0
/// </code>
/// <para>
/// WHY IT CLASSIFIES RATHER THAN COUNTS. Every expression is asked twice — for its value and for
/// Spark's own <c>CAST(… AS STRING)</c> rendering of that value — and the pair of answers is what
/// makes a difference attributable. A wrong renderer and a wrong computation both surface as
/// "the answers differ", and they are different bugs with different fixes; without the second
/// answer one broken renderer is reported once per template that happens to produce a double.
/// See the module docstring in <c>fuzz_expressions.py</c>.
/// </para>
/// </remarks>
public sealed class SparkFuzzTriage
{
    private readonly ITestOutputHelper _output;

    public SparkFuzzTriage(ITestOutputHelper output) => _output = output;

    private static string? CorpusPath => Environment.GetEnvironmentVariable("EW_FUZZ_CORPUS");

    /// <summary>How a single expression's two answers compare. Ordered worst-first.</summary>
    private enum Verdict
    {
        /// <summary>We computed a different value, and our rendering differs too.</summary>
        ValueDiffers,

        /// <summary>
        /// Spark's answer and ours are not the same KIND of value, so no comparison is possible.
        /// </summary>
        /// <remarks>
        /// Only a generated corpus reaches this. The curated fixture never does, because every
        /// expression in it was added alongside an answer we already produced the right type for
        /// -- so <see cref="CorpusEvaluation.Compare"/> is free to assume the recorded JSON kind
        /// matches our array, and it throws rather than reports when it does not. A result-type
        /// divergence is a defect in its own right (it is how the `greatest` scale bug shows up),
        /// so it is caught and named here rather than papered over.
        /// </remarks>
        TypeDiffers,

        /// <summary>The value agrees and only the rendering differs: a spelling defect.</summary>
        TextDiffers,

        /// <summary>Spark answered and we threw.</summary>
        WeThrew,

        /// <summary>Spark refused and we produced a value — the dangerous direction.</summary>
        WeAnsweredWhereSparkRefused,

        /// <summary>
        /// The value differs but the rendering agrees, which cannot both be true of a correct
        /// comparison: the fixture's Python-rendered value is lossy for this type.
        /// </summary>
        ComparisonUnreliable,

        /// <summary>
        /// The corpus cannot answer: Spark's typed value never survived the trip, or it is a
        /// type the comparison has no case for. A limit of the harness, not a defect.
        /// </summary>
        Unusable,

        /// <summary>Nothing to report.</summary>
        Agree,
    }

    /// <summary>One expression's verdict.</summary>
    /// <remarks>
    /// A plain class rather than a positional record: this project still targets net472, where a
    /// record's init-only setters need an <c>IsExternalInit</c> the framework does not define.
    /// </remarks>
    private sealed class Finding
    {
        public Finding(Verdict verdict, string group, string expression, string detail,
            string signature)
        {
            Verdict = verdict;
            Group = group;
            Expression = expression;
            Detail = detail;
            Signature = signature;
        }

        public Verdict Verdict { get; }

        public string Group { get; }

        public string Expression { get; }

        public string Detail { get; }

        public string Signature { get; }
    }

    /// <summary>Worst-first, so the report leads with what matters.</summary>
    /// <remarks>Materialised once: net472 has no generic <c>Enum.GetValues&lt;T&gt;()</c>.</remarks>
    private static readonly Verdict[] Verdicts = (Verdict[])Enum.GetValues(typeof(Verdict));

    private static int Count(Dictionary<Verdict, int> counts, Verdict verdict) =>
        counts.TryGetValue(verdict, out var n) ? n : 0;

    /// <summary>The JVM exception names a recorded refusal can legitimately carry.</summary>
    /// <remarks>
    /// Not decoration. `eval` is what PySpark's `collect()` raised, and Python can fail on the way
    /// back with the JVM never having refused anything: measured, `CAST(-2.675 AS TIMESTAMP)` is
    /// answered by Spark and then raises `OSError: [Errno 22]` converting a pre-epoch instant to a
    /// `datetime` on Windows. Reading that as "Spark refused" invents a defect out of the
    /// harvesting machine's C library -- 15 of them in the first 3000-expression run.
    /// <para>
    /// An ALLOWLIST rather than a denylist of Python builtins, and anything unrecognised is
    /// reported as <see cref="Verdict.Unusable"/> rather than quietly believed: a new JVM
    /// exception name showing up as "unusable" is visible and fixable, where a new Python failure
    /// silently counted as a Spark refusal is neither.
    /// </para>
    /// </remarks>
    private static readonly HashSet<string> JvmExceptions = new(StringComparer.Ordinal)
    {
        "AnalysisException", "ArithmeticException", "ParseException", "NumberFormatException",
        "DateTimeException", "SparkRuntimeException", "SparkArithmeticException",
        "SparkNumberFormatException", "SparkDateTimeException", "SparkUpgradeException",
        "SparkIllegalArgumentException", "SparkUnsupportedOperationException",
        "IllegalArgumentException", "UnsupportedOperationException", "Py4JJavaError",
    };

    private static bool IsJvmRefusal(string? error) =>
        error is not null && JvmExceptions.Contains(error.Split(':')[0]);

    [SkippableFact]
    public void TriageTheGeneratedCorpus()
    {
        Skip.If(string.IsNullOrEmpty(CorpusPath),
            "set EW_FUZZ_CORPUS to a corpus written by fuzz_expressions.py");
        Skip.IfNot(File.Exists(CorpusPath), $"no corpus at {CorpusPath}");

        using var corpus = JsonDocument.Parse(File.ReadAllText(CorpusPath!));
        var root = corpus.RootElement;
        var schema = root.GetProperty("schema");
        var rows = root.GetProperty("rows");

        var report = new StringBuilder();
        report.AppendLine("# Spark expression fuzz triage");
        report.AppendLine();
        report.AppendLine($"- corpus: `{CorpusPath}`");
        report.AppendLine($"- spark {root.GetProperty("spark_version").GetString()} "
                          + $"on java {root.GetProperty("java_version").GetString()}"
                          + $", seed {root.GetProperty("seed").GetInt32()}");
        report.AppendLine();

        var findings = new List<Finding>();
        findings.AddRange(Triage(root.GetProperty("groups"), schema, rows,
            new SparkFunctionRegistry(), "ansi", report));

        if (root.TryGetProperty("legacy", out var legacy))
        {
            findings.AddRange(Triage(legacy.GetProperty("groups"), schema, rows,
                new SparkFunctionRegistry(new SparkDialectOptions { Ansi = false }),
                "legacy", report));
        }

        WriteFindings(findings, report);

        var path = Path.ChangeExtension(CorpusPath!, null) + "-triage.md";
        File.WriteAllText(path, report.ToString());
        _output.WriteLine(report.ToString());
        _output.WriteLine($"report written to {path}");
    }

    /// <summary>Evaluates one dialect's section and returns everything that did not agree.</summary>
    private static List<Finding> Triage(
        JsonElement groups, JsonElement schema, JsonElement rows,
        SparkFunctionRegistry registry, string dialect, StringBuilder report)
    {
        var batch = CorpusEvaluation.BuildBatch(schema, rows);
        var findings = new List<Finding>();
        var counts = new Dictionary<Verdict, int>();
        var total = 0;

        foreach (var group in groups.EnumerateObject())
        {
            foreach (var entry in group.Value.EnumerateArray())
            {
                total++;
                var finding = TriageOne(entry, batch, registry, $"{dialect}/{group.Name}");
                counts[finding.Verdict] = Count(counts, finding.Verdict) + 1;
                if (finding.Verdict != Verdict.Agree)
                    findings.Add(finding);
            }
        }

        report.AppendLine($"## {dialect}: {total} expressions");
        report.AppendLine();
        foreach (var verdict in Verdicts)
            report.AppendLine($"- {verdict}: {Count(counts, verdict)}");
        report.AppendLine();

        return findings;
    }

    private static Finding TriageOne(
        JsonElement entry, RecordBatch batch, SparkFunctionRegistry registry, string group)
    {
        var expression = entry.GetProperty("expression").GetString()!;

        if (!entry.TryGetProperty("eval", out var eval))
            return new Finding(Verdict.Agree, group, expression, "", "");

        var sparkOk = eval.GetProperty("ok").GetBoolean();
        var (array, threw) = Evaluate(expression, batch, registry);

        if (!sparkOk)
        {
            var error = eval.GetProperty("error").GetString();
            if (!IsJvmRefusal(error))
            {
                // The JVM never refused; the answer died on the way back through Python. The
                // RENDERED answer usually survives that -- it crosses as text -- so the
                // expression is still worth comparing, just not through its typed value.
                var textOnly = CompareText(entry, expression, batch, registry);
                return textOnly is null
                    ? new Finding(Verdict.Unusable, group, expression,
                        $"no typed answer: {Truncate(error)}", Signature(error))
                    : new Finding(Verdict.TextDiffers, group, expression, textOnly,
                        Signature(textOnly));
            }

            // Spark refused. Refusing is the right answer; a value is not. This is the direction
            // that matters most for a CHECK constraint -- Spark rejects the row and we admit it.
            return threw is not null
                ? new Finding(Verdict.Agree, group, expression, "", "")
                : new Finding(Verdict.WeAnsweredWhereSparkRefused, group, expression,
                    $"Spark: {Truncate(error)}", Signature(error));
        }

        if (threw is not null)
        {
            return new Finding(Verdict.WeThrew, group, expression, threw, Signature(threw));
        }

        // Spark's typed answer, compared semantically -- the fixture holds Python's rendering, so
        // a decimal is compared as an unscaled integer and a double as a double.
        string? valueProblem = null;
        var values = eval.GetProperty("values");
        try
        {
            for (var row = 0; row < values.GetArrayLength() && valueProblem is null; row++)
                valueProblem = CorpusEvaluation.Compare(values[row], array!, row) is { } p
                    ? $"row {row}: {p}"
                    : null;
        }
        catch (Exception ex)
        {
            var sparkType = entry.TryGetProperty("type", out var t)
                            && t.GetProperty("ok").GetBoolean()
                ? t.GetProperty("type").GetString()
                : "unresolved";
            var ours = array!.Data.DataType.Name;
            return new Finding(Verdict.TypeDiffers, group, expression,
                $"Spark resolved {sparkType}, we produced {ours} ({ex.GetType().Name})",
                Signature($"{sparkType} vs {ours}"));
        }

        // Spark's OWN rendering of the same answer, which crossed the wire as text and is
        // therefore exact. Ours has to be the same string, not merely the same number.
        var textProblem = CompareText(entry, expression, batch, registry);

        return (valueProblem, textProblem) switch
        {
            (null, null) => new Finding(Verdict.Agree, group, expression, "", ""),
            (not null, _) when valueProblem.Contains("no comparison for") =>
                new Finding(Verdict.Unusable, group, expression, valueProblem,
                    Signature(valueProblem)),
            (not null, not null) => new Finding(Verdict.ValueDiffers, group, expression,
                valueProblem, Signature(valueProblem)),
            (null, not null) => new Finding(Verdict.TextDiffers, group, expression,
                textProblem, Signature(textProblem)),
            (not null, null) => new Finding(Verdict.ComparisonUnreliable, group, expression,
                valueProblem, Signature(valueProblem)),
        };
    }

    /// <summary>
    /// Compares our <c>CAST(… AS STRING)</c> against the one the JVM produced, exactly.
    /// </summary>
    private static string? CompareText(
        JsonElement entry, string expression, RecordBatch batch, SparkFunctionRegistry registry)
    {
        if (!entry.TryGetProperty("text", out var text) || !text.GetProperty("ok").GetBoolean())
            return null;   // Spark could not render it either; nothing to compare.

        var (array, threw) = Evaluate($"CAST(({expression}) AS STRING)", batch, registry);
        if (threw is not null)
            return $"Spark rendered it, we threw {threw}";
        if (array is not StringArray rendered)
            return $"CAST(... AS STRING) produced {array!.Data.DataType.Name}, not a string";

        var values = text.GetProperty("values");
        for (var row = 0; row < values.GetArrayLength(); row++)
        {
            var want = values[row].ValueKind == JsonValueKind.Null ? null : values[row].GetString();
            var got = rendered.IsNull(row) ? null : rendered.GetString(row);
            if (!string.Equals(want, got, StringComparison.Ordinal))
                return $"row {row}: Spark renders '{want}', we render '{got}'";
        }

        return null;
    }

    private static (IArrowArray? Array, string? Threw) Evaluate(
        string expression, RecordBatch batch, SparkFunctionRegistry registry)
    {
        try
        {
            return (new ArrowRowEvaluator(registry)
                .EvaluateExpression(SparkSqlParser.ParseExpression(expression), batch), null);
        }
        catch (Exception ex)
        {
            return (null, $"{ex.GetType().Name}: {ex.Message}");
        }
    }

    /// <summary>
    /// Collapses a detail down to what makes two findings the same finding.
    /// </summary>
    /// <remarks>
    /// Not a shrinker -- it does not touch the expression. It only strips the VALUES out of a
    /// message so that fifty overflows at fifty different magnitudes group as one line. Without
    /// it the report is a transcript rather than a list of things to fix.
    /// </remarks>
    private static string Signature(string? detail)
    {
        if (string.IsNullOrEmpty(detail))
            return "";

        var sb = new StringBuilder(detail.Length);
        var inNumber = false;
        foreach (var c in detail)
        {
            if (c >= '0' && c <= '9')
            {
                if (!inNumber)
                    sb.Append('#');
                inNumber = true;
                continue;
            }

            inNumber = false;
            sb.Append(c);
        }

        return Truncate(sb.ToString(), 160)!;
    }

    private static string? Truncate(string? text, int max = 200) =>
        text is not null && text.Length > max ? text.Substring(0, max) + "..." : text;

    /// <summary>Writes the findings grouped by verdict then by signature, worst first.</summary>
    private static void WriteFindings(List<Finding> findings, StringBuilder report)
    {
        report.AppendLine($"## {findings.Count} findings");
        report.AppendLine();

        foreach (var verdict in Verdicts)
        {
            var forVerdict = findings.Where(f => f.Verdict == verdict).ToList();
            if (forVerdict.Count == 0)
                continue;

            report.AppendLine($"### {verdict} ({forVerdict.Count})");
            report.AppendLine();

            var bySignature = forVerdict
                .GroupBy(f => f.Signature)
                .OrderByDescending(g => g.Count());

            foreach (var bucket in bySignature)
            {
                var groups = string.Join(", ", bucket.Select(f => f.Group).Distinct().OrderBy(g => g, StringComparer.Ordinal));
                report.AppendLine($"- **{bucket.Count()}x** `{bucket.Key}`  \\\n  _groups: {groups}_");
                // Three examples: one is an anecdote, and all of them is the transcript this
                // report exists to avoid.
                foreach (var example in bucket.Take(3))
                {
                    report.AppendLine($"    - `{example.Expression}`");
                    report.AppendLine($"      {example.Detail}");
                }
            }

            report.AppendLine();
        }
    }
}

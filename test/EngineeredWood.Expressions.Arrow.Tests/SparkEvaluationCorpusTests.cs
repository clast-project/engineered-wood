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

    /// <summary>The frame the `groups` and `legacy` sections were harvested against.</summary>
    private static JsonElement RootSchema => Corpus.RootElement.GetProperty("schema");

    private static JsonElement RootRows => Corpus.RootElement.GetProperty("rows");

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
        // THE TWO TIMESTAMP ROWS THAT USED TO SIT HERE ARE GONE, and their absence is what makes
        // #311's group measurable. PySpark converts a timestamp to a naive datetime in the
        // DRIVER's local zone on collect, so `CAST(dt AS TIMESTAMP)` and
        // `TIMESTAMP'2026-08-11 12:30:00'` were recorded as America/Los_Angeles despite the
        // session pinning UTC -- the recorded text was a property of the harvest machine. The
        // driver now renders a timestamp through the JVM instead (`_collectable` in
        // spark_driver.py), so both are compared, and so is every timestamp #311's fold resolves.
        // Excluding them instead would have taken the bare rows out of the TYPE comparison too,
        // which is the half that measures the rule.

        // The two binary rows used to sit here, excluded because `default=str` recorded a
        // bytearray as PYTHON'S REPR rather than as a value. #295 records binary as hex instead,
        // so they are compared now — and so is every binary answer the new group asks for.

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
        // WHAT USED TO BE HERE, and why its absence is the point. Two blocks stood here: 34 rows
        // of the `boolean-equality` group, where ANSI Spark refuses a boolean against a numeric at
        // analysis and we answered a column of nulls; and `a IN (bl)` / `ns IN (a, bl)`, declared
        // against #261 as a type check we had decided NOT to adopt.
        //
        // Both are gone because #286's comparison table is implemented -- see
        // `SparkFunctionRegistry.CheckComparison`, and the `comparison-families` group that
        // measures it. The #261 decision went with them: its "the members must share a type" rule
        // is the SAME family table asked of a list, so adopting the comparison half answered the
        // set half for free rather than by a second judgement call.

        // #319, and the two rows of the `null-propagation` group that diverge. Spark discards the
        // operand of IS NULL / IS NOT NULL when the operand can never be null, so an error inside
        // it never happens; we reproduce that for the shapes whose nullability is STRUCTURAL --
        // the conditional family, the predicates, a literal -- and deliberately not for
        // arithmetic, which is the one shape that looks structural and is not.
        //
        // WHY NOT ARITHMETIC. Integral arithmetic is non-nullable, so the first row folds in
        // Spark and raises here. Decimal arithmetic follows the PROMOTED precision instead:
        // measured under ANSI, `CAST(1 AS DECIMAL(10,2)) + CAST(1 AS DECIMAL(10,2))` is NULLABLE
        // (the sum wants decimal(11,2)) while `CAST(1 AS DECIMAL(38,0)) + CAST(1 AS DECIMAL(38,0))`
        // is not -- two non-null literals added together in both cases, so no rule phrased in
        // terms of the arguments' nullability can tell them apart. The group carries a third row,
        // `(99999999999999999999999999999999999999 + 1) IS NULL`, which AGREES: Spark calls that
        // sum nullable and raises, and it is exactly what an implementation that folded
        // arithmetic would answer false for -- a constant `IS NOT NULL` over an expression that
        // really can be null, which in a CHECK constraint admits a row Spark rejects.
        //
        // So this is fail-CLOSED on purpose: we refuse a write Spark accepts, rather than risk
        // accepting one Spark refuses. Reversing it needs the result TYPE of an arithmetic node,
        // which the nullability seam does not carry.
        // #319, the third row of that group, and a different kind of gap: not a nullability
        // judgement but the TYPE CHECK the fold does in place of Spark's analysis. `TypeOver`
        // types an operand by evaluating it over NO rows, and retries WITH rows when that cannot
        // answer -- which `round` cannot, because its scale decides the result type and is read
        // out of row 0. The retry then reads the malformed cast the fold existed to avoid.
        //
        // Not worth trading away: dropping the retry only changes which error is raised (the
        // scale read fails instead), and folding through a failed type check would swallow the
        // analysis errors the group's `if(1, 1, 2)` rows exist to keep. This is the residual
        // `TypeOver` already names for itself -- an expression needing BOTH a value-dependent
        // result type and a branch that raises -- and answering it needs the type INFERRED rather
        // than evaluated, which is a different design.
        ["coalesce(1, round(CAST('abc' AS DOUBLE), 1 + 1)) IS NULL"] =
            "#319: TypeOver retries with rows for a value-dependent result type, and the retry "
            + "reads the operand the fold would have skipped",

        ["(2147483647 + 1) IS NULL"] =
            "#319: integral arithmetic is non-nullable to Spark and folds; we evaluate and raise",
        ["(CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)) "
            + "+ CAST(1 AS DECIMAL(38,0))) IS NULL"] =
            "#319: decimal arithmetic nullability follows the promoted precision, which the rule "
            + "cannot see, so we evaluate and raise",

        // ── NARROWER THAN SPARK, not different from it. The `temporal-text` group's five, all
        // from #318's fix, and each is a REFUSAL where Spark answers rather than a wrong value.
        //
        // The first three are DateTimeOffset's range. An instant travels through the cast as one
        // — `SparkArrays.ReadForCast` hands one over and `BuildDate32` takes one back — so the
        // years it cannot hold are years this cast cannot answer, whatever the grammar reads.
        // Spark's own bound is Java's: seven digits for a date, six for a timestamp, and a
        // leading `-` on either. Widening means carrying days-from-epoch through the cast
        // instead, which is a change to that shape rather than to the grammar.
        ["CAST(CAST('-2026-08-11' AS DATE) AS STRING)"] =
            "#318: a year before 1 is outside DateTimeOffset, which is what the cast carries",
        ["CAST(CAST('123456-01-01' AS DATE) AS STRING)"] =
            "#318: a year past 9999 is outside DateTimeOffset, which is what the cast carries",
        ["CAST(CAST('-2026-08-11 12:30:00' AS TIMESTAMP) AS STRING)"] =
            "#318: a year before 1 is outside DateTimeOffset, which is what the cast carries",

        // ...and the last two are the tz database. Resolving a REGION id needs one, and .NET's
        // is not the same on every target framework — `FindSystemTimeZoneById` reads an IANA id
        // on .NET 6 and later and throws on .NET Framework, where the id has a Windows spelling.
        // A cast that answered on net10.0 and refused on net472 would make one CHECK constraint
        // accept a row on one host and reject it on another, so both refuse. Every
        // SELF-DESCRIBING zone is read: `Z`, an offset in any of Java's spellings, and the
        // `UTC`/`GMT`/`UT` prefixes, which is how a timestamp normally carries its zone.
        ["CAST(CAST('2026-08-11 12:30:00America/Los_Angeles' AS TIMESTAMP) AS STRING)"] =
            "#318: a region timezone needs a tz database our target frameworks do not share",
        ["CAST(CAST('2026-08-11 12:30:00EST' AS TIMESTAMP) AS STRING)"] =
            "#318: a short-id timezone resolves through the same database",

        // #301, and the one row of the `binary-casts` group that diverges. Spark's STRING is a
        // BYTE string: `CAST(X'FF' AS STRING)` holds the raw FF, and casting it back hands the
        // same byte over. A .NET string is UTF-16 and cannot hold an unpaired FF, so the decode
        // substitutes U+FFFD before anything can cast it back, and we answer its UTF-8 instead.
        //
        // Two rows in the same group agree BY LUCK and are there to say so:
        // `X'FF' = CAST(X'FF' AS STRING)` and `nullif(X'FF', CAST(X'FF' AS STRING))`. Spark
        // compares FF with FF; we compare U+FFFD with U+FFFD; both routes say equal.
        ["CAST(CAST(X'FF' AS STRING) AS BINARY)"] =
            "#301: Spark's STRING is bytes, ours is UTF-16, so FF becomes U+FFFD",


        // ── DIVERGENT BY JDK: the fixture's answers, not Spark's alone. ───────────────────────
        // Spark reaches a decimal from a double through Double.toString, which did not produce
        // the shortest representation before JDK 19 (JDK-4511638). This corpus was gathered on
        // the JDK named by `java_version` beside `conf`; .NET renders the shortest form, so these
        // three land in the band where the two disagree. Each pair round-trips to the SAME
        // double -- they are two spellings of one value, not two values.
        //
        // Measured over ~1e6 doubles: the two renderings differ on 2.4%, all of them needing 17
        // or 18 digits where the shortest form needs 16 or 17, and on NONE past 7.9e28. #244
        // deliberately documents this rather than resolving it: matching would mean
        // reimplementing a JDK algorithm that Java itself has replaced. Re-harvesting on JDK 19
        // or later should make all three vanish, and the test will then ask for their removal.
        ["CAST(CAST(1e23 AS DOUBLE) AS DECIMAL(38,0))"] =
            "#244: JDK 17 renders 9.999999999999999E22 where the shortest form is 1E23",
        ["CAST(CAST(2.7703798343611187E17 AS DOUBLE) AS DECIMAL(38,0))"] =
            "#244: JDK 17 renders 18 digits where the shortest form needs 17",
        ["CAST(CAST(3.333333333333333E17 AS DOUBLE) AS DECIMAL(38,0))"] =
            "#244: JDK 17 renders 17 digits where the shortest form needs 16",

        // The same two values through CAST(... AS STRING), which prints with the same
        // Double.toString and so lands in the same band. That they are the SAME two, and that
        // the other 26 rows of the float-to-string group agree exactly, is what says the
        // divergence is the JDK's and not this library's. #248.
        ["CAST(CAST(1e23 AS DOUBLE) AS STRING)"] =
            "#244: JDK 17 prints 9.999999999999999E22 where the shortest form is 1.0E23",
        ["CAST(CAST(3.333333333333333E17 AS DOUBLE) AS STRING)"] =
            "#244: JDK 17 prints 17 digits where the shortest form needs 16",

        // The same band, reached from the other end by #288's subnormals -- and these two are
        // not about digit COUNT at all. `1e-323` is the double two steps above the smallest
        // there is, and both 1.0E-323 and 9.9E-324 are two digits long and read back as it; JDK
        // 19 picks the closer one and JDK 17 does not. Verified on JDK 21 directly, which prints
        // 9.9E-324, and over all 8,388,607 subnormal floats and 22,000 subnormal doubles: these
        // are the ONLY rows of the `subnormal-floats` group that move, and the other 20 agree
        // exactly, which is what says the divergence is the JVM's.
        ["CAST(CAST(1e-323 AS DOUBLE) AS STRING)"] =
            "#288: JDK 17 prints 1.0E-323 where JDK 19+ prints the closer 9.9E-324",
        ["CAST(CAST(-1e-323 AS DOUBLE) AS STRING)"] =
            "#288: JDK 17 prints -1.0E-323 where JDK 19+ prints the closer -9.9E-324",

        // ── NOT IMPLEMENTED: the `date_format` pattern letters beyond y M d H m s. ────────────
        // #284 made the pattern language Java's rather than .NET's, which is what lets these be
        // DECLARED at all: before it, an unimplemented letter and one whose two languages
        // disagree were the same refusal. Every row below is one Spark answers and we refuse, so
        // the list is exactly the shape of the gap -- and its neighbours in the group, `n` and
        // `V`, are rows Spark refuses TOO, which is what says the boundary is the implementation
        // and not the corpus.
        //
        // Adding one is a day's arithmetic each and none of them is needed by a Delta CHECK or
        // generation expression, which is the scope the registry serves. They stay declared
        // until something asks.
        ["date_format(ts, 'D')"] = "day-of-year is not implemented",
        ["date_format(ts, 'E')"] = "day-of-week name is not implemented",
        ["date_format(ts, 'a')"] = "am/pm is not implemented",
        ["date_format(ts, 'h')"] = "the 12-hour clock is not implemented",
        ["date_format(ts, 'S')"] = "fraction-of-second is not implemented",
        ["date_format(ts, 'G')"] = "era is not implemented",
        ["date_format(ts, 'q')"] = "quarter is not implemented",
        ["date_format(ts, 'LLL')"] = "the standalone month is not implemented",
        ["date_format(ts, 'Z')"] = "zone offset is not implemented",
        ["date_format(ts, 'z')"] = "zone name is not implemented",
        ["date_format(ts, 'X')"] = "ISO zone offset is not implemented",

        // Spark accepts `[ ]` when FORMATTING -- an optional section whose fields are all present
        // simply outputs, so `[yyyy]` is 2026 -- but the construct is a parse-side one and
        // implementing it for the format side alone would put its boundary somewhere arbitrary.
        // Refused with Java's reserved `#`, `{` and `}`, which are in the group beside it and
        // which Spark refuses too.
        ["date_format(ts, '[yyyy]')"] = "#284: an optional section is not implemented",

        // ── NOT IMPLEMENTED: no function or materialisation for these yet. ────────────────────
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
        // #308. Spark reads the first argument of `nvl2` for nullness alone, so a struct is a
        // perfectly good subject there; the harness still cannot build the column to ask.
        ["nvl2(nested, a, 0)"] = "struct columns are not modelled",

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

            // THE FOUR `IN` ROWS THAT STOOD HERE ARE GONE, and the legacy half is why the set
            // rule needed measuring rather than assuming. Spark's string promotion excludes
            // boolean and binary in a SET under this dialect and not under ANSI, so
            // `bl IN ('true')` and `bin IN ('A')` are refused here and resolved there -- the only
            // four rows of the 9x9 matrix where the dialects disagree about a set.
            // `CheckSetComparison` carries that asymmetry, which is also why it is a question of
            // its own and not `CheckComparison` asked once per member.

            // NOTHING OF THE `boolean-equality` GROUP IS LEFT HERE. The ordering rows went when
            // #286's comparison table landed -- `a < bl` is refused in both dialects, the boolean
            // exception being equality's alone -- and the `IN` rows went with the set rule beside
            // it.

            // `'\u0663' + 1` used to sit here as #296: arithmetic had no string branch at all,
            // so every string operand reached IntegralRank and threw. It is gone because the
            // `arithmetic-string-coercion` group now measures both dialects' rules and the
            // registry reproduces them -- and the entry could only ever have been deleted from
            // THIS list, since the ANSI section passed throughout on a right answer for a wrong
            // reason (Spark refuses the string as a BIGINT, and any throw reads as agreement).

            // #318's five, as in the ANSI list and for the same two reasons. What differs is
            // only the shape of our refusal: this dialect answers NULL where the other raises,
            // so a row Spark answers reads as a null here rather than as an exception.
            ["CAST(CAST('-2026-08-11' AS DATE) AS STRING)"] =
                "#318: a year before 1 is outside DateTimeOffset, which is what the cast carries",
            ["CAST(CAST('123456-01-01' AS DATE) AS STRING)"] =
                "#318: a year past 9999 is outside DateTimeOffset, which is what the cast carries",
            ["CAST(CAST('-2026-08-11 12:30:00' AS TIMESTAMP) AS STRING)"] =
                "#318: a year before 1 is outside DateTimeOffset, which is what the cast carries",
            ["CAST(CAST('2026-08-11 12:30:00America/Los_Angeles' AS TIMESTAMP) AS STRING)"] =
                "#318: a region timezone needs a tz database our target frameworks do not share",
            ["CAST(CAST('2026-08-11 12:30:00EST' AS TIMESTAMP) AS STRING)"] =
                "#318: a short-id timezone resolves through the same database",

            // #301, as in the ANSI list: the round trip through STRING loses the raw byte.
            ["CAST(CAST(X'FF' AS STRING) AS BINARY)"] =
                "#301: Spark's STRING is bytes, ours is UTF-16, so FF becomes U+FFFD",

            // #325, found by the `negative-zero` group and nothing to do with the sign: the
            // VALUE is an ordinary zero and the two dialects render it differently. A decimal
            // casts to string through `toPlainString` under ANSI and through Java's
            // `BigDecimal.toString` without it, which goes scientific once the adjusted exponent
            // drops below -6 -- so decimal(38,37) zero is `0E-37` here and
            // `0.0000000000000000000000000000000000000` in the ANSI section. We render plainly in
            // both.
            //
            // ONLY THIS DIALECT SEES IT, like #296 above: the ANSI section passes on the same
            // expression, so deleting this entry is what will prove #325 fixed.
            ["CAST(CAST(negative(CAST(0.0 AS DOUBLE)) AS DECIMAL(38,37)) AS STRING)"] =
                "#325: legacy renders a small decimal in scientific notation; we render plainly",

            // `coalesce(X'00', CAST(NULL AS STRING))` used to sit here as #293: a typed null string
            // was indistinguishable from the untyped placeholder, so we dropped it and answered
            // the binary where Spark refuses the pair. #279 replaced the content test with a
            // structural one -- the conditional family asks the EXPRESSION which branch is a bare
            // NULL -- and a typed null stopped being one.

            // #299, and visible ONLY here. An integral compared with a FLOAT unifies to double
            // under ANSI -- because int->float is lossy and ANSI refuses to lose bits -- and to
            // FLOAT under this dialect, which rounds the integral onto the float and makes the
            // two equal. We answer ANSI's rule under both. 16777217 is the first integer a float
            // cannot hold.
            //
            // `greatest` shows it in the TYPE as well as the value, double against float, so it
            // is a resolution difference and not only an evaluation one. The corpus carries
            // `9007199254740993 = CAST(9007199254740992 AS DOUBLE)` beside these: it agrees in
            // both dialects, which is what says #299 is about float specifically and not about
            // floating point.
            ["16777217 = CAST(16777216 AS FLOAT)"] = "#299: legacy unifies int/float as float",
            ["16777217 > CAST(16777216 AS FLOAT)"] = "#299: legacy unifies int/float as float",
            ["16777217 <=> CAST(16777216 AS FLOAT)"] = "#299: legacy unifies int/float as float",
            ["CAST(16777216 AS FLOAT) IN (16777217)"] = "#299: legacy unifies int/float as float",
            ["greatest(16777217, CAST(16777216 AS FLOAT))"] = "#299: legacy resolves float, not double",
            ["nullif(16777217, CAST(16777216 AS FLOAT))"] = "#299: legacy unifies int/float as float",
            ["nullif(CAST(16777216 AS FLOAT), 16777217)"] = "#299: legacy unifies int/float as float",

            // ── DIVERGENT BY JDK: the fixture's answers, not Spark's alone. ───────────────────────
            // Spark reaches a decimal from a double through Double.toString, which did not produce
            // the shortest representation before JDK 19 (JDK-4511638). This corpus was gathered on
            // the JDK named by `java_version` beside `conf`; .NET renders the shortest form, so these
            // three land in the band where the two disagree. Each pair round-trips to the SAME
            // double -- they are two spellings of one value, not two values.
            //
            // Measured over ~1e6 doubles: the two renderings differ on 2.4%, all of them needing 17
            // or 18 digits where the shortest form needs 16 or 17, and on NONE past 7.9e28. #244
            // deliberately documents this rather than resolving it: matching would mean
            // reimplementing a JDK algorithm that Java itself has replaced. Re-harvesting on JDK 19
            // or later should make all three vanish, and the test will then ask for their removal.
            ["CAST(CAST(1e23 AS DOUBLE) AS DECIMAL(38,0))"] =
                "#244: JDK 17 renders 9.999999999999999E22 where the shortest form is 1E23",
            ["CAST(CAST(2.7703798343611187E17 AS DOUBLE) AS DECIMAL(38,0))"] =
                "#244: JDK 17 renders 18 digits where the shortest form needs 17",
            ["CAST(CAST(3.333333333333333E17 AS DOUBLE) AS DECIMAL(38,0))"] =
                "#244: JDK 17 renders 17 digits where the shortest form needs 16",
        };

    private static HashSet<string> ExpressionsIn(JsonElement groups)
    {
        var known = new HashSet<string>(StringComparer.Ordinal);
        foreach (var group in groups.EnumerateObject())
            foreach (var entry in group.Value.EnumerateArray())
                known.Add(entry.GetProperty("expression").GetString()!);

        return known;
    }


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

    /// <summary>
    /// The Arrow type we PRODUCE for a group's expressions, against the type Spark RESOLVED.
    /// </summary>
    /// <remarks>
    /// <para>
    /// #281, and the gap that let it live: a decimal's SCALE is not visible in an evaluation
    /// comparison. <see cref="CorpusEvaluation.Compare"/> reads a recorded decimal back as a
    /// number, because the fixture holds Python's rendering rather than Spark's, so
    /// <c>0.750000</c> and <c>0.750000000000</c> compare equal and a wrong result type passes.
    /// The corpus's <c>type</c> answers were checked only by <c>SparkNumericTypesTests</c>, which
    /// walks the rules directly and skips every expression involving a literal — exactly the
    /// shape this issue is about.
    /// </para>
    /// <para>
    /// Asked of the EVALUATOR — and of BOTH registries, the legacy half in the theory below — so
    /// it measures the type a caller actually
    /// receives: a Delta generated column is written at the type the array carries, and a
    /// decimal(13,12) where Spark writes a decimal(7,6) is a different column however equal the
    /// values look. Scoped to the decimal groups, whose types we model completely; a name this
    /// cannot spell is a failure rather than a skip, so the scope cannot quietly widen.
    /// </para>
    /// <para>
    /// <b>Over NO ROWS, which is the condition Spark's own answer was harvested under</b> — the
    /// corpus resolves <c>type</c> against an empty frame. It is not a convenience: the corpus
    /// carries a boundary row of zeros, so <c>2 / d2</c> and <c>a + sh</c> RAISE over the full
    /// batch while resolving perfectly well, and reading their type from an evaluation would
    /// mean skipping exactly the rows a division or an overflow makes interesting.
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData("decimal-literal-precision")]
    [InlineData("decimal-common-type")]
    [InlineData("coercion")]
    // #296, and the group that needs this gate most: its rule is a CAST TARGET, and the two
    // candidates agree on the value. `'1' + 1` is 2 whichever type the string took, so an
    // evaluation comparison cannot tell a bigint from a double and this is the only test that
    // can.
    [InlineData("arithmetic-string-coercion")]
    // #313/#340, and the group that needs this gate for the same reason `arithmetic-string-coercion`
    // does: the rule IS a type. `+a` must stay an `int` and `+NULL` must become a `double`, and the
    // evaluation gate either side of this one compares VALUES -- where the recorded 1 and a wrongly
    // widened 1.0 are the same number. Only this test can see the difference.
    [InlineData("unary-operators")]
    // #311, and the group that needs this gate MOST of all: the whole defect was a resolution
    // one. Every site in it threw `no common type for timestamp and date32` where Spark resolves
    // a timestamp, so before the fix these rows failed here as exceptions rather than as wrong
    // values -- and the evaluation gate below cannot tell a date from a timestamp holding the
    // same midnight, which is the answer a fold that truncated the other way would give.
    [InlineData("date-timestamp-fold")]
    // #303, and a gate the value comparison cannot replace: `-2147483648` is the same number as
    // an `int` and as a `bigint`, and the whole defect was that we resolved the wider one.
    [InlineData("negative-literal-fold")]
    public void TheTypeWeProduceIsTheTypeSparkResolved(string group) =>
        AssertTypesMatchSpark(
            Corpus.RootElement.GetProperty("groups"), group, Ansi, Excluded, KnownDifferences);

    /// <summary>
    /// The same question of the LEGACY registry, against the corpus harvested with ANSI off.
    /// </summary>
    /// <remarks>
    /// Both groups were harvested twice in order to record that their types are
    /// dialect-independent, and a claim the tests do not check is not recorded — the ANSI theory
    /// above would pass unchanged if the legacy registry started resolving a different decimal,
    /// since the two evaluation gates either side of it compare VALUES, which carry no scale.
    /// The `coercion` group is absent here because it is not in the harvest's `LEGACY_GROUPS`,
    /// so there is no second section for it to check; `arithmetic-string-coercion` is present
    /// for the opposite reason — it is harvested twice precisely because its answers move.
    /// </remarks>
    [Theory]
    [InlineData("decimal-literal-precision")]
    [InlineData("decimal-common-type")]
    // ...and here it is not a claim about dialect-independence at all: these types are the half
    // of #296 that DIFFERS, since a string this dialect reads as a double is one ANSI reads as a
    // bigint.
    [InlineData("arithmetic-string-coercion")]
    // ...and here because the types are dialect-INDEPENDENT and that is worth checking rather than
    // assuming: measured, `+'1'` is a double under both dialects and only what a BAD string does
    // differs. A registry that started resolving a bigint under one of them would move no value.
    [InlineData("unary-operators")]
    // ...and #311 here because half its rows are dialect-INDEPENDENT and half are not, in one
    // group. The fold's own rule is the same under both -- `coalesce(ts, dt)` is a timestamp
    // either way -- while its last four rows are string coercions, where the dialects choose
    // opposite directions: `coalesce(ts, s)` is a `timestamp` under ANSI and a `string` here.
    // Only this theory sees that second half at all.
    [InlineData("date-timestamp-fold")]
    // ...and #303 here because the fold is the parser's and so cannot differ by dialect -- which
    // is a claim until this theory checks it, while the VALUES it leads to plainly do differ.
    [InlineData("negative-literal-fold")]
    public void TheTypeWeProduceIsTheTypeSparkResolvedUnderTheLegacyDialect(string group) =>
        AssertTypesMatchSpark(
            Corpus.RootElement.GetProperty("legacy").GetProperty("groups"), group, Legacy,
            LegacyExcluded, LegacyKnownDifferences);

    private static void AssertTypesMatchSpark(
        JsonElement groups, string group, SparkFunctionRegistry registry,
        Dictionary<string, string> excluded, Dictionary<string, string> knownDifferences)
    {
        var batch = CorpusEvaluation.BuildBatch(RootSchema, RootRows).Slice(0, 0);
        var differing = new List<string>();
        var compared = 0;

        foreach (var entry in groups.GetProperty(group).EnumerateArray())
        {
            var expression = entry.GetProperty("expression").GetString()!;
            // The dialect's OWN declared lists, not the ANSI ones: a row this dialect answers
            // differently is declared in its own table, and reading the other dialect's would
            // both skip rows that are fine here and compare rows that are known not to be.
            if (excluded.ContainsKey(expression) || knownDifferences.ContainsKey(expression))
                continue;

            var recorded = entry.GetProperty("type");
            if (!recorded.GetProperty("ok").GetBoolean())
                continue;   // Spark refused to resolve it; the eval test owns that half

            compared++;

            try
            {
                var actual = new ArrowRowEvaluator(registry)
                    .EvaluateExpression(SparkSqlParser.ParseExpression(expression), batch);
                var ours = SparkTypeName(actual.Data.DataType);
                var theirs = recorded.GetProperty("type").GetString();
                if (ours != theirs)
                    differing.Add($"{expression}: spark {theirs}, we {ours}");
            }
            catch (Exception ex)
            {
                differing.Add($"{expression}: we threw {ex.GetType().Name}: {ex.Message}");
            }
        }

        Assert.Empty(differing);
        Assert.True(compared > 10, $"only {compared} expressions in '{group}' were compared");
    }

    /// <summary>An Arrow type spelled the way the corpus spells a Spark one.</summary>
    /// <remarks>
    /// Throws rather than falling back on <see cref="IArrowType.Name"/>, so a type this does not
    /// know cannot pass as a mismatch-free comparison of two strings neither of which is Spark's.
    /// </remarks>
    private static string SparkTypeName(IArrowType type) => type switch
    {
        Decimal128Type d => $"decimal({d.Precision},{d.Scale})",
        Decimal256Type d => $"decimal({d.Precision},{d.Scale})",
        Int8Type => "tinyint",
        Int16Type => "smallint",
        Int32Type => "int",
        Int64Type => "bigint",
        FloatType => "float",
        DoubleType => "double",
        BooleanType => "boolean",
        StringType => "string",
        NullType => "void",
        // Spark has ONE name for each, whatever width or unit Arrow carries underneath -- which
        // is the point of the comparison for #311's group, since resolving a millisecond
        // timestamp where the evaluator builds a microsecond one is a difference this deliberately
        // cannot see. SparkArrays.Timestamp is what keeps the two in step.
        Date32Type or Date64Type => "date",
        TimestampType => "timestamp",
        _ => throw new NotSupportedException($"no Spark spelling for {type.Name}"),
    };

    [Fact]
    public void EveryCorpusExpressionEvaluatesToSparksAnswer()
    {
        var differing = EvaluateSection(
            Corpus.RootElement.GetProperty("groups"), RootSchema, RootRows,
            Ansi, Excluded, out var compared, out var skipped);

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
            legacy.GetProperty("groups"), RootSchema, RootRows,
            Legacy, LegacyExcluded, out var compared, out var skipped);

        Assert.True(compared > 60, $"only {compared} expressions were compared");
        Assert.Equal(LegacyExcluded.Count, skipped);
        AssertOnlyDeclaredDifferences(differing, LegacyKnownDifferences);
    }

    /// <summary>
    /// The same comparison against the section harvested with a schema whose names differ in case.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Issue #181: Spark resolves identifiers case-insensitively by default, and we matched
    /// exactly — so a CHECK constraint or generation expression Spark wrote, stored as text and
    /// re-evaluated later, refused the whole write whenever it named a column in a case the
    /// schema does not use. That is a compatibility failure on a table we did not create.
    /// </para>
    /// <para>
    /// The section exists because resolution is a property of the SCHEMA. Two of the answers here
    /// could not be reached from the main corpus at all, and both of them decided the fix:
    /// <c>`WEIRD NAME`</c> resolves to a column named <c>weird name</c>, so backticks are about
    /// which characters a name may contain and not about whether its case is honoured; and with
    /// <c>dup</c> and <c>DUP</c> in one schema, the exactly-spelled <c>dup</c> is refused as
    /// AMBIGUOUS_REFERENCE along with every other spelling — so an exact-match-first rule, which
    /// is the obvious way to keep a dictionary lookup on the fast path, would answer where Spark
    /// refuses.
    /// </para>
    /// <para>
    /// Nothing is excluded and nothing is declared different: every expression in the section is
    /// one we should match exactly.
    /// </para>
    /// </remarks>
    [Fact]
    public void EveryIdentifierCaseExpressionResolvesTheWaySparkDoes()
    {
        var section = Corpus.RootElement.GetProperty("identifier_case");

        // The section is only meaningful under Spark's DEFAULT resolution. A harvest that pinned
        // caseSensitive on would record the opposite answers and read as a pile of differences.
        Assert.False(
            section.GetProperty("conf").TryGetProperty("spark.sql.caseSensitive", out _),
            "the identifier_case section must be harvested under Spark's default resolution");

        var differing = EvaluateSection(
            section.GetProperty("groups"),
            section.GetProperty("schema"),
            section.GetProperty("rows"),
            Ansi,
            NoExclusions,
            out var compared,
            out var skipped);

        Assert.True(compared > 20, $"only {compared} expressions were compared");
        Assert.Equal(0, skipped);
        AssertOnlyDeclaredDifferences(differing, NoDifferences);
    }

    private static readonly Dictionary<string, string> NoExclusions = new(StringComparer.Ordinal);

    private static readonly Dictionary<string, string> NoDifferences = new(StringComparer.Ordinal);

    /// <summary>
    /// Evaluates every expression in one corpus section, returning those whose answer differs.
    /// </summary>
    private static Dictionary<string, string> EvaluateSection(
        JsonElement groups,
        JsonElement schema,
        JsonElement rows,
        SparkFunctionRegistry registry,
        Dictionary<string, string> excluded,
        out int compared,
        out int skipped)
    {
        var batch = CorpusEvaluation.BuildBatch(schema, rows);
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

                // The same hazard the triage was corrected for on #291: skipping silently drops
                // the expression from a test whose whole contract is that the differences are
                // EXACTLY the declared ones, and a shrinking corpus would look like a passing one.
                // No entry in the fixture lacks `eval` today; this keeps it that way loudly.
                if (!entry.TryGetProperty("eval", out var eval))
                {
                    differing[expression] = "corpus entry carries no eval answer";
                    continue;
                }

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
                    // A SHAPE mismatch is reported like any other difference HERE, and is a
                    // signal rather than a string in `SparkFuzzTriage` -- see
                    // `CorpusTypeMismatchException`. This fixture is curated, so every row is one
                    // somebody chose and a difference names the expression either way; the
                    // fuzzer sorts thousands of generated rows and needs the verdict.
                    string? problem;
                    try
                    {
                        problem = CorpusEvaluation.Compare(values[row], actual!, row);
                    }
                    catch (CorpusTypeMismatchException mismatch)
                    {
                        problem = mismatch.Message;
                    }

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


    [Theory]
    // The comparison oracle needs its own test: a wrong `true` here is silent, and silence is the
    // failure mode this whole file exists to remove.
    [InlineData(1.0, 1.0, true)]
    // Last-bit drift, which used to be tolerated and is now a difference. 0.1 + 0.2 is one ulp
    // above 0.3, the same distance as the defect in #202 — tolerating it is what hid that one.
    [InlineData(0.1 + 0.2, 0.3, false)]
    // Two spellings of ONE double still agree: the comparison is on the value, not the text.
    [InlineData(1e30, 1000000000000000000000000000000.0, true)]
    [InlineData(1.0, 1.5, false)]
    [InlineData(0.0, 0.0, true)]
    [InlineData(double.NaN, double.NaN, true)]
    [InlineData(double.NaN, 1.0, false)]
    [InlineData(double.PositiveInfinity, double.PositiveInfinity, true)]
    [InlineData(double.NegativeInfinity, double.NegativeInfinity, true)]
    // Non-finite against finite. These needed their own guard while a tolerance existed —
    // an infinite `scale` made the tolerance infinite and swallowed all four.
    [InlineData(double.PositiveInfinity, 1.0, false)]
    [InlineData(1.0, double.PositiveInfinity, false)]
    [InlineData(double.PositiveInfinity, double.NegativeInfinity, false)]
    [InlineData(double.NegativeInfinity, 0.0, false)]
    public void TheFloatComparisonIsExactAndStillSettlesNaN(double expected, double actual, bool equal) =>
        Assert.Equal(equal, CorpusEvaluation.SameDouble(expected, actual));

}

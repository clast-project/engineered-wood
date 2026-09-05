#!/usr/bin/env python3
# Copyright (c) clast-project. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.
"""Harvest Spark's answers about expression syntax and typing into a checked-in fixture.

Phase 9 of the predicate-pushdown design replaces a generated grammar with a hand-written
parser. This is the independent check on it. Spark is asked three questions per expression --
how it parses, what type it resolves to, and what it evaluates to -- and the answers become
`test/EngineeredWood.Expressions.Tests/Fixtures/spark-expression-corpus.json`.

The point of a CHECKED-IN fixture is that the parser can be developed and tested offline:
nothing in EngineeredWood.Expressions.Tests needs Spark, a JVM, or a network. Rerun this only
to extend the corpus or to move to a new Spark version.

    JAVA_HOME=/opt/homebrew/opt/openjdk@17/... \
        ~/.venvs/ew-spark40/bin/python harvest_expression_corpus.py

CONFIG IS PART OF THE DATA. Delta pins nothing when it evaluates a constraint -- it runs under
whatever session writes the row -- so an expectation is only meaningful next to the settings it
was gathered under. They are pinned here and echoed into the fixture. Do not merge corpora
gathered under different settings. The `legacy` section is a SECOND corpus, gathered under a
second conf with ansi off and kept beside the first rather than merged into it.

WHAT THE THREE ANSWERS ARE FOR:
  parse -- `sql` comes back fully parenthesised, which makes it a precedence and associativity
           oracle. `1 + 2 * 3` renders as `(1 + (2 * 3))`. Diffing our own rendering against
           this is the single highest-value test for a precedence-climbing parser, and it needs
           no data and no evaluation.
  type  -- resolved against an EMPTY frame, so this measures Spark's coercion rules -- decimal
           promotion above all -- without evaluating a row. These are the rules the
           SparkFunctionRegistry has to reproduce.
  eval  -- three-valued logic, which is the part that is easy to get subtly wrong.

A row where `ok` is false is a RESULT, not a failure: recording that Spark rejects
`a + INTERVAL 1 DAY` against a given schema is as useful as recording what it accepts.
"""
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
DRIVER = os.path.join(HERE, "spark_driver.py")
FIXTURE = os.path.normpath(os.path.join(
    HERE, "..", "..", "EngineeredWood.Expressions.Tests",
    "Fixtures", "spark-expression-corpus.json"))

# Pinned deliberately. ANSI is Spark 4's default and the semantics EngineeredWood targets;
# UTC removes the machine's timezone from date/time answers (the driver does not pin it, so
# without this the corpus would differ between machines).
CONF = {
    "spark.sql.ansi.enabled": "true",
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.storeAssignmentPolicy": "ANSI",
}

# The legacy dialect, harvested SEPARATELY and kept separately. `SparkDialectOptions.Ansi = false`
# selects a whole second set of answers -- an overflow yields null where ANSI raises -- and until
# #174 asked whether a string-to-decimal cast follows that pattern, none of them had been measured
# at all. They are not merged into `groups`, because merging corpora gathered under different
# settings is the one thing the rule at the top of this file forbids.
LEGACY_CONF = dict(CONF, **{"spark.sql.ansi.enabled": "false"})

# Which groups are worth asking twice. Every expression here is one whose answer the ANSI switch
# can change; the rest of the corpus would return the same answer under both and doubling it would
# only make the fixture harder to read.
LEGACY_GROUPS = ("wide-decimal", "ansi-sensitive", "string-to-decimal")

# One schema wide enough for every expression below. Names are terse because they appear in
# hundreds of expressions and the corpus is read as a table.
SCHEMA = [
    {"name": "a", "type": "int"},
    {"name": "b", "type": "bigint"},
    {"name": "sh", "type": "smallint"},
    {"name": "f", "type": "float"},
    {"name": "g", "type": "double"},
    {"name": "d1", "type": "decimal(10,2)"},
    {"name": "d2", "type": "decimal(6,4)"},
    {"name": "d3", "type": "decimal(38,10)"},
    # d4 and d5 sit where System.Decimal cannot follow: d4 holds 10^30 against decimal's ~7.9e28
    # ceiling, and d5 is all scale, which is the shape that makes division pre-scale hardest.
    {"name": "d4", "type": "decimal(38,0)"},
    {"name": "d5", "type": "decimal(38,38)"},
    {"name": "s", "type": "string"},
    {"name": "t", "type": "string"},
    {"name": "ts", "type": "timestamp"},
    {"name": "dt", "type": "date"},
    {"name": "bl", "type": "boolean"},
    {"name": "bin", "type": "binary"},
    {"name": "nested", "type": "struct<arr:array<int>,m:map<string,int>,name:string>"},
]

# Three rows: ordinary, all-null, and boundary. The all-null row is what makes the eval answers
# a three-valued-logic corpus rather than a two-valued one; the boundary row carries INT_MIN and
# zeros so the ANSI-sensitive group has something to overflow and divide by.
#
# Values are SQL literal text, cast to the declared type by the driver.
#
# d4 and d5 deliberately hold values that do NOT overflow, on every row: `eval` is answered per
# EXPRESSION, not per row, so one overflowing row turns the whole answer into a single error and
# the values from the other rows are lost. Cases meant to overflow are written as literals in the
# wide-decimal group instead, where erroring on every row is the point.
ROWS = [
    ["1", "10", "2", "1.5", "2.5", "'12.34'", "'1.2345'", "'9.99'",
     "'1000000000000000000000000000000'", "'0.1'", "'abc'", "'abc'",
     "'2026-08-11 12:30:00'", "'2026-08-11'", "true", "X'00'",
     "named_struct('arr', array(1, 2, 3), 'm', map('k', 7), 'name', 'leaf')"],
    ["NULL"] * 17,
    ["-2147483648", "0", "-1", "0.0", "0.0", "'0.00'", "'0.0000'", "'0.0'",
     "'-1000000000000000000000000000000'", "'0.5'", "''", "'xyz'",
     "'1970-01-01 00:00:00'", "'1970-01-01'", "false", "NULL",
     "named_struct('arr', array(CAST(NULL AS int)), 'm', map(), 'name', CAST(NULL AS string))"],
]

# --- The corpus itself -----------------------------------------------------------------
# Grouped so a failure points at a feature rather than at an index. Groups map onto the
# parser's target scope in the design doc: parentheses, arithmetic, CASE, CAST,
# IN/BETWEEN/LIKE, function calls, comparison including <=>, AND/OR/NOT, IS predicates.

GROUPS = {
    "arithmetic-precedence": [
        "1 + 2 * 3", "(1 + 2) * 3", "1 - 2 - 3", "1 - (2 - 3)",
        "2 * 3 % 4", "1 + 2 - 3 + 4", "a * b + a * b",
        "1 / 2 / 4", "-1 + 2", "- (1 + 2)", "-a", "- -a", "+a",
        "a + 1 > b * 2", "2 * (a + b) <= 10",
    ],
    "logical-precedence": [
        "a > 0 OR b > 0 AND g > 0",
        "(a > 0 OR b > 0) AND g > 0",
        "NOT a > 0 AND b > 0",
        "NOT (a > 0 AND b > 0)",
        "NOT NOT bl",
        "a > 0 AND b > 0 AND g > 0",
        "a > 0 OR b > 0 OR g > 0",
        "bl AND NOT bl",
    ],
    "comparison": [
        "a = b", "a <> b", "a != b", "a < b", "a <= b", "a > b", "a >= b",
        "a <=> b", "a <=> NULL", "NULL <=> NULL",
        "a = b AND b = a",
        "s = t", "ts = dt",
    ],
    "is-predicates": [
        "a IS NULL", "a IS NOT NULL",
        "bl IS TRUE", "bl IS NOT TRUE", "bl IS FALSE", "bl IS NOT FALSE",
        "a IS NULL OR a > 0",
        "nested IS NULL", "nested.name IS NULL",
    ],
    "set-and-pattern": [
        "a IN (1, 2, 3)", "a NOT IN (1, 2, 3)", "a IN (b, 5)",
        "s IN ('x', 'y')", "a IN (1)",
        "a BETWEEN 1 AND 10", "a NOT BETWEEN 1 AND 10", "a between 1 and 10",
        "s LIKE 'a%'", "s NOT LIKE 'a%'", "s LIKE 'a_c'", "s LIKE '100\\%'",
        "s RLIKE '^a'", "s ILIKE 'A%'",
    ],
    "case-and-conditional": [
        "CASE WHEN a > 0 THEN 'pos' ELSE 'neg' END",
        "CASE WHEN a > 0 THEN 1 WHEN a < 0 THEN -1 ELSE 0 END",
        "CASE WHEN a > 0 THEN 1 END",
        "CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END",
        "CASE WHEN a > 0 THEN a ELSE g END",
        "if(a > 0, 1, 0)", "if(a > 0, a, g)",
        "coalesce(a, b)", "coalesce(a, 0)", "coalesce(a, g)",
        "nullif(a, 0)", "ifnull(a, 0)", "nvl(a, 0)",
    ],
    "cast": [
        "CAST(a AS BIGINT)", "CAST(a AS STRING)", "CAST(s AS INT)",
        "CAST(ts AS DATE)", "CAST(dt AS TIMESTAMP)", "CAST(g AS DECIMAL(10,2))",
        "CAST(d1 AS DOUBLE)", "CAST(a AS DECIMAL(3,0))",
        "TRY_CAST(s AS INT)", "TRY_CAST(a AS SMALLINT)",
        "CAST(NULL AS INT)", "a::bigint",
    ],
    "functions": [
        "SUBSTRING(s, 1, 2)", "substring(s, 1, 2)", "SUBSTRING ( s , 1 , 2 ) = 'ab'",
        "length(s)", "upper(s)", "lower(s)", "trim(s)", "concat(s, t)",
        "concat(s, a)", "s || t", "s || a",
        "year(ts)", "month(ts)", "day(ts)", "hour(ts)",
        "date_format(ts, 'yyyy-MM')", "to_date(s)",
        "abs(a)", "round(g, 2)", "greatest(a, b)", "least(a, b)",
        "current_date()", "current_timestamp()",
        "nested.name", "nested.arr[0]", "nested.m['k']",
        "nested . arr [ 1 ] < 5",
        "size(nested.arr)", "element_at(nested.m, 'k')",
    ],
    "literals": [
        "1", "-1", "1000000000000", "1.5", "1e3", "1.5e-2", ".5", "1.",
        "'abc'", "'it''s'", "\"abc\"", "''",
        "true", "false", "TRUE", "NULL", "null",
        "X'ABCD'", "x'00'",
        "DATE'2026-08-11'", "TIMESTAMP'2026-08-11 12:30:00'",
        "INTERVAL 1 DAY", "1Y", "1S", "1L", "1BD", "1D", "1F",
    ],
    "string-literals": [
        # Issue #179. Spark's lexer does NOT read '' as an escaped quote: STRING_LITERAL ends at
        # the first unescaped quote, so 'it''s' is TWO literals, and the grammar's `stringLit+`
        # concatenates them into `its`. That rule is surprising enough that #179 asks for its
        # corners to be measured rather than reasoned about, and this group is that measurement.

        # The rule itself, and how far it reaches.
        "'it''s'",
        "'a' 'b'",
        "'a'  'b'",
        "'a''b''c'",
        "'a' 'b' 'c'",
        "''''",
        "''''''",
        "'a'''",
        # Whitespace and comments are lexer-skipped. If concatenation is over TOKENS these join
        # too; if it is over literal adjacency, they do not.
        "'a'\n'b'",
        "'a' /* c */ 'b'",
        "'a' -- c\n'b'",
        # In an argument and in a comparison, so the rule is not an artefact of a bare literal.
        "concat('a' 'b', 'c')",
        "s = 'a' 'b'",
        "length('a' 'b')",
        # Across quote STYLES, which is a different token type reaching the same rule.
        '"a" "b"',
        "'a' \"b\"",
        '"a""b"',
        # A literal followed by something that is not one. `identifier stringLit` is its own
        # grammar rule -- DATE '...', X'...' -- so this asks where that rule stops.
        "'a' x",
        "DATE'2026-08-11' '2026-08-12'",

        # The escape rule the fix has to KEEP, since a backslash is what replaces the doubled
        # quote. Getting the concatenation right while dropping these would trade one defect for
        # a worse one.
        "'it\\'s'",
        "'a\\\\b'",
        "'a\\nb'",
        "'a\\tb'",
        "'a\\rb'",
        "'a\\bb'",
        "'a\\fb'",
        "'a\\\"b'",
        # An unrecognised escape. EngineeredWood keeps the backslash, which is what makes one
        # usable in a LIKE pattern -- asked rather than assumed, because Spark 4 added error
        # classes for invalid escapes.
        "'100\\%'",
        "'a\\qb'",
        "'a\\'",
        # Numeric and unicode escapes, which EngineeredWood does not implement at all.
        "'\\101'",
        "'\\0'",
        "'\\u0041'",
        "'\\U00000041'",
        # `\%` keeps its backslash while `\q` and `\f` lose theirs, so "unrecognised" is not one
        # rule but two. These separate them: the other LIKE wildcard, the fourth quote character,
        # the octal edges, an incomplete unicode escape, and a handful of escapes C recognises
        # that Spark may not.
        "'a\\_b'",
        "'a\\`b'",
        "'a\\Zb'",
        "'a\\8b'",
        "'\\7'",
        "'\\400'",
        "'\\777'",
        "'\\u12'",
        "'\\x41'",
        "'a\\vb'",
        "'a\\ b'",
        # `\101` is 'A' but `\377` is the text "377", so the octal escape does not take every
        # 3-digit value. These bracket where it stops, and the lowercase forms check that the hex
        # escapes are not case-sensitive.
        "'\\177'",
        "'\\200'",
        "'\\201'",
        "'\\277'",
        "'\\u004a'",
        "'\\U0000004a'",
        # The width rules, which decide how many characters each escape consumes. Every line here
        # is a branch of the unescaping that measuring `\7` and `\400` proved is not guessable:
        # a single octal digit is NOT an octal escape, and three digits only count when the value
        # fits a byte.
        "'\\377'",
        "'\\1011'",
        "'\\u00411'",
        "'\\uZZZZ'",
        "'\\U0001F600'",
        "'\\uD83D\\uDE00'",
        "'\\U00110000'",
        # The same escaping through the other quote style, which is a different lexer rule.
        '"a\\nb"',
        '"a\\%b"',
        # WHICH COMES FIRST, unescaping or joining. Spark maps createString over the pieces and
        # then joins, so an escape cannot span two literals: unescape-then-join answers "u0041"
        # and "101", where join-then-unescape would answer "A" for both.
        "'\\u00' '41'",
        "'\\1' '01'",
        # Eight hex digits that overflow a signed int, which is where Java's own parse gives up.
        "'\\UFFFFFFFF'",
        # Raw string literals, where no escape applies and the doubled quote may not either.
        "R'a\\nb'",
        "r'a\\nb'",
        "R'it''s'",
    ],
    "identifiers": [
        "`a`", "`a` > 0", "`weird name`", "nested.`name`",
        "a", "A", "nested.arr", "nested.m",
    ],
    "null-semantics": [
        "a > 0", "a > 0 AND b > 5", "a > 0 OR b > 5",
        "NOT (a > 0)", "a + b", "a = NULL", "NULL AND false", "NULL OR true",
        "coalesce(a, -99)", "s LIKE 'a%'",
    ],
    "coercion": [
        "a + b", "a + sh", "a + f", "a + g", "a + d1",
        "d1 + d2", "d1 - d2", "d1 * d2", "d1 / d2", "d1 % d2",
        "d3 * d3", "d3 + d1",
        "a / b", "a / g", "a % b",
        "b + b", "f + g", "sh * sh",
        "s = a", "s < a", "dt < ts",
        "concat(s, d1)", "greatest(a, g)", "coalesce(d1, a)",
    ],
    "wide-decimal": [
        # Spark decimals reach precision 38; System.Decimal stops near 7.9e28. Everything here
        # lives above that line, so none of it could be evaluated exactly before issue #131 and
        # none of its answers were measured until this group existed.

        # Values, on columns that do not overflow on any row.
        "d4 + 1", "d4 - 1", "d4 * 2", "-d4",
        "d4 + d1", "d4 + d3", "d4 * d1",
        "d5 / d5", "d5 * d5", "d5 + d5",
        "d4 = d4", "d4 > d1", "coalesce(d4, d1)",
        "CAST(d4 AS DECIMAL(38,2))", "CAST(d4 AS DOUBLE)", "CAST(d4 AS STRING)",

        # Rounding, as literals so that every row gives the same answer and the answer is the
        # whole point. Each denominator makes the discarded part EXACTLY half a unit with an even
        # digit before it, which is the only case where half-up and half-even disagree:
        # 246913/2000000 is 0.1234565, and decimal(38,0)/decimal(38,0) resolves to decimal(38,6).
        "CAST(246913 AS DECIMAL(38,0)) / CAST(2000000 AS DECIMAL(38,0))",
        "CAST(-246913 AS DECIMAL(38,0)) / CAST(2000000 AS DECIMAL(38,0))",
        "CAST(-1 AS DECIMAL(38,0)) / CAST(2000000 AS DECIMAL(38,0))",
        "CAST(1 AS DECIMAL(38,0)) / CAST(2000000 AS DECIMAL(38,0))",
        # A rescale rather than a division, and both signs, because away-from-zero and
        # toward-negative-infinity differ only on the negative side.
        "CAST(CAST(2.5 AS DECIMAL(38,1)) AS DECIMAL(38,0))",
        "CAST(CAST(-2.5 AS DECIMAL(38,1)) AS DECIMAL(38,0))",
        "CAST(CAST(1.45 AS DECIMAL(38,2)) AS DECIMAL(38,1))",
        # Control: half-up and half-even agree here, so a disagreement would mean something else
        # is wrong.
        "CAST(CAST(3.5 AS DECIMAL(38,1)) AS DECIMAL(38,0))",
        # Clamped multiply, where the result scale falls below s1+s2 and digits are discarded.
        "d3 * d3",

        # Overflow, as literals so every row errors and the ERROR CLASS is what gets recorded.
        # 6e37 + 6e37 is 1.2e38: too many digits for decimal(38,0), but comfortably inside a
        # 128-bit mantissa, so this is the gap a width check alone would let through.
        "CAST(60000000000000000000000000000000000000 AS DECIMAL(38,0))"
        " + CAST(60000000000000000000000000000000000000 AS DECIMAL(38,0))",
        "d4 * d4",
        "CAST(d4 AS DECIMAL(10,0))",
        "CAST(d4 AS INT)",
        # The same overflow reached from the sources that do NOT have an exact integer form, to
        # check whether the error class follows the target type or the source. A decimal source
        # and a double source landing on different classes would be worth knowing.
        "CAST(CAST(12345 AS DOUBLE) AS DECIMAL(3,0))",
        "CAST('12345' AS DECIMAL(3,0))",
        "CAST(a AS DECIMAL(2,0))",
    ],
    "string-to-decimal": [
        # Issue #174. After #131 every route to a wide decimal reaches Spark's precision 38 --
        # except a CAST from a STRING, which stayed on System.Decimal and its ~7.9e28 ceiling
        # because what Spark does with an over-long string was UNMEASURED. This group is that
        # measurement, and nothing in the cast path was allowed to change before it existed.
        #
        # Literals rather than columns, for the same reason as wide-decimal: `eval` is answered per
        # EXPRESSION, so a literal makes every row give the same answer and the answer is the point.

        # Past System.Decimal's ceiling but inside precision 38 -- the values the gap makes
        # unreachable. Both signs.
        "CAST('123456789012345678901234567890' AS DECIMAL(38,0))",
        "CAST('-123456789012345678901234567890' AS DECIMAL(38,0))",
        # The full width of the type, and one digit more than it holds.
        "CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))",
        "CAST('999999999999999999999999999999999999999' AS DECIMAL(38,0))",
        # More SIGNIFICANT digits than System.Decimal keeps, at a magnitude it can hold. This is
        # the trap a range check alone misses: decimal.TryParse ACCEPTS these and silently rounds
        # to 28-29 digits, reporting success, so the wrong answer arrives as a valid one.
        "CAST('1.0000000000000000000000000000001' AS DECIMAL(38,31))",
        "CAST('123456789012345678901234567890.12345678' AS DECIMAL(38,8))",

        # More FRACTIONAL digits than the target scale: round, truncate, error or null -- and if it
        # rounds, does it agree with the HALF_UP measured everywhere else on this path? Both signs,
        # because away-from-zero and toward-negative-infinity differ only on the negative side.
        "CAST('2.5' AS DECIMAL(38,0))",
        "CAST('-2.5' AS DECIMAL(38,0))",
        "CAST('1.45' AS DECIMAL(38,1))",
        "CAST('-1.45' AS DECIMAL(38,1))",
        # Control: half-up and half-even agree here, so a disagreement means something else is wrong.
        "CAST('3.5' AS DECIMAL(38,0))",
        # An even digit before an exactly-half remainder -- the only case where the two rules
        # disagree -- at a width no System.Decimal rounding could have reached.
        "CAST('123456789012345678901234567890.1234565' AS DECIMAL(38,6))",

        # Rounding that CARRIES past the declared precision: the value fits, its rounded form
        # does not.
        "CAST('99999999999999999999999999999999999999.5' AS DECIMAL(38,0))",
        # Rounding away entirely, at the bottom of an all-scale type.
        "CAST('0.000000000000000000000000000000000000005' AS DECIMAL(38,38))",

        # Exponent notation, which the NumberStyles.Float parse in use today accepts.
        "CAST('1e30' AS DECIMAL(38,0))",
        "CAST('1.5e3' AS DECIMAL(38,0))",
        "CAST('1e39' AS DECIMAL(38,0))",

        # Forms AROUND the number. Surrounding space, an explicit plus, a trailing point and a
        # leading point are all accepted by decimal.TryParse today; a thousands separator is not a
        # number to Spark but is one to several parsing modes, so it is asked rather than assumed.
        "CAST(' 42 ' AS DECIMAL(38,0))",
        "CAST('+42' AS DECIMAL(38,0))",
        "CAST('42.' AS DECIMAL(38,0))",
        "CAST('.5' AS DECIMAL(38,1))",
        "CAST('abc' AS DECIMAL(38,0))",
        "CAST('' AS DECIMAL(38,0))",
        "CAST('1,000' AS DECIMAL(38,0))",

        # A wide string against a NARROW target, where the refusal is about the target rather than
        # about System.Decimal -- so the two reasons cannot be confused for one another.
        "CAST('123456789012345678901234567890' AS DECIMAL(10,0))",

        # try_cast is the non-raising path, which is also the shape the legacy dialect takes. One
        # value that succeeds and three refusals, so both branches are recorded.
        "TRY_CAST('123456789012345678901234567890' AS DECIMAL(38,0))",
        "TRY_CAST('999999999999999999999999999999999999999' AS DECIMAL(38,0))",
        "TRY_CAST('abc' AS DECIMAL(38,0))",
        "TRY_CAST('2.5' AS DECIMAL(38,0))",

        # A string COLUMN rather than a literal, so at least one answer here is not a constant
        # fold. `s` holds 'abc' / NULL / '', which is a refusal, a null and a refusal.
        "CAST(s AS DECIMAL(38,0))",
        "TRY_CAST(s AS DECIMAL(38,0))",
    ],
    "ansi-sensitive": [
        "a / 0", "a % 0", "CAST(s AS INT)", "a + 2147483647",
        "CAST(g AS INT)", "CAST('abc' AS DATE)", "nested.arr[99]",
        "element_at(nested.m, 'missing')",
    ],
    "beyond-constraint-scope": [
        # Spark's expression parser ACCEPTS all of these -- they parse and mostly type-check.
        # What rejects them is Delta, separately and later:
        # DELTA_UNSUPPORTED_EXPRESSION_CHECK_CONSTRAINT for subqueries,
        # DELTA_UDF_IN_CHECK_CONSTRAINT for UDFs. So refusing an aggregate or a window
        # function is a post-parse VALIDATION concern for us too, not a grammar one; a parser
        # that rejects them at the syntax level diverges from Spark rather than matching it.
        "count(a)", "sum(a) > 0", "a > (SELECT 1)",
        "*", "a IN (SELECT 1)", "rank() OVER (ORDER BY a)",
    ],
    "malformed": [
        # Genuine parse errors, recorded so our error paths can be checked against Spark's.
        "a +", "((a)", "a > > 0", "",
    ],
}


def _run_driver(args):
    with tempfile.TemporaryDirectory() as tmp:
        args_path = os.path.join(tmp, "args.json")
        result_path = os.path.join(tmp, "result.json")
        with open(args_path, "w", encoding="utf-8") as fh:
            json.dump(args, fh)
        proc = subprocess.run(
            [sys.executable, DRIVER, "expr_oracle", args_path, result_path],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        if not os.path.exists(result_path):
            sys.stderr.write(proc.stderr.decode("utf-8", "replace")[-4000:])
            raise SystemExit(f"driver produced no result (exit {proc.returncode})")
        with open(result_path, "r", encoding="utf-8") as fh:
            return json.load(fh)


def main():
    expressions = [e for group in GROUPS.values() for e in group]
    # Same expression can appear in two groups; the driver would answer twice identically.
    ordered_unique = list(dict.fromkeys(expressions))

    result = _run_driver({"expressions": ordered_unique, "schema": SCHEMA,
                          "rows": ROWS, "conf": CONF})
    if not result.get("ok"):
        raise SystemExit("driver error: " + json.dumps(result)[:2000])

    by_expr = {r["expression"]: r for r in result["results"]}
    groups = {name: [by_expr[e] for e in exprs if e in by_expr]
              for name, exprs in GROUPS.items()}

    legacy_exprs = list(dict.fromkeys(e for name in LEGACY_GROUPS for e in GROUPS[name]))
    legacy = _run_driver({"expressions": legacy_exprs, "schema": SCHEMA,
                          "rows": ROWS, "conf": LEGACY_CONF})
    if not legacy.get("ok"):
        raise SystemExit("driver error (legacy): " + json.dumps(legacy)[:2000])

    legacy_by_expr = {r["expression"]: r for r in legacy["results"]}

    fixture = {
        "_comment": "Generated by harvest_expression_corpus.py. Do not edit by hand. "
                    "Answers come from Spark and are only valid under `conf`.",
        "source": "delta-spark interop tier (pyspark 4.0.1)",
        "conf": result["conf"],
        "schema": SCHEMA,
        "rows": ROWS,
        "groups": groups,
        "legacy": {
            "_comment": "The SAME expressions under ansi off, gathered in a separate session and "
                        "kept in a separate section. Valid only under the `conf` below.",
            "conf": legacy["conf"],
            "groups": {name: [legacy_by_expr[e] for e in GROUPS[name] if e in legacy_by_expr]
                       for name in LEGACY_GROUPS},
        },
    }

    os.makedirs(os.path.dirname(FIXTURE), exist_ok=True)
    with open(FIXTURE, "w", encoding="utf-8") as fh:
        json.dump(fixture, fh, indent=1, ensure_ascii=False, default=str)
        fh.write("\n")

    total = len(ordered_unique)
    parsed = sum(1 for r in result["results"] if r.get("parse", {}).get("ok"))
    typed = sum(1 for r in result["results"] if r.get("type", {}).get("ok"))
    print(f"{total} expressions -> {FIXTURE}")
    print(f"  parse ok: {parsed}/{total}   type ok: {typed}/{total}")
    print(f"  legacy (ansi off): {len(legacy_exprs)} expressions")


if __name__ == "__main__":
    main()

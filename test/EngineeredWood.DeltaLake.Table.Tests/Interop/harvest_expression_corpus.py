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
LEGACY_GROUPS = (
    "wide-decimal", "ansi-sensitive", "string-to-decimal", "integral-cast-overflow",
    "double-to-decimal", "string-coercion", "numeric-text")

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
    # Strings a numeric cast ACCEPTS, which `s` and `t` deliberately are not. `ns` is valid for
    # every numeric target; `fs` is valid for a floating one and not for an integral one, which
    # is the pair that tells the two dialects' comparison targets apart. #180.
    {"name": "ns", "type": "string"},
    {"name": "fs", "type": "string"},
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
     "'1'", "'1.5'",
     "'2026-08-11 12:30:00'", "'2026-08-11'", "true", "X'00'",
     "named_struct('arr', array(1, 2, 3), 'm', map('k', 7), 'name', 'leaf')"],
    ["NULL"] * 19,
    ["-2147483648", "0", "-1", "0.0", "0.0", "'0.00'", "'0.0000'", "'0.0'",
     "'-1000000000000000000000000000000'", "'0.5'", "''", "'xyz'",
     "'0'", "'0.5'",
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
    "integral-cast-overflow": [
        # Issue #243. Under ANSI every overflow here raises; the legacy dialect ANSWERS, and #174
        # measured one of those answers -- CAST(d4 AS INT) is 1073741824, the wrapped low bits --
        # without asking whether one rule covers the rest. Scala's Double.toInt SATURATES where
        # BigDecimal.longValue WRAPS, so "the legacy dialect wraps" is a claim about the source
        # type and not about the cast, and this group is what decides it.
        #
        # Harvested under BOTH confs: the ANSI answers pin the error classes, the legacy answers
        # pin the values.

        # A DECIMAL source at every integral width. d4 holds 10^30 and -10^30.
        "CAST(d4 AS INT)",
        "CAST(d4 AS BIGINT)",
        "CAST(d4 AS SMALLINT)",
        "CAST(d4 AS TINYINT)",
        # ...and as literals, so the answer does not depend on which row it came from.
        "CAST(CAST(1000000000000000000000000000000 AS DECIMAL(38,0)) AS INT)",
        "CAST(CAST(1000000000000000000000000000000 AS DECIMAL(38,0)) AS BIGINT)",
        "CAST(CAST(-1000000000000000000000000000000 AS DECIMAL(38,0)) AS INT)",
        # A fraction, where truncation toward zero happens before any width cut. The third has an
        # integer part that overflows an int by exactly two.
        "CAST(CAST(2.9 AS DECIMAL(10,1)) AS INT)",
        "CAST(CAST(-2.9 AS DECIMAL(10,1)) AS INT)",
        "CAST(CAST(4294967298.5 AS DECIMAL(20,1)) AS INT)",
        # Inside System.Decimal's range but outside the target's, which is the branch that already
        # has a `long` to truncate.
        "CAST(CAST(300 AS DECIMAL(10,0)) AS TINYINT)",
        "CAST(CAST(70000 AS DECIMAL(10,0)) AS SMALLINT)",

        # A DOUBLE source. If this saturates, the fix needs two rules rather than one.
        "CAST(CAST(1e30 AS DOUBLE) AS INT)",
        "CAST(CAST(-1e30 AS DOUBLE) AS INT)",
        "CAST(CAST(1e30 AS DOUBLE) AS BIGINT)",
        "CAST(CAST(4294967298.5 AS DOUBLE) AS INT)",
        "CAST(CAST(300 AS DOUBLE) AS TINYINT)",
        "CAST(CAST(1e30 AS FLOAT) AS INT)",
        "CAST(CAST(2.9 AS DOUBLE) AS INT)",
        # The two values that have no integer at all.
        "CAST(CAST('NaN' AS DOUBLE) AS INT)",
        "CAST(CAST('Infinity' AS DOUBLE) AS INT)",
        "CAST(CAST('-Infinity' AS DOUBLE) AS BIGINT)",

        # An INTEGRAL source narrowing, which is the case SparkArrays.Truncate already covers on
        # the arithmetic path.
        "CAST(4294967298 AS INT)",
        "CAST(300 AS TINYINT)",
        "CAST(-300 AS TINYINT)",
        "CAST(70000 AS SMALLINT)",
        "CAST(b AS INT)",

        # A STRING source, where being out of range is a different question from being malformed.
        "CAST('4294967298' AS INT)",
        "CAST('300' AS TINYINT)",
        "CAST('12.5' AS INT)",
        "CAST('1e30' AS INT)",
        "CAST('abc' AS INT)",

        # The discriminating case for the floating-point rule. If Spark saturated at the TARGET
        # width this would be 127; if it saturates at INT and then narrows by wrapping, it is -1.
        # 300.0 -> TINYINT giving 44 already says the narrowing wraps, and this says where the
        # saturation happens.
        "CAST(CAST(4294967298.5 AS DOUBLE) AS TINYINT)",
        "CAST(CAST(1e30 AS DOUBLE) AS TINYINT)",
        "CAST(CAST(-1e30 AS DOUBLE) AS SMALLINT)",

        # A TEMPORAL source, which becomes epoch seconds and can overflow the narrower widths.
        # Left unmeasured, its branch would be the one rule in this method nobody had asked about.
        "CAST(TIMESTAMP'9999-12-31 23:59:59' AS INT)",
        "CAST(TIMESTAMP'9999-12-31 23:59:59' AS SMALLINT)",
        "CAST(TIMESTAMP'9999-12-31 23:59:59' AS BIGINT)",

        # A STRING carrying a fraction, both signs, and one that is out of range as well as
        # fractional -- so truncation and range are separated rather than tangled.
        "CAST('-12.9' AS INT)",
        "CAST('300.5' AS TINYINT)",
        # try_cast is NOT the legacy dialect. Both refuse to raise, so one `raising: false` covered
        # them for as long as every non-raising answer was null -- and it stops covering them the
        # moment the legacy dialect answers a VALUE. These separate the two under both confs.
        "TRY_CAST(300 AS TINYINT)",
        "TRY_CAST(d4 AS INT)",
        "TRY_CAST(CAST(1e30 AS DOUBLE) AS INT)",
        "TRY_CAST('12.5' AS INT)",
        # Controls that must not move whatever the overflow rule turns out to be.
        "CAST(true AS INT)",
        "CAST(a AS BIGINT)",
        "CAST(g AS INT)",
    ],
    "double-to-decimal": [
        # Issue #244. A double past System.Decimal's ~7.9e28 was refused where Spark answers, and
        # the issue held the fix back until the JVM's part in it was settled: Spark converts a
        # double through BigDecimal.valueOf, which is new BigDecimal(Double.toString(d)), and
        # Double.toString did not produce the shortest representation before JDK 19.
        #
        # MEASURED, comparing this JDK's Double.toString against the shortest form over ~1e6
        # doubles: they differ on 2.4% -- and on NONE of the 130,152 sampled past 7.9e28. The
        # JVM matters, and it does not matter anywhere in this issue's range. `java_version` now
        # sits next to `conf` so the claim stays checkable.

        # The values from the issue. A rendering and not a binary expansion: the FLOAT row is the
        # proof, since 1e30f widens to 1.0000000150474662E30 and Spark answers those digits.
        "CAST(CAST(1e30 AS DOUBLE) AS DECIMAL(38,0))",
        "CAST(CAST(-1e30 AS DOUBLE) AS DECIMAL(38,0))",
        "CAST(CAST(1e30 AS DOUBLE) AS DECIMAL(38,2))",
        "CAST(CAST(1e30 AS FLOAT) AS DECIMAL(38,0))",
        "CAST(CAST(1e39 AS DOUBLE) AS DECIMAL(38,0))",
        "CAST(CAST(1e37 AS DOUBLE) AS DECIMAL(38,0))",
        "CAST(CAST(-1e37 AS DOUBLE) AS DECIMAL(38,0))",

        # Below the ceiling, where the value already had an answer -- and where the answer was
        # wrong in a quieter way. (decimal)double rounds to 15 significant digits while Spark
        # keeps up to 17, so these are the shape that was silently losing digits.
        "CAST(CAST(0.1 AS DOUBLE) AS DECIMAL(38,30))",
        "CAST(CAST(1.0E-3 AS DOUBLE) AS DECIMAL(38,20))",
        "CAST(CAST(2.7703798343611187E17 AS DOUBLE) AS DECIMAL(38,0))",
        "CAST(CAST(0.3333333333333333 AS DOUBLE) AS DECIMAL(38,20))",

        # THE JDK BAND. Both of these need 17 digits under this JDK's Double.toString and 16 under
        # the shortest form, so they are the cases where a Spark on 17 and a Spark on 21 disagree
        # -- recorded so the divergence is on the record rather than in a comment.
        "CAST(CAST(1e23 AS DOUBLE) AS DECIMAL(38,0))",
        "CAST(CAST(3.333333333333333E17 AS DOUBLE) AS DECIMAL(38,0))",

        # Values with no decimal at all.
        "CAST(CAST('NaN' AS DOUBLE) AS DECIMAL(10,2))",
        "CAST(CAST('Infinity' AS DOUBLE) AS DECIMAL(10,2))",
        "CAST(CAST('-Infinity' AS DOUBLE) AS DECIMAL(38,0))",

        # Rounding, which the rendering hands to the same rescale every other source uses.
        "CAST(CAST(2.5 AS DOUBLE) AS DECIMAL(38,0))",
        "CAST(CAST(-2.5 AS DOUBLE) AS DECIMAL(38,0))",
        "CAST(CAST(1.45 AS DOUBLE) AS DECIMAL(38,1))",
        # Below the target's last place entirely.
        "CAST(CAST(1e-30 AS DOUBLE) AS DECIMAL(38,2))",

        # A float source at ordinary magnitudes, where the widening is what decides the digits.
        "CAST(CAST(0.1 AS FLOAT) AS DECIMAL(38,20))",
        "CAST(f AS DECIMAL(38,10))",
        "CAST(g AS DECIMAL(38,10))",

        # Narrow targets, so the refusal is about the target rather than the ceiling.
        "CAST(CAST(1e30 AS DOUBLE) AS DECIMAL(10,0))",
        "CAST(CAST(12345 AS DOUBLE) AS DECIMAL(3,0))",
    ],
    "float-to-string": [
        # Issue #248. Render formats a float or a double with ToString("R"), which is the shortest
        # round-trip form only on .NET Core -- so the netstandard2.0 build and the net10.0 build
        # print the same value differently. #244 measured that and routed the DECIMAL cast around
        # it; this group asks what the STRING cast should print, which #244 left unmeasured.
        #
        # The digits are only half the question. A cast to a string produces TEXT, so Java's
        # formatting conventions matter as well: where it switches to scientific notation, whether
        # a whole number keeps a ".0", how the exponent is spelled. None of that is asked by a
        # cast to a decimal, where only the value survives.

        # Notation. Java switches to scientific outside [1e-3, 1e7); .NET switches elsewhere and
        # spells the exponent differently, so these are where the two disagree on shape rather
        # than on digits.
        "CAST(CAST(1.0 AS DOUBLE) AS STRING)",
        "CAST(CAST(2.5 AS DOUBLE) AS STRING)",
        "CAST(CAST(1234567 AS DOUBLE) AS STRING)",
        "CAST(CAST(12345678 AS DOUBLE) AS STRING)",
        "CAST(CAST(1e7 AS DOUBLE) AS STRING)",
        "CAST(CAST(9999999 AS DOUBLE) AS STRING)",
        "CAST(CAST(0.001 AS DOUBLE) AS STRING)",
        "CAST(CAST(0.0001 AS DOUBLE) AS STRING)",
        "CAST(CAST(1e-7 AS DOUBLE) AS STRING)",
        "CAST(CAST(1e30 AS DOUBLE) AS STRING)",
        "CAST(CAST(-1e30 AS DOUBLE) AS STRING)",
        "CAST(CAST(1e-30 AS DOUBLE) AS STRING)",

        # Zero, negative zero and the values that are not numbers.
        "CAST(CAST(0.0 AS DOUBLE) AS STRING)",
        "CAST(CAST(-0.0 AS DOUBLE) AS STRING)",
        "CAST(CAST('NaN' AS DOUBLE) AS STRING)",
        "CAST(CAST('Infinity' AS DOUBLE) AS STRING)",
        "CAST(CAST('-Infinity' AS DOUBLE) AS STRING)",

        # The JDK band from #244, asked again on this path: Double.toString is what prints here
        # too, so the same 2.4% of doubles should print differently on JDK 19+.
        "CAST(CAST(1e23 AS DOUBLE) AS STRING)",
        "CAST(CAST(3.333333333333333E17 AS DOUBLE) AS STRING)",
        "CAST(CAST(0.3333333333333333 AS DOUBLE) AS STRING)",

        # A FLOAT source, and the question that makes it its own ladder: the decimal cast renders
        # the WIDENED double (measured -- 1e30f answers 1000000015047466200000000000000), so this
        # asks whether printing does the same or keeps the float's own shorter digits.
        "CAST(CAST(1e30 AS FLOAT) AS STRING)",
        "CAST(CAST(0.1 AS FLOAT) AS STRING)",
        "CAST(CAST(1.5 AS FLOAT) AS STRING)",
        "CAST(CAST(0.3333333 AS FLOAT) AS STRING)",
        "CAST(CAST('NaN' AS FLOAT) AS STRING)",

        # Through the columns, so at least one answer is not a constant fold.
        "CAST(g AS STRING)",
        "CAST(f AS STRING)",
        # And through concat, which casts implicitly and is how a constraint usually meets one.
        "concat(s, g)",
    ],
    "wide-decimal-literals": [
        # Issue #173. SparkLiteral.ParseDecimal goes through decimal.TryParse, so a literal wider
        # than System.Decimal fails to parse — while a decimal(38,0) COLUMN evaluates fine across
        # the whole range. The seam is the parser alone.
        #
        # Bare literals, so `type` answers what Spark INFERS for one rather than what an
        # expression around it resolves to. That inference is the thing the fallback must not
        # guess: where precision comes from, where scale comes from, and where it stops.

        # 38 digits, which is as wide as a Spark decimal goes.
        "12345678901234567890123456789012345678",
        "-12345678901234567890123456789012345678",
        # 39, which is one past it.
        "123456789012345678901234567890123456789",
        "-123456789012345678901234567890123456789",

        # A fraction, so the scale is not zero, at and past the 38-digit total.
        "1234567890123456789012345678901234.5678",
        "12345678901234567890123456789012345.6789",
        "0.12345678901234567890123456789012345678",
        "0.123456789012345678901234567890123456789",

        # Around System.Decimal's own ceiling, which is where the current parse gives up: its
        # largest value, the next integer up, and the first width past its 28-29 digits.
        "79228162514264337593543950335",
        "79228162514264337593543950336",
        "12345678901234567890123456789",

        # Shapes that decide precision and scale separately from the value.
        "1.50",
        "100",
        "0.0000000000000000000000000000000000001",
        "0000000000000000000000000000000000000001",

        # Scale counts toward precision as well as digits: Spark's rule is
        # max(digits, scale) <= 38, so a value with ONE significant digit can still be too wide if
        # its scale is. Asked because the first cut of #173 checked only the digit count and let
        # 1e-45BD through as a scale-45 decimal that no Spark type can hold.
        "0.00000000000000000000000000000000000001",
        "0.000000000000000000000000000000000000001",
        "1e-38BD",
        "1e-39BD",
        "1e-45BD",
        # The BD suffix, which asks for a decimal explicitly.
        "12345678901234567890123456789012345678BD",
        "1.5BD",

        # And in the expressions the seam actually shows up in.
        "d4 + 12345678901234567890123456789012345678",
        "d4 = 1000000000000000000000000000000",
        "12345678901234567890123456789012345678 + 1",
    ],
    "round-greatest-least": [
        # Issue #182. round, greatest and least have corpus answers and no implementation, and the
        # four entries recording that are the ones this group is here to retire. Four answers are
        # not enough to write them from, so the rules they depend on are asked here.

        # ROUNDING MODE. Spark has both: `round` is half-up and `bround` is half-even, and the
        # only inputs that tell them apart are exact halves with an even digit before them.
        "round(2.5)",
        "round(-2.5)",
        "round(3.5)",
        "round(1.45, 1)",
        "round(-1.45, 1)",
        "round(0.5)",
        "round(1.5)",

        # ARITY and the scale argument, including a negative one, which rounds to the left of the
        # point rather than the right.
        "round(g)",
        "round(g, 0)",
        "round(g, 2)",
        "round(12345, -2)",
        "round(12345, -5)",
        "round(a, -1)",
        "round(d1, 1)",
        "round(d1, 0)",
        "round(d3, 2)",
        "round(d4, 0)",
        "round(g, 20)",
        # A negative scale on a DOUBLE, and a scale WIDER than the decimal already has -- the two
        # shapes the rows above leave open. Without them the implementation would be deriving the
        # double's negative-scale rule from the integral one and the widening rule from nothing.
        "round(g, -1)",
        "round(d1, 5)",
        "round(d1, 2)",
        "round(b, -3)",
        # Is the scale allowed to VARY BY ROW? Raised in review of #255: RoundScale reads row 0,
        # which is either correct because Spark requires a constant there, or a silent wrong
        # answer for every row after the first. Asking rather than arguing.
        "round(g, a)",
        "round(g, sh)",

        # The values with no rounding to do.
        "round(CAST('NaN' AS DOUBLE), 2)",
        "round(CAST('Infinity' AS DOUBLE), 2)",
        "round(NULL, 2)",

        # GREATEST and LEAST: how nulls are treated is the whole question. Spark SKIPS them rather
        # than propagating, which is the opposite of most functions here.
        "greatest(a, b)",
        "greatest(a, g)",
        "least(a, b)",
        "least(a, g)",
        "greatest(a, NULL)",
        "least(a, NULL)",
        "greatest(NULL, NULL)",
        "greatest(a, b, g)",
        "least(a, b, sh)",
        "greatest(s, t)",
        "greatest(d1, a)",
        "least(d1, d2)",
        "greatest(dt, dt)",
        "greatest(bl, bl)",
        # One argument, and none, so the arity rule is on the record too.
        "greatest(a)",
        "greatest()",

        # The DATE and TIMESTAMP literals, which parse and resolve and then cannot be materialised.
        # Recorded here as VALUES rather than only as types.
        "DATE'2026-08-11'",
        "DATE'2026-08-11' = dt",
        "TIMESTAMP'2026-08-11 12:30:00' = ts",
        "year(DATE'2026-08-11')",
        "CAST(DATE'2026-08-11' AS STRING)",
    ],
    # Comparing a string against a non-string. Spark CASTS THE STRING, and the target is not the
    # same under the two dialects -- which is why every expression here is asked twice. #180.
    #
    #   ANSI   widens a numeric target first: BIGINT for every integral width, DOUBLE for float,
    #          double AND decimal.
    #   legacy casts to the other side's own type, so the string can overflow a SMALLINT, land
    #          exactly on a FLOAT, or stay exact against a DECIMAL(38,0).
    #
    # Boolean, date and timestamp take the other side's type under BOTH dialects. The dialect
    # difference for those is only what a malformed value does: raise, or null.
    "string-coercion": [
        # The two the issue names, plus the null-safe operator -- and then the VALID comparisons
        # those hid. EngineeredWood answers null for every one of these today, including the ones
        # Spark answers with a value, which is the larger half of the defect.
        "s = a", "s < a", "s <=> a",
        "ns = a", "a = ns", "ns <> a", "ns < a", "ns > a", "ns <= a", "ns >= a", "ns <=> a",

        # Which target the numeric side picks, across every numeric type.
        "ns = sh", "ns = b", "ns = f", "ns = g", "ns = d1", "ns = d4",
        "fs = a", "fs = sh", "fs = b", "fs = f", "fs = g", "fs = d1",
        "s = b", "s = f", "s = g", "s = d1",

        # The four that tell the two targets apart, each sharp because one candidate target
        # answers differently from the other:
        #   '32768' and '2147483648' overflow SMALLINT and INT, and not BIGINT
        #   '0.1' is exact as a FLOAT and is not as a DOUBLE
        #   10^30+1 is exact as DECIMAL(38,0) and is not as a DOUBLE
        "'32768' = sh", "'2147483648' = a",
        "'0.1' = CAST(0.1 AS FLOAT)",
        "'1000000000000000000000000000001' = d4",
        "'1000000000000000000000000000001' > d4",

        # A string literal against a numeric column, and a numeric literal against a string
        # column: the string is the side that is cast, whichever side it is on.
        "'1' = a", "'1.5' = a", "'abc' = a", "ns = 1", "fs = 1", "s = 1", "ns = 1.5",

        # What the string cast itself accepts on the way through: padding is trimmed, scientific
        # notation reaches a floating target and not an integral one, and 20 digits overflow.
        "'  1  ' = a", "'1e3' = a", "'1e3' = g", "'99999999999999999999' = a",

        # NaN and infinity, where Spark's float order is not .NET's and the coerced value has to
        # land on the same side of it.
        "'NaN' = CAST('NaN' AS DOUBLE)", "'Infinity' > g", "'NaN' > g",

        # BETWEEN desugars into two comparisons, so it follows the comparison rule.
        "ns BETWEEN a AND b", "s BETWEEN a AND b",

        # Boolean and temporal, the same shape against a target that needs no widening. Spark
        # takes 'true', 'TRUE', '1', 'yes' and 't' as booleans, and truncates a timestamp-shaped
        # string to a DATE rather than rendering the date as a string.
        "'true' = bl", "'1' = bl", "s = bl",
        "dt = '2026-08-11'", "dt = '2026-08-11 12:30:00'", "dt > '1970-01-01'", "s = dt",
        "ts = '2026-08-11 12:30:00'", "ts = '2026-08-11'", "s = ts",

        # An operand that is neither a column nor a literal still has a declared type, and it is
        # the CAST's target rather than anything its values imply. Each of these separates the
        # two: a date-typed operand read as an instant compares a timestamp-shaped string against
        # 12:30 instead of truncating it, a decimal(38,0) read as the narrower decimal its value
        # needs overflows a 38-digit string, and an all-null operand types nothing at all -- which
        # `<=>` notices, because it reads both sides whatever their nullness.
        "'2026-08-11 12:30:00' = CAST(ts AS DATE)",
        "'2026-08-11' = CAST(ts AS DATE)",
        "'99999999999999999999999999999999999999' = CAST(d4 AS DECIMAL(38,0))",
        "s <=> CAST(NULL AS INT)",
        "s = CAST(NULL AS INT)",

        # IN over a LITERAL list, which is the shape a real constraint uses and the one with no
        # coercion at all today. Spark resolves ONE common type over the operand and the whole
        # list rather than pairwise: ANSI casts the strings to it, and the legacy dialect
        # promotes everything to STRING -- which `a IN ('01')` separates, since 1 and 01 are the
        # same number and different text. Boolean, binary and timestamp are the mixes Spark's
        # string promotion excludes.
        "s IN (1, 2)", "ns IN (1, 2)", "fs IN (1, 2)", "ns IN (1.5, 2)",
        "a IN ('1', '2')", "a IN ('01')", "a IN ('1.5')", "a IN (' 1')",
        "d1 IN ('12.340')", "g IN ('2.50')",
        "ns NOT IN (1, 2)", "s NOT IN (1, 2)",
        "ns IN (1, NULL)", "s IN (1, NULL)",
        "dt IN ('2026-08-11')", "dt IN ('2026-08-11 12:30:00')",
        "bl IN ('true')", "ts IN ('2026-08-11 12:30:00')", "bin IN ('A')",

        # IN does NOT follow the comparison rule -- under the legacy dialect it compares as
        # STRINGS rather than casting, so `s IN (a, b)` is false where `s = a` is null -- and a
        # binary operand takes no numeric-style coercion at all. Recorded here so the answers
        # exist, and declared as differences rather than implemented.
        "ns IN (a, b)", "s IN (a, b)", "fs IN (a, b)",

        # A list containing COLUMNS, which #259 could not reach: the parser expanded it into a
        # disjunction and each pair resolved its own type. `fs IN (a, g)` is what says the
        # divergence is not legacy-only -- an int and a double resolve through double for the
        # WHOLE list, where pairwise the string is cast to bigint against `a` and refuses.
        "fs IN (a, g)", "ns IN (a, g)", "s IN (a, g)",
        "ns IN (b)", "a IN (ns)", "a IN (ns, fs)",
        "ns NOT IN (a, b)", "ns IN (a, NULL)", "s IN (a, NULL)",

        # A list mixing a column and a string literal, where the string promotion has to see
        # both, and the mixes Spark type-checks away rather than coercing.
        "ns IN (a, 'x')", "fs IN (a, 'x')", "ns IN (a, bl)", "a IN (bl)",

        # A binary against a string is the pair where the OTHER operand moves: the binary is
        # rendered as text. The first of these is what says so -- X'FF' is not valid UTF-8, so
        # the two directions disagree about it, and only "both became text" makes it true.
        "CAST(X'FF' AS STRING) = X'FF'", "CAST(bin AS STRING)",
        "'A' = X'41'", "X'41' < 'B'", "s = bin", "'00' = bin",
    ],
    # What Spark's numeric TEXT parse accepts, per target. The three targets do not agree, and
    # the integral one is the strict one: it takes a sign, digits and an optional '.', and NO
    # exponent -- in either dialect. ANSI then refuses the '.' form outright while the legacy
    # dialect truncates it. The floating targets take Java's trailing type suffix, which .NET's
    # parse does not, and the decimal target takes an exponent but not a suffix. #258.
    "numeric-text": [
        # An exponent: valid for a floating or decimal target, never for an integral one.
        "CAST('1e3' AS BIGINT)", "CAST('1E3' AS BIGINT)", "CAST('1e+3' AS BIGINT)",
        "CAST('1e-3' AS BIGINT)", "CAST('1.5e2' AS BIGINT)", "CAST('1e3' AS INT)",
        "CAST('1e3' AS DOUBLE)", "CAST('1e3' AS DECIMAL(20,4))",

        # A decimal point, whose fraction need not be non-zero to be refused under ANSI.
        "CAST('1.0' AS BIGINT)", "CAST('1.' AS BIGINT)", "CAST('.0' AS BIGINT)",
        "CAST('10.' AS BIGINT)", "CAST('1.5' AS BIGINT)", "CAST('-1.' AS BIGINT)",
        "CAST('1.0' AS TINYINT)", "CAST('1.0' AS DECIMAL(20,4))",

        # Accepted by every target, and the shapes that are refused by all of them.
        "CAST('+1' AS BIGINT)", "CAST('  1  ' AS BIGINT)",
        "CAST('1_0' AS BIGINT)", "CAST('' AS BIGINT)", "CAST('12abc' AS BIGINT)",
        "CAST('9223372036854775808' AS BIGINT)",

        # Java's type suffix, which attaches to a numeric form and not to a named one.
        "CAST('1d' AS DOUBLE)", "CAST('1D' AS DOUBLE)", "CAST('1.5f' AS DOUBLE)",
        "CAST('1e3d' AS DOUBLE)", "CAST('1f' AS FLOAT)",
        "CAST('NaNd' AS DOUBLE)", "CAST('Infinityf' AS DOUBLE)", "CAST('1l' AS DOUBLE)",
        "CAST('1 d' AS DOUBLE)", "CAST('1d' AS DECIMAL(20,4))",
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


def _json_safe(value):
    """Replace non-finite floats with their names, so the fixture stays valid JSON.

    Python's json writes NaN and Infinity as bare tokens, which json.load accepts and every
    stricter reader refuses -- System.Text.Json among them, so the fixture simply failed to load.
    They are legitimate Spark answers (`round(CAST('NaN' AS DOUBLE), 2)` is NaN), so they are
    recorded as the strings Java prints for them and the comparison reads them back.
    """
    if isinstance(value, float):
        if value != value:
            return "NaN"
        if value == float("inf"):
            return "Infinity"
        if value == float("-inf"):
            return "-Infinity"
        return value
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


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
        # The JVM belongs next to the conf, not in the prose. Anything that renders a double goes
        # through Double.toString, which did not produce the shortest representation before JDK 19
        # -- so two corpora gathered on different JDKs are two different claims. #244.
        "java_version": result["java_version"],
        "spark_version": result["spark_version"],
        "schema": SCHEMA,
        "rows": ROWS,
        "groups": groups,
        "legacy": {
            "_comment": "The SAME expressions under ansi off, gathered in a separate session and "
                        "kept in a separate section. Valid only under the `conf` below.",
            "conf": legacy["conf"],
            "java_version": legacy["java_version"],
            "groups": {name: [legacy_by_expr[e] for e in GROUPS[name] if e in legacy_by_expr]
                       for name in LEGACY_GROUPS},
        },
    }

    os.makedirs(os.path.dirname(FIXTURE), exist_ok=True)
    with open(FIXTURE, "w", encoding="utf-8") as fh:
        json.dump(_json_safe(fixture), fh, indent=1, ensure_ascii=False, default=str,
                  allow_nan=False)
        fh.write("\n")

    total = len(ordered_unique)
    parsed = sum(1 for r in result["results"] if r.get("parse", {}).get("ok"))
    typed = sum(1 for r in result["results"] if r.get("type", {}).get("ok"))
    print(f"{total} expressions -> {FIXTURE}")
    print(f"  parse ok: {parsed}/{total}   type ok: {typed}/{total}")
    print(f"  legacy (ansi off): {len(legacy_exprs)} expressions")
    print(f"  spark {result['spark_version']} on java {result['java_version']}")


if __name__ == "__main__":
    main()

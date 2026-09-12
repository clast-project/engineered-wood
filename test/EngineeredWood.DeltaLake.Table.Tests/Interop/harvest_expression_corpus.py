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

THE SCHEMA IS PART OF THE DATA TOO, for the same reason. The `identifier_case` section is a THIRD
corpus, gathered under the same conf against a schema carrying names that differ only in case --
which the main schema cannot, since one such name would make every reference to `a` ambiguous.

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
    "double-to-decimal", "string-coercion", "numeric-text",
    # #293. `void` ought to be dialect-independent -- it carries no value to overflow and no text
    # to parse -- but #278 measured the conditional family coercing in OPPOSITE directions per
    # dialect, so "ought to" is not a measurement. Asked twice to find out.
    "null-literal",
    # #283. The decimal answers are the same in both dialects; the OTHER targets are what differ,
    # raising CAST_INVALID_INPUT under ANSI and answering NULL without it, and recording only half
    # of that would leave the legacy registry's version of the rule unmeasured.
    "unicode-digits",
    # #290. The equality answers themselves are dialect-independent, and that is worth RECORDING
    # rather than assuming: what the second harvest actually pins is #298's string rows, which
    # refuse under ANSI and answer under legacy, and #299's float rows, which answer differently
    # in each. Both were found by this group and neither is visible from one dialect.
    "nullif-equality",
    # #295. The dialects disagree twice over: ANSI refuses the integral-to-binary CAST that legacy
    # allows, and legacy refuses the binary/string CONDITIONAL that ANSI resolves. Opposite
    # directions in one group, so one harvest would describe neither.
    "binary-casts",
    # #285. The integral rows answer DIFFERENTLY under each dialect -- ANSI raises where legacy
    # wraps -- while the decimal rows raise under both. Neither half is visible from one harvest,
    # and the second is the surprising one.
    "round-overflow",
    # #278. Not merely ansi-SENSITIVE: the two dialects coerce in opposite directions, so the
    # legacy answers are a different rule rather than a different failure mode.
    "conditional-string-coercion",
    # #280. Measured identical under both dialects, all 24 rows -- the least common type is a
    # coercion rule and the ANSI switch moves overflow behaviour, not coercion. Recorded rather
    # than assumed, for the reason nullif-equality is: the registry has a legacy variant that
    # shares this rule, and "it cannot differ" is a claim until a second harvest says so.
    "decimal-common-type")

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

# --- Identifier case, which needs a schema of its own ----------------------------------
# Issue #181. Spark resolves an identifier case-insensitively by default, and EngineeredWood did
# not -- so a CHECK constraint or generation expression Spark wrote, which is stored as TEXT and
# re-evaluated later, refused the whole write whenever it named a column in a case the schema does
# not use.
#
# WHY A SECOND SCHEMA. Resolution is a property of the schema, not of the expression. The two
# questions the issue asks -- what an AMBIGUOUS match does, and whether backticks make a reference
# exact -- cannot be asked against SCHEMA above: it carries no name differing from another only in
# case, and adding one would make `a` ambiguous for every other expression in the corpus. So this
# is a separate section for the same reason `legacy` is one.
#
# The conf is CONF unchanged. `spark.sql.caseSensitive` is deliberately not pinned: its default,
# false, is the rule a Spark-written table was created under, and pinning it here would record a
# setting no writer of such a table had set.
IDENTIFIER_CASE_SCHEMA = [
    {"name": "a", "type": "int"},
    # A name that is not all lower case, so "case-insensitive" is measured in both directions
    # rather than only against a schema that never had an upper-case letter in it.
    {"name": "mixedCase", "type": "int"},
    # A name that can only be written quoted, which is where the backtick question lives.
    {"name": "weird name", "type": "int"},
    # The ambiguous pair. Only `dup` is ambiguous; the columns above stay resolvable, so one
    # schema answers both halves of the issue.
    {"name": "dup", "type": "int"},
    {"name": "DUP", "type": "int"},
]

# Two rows: values and nulls. This section is about which column a name reaches, so distinct
# values per column are what the answers have to distinguish -- and the null row keeps a null
# answer in the record.
IDENTIFIER_CASE_ROWS = [
    ["1", "2", "3", "4", "5"],
    ["NULL"] * 5,
]

IDENTIFIER_CASE_GROUPS = {
    "resolution": [
        # The reference from the issue, and the same name quoted. If backticks made a reference
        # exact, these two would differ.
        "a", "A", "`a`", "`A`",
        # ...from the other direction: a schema name carrying upper-case letters, referenced in
        # every case. `mixedcase` is the one an exact match would refuse in EITHER rule.
        "mixedCase", "MIXEDCASE", "mixedcase", "MixedCase", "`MIXEDCASE`",
        # A name that must be quoted to be written at all, so quoting and case are separated:
        # these three are all the same column if quoting is only about the characters.
        "`weird name`", "`WEIRD NAME`", "`Weird Name`",
        # Case-insensitive resolution reaches through the rest of the grammar, or it is a
        # property of a bare reference rather than of resolution.
        "A + a", "CAST(A AS BIGINT)", "A IS NULL", "UPPER(A)",
        # Absent in every case, so "not found" stays distinguishable from "found by folding".
        "nothere", "NOTHERE",
    ],
    "ambiguity": [
        # THE QUESTION THE FIX TURNS ON. If the exactly-spelled name won, `dup` and `DUP` would
        # each resolve and only the odd spellings would refuse.
        "dup", "DUP", "`dup`", "`DUP`", "Dup",
        # A column that is not half of the pair, so an ambiguous schema is shown not to poison
        # the names around it.
        "a",
    ],
}


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
    # Issue #277, found by fuzz_expressions.py: we refused every mix of a decimal with a float
    # or a double, on the theory that neither has an exact decimal representation. Spark does not
    # refuse -- it goes the OTHER WAY, promoting the decimal to double and doing the arithmetic
    # there. Measured identical under both dialects, which is why this group is not in
    # LEGACY_GROUPS: asking twice would double the fixture for no second answer.
    #
    # DIVISION AND MODULO ARE WRITTEN WITH LITERALS, not columns. The boundary row zeroes f, g,
    # d1 and d2, so `(d1 / g)` divides by zero there -- and `eval` is answered per EXPRESSION, so
    # one erroring row would replace the answers for all three and the values this group exists to
    # pin would be lost. The column forms are kept for the operators that have no zero problem.
    "decimal-float-mixing": [
        # Arithmetic, both operand orders. The result is a double every time, never a decimal
        # and never a float.
        "(d1 + g)",
        "(d1 - g)",
        "(d1 * g)",
        "(g + d1)",
        "(d3 + g)",
        "(d5 + g)",
        # decimal + float is a DOUBLE, not a float -- the case most likely to be guessed wrong.
        "(d1 + f)",
        "(d1 * f)",
        "(f + d1)",
        # Division and modulo, over literals for the reason above.
        "(CAST(12.34 AS DECIMAL(10,2)) / CAST(2.5 AS DOUBLE))",
        "(CAST(2.5 AS DOUBLE) / CAST(12.34 AS DECIMAL(10,2)))",
        "(CAST(12.34 AS DECIMAL(10,2)) % CAST(2.5 AS DOUBLE))",
        "(CAST(2.5 AS DOUBLE) % CAST(12.34 AS DECIMAL(10,2)))",
        # The arithmetic is genuinely done in double, not in decimal and then converted: an exact
        # decimal 0.1 plus a double 0.2 gives the DOUBLE answer, ulp and all.
        "(CAST(0.1 AS DECIMAL(38,38)) + CAST(0.2 AS DOUBLE))",
        "(CAST(1 AS DECIMAL(38,0)) * CAST(3.0 AS DOUBLE))",
        # The same promotion in every other context that unifies two types.
        "greatest(d1, g)",
        "least(d1, g)",
        "greatest(d1, f)",
        "greatest(f, d1)",
        "coalesce(d1, g)",
        "coalesce(d1, f)",
        "nvl(d1, g)",
        "if(true, d1, g)",
        "CASE WHEN true THEN d1 ELSE g END",
        "(d1 = g)",
        "(d1 < g)",
        # THE NEIGHBOURS. None of these involves a decimal-with-float mix, and all of them sit on
        # a branch the fix reordered, so they are pinned here to stop it drifting back. `/` is a
        # double even for two floats while `%` is a float; decimal / decimal stays a decimal;
        # and unifying an integral with a decimal is still a decimal.
        "(CAST(1.5 AS FLOAT) / CAST(1.5 AS FLOAT))",
        "(CAST(1.5 AS FLOAT) % CAST(1.5 AS FLOAT))",
        "(f + f)",
        "(f * f)",
        "(a / a)",
        "(CAST(1 AS INT) / CAST(1.5 AS FLOAT))",
        "(CAST(12.34 AS DECIMAL(10,2)) / CAST(1.2345 AS DECIMAL(6,4)))",
        "(CAST(12.34 AS DECIMAL(10,2)) % CAST(1.2345 AS DECIMAL(6,4)))",
        "coalesce(f, f)",
        "coalesce(a, f)",
        "greatest(a, f)",
        "coalesce(a, d1)",
        "greatest(d1, d2)",
    ],

    # Spark orders NaN ABOVE everything, +Infinity included, and treats -0.0 and 0.0 as equal --
    # `SQLOrderingUtil.compareDoubles`. .NET's `double.CompareTo` does the opposite on both
    # counts, so `greatest` and `least` picked the wrong operand.
    #
    # This was reachable only after #277 stopped refusing a decimal mixed with a double: before
    # that the decimal cases threw, and afterwards they quietly answered the wrong value. The
    # pure-double cases were wrong all along and nothing here had asked about them.
    "greatest-least-nan": [
        "greatest(CAST('NaN' AS DOUBLE), CAST(1.0 AS DOUBLE))",
        "least(CAST('NaN' AS DOUBLE), CAST(1.0 AS DOUBLE))",
        # NaN outranks the largest ordinary value there is.
        "greatest(CAST('NaN' AS DOUBLE), CAST('Infinity' AS DOUBLE))",
        "least(CAST('NaN' AS DOUBLE), CAST('-Infinity' AS DOUBLE))",
        # NaN against itself is EQUAL here, which is not what `=` says about it.
        "greatest(CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE))",
        "least(CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE))",
        "greatest(CAST('NaN' AS FLOAT), CAST(2.0 AS FLOAT))",
        "least(CAST('NaN' AS FLOAT), CAST(2.0 AS FLOAT))",
        # The mixes that only #277 made reachable.
        "greatest(CAST('NaN' AS DOUBLE), -1.5BD)",
        "least(CAST('NaN' AS DOUBLE), -1.5BD)",
        "greatest(CAST('NaN' AS FLOAT), 2.5BD)",
        # Rendered as well as valued: a wrong pick and a wrong spelling are different defects.
        "CAST(greatest(CAST('NaN' AS DOUBLE), -1.5BD) AS STRING)",
        "CAST(least(CAST('NaN' AS DOUBLE), CAST('-Infinity' AS DOUBLE)) AS STRING)",
    ],

    # Issue #278, found by fuzz_expressions.py: we refused a string branch against a numeric one
    # with "no common type for int64 and utf8". Spark coerces -- and the TWO DIALECTS COERCE IN
    # OPPOSITE DIRECTIONS, which is the whole reason this group is in LEGACY_GROUPS while #277's
    # is not. Under ANSI the STRING moves to the other type; under legacy the OTHER TYPE moves to
    # string. `coalesce(CAST(1 AS INT), '2')` is a bigint holding 1 under ANSI and a string
    # holding '1' under legacy.
    #
    # The family is coalesce/nvl/ifnull/if/CASE. greatest/least are NOT in it: they refuse the
    # same pair under both dialects, and the two guards at the end of this list pin that so the
    # coercion cannot leak into them.
    "conditional-string-coercion": [
        # ANSI widens: every integral width goes to bigint, every fractional one to double.
        "coalesce(CAST(1 AS INT), '2')",
        "coalesce(CAST(1 AS BIGINT), '2')",
        "coalesce(CAST(1 AS SMALLINT), '2')",
        "coalesce(CAST(1 AS TINYINT), '2')",
        "coalesce(CAST(1.5 AS FLOAT), '2')",
        "coalesce(CAST(1.5 AS DOUBLE), '2')",
        "coalesce(CAST(1.5 AS DECIMAL(10,2)), '2')",
        # Operand order does not change the target.
        "coalesce('2', CAST(1 AS INT))",
        # The rest of the family answers the same way.
        "nvl(CAST(1 AS INT), '2')",
        "ifnull(CAST(1 AS INT), '2')",
        "if(true, CAST(1 AS INT), '2')",
        "if(false, CAST(1.5 AS DECIMAL(10,2)), '2')",
        "CASE WHEN true THEN CAST(1 AS INT) ELSE '2' END",
        "CASE WHEN false THEN CAST(1 AS INT) ELSE '2' END",
        # ONLY THE CHOSEN BRANCH IS CONVERTED. The first answers 1 and does not raise even though
        # 'abc' is not a number, because no row selects it; the second raises under ANSI because
        # the string IS what the row gets. This pair is the reason the cast is masked to the
        # winning rows rather than applied to the column.
        "coalesce(CAST(1 AS INT), 'abc')",
        "coalesce(CAST(NULL AS INT), 'abc')",
        "coalesce(CAST(NULL AS INT), '2')",
        # A string that reaches an INTEGRAL target obeys the integral rules, so a decimal point or
        # an exponent is refused under ANSI where a double target would take it. #243, #258.
        "ifnull(CAST(NULL AS DOUBLE), '  1.5  ')",
        "ifnull(CAST(NULL AS INT), '1e2')",
        "ifnull(CAST(NULL AS BIGINT), '2.7')",
        # Three branches fold pairwise, so the target keeps widening.
        "coalesce(CAST(NULL AS INT), '2', CAST(3.5 AS DOUBLE))",
        # Non-numeric partners. ANSI takes the string into a boolean or a date; legacy REFUSES the
        # boolean and renders the date, so these two disagree about more than the target.
        "coalesce(true, '2')",
        "coalesce(CAST('2026-01-01' AS DATE), '2')",
        # Binary is the one target in this rule we cannot build: ANSI resolves it and we refuse,
        # which is declared in SparkEvaluationCorpusTests rather than hidden. #295.
        "coalesce(X'00', '2')",
        # A bare NULL is `void` and constrains nothing: this is an int, not the bigint that
        # unifying with a string placeholder would give.
        "coalesce(a, NULL)",
        # round() takes a string as a DOUBLE under both dialects; only the failure differs.
        "round('1.5', 2)",
        "round('1.5')",
        "round('abc', 2)",
        "round(CAST(NULL AS STRING), 2)",
        # nullif keeps its FIRST argument's type rather than a unified one, under both dialects.
        "nullif(CAST(1 AS INT), '1')",
        "nullif(CAST(1 AS INT), '2')",
        "nullif('1', CAST(1 AS INT))",
        # THE GUARDS. greatest/least refuse a string against a number in BOTH dialects. They share
        # every other part of this machinery, so without these the coercion could spread into them
        # unnoticed.
        "greatest(CAST(1 AS INT), '2')",
        "least(CAST(1 AS INT), '2')",
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
    # Which CHARACTERS count as digits, which #258 never asked. Spark's string-to-DECIMAL parse is
    # java.math.BigDecimal, and BigDecimal reads whatever Character.digit(c, 10) reads -- every BMP
    # character in Unicode category Nd. No other numeric target does: the integral parse is
    # UTF8String.toLong and the floating one is Double.parseDouble, and both compare against
    # '0'..'9'. #283 read this as a property of "Spark's string-to-number parse"; the first block
    # below is what says it belongs to the decimal target alone.
    #
    # Written as \u escapes rather than as the characters themselves, deliberately: the corpus is
    # read as a table and a diff of it should stay legible, and the escapes also put EngineeredWood's
    # own unescaping (#179) on the same path as Spark's. Verified equal to the literal characters --
    # both spellings were measured and gave identical answers.
    "unicode-digits": [
        # The repro, then the same digit against every other numeric target.
        r"CAST('\u0663' AS DECIMAL(10,0))", r"CAST('\u0663' AS DECIMAL(10,2))",
        r"CAST('\u0663' AS INT)", r"CAST('\u0663' AS BIGINT)",
        r"CAST('\u0663' AS SMALLINT)", r"CAST('\u0663' AS TINYINT)",
        r"CAST('\u0663' AS DOUBLE)", r"CAST('\u0663' AS FLOAT)",
        # Comparison and arithmetic coerce the string to DOUBLE, so they follow that target and
        # not the decimal one -- which is what makes the rule's narrowness visible from outside a
        # cast.
        r"'\u0663' + 1", r"'\u0663' > CAST(2 AS DECIMAL(10,0))",

        # Six more spellings of three, so the rule is a Unicode CATEGORY and not one block.
        r"CAST('\u06F3' AS DECIMAL(10,2))",   # EXTENDED ARABIC-INDIC DIGIT THREE
        r"CAST('\u0969' AS DECIMAL(10,2))",   # DEVANAGARI DIGIT THREE
        r"CAST('\u09E9' AS DECIMAL(10,2))",   # BENGALI DIGIT THREE
        r"CAST('\u0E53' AS DECIMAL(10,2))",   # THAI DIGIT THREE
        r"CAST('\u07C3' AS DECIMAL(10,2))",   # NKO DIGIT THREE
        r"CAST('\uFF13' AS DECIMAL(10,2))",   # FULLWIDTH DIGIT THREE

        # Digit-LIKE characters outside category Nd, and one that IS Nd but is not in the BMP:
        # BigDecimal walks UTF-16 units, so a surrogate pair is not a digit to it however plainly
        # Character.isDigit(int) says it is.
        r"CAST('\u00B3' AS DECIMAL(10,2))",        # SUPERSCRIPT THREE
        r"CAST('\u2162' AS DECIMAL(10,2))",        # ROMAN NUMERAL THREE
        r"CAST('\u2462' AS DECIMAL(10,2))",        # CIRCLED DIGIT THREE
        r"CAST('\uD835\uDFD1' AS DECIMAL(10,2))",  # U+1D7D1 MATHEMATICAL BOLD DIGIT THREE

        # The STRUCTURE stays ASCII while the digits around it do not. Sign, point and exponent
        # marker each measured on its own, against a mantissa Spark does read.
        r"CAST('-\u0663' AS DECIMAL(10,2))", r"CAST('+\u0663' AS DECIMAL(10,2))",
        r"CAST(' \u0663 ' AS DECIMAL(10,2))",
        r"CAST('\u0663.\u0665' AS DECIMAL(10,2))",
        r"CAST('\u0663e2' AS DECIMAL(10,2))", r"CAST('1e\u0663' AS DECIMAL(10,2))",
        r"CAST('\u0663\u066B\u0665' AS DECIMAL(10,2))",  # ARABIC DECIMAL SEPARATOR
        r"CAST('\uFF13\uFF0E\uFF15' AS DECIMAL(10,2))",  # FULLWIDTH FULL STOP
        r"CAST('\u2212\u0663' AS DECIMAL(10,2))",        # MINUS SIGN
        r"CAST('\uFF0B\uFF13' AS DECIMAL(10,2))",        # FULLWIDTH PLUS SIGN
        r"CAST('\u0665\uFF25\u0662' AS DECIMAL(10,2))",  # FULLWIDTH LATIN CAPITAL LETTER E
        r"CAST('\u200E\u0663' AS DECIMAL(10,2))",        # LEFT-TO-RIGHT MARK, which the trim keeps

        # Mixing, a leading zero that is not '0', rounding, and a mantissa long enough to leave
        # BigDecimal's compact path.
        r"CAST('1\u0663' AS DECIMAL(10,2))", r"CAST('\u0663\u0969' AS DECIMAL(10,2))",
        r"CAST('\u0660\u0663' AS DECIMAL(10,2))",
        r"CAST('\u0663.\u0665\u0665' AS DECIMAL(10,1))",
        r"CAST('\u0663\u0663\u0663\u0663\u0663\u0663\u0663\u0663\u0663\u0663"
        r"\u0663\u0663\u0663\u0663\u0663\u0663\u0663\u0663\u0663\u0663' AS DECIMAL(38,2))",
    ],

    # Equality between operands that have no exact System.Decimal form, which is `nullif`'s
    # question alone -- the comparison operators answer their own way and never take this route.
    # #290: a magnitude past decimal's ceiling near 7.9e28, and every NaN and infinity, reached a
    # checked conversion that threw a bare OverflowException out of the evaluator.
    #
    # THE CEILING IS System.Decimal's, NOT SPARK'S, which is why the boundary rows look arbitrary
    # from Spark's side: 7.9e28 answers and 1e29 crashed, and Spark has no boundary there at all.
    "nullif-equality": [
        # Either side of that ceiling, and far past it.
        "nullif(1, 7.9e28)", "nullif(1, 1e29)", "nullif(1, 1e308)",

        # Non-finite, which has no exact form at any magnitude -- and where Spark holds a NaN
        # EQUAL to itself, so an IEEE `==` in the fallback would answer NaN where Spark says NULL.
        "nullif(CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE))",
        "nullif(CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT))",
        "nullif(CAST('NaN' AS DOUBLE), 1)",
        "nullif(1, CAST('Infinity' AS DOUBLE))",
        "nullif(CAST('Infinity' AS DOUBLE), CAST('Infinity' AS DOUBLE))",

        # The SAME pair either way round. Only one order crashed, because the two sides signalled
        # "no exact form" with two different exceptions and only one of them was caught.
        "nullif(1e29, CAST(1e29 AS DECIMAL(38,0)))",
        "nullif(CAST(1e29 AS DECIMAL(38,0)), 1e29)",

        # Wide and NOT equal, so "neither has an exact form" must not collapse to "equal".
        "nullif(1e29, 2e29)", "nullif(1e308, 1)", "nullif(1e308, 1e308)",

        # A decimal whose nearest double lands ON the other operand: 38 nines rounds to 1e38.
        "nullif(CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)), 1e38)",

        # Signed zero, which the exact path and the double path must BOTH call equal.
        "nullif(CAST(0.0 AS DOUBLE), CAST(-0.0 AS DOUBLE))",

        # Controls: the operators that never took this route, carrying Spark's NaN rule so the
        # fallback above is agreeing with them rather than holding a second opinion.
        "CAST('NaN' AS DOUBLE) = CAST('NaN' AS DOUBLE)",
        "CAST('NaN' AS DOUBLE) <=> CAST('NaN' AS DOUBLE)",
        "CAST('NaN' AS DOUBLE) IN (CAST('NaN' AS DOUBLE))",
        "CAST(0.0 AS DOUBLE) = CAST(-0.0 AS DOUBLE)",
        "greatest(1, 1e308)", "coalesce(1, 1e308)",

        # A STRING operand, where the coercion is a different rule again and we compare as text.
        # #298. 'nullif(...1.0..., 1)' is the discriminator: as text it is neither answer.
        "nullif('1', 1)", "nullif('1.0', 1)", "nullif('1e0', 1)", "nullif(' 1', 1)",
        "nullif('abc', 1)", "nullif('x', 1e308)", "nullif('1.0', '1')",

        # An integral against a FLOAT, where the two dialects pick DIFFERENT common types and we
        # answer ANSI's under both. #299. 16777217 is the first integer a float cannot hold.
        "16777217 = CAST(16777216 AS FLOAT)",
        "16777217 > CAST(16777216 AS FLOAT)",
        "16777217 <=> CAST(16777216 AS FLOAT)",
        "CAST(16777216 AS FLOAT) IN (16777217)",
        "nullif(16777217, CAST(16777216 AS FLOAT))",
        "nullif(CAST(16777216 AS FLOAT), 16777217)",
        "greatest(16777217, CAST(16777216 AS FLOAT))",
        # ...and the same shape against a DOUBLE, which does NOT split: both dialects unify to
        # double, so this row is what says #299 is about float specifically.
        "9007199254740993 = CAST(9007199254740992 AS DOUBLE)",
    ],

    # Casts to and from BINARY, and the conditionals that need them. #295. Binary answers are
    # recorded as HEX (see `_json_safe`); before that no binary answer was comparable at all,
    # which is part of why this gap lived so long.
    "binary-casts": [
        # A string is a UTF-8 encode, in both dialects. The empty string is bytes, not null.
        "CAST('a' AS BINARY)", "CAST('abc' AS BINARY)", "CAST('' AS BINARY)",
        "CAST(CAST(NULL AS STRING) AS BINARY)",
        "CAST(s AS BINARY)", "CAST(ns AS BINARY)",

        # An INTEGRAL is the legacy dialect's alone: big-endian at the SOURCE type's width, so a
        # tinyint is one byte and a bigint eight, and a negative is its two's complement. ANSI
        # refuses the same cast, and try_cast refuses it under BOTH dialects.
        "CAST(CAST(1 AS TINYINT) AS BINARY)", "CAST(CAST(1 AS SMALLINT) AS BINARY)",
        "CAST(CAST(1 AS INT) AS BINARY)", "CAST(CAST(1 AS BIGINT) AS BINARY)",
        "CAST(CAST(-2 AS SMALLINT) AS BINARY)", "CAST(9223372036854775807 AS BINARY)",
        "TRY_CAST(CAST(1 AS INT) AS BINARY)", "TRY_CAST('a' AS BINARY)",

        # Refused by both dialects, so the allowance above is not "anything numeric".
        "CAST(CAST(1.5 AS DOUBLE) AS BINARY)", "CAST(CAST(1.5 AS FLOAT) AS BINARY)",
        "CAST(d1 AS BINARY)", "CAST(true AS BINARY)",
        "CAST(dt AS BINARY)", "CAST(ts AS BINARY)",

        # Binary to binary is the identity; binary to STRING is a UTF-8 DECODE that REPLACES what
        # is not valid, so the round trip is not the identity and both halves are here.
        "CAST(X'00FF' AS BINARY)", "CAST(bin AS BINARY)",
        "CAST(X'41' AS STRING)", "CAST(X'FF' AS STRING)",
        "CAST(CAST('abc' AS BINARY) AS STRING)",
        "CAST(CAST(X'FF' AS STRING) AS BINARY)",

        # The conditional family, which is what #295 was filed for: ANSI resolves the pair to
        # BINARY and moves the string into it as UTF-8, in EITHER operand order; the legacy
        # dialect refuses the pair outright.
        "coalesce(X'00', '2')", "coalesce('2', X'00')",
        "coalesce(CAST(NULL AS BINARY), '2')", "coalesce(X'FF', '2')",
        "coalesce(bin, '2')", "ifnull(X'00', '2')", "nvl(X'00', '2')",
        "if(true, X'00', '2')", "if(false, X'00', '2')",
        "CASE WHEN false THEN X'00' ELSE '2' END",
        # A typed null still constrains the pair, which is #293's shape rather than this one's.
        "coalesce(X'00', CAST(NULL AS STRING))",

        # ...and a binary against a binary, which needed no string rule at all and was broken too.
        "coalesce(X'00', X'01')", "coalesce(CAST(NULL AS BINARY), X'01')",
        # Refused in both dialects, so binary does not absorb everything.
        "coalesce(X'00', 1)", "coalesce(X'00', true)",

        # greatest/least ORDER a binary pair -- unsigned, and a prefix sorts first. They refuse a
        # binary against a string where coalesce coerces it, which is #278's split again.
        "greatest(X'00', X'01')", "least(X'00', X'01')",
        "greatest(X'00', X'FF')", "greatest(X'7F', X'80')",
        "greatest(X'01', X'0100')", "least(X'01', X'0100')",
        "greatest(X'00', '2')", "least(X'00', '2')",

        # nullif takes the FIRST argument's type, so it answers binary in both dialects. Equality
        # is by BYTES against another binary -- X'FF' and X'FE' decode to the same U+FFFD and are
        # still unequal -- but against a STRING it is the binary that is rendered as text (#262),
        # which is why the last row is NULL and the one above it is not.
        "nullif(X'00', X'01')", "nullif(X'00', X'00')", "nullif(X'FF', X'FE')",
        "nullif(X'00', '2')", "nullif(X'FF', CAST(X'FF' AS STRING))",
    ],

    # Rounding at the top of a type's range, which has nowhere to go. #285. Kept apart from
    # `round-greatest-least` because the question is overflow rather than the rounding mode, and
    # because the two dialects answer it DIFFERENTLY for an integral and IDENTICALLY for a decimal
    # -- which is the pair of facts the group exists to pin.
    "round-overflow": [
        # Every integral width, both signs. ANSI raises; legacy WRAPS to the width, it does not
        # null -- the shape #243 found for an integral cast.
        "round(9223372036854775807, -1)", "round(CAST(-9223372036854775808 AS BIGINT), -1)",
        "round(CAST(127 AS TINYINT), -1)", "round(CAST(-128 AS TINYINT), -1)",
        "round(CAST(32767 AS SMALLINT), -1)", "round(CAST(2147483647 AS INT), -1)",
        "round(CAST(-2147483648 AS INT), -1)",
        # Just inside, so the refusal is about the boundary and not about the shape.
        "round(9223372036854775806, -1)", "round(CAST(124 AS TINYINT), -1)",
        "round(9223372036854775807, -18)",

        # THE THREE `places` BANDS. At 19 the step is 10^19, which no long holds, so the only
        # answers are 0 and +/-10^19; at 20 and beyond every value rounds to zero. The boundary
        # between them is 5e18, which is half of 10^19 and does fit.
        "round(4999999999999999999, -19)", "round(5000000000000000000, -19)",
        "round(9223372036854775807, -19)", "round(9223372036854775807, -20)",
        "round(123, -19)", "round(123, -3)",

        # A non-negative scale on an integral has nothing to round and cannot overflow.
        "round(9223372036854775807)", "round(9223372036854775807, 2)",

        # A DECIMAL with a negative scale rounds to a multiple of a power of ten, ONCE. 14.6 is
        # the row that says so: rounding to an integer first and to the multiple second would
        # answer 20 by way of 15.
        "round(CAST(14.6 AS DECIMAL(10,1)), -1)", "round(CAST(-14.6 AS DECIMAL(10,1)), -1)",
        "round(CAST(15.0 AS DECIMAL(10,1)), -1)", "round(CAST(14.9 AS DECIMAL(10,1)), -1)",
        "round(CAST(4.6 AS DECIMAL(10,1)), -1)",
        "round(CAST(12.34 AS DECIMAL(10,2)), -1)", "round(CAST(12.34 AS DECIMAL(10,2)), -3)",
        "round(CAST(15 AS DECIMAL(2,0)), -1)", "round(CAST(5 AS DECIMAL(1,0)), -1)",
        # The carry that the +1 of precision exists for.
        "round(CAST(99 AS DECIMAL(2,0)), -1)",
        # ...and the row that says the type reserves max(p - s, places) integral digits rather
        # than p - s: a decimal(2,0) at -20 resolves to decimal(21,0), and answers 0.
        "round(CAST(99 AS DECIMAL(2,0)), -20)",

        # A decimal overflow RAISES IN BOTH DIALECTS, unlike the integral one above it.
        "round(99999999999999999999999999999999999999, -1)",
        "round(CAST(-99999999999999999999999999999999999999 AS DECIMAL(38,0)), -1)",

        # Controls: a positive scale on a decimal, including the carry past the precision.
        "round(CAST(9.99 AS DECIMAL(3,2)), 1)", "round(CAST(9.99 AS DECIMAL(3,2)), 0)",
        "round(CAST(0.5 AS DECIMAL(2,1)), 0)",

        # Floating point has no overflow to find -- it saturates to an infinity instead.
        "round(CAST(1.7976931348623157E308 AS DOUBLE), -1)",
        "round(CAST(3.4028235E38 AS FLOAT), -1)",

        # Nulls stay null on both paths.
        "round(CAST(NULL AS BIGINT), -1)", "round(CAST(NULL AS DECIMAL(10,2)), -1)",
    ],

    "decimal-common-type": [
        # Issue #280. Unification and arithmetic BOTH sacrifice scale when the natural precision
        # passes 38, and they sacrifice it to different floors. Spark's least-common-type rule is
        # `DecimalType.boundedPreferIntegralDigits`, whose floor is ZERO -- read out of the 4.0.3
        # jar and pinned here -- while arithmetic's `adjustPrecisionScale` stops at 6. Sharing
        # arithmetic's clamp with this half is the whole defect, and no row of the corpus reached
        # the overflow branch before this group, which is why the fuzzer found it and the corpus
        # did not.

        # THE REPRO. decimal(4,3) unified with decimal(38,0) is naturally decimal(41,3); the
        # answer is decimal(38,0), having given the scale up entirely rather than down to 3.
        "greatest(1.005, 99999999999999999999999999999999999999)",
        "least(1.005, 99999999999999999999999999999999999999)",
        # Asked twice, typed and rendered, so a disagreement is attributable to the value rather
        # than to how a decimal reaches the fixture. #291.
        "CAST(greatest(1.005, 99999999999999999999999999999999999999) AS STRING)",
        "CAST(least(1.005, 99999999999999999999999999999999999999) AS STRING)",

        # THE HALF THE ISSUE DID NOT NAME. greatest and least were the visible symptom, but they
        # merely share the rule; the conditionals unify through the same one. They also fail
        # DIFFERENTLY, which is why recording them separately matters: greatest skips an argument
        # that will not fit the common type and answers the other one, where coalesce has nothing
        # to skip to and answers NULL -- a non-null input reaching a caller as a null.
        "coalesce(1.005, 99999999999999999999999999999999999999)",
        "coalesce(CAST(NULL AS DECIMAL(4,3)), CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)))",
        "if(false, CAST(1.005 AS DECIMAL(4,3)), CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)))",
        "CASE WHEN false THEN CAST(1.005 AS DECIMAL(4,3)) ELSE CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)) END",

        # UNIFYING ROUNDS, HALF AWAY FROM ZERO, AND THE COMPARISON HAPPENS AFTERWARDS. These are
        # the rows that say greatest is not "return the larger argument": every argument is cast
        # to the common type FIRST, so an answer can be a value neither argument held. 0.5 at
        # scale 0 is 1, and 1 beats 0.
        "greatest(CAST(0.5 AS DECIMAL(38,38)), CAST(0 AS DECIMAL(38,0)))",
        "least(CAST(0.4 AS DECIMAL(38,38)), CAST(1 AS DECIMAL(38,0)))",
        # ...including against a negative operand, where an implementation that compared the
        # unscaled integers without aligning first would answer the other one.
        "greatest(CAST(-99999999999999999999999999999999999999 AS DECIMAL(38,0)), CAST(0.5 AS DECIMAL(38,38)))",
        "least(CAST(-99999999999999999999999999999999999999 AS DECIMAL(38,0)), CAST(0.5 AS DECIMAL(38,38)))",

        # THE CLAMP ACROSS ITS BOUNDARY, as types. Natural common type in brackets:
        #   d1,d2 (12,4) and d1,d3 (40,10) -- one under the maximum, one just over it
        #   d1,d5 (46,38) -> (38,30) and d3,d5 (66,38) -> (38,10): the scale survives in part
        #   d2,d4 (42,4) -> (38,0) and d4,d5 (76,38) -> (38,0): it does not survive at all
        # The last two are exactly the pairs arithmetic's floor of 6 would have kept 4 and 6
        # digits of, leaving too few integer digits for d4 and turning it into a null.
        "greatest(d1, d2)",
        "greatest(d1, d3)",
        "greatest(d1, d5)",
        "greatest(d3, d5)",
        "greatest(d2, d4)",
        "greatest(d4, d5)",
        "coalesce(d2, d4)",
        "if(true, d4, d5)",

        # An integral unifies as the decimal that holds it, so the same clamp applies to it.
        "greatest(d5, b)",
        "greatest(d5, a)",

        # THE CONTROL THAT KEEPS THE TWO CLAMPS APART. Same operand types as `greatest(d2, d4)`
        # above, and a different answer: unification gives the scale up to 0 where `+` keeps 4.
        # If this row ever agrees with that one, the two rules have been collapsed into one.
        "d2 + d4",
        "d1 + d3",

        # ── COMPARISON THROUGH THE SAME TYPE ──────────────────────────────────────────────
        # A comparison resolves a least common type too, and casts BOTH operands to it before
        # comparing -- so once that type gives up scale the comparison is made on ROUNDED
        # values, and it stops agreeing with an exact one. Every operator, because they do not
        # all diverge the same way: `=` and `>` both flip, `<` does not.
        "CAST(1.005 AS DECIMAL(4,3)) = CAST(1 AS DECIMAL(38,0))",
        "CAST(1.005 AS DECIMAL(4,3)) <> CAST(1 AS DECIMAL(38,0))",
        "CAST(1.005 AS DECIMAL(4,3)) > CAST(1 AS DECIMAL(38,0))",
        "CAST(1.005 AS DECIMAL(4,3)) < CAST(1 AS DECIMAL(38,0))",
        "CAST(1.005 AS DECIMAL(4,3)) >= CAST(1 AS DECIMAL(38,0))",
        "CAST(1.005 AS DECIMAL(4,3)) <= CAST(1 AS DECIMAL(38,0))",
        "CAST(1.005 AS DECIMAL(4,3)) <=> CAST(1 AS DECIMAL(38,0))",
        "CAST(1.005 AS DECIMAL(4,3)) IN (CAST(1 AS DECIMAL(38,0)))",
        "CAST(1.005 AS DECIMAL(4,3)) IN (CAST(2 AS DECIMAL(38,0)), CAST(1 AS DECIMAL(38,0)))",
        "CAST(1.005 AS DECIMAL(4,3)) BETWEEN CAST(1 AS DECIMAL(38,0)) AND CAST(2 AS DECIMAL(38,0))",

        # Rounding half AWAY FROM ZERO, on both sides of zero, at the scale the pair resolves to.
        "CAST(0.4 AS DECIMAL(38,38)) = CAST(0 AS DECIMAL(38,0))",
        "CAST(0.4 AS DECIMAL(38,38)) > CAST(0 AS DECIMAL(38,0))",
        "CAST(0.5 AS DECIMAL(38,38)) = CAST(1 AS DECIMAL(38,0))",
        "CAST(0.6 AS DECIMAL(38,38)) = CAST(1 AS DECIMAL(38,0))",
        "CAST(-0.5 AS DECIMAL(38,38)) = CAST(-1 AS DECIMAL(38,0))",

        # THE CONTROLS. The same operand against a decimal(10,0), where the common type is
        # decimal(11,3) and loses nothing: the answers go back to the exact ones. Without these
        # a fix that rounded every decimal comparison would look correct.
        "CAST(1.005 AS DECIMAL(4,3)) = CAST(1 AS DECIMAL(10,0))",
        "CAST(1.005 AS DECIMAL(4,3)) > CAST(1 AS DECIMAL(10,0))",

        # AN INTEGRAL'S WIDTH DECIDES THE ANSWER, which is the row that says an integral takes
        # part in this rather than sitting outside it. Against 4E-32 the same `=` is FALSE for a
        # tinyint (the pair resolves to decimal(38,35), which still holds the value), TRUE for an
        # int (decimal(38,28)) and TRUE for a bigint (decimal(38,18)) -- the wider the integral,
        # the more integer digits it reserves and the sooner the other operand rounds away.
        "CAST(0 AS TINYINT) = CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38))",
        "CAST(0 AS INT) = CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38))",
        "CAST(0 AS BIGINT) = CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38))",
        "CAST(0 AS INT) > CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38))",
        "CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38)) IN (CAST(0 AS INT))",
        "greatest(CAST(0 AS INT), CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38)))",
        # ...and an int against a value that does NOT round away, so the width rule is pinned
        # from both sides.
        "CAST(0 AS INT) = CAST(0.4 AS DECIMAL(38,38))",

        # FLOATING POINT IS NOT PART OF THIS. A decimal against a double is compared as a
        # DOUBLE (#277), not through a common decimal, and these rows keep the new rule from
        # intercepting that one.
        "CAST(0.1 AS DECIMAL(38,38)) = CAST(0.1 AS DOUBLE)",
        "CAST(1.005 AS DECIMAL(4,3)) = CAST(1.005 AS DOUBLE)",

        # Column forms of the same question, so the rule is pinned over real columns and not
        # only over folded literals.
        "d5 = d4",
        "d5 < d4",
        "d2 = d4",
        "d5 IN (d4)",

        # A BARE NULL IN THE LIST CONSTRAINS NOTHING. Spark types it `void`, so the set still
        # resolves through the other members and still rounds -- the first row is TRUE, the same
        # answer the list gives without the NULL. Worth pinning because a NULL member has no
        # value to read a type from, and calling it a string instead would send an otherwise
        # numeric set down the string-promotion rule and lose the match.
        "CAST(1.005 AS DECIMAL(4,3)) IN (CAST(1 AS DECIMAL(38,0)), NULL)",
        "CAST(1.005 AS DECIMAL(4,3)) IN (CAST(2 AS DECIMAL(38,0)), NULL)",
        "CAST(1.005 AS DECIMAL(4,3)) IN (NULL)",
        "CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38)) IN (CAST(0 AS INT), NULL)",
        # The exact control: no rounding, so no match, and the NULL member makes it null rather
        # than false -- ordinary three-valued IN.
        "CAST(1.005 AS DECIMAL(10,3)) IN (CAST(1 AS DECIMAL(10,0)), NULL)",
        # ...and an integral set, which reaches none of this and must not start to. The pair
        # differs only in whether the list holds the operand's exact value.
        "9007199254740993 IN (9007199254740992, NULL)",
        "9007199254740993 IN (9007199254740993, NULL)",
    ],

    # Issue #279, and #306 beside it. Spark evaluates a conditional's branch only over the rows
    # that select it, and the two halves of AND/OR only over the rows the other half left
    # undecided -- so a cast that would fail, an overflow or a division by zero in a branch no row
    # reaches never happens. Under ANSI that is the difference between a write succeeding and
    # failing, and every row of a table is on the wrong side of it.
    #
    # WHY BOTH DIRECTIONS ARE HERE. Half these rows ask whether an unreached error is skipped, and
    # half ask whether a REACHED one still raises. Only the first half fails today, but a corpus
    # carrying only that half would be satisfied by an implementation that evaluated nothing at
    # all, which is a worse bug in the more dangerous direction. The `nullif`/`greatest`/`least`
    # rows are there for the same reason from the other side: they are measured EAGER, so they pin
    # the boundary of the family rather than assuming it.
    #
    # WHY THE CONDITIONS LOOK ROUNDABOUT. The all-null row means a plain `coalesce(a, ...)` selects
    # its second branch on row 2, so an expression whose bad branch is genuinely unreached on every
    # row has to be written around a column that is not null there. `s IS NOT NULL` and
    # `b = 0 OR ...` are doing that work, and they are also the shapes a real CHECK constraint is
    # written in.
    "short-circuit": [
        # The issue's repro, and the same shape through each spelling of the family. The condition
        # is a literal in these, so Spark could answer them by folding rather than by skipping --
        # which is exactly why the column-driven rows below exist. We fold nothing, so they are a
        # real test for us either way.
        "nvl(1, CAST('0x10' AS DOUBLE))",
        "coalesce(1, 1/0)",
        "ifnull(1, CAST('0x10' AS DOUBLE))",
        "if(true, 1, CAST('0x10' AS DOUBLE))",
        "CASE WHEN true THEN 1 ELSE CAST('0x10' AS DOUBLE) END",
        # An unreached CONDITION, not an unreached value: the second WHEN is never asked.
        "CASE WHEN true THEN 1 WHEN CAST('0x10' AS DOUBLE) > 0 THEN 2 END",

        # Column-driven, so no rewrite can decide them ahead of time: whether the branch is
        # evaluated is a property of the ROW. These are the rows that prove the behaviour is
        # per-row laziness rather than constant folding.
        "if(s IS NOT NULL, 0, CAST(s AS INT))",
        "CASE WHEN s IS NOT NULL THEN 0 ELSE CAST(s AS INT) END",
        "coalesce(a, CAST(t AS INT))",
        "coalesce(a, b, CAST(t AS INT))",
        "if(a IS NOT NULL, a, if(b IS NOT NULL, b, CAST(t AS INT)))",

        # AND / OR, which #279 does not mention and which carry the same defect. The first two are
        # the ordinary shape of a CHECK constraint that has to tolerate a zero divisor.
        "b = 0 OR a / b > 1",
        "b <> 0 AND a / b > 1",
        "a IS NOT NULL OR CAST(t AS INT) > 0",
        "a IS NULL AND CAST(t AS INT) > 0",
        "NOT (a IS NULL AND CAST(t AS INT) > 0)",

        # The same two with the guard inverted, so the left operand is FALSE (for OR) or TRUE (for
        # AND) on the zero-divisor row and the right one IS evaluated there. These raise, and they
        # are what stops "skip the right operand" from being read as "skip it whenever the left
        # decided something".
        "b <> 0 OR a / b > 1",
        "b = 0 AND a / b > 1",

        # NULL DOES NOT SHORT-CIRCUIT, which is the subtle half of the rule: AND skips only where
        # the left is FALSE and OR only where it is TRUE, so an UNKNOWN left keeps the right
        # operand live. Each of these pairs differs only in whether the null row reaches the right
        # operand, and the right operand raises on any row that evaluates it -- so the raising one
        # is raising BECAUSE of the null, and the answering one proves the guard is what saved it.
        #
        # `a` is 1 / NULL / INT_MIN, so `a >= -2147483648` is TRUE, NULL, TRUE and
        # `a < -2147483648` is FALSE, NULL, FALSE: the null row is the only one left undecided by
        # the guard, on a schema whose null row nulls EVERY column and so cannot raise by
        # arithmetic. That is why the right operand is a cast of a literal rather than a division.
        "a >= -2147483648 OR CAST('x' AS INT) > 0",
        "a IS NULL OR a >= -2147483648 OR CAST('x' AS INT) > 0",
        "a < -2147483648 AND CAST('x' AS INT) > 0",
        "a IS NOT NULL AND a < -2147483648 AND CAST('x' AS INT) > 0",

        # Reached, and still raising. The other direction.
        "coalesce(a, CAST('0x10' AS DOUBLE))",
        "if(a IS NULL, CAST('0x10' AS DOUBLE), 0)",
        "CASE WHEN a IS NULL THEN CAST('0x10' AS DOUBLE) ELSE 0 END",

        # Eager in Spark, all three -- they raise over the same batch the conditional rows above
        # answer over. They have no branch to skip either, each needing every argument before it can
        # decide anything, so these pin the BOUNDARY of the family rather than assuming it.
        "nullif(a, CAST('0x10' AS DOUBLE))",
        "greatest(a, CAST('0x10' AS DOUBLE))",
        "least(1, CAST('0x10' AS DOUBLE))",

        # A branch nothing selects whose result type depends on an argument's VALUE. `round`'s
        # scale decides the result's precision and scale, and it is read out of row 0 -- so typing
        # this branch cannot be done over zero rows the way every other branch can, and a literal
        # scale would have hidden it. Regression guard for the fallback in `TypeOver`.
        "coalesce(a, round(d1, 1 + 1))",
        "if(1 = 1, a, round(d1, 1 + 1))",

        # An unreached branch still TYPES the result, and still has to type-check. This is the
        # constraint that stops "do not evaluate it" from meaning "do not look at it": dropping the
        # unreached branch would make the first of these a float rather than a double, which is a
        # different VALUE and not merely a different label.
        "if(1 = 1, f, 'abc')",
        "if(1 = 1, a, 'abc')",
        "coalesce(ns, 'abc')",
        # ...and a bare NULL still does not, being `void`. The pair below is the discriminator: a
        # TYPED null constrains the result and an untyped one does not, however alike the two look
        # once they are columns. #293.
        "if(1 = 1, a, NULL)",
        "if(1 = 1, a, CAST(NULL AS STRING))",
        "coalesce(a, NULL)",
        # A pair with no common type is refused however few rows reach it.
        "if(1 = 1, a, bin)",

        # nvl2, #308, which was not registered at all. It is `if(x IS NOT NULL, a, b)` and Spark
        # says so itself -- the refusal below reports `Cannot resolve "(IF((a IS NOT NULL), a,
        # bin))"`, naming a rewrite the expression never mentioned -- so these rows ask whether it
        # behaves like the `if` rows above rather than assuming it inherits them.
        "nvl2(a, a, 0)",
        # Lazy in the ELSE, column-driven: rows 0 and 2 hold 'abc' and '', which the cast refuses
        # under ANSI, and neither reaches it.
        "nvl2(s, 0, CAST(s AS INT))",
        "nvl2(a, a, CAST(s AS INT))",
        # Lazy in the THEN, which the shape above cannot reach: the first argument decides both
        # branches, so a row that skips `a` is a row that took `b`. A first argument no row finds
        # present is what leaves the then-branch unevaluated on every row -- and it still TYPES the
        # result, which is why this answers `0.0` as a double rather than an int.
        "nvl2(NULL, CAST('0x10' AS DOUBLE), 0)",
        # Reached, and still raising, in each branch and in the subject. The subject is the
        # interesting one: it is evaluated on every row whatever the branches say, because it is
        # what decides them.
        "nvl2(a, CAST(s AS INT), 0)",
        "nvl2(a, a, 1/0)",
        "nvl2(CAST(t AS INT), 1, 2)",
        # The subject is read for NULLNESS, not for truth, so unlike `if` it needs no boolean --
        # and needs no scalar either.
        "nvl2(s, a, a)",
        "nvl2(bl, a, a)",
        "nvl2(bin, 1, 2)",
        "nvl2(ts, 1, 2)",
        "nvl2(nested, a, 0)",
        # ...and it takes no part in the result type. Both of these are `int`, not `string`.
        "nvl2(ns, a, 0)",
        # The branches unify exactly as `if`'s do, bare NULL dropped from the fold and all.
        "nvl2(a, a, b)",
        "nvl2(a, a, NULL)",
        "nvl2(a, NULL, 'abc')",
        "nvl2(a, a, d1)",
        "nvl2(a, f, a)",
        "nvl2(a, ns, 0)",
        # A branch nothing selects whose type is read out of a VALUE -- the `TypeOver` fallback,
        # the same guard the `coalesce(a, round(d1, 1 + 1))` row above is.
        "nvl2(a, a, round(d1, 1 + 1))",
        # Refused: no common type, and the message is where the rewrite to `if` is visible.
        "nvl2(a, a, bin)",
        # Strictly three arguments -- WRONG_NUM_ARGS, not a silent default.
        "nvl2(a, a)",
        "nvl2(a, a, 0, 0)",
    ],

    # #293. A bare NULL is `void` in Spark, and EngineeredWood used to materialise one as an
    # all-null STRING column -- so the type was a lie, and one that nothing downstream could see
    # through. Two consequences, and only the second was the one the issue was filed for.
    #
    # ARITHMETIC SIMPLY REFUSED. `a + NULL` arrived as `int + utf8` and threw "arithmetic is not
    # defined for utf8", for every operator and every numeric type. Nothing in the corpus asked,
    # which is why it went unnoticed for as long as it did -- so the operators are enumerated here
    # rather than sampled.
    #
    # AND A TYPE DEPENDED ON THE BATCH. A real string column holding nothing in this batch is the
    # same array as the placeholder, so `greatest` dropped it and answered where Spark refuses.
    # That half cannot be asked here -- this corpus is ONE batch, and every string column in it
    # holds values -- so the rows below pin the REFUSAL and a unit test carries the empty batch.
    # The corpus's job here is the rule; the test's job is that the rule does not move.
    "null-literal": [
        # What `void` is, on its own.
        "NULL",
        "CAST(NULL AS INT)", "CAST(NULL AS BINARY)", "CAST(NULL AS DATE)",
        "CAST(NULL AS DECIMAL(10,2))", "CAST(NULL AS BOOLEAN)", "CAST(NULL AS STRING)",

        # Arithmetic: the void operand takes the OTHER one's type. Every operator, because it was
        # every operator that threw.
        "a + NULL", "NULL + a", "a - NULL", "a * NULL", "a / NULL", "a % NULL",
        # ...and every numeric width, since the answer is the other operand's own type and not a
        # default. `d1 + NULL` is the shape that says so loudest: decimal(11,2) is decimal(10,2)
        # against ITSELF, the digit addition reserves for a carry.
        "b + NULL", "sh + NULL", "f + NULL", "g + NULL", "d1 + NULL", "d3 + NULL",
        # TWO voids have no other operand to take, and Spark does not answer void.
        "NULL + NULL", "NULL - NULL", "NULL * NULL", "NULL / NULL",
        "-NULL", "NULL / 2", "1 + NULL",

        # Where the result IS void, because nothing else constrained it.
        "coalesce(NULL, NULL)", "if(true, NULL, NULL)", "nvl2(a, NULL, NULL)",
        "CASE WHEN true THEN NULL ELSE NULL END", "nullif(NULL, a)",
        # A void that is not a LITERAL null -- the fold has to keep stepping aside for it.
        "coalesce(coalesce(NULL, NULL), a)", "greatest(coalesce(NULL, NULL), a)",
        "coalesce(NULL, NULL) + a",

        # Where it is not: one typed branch settles the whole conditional.
        "coalesce(a, NULL)", "if(true, a, NULL)", "greatest(a, NULL)", "least(a, NULL)",
        "greatest(d1, NULL)", "greatest(NULL, 'x')", "nullif(a, NULL)",
        "coalesce(a, greatest(a, NULL))",

        # greatest/least REFUSE a string against a number, which is the rule the content test
        # used to break: over a batch where `s` held nothing it answered instead. Both orders,
        # because the refusal is not about which side is written first.
        "greatest(a, s)", "least(a, s)", "greatest(s, a)",

        # The functions that read a void argument. None of these was wrong -- a string
        # placeholder reads as null too -- but each is a reader that now has to say so itself.
        "length(NULL)", "upper(NULL)", "concat('x', NULL)", "substring(NULL, 1, 2)",
        "round(NULL)", "round(NULL, 2)", "year(NULL)", "date_format(NULL, 'y')",
        "NULL || 'x'", "NULL LIKE 'a'", "s LIKE NULL",
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

    Binary needs the same treatment and does not get it here: the driver has already converted it
    to hex by the time these values arrive. See `_expr_value` in spark_driver.py for why. #295.
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

    ident_exprs = list(dict.fromkeys(e for g in IDENTIFIER_CASE_GROUPS.values() for e in g))
    ident = _run_driver({"expressions": ident_exprs, "schema": IDENTIFIER_CASE_SCHEMA,
                         "rows": IDENTIFIER_CASE_ROWS, "conf": CONF})
    if not ident.get("ok"):
        raise SystemExit("driver error (identifier-case): " + json.dumps(ident)[:2000])

    ident_by_expr = {r["expression"]: r for r in ident["results"]}

    fixture = {
        "_comment": "Generated by harvest_expression_corpus.py. Do not edit by hand. "
                    "Answers come from Spark and are only valid under `conf`.",
        # Derived, not typed. It said "pyspark 4.0.1" while `spark_version` beside it read
        # 4.0.3, because the version moved with the venv and the prose did not.
        #
        # It says SPARK, not pyspark, and the distinction is the point: this value is
        # `spark.version` off the live session -- the JVM that actually computed the answers --
        # not the version of the pyspark package that started it. They agree in a stock venv and
        # are free not to, and it is the JVM's version that an answer is a property of. Same
        # reasoning as recording `java_version`. The tier keeps its name because that is the
        # harness the harvest runs through, not a version claim about delta-spark.
        "source": f"delta-spark interop tier (spark {result['spark_version']})",
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
        "identifier_case": {
            "_comment": "A SECOND SCHEMA, under the same conf. Identifier resolution is a "
                        "property of the schema, and the questions #181 asks -- ambiguity, and "
                        "whether backticks make a reference exact -- need names that differ only "
                        "in case. Adding those to the schema above would have made `a` ambiguous "
                        "for the whole corpus.",
            "conf": ident["conf"],
            "schema": IDENTIFIER_CASE_SCHEMA,
            "rows": IDENTIFIER_CASE_ROWS,
            "groups": {name: [ident_by_expr[e] for e in exprs if e in ident_by_expr]
                       for name, exprs in IDENTIFIER_CASE_GROUPS.items()},
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
    print(f"  identifier case (second schema): {len(ident_exprs)} expressions")
    print(f"  spark {result['spark_version']} on java {result['java_version']}")


if __name__ == "__main__":
    main()

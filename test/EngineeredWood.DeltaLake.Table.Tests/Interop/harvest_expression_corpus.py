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
    # #298. The string rows answer in BOTH dialects and answer DIFFERENTLY -- ANSI refuses a
    # string that will not cast, legacy reads it as null, and the widening picks a different
    # target again. One harvest would record half a rule.
    "nullif-coercion",
    # #314. The vocabulary itself is dialect-independent, but what a word OUTSIDE it does is not:
    # ANSI raises CAST_INVALID_INPUT where legacy answers NULL, and every refusal in the group is
    # a row the ANSI switch moves. Recording one dialect would describe the accept-set and leave
    # the whole boundary around it unmeasured.
    "string-to-boolean",
    # #316. The trim itself is dialect-independent -- both dialects trim the same set -- and what
    # a string it does NOT rescue then does is not: ANSI raises CAST_INVALID_INPUT where legacy
    # answers NULL. Since the whole point of the group is which strings survive the trim, the
    # refusals ARE the measurement, and one harvest would record only half of each of them.
    "string-trim",
    # #318. The grammar is dialect-independent and the group is mostly REFUSALS, which are not:
    # ANSI raises CAST_INVALID_INPUT where legacy answers NULL. Since what the group measures is
    # which strings are dates at all, the refusals ARE the measurement, and one harvest would
    # record only half of each of them -- the same argument as `string-trim` above, which is the
    # group this one was spun off from.
    "temporal-text",
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
    # #319. The nullability rule itself ought to be dialect-independent -- it is a structural
    # property of the tree, not a failure mode -- but most of the group's rows are casts and
    # divisions that RAISE under ANSI and answer NULL without it, so what the fold has to step
    # aside for is completely different in the two dialects. Recorded rather than assumed, for
    # the reason decimal-common-type is: the registry has a legacy variant that shares this rule.
    "null-propagation",
    # #280. Measured identical under both dialects, all 24 rows -- the least common type is a
    # coercion rule and the ANSI switch moves overflow behaviour, not coercion. Recorded rather
    # than assumed, for the reason nullif-equality is: the registry has a legacy variant that
    # shares this rule, and "it cannot differ" is a claim until a second harvest says so.
    "decimal-common-type",
    # #281. The TYPES are identical under both dialects, all 56 rows -- `literalPickMinimumPrecision`
    # is a coercion rule, and the ANSI switch moves overflow and cast behaviour rather than
    # coercion. Measured rather than assumed, and the second harvest earned its place anyway: two
    # rows do differ, `2 / d2` and `2 % d2`, which raise DIVIDE_BY_ZERO on the boundary row under
    # ANSI and answer null without it. One harvest would have recorded the rule with no evidence
    # that the dialect leaves it alone, which is the whole reason decimal-common-type is here too.
    "decimal-literal-precision",
    # #282. The sign of a zero ought to be dialect-independent -- negation, rounding and the
    # number parse are computations, and the ANSI switch moves overflow and cast BEHAVIOUR rather
    # than arithmetic -- but the group is half made of casts, and #258, #314 and #316 each found a
    # cast whose answer the switch does move. Asked twice to find out whether this is another, for
    # the reason decimal-common-type is asked twice: the registry has a legacy variant that shares
    # every one of these code paths.
    "negative-zero",
    # #296. THE WHOLE POINT OF THE GROUP is that the two dialects read a string operand as
    # different types, so one harvest would record half a rule -- and the ANSI half is the half
    # that hides, since a refusal reads as agreement against a registry that throws. It was the
    # legacy section of `unicode-digits` that found the defect for exactly that reason.
    "arithmetic-string-coercion",
    # #333. THE MEASUREMENT IS THE LEGACY COLUMN. Under ANSI every equality in the group is refused
    # at analysis -- that half belongs to #286 -- so an ANSI-only harvest would record a wall of
    # refusals and leave `BooleanEquality` itself entirely unmeasured.
    "boolean-equality",
    # #286. The ANSI and legacy analyzers refuse DIFFERENT sets -- boolean joins the numeric
    # family for equality under legacy and under ANSI it does not -- so the rule cannot be read
    # off one dialect. Both halves are the measurement.
    "comparison-families")

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

# ONE COLUMN PER TYPE EngineeredWood MODELS, for the 9x9 matrix of #286. Every entry is a column
# of SCHEMA above and they are deliberately columns rather than literals: a literal folds, and a
# fold can take a different route through the analyzer than a reference does.
COMPARISON_COLUMNS = ("a", "b", "g", "d1", "dt", "ts", "bl", "bin", "s")

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
    "subnormal-floats": [
        # Issue #288. Below 2.2250738585072014e-308 a double stops carrying 53 bits: the step
        # between neighbours stays a fixed 2^-1074 however small the value gets, so at the bottom
        # of the range half a step is half the value itself and ONE digit round-trips. The
        # G15/G16/G17 ladder that #248 built starts at fifteen because fifteen is where a NORMAL
        # double's shortest form can first appear -- so it printed 9.88131291682493E-324 where
        # Spark prints two digits, and the whole subnormal range was rendering long.
        #
        # What this group is FOR is the exact digits, so every expression below reaches a
        # different part of the range: the smallest double there is, the ones just above it where
        # one and two digits compete, the middle where the answer is short for a different reason,
        # and the top where seventeen digits are genuinely needed.

        # The bottom of the range, where the step is a large fraction of the value.
        "CAST(CAST(4.9e-324 AS DOUBLE) AS STRING)",
        "CAST(CAST(1e-323 AS DOUBLE) AS STRING)",
        "CAST(CAST(1.5e-323 AS DOUBLE) AS STRING)",
        "CAST(CAST(2e-323 AS DOUBLE) AS STRING)",
        "CAST(CAST(-1e-323 AS DOUBLE) AS STRING)",

        # The middle, where a short answer comes from the value being near a round decimal rather
        # than from the step being coarse.
        "CAST(CAST(1e-320 AS DOUBLE) AS STRING)",
        "CAST(CAST(1e-315 AS DOUBLE) AS STRING)",
        "CAST(CAST(1e-310 AS DOUBLE) AS STRING)",
        "CAST(CAST(1.23456789e-310 AS DOUBLE) AS STRING)",

        # The top, one value below the smallest normal double, where sixteen digits are needed --
        # and a value whose sixteenth digit .NET Framework's own formatter gets wrong.
        "CAST(CAST(2.2250738585072011e-308 AS DOUBLE) AS STRING)",
        "CAST(CAST(9.337299911372906e-309 AS DOUBLE) AS STRING)",

        # A FLOAT is subnormal below 1.17549435e-38 and asks the same question at its own width.
        "CAST(CAST(1.4e-45 AS FLOAT) AS STRING)",
        "CAST(CAST(7e-45 AS FLOAT) AS STRING)",
        "CAST(CAST(1e-44 AS FLOAT) AS STRING)",
        "CAST(CAST(1.618e-42 AS FLOAT) AS STRING)",
        "CAST(CAST(1e-40 AS FLOAT) AS STRING)",
        "CAST(CAST(1.1754942e-38 AS FLOAT) AS STRING)",

        # Reached by arithmetic rather than written down, so at least one answer is not a constant
        # fold -- and so the underflow itself is measured rather than assumed.
        "CAST(g * CAST(1e-323 AS DOUBLE) AS STRING)",
        "CAST(CAST(1e-300 AS DOUBLE) * CAST(1e-20 AS DOUBLE) AS STRING)",
        "CAST(CAST(4.9e-324 AS DOUBLE) / 2 AS STRING)",

        # And the other consumer of the same digits: Spark reaches a decimal from a double through
        # Double.toString, so whatever this group settles the DECIMAL cast inherits.
        "CAST(CAST(1e-323 AS DOUBLE) AS DECIMAL(38,38))",
        "CAST(CAST(1e-310 AS DOUBLE) AS DECIMAL(10,2))",
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

        # A STRING operand is a rule of its own and has a group of its own -- see
        # `nullif-coercion` below, which this group's first harvest is what found. #298.

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

    # A STRING operand of `nullif`, which is the ordinary comparison coercion and was a TEXT
    # comparison instead. #298, found by `nullif-equality` above.
    #
    # Spark rewrites `nullif(a, b)` to `if(a = b, NULL, a)`, so the pair takes the `=` rule
    # (#180/#259): the string is cast to the OTHER operand's type, and under ANSI one that will
    # not cast refuses the comparison. The trailing `a` is the UNCAST operand, so the result keeps
    # the first operand's type and its original text -- which is why the rows below ask both what
    # matches and what the answer is when it does not.
    #
    # This is the one call site that never got the rule, so the `=` controls at the end are not
    # decoration: they are the same pairs through the path that already coerces, and a row where
    # the two disagree means the two sites have drifted apart again.
    "nullif-coercion": [
        # THE DISCRIMINATORS. Each of these casts to the other operand's value while differing
        # from it as text, so a text comparison answers the first operand where Spark answers
        # NULL -- a wrong VALUE, not merely a different error class.
        "nullif(' 1', 1)", "nullif('01', 1)", "nullif('+1', 1)", "nullif('1 ', 1)",
        "nullif('1.0', 1)", "nullif('1e0', 1)",

        # ...and the other way round. Which side is the string decides nothing about which one
        # moves, but it decides the RESULT, so both orders are asked.
        "nullif(1, ' 1')", "nullif(1, '01')", "nullif(1, '1.0')",

        # A string that casts and does NOT match: the answer has to be the original text, not the
        # value the comparison went through.
        "nullif(' 2', 1)", "nullif('1.5', 1)", "nullif('0.10', 0.1)",

        # Agrees BY LUCK, and is here to say so: '1' and the rendering of 1 are the same string,
        # so the text route and the cast route happen to meet.
        "nullif('1', 1)",

        # A string no numeric cast accepts. ANSI refuses the comparison; legacy reads it as NULL,
        # which is not equal, so the first operand comes back.
        "nullif('abc', 1)", "nullif('x', 1e308)", "nullif('', 1)", "nullif(s, a)",

        # BOTH operands strings, which is the one pair a text comparison IS right for. The fix
        # must leave these alone.
        "nullif('1.0', '1')", "nullif('abc', 'abc')", "nullif(s, t)", "nullif(ns, s)",

        # THE WIDENING, which is #180's half of the rule and splits the dialects: ANSI casts the
        # string to bigint or double, legacy to the operand's OWN type, where it can overflow or
        # round away.
        "nullif('32768', CAST(32767 AS SMALLINT))",
        "nullif('0.1', CAST(0.1 AS FLOAT))",
        "nullif('1000000000000000000000000000001', d4)",
        "nullif(fs, a)", "nullif(ns, a)", "nullif(ns, f)",

        # The kinds that take no widening in either dialect -- the string is cast straight to the
        # other operand's type.
        "nullif('true', true)", "nullif('TRUE', bl)", "nullif('t', true)",
        "nullif('2026-08-11', dt)", "nullif(dt, '2026-08-11')",
        "nullif('2026-08-11 12:30:00', ts)", "nullif('2026-08-11', ts)",

        # A BINARY against a string is the pair where the BINARY moves and is rendered as text
        # (#259/#295), so the existing text route is right for it and must survive.
        "nullif(X'41', 'A')", "nullif('A', X'41')", "nullif(bin, s)",

        # THE NULL MASK. A relational operator evaluates nothing once an operand is null, so a
        # string that would not cast is never read -- and never refused -- opposite one.
        # Without this a batch mixing such a row with an ordinary one would refuse where Spark
        # answers. `<=>` has no such short-circuit and is asked beside it to show the difference.
        "nullif('abc', CAST(NULL AS INT))",
        "nullif(CAST(NULL AS STRING), 1)",
        "'abc' = CAST(NULL AS INT)",
        "'abc' <=> CAST(NULL AS INT)",

        # THE OTHER HALF OF THE SAME CALL. `=` casts BOTH operands to their least common type,
        # and once that type gives up scale the comparison is made on ROUNDED values (#280).
        # `nullif` reaches the rule through the same question the string rows do, so asking only
        # about strings would leave half of what the coercion returns unmeasured -- and #280
        # measured every other operator over these pairs without a `nullif` among them.
        "nullif(CAST(1.005 AS DECIMAL(4,3)), CAST(1 AS DECIMAL(38,0)))",
        "nullif(CAST(1 AS DECIMAL(38,0)), CAST(1.005 AS DECIMAL(4,3)))",
        "nullif(CAST(0.4 AS DECIMAL(38,38)), CAST(0 AS DECIMAL(38,0)))",
        # An INTEGRAL counts and its WIDTH decides the answer, because it unifies as the decimal
        # that holds it: against decimal(38,38) the common scale is 35, 28 and 18 for a tinyint,
        # an int and a bigint, and 4E-32 survives only the first.
        "nullif(CAST(4E-32 AS DECIMAL(38,38)), CAST(0 AS TINYINT))",
        "nullif(CAST(4E-32 AS DECIMAL(38,38)), CAST(0 AS INT))",
        "nullif(CAST(4E-32 AS DECIMAL(38,38)), CAST(0 AS BIGINT))",
        # ...and the controls for those, through the operator that already rounds.
        "CAST(4E-32 AS DECIMAL(38,38)) = CAST(0 AS TINYINT)",
        "CAST(4E-32 AS DECIMAL(38,38)) = CAST(0 AS INT)",
        "CAST(4E-32 AS DECIMAL(38,38)) = CAST(0 AS BIGINT)",

        # CONTROLS: the same pairs through `=`, which already coerces. These are what say the two
        # call sites agree rather than each holding an opinion.
        "' 1' = 1", "'1.0' = 1", "'1e0' = 1", "'abc' = 1", "'32768' = CAST(32767 AS SMALLINT)",
        "'0.1' = CAST(0.1 AS FLOAT)", "X'41' = 'A'", "'true' = true", "'2026-08-11' = dt",
    ],

    # `CAST(<string> AS BOOLEAN)`, where Spark reads a WORD out of a vocabulary and we read one
    # of `bool.TryParse`'s two -- and where a numeric-looking STRING must not be read as a number
    # at all. #314, found by the `nullif-coercion` group above.
    #
    # The group is deliberately wider than the string cast it is named for. Two of its sections
    # are the CONTROLS that make the string rows mean something: the numbers, which take the
    # branch a numeric string must not reach, and the comparison forms, which reach this cast
    # without a CAST being written -- since #298 a string against a boolean operand is coerced,
    # so `bl = 't'` in a CHECK constraint is a string-to-boolean cast under another name.
    "string-to-boolean": [
        # THE ACCEPT-SET, in each case. `t`/`y`/`yes` and `f`/`n`/`no` are the eight words
        # `bool.TryParse` never knew; `true`/`false` are the two it did.
        "CAST('true' AS BOOLEAN)", "CAST('TRUE' AS BOOLEAN)", "CAST('True' AS BOOLEAN)",
        "CAST('t' AS BOOLEAN)", "CAST('T' AS BOOLEAN)",
        "CAST('y' AS BOOLEAN)", "CAST('Y' AS BOOLEAN)",
        "CAST('yes' AS BOOLEAN)", "CAST('YES' AS BOOLEAN)", "CAST('Yes' AS BOOLEAN)",
        "CAST('1' AS BOOLEAN)",
        "CAST('false' AS BOOLEAN)", "CAST('FALSE' AS BOOLEAN)", "CAST('False' AS BOOLEAN)",
        "CAST('f' AS BOOLEAN)", "CAST('F' AS BOOLEAN)",
        "CAST('n' AS BOOLEAN)", "CAST('N' AS BOOLEAN)",
        "CAST('no' AS BOOLEAN)", "CAST('NO' AS BOOLEAN)",
        "CAST('0' AS BOOLEAN)",

        # TRIMMED -- and WHICH whitespace is a question of its own, because the two runtimes do
        # not agree on what whitespace is. The tab and the newline below are REAL characters in
        # the SQL text, not SQL escape sequences, so what these rows measure is the trim and not
        # the tokenizer.
        "CAST(' t ' AS BOOLEAN)", "CAST('  true  ' AS BOOLEAN)",
        "CAST('\ttrue' AS BOOLEAN)", "CAST('true\n' AS BOOLEAN)",
        # ...and the BOUNDARY between the two rules: a NO-BREAK SPACE. .NET's `string.Trim`
        # removes it, because it is Unicode whitespace; Spark's `trimAll` removes bytes <= 0x20
        # and leaves it. If the two trims differ anywhere they differ here.
        "CAST('\u00a0true' AS BOOLEAN)",
        # Whitespace ALONE, which trims down to the empty string, and the empty string itself.
        "CAST(' ' AS BOOLEAN)", "CAST('' AS BOOLEAN)",

        # REFUSED, and each refusal carries as much of the rule as an acceptance does: `on`/`off`
        # are a vocabulary other systems have and this one does not, and a PREFIX of an accepted
        # word is not accepted.
        "CAST('on' AS BOOLEAN)", "CAST('off' AS BOOLEAN)",
        "CAST('tr' AS BOOLEAN)", "CAST('ye' AS BOOLEAN)", "CAST('truex' AS BOOLEAN)",
        "CAST('yeah' AS BOOLEAN)", "CAST('t t' AS BOOLEAN)",

        # THE OTHER DIRECTION, and the half that answers wrongly rather than loudly: a
        # numeric-looking STRING is refused where the NUMBER is accepted. `'1'` and `'0'` above
        # agree by luck -- they are in the vocabulary as text, not as numbers.
        "CAST('2' AS BOOLEAN)", "CAST('-1' AS BOOLEAN)", "CAST('0.0' AS BOOLEAN)",
        "CAST('1.0' AS BOOLEAN)", "CAST('1e0' AS BOOLEAN)", "CAST('+1' AS BOOLEAN)",
        "CAST(ns AS BOOLEAN)", "CAST(fs AS BOOLEAN)", "CAST(s AS BOOLEAN)",

        # ...and the NUMBERS themselves, which is the branch those strings must not reach. Any
        # non-zero is true, including a negative and a NaN.
        "CAST(0 AS BOOLEAN)", "CAST(1 AS BOOLEAN)", "CAST(2 AS BOOLEAN)", "CAST(-1 AS BOOLEAN)",
        "CAST(a AS BOOLEAN)", "CAST(b AS BOOLEAN)", "CAST(g AS BOOLEAN)", "CAST(d1 AS BOOLEAN)",
        "CAST(CAST(-0.0 AS DOUBLE) AS BOOLEAN)", "CAST(CAST('NaN' AS DOUBLE) AS BOOLEAN)",
        "CAST(bl AS BOOLEAN)", "CAST(true AS BOOLEAN)",

        # The sources that are NEITHER a number nor a string. A binary RENDERS as text and must
        # not be read as one -- `X'74727565'` is the bytes of "true" -- and a temporal has no
        # boolean in it at all.
        "CAST(X'74727565' AS BOOLEAN)", "CAST(bin AS BOOLEAN)",
        "CAST(dt AS BOOLEAN)", "CAST(ts AS BOOLEAN)",
        # A null carries no text to be malformed, in either direction.
        "CAST(CAST(NULL AS STRING) AS BOOLEAN)", "CAST(NULL AS BOOLEAN)",

        # TRY_CAST follows the SAME set. That is what says the vocabulary belongs to the
        # conversion and not to a dialect branch above it -- otherwise `try_cast`, which nulls
        # under both dialects, could have had a set of its own.
        "TRY_CAST('t' AS BOOLEAN)", "TRY_CAST('yes' AS BOOLEAN)", "TRY_CAST('2' AS BOOLEAN)",
        "TRY_CAST('tr' AS BOOLEAN)", "TRY_CAST(s AS BOOLEAN)", "TRY_CAST(X'74727565' AS BOOLEAN)",

        # THE REACH, which is why this is not only a CAST defect. Since #298 a string against a
        # boolean operand is cast to boolean, so the vocabulary decides these too -- and the last
        # two are the shape a Spark-written CHECK constraint actually has.
        "'t' = true", "'y' = bl", "'no' = false", "'on' = true", "'2' = true",
        "nullif('t', true)", "nullif('no', bl)", "nullif('on', true)",
        "if('yes' = bl, 1, 0)", "coalesce(CAST('y' AS BOOLEAN), false)",
        "bl = 't'", "NOT (bl = 'n')",
    ],

    # WHICH whitespace a string cast trims. #316, found by the `string-to-boolean` group above,
    # which asked the question for one target and got an answer that belongs to all of them.
    #
    # Spark trims with `UTF8String.trimAll`: leading and trailing BYTES of 0x20 or below. .NET's
    # `string.Trim` removes UNICODE whitespace. NEITHER SET CONTAINS THE OTHER, so the rows below
    # come in two halves that fail in OPPOSITE directions, and a group carrying only one half
    # would read as "we are too strict" or "too lenient" instead of "we are using the wrong set".
    #
    # Every character is written as a python escape and reaches Spark as one real character; none
    # of them is a SQL escape sequence, so what these rows measure is the trim rather than the
    # tokenizer.
    #
    # The tail of the group is a THIRD rule and is here because it is the same question: Spark's
    # `trim`/`ltrim`/`rtrim` FUNCTIONS remove the space alone. Three rules for one word, and each
    # of the other two is the wrong answer in a different direction.
    "string-trim": [
        # --- SPARK TRIMS IT AND .NET DOES NOT. `char.IsWhiteSpace` is false below 0x09, and for
        # 0x0E through 0x1F, so a `Trim` leaves these in place and the parse then refuses a string
        # Spark reads.
        "CAST('1' AS INT)", "CAST('1' AS INT)", "CAST('1' AS INT)",
        "CAST('1' AS INT)", "CAST('1' AS INT)", "CAST('1' AS INT)",
        "CAST('1' AS BIGINT)", "CAST('1' AS SMALLINT)", "CAST('1' AS TINYINT)",
        "CAST('1.5' AS DOUBLE)", "CAST('1.5' AS FLOAT)",
        "CAST('1.5' AS DECIMAL(10,2))",
        "CAST('2026-08-11' AS DATE)", "CAST('2026-08-11' AS DATE)",
        "CAST(CAST('2026-08-11 12:30:00' AS TIMESTAMP) AS STRING)",
        "CAST('true' AS BOOLEAN)",
        "'1' = 1",

        # --- .NET TRIMS IT AND SPARK DOES NOT. This is the half that answers WRONGLY rather than
        # loudly: a `Trim` removes the character and the parse then succeeds, so a CHECK
        # constraint admits a row Spark rejects.
        "CAST(' 1' AS INT)", "CAST('1 ' AS INT)", "CAST(' 1 ' AS INT)",
        "CAST(' 1' AS BIGINT)", "CAST(' 1' AS SMALLINT)", "CAST(' 1' AS TINYINT)",
        "CAST(' 1.5' AS DOUBLE)", "CAST(' 1.5' AS FLOAT)",
        "CAST(' 1.5' AS DECIMAL(10,2))",
        # THE TEMPORAL TARGETS ARE THE ONES THAT NEED MORE THAN A TRIM. `DateTimeOffset.TryParse`
        # skips `char.IsWhiteSpace` ITSELF, at either end, so removing the `Trim` above them does
        # not reach these rows -- measured in .NET, it reads U+00A0 + '2026-08-11' as the date.
        #
        # The TIMESTAMP rows are wrapped in a cast to STRING, and only for a reason about the
        # fixture: PySpark localises a timestamp to the DRIVER's zone on collect, so a bare
        # timestamp answer records the harvest machine rather than the session's UTC and has to
        # be excluded from the comparison. Spark's own rendering crosses as text in the pinned
        # zone. The refusing rows refuse either way; wrapping all of them keeps the block one
        # shape.
        "CAST(' 2026-08-11' AS DATE)", "CAST('2026-08-11 ' AS DATE)",
        "CAST(CAST(' 2026-08-11 12:30:00' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00 ' AS TIMESTAMP) AS STRING)",
        # ...and two more of the same kind, so the rule is "Unicode whitespace above 0x20" rather
        # than "the no-break space".
        "CAST('　1' AS INT)", "CAST(' 1' AS INT)", "CAST(' 1' AS INT)",
        # Whitespace to .NET and a VALUE to Spark, with nothing else in it.
        "CAST(' ' AS INT)", "CAST(' ' AS DOUBLE)",
        # The comparison coercion, which is where this costs a wrong answer rather than a
        # different error class: since #180 a string against a number is cast, not compared as
        # text. `nullif` reaches the same cast through #298.
        "' 1' = 1", "' 1' > 0", "nullif(' 1', 1)",
        "' 2026-08-11' = dt",

        # --- TRIMMED BY BOTH, which is what says the fix did not simply stop trimming. The whole
        # 0x09-0x0D run plus the space, at both ends.
        "CAST(' 1 ' AS INT)", "CAST('\t1\n' AS INT)", "CAST('\r\n1' AS BIGINT)",
        "CAST('1' AS INT)", "CAST('1' AS INT)",
        "CAST('  1.5  ' AS DOUBLE)", "CAST(' 1.5 ' AS DECIMAL(10,2))",
        "CAST(' 2026-08-11 ' AS DATE)",
        "CAST(CAST('  2026-08-11 12:30:00  ' AS TIMESTAMP) AS STRING)",
        "' 1 ' = 1",
        # Whitespace ALONE under both rules, and the empty string it trims down to.
        "CAST('  ' AS INT)", "CAST('\t' AS INT)", "CAST('' AS INT)",

        # --- INTERIOR whitespace is trimmed by neither rule, so a fix that stripped instead of
        # trimming would show up here.
        "CAST('1 2' AS INT)", "CAST('1 2' AS INT)", "CAST('12' AS INT)",
        "CAST('2026-08 -11' AS DATE)",

        # --- The parses that sit BESIDE the shared one and have to take the same trim: Java's
        # trailing type suffix on a floating literal (#258), and a sign, which must still be the
        # first thing the parse sees once the trim is done.
        "CAST(' 1d ' AS DOUBLE)", "CAST('1d' AS DOUBLE)", "CAST(' 1d' AS DOUBLE)",
        "CAST(' -1' AS INT)", "CAST('-1' AS INT)", "CAST(' -1' AS INT)",
        "CAST('- 1' AS INT)",

        # --- THE TRIM FUNCTIONS ARE A THIRD RULE, which is the question the issue raised and this
        # is the answer: `trim(str)` removes the SPACE and nothing else. Not the cast rule above,
        # and not .NET's. Bracketed with `concat` so a surviving tab or space is visible in the
        # recorded answer rather than being something the reader has to take on trust.
        "concat('[', trim('  x  '), ']')",
        "concat('[', trim('\tx\t'), ']')",
        "concat('[', trim('\nx\n'), ']')",
        "concat('[', trim('x'), ']')",
        "concat('[', trim(' x '), ']')",
        "concat('[', trim('\t x \t'), ']')",
        "concat('[', ltrim('  x'), ']')", "concat('[', ltrim('\tx'), ']')",
        "concat('[', rtrim('x  '), ']')", "concat('[', rtrim('x\t'), ']')",
        # ...and LIKE trims NOTHING, under any of the three, so a padded string is not the bare
        # one. The `'x'` patterns are what make these DISTINGUISHING -- they are false, and an
        # accidental trim anywhere in the path would turn them true. The `'%x'` row is the control
        # beside the tab one saying the tab is still there to be matched; on its own it would be
        # true either way and would pin nothing.
        "'  x  ' LIKE 'x'", "'\tx' LIKE 'x'", "'\tx' LIKE '%x'",
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

    # #319. Spark DISCARDS the operand of IS NULL / IS NOT NULL when the operand is provably
    # non-nullable, so an error inside it never happens. That is `NullPropagation`, and it is NOT
    # the per-row laziness of the `short-circuit` group: it fires before a row is read, and it
    # fires on an operand every row of which would otherwise be evaluated.
    #
    # THE GROUP CARRIES BOTH DIRECTIONS, for the reason `short-circuit` does. Half the rows ask
    # whether the fold happens; the other half ask whether an operand Spark calls NULLABLE is
    # still evaluated and still raises. A group carrying only the first half is satisfied by an
    # implementation that folds everything -- which answers `x IS NOT NULL` true for a genuine
    # null and admits a row Spark rejects, the one direction a CHECK constraint must not fail in.
    #
    # EVERY ROW IS LITERAL-DRIVEN OR COLUMN-NULLABLE, because a non-nullable COLUMN cannot be
    # expressed here at all: `cmd_expression_corpus` builds its frame with `createDataFrame([],
    # ddl)` and its rows from `SELECT CAST(...)`, so every column of this schema is nullable. The
    # half of the rule that reads a column's declared nullability is out of reach of this fixture
    # by construction, and is declared in SparkEvaluationCorpusTests instead.
    "null-propagation": [
        # The fold, once per shape that reaches it. Each operand contains a cast that raises, and
        # each wrapper is what makes the operand non-nullable.
        "CASE WHEN (CAST('abc' AS INT) > 1) THEN 1 ELSE 2 END IS NULL",
        "CASE WHEN (CAST(s AS INT) > 1) THEN 1 ELSE 2 END IS NOT NULL",
        "if(CAST('abc' AS INT) > 1, 1, 2) IS NULL",
        "coalesce(CAST(s AS INT), 0) IS NOT NULL",
        "coalesce(1, CAST('abc' AS INT)) IS NULL",
        "nvl(CAST(s AS INT), 0) IS NOT NULL",
        "ifnull(CAST(s AS INT), 0) IS NOT NULL",
        "nvl2(CAST(s AS INT), 1, 2) IS NULL",
        "greatest(CAST(s AS INT), 1) IS NULL",
        "least(CAST(s AS INT), 1) IS NULL",

        # ARITHMETIC, WHICH WE DELIBERATELY DO NOT FOLD -- three rows that between them say why.
        # Integral arithmetic is non-nullable, so Spark answers the first without adding. DECIMAL
        # arithmetic follows the PROMOTED precision instead: the second is two non-null literals
        # whose sum Spark calls NULLABLE (it evaluates, and raises at plan time), and the third is
        # the same sum through casts, whose sum Spark calls non-nullable. No rule phrased in terms
        # of the arguments' nullability can separate the last two, so arithmetic is off the
        # allow-list and the first and third are declared differences. The middle one AGREES, and
        # is the control: it is what an implementation that folded arithmetic would get wrong in
        # the dangerous direction.
        "(2147483647 + 1) IS NULL",
        "(99999999999999999999999999999999999999 + 1) IS NULL",
        "(CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)) + CAST(1 AS DECIMAL(38,0))) IS NULL",

        # Predicates are never null themselves, so a doubled IS NULL folds where the inner one
        # alone raises -- and `<=>` answers for a null pair, which makes it non-nullable whatever
        # its operands are.
        "(CAST(s AS INT) IS NULL) IS NULL",
        "(CAST(s AS INT) <=> 1) IS NULL",

        # COMPARISONS, WHICH WE DELIBERATELY DO NOT FOLD EITHER -- the same trap as arithmetic,
        # found by the review of PR #321 after the first version DID fold them. A comparison that
        # needs coercion inserts a CAST, and a cast is nullable: `1 = 1` and `'a' = 'b'` are
        # non-nullable while `'abc' = 1` and `'abc' > 1` are NULLABLE in BOTH dialects. Two
        # non-null literals compared in every case, so the operands' nullability cannot separate
        # them. The legacy answers are what make this urgent rather than tidy: `('abc' = 1) IS
        # NULL` is TRUE there, so folding it to false is a WRONG VALUE, not a suppressed error.
        "('abc' = 1) IS NULL",
        "('abc' > 1) IS NULL",
        "(1 = 1) IS NULL",
        "('a' = 'b') IS NULL",
        # ...and the over-claim travels through the connectives, so AND carries a row of its own.
        "(true AND 'abc' = 1) IS NULL",
        # `IN` is worse: nullable under ANSI and NON-nullable under legacy, for the same set.
        "('abc' IN (1)) IS NULL",
        "('abc' IN ('x')) IS NULL",
        # `<=>` is the one comparison that DOES fold, because its non-nullability is structural
        # rather than a property of the coercion. Same operands as the `=` row above, opposite
        # answer -- which is what says the rule is about the OPERATOR and not about the literals.
        "('abc' <=> 1) IS NULL",

        # A non-boolean CONDITION, which Spark refuses at ANALYSIS under both dialects. The fold
        # must not answer for it, which means the condition's type has to be checked over no rows
        # -- the first version of this change checked it per row and so never checked it at all
        # once the fold skipped the evaluation.
        "if(1, 1, 2) IS NOT NULL",
        "CASE WHEN 1 THEN 1 ELSE 2 END IS NOT NULL",

        # A folded operand whose result TYPE depends on an argument's VALUE. `round`'s scale is
        # read from row 0, so typing this over zero rows fails and `TypeOver` retries WITH rows --
        # which reads the malformed cast the fold existed to avoid. Declared; the residual is the
        # one `TypeOver` names for itself.
        "coalesce(1, round(CAST('abc' AS DOUBLE), 1 + 1)) IS NULL",
        # NOT and AND over a folded operand, so the structural half of the rule is exercised
        # through something the fold actually reaches. The second is the shape a real constraint
        # takes: a conjunct Spark discards beside one that still decides the row.
        "NOT (coalesce(CAST(s AS INT), 0) IS NULL)",
        "(coalesce(CAST(s AS INT), 0) IS NOT NULL) AND a > 0",
        "1 IS NULL", "'x' IS NOT NULL", "true IS NOT NULL",

        # ── The other direction: Spark calls these NULLABLE, evaluates, and raises. ──
        # A cast is nullable whatever its source -- `Cast.forceNullable` is a property of the type
        # pair, not of ANSI -- which is why the whole cast family stays evaluated.
        "CAST(s AS INT) IS NULL",
        "CAST('abc' AS INT) IS NOT NULL",
        "try_cast(s AS INT) IS NULL",
        # Nullable whatever their arguments.
        "nullif(CAST(s AS INT), 0) IS NULL",
        "(1 / 0) IS NULL",
        "(1 % 0) IS NULL",
        "round(CAST(s AS DOUBLE), 0) IS NULL",
        # A CASE with no ELSE is null for a row matching no branch.
        "CASE WHEN (CAST(s AS INT) > 1) THEN 1 END IS NULL",
        # One nullable result is enough to make the whole conditional nullable.
        "CASE WHEN (CAST('abc' AS INT) > 1) THEN 1 ELSE CAST(NULL AS INT) END IS NULL",
        "if(CAST('abc' AS INT) > 1, 1, CAST(NULL AS INT)) IS NULL",
        "greatest(CAST(s AS INT), CAST(NULL AS INT)) IS NULL",
        # A comparison is nullable when either operand is; a string function when its argument is.
        "(CAST(s AS INT) = 1) IS NULL",
        "concat('a', CAST(CAST(s AS INT) AS STRING)) IS NULL",

        # ── The value half: an operand that really can be null still answers per row. ──
        "a IS NULL", "s IS NOT NULL", "CAST(NULL AS INT) IS NULL", "NULL IS NULL",
        "(a + 1) IS NULL", "coalesce(a, 0) IS NOT NULL",
        "CASE WHEN a > 0 THEN 1 ELSE 2 END IS NOT NULL",
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
    # ------------------------------------------------------------------------------------
    # Issue #281, found by fuzz_expressions.py: Spark reads an integral LITERAL met with a
    # decimal as the narrowest decimal holding its VALUE, not as the one holding its TYPE.
    # `DecimalType.fromLiteral` via `DecimalPrecisionTypeCoercion.nondecimalAndDecimal`, under
    # `spark.sql.decimalOperations.literalPickMinimumPrecision` (default true). The analyzed plan
    # for `d1 + 2` is `(d1 + cast(2 as decimal(1,0)))`, and those nine integer digits an int
    # column would have reserved are nine the result keeps as SCALE.
    #
    # The group's job is the BOUNDARY as much as the rule. Spark inserts the cast at a binary
    # operator and nowhere else, so the controls -- a column, a CAST, a folded-looking sum,
    # unification, an IN list -- are not decoration: reproducing the rule one call too widely is
    # as wrong as not reproducing it, and only the pairs here say where the line falls.
    "decimal-literal-precision": [
        # THE TWO REPROS from the issue. Both agree numerically and disagree in scale, which is
        # what makes them a rendering and a downstream-cast defect rather than a value one.
        "1.5BD / 2",
        "1.0000000000000000000000000000000000001 + 1",
        "CAST(1.5BD / 2 AS STRING)",
        "CAST(1.0000000000000000000000000000000000001 + 1 AS STRING)",

        # EVERY OPERATOR, both operand orders. `%` is the one that can answer a NARROWER type
        # than either operand -- decimal(3,2) here, against the decimal(10,2) the int width
        # gives -- and `/` the one where the literal's width reaches the scale twice over.
        "d1 + 2",
        "2 + d1",
        "d1 - 2",
        "2 - d1",
        "d1 * 2",
        "2 * d1",
        "d1 / 2",
        "2 / d2",
        "d1 % 2",
        "2 % d2",

        # THE VALUE DECIDES, not the type: a ten-digit literal reserves exactly the ten digits an
        # int column would, so this row is the control where narrowing changes nothing, and the
        # 19-digit one is the control where it is a bigint's width that is not taken.
        "d1 + 1000000000",
        "d1 + 9999999999999999999",
        "d1 + 0",

        # A NEGATED LITERAL IS A LITERAL. Spark folds the sign at parse time; a precision never
        # counts one, so these must answer exactly as the positive rows do.
        "d1 + -2",
        "d1 + -2147483648",

        # AT THE CLAMP, where the digits the rule saves are the difference between an answer with
        # 36 fractional digits and one with 27. d5 is all scale, so every integer digit an
        # operand reserves comes straight off the result.
        "d5 + 2",
        "d5 - 1",
        "d5 * 100",
        "d5 + 1000000",
        "CAST(d5 + 2 AS STRING)",

        # THE CONTROLS THAT SAY "LITERAL". Same shapes, none of them a literal to Spark: a
        # column, a CAST of a literal, and a sum of two literals -- which is NOT folded before
        # coercion runs, so it keeps the int width. A rule that fired on any of these would make
        # `d1 + a` and `d1 + 2` the same type, and they are not.
        "d1 + a",
        "d1 + b",
        "d1 + CAST(2 AS INT)",
        "d1 + (2 + 2)",
        "d1 + 2 + a",

        # WHAT DOES NOT TAKE THE RULE. Unification resolves `(d1, 2)` through the int width in
        # every one of these -- decimal(12,2), where the rule would say decimal(10,2) -- so they
        # are the rows that keep this from leaking into #280's least-common-type clamp.
        "greatest(d1, 2)",
        "least(d1, 2)",
        "coalesce(d1, 2)",
        "nvl(d1, 2)",
        "if(bl, d1, 2)",
        "CASE WHEN bl THEN d1 ELSE 2 END",
        # ...and `round`, whose second argument is a scale it needs as an integer.
        "round(d1 + 2, 1)",
        "round(1.5BD / 2, 2)",
        # A negation carries the operand's type through, so it shows the inner rule and adds no
        # rule of its own.
        "-(1.5BD / 2)",

        # -- COMPARISON TAKES IT, AND AN IN LIST DOES NOT --------------------------------
        # One expression apart, and opposite answers. Against 4E-32 the comparison resolves
        # through decimal(38,37) -- the literal narrowed to decimal(1,0) -- and the value
        # survives, so `= 0` is FALSE; the IN list resolves through decimal(38,28), where it
        # rounds away, so `IN (0)` is TRUE. Both orders and every operator, since a rule applied
        # to one of them would be visible in no other row.
        "CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38)) = 0",
        "0 = CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38))",
        "CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38)) <> 0",
        "CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38)) > 0",
        "CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38)) < 0",
        "CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38)) >= 0",
        "CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38)) <= 0",
        "CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38)) <=> 0",
        "CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38)) IN (0)",
        "CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38)) IN (0, 1)",
        # The same comparison with the literal spelled as a CAST, which is the int-width answer
        # and the row that makes the pair above attributable to the literal rather than to the
        # value.
        "CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38)) = CAST(0 AS INT)",
        # A decimal literal reaches none of this: it is already a decimal and is typed from its
        # own digits either way.
        "CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38)) = 0.0BD",

        # NULLIF IS ASYMMETRIC, which no other row in the corpus would show. Its second argument
        # takes the rule and its first does not: from the optimized plans, `nullif(d5, 0)` is
        # `if (cast(d5 as decimal(38,37)) = cast(cast(0 as decimal(1,0)) as decimal(38,37)))
        # null else d5` while `nullif(0, d5)` is `if (cast(0 as decimal(38,28)) = cast(d5 as
        # decimal(38,28))) null else 0`. So these two answer DIFFERENTLY over the same values --
        # the first keeps 4E-32 and the second is NULL -- and neither is a typo.
        "nullif(CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38)), 0)",
        "nullif(0, CAST(0.00000000000000000000000000000004 AS DECIMAL(38,38)))",
        "nullif(d1, 2)",
        "nullif(2, d1)",

        # FLOATING POINT IS NOT PART OF THIS, for the reason #277 gives: a decimal mixed with a
        # double is a DOUBLE, so there is no decimal for a literal to be narrowed against.
        "g + 2",
        "CAST(0.1 AS DOUBLE) = 0",
    ],

    # `date_format`'s pattern language, which is JAVA'S and not .NET's. #284.
    #
    # NOT IN LEGACY_GROUPS. Every answer here is a formatting rule and every refusal is a pattern
    # check, and the ANSI switch moves neither -- what governs a datetime pattern in Spark is
    # `spark.sql.legacy.timeParserPolicy`, which this corpus does not touch. Asking twice would
    # double the group for the same answers.
    #
    # THE POINT OF THE GROUP is that the two languages agree over a narrow middle and nowhere
    # else, so the rows are chosen to sit on both sides of every edge: a run of two or more of
    # `y M d H m s` with punctuation between them agrees, and the empty pattern, a pattern of one
    # character, a lone `y`, and every character .NET reads as a construct do not.
    "date-format": [
        # --- THE ISSUE AS FILED. .NET reads an EMPTY format string as a request for the general
        # format, so this answered a whole timestamp -- `08/11/2026 12:30:00 +00:00` -- where
        # Spark formats no fields at all. Asked bracketed as well, so the recorded answer says
        # "empty string" rather than leaving a reader to wonder whether the row is missing.
        "date_format(ts, '')",
        "concat('[', date_format(ts, ''), ']')",
        "date_format(dt, '')",

        # --- A PATTERN OF EXACTLY ONE CHARACTER, which .NET reads as a STANDARD specifier rather
        # than a custom one. So the shortest spelling of each field was the one spelling that
        # could not mean the field: `d` was the short date, `s` the sortable timestamp, `M` the
        # month-and-day, and `H` is no standard specifier at all and threw FormatException out of
        # the evaluator.
        "date_format(ts, 'y')", "date_format(ts, 'M')", "date_format(ts, 'd')",
        "date_format(ts, 'H')", "date_format(ts, 'm')", "date_format(ts, 's')",
        "date_format(dt, 'y')", "date_format(dt, 'M')", "date_format(dt, 'd')",

        # --- TWO OF THE SAME LETTER, which is the width the two languages DO agree on. These are
        # the control: a fix that stopped using .NET's formatter has to leave them alone.
        "date_format(ts, 'yy')", "date_format(ts, 'MM')", "date_format(ts, 'dd')",
        "date_format(ts, 'HH')", "date_format(ts, 'mm')", "date_format(ts, 'ss')",
        "date_format(ts, 'yyyy-MM-dd')", "date_format(ts, 'HH:mm:ss')",
        "date_format(ts, 'MM/dd/yyyy')", "date_format(ts, 'yyyyMMdd')",
        "date_format(dt, 'yyyy-MM-dd')",

        # --- THE YEAR HAS THREE RULES, and only one of them is "pad to the count": count 1 is the
        # year in its own width, count 2 is the last two digits, count 3 or more pads. Above 999
        # the first and the last agree and the rule is invisible, so the boundary is asked as a
        # LITERAL -- the `ts` column cannot carry two years at once, and this is the year that
        # tells `y` (999) from `yyy` (999) from `yyyy` (0999). It is also the divergence no
        # translation into .NET's language could have closed: .NET's `y` is the last two digits
        # and it has no spelling for "as many digits as the year needs".
        "date_format(TIMESTAMP'0999-01-02 03:04:05', 'y')",
        "date_format(TIMESTAMP'0999-01-02 03:04:05', 'yy')",
        "date_format(TIMESTAMP'0999-01-02 03:04:05', 'yyy')",
        "date_format(TIMESTAMP'0999-01-02 03:04:05', 'yyyy')",
        "date_format(TIMESTAMP'0999-01-02 03:04:05', 'yyyyy')",
        "date_format(TIMESTAMP'0999-01-02 03:04:05', 'yyyy-MM-dd HH:mm:ss')",
        "date_format(ts, 'yyy')", "date_format(ts, 'yyyy')", "date_format(ts, 'yyyyy')",

        # --- MONTH NAMES. Spark formats with Locale.US, so these are English on every machine --
        # which is why the implementation reads them off InvariantCulture rather than the
        # process's. One past the last name, Spark REFUSES rather than narrowing.
        "date_format(ts, 'MMM')", "date_format(ts, 'MMMM')", "date_format(ts, 'MMMMM')",

        # --- A RUN TOO LONG FOR ITS LETTER. Java would widen the field to the count, so `ddd`
        # would be a three-digit day -- but Spark raises DATETIME_PATTERN_RECOGNITION for all of
        # these, because the meaning changed when it moved to java.time in 3.0. Answering where
        # Spark refuses is the worse half of the same defect, so they are asked here.
        "date_format(ts, 'ddd')", "date_format(ts, 'dddd')",
        "date_format(ts, 'HHH')", "date_format(ts, 'mmm')", "date_format(ts, 'sss')",

        # --- THE CHARACTERS .NET READS AS CONSTRUCTS AND JAVA OUTPUTS AS THEMSELVES: the escape,
        # the single-custom-specifier prefix, and the other literal delimiter. A lone trailing
        # backslash, which .NET refuses outright, is just a backslash.
        r"date_format(ts, '\\d')",
        "date_format(ts, '%d')",
        """date_format(ts, '"yy"')""",
        r"date_format(ts, '\\')",
        r"date_format(ts, '\\\\')",
        # ...and the punctuation that agrees, which says the fix did not start escaping
        # everything. `/` and `:` are .NET's culture-dependent date and time separators, so they
        # only agree because the formatter was invariant.
        "date_format(ts, '-')", "date_format(ts, ' ')", "date_format(ts, '..')",

        # --- JAVA'S OWN LITERAL, which is the single-quoted section, with both of its special
        # cases: an EMPTY section is a literal apostrophe rather than nothing, and a doubled quote
        # inside a section is one apostrophe rather than two section boundaries. A pattern ending
        # inside a section is refused, by Java and here alike.
        r"date_format(ts, 'yyyy\'T\'HH')",
        r"date_format(ts, '\'yyyy\'')",
        r"date_format(ts, '\'\'')",
        r"date_format(ts, 'yyyy\'')",

        # ...and THE SCAN IS GREEDY, which is the part a reader gets wrong. A run of apostrophes
        # is ONE section, not a chain of `''` pairs, so four of them render one apostrophe rather
        # than two -- 2n render n-1, and an odd count ends inside the section and refuses. Asked
        # because the other reading is plausible enough to have been raised in review of #284,
        # and bracketed by `concat` so the recorded answer distinguishes one apostrophe from two.
        # The rows with fields around the run are what say the section ends where the scan says.
        r"""concat('[', date_format(ts, '\'\''), ']')""",
        r"""concat('[', date_format(ts, '\'\'\''), ']')""",
        r"""concat('[', date_format(ts, '\'\'\'\''), ']')""",
        r"""concat('[', date_format(ts, '\'\'\'\'\''), ']')""",
        r"""concat('[', date_format(ts, '\'\'\'\'\'\''), ']')""",
        r"""concat('[', date_format(ts, '\'\'\'\'\'\'\'\''), ']')""",
        r"""concat('[', date_format(ts, 'yyyy\'\'MM'), ']')""",
        r"""concat('[', date_format(ts, '\'\'yyyy'), ']')""",
        r"""concat('[', date_format(ts, '\'It\'\'s\''), ']')""",
        r"""concat('[', date_format(ts, '\'a\'\'\''), ']')""",

        # --- THE STRUCTURAL CHARACTERS. Java reserves `#`, `{` and `}` and throws on them; `[` and
        # `]` open and close an OPTIONAL SECTION, which Spark accepts when formatting and this does
        # not implement. Both halves are asked so the boundary is recorded rather than assumed.
        "date_format(ts, '#')", "date_format(ts, '{')", "date_format(ts, '}')",
        "date_format(ts, ']')", "date_format(ts, 'yyyy]')", "date_format(ts, '[yyyy]')",

        # --- PATTERN LETTERS SPARK SUPPORTS AND THIS DOES NOT, which is a gap #284 does not close
        # and does not widen. Recorded because an unimplemented letter and an INVALID one look the
        # same from here -- both refuse -- and only the corpus says which is which: `D E a h S G q
        # L Z z X` all answer in Spark, while `n` and `V` refuse there too.
        "date_format(ts, 'D')", "date_format(ts, 'E')", "date_format(ts, 'a')",
        "date_format(ts, 'h')", "date_format(ts, 'S')", "date_format(ts, 'G')",
        "date_format(ts, 'q')", "date_format(ts, 'LLL')", "date_format(ts, 'Z')",
        "date_format(ts, 'z')", "date_format(ts, 'X')",
        "date_format(ts, 'n')", "date_format(ts, 'V')",

        # --- THE PATTERN IS CHECKED WHATEVER THE VALUE IS. Spark resolves it at analysis, so a
        # pattern it rejects rejects the expression over an all-null column too. Checking it after
        # the null test would answer null here and refuse only once a value showed up.
        "date_format(NULL, 'ddd')", "date_format(NULL, 'yyyy')",
        "date_format(CAST(NULL AS TIMESTAMP), 'ddd')",
    ],

    # IEEE 754 HAS TWO ZEROS AND SPARK CAN TELL THEM APART. #282. The group exists because the
    # sign of a zero is invisible to every comparison in SQL and in .NET alike -- `-0.0 = 0.0` is
    # true in both -- so the only channel that can see it is the RENDERING, and every row here
    # that carries a double is therefore asked through `CAST(... AS STRING)`. That is also why
    # the defect survived a corpus which already evaluated `-(g)`: the value matched.
    #
    # THREE RULES, and they do not point the same way, which is why they are gathered together.
    # Unary minus MAKES a negative zero where subtracting from zero cannot. `round` DESTROYS one,
    # because Spark rounds a double through BigDecimal and a BigDecimal has no signed zero. And
    # every cast OUT of a double -- to a decimal, to an integral, to a boolean -- drops it too. A
    # fix that reads only the first rule trades one divergence for another.
    "negative-zero": [
        # --- THE ISSUE AS FILED, in both channels and in every spelling of unary minus. The
        # first row is the one a value comparison cannot see; the second is the one that caught it.
        "negative(CAST(0.0 AS DOUBLE))",
        "CAST(negative(CAST(0.0 AS DOUBLE)) AS STRING)",
        "-CAST(0.0 AS DOUBLE)",
        "CAST(-CAST(0.0 AS DOUBLE) AS STRING)",
        "CAST(-0.0D AS STRING)",
        "CAST(negative(CAST(0.0 AS FLOAT)) AS STRING)",
        # ...and over a zero the OPTIMIZER cannot fold away, so the rule is about evaluation
        # rather than about constant folding.
        "CAST(negative(g - g) AS STRING)",
        "CAST(negative(f - f) AS STRING)",

        # --- THE CONTROL, and the reason this is a defect rather than a preference: `0.0 - x` and
        # `-x` are the same value at every double except this one. Under round-to-nearest
        # `0.0 - 0.0` is a POSITIVE zero, so an implementation that reuses subtraction for
        # negation cannot reach the answer above however it is written.
        "CAST(CAST(0.0 AS DOUBLE) - CAST(0.0 AS DOUBLE) AS STRING)",
        "CAST(0.0D AS STRING)",
        "CAST(negative(negative(CAST(0.0 AS DOUBLE))) AS STRING)",
        "CAST(negative(CAST(0.0 AS DOUBLE)) + CAST(0.0 AS DOUBLE) AS STRING)",
        "CAST(negative(CAST(0.0 AS DOUBLE)) + negative(CAST(0.0 AS DOUBLE)) AS STRING)",
        "CAST(CAST(0.0 AS DOUBLE) * CAST(-1.0 AS DOUBLE) AS STRING)",

        # --- FROM TEXT, which is the second producer, and the one that differs per RUNTIME rather
        # than per implementation: .NET Framework's number parser answers a positive zero for
        # "-0.0" where .NET Core and Java answer the negative one. The sign has to be read off the
        # TEXT, and `-1e-400` is the proof -- it underflows to a zero whose sign appears nowhere
        # in the digits.
        "CAST(CAST('-0.0' AS DOUBLE) AS STRING)",
        "CAST(CAST('-0' AS DOUBLE) AS STRING)",
        "CAST(CAST('-.0' AS DOUBLE) AS STRING)",
        "CAST(CAST('-0E5' AS DOUBLE) AS STRING)",
        "CAST(CAST('  -0.0  ' AS DOUBLE) AS STRING)",
        "CAST(CAST('-0.0' AS FLOAT) AS STRING)",
        "CAST(CAST('-1e-400' AS DOUBLE) AS STRING)",
        # ...including through the Java type suffix #258 is about, which is a SECOND parse and
        # needs the same rule.
        "CAST(CAST('-0.0d' AS DOUBLE) AS STRING)",
        "CAST(CAST('-0.0f' AS FLOAT) AS STRING)",
        # ...and the two forms that must NOT produce one.
        "CAST(CAST('+0.0' AS DOUBLE) AS STRING)",
        "CAST(CAST('1e-400' AS DOUBLE) AS STRING)",

        # --- A NEGATIVE ZERO LITERAL IS NOT ONE. `-0.0` is a decimal in Spark, and a decimal has
        # no signed zero, so the sign is gone before the cast to double ever runs. Only the `D`
        # suffix above spells a negative zero as a literal.
        "CAST(-0.0 AS STRING)",
        "CAST(-0.0BD AS STRING)",
        "CAST(CAST(-0.0 AS DOUBLE) AS STRING)",
        "CAST(negative(CAST(0.0 AS DECIMAL(10,2))) AS STRING)",
        "CAST(negative(CAST(0 AS INT)) AS STRING)",

        # --- ROUND DESTROYS IT, which is the rule that runs the other way. Spark rounds a double
        # through BigDecimal, so any answer landing on zero comes back positive however the input
        # was signed -- where IEEE rounding keeps the sign and would answer -0.0.
        "CAST(round(CAST(-0.4 AS DOUBLE)) AS STRING)",
        "CAST(round(CAST(-0.04 AS DOUBLE), 1) AS STRING)",
        "CAST(round(CAST(-0.0001 AS DOUBLE), 2) AS STRING)",
        "CAST(round(CAST(-0.4 AS DOUBLE), -1) AS STRING)",
        "CAST(round(CAST(-0.4 AS FLOAT)) AS STRING)",
        # ...and over a negative zero itself, at three scales. These are the rows that say the two
        # rules have to be fixed TOGETHER: each of them agreed before #282 only because unary
        # minus had already lost the sign, so closing that half alone would have opened these.
        "CAST(round(negative(CAST(0.0 AS DOUBLE)), 0) AS STRING)",
        "CAST(round(negative(CAST(0.0 AS DOUBLE)), 1) AS STRING)",
        "CAST(round(negative(CAST(0.0 AS FLOAT)), 2) AS STRING)",
        "CAST(round(negative(g - g), 2) AS STRING)",
        # ...while a rounded value that does NOT land on zero keeps its sign, which is what says
        # the rule is about the zero and not about rounding a negative.
        "CAST(round(CAST(-1.5 AS DOUBLE)) AS STRING)",
        "CAST(round(CAST(-0.5 AS DOUBLE)) AS STRING)",
        "CAST(round(CAST(-0.4 AS DOUBLE), 3) AS STRING)",

        # --- AND AT A SCALE WHOSE POWER OF TEN IS NOT A DOUBLE, which is where that rule was
        # easiest to implement only halfway. Raised in review of #282: above about 308 the
        # scaling factor overflows to an infinity and `0 * infinity` is NaN, so a zero answered
        # NaN -- a value out of nowhere, and the same failure #285 fixed at the other end of the
        # range without anyone measuring this one. Both zeros and both widths, because it was
        # wrong for the POSITIVE zero too and so is not a negative-zero regression at all.
        "CAST(round(CAST(0.0 AS DOUBLE), 400) AS STRING)",
        "CAST(round(negative(CAST(0.0 AS DOUBLE)), 400) AS STRING)",
        "CAST(round(g - g, 400) AS STRING)",
        "CAST(round(negative(g - g), 400) AS STRING)",
        "CAST(round(CAST(0.0 AS FLOAT), 400) AS STRING)",
        # ...on both sides of the boundary, 10^308 being a double and 10^309 not.
        "CAST(round(CAST(0.0 AS DOUBLE), 308) AS STRING)",
        "CAST(round(CAST(0.0 AS DOUBLE), 309) AS STRING)",
        "CAST(round(negative(CAST(0.0 AS DOUBLE)), 309) AS STRING)",
        # ...and the other end, where the factor underflows instead. #285 measured it for a
        # value; these are the zeros it did not ask about.
        "CAST(round(CAST(0.0 AS DOUBLE), -400) AS STRING)",
        "CAST(round(negative(CAST(0.0 AS DOUBLE)), -400) AS STRING)",
        # ...with the controls that say the short-circuit is about the ZERO and not the scale: a
        # non-zero value at the same scales is untouched, because the range guard already caught
        # its overflow. Only `0 * infinity` was ever a NaN.
        "CAST(round(CAST(-1.5 AS DOUBLE), 400) AS STRING)",
        "CAST(round(g, 400) AS STRING)",
        "CAST(round(CAST(2.5 AS DOUBLE), 309) AS STRING)",
        "CAST(round(CAST(-1.5 AS DOUBLE), -400) AS STRING)",
        # ...and the non-finite values, which pass through ahead of all of it.
        "CAST(round(CAST('NaN' AS DOUBLE), 400) AS STRING)",
        "CAST(round(CAST('Infinity' AS DOUBLE), 400) AS STRING)",

        # --- NOTHING SURVIVES A CAST OUT of the double, the widest decimal included, so a fix
        # that made the negation right must not start rendering "-0.00". The double-to-decimal
        # cast is the one to watch: #244 made it go through the value's RENDERING, which is now
        # the one place the sign is written down.
        "CAST(CAST(negative(CAST(0.0 AS DOUBLE)) AS DECIMAL(10,2)) AS STRING)",
        "CAST(CAST(negative(CAST(0.0 AS DOUBLE)) AS DECIMAL(38,37)) AS STRING)",
        "CAST(CAST(negative(CAST(0.0 AS FLOAT)) AS DECIMAL(10,2)) AS STRING)",
        "CAST(CAST(negative(CAST(0.0 AS DOUBLE)) AS BIGINT) AS STRING)",
        "CAST(CAST(negative(CAST(0.0 AS DOUBLE)) AS SMALLINT) AS STRING)",
        "CAST(CAST(negative(CAST(0.0 AS DOUBLE)) AS BOOLEAN) AS STRING)",
        "CAST(CAST('-0.0' AS DECIMAL(10,2)) AS STRING)",
        "CAST(CAST('-0' AS DECIMAL(10,2)) AS STRING)",
        # ...but it does survive a cast to the OTHER float width, which is a sign-bit copy.
        "CAST(CAST(negative(CAST(0.0 AS DOUBLE)) AS FLOAT) AS STRING)",

        # --- INVISIBLE TO EVERY COMPARISON, which is why only the rendering channel above can
        # measure any of this. Equality, ordering, the null-safe form and IN all hold the two
        # zeros equal, and so does the equality `nullif` is defined by.
        "negative(CAST(0.0 AS DOUBLE)) = CAST(0.0 AS DOUBLE)",
        "negative(CAST(0.0 AS DOUBLE)) < CAST(0.0 AS DOUBLE)",
        "negative(CAST(0.0 AS DOUBLE)) <=> CAST(0.0 AS DOUBLE)",
        "negative(CAST(0.0 AS DOUBLE)) IN (CAST(0.0 AS DOUBLE))",
        "CAST(nullif(negative(CAST(0.0 AS DOUBLE)), CAST(0.0 AS DOUBLE)) AS STRING)",
        # ...so `greatest` and `least` cannot choose between them by value, and both keep whichever
        # argument came FIRST. Asked in both orders, because one order alone would look like a
        # rule about the sign.
        "CAST(greatest(negative(CAST(0.0 AS DOUBLE)), CAST(0.0 AS DOUBLE)) AS STRING)",
        "CAST(least(negative(CAST(0.0 AS DOUBLE)), CAST(0.0 AS DOUBLE)) AS STRING)",
        "CAST(greatest(CAST(0.0 AS DOUBLE), negative(CAST(0.0 AS DOUBLE))) AS STRING)",
        "CAST(least(CAST(0.0 AS DOUBLE), negative(CAST(0.0 AS DOUBLE))) AS STRING)",
        # ...while a conditional carries out whichever branch it took, per row.
        "CAST(if(bl, negative(CAST(0.0 AS DOUBLE)), CAST(0.0 AS DOUBLE)) AS STRING)",
        "CAST(coalesce(negative(CAST(0.0 AS DOUBLE)), CAST(1.0 AS DOUBLE)) AS STRING)",

        # --- THE NON-FINITE NEIGHBOURS, because a sign-bit flip reaches them too and only two of
        # the three show it: Java prints a negated NaN as "NaN", so the sign bit unary minus
        # really does set there is written down nowhere.
        "CAST(negative(CAST('NaN' AS DOUBLE)) AS STRING)",
        "CAST(negative(CAST('Infinity' AS DOUBLE)) AS STRING)",
        "CAST(negative(CAST('-Infinity' AS DOUBLE)) AS STRING)",
    ],

    # THE RANGE OF A NUMERIC LITERAL, which Spark checks in its PARSER. #287. A group of parse
    # questions rather than evaluation ones: `1e400` never reaches a row, and the whole point is
    # that it never reaches one here either.
    #
    # THE BOUND IS COMPARED AGAINST THE LITERAL EXACTLY, and that is the part an implementation
    # gets wrong by reading the parsed value instead of the text -- `1.79769313486231575e308`
    # rounds to Double.MaxValue and is still refused. The bound itself is the SHORTEST repr of
    # the type's maximum, which is why the ordinary spelling of the largest float,
    # `3.4028235e38F`, is over it.
    #
    # AND ONLY FROM ABOVE. #287 reports `1e-400` as the same gap at the other end; measured, it
    # is not one -- Spark compares against [-MaxValue, MaxValue] and an underflow sits well
    # inside that. The underflow rows are here to record that, and `-1e-400` is here because it
    # is a NEGATIVE ZERO literal, which is #282 and not this.
    "numeric-literal-range": [
        # --- THE ISSUE AS FILED, in the three spellings of a double literal.
        "1e400", "CAST(1e400 AS DOUBLE)", "1e400D", "-1e400",
        # ...and in the places a literal can sit, since the refusal is the parser's and so cannot
        # depend on context.
        "CAST(1e400 AS STRING)", "1e400 + 1", "g + 1e400",

        # --- THE DOUBLE BOUNDARY, one step either side. 1.7976931348623157e308 is the largest
        # accepted; the next four are all refused, and the first two of them ROUND to it.
        "1.7976931348623157e308",
        "1.79769313486231575e308",
        "1.7976931348623158e308",
        "1.7976931348623159e308",
        "1.8e308",
        "1e308", "1e309",
        # ...and the same value with the point moved, which says the check is on the value and
        # not on the spelling.
        "17976931348623157e292",

        # --- THE FLOAT BOUNDARY, which is NOT where Java prints float.MaxValue. Spark states the
        # bound as a widened double, 3.4028234663852886E38, so the familiar 3.4028235e38 is over
        # it by a hair and refused.
        "3.4028234663852886e38F",
        "3.4028234663852887e38F",
        "3.4028235e38F",
        "3.4e38F", "3.5e38F", "1e38F", "1e39F", "1e400F",

        # --- UNDERFLOW IS NOT A RANGE ERROR. Every one of these answers, and the last is a
        # negative zero -- Spark folds the sign into the literal, so it is a Literal there and a
        # `negative` call here. #282.
        "1e-400", "CAST(1e-400 AS DOUBLE)", "1e-400D", "1e-325", "1e-324", "1e-323",
        "4.9e-324", "4e-324", "1e-45F", "1e-46F", "-1e-400",

        # --- A ZERO MANTISSA IS IN RANGE AT EVERY EXPONENT, which is what stops a check written
        # on the exponent alone.
        "0e400", "0e-400", "0.0e400", "000e400",

        # --- AN EXPONENT THE SCALE CANNOT CARRY is refused ahead of the range comparison, and
        # `0e2147483648` is the row that proves the order: its mantissa is zero, so only a check
        # that runs FIRST can refuse it. `1e-2147483648` refuses although the exponent fits an
        # int, because it is the negation that overflows.
        "1e2147483647", "1e2147483648", "1e99999999999", "1e-99999999999",
        "1e-2147483648", "1e-2147483649", "0e2147483648",

        # --- A DECIMAL LITERAL IS BOUNDED BY ITS PRECISION INSTEAD, a different error class and
        # a different rule: 39 digits is too many however small the value. Recorded beside the
        # others so the two are not confused -- #173 already implements this one.
        "1e400BD", "1e38BD", "1e37BD",

        # --- AND AN INTEGRAL LITERAL HAS NO UPPER BOUND AT ALL, because Spark's ladder does not
        # stop at bigint: past it the literal becomes a DECIMAL. The control that says "out of
        # range for the type" is a floating-point rule and not a numeric-literal rule. #173.
        "9223372036854775808", "-9223372036854775809",
    ],

    # What a string operand of an ARITHMETIC operator is read as, which #180/#259 measured for
    # comparison and left unmeasured here. The dialects pick different targets again, and this
    # time the target decides which strings are ACCEPTED rather than only what type comes back:
    # ANSI's integral target inherits #258's integral TEXT rule, under which '1.5' is not a
    # number at all. #296.
    "arithmetic-string-coercion": [
        # The four rows that separate the two rules. Legacy reads every string as a DOUBLE;
        # ANSI reads it as the other operand's family, so an integral operand makes '1.5' and
        # '1e3' refusals rather than 2.5 and 1001.0.
        "'1.5' + 1", "'1e3' + 1", "'abc' + 1", "'1' + 1",

        # Every operator, so the rule is arithmetic's and not addition's...
        "'1' - 1", "'2' * 3", "'7' % 3", "'6' / 2",

        # ...and `/` is the sharp one. Its RESULT is a double whatever the operands are, and the
        # string is still read as a BIGINT against an integral: `'1.5' / 3` refuses under ANSI
        # while `'1.5' / g` answers. Two expressions that would agree under "`/` is double".
        "'1.5' / 3", "'1e3' / 2", "'1.5' / g", "'1.5' % 2", "fs / a", "fs % a", "fs % g",

        # The other operand's type, across the numeric families. The integral target is BIGINT
        # at every width -- '32768' against a smallint is 32768 and not an overflow -- and a
        # decimal goes to DOUBLE under both dialects, which is not what a comparison does.
        "'1' + sh", "'1' + b", "'1' + CAST(1 AS TINYINT)", "'1' + f", "'1' + g", "'1' + d1",
        "'1' + d5", "'32768' + CAST(0 AS SMALLINT)", "'2147483648' + CAST(0 AS INT)",
        "'1' + CAST(1 AS DECIMAL(10,2))",

        # The value discriminators for the target, each sharp because the two candidates answer
        # differently: 10^30+1 is exact as a decimal(38,0) and is not as a double, and 0.1 is
        # exact as a float and is not as a double.
        "'1000000000000000000000000000001' + CAST(0 AS DECIMAL(38,0))",
        "'1000000000000000000000000000001' * CAST(1 AS DECIMAL(38,0))",
        "'0.1' + CAST(0 AS FLOAT)", "'0.1' * CAST(1 AS FLOAT)",

        # Either operand order, and a column rather than a literal on each side: measured
        # identical, so one rule covers all four shapes.
        "1 + '1'", "1 - '2'", "3 * '2'", "3 % '7'", "2 / '6'", "1 + '1.5'", "1.5 + '1'",
        "g + '1'", "d1 + '1'", "ns + a", "a + ns", "s + a", "fs + a", "fs + g",
        "s * 2", "ns % a", "ns - b", "ns + d4", "ns / g", "ns / a",

        # TWO STRINGS ARE NOT AN ARITHMETIC PAIR UNDER ANSI, for any of the five operators --
        # the legacy dialect answers a double. Same shape as the `bl IN ('true')` split.
        "'1' + '2'", "'1' - '2'", "'1' * '2'", "'1' / '2'", "'1' % '2'",
        "ns + ns", "ns / ns", "ns + '2'", "ns + fs",

        # ...and neither is a string against a bare NULL. It is the absence of a TYPE that
        # refuses rather than the nullness: `'1' + CAST(NULL AS INT)` is a perfectly good bigint
        # null one expression away.
        "'1' + NULL", "NULL + '1'", "ns + NULL", "'1' / NULL", "'1' % NULL",
        "'1' + CAST(NULL AS INT)", "'1' + CAST(NULL AS DOUBLE)", "'1' + CAST(NULL AS STRING)",

        # UNARY MINUS IS THE ONE PLACE THE TWO DIALECTS AGREE: a double in both, even against
        # the integral shape where binary `+` is a bigint. So `-'1e3'` is -1000.0 where
        # `'1e3' + 1` refuses -- one exponent, two answers, one dialect.
        "-'1'", "-'1.5'", "-'1e3'", "-'abc'", "-''", "-'  1  '", "-'1d'", "-ns", "-s", "-fs",
        "-CAST(NULL AS STRING)", "-'1' + 1", "- -'1'",
        # A negative zero, which is what says the string reached a DOUBLE rather than a parse
        # that dropped the sign. #282.
        "-'0'",

        # What the ANSI cast accepts on the way through, since the integral target is the strict
        # one: padding is trimmed, an empty string is not a zero, 20 digits do not fit a bigint,
        # and a value that does fit can still overflow the ADDITION.
        "' 1 ' + 1", "'' + 1", "'99999999999999999999' + 1", "'9223372036854775807' + 1",

        # The pairs with no rule in EITHER dialect, so the refusal is not ANSI's alone.
        "'1' + bl", "'1' + ts", "'1' + X'41'", "'1' + dt", "dt + '1'",
    ],

    # The DATE and TIMESTAMP text grammars, which are Spark's OWN and not any general parser's.
    # #318, found by the group above: its one interior-whitespace row failed, and not because of
    # trimming. `CastToDate` and `CastToTimestamp` read their text with `DateTimeOffset.TryParse`,
    # which accepts a whole culture's worth of formats Spark refuses and refuses two forms Spark
    # accepts -- so the cast was wrong in both directions, exactly as the trim had been.
    #
    # TIMESTAMP answers are wrapped in `CAST(... AS STRING)` for the reason the trim group's are:
    # PySpark localises a timestamp to the DRIVER's zone on collect, so a bare one would record the
    # harvest machine rather than the pinned UTC. Wrapping also puts Spark's own RENDERING in the
    # fixture, which is a second thing worth pinning -- the sub-second was dropped on the way OUT
    # here, and from outside a corpus row cannot tell that apart from a defect in the parse.
    "temporal-text": [
        # --- A DATE IS NOT THE WHOLE STRING. The parse stops at the first space or `T` and throws
        # away everything after it -- but only once BOTH separators have been seen, which is the
        # half that makes it a rule rather than "anything may follow a date". The last four rows
        # are the ones that pin the second half.
        "CAST('2026-08-11 extra' AS DATE)", "CAST('2026-08-11Textra' AS DATE)",
        "CAST('2026-08-11T' AS DATE)", "CAST('2026-08-11 12:30:00' AS DATE)",
        "CAST('2026-08-11T12:30:00' AS DATE)", "CAST('2026-08-11 12:30 PM' AS DATE)",
        "CAST('2026-08-1 1' AS DATE)",
        "CAST('2026 extra' AS DATE)", "CAST('2026-08 extra' AS DATE)",
        "CAST('2026-08T' AS DATE)", "CAST('2026T' AS DATE)",

        # --- EVERY SEGMENT BELOW THE LAST ONE WRITTEN DEFAULTS, so a bare year is a whole date.
        "CAST('2026' AS DATE)", "CAST('2026 ' AS DATE)", "CAST('2026-08' AS DATE)",
        "CAST('2026-8-1' AS DATE)", "CAST('2026-8' AS DATE)",
        "CAST('+2026-08-11' AS DATE)", "CAST('+2026' AS DATE)", "CAST('0001-01-01' AS DATE)",

        # --- THE DIGIT COUNTS ARE THE GRAMMAR: four to seven for a year, one or two for a month
        # or a day. So the compact ISO spelling is not a date at all, and neither is a two-digit
        # year -- which is worth having written down, because a reader will assume both work.
        "CAST('20260811' AS DATE)", "CAST('202-01-01' AS DATE)", "CAST('26-01-01' AS DATE)",
        "CAST('2026-013-11' AS DATE)", "CAST('2026-08-011' AS DATE)", "CAST('2026-008' AS DATE)",
        "CAST('12345678-01-01' AS DATE)",

        # --- WHAT .NET READ AND SPARK REFUSES. Every one of these was a date here before #318,
        # and the first two are the dangerous ones: read under InvariantCulture they answer a
        # DIFFERENT DAY from the one whoever wrote `dd/MM/yyyy` meant. Silently answering a date
        # nobody meant is worse than refusing.
        "CAST('08/11/2026' AS DATE)", "CAST('2026/08/11' AS DATE)",
        "CAST('11 Aug 2026' AS DATE)", "CAST('Aug 11, 2026' AS DATE)",
        "CAST('2026.08.11' AS DATE)", "CAST('2026-08 -11' AS DATE)",
        "CAST('2026- 08-11' AS DATE)", "CAST('2026-08-11Z' AS DATE)",
        "CAST('2026-08-11+02:00' AS DATE)",

        # --- Refused by both, so the accepting half above is not "anything with digits in it".
        # The last row is the one #283 makes worth asking: a DECIMAL cast reads BMP digits, and
        # a date does not, because this grammar tests BYTES against '0'-'9'.
        "CAST('' AS DATE)", "CAST('2026-' AS DATE)", "CAST('2026-08-' AS DATE)",
        "CAST('-' AS DATE)", "CAST('T' AS DATE)", "CAST('T2026' AS DATE)",
        "CAST('2026-08-11-12' AS DATE)", "CAST('2026-02-30' AS DATE)",
        "CAST('2026-00-01' AS DATE)", "CAST('2026-01-00' AS DATE)",
        "CAST('١٤٤٧-01-01' AS DATE)",

        # --- THE TIMESTAMP GRAMMAR IS THE DATE ONE PLUS A TIME, and the time's segments default
        # the same way: an hour alone is a time.
        "CAST(CAST('2026' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11T12' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 2:3:4' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00' AS TIMESTAMP) AS STRING)",
        # ...and `T` alone does NOT end a timestamp's date, where it ends a DATE's. One character
        # of difference between the two grammars, and nothing but a row says so.
        "CAST(CAST('2026-08-11T' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('12' AS TIMESTAMP) AS STRING)",
        # The year is SIX digits here and SEVEN for a DATE. Spark's own asymmetry, not a slip.
        "CAST(CAST('1234567-01-01 00:00:00' AS TIMESTAMP) AS STRING)",

        # --- THE SUB-SECOND, which is a defect on the way out as well as in. Past six digits the
        # rest are DROPPED rather than refused, and the rendering strips trailing zeros -- so the
        # number of digits that comes back is a property of the value, not a fixed width.
        "CAST(CAST('2026-08-11 12:30:00.1' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00.12' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00.123456' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00.1234567' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00.1234567890123' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00.' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00.000000' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00.100000' AS TIMESTAMP) AS STRING)",
        "CAST(TIMESTAMP'2026-08-11 12:30:00.010000' AS STRING)",
        "CAST(TIMESTAMP'2026-08-11 12:30:00.123400' AS STRING)",
        "CAST(TIMESTAMP'2026-08-11 12:30:00.000001' AS STRING)",

        # --- TRAILING TEXT IS A TIMEZONE HERE, not junk to ignore: everything from the first
        # character that cannot continue the time is handed to Java's zone parser, and a name that
        # parser rejects fails the whole cast. That single rule is why the first block reads and
        # the second is refused, and it is the sharpest difference from the DATE grammar above.
        "CAST(CAST('2026-08-11 12:30:00Z' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00+02:00' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00-08:00' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00+02' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00+0200' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00+02:00:00' AS TIMESTAMP) AS STRING)",
        # Two spellings Java alone would refuse: Spark rewrites a single-digit hour or minute
        # field before handing the text over, because ZoneOffset reads a field's width from the
        # string's LENGTH.
        "CAST(CAST('2026-08-11 12:30:00+2:00' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00+02:0' AS TIMESTAMP) AS STRING)",
        # ...and BOTH rewrites reach a PREFIXED zone, which is a property of where the patterns
        # anchor rather than something either one says. The hour rewrite replaces its first match
        # anywhere in the text; the minute one matches FIVE characters against the end, so its
        # sign sits five back from the end whether or not `UTC` precedes it. Rows rather than an
        # argument, because reading the regexes suggests otherwise and a reviewer did.
        "CAST(CAST('2026-08-11 12:30:00UTC+02:0' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00UTC+2:0' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00GMT+2:0' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00UT+02:0' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00UTC-02:0' AS TIMESTAMP) AS STRING)",
        # One rewrite each, and no more: a seconds field spelled short is refused, because
        # neither pattern is about it.
        "CAST(CAST('2026-08-11 12:30:00UTC+02:00:0' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00+2:0:0' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00 +02:00' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00UTC' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00 UTC' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00UT' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00GMT+02:00' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00UTC-08' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00.UTC' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00.123456+02:00' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00+18:00' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00-18:00' AS TIMESTAMP) AS STRING)",
        # ...and the refusals that come from the same rule.
        "CAST(CAST('2026-08-11 12:30:00 extra' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00 PM' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00 +02:00 junk' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00 1:2' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00+19:00' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00+' AS TIMESTAMP) AS STRING)",
        # Only the CAPITAL Z, which is Java's rule and not a courtesy.
        "CAST(CAST('2026-08-11 12:30:00z' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12 UTC' AS TIMESTAMP) AS STRING)",

        # --- Malformed times, refused by both. The double space is the one worth reading: ONE
        # space separates the date from the time and a second one starts a zone name.
        "CAST(CAST('2026-08-11  12:30:00' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 :30:00' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 25:00:00' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:60:00' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:61' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026/08/11 12:30:00' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08 -11 12:30:00' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('' AS TIMESTAMP) AS STRING)",

        # --- A TIME ALONE IS TODAY, in both engines, so the rows are written to compare the part
        # that is not the clock. A leading `T` means the same thing -- and Spark tests it at index
        # 0 of the UNTRIMMED string, so one leading space refuses what is otherwise the same text.
        "CAST(CAST('12:30:00' AS TIMESTAMP) AS STRING) LIKE '% 12:30:00'",
        "CAST(CAST('12:30' AS TIMESTAMP) AS STRING) LIKE '% 12:30:00'",
        "CAST(CAST('12:30:00.5' AS TIMESTAMP) AS STRING) LIKE '% 12:30:00.5'",
        "CAST(CAST('12:30:00+02:00' AS TIMESTAMP) AS STRING) LIKE '% 10:30:00'",
        "CAST(CAST('T12' AS TIMESTAMP) AS STRING) LIKE '% 12:00:00'",
        "CAST(CAST(' T12:30:00' AS TIMESTAMP) AS STRING) LIKE '% 12:30:00'",
        "CAST(CAST('+12:30:00' AS TIMESTAMP) AS STRING) LIKE '% 12:30:00'",
        "CAST(CAST(':30:00' AS TIMESTAMP) AS STRING) LIKE '%:30:00'",
        "CAST(CAST('12:30:00:00' AS TIMESTAMP) AS STRING) LIKE '% 12:30:00'",

        # --- A YEAR OUTSIDE DateTimeOffset'S RANGE, which is where EngineeredWood is narrower
        # than Spark rather than different from it. Declared as known differences against the
        # issue that would widen it; see SparkTemporalText's remarks for why widening is a change
        # to how an instant travels through the cast rather than to this grammar.
        "CAST(CAST('-2026-08-11' AS DATE) AS STRING)",
        "CAST(CAST('123456-01-01' AS DATE) AS STRING)",
        "CAST(CAST('-2026-08-11 12:30:00' AS TIMESTAMP) AS STRING)",
        # ...and a REGION timezone, which is the other place it is narrower: resolving one needs a
        # tz database, and .NET's is not the same on every target framework.
        "CAST(CAST('2026-08-11 12:30:00America/Los_Angeles' AS TIMESTAMP) AS STRING)",
        "CAST(CAST('2026-08-11 12:30:00EST' AS TIMESTAMP) AS STRING)",

        # --- The coercion, which is where the grammar costs a wrong ANSWER rather than a wrong
        # error class: a string compared against a temporal column is CAST, so every row above is
        # reachable from a CHECK constraint that never writes the word CAST.
        "dt = '2026-08-11'", "dt = '2026-08-11 extra'", "dt = '08/11/2026'",
        "ts = '2026-08-11 12:30:00'", "ts = '2026-08-11 12:30:00 extra'",
    ],

    # A BOOLEAN against a NUMERIC, which under the legacy dialect is an equality Spark ANSWERS and
    # we answer NULL for. #333.
    #
    # Spark's `BooleanEquality` coercion: for an EQUALITY whose operands are a boolean and a
    # numeric, the BOOLEAN is cast to the numeric type (true -> 1, false -> 0) and the two are
    # compared as numbers. `sh = bl` over `sh smallint` = 2 is the discriminator that says which
    # way the cast goes -- 2 is not 1, so it is false, where reading the NUMBER for truthiness
    # would make it true.
    #
    # EQUALITY ONLY, and the ordering rows are here to pin that rather than as decoration: `a < bl`
    # is DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES in BOTH dialects, so a fix that reached ordering
    # would be adding a rule Spark does not have.
    #
    # LEGACY ONLY, which is why the group is in LEGACY_GROUPS: under ANSI every equality row below
    # is refused at analysis, and that half is #286's question rather than this one's. One harvest
    # would therefore record a refusal and miss the rule entirely -- the ANSI column here is the
    # control, not the measurement.
    "boolean-equality": [
        # The rule, over a column pair. Both orders, because which operand moves is the answer.
        "a = bl", "a <> bl", "a <=> bl", "bl = a", "bl <> a",

        # THE DISCRIMINATOR. `sh` is 2, so a boolean cast to smallint gives 1 and the answer is
        # false; a numeric read for truthiness would give true.
        "sh = bl", "sh <=> bl",

        # Every other numeric family, because the cast target is the NUMERIC's type and each of
        # these is a different target -- and d5 is decimal(38,38), where 1 does not fit at all.
        "b = bl", "f = bl", "g = bl", "d1 = bl", "d3 = bl", "d5 = bl",

        # ORDERING, refused in both dialects. The rule is equality's alone.
        "a < bl", "a > bl", "a <= bl", "bl > a",

        # `IS TRUE` / `IS FALSE`, which the parser lowers to `<=> TRUE` / `<=> FALSE` and which are
        # therefore the same rule wearing a different hat. `a IS FALSE` agrees with us today by
        # LUCK -- the comparison against 0 happens to give the right answer for these rows -- so
        # both polarities are asked, and both negations with them.
        "a IS TRUE", "a IS NOT TRUE", "a IS FALSE", "a IS NOT FALSE",
        "b IS TRUE", "b IS NOT FALSE", "g IS TRUE", "d1 IS TRUE",

        # A LITERAL either side, where constant folding could take a different route from the
        # column rows above.
        "1 = TRUE", "0 = FALSE", "2 = TRUE", "TRUE = 1", "1 <=> TRUE",

        # `nullif`, which Spark rewrites to `if(a = b, NULL, a)` and which therefore takes the
        # equality rule (#298/#315). If BooleanEquality is an `=` coercion rather than a property
        # of the operator node, this row moves with the others.
        "nullif(a, bl)", "nullif(bl, a)",

        # `IN`, which resolves ONE type over the operand and the list rather than per pair, so
        # whether the rule reaches it is a separate question from `=`. #261/#286.
        "a IN (bl)", "bl IN (a)", "a IN (bl, 1)",

        # The conditional family, which folds branch TYPES rather than coercing a pair -- a
        # boolean and a numeric have no common type, so these say whether the fold borrows the
        # equality rule or refuses.
        "coalesce(a, bl)", "greatest(a, bl)", "if(bl, a, bl)",

        # CONTROLS. A boolean against a boolean and a numeric against a numeric are untouched by
        # the rule, and `NOT (a = bl)` is the shape that matters in a CHECK constraint: a row whose
        # rule evaluates to NULL is ADMITTED, so answering null where Spark answers false accepts
        # data Spark rejects.
        "bl = TRUE", "bl <=> TRUE", "a = 1", "NOT (a = bl)", "NOT (bl = a)",

        # A STRING against a boolean, which is NOT this rule and is asked so that a fix cannot
        # quietly widen to it: the string rules of #180/#259 own this pair.
        "s = bl", "ns = bl",
    ],

    # THE 9x9 COMPARISON MATRIX. #286, and the measurement the family rule is derived from rather
    # than remembered: one column per type EW models, asked of both dialects, under the five
    # shapes below.
    #
    # WHY IT IS GENERATED. 81 pairs x 5 shapes is 405 expressions, and writing them out by hand
    # would invite exactly the gap this group exists to close -- a pair nobody thought to ask
    # about becomes a rule nobody implemented. The ORDER matters and both directions are asked:
    # which operand a rule moves is part of the answer (a string against a number is cast to the
    # number; a string against a binary stays put and the BINARY is rendered as text).
    #
    # `<=>` is here beside `=` because it is an equality that never answers null, and `<` because
    # ordering is where the boolean rule of #333 must NOT reach. `IN` is here because it is not
    # the disjunction of equalities it resembles: it resolves ONE type over the whole list, so it
    # can refuse a pair that `=` accepts -- measured, `a = bl` answers under the legacy dialect
    # while `a IN (bl)` is refused in BOTH.
    "comparison-families": [
        f"{left} {op} {right}"
        for op in ("=", "<>", "<=>", "<")
        for left in COMPARISON_COLUMNS
        for right in COMPARISON_COLUMNS
    ] + [
        f"{left} IN ({right})"
        for left in COMPARISON_COLUMNS
        for right in COMPARISON_COLUMNS
    ] + [
        # VOID, which the matrix cannot reach because `void` is not a column of SCHEMA and cannot
        # be: a bare NULL is a LITERAL and only a literal. Spark types one `void`, which
        # constrains nothing -- so the question is whether the rest of a set still has to agree
        # among itself once a void is dropped from it, and whether a void operand exempts the
        # members from each other.
        #
        # Asked because the answer decides a real branch: a void operand cannot be typed, so an
        # analyzer that gives up when it meets one would skip `NULL IN (1, TRUE)` entirely.
        "NULL IN (1, TRUE)",
        "NULL IN (1, 2)",
        "NULL IN (a, bl)",
        "NULL IN (bl)",
        "a IN (NULL, bl)",
        "a IN (bl, NULL)",
        "a IN (NULL, 1)",
        "bl IN (NULL, a)",

        # ...and the same question for a comparison, where a void operand is the shape #293
        # measured and this one only has to not regress.
        "NULL = bl",
        "NULL = a",
        "a = NULL",
        "NULL < bl",

        # A TYPED null is not a void and does constrain: `CAST(NULL AS INT)` is an int, so these
        # must be refused exactly as the column rows are. The pair that says an analyzer reading
        # "all rows null" instead of the TREE would answer wrongly.
        "CAST(NULL AS INT) IN (bl)",
        "CAST(NULL AS INT) = bl",
        "CAST(NULL AS BOOLEAN) IN (a)",
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

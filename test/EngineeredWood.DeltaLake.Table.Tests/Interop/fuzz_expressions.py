#!/usr/bin/env python3
# Copyright (c) clast-project. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.
"""Generate expressions and ask Spark what they mean, for differential-testing EngineeredWood.

This is `harvest_expression_corpus.py`'s generator counterpart. That script asks Spark about a
HAND-WRITTEN list; this one asks about a machine-generated one. Everything else is shared: the
same driver command (`expr_oracle`), the same schema, the same rows, the same conf discipline.
The output has the same shape as the checked-in fixture, so the .NET side reads both with one
code path.

WHY TEMPLATES x LITERALS AND NOT RANDOM TREES. The defects this is hunting live in the LEAVES,
not in the tree shape. Of the expression issues closed so far -- #173 #174 #179 #180 #181 #202
#243 #244 #248 #251 #258 #259 -- ten are a rendering, a rounding rule or an overflow boundary,
and two are structural. Precedence and associativity are already covered cheaply and exhaustively
by diffing `parse.sql`, which comes back fully parenthesised. So the generator's job is to reach
hostile VALUES through a modest set of shapes, not to grow deep trees over boring ones. Shapes
still nest -- see MAX_DEPTH -- but nesting is the seasoning, not the meal.

WHY THE ANSWERS COME BACK TWICE. `eval` is what PySpark's `collect()` handed back, serialised
with `default=str`: a double reaches the fixture through PYTHON's repr, not Java's. That is
survivable for a hand-picked corpus, where the comparison can afford to parse the value back and
compare numerically, and it is not survivable here -- a generated corpus is mostly doubles and
decimals, exactly the values whose SPELLING is the thing under test (#244, #248, #250). So every
expression E is submitted twice, as `E` and as `CAST((E) AS STRING)`. The second answer is
rendered by the JVM and crosses the wire as text, losslessly.

Comparing both is also what makes a failure ATTRIBUTABLE. Two bits, four cases:

    value agrees, text agrees      -- no finding
    value agrees, text DIFFERS     -- our CAST(... AS STRING) renders differently: a spelling bug
    value DIFFERS, text differs    -- we computed a different value: a value bug
    value DIFFERS, text agrees     -- the comparison is lying; the fixture's Python repr is lossy

Without the second question every spelling bug would be reported against whatever expression
happened to produce a double, and one wrong renderer would be attributed to a hundred templates.

WHAT THIS DELIBERATELY DOES NOT DO. It does not shrink, and it does not write to
`Fixtures/spark-expression-corpus.json`. The fixture is checked in so the expression tests need
no Spark, no JVM and no network, and it is meant to be read as a table; ten thousand generated
rows would destroy both properties. Findings are minimised and promoted BY HAND into a named
group there, the same as every other group.

    JAVA_HOME=... ~/.venvs/ew-base-py313/bin/python fuzz_expressions.py --count 500

Determinism: the corpus is a pure function of --seed and --count. Re-running with the same pair
regenerates the same expressions, so a finding can be reproduced without keeping the output.
"""
import argparse
import json
import os
import random
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

# The schema, the rows and the conf are the harvest script's, imported rather than copied. They
# are not incidental: the .NET comparison builds its Arrow frame from the schema and rows the
# corpus records, so a fuzz corpus carrying a schema of its own would need a second frame builder.
from harvest_expression_corpus import (  # noqa: E402
    CONF, LEGACY_CONF, ROWS, SCHEMA, _json_safe, _run_driver)


# --- The leaves ------------------------------------------------------------------------
# Everything below is chosen to sit ON a boundary rather than near one. A fuzzer that samples
# uniformly from the integers spends its budget proving that 7 + 3 is 10.

# Integers at every width's edge, plus the values just past it. `2147483648` is a BIGINT literal
# to Spark, which is what makes `CAST(2147483648 AS INT)` an overflow question rather than a
# parse question. #243.
INT_LITERALS = [
    "0", "1", "-1", "2", "-2", "10",
    "127", "128", "-128", "-129",
    "32767", "32768", "-32768", "-32769",
    "2147483647", "2147483648", "-2147483648", "-2147483649",
    "9223372036854775807", "-9223372036854775808",
    "4294967296",
]

# Doubles chosen for their SPELLING as much as their value: the JDK-17 band where Double.toString
# is not the shortest representation (#244), both subnormal edges, and the overflow literal.
FLOAT_LITERALS = [
    "0.0", "-0.0", "1.5", "2.5", "0.5", "-0.5", "0.1",
    "1e23", "1e308", "1.7976931348623157E308", "1e-323", "4.9E-324",
    "2.2250738585072014E-308", "1.1754944E-38", "3.4028235E38", "1e-45",
    "3.333333333333333E17", "2.7703798343611187E17", "1e400", "1e-400",
    "CAST('NaN' AS DOUBLE)", "CAST('Infinity' AS DOUBLE)", "CAST('-Infinity' AS DOUBLE)",
    "CAST(1.5 AS FLOAT)", "CAST('NaN' AS FLOAT)", "CAST(0.1 AS FLOAT)",
]

# Bare decimal literals. Spark types these as DECIMAL, so the wide ones are a precision-promotion
# question (#173) and the half-way ones are a rounding-mode question (#182).
DECIMAL_LITERALS = [
    "0.00", "1.005", "2.675", "-2.675", "0.005", "1.5BD", "2.5BD", "-1.5BD",
    "99999999999999999999999999999999999999",
    "-99999999999999999999999999999999999999",
    "12345678901234567890.123456789",
    "0.99999999999999999999999999999999999999",
    "1.0000000000000000000000000000000000001",
]

# Strings that a numeric parse has to make a decision about. Each one is a decision some engine
# gets wrong: whitespace, an exponent with no digits, a type suffix, a bare sign, a non-ASCII
# digit, a separator. #174, #258.
NUMERIC_TEXT = [
    "''", "' '", "'  '", "'12'", "'  12  '", "'\\t12\\n'",
    "'1e'", "'e1'", "'1e+'", "'1e400'", "'1e-400'",
    "'0x10'", "'0b101'", "'1_000'", "'1,000'",
    "'+1'", "'-0'", "'-'", "'+'", "'.'", "'0.'", "'.5'", "'12.'",
    "'1d'", "'1f'", "'1L'", "'1.2.3'",
    "'NaN'", "'nan'", "'Infinity'", "'-Infinity'", "'inf'",
    "'true'", "'null'", "'NULL'",
    "'99999999999999999999999999999999999999999'",
    "'0.00000000000000000000000000000000000000001'",
    "'٣'",   # ARABIC-INDIC DIGIT THREE -- a digit to Character.isDigit, not to a parser
    "'１'",   # FULLWIDTH DIGIT ONE
]

# General strings, for the string functions and the LIKE family.
STRING_LITERALS = [
    "''", "'abc'", "'ABC'", "'AbC'", "'  abc  '", "'a'", "'%'", "'_'",
    "'é'", "'\U0001F600'",   # a 2-byte char and a surrogate pair: length() counts UTF-16 units
    "'a%c'", "'\\\\'", "'x'",
]

LIKE_PATTERNS = ["'a%'", "'%c'", "'%b%'", "'a_c'", "'abc'", "'%'", "'_'", "''", "'\\\\%'"]
RLIKE_PATTERNS = ["'^a'", "'c$'", "'a.c'", "'[a-c]+'", "'.*'", "'a{2,}'"]

# Cast targets. The decimal widths are the interesting ones: (1,0) overflows almost everything,
# (38,38) has no integer part at all, (38,0) has no fraction.
CAST_TYPES = [
    "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "STRING", "BOOLEAN",
    "DECIMAL(1,0)", "DECIMAL(6,4)", "DECIMAL(10,2)", "DECIMAL(38,0)", "DECIMAL(38,10)",
    "DECIMAL(38,38)", "DATE", "TIMESTAMP", "BINARY",
]

# Columns, split by what they can stand in for. Taken from SCHEMA so the two cannot drift.
NUM_COLUMNS = ["a", "b", "sh", "f", "g", "d1", "d2", "d3", "d4", "d5"]
STR_COLUMNS = ["s", "t", "ns", "fs"]
DATE_COLUMNS = ["dt", "ts"]
BOOL_COLUMNS = ["bl"]

NUM_LEAVES = INT_LITERALS + FLOAT_LITERALS + DECIMAL_LITERALS + NUM_COLUMNS
STR_LEAVES = STRING_LITERALS + NUMERIC_TEXT + STR_COLUMNS
ANY_LEAVES = NUM_LEAVES + STR_LEAVES + BOOL_COLUMNS + ["NULL", "true", "false"]

# How often a slot grows a subtree instead of taking a leaf, and how deep that can go. Low on
# purpose: see the module docstring. At 0.25/2 about a third of expressions carry a nested call,
# which is enough to catch a coercion that only goes wrong when it feeds another coercion.
NEST_PROBABILITY = 0.25
MAX_DEPTH = 2


def _pick(rng, pool):
    return rng.choice(pool)


def _num(rng, depth):
    """A numeric-ish operand: a leaf, or a nested expression that produces a number."""
    if depth < MAX_DEPTH and rng.random() < NEST_PROBABILITY:
        return _pick(rng, [t_cast, t_arith, t_round, t_greatest_least, t_negative])(rng, depth + 1)
    return _pick(rng, NUM_LEAVES)


def _str(rng, depth):
    if depth < MAX_DEPTH and rng.random() < NEST_PROBABILITY:
        return _pick(rng, [t_string_fn, t_concat, t_substring])(rng, depth + 1)
    return _pick(rng, STR_LEAVES)


def _any(rng, depth):
    if depth < MAX_DEPTH and rng.random() < NEST_PROBABILITY:
        return _pick(rng, [t_cast, t_arith, t_case, t_coalesce])(rng, depth + 1)
    return _pick(rng, ANY_LEAVES)


# --- The shapes ------------------------------------------------------------------------
# Restricted to what SparkFunctionRegistry actually implements. A generator that reaches for
# `sqrt` would spend its budget rediscovering that we have not implemented `sqrt`, which is not a
# defect and not news. `t_unsupported` below is the deliberate, small exception.

def t_cast(rng, depth=0):
    return f"CAST({_any(rng, depth)} AS {_pick(rng, CAST_TYPES)})"


def t_try_cast(rng, depth=0):
    return f"TRY_CAST({_any(rng, depth)} AS {_pick(rng, CAST_TYPES)})"


def t_arith(rng, depth=0):
    op = _pick(rng, ["+", "-", "*", "/", "%"])
    return f"({_num(rng, depth)} {op} {_num(rng, depth)})"


def t_compare(rng, depth=0):
    cmp = _pick(rng, ["<", "<=", ">", ">=", "=", "!=", "<=>"])
    # Deliberately mixes the pools: a string against a number is the coercion question of
    # #180/#259, where the two dialects pick DIFFERENT cast targets.
    return f"({_any(rng, depth)} {cmp} {_any(rng, depth)})"


def t_round(rng, depth=0):
    return f"round({_num(rng, depth)}, {rng.randint(-5, 10)})"


def t_greatest_least(rng, depth=0):
    fn = _pick(rng, ["greatest", "least"])
    return f"{fn}({_num(rng, depth)}, {_num(rng, depth)})"


def t_negative(rng, depth=0):
    return f"negative({_num(rng, depth)})"


def t_in(rng, depth=0):
    members = ", ".join(_any(rng, depth) for _ in range(rng.randint(1, 3)))
    return f"{_any(rng, depth)} IN ({members})"


def t_between(rng, depth=0):
    return f"{_num(rng, depth)} BETWEEN {_num(rng, depth)} AND {_num(rng, depth)}"


def t_coalesce(rng, depth=0):
    fn = _pick(rng, ["coalesce", "ifnull", "nvl", "nullif"])
    return f"{fn}({_any(rng, depth)}, {_any(rng, depth)})"


def t_case(rng, depth=0):
    return (f"CASE WHEN {t_compare(rng, depth)} THEN {_any(rng, depth)} "
            f"ELSE {_any(rng, depth)} END")


def t_if(rng, depth=0):
    return f"if({t_compare(rng, depth)}, {_any(rng, depth)}, {_any(rng, depth)})"


def t_string_fn(rng, depth=0):
    fn = _pick(rng, ["length", "upper", "lower", "trim", "ltrim", "rtrim"])
    return f"{fn}({_str(rng, depth)})"


def t_substring(rng, depth=0):
    fn = _pick(rng, ["substring", "substr"])
    return f"{fn}({_str(rng, depth)}, {rng.randint(-3, 5)}, {rng.randint(-2, 6)})"


def t_concat(rng, depth=0):
    return f"concat({_str(rng, depth)}, {_str(rng, depth)})"


def t_like(rng, depth=0):
    op = _pick(rng, ["LIKE", "ILIKE"])
    return f"{_str(rng, depth)} {op} {_pick(rng, LIKE_PATTERNS)}"


def t_rlike(rng, depth=0):
    return f"{_str(rng, depth)} RLIKE {_pick(rng, RLIKE_PATTERNS)}"


def t_is_predicate(rng, depth=0):
    pred = _pick(rng, ["IS NULL", "IS NOT NULL", "IS TRUE", "IS FALSE", "IS NOT TRUE"])
    return f"{_any(rng, depth)} {pred}"


def t_logical(rng, depth=0):
    op = _pick(rng, ["AND", "OR"])
    neg = "NOT " if rng.random() < 0.3 else ""
    return f"({neg}{t_compare(rng, depth)} {op} {t_compare(rng, depth)})"


def t_datepart(rng, depth=0):
    fn = _pick(rng, ["year", "month", "day", "dayofmonth", "hour", "minute", "second"])
    operand = _pick(rng, DATE_COLUMNS + ["CAST(" + _pick(rng, NUMERIC_TEXT) + " AS DATE)"])
    return f"{fn}({operand})"


def t_date_format(rng, depth=0):
    fmt = _pick(rng, ["'yyyy-MM-dd'", "'yyyy'", "'HH:mm:ss'", "'MM/dd/yyyy'", "''"])
    return f"date_format({_pick(rng, DATE_COLUMNS)}, {fmt})"


def t_unsupported(rng, depth=0):
    """Deliberately outside the registry.

    Not noise: the property under test is that we REFUSE cleanly rather than answer wrongly. A
    silent wrong answer for a function nobody implemented is the worst outcome available, and
    nothing else in the suite looks for it.
    """
    fn = _pick(rng, ["abs", "sqrt", "ceil", "floor", "exp", "ln", "pow", "sign", "hex", "md5"])
    return f"{fn}({_num(rng, depth)})"


TEMPLATES = {
    "cast": t_cast,
    "try-cast": t_try_cast,
    "arith": t_arith,
    "compare": t_compare,
    "round": t_round,
    "greatest-least": t_greatest_least,
    "in": t_in,
    "between": t_between,
    "coalesce-family": t_coalesce,
    "case": t_case,
    "if": t_if,
    "string-fn": t_string_fn,
    "substring": t_substring,
    "concat": t_concat,
    "like": t_like,
    "rlike": t_rlike,
    "is-predicate": t_is_predicate,
    "logical": t_logical,
    "datepart": t_datepart,
    "date-format": t_date_format,
    "negative": t_negative,
    "unsupported": t_unsupported,
}

# `cast` gets the largest share because it is where the closed issues cluster, and `unsupported`
# the smallest because one confirmation per function is enough.
WEIGHTS = dict.fromkeys(TEMPLATES, 3)
WEIGHTS.update({"cast": 12, "try-cast": 6, "arith": 8, "compare": 8, "round": 5,
                "in": 4, "coalesce-family": 4, "unsupported": 1})


def generate(seed, count):
    """Returns {group: [expression]}, deduplicated, deterministic in (seed, count)."""
    rng = random.Random(seed)
    names = list(TEMPLATES)
    weights = [WEIGHTS[n] for n in names]

    groups = {name: [] for name in names}
    seen = set()
    # Bounded rather than while-True: a small pool plus a fixed shape can genuinely run out of
    # distinct expressions, and spinning forever on that is worse than returning fewer.
    for _ in range(count * 20):
        if sum(len(v) for v in groups.values()) >= count:
            break
        name = rng.choices(names, weights=weights)[0]
        expr = TEMPLATES[name](rng)
        if expr in seen:
            continue
        seen.add(expr)
        groups[name].append(expr)

    return {k: v for k, v in groups.items() if v}


def _canonical(expr):
    """The same question, answered by the JVM as text. See the module docstring."""
    return f"CAST(({expr}) AS STRING)"


def harvest(groups, conf, chunk):
    """Asks Spark about every expression and its canonical rendering, merging the two answers."""
    flat = [e for exprs in groups.values() for e in exprs]
    asked = list(dict.fromkeys(flat + [_canonical(e) for e in flat]))

    by_expr = {}
    meta = {}
    # Chunked so a session-killing expression costs one chunk rather than the whole run, and so a
    # long run reports progress. The driver holds one SparkSession per process, so a chunk is a
    # process start -- keep chunks large.
    for start in range(0, len(asked), chunk):
        piece = asked[start:start + chunk]
        result = _run_driver({"expressions": piece, "schema": SCHEMA, "rows": ROWS, "conf": conf})
        if not result.get("ok"):
            raise SystemExit("driver error: " + json.dumps(result)[:2000])
        for r in result["results"]:
            by_expr[r["expression"]] = r
        meta = result
        done = min(start + chunk, len(asked))
        print(f"    {done}/{len(asked)} answered", flush=True)

    out = {}
    for name, exprs in groups.items():
        entries = []
        for e in exprs:
            entry = by_expr.get(e)
            if entry is None:
                continue
            rendered = by_expr.get(_canonical(e))
            if rendered is not None:
                # Only the evaluated answer is worth carrying: the parse and type of a cast to
                # string say nothing the inner expression's own answers do not.
                entry["text"] = rendered.get("eval", {"ok": False, "error": "not evaluated"})
            entries.append(entry)
        if entries:
            out[name] = entries

    return out, meta


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--count", type=int, default=500, help="expressions per dialect")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--chunk", type=int, default=2000, help="expressions per SparkSession")
    ap.add_argument("--dialects", default="ansi,legacy", help="comma-separated: ansi, legacy")
    ap.add_argument("--out", default=os.path.join(tempfile.gettempdir(), "ew-fuzz",
                                                  "fuzz-expression-corpus.json"))
    args = ap.parse_args()

    groups = generate(args.seed, args.count)
    total = sum(len(v) for v in groups.values())
    print(f"generated {total} expressions in {len(groups)} groups "
          f"(seed {args.seed}); asking Spark about {total * 2} including renderings")

    dialects = [d.strip() for d in args.dialects.split(",") if d.strip()]
    fixture = {
        "_comment": "Generated by fuzz_expressions.py. NOT the checked-in corpus: findings are "
                    "minimised and promoted into Fixtures/spark-expression-corpus.json by hand.",
        "source": "fuzz_expressions.py",
        "seed": args.seed,
        "schema": SCHEMA,
        "rows": ROWS,
    }

    for dialect in dialects:
        conf = CONF if dialect == "ansi" else LEGACY_CONF
        print(f"  {dialect}...", flush=True)
        harvested, meta = harvest(groups, conf, args.chunk)
        section = {
            "conf": meta["conf"],
            "java_version": meta["java_version"],
            "spark_version": meta["spark_version"],
            "groups": harvested,
        }
        if dialect == "ansi":
            fixture.update({k: section[k] for k in ("conf", "java_version", "spark_version")})
            fixture["groups"] = harvested
        else:
            fixture["legacy"] = section

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(_json_safe(fixture), fh, indent=1, ensure_ascii=False, default=str,
                  allow_nan=False)
        fh.write("\n")

    print(f"{total} expressions -> {args.out}")
    print(f"  spark {fixture['spark_version']} on java {fixture['java_version']}")


if __name__ == "__main__":
    main()

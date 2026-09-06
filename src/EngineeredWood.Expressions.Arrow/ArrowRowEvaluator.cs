// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Expressions.Arrow;

/// <summary>
/// Walks <see cref="Expression"/> and <see cref="Predicate"/> trees against
/// a <see cref="RecordBatch"/>, producing typed Arrow arrays.
/// </summary>
/// <remarks>
/// Built-in support: column references, literals, IS NULL / IS NOT NULL,
/// IN / NOT IN, comparisons (with cross-type numeric promotion), AND / OR /
/// NOT with three-valued logic. Function calls are dispatched to an optional
/// <see cref="IFunctionRegistry"/>; if absent or the function isn't
/// registered, evaluation throws.
///
/// Internally each value expression evaluates to a <c>LiteralValue?[]</c>
/// (one element per row, null = SQL null). Predicates evaluate to a
/// <c>bool?[]</c> with the same null semantics. Both are converted to Arrow
/// arrays at the public boundary.
/// </remarks>
public sealed class ArrowRowEvaluator : IRowEvaluator
{
    private readonly IFunctionRegistry? _functions;

    /// <summary>The registry's comparison rules, when it has any. See <see cref="CoerceOperands"/>.</summary>
    private readonly IComparisonCoercion? _coercion;

    public ArrowRowEvaluator(IFunctionRegistry? functions = null)
    {
        _functions = functions;
        _coercion = functions as IComparisonCoercion;
    }

    public BooleanArray EvaluatePredicate(Predicate predicate, RecordBatch batch)
    {
        var result = EvalPredicate(predicate, batch);
        return ToBooleanArray(result, batch.Length);
    }

    public IArrowArray EvaluateExpression(Expression expression, RecordBatch batch) =>
        EvalExpressionAsArray(expression, batch);

    public IArrowArray EvaluateExpression(Expression expression, RecordBatch batch, IArrowType targetType)
    {
        var values = EvalExpression(expression, batch);
        return MaterializeAsArray(values, batch.Length, targetType);
    }

    // ── Predicate evaluation ──

    private bool?[] EvalPredicate(Predicate predicate, RecordBatch batch)
    {
        return predicate switch
        {
            TruePredicate => Constant(true, batch.Length),
            FalsePredicate => Constant(false, batch.Length),
            AndPredicate and => EvalAnd(and, batch),
            OrPredicate or => EvalOr(or, batch),
            NotPredicate not => EvalNot(not, batch),
            ComparisonPredicate cmp => EvalComparison(cmp, batch),
            UnaryPredicate unary => EvalUnary(unary, batch),
            SetPredicate set => EvalSet(set, batch),
            _ => throw new NotSupportedException(
                $"Unsupported predicate kind: {predicate.GetType().Name}"),
        };
    }

    private bool?[] EvalAnd(AndPredicate and, RecordBatch batch)
    {
        var result = new bool?[batch.Length];
        for (int i = 0; i < result.Length; i++) result[i] = true;

        foreach (var child in and.Children)
        {
            var childResult = EvalPredicate(child, batch);
            for (int i = 0; i < result.Length; i++)
            {
                // SQL three-valued AND:
                //   any child false → false
                //   any child null and no false → null
                //   all true → true
                if (result[i] == false || childResult[i] == false)
                    result[i] = false;
                else if (result[i] is null || childResult[i] is null)
                    result[i] = null;
                // else both true, keep true
            }
        }
        return result;
    }

    private bool?[] EvalOr(OrPredicate or, RecordBatch batch)
    {
        var result = new bool?[batch.Length];
        for (int i = 0; i < result.Length; i++) result[i] = false;

        foreach (var child in or.Children)
        {
            var childResult = EvalPredicate(child, batch);
            for (int i = 0; i < result.Length; i++)
            {
                if (result[i] == true || childResult[i] == true)
                    result[i] = true;
                else if (result[i] is null || childResult[i] is null)
                    result[i] = null;
                // else both false, keep false
            }
        }
        return result;
    }

    private bool?[] EvalNot(NotPredicate not, RecordBatch batch)
    {
        var child = EvalPredicate(not.Child, batch);
        var result = new bool?[child.Length];
        for (int i = 0; i < child.Length; i++)
            result[i] = child[i] is null ? null : !child[i];
        return result;
    }

    private bool?[] EvalComparison(ComparisonPredicate cmp, RecordBatch batch)
    {
        var (left, leftType) = EvalOperand(cmp.Left, batch);
        var (right, rightType) = EvalOperand(cmp.Right, batch);
        CoerceOperands(cmp, leftType, rightType, ref left, ref right);
        var result = new bool?[batch.Length];

        for (int i = 0; i < batch.Length; i++)
        {
            var l = left[i];
            var r = right[i];

            if (cmp.Op == ComparisonOperator.NullSafeEqual)
            {
                bool bothNull = !l.HasValue && !r.HasValue;
                bool oneNull = l.HasValue ^ r.HasValue;
                result[i] = bothNull
                    ? true
                    : oneNull
                        ? false
                        : ValueEqual(l!.Value, r!.Value);
                continue;
            }

            if (!l.HasValue || !r.HasValue)
            {
                result[i] = null;
                continue;
            }

            try
            {
                int c = l.Value.CompareTo(r.Value);
                result[i] = cmp.Op switch
                {
                    ComparisonOperator.Equal => c == 0,
                    ComparisonOperator.NotEqual => c != 0,
                    ComparisonOperator.LessThan => c < 0,
                    ComparisonOperator.LessThanOrEqual => c <= 0,
                    ComparisonOperator.GreaterThan => c > 0,
                    ComparisonOperator.GreaterThanOrEqual => c >= 0,
                    ComparisonOperator.StartsWith => StartsWith(l.Value, r.Value),
                    ComparisonOperator.NotStartsWith => !StartsWith(l.Value, r.Value),
                    _ => null,
                };
            }
            catch (InvalidOperationException)
            {
                // A pair with no comparison between them at all -- a boolean against a number, or
                // anything at all when no registry was supplied to coerce with. Not a string
                // against a number, a boolean, an instant or a binary any more: those are cast
                // before the loop, and under ANSI a malformed value raises out of the cast
                // rather than arriving here.
                result[i] = null;
            }
        }
        return result;
    }

    /// <summary>
    /// Casts the operand and every member of a set test to the one type they compare through.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>IN</c> is not the disjunction of equalities it resembles. Spark resolves ONE type over
    /// the operand and the whole list, so <c>a IN ('01')</c> is FALSE under the legacy dialect —
    /// the list resolves to text, and <c>'1'</c> is not <c>'01'</c> — while <c>a = '01'</c> is
    /// true. Which type is the registry's answer; see <see cref="IComparisonCoercion"/>.
    /// </para>
    /// <para>
    /// Without this the set compared through <see cref="LiteralValue.CompareTo(LiteralValue)"/>,
    /// which has no cross-kind branch for a string, so every mixed set answered false — not only
    /// where Spark refuses, but <c>ns IN (1, 2)</c> over the string <c>'1'</c>, which Spark
    /// answers TRUE in both dialects. #259.
    /// </para>
    /// <para>
    /// The kinds are checked before any type is resolved, because resolving one allocates and
    /// the overwhelmingly common set — a column against literals of its own type — needs none.
    /// </para>
    /// </remarks>
    private void CoerceSet(
        ref LiteralValue?[] operand, IArrowType? operandType,
        LiteralValue?[][] members, IArrowType?[] memberTypes, int rowCount)
    {
        if (_coercion is null)
            return;

        bool anyString = IsString(operandType, operand);
        bool anyOther = !anyString && FirstKind(operand) is not null;

        for (var k = 0; k < members.Length; k++)
        {
            if (IsString(memberTypes[k], members[k])) anyString = true;
            else if (FirstKind(members[k]) is not null) anyOther = true;
        }

        if (!anyString || !anyOther)
            return;   // one kind throughout: nothing to resolve

        // Resolved once and reused, because the type decides two things: which target the set
        // takes, and how each member is rebuilt as an Arrow array. Inferring the second from
        // values instead cannot build a decimal at all and reads a date as an instant.
        var operandResolved = OperandType(operandType, operand);
        var resolved = new IArrowType[members.Length];
        var types = new List<IArrowType>(members.Length + 1) { operandResolved };
        for (var k = 0; k < members.Length; k++)
        {
            resolved[k] = OperandType(memberTypes[k], members[k]);
            types.Add(resolved[k]);
        }

        var target = _coercion.SetComparisonTarget(types);
        if (target is null)
            return;   // a set the registry has no rule for; compare it as it stands

        operand = CastMember(operand, operandResolved, target, rowCount);
        for (var k = 0; k < members.Length; k++)
            members[k] = CastMember(members[k], resolved[k], target, rowCount);
    }

    /// <summary>Rebuilds one member of a set as <paramref name="target"/>.</summary>
    private LiteralValue?[] CastMember(
        LiteralValue?[] values, IArrowType type, IArrowType target, int rowCount) =>
        ArrowToLiteralValues(
            _coercion!.CastForComparison(
                MaterializeAsArray(values, rowCount, type), target, rowCount),
            rowCount);

    /// <summary>
    /// Casts whichever operand has to move before the two can be compared, in place.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Spark resolves a string against a non-string by CASTING — it does not refuse, and it
    /// does not always cast the string. Without this the comparison reached
    /// <see cref="LiteralValue.CompareTo(LiteralValue)"/>, which has no cross-kind branch for a
    /// string, and every such comparison answered null: not only <c>s = a</c> over a malformed
    /// string, where Spark refuses under ANSI, but <c>'1' = a</c> over a valid one, where Spark
    /// answers true. The wrong answer was the larger half. #180.
    /// </para>
    /// <para>
    /// A string against a BINARY is the pair where the other operand moves: the binary is
    /// rendered as text and two strings are compared. #259.
    /// </para>
    /// <para>
    /// The target is dialect-dependent, so the registry chooses it — see
    /// <see cref="IComparisonCoercion"/>. Without a registry there is nothing to cast with and
    /// the comparison is left as it was.
    /// </para>
    /// <para>
    /// <b>A row whose other operand is null is not cast, and <c>&lt;=&gt;</c> is the exception.</b>
    /// Spark's relational operators evaluate nothing once an operand is null, so a malformed
    /// string sitting opposite a null is never read and never refused; null-safe equality has no
    /// such short-circuit and does refuse. Measured over a row of <c>(a = NULL, s = 'abc')</c>:
    /// <c>s = a</c> is null under ANSI, in both operand orders, while <c>s &lt;=&gt; a</c> raises
    /// CAST_INVALID_INPUT. Without the mask a batch mixing one such row with an ordinary one
    /// would refuse a write Spark accepts.
    /// </para>
    /// </remarks>
    private void CoerceOperands(
        ComparisonPredicate cmp, IArrowType? leftType, IArrowType? rightType,
        ref LiteralValue?[] left, ref LiteralValue?[] right)
    {
        if (_coercion is null)
            return;

        // Every measured rule has a string on exactly one side, so this one cheap test decides
        // whether to resolve types at all -- and typing an operand can allocate.
        bool leftIsString = IsString(leftType, left);
        if (leftIsString == IsString(rightType, right))
            return;   // two strings, or neither: nothing to coerce

        var strings = leftIsString ? left : right;
        var other = leftIsString ? right : left;
        var stringType = OperandType(leftIsString ? leftType : rightType, strings);
        var otherType = OperandType(leftIsString ? rightType : leftType, other);

        // Which operand moves is the registry's answer, not an assumption here: a string against
        // a number is cast to the number, while a string against a binary stays and the BINARY
        // is rendered as text. At most one side moves, so the second question is only asked when
        // the first declines.
        bool castTheString = true;
        var target = _coercion.ComparisonTarget(stringType, otherType);
        if (target is null)
        {
            castTheString = false;
            target = _coercion.ComparisonTarget(otherType, stringType);
        }

        if (target is null)
            return;   // a pair the registry has no rule for; compare it as it stands

        var moving = castTheString ? strings : other;
        var staying = castTheString ? other : strings;
        int rowCount = moving.Length;

        var coerced = _coercion.CastForComparison(
            MaterializeAsArray(
                cmp.Op == ComparisonOperator.NullSafeEqual ? moving : NulledWhere(moving, staying),
                rowCount),
            target,
            rowCount);

        var values = ArrowToLiteralValues(coerced, rowCount);
        if (leftIsString == castTheString) left = values;
        else right = values;
    }

    /// <summary>
    /// Evaluates a comparison operand, keeping the Arrow type when the operand declares one.
    /// </summary>
    /// <remarks>
    /// The same calls <see cref="EvalExpression"/> makes for these three cases, with the array
    /// KEPT rather than discarded — no operand is evaluated twice. The type is what a
    /// <see cref="LiteralValue"/> array cannot carry and what the coercion turns on: a decimal's
    /// precision and scale, a date against a timestamp, and the type of an operand whose every
    /// row is null. All three are reachable through a cast, whose result carries exactly the type
    /// the cast was asked for — measured, <c>'2026-08-11 12:30:00' = CAST(ts AS DATE)</c> is true
    /// in Spark, and reading that operand as the instant its values look like would compare the
    /// string against midnight instead of truncating it.
    /// <para>
    /// A literal is deliberately not routed through an array. It is typed from its own value,
    /// which is what Spark does with one, and materialising a constant array per comparison would
    /// put an allocation on the hot path for the sake of a type already known.
    /// </para>
    /// </remarks>
    private (LiteralValue?[] Values, IArrowType? Type) EvalOperand(
        Expression expression, RecordBatch batch)
    {
        IArrowArray array;
        switch (expression)
        {
            case UnboundReference u: array = GetColumn(batch, u.Name); break;
            case BoundReference b: array = GetColumn(batch, b.Name); break;
            case FunctionCall fc: array = InvokeFunction(fc, batch); break;
            default: return (EvalExpression(expression, batch), null);
        }

        return (ArrowToLiteralValues(array, batch.Length), array.Data.DataType);
    }

    /// <summary>Whether an operand is a string: from its declared type, or from its values.</summary>
    /// <remarks>
    /// The declared type answers even for an all-null operand, where the values cannot, and that
    /// matters in both directions. An all-null string casts to null under every target and so
    /// cannot change an answer; an all-null operand on the OTHER side still types the cast, and
    /// <c>&lt;=&gt;</c> reads it — measured, <c>s &lt;=&gt; CAST(NULL AS INT)</c> raises under
    /// ANSI rather than answering false.
    /// </remarks>
    private static bool IsString(IArrowType? declared, LiteralValue?[] values) =>
        declared is not null
            ? declared is StringType
            : FirstKind(values) == LiteralValue.Kind.String;

    /// <summary>
    /// <paramref name="values"/> with a null wherever <paramref name="mask"/> is null.
    /// </summary>
    /// <remarks>
    /// Returns the original array when there is nothing to null out, which is the ordinary case:
    /// a column with no nulls opposite it costs one pass and no allocation.
    /// </remarks>
    private static LiteralValue?[] NulledWhere(LiteralValue?[] values, LiteralValue?[] mask)
    {
        LiteralValue?[]? masked = null;
        for (int i = 0; i < values.Length; i++)
        {
            if (mask[i].HasValue || !values[i].HasValue)
                continue;

            masked ??= (LiteralValue?[])values.Clone();
            masked[i] = null;
        }

        return masked ?? values;
    }

    /// <summary>The kind an operand's values carry, or null when every row is null.</summary>
    /// <remarks>
    /// One array holds one kind — it is built from one Arrow array, one literal, or one
    /// predicate — so the first non-null value speaks for all of them.
    /// </remarks>
    private static LiteralValue.Kind? FirstKind(LiteralValue?[] values)
    {
        foreach (var value in values)
        {
            if (value.HasValue)
                return value.Value.Type;
        }

        return null;
    }

    /// <summary>The Arrow type of a comparison operand, for choosing what to cast against.</summary>
    /// <remarks>
    /// The declared type where the operand has one. A literal has none, and is typed from its own
    /// value through the rules Spark gives one — <c>1.5</c> is a <c>decimal(2,1)</c>, which is
    /// what <see cref="ConstantArray"/> already encodes.
    /// </remarks>
    private static IArrowType OperandType(IArrowType? declared, LiteralValue?[] values)
    {
        if (declared is not null)
            return declared;

        foreach (var value in values)
        {
            if (value.HasValue)
                return ConstantArray(value.Value, 1).Data.DataType;
        }

        // An untyped operand null in every row — a bare NULL literal. String is the type with no
        // coercion rule, so the comparison is left as it stands.
        return StringType.Default;
    }

    private bool?[] EvalUnary(UnaryPredicate unary, RecordBatch batch)
    {
        var operand = EvalExpression(unary.Operand, batch);
        var result = new bool?[batch.Length];

        for (int i = 0; i < batch.Length; i++)
        {
            var v = operand[i];
            result[i] = unary.Op switch
            {
                UnaryOperator.IsNull => !v.HasValue,
                UnaryOperator.IsNotNull => v.HasValue,
                UnaryOperator.IsNaN => v.HasValue && IsNaN(v.Value),
                UnaryOperator.IsNotNaN => !v.HasValue ? null : !IsNaN(v.Value),
                _ => throw new NotSupportedException($"Unary op {unary.Op}"),
            };
        }
        return result;
    }

    private bool?[] EvalSet(SetPredicate set, RecordBatch batch)
    {
        var (operand, operandType) = EvalOperand(set.Operand, batch);

        // Every member is an expression, so each is a column of its own: `x IN (a, b)` compares
        // row i of x against row i of a and of b, not against two constants.
        var members = new LiteralValue?[set.Values.Count][];
        var memberTypes = new IArrowType?[set.Values.Count];
        for (var k = 0; k < set.Values.Count; k++)
            (members[k], memberTypes[k]) = EvalOperand(set.Values[k], batch);

        CoerceSet(ref operand, operandType, members, memberTypes, batch.Length);

        var result = new bool?[batch.Length];
        bool isIn = set.Op == SetOperator.In;

        for (int i = 0; i < batch.Length; i++)
        {
            var v = operand[i];
            if (!v.HasValue)
            {
                // SQL: NULL IN (...) is null; NULL NOT IN (...) is also null.
                result[i] = null;
                continue;
            }

            bool found = false;
            bool sawNullInList = false;
            foreach (var member in members)
            {
                var lit = member[i];
                if (!lit.HasValue) { sawNullInList = true; continue; }
                try
                {
                    if (v.Value.CompareTo(lit.Value) == 0) { found = true; break; }
                }
                catch (InvalidOperationException) { /* incompatible types */ }
            }

            // SQL semantics: IN with a null in the list and no match → null.
            if (isIn)
                result[i] = found ? true : (sawNullInList ? null : false);
            else
                result[i] = found ? false : (sawNullInList ? null : true);
        }
        return result;
    }

    // ── Expression evaluation ──

    private LiteralValue?[] EvalExpression(Expression expression, RecordBatch batch)
    {
        switch (expression)
        {
            case LiteralExpression lit:
                return Repeat(lit.Value.IsNull ? null : (LiteralValue?)lit.Value, batch.Length);

            case UnboundReference u:
                return ArrowToLiteralValues(GetColumn(batch, u.Name), batch.Length);

            case BoundReference b:
                return ArrowToLiteralValues(GetColumn(batch, b.Name), batch.Length);

            case Predicate p:
                return BoolsToLiteralValues(EvalPredicate(p, batch));

            case FunctionCall fc:
                return ArrowToLiteralValues(InvokeFunction(fc, batch), batch.Length);

            default:
                throw new NotSupportedException(
                    $"Unsupported expression: {expression.GetType().Name}");
        }
    }

    // ── Arrow-native evaluation, for the function boundary ──

    /// <summary>
    /// Evaluates an expression straight to an Arrow array, without the
    /// <c>LiteralValue?[]</c> detour the rest of the evaluator uses.
    /// </summary>
    /// <remarks>
    /// This exists because a <see cref="LiteralValue"/> cannot carry a declared type. A
    /// <c>decimal(10,2)</c> column round-tripped through one arrives as a bare
    /// <see cref="decimal"/>, and the Arrow array rebuilt from it has lost the precision and
    /// scale — which is exactly what Spark's promotion rules are computed from. A function
    /// receiving two such arguments cannot know that <c>d1 + d2</c> should produce
    /// <c>decimal(13,4)</c>. Worse, the type-inferring materializer has no decimal case at all,
    /// so a decimal argument threw before the registry was ever consulted.
    ///
    /// Column references therefore pass through as the batch's own arrays, and a nested call's
    /// result travels on as whatever the registry returned. Only literals are built here, and
    /// only from what the value itself implies.
    /// </remarks>
    private IArrowArray EvalExpressionAsArray(Expression expression, RecordBatch batch) =>
        expression switch
        {
            UnboundReference u => GetColumn(batch, u.Name),
            BoundReference b => GetColumn(batch, b.Name),
            FunctionCall fc => InvokeFunction(fc, batch),
            Predicate p => ToBooleanArray(EvalPredicate(p, batch), batch.Length),
            LiteralExpression lit => ConstantArray(lit.Value, batch.Length),
            _ => MaterializeAsArray(EvalExpression(expression, batch), batch.Length),
        };

    private IArrowArray InvokeFunction(FunctionCall call, RecordBatch batch)
    {
        if (_functions is null || !_functions.IsRegistered(call.Name))
            throw new InvalidOperationException(
                $"No function registered for '{call.Name}'. " +
                "Provide an IFunctionRegistry to ArrowRowEvaluator.");

        var arguments = new IArrowArray[call.Arguments.Count];
        for (int i = 0; i < call.Arguments.Count; i++)
            arguments[i] = EvalExpressionAsArray(call.Arguments[i], batch);

        return _functions.Invoke(call.Name, arguments, batch.Length);
    }

    /// <summary>Builds a constant array of <paramref name="value"/>, repeated.</summary>
    private static IArrowArray ConstantArray(LiteralValue value, int length)
    {
        // A decimal literal's type comes from the value, matching Spark: `1.5` is decimal(2,1),
        // `.5` is decimal(1,1) and `1.` is decimal(1,0).
        if (!value.IsNull && value.Type == LiteralValue.Kind.Decimal)
        {
            var (precision, scale) = DecimalTypeOf(value.AsDecimal);
            return MaterializeAsArray(
                Repeat(value, length), length, new Decimal128Type(precision, scale));
        }

        // ...and the same for one too wide for System.Decimal, which the parser now reads (#173).
        // Without this the literal parses and then cannot be turned into a column, which is the
        // same seam one method further along: `d4 + <38 digits>` failed here rather than there.
        if (!value.IsNull && value.Type == LiteralValue.Kind.HighPrecisionDecimal)
        {
            var (unscaled, wideScale) = value.AsHighPrecisionDecimal;
            return MaterializeAsArray(
                Repeat(value, length), length,
                new Decimal128Type(DecimalPrecisionOf(unscaled, wideScale), wideScale));
        }

        return MaterializeAsArray(Repeat(value.IsNull ? null : (LiteralValue?)value, length), length);
    }

    /// <summary>
    /// The precision Spark gives a decimal literal, from its unscaled value and scale.
    /// </summary>
    /// <remarks>
    /// The scale is a floor, for the reason <see cref="DecimalTypeOf"/> gives: <c>0.05</c> has one
    /// significant digit and still needs precision 2 to exist at scale 2.
    /// </remarks>
    private static int DecimalPrecisionOf(BigInteger unscaled, int scale) =>
        Math.Max(Math.Max(DigitCount(unscaled), scale), 1);

    /// <summary>
    /// The number of decimal digits in a value, counted exactly.
    /// </summary>
    /// <remarks>
    /// <b>Not <c>BigInteger.Log10</c>, which is a double and gets the boundaries wrong in both
    /// directions.</b> Measured: it types 10^30 as thirty digits, because its logarithm comes back
    /// as 29.999999999999996 rather than 30 — and 10^38-1 as thirty-nine, because that one rounds
    /// UP to 38.0. The first made <c>-1000000000000000000000000000000</c> evaluate to null, since
    /// a value needing thirty-one digits was given a decimal(30,0) to live in and overflowed it;
    /// the second built a decimal(39,0), which is wider than any Spark decimal.
    /// <para>
    /// Repeated division is exact and costs nothing that matters: this runs once per literal, not
    /// once per row.
    /// </para>
    /// </remarks>
    private static int DigitCount(BigInteger value)
    {
        var magnitude = BigInteger.Abs(value);
        var digits = 0;

        do
        {
            digits++;
            magnitude /= Ten;
        }
        while (!magnitude.IsZero);

        return digits;
    }

    private static readonly BigInteger Ten = new(10);

    /// <summary>The precision and scale Spark gives a decimal literal.</summary>
    private static (int Precision, int Scale) DecimalTypeOf(decimal value)
    {
        var bits = decimal.GetBits(value);
        int scale = (bits[3] >> 16) & 0xFF;

        // Digits in the unscaled value. `0.05` has one significant digit but needs precision 2
        // to be representable at scale 2, so the scale is a floor.
        var unscaled = BigInteger.Abs(
            new BigInteger((uint)bits[0]) |
            (new BigInteger((uint)bits[1]) << 32) |
            (new BigInteger((uint)bits[2]) << 64));

        return (Math.Max(Math.Max(DigitCount(unscaled), scale), 1), scale);
    }

    private static IArrowArray GetColumn(RecordBatch batch, string name)
    {
        int idx = batch.Schema.GetFieldIndex(name);
        if (idx < 0)
            throw new ArgumentException(
                $"Column '{name}' not found in batch schema.");
        return batch.Column(idx);
    }

    // ── Arrow ↔ LiteralValue ──

    private static LiteralValue?[] ArrowToLiteralValues(IArrowArray array, int length)
    {
        var result = new LiteralValue?[length];
        switch (array)
        {
            case BooleanArray a:
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null : (LiteralValue?)LiteralValue.Of(a.GetValue(i)!.Value);
                break;
            case Int8Array a:
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null : (LiteralValue?)LiteralValue.Of((int)a.GetValue(i)!.Value);
                break;
            case Int16Array a:
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null : (LiteralValue?)LiteralValue.Of((int)a.GetValue(i)!.Value);
                break;
            case Int32Array a:
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null : (LiteralValue?)LiteralValue.Of(a.GetValue(i)!.Value);
                break;
            case Int64Array a:
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null : (LiteralValue?)LiteralValue.Of(a.GetValue(i)!.Value);
                break;
            case UInt8Array a:
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null : (LiteralValue?)LiteralValue.Of((int)a.GetValue(i)!.Value);
                break;
            case UInt16Array a:
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null : (LiteralValue?)LiteralValue.Of((int)a.GetValue(i)!.Value);
                break;
            case UInt32Array a:
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null : (LiteralValue?)LiteralValue.Of(a.GetValue(i)!.Value);
                break;
            case UInt64Array a:
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null : (LiteralValue?)LiteralValue.Of(a.GetValue(i)!.Value);
                break;
            case FloatArray a:
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null : (LiteralValue?)LiteralValue.Of(a.GetValue(i)!.Value);
                break;
            case DoubleArray a:
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null : (LiteralValue?)LiteralValue.Of(a.GetValue(i)!.Value);
                break;
            case StringArray a:
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null : (LiteralValue?)LiteralValue.Of(a.GetString(i));
                break;
            case BinaryArray a:
                for (int i = 0; i < length; i++)
                {
                    if (a.IsNull(i)) result[i] = null;
                    else result[i] = LiteralValue.Of(a.GetBytes(i).ToArray());
                }
                break;
            // Temporal + decimal columns map to the SAME LiteralValue kinds a stats/JSON decoder would
            // produce for the corresponding logical types (DateTimeOffset for date and timestamp; decimal
            // or high-precision decimal for decimal), so a predicate literal compares identically whether
            // it is tested against a per-row column value here or against file statistics elsewhere.
            case Date32Array a:
                // Date32 = days since the Unix epoch; a calendar date is UTC midnight of that day.
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null
                        : (LiteralValue?)LiteralValue.Of(Epoch.AddDays(a.GetValue(i)!.Value));
                break;
            case Date64Array a:
                // Date64 = milliseconds since the Unix epoch (a whole number of days per the Arrow spec).
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null
                        : (LiteralValue?)LiteralValue.Of(Epoch.AddMilliseconds(a.GetValue(i)!.Value));
                break;
            case TimestampArray a:
                // GetTimestamp honours the column's unit and timezone, yielding the instant as a
                // DateTimeOffset (UTC) — the same instant the stats decoder recovers from the ISO string.
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null
                        : (LiteralValue?)LiteralValue.Of(a.GetTimestamp(i)!.Value);
                break;
            // Decimal32/64 (precision <= 18) always fit System.Decimal, so no high-precision path is
            // needed; a reader may narrow a small-precision decimal column to one of these.
            case Decimal32Array a:
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null : (LiteralValue?)LiteralValue.Of(a.GetValue(i)!.Value);
                break;
            case Decimal64Array a:
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null : (LiteralValue?)LiteralValue.Of(a.GetValue(i)!.Value);
                break;
            case Decimal128Array a:
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null : (LiteralValue?)DecimalLiteral(a, i);
                break;
            case Decimal256Array a:
                for (int i = 0; i < length; i++)
                    result[i] = a.IsNull(i) ? null : (LiteralValue?)DecimalLiteral(a, i);
                break;
            default:
                throw new NotSupportedException(
                    $"Cannot evaluate over Arrow array of type {array.Data.DataType.Name}.");
        }
        return result;
    }

    private static IArrowArray MaterializeAsArray(LiteralValue?[] values, int length)
    {
        // Choose an Arrow type from the first non-null value; default to string
        // if everything is null.
        LiteralValue.Kind? kind = null;
        for (int i = 0; i < length; i++)
        {
            if (values[i].HasValue) { kind = values[i]!.Value.Type; break; }
        }

        if (kind is null) return BuildAllNullStrings(length);

        switch (kind.Value)
        {
            case LiteralValue.Kind.Boolean:
                var bb = new BooleanArray.Builder();
                for (int i = 0; i < length; i++)
                {
                    if (values[i].HasValue) bb.Append(values[i]!.Value.AsBoolean);
                    else bb.AppendNull();
                }
                return bb.Build();
            case LiteralValue.Kind.Int32:
                var i32b = new Int32Array.Builder();
                for (int i = 0; i < length; i++)
                {
                    if (values[i].HasValue) i32b.Append(values[i]!.Value.AsInt32);
                    else i32b.AppendNull();
                }
                return i32b.Build();
            case LiteralValue.Kind.Int64:
                var i64b = new Int64Array.Builder();
                for (int i = 0; i < length; i++)
                {
                    if (values[i].HasValue) i64b.Append(values[i]!.Value.AsInt64);
                    else i64b.AppendNull();
                }
                return i64b.Build();
            case LiteralValue.Kind.Float:
                var fb = new FloatArray.Builder();
                for (int i = 0; i < length; i++)
                {
                    if (values[i].HasValue) fb.Append(values[i]!.Value.AsFloat);
                    else fb.AppendNull();
                }
                return fb.Build();
            case LiteralValue.Kind.Double:
                var db = new DoubleArray.Builder();
                for (int i = 0; i < length; i++)
                {
                    if (values[i].HasValue) db.Append(values[i]!.Value.AsDouble);
                    else db.AppendNull();
                }
                return db.Build();
            case LiteralValue.Kind.String:
                var sb = new StringArray.Builder();
                for (int i = 0; i < length; i++)
                {
                    if (values[i].HasValue) sb.Append(values[i]!.Value.AsString);
                    else sb.AppendNull();
                }
                return sb.Build();
            // A timestamp literal, and the instant a DATE literal carries before its cast. The
            // type-inferring path could not build either, so `TIMESTAMP'…'` parsed and resolved
            // and then failed at materialisation. Microseconds in UTC, matching what the readers
            // produce and what SparkLiteral resolves a zone-less literal to. #254.
            case LiteralValue.Kind.DateTimeOffset:
                return BuildTimestampArray(
                    values, length, new TimestampType(TimeUnit.Microsecond, "UTC"));

            case LiteralValue.Kind.Binary:
                var binb = new BinaryArray.Builder();
                for (int i = 0; i < length; i++)
                {
                    if (values[i].HasValue) binb.Append(values[i]!.Value.AsBinary);
                    else binb.AppendNull();
                }
                return binb.Build();
            default:
                throw new NotSupportedException(
                    $"Cannot materialize LiteralValue kind {kind.Value} as Arrow array.");
        }
    }

    // Materialize against a caller-supplied Arrow type. The decimal / temporal cases need metadata a bare
    // LiteralValue cannot carry (precision/scale/width, unit/timezone, date-vs-timestamp); every other type
    // is inferrable, so it falls through to the type-inferring overload.
    private static IArrowArray MaterializeAsArray(LiteralValue?[] values, int length, IArrowType targetType) =>
        targetType switch
        {
            Decimal128Type dt => BuildDecimalArray(values, length, dt.Scale, 16,
                data => new Decimal128Array(data), dt),
            Decimal256Type dt => BuildDecimalArray(values, length, dt.Scale, 32,
                data => new Decimal256Array(data), dt),
            TimestampType tt => BuildTimestampArray(values, length, tt),
            Date32Type => BuildDate32Array(values, length),
            Date64Type => BuildDate64Array(values, length),
            _ => MaterializeAsArray(values, length),
        };

    private static IArrowArray BuildDecimalArray(
        LiteralValue?[] values, int length, int scale, int byteWidth,
        Func<ArrayData, IArrowArray> create, IArrowType type)
    {
        var bytes = new byte[length * byteWidth];
        var validity = new ArrowBuffer.BitmapBuilder();
        int nullCount = 0;
        for (int i = 0; i < length; i++)
        {
            if (!values[i].HasValue)
            {
                validity.Append(false);
                nullCount++;
                continue;
            }
            validity.Append(true);
            BigInteger unscaled = ToUnscaled(values[i]!.Value, scale);
            var dest = bytes.AsSpan(i * byteWidth, byteWidth);
            dest.Fill(unscaled.Sign < 0 ? (byte)0xFF : (byte)0x00);
#if NET6_0_OR_GREATER
            unscaled.TryWriteBytes(dest, out _, isUnsigned: false, isBigEndian: false);
#else
            byte[] le = unscaled.ToByteArray();
            le.AsSpan(0, Math.Min(le.Length, byteWidth)).CopyTo(dest);
#endif
        }
        var data = new ArrayData(type, length, nullCount, 0, [validity.Build(), new ArrowBuffer(bytes)]);
        return create(data);
    }

    private static IArrowArray BuildTimestampArray(LiteralValue?[] values, int length, TimestampType type)
    {
        var b = new TimestampArray.Builder(type);
        for (int i = 0; i < length; i++)
        {
            if (values[i].HasValue) b.Append(ToDateTimeOffset(values[i]!.Value));
            else b.AppendNull();
        }
        return b.Build();
    }

    private static IArrowArray BuildDate32Array(LiteralValue?[] values, int length)
    {
        var b = new Date32Array.Builder();
        for (int i = 0; i < length; i++)
        {
            if (values[i].HasValue) b.Append(ToDateTimeOffset(values[i]!.Value).UtcDateTime);
            else b.AppendNull();
        }
        return b.Build();
    }

    private static IArrowArray BuildDate64Array(LiteralValue?[] values, int length)
    {
        var b = new Date64Array.Builder();
        for (int i = 0; i < length; i++)
        {
            if (values[i].HasValue) b.Append(ToDateTimeOffset(values[i]!.Value).UtcDateTime);
            else b.AppendNull();
        }
        return b.Build();
    }

    // A decimal/integer value as an unscaled integer at the target column scale.
    private static BigInteger ToUnscaled(LiteralValue v, int targetScale) => v.Type switch
    {
        LiteralValue.Kind.HighPrecisionDecimal => Rescale(
            v.AsHighPrecisionDecimal.UnscaledValue, v.AsHighPrecisionDecimal.Scale, targetScale),
        LiteralValue.Kind.Decimal => RescaleDecimal(v.AsDecimal, targetScale),
        LiteralValue.Kind.Int32 => Rescale(v.AsInt32, 0, targetScale),
        LiteralValue.Kind.Int64 => Rescale(v.AsInt64, 0, targetScale),
        _ => throw new NotSupportedException($"Cannot materialize {v.Type} as a decimal."),
    };

    private static BigInteger RescaleDecimal(decimal value, int targetScale)
    {
        int[] bits = decimal.GetBits(value);
        int scale = (bits[3] >> 16) & 0x7F;
        bool negative = (bits[3] & unchecked((int)0x80000000)) != 0;
        var magnitude = (new BigInteger((uint)bits[2]) << 64)
            | (new BigInteger((uint)bits[1]) << 32)
            | new BigInteger((uint)bits[0]);
        return Rescale(negative ? -magnitude : magnitude, scale, targetScale);
    }

    private static BigInteger Rescale(BigInteger unscaled, int fromScale, int toScale)
    {
        if (toScale > fromScale) return unscaled * BigInteger.Pow(10, toScale - fromScale);
        if (toScale < fromScale) return unscaled / BigInteger.Pow(10, fromScale - toScale);
        return unscaled;
    }

    private static DateTimeOffset ToDateTimeOffset(LiteralValue v) => v.Type switch
    {
        LiteralValue.Kind.DateTimeOffset => v.AsDateTimeOffset,
#if NET6_0_OR_GREATER
        // A calendar date is UTC midnight — symmetric with how date columns are read as DateTimeOffset.
        LiteralValue.Kind.DateOnly => new DateTimeOffset(
            v.AsDateOnly.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero),
#endif
        _ => throw new NotSupportedException($"Cannot materialize {v.Type} as a date/timestamp."),
    };

    private static IArrowArray BuildAllNullStrings(int length)
    {
        var b = new StringArray.Builder();
        for (int i = 0; i < length; i++) b.AppendNull();
        return b.Build();
    }

    private static BooleanArray ToBooleanArray(bool?[] values, int length)
    {
        var b = new BooleanArray.Builder();
        for (int i = 0; i < length; i++)
        {
            if (values[i].HasValue) b.Append(values[i]!.Value);
            else b.AppendNull();
        }
        return (BooleanArray)b.Build();
    }

    // ── Helpers ──

    private static bool?[] Constant(bool value, int length)
    {
        var arr = new bool?[length];
        for (int i = 0; i < length; i++) arr[i] = value;
        return arr;
    }

    private static LiteralValue?[] Repeat(LiteralValue? value, int length)
    {
        var arr = new LiteralValue?[length];
        for (int i = 0; i < length; i++) arr[i] = value;
        return arr;
    }

    private static LiteralValue?[] BoolsToLiteralValues(bool?[] bools)
    {
        var arr = new LiteralValue?[bools.Length];
        for (int i = 0; i < bools.Length; i++)
            arr[i] = bools[i].HasValue ? (LiteralValue?)LiteralValue.Of(bools[i]!.Value) : null;
        return arr;
    }

    private static bool ValueEqual(LiteralValue a, LiteralValue b)
    {
        try { return a.CompareTo(b) == 0; }
        catch (InvalidOperationException) { return false; }
    }

    private static bool StartsWith(LiteralValue value, LiteralValue prefix) =>
        value.Type == LiteralValue.Kind.String
        && prefix.Type == LiteralValue.Kind.String
        && value.AsString.StartsWith(prefix.AsString, StringComparison.Ordinal);

    private static bool IsNaN(LiteralValue v) => v.Type switch
    {
        LiteralValue.Kind.Float => float.IsNaN(v.AsFloat),
        LiteralValue.Kind.Double => double.IsNaN(v.AsDouble),
        _ => false,
    };

    private static readonly DateTimeOffset Epoch = new(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>The widest precision and scale <see cref="decimal"/> represents without loss.</summary>
    /// <remarks>
    /// Its mantissa is 96 bits, so it tops out near 7.9228e28 — 29 digits, but not all 29-digit
    /// values — and its scale runs 0 to 28. A column declared no wider than this in BOTH holds no
    /// value it cannot carry exactly; one declared wider holds values it cannot, so the whole
    /// column takes the exact path.
    /// </remarks>
    private const int MaxExactDecimalDigits = 28;

    // A decimal column value: the common in-range case as System.Decimal (how a decimal literal and a
    // stats decoder also represent it); a column that can hold values System.Decimal cannot takes its
    // exact unscaled BigInteger plus the column's scale, read straight from the fixed-width
    // little-endian value buffer — the same raw layout the format writers use.
    //
    // DECIDED FROM THE DECLARED TYPE, NOT FROM AN EXCEPTION. Decimal128Array.GetValue raises
    // OverflowException only for excess MAGNITUDE; for excess significant DIGITS it silently rounds
    // to 28 and reports success. Keying the fallback on the exception therefore missed exactly the
    // values it existed to protect — a decimal(38,38) is under 1, never overflows, and arrived
    // already rounded — so the cell was wrong before any comparison touched it. See #205, and #175
    // for the same rounding surfacing in rendering.
    //
    // Conservative on purpose: a small value in a wide-declared column takes the exact path it does
    // not strictly need. That costs a BigInteger and is the side to err on, because the alternative
    // is a per-cell test that has to be right about every corner of decimal's 96-bit mantissa.
    private static LiteralValue DecimalLiteral(Decimal128Array a, int index)
    {
        var type = (Decimal128Type)a.Data.DataType;
        if (type.Precision <= MaxExactDecimalDigits && type.Scale <= MaxExactDecimalDigits)
            return LiteralValue.Of(a.GetValue(index)!.Value);

        // ToBigInteger is a call, and therefore a point at which `a` — whose last use is the span
        // it is being handed — could otherwise be collected out from under that span.
        // See doc/arrow-span-lifetime.md.
        var literal = LiteralValue.HighPrecisionDecimalOf(
            ToBigInteger(a.ValueBuffer.Span.Slice(index * 16, 16)), type.Scale);
        GC.KeepAlive(a);
        return literal;
    }

    private static LiteralValue DecimalLiteral(Decimal256Array a, int index)
    {
        var type = (Decimal256Type)a.Data.DataType;
        if (type.Precision <= MaxExactDecimalDigits && type.Scale <= MaxExactDecimalDigits)
            return LiteralValue.Of(a.GetValue(index)!.Value);

        // See the Decimal128 overload above, and doc/arrow-span-lifetime.md.
        var literal = LiteralValue.HighPrecisionDecimalOf(
            ToBigInteger(a.ValueBuffer.Span.Slice(index * 32, 32)), type.Scale);
        GC.KeepAlive(a);
        return literal;
    }

    private static BigInteger ToBigInteger(ReadOnlySpan<byte> littleEndianTwosComplement)
    {
#if NET6_0_OR_GREATER
        return new BigInteger(littleEndianTwosComplement, isUnsigned: false, isBigEndian: false);
#else
        return new BigInteger(littleEndianTwosComplement.ToArray());
#endif
    }
}

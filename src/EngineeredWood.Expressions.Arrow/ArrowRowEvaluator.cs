// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using ArrowCompute = EngineeredWood.Arrow.ArrowCompute;

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

    /// <summary>
    /// The registry's short-circuiting functions, when it has any. See
    /// <see cref="IShortCircuitingFunctions"/>.
    /// </summary>
    private readonly IShortCircuitingFunctions? _shortCircuiting;

    /// <summary>The registry's nullability rules, when it has any. See <see cref="NeverNull"/>.</summary>
    private readonly INullabilityRules? _nullability;

    /// <summary>
    /// The registry's rule for narrowing an integral literal met with a decimal, when it has one.
    /// See <see cref="PickMinimumPrecision"/>.
    /// </summary>
    private readonly ILiteralPrecisionRules? _literalPrecision;

    /// <summary>
    /// The registry's analyzer, when it has one. See <see cref="CheckComparable"/>.
    /// </summary>
    private readonly IAnalysisRules? _analysis;

    /// <summary>
    /// The registry's rule for a call every argument of which is constant, when it has one. See
    /// <see cref="IsConstant"/>.
    /// </summary>
    private readonly IConstantFoldedFunctions? _constantFolded;

    public ArrowRowEvaluator(IFunctionRegistry? functions = null)
    {
        _functions = functions;
        _coercion = functions as IComparisonCoercion;
        _shortCircuiting = functions as IShortCircuitingFunctions;
        _nullability = functions as INullabilityRules;
        _literalPrecision = functions as ILiteralPrecisionRules;
        _analysis = functions as IAnalysisRules;
        _constantFolded = functions as IConstantFoldedFunctions;
    }

    public BooleanArray EvaluatePredicate(Predicate predicate, RecordBatch batch)
    {
        var result = EvalPredicate(predicate, batch);
        return ToBooleanArray(result, batch.Length);
    }

    public IArrowArray EvaluateExpression(Expression expression, RecordBatch batch)
    {
        var result = EvalExpressionAsArray(expression, batch);

        // A literal is built at least one row long even over an empty batch (see ConstantArray),
        // so trim the answer back to the batch's length.
        return result.Length == batch.Length
            ? result
            : ArrowArrayFactory.Slice(result, 0, batch.Length);
    }

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

    /// <summary>
    /// SQL three-valued AND, evaluating each operand only over the rows the ones before it left
    /// undecided.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Short-circuiting is per row, because Spark's is: its <c>And</c> skips the right operand
    /// once the left is false, so an error on a row the left already decided never happens. Over
    /// <c>z = [0, 1, 2]</c>, <c>z &lt;&gt; 0 AND 1/z &gt; 0</c> answers
    /// <c>[false, true, true]</c> rather than raising DIVIDE_BY_ZERO.
    /// </para>
    /// <para>
    /// NULL does not short-circuit: <c>n &gt; 0 AND 1/z &gt; 0</c> over an all-null <c>n</c>
    /// raises on the row where <c>z</c> is zero. So a row whose answer so far is null stays live.
    /// </para>
    /// <para>
    /// Every operand is evaluated even once no row is live, over an empty selection. That reads no
    /// value and so cannot raise, and it keeps an operand that cannot be evaluated at all -- an
    /// unresolvable column, a pair of types with no comparison -- raising rather than being
    /// quietly skipped.
    /// </para>
    /// </remarks>
    private bool?[] EvalAnd(AndPredicate and, RecordBatch batch)
    {
        var result = new bool?[batch.Length];
        for (int i = 0; i < result.Length; i++) result[i] = true;

        // The rows whose answer is not yet false, which are exactly the rows Spark would still be
        // evaluating operands for.
        var live = new bool[batch.Length];
        for (int i = 0; i < live.Length; i++) live[i] = true;

        foreach (var child in and.Children)
        {
            var childResult = EvalPredicateOver(child, batch, live);
            for (int i = 0; i < result.Length; i++)
            {
                if (!live[i])
                    continue;

                if (childResult[i] == false)
                {
                    result[i] = false;
                    live[i] = false;
                }
                else if (childResult[i] is null)
                {
                    // Unknown so far, but a later operand can still make it false, so the row
                    // stays live.
                    result[i] = null;
                }

                // else true, which leaves the accumulated answer -- true or null -- as it was.
            }
        }

        return result;
    }

    /// <summary>
    /// SQL three-valued OR, the mirror of <see cref="EvalAnd"/>: an operand is evaluated only over
    /// the rows no earlier one made true.
    /// </summary>
    /// <remarks>
    /// <c>z = 0 OR 1/z &gt; 0</c> answers <c>[true, true, true]</c> over <c>z = [0, 1, 2]</c>.
    /// NULL does not short-circuit here either: <c>n &gt; 0 OR 1/z &gt; 0</c> raises.
    /// </remarks>
    private bool?[] EvalOr(OrPredicate or, RecordBatch batch)
    {
        var result = new bool?[batch.Length];
        for (int i = 0; i < result.Length; i++) result[i] = false;

        var live = new bool[batch.Length];
        for (int i = 0; i < live.Length; i++) live[i] = true;

        foreach (var child in or.Children)
        {
            var childResult = EvalPredicateOver(child, batch, live);
            for (int i = 0; i < result.Length; i++)
            {
                if (!live[i])
                    continue;

                if (childResult[i] == true)
                {
                    result[i] = true;
                    live[i] = false;
                }
                else if (childResult[i] is null)
                {
                    result[i] = null;
                }
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
        // Asked before either operand is evaluated, so that an operand which raises cannot
        // decide the analysis: `CAST(s AS INT) = bl` over a row holding 'abc' would otherwise
        // report CAST_INVALID_INPUT and never reach the refusal.
        bool asked = CheckComparableFromTree(cmp, batch);

        var (left, leftType) = EvalOperand(cmp.Left, batch);
        var (right, rightType) = EvalOperand(cmp.Right, batch);
        if (!asked)
            CheckComparable(cmp.Op, leftType, left, rightType, right);
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
                // A pair with no comparison between them -- e.g. a boolean against a number the
                // registry did not coerce, or any mixed pair when no registry was supplied. A
                // string against a number, boolean, instant or binary is cast before the loop.
                result[i] = null;
            }
        }
        return result;
    }

    /// <summary>
    /// Asks about a comparison whose operand types can both be read without evaluating either
    /// operand, and reports whether it asked.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is what makes a refusal independent of the data; <see cref="CheckComparable"/> is
    /// only a fallback. An analysis refusal is a property of two types, so it must not be
    /// displaced by an operand that happens to raise first. Reading both types before either
    /// operand runs matches Spark, whose analyzer refuses before the plan executes.
    /// </para>
    /// <para>
    /// False when the types could not both be read, which leaves the question to the fallback. A
    /// bare <c>NULL</c> operand answers false here and is not asked about there either; see
    /// <see cref="AnalysisType"/> for what can be read and what cannot.
    /// </para>
    /// </remarks>
    private bool CheckComparableFromTree(ComparisonPredicate cmp, RecordBatch batch)
    {
        if (_analysis is null)
            return false;

        var left = AnalysisType(cmp.Left, batch);
        var right = AnalysisType(cmp.Right, batch);
        if (left is null || right is null)
            return false;

        Refuse(_analysis.CheckComparison(cmp.Op, left, right));
        return true;
    }

    /// <summary>
    /// Asks about a set test whose operand and members can all be typed without evaluating any of
    /// them, and reports whether it asked.
    /// </summary>
    /// <remarks>
    /// <para>
    /// One question over the whole list, not one per member. <c>IN</c> resolves a single type over
    /// the operand and every member, so a list can be refused although each pair in it would be
    /// accepted: <c>a = bl</c> answers under the legacy dialect and <c>a IN (bl)</c> is refused
    /// under both. See <see cref="IAnalysisRules.CheckSetComparison"/>.
    /// </para>
    /// <para>
    /// All or nothing: a member that cannot be typed from the tree leaves the whole list to the
    /// fallback, because a list asked about with one of its members missing is a different list.
    /// A bare <c>NULL</c> is not missing, though — it is left out, since Spark types one
    /// <c>void</c> and a void constrains nothing. That applies to the operand too:
    /// <c>NULL IN (1, TRUE)</c> is refused in both dialects, because the members must still agree
    /// with each other.
    /// </para>
    /// </remarks>
    private bool CheckSetFromTree(SetPredicate set, RecordBatch batch)
    {
        if (_analysis is null)
            return false;

        var types = new List<IArrowType>(set.Values.Count + 1);

        if (!IsNullLiteral(set.Operand))
        {
            var operandType = AnalysisType(set.Operand, batch);
            if (operandType is null)
                return false;

            types.Add(operandType);
        }

        foreach (var member in set.Values)
        {
            if (IsNullLiteral(member))
                continue;

            var memberType = AnalysisType(member, batch);
            if (memberType is null)
                return false;

            types.Add(memberType);
        }

        Refuse(_analysis.CheckSetComparison(types));
        return true;
    }

    /// <summary>Whether an expression is a bare <c>NULL</c> literal, structurally.</summary>
    /// <remarks>
    /// Read from the tree rather than from a column that came back all null (as in
    /// <see cref="IConditionalArguments.IsNullLiteral"/>): a string column holding nothing in this
    /// batch is not a void, and treating it as one retypes the answer.
    /// </remarks>
    private static bool IsNullLiteral(Expression expression) =>
        expression is LiteralExpression literal && literal.Value.IsNull;

    /// <summary>
    /// The type an expression produces, read without reading any of its values, or null when that
    /// cannot be done.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Three tiers, cheapest first. A reference takes its type from the batch's schema, which
    /// holds whether or not the batch has rows. A literal takes its own type from the tree, so an
    /// expression made only of literals is still analysed when there is no data (as in a
    /// definition-time pass). Anything else is evaluated over no rows, as
    /// <see cref="TypeOver"/> does: over an empty selection a cast reads no value, so it produces
    /// its type without being able to raise on one.
    /// </para>
    /// <para>
    /// Unlike <see cref="TypeOver"/>, this does not retry over the batch: a registry that cannot
    /// type something over an empty selection leaves the comparison to the fallback, and retrying
    /// over rows would read the values this exists not to read.
    /// </para>
    /// <para>
    /// A bare <c>NULL</c> literal answers null rather than a type, because Spark types one
    /// <c>void</c> and compares it with anything. See <see cref="CheckComparable"/> for the
    /// related trap on the value side.
    /// </para>
    /// </remarks>
    private IArrowType? AnalysisType(Expression expression, RecordBatch batch)
    {
        switch (expression)
        {
            case UnboundReference u:
                return GetColumn(batch, u.Name).Data.DataType;

            case BoundReference b:
                return GetColumn(batch, b.Name).Data.DataType;

            case LiteralExpression literal:
                return literal.Value.IsNull
                    ? null
                    : ConstantArray(literal.Value, 1).Data.DataType;
        }

        try
        {
            return EvalExpressionAsArray(expression, Restrict(expression, batch, NoRows))
                .Data.DataType;
        }
        catch (ExpressionAnalysisException)
        {
            // A refusal from further down the tree is an answer, not a failure to produce one.
            // Swallowing it would hide a refused sub-expression behind the fallback.
            throw;
        }
        catch (Exception)
        {
            // Deliberately broad, as in TypeOver: the ways of failing to produce a type over no
            // rows are the registry's business.
            return null;
        }
    }

    /// <summary>
    /// Refuses a comparison the registry's analyzer refuses, where the operand types could only be
    /// read from the values.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The fallback behind <see cref="CheckComparableFromTree"/>, reached only where that could
    /// not read a type without evaluating — a registry whose function cannot be typed over an
    /// empty selection. Being asked late, it is never reached when an operand raises during
    /// evaluation. A no-op without a registry that implements <see cref="IAnalysisRules"/>.
    /// </para>
    /// <para>
    /// An operand whose type cannot be read is not asked about, rather than being given
    /// <see cref="OperandType"/>'s string fallback. That fallback suits coercion, where a string
    /// has no rule and is left as it stands, but here it would present a bare <c>NULL</c> as a
    /// string, and <c>bin = NULL</c> would be refused as a binary against a string where Spark
    /// types the NULL <c>void</c> and compares it with anything.
    /// </para>
    /// </remarks>
    private void CheckComparable(
        ComparisonOperator op,
        IArrowType? leftType, LiteralValue?[] left,
        IArrowType? rightType, LiteralValue?[] right)
    {
        if (_analysis is null)
            return;

        var l = ReadType(leftType, left);
        var r = ReadType(rightType, right);
        if (l is null || r is null)
            return;

        Refuse(_analysis.CheckComparison(op, l, r));
    }

    /// <summary>
    /// Refuses a set test whose operand cannot be compared against one of its members.
    /// </summary>
    /// <remarks>
    /// The fallback behind <see cref="CheckSetFromTree"/>, reached only when that could not type
    /// the operand or one of the members without evaluating it. Asks the same single question
    /// over the same list; see <see cref="IAnalysisRules.CheckSetComparison"/> for why it is one
    /// question and not one per member.
    /// </remarks>
    private void CheckSetMembers(
        IArrowType? operandType, LiteralValue?[] operand,
        SetMember[] members, IArrowType?[] memberTypes, bool askedFromTree)
    {
        if (_analysis is null || askedFromTree)
            return;

        // An operand with no readable type is the value-side spelling of a bare NULL, and is left
        // out rather than abandoning the check -- see CheckSetFromTree.
        var types = new List<IArrowType>(members.Length + 1);
        if (ReadType(operandType, operand) is { } type)
            types.Add(type);
        for (var k = 0; k < members.Length; k++)
        {
            // A member null in every row with no declared type is the value-side spelling of a
            // bare NULL literal, and is left out for the same reason.
            var memberType = memberTypes[k]
                ?? (members[k].IsConstant
                    ? members[k].Constant is { } constant
                        ? ConstantArray(constant, 1).Data.DataType
                        : null
                    : ReadType(null, members[k].PerRow));

            if (memberType is null)
                continue;

            types.Add(memberType);
        }

        Refuse(_analysis.CheckSetComparison(types));
    }

    /// <summary>
    /// The type an operand carries, or null when every row is null and no type is declared.
    /// </summary>
    /// <remarks>
    /// Not <see cref="OperandType"/>, which substitutes a string where this declines to answer;
    /// see <see cref="CheckComparable"/> for why.
    /// </remarks>
    private static IArrowType? ReadType(IArrowType? declared, LiteralValue?[] values)
    {
        if (declared is not null)
            return declared;

        foreach (var value in values)
        {
            if (value.HasValue)
                return ConstantArray(value.Value, 1).Data.DataType;
        }

        return null;
    }

    /// <summary>Raises a diagnostic the analyzer returned; a null one is an acceptance.</summary>
    private static void Refuse(AnalysisDiagnostic? diagnostic)
    {
        if (diagnostic is not null)
            throw new ExpressionAnalysisException(diagnostic);
    }

    /// <summary>
    /// Casts the operand and every member of a set test to the one type they compare through.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>IN</c> is not the disjunction of equalities it resembles. Spark resolves one type over
    /// the operand and the whole list, so <c>a IN ('01')</c> is false under the legacy dialect —
    /// the list resolves to text, and <c>'1'</c> is not <c>'01'</c> — while <c>a = '01'</c> is
    /// true. Which type is the registry's answer; see <see cref="IComparisonCoercion"/>.
    /// </para>
    /// <para>
    /// The kinds are checked before any type is resolved, because resolving one allocates and
    /// the overwhelmingly common set — a column against literals of its own type — needs none.
    /// </para>
    /// </remarks>
    private void CoerceSet(
        ref LiteralValue?[] operand, IArrowType? operandType,
        SetMember[] members, IArrowType?[] memberTypes, int rowCount)
    {
        if (_coercion is null)
            return;

        bool anyString = IsString(operandType, operand);
        bool anyOther = !anyString && (operandType is not null || FirstKind(operand) is not null);
        bool anyDecimal = MightRound(operandType, operand);

        for (var k = 0; k < members.Length; k++)
        {
            if (IsMemberString(members[k], memberTypes[k])) anyString = true;
            else if (MemberIsTyped(members[k], memberTypes[k])) anyOther = true;

            if (MemberMightRound(members[k], memberTypes[k])) anyDecimal = true;
        }

        // A string mixed with anything else takes the promotion rule; a set with no string at
        // all takes it only when a decimal is present, because that is the one case where the
        // type the set resolves through can round a member away. Everything else is one kind
        // throughout, or exact, and is compared as it stands.
        if (anyString ? !anyOther : !anyDecimal)
            return;

        // Resolved once and reused, because the type decides two things: which target the set
        // takes, and how each member is rebuilt as an Arrow array. Inferring the second from
        // values instead cannot build a decimal at all and reads a date as an instant.
        var operandResolved = OperandType(operandType, operand);
        var resolved = new IArrowType[members.Length];

        // A bare NULL member constrains nothing and is left out of the resolution. Spark types it
        // `void`, but `MemberType` calls it a string for want of a value, which would make
        // `d IN (wide, NULL)` look like a set with a string in it. Spark answers true for
        // `CAST(1.005 AS DECIMAL(4,3)) IN (CAST(1 AS DECIMAL(38,0)), NULL)`, the same rounding
        // match it gives without the NULL. It is not cast either: a null is null at every type.
        var untypedNull = new bool[members.Length];
        var types = new List<IArrowType>(members.Length + 1) { operandResolved };
        for (var k = 0; k < members.Length; k++)
        {
            untypedNull[k] = members[k].IsConstant
                && !members[k].Constant.HasValue
                && memberTypes[k] is null;

            resolved[k] = MemberType(members[k], memberTypes[k]);
            if (!untypedNull[k])
                types.Add(resolved[k]);
        }

        var target = _coercion.SetComparisonTarget(types);
        if (target is null)
            return;   // a set the registry has no rule for; compare it as it stands

        operand = CastMember(operand, operandResolved, target, rowCount);
        for (var k = 0; k < members.Length; k++)
        {
            if (untypedNull[k])
                continue;

            // A constant is cast as a one-row array, not as one row per row of the batch.
            members[k] = members[k].IsConstant
                ? new SetMember(CastMember(
                    new[] { members[k].Constant }, resolved[k], target, 1)[0])
                : new SetMember(CastMember(members[k].PerRow, resolved[k], target, rowCount));
        }
    }

    /// <summary>Whether a set member could make the set's common type round a value away.</summary>
    private static bool MemberMightRound(in SetMember member, IArrowType? declared) =>
        member.IsConstant
            ? member.Constant?.Type
                is LiteralValue.Kind.Decimal or LiteralValue.Kind.HighPrecisionDecimal or LiteralValue.Kind.Float
            : MightRound(declared, member.PerRow);

    /// <summary>Whether a set member is a string, from its declared type or its value.</summary>
    private static bool IsMemberString(in SetMember member, IArrowType? declared) =>
        member.IsConstant
            ? member.Constant?.Type == LiteralValue.Kind.String
            : IsString(declared, member.PerRow);

    /// <summary>Whether a member brings a type to the resolution at all.</summary>
    /// <remarks>
    /// A declared type answers on its own — a column has one whether or not any row is populated
    /// — so the values are read only for a member that has none, and a typed member is never
    /// scanned. What is left with nothing either way is a null literal, which says nothing about
    /// what the set resolves through.
    /// </remarks>
    private static bool MemberIsTyped(in SetMember member, IArrowType? declared) =>
        member.IsConstant
            ? member.Constant.HasValue
            : declared is not null || FirstKind(member.PerRow) is not null;

    /// <summary>The Arrow type a set member resolves through.</summary>
    private static IArrowType MemberType(in SetMember member, IArrowType? declared) =>
        member.IsConstant
            ? (member.Constant is { } value
                ? ConstantArray(value, 1).Data.DataType
                : StringType.Default)
            : OperandType(declared, member.PerRow);

    /// <summary>Rebuilds one member of a set as <paramref name="target"/>.</summary>
    private LiteralValue?[] CastMember(
        LiteralValue?[] values, IArrowType type, IArrowType target, int rowCount) =>
        ArrowToLiteralValues(
            _coercion!.CastForComparison(
                MaterializeAsArray(values, rowCount, type), target, rowCount),
            rowCount);

    /// <summary>
    /// One member of a set test: a constant, or a value per row.
    /// </summary>
    /// <remarks>
    /// The distinction is worth a type because a literal list is the common one. Evaluating a
    /// member through the ordinary expression path would repeat every literal across the batch,
    /// so `x IN (1, 2)` over a hundred thousand rows would allocate two hundred thousand cells
    /// for two values that never change.
    /// </remarks>
    private readonly struct SetMember
    {
        private readonly LiteralValue?[]? _perRow;

        public SetMember(LiteralValue? constant)
        {
            _perRow = null;
            Constant = constant;
        }

        public SetMember(LiteralValue?[] perRow)
        {
            _perRow = perRow;
            Constant = null;
        }

        public bool IsConstant => _perRow is null;

        /// <summary>The value, when this member is a constant.</summary>
        public LiteralValue? Constant { get; }

        /// <summary>The values, when this member varies by row.</summary>
        public LiteralValue?[] PerRow => _perRow!;

        public LiteralValue? At(int row) => _perRow is null ? Constant : _perRow[row];
    }

    /// <summary>
    /// Casts whichever operand has to move before the two can be compared, in place.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Spark resolves a string against a non-string by casting — it does not refuse, and it does
    /// not always cast the string: against a binary, the binary is rendered as text and two
    /// strings are compared. <see cref="LiteralValue.CompareTo(LiteralValue)"/> has no cross-kind
    /// branch for a string, so without this such a comparison answers null.
    /// </para>
    /// <para>
    /// The target is dialect-dependent, so the registry chooses it — see
    /// <see cref="IComparisonCoercion"/>. Without a registry there is nothing to cast with and
    /// the comparison is left as it was.
    /// </para>
    /// <para>
    /// A row whose other operand is null is not cast, except under <c>&lt;=&gt;</c>. Spark's
    /// relational operators evaluate nothing once an operand is null, so a malformed string
    /// opposite a null is never read and never refused; null-safe equality has no such
    /// short-circuit. Over a row of <c>(a = NULL, s = 'abc')</c>, <c>s = a</c> is null under
    /// ANSI in both operand orders, while <c>s &lt;=&gt; a</c> raises CAST_INVALID_INPUT.
    /// </para>
    /// </remarks>
    private void CoerceOperands(
        ComparisonPredicate cmp, IArrowType? leftType, IArrowType? rightType,
        ref LiteralValue?[] left, ref LiteralValue?[] right)
    {
        if (_coercion is null)
            return;

        // One cheap test picks the rule: the string rules below need a string on exactly one
        // side, and the numeric one needs a string on neither. Typing an operand can allocate,
        // so neither path resolves a type until it knows it is the path being taken.
        bool leftIsString = IsString(leftType, left);
        bool rightIsString = IsString(rightType, right);

        if (leftIsString && rightIsString)
            return;   // two strings: compared as text, nothing to coerce

        if (!leftIsString && !rightIsString)
        {
            // A boolean against a non-boolean is the other pair where a single operand moves, and
            // the registry decides whether it does: under the legacy dialect Spark casts the
            // boolean to the numeric opposite it, and under ANSI it refuses the comparison. The
            // cheap kind test comes first, as for strings.
            bool leftIsBoolean = IsBoolean(leftType, left);
            if (leftIsBoolean != IsBoolean(rightType, right)
                && CoerceOneSide(cmp, leftIsBoolean, leftType, rightType, ref left, ref right))
            {
                return;
            }

            CoerceExactNumerics(cmp, leftType, rightType, ref left, ref right);
            return;
        }

        CoerceOneSide(cmp, leftIsString, leftType, rightType, ref left, ref right);
    }

    /// <summary>
    /// Casts whichever of two operands the registry says moves, and reports whether one did.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Which operand moves is the registry's answer: a string against a number is cast to the
    /// number, while a string against a binary stays and the binary is rendered as text. At most
    /// one side moves, so the second question is only asked when the first declines.
    /// </para>
    /// <para>
    /// <paramref name="leftIsCandidate"/> says which side to offer first, not which one moves:
    /// the operand whose kind selected this path (the string or the boolean), so a registry can
    /// answer the common case in one call.
    /// </para>
    /// </remarks>
    private bool CoerceOneSide(
        ComparisonPredicate cmp, bool leftIsCandidate,
        IArrowType? leftType, IArrowType? rightType,
        ref LiteralValue?[] left, ref LiteralValue?[] right)
    {
        var candidate = leftIsCandidate ? left : right;
        var other = leftIsCandidate ? right : left;
        var candidateType = OperandType(leftIsCandidate ? leftType : rightType, candidate);
        var otherType = OperandType(leftIsCandidate ? rightType : leftType, other);

        bool castTheCandidate = true;
        var target = _coercion!.ComparisonTarget(cmp.Op, candidateType, otherType);
        if (target is null)
        {
            castTheCandidate = false;
            target = _coercion.ComparisonTarget(cmp.Op, otherType, candidateType);
        }

        if (target is null)
            return false;   // a pair the registry has no rule for; compare it as it stands

        var moving = castTheCandidate ? candidate : other;
        var staying = castTheCandidate ? other : candidate;
        int rowCount = moving.Length;

        var coerced = _coercion.CastForComparison(
            MaterializeAsArray(
                cmp.Op == ComparisonOperator.NullSafeEqual ? moving : NulledWhere(moving, staying),
                rowCount),
            target,
            rowCount);

        var values = ArrowToLiteralValues(coerced, rowCount);
        if (leftIsCandidate == castTheCandidate) left = values;
        else right = values;
        return true;
    }

    /// <summary>
    /// Rounds two numeric operands to the type they compare through, where that type can round a
    /// value away.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The one comparison rule where both operands can move, so it does not reuse
    /// <see cref="CoerceOneSide"/>. Spark compares a decimal against a decimal by casting each to
    /// their least common type, and that type gives up scale once the natural precision passes
    /// 38, so the comparison is made on rounded values:
    /// <c>CAST(1.005 AS DECIMAL(4,3)) = CAST(1 AS DECIMAL(38,0))</c> is true. A float can round
    /// an integral too; see <see cref="MightRound"/>.
    /// </para>
    /// <para>
    /// In every ordinary case the registry answers null for both operands — the common type keeps
    /// their scales — so nothing is cast. The types still have to be resolved to ask, which is
    /// why the tests that choose a path are kind tests rather than type tests.
    /// </para>
    /// </remarks>
    private void CoerceExactNumerics(
        ComparisonPredicate cmp,
        IArrowType? leftType, IArrowType? rightType,
        ref LiteralValue?[] left, ref LiteralValue?[] right)
    {
        // An operand untyped and null in every row (a bare NULL literal) has no value to round.
        // A decimal on one side is what the minimum-precision rule below needs too, so nothing
        // it applies to is skipped here.
        if (!MightRound(leftType, left) && !MightRound(rightType, right))
            return;

        var resolvedLeft = OperandType(leftType, left);
        var resolvedRight = OperandType(rightType, right);

        // An integral literal against a decimal is read as the narrowest decimal holding its
        // value, not its type. Only the resolved type moves -- a scale-0 operand never rounds --
        // so what changes is the common type the other operand rounds to:
        // `CAST(4E-32 AS DECIMAL(38,38)) = 0` compares at decimal(38,37) and is false, while
        // `= CAST(0 AS INT)` compares at decimal(38,28) and is true. See PickMinimumPrecision for
        // the same rule in arithmetic and for the calls that do not take it -- an IN list among
        // them: `CAST(4E-32 AS DECIMAL(38,38)) IN (0)` is true.
        var minimumLeft = MinimumPrecision(cmp.Left, resolvedRight);
        var minimumRight = MinimumPrecision(cmp.Right, resolvedLeft);
        resolvedLeft = minimumLeft ?? resolvedLeft;
        resolvedRight = minimumRight ?? resolvedRight;

        var leftTarget = _coercion!.ComparisonTarget(cmp.Op, resolvedLeft, resolvedRight);
        var rightTarget = _coercion.ComparisonTarget(cmp.Op, resolvedRight, resolvedLeft);
        if (leftTarget is null && rightTarget is null)
            return;

        // No null mask as on the string path: this cast cannot fail, so there is no refusal for
        // a null on the other side to suppress. The common type leaves max(p1 - s1, p2 - s2)
        // integer digits, and an operand that rounds always has strictly fewer: rounding needs
        // its scale to be the wider one, and if its integer digits were also the widest the
        // natural precision would be its own and never pass 38. That spare digit absorbs a
        // carry: decimal(38,2) at 36 nines rounds to 10^36 against a decimal(38,0).
        int rowCount = left.Length;
        if (leftTarget is not null)
            left = Rounded(left, resolvedLeft, leftTarget, rowCount);
        if (rightTarget is not null)
            right = Rounded(right, resolvedRight, rightTarget, rowCount);
    }

    /// <summary>
    /// The decimal an integral literal compares as, or null when the rule does not apply.
    /// </summary>
    /// <remarks>
    /// Guarded on the other operand being a decimal, which is Spark's own guard: the cast is
    /// inserted only where the literal meets one, so <c>a = 2</c> stays an integral comparison.
    /// Asked of each side in turn — <c>1.5BD = 2</c> and <c>2 = 1.5BD</c> both resolve through
    /// decimal(2,1) — and at most one side can answer, since the other must already be a decimal.
    /// </remarks>
    private IArrowType? MinimumPrecision(Expression operand, IArrowType other)
    {
        if (_literalPrecision is null || IntegralLiteral(operand) is not long value)
            return null;

        return _literalPrecision.LiteralComparisonType(value, other);
    }

    /// <summary>Whether an operand could make a comparison's common type round a value away.</summary>
    /// <remarks>
    /// <para>
    /// A decimal can, because the common type gives up scale. So can a float: the legacy dialect
    /// compares an integral with one as a float, rounding the integral onto it, where ANSI
    /// compares both as doubles. Which applies is the registry's decision; this only decides
    /// whether to ask.
    /// </para>
    /// <para>
    /// Asked before any type is resolved, from the declared type when there is one and from the
    /// first populated value otherwise, so a comparison of two <c>int</c> columns costs two field
    /// reads.
    /// </para>
    /// </remarks>
    private static bool MightRound(IArrowType? declared, LiteralValue?[] values) =>
        declared is not null
            ? declared is Decimal128Type or Decimal256Type or FloatType
            : FirstKind(values)
                is LiteralValue.Kind.Decimal or LiteralValue.Kind.HighPrecisionDecimal or LiteralValue.Kind.Float;

    private LiteralValue?[] Rounded(
        LiteralValue?[] values, IArrowType type, IArrowType target, int rowCount) =>
        ArrowToLiteralValues(
            _coercion!.CastForComparison(
                MaterializeAsArray(values, rowCount, type), target, rowCount),
            rowCount);

    /// <summary>
    /// Evaluates a comparison operand, keeping the Arrow type when the operand declares one.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The same calls <see cref="EvalExpression"/> makes for these three cases, with the array
    /// kept rather than discarded. The type is what a <see cref="LiteralValue"/> array cannot
    /// carry and what the coercion turns on: a decimal's precision and scale, a date against a
    /// timestamp, and the type of an operand whose every row is null. A cast's result carries
    /// exactly the type asked for: <c>'2026-08-11 12:30:00' = CAST(ts AS DATE)</c> is true in
    /// Spark, and reading that operand as an instant would compare the string against midnight
    /// instead of truncating it.
    /// </para>
    /// <para>
    /// A literal is not routed through an array. It is typed from its own value, as Spark does,
    /// and materialising a constant array per comparison would allocate on the hot path.
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
    /// The declared type answers even for an all-null operand, where the values cannot. An
    /// all-null string casts to null under every target and so cannot change an answer, but an
    /// all-null operand on the other side still types the cast, and <c>&lt;=&gt;</c> reads it:
    /// <c>s &lt;=&gt; CAST(NULL AS INT)</c> raises under ANSI rather than answering false.
    /// </remarks>
    private static bool IsString(IArrowType? declared, LiteralValue?[] values) =>
        declared is not null
            ? declared is StringType
            : FirstKind(values) == LiteralValue.Kind.String;

    /// <summary>Whether an operand is a boolean, without resolving its type.</summary>
    /// <remarks>
    /// The <see cref="IsString"/> shape, for the same reason: it picks the coercion path without
    /// the allocation of resolving a type. An operand null in every row with no declared type
    /// answers false.
    /// </remarks>
    private static bool IsBoolean(IArrowType? declared, LiteralValue?[] values) =>
        declared is not null
            ? declared is BooleanType
            : FirstKind(values) == LiteralValue.Kind.Boolean;

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
        // An operand that can never be null is not evaluated, so an error inside it never
        // happens. Spark's `NullPropagation` rewrites the whole predicate to a constant, so under
        // ANSI `CASE WHEN (CAST('abc' AS INT) > 1) THEN 1 ELSE 2 END IS NULL` answers false while
        // the same CASE on its own raises CAST_INVALID_INPUT.
        //
        // This is independent of per-row short-circuiting: it fires before a row is read, on an
        // operand every row of which would be evaluated. Short-circuiting answers
        // `coalesce(1, CAST('abc' AS INT)) IS NULL`, but only this reaches
        // `(2147483647 + 1) IS NULL`.
        if ((unary.Op is UnaryOperator.IsNull or UnaryOperator.IsNotNull)
            && NeverNull(unary.Operand))
        {
            // Typed and discarded, as an unreached branch is. Spark analyses the operand before
            // the optimizer folds it, so `if(a > 0, a, bin) IS NOT NULL` is still refused for
            // having no common type; over no rows this cannot raise on a value.
            TypeOver(unary.Operand, batch);
            return Constant(unary.Op == UnaryOperator.IsNotNull, batch.Length);
        }

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

    /// <summary>
    /// Whether <paramref name="expression"/> can never produce a null, whatever the data.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Spark's <c>Expression.nullable</c>, for the subset that can be answered without types.
    /// Only <see cref="EvalUnary"/> asks, and only so that it can skip an operand Spark skips;
    /// the judgement never changes a value.
    /// </para>
    /// <para>
    /// A column reference is always nullable here, even one whose field says otherwise. Spark
    /// reads nullability from the table's schema, but the only schema visible here is the batch
    /// the caller built — <c>DeltaConstraintEnforcer</c> is handed the caller's rows, not the
    /// snapshot's. Trusting a batch that declares non-nullable a field the table declares
    /// nullable would make <c>x IS NOT NULL</c> a constant <c>true</c> and admit a row Spark
    /// rejects. The cost is only that <c>(a + b) IS NOT NULL</c> over two NOT NULL columns can
    /// raise where Spark answers — the safe direction.
    /// </para>
    /// <para>
    /// Everything unrecognised is nullable: under-claiming costs a fold, over-claiming yields a
    /// wrong answer. So this is an allow-list of shapes measured against Spark, and a shape not
    /// on it — <c>IS NAN</c>, a cast, <c>nullif</c>, <c>round</c>, a division, <c>IN</c>, and
    /// every comparison but <c>&lt;=&gt;</c> — answers false.
    /// </para>
    /// <para>
    /// The rule is structural: it never reads a type, so any shape whose nullability depends on
    /// one is off the list. That excludes the arithmetic and comparison families, where an
    /// implicit cast can appear between operands whose types this cannot see.
    /// </para>
    /// </remarks>
    private bool NeverNull(Expression expression)
    {
        switch (expression)
        {
            case LiteralExpression literal:
                return !literal.Value.IsNull;

            case TruePredicate or FalsePredicate:
                return true;

            // `CAST(s AS INT) IS NULL` is itself non-nullable, so a doubled `IS NULL` folds where
            // the inner one alone raises. IS NAN is absent: Spark answers false for a null
            // operand where this evaluator answers null.
            case UnaryPredicate unary:
                return unary.Op is UnaryOperator.IsNull or UnaryOperator.IsNotNull;

            case NotPredicate not:
                return NeverNull(not.Child);

            case AndPredicate and:
                return AllNeverNull(and.Children);

            case OrPredicate or:
                return AllNeverNull(or.Children);

            // Only `<=>`; the rest of the comparison family is absent for the same reason
            // arithmetic is. A comparison that needs coercion inserts a cast, and a cast is
            // nullable: in both dialects `1 = 1` and `'a' = 'b'` are non-nullable while
            // `'abc' = 1` and `'abc' > 1` are nullable. No rule phrased in terms of the operands'
            // nullability can separate them, and the operands' types are not known yet. `IN` is
            // worse still: `'abc' IN (1)` is nullable under ANSI and non-nullable under legacy.
            //
            // Nothing is lost by leaving them out. A comparison could fold only when both operands
            // are non-nullable, and an operand that could raise is a cast or an arithmetic node,
            // which is nullable anyway -- so the shapes given up (`(1 = 1) IS NULL`) evaluate to
            // the same answer without the fold.
            //
            // `<=>` stays because its non-nullability is structural: it answers for a null pair,
            // coercion or not. `('abc' <=> 1) IS NULL` is false in both dialects, where the `=`
            // spelling raises under ANSI.
            case ComparisonPredicate comparison:
                return comparison.Op == ComparisonOperator.NullSafeEqual;

            case FunctionCall call:
                return NeverNullCall(call);

            // A column reference, and anything else this does not recognise.
            default:
                return false;
        }
    }

    private bool AllNeverNull(IReadOnlyList<Expression> expressions)
    {
        for (var i = 0; i < expressions.Count; i++)
        {
            if (!NeverNull(expressions[i]))
                return false;
        }

        return true;
    }

    private bool NeverNullCall(FunctionCall call)
    {
        if (_nullability is null)
            return false;

        var arguments = new bool[call.Arguments.Count];
        for (var i = 0; i < arguments.Length; i++)
            arguments[i] = NeverNull(call.Arguments[i]);

        return _nullability.NeverNull(call.Name, arguments);
    }

    private bool?[] EvalSet(SetPredicate set, RecordBatch batch)
    {
        // Before any member is evaluated, for the reason EvalComparison gives: a member that
        // raises must not displace the refusal of a member whose type is already wrong.
        bool askedFromTree = CheckSetFromTree(set, batch);

        var (operand, operandType) = EvalOperand(set.Operand, batch);

        // A member is an expression, so `x IN (a, b)` compares row i of x against row i of a and
        // of b. A literal member stays a single value rather than being repeated per row (see
        // SetMember).
        var members = new SetMember[set.Values.Count];
        var memberTypes = new IArrowType?[set.Values.Count];
        for (var k = 0; k < set.Values.Count; k++)
        {
            if (set.Values[k] is LiteralExpression literal)
            {
                members[k] = new SetMember(
                    literal.Value.IsNull ? null : (LiteralValue?)literal.Value);
                continue;
            }

            var (values, type) = EvalOperand(set.Values[k], batch);
            members[k] = new SetMember(values);
            memberTypes[k] = type;
        }

        CheckSetMembers(operandType, operand, members, memberTypes, askedFromTree);
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
                var lit = member.At(i);
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
    /// scale that Spark's promotion rules are computed from (and the type-inferring materializer
    /// has no decimal case at all).
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

        // A short-circuiting function is handed its arguments unevaluated, because which of them
        // to evaluate -- and over which rows -- is part of what the function means. Everything
        // else is evaluated first and invoked with the answers.
        if (_shortCircuiting is not null && _shortCircuiting.ShortCircuits(call.Name))
            return _shortCircuiting.Invoke(call.Name, new CallArguments(this, call, batch), batch.Length);

        var arguments = new IArrowArray[call.Arguments.Count];
        for (int i = 0; i < call.Arguments.Count; i++)
            arguments[i] = EvalExpressionAsArray(call.Arguments[i], batch);

        // A call whose every argument is constant may mean something the same call over a column
        // does not -- see IConstantFoldedFunctions. Asked before PickMinimumPrecision, which has
        // nothing to say about a call answered without being invoked.
        if (_constantFolded is not null && AllConstant(call.Arguments))
        {
            var folded = _constantFolded.InvokeOverConstants(call.Name, arguments, batch.Length);
            if (folded is not null)
                return folded;
        }

        PickMinimumPrecision(call, arguments);

        return _functions.Invoke(call.Name, arguments, batch.Length);
    }

    private static bool AllConstant(IReadOnlyList<Expression> expressions)
    {
        for (var i = 0; i < expressions.Count; i++)
        {
            if (!IsConstant(expressions[i]))
                return false;
        }

        return true;
    }

    /// <summary>
    /// Whether <paramref name="expression"/> has the same value in every row -- Spark's
    /// <c>foldable</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Structural: the question is whether the expression reads a column, not whether the column
    /// holds one value in this batch. Spark refuses
    /// <c>CAST(CASE WHEN a &gt; 0 THEN 'epoch' ELSE 'epoch' END AS DATE)</c> although every row
    /// of it is <c>'epoch'</c>; a content test would answer 1970-01-01, and could answer
    /// differently for the same CHECK constraint over the next batch.
    /// </para>
    /// <para>
    /// A reference is the only thing this rejects, because every function a registry may hold
    /// here is deterministic: Spark's <c>foldable</c> also excludes <c>rand()</c>,
    /// <c>current_date()</c> and the rest of its non-deterministic family, and none of them is
    /// registered. A registry that adds one must be accounted for here.
    /// </para>
    /// </remarks>
    private static bool IsConstant(Expression expression)
    {
        switch (expression)
        {
            case LiteralExpression or TruePredicate or FalsePredicate:
                return true;

            case UnboundReference or BoundReference:
                return false;

            case FunctionCall call:
                return AllConstant(call.Arguments);

            case AndPredicate and:
                return AllConstant(and.Children);

            case OrPredicate or:
                return AllConstant(or.Children);

            case NotPredicate not:
                return IsConstant(not.Child);

            case ComparisonPredicate comparison:
                return IsConstant(comparison.Left) && IsConstant(comparison.Right);

            case UnaryPredicate unary:
                return IsConstant(unary.Operand);

            case SetPredicate set:
                return IsConstant(set.Operand) && AllConstant(set.Values);

            // Anything this does not recognise. Answering false only costs the fold, and a node
            // added later is far likelier to read a column than not.
            default:
                return false;
        }
    }

    /// <summary>
    /// Reads an integral literal that meets a decimal as the narrowest decimal holding it, which
    /// is the cast Spark's analyzer inserts at a binary operator.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The rule is a cast on one operand — Spark's analyzed plan for <c>d1 + 2</c> is
    /// <c>(d1 + cast(2 as decimal(1,0)))</c> — so reproducing it as one leaves every type and
    /// value rule downstream untouched: <c>SparkNumericTypes.ArithmeticResult</c> answers
    /// <c>decimal(7,6)</c> for <c>1.5BD / 2</c> without a literal case of its own.
    /// </para>
    /// <para>
    /// Fewer calls take it than the name suggests. Arithmetic does. <c>nullif</c> takes it on its
    /// second argument only, per the optimized plans: <c>nullif(d5, 0)</c> becomes
    /// <c>if (cast(d5 as decimal(38,37)) = cast(cast(0 as decimal(1,0)) as decimal(38,37))) null
    /// else d5</c>, while <c>nullif(0, d5)</c> becomes
    /// <c>if (cast(0 as decimal(38,28)) = cast(d5 as decimal(38,28))) null else 0</c>. So against
    /// <c>CAST(4E-32 AS DECIMAL(38,38))</c> the first keeps the value and the second is NULL.
    /// </para>
    /// <para>
    /// <c>greatest</c>, <c>least</c>, <c>coalesce</c>, <c>if</c>, <c>CASE</c> and <c>round</c>
    /// do not take it — they are not binary operators, and <c>round</c>'s second argument must
    /// stay an integer. The comparison operators do, through <see cref="CoerceExactNumerics"/>.
    /// </para>
    /// </remarks>
    private void PickMinimumPrecision(FunctionCall call, IArrowArray[] arguments)
    {
        // Two arguments, because the rule is Spark's for a binary operator.
        if (_literalPrecision is null || arguments.Length != 2)
            return;

        // The two positions are independent even though the second reads an argument the first
        // may have rebuilt: an argument is only rebuilt when the other one is a decimal, and an
        // integral literal is not one, so no call can narrow both.
        for (var i = 0; i < 2; i++)
        {
            if (IntegralLiteral(call.Arguments[i]) is not long value)
                continue;

            if (_literalPrecision.LiteralArgumentType(
                    call.Name, i, value, arguments[1 - i].Data.DataType) is not Decimal128Type target)
                continue;

            // Built from the value rather than by reading the array back, which would cost an
            // O(batch) scan and a temporary array. The replacement is at the array's own length,
            // not the batch's, because a literal evaluated over no rows carries the extra row
            // ConstantArray adds so that a type survives an empty selection.
            arguments[i] = ConstantDecimalArray(value, target, arguments[i].Length);
        }
    }

    /// <summary>A decimal column holding one integral value in every row.</summary>
    /// <remarks>
    /// Unlike the general <see cref="BuildDecimalArray"/>, which goes through
    /// <see cref="BigInteger"/> per row, the value here is constant with scale zero, so its
    /// sixteen bytes are laid out once and copied. Nothing is allocated per row.
    /// </remarks>
    private static IArrowArray ConstantDecimalArray(long value, Decimal128Type type, int length)
    {
        const int ByteWidth = 16;

        var bytes = new byte[length * ByteWidth];

        // Zero rows is a real case: it is how a branch nothing selected gets its type.
        if (length > 0)
        {
            var first = bytes.AsSpan(0, ByteWidth);

            // Two's complement over the full width, so a negative value sign-extends through the
            // upper eight bytes rather than reading as a very large positive one.
            if (value < 0)
                first.Fill(0xFF);

            BinaryPrimitives.WriteInt64LittleEndian(first, value);

            for (var row = 1; row < length; row++)
                first.CopyTo(bytes.AsSpan(row * ByteWidth, ByteWidth));
        }

        var validity = new ArrowBuffer.BitmapBuilder(length);
        for (var row = 0; row < length; row++)
            validity.Append(true);

        return new Decimal128Array(new ArrayData(
            type, length, 0, 0, [validity.Build(), new ArrowBuffer(bytes)]));
    }

    /// <summary>
    /// The value of the integral literal an operand is, for Spark's minimum-precision rule, or
    /// null when it is anything else.
    /// </summary>
    /// <remarks>
    /// A negated literal is one only when the parser folded it. Spark's grammar makes <c>-2</c> a
    /// single literal and <c>SparkSqlParser</c> does the same, so a <c>negative</c> call here is a
    /// real operator and is not unwrapped: <c>- -2</c> is a UnaryMinus over a literal in Spark and
    /// takes no cast. <c>d1 + -2</c> is decimal(11,2), the same as <c>d1 + 2</c>, since the rule
    /// reads a precision, and <see cref="long.MinValue"/> is a bigint literal Spark's rule reads
    /// as decimal(19,0).
    /// </remarks>
    private static long? IntegralLiteral(Expression expression)
    {
        if (expression is not LiteralExpression literal
            || literal.Value.Type is not (LiteralValue.Kind.Int32 or LiteralValue.Kind.Int64))
        {
            return null;
        }

        return literal.Value.Type == LiteralValue.Kind.Int32
            ? literal.Value.AsInt32
            : literal.Value.AsInt64;
    }

    // -- Evaluating over a selection of rows --

    /// <summary>The empty row selection, which types an expression without evaluating one.</summary>
    private static readonly int[] NoRows = new int[0];

    /// <summary>
    /// Evaluates <paramref name="expression"/> over the rows <paramref name="rows"/> selects, and
    /// returns an array of the batch's full length whose other rows are NULL.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Three cases rather than one general path, because two of them are common and cost
    /// nothing. A selection covering every row is evaluated against the batch itself with nothing
    /// copied. An empty one evaluates over no rows, which reads no value and so cannot raise, to
    /// answer what type the expression would have produced: Spark types a conditional from every
    /// branch, including one nothing selected, and refuses one whose branches share no type.
    /// </para>
    /// <para>
    /// Only a mixed selection gathers. The columns are gathered, not the answers, so the
    /// expression is evaluated over a short batch exactly as over a whole one, and a nested
    /// conditional short-circuits again over the rows it was given.
    /// </para>
    /// <para>
    /// Gathering, not masking: nulling the unselected rows of the input columns and evaluating
    /// whole would be cheaper but wrong, because null-propagation is not universal. In
    /// <c>coalesce(a, coalesce(z, 1/0))</c> a nulled <c>z</c> makes the inner conditional choose
    /// <c>1/0</c> on exactly the rows the outer one had already decided.
    /// </para>
    /// </remarks>
    private IArrowArray EvaluateOver(Expression expression, RecordBatch batch, ReadOnlySpan<bool> rows)
    {
        var selected = Selected(rows);

        // Must stay before the full-selection test: over an empty batch both are true, and the
        // full-selection path would hand a caller the one-row array ConstantArray builds for a
        // literal.
        if (selected == 0)
            return ArrowCompute.MakeNullArray(TypeOver(expression, batch), batch.Length);

        // Every row wants it: the ordinary case for a first operand and for a batch that takes
        // the same branch throughout.
        if (selected == batch.Length)
            return EvalExpressionAsArray(expression, batch);

        var indices = Indices(rows, selected);
        var computed = EvalExpressionAsArray(expression, Restrict(expression, batch, indices));
        return ArrowCompute.Scatter(computed, indices, batch.Length);
    }

    /// <summary>
    /// <see cref="EvaluateOver"/> for a predicate: the rows outside the selection come back null,
    /// which is the value three-valued logic already has for "not known".
    /// </summary>
    private bool?[] EvalPredicateOver(Predicate predicate, RecordBatch batch, bool[] rows)
    {
        var selected = Selected(rows);

        if (selected == 0)
        {
            // Evaluated and discarded, for the reason TypeOver gives: over no rows it cannot raise
            // on a value, and an operand that cannot be evaluated at all still says so.
            TypeOver(predicate, batch);
            return new bool?[batch.Length];
        }

        if (selected == batch.Length)
            return EvalPredicate(predicate, batch);

        var result = new bool?[batch.Length];
        var indices = Indices(rows, selected);
        var partial = EvalPredicate(predicate, Restrict(predicate, batch, indices));
        for (var i = 0; i < indices.Length; i++)
            result[indices[i]] = partial[i];

        return result;
    }

    /// <summary>
    /// The type <paramref name="expression"/> produces, for a selection no row is in.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Over no rows nothing can be read, so an error in a branch nobody selected cannot happen,
    /// and what comes back still carries the type Spark would have typed the conditional from.
    /// </para>
    /// <para>
    /// With rows, if it has to be. A few functions read a scalar argument whose value decides the
    /// result type out of row 0 — <c>round</c>'s scale, which may itself be computed. A literal
    /// survives an empty batch (<see cref="ConstantArray"/> keeps one row for this), but
    /// <c>round(f, 1 + 1)</c> hands <c>round</c> a zero-length scale and it throws. So when the
    /// empty evaluation fails, it is retried over the batch.
    /// </para>
    /// <para>
    /// The empty attempt must stay first: the retry evaluates a branch no row selected, which is
    /// what can raise. A failing retry throws in place of the first failure, so it never turns an
    /// answer into an error.
    /// </para>
    /// <para>
    /// The residual gap is an expression with both a value-dependent result type and a branch that
    /// raises over real rows, as in <c>round(CAST('x' AS DOUBLE), 1 + 1)</c> inside a branch
    /// nothing reaches. That needs the type inferred rather than evaluated.
    /// </para>
    /// </remarks>
    private IArrowType TypeOver(Expression expression, RecordBatch batch)
    {
        try
        {
            return EvalExpressionAsArray(expression, Restrict(expression, batch, NoRows))
                .Data.DataType;
        }
        catch (ExpressionAnalysisException)
        {
            // An analysis refusal is the answer, so it is not retried: it is a property of the
            // operand types, and retrying would replace it with whatever the first value raises
            // (`false AND CAST(s AS INT) = bl` over a row holding 'abc' would report
            // CAST_INVALID_INPUT instead of the type refusal).
            throw;
        }
        catch (Exception)
        {
            // Deliberately broad: the ways of failing to answer are the registry's business.
            // Nothing is swallowed -- a retry that fails throws in place of what was caught.
            return EvalExpressionAsArray(expression, batch).Data.DataType;
        }
    }

    private static int Selected(ReadOnlySpan<bool> rows)
    {
        var selected = 0;
        for (var row = 0; row < rows.Length; row++)
        {
            if (rows[row]) selected++;
        }

        return selected;
    }

    private static int[] Indices(ReadOnlySpan<bool> rows, int selected)
    {
        var indices = new int[selected];
        var next = 0;
        for (var row = 0; row < rows.Length; row++)
        {
            if (rows[row]) indices[next++] = row;
        }

        return indices;
    }

    /// <summary>
    /// A batch holding only <paramref name="rows"/>, and only the columns
    /// <paramref name="expression"/> names.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Only the named columns: it is cheaper on a wide batch, and a column of a type
    /// <c>ArrowCompute.Take</c> declines to gather would otherwise fail a branch that does not
    /// read it.
    /// </para>
    /// <para>
    /// Every column matching a name is kept, not the first. <see cref="GetColumn"/> resolves
    /// case-insensitively and refuses an ambiguous match, and dropping the second of a colliding
    /// pair here would turn that refusal into a wrong answer.
    /// </para>
    /// </remarks>
    private static RecordBatch Restrict(Expression expression, RecordBatch batch, int[] rows)
    {
        var names = new List<string>();
        CollectReferences(expression, names);

        var fields = batch.Schema.FieldsList;
        var schema = new Schema.Builder();
        var columns = new List<IArrowArray>();

        for (var i = 0; i < fields.Count; i++)
        {
            if (!Names(names, fields[i].Name))
                continue;

            schema.Field(fields[i]);
            columns.Add(ArrowCompute.Take(batch.Column(i), rows));
        }

        return new RecordBatch(schema.Build(), columns, rows.Length);
    }

    private static bool Names(List<string> names, string field)
    {
        foreach (var name in names)
        {
            if (string.Equals(name, field, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    /// <summary>Collects every column name <paramref name="expression"/> reads, duplicates and all.</summary>
    /// <remarks>
    /// Exhaustive on purpose: an unlisted node kind would be read as naming no columns and the
    /// restricted batch would be missing a column it reads, so the default throws.
    /// </remarks>
    private static void CollectReferences(Expression expression, List<string> names)
    {
        switch (expression)
        {
            case UnboundReference u:
                names.Add(u.Name);
                break;

            case BoundReference b:
                names.Add(b.Name);
                break;

            case LiteralExpression:
            case TruePredicate:
            case FalsePredicate:
                break;

            case FunctionCall fc:
                foreach (var argument in fc.Arguments) CollectReferences(argument, names);
                break;

            case AndPredicate and:
                foreach (var child in and.Children) CollectReferences(child, names);
                break;

            case OrPredicate or:
                foreach (var child in or.Children) CollectReferences(child, names);
                break;

            case NotPredicate not:
                CollectReferences(not.Child, names);
                break;

            case ComparisonPredicate cmp:
                CollectReferences(cmp.Left, names);
                CollectReferences(cmp.Right, names);
                break;

            case UnaryPredicate unary:
                CollectReferences(unary.Operand, names);
                break;

            case SetPredicate set:
                CollectReferences(set.Operand, names);
                foreach (var value in set.Values) CollectReferences(value, names);
                break;

            default:
                throw new NotSupportedException(
                    $"Cannot find the columns read by {expression.GetType().Name}");
        }
    }

    /// <summary>
    /// One call's arguments, evaluated on demand. See <see cref="IConditionalArguments"/>.
    /// </summary>
    private sealed class CallArguments : IConditionalArguments
    {
        private readonly ArrowRowEvaluator _evaluator;
        private readonly FunctionCall _call;
        private readonly RecordBatch _batch;

        public CallArguments(ArrowRowEvaluator evaluator, FunctionCall call, RecordBatch batch)
        {
            _evaluator = evaluator;
            _call = call;
            _batch = batch;
        }

        public int Count => _call.Arguments.Count;

        /// <remarks>
        /// Read off the tree rather than the evaluated array: a bare <c>NULL</c> is a null
        /// literal, and <c>CAST(NULL AS INT)</c> -- a call carrying a type that Spark lets
        /// constrain the result -- is not, however alike the two look once they are columns.
        /// </remarks>
        public bool IsNullLiteral(int index) =>
            _call.Arguments[index] is LiteralExpression literal && literal.Value.IsNull;

        public IArrowArray Evaluate(int index, ReadOnlySpan<bool> rows) =>
            _evaluator.EvaluateOver(_call.Arguments[index], _batch, rows);
    }

    /// <summary>Builds a constant array of <paramref name="value"/>, repeated.</summary>
    /// <remarks>
    /// <para>
    /// At least one row long, even over an empty batch. The registry reads a scalar argument out
    /// of row 0 -- a cast's target type, the scale <c>round</c> was asked for -- so over no rows
    /// there would be nothing to read, and evaluating over zero rows is how
    /// <see cref="EvaluateOver"/> learns the type of a branch no row selected.
    /// </para>
    /// <para>
    /// The extra row is internal: every function is bounded by the row count rather than by its
    /// arguments' lengths, and <see cref="EvaluateExpression(Expression, RecordBatch)"/> trims it
    /// off at the boundary.
    /// </para>
    /// </remarks>
    private static IArrowArray ConstantArray(LiteralValue value, int length)
    {
        length = Math.Max(length, 1);

        if (value.IsNull)
            return NullLiteralArray(length);

        // A decimal literal's type comes from the value, matching Spark: `1.5` is decimal(2,1),
        // `.5` is decimal(1,1) and `1.` is decimal(1,0).
        if (value.Type == LiteralValue.Kind.Decimal)
        {
            var (precision, scale) = DecimalTypeOf(value.AsDecimal);
            return MaterializeAsArray(
                Repeat(value, length), length, new Decimal128Type(precision, scale));
        }

        // ...and the same for one too wide for System.Decimal.
        if (value.Type == LiteralValue.Kind.HighPrecisionDecimal)
        {
            var (unscaled, wideScale) = value.AsHighPrecisionDecimal;
            return MaterializeAsArray(
                Repeat(value, length), length,
                new Decimal128Type(DecimalPrecisionOf(unscaled, wideScale), wideScale));
        }

        return MaterializeAsArray(Repeat(value, length), length);
    }

    /// <summary>The column a bare <c>NULL</c> literal becomes.</summary>
    /// <remarks>
    /// <para>
    /// Arrow's <see cref="NullType"/>, which is Spark's <c>void</c> — not an all-null string
    /// column, which a real string column holding nothing in this batch would be
    /// indistinguishable from.
    /// </para>
    /// <para>
    /// <c>SparkArrays</c> and <c>SparkFunctions</c> read a void cell as null, and
    /// <c>SparkNumericTypes</c> gives the type rules, so a function that reads its arguments
    /// through those needs no case of its own.
    /// </para>
    /// </remarks>
    private static IArrowArray NullLiteralArray(int length) => new NullArray(length);

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
    /// <para>
    /// Not <c>BigInteger.Log10</c>, which is a double and gets the boundaries wrong in both
    /// directions: it counts 10^30 as thirty digits (its logarithm is 29.999999999999996) and
    /// 10^38-1 as thirty-nine (it rounds up to 38.0), giving a type too narrow for the value or
    /// wider than any Spark decimal.
    /// </para>
    /// <para>
    /// Repeated division is exact, and this runs once per literal, not once per row.
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

    /// <summary>Finds the column a reference names, the way Spark resolves an identifier.</summary>
    /// <remarks>
    /// <para>
    /// Case-insensitively, because Spark is: <c>spark.sql.caseSensitive</c> defaults to false,
    /// and a Delta CHECK constraint or generation expression is stored as text, so a table Spark
    /// created can carry an expression whose identifiers are spelled in a case its schema does
    /// not use.
    /// </para>
    /// <para>
    /// An ambiguous match refuses, and the exactly-spelled name does not win it: with <c>a</c>
    /// and <c>A</c> in one schema, all four of <c>a</c>, <c>A</c>, <c>`a`</c> and <c>`A`</c>
    /// raise AMBIGUOUS_REFERENCE in Spark. Backticks do not make a name case-sensitive either:
    /// <c>`WEIRD NAME`</c> resolves to a column named <c>weird name</c>.
    /// </para>
    /// <para>
    /// A linear scan on purpose: this runs once per reference per batch, never per row, and a
    /// case-insensitive dictionary would need an ambiguity sentinel and a rebuild per batch.
    /// </para>
    /// </remarks>
    private static IArrowArray GetColumn(RecordBatch batch, string name)
    {
        var fields = batch.Schema.FieldsList;
        var match = -1;
        var collision = -1;

        for (var i = 0; i < fields.Count; i++)
        {
            if (!string.Equals(fields[i].Name, name, StringComparison.OrdinalIgnoreCase))
                continue;

            if (match < 0)
            {
                match = i;
            }
            else
            {
                collision = i;
                break;
            }
        }

        if (match < 0)
            throw new ArgumentException(
                $"Column '{name}' not found in batch schema.");

        if (collision >= 0)
            throw new ArgumentException(
                $"Column '{name}' is ambiguous in the batch schema: '{fields[match].Name}' and "
                + $"'{fields[collision].Name}' differ only in case, and identifiers resolve "
                + "case-insensitively.");

        return batch.Column(match);
    }

    // ── Arrow ↔ LiteralValue ──

    private static LiteralValue?[] ArrowToLiteralValues(IArrowArray array, int length)
    {
        var result = new LiteralValue?[length];
        switch (array)
        {
            // A `void` column -- a bare NULL literal, or a conditional every branch of which was
            // one. Every row is null, which is what the freshly-allocated array already holds.
            case NullArray:
                break;
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
            // Temporal + decimal columns map to the same LiteralValue kinds a stats/JSON decoder would
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
        // Choose an Arrow type from the first non-null value.
        LiteralValue.Kind? kind = null;
        for (int i = 0; i < length; i++)
        {
            if (values[i].HasValue) { kind = values[i]!.Value.Type; break; }
        }

        // Nothing to infer a type from. That is a bare NULL by any other name, so it takes the
        // same `void` column one does.
        if (kind is null) return NullLiteralArray(length);

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
            // A timestamp literal, and the instant a DATE literal carries before its cast.
            // Microseconds in UTC, matching what the readers produce and what SparkLiteral
            // resolves a zone-less literal to.
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

    /// <summary>
    /// Materializes against a caller-supplied Arrow type, which the answer always carries.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The decimal and temporal cases need metadata a bare <see cref="LiteralValue"/> cannot
    /// carry — precision/scale/width, unit/timezone, date-vs-timestamp — and every other type is
    /// inferrable from a value, so those fall through to the type-inferring overload.
    /// </para>
    /// <para>
    /// Except when there is no value to infer from: an all-null input would otherwise come back
    /// <c>void</c>, where the callers of the public
    /// <see cref="EvaluateExpression(Expression, RecordBatch, IArrowType)"/> overload
    /// (<c>DeltaGeneratedColumns</c>, the Lance writer) put the array straight into a batch
    /// whose schema declares <paramref name="targetType"/>. A null is null at every type, so the
    /// target decides which one it is.
    /// </para>
    /// </remarks>
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
            _ when FirstKind(values) is null => ArrowCompute.MakeNullArray(targetType, length),
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
    /// values — and its scale runs 0 to 28. A column declared no wider than this in both holds no
    /// value it cannot carry exactly; one declared wider holds values it cannot, so the whole
    /// column takes the exact path.
    /// </remarks>
    private const int MaxExactDecimalDigits = 28;

    // A decimal column value: the common in-range case as System.Decimal (how a decimal literal and a
    // stats decoder also represent it); a column that can hold values System.Decimal cannot takes its
    // exact unscaled BigInteger plus the column's scale, read straight from the fixed-width
    // little-endian value buffer — the same raw layout the format writers use.
    //
    // Decided from the declared type, not from an exception. Decimal128Array.GetValue raises
    // OverflowException only for excess magnitude; for excess significant digits it silently rounds
    // to 28 and reports success. So keying the fallback on the exception would miss exactly the
    // values it exists to protect: a decimal(38,38) is under 1, never overflows, and would arrive
    // already rounded.
    //
    // Conservative on purpose: a small value in a wide-declared column takes the exact path it does
    // not strictly need, which costs a BigInteger but avoids a per-cell test that has to be right
    // about every corner of decimal's 96-bit mantissa.
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

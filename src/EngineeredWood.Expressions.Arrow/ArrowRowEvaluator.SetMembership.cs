// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Expressions.Arrow;

/// <summary>
/// The direct membership test of a column against literals (#799).
/// </summary>
/// <remarks>
/// <para>
/// The general path turns every row of the operand into a <see cref="LiteralValue"/> and compares it with
/// each member through <see cref="LiteralValue.CompareTo(LiteralValue)"/> in a try/catch: a five-string
/// <c>IN</c> over a 2M-row column cost about sixteen times the equality the direct comparison (#560) answers.
/// </para>
/// <para>
/// This path asks the direct comparison's kernels each member in turn under <c>=</c> and combines their
/// answers: a row is in the set when any member equals it. A kernel reproduces
/// <see cref="LiteralValue.CompareTo(LiteralValue)"/> for its column's kind, which is the comparison the
/// general path makes, so the answers cannot drift; a member a kernel declines declines the whole set.
/// </para>
/// <para>
/// The analysis check is asked exactly as the general path asks it, with the column as a one-row operand.
/// A set the registry's coercion would resolve through a common type (a string mixed with anything else, or
/// a decimal anywhere) declines: that can cast the column. Only literal members are taken; a member that is
/// an expression varies by row, or may raise, and is the general path's.
/// </para>
/// <para>
/// SQL's NULL rules, as the general path keeps them: a NULL operand row is NULL, for <c>IN</c> and
/// <c>NOT IN</c> alike; a row no member equals is NULL when a member is NULL, and otherwise FALSE for
/// <c>IN</c> and TRUE for <c>NOT IN</c>; a row a member equals is TRUE for <c>IN</c> and FALSE for
/// <c>NOT IN</c>, NULL members or not.
/// </para>
/// </remarks>
public sealed partial class ArrowRowEvaluator
{
    /// <summary>How many membership tests the direct path answered, so a test can tell it ran.</summary>
    internal int DirectSets { get; private set; }

    /// <summary>Whether <paramref name="set"/> has the shape the direct path takes: a column against literals.</summary>
    private bool MayTakeSetDirectly(SetPredicate set) =>
        DirectColumnComparison && set.Values.Count > 0 && IsReference(set.Operand)
        && set.Values.All(v => v is LiteralExpression);

    private DirectAnswer? TrySetOverColumn(SetPredicate set, RecordBatch batch, bool askedFromTree)
    {
        if (batch.Length == 0 || !MayTakeSetDirectly(set))
            return null;

        var members = new SetMember[set.Values.Count];
        var memberTypes = new IArrowType?[set.Values.Count];
        bool anyNull = false;
        for (var k = 0; k < members.Length; k++)
        {
            if (set.Values[k] is not LiteralExpression literal)
                return null;
            members[k] = new SetMember(literal.Value.IsNull ? null : (LiteralValue?)literal.Value);
            anyNull |= literal.Value.IsNull;
        }

        IArrowArray column;
        try
        {
            // As the batch holds it, as for a comparison: a view or large string is read in place (#859).
            column = set.Operand is UnboundReference u ? FindColumn(batch, u) : FindColumn(batch, ((BoundReference)set.Operand).Name);
        }
        catch (ExpressionAnalysisException)
        {
            throw;
        }
        catch (Exception)
        {
            // The general path raises whatever this was, in its own order.
            return null;
        }

        if (!HasKernel(column))
            return null;

        var columnType = ByteArrayOffsets.TypeOf(column.Data.DataType);
        var placeholder = new LiteralValue?[] { Placeholder };
        CheckSetMembers(columnType, placeholder, members, memberTypes, askedFromTree);
        if (_coercion is not null && SetCoerces(columnType, placeholder, members, memberTypes))
            return null;

        // Each non-null member's rows, OR-ed: a row's bit is set when some member equals it. A kernel clears a
        // null row's bit, and its validity is the column's, the same for every member.
        int length = batch.Length;
        byte[]? found = null;
        byte[]? validity = null;
        bool anyAnswer = false;
        foreach (var member in members)
        {
            if (member.Constant is not { } constant)
                continue;

            if (Kernel(column, ComparisonOperator.Equal, constant) is not { } answer)
            {
                GC.KeepAlive(column);
                return null;
            }

            if (!anyAnswer)
            {
                found = answer.Values;
                validity = answer.Validity;
                anyAnswer = true;
                continue;
            }

            var bits = answer.Values;
            for (int b = 0; b < bits.Length; b++)
                found![b] |= bits[b];
        }

        // The kernels read raw spans over the column's buffers; see TryCompareColumnToConstant.
        GC.KeepAlive(column);

        // Every member NULL: the general path's answer is NULL throughout, which is not worth a kernel.
        if (!anyAnswer)
            return null;

        DirectSets++;
        return Combine(found!, validity, length, set.Op == SetOperator.In, anyNull);
    }

    /// <summary>
    /// The set's answer from the rows some member equals (<paramref name="found"/>, a null row's bit clear)
    /// and the column's validity (null: every row valid), by the rules in the remarks above.
    /// </summary>
    private static DirectAnswer Combine(byte[] found, byte[]? columnValidity, int length, bool isIn, bool anyNullMember)
    {
        int bytes = BitmapBytes(length);
        int tail = length & 7;
        byte lastMask = tail == 0 ? (byte)0xFF : (byte)((1 << tail) - 1);

        var values = new byte[bytes];
        var validity = new byte[bytes];
        int valid = 0;
        for (int b = 0; b < bytes; b++)
        {
            byte rowValid = columnValidity is null ? (byte)0xFF : columnValidity[b];
            if (b == bytes - 1)
                rowValid &= lastMask;
            byte hit = (byte)(found[b] & rowValid);

            // A valid row a member equals is decided; one no member equals is decided only when no member is NULL.
            byte decided = anyNullMember ? hit : rowValid;
            values[b] = isIn ? hit : (byte)(decided & ~hit);
            validity[b] = decided;
            valid += PopCount(decided);
        }

        return valid == length
            ? new DirectAnswer(values, null, 0, length)
            : new DirectAnswer(values, validity, length - valid, length);
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Expressions.Arrow;

/// <summary>
/// The direct comparison of a column against a constant (#560).
/// </summary>
/// <remarks>
/// <para>
/// The general path turns every row of both operands into a <see cref="LiteralValue"/>: a string
/// per row for a string column, a 1M-element array per operand, a type switch and a try/catch per
/// comparison. For <c>category = 'cat_03'</c> that cost about four times the Parquet decode of the
/// column it filters.
/// </para>
/// <para>
/// This path reads the column's own buffers and compares each value against the constant, which is
/// converted once. It is an optimisation only: it answers exactly what the general path answers,
/// or it declines and the general path runs. Each kernel reproduces
/// <see cref="LiteralValue.CompareTo(LiteralValue)"/> for the kinds the column's values would have
/// been read as (see <c>ArrowToLiteralValues</c>), using the same conversions, so the two cannot
/// drift on a promotion rule. Anything not listed below declines.
/// </para>
/// <para>
/// The analysis check and the registry's coercion rules are asked exactly as the general path asks
/// them, with the constant as a one-row operand. A coercion that would cast the column declines; one
/// that casts the constant is applied once, to the constant. A constant that cannot be evaluated --
/// a cast that raises, say -- also declines, so the general path raises it in its own order.
/// </para>
/// <para>
/// One place the answers can differ, deliberately: a date or timestamp so far out of range that
/// <see cref="DateTimeOffset"/> cannot hold it makes the general path throw while it builds the
/// row's value. Here the raw value is compared against the constant and answers.
/// </para>
/// </remarks>
public sealed partial class ArrowRowEvaluator
{
    /// <summary>Whether the direct comparison may run. Tests turn it off to compare the two paths.</summary>
    internal bool DirectColumnComparison { get; set; } = true;

    /// <summary>How many comparisons the direct path answered, so a test can tell it ran.</summary>
    internal int DirectComparisons { get; private set; }

    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    private static readonly Schema EmptySchema = new Schema.Builder().Build();

    private bool?[]? TryCompareColumnToConstant(ComparisonPredicate cmp, RecordBatch batch, bool asked)
    {
        if (!DirectColumnComparison || batch.Length == 0)
            return null;

        bool columnOnLeft;
        Expression columnExpression, constantExpression;
        if (IsReference(cmp.Left) && IsConstantOperand(cmp.Right))
        {
            columnOnLeft = true;
            columnExpression = cmp.Left;
            constantExpression = cmp.Right;
        }
        else if (IsConstantOperand(cmp.Left) && IsReference(cmp.Right))
        {
            columnOnLeft = false;
            columnExpression = cmp.Right;
            constantExpression = cmp.Left;
        }
        else
        {
            return null;
        }

        // `'abc' STARTSWITH col` reads the column as the prefix; not worth a kernel.
        if (!columnOnLeft && cmp.Op is ComparisonOperator.StartsWith or ComparisonOperator.NotStartsWith)
            return null;

        IArrowArray column;
        LiteralValue constant;
        IArrowType? constantType;
        try
        {
            column = columnExpression is UnboundReference u ? GetColumn(batch, u) : GetColumn(batch, ((BoundReference)columnExpression).Name);
            if (!HasKernel(column))
                return null;

            if (!TryEvaluateConstant(constantExpression, out constant, out constantType))
                return null;
        }
        catch (ExpressionAnalysisException)
        {
            throw;
        }
        catch (Exception)
        {
            // The general path raises whatever this was, in the order it evaluates operands.
            return null;
        }

        var columnType = column.Data.DataType;
        var columnSide = new LiteralValue?[] { Placeholder };
        var constantSide = new LiteralValue?[] { constant };
        var left = columnOnLeft ? columnSide : constantSide;
        var right = columnOnLeft ? constantSide : columnSide;
        var leftType = columnOnLeft ? columnType : constantType;
        var rightType = columnOnLeft ? constantType : columnType;

        if (!asked)
            CheckComparable(cmp.Op, leftType, left, rightType, right);

        if (_coercion is not null)
        {
            bool declined = false;
            try
            {
                CoerceOperandsExcept(
                    cmp, leftType, rightType, ref left, ref right,
                    columnOnLeft ? FixedOperand.Left : FixedOperand.Right, ref declined);
            }
            catch (ExpressionAnalysisException)
            {
                throw;
            }
            catch (Exception)
            {
                // A constant that will not cast. The general path masks the cast by the column's
                // nulls before it runs, so whether -- and what -- it raises is its decision.
                return null;
            }

            if (declined)
                return null;

            if ((columnOnLeft ? right : left)[0] is not { } coerced)
                return null;

            constant = coerced;
        }

        var op = columnOnLeft ? cmp.Op : Mirror(cmp.Op);
        var answer = column switch
        {
            StringArray a => CompareStrings(a, op, constant),
            Int8Array a => CompareIntegers(a, a.Values, op, constant),
            Int16Array a => CompareIntegers(a, a.Values, op, constant),
            Int32Array a => CompareIntegers(a, a.Values, op, constant),
            Int64Array a => CompareIntegers(a, a.Values, op, constant),
            UInt8Array a => CompareIntegers(a, a.Values, op, constant),
            UInt16Array a => CompareIntegers(a, a.Values, op, constant),
            UInt32Array a => CompareIntegers(a, a.Values, op, constant),
            FloatArray a => CompareFloating(a, a.Values, op, constant),
            DoubleArray a => CompareFloating(a, a.Values, op, constant),
            BooleanArray a => CompareBooleans(a, op, constant),
            Date32Array a => CompareInstants(a, a.Values, TimeSpan.TicksPerDay, op, constant),
            Date64Array a => CompareInstants(a, a.Values, TimeSpan.TicksPerMillisecond, op, constant),
            TimestampArray a => CompareTimestamps(a, op, constant),
            _ => null,
        };

        if (answer is not null)
            DirectComparisons++;
        return answer;
    }

    /// <summary>
    /// The column's stand-in on the coercion path. Never read: the column's declared type answers
    /// every question there. Non-null so that a null mask over it removes nothing.
    /// </summary>
    private static readonly LiteralValue Placeholder = LiteralValue.Of(0);

    private static bool IsReference(Expression expression) =>
        expression is UnboundReference or BoundReference;

    private bool IsConstantOperand(Expression expression) => expression switch
    {
        LiteralExpression literal => !literal.Value.IsNull,
        FunctionCall call => _functions is not null && _functions.IsRegistered(call.Name) && IsConstant(call),
        _ => false,
    };

    private static bool HasKernel(IArrowArray column) => column is
        StringArray or Int8Array or Int16Array or Int32Array or Int64Array
        or UInt8Array or UInt16Array or UInt32Array or FloatArray or DoubleArray
        or BooleanArray or Date32Array or Date64Array or TimestampArray;

    /// <summary>
    /// Evaluates a constant operand once. A literal keeps no declared type, as on the general path;
    /// a constant call -- <c>DATE'2026-10-01'</c> is a cast -- is evaluated over one row and keeps
    /// the type it produced.
    /// </summary>
    private bool TryEvaluateConstant(Expression expression, out LiteralValue value, out IArrowType? type)
    {
        if (expression is LiteralExpression literal)
        {
            value = literal.Value;
            type = null;
            return true;
        }

        var array = InvokeFunction((FunctionCall)expression, new RecordBatch(EmptySchema, [], 1));
        type = array.Data.DataType;
        if (ArrowToLiteralValues(array, 1)[0] is { } v)
        {
            value = v;
            return true;
        }

        value = default;
        return false;
    }

    private static ComparisonOperator Mirror(ComparisonOperator op) => op switch
    {
        ComparisonOperator.LessThan => ComparisonOperator.GreaterThan,
        ComparisonOperator.LessThanOrEqual => ComparisonOperator.GreaterThanOrEqual,
        ComparisonOperator.GreaterThan => ComparisonOperator.LessThan,
        ComparisonOperator.GreaterThanOrEqual => ComparisonOperator.LessThanOrEqual,
        _ => op,
    };

    /// <summary>The answer for a non-null row, from the sign of column-against-constant.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool Decide(ComparisonOperator op, int sign) => op switch
    {
        ComparisonOperator.Equal or ComparisonOperator.NullSafeEqual => sign == 0,
        ComparisonOperator.NotEqual => sign != 0,
        ComparisonOperator.LessThan => sign < 0,
        ComparisonOperator.LessThanOrEqual => sign <= 0,
        ComparisonOperator.GreaterThan => sign > 0,
        _ => sign >= 0,   // GreaterThanOrEqual; the caller has filtered the rest
    };

    private static bool IsOrdering(ComparisonOperator op) => op is
        ComparisonOperator.Equal or ComparisonOperator.NullSafeEqual or ComparisonOperator.NotEqual
        or ComparisonOperator.LessThan or ComparisonOperator.LessThanOrEqual
        or ComparisonOperator.GreaterThan or ComparisonOperator.GreaterThanOrEqual;

    /// <summary>
    /// The answer for a null row: null, except under <c>&lt;=&gt;</c>, where a null column against
    /// the (non-null) constant is false.
    /// </summary>
    private static bool? NullRow(ComparisonOperator op) =>
        op == ComparisonOperator.NullSafeEqual ? false : null;

    // ── Kernels ──

    /// <summary>
    /// UTF-8 bytes against the constant's UTF-8 bytes. <see cref="StringOrdering"/> orders by code
    /// point, which for well-formed text is UTF-8 byte order, and a UTF-16 prefix is a UTF-8 prefix.
    /// A constant with an unpaired surrogate has no UTF-8 form and declines.
    /// </summary>
    /// <remarks>
    /// The column is not decoded, so an ill-formed byte sequence in it is compared as it stands
    /// rather than as the U+FFFD the general path decodes it to. Arrow requires a string column to
    /// be valid UTF-8, and Spark compares the raw bytes too.
    /// </remarks>
    private static bool?[]? CompareStrings(StringArray column, ComparisonOperator op, LiteralValue constant)
    {
        if (constant.Type != LiteralValue.Kind.String)
            return null;

        bool startsWith = op is ComparisonOperator.StartsWith or ComparisonOperator.NotStartsWith;
        if (!startsWith && !IsOrdering(op))
            return null;

        byte[] literal;
        try
        {
            literal = StrictUtf8.GetBytes(constant.AsString);
        }
        catch (EncoderFallbackException)
        {
            return null;
        }

        int length = column.Length;
        var result = new bool?[length];
        var offsets = column.ValueOffsets;
        var data = column.ValueBuffer.Span;
        bool hasNulls = column.NullCount > 0;
        var target = new ReadOnlySpan<byte>(literal);

        if (startsWith)
        {
            bool want = op == ComparisonOperator.StartsWith;
            for (int i = 0; i < length; i++)
            {
                if (hasNulls && column.IsNull(i))
                    continue;   // null under both

                int start = offsets[i];
                result[i] = data.Slice(start, offsets[i + 1] - start).StartsWith(target) == want;
            }

            return result;
        }

        bool? nullRow = NullRow(op);
        for (int i = 0; i < length; i++)
        {
            if (hasNulls && column.IsNull(i))
            {
                result[i] = nullRow;
                continue;
            }

            int start = offsets[i];
            var value = data.Slice(start, offsets[i + 1] - start);
            result[i] = op switch
            {
                // Equality needs no order, and SequenceEqual stops at a length mismatch.
                ComparisonOperator.Equal or ComparisonOperator.NullSafeEqual => value.SequenceEqual(target),
                ComparisonOperator.NotEqual => !value.SequenceEqual(target),
                _ => Decide(op, value.SequenceCompareTo(target)),
            };
        }

        return result;
    }

    /// <summary>
    /// An integral column, every value of which <see cref="LiteralValue.TryAsInt64"/> accepts, against
    /// an integral, floating or decimal constant -- the three branches
    /// <see cref="LiteralValue.CompareTo(LiteralValue)"/> takes for those pairs, in its order.
    /// </summary>
    private static bool?[]? CompareIntegers<T>(
        IArrowArray column, ReadOnlySpan<T> values, ComparisonOperator op, LiteralValue constant)
        where T : struct
    {
        if (!IsOrdering(op))
            return null;

        int length = column.Length;
        var result = new bool?[length];
        bool hasNulls = column.NullCount > 0;
        bool? nullRow = NullRow(op);

        if (LiteralValue.TryAsInt64(constant, out long target))
        {
            for (int i = 0; i < length; i++)
            {
                if (hasNulls && column.IsNull(i))
                    result[i] = nullRow;
                else
                    result[i] = Decide(op, ToInt64(values[i]).CompareTo(target));
            }

            return result;
        }

        if (LiteralValue.IsFloating(constant.Type) && LiteralValue.TryAsDouble(constant, out double floating))
        {
            for (int i = 0; i < length; i++)
            {
                if (hasNulls && column.IsNull(i))
                    result[i] = nullRow;
                else
                    result[i] = Decide(op, LiteralValue.CompareFloating(ToInt64(values[i]), floating));
            }

            return result;
        }

        if (constant.Type == LiteralValue.Kind.Decimal)
        {
            // Exact, as CompareScaledIntegers is: every long is a decimal.
            decimal exact = constant.AsDecimal;
            for (int i = 0; i < length; i++)
            {
                if (hasNulls && column.IsNull(i))
                    result[i] = nullRow;
                else
                    result[i] = Decide(op, ((decimal)ToInt64(values[i])).CompareTo(exact));
            }

            return result;
        }

        return null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long ToInt64<T>(T value) where T : struct
    {
        if (typeof(T) == typeof(long)) return Unsafe.As<T, long>(ref value);
        if (typeof(T) == typeof(int)) return Unsafe.As<T, int>(ref value);
        if (typeof(T) == typeof(short)) return Unsafe.As<T, short>(ref value);
        if (typeof(T) == typeof(sbyte)) return Unsafe.As<T, sbyte>(ref value);
        if (typeof(T) == typeof(uint)) return Unsafe.As<T, uint>(ref value);
        if (typeof(T) == typeof(ushort)) return Unsafe.As<T, ushort>(ref value);
        if (typeof(T) == typeof(byte)) return Unsafe.As<T, byte>(ref value);
        throw new NotSupportedException(typeof(T).Name);
    }

    /// <summary>
    /// A float or double column against any numeric constant: every such pair reaches
    /// <see cref="LiteralValue.CompareFloating"/> through <see cref="LiteralValue.TryAsDouble"/>,
    /// the floating kinds being excluded from the exact branch.
    /// </summary>
    private static bool?[]? CompareFloating<T>(
        IArrowArray column, ReadOnlySpan<T> values, ComparisonOperator op, LiteralValue constant)
        where T : struct
    {
        if (!IsOrdering(op) || !LiteralValue.TryAsDouble(constant, out double target))
            return null;

        int length = column.Length;
        var result = new bool?[length];
        bool hasNulls = column.NullCount > 0;
        bool? nullRow = NullRow(op);
        for (int i = 0; i < length; i++)
        {
            if (hasNulls && column.IsNull(i))
                result[i] = nullRow;
            else
                result[i] = Decide(op, LiteralValue.CompareFloating(ToDouble(values[i]), target));
        }

        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static double ToDouble<T>(T value) where T : struct
    {
        if (typeof(T) == typeof(double)) return Unsafe.As<T, double>(ref value);
        if (typeof(T) == typeof(float)) return Unsafe.As<T, float>(ref value);
        throw new NotSupportedException(typeof(T).Name);
    }

    private static bool?[]? CompareBooleans(BooleanArray column, ComparisonOperator op, LiteralValue constant)
    {
        if (!IsOrdering(op) || constant.Type != LiteralValue.Kind.Boolean)
            return null;

        bool target = constant.AsBoolean;
        int length = column.Length;
        var result = new bool?[length];
        bool hasNulls = column.NullCount > 0;
        bool? nullRow = NullRow(op);
        for (int i = 0; i < length; i++)
        {
            if (hasNulls && column.IsNull(i))
                result[i] = nullRow;
            else
                result[i] = Decide(op, column.GetValue(i)!.Value.CompareTo(target));
        }

        return result;
    }

    /// <summary>
    /// A date column, read by the general path as UTC midnight of the day, against an instant: a
    /// <see cref="DateTimeOffset"/>, or a <see cref="DateOnly"/>, which compares as UTC midnight too.
    /// </summary>
    /// <remarks>
    /// Compared in the column's own unit against the constant's offset from the epoch, split once
    /// into whole units and a remainder, so no row is scaled into ticks and nothing can overflow.
    /// </remarks>
    private static bool?[]? CompareInstants<T>(
        IArrowArray column, ReadOnlySpan<T> values, long ticksPerUnit, ComparisonOperator op, LiteralValue constant)
        where T : struct
    {
        if (!IsOrdering(op) || !TryAsInstant(constant, out var instant))
            return null;

        long offset = instant.UtcTicks - Epoch.UtcTicks;
        long whole = FloorDivide(offset, ticksPerUnit);
        bool exact = offset - whole * ticksPerUnit == 0;

        int length = column.Length;
        var result = new bool?[length];
        bool hasNulls = column.NullCount > 0;
        bool? nullRow = NullRow(op);
        for (int i = 0; i < length; i++)
        {
            if (hasNulls && column.IsNull(i))
            {
                result[i] = nullRow;
                continue;
            }

            long v = ToInt64(values[i]);
            int sign = v < whole ? -1 : v > whole ? 1 : exact ? 0 : -1;
            result[i] = Decide(op, sign);
        }

        return result;
    }

    /// <summary>
    /// A timestamp column against an instant. The general path reads a value through
    /// <see cref="TimestampArray.GetTimestamp"/>, which truncates nanoseconds to ticks, so the
    /// nanosecond case truncates the same way before comparing; the coarser units scale exactly.
    /// The column's time zone never matters: an instant compares in UTC.
    /// </summary>
    private static bool?[]? CompareTimestamps(TimestampArray column, ComparisonOperator op, LiteralValue constant)
    {
        var unit = ((TimestampType)column.Data.DataType).Unit;
        switch (unit)
        {
            case TimeUnit.Second:
                return CompareInstants(column, column.Values, TimeSpan.TicksPerSecond, op, constant);
            case TimeUnit.Millisecond:
                return CompareInstants(column, column.Values, TimeSpan.TicksPerMillisecond, op, constant);
            case TimeUnit.Microsecond:
                return CompareInstants(column, column.Values, 10, op, constant);
            case TimeUnit.Nanosecond:
                break;
            default:
                return null;
        }

        if (!IsOrdering(op) || !TryAsInstant(constant, out var instant))
            return null;

        long target = instant.UtcTicks - Epoch.UtcTicks;
        var values = column.Values;
        int length = column.Length;
        var result = new bool?[length];
        bool hasNulls = column.NullCount > 0;
        bool? nullRow = NullRow(op);
        for (int i = 0; i < length; i++)
        {
            if (hasNulls && column.IsNull(i))
                result[i] = nullRow;
            else
                result[i] = Decide(op, (values[i] / 100).CompareTo(target));
        }

        return result;
    }

    private static bool TryAsInstant(LiteralValue constant, out DateTimeOffset instant)
    {
#if NET6_0_OR_GREATER
        return LiteralValue.TryAsInstant(constant, out instant);
#else
        if (constant.Type == LiteralValue.Kind.DateTimeOffset)
        {
            instant = constant.AsDateTimeOffset;
            return true;
        }

        instant = default;
        return false;
#endif
    }

    private static long FloorDivide(long value, long divisor)
    {
        long quotient = value / divisor;
        return value % divisor < 0 ? quotient - 1 : quotient;
    }
}

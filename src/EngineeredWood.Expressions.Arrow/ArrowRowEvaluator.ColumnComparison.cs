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

    private DirectAnswer? TryCompareColumnToConstant(ComparisonPredicate cmp, RecordBatch batch, bool asked)
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
            // As the batch holds it: a view or large string is compared in place, not copied (#859).
            column = columnExpression is UnboundReference u ? FindColumn(batch, u) : FindColumn(batch, ((BoundReference)columnExpression).Name);
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

        // Typed as GetColumn would convert it, which is what the general path analyses and coerces.
        var columnType = ByteArrayOffsets.TypeOf(column.Data.DataType);
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
        var answer = Kernel(column, op, constant);

        // The kernels read raw spans over the column's buffers -- Answer reads its null bitmap after
        // an allocation -- and a caller's array may be backed by native memory that its finalizer
        // frees. This one root covers every kernel. See doc/arrow-span-lifetime.md.
        GC.KeepAlive(column);

        if (answer is not null)
            DirectComparisons++;
        return answer;
    }

    /// <summary>
    /// The kernel for <paramref name="column"/>'s type, comparing every row against
    /// <paramref name="constant"/> (column on the left); null when it declines. Shared with a set
    /// membership test (#799), which asks each member under <c>=</c>. The caller roots the column.
    /// </summary>
    private static DirectAnswer? Kernel(IArrowArray column, ComparisonOperator op, LiteralValue constant) => column switch
    {
        StringArray a => CompareStrings(a, op, constant),
        LargeStringArray a => CompareStrings(a, new LargeStrings(a), op, constant),
        StringViewArray a => CompareStrings(a, new StringViews(a), op, constant),
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
        FunctionCall call => IsConstant(call) && AllCallsFoldable(call),
        _ => false,
    };

    /// <summary>
    /// Whether every call in a constant expression is one the registry promises is foldable, so that it
    /// may be evaluated once for the whole batch. <see cref="IsConstant"/> alone is structural -- it says
    /// no column is read -- and a registered function may still differ per row (<c>row_index()</c>).
    /// </summary>
    private bool AllCallsFoldable(Expression expression)
    {
        if (_functions is null || _functions is not IFoldableFunctions foldable)
            return false;

        switch (expression)
        {
            case LiteralExpression:
                return true;
            case FunctionCall call:
                if (!_functions.IsRegistered(call.Name) || !foldable.IsFoldable(call.Name))
                    return false;
                foreach (var argument in call.Arguments)
                {
                    if (!AllCallsFoldable(argument))
                        return false;
                }

                return true;
            default:
                // A nested predicate or anything else: not worth proving, so evaluated per row.
                return false;
        }
    }

    private static bool HasKernel(IArrowArray column) => column is
        StringArray or LargeStringArray or StringViewArray or Int8Array or Int16Array or Int32Array or Int64Array
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

    // ── The answer ──

    /// <summary>
    /// A direct comparison's answer, as the value and validity bitmaps of a <see cref="BooleanArray"/>
    /// (#608): one bit a row where a <c>bool?[]</c> took two bytes, which for a 50K-row batch put the
    /// mask on the large-object heap.
    /// </summary>
    private readonly struct DirectAnswer
    {
        private readonly byte[] _values;
        private readonly byte[]? _validity;
        private readonly int _nullCount;
        private readonly int _length;

        public DirectAnswer(byte[] values, byte[]? validity, int nullCount, int length)
        {
            _values = values;
            _validity = validity;
            _nullCount = nullCount;
            _length = length;
        }

        /// <summary>The value bits, a null row's cleared.</summary>
        public byte[] Values => _values;

        /// <summary>The validity bits, or null when every row is valid.</summary>
        public byte[]? Validity => _validity;

        public BooleanArray ToBooleanArray() => new(
            new ArrowBuffer(_values),
            _validity is null ? ArrowBuffer.Empty : new ArrowBuffer(_validity),
            _length, _nullCount, 0);

        /// <summary>For a compound predicate, which still combines its operands as <c>bool?[]</c>.</summary>
        public bool?[] ToNullable()
        {
            var result = new bool?[_length];
            for (int i = 0; i < _length; i++)
            {
                if (_validity is null || IsSet(_validity, i))
                    result[i] = IsSet(_values, i);
            }

            return result;
        }
    }

    private static int BitmapBytes(int length) => (length + 7) / 8;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsSet(byte[] bits, int i) => (bits[i >> 3] & (1 << (i & 7))) != 0;

    /// <summary>Sets row <paramref name="i"/>'s bit, without a branch on <paramref name="value"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Set(byte[] bits, int i, bool value) =>
        bits[i >> 3] |= (byte)((value ? 1 : 0) << (i & 7));

    /// <summary>The eight bits of a bitmap that start at any bit offset.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte ReadByte(ReadOnlySpan<byte> bitmap, int bitOffset)
    {
        int index = bitOffset >> 3, shift = bitOffset & 7;
        int bits = bitmap[index] >> shift;
        if (shift != 0 && index + 1 < bitmap.Length)
            bits |= bitmap[index + 1] << (8 - shift);
        return (byte)bits;
    }

    private static int PopCount(byte value)
    {
#if NET6_0_OR_GREATER
        return System.Numerics.BitOperations.PopCount(value);
#else
        int count = 0;
        for (; value != 0; value &= (byte)(value - 1))
            count++;
        return count;
#endif
    }

    /// <summary>
    /// Finishes a kernel's value bits with the column's nulls. A null row is null, except under
    /// <c>&lt;=&gt;</c>, where a null column against the (non-null) constant is false. Either way its
    /// value bit is cleared, so a kernel may compare every row without looking at the nulls.
    /// </summary>
    private static DirectAnswer Answer(IArrowArray column, ComparisonOperator op, byte[] values)
    {
        int length = column.Length;
        if (length == 0)
            return new DirectAnswer(values, null, 0, 0);

        int tail = length & 7;
        byte lastMask = tail == 0 ? (byte)0xFF : (byte)((1 << tail) - 1);
        values[values.Length - 1] &= lastMask;

        // NullCount -1 is "not counted", so the bitmap decides.
        var bitmap = column.Data.Buffers[0].Span;
        if (column.NullCount == 0 || bitmap.IsEmpty)
            return new DirectAnswer(values, null, 0, length);

        var validity = new byte[values.Length];
        int offset = column.Offset;
        int valid = 0;
        for (int b = 0; b < validity.Length; b++)
        {
            byte bits = ReadByte(bitmap, offset + b * 8);
            if (b == validity.Length - 1)
                bits &= lastMask;
            validity[b] = bits;
            values[b] &= bits;
            valid += PopCount(bits);
        }

        return valid == length || op == ComparisonOperator.NullSafeEqual
            ? new DirectAnswer(values, null, 0, length)
            : new DirectAnswer(values, validity, length - valid, length);
    }

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
    private static DirectAnswer? CompareStrings(StringArray column, ComparisonOperator op, LiteralValue constant)
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
        var result = new byte[BitmapBytes(length)];
        var offsets = column.ValueOffsets;
        var data = column.ValueBuffer.Span;
        bool hasNulls = column.NullCount != 0;   // -1 is "not counted", so probe
        var target = new ReadOnlySpan<byte>(literal);

        // Unlike a fixed-width slot, a string costs something to compare, so a null row is skipped.
        // Answer clears its bit either way.
        if (startsWith || op is ComparisonOperator.Equal or ComparisonOperator.NullSafeEqual or ComparisonOperator.NotEqual)
        {
            // Equality needs no order, and SequenceEqual stops at a length mismatch.
            bool want = op is ComparisonOperator.StartsWith or ComparisonOperator.Equal or ComparisonOperator.NullSafeEqual;
            for (int i = 0; i < length; i++)
            {
                if (hasNulls && column.IsNull(i))
                    continue;

                int start = offsets[i];
                var value = data.Slice(start, offsets[i + 1] - start);
                Set(result, i, (startsWith ? value.StartsWith(target) : value.SequenceEqual(target)) == want);
            }
        }
        else
        {
            for (int i = 0; i < length; i++)
            {
                if (hasNulls && column.IsNull(i))
                    continue;

                int start = offsets[i];
                Set(result, i, Decide(op, data.Slice(start, offsets[i + 1] - start).SequenceCompareTo(target)));
            }
        }

        return Answer(column, op, result);
    }

    /// <summary>A string column's values by row, for a layout without 32-bit offsets (#859).</summary>
    private interface IStringValues
    {
        ReadOnlySpan<byte> Get(int index);
    }

    private readonly struct LargeStrings(LargeStringArray array) : IStringValues
    {
        public ReadOnlySpan<byte> Get(int index) => array.GetBytes(index);
    }

    private readonly struct StringViews(StringViewArray array) : IStringValues
    {
        public ReadOnlySpan<byte> Get(int index) => array.GetBytes(index);
    }

    /// <summary>
    /// <see cref="CompareStrings(StringArray, ComparisonOperator, LiteralValue)"/> over a <c>large_utf8</c> or
    /// <c>utf8view</c> column, read in place rather than copied into offsets first (#859). Same rules, same answers.
    /// </summary>
    private static DirectAnswer? CompareStrings<TValues>(
        IArrowArray column, TValues values, ComparisonOperator op, LiteralValue constant)
        where TValues : struct, IStringValues
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
        var result = new byte[BitmapBytes(length)];
        bool hasNulls = column.NullCount != 0;   // -1 is "not counted", so probe
        var target = new ReadOnlySpan<byte>(literal);

        if (startsWith || op is ComparisonOperator.Equal or ComparisonOperator.NullSafeEqual or ComparisonOperator.NotEqual)
        {
            bool want = op is ComparisonOperator.StartsWith or ComparisonOperator.Equal or ComparisonOperator.NullSafeEqual;
            for (int i = 0; i < length; i++)
            {
                if (hasNulls && column.IsNull(i))
                    continue;

                var value = values.Get(i);
                Set(result, i, (startsWith ? value.StartsWith(target) : value.SequenceEqual(target)) == want);
            }
        }
        else
        {
            for (int i = 0; i < length; i++)
            {
                if (hasNulls && column.IsNull(i))
                    continue;

                Set(result, i, Decide(op, values.Get(i).SequenceCompareTo(target)));
            }
        }

        return Answer(column, op, result);
    }

    /// <summary>
    /// An integral column, every value of which <see cref="LiteralValue.TryAsInt64"/> accepts, against
    /// an integral, floating or decimal constant -- the three branches
    /// <see cref="LiteralValue.CompareTo(LiteralValue)"/> takes for those pairs, in its order.
    /// </summary>
    /// <remarks>
    /// This and the other fixed-width kernels compare every row, null or not: a null slot still holds
    /// some value, and <see cref="Answer"/> clears its bit.
    /// </remarks>
    private static DirectAnswer? CompareIntegers<T>(
        IArrowArray column, ReadOnlySpan<T> values, ComparisonOperator op, LiteralValue constant)
        where T : struct
    {
        if (!IsOrdering(op))
            return null;

        int length = column.Length;
        var result = new byte[BitmapBytes(length)];

        if (LiteralValue.TryAsInt64(constant, out long target))
        {
            for (int i = 0; i < length; i++)
                Set(result, i, Decide(op, ToInt64(values[i]).CompareTo(target)));
            return Answer(column, op, result);
        }

        if (LiteralValue.IsFloating(constant.Type) && LiteralValue.TryAsDouble(constant, out double floating))
        {
            for (int i = 0; i < length; i++)
                Set(result, i, Decide(op, LiteralValue.CompareFloating(ToInt64(values[i]), floating)));
            return Answer(column, op, result);
        }

        if (constant.Type == LiteralValue.Kind.Decimal)
        {
            // Exact, as CompareScaledIntegers is: every long is a decimal.
            decimal exact = constant.AsDecimal;
            for (int i = 0; i < length; i++)
                Set(result, i, Decide(op, ((decimal)ToInt64(values[i])).CompareTo(exact)));
            return Answer(column, op, result);
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
    private static DirectAnswer? CompareFloating<T>(
        IArrowArray column, ReadOnlySpan<T> values, ComparisonOperator op, LiteralValue constant)
        where T : struct
    {
        if (!IsOrdering(op) || !LiteralValue.TryAsDouble(constant, out double target))
            return null;

        int length = column.Length;
        var result = new byte[BitmapBytes(length)];
        for (int i = 0; i < length; i++)
            Set(result, i, Decide(op, LiteralValue.CompareFloating(ToDouble(values[i]), target)));
        return Answer(column, op, result);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static double ToDouble<T>(T value) where T : struct
    {
        if (typeof(T) == typeof(double)) return Unsafe.As<T, double>(ref value);
        if (typeof(T) == typeof(float)) return Unsafe.As<T, float>(ref value);
        throw new NotSupportedException(typeof(T).Name);
    }

    /// <summary>
    /// A boolean column, eight rows at a time. A row's answer depends only on its own bit, so the
    /// answer is the column's set bits where the answer for <c>true</c> holds and its clear bits where
    /// the answer for <c>false</c> does.
    /// </summary>
    private static DirectAnswer? CompareBooleans(BooleanArray column, ComparisonOperator op, LiteralValue constant)
    {
        if (!IsOrdering(op) || constant.Type != LiteralValue.Kind.Boolean)
            return null;

        bool target = constant.AsBoolean;
        int ifTrue = Decide(op, true.CompareTo(target)) ? 0xFF : 0;
        int ifFalse = Decide(op, false.CompareTo(target)) ? 0xFF : 0;

        var bits = column.ValueBuffer.Span;
        int offset = column.Offset;
        var result = new byte[BitmapBytes(column.Length)];
        for (int b = 0; b < result.Length; b++)
        {
            int v = ReadByte(bits, offset + b * 8);
            result[b] = (byte)((v & ifTrue) | (~v & ifFalse));
        }

        return Answer(column, op, result);
    }

    /// <summary>
    /// A date column, read by the general path as UTC midnight of the day, against an instant: a
    /// <see cref="DateTimeOffset"/>, or a <see cref="DateOnly"/>, which compares as UTC midnight too.
    /// </summary>
    /// <remarks>
    /// Compared in the column's own unit against the constant's offset from the epoch, split once
    /// into whole units and a remainder, so no row is scaled into ticks and nothing can overflow.
    /// </remarks>
    private static DirectAnswer? CompareInstants<T>(
        IArrowArray column, ReadOnlySpan<T> values, long ticksPerUnit, ComparisonOperator op, LiteralValue constant)
        where T : struct
    {
        if (!IsOrdering(op) || !TryAsInstant(constant, out var instant))
            return null;

        long offset = instant.UtcTicks - Epoch.UtcTicks;
        long whole = FloorDivide(offset, ticksPerUnit);
        bool exact = offset - whole * ticksPerUnit == 0;

        int length = column.Length;
        var result = new byte[BitmapBytes(length)];
        for (int i = 0; i < length; i++)
        {
            long v = ToInt64(values[i]);
            int sign = v < whole ? -1 : v > whole ? 1 : exact ? 0 : -1;
            Set(result, i, Decide(op, sign));
        }

        return Answer(column, op, result);
    }

    /// <summary>
    /// A timestamp column against an instant. The general path reads a value through
    /// <see cref="TimestampArray.GetTimestamp"/>, which truncates nanoseconds to ticks, so the
    /// nanosecond case truncates the same way before comparing; the coarser units scale exactly.
    /// The column's time zone never matters: an instant compares in UTC.
    /// </summary>
    private static DirectAnswer? CompareTimestamps(TimestampArray column, ComparisonOperator op, LiteralValue constant)
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
        var result = new byte[BitmapBytes(length)];
        for (int i = 0; i < length; i++)
            Set(result, i, Decide(op, (values[i] / 100).CompareTo(target)));
        return Answer(column, op, result);
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

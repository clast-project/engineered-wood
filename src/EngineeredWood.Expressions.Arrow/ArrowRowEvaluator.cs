// Copyright (c) Curt Hagenlocher. All rights reserved.
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

    public ArrowRowEvaluator(IFunctionRegistry? functions = null)
    {
        _functions = functions;
    }

    public BooleanArray EvaluatePredicate(Predicate predicate, RecordBatch batch)
    {
        var result = EvalPredicate(predicate, batch);
        return ToBooleanArray(result, batch.Length);
    }

    public IArrowArray EvaluateExpression(Expression expression, RecordBatch batch)
    {
        var values = EvalExpression(expression, batch);
        return MaterializeAsArray(values, batch.Length);
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
        var left = EvalExpression(cmp.Left, batch);
        var right = EvalExpression(cmp.Right, batch);
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
                // Type-incompatible compare → null, like SQL.
                result[i] = null;
            }
        }
        return result;
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
        var operand = EvalExpression(set.Operand, batch);
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
            foreach (var lit in set.Values)
            {
                if (lit.IsNull) { sawNullInList = true; continue; }
                try
                {
                    if (v.Value.CompareTo(lit) == 0) { found = true; break; }
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
                if (_functions is null || !_functions.IsRegistered(fc.Name))
                    throw new InvalidOperationException(
                        $"No function registered for '{fc.Name}'. " +
                        "Provide an IFunctionRegistry to ArrowRowEvaluator.");

                var argArrays = new IArrowArray[fc.Arguments.Count];
                for (int i = 0; i < fc.Arguments.Count; i++)
                    argArrays[i] = MaterializeAsArray(
                        EvalExpression(fc.Arguments[i], batch), batch.Length);

                var result = _functions.Invoke(fc.Name, argArrays, batch.Length);
                return ArrowToLiteralValues(result, batch.Length);

            default:
                throw new NotSupportedException(
                    $"Unsupported expression: {expression.GetType().Name}");
        }
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

    // A decimal column value: the common in-range case as System.Decimal (how a decimal literal and a
    // stats decoder also represent it); a value that overflows System.Decimal falls back to its exact
    // unscaled BigInteger plus the column's scale, read straight from the fixed-width little-endian value
    // buffer — the same raw layout the format writers use.
    private static LiteralValue DecimalLiteral(Decimal128Array a, int index)
    {
        try { return LiteralValue.Of(a.GetValue(index)!.Value); }
        catch (OverflowException)
        {
            int scale = ((Decimal128Type)a.Data.DataType).Scale;
            return LiteralValue.HighPrecisionDecimalOf(
                ToBigInteger(a.ValueBuffer.Span.Slice(index * 16, 16)), scale);
        }
    }

    private static LiteralValue DecimalLiteral(Decimal256Array a, int index)
    {
        try { return LiteralValue.Of(a.GetValue(index)!.Value); }
        catch (OverflowException)
        {
            int scale = ((Decimal256Type)a.Data.DataType).Scale;
            return LiteralValue.HighPrecisionDecimalOf(
                ToBigInteger(a.ValueBuffer.Span.Slice(index * 32, 32)), scale);
        }
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

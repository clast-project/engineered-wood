// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Arrow;

/// <summary>
/// Gathers a selection of rows out of an Arrow array — the equivalent of Arrow's <c>take</c> kernel,
/// which Apache.Arrow for .NET does not ship.
///
/// <para>Buffers are built directly rather than through the typed <c>XArray.Builder</c> classes. That
/// matters for more than speed: a builder round-trips each value through its .NET surface type, so a
/// timestamp becomes a <see cref="DateTimeOffset"/> and a decimal becomes a <see cref="decimal"/> on the
/// way through. Both of those conversions are lossy, and the builder for a narrower type also silently
/// discards the source's type parameters (unit, timezone, precision, scale). Copying the raw value slots
/// keeps every column bit-identical and type-identical to its source.</para>
///
/// <para>Callers that need a type this cannot gather get a <see cref="NotSupportedException"/>. Never
/// relax that into passing the column through unfiltered: an unfiltered column has the wrong length for
/// the batch it lands in, which mispairs rows and overruns read buffers downstream.</para>
/// </summary>
public static class ArrowCompute
{
    /// <summary>
    /// Builds a new array holding <paramref name="indices"/> — logical row positions in
    /// <paramref name="source"/>, in the order given, duplicates allowed.
    /// </summary>
    public static IArrowArray Take(IArrowArray source, ReadOnlySpan<int> indices)
    {
        switch (source)
        {
            // Extension arrays (VARIANT, GUID, ...) gather through their STORAGE and are re-wrapped, so the
            // extension annotation survives. Must precede the storage-shaped cases below: an extension over a
            // primitive would otherwise be gathered down to bare storage and land in a batch whose schema
            // still claims the extension type.
            case ExtensionArray ext:
                return ((ExtensionType)ext.Data.DataType).CreateArray(Take(ext.Storage, indices));

            // A null array carries no buffers, so gathering is just a length change.
            case NullArray:
                return new NullArray(indices.Length);

            // Boolean values are bit-packed, so they cannot be copied by value slot.
            case BooleanArray a:
                return TakeBoolean(a, indices);

            // StringArray derives from BinaryArray (and LargeStringArray from LargeBinaryArray), so these
            // four cases share a helper. Each passes source.Data.DataType through rather than a hardcoded
            // default, which is what keeps a Large* column from being narrowed to its 32-bit counterpart.
            case StringArray a:
                return TakeVarBinary32(a, indices);
            case BinaryArray a:
                return TakeVarBinary32(a, indices);
            case LargeStringArray a:
                return TakeVarBinary64(a, indices);
            case LargeBinaryArray a:
                return TakeVarBinary64(a, indices);

            case StructArray a:
                return TakeStruct(a, indices);

            // A map IS a list of key/value entry structs, and Apache.Arrow models that literally: MapArray
            // DERIVES FROM ListArray (measured), so `case ListArray` on its own would match maps too. The map
            // arm is spelled out separately and FIRST so the derived type cannot be swallowed by the base one
            // — and because they are ordered this way, the compiler enforces it: putting the list arm first
            // makes this one unreachable and fails the build with CS8120 (measured). What both arms rely on
            // is passing source.Data.DataType through, which is what keeps a map a map rather than the plain
            // list its layout looks like. Note this is the ARRAY hierarchy only — the TYPES are unrelated
            // (MapType is not a ListType), which DeltaLake's
            // MapType_IsNotAListType_SoTheWalkArmOrderIsIrrelevant pins for type-driven walks.
            case MapArray a:
                return TakeList32(a, indices);
            case ListArray a:
                return TakeList32(a, indices);

            // Kept distinct from the 32-bit arm for the same reason as LargeString/LargeBinary: rebuilding a
            // LargeList through the narrow path would contradict the schema and overflow past 2^31 children.
            case LargeListArray a:
                return TakeList64(a, indices);

            case FixedSizeListArray a:
                return TakeFixedSizeList(a, indices);

            default:
            {
                // Every fixed-width type — signed/unsigned ints, floats, HalfFloat, Date/Time/Timestamp/
                // Duration/Interval, Decimal32/64/128/256 and FixedSizeBinary — is gathered by copying raw
                // value-slot bytes. This is why there is no per-type case here: the width is all the copy
                // needs, and the source's own DataType is reused verbatim.
                int? width = FixedWidthBytes(source.Data.DataType);
                if (width is int byteWidth)
                    return TakeFixedWidth(source, indices, byteWidth);

                throw new NotSupportedException(
                    $"ArrowCompute cannot gather rows from a column of type {source.Data.DataType.TypeId}.");
            }
        }
    }

    /// <summary>
    /// <see cref="Take(IArrowArray, ReadOnlySpan{int})"/> for callers holding a <see cref="List{T}"/>.
    /// </summary>
    public static IArrowArray Take(IArrowArray source, List<int> indices) =>
#if NET6_0_OR_GREATER
        Take(source, CollectionsMarshal.AsSpan(indices));
#else
        Take(source, indices.ToArray());
#endif

    /// <summary>
    /// Gathers the same rows from every column of <paramref name="batch"/>, returning a batch that declares
    /// <paramref name="schema"/>. Column count and order are preserved.
    /// </summary>
    public static RecordBatch Take(
        RecordBatch batch, Apache.Arrow.Schema schema, ReadOnlySpan<int> indices)
    {
        var columns = new IArrowArray[batch.ColumnCount];
        for (int c = 0; c < batch.ColumnCount; c++)
            columns[c] = Take(batch.Column(c), indices);

        return new RecordBatch(schema, columns, indices.Length);
    }

    /// <summary>
    /// <see cref="Take(RecordBatch, Apache.Arrow.Schema, ReadOnlySpan{int})"/> for callers holding a
    /// <see cref="List{T}"/>.
    /// </summary>
    public static RecordBatch Take(
        RecordBatch batch, Apache.Arrow.Schema schema, List<int> indices) =>
#if NET6_0_OR_GREATER
        Take(batch, schema, CollectionsMarshal.AsSpan(indices));
#else
        Take(batch, schema, indices.ToArray());
#endif

    /// <summary>
    /// Builds an all-NULL array of <paramref name="type"/> and <paramref name="length"/> — every row null,
    /// with the type reproduced exactly, including the parameters a typed builder for a narrower type would
    /// discard (timestamp unit and timezone, decimal precision and scale, list and struct child types).
    ///
    /// <para>Used to backfill a column that is absent from a data file but present in the schema it must be
    /// read under. The type fidelity is the point: a null column built as the wrong Arrow type contradicts
    /// the schema of the batch it lands in, which is a silent mismatch rather than a failure.</para>
    ///
    /// <para>Nested types recurse — a struct's children are all-null arrays of the same length, and a list's
    /// offsets are all zero over an empty child, so every row is a null rather than an empty list.</para>
    /// </summary>
    /// <exception cref="NotSupportedException">
    /// <paramref name="type"/> is one this cannot construct. Never substitute another type for it: that is
    /// the silent schema mismatch above.
    /// </exception>
    public static IArrowArray MakeNullArray(IArrowType type, int length)
    {
        switch (type)
        {
            // Extension types first, for the same reason as in Take: an extension over a struct or primitive
            // storage type (VARIANT) would otherwise be built as its bare storage and land in a batch whose
            // schema still declares the extension.
            case ExtensionType ext:
                return ext.CreateArray(MakeNullArray(ext.StorageType, length));

            // A null array is all-null by construction and carries no buffers to fill.
            case NullType:
                return new NullArray(length);

#if !NET6_0_OR_GREATER
            // Apache.Arrow cannot construct a HalfFloatArray on netstandard2.0 at all — BuildArray throws
            // "Half-float arrays are not supported by this target framework". Take needs no such guard
            // because a target that cannot represent the array cannot hand one over to be gathered; this
            // method is reached from a SCHEMA, so a caller genuinely can ask for the impossible. Say why
            // rather than surfacing Arrow's message from inside a backfill.
            case HalfFloatType:
                throw new NotSupportedException(
                    "An all-null HalfFloat column cannot be built on this target framework, because "
                    + "Apache.Arrow cannot construct a HalfFloatArray here at all. Use net6.0 or later.");
#endif

            case BooleanType:
                // The values bitmap is zeroed alongside the validity bitmap. Every value bit is unset, so a
                // consumer that reads values without consulting validity sees false rather than garbage.
                return BuildNull(type, length, new[] { AllNullBitmap(length), Zeros(BitmapBytes(length)) });

            case StringType or BinaryType:
                return BuildNull(type, length, new[]
                {
                    AllNullBitmap(length), Zeros((length + 1) * sizeof(int)), ArrowBuffer.Empty,
                });

            // Kept distinct from the 32-bit arm so a Large* column is built as Large*: narrowing it would
            // contradict the schema, the same trap TakeVarBinary64 exists to avoid.
            case LargeStringType or LargeBinaryType:
                return BuildNull(type, length, new[]
                {
                    AllNullBitmap(length), Zeros((length + 1) * sizeof(long)), ArrowBuffer.Empty,
                });

            // All-zero offsets make every row a zero-length slice of an empty child — which, combined with
            // the cleared validity bit, reads as NULL rather than as an empty list.
            case ListType lt:
                return BuildNull(
                    type, length,
                    new[] { AllNullBitmap(length), Zeros((length + 1) * sizeof(int)) },
                    new[] { MakeNullArray(lt.ValueDataType, 0).Data });

            case LargeListType llt:
                return BuildNull(
                    type, length,
                    new[] { AllNullBitmap(length), Zeros((length + 1) * sizeof(long)) },
                    new[] { MakeNullArray(llt.ValueDataType, 0).Data });

            // A fixed-size list has no offsets buffer: the child occupies length * ListSize slots whether or
            // not the parent rows are null, so the child has to be sized accordingly rather than left empty.
            case FixedSizeListType flt:
                return BuildNull(
                    type, length,
                    new[] { AllNullBitmap(length) },
                    new[] { MakeNullArray(flt.ValueDataType, length * flt.ListSize).Data });

            // A map is a list of key/value entry structs, so it takes the list layout with an empty child.
            // Apache.Arrow models MapType as unrelated to ListType, so arm order between them is irrelevant
            // (pinned by DeltaLake's MapType_IsNotAListType_SoTheWalkArmOrderIsIrrelevant).
            case MapType mt:
                return BuildNull(
                    type, length,
                    new[] { AllNullBitmap(length), Zeros((length + 1) * sizeof(int)) },
                    new[] { MakeNullArray(mt.KeyValueType, 0).Data });

            // Children are parallel to the parent's rows, so each is an all-null array of the same length.
            case StructType st:
            {
                var children = new ArrayData[st.Fields.Count];
                for (int i = 0; i < children.Length; i++)
                    children[i] = MakeNullArray(st.Fields[i].DataType, length).Data;

                return BuildNull(type, length, new[] { AllNullBitmap(length) }, children);
            }

            default:
            {
                // Every fixed-width type — ints, floats, Date/Time/Timestamp/Duration/Interval,
                // Decimal32/64/128/256 and FixedSizeBinary — is a zeroed value buffer of the right size.
                // The source type is reused verbatim, so its unit/timezone/precision/scale ride along.
                int? width = FixedWidthBytes(type);
                if (width is int byteWidth)
                {
                    return BuildNull(
                        type, length, new[] { AllNullBitmap(length), Zeros(length * byteWidth) });
                }

                throw new NotSupportedException(
                    $"ArrowCompute cannot build an all-null column of type {type.TypeId}.");
            }
        }
    }

    /// <summary>
    /// Builds an array of <paramref name="length"/> rows all holding the same non-null value, given that
    /// value's raw Arrow encoding. The inverse of <see cref="MakeNullArray"/>: no validity buffer is
    /// allocated and the null count is zero, which is what lets Arrow skip the per-element validity check
    /// downstream.
    ///
    /// <para>Taking raw bytes rather than a .NET value is what keeps this lossless. A constant built through
    /// a typed builder round-trips through the builder's surface type — a decimal wider than
    /// <see cref="decimal"/>'s 28 digits cannot survive that, and a timestamp is re-derived from a
    /// <see cref="DateTimeOffset"/> — whereas these bytes are stored verbatim and the type is reused as
    /// given, so unit, timezone, precision and scale all ride along.</para>
    /// </summary>
    /// <param name="value">
    /// The value's Arrow encoding: for a fixed-width type, exactly one value slot, little-endian (so
    /// <c>FixedWidthBytes(type)</c> bytes); for String/Binary and their Large twins, the encoded bytes, of
    /// any length; for Boolean, a single byte, zero for false and non-zero for true.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="value"/> is not the width the type requires.</exception>
    /// <exception cref="NotSupportedException">The type is one this cannot build a constant of.</exception>
    public static IArrowArray Repeat(IArrowType type, ReadOnlySpan<byte> value, int length)
    {
        switch (type)
        {
            // Extension types first, matching Take and MakeNullArray: the constant is built as the storage
            // type and re-wrapped, so the annotation survives.
            case ExtensionType ext:
                return ext.CreateArray(Repeat(ext.StorageType, value, length));

            case BooleanType:
            {
                if (value.Length != 1)
                {
                    throw new ArgumentException(
                        $"A Boolean constant needs exactly 1 byte, got {value.Length}.", nameof(value));
                }

                // Bit-packed, so a true constant is an all-ones bitmap. Bits past `length` are padding and
                // are never read, so setting them costs nothing and keeps this a single Fill.
                var bits = new byte[BitmapBytes(length)];
                if (value[0] != 0)
                    bits.AsSpan().Fill(0xFF);

                return new BooleanArray(new ArrayData(
                    BooleanType.Default, length, nullCount: 0, offset: 0,
                    new[] { ArrowBuffer.Empty, new ArrowBuffer(bits) }));
            }

            case StringType or BinaryType:
                return RepeatVarBinary(type, value, length, largeOffsets: false);

            case LargeStringType or LargeBinaryType:
                return RepeatVarBinary(type, value, length, largeOffsets: true);

            default:
            {
                int? width = FixedWidthBytes(type);
                if (width is int byteWidth)
                {
                    if (value.Length != byteWidth)
                    {
                        throw new ArgumentException(
                            $"A constant of type {type.TypeId} needs exactly {byteWidth} bytes, "
                            + $"got {value.Length}.", nameof(value));
                    }

                    var values = new byte[length * byteWidth];
                    FillRepeating(values, value);

                    return ArrowArrayFactory.BuildArray(new ArrayData(
                        type, length, nullCount: 0, offset: 0,
                        new[] { ArrowBuffer.Empty, new ArrowBuffer(values) }));
                }

                throw new NotSupportedException(
                    $"ArrowCompute cannot build a constant column of type {type.TypeId}.");
            }
        }
    }

    /// <summary>
    /// Widens a fixed-width column to a wider type, losslessly — a value-slot copy per row rather than a
    /// round-trip through either type's .NET surface representation.
    ///
    /// <para>Supported: the integer ladder (Int8 → Int16 → Int32 → Int64), an unsigned source into any
    /// signed type that can hold its whole range (UInt8 → Int16, UInt16 → Int32, UInt32 → Int64, …),
    /// Int8/Int16/Int32 → Double, Float → Double, and Date32 → Timestamp at any unit. Every one of those is
    /// exactly representable in the target, which is what makes this a widening rather than a cast: nothing
    /// here can round.</para>
    ///
    /// <para>Narrowing conversions are deliberately absent rather than throwing at runtime for some values
    /// and succeeding for others — an unlisted pair is a <see cref="NotSupportedException"/>.</para>
    /// </summary>
    public static IArrowArray Widen(IArrowArray source, IArrowType targetType) =>
        (source.Data.DataType, targetType) switch
        {
            (Int8Type, Int16Type) => WidenSlots<sbyte, short, SByteToInt16>(source, targetType),
            (Int8Type, Int32Type) => WidenSlots<sbyte, int, SByteToInt32>(source, targetType),
            (Int8Type, Int64Type) => WidenSlots<sbyte, long, SByteToInt64>(source, targetType),
            (Int8Type, DoubleType) => WidenSlots<sbyte, double, SByteToDouble>(source, targetType),

            (Int16Type, Int32Type) => WidenSlots<short, int, Int16ToInt32>(source, targetType),
            (Int16Type, Int64Type) => WidenSlots<short, long, Int16ToInt64>(source, targetType),
            (Int16Type, DoubleType) => WidenSlots<short, double, Int16ToDouble>(source, targetType),

            (Int32Type, Int64Type) => WidenSlots<int, long, Int32ToInt64>(source, targetType),
            (Int32Type, DoubleType) => WidenSlots<int, double, Int32ToDouble>(source, targetType),

            // Unsigned sources are ZERO-extended into the next signed type that holds their whole range —
            // every value stays positive, so the target's signed ordering agrees with the source's unsigned
            // one. (An unsigned source into the SAME-width signed type would wrap, so those pairs are absent.)
            (UInt8Type, Int16Type) => WidenSlots<byte, short, ByteToInt16>(source, targetType),
            (UInt8Type, Int32Type) => WidenSlots<byte, int, ByteToInt32>(source, targetType),
            (UInt8Type, Int64Type) => WidenSlots<byte, long, ByteToInt64>(source, targetType),

            (UInt16Type, Int32Type) => WidenSlots<ushort, int, UInt16ToInt32>(source, targetType),
            (UInt16Type, Int64Type) => WidenSlots<ushort, long, UInt16ToInt64>(source, targetType),

            (UInt32Type, Int64Type) => WidenSlots<uint, long, UInt32ToInt64>(source, targetType),

            (FloatType, DoubleType) => WidenSlots<float, double, FloatToDouble>(source, targetType),

            (Date32Type, TimestampType ts) => WidenDate32ToTimestamp(source, ts),

            _ => throw new NotSupportedException(
                $"ArrowCompute cannot widen a column of type {source.Data.DataType.TypeId} to "
                + $"{targetType.TypeId}."),
        };

    /// <summary>
    /// One value's widening conversion. Taken as a <c>struct</c> type parameter rather than a delegate so the
    /// JIT specializes each instantiation and inlines <see cref="Apply"/>, instead of paying an indirect call
    /// per element — which on a 64K-row batch is the difference between the copy and the dispatch dominating.
    /// </summary>
    private interface IWideningOp<TFrom, TTo>
    {
        TTo Apply(TFrom value);
    }

    // Each conversion is a C# implicit numeric conversion, i.e. guaranteed lossless by the language.
    private readonly struct SByteToInt16 : IWideningOp<sbyte, short> { public short Apply(sbyte v) => v; }
    private readonly struct SByteToInt32 : IWideningOp<sbyte, int> { public int Apply(sbyte v) => v; }
    private readonly struct SByteToInt64 : IWideningOp<sbyte, long> { public long Apply(sbyte v) => v; }
    private readonly struct SByteToDouble : IWideningOp<sbyte, double> { public double Apply(sbyte v) => v; }
    private readonly struct Int16ToInt32 : IWideningOp<short, int> { public int Apply(short v) => v; }
    private readonly struct Int16ToInt64 : IWideningOp<short, long> { public long Apply(short v) => v; }
    private readonly struct Int16ToDouble : IWideningOp<short, double> { public double Apply(short v) => v; }
    private readonly struct Int32ToInt64 : IWideningOp<int, long> { public long Apply(int v) => v; }
    private readonly struct Int32ToDouble : IWideningOp<int, double> { public double Apply(int v) => v; }
    private readonly struct ByteToInt16 : IWideningOp<byte, short> { public short Apply(byte v) => v; }
    private readonly struct ByteToInt32 : IWideningOp<byte, int> { public int Apply(byte v) => v; }
    private readonly struct ByteToInt64 : IWideningOp<byte, long> { public long Apply(byte v) => v; }
    private readonly struct UInt16ToInt32 : IWideningOp<ushort, int> { public int Apply(ushort v) => v; }
    private readonly struct UInt16ToInt64 : IWideningOp<ushort, long> { public long Apply(ushort v) => v; }
    private readonly struct UInt32ToInt64 : IWideningOp<uint, long> { public long Apply(uint v) => v; }
    private readonly struct FloatToDouble : IWideningOp<float, double> { public double Apply(float v) => v; }

    private static IArrowArray WidenSlots<TFrom, TTo, TOp>(IArrowArray source, IArrowType targetType)
        where TFrom : struct
        where TTo : struct
        where TOp : struct, IWideningOp<TFrom, TTo>
    {
        int length = source.Length;

        // Slicing by the logical offset here is what lets the result carry offset 0 — see AlignedValidity
        // for the matching half of that decision on the bitmap.
        ReadOnlySpan<TFrom> src = MemoryMarshal
            .Cast<byte, TFrom>(source.Data.Buffers[1].Span)
            .Slice(source.Data.Offset, length);

        // Every slot is written below — including the ones under nulls, whose source value is copied like any
        // other rather than being skipped — so the buffer needs no zero-fill first.
        var values = Uninitialized<byte>(length * Unsafe.SizeOf<TTo>());
        Span<TTo> dst = MemoryMarshal.Cast<byte, TTo>(values.AsSpan());

        var op = default(TOp);
        for (int i = 0; i < length; i++)
            dst[i] = op.Apply(src[i]);

        return BuildWidened(source, targetType, values);
    }

    /// <summary>
    /// Date32 holds a day count, so widening it to a timestamp is a multiplication by the target unit's
    /// ticks-per-day — not a trip through <see cref="DateTimeOffset"/> per row. Unlike a raw stored timestamp,
    /// a day count carries no unit of its own to misread, and a date has no sub-day precision to lose.
    /// </summary>
    private static IArrowArray WidenDate32ToTimestamp(IArrowArray source, TimestampType targetType)
    {
        long perDay = targetType.Unit switch
        {
            TimeUnit.Second => 86_400L,
            TimeUnit.Millisecond => 86_400_000L,
            TimeUnit.Microsecond => 86_400_000_000L,
            TimeUnit.Nanosecond => 86_400_000_000_000L,
            _ => throw new NotSupportedException(
                $"ArrowCompute cannot widen Date32 to a timestamp with {targetType.Unit} precision."),
        };

        int length = source.Length;
        ReadOnlySpan<int> days = MemoryMarshal
            .Cast<byte, int>(source.Data.Buffers[1].Span)
            .Slice(source.Data.Offset, length);

        // Every slot is written below, so no zero-fill is needed first.
        var values = Uninitialized<byte>(length * sizeof(long));
        Span<long> dst = MemoryMarshal.Cast<byte, long>(values.AsSpan());

        for (int i = 0; i < length; i++)
        {
            // Checked: a far-future date at nanosecond precision genuinely overflows (Int64 nanoseconds run
            // out in 2262), and wrapping silently would be worse than failing.
            dst[i] = checked(days[i] * perDay);
        }

        return BuildWidened(source, targetType, values);
    }

    private static IArrowArray BuildWidened(IArrowArray source, IArrowType targetType, byte[] values)
    {
        var (validity, nullCount) = AlignedValidity(source);
        return ArrowArrayFactory.BuildArray(new ArrayData(
            targetType, source.Length, nullCount, offset: 0,
            new[] { validity, new ArrowBuffer(values) }));
    }

    /// <summary>
    /// The source's validity, restated for an array whose logical row 0 is its physical slot 0.
    ///
    /// <para>Widening never changes which rows are null, so when the source already starts at offset 0 its
    /// bitmap is returned <b>by reference</b> — <see cref="ArrowBuffer"/> is a readonly struct over
    /// <see cref="ReadOnlyMemory{T}"/>, so that is safe and skips rebuilding it entirely.</para>
    ///
    /// <para>At a non-zero offset it must NOT be reused: a validity bitmap is indexed by PHYSICAL slot, so
    /// sharing it while the values have been re-based to 0 would read every row's null flag from the wrong
    /// bit — silently, and only for sliced arrays. The bitmap is re-derived instead. The same path handles a
    /// declared-but-unknown null count, which has to be counted rather than trusted.</para>
    /// </summary>
    private static (ArrowBuffer Validity, int NullCount) AlignedValidity(IArrowArray source)
    {
        int declared = source.NullCount;

        // No nulls: an absent bitmap means all-valid, which is also what lets Arrow skip the per-element check.
        if (declared == 0 || source.Data.Buffers[0].Length == 0)
            return (ArrowBuffer.Empty, 0);

        if (source.Data.Offset == 0 && declared > 0)
            return (source.Data.Buffers[0], declared);

        int length = source.Length;
        var bits = new byte[BitmapBytes(length)];
        bits.AsSpan().Fill(0xFF);

        int nulls = 0;
        for (int i = 0; i < length; i++)
        {
            // IsNull applies the array's own offset, so i stays logical.
            if (source.IsNull(i))
            {
                bits[i >> 3] &= (byte)~(1 << (i & 7));
                nulls++;
            }
        }

        return nulls == 0 ? (ArrowBuffer.Empty, 0) : (new ArrowBuffer(bits), nulls);
    }

    /// <summary>
    /// The variable-width constant: the value bytes repeated, over offsets that are simply
    /// <c>i * value.Length</c> — no scan of the values is needed to derive them.
    /// </summary>
    private static IArrowArray RepeatVarBinary(
        IArrowType type, ReadOnlySpan<byte> value, int length, bool largeOffsets)
    {
        long total = (long)length * value.Length;
        if (total > int.MaxValue)
        {
            throw new ArgumentException(
                $"A constant of {value.Length} bytes repeated {length} times needs {total} bytes, which "
                + "overflows a single Arrow buffer.", nameof(value));
        }

        var values = new byte[total];
        FillRepeating(values, value);

        byte[] offsetBytes;
        if (largeOffsets)
        {
            var offsets = new long[length + 1];
            for (int i = 0; i <= length; i++)
                offsets[i] = (long)i * value.Length;
            offsetBytes = new byte[(length + 1) * sizeof(long)];
            MemoryMarshal.AsBytes(offsets.AsSpan()).CopyTo(offsetBytes);
        }
        else
        {
            var offsets = new int[length + 1];
            for (int i = 0; i <= length; i++)
                offsets[i] = i * value.Length;
            offsetBytes = new byte[(length + 1) * sizeof(int)];
            MemoryMarshal.AsBytes(offsets.AsSpan()).CopyTo(offsetBytes);
        }

        return ArrowArrayFactory.BuildArray(new ArrayData(
            type, length, nullCount: 0, offset: 0,
            new[] { ArrowBuffer.Empty, new ArrowBuffer(offsetBytes), new ArrowBuffer(values) }));
    }

    /// <summary>
    /// Tiles <paramref name="pattern"/> across <paramref name="destination"/> by repeatedly doubling the
    /// filled prefix, so a long run costs a handful of large block copies instead of one copy per repetition.
    /// </summary>
    private static void FillRepeating(Span<byte> destination, ReadOnlySpan<byte> pattern)
    {
        // Also covers a zero-length value: the destination is then empty too.
        if (destination.IsEmpty)
            return;

        if (pattern.Length == 1)
        {
            destination.Fill(pattern[0]);
            return;
        }

        pattern.CopyTo(destination);
        int filled = pattern.Length;
        while (filled < destination.Length)
        {
            int n = Math.Min(filled, destination.Length - filled);
            destination.Slice(0, n).CopyTo(destination.Slice(filled, n));
            filled += n;
        }
    }

    private static IArrowArray BuildNull(
        IArrowType type, int length, ArrowBuffer[] buffers, ArrayData[]? children = null) =>
        ArrowArrayFactory.BuildArray(new ArrayData(
            type, length, nullCount: length, offset: 0, buffers, children));

    /// <summary>
    /// A validity bitmap with every bit cleared — the all-null case. This is the inverse of the no-null
    /// shape <see cref="ValidityWriter"/> produces (<see cref="ArrowBuffer.Empty"/> with a zero null count),
    /// and unlike that one it must actually be allocated: an absent bitmap means all-valid.
    /// </summary>
    private static ArrowBuffer AllNullBitmap(int length) => Zeros(BitmapBytes(length));

    /// <summary>A zero-filled buffer; <c>new byte[]</c> is already zeroed, so there is nothing to fill.</summary>
    private static ArrowBuffer Zeros(int byteCount) =>
        byteCount == 0 ? ArrowBuffer.Empty : new ArrowBuffer(new byte[byteCount]);

    /// <summary>
    /// An array the caller is about to overwrite in full, allocated WITHOUT the runtime's zero-fill.
    ///
    /// <para>The zeroing is not a rounding error on these buffers — it is a second full pass over memory the
    /// very next loop overwrites. Measured on a 1M-row Int32→Int64 widening, zeroing the 8 MB output cost
    /// more than the entire conversion loop (0.371 ns/row against 0.308), and dropping it took the whole
    /// operation from 1.001 to 0.649 ns/row.</para>
    ///
    /// <para>The contract is the caller's to keep: EVERY element must be written before the array is handed
    /// out, because what is in it otherwise is whatever the heap last held. That rules this out for any
    /// buffer whose correctness rests on being zero — <see cref="TakeFixedWidth"/>'s value slots under nulls,
    /// the bit-packed boolean paths that only ever SET bits, and everything <see cref="Zeros"/> builds. Those
    /// deliberately still allocate zeroed; see the note at each.</para>
    ///
    /// <para>Below the runtime's threshold (a couple of KiB) this hands back a zeroed array anyway, so a
    /// short buffer neither gains nor loses. Never rely on that: it is an allocator detail, not a guarantee.</para>
    /// </summary>
    private static T[] Uninitialized<T>(int count) =>
#if NET6_0_OR_GREATER
        GC.AllocateUninitializedArray<T>(count);
#else
        new T[count];
#endif

    /// <summary>
    /// Byte width of a fixed-width Arrow type, or null for variable-width and nested types.
    /// Boolean is deliberately absent — it is bit-packed, not one byte per value.
    /// </summary>
    private static int? FixedWidthBytes(IArrowType type) => type switch
    {
        Int8Type or UInt8Type => 1,
        // HalfFloatType needs no target-framework guard even though HalfFloatArray does not exist on
        // netstandard2.0: this switch keys off the TYPE, which is present everywhere, and on a target
        // without the array class Arrow cannot hand us one to gather in the first place. Matching on the
        // array class instead is what forced the #if in the per-type code this replaced.
        Int16Type or UInt16Type or HalfFloatType => 2,
        Int32Type or UInt32Type or FloatType or Date32Type or Time32Type => 4,
        Int64Type or UInt64Type or DoubleType or Date64Type or Time64Type
            or TimestampType or DurationType => 8,
        // IntervalType is deliberately absent. No caller of this helper produces interval columns (the only
        // interval arrays in the codebase come out of Avro, which gathers rows its own way), so a width table
        // for it could not be exercised by any test — and an unverified width silently truncates or overruns
        // rather than failing. Add the unit here together with a test when a caller actually needs it.
        // Decimal32/64/128/256Type all derive from FixedSizeBinaryType, so ByteWidth (4/8/16/32) is exact
        // and this one arm covers them along with FixedSizeBinary itself.
        FixedSizeBinaryType fsb => fsb.ByteWidth,
        _ => null,
    };

    /// <summary>
    /// Gathers a fixed-width array by copying each kept row's raw value slot. Type-agnostic and lossless:
    /// timestamp unit/timezone and decimal precision/scale ride along in the reused DataType, and the bytes
    /// are never interpreted, so a decimal wider than <see cref="decimal"/>'s 28 digits survives intact.
    /// </summary>
    private static IArrowArray TakeFixedWidth(
        IArrowArray source, ReadOnlySpan<int> indices, int byteWidth)
    {
        int count = indices.Length;
        ReadOnlySpan<byte> src = source.Data.Buffers[1].Span;
        int srcOffset = source.Data.Offset;

        // Deliberately a ZEROED allocation, unlike the gathers that use Uninitialized: the null path below
        // writes nothing at all, so a null row's slot is whatever the buffer arrived holding. Zeroed, that is
        // a defined value Arrow consumers already expect; uninitialized, it would be leftover heap contents
        // that a downstream encoder or statistics pass could read and write into a file.
        var values = new byte[count * byteWidth];
        var validity = new ValidityWriter(count, source.NullCount);
        bool probeNulls = validity.SourceHasNulls;

        for (int i = 0; i < count; i++)
        {
            int r = indices[i];
            if (probeNulls && source.IsNull(r))
            {
                validity.SetNull(i);
                continue; // value slot stays zero
            }

            src.Slice((srcOffset + r) * byteWidth, byteWidth)
               .CopyTo(values.AsSpan(i * byteWidth, byteWidth));
        }

        return ArrowArrayFactory.BuildArray(new ArrayData(
            source.Data.DataType, count, validity.NullCount, offset: 0,
            new[] { validity.Build(), new ArrowBuffer(values) }));
    }

    private static IArrowArray TakeBoolean(BooleanArray source, ReadOnlySpan<int> indices)
    {
        int count = indices.Length;
        // Zeroed for the same reason as TakeFixedWidth's value buffer: the loop below only ever SETS bits,
        // so every false and every null is carried by a bit that was never written.
        var values = new byte[BitmapBytes(count)];
        var validity = new ValidityWriter(count, source.NullCount);

        for (int i = 0; i < count; i++)
        {
            int r = indices[i];
            // GetValue applies the array's own offset, so r stays logical here.
            bool? v = source.GetValue(r);
            if (v is null)
            {
                validity.SetNull(i);
                continue;
            }

            if (v.Value)
                values[i >> 3] |= (byte)(1 << (i & 7));
        }

        return new BooleanArray(new ArrayData(
            BooleanType.Default, count, validity.NullCount, offset: 0,
            new[] { validity.Build(), new ArrowBuffer(values) }));
    }

    private static IArrowArray TakeVarBinary32(IArrowArray source, ReadOnlySpan<int> indices)
    {
        int count = indices.Length;
        ReadOnlySpan<int> srcOffsets = MemoryMarshal.Cast<byte, int>(source.Data.Buffers[1].Span);
        ReadOnlySpan<byte> srcValues = source.Data.Buffers[2].Span;
        int arrOffset = source.Data.Offset;

        // Both passes below write every element: the offsets get one per row plus the terminator, and the
        // kept rows' slices tile the value buffer end to end (a null contributes a zero-length slice, so it
        // leaves no gap between its neighbours). Neither needs a zero-fill first.
        var newOffsets = Uninitialized<int>(count + 1);
        var validity = new ValidityWriter(count, source.NullCount);
        bool probeNulls = validity.SourceHasNulls;

        // First pass sizes the value buffer and fills the offsets; a null contributes a zero-length slot,
        // which still needs its offset written so the run stays monotonic.
        int total = 0;
        for (int i = 0; i < count; i++)
        {
            int r = indices[i];
            newOffsets[i] = total;

            if (probeNulls && source.IsNull(r))
            {
                validity.SetNull(i);
                continue;
            }

            int slot = arrOffset + r;
            total += srcOffsets[slot + 1] - srcOffsets[slot];
        }
        newOffsets[count] = total;

        var values = Uninitialized<byte>(total);
        for (int i = 0; i < count; i++)
        {
            int r = indices[i];
            if (probeNulls && source.IsNull(r))
                continue;

            int slot = arrOffset + r;
            int start = srcOffsets[slot];
            int len = srcOffsets[slot + 1] - start;
            srcValues.Slice(start, len).CopyTo(values.AsSpan(newOffsets[i], len));
        }

        var offsetBytes = Uninitialized<byte>((count + 1) * sizeof(int));
        MemoryMarshal.AsBytes(newOffsets.AsSpan()).CopyTo(offsetBytes);

        return ArrowArrayFactory.BuildArray(new ArrayData(
            source.Data.DataType, count, validity.NullCount, offset: 0,
            new[] { validity.Build(), new ArrowBuffer(offsetBytes), new ArrowBuffer(values) }));
    }

    /// <summary>
    /// The 64-bit-offset twin of <see cref="TakeVarBinary32"/>, for LargeString/LargeBinary. Kept separate
    /// so a Large* column is rebuilt as Large*: gathering it through the 32-bit path would hand back a
    /// column whose type contradicts the schema it lands in, and would overflow on more than 2 GiB of values.
    /// </summary>
    private static IArrowArray TakeVarBinary64(IArrowArray source, ReadOnlySpan<int> indices)
    {
        int count = indices.Length;
        ReadOnlySpan<long> srcOffsets = MemoryMarshal.Cast<byte, long>(source.Data.Buffers[1].Span);
        ReadOnlySpan<byte> srcValues = source.Data.Buffers[2].Span;
        int arrOffset = source.Data.Offset;

        var newOffsets = Uninitialized<long>(count + 1);
        var validity = new ValidityWriter(count, source.NullCount);
        bool probeNulls = validity.SourceHasNulls;

        long total = 0;
        for (int i = 0; i < count; i++)
        {
            int r = indices[i];
            newOffsets[i] = total;

            if (probeNulls && source.IsNull(r))
            {
                validity.SetNull(i);
                continue;
            }

            int slot = arrOffset + r;
            total += srcOffsets[slot + 1] - srcOffsets[slot];
        }
        newOffsets[count] = total;

        var values = Uninitialized<byte>(checked((int)total));
        for (int i = 0; i < count; i++)
        {
            int r = indices[i];
            if (probeNulls && source.IsNull(r))
                continue;

            int slot = arrOffset + r;
            long start = srcOffsets[slot];
            int len = checked((int)(srcOffsets[slot + 1] - start));
            srcValues.Slice(checked((int)start), len)
                     .CopyTo(values.AsSpan(checked((int)newOffsets[i]), len));
        }

        var offsetBytes = Uninitialized<byte>((count + 1) * sizeof(long));
        MemoryMarshal.AsBytes(newOffsets.AsSpan()).CopyTo(offsetBytes);

        return ArrowArrayFactory.BuildArray(new ArrayData(
            source.Data.DataType, count, validity.NullCount, offset: 0,
            new[] { validity.Build(), new ArrowBuffer(offsetBytes), new ArrowBuffer(values) }));
    }

    private static IArrowArray TakeStruct(StructArray source, ReadOnlySpan<int> indices)
    {
        int count = indices.Length;

        // A struct's children are parallel to the parent's LOGICAL rows but do not incorporate the parent's
        // offset themselves (Data.Children are the raw child ArrayData), so a child row sits at
        // parentOffset + r. The recursive call then adds the child's own offset on top.
        int parentOffset = source.Data.Offset;
        ReadOnlySpan<int> childIndices = indices;
        if (parentOffset != 0)
        {
            var shifted = new int[count];
            for (int i = 0; i < count; i++)
                shifted[i] = indices[i] + parentOffset;
            childIndices = shifted;
        }

        var children = new ArrayData[source.Data.Children.Length];
        for (int c = 0; c < children.Length; c++)
        {
            children[c] = Take(
                ArrowArrayFactory.BuildArray(source.Data.Children[c]), childIndices).Data;
        }

        var validity = new ValidityWriter(count, source.NullCount);
        if (validity.SourceHasNulls)
        {
            for (int i = 0; i < count; i++)
            {
                if (source.IsNull(indices[i]))
                    validity.SetNull(i);
            }
        }

        return ArrowArrayFactory.BuildArray(new ArrayData(
            source.Data.DataType, count, validity.NullCount, offset: 0,
            new[] { validity.Build() }, children));
    }

    /// <summary>
    /// Gathers a LIST or MAP — the two that share the 32-bit-offset list layout — by turning each kept row's
    /// offset range into the child positions it covers, then gathering the child by those positions.
    ///
    /// <para>A list's child is reached through the offsets buffer, NOT by being sliced alongside its parent
    /// the way a struct's children are. That difference is the whole of this method: the offsets buffer is
    /// indexed by the parent's PHYSICAL slot (so the parent's offset applies to it), while the values it
    /// holds are LOGICAL positions in the child, with the child's own offset applying on top. Measured
    /// against Apache.Arrow's own <c>GetSlicedValues</c>, which resolves them the same way — so handing those
    /// positions straight to the recursive <see cref="Take(IArrowArray, ReadOnlySpan{int})"/> is correct, and
    /// shifting them by the parent's offset (as the struct gather must) would read the wrong child rows.</para>
    ///
    /// <para>The source's DataType is reused verbatim, which is what keeps a MAP a map — including its
    /// keysSorted flag and its entry field names — rather than the list its layout looks like.</para>
    /// </summary>
    private static IArrowArray TakeList32(IArrowArray source, ReadOnlySpan<int> indices)
    {
        int count = indices.Length;
        ReadOnlySpan<int> srcOffsets = MemoryMarshal.Cast<byte, int>(source.Data.Buffers[1].Span);
        int arrOffset = source.Data.Offset;

        var newOffsets = new int[count + 1];
        var validity = new ValidityWriter(count, source.NullCount);
        bool probeNulls = validity.SourceHasNulls;

        // First pass sizes the child selection and fills the offsets; a null contributes a zero-length slice,
        // which still needs its offset written so the run stays monotonic.
        int total = 0;
        for (int i = 0; i < count; i++)
        {
            int r = indices[i];
            newOffsets[i] = total;

            if (probeNulls && source.IsNull(r))
            {
                validity.SetNull(i);
                continue;
            }

            int slot = arrOffset + r;
            // Checked because duplicated indices can select more child rows than the source holds, and a
            // wrapped total would size the child buffer wrong rather than fail.
            total = checked(total + (srcOffsets[slot + 1] - srcOffsets[slot]));
        }
        newOffsets[count] = total;

        var childIndices = Uninitialized<int>(total);
        int w = 0;
        for (int i = 0; i < count; i++)
        {
            int r = indices[i];
            if (probeNulls && source.IsNull(r))
                continue;

            int slot = arrOffset + r;
            int start = srcOffsets[slot];
            int len = srcOffsets[slot + 1] - start;
            for (int k = 0; k < len; k++)
                childIndices[w++] = start + k;
        }

        var offsetBytes = Uninitialized<byte>((count + 1) * sizeof(int));
        MemoryMarshal.AsBytes(newOffsets.AsSpan()).CopyTo(offsetBytes);

        return ArrowArrayFactory.BuildArray(new ArrayData(
            source.Data.DataType, count, validity.NullCount, offset: 0,
            new[] { validity.Build(), new ArrowBuffer(offsetBytes) },
            new[] { TakeListChild(source, childIndices) }));
    }

    /// <summary>
    /// The 64-bit-offset twin of <see cref="TakeList32"/>, for LargeList. The child positions themselves
    /// still have to fit an <see cref="int"/> — an Arrow array cannot be longer than that — so the narrowing
    /// is checked rather than assumed.
    /// </summary>
    private static IArrowArray TakeList64(IArrowArray source, ReadOnlySpan<int> indices)
    {
        int count = indices.Length;
        ReadOnlySpan<long> srcOffsets = MemoryMarshal.Cast<byte, long>(source.Data.Buffers[1].Span);
        int arrOffset = source.Data.Offset;

        var newOffsets = Uninitialized<long>(count + 1);
        var validity = new ValidityWriter(count, source.NullCount);
        bool probeNulls = validity.SourceHasNulls;

        long total = 0;
        for (int i = 0; i < count; i++)
        {
            int r = indices[i];
            newOffsets[i] = total;

            if (probeNulls && source.IsNull(r))
            {
                validity.SetNull(i);
                continue;
            }

            int slot = arrOffset + r;
            total = checked(total + (srcOffsets[slot + 1] - srcOffsets[slot]));
        }
        newOffsets[count] = total;

        var childIndices = Uninitialized<int>(checked((int)total));
        int w = 0;
        for (int i = 0; i < count; i++)
        {
            int r = indices[i];
            if (probeNulls && source.IsNull(r))
                continue;

            int slot = arrOffset + r;
            int start = checked((int)srcOffsets[slot]);
            int len = checked((int)(srcOffsets[slot + 1] - srcOffsets[slot]));
            for (int k = 0; k < len; k++)
                childIndices[w++] = start + k;
        }

        var offsetBytes = Uninitialized<byte>((count + 1) * sizeof(long));
        MemoryMarshal.AsBytes(newOffsets.AsSpan()).CopyTo(offsetBytes);

        return ArrowArrayFactory.BuildArray(new ArrayData(
            source.Data.DataType, count, validity.NullCount, offset: 0,
            new[] { validity.Build(), new ArrowBuffer(offsetBytes) },
            new[] { TakeListChild(source, childIndices) }));
    }

    /// <summary>
    /// A fixed-size list carries no offsets buffer at all — its single buffer is the validity bitmap — so a
    /// row's child positions are arithmetic: <c>ListSize</c> of them starting at
    /// <c>(offset + row) * ListSize</c>, in the child's logical space.
    ///
    /// <para>A NULL row still owns its full <c>ListSize</c> of child slots, because Arrow requires the child
    /// to be exactly <c>length * ListSize</c> long whatever the validity says. Its source slots are therefore
    /// gathered like any other row rather than skipped: the values under a null are undefined either way, and
    /// carrying the source's over keeps the length invariant without inventing data.</para>
    /// </summary>
    private static IArrowArray TakeFixedSizeList(FixedSizeListArray source, ReadOnlySpan<int> indices)
    {
        int count = indices.Length;
        int listSize = ((FixedSizeListType)source.Data.DataType).ListSize;
        int arrOffset = source.Data.Offset;

        var childIndices = Uninitialized<int>(checked(count * listSize));
        int w = 0;
        for (int i = 0; i < count; i++)
        {
            int start = checked((arrOffset + indices[i]) * listSize);
            for (int k = 0; k < listSize; k++)
                childIndices[w++] = start + k;
        }

        var validity = new ValidityWriter(count, source.NullCount);
        if (validity.SourceHasNulls)
        {
            for (int i = 0; i < count; i++)
            {
                if (source.IsNull(indices[i]))
                    validity.SetNull(i);
            }
        }

        return ArrowArrayFactory.BuildArray(new ArrayData(
            source.Data.DataType, count, validity.NullCount, offset: 0,
            new[] { validity.Build() },
            new[] { TakeListChild(source, childIndices) }));
    }

    /// <summary>
    /// Gathers the child rows a list-shaped array's parent gather selected. The child is rebuilt from its raw
    /// <see cref="ArrayData"/> so the recursion re-enters <see cref="Take(IArrowArray, ReadOnlySpan{int})"/>
    /// by array type — which is how a list of structs, a map's key/value entries, and a list of lists all
    /// gather without a case of their own here.
    /// </summary>
    private static ArrayData TakeListChild(IArrowArray source, int[] childIndices) =>
        Take(ArrowArrayFactory.BuildArray(source.Data.Children[0]), childIndices).Data;

    private static int BitmapBytes(int count) => (count + 7) / 8;

    /// <summary>
    /// Accumulates a validity bitmap, allocating it only if a null actually shows up. A source array with no
    /// nulls cannot produce one no matter which rows are gathered, so the common case allocates nothing and
    /// returns <see cref="ArrowBuffer.Empty"/> with a null count of zero — which is what lets Arrow skip the
    /// per-element validity check on the resulting array.
    /// </summary>
    private struct ValidityWriter
    {
        private readonly int _count;
        private byte[]? _bitmap;

        public ValidityWriter(int count, int sourceNullCount)
        {
            _count = count;
            _bitmap = null;
            NullCount = 0;
            SourceHasNulls = sourceNullCount != 0;
        }

        public int NullCount { get; private set; }

        /// <summary>
        /// False when the source array declares no nulls, letting callers skip their per-row null probe.
        /// </summary>
        public bool SourceHasNulls { get; }

        public void SetNull(int index)
        {
            if (_bitmap is null)
            {
                // Start all-valid, then clear as nulls arrive — nulls are the rarer case.
                _bitmap = new byte[BitmapBytes(_count)];
                _bitmap.AsSpan().Fill(0xFF);
            }

            _bitmap[index >> 3] &= (byte)~(1 << (index & 7));
            NullCount++;
        }

        public ArrowBuffer Build() =>
            _bitmap is null ? ArrowBuffer.Empty : new ArrowBuffer(_bitmap);
    }
}

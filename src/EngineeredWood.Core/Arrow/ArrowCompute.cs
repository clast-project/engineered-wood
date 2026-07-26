// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

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

        var newOffsets = new int[count + 1];
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

        var values = new byte[total];
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

        var offsetBytes = new byte[(count + 1) * sizeof(int)];
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

        var newOffsets = new long[count + 1];
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

        var values = new byte[checked((int)total)];
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

        var offsetBytes = new byte[(count + 1) * sizeof(long)];
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

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.DeltaLake.Checkpoint;

/// <summary>
/// Accumulates an Arrow validity bitmap in native memory, one bit per row. Ownership transfers
/// to a zero-copy <see cref="ArrowBuffer"/> at <see cref="Build"/>; when no null was appended the
/// bitmap is dropped and <see cref="ArrowBuffer.Empty"/> returned, which Arrow reads as all-valid.
/// </summary>
internal sealed class ValidityBuilder : IDisposable
{
    private NativeBuffer<byte>? _bits;
    private int _capacityBytes;
    private int _count;
    private int _nullCount;

    public ValidityBuilder(int rowCapacity)
    {
        _capacityBytes = Math.Max(1, (Math.Max(rowCapacity, 1) + 7) / 8);
        _bits = new NativeBuffer<byte>(_capacityBytes, zeroFill: true);
    }

    public int Count => _count;

    public int NullCount => _nullCount;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(bool valid)
    {
        int byteIndex = _count >> 3;
        if (byteIndex >= _capacityBytes)
            Grow(byteIndex);
        if (valid)
            _bits!.ByteSpan[byteIndex] |= (byte)(1 << (_count & 7));
        else
            _nullCount++;   // bit stays 0
        _count++;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Grow(int byteIndex)
    {
        int oldBytes = _capacityBytes;
        _bits!.Grow(byteIndex + 1);
        // NativeBuffer.Grow does not zero new bytes; validity needs zeros for unset (null) bits.
        _bits.ByteSpan.Slice(oldBytes).Clear();
        _capacityBytes = _bits.Length;
    }

    /// <summary>Transfers bitmap ownership to an <see cref="ArrowBuffer"/>; the builder holds no buffer after.</summary>
    public ArrowBuffer Build()
    {
        var bits = _bits ?? throw new ObjectDisposedException(nameof(ValidityBuilder));
        _bits = null;
        _capacityBytes = 0;
        if (_nullCount == 0)
        {
            bits.Dispose();
            return ArrowBuffer.Empty;
        }
        return bits.Build();
    }

    public void Dispose()
    {
        _bits?.Dispose();
        _bits = null;
        _capacityBytes = 0;
    }
}

/// <summary>
/// Accumulates a fixed-width scalar column (int/long/float/…) into a native buffer pre-sized to the
/// row count, transferring ownership to Arrow zero-copy at <see cref="Build"/>. Replaces
/// <c>PrimitiveArrayBuilder</c>, whose per-append managed <c>List&lt;T&gt;</c> is copied again when
/// the array is built.
/// </summary>
internal sealed class FixedWidthColumn<T> : IDisposable where T : unmanaged
{
    private readonly ValidityBuilder _validity;
    private NativeBuffer<T>? _values;
    private int _capacity;
    private int _count;

    public FixedWidthColumn(int rowCapacity)
    {
        _capacity = Math.Max(1, rowCapacity);
        // zeroFill:false — every slot up to _count is written (the value, or default for a null).
        _values = new NativeBuffer<T>(_capacity, zeroFill: false);
        _validity = new ValidityBuilder(rowCapacity);
    }

    public int Count => _count;

    public void Append(T value)
    {
        EnsureSlot();
        _values!.Span[_count++] = value;
        _validity.Append(true);
    }

    public void AppendNull()
    {
        EnsureSlot();
        _values!.Span[_count++] = default;
        _validity.Append(false);
    }

    /// <summary>Appends nulls until the column holds <paramref name="rowCount"/> entries.</summary>
    public void PadTo(int rowCount)
    {
        while (_count < rowCount)
            AppendNull();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureSlot()
    {
        if (_count < _capacity)
            return;
        _values!.Grow(_count + 1);
        _capacity = _values.Length;
    }

    public IArrowArray Build(IArrowType type)
    {
        var values = _values ?? throw new ObjectDisposedException(nameof(FixedWidthColumn<T>));
        _values = null;
        _capacity = 0;
        var data = new ArrayData(
            type, _count, _validity.NullCount, 0, [_validity.Build(), values.Build()]);
        return ArrowArrayFactory.BuildArray(data);
    }

    public void Dispose()
    {
        _values?.Dispose();
        _values = null;
        _capacity = 0;
        _validity.Dispose();
    }
}

/// <summary>
/// Accumulates a fixed-width binary column — Arrow's layout for <c>Decimal128</c> and friends, where
/// each value is a fixed run of bytes rather than a primitive.
/// </summary>
internal sealed class FixedSizeBinaryColumn : IDisposable
{
    private readonly ValidityBuilder _validity;
    private readonly int _width;
    private NativeBuffer<byte>? _values;
    private int _capacityElements;
    private int _count;

    public FixedSizeBinaryColumn(int rowCapacity, int byteWidth)
    {
        _width = byteWidth;
        _capacityElements = Math.Max(1, rowCapacity);
        // zeroFill:false — every slot up to _count is written, values or zeros for a null.
        _values = new NativeBuffer<byte>(_capacityElements * byteWidth, zeroFill: false);
        _validity = new ValidityBuilder(rowCapacity);
    }

    public int Count => _count;

    /// <summary>Appends one value; <paramref name="value"/> must be exactly the column's byte width.</summary>
    public void Append(ReadOnlySpan<byte> value)
    {
        EnsureSlot();
        value.CopyTo(_values!.ByteSpan.Slice(_count * _width, _width));
        _count++;
        _validity.Append(true);
    }

    public void AppendNull()
    {
        EnsureSlot();
        _values!.ByteSpan.Slice(_count * _width, _width).Clear();
        _count++;
        _validity.Append(false);
    }

    public void PadTo(int rowCount)
    {
        while (_count < rowCount)
            AppendNull();
    }

    private void EnsureSlot()
    {
        if (_count < _capacityElements)
            return;
        _values!.Grow((_count + 1) * _width);
        _capacityElements = _values.Length / _width;
    }

    public IArrowArray Build(IArrowType type)
    {
        var values = _values ?? throw new ObjectDisposedException(nameof(FixedSizeBinaryColumn));
        _values = null;
        _capacityElements = 0;
        var data = new ArrayData(
            type, _count, _validity.NullCount, 0, [_validity.Build(), values.Build()]);
        return ArrowArrayFactory.BuildArray(data);
    }

    public void Dispose()
    {
        _values?.Dispose();
        _values = null;
        _capacityElements = 0;
        _validity.Dispose();
    }
}

/// <summary>
/// Accumulates a boolean column into a native value bitmap (one bit per row, set when true)
/// alongside a native validity bitmap — the layout Arrow already uses, so no re-packing is needed
/// at <see cref="Build"/>.
/// </summary>
internal sealed class BooleanColumn : IDisposable
{
    private readonly ValidityBuilder _validity;
    private NativeBuffer<byte>? _values;
    private int _capacityBytes;
    private int _count;

    public BooleanColumn(int rowCapacity)
    {
        _capacityBytes = Math.Max(1, (Math.Max(rowCapacity, 1) + 7) / 8);
        _values = new NativeBuffer<byte>(_capacityBytes, zeroFill: true);
        _validity = new ValidityBuilder(rowCapacity);
    }

    public int Count => _count;

    public void Append(bool value)
    {
        int byteIndex = _count >> 3;
        if (byteIndex >= _capacityBytes)
            Grow(byteIndex);
        if (value)
            _values!.ByteSpan[byteIndex] |= (byte)(1 << (_count & 7));
        _count++;
        _validity.Append(true);
    }

    public void AppendNull()
    {
        int byteIndex = _count >> 3;
        if (byteIndex >= _capacityBytes)
            Grow(byteIndex);
        // value bit stays 0
        _count++;
        _validity.Append(false);
    }

    public void PadTo(int rowCount)
    {
        while (_count < rowCount)
            AppendNull();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Grow(int byteIndex)
    {
        int oldBytes = _capacityBytes;
        _values!.Grow(byteIndex + 1);
        // NativeBuffer.Grow does not zero new bytes; false/unset value bits need zeros.
        _values.ByteSpan.Slice(oldBytes).Clear();
        _capacityBytes = _values.Length;
    }

    public BooleanArray Build()
    {
        var values = _values ?? throw new ObjectDisposedException(nameof(BooleanColumn));
        _values = null;
        _capacityBytes = 0;
        var data = new ArrayData(
            BooleanType.Default, _count, _validity.NullCount, 0,
            [_validity.Build(), values.Build()]);
        return new BooleanArray(data);
    }

    public void Dispose()
    {
        _values?.Dispose();
        _values = null;
        _capacityBytes = 0;
        _validity.Dispose();
    }
}

/// <summary>
/// Accumulates a UTF-8 string column (Arrow layout: validity, int32 offsets, data bytes) in native
/// memory. Values are encoded straight into the data buffer — no intermediate <c>byte[]</c> per
/// value — and both buffers transfer to Arrow zero-copy at <see cref="Build"/>.
/// </summary>
internal sealed class StringColumn : IDisposable
{
    private readonly ValidityBuilder _validity;
    private NativeBuffer<int>? _offsets;     // count+1 running byte offsets, offsets[0] = 0
    private int _offsetCapacity;
    private NativeBuffer<byte>? _data;
    private int _dataCapacity;
    private int _dataLength;
    private int _count;

    /// <param name="rowCapacity">Expected number of values.</param>
    /// <param name="bytesPerValueHint">Initial data-buffer sizing hint; the buffer grows on demand.
    /// Pass 0 for a column that is null on almost every row (a checkpoint's metaData/txn fields), so
    /// it starts at the 64-byte minimum instead of reserving bytes for rows that hold nothing.</param>
    public StringColumn(int rowCapacity, int bytesPerValueHint = 16)
    {
        _offsetCapacity = Math.Max(1, rowCapacity) + 1;
        _offsets = new NativeBuffer<int>(_offsetCapacity, zeroFill: false);
        _offsets.Span[0] = 0;
        _dataCapacity = Math.Max(64, Math.Max(1, rowCapacity) * bytesPerValueHint);
        _data = new NativeBuffer<byte>(_dataCapacity, zeroFill: false);
        _validity = new ValidityBuilder(rowCapacity);
    }

    public int Count => _count;

    public void Append(string value)
    {
        int byteCount = value.Length == 0 ? 0 : System.Text.Encoding.UTF8.GetByteCount(value);
        EnsureOffsetSlot();
        if (byteCount > 0)
        {
            EnsureDataCapacity(byteCount);
            WriteUtf8(value, _data!.ByteSpan.Slice(_dataLength, byteCount));
            _dataLength += byteCount;
        }
        _count++;
        _offsets!.Span[_count] = _dataLength;
        _validity.Append(true);
    }

    public void AppendNull()
    {
        EnsureOffsetSlot();
        _count++;
        _offsets!.Span[_count] = _dataLength;   // repeat the last offset -> empty slot
        _validity.Append(false);
    }

    /// <summary>Appends <paramref name="value"/>, or a null when it is <c>null</c>.</summary>
    public void AppendOrNull(string? value)
    {
        if (value is null)
            AppendNull();
        else
            Append(value);
    }

    public void PadTo(int rowCount)
    {
        while (_count < rowCount)
            AppendNull();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureOffsetSlot()
    {
        if (_count + 1 < _offsetCapacity)
            return;
        GrowOffsets();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void GrowOffsets()
    {
        _offsets!.Grow(_count + 2);
        _offsetCapacity = _offsets.Length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureDataCapacity(int extra)
    {
        if (_dataLength + extra <= _dataCapacity)
            return;
        GrowData(extra);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void GrowData(int extra)
    {
        _data!.Grow(_dataLength + extra);
        _dataCapacity = _data.Length;
    }

    private static void WriteUtf8(string value, Span<byte> destination)
    {
#if NET8_0_OR_GREATER
        System.Text.Encoding.UTF8.GetBytes(value.AsSpan(), destination);
#else
        // netstandard2.0 has no span-based GetBytes; stage through a pooled array (no per-value allocation).
        byte[] staging = ArrayPool<byte>.Shared.Rent(destination.Length);
        try
        {
            int written = System.Text.Encoding.UTF8.GetBytes(value, 0, value.Length, staging, 0);
            staging.AsSpan(0, written).CopyTo(destination);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(staging);
        }
#endif
    }

    public StringArray Build()
    {
        var offsets = _offsets ?? throw new ObjectDisposedException(nameof(StringColumn));
        var data = _data!;
        _offsets = null;
        _data = null;
        _offsetCapacity = 0;
        _dataCapacity = 0;
        var arrayData = new ArrayData(
            StringType.Default, _count, _validity.NullCount, 0,
            [_validity.Build(), offsets.Build(), data.Build()]);
        return new StringArray(arrayData);
    }

    public void Dispose()
    {
        _offsets?.Dispose();
        _data?.Dispose();
        _offsets = null;
        _data = null;
        _offsetCapacity = 0;
        _dataCapacity = 0;
        _validity.Dispose();
    }
}

/// <summary>
/// Accumulates Arrow list/map offsets (int32, <c>count+1</c> running child offsets) in a native
/// buffer pre-sized to the parent row count, transferring ownership to Arrow zero-copy at
/// <see cref="Build"/>.
/// </summary>
internal sealed class OffsetsBuilder : IDisposable
{
    private NativeBuffer<int>? _offsets;
    private int _capacity;
    private int _count;
    private int _running;

    public OffsetsBuilder(int rowCapacity)
    {
        _capacity = Math.Max(1, rowCapacity) + 1;
        _offsets = new NativeBuffer<int>(_capacity, zeroFill: false);
        _offsets.Span[0] = 0;
    }

    public int Count => _count;

    /// <summary>Total number of child elements appended so far.</summary>
    public int Total => _running;

    /// <summary>Appends one parent row whose child span holds <paramref name="childCount"/> elements.</summary>
    public void Append(int childCount)
    {
        if (_count + 1 >= _capacity)
        {
            _offsets!.Grow(_count + 2);
            _capacity = _offsets.Length;
        }
        _running += childCount;
        _count++;
        _offsets!.Span[_count] = _running;
    }

    /// <summary>Appends one parent row with no child elements (an empty or null slot).</summary>
    public void AppendEmpty() => Append(0);

    public ArrowBuffer Build()
    {
        var offsets = _offsets ?? throw new ObjectDisposedException(nameof(OffsetsBuilder));
        _offsets = null;
        _capacity = 0;
        return offsets.Build();
    }

    public void Dispose()
    {
        _offsets?.Dispose();
        _offsets = null;
        _capacity = 0;
    }
}

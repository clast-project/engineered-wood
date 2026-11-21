// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Parquet;

/// <summary>
/// The rows of small batches that <see cref="ParquetFileWriter.WriteBatchAsync"/> holds, copied, until there are
/// enough of them to encode together (#941). Encoding a batch costs about the same whatever its length, so a run of
/// one-row batches cost some fifty times a row of a whole one; copying a row into these buffers costs a few
/// hundredths of that.
/// </summary>
/// <remarks>
/// <para>Only flat columns are held, of the layouts copied here as they are: booleans, fixed-width values and
/// 32-bit-offset strings and binaries. A batch with any other column is encoded as it comes. So is a second-precision
/// time, which the writer rescales, since rescaling can refuse a value and a refusal belongs to the call that passed
/// it. Every held batch has the same column layouts: one with others is encoded after the rows held before it.</para>
/// <para>The arrays <see cref="Build"/> returns view these buffers, and <see cref="Clear"/> lets them be written
/// over: the writer keeps nothing of a batch once it returns, which is all an encoded held batch needs.</para>
/// </remarks>
internal sealed class HeldRows
{
    private readonly Apache.Arrow.Schema _schema;
    private readonly Column[] _columns;

    private HeldRows(Apache.Arrow.Schema schema, Column[] columns)
    {
        _schema = schema;
        _columns = columns;
    }

    /// <summary>The rows held.</summary>
    public int Rows { get; private set; }

    /// <summary>The bytes the held rows take in Arrow's layout: what the buffers hold of them.</summary>
    public long Bytes { get; private set; }

    /// <summary>The bytes of the buffers, held rows or not: for tests.</summary>
    internal long BufferBytes => _columns.Sum(c => c.BufferBytes);

    /// <summary>Buffers for batches of <paramref name="batch"/>'s layout, or null if it has a column not held.</summary>
    public static HeldRows? For(RecordBatch batch)
    {
        var fields = batch.Schema.FieldsList;
        if (fields.Count != batch.ColumnCount)
            return null;
        var columns = new Column[fields.Count];
        for (int i = 0; i < columns.Length; i++)
        {
            if (Column.For(fields[i].DataType) is not { } column)
                return null;
            columns[i] = column;
        }

        var held = new HeldRows(batch.Schema, columns);
        return held.Takes(batch) ? held : null;
    }

    /// <summary>
    /// Whether <paramref name="batch"/> has the held layouts: the same field names, nullability and types, and arrays
    /// of those types.
    /// </summary>
    public bool Takes(RecordBatch batch)
    {
        if (batch.ColumnCount != _columns.Length)
            return false;
        if (!ReferenceEquals(batch.Schema, _schema) && !SameFields(batch.Schema.FieldsList, _schema.FieldsList))
            return false;
        for (int i = 0; i < _columns.Length; i++)
        {
            if (!_columns[i].Takes(batch.Column(i)))
                return false;
        }
        return true;
    }

    /// <summary>
    /// The bytes holding <paramref name="batch"/>, which <see cref="Takes"/>, would add to <see cref="Bytes"/>: known
    /// before anything of it is copied, so a batch too large to hold grows no buffer (#945 review).
    /// </summary>
    public long BytesFor(RecordBatch batch)
    {
        long bytes = 0;
        for (int i = 0; i < _columns.Length; i++)
            bytes += _columns[i].BytesFor(batch.Column(i), Rows);
        GC.KeepAlive(batch); // binary offsets are read as raw spans of its buffers
        return bytes;
    }

    /// <summary>
    /// Copies the rows of <paramref name="batch"/>, which <see cref="Takes"/>, after those held; <paramref name="bytes"/>
    /// is its <see cref="BytesFor"/>.
    /// </summary>
    public void Append(RecordBatch batch, long bytes)
    {
        for (int i = 0; i < _columns.Length; i++)
            _columns[i].Append(batch.Column(i), Rows);
        Rows += batch.Length;
        Bytes += bytes;
        GC.KeepAlive(batch); // the copies read raw spans of its buffers
    }

    /// <summary>The held rows as one batch, over these buffers: valid until <see cref="Clear"/> or the next append.</summary>
    public RecordBatch Build()
    {
        var arrays = new IArrowArray[_columns.Length];
        for (int i = 0; i < arrays.Length; i++)
            arrays[i] = _columns[i].Build(_schema.FieldsList[i].DataType, Rows);
        return new RecordBatch(_schema, arrays, Rows);
    }

    /// <summary>Lets go of the held rows, keeping the buffers for the next.</summary>
    public void Clear()
    {
        foreach (var column in _columns)
            column.Clear();
        Rows = 0;
        Bytes = 0;
    }

    private static bool SameFields(IReadOnlyList<Field> a, IReadOnlyList<Field> b)
    {
        if (a.Count != b.Count)
            return false;
        for (int i = 0; i < a.Count; i++)
        {
            if (!string.Equals(a[i].Name, b[i].Name, StringComparison.Ordinal)
                || a[i].IsNullable != b[i].IsNullable
                || !SameType(a[i].DataType, b[i].DataType))
            {
                return false;
            }
        }
        return true;
    }

    // Exactly the same type, parameters included: a held batch is encoded under the first held batch's schema.
    private static bool SameType(IArrowType a, IArrowType b) => a.TypeId == b.TypeId && (a, b) switch
    {
        (TimestampType x, TimestampType y) => x.Unit == y.Unit && string.Equals(x.Timezone, y.Timezone, StringComparison.Ordinal),
        (Time32Type x, Time32Type y) => x.Unit == y.Unit,
        (Time64Type x, Time64Type y) => x.Unit == y.Unit,
        (DurationType x, DurationType y) => x.Unit == y.Unit,
        (Decimal32Type x, Decimal32Type y) => x.Precision == y.Precision && x.Scale == y.Scale,
        (Decimal64Type x, Decimal64Type y) => x.Precision == y.Precision && x.Scale == y.Scale,
        (Decimal128Type x, Decimal128Type y) => x.Precision == y.Precision && x.Scale == y.Scale,
        (Decimal256Type x, Decimal256Type y) => x.Precision == y.Precision && x.Scale == y.Scale,
        (FixedSizeBinaryType x, FixedSizeBinaryType y) => x.ByteWidth == y.ByteWidth,
        _ => true,
    };

    private enum Layout { Bits, Fixed, Binary }

    /// <summary>
    /// One column's held values, validity and (for strings and binaries) offsets. Also the open page of a flat
    /// column that is not dictionary-encoded (<see cref="Data.ColumnChunkWriter"/>, #940).
    /// </summary>
    internal sealed class Column
    {
        private readonly Layout _layout;
        private readonly ArrowTypeId _typeId;
        private readonly int _width; // bytes per value, for Fixed

        private byte[] _values = new byte[256];
        private int _valueBytes; // bytes of _values in use; for Bits, unused (Rows bits are)
        private byte[] _offsets = new byte[4 * 65]; // int32 offsets
        private byte[]? _validity; // the held rows' validity once _tracksNulls; kept, for reuse, after a Clear
        private bool _tracksNulls; // false while every held row is valid
        private int _nulls;

        private Column(Layout layout, ArrowTypeId typeId, int width)
        {
            _layout = layout;
            _typeId = typeId;
            _width = width;
        }

        public static Column? For(IArrowType type) => type switch
        {
            BooleanType => new Column(Layout.Bits, type.TypeId, 0),
            StringType or BinaryType when type.TypeId is ArrowTypeId.String or ArrowTypeId.Binary =>
                new Column(Layout.Binary, type.TypeId, 0),
            // Rescaled to milliseconds by the writer, which can refuse a value.
            TimestampType { Unit: TimeUnit.Second } or Time32Type { Unit: TimeUnit.Second } => null,
            _ when type.TypeId is ArrowTypeId.Int8 or ArrowTypeId.UInt8 or ArrowTypeId.Int16 or ArrowTypeId.UInt16
                    or ArrowTypeId.Int32 or ArrowTypeId.UInt32 or ArrowTypeId.Int64 or ArrowTypeId.UInt64
                    or ArrowTypeId.HalfFloat or ArrowTypeId.Float or ArrowTypeId.Double
                    or ArrowTypeId.Date32 or ArrowTypeId.Date64 or ArrowTypeId.Time32 or ArrowTypeId.Time64
                    or ArrowTypeId.Timestamp or ArrowTypeId.Duration
                    or ArrowTypeId.Decimal32 or ArrowTypeId.Decimal64 or ArrowTypeId.Decimal128 or ArrowTypeId.Decimal256
                    or ArrowTypeId.FixedSizedBinary
                && type is FixedWidthType { BitWidth: > 0 } fixedWidth && fixedWidth.BitWidth % 8 == 0 =>
                new Column(Layout.Fixed, type.TypeId, fixedWidth.BitWidth / 8),
            _ => null,
        };

        public long BufferBytes => _values.Length + _offsets.Length + (_validity?.Length ?? 0);

        public bool Takes(IArrowArray array)
        {
            if (array is not Apache.Arrow.Array)
                return false; // an extension array, whose storage is not what the field says
            var type = array.Data.DataType;
            if (type.TypeId != _typeId)
                return false;
            return _layout != Layout.Fixed || (type is FixedWidthType f && f.BitWidth == _width * 8);
        }

        public void Clear()
        {
            _valueBytes = 0;
            _nulls = 0;
            _tracksNulls = false;
        }

        /// <summary>
        /// The bytes appending <paramref name="array"/> after <paramref name="rows"/> held rows adds: its values and
        /// offsets, and its validity bits, or every held row's once its first null takes a bitmap.
        /// </summary>
        public long BytesFor(IArrowArray array, int rows)
        {
            int length = array.Length;
            long bitmapGrowth = (rows + length + 7) / 8 - (rows + 7) / 8;
            long bytes = _layout switch
            {
                Layout.Bits => bitmapGrowth,
                Layout.Fixed => (long)length * _width,
                _ => BinaryBytes(array) + 4L * length,
            };
            if (_tracksNulls)
                bytes += bitmapGrowth;
            else if (length > 0 && array.NullCount > 0)
                bytes += (rows + length + 7) / 8;
            return bytes;
        }

        private static long BinaryBytes(IArrowArray array)
        {
            var data = array.Data;
            var offsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span).Slice(data.Offset, array.Length + 1);
            return offsets[array.Length] - offsets[0];
        }

        /// <summary>Copies <paramref name="array"/> after the <paramref name="rows"/> held.</summary>
        public void Append(IArrowArray array, int rows)
        {
            var data = array.Data;
            int length = array.Length;
            int offset = data.Offset;

            int nulls = length == 0 ? 0 : array.NullCount;
            if (nulls > 0 && !_tracksNulls)
            {
                int needed = (rows + length + 7) / 8;
                if (_validity is null || _validity.Length < needed)
                    _validity = new byte[Math.Max(32, needed * 2)];
                SetBits(_validity, 0, rows);
                _tracksNulls = true;
            }
            if (_tracksNulls)
            {
                var validity = _validity!; // allocated by the first null
                Grow(ref validity, (rows + length + 7) / 8);
                _validity = validity;
                if (nulls > 0)
                    CopyBits(data.Buffers[0].Span, offset, validity, rows, length);
                else
                    SetBits(validity, rows, length);
                _nulls += nulls;
            }

            switch (_layout)
            {
                case Layout.Bits:
                    Grow(ref _values, (rows + length + 7) / 8);
                    CopyBits(data.Buffers[1].Span, offset, _values, rows, length);
                    break;

                case Layout.Fixed:
                {
                    var source = data.Buffers[1].Span.Slice(offset * _width, length * _width);
                    Grow(ref _values, _valueBytes + source.Length);
                    source.CopyTo(_values.AsSpan(_valueBytes));
                    _valueBytes += source.Length;
                    break;
                }

                case Layout.Binary:
                {
                    var offsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span).Slice(offset, length + 1);
                    int first = offsets[0];
                    var source = data.Buffers[2].Span.Slice(first, offsets[length] - first);
                    Grow(ref _offsets, 4 * (rows + length + 1));
                    var held = MemoryMarshal.Cast<byte, int>(_offsets.AsSpan());
                    held[rows] = _valueBytes;
                    int shift = _valueBytes - first;
                    for (int i = 1; i <= length; i++)
                        held[rows + i] = offsets[i] + shift;
                    Grow(ref _values, _valueBytes + source.Length);
                    source.CopyTo(_values.AsSpan(_valueBytes));
                    _valueBytes += source.Length;
                    break;
                }
            }
        }

        public IArrowArray Build(IArrowType type, int rows)
        {
            var validity = !_tracksNulls || _nulls == 0
                ? ArrowBuffer.Empty
                : new ArrowBuffer(_validity!.AsMemory(0, (rows + 7) / 8));
            ArrowBuffer[] buffers = _layout switch
            {
                Layout.Bits => [validity, new ArrowBuffer(_values.AsMemory(0, (rows + 7) / 8))],
                Layout.Fixed => [validity, new ArrowBuffer(_values.AsMemory(0, _valueBytes))],
                _ => [validity,
                    new ArrowBuffer(_offsets.AsMemory(0, 4 * (rows + 1))),
                    new ArrowBuffer(_values.AsMemory(0, _valueBytes))],
            };
            return ArrowArrayFactory.BuildArray(new ArrayData(type, rows, _tracksNulls ? _nulls : 0, 0, buffers));
        }

        private static void Grow<T>(ref T[] array, int needed)
        {
            if (needed > array.Length)
                System.Array.Resize(ref array, Math.Max(needed, array.Length * 2));
        }

        private static void SetBits(byte[] bits, int start, int count)
        {
            for (int i = start; i < start + count; i++)
                bits[i >> 3] |= (byte)(1 << (i & 7));
        }

        private static void CopyBits(ReadOnlySpan<byte> source, int sourceBit, byte[] destination, int destinationBit, int count)
        {
            for (int i = 0; i < count; i++)
            {
                int s = sourceBit + i, d = destinationBit + i;
                if ((source[s >> 3] & (1 << (s & 7))) != 0)
                    destination[d >> 3] |= (byte)(1 << (d & 7));
                else
                    destination[d >> 3] &= (byte)~(1 << (d & 7));
            }
        }
    }
}

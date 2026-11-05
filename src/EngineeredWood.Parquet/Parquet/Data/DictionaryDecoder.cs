// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Stores a dictionary page's PLAIN-decoded values and resolves dictionary indices.
/// </summary>
/// <remarks>
/// The value arrays are rented from <see cref="ArrayPool{T}.Shared"/>. A writer that dictionary-encodes
/// every column (arrow-rs, so delta-rs) gives a high-cardinality chunk a dictionary of hundreds of KB, and
/// allocating each one new put it on the large-object heap and drove gen2 collections (#644).
/// <para><see cref="Dispose"/> returns the arrays, so it must come after the last index is resolved: an
/// array returned while a page still decodes from it would be handed out again and overwritten. A
/// dictionary that is never disposed is only collected, as before. After disposal the getters throw
/// rather than read a returned array.</para>
/// </remarks>
internal sealed class DictionaryDecoder : IDisposable
{
    private readonly PhysicalType _physicalType;

    // Typed storage (only one is populated based on physical type)
    private int[]? _int32Values;
    private long[]? _int64Values;
    private float[]? _floatValues;
    private double[]? _doubleValues;
    private bool[]? _boolValues;
    private byte[]? _fixedByteValues; // for INT96, FIXED_LEN_BYTE_ARRAY
    private int _fixedLength;

    // For BYTE_ARRAY: offsets + data buffer
    private int[]? _byteArrayOffsets;
    private byte[]? _byteArrayData;

    private bool _disposed;

    /// <summary>Number of entries in this dictionary.</summary>
    public int Count { get; private set; }

    public DictionaryDecoder(PhysicalType physicalType)
    {
        _physicalType = physicalType;
    }

    /// <summary>
    /// Loads dictionary entries from PLAIN-encoded data.
    /// </summary>
    public void Load(ReadOnlySpan<byte> data, int numValues, int typeLength)
    {
        // The arrays below are sized from the header's count before the data is read, so a tiny
        // page declaring int.MaxValue entries would allocate gigabytes. Every PLAIN entry takes at
        // least a bit (BOOLEAN) or MinEntryBytes bytes, which bounds the count by the data.
        long capacity = _physicalType == PhysicalType.Boolean
            ? data.Length * 8L
            : data.Length / Math.Max(1, MinEntryBytes(typeLength));
        if (numValues < 0 || numValues > capacity)
        {
            throw new ParquetFormatException(
                $"Dictionary page declares {numValues} entries, but its {data.Length} bytes of " +
                $"{_physicalType} data can hold at most {capacity}.");
        }

        Count = numValues;
        switch (_physicalType)
        {
            case PhysicalType.Boolean:
                _boolValues = Rent<bool>(numValues);
                PlainDecoder.DecodeBooleans(data, _boolValues, numValues);
                break;

            case PhysicalType.Int32:
                _int32Values = Rent<int>(numValues);
                PlainDecoder.DecodeInt32s(data, _int32Values, numValues);
                break;

            case PhysicalType.Int64:
                _int64Values = Rent<long>(numValues);
                PlainDecoder.DecodeInt64s(data, _int64Values, numValues);
                break;

            case PhysicalType.Float:
                _floatValues = Rent<float>(numValues);
                PlainDecoder.DecodeFloats(data, _floatValues, numValues);
                break;

            case PhysicalType.Double:
                _doubleValues = Rent<double>(numValues);
                PlainDecoder.DecodeDoubles(data, _doubleValues, numValues);
                break;

            case PhysicalType.Int96:
                _fixedLength = 12;
                _fixedByteValues = Rent<byte>(numValues * 12);
                PlainDecoder.DecodeInt96s(data, _fixedByteValues, numValues);
                break;

            case PhysicalType.FixedLenByteArray:
                _fixedLength = typeLength;
                _fixedByteValues = Rent<byte>(numValues * typeLength);
                PlainDecoder.DecodeFixedLenByteArrays(data, _fixedByteValues, numValues, typeLength);
                break;

            case PhysicalType.ByteArray:
            {
                _byteArrayOffsets = Rent<int>(numValues + 1);
                int total = PlainDecoder.MeasureByteArrays(data, _byteArrayOffsets, numValues);
                _byteArrayData = Rent<byte>(total);
                PlainDecoder.CopyByteArrayData(data, _byteArrayOffsets, _byteArrayData, numValues);
                break;
            }

            default:
                throw new NotSupportedException($"Dictionary not supported for physical type '{_physicalType}'.");
        }
    }

    /// <summary>Returns the value arrays to the pool. Idempotent.</summary>
    public void Dispose()
    {
        Return(ref _boolValues);
        Return(ref _int32Values);
        Return(ref _int64Values);
        Return(ref _floatValues);
        Return(ref _doubleValues);
        Return(ref _fixedByteValues);
        Return(ref _byteArrayOffsets);
        Return(ref _byteArrayData);
        Count = 0;
        _disposed = true;
    }

    private static T[] Rent<T>(int length) =>
        length == 0 ? Array.Empty<T>() : ArrayPool<T>.Shared.Rent(length);

    private static void Return<T>(ref T[]? array)
    {
        // Rented arrays are never shared, so this releases each one once even if two threads dispose.
        var rented = Interlocked.Exchange(ref array, null);
        if (rented is { Length: > 0 })
            ArrayPool<T>.Shared.Return(rented);
    }

    /// <summary>The fewest bytes a PLAIN entry of this type takes (a BYTE_ARRAY's length prefix).</summary>
    private int MinEntryBytes(int typeLength) => _physicalType switch
    {
        PhysicalType.Int32 or PhysicalType.Float or PhysicalType.ByteArray => 4,
        PhysicalType.Int64 or PhysicalType.Double => 8,
        PhysicalType.Int96 => 12,
        PhysicalType.FixedLenByteArray => typeLength,
        _ => 1,
    };

    /// <summary>
    /// Refuses a data page whose indices reach past the dictionary. The getters below trust their
    /// index, and a BYTE_ARRAY index one past the end read an offset that is not there.
    /// </summary>
    /// <param name="indices">The page's decoded indices.</param>
    /// <param name="bitWidth">The width they were encoded at: indices narrower than the dictionary's size need no check.</param>
    /// <param name="column">The column, which the error names.</param>
    /// <exception cref="ParquetFormatException">An index is not an entry of this dictionary.</exception>
    /// <exception cref="ObjectDisposedException">The dictionary was released.</exception>
    public void CheckIndices(ReadOnlySpan<int> indices, int bitWidth, Schema.ColumnDescriptor column)
    {
        // A page decoded after its chunk released the dictionary is a reader bug, not a corrupt file.
        if (_disposed)
            throw new ObjectDisposedException(nameof(DictionaryDecoder), $"Column '{column.DottedPath}': the dictionary was already released.");

        if (bitWidth < 31 && (1 << bitWidth) <= Count)
            return;

        uint count = (uint)Count;
        for (int i = 0; i < indices.Length; i++)
        {
            if ((uint)indices[i] >= count)
            {
                throw new ParquetFormatException(
                    $"Column '{column.DottedPath}': a data page refers to dictionary entry {(uint)indices[i]}, " +
                    $"but the dictionary has {Count} entries.");
            }
        }
    }

    /// <summary>Gets an Int32 dictionary value by index.</summary>
    public int GetInt32(int index) => _int32Values![index];

    /// <summary>Gets an Int64 dictionary value by index.</summary>
    public long GetInt64(int index) => _int64Values![index];

    /// <summary>Gets a Float dictionary value by index.</summary>
    public float GetFloat(int index) => _floatValues![index];

    /// <summary>Gets a Double dictionary value by index.</summary>
    public double GetDouble(int index) => _doubleValues![index];

    /// <summary>Gets a Boolean dictionary value by index.</summary>
    public bool GetBoolean(int index) => _boolValues![index];

    /// <summary>Gets a fixed-length byte array dictionary value by index (INT96 or FIXED_LEN_BYTE_ARRAY).</summary>
    public ReadOnlySpan<byte> GetFixedBytes(int index) =>
        _fixedByteValues!.AsSpan(index * _fixedLength, _fixedLength);

    /// <summary>Gets a BYTE_ARRAY dictionary value by index.</summary>
    public ReadOnlySpan<byte> GetByteArray(int index)
    {
        int start = _byteArrayOffsets![index];
        int end = _byteArrayOffsets[index + 1];
        return _byteArrayData!.AsSpan(start, end - start);
    }

    /// <summary>Gets the byte length of a BYTE_ARRAY dictionary entry without creating a span.</summary>
    public int GetByteArrayLength(int index) =>
        _byteArrayOffsets![index + 1] - _byteArrayOffsets[index];
}

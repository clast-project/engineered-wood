// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Stores a dictionary page's PLAIN-decoded values and resolves dictionary indices.
/// </summary>
internal sealed class DictionaryDecoder
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
        Count = numValues;
        switch (_physicalType)
        {
            case PhysicalType.Boolean:
                _boolValues = new bool[numValues];
                PlainDecoder.DecodeBooleans(data, _boolValues, numValues);
                break;

            case PhysicalType.Int32:
                _int32Values = new int[numValues];
                PlainDecoder.DecodeInt32s(data, _int32Values, numValues);
                break;

            case PhysicalType.Int64:
                _int64Values = new long[numValues];
                PlainDecoder.DecodeInt64s(data, _int64Values, numValues);
                break;

            case PhysicalType.Float:
                _floatValues = new float[numValues];
                PlainDecoder.DecodeFloats(data, _floatValues, numValues);
                break;

            case PhysicalType.Double:
                _doubleValues = new double[numValues];
                PlainDecoder.DecodeDoubles(data, _doubleValues, numValues);
                break;

            case PhysicalType.Int96:
                _fixedLength = 12;
                _fixedByteValues = new byte[numValues * 12];
                PlainDecoder.DecodeInt96s(data, _fixedByteValues, numValues);
                break;

            case PhysicalType.FixedLenByteArray:
                _fixedLength = typeLength;
                _fixedByteValues = new byte[numValues * typeLength];
                PlainDecoder.DecodeFixedLenByteArrays(data, _fixedByteValues, numValues, typeLength);
                break;

            case PhysicalType.ByteArray:
                _byteArrayOffsets = new int[numValues + 1];
                PlainDecoder.DecodeByteArrays(data, _byteArrayOffsets, out _byteArrayData!, numValues);
                break;

            default:
                throw new NotSupportedException($"Dictionary not supported for physical type '{_physicalType}'.");
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
        _fixedByteValues.AsSpan(index * _fixedLength, _fixedLength);

    /// <summary>Gets a BYTE_ARRAY dictionary value by index.</summary>
    public ReadOnlySpan<byte> GetByteArray(int index)
    {
        int start = _byteArrayOffsets![index];
        int end = _byteArrayOffsets[index + 1];
        return _byteArrayData.AsSpan(start, end - start);
    }

    /// <summary>Gets the byte length of a BYTE_ARRAY dictionary entry without creating a span.</summary>
    public int GetByteArrayLength(int index) =>
        _byteArrayOffsets![index + 1] - _byteArrayOffsets[index];
}

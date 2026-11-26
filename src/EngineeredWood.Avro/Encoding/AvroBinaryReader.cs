// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using EngineeredWood.Avro.Schema;
using EngineeredWood.Encodings;

namespace EngineeredWood.Avro.Encoding;

/// <summary>
/// Zero-allocation Avro binary decoder over a <see cref="ReadOnlySpan{T}"/>.
/// Reads values using the Avro binary encoding specification.
/// </summary>
internal ref struct AvroBinaryReader
{
    private readonly ReadOnlySpan<byte> _data;
    private int _pos;

    public AvroBinaryReader(ReadOnlySpan<byte> data)
    {
        _data = data;
        _pos = 0;
    }

    public int Position => _pos;
    public int Remaining => _data.Length - _pos;
    public bool IsEmpty => _pos >= _data.Length;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool ReadBoolean() => _data[_pos++] != 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ReadInt() => checked((int)Varint.ReadSigned(_data, ref _pos));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long ReadLong() => Varint.ReadSigned(_data, ref _pos);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public float ReadFloat()
    {
#if NET8_0_OR_GREATER
        var value = BinaryPrimitives.ReadSingleLittleEndian(_data.Slice(_pos));
#else
        var value = MemoryMarshal.Read<float>(_data.Slice(_pos));
#endif
        _pos += 4;
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public double ReadDouble()
    {
#if NET8_0_OR_GREATER
        var value = BinaryPrimitives.ReadDoubleLittleEndian(_data.Slice(_pos));
#else
        var value = MemoryMarshal.Read<double>(_data.Slice(_pos));
#endif
        _pos += 8;
        return value;
    }

    public ReadOnlySpan<byte> ReadBytes()
    {
        int length = checked((int)Varint.ReadSigned(_data, ref _pos));
        if (length < 0) throw new InvalidDataException("Negative bytes length in Avro data.");
        var result = _data.Slice(_pos, length);
        _pos += length;
        return result;
    }

    public ReadOnlySpan<byte> ReadStringBytes() => ReadBytes();

    public string ReadString()
    {
        var bytes = ReadStringBytes();
#if NETSTANDARD2_0
        return System.Text.Encoding.UTF8.GetString(bytes.ToArray());
#else
        return System.Text.Encoding.UTF8.GetString(bytes);
#endif
    }

    public ReadOnlySpan<byte> ReadFixed(int size)
    {
        var result = _data.Slice(_pos, size);
        _pos += size;
        return result;
    }

    /// <summary>Reads a union branch index (0-based) without checking it against the union.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ReadUnionIndex() => checked((int)Varint.ReadSigned(_data, ref _pos));

    /// <summary>
    /// Reads a union branch index (0-based) and checks that it names one of the union's
    /// <paramref name="branchCount"/> branches. An index outside the union means the data does not match
    /// its schema, or decoding has already lost its place; taking it as some branch anyway reads the bytes
    /// that follow as the wrong type, which can go on without an error for many more values.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ReadUnionIndex(int branchCount)
    {
        long index = Varint.ReadSigned(_data, ref _pos);
        if ((ulong)index >= (ulong)branchCount)
            ThrowUnionIndexOutOfRange(index, branchCount);
        return (int)index;
    }

    private static void ThrowUnionIndexOutOfRange(long index, int branchCount) =>
        throw new InvalidDataException(
            $"Avro union branch index {index} is out of range for a union of {branchCount} branches.");

    /// <summary>
    /// Skips over a value of the given schema type without materializing it.
    /// Used for skipping unwanted fields during schema evolution.
    /// </summary>
    public void Skip(AvroSchemaNode schema)
    {
        switch (schema)
        {
            case AvroPrimitiveSchema { Type: AvroType.Null }:
                break;
            case AvroPrimitiveSchema { Type: AvroType.Boolean }:
                Advance(1);
                break;
            case AvroPrimitiveSchema { Type: AvroType.Int or AvroType.Long }:
                Varint.ReadSigned(_data, ref _pos);
                break;
            case AvroPrimitiveSchema { Type: AvroType.Float }:
                Advance(4);
                break;
            case AvroPrimitiveSchema { Type: AvroType.Double }:
                Advance(8);
                break;
            case AvroPrimitiveSchema { Type: AvroType.Bytes or AvroType.String }:
                ReadBytes();
                break;
            case AvroFixedSchema f:
                Advance(f.Size);
                break;
            case AvroEnumSchema:
                Varint.ReadSigned(_data, ref _pos);
                break;
            case AvroRecordSchema r:
                // A recursive reference has no fields of its own; its values are its definition's.
                foreach (var field in (r.Definition ?? r).Fields)
                    Skip(field.Schema);
                break;
            case AvroArraySchema a:
                SkipArray(a.Items);
                break;
            case AvroMapSchema m:
                SkipMap(m.Values);
                break;
            case AvroUnionSchema u:
                Skip(u.Branches[ReadUnionIndex(u.Branches.Count)]);
                break;
            default:
                throw new InvalidOperationException($"Cannot skip unknown schema type: {schema.Type}");
        }
    }

    /// <summary>
    /// Moves past <paramref name="count"/> bytes that are skipped, not read. Reading checks its bounds through the
    /// span, but a bare advance did not, so a block that ended inside a skipped fixed-width value read on as if
    /// the bytes were there.
    /// </summary>
    private void Advance(int count)
    {
        if (count > _data.Length - _pos)
            throw new InvalidDataException(
                $"Avro data ends inside a value: {count} bytes needed at offset {_pos}, {_data.Length - _pos} left.");
        _pos += count;
    }

    private void SkipArray(AvroSchemaNode items)
    {
        while (true)
        {
            long count = Varint.ReadSigned(_data, ref _pos);
            if (count == 0) break;
            if (count < 0)
            {
                // Negative count means the next long is the byte size of the block
                count = -count;
                Varint.ReadSigned(_data, ref _pos); // skip byte size
            }
            for (long i = 0; i < count; i++)
                Skip(items);
        }
    }

    private void SkipMap(AvroSchemaNode values)
    {
        while (true)
        {
            long count = Varint.ReadSigned(_data, ref _pos);
            if (count == 0) break;
            if (count < 0)
            {
                count = -count;
                Varint.ReadSigned(_data, ref _pos); // skip byte size
            }
            for (long i = 0; i < count; i++)
            {
                ReadBytes(); // skip key (string)
                Skip(values);
            }
        }
    }
}

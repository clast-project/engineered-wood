// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using EngineeredWood.Encodings;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// EXPERIMENT (perf/selection-pushdown-ab): PLAIN and dictionary decoders that write only the values
/// <see cref="ColumnBuildState.PageValueMask"/> keeps, so a dropped value is never copied into the state.
/// </summary>
/// <remarks>
/// Covers INT32/INT64/FLOAT/DOUBLE and offset-mode BYTE_ARRAY. Anything else answers false, and the
/// caller decodes the page whole and compacts it afterwards.
/// </remarks>
internal static class SelectiveDecoder
{
    public static bool TryDecode(
        ReadOnlySpan<byte> data,
        Encoding encoding,
        ColumnDescriptor column,
        DictionaryDecoder? dictionary,
        int count,
        ColumnBuildState state)
    {
        if (column.PhysicalType == PhysicalType.ByteArray && state.IsViewMode)
            return false;
        if (column.PhysicalType is not (PhysicalType.Int32 or PhysicalType.Int64 or PhysicalType.Float
            or PhysicalType.Double or PhysicalType.ByteArray))
            return false;

        if (encoding == Encoding.Plain)
        {
            DecodePlain(data, column, count, state);
            return true;
        }

        if (encoding == Encoding.DeltaLengthByteArray && column.PhysicalType == PhysicalType.ByteArray)
        {
            DecodeDeltaLengthByteArrays(data, count, state);
            return true;
        }

        if (encoding is Encoding.PlainDictionary or Encoding.RleDictionary && dictionary is not null)
        {
            DecodeDictionary(data, column, dictionary, count, state);
            return true;
        }

        return false;
    }

    private static void DecodePlain(ReadOnlySpan<byte> data, ColumnDescriptor column, int count, ColumnBuildState state)
    {
        var mask = state.PageValueMask;
        int kept = state.PageKeptValues;
        switch (column.PhysicalType)
        {
            case PhysicalType.Int32: GatherPlain<int>(data, mask, count, kept, state); break;
            case PhysicalType.Int64: GatherPlain<long>(data, mask, count, kept, state); break;
            case PhysicalType.Float: GatherPlain<float>(data, mask, count, kept, state); break;
            case PhysicalType.Double: GatherPlain<double>(data, mask, count, kept, state); break;
            case PhysicalType.ByteArray: DecodePlainByteArrays(data, column, mask, count, kept, state); break;
        }
    }

    private static void GatherPlain<T>(ReadOnlySpan<byte> data, ReadOnlySpan<byte> mask, int count, int kept, ColumnBuildState state)
        where T : unmanaged
    {
        var source = MemoryMarshal.Cast<byte, T>(data);
        if (source.Length < count)
            throw new ParquetFormatException($"A PLAIN page holds {source.Length} values; {count} were expected.");

        // Branch-free: reserve room for every value, write each, advance only past kept ones, give back the rest.
        var dest = state.ReserveValues<T>(count);
        Gather(source, mask, count, dest);
        state.ReleaseValues<T>(count - kept);
    }

    private static void Gather<T>(ReadOnlySpan<T> source, ReadOnlySpan<byte> mask, int count, Span<T> dest)
    {
        int write = 0;
        for (int i = 0; i < count; i++)
        {
            dest[write] = source[i];
            write += mask[i];
        }
    }

    private static void DecodePlainByteArrays(
        ReadOnlySpan<byte> data, ColumnDescriptor column, ReadOnlySpan<byte> mask, int count, int kept,
        ColumnBuildState state)
    {
        var offsets = ArrayPool<int>.Shared.Rent(kept + 1);
        var starts = ArrayPool<int>.Shared.Rent(Math.Max(kept, 1));
        try
        {
            // Lengths prefix every value, so the walk visits each; only kept values are recorded.
            int pos = 0, total = 0, write = 0;
            offsets[0] = 0;
            for (int i = 0; i < count; i++)
            {
                if (pos > data.Length - 4)
                    throw new ParquetFormatException($"Column '{column.DottedPath}': a PLAIN page ends early.");
                int len = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(pos));
                pos += 4;
                if (len < 0 || len > data.Length - pos)
                    throw new ParquetFormatException($"Column '{column.DottedPath}': a PLAIN value overruns its page.");
                if (mask[i] != 0)
                {
                    starts[write] = pos;
                    total += len;
                    offsets[++write] = total;
                }
                pos += len;
            }

            var dest = state.ReserveByteArrayData(total);
            for (int i = 0; i < kept; i++)
                data.Slice(starts[i], offsets[i + 1] - offsets[i]).CopyTo(dest.Slice(offsets[i]));
            state.CommitByteArrayData(offsets.AsSpan(0, kept + 1), kept, total);
        }
        finally
        {
            ArrayPool<int>.Shared.Return(offsets);
            ArrayPool<int>.Shared.Return(starts);
        }
    }

    private static void DecodeDeltaLengthByteArrays(ReadOnlySpan<byte> data, int count, ColumnBuildState state)
    {
        var mask = state.PageValueMask;
        int kept = state.PageKeptValues;
        var lengthDecoder = new DeltaBinaryPackedDecoder(data);
        var lengths = new int[count];
        lengthDecoder.DecodeInt32s(lengths);

        var offsets = ArrayPool<int>.Shared.Rent(kept + 1);
        try
        {
            int total = 0, w = 0;
            offsets[0] = 0;
            for (int i = 0; i < count; i++)
            {
                if (mask[i] != 0)
                {
                    total += lengths[i];
                    offsets[++w] = total;
                }
            }

            // The values are contiguous, so each run of kept values is one copy.
            var source = data.Slice(lengthDecoder.BytesConsumed);
            var dest = state.ReserveByteArrayData(total);
            int readPos = 0, writePos = 0, i2 = 0;
            while (i2 < count)
            {
                int runStart = readPos;
                bool keep = mask[i2] != 0;
                while (i2 < count && (mask[i2] != 0) == keep)
                    readPos += lengths[i2++];
                if (keep)
                {
                    int len = readPos - runStart;
                    source.Slice(runStart, len).CopyTo(dest.Slice(writePos));
                    writePos += len;
                }
            }
            state.CommitByteArrayData(offsets.AsSpan(0, kept + 1), kept, total);
        }
        finally
        {
            ArrayPool<int>.Shared.Return(offsets);
        }
    }

    private static void DecodeDictionary(
        ReadOnlySpan<byte> data, ColumnDescriptor column, DictionaryDecoder dictionary, int count,
        ColumnBuildState state)
    {
        if (count == 0)
            return;
        int bitWidth = data[0];
        if (bitWidth > 32)
            throw new ParquetFormatException(
                $"Column '{column.DottedPath}': a dictionary-encoded page's indices are {bitWidth} bits wide.");

        var mask = state.PageValueMask;
        int kept = state.PageKeptValues;
        var indicesArray = ArrayPool<int>.Shared.Rent(count);
        try
        {
            // The RLE runs are decoded whole: skipping inside them is a further step not taken here.
            new RleBitPackedDecoder(data.Slice(1), bitWidth).ReadBatch(indicesArray.AsSpan(0, count));
            ReadOnlySpan<int> indices = indicesArray.AsSpan(0, count);
            dictionary.CheckIndices(indices, bitWidth, column);

            switch (column.PhysicalType)
            {
                case PhysicalType.Int32:
                {
                    var dest = state.ReserveValues<int>(count);
                    int w = 0;
                    for (int i = 0; i < count; i++)
                    {
                        dest[w] = dictionary.GetInt32(indices[i]);
                        w += mask[i];
                    }
                    state.ReleaseValues<int>(count - kept);
                    break;
                }
                case PhysicalType.Int64:
                {
                    var dest = state.ReserveValues<long>(count);
                    int w = 0;
                    for (int i = 0; i < count; i++)
                    {
                        dest[w] = dictionary.GetInt64(indices[i]);
                        w += mask[i];
                    }
                    state.ReleaseValues<long>(count - kept);
                    break;
                }
                case PhysicalType.Float:
                {
                    var dest = state.ReserveValues<float>(count);
                    int w = 0;
                    for (int i = 0; i < count; i++)
                    {
                        dest[w] = dictionary.GetFloat(indices[i]);
                        w += mask[i];
                    }
                    state.ReleaseValues<float>(count - kept);
                    break;
                }
                case PhysicalType.Double:
                {
                    var dest = state.ReserveValues<double>(count);
                    int w = 0;
                    for (int i = 0; i < count; i++)
                    {
                        dest[w] = dictionary.GetDouble(indices[i]);
                        w += mask[i];
                    }
                    state.ReleaseValues<double>(count - kept);
                    break;
                }
                case PhysicalType.ByteArray:
                {
                    var offsets = ArrayPool<int>.Shared.Rent(kept + 1);
                    try
                    {
                        int total = 0, w = 0;
                        offsets[0] = 0;
                        for (int i = 0; i < count; i++)
                        {
                            if (mask[i] != 0)
                            {
                                total += dictionary.GetByteArrayLength(indices[i]);
                                offsets[++w] = total;
                            }
                        }

                        var dest = state.ReserveByteArrayData(total);
                        w = 0;
                        for (int i = 0; i < count; i++)
                        {
                            if (mask[i] != 0)
                                dictionary.GetByteArray(indices[i]).CopyTo(dest.Slice(offsets[w++]));
                        }
                        state.CommitByteArrayData(offsets.AsSpan(0, kept + 1), kept, total);
                    }
                    finally
                    {
                        ArrayPool<int>.Shared.Return(offsets);
                    }
                    break;
                }
            }
        }
        finally
        {
            ArrayPool<int>.Shared.Return(indicesArray);
        }
    }
}

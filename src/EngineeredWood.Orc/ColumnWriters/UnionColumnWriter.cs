// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;
using ProtoStream = EngineeredWood.Orc.Proto.Stream;

namespace EngineeredWood.Orc.ColumnWriters;

internal sealed class UnionColumnWriter : ColumnWriter
{
    private readonly GrowableBuffer _tagStream = new();
    private readonly ByteRleEncoder _tagEncoder;
    private readonly List<ColumnWriter> _children = [];

    public UnionColumnWriter(int columnId) : base(columnId)
    {
        _tagEncoder = new ByteRleEncoder(_tagStream);
    }

    public void AddChild(ColumnWriter child) => _children.Add(child);

    public override void Write(IArrowArray array)
    {
        WritePresent(array);
        var unionArray = (UnionArray)array;

        // Write tags for non-null values
        Span<byte> tagBuf = stackalloc byte[1];
        for (int i = 0; i < unionArray.Length; i++)
        {
            if (!unionArray.IsValid(i)) continue;
            tagBuf[0] = unionArray.TypeIds[i];
            _tagEncoder.WriteValues(tagBuf);
        }

        if (unionArray is DenseUnionArray denseUnion)
            WriteDenseChildren(denseUnion);
        else
            WriteSparseChildren((SparseUnionArray)unionArray);
    }

    private void WriteDenseChildren(DenseUnionArray denseUnion)
    {
        // For dense union, each child gets a slice of values.
        // Group values by type tag and write each child's values.
        // ORC stores child values contiguously per tag — same as dense union offsets.

        // Count values per child
        var counts = new int[_children.Count];
        for (int i = 0; i < denseUnion.Length; i++)
        {
            if (!denseUnion.IsValid(i)) continue;
            int tag = denseUnion.TypeIds[i];
            if (tag < counts.Length) counts[tag]++;
        }

        // Each child array in dense union already contains only its values
        for (int c = 0; c < _children.Count; c++)
        {
            if (counts[c] > 0)
            {
                var childArray = denseUnion.Fields[c];
                _children[c].Write(childArray);
            }
        }
    }

    private void WriteSparseChildren(SparseUnionArray sparseUnion)
    {
        // For sparse union, all child arrays are full-length.
        // ORC stores only the tag-selected values for each child.
        // We must extract selected values into compact arrays.

        for (int c = 0; c < _children.Count; c++)
        {
            // Collect indices where this child is selected
            var indices = new List<int>();
            for (int i = 0; i < sparseUnion.Length; i++)
            {
                if (sparseUnion.IsValid(i) && sparseUnion.TypeIds[i] == c)
                    indices.Add(i);
            }

            if (indices.Count == 0) continue;

            var childArray = sparseUnion.Fields[c];
            var compactArray = TakeIndices(childArray, indices);
            _children[c].Write(compactArray);
        }
    }

    /// <summary>
    /// Extracts elements at the given indices from an Arrow array into a new compact array.
    /// </summary>
    private static IArrowArray TakeIndices(IArrowArray array, List<int> indices)
    {
        var data = array.Data;
        var type = data.DataType;

        // Build validity bitmap
        var validityBuilder = new ArrowBuffer.BitmapBuilder();
        int nullCount = 0;
        foreach (int idx in indices)
        {
            bool valid = array.IsValid(idx);
            validityBuilder.Append(valid);
            if (!valid) nullCount++;
        }
        var validityBuffer = nullCount > 0 ? validityBuilder.Build() : ArrowBuffer.Empty;

        return type switch
        {
            // Fixed-width primitive types
            BooleanType => TakeBooleans(data, indices, validityBuffer, nullCount),
            Int8Type or UInt8Type => TakeFixedWidth(data, indices, 1, validityBuffer, nullCount),
            Int16Type or UInt16Type or HalfFloatType => TakeFixedWidth(data, indices, 2, validityBuffer, nullCount),
            Int32Type or UInt32Type or FloatType or Date32Type => TakeFixedWidth(data, indices, 4, validityBuffer, nullCount),
            Int64Type or UInt64Type or DoubleType or TimestampType or Date64Type => TakeFixedWidth(data, indices, 8, validityBuffer, nullCount),
            Decimal128Type => TakeFixedWidth(data, indices, 16, validityBuffer, nullCount),
            Decimal256Type => TakeFixedWidth(data, indices, 32, validityBuffer, nullCount),

            // Variable-width types (offset-based)
            StringType or BinaryType => TakeVariableWidth(data, indices, validityBuffer, nullCount),

            // Nested types — delegate to child extraction
            StructType => TakeStruct(data, indices, validityBuffer, nullCount),
            ListType => TakeList(data, indices, validityBuffer, nullCount),

            _ => throw new NotSupportedException($"TakeIndices not supported for {type}")
        };
    }

    private static IArrowArray TakeBooleans(ArrayData data, List<int> indices,
        ArrowBuffer validityBuffer, int nullCount)
    {
        var builder = new ArrowBuffer.BitmapBuilder();
        var valueBuf = data.Buffers[1];
        var valueSpan = valueBuf.Span;
        foreach (int idx in indices)
        {
            int actualIdx = idx + data.Offset;
            bool value = (valueSpan[actualIdx >> 3] & (1 << (actualIdx & 7))) != 0;
            builder.Append(value);
        }

        var newData = new ArrayData(data.DataType, indices.Count, nullCount, 0,
            [validityBuffer, builder.Build()]);
        return ArrowArrayFactory.BuildArray(newData);
    }

    private static IArrowArray TakeFixedWidth(ArrayData data, List<int> indices,
        int byteWidth, ArrowBuffer validityBuffer, int nullCount)
    {
        var valueBytes = new byte[indices.Count * byteWidth];
        var srcSpan = data.Buffers[1].Span;
        int srcOffset = data.Offset * byteWidth;
        for (int i = 0; i < indices.Count; i++)
        {
            srcSpan.Slice(srcOffset + indices[i] * byteWidth, byteWidth)
                .CopyTo(valueBytes.AsSpan(i * byteWidth));
        }

        var newData = new ArrayData(data.DataType, indices.Count, nullCount, 0,
            [validityBuffer, new ArrowBuffer(valueBytes)]);
        return ArrowArrayFactory.BuildArray(newData);
    }

    private static IArrowArray TakeVariableWidth(ArrayData data, List<int> indices,
        ArrowBuffer validityBuffer, int nullCount)
    {
        var offsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
        var srcData = data.Buffers[2].Span;
        int baseOffset = data.Offset;

        // Build new offsets and data
        var newOffsets = new int[indices.Count + 1];
        var newDataStream = new GrowableBuffer();
        newOffsets[0] = 0;
        for (int i = 0; i < indices.Count; i++)
        {
            int srcIdx = baseOffset + indices[i];
            int start = offsets[srcIdx];
            int end = offsets[srcIdx + 1];
            int len = end - start;
            if (len > 0)
                newDataStream.Write(srcData.Slice(start, len));
            newOffsets[i + 1] = newOffsets[i] + len;
        }

        var newData = new ArrayData(data.DataType, indices.Count, nullCount, 0,
            [validityBuffer,
             new ArrowBuffer(MemoryMarshal.AsBytes(newOffsets.AsSpan()).ToArray()),
             new ArrowBuffer(newDataStream.WrittenSpan.ToArray())]);
        return ArrowArrayFactory.BuildArray(newData);
    }

    private static IArrowArray TakeStruct(ArrayData data, List<int> indices,
        ArrowBuffer validityBuffer, int nullCount)
    {
        var childArrays = new ArrayData[data.Children.Length];
        for (int i = 0; i < data.Children.Length; i++)
        {
            var childArr = ArrowArrayFactory.BuildArray(data.Children[i]);
            var taken = TakeIndices(childArr, indices);
            childArrays[i] = taken.Data;
        }

        var newData = new ArrayData(data.DataType, indices.Count, nullCount, 0,
            [validityBuffer], childArrays);
        return ArrowArrayFactory.BuildArray(newData);
    }

    private static IArrowArray TakeList(ArrayData data, List<int> indices,
        ArrowBuffer validityBuffer, int nullCount)
    {
        var offsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
        int baseOffset = data.Offset;

        // Collect all child element indices and build new offsets
        var newOffsets = new int[indices.Count + 1];
        var childIndices = new List<int>();
        newOffsets[0] = 0;
        for (int i = 0; i < indices.Count; i++)
        {
            int srcIdx = baseOffset + indices[i];
            int start = offsets[srcIdx];
            int end = offsets[srcIdx + 1];
            for (int j = start; j < end; j++)
                childIndices.Add(j);
            newOffsets[i + 1] = newOffsets[i] + (end - start);
        }

        var childArr = ArrowArrayFactory.BuildArray(data.Children[0]);
        var takenChild = TakeIndices(childArr, childIndices);

        var newData = new ArrayData(data.DataType, indices.Count, nullCount, 0,
            [validityBuffer, new ArrowBuffer(MemoryMarshal.AsBytes(newOffsets.AsSpan()).ToArray())],
            [takenChild.Data]);
        return ArrowArrayFactory.BuildArray(newData);
    }

    public override ColumnEncoding GetEncoding() => new() { Kind = ColumnEncoding.Types.Kind.Direct };

    public override long EstimateBufferedBytes() =>
        base.EstimateBufferedBytes() + _tagStream.Length + _tagEncoder.BufferedCount;

    public override void FlushEncoders()
    {
        base.FlushEncoders();
        _tagEncoder.Flush();
    }

    public override void GetStreamPositions(IList<ulong> positions)
    {
        base.GetStreamPositions(positions);
        positions.Add((ulong)_tagStream.Length);
        positions.Add(0); // byte RLE remaining
    }

    public override void GetPositionLayout(IList<int> extrasPerStream)
    {
        base.GetPositionLayout(extrasPerStream);
        extrasPerStream.Add(1); // DATA (byte RLE): rle_remaining
    }

    public override void GetStreams(List<OrcStream> streams)
    {
        base.GetStreams(streams);
        _tagEncoder.Flush();
        streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.Data, _tagStream));
    }

    public override void Reset()
    {
        base.Reset();
        _tagStream.Reset();
    }
}

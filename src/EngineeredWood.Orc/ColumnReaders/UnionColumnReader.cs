// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.ColumnReaders;

internal sealed class UnionColumnReader : ColumnReader
{
    private readonly OrcSchema _schema;
    private readonly List<ColumnReader> _childReaders = [];
    private ByteRleDecoder? _tagDecoder;

    public UnionColumnReader(OrcSchema schema) : base(schema.ColumnId)
    {
        _schema = schema;
    }

    public void AddChildReader(ColumnReader reader) => _childReaders.Add(reader);

    public void SetDataStream(OrcByteStream stream)
    {
        _tagDecoder = new ByteRleDecoder(stream);
    }

    public override IArrowArray ReadBatch(int batchSize)
    {
        var present = ReadPresent(batchSize);
        int nonNullCount = CountNonNull(present, batchSize);

        // Read tags for non-null values
        var tags = new byte[nonNullCount];
        if (nonNullCount > 0 && _tagDecoder != null)
            _tagDecoder.ReadValues(tags);

        // Expand tags to full batch size (nulls get tag 0)
        var fullTags = new byte[batchSize];
        if (present != null)
        {
            int tagIdx = 0;
            for (int i = 0; i < batchSize; i++)
            {
                if (present[i])
                    fullTags[i] = tags[tagIdx++];
            }
        }
        else
        {
            tags.CopyTo(fullTags.AsSpan());
        }

        // Count how many values each child needs to read.
        // ORC union is like Arrow dense union: each child only stores values
        // for rows where that child's tag is selected.
        var childCounts = new int[_childReaders.Count];
        for (int i = 0; i < batchSize; i++)
        {
            if (present == null || present[i])
            {
                int tag = fullTags[i];
                if (tag < childCounts.Length)
                    childCounts[tag]++;
            }
        }

        // Read child arrays (each child reads only its count)
        var childArrays = new IArrowArray[_childReaders.Count];
        for (int i = 0; i < _childReaders.Count; i++)
        {
            if (childCounts[i] > 0)
                childArrays[i] = _childReaders[i].ReadBatch(childCounts[i]);
            else
                childArrays[i] = CreateEmptyArray(_schema.Children[i].ToArrowType());
        }

        // Build offsets for dense union: maps each row to its index within the child array
        var offsets = new int[batchSize];
        var childOffsets = new int[_childReaders.Count];
        for (int i = 0; i < batchSize; i++)
        {
            int tag = fullTags[i];
            if (tag < childOffsets.Length)
            {
                offsets[i] = childOffsets[tag];
                if (present == null || present[i])
                    childOffsets[tag]++;
            }
        }

        // Build fields for the union type
        var fields = new List<Field>();
        var typeIds = new int[_childReaders.Count];
        for (int i = 0; i < _schema.Children.Count; i++)
        {
            var child = _schema.Children[i];
            fields.Add(new Field(child.Name ?? $"col_{child.ColumnId}", child.ToArrowType(), nullable: true));
            typeIds[i] = i;
        }

        var unionType = new UnionType(fields, typeIds, UnionMode.Dense);

        // Build Arrow buffers
        var typeIdBuffer = new ArrowBuffer(fullTags);
        var offsetBuffer = new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray());

        return new DenseUnionArray(unionType, batchSize, childArrays, typeIdBuffer, offsetBuffer);
    }

    private static IArrowArray CreateEmptyArray(IArrowType type)
    {
        return type switch
        {
            BooleanType => new BooleanArray.Builder().Build(),
            Int8Type => new Int8Array.Builder().Build(),
            Int16Type => new Int16Array.Builder().Build(),
            Int32Type => new Int32Array.Builder().Build(),
            Int64Type => new Int64Array.Builder().Build(),
            FloatType => new FloatArray.Builder().Build(),
            DoubleType => new DoubleArray.Builder().Build(),
            StringType => new StringArray.Builder().Build(),
            BinaryType => new BinaryArray.Builder().Build(),
            _ => new StringArray.Builder().Build(), // fallback
        };
    }
}

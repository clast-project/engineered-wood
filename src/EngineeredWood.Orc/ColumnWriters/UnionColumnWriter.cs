// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using EngineeredWood.Arrow;
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
        // ORC stores each branch's values for the rows that select it, in row order. A dense union's child
        // need not be exactly that (a sliced union's children reach past it, and a child may hold values no
        // row points at), so each branch gets the values its rows point at, as runs of slices. Nothing is
        // gathered, so a branch of any type, a union included, is written as is (#585).
        var runs = new List<(int Start, int Length)>[_children.Count];
        for (int c = 0; c < runs.Length; c++)
            runs[c] = [];
        var offsets = denseUnion.ValueOffsets;
        for (int i = 0; i < denseUnion.Length; i++)
        {
            if (!denseUnion.IsValid(i)) continue;
            int tag = denseUnion.TypeIds[i];
            if (tag >= runs.Length) continue;
            var branch = runs[tag];
            int offset = offsets[i];
            if (branch.Count > 0 && branch[branch.Count - 1].Start + branch[branch.Count - 1].Length == offset)
                branch[branch.Count - 1] = (branch[branch.Count - 1].Start, branch[branch.Count - 1].Length + 1);
            else
                branch.Add((offset, 1));
        }

        for (int c = 0; c < _children.Count; c++)
            WriteParts(_children[c], denseUnion.Fields[c], runs[c]);
    }

    private void WriteSparseChildren(SparseUnionArray sparseUnion)
    {
        // A sparse union's children are full length; ORC stores each branch's values for the rows that select
        // it. Each branch gets the runs of its rows as slices, which honour every offset on the way down, a
        // nested struct's included (#585).
        var runs = new List<(int Start, int Length)>[_children.Count];
        for (int c = 0; c < runs.Length; c++)
            runs[c] = [];
        for (int i = 0; i < sparseUnion.Length; i++)
        {
            if (!sparseUnion.IsValid(i)) continue;
            int tag = sparseUnion.TypeIds[i];
            if (tag >= runs.Length) continue;
            var branch = runs[tag];
            if (branch.Count > 0 && branch[branch.Count - 1].Start + branch[branch.Count - 1].Length == i)
                branch[branch.Count - 1] = (branch[branch.Count - 1].Start, branch[branch.Count - 1].Length + 1);
            else
                branch.Add((i, 1));
        }

        for (int c = 0; c < _children.Count; c++)
            WriteParts(_children[c], sparseUnion.Fields[c], runs[c]);
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

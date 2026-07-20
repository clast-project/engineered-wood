// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.ColumnWriters;

internal sealed class StructColumnWriter : ColumnWriter
{
    private readonly List<ColumnWriter> _children = [];

    public StructColumnWriter(int columnId) : base(columnId) { }

    public void AddChild(ColumnWriter child) => _children.Add(child);
    public IReadOnlyList<ColumnWriter> Children => _children;

    public override void Write(IArrowArray array)
    {
        WritePresent(array);
        var structArray = (StructArray)array;

        for (int i = 0; i < _children.Count; i++)
        {
            _children[i].Write(structArray.Fields[i]);
        }
    }

    public override ColumnEncoding GetEncoding() => new() { Kind = ColumnEncoding.Types.Kind.Direct };

    // Struct has no data streams of its own, only PRESENT (handled by base).
    // No additional positions, flush, or stats to manage.
}

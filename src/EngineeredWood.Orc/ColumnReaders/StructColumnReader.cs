// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.ColumnReaders;

internal sealed class StructColumnReader : ColumnReader
{
    private readonly OrcSchema _schema;
    private readonly List<ColumnReader> _childReaders = [];

    public StructColumnReader(OrcSchema schema) : base(schema.ColumnId)
    {
        _schema = schema;
    }

    public void AddChildReader(ColumnReader reader) => _childReaders.Add(reader);
    public IReadOnlyList<ColumnReader> ChildReaders => _childReaders;

    /// <summary>The file was written by EngineeredWood 0.1 or 0.2, which wrote a struct's children for every
    /// row, null rows included (#585).</summary>
    internal bool ChildrenForEveryRow { get; set; }

    public override IArrowArray ReadBatch(int batchSize)
    {
        var present = ReadPresent(batchSize);
        int nullCount = present == null ? 0 : batchSize - CountNonNull(present, batchSize);

        // A struct's children hold values only for its non-null rows. Each child reads the whole batch with this
        // struct's present rows as its parent mask: a row the struct does not have is null in the child and takes
        // nothing from its streams, so nothing is gathered or copied (#585).
        var parentMask = nullCount > 0 && !ChildrenForEveryRow ? present : null;
        var childArrays = new IArrowArray[_childReaders.Count];
        for (int i = 0; i < _childReaders.Count; i++)
        {
            _childReaders[i].ParentPresent = parentMask;
            childArrays[i] = _childReaders[i].ReadBatch(batchSize);
        }

        var fields = new List<Field>();
        for (int i = 0; i < _schema.Children.Count; i++)
        {
            var child = _schema.Children[i];
            fields.Add(new Field(child.Name ?? $"col_{child.ColumnId}", child.ToArrowType(), nullable: true));
        }

        var structType = new StructType(fields);
        var validityBuffer = CreateValidityBuffer(present, batchSize);

        return new StructArray(structType, batchSize, childArrays, validityBuffer, nullCount);
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Arrow;
using System.Buffers;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.ColumnReaders;

/// <summary>
/// Base class for reading ORC column data and producing Arrow arrays.
/// </summary>
internal abstract class ColumnReader
{
    public int ColumnId { get; }
    private BooleanDecoder? _presentDecoder;

    protected ColumnReader(int columnId)
    {
        ColumnId = columnId;
    }

    public void SetPresentStream(OrcByteStream? stream)
    {
        _presentDecoder = stream != null ? new BooleanDecoder(stream) : null;
    }

    /// <summary>
    /// Reads a batch of values and returns an Arrow array.
    /// </summary>
    public abstract IArrowArray ReadBatch(int batchSize);

    /// <summary>The file was written by EngineeredWood before #584 (<c>"EngineeredWood.Orc 0.1"</c>): its signed
    /// RLEv2 Patched Base runs and its timestamps follow that writer's rules, not the spec. Set before any stream
    /// is assigned.</summary>
    internal bool LegacyEngineeredWood { get; set; }

    /// <summary>
    /// Reads the PRESENT (null) bitmap for the given batch size.
    /// Returns null if no PRESENT stream exists (all values are non-null).
    /// The decoder is preserved across batches to maintain bit alignment.
    /// </summary>
    // Reusable present buffer to avoid per-batch allocation
    private bool[]? _presentBuffer;

    /// <summary>
    /// The present rows of the struct this column is a field of, for the batch about to be read, or null when
    /// all are present. A struct's fields hold values only for its present rows (#585), so a row the struct does
    /// not have is null here too and takes no PRESENT bit or value. Set by <see cref="StructColumnReader"/>
    /// before each batch.
    /// </summary>
    internal bool[]? ParentPresent { get; set; }

    protected bool[]? ReadPresent(int batchSize)
    {
        var parent = ParentPresent;
        if (parent == null && _presentDecoder == null) return null;
        if (_presentBuffer == null || _presentBuffer.Length < batchSize)
            _presentBuffer = new bool[batchSize];
        var present = _presentBuffer.AsSpan(0, batchSize);

        if (parent == null)
        {
            _presentDecoder!.ReadValues(present);
        }
        else if (_presentDecoder == null)
        {
            parent.AsSpan(0, batchSize).CopyTo(present);
        }
        else
        {
            // The column's own bits cover only the parent's present rows. They are decoded into the front of the
            // buffer and spread out from the back: a row's own bit is never after the row, so none is overwritten
            // before it is read.
            int k = CountNonNull(parent, batchSize);
            _presentDecoder.ReadValues(present.Slice(0, k));
            for (int i = batchSize - 1; i >= 0; i--)
                present[i] = parent[i] && present[--k];
        }
        return _presentBuffer;
    }

    protected static int CountNonNull(bool[]? present, int batchSize)
    {
        if (present == null) return batchSize;
        var span = present.AsSpan(0, batchSize);
        int nonNull = 0;

        foreach (bool v in span)
            nonNull += v ? 1 : 0;

        return nonNull;
    }

    /// <summary>
    /// Creates an Arrow validity (null) bitmap buffer from the present array.
    /// Uses SIMD where hardware-accelerated.
    /// </summary>
    protected static ArrowBuffer CreateValidityBuffer(bool[]? present, int length)
    {
        if (present == null) return ArrowBuffer.Empty;

        int byteCount = (length + 7) / 8;
        using var buf = new NativeBuffer<byte>(byteCount, zeroFill: false);
        BitmapHelper.BuildFromBooleans(present.AsSpan(0, length), buf.Span, length);
        return buf.Build();
    }

    /// <summary>
    /// Recursively creates a column reader tree for the given schema node.
    /// Populates the allReaders dictionary with all readers keyed by column ID.
    /// </summary>
    public static ColumnReader Create(OrcSchema schema, IReadOnlyList<ColumnEncoding> encodings, Dictionary<int, ColumnReader> allReaders)
    {
        var encoding = schema.ColumnId < encodings.Count
            ? encodings[schema.ColumnId].Kind
            : ColumnEncoding.Types.Kind.Direct;

        ColumnReader reader;

        if (schema.Kind == Proto.Type.Types.Kind.Struct)
        {
            var structReader = new StructColumnReader(schema);
            foreach (var child in schema.Children)
            {
                var childReader = Create(child, encodings, allReaders);
                structReader.AddChildReader(childReader);
            }
            reader = structReader;
        }
        else if (schema.Kind == Proto.Type.Types.Kind.List)
        {
            var listReader = new ListColumnReader(schema, encoding);
            if (schema.Children.Count > 0)
            {
                var elementReader = Create(schema.Children[0], encodings, allReaders);
                listReader.SetElementReader(elementReader);
            }
            reader = listReader;
        }
        else if (schema.Kind == Proto.Type.Types.Kind.Map)
        {
            var mapReader = new MapColumnReader(schema, encoding);
            if (schema.Children.Count >= 2)
            {
                mapReader.SetKeyReader(Create(schema.Children[0], encodings, allReaders));
                mapReader.SetValueReader(Create(schema.Children[1], encodings, allReaders));
            }
            reader = mapReader;
        }
        else if (schema.Kind == Proto.Type.Types.Kind.Union)
        {
            var unionReader = new UnionColumnReader(schema);
            foreach (var child in schema.Children)
            {
                var childReader = Create(child, encodings, allReaders);
                unionReader.AddChildReader(childReader);
            }
            reader = unionReader;
        }
        else
        {
            reader = schema.Kind switch
            {
                Proto.Type.Types.Kind.Boolean => new BooleanColumnReader(schema.ColumnId),
                Proto.Type.Types.Kind.Byte => new ByteColumnReader(schema.ColumnId),
                Proto.Type.Types.Kind.Short => new IntegerColumnReader(schema.ColumnId, encoding, Int16Type.Default),
                Proto.Type.Types.Kind.Int => new IntegerColumnReader(schema.ColumnId, encoding, Int32Type.Default),
                Proto.Type.Types.Kind.Long => new IntegerColumnReader(schema.ColumnId, encoding, Int64Type.Default),
                Proto.Type.Types.Kind.Float => new FloatColumnReader(schema.ColumnId),
                Proto.Type.Types.Kind.Double => new DoubleColumnReader(schema.ColumnId),
                Proto.Type.Types.Kind.String or Proto.Type.Types.Kind.Varchar or Proto.Type.Types.Kind.Char =>
                    new StringColumnReader(schema.ColumnId, encoding),
                Proto.Type.Types.Kind.Binary => new BinaryColumnReader(schema.ColumnId, encoding),
                Proto.Type.Types.Kind.Date => new DateColumnReader(schema.ColumnId, encoding),
                Proto.Type.Types.Kind.Timestamp or Proto.Type.Types.Kind.TimestampInstant =>
                    new TimestampColumnReader(schema.ColumnId, encoding, schema.Kind == Proto.Type.Types.Kind.TimestampInstant),
                Proto.Type.Types.Kind.Decimal => new DecimalColumnReader(schema.ColumnId, encoding, (int)schema.Precision, (int)schema.Scale),
                _ => throw new NotSupportedException($"Column type {schema.Kind} is not yet supported.")
            };
        }

        allReaders[schema.ColumnId] = reader;
        return reader;
    }
}

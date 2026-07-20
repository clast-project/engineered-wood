// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.BloomFilter;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;
using ProtoStream = EngineeredWood.Orc.Proto.Stream;

namespace EngineeredWood.Orc.ColumnWriters;

/// <summary>
/// Base class for writing ORC column data from Arrow arrays.
/// Each writer accumulates data for a stripe, then produces streams on flush.
/// </summary>
internal abstract class ColumnWriter
{
    public int ColumnId { get; }
    protected readonly GrowableBuffer PresentStream = new();
    protected readonly BooleanEncoder PresentEncoder;
    protected bool HasNulls;          // Row-group level: reset at row group boundaries
    protected bool StripeHasNulls;    // Stripe level: only reset with full Reset()
    protected long RowCount;
    protected long NonNullCount;

    internal OrcBloomFilterBuilder? BloomFilter { get; set; }
    private List<Proto.BloomFilter>? _bloomFilterEntries;

    protected ColumnWriter(int columnId)
    {
        ColumnId = columnId;
        PresentEncoder = new BooleanEncoder(PresentStream);
    }

    /// <summary>
    /// Writes the null bitmap (PRESENT stream) for the given array.
    /// Returns the number of non-null values.
    /// </summary>
    protected int WritePresent(IArrowArray array)
    {
        int length = array.Length;
        RowCount += length;

        if (array.NullCount == 0)
        {
            NonNullCount += length;
            return length;
        }

        HasNulls = true;
        StripeHasNulls = true;
        int nonNullCount = 0;
        Span<bool> v = stackalloc bool[1];
        for (int i = 0; i < length; i++)
        {
            bool isValid = array.IsValid(i);
            v[0] = isValid;
            PresentEncoder.WriteValues(v);
            if (isValid) nonNullCount++;
        }
        NonNullCount += nonNullCount;
        return nonNullCount;
    }

    /// <summary>
    /// Writes data from an Arrow array.
    /// </summary>
    public abstract void Write(IArrowArray array);

    /// <summary>
    /// Returns the ORC column encoding for this writer.
    /// </summary>
    public abstract ColumnEncoding GetEncoding();

    /// <summary>
    /// Returns the column statistics collected so far.
    /// </summary>
    public virtual ColumnStatistics GetStatistics()
    {
        return new ColumnStatistics
        {
            NumberOfValues = (ulong)NonNullCount,
            HasNull = HasNulls,
        };
    }

    /// <summary>
    /// Collects the ORC streams produced by this writer.
    /// Caller should call this after all batches for a stripe are written.
    /// </summary>
    public virtual void GetStreams(List<OrcStream> streams)
    {
        if (StripeHasNulls)
        {
            PresentEncoder.Flush();
            streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.Present, PresentStream));
        }
    }

    /// <summary>
    /// Returns an estimate of the bytes currently buffered in this writer's streams.
    /// Used for stripe size enforcement.
    /// </summary>
    public virtual long EstimateBufferedBytes() => PresentStream.Length;

    /// <summary>
    /// Flushes internal encoder buffers without resetting streams.
    /// Called at row group boundaries to ensure positions are accurate.
    /// </summary>
    public virtual void FlushEncoders()
    {
        if (StripeHasNulls)
            PresentEncoder.Flush();
    }

    /// <summary>
    /// Appends the current stream positions to the list.
    /// Order must match the streams returned by GetStreams().
    /// Uses StripeHasNulls to match GetStreams() behavior.
    /// </summary>
    public virtual void GetStreamPositions(IList<ulong> positions)
    {
        if (StripeHasNulls)
        {
            positions.Add((ulong)PresentStream.Length);
            positions.Add(0); // byte RLE remaining
            positions.Add(0); // bit offset
        }
    }

    /// <summary>
    /// Returns the position layout for compression translation.
    /// Each entry is the number of encoding-specific extra positions after the byte offset
    /// for each stream (in the order returned by GetStreamPositions/GetStreams).
    /// </summary>
    public virtual void GetPositionLayout(IList<int> extrasPerStream)
    {
        if (StripeHasNulls)
            extrasPerStream.Add(2); // PRESENT (boolean): rle_remaining + bit_offset
    }

    /// <summary>
    /// Serializes the current bloom filter state for the completed row group and resets the builder.
    /// </summary>
    internal void FinishBloomFilterRowGroup()
    {
        if (BloomFilter == null) return;
        _bloomFilterEntries ??= new List<Proto.BloomFilter>();
        _bloomFilterEntries.Add(BloomFilter.ToProto());
        BloomFilter.Reset();
    }

    /// <summary>
    /// Assembles the collected per-row-group bloom filters into a BloomFilterIndex.
    /// Returns null if no bloom filters were collected.
    /// </summary>
    internal Proto.BloomFilterIndex? GetBloomFilterIndex()
    {
        if (_bloomFilterEntries == null || _bloomFilterEntries.Count == 0)
            return null;
        var index = new Proto.BloomFilterIndex();
        index.BloomFilter.AddRange(_bloomFilterEntries);
        return index;
    }

    /// <summary>
    /// Resets only the statistics tracking, not the stream data.
    /// Called at row group boundaries so GetStatistics() returns per-row-group stats.
    /// </summary>
    public virtual void ResetStatistics()
    {
        RowCount = 0;
        NonNullCount = 0;
        HasNulls = false;
    }

    /// <summary>
    /// Resets the writer for a new stripe.
    /// </summary>
    public virtual void Reset()
    {
        PresentStream.Reset();
        HasNulls = false;
        StripeHasNulls = false;
        RowCount = 0;
        NonNullCount = 0;
        _bloomFilterEntries?.Clear();
        BloomFilter?.Reset();
    }

    /// <summary>
    /// Creates a column writer for the given Arrow field and ORC type.
    /// </summary>
    public static (List<ColumnWriter> writers, int nextColumnId) Create(
        Field field, int columnId, OrcWriterOptions options)
    {
        var writers = new List<ColumnWriter>();
        CreateRecursive(field.DataType, field.Name, columnId, options, writers);
        return (writers, columnId + writers.Count);
    }

    private static int CreateRecursive(
        IArrowType type, string? name, int columnId, OrcWriterOptions options, List<ColumnWriter> writers)
    {
        var encoding = EncodingFamily.V2;
        if (name != null && options.ColumnEncodings?.TryGetValue(name, out var colEnc) == true)
            encoding = colEnc;

        switch (type)
        {
            case BooleanType:
                writers.Add(new BooleanColumnWriter(columnId));
                return columnId + 1;
            case Int8Type:
                writers.Add(new ByteColumnWriter(columnId));
                return columnId + 1;
            case Int16Type or Int32Type or Int64Type:
                writers.Add(new IntegerColumnWriter(columnId, type));
                return columnId + 1;
            case FloatType:
                writers.Add(new FloatColumnWriter(columnId));
                return columnId + 1;
            case DoubleType:
                writers.Add(new DoubleColumnWriter(columnId));
                return columnId + 1;
            case StringType:
            {
                var strEnc = name != null && options.ColumnEncodings?.TryGetValue(name, out var se) == true
                    ? se : options.DefaultStringEncoding;
                writers.Add(new StringColumnWriter(columnId, strEnc, options.DictionaryKeySizeThreshold));
                return columnId + 1;
            }
            case Date32Type:
                writers.Add(new DateColumnWriter(columnId));
                return columnId + 1;
            case TimestampType:
                writers.Add(new TimestampColumnWriter(columnId));
                return columnId + 1;
            case BinaryType:
                writers.Add(new BinaryColumnWriter(columnId));
                return columnId + 1;
            case Decimal128Type dt:
                writers.Add(new DecimalColumnWriter(columnId, dt.Precision, dt.Scale));
                return columnId + 1;
            case StructType st:
            {
                var structWriter = new StructColumnWriter(columnId);
                writers.Add(structWriter);
                int nextStructId = columnId + 1;
                foreach (var childField in st.Fields)
                {
                    int childIndex = writers.Count;
                    nextStructId = CreateRecursive(childField.DataType, childField.Name, nextStructId, options, writers);
                    structWriter.AddChild(writers[childIndex]);
                }
                return nextStructId;
            }
            case ListType lt:
            {
                var listWriter = new ListColumnWriter(columnId);
                writers.Add(listWriter);
                int elemIndex = writers.Count;
                int nextListId = CreateRecursive(lt.ValueDataType, null, columnId + 1, options, writers);
                listWriter.SetElementWriter(writers[elemIndex]);
                return nextListId;
            }
            case MapType mt:
            {
                var mapWriter = new MapColumnWriter(columnId);
                writers.Add(mapWriter);
                int keyIndex = writers.Count;
                int nextMapId = CreateRecursive(mt.KeyField.DataType, null, columnId + 1, options, writers);
                mapWriter.SetKeyWriter(writers[keyIndex]);
                int valIndex = writers.Count;
                nextMapId = CreateRecursive(mt.ValueField.DataType, null, nextMapId, options, writers);
                mapWriter.SetValueWriter(writers[valIndex]);
                return nextMapId;
            }
            case UnionType ut:
            {
                var unionWriter = new UnionColumnWriter(columnId);
                writers.Add(unionWriter);
                int nextUnionId = columnId + 1;
                foreach (var childField in ut.Fields)
                {
                    int childIndex = writers.Count;
                    nextUnionId = CreateRecursive(childField.DataType, null, nextUnionId, options, writers);
                    unionWriter.AddChild(writers[childIndex]);
                }
                return nextUnionId;
            }
            default:
                throw new NotSupportedException($"Arrow type {type} is not yet supported for writing.");
        }
    }
}

/// <summary>
/// Represents a finalized ORC data stream ready to be written to the file.
/// </summary>
internal sealed class OrcStream
{
    public int ColumnId { get; }
    public ProtoStream.Types.Kind Kind { get; }
    public GrowableBuffer Data { get; }

    public OrcStream(int columnId, ProtoStream.Types.Kind kind, GrowableBuffer data)
    {
        ColumnId = columnId;
        Kind = kind;
        Data = data;
    }
}

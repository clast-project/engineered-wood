// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.Avro.Encoding;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro.Data;

/// <summary>
/// Decodes Avro binary-encoded records into Arrow RecordBatches.
/// </summary>
internal sealed class RecordBatchAssembler
{
    private const int MaxSchemaDepth = 64;
    private readonly AvroRecordSchema _writerSchema;
    private readonly Apache.Arrow.Schema _arrowSchema;
    private readonly SchemaResolution? _resolution;
    private readonly IColumnBuilder[] _builders;

    public RecordBatchAssembler(AvroRecordSchema schema, Apache.Arrow.Schema arrowSchema)
    {
        _writerSchema = schema;
        _arrowSchema = arrowSchema;
        _builders = CreateBuilders(schema, arrowSchema);
    }

    public RecordBatchAssembler(AvroRecordSchema writerSchema, SchemaResolution resolution)
    {
        _writerSchema = writerSchema;
        _arrowSchema = resolution.ArrowSchema;
        _resolution = resolution;
        _builders = CreateResolvedBuilders(writerSchema, resolution);
    }

    /// <summary>
    /// Decodes up to <paramref name="maxRows"/> records from the data span.
    /// Returns the RecordBatch and the number of bytes consumed.
    /// </summary>
    public (RecordBatch batch, int bytesConsumed) Decode(ReadOnlySpan<byte> data, int maxRows)
    {
        var reader = new AvroBinaryReader(data);
        var effectiveSchema = _resolution?.ArrowSchema ?? _arrowSchema;
        ResetBuilders();
        ReserveBuilders(maxRows);

        int rowCount = 0;
        for (int i = 0; i < maxRows && !reader.IsEmpty; i++)
        {
            DecodeRecordDispatch(ref reader, _builders);
            rowCount++;
        }

        var arrays = BuildArrays(_builders, effectiveSchema);
        var batch = new RecordBatch(effectiveSchema, arrays, rowCount);
        return (batch, reader.Position);
    }

    /// <summary>
    /// Decodes all records from a block (known object count).
    /// </summary>
    public RecordBatch DecodeBlock(ReadOnlySpan<byte> data, int objectCount)
    {
        var reader = new AvroBinaryReader(data);
        var effectiveSchema = _resolution?.ArrowSchema ?? _arrowSchema;
        ResetBuilders();
        ReserveBuilders(objectCount);

        for (int i = 0; i < objectCount; i++)
            DecodeRecordDispatch(ref reader, _builders);

        var arrays = BuildArrays(_builders, effectiveSchema);
        return new RecordBatch(effectiveSchema, arrays, objectCount);
    }

    private void ResetBuilders()
    {
        for (int i = 0; i < _builders.Length; i++)
            _builders[i].Reset();
    }

    /// <summary>
    /// Preallocates exactly-sized buffers for the known row count of the next batch,
    /// so scalar columns fill without growth reallocations. Container builders treat
    /// the count as a hint for their own validity/offsets.
    /// </summary>
    private void ReserveBuilders(int rowCount)
    {
        for (int i = 0; i < _builders.Length; i++)
            _builders[i].Reserve(rowCount);
    }

    private void DecodeRecordDispatch(ref AvroBinaryReader reader, IColumnBuilder[] builders)
    {
        if (_resolution != null)
            DecodeRecordWithResolution(ref reader, _writerSchema, _resolution, builders);
        else
            DecodeRecord(ref reader, _writerSchema, builders);
    }

    private static void DecodeRecord(ref AvroBinaryReader reader, AvroRecordSchema schema, IColumnBuilder[] builders)
    {
        for (int i = 0; i < schema.Fields.Count; i++)
            builders[i].Append(ref reader);
    }

    /// <summary>
    /// Decodes a record using schema resolution: iterates writer fields, skips unmatched,
    /// dispatches to reader builders, and fills defaults for missing reader fields.
    /// </summary>
    private static void DecodeRecordWithResolution(
        ref AvroBinaryReader reader,
        AvroRecordSchema writerSchema,
        SchemaResolution resolution,
        IColumnBuilder[] builders)
    {
        // builders are indexed by reader field index.
        for (int wi = 0; wi < writerSchema.Fields.Count; wi++)
        {
            var action = resolution.WriterActions[wi];
            if (action.Skip)
            {
                reader.Skip(writerSchema.Fields[wi].Schema);
            }
            else
            {
                builders[action.ReaderFieldIndex].Append(ref reader);
            }
        }

        // Fill defaults for reader fields not in writer
        foreach (var df in resolution.DefaultFields)
        {
            DefaultValueApplicator.AppendDefault(builders[df.ReaderIndex], df.DefaultValue, df.Schema);
        }
    }

    /// <summary>
    /// Creates builders for resolved schema: one builder per reader field,
    /// using promoting builders where type promotion is needed.
    /// </summary>
    private static IColumnBuilder[] CreateResolvedBuilders(
        AvroRecordSchema writerSchema, SchemaResolution resolution)
    {
        var readerSchema = resolution.ReaderSchema;
        var arrowSchema = resolution.ArrowSchema;
        var builders = new IColumnBuilder[readerSchema.Fields.Count];

        for (int ri = 0; ri < readerSchema.Fields.Count; ri++)
        {
            var field = arrowSchema.FieldsList[ri];
            var readerFieldSchema = readerSchema.Fields[ri].Schema;
            var promotion = resolution.Promotions[ri];

            if (promotion != null)
            {
                builders[ri] = CreatePromotingBuilder(field, readerFieldSchema, promotion, 0);
            }
            else
            {
                // Find the writer field that maps to this reader field, if any
                AvroSchemaNode avroSchemaForBuilder = readerFieldSchema;
                for (int wi = 0; wi < writerSchema.Fields.Count; wi++)
                {
                    if (!resolution.WriterActions[wi].Skip &&
                        resolution.WriterActions[wi].ReaderFieldIndex == ri)
                    {
                        avroSchemaForBuilder = writerSchema.Fields[wi].Schema;
                        break;
                    }
                }
                builders[ri] = CreateBuilder(field, avroSchemaForBuilder, 0);
            }
        }

        return builders;
    }

    /// <summary>
    /// Creates a builder that reads using writer encoding but stores in reader type.
    /// </summary>
    private static IColumnBuilder CreatePromotingBuilder(
        Field field, AvroSchemaNode readerSchema, TypePromotion promotion, int depth)
    {
        if (depth >= MaxSchemaDepth)
            throw new NotSupportedException(
                "Avro schema exceeds maximum nesting depth. Recursive schemas cannot be represented in Arrow.");

        // Handle nullable wrapper
        bool isNullable = readerSchema is AvroUnionSchema union && union.IsNullable(out _, out _);
        int nullIndex = 0;
        if (readerSchema is AvroUnionSchema u && u.IsNullable(out _, out int ni))
            nullIndex = ni;

        IColumnBuilder inner = promotion.Kind switch
        {
            PromotionKind.IntToLong => new PromotingIntToLongBuilder(),
            PromotionKind.IntToFloat => new PromotingIntToFloatBuilder(),
            PromotionKind.IntToDouble => new PromotingIntToDoubleBuilder(),
            PromotionKind.LongToFloat => new PromotingLongToFloatBuilder(),
            PromotionKind.LongToDouble => new PromotingLongToDoubleBuilder(),
            PromotionKind.FloatToDouble => new PromotingFloatToDoubleBuilder(),
            PromotionKind.StringToBytes => new PromotingStringToBytesBuilder(),
            PromotionKind.BytesToString => new PromotingBytesToStringBuilder(),
            PromotionKind.NestedRecord => CreateResolvingStructBuilder(field, promotion, depth + 1),
            PromotionKind.EnumRemap => new RemappingEnumBuilder(
                (AvroEnumSchema)promotion.WriterSchema!, (AvroEnumSchema)promotion.ReaderSchema!),
            _ => CreateBuilder(field, readerSchema, depth),
        };

        if (isNullable)
            return new NullableBuilder(inner, nullIndex);
        return inner;
    }

    /// <summary>
    /// Creates a <see cref="ResolvingStructBuilder"/> that reads wire data in writer field order
    /// but produces a StructArray in reader field order, handling field additions, removals,
    /// and nested type promotions.
    /// </summary>
    private static IColumnBuilder CreateResolvingStructBuilder(Field field, TypePromotion promotion, int depth)
    {
        var writerRecord = (AvroRecordSchema)promotion.WriterSchema!;
        var readerRecord = (AvroRecordSchema)promotion.ReaderSchema!;

        // Resolve the nested schemas to get field mappings
        var nestedResolution = SchemaResolver.Resolve(writerRecord, readerRecord);

        // Create child builders for each reader field (in reader order)
        var readerBuilders = new IColumnBuilder[readerRecord.Fields.Count];
        for (int ri = 0; ri < readerRecord.Fields.Count; ri++)
        {
            var readerFieldSchema = readerRecord.Fields[ri].Schema;
            var (arrowType, nullable) = ArrowSchemaConverter.ToArrowType(readerFieldSchema, depth);
            var childField = new Field(readerRecord.Fields[ri].Name, arrowType, nullable);
            var nestedPromotion = nestedResolution.Promotions[ri];

            if (nestedPromotion != null)
            {
                readerBuilders[ri] = CreatePromotingBuilder(childField, readerFieldSchema, nestedPromotion, depth);
            }
            else
            {
                // Find the writer field that maps to this reader field, if any
                AvroSchemaNode avroSchemaForBuilder = readerFieldSchema;
                for (int wi = 0; wi < writerRecord.Fields.Count; wi++)
                {
                    if (!nestedResolution.WriterActions[wi].Skip &&
                        nestedResolution.WriterActions[wi].ReaderFieldIndex == ri)
                    {
                        avroSchemaForBuilder = writerRecord.Fields[wi].Schema;
                        break;
                    }
                }
                readerBuilders[ri] = CreateBuilder(childField, avroSchemaForBuilder, depth);
            }
        }

        return new ResolvingStructBuilder(
            writerRecord, readerRecord, nestedResolution, readerBuilders);
    }

    private static IColumnBuilder[] CreateBuilders(AvroRecordSchema avroSchema, Apache.Arrow.Schema arrowSchema)
    {
        var builders = new IColumnBuilder[arrowSchema.FieldsList.Count];
        for (int i = 0; i < builders.Length; i++)
        {
            var field = arrowSchema.FieldsList[i];
            var avroFieldSchema = avroSchema.Fields[i].Schema;
            builders[i] = CreateBuilder(field, avroFieldSchema, 0);
        }
        return builders;
    }

    private static IColumnBuilder CreateBuilder(Field field, AvroSchemaNode avroSchema, int depth)
    {
        if (depth >= MaxSchemaDepth)
            throw new NotSupportedException(
                "Avro schema exceeds maximum nesting depth. Recursive schemas cannot be represented in Arrow.");

        // Handle nullable unions
        if (avroSchema is AvroUnionSchema union && union.IsNullable(out var inner, out int nullIndex))
            return new NullableBuilder(CreateBuilderForType(field.DataType, inner, depth + 1), nullIndex);

        // Handle general unions → DenseUnion
        if (avroSchema is AvroUnionSchema generalUnion)
        {
            var unionType = (UnionType)field.DataType;
            var branchBuilders = new IColumnBuilder[generalUnion.Branches.Count];
            for (int i = 0; i < generalUnion.Branches.Count; i++)
            {
                var branchField = unionType.Fields[i];
                branchBuilders[i] = CreateBuilderForType(branchField.DataType, generalUnion.Branches[i], depth + 1);
            }
            return new DenseUnionBuilder(generalUnion, branchBuilders);
        }

        return CreateBuilderForType(field.DataType, avroSchema, depth);
    }

    private static IColumnBuilder CreateBuilderForType(IArrowType type, AvroSchemaNode avroSchema, int depth)
    {
        // Check for logical types on primitives
        if (avroSchema is AvroPrimitiveSchema { LogicalType: not null } p)
        {
            return p.LogicalType switch
            {
                "date" => new Date32Builder(),
                "time-millis" => new Time32MillisBuilder(),
                "time-micros" => new Time64MicrosBuilder(),
                "time-nanos" => new Time64NanosBuilder(),
                "timestamp-millis" or "local-timestamp-millis" => new TimestampBuilder(),
                "timestamp-micros" or "local-timestamp-micros" => new TimestampBuilder(),
                "timestamp-nanos" or "local-timestamp-nanos" => new TimestampBuilder(),
                "decimal" => new DecimalBytesBuilder(p.Precision ?? 38, p.Scale ?? 0),
                // The schema converter chose GuidType (extension) when the
                // caller registered arrow.uuid; otherwise StringType. Mirror
                // that choice here so the wire format stays a 36-char string
                // either way.
                "uuid" when type is ExtensionType => new GuidBuilder(),
                "uuid" => new StringBuilder(),
                _ => CreateBuilderForBaseType(type, avroSchema, depth),
            };
        }

        return CreateBuilderForBaseType(type, avroSchema, depth);
    }

    private static IColumnBuilder CreateBuilderForBaseType(IArrowType type, AvroSchemaNode avroSchema, int depth)
    {
        switch (avroSchema)
        {
            case AvroPrimitiveSchema { Type: AvroType.Null }:
                return new NullBuilder();
            case AvroPrimitiveSchema { Type: AvroType.Boolean }:
                return new BooleanBuilder();
            case AvroPrimitiveSchema { Type: AvroType.Int }:
                return new Int32Builder();
            case AvroPrimitiveSchema { Type: AvroType.Long }:
                return new Int64Builder();
            case AvroPrimitiveSchema { Type: AvroType.Float }:
                return new FloatBuilder();
            case AvroPrimitiveSchema { Type: AvroType.Double }:
                return new DoubleBuilder();
            case AvroPrimitiveSchema { Type: AvroType.Bytes }:
                return new BinaryBuilder();
            case AvroPrimitiveSchema { Type: AvroType.String }:
                return new StringBuilder();

            case AvroEnumSchema e:
                return new EnumBuilder(e.Symbols);

            case AvroFixedSchema f when f.LogicalType == "decimal":
                return new DecimalFixedBuilder(f.Size, f.Precision ?? 38, f.Scale ?? 0);
            case AvroFixedSchema f when f.LogicalType == "duration" && f.Size == 12:
                return new DurationBuilder();
            case AvroFixedSchema f:
                return new FixedBuilder(f.Size);

            case AvroArraySchema a:
            {
                var (itemArrowType, itemNullable) = ArrowSchemaConverter.ToArrowType(a.Items, depth + 1);
                var itemBuilder = CreateBuilder(
                    new Field("item", itemArrowType, itemNullable), a.Items, depth + 1);
                return new ArrayBuilder(itemBuilder);
            }

            case AvroMapSchema m:
            {
                var (valArrowType, valNullable) = ArrowSchemaConverter.ToArrowType(m.Values, depth + 1);
                var valBuilder = CreateBuilder(
                    new Field("value", valArrowType, valNullable), m.Values, depth + 1);
                return new MapBuilder(valBuilder);
            }

            case AvroRecordSchema r:
            {
                var childBuilders = new List<(string name, IColumnBuilder builder)>();
                foreach (var f in r.Fields)
                {
                    var (ft, fn) = ArrowSchemaConverter.ToArrowType(f.Schema, depth + 1);
                    childBuilders.Add((f.Name, CreateBuilder(new Field(f.Name, ft, fn), f.Schema, depth + 1)));
                }
                return new StructBuilder(childBuilders);
            }

            default:
                throw new NotSupportedException(
                    $"Avro schema type {avroSchema.Type} not yet supported in decoder.");
        }
    }

    private static IArrowArray[] BuildArrays(IColumnBuilder[] builders, Apache.Arrow.Schema schema)
    {
        var arrays = new IArrowArray[builders.Length];
        for (int i = 0; i < builders.Length; i++)
            arrays[i] = builders[i].Build(schema.FieldsList[i]);
        return arrays;
    }
}

/// <summary>Column builder interface for accumulating decoded Avro values.</summary>
internal interface IColumnBuilder
{
    void Append(ref AvroBinaryReader reader);
    void AppendNull();
    IArrowArray Build(Field field);
    /// <summary>Resets accumulated state so the builder can be reused for the next batch.</summary>
    void Reset();
    /// <summary>
    /// Hints the row count of the next batch so buffers can be preallocated. For fixed-width
    /// scalar columns this is exact (one value per row); container builders treat it as a hint.
    /// Callers must invoke <see cref="Reset"/> first. Default implementations may ignore it.
    /// </summary>
    void Reserve(int rowCount);
}

/// <summary>Wraps a non-nullable builder to handle ["null", T] unions.</summary>
internal sealed class NullableBuilder : IColumnBuilder
{
    private readonly IColumnBuilder _inner;
    private readonly int _nullIndex;

    public NullableBuilder(IColumnBuilder inner, int nullIndex = 0)
    {
        _inner = inner;
        _nullIndex = nullIndex;
    }

    public void Append(ref AvroBinaryReader reader)
    {
        int branchIndex = reader.ReadUnionIndex();
        if (branchIndex == _nullIndex)
        {
            _inner.AppendNull();
        }
        else
        {
            _inner.Append(ref reader);
        }
    }

    public void AppendNull() => _inner.AppendNull();
    public IArrowArray Build(Field field) => _inner.Build(field);
    public void Reset() => _inner.Reset();
    public void Reserve(int rowCount) => _inner.Reserve(rowCount);
}

// ─── Validity bitmap accumulator ───

/// <summary>
/// Accumulates an Arrow validity bitmap in native memory, one bit per value. The bitmap is
/// allocated lazily on the first null (back-filling the prior all-valid entries), so a
/// non-nullable column — the common case — never allocates or touches a validity buffer.
/// Ownership of the bitmap transfers to a zero-copy <see cref="ArrowBuffer"/> at
/// <see cref="BuildBitmap"/> (called only when <see cref="NullCount"/> &gt; 0); the tracker
/// then holds no buffer until the next <see cref="Reserve"/>, so a built batch's bitmap is
/// never mutated by a later batch. Used as a mutable in-place field — never copy it by value.
/// </summary>
internal struct ValidityTracker
{
    private NativeBuffer<byte>? _bitmap;   // null until the first null is appended
    private int _capacityBytes;
    private int _reserveBits;              // capacity hint from Reserve
    private int _count;
    private int _nullCount;

    public ValidityTracker() { }

    public int Count => _count;
    public int NullCount => _nullCount;

    /// <summary>Records the expected row count; defers bitmap allocation until a null appears.</summary>
    public void Reserve(int rowCount)
    {
        _bitmap?.Dispose();
        _bitmap = null;
        _capacityBytes = 0;
        _reserveBits = rowCount;
        _count = 0;
        _nullCount = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AppendValid()
    {
        if (_bitmap != null)
            SetValidBit(_count);
        _count++;
    }

    public void AppendNull()
    {
        if (_bitmap == null)
            AllocateAndBackfill();
        else
            EnsureByte(_count >> 3);
        // bit stays 0
        _count++;
        _nullCount++;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SetValidBit(int index)
    {
        int byteIndex = index >> 3;
        if (byteIndex >= _capacityBytes)
            EnsureByte(byteIndex);
        _bitmap!.ByteSpan[byteIndex] |= (byte)(1 << (index & 7));
    }

    /// <summary>First null: allocate the bitmap and mark all prior <see cref="_count"/> entries valid.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void AllocateAndBackfill()
    {
        _capacityBytes = Math.Max(1, (Math.Max(_reserveBits, _count + 1) + 7) / 8);
        _bitmap = new NativeBuffer<byte>(_capacityBytes, zeroFill: true);
        var span = _bitmap.ByteSpan;
        int fullBytes = _count >> 3;
        for (int i = 0; i < fullBytes; i++)
            span[i] = 0xFF;
        int rem = _count & 7;
        if (rem != 0)
            span[fullBytes] = (byte)((1 << rem) - 1);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void EnsureByte(int byteIndex)
    {
        if (byteIndex < _capacityBytes)
            return;
        int oldBytes = _capacityBytes;
        _bitmap!.Grow(byteIndex + 1);
        // NativeBuffer.Grow does not zero new bytes; validity needs zeros for unset (null) bits.
        _bitmap.ByteSpan.Slice(oldBytes).Clear();
        _capacityBytes = _bitmap.Length;
    }

    /// <summary>Transfers bitmap ownership to an <see cref="ArrowBuffer"/>. Only valid when NullCount &gt; 0.</summary>
    public ArrowBuffer BuildBitmap()
    {
        var buf = _bitmap!;
        _bitmap = null;
        _capacityBytes = 0;
        return buf.Build();
    }

    /// <summary>Discards any un-built bitmap so the next batch starts fresh.</summary>
    public void Reset()
    {
        _bitmap?.Dispose();
        _bitmap = null;
        _capacityBytes = 0;
        _reserveBits = 0;
        _count = 0;
        _nullCount = 0;
    }
}

/// <summary>
/// Accumulates Arrow list/map offsets (int32, count+1 running byte/element offsets) in native
/// memory, transferring ownership to a zero-copy <see cref="ArrowBuffer"/> at <see cref="Build"/>.
/// Used as a mutable in-place field — never copy it by value.
/// </summary>
internal struct OffsetsAccumulator
{
    private NativeBuffer<int>? _offsets;
    private int _cap;        // element capacity of _offsets
    private int _count;      // number of appended elements (offsets holds _count+1 entries)
    private int _running;    // current running total

    public OffsetsAccumulator() { }

    public int Count => _count;

    public void Reserve(int rowCount)
    {
        _offsets?.Dispose();
        _cap = Math.Max(1, rowCount) + 1;
        _offsets = new NativeBuffer<int>(_cap, zeroFill: false);
        _offsets.Span[0] = 0;
        _count = 0;
        _running = 0;
    }

    /// <summary>Appends one parent element whose child span advanced by <paramref name="length"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(int length)
    {
        EnsureSlot();
        _running += length;
        _count++;
        _offsets!.Span[_count] = _running;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureSlot()
    {
        if (_offsets != null && _count + 1 < _cap)
            return;
        Grow();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Grow()
    {
        if (_offsets == null)
        {
            _cap = 64;
            _offsets = new NativeBuffer<int>(_cap, zeroFill: false);
            _offsets.Span[0] = 0;
            return;
        }
        _offsets.Grow(_count + 2);
        _cap = _offsets.Length;
    }

    public ArrowBuffer Build()
    {
        var buf = _offsets!;
        _offsets = null;
        _cap = 0;
        return buf.Build();
    }

    public void Reset()
    {
        _offsets?.Dispose();
        _offsets = null;
        _cap = 0;
        _count = 0;
        _running = 0;
    }
}

/// <summary>
/// Base for fixed-width scalar columns. Accumulates values into a native buffer sized to the
/// batch row count (via <see cref="Reserve"/>), then transfers that buffer to Arrow zero-copy
/// at <see cref="Build"/>. Ownership moves to the built <see cref="RecordBatch"/> — the builder
/// holds no buffer afterwards — so a built batch is safe to retain while later batches decode.
/// When there are no nulls the validity buffer is elided.
/// </summary>
internal abstract class FixedWidthBuilder<T> : IColumnBuilder
    where T : unmanaged
{
    private NativeBuffer<T>? _values;
    private int _capacity;
    private int _count;
    private ValidityTracker _validity = new();

    protected abstract T ReadValue(ref AvroBinaryReader reader);

    public void Append(ref AvroBinaryReader reader)
    {
        T value = ReadValue(ref reader);
        if (_count >= _capacity)
            Grow();
        _values!.Span[_count] = value;
        _count++;
        _validity.AppendValid();
    }

    public void AppendNull()
    {
        if (_count >= _capacity)
            Grow();
        _values!.Span[_count] = default;
        _count++;
        _validity.AppendNull();
    }

    /// <summary>Appends a known, non-null value (schema-resolution default path).</summary>
    protected void AppendValueDirect(T value)
    {
        if (_count >= _capacity)
            Grow();
        _values!.Span[_count] = value;
        _count++;
        _validity.AppendValid();
    }

    public void Reserve(int rowCount)
    {
        _values?.Dispose();
        _capacity = Math.Max(1, rowCount);
        // zeroFill:false — every slot up to _count is written (values or default for nulls).
        _values = new NativeBuffer<T>(_capacity, zeroFill: false);
        _validity.Reserve(rowCount);
        _count = 0;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Grow()
    {
        if (_values == null)
        {
            _capacity = 64;
            _values = new NativeBuffer<T>(_capacity, zeroFill: false);
            return;
        }
        _values.Grow(_count + 1);
        _capacity = _values.Length;
    }

    public IArrowArray Build(Field field)
    {
        var validity = _validity.NullCount > 0 ? _validity.BuildBitmap() : ArrowBuffer.Empty;
        int count = _count;
        int nullCount = _validity.NullCount;
        var values = _values!.Build();
        _values = null;
        _capacity = 0;
        var data = new ArrayData(field.DataType, count, nullCount, 0, [validity, values]);
        return ArrowArrayFactory.BuildArray(data);
    }

    public void Reset()
    {
        _values?.Dispose();
        _values = null;
        _capacity = 0;
        _count = 0;
        _validity.Reset();
    }
}

// ─── Primitive builders ───

internal sealed class NullBuilder : IColumnBuilder
{
    private int _count;
    public void Append(ref AvroBinaryReader reader) => _count++;
    public void AppendNull() => _count++;
    public IArrowArray Build(Field field) => new NullArray(_count);
    public void Reset() => _count = 0;
    public void Reserve(int rowCount) { }
}

/// <summary>
/// Arrow <see cref="BooleanArray"/> stores values bit-packed. Values are accumulated into a
/// native value bitmap (one bit per row, set when true) alongside a validity bitmap — the same
/// native-bit-packing pattern the ORC and Parquet readers use — so the column never touches the
/// managed heap. Both buffers transfer ownership to Arrow zero-copy at <see cref="Build"/>.
/// </summary>
internal sealed class BooleanBuilder : IColumnBuilder
{
    private NativeBuffer<byte>? _values;   // 1 bit per value; bit set when the value is true
    private int _capacityBytes;
    private int _count;
    private ValidityTracker _validity = new();

    public void Append(ref AvroBinaryReader reader) => AppendBool(reader.ReadBoolean());
    public void AppendDefault(bool value) => AppendBool(value);

    public void AppendNull()
    {
        int byteIndex = _count >> 3;
        if (byteIndex >= _capacityBytes)
            Grow(byteIndex);
        // value bit stays 0 (buffer is zero-filled)
        _count++;
        _validity.AppendNull();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AppendBool(bool value)
    {
        int byteIndex = _count >> 3;
        if (byteIndex >= _capacityBytes)
            Grow(byteIndex);
        if (value)
            _values!.ByteSpan[byteIndex] |= (byte)(1 << (_count & 7));
        _count++;
        _validity.AppendValid();
    }

    public void Reserve(int rowCount)
    {
        _values?.Dispose();
        _capacityBytes = Math.Max(1, (rowCount + 7) / 8);
        _values = new NativeBuffer<byte>(_capacityBytes, zeroFill: true);
        _count = 0;
        _validity.Reserve(rowCount);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Grow(int byteIndex)
    {
        if (_values == null)
        {
            _capacityBytes = Math.Max(8, byteIndex + 1);
            _values = new NativeBuffer<byte>(_capacityBytes, zeroFill: true);
            return;
        }
        int oldBytes = _capacityBytes;
        _values.Grow(byteIndex + 1);
        // NativeBuffer.Grow does not zero new bytes; false/unset value bits need zeros.
        _values.ByteSpan.Slice(oldBytes).Clear();
        _capacityBytes = _values.Length;
    }

    public IArrowArray Build(Field field)
    {
        var validity = _validity.NullCount > 0 ? _validity.BuildBitmap() : ArrowBuffer.Empty;
        int count = _count;
        int nullCount = _validity.NullCount;
        var values = _values!.Build();
        _values = null;
        _capacityBytes = 0;
        var data = new ArrayData(field.DataType, count, nullCount, 0, [validity, values]);
        return ArrowArrayFactory.BuildArray(data);
    }

    public void Reset()
    {
        _values?.Dispose();
        _values = null;
        _capacityBytes = 0;
        _count = 0;
        _validity.Reset();
    }
}

internal sealed class Int32Builder : FixedWidthBuilder<int>
{
    protected override int ReadValue(ref AvroBinaryReader reader) => reader.ReadInt();
    public void AppendDefault(int value) => AppendValueDirect(value);
}

internal sealed class Int64Builder : FixedWidthBuilder<long>
{
    protected override long ReadValue(ref AvroBinaryReader reader) => reader.ReadLong();
    public void AppendDefault(long value) => AppendValueDirect(value);
}

internal sealed class FloatBuilder : FixedWidthBuilder<float>
{
    protected override float ReadValue(ref AvroBinaryReader reader) => reader.ReadFloat();
    public void AppendDefault(float value) => AppendValueDirect(value);
}

internal sealed class DoubleBuilder : FixedWidthBuilder<double>
{
    protected override double ReadValue(ref AvroBinaryReader reader) => reader.ReadDouble();
    public void AppendDefault(double value) => AppendValueDirect(value);
}

/// <summary>
/// Base for variable-length columns (Arrow string/binary layout: validity, int32 offsets,
/// data bytes). Raw element bytes are copied straight from the wire into a native data buffer
/// — no intermediate <c>string</c> or <c>byte[]</c> per value — and both offset and data
/// buffers transfer ownership to Arrow zero-copy at <see cref="Build"/>. Validity is elided
/// when there are no nulls; the built batch is safe to retain while later batches decode.
/// </summary>
internal abstract class VarLengthBuilder : IColumnBuilder
{
    private NativeBuffer<int>? _offsets;   // count+1 running byte offsets, offsets[0] = 0
    private int _offsetCap;                // element capacity of _offsets (i.e. offsets.Length)
    private NativeBuffer<byte>? _data;
    private int _dataCap;
    private int _dataLen;
    private int _count;
    private ValidityTracker _validity = new();

    /// <summary>Returns the raw element bytes from the wire (valid until the next read).</summary>
    protected abstract ReadOnlySpan<byte> ReadRaw(ref AvroBinaryReader reader);

    public void Append(ref AvroBinaryReader reader) => AppendRawValid(ReadRaw(ref reader));

    public void AppendNull()
    {
        EnsureOffsetSlot();
        _count++;
        _offsets!.Span[_count] = _dataLen;   // repeat last offset -> empty slot
        _validity.AppendNull();
    }

    /// <summary>Appends one non-null element from raw bytes (also the schema-default path).</summary>
    protected void AppendRawValid(ReadOnlySpan<byte> bytes)
    {
        EnsureOffsetSlot();
        if (bytes.Length > 0)
        {
            EnsureDataCapacity(bytes.Length);
            bytes.CopyTo(_data!.ByteSpan.Slice(_dataLen));
            _dataLen += bytes.Length;
        }
        _count++;
        _offsets!.Span[_count] = _dataLen;
        _validity.AppendValid();
    }

    public void Reserve(int rowCount)
    {
        _offsets?.Dispose();
        _data?.Dispose();
        _offsetCap = Math.Max(1, rowCount) + 1;
        _offsets = new NativeBuffer<int>(_offsetCap, zeroFill: false);
        _offsets.Span[0] = 0;
        // Estimate ~8 bytes/element; grows on demand.
        _dataCap = Math.Max(256, Math.Max(1, rowCount) * 8);
        _data = new NativeBuffer<byte>(_dataCap, zeroFill: false);
        _dataLen = 0;
        _count = 0;
        _validity.Reserve(rowCount);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureOffsetSlot()
    {
        if (_offsets != null && _count + 1 < _offsetCap)
            return;
        GrowOffsets();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void GrowOffsets()
    {
        if (_offsets == null)
        {
            _offsetCap = 64;
            _offsets = new NativeBuffer<int>(_offsetCap, zeroFill: false);
            _offsets.Span[0] = 0;
            return;
        }
        _offsets.Grow(_count + 2);
        _offsetCap = _offsets.Length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureDataCapacity(int extra)
    {
        if (_data != null && _dataLen + extra <= _dataCap)
            return;
        GrowData(extra);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void GrowData(int extra)
    {
        if (_data == null)
        {
            _dataCap = Math.Max(256, extra);
            _data = new NativeBuffer<byte>(_dataCap, zeroFill: false);
            return;
        }
        _data.Grow(_dataLen + extra);
        _dataCap = _data.Length;
    }

    public IArrowArray Build(Field field)
    {
        var validity = _validity.NullCount > 0 ? _validity.BuildBitmap() : ArrowBuffer.Empty;
        int count = _count;
        int nullCount = _validity.NullCount;
        var offsets = _offsets!.Build();
        var data = _data!.Build();
        _offsets = null;
        _data = null;
        _offsetCap = 0;
        _dataCap = 0;
        var arrayData = new ArrayData(field.DataType, count, nullCount, 0, [validity, offsets, data]);
        return ArrowArrayFactory.BuildArray(arrayData);
    }

    public void Reset()
    {
        _offsets?.Dispose();
        _data?.Dispose();
        _offsets = null;
        _data = null;
        _offsetCap = 0;
        _dataCap = 0;
        _dataLen = 0;
        _count = 0;
        _validity.Reset();
    }
}

internal sealed class BinaryBuilder : VarLengthBuilder
{
    protected override ReadOnlySpan<byte> ReadRaw(ref AvroBinaryReader reader) => reader.ReadBytes();
    public void AppendDefault(byte[] value) => AppendRawValid(value);
}

internal sealed class StringBuilder : VarLengthBuilder
{
    protected override ReadOnlySpan<byte> ReadRaw(ref AvroBinaryReader reader) => reader.ReadStringBytes();
    public void AppendDefault(string value)
        => AppendRawValid(System.Text.Encoding.UTF8.GetBytes(value));
}

/// <summary>
/// Builds a <see cref="Apache.Arrow.GuidArray"/> from Avro <c>uuid</c>
/// logical-type strings (36-char canonical form per the Avro spec).
/// Selected by <see cref="RecordBatchAssembler.CreateBuilderForType"/> when
/// the target Arrow type is an <c>arrow.uuid</c> extension; falls back to
/// <see cref="StringBuilder"/> when no registry is supplied.
/// <para>
/// The 16-byte storage uses RFC 4122 big-endian order, which is exactly the byte order of
/// the canonical string's hex — so the wire bytes are parsed straight into the value slot,
/// with no intermediate <see cref="string"/> or <see cref="Guid"/> per value. The value
/// type is the <c>arrow.uuid</c> extension; <see cref="ArrowArrayFactory"/> builds the
/// <see cref="FixedSizeBinaryArray"/> storage from the same buffers and wraps it as a
/// <see cref="GuidArray"/>.
/// </para>
/// </summary>
internal sealed class GuidBuilder : ByteBlobBuilder
{
    public GuidBuilder() : base(16) { }

    protected override void WriteElement(ref AvroBinaryReader reader, Span<byte> dest)
        => UuidHex.Parse(reader.ReadStringBytes(), dest);

    protected override IArrowType ValueType(Field field) => field.DataType;

    /// <summary>Schema-default path: the default is a canonical UUID string.</summary>
    public void AppendDefault(string value)
        => GuidArray.GuidToRFC4122(Guid.Parse(value), NextValidSlot());
}

/// <summary>Parses an Avro <c>uuid</c> string into the 16-byte RFC 4122 big-endian layout.</summary>
internal static class UuidHex
{
    /// <summary>
    /// Parses UTF-8/ASCII UUID bytes into <paramref name="dest16"/>. Fast path for the canonical
    /// 8-4-4-4-12 form; falls back to lenient <see cref="Guid.Parse(string)"/> for any other
    /// accepted format (rare — valid Avro always emits the canonical form).
    /// </summary>
    public static void Parse(ReadOnlySpan<byte> ascii, Span<byte> dest16)
    {
        if (TryParseCanonical(ascii, dest16))
            return;
#if NETSTANDARD2_0
        var s = System.Text.Encoding.UTF8.GetString(ascii.ToArray());
#else
        var s = System.Text.Encoding.UTF8.GetString(ascii);
#endif
        GuidArray.GuidToRFC4122(Guid.Parse(s), dest16);
    }

    private static bool TryParseCanonical(ReadOnlySpan<byte> ascii, Span<byte> dest16)
    {
        if (ascii.Length != 36
            || ascii[8] != (byte)'-' || ascii[13] != (byte)'-'
            || ascii[18] != (byte)'-' || ascii[23] != (byte)'-')
            return false;

        int di = 0;
        for (int i = 0; i < 36;)
        {
            if (i == 8 || i == 13 || i == 18 || i == 23) { i++; continue; }
            int hi = HexVal(ascii[i]);
            int lo = HexVal(ascii[i + 1]);
            if ((hi | lo) < 0)
                return false;
            dest16[di++] = (byte)((hi << 4) | lo);
            i += 2;
        }
        return di == 16;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int HexVal(byte c)
    {
        if (c >= (byte)'0' && c <= (byte)'9') return c - '0';
        if (c >= (byte)'a' && c <= (byte)'f') return c - 'a' + 10;
        if (c >= (byte)'A' && c <= (byte)'F') return c - 'A' + 10;
        return -1;
    }
}

// ─── Logical type builders ───

// Avro's date logical type is an int count of days since the epoch — exactly the
// Date32 value-buffer layout — so we store the raw int with no DateOnly round-trip.
internal sealed class Date32Builder : FixedWidthBuilder<int>
{
    protected override int ReadValue(ref AvroBinaryReader reader) => reader.ReadInt();
    public void AppendDefault(int daysSinceEpoch) => AppendValueDirect(daysSinceEpoch);
}

internal sealed class Time32MillisBuilder : FixedWidthBuilder<int>
{
    protected override int ReadValue(ref AvroBinaryReader reader) => reader.ReadInt();
    public void AppendDefault(int value) => AppendValueDirect(value);
}

internal sealed class Time64MicrosBuilder : FixedWidthBuilder<long>
{
    protected override long ReadValue(ref AvroBinaryReader reader) => reader.ReadLong();
    public void AppendDefault(long value) => AppendValueDirect(value);
}

internal sealed class Time64NanosBuilder : FixedWidthBuilder<long>
{
    protected override long ReadValue(ref AvroBinaryReader reader) => reader.ReadLong();
    public void AppendDefault(long value) => AppendValueDirect(value);
}

/// <summary>
/// Reads Avro duration (fixed 12 bytes: months u32 LE, days u32 LE, millis u32 LE)
/// into an Arrow MonthDayNanosecondIntervalArray (months, days, nanoseconds). The Arrow value
/// buffer stores <see cref="MonthDayNanosecondInterval"/> structs directly (16 bytes each:
/// months@0, days@4, nanoseconds@8), so the fixed-width native builder writes them straight in.
/// </summary>
internal sealed class DurationBuilder : FixedWidthBuilder<MonthDayNanosecondInterval>
{
    protected override MonthDayNanosecondInterval ReadValue(ref AvroBinaryReader reader)
    {
        var bytes = reader.ReadFixed(12);
        uint months = BinaryPrimitives.ReadUInt32LittleEndian(bytes);
        uint days = BinaryPrimitives.ReadUInt32LittleEndian(bytes[4..]);
        uint millis = BinaryPrimitives.ReadUInt32LittleEndian(bytes[8..]);
        return new MonthDayNanosecondInterval(
            checked((int)months), checked((int)days), (long)millis * 1_000_000);
    }
}

// Avro timestamp logical types read a single int64 (millis/micros/nanos since epoch),
// which is exactly the Timestamp value-buffer layout. DataType comes from the field.
internal sealed class TimestampBuilder : FixedWidthBuilder<long>
{
    protected override long ReadValue(ref AvroBinaryReader reader) => reader.ReadLong();
    public void AppendDefault(long value) => AppendValueDirect(value);
}

// ─── Complex type builders ───

internal sealed class EnumBuilder : IColumnBuilder
{
    // Dictionary indices are int32 — accumulate them through the native-backed Int32 builder.
    private static readonly Field IndexField = new("indices", Int32Type.Default, nullable: true);
    private readonly IReadOnlyList<string> _symbols;
    private readonly Int32Builder _indexBuilder = new();
    private StringArray? _dictionary;   // immutable — built once, shared across batches

    public EnumBuilder(IReadOnlyList<string> symbols) => _symbols = symbols;

    public void Append(ref AvroBinaryReader reader) => _indexBuilder.Append(ref reader);
    public void AppendNull() => _indexBuilder.AppendNull();
    public void AppendDefault(int index) => _indexBuilder.AppendDefault(index);

    public IArrowArray Build(Field field)
        => new DictionaryArray((DictionaryType)field.DataType,
            _indexBuilder.Build(IndexField), BuildDictionary(_symbols, ref _dictionary));

    public void Reset() => _indexBuilder.Reset();
    public void Reserve(int rowCount) => _indexBuilder.Reserve(rowCount);

    /// <summary>Builds the immutable symbol dictionary once and caches it for reuse across batches.</summary>
    internal static StringArray BuildDictionary(IReadOnlyList<string> symbols, ref StringArray? cache)
    {
        if (cache != null)
            return cache;
        var b = new StringArray.Builder();
        foreach (var sym in symbols)
            b.Append(sym);
        return cache = b.Build();
    }
}

/// <summary>
/// Reads enum indices using the writer's symbol list and remaps them to the reader's symbol indices.
/// Per the Avro spec, writer symbols are mapped to reader symbol positions by name.
/// </summary>
internal sealed class RemappingEnumBuilder : IColumnBuilder
{
    private static readonly Field IndexField = new("indices", Int32Type.Default, nullable: true);
    private readonly IReadOnlyList<string> _readerSymbols;
    private readonly int[] _writerToReaderIndex;
    private readonly Int32Builder _indexBuilder = new();
    private StringArray? _dictionary;   // immutable — built once, shared across batches

    public RemappingEnumBuilder(AvroEnumSchema writerSchema, AvroEnumSchema readerSchema)
    {
        _readerSymbols = readerSchema.Symbols;

        // Build reader symbol lookup
        var readerLookup = new Dictionary<string, int>(StringComparer.Ordinal);
        for (int i = 0; i < readerSchema.Symbols.Count; i++)
            readerLookup[readerSchema.Symbols[i]] = i;

        // Resolve the default index (if reader has a default symbol)
        int defaultIndex = -1;
        if (readerSchema.Default != null)
        {
            if (!readerLookup.TryGetValue(readerSchema.Default, out defaultIndex))
                throw new InvalidOperationException(
                    $"Reader enum default '{readerSchema.Default}' is not in reader symbol list.");
        }

        // Build remap table
        _writerToReaderIndex = new int[writerSchema.Symbols.Count];
        for (int wi = 0; wi < writerSchema.Symbols.Count; wi++)
        {
            if (readerLookup.TryGetValue(writerSchema.Symbols[wi], out int ri))
            {
                _writerToReaderIndex[wi] = ri;
            }
            else if (defaultIndex >= 0)
            {
                _writerToReaderIndex[wi] = defaultIndex;
            }
            else
            {
                throw new InvalidOperationException(
                    $"Writer enum symbol '{writerSchema.Symbols[wi]}' is not in reader schema and reader has no default.");
            }
        }
    }

    public void Append(ref AvroBinaryReader reader)
    {
        int writerIndex = reader.ReadInt();
        _indexBuilder.AppendDefault(_writerToReaderIndex[writerIndex]);
    }

    public void AppendNull() => _indexBuilder.AppendNull();

    /// <summary>Appends a default using a reader symbol index.</summary>
    public void AppendDefault(int readerIndex) => _indexBuilder.AppendDefault(readerIndex);

    public IArrowArray Build(Field field)
        => new DictionaryArray((DictionaryType)field.DataType,
            _indexBuilder.Build(IndexField), EnumBuilder.BuildDictionary(_readerSymbols, ref _dictionary));

    public void Reset() => _indexBuilder.Reset();
    public void Reserve(int rowCount) => _indexBuilder.Reserve(rowCount);
}

/// <summary>
/// Base for columns whose value buffer holds a fixed number of bytes per row (fixed-size
/// binary, decimal). Accumulates into a native buffer sized to the batch row count and
/// transfers ownership to Arrow zero-copy at <see cref="Build"/>, so a built batch is safe to
/// retain while later batches decode. Validity is elided when there are no nulls.
/// </summary>
internal abstract class ByteBlobBuilder : IColumnBuilder
{
    private readonly int _elemBytes;
    private NativeBuffer<byte>? _values;
    private int _capacityElems;
    private int _count;
    private ValidityTracker _validity = new();

    protected ByteBlobBuilder(int elemBytes) => _elemBytes = elemBytes;

    /// <summary>Writes exactly <c>elemBytes</c> bytes for one element into <paramref name="dest"/>.</summary>
    protected abstract void WriteElement(ref AvroBinaryReader reader, Span<byte> dest);

    /// <summary>The Arrow value type for this column.</summary>
    protected abstract IArrowType ValueType(Field field);

    public void Append(ref AvroBinaryReader reader) => WriteElement(ref reader, NextValidSlot());

    public void AppendNull()
    {
        EnsureSlot();
        _values!.ByteSpan.Slice(_count * _elemBytes, _elemBytes).Clear();
        _count++;
        _validity.AppendNull();
    }

    /// <summary>Reserves the next slot, marks it valid, and returns it for the caller to fill.</summary>
    protected Span<byte> NextValidSlot()
    {
        EnsureSlot();
        var dest = _values!.ByteSpan.Slice(_count * _elemBytes, _elemBytes);
        _count++;
        _validity.AppendValid();
        return dest;
    }

    public void Reserve(int rowCount)
    {
        _values?.Dispose();
        _capacityElems = Math.Max(1, rowCount);
        _values = new NativeBuffer<byte>(_capacityElems * _elemBytes, zeroFill: false);
        _validity.Reserve(rowCount);
        _count = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureSlot()
    {
        if (_count < _capacityElems)
            return;
        Grow();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Grow()
    {
        if (_values == null)
        {
            _capacityElems = 64;
            _values = new NativeBuffer<byte>(_capacityElems * _elemBytes, zeroFill: false);
            return;
        }
        _values.Grow((_count + 1) * _elemBytes);
        _capacityElems = _values.Length / _elemBytes;
    }

    public IArrowArray Build(Field field)
    {
        var validity = _validity.NullCount > 0 ? _validity.BuildBitmap() : ArrowBuffer.Empty;
        int count = _count;
        int nullCount = _validity.NullCount;
        var values = _values!.Build();
        _values = null;
        _capacityElems = 0;
        var data = new ArrayData(ValueType(field), count, nullCount, 0, [validity, values]);
        return ArrowArrayFactory.BuildArray(data);
    }

    public void Reset()
    {
        _values?.Dispose();
        _values = null;
        _capacityElems = 0;
        _count = 0;
        _validity.Reset();
    }
}

internal sealed class FixedBuilder : ByteBlobBuilder
{
    private readonly int _size;
    public FixedBuilder(int size) : base(size) => _size = size;

    protected override void WriteElement(ref AvroBinaryReader reader, Span<byte> dest)
        => reader.ReadFixed(_size).CopyTo(dest);

    protected override IArrowType ValueType(Field field)
        => field.DataType as FixedSizeBinaryType ?? new FixedSizeBinaryType(_size);
}

internal sealed class ArrayBuilder : IColumnBuilder
{
    private readonly IColumnBuilder _itemBuilder;
    private OffsetsAccumulator _offsets = new();
    private ValidityTracker _validity = new();

    public ArrayBuilder(IColumnBuilder itemBuilder) => _itemBuilder = itemBuilder;

    public void Append(ref AvroBinaryReader reader)
    {
        int count = 0;
        while (true)
        {
            long blockCount = reader.ReadLong();
            if (blockCount == 0) break;
            if (blockCount < 0)
            {
                blockCount = -blockCount;
                reader.ReadLong(); // skip block byte size
            }
            for (long i = 0; i < blockCount; i++)
            {
                _itemBuilder.Append(ref reader);
                count++;
            }
        }
        _offsets.Append(count);
        _validity.AppendValid();
    }

    public void AppendNull()
    {
        _offsets.Append(0);
        _validity.AppendNull();
    }

    public IArrowArray Build(Field field)
    {
        var listType = (ListType)field.DataType;
        var values = _itemBuilder.Build(listType.ValueField);

        var validity = _validity.NullCount > 0 ? _validity.BuildBitmap() : ArrowBuffer.Empty;
        return new ListArray(listType, _validity.Count,
            _offsets.Build(), values, validity, _validity.NullCount);
    }

    public void Reset()
    {
        _itemBuilder.Reset();
        _offsets.Reset();
        _validity.Reset();
    }

    public void Reserve(int rowCount)
    {
        _validity.Reserve(rowCount);
        _offsets.Reserve(rowCount);
        _itemBuilder.Reserve(rowCount);
    }
}

internal sealed class MapBuilder : IColumnBuilder
{
    private readonly IColumnBuilder _valueBuilder;
    private readonly StringBuilder _keyBuilder = new();
    private OffsetsAccumulator _offsets = new();
    private ValidityTracker _validity = new();

    public MapBuilder(IColumnBuilder valueBuilder) => _valueBuilder = valueBuilder;

    public void Append(ref AvroBinaryReader reader)
    {
        int count = 0;
        while (true)
        {
            long blockCount = reader.ReadLong();
            if (blockCount == 0) break;
            if (blockCount < 0)
            {
                blockCount = -blockCount;
                reader.ReadLong(); // skip block byte size
            }
            for (long i = 0; i < blockCount; i++)
            {
                _keyBuilder.Append(ref reader);   // map keys are Avro strings, read first
                _valueBuilder.Append(ref reader);
                count++;
            }
        }
        _offsets.Append(count);
        _validity.AppendValid();
    }

    public void AppendNull()
    {
        _offsets.Append(0);
        _validity.AppendNull();
    }

    public IArrowArray Build(Field field)
    {
        var mapType = (MapType)field.DataType;
        var keys = _keyBuilder.Build(mapType.KeyField);
        var values = _valueBuilder.Build(mapType.ValueField);

        var structFields = new List<Field> { mapType.KeyField, mapType.ValueField };
        var structType = new StructType(structFields);
        var entries = new StructArray(structType, keys.Length,
            new IArrowArray[] { keys, values }, ArrowBuffer.Empty);

        var validity = _validity.NullCount > 0 ? _validity.BuildBitmap() : ArrowBuffer.Empty;
        return new MapArray(mapType, _validity.Count,
            _offsets.Build(), entries, validity, _validity.NullCount);
    }

    public void Reset()
    {
        _valueBuilder.Reset();
        _keyBuilder.Reset();
        _offsets.Reset();
        _validity.Reset();
    }

    public void Reserve(int rowCount)
    {
        _validity.Reserve(rowCount);
        _keyBuilder.Reserve(rowCount);
        _offsets.Reserve(rowCount);
        _valueBuilder.Reserve(rowCount);
    }
}

internal sealed class StructBuilder : IColumnBuilder
{
    private readonly List<(string name, IColumnBuilder builder)> _children;
    private ValidityTracker _validity = new();

    public StructBuilder(List<(string name, IColumnBuilder builder)> children)
        => _children = children;

    public void Append(ref AvroBinaryReader reader)
    {
        foreach (var (_, builder) in _children)
            builder.Append(ref reader);
        _validity.AppendValid();
    }

    public void AppendNull()
    {
        foreach (var (_, builder) in _children)
            builder.AppendNull();
        _validity.AppendNull();
    }

    public IArrowArray Build(Field field)
    {
        var structType = (StructType)field.DataType;
        var childArrays = new IArrowArray[_children.Count];
        for (int i = 0; i < _children.Count; i++)
            childArrays[i] = _children[i].builder.Build(structType.Fields[i]);

        var validity = _validity.NullCount > 0 ? _validity.BuildBitmap() : ArrowBuffer.Empty;
        return new StructArray(structType, _validity.Count,
            childArrays, validity, _validity.NullCount);
    }

    public void Reset()
    {
        foreach (var (_, builder) in _children)
            builder.Reset();
        _validity.Reset();
    }

    public void Reserve(int rowCount)
    {
        _validity.Reserve(rowCount);
        foreach (var (_, builder) in _children)
            builder.Reserve(rowCount);
    }
}

/// <summary>
/// A struct builder that handles nested record schema evolution.
/// Reads wire data in writer field order but produces a StructArray in reader field order.
/// Writer fields not in the reader are skipped; reader fields not in the writer get defaults.
/// </summary>
internal sealed class ResolvingStructBuilder : IColumnBuilder
{
    private readonly AvroRecordSchema _writerRecord;
    private readonly SchemaResolution _resolution;
    private readonly IColumnBuilder[] _readerBuilders;
    private ValidityTracker _validity = new();

    public ResolvingStructBuilder(
        AvroRecordSchema writerRecord,
        AvroRecordSchema readerRecord,
        SchemaResolution resolution,
        IColumnBuilder[] readerBuilders)
    {
        _writerRecord = writerRecord;
        _resolution = resolution;
        _readerBuilders = readerBuilders;
    }

    public void Append(ref AvroBinaryReader reader)
    {
        // Read writer fields in wire order, dispatching to the correct reader builder or skipping
        for (int wi = 0; wi < _writerRecord.Fields.Count; wi++)
        {
            var action = _resolution.WriterActions[wi];
            if (action.Skip)
            {
                reader.Skip(_writerRecord.Fields[wi].Schema);
            }
            else
            {
                _readerBuilders[action.ReaderFieldIndex].Append(ref reader);
            }
        }

        // Fill defaults for reader fields not present in writer
        foreach (var df in _resolution.DefaultFields)
        {
            DefaultValueApplicator.AppendDefault(_readerBuilders[df.ReaderIndex], df.DefaultValue, df.Schema);
        }

        _validity.AppendValid();
    }

    public void AppendNull()
    {
        foreach (var builder in _readerBuilders)
            builder.AppendNull();
        _validity.AppendNull();
    }

    public IArrowArray Build(Field field)
    {
        var structType = (StructType)field.DataType;
        var childArrays = new IArrowArray[_readerBuilders.Length];
        for (int i = 0; i < _readerBuilders.Length; i++)
            childArrays[i] = _readerBuilders[i].Build(structType.Fields[i]);

        var validity = _validity.NullCount > 0 ? _validity.BuildBitmap() : ArrowBuffer.Empty;
        return new StructArray(structType, _validity.Count,
            childArrays, validity, _validity.NullCount);
    }

    public void Reset()
    {
        foreach (var builder in _readerBuilders)
            builder.Reset();
        _validity.Reset();
    }

    public void Reserve(int rowCount)
    {
        _validity.Reserve(rowCount);
        foreach (var builder in _readerBuilders)
            builder.Reserve(rowCount);
    }
}

// ─── Decimal builders ───

/// <summary>Reads Avro fixed-size bytes and converts to Arrow Decimal128 (big-endian → little-endian).</summary>
internal sealed class DecimalFixedBuilder : ByteBlobBuilder
{
    private readonly int _size;
    private readonly int _precision;
    private readonly int _scale;

    public DecimalFixedBuilder(int size, int precision, int scale) : base(16)
    {
        _size = size;
        _precision = precision;
        _scale = scale;
    }

    protected override void WriteElement(ref AvroBinaryReader reader, Span<byte> dest)
        => DecimalArrayHelper.BigEndianToDecimal128Bytes(reader.ReadFixed(_size), dest);

    protected override IArrowType ValueType(Field field) => new Decimal128Type(_precision, _scale);

    /// <summary>Appends a default value from big-endian Avro bytes.</summary>
    public void AppendDefaultFromBigEndian(ReadOnlySpan<byte> bigEndianBytes)
        => DecimalArrayHelper.BigEndianToDecimal128Bytes(bigEndianBytes, NextValidSlot());
}

/// <summary>Reads Avro variable-length bytes and converts to Arrow Decimal128 (big-endian → little-endian).</summary>
internal sealed class DecimalBytesBuilder : ByteBlobBuilder
{
    private readonly int _precision;
    private readonly int _scale;

    public DecimalBytesBuilder(int precision, int scale) : base(16)
    {
        _precision = precision;
        _scale = scale;
    }

    protected override void WriteElement(ref AvroBinaryReader reader, Span<byte> dest)
        => DecimalArrayHelper.BigEndianToDecimal128Bytes(reader.ReadBytes(), dest);

    protected override IArrowType ValueType(Field field) => new Decimal128Type(_precision, _scale);

    /// <summary>Appends a default value from big-endian Avro bytes.</summary>
    public void AppendDefaultFromBigEndian(ReadOnlySpan<byte> bigEndianBytes)
        => DecimalArrayHelper.BigEndianToDecimal128Bytes(bigEndianBytes, NextValidSlot());
}

internal static class DecimalArrayHelper
{
    /// <summary>Converts Avro big-endian two's complement bytes to 16-byte little-endian for Arrow Decimal128, writing directly into destination.</summary>
    public static void BigEndianToDecimal128Bytes(ReadOnlySpan<byte> bigEndian, Span<byte> dest)
    {
        byte signExtend = (bigEndian.Length > 0 && (bigEndian[0] & 0x80) != 0) ? (byte)0xFF : (byte)0x00;
        dest[..16].Fill(signExtend);
        int len = Math.Min(bigEndian.Length, 16);
        for (int i = 0; i < len; i++)
            dest[i] = bigEndian[bigEndian.Length - 1 - i];
    }

    /// <summary>Allocating overload for default value paths.</summary>
    public static byte[] BigEndianToDecimal128Bytes(ReadOnlySpan<byte> bigEndian)
    {
        var result = new byte[16];
        BigEndianToDecimal128Bytes(bigEndian, result);
        return result;
    }
}

// ─── Dense Union builder ───

internal sealed class DenseUnionBuilder : IColumnBuilder
{
    private readonly IColumnBuilder[] _branchBuilders;
    private readonly AvroUnionSchema _unionSchema;
    private readonly List<byte> _typeIds = new();
    private readonly List<int> _offsets = new();
    private readonly int[] _branchCounts;

    public DenseUnionBuilder(AvroUnionSchema unionSchema, IColumnBuilder[] branchBuilders)
    {
        _unionSchema = unionSchema;
        _branchBuilders = branchBuilders;
        _branchCounts = new int[branchBuilders.Length];
    }

    public void Append(ref AvroBinaryReader reader)
    {
        int branchIndex = reader.ReadUnionIndex();
        _typeIds.Add(checked((byte)branchIndex));
        _offsets.Add(_branchCounts[branchIndex]);
        _branchCounts[branchIndex]++;
        _branchBuilders[branchIndex].Append(ref reader);
    }

    public void AppendNull()
    {
        throw new NotSupportedException("Cannot append null to a non-nullable union.");
    }

    public IArrowArray Build(Field field)
    {
        var unionType = (UnionType)field.DataType;

        // Build child arrays
        var children = new IArrowArray[_branchBuilders.Length];
        for (int i = 0; i < _branchBuilders.Length; i++)
            children[i] = _branchBuilders[i].Build(unionType.Fields[i]);

        var typeIdBuffer = new ArrowBuffer(_typeIds.ToArray());
        var offsetBytes = new int[_offsets.Count];
        for (int i = 0; i < _offsets.Count; i++)
            offsetBytes[i] = _offsets[i];
        var offsetBuffer = new ArrowBuffer(MemoryMarshal.AsBytes(offsetBytes.AsSpan()).ToArray());

        return new DenseUnionArray(unionType, _typeIds.Count, children, typeIdBuffer, offsetBuffer);
    }

    public void Reset()
    {
        _typeIds.Clear();
        _offsets.Clear();
        System.Array.Clear(_branchCounts, 0, _branchCounts.Length);
        foreach (var builder in _branchBuilders)
            builder.Reset();
    }

    public void Reserve(int rowCount)
    {
        // Branch cardinalities are unknown up front; forward the row count as a hint so
        // each branch's value buffer starts at a reasonable size.
        foreach (var builder in _branchBuilders)
            builder.Reserve(rowCount);
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;
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
}

// ─── Validity bitmap accumulator ───

/// <summary>
/// Compact validity bitmap that stores 1 bit per value instead of 8 bytes (List&lt;bool&gt;).
/// Tracks null count inline to avoid a second pass.
/// </summary>
internal struct ValidityTracker
{
    private byte[] _bitmap;
    private int _count;
    private int _nullCount;

    public ValidityTracker() : this(64) { }

    public ValidityTracker(int initialCapacity)
    {
        _bitmap = new byte[(initialCapacity + 7) / 8];
        _count = 0;
        _nullCount = 0;
    }

    public int Count => _count;
    public int NullCount => _nullCount;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AppendValid()
    {
        int byteIndex = _count / 8;
        if (byteIndex >= _bitmap.Length)
            Grow();
        _bitmap[byteIndex] |= (byte)(1 << (_count % 8));
        _count++;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AppendNull()
    {
        int byteIndex = _count / 8;
        if (byteIndex >= _bitmap.Length)
            Grow();
        // bit is already 0
        _count++;
        _nullCount++;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Grow()
    {
        int newSize = Math.Max(_bitmap.Length * 2, 8);
        var newBitmap = new byte[newSize];
        _bitmap.CopyTo(newBitmap, 0);
        _bitmap = newBitmap;
    }

    public ArrowBuffer BuildBitmap() => new(_bitmap.AsMemory(0, (_count + 7) / 8));

    /// <summary>Resets count and null count, retaining the allocated bitmap.</summary>
    public void Reset()
    {
        _bitmap.AsSpan(0, (_count + 7) / 8).Clear();
        _count = 0;
        _nullCount = 0;
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
}

internal sealed class BooleanBuilder : IColumnBuilder
{
    private Apache.Arrow.BooleanArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadBoolean());
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(bool value) => _builder.Append(value);
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new();
}

internal sealed class Int32Builder : IColumnBuilder
{
    private Apache.Arrow.Int32Array.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadInt());
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(int value) => _builder.Append(value);
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new();
}

internal sealed class Int64Builder : IColumnBuilder
{
    private Apache.Arrow.Int64Array.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadLong());
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(long value) => _builder.Append(value);
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new();
}

internal sealed class FloatBuilder : IColumnBuilder
{
    private Apache.Arrow.FloatArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadFloat());
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(float value) => _builder.Append(value);
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new();
}

internal sealed class DoubleBuilder : IColumnBuilder
{
    private Apache.Arrow.DoubleArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadDouble());
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(double value) => _builder.Append(value);
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new();
}

internal sealed class BinaryBuilder : IColumnBuilder
{
    private Apache.Arrow.BinaryArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadBytes());
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(byte[] value) => _builder.Append(value);
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new();
}

internal sealed class StringBuilder : IColumnBuilder
{
    private Apache.Arrow.StringArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadString());
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(string value) => _builder.Append(value);
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new();
}

/// <summary>
/// Builds a <see cref="Apache.Arrow.GuidArray"/> from Avro <c>uuid</c>
/// logical-type strings (36-char canonical form per the Avro spec).
/// Selected by <see cref="RecordBatchAssembler.CreateBuilderForType"/> when
/// the target Arrow type is an <c>arrow.uuid</c> extension; falls back to
/// <see cref="StringBuilder"/> when no registry is supplied.
/// </summary>
internal sealed class GuidBuilder : IColumnBuilder
{
    private Apache.Arrow.GuidArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(Guid.Parse(reader.ReadString()));
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(string value) => _builder.Append(Guid.Parse(value));
    public IArrowArray Build(Field field) => _builder.Build(allocator: null);
    public void Reset() => _builder = new();
}

// ─── Logical type builders ───

internal sealed class Date32Builder : IColumnBuilder
{
#if NET6_0_OR_GREATER
    private static readonly DateOnly Epoch = new(1970, 1, 1);
    private Apache.Arrow.Date32Array.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader)
        => _builder.Append(Epoch.AddDays(reader.ReadInt()));
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(int daysSinceEpoch) => _builder.Append(Epoch.AddDays(daysSinceEpoch));
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new();
#else
    private static readonly DateTime Epoch = new DateTime(1970, 1, 1);
    private Apache.Arrow.Date32Array.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader)
        => _builder.Append(Epoch.AddDays(reader.ReadInt()));
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(int daysSinceEpoch) => _builder.Append(Epoch.AddDays(daysSinceEpoch));
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new();
#endif
}

internal sealed class Time32MillisBuilder : IColumnBuilder
{
    private Apache.Arrow.Time32Array.Builder _builder = new(new Time32Type(TimeUnit.Millisecond));
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadInt());
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(int value) => _builder.Append(value);
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new(new Time32Type(TimeUnit.Millisecond));
}

internal sealed class Time64MicrosBuilder : IColumnBuilder
{
    private Apache.Arrow.Time64Array.Builder _builder = new(new Time64Type(TimeUnit.Microsecond));
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadLong());
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(long value) => _builder.Append(value);
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new(new Time64Type(TimeUnit.Microsecond));
}

internal sealed class Time64NanosBuilder : IColumnBuilder
{
    private Apache.Arrow.Time64Array.Builder _builder = new(new Time64Type(TimeUnit.Nanosecond));
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadLong());
    public void AppendNull() => _builder.AppendNull();
    public void AppendDefault(long value) => _builder.Append(value);
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new(new Time64Type(TimeUnit.Nanosecond));
}

/// <summary>
/// Reads Avro duration (fixed 12 bytes: months u32 LE, days u32 LE, millis u32 LE)
/// into Arrow MonthDayNanosecondIntervalArray (months, days, nanoseconds).
/// </summary>
internal sealed class DurationBuilder : IColumnBuilder
{
    private MonthDayNanosecondIntervalArray.Builder _builder = new();

    public void Append(ref AvroBinaryReader reader)
    {
        var bytes = reader.ReadFixed(12);
        uint months = BinaryPrimitives.ReadUInt32LittleEndian(bytes);
        uint days = BinaryPrimitives.ReadUInt32LittleEndian(bytes[4..]);
        uint millis = BinaryPrimitives.ReadUInt32LittleEndian(bytes[8..]);
        _builder.Append(new MonthDayNanosecondInterval(
            checked((int)months), checked((int)days), (long)millis * 1_000_000));
    }

    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new();
}

internal sealed class TimestampBuilder : IColumnBuilder
{
    private byte[] _buffer = new byte[64];
    private int _count;
    private ValidityTracker _validity = new();

    public void Append(ref AvroBinaryReader reader) { AppendValue(reader.ReadLong()); _validity.AppendValid(); }
    public void AppendNull() { AppendValue(0); _validity.AppendNull(); }
    public void AppendDefault(long value) { AppendValue(value); _validity.AppendValid(); }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AppendValue(long value)
    {
        int offset = _count * 8;
        if (offset + 8 > _buffer.Length)
            System.Array.Resize(ref _buffer, _buffer.Length * 2);
        BinaryPrimitives.WriteInt64LittleEndian(_buffer.AsSpan(offset), value);
        _count++;
    }

    public IArrowArray Build(Field field)
    {
        var data = new ArrayData(field.DataType, _count, _validity.NullCount,
            0, [_validity.BuildBitmap(), new ArrowBuffer(_buffer.AsMemory(0, _count * 8))]);
        return ArrowArrayFactory.BuildArray(data);
    }

    public void Reset() { _count = 0; _validity.Reset(); }
}

// ─── Complex type builders ───

internal sealed class EnumBuilder : IColumnBuilder
{
    private readonly IReadOnlyList<string> _symbols;
    private Int32Array.Builder _indexBuilder = new();

    public EnumBuilder(IReadOnlyList<string> symbols) => _symbols = symbols;

    public void Append(ref AvroBinaryReader reader) => _indexBuilder.Append(reader.ReadInt());
    public void AppendNull() => _indexBuilder.AppendNull();
    public void AppendDefault(int index) => _indexBuilder.Append(index);

    public IArrowArray Build(Field field)
    {
        var dictBuilder = new StringArray.Builder();
        foreach (var sym in _symbols)
            dictBuilder.Append(sym);

        return new DictionaryArray((DictionaryType)field.DataType,
            _indexBuilder.Build(), dictBuilder.Build());
    }

    public void Reset() => _indexBuilder = new();
}

/// <summary>
/// Reads enum indices using the writer's symbol list and remaps them to the reader's symbol indices.
/// Per the Avro spec, writer symbols are mapped to reader symbol positions by name.
/// </summary>
internal sealed class RemappingEnumBuilder : IColumnBuilder
{
    private readonly IReadOnlyList<string> _readerSymbols;
    private readonly int[] _writerToReaderIndex;
    private Int32Array.Builder _indexBuilder = new();

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
        _indexBuilder.Append(_writerToReaderIndex[writerIndex]);
    }

    public void AppendNull() => _indexBuilder.AppendNull();

    /// <summary>Appends a default using a reader symbol index.</summary>
    public void AppendDefault(int readerIndex) => _indexBuilder.Append(readerIndex);

    public IArrowArray Build(Field field)
    {
        var dictBuilder = new StringArray.Builder();
        foreach (var sym in _readerSymbols)
            dictBuilder.Append(sym);

        return new DictionaryArray((DictionaryType)field.DataType,
            _indexBuilder.Build(), dictBuilder.Build());
    }

    public void Reset() => _indexBuilder = new();
}

internal sealed class FixedBuilder : IColumnBuilder
{
    private readonly int _size;
    private byte[] _values;
    private int _count;
    private ValidityTracker _validity = new();

    public FixedBuilder(int size)
    {
        _size = size;
        _values = new byte[size * 64];
    }

    public void Append(ref AvroBinaryReader reader)
    {
        EnsureCapacity();
        reader.ReadFixed(_size).CopyTo(_values.AsSpan(_count * _size));
        _count++;
        _validity.AppendValid();
    }

    public void AppendNull()
    {
        EnsureCapacity();
        _values.AsSpan(_count * _size, _size).Clear();
        _count++;
        _validity.AppendNull();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureCapacity()
    {
        int required = (_count + 1) * _size;
        if (required <= _values.Length) return;
        var newValues = new byte[Math.Max(_values.Length * 2, required)];
        _values.AsSpan(0, _count * _size).CopyTo(newValues);
        _values = newValues;
    }

    public IArrowArray Build(Field field)
    {
        var dataType = field.DataType as FixedSizeBinaryType ?? new FixedSizeBinaryType(_size);
        var data = new ArrayData(dataType, _count, _validity.NullCount,
            0, [_validity.BuildBitmap(), new ArrowBuffer(_values.AsMemory(0, _count * _size))]);
        return ArrowArrayFactory.BuildArray(data);
    }

    public void Reset() { _count = 0; _validity.Reset(); }
}

internal sealed class ArrayBuilder : IColumnBuilder
{
    private readonly IColumnBuilder _itemBuilder;
    private readonly List<int> _offsets = new() { 0 };
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
        _offsets.Add(_offsets[^1] + count);
        _validity.AppendValid();
    }

    public void AppendNull()
    {
        _offsets.Add(_offsets[^1]);
        _validity.AppendNull();
    }

    public IArrowArray Build(Field field)
    {
        var listType = (ListType)field.DataType;
        var values = _itemBuilder.Build(listType.ValueField);
        return BuildListArray(listType, values, _offsets, _validity);
    }

    public void Reset()
    {
        _itemBuilder.Reset();
        _offsets.Clear();
        _offsets.Add(0);
        _validity.Reset();
    }

    internal static ListArray BuildListArray(
        ListType listType, IArrowArray values, List<int> offsets, ValidityTracker validity)
    {
        var offsetBuffer = new ArrowBuffer.Builder<int>();
        foreach (var o in offsets) offsetBuffer.Append(o);

        return new ListArray(listType, validity.Count,
            offsetBuffer.Build(), values, validity.BuildBitmap(), validity.NullCount);
    }
}

internal sealed class MapBuilder : IColumnBuilder
{
    private readonly IColumnBuilder _valueBuilder;
    private StringArray.Builder _keyBuilder = new();
    private readonly List<int> _offsets = new() { 0 };
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
                _keyBuilder.Append(reader.ReadString());
                _valueBuilder.Append(ref reader);
                count++;
            }
        }
        _offsets.Add(_offsets[^1] + count);
        _validity.AppendValid();
    }

    public void AppendNull()
    {
        _offsets.Add(_offsets[^1]);
        _validity.AppendNull();
    }

    public IArrowArray Build(Field field)
    {
        var mapType = (MapType)field.DataType;
        var keys = _keyBuilder.Build();
        var values = _valueBuilder.Build(mapType.ValueField);

        var structFields = new List<Field> { mapType.KeyField, mapType.ValueField };
        var structType = new StructType(structFields);
        var entries = new StructArray(structType, keys.Length,
            new IArrowArray[] { keys, values }, ArrowBuffer.Empty);

        var offsetBuffer = new ArrowBuffer.Builder<int>();
        foreach (var o in _offsets) offsetBuffer.Append(o);

        return new MapArray(mapType, _validity.Count,
            offsetBuffer.Build(), entries, _validity.BuildBitmap(), _validity.NullCount);
    }

    public void Reset()
    {
        _valueBuilder.Reset();
        _keyBuilder = new();
        _offsets.Clear();
        _offsets.Add(0);
        _validity.Reset();
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

        return new StructArray(structType, _validity.Count,
            childArrays, _validity.BuildBitmap(), _validity.NullCount);
    }

    public void Reset()
    {
        foreach (var (_, builder) in _children)
            builder.Reset();
        _validity.Reset();
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

        return new StructArray(structType, _validity.Count,
            childArrays, _validity.BuildBitmap(), _validity.NullCount);
    }

    public void Reset()
    {
        foreach (var builder in _readerBuilders)
            builder.Reset();
        _validity.Reset();
    }
}

// ─── Decimal builders ───

/// <summary>Reads Avro fixed-size bytes and converts to Arrow Decimal128 (big-endian → little-endian).</summary>
internal sealed class DecimalFixedBuilder : IColumnBuilder
{
    private readonly int _size;
    private readonly int _precision;
    private readonly int _scale;
    private byte[] _values;
    private int _count;
    private ValidityTracker _validity = new();

    public DecimalFixedBuilder(int size, int precision, int scale)
    {
        _size = size;
        _precision = precision;
        _scale = scale;
        _values = new byte[16 * 64];
    }

    public void Append(ref AvroBinaryReader reader)
    {
        EnsureCapacity();
        var bytes = reader.ReadFixed(_size);
        DecimalArrayHelper.BigEndianToDecimal128Bytes(bytes, _values.AsSpan(_count * 16));
        _count++;
        _validity.AppendValid();
    }

    public void AppendNull()
    {
        EnsureCapacity();
        _values.AsSpan(_count * 16, 16).Clear();
        _count++;
        _validity.AppendNull();
    }

    /// <summary>Appends a default value from big-endian Avro bytes.</summary>
    public void AppendDefaultFromBigEndian(ReadOnlySpan<byte> bigEndianBytes)
    {
        EnsureCapacity();
        DecimalArrayHelper.BigEndianToDecimal128Bytes(bigEndianBytes, _values.AsSpan(_count * 16));
        _count++;
        _validity.AppendValid();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureCapacity()
    {
        int required = (_count + 1) * 16;
        if (required <= _values.Length) return;
        var newValues = new byte[Math.Max(_values.Length * 2, required)];
        _values.AsSpan(0, _count * 16).CopyTo(newValues);
        _values = newValues;
    }

    public IArrowArray Build(Field field)
    {
        var dataType = new Decimal128Type(_precision, _scale);
        var data = new ArrayData(dataType, _count, _validity.NullCount,
            0, [_validity.BuildBitmap(), new ArrowBuffer(_values.AsMemory(0, _count * 16))]);
        return ArrowArrayFactory.BuildArray(data);
    }

    public void Reset() { _count = 0; _validity.Reset(); }
}

/// <summary>Reads Avro variable-length bytes and converts to Arrow Decimal128 (big-endian → little-endian).</summary>
internal sealed class DecimalBytesBuilder : IColumnBuilder
{
    private readonly int _precision;
    private readonly int _scale;
    private byte[] _values;
    private int _count;
    private ValidityTracker _validity = new();

    public DecimalBytesBuilder(int precision, int scale)
    {
        _precision = precision;
        _scale = scale;
        _values = new byte[16 * 64];
    }

    public void Append(ref AvroBinaryReader reader)
    {
        EnsureCapacity();
        var bytes = reader.ReadBytes();
        DecimalArrayHelper.BigEndianToDecimal128Bytes(bytes, _values.AsSpan(_count * 16));
        _count++;
        _validity.AppendValid();
    }

    public void AppendNull()
    {
        EnsureCapacity();
        _values.AsSpan(_count * 16, 16).Clear();
        _count++;
        _validity.AppendNull();
    }

    /// <summary>Appends a default value from big-endian Avro bytes.</summary>
    public void AppendDefaultFromBigEndian(ReadOnlySpan<byte> bigEndianBytes)
    {
        EnsureCapacity();
        DecimalArrayHelper.BigEndianToDecimal128Bytes(bigEndianBytes, _values.AsSpan(_count * 16));
        _count++;
        _validity.AppendValid();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureCapacity()
    {
        int required = (_count + 1) * 16;
        if (required <= _values.Length) return;
        var newValues = new byte[Math.Max(_values.Length * 2, required)];
        _values.AsSpan(0, _count * 16).CopyTo(newValues);
        _values = newValues;
    }

    public IArrowArray Build(Field field)
    {
        var dataType = new Decimal128Type(_precision, _scale);
        var data = new ArrayData(dataType, _count, _validity.NullCount,
            0, [_validity.BuildBitmap(), new ArrowBuffer(_values.AsMemory(0, _count * 16))]);
        return ArrowArrayFactory.BuildArray(data);
    }

    public void Reset() { _count = 0; _validity.Reset(); }
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

    public static IArrowArray BuildDecimal128Array(
        Field field, byte[] values, int count, ValidityTracker validity, int precision, int scale)
    {
        var dataType = new Decimal128Type(precision, scale);
        var data = new ArrayData(dataType, count, validity.NullCount,
            0, [validity.BuildBitmap(), new ArrowBuffer(values.AsMemory(0, count * 16))]);
        return ArrowArrayFactory.BuildArray(data);
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
}

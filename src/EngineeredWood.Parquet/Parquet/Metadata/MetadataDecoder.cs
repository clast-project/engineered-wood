// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Compression;
using EngineeredWood.Parquet.Thrift;

namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// Decodes Parquet file metadata from Thrift Compact Protocol bytes.
/// </summary>
internal static class MetadataDecoder
{
    // Field dispatch is guarded by the WIRE type (`case N when type == ...`): a field id whose encoded
    // type doesn't match the parquet.thrift declaration falls through to `default` and is skipped, the
    // same way Thrift-generated readers behave. Old or foreign writers reuse field ids with different
    // types — e.g. Impala's dict-page-offset-zero.parquet carries a LIST at ColumnMetaData field 15,
    // which modern parquet.thrift assigns to bloom_filter_length: i32; reading the list header as a
    // varint desynchronizes the whole stream.

    /// <summary>
    /// Decodes a <see cref="FileMetaData"/> from the raw Thrift-encoded footer bytes.
    /// </summary>
    public static FileMetaData DecodeFileMetaData(ReadOnlySpan<byte> data)
    {
        var reader = new ThriftCompactReader(data);
        return ReadFileMetaData(ref reader);
    }

    private static FileMetaData ReadFileMetaData(ref ThriftCompactReader reader)
    {
        reader.PushStruct();

        int? version = null;
        IReadOnlyList<SchemaElement>? schema = null;
        long? numRows = null;
        IReadOnlyList<RowGroup>? rowGroups = null;
        IReadOnlyList<KeyValue>? keyValueMetadata = null;
        string? createdBy = null;
        IReadOnlyList<ColumnOrder>? columnOrders = null;

        while (true)
        {
            var (type, fieldId) = reader.ReadFieldHeader();
            if (type == ThriftType.Stop) break;

            switch (fieldId)
            {
                case 1 when type == ThriftType.I32: // version: i32
                    version = reader.ReadZigZagInt32();
                    break;
                case 2 when type == ThriftType.List: // schema: list<SchemaElement>
                    schema = ReadSchemaList(ref reader);
                    break;
                case 3 when type == ThriftType.I64: // num_rows: i64
                    numRows = reader.ReadZigZagInt64();
                    break;
                case 4 when type == ThriftType.List: // row_groups: list<RowGroup>
                    rowGroups = ReadRowGroupList(ref reader);
                    break;
                case 5 when type == ThriftType.List: // key_value_metadata: list<KeyValue>
                    keyValueMetadata = ReadKeyValueList(ref reader);
                    break;
                case 6 when type == ThriftType.Binary: // created_by: string
                    createdBy = reader.ReadString();
                    break;
                case 7 when type == ThriftType.List: // column_orders: list<ColumnOrder>
                    columnOrders = ReadColumnOrderList(ref reader);
                    break;
                default:
                    reader.Skip(type);
                    break;
            }
        }

        reader.PopStruct();

        return new FileMetaData
        {
            Version = version ?? throw new ParquetFormatException("FileMetaData missing required field: version"),
            Schema = schema ?? throw new ParquetFormatException("FileMetaData missing required field: schema"),
            NumRows = numRows ?? throw new ParquetFormatException("FileMetaData missing required field: num_rows"),
            RowGroups = rowGroups ?? throw new ParquetFormatException("FileMetaData missing required field: row_groups"),
            KeyValueMetadata = keyValueMetadata,
            CreatedBy = createdBy,
            ColumnOrders = columnOrders,
        };
    }

    private static ColumnOrder[] ReadColumnOrderList(ref ThriftCompactReader reader)
    {
        var (elemType, count) = reader.ReadListHeader();
        var array = new ColumnOrder[count];
        for (int i = 0; i < count; i++)
        {
            if (elemType == ThriftType.Struct)
            {
                array[i] = ReadColumnOrder(ref reader);
            }
            else
            {
                reader.Skip(elemType);
                array[i] = ColumnOrder.Undefined;
            }
        }
        return array;
    }

    /// <summary>
    /// Reads a single <c>ColumnOrder</c> union. The union is encoded as a struct
    /// with exactly one (empty-struct) field set; an unrecognized or absent
    /// member decodes to <see cref="ColumnOrder.Undefined"/>.
    /// </summary>
    private static ColumnOrder ReadColumnOrder(ref ThriftCompactReader reader)
    {
        reader.PushStruct();

        var order = ColumnOrder.Undefined;
        while (true)
        {
            var (type, fid) = reader.ReadFieldHeader();
            if (type == ThriftType.Stop) break;

            order = fid switch
            {
                1 => ColumnOrder.TypeDefined,        // TYPE_ORDER: TypeDefinedOrder
                2 => ColumnOrder.Ieee754TotalOrder,  // IEEE_754_TOTAL_ORDER: IEEE754TotalOrder
                _ => order,                          // unknown future member
            };
            reader.Skip(type); // consume the (empty) member struct
        }

        reader.PopStruct();
        return order;
    }

    private static SchemaElement[] ReadSchemaList(ref ThriftCompactReader reader)
    {
        var (elemType, count) = reader.ReadListHeader();
        var array = new SchemaElement[count];
        for (int i = 0; i < count; i++)
        {
            if (elemType == ThriftType.Struct)
                array[i] = ReadSchemaElement(ref reader);
            else
                reader.Skip(elemType);
        }
        return array;
    }

    private static SchemaElement ReadSchemaElement(ref ThriftCompactReader reader)
    {
        reader.PushStruct();

        string? name = null;
        PhysicalType? physicalType = null;
        int? typeLength = null;
        FieldRepetitionType? repetitionType = null;
        int? numChildren = null;
        ConvertedType? convertedType = null;
        int? scale = null;
        int? precision = null;
        int? fieldId = null;
        LogicalType? logicalType = null;

        while (true)
        {
            var (type, fid) = reader.ReadFieldHeader();
            if (type == ThriftType.Stop) break;

            switch (fid)
            {
                case 1 when type == ThriftType.I32: // type: PhysicalType enum (i32)
                    physicalType = (PhysicalType)reader.ReadZigZagInt32();
                    break;
                case 2 when type == ThriftType.I32: // type_length: i32
                    typeLength = reader.ReadZigZagInt32();
                    break;
                case 3 when type == ThriftType.I32: // repetition_type: FieldRepetitionType enum (i32)
                    repetitionType = (FieldRepetitionType)reader.ReadZigZagInt32();
                    break;
                case 4 when type == ThriftType.Binary: // name: string
                    name = reader.ReadString();
                    break;
                case 5 when type == ThriftType.I32: // num_children: i32
                    numChildren = reader.ReadZigZagInt32();
                    break;
                case 6 when type == ThriftType.I32: // converted_type: ConvertedType enum (i32)
                    convertedType = (ConvertedType)reader.ReadZigZagInt32();
                    break;
                case 7 when type == ThriftType.I32: // scale: i32
                    scale = reader.ReadZigZagInt32();
                    break;
                case 8 when type == ThriftType.I32: // precision: i32
                    precision = reader.ReadZigZagInt32();
                    break;
                case 9 when type == ThriftType.I32: // field_id: i32
                    fieldId = reader.ReadZigZagInt32();
                    break;
                case 10 when type == ThriftType.Struct: // logicalType: LogicalType union
                    logicalType = ReadLogicalType(ref reader);
                    break;
                default:
                    reader.Skip(type);
                    break;
            }
        }

        reader.PopStruct();

        return new SchemaElement
        {
            Name = name ?? throw new ParquetFormatException("SchemaElement missing required field: name"),
            Type = physicalType,
            TypeLength = typeLength,
            RepetitionType = repetitionType,
            NumChildren = numChildren,
            ConvertedType = convertedType,
            Scale = scale,
            Precision = precision,
            FieldId = fieldId,
            LogicalType = logicalType,
        };
    }

    // Cached singletons for parameterless LogicalType variants.
    private static readonly LogicalType CachedString = new LogicalType.StringType();
    private static readonly LogicalType CachedMap = new LogicalType.MapType();
    private static readonly LogicalType CachedList = new LogicalType.ListType();
    private static readonly LogicalType CachedEnum = new LogicalType.EnumType();
    private static readonly LogicalType CachedDate = new LogicalType.DateType();
    private static readonly LogicalType CachedJson = new LogicalType.JsonType();
    private static readonly LogicalType CachedBson = new LogicalType.BsonType();
    private static readonly LogicalType CachedUuid = new LogicalType.UuidType();
    private static readonly LogicalType CachedFloat16 = new LogicalType.Float16Type();
    private static readonly LogicalType CachedVariant = new LogicalType.VariantType();

    private static LogicalType ReadLogicalType(ref ThriftCompactReader reader)
    {
        // LogicalType is a Thrift union — exactly one field is set.
        // Each field is a struct (possibly empty) identified by field ID.
        reader.PushStruct();

        LogicalType? result = null;
        while (true)
        {
            var (type, fid) = reader.ReadFieldHeader();
            if (type == ThriftType.Stop) break;

            switch (fid)
            {
                case 1 when type == ThriftType.Struct: // STRING
                    SkipEmptyStruct(ref reader);
                    result = CachedString;
                    break;
                case 2 when type == ThriftType.Struct: // MAP
                    SkipEmptyStruct(ref reader);
                    result = CachedMap;
                    break;
                case 3 when type == ThriftType.Struct: // LIST
                    SkipEmptyStruct(ref reader);
                    result = CachedList;
                    break;
                case 4 when type == ThriftType.Struct: // ENUM
                    SkipEmptyStruct(ref reader);
                    result = CachedEnum;
                    break;
                case 5 when type == ThriftType.Struct: // DECIMAL
                    result = ReadDecimalLogicalType(ref reader);
                    break;
                case 6 when type == ThriftType.Struct: // DATE
                    SkipEmptyStruct(ref reader);
                    result = CachedDate;
                    break;
                case 7 when type == ThriftType.Struct: // TIME
                    result = ReadTimeLogicalType(ref reader);
                    break;
                case 8 when type == ThriftType.Struct: // TIMESTAMP
                    result = ReadTimestampLogicalType(ref reader);
                    break;
                case 10 when type == ThriftType.Struct: // INTEGER
                    result = ReadIntLogicalType(ref reader);
                    break;
                case 11 when type == ThriftType.Struct: // UNKNOWN (null type)
                    SkipEmptyStruct(ref reader);
                    result = new LogicalType.UnknownLogicalType(fid);
                    break;
                case 12 when type == ThriftType.Struct: // JSON
                    SkipEmptyStruct(ref reader);
                    result = CachedJson;
                    break;
                case 13 when type == ThriftType.Struct: // BSON
                    SkipEmptyStruct(ref reader);
                    result = CachedBson;
                    break;
                case 14 when type == ThriftType.Struct: // UUID
                    SkipEmptyStruct(ref reader);
                    result = CachedUuid;
                    break;
                case 15 when type == ThriftType.Struct: // FLOAT16
                    SkipEmptyStruct(ref reader);
                    result = CachedFloat16;
                    break;
                case 16 when type == ThriftType.Struct: // VARIANT
                    SkipEmptyStruct(ref reader);
                    result = CachedVariant;
                    break;
                default:
                    reader.Skip(type);
                    result = new LogicalType.UnknownLogicalType(fid);
                    break;
            }
        }

        reader.PopStruct();
        return result ?? new LogicalType.UnknownLogicalType(0);
    }

    private static void SkipEmptyStruct(ref ThriftCompactReader reader)
    {
        reader.PushStruct();
        while (true)
        {
            var (type, _) = reader.ReadFieldHeader();
            if (type == ThriftType.Stop) break;
            reader.Skip(type);
        }
        reader.PopStruct();
    }

    private static LogicalType.DecimalType ReadDecimalLogicalType(ref ThriftCompactReader reader)
    {
        reader.PushStruct();
        int scale = 0;
        int precision = 0;
        while (true)
        {
            var (type, fid) = reader.ReadFieldHeader();
            if (type == ThriftType.Stop) break;
            switch (fid)
            {
                case 1 when type == ThriftType.I32: scale = reader.ReadZigZagInt32(); break;
                case 2 when type == ThriftType.I32: precision = reader.ReadZigZagInt32(); break;
                default: reader.Skip(type); break;
            }
        }
        reader.PopStruct();
        return new LogicalType.DecimalType(scale, precision);
    }

    private static TimeUnit ReadTimeUnit(ref ThriftCompactReader reader)
    {
        reader.PushStruct();
        TimeUnit unit = TimeUnit.Millis;
        while (true)
        {
            var (type, fid) = reader.ReadFieldHeader();
            if (type == ThriftType.Stop) break;
            switch (fid)
            {
                case 1 when type == ThriftType.Struct: SkipEmptyStruct(ref reader); unit = TimeUnit.Millis; break;
                case 2 when type == ThriftType.Struct: SkipEmptyStruct(ref reader); unit = TimeUnit.Micros; break;
                case 3 when type == ThriftType.Struct: SkipEmptyStruct(ref reader); unit = TimeUnit.Nanos; break;
                default: reader.Skip(type); break;
            }
        }
        reader.PopStruct();
        return unit;
    }

    private static LogicalType.TimeType ReadTimeLogicalType(ref ThriftCompactReader reader)
    {
        reader.PushStruct();
        bool isAdjustedToUtc = false;
        TimeUnit unit = TimeUnit.Millis;
        while (true)
        {
            var (type, fid) = reader.ReadFieldHeader();
            if (type == ThriftType.Stop) break;
            switch (fid)
            {
                case 1 when type is ThriftType.BooleanTrue or ThriftType.BooleanFalse: isAdjustedToUtc = reader.ReadBool(); break;
                case 2 when type == ThriftType.Struct: unit = ReadTimeUnit(ref reader); break;
                default: reader.Skip(type); break;
            }
        }
        reader.PopStruct();
        return new LogicalType.TimeType(isAdjustedToUtc, unit);
    }

    private static LogicalType.TimestampType ReadTimestampLogicalType(ref ThriftCompactReader reader)
    {
        reader.PushStruct();
        bool isAdjustedToUtc = false;
        TimeUnit unit = TimeUnit.Millis;
        while (true)
        {
            var (type, fid) = reader.ReadFieldHeader();
            if (type == ThriftType.Stop) break;
            switch (fid)
            {
                case 1 when type is ThriftType.BooleanTrue or ThriftType.BooleanFalse: isAdjustedToUtc = reader.ReadBool(); break;
                case 2 when type == ThriftType.Struct: unit = ReadTimeUnit(ref reader); break;
                default: reader.Skip(type); break;
            }
        }
        reader.PopStruct();
        return new LogicalType.TimestampType(isAdjustedToUtc, unit);
    }

    private static LogicalType.IntType ReadIntLogicalType(ref ThriftCompactReader reader)
    {
        reader.PushStruct();
        int bitWidth = 0;
        bool isSigned = false;
        while (true)
        {
            var (type, fid) = reader.ReadFieldHeader();
            if (type == ThriftType.Stop) break;
            switch (fid)
            {
                case 1 when type == ThriftType.Byte: bitWidth = checked((int)reader.ReadByte()); break;
                case 2 when type is ThriftType.BooleanTrue or ThriftType.BooleanFalse: isSigned = reader.ReadBool(); break;
                default: reader.Skip(type); break;
            }
        }
        reader.PopStruct();
        return new LogicalType.IntType(bitWidth, isSigned);
    }

    private static RowGroup[] ReadRowGroupList(ref ThriftCompactReader reader)
    {
        var (elemType, count) = reader.ReadListHeader();
        var array = new RowGroup[count];
        for (int i = 0; i < count; i++)
        {
            if (elemType == ThriftType.Struct)
                array[i] = ReadRowGroup(ref reader);
            else
                reader.Skip(elemType);
        }
        return array;
    }

    private static RowGroup ReadRowGroup(ref ThriftCompactReader reader)
    {
        reader.PushStruct();

        IReadOnlyList<ColumnChunk>? columns = null;
        long? totalByteSize = null;
        long? numRows = null;
        IReadOnlyList<SortingColumn>? sortingColumns = null;
        long? totalCompressedSize = null;
        short? ordinal = null;

        while (true)
        {
            var (type, fid) = reader.ReadFieldHeader();
            if (type == ThriftType.Stop) break;

            switch (fid)
            {
                case 1 when type == ThriftType.List: // columns: list<ColumnChunk>
                    columns = ReadColumnChunkList(ref reader);
                    break;
                case 2 when type == ThriftType.I64: // total_byte_size: i64
                    totalByteSize = reader.ReadZigZagInt64();
                    break;
                case 3 when type == ThriftType.I64: // num_rows: i64
                    numRows = reader.ReadZigZagInt64();
                    break;
                case 4 when type == ThriftType.List: // sorting_columns: list<SortingColumn>
                    sortingColumns = ReadSortingColumnList(ref reader);
                    break;
                case 5: // file_offset: i64 (deprecated, skip)
                    reader.Skip(type);
                    break;
                case 6 when type == ThriftType.I64: // total_compressed_size: i64
                    totalCompressedSize = reader.ReadZigZagInt64();
                    break;
                case 7 when type == ThriftType.I16: // ordinal: i16
                    ordinal = reader.ReadI16();
                    break;
                default:
                    reader.Skip(type);
                    break;
            }
        }

        reader.PopStruct();

        return new RowGroup
        {
            Columns = columns ?? throw new ParquetFormatException("RowGroup missing required field: columns"),
            TotalByteSize = totalByteSize ?? throw new ParquetFormatException("RowGroup missing required field: total_byte_size"),
            NumRows = numRows ?? throw new ParquetFormatException("RowGroup missing required field: num_rows"),
            SortingColumns = sortingColumns,
            TotalCompressedSize = totalCompressedSize,
            Ordinal = ordinal,
        };
    }

    private static ColumnChunk[] ReadColumnChunkList(ref ThriftCompactReader reader)
    {
        var (elemType, count) = reader.ReadListHeader();
        var array = new ColumnChunk[count];
        for (int i = 0; i < count; i++)
        {
            if (elemType == ThriftType.Struct)
                array[i] = ReadColumnChunk(ref reader);
            else
                reader.Skip(elemType);
        }
        return array;
    }

    private static ColumnChunk ReadColumnChunk(ref ThriftCompactReader reader)
    {
        reader.PushStruct();

        string? filePath = null;
        long? fileOffset = null;
        ColumnMetaData? metaData = null;

        while (true)
        {
            var (type, fid) = reader.ReadFieldHeader();
            if (type == ThriftType.Stop) break;

            switch (fid)
            {
                case 1 when type == ThriftType.Binary: // file_path: string
                    filePath = reader.ReadString();
                    break;
                case 2 when type == ThriftType.I64: // file_offset: i64
                    fileOffset = reader.ReadZigZagInt64();
                    break;
                case 3 when type == ThriftType.Struct: // meta_data: ColumnMetaData
                    metaData = ReadColumnMetaData(ref reader);
                    break;
                default:
                    reader.Skip(type);
                    break;
            }
        }

        reader.PopStruct();

        return new ColumnChunk
        {
            FilePath = filePath,
            FileOffset = fileOffset ?? throw new ParquetFormatException("ColumnChunk missing required field: file_offset"),
            MetaData = metaData,
        };
    }

    private static ColumnMetaData ReadColumnMetaData(ref ThriftCompactReader reader)
    {
        reader.PushStruct();

        PhysicalType? physicalType = null;
        IReadOnlyList<Encoding>? encodings = null;
        IReadOnlyList<string>? pathInSchema = null;
        CompressionCodec? codec = null;
        long? numValues = null;
        long? totalUncompressedSize = null;
        long? totalCompressedSize = null;
        long? dataPageOffset = null;
        long? indexPageOffset = null;
        long? dictionaryPageOffset = null;
        Statistics? statistics = null;
        long? bloomFilterOffset = null;
        int? bloomFilterLength = null;

        while (true)
        {
            var (type, fid) = reader.ReadFieldHeader();
            if (type == ThriftType.Stop) break;

            switch (fid)
            {
                case 1 when type == ThriftType.I32: // type: PhysicalType
                    physicalType = (PhysicalType)reader.ReadZigZagInt32();
                    break;
                case 2 when type == ThriftType.List: // encodings: list<Encoding>
                    encodings = ReadEncodingList(ref reader);
                    break;
                case 3 when type == ThriftType.List: // path_in_schema: list<string>
                    pathInSchema = ReadStringList(ref reader);
                    break;
                case 4 when type == ThriftType.I32: // codec: CompressionCodec
                    codec = ParquetCodecFromThrift(reader.ReadZigZagInt32());
                    break;
                case 5 when type == ThriftType.I64: // num_values: i64
                    numValues = reader.ReadZigZagInt64();
                    break;
                case 6 when type == ThriftType.I64: // total_uncompressed_size: i64
                    totalUncompressedSize = reader.ReadZigZagInt64();
                    break;
                case 7 when type == ThriftType.I64: // total_compressed_size: i64
                    totalCompressedSize = reader.ReadZigZagInt64();
                    break;
                case 8: // key_value_metadata: list<KeyValue> (skip)
                    reader.Skip(type);
                    break;
                case 9 when type == ThriftType.I64: // data_page_offset: i64
                    dataPageOffset = reader.ReadZigZagInt64();
                    break;
                case 10 when type == ThriftType.I64: // index_page_offset: i64
                    indexPageOffset = reader.ReadZigZagInt64();
                    break;
                case 11 when type == ThriftType.I64: // dictionary_page_offset: i64
                    dictionaryPageOffset = reader.ReadZigZagInt64();
                    break;
                case 12 when type == ThriftType.Struct: // statistics: Statistics
                    statistics = ReadStatistics(ref reader);
                    break;
                case 13: // encoding_stats: list<PageEncodingStats> (skip)
                    reader.Skip(type);
                    break;
                case 14 when type == ThriftType.I64: // bloom_filter_offset: i64
                    bloomFilterOffset = reader.ReadZigZagInt64();
                    break;
                case 15 when type == ThriftType.I32: // bloom_filter_length: i32
                    bloomFilterLength = checked((int)reader.ReadZigZagInt32());
                    break;
                default:
                    reader.Skip(type);
                    break;
            }
        }

        reader.PopStruct();

        return new ColumnMetaData
        {
            Type = physicalType ?? throw new ParquetFormatException("ColumnMetaData missing required field: type"),
            Encodings = encodings ?? throw new ParquetFormatException("ColumnMetaData missing required field: encodings"),
            // path_in_schema is described as required by the Parquet spec but several writers
            // omit it (and an in-progress spec change makes it officially optional). The reader
            // matches columns to the schema by ordinal position, so a missing path is fine.
            PathInSchema = pathInSchema,
            Codec = codec ?? throw new ParquetFormatException("ColumnMetaData missing required field: codec"),
            NumValues = numValues ?? throw new ParquetFormatException("ColumnMetaData missing required field: num_values"),
            TotalUncompressedSize = totalUncompressedSize ?? throw new ParquetFormatException("ColumnMetaData missing required field: total_uncompressed_size"),
            TotalCompressedSize = totalCompressedSize ?? throw new ParquetFormatException("ColumnMetaData missing required field: total_compressed_size"),
            DataPageOffset = dataPageOffset ?? throw new ParquetFormatException("ColumnMetaData missing required field: data_page_offset"),
            IndexPageOffset = indexPageOffset,
            DictionaryPageOffset = dictionaryPageOffset,
            Statistics = statistics,
            BloomFilterOffset = bloomFilterOffset,
            BloomFilterLength = bloomFilterLength,
        };
    }

    private static Statistics ReadStatistics(ref ThriftCompactReader reader)
    {
        reader.PushStruct();

        byte[]? max = null;
        byte[]? min = null;
        long? nullCount = null;
        long? distinctCount = null;
        byte[]? maxValue = null;
        byte[]? minValue = null;
        bool? isMaxValueExact = null;
        bool? isMinValueExact = null;
        long? nanCount = null;

        while (true)
        {
            var (type, fid) = reader.ReadFieldHeader();
            if (type == ThriftType.Stop) break;

            switch (fid)
            {
                case 1 when type == ThriftType.Binary: // max: binary (deprecated)
                    max = reader.ReadBinary().ToArray();
                    break;
                case 2 when type == ThriftType.Binary: // min: binary (deprecated)
                    min = reader.ReadBinary().ToArray();
                    break;
                case 3 when type == ThriftType.I64: // null_count: i64
                    nullCount = reader.ReadZigZagInt64();
                    break;
                case 4 when type == ThriftType.I64: // distinct_count: i64
                    distinctCount = reader.ReadZigZagInt64();
                    break;
                case 5 when type == ThriftType.Binary: // max_value: binary
                    maxValue = reader.ReadBinary().ToArray();
                    break;
                case 6 when type == ThriftType.Binary: // min_value: binary
                    minValue = reader.ReadBinary().ToArray();
                    break;
                case 7 when type is ThriftType.BooleanTrue or ThriftType.BooleanFalse: // is_max_value_exact: bool
                    isMaxValueExact = reader.ReadBool();
                    break;
                case 8 when type is ThriftType.BooleanTrue or ThriftType.BooleanFalse: // is_min_value_exact: bool
                    isMinValueExact = reader.ReadBool();
                    break;
                case 9 when type == ThriftType.I64: // nan_count: i64 (PARQUET-2249)
                    nanCount = reader.ReadZigZagInt64();
                    break;
                default:
                    reader.Skip(type);
                    break;
            }
        }

        reader.PopStruct();

        return new Statistics
        {
            Max = max,
            Min = min,
            NullCount = nullCount,
            DistinctCount = distinctCount,
            MaxValue = maxValue,
            MinValue = minValue,
            IsMaxValueExact = isMaxValueExact,
            IsMinValueExact = isMinValueExact,
            NanCount = nanCount,
        };
    }

    private static Encoding[] ReadEncodingList(ref ThriftCompactReader reader)
    {
        var (_, count) = reader.ReadListHeader();
        var array = new Encoding[count];
        for (int i = 0; i < count; i++)
            array[i] = (Encoding)reader.ReadZigZagInt32();
        return array;
    }

    private static string[] ReadStringList(ref ThriftCompactReader reader)
    {
        var (_, count) = reader.ReadListHeader();
        var array = new string[count];
        for (int i = 0; i < count; i++)
            array[i] = reader.ReadString();
        return array;
    }

    private static KeyValue[] ReadKeyValueList(ref ThriftCompactReader reader)
    {
        var (elemType, count) = reader.ReadListHeader();
        var array = new KeyValue[count];
        for (int i = 0; i < count; i++)
        {
            if (elemType == ThriftType.Struct)
                array[i] = ReadKeyValue(ref reader);
            else
                reader.Skip(elemType);
        }
        return array;
    }

    private static KeyValue ReadKeyValue(ref ThriftCompactReader reader)
    {
        reader.PushStruct();

        string? key = null;
        string? value = null;

        while (true)
        {
            var (type, fid) = reader.ReadFieldHeader();
            if (type == ThriftType.Stop) break;

            switch (fid)
            {
                case 1 when type == ThriftType.Binary: key = reader.ReadString(); break;
                case 2 when type == ThriftType.Binary: value = reader.ReadString(); break;
                default: reader.Skip(type); break;
            }
        }

        reader.PopStruct();

        return new KeyValue(
            key ?? throw new ParquetFormatException("KeyValue missing required field: key"),
            value);
    }

    private static SortingColumn[] ReadSortingColumnList(ref ThriftCompactReader reader)
    {
        var (elemType, count) = reader.ReadListHeader();
        var array = new SortingColumn[count];
        for (int i = 0; i < count; i++)
        {
            if (elemType == ThriftType.Struct)
                array[i] = ReadSortingColumn(ref reader);
            else
                reader.Skip(elemType);
        }
        return array;
    }

    private static SortingColumn ReadSortingColumn(ref ThriftCompactReader reader)
    {
        reader.PushStruct();

        int columnIndex = 0;
        bool descending = false;
        bool nullsFirst = false;

        while (true)
        {
            var (type, fid) = reader.ReadFieldHeader();
            if (type == ThriftType.Stop) break;

            switch (fid)
            {
                case 1 when type == ThriftType.I32: columnIndex = reader.ReadZigZagInt32(); break;
                case 2 when type is ThriftType.BooleanTrue or ThriftType.BooleanFalse: descending = reader.ReadBool(); break;
                case 3 when type is ThriftType.BooleanTrue or ThriftType.BooleanFalse: nullsFirst = reader.ReadBool(); break;
                default: reader.Skip(type); break;
            }
        }

        reader.PopStruct();

        return new SortingColumn(columnIndex, descending, nullsFirst);
    }

    /// <summary>
    /// Maps a Parquet Thrift CompressionCodec integer to the shared <see cref="CompressionCodec"/> enum.
    /// </summary>
    private static CompressionCodec ParquetCodecFromThrift(int thriftValue) => thriftValue switch
    {
        0 => CompressionCodec.Uncompressed,
        1 => CompressionCodec.Snappy,
        2 => CompressionCodec.Gzip,
        3 => CompressionCodec.Lzo,
        4 => CompressionCodec.Brotli,
        5 => CompressionCodec.Lz4Hadoop,
        6 => CompressionCodec.Zstd,
        7 => CompressionCodec.Lz4,
        _ => throw new ParquetFormatException($"Unknown compression codec: {thriftValue}."),
    };
}

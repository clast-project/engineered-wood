// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Compression;
using EngineeredWood.Parquet.Thrift;

namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// Encodes Parquet file metadata to Thrift Compact Protocol bytes.
/// Mirror of <see cref="MetadataDecoder"/>.
/// </summary>
internal static class MetadataEncoder
{
    /// <summary>
    /// Encodes a <see cref="FileMetaData"/> to Thrift Compact Protocol bytes.
    /// </summary>
    /// <param name="metadata">The file metadata to encode.</param>
    /// <param name="writePathInSchema">
    /// When <see langword="false"/>, omits the <c>path_in_schema</c> field from each
    /// column chunk's metadata. The Parquet spec describes the field as required, but
    /// the field is redundant with the schema and an in-progress spec change makes it
    /// officially optional. Default is <see langword="true"/>.
    /// </param>
    public static byte[] EncodeFileMetaData(FileMetaData metadata, bool writePathInSchema = true)
    {
        var writer = new ThriftCompactWriter(4096);
        WriteFileMetaData(writer, metadata, writePathInSchema);
        return writer.ToArray();
    }

    private static void WriteFileMetaData(ThriftCompactWriter writer, FileMetaData metadata, bool writePathInSchema)
    {
        writer.PushStruct();

        // Field 1: version (i32)
        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32(metadata.Version);

        // Field 2: schema (list<SchemaElement>)
        writer.WriteFieldHeader(ThriftType.List, 2);
        WriteSchemaList(writer, metadata.Schema);

        // Field 3: num_rows (i64)
        writer.WriteFieldHeader(ThriftType.I64, 3);
        writer.WriteZigZagInt64(metadata.NumRows);

        // Field 4: row_groups (list<RowGroup>)
        writer.WriteFieldHeader(ThriftType.List, 4);
        WriteRowGroupList(writer, metadata.RowGroups, writePathInSchema);

        // Field 5: key_value_metadata (optional, list<KeyValue>)
        if (metadata.KeyValueMetadata is { Count: > 0 })
        {
            writer.WriteFieldHeader(ThriftType.List, 5);
            WriteKeyValueList(writer, metadata.KeyValueMetadata);
        }

        // Field 6: created_by (optional, string)
        if (metadata.CreatedBy != null)
        {
            writer.WriteFieldHeader(ThriftType.Binary, 6);
            writer.WriteString(metadata.CreatedBy);
        }

        // Field 7: column_orders (optional, list<ColumnOrder>)
        if (metadata.ColumnOrders is { Count: > 0 })
        {
            writer.WriteFieldHeader(ThriftType.List, 7);
            WriteColumnOrderList(writer, metadata.ColumnOrders);
        }

        writer.WriteStructStop();
        writer.PopStruct();
    }

    private static void WriteColumnOrderList(
        ThriftCompactWriter writer, IReadOnlyList<ColumnOrder> orders)
    {
        writer.WriteListHeader(ThriftType.Struct, orders.Count);
        for (int i = 0; i < orders.Count; i++)
            WriteColumnOrder(writer, orders[i]);
    }

    /// <summary>
    /// Writes a single <c>ColumnOrder</c> union: a struct whose one set field is
    /// an empty member struct. <see cref="ColumnOrder.Undefined"/> writes an
    /// empty union (no member set), which readers treat as unknown ordering.
    /// </summary>
    private static void WriteColumnOrder(ThriftCompactWriter writer, ColumnOrder order)
    {
        writer.PushStruct();
        switch (order)
        {
            case ColumnOrder.TypeDefined:
                writer.WriteFieldHeader(ThriftType.Struct, 1); // TYPE_ORDER
                writer.WriteStructStop();                      // empty TypeDefinedOrder
                break;
            case ColumnOrder.Ieee754TotalOrder:
                writer.WriteFieldHeader(ThriftType.Struct, 2); // IEEE_754_TOTAL_ORDER
                writer.WriteStructStop();                      // empty IEEE754TotalOrder
                break;
        }
        writer.WriteStructStop(); // union stop
        writer.PopStruct();
    }

    private static void WriteSchemaList(ThriftCompactWriter writer, IReadOnlyList<SchemaElement> schema)
    {
        writer.WriteListHeader(ThriftType.Struct, schema.Count);
        for (int i = 0; i < schema.Count; i++)
            WriteSchemaElement(writer, schema[i]);
    }

    private static void WriteSchemaElement(ThriftCompactWriter writer, SchemaElement element)
    {
        writer.PushStruct();

        // Field 1: type (optional, PhysicalType enum as i32)
        if (element.Type.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I32, 1);
            writer.WriteZigZagInt32((int)element.Type.Value);
        }

        // Field 2: type_length (optional, i32)
        if (element.TypeLength.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I32, 2);
            writer.WriteZigZagInt32(element.TypeLength.Value);
        }

        // Field 3: repetition_type (optional, FieldRepetitionType enum as i32)
        if (element.RepetitionType.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I32, 3);
            writer.WriteZigZagInt32((int)element.RepetitionType.Value);
        }

        // Field 4: name (required, string)
        writer.WriteFieldHeader(ThriftType.Binary, 4);
        writer.WriteString(element.Name);

        // Field 5: num_children (optional, i32)
        if (element.NumChildren.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I32, 5);
            writer.WriteZigZagInt32(element.NumChildren.Value);
        }

        // Field 6: converted_type (optional, ConvertedType enum as i32)
        if (element.ConvertedType.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I32, 6);
            writer.WriteZigZagInt32((int)element.ConvertedType.Value);
        }

        // Field 7: scale (optional, i32)
        if (element.Scale.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I32, 7);
            writer.WriteZigZagInt32(element.Scale.Value);
        }

        // Field 8: precision (optional, i32)
        if (element.Precision.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I32, 8);
            writer.WriteZigZagInt32(element.Precision.Value);
        }

        // Field 9: field_id (optional, i32)
        if (element.FieldId.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I32, 9);
            writer.WriteZigZagInt32(element.FieldId.Value);
        }

        // Field 10: logicalType (optional, LogicalType union)
        if (element.LogicalType != null)
        {
            writer.WriteFieldHeader(ThriftType.Struct, 10);
            WriteLogicalType(writer, element.LogicalType);
        }

        writer.WriteStructStop();
        writer.PopStruct();
    }

    private static void WriteLogicalType(ThriftCompactWriter writer, LogicalType logicalType)
    {
        writer.PushStruct();

        switch (logicalType)
        {
            case LogicalType.StringType:
                writer.WriteFieldHeader(ThriftType.Struct, 1);
                WriteEmptyStruct(writer);
                break;
            case LogicalType.MapType:
                writer.WriteFieldHeader(ThriftType.Struct, 2);
                WriteEmptyStruct(writer);
                break;
            case LogicalType.ListType:
                writer.WriteFieldHeader(ThriftType.Struct, 3);
                WriteEmptyStruct(writer);
                break;
            case LogicalType.EnumType:
                writer.WriteFieldHeader(ThriftType.Struct, 4);
                WriteEmptyStruct(writer);
                break;
            case LogicalType.DecimalType dec:
                writer.WriteFieldHeader(ThriftType.Struct, 5);
                WriteDecimalLogicalType(writer, dec);
                break;
            case LogicalType.DateType:
                writer.WriteFieldHeader(ThriftType.Struct, 6);
                WriteEmptyStruct(writer);
                break;
            case LogicalType.TimeType time:
                writer.WriteFieldHeader(ThriftType.Struct, 7);
                WriteTimeLogicalType(writer, time);
                break;
            case LogicalType.TimestampType ts:
                writer.WriteFieldHeader(ThriftType.Struct, 8);
                WriteTimestampLogicalType(writer, ts);
                break;
            case LogicalType.IntType intType:
                writer.WriteFieldHeader(ThriftType.Struct, 10);
                WriteIntLogicalType(writer, intType);
                break;
            case LogicalType.JsonType:
                writer.WriteFieldHeader(ThriftType.Struct, 12);
                WriteEmptyStruct(writer);
                break;
            case LogicalType.BsonType:
                writer.WriteFieldHeader(ThriftType.Struct, 13);
                WriteEmptyStruct(writer);
                break;
            case LogicalType.UuidType:
                writer.WriteFieldHeader(ThriftType.Struct, 14);
                WriteEmptyStruct(writer);
                break;
            case LogicalType.Float16Type:
                writer.WriteFieldHeader(ThriftType.Struct, 15);
                WriteEmptyStruct(writer);
                break;
            case LogicalType.VariantType:
                writer.WriteFieldHeader(ThriftType.Struct, 16);
                // VariantType { 1: optional i8 specification_version }: written EXPLICITLY as 1 — Spark's
                // parquet variant reader rejects an annotation without it (generic FAILED_READ_FILE), while
                // lenient readers (delta-kernel, DuckDB) accept either form. DuckDB's writer sets it too.
                writer.PushStruct();
                writer.WriteFieldHeader(ThriftType.Byte, 1);
                writer.WriteByte(1);
                writer.WriteStructStop();
                writer.PopStruct();
                break;
            // UnknownLogicalType is not written — it only exists for forward-compat on read
        }

        writer.WriteStructStop();
        writer.PopStruct();
    }

    private static void WriteEmptyStruct(ThriftCompactWriter writer)
    {
        writer.PushStruct();
        writer.WriteStructStop();
        writer.PopStruct();
    }

    private static void WriteDecimalLogicalType(ThriftCompactWriter writer, LogicalType.DecimalType dec)
    {
        writer.PushStruct();

        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32(dec.Scale);

        writer.WriteFieldHeader(ThriftType.I32, 2);
        writer.WriteZigZagInt32(dec.Precision);

        writer.WriteStructStop();
        writer.PopStruct();
    }

    private static void WriteTimeUnit(ThriftCompactWriter writer, TimeUnit unit)
    {
        writer.PushStruct();
        short fieldId = unit switch
        {
            TimeUnit.Millis => 1,
            TimeUnit.Micros => 2,
            TimeUnit.Nanos => 3,
            _ => throw new ArgumentOutOfRangeException(nameof(unit)),
        };
        writer.WriteFieldHeader(ThriftType.Struct, fieldId);
        WriteEmptyStruct(writer);
        writer.WriteStructStop();
        writer.PopStruct();
    }

    private static void WriteTimeLogicalType(ThriftCompactWriter writer, LogicalType.TimeType time)
    {
        writer.PushStruct();

        writer.WriteBoolField(1, time.IsAdjustedToUtc);

        writer.WriteFieldHeader(ThriftType.Struct, 2);
        WriteTimeUnit(writer, time.Unit);

        writer.WriteStructStop();
        writer.PopStruct();
    }

    private static void WriteTimestampLogicalType(ThriftCompactWriter writer, LogicalType.TimestampType ts)
    {
        writer.PushStruct();

        writer.WriteBoolField(1, ts.IsAdjustedToUtc);

        writer.WriteFieldHeader(ThriftType.Struct, 2);
        WriteTimeUnit(writer, ts.Unit);

        writer.WriteStructStop();
        writer.PopStruct();
    }

    private static void WriteIntLogicalType(ThriftCompactWriter writer, LogicalType.IntType intType)
    {
        writer.PushStruct();

        writer.WriteFieldHeader(ThriftType.Byte, 1);
        writer.WriteByte(checked((byte)intType.BitWidth));

        writer.WriteBoolField(2, intType.IsSigned);

        writer.WriteStructStop();
        writer.PopStruct();
    }

    private static void WriteRowGroupList(ThriftCompactWriter writer, IReadOnlyList<RowGroup> rowGroups, bool writePathInSchema)
    {
        writer.WriteListHeader(ThriftType.Struct, rowGroups.Count);
        for (int i = 0; i < rowGroups.Count; i++)
            WriteRowGroup(writer, rowGroups[i], writePathInSchema);
    }

    private static void WriteRowGroup(ThriftCompactWriter writer, RowGroup rowGroup, bool writePathInSchema)
    {
        writer.PushStruct();

        // Field 1: columns (list<ColumnChunk>)
        writer.WriteFieldHeader(ThriftType.List, 1);
        WriteColumnChunkList(writer, rowGroup.Columns, writePathInSchema);

        // Field 2: total_byte_size (i64)
        writer.WriteFieldHeader(ThriftType.I64, 2);
        writer.WriteZigZagInt64(rowGroup.TotalByteSize);

        // Field 3: num_rows (i64)
        writer.WriteFieldHeader(ThriftType.I64, 3);
        writer.WriteZigZagInt64(rowGroup.NumRows);

        // Field 4: sorting_columns (optional, list<SortingColumn>)
        if (rowGroup.SortingColumns is { Count: > 0 })
        {
            writer.WriteFieldHeader(ThriftType.List, 4);
            WriteSortingColumnList(writer, rowGroup.SortingColumns);
        }

        // Field 6: total_compressed_size (optional, i64)
        if (rowGroup.TotalCompressedSize.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I64, 6);
            writer.WriteZigZagInt64(rowGroup.TotalCompressedSize.Value);
        }

        // Field 7: ordinal (optional, i16)
        if (rowGroup.Ordinal.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I16, 7);
            writer.WriteI16(rowGroup.Ordinal.Value);
        }

        writer.WriteStructStop();
        writer.PopStruct();
    }

    private static void WriteColumnChunkList(ThriftCompactWriter writer, IReadOnlyList<ColumnChunk> chunks, bool writePathInSchema)
    {
        writer.WriteListHeader(ThriftType.Struct, chunks.Count);
        for (int i = 0; i < chunks.Count; i++)
            WriteColumnChunk(writer, chunks[i], writePathInSchema);
    }

    private static void WriteColumnChunk(ThriftCompactWriter writer, ColumnChunk chunk, bool writePathInSchema)
    {
        writer.PushStruct();

        // Field 1: file_path (optional, string)
        if (chunk.FilePath != null)
        {
            writer.WriteFieldHeader(ThriftType.Binary, 1);
            writer.WriteString(chunk.FilePath);
        }

        // Field 2: file_offset (i64)
        writer.WriteFieldHeader(ThriftType.I64, 2);
        writer.WriteZigZagInt64(chunk.FileOffset);

        // Field 3: meta_data (optional, ColumnMetaData)
        if (chunk.MetaData != null)
        {
            writer.WriteFieldHeader(ThriftType.Struct, 3);
            WriteColumnMetaData(writer, chunk.MetaData, writePathInSchema);
        }

        writer.WriteStructStop();
        writer.PopStruct();
    }

    private static void WriteColumnMetaData(ThriftCompactWriter writer, ColumnMetaData meta, bool writePathInSchema)
    {
        writer.PushStruct();

        // Field 1: type (PhysicalType as i32)
        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32((int)meta.Type);

        // Field 2: encodings (list<Encoding>)
        writer.WriteFieldHeader(ThriftType.List, 2);
        WriteEncodingList(writer, meta.Encodings);

        // Field 3: path_in_schema (list<string>) — described as required by the Parquet
        // spec, but redundant with the schema and made optional in an in-progress spec
        // change. Skip when the writer was asked to omit it, or when the source metadata
        // didn't carry one (e.g. round-tripping a file produced without it).
        if (writePathInSchema && meta.PathInSchema is not null)
        {
            writer.WriteFieldHeader(ThriftType.List, 3);
            WriteStringList(writer, meta.PathInSchema);
        }

        // Field 4: codec (CompressionCodec as i32)
        writer.WriteFieldHeader(ThriftType.I32, 4);
        writer.WriteZigZagInt32(ParquetCodecToThrift(meta.Codec));

        // Field 5: num_values (i64)
        writer.WriteFieldHeader(ThriftType.I64, 5);
        writer.WriteZigZagInt64(meta.NumValues);

        // Field 6: total_uncompressed_size (i64)
        writer.WriteFieldHeader(ThriftType.I64, 6);
        writer.WriteZigZagInt64(meta.TotalUncompressedSize);

        // Field 7: total_compressed_size (i64)
        writer.WriteFieldHeader(ThriftType.I64, 7);
        writer.WriteZigZagInt64(meta.TotalCompressedSize);

        // Field 9: data_page_offset (i64)
        writer.WriteFieldHeader(ThriftType.I64, 9);
        writer.WriteZigZagInt64(meta.DataPageOffset);

        // Field 10: index_page_offset (optional, i64)
        if (meta.IndexPageOffset.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I64, 10);
            writer.WriteZigZagInt64(meta.IndexPageOffset.Value);
        }

        // Field 11: dictionary_page_offset (optional, i64)
        if (meta.DictionaryPageOffset.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I64, 11);
            writer.WriteZigZagInt64(meta.DictionaryPageOffset.Value);
        }

        // Field 12: statistics (optional)
        if (meta.Statistics != null)
        {
            writer.WriteFieldHeader(ThriftType.Struct, 12);
            WriteStatistics(writer, meta.Statistics);
        }

        // Field 14: bloom_filter_offset (optional, i64)
        if (meta.BloomFilterOffset.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I64, 14);
            writer.WriteZigZagInt64(meta.BloomFilterOffset.Value);
        }

        // Field 15: bloom_filter_length (optional, i32)
        if (meta.BloomFilterLength.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I32, 15);
            writer.WriteZigZagInt32(meta.BloomFilterLength.Value);
        }

        writer.WriteStructStop();
        writer.PopStruct();
    }

    private static void WriteStatistics(ThriftCompactWriter writer, Statistics stats)
    {
        writer.PushStruct();

        // Field 1: max (optional, binary) — deprecated
        if (stats.Max != null)
        {
            writer.WriteFieldHeader(ThriftType.Binary, 1);
            writer.WriteBinary(stats.Max);
        }

        // Field 2: min (optional, binary) — deprecated
        if (stats.Min != null)
        {
            writer.WriteFieldHeader(ThriftType.Binary, 2);
            writer.WriteBinary(stats.Min);
        }

        // Field 3: null_count (optional, i64)
        if (stats.NullCount.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I64, 3);
            writer.WriteZigZagInt64(stats.NullCount.Value);
        }

        // Field 4: distinct_count (optional, i64)
        if (stats.DistinctCount.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I64, 4);
            writer.WriteZigZagInt64(stats.DistinctCount.Value);
        }

        // Field 5: max_value (optional, binary)
        if (stats.MaxValue != null)
        {
            writer.WriteFieldHeader(ThriftType.Binary, 5);
            writer.WriteBinary(stats.MaxValue);
        }

        // Field 6: min_value (optional, binary)
        if (stats.MinValue != null)
        {
            writer.WriteFieldHeader(ThriftType.Binary, 6);
            writer.WriteBinary(stats.MinValue);
        }

        // Field 7: is_max_value_exact (optional, bool)
        if (stats.IsMaxValueExact.HasValue)
            writer.WriteBoolField(7, stats.IsMaxValueExact.Value);

        // Field 8: is_min_value_exact (optional, bool)
        if (stats.IsMinValueExact.HasValue)
            writer.WriteBoolField(8, stats.IsMinValueExact.Value);

        // Field 9: nan_count (optional, i64) — PARQUET-2249
        if (stats.NanCount.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I64, 9);
            writer.WriteZigZagInt64(stats.NanCount.Value);
        }

        writer.WriteStructStop();
        writer.PopStruct();
    }

    private static void WriteEncodingList(ThriftCompactWriter writer, IReadOnlyList<Encoding> encodings)
    {
        writer.WriteListHeader(ThriftType.I32, encodings.Count);
        for (int i = 0; i < encodings.Count; i++)
            writer.WriteZigZagInt32((int)encodings[i]);
    }

    private static void WriteStringList(ThriftCompactWriter writer, IReadOnlyList<string> strings)
    {
        writer.WriteListHeader(ThriftType.Binary, strings.Count);
        for (int i = 0; i < strings.Count; i++)
            writer.WriteString(strings[i]);
    }

    private static void WriteKeyValueList(ThriftCompactWriter writer, IReadOnlyList<KeyValue> kvs)
    {
        writer.WriteListHeader(ThriftType.Struct, kvs.Count);
        for (int i = 0; i < kvs.Count; i++)
            WriteKeyValue(writer, kvs[i]);
    }

    private static void WriteKeyValue(ThriftCompactWriter writer, KeyValue kv)
    {
        writer.PushStruct();

        writer.WriteFieldHeader(ThriftType.Binary, 1);
        writer.WriteString(kv.Key);

        if (kv.Value != null)
        {
            writer.WriteFieldHeader(ThriftType.Binary, 2);
            writer.WriteString(kv.Value);
        }

        writer.WriteStructStop();
        writer.PopStruct();
    }

    private static void WriteSortingColumnList(ThriftCompactWriter writer, IReadOnlyList<SortingColumn> cols)
    {
        writer.WriteListHeader(ThriftType.Struct, cols.Count);
        for (int i = 0; i < cols.Count; i++)
            WriteSortingColumn(writer, cols[i]);
    }

    private static void WriteSortingColumn(ThriftCompactWriter writer, SortingColumn col)
    {
        writer.PushStruct();

        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32(col.ColumnIndex);

        writer.WriteBoolField(2, col.Descending);
        writer.WriteBoolField(3, col.NullsFirst);

        writer.WriteStructStop();
        writer.PopStruct();
    }

    /// <summary>
    /// Encodes a page header to Thrift Compact Protocol bytes.
    /// Used by the write path to produce page header bytes inline.
    /// </summary>
    public static byte[] EncodePageHeader(Data.PageHeader header)
    {
        var writer = new ThriftCompactWriter(64);
        WritePageHeader(writer, header);
        return writer.ToArray();
    }

    private static void WritePageHeader(ThriftCompactWriter writer, Data.PageHeader header)
    {
        writer.PushStruct();

        // Field 1: type (PageType as i32)
        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32((int)header.Type);

        // Field 2: uncompressed_page_size (i32)
        writer.WriteFieldHeader(ThriftType.I32, 2);
        writer.WriteZigZagInt32(header.UncompressedPageSize);

        // Field 3: compressed_page_size (i32)
        writer.WriteFieldHeader(ThriftType.I32, 3);
        writer.WriteZigZagInt32(header.CompressedPageSize);

        // Field 4: crc (optional i32, CRC-32C of page data)
        if (header.Crc.HasValue)
        {
            writer.WriteFieldHeader(ThriftType.I32, 4);
            writer.WriteZigZagInt32(header.Crc.Value);
        }

        // Field 5: data_page_header (optional)
        if (header.DataPageHeader != null)
        {
            writer.WriteFieldHeader(ThriftType.Struct, 5);
            WriteDataPageHeader(writer, header.DataPageHeader);
        }

        // Field 7: dictionary_page_header (optional)
        if (header.DictionaryPageHeader != null)
        {
            writer.WriteFieldHeader(ThriftType.Struct, 7);
            WriteDictionaryPageHeader(writer, header.DictionaryPageHeader);
        }

        // Field 8: data_page_header_v2 (optional)
        if (header.DataPageHeaderV2 != null)
        {
            writer.WriteFieldHeader(ThriftType.Struct, 8);
            WriteDataPageHeaderV2(writer, header.DataPageHeaderV2);
        }

        writer.WriteStructStop();
        writer.PopStruct();
    }

    private static void WriteDataPageHeader(ThriftCompactWriter writer, Data.DataPageHeader header)
    {
        writer.PushStruct();

        // Field 1: num_values (i32)
        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32(header.NumValues);

        // Field 2: encoding (Encoding as i32)
        writer.WriteFieldHeader(ThriftType.I32, 2);
        writer.WriteZigZagInt32((int)header.Encoding);

        // Field 3: definition_level_encoding (Encoding as i32)
        writer.WriteFieldHeader(ThriftType.I32, 3);
        writer.WriteZigZagInt32((int)header.DefinitionLevelEncoding);

        // Field 4: repetition_level_encoding (Encoding as i32)
        writer.WriteFieldHeader(ThriftType.I32, 4);
        writer.WriteZigZagInt32((int)header.RepetitionLevelEncoding);

        writer.WriteStructStop();
        writer.PopStruct();
    }

    private static void WriteDataPageHeaderV2(ThriftCompactWriter writer, Data.DataPageHeaderV2 header)
    {
        writer.PushStruct();

        // Field 1: num_values (i32)
        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32(header.NumValues);

        // Field 2: num_nulls (i32)
        writer.WriteFieldHeader(ThriftType.I32, 2);
        writer.WriteZigZagInt32(header.NumNulls);

        // Field 3: num_rows (i32)
        writer.WriteFieldHeader(ThriftType.I32, 3);
        writer.WriteZigZagInt32(header.NumRows);

        // Field 4: encoding (Encoding as i32)
        writer.WriteFieldHeader(ThriftType.I32, 4);
        writer.WriteZigZagInt32((int)header.Encoding);

        // Field 5: definition_levels_byte_length (i32)
        writer.WriteFieldHeader(ThriftType.I32, 5);
        writer.WriteZigZagInt32(header.DefinitionLevelsByteLength);

        // Field 6: repetition_levels_byte_length (i32)
        writer.WriteFieldHeader(ThriftType.I32, 6);
        writer.WriteZigZagInt32(header.RepetitionLevelsByteLength);

        // Field 7: is_compressed (optional, bool — default true, only write if false)
        if (!header.IsCompressed)
            writer.WriteBoolField(7, false);

        writer.WriteStructStop();
        writer.PopStruct();
    }

    private static void WriteDictionaryPageHeader(ThriftCompactWriter writer, Data.DictionaryPageHeader header)
    {
        writer.PushStruct();

        // Field 1: num_values (i32)
        writer.WriteFieldHeader(ThriftType.I32, 1);
        writer.WriteZigZagInt32(header.NumValues);

        // Field 2: encoding (Encoding as i32)
        writer.WriteFieldHeader(ThriftType.I32, 2);
        writer.WriteZigZagInt32((int)header.Encoding);

        writer.WriteStructStop();
        writer.PopStruct();
    }

    /// <summary>
    /// Maps the shared <see cref="CompressionCodec"/> enum to Parquet Thrift integer values.
    /// </summary>
    private static int ParquetCodecToThrift(CompressionCodec codec) => codec switch
    {
        CompressionCodec.Uncompressed => 0,
        CompressionCodec.Snappy => 1,
        CompressionCodec.Gzip => 2,
        CompressionCodec.Lzo => 3,
        CompressionCodec.Brotli => 4,
        CompressionCodec.Lz4Hadoop => 5,
        CompressionCodec.Zstd => 6,
        CompressionCodec.Lz4 => 7,
        _ => throw new NotSupportedException($"Compression codec '{codec}' is not supported in the Parquet format."),
    };
}

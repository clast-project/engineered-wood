// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet.Thrift;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Decodes Parquet page headers from Thrift compact protocol bytes.
/// </summary>
internal static class PageHeaderDecoder
{
    /// <summary>
    /// Decodes a page header from the given data span.
    /// </summary>
    /// <param name="data">The raw bytes starting at the page header.</param>
    /// <param name="bytesConsumed">The number of bytes consumed by the page header.</param>
    /// <returns>The decoded page header.</returns>
    public static PageHeader Decode(ReadOnlySpan<byte> data, out int bytesConsumed)
    {
        var reader = new ThriftCompactReader(data);

        PageType? type = null;
        int uncompressedPageSize = 0;
        int compressedPageSize = 0;
        int? crc = null;
        DataPageHeader? dataPageHeader = null;
        DictionaryPageHeader? dictionaryPageHeader = null;
        DataPageHeaderV2? dataPageHeaderV2 = null;

        reader.PushStruct();
        while (true)
        {
            var (fieldType, fieldId) = reader.ReadFieldHeader();
            if (fieldType == ThriftType.Stop)
                break;

            switch (fieldId)
            {
                case 1: // type
                    type = (PageType)reader.ReadZigZagInt32();
                    break;
                case 2: // uncompressed_page_size
                    uncompressedPageSize = reader.ReadZigZagInt32();
                    break;
                case 3: // compressed_page_size
                    compressedPageSize = reader.ReadZigZagInt32();
                    break;
                case 4: // crc (optional i32, CRC-32C of page data)
                    crc = reader.ReadZigZagInt32();
                    break;
                case 5: // data_page_header
                    dataPageHeader = ReadDataPageHeader(ref reader);
                    break;
                case 6: // index_page_header (skip)
                    reader.Skip(fieldType);
                    break;
                case 7: // dictionary_page_header
                    dictionaryPageHeader = ReadDictionaryPageHeader(ref reader);
                    break;
                case 8: // data_page_header_v2
                    dataPageHeaderV2 = ReadDataPageHeaderV2(ref reader);
                    break;
                default:
                    reader.Skip(fieldType);
                    break;
            }
        }
        reader.PopStruct();

        bytesConsumed = reader.Position;

        if (!type.HasValue)
            throw new ParquetFormatException("PageHeader is missing required 'type' field.");

        return new PageHeader
        {
            Type = type.Value,
            UncompressedPageSize = uncompressedPageSize,
            CompressedPageSize = compressedPageSize,
            Crc = crc,
            DataPageHeader = dataPageHeader,
            DictionaryPageHeader = dictionaryPageHeader,
            DataPageHeaderV2 = dataPageHeaderV2,
        };
    }

    private static DataPageHeader ReadDataPageHeader(ref ThriftCompactReader reader)
    {
        int numValues = 0;
        Encoding encoding = Encoding.Plain;
        Encoding defLevelEncoding = Encoding.Plain;
        Encoding repLevelEncoding = Encoding.Plain;

        reader.PushStruct();
        while (true)
        {
            var (fieldType, fieldId) = reader.ReadFieldHeader();
            if (fieldType == ThriftType.Stop)
                break;

            switch (fieldId)
            {
                case 1: // num_values
                    numValues = reader.ReadZigZagInt32();
                    break;
                case 2: // encoding
                    encoding = (Encoding)reader.ReadZigZagInt32();
                    break;
                case 3: // definition_level_encoding
                    defLevelEncoding = (Encoding)reader.ReadZigZagInt32();
                    break;
                case 4: // repetition_level_encoding
                    repLevelEncoding = (Encoding)reader.ReadZigZagInt32();
                    break;
                default:
                    reader.Skip(fieldType);
                    break;
            }
        }
        reader.PopStruct();

        return new DataPageHeader
        {
            NumValues = numValues,
            Encoding = encoding,
            DefinitionLevelEncoding = defLevelEncoding,
            RepetitionLevelEncoding = repLevelEncoding,
        };
    }

    private static DataPageHeaderV2 ReadDataPageHeaderV2(ref ThriftCompactReader reader)
    {
        int numValues = 0;
        int numNulls = 0;
        int numRows = 0;
        Encoding encoding = Encoding.Plain;
        int defByteLength = 0;
        int repByteLength = 0;
        bool isCompressed = true;

        reader.PushStruct();
        while (true)
        {
            var (fieldType, fieldId) = reader.ReadFieldHeader();
            if (fieldType == ThriftType.Stop)
                break;

            switch (fieldId)
            {
                case 1: // num_values
                    numValues = reader.ReadZigZagInt32();
                    break;
                case 2: // num_nulls
                    numNulls = reader.ReadZigZagInt32();
                    break;
                case 3: // num_rows
                    numRows = reader.ReadZigZagInt32();
                    break;
                case 4: // encoding
                    encoding = (Encoding)reader.ReadZigZagInt32();
                    break;
                case 5: // definition_levels_byte_length
                    defByteLength = reader.ReadZigZagInt32();
                    break;
                case 6: // repetition_levels_byte_length
                    repByteLength = reader.ReadZigZagInt32();
                    break;
                case 7: // is_compressed
                    isCompressed = reader.ReadBool();
                    break;
                default:
                    reader.Skip(fieldType);
                    break;
            }
        }
        reader.PopStruct();

        return new DataPageHeaderV2
        {
            NumValues = numValues,
            NumNulls = numNulls,
            NumRows = numRows,
            Encoding = encoding,
            DefinitionLevelsByteLength = defByteLength,
            RepetitionLevelsByteLength = repByteLength,
            IsCompressed = isCompressed,
        };
    }

    private static DictionaryPageHeader ReadDictionaryPageHeader(ref ThriftCompactReader reader)
    {
        int numValues = 0;
        Encoding encoding = Encoding.Plain;

        reader.PushStruct();
        while (true)
        {
            var (fieldType, fieldId) = reader.ReadFieldHeader();
            if (fieldType == ThriftType.Stop)
                break;

            switch (fieldId)
            {
                case 1: // num_values
                    numValues = reader.ReadZigZagInt32();
                    break;
                case 2: // encoding
                    encoding = (Encoding)reader.ReadZigZagInt32();
                    break;
                default:
                    reader.Skip(fieldType);
                    break;
            }
        }
        reader.PopStruct();

        return new DictionaryPageHeader
        {
            NumValues = numValues,
            Encoding = encoding,
        };
    }
}

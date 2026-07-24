// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Clast.BloomFilter;
using EngineeredWood.Compression;
using EngineeredWood.Parquet.BloomFilter;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Encodes an Arrow column into Parquet pages (PLAIN or dictionary encoding).
/// Returns the complete column chunk bytes and metadata.
/// </summary>
internal static class ColumnChunkWriter
{
    /// <summary>
    /// Result of encoding a column chunk.
    /// </summary>
    internal readonly struct ColumnChunkResult
    {
        public required ArraySegment<byte> Data { get; init; }
        public required ColumnMetaData MetaData { get; init; }

        /// <summary>
        /// Size of the dictionary page in bytes (including header), or 0 if no dictionary.
        /// Used by the caller to calculate DictionaryPageOffset vs DataPageOffset.
        /// </summary>
        public int DictionaryPageSize { get; init; }

        /// <summary>
        /// Serialized Bloom filter block (Thrift header + bitset), or null if not enabled.
        /// </summary>
        public byte[]? BloomFilterData { get; init; }
    }

    [ThreadStatic]
    private static byte[]? t_compressBuffer;

    /// <summary>
    /// Encodes an Arrow array as a Parquet column chunk (flat column, single def level).
    /// </summary>
    public static ColumnChunkResult WriteColumn(
        IArrowArray array,
        IReadOnlyList<string> pathInSchema,
        PhysicalType physicalType,
        int typeLength,
        bool isNullable,
        ParquetWriteOptions options)
    {
        int rowCount = array.Length;
        int maxDefLevel = isNullable ? 1 : 0;

        // Extract definition levels for nullable columns
        int[]? defLevels = null;
        int nonNullCount = rowCount;

        if (isNullable)
        {
            defLevels = new int[rowCount];
            nonNullCount = 0;
            for (int i = 0; i < rowCount; i++)
            {
                bool isNull = array.IsNull(i);
                defLevels[i] = isNull ? 0 : 1;
                if (!isNull) nonNullCount++;
            }
        }

        return WriteColumnCore(array, rowCount, pathInSchema, physicalType, typeLength,
            maxDefLevel, 0, defLevels, null, nonNullCount, options);
    }

    /// <summary>
    /// Encodes an Arrow array as a Parquet column chunk with pre-computed def/rep levels
    /// (for nested columns).
    /// </summary>
    public static ColumnChunkResult WriteColumn(
        IArrowArray array,
        IReadOnlyList<string> pathInSchema,
        PhysicalType physicalType,
        int typeLength,
        int maxDefLevel,
        int maxRepLevel,
        int[] defLevels,
        int[]? repLevels,
        int nonNullCount,
        int levelCount,
        ParquetWriteOptions options)
    {
        return WriteColumnCore(array, levelCount, pathInSchema, physicalType, typeLength,
            maxDefLevel, maxRepLevel, defLevels, repLevels, nonNullCount, options);
    }

    private static ColumnChunkResult WriteColumnCore(
        IArrowArray array,
        int rowCount,
        IReadOnlyList<string> pathInSchema,
        PhysicalType physicalType,
        int typeLength,
        int maxDefLevel,
        int maxRepLevel,
        int[]? defLevels,
        int[]? repLevels,
        int nonNullCount,
        ParquetWriteOptions options)
    {
        // Resolve per-column overrides for compression, level, and byte-array encoding
        var columnCodec = options.GetCodec(pathInSchema);
        var columnLevel = options.GetCompressionLevel(pathInSchema);
        var columnByteArrayEncoding = options.GetByteArrayEncoding(pathInSchema);
        if (columnCodec != options.Compression
            || columnLevel != options.CompressionLevel
            || columnByteArrayEncoding != options.ByteArrayEncoding)
        {
            options = options with
            {
                Compression = columnCodec,
                CompressionLevel = columnLevel,
                ByteArrayEncoding = columnByteArrayEncoding,
            };
        }

        // Normalize def levels for value-encoding and dictionary methods:
        // these check defLevels[i] == 0 for null, which only works when maxDefLevel <= 1.
        int[]? valueDefLevels = NormalizeDefLevels(defLevels, maxDefLevel);

        // NARROW integer arrays (1-/2-byte Arrow buffers: Int8/UInt8/Int16/UInt16) write as the 4-byte
        // Int32 PHYSICAL type, but the value extraction below (dictionary, PLAIN, the V2 encoders, stats)
        // reinterprets the raw value buffer AT THE PHYSICAL WIDTH — reading a 1-byte buffer as int packs
        // four values per element: SILENT corruption when the Arrow buffer's 64-byte padding hides the
        // overrun, an out-of-range exception when it does not. Widen ONCE here so every downstream
        // consumer sees an aligned buffer (same single-chokepoint shape as the FLBA byte reversal below).
        if (physicalType == PhysicalType.Int32)
        {
            array = WidenNarrowIntArray(array);
        }

        // For decimal FLBA types, reverse bytes from Arrow little-endian to Parquet big-endian.
        // This must happen before encoding/dictionary/statistics so all downstream code sees big-endian.
        if (physicalType == PhysicalType.FixedLenByteArray &&
            array.Data.DataType is Apache.Arrow.Types.Decimal128Type or Apache.Arrow.Types.Decimal256Type)
        {
            array = ReverseFlbaDecimalBytes(array, typeLength);
        }

        // Try dictionary encoding (uses normalized levels for null detection)
        var dictResult = DictionaryEncoder.TryEncode(
            array, physicalType, typeLength, valueDefLevels, nonNullCount, options);

        ColumnChunkResult result;

        if (dictResult != null)
        {
            result = WriteDictionaryColumn(
                dictResult.Value, rowCount, pathInSchema, physicalType,
                maxDefLevel, maxRepLevel, defLevels, repLevels, options);
        }
        else
        {
            // Non-dictionary encoding (PLAIN for V1, type-aware for V2)
            result = WriteNonDictionaryColumn(
                array, rowCount, pathInSchema, physicalType, typeLength,
                maxDefLevel, maxRepLevel, defLevels, repLevels, valueDefLevels, nonNullCount, options);
        }

        // Compute column-level statistics: use dictionary entries when available (O(unique) vs O(total)).
        // FLOAT/DOUBLE columns are always full-scanned so the NaN count covers every value,
        // not just the distinct dictionary entries.
        bool isFloatingPoint = physicalType is PhysicalType.Float or PhysicalType.Double;
        bool floatingPointTotalOrder =
            options.FloatingPointOrder == FloatingPointColumnOrder.Ieee754TotalOrder;
        var stats = dictResult != null && !isFloatingPoint
            ? StatisticsCollector.ComputeFromDictEntries(
                dictResult.Value.DictionaryPageData, dictResult.Value.DictionaryCount,
                physicalType, typeLength, rowCount - nonNullCount)
            : StatisticsCollector.Compute(
                array, physicalType, typeLength, valueDefLevels, nonNullCount, rowCount,
                floatingPointTotalOrder);
        // The DEPRECATED min/max fields are defined with SIGNED byte ordering. Our values are computed with the
        // CORRECT logical ordering (they mirror min_value/max_value), so a legacy reader that honors the
        // deprecated fields under signed semantics would mis-prune UTF-8 / unsigned / decimal-FLBA columns.
        // parquet-mr's rule: emit the deprecated fields only where signed ordering IS the logical ordering
        // (booleans, signed ints incl. date/time/timestamp, floats); drop them elsewhere.
        if (!SignedOrderMatchesLogical(array.Data.DataType))
        {
            stats = new Statistics
            {
                NullCount = stats.NullCount,
                MinValue = stats.MinValue,
                MaxValue = stats.MaxValue,
                IsMinValueExact = stats.IsMinValueExact,
                IsMaxValueExact = stats.IsMaxValueExact,
                NanCount = stats.NanCount,
                DistinctCount = stats.DistinctCount,
            };
        }
        result.MetaData.Statistics = stats;

        // Build Bloom filter if enabled for this column.
        if (options.HasBloomFilter(pathInSchema))
        {
            int ndv = dictResult?.DictionaryCount ?? nonNullCount;
            int filterBytes = SplitBlockBloomFilterBuilder.OptimalNumBytes(
                ndv, options.BloomFilterFpp, options.BloomFilterMaxBytes);

            var bfBuilder = new SplitBlockBloomFilterBuilder(filterBytes);
            BloomFilterArrowEncoder.AddArrowValues(bfBuilder, array, physicalType, typeLength);

            result = new ColumnChunkResult
            {
                Data = result.Data,
                MetaData = result.MetaData,
                DictionaryPageSize = result.DictionaryPageSize,
                BloomFilterData = BloomFilterSerializer.Serialize(bfBuilder.ToArray()),
            };
        }

        return result;
    }

    private static ColumnChunkResult WriteNonDictionaryColumn(
        IArrowArray array, int rowCount,
        IReadOnlyList<string> pathInSchema,
        PhysicalType physicalType, int typeLength,
        int maxDefLevel, int maxRepLevel, int[]? defLevels, int[]? repLevels,
        int[]? valueDefLevels, int nonNullCount,
        ParquetWriteOptions options)
    {
        var output = new MemoryStream(EstimateColumnSize(rowCount, physicalType, typeLength));
        int totalUncompressedSize = 0;
        int totalCompressedSize = 0;
        var encodings = new HashSet<Encoding>();

        if (maxDefLevel > 0 || maxRepLevel > 0)
            encodings.Add(Encoding.Rle);

        int valuesPerPage = EstimateValuesPerPage(array, physicalType, typeLength, options.DataPageSize);
        if (valuesPerPage < 1) valuesPerPage = 1;

        // Reusable encoder for def/rep levels across pages
        var defEncoder = maxDefLevel > 0 ? new RleBitPackedEncoder(BitWidth(maxDefLevel)) : null;
        var repEncoder = maxRepLevel > 0 ? new RleBitPackedEncoder(BitWidth(maxRepLevel)) : null;

        int offset = 0;
        while (offset < rowCount)
        {
            int pageValues = Math.Min(valuesPerPage, rowCount - offset);
            int pageNonNull = maxDefLevel > 0
                ? CountNonNull(defLevels!, offset, pageValues, maxDefLevel)
                : pageValues;

            Encoding pageEncoding;

            if (options.DataPageVersion == DataPageVersion.V2)
            {
                WriteDataPageV2(output, array, offset, pageValues, pageNonNull,
                    physicalType, typeLength, maxDefLevel, maxRepLevel, defLevels, repLevels,
                    valueDefLevels, options, defEncoder, repEncoder,
                    ref totalUncompressedSize, ref totalCompressedSize, out pageEncoding);
            }
            else
            {
                WriteDataPageV1(output, array, offset, pageValues, pageNonNull,
                    physicalType, typeLength, maxDefLevel, maxRepLevel, defLevels, repLevels,
                    valueDefLevels, options, defEncoder, repEncoder,
                    ref totalUncompressedSize, ref totalCompressedSize);
                pageEncoding = Encoding.Plain;
            }

            encodings.Add(pageEncoding);
            offset += pageValues;
        }

        var metadata = new ColumnMetaData
        {
            Type = physicalType,
            Encodings = encodings.ToArray(),
            PathInSchema = pathInSchema is string[] arr ? arr : pathInSchema.ToArray(),
            Codec = options.Compression,
            NumValues = rowCount,
            TotalUncompressedSize = totalUncompressedSize,
            TotalCompressedSize = totalCompressedSize,
            DataPageOffset = 0, // set by caller
        };

        output.TryGetBuffer(out var buffer);
        return new ColumnChunkResult { Data = buffer, MetaData = metadata };
    }

    /// <summary>
    /// Writes a dictionary-encoded column from a pre-built <see cref="DictionaryEncoder.DictionaryResult"/>.
    /// Used by <see cref="BufferedParquetWriter"/> which builds dictionaries incrementally.
    /// </summary>
    internal static ColumnChunkResult WriteDictionaryColumnFromResult(
        DictionaryEncoder.DictionaryResult dictResult,
        int rowCount,
        IReadOnlyList<string> pathInSchema,
        PhysicalType physicalType,
        int maxDefLevel,
        int maxRepLevel,
        int[]? defLevels,
        int[]? repLevels,
        ParquetWriteOptions options) =>
        WriteDictionaryColumn(dictResult, rowCount, pathInSchema, physicalType,
            maxDefLevel, maxRepLevel, defLevels, repLevels, options);

    private static ColumnChunkResult WriteDictionaryColumn(
        DictionaryEncoder.DictionaryResult dictResult,
        int rowCount,
        IReadOnlyList<string> pathInSchema,
        PhysicalType physicalType,
        int maxDefLevel,
        int maxRepLevel,
        int[]? defLevels,
        int[]? repLevels,
        ParquetWriteOptions options)
    {
        var output = new MemoryStream(dictResult.DictionaryPageData.Length + rowCount);
        int totalUncompressedSize = 0;
        int totalCompressedSize = 0;

        int bitWidth = DictionaryEncoder.GetIndexBitWidth(dictResult.DictionaryCount);

        // 1. Encode dictionary page
        int dictionaryPageSize = WriteDictionaryPage(output, dictResult, options,
            ref totalUncompressedSize, ref totalCompressedSize);

        // 2. Encode data pages with RLE dictionary indices
        int bytesPerIndex = Math.Max(1, (bitWidth + 7) / 8);
        int valuesPerPage = Math.Max(1, options.DataPageSize / bytesPerIndex);

        // Reusable encoders across pages
        var defEncoder = maxDefLevel > 0 ? new RleBitPackedEncoder(BitWidth(maxDefLevel)) : null;
        var repEncoder = maxRepLevel > 0 ? new RleBitPackedEncoder(BitWidth(maxRepLevel)) : null;
        var indexEncoder = new RleBitPackedEncoder(bitWidth);

        int offset = 0;
        int indexOffset = 0;
        while (offset < rowCount)
        {
            int pageValues = Math.Min(valuesPerPage, rowCount - offset);
            int pageNonNull = maxDefLevel > 0
                ? CountNonNull(defLevels!, offset, pageValues, maxDefLevel)
                : pageValues;

            var pageIndices = dictResult.Indices.AsSpan(indexOffset, pageNonNull);

            if (options.DataPageVersion == DataPageVersion.V2)
            {
                WriteDictDataPageV2(output, pageIndices, offset, pageValues, pageNonNull,
                    bitWidth, maxDefLevel, maxRepLevel, defLevels, repLevels, options,
                    defEncoder, repEncoder, indexEncoder,
                    ref totalUncompressedSize, ref totalCompressedSize);
            }
            else
            {
                WriteDictDataPageV1(output, pageIndices, offset, pageValues, pageNonNull,
                    bitWidth, maxDefLevel, maxRepLevel, defLevels, repLevels, options,
                    defEncoder, repEncoder, indexEncoder,
                    ref totalUncompressedSize, ref totalCompressedSize);
            }

            offset += pageValues;
            indexOffset += pageNonNull;
        }

        var encodings = maxDefLevel > 0
            ? new[] { Encoding.Plain, Encoding.RleDictionary, Encoding.Rle }
            : new[] { Encoding.Plain, Encoding.RleDictionary };

        var metadata = new ColumnMetaData
        {
            Type = physicalType,
            Encodings = encodings,
            PathInSchema = pathInSchema is string[] arr ? arr : pathInSchema.ToArray(),
            Codec = options.Compression,
            NumValues = rowCount,
            TotalUncompressedSize = totalUncompressedSize,
            TotalCompressedSize = totalCompressedSize,
            DataPageOffset = 0, // set by caller
            DictionaryPageOffset = 0, // set by caller
        };

        output.TryGetBuffer(out var buffer);
        return new ColumnChunkResult
        {
            Data = buffer,
            MetaData = metadata,
            DictionaryPageSize = dictionaryPageSize,
        };
    }

    private static int WriteDictionaryPage(
        MemoryStream output,
        DictionaryEncoder.DictionaryResult dictResult,
        ParquetWriteOptions options,
        ref int totalUncompressed, ref int totalCompressed)
    {
        byte[] dictData = dictResult.DictionaryPageData;
        int uncompressedSize = dictData.Length;

        int compressedLen = CompressTo(dictData, options);

        var pageHeader = new PageHeader
        {
            Type = PageType.DictionaryPage,
            UncompressedPageSize = uncompressedSize,
            CompressedPageSize = compressedLen,
            Crc = options.PageChecksumEnabled
                ? unchecked((int)ComputeCrc32C(t_compressBuffer.AsSpan(0, compressedLen)))
                : null,
            DictionaryPageHeader = new DictionaryPageHeader
            {
                NumValues = dictResult.DictionaryCount,
                Encoding = Encoding.Plain,
            },
        };

        byte[] headerBytes = MetadataEncoder.EncodePageHeader(pageHeader);
        int pageSize = headerBytes.Length + compressedLen;

#if NET8_0_OR_GREATER
        output.Write(headerBytes);
#else
        output.Write(headerBytes, 0, headerBytes.Length);
#endif
        output.Write(t_compressBuffer!, 0, compressedLen);

        totalUncompressed += headerBytes.Length + uncompressedSize;
        totalCompressed += pageSize;

        return pageSize;
    }

    private static void WriteDictDataPageV2(
        MemoryStream output,
        ReadOnlySpan<int> indices, int rowOffset, int numValues, int nonNullCount,
        int bitWidth, int maxDefLevel, int maxRepLevel, int[]? defLevels, int[]? repLevels,
        ParquetWriteOptions options,
        RleBitPackedEncoder? defEncoder, RleBitPackedEncoder? repEncoder,
        RleBitPackedEncoder indexEncoder,
        ref int totalUncompressed, ref int totalCompressed)
    {
        // Encode repetition levels (RLE, no length prefix for V2)
        int repLevelLen = 0;
        if (repEncoder != null)
        {
            repEncoder.Reset();
            repEncoder.Encode(repLevels.AsSpan(rowOffset, numValues));
            repLevelLen = repEncoder.Length;
        }

        // Encode definition levels (RLE, no length prefix for V2)
        int defLevelLen = 0;
        if (defEncoder != null)
        {
            defEncoder.Reset();
            defEncoder.Encode(defLevels.AsSpan(rowOffset, numValues));
            defLevelLen = defEncoder.Length;
        }

        // Encode indices into reusable buffer: 1-byte bit width + RLE/BP hybrid
        int uncompressedValuesSize = EncodeDictionaryIndicesToBuffer(
            indices, nonNullCount, bitWidth, indexEncoder);

        // Compress values section only (V2: levels are uncompressed)
        int compressedValuesLen = CompressTo(
            t_valuesBuffer.AsSpan(0, uncompressedValuesSize), options);

        int numRows = maxRepLevel > 0 ? CountRows(repLevels, rowOffset, numValues, maxRepLevel) : numValues;

        int? pageCrc = null;
        if (options.PageChecksumEnabled)
        {
            var crc = new System.IO.Hashing.Crc32();
            if (repLevelLen > 0) crc.Append(repEncoder!.WrittenSpan);
            if (defLevelLen > 0) crc.Append(defEncoder!.WrittenSpan);
            crc.Append(t_compressBuffer.AsSpan(0, compressedValuesLen));
            pageCrc = unchecked((int)CrcFinish(crc));
        }

        var pageHeader = new PageHeader
        {
            Type = PageType.DataPageV2,
            UncompressedPageSize = repLevelLen + defLevelLen + uncompressedValuesSize,
            CompressedPageSize = repLevelLen + defLevelLen + compressedValuesLen,
            Crc = pageCrc,
            DataPageHeaderV2 = new DataPageHeaderV2
            {
                NumValues = numValues,
                NumNulls = numValues - nonNullCount,
                NumRows = numRows,
                Encoding = Encoding.RleDictionary,
                DefinitionLevelsByteLength = defLevelLen,
                RepetitionLevelsByteLength = repLevelLen,
                IsCompressed = options.Compression != CompressionCodec.Uncompressed,
            },
        };

        byte[] headerBytes = MetadataEncoder.EncodePageHeader(pageHeader);

#if NET8_0_OR_GREATER
        output.Write(headerBytes);
        if (repLevelLen > 0) output.Write(repEncoder!.WrittenSpan);
        if (defLevelLen > 0) output.Write(defEncoder!.WrittenSpan);
#else
        output.Write(headerBytes, 0, headerBytes.Length);
        if (repLevelLen > 0) { var tmp = repEncoder!.WrittenSpan.ToArray(); output.Write(tmp, 0, tmp.Length); }
        if (defLevelLen > 0) { var tmp = defEncoder!.WrittenSpan.ToArray(); output.Write(tmp, 0, tmp.Length); }
#endif
        output.Write(t_compressBuffer!, 0, compressedValuesLen);

        int uncompressedPageSize = headerBytes.Length + repLevelLen + defLevelLen + uncompressedValuesSize;
        int compressedPageSize = headerBytes.Length + repLevelLen + defLevelLen + compressedValuesLen;
        totalUncompressed += uncompressedPageSize;
        totalCompressed += compressedPageSize;
    }

    private static void WriteDictDataPageV1(
        MemoryStream output,
        ReadOnlySpan<int> indices, int rowOffset, int numValues, int nonNullCount,
        int bitWidth, int maxDefLevel, int maxRepLevel, int[]? defLevels, int[]? repLevels,
        ParquetWriteOptions options,
        RleBitPackedEncoder? defEncoder, RleBitPackedEncoder? repEncoder,
        RleBitPackedEncoder indexEncoder,
        ref int totalUncompressed, ref int totalCompressed)
    {
        // Build uncompressed body: rep levels + def levels + values
        // All concatenated and compressed together for V1
        int repRleLen = 0, defRleLen = 0;

        if (repEncoder != null)
        {
            repEncoder.Reset();
            repEncoder.Encode(repLevels.AsSpan(rowOffset, numValues));
            repRleLen = repEncoder.Length;
        }

        if (defEncoder != null)
        {
            defEncoder.Reset();
            defEncoder.Encode(defLevels.AsSpan(rowOffset, numValues));
            defRleLen = defEncoder.Length;
        }

        int valuesLen = EncodeDictionaryIndicesToBuffer(indices, nonNullCount, bitWidth, indexEncoder);

        int repPrefixedLen = repRleLen > 0 ? 4 + repRleLen : 0;
        int defPrefixedLen = defRleLen > 0 ? 4 + defRleLen : 0;
        int uncompressedBodySize = repPrefixedLen + defPrefixedLen + valuesLen;
        byte[] uncompressedBody = new byte[uncompressedBodySize];
        int bPos = 0;

        if (repRleLen > 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(uncompressedBody.AsSpan(bPos), repRleLen);
            bPos += 4;
            repEncoder!.WrittenSpan.CopyTo(uncompressedBody.AsSpan(bPos));
            bPos += repRleLen;
        }
        if (defRleLen > 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(uncompressedBody.AsSpan(bPos), defRleLen);
            bPos += 4;
            defEncoder!.WrittenSpan.CopyTo(uncompressedBody.AsSpan(bPos));
            bPos += defRleLen;
        }
        t_valuesBuffer.AsSpan(0, valuesLen).CopyTo(uncompressedBody.AsSpan(bPos));

        int compressedLen = CompressTo(uncompressedBody, options);

        var pageHeader = new PageHeader
        {
            Type = PageType.DataPage,
            UncompressedPageSize = uncompressedBodySize,
            CompressedPageSize = compressedLen,
            Crc = options.PageChecksumEnabled
                ? unchecked((int)ComputeCrc32C(t_compressBuffer.AsSpan(0, compressedLen)))
                : null,
            DataPageHeader = new DataPageHeader
            {
                NumValues = numValues,
                Encoding = Encoding.RleDictionary,
                DefinitionLevelEncoding = Encoding.Rle,
                RepetitionLevelEncoding = Encoding.Rle,
            },
        };

        byte[] headerBytes = MetadataEncoder.EncodePageHeader(pageHeader);

#if NET8_0_OR_GREATER
        output.Write(headerBytes);
#else
        output.Write(headerBytes, 0, headerBytes.Length);
#endif
        output.Write(t_compressBuffer!, 0, compressedLen);

        totalUncompressed += headerBytes.Length + uncompressedBodySize;
        totalCompressed += headerBytes.Length + compressedLen;
    }

    /// <summary>
    /// Encodes dictionary indices (1-byte bit width + RLE/BP hybrid) into the
    /// thread-static values buffer. Returns the number of bytes written.
    /// </summary>
    private static int EncodeDictionaryIndicesToBuffer(
        ReadOnlySpan<int> indices, int nonNullCount, int bitWidth,
        RleBitPackedEncoder indexEncoder)
    {
        if (nonNullCount == 0)
        {
            EnsureValuesBuffer(1);
            t_valuesBuffer![0] = (byte)bitWidth;
            return 1;
        }

        indexEncoder.Reset();
        indexEncoder.Encode(indices);

        int totalLen = 1 + indexEncoder.Length;
        EnsureValuesBuffer(totalLen);
        t_valuesBuffer![0] = (byte)bitWidth;
        indexEncoder.WrittenSpan.CopyTo(t_valuesBuffer.AsSpan(1));
        return totalLen;
    }

    [ThreadStatic]
    private static byte[]? t_valuesBuffer;

    private static void EnsureValuesBuffer(int size)
    {
        if (t_valuesBuffer == null || t_valuesBuffer.Length < size)
            t_valuesBuffer = new byte[Math.Max(size, 4096)];
    }

    // ───── PLAIN encoding path ─────

    private static int CountNonNull(int[] defLevels, int offset, int count, int maxDefLevel)
    {
        var span = defLevels.AsSpan(offset, count);
        int nonNull = 0;

        foreach (int v in span)
            nonNull += (v >= maxDefLevel) ? 1 : 0;

        return nonNull;
    }

    private static void WriteDataPageV2(
        MemoryStream output,
        IArrowArray array, int offset, int numValues, int nonNullCount,
        PhysicalType physicalType, int typeLength, int maxDefLevel, int maxRepLevel,
        int[]? defLevels, int[]? repLevels, int[]? valueDefLevels,
        ParquetWriteOptions options,
        RleBitPackedEncoder? defEncoder, RleBitPackedEncoder? repEncoder,
        ref int totalUncompressed, ref int totalCompressed,
        out Encoding valueEncoding)
    {
        // Encode repetition levels (RLE, no length prefix for V2)
        int repLevelLen = 0;
        if (repEncoder != null)
        {
            repEncoder.Reset();
            repEncoder.Encode(repLevels.AsSpan(offset, numValues));
            repLevelLen = repEncoder.Length;
        }

        // Encode definition levels (RLE, no length prefix for V2)
        int defLevelLen = 0;
        if (defEncoder != null)
        {
            defEncoder.Reset();
            defEncoder.Encode(defLevels.AsSpan(offset, numValues));
            defLevelLen = defEncoder.Length;
        }

        // Encode values into reusable buffer with type-aware V2 encoding
        int uncompressedValuesSize = EncodeValuesToBuffer(
            array, offset, numValues, nonNullCount, physicalType, typeLength,
            valueDefLevels, options, out valueEncoding);

        // Compress values section only (V2: levels are uncompressed)
        int compressedValuesLen = CompressTo(
            t_valuesBuffer.AsSpan(0, uncompressedValuesSize), options);

        int numNulls = numValues - nonNullCount;
        int numRows = maxRepLevel > 0 ? CountRows(repLevels, offset, numValues, maxRepLevel) : numValues;

        // CRC-32C covers the entire page payload (rep levels + def levels + compressed values).
        int? pageCrc = null;
        if (options.PageChecksumEnabled)
        {
            var crc = new System.IO.Hashing.Crc32();
            if (repLevelLen > 0) crc.Append(repEncoder!.WrittenSpan);
            if (defLevelLen > 0) crc.Append(defEncoder!.WrittenSpan);
            crc.Append(t_compressBuffer.AsSpan(0, compressedValuesLen));
            pageCrc = unchecked((int)CrcFinish(crc));
        }

        var pageHeader = new PageHeader
        {
            Type = PageType.DataPageV2,
            UncompressedPageSize = repLevelLen + defLevelLen + uncompressedValuesSize,
            CompressedPageSize = repLevelLen + defLevelLen + compressedValuesLen,
            Crc = pageCrc,
            DataPageHeaderV2 = new DataPageHeaderV2
            {
                NumValues = numValues,
                NumNulls = numNulls,
                NumRows = numRows,
                Encoding = valueEncoding,
                DefinitionLevelsByteLength = defLevelLen,
                RepetitionLevelsByteLength = repLevelLen,
                IsCompressed = options.Compression != CompressionCodec.Uncompressed,
            },
        };

        byte[] headerBytes = MetadataEncoder.EncodePageHeader(pageHeader);

#if NET8_0_OR_GREATER
        output.Write(headerBytes);
        if (repLevelLen > 0) output.Write(repEncoder!.WrittenSpan);
        if (defLevelLen > 0) output.Write(defEncoder!.WrittenSpan);
#else
        output.Write(headerBytes, 0, headerBytes.Length);
        if (repLevelLen > 0) { var tmp = repEncoder!.WrittenSpan.ToArray(); output.Write(tmp, 0, tmp.Length); }
        if (defLevelLen > 0) { var tmp = defEncoder!.WrittenSpan.ToArray(); output.Write(tmp, 0, tmp.Length); }
#endif
        output.Write(t_compressBuffer!, 0, compressedValuesLen);

        int uncompressedPageSize = headerBytes.Length + repLevelLen + defLevelLen + uncompressedValuesSize;
        int compressedPageSize = headerBytes.Length + repLevelLen + defLevelLen + compressedValuesLen;
        totalUncompressed += uncompressedPageSize;
        totalCompressed += compressedPageSize;
    }

    private static void WriteDataPageV1(
        MemoryStream output,
        IArrowArray array, int offset, int numValues, int nonNullCount,
        PhysicalType physicalType, int typeLength, int maxDefLevel, int maxRepLevel,
        int[]? defLevels, int[]? repLevels, int[]? valueDefLevels,
        ParquetWriteOptions options,
        RleBitPackedEncoder? defEncoder, RleBitPackedEncoder? repEncoder,
        ref int totalUncompressed, ref int totalCompressed)
    {
        // Encode levels
        int repRleLen = 0, defRleLen = 0;

        if (repEncoder != null)
        {
            repEncoder.Reset();
            repEncoder.Encode(repLevels.AsSpan(offset, numValues));
            repRleLen = repEncoder.Length;
        }

        if (defEncoder != null)
        {
            defEncoder.Reset();
            defEncoder.Encode(defLevels.AsSpan(offset, numValues));
            defRleLen = defEncoder.Length;
        }

        // Encode values (use normalized valueDefLevels for null detection)
        byte[] valuesBytes = EncodeValues(array, offset, numValues, nonNullCount,
            physicalType, typeLength, valueDefLevels);

        // Concatenate levels + values, then compress together (V1)
        int repPrefixedLen = repRleLen > 0 ? 4 + repRleLen : 0;
        int defPrefixedLen = defRleLen > 0 ? 4 + defRleLen : 0;
        int uncompressedBodySize = repPrefixedLen + defPrefixedLen + valuesBytes.Length;
        byte[] uncompressedBody = new byte[uncompressedBodySize];
        int bPos = 0;

        if (repRleLen > 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(uncompressedBody.AsSpan(bPos), repRleLen);
            bPos += 4;
            repEncoder!.WrittenSpan.CopyTo(uncompressedBody.AsSpan(bPos));
            bPos += repRleLen;
        }
        if (defRleLen > 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(uncompressedBody.AsSpan(bPos), defRleLen);
            bPos += 4;
            defEncoder!.WrittenSpan.CopyTo(uncompressedBody.AsSpan(bPos));
            bPos += defRleLen;
        }
        valuesBytes.CopyTo(uncompressedBody.AsSpan(bPos));

        int compressedLen = CompressTo(uncompressedBody, options);

        var pageHeader = new PageHeader
        {
            Type = PageType.DataPage,
            UncompressedPageSize = uncompressedBodySize,
            CompressedPageSize = compressedLen,
            Crc = options.PageChecksumEnabled
                ? unchecked((int)ComputeCrc32C(t_compressBuffer.AsSpan(0, compressedLen)))
                : null,
            DataPageHeader = new DataPageHeader
            {
                NumValues = numValues,
                Encoding = Encoding.Plain,
                DefinitionLevelEncoding = Encoding.Rle,
                RepetitionLevelEncoding = Encoding.Rle,
            },
        };

        byte[] headerBytes = MetadataEncoder.EncodePageHeader(pageHeader);

#if NET8_0_OR_GREATER
        output.Write(headerBytes);
#else
        output.Write(headerBytes, 0, headerBytes.Length);
#endif
        output.Write(t_compressBuffer!, 0, compressedLen);

        totalUncompressed += headerBytes.Length + uncompressedBodySize;
        totalCompressed += headerBytes.Length + compressedLen;
    }

    // ───── Compression ─────

    /// <summary>
    /// Compresses data into the thread-static compression buffer.
    /// Returns the number of compressed bytes written.
    /// For Uncompressed codec, copies data into the buffer and returns data.Length.
    /// </summary>
    private static int CompressTo(ReadOnlySpan<byte> data, ParquetWriteOptions options)
    {
        // An EMPTY section still needs a VALID compressed stream: a 0-byte payload declared compressed is
        // rejected by strict decoders (snappy's empty stream is the single 0x00 length varint — SQL Server
        // fails an all-null V2 page's zero-byte values section with "Corrupt snappy compressed data";
        // Delta CHECKPOINT files are full of all-null column chunks). Let the codec encode emptiness.
        int maxLen = Compressor.GetMaxCompressedLength(options.Compression, data.Length);
        EnsureCompressBuffer(maxLen);
        return Compressor.Compress(
            options.Compression, data, t_compressBuffer!.AsSpan(0, maxLen),
            options.CompressionLevel, options.CustomCompressionLevel);
    }

    private static void EnsureCompressBuffer(int size)
    {
        if (t_compressBuffer == null || t_compressBuffer.Length < size)
            t_compressBuffer = new byte[Math.Max(size, 4096)];
    }

    // ───── V2 type-aware encoding dispatch ─────

    /// <summary>
    /// Encodes values into <see cref="t_valuesBuffer"/> using type-aware V2 encoding.
    /// Returns the number of bytes written and the encoding used.
    /// </summary>
    private static int EncodeValuesToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount,
        PhysicalType physicalType, int typeLength, int[]? defLevels,
        ParquetWriteOptions options, out Encoding encoding)
    {
        bool useDba = options.ByteArrayEncoding == ByteArrayEncoding.DeltaByteArray;
#pragma warning disable EWPARQUET0001 // Caller has opted in via the experimental enum value; internal dispatch should not re-warn.
        bool useAlp = options.FloatingPointEncoding == FloatingPointEncoding.Alp;
#pragma warning restore EWPARQUET0001
        bool usePlainFp = options.FloatingPointEncoding == FloatingPointEncoding.Plain;

        if (nonNullCount == 0)
        {
            // Zero defined values (an all-null page — e.g. every row's struct ancestor is null). The V2 delta
            // encodings (DELTA_BINARY_PACKED / DELTA_LENGTH_BYTE_ARRAY / ...) require a non-empty HEADER even
            // for zero values — declaring them with a 0-byte payload makes readers fail with buffer underruns.
            // PLAIN is the only encoding whose empty representation is valid for every type.
            encoding = Encoding.Plain;
            EnsureValuesBuffer(0);
            return 0;
        }

        encoding = EncodingStrategyResolver.GetV2Encoding(physicalType, options.ByteArrayEncoding, options.FloatingPointEncoding);
        return physicalType switch
        {
            PhysicalType.Boolean => EncodeBooleanValuesRleToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Int32 => EncodeDeltaInt32ToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Int64 => EncodeDeltaInt64ToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Float when useAlp => EncodeAlpSingleToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Float when usePlainFp => EncodePlainToBuffer(array, offset, numValues, nonNullCount, physicalType, typeLength, defLevels),
            PhysicalType.Float => EncodeBssSingleToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Double when useAlp => EncodeAlpDoubleToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Double when usePlainFp => EncodePlainToBuffer(array, offset, numValues, nonNullCount, physicalType, typeLength, defLevels),
            PhysicalType.Double => EncodeBssDoubleToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.ByteArray when useDba => EncodeDbaByteArrayToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.ByteArray => EncodeDlbaToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.FixedLenByteArray when useDba => EncodeDbaFlbaToBuffer(array, offset, numValues, nonNullCount, defLevels, typeLength),
            _ => EncodePlainToBuffer(array, offset, numValues, nonNullCount, physicalType, typeLength, defLevels),
        };
    }

    private static int EncodeAlpSingleToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        ReadOnlySpan<float> source = MemoryMarshal.Cast<byte, float>(array.Data.Buffers[1].Span);
        float[] dense;
        ReadOnlySpan<float> values;
        if (defLevels == null)
        {
            values = source.Slice(offset, numValues);
            dense = [];
        }
        else
        {
            dense = ExtractNonNullValues<float>(array, offset, numValues, nonNullCount, defLevels);
            values = dense;
        }

        byte[] page = AlpEncoder.EncodeFloats(values);
        EnsureValuesBuffer(page.Length);
        page.CopyTo(t_valuesBuffer!, 0);
        return page.Length;
    }

    private static int EncodeAlpDoubleToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        ReadOnlySpan<double> source = MemoryMarshal.Cast<byte, double>(array.Data.Buffers[1].Span);
        double[] dense;
        ReadOnlySpan<double> values;
        if (defLevels == null)
        {
            values = source.Slice(offset, numValues);
            dense = [];
        }
        else
        {
            dense = ExtractNonNullValues<double>(array, offset, numValues, nonNullCount, defLevels);
            values = dense;
        }

        byte[] page = AlpEncoder.EncodeDoubles(values);
        EnsureValuesBuffer(page.Length);
        page.CopyTo(t_valuesBuffer!, 0);
        return page.Length;
    }


    private static int EncodeBooleanValuesRleToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        var boolArray = (BooleanArray)array;
        var values = new int[nonNullCount];
        int idx = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels == null || defLevels[offset + i] != 0)
                values[idx++] = boolArray.GetValue(offset + i) == true ? 1 : 0;
        }

        var encoder = new RleBitPackedEncoder(1);
        encoder.Encode(values);

        // 4-byte LE length prefix + RLE data
        int totalLen = 4 + encoder.Length;
        EnsureValuesBuffer(totalLen);
        BinaryPrimitives.WriteInt32LittleEndian(t_valuesBuffer, encoder.Length);
        encoder.WrittenSpan.CopyTo(t_valuesBuffer.AsSpan(4));
        return totalLen;
    }

    private static int EncodeDeltaInt32ToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        var encoder = new DeltaBinaryPackedEncoder();
        if (defLevels == null)
        {
            var span = MemoryMarshal.Cast<byte, int>(array.Data.Buffers[1].Span).Slice(offset, numValues);
            encoder.EncodeInt32s(span);
        }
        else
        {
            var values = ExtractNonNullValues<int>(array, offset, numValues, nonNullCount, defLevels);
            encoder.EncodeInt32s(values);
        }

        EnsureValuesBuffer(encoder.Length);
        encoder.WrittenSpan.CopyTo(t_valuesBuffer);
        return encoder.Length;
    }

    private static int EncodeDeltaInt64ToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        var encoder = new DeltaBinaryPackedEncoder();
        if (defLevels == null)
        {
            var span = MemoryMarshal.Cast<byte, long>(array.Data.Buffers[1].Span).Slice(offset, numValues);
            encoder.EncodeInt64s(span);
        }
        else
        {
            var values = ExtractNonNullValues<long>(array, offset, numValues, nonNullCount, defLevels);
            encoder.EncodeInt64s(values);
        }

        EnsureValuesBuffer(encoder.Length);
        encoder.WrittenSpan.CopyTo(t_valuesBuffer);
        return encoder.Length;
    }

    private static int EncodeBssSingleToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        int totalLen = nonNullCount * sizeof(float);
        EnsureValuesBuffer(totalLen);

        if (defLevels == null)
        {
            var span = MemoryMarshal.Cast<byte, float>(array.Data.Buffers[1].Span).Slice(offset, numValues);
            ByteStreamSplitEncoder.EncodeFloatsTo(span, t_valuesBuffer!);
        }
        else
        {
            var valueBuffer = MemoryMarshal.Cast<byte, float>(array.Data.Buffers[1].Span);
            ByteStreamSplitEncoder.EncodeFloatsTo(valueBuffer, offset, numValues, nonNullCount, defLevels, t_valuesBuffer!);
        }

        return totalLen;
    }

    private static int EncodeBssDoubleToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        int totalLen = nonNullCount * sizeof(double);
        EnsureValuesBuffer(totalLen);

        if (defLevels == null)
        {
            var span = MemoryMarshal.Cast<byte, double>(array.Data.Buffers[1].Span).Slice(offset, numValues);
            ByteStreamSplitEncoder.EncodeDoublesTo(span, t_valuesBuffer!);
        }
        else
        {
            var valueBuffer = MemoryMarshal.Cast<byte, double>(array.Data.Buffers[1].Span);
            ByteStreamSplitEncoder.EncodeDoublesTo(valueBuffer, offset, numValues, nonNullCount, defLevels, t_valuesBuffer!);
        }

        return totalLen;
    }

    private static int EncodeDlbaToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        var data = array.Data;
        ReadOnlySpan<int> arrowOffsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
        ReadOnlySpan<byte> arrowData = data.Buffers[2].Span;

        // Compute lengths and total data size in one pass
        var lengths = new int[nonNullCount];
        int totalDataLen = 0;
        int idx = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels != null && defLevels[offset + i] == 0) continue;
            int start = arrowOffsets[offset + i];
            int len = arrowOffsets[offset + i + 1] - start;
            lengths[idx++] = len;
            totalDataLen += len;
        }

        // Delta-encode lengths
        var deltaEncoder = new DeltaBinaryPackedEncoder();
        deltaEncoder.EncodeInt32s(lengths);

        // Write delta_lengths + compact_data into values buffer
        int totalLen = deltaEncoder.Length + totalDataLen;
        EnsureValuesBuffer(totalLen);
        deltaEncoder.WrittenSpan.CopyTo(t_valuesBuffer);

        // Copy non-null byte data directly
        int pos = deltaEncoder.Length;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels != null && defLevels[offset + i] == 0) continue;
            int start = arrowOffsets[offset + i];
            int len = arrowOffsets[offset + i + 1] - start;
            arrowData.Slice(start, len).CopyTo(t_valuesBuffer.AsSpan(pos));
            pos += len;
        }

        return totalLen;
    }

    private static int EncodeDbaByteArrayToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        var data = array.Data;
        ReadOnlySpan<int> arrowOffsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
        ReadOnlySpan<byte> arrowData = data.Buffers[2].Span;

        // Compute total data bytes for buffer sizing
        int totalDataBytes = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels != null && defLevels[offset + i] == 0) continue;
            totalDataBytes += arrowOffsets[offset + i + 1] - arrowOffsets[offset + i];
        }

        int maxSize = DeltaByteArrayEncoder.EstimateMaxSize(nonNullCount, totalDataBytes);
        EnsureValuesBuffer(maxSize);

        return DeltaByteArrayEncoder.Encode(
            arrowOffsets, arrowData, offset, numValues, nonNullCount, defLevels, t_valuesBuffer!);
    }

    private static int EncodeDbaFlbaToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount,
        int[]? defLevels, int typeLength)
    {
        var valueBuffer = array.Data.Buffers[1].Span;
        int totalDataBytes = nonNullCount * typeLength;

        int maxSize = DeltaByteArrayEncoder.EstimateMaxSize(nonNullCount, totalDataBytes);
        EnsureValuesBuffer(maxSize);

        return DeltaByteArrayEncoder.EncodeFixed(
            valueBuffer, typeLength, offset, numValues, nonNullCount, defLevels, t_valuesBuffer!);
    }

    private static int EncodePlainToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount,
        PhysicalType physicalType, int typeLength, int[]? defLevels)
    {
        byte[] valuesBytes = EncodeValues(array, offset, numValues, nonNullCount,
            physicalType, typeLength, defLevels);
        EnsureValuesBuffer(valuesBytes.Length);
        valuesBytes.CopyTo(t_valuesBuffer.AsSpan());
        return valuesBytes.Length;
    }

    private static T[] ExtractNonNullValues<T>(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
        where T : unmanaged
    {
        var valueBuffer = MemoryMarshal.Cast<byte, T>(array.Data.Buffers[1].Span);

        if (defLevels == null)
        {
            var result = new T[numValues];
            valueBuffer.Slice(offset, numValues).CopyTo(result);
            return result;
        }

        var values = new T[nonNullCount];
        int idx = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels[offset + i] != 0)
                values[idx++] = valueBuffer[offset + i];
        }
        return values;
    }

    // ───── PLAIN encoding (V1 and FLBA fallback) ─────

    private static byte[] EncodeValues(
        IArrowArray array, int offset, int numValues, int nonNullCount,
        PhysicalType physicalType, int typeLength, int[]? defLevels)
    {
        if (nonNullCount == 0)
            return [];

        return physicalType switch
        {
            PhysicalType.Boolean => EncodeBooleanValues(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Int32 => EncodeFixedValues<int>(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Int64 => EncodeFixedValues<long>(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Float => EncodeFixedValues<float>(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Double => EncodeFixedValues<double>(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.ByteArray => EncodeByteArrayValues(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.FixedLenByteArray => EncodeFixedLenByteArrayValues(
                array, offset, numValues, nonNullCount, defLevels, typeLength),
            _ => throw new NotSupportedException($"Unsupported physical type: {physicalType}"),
        };
    }

    private static byte[] EncodeBooleanValues(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        var boolArray = (BooleanArray)array;
        var nonNullValues = new bool[nonNullCount];
        int idx = 0;

        for (int i = 0; i < numValues; i++)
        {
            if (defLevels == null || defLevels[offset + i] != 0)
                nonNullValues[idx++] = boolArray.GetValue(offset + i) ?? false;
        }

        var encoded = new byte[(nonNullCount + 7) / 8];
        PlainEncoder.EncodeBooleans(nonNullValues, encoded);
        return encoded;
    }

    private static byte[] EncodeFixedValues<T>(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
        where T : unmanaged
    {
        int elementSize = Marshal.SizeOf<T>();

        if (defLevels == null)
        {
            // Required column: direct copy from Arrow value buffer
            var valueSpan = MemoryMarshal.Cast<byte, T>(array.Data.Buffers[1].Span).Slice(offset, numValues);
            var result = new byte[numValues * elementSize];
            MemoryMarshal.AsBytes(valueSpan).CopyTo(result);
            return result;
        }

        // Optional column: extract non-null values
        var nonNullValues = new T[nonNullCount];
        int idx = 0;
        var valueBuffer = MemoryMarshal.Cast<byte, T>(array.Data.Buffers[1].Span);
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels[offset + i] != 0)
                nonNullValues[idx++] = valueBuffer[offset + i];
        }

        var encoded = new byte[nonNullCount * elementSize];
        MemoryMarshal.AsBytes(nonNullValues.AsSpan()).CopyTo(encoded);
        return encoded;
    }

    private static byte[] EncodeByteArrayValues(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        var data = array.Data;
        ReadOnlySpan<int> arrowOffsets;
        ReadOnlySpan<byte> arrowData;

        if (array is StringArray || array is BinaryArray)
        {
            arrowOffsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
            arrowData = data.Buffers[2].Span;
        }
        else
        {
            throw new NotSupportedException(
                $"Unsupported Arrow array type for BYTE_ARRAY: {array.GetType().Name}");
        }

        // Calculate total size
        int totalSize = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels != null && defLevels[offset + i] == 0)
                continue;
            int idx = offset + i;
            int len = arrowOffsets[idx + 1] - arrowOffsets[idx];
            totalSize += 4 + len;
        }

        var result = new byte[totalSize];
        int pos = 0;

        for (int i = 0; i < numValues; i++)
        {
            if (defLevels != null && defLevels[offset + i] == 0)
                continue;

            int idx = offset + i;
            int start = arrowOffsets[idx];
            int len = arrowOffsets[idx + 1] - start;

            BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(pos), len);
            pos += 4;
            arrowData.Slice(start, len).CopyTo(result.AsSpan(pos));
            pos += len;
        }

        return result;
    }

    private static byte[] EncodeFixedLenByteArrayValues(
        IArrowArray array, int offset, int numValues, int nonNullCount,
        int[]? defLevels, int typeLength)
    {
        var data = array.Data;
        var valueBuffer = data.Buffers[1].Span;

        if (defLevels == null)
        {
            var result = new byte[numValues * typeLength];
            valueBuffer.Slice(offset * typeLength, numValues * typeLength).CopyTo(result);
            return result;
        }

        var encoded = new byte[nonNullCount * typeLength];
        int pos = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels[offset + i] == 0) continue;
            int idx = offset + i;
            valueBuffer.Slice(idx * typeLength, typeLength).CopyTo(encoded.AsSpan(pos));
            pos += typeLength;
        }

        return encoded;
    }

    private static int EstimateValuesPerPage(
        IArrowArray array, PhysicalType physicalType, int typeLength, int targetPageSize)
    {
        int bytesPerValue = physicalType switch
        {
            PhysicalType.Boolean => 1, // conservative
            PhysicalType.Int32 or PhysicalType.Float => 4,
            PhysicalType.Int64 or PhysicalType.Double => 8,
            PhysicalType.Int96 => 12,
            PhysicalType.FixedLenByteArray => typeLength > 0 ? typeLength : 16,
            PhysicalType.ByteArray => EstimateAvgByteArraySize(array),
            _ => 8,
        };

        if (bytesPerValue == 0) bytesPerValue = 1;
        return Math.Max(1, targetPageSize / bytesPerValue);
    }

    private static int EstimateAvgByteArraySize(IArrowArray array)
    {
        if (array.Length == 0) return 32;

        var data = array.Data;
        if (data.Buffers.Length >= 3)
        {
            var offsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
            int totalDataBytes = offsets[array.Length] - offsets[0];
            int avgLen = totalDataBytes / Math.Max(1, array.Length);
            return 4 + avgLen;
        }

        return 32;
    }

    private static int EstimateColumnSize(int rowCount, PhysicalType physicalType, int typeLength)
    {
        int bytesPerValue = physicalType switch
        {
            PhysicalType.Boolean => 1,
            PhysicalType.Int32 or PhysicalType.Float => 4,
            PhysicalType.Int64 or PhysicalType.Double => 8,
            PhysicalType.FixedLenByteArray => typeLength > 0 ? typeLength : 16,
            _ => 8,
        };
        // Compressed data is typically smaller; estimate 75% of raw size + overhead
        return Math.Max(256, (int)(rowCount * bytesPerValue * 0.75) + 256);
    }

    /// <summary>
    /// For nested columns (maxDefLevel > 1), normalizes def levels to 0/1
    /// so value encoding methods can check != 0 for presence.
    /// Returns null if defLevels is null. Returns defLevels unchanged if maxDefLevel <= 1.
    /// </summary>
    // True when the type's SIGNED byte ordering equals its logical ordering — the precondition for emitting
    // the deprecated Statistics.min/max fields (see the call site).
    private static bool SignedOrderMatchesLogical(Apache.Arrow.Types.IArrowType type) => type switch
    {
        BooleanType => true,
        Int8Type or Int16Type or Int32Type or Int64Type => true,
        FloatType or DoubleType or HalfFloatType => true,
        Date32Type or Date64Type or Time32Type or Time64Type or TimestampType or DurationType => true,
        Decimal32Type or Decimal64Type => true, // INT32/INT64 physical — signed numeric ordering
        _ => false, // UTF-8 strings/binary (unsigned lexical), unsigned ints, decimal FLBA, nested, ...
    };

    private static int[]? NormalizeDefLevels(int[]? defLevels, int maxDefLevel)
    {
        if (defLevels == null)
            return null;
        if (maxDefLevel == 0)
            return null; // fully required column — no nulls possible
        if (maxDefLevel == 1)
            return defLevels; // already 0/1

        // Multi-level: normalize to 0 (absent) / 1 (present)
        var normalized = new int[defLevels.Length];
        for (int i = 0; i < defLevels.Length; i++)
            normalized[i] = defLevels[i] >= maxDefLevel ? 1 : 0;
        return normalized;
    }

    /// <summary>
    /// Widens a 1-/2-byte integer array (Int8/UInt8/Int16/UInt16 — Arrow types whose Parquet physical
    /// type is the 4-byte Int32) to a position-identical <see cref="Int32Array"/>, so the width-naive
    /// buffer reinterpretation in the encoders reads correct values. Sign- vs zero-extension follows the
    /// source type. Any other array (already 4-byte, or a non-integer) passes through unchanged.
    /// </summary>
    private static IArrowArray WidenNarrowIntArray(IArrowArray array)
    {
        switch (array)
        {
            case Int8Array a:
            {
                var b = new Int32Array.Builder();
                b.Reserve(a.Length);
                for (int i = 0; i < a.Length; i++)
                {
                    if (a.IsNull(i)) b.AppendNull();
                    else b.Append(a.GetValue(i)!.Value);
                }
                return b.Build();
            }
            case UInt8Array a:
            {
                var b = new Int32Array.Builder();
                b.Reserve(a.Length);
                for (int i = 0; i < a.Length; i++)
                {
                    if (a.IsNull(i)) b.AppendNull();
                    else b.Append(a.GetValue(i)!.Value);
                }
                return b.Build();
            }
            case Int16Array a:
            {
                var b = new Int32Array.Builder();
                b.Reserve(a.Length);
                for (int i = 0; i < a.Length; i++)
                {
                    if (a.IsNull(i)) b.AppendNull();
                    else b.Append(a.GetValue(i)!.Value);
                }
                return b.Build();
            }
            case UInt16Array a:
            {
                var b = new Int32Array.Builder();
                b.Reserve(a.Length);
                for (int i = 0; i < a.Length; i++)
                {
                    if (a.IsNull(i)) b.AppendNull();
                    else b.Append(a.GetValue(i)!.Value);
                }
                return b.Build();
            }
            default:
                return array;
        }
    }

    /// <summary>
    /// Creates a copy of an FLBA array with each value's bytes reversed (little-endian → big-endian).
    /// Used for decimal FLBA columns where Arrow stores little-endian but Parquet requires big-endian.
    /// </summary>
    private static IArrowArray ReverseFlbaDecimalBytes(IArrowArray array, int typeLength)
    {
        var data = array.Data;
        var sourceValues = data.Buffers[1].Span;
        int count = array.Length;

        var reversed = new byte[count * typeLength];
        for (int i = 0; i < count; i++)
        {
            var src = sourceValues.Slice(i * typeLength, typeLength);
            var dst = reversed.AsSpan(i * typeLength, typeLength);
            for (int j = 0; j < typeLength; j++)
                dst[j] = src[typeLength - 1 - j];
        }

        // Build new ArrayData with reversed value buffer, preserving null bitmap
        var buffers = new ArrowBuffer[]
        {
            data.Buffers[0], // null bitmap (unchanged)
            new ArrowBuffer(reversed)
        };
        var newData = new Apache.Arrow.ArrayData(
            data.DataType, count, data.NullCount, data.Offset, buffers);
        return Apache.Arrow.ArrowArrayFactory.BuildArray(newData);
    }

    /// <summary>Minimum bit width to represent values 0..maxValue.</summary>
    private static int BitWidth(int maxValue) =>
#if NET8_0_OR_GREATER
        maxValue <= 0 ? 0 : (32 - int.LeadingZeroCount(maxValue));
#else
        maxValue <= 0 ? 0 : (32 - BitPolyfills.LeadingZeroCount((uint)maxValue));
#endif

    /// <summary>
    /// Counts the number of rows in a page by counting entries where repLevel &lt; maxRepLevel
    /// (i.e., rep level 0 starts a new row at the top level).
    /// </summary>
    private static int CountRows(int[]? repLevels, int offset, int numValues, int maxRepLevel)
    {
        if (repLevels == null) return numValues;
        int rows = 0;
        for (int i = offset; i < offset + numValues; i++)
        {
            if (repLevels[i] == 0)
                rows++;
        }
        return rows;
    }

    /// <summary>Computes CRC-32C of a contiguous span.</summary>
    private static uint ComputeCrc32C(ReadOnlySpan<byte> data)
    {
        var crc = new System.IO.Hashing.Crc32();
        crc.Append(data);
        return CrcFinish(crc);
    }

    /// <summary>Extracts the uint32 hash value from a Crc32 instance.</summary>
    private static uint CrcFinish(System.IO.Hashing.Crc32 crc)
    {
        Span<byte> hash = stackalloc byte[4];
        crc.GetHashAndReset(hash);
        return BinaryPrimitives.ReadUInt32LittleEndian(hash);
    }
}

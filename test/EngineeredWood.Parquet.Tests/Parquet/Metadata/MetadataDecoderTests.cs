// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Compression;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet.Metadata;

public class MetadataDecoderTests
{
    [Fact]
    public void DecodeAlltypesPlain_BasicFields()
    {
        var footerBytes = TestData.ReadFooterBytes("alltypes_plain.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);

        Assert.Equal(1, metadata.Version);
        Assert.Equal(8, metadata.NumRows);
        Assert.Single(metadata.RowGroups);
        Assert.NotNull(metadata.CreatedBy);
        Assert.Contains("impala", metadata.CreatedBy, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void DecodeAlltypesPlain_SchemaElements()
    {
        var footerBytes = TestData.ReadFooterBytes("alltypes_plain.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);

        // First element is the root "schema" group
        Assert.Equal("schema", metadata.Schema[0].Name);
        Assert.NotNull(metadata.Schema[0].NumChildren);
        Assert.True(metadata.Schema[0].NumChildren > 0);

        // Schema should contain leaf columns
        Assert.True(metadata.Schema.Count > 1);

        // Collect leaf column names (elements with a physical type)
        var leafNames = metadata.Schema
            .Where(e => e.Type.HasValue)
            .Select(e => e.Name)
            .ToList();

        Assert.Contains("id", leafNames);
        Assert.Contains("bool_col", leafNames);
        Assert.Contains("tinyint_col", leafNames);
        Assert.Contains("smallint_col", leafNames);
        Assert.Contains("int_col", leafNames);
        Assert.Contains("bigint_col", leafNames);
        Assert.Contains("float_col", leafNames);
        Assert.Contains("double_col", leafNames);
        Assert.Contains("date_string_col", leafNames);
        Assert.Contains("string_col", leafNames);
        Assert.Contains("timestamp_col", leafNames);
    }

    [Fact]
    public void DecodeAlltypesPlain_PhysicalTypes()
    {
        var footerBytes = TestData.ReadFooterBytes("alltypes_plain.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);

        var schemaByName = metadata.Schema.ToDictionary(e => e.Name);

        Assert.Equal(PhysicalType.Int32, schemaByName["id"].Type);
        Assert.Equal(PhysicalType.Boolean, schemaByName["bool_col"].Type);
        Assert.Equal(PhysicalType.Int32, schemaByName["tinyint_col"].Type);
        Assert.Equal(PhysicalType.Int32, schemaByName["smallint_col"].Type);
        Assert.Equal(PhysicalType.Int32, schemaByName["int_col"].Type);
        Assert.Equal(PhysicalType.Int64, schemaByName["bigint_col"].Type);
        Assert.Equal(PhysicalType.Float, schemaByName["float_col"].Type);
        Assert.Equal(PhysicalType.Double, schemaByName["double_col"].Type);
        Assert.Equal(PhysicalType.ByteArray, schemaByName["date_string_col"].Type);
        Assert.Equal(PhysicalType.ByteArray, schemaByName["string_col"].Type);
        Assert.Equal(PhysicalType.Int96, schemaByName["timestamp_col"].Type);
    }

    [Fact]
    public void DecodeAlltypesPlain_RowGroupColumns()
    {
        var footerBytes = TestData.ReadFooterBytes("alltypes_plain.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);

        var rowGroup = metadata.RowGroups[0];
        Assert.Equal(8, rowGroup.NumRows);

        // Should have one column chunk per leaf column
        var leafCount = metadata.Schema.Count(e => e.Type.HasValue);
        Assert.Equal(leafCount, rowGroup.Columns.Count);

        // Each column chunk should have metadata
        foreach (var col in rowGroup.Columns)
        {
            Assert.NotNull(col.MetaData);
            Assert.True(col.MetaData!.NumValues > 0);
            Assert.True(col.MetaData.TotalCompressedSize > 0);
            Assert.True(col.MetaData.TotalUncompressedSize > 0);
        }
    }

    [Fact]
    public void DecodeAlltypesPlain_ColumnPaths()
    {
        var footerBytes = TestData.ReadFooterBytes("alltypes_plain.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);

        var columnPaths = metadata.RowGroups[0].Columns
            .Select(c => string.Join(".", c.MetaData!.PathInSchema!))
            .ToList();

        Assert.Contains("id", columnPaths);
        Assert.Contains("bool_col", columnPaths);
        Assert.Contains("string_col", columnPaths);
    }

    [Fact]
    public void DecodeAlltypesDictionary_HasDictionaryEncoding()
    {
        var footerBytes = TestData.ReadFooterBytes("alltypes_dictionary.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);

        // At least some columns should use dictionary encoding
        var hasDictEncoding = metadata.RowGroups[0].Columns
            .Any(c => c.MetaData!.Encodings.Contains(Encoding.PlainDictionary) ||
                       c.MetaData!.Encodings.Contains(Encoding.RleDictionary));
        Assert.True(hasDictEncoding);
    }

    [Fact]
    public void DecodeAlltypesSnappy_HasSnappyCompression()
    {
        var footerBytes = TestData.ReadFooterBytes("alltypes_plain.snappy.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);

        Assert.All(metadata.RowGroups[0].Columns,
            c => Assert.Equal(CompressionCodec.Snappy, c.MetaData!.Codec));
    }

    [Fact]
    public void DecodeDatapageV2_ParsesSuccessfully()
    {
        var footerBytes = TestData.ReadFooterBytes("datapage_v2.snappy.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);

        Assert.True(metadata.NumRows > 0);
        Assert.NotEmpty(metadata.RowGroups);
        Assert.NotEmpty(metadata.Schema);
    }

    [Fact]
    public void DecodeByteArrayDecimal_HasDecimalType()
    {
        var footerBytes = TestData.ReadFooterBytes("byte_array_decimal.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);

        // Should have at least one column with DECIMAL converted type
        var hasDecimal = metadata.Schema.Any(e => e.ConvertedType == ConvertedType.Decimal);
        Assert.True(hasDecimal);
    }

    [Fact]
    public void DecodeNestedLists_ParsesSuccessfully()
    {
        var footerBytes = TestData.ReadFooterBytes("nested_lists.snappy.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);

        Assert.True(metadata.NumRows > 0);
        // Nested schemas have group nodes with children
        var groupNodes = metadata.Schema.Where(e => e.NumChildren is > 0).ToList();
        Assert.True(groupNodes.Count >= 2, "Expected multiple group nodes for nested lists");
    }

    [Fact]
    public void DecodeNestedMaps_ParsesSuccessfully()
    {
        var footerBytes = TestData.ReadFooterBytes("nested_maps.snappy.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);

        Assert.True(metadata.NumRows > 0);
        var hasMapType = metadata.Schema.Any(e => e.ConvertedType == ConvertedType.Map ||
                                                   e.ConvertedType == ConvertedType.MapKeyValue);
        Assert.True(hasMapType);
    }

    [Fact]
    public void DecodeColumnChunkKeyValueMetadata_ParsesSuccessfully()
    {
        var footerBytes = TestData.ReadFooterBytes("column_chunk_key_value_metadata.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);

        // This file legitimately has 0 rows; just verify it parses
        Assert.NotEmpty(metadata.Schema);
        Assert.NotEmpty(metadata.RowGroups);
    }

    [Fact]
    public void DecodeAlltypesTinyPages_Statistics()
    {
        // alltypes_tiny_pages.parquet is a newer file that includes statistics
        var footerBytes = TestData.ReadFooterBytes("alltypes_tiny_pages.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);

        var hasStats = metadata.RowGroups[0].Columns.Any(c => c.MetaData?.Statistics != null);
        Assert.True(hasStats);
    }

    [Fact]
    public void AllTestFiles_ParseWithoutError()
    {
        var files = TestData.GetAllParquetFiles().ToList();
        Assert.NotEmpty(files);

        var failures = new List<string>();
        foreach (var file in files)
        {
            var fileName = Path.GetFileName(file);
            // Skip encrypted files
            if (fileName.Contains("encrypted", StringComparison.OrdinalIgnoreCase))
                continue;

            try
            {
                var footerBytes = TestData.ReadFooterBytes(fileName);
                MetadataDecoder.DecodeFileMetaData(footerBytes);
            }
            catch (Exception ex)
            {
                failures.Add($"{fileName}: {ex.Message}");
            }
        }

        Assert.True(failures.Count == 0,
            $"Failed to parse {failures.Count} file(s):\n" + string.Join("\n", failures));
    }

    /// <summary>
    /// The footer's lists size an array from their count before reading an element, so a count the
    /// footer's bytes cannot hold must be refused rather than allocate: here, int.MaxValue row
    /// groups in a 7-byte footer, and likewise for each list in <see cref="FooterListFields"/>.
    /// </summary>
    [Theory]
    [MemberData(nameof(FooterListFields))]
    public void AFooterListClaimingMoreEntriesThanItsBytes_IsRefusedBeforeAllocating(string field, byte[] bytes)
    {
        _ = field;
#if NET
        long before = GC.GetAllocatedBytesForCurrentThread();
#endif
        var ex = Assert.Throws<ParquetFormatException>(() => MetadataDecoder.DecodeFileMetaData(bytes));
        Assert.Contains("claims 2147483647 elements", ex.Message);
#if NET
        Assert.True(GC.GetAllocatedBytesForCurrentThread() - before < 1_000_000, "the refusal allocated as if the count were real");
#endif
    }

    // A FileMetaData field in short form (delta, LIST), then a large-list header of STRUCT (0xFC)
    // or BINARY (0xF8) claiming int.MaxValue elements, and nothing after it.
    public static TheoryData<string, byte[]> FooterListFields() => new()
    {
        { "schema", [0x29, 0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0x07] },
        { "row_groups", [0x49, 0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0x07] },
        { "key_value_metadata", [0x59, 0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0x07] },
        { "column_orders", [0x79, 0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0x07] },
    };

    /// <summary>
    /// A footer list of an unexpected element type is skipped element by element. A bool element is
    /// a byte of its own, unlike a bool field, so the skip must consume it or the fields after the
    /// list are misread. (Two bools 0x01 0x02 would happen to re-parse as a harmless field header,
    /// hiding the bug; three 0x01 bytes do not.)
    /// </summary>
    [Fact]
    public void AFooterListOfUnexpectedBools_IsSkippedWithoutMisaligningTheFooter()
    {
        byte[] bytes =
        [
            0x15, 0x02,                         // 1: version = 1
            0x19, 0x1C, 0x48, 0x01, 0x73, 0x00, // 2: schema = [{ name = "s" }]
            0x16, 0x00,                         // 3: num_rows = 0
            0x19, 0x0C,                         // 4: row_groups = []
            0x39, 0x31, 0x01, 0x01, 0x01,       // 7: column_orders as list<bool> [true, true, true], not structs
            0x08, 0x0C, 0x02, 0x61, 0x62,       // 6: created_by = "ab" (long-form field header)
            0x00,
        ];

        var metadata = MetadataDecoder.DecodeFileMetaData(bytes);

        Assert.Equal(1, metadata.Version);
        Assert.Equal(0, metadata.NumRows);
        Assert.Equal("s", Assert.Single(metadata.Schema).Name);
        Assert.Empty(metadata.RowGroups);
        Assert.Equal(3, metadata.ColumnOrders!.Count);
        Assert.Equal("ab", metadata.CreatedBy);
    }
}

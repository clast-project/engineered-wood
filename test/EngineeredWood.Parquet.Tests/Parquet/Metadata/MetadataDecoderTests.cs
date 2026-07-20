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
}

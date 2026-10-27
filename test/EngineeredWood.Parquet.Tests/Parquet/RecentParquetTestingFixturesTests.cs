// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet;

public sealed class RecentParquetTestingFixturesTests
{
    public static TheoryData<string> MalformedVariantFiles => new()
    {
        "variants/duplicate_field_offsets.parquet",
        "variants/field_id_out_of_range.parquet",
        "variants/int_overflow_in_bounds_check.parquet",
        "variants/malformed_child_inside_well_formed_parent.parquet",
        "variants/negative_dictionary_size.parquet",
        "variants/out_of_range_child_offset.parquet",
        "variants/out_of_range_dictionary_size.parquet",
        "variants/out_of_range_element_count.parquet",
        "variants/over_deep_nested_children.parquet",
        "variants/oversized_primitive_size.parquet",
        "variants/short_string_length_exceeds_buffer.parquet",
        "variants/truncated_primitive_size.parquet",
        "variants/unknown_primitive_type.parquet",
        "variants/variant_version_2_header.parquet",
    };

    [Fact]
    public async Task JsonLogicalType_ReadsDocumentsAndNull()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("json.parquet"));
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);
        var values = Assert.IsType<StringArray>(batch.Column(0));

        Assert.Equal(4, values.Length);
        Assert.Equal("{\"a\":1}", values.GetString(0));
        Assert.Equal("{\"a\":1,\"b\":null}", values.GetString(1));
        Assert.Equal("[1,null,3]", values.GetString(2));
        Assert.True(values.IsNull(3));
    }

    [Fact]
    public async Task BsonLogicalType_ReadsDocumentsAndNull()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("bson.parquet"));
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);
        var values = Assert.IsType<BinaryArray>(batch.Column(0));

        Assert.Equal(3, values.Length);
        Assert.Equal(
            new byte[] { 0x0c, 0, 0, 0, 0x10, 0x61, 0, 1, 0, 0, 0, 0 },
            values.GetBytes(0).ToArray());
        Assert.Equal(
            new byte[] { 0x0f, 0, 0, 0, 0x10, 0x61, 0, 1, 0, 0, 0, 0x0a, 0x62, 0, 0 },
            values.GetBytes(1).ToArray());
        Assert.True(values.IsNull(2));
    }

    /// <summary>
    /// UUID on INT32 is not a valid combination; parquet-format 2.14 (GH-606) says to ignore the
    /// annotation and read the physical type. This used to build a 16-byte fixed-size binary array
    /// over the column's 4-byte values.
    /// </summary>
    [Fact]
    public async Task UuidOnInt32_IgnoresTheAnnotationAndReadsInt32()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("int32_with_uuid_logical_type.parquet"));
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);
        var values = Assert.IsType<Int32Array>(batch.Column(0));

        Assert.Equal(Enumerable.Range(0, 10).Select(i => (int?)i), values.ToList());

        // The footer keeps the annotation as written; only the reader's view drops it.
        var metadata = await reader.ReadMetadataAsync();
        Assert.IsType<LogicalType.UuidType>(metadata.Schema[1].LogicalType);
        var schema = await reader.GetSchemaAsync();
        Assert.Null(schema.Columns[0].SchemaElement.LogicalType);

        // Its min/max were ordered by the annotation, so they are not used (the order is ignored too).
        Assert.NotNull(metadata.RowGroups[0].Columns[0].MetaData!.Statistics?.MinValue);
        var accessor = new ParquetStatisticsAccessor(schema);
        Assert.Null(accessor.GetMinValue(metadata.RowGroups[0], "int32_uuid"));
        Assert.Null(accessor.GetMaxValue(metadata.RowGroups[0], "int32_uuid"));
    }

    /// <summary>
    /// An unrecognized logical type (here LogicalType union field 2555, standing in for one added after
    /// this reader) is ignored and the column read as its physical type, BYTE_ARRAY — as binary, not as a
    /// string, since there is nothing to say the bytes are UTF-8.
    /// </summary>
    [Fact]
    public async Task UnrecognizedLogicalType_ReadsThePhysicalType()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("unknown-logical-type.parquet"));
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        var known = Assert.IsType<StringArray>(batch.Column(0));
        Assert.Equal("known string 1", known.GetString(0));
        var unknown = Assert.IsType<BinaryArray>(batch.Column(1));
        Assert.Equal(
            ["unknown string 1", "unknown string 2", "unknown string 3"],
            Enumerable.Range(0, 3).Select(i => System.Text.Encoding.UTF8.GetString(unknown.GetBytes(i).ToArray())));
    }

    [Fact]
    public async Task Int96TimestampOrder_IsUnknownAndValuesReadAsTimestamps()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("int96_timestamp_order.parquet"));
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var metadata = await reader.ReadMetadataAsync();
        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal([ColumnOrder.Undefined], metadata.ColumnOrders);
        var values = Assert.IsType<TimestampArray>(batch.Column(0));
        var type = Assert.IsType<TimestampType>(values.Data.DataType);
        Assert.Equal(Apache.Arrow.Types.TimeUnit.Microsecond, type.Unit);
        Assert.Null(type.Timezone);
        Assert.Equal(
            [86_399_999_999L, 86_400_000_000L, -50_803_200_000_000L, 1L],
            values.Values.ToArray());
    }

    [Theory]
    [MemberData(nameof(MalformedVariantFiles))]
    public async Task MalformedVariantStorage_ReadsAsBoundedBinaryColumns(string relativePath)
    {
        await using var file = new LocalRandomAccessFile(TestData.GetBadDataPath(relativePath));
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);
        var metadata = Assert.IsType<BinaryArray>(batch.Column("metadata"));
        var encodedValues = Assert.IsType<BinaryArray>(batch.Column("value"));

        Assert.Equal(batch.Length, metadata.Length);
        Assert.Equal(batch.Length, encodedValues.Length);
        for (int i = 0; i < batch.Length; i++)
        {
            Assert.InRange(metadata.GetBytes(i).Length, 1, 4_096);
            Assert.InRange(encodedValues.GetBytes(i).Length, 1, 4_096);
        }
    }
}

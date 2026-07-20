// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Compression;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet.Metadata;

public class MetadataEncoderTests
{
    [Fact]
    public void RoundTrip_MinimalFileMetaData()
    {
        var original = new FileMetaData
        {
            Version = 2,
            Schema =
            [
                new SchemaElement
                {
                    Name = "schema",
                    NumChildren = 1,
                },
                new SchemaElement
                {
                    Name = "id",
                    Type = PhysicalType.Int32,
                    RepetitionType = FieldRepetitionType.Required,
                },
            ],
            NumRows = 100,
            RowGroups =
            [
                new RowGroup
                {
                    Columns =
                    [
                        new ColumnChunk
                        {
                            FileOffset = 4,
                            MetaData = new ColumnMetaData
                            {
                                Type = PhysicalType.Int32,
                                Encodings = [Encoding.Plain],
                                PathInSchema = ["id"],
                                Codec = CompressionCodec.Uncompressed,
                                NumValues = 100,
                                TotalUncompressedSize = 400,
                                TotalCompressedSize = 400,
                                DataPageOffset = 4,
                            },
                        },
                    ],
                    TotalByteSize = 400,
                    NumRows = 100,
                },
            ],
            CreatedBy = "EngineeredWood",
        };

        byte[] encoded = MetadataEncoder.EncodeFileMetaData(original);
        var decoded = MetadataDecoder.DecodeFileMetaData(encoded);

        Assert.Equal(original.Version, decoded.Version);
        Assert.Equal(original.NumRows, decoded.NumRows);
        Assert.Equal(original.CreatedBy, decoded.CreatedBy);
        Assert.Equal(original.Schema.Count, decoded.Schema.Count);
        Assert.Equal(original.RowGroups.Count, decoded.RowGroups.Count);

        // Verify schema
        Assert.Equal("schema", decoded.Schema[0].Name);
        Assert.Equal(1, decoded.Schema[0].NumChildren);
        Assert.Null(decoded.Schema[0].Type);

        Assert.Equal("id", decoded.Schema[1].Name);
        Assert.Equal(PhysicalType.Int32, decoded.Schema[1].Type);
        Assert.Equal(FieldRepetitionType.Required, decoded.Schema[1].RepetitionType);

        // Verify row group
        var rg = decoded.RowGroups[0];
        Assert.Equal(100, rg.NumRows);
        Assert.Equal(400, rg.TotalByteSize);
        Assert.Single(rg.Columns);

        var col = rg.Columns[0].MetaData!;
        Assert.Equal(PhysicalType.Int32, col.Type);
        Assert.Equal(CompressionCodec.Uncompressed, col.Codec);
        Assert.Equal(100, col.NumValues);
        Assert.Equal(400, col.TotalUncompressedSize);
        Assert.Equal(400, col.TotalCompressedSize);
        Assert.Equal(4, col.DataPageOffset);
        Assert.Equal(["id"], col.PathInSchema);
        Assert.Equal([Encoding.Plain], col.Encodings);
    }

    [Fact]
    public void RoundTrip_WithAllOptionalFields()
    {
        var original = new FileMetaData
        {
            Version = 2,
            Schema =
            [
                new SchemaElement { Name = "schema", NumChildren = 2 },
                new SchemaElement
                {
                    Name = "name",
                    Type = PhysicalType.ByteArray,
                    RepetitionType = FieldRepetitionType.Optional,
                    LogicalType = new LogicalType.StringType(),
                    ConvertedType = ConvertedType.Utf8,
                },
                new SchemaElement
                {
                    Name = "amount",
                    Type = PhysicalType.Int64,
                    RepetitionType = FieldRepetitionType.Required,
                    LogicalType = new LogicalType.DecimalType(2, 10),
                    ConvertedType = ConvertedType.Decimal,
                    Scale = 2,
                    Precision = 10,
                    FieldId = 42,
                },
            ],
            NumRows = 1000,
            RowGroups =
            [
                new RowGroup
                {
                    Columns =
                    [
                        new ColumnChunk
                        {
                            FileOffset = 100,
                            MetaData = new ColumnMetaData
                            {
                                Type = PhysicalType.ByteArray,
                                Encodings = [Encoding.Plain, Encoding.RleDictionary],
                                PathInSchema = ["name"],
                                Codec = CompressionCodec.Snappy,
                                NumValues = 1000,
                                TotalUncompressedSize = 5000,
                                TotalCompressedSize = 3000,
                                DataPageOffset = 200,
                                DictionaryPageOffset = 100,
                                Statistics = new Statistics
                                {
                                    NullCount = 50,
                                    MinValue = [0x41], // "A"
                                    MaxValue = [0x5A], // "Z"
                                    IsMinValueExact = true,
                                    IsMaxValueExact = true,
                                },
                            },
                        },
                        new ColumnChunk
                        {
                            FileOffset = 3100,
                            MetaData = new ColumnMetaData
                            {
                                Type = PhysicalType.Int64,
                                Encodings = [Encoding.Plain],
                                PathInSchema = ["amount"],
                                Codec = CompressionCodec.Snappy,
                                NumValues = 1000,
                                TotalUncompressedSize = 8000,
                                TotalCompressedSize = 6000,
                                DataPageOffset = 3100,
                            },
                        },
                    ],
                    TotalByteSize = 13000,
                    NumRows = 1000,
                    TotalCompressedSize = 9000,
                    Ordinal = 0,
                    SortingColumns = [new SortingColumn(1, true, false)],
                },
            ],
            KeyValueMetadata = [new KeyValue("key1", "value1"), new KeyValue("key2", null)],
            CreatedBy = "EngineeredWood test",
        };

        byte[] encoded = MetadataEncoder.EncodeFileMetaData(original);
        var decoded = MetadataDecoder.DecodeFileMetaData(encoded);

        // Verify logical types
        Assert.IsType<LogicalType.StringType>(decoded.Schema[1].LogicalType);
        Assert.Equal(ConvertedType.Utf8, decoded.Schema[1].ConvertedType);

        var decimalType = Assert.IsType<LogicalType.DecimalType>(decoded.Schema[2].LogicalType);
        Assert.Equal(2, decimalType.Scale);
        Assert.Equal(10, decimalType.Precision);
        Assert.Equal(42, decoded.Schema[2].FieldId);

        // Verify statistics
        var stats = decoded.RowGroups[0].Columns[0].MetaData!.Statistics!;
        Assert.Equal(50, stats.NullCount);
        Assert.Equal([0x41], stats.MinValue);
        Assert.Equal([0x5A], stats.MaxValue);
        Assert.True(stats.IsMinValueExact);
        Assert.True(stats.IsMaxValueExact);

        // Verify optional row group fields
        var rg = decoded.RowGroups[0];
        Assert.Equal(9000, rg.TotalCompressedSize);
        Assert.Equal((short)0, rg.Ordinal);
        Assert.NotNull(rg.SortingColumns);
        Assert.Single(rg.SortingColumns);
        Assert.Equal(1, rg.SortingColumns[0].ColumnIndex);
        Assert.True(rg.SortingColumns[0].Descending);
        Assert.False(rg.SortingColumns[0].NullsFirst);

        // Verify key-value metadata
        Assert.NotNull(decoded.KeyValueMetadata);
        Assert.Equal(2, decoded.KeyValueMetadata.Count);
        Assert.Equal("key1", decoded.KeyValueMetadata[0].Key);
        Assert.Equal("value1", decoded.KeyValueMetadata[0].Value);
        Assert.Equal("key2", decoded.KeyValueMetadata[1].Key);
        Assert.Null(decoded.KeyValueMetadata[1].Value);

        // Verify dictionary page offset
        Assert.Equal(100, decoded.RowGroups[0].Columns[0].MetaData!.DictionaryPageOffset);
    }

    [Fact]
    public void RoundTrip_NanCountAndColumnOrders()
    {
        var original = new FileMetaData
        {
            Version = 2,
            Schema =
            [
                new SchemaElement { Name = "schema", NumChildren = 2 },
                new SchemaElement
                {
                    Name = "id",
                    Type = PhysicalType.Int32,
                    RepetitionType = FieldRepetitionType.Required,
                },
                new SchemaElement
                {
                    Name = "measure",
                    Type = PhysicalType.Double,
                    RepetitionType = FieldRepetitionType.Required,
                },
            ],
            NumRows = 10,
            RowGroups =
            [
                new RowGroup
                {
                    Columns =
                    [
                        new ColumnChunk
                        {
                            FileOffset = 4,
                            MetaData = new ColumnMetaData
                            {
                                Type = PhysicalType.Double,
                                Encodings = [Encoding.Plain],
                                PathInSchema = ["measure"],
                                Codec = CompressionCodec.Uncompressed,
                                NumValues = 10,
                                TotalUncompressedSize = 80,
                                TotalCompressedSize = 80,
                                DataPageOffset = 4,
                                Statistics = new Statistics
                                {
                                    NullCount = 0,
                                    MinValue = BitConverter.GetBytes(-1.5),
                                    MaxValue = BitConverter.GetBytes(9.0),
                                    NanCount = 3,
                                },
                            },
                        },
                    ],
                    TotalByteSize = 80,
                    NumRows = 10,
                },
            ],
            ColumnOrders = [ColumnOrder.TypeDefined, ColumnOrder.Ieee754TotalOrder],
            CreatedBy = "EngineeredWood",
        };

        byte[] encoded = MetadataEncoder.EncodeFileMetaData(original);
        var decoded = MetadataDecoder.DecodeFileMetaData(encoded);

        var stats = decoded.RowGroups[0].Columns[0].MetaData!.Statistics!;
        Assert.Equal(3, stats.NanCount);
        Assert.Equal(BitConverter.GetBytes(-1.5), stats.MinValue);

        Assert.NotNull(decoded.ColumnOrders);
        Assert.Equal(
            [ColumnOrder.TypeDefined, ColumnOrder.Ieee754TotalOrder],
            decoded.ColumnOrders);
    }

    [Fact]
    public void RoundTrip_LogicalTypes_Time()
    {
        var original = new FileMetaData
        {
            Version = 2,
            Schema =
            [
                new SchemaElement { Name = "schema", NumChildren = 1 },
                new SchemaElement
                {
                    Name = "ts",
                    Type = PhysicalType.Int64,
                    RepetitionType = FieldRepetitionType.Required,
                    LogicalType = new LogicalType.TimestampType(true, TimeUnit.Micros),
                },
            ],
            NumRows = 0,
            RowGroups = [],
        };

        byte[] encoded = MetadataEncoder.EncodeFileMetaData(original);
        var decoded = MetadataDecoder.DecodeFileMetaData(encoded);

        var ts = Assert.IsType<LogicalType.TimestampType>(decoded.Schema[1].LogicalType);
        Assert.True(ts.IsAdjustedToUtc);
        Assert.Equal(TimeUnit.Micros, ts.Unit);
    }

    [Fact]
    public void RoundTrip_LogicalTypes_Int()
    {
        var original = new FileMetaData
        {
            Version = 2,
            Schema =
            [
                new SchemaElement { Name = "schema", NumChildren = 1 },
                new SchemaElement
                {
                    Name = "age",
                    Type = PhysicalType.Int32,
                    RepetitionType = FieldRepetitionType.Required,
                    LogicalType = new LogicalType.IntType(8, false),
                },
            ],
            NumRows = 0,
            RowGroups = [],
        };

        byte[] encoded = MetadataEncoder.EncodeFileMetaData(original);
        var decoded = MetadataDecoder.DecodeFileMetaData(encoded);

        var intType = Assert.IsType<LogicalType.IntType>(decoded.Schema[1].LogicalType);
        Assert.Equal(8, intType.BitWidth);
        Assert.False(intType.IsSigned);
    }

    [Theory]
    [InlineData(TimeUnit.Millis)]
    [InlineData(TimeUnit.Micros)]
    [InlineData(TimeUnit.Nanos)]
    public void RoundTrip_LogicalTypes_TimeUnits(TimeUnit unit)
    {
        var original = new FileMetaData
        {
            Version = 2,
            Schema =
            [
                new SchemaElement { Name = "schema", NumChildren = 1 },
                new SchemaElement
                {
                    Name = "t",
                    Type = PhysicalType.Int32,
                    RepetitionType = FieldRepetitionType.Required,
                    LogicalType = new LogicalType.TimeType(false, unit),
                },
            ],
            NumRows = 0,
            RowGroups = [],
        };

        byte[] encoded = MetadataEncoder.EncodeFileMetaData(original);
        var decoded = MetadataDecoder.DecodeFileMetaData(encoded);

        var time = Assert.IsType<LogicalType.TimeType>(decoded.Schema[1].LogicalType);
        Assert.False(time.IsAdjustedToUtc);
        Assert.Equal(unit, time.Unit);
    }

    [Fact]
    public void RoundTrip_RealParquetFooter()
    {
        // Decode a real footer, re-encode it, decode again, and compare
        var footerBytes = TestData.ReadFooterBytes("alltypes_plain.parquet");
        var original = MetadataDecoder.DecodeFileMetaData(footerBytes);

        byte[] reEncoded = MetadataEncoder.EncodeFileMetaData(original);
        var decoded = MetadataDecoder.DecodeFileMetaData(reEncoded);

        Assert.Equal(original.Version, decoded.Version);
        Assert.Equal(original.NumRows, decoded.NumRows);
        Assert.Equal(original.Schema.Count, decoded.Schema.Count);
        Assert.Equal(original.RowGroups.Count, decoded.RowGroups.Count);

        for (int i = 0; i < original.Schema.Count; i++)
        {
            Assert.Equal(original.Schema[i].Name, decoded.Schema[i].Name);
            Assert.Equal(original.Schema[i].Type, decoded.Schema[i].Type);
            Assert.Equal(original.Schema[i].RepetitionType, decoded.Schema[i].RepetitionType);
            Assert.Equal(original.Schema[i].NumChildren, decoded.Schema[i].NumChildren);
        }

        for (int rg = 0; rg < original.RowGroups.Count; rg++)
        {
            Assert.Equal(original.RowGroups[rg].NumRows, decoded.RowGroups[rg].NumRows);
            Assert.Equal(original.RowGroups[rg].Columns.Count, decoded.RowGroups[rg].Columns.Count);

            for (int c = 0; c < original.RowGroups[rg].Columns.Count; c++)
            {
                var origMeta = original.RowGroups[rg].Columns[c].MetaData!;
                var decMeta = decoded.RowGroups[rg].Columns[c].MetaData!;

                Assert.Equal(origMeta.Type, decMeta.Type);
                Assert.Equal(origMeta.Codec, decMeta.Codec);
                Assert.Equal(origMeta.NumValues, decMeta.NumValues);
                Assert.Equal(origMeta.DataPageOffset, decMeta.DataPageOffset);
                Assert.Equal(origMeta.TotalCompressedSize, decMeta.TotalCompressedSize);
                Assert.Equal(origMeta.TotalUncompressedSize, decMeta.TotalUncompressedSize);
                Assert.Equal(origMeta.PathInSchema, decMeta.PathInSchema);
            }
        }
    }
}

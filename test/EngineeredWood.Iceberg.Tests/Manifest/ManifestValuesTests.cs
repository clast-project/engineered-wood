// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Iceberg.Expressions;
using EngineeredWood.Iceberg.Manifest;

namespace EngineeredWood.Iceberg.Tests.Manifest;

/// <summary>Which column bounds planning gets from a manifest.</summary>
public class ManifestValuesTests
{
    private static readonly Schema TableSchema = new(0,
    [
        new NestedField(1, "id", IcebergType.Long, true),
        new NestedField(2, "key", IcebergType.Uuid, false),
        new NestedField(3, "point", new StructType([new NestedField(4, "x", IcebergType.Double, false)]), false),
        new NestedField(5, "tags", new ListType(6, IcebergType.String, false), false),
    ]);

    private static byte[] UuidBytes(string text) => IcebergAvroTypes.GuidToBigEndian(Guid.Parse(text));

    [Fact]
    public void BoundColumnTypes_HoldStructLeavesButNotUuids()
    {
        var types = ManifestValues.BoundColumnTypes(TableSchema);
        Assert.Equal([1, 4], types.Keys.OrderBy(k => k));
        Assert.Equal(IcebergType.Double, types[4]);
    }

    [Fact]
    public void UuidBounds_AreNotUsedForPlanning()
    {
        // Iceberg Java orders UUIDs by signed 64-bit halves: from its point of view this range holds
        // 00000000-0000-0000-0000-000000000000 (the low half 0 lies between 0x8000... and 0x0000...0001's
        // signed values), while Guid ordering would put it below the lower bound.
        var file = new DataFile
        {
            FilePath = "a.parquet", RecordCount = 1, FileSizeInBytes = 1,
            LowerBounds = new Dictionary<int, byte[]> { [2] = UuidBytes("00000000-0000-0000-8000-000000000000"), [1] = new byte[8] },
            UpperBounds = new Dictionary<int, byte[]> { [2] = UuidBytes("00000000-0000-0001-0000-000000000000"), [1] = new byte[8] },
        };

        var decoded = ManifestValues.WithDecodedBounds(file, ManifestValues.BoundColumnTypes(TableSchema));
        Assert.False(decoded.ColumnLowerBounds!.ContainsKey(2));
        Assert.False(decoded.ColumnUpperBounds!.ContainsKey(2));
        Assert.True(decoded.ColumnLowerBounds.ContainsKey(1));

        var metadata = TableMetadata.Create(TableSchema, location: Path.GetTempPath());
        var filter = EngineeredWood.Iceberg.Expressions.Expressions.Equal(
            new UnboundReference("key"), new LiteralExpression(LiteralValue.Of(Guid.Empty)));
        var planned = new TableScan(metadata, new LocalTableFileSystem(Path.GetTempPath())).Filter(filter).PlanFiles([decoded]);
        Assert.Single(planned.DataFiles);
    }
}

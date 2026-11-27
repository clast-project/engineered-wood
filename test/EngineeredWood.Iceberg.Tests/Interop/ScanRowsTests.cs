// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg.Tests.Interop;

/// <summary>The logical spelling the reader tiers compare column types in. Needs no reader.</summary>
public class ScanRowsTests
{
    [Theory]
    [InlineData("string", "string")]
    [InlineData("large_string", "string")]
    [InlineData("utf8", "string")]
    [InlineData("int64", "int64")]
    [InlineData("int32", "int32")]
    [InlineData("double", "float64")]
    [InlineData("float64", "float64")]
    [InlineData("date32[day]", "date")]
    [InlineData("timestamp[us, tz=UTC]", "timestamptz[us]")]
    [InlineData("timestamp[us, tz=America/Los_Angeles]", "timestamptz[us]")]
    [InlineData("timestamp[ns, tz=UTC]", "timestamptz[ns]")]
    [InlineData("timestamp[us]", "timestamp[us]")]
    [InlineData("decimal128(10, 2)", "decimal(10, 2)")]
    [InlineData("decimal(10, 2)", "decimal(10, 2)")]
    [InlineData("fixed_size_binary[16]", "fixed[16]")]
    public void ArrowTypesHaveOneLogicalSpelling(string arrowType, string expected) =>
        Assert.Equal(expected, ScanRows.LogicalType(arrowType));

    [Fact]
    public void ATimestampsUnitAndZoneAreKept()
    {
        Assert.NotEqual(ScanRows.LogicalType("timestamp[us]"), ScanRows.LogicalType("timestamp[us, tz=UTC]"));
        Assert.NotEqual(ScanRows.LogicalType("timestamp[us, tz=UTC]"), ScanRows.LogicalType("timestamp[ns, tz=UTC]"));
    }

    [Fact]
    public void AnUnmappedTypeFails() =>
        Assert.Throws<Xunit.Sdk.XunitException>(() => ScanRows.LogicalType("list<item: int64>"));
}

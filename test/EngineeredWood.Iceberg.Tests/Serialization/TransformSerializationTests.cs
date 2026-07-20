// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Iceberg.Serialization;

namespace EngineeredWood.Iceberg.Tests.Serialization;

public class TransformSerializationTests
{
    [Theory]
    [InlineData("identity")]
    [InlineData("year")]
    [InlineData("month")]
    [InlineData("day")]
    [InlineData("hour")]
    [InlineData("void")]
    public void SimpleTransform_RoundTrips(string transformString)
    {
        var json = $"\"{transformString}\"";
        var transform = IcebergJsonSerializer.Deserialize<Transform>(json);
        var serialized = IcebergJsonSerializer.Serialize(transform);
        Assert.Equal(json, serialized);
    }

    [Fact]
    public void BucketTransform_RoundTrips()
    {
        var transform = Transform.Bucket(16);
        var json = IcebergJsonSerializer.Serialize(transform);
        Assert.Equal("\"bucket[16]\"", json);

        var deserialized = IcebergJsonSerializer.Deserialize<Transform>(json);
        Assert.Equal(transform, deserialized);
    }

    [Fact]
    public void TruncateTransform_RoundTrips()
    {
        var transform = Transform.Truncate(10);
        var json = IcebergJsonSerializer.Serialize(transform);
        Assert.Equal("\"truncate[10]\"", json);

        var deserialized = IcebergJsonSerializer.Deserialize<Transform>(json);
        Assert.Equal(transform, deserialized);
    }
}

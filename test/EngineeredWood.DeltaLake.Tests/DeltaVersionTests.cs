// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Tests;

public class DeltaVersionTests
{
    [Theory]
    [InlineData(0, "00000000000000000000")]
    [InlineData(1, "00000000000000000001")]
    [InlineData(42, "00000000000000000042")]
    [InlineData(99999, "00000000000000099999")]
    public void Format_ProducesZeroPaddedString(long version, string expected)
    {
        Assert.Equal(expected, DeltaVersion.Format(version));
    }

    [Fact]
    public void CommitPath_ProducesCorrectPath()
    {
        Assert.Equal("_delta_log/00000000000000000005.json",
            DeltaVersion.CommitPath(5));
    }

    [Fact]
    public void CheckpointPath_ProducesCorrectPath()
    {
        Assert.Equal("_delta_log/00000000000000000010.checkpoint.parquet",
            DeltaVersion.CheckpointPath(10));
    }

    [Theory]
    [InlineData("00000000000000000000.json", true, 0)]
    [InlineData("00000000000000000005.json", true, 5)]
    [InlineData("00000000000000099999.json", true, 99999)]
    [InlineData("abc.json", false, -1)]
    [InlineData("00000000000000000005.checkpoint.parquet", false, -1)]
    [InlineData("00000000000000000005.txt", false, -1)]
    public void TryParseCommitVersion(string fileName, bool expected, long expectedVersion)
    {
        bool result = DeltaVersion.TryParseCommitVersion(fileName, out long version);
        Assert.Equal(expected, result);
        if (expected)
            Assert.Equal(expectedVersion, version);
    }

    [Theory]
    [InlineData("00000000000000000010.checkpoint.parquet", true, 10)]
    [InlineData("00000000000000000010.checkpoint.0000000001.0000000003.parquet", true, 10)]
    [InlineData("00000000000000000010.json", false, -1)]
    public void TryParseCheckpointVersion(string fileName, bool expected, long expectedVersion)
    {
        bool result = DeltaVersion.TryParseCheckpointVersion(fileName, out long version);
        Assert.Equal(expected, result);
        if (expected)
            Assert.Equal(expectedVersion, version);
    }
}

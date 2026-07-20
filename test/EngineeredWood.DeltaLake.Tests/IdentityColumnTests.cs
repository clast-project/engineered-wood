// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Tests;

public class IdentityColumnTests
{
    [Fact]
    public void GetConfig_ReturnsConfig()
    {
        var field = new StructField
        {
            Name = "id",
            Type = new PrimitiveType { TypeName = "long" },
            Nullable = false,
            Metadata = IdentityColumn.CreateMetadata(start: 1, step: 1),
        };

        var config = IdentityColumn.GetConfig(field);
        Assert.NotNull(config);
        Assert.Equal(1L, config!.Start);
        Assert.Equal(1L, config.Step);
        Assert.Null(config.HighWaterMark);
        Assert.False(config.AllowExplicitInsert);
    }

    [Fact]
    public void GetConfig_WithHighWaterMark()
    {
        var field = new StructField
        {
            Name = "id",
            Type = new PrimitiveType { TypeName = "long" },
            Nullable = false,
            Metadata = new Dictionary<string, string>
            {
                { IdentityColumn.StartKey, "1" },
                { IdentityColumn.StepKey, "2" },
                { IdentityColumn.HighWaterMarkKey, "99" },
                { IdentityColumn.AllowExplicitInsertKey, "true" },
            },
        };

        var config = IdentityColumn.GetConfig(field);
        Assert.NotNull(config);
        Assert.Equal(1L, config!.Start);
        Assert.Equal(2L, config.Step);
        Assert.Equal(99L, config.HighWaterMark);
        Assert.True(config.AllowExplicitInsert);
    }

    [Fact]
    public void GetConfig_NonIdentityField_ReturnsNull()
    {
        var field = new StructField
        {
            Name = "name",
            Type = new PrimitiveType { TypeName = "string" },
            Nullable = true,
        };

        Assert.Null(IdentityColumn.GetConfig(field));
    }

    [Fact]
    public void GenerateValues_FirstGeneration()
    {
        var config = new IdentityColumnConfig
        {
            Start = 1, Step = 1, AllowExplicitInsert = false,
        };

        var (values, hwm) = IdentityColumn.GenerateValues(config, 3);

        Assert.Equal(new long[] { 1, 2, 3 }, values);
        Assert.Equal(3L, hwm);
    }

    [Fact]
    public void GenerateValues_WithExistingHighWaterMark()
    {
        var config = new IdentityColumnConfig
        {
            Start = 1, Step = 1, HighWaterMark = 10, AllowExplicitInsert = false,
        };

        var (values, hwm) = IdentityColumn.GenerateValues(config, 3);

        Assert.Equal(new long[] { 11, 12, 13 }, values);
        Assert.Equal(13L, hwm);
    }

    [Fact]
    public void GenerateValues_CustomStep()
    {
        var config = new IdentityColumnConfig
        {
            Start = 10, Step = 5, AllowExplicitInsert = false,
        };

        var (values, hwm) = IdentityColumn.GenerateValues(config, 4);

        Assert.Equal(new long[] { 10, 15, 20, 25 }, values);
        Assert.Equal(25L, hwm);
    }

    [Fact]
    public void GenerateValues_NegativeStep()
    {
        var config = new IdentityColumnConfig
        {
            Start = -1, Step = -1, AllowExplicitInsert = false,
        };

        var (values, hwm) = IdentityColumn.GenerateValues(config, 3);

        Assert.Equal(new long[] { -1, -2, -3 }, values);
        Assert.Equal(-3L, hwm);
    }

    [Fact]
    public void GenerateValues_ZeroCount()
    {
        var config = new IdentityColumnConfig
        {
            Start = 1, Step = 1, HighWaterMark = 5, AllowExplicitInsert = false,
        };

        var (values, hwm) = IdentityColumn.GenerateValues(config, 0);
        Assert.Empty(values);
        Assert.Equal(5L, hwm);
    }

    [Fact]
    public void UpdateHighWaterMark_CreatesNewField()
    {
        var field = new StructField
        {
            Name = "id",
            Type = new PrimitiveType { TypeName = "long" },
            Nullable = false,
            Metadata = IdentityColumn.CreateMetadata(start: 1, step: 1),
        };

        var updated = IdentityColumn.UpdateHighWaterMark(field, 42);

        Assert.Equal("42", updated.Metadata![IdentityColumn.HighWaterMarkKey]);
        Assert.Equal("1", updated.Metadata[IdentityColumn.StartKey]); // Preserved
    }

    [Fact]
    public void CreateMetadata_StepZero_Throws()
    {
        Assert.Throws<ArgumentException>(() => IdentityColumn.CreateMetadata(step: 0));
    }

    [Fact]
    public void IsIdentityColumn_True()
    {
        var field = new StructField
        {
            Name = "id",
            Type = new PrimitiveType { TypeName = "long" },
            Nullable = false,
            Metadata = IdentityColumn.CreateMetadata(),
        };

        Assert.True(IdentityColumn.IsIdentityColumn(field));
    }

    [Fact]
    public void IsIdentityColumn_False()
    {
        var field = new StructField
        {
            Name = "name",
            Type = new PrimitiveType { TypeName = "string" },
            Nullable = true,
        };

        Assert.False(IdentityColumn.IsIdentityColumn(field));
    }
}

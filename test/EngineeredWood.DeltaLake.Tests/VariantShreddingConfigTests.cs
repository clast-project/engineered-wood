// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Operations.Shredding;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>The log-layer half of the variant shredding table features (#671).</summary>
public class VariantShreddingConfigTests
{
    private static ProtocolAction Features(params string[] features) => new()
    {
        MinReaderVersion = 3,
        MinWriterVersion = 7,
        ReaderFeatures = features,
        WriterFeatures = features,
    };

    [Theory]
    [InlineData("variantShredding")]
    [InlineData("variantShredding-preview")]
    public void EitherName_IsReadableAndWritable(string feature)
    {
        var protocol = Features("variantType", feature);
        ProtocolVersions.ValidateReadSupport(protocol);
        ProtocolVersions.ValidateWriteSupport(protocol);
        ProtocolVersions.ValidateVacuumSupport(protocol);
    }

    [Theory]
    [InlineData("variantShredding", "true", true)]
    [InlineData("variantShredding-preview", "TRUE", true)]
    [InlineData("variantShredding", "false", false)]
    [InlineData("variantType", "true", false)]   // the property without the feature
    public void AllowsShreddedWrites_NeedsTheFeatureAndTheProperty(string feature, string value, bool expected)
    {
        var configuration = new Dictionary<string, string> { [VariantShreddingConfig.EnableKey] = value };
        Assert.Equal(expected, VariantShreddingConfig.AllowsShreddedWrites(Features(feature), configuration));
    }

    [Fact]
    public void AllowsShreddedWrites_FeatureWithoutTheProperty_IsFalse()
    {
        Assert.False(VariantShreddingConfig.AllowsShreddedWrites(Features("variantShredding"), configuration: null));
    }

    [Fact]
    public void CheckpointOptions_NeverShred()
    {
        var options = ParquetWriteOptions.Default with
        {
            ShredVariants = ShredOptions.Default,
            VariantShredSchemas = new Dictionary<string, ShredSchema>
            {
                ["v"] = ShredSchema.Primitive(ShredType.Int32),
            },
        };

        var narrowed = CheckpointParquetOptions.For(options);

        Assert.Null(narrowed.ShredVariants);
        Assert.Null(narrowed.VariantShredSchemas);
    }
}

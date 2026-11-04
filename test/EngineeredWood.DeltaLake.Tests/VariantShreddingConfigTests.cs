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
        Assert.Equal(expected, VariantShreddingConfig.AllowsShreddedWrites(Features("variantType", feature), configuration));
    }

    private static readonly Dictionary<string, string> Enabled = new() { [VariantShreddingConfig.EnableKey] = "true" };

    [Fact]
    public void AllowsShreddedWrites_GaNameWithoutVariantType_IsFalse()
    {
        // variantShredding depends on variantType; a protocol missing it is one a strict reader rejects.
        Assert.False(VariantShreddingConfig.AllowsShreddedWrites(Features("variantShredding"), Enabled));
    }

    [Fact]
    public void AllowsShreddedWrites_GaNameWithVariantTypeOnOneSideOnly_IsFalse()
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 3,
            MinWriterVersion = 7,
            ReaderFeatures = ["variantShredding"],
            WriterFeatures = ["variantType", "variantShredding"],
        };

        Assert.False(VariantShreddingConfig.AllowsShreddedWrites(protocol, Enabled));
    }

    [Fact]
    public void AllowsShreddedWrites_PreviewNameWithoutVariantType_IsTrue()
    {
        // The preview name has no dependency (delta-spark 4.0's VariantShreddingPreviewTableFeature).
        Assert.True(VariantShreddingConfig.AllowsShreddedWrites(Features("variantShredding-preview"), Enabled));
    }

    [Theory]
    [InlineData(2, 7)]
    [InlineData(3, 6)]
    public void AllowsShreddedWrites_BelowTheTableFeaturesVersions_IsFalse(int reader, int writer)
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = reader,
            MinWriterVersion = writer,
            ReaderFeatures = ["variantType", "variantShredding"],
            WriterFeatures = ["variantType", "variantShredding"],
        };

        Assert.False(VariantShreddingConfig.AllowsShreddedWrites(protocol, Enabled));
    }

    [Fact]
    public void AllowsShreddedWrites_WriterSideOnly_IsFalse()
    {
        // A reader-writer feature named only among the writer features has not told readers to expect shredding.
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 3,
            MinWriterVersion = 7,
            ReaderFeatures = ["variantType"],
            WriterFeatures = ["variantType", "variantShredding"],
        };
        var configuration = new Dictionary<string, string> { [VariantShreddingConfig.EnableKey] = "true" };

        Assert.False(VariantShreddingConfig.AllowsShreddedWrites(protocol, configuration));
    }

    [Fact]
    public void AllowsShreddedWrites_DifferentNamesOnEachSide_IsFalse()
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 3,
            MinWriterVersion = 7,
            ReaderFeatures = ["variantType", "variantShredding-preview"],
            WriterFeatures = ["variantType", "variantShredding"],
        };
        var configuration = new Dictionary<string, string> { [VariantShreddingConfig.EnableKey] = "true" };

        Assert.False(VariantShreddingConfig.AllowsShreddedWrites(protocol, configuration));
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

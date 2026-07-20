// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;

namespace EngineeredWood.DeltaLake.Tests;

public class ProtocolVersionTests
{
    [Theory]
    [InlineData(1, 2)]
    [InlineData(2, 5)]
    [InlineData(3, 7)]
    public void ValidateReadSupport_SupportedVersions_Succeeds(int readerVersion, int writerVersion)
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = readerVersion,
            MinWriterVersion = writerVersion,
        };

        ProtocolVersions.ValidateReadSupport(protocol);
    }

    [Fact]
    public void ValidateReadSupport_V4_Throws()
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 4,
            MinWriterVersion = 7,
        };

        Assert.Throws<DeltaFormatException>(
            () => ProtocolVersions.ValidateReadSupport(protocol));
    }

    [Fact]
    public void ValidateReadSupport_SupportedReaderFeatures_Succeeds()
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 3,
            MinWriterVersion = 7,
            ReaderFeatures = ["deletionVectors", "columnMapping"],
        };

        // Both features are supported
        ProtocolVersions.ValidateReadSupport(protocol);
    }

    [Fact]
    public void ValidateReadSupport_UnsupportedReaderFeature_Throws()
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 3,
            MinWriterVersion = 7,
            ReaderFeatures = ["deletionVectors", "liquidClustering"],
        };

        var ex = Assert.Throws<DeltaFormatException>(
            () => ProtocolVersions.ValidateReadSupport(protocol));
        Assert.Contains("liquidClustering", ex.Message);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(7)]
    public void ValidateWriteSupport_SupportedVersions_Succeeds(int writerVersion)
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 1,
            MinWriterVersion = writerVersion,
        };

        ProtocolVersions.ValidateWriteSupport(protocol);
    }

    [Fact]
    public void ValidateWriteSupport_V8_Throws()
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 1,
            MinWriterVersion = 8,
        };

        Assert.Throws<DeltaFormatException>(
            () => ProtocolVersions.ValidateWriteSupport(protocol));
    }

    [Fact]
    public void ValidateWriteSupport_SupportedWriterFeatures_Succeeds()
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 3,
            MinWriterVersion = 7,
            WriterFeatures = ["columnMapping", "deletionVectors"],
        };

        ProtocolVersions.ValidateWriteSupport(protocol);
    }

    [Fact]
    public void ValidateWriteSupport_UnsupportedWriterFeature_Throws()
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 1,
            MinWriterVersion = 7,
            // A feature this writer does not understand — generatedColumns is now supported (listed +
            // enforced via HonorWriterFeatures), so use a genuinely unknown feature here.
            WriterFeatures = ["someUnsupportedFeature"],
        };

        var ex = Assert.Throws<DeltaFormatException>(
            () => ProtocolVersions.ValidateWriteSupport(protocol));
        Assert.Contains("someUnsupportedFeature", ex.Message);
    }
}

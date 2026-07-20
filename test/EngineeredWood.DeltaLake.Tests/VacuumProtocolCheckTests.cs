// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;

namespace EngineeredWood.DeltaLake.Tests;

public class VacuumProtocolCheckTests
{
    [Fact]
    public void ValidateVacuumSupport_NoFeatures_Succeeds()
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 1,
            MinWriterVersion = 2,
        };

        // No vacuumProtocolCheck → no extra validation needed
        ProtocolVersions.ValidateVacuumSupport(protocol);
    }

    [Fact]
    public void ValidateVacuumSupport_WithCheck_AllFeaturesSupported_Succeeds()
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 3,
            MinWriterVersion = 7,
            ReaderFeatures = ["deletionVectors", "vacuumProtocolCheck"],
            WriterFeatures = ["deletionVectors", "vacuumProtocolCheck"],
        };

        ProtocolVersions.ValidateVacuumSupport(protocol);
    }

    [Fact]
    public void ValidateVacuumSupport_WithCheck_UnsupportedWriterFeature_Throws()
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 3,
            MinWriterVersion = 7,
            ReaderFeatures = ["vacuumProtocolCheck"],
            WriterFeatures = ["vacuumProtocolCheck", "liquidClustering"],
        };

        // vacuumProtocolCheck requires ALL features to be understood
        var ex = Assert.Throws<DeltaFormatException>(
            () => ProtocolVersions.ValidateVacuumSupport(protocol));
        Assert.Contains("liquidClustering", ex.Message);
    }

    [Fact]
    public void ValidateVacuumSupport_WithoutCheck_UnsupportedWriterFeature_Succeeds()
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 1,
            MinWriterVersion = 7,
            // No vacuumProtocolCheck → only reader validation
            WriterFeatures = ["liquidClustering"],
        };

        // Without vacuumProtocolCheck, vacuum only checks reader support
        ProtocolVersions.ValidateVacuumSupport(protocol);
    }

    [Fact]
    public void ValidateVacuumSupport_WithCheck_ManyFeatures_Succeeds()
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 3,
            MinWriterVersion = 7,
            ReaderFeatures = [
                "columnMapping", "deletionVectors", "timestampNtz",
                "typeWidening", "v2Checkpoint", "vacuumProtocolCheck",
            ],
            WriterFeatures = [
                "columnMapping", "deletionVectors", "domainMetadata",
                "identityColumns", "inCommitTimestamp", "rowTracking",
                "timestampNtz", "typeWidening", "v2Checkpoint", "vacuumProtocolCheck",
            ],
        };

        ProtocolVersions.ValidateVacuumSupport(protocol);
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;

namespace EngineeredWood.DeltaLake.Tests;

public class DomainMetadataValidationTests
{
    [Theory]
    [InlineData("delta.columnMapping", true)]
    [InlineData("delta.rowTracking", true)]
    [InlineData("delta.", true)]
    [InlineData("myApp.config", false)]
    [InlineData("userDomain", false)]
    [InlineData("", false)]
    public void IsSystemDomain(string domain, bool expected)
    {
        Assert.Equal(expected, DomainMetadataValidation.IsSystemDomain(domain));
    }

    [Fact]
    public void ValidateUserModification_UserDomain_Succeeds()
    {
        // User domains can be freely modified
        DomainMetadataValidation.ValidateUserModification("myApp.config");
        DomainMetadataValidation.ValidateUserModification("analytics.settings");
        DomainMetadataValidation.ValidateUserModification("customDomain");
    }

    [Fact]
    public void ValidateUserModification_SystemDomain_Throws()
    {
        // Unknown system domains cannot be modified
        var ex = Assert.Throws<DeltaFormatException>(
            () => DomainMetadataValidation.ValidateUserModification("delta.rowTracking"));
        Assert.Contains("system-controlled", ex.Message);
        Assert.Contains("delta.rowTracking", ex.Message);
    }

    [Fact]
    public void ValidateUserModification_SystemDomainPrefix_Throws()
    {
        Assert.Throws<DeltaFormatException>(
            () => DomainMetadataValidation.ValidateUserModification("delta.unknownFeature"));
    }
}

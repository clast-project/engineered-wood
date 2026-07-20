// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Actions;

/// <summary>
/// Validation rules for domain metadata per the Delta Lake protocol.
/// </summary>
public static class DomainMetadataValidation
{
    /// <summary>Prefix for system-controlled metadata domains.</summary>
    public const string SystemDomainPrefix = "delta.";

    /// <summary>
    /// System domains that this implementation understands and may modify.
    /// All other <c>delta.*</c> domains must be preserved as-is.
    /// </summary>
    private static readonly HashSet<string> KnownSystemDomains = new(StringComparer.Ordinal)
    {
        // Add known system domains here as they are implemented
    };

    /// <summary>
    /// Returns true if the domain name identifies a system-controlled domain.
    /// </summary>
    public static bool IsSystemDomain(string domain) =>
        domain.StartsWith(SystemDomainPrefix, StringComparison.Ordinal);

    /// <summary>
    /// Returns true if the domain is a system domain that this implementation
    /// understands and may modify.
    /// </summary>
    public static bool IsKnownSystemDomain(string domain) =>
        KnownSystemDomains.Contains(domain);

    /// <summary>
    /// Validates that a domain metadata modification is allowed.
    /// Users may freely set/remove user domains but must not modify
    /// system domains (<c>delta.*</c>) that this implementation does not understand.
    /// </summary>
    public static void ValidateUserModification(string domain)
    {
        if (IsSystemDomain(domain) && !IsKnownSystemDomain(domain))
        {
            throw new DeltaFormatException(
                $"Cannot modify system-controlled domain '{domain}'. " +
                $"System domains (starting with '{SystemDomainPrefix}') can only be " +
                $"modified by implementations that understand them.");
        }
    }
}

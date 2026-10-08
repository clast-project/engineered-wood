// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Schema;

/// <summary>
/// UniForm: the <c>delta.universalFormat.enabledFormats</c> table property, which promises readers of another table
/// format (Iceberg, Hudi) metadata that tracks the Delta table. A Delta writer keeps that promise by converting after
/// each commit (Spark's converter is a post-commit step). This library has no converter.
/// </summary>
/// <remarks>
/// UniForm is no table feature of its own. Iceberg conversion rides on <c>icebergCompatV1</c>/<c>V2</c>, which
/// this library can write, so the protocol alone never stops a write to a UniForm table: the property has to be
/// read for that.
/// </remarks>
public static class UniversalFormat
{
    /// <summary>The table property naming the formats the table is converted to.</summary>
    public const string EnabledFormatsKey = "delta.universalFormat.enabledFormats";

    /// <summary>
    /// The formats <paramref name="configuration"/> enables; empty when it enables none.
    /// </summary>
    /// <remarks>
    /// Parsed as delta-spark parses it (<c>DeltaConfigs.UNIVERSAL_FORMAT_ENABLED_FORMATS</c>, read from the jar): an
    /// empty value is none, anything else is split on commas the way Java splits, dropping trailing empty entries
    /// (so <c>","</c> is none too). Spark then accepts only distinct entries from
    /// <c>iceberg</c> and <c>hudi</c>, with no trimming and in that case. This method does not validate: an entry
    /// Spark would refuse is still returned, because the table still claims something no one here maintains.
    /// </remarks>
    public static IReadOnlyList<string> GetEnabledFormats(IReadOnlyDictionary<string, string>? configuration)
    {
        if (configuration is null
            || !configuration.TryGetValue(EnabledFormatsKey, out string? value)
            || value.Length == 0)
        {
            return [];
        }

        // Java's String.split, which Spark calls, drops TRAILING empty strings (and only those): "iceberg," is
        // [iceberg], and "," is no format at all. .NET's Split keeps them.
        string[] parts = value.Split(',');
        int count = parts.Length;
        while (count > 0 && parts[count - 1].Length == 0)
            count--;
        return count == parts.Length ? parts : parts.Take(count).ToArray();
    }
}

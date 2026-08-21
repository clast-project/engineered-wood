// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Compression;

namespace EngineeredWood.Parquet.Bridge;

/// <summary>
/// Maps Parquity's provider-neutral writer profiles onto <see cref="ParquetWriteOptions"/>.
/// </summary>
/// <remarks>
/// A profile is declared only when the option behind it demonstrably takes effect. Declaring one
/// the writer cannot honor would record an effective option that never happened, which is worse
/// than the UNSUPPORTED that an absent profile reports.
/// </remarks>
public static class WriterProfiles
{
    /// <summary>
    /// The profiles this bridge declares, each with the exact options Parquity should record.
    /// </summary>
    public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, object>> Supported { get; } =
        new Dictionary<string, IReadOnlyDictionary<string, object>>(StringComparer.Ordinal)
        {
            ["compression-gzip"] = new Dictionary<string, object>(StringComparer.Ordinal)
            {
                ["Compression"] = "Gzip",
            },
            ["compression-brotli"] = new Dictionary<string, object>(StringComparer.Ordinal)
            {
                ["Compression"] = "Brotli",
            },
            ["row-group-2"] = new Dictionary<string, object>(StringComparer.Ordinal)
            {
                ["RowGroupMaxRows"] = 2,
            },
            ["min-max-statistics-off"] = new Dictionary<string, object>(StringComparer.Ordinal)
            {
                ["WriteStatistics"] = false,
            },
        };

    /// <summary>Builds the write options for a profile, or the defaults when none is selected.</summary>
    /// <param name="profile">The Parquity profile name, or null for a default write.</param>
    public static ParquetWriteOptions Apply(string? profile) => profile switch
    {
        null => ParquetWriteOptions.Default,
        "compression-gzip" => ParquetWriteOptions.Default with { Compression = CompressionCodec.Gzip },
        "compression-brotli" => ParquetWriteOptions.Default with { Compression = CompressionCodec.Brotli },
        "row-group-2" => ParquetWriteOptions.Default with { RowGroupMaxRows = 2 },
        // Suppresses the whole Statistics struct, null count included, which is what PyArrow's
        // write_statistics=False and parquet-cpp's disable_statistics do. Parquity's contract only
        // checks that min and max are absent, so it is satisfied either way.
        "min-max-statistics-off" => ParquetWriteOptions.Default with { WriteStatistics = false },
        _ => throw new BridgeRequestException($"undeclared writer profile: {profile}"),
    };
}

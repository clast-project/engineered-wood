// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;

namespace EngineeredWood.DeltaLake;

/// <summary>
/// Utilities for Delta Lake version numbering.
/// Versions are zero-padded 20-digit numbers in file names.
/// </summary>
public static class DeltaVersion
{
    private const int VersionDigits = 20;

    /// <summary>
    /// Formats a version number as a 20-digit zero-padded string.
    /// </summary>
    public static string Format(long version) =>
        version.ToString($"D{VersionDigits}", CultureInfo.InvariantCulture);

    /// <summary>
    /// Gets the commit file path for a given version within the <c>_delta_log</c> directory.
    /// </summary>
    public static string CommitPath(long version) =>
        $"_delta_log/{Format(version)}.json";

    /// <summary>
    /// Gets the classic checkpoint file path for a given version.
    /// </summary>
    public static string CheckpointPath(long version) =>
        $"_delta_log/{Format(version)}.checkpoint.parquet";

    /// <summary>
    /// Gets the multi-part checkpoint file path.
    /// </summary>
    public static string CheckpointPartPath(long version, int part, int totalParts) =>
        $"_delta_log/{Format(version)}.checkpoint.{part:D10}.{totalParts:D10}.parquet";

    /// <summary>
    /// Path to the <c>_last_checkpoint</c> file.
    /// </summary>
    public const string LastCheckpointPath = "_delta_log/_last_checkpoint";

    /// <summary>
    /// Path prefix for the <c>_delta_log</c> directory.
    /// </summary>
    public const string LogPrefix = "_delta_log/";

    /// <summary>
    /// Attempts to parse a version number from a commit file name
    /// (e.g., <c>00000000000000000005.json</c>).
    /// </summary>
    public static bool TryParseCommitVersion(string fileName, out long version)
    {
        version = -1;

        if (!fileName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
            return false;

        string stem = Path.GetFileNameWithoutExtension(fileName);
        return stem.Length == VersionDigits &&
               long.TryParse(stem, NumberStyles.None, CultureInfo.InvariantCulture, out version);
    }

    /// <summary>
    /// Gets the log compaction file path for a version range [startVersion, endVersion].
    /// </summary>
    public static string CompactedPath(long startVersion, long endVersion) =>
        $"_delta_log/{Format(startVersion)}.{Format(endVersion)}.compacted.json";

    /// <summary>
    /// Attempts to parse a compacted file name (e.g., <c>00000000000000000004.00000000000000000006.compacted.json</c>).
    /// </summary>
    public static bool TryParseCompactedRange(
        string fileName, out long startVersion, out long endVersion)
    {
        startVersion = -1;
        endVersion = -1;

        if (!fileName.EndsWith(".compacted.json", StringComparison.OrdinalIgnoreCase))
            return false;

        // Strip .compacted.json suffix
        string stem = fileName[..^".compacted.json".Length];
        if (stem.Contains('/') || stem.Contains('\\'))
            stem = Path.GetFileName(stem);

        // Split on dot: startVersion.endVersion
        int dotIdx = stem.IndexOf('.');
        if (dotIdx < 0)
            return false;

        string startStr = stem[..dotIdx];
        string endStr = stem[(dotIdx + 1)..];

        return startStr.Length == VersionDigits &&
               endStr.Length == VersionDigits &&
               long.TryParse(startStr, NumberStyles.None, CultureInfo.InvariantCulture, out startVersion) &&
               long.TryParse(endStr, NumberStyles.None, CultureInfo.InvariantCulture, out endVersion) &&
               endVersion > startVersion;
    }

    /// <summary>
    /// Attempts to parse a version number from a checkpoint file name.
    /// </summary>
    public static bool TryParseCheckpointVersion(string fileName, out long version)
    {
        version = -1;

        // Classic: 00000000000000000005.checkpoint.parquet
        int idx = fileName.IndexOf(".checkpoint.", StringComparison.OrdinalIgnoreCase);
        if (idx < 0)
            return false;

        string stem = fileName[..idx];
        if (stem.Contains('/') || stem.Contains('\\'))
            stem = Path.GetFileName(stem);

        return stem.Length == VersionDigits &&
               long.TryParse(stem, NumberStyles.None, CultureInfo.InvariantCulture, out version);
    }
}

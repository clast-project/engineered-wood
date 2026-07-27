// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Checkpoint;

/// <summary>
/// Which copies of the per-file statistics a checkpoint carries, from the table's
/// <c>delta.checkpoint.writeStatsAsJson</c> / <c>delta.checkpoint.writeStatsAsStruct</c> properties.
/// </summary>
/// <remarks>
/// Both default to true, matching delta-spark: a checkpoint then carries the JSON <c>add.stats</c>
/// string that every reader understands AND the typed <c>add.stats_parsed</c> struct. Turning JSON
/// off makes the typed struct the only source of statistics — supported here so the shape can be
/// produced and validated, but note that EW's own <see cref="CheckpointReader"/> reads only the JSON
/// string, so a table it writes that way reads back (in EW) with no statistics and no file skipping.
/// </remarks>
internal readonly record struct CheckpointStatsMode(bool WriteJson, bool WriteStruct)
{
    private const string WriteStatsAsJsonKey = "delta.checkpoint.writeStatsAsJson";
    private const string WriteStatsAsStructKey = "delta.checkpoint.writeStatsAsStruct";

    /// <summary>Reads the mode from a table's configuration; absent or unparseable means the default.</summary>
    public static CheckpointStatsMode FromConfiguration(IReadOnlyDictionary<string, string>? configuration) =>
        new(ReadFlag(configuration, WriteStatsAsJsonKey),
            ReadFlag(configuration, WriteStatsAsStructKey));

    private static bool ReadFlag(IReadOnlyDictionary<string, string>? configuration, string key) =>
        configuration is null
        || !configuration.TryGetValue(key, out var raw)
        || !bool.TryParse(raw, out bool value)
        || value;
}

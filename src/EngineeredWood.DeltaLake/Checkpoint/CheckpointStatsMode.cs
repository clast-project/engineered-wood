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
/// off makes the typed struct the only source of statistics, which
/// <see cref="CheckpointReader"/> does read — it builds a <see cref="CheckpointStatsView"/> over the
/// struct and points each <see cref="Actions.AddFile"/> at its row. Reach for those statistics
/// through <see cref="Actions.AddFile.GetStatsJson"/> or
/// <see cref="Actions.AddFile.GetNumRecords"/>, never <see cref="Actions.AddFile.Stats"/> directly:
/// the JSON string really is absent in this mode, and a caller reading it would conclude the file
/// has no statistics at all.
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

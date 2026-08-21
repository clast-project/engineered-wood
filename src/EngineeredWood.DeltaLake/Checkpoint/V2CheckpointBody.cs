// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Checkpoint;

/// <summary>
/// Which of the two file formats PROTOCOL.md defines for a UUID-named V2 checkpoint
/// (<c>n.checkpoint.u.{json/parquet}</c>) a <see cref="V2CheckpointWriter"/> produces.
/// </summary>
/// <remarks>
/// Both are spec-legal and carry the same actions; the choice is not derived from the table. delta-spark
/// makes it a session config (<c>CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT</c>) that defaults to JSON, and this
/// follows suit — see <see cref="Json"/> for why the default matters.
/// </remarks>
public enum V2CheckpointBody
{
    /// <summary>
    /// NDJSON, one action per line. The default, and the one to keep unless something forces otherwise:
    /// it is what delta-spark writes by default, so it is the form foreign readers are best exercised
    /// against, and a reader can take the whole body without a Parquet footer round-trip.
    /// </summary>
    Json = 0,

    /// <summary>
    /// Parquet, in the same struct-per-action schema as a classic checkpoint plus the
    /// <c>checkpointMetadata</c> and <c>sidecar</c> columns.
    /// </summary>
    Parquet = 1,
}

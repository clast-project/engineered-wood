// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Checkpoint;

/// <summary>
/// Which checkpoint spec a <see cref="CheckpointWriter"/> produces.
/// </summary>
public enum CheckpointFormat
{
    /// <summary>
    /// Take the table's word for it: a UUID-named V2 checkpoint when the table asks for one, and a
    /// classic V1 checkpoint otherwise. The default.
    /// </summary>
    /// <remarks>
    /// <para>"Asks for one" means the <c>delta.checkpointPolicy</c> table property is <c>v2</c> — the
    /// property delta-spark uses for exactly this decision — <b>and</b> the <c>v2Checkpoint</c> table
    /// feature is enabled, which PROTOCOL.md requires before a V2 checkpoint may be written at all.</para>
    ///
    /// <para>The feature being merely PRESENT is deliberately not enough. A table can support V2
    /// checkpoints while its policy still says classic, and delta-spark writes classic checkpoints for
    /// exactly that table — so treating the feature as the trigger would silently change a table's
    /// checkpoint form away from what its own configured engine produces. The property is the knob; the
    /// feature is the permission.</para>
    ///
    /// <para>A policy of <c>v2</c> on a table WITHOUT the feature is a contradiction the table itself
    /// carries, and it throws rather than quietly falling back — see
    /// <see cref="ProtocolVersions.SupportsV2Checkpoints"/>.</para>
    /// </remarks>
    Automatic = 0,

    /// <summary>
    /// Always a classic <c>&lt;version&gt;.checkpoint.parquet</c>, whatever the table says.
    /// </summary>
    /// <remarks>
    /// The conservative choice, and a defensible one to pin: a classic V1 checkpoint is the form every
    /// Delta reader handles, and <c>SelectLatestCheckpoint</c> prefers it on read for that reason. What
    /// V2 buys is the sidecar split, which matters on very large tables and not on most.
    /// </remarks>
    Classic = 1,

    /// <summary>
    /// Always a UUID-named V2 checkpoint. Throws if the table has not enabled the <c>v2Checkpoint</c>
    /// feature, since the spec permits the form only then.
    /// </summary>
    V2 = 2,
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Checkpoint;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>
/// Policy for a <see cref="LogCommitter"/>: how often it checkpoints, whether it gates on the writer
/// protocol, and how it reads statistics when deciding conflicts. Fixed for the lifetime of a committer —
/// everything that varies per commit lives on <see cref="LogCommitRequest"/>.
/// </summary>
public sealed record LogCommitOptions
{
    /// <summary>The defaults: checkpoint every 10 versions, write a version checksum beside every commit,
    /// validate the writer protocol, prefer typed stats.</summary>
    public static LogCommitOptions Default { get; } = new();

    /// <summary>
    /// Write a <c>_delta_log/&lt;version&gt;.crc</c> version checksum after each commit, summarising the
    /// table state at the version it lands on. Default: true.
    ///
    /// <para><b>Why on.</b> PROTOCOL.md says writers SHOULD produce one for each commit; delta-spark
    /// writes one by default; delta-kernel-rs documents calling <c>write_checksum()</c> on the
    /// post-commit snapshot after every successful commit. A table maintained by this library and another
    /// engine in turn otherwise carries checksums only for the other engine's versions, which is the
    /// state that makes divergence introduced across ours invisible.</para>
    ///
    /// <para><b>What it costs.</b> One extra create-if-absent request per commit, and one small JSON
    /// object per version in the log directory — collected by <see cref="LogCleanup"/> along with the
    /// commits it describes. Turn it off for a writer where that request is the thing being optimised,
    /// or where something else is producing checksums for this table.</para>
    ///
    /// <para>The file is advisory, so a failure to write one never fails the commit — see
    /// <see cref="VersionChecksumWriter"/>.</para>
    /// </summary>
    public bool WriteVersionChecksums { get; init; } = true;

    /// <summary>
    /// Write a checkpoint after every Nth version, or 0 to never checkpoint. A commit checkpoints when the
    /// version it LANDED on is a multiple of this — which after a rebase is not the version it first
    /// attempted — so the interval is honoured against the log's real numbering rather than the caller's
    /// intent. Individual commits can still opt out with
    /// <see cref="LogCommitRequest.WriteCheckpointOnInterval"/>.
    /// </summary>
    public int CheckpointInterval { get; init; } = 10;

    /// <summary>
    /// The checkpoint writer to use, or null to construct a default one over the log's filesystem. Supply
    /// one to control the checkpoint's parquet encoding (compression, statistics) — see
    /// <see cref="CheckpointParquetOptions"/>. Ignored when <see cref="CheckpointInterval"/> is 0.
    /// </summary>
    public CheckpointWriter? CheckpointWriter { get; init; }

    /// <summary>
    /// Refuse to commit to a table whose protocol declares writer features this library does not
    /// implement (<see cref="ProtocolVersions.ValidateWriteSupport"/>).
    ///
    /// <para>On by default, and the reason is that the gate has to travel with the COMMIT rather than with
    /// any one API above it. A writer that ignores an unknown writer feature does not merely write
    /// something suboptimal — it can silently violate an invariant the feature exists to maintain, and a
    /// checkpoint it later writes can drop the state that feature added, because unrecognised actions are
    /// skipped on read. Turn it off only if the caller performs the equivalent check itself.</para>
    ///
    /// <para>Checked once, against the base snapshot's protocol. That is sufficient across a rebase: a
    /// concurrent <c>protocol</c> action is an unconditional conflict, so a commit never rebases past
    /// one.</para>
    /// </summary>
    public bool ValidateWriteProtocol { get; init; } = true;

    /// <summary>
    /// Whether a checkpoint's typed <c>stats_parsed</c> columns win over its JSON <c>stats</c> string when
    /// the conflict checker prunes a concurrent add against a read predicate. Only reaches the default
    /// <see cref="DeltaFilePruner"/> the committer builds; a caller supplying
    /// <see cref="LogCommitRequest.Pruner"/> has already made this choice.
    /// </summary>
    public bool PreferTypedCheckpointStats { get; init; } = true;
}

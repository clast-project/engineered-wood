// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// One data file surviving <see cref="DeltaTable.PlanFiles"/>' prune, paired with its ordinal in the
/// snapshot's file-addressing domain.
/// </summary>
/// <param name="FileOrdinal">
/// The file's position in the snapshot's PATH-SORTED active set — the high bits of a
/// <see cref="TransientRowAddress"/> (<c>(fileOrdinal &lt;&lt; 40) | absolute-in-file position</c>), and the
/// key of the lower-layer primitives <see cref="DeltaTable.ComputeDeletionVectorActionsAsync"/>
/// (<c>positionsByOrdinal</c>), <see cref="DeltaTable.RebaseDvDmlActionsAsync"/>
/// (<c>newPositionsByOrdinal</c>) and <see cref="DeltaTable.CommitDataFilesAsync"/>
/// (<c>deletedPositionsByFileIndex</c>).
/// <para>
/// The row-level DML boundary is keyed by <see cref="File"/>'s PATH instead — see
/// <see cref="RowSelection"/> — precisely because an ordinal cannot be told stale. Ordinals are assigned
/// over the WHOLE active set BEFORE pruning, so a pruned file still consumes its ordinal and the returned
/// sequence is ascending but gapped. They are meaningful only against the snapshot they were planned from:
/// a concurrent append can insert a path anywhere in the sort order and renumber everything after it, so a
/// caller holding ordinals across a version change must re-plan.
/// </para>
/// </param>
/// <param name="File">The <c>add</c> action, carrying the path, partition values, statistics, deletion
/// vector, and row-tracking fields.</param>
public readonly record struct PlannedFile(
    int FileOrdinal,
    AddFile File);

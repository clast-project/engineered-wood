// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Everything a <see cref="DeltaTable.ReadAsync"/> call can vary: projection, pruning, which version, and
/// which per-row metadata columns to append. One options object rather than a method per combination —
/// <see cref="DeltaRowMetadata"/> is <see cref="FlagsAttribute"/>, so two metadata kinds cost one pass
/// instead of two reads of the table.
/// </summary>
public sealed record DeltaReadOptions
{
    /// <summary>
    /// Logical column names to project. Null reads all columns. The emitted columns follow the TABLE's
    /// field order, not the order named here, and an unknown name is ignored rather than erroring —
    /// <see cref="DeltaTable.GetReadSchema"/> reports exactly what will come back.
    /// </summary>
    public IReadOnlyList<string>? Columns { get; init; }

    /// <summary>
    /// Superset-safe file pruning by partition values and column statistics: a file is skipped only when its
    /// statistics PROVE no row can match. The reader does NOT re-apply this per row, so surviving batches
    /// still contain non-matching rows — filter them yourself.
    /// </summary>
    public Expressions.Predicate? Filter { get; init; }

    /// <summary>Which per-row metadata columns to append. Combinable; defaults to none.</summary>
    public DeltaRowMetadata Metadata { get; init; } = DeltaRowMetadata.None;

    /// <summary>
    /// Prefix for the <see cref="DeltaRowMetadata.Locator"/> and <see cref="DeltaRowMetadata.RowTracking"/>
    /// column names (see <see cref="DeltaMetadataColumns"/>). Change it when the table has columns of its
    /// own under the default — a collision is refused rather than shadowed, so there has to be somewhere to
    /// move them to. <see cref="DeltaRowMetadata.RowAddress"/> is not prefixed.
    /// </summary>
    public string MetadataPrefix { get; init; } = DeltaMetadataColumns.DefaultPrefix;

    /// <summary>
    /// Time travel: read this version instead of the latest. Null reads the current snapshot. Mutually
    /// exclusive with <see cref="Snapshot"/> — setting both throws rather than picking one.
    /// </summary>
    public long? AtVersion { get; init; }

    /// <summary>
    /// Read against a snapshot the caller already pinned — ordinarily
    /// <see cref="DeltaTransaction.Snapshot"/>, so that planning, reading, addressing and committing all name
    /// ONE version. Resolves with no I/O, unlike <see cref="AtVersion"/>.
    ///
    /// <para>This is what makes the host guide's "pin one version and use it everywhere" true at every step
    /// rather than at three of four. Without it a read inside a transaction silently follows
    /// <see cref="DeltaTable.CurrentSnapshot"/>, so the rows it returns — and any address minted from them —
    /// can come from a version the transaction is not validating against.</para>
    ///
    /// <para>Mutually exclusive with <see cref="AtVersion"/>; setting both throws.</para>
    /// </summary>
    public Snapshot.Snapshot? Snapshot { get; init; }
}

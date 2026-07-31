// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// The Change Data Feed's counterpart to <see cref="DeltaReadOptions"/>: which versions, which columns, and
/// whether each change row carries its stable identity.
/// </summary>
public sealed record DeltaChangeReadOptions
{
    /// <summary>First version of the range, inclusive.</summary>
    public long StartVersion { get; init; }

    /// <summary>Last version of the range, inclusive.</summary>
    public long EndVersion { get; init; }

    /// <summary>
    /// Logical column names to keep. Null keeps all of them. The feed's own <c>_change_type</c>,
    /// <c>_commit_version</c> and <c>_commit_timestamp</c> columns are always kept — they are what makes a
    /// change row a change rather than a row — as are any metadata columns <see cref="Metadata"/> asks for.
    ///
    /// <para>This is a projection of the emitted batches, NOT a pushdown: the underlying files are read in
    /// full. It shrinks what crosses the API boundary, not what is read from storage.</para>
    /// </summary>
    public IReadOnlyList<string>? Columns { get; init; }

    /// <summary>
    /// Which per-row metadata columns to append. Only <see cref="DeltaRowMetadata.RowTracking"/> is valid
    /// here; <see cref="DeltaRowMetadata.Locator"/> and <see cref="DeltaRowMetadata.RowAddress"/> throw.
    ///
    /// <para>Both name a row's position in the ACTIVE set, and a <c>_change_data</c> file is not in the
    /// active set — so neither address would mean anything a caller could use as a key. That was true by
    /// accident before (there was no way to ask); it is now true by declaration.</para>
    /// </summary>
    public DeltaRowMetadata Metadata { get; init; } = DeltaRowMetadata.None;

    /// <summary>Prefix for the <see cref="DeltaRowMetadata.RowTracking"/> column names. See
    /// <see cref="DeltaReadOptions.MetadataPrefix"/>.</summary>
    public string MetadataPrefix { get; init; } = DeltaMetadataColumns.DefaultPrefix;
}

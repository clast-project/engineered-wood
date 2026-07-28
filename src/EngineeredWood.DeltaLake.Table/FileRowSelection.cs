// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// A SELF-DESCRIBING set of rows to act on: for each data file — keyed by its log <c>add.path</c>
/// (the URL-ENCODED form, exactly as it appears in the snapshot) — the ABSOLUTE physical row positions
/// the caller selected. Positions are parquet row indexes that COUNT rows already masked by the file's
/// deletion vector (Spark's <c>_metadata.row_index</c> semantics), which is what makes repeated DV
/// deletes compose and what the DELETE machinery records in <c>DeleteDvEdit</c>.
/// </summary>
/// <remarks>
/// <para>
/// This is the preferred key for the row-level DML entry points. It replaces keying by a file's
/// PATH-SORTED ORDINAL within a snapshot's active set, which has two problems for an out-of-process
/// caller:
/// </para>
/// <list type="bullet">
///   <item><description>
///     The ordinal is only meaningful relative to a specific snapshot's active set, so the caller must
///     reproduce this library's sort rule EXACTLY, and both sides must agree about which snapshot they
///     mean. Nothing in the type system enforces either. A path needs no shared convention.
///   </description></item>
///   <item><description>
///     An ordinal that does not resolve is INDISTINGUISHABLE from one that resolves to a file with
///     nothing to delete — so the historical ordinal-keyed overloads SKIP it, and a caller whose
///     ordinals were computed against the wrong snapshot silently deletes nothing. A path that is not
///     active is recognisably wrong, so the path-keyed overloads THROW.
///   </description></item>
/// </list>
/// <para>
/// Produced either by an engine whose scan carried the file path per row, or by decoding a positional
/// row identifier against a snapshot (see the ordinal-keyed overloads, which do exactly that).
/// </para>
/// </remarks>
public sealed record FileRowSelection(
    IReadOnlyDictionary<string, IReadOnlyCollection<long>> RowsByFile);

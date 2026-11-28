// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Expressions;
using EngineeredWood.IO;

namespace EngineeredWood.Lance.Table.Indices;

/// <summary>
/// Predicate-driven fragment pruning using BTREE scalar indices.
///
/// <para>The walker recurses over the predicate tree and returns a "set
/// of fragments that may contain matching rows", or <c>null</c> when
/// the predicate can't be used to prune (e.g. references a non-indexed
/// column, uses an operator the index can't answer, or contains a NOT
/// branch we don't try to invert).</para>
///
/// <para>Recursion rules — designed to be conservative (always-correct
/// over-approximation):</para>
/// <list type="bullet">
///   <item><c>column op literal</c> on an indexed column with a
///   supported operator → ask every segment of the column's indices that
///   can answer, and return the fragments they hit plus every fragment
///   none of them covers (#991): a segment knows nothing about a fragment
///   outside its bitmap, such as one appended after it was built.</item>
///   <item><c>column IN (lits)</c> on an indexed column → the same, with
///   the union of the per-literal hits.</item>
///   <item><c>AND</c> → intersect children's non-null sets; null
///   children are ignored (a child with no info imposes no constraint).</item>
///   <item><c>OR</c> → union of children's sets, but ANY null child
///   poisons the result (we have no info about that branch's
///   fragments, so we must scan all).</item>
///   <item><c>NOT</c>, <c>IS [NOT] NULL</c>, function calls, anything
///   else → return null (no pruning).</item>
/// </list>
///
/// <para>This slice supports Int32 BTREE indices only; other value
/// types follow the same shape and can be added when fixtures exist.</para>
/// </summary>
internal static class IndexPruner
{
    /// <summary>
    /// Returns the set of fragment ids that may contain rows matching
    /// the filter, or <c>null</c> when no useful pruning is possible.
    /// </summary>
    /// <param name="filter">The read's filter.</param>
    /// <param name="indices">The dataset's index segments.</param>
    /// <param name="fragmentIds">Every fragment of the dataset version: those no segment covers are always kept.</param>
    /// <param name="fs">The dataset's file system.</param>
    /// <param name="cancellationToken">Cancels the index reads.</param>
    public static async ValueTask<HashSet<uint>?> ComputeCandidatesAsync(
        Predicate filter,
        IReadOnlyList<IndexInfo> indices,
        IReadOnlyCollection<uint> fragmentIds,
        ITableFileSystem fs,
        CancellationToken cancellationToken)
    {
        // Build column-path → segments lookup, by path rather than by name, so an index on a struct's
        // field never answers for a top-level column of the field's name (#771). Only single-column scalar
        // indices we know how to query (BTREE or BITMAP) get registered;
        // others fall through to "no info" so the caller scans every
        // fragment for that branch. A column may have several segments, of one index or of several; each
        // answers only for the fragments it covers.
        var byColumn = new Dictionary<IReadOnlyList<string>, List<IndexInfo>>(ColumnPathComparer.Ordinal);
        foreach (var info in indices)
        {
            if (info.ColumnPaths.Count != 1) continue;
            if (!IsBTree(info) && !IsBitmap(info))
                continue;
            if (!byColumn.TryGetValue(info.ColumnPaths[0], out var segments))
                byColumn[info.ColumnPaths[0]] = segments = new List<IndexInfo>();
            segments.Add(info);
        }
        if (byColumn.Count == 0) return null;

        // Each segment may be queried multiple times across an N-ary AND/OR
        // or IN list, so the context caches opened readers by segment UUID.
        var context = new Context(byColumn, fragmentIds, fs);
        try
        {
            return await VisitAsync(filter, context, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await context.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static bool IsBTree(IndexInfo info) =>
        info.TypeUrl.Contains("BTreeIndexDetails", StringComparison.Ordinal);

    private static bool IsBitmap(IndexInfo info) =>
        info.TypeUrl.Contains("BitmapIndexDetails", StringComparison.Ordinal);

    private static async ValueTask<HashSet<uint>?> VisitAsync(
        Predicate predicate, Context context, CancellationToken cancellationToken)
    {
        switch (predicate)
        {
            case ComparisonPredicate cp:
                return await VisitComparisonAsync(cp, context, cancellationToken).ConfigureAwait(false);

            case SetPredicate sp when sp.Op == SetOperator.In:
                return await VisitInAsync(sp, context, cancellationToken).ConfigureAwait(false);

            case AndPredicate ap:
                {
                    HashSet<uint>? acc = null;
                    foreach (var child in ap.Children)
                    {
                        var childSet = await VisitAsync(child, context, cancellationToken).ConfigureAwait(false);
                        if (childSet is null) continue;  // no info, no constraint
                        if (acc is null) acc = new HashSet<uint>(childSet);
                        else acc.IntersectWith(childSet);
                        if (acc.Count == 0) return acc;  // empty intersection — definitely empty
                    }
                    return acc;
                }

            case OrPredicate op:
                {
                    HashSet<uint> acc = new();
                    foreach (var child in op.Children)
                    {
                        var childSet = await VisitAsync(child, context, cancellationToken).ConfigureAwait(false);
                        if (childSet is null) return null;  // no info on a branch — must scan all
                        acc.UnionWith(childSet);
                    }
                    return acc;
                }

            case TruePredicate: return null;     // no constraint
            case FalsePredicate: return new HashSet<uint>();  // matches nothing

            // NotPredicate, UnaryPredicate (IS NULL etc.), FunctionCall,
            // SetPredicate(NotIn), unknown: punt — we return null and the
            // caller scans all fragments.
            default: return null;
        }
    }

    private static async ValueTask<HashSet<uint>?> VisitComparisonAsync(
        ComparisonPredicate cp, Context context, CancellationToken cancellationToken)
    {
        if (!TryNormalizeColumnVsLiteral(cp, out var columnName, out var op, out var value))
            return null;
        if (!context.ByColumn.TryGetValue(columnName, out var segments)) return null;
        if (value.Type != LiteralValue.Kind.Int32) return null;
        int v = value.AsInt32;

        // NotEqual: complementing matched rows would require knowing
        // every fragment, and the result wouldn't help prune anyway.
        if (op is not (ComparisonOperator.Equal or ComparisonOperator.LessThan or ComparisonOperator.LessThanOrEqual
            or ComparisonOperator.GreaterThan or ComparisonOperator.GreaterThanOrEqual))
            return null;

        return await QuerySegmentsAsync(segments, context,
            async (idx, into, ct) => AddFragments(into, await (op switch
            {
                ComparisonOperator.Equal => idx.QueryEqualAsync(v, ct),
                ComparisonOperator.LessThan =>
                    idx.QueryRangeAsync(min: null, max: v, includeMax: false, cancellationToken: ct),
                ComparisonOperator.LessThanOrEqual =>
                    idx.QueryRangeAsync(min: null, max: v, includeMax: true, cancellationToken: ct),
                ComparisonOperator.GreaterThan =>
                    idx.QueryRangeAsync(min: v, max: null, includeMin: false, cancellationToken: ct),
                _ => idx.QueryRangeAsync(min: v, max: null, includeMin: true, cancellationToken: ct),
            }).ConfigureAwait(false)),
            // BITMAP only handles equality; for anything else a bitmap segment answers nothing.
            op == ComparisonOperator.Equal
                ? async (idx, into, ct) => AddFragments(into, await idx.QueryEqualAsync(v, ct).ConfigureAwait(false))
                : null,
            cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask<HashSet<uint>?> VisitInAsync(
        SetPredicate sp, Context context, CancellationToken cancellationToken)
    {
        if (!TryGetReferenceName(sp.Operand, out IReadOnlyList<string> colName)) return null;
        if (!context.ByColumn.TryGetValue(colName, out var segments)) return null;

        // An index answers "which fragments hold this VALUE", so a list holding anything else --
        // `x IN (a, b)` names two other columns -- is one it cannot be asked about.
        if (!sp.TryGetLiteralValues(out var values)) return null;
        var ints = new List<int>(values.Count);
        foreach (var lit in values)
        {
            if (lit.Type != LiteralValue.Kind.Int32) return null;
            ints.Add(lit.AsInt32);
        }

        // Each literal's rows are reduced to their fragments as soon as they are read, so at most one query's
        // row addresses are held at a time.
        return await QuerySegmentsAsync(segments, context,
            async (idx, into, ct) =>
            {
                foreach (int v in ints)
                    AddFragments(into, await idx.QueryEqualAsync(v, ct).ConfigureAwait(false));
            },
            async (idx, into, ct) =>
            {
                foreach (int v in ints)
                    AddFragments(into, await idx.QueryEqualAsync(v, ct).ConfigureAwait(false));
            },
            cancellationToken).ConfigureAwait(false);
    }

    private static void AddFragments(HashSet<uint> into, IReadOnlyList<IndexedRowAddress> hits)
    {
        foreach (var h in hits) into.Add(h.FragmentId);
    }

    /// <summary>
    /// Asks each segment that can answer (an Int32 index of a kind the queries cover, whose directory is reachable)
    /// and returns the fragments they hit plus every fragment none of them covers; null when none can answer. Each
    /// query adds the fragments it hits to the set it is given.
    /// </summary>
    private static async ValueTask<HashSet<uint>?> QuerySegmentsAsync(
        IReadOnlyList<IndexInfo> segments,
        Context context,
        Func<BTreeIndex, HashSet<uint>, CancellationToken, Task> btree,
        Func<BitmapIndex, HashSet<uint>, CancellationToken, Task>? bitmap,
        CancellationToken cancellationToken)
    {
        HashSet<uint>? candidates = null;
        var covered = new HashSet<uint>();
        foreach (var segment in segments)
        {
            if (IsBitmap(segment))
            {
                if (bitmap is null) continue;
                var idx = await context.OpenBitmapAsync(segment, cancellationToken).ConfigureAwait(false);
                if (idx is null || idx.ValueType is not Apache.Arrow.Types.Int32Type) continue;
                candidates ??= new HashSet<uint>();
                await bitmap(idx, candidates, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var idx = await context.OpenBTreeAsync(segment, cancellationToken).ConfigureAwait(false);
                if (idx is null || idx.ValueType is not Apache.Arrow.Types.Int32Type) continue;
                candidates ??= new HashSet<uint>();
                await btree(idx, candidates, cancellationToken).ConfigureAwait(false);
            }

            covered.UnionWith(segment.FragmentIds);
        }
        if (candidates is null) return null;

        foreach (uint fragmentId in context.FragmentIds)
        {
            if (!covered.Contains(fragmentId))
                candidates.Add(fragmentId);
        }
        return candidates;
    }

    /// <summary>The segments by column, every fragment, and the readers opened so far.</summary>
    private sealed class Context : IAsyncDisposable
    {
        private readonly ITableFileSystem _fs;
        private readonly Dictionary<Guid, BTreeIndex?> _btrees = new();
        private readonly Dictionary<Guid, BitmapIndex?> _bitmaps = new();

        public Context(
            IReadOnlyDictionary<IReadOnlyList<string>, List<IndexInfo>> byColumn,
            IReadOnlyCollection<uint> fragmentIds,
            ITableFileSystem fs)
        {
            ByColumn = byColumn;
            FragmentIds = fragmentIds;
            _fs = fs;
        }

        public IReadOnlyDictionary<IReadOnlyList<string>, List<IndexInfo>> ByColumn { get; }

        public IReadOnlyCollection<uint> FragmentIds { get; }

        /// <summary>The segment's reader, or null when its base is outside the dataset and not registered.</summary>
        public async ValueTask<BTreeIndex?> OpenBTreeAsync(IndexInfo info, CancellationToken cancellationToken)
        {
            if (_btrees.TryGetValue(info.Uuid, out var idx)) return idx;
            idx = TryResolve(info) is { } dir
                ? await BTreeIndex.OpenAsync(dir.FileSystem, dir.Path, cancellationToken).ConfigureAwait(false)
                : null;
            _btrees[info.Uuid] = idx;
            return idx;
        }

        /// <summary>The segment's reader, or null when its base is outside the dataset and not registered.</summary>
        public async ValueTask<BitmapIndex?> OpenBitmapAsync(IndexInfo info, CancellationToken cancellationToken)
        {
            if (_bitmaps.TryGetValue(info.Uuid, out var idx)) return idx;
            idx = TryResolve(info) is { } dir
                ? await BitmapIndex.OpenAsync(dir.FileSystem, dir.Path, cancellationToken).ConfigureAwait(false)
                : null;
            _bitmaps[info.Uuid] = idx;
            return idx;
        }

        // An index under an unregistered base only loses its pruning: the read may never touch that base (#992).
        // Once resolved, reading the index's files still surfaces every I/O error.
        private ResolvedLanceFile? TryResolve(IndexInfo info)
        {
            try
            {
                return info.ResolveDirectory(_fs);
            }
            catch (UnregisteredStorageLocationException)
            {
                return null;
            }
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var idx in _btrees.Values)
                if (idx is not null) await idx.DisposeAsync().ConfigureAwait(false);
            foreach (var idx in _bitmaps.Values)
                if (idx is not null) await idx.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static bool TryNormalizeColumnVsLiteral(
        ComparisonPredicate cp,
        out IReadOnlyList<string> columnName,
        out ComparisonOperator op,
        out LiteralValue value)
    {
        // Accept (column, op, literal) and (literal, op, column); for the
        // latter, flip the operator's direction.
        if (TryGetReferenceName(cp.Left, out columnName) && cp.Right is LiteralExpression litR)
        {
            op = cp.Op;
            value = litR.Value;
            return true;
        }
        if (cp.Left is LiteralExpression litL && TryGetReferenceName(cp.Right, out columnName))
        {
            op = FlipOperator(cp.Op);
            value = litL.Value;
            return true;
        }
        columnName = [];
        op = default;
        value = default;
        return false;
    }

    // The path a reference names, as the row evaluator resolves it: its parts, not its dotted Name.
    private static bool TryGetReferenceName(Expression e, out IReadOnlyList<string> name)
    {
        switch (e)
        {
            case BoundReference b: name = [b.Name]; return true;
            case UnboundReference u: name = u.NameParts; return true;
            default: name = []; return false;
        }
    }

    private static ComparisonOperator FlipOperator(ComparisonOperator op) => op switch
    {
        ComparisonOperator.LessThan => ComparisonOperator.GreaterThan,
        ComparisonOperator.LessThanOrEqual => ComparisonOperator.GreaterThanOrEqual,
        ComparisonOperator.GreaterThan => ComparisonOperator.LessThan,
        ComparisonOperator.GreaterThanOrEqual => ComparisonOperator.LessThanOrEqual,
        _ => op,
    };
}

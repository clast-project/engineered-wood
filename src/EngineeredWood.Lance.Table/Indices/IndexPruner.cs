// Copyright (c) Curt Hagenlocher. All rights reserved.
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
///   supported operator → query the index, return its fragment set.</item>
///   <item><c>column IN (lits)</c> on an indexed column → union of
///   per-literal fragment sets.</item>
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
    public static async ValueTask<HashSet<uint>?> ComputeCandidatesAsync(
        Predicate filter,
        IReadOnlyList<IndexInfo> indices,
        ITableFileSystem fs,
        CancellationToken cancellationToken)
    {
        // Build column-name → IndexInfo lookup. Only single-column scalar
        // indices we know how to query (BTREE or BITMAP) get registered;
        // others fall through to "no info" so the caller scans every
        // fragment for that branch.
        var byColumn = new Dictionary<string, IndexInfo>(StringComparer.Ordinal);
        foreach (var info in indices)
        {
            if (info.ColumnNames.Count != 1) continue;
            if (!info.TypeUrl.Contains("BTreeIndexDetails", StringComparison.Ordinal)
                && !info.TypeUrl.Contains("BitmapIndexDetails", StringComparison.Ordinal))
                continue;
            byColumn[info.ColumnNames[0]] = info;
        }
        if (byColumn.Count == 0) return null;

        // Each index may be queried multiple times across an N-ary AND/OR
        // or IN list, so we cache opened readers keyed by column name.
        // BTREE and BITMAP have separate readers; the IndexInfo's TypeUrl
        // chooses which one to open.
        var btreeCache = new Dictionary<string, BTreeIndex>(StringComparer.Ordinal);
        var bitmapCache = new Dictionary<string, BitmapIndex>(StringComparer.Ordinal);
        try
        {
            return await VisitAsync(filter, byColumn, btreeCache, bitmapCache, fs, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            foreach (var idx in btreeCache.Values)
                await idx.DisposeAsync().ConfigureAwait(false);
            foreach (var idx in bitmapCache.Values)
                await idx.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async ValueTask<HashSet<uint>?> VisitAsync(
        Predicate predicate,
        IReadOnlyDictionary<string, IndexInfo> byColumn,
        Dictionary<string, BTreeIndex> btreeCache,
        Dictionary<string, BitmapIndex> bitmapCache,
        ITableFileSystem fs,
        CancellationToken cancellationToken)
    {
        switch (predicate)
        {
            case ComparisonPredicate cp:
                return await VisitComparisonAsync(cp, byColumn, btreeCache, bitmapCache, fs, cancellationToken)
                    .ConfigureAwait(false);

            case SetPredicate sp when sp.Op == SetOperator.In:
                return await VisitInAsync(sp, byColumn, btreeCache, bitmapCache, fs, cancellationToken)
                    .ConfigureAwait(false);

            case AndPredicate ap:
                {
                    HashSet<uint>? acc = null;
                    foreach (var child in ap.Children)
                    {
                        var childSet = await VisitAsync(child, byColumn, btreeCache, bitmapCache, fs, cancellationToken)
                            .ConfigureAwait(false);
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
                        var childSet = await VisitAsync(child, byColumn, btreeCache, bitmapCache, fs, cancellationToken)
                            .ConfigureAwait(false);
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
        ComparisonPredicate cp,
        IReadOnlyDictionary<string, IndexInfo> byColumn,
        Dictionary<string, BTreeIndex> btreeCache,
        Dictionary<string, BitmapIndex> bitmapCache,
        ITableFileSystem fs,
        CancellationToken cancellationToken)
    {
        if (!TryNormalizeColumnVsLiteral(cp, out var columnName, out var op, out var value))
            return null;
        if (!byColumn.TryGetValue(columnName, out var info)) return null;
        if (value.Type != LiteralValue.Kind.Int32) return null;
        int v = value.AsInt32;

        // BITMAP only handles equality. For everything else we punt to "no
        // pruning" rather than open the index unnecessarily.
        bool isBitmap = info.TypeUrl.Contains("BitmapIndexDetails", StringComparison.Ordinal);
        if (isBitmap && op != ComparisonOperator.Equal) return null;

        IReadOnlyList<IndexedRowAddress>? hits;
        if (isBitmap)
        {
            var idx = await GetOrOpenBitmapAsync(info, bitmapCache, fs, cancellationToken).ConfigureAwait(false);
            if (idx.ValueType is not Apache.Arrow.Types.Int32Type) return null;
            hits = await idx.QueryEqualAsync(v, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            var idx = await GetOrOpenBTreeAsync(info, btreeCache, fs, cancellationToken).ConfigureAwait(false);
            if (idx.ValueType is not Apache.Arrow.Types.Int32Type) return null;
            hits = op switch
            {
                ComparisonOperator.Equal =>
                    await idx.QueryEqualAsync(v, cancellationToken).ConfigureAwait(false),
                ComparisonOperator.LessThan =>
                    await idx.QueryRangeAsync(min: null, max: v, includeMax: false, cancellationToken: cancellationToken).ConfigureAwait(false),
                ComparisonOperator.LessThanOrEqual =>
                    await idx.QueryRangeAsync(min: null, max: v, includeMax: true, cancellationToken: cancellationToken).ConfigureAwait(false),
                ComparisonOperator.GreaterThan =>
                    await idx.QueryRangeAsync(min: v, max: null, includeMin: false, cancellationToken: cancellationToken).ConfigureAwait(false),
                ComparisonOperator.GreaterThanOrEqual =>
                    await idx.QueryRangeAsync(min: v, max: null, includeMin: true, cancellationToken: cancellationToken).ConfigureAwait(false),
                // NotEqual: complementing matched rows would require knowing
                // every fragment, and the result wouldn't help prune anyway.
                _ => null,
            };
        }
        if (hits is null) return null;
        return ToFragmentSet(hits);
    }

    private static async ValueTask<HashSet<uint>?> VisitInAsync(
        SetPredicate sp,
        IReadOnlyDictionary<string, IndexInfo> byColumn,
        Dictionary<string, BTreeIndex> btreeCache,
        Dictionary<string, BitmapIndex> bitmapCache,
        ITableFileSystem fs,
        CancellationToken cancellationToken)
    {
        if (!TryGetReferenceName(sp.Operand, out string colName)) return null;
        if (!byColumn.TryGetValue(colName, out var info)) return null;
        bool isBitmap = info.TypeUrl.Contains("BitmapIndexDetails", StringComparison.Ordinal);

        var acc = new HashSet<uint>();
        foreach (var lit in sp.Values)
        {
            if (lit.Type != LiteralValue.Kind.Int32) return null;
            int v = lit.AsInt32;
            IReadOnlyList<IndexedRowAddress> hits;
            if (isBitmap)
            {
                var idx = await GetOrOpenBitmapAsync(info, bitmapCache, fs, cancellationToken).ConfigureAwait(false);
                if (idx.ValueType is not Apache.Arrow.Types.Int32Type) return null;
                hits = await idx.QueryEqualAsync(v, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var idx = await GetOrOpenBTreeAsync(info, btreeCache, fs, cancellationToken).ConfigureAwait(false);
                if (idx.ValueType is not Apache.Arrow.Types.Int32Type) return null;
                hits = await idx.QueryEqualAsync(v, cancellationToken).ConfigureAwait(false);
            }
            foreach (var h in hits) acc.Add(h.FragmentId);
        }
        return acc;
    }

    private static async ValueTask<BTreeIndex> GetOrOpenBTreeAsync(
        IndexInfo info, Dictionary<string, BTreeIndex> opened,
        ITableFileSystem fs, CancellationToken cancellationToken)
    {
        if (opened.TryGetValue(info.Name, out var idx)) return idx;
        idx = await BTreeIndex.OpenAsync(fs, info.DirectoryPath, cancellationToken)
            .ConfigureAwait(false);
        opened[info.Name] = idx;
        return idx;
    }

    private static async ValueTask<BitmapIndex> GetOrOpenBitmapAsync(
        IndexInfo info, Dictionary<string, BitmapIndex> opened,
        ITableFileSystem fs, CancellationToken cancellationToken)
    {
        if (opened.TryGetValue(info.Name, out var idx)) return idx;
        idx = await BitmapIndex.OpenAsync(fs, info.DirectoryPath, cancellationToken)
            .ConfigureAwait(false);
        opened[info.Name] = idx;
        return idx;
    }

    private static bool TryNormalizeColumnVsLiteral(
        ComparisonPredicate cp,
        out string columnName,
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
        columnName = string.Empty;
        op = default;
        value = default;
        return false;
    }

    private static bool TryGetReferenceName(Expression e, out string name)
    {
        switch (e)
        {
            case BoundReference b: name = b.Name; return true;
            case UnboundReference u: name = u.Name; return true;
            default: name = string.Empty; return false;
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

    private static HashSet<uint> ToFragmentSet(IReadOnlyList<IndexedRowAddress> hits)
    {
        var set = new HashSet<uint>();
        foreach (var h in hits) set.Add(h.FragmentId);
        return set;
    }
}

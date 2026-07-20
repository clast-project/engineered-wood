// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.Parquet.BloomFilter;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet;

/// <summary>
/// Walks a <see cref="Predicate"/> tree and uses Bloom filters (where
/// available) to derive <see cref="FilterResult.AlwaysFalse"/> for
/// equality and IN sub-predicates whose values miss the filter.
/// </summary>
/// <remarks>
/// Bloom filters can only prove absence, never presence, so this evaluator
/// returns either <see cref="FilterResult.AlwaysFalse"/> or
/// <see cref="FilterResult.Unknown"/>. Other predicate kinds (range, IS NULL,
/// function calls) are treated as Unknown and contribute nothing.
///
/// Compose with <see cref="StatisticsEvaluator"/>: run statistics first, then
/// fall back to Bloom filters only for row groups still marked Unknown.
/// </remarks>
internal static class BloomFilterPredicateEvaluator
{
    /// <summary>
    /// Evaluates the predicate against the row group's Bloom filters, reading
    /// filter blocks lazily from <paramref name="file"/>. Returns
    /// <see cref="FilterResult.AlwaysFalse"/> only when one or more
    /// equality/IN sub-predicates definitively miss.
    /// </summary>
    public static async ValueTask<FilterResult> EvaluateAsync(
        Predicate predicate,
        int rowGroupIndex,
        FileMetaData metadata,
        SchemaDescriptor schema,
        IRandomAccessFile file,
        long fileLength,
        CancellationToken ct)
    {
        var ctx = new Context(rowGroupIndex, metadata, schema, file, fileLength);
        return await EvaluateAsync(predicate, ctx, ct).ConfigureAwait(false);
    }

    private static async ValueTask<FilterResult> EvaluateAsync(
        Predicate predicate, Context ctx, CancellationToken ct)
    {
        switch (predicate)
        {
            case TruePredicate:
                return FilterResult.AlwaysTrue;
            case FalsePredicate:
                return FilterResult.AlwaysFalse;

            case AndPredicate and:
            {
                bool allTrue = true;
                foreach (var child in and.Children)
                {
                    var r = await EvaluateAsync(child, ctx, ct).ConfigureAwait(false);
                    if (r == FilterResult.AlwaysFalse) return FilterResult.AlwaysFalse;
                    if (r != FilterResult.AlwaysTrue) allTrue = false;
                }
                return allTrue ? FilterResult.AlwaysTrue : FilterResult.Unknown;
            }

            case OrPredicate or:
            {
                bool allFalse = true;
                foreach (var child in or.Children)
                {
                    var r = await EvaluateAsync(child, ctx, ct).ConfigureAwait(false);
                    if (r == FilterResult.AlwaysTrue) return FilterResult.AlwaysTrue;
                    if (r != FilterResult.AlwaysFalse) allFalse = false;
                }
                return allFalse ? FilterResult.AlwaysFalse : FilterResult.Unknown;
            }

            case NotPredicate not:
                return (await EvaluateAsync(not.Child, ctx, ct).ConfigureAwait(false)) switch
                {
                    FilterResult.AlwaysTrue => FilterResult.AlwaysFalse,
                    FilterResult.AlwaysFalse => FilterResult.AlwaysTrue,
                    _ => FilterResult.Unknown,
                };

            case ComparisonPredicate cmp when IsEquality(cmp.Op):
                return await EvaluateEqualityAsync(cmp, ctx, ct).ConfigureAwait(false);

            case SetPredicate set when set.Op == SetOperator.In:
                return await EvaluateInAsync(set, ctx, ct).ConfigureAwait(false);

            // Range, IS NULL, NOT IN, function calls, etc. — Bloom filters can't help.
            default:
                return FilterResult.Unknown;
        }
    }

    private static bool IsEquality(ComparisonOperator op) =>
        op == ComparisonOperator.Equal || op == ComparisonOperator.NullSafeEqual;

    private static async ValueTask<FilterResult> EvaluateEqualityAsync(
        ComparisonPredicate cmp, Context ctx, CancellationToken ct)
    {
        if (!TryGetColumnAndLiteral(cmp.Left, cmp.Right, out string? column, out var value)
            && !TryGetColumnAndLiteral(cmp.Right, cmp.Left, out column, out value))
            return FilterResult.Unknown;

        if (value.IsNull)
            return FilterResult.Unknown;

        return await ProbeAsync(column!, [value], ctx, ct).ConfigureAwait(false);
    }

    private static async ValueTask<FilterResult> EvaluateInAsync(
        SetPredicate set, Context ctx, CancellationToken ct)
    {
        if (!TryGetColumnName(set.Operand, out string? column))
            return FilterResult.Unknown;
        if (set.Values.Count == 0)
            return FilterResult.AlwaysFalse;

        return await ProbeAsync(column!, set.Values, ctx, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Probes the Bloom filter for any of the provided values. Returns
    /// AlwaysFalse if every value misses; Unknown if any might match or if no
    /// Bloom filter is available.
    /// </summary>
    private static async ValueTask<FilterResult> ProbeAsync(
        string column, IReadOnlyList<LiteralValue> values, Context ctx, CancellationToken ct)
    {
        if (!ctx.TryFindColumn(column, out int columnIndex, out var descriptor))
            return FilterResult.Unknown;

        var colMeta = ctx.Metadata.RowGroups[ctx.RowGroupIndex]
            .Columns[columnIndex].MetaData;
        if (colMeta?.BloomFilterOffset is not long offset || offset <= 0)
            return FilterResult.Unknown;

        long length = colMeta.BloomFilterLength
            ?? Math.Min(4096, ctx.FileLength - offset);

        using var buffer = (await ctx.File.ReadRangesAsync(
            new[] { new FileRange(offset, length) }, ct).ConfigureAwait(false))[0];

        var filter = BloomFilterReader.Parse(buffer.Memory.Span);

        foreach (var v in values)
        {
            if (!TryEncodeForBloom(v, descriptor!, out byte[] bytes))
                return FilterResult.Unknown; // can't encode → can't decide
            if (filter.MightContain(bytes))
                return FilterResult.Unknown; // maybe present
        }

        return FilterResult.AlwaysFalse;
    }

    // ── Helpers ──

    private static bool TryGetColumnAndLiteral(
        Expression maybeRef, Expression maybeLit,
        out string? column, out LiteralValue value)
    {
        if (TryGetColumnName(maybeRef, out column) && maybeLit is LiteralExpression lit)
        {
            value = lit.Value;
            return true;
        }
        column = null;
        value = LiteralValue.Null;
        return false;
    }

    private static bool TryGetColumnName(Expression expr, out string? name)
    {
        switch (expr)
        {
            case UnboundReference u: name = u.Name; return true;
            case BoundReference b: name = b.Name; return true;
            default: name = null; return false;
        }
    }

    /// <summary>
    /// Encodes a typed <see cref="LiteralValue"/> into the byte representation
    /// the Bloom filter was built from. Mirrors
    /// <see cref="BloomFilterValueEncoder"/> but operates on the typed value
    /// instead of <c>object</c>.
    /// </summary>
    private static bool TryEncodeForBloom(
        LiteralValue value, ColumnDescriptor descriptor, out byte[] bytes)
    {
        try
        {
            object? boxed = ToObjectForPhysicalType(value, descriptor.PhysicalType);
            if (boxed is null) { bytes = []; return false; }
            bytes = BloomFilterValueEncoder.Encode(boxed, descriptor.PhysicalType);
            return true;
        }
        catch (ArgumentException)
        {
            bytes = [];
            return false;
        }
    }

    /// <summary>
    /// Converts a <see cref="LiteralValue"/> to the boxed .NET type that
    /// <see cref="BloomFilterValueEncoder"/> expects for the column's physical
    /// type. Returns null when no safe conversion exists.
    /// </summary>
    private static object? ToObjectForPhysicalType(LiteralValue v, PhysicalType pt) => pt switch
    {
        PhysicalType.Boolean => v.Type == LiteralValue.Kind.Boolean ? (object)v.AsBoolean : null,
        PhysicalType.Int32 => v.Type switch
        {
            LiteralValue.Kind.Int32 => v.AsInt32,
            LiteralValue.Kind.Int64 => v.AsInt64 is >= int.MinValue and <= int.MaxValue
                ? (object)(int)v.AsInt64 : null,
            _ => null,
        },
        PhysicalType.Int64 => v.Type switch
        {
            LiteralValue.Kind.Int64 => (object)v.AsInt64,
            LiteralValue.Kind.Int32 => (long)v.AsInt32,
            _ => null,
        },
        PhysicalType.Float => v.Type == LiteralValue.Kind.Float ? (object)v.AsFloat : null,
        PhysicalType.Double => v.Type switch
        {
            LiteralValue.Kind.Double => v.AsDouble,
            LiteralValue.Kind.Float => (double)v.AsFloat,
            _ => null,
        },
        PhysicalType.ByteArray or PhysicalType.FixedLenByteArray => v.Type switch
        {
            LiteralValue.Kind.String => v.AsString,
            LiteralValue.Kind.Binary => v.AsBinary,
            _ => null,
        },
        _ => null,
    };

    private sealed class Context
    {
        public Context(int rgIndex, FileMetaData metadata, SchemaDescriptor schema,
            IRandomAccessFile file, long fileLength)
        {
            RowGroupIndex = rgIndex;
            Metadata = metadata;
            Schema = schema;
            File = file;
            FileLength = fileLength;
        }

        public int RowGroupIndex { get; }
        public FileMetaData Metadata { get; }
        public SchemaDescriptor Schema { get; }
        public IRandomAccessFile File { get; }
        public long FileLength { get; }

        public bool TryFindColumn(string name, out int index, out ColumnDescriptor? descriptor)
        {
            for (int i = 0; i < Schema.Columns.Count; i++)
            {
                if (Schema.Columns[i].DottedPath == name
                    || (Schema.Columns[i].Path.Count == 1
                        && Schema.Columns[i].Path[0] == name))
                {
                    index = i;
                    descriptor = Schema.Columns[i];
                    return true;
                }
            }
            index = -1;
            descriptor = null;
            return false;
        }
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.Parquet.BloomFilter;
using EngineeredWood.Parquet.Data;
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
            object? boxed = ToObjectForColumn(value, descriptor);
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
    /// Converts a <see cref="LiteralValue"/> to the boxed .NET value the column's bytes would hold.
    /// </summary>
    /// <remarks>
    /// Temporal literals are decided by the LOGICAL type, not the physical one, so they are handled
    /// before the physical dispatch below. Until this existed, a predicate on a DATE, TIME or TIMESTAMP
    /// column could never probe a bloom filter at all: the statistics layer hands those over as
    /// DateOnly / TimeOnly / DateTimeOffset, and every one of them fell through to null.
    /// </remarks>
    private static object? ToObjectForColumn(LiteralValue v, ColumnDescriptor desc)
        => v.Type is LiteralValue.Kind.DateTimeOffset or LiteralValue.Kind.DateOnly
            or LiteralValue.Kind.TimeOnly
            ? TemporalToObject(v, desc)
            : ToObjectForPhysicalType(v, desc.PhysicalType);

    /// <summary>Ticks (100 ns) from .NET's epoch (0001-01-01) to the Unix epoch.</summary>
    private const long UnixEpochTicks = 621_355_968_000_000_000L;

    /// <summary>
    /// Converts a temporal literal to the exact value stored in the column, or null when no stored
    /// value could equal it.
    /// </summary>
    /// <remarks>
    /// <para>EXACTNESS IS THE WHOLE RULE. A bloom filter answers "these bytes, or definitely nothing",
    /// so a literal is only worth probing with if it converts to the column's unit without a remainder.
    /// A DateTimeOffset of 1.5 ms against a MILLIS column does not, and rounding it would probe for a
    /// value the caller never asked about. Declining costs a pruning opportunity and nothing else.</para>
    ///
    /// <para>Returning null is always safe here: it means the filter is not consulted, so the row group
    /// is read. The unsafe direction would be probing with the wrong bytes and being told "absent".</para>
    /// </remarks>
    private static object? TemporalToObject(LiteralValue v, ColumnDescriptor desc)
    {
        var logical = desc.SchemaElement.LogicalType;

        switch (v.Type)
        {
            case LiteralValue.Kind.DateTimeOffset when logical is LogicalType.TimestampType ts:
            {
                // Normalising by the offset is right for an isAdjustedToUTC column, and a no-op for a
                // naive one -- this reader only ever produces those with a zero offset.
                long ticks = v.AsDateTimeOffset.ToUniversalTime().Ticks - UnixEpochTicks;
                if (!ExtendedTimestamp.TryTicksToUnit(ticks, ts.Unit, out Int128 count))
                    return null;

                // Computed in 128 bits because NANOS does not fit 64: year 9999 is 2.5e20 nanoseconds.
                // An INT64 column cannot hold such a value at all, so a literal that big matches nothing
                // there and there is no probe to make.
                if (desc.PhysicalType == PhysicalType.Int64)
                    return ExtendedTimestamp.TryToInt64(count, out long narrowed) ? narrowed : (object?)null;

                if (desc.PhysicalType == PhysicalType.FixedLenByteArray
                    && desc.TypeLength == ExtendedTimestamp.ByteWidth)
                {
                    // The filter holds the hash of the bytes as they sit in the file, so the literal has
                    // to become those same twelve little-endian bytes. The carrier holds +/-2^95, so a
                    // value it cannot represent is likewise not in the column.
                    if (!ExtendedTimestamp.IsRepresentable(count))
                        return null;

                    var carrier = new byte[ExtendedTimestamp.ByteWidth];
                    ExtendedTimestamp.Write(count, carrier);
                    return carrier;
                }

                return null;
            }

#if NET6_0_OR_GREATER
            case LiteralValue.Kind.DateOnly
                when logical is LogicalType.DateType && desc.PhysicalType == PhysicalType.Int32:
                return v.AsDateOnly.DayNumber - EpochDays;

            case LiteralValue.Kind.TimeOnly when logical is LogicalType.TimeType time:
            {
                long ticks = v.AsTimeOnly.Ticks;
                return time.Unit switch
                {
                    Metadata.TimeUnit.Millis when desc.PhysicalType == PhysicalType.Int32
                        => ticks % 10_000 == 0 ? (object)(int)(ticks / 10_000) : null,
                    Metadata.TimeUnit.Micros when desc.PhysicalType == PhysicalType.Int64
                        => ticks % 10 == 0 ? (object)(ticks / 10) : null,
                    // Safe in 64 bits, unlike the TIMESTAMP case: a time of day is at most 8.64e13
                    // nanoseconds, nowhere near where a long runs out.
                    Metadata.TimeUnit.Nanos when desc.PhysicalType == PhysicalType.Int64
                        => ticks * 100,
                    _ => null,
                };
            }
#endif
            default:
                return null;
        }
    }

    /// <summary>Days from .NET's epoch (0001-01-01) to the Unix epoch (1970-01-01).</summary>
    private const int EpochDays = 719_162;

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

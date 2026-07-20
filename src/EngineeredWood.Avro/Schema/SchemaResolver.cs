// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

namespace EngineeredWood.Avro.Schema;

/// <summary>
/// Resolves differences between writer and reader Avro schemas per the Avro specification.
/// Produces a <see cref="SchemaResolution"/> that drives the decode loop.
/// </summary>
internal static class SchemaResolver
{
    private const int MaxSchemaDepth = 64;
    /// <summary>
    /// Resolves a writer record schema against a reader record schema.
    /// </summary>
    public static SchemaResolution Resolve(AvroRecordSchema writerSchema, AvroRecordSchema readerSchema)
    {
        // Build a lookup: reader field name → index, plus aliases → index
        var readerFieldIndex = new Dictionary<string, int>(StringComparer.Ordinal);
        for (int i = 0; i < readerSchema.Fields.Count; i++)
        {
            var rf = readerSchema.Fields[i];
            readerFieldIndex[rf.Name] = i;
            foreach (var alias in rf.Aliases)
            {
#if NET8_0_OR_GREATER
                readerFieldIndex.TryAdd(alias, i);
#else
                if (!readerFieldIndex.ContainsKey(alias))
                    readerFieldIndex[alias] = i;
#endif
            }
        }

        // For each writer field, find the matching reader field
        var writerActions = new WriterFieldAction[writerSchema.Fields.Count];
        var matchedReaderFields = new HashSet<int>();

        for (int wi = 0; wi < writerSchema.Fields.Count; wi++)
        {
            var wf = writerSchema.Fields[wi];
            if (readerFieldIndex.TryGetValue(wf.Name, out int ri))
            {
                // Matched by name or alias — resolve schemas recursively
                var readerFieldSchema = readerSchema.Fields[ri].Schema;
                var writerFieldSchema = wf.Schema;
                ValidateSchemaCompatibility(writerFieldSchema, readerFieldSchema, wf.Name);

                writerActions[wi] = new WriterFieldAction(ri, false);
                matchedReaderFields.Add(ri);
            }
            else
            {
                // Writer field not in reader: skip it
                writerActions[wi] = new WriterFieldAction(-1, true);
            }
        }

        // Determine reader fields that need defaults
        var defaultFields = new List<DefaultField>();
        for (int ri = 0; ri < readerSchema.Fields.Count; ri++)
        {
            if (matchedReaderFields.Contains(ri))
                continue;

            var rf = readerSchema.Fields[ri];
            if (!rf.Default.HasValue)
                throw new InvalidOperationException(
                    $"Reader schema field '{rf.Name}' is not in the writer schema and has no default value.");

            defaultFields.Add(new DefaultField(ri, rf.Default.Value, rf.Schema));
        }

        // Build the resolved Arrow schema from the reader schema
        var resolvedArrowSchema = ArrowSchemaConverter.ToArrow(readerSchema);

        // Build promotion map: for matched fields, determine if type promotion is needed
        var promotions = new TypePromotion?[readerSchema.Fields.Count];
        for (int wi = 0; wi < writerSchema.Fields.Count; wi++)
        {
            if (writerActions[wi].Skip) continue;
            int ri = writerActions[wi].ReaderFieldIndex;
            var writerFieldSchema = UnwrapNullable(writerSchema.Fields[wi].Schema);
            var readerFieldSchema = UnwrapNullable(readerSchema.Fields[ri].Schema);
            promotions[ri] = DetectPromotion(writerFieldSchema, readerFieldSchema, 0);
        }

        return new SchemaResolution(
            writerActions,
            defaultFields.ToArray(),
            promotions,
            readerSchema,
            resolvedArrowSchema);
    }

    /// <summary>Unwrap nullable union to get inner type.</summary>
    private static AvroSchemaNode UnwrapNullable(AvroSchemaNode schema)
    {
        if (schema is AvroUnionSchema union && union.IsNullable(out var inner, out _))
            return inner;
        return schema;
    }

    /// <summary>
    /// Validates that the writer schema can be resolved to the reader schema.
    /// Throws if incompatible.
    /// </summary>
    private static void ValidateSchemaCompatibility(
        AvroSchemaNode writerSchema, AvroSchemaNode readerSchema, string fieldName)
    {
        var w = UnwrapNullable(writerSchema);
        var r = UnwrapNullable(readerSchema);

        // Same type: validate further for enums
        if (w.Type == r.Type)
        {
            if (w is AvroEnumSchema we && r is AvroEnumSchema re)
            {
                var readerSymbolSet = new HashSet<string>(re.Symbols, StringComparer.Ordinal);
                foreach (var sym in we.Symbols)
                {
                    if (!readerSymbolSet.Contains(sym) && re.Default == null)
                        throw new InvalidOperationException(
                            $"Writer enum symbol '{sym}' for field '{fieldName}' is not in reader schema and reader has no default.");
                }
            }
            return;
        }

        // Check type promotions
        if (IsPromotable(w.Type, r.Type)) return;

        throw new InvalidOperationException(
            $"Incompatible types for field '{fieldName}': writer is {w.Type}, reader is {r.Type}.");
    }

    /// <summary>
    /// Returns true if <paramref name="writerType"/> can be promoted to <paramref name="readerType"/>.
    /// </summary>
    internal static bool IsPromotable(AvroType writerType, AvroType readerType)
    {
        return (writerType, readerType) switch
        {
            (AvroType.Int, AvroType.Long) => true,
            (AvroType.Int, AvroType.Float) => true,
            (AvroType.Int, AvroType.Double) => true,
            (AvroType.Long, AvroType.Float) => true,
            (AvroType.Long, AvroType.Double) => true,
            (AvroType.Float, AvroType.Double) => true,
            (AvroType.String, AvroType.Bytes) => true,
            (AvroType.Bytes, AvroType.String) => true,
            _ => false,
        };
    }

    /// <summary>
    /// Detects the type promotion needed for a matched field pair.
    /// Returns null if no promotion is needed (same types).
    /// </summary>
    private static TypePromotion? DetectPromotion(AvroSchemaNode writerSchema, AvroSchemaNode readerSchema, int depth)
    {
        if (depth >= MaxSchemaDepth)
            throw new NotSupportedException(
                "Avro schema exceeds maximum nesting depth. Recursive schemas cannot be represented in Arrow.");

        if (writerSchema.Type == readerSchema.Type)
        {
            // Same type — check for nested record resolution
            if (writerSchema is AvroRecordSchema wr && readerSchema is AvroRecordSchema rr)
                return new TypePromotion(PromotionKind.NestedRecord, wr, rr);
            if (writerSchema is AvroArraySchema wa && readerSchema is AvroArraySchema ra)
            {
                var itemPromotion = DetectPromotion(UnwrapNullable(wa.Items), UnwrapNullable(ra.Items), depth + 1);
                if (itemPromotion != null)
                    return new TypePromotion(PromotionKind.ArrayElement, wa, ra);
            }
            if (writerSchema is AvroMapSchema wm && readerSchema is AvroMapSchema rm)
            {
                var valPromotion = DetectPromotion(UnwrapNullable(wm.Values), UnwrapNullable(rm.Values), depth + 1);
                if (valPromotion != null)
                    return new TypePromotion(PromotionKind.MapValue, wm, rm);
            }
            if (writerSchema is AvroEnumSchema we && readerSchema is AvroEnumSchema re)
            {
                // Check if symbol lists differ (different count or different order/content)
                if (!we.Symbols.SequenceEqual(re.Symbols, StringComparer.Ordinal))
                    return new TypePromotion(PromotionKind.EnumRemap, we, re);
            }
            return null;
        }

        // Numeric promotions
        return (writerSchema.Type, readerSchema.Type) switch
        {
            (AvroType.Int, AvroType.Long) => new TypePromotion(PromotionKind.IntToLong),
            (AvroType.Int, AvroType.Float) => new TypePromotion(PromotionKind.IntToFloat),
            (AvroType.Int, AvroType.Double) => new TypePromotion(PromotionKind.IntToDouble),
            (AvroType.Long, AvroType.Float) => new TypePromotion(PromotionKind.LongToFloat),
            (AvroType.Long, AvroType.Double) => new TypePromotion(PromotionKind.LongToDouble),
            (AvroType.Float, AvroType.Double) => new TypePromotion(PromotionKind.FloatToDouble),
            (AvroType.String, AvroType.Bytes) => new TypePromotion(PromotionKind.StringToBytes),
            (AvroType.Bytes, AvroType.String) => new TypePromotion(PromotionKind.BytesToString),
            _ => null,
        };
    }
}

/// <summary>Action for a single writer field during decode.</summary>
internal readonly record struct WriterFieldAction(int ReaderFieldIndex, bool Skip);

/// <summary>A reader field not in the writer that needs a default value.</summary>
internal readonly record struct DefaultField(int ReaderIndex, JsonElement DefaultValue, AvroSchemaNode Schema);

/// <summary>Type promotion kind.</summary>
internal enum PromotionKind
{
    IntToLong,
    IntToFloat,
    IntToDouble,
    LongToFloat,
    LongToDouble,
    FloatToDouble,
    StringToBytes,
    BytesToString,
    NestedRecord,
    ArrayElement,
    MapValue,
    EnumRemap,
}

/// <summary>Describes a type promotion for a field.</summary>
internal sealed class TypePromotion
{
    public PromotionKind Kind { get; }
    public AvroSchemaNode? WriterSchema { get; }
    public AvroSchemaNode? ReaderSchema { get; }

    public TypePromotion(PromotionKind kind, AvroSchemaNode? writerSchema = null, AvroSchemaNode? readerSchema = null)
    {
        Kind = kind;
        WriterSchema = writerSchema;
        ReaderSchema = readerSchema;
    }
}

/// <summary>
/// The result of schema resolution: instructions for how to decode writer data into reader schema.
/// </summary>
internal sealed class SchemaResolution
{
    /// <summary>Actions for each writer field, in writer field order.</summary>
    public WriterFieldAction[] WriterActions { get; }

    /// <summary>Reader fields that need defaults (not present in writer).</summary>
    public DefaultField[] DefaultFields { get; }

    /// <summary>Type promotion for each reader field index (null if no promotion).</summary>
    public TypePromotion?[] Promotions { get; }

    /// <summary>The reader's record schema.</summary>
    public AvroRecordSchema ReaderSchema { get; }

    /// <summary>The Arrow schema derived from the reader schema.</summary>
    public Apache.Arrow.Schema ArrowSchema { get; }

    public SchemaResolution(
        WriterFieldAction[] writerActions,
        DefaultField[] defaultFields,
        TypePromotion?[] promotions,
        AvroRecordSchema readerSchema,
        Apache.Arrow.Schema arrowSchema)
    {
        WriterActions = writerActions;
        DefaultFields = defaultFields;
        Promotions = promotions;
        ReaderSchema = readerSchema;
        ArrowSchema = arrowSchema;
    }
}

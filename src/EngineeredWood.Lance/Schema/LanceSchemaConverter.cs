// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Types;
using EngineeredWood.Lance.Proto;
using Google.Protobuf;
using Google.Protobuf.Collections;

namespace EngineeredWood.Lance.Schema;

/// <summary>
/// Converts a Lance <see cref="Proto.Schema"/> (flat list of fields linked by
/// <c>parent_id</c>, with a <c>logical_type</c> string per leaf) into an
/// Apache Arrow <see cref="Apache.Arrow.Schema"/>.
/// </summary>
internal static class LanceSchemaConverter
{
    // Lance encodes the logical type of a PARENT/REPEATED field in a small
    // vocabulary of strings. Using string literals directly in patterns below
    // avoids collisions with same-named Apache.Arrow types.
    private const string StructKind = "struct";
    private const string ListKind = "list";
    private const string LargeListKind = "large_list";
    private const string ListStructKind = "list.struct";
    private const string LargeListStructKind = "large_list.struct";

    public static Apache.Arrow.Schema ToArrowSchema(Proto.Schema schema)
    {
        // Lance fields are flat with parent_id pointers; reshape into a tree.
        // A field is a root when its parent_id doesn't reference any actual
        // field in the schema. pylance marks root fields with parent_id == -1;
        // hand-crafted fields default to parent_id == 0 and assign IDs
        // starting at 1, so 0 is "no existing field". Both work.
        var allIds = BuildIdSet(schema);
        var byParent = new Dictionary<int, List<Proto.Field>>();
        var roots = new List<Proto.Field>();
        foreach (var f in schema.Fields)
        {
            if (IsRoot(f.ParentId, allIds))
            {
                roots.Add(f);
            }
            else
            {
                if (!byParent.TryGetValue(f.ParentId, out var list))
                {
                    list = new List<Proto.Field>();
                    byParent[f.ParentId] = list;
                }
                list.Add(f);
            }
        }

        var arrowFields = new Apache.Arrow.Field[roots.Count];
        for (int i = 0; i < roots.Count; i++)
            arrowFields[i] = BuildField(roots[i], byParent);

        var metadata = ConvertMetadata(schema.Metadata);
        return new Apache.Arrow.Schema(arrowFields, metadata);
    }

    private static HashSet<int> BuildIdSet(Proto.Schema schema)
    {
        var set = new HashSet<int>();
        foreach (var f in schema.Fields) set.Add(f.Id);
        return set;
    }

    internal static bool IsRoot(int parentId, HashSet<int> allIds) =>
        parentId < 0 || !allIds.Contains(parentId);

    private static Apache.Arrow.Field BuildField(
        Proto.Field field, Dictionary<int, List<Proto.Field>> byParent)
    {
        var (type, childFields) = BuildType(field, byParent);
        _ = childFields; // children are already wired into the IArrowType by BuildType
        var metadata = ConvertMetadata(field.Metadata);
        return new Apache.Arrow.Field(field.Name, type, field.Nullable, metadata);
    }

    private static (IArrowType Type, IReadOnlyList<Apache.Arrow.Field> Children) BuildType(
        Proto.Field field, Dictionary<int, List<Proto.Field>> byParent)
    {
        // pylance and other Lance writers don't reliably set Field.type — it
        // often stays at the proto3 default (0 = PARENT). logical_type is the
        // authoritative discriminator in practice, so dispatch on that.
        string logical = field.LogicalType ?? string.Empty;

        switch (logical)
        {
            case StructKind:
                {
                    var children = BuildChildren(field.Id, byParent);
                    return (new StructType(children), children);
                }
            case ListKind:
            case LargeListKind:
            case ListStructKind:
            case LargeListStructKind:
                return BuildListType(field, byParent, logical);
            default:
                return (ParseLeafType(logical, field.Name), Array.Empty<Apache.Arrow.Field>());
        }
    }

    private static (IArrowType Type, IReadOnlyList<Apache.Arrow.Field> Children) BuildListType(
        Proto.Field field, Dictionary<int, List<Proto.Field>> byParent, string logical)
    {
        bool large = logical is LargeListKind or LargeListStructKind;
        bool structItem = logical is ListStructKind or LargeListStructKind;

        var children = BuildChildren(field.Id, byParent);
        Apache.Arrow.Field itemField;

        // Both `list` and `list.struct` variants surface in practice with a
        // single direct child field that carries the item's type. For `list`
        // that child is a leaf; for `list.struct` pylance emits an explicit
        // intermediate field whose logical_type is "struct" (already built as
        // an Arrow StructType by BuildType).
        if (children.Length != 1)
            throw new LanceFormatException(
                $"REPEATED field '{field.Name}' (logical '{logical}') expects exactly one child, got {children.Length}.");

        if (structItem && children[0].DataType is not StructType)
            throw new LanceFormatException(
                $"REPEATED '{logical}' field '{field.Name}' has non-struct child type {children[0].DataType}.");

        itemField = children[0];

        IArrowType listType = large
            ? new LargeListType(itemField)
            : new Apache.Arrow.Types.ListType(itemField);

        return (listType, new[] { itemField });
    }

    private static Apache.Arrow.Field[] BuildChildren(
        int parentId, Dictionary<int, List<Proto.Field>> byParent)
    {
        if (!byParent.TryGetValue(parentId, out var kids) || kids.Count == 0)
            return Array.Empty<Apache.Arrow.Field>();

        var result = new Apache.Arrow.Field[kids.Count];
        for (int i = 0; i < kids.Count; i++)
            result[i] = BuildField(kids[i], byParent);
        return result;
    }

    private static IArrowType ParseLeafType(string logical, string fieldName)
    {
        // Fast path for the common fixed strings.
        switch (logical)
        {
            case "null": return NullType.Default;
            case "bool": return BooleanType.Default;
            case "int8": return Int8Type.Default;
            case "uint8": return UInt8Type.Default;
            case "int16": return Int16Type.Default;
            case "uint16": return UInt16Type.Default;
            case "int32": return Int32Type.Default;
            case "uint32": return UInt32Type.Default;
            case "int64": return Int64Type.Default;
            case "uint64": return UInt64Type.Default;
            case "halffloat": return HalfFloatType.Default;
            case "float": return FloatType.Default;
            case "double": return DoubleType.Default;
            case "string": return StringType.Default;
            case "large_string": return LargeStringType.Default;
            case "binary": return BinaryType.Default;
            case "large_binary": return LargeBinaryType.Default;
            case "date32:day": return Date32Type.Default;
            case "date64:ms": return Date64Type.Default;
        }

        // Parameterized forms: "decimal:...", "time:...", "timestamp:...",
        // "duration:...", "dict:...", "fixed_size_binary:{width}".
        int colon = logical.IndexOf(':');
        if (colon > 0)
        {
            string head = logical.Substring(0, colon);
            string tail = logical.Substring(colon + 1);

            switch (head)
            {
                case "decimal": return ParseDecimal(tail, logical, fieldName);
                case "time": return ParseTime(tail, logical, fieldName);
                case "timestamp": return ParseTimestamp(tail, logical, fieldName);
                case "duration": return ParseDuration(tail, logical, fieldName);
                case "fixed_size_binary":
                    if (!int.TryParse(tail, out int width) || width <= 0)
                        throw new LanceFormatException(
                            $"Field '{fieldName}' has malformed fixed_size_binary width in '{logical}'.");
                    return new FixedSizeBinaryType(width);
                case "fixed_size_list":
                    // "fixed_size_list:{inner_logical}:{dim}". `inner` may itself
                    // contain ':' (e.g. "decimal:128:12:4"), so split the dim
                    // from the right.
                    {
                        int lastColon = tail.LastIndexOf(':');
                        if (lastColon <= 0)
                            throw new LanceFormatException(
                                $"Field '{fieldName}' has malformed fixed_size_list in '{logical}'.");
                        if (!int.TryParse(tail.Substring(lastColon + 1), out int dim) || dim <= 0)
                            throw new LanceFormatException(
                                $"Field '{fieldName}' has non-numeric fixed_size_list dimension in '{logical}'.");
                        string innerLogical = tail.Substring(0, lastColon);
                        var innerType = ParseLeafType(innerLogical, fieldName);
                        var itemField = new Apache.Arrow.Field("item", innerType, nullable: true);
                        return new FixedSizeListType(itemField, dim);
                    }
                case "dict":
                    // dict:{value_type}:{index_type}:false — for Phase 1 we
                    // return the value type; dictionary wrapping is an
                    // encoding concern handled by later phases.
                    {
                        int next = tail.IndexOf(':');
                        string valueType = next > 0 ? tail.Substring(0, next) : tail;
                        return ParseLeafType(valueType, fieldName);
                    }
            }
        }

        throw new LanceFormatException(
            $"Field '{fieldName}' has unsupported logical_type '{logical}'.");
    }

    private static IArrowType ParseDecimal(string tail, string logical, string fieldName)
    {
        // "128:{precision}:{scale}" or "256:{precision}:{scale}"
        string[] parts = tail.Split(':');
        if (parts.Length != 3)
            throw new LanceFormatException(
                $"Field '{fieldName}' has malformed decimal logical_type '{logical}'.");

        if (!int.TryParse(parts[1], out int precision) ||
            !int.TryParse(parts[2], out int scale))
            throw new LanceFormatException(
                $"Field '{fieldName}' has non-numeric decimal precision/scale in '{logical}'.");

        return parts[0] switch
        {
            "128" => new Decimal128Type(precision, scale),
            "256" => new Decimal256Type(precision, scale),
            _ => throw new LanceFormatException(
                $"Field '{fieldName}' has unsupported decimal width '{parts[0]}' in '{logical}'."),
        };
    }

    private static IArrowType ParseTime(string tail, string logical, string fieldName)
    {
        var unit = ParseTimeUnit(tail, logical, fieldName);
        return unit switch
        {
            TimeUnit.Second or TimeUnit.Millisecond => new Time32Type(unit),
            _ => new Time64Type(unit),
        };
    }

    private static IArrowType ParseTimestamp(string tail, string logical, string fieldName)
    {
        // Lance timestamp logical_type can be either "timestamp:{unit}" or
        // "timestamp:{unit}:{tz}". pylance always writes the three-segment
        // form with "-" as the placeholder for "no timezone" (e.g.
        // "timestamp:us:-"); other timezones surface as IANA strings like
        // "timestamp:us:UTC" or "timestamp:us:America/New_York".
        int sep = tail.IndexOf(':');
        string unitStr = sep < 0 ? tail : tail.Substring(0, sep);
        string? tz = null;
        if (sep >= 0)
        {
            string rawTz = tail.Substring(sep + 1);
            if (rawTz != "-" && rawTz.Length > 0)
                tz = rawTz;
        }
        var unit = ParseTimeUnit(unitStr, logical, fieldName);
        return new TimestampType(unit, timezone: tz);
    }

    private static IArrowType ParseDuration(string tail, string logical, string fieldName)
    {
        // DurationType in Apache.Arrow exposes only static instances; its
        // constructor is private.
        return ParseTimeUnit(tail, logical, fieldName) switch
        {
            TimeUnit.Second => DurationType.Second,
            TimeUnit.Millisecond => DurationType.Millisecond,
            TimeUnit.Microsecond => DurationType.Microsecond,
            TimeUnit.Nanosecond => DurationType.Nanosecond,
            _ => throw new LanceFormatException(
                $"Field '{fieldName}' has unsupported duration unit in '{logical}'."),
        };
    }

    private static TimeUnit ParseTimeUnit(string unit, string logical, string fieldName) =>
        unit switch
        {
            "s" => TimeUnit.Second,
            "ms" => TimeUnit.Millisecond,
            "us" => TimeUnit.Microsecond,
            "ns" => TimeUnit.Nanosecond,
            _ => throw new LanceFormatException(
                $"Field '{fieldName}' has unsupported time unit in '{logical}'."),
        };

    private static IReadOnlyDictionary<string, string>? ConvertMetadata(
        MapField<string, ByteString> lanceMetadata)
    {
        if (lanceMetadata == null || lanceMetadata.Count == 0)
            return null;

        var dict = new Dictionary<string, string>(lanceMetadata.Count);
        foreach (var kv in lanceMetadata)
            dict[kv.Key] = kv.Value.ToStringUtf8();
        return dict;
    }
}

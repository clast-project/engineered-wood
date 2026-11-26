// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;

namespace EngineeredWood.Avro.Schema;

/// <summary>
/// Leaves records with no fields out of a schema before it is read into Arrow. Apache.Arrow refuses a struct
/// of no fields, so such a record has no Arrow type; Iceberg writes one for every unpartitioned table's
/// <c>partition</c>. A record with no fields is encoded as zero bytes, so a field of that type can be dropped
/// from the writer's and the reader's schema alike without moving any other value on the wire, and nothing is
/// lost but the field itself. A record whose fields are all such records is empty in the same way.
/// </summary>
/// <remarks>
/// Only a record's field is left out. An empty record as an array's items, a map's values or a union's branch
/// is kept, since dropping it would lose the element count or, in a union, which branch the value took; the
/// Arrow conversion refuses it if it is read, and a reader schema that leaves its field out reads the rest.
/// </remarks>
internal static class EmptyRecords
{
    private const int MaxSchemaDepth = 64;

    /// <summary>
    /// <paramref name="record"/> with every field of an empty record type left out, or the same instance when
    /// it has none.
    /// </summary>
    public static AvroRecordSchema Strip(AvroRecordSchema record)
    {
        if (!Contains(record, new HashSet<AvroSchemaNode>(ReferenceComparer.Instance)))
            return record;
        return (AvroRecordSchema)Strip(record, 0);
    }

    /// <summary>Whether a value of <paramref name="node"/> is a record with nothing in it.</summary>
    public static bool IsEmpty(AvroSchemaNode node) => IsEmpty(node, 0);

    private static bool IsEmpty(AvroSchemaNode node, int depth)
    {
        if (node is not AvroRecordSchema r)
            return false;
        // A record that refers to itself is never empty: it has no Arrow type for other reasons, which the
        // converter reports.
        if (depth >= MaxSchemaDepth)
            return false;
        foreach (var f in r.Fields)
        {
            if (!IsEmpty(f.Schema, depth + 1))
                return false;
        }
        return true;
    }

    private static bool Contains(AvroSchemaNode node, HashSet<AvroSchemaNode> seen)
    {
        if (!seen.Add(node))
            return false;
        switch (node)
        {
            case AvroRecordSchema r:
                if (r.Fields.Count == 0)
                    return true;
                foreach (var f in r.Fields)
                {
                    if (Contains(f.Schema, seen))
                        return true;
                }
                return false;
            case AvroArraySchema a:
                return Contains(a.Items, seen);
            case AvroMapSchema m:
                return Contains(m.Values, seen);
            case AvroUnionSchema u:
                foreach (var b in u.Branches)
                {
                    if (Contains(b, seen))
                        return true;
                }
                return false;
            default:
                return false;
        }
    }

    private static AvroSchemaNode Strip(AvroSchemaNode node, int depth)
    {
        if (depth >= MaxSchemaDepth)
            throw new NotSupportedException(
                "Avro schema exceeds maximum nesting depth. Recursive schemas cannot be represented in Arrow.");

        switch (node)
        {
            case AvroRecordSchema r:
            {
                bool changed = false;
                var fields = new List<AvroFieldNode>(r.Fields.Count);
                foreach (var f in r.Fields)
                {
                    if (IsEmpty(f.Schema))
                    {
                        changed = true;
                        continue;
                    }

                    var schema = Strip(f.Schema, depth + 1);
                    if (ReferenceEquals(schema, f.Schema))
                    {
                        fields.Add(f);
                        continue;
                    }

                    changed = true;
                    fields.Add(new AvroFieldNode(f.Name, schema)
                    {
                        Aliases = f.Aliases,
                        Doc = f.Doc,
                        Default = f.Default,
                        Properties = f.Properties,
                    });
                }

                if (!changed)
                    return r;
                return new AvroRecordSchema(r.Name, r.Namespace, fields)
                {
                    Doc = r.Doc,
                    Aliases = r.Aliases,
                    LogicalType = r.LogicalType,
                    Properties = r.Properties,
                };
            }

            case AvroArraySchema a:
            {
                var items = Strip(a.Items, depth + 1);
                return ReferenceEquals(items, a.Items)
                    ? a
                    : new AvroArraySchema(items) { LogicalType = a.LogicalType, Properties = a.Properties };
            }

            case AvroMapSchema m:
            {
                var values = Strip(m.Values, depth + 1);
                return ReferenceEquals(values, m.Values)
                    ? m
                    : new AvroMapSchema(values) { LogicalType = m.LogicalType, Properties = m.Properties };
            }

            case AvroUnionSchema u:
            {
                bool changed = false;
                var branches = new AvroSchemaNode[u.Branches.Count];
                for (int i = 0; i < branches.Length; i++)
                {
                    branches[i] = Strip(u.Branches[i], depth + 1);
                    changed |= !ReferenceEquals(branches[i], u.Branches[i]);
                }
                return changed
                    ? new AvroUnionSchema(branches) { LogicalType = u.LogicalType, Properties = u.Properties }
                    : u;
            }

            default:
                return node;
        }
    }

    private sealed class ReferenceComparer : IEqualityComparer<AvroSchemaNode>
    {
        public static readonly ReferenceComparer Instance = new();
        public bool Equals(AvroSchemaNode? x, AvroSchemaNode? y) => ReferenceEquals(x, y);
        public int GetHashCode(AvroSchemaNode obj) => RuntimeHelpers.GetHashCode(obj);
    }
}

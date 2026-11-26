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
    /// <remarks>
    /// A named record can be referred to many times, so both the emptiness check and the rewrite are memoized by
    /// node: walking the tree afresh at each reference is exponential in how deeply such references nest, and a
    /// shared record stays shared once rewritten.
    /// </remarks>
    public static AvroRecordSchema Strip(AvroRecordSchema record)
    {
        var context = new Context();
        if (!context.ContainsEmpty(record))
            return record;
        return (AvroRecordSchema)context.Strip(record, 0);
    }

    private sealed class Context
    {
        private readonly Dictionary<AvroSchemaNode, bool> _isEmpty = new(ReferenceComparer.Instance);
        private readonly Dictionary<AvroSchemaNode, bool> _containsEmpty = new(ReferenceComparer.Instance);
        private readonly Dictionary<AvroSchemaNode, AvroSchemaNode> _stripped = new(ReferenceComparer.Instance);

        /// <summary>Whether a value of <paramref name="node"/> is a record with nothing in it.</summary>
        /// <remarks>
        /// A recursive reference is a record with no fields of its own, but its values are whole records, so it
        /// is never empty; the converter refuses it as recursion.
        /// </remarks>
        public bool IsEmpty(AvroSchemaNode node, int depth = 0)
        {
            if (node is not AvroRecordSchema r || r.IsRecursiveReference || depth >= MaxSchemaDepth)
                return false;
            if (_isEmpty.TryGetValue(r, out bool known))
                return known;

            bool empty = true;
            foreach (var f in r.Fields)
            {
                if (!IsEmpty(f.Schema, depth + 1))
                {
                    empty = false;
                    break;
                }
            }
            _isEmpty[r] = empty;
            return empty;
        }

        public bool ContainsEmpty(AvroSchemaNode node)
        {
            if (_containsEmpty.TryGetValue(node, out bool known))
                return known;
            _containsEmpty[node] = false; // a placeholder while this node is walked

            bool contains = node switch
            {
                AvroRecordSchema { IsRecursiveReference: true } => false,
                AvroRecordSchema r => r.Fields.Count == 0 || r.Fields.Any(f => ContainsEmpty(f.Schema)),
                AvroArraySchema a => ContainsEmpty(a.Items),
                AvroMapSchema m => ContainsEmpty(m.Values),
                AvroUnionSchema u => u.Branches.Any(ContainsEmpty),
                _ => false,
            };
            _containsEmpty[node] = contains;
            return contains;
        }

        public AvroSchemaNode Strip(AvroSchemaNode node, int depth)
        {
            if (depth >= MaxSchemaDepth)
                throw new NotSupportedException(
                    "Avro schema exceeds maximum nesting depth. Recursive schemas cannot be represented in Arrow.");
            if (!ContainsEmpty(node))
                return node;
            if (_stripped.TryGetValue(node, out var done))
                return done;

            var result = StripUncached(node, depth);
            _stripped[node] = result;
            return result;
        }

        private AvroSchemaNode StripUncached(AvroSchemaNode node, int depth)
        {
            switch (node)
            {
                case AvroRecordSchema r:
                {
                    var fields = new List<AvroFieldNode>(r.Fields.Count);
                    foreach (var f in r.Fields)
                    {
                        if (IsEmpty(f.Schema))
                            continue;

                        var schema = Strip(f.Schema, depth + 1);
                        fields.Add(ReferenceEquals(schema, f.Schema)
                            ? f
                            : new AvroFieldNode(f.Name, schema)
                            {
                                Aliases = f.Aliases,
                                Doc = f.Doc,
                                Default = f.Default,
                                Properties = f.Properties,
                            });
                    }

                    return new AvroRecordSchema(r.Name, r.Namespace, fields)
                    {
                        Doc = r.Doc,
                        Aliases = r.Aliases,
                        LogicalType = r.LogicalType,
                        Properties = r.Properties,
                    };
                }

                case AvroArraySchema a:
                    return new AvroArraySchema(Strip(a.Items, depth + 1)) { LogicalType = a.LogicalType, Properties = a.Properties };

                case AvroMapSchema m:
                    return new AvroMapSchema(Strip(m.Values, depth + 1)) { LogicalType = m.LogicalType, Properties = m.Properties };

                case AvroUnionSchema u:
                {
                    var branches = new AvroSchemaNode[u.Branches.Count];
                    for (int i = 0; i < branches.Length; i++)
                        branches[i] = Strip(u.Branches[i], depth + 1);
                    return new AvroUnionSchema(branches) { LogicalType = u.LogicalType, Properties = u.Properties };
                }

                default:
                    return node;
            }
        }
    }

    private sealed class ReferenceComparer : IEqualityComparer<AvroSchemaNode>
    {
        public static readonly ReferenceComparer Instance = new();
        public bool Equals(AvroSchemaNode? x, AvroSchemaNode? y) => ReferenceEquals(x, y);
        public int GetHashCode(AvroSchemaNode obj) => RuntimeHelpers.GetHashCode(obj);
    }
}

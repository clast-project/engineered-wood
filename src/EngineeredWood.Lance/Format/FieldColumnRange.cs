// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Lance.Proto;
using EngineeredWood.Lance.Schema;

namespace EngineeredWood.Lance.Format;

/// <summary>
/// Maps a top-level Arrow field (as exposed by
/// <see cref="Apache.Arrow.Schema.FieldsList"/>) to the contiguous range of
/// physical columns in the Lance file that carry its data.
///
/// <para>Lance stores each <see cref="Proto.Field"/> as one physical column
/// in schema declaration order. A top-level struct with two leaves becomes
/// three columns; a list of primitives becomes two; a fixed-size-list stays
/// a single column because its items are encoded inline.</para>
/// </summary>
internal readonly record struct FieldColumnRange(int StartColumn, int ColumnCount)
{
    public int EndColumn => StartColumn + ColumnCount;

    public static FieldColumnRange[] BuildFromSchema(Proto.Schema protoSchema)
    {
        var allIds = new HashSet<int>();
        foreach (var f in protoSchema.Fields) allIds.Add(f.Id);

        var result = new List<FieldColumnRange>();
        int i = 0;
        while (i < protoSchema.Fields.Count)
        {
            if (!LanceSchemaConverter.IsRoot(protoSchema.Fields[i].ParentId, allIds))
                throw new LanceFormatException(
                    $"Field at schema index {i} has parent_id {protoSchema.Fields[i].ParentId} " +
                    "but no preceding root field to attach to.");

            int start = i;
            int j = i + 1;
            while (j < protoSchema.Fields.Count
                   && !LanceSchemaConverter.IsRoot(protoSchema.Fields[j].ParentId, allIds))
                j++;

            result.Add(new FieldColumnRange(start, j - start));
            i = j;
        }
        return result.ToArray();
    }
}

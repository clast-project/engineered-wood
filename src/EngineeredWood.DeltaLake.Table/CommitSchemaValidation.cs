// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Checks a schema this library is about to COMMIT, on every path that commits one.
/// </summary>
/// <remarks>
/// <para>Two checks, and both must run BEFORE the commit:</para>
/// <list type="bullet">
/// <item>The schema must convert to Arrow. That conversion is what building the next snapshot does,
/// so a schema it refuses (a decimal precision above 38, say) would otherwise be committed and then
/// leave the table unopenable. The callers that take a Delta schema directly
/// (<c>preAssignedSchema</c>, <c>AddColumnAsync(StructField)</c>) never pass through the
/// Arrow-to-Delta conversion that refuses such types on the way in.</item>
/// <item>No two fields of one struct may differ only in case. Delta column names are
/// case-insensitive: Spark refuses such a schema (<c>DELTA_DUPLICATE_COLUMNS_FOUND</c>) and cannot
/// read a table that has one. Checked at every depth, including structs inside array elements and
/// map keys/values.</item>
/// </list>
/// <para>Reading a table that already breaks either rule is left alone; the point is not to
/// produce one.</para>
/// </remarks>
internal static class CommitSchemaValidation
{
    public static void EnsureValid(StructType schema)
    {
        SchemaConverter.ToArrowSchema(schema);
        CheckNames(schema.Fields, prefix: "");
    }

    private static void CheckNames(IReadOnlyList<StructField> fields, string prefix)
    {
        var seen = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var field in fields)
        {
            if (seen.TryGetValue(field.Name, out var first))
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.DuplicateColumnsFound,
                    $"Columns '{prefix}{first}' and '{prefix}{field.Name}' differ only in case. Delta column "
                    + "names are case-insensitive, so a schema cannot hold both.");
            }
            seen.Add(field.Name, field.Name);
            CheckNames(field.Type, prefix + field.Name);
        }
    }

    private static void CheckNames(DeltaDataType type, string path)
    {
        switch (type)
        {
            case StructType st:
                CheckNames(st.Fields, path + ".");
                break;
            case ArrayType at:
                CheckNames(at.ElementType, path + ".element");
                break;
            case MapType mt:
                CheckNames(mt.KeyType, path + ".key");
                CheckNames(mt.ValueType, path + ".value");
                break;
        }
    }
}

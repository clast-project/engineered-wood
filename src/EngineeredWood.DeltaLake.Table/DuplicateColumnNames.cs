// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Refuses a schema in which two fields of one struct differ only in case.
/// </summary>
/// <remarks>
/// Delta column names are case-insensitive: Spark refuses such a schema
/// (<c>DELTA_DUPLICATE_COLUMNS_FOUND</c>) and cannot read a table that has one. The check runs at
/// every depth, including structs inside array elements and map keys/values, and only on schemas
/// this library is about to COMMIT — reading a table that already has duplicates is left alone.
/// </remarks>
internal static class DuplicateColumnNames
{
    public static void EnsureNone(StructType schema) => Check(schema.Fields, prefix: "");

    private static void Check(IReadOnlyList<StructField> fields, string prefix)
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
            Check(field.Type, prefix + field.Name);
        }
    }

    private static void Check(DeltaDataType type, string path)
    {
        switch (type)
        {
            case StructType st:
                Check(st.Fields, path + ".");
                break;
            case ArrayType at:
                Check(at.ElementType, path + ".element");
                break;
            case MapType mt:
                Check(mt.KeyType, path + ".key");
                Check(mt.ValueType, path + ".value");
                break;
        }
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake;

/// <summary>
/// Which <c>add.partitionValues</c> strings mean SQL NULL. The read path and the pruner must agree on this: a
/// pruner that read such a value as a value skipped the files an <c>IS NULL</c> scan or DELETE had to see (#511).
/// <list type="bullet">
/// <item>JSON null, as the spec and Spark write it.</item>
/// <item>An empty string, for every type: the protocol says so ("An empty string for any type translates to a
/// null partition value"), delta-rs reads it so, and Spark writes an empty string as null.</item>
/// <item>Hive's <c>__HIVE_DEFAULT_PARTITION__</c> directory sentinel, for a column that is not a string. Some
/// writers put it into the map; Spark's read casts it to NULL, as it does any text that is not a value of the
/// type. For a string column it is the text itself, as Spark and Delta Kernel read it: the protocol knows the
/// sentinel only as a directory name.</item>
/// </list>
/// </summary>
internal static class PartitionNulls
{
    /// <summary>Hive's directory name for a null partition value.</summary>
    public const string HiveDefaultPartition = "__HIVE_DEFAULT_PARTITION__";

    /// <summary>Whether <paramref name="value"/>, stored for a column of string type or not as
    /// <paramref name="isString"/> says, is NULL.</summary>
    public static bool IsNull(string? value, bool isString) =>
        value is null || value.Length == 0 || (!isString && value == HiveDefaultPartition);
}

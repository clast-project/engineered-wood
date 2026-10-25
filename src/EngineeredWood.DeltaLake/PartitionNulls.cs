// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake;

/// <summary>
/// Which <c>add.partitionValues</c> strings mean SQL NULL. The spec writes a null partition value as JSON null,
/// but some writers put Hive's <c>__HIVE_DEFAULT_PARTITION__</c> directory sentinel into the map, and an empty
/// string cannot be a value of any type but string. The read path and the pruner must agree on this: a pruner
/// that read the sentinel as a value skipped the files an <c>IS NULL</c> scan or DELETE had to see (#511).
/// </summary>
internal static class PartitionNulls
{
    /// <summary>Hive's directory name for a null partition value.</summary>
    public const string HiveDefaultPartition = "__HIVE_DEFAULT_PARTITION__";

    /// <summary>Whether <paramref name="value"/>, stored for a column of string type or not as
    /// <paramref name="isString"/> says, is NULL.</summary>
    public static bool IsNull(string? value, bool isString) =>
        value is null || value == HiveDefaultPartition || (value.Length == 0 && !isString);
}

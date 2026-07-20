// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.ChangeDataFeed;

/// <summary>
/// Configuration and constants for Change Data Feed.
/// </summary>
public static class CdfConfig
{
    /// <summary>Table property to enable Change Data Feed.</summary>
    public const string EnableKey = "delta.enableChangeDataFeed";

    /// <summary>Column name for the change type in CDC files.</summary>
    public const string ChangeTypeColumn = "_change_type";

    /// <summary>Column name for the commit version in CDC output.</summary>
    public const string CommitVersionColumn = "_commit_version";

    /// <summary>Column name for the commit timestamp in CDC output.</summary>
    public const string CommitTimestampColumn = "_commit_timestamp";

    /// <summary>Directory for CDC files relative to the table root.</summary>
    public const string ChangeDataDir = "_change_data";

    /// <summary>Change type value for inserted rows.</summary>
    public const string Insert = "insert";

    /// <summary>Change type value for the row state before an update.</summary>
    public const string UpdatePreimage = "update_preimage";

    /// <summary>Change type value for the row state after an update.</summary>
    public const string UpdatePostimage = "update_postimage";

    /// <summary>Change type value for deleted rows.</summary>
    public const string Delete = "delete";

    /// <summary>
    /// Returns true if Change Data Feed is enabled for the table.
    /// </summary>
    public static bool IsEnabled(IReadOnlyDictionary<string, string>? configuration) =>
        configuration is not null &&
        configuration.TryGetValue(EnableKey, out string? value) &&
        string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
}

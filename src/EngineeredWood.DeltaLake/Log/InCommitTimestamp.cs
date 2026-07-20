// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.DeltaLake.Actions;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>
/// Utilities for in-commit timestamp support.
/// When enabled, the <c>commitInfo</c> action must be the first action
/// in every commit and must include an <c>inCommitTimestamp</c> field
/// (milliseconds since epoch).
/// </summary>
public static class InCommitTimestamp
{
    /// <summary>Table configuration key to enable in-commit timestamps.</summary>
    public const string EnableKey = "delta.enableInCommitTimestamps";

    /// <summary>
    /// Returns true if in-commit timestamps are enabled for the table.
    /// </summary>
    public static bool IsEnabled(IReadOnlyDictionary<string, string>? configuration)
    {
        if (configuration is null)
            return false;

        return configuration.TryGetValue(EnableKey, out string? value) &&
               string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Creates a <see cref="CommitInfo"/> action with the <c>inCommitTimestamp</c> field
    /// set to the current time.
    /// </summary>
    public static CommitInfo CreateCommitInfo(
        string operation = "WRITE",
        IDictionary<string, JsonElement>? additionalValues = null)
    {
        long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return CreateCommitInfo(timestamp, operation, additionalValues);
    }

    /// <summary>
    /// Creates a <see cref="CommitInfo"/> action with the specified timestamp.
    /// </summary>
    public static CommitInfo CreateCommitInfo(
        long timestampMs,
        string operation = "WRITE",
        IDictionary<string, JsonElement>? additionalValues = null,
        bool includeInCommitTimestamp = true)
    {
        var values = new Dictionary<string, JsonElement>();

        if (additionalValues is not null)
        {
            foreach (var kvp in additionalValues)
                values[kvp.Key] = kvp.Value;
        }

        // `timestamp` + `operation` are standard, feature-free commitInfo fields (every Spark/delta-rs commit
        // writes them) — safe on a plain (writer-v2) table. The `inCommitTimestamp` field is the opt-in
        // monotonic timestamp and is written ONLY when the inCommitTimestamps feature is declared (writing it
        // without the feature would imply a feature the protocol doesn't list).
        values["timestamp"] = JsonDocument.Parse(
            timestampMs.ToString()).RootElement.Clone();
        values["operation"] = JsonDocument.Parse(
            $"\"{operation}\"").RootElement.Clone();
        // Standard informational fields Spark/delta-rs write on every commit: engineInfo identifies the
        // writer (surfaced by history views); operationParameters must at least be an empty object for
        // history tooling that expects the key.
        if (!values.ContainsKey("engineInfo"))
        {
            values["engineInfo"] = JsonDocument.Parse(
                "\"EngineeredWood.DeltaLake\"").RootElement.Clone();
        }
        if (!values.ContainsKey("operationParameters"))
        {
            values["operationParameters"] = JsonDocument.Parse("{}").RootElement.Clone();
        }
        if (includeInCommitTimestamp)
        {
            values["inCommitTimestamp"] = JsonDocument.Parse(
                timestampMs.ToString()).RootElement.Clone();
        }

        return new CommitInfo { Values = values };
    }

    /// <summary>
    /// Extracts the <c>inCommitTimestamp</c> from a <see cref="CommitInfo"/> action.
    /// Returns null if not present.
    /// </summary>
    public static long? GetTimestamp(CommitInfo commitInfo)
    {
        // Prefer the in-protocol monotonic inCommitTimestamp (when the feature is enabled); otherwise
        // fall back to commitInfo's standard informational `timestamp` (always written by EnsureCommitInfo),
        // so CDF's _commit_timestamp is populated even without the inCommitTimestamps feature.
        var ts = commitInfo.GetValue("inCommitTimestamp");
        if (ts?.ValueKind == JsonValueKind.Number)
            return ts.Value.GetInt64();
        var legacy = commitInfo.GetValue("timestamp");
        return legacy?.ValueKind == JsonValueKind.Number ? legacy.Value.GetInt64() : null;
    }

    /// <summary>
    /// Extracts the <c>inCommitTimestamp</c> from a list of actions.
    /// The <c>commitInfo</c> should be the first action when in-commit timestamps are enabled.
    /// </summary>
    public static long? GetTimestampFromActions(IReadOnlyList<DeltaAction> actions)
    {
        foreach (var action in actions)
        {
            if (action is CommitInfo ci)
            {
                var ts = GetTimestamp(ci);
                if (ts.HasValue)
                    return ts;
            }
        }

        return null;
    }

    /// <summary>
    /// Prepends a <see cref="CommitInfo"/> (the first action) to a commit, recording <c>operation</c> and a
    /// <c>timestamp</c> — standard, feature-free fields written on EVERY commit so the table has a usable
    /// history (the snapshots/versions view) even on a plain writer-v2 table. The opt-in <c>inCommitTimestamp</c>
    /// field is added only when the table has in-commit timestamps enabled. No-op if a commitInfo is already present.
    /// </summary>
    public static IReadOnlyList<DeltaAction> EnsureCommitInfo(
        IReadOnlyList<DeltaAction> actions,
        IReadOnlyDictionary<string, string>? configuration,
        string operation = "WRITE")
    {
        // Already has a commitInfo (any kind) — leave it.
        foreach (var action in actions)
        {
            if (action is CommitInfo)
                return actions;
        }

        var result = new List<DeltaAction>(actions.Count + 1);
        result.Add(CreateCommitInfo(
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), operation,
            additionalValues: null, includeInCommitTimestamp: IsEnabled(configuration)));
        result.AddRange(actions);
        return result;
    }
}

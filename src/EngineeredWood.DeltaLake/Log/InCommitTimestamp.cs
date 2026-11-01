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
        bool includeInCommitTimestamp = true,
        CommitProperties? commitProperties = null)
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

        // Last, so a collision with anything this layer wrote is refused rather than overwritten.
        commitProperties?.ApplyTo(values);

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
    /// <param name="isBlindAppend">The transaction's own claim about whether it READ anything — Delta's
    /// <c>commitInfo.isBlindAppend</c>. <c>null</c> writes NO field, which is not the same as <c>false</c>,
    /// and which readers do not all resolve the same way: delta-spark takes an absent flag as "not blind"
    /// and examines the commit, while this library's own <see cref="Concurrency.ConflictChecker"/> falls
    /// back to inferring from the commit's shape and reads an adds-only commit as blind. So silence is the
    /// honest answer for a caller that does not track its reads, but a caller that KNOWS it read should
    /// pass <c>false</c> — see <see cref="LogCommitRequest.IsBlindAppend"/>.</param>
    /// <param name="commitProperties">The caller's own commitInfo keys. Added to a commitInfo the actions
    /// already carry, too, since that is the commitInfo this commit will record.</param>
    /// <param name="operationParameters">Written as <c>commitInfo.operationParameters</c>, a map of strings as
    /// Spark writes it. Null writes the empty object every commit carries. A commitInfo the actions already carry
    /// keeps its own.</param>
    /// <param name="operationMetrics">Written as <c>commitInfo.operationMetrics</c>, a map of strings as Spark
    /// writes it. A commitInfo the actions already carry keeps its own if it has one.</param>
    /// <exception cref="ArgumentException"><paramref name="commitProperties"/> names a key the commitInfo
    /// already records.</exception>
    public static IReadOnlyList<DeltaAction> EnsureCommitInfo(
        IReadOnlyList<DeltaAction> actions,
        IReadOnlyDictionary<string, string>? configuration,
        string operation = "WRITE",
        bool? isBlindAppend = null,
        CommitProperties? commitProperties = null,
        IReadOnlyDictionary<string, string>? operationMetrics = null,
        IReadOnlyDictionary<string, string>? operationParameters = null)
    {
        // Already has a commitInfo (any kind): leave it, apart from adding the caller's properties to it.
        for (int i = 0; i < actions.Count; i++)
        {
            if (actions[i] is not CommitInfo existing)
                continue;
            if (commitProperties is null && operationMetrics is null && operationParameters is null)
                return actions;

            var values = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
            foreach (var kvp in existing.Values)
                values[kvp.Key] = kvp.Value;
            if (operationMetrics is not null && !values.ContainsKey("operationMetrics"))
                values["operationMetrics"] = StringMapJson(operationMetrics);
            if (operationParameters is not null && !values.ContainsKey("operationParameters"))
                values["operationParameters"] = StringMapJson(operationParameters);
            commitProperties?.ApplyTo(values);
            var replaced = new List<DeltaAction>(actions);
            replaced[i] = existing with { Values = values };
            return replaced;
        }

        Dictionary<string, JsonElement>? extra = null;
        if (isBlindAppend is { } blind)
        {
            extra = new Dictionary<string, JsonElement>(StringComparer.Ordinal)
            {
                ["isBlindAppend"] = JsonDocument.Parse(blind ? "true" : "false").RootElement.Clone(),
            };
        }
        if (operationMetrics is not null)
        {
            extra ??= new Dictionary<string, JsonElement>(StringComparer.Ordinal);
            extra["operationMetrics"] = StringMapJson(operationMetrics);
        }
        if (operationParameters is not null)
        {
            extra ??= new Dictionary<string, JsonElement>(StringComparer.Ordinal);
            extra["operationParameters"] = StringMapJson(operationParameters);
        }

        var result = new List<DeltaAction>(actions.Count + 1);
        result.Add(CreateCommitInfo(
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), operation,
            additionalValues: extra, includeInCommitTimestamp: IsEnabled(configuration),
            commitProperties: commitProperties));
        result.AddRange(actions);
        return result;
    }

    // A JSON object of string values, which is how Spark writes operationMetrics and operationParameters (each a
    // Map[String, String]).
    private static JsonElement StringMapJson(IReadOnlyDictionary<string, string> metrics)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            foreach (var kvp in metrics)
                writer.WriteString(kvp.Key, kvp.Value);
            writer.WriteEndObject();
        }
        using var document = JsonDocument.Parse(stream.ToArray());
        return document.RootElement.Clone();
    }
}

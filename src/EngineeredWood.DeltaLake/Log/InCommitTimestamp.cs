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
    /// The version of the commit that turned in-commit timestamps on, when that was not the table's creation. Commits
    /// before it carry no <c>inCommitTimestamp</c>; without it a reader assumes every commit does.
    /// </summary>
    public const string EnablementVersionKey = "delta.inCommitTimestampEnablementVersion";

    /// <summary>The <c>inCommitTimestamp</c> of the commit named by <see cref="EnablementVersionKey"/>.</summary>
    public const string EnablementTimestampKey = "delta.inCommitTimestampEnablementTimestamp";

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
    /// The enablement point recorded in <paramref name="configuration"/>, or null when it records none (in-commit
    /// timestamps were on from the table's creation, or are off). Both properties are required; one without the
    /// other, or a value that is not a number, reads as none.
    /// </summary>
    public static (long Version, long Timestamp)? GetEnablement(IReadOnlyDictionary<string, string>? configuration)
    {
        if (configuration is null
            || !configuration.TryGetValue(EnablementVersionKey, out string? version)
            || !configuration.TryGetValue(EnablementTimestampKey, out string? timestamp)
            || !long.TryParse(version, System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture, out long v)
            || !long.TryParse(timestamp, System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture, out long t))
        {
            return null;
        }
        return (v, t);
    }

    /// <summary>
    /// The timestamp a commit of <paramref name="actions"/> on <paramref name="baseSnapshot"/> must stay after: the
    /// snapshot's own, or, when it has none and the commit will be timestamped, read from the base version's commit.
    /// </summary>
    /// <remarks>
    /// A snapshot that ends on a checkpoint has no timestamp unless the table already had in-commit timestamps (the
    /// snapshot builder reads one only then). A commit that turns them on is the case left: its floor is the commit
    /// before it, as Spark's is (<c>snapshot.timestamp</c>). Read only when the commit will carry a timestamp, so
    /// no other commit pays for it. Null when it cannot be read; the timestamp is then the clock's alone.
    /// </remarks>
    internal static async ValueTask<long?> ResolvePreviousTimestampAsync(
        TransactionLog log, Snapshot.Snapshot? baseSnapshot, IReadOnlyList<DeltaAction> actions,
        CancellationToken cancellationToken)
    {
        if (baseSnapshot is null)
            return null;
        if (baseSnapshot.InCommitTimestamp is { } known)
            return known;

        MetadataAction? installed = null;
        foreach (var action in actions)
        {
            if (action is MetadataAction metadata)
                installed = metadata;
        }
        if (!IsEnabled(installed is null ? baseSnapshot.Metadata.Configuration : installed.Configuration))
            return null;

        try
        {
            var previous = await log.ReadCommitAsync(baseSnapshot.Version, cancellationToken).ConfigureAwait(false);
            return GetTimestampFromActions(previous);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            return null;
        }
    }

    /// <summary>
    /// Prepends a <see cref="CommitInfo"/> (the first action) to a commit, recording <c>operation</c> and a
    /// <c>timestamp</c> — standard, feature-free fields written on EVERY commit so the table has a usable
    /// history (the snapshots/versions view) even on a plain writer-v2 table.
    ///
    /// <para><b>In-commit timestamps follow the metadata the commit INSTALLS</b>, as delta-spark's do: a commit whose
    /// <c>metaData</c> enables them carries an <c>inCommitTimestamp</c> even though the table it is based on does
    /// not, and one whose <c>metaData</c> disables them carries none. A commit without <c>metaData</c> follows
    /// <paramref name="configuration"/>. The timestamp is the current time, raised where needed to one past
    /// <paramref name="previousCommitTimestamp"/> so the sequence never goes backwards.</para>
    ///
    /// <para><b>A mid-life enablement is stamped.</b> When the commit's <c>metaData</c> turns in-commit timestamps on,
    /// <paramref name="configuration"/> has them off, and <paramref name="version"/> is not 0, the metadata gets
    /// <see cref="EnablementVersionKey"/> = <paramref name="version"/> and <see cref="EnablementTimestampKey"/> = the
    /// commit's timestamp. Without them a reader assumes every earlier commit carries a timestamp, and Spark's
    /// history and time travel then fail with <c>DELTA_MISSING_COMMIT_TIMESTAMP</c>. Both depend on the version, so
    /// a commit retried at a later version must be prepared again from its staged actions.</para>
    ///
    /// <para>A commitInfo the actions already carry is kept, apart from the caller's properties, metrics and
    /// parameters, and, when in-commit timestamps apply, its <c>inCommitTimestamp</c>: one it lacks is added,
    /// and one at or before <paramref name="previousCommitTimestamp"/> is raised past it.</para>
    /// </summary>
    /// <param name="actions">The commit's actions.</param>
    /// <param name="configuration">The configuration of the table the commit is BASED on: null for a new table.
    /// Not the configuration the commit installs; that comes from its own <c>metaData</c>.</param>
    /// <param name="version">The version the commit is about to be attempted at.</param>
    /// <param name="operation">The commitInfo's <c>operation</c>.</param>
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
    /// <param name="previousCommitTimestamp">The timestamp of the commit at <paramref name="version"/> - 1, when
    /// known: its in-commit timestamp, or its <c>commitInfo.timestamp</c> if it has none. The in-commit timestamp
    /// written here is kept strictly after it.</param>
    /// <exception cref="ArgumentException"><paramref name="commitProperties"/> names a key the commitInfo
    /// already records.</exception>
    public static IReadOnlyList<DeltaAction> EnsureCommitInfo(
        IReadOnlyList<DeltaAction> actions,
        IReadOnlyDictionary<string, string>? configuration,
        long version,
        string operation = "WRITE",
        bool? isBlindAppend = null,
        CommitProperties? commitProperties = null,
        IReadOnlyDictionary<string, string>? operationMetrics = null,
        IReadOnlyDictionary<string, string>? operationParameters = null,
        long? previousCommitTimestamp = null) =>
        EnsureCommitInfo(
            actions, configuration, version, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), operation,
            isBlindAppend, commitProperties, operationMetrics, operationParameters, previousCommitTimestamp);

    /// <summary>
    /// <see cref="EnsureCommitInfo(IReadOnlyList{DeltaAction}, IReadOnlyDictionary{string, string}?, long, string, bool?, CommitProperties?, IReadOnlyDictionary{string, string}?, IReadOnlyDictionary{string, string}?, long?)"/>
    /// with the clock injected.
    /// </summary>
    internal static IReadOnlyList<DeltaAction> EnsureCommitInfo(
        IReadOnlyList<DeltaAction> actions,
        IReadOnlyDictionary<string, string>? configuration,
        long version,
        long nowMs,
        string operation = "WRITE",
        bool? isBlindAppend = null,
        CommitProperties? commitProperties = null,
        IReadOnlyDictionary<string, string>? operationMetrics = null,
        IReadOnlyDictionary<string, string>? operationParameters = null,
        long? previousCommitTimestamp = null)
    {
        // The metadata this commit installs, if any: it decides whether the commit is timestamped, not the base's.
        int metadataIndex = -1;
        for (int i = 0; i < actions.Count; i++)
        {
            if (actions[i] is MetadataAction)
                metadataIndex = i;
        }
        var installed = metadataIndex >= 0 ? (MetadataAction)actions[metadataIndex] : null;
        bool timestamped = IsEnabled(installed is null ? configuration : installed.Configuration);
        // Spark's InCommitTimestampUtils.didCurrentTransactionEnableICT, and its rule that the creating commit
        // (version 0) records no enablement point: every commit of such a table carries a timestamp.
        bool enables = timestamped && installed is not null && !IsEnabled(configuration) && version != 0;

        long Floor(long candidate) =>
            previousCommitTimestamp is { } previous && candidate <= previous ? previous + 1 : candidate;

        // Already has a commitInfo (any kind): keep it, apart from the caller's additions and the timestamp.
        for (int i = 0; i < actions.Count; i++)
        {
            if (actions[i] is not CommitInfo existing)
                continue;
            bool additions = commitProperties is not null || operationMetrics is not null
                || operationParameters is not null;
            if (!timestamped && !additions)
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
            bool changed = additions;
            if (timestamped)
            {
                long? given = values.TryGetValue("inCommitTimestamp", out var raw) && raw.ValueKind == JsonValueKind.Number
                    ? raw.GetInt64()
                    : null;
                long timestamp = Floor(given ?? nowMs);
                if (given != timestamp)
                {
                    values["inCommitTimestamp"] = NumberJson(timestamp);
                    changed = true;
                }
                changed |= ApplyMetadataStamp(
                    replaced, metadataIndex, installed, configuration, enables, version, timestamp);
            }
            // With in-commit timestamps the commitInfo must be the commit's FIRST action: a reader may look for the
            // timestamp there and nowhere else.
            bool move = timestamped && i != 0;
            if (!changed && !move)
                return actions;
            var updated = changed ? existing with { Values = values } : existing;
            if (move)
            {
                replaced.RemoveAt(i);
                replaced.Insert(0, updated);
            }
            else
            {
                replaced[i] = updated;
            }
            return replaced;
        }

        var stamped = new List<DeltaAction>(actions);
        long commitTimestamp = timestamped ? Floor(nowMs) : nowMs;
        if (timestamped)
            ApplyMetadataStamp(stamped, metadataIndex, installed, configuration, enables, version, commitTimestamp);

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
            commitTimestamp, operation,
            additionalValues: extra, includeInCommitTimestamp: timestamped,
            commitProperties: commitProperties));
        result.AddRange(stamped);
        return result;
    }

    // The enablement pair on the installed metadata. A commit that enables in-commit timestamps names itself, over
    // whatever pair its metadata brought along (RESTORE brings back an older version's). A commit that keeps them on
    // and installs metadata without the pair keeps the base's: the commits before the enablement still have no
    // timestamp, and replacing the metadata does not give them one.
    // Returns whether it changed the metadata.
    private static bool ApplyMetadataStamp(
        List<DeltaAction> actions, int metadataIndex, MetadataAction? installed,
        IReadOnlyDictionary<string, string>? baseConfiguration, bool enables, long version, long timestamp)
    {
        if (installed is null)
            return false;

        (long Version, long Timestamp)? pair;
        if (enables)
            pair = (version, timestamp);
        else if (GetEnablement(installed.Configuration) is null && GetEnablement(baseConfiguration) is { } kept)
            pair = kept;
        else
            return false;
        if (GetEnablement(installed.Configuration) == pair)
            return false;

        var configuration = new Dictionary<string, string>(StringComparer.Ordinal);
        if (installed.Configuration is not null)
        {
            foreach (var kvp in installed.Configuration)
                configuration[kvp.Key] = kvp.Value;
        }
        configuration[EnablementVersionKey] =
            pair.Value.Version.ToString(System.Globalization.CultureInfo.InvariantCulture);
        configuration[EnablementTimestampKey] =
            pair.Value.Timestamp.ToString(System.Globalization.CultureInfo.InvariantCulture);
        actions[metadataIndex] = installed with { Configuration = configuration };
        return true;
    }

    private static JsonElement NumberJson(long value)
    {
        using var document = JsonDocument.Parse(value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        return document.RootElement.Clone();
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

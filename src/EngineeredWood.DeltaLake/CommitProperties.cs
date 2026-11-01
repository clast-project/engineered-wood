// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Collections.ObjectModel;
using System.Text.Json;

namespace EngineeredWood.DeltaLake;

/// <summary>
/// The caller's own metadata for a commit, written into its <c>commitInfo</c> action: a job id, a source
/// watermark, the user who triggered the run. Pipelines read it back through the table's history for lineage
/// and auditing.
///
/// <para><see cref="UserMetadata"/> is the field Spark writes from
/// <c>spark.databricks.delta.commitInfo.userMetadata</c> (or the <c>userMetadata</c> write option), and
/// Spark's <c>DESCRIBE HISTORY</c> has a column for it. <see cref="Metadata"/> carries any other keys, the
/// way delta-rs's <c>CommitProperties::with_metadata</c> does.</para>
/// </summary>
/// <remarks>
/// A key in <see cref="Metadata"/> may not be one of <see cref="ReservedKeys"/>: the fields this library
/// writes itself, and the rest of Spark's typed <c>CommitInfo</c> fields. Such a key is refused rather than
/// allowed to shadow or be shadowed. Spark deserializes those fields by type, so a <c>readVersion</c> holding
/// a string, say, would stop Spark from reading the table's history at all. Keys outside that set are
/// ignored by Spark and kept by every reader that keeps unknown fields.
/// </remarks>
public sealed class CommitProperties
{
    /// <summary>
    /// The <c>commitInfo</c> keys <see cref="Metadata"/> may not use: every field of delta-spark's
    /// <c>CommitInfo</c> (read from the delta-spark 4.0.0 jar). This library writes several of them itself
    /// (<c>timestamp</c>, <c>inCommitTimestamp</c>, <c>operation</c>, <c>operationParameters</c>,
    /// <c>operationMetrics</c>, <c>isBlindAppend</c>, <c>engineInfo</c>, <c>userMetadata</c>), and Spark
    /// reads every one of them as a typed value.
    /// </summary>
    /// <remarks>A read-only view: the set the validation consults is private, so nothing a caller does to this
    /// collection can change what is refused.</remarks>
    public static IReadOnlyCollection<string> ReservedKeys { get; }

    private static readonly HashSet<string> Reserved = new(StringComparer.Ordinal)
    {
        "version",
        "inCommitTimestamp",
        "timestamp",
        "userId",
        "userName",
        "operation",
        "operationParameters",
        "job",
        "notebook",
        "clusterId",
        "readVersion",
        "isolationLevel",
        "isBlindAppend",
        "operationMetrics",
        "userMetadata",
        "tags",
        "engineInfo",
        "txnId",
    };

    static CommitProperties()
    {
        var keys = new string[Reserved.Count];
        Reserved.CopyTo(keys);
        ReservedKeys = Array.AsReadOnly(keys);
    }

    private readonly IReadOnlyDictionary<string, JsonElement>? _metadata;

    /// <summary>
    /// Written as <c>commitInfo.userMetadata</c>, the way Spark writes it. Null writes no field.
    /// </summary>
    public string? UserMetadata { get; init; }

    /// <summary>
    /// Further <c>commitInfo</c> keys, written as given. The dictionary is copied when it is set, so changing
    /// it afterwards has no effect on these properties, and the copy is read-only, so the keys cannot be changed
    /// past the validation either.
    /// </summary>
    /// <exception cref="ArgumentException">A key is one of <see cref="ReservedKeys"/>, or is empty.</exception>
    public IReadOnlyDictionary<string, JsonElement>? Metadata
    {
        get => _metadata;
        init
        {
            if (value is null)
            {
                _metadata = null;
                return;
            }

            var copy = new Dictionary<string, JsonElement>(value.Count, StringComparer.Ordinal);
            foreach (var kvp in value)
            {
                if (string.IsNullOrEmpty(kvp.Key))
                    throw new ArgumentException("A commitInfo key may not be empty.", nameof(Metadata));
                if (Reserved.Contains(kvp.Key))
                {
                    throw new ArgumentException(
                        $"'{kvp.Key}' is a commitInfo field this library or Spark defines, so it cannot be set " +
                        "as extra commit metadata." +
                        (kvp.Key == "userMetadata" ? $" Set {nameof(UserMetadata)} instead." : ""),
                        nameof(Metadata));
                }

                // Clone: an element still tied to its JsonDocument is unusable once the caller disposes it.
                copy[kvp.Key] = kvp.Value.Clone();
            }
            _metadata = new ReadOnlyDictionary<string, JsonElement>(copy);
        }
    }

    /// <summary>
    /// Adds these properties to a <c>commitInfo</c> value map. A key already present is refused: this layer
    /// never overwrites what the commit itself recorded, and never lets the commit overwrite the caller's.
    /// </summary>
    internal void ApplyTo(IDictionary<string, JsonElement> values)
    {
        if (UserMetadata is { } userMetadata)
            Add(values, "userMetadata", JsonSerializedString(userMetadata));

        if (_metadata is not null)
        {
            foreach (var kvp in _metadata)
                Add(values, kvp.Key, kvp.Value);
        }

        static void Add(IDictionary<string, JsonElement> values, string key, JsonElement value)
        {
            if (values.ContainsKey(key))
            {
                throw new ArgumentException(
                    $"The commit already records commitInfo.{key}, so the commit properties cannot set it.");
            }
            values[key] = value;
        }
    }

    private static JsonElement JsonSerializedString(string value)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
            writer.WriteStringValue(value);
        using var document = JsonDocument.Parse(stream.ToArray());
        return document.RootElement.Clone();
    }
}

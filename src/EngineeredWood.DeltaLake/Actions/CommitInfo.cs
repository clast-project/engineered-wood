// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

namespace EngineeredWood.DeltaLake.Actions;

/// <summary>
/// Optional provenance information about the operation that produced
/// this commit. The content is open-ended: arbitrary key-value pairs
/// are preserved as <see cref="JsonElement"/> values.
/// </summary>
public sealed record CommitInfo : DeltaAction
{
    /// <summary>
    /// All key-value pairs in the commitInfo JSON object.
    /// Common keys include <c>operation</c>, <c>operationParameters</c>,
    /// <c>timestamp</c>, <c>userId</c>, <c>userName</c>, <c>engineInfo</c>,
    /// <c>txnId</c>, and <c>inCommitTimestamp</c>.
    /// </summary>
    public required IReadOnlyDictionary<string, JsonElement> Values { get; init; }

    /// <summary>Gets a value by key, or null if not present.</summary>
    public JsonElement? GetValue(string key) =>
        Values.TryGetValue(key, out var value) ? value : null;
}

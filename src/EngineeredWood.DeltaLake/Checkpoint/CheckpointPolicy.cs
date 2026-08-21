// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Checkpoint;

/// <summary>
/// The <c>delta.checkpointPolicy</c> table property: the knob that says which checkpoint spec a table
/// wants, as distinct from the <c>v2Checkpoint</c> table feature, which says which it is ALLOWED.
/// </summary>
/// <remarks>
/// The property is not in PROTOCOL.md — the spec describes the two checkpoint specs and the feature that
/// permits V2, and stops there, leaving the choice to the writer. delta-spark settled it with this
/// property, and it is what a table carries in practice, so honoring it is how a table maintained by more
/// than one engine keeps one answer.
/// </remarks>
internal static class CheckpointPolicy
{
    /// <summary>The table property key.</summary>
    public const string PropertyKey = "delta.checkpointPolicy";

    /// <summary>The property value that selects V2 spec checkpoints.</summary>
    public const string V2Value = "v2";

    /// <summary>The table feature that must be enabled before a V2 checkpoint may be written.</summary>
    public const string FeatureName = "v2Checkpoint";

    /// <summary>Whether this table's configuration asks for V2 spec checkpoints.</summary>
    public static bool WantsV2(IReadOnlyDictionary<string, string>? configuration) =>
        configuration is not null &&
        configuration.TryGetValue(PropertyKey, out string? policy) &&
        string.Equals(policy, V2Value, StringComparison.OrdinalIgnoreCase);
}

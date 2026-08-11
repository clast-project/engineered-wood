// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;

namespace EngineeredWood.DeltaLake.Checkpoint;

/// <summary>
/// The <c>delta.checkpointInterval</c> table property: how often a table says it wants checkpointing,
/// as distinct from <see cref="CheckpointPolicy"/>, which says which SPEC those checkpoints take.
/// </summary>
/// <remarks>
/// A table's own statement about a cost it pays per commit and an object count another engine may be
/// tuning deliberately. The value is STORED by every writer that accepts the property, so ignoring it is
/// not neutral — it is reading someone else's declaration and then doing something else.
/// </remarks>
internal static class CheckpointIntervalProperty
{
    /// <summary>The table property key.</summary>
    public const string PropertyKey = "delta.checkpointInterval";

    /// <summary>
    /// The interval this table's configuration declares, or null when it declares none usable.
    /// </summary>
    /// <remarks>
    /// A malformed or non-positive value reads as "declares none" rather than throwing: this is a
    /// declaration read from a table someone else may have written, and refusing to OPEN a table over a
    /// bad property would turn a typo in someone's <c>set_tblproperties</c> into an unreadable table.
    /// Zero is not honoured from the property either — "never checkpoint" is a decision a caller takes,
    /// not one a table takes for the engines reading it.
    /// </remarks>
    public static int? TryGet(IReadOnlyDictionary<string, string>? configuration)
    {
        if (configuration is null || !configuration.TryGetValue(PropertyKey, out string? raw))
            return null;

        return int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed)
               && parsed > 0
            ? parsed
            : null;
    }
}

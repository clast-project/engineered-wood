// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake;

/// <summary>
/// The <c>delta.isolationLevel</c> table property: the isolation level the table demands of every
/// data-changing commit, as distinct from the level a caller asks a transaction for.
/// </summary>
/// <remarks>
/// <para>Delta's rule (delta-spark 4.4.0, <c>TransactionHelper.getDefaultIsolationLevel</c> /
/// <c>OptimisticTransactionImpl.getIsolationLevelToUse</c>): every commit runs at the table's level unless it
/// changes no data, in which case it may drop to snapshot isolation. Spark only lets <c>Serializable</c> be SET, but
/// parses all three of its level names, case-insensitively, when it reads one.</para>
/// <para><b>Absent is NOT Spark's default here.</b> delta-spark builds the config with default value
/// <c>Serializable</c> and <c>fromMap</c> falls back to it, so OSS delta-spark runs every data-changing commit at
/// Serializable whether or not the property is set; <c>WriteSerializable</c> is Databricks' default. This
/// library reads absence as <see cref="IsolationLevel.WriteSerializable"/>, its long-standing default, and that is a
/// decision rather than an oversight: Serializable by default would also switch off row-level concurrency (the
/// deletion-vector union) for every table that never asked for it. A table wanting Spark's behaviour sets the
/// property.</para>
/// <para><c>SnapshotIsolation</c> as a TABLE's level is read as <see cref="IsolationLevel.WriteSerializable"/>.
/// Spark's setter refuses it, so only another engine could have written it, and running stricter than it asks
/// costs conflicts, never correctness; adding a third public level for a value the reference implementation
/// will not write is not worth that.</para>
/// <para>An unparseable value makes every Spark commit fail with <c>DELTA_INVALID_ISOLATION_LEVEL</c>; this
/// refuses the same commits the same way, rather than guessing at a level the table never stated.</para>
/// </remarks>
internal static class IsolationLevelProperty
{
    /// <summary>The table property key.</summary>
    public const string PropertyKey = "delta.isolationLevel";

    /// <summary>The level <paramref name="configuration"/> demands.</summary>
    /// <exception cref="DeltaFormatException">The value names no isolation level.</exception>
    public static IsolationLevel Get(IReadOnlyDictionary<string, string>? configuration)
    {
        if (configuration is null || !configuration.TryGetValue(PropertyKey, out string? raw))
            return IsolationLevel.WriteSerializable;

        if (string.Equals(raw, "Serializable", StringComparison.OrdinalIgnoreCase))
            return IsolationLevel.Serializable;

        // SnapshotIsolation is a level Spark only ever DOWNGRADES a no-data-change commit to, and refuses to SET.
        // Read as the weakest level this library has: stricter than asked, never looser (see the remarks).
        if (string.Equals(raw, "WriteSerializable", StringComparison.OrdinalIgnoreCase)
            || string.Equals(raw, "SnapshotIsolation", StringComparison.OrdinalIgnoreCase))
            return IsolationLevel.WriteSerializable;

        throw new DeltaFormatException(
            DeltaErrorCodes.InvalidIsolationLevel,
            $"invalid isolation level '{raw}': the table property {PropertyKey} must be 'Serializable' or "
            + "'WriteSerializable' (the default when absent); 'SnapshotIsolation' is also accepted, and read as "
            + "WriteSerializable.");
    }

    /// <summary>
    /// The level a commit on a table configured with <paramref name="configuration"/> runs at: the table's
    /// level when <paramref name="requested"/> is null, otherwise the requested one — which may be stronger
    /// than the table's, never weaker.
    /// </summary>
    /// <exception cref="ArgumentException"><paramref name="requested"/> is weaker than the table demands.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="requested"/> is not a defined level.</exception>
    public static IsolationLevel Resolve(
        IReadOnlyDictionary<string, string>? configuration, IsolationLevel? requested, string paramName)
    {
        // Checked first: an undefined value compares as "stronger" below, and then runs as WriteSerializable
        // everywhere the checker asks `== Serializable`, which would let it slip under a Serializable table.
        if (requested is { } candidate && candidate is not (IsolationLevel.WriteSerializable or IsolationLevel.Serializable))
            throw new ArgumentOutOfRangeException(paramName, candidate, "Not a defined isolation level.");

        var table = Get(configuration);
        if (requested is not { } level)
            return table;
        if (level < table)
        {
            // Refused, not silently raised: a caller who asked for WriteSerializable by name is relying on
            // its blind-append exemption, and quietly withdrawing it would turn their expectation into
            // conflicts they cannot explain.
            throw new ArgumentException(
                $"The table demands {table} isolation ({PropertyKey}), so a transaction cannot run at the "
                + $"weaker {level}. Pass null to run at the table's level.",
                paramName);
        }
        return level;
    }
}

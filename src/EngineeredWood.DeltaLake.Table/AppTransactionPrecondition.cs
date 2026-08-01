// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// The compare-and-set guard on a <see cref="DeltaTransaction.RequireAppTransaction"/>: what the table must
/// ALREADY record for the producer before the commit is allowed to record something new.
///
/// <para>Four states rather than a nullable version, because a producer's needs differ by situation and
/// three of them cannot be expressed by naming a prior version. A union also has no sentinel to collide
/// with: the recorded value is the APPLICATION's own counter, not a table version, so every <c>long</c> is
/// a value some caller could legitimately record.</para>
///
/// <list type="bullet">
/// <item><see cref="None"/> — record unconditionally. The default, and the primitive a host wanting to
/// implement its own policy needs.</item>
/// <item><see cref="Absent"/> — the table must record nothing. The precondition a producer's FIRST batch
/// needs, and the one a prior-version comparison cannot state.</item>
/// <item><see cref="Exactly"/> — the table must record precisely this version. Catches a replay whose batch
/// boundary MOVED (see the remarks below), which is the case nothing else catches.</item>
/// <item><see cref="NotApplied"/> — the table must record nothing, or something lower. Delta-Spark's
/// idempotent-write rule.</item>
/// </list>
///
/// <para><b>Choosing between <see cref="Exactly"/> and <see cref="NotApplied"/>.</b> <c>NotApplied</c>
/// deduplicates an exact replay of the same batch, and tolerates gaps — it is right when the version is a
/// dense counter whose batches have fixed boundaries. It is BLIND to a batch that overlaps already-applied
/// data but ends higher: with 1000 recorded, a producer that restarts from a stale checkpoint at 800 and
/// submits 801-1300 passes <c>NotApplied</c> and writes rows 801-1000 a second time. <c>Exactly(800)</c>
/// refuses it, because the producer's belief about where it left off is wrong and that is exactly what the
/// comparison tests. Delta-Spark can rely on the monotonic rule alone because its version is a structured
/// streaming <c>batchId</c> whose checkpoint binds it to a fixed offset range, making such an overlap
/// unrepresentable; a host choosing its own counter has no such guarantee.</para>
/// </summary>
public readonly struct AppTransactionPrecondition : IEquatable<AppTransactionPrecondition>
{
    private AppTransactionPrecondition(AppTransactionPreconditionKind kind, long version)
    {
        Kind = kind;
        Version = version;
    }

    /// <summary>Which precondition this states.</summary>
    public AppTransactionPreconditionKind Kind { get; }

    /// <summary>The version <see cref="Exactly"/> requires; zero and meaningless for every other kind.</summary>
    public long Version { get; }

    /// <summary>No check — the version is recorded unconditionally. The <c>default</c> value of this type,
    /// so an omitted precondition means this.</summary>
    public static AppTransactionPrecondition None => default;

    /// <summary>The table must record NO version for this producer.</summary>
    public static AppTransactionPrecondition Absent =>
        new(AppTransactionPreconditionKind.Absent, 0);

    /// <summary>The table must record nothing, or a version lower than the one being committed — so a batch
    /// already applied is refused. Delta-Spark's rule; see the type's remarks for when it is not enough.</summary>
    public static AppTransactionPrecondition NotApplied =>
        new(AppTransactionPreconditionKind.NotApplied, 0);

    /// <summary>The table must record precisely <paramref name="version"/>.</summary>
    public static AppTransactionPrecondition Exactly(long version) =>
        new(AppTransactionPreconditionKind.Exactly, version);

    /// <summary>
    /// Whether this precondition holds, given what the table records (<paramref name="recorded"/>, null when
    /// it records nothing) and the version about to be committed. The single definition of every rule — the
    /// commit-time check and the pre-commit
    /// <see cref="DeltaTransaction.IsAppTransactionApplied"/> both answer from here, so they cannot drift.
    /// </summary>
    public bool Holds(long? recorded, long committing) => Kind switch
    {
        AppTransactionPreconditionKind.None => true,
        AppTransactionPreconditionKind.Absent => recorded is null,
        AppTransactionPreconditionKind.Exactly => recorded == Version,
        AppTransactionPreconditionKind.NotApplied => recorded is not { } r || r < committing,
        _ => throw new InvalidOperationException($"Unknown precondition kind {Kind}."),
    };

    /// <summary>What this precondition required, phrased to sit after "expected the table to record".</summary>
    public string Describe() => Kind switch
    {
        AppTransactionPreconditionKind.None => "anything (no precondition)",
        AppTransactionPreconditionKind.Absent => "no transaction at all",
        AppTransactionPreconditionKind.Exactly => $"version {Version}",
        AppTransactionPreconditionKind.NotApplied => "no transaction at all, or a lower version",
        _ => Kind.ToString(),
    };

    /// <inheritdoc/>
    public bool Equals(AppTransactionPrecondition other) =>
        Kind == other.Kind && Version == other.Version;

    /// <inheritdoc/>
    public override bool Equals(object? obj) =>
        obj is AppTransactionPrecondition other && Equals(other);

    /// <inheritdoc/>
    // Combined by hand rather than with HashCode.Combine, which netstandard2.0 does not have.
    public override int GetHashCode()
    {
        unchecked
        {
            return ((int)Kind * 397) ^ Version.GetHashCode();
        }
    }

    /// <inheritdoc/>
    public override string ToString() => Kind == AppTransactionPreconditionKind.Exactly
        ? $"Exactly({Version})" : Kind.ToString();

    public static bool operator ==(AppTransactionPrecondition left, AppTransactionPrecondition right) =>
        left.Equals(right);

    public static bool operator !=(AppTransactionPrecondition left, AppTransactionPrecondition right) =>
        !left.Equals(right);
}

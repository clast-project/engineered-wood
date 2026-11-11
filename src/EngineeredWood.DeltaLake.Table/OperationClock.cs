// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// The instant an operation reads its clock words at (#801): <c>CAST('now' AS TIMESTAMP)</c> and the like in a DML
/// predicate, a MERGE condition or value, a CHECK constraint, a generated column or a scan's filter. Read once where
/// the operation takes its expressions; every evaluation after that sees the same one.
/// </summary>
/// <remarks>
/// The wall clock, but for a test that fixes it (<see cref="Fix"/>), per async flow so that tests running in parallel
/// do not see each other's. A test fixes it far from the real time: an expression left unresolved is then folded at
/// the real time when evaluated, which the test can tell apart.
/// </remarks>
internal static class OperationClock
{
    private static readonly AsyncLocal<DateTimeOffset?> s_fixed = new();

    /// <summary>The current instant: the fixed one when a test has fixed it, else the wall clock's.</summary>
    public static DateTimeOffset Now => s_fixed.Value ?? DateTimeOffset.UtcNow;

    /// <summary>Fixes <see cref="Now"/> at <paramref name="instant"/> in this async flow until disposed.</summary>
    internal static IDisposable Fix(DateTimeOffset instant)
    {
        var previous = s_fixed.Value;
        s_fixed.Value = instant;
        return new Restore(previous);
    }

    private sealed class Restore(DateTimeOffset? previous) : IDisposable
    {
        public void Dispose() => s_fixed.Value = previous;
    }
}

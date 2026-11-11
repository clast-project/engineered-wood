// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// The instant an operation reads its clock words at (#801): <c>CAST('now' AS TIMESTAMP)</c> and the like in a DML
/// predicate, a MERGE condition or value, a CHECK constraint, a generated column or a scan's filter. One instant per
/// operation, shared by every part of it that reads one.
/// </summary>
/// <remarks>
/// <para>
/// An operation pins the instant where it starts (<see cref="Begin"/>), and every read of <see cref="Now"/> in its
/// async flow after that sees the same one: its predicate, its generated columns and its constraints alike (#828
/// review: read separately, a generated <c>g = CAST('now' AS TIMESTAMP)</c> could fail
/// <c>CHECK (g = CAST('now' AS TIMESTAMP))</c> in the same write). A nested <see cref="Begin"/> keeps the outer
/// instant, so a method may pin wherever it might be the operation's first reader.
/// </para>
/// <para>
/// The source is the wall clock, but for a test that replaces it (<see cref="Fix"/>, <see cref="Use"/>), per async flow
/// so that tests running in parallel do not see each other's.
/// </para>
/// </remarks>
internal static class OperationClock
{
    private static readonly AsyncLocal<DateTimeOffset?> s_pinned = new();
    private static readonly AsyncLocal<Func<DateTimeOffset>?> s_source = new();

    /// <summary>The operation's instant once pinned; before that, the source's current one.</summary>
    public static DateTimeOffset Now => s_pinned.Value ?? Read();

    private static DateTimeOffset Read() => s_source.Value?.Invoke() ?? DateTimeOffset.UtcNow;

    /// <summary>
    /// Pins <see cref="Now"/> for the rest of this async flow, unless an outer operation already has, until disposed.
    /// </summary>
    public static IDisposable Begin()
    {
        if (s_pinned.Value is not null)
            return NoChange.Instance;
        s_pinned.Value = Read();
        return new Unpin();
    }

    /// <summary>A test's source: <see cref="Now"/> is <paramref name="instant"/> in this async flow until disposed.</summary>
    internal static IDisposable Fix(DateTimeOffset instant) => Use(() => instant);

    /// <summary>A test's source, read wherever the wall clock would be, in this async flow until disposed.</summary>
    internal static IDisposable Use(Func<DateTimeOffset> source)
    {
        var previous = s_source.Value;
        s_source.Value = source;
        return new RestoreSource(previous);
    }

    private sealed class Unpin : IDisposable
    {
        public void Dispose() => s_pinned.Value = null;
    }

    private sealed class NoChange : IDisposable
    {
        public static readonly NoChange Instance = new();

        public void Dispose()
        {
        }
    }

    private sealed class RestoreSource(Func<DateTimeOffset>? previous) : IDisposable
    {
        public void Dispose() => s_source.Value = previous;
    }
}

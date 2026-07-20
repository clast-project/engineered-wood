// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg;

/// <summary>
/// Generates snapshot IDs that are strictly increasing and collision-free across
/// the whole process, regardless of how many snapshots are created within a single
/// clock tick.
/// </summary>
/// <remarks>
/// The ID embeds the wall-clock millisecond (times 1000) plus a small random
/// component. Two hazards make a naive <c>now * 1000 + random</c> collide, both
/// pronounced on .NET Framework: its <see cref="System.DateTime"/> clock resolves
/// to ~15 ms, and a per-call <c>new Random()</c> seeds from that same low-resolution
/// tick counter — so several IDs generated in one tick share both the base and the
/// random. A single shared <see cref="System.Random"/> plus a monotonic guard, both
/// under one lock shared by every generation site (appends, cherry-picks), removes
/// the collision.
/// </remarks>
internal static class SnapshotIdFactory
{
    private static readonly object Lock = new();
    private static long _lastId;
#if !NET6_0_OR_GREATER
    private static readonly Random LegacyRandom = new();
#endif

    public static long Generate()
    {
        lock (Lock)
        {
#if NET6_0_OR_GREATER
            var random = Random.Shared.Next(1000);
#else
            var random = LegacyRandom.Next(1000);
#endif
            var candidate = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000 + random;
            if (candidate <= _lastId)
                candidate = _lastId + 1;
            _lastId = candidate;
            return candidate;
        }
    }
}

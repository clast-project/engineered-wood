// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;

namespace EngineeredWood.Expressions;

/// <summary>
/// An instant with nanosecond precision: nanoseconds since 1970-01-01T00:00:00Z. What
/// <see cref="LiteralValue.ToObject"/> returns for a <see cref="LiteralValue.Kind.TimestampNanos"/> value.
/// </summary>
/// <remarks>
/// <see cref="DateTimeOffset"/> resolves only 100 ns ticks, so it cannot hold this value exactly;
/// <see cref="ToDateTimeOffset"/> says which way it rounds.
/// </remarks>
public readonly struct NanosecondTimestamp : IEquatable<NanosecondTimestamp>, IComparable<NanosecondTimestamp>
{
    private static readonly DateTimeOffset UnixEpoch = new(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Creates the instant <paramref name="nanosSinceEpoch"/> nanoseconds after the Unix epoch.</summary>
    public NanosecondTimestamp(long nanosSinceEpoch) => NanosSinceEpoch = nanosSinceEpoch;

    /// <summary>Nanoseconds since 1970-01-01T00:00:00Z.</summary>
    public long NanosSinceEpoch { get; }

    /// <summary>The instant floored to a 100 ns tick, in UTC.</summary>
    public DateTimeOffset ToDateTimeOffset() => UnixEpoch.AddTicks(FloorDiv(NanosSinceEpoch, 100));

    /// <summary>The sub-tick nanoseconds that <see cref="ToDateTimeOffset"/> drops, 0 to 99.</summary>
    public int NanosBeyondTick => (int)(NanosSinceEpoch - (FloorDiv(NanosSinceEpoch, 100) * 100));

    /// <inheritdoc/>
    public bool Equals(NanosecondTimestamp other) => NanosSinceEpoch == other.NanosSinceEpoch;

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is NanosecondTimestamp other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => NanosSinceEpoch.GetHashCode();

    /// <inheritdoc/>
    public int CompareTo(NanosecondTimestamp other) => NanosSinceEpoch.CompareTo(other.NanosSinceEpoch);

    /// <summary>ISO 8601 in UTC with nine fractional digits, e.g. <c>2017-11-16T22:31:08.123456789Z</c>.</summary>
    public override string ToString()
    {
        var instant = ToDateTimeOffset();
        long fraction = (instant.UtcTicks % TimeSpan.TicksPerSecond * 100) + NanosBeyondTick;
        return instant.UtcDateTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss", CultureInfo.InvariantCulture)
            + "." + fraction.ToString("D9", CultureInfo.InvariantCulture) + "Z";
    }

    /// <summary>Whether two instants are the same nanosecond.</summary>
    public static bool operator ==(NanosecondTimestamp left, NanosecondTimestamp right) => left.Equals(right);

    /// <summary>Whether two instants differ.</summary>
    public static bool operator !=(NanosecondTimestamp left, NanosecondTimestamp right) => !left.Equals(right);

    private static long FloorDiv(long value, long divisor)
    {
        long quotient = value / divisor;
        return value % divisor < 0 ? quotient - 1 : quotient;
    }
}

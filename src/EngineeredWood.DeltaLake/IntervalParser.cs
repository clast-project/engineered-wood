// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;

namespace EngineeredWood.DeltaLake;

/// <summary>
/// Parses the Calendar-interval strings Delta uses for duration table properties —
/// <c>delta.deletedFileRetentionDuration</c>, <c>delta.logRetentionDuration</c> and friends.
///
/// <para>Spark writes these as <c>"interval 1 week"</c>, <c>"interval 7 days"</c>,
/// <c>"interval 30 minutes"</c>. The leading <c>interval</c> keyword is optional in practice and
/// several unit/count pairs may appear in one string (<c>"interval 1 day 6 hours"</c>).</para>
///
/// <para>Months and years are deliberately NOT supported: they are calendar-relative, so converting
/// them to a fixed <see cref="TimeSpan"/> requires an anchor date and would silently be wrong by up to
/// three days a month. A property using them fails to parse, and callers fall back to their default
/// rather than acting on a value they cannot honor faithfully.</para>
/// </summary>
internal static class IntervalParser
{
    public static bool TryParse(string? value, out TimeSpan result)
    {
        result = default;
        if (string.IsNullOrWhiteSpace(value))
            return false;

        var tokens = value!.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
        int index = 0;
        if (index < tokens.Length && tokens[index].Equals("interval", StringComparison.OrdinalIgnoreCase))
            index++;

        var total = TimeSpan.Zero;
        bool sawAny = false;

        while (index + 1 < tokens.Length)
        {
            if (!long.TryParse(tokens[index], NumberStyles.Integer, CultureInfo.InvariantCulture, out long count))
                return false;

            if (!TryUnit(tokens[index + 1], count, out var span))
                return false;

            total += span;
            sawAny = true;
            index += 2;
        }

        // A trailing token with no pair (e.g. "interval 5") is malformed, not a partial success.
        if (!sawAny || index != tokens.Length)
            return false;

        result = total;
        return true;
    }

    private static bool TryUnit(string unit, long count, out TimeSpan span)
    {
        // Spark accepts singular and plural spellings interchangeably.
        string normalized = unit.TrimEnd('s').ToLowerInvariant();
        span = normalized switch
        {
            "week" => TimeSpan.FromDays(7 * count),
            "day" => TimeSpan.FromDays(count),
            "hour" => TimeSpan.FromHours(count),
            "minute" => TimeSpan.FromMinutes(count),
            "second" => TimeSpan.FromSeconds(count),
            "millisecond" => TimeSpan.FromMilliseconds(count),
            // "month" / "year" are calendar-relative — see the type remarks.
            _ => TimeSpan.MinValue,
        };

        return span != TimeSpan.MinValue;
    }
}

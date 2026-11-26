// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text;

namespace EngineeredWood.Iceberg.Values;

/// <summary>
/// ISO 8601 dates, times and timestamps as Iceberg's JSON single values spell them, read and written the way
/// Iceberg Java's <c>java.time</c> formatters do.
/// </summary>
/// <remarks>
/// Not .NET's own parsers: they resolve only 100 ns ticks and years 1 to 9999, and a nanosecond timestamp
/// needs nine fractional digits. What is accepted follows <c>ISO_LOCAL_DATE</c>, <c>ISO_LOCAL_TIME</c> and
/// <c>ISO_LOCAL_DATE_TIME</c>: a four-digit year, or a signed one when it needs more digits or is negative;
/// seconds and the fraction optional; one to nine fractional digits. Written text matches Java's too:
/// seconds always present, the fraction only when non-zero and without trailing zeros.
/// </remarks>
internal static class IsoTemporalText
{
    /// <summary>Whether a timestamp's text carried a zone offset, and whether that offset was zero.</summary>
    internal enum Offset
    {
        None,
        Zero,
        NonZero,
    }

    internal static bool TryParseDate(string text, out long epochDays)
    {
        int pos = 0;
        if (TryParseDatePart(text, ref pos, out epochDays) && pos == text.Length)
            return true;
        epochDays = 0;
        return false;
    }

    internal static bool TryParseTime(string text, out long nanosOfDay)
    {
        int pos = 0;
        if (TryParseTimePart(text, ref pos, out nanosOfDay) && pos == text.Length)
            return true;
        nanosOfDay = 0;
        return false;
    }

    /// <summary>
    /// A date, a <c>T</c>, a time, and an optional offset: <c>Z</c>, <c>±HH:MM</c> or <c>±HH:MM:SS</c>. The
    /// <c>T</c> and <c>Z</c> may be lower case, as java.time accepts.
    /// </summary>
    internal static bool TryParseTimestamp(string text, out long epochDays, out long nanosOfDay, out Offset offset)
    {
        int pos = 0;
        nanosOfDay = 0;
        offset = Offset.None;
        if (!TryParseDatePart(text, ref pos, out epochDays)
            || pos >= text.Length || text[pos] is not ('T' or 't'))
            return false;
        pos++;
        if (!TryParseTimePart(text, ref pos, out nanosOfDay))
            return false;
        if (pos == text.Length)
            return true;

        if (text[pos] is 'Z' or 'z' && pos + 1 == text.Length)
        {
            offset = Offset.Zero;
            return true;
        }

        if (text[pos] is '+' or '-' && pos + 6 <= text.Length && text[pos + 3] == ':'
            && TryDigits(text, pos + 1, 2, out int hours) && TryDigits(text, pos + 4, 2, out int minutes)
            && hours <= 18 && minutes <= 59)
        {
            int seconds = 0;
            if (pos + 6 != text.Length
                && !(pos + 9 == text.Length && text[pos + 6] == ':' && TryDigits(text, pos + 7, 2, out seconds) && seconds <= 59))
                return false;
            offset = hours == 0 && minutes == 0 && seconds == 0 ? Offset.Zero : Offset.NonZero;
            return true;
        }

        return false;
    }

    internal static string FormatDate(long epochDays)
    {
        var sb = new StringBuilder(16);
        AppendDate(sb, epochDays);
        return sb.ToString();
    }

    internal static string FormatTime(long nanosOfDay)
    {
        var sb = new StringBuilder(18);
        AppendTime(sb, nanosOfDay);
        return sb.ToString();
    }

    internal static string FormatTimestamp(long epochDays, long nanosOfDay, bool utcOffset)
    {
        var sb = new StringBuilder(36);
        AppendDate(sb, epochDays);
        sb.Append('T');
        AppendTime(sb, nanosOfDay);
        if (utcOffset)
            sb.Append("+00:00");
        return sb.ToString();
    }

    // ── Reading ──

    private static bool TryParseDatePart(string text, ref int pos, out long epochDays)
    {
        epochDays = 0;
        bool negative = false, signed = false;
        if (pos < text.Length && text[pos] is '+' or '-')
        {
            signed = true;
            negative = text[pos] == '-';
            pos++;
        }

        int start = pos;
        while (pos < text.Length && IsDigit(text[pos]))
            pos++;
        int yearDigits = pos - start;
        // Four digits unsigned; a sign only where Java's EXCEEDS_PAD puts one: a negative year, or a
        // positive one of more than four digits.
        if (yearDigits < 4 || yearDigits > 9 || (!signed && yearDigits != 4) || (signed && !negative && yearDigits == 4))
            return false;
        long year = long.Parse(text.Substring(start, yearDigits), NumberStyles.None, CultureInfo.InvariantCulture);
        if (negative)
            year = -year;

        if (pos + 6 > text.Length || text[pos] != '-' || text[pos + 3] != '-'
            || !TryDigits(text, pos + 1, 2, out int month) || !TryDigits(text, pos + 4, 2, out int day))
            return false;
        pos += 6;

        if (month is < 1 or > 12 || day < 1 || day > DaysInMonth(year, month))
            return false;

        epochDays = DaysFromCivil(year, month, day);
        return true;
    }

    private static bool TryParseTimePart(string text, ref int pos, out long nanosOfDay)
    {
        nanosOfDay = 0;
        if (pos + 5 > text.Length || text[pos + 2] != ':'
            || !TryDigits(text, pos, 2, out int hour) || !TryDigits(text, pos + 3, 2, out int minute))
            return false;
        pos += 5;

        int second = 0;
        long fraction = 0;
        if (pos < text.Length && text[pos] == ':')
        {
            if (!TryDigits(text, pos + 1, 2, out second))
                return false;
            pos += 3;

            if (pos < text.Length && text[pos] == '.')
            {
                pos++;
                int start = pos;
                while (pos < text.Length && IsDigit(text[pos]))
                    pos++;
                int digits = pos - start;
                if (digits is < 1 or > 9)
                    return false;
                fraction = long.Parse(text.Substring(start, digits), NumberStyles.None, CultureInfo.InvariantCulture);
                for (int i = digits; i < 9; i++)
                    fraction *= 10;
            }
        }

        if (hour > 23 || minute > 59 || second > 59)
            return false;

        nanosOfDay = (((hour * 60L) + minute) * 60 + second) * 1_000_000_000L + fraction;
        return true;
    }

    private static bool TryDigits(string text, int pos, int count, out int value)
    {
        value = 0;
        if (pos + count > text.Length)
            return false;
        for (int i = 0; i < count; i++)
        {
            char c = text[pos + i];
            if (!IsDigit(c))
                return false;
            value = (value * 10) + (c - '0');
        }

        return true;
    }

    private static bool IsDigit(char c) => c is >= '0' and <= '9';

    // ── Writing ──

    private static void AppendDate(StringBuilder sb, long epochDays)
    {
        var (year, month, day) = CivilFromDays(epochDays);
        if (year > 9999)
            sb.Append('+').Append(year.ToString(CultureInfo.InvariantCulture));
        else if (year < 0)
            sb.Append('-').Append((-year).ToString("D4", CultureInfo.InvariantCulture));
        else
            sb.Append(year.ToString("D4", CultureInfo.InvariantCulture));
        sb.Append('-').Append(month.ToString("D2", CultureInfo.InvariantCulture))
          .Append('-').Append(day.ToString("D2", CultureInfo.InvariantCulture));
    }

    private static void AppendTime(StringBuilder sb, long nanosOfDay)
    {
        long seconds = nanosOfDay / 1_000_000_000L;
        long fraction = nanosOfDay % 1_000_000_000L;
        sb.Append((seconds / 3600).ToString("D2", CultureInfo.InvariantCulture))
          .Append(':').Append((seconds / 60 % 60).ToString("D2", CultureInfo.InvariantCulture))
          .Append(':').Append((seconds % 60).ToString("D2", CultureInfo.InvariantCulture));
        if (fraction != 0)
            sb.Append('.').Append(fraction.ToString("D9", CultureInfo.InvariantCulture).TrimEnd('0'));
    }

    // ── The proleptic Gregorian calendar (H. Hinnant, "chrono-Compatible Low-Level Date Algorithms") ──

    internal static long DaysFromCivil(long year, int month, int day)
    {
        year -= month <= 2 ? 1 : 0;
        long era = (year >= 0 ? year : year - 399) / 400;
        long yoe = year - (era * 400);
        long doy = ((153 * (month + (month > 2 ? -3 : 9))) + 2) / 5 + day - 1;
        long doe = (yoe * 365) + (yoe / 4) - (yoe / 100) + doy;
        return (era * 146097) + doe - 719468;
    }

    internal static (long Year, int Month, int Day) CivilFromDays(long epochDays)
    {
        long z = epochDays + 719468;
        long era = (z >= 0 ? z : z - 146096) / 146097;
        long doe = z - (era * 146097);
        long yoe = (doe - (doe / 1460) + (doe / 36524) - (doe / 146096)) / 365;
        long year = yoe + (era * 400);
        long doy = doe - ((365 * yoe) + (yoe / 4) - (yoe / 100));
        long mp = ((5 * doy) + 2) / 153;
        int day = (int)(doy - (((153 * mp) + 2) / 5) + 1);
        int month = (int)(mp < 10 ? mp + 3 : mp - 9);
        return (month <= 2 ? year + 1 : year, month, day);
    }

    private static int DaysInMonth(long year, int month) => month switch
    {
        2 => (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 29 : 28,
        4 or 6 or 9 or 11 => 30,
        _ => 31,
    };
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions;

/// <summary>
/// Spark's own grammar for a DATE or a TIMESTAMP written as text.
/// </summary>
/// <remarks>
/// <b>Spark does not parse a date the way a culture-aware parser does, and it is wrong in both
/// directions to use one.</b> Until #318 both temporal casts read their text with
/// <see cref="DateTimeOffset.TryParse(string, IFormatProvider, System.Globalization.DateTimeStyles, out DateTimeOffset)"/>
/// under <c>InvariantCulture</c>, which accepts a whole culture's worth of formats Spark refuses
/// and refuses two forms Spark accepts. Measured on Spark 4.0.3 / JDK 17.0.20, session zone UTC:
/// <list type="bullet">
/// <item><description>
/// <c>CAST('08/11/2026' AS DATE)</c> is refused by Spark and was read here as 11 August — a
/// <c>MM/dd/yyyy</c> reading that is a <b>different day</b> to whoever wrote <c>dd/MM/yyyy</c>.
/// Silently answering a date nobody meant is the worst of these. <c>'2026/08/11'</c>,
/// <c>'11 Aug 2026'</c>, <c>'Aug 11, 2026'</c> and <c>'2026-08 -11'</c> are the same shape.
/// </description></item>
/// <item><description>
/// <c>CAST('2026' AS DATE)</c> is 2026-01-01 to Spark — a bare year is a date, and the month and
/// day default — and was refused here.
/// </description></item>
/// <item><description>
/// <c>CAST('2026-08-11 extra' AS DATE)</c> is 2026-08-11 to Spark. <b>A date parse stops at the
/// first space or <c>T</c> and ignores the rest of the string</b>, which is also why
/// <c>'2026-08-11 12:30:00'</c> casts to a date. It is not "the whole string must be a date".
/// </description></item>
/// </list>
/// <para>
/// So the rule is a hand-rolled reader, for the reason <c>SparkDecimalText</c> and
/// <c>SparkIntegralCasts</c> are: the .NET parser next to it answers a different question.
/// This is a port of <c>SparkDateTimeUtils.stringToDate</c> and
/// <c>SparkDateTimeUtils.parseTimestampString</c> from the Spark source, checked against the
/// measurements above rather than against the reading of it.
/// </para>
/// <para>
/// <b>Two limits are ours rather than Spark's</b>, both because an instant travels through this
/// library as a <see cref="DateTimeOffset"/>:
/// </para>
/// <list type="number">
/// <item><description>
/// <b>The year must be 1 through 9999.</b> Spark reads a date's year from up to seven digits and
/// honours a leading <c>-</c>, so <c>CAST('-2026-08-11' AS DATE)</c> and
/// <c>CAST('1234567-01-01' AS DATE)</c> are dates to it and are refused here. Widening that means
/// carrying days-from-epoch rather than a <see cref="DateTimeOffset"/> through the cast, which is
/// a change to <c>SparkArrays.ReadForCast</c>'s shape rather than to this grammar.
/// </description></item>
/// <item><description>
/// <b>A named zone is refused</b> — see <see cref="TryReadZone"/>.
/// </description></item>
/// </list>
/// </remarks>
internal static class SparkTemporalText
{
    /// <summary>The session time zone a zone-less DATE or TIMESTAMP is read in.</summary>
    /// <remarks>
    /// <para>
    /// Here rather than in <c>SparkDialectOptions</c>, which reads it from here: this reader is used
    /// by the typed-literal parser as well as by the casts, and the parser's assembly cannot see
    /// the options (#341). One zone for both is the point -- a <c>TIMESTAMP'…'</c> literal and a
    /// column cast from the same text must name the same instant.
    /// </para>
    /// <para>
    /// UTC, because that is what the harvested corpus is pinned to, and because the rest of the
    /// library already assumes it: a Date32 is read as UTC midnight of its day.
    /// </para>
    /// </remarks>
    internal static TimeZoneInfo SessionTimeZone => TimeZoneInfo.Utc;

    /// <summary>The <see cref="DateTime"/> ticks in one microsecond.</summary>
    private const long TicksPerMicrosecond = 10L;

    /// <summary>
    /// Reads <paramref name="text"/> as a Spark DATE, as UTC midnight of the day it names.
    /// </summary>
    /// <remarks>
    /// <b>Takes the RAW text and trims it itself</b>, with <see cref="SparkText.TrimBounds"/> —
    /// which is the same rule Spark's own <c>getTrimmedStart</c>/<c>getTrimmedEnd</c> apply, and
    /// the reason #316's <c>TemporalText</c> guard is gone: <c>DateTimeOffset.TryParse</c> skipped
    /// <see cref="char.IsWhiteSpace(char)"/> of its own accord underneath that trim, and nothing
    /// here does. A leading U+00A0 is simply not a digit.
    /// <para>
    /// The grammar is <c>[+-]yyyy[-[m]m[-[d]d]]</c>: a year of four to seven digits, then a month
    /// and a day of one or two each, and then — <b>only once both separators have been seen</b> —
    /// a space or a <c>T</c> that ends the parse and discards the rest. That last clause is what
    /// makes <c>'2026-08-11 extra'</c> a date and <c>'2026 extra'</c> and <c>'2026-08T'</c>
    /// refusals, all three measured.
    /// </para>
    /// </remarks>
    public static bool TryReadDate(ReadOnlySpan<char> text, out DateTimeOffset value)
    {
        value = default;

        var (start, end) = SparkText.TrimBounds(text);
        if (start == end)
            return false;

        // Year, month, day. The month and day default to 1, which is what makes a bare year and a
        // year-month whole dates.
        Span<int> segments = stackalloc int[3] { 1, 1, 1 };

        var sign = 1;
        var segment = 0;
        var current = 0;
        var digits = 0;
        var j = start;

        if (text[j] == '-' || text[j] == '+')
        {
            sign = text[j] == '-' ? -1 : 1;
            j++;
        }

        while (j < end && text[j] != ' ' && text[j] != 'T')
        {
            var c = text[j];
            if (segment < 2 && c == '-')
            {
                if (!ValidDateDigits(segment, digits))
                    return false;

                segments[segment] = current;
                current = 0;
                digits = 0;
                segment++;
            }
            else
            {
                if (c < '0' || c > '9')
                    return false;

                current = Accumulate(current, c, digits);
                digits++;
            }

            j++;
        }

        if (!ValidDateDigits(segment, digits))
            return false;

        // The scan stopped early, and a space or a `T` only ends a date that already has all three
        // segments. `'2026-08-11 extra'` passes here and `'2026-08 extra'` does not.
        if (segment < 2 && j < end)
            return false;

        segments[segment] = current;

        return TryMakeDate(sign * segments[0], segments[1], segments[2], out value);
    }

    /// <summary>Reads <paramref name="text"/> as a Spark TIMESTAMP, as an instant.</summary>
    /// <remarks>
    /// The grammar is a date as <see cref="TryReadDate"/> reads one — with a <b>six</b>-digit
    /// year rather than seven — then <c>[h]h:[m]m[:[s]s[.f…]]</c>, then a timezone. Every
    /// component below the one that ends the text defaults, so <c>'2026'</c>,
    /// <c>'2026-08-11 12'</c> and <c>'2026-08-11 12:30'</c> are all timestamps.
    /// <para>
    /// Three things the shape does not show:
    /// </para>
    /// <list type="bullet">
    /// <item><description>
    /// <b>A time alone means today</b>, in the resolved zone: <c>'12:30:00'</c> is today at
    /// 12:30, and so is <c>'T12'</c>. Spark reads it with <c>LocalDate.now</c>, so the value is a
    /// function of the clock in both engines.
    /// </description></item>
    /// <item><description>
    /// <b>Past six fraction digits the rest are dropped rather than refused</b>:
    /// <c>'…12:30:00.1234567'</c> is <c>12:30:00.123456</c>. Spark stops accumulating and stops
    /// padding, which is a truncation toward zero and not a rounding.
    /// </description></item>
    /// <item><description>
    /// <b>Trailing text is a ZONE here, not junk to ignore.</b> Where a DATE discards whatever
    /// follows, <c>CAST('2026-08-11 12:30:00 extra' AS TIMESTAMP)</c> is refused, because
    /// everything from the first character that cannot continue the time is handed to the zone
    /// parser. The same rule is why <c>'…12:30:00 UTC'</c> and <c>'…12:30:00+02:00'</c> are read.
    /// </description></item>
    /// </list>
    /// </remarks>
    public static bool TryReadTimestamp(ReadOnlySpan<char> text, out DateTimeOffset value)
    {
        value = default;

        var (start, end) = SparkText.TrimBounds(text);
        if (start == end)
            return false;

        // Year, month, day, hour, minute, second, microsecond. Spark carries two further segments
        // that no input can reach; see the comment on the tail of the scan.
        Span<int> segments = stackalloc int[7] { 1, 1, 1, 0, 0, 0, 0 };

        var sign = 1;
        var signed = false;
        var segment = 0;
        var current = 0;
        var digits = 0;
        var fractionDigits = 0;
        var justTime = false;
        var zoneStart = -1;
        var j = start;

        if (text[j] == '-' || text[j] == '+')
        {
            sign = text[j] == '-' ? -1 : 1;
            signed = true;
            j++;
        }

        while (j < end)
        {
            var c = text[j];
            if (c >= '0' && c <= '9')
            {
                if (segment == 6)
                {
                    // Counted even past the sixth digit, which is what stops the padding below
                    // and truncates rather than refusing.
                    fractionDigits++;
                    if (digits < 6)
                        current = (current * 10) + (c - '0');
                }
                else
                {
                    current = Accumulate(current, c, digits);
                }

                digits++;
            }
            else if (j == 0 && c == 'T')
            {
                // A LEADING `T` means the rest is a time. Spark tests index 0 of the whole string
                // rather than of the trimmed one, so this is deliberately `j == 0` and not
                // `j == start`: measured, `CAST('T12:30:00' AS TIMESTAMP)` is today at 12:30 and
                // `CAST(' T12:30:00' AS TIMESTAMP)` -- one leading space -- is refused.
                justTime = true;
                segment += 3;
            }
            else if (segment < 2)
            {
                if (c == '-')
                {
                    if (!ValidTimestampDigits(segment, digits))
                        return false;

                    segments[segment] = current;
                    current = 0;
                    digits = 0;
                    segment++;
                }
                else if (segment == 0 && c == ':' && !signed)
                {
                    // A colon where the year would be: the text is a time alone. A SIGN rules that
                    // out -- `'+12:30:00'` is refused -- because a sign begins a year.
                    justTime = true;
                    if (!ValidTimestampDigits(3, digits))
                        return false;

                    segments[3] = current;
                    current = 0;
                    digits = 0;
                    segment = 4;
                }
                else
                {
                    return false;
                }
            }
            else if (segment == 2)
            {
                // The date ends at a space or a `T`, and at nothing else: `'2026-08-11Z'` is
                // refused where `'2026-08-11T12:30:00Z'` is read.
                if (c != ' ' && c != 'T')
                    return false;

                if (!ValidTimestampDigits(segment, digits))
                    return false;

                segments[segment] = current;
                current = 0;
                digits = 0;
                segment++;
            }
            else if (segment is 3 or 4)
            {
                if (c != ':')
                    return false;

                if (!ValidTimestampDigits(segment, digits))
                    return false;

                segments[segment] = current;
                current = 0;
                digits = 0;
                segment++;
            }
            else if (segment is 5 or 6)
            {
                if (c == '.' && segment == 5)
                {
                    if (!ValidTimestampDigits(segment, digits))
                        return false;

                    segments[segment] = current;
                    current = 0;
                    digits = 0;
                    segment++;
                }
                else
                {
                    // ANYTHING ELSE HERE IS THE TIMEZONE, to the end of the text. Nothing after it
                    // is parsed, which is why `'…12:30:00 +02:00 junk'` is refused as a zone name
                    // rather than read as an offset with junk after it.
                    if (!ValidTimestampDigits(segment, digits))
                        return false;

                    segments[segment] = current;
                    current = 0;
                    digits = 0;
                    segment++;
                    zoneStart = j;
                    j = end - 1;
                }

                // Spark's `if (i == 6 && b != '.')`. Reached only when the zone was taken from the
                // seconds, where the fraction is skipped rather than defaulted.
                if (segment == 6 && c != '.')
                    segment++;
            }
            else
            {
                // Spark carries a further branch for segment 7 and 8, accepting `:` or ` `. It
                // cannot be reached: the only path to segment 7 is the timezone branch above, and
                // that ends the scan on the same character.
                return false;
            }

            j++;
        }

        if (!ValidTimestampDigits(segment, digits))
            return false;

        if (segment < segments.Length)
            segments[segment] = current;

        // `.1` is a tenth of a second, so the fraction is padded to microseconds -- and only while
        // fewer than six digits were SEEN, which is what leaves a truncated fraction alone.
        while (fractionDigits < 6)
        {
            segments[6] *= 10;
            fractionDigits++;
        }

        TimeSpan? zone = null;
        if (zoneStart >= 0)
        {
            if (!TryReadZone(text.Slice(zoneStart, end - zoneStart), out var offset))
                return false;

            zone = offset;
        }

        if (segments[3] > 23 || segments[4] > 59 || segments[5] > 59)
            return false;

        var time = new TimeSpan((segments[3] * TimeSpan.TicksPerHour)
            + (segments[4] * TimeSpan.TicksPerMinute)
            + (segments[5] * TimeSpan.TicksPerSecond)
            + (segments[6] * TicksPerMicrosecond));

        DateTime day;
        if (justTime)
        {
            day = (zone is { } today
                ? DateTimeOffset.UtcNow.UtcDateTime + today
                : TimeZoneInfo.ConvertTime(DateTimeOffset.UtcNow, SessionTimeZone).DateTime)
                .Date;
        }
        else
        {
            if (!TryMakeDate(sign * segments[0], segments[1], segments[2], out var date))
                return false;

            day = date.UtcDateTime;
        }

        var local = DateTime.SpecifyKind(day + time, DateTimeKind.Unspecified);

        try
        {
            // Subtracted rather than handed to DateTimeOffset as its offset, because .NET bounds
            // an offset at 14 HOURS and Java bounds one at 18 -- and Spark reads the whole Java
            // range: measured, `'…12:30:00+18:00'` is 18:30 on the previous day. The instant
            // itself is in range either way; only the constructor's argument check is not.
            var offset = zone ?? SessionTimeZone.GetUtcOffset(local);
            value = new DateTimeOffset(local - offset, TimeSpan.Zero);
        }
        catch (ArgumentOutOfRangeException)
        {
            // The offset carried the instant outside DateTime's range, which only the first and
            // last representable days can do. Spark catches the same overflow from `Instant.from`
            // and answers None.
            return false;
        }

        return true;
    }

    /// <summary>Spark's digit-count rule for a DATE segment.</summary>
    /// <remarks>
    /// A year is four to SEVEN digits — <c>'20260811'</c> is not a date, and neither is
    /// <c>'202-01-01'</c> — and a month or a day is one or two, so <c>'2026-08-011'</c> is
    /// refused where <c>'2026-8-1'</c> is read.
    /// </remarks>
    private static bool ValidDateDigits(int segment, int digits) =>
        segment == 0 ? digits is >= 4 and <= 7 : digits is >= 1 and <= 2;

    /// <summary>Spark's digit-count rule for a TIMESTAMP segment.</summary>
    /// <remarks>
    /// <b>The year is SIX digits here and seven for a DATE</b>, which is Spark's own asymmetry
    /// and not a transcription slip: measured, <c>CAST('1234567-01-01' AS DATE)</c> is a date and
    /// <c>CAST('1234567-01-01 00:00:00' AS TIMESTAMP)</c> is refused.
    /// <para>
    /// Segment 6 is the fraction, which has no count rule at all: no digits is a valid fraction
    /// (<c>'…12:30:00.'</c> is read) and so is any number of them.
    /// </para>
    /// </remarks>
    private static bool ValidTimestampDigits(int segment, int digits) => segment switch
    {
        0 => digits is >= 4 and <= 6,
        6 => true,
        // Segment 7 exists only as the state the timezone branch leaves behind, with no digits.
        >= 7 => digits <= 2,
        _ => digits is >= 1 and <= 2,
    };

    /// <summary>One more digit of a segment, without letting a long run overflow.</summary>
    /// <remarks>
    /// Java wraps where <c>checked</c> C# would raise, and the wrapped value is never used: every
    /// segment this guards is refused by its digit count long before eight digits. Stopping the
    /// accumulation keeps that true without depending on which arithmetic mode the assembly is
    /// built in.
    /// </remarks>
    private static int Accumulate(int current, char c, int digits) =>
        digits < 8 ? (current * 10) + (c - '0') : current;

    /// <summary>The calendar date <paramref name="year"/>-<paramref name="month"/>-<paramref name="day"/>, at UTC midnight.</summary>
    /// <remarks>
    /// A date is UTC midnight of the day because that is how this library already surfaces one —
    /// see <c>SparkArrays.ReadInstant</c>, where a Date32 is read the same way, so a
    /// literal and a column value compare on the same footing.
    /// <para>
    /// The year bound is EngineeredWood's and not Spark's; see the type's remarks.
    /// </para>
    /// </remarks>
    private static bool TryMakeDate(int year, int month, int day, out DateTimeOffset value)
    {
        value = default;

        if (year < 1 || year > 9999 || month < 1 || month > 12)
            return false;

        if (day < 1 || day > DateTime.DaysInMonth(year, month))
            return false;

        value = new DateTimeOffset(year, month, day, 0, 0, 0, TimeSpan.Zero);
        return true;
    }

    /// <summary>
    /// Whether <paramref name="zone"/> names a timezone this library can resolve.
    /// </summary>
    /// <remarks>
    /// For <c>SparkSpecialDatetimeValues</c>, whose <c>isValid</c> guard asks only whether the
    /// text after a special word resolves as a zone and never uses the offset. The same
    /// region-id limit applies -- see <see cref="TryReadZone"/> -- which there costs
    /// <c>CAST('epoch America/Los_Angeles' AS DATE)</c> an answer it would otherwise have given
    /// without consulting the zone at all.
    /// </remarks>
    internal static bool IsResolvableZone(ReadOnlySpan<char> zone) => TryReadZone(zone, out _);

    /// <summary>
    /// The offset a timestamp's trailing timezone names, or false if it is one we cannot resolve.
    /// </summary>
    /// <remarks>
    /// Spark resolves this with <c>ZoneId.of(id, ZoneId.SHORT_IDS)</c> after normalising two
    /// single-digit spellings, so it accepts everything Java does: <c>Z</c>, an offset, the
    /// <c>UTC</c>/<c>GMT</c>/<c>UT</c> prefixes with or without one, and every region id in the
    /// tz database plus Java's short aliases.
    /// <para>
    /// <b>The region ids are refused here, deliberately.</b> Resolving <c>America/Los_Angeles</c>
    /// needs a tz database, and .NET's availability of one is not uniform across this library's
    /// target frameworks — <see cref="TimeZoneInfo.FindSystemTimeZoneById"/> reads an IANA id on
    /// .NET 6 and later and throws on .NET Framework, where the same id has a Windows spelling.
    /// A cast that answered on net10.0 and refused on net472 would make a CHECK constraint accept
    /// a row on one host and reject it on another, which is worse than refusing both times.
    /// So <c>CAST('2026-08-11 12:30:00America/Los_Angeles' AS TIMESTAMP)</c> is a measured
    /// difference from Spark (19:30 UTC there, refused here) and <c>EST</c> and <c>PST</c> are
    /// the same.
    /// </para>
    /// <para>
    /// Everything self-describing IS read, which is the part that matters for data written by
    /// another engine: an ISO offset is how a timestamp normally carries its zone.
    /// </para>
    /// </remarks>
    private static bool TryReadZone(ReadOnlySpan<char> zone, out TimeSpan offset)
    {
        offset = TimeSpan.Zero;

        // Java's `String.trim`, which is the same set as the cast's own trim.
        var (start, end) = SparkText.TrimBounds(zone);
        var trimmed = zone.Slice(start, end - start);

        // The longest form read below is `UTC+hh:mm:ss`, and the normalisation adds at most two
        // characters. Anything longer is a region id, which is refused either way -- and bounding
        // it here is what keeps the scratch buffer a fixed size rather than the input's.
        if (trimmed.Length == 0 || trimmed.Length > 12)
            return false;

        Span<char> buffer = stackalloc char[14];
        var id = Normalise(trimmed, buffer);

        // `ZoneId.of`: one character or a sign is an offset, then the three prefixes, and anything
        // else is a region id.
        if (id.Length <= 1 || id[0] == '+' || id[0] == '-')
            return TryReadOffset(id, out offset);

        var prefix = id.StartsWith("UTC".AsSpan()) || id.StartsWith("GMT".AsSpan()) ? 3
            : id.StartsWith("UT".AsSpan()) ? 2
            : 0;

        if (prefix == 0)
            return false;

        if (id.Length == prefix)
            return true;

        return (id[prefix] == '+' || id[prefix] == '-')
            && TryReadOffset(id.Slice(prefix), out offset);
    }

    /// <summary>
    /// Spark's two normalisations of a single-digit offset field, applied in its order.
    /// </summary>
    /// <remarks>
    /// <c>getZoneId</c> runs <c>([+-])(\d):</c> then <c>([+-])(\d\d):(\d)$</c> over the zone text
    /// before handing it to Java, each replacing its FIRST match, so <c>'+2:00'</c> becomes
    /// <c>'+02:00'</c> and <c>'+02:0'</c> becomes <c>'+02:00'</c>. Both are measured as accepted.
    /// Java itself would refuse them: <c>ZoneOffset.of</c> reads a field's width from the string's
    /// LENGTH, so a five-character <c>'+2:00'</c> is read as <c>+hhmm</c> and fails on the colon.
    /// <para>
    /// Hand-rolled rather than two <c>Regex</c> replacements because both run on a per-row path,
    /// and because the first pattern is "insert a zero after the sign" once written out.
    /// </para>
    /// </remarks>
    private static ReadOnlySpan<char> Normalise(ReadOnlySpan<char> id, Span<char> buffer)
    {
        id.CopyTo(buffer);
        var length = id.Length;

        for (var i = 0; i + 2 < length; i++)
        {
            if ((buffer[i] == '+' || buffer[i] == '-')
                && IsDigit(buffer[i + 1]) && buffer[i + 2] == ':')
            {
                buffer.Slice(i + 1, length - i - 1).CopyTo(buffer.Slice(i + 2));
                buffer[i + 1] = '0';
                length++;
                break;
            }
        }

        if (length >= 5
            && IsDigit(buffer[length - 1]) && buffer[length - 2] == ':'
            && IsDigit(buffer[length - 3]) && IsDigit(buffer[length - 4])
            && (buffer[length - 5] == '+' || buffer[length - 5] == '-'))
        {
            buffer[length] = buffer[length - 1];
            buffer[length - 1] = '0';
            length++;
        }

        return buffer.Slice(0, length);
    }

    /// <summary>Java's <c>ZoneOffset.of</c>, which reads a field's width from the id's LENGTH.</summary>
    /// <remarks>
    /// <c>'Z'</c>, <c>'+h'</c>, <c>'+hh'</c>, <c>'+hhmm'</c>, <c>'+hh:mm'</c>, <c>'+hhmmss'</c>
    /// and <c>'+hh:mm:ss'</c>, and nothing else — a length Java does not list is refused without
    /// looking at the characters, so <c>'+02:0'</c> is only read because
    /// <see cref="Normalise"/> already lengthened it.
    /// <para>
    /// The bound is Java's: eighteen hours, and exactly eighteen admits no minutes or seconds.
    /// <c>'+19:00'</c> is refused by Spark and <c>'+18:00'</c> is a timestamp on the previous day.
    /// </para>
    /// </remarks>
    private static bool TryReadOffset(ReadOnlySpan<char> id, out TimeSpan offset)
    {
        offset = TimeSpan.Zero;

        // Only the capital, measured: `'…12:30:00z'` is refused where `'…12:30:00Z'` is read.
        if (id.Length == 1)
            return id[0] == 'Z';

        int hours, minutes = 0, seconds = 0;
        switch (id.Length)
        {
            case 2:
                if (!TryDigit(id[1], out hours)) return false;
                break;
            case 3:
                if (!TryPair(id, 1, out hours)) return false;
                break;
            case 5:
                if (!TryPair(id, 1, out hours) || !TryPair(id, 3, out minutes)) return false;
                break;
            case 6:
                if (!TryPair(id, 1, out hours) || id[3] != ':' || !TryPair(id, 4, out minutes))
                    return false;
                break;
            case 7:
                if (!TryPair(id, 1, out hours) || !TryPair(id, 3, out minutes)
                    || !TryPair(id, 5, out seconds)) return false;
                break;
            case 9:
                if (!TryPair(id, 1, out hours) || id[3] != ':' || !TryPair(id, 4, out minutes)
                    || id[6] != ':' || !TryPair(id, 7, out seconds)) return false;
                break;
            default:
                return false;
        }

        if (id[0] != '+' && id[0] != '-')
            return false;

        if (hours > 18 || minutes > 59 || seconds > 59)
            return false;

        if (hours == 18 && (minutes > 0 || seconds > 0))
            return false;

        var magnitude = new TimeSpan(hours, minutes, seconds);
        offset = id[0] == '-' ? magnitude.Negate() : magnitude;
        return true;
    }

    private static bool IsDigit(char c) => c >= '0' && c <= '9';

    private static bool TryDigit(char c, out int value)
    {
        value = c - '0';
        return IsDigit(c);
    }

    private static bool TryPair(ReadOnlySpan<char> id, int index, out int value)
    {
        value = 0;
        if (!IsDigit(id[index]) || !IsDigit(id[index + 1]))
            return false;

        value = ((id[index] - '0') * 10) + (id[index + 1] - '0');
        return true;
    }
}

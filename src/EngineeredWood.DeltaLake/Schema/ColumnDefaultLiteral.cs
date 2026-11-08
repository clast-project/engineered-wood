// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;
using System.Text.RegularExpressions;

namespace EngineeredWood.DeltaLake.Schema;

/// <summary>
/// Whether a column's default (its <c>CURRENT_DEFAULT</c> field metadata, the SQL text of the expression) is a
/// literal of the column's type, as delta-spark's <c>DeltaColumnDefaults.isLiteral</c> decides it (read with javap
/// from the 4.3.1 jar; IcebergCompatV3 refuses any other default).
///
/// <para>Spark does not parse SQL here. It tries to read the text as a value of the type with a Java parser, and
/// the text is a literal exactly when that parse succeeds:</para>
/// <list type="bullet">
/// <item>byte, short, integer: <c>Integer.valueOf</c> (no range check for byte or short); long: <c>Long.valueOf</c>
/// after one trailing <c>l</c>/<c>L</c> is dropped.</item>
/// <item>float, double: <c>Float.valueOf</c> / <c>Double.valueOf</c>.</item>
/// <item>decimal(p, s): <c>new BigDecimal(text, new MathContext(p)).setScale(s)</c>, which fails when the scale
/// change would have to round.</item>
/// <item>boolean: <c>Boolean.valueOf</c>, which never fails, so any text passes.</item>
/// <item>string: quoted in <c>'</c> or <c>"</c>. date: quoted, then ISO <c>yyyy-MM-dd</c>. timestamp: quoted, then
/// an ISO date-time with an offset, else as timestamp_ntz. timestamp_ntz: quoted, then <c>yyyy-MM-dd HH:mm:ss</c>
/// or an ISO local date-time.</item>
/// <item>binary: text starting with <c>x</c>/<c>X</c> must be followed by a quoted run of hex digits; any other
/// text passes.</item>
/// <item>Any other type (array, map, struct, variant): never a literal.</item>
/// </list>
/// </summary>
internal static class ColumnDefaultLiteral
{
    // Java's Integer/Long.valueOf: an optional sign, then ASCII digits.
    private static readonly Regex IntegerText = new(@"^[+-]?[0-9]+$", RegexOptions.CultureInvariant);

    // Java's FloatingDecimal.readJavaFormatString, after it trims: NaN, Infinity, a decimal or a hexadecimal
    // floating-point number, with an optional f/F/d/D suffix on a number.
    private static readonly Regex FloatText = new(
        @"^[+-]?(NaN|Infinity|(([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?|0[xX]([0-9a-fA-F]+\.?[0-9a-fA-F]*|\.[0-9a-fA-F]+)[pP][+-]?[0-9]+)[fFdD]?)$",
        RegexOptions.CultureInvariant);

    // java.math.BigDecimal(String): an optional sign, digits with an optional point, an optional exponent.
    private static readonly Regex DecimalLiteralText = new(
        @"^([+-]?)([0-9]*)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?$", RegexOptions.CultureInvariant);

    private static readonly Regex DecimalTypeText = new(
        @"^decimal\(\s*([0-9]+)\s*,\s*([0-9]+)\s*\)$", RegexOptions.CultureInvariant);

    // DateTimeFormatter.ISO_LOCAL_DATE, ISO_LOCAL_TIME (seconds and fraction optional) and an ISO offset.
    // ISO_LOCAL_DATE's year (SignStyle.EXCEEDS_PAD, 4 to 9 digits): four digits with no sign, 0000 included, or a
    // sign and more. A plus sign needs more than four digits (+2024 is refused, +10000 accepted); a minus sign takes
    // four (-0001). Measured with jshell.
    private const string IsoDate = @"([0-9]{4}|-[0-9]{4,9}|\+[0-9]{5,9})-([0-9]{2})-([0-9]{2})";
    private const string IsoTime = @"([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\.[0-9]{1,9})?)?";

    private static readonly Regex DateText = new("^" + IsoDate + "$", RegexOptions.CultureInvariant);

    private static readonly Regex LocalDateTimeText =
        new("^" + IsoDate + "T" + IsoTime + "$", RegexOptions.CultureInvariant);

    // "yyyy-MM-dd HH:mm:ss", Spark's other timestamp_ntz pattern.
    private static readonly Regex SpacedDateTimeText = new(
        @"^([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})$", RegexOptions.CultureInvariant);

    // ZoneOffset as ISO_DATE_TIME parses it ("+HH:MM:ss"): Z (either case), or a sign, hours and minutes, and
    // optionally seconds, each part two digits after a colon. +01 and +0100 are refused (measured with jshell).
    private const string IsoOffset = @"([Zz]|[+-][0-9]{2}:[0-9]{2}(?::[0-9]{2})?)";

    // DateTimeFormatter.ISO_DATE_TIME with the offset OffsetDateTime.parse needs, and an optional [zone] after it.
    private static readonly Regex OffsetDateTimeText = new(
        "^" + IsoDate + "T" + IsoTime + IsoOffset + @"(?:\[([^\]]*)\])?$", RegexOptions.CultureInvariant);

    private static readonly Regex OffsetText = new("^" + IsoOffset + "$", RegexOptions.CultureInvariant);

    // ZoneId.of's region-id syntax: a letter, then letters, digits and ~/._+-.
    private static readonly Regex RegionIdText = new(@"^[A-Za-z][A-Za-z0-9~/._+-]+$", RegexOptions.CultureInvariant);

    /// <summary>True when <paramref name="text"/> is a literal of <paramref name="type"/>.</summary>
    public static bool IsLiteral(string text, DeltaDataType type)
    {
        if (type is not PrimitiveType primitive)
            return false;

        string name = primitive.TypeName;
        switch (name)
        {
            case "string":
                return Unquote(text) is not null;
            case "long":
                return IsJavaInteger(text.EndsWith("l", StringComparison.Ordinal)
                    || text.EndsWith("L", StringComparison.Ordinal)
                        ? text.Substring(0, text.Length - 1)
                        : text, long.MinValue, long.MaxValue);
            case "integer" or "short" or "byte":
                return IsJavaInteger(text, int.MinValue, int.MaxValue);
            case "float" or "double":
                return FloatText.IsMatch(TrimJava(text));
            case "boolean":
                return true;
            case "binary":
                if (!text.StartsWith("x", StringComparison.Ordinal) && !text.StartsWith("X", StringComparison.Ordinal))
                    return true;
                return Unquote(text.Substring(1)) is { } hex && hex.All(IsHexDigit);
            case "date":
                return Unquote(text) is { } date && IsDate(DateText.Match(date));
            case "timestamp":
                return Unquote(text) is { } timestamp
                    && (IsOffsetDateTime(OffsetDateTimeText.Match(timestamp)) || IsLocalTimestamp(timestamp));
            case "timestamp_ntz":
                return Unquote(text) is { } local && IsLocalTimestamp(local);
        }

        if (TryParseDecimalType(name, out int precision, out int scale))
            return IsDecimal(text, precision, scale);
        return false;
    }

    /// <summary>Parses a <c>decimal(p,s)</c> type name.</summary>
    public static bool TryParseDecimalType(string typeName, out int precision, out int scale)
    {
        precision = scale = 0;
        var match = DecimalTypeText.Match(typeName);
        return match.Success
            && int.TryParse(match.Groups[1].Value, NumberStyles.None, CultureInfo.InvariantCulture, out precision)
            && int.TryParse(match.Groups[2].Value, NumberStyles.None, CultureInfo.InvariantCulture, out scale);
    }

    // DeltaColumnDefaults.parseString: the text inside matching single or double quotes. Spark then unescapes it,
    // which cannot fail.
    private static string? Unquote(string text) =>
        text.Length > 1
        && ((text[0] == '\'' && text[text.Length - 1] == '\'') || (text[0] == '"' && text[text.Length - 1] == '"'))
            ? text.Substring(1, text.Length - 2)
            : null;

    private static bool IsJavaInteger(string text, long min, long max) =>
        IntegerText.IsMatch(text)
        && long.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out long value)
        && value >= min && value <= max;

    // Java's String.trim, which FloatingDecimal applies: every character up to U+0020 at either end.
    private static string TrimJava(string text)
    {
        int start = 0, end = text.Length;
        while (start < end && text[start] <= ' ')
            start++;
        while (end > start && text[end - 1] <= ' ')
            end--;
        return text.Substring(start, end - start);
    }

    private static bool IsHexDigit(char c) =>
        c is (>= '0' and <= '9') or (>= 'a' and <= 'f') or (>= 'A' and <= 'F');

    // new BigDecimal(text, new MathContext(precision)) rounds to `precision` significant digits, half up; then
    // setScale(scale) without a rounding mode throws when it would have to drop a non-zero digit.
    private static bool IsDecimal(string text, int precision, int scale)
    {
        var match = DecimalLiteralText.Match(text);
        if (!match.Success)
            return false;
        string integerDigits = match.Groups[2].Value;
        string fractionDigits = match.Groups[3].Value;
        if (integerDigits.Length + fractionDigits.Length == 0)
            return false;

        // BigDecimal refuses an exponent, and a scale, that does not fit an int (NumberFormatException).
        long exponent = 0;
        if (match.Groups[4].Success
            && !long.TryParse(match.Groups[4].Value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture,
                out exponent))
        {
            return false;
        }
        if (exponent < int.MinValue || exponent > int.MaxValue)
            return false;

        var unscaled = BigInteger.Parse(integerDigits + fractionDigits, CultureInfo.InvariantCulture);
        long valueScale = fractionDigits.Length - exponent;
        if (valueScale < int.MinValue || valueScale > int.MaxValue)
            return false;

        if (precision > 0)
        {
            int digits;
            while ((digits = DigitCount(unscaled)) > precision)
            {
                int drop = digits - precision;
                var divisor = BigInteger.Pow(10, drop);
                // HALF_UP rounds a half away from zero. `unscaled` is the magnitude (the sign is matched but not
                // applied: whether a value is a literal does not depend on it), so rounding up is away from zero.
                var quotient = BigInteger.DivRem(unscaled, divisor, out var remainder);
                if (remainder * 2 >= divisor)
                    quotient += 1;
                unscaled = quotient;
                valueScale -= drop;
            }
        }

        // Rounding lowers the scale, and BigDecimal refuses one that leaves the int range (ArithmeticException).
        if (valueScale < int.MinValue)
            return false;
        if (unscaled.IsZero)
            return true;

        // setScale raising the scale only appends zeros, but the raise itself must fit an int ("1e2147483647" is
        // refused, measured).
        long raise = scale - valueScale;
        if (raise >= 0)
            return raise <= int.MaxValue;

        // Lowering it drops the last `excess` digits, which must all be zeros. A non-zero value with no more digits
        // than that cannot end in that many zeros, so it is refused without building the power of ten: an exponent
        // such as 1e-1000000000 would otherwise ask for a number of about 400 MB.
        long excess = -raise;
        if (excess >= DigitCount(unscaled))
            return false;
        return BigInteger.Remainder(unscaled, BigInteger.Pow(10, (int)excess)).IsZero;
    }

    private static int DigitCount(BigInteger value) =>
        value.IsZero ? 1 : BigInteger.Abs(value).ToString(CultureInfo.InvariantCulture).Length;

    // OffsetDateTime.parse(text, ISO_DATE_TIME): a valid date-time, an offset within ZoneOffset's range, and, when a
    // [zone] follows, an id ZoneId's region-id parser takes.
    private static bool IsOffsetDateTime(Match match) =>
        IsDateTime(match)
        && IsValidOffset(match.Groups[7].Value)
        && (!match.Groups[8].Success || IsZoneId(match.Groups[8].Value));

    // ZoneOffset.ofHoursMinutesSeconds: hours up to 18, minutes and seconds up to 59, and no more than 18:00 in all.
    private static bool IsValidOffset(string offset)
    {
        if (offset is "Z" or "z")
            return true;
        int hours = int.Parse(offset.Substring(1, 2), CultureInfo.InvariantCulture);
        int minutes = int.Parse(offset.Substring(4, 2), CultureInfo.InvariantCulture);
        int seconds = offset.Length > 6 ? int.Parse(offset.Substring(7, 2), CultureInfo.InvariantCulture) : 0;
        return hours <= 18 && minutes <= 59 && seconds <= 59 && hours * 3600 + minutes * 60 + seconds <= 18 * 3600;
    }

    // What ISO_DATE_TIME's appendZoneRegionId takes (measured with jshell on JDK 17): Z, UTC, GMT and UT; an offset;
    // one of UTC, GMT or UT followed by an offset; or a region id. GMT+1, EST and an empty [] are refused. Java also
    // requires a region id to name a zone in its tz database (Europe/Paris passes, Foo/Bar and europe/paris do not).
    // That needs a tz database, and what .NET offers depends on the platform and on globalization mode, so a region
    // id is checked for its syntax alone. A well-formed id naming no zone passes here and fails in Spark.
    private static bool IsZoneId(string zone)
    {
        if (zone is "Z" or "UTC" or "GMT" or "UT")
            return true;
        foreach (string prefix in new[] { "UTC", "GMT", "UT", "" })
        {
            if (zone.StartsWith(prefix, StringComparison.Ordinal) && zone.Length > prefix.Length
                && zone[prefix.Length] is '+' or '-')
            {
                string offset = zone.Substring(prefix.Length);
                return OffsetText.IsMatch(offset) && IsValidOffset(offset);
            }
        }
        return RegionIdText.IsMatch(zone);
    }

    // "yyyy-MM-dd HH:mm:ss" reads a year of the era, so it refuses year 0000, which ISO_LOCAL_DATE_TIME accepts
    // (measured). Its four-digit year carries no sign.
    private static bool IsLocalTimestamp(string text) =>
        (SpacedDateTimeText.Match(text) is var spaced && IsDateTime(spaced) && spaced.Groups[1].Value != "0000")
        || IsDateTime(LocalDateTimeText.Match(text));

    // java.time's STRICT resolution: the day must exist in the month.
    private static bool IsDate(Match match) =>
        match.Success && IsValidDate(match.Groups[1].Value, match.Groups[2].Value, match.Groups[3].Value);

    private static bool IsDateTime(Match match)
    {
        if (!match.Success || !IsValidDate(match.Groups[1].Value, match.Groups[2].Value, match.Groups[3].Value))
            return false;
        int hour = int.Parse(match.Groups[4].Value, CultureInfo.InvariantCulture);
        int minute = int.Parse(match.Groups[5].Value, CultureInfo.InvariantCulture);
        int second = match.Groups[6].Success ? int.Parse(match.Groups[6].Value, CultureInfo.InvariantCulture) : 0;
        return hour < 24 && minute < 60 && second < 60;
    }

    // The proleptic Gregorian calendar java.time uses, for any year it can parse: year 0 and -4 are leap years, 1900
    // is not.
    private static bool IsValidDate(string year, string month, string day)
    {
        int y = int.Parse(year, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
        int m = int.Parse(month, CultureInfo.InvariantCulture);
        int d = int.Parse(day, CultureInfo.InvariantCulture);
        if (m is < 1 or > 12 || d < 1)
            return false;
        bool leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
        int days = m == 2 ? (leap ? 29 : 28) : m is 4 or 6 or 9 or 11 ? 30 : 31;
        return d <= days;
    }
}

// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Shortens BYTE_ARRAY bounds for a page index, keeping each a valid bound: a minimum no greater than
/// any value it stood for, a maximum no less, and — for UTF-8 — still valid UTF-8, since the format
/// requires a truncated bound to remain a value of its logical type.
/// </summary>
/// <remarks>
/// Follows parquet-java's <c>BinaryTruncator</c>: a minimum becomes a prefix; a maximum becomes a
/// prefix whose last unit is incremented. A maximum that cannot be shortened (every byte is
/// <c>0xFF</c>, or every code point is U+10FFFF) is kept in full.
/// </remarks>
internal static class PageIndexTruncation
{
    /// <summary>A lower bound of at most <paramref name="limit"/> bytes.</summary>
    public static byte[] TruncateMin(byte[] value, int limit, bool utf8)
    {
        if (value.Length <= limit)
            return value;

        int cut = utf8 && IsUtf8Prefix(value, limit) ? CodePointBoundaryAtOrBefore(value, limit) : limit;
        return value.AsSpan(0, cut).ToArray();
    }

    /// <summary>
    /// An upper bound of at most <paramref name="limit"/> bytes, or <paramref name="value"/> itself when
    /// none exists.
    /// </summary>
    public static byte[] TruncateMax(byte[] value, int limit, bool utf8)
    {
        if (value.Length <= limit)
            return value;

        byte[]? shortened = utf8 && IsUtf8Prefix(value, limit)
            ? IncrementUtf8Prefix(value, limit)
            : IncrementBinaryPrefix(value, limit);
        return shortened ?? value;
    }

    private static byte[]? IncrementBinaryPrefix(byte[] value, int limit)
    {
        for (int i = limit - 1; i >= 0; i--)
        {
            if (value[i] == 0xFF)
                continue;

            // Everything after position i is dropped, so the result is shorter than the prefix and
            // greater than every value that starts with value[0..i].
            byte[] result = value.AsSpan(0, i + 1).ToArray();
            result[i]++;
            return result;
        }

        return null;
    }

    private static byte[]? IncrementUtf8Prefix(byte[] value, int limit)
    {
        // Walk back over the code points that fit, trying to increment each in turn. Dropping the ones
        // after it keeps the result above every value with the same leading code points.
        int end = CodePointBoundaryAtOrBefore(value, limit);
        while (end > 0)
        {
            int start = end - 1;
            while (start > 0 && IsContinuation(value[start]))
                start--;

            int codePoint = Decode(value, start, end - start);
            int next = codePoint + 1;
            if (next is >= 0xD800 and <= 0xDFFF)
                next = 0xE000;

            if (next <= 0x10FFFF)
            {
                int length = EncodedLength(next);
                if (start + length <= limit)
                {
                    byte[] result = new byte[start + length];
                    value.AsSpan(0, start).CopyTo(result);
                    Encode(next, result.AsSpan(start));
                    return result;
                }
            }

            end = start;
        }

        return null;
    }

    /// <summary>The largest position no greater than <paramref name="limit"/> that starts a code point.</summary>
    private static int CodePointBoundaryAtOrBefore(byte[] value, int limit)
    {
        int cut = limit;
        while (cut > 0 && IsContinuation(value[cut]))
            cut--;
        return cut;
    }

    private static bool IsContinuation(byte b) => (b & 0xC0) == 0x80;

    /// <summary>
    /// Whether the code points that fit in <paramref name="limit"/> bytes are valid UTF-8. Only that
    /// prefix is kept, so only it is checked; bytes that are not UTF-8 are truncated as binary.
    /// </summary>
    private static bool IsUtf8Prefix(byte[] value, int limit)
    {
        try
        {
            StrictUtf8.GetCharCount(value, 0, CodePointBoundaryAtOrBefore(value, limit));
            return true;
        }
        catch (System.Text.DecoderFallbackException)
        {
            return false;
        }
    }

    private static readonly System.Text.UTF8Encoding StrictUtf8 = new(false, throwOnInvalidBytes: true);

    private static int Decode(byte[] value, int start, int length) => length switch
    {
        1 => value[start],
        2 => ((value[start] & 0x1F) << 6) | (value[start + 1] & 0x3F),
        3 => ((value[start] & 0x0F) << 12) | ((value[start + 1] & 0x3F) << 6) | (value[start + 2] & 0x3F),
        _ => ((value[start] & 0x07) << 18) | ((value[start + 1] & 0x3F) << 12)
            | ((value[start + 2] & 0x3F) << 6) | (value[start + 3] & 0x3F),
    };

    private static int EncodedLength(int codePoint) => codePoint switch
    {
        < 0x80 => 1,
        < 0x800 => 2,
        < 0x10000 => 3,
        _ => 4,
    };

    private static void Encode(int codePoint, Span<byte> destination)
    {
        switch (EncodedLength(codePoint))
        {
            case 1:
                destination[0] = (byte)codePoint;
                break;
            case 2:
                destination[0] = (byte)(0xC0 | (codePoint >> 6));
                destination[1] = (byte)(0x80 | (codePoint & 0x3F));
                break;
            case 3:
                destination[0] = (byte)(0xE0 | (codePoint >> 12));
                destination[1] = (byte)(0x80 | ((codePoint >> 6) & 0x3F));
                destination[2] = (byte)(0x80 | (codePoint & 0x3F));
                break;
            default:
                destination[0] = (byte)(0xF0 | (codePoint >> 18));
                destination[1] = (byte)(0x80 | ((codePoint >> 12) & 0x3F));
                destination[2] = (byte)(0x80 | ((codePoint >> 6) & 0x3F));
                destination[3] = (byte)(0x80 | (codePoint & 0x3F));
                break;
        }
    }
}

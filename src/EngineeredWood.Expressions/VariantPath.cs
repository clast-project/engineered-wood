// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text;

namespace EngineeredWood.Expressions;

/// <summary>One step of a <see cref="VariantPath"/>: an object field by name, or an array element by index.</summary>
public readonly record struct VariantPathSegment
{
    private VariantPathSegment(string? key, int index)
    {
        Key = key;
        Index = index;
    }

    /// <summary>The object field this step selects, or null for an array element.</summary>
    public string? Key { get; }

    /// <summary>The array element this step selects; meaningful only when <see cref="Key"/> is null.</summary>
    public int Index { get; }

    /// <summary>Whether this step selects an object field.</summary>
    public bool IsKey => Key is not null;

    /// <summary>A step selecting the object field <paramref name="key"/>.</summary>
    public static VariantPathSegment Field(string key) =>
        new(key ?? throw new ArgumentNullException(nameof(key)), 0);

    /// <summary>A step selecting array element <paramref name="index"/>.</summary>
    public static VariantPathSegment Element(int index) =>
        index >= 0 ? new(null, index) : throw new ArgumentOutOfRangeException(nameof(index));

    public override string ToString() =>
        Key is { } key ? $"['{key}']" : $"[{Index.ToString(CultureInfo.InvariantCulture)}]";
}

/// <summary>
/// The path argument of Spark's <c>variant_get</c> / <c>try_variant_get</c>: <c>$</c>, then a step per object
/// field or array element, as in <c>$.a.b</c>, <c>$['a b']</c> or <c>$.a[0]</c>.
/// </summary>
/// <remarks>
/// <para>The grammar is Spark's (<c>VariantPathParser</c>), measured against Spark 4.1. A step is <c>.</c> then
/// one or more characters other than <c>.</c> and <c>[</c>; <c>['</c> then characters other than <c>'</c>, then
/// <c>']</c>; the same with double quotes; or <c>[</c> digits <c>]</c>. Nothing is trimmed: <c>$. a</c> names the
/// key <c>" a"</c> and <c>$.a </c> the key <c>"a "</c>. Keys are case-sensitive. <c>$ .a</c>, <c>$.</c>,
/// <c>$..a</c> and <c>$[ 'a' ]</c> are not paths.</para>
/// <para>A text this does not parse may still be one Spark accepts. Since every use is a prune, refusing
/// costs only pruning; a key read differently from Spark would not be safe.</para>
/// </remarks>
public sealed class VariantPath : IEquatable<VariantPath>
{
    private VariantPath(IReadOnlyList<VariantPathSegment> segments) => Segments = segments;

    /// <summary>The root, <c>$</c>: the whole variant.</summary>
    public static VariantPath Root { get; } = new(Array.Empty<VariantPathSegment>());

    /// <summary>The steps from the root, in order.</summary>
    public IReadOnlyList<VariantPathSegment> Segments { get; }

    /// <summary>Whether every step selects an object field (no array element).</summary>
    public bool IsFieldsOnly => Segments.All(s => s.IsKey);

    /// <summary>A path of object fields only.</summary>
    public static VariantPath OfFields(params string[] keys) =>
        new(Array.AsReadOnly(keys.Select(VariantPathSegment.Field).ToArray()));

    /// <summary>A path of the given steps.</summary>
    public static VariantPath Of(IEnumerable<VariantPathSegment> segments) =>
        new(Array.AsReadOnly(segments.ToArray()));

    /// <summary>This path followed by <paramref name="suffix"/>.</summary>
    public VariantPath Append(VariantPath suffix) =>
        suffix.Segments.Count == 0 ? this : Of(Segments.Concat(suffix.Segments));

    /// <summary>Parses Spark path text, or false when it is not one (see the remarks on the type).</summary>
    public static bool TryParse(string? text, out VariantPath path)
    {
        path = Root;
        if (text is null || text.Length == 0 || text[0] != '$')
            return false;

        var segments = new List<VariantPathSegment>();
        int i = 1;
        while (i < text.Length)
        {
            if (text[i] == '.')
            {
                int start = ++i;
                while (i < text.Length && text[i] != '.' && text[i] != '[')
                    i++;
                if (i == start)
                    return false;
                segments.Add(VariantPathSegment.Field(text.Substring(start, i - start)));
            }
            else if (text[i] == '[')
            {
                if (i + 1 >= text.Length)
                    return false;
                char open = text[i + 1];
                if (open is '\'' or '"')
                {
                    int start = i + 2;
                    int close = text.IndexOf(open, start);
                    if (close <= start || close + 1 >= text.Length || text[close + 1] != ']')
                        return false;
                    segments.Add(VariantPathSegment.Field(text.Substring(start, close - start)));
                    i = close + 2;
                }
                else
                {
                    int start = i + 1;
                    int end = start;
                    while (end < text.Length && text[end] is >= '0' and <= '9')
                        end++;
                    if (end == start || end >= text.Length || text[end] != ']'
                        || !int.TryParse(text.Substring(start, end - start), NumberStyles.None,
                            CultureInfo.InvariantCulture, out int index))
                    {
                        return false;
                    }
                    segments.Add(VariantPathSegment.Element(index));
                    i = end + 1;
                }
            }
            else
            {
                return false;
            }
        }

        path = segments.Count == 0 ? Root : new VariantPath(segments.AsReadOnly());
        return true;
    }

    /// <summary>
    /// Path text that <see cref="TryParse"/> (and Spark) read back as this path, or null when a key cannot be
    /// spelled: one holding both kinds of quote and a <c>.</c> or <c>[</c>.
    /// </summary>
    public string? ToPathText()
    {
        var sb = new StringBuilder("$");
        foreach (var segment in Segments)
        {
            if (segment.Key is not { } key)
            {
                sb.Append('[').Append(segment.Index.ToString(CultureInfo.InvariantCulture)).Append(']');
            }
            else if (key.Length > 0 && key.IndexOf('.') < 0 && key.IndexOf('[') < 0)
            {
                sb.Append('.').Append(key);
            }
            else if (key.Length > 0 && key.IndexOf('\'') < 0)
            {
                sb.Append("['").Append(key).Append("']");
            }
            else if (key.Length > 0 && key.IndexOf('"') < 0)
            {
                sb.Append("[\"").Append(key).Append("\"]");
            }
            else
            {
                return null;
            }
        }
        return sb.ToString();
    }

    public bool Equals(VariantPath? other) =>
        other is not null && Segments.SequenceEqual(other.Segments);

    public override bool Equals(object? obj) => Equals(obj as VariantPath);

    public override int GetHashCode()
    {
        int hash = 17;
        foreach (var segment in Segments)
            hash = unchecked(hash * 31 + segment.GetHashCode());
        return hash;
    }

    public override string ToString() => ToPathText() ?? "$" + string.Concat(Segments);
}

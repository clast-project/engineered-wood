// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;

namespace EngineeredWood.IO;

/// <summary>
/// <para>An absolute storage location: a local file or directory, or an object or prefix in an object store, named
/// the way table formats name them (<c>s3://bucket/key</c>, <c>abfss://container@account.dfs.core.windows.net/path</c>,
/// <c>file:/C:/wh/t</c>, <c>C:\wh\t</c>, <c>/tmp/wh/t</c>).</para>
///
/// <para>This is its own parser, not <see cref="System.Uri"/>, because <see cref="System.Uri"/> changes which object a
/// path names (#963): it collapses <c>.</c> and <c>..</c> for every scheme, decodes <c>%2E%2E</c> before collapsing it
/// on <c>https</c>, and lowercases hosts. Here the path is kept exactly as given, and only what a scheme defines is
/// normalized:</para>
/// <list type="bullet">
/// <item><c>s3a</c> and <c>s3n</c> are <c>s3</c>.</item>
/// <item><c>abfs[s]://container@account.dfs.core.windows.net/p</c>, <c>wasb[s]://container@account.blob.core.windows.net/p</c>
/// and <c>https://account.blob.core.windows.net/container/p</c> are one Azure blob location.</item>
/// <item>A <c>file</c> location uses <c>/</c> separators, collapses <c>.</c> and <c>..</c>, and compares
/// case-insensitively on Windows.</item>
/// <item>Nothing is percent-decoded. A <c>%</c> is a literal character of the name, as it is for Iceberg Java's file
/// IO and for the Hive-escaped partition directories that Spark writes.</item>
/// <item>A trailing <c>/</c> is not part of the location: <c>s3://b/t/</c> and <c>s3://b/t</c> are the same table
/// location.</item>
/// </list>
///
/// <para>A segment that is exactly <c>.</c> or <c>..</c> in any location other than <c>file</c> is REFUSED. In an
/// object store such a segment is a legal, literal key segment (S3 permits <c>a/../b</c> as a key distinct from
/// <c>b</c>). The engines measured in #963 do not agree on what such a path names: some collapse it (and then reach
/// outside the table, or even the bucket), some request the literal key, some reject or rewrite it, and none reads
/// the literal key on its default path. No reading of it is portable, so the refusal is permanent: EW neither reads
/// nor writes such a location. A <c>%2E%2E</c> segment is not a dot segment; like every <c>%</c>, it is literal.</para>
///
/// <para>An empty segment (<c>s3://b/t//x</c>) or a <c>\</c> in an object-store location is refused too: the object
/// stores' file systems strip a leading <c>/</c> and turn <c>\</c> into <c>/</c>, so they would read a different key
/// than the location names.</para>
///
/// <para>On Windows a <c>file</c> URI treats <c>\</c> as a separator, as the operating system does (a bare path always
/// does), and a drive (<c>C:</c>) anywhere but at the start of a local path is refused: below a root, Windows would
/// take it as a new root.</para>
///
/// <para>Equality and hashing use the normalized form, so one location has one identity however it was spelled.
/// <see cref="ToString"/> keeps the spelling it was given, so a location EW derives from a table's own location is
/// written back in that table's spelling.</para>
/// </summary>
public sealed class StorageLocation : IEquatable<StorageLocation>
{
    private static readonly bool IsWindows = Path.DirectorySeparatorChar == '\\';

    private readonly string _text;

    // The normalized identity: scheme, authority, and the path's segments (no leading or trailing empty segment).
    private readonly string _keyScheme;
    private readonly string _keyAuthority;
    private readonly string[] _segments;

    private StorageLocation(string text, string scheme, string keyScheme, string keyAuthority, string[] segments)
    {
        _text = text;
        Scheme = scheme;
        _keyScheme = keyScheme;
        _keyAuthority = keyAuthority;
        _segments = segments;
    }

    /// <summary>The scheme as given, lowercased (<c>s3a</c>, <c>abfss</c>, <c>file</c>).</summary>
    public string Scheme { get; }

    /// <summary>True for a location on the local file system (<c>file</c> URIs and bare local paths).</summary>
    public bool IsFile => _keyScheme == "file";

    /// <summary>
    /// For a <see cref="IsFile">file</see> location, the path in the form the local operating system opens:
    /// <c>C:\wh\t</c>, <c>\\server\share\x</c> or <c>/tmp/wh/t</c>. <see langword="null"/> for any other location.
    /// </summary>
    public string? LocalPath
    {
        get
        {
            if (!IsFile)
                return null;
            string joined = string.Join("/", _segments);
            string path = _keyAuthority.Length > 0
                ? "//" + _keyAuthority + "/" + joined
                : IsDriveSegment(_segments) ? joined + (_segments.Length == 1 ? "/" : "") : "/" + joined;
            return IsWindows ? path.Replace('/', '\\') : path;
        }
    }

    /// <summary>
    /// Parses an absolute location. Throws <see cref="FormatException"/> for text that is not one (a relative
    /// path, an empty string, a malformed URI) and <see cref="NotSupportedException"/> for a <c>.</c> or <c>..</c>
    /// segment, an empty segment or a <c>\</c> outside a <c>file</c> location.
    /// </summary>
    public static StorageLocation Parse(string location)
    {
        if (location is null)
            throw new ArgumentNullException(nameof(location));
        return ParseCore(location, decode: false, throwOnError: true)!;
    }

    /// <summary>
    /// <para>Parses a location given as a percent-encoded URI, as Delta's absolute deletion-vector paths are. The
    /// URI's structure (scheme, authority, path, and any query or fragment, which a location cannot have) is read from
    /// the text as given, and only then is each path segment decoded. So <c>a%23b</c> is the segment <c>a#b</c>, not
    /// the start of a fragment, as with <c>java.net.URI</c>.</para>
    /// <para>A segment that decodes to <c>.</c> or <c>..</c>, or to text with a <c>/</c>, a <c>\</c> or a control
    /// character, is refused (<see cref="NotSupportedException"/>), so a decoded segment can neither navigate nor
    /// become two. A malformed escape (<c>%ZZ</c>, a trailing <c>%</c>) is refused (<see cref="FormatException"/>), as
    /// <c>java.net.URI</c> refuses it, and so is a raw ASCII character a URI cannot hold (a space, <c>\</c>,
    /// <c>"</c>, <c>&lt;</c>, <c>&gt;</c>, <c>^</c>, <c>`</c>, <c>{</c>, <c>|</c>, <c>}</c>, <c>[</c>, <c>]</c>), which
    /// would otherwise mean one file on Windows and another elsewhere. <see cref="ToString"/> keeps the encoded
    /// spelling; equality uses the decoded segments.</para>
    /// </summary>
    public static StorageLocation ParsePercentEncoded(string location)
    {
        if (location is null)
            throw new ArgumentNullException(nameof(location));
        return ParseCore(location, decode: true, throwOnError: true)!;
    }

    /// <summary>
    /// Parses an absolute location, returning <see langword="false"/> for anything <see cref="Parse"/> would refuse.
    /// </summary>
    public static bool TryParse(string? location, out StorageLocation? result)
    {
        result = location is null ? null : ParseCore(location, decode: false, throwOnError: false);
        return result is not null;
    }

    /// <summary>
    /// True when <paramref name="text"/> names an absolute location (it has a URI scheme, or is a rooted local path)
    /// rather than a path relative to some base. It says nothing about whether <see cref="Parse"/> accepts it.
    /// </summary>
    public static bool LooksAbsolute(string text)
    {
        if (string.IsNullOrEmpty(text))
            return false;
        if (text[0] is '/' or '\\')
            return true;
        if (IsDriveLetterPath(text))
            return true;
        return SchemeLength(text) > 0;
    }

    /// <summary>
    /// The location <paramref name="relativePath"/> names under this one. Segments are separated by <c>/</c> and
    /// appended as given. In a <c>file</c> location <c>.</c> and <c>..</c> navigate, and climbing above this location
    /// is refused; anywhere else they are refused (see the type's remarks). An absolute path or URI is refused too:
    /// parse it instead.
    /// </summary>
    public StorageLocation Combine(string relativePath)
    {
        if (relativePath is null)
            throw new ArgumentNullException(nameof(relativePath));
        if (LooksAbsolute(relativePath))
            throw new ArgumentException(
                $"'{relativePath}' is absolute; parse it as a location rather than combining it with '{this}'.",
                nameof(relativePath));

        // In a file location \ separates segments where the operating system takes it so (Windows), as in a parsed
        // file URI; elsewhere it is a file-name character.
        string rel = IsFile && IsWindows ? relativePath.Replace('\\', '/') : relativePath;
        rel = rel.TrimEnd('/');
        if (rel.Length == 0)
            return this;

        string[] parts = rel.Split('/');
        var segments = new List<string>(_segments);
        int floor = _segments.Length;
        foreach (string part in parts)
        {
            if (part is "." or "..")
            {
                if (!IsFile)
                    throw DotSegmentRefusal(relativePath);
                if (part == ".")
                    continue;
                if (segments.Count <= floor)
                    throw new ArgumentException(
                        $"'{relativePath}' climbs above '{this}'.", nameof(relativePath));
                segments.RemoveAt(segments.Count - 1);
                continue;
            }

            if (IsFile && part.Length == 0)
                continue;
            if (!IsFile && (part.Length == 0 || part.IndexOf('\\') >= 0))
                throw UnreachableKeyRefusal(relativePath);
            if (IsFile && IsMisplacedDrive(part, segments.Count, _keyAuthority))
                throw new ArgumentException(
                    $"'{relativePath}' has a drive segment, which would root it outside '{this}'.", nameof(relativePath));
            segments.Add(part);
        }

        string text = _text + (_text.EndsWith("/", StringComparison.Ordinal) ? "" : "/") +
            string.Join("/", segments.Skip(_segments.Length));
        return new StorageLocation(text, Scheme, _keyScheme, _keyAuthority, segments.ToArray());
    }

    /// <summary>
    /// True when this location is <paramref name="prefix"/> or lies under it. Segments are compared whole, so
    /// <c>s3://b/table-other</c> is not under <c>s3://b/table</c>.
    /// </summary>
    public bool IsUnder(StorageLocation prefix) => RelativeSegmentsFrom(prefix) >= 0;

    /// <summary>
    /// The path of this location relative to <paramref name="prefix"/>, with <c>/</c> separators: <c>""</c> when the
    /// two are the same location. Throws <see cref="ArgumentException"/> when this location is not under it.
    /// </summary>
    public string RelativeTo(StorageLocation prefix)
    {
        int start = RelativeSegmentsFrom(prefix);
        if (start < 0)
            throw new ArgumentException($"'{this}' is not under '{prefix}'.", nameof(prefix));
        return string.Join("/", _segments, start, _segments.Length - start);
    }

    /// <summary>The number of path segments this location has after its authority.</summary>
    internal int SegmentCount => _segments.Length;

    private int RelativeSegmentsFrom(StorageLocation prefix)
    {
        if (prefix is null)
            throw new ArgumentNullException(nameof(prefix));
        if (_keyScheme != prefix._keyScheme || !AuthorityEquals(prefix) || prefix._segments.Length > _segments.Length)
            return -1;
        for (int i = 0; i < prefix._segments.Length; i++)
        {
            if (!string.Equals(_segments[i], prefix._segments[i], SegmentComparison))
                return -1;
        }

        return prefix._segments.Length;
    }

    private StringComparison SegmentComparison =>
        IsFile && IsWindows ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

    private bool AuthorityEquals(StorageLocation other) =>
        string.Equals(_keyAuthority, other._keyAuthority, IsFile ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);

    /// <summary>The location as it was given (without a trailing <c>/</c>), or as built by <see cref="Combine"/>.</summary>
    public override string ToString() => _text;

    /// <inheritdoc/>
    public bool Equals(StorageLocation? other)
    {
        if (other is null)
            return false;
        if (ReferenceEquals(this, other))
            return true;
        return _segments.Length == other._segments.Length && RelativeSegmentsFrom(other) == _segments.Length;
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj) => Equals(obj as StorageLocation);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        StringComparer comparer = IsFile && IsWindows ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
        int hash = StringComparer.Ordinal.GetHashCode(_keyScheme);
        hash = unchecked(hash * 31 + StringComparer.OrdinalIgnoreCase.GetHashCode(_keyAuthority));
        foreach (string segment in _segments)
            hash = unchecked(hash * 31 + comparer.GetHashCode(segment));
        return hash;
    }

    /// <summary>Compares two locations by their normalized form.</summary>
    public static bool operator ==(StorageLocation? left, StorageLocation? right) =>
        left is null ? right is null : left.Equals(right);

    /// <summary>Compares two locations by their normalized form.</summary>
    public static bool operator !=(StorageLocation? left, StorageLocation? right) => !(left == right);

    // ---------------------------------------------------------------- parsing

    private static StorageLocation? ParseCore(string location, bool decode, bool throwOnError)
    {
        if (location.Length == 0)
            return Fail(throwOnError, "A storage location cannot be empty.");
        foreach (char c in location)
        {
            if (char.IsControl(c))
                return Fail(throwOnError, $"Storage location '{location}' contains a control character.");

            // A URI cannot hold these ASCII characters raw (RFC 3986; java.net.URI refuses each, MEASURED on Java
            // 17), so a reader that parses the location as a URI, as delta-spark does, would refuse it. '[' and ']'
            // are refused in the path below: around an IPv6 host they are legal.
            if (decode && c is ' ' or '"' or '<' or '>' or '\\' or '^' or '`' or '{' or '|' or '}')
                return Fail(throwOnError,
                    $"URI '{location}' contains a raw '{c}', which a URI cannot hold; it must be percent-encoded.");
        }

        if (IsDriveLetterPath(location) || location[0] is '/' or '\\')
            return ParseLocalPath(location, decode, throwOnError);

        int schemeLength = SchemeLength(location);
        if (schemeLength == 0)
            return Fail(throwOnError,
                $"'{location}' is not an absolute storage location: it has no scheme and is not a rooted local path.");

        string scheme = location.Substring(0, schemeLength).ToLowerInvariant();
        string rest = location.Substring(schemeLength + 1);
        if (scheme == "file")
            return ParseFileUri(location, rest, decode, throwOnError);

        if (!rest.StartsWith("//", StringComparison.Ordinal))
            return Fail(throwOnError, $"Storage location '{location}' has no authority ('{scheme}://<authority>/...').");
        rest = rest.Substring(2);
        int slash = rest.IndexOf('/');
        string authority = slash < 0 ? rest : rest.Substring(0, slash);
        string path = slash < 0 ? "" : rest.Substring(slash + 1);
        if (authority.Length == 0)
            return Fail(throwOnError, $"Storage location '{location}' has an empty authority.");
        // In http(s), and in any location read as a URI, ? and # start a query and a fragment; elsewhere they are
        // ordinary key characters.
        if (authority.IndexOfAny(['?', '#']) >= 0 ||
            ((decode || scheme is "http" or "https") && path.IndexOfAny(['?', '#']) >= 0))
            return Fail(throwOnError, $"Storage location '{location}' has a query or fragment, which a storage location cannot.");
        if (decode && path.IndexOfAny(['[', ']']) >= 0)
            return Fail(throwOnError, RawBracketMessage(location));

        string text = location.TrimEnd('/');
        if (text.Length < schemeLength + 3 + authority.Length)
            text = location.Substring(0, schemeLength + 3 + authority.Length);
        string[] segments = SplitPath(path.TrimEnd('/'));
        for (int i = 0; i < segments.Length; i++)
        {
            string segment = segments[i];
            if (segment is "." or "..")
            {
                if (throwOnError)
                    throw DotSegmentRefusal(location);
                return null;
            }

            if (segment.Length == 0 || segment.IndexOf('\\') >= 0)
            {
                if (throwOnError)
                    throw UnreachableKeyRefusal(location);
                return null;
            }

            if (decode)
            {
                string? decoded = DecodeSegment(location, segment, throwOnError);
                if (decoded is null)
                    return null;
                segments[i] = decoded;
            }
        }

        switch (scheme)
        {
            case "s3":
            case "s3a":
            case "s3n":
                return new StorageLocation(text, scheme, "s3", authority, segments);

            case "abfs":
            case "abfss":
            case "wasb":
            case "wasbs":
            {
                int at = authority.IndexOf('@');
                if (at <= 0 || at == authority.Length - 1)
                    return Fail(throwOnError,
                        $"Azure location '{location}' must name its container: '{scheme}://<container>@<account host>/...'.");
                string container = authority.Substring(0, at);
                string host = BlobHost(authority.Substring(at + 1));
                return new StorageLocation(text, scheme, "azure", host, Prepend(container, segments));
            }

            case "http":
            case "https":
            {
                string host = authority.ToLowerInvariant();
                if (IsAzureStorageHost(host))
                {
                    // The blob endpoint names the container as the first segment.
                    return new StorageLocation(text, scheme, "azure", BlobHost(host), segments);
                }

                return new StorageLocation(text, scheme, scheme, host, segments);
            }

            default:
                return new StorageLocation(text, scheme, scheme, authority, segments);
        }
    }

    private static StorageLocation? ParseFileUri(string location, string rest, bool decode, bool throwOnError)
    {
        // file:/C:/x and file:/tmp/x (Hadoop), file:///C:/x and file:///tmp/x (RFC 8089), file://localhost/x,
        // file://server/share/x (UNC). A query or fragment cannot be part of a file location.
        string authority = "";
        string path;
        if (rest.StartsWith("//", StringComparison.Ordinal))
        {
            string afterSlashes = rest.Substring(2);
            int slash = afterSlashes.IndexOf('/');
            authority = slash < 0 ? afterSlashes : afterSlashes.Substring(0, slash);
            path = slash < 0 ? "/" : afterSlashes.Substring(slash);
            if (string.Equals(authority, "localhost", StringComparison.OrdinalIgnoreCase))
                authority = "";
        }
        else if (rest.StartsWith("/", StringComparison.Ordinal))
        {
            path = rest;
        }
        else
        {
            return Fail(throwOnError, $"File location '{location}' is not absolute.");
        }

        if (path.IndexOfAny(['?', '#']) >= 0)
            return Fail(throwOnError, $"File location '{location}' has a query or fragment.");
        if (decode && path.IndexOfAny(['[', ']']) >= 0)
            return Fail(throwOnError, RawBracketMessage(location));

        // Windows opens a path with \ as it does one with /, so \ separates segments here as it does in a bare path.
        if (IsWindows)
            path = path.Replace('\\', '/');

        // /C:/x is the drive path C:/x. Under a server (file://server/C:/x) it stays a segment, which is refused.
        string localPath;
        if (authority.Length > 0)
            localPath = "//" + authority + path;
        else if (path.Length >= 3 && path[0] == '/' && IsDriveLetterPath(path.Substring(1)))
            localPath = path.Substring(1);
        else
            localPath = path;
        return BuildFile(location, localPath, decode, throwOnError);
    }

    private static StorageLocation? ParseLocalPath(string location, bool decode, bool throwOnError)
    {
        string text = location.Replace('\\', '/');
        if (IsDriveLetterPath(text) && (text.Length == 2 || text[2] != '/'))
            return Fail(throwOnError, $"'{location}' is relative to the current directory of drive {text[0]}:.");
        return BuildFile(null, text, decode, throwOnError);
    }

    /// <summary>Builds a file location from a <c>/</c>-separated local path: <c>C:/x</c>, <c>//server/share/x</c> or
    /// <c>/x</c>. <paramref name="uriText"/> is the URI it was given as, or null for a bare path.</summary>
    private static StorageLocation? BuildFile(string? uriText, string localPath, bool decode, bool throwOnError)
    {
        string authority = "";
        string path = localPath;
        if (path.StartsWith("//", StringComparison.Ordinal))
        {
            int slash = path.IndexOf('/', 2);
            authority = slash < 0 ? path.Substring(2) : path.Substring(2, slash - 2);
            path = slash < 0 ? "" : path.Substring(slash);
            if (authority.Length == 0)
                return Fail(throwOnError, $"'{uriText ?? localPath}' names no UNC server.");

            // \\?\ and \\.\ device paths (file://?/ and file://./ too) name things a table format never points at.
            if (authority is "?" or ".")
                return Fail(throwOnError, $"Device path '{uriText ?? localPath}' is not a storage location.");

            // A UNC server is a host name: file://server:80/x and file://user@server/x name no share an OS opens,
            // and in file://serv?er/x or file://server#f a query or fragment would start inside it.
            if (authority.IndexOfAny([':', '@', '?', '#']) >= 0)
                return Fail(throwOnError,
                    $"'{uriText ?? localPath}' names the server '{authority}'; a file location's server cannot have a " +
                    "port, user information, a query or a fragment.");
        }

        var segments = new List<string>();
        int floor = 0;
        string[] parts = path.Split('/');
        for (int i = 0; i < parts.Length; i++)
        {
            string part = parts[i];
            if (decode && part is not ("" or "." or ".."))
            {
                string? decoded = DecodeSegment(uriText ?? localPath, part, throwOnError);
                if (decoded is null)
                    return null;
                part = decoded;
            }

            if (part.Length == 0 || part == ".")
                continue;
            if (part == "..")
            {
                if (segments.Count <= floor)
                    return Fail(throwOnError, $"'{uriText ?? localPath}' climbs above its root.");
                segments.RemoveAt(segments.Count - 1);
                continue;
            }

            if (IsMisplacedDrive(part, segments.Count, authority))
                return Fail(throwOnError,
                    $"'{uriText ?? localPath}' has a drive segment '{part}' below its root, which would root the path elsewhere.");

            segments.Add(part);
            // A drive (C:) or a UNC share is the root: nothing climbs above it.
            if (segments.Count == 1 && (IsDriveSegment(part) || authority.Length > 0))
                floor = 1;
        }

        string text;
        if (uriText is not null)
        {
            text = uriText.TrimEnd('/');
            if (text.EndsWith(":", StringComparison.Ordinal) || text.Length < "file:/".Length)
                text = uriText;
        }
        else
        {
            // A bare path is formatted as the URI other engines read: file:///C:/x, file:///tmp/x, file://server/x.
            var sb = new StringBuilder("file://");
            if (authority.Length > 0)
                sb.Append(authority);
            foreach (string segment in segments)
                sb.Append('/').Append(segment);
            if (segments.Count == 0)
                sb.Append('/');
            text = sb.ToString();
        }

        return new StorageLocation(text, "file", "file", authority, segments.ToArray());
    }

    /// <summary>A drive segment (<c>C:</c>) anywhere but first in a path with no UNC server.</summary>
    private static bool IsMisplacedDrive(string segment, int index, string authority) =>
        IsDriveSegment(segment) && (index > 0 || authority.Length > 0);

    /// <summary>One percent-decoded path segment, or a refusal when it decodes to anything but one plain segment.</summary>
    private static string? DecodeSegment(string location, string segment, bool throwOnError)
    {
        if (segment.IndexOf('%') < 0)
            return segment;

        // Uri.UnescapeDataString leaves a malformed escape (%ZZ, a trailing %) as text; java.net.URI refuses it.
        for (int i = segment.IndexOf('%'); i >= 0; i = segment.IndexOf('%', i + 1))
        {
            if (i + 2 >= segment.Length || !IsHexDigit(segment[i + 1]) || !IsHexDigit(segment[i + 2]))
                return throwOnError
                    ? throw new FormatException($"'{location}' has a malformed percent-escape in its segment '{segment}'.")
                    : null;
        }

        string decoded = Uri.UnescapeDataString(segment);
        bool plain = decoded is not ("" or "." or "..") && decoded.IndexOfAny(['/', '\\']) < 0;
        foreach (char c in decoded)
            plain &= !char.IsControl(c);
        if (plain)
            return decoded;
        if (throwOnError)
            throw new NotSupportedException(
                $"'{location}' has a segment '{segment}' that decodes to '{decoded}', which is not one plain segment " +
                "(it is '.' or '..', or has a separator or a control character). EW refuses such locations (#963).");
        return null;
    }

    private static string[] SplitPath(string path) => path.Length == 0 ? [] : path.Split('/');

    private static string[] Prepend(string first, string[] rest)
    {
        var all = new string[rest.Length + 1];
        all[0] = first;
        Array.Copy(rest, 0, all, 1, rest.Length);
        return all;
    }

    /// <summary>The DNS suffixes of Azure Storage's public and sovereign clouds.</summary>
    private static readonly string[] AzureStorageSuffixes =
        ["core.windows.net", "core.chinacloudapi.cn", "core.usgovcloudapi.net"];

    /// <summary>True for an Azure Storage blob or DFS endpoint, <c>&lt;account&gt;.(blob|dfs).&lt;cloud suffix&gt;</c>
    /// (a port allowed), and for nothing else: <c>service.blob.example.com</c> is an ordinary HTTP host.</summary>
    private static bool IsAzureStorageHost(string host)
    {
        int colon = host.IndexOf(':');
        string name = colon < 0 ? host : host.Substring(0, colon);
        int dot = name.IndexOf('.');
        if (dot <= 0)
            return false;
        string rest = name.Substring(dot + 1);
        foreach (string service in new[] { "blob.", "dfs." })
        {
            if (!rest.StartsWith(service, StringComparison.Ordinal))
                continue;
            string suffix = rest.Substring(service.Length);
            foreach (string cloud in AzureStorageSuffixes)
            {
                if (suffix == cloud)
                    return true;
            }
        }

        return false;
    }

    /// <summary>The blob endpoint of an Azure storage host: <c>account.dfs.core.windows.net</c> becomes
    /// <c>account.blob.core.windows.net</c>. Hosts are case-insensitive, so it is lowercased.</summary>
    private static string BlobHost(string host)
    {
        host = host.ToLowerInvariant();
        int dfs = host.IndexOf(".dfs.", StringComparison.Ordinal);
        return dfs > 0 ? host.Substring(0, dfs) + ".blob." + host.Substring(dfs + ".dfs.".Length) : host;
    }

    /// <summary>The length of a URI scheme at the start of <paramref name="text"/> (before its <c>:</c>), or 0. A
    /// single letter followed by <c>:</c> is a drive, not a scheme.</summary>
    private static int SchemeLength(string text)
    {
        if (text.Length == 0 || !IsAsciiLetter(text[0]))
            return 0;
        for (int i = 1; i < text.Length; i++)
        {
            char c = text[i];
            if (c == ':')
                return i >= 2 ? i : 0;
            if (!(IsAsciiLetter(c) || c is >= '0' and <= '9' || c is '+' or '-' or '.'))
                return 0;
        }

        return 0;
    }

    private static bool IsDriveLetterPath(string text) =>
        text.Length >= 2 && IsAsciiLetter(text[0]) && text[1] == ':' && (text.Length == 2 || text[2] is '/' or '\\');

    private static bool IsDriveSegment(string segment) =>
        segment.Length == 2 && IsAsciiLetter(segment[0]) && segment[1] == ':';

    private static bool IsDriveSegment(string[] segments) => segments.Length > 0 && IsDriveSegment(segments[0]);

    private static bool IsHexDigit(char c) => c is >= '0' and <= '9' or >= 'a' and <= 'f' or >= 'A' and <= 'F';

    private static bool IsAsciiLetter(char c) => c is >= 'a' and <= 'z' or >= 'A' and <= 'Z';

    private static StorageLocation? Fail(bool throwOnError, string message) =>
        throwOnError ? throw new FormatException(message) : null;

    private static string RawBracketMessage(string location) =>
        $"URI '{location}' has a raw '[' or ']' in its path, which a URI cannot hold there (only around an IPv6 " +
        "host); it must be percent-encoded.";

    private static NotSupportedException UnreachableKeyRefusal(string path) =>
        new($"'{path}' has an empty segment or a '\\' in an object-store location. The object stores' file systems " +
            "strip a leading '/' and turn '\\' into '/', so they would read a different key than it names. EW refuses " +
            "such locations (#963).");

    private static NotSupportedException DotSegmentRefusal(string path) =>
        new($"'{path}' has a '.' or '..' segment in an object-store location. In an object store such a segment " +
            "is a literal part of the key, and engines disagree on whether it means that key or navigation (some " +
            "collapse it out of the table or bucket), so no reading of it is portable. EW refuses such locations (#963).");
}

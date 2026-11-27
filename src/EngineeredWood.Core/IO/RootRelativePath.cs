// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.IO;

/// <summary>
/// <para>The checks an <see cref="ITableFileSystem"/> applies to the paths it is handed, so that a path relative to
/// its root names something under that root and nothing else (#962).</para>
///
/// <para>A table format builds those paths from what it READ: a Delta log's <c>add.path</c>, a percent-decoded
/// deletion-vector path, an Iceberg manifest entry. Without these checks a crafted table could read, overwrite or
/// delete files outside itself: MEASURED on the local backend before #962, <c>../x</c>, <c>..\x</c>, an absolute
/// path with either separator, a UNC or <c>\\?\</c> path, a drive-relative <c>C:x</c> and the device name
/// <c>NUL</c> all reached outside the root, for reads, writes, deletes and listings alike. The Azure SDK collapses
/// <c>..</c> in a blob name before it sends the request (MEASURED: <c>tables/t/../x</c> requests
/// <c>tables/x</c>), so on Azure enough <c>..</c> segments reach other containers.</para>
///
/// <para>An absolute location is not a path for these file systems at all: resolve it through a
/// <see cref="StorageResolver"/>, which hands the file system that covers it a path relative to that one's root.</para>
/// </summary>
public static class RootRelativePath
{
    /// <summary>
    /// <para>Refuses a path a dot-collapsing file system (a local directory, or a wrapper over one) would resolve
    /// outside its root: a rooted path or absolute URI, or one whose <c>..</c> segments climb above the root at any point, even
    /// when later segments come back down (<c>../root/x</c>). <c>.</c> and <c>..</c> that stay inside are
    /// allowed, and collapse.</para>
    /// </summary>
    /// <param name="path">The path relative to the root.</param>
    /// <param name="backslashSeparates">Whether <c>\</c> separates segments, as it does in a Windows path. A
    /// wrapper that does not know which backend it wraps should pass <see langword="true"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="path"/> is rooted or climbs above the root.</exception>
    public static void ThrowIfEscapesRoot(string path, bool backslashSeparates)
    {
        if (path is null)
            throw new ArgumentNullException(nameof(path));
        if (path.Length == 0)
            return;

        if (IsSeparator(path[0], backslashSeparates) || IsUri(path))
            throw Rooted(path);

        int depth = 0;
        int start = 0;
        for (int i = 0; i <= path.Length; i++)
        {
            if (i < path.Length && !IsSeparator(path[i], backslashSeparates))
                continue;

            int length = i - start;
            if (length == 2 && path[start] == '.' && path[start + 1] == '.')
            {
                if (--depth < 0)
                    throw Climbs(path);
            }
            else if (length > 0 && !(length == 1 && path[start] == '.'))
            {
                depth++;
            }

            start = i + 1;
        }
    }

    /// <summary>
    /// <para>Checks a path for an object store and returns it as the key suffix to append to the store's root
    /// prefix, with <c>\</c> turned into <c>/</c> as the object-store file systems have always done.</para>
    ///
    /// <para>Refuses a rooted path, which the object-store file systems used to strip silently and so read a key
    /// under the root that the path did not name; an absolute URI or Windows drive path, which they read as a key
    /// under the root (<c>root/s3://b/x</c>); and any <c>.</c> or <c>..</c> segment. In an object store such a
    /// segment is a literal part of the key (S3 and GCS send it as one, MEASURED), but the Azure SDK collapses it
    /// before sending, and engines disagree on which reading is right, so EW refuses it permanently, as
    /// <see cref="StorageLocation"/> does (#963). Percent-encoded forms such as <c>%2E%2E</c> are literal key
    /// characters to every backend (MEASURED: each SDK encodes the <c>%</c>), so they are allowed.</para>
    /// </summary>
    /// <param name="path">The path relative to the root.</param>
    /// <returns><paramref name="path"/> with <c>\</c> replaced by <c>/</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="path"/> is rooted or has a <c>.</c> or <c>..</c>
    /// segment.</exception>
    public static string ToObjectKey(string path)
    {
        if (path is null)
            throw new ArgumentNullException(nameof(path));

        string key = path.Replace('\\', '/');
        if (key.Length == 0)
            return key;
        if (key[0] == '/' || IsUri(key) || StorageLocation.IsDriveLetterPath(key))
            throw Rooted(path);

        foreach (string segment in key.Split('/'))
        {
            if (segment is "." or "..")
            {
                throw new ArgumentException(
                    $"'{path}' has a '.' or '..' segment. In an object store such a segment is a literal part of the " +
                    "key, but some clients collapse it (the Azure SDK does, out of the table and even the container), " +
                    "and engines disagree on which reading is right, so EW refuses it (#962, #963).",
                    nameof(path));
            }
        }

        return key;
    }

    private static bool IsSeparator(char c, bool backslashSeparates) => c == '/' || (backslashSeparates && c == '\\');

    /// <summary>
    /// An absolute URI (<c>s3://b/x</c>, <c>file:/x</c>): a scheme, then <c>:/</c>. Before #962 the object stores
    /// read one as a key under the root (<c>root/s3://b/x</c>). A colon alone is not enough, since <c>db.t:v1/x</c> is
    /// a legal relative key.
    /// </summary>
    private static bool IsUri(string path)
    {
        int scheme = StorageLocation.SchemeLength(path);
        return scheme > 0 && scheme + 1 < path.Length && path[scheme + 1] is '/' or '\\';
    }

    internal static ArgumentException Rooted(string path) =>
        new($"'{path}' is rooted or absolute, but this file system takes paths relative to its root. Resolve an absolute " +
            $"location through a {nameof(StorageResolver)} instead (#962).", nameof(path));

    internal static ArgumentException Climbs(string path) =>
        new($"'{path}' climbs above this file system's root with '..'. A path it is handed must stay under its root " +
            "(#962).", nameof(path));

    internal static ArgumentException Outside(string path) =>
        new($"'{path}' resolves outside this file system's root. A path it is handed must stay under its root (#962).",
            nameof(path));
}

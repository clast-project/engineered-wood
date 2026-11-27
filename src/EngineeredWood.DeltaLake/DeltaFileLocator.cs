// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake;

/// <summary>
/// <para>Finds the file an action names. An <c>add</c>, <c>remove</c> or <c>cdc</c> path is "a relative path ... or an
/// absolute path" (PROTOCOL.md), URL-encoded either way, and an absolute-path (<c>p</c>) deletion vector names its
/// file by absolute location. A relative path is the table's, under its root. An absolute one is the table's when it
/// lies under the table's own location, and otherwise is reached through the <see cref="StorageResolver"/>, which
/// refuses a location nothing is registered for rather than reach storage it was not given.</para>
///
/// <para><b>What "absolute" means.</b> A path is absolute when it starts with a URI scheme (RFC 3986), which is how
/// delta-rs decides (<c>Url::parse(path)</c> succeeding, in <c>filesystem_check.rs</c> and the scan's
/// <c>parse_path</c>, deltalake-core 0.32.1 source) and how delta-spark's writers spell one (a qualified Hadoop path,
/// such as <c>s3://b/t/x.parquet</c> or <c>file:/tmp/t/x.parquet</c>). It is then read as a URI: its structure first,
/// then each segment percent-decoded (<see cref="StorageLocation.ParsePercentEncoded"/>), as delta-spark's
/// <c>new Path(new URI(path))</c> does (<c>DeltaFileOperations.absolutePath</c>, delta-spark 4.4.1 bytecode).</para>
///
/// <para>A ROOTED path with no scheme (<c>/data/x.parquet</c>, <c>C:/data/x.parquet</c>) is refused. The engines do
/// not agree on what it names: delta-spark keeps it as an absolute Hadoop path and opens it on the default file
/// system (<c>fs.defaultFS</c>), whatever the table's is; delta-kernel-rs joins it to the table root's URL, which
/// keeps the scheme and authority (the bucket) and drops the table's path; and delta-spark reads <c>C:/x</c> as a URI
/// with scheme <c>C</c>. No writer measured produces one. Before #963, EW handed it to the table's file system, which
/// on the local backend reached the absolute local path.</para>
/// </summary>
internal sealed class DeltaFileLocator
{
    private readonly StorageResolver _resolver;

    /// <param name="table">The table's file system, rooted at the table.</param>
    /// <param name="resolver">Reaches absolute locations outside the table; null reaches none.</param>
    public DeltaFileLocator(ITableFileSystem table, StorageResolver? resolver)
    {
        Table = table ?? throw new ArgumentNullException(nameof(table));
        _resolver = resolver ?? new StorageResolver();
        TableLocation = LocationOf(table);
        PathComparer = TableLocation?.SegmentComparison == StringComparison.OrdinalIgnoreCase
            ? StringComparer.OrdinalIgnoreCase
            : StringComparer.Ordinal;
    }

    /// <summary>
    /// <para>How two paths under the table root compare as names of one file: case-insensitively for a local table on
    /// Windows, as its file system compares them, and ordinally everywhere else (an unknown location included).</para>
    /// <para>A set that decides what must NOT be deleted (VACUUM's keep-set, the discard guard) uses it: a path an
    /// absolute URI gives keeps the URI's spelling, and on Windows <c>PART.parquet</c> there opens the listed
    /// <c>part.parquet</c>, so an ordinal set would miss the live file.</para>
    /// </summary>
    public StringComparer PathComparer { get; }

    /// <summary>The table's file system.</summary>
    public ITableFileSystem Table { get; }

    /// <summary>
    /// Where the table is, when its file system says (<see cref="ILocatedFileSystem"/>); null when it does not, in
    /// which case every absolute location goes through the resolver, and nothing can be proved to lie outside the
    /// table.
    /// </summary>
    public StorageLocation? TableLocation { get; }

    /// <summary>True when the URL-encoded action path <paramref name="encodedPath"/> is an absolute location.</summary>
    public static bool IsAbsolute(string encodedPath) => HasUriScheme(encodedPath);

    /// <summary>
    /// The location an absolute action path names. Throws <see cref="DeltaFormatException"/> when it is not a valid
    /// absolute URI, or names a location <see cref="StorageLocation"/> refuses (a <c>.</c> or <c>..</c> segment in an
    /// object store, #963).
    /// </summary>
    public static StorageLocation AbsoluteLocation(string encodedPath)
    {
        try
        {
            return StorageLocation.ParsePercentEncoded(encodedPath);
        }
        catch (Exception ex) when (ex is FormatException or ArgumentException or NotSupportedException)
        {
            throw new DeltaFormatException(
                DeltaErrorCodes.InvalidFilePath,
                $"The file path '{encodedPath}' is not a valid absolute location: {ex.Message}", ex);
        }
    }

    /// <summary>The file system and path of the file a URL-encoded <c>add</c>, <c>remove</c> or <c>cdc</c> path names.</summary>
    public ResolvedDeltaFile Resolve(string encodedPath)
    {
        if (encodedPath is null)
            throw new ArgumentNullException(nameof(encodedPath));
        if (HasUriScheme(encodedPath))
            return Resolve(AbsoluteLocation(encodedPath));
        return new ResolvedDeltaFile(Table, DecodeRelative(encodedPath), InTable: true);
    }

    /// <summary>
    /// The table-relative path of the file a URL-encoded action path names, for a reader that takes only paths under
    /// the table root: the codec seam's <c>IDataFileReader</c>. Throws <see cref="NotSupportedException"/> for a file
    /// outside the table (a shallow clone's source file), which such a reader cannot be handed.
    /// </summary>
    public string ResolveInTable(string encodedPath, string reader)
    {
        var file = Resolve(encodedPath);
        if (!file.InTable)
            throw new NotSupportedException(
                $"'{encodedPath}' lies outside the table, and {reader} reads only files under the table root.");
        return file.Path;
    }

    /// <summary>The file system and path of an absolute location: the table's when it lies under the table.</summary>
    public ResolvedDeltaFile Resolve(StorageLocation location)
    {
        if (location is null)
            throw new ArgumentNullException(nameof(location));
        if (TableLocation is not null && location.IsUnder(TableLocation))
            return new ResolvedDeltaFile(Table, location.RelativeTo(TableLocation), InTable: true);
        var resolved = _resolver.Resolve(location);
        return new ResolvedDeltaFile(resolved.FileSystem, resolved.Path, InTable: false);
    }

    /// <summary>
    /// <para>Where a URL-encoded action path lies relative to the table, without resolving anything: its path under the
    /// table root (decoded), or null when it is an absolute location outside the table.</para>
    /// <para>Throws <see cref="NotSupportedException"/> for an absolute location when the table's own location is
    /// unknown: whether it lies inside cannot then be decided, and a caller that would delete what it cannot place
    /// must not guess.</para>
    /// </summary>
    public string? TableRelativePath(string encodedPath)
    {
        if (encodedPath is null)
            throw new ArgumentNullException(nameof(encodedPath));
        if (!HasUriScheme(encodedPath))
            return DecodeRelative(encodedPath);

        return TableRelativePath(AbsoluteLocation(encodedPath), encodedPath);
    }

    /// <summary>As <see cref="TableRelativePath(string)"/>, for an absolute location; <paramref name="name"/> is how a
    /// refusal names it.</summary>
    public string? TableRelativePath(StorageLocation location, string name)
    {
        if (TableLocation is null)
            throw new NotSupportedException(
                $"'{name}' is an absolute location, and this table's file system does not say where the table is "
                + $"({nameof(ILocatedFileSystem)}), so whether it lies inside the table cannot be decided.");
        return location.IsUnder(TableLocation) ? location.RelativeTo(TableLocation) : null;
    }

    /// <summary>
    /// The URL-encoded directory a file written to replace <paramref name="encodedSourcePath"/> goes in, with a
    /// trailing <c>/</c> (or <c>""</c> for the table root). A relative source's own directory, verbatim, so it is
    /// never re-encoded. An absolute source inside the table gives its directory relative to the table; one outside
    /// the table (a shallow clone's source file) gives null: a write goes under the table, never next to another
    /// table's files, and the caller picks the directory.
    /// </summary>
    public string? EncodedDirectoryForRewrite(string encodedSourcePath)
    {
        if (!HasUriScheme(encodedSourcePath))
        {
            _ = DecodeRelative(encodedSourcePath);
            int slash = encodedSourcePath.LastIndexOf('/');
            return slash >= 0 ? encodedSourcePath.Substring(0, slash + 1) : "";
        }

        var location = AbsoluteLocation(encodedSourcePath);
        if (TableLocation is null || !location.IsUnder(TableLocation))
            return null;
        string relative = location.RelativeTo(TableLocation);
        int dirSlash = relative.LastIndexOf('/');
        return dirSlash >= 0 ? DeltaPath.Encode(relative.Substring(0, dirSlash)) + "/" : "";
    }

    /// <summary>True when <paramref name="text"/> starts with a URI scheme (RFC 3986: a letter, then letters,
    /// digits, <c>+</c>, <c>-</c> or <c>.</c>, then <c>:</c>). A single letter is a Windows drive, not a scheme.</summary>
    internal static bool HasUriScheme(string text)
    {
        int colon = text.IndexOf(':');
        if (colon < 2 || !IsAsciiLetter(text[0]))
            return false;
        for (int i = 1; i < colon; i++)
        {
            char c = text[i];
            if (!(IsAsciiLetter(c) || c is >= '0' and <= '9' || c is '+' or '-' or '.'))
                return false;
        }

        return true;
    }

    private static bool IsAsciiLetter(char c) => c is >= 'a' and <= 'z' or >= 'A' and <= 'Z';

    /// <summary>
    /// Decodes a relative action path, refusing one that is rooted (see the type's remarks). The DECODED path is
    /// checked, so an encoded <c>%2F</c> or drive does not pass for relative.
    /// </summary>
    private static string DecodeRelative(string encodedPath)
    {
        string decoded = DeltaPath.Decode(encodedPath);
        bool rooted = decoded.Length > 0 && (decoded[0] is '/' or '\\')
            || decoded.Length >= 2 && IsAsciiLetter(decoded[0]) && decoded[1] == ':';
        if (!rooted)
            return decoded;
        throw new DeltaFormatException(
            DeltaErrorCodes.InvalidFilePath,
            $"The file path '{encodedPath}' is rooted but has no URI scheme. A Delta file path is either relative "
            + "to the table root or an absolute URI (such as 's3://bucket/key' or 'file:/tmp/t/x.parquet'); "
            + "engines disagree on which file a rooted path without a scheme names, so it is not read.");
    }

    private static StorageLocation? LocationOf(ITableFileSystem fs)
    {
        if (fs is not ILocatedFileSystem located)
            return null;
        try
        {
            return located.Location;
        }
        catch (NotSupportedException)
        {
            return null;
        }
    }
}

/// <summary>A file an action names: the file system that holds it, its path there, and whether it is the table's
/// own (under the table root, reached through the table's file system).</summary>
internal readonly record struct ResolvedDeltaFile(ITableFileSystem FileSystem, string Path, bool InTable);

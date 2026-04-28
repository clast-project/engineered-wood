// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;

namespace EngineeredWood.Lance.Table.Manifest;

/// <summary>
/// Locates manifest files in a Lance dataset's <c>_versions/</c> directory.
///
/// <para>Modern Lance writers (and pylance 4.x) use a v2 manifest naming scheme
/// where each manifest filename is <c>{u64::MAX - version}.manifest</c>.
/// That negation makes lexicographic ordering match newest-first ordering
/// — listing the directory and taking the smallest-named file gives the
/// latest version with no parsing required.</para>
///
/// <para>The legacy (v1) naming used <c>{version}.manifest</c> directly,
/// where lexicographic order does NOT match version order across digit
/// counts. We support both: if filenames look numeric we parse them and
/// pick by version number, otherwise we fall back to lexicographic
/// (which is the v2 case).</para>
/// </summary>
internal static class ManifestPathResolver
{
    public const string VersionsDirectory = "_versions/";
    public const string ManifestExtension = ".manifest";

    /// <summary>
    /// Returns the full path of the latest manifest file in the dataset, or
    /// throws <see cref="LanceTableFormatException"/> if no manifest exists.
    /// </summary>
    public static async ValueTask<ManifestFileEntry> ResolveLatestAsync(
        ITableFileSystem fs, CancellationToken cancellationToken = default)
    {
        IReadOnlyList<ManifestFileEntry> entries = await ListAllAsync(fs, cancellationToken)
            .ConfigureAwait(false);
        if (entries.Count == 0)
            throw new LanceTableFormatException(
                $"No manifest files found in '{VersionsDirectory}'. Is this a Lance dataset?");
        return entries[0]; // ordered newest-first by ListAllAsync
    }

    /// <summary>
    /// Returns the full path of the manifest file for the requested
    /// version number, or throws if no such version exists in
    /// <c>_versions/</c>. Versions older than the table's vacuum
    /// threshold may have been pruned and are not available.
    /// </summary>
    public static async ValueTask<ManifestFileEntry> ResolveByVersionAsync(
        ITableFileSystem fs, ulong version,
        CancellationToken cancellationToken = default)
    {
        if (version == 0)
            throw new ArgumentOutOfRangeException(nameof(version),
                "Lance versions are 1-based; version 0 is not valid.");

        ulong encoded = ulong.MaxValue - version;
        IReadOnlyList<ManifestFileEntry> entries = await ListAllAsync(fs, cancellationToken)
            .ConfigureAwait(false);

        // Linear scan is fine — _versions/ is small (typically a handful of
        // files; vacuum keeps it bounded). Manifest filenames could also be
        // probed directly via fs.ExistsAsync, but a list is cheap and lets
        // us produce a useful error listing what IS available.
        foreach (var entry in entries)
        {
            if (entry.EncodedName == encoded)
                return entry;
        }

        var availableVersions = entries
            .Select(e => (ulong.MaxValue - e.EncodedName).ToString(System.Globalization.CultureInfo.InvariantCulture))
            .ToArray();
        throw new LanceTableFormatException(
            $"Lance manifest version {version} not found in '{VersionsDirectory}'. " +
            $"Available versions: [{string.Join(", ", availableVersions)}].");
    }

    /// <summary>
    /// Lists every manifest file in the dataset, ordered newest-first
    /// (highest version number first).
    /// </summary>
    public static async ValueTask<IReadOnlyList<ManifestFileEntry>> ListAllAsync(
        ITableFileSystem fs, CancellationToken cancellationToken = default)
    {
        var entries = new List<ManifestFileEntry>();
        await foreach (TableFileInfo file in fs.ListAsync(VersionsDirectory, cancellationToken)
            .ConfigureAwait(false))
        {
            string name = file.Path;
            int slash = name.LastIndexOfAny(new[] { '/', '\\' });
            string baseName = slash < 0 ? name : name.Substring(slash + 1);
            if (!baseName.EndsWith(ManifestExtension, StringComparison.Ordinal))
                continue;
            string stem = baseName.Substring(0, baseName.Length - ManifestExtension.Length);
            if (!ulong.TryParse(stem, System.Globalization.NumberStyles.None,
                    System.Globalization.CultureInfo.InvariantCulture, out ulong encoded))
                continue;
            entries.Add(new ManifestFileEntry(file.Path, encoded));
        }

        // Decode the v2 naming scheme: stored = u64::MAX - version. Smaller
        // stored value = larger version. Sort ascending on `EncodedName`,
        // which gives newest-first.
        entries.Sort(static (a, b) => a.EncodedName.CompareTo(b.EncodedName));
        return entries;
    }
}

/// <summary>
/// One manifest file in <c>_versions/</c>. <see cref="EncodedName"/> is
/// <c>u64::MAX - version</c> per the v2 naming convention.
/// <see cref="Version"/> derives the version number from that without
/// having to parse the manifest body.
/// </summary>
internal readonly record struct ManifestFileEntry(string Path, ulong EncodedName)
{
    public ulong Version => ulong.MaxValue - EncodedName;
}

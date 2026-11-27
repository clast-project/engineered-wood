// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using EngineeredWood.Iceberg.Serialization;
using EngineeredWood.IO;

namespace EngineeredWood.Iceberg;

/// <summary>
/// <para>An <see cref="ICatalog"/> implementation that persists namespace and table metadata
/// as JSON files under a warehouse location, using the Hadoop-style layout
/// with version-hint files and numbered metadata versions.</para>
/// <para>Every location it writes into table metadata is absolute, under the warehouse (for example
/// <c>file:///C:/wh/db/t</c> or <c>s3://bucket/wh/db/t</c>), as other engines require. Files a table names
/// elsewhere are reached through the same <see cref="Resolver"/>.</para>
/// </summary>
public sealed class FileSystemCatalog : ICatalog
{
    private readonly ITableFileSystem _fs;
    private readonly StorageLocation _warehouse;

    /// <summary>
    /// Initializes a catalog over the warehouse at <paramref name="warehouse"/>, which <paramref name="resolver"/>
    /// must reach (it may be registered there itself or lie under a registered prefix). The catalog's own files
    /// (metadata versions, version hints, namespace properties) are read and written through the file system the
    /// warehouse resolves to, as a Hadoop catalog uses one FileIO for its warehouse; a narrower registration, for one
    /// table's credentials say, serves the files that table names (manifests, data), which are reached through
    /// <see cref="Resolver"/>. Both must therefore reach the same storage.
    /// </summary>
    public FileSystemCatalog(StorageResolver resolver, StorageLocation warehouse)
    {
        Resolver = resolver ?? throw new ArgumentNullException(nameof(resolver));
        _warehouse = warehouse ?? throw new ArgumentNullException(nameof(warehouse));
        var resolved = resolver.Resolve(warehouse);
        _fs = SubtreeFileSystem.Create(resolved.FileSystem, resolved.Path);
    }

    /// <summary>Initializes a catalog over the local directory <paramref name="fs"/> is rooted at.</summary>
    public FileSystemCatalog(IO.Local.LocalTableFileSystem fs)
        : this(new StorageResolver().Register(fs), fs.Location)
    {
    }

    /// <summary>Reaches the files the catalog's tables name, for <see cref="Manifest.TableOperations"/> and
    /// <see cref="Expressions.TableScan"/>.</summary>
    public StorageResolver Resolver { get; }

    /// <summary>The warehouse location tables are created under.</summary>
    public StorageLocation Warehouse => _warehouse;

    /// <summary>The absolute location of a warehouse-relative path.</summary>
    private string Absolute(string relativePath) => _warehouse.Combine(relativePath).ToString();

    // --- Namespace operations ---

    private static string NamespacePath(Namespace ns)
    {
        foreach (var level in ns.Levels)
            ThrowIfNotOneSegment(level, "namespace level");
        return string.Join("/", ns.Levels);
    }

    /// <summary>
    /// Refuses a namespace level or table name that is not one plain path segment. Each one is a directory under the
    /// warehouse, so a <c>..</c>, a <c>.</c>, an empty name or a separator would put the files elsewhere: in a
    /// sibling of the warehouse, still inside the backend's root, where no root check catches it.
    /// </summary>
    private static void ThrowIfNotOneSegment(string name, string what)
    {
        if (name.Length == 0 || name is "." or ".." || name.IndexOfAny(['/', '\\']) >= 0)
            throw new ArgumentException(
                $"The {what} '{name}' is not a valid name in a file-system catalog: it is a directory under the " +
                "warehouse, so it cannot be empty, '.' or '..', or hold a '/' or '\\'.");
    }

    private static string NamespacePropertiesPath(Namespace ns) =>
        $"{NamespacePath(ns)}/.namespace-properties.json";

    public async Task<IReadOnlyList<Namespace>> ListNamespacesAsync(
        Namespace? parent = null, CancellationToken ct = default)
    {
        var prefix = parent is null || parent.IsEmpty
            ? ""
            : NamespacePath(parent) + "/";

        var depth = parent is null || parent.IsEmpty ? 1 : parent.Length + 1;
        var result = new List<Namespace>();
        var seen = new HashSet<string>();

        await foreach (var file in _fs.ListAsync(prefix, ct).ConfigureAwait(false))
        {
            if (!file.Path.EndsWith("/.namespace-properties.json", StringComparison.Ordinal))
                continue;

            // Extract the namespace from the path by taking the expected depth of levels
            var parts = file.Path.Split('/');
            if (parts.Length < depth + 1) // need at least depth levels + the filename
                continue;

            var levels = parts.Take(depth).ToArray();
            var key = string.Join("/", levels);
            if (seen.Add(key))
                result.Add(new Namespace(levels));
        }

        return result;
    }

    public async Task CreateNamespaceAsync(
        Namespace ns,
        IReadOnlyDictionary<string, string>? properties = null,
        CancellationToken ct = default)
    {
        if (ns.IsEmpty)
            throw new ArgumentException("Cannot create empty namespace");

        var propsPath = NamespacePropertiesPath(ns);

        if (await _fs.ExistsAsync(propsPath, ct).ConfigureAwait(false))
            throw new NamespaceAlreadyExistsException(ns);

        var props = properties ?? new Dictionary<string, string>();
        var json = IcebergJsonSerializer.Serialize(props);
        await _fs.WriteAllBytesAsync(propsPath, Encoding.UTF8.GetBytes(json), ct)
            .ConfigureAwait(false);
    }

    public async Task<IReadOnlyDictionary<string, string>> GetNamespacePropertiesAsync(
        Namespace ns, CancellationToken ct = default)
    {
        var propsPath = NamespacePropertiesPath(ns);

        if (!await _fs.ExistsAsync(propsPath, ct).ConfigureAwait(false))
            throw new NoSuchNamespaceException(ns);

        var data = await _fs.ReadAllBytesAsync(propsPath, ct).ConfigureAwait(false);
        var json = Encoding.UTF8.GetString(data);
        return IcebergJsonSerializer.Deserialize<Dictionary<string, string>>(json);
    }

    public async Task UpdateNamespacePropertiesAsync(
        Namespace ns,
        IReadOnlyList<string>? removals = null,
        IReadOnlyDictionary<string, string>? updates = null,
        CancellationToken ct = default)
    {
        var propsPath = NamespacePropertiesPath(ns);

        if (!await _fs.ExistsAsync(propsPath, ct).ConfigureAwait(false))
            throw new NoSuchNamespaceException(ns);

        var data = await _fs.ReadAllBytesAsync(propsPath, ct).ConfigureAwait(false);
        var props = IcebergJsonSerializer.Deserialize<Dictionary<string, string>>(
            Encoding.UTF8.GetString(data));

        if (removals is not null)
            foreach (var key in removals)
                props.Remove(key);

        if (updates is not null)
            foreach (var (key, value) in updates)
                props[key] = value;

        var updatedJson = IcebergJsonSerializer.Serialize(props);
        await _fs.WriteAllBytesAsync(propsPath, Encoding.UTF8.GetBytes(updatedJson), ct)
            .ConfigureAwait(false);
    }

    public async Task DropNamespaceAsync(Namespace ns, CancellationToken ct = default)
    {
        var propsPath = NamespacePropertiesPath(ns);

        if (!await _fs.ExistsAsync(propsPath, ct).ConfigureAwait(false))
            throw new NoSuchNamespaceException(ns);

        var nsPrefix = NamespacePath(ns) + "/";

        // Check for tables and child namespaces by scanning files
        await foreach (var file in _fs.ListAsync(nsPrefix, ct).ConfigureAwait(false))
        {
            // Skip the namespace's own properties file
            if (file.Path == propsPath.Replace('\\', '/'))
                continue;

            // A table: any .../metadata/v<N>.metadata.json below the namespace.
            var parts = file.Path.Split('/');
            if (parts.Length >= 2 && parts[^2] == "metadata" && ParseMetadataVersion(parts[^1]) is not null)
                throw new NamespaceNotEmptyException(ns);

            if (file.Path.EndsWith("/.namespace-properties.json", StringComparison.Ordinal))
                throw new NamespaceNotEmptyException(ns);
        }

        await _fs.DeleteAsync(propsPath, ct).ConfigureAwait(false);
    }

    public async Task<bool> NamespaceExistsAsync(Namespace ns, CancellationToken ct = default)
    {
        return await _fs.ExistsAsync(NamespacePropertiesPath(ns), ct).ConfigureAwait(false);
    }

    // --- Table operations ---
    //
    // Layout (Hadoop-style, as Iceberg Java's HadoopTableOperations writes it):
    //   <ns...>/<table>/metadata/v<N>.metadata.json   one file per version, N = 1, 2, ...
    //   <ns...>/<table>/metadata/version-hint.text    the latest N, as a hint only
    //
    // A table exists while it has a version file. Committing version N+1 is an atomic create-if-absent of
    // v<N+1>.metadata.json, so of two writers based on version N exactly one wins and the other gets
    // CommitFailedException. The hint is written after the commit and may be stale or missing (a writer can
    // fail between the two writes, and two writers' hint updates can land out of order); readers start from
    // it and walk forward to the newest version file.

    private const string MetadataFilePrefix = "v";
    private const string MetadataFileSuffix = ".metadata.json";
    private const string VersionHintFileName = "version-hint.text";

    private static string TablePath(TableIdentifier id)
    {
        ThrowIfNotOneSegment(id.Name, "table name");
        return $"{NamespacePath(id.Namespace)}/{id.Name}";
    }

    private static string MetadataDir(TableIdentifier id) =>
        $"{TablePath(id)}/metadata";

    private static string VersionHintPath(TableIdentifier id) =>
        $"{MetadataDir(id)}/{VersionHintFileName}";

    private static string MetadataFilePath(TableIdentifier id, int version) =>
        $"{MetadataDir(id)}/{MetadataFilePrefix}{version}{MetadataFileSuffix}";

    /// <summary>The version of a <c>v&lt;N&gt;.metadata.json</c> file name, or null for any other name.</summary>
    private static int? ParseMetadataVersion(string fileName)
    {
        if (!fileName.StartsWith(MetadataFilePrefix, StringComparison.Ordinal) ||
            !fileName.EndsWith(MetadataFileSuffix, StringComparison.Ordinal))
            return null;

        var digits = fileName.Substring(
            MetadataFilePrefix.Length,
            fileName.Length - MetadataFilePrefix.Length - MetadataFileSuffix.Length);
        if (digits.Length == 0 || digits.Length > 9 || !digits.All(c => c is >= '0' and <= '9'))
            return null;

        var version = int.Parse(digits, System.Globalization.CultureInfo.InvariantCulture);
        return version >= 1 ? version : null;
    }

    public async Task<IReadOnlyList<TableIdentifier>> ListTablesAsync(
        Namespace ns, CancellationToken ct = default)
    {
        if (!await _fs.ExistsAsync(NamespacePropertiesPath(ns), ct).ConfigureAwait(false))
            throw new NoSuchNamespaceException(ns);

        var nsPrefix = NamespacePath(ns) + "/";
        var result = new List<TableIdentifier>();
        var seen = new HashSet<string>();

        await foreach (var file in _fs.ListAsync(nsPrefix, ct).ConfigureAwait(false))
        {
            // Exactly <table>/metadata/v<N>.metadata.json below the namespace: deeper paths belong to child
            // namespaces' tables.
            var parts = file.Path.Substring(nsPrefix.Length).Split('/');
            if (parts.Length != 3 || parts[1] != "metadata" || ParseMetadataVersion(parts[2]) is null)
                continue;

            if (seen.Add(parts[0]))
                result.Add(new TableIdentifier(ns, parts[0]));
        }

        return result;
    }

    public async Task<Table> CreateTableAsync(
        TableIdentifier identifier,
        Schema schema,
        PartitionSpec? spec = null,
        SortOrder? sortOrder = null,
        string? location = null,
        IReadOnlyDictionary<string, string>? properties = null,
        CancellationToken ct = default)
    {
        if (!await _fs.ExistsAsync(NamespacePropertiesPath(identifier.Namespace), ct).ConfigureAwait(false))
            throw new NoSuchNamespaceException(identifier.Namespace);

        // Iceberg locations are absolute: a relative one names no base other engines would agree on.
        var tableLocation = location is null ? Absolute(TablePath(identifier)) : StorageLocation.Parse(location).ToString();
        var metadata = TableMetadata.Create(schema, spec, sortOrder, tableLocation, properties);
        return await CreateAsync(identifier, metadata, ct).ConfigureAwait(false);
    }

    public async Task<Table> LoadTableAsync(
        TableIdentifier identifier, CancellationToken ct = default)
    {
        var version = await FindCurrentVersionAsync(identifier, ct).ConfigureAwait(false)
            ?? throw new NoSuchTableException(identifier);
        var path = MetadataFilePath(identifier, version);
        var data = await _fs.ReadAllBytesAsync(path, ct).ConfigureAwait(false);
        var metadata = IcebergJsonSerializer.Deserialize<TableMetadata>(Encoding.UTF8.GetString(data));
        return new Table(identifier, metadata, Absolute(path));
    }

    /// <summary>
    /// Drops a table by deleting its metadata directory. With <paramref name="purge"/>, every file under the
    /// table's directory goes, data included; without it, data files are left in place.
    /// </summary>
    public async Task DropTableAsync(
        TableIdentifier identifier, bool purge = false, CancellationToken ct = default)
    {
        if (!await TableExistsAsync(identifier, ct).ConfigureAwait(false))
            throw new NoSuchTableException(identifier);

        // A commit can publish a new version while the old ones are being deleted, so delete until a listing
        // finds none. That narrows the race but cannot close it: the Hadoop layout has no way to make the
        // delete conditional, so a commit that publishes after the last listing re-creates the table at its
        // version (see doc/known-issues.md, "Commits").
        for (var round = 1; ; round++)
        {
            await RemoveVersionsAsync(identifier, ct).ConfigureAwait(false);
            if ((await ListVersionsAsync(identifier, ct).ConfigureAwait(false)).Count == 0)
                break;
            if (round == MaxRemovalRounds)
                throw new CommitFailedException(identifier,
                    $"new versions kept being committed while the table was dropped ({round} rounds); it was not dropped.");
        }

        var prefix = (purge ? TablePath(identifier) : MetadataDir(identifier)) + "/";
        var remaining = new List<string>();
        await foreach (var file in _fs.ListAsync(prefix, ct).ConfigureAwait(false))
            remaining.Add(file.Path);
        foreach (var path in remaining)
            await _fs.DeleteAsync(path, ct).ConfigureAwait(false);
    }

    public async Task RenameTableAsync(
        TableIdentifier from, TableIdentifier to, CancellationToken ct = default)
    {
        var current = await LoadTableAsync(from, ct).ConfigureAwait(false);

        if (!await _fs.ExistsAsync(NamespacePropertiesPath(to.Namespace), ct).ConfigureAwait(false))
            throw new NoSuchNamespaceException(to.Namespace);

        // The destination is created like a new table, so a concurrent create or rename of `to` loses
        // cleanly. Then only the source's version files and hint go: the renamed table's snapshots still
        // name the manifest lists and manifests under the source's metadata directory, and its data files
        // stay where they are. Nothing is moved.
        var baseVersion = ParseBaseVersion(current);
        var renamed = current.Metadata with
        {
            Location = Absolute(TablePath(to)),
            MetadataLog = [],
        };
        await CreateAsync(to, renamed, ct).ConfigureAwait(false);

        try
        {
            // A commit to the source after it was loaded would be lost by copying the loaded version, so
            // undo the destination and fail instead. Checked before the source is touched...
            // The version number alone is not enough: a drop and re-create of the source starts again at v1,
            // and removing that would delete an unrelated table.
            if (await FindCurrentVersionAsync(from, ct).ConfigureAwait(false) != baseVersion ||
                await ReadTableUuidAsync(from, baseVersion, ct).ConfigureAwait(false) != current.Metadata.TableUuid)
                throw new CommitFailedException(from, "the table changed while it was being renamed; it was not renamed.");

            // ...and again after its versions up to the loaded one are gone: one published in between
            // survives (it holds the complete metadata), so the source keeps its name at that version.
            await RemoveVersionsAsync(from, ct, upTo: baseVersion).ConfigureAwait(false);
            if ((await ListVersionsAsync(from, ct).ConfigureAwait(false)).Count > 0)
                throw new CommitFailedException(from,
                    "a commit landed while the table was being renamed; it keeps its old name at that commit.");
        }
        catch (CommitFailedException)
        {
            // Only the v1 this rename wrote: a version someone already committed on top of it is theirs.
            await RemoveVersionsAsync(to, CancellationToken.None, upTo: 1).ConfigureAwait(false);
            throw;
        }
    }

    public async Task<bool> TableExistsAsync(
        TableIdentifier identifier, CancellationToken ct = default)
    {
        return (await ListVersionsAsync(identifier, ct).ConfigureAwait(false)).Count > 0;
    }

    public async Task<Table> CommitTableAsync(
        Table baseTable, TableMetadata newMetadata, CancellationToken ct = default)
    {
        var identifier = baseTable.Identifier;
        var baseVersion = ParseBaseVersion(baseTable);

        if (newMetadata.TableUuid != baseTable.Metadata.TableUuid)
            throw new ArgumentException(
                $"The new metadata belongs to table {newMetadata.TableUuid}, not {baseTable.Metadata.TableUuid}.",
                nameof(newMetadata));

        var current = await FindCurrentVersionAsync(identifier, ct).ConfigureAwait(false)
            ?? throw new NoSuchTableException(identifier);
        if (current != baseVersion)
            throw new CommitFailedException(identifier,
                $"the commit is based on version {baseVersion}, but the table is at version {current}.");

        // Same version number, but is it the same table? A drop and re-create starts again at v1.
        if (await ReadTableUuidAsync(identifier, current, ct).ConfigureAwait(false) != baseTable.Metadata.TableUuid)
            throw new CommitFailedException(identifier,
                "the table was dropped and re-created after the commit's base was loaded.");

        var committed = WithPreviousMetadataLogged(newMetadata, baseTable);
        var path = MetadataFilePath(identifier, baseVersion + 1);
        var json = IcebergJsonSerializer.Serialize(committed);

        // The commit point: whoever creates v<N+1> first has committed version N+1.
        if (!await _fs.TryWriteAllBytesAsync(path, Encoding.UTF8.GetBytes(json), ct).ConfigureAwait(false))
            throw new CommitFailedException(identifier,
                $"another writer committed version {baseVersion + 1} first.");

        await TryWriteVersionHintAsync(identifier, baseVersion + 1).ConfigureAwait(false);
        return new Table(identifier, committed, Absolute(path));
    }

    // --- Internal helpers ---

    /// <summary>
    /// Ends the table's existence by deleting its hint and version files. The hint goes first, then the
    /// versions oldest first, so a removal that fails part way leaves the newest versions (which a reader
    /// would find) rather than resurrecting an older one.
    /// </summary>
    private async ValueTask RemoveVersionsAsync(TableIdentifier identifier, CancellationToken ct, int? upTo = null)
    {
        await _fs.DeleteAsync(VersionHintPath(identifier), ct).ConfigureAwait(false);
        foreach (var (version, path) in await ListVersionsAsync(identifier, ct).ConfigureAwait(false))
        {
            if (upTo is { } last && version > last)
                break;
            await _fs.DeleteAsync(path, ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The table UUID recorded in the table's <paramref name="version"/>, or null when that version is gone (a
    /// concurrent drop), which callers treat as a different table.
    /// </summary>
    private async ValueTask<string?> ReadTableUuidAsync(TableIdentifier identifier, int version, CancellationToken ct)
    {
        var path = MetadataFilePath(identifier, version);
        if (!await _fs.ExistsAsync(path, ct).ConfigureAwait(false))
            return null;
        byte[] data;
        try
        {
            data = await _fs.ReadAllBytesAsync(path, ct).ConfigureAwait(false);
        }
        catch (FileNotFoundException)
        {
            return null;
        }
        return IcebergJsonSerializer.Deserialize<TableMetadata>(Encoding.UTF8.GetString(data)).TableUuid;
    }

    /// <summary>How many times a drop deletes newly committed versions before giving up.</summary>
    private const int MaxRemovalRounds = 5;

    private async ValueTask<Table> CreateAsync(
        TableIdentifier identifier, TableMetadata metadata, CancellationToken ct)
    {
        if (await TableExistsAsync(identifier, ct).ConfigureAwait(false))
            throw new TableAlreadyExistsException(identifier);

        // Two concurrent creators both reach here; v1 is created by exactly one of them.
        var path = MetadataFilePath(identifier, 1);
        var json = IcebergJsonSerializer.Serialize(metadata);
        if (!await _fs.TryWriteAllBytesAsync(path, Encoding.UTF8.GetBytes(json), ct).ConfigureAwait(false))
            throw new TableAlreadyExistsException(identifier);

        await TryWriteVersionHintAsync(identifier, 1).ConfigureAwait(false);
        return new Table(identifier, metadata, Absolute(path));
    }

    private int ParseBaseVersion(Table baseTable)
    {
        // Compared as locations, so a table loaded by a catalog that spells the warehouse differently (file:/wh and
        // file:///wh, s3a:// and s3://) commits here too.
        var expectedDir = _warehouse.Combine(MetadataDir(baseTable.Identifier));
        var location = baseTable.MetadataLocation;
        if (location is null || !StorageLocation.TryParse(location, out var parsed) ||
            !parsed!.IsUnder(expectedDir) ||
            parsed.RelativeTo(expectedDir) is not { Length: > 0 } fileName || fileName.IndexOf('/') >= 0 ||
            ParseMetadataVersion(fileName) is not { } version)
            throw new ArgumentException(
                $"The base table's metadata location '{location}' is not a version of {baseTable.Identifier} in " +
                "this catalog; load the table from this catalog before committing to it.",
                nameof(baseTable));
        return version;
    }

    /// <summary>
    /// Adds the base version's file to the metadata log, as Iceberg's metadata builder does, keeping at most
    /// <c>write.metadata.previous-versions-max</c> entries (default 100).
    /// </summary>
    private static TableMetadata WithPreviousMetadataLogged(TableMetadata metadata, Table baseTable)
    {
        var location = baseTable.MetadataLocation!;
        var log = metadata.MetadataLog.ToList();
        if (log.Count == 0 || log[^1].MetadataFile != location)
            log.Add(new MetadataLogEntry(baseTable.Metadata.LastUpdatedMs, location));

        var max = 100;
        if (metadata.Properties.TryGetValue("write.metadata.previous-versions-max", out var text) &&
            int.TryParse(text, System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture, out var parsed) && parsed >= 1)
            max = parsed;
        if (log.Count > max)
            log.RemoveRange(0, log.Count - max);

        return metadata with { MetadataLog = log };
    }

    /// <summary>The versions of the table's metadata files, oldest first.</summary>
    private async ValueTask<List<(int Version, string Path)>> ListVersionsAsync(
        TableIdentifier identifier, CancellationToken ct)
    {
        var dir = MetadataDir(identifier) + "/";
        var versions = new List<(int, string)>();
        await foreach (var file in _fs.ListAsync(dir, ct).ConfigureAwait(false))
        {
            var name = file.Path.Substring(dir.Length);
            if (name.IndexOf('/') < 0 && ParseMetadataVersion(name) is { } version)
                versions.Add((version, file.Path));
        }

        versions.Sort((a, b) => a.Item1.CompareTo(b.Item1));
        return versions;
    }

    /// <summary>
    /// The table's newest version: the hint's, when it names an existing version file, else the newest
    /// version file listed; then forward past any versions committed after the hint was written. Null when
    /// the table has no version file.
    /// </summary>
    private async ValueTask<int?> FindCurrentVersionAsync(TableIdentifier identifier, CancellationToken ct)
    {
        var version = await TryReadVersionHintAsync(identifier, ct).ConfigureAwait(false);
        if (version is not null &&
            !await _fs.ExistsAsync(MetadataFilePath(identifier, version.Value), ct).ConfigureAwait(false))
            version = null;

        if (version is null)
        {
            var versions = await ListVersionsAsync(identifier, ct).ConfigureAwait(false);
            if (versions.Count == 0)
                return null;
            version = versions[^1].Version;
        }

        while (await _fs.ExistsAsync(MetadataFilePath(identifier, version.Value + 1), ct).ConfigureAwait(false))
            version++;

        return version;
    }

    /// <summary>
    /// The version the hint names, or null when it is missing or unreadable: the file may be mid-overwrite
    /// by another writer (on Windows, a sharing violation) or deleted by a concurrent drop, and since it is
    /// only a hint the caller falls back to listing.
    /// </summary>
    private async ValueTask<int?> TryReadVersionHintAsync(TableIdentifier identifier, CancellationToken ct)
    {
        string text;
        try
        {
            var hint = VersionHintPath(identifier);
            if (!await _fs.ExistsAsync(hint, ct).ConfigureAwait(false))
                return null;
            text = Encoding.UTF8.GetString(await _fs.ReadAllBytesAsync(hint, ct).ConfigureAwait(false)).Trim();
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            return null;
        }

        return int.TryParse(text, System.Globalization.NumberStyles.None,
                   System.Globalization.CultureInfo.InvariantCulture, out var hinted) && hinted >= 1
            ? hinted
            : null;
    }

    /// <summary>
    /// Points the hint at <paramref name="version"/>. Best effort: the commit has already happened, and a
    /// reader finds the version without the hint, so a failure here is not the commit's failure.
    /// </summary>
    /// <remarks>
    /// Runs only after the version is published, so it takes no cancellation token and swallows every
    /// failure, cancellation included: a caller cancelled at this point must see the commit it made, not an
    /// exception that invites retrying (and so duplicating) it.
    /// </remarks>
    private async ValueTask TryWriteVersionHintAsync(TableIdentifier identifier, int version)
    {
        try
        {
            await _fs.WriteAllBytesAsync(
                    VersionHintPath(identifier),
                    Encoding.UTF8.GetBytes(version.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                    CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Ignored, as above.
        }
    }
}

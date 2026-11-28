// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;
using EngineeredWood.Lance.Table.Proto;

namespace EngineeredWood.Lance.Table;

/// <summary>
/// <para>Finds the files a Lance manifest names. A data file, deletion file or index with no <c>base_id</c> lies under
/// the dataset root. One with a <c>base_id</c> lies under the manifest's <c>base_paths</c> entry of that id, which a
/// shallow clone sets to its source dataset and an import or multi-base write sets to another location. Such a file
/// is the dataset's own when it lies under the dataset's location, and is otherwise reached through the
/// <see cref="StorageResolver"/>, which refuses a location nothing is registered for (#963).</para>
///
/// <para><b>Layout</b> (lance 14.0.3 source, <c>Dataset::data_file_dir_for_base</c>,
/// <c>dataset_dir_for_deletion</c> and <c>indice_files_dir</c>): a base with <c>is_dataset_root</c> holds its files
/// as a dataset does (<c>{base}/data/</c>, <c>{base}/_deletions/</c>, <c>{base}/_indices/{uuid}/</c>); one without
/// is the directory itself (<c>{base}/{file}</c>, <c>{base}/{uuid}/</c>), and a deletion file under one is an
/// error.</para>
///
/// <para><b>What a base's path names.</b> Lance parses it with <c>uri_to_url</c> (lance-io 14.0.3): text with a URI
/// scheme is a URI, whose path the object store percent-decodes (<c>Path::from_url_path</c>); anything else is a
/// local path, made absolute against the reading PROCESS's working directory and taken literally. pylance's own
/// <c>shallow_clone</c> on local disk stores a bare absolute path (measured, pylance 14.0.3: <c>C:/…/m1\src</c> on
/// Windows). So:</para>
/// <list type="bullet">
/// <item>A URI is parsed with <see cref="StorageLocation.ParsePercentEncoded"/>.</item>
/// <item>A rooted path with no scheme is a local file location, parsed literally (<see cref="StorageLocation.Parse"/>).</item>
/// <item>A relative path is REFUSED: what it names depends on the working directory of whoever opens the dataset,
/// not on the dataset (pylance 14.0.3 stores <c>relbase</c> and <c>main/../dd</c> verbatim).</item>
/// <item>A <c>.</c> or <c>..</c> segment outside a <c>file</c> location is refused, as <see cref="StorageLocation"/>
/// refuses it everywhere (#963).</item>
/// </list>
///
/// <para>Nothing here writes or deletes. Every file EW writes goes under the dataset root, as lance's do unless a
/// writer names a target base.</para>
/// </summary>
internal sealed class LanceFileLocator
{
    private const string DataDirectory = "data";
    private const string DeletionsDirectory = "_deletions";
    private const string IndicesDirectory = "_indices";
    private static readonly bool IsWindows = Path.DirectorySeparatorChar == '\\';

    private readonly StorageResolver _resolver;
    private readonly Dictionary<uint, BasePath> _basesById;
    private readonly Dictionary<uint, StorageLocation> _parsed = new();

    /// <param name="table">The dataset's file system, rooted at the dataset.</param>
    /// <param name="manifest">The manifest whose <c>base_paths</c> name the other locations.</param>
    /// <param name="resolver">Reaches locations outside the dataset; null reaches none.</param>
    public LanceFileLocator(ITableFileSystem table, Proto.Manifest manifest, StorageResolver? resolver)
    {
        Table = table ?? throw new ArgumentNullException(nameof(table));
        if (manifest is null) throw new ArgumentNullException(nameof(manifest));
        _resolver = resolver ?? new StorageResolver();
        TableLocation = LocationOf(table);

        _basesById = new Dictionary<uint, BasePath>(manifest.BasePaths.Count);
        foreach (var basePath in manifest.BasePaths)
        {
            if (_basesById.ContainsKey(basePath.Id))
                throw new LanceTableFormatException(
                    $"The manifest lists base path id {basePath.Id} more than once.");
            _basesById.Add(basePath.Id, basePath);
        }
    }

    /// <summary>The dataset's file system.</summary>
    public ITableFileSystem Table { get; }

    /// <summary>
    /// Where the dataset is, when its file system says (<see cref="ILocatedFileSystem"/>); null when it does not, in
    /// which case every base goes through the resolver.
    /// </summary>
    public StorageLocation? TableLocation { get; }

    /// <summary>
    /// Checks every base the manifest's fragments refer to: that it exists, that a deletion file's base is a dataset
    /// root, and that its path is one EW reads. Resolution against the resolver is left to the first read, so a
    /// dataset whose bases are not registered still opens, and fails only when it reads such a file.
    /// </summary>
    public void Validate(Proto.Manifest manifest)
    {
        foreach (var fragment in manifest.Fragments)
        {
            foreach (var file in fragment.Files)
            {
                if (file.HasBaseId)
                    _ = BaseLocation(file.BaseId, $"data file '{file.Path}' of fragment {fragment.Id}");
            }

            var deletion = fragment.DeletionFile;
            if (deletion is not null && deletion.HasBaseId)
                _ = DeletionDirectory(deletion.BaseId, fragment.Id);
        }
    }

    /// <summary>The file system and path of a fragment's data file.</summary>
    public ResolvedLanceFile ResolveDataFile(DataFragment fragment, DataFile file)
    {
        if (!file.HasBaseId)
            return new ResolvedLanceFile(Table, DataDirectory + "/" + file.Path);

        string what = $"data file '{file.Path}' of fragment {fragment.Id}";
        var basePath = Base(file.BaseId, what);
        var location = BaseLocation(file.BaseId, what);
        var directory = basePath.IsDatasetRoot ? location.Combine(DataDirectory) : location;
        return Resolve(Combine(directory, file.Path, what));
    }

    /// <summary>
    /// The file system and path of a fragment's deletion file, given its name
    /// (<c>{fragment_id}-{read_version}-{id}.{ext}</c>).
    /// </summary>
    public ResolvedLanceFile ResolveDeletionFile(DataFragment fragment, string fileName)
    {
        var deletion = fragment.DeletionFile
            ?? throw new ArgumentException($"Fragment {fragment.Id} has no deletion file.", nameof(fragment));
        if (!deletion.HasBaseId)
            return new ResolvedLanceFile(Table, DeletionsDirectory + "/" + fileName);

        var directory = DeletionDirectory(deletion.BaseId, fragment.Id);
        return Resolve(Combine(directory, fileName, $"deletion file of fragment {fragment.Id}"));
    }

    /// <summary>The file system and directory of an index's files.</summary>
    public ResolvedLanceFile ResolveIndexDirectory(IndexMetadata index, Guid uuid)
    {
        string name = uuid.ToString("D");
        if (!index.HasBaseId)
            return new ResolvedLanceFile(Table, IndicesDirectory + "/" + name);

        string what = $"index '{index.Name}' ({name})";
        var basePath = Base(index.BaseId, what);
        var location = BaseLocation(index.BaseId, what);
        var directory = basePath.IsDatasetRoot ? location.Combine(IndicesDirectory) : location;
        return Resolve(Combine(directory, name, what));
    }

    private StorageLocation DeletionDirectory(uint baseId, ulong fragmentId)
    {
        string what = $"deletion file of fragment {fragmentId}";
        var basePath = Base(baseId, what);
        // lance 14.0.3 Dataset::dataset_dir_for_deletion: "base_path id {} is not a dataset root for deletion_file".
        if (!basePath.IsDatasetRoot)
            throw new LanceTableFormatException(
                $"The {what} names base path {baseId}, which is not a dataset root; a deletion file must lie under "
                + "a dataset root's _deletions/.");
        return BaseLocation(baseId, what).Combine(DeletionsDirectory);
    }

    private BasePath Base(uint baseId, string what) =>
        _basesById.TryGetValue(baseId, out var basePath)
            ? basePath
            : throw new LanceTableFormatException(
                $"The {what} names base path id {baseId}, which the manifest does not list.");

    /// <summary>The location base <paramref name="baseId"/> names, parsed once.</summary>
    private StorageLocation BaseLocation(uint baseId, string what)
    {
        lock (_parsed)
        {
            if (_parsed.TryGetValue(baseId, out var cached))
                return cached;
        }

        var location = ParseBasePath(Base(baseId, what).Path, baseId);
        lock (_parsed)
            _parsed[baseId] = location;
        return location;
    }

    /// <summary>Reads a base's path (see the type's remarks).</summary>
    internal static StorageLocation ParseBasePath(string path, uint baseId)
    {
        if (string.IsNullOrEmpty(path))
            throw new LanceTableFormatException($"Base path {baseId} has an empty path.");

        if (!StorageLocation.LooksAbsolute(path))
            throw new NotSupportedException(
                $"Base path {baseId} is the relative path '{path}'. Lance resolves a relative base against the "
                + "working directory of the process that opens the dataset, so what it names is not part of the "
                + "dataset, and EW does not read it.");

        // Off Windows a '\' in a bare path is a file-name character (lance takes the path literally), but
        // StorageLocation.Parse reads it as a separator on every platform, so '/t\src' would name '/t/src', which may
        // even lie under the dataset. Refuse it rather than read a different directory.
        if (!IsWindows && !HasUriScheme(path) && path.IndexOf('\\') >= 0)
            throw new NotSupportedException(
                $"Base path {baseId} is the local path '{path}', which has a '\\'. Here that is a character of a file "
                + "name, and EW does not yet read such a path literally.");

        try
        {
            var location = HasUriScheme(path) ? StorageLocation.ParsePercentEncoded(path) : StorageLocation.Parse(path);
            if (!HasUriScheme(path) && !location.IsFile)
                throw new FormatException($"'{path}' has no scheme but is not a local path.");
            return location;
        }
        catch (NotSupportedException ex)
        {
            throw new NotSupportedException($"Base path {baseId} ('{path}') is not read: {ex.Message}", ex);
        }
        catch (Exception ex) when (ex is FormatException or ArgumentException)
        {
            throw new LanceTableFormatException(
                $"Base path {baseId} ('{path}') is not a valid location: {ex.Message}", ex);
        }
    }

    private static StorageLocation Combine(StorageLocation directory, string name, string what)
    {
        if (string.IsNullOrEmpty(name))
            throw new LanceTableFormatException($"The {what} has an empty path.");
        try
        {
            return directory.Combine(name);
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException)
        {
            throw new LanceTableFormatException(
                $"The {what} has the path '{name}', which does not name a file under '{directory}': {ex.Message}", ex);
        }
    }

    private ResolvedLanceFile Resolve(StorageLocation location)
    {
        if (TableLocation is not null && location.IsUnder(TableLocation))
            return new ResolvedLanceFile(Table, location.RelativeTo(TableLocation));
        var resolved = _resolver.Resolve(location);
        return new ResolvedLanceFile(resolved.FileSystem, resolved.Path);
    }

    /// <summary>True when <paramref name="text"/> starts with a URI scheme (RFC 3986) of two or more characters; a
    /// single letter is a Windows drive, as lance reads it on Windows (<c>uri_to_url</c>).</summary>
    private static bool HasUriScheme(string text)
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

/// <summary>A file a manifest names: the file system that holds it and its path there.</summary>
internal readonly record struct ResolvedLanceFile(ITableFileSystem FileSystem, string Path);

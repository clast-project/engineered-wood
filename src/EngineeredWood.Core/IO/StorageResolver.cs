// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO.Local;

namespace EngineeredWood.IO;

/// <summary>
/// <para>Reaches absolute <see cref="StorageLocation"/>s through file systems registered for location prefixes. A
/// location resolves to the file system registered for its longest matching prefix, and to its path relative to that
/// prefix; that path is what the file system, which is rooted at the prefix, takes.</para>
///
/// <para>Prefixes rather than schemes, because credentials are per table: two tables in one bucket may need two
/// clients (Iceberg REST vended credentials, Unity Catalog). A resolver knows only what it was given. It never builds a
/// client from ambient credentials, and a location under no registered prefix, whatever its scheme, is refused.</para>
///
/// <para>Each cloud package adds a <c>Register</c> overload for its own file system, which knows the location it is
/// rooted at. Registration is safe to call concurrently with resolution.</para>
/// </summary>
public sealed class StorageResolver
{
    private readonly object _lock = new();
    private Registration[] _registrations = [];

    /// <summary>
    /// Registers <paramref name="fileSystem"/> for every location under <paramref name="prefix"/>.
    /// <paramref name="fileSystem"/> must be rooted at <paramref name="prefix"/>: it is handed paths relative to it.
    /// A prefix can be registered once.
    /// </summary>
    /// <returns>This resolver, so registrations can be chained.</returns>
    public StorageResolver Register(StorageLocation prefix, ITableFileSystem fileSystem)
    {
        if (prefix is null)
            throw new ArgumentNullException(nameof(prefix));
        if (fileSystem is null)
            throw new ArgumentNullException(nameof(fileSystem));

        lock (_lock)
        {
            if (_registrations.Any(r => r.Prefix == prefix))
                throw new ArgumentException($"A file system is already registered for '{prefix}'.", nameof(prefix));

            // Longest prefix first, so the first match is the most specific one.
            _registrations = _registrations
                .Append(new Registration(prefix, fileSystem))
                .OrderByDescending(r => r.Prefix.SegmentCount)
                .ToArray();
        }

        return this;
    }

    /// <summary>Registers <paramref name="fileSystem"/> for every location under <paramref name="prefix"/>.</summary>
    public StorageResolver Register(string prefix, ITableFileSystem fileSystem) =>
        Register(StorageLocation.Parse(prefix), fileSystem);

    /// <summary>Registers a local file system for every location under the directory it is rooted at.</summary>
    public StorageResolver Register(LocalTableFileSystem fileSystem)
    {
        if (fileSystem is null)
            throw new ArgumentNullException(nameof(fileSystem));
        return Register(fileSystem.Location, fileSystem);
    }

    /// <summary>The prefixes registered so far, most specific first.</summary>
    public IReadOnlyList<StorageLocation> Prefixes => Volatile.Read(ref _registrations).Select(r => r.Prefix).ToArray();

    /// <summary>
    /// Finds the file system for <paramref name="location"/> and its path there, or returns <see langword="false"/>
    /// when no registered prefix covers it.
    /// </summary>
    public bool TryResolve(StorageLocation location, out ResolvedStorageLocation resolved)
    {
        if (location is null)
            throw new ArgumentNullException(nameof(location));

        foreach (var registration in Volatile.Read(ref _registrations))
        {
            if (location.IsUnder(registration.Prefix))
            {
                resolved = new ResolvedStorageLocation(
                    registration.FileSystem, location.RelativeTo(registration.Prefix), registration.Prefix);
                return true;
            }
        }

        resolved = default;
        return false;
    }

    /// <summary>
    /// Finds the file system for <paramref name="location"/> and its path there. Throws
    /// <see cref="UnregisteredStorageLocationException"/> when no registered prefix covers it.
    /// </summary>
    public ResolvedStorageLocation Resolve(StorageLocation location)
    {
        if (TryResolve(location, out var resolved))
            return resolved;
        throw new UnregisteredStorageLocationException(location);
    }

    /// <summary>Parses <paramref name="location"/> and resolves it; see <see cref="Resolve(StorageLocation)"/>.</summary>
    public ResolvedStorageLocation Resolve(string location) => Resolve(StorageLocation.Parse(location));

    private sealed record Registration(StorageLocation Prefix, ITableFileSystem FileSystem);
}

/// <summary>A location resolved by a <see cref="StorageResolver"/>.</summary>
/// <param name="FileSystem">The file system registered for <paramref name="Prefix"/>.</param>
/// <param name="Path">The location's path relative to <paramref name="Prefix"/>, which is what
/// <paramref name="FileSystem"/> takes. Empty when the location is the prefix itself.</param>
/// <param name="Prefix">The registered prefix the location lies under.</param>
public readonly record struct ResolvedStorageLocation(ITableFileSystem FileSystem, string Path, StorageLocation Prefix);

/// <summary>
/// Thrown when a <see cref="StorageResolver"/> has no file system registered for a prefix of a location. EW never
/// falls back to a default client or to ambient credentials; register a file system for the location's prefix.
/// </summary>
public sealed class UnregisteredStorageLocationException : InvalidOperationException
{
    /// <summary>Creates the exception for <paramref name="location"/>.</summary>
    public UnregisteredStorageLocationException(StorageLocation location)
        : base($"No file system is registered for '{location}' or any prefix of it. Register one with " +
            $"{nameof(StorageResolver)}.{nameof(StorageResolver.Register)}; EW does not build storage clients from " +
            "ambient credentials.")
    {
        Location = location;
    }

    /// <summary>The location that could not be resolved.</summary>
    public StorageLocation Location { get; }
}

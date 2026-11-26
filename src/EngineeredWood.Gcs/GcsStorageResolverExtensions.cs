// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;

namespace EngineeredWood.IO.Gcs;

/// <summary>Registers <see cref="GcsTableFileSystem"/> instances with a <see cref="StorageResolver"/>.</summary>
public static class GcsStorageResolverExtensions
{
    /// <summary>
    /// Registers <paramref name="fileSystem"/> for every location under the one it is rooted at
    /// (<see cref="GcsTableFileSystem.Location"/>).
    /// </summary>
    /// <returns>The resolver, so registrations can be chained.</returns>
    public static StorageResolver Register(this StorageResolver resolver, GcsTableFileSystem fileSystem)
    {
        if (resolver is null)
            throw new ArgumentNullException(nameof(resolver));
        if (fileSystem is null)
            throw new ArgumentNullException(nameof(fileSystem));
        return resolver.Register(fileSystem.Location, fileSystem);
    }
}
